use std::io::{Read, Write, Result};
use std::net::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::thread;
use std::process::Command;
use std::borrow::Cow;
use std::env;
use std::sync::{Arc, mpsc, Mutex};

mod operations;
use operations::Operations;

static mut MAX_PROPOSAL_NUMBER: u8 = 1;
static mut IS_CONSENSUS_ACHIEVED: bool = false;

fn build_response(proposal_type: &str, proposal_number: &str) -> String {
    let mut proposal = String::new();
    proposal.push_str(proposal_type);
    proposal.push_str(" ");
    proposal.push_str(proposal_number);
    return proposal.to_string();
}

fn get_max_proposal_number() -> u8 {
    unsafe {
        return MAX_PROPOSAL_NUMBER;
    }
}

fn respond_to_client(message: Cow<str>, mut stream: TcpStream, hash_table: Arc<Mutex<HashMap<String, String>>>) -> Result<()>{
    unsafe {
        if IS_CONSENSUS_ACHIEVED {
            // Parse the command, key and value from the message
            let chunks: Vec<&str> = message.split_whitespace().collect();
            let command = chunks[0];
            let key = chunks[1];
            let mut value = "0";
            if chunks.len() > 2 {
                value = chunks[2];
                println!("Proposer: Command : {}, Key: {}, Value: {} parsed from the client message", command, key, value);
            }
            else {
                println!("Proposer: Command : {}, Key: {} parsed from the client message", command, key);
            }
    
            // Match the command and modify hash_table if applicable
            let response = match command {
                "get" => match hash_table.lock().unwrap().get(key) {
                    Some(value) => format!("Key {} has Value {}", key, value),
                    None => "ERROR Key not found".to_string(),
                },
                "put" => {
                    hash_table.lock().unwrap().insert(key.to_string(), value.to_string());
                    "OK".to_string()
                }
                _ => "ERROR Invalid command".to_string(),
            };
    
            println!("Proposer: Response back to client: {}", response);
    
            // Send the response back to the client
            stream.write(response.as_bytes())?;
        }
        else {
            let response = "Consensus not reached by the network";
            println!("Response : {}", response);
    
            // Send the response back to the client
            stream.write(response.as_bytes())?;
        }
        IS_CONSENSUS_ACHIEVED = false;
    }
    return Ok(());
}

fn communicate_with_nodes(mut server_stream: TcpStream, tx: mpsc::Sender<String>, proposal_number: u8, node_idx: u16)  -> Result<()>{
    let initial_message = build_response(Operations::Prepare.to_string().as_str(), proposal_number.to_string().as_str());
    server_stream.write_all(initial_message.as_bytes()).unwrap();
    println!("Proposer: Sent message : {} to node: {}", initial_message, node_idx);

    // Receive message from nodes
    let mut buffer = [0; 1024];
    
    loop {
        match server_stream.read(&mut buffer) {
            Ok(0) => break, // End of server_stream
            Ok(n) => {
                let message = String::from_utf8_lossy(&buffer[0..n]).trim().to_string();
                if message.len() > 1 {
                    // Send a set of messages to server A
                    let chunks: Vec<&str> = message.split_whitespace().collect();
                    let operation = chunks[0];
                    let proposal_number = chunks[1];
                    println!("Proposer: Received operation: {} and proposal number: {} from node {}", operation, proposal_number, node_idx);
    
                    let response = match operation {
                        "promise" => build_response(Operations::AcceptRequest.to_string().as_str(), proposal_number),
                        "accepted" => {
                            tx.send(build_response(Operations::Accepted.to_string().as_str(), proposal_number)).unwrap();
                            break;
                        },
                        _ => "ERROR Invalid operation".to_string(),
                    };
                    server_stream.write_all(response.as_bytes()).unwrap();
                    println!("Proposer: Sent message {} to node: {}", response, node_idx);
                }
            }
            Err(err) => panic!("Proposer: Error reading from server_stream: {}, node : {}", err, node_idx),
        }
    }
    return Ok(());
}

fn handle_client(base_port: u16, num_nodes: u16, mut stream: TcpStream, proposal_number: u8, hash_table: Arc<Mutex<HashMap<String, String>>>) -> Result<()>{  
    let (tx, rx) = mpsc::channel::<String>();

    let mut counter: u16 = 1;

    thread::spawn(move || {
        for received in rx {
            let chunks: Vec<&str> = received.split_whitespace().collect();
            let operation = chunks[0];
            let proposal_number = chunks[1];
            let prop_num: u8 = proposal_number.to_string().parse::<u8>().unwrap();
            unsafe {
                if prop_num == MAX_PROPOSAL_NUMBER {                   
                    if operation.to_string() == Operations::Accepted.to_string() {
                        if counter != (num_nodes/2 + 1) {
                            counter += 1;
                        }
                        else {
                            counter = 0;
                            MAX_PROPOSAL_NUMBER += 1;
                            IS_CONSENSUS_ACHIEVED = true;
                            println!("Proposer: Quorum Reached, next proposal number: {}", MAX_PROPOSAL_NUMBER);
                        }
                    }
                }
            }
        }
    });    
    
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer)?;

    // Get the incoming message from the client
    let message = String::from_utf8_lossy(&buffer[..bytes_read]);

    if message.len() > 0 {
        let mut handles = vec![];

        // Connect to the servers
        for i in 1..=num_nodes {
            let tx_ref = tx.clone();
            handles.push(thread::spawn(move || {
                match TcpStream::connect(format!("127.0.0.1:{}", base_port+i)) {
                    Ok(server_stream) => {
                        // Connection succeeded, handle the stream here
                        println!("Proposer: Connected to Node with IP: {}", base_port + i);
                        communicate_with_nodes(server_stream, tx_ref, proposal_number, i).unwrap_or_else(|error| {
                            eprintln!("Proposer: Error handling node: {}, error {:?}", i, error);
                        });
                    },
                    Err(e) => {
                        match e.kind() {
                            std::io::ErrorKind::ConnectionRefused => {
                                // Connection refused handling
                                println!("Proposer: Connection refused: {}", e);
                            },
                            _ => {
                                // Handling for other errors
                                panic!("Proposer: Error: {}", e);
                            },
                        }
                    }
                }                
            }))
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    std::thread::sleep(std::time::Duration::from_millis(1));
    return respond_to_client(message, stream, hash_table);
}

fn spawn_nodes(base_port: u16, num_nodes: u16) {
    // Spawn the nodes as separate processes
    for i in 1..=num_nodes {
        let child = Command::new("target/release/node")
            .arg(format!("{}", i))
            .arg(format!("{}", base_port))
            .spawn()
            .unwrap();
        println!("Proposer: Spawned Node {} with PID {} on IP Address: {}", i, child.id(), format!("127.0.0.1:{}", base_port + i));
    }
}

fn start_proposer(base_port: u16, num_nodes: u16) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", base_port)).unwrap();
    println!("Proposer: Spawned proposer on IP Address: {}", format!("127.0.0.1:{}", base_port));
    let hash_table = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Proposer: Client connected");
                let hash_table_ref = hash_table.clone();
                // Spawn a new thread to handle the client's connection
                thread::spawn(move || {
                    handle_client(base_port, num_nodes, stream, get_max_proposal_number(), hash_table_ref).unwrap_or_else(|error| {
                        eprintln!("Proposer: Error handling client: {:?}", error);
                    });
                });
            }
            Err(error) => {
                eprintln!("Proposer: Error accepting connection: {:?}", error);
            }
        } 
    }
}

fn main() {
    // Get the command-line arguments
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        eprintln!("Proposer: Usage: {} <F> <port number>", args[0]);
        return;
    }
    
    let num_f: u16 = args[1].parse().unwrap();
    let base_port: u16 = args[2].parse().unwrap();

    // No. of processes = 2 * F + 1
    let num_nodes: u16 = 2*num_f + 1;

    // Spawn nodes and proposer, assigning them IPs ranging from the [base_port](for proposer) to [base_port + nodeIdx](for nodes)
    spawn_nodes(base_port, num_nodes);
    start_proposer(base_port, num_nodes);
}