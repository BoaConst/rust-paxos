use std::io::{Read, Write};
use std::net::{TcpStream, TcpListener};
use std::env;
use std::thread;

mod operations;
use operations::Operations;

fn build_response(proposal_type: &str, proposal_number: &str) -> String {
    let mut proposal = String::new();
    proposal.push_str(proposal_type);
    proposal.push_str(" ");
    proposal.push_str(proposal_number);
    return proposal.to_string();
}

// The proposer is a client for the node
fn handle_client(mut stream: TcpStream, node_idx: u16) {
    // Receive a set of messages from proposer
    let mut buffer = [0; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                let message = String::from_utf8_lossy(&buffer[0..n]).trim().to_string();
                if message.len() > 1 {
                    // Send a set of messages to proposer
                    let chunks: Vec<&str> = message.split_whitespace().collect();
                    let operation = chunks[0];
                    let proposal_number = chunks[1];
                    println!("Server {}: Received operation: {} and proposal number: {}", node_idx, operation, proposal_number);

                    // Returning appropriate message(s) to the proposer
                    let response = match operation {
                        "prepare" => build_response(Operations::Promise.to_string().as_str(), proposal_number),
                        "accept-request" => build_response(Operations::Accepted.to_string().as_str(), proposal_number),
                        _ => "ERROR Invalid operation".to_string(),
                    };
                    stream.write_all(response.as_bytes()).unwrap();
                    println!("Server {}: message sent to proposer: {}", node_idx, response);
                }
            }
            Err(err) => panic!("Server {}: Error reading from stream: {}", node_idx, err),
        }
    }
    
}

fn main() {
    let args: Vec<_> = env::args().collect();

    // Node Identity inferred from the args supplied by the leader
    let node_idx: u16 = args[1].parse().unwrap();
    let base_port: u16 = args[2].parse().unwrap();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", base_port + node_idx)).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Server {} : Connected to Proposer", node_idx);
                thread::spawn(move || {
                    handle_client(stream, node_idx)
                });
            }
            Err(error) => {
                eprintln!("Server {} : Error accepting connection: {:?}", node_idx, error);
            }
        }
        
    }

    println!("Server {}: Finished", node_idx);
}
