#[allow(dead_code)]
pub enum Operations {
    Prepare,
    Promise,
    AcceptRequest,
    Accepted,
}

impl std::string::ToString for Operations {
    fn to_string(&self) -> String {
        match self {
            Operations::Prepare => "prepare".to_string(),
            Operations::Promise => "promise".to_string(),
            Operations::AcceptRequest => "accept-request".to_string(),
            Operations::Accepted => "accepted".to_string(),
        }
    }
}
