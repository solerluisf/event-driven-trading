// adapters/broker/broker_error.rs

#[derive(Debug)]
pub enum BrokerError {
    Apca(apca::Error),
    InvalidOrderId(String),
    ConnectionFailed(String),
    Unauthorized,
    RateLimited,
    Unknown(String),
}

// lets you use ? operator when calling apca methods
impl From<apca::Error> for BrokerError {
    fn from(e: apca::Error) -> Self {
        BrokerError::Apca(e)
    }
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::Apca(e)                => write!(f, "Apca error: {}", e),
            BrokerError::InvalidOrderId(id)     => write!(f, "Invalid order id: {}", id),
            BrokerError::ConnectionFailed(msg)  => write!(f, "Connection failed: {}", msg),
            BrokerError::Unauthorized           => write!(f, "Unauthorized"),
            BrokerError::RateLimited            => write!(f, "Rate limited"),
            BrokerError::Unknown(msg)           => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BrokerError::Apca(e) => Some(e),
            _ => None,
        }
    }
}
