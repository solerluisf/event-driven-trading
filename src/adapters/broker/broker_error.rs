// adapters/broker/broker_error.rs

use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum BrokerError {
    Apca(Arc<apca::Error>),
    InvalidOrderId(String),
    ConnectionFailed(String),
    Unauthorized,
    RateLimited,
    ConfigError(String),
    Unknown(String),
}

// lets you use ? operator when calling apca methods
impl From<apca::Error> for BrokerError {
    fn from(e: apca::Error) -> Self {
        BrokerError::Apca(Arc::new(e))
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
            BrokerError::ConfigError(msg)       => write!(f, "Configuration error: {}", msg),
            BrokerError::Unknown(msg)           => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for BrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BrokerError::Apca(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    fn create_test_apca_error() -> apca::Error {
        // Create a simple apca error using the From trait
        // apca::Error implements From<serde_json::Error> and other types
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "test error");
        let json_err = serde_json::Error::io(io_err);
        apca::Error::from(json_err)
    }

    #[test]
    fn broker_error_clone_apca() {
        // Create an apca error and wrap it
        let apca_err = create_test_apca_error();
        let error = BrokerError::from(apca_err);
        
        // Clone should work
        let cloned = error.clone();
        
        // Both should display the same
        assert_eq!(format!("{}", error), format!("{}", cloned));
        assert!(matches!(cloned, BrokerError::Apca(_)));
    }

    #[test]
    fn broker_error_clone_invalid_order_id() {
        let error = BrokerError::InvalidOrderId("order123".to_string());
        let cloned = error.clone();
        
        assert!(matches!(cloned, BrokerError::InvalidOrderId(ref id) if id == "order123"));
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn broker_error_clone_connection_failed() {
        let error = BrokerError::ConnectionFailed("timeout".to_string());
        let cloned = error.clone();
        
        assert!(matches!(cloned, BrokerError::ConnectionFailed(ref msg) if msg == "timeout"));
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn broker_error_clone_unauthorized() {
        let error = BrokerError::Unauthorized;
        let cloned = error.clone();
        
        assert!(matches!(cloned, BrokerError::Unauthorized));
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn broker_error_clone_rate_limited() {
        let error = BrokerError::RateLimited;
        let cloned = error.clone();
        
        assert!(matches!(cloned, BrokerError::RateLimited));
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn broker_error_clone_config_error() {
        let error = BrokerError::ConfigError("missing api key".to_string());
        let cloned = error.clone();
        
        assert!(matches!(cloned, BrokerError::ConfigError(ref msg) if msg == "missing api key"));
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn broker_error_clone_unknown() {
        let error = BrokerError::Unknown("something went wrong".to_string());
        let cloned = error.clone();
        
        assert!(matches!(cloned, BrokerError::Unknown(ref msg) if msg == "something went wrong"));
        assert_eq!(format!("{}", error), format!("{}", cloned));
    }

    #[test]
    fn broker_error_multiple_clones() {
        let error = BrokerError::Unknown("test".to_string());
        let cloned1 = error.clone();
        let cloned2 = cloned1.clone();
        let cloned3 = error.clone();
        
        // All clones should be equal in value
        assert!(matches!(cloned1, BrokerError::Unknown(ref msg) if msg == "test"));
        assert!(matches!(cloned2, BrokerError::Unknown(ref msg) if msg == "test"));
        assert!(matches!(cloned3, BrokerError::Unknown(ref msg) if msg == "test"));
    }

    #[test]
    fn broker_error_clone_preserves_source() {
        let apca_err = create_test_apca_error();
        let error = BrokerError::from(apca_err);
        let cloned = error.clone();
        
        // Both should have the same source
        assert!(error.source().is_some());
        assert!(cloned.source().is_some());
    }

    #[test]
    fn broker_error_from_apca_error() {
        let apca_err = create_test_apca_error();
        let error: BrokerError = apca_err.into();
        
        assert!(matches!(error, BrokerError::Apca(_)));
    }

    #[test]
    fn broker_error_display_formatting() {
        assert_eq!(
            format!("{}", BrokerError::Unauthorized),
            "Unauthorized"
        );
        assert_eq!(
            format!("{}", BrokerError::RateLimited),
            "Rate limited"
        );
        assert_eq!(
            format!("{}", BrokerError::InvalidOrderId("abc".to_string())),
            "Invalid order id: abc"
        );
        assert_eq!(
            format!("{}", BrokerError::ConnectionFailed("timeout".to_string())),
            "Connection failed: timeout"
        );
        assert_eq!(
            format!("{}", BrokerError::ConfigError("bad config".to_string())),
            "Configuration error: bad config"
        );
        assert_eq!(
            format!("{}", BrokerError::Unknown("oops".to_string())),
            "Unknown error: oops"
        );
    }
}
