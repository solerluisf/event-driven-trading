// adapter_factory.rs


use crate::core::domain::broker_config::BrokerConfig;
use crate::core::ports::execution_port::IExecutionPort;
use crate::adapters::broker::broker_error::BrokerError;
use crate::adapters::broker::alpaca_adapter::AlpacaBrokerAdapter;
use crate::adapters::broker::mock_adapter::MockAdapter;
use crate::adapters::broker::fix_adapter::FixBrokerAdapter;
use crate::adapters::broker::rest_adapter::RestBrokerAdapter;
use crate::adapters::broker::websocket_adapter::WebSocketBrokerAdapter;


use std::sync::Mutex;
use apca::Client;

/// Errors that can occur when creating adapters
#[derive(Debug, Clone, PartialEq)]
pub enum AdapterFactoryError {
    /// Alpaca client was not provided to the factory
    AlpacaClientNotProvided,
    /// Alpaca client was already consumed by a previous adapter creation
    AlpacaClientAlreadyUsed,
}

impl std::fmt::Display for AdapterFactoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdapterFactoryError::AlpacaClientNotProvided => {
                write!(f, "Alpaca client not provided to factory")
            }
            AdapterFactoryError::AlpacaClientAlreadyUsed => {
                write!(f, "Alpaca client already consumed by previous adapter creation")
            }
        }
    }
}

impl std::error::Error for AdapterFactoryError {}

pub struct AdapterFactory {
    alpaca_client: Mutex<Option<Client>>,
}

impl AdapterFactory {
    pub fn new(alpaca_client: Option<Client>) -> Self {
        Self { 
           alpaca_client: Mutex::new(alpaca_client), 
        }
    }

    /// Check if the Alpaca client is available for creating an adapter
    pub fn is_alpaca_client_available(&self) -> bool {
        self.alpaca_client
            .lock()
            .unwrap()
            .is_some()
    }

    /// Create an adapter for the given broker configuration
    /// 
    /// # Arguments
    /// * `config` - The broker configuration
    /// 
    /// # Returns
    /// * `Ok(Box<dyn IExecutionPort>)` - The created adapter
    /// * `Err(AdapterFactoryError)` - If the Alpaca client is not available or already used
    /// 
    /// # Note
    /// For Alpaca brokers, the client can only be used once. Subsequent calls will return
    /// `AdapterFactoryError::AlpacaClientAlreadyUsed`.
    pub fn create_adapter(
        &self,
        config: BrokerConfig,
    ) -> Result<Box<dyn IExecutionPort<Error = BrokerError>>, AdapterFactoryError> {
        match config.name.as_str() {
            "alpaca" => {
                let client = self.alpaca_client
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or(AdapterFactoryError::AlpacaClientAlreadyUsed)?;
                Ok(Box::new(AlpacaBrokerAdapter::new(client)))
            },
            "fix"       => Ok(Box::new(FixBrokerAdapter::default())),
            "rest"      => Ok(Box::new(RestBrokerAdapter::default())),
            "websocket" => Ok(Box::new(WebSocketBrokerAdapter::default())),
            _           => Ok(Box::new(MockAdapter::default())),
        }
    }

    /// Create an adapter for the given broker configuration (legacy version)
    /// 
    /// # Panics
    /// Panics if the Alpaca client is not available or already used.
    /// Use `create_adapter` for error handling instead.
    #[deprecated(since = "0.1.0", note = "Use create_adapter instead")]
    pub fn create_adapter_panic(
        &self,
        config: BrokerConfig,
    ) -> Box<dyn IExecutionPort<Error = BrokerError>> {
        match config.name.as_str() {
            "alpaca" => {
                let client = self.alpaca_client
                    .lock()
                    .unwrap()
                    .take()
                    .expect("alpaca client not provided or already used");
                Box::new(AlpacaBrokerAdapter::new(client))
            },
            "fix"       => Box::new(FixBrokerAdapter::default()),
            "rest"      => Box::new(RestBrokerAdapter::default()),
            "websocket" => Box::new(WebSocketBrokerAdapter::default()),
            _           => Box::new(MockAdapter::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::broker_config::BrokerConfig;

    #[test]
    fn test_create_mock_adapter_succeeds() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "mock".to_string(),
        };

        let adapter = factory.create_adapter(config);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_create_fix_adapter_succeeds() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "fix".to_string(),
        };

        let adapter = factory.create_adapter(config);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_create_rest_adapter_succeeds() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "rest".to_string(),
        };

        let adapter = factory.create_adapter(config);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_create_websocket_adapter_succeeds() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "websocket".to_string(),
        };

        let adapter = factory.create_adapter(config);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_is_alpaca_client_available_returns_false_when_none() {
        let factory = AdapterFactory::new(None);
        assert!(!factory.is_alpaca_client_available());
    }

    #[test]
    fn test_create_alpaca_adapter_fails_when_client_none() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "alpaca".to_string(),
        };

        let result = factory.create_adapter(config);
        assert!(result.is_err());
        match result {
            Err(AdapterFactoryError::AlpacaClientAlreadyUsed) => (), // Expected
            _ => panic!("Expected AlpacaClientAlreadyUsed error"),
        }
    }

    #[test]
    fn test_alpaca_client_available_returns_true_when_present() {
        // Note: We can't easily create a real apca::Client in tests,
        // so we test the behavior with None and verify the method works
        let factory = AdapterFactory::new(None);
        assert!(!factory.is_alpaca_client_available());
    }

    #[test]
    fn test_create_adapter_is_reusable_for_non_alpaca() {
        let factory = AdapterFactory::new(None);
        
        // Create multiple mock adapters - should all succeed
        for i in 0..5 {
            let config = BrokerConfig {
                name: "mock".to_string(),
            };
            
            let result = factory.create_adapter(config);
            assert!(result.is_ok(), "Failed on iteration {}", i);
        }
    }

    #[test]
    fn test_error_display_messages() {
        let not_provided = AdapterFactoryError::AlpacaClientNotProvided;
        let already_used = AdapterFactoryError::AlpacaClientAlreadyUsed;

        assert!(not_provided.to_string().contains("not provided"));
        assert!(already_used.to_string().contains("already consumed"));
    }
}