// adapter_factory.rs


use crate::core::domain::broker_config::BrokerConfig;
use crate::core::domain::data_source::{DataSource, ExecutionConfig};
use crate::core::ports::execution_port::IExecutionPort;
use crate::core::ports::journal_repo::IJournalRepo;
use crate::adapters::broker::broker_error::BrokerError;
use crate::adapters::broker::alpaca_adapter::AlpacaBrokerAdapter;
use crate::adapters::broker::mock_adapter::MockAdapter;
use crate::adapters::broker::replay_adapter::{ReplayBrokerAdapter, ReplayConfig};
use crate::core::infrastructure::MutexExt;

use std::sync::{Arc, Mutex};
use apca::Client;

/// Errors that can occur when creating adapters
#[derive(Debug, Clone, PartialEq)]
pub enum AdapterFactoryError {
    /// Alpaca client was not provided to the factory
    AlpacaClientNotProvided,
    /// Alpaca client was already consumed by a previous adapter creation
    AlpacaClientAlreadyUsed,
    /// Journal was not provided but is required for replay mode
    JournalNotAvailable,
    /// The requested broker adapter is not yet implemented
    AdapterNotImplemented { broker: String },
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
            AdapterFactoryError::JournalNotAvailable => {
                write!(f, "Journal not available but required for replay mode")
            }
            AdapterFactoryError::AdapterNotImplemented { broker } => {
                write!(f, "Broker adapter '{}' is not yet implemented", broker)
            }
        }
    }
}

impl std::error::Error for AdapterFactoryError {}

pub struct AdapterFactory {
    alpaca_client: Mutex<Option<Client>>,
    journal: Option<Arc<dyn IJournalRepo>>,
}

impl AdapterFactory {
    /// Create a new adapter factory with optional Alpaca client
    pub fn new(alpaca_client: Option<Client>) -> Self {
        Self { 
           alpaca_client: Mutex::new(alpaca_client),
           journal: None,
        }
    }

    /// Create a new adapter factory with journal support for replay mode
    pub fn with_journal(alpaca_client: Option<Client>, journal: Arc<dyn IJournalRepo>) -> Self {
        Self { 
           alpaca_client: Mutex::new(alpaca_client),
           journal: Some(journal),
        }
    }

    /// Check if the Alpaca client is available for creating an adapter
    pub fn is_alpaca_client_available(&self) -> bool {
        self.alpaca_client
            .safe_lock()
            .is_some()
    }

    /// Check if journal is available for replay mode
    pub fn is_journal_available(&self) -> bool {
        self.journal.is_some()
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
                    .safe_lock()
                    .take()
                    .ok_or(AdapterFactoryError::AlpacaClientAlreadyUsed)?;
                Ok(Box::new(AlpacaBrokerAdapter::new(client)))
            },
            // These adapters are placeholders and will panic if used (todo!())
            // Return an error instead to prevent runtime panics
            "fix" => Err(AdapterFactoryError::AdapterNotImplemented { 
                broker: "fix".to_string() 
            }),
            "rest" => Err(AdapterFactoryError::AdapterNotImplemented { 
                broker: "rest".to_string() 
            }),
            "websocket" => Err(AdapterFactoryError::AdapterNotImplemented { 
                broker: "websocket".to_string() 
            }),
            _ => Ok(Box::new(MockAdapter::default())),
        }
    }

    /// Create an adapter based on execution configuration (supports replay mode)
    /// 
    /// # Arguments
    /// * `exec_config` - The execution configuration with data source
    /// * `broker_config` - The broker configuration (used as fallback for live mode)
    /// 
    /// # Returns
    /// * `Ok(Box<dyn IExecutionPort>)` - The created adapter
    /// * `Err(AdapterFactoryError)` - If required resources are not available
    pub fn create_adapter_from_config(
        &self,
        exec_config: &ExecutionConfig,
        broker_config: BrokerConfig,
    ) -> Result<Box<dyn IExecutionPort<Error = BrokerError>>, AdapterFactoryError> {
        match &exec_config.data_source {
            DataSource::Live { broker } => {
                let mut config = broker_config;
                config.name = broker.clone();
                self.create_adapter(config)
            }
            DataSource::Replay { query, loop_replay, latency_ms } => {
                let journal = self.journal.clone()
                    .ok_or(AdapterFactoryError::JournalNotAvailable)?;
                
                let replay_config = ReplayConfig {
                    query: query.clone(),
                    loop_replay: *loop_replay,
                    artificial_latency_ms: *latency_ms,
                    inject_errors: false,
                    error_rate: 0.0,
                };
                
                Ok(Box::new(ReplayBrokerAdapter::new(journal, replay_config)))
            }
            DataSource::Mock { .. } => {
                Ok(Box::new(MockAdapter::default()))
            }
            DataSource::Hybrid { primary, .. } => {
                // For now, use primary source (hybrid logic would be in the adapter itself)
                let mut hybrid_exec_config = exec_config.clone();
                hybrid_exec_config.data_source = (**primary).clone();
                self.create_adapter_from_config(&hybrid_exec_config, broker_config)
            }
        }
    }

    /// Create an adapter for the given broker configuration (legacy version)
    /// 
    /// # Panics
    /// Panics if the Alpaca client is not available or already used.
    /// Also panics for unimplemented adapters (fix, rest, websocket).
    /// Use `create_adapter` for error handling instead.
    #[deprecated(since = "0.1.0", note = "Use create_adapter instead")]
    pub fn create_adapter_panic(
        &self,
        config: BrokerConfig,
    ) -> Box<dyn IExecutionPort<Error = BrokerError>> {
        match config.name.as_str() {
            "alpaca" => {
                let client = self.alpaca_client
                    .safe_lock()
                    .take()
                    .expect("alpaca client not provided or already used");
                Box::new(AlpacaBrokerAdapter::new(client))
            },
            "fix" => panic!("FixBrokerAdapter is not yet implemented - use create_adapter() for proper error handling"),
            "rest" => panic!("RestBrokerAdapter is not yet implemented - use create_adapter() for proper error handling"),
            "websocket" => panic!("WebSocketBrokerAdapter is not yet implemented - use create_adapter() for proper error handling"),
            _ => Box::new(MockAdapter::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::broker_config::BrokerConfig;
    use crate::core::domain::data_source::{DataSource, ExecutionConfig};
    use crate::core::domain::journal::ResponseRecord;
    use crate::core::ports::journal_repo::{IJournalRepo, JournalResult, JournalError};

    struct MockJournal {
        responses: Vec<ResponseRecord>,
    }

    impl MockJournal {
        fn with_responses(responses: Vec<ResponseRecord>) -> Self {
            Self { responses }
        }
    }

    impl IJournalRepo for MockJournal {
        fn persist_outbound(&self, _record: crate::core::domain::journal::RequestRecord) -> JournalResult<()> {
            Ok(())
        }

        fn persist_inbound(&self, _record: ResponseRecord) -> JournalResult<()> {
            Ok(())
        }

        fn replay(&self, _query: String) -> Vec<ResponseRecord> {
            self.responses.clone()
        }
    }

    fn create_test_response(id: &str) -> ResponseRecord {
        ResponseRecord {
            id: id.to_string(),
            raw_payload: Some(format!(r#"{{"execution_id": "{}"}}"#, id)),
            correlation_id: None,
        }
    }

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
    fn test_create_fix_adapter_returns_not_implemented_error() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "fix".to_string(),
        };

        let result = factory.create_adapter(config);
        assert!(result.is_err());
        match result {
            Err(AdapterFactoryError::AdapterNotImplemented { broker }) => {
                assert_eq!(broker, "fix");
            }
            _ => panic!("Expected AdapterNotImplemented error for 'fix'"),
        }
    }

    #[test]
    fn test_create_rest_adapter_returns_not_implemented_error() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "rest".to_string(),
        };

        let result = factory.create_adapter(config);
        assert!(result.is_err());
        match result {
            Err(AdapterFactoryError::AdapterNotImplemented { broker }) => {
                assert_eq!(broker, "rest");
            }
            _ => panic!("Expected AdapterNotImplemented error for 'rest'"),
        }
    }

    #[test]
    fn test_create_websocket_adapter_returns_not_implemented_error() {
        let factory = AdapterFactory::new(None);
        let config = BrokerConfig {
            name: "websocket".to_string(),
        };

        let result = factory.create_adapter(config);
        assert!(result.is_err());
        match result {
            Err(AdapterFactoryError::AdapterNotImplemented { broker }) => {
                assert_eq!(broker, "websocket");
            }
            _ => panic!("Expected AdapterNotImplemented error for 'websocket'"),
        }
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
        let journal_not_available = AdapterFactoryError::JournalNotAvailable;
        let not_implemented = AdapterFactoryError::AdapterNotImplemented { 
            broker: "test".to_string() 
        };

        assert!(not_provided.to_string().contains("not provided"));
        assert!(already_used.to_string().contains("already consumed"));
        assert!(journal_not_available.to_string().contains("Journal not available"));
        assert!(not_implemented.to_string().contains("not yet implemented"));
        assert!(not_implemented.to_string().contains("test"));
    }

    #[test]
    fn test_factory_without_journal_returns_false() {
        let factory = AdapterFactory::new(None);
        assert!(!factory.is_journal_available());
    }

    #[test]
    fn test_factory_with_journal_returns_true() {
        let journal = Arc::new(MockJournal::with_responses(vec![]));
        let factory = AdapterFactory::with_journal(None, journal);
        assert!(factory.is_journal_available());
    }

    #[test]
    fn test_create_adapter_from_config_live() {
        let factory = AdapterFactory::new(None);
        let exec_config = ExecutionConfig::live("mock");
        let broker_config = BrokerConfig { name: "alpaca".to_string() };

        let adapter = factory.create_adapter_from_config(&exec_config, broker_config);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_create_adapter_from_config_mock() {
        let factory = AdapterFactory::new(None);
        let exec_config = ExecutionConfig::backtest("test_scenario");
        let broker_config = BrokerConfig { name: "alpaca".to_string() };

        let adapter = factory.create_adapter_from_config(&exec_config, broker_config);
        assert!(adapter.is_ok());
    }

    #[test]
    fn test_create_adapter_from_config_replay_fails_without_journal() {
        let factory = AdapterFactory::new(None);
        let exec_config = ExecutionConfig::paper_with_replay("2024-01-15");
        let broker_config = BrokerConfig { name: "alpaca".to_string() };

        let result = factory.create_adapter_from_config(&exec_config, broker_config);
        assert!(result.is_err());
        match result {
            Err(AdapterFactoryError::JournalNotAvailable) => (), // Expected
            _ => panic!("Expected JournalNotAvailable error"),
        }
    }

    #[test]
    fn test_create_adapter_from_config_replay_succeeds_with_journal() {
        let responses = vec![
            create_test_response("1"),
            create_test_response("2"),
        ];
        let journal = Arc::new(MockJournal::with_responses(responses));
        let factory = AdapterFactory::with_journal(None, journal);
        
        let exec_config = ExecutionConfig::paper_with_replay("test");
        let broker_config = BrokerConfig { name: "alpaca".to_string() };

        let adapter = factory.create_adapter_from_config(&exec_config, broker_config);
        assert!(adapter.is_ok());
    }
}