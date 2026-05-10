// core/domain/data_source.rs
//
// Defines data sources for broker operations - live API vs replay from journal

use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for the data source used by broker adapters
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataSource {
    /// Live broker API calls
    Live {
        /// Broker identifier (alpaca, fix, etc.)
        broker: String,
    },
    /// Replay historical responses from journal
    Replay {
        /// Query to filter journal records
        query: String,
        /// Whether to loop through responses
        loop_replay: bool,
        /// Artificial latency in milliseconds
        latency_ms: u64,
    },
    /// Synthetic/mock responses for testing
    Mock {
        /// Scenario identifier for deterministic behavior
        scenario: String,
    },
    /// Hybrid mode - live with fallback to replay on errors
    Hybrid {
        /// Primary data source (usually Live)
        primary: Box<DataSource>,
        /// Fallback data source (usually Replay or Mock)
        fallback: Box<DataSource>,
        /// Error threshold before switching to fallback
        error_threshold: u32,
    },
}

impl DataSource {
    /// Create a live data source for the given broker
    pub fn live(broker: impl Into<String>) -> Self {
        Self::Live {
            broker: broker.into(),
        }
    }

    /// Create a replay data source with the given query
    pub fn replay(query: impl Into<String>) -> Self {
        Self::Replay {
            query: query.into(),
            loop_replay: false,
            latency_ms: 0,
        }
    }

    /// Create a replay data source that loops through responses
    pub fn replay_looping(query: impl Into<String>) -> Self {
        Self::Replay {
            query: query.into(),
            loop_replay: true,
            latency_ms: 0,
        }
    }

    /// Create a mock data source
    pub fn mock(scenario: impl Into<String>) -> Self {
        Self::Mock {
            scenario: scenario.into(),
        }
    }

    /// Check if this is a live data source
    pub fn is_live(&self) -> bool {
        matches!(self, DataSource::Live { .. })
    }

    /// Check if this is a replay data source
    pub fn is_replay(&self) -> bool {
        matches!(self, DataSource::Replay { .. })
    }

    /// Check if this is a mock data source
    pub fn is_mock(&self) -> bool {
        matches!(self, DataSource::Mock { .. })
    }

    /// Check if this is a hybrid data source
    pub fn is_hybrid(&self) -> bool {
        matches!(self, DataSource::Hybrid { .. })
    }

    /// Returns true if this data source requires network connectivity
    pub fn requires_connectivity(&self) -> bool {
        match self {
            DataSource::Live { .. } => true,
            DataSource::Replay { .. } => false,
            DataSource::Mock { .. } => false,
            DataSource::Hybrid { primary, .. } => primary.requires_connectivity(),
        }
    }

    /// Returns true if this data source uses the journal
    pub fn uses_journal(&self) -> bool {
        match self {
            DataSource::Live { .. } => false,
            DataSource::Replay { .. } => true,
            DataSource::Mock { .. } => false,
            DataSource::Hybrid { primary, fallback, .. } => {
                primary.uses_journal() || fallback.uses_journal()
            }
        }
    }

    /// Add artificial latency to a replay source
    pub fn with_latency(self, ms: u64) -> Self {
        match self {
            DataSource::Replay { query, loop_replay, .. } => DataSource::Replay {
                query,
                loop_replay,
                latency_ms: ms,
            },
            _ => self, // No-op for non-replay sources
        }
    }

    /// Get the broker identifier if this is a live source
    pub fn broker_id(&self) -> Option<&str> {
        match self {
            DataSource::Live { broker } => Some(broker),
            _ => None,
        }
    }

    /// Get the replay query if this is a replay source
    pub fn replay_query(&self) -> Option<&str> {
        match self {
            DataSource::Replay { query, .. } => Some(query),
            _ => None,
        }
    }
}

impl Default for DataSource {
    fn default() -> Self {
        DataSource::mock("default")
    }
}

impl fmt::Display for DataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataSource::Live { broker } => write!(f, "live:{}", broker),
            DataSource::Replay { query, loop_replay, latency_ms } => {
                write!(f, "replay:{}", query)?;
                if *loop_replay {
                    write!(f, ":loop")?;
                }
                if *latency_ms > 0 {
                    write!(f, ":{}ms", latency_ms)?;
                }
                Ok(())
            }
            DataSource::Mock { scenario } => write!(f, "mock:{}", scenario),
            DataSource::Hybrid { primary, fallback, error_threshold } => {
                write!(f, "hybrid:{}:{}:threshold={}", primary, fallback, error_threshold)
            }
        }
    }
}

/// Configuration that combines operation mode with data source
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// The data source to use
    pub data_source: DataSource,
    /// Whether to enable detailed logging of all operations
    pub verbose_logging: bool,
    /// Timeout for operations in milliseconds
    pub timeout_ms: u64,
}

impl ExecutionConfig {
    /// Create a live trading configuration
    pub fn live(broker: impl Into<String>) -> Self {
        Self {
            data_source: DataSource::live(broker),
            verbose_logging: false,
            timeout_ms: 30000, // 30 seconds
        }
    }

    /// Create a paper trading configuration with replay
    pub fn paper_with_replay(query: impl Into<String>) -> Self {
        Self {
            data_source: DataSource::replay(query),
            verbose_logging: true,
            timeout_ms: 60000, // 60 seconds for replay
        }
    }

    /// Create a backtesting configuration
    pub fn backtest(scenario: impl Into<String>) -> Self {
        Self {
            data_source: DataSource::mock(scenario),
            verbose_logging: true,
            timeout_ms: 1000, // Fast timeout for backtesting
        }
    }

    /// Enable verbose logging
    pub fn with_verbose_logging(mut self) -> Self {
        self.verbose_logging = true;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Check if this config uses live broker connectivity
    pub fn is_live(&self) -> bool {
        self.data_source.is_live()
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            data_source: DataSource::default(),
            verbose_logging: false,
            timeout_ms: 30000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_data_source() {
        let ds = DataSource::live("alpaca");
        assert!(ds.is_live());
        assert!(!ds.is_replay());
        assert!(!ds.is_mock());
        assert!(ds.requires_connectivity());
        assert!(!ds.uses_journal());
        assert_eq!(ds.broker_id(), Some("alpaca"));
        assert_eq!(ds.to_string(), "live:alpaca");
    }

    #[test]
    fn test_replay_data_source() {
        let ds = DataSource::replay("2024-01-15");
        assert!(!ds.is_live());
        assert!(ds.is_replay());
        assert!(!ds.is_mock());
        assert!(!ds.requires_connectivity());
        assert!(ds.uses_journal());
        assert_eq!(ds.replay_query(), Some("2024-01-15"));
        assert_eq!(ds.to_string(), "replay:2024-01-15");
    }

    #[test]
    fn test_mock_data_source() {
        let ds = DataSource::mock("scenario_1");
        assert!(!ds.is_live());
        assert!(!ds.is_replay());
        assert!(ds.is_mock());
        assert!(!ds.requires_connectivity());
        assert!(!ds.uses_journal());
        assert_eq!(ds.to_string(), "mock:scenario_1");
    }

    #[test]
    fn test_replay_looping() {
        let ds = DataSource::replay_looping("AAPL");
        match ds {
            DataSource::Replay { query, loop_replay, .. } => {
                assert_eq!(query, "AAPL");
                assert!(loop_replay);
            }
            _ => panic!("Expected Replay data source"),
        }
    }

    #[test]
    fn test_replay_with_latency() {
        let ds = DataSource::replay("test").with_latency(100);
        match ds {
            DataSource::Replay { latency_ms, .. } => {
                assert_eq!(latency_ms, 100);
            }
            _ => panic!("Expected Replay data source"),
        }
    }

    #[test]
    fn test_hybrid_data_source() {
        let primary = Box::new(DataSource::live("alpaca"));
        let fallback = Box::new(DataSource::mock("fallback"));
        let ds = DataSource::Hybrid {
            primary,
            fallback,
            error_threshold: 3,
        };
        
        assert!(ds.is_hybrid());
        assert!(ds.requires_connectivity()); // Primary is live
        assert!(!ds.uses_journal()); // Neither uses journal
    }

    #[test]
    fn test_execution_config_live() {
        let config = ExecutionConfig::live("alpaca");
        assert!(config.is_live());
        assert!(!config.verbose_logging);
        assert_eq!(config.timeout_ms, 30000);
    }

    #[test]
    fn test_execution_config_paper() {
        let config = ExecutionConfig::paper_with_replay("2024-01-15");
        assert!(!config.is_live());
        assert!(config.data_source.is_replay());
        assert!(config.verbose_logging);
    }

    #[test]
    fn test_execution_config_backtest() {
        let config = ExecutionConfig::backtest("bull_market");
        assert!(config.data_source.is_mock());
        assert!(config.verbose_logging);
    }

    #[test]
    fn test_execution_config_builder() {
        let config = ExecutionConfig::live("alpaca")
            .with_verbose_logging()
            .with_timeout(5000);
        
        assert!(config.verbose_logging);
        assert_eq!(config.timeout_ms, 5000);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let ds = DataSource::replay("AAPL").with_latency(50);
        let json = serde_json::to_string(&ds).unwrap();
        let deserialized: DataSource = serde_json::from_str(&json).unwrap();
        assert_eq!(ds, deserialized);

        let config = ExecutionConfig::paper_with_replay("test");
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ExecutionConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_default_data_source_is_mock() {
        let ds: DataSource = Default::default();
        assert!(ds.is_mock());
    }
}
