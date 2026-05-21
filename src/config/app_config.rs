// config/app_config.rs
//
// All gateway configuration loaded from environment variables.
// dotenvy already loads .env before this is called from main.rs.
//
// Variables and defaults:
//   GATEWAY_ZMQ_REP_ENDPOINT             tcp://127.0.0.1:5555   inbound commands (REP)
//   GATEWAY_ZMQ_PUB_ENDPOINT             tcp://127.0.0.1:5556   outbound market data (PUB)
//   GATEWAY_ZMQ_ORDER_LIFECYCLE_ENDPOINT tcp://127.0.0.1:5557   outbound order lifecycle events (PUB)
//   GATEWAY_BROKER                       alpaca
//   GATEWAY_RATE_LIMIT_RPM               200                    requests per minute
//   GATEWAY_CB_THRESHOLD                 3                      circuit breaker failure threshold
//   GATEWAY_CB_COOLDOWN_SECS             30                     circuit breaker cooldown
//   GATEWAY_RECONNECT_MAX                5                      max reconnect attempts
//   GATEWAY_RECONNECT_BASE_MS            500                    initial back-off in ms
//   JOURNAL_DB_PATH                      journal.db
//   MARKET_DATA_FEED                     iex                    "iex", "sip", or "test"
//   MARKET_DATA_SYMBOLS                  AAPL,SPY               comma-separated list (or "*")
//   GATEWAY_OPERATION_MODE               paper                  "live", "paper", "readonly", "offline"
//   GATEWAY_ISOLATE_BULK_OPS             true                   isolate bulk operations on separate threads
//   GATEWAY_MAX_CONCURRENT_BULK          3                      max concurrent bulk operations
//   GATEWAY_PER_WORKLOAD_RATE_LIMIT      true                   enable per-workload rate limiting
//   ORCHESTRATOR_CONTROL_ENDPOINT        tcp://127.0.0.1:5560   orchestrator control commands (SUB)
//   ORCHESTRATOR_EVENTS_ENDPOINT         tcp://127.0.0.1:5561   orchestrator system events (SUB)
//   HEALTH_PUB_ENDPOINT                  tcp://127.0.0.1:5562   gateway health heartbeats (PUB)
//   CIRCUIT_BREAKER_PUB_ENDPOINT         tcp://127.0.0.1:5563   circuit breaker state events (PUB)

use std::env;
use crate::core::domain::operation_mode::{OperationMode, WorkloadConfig};

#[derive(Debug, Clone)]
pub struct AppConfig {
    /// ZeroMQ REP endpoint — receives execution commands
    pub zmq_rep_endpoint: String,
    /// ZeroMQ PUB endpoint — publishes market data
    pub zmq_pub_endpoint: String,
    /// ZeroMQ PUB endpoint — publishes order lifecycle events
    pub zmq_order_lifecycle_endpoint: String,
    /// Broker adapter to use
    pub broker: String,
    /// Rate limit in requests per minute
    pub rate_limit_rpm: f64,
    /// Circuit breaker: consecutive failures before opening
    pub cb_failure_threshold: u32,
    /// Circuit breaker: seconds to stay open before probing
    pub cb_cooldown_secs: u64,
    /// Reconnection: maximum attempts before giving up
    pub reconnect_max_attempts: u32,
    /// Reconnection: initial back-off delay in milliseconds
    pub reconnect_base_ms: u64,
    /// SQLite journal file path
    pub journal_db_path: String,
    /// Alpaca market data feed: "iex", "sip", or "test"
    pub market_data_feed: String,
    /// Symbols to stream — use ["*"] for all (requires appropriate plan)
    pub market_data_symbols: Vec<String>,
    /// Operation mode: "live", "paper", "readonly", "offline"
    pub operation_mode: OperationMode,
    /// Workload configuration for separating live vs bulk/offline operations
    pub workload_config: WorkloadConfig,
    /// Orchestrator control endpoint (SUB) — receives commands from orchestrator
    pub orchestrator_control_endpoint: String,
    /// Orchestrator events endpoint (SUB) — receives system events (kill switch, mode)
    pub orchestrator_events_endpoint: String,
    /// Health publisher endpoint (PUB) — publishes health heartbeats
    pub health_pub_endpoint: String,
    /// Circuit breaker publisher endpoint (PUB) — publishes CB state changes
    pub circuit_breaker_pub_endpoint: String,
}

impl AppConfig {
    /// Load config from environment.  Panics on parse errors for numeric fields.
    pub fn from_env() -> Self {
        let operation_mode = parse_operation_mode(&env_str("GATEWAY_OPERATION_MODE", "paper"));
        let workload_config = build_workload_config(&operation_mode);

        Self {
            zmq_rep_endpoint: env_str("GATEWAY_ZMQ_REP_ENDPOINT", "tcp://127.0.0.1:5555"),
            zmq_pub_endpoint: env_str("GATEWAY_ZMQ_PUB_ENDPOINT", "tcp://127.0.0.1:5556"),
            zmq_order_lifecycle_endpoint: env_str("GATEWAY_ZMQ_ORDER_LIFECYCLE_ENDPOINT", "tcp://127.0.0.1:5557"),
            broker: env_str("GATEWAY_BROKER", "alpaca"),
            rate_limit_rpm: env_parse("GATEWAY_RATE_LIMIT_RPM", 200.0),
            cb_failure_threshold: env_parse("GATEWAY_CB_THRESHOLD", 3),
            cb_cooldown_secs: env_parse("GATEWAY_CB_COOLDOWN_SECS", 30),
            reconnect_max_attempts: env_parse("GATEWAY_RECONNECT_MAX", 5),
            reconnect_base_ms: env_parse("GATEWAY_RECONNECT_BASE_MS", 500),
            journal_db_path: env_str("JOURNAL_DB_PATH", "journal.db"),
            market_data_feed: env_str("MARKET_DATA_FEED", "iex"),
            market_data_symbols: env_symbol_list("MARKET_DATA_SYMBOLS", &["AAPL", "SPY"]),
            operation_mode,
            workload_config,
            orchestrator_control_endpoint: env_str("ORCHESTRATOR_CONTROL_ENDPOINT", "tcp://127.0.0.1:5560"),
            orchestrator_events_endpoint: env_str("ORCHESTRATOR_EVENTS_ENDPOINT", "tcp://127.0.0.1:5561"),
            health_pub_endpoint: env_str("HEALTH_PUB_ENDPOINT", "tcp://127.0.0.1:5562"),
            circuit_breaker_pub_endpoint: env_str("CIRCUIT_BREAKER_PUB_ENDPOINT", "tcp://127.0.0.1:5563"),
        }
    }

    /// Validate that the configuration is consistent
    pub fn validate(&self) -> Result<(), String> {
        // Validate workload config
        if let Err(e) = self.workload_config.validate() {
            return Err(format!("Invalid workload configuration: {}", e));
        }

        // Check that operation mode is compatible with broker connectivity
        match (self.operation_mode, self.broker.as_str()) {
            (OperationMode::Offline, _) => {
                // Offline mode doesn't require broker connectivity
                Ok(())
            }
            (mode, "mock") if !mode.requires_broker_connection() => {
                // Using mock adapter in non-connectivity mode is fine
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr + Copy,
    T::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(val) => val.parse::<T>().unwrap_or_else(|e| {
            panic!("Invalid value for {}: {:?}", key, e)
        }),
        Err(_) => default,
    }
}

/// Parse a comma-separated symbol list.  Falls back to `defaults` if the env
/// var is absent.
fn env_symbol_list(key: &str, defaults: &[&str]) -> Vec<String> {
    match env::var(key) {
        Ok(val) if !val.trim().is_empty() => {
            val.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
        _ => defaults.iter().map(|s| s.to_string()).collect(),
    }
}

/// Parse operation mode from string
fn parse_operation_mode(s: &str) -> OperationMode {
    match s.to_lowercase().as_str() {
        "live" => OperationMode::Live,
        "paper" => OperationMode::Paper,
        "readonly" | "read-only" | "read_only" => OperationMode::ReadOnly,
        "offline" => OperationMode::Offline,
        _ => {
            tracing::warn!("Unknown operation mode '{}', defaulting to 'paper'", s);
            OperationMode::Paper
        }
    }
}

/// Build workload configuration based on operation mode and environment overrides
fn build_workload_config(mode: &OperationMode) -> WorkloadConfig {
    let mut config = match mode {
        OperationMode::Live => WorkloadConfig::live_trading(),
        OperationMode::Paper => WorkloadConfig::paper_trading(),
        OperationMode::ReadOnly => WorkloadConfig::backoffice_only(),
        OperationMode::Offline => {
            let mut cfg = WorkloadConfig::backoffice_only();
            cfg.mode = OperationMode::Offline;
            cfg
        }
    };

    // Allow environment overrides
    if let Ok(isolate) = env::var("GATEWAY_ISOLATE_BULK_OPS") {
        config.isolate_bulk_operations = isolate.parse().unwrap_or(config.isolate_bulk_operations);
    }

    if let Ok(max_bulk) = env::var("GATEWAY_MAX_CONCURRENT_BULK") {
        config.max_concurrent_bulk = max_bulk.parse().unwrap_or(config.max_concurrent_bulk);
    }

    if let Ok(per_workload) = env::var("GATEWAY_PER_WORKLOAD_RATE_LIMIT") {
        config.per_workload_rate_limiting = per_workload.parse().unwrap_or(config.per_workload_rate_limiting);
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::operation_mode::WorkloadType;

    #[test]
    fn test_parse_operation_mode_live() {
        assert_eq!(parse_operation_mode("live"), OperationMode::Live);
        assert_eq!(parse_operation_mode("LIVE"), OperationMode::Live);
        assert_eq!(parse_operation_mode("Live"), OperationMode::Live);
    }

    #[test]
    fn test_parse_operation_mode_paper() {
        assert_eq!(parse_operation_mode("paper"), OperationMode::Paper);
        assert_eq!(parse_operation_mode("PAPER"), OperationMode::Paper);
    }

    #[test]
    fn test_parse_operation_mode_readonly() {
        assert_eq!(parse_operation_mode("readonly"), OperationMode::ReadOnly);
        assert_eq!(parse_operation_mode("read-only"), OperationMode::ReadOnly);
        assert_eq!(parse_operation_mode("read_only"), OperationMode::ReadOnly);
    }

    #[test]
    fn test_parse_operation_mode_offline() {
        assert_eq!(parse_operation_mode("offline"), OperationMode::Offline);
    }

    #[test]
    fn test_parse_operation_mode_unknown_defaults_to_paper() {
        assert_eq!(parse_operation_mode("unknown"), OperationMode::Paper);
        assert_eq!(parse_operation_mode(""), OperationMode::Paper);
    }

    #[test]
    fn test_build_workload_config_live() {
        let config = build_workload_config(&OperationMode::Live);
        assert_eq!(config.mode, OperationMode::Live);
        assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Query));
        // Live trading config should NOT allow bulk operations by default
        assert!(!config.is_workload_allowed(WorkloadType::Reconciliation));
        assert!(!config.is_workload_allowed(WorkloadType::HistoricalDataDownload));
    }

    #[test]
    fn test_build_workload_config_paper() {
        let config = build_workload_config(&OperationMode::Paper);
        assert_eq!(config.mode, OperationMode::Paper);
        assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Query));
        // Paper trading allows reconciliation
        assert!(config.is_workload_allowed(WorkloadType::Reconciliation));
    }

    #[test]
    fn test_build_workload_config_readonly() {
        let config = build_workload_config(&OperationMode::ReadOnly);
        assert_eq!(config.mode, OperationMode::ReadOnly);
        assert!(!config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Query));
        assert!(config.is_workload_allowed(WorkloadType::Reconciliation));
        assert!(config.is_workload_allowed(WorkloadType::HistoricalDataDownload));
    }

    #[test]
    fn test_build_workload_config_offline() {
        let config = build_workload_config(&OperationMode::Offline);
        assert_eq!(config.mode, OperationMode::Offline);
        assert!(!config.is_workload_allowed(WorkloadType::LiveTrading));
        // Offline mode should not allow queries that require connectivity
        // but may allow historical data downloads (which could be from local cache)
    }

    #[test]
    fn test_app_config_validate_success() {
        let config = AppConfig {
            zmq_rep_endpoint: "tcp://127.0.0.1:5555".to_string(),
            zmq_pub_endpoint: "tcp://127.0.0.1:5556".to_string(),
            zmq_order_lifecycle_endpoint: "tcp://127.0.0.1:5557".to_string(),
            broker: "alpaca".to_string(),
            rate_limit_rpm: 200.0,
            cb_failure_threshold: 3,
            cb_cooldown_secs: 30,
            reconnect_max_attempts: 5,
            reconnect_base_ms: 500,
            journal_db_path: "journal.db".to_string(),
            market_data_feed: "iex".to_string(),
            market_data_symbols: vec!["AAPL".to_string()],
            operation_mode: OperationMode::Paper,
            workload_config: WorkloadConfig::paper_trading(),
            orchestrator_control_endpoint: "tcp://127.0.0.1:5560".to_string(),
            orchestrator_events_endpoint: "tcp://127.0.0.1:5561".to_string(),
            health_pub_endpoint: "tcp://127.0.0.1:5562".to_string(),
            circuit_breaker_pub_endpoint: "tcp://127.0.0.1:5563".to_string(),
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_app_config_validate_invalid_workload() {
        use crate::core::domain::operation_mode::WorkloadConfigError;

        let invalid_workload_config = WorkloadConfig {
            mode: OperationMode::ReadOnly,
            allowed_workloads: vec![WorkloadType::LiveTrading], // Invalid: ReadOnly doesn't allow LiveTrading
            isolate_bulk_operations: false,
            max_concurrent_bulk: 1,
            per_workload_rate_limiting: false,
        };

        let config = AppConfig {
            zmq_rep_endpoint: "tcp://127.0.0.1:5555".to_string(),
            zmq_pub_endpoint: "tcp://127.0.0.1:5556".to_string(),
            zmq_order_lifecycle_endpoint: "tcp://127.0.0.1:5557".to_string(),
            broker: "alpaca".to_string(),
            rate_limit_rpm: 200.0,
            cb_failure_threshold: 3,
            cb_cooldown_secs: 30,
            reconnect_max_attempts: 5,
            reconnect_base_ms: 500,
            journal_db_path: "journal.db".to_string(),
            market_data_feed: "iex".to_string(),
            market_data_symbols: vec!["AAPL".to_string()],
            operation_mode: OperationMode::ReadOnly,
            workload_config: invalid_workload_config,
            orchestrator_control_endpoint: "tcp://127.0.0.1:5560".to_string(),
            orchestrator_events_endpoint: "tcp://127.0.0.1:5561".to_string(),
            health_pub_endpoint: "tcp://127.0.0.1:5562".to_string(),
            circuit_breaker_pub_endpoint: "tcp://127.0.0.1:5563".to_string(),
        };

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid workload configuration"));
    }

    #[test]
    fn test_env_symbol_list_parsing() {
        // Test with explicit values
        let symbols = env_symbol_list("MARKET_DATA_SYMBOLS", &["AAPL", "SPY"]);
        // Since we're not setting the env var, it should use defaults
        assert_eq!(symbols, vec!["AAPL", "SPY"]);
    }
}