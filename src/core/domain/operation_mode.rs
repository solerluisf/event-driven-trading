// core/domain/operation_mode.rs
//
// Defines operation modes and workload types for separating live trading
// from offline/bulk operations like reconciliation and historical downloads.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Operational mode for the gateway - determines what types of operations are allowed
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationMode {
    /// Full live trading mode - all operations allowed
    Live,
    /// Paper trading / sandbox mode - no real money at risk
    Paper,
    /// Read-only mode - queries allowed, no order modifications
    ReadOnly,
    /// Offline mode - only local operations, no broker connectivity
    Offline,
}

impl OperationMode {
    /// Check if live trading operations are allowed
    pub fn allows_live_trading(&self) -> bool {
        matches!(self, OperationMode::Live | OperationMode::Paper)
    }

    /// Check if order modifications (submit, cancel, replace) are allowed
    pub fn allows_order_modifications(&self) -> bool {
        matches!(self, OperationMode::Live | OperationMode::Paper)
    }

    /// Check if broker connectivity is required
    pub fn requires_broker_connection(&self) -> bool {
        matches!(self, OperationMode::Live | OperationMode::Paper | OperationMode::ReadOnly)
    }

    /// Check if queries to broker are allowed
    pub fn allows_queries(&self) -> bool {
        matches!(self, OperationMode::Live | OperationMode::Paper | OperationMode::ReadOnly)
    }

    /// Check if offline operations are allowed
    pub fn allows_offline_operations(&self) -> bool {
        true // All modes allow offline operations
    }
}

impl Default for OperationMode {
    fn default() -> Self {
        OperationMode::Paper // Safer default
    }
}

impl fmt::Display for OperationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationMode::Live => write!(f, "live"),
            OperationMode::Paper => write!(f, "paper"),
            OperationMode::ReadOnly => write!(f, "readonly"),
            OperationMode::Offline => write!(f, "offline"),
        }
    }
}

/// Types of workloads that can be executed
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WorkloadType {
    /// Real-time order submission, cancellation, modification
    LiveTrading,
    /// Order status queries and position queries
    Query,
    /// Historical order reconciliation (bulk status checks)
    Reconciliation,
    /// Historical market data download
    HistoricalDataDownload,
    /// Market data streaming subscription
    MarketDataStreaming,
    /// Administrative operations (config, health checks)
    Administrative,
}

impl WorkloadType {
    /// Get the priority level (lower = higher priority)
    pub fn priority(&self) -> u8 {
        match self {
            WorkloadType::LiveTrading => 1,        // Highest priority
            WorkloadType::MarketDataStreaming => 2,
            WorkloadType::Query => 3,
            WorkloadType::Administrative => 4,
            WorkloadType::Reconciliation => 5,     // Lower priority - bulk operation
            WorkloadType::HistoricalDataDownload => 6, // Lowest priority - can be throttled
        }
    }

    /// Check if this workload requires live broker connectivity
    pub fn requires_live_connectivity(&self) -> bool {
        matches!(
            self,
            WorkloadType::LiveTrading | WorkloadType::Query | WorkloadType::MarketDataStreaming
        )
    }

    /// Check if this is a bulk/offline operation that can be rate-limited separately
    pub fn is_bulk_operation(&self) -> bool {
        matches!(
            self,
            WorkloadType::Reconciliation | WorkloadType::HistoricalDataDownload
        )
    }

    /// Check if this workload can be queued when system is under load
    pub fn can_be_queued(&self) -> bool {
        matches!(
            self,
            WorkloadType::Reconciliation | WorkloadType::HistoricalDataDownload | WorkloadType::Administrative
        )
    }

    /// Get the recommended rate limit (requests per minute) for this workload type
    pub fn recommended_rate_limit_rpm(&self) -> f64 {
        match self {
            WorkloadType::LiveTrading => 300.0,         // High limit for live trading
            WorkloadType::MarketDataStreaming => 1000.0, // Very high for streaming
            WorkloadType::Query => 200.0,
            WorkloadType::Administrative => 60.0,
            WorkloadType::Reconciliation => 30.0,       // Throttled - bulk operation
            WorkloadType::HistoricalDataDownload => 10.0, // Very throttled - can be slow
        }
    }
}

impl fmt::Display for WorkloadType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadType::LiveTrading => write!(f, "live_trading"),
            WorkloadType::Query => write!(f, "query"),
            WorkloadType::Reconciliation => write!(f, "reconciliation"),
            WorkloadType::HistoricalDataDownload => write!(f, "historical_download"),
            WorkloadType::MarketDataStreaming => write!(f, "market_data_streaming"),
            WorkloadType::Administrative => write!(f, "administrative"),
        }
    }
}

/// Configuration for workload separation
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkloadConfig {
    /// Current operational mode
    pub mode: OperationMode,
    /// Allowed workload types (empty = all allowed)
    pub allowed_workloads: Vec<WorkloadType>,
    /// Whether bulk operations should be isolated on separate threads
    pub isolate_bulk_operations: bool,
    /// Maximum concurrent bulk operations
    pub max_concurrent_bulk: usize,
    /// Whether to enable separate rate limiting per workload type
    pub per_workload_rate_limiting: bool,
}

impl WorkloadConfig {
    /// Create a new workload configuration for live trading
    pub fn live_trading() -> Self {
        Self {
            mode: OperationMode::Live,
            allowed_workloads: vec![
                WorkloadType::LiveTrading,
                WorkloadType::Query,
                WorkloadType::MarketDataStreaming,
                WorkloadType::Administrative,
            ],
            isolate_bulk_operations: true,
            max_concurrent_bulk: 2,
            per_workload_rate_limiting: true,
        }
    }

    /// Create a new workload configuration for paper trading
    pub fn paper_trading() -> Self {
        Self {
            mode: OperationMode::Paper,
            allowed_workloads: vec![
                WorkloadType::LiveTrading,
                WorkloadType::Query,
                WorkloadType::MarketDataStreaming,
                WorkloadType::Administrative,
                WorkloadType::Reconciliation,
            ],
            isolate_bulk_operations: true,
            max_concurrent_bulk: 3,
            per_workload_rate_limiting: true,
        }
    }

    /// Create a new workload configuration for backoffice/reconciliation only
    pub fn backoffice_only() -> Self {
        Self {
            mode: OperationMode::ReadOnly,
            allowed_workloads: vec![
                WorkloadType::Query,
                WorkloadType::Reconciliation,
                WorkloadType::HistoricalDataDownload,
                WorkloadType::Administrative,
            ],
            isolate_bulk_operations: false,
            max_concurrent_bulk: 5,
            per_workload_rate_limiting: true,
        }
    }

    /// Check if a workload type is allowed
    pub fn is_workload_allowed(&self, workload: WorkloadType) -> bool {
        self.allowed_workloads.is_empty() || self.allowed_workloads.contains(&workload)
    }

    /// Validate that current mode supports the allowed workloads
    pub fn validate(&self) -> Result<(), WorkloadConfigError> {
        // Check that mode allows the configured workloads
        for workload in &self.allowed_workloads {
            match (self.mode, workload) {
                (OperationMode::ReadOnly, WorkloadType::LiveTrading) => {
                    return Err(WorkloadConfigError::InvalidWorkloadForMode {
                        mode: self.mode,
                        workload: *workload,
                    });
                }
                (OperationMode::Offline, w) if w.requires_live_connectivity() => {
                    return Err(WorkloadConfigError::InvalidWorkloadForMode {
                        mode: self.mode,
                        workload: *workload,
                    });
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self::paper_trading()
    }
}

/// Errors that can occur with workload configuration
#[derive(Debug, Clone, PartialEq)]
pub enum WorkloadConfigError {
    InvalidWorkloadForMode {
        mode: OperationMode,
        workload: WorkloadType,
    },
    WorkloadNotAllowed(WorkloadType),
}

impl fmt::Display for WorkloadConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkloadConfigError::InvalidWorkloadForMode { mode, workload } => {
                write!(
                    f,
                    "Workload {} is not compatible with operation mode {}",
                    workload, mode
                )
            }
            WorkloadConfigError::WorkloadNotAllowed(workload) => {
                write!(f, "Workload {} is not allowed in current configuration", workload)
            }
        }
    }
}

impl std::error::Error for WorkloadConfigError {}

/// Validates a GatewayRequest against operation mode and workload config
pub fn validate_request_for_mode(
    mode: OperationMode,
    config: &WorkloadConfig,
    workload: WorkloadType,
) -> Result<(), WorkloadConfigError> {
    // Check if workload is allowed
    if !config.is_workload_allowed(workload) {
        return Err(WorkloadConfigError::WorkloadNotAllowed(workload));
    }

    // Check mode-specific restrictions
    match (mode, workload) {
        (OperationMode::ReadOnly | OperationMode::Offline, WorkloadType::LiveTrading) => {
            Err(WorkloadConfigError::InvalidWorkloadForMode { mode, workload })
        }
        (OperationMode::Offline, w) if w.requires_live_connectivity() => {
            Err(WorkloadConfigError::InvalidWorkloadForMode { mode, workload })
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_mode_live_allows_trading() {
        assert!(OperationMode::Live.allows_live_trading());
        assert!(OperationMode::Live.allows_order_modifications());
        assert!(OperationMode::Live.requires_broker_connection());
    }

    #[test]
    fn test_operation_mode_paper_allows_trading() {
        assert!(OperationMode::Paper.allows_live_trading());
        assert!(OperationMode::Paper.allows_order_modifications());
        assert!(OperationMode::Paper.requires_broker_connection());
    }

    #[test]
    fn test_operation_mode_readonly_blocks_trading() {
        assert!(!OperationMode::ReadOnly.allows_live_trading());
        assert!(!OperationMode::ReadOnly.allows_order_modifications());
        assert!(OperationMode::ReadOnly.allows_queries());
        assert!(OperationMode::ReadOnly.requires_broker_connection());
    }

    #[test]
    fn test_operation_mode_offline_blocks_all_broker_ops() {
        assert!(!OperationMode::Offline.allows_live_trading());
        assert!(!OperationMode::Offline.allows_order_modifications());
        assert!(!OperationMode::Offline.allows_queries());
        assert!(!OperationMode::Offline.requires_broker_connection());
    }

    #[test]
    fn test_default_operation_mode_is_paper() {
        assert_eq!(OperationMode::default(), OperationMode::Paper);
    }

    #[test]
    fn test_operation_mode_display() {
        assert_eq!(OperationMode::Live.to_string(), "live");
        assert_eq!(OperationMode::Paper.to_string(), "paper");
        assert_eq!(OperationMode::ReadOnly.to_string(), "readonly");
        assert_eq!(OperationMode::Offline.to_string(), "offline");
    }

    #[test]
    fn test_workload_type_priorities() {
        assert_eq!(WorkloadType::LiveTrading.priority(), 1);
        assert_eq!(WorkloadType::MarketDataStreaming.priority(), 2);
        assert_eq!(WorkloadType::Query.priority(), 3);
        assert_eq!(WorkloadType::Administrative.priority(), 4);
        assert_eq!(WorkloadType::Reconciliation.priority(), 5);
        assert_eq!(WorkloadType::HistoricalDataDownload.priority(), 6);
    }

    #[test]
    fn test_bulk_operations_identified_correctly() {
        assert!(WorkloadType::Reconciliation.is_bulk_operation());
        assert!(WorkloadType::HistoricalDataDownload.is_bulk_operation());
        assert!(!WorkloadType::LiveTrading.is_bulk_operation());
        assert!(!WorkloadType::Query.is_bulk_operation());
    }

    #[test]
    fn test_workload_rate_limits() {
        assert!(WorkloadType::LiveTrading.recommended_rate_limit_rpm() > WorkloadType::Reconciliation.recommended_rate_limit_rpm());
        assert!(WorkloadType::MarketDataStreaming.recommended_rate_limit_rpm() > WorkloadType::HistoricalDataDownload.recommended_rate_limit_rpm());
    }

    #[test]
    fn test_live_trading_config_allows_trading_not_bulk() {
        let config = WorkloadConfig::live_trading();
        assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Query));
        assert!(!config.is_workload_allowed(WorkloadType::Reconciliation));
        assert!(!config.is_workload_allowed(WorkloadType::HistoricalDataDownload));
    }

    #[test]
    fn test_paper_trading_config_allows_more_workloads() {
        let config = WorkloadConfig::paper_trading();
        assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Reconciliation));
    }

    #[test]
    fn test_backoffice_config_blocks_trading() {
        let config = WorkloadConfig::backoffice_only();
        assert!(!config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Reconciliation));
        assert!(config.is_workload_allowed(WorkloadType::HistoricalDataDownload));
    }

    #[test]
    fn test_validate_live_trading_config_succeeds() {
        let config = WorkloadConfig::live_trading();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_backoffice_config_succeeds() {
        let config = WorkloadConfig::backoffice_only();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_readonly_with_live_trading() {
        let config = WorkloadConfig {
            mode: OperationMode::ReadOnly,
            allowed_workloads: vec![WorkloadType::LiveTrading],
            isolate_bulk_operations: false,
            max_concurrent_bulk: 1,
            per_workload_rate_limiting: false,
        };
        
        let result = config.validate();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            WorkloadConfigError::InvalidWorkloadForMode { .. }
        ));
    }

    #[test]
    fn test_validate_rejects_offline_with_live_connectivity() {
        let config = WorkloadConfig {
            mode: OperationMode::Offline,
            allowed_workloads: vec![WorkloadType::Query],
            isolate_bulk_operations: false,
            max_concurrent_bulk: 1,
            per_workload_rate_limiting: false,
        };
        
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_request_allows_live_trading_in_live_mode() {
        let config = WorkloadConfig::live_trading();
        let result = validate_request_for_mode(
            OperationMode::Live,
            &config,
            WorkloadType::LiveTrading,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_request_blocks_live_trading_in_readonly() {
        let config = WorkloadConfig::backoffice_only();
        let result = validate_request_for_mode(
            OperationMode::ReadOnly,
            &config,
            WorkloadType::LiveTrading,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_request_blocks_query_in_offline_mode() {
        let config = WorkloadConfig::backoffice_only();
        let result = validate_request_for_mode(
            OperationMode::Offline,
            &config,
            WorkloadType::Query,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_request_blocks_disallowed_workload() {
        let mut config = WorkloadConfig::live_trading();
        config.allowed_workloads = vec![WorkloadType::LiveTrading]; // Only live trading allowed
        
        let result = validate_request_for_mode(
            OperationMode::Live,
            &config,
            WorkloadType::Administrative,
        );
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            WorkloadConfigError::WorkloadNotAllowed(_)
        ));
    }

    #[test]
    fn test_default_workload_config_is_paper() {
        let config: WorkloadConfig = Default::default();
        assert_eq!(config.mode, OperationMode::Paper);
        assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
    }

    #[test]
    fn test_all_modes_allow_offline_operations() {
        assert!(OperationMode::Live.allows_offline_operations());
        assert!(OperationMode::Paper.allows_offline_operations());
        assert!(OperationMode::ReadOnly.allows_offline_operations());
        assert!(OperationMode::Offline.allows_offline_operations());
    }

    #[test]
    fn test_empty_allowed_workloads_means_all_allowed() {
        let config = WorkloadConfig {
            mode: OperationMode::Live,
            allowed_workloads: vec![], // Empty means all allowed
            isolate_bulk_operations: false,
            max_concurrent_bulk: 5,
            per_workload_rate_limiting: false,
        };
        
        assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
        assert!(config.is_workload_allowed(WorkloadType::Reconciliation));
        assert!(config.is_workload_allowed(WorkloadType::HistoricalDataDownload));
    }

    #[test]
    fn test_error_display_messages() {
        let error = WorkloadConfigError::InvalidWorkloadForMode {
            mode: OperationMode::ReadOnly,
            workload: WorkloadType::LiveTrading,
        };
        let msg = error.to_string();
        assert!(msg.contains("readonly"), "Expected 'readonly' in message: {}", msg);
        assert!(msg.contains("live_trading"), "Expected 'live_trading' in message: {}", msg);

        let error2 = WorkloadConfigError::WorkloadNotAllowed(WorkloadType::Reconciliation);
        let msg2 = error2.to_string();
        assert!(msg2.contains("reconciliation"), "Expected 'reconciliation' in message: {}", msg2);
        assert!(msg2.contains("not allowed"), "Expected 'not allowed' in message: {}", msg2);
    }

    #[test]
    fn test_workload_type_display() {
        assert_eq!(WorkloadType::LiveTrading.to_string(), "live_trading");
        assert_eq!(WorkloadType::Reconciliation.to_string(), "reconciliation");
        assert_eq!(WorkloadType::HistoricalDataDownload.to_string(), "historical_download");
    }

    #[test]
    fn test_live_trading_config_isolates_bulk_ops() {
        let config = WorkloadConfig::live_trading();
        assert!(config.isolate_bulk_operations);
        assert_eq!(config.max_concurrent_bulk, 2);
        assert!(config.per_workload_rate_limiting);
    }

    #[test]
    fn test_backoffice_allows_more_concurrent_bulk() {
        let config = WorkloadConfig::backoffice_only();
        assert_eq!(config.max_concurrent_bulk, 5);
    }
}
