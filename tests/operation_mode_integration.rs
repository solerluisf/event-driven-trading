// tests/operation_mode_integration.rs
//
// Integration tests for operation mode and workload separation
// Verifies that different modes properly restrict operations

use broker_gateway_service::core::domain::operation_mode::{
    OperationMode, WorkloadConfig, WorkloadType, WorkloadConfigError,
    validate_request_for_mode
};

#[test]
fn test_live_mode_allows_all_trading_operations() {
    let config = WorkloadConfig::live_trading();
    
    // Live trading should work
    assert!(validate_request_for_mode(
        OperationMode::Live,
        &config,
        WorkloadType::LiveTrading,
    ).is_ok());
    
    // Queries should work
    assert!(validate_request_for_mode(
        OperationMode::Live,
        &config,
        WorkloadType::Query,
    ).is_ok());
    
    // Market data streaming should work
    assert!(validate_request_for_mode(
        OperationMode::Live,
        &config,
        WorkloadType::MarketDataStreaming,
    ).is_ok());
}

#[test]
fn test_live_mode_blocks_bulk_operations_by_default() {
    let config = WorkloadConfig::live_trading();
    
    // Reconciliation should be blocked in live trading config
    let result = validate_request_for_mode(
        OperationMode::Live,
        &config,
        WorkloadType::Reconciliation,
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), WorkloadConfigError::WorkloadNotAllowed(_)));
}

#[test]
fn test_paper_mode_allows_bulk_operations() {
    let config = WorkloadConfig::paper_trading();
    
    // Reconciliation should work in paper mode
    assert!(validate_request_for_mode(
        OperationMode::Paper,
        &config,
        WorkloadType::Reconciliation,
    ).is_ok());
    
    // Historical downloads should NOT be in paper config by default
    // (it would be in backoffice config)
    let result = validate_request_for_mode(
        OperationMode::Paper,
        &config,
        WorkloadType::HistoricalDataDownload,
    );
    assert!(result.is_err());
}

#[test]
fn test_readonly_mode_blocks_order_submission() {
    // First test that LiveTrading is not in allowed workloads for backoffice
    let config = WorkloadConfig::backoffice_only();
    assert!(!config.is_workload_allowed(WorkloadType::LiveTrading));
    
    // Even if we add LiveTrading to allowed list, ReadOnly mode should block it
    let config_with_trading = WorkloadConfig {
        mode: OperationMode::ReadOnly,
        allowed_workloads: vec![WorkloadType::LiveTrading, WorkloadType::Query],
        isolate_bulk_operations: false,
        max_concurrent_bulk: 1,
        per_workload_rate_limiting: false,
    };
    
    let result = validate_request_for_mode(
        OperationMode::ReadOnly,
        &config_with_trading,
        WorkloadType::LiveTrading,
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), WorkloadConfigError::InvalidWorkloadForMode { .. }));
}

#[test]
fn test_readonly_mode_allows_queries() {
    let config = WorkloadConfig::backoffice_only();
    
    // Queries should work
    assert!(validate_request_for_mode(
        OperationMode::ReadOnly,
        &config,
        WorkloadType::Query,
    ).is_ok());
}

#[test]
fn test_offline_mode_blocks_all_broker_operations() {
    let config = WorkloadConfig::backoffice_only();
    
    // Any operation requiring broker connection should fail
    let result = validate_request_for_mode(
        OperationMode::Offline,
        &config,
        WorkloadType::Query,
    );
    assert!(result.is_err());
    
    let result = validate_request_for_mode(
        OperationMode::Offline,
        &config,
        WorkloadType::LiveTrading,
    );
    assert!(result.is_err());
    
    let result = validate_request_for_mode(
        OperationMode::Offline,
        &config,
        WorkloadType::MarketDataStreaming,
    );
    assert!(result.is_err());
}

#[test]
fn test_bulk_operations_get_lower_rate_limits() {
    // Live trading gets high rate limit
    let live_limit = WorkloadType::LiveTrading.recommended_rate_limit_rpm();
    
    // Bulk operations get much lower rate limits
    let reconciliation_limit = WorkloadType::Reconciliation.recommended_rate_limit_rpm();
    let download_limit = WorkloadType::HistoricalDataDownload.recommended_rate_limit_rpm();
    
    assert!(live_limit > reconciliation_limit);
    assert!(reconciliation_limit > download_limit);
    
    // Specific values
    assert_eq!(live_limit, 300.0);
    assert_eq!(reconciliation_limit, 30.0);
    assert_eq!(download_limit, 10.0);
}

#[test]
fn test_live_trading_has_highest_priority() {
    assert!(WorkloadType::LiveTrading.priority() < WorkloadType::Query.priority());
    assert!(WorkloadType::Query.priority() < WorkloadType::Reconciliation.priority());
    assert!(WorkloadType::Reconciliation.priority() < WorkloadType::HistoricalDataDownload.priority());
}

#[test]
fn test_bulk_operations_can_be_queued() {
    assert!(WorkloadType::Reconciliation.can_be_queued());
    assert!(WorkloadType::HistoricalDataDownload.can_be_queued());
    assert!(!WorkloadType::LiveTrading.can_be_queued());
}

#[test]
fn test_live_config_isolates_bulk_operations() {
    let config = WorkloadConfig::live_trading();
    
    assert!(config.isolate_bulk_operations);
    assert_eq!(config.max_concurrent_bulk, 2);
}

#[test]
fn test_backoffice_config_allows_more_bulk_concurrency() {
    let config = WorkloadConfig::backoffice_only();
    
    assert_eq!(config.max_concurrent_bulk, 5);
}

#[test]
fn test_custom_config_can_allow_specific_workloads() {
    let config = WorkloadConfig {
        mode: OperationMode::Live,
        allowed_workloads: vec![
            WorkloadType::LiveTrading,
            WorkloadType::Query,
        ],
        isolate_bulk_operations: true,
        max_concurrent_bulk: 1,
        per_workload_rate_limiting: true,
    };
    
    // Allowed workloads work
    assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
    assert!(config.is_workload_allowed(WorkloadType::Query));
    
    // Disallowed workloads fail
    assert!(!config.is_workload_allowed(WorkloadType::Administrative));
    assert!(!config.is_workload_allowed(WorkloadType::Reconciliation));
}

#[test]
fn test_empty_allowed_list_means_all_allowed() {
    let config = WorkloadConfig {
        mode: OperationMode::Live,
        allowed_workloads: vec![], // Empty
        isolate_bulk_operations: false,
        max_concurrent_bulk: 5,
        per_workload_rate_limiting: false,
    };
    
    // All workloads should be allowed
    assert!(config.is_workload_allowed(WorkloadType::LiveTrading));
    assert!(config.is_workload_allowed(WorkloadType::Reconciliation));
    assert!(config.is_workload_allowed(WorkloadType::HistoricalDataDownload));
}

#[test]
fn test_config_validation_catches_invalid_combinations() {
    // ReadOnly mode with LiveTrading workload should fail validation
    let invalid_config = WorkloadConfig {
        mode: OperationMode::ReadOnly,
        allowed_workloads: vec![WorkloadType::LiveTrading],
        isolate_bulk_operations: false,
        max_concurrent_bulk: 1,
        per_workload_rate_limiting: false,
    };
    
    let result = invalid_config.validate();
    assert!(result.is_err());
}

#[test]
fn test_operation_mode_display_format() {
    assert_eq!(OperationMode::Live.to_string(), "live");
    assert_eq!(OperationMode::Paper.to_string(), "paper");
    assert_eq!(OperationMode::ReadOnly.to_string(), "readonly");
    assert_eq!(OperationMode::Offline.to_string(), "offline");
}

#[test]
fn test_workload_type_display_format() {
    assert_eq!(WorkloadType::LiveTrading.to_string(), "live_trading");
    assert_eq!(WorkloadType::Reconciliation.to_string(), "reconciliation");
    assert_eq!(WorkloadType::HistoricalDataDownload.to_string(), "historical_download");
    assert_eq!(WorkloadType::MarketDataStreaming.to_string(), "market_data_streaming");
}

#[test]
fn test_error_types_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OperationMode>();
    assert_send_sync::<WorkloadType>();
    assert_send_sync::<WorkloadConfig>();
    assert_send_sync::<WorkloadConfigError>();
}

#[test]
fn test_default_configs_are_sensible() {
    // Live trading config should be restrictive
    let live = WorkloadConfig::live_trading();
    assert_eq!(live.mode, OperationMode::Live);
    assert!(!live.is_workload_allowed(WorkloadType::HistoricalDataDownload));
    
    // Paper config should be more permissive
    let paper = WorkloadConfig::paper_trading();
    assert_eq!(paper.mode, OperationMode::Paper);
    assert!(paper.is_workload_allowed(WorkloadType::Reconciliation));
    
    // Backoffice should not allow trading
    let backoffice = WorkloadConfig::backoffice_only();
    assert_eq!(backoffice.mode, OperationMode::ReadOnly);
    assert!(!backoffice.is_workload_allowed(WorkloadType::LiveTrading));
}

#[test]
fn test_all_modes_allow_offline_operations() {
    for mode in [OperationMode::Live, OperationMode::Paper, OperationMode::ReadOnly, OperationMode::Offline] {
        assert!(mode.allows_offline_operations(), "Mode {:?} should allow offline operations", mode);
    }
}

#[test]
fn test_serialization_roundtrip() {
    use serde_json;
    
    let config = WorkloadConfig::paper_trading();
    let json = serde_json::to_string(&config).expect("Should serialize");
    let deserialized: WorkloadConfig = serde_json::from_str(&json).expect("Should deserialize");
    
    assert_eq!(config.mode, deserialized.mode);
    assert_eq!(config.allowed_workloads, deserialized.allowed_workloads);
    assert_eq!(config.isolate_bulk_operations, deserialized.isolate_bulk_operations);
    assert_eq!(config.max_concurrent_bulk, deserialized.max_concurrent_bulk);
    assert_eq!(config.per_workload_rate_limiting, deserialized.per_workload_rate_limiting);
}
