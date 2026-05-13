// tests/sync_async_split_integration.rs
//
// Integration tests for sync/async critical path separation
// Verifies that execution commands get priority handling

use broker_gateway_service::core::domain::wire_message::GatewayRequest;
use broker_gateway_service::core::domain::order::{OrderCmd, OrderSide, OrderType, TimeInForce, CancelCmd, ExecutionId, ReplaceCmd, StatusQuery};
use broker_gateway_service::core::domain::market_data::MarketSubscription;
use broker_gateway_service::adapters::messaging::priority_bus_adapter::{CommandPriority, BusAdapterConfig};

#[test]
fn test_all_execution_commands_are_critical() {
    let submit = GatewayRequest::SubmitOrder(OrderCmd {
        symbol: "AAPL".to_string(),
        qty: 100,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Day,
        limit_price: None,
        stop_price: None,
        client_order_id: Some("test-123".to_string()),
        extended_hours: false,
        notional: None,
        correlation_id: None,
    });
    
    let cancel = GatewayRequest::CancelOrder(CancelCmd {
        execution_id: ExecutionId("exec-456".to_string()),
        correlation_id: None,
    });
    
    let replace = GatewayRequest::ReplaceOrder(ReplaceCmd {
        execution_id: ExecutionId("exec-789".to_string()),
        symbol: "AAPL".to_string(),
        side: OrderSide::Buy,
        qty: Some(200),
        limit_price: Some(150.0),
        correlation_id: None,
    });
    
    // All execution commands should be CRITICAL priority
    assert_eq!(CommandPriority::for_request(&submit), CommandPriority::Critical);
    assert_eq!(CommandPriority::for_request(&cancel), CommandPriority::Critical);
    assert_eq!(CommandPriority::for_request(&replace), CommandPriority::Critical);
    
    // All should use synchronous processing
    assert!(CommandPriority::for_request(&submit).is_synchronous());
    assert!(CommandPriority::for_request(&cancel).is_synchronous());
    assert!(CommandPriority::for_request(&replace).is_synchronous());
}

#[test]
fn test_query_is_normal_priority() {
    let query = GatewayRequest::QueryStatus(StatusQuery {
        execution_id: ExecutionId("exec-123".to_string()),
        correlation_id: None,
    });
    
    assert_eq!(CommandPriority::for_request(&query), CommandPriority::Normal);
    assert!(!CommandPriority::for_request(&query).is_synchronous());
}

#[test]
fn test_subscription_is_low_priority() {
    let subscribe = GatewayRequest::Subscribe(MarketSubscription {
        symbol: "AAPL".to_string(),
        correlation_id: None,
    });
    
    let unsubscribe = GatewayRequest::Unsubscribe(MarketSubscription {
        symbol: "AAPL".to_string(),
        correlation_id: None,
    });
    
    assert_eq!(CommandPriority::for_request(&subscribe), CommandPriority::Low);
    assert_eq!(CommandPriority::for_request(&unsubscribe), CommandPriority::Low);
    
    assert!(!CommandPriority::for_request(&subscribe).is_synchronous());
    assert!(!CommandPriority::for_request(&unsubscribe).is_synchronous());
}

#[test]
fn test_priority_ordering_is_correct() {
    // Verify ordering: Critical < Normal < Low
    // This ensures proper prioritization in queues
    assert!(CommandPriority::Critical < CommandPriority::Normal);
    assert!(CommandPriority::Normal < CommandPriority::Low);
    assert!(CommandPriority::Critical < CommandPriority::Low);
}

#[test]
fn test_default_config_enables_sync_path() {
    let config = BusAdapterConfig::default();
    
    assert!(config.enable_sync_critical_path);
    assert_eq!(config.sync_timeout_ms, 500);
    assert_eq!(config.critical_channel_capacity, 128);
    assert_eq!(config.normal_channel_capacity, 64);
    assert_eq!(config.low_channel_capacity, 32);
}

#[test]
fn test_hft_config_has_stricter_timeouts() {
    let config = BusAdapterConfig::hft();
    
    // HFT requires faster response times
    assert_eq!(config.sync_timeout_ms, 100);
    assert!(config.enable_sync_critical_path);
}

#[test]
fn test_config_can_disable_sync_path() {
    let config = BusAdapterConfig::default().without_sync_path();
    
    assert!(!config.enable_sync_critical_path);
}

#[test]
fn test_critical_path_only_for_execution_commands() {
    // Verify ONLY execution commands use critical path
    let execution_commands = vec![
        GatewayRequest::SubmitOrder(OrderCmd {
            symbol: "TEST".to_string(),
            qty: 1,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: None,
            extended_hours: false,
            notional: None,
            correlation_id: None,
        }),
        GatewayRequest::CancelOrder(CancelCmd {
            execution_id: ExecutionId("test".to_string()),
            correlation_id: None,
        }),
        GatewayRequest::ReplaceOrder(ReplaceCmd {
            execution_id: ExecutionId("test".to_string()),
            symbol: "TEST".to_string(),
            side: OrderSide::Buy,
            qty: None,
            limit_price: None,
            correlation_id: None,
        }),
    ];
    
    for cmd in execution_commands {
        assert!(
            CommandPriority::for_request(&cmd).is_synchronous(),
            "Execution command should use synchronous path: {:?}",
            cmd
        );
    }
}

#[test]
fn test_non_execution_commands_use_async() {
    // Verify non-execution commands do NOT use critical path
    let non_execution_commands = vec![
        GatewayRequest::QueryStatus(StatusQuery {
            execution_id: ExecutionId("test".to_string()),
            correlation_id: None,
        }),
        GatewayRequest::Subscribe(MarketSubscription {
            symbol: "TEST".to_string(),
            correlation_id: None,
        }),
        GatewayRequest::Unsubscribe(MarketSubscription {
            symbol: "TEST".to_string(),
            correlation_id: None,
        }),
    ];
    
    for cmd in non_execution_commands {
        assert!(
            !CommandPriority::for_request(&cmd).is_synchronous(),
            "Non-execution command should use async path: {:?}",
            cmd
        );
    }
}

#[test]
fn test_priority_values() {
    // Verify numeric priority values
    assert_eq!(CommandPriority::Critical as u8, 0);
    assert_eq!(CommandPriority::Normal as u8, 1);
    assert_eq!(CommandPriority::Low as u8, 2);
}

#[test]
fn test_channel_capacities_appropriate() {
    let config = BusAdapterConfig::default();
    
    // Critical path should have highest capacity
    assert!(config.critical_channel_capacity >= config.normal_channel_capacity);
    assert!(config.normal_channel_capacity >= config.low_channel_capacity);
    
    // Specific values
    assert!(config.critical_channel_capacity >= 128);
    assert!(config.normal_channel_capacity >= 32);
    assert!(config.low_channel_capacity >= 16);
}
