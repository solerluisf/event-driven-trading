// adapters/messaging/bus_adapter.rs (Enhanced with sync/async split)
//
// Binds a ZeroMQ REP socket with CRITICAL PATH separation:
// - Synchronous fast-path for time-sensitive execution commands (Submit/Cancel/Replace)
// - Async handling for non-critical operations (Query, Subscribe/Unsubscribe)
// - Priority-based request routing

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use zmq::Context;

use crate::core::application::gateway_service::GatewayService;
use crate::core::domain::wire_message::{
    GatewayRequest, GatewayResponse, ResponsePayload, ErrorPayload,
};
use super::wire_codec::{
    decode_gateway_request, encode_gateway_response, WireFormat,
};

/// Command priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CommandPriority {
    /// Critical execution commands that require immediate response
    Critical = 0,
    /// Standard priority for most operations
    Normal = 1,
    /// Low priority for background operations
    Low = 2,
}

impl CommandPriority {
    /// Get the priority for a gateway request
    pub fn for_request(req: &GatewayRequest) -> Self {
        match req {
            // Execution commands are CRITICAL - must be fast
            GatewayRequest::SubmitOrder(_) => CommandPriority::Critical,
            GatewayRequest::CancelOrder(_) => CommandPriority::Critical,
            GatewayRequest::ReplaceOrder(_) => CommandPriority::Critical,
            
            // Queries are normal priority
            GatewayRequest::QueryStatus(_) => CommandPriority::Normal,
            
            // Subscribe/Unsubscribe are low priority (can be delayed)
            GatewayRequest::Subscribe(_) => CommandPriority::Low,
            GatewayRequest::Unsubscribe(_) => CommandPriority::Low,
        }
    }
    
    /// Check if this priority should use synchronous processing
    pub fn is_synchronous(&self) -> bool {
        matches!(self, CommandPriority::Critical)
    }
}

/// Configuration for the bus adapter
#[derive(Debug, Clone)]
pub struct BusAdapterConfig {
    /// Whether to enable synchronous critical path
    pub enable_sync_critical_path: bool,
    /// Timeout for synchronous operations (milliseconds)
    pub sync_timeout_ms: u64,
    /// Max concurrent async operations
    pub max_concurrent_async: usize,
    /// Channel capacity for critical path
    pub critical_channel_capacity: usize,
    /// Channel capacity for normal priority
    pub normal_channel_capacity: usize,
    /// Channel capacity for low priority
    pub low_channel_capacity: usize,
}

impl Default for BusAdapterConfig {
    fn default() -> Self {
        Self {
            enable_sync_critical_path: true,
            sync_timeout_ms: 500, // 500ms max for critical path
            max_concurrent_async: 10,
            critical_channel_capacity: 128,
            normal_channel_capacity: 64,
            low_channel_capacity: 32,
        }
    }
}

impl BusAdapterConfig {
    /// High-frequency trading configuration (stricter timeouts)
    pub fn hft() -> Self {
        Self {
            sync_timeout_ms: 100, // 100ms for HFT
            ..Default::default()
        }
    }
    
    /// Disable sync critical path (all async)
    pub fn without_sync_path(mut self) -> Self {
        self.enable_sync_critical_path = false;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::order::{OrderCmd, OrderSide, OrderType, TimeInForce, CancelCmd, ExecutionId};
    use crate::core::domain::market_data::MarketSubscription;

    fn create_submit_request() -> GatewayRequest {
        GatewayRequest::SubmitOrder(OrderCmd {
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
            correlation_id: Some("corr-priority-001".into()),
        })
    }

    fn create_cancel_request() -> GatewayRequest {
        GatewayRequest::CancelOrder(CancelCmd {
            execution_id: ExecutionId("exec-123".to_string()),
            correlation_id: Some("corr-priority-002".into()),
        })
    }

    #[test]
    fn test_submit_order_is_critical_priority() {
        let req = create_submit_request();
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Critical);
        assert!(CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_cancel_order_is_critical_priority() {
        let req = create_cancel_request();
        assert_eq!(CommandPriority::for_request(&req), CommandPriority::Critical);
        assert!(CommandPriority::for_request(&req).is_synchronous());
    }

    #[test]
    fn test_priority_ordering() {
        assert!(CommandPriority::Critical < CommandPriority::Normal);
        assert!(CommandPriority::Normal < CommandPriority::Low);
    }

    #[test]
    fn test_bus_adapter_config_default() {
        let config = BusAdapterConfig::default();
        assert!(config.enable_sync_critical_path);
        assert_eq!(config.sync_timeout_ms, 500);
    }
}
