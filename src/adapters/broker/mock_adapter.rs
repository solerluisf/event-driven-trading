// adapters/broker/mock_adapter.rs
//
// Enhanced mock broker adapter for testing with support for:
// - Unique execution ID generation for concurrent order testing
// - Configurable success/failure scenarios
// - Order tracking for realistic status queries
// - Simulated latency and error injection

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::core::ports::execution_port::IExecutionPort;
use crate::core::domain::order::{
    OrderCmd,
    CancelCmd,
    ReplaceCmd,
    StatusQuery,
    ExecutionId,
    OrderStatusResponse,
    OrderSide,
};
use crate::adapters::broker::broker_error::BrokerError;

/// Configuration for mock adapter behavior
#[derive(Clone, Debug)]
pub struct MockAdapterConfig {
    /// Global success rate (0.0 - 1.0). Orders may still fail based on individual failure keys.
    pub global_success_rate: f64,
    /// Specific keys that should always fail
    pub failure_keys: Vec<String>,
    /// Specific keys that should always succeed
    pub success_keys: Vec<String>,
    /// Artificial delay in milliseconds
    pub latency_ms: u64,
    /// Whether to return unique execution IDs
    pub unique_ids: bool,
    /// Prefix for execution IDs
    pub id_prefix: String,
    /// Whether to allow cancel/replace operations on untracked orders
    /// When true (default), these operations succeed even for unknown orders
    /// When false, operations fail if the order wasn't submitted through this adapter
    pub allow_untracked_operations: bool,
}

impl Default for MockAdapterConfig {
    fn default() -> Self {
        Self {
            global_success_rate: 1.0,
            failure_keys: Vec::new(),
            success_keys: Vec::new(),
            latency_ms: 0,
            unique_ids: true,
            id_prefix: "mock".to_string(),
            allow_untracked_operations: true,
        }
    }
}

impl MockAdapterConfig {
    /// Create a default config that always succeeds
    pub fn always_succeed() -> Self {
        Self::default()
    }

    /// Create a config that always fails
    pub fn always_fail() -> Self {
        Self {
            global_success_rate: 0.0,
            ..Default::default()
        }
    }

    /// Create a config with a specific success rate
    pub fn with_success_rate(rate: f64) -> Self {
        Self {
            global_success_rate: rate.clamp(0.0, 1.0),
            ..Default::default()
        }
    }

    /// Add a key that should always fail
    pub fn with_failure_key(mut self, key: impl Into<String>) -> Self {
        self.failure_keys.push(key.into());
        self
    }

    /// Add a key that should always succeed
    pub fn with_success_key(mut self, key: impl Into<String>) -> Self {
        self.success_keys.push(key.into());
        self
    }

    /// Set artificial latency
    pub fn with_latency(mut self, ms: u64) -> Self {
        self.latency_ms = ms;
        self
    }

    /// Use sequential IDs instead of unique IDs
    pub fn with_sequential_ids(mut self) -> Self {
        self.unique_ids = false;
        self
    }

    /// Set a custom ID prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.id_prefix = prefix.into();
        self
    }

    /// Require orders to be tracked for cancel/replace operations
    /// When enabled, cancel/replace will fail for orders not submitted through this adapter
    pub fn require_tracked_orders(mut self) -> Self {
        self.allow_untracked_operations = false;
        self
    }
}

/// Tracked order state
#[derive(Clone, Debug)]
struct TrackedOrder {
    execution_id: ExecutionId,
    symbol: String,
    side: OrderSide,
    qty: u32,
    status: String,
}

/// Enhanced mock broker adapter for testing
///
/// This adapter generates unique execution IDs for each order submission,
/// making it suitable for testing scenarios with multiple concurrent orders.
/// It also supports configurable success/failure behavior and order tracking.
pub struct MockAdapter {
    config: MockAdapterConfig,
    counter: AtomicU64,
    orders: Mutex<HashMap<String, TrackedOrder>>,
}

impl MockAdapter {
    /// Create a new mock adapter with default configuration
    pub fn new() -> Self {
        Self::with_config(MockAdapterConfig::default())
    }

    /// Create a new mock adapter with custom configuration
    pub fn with_config(config: MockAdapterConfig) -> Self {
        Self {
            config,
            counter: AtomicU64::new(1),
            orders: Mutex::new(HashMap::new()),
        }
    }

    /// Generate a unique execution ID
    fn generate_execution_id(&self) -> ExecutionId {
        let seq = self.counter.fetch_add(1, Ordering::SeqCst);
        ExecutionId(format!("{}-exec-{}", self.config.id_prefix, seq))
    }

    /// Generate a fixed execution ID (for backward compatibility)
    fn generate_fixed_id(&self) -> ExecutionId {
        ExecutionId(format!("{}-execution-id", self.config.id_prefix))
    }

    /// Determine if a request should succeed based on config
    fn should_succeed(&self, cmd: &OrderCmd) -> bool {
        // Check specific success keys first
        if let Some(ref client_order_id) = cmd.client_order_id {
            if self.config.success_keys.iter().any(|k| k == client_order_id) {
                return true;
            }
        }
        if let Some(ref correlation_id) = cmd.correlation_id {
            if self.config.success_keys.iter().any(|k| k == correlation_id) {
                return true;
            }
        }

        // Check specific failure keys
        if let Some(ref client_order_id) = cmd.client_order_id {
            if self.config.failure_keys.iter().any(|k| k == client_order_id) {
                return false;
            }
        }
        if let Some(ref correlation_id) = cmd.correlation_id {
            if self.config.failure_keys.iter().any(|k| k == correlation_id) {
                return false;
            }
        }

        // Use global success rate
        rand::random::<f64>() < self.config.global_success_rate
    }

    /// Apply configured latency
    async fn apply_latency(&self) {
        if self.config.latency_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.config.latency_ms)).await;
        }
    }

    /// Get the number of tracked orders
    pub fn order_count(&self) -> usize {
        self.orders.lock().unwrap().len()
    }

    /// Check if an order is tracked
    pub fn has_order(&self, execution_id: &ExecutionId) -> bool {
        self.orders.lock().unwrap().contains_key(&execution_id.0)
    }

    /// Clear all tracked orders
    pub fn clear_orders(&self) {
        self.orders.lock().unwrap().clear();
    }

    /// Get the current configuration
    pub fn config(&self) -> &MockAdapterConfig {
        &self.config
    }
}

impl Default for MockAdapter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IExecutionPort for MockAdapter {
    type Error = BrokerError;

    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, Self::Error> {
        self.apply_latency().await;

        // Determine if this order should succeed
        if !self.should_succeed(&cmd) {
            return Err(BrokerError::Unknown(
                "Mock failure: order rejected".to_string()
            ));
        }

        // Generate execution ID
        let execution_id = if self.config.unique_ids {
            self.generate_execution_id()
        } else {
            self.generate_fixed_id()
        };

        // Track the order
        let tracked = TrackedOrder {
            execution_id: execution_id.clone(),
            symbol: cmd.symbol.clone(),
            side: cmd.side.clone(),
            qty: cmd.qty,
            status: "new".to_string(),
        };

        self.orders.lock().unwrap().insert(execution_id.0.clone(), tracked);

        Ok(execution_id)
    }

    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), Self::Error> {
        self.apply_latency().await;

        let mut orders = self.orders.lock().unwrap();
        
        if let Some(order) = orders.get_mut(&cmd.execution_id.0) {
            order.status = "canceled".to_string();
            Ok(())
        } else if self.config.allow_untracked_operations {
            // For backward compatibility, succeed even for untracked orders
            Ok(())
        } else {
            Err(BrokerError::Unknown(
                format!("Mock: Order {} not found", cmd.execution_id.0)
            ))
        }
    }

    async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), Self::Error> {
        self.apply_latency().await;

        let mut orders = self.orders.lock().unwrap();
        
        if let Some(order) = orders.get_mut(&cmd.execution_id.0) {
            order.status = "replaced".to_string();
            Ok(())
        } else if self.config.allow_untracked_operations {
            // For backward compatibility, succeed even for untracked orders
            Ok(())
        } else {
            Err(BrokerError::Unknown(
                format!("Mock: Order {} not found", cmd.execution_id.0)
            ))
        }
    }

    async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, Self::Error> {
        self.apply_latency().await;

        let orders = self.orders.lock().unwrap();
        
        if let Some(order) = orders.get(&query.execution_id.0) {
            Ok(OrderStatusResponse::new(
                query.execution_id.0.clone(),
                &order.status,
                &order.symbol,
                order.side.clone(),
                order.qty,
            ))
        } else {
            // Return a default response for unknown orders (backward compatibility)
            Ok(OrderStatusResponse::new(
                query.execution_id.0.clone(),
                "new",
                "MOCK",
                OrderSide::Buy,
                100,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_order(symbol: &str, client_order_id: Option<&str>) -> OrderCmd {
        OrderCmd {
            symbol: symbol.to_string(),
            qty: 100,
            side: OrderSide::Buy,
            order_type: crate::core::domain::order::OrderType::Market,
            time_in_force: crate::core::domain::order::TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: client_order_id.map(|s| s.to_string()),
            extended_hours: false,
            notional: None,
            correlation_id: None,
        }
    }

    #[tokio::test]
    async fn test_unique_execution_ids() {
        let adapter = MockAdapter::new();

        let order1 = create_test_order("AAPL", None);
        let order2 = create_test_order("GOOGL", None);
        let order3 = create_test_order("MSFT", None);

        let id1 = adapter.submit_order(order1).await.unwrap();
        let id2 = adapter.submit_order(order2).await.unwrap();
        let id3 = adapter.submit_order(order3).await.unwrap();

        // IDs should be unique
        assert_ne!(id1.0, id2.0);
        assert_ne!(id2.0, id3.0);
        assert_ne!(id1.0, id3.0);

        // IDs should follow expected pattern
        assert!(id1.0.starts_with("mock-exec-"));
        assert!(id2.0.starts_with("mock-exec-"));
        assert!(id3.0.starts_with("mock-exec-"));
    }

    #[tokio::test]
    async fn test_sequential_ids() {
        let adapter = MockAdapter::new();

        let order1 = create_test_order("AAPL", None);
        let order2 = create_test_order("GOOGL", None);

        let id1 = adapter.submit_order(order1).await.unwrap();
        let id2 = adapter.submit_order(order2).await.unwrap();

        // Extract sequence numbers
        let seq1: u64 = id1.0.split("-").last().unwrap().parse().unwrap();
        let seq2: u64 = id2.0.split("-").last().unwrap().parse().unwrap();

        // Should be sequential
        assert_eq!(seq2, seq1 + 1);
    }

    #[tokio::test]
    async fn test_fixed_id_mode() {
        let config = MockAdapterConfig::default()
            .with_sequential_ids();
        let adapter = MockAdapter::with_config(config);

        let order1 = create_test_order("AAPL", None);
        let order2 = create_test_order("GOOGL", None);

        let id1 = adapter.submit_order(order1).await.unwrap();
        let id2 = adapter.submit_order(order2).await.unwrap();

        // Both should return the same fixed ID
        assert_eq!(id1.0, "mock-execution-id");
        assert_eq!(id2.0, "mock-execution-id");
    }

    #[tokio::test]
    async fn test_custom_prefix() {
        let config = MockAdapterConfig::default()
            .with_prefix("test");
        let adapter = MockAdapter::with_config(config);

        let order = create_test_order("AAPL", None);
        let id = adapter.submit_order(order).await.unwrap();

        assert!(id.0.starts_with("test-exec-"));
    }

    #[tokio::test]
    async fn test_order_tracking() {
        let adapter = MockAdapter::new();

        let order = create_test_order("AAPL", None);
        let id = adapter.submit_order(order).await.unwrap();

        // Order should be tracked
        assert!(adapter.has_order(&id));
        assert_eq!(adapter.order_count(), 1);

        // Query status should return tracked order info
        let status = adapter.query_status(StatusQuery {
            execution_id: id.clone(),
            correlation_id: None,
        }).await.unwrap();

        assert_eq!(status.execution_id, id.0);
        assert_eq!(status.symbol, "AAPL");
    }

    #[tokio::test]
    async fn test_cancel_order() {
        let adapter = MockAdapter::new();

        let order = create_test_order("AAPL", None);
        let id = adapter.submit_order(order).await.unwrap();

        // Cancel the order
        let result = adapter.cancel_order(CancelCmd {
            execution_id: id.clone(),
            symbol: "AAPL".to_string(),
            correlation_id: None,
        }).await;

        assert!(result.is_ok());

        // Status should be updated
        let status = adapter.query_status(StatusQuery {
            execution_id: id.clone(),
            correlation_id: None,
        }).await.unwrap();

        assert_eq!(status.status, "canceled");
    }

    #[tokio::test]
    async fn test_cancel_nonexistent_order() {
        let config = MockAdapterConfig::default()
            .require_tracked_orders();
        let adapter = MockAdapter::with_config(config);

        let result = adapter.cancel_order(CancelCmd {
            execution_id: ExecutionId("nonexistent".to_string()),
            symbol: "AAPL".to_string(),
            correlation_id: None,
        }).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_replace_order() {
        let adapter = MockAdapter::new();

        let order = create_test_order("AAPL", None);
        let id = adapter.submit_order(order).await.unwrap();

        // Replace the order
        let result = adapter.replace_order(ReplaceCmd {
            execution_id: id.clone(),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            qty: Some(200),
            limit_price: Some(150.0),
            correlation_id: None,
        }).await;

        assert!(result.is_ok());

        // Status should be updated
        let status = adapter.query_status(StatusQuery {
            execution_id: id.clone(),
            correlation_id: None,
        }).await.unwrap();

        assert_eq!(status.status, "replaced");
    }

    #[tokio::test]
    async fn test_failure_keys() {
        let config = MockAdapterConfig::default()
            .with_failure_key("fail-this");
        let adapter = MockAdapter::with_config(config);

        let fail_order = create_test_order("AAPL", Some("fail-this"));
        let success_order = create_test_order("GOOGL", Some("succeed-this"));

        let result1 = adapter.submit_order(fail_order).await;
        let result2 = adapter.submit_order(success_order).await;

        assert!(result1.is_err());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_always_fail_config() {
        let config = MockAdapterConfig::always_fail();
        let adapter = MockAdapter::with_config(config);

        let order = create_test_order("AAPL", None);
        let result = adapter.submit_order(order).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_always_succeed_config() {
        let config = MockAdapterConfig::always_succeed();
        let adapter = MockAdapter::with_config(config);

        let order = create_test_order("AAPL", None);
        let result = adapter.submit_order(order).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_success_rate() {
        // With 0.0 success rate, all orders should fail
        let config = MockAdapterConfig::with_success_rate(0.0);
        let adapter = MockAdapter::with_config(config);

        let order = create_test_order("AAPL", None);
        let result = adapter.submit_order(order).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_clear_orders() {
        let adapter = MockAdapter::new();

        let order1 = create_test_order("AAPL", None);
        let order2 = create_test_order("GOOGL", None);

        let id1 = adapter.submit_order(order1).await.unwrap();
        let id2 = adapter.submit_order(order2).await.unwrap();

        assert_eq!(adapter.order_count(), 2);

        adapter.clear_orders();

        assert_eq!(adapter.order_count(), 0);
        assert!(!adapter.has_order(&id1));
        assert!(!adapter.has_order(&id2));
    }

    #[tokio::test]
    async fn test_latency() {
        let config = MockAdapterConfig::default()
            .with_latency(50);
        let adapter = MockAdapter::with_config(config);

        let start = std::time::Instant::now();
        let order = create_test_order("AAPL", None);
        let _ = adapter.submit_order(order).await;
        let elapsed = start.elapsed();

        // Should have taken at least 50ms
        assert!(elapsed >= std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_concurrent_orders_unique_ids() {
        use std::sync::Arc;
        use tokio::task;

        let adapter = Arc::new(MockAdapter::new());
        let mut handles = vec![];

        // Submit 100 orders concurrently
        for i in 0..100 {
            let adapter_clone = Arc::clone(&adapter);
            let handle = task::spawn(async move {
                let order = create_test_order(&format!("SYM{}", i), None);
                adapter_clone.submit_order(order).await.unwrap()
            });
            handles.push(handle);
        }

        // Collect all IDs
        let mut ids = vec![];
        for handle in handles {
            ids.push(handle.await.unwrap().0);
        }

        // All IDs should be unique
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 100);
    }

    #[tokio::test]
    async fn test_query_status_backward_compatibility() {
        // Querying an untracked order should still return a response
        let adapter = MockAdapter::new();

        let status = adapter.query_status(StatusQuery {
            execution_id: ExecutionId("untracked-id".to_string()),
            correlation_id: None,
        }).await.unwrap();

        assert_eq!(status.execution_id, "untracked-id");
        assert_eq!(status.status, "new");
    }
}
