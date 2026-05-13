// core/application/order_submission_service.rs
//
// Kill switch, circuit breaker, and rate limiter are now wired in here
// so every broker call is guarded before it leaves the core.

use std::sync::Arc;

use crate::core::application::validator::RequestValidator;
use crate::core::application::idempotency::IdempotencyStore;
use crate::core::application::kill_switch::KillSwitch;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId, OrderStatusResponse};
use crate::core::ports::execution_port::IExecutionPort;
use crate::core::patterns::circuit_breaker::CircuitBreaker;
use crate::adapters::broker::broker_error::BrokerError;

pub struct OrderSubmissionService {
    validator: RequestValidator,
    idempotency: IdempotencyStore,
    execution_port: Arc<dyn IExecutionPort<Error = BrokerError> + Send + Sync>,
    kill_switch: Arc<KillSwitch>,
    circuit_breaker: Arc<CircuitBreaker>,
    broker_id: String,
}

impl OrderSubmissionService {
    pub fn new(
        validator: RequestValidator,
        idempotency: IdempotencyStore,
        execution_port: Box<dyn IExecutionPort<Error = BrokerError> + Send + Sync>,
        kill_switch: Arc<KillSwitch>,
        circuit_breaker: Arc<CircuitBreaker>,
        broker_id: impl Into<String>,
    ) -> Self {
        // Convert Box to Arc so we can share it with the kill switch cancel task
        let execution_port: Arc<dyn IExecutionPort<Error = BrokerError> + Send + Sync> = 
            box_to_arc(execution_port);
        
        // Register a cancel channel and spawn a task to process kill switch cancels
        let mut cancel_rx = kill_switch.register_cancel_channel();
        let exec_port_clone = execution_port.clone();
        let circuit_breaker_clone = circuit_breaker.clone();
        
        // Only spawn the task if we're in a Tokio runtime context
        // This allows the service to be created in non-async contexts (like some tests)
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                tracing::info!("Kill switch cancel processor started");
                
                while let Some(exec_id) = cancel_rx.recv().await {
                    let exec_id_str = exec_id.0.clone();
                    tracing::error!("🚨 Kill switch processing cancel for order: {}", exec_id_str);
                    
                    let cancel_cmd = CancelCmd {
                        execution_id: exec_id,
                        correlation_id: Some("kill_switch".to_string()),
                    };
                    
                    // Actually send the cancel to the broker
                    match exec_port_clone.cancel_order(cancel_cmd).await {
                        Ok(_) => {
                            tracing::info!("Kill switch successfully cancelled order: {}", exec_id_str);
                            circuit_breaker_clone.record_success();
                        }
                        Err(e) => {
                            tracing::error!("Kill switch failed to cancel order {}: {:?}", exec_id_str, e);
                            circuit_breaker_clone.record_failure(&e);
                        }
                    }
                }
                
                tracing::warn!("Kill switch cancel processor shutting down - channel closed");
            });
        } else {
            tracing::warn!("No Tokio runtime available - kill switch cancel task not spawned");
        }
        
        Self {
            validator,
            idempotency,
            execution_port,
            kill_switch,
            circuit_breaker,
            broker_id: broker_id.into(),
        }
    }

    /// Execution-level guard rail checked before every broker call.
    /// Kill switch and rate limiter checks are handled by GatewayService
    /// via RiskManagementService to avoid double-checking and double token consumption.
    fn pre_flight(&self) -> Result<(), BrokerError> {
        // Circuit breaker open check — this is an execution-layer concern
        // protecting the broker connection from cascading failures.
        if self.circuit_breaker.is_open() {
            tracing::warn!("order blocked: circuit breaker open for {}", self.broker_id);
            return Err(BrokerError::Unknown("circuit breaker open".into()));
        }

        Ok(())
    }

    pub async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, BrokerError> {
        // Validate order parameters
        self.validator.validate_order(&cmd)?;

        // Idempotency key: prefer client_order_id, fall back to symbol
        let idem_key = cmd.client_order_id.clone().unwrap_or_else(|| cmd.symbol.clone());

        if self.idempotency.is_processed(&idem_key) {
            tracing::warn!("duplicate order ignored key={}", idem_key);
            return Err(BrokerError::Unknown("duplicate order".into()));
        }

        self.pre_flight()?;

        // Route through circuit breaker
        // Note: async closures aren't stable, so we drive the future outside
        // the cb.call() wrapper and record success/failure manually.
        let result = self.execution_port.submit_order(cmd.clone()).await;

        match &result {
            Ok(exec_id) => {
                self.circuit_breaker.record_success();
                // Track the order as open for kill switch monitoring
                self.kill_switch.track_open_order(exec_id);
                tracing::debug!("Order submitted and tracked: {}", exec_id.0);
            }
            Err(e) => {
                self.circuit_breaker.record_failure(e);
            }
        }

        let execution_id = result?;
        self.idempotency.mark_processed(idem_key, execution_id.0.clone());
        Ok(execution_id)
    }

    pub async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError> {
        // Validate cancel parameters
        self.validator.validate_cancel(&cmd)?;

        self.pre_flight()?;

        let result = self.execution_port.cancel_order(cmd.clone()).await;
        match &result {
            Ok(_) => {
                self.circuit_breaker.record_success();
                // Remove order from tracking
                self.kill_switch.remove_open_order(&cmd.execution_id);
                tracing::debug!("Order cancelled and removed from tracking: {}", cmd.execution_id.0);
            }
            Err(e) => {
                self.circuit_breaker.record_failure(e);
            }
        }
        result
    }

    pub async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), BrokerError> {
        // Validate replace parameters
        self.validator.validate_replace(&cmd)?;

        self.pre_flight()?;

        let result = self.execution_port.replace_order(cmd).await;
        match &result {
            Ok(_)  => self.circuit_breaker.record_success(),
            Err(e) => self.circuit_breaker.record_failure(e),
        }
        result
    }

    pub async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, BrokerError> {
        // Validate query parameters
        self.validator.validate_query(&query)?;

        self.pre_flight()?;

        let result = self.execution_port.query_status(query).await;
        match &result {
            Ok(_)  => self.circuit_breaker.record_success(),
            Err(e) => self.circuit_breaker.record_failure(e),
        }
        result
    }

    /// Mark an order as filled - removes it from kill switch tracking
    /// 
    /// # Arguments
    /// * `execution_id` - The execution ID of the filled order
    /// 
    /// This should be called when a fill event is received from the broker
    pub fn mark_order_filled(&self, execution_id: &ExecutionId) {
        if self.kill_switch.remove_open_order(execution_id) {
            tracing::debug!("Order marked as filled and removed from tracking: {}", execution_id.0);
        }
    }

    /// Get the number of open orders being tracked
    pub fn open_order_count(&self) -> usize {
        self.kill_switch.open_order_count()
    }

    /// Check if a specific order is being tracked as open
    pub fn is_order_open(&self, execution_id: &ExecutionId) -> bool {
        self.kill_switch.is_order_tracked(execution_id)
    }
}

/// Helper function to convert Box<dyn Trait> to Arc<dyn Trait>
fn box_to_arc(
    b: Box<dyn IExecutionPort<Error = BrokerError> + Send + Sync>,
) -> Arc<dyn IExecutionPort<Error = BrokerError> + Send + Sync> {
    Arc::from(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::application::validator::RequestValidator;
    use crate::core::application::idempotency::IdempotencyStore;
    use crate::core::domain::order::{OrderSide, OrderType, TimeInForce};
    use crate::core::ports::observability::IObservability;
    use crate::core::patterns::circuit_breaker::CircuitBreaker;
    use std::sync::{Mutex, Arc as StdArc};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use async_trait::async_trait;

    // Mock observability for testing
    #[derive(Clone)]
    struct MockObservability;
    impl IObservability for MockObservability {
        fn emit(&self, _msg: String) {}
    }

    // Mock execution port that tracks cancel calls
    struct MockExecutionPort {
        cancel_calls: Arc<Mutex<Vec<String>>>,
        order_counter: AtomicU64,
    }

    impl MockExecutionPort {
        fn new() -> Self {
            Self {
                cancel_calls: Arc::new(Mutex::new(Vec::new())),
                order_counter: AtomicU64::new(1),
            }
        }

        fn get_cancel_calls(&self) -> Vec<String> {
            self.cancel_calls.lock().unwrap().clone()
        }

        fn next_order_id(&self) -> String {
            format!("test-order-{}", self.order_counter.fetch_add(1, Ordering::SeqCst))
        }
    }

    // Wrapper that implements IExecutionPort and delegates to the mock
    // This allows us to share the mock between the service and test assertions
    struct MockPortWrapper {
        inner: Arc<MockExecutionPort>,
    }

    impl MockPortWrapper {
        fn new(inner: Arc<MockExecutionPort>) -> Self {
            Self { inner }
        }
    }

    #[async_trait]
    impl IExecutionPort for MockPortWrapper {
        type Error = BrokerError;

        async fn submit_order(&self, _cmd: OrderCmd) -> Result<ExecutionId, BrokerError> {
            Ok(ExecutionId(self.inner.next_order_id()))
        }

        async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError> {
            self.inner.cancel_calls.lock().unwrap().push(cmd.execution_id.0.clone());
            Ok(())
        }

        async fn replace_order(&self, _cmd: ReplaceCmd) -> Result<(), BrokerError> {
            Ok(())
        }

        async fn query_status(&self, _query: StatusQuery) -> Result<OrderStatusResponse, BrokerError> {
            Ok(OrderStatusResponse::new(
                "test".to_string(),
                "FILLED".to_string(),
                "AAPL".to_string(),
                OrderSide::Buy,
                100,
            ))
        }
    }

    fn create_test_order(symbol: &str) -> OrderCmd {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        
        OrderCmd {
            symbol: symbol.to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            qty: 100,
            limit_price: None,
            stop_price: None,
            time_in_force: TimeInForce::Day,
            client_order_id: Some(format!("test-client-id-{}", id)),
            extended_hours: false,
            notional: None,
            correlation_id: None,
        }
    }

    fn make_service() -> (OrderSubmissionService, Arc<MockExecutionPort>) {
        let validator = RequestValidator::default();
        let idempotency = IdempotencyStore::default();
        let mock_port: Arc<MockExecutionPort> = Arc::new(MockExecutionPort::new());
        let kill_switch = Arc::new(KillSwitch::new());
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            "test-broker",
            5,
            30,
            Arc::new(MockObservability),
        ));

        // Create a wrapper that implements IExecutionPort and delegates to mock_port
        let service = OrderSubmissionService::new(
            validator,
            idempotency,
            Box::new(MockPortWrapper::new(mock_port.clone())),
            kill_switch,
            circuit_breaker,
            "test-broker",
        );

        (service, mock_port)
    }

    #[tokio::test]
    async fn kill_switch_cancels_open_orders() {
        let (service, mock_port) = make_service();

        // Submit an order to track it
        let order = create_test_order("AAPL");
        let exec_id = service.submit_order(order).await.unwrap();
        
        // Verify order is tracked
        assert!(service.is_order_open(&exec_id));
        assert_eq!(service.open_order_count(), 1);

        // Activate kill switch - this should send cancel commands
        let cancelled_count = service.kill_switch.enable();
        assert_eq!(cancelled_count, 1);

        // Wait for the cancel task to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify the cancel was actually called on the mock port
        let cancel_calls = mock_port.get_cancel_calls();
        assert_eq!(cancel_calls.len(), 1);
        assert_eq!(cancel_calls[0], exec_id.0);
    }

    #[tokio::test]
    async fn kill_switch_cancels_multiple_orders() {
        let (service, mock_port) = make_service();

        // Submit multiple orders
        let order1 = create_test_order("AAPL");
        let order2 = create_test_order("TSLA");
        let order3 = create_test_order("GOOGL");
        
        let exec_id1 = service.submit_order(order1).await.expect("order1 should succeed");
        let exec_id2 = service.submit_order(order2).await.expect("order2 should succeed");
        let exec_id3 = service.submit_order(order3).await.expect("order3 should succeed");
        
        assert_eq!(service.open_order_count(), 3, "Expected 3 open orders, got {}", service.open_order_count());

        // Activate kill switch
        service.kill_switch.enable();

        // Wait for processing (longer for multiple orders)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify all orders were cancelled
        let cancel_calls = mock_port.get_cancel_calls();
        assert_eq!(cancel_calls.len(), 3, "Expected 3 cancel calls, got: {:?}", cancel_calls);
        
        // Verify all execution IDs were cancelled
        let cancelled_ids: std::collections::HashSet<_> = cancel_calls.into_iter().collect();
        assert!(cancelled_ids.contains(&exec_id1.0));
        assert!(cancelled_ids.contains(&exec_id2.0));
        assert!(cancelled_ids.contains(&exec_id3.0));
    }

    #[tokio::test]
    async fn kill_switch_no_callback_when_no_orders() {
        let (service, mock_port) = make_service();

        // No orders submitted
        assert_eq!(service.open_order_count(), 0);

        // Activate kill switch
        let cancelled_count = service.kill_switch.enable();
        assert_eq!(cancelled_count, 0);

        // Wait and verify no cancels were called
        tokio::time::sleep(Duration::from_millis(50)).await;
        let cancel_calls = mock_port.get_cancel_calls();
        assert!(cancel_calls.is_empty());
    }
}
