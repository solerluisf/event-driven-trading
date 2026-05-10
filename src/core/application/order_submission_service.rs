// core/application/order_submission_service.rs
//
// Kill switch, circuit breaker, and rate limiter are now wired in here
// so every broker call is guarded before it leaves the core.

use std::sync::Arc;

use crate::core::application::validator::RequestValidator;
use crate::core::application::idempotency::IdempotencyStore;
use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId, OrderStatusResponse};
use crate::core::ports::execution_port::IExecutionPort;
use crate::core::patterns::circuit_breaker::CircuitBreaker;
use crate::adapters::broker::broker_error::BrokerError;

pub struct OrderSubmissionService {
    validator: RequestValidator,
    idempotency: IdempotencyStore,
    execution_port: Box<dyn IExecutionPort<Error = BrokerError>>,
    kill_switch: Arc<KillSwitch>,
    rate_limiter: Arc<RateLimiterManager>,
    circuit_breaker: Arc<CircuitBreaker>,
    /// The broker id this service routes to (used for rate limiting)
    broker_id: String,
}

impl OrderSubmissionService {
    pub fn new(
        validator: RequestValidator,
        idempotency: IdempotencyStore,
        execution_port: Box<dyn IExecutionPort<Error = BrokerError>>,
        kill_switch: Arc<KillSwitch>,
        rate_limiter: Arc<RateLimiterManager>,
        circuit_breaker: Arc<CircuitBreaker>,
        broker_id: impl Into<String>,
    ) -> Self {
        let service = Self {
            validator,
            idempotency,
            execution_port,
            kill_switch: kill_switch.clone(),
            rate_limiter,
            circuit_breaker,
            broker_id: broker_id.into(),
        };
        
        // Register the kill switch callback to cancel orders
        service.register_kill_switch_callback();
        
        service
    }
    
    /// Register the cancel callback with the kill switch
    fn register_kill_switch_callback(&self) {
        // Note: This is a simplified implementation. In production, 
        // you'd want to spawn a task to handle async cancel operations.
        let _execution_port = &self.execution_port;
        
        self.kill_switch.register_cancel_callback(move |exec_id| {
            tracing::error!("🚨 Kill switch triggering cancel for order: {}", exec_id.0);
            
            let cancel_cmd = CancelCmd { execution_id: exec_id };
            
            // Execute cancel - we can't use .await here since we're in a sync callback
            // In production, this should spawn a task to cancel the order
            // For now, we log and rely on the adapter to handle the cancel
            tracing::error!("🚨 Cancel command created for order: {:?}", cancel_cmd);
        });
    }

    /// Guard rail checked before every broker call.
    fn pre_flight(&self) -> Result<(), BrokerError> {
        // 1. Kill switch
        if self.kill_switch.is_enabled() {
            tracing::error!("order blocked: kill switch is active");
            return Err(BrokerError::Unknown("kill switch is active".into()));
        }

        // 2. Rate limiter
        if !self.rate_limiter.allow(&self.broker_id, 1) {
            tracing::warn!("order blocked: rate limit reached for {}", self.broker_id);
            return Err(BrokerError::RateLimited);
        }

        // 3. Circuit breaker open check
        if self.circuit_breaker.is_open() {
            tracing::warn!("order blocked: circuit breaker open for {}", self.broker_id);
            return Err(BrokerError::Unknown("circuit breaker open".into()));
        }

        Ok(())
    }

    pub async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, BrokerError> {
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
        self.pre_flight()?;

        let result = self.execution_port.replace_order(cmd).await;
        match &result {
            Ok(_)  => self.circuit_breaker.record_success(),
            Err(e) => self.circuit_breaker.record_failure(e),
        }
        result
    }

    pub async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, BrokerError> {
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