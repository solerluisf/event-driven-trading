// core/application/order_submission_service.rs
//
// Kill switch, circuit breaker, and rate limiter are now wired in here
// so every broker call is guarded before it leaves the core.

use std::sync::Arc;

use crate::core::application::validator::RequestValidator;
use crate::core::application::idempotency::IdempotencyStore;
use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId};
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
        Self {
            validator,
            idempotency,
            execution_port,
            kill_switch,
            rate_limiter,
            circuit_breaker,
            broker_id: broker_id.into(),
        }
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
            Ok(_)  => self.circuit_breaker.record_success(),
            Err(e) => self.circuit_breaker.record_failure(e),
        }

        let execution_id = result?;
        self.idempotency.mark_processed(idem_key, execution_id.0.clone());
        Ok(execution_id)
    }

    pub async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), BrokerError> {
        self.pre_flight()?;

        let result = self.execution_port.cancel_order(cmd).await;
        match &result {
            Ok(_)  => self.circuit_breaker.record_success(),
            Err(e) => self.circuit_breaker.record_failure(e),
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

    pub async fn query_status(&self, query: StatusQuery) -> Result<(), BrokerError> {
        self.pre_flight()?;

        let result = self.execution_port.query_status(query).await;
        match &result {
            Ok(_)  => self.circuit_breaker.record_success(),
            Err(e) => self.circuit_breaker.record_failure(e),
        }
        result
    }
}