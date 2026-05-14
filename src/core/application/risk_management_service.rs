// core/application/risk_management_service.rs

use std::sync::Arc;
use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::adapters::broker::broker_error::BrokerError;

pub struct RiskManagementService {
    kill_switch: KillSwitch,
    rate_limiter: Arc<RateLimiterManager>,
}

impl RiskManagementService {
    pub fn new(
        kill_switch: KillSwitch,
        rate_limiter: Arc<RateLimiterManager>,
    ) -> Self {
        Self {
            kill_switch,
            rate_limiter,
        }
    }

    /// Performs pre-flight risk checks before order execution.
    ///
    /// # Checks Performed
    /// 1. Kill switch - rejects if active
    /// 2. Rate limiter - rejects if bucket exhausted
    ///
    /// # Known Limitation: TOCTOU Race Window
    /// This check is performed at the gateway layer, but the actual execution
    /// happens asynchronously after journaling and other processing. There is
    /// a theoretical window where:
    /// 1. This check passes (kill switch inactive)
    /// 2. Kill switch gets activated by another thread
    /// 3. Order proceeds to execution anyway
    ///
    /// This is an accepted trade-off for performance. The kill switch cancellation
    /// task will catch and cancel any orders that slip through this window.
    /// For stronger guarantees, use OrderSubmissionService::submit_order which
    /// performs a second check right before broker submission.
    pub fn check(&self, broker_id: &str, tokens: u32) -> Result<(), BrokerError> {
        // check kill switch first
        if self.kill_switch.is_enabled() {
            return Err(BrokerError::Unknown("kill switch is active".into()));
        }

        // check rate limit
        if !self.rate_limiter.allow(broker_id, tokens) {
            return Err(BrokerError::RateLimited);
        }

        Ok(())
    }

    pub fn activate_kill_switch(&self) {
        self.kill_switch.enable();
    }

    pub fn deactivate_kill_switch(&self) {
        self.kill_switch.disable();
    }
}