// core/application/risk_management_service.rs

use crate::core::application::kill_switch::KillSwitch;
use crate::core::application::rate_limiter::RateLimiterManager;
use crate::adapters::broker::broker_error::BrokerError;

pub struct RiskManagementService {
    kill_switch: KillSwitch,
    rate_limiter: RateLimiterManager,
}

impl RiskManagementService {
    pub fn new(
        kill_switch: KillSwitch,
        rate_limiter: RateLimiterManager,
    ) -> Self {
        Self {
            kill_switch,
            rate_limiter,
        }
    }

    // call this before every order submission
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