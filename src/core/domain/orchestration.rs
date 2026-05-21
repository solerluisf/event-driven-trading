// core/domain/orchestration.rs
//
// Domain types for the Orchestrator-to-Gateway control plane.
// Maps to the Orchestrator's ServiceCommand enum.

use serde::{Deserialize, Serialize};

use super::operation_mode::OperationMode;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "command", content = "payload", rename_all = "snake_case")]
pub enum OrchestrationCommand {
    SetOperationMode(OperationMode),
    ActivateKillSwitch { reason: String, actor: String },
    ClearKillSwitch { reason: String, actor: String },
    ReloadPolicies,
    UpdateCircuitBreaker {
        failure_threshold: u32,
        cooldown_secs: u64,
    },
    ResetCircuitBreaker,
    UpdateRateLimiter {
        max_requests_per_min: u32,
        burst_capacity: u32,
    },
    PauseSymbol { symbol: String },
    ResumeSymbol { symbol: String },
    FlushPendingOrders,
    SetLogLevel { level: String },
    HealthCheck,
}

impl OrchestrationCommand {
    pub fn command_type(&self) -> &'static str {
        match self {
            Self::SetOperationMode(_) => "set_operation_mode",
            Self::ActivateKillSwitch { .. } => "activate_kill_switch",
            Self::ClearKillSwitch { .. } => "clear_kill_switch",
            Self::ReloadPolicies => "reload_policies",
            Self::UpdateCircuitBreaker { .. } => "update_circuit_breaker",
            Self::ResetCircuitBreaker => "reset_circuit_breaker",
            Self::UpdateRateLimiter { .. } => "update_rate_limiter",
            Self::PauseSymbol { .. } => "pause_symbol",
            Self::ResumeSymbol { .. } => "resume_symbol",
            Self::FlushPendingOrders => "flush_pending_orders",
            Self::SetLogLevel { .. } => "set_log_level",
            Self::HealthCheck => "health_check",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestrationAck {
    pub command_type: String,
    pub success: bool,
    pub error: Option<String>,
    pub timestamp_ms: u64,
}

impl OrchestrationAck {
    pub fn success(command_type: &str) -> Self {
        Self {
            command_type: command_type.to_string(),
            success: true,
            error: None,
            timestamp_ms: current_time_ms(),
        }
    }

    pub fn failure(command_type: &str, error: impl Into<String>) -> Self {
        Self {
            command_type: command_type.to_string(),
            success: false,
            error: Some(error.into()),
            timestamp_ms: current_time_ms(),
        }
    }
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_type_names() {
        let cmd = OrchestrationCommand::HealthCheck;
        assert_eq!(cmd.command_type(), "health_check");

        let cmd = OrchestrationCommand::ReloadPolicies;
        assert_eq!(cmd.command_type(), "reload_policies");

        let cmd = OrchestrationCommand::ResetCircuitBreaker;
        assert_eq!(cmd.command_type(), "reset_circuit_breaker");

        let cmd = OrchestrationCommand::FlushPendingOrders;
        assert_eq!(cmd.command_type(), "flush_pending_orders");
    }

    #[test]
    fn test_ack_success() {
        let ack = OrchestrationAck::success("health_check");
        assert!(ack.success);
        assert_eq!(ack.command_type, "health_check");
        assert!(ack.error.is_none());
        assert!(ack.timestamp_ms > 0);
    }

    #[test]
    fn test_ackfailure() {
        let ack = OrchestrationAck::failure("set_operation_mode", "invalid mode");
        assert!(!ack.success);
        assert_eq!(ack.command_type, "set_operation_mode");
        assert_eq!(ack.error, Some("invalid mode".to_string()));
    }

    #[test]
    fn test_serialize_deserialize_command() {
        let cmd = OrchestrationCommand::ActivateKillSwitch {
            reason: "manual override".to_string(),
            actor: "admin".to_string(),
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        let decoded: OrchestrationCommand = serde_json::from_str(&json).expect("deserialize");
        assert!(matches!(decoded, OrchestrationCommand::ActivateKillSwitch { .. }));
    }

    #[test]
    fn test_serialize_deserialize_ack() {
        let ack = OrchestrationAck::success("reload_policies");
        let json = serde_json::to_string(&ack).expect("serialize");
        let decoded: OrchestrationAck = serde_json::from_str(&json).expect("deserialize");
        assert!(decoded.success);
        assert_eq!(decoded.command_type, "reload_policies");
    }
}
