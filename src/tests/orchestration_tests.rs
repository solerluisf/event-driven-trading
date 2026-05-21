// tests/orchestration_tests.rs
//
// Tests for the Orchestrator-Gateway integration.

#[cfg(test)]
mod orchestration_tests {
    use crate::core::application::kill_switch::KillSwitch;
    use crate::core::application::rate_limiter::RateLimiterManager;
    use crate::core::domain::gateway_health::GatewayHealthSnapshot;
    use crate::core::domain::orchestration::{OrchestrationAck, OrchestrationCommand};
    use crate::core::domain::operation_mode::OperationMode;
    use crate::core::patterns::circuit_breaker::CircuitBreaker;
    use crate::adapters::metrics::metrics_adapter::MetricsAdapter;
    use std::sync::Arc;

    struct NoopObservability;
    impl crate::core::ports::observability::IObservability for NoopObservability {
        fn emit(&self, _event: String) {}
    }

    fn noop_obs() -> Arc<dyn crate::core::ports::observability::IObservability + Send + Sync> {
        Arc::new(NoopObservability)
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Orchestration Command Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_orchestration_command_serialization() {
        let cmd = OrchestrationCommand::SetOperationMode(OperationMode::Live);
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("set_operation_mode"));
        assert!(json.contains("Live") || json.contains("live"));
    }

    #[test]
    fn test_orchestration_command_deserialization() {
        let json = r#"{"command":"health_check","payload":null}"#;
        let cmd: OrchestrationCommand = serde_json::from_str(json).expect("deserialize");
        assert!(matches!(cmd, OrchestrationCommand::HealthCheck));
    }

    #[test]
    fn test_activate_kill_switch_serialization() {
        let cmd = OrchestrationCommand::ActivateKillSwitch {
            reason: "system overload".to_string(),
            actor: "orchestrator".to_string(),
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("activate_kill_switch"));
        assert!(json.contains("system overload"));
        assert!(json.contains("orchestrator"));
    }

    #[test]
    fn test_update_circuit_breaker_serialization() {
        let cmd = OrchestrationCommand::UpdateCircuitBreaker {
            failure_threshold: 5,
            cooldown_secs: 60,
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("update_circuit_breaker"));
        assert!(json.contains("5"));
        assert!(json.contains("60"));
    }

    #[test]
    fn test_update_rate_limiter_serialization() {
        let cmd = OrchestrationCommand::UpdateRateLimiter {
            max_requests_per_min: 300,
            burst_capacity: 50,
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("update_rate_limiter"));
        assert!(json.contains("300"));
    }

    #[test]
    fn test_pause_symbol_serialization() {
        let cmd = OrchestrationCommand::PauseSymbol {
            symbol: "AAPL".to_string(),
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("pause_symbol"));
        assert!(json.contains("AAPL"));
    }

    #[test]
    fn test_resume_symbol_serialization() {
        let cmd = OrchestrationCommand::ResumeSymbol {
            symbol: "AAPL".to_string(),
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("resume_symbol"));
        assert!(json.contains("AAPL"));
    }

    #[test]
    fn test_set_log_level_serialization() {
        let cmd = OrchestrationCommand::SetLogLevel {
            level: "debug".to_string(),
        };
        let json = serde_json::to_string(&cmd).expect("serialize");
        assert!(json.contains("set_log_level"));
        assert!(json.contains("debug"));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // OrchestrationAck Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_ack_success_serialization() {
        let ack = OrchestrationAck::success("health_check");
        let json = serde_json::to_string(&ack).expect("serialize");
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("health_check"));
    }

    #[test]
    fn test_ackfailure_serialization() {
        let ack = OrchestrationAck::failure("set_operation_mode", "invalid mode");
        let json = serde_json::to_string(&ack).expect("serialize");
        assert!(json.contains("\"success\":false"));
        assert!(json.contains("invalid mode"));
    }

    #[test]
    fn test_ack_deserialization() {
        let json = r#"{"command_type":"reload_policies","success":true,"error":null,"timestamp_ms":1234567890}"#;
        let ack: OrchestrationAck = serde_json::from_str(json).expect("deserialize");
        assert!(ack.success);
        assert_eq!(ack.command_type, "reload_policies");
        assert!(ack.error.is_none());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Kill Switch Integration Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_kill_switch_enable_disable() {
        let ks = KillSwitch::new();
        assert!(!ks.is_enabled());

        ks.enable();
        assert!(ks.is_enabled());

        ks.disable();
        assert!(!ks.is_enabled());
    }

    #[test]
    fn test_kill_switch_enable_is_idempotent() {
        let ks = KillSwitch::new();
        ks.enable();
        let count1 = ks.enable();
        assert_eq!(count1, 0);
        assert!(ks.is_enabled());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Health Snapshot Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_health_snapshot_builder() {
        let snapshot = GatewayHealthSnapshot::new("broker_gateway")
            .with_kill_switch(false)
            .with_operation_mode("paper")
            .with_circuit_breaker_state("closed")
            .with_rate_limiter(180.0, 200.0)
            .with_symbols(vec!["AAPL".to_string(), "SPY".to_string()]);

        assert_eq!(snapshot.service_id, "broker_gateway");
        assert_eq!(snapshot.status, "healthy");
        assert!(!snapshot.kill_switch_active);
        assert_eq!(snapshot.operation_mode, "paper");
        assert_eq!(snapshot.circuit_breaker_state, "closed");
        assert_eq!(snapshot.rate_limiter_tokens_remaining, 180.0);
        assert_eq!(snapshot.rate_limiter_capacity, 200.0);
        assert_eq!(snapshot.symbols_subscribed.len(), 2);
    }

    #[test]
    fn test_health_snapshot_degraded_when_kill_switch_active() {
        let snapshot = GatewayHealthSnapshot::new("broker_gateway")
            .with_kill_switch(true);

        assert_eq!(snapshot.status, "degraded");
        assert!(snapshot.kill_switch_active);
    }

    #[test]
    fn test_health_snapshot_json_serialization() {
        let snapshot = GatewayHealthSnapshot::new("broker_gateway")
            .with_kill_switch(false)
            .with_operation_mode("paper");

        let json = serde_json::to_string(&snapshot).expect("serialize");
        let decoded: serde_json::Value = serde_json::from_str(&json).expect("deserialize json");

        assert_eq!(decoded["service_id"], "broker_gateway");
        assert_eq!(decoded["status"], "healthy");
        assert_eq!(decoded["kill_switch_active"], false);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Circuit Breaker Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_circuit_breaker_initial_state() {
        let cb = CircuitBreaker::new("test", 3, 30, noop_obs());
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new("test", 2, 30, noop_obs());
        cb.record_failure(&"error1");
        assert!(!cb.is_open());
        cb.record_failure(&"error2");
        assert!(cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_closes_on_success() {
        let cb = CircuitBreaker::new("test", 2, 1, noop_obs());
        cb.record_failure(&"error1");
        cb.record_success();
        assert!(!cb.is_open());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Rate Limiter Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_rate_limiter_allows_within_limit() {
        let rl = RateLimiterManager::new(100.0);
        assert!(rl.allow("test", 1));
    }

    #[test]
    fn test_rate_limiter_registers_custom_rpm() {
        let rl = RateLimiterManager::new(100.0);
        rl.register("custom", 50.0);
        assert!(rl.allow("custom", 1));
    }

    #[test]
    fn test_rate_limiter_tokens_and_capacity() {
        let rl = RateLimiterManager::new(200.0);
        rl.register("test", 200.0);
        let tokens = rl.tokens_remaining("test");
        let capacity = rl.get_capacity("test");
        assert!(tokens > 0.0);
        assert_eq!(capacity, 200.0);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Metrics Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_metrics_adapter_orch_command_counters() {
        let metrics = MetricsAdapter::default();
        assert_eq!(metrics.orchestrator_commands_received.load(std::sync::atomic::Ordering::Relaxed), 0);

        metrics.record_orch_command_received();
        metrics.record_orch_command_received();
        assert_eq!(metrics.orchestrator_commands_received.load(std::sync::atomic::Ordering::Relaxed), 2);

        metrics.record_orch_command_succeeded();
        assert_eq!(metrics.orchestrator_commands_succeeded.load(std::sync::atomic::Ordering::Relaxed), 1);

        metrics.record_orch_command_failed();
        assert_eq!(metrics.orchestrator_commands_failed.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metrics_adapter_health_published() {
        let metrics = MetricsAdapter::default();
        metrics.record_health_published();
        metrics.record_health_published();
        assert_eq!(metrics.health_published_total.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn test_metrics_adapter_cb_state_changes() {
        let metrics = MetricsAdapter::default();
        metrics.record_circuit_breaker_state_change();
        assert_eq!(metrics.circuit_breaker_state_changes.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Config Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn test_app_config_has_orchestrator_endpoints() {
        use crate::config::app_config::AppConfig;
        let cfg = AppConfig::from_env();
        assert_eq!(cfg.orchestrator_control_endpoint, "tcp://127.0.0.1:5560");
        assert_eq!(cfg.orchestrator_events_endpoint, "tcp://127.0.0.1:5561");
        assert_eq!(cfg.health_pub_endpoint, "tcp://127.0.0.1:5562");
        assert_eq!(cfg.circuit_breaker_pub_endpoint, "tcp://127.0.0.1:5563");
    }
}
