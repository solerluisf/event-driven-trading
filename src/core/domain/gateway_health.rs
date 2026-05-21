// core/domain/gateway_health.rs
//
// Health snapshot the Gateway emits to the Orchestrator.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct GatewayHealthSnapshot {
    pub timestamp_ms: u64,
    pub service_id: String,
    pub status: String,
    pub kill_switch_active: bool,
    pub operation_mode: String,
    pub circuit_breaker_state: String,
    pub rate_limiter_tokens_remaining: f64,
    pub rate_limiter_capacity: f64,
    pub active_connections: usize,
    pub orders_submitted_total: u64,
    pub orders_rejected_total: u64,
    pub ws_reconnect_count: u64,
    pub symbols_subscribed: Vec<String>,
}

impl GatewayHealthSnapshot {
    pub fn new(service_id: impl Into<String>) -> Self {
        Self {
            timestamp_ms: current_time_ms(),
            service_id: service_id.into(),
            status: "healthy".to_string(),
            kill_switch_active: false,
            operation_mode: "paper".to_string(),
            circuit_breaker_state: "closed".to_string(),
            rate_limiter_tokens_remaining: 0.0,
            rate_limiter_capacity: 0.0,
            active_connections: 0,
            orders_submitted_total: 0,
            orders_rejected_total: 0,
            ws_reconnect_count: 0,
            symbols_subscribed: Vec::new(),
        }
    }

    pub fn with_kill_switch(mut self, active: bool) -> Self {
        self.kill_switch_active = active;
        if active && self.status == "healthy" {
            self.status = "degraded".to_string();
        }
        self
    }

    pub fn with_operation_mode(mut self, mode: impl Into<String>) -> Self {
        self.operation_mode = mode.into();
        self
    }

    pub fn with_circuit_breaker_state(mut self, state: impl Into<String>) -> Self {
        self.circuit_breaker_state = state.into();
        self
    }

    pub fn with_rate_limiter(mut self, tokens: f64, capacity: f64) -> Self {
        self.rate_limiter_tokens_remaining = tokens;
        self.rate_limiter_capacity = capacity;
        self
    }

    pub fn with_symbols(mut self, symbols: Vec<String>) -> Self {
        self.symbols_subscribed = symbols;
        self
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
    fn test_new_snapshot_defaults() {
        let snapshot = GatewayHealthSnapshot::new("test_gateway");
        assert_eq!(snapshot.service_id, "test_gateway");
        assert_eq!(snapshot.status, "healthy");
        assert!(!snapshot.kill_switch_active);
        assert_eq!(snapshot.operation_mode, "paper");
        assert_eq!(snapshot.circuit_breaker_state, "closed");
    }

    #[test]
    fn test_with_kill_switch_changes_status() {
        let snapshot = GatewayHealthSnapshot::new("gw")
            .with_kill_switch(true);
        assert!(snapshot.kill_switch_active);
        assert_eq!(snapshot.status, "degraded");
    }

    #[test]
    fn test_with_operation_mode() {
        let snapshot = GatewayHealthSnapshot::new("gw")
            .with_operation_mode("live");
        assert_eq!(snapshot.operation_mode, "live");
    }

    #[test]
    fn test_with_circuit_breaker_state() {
        let snapshot = GatewayHealthSnapshot::new("gw")
            .with_circuit_breaker_state("open");
        assert_eq!(snapshot.circuit_breaker_state, "open");
    }

    #[test]
    fn test_with_rate_limiter() {
        let snapshot = GatewayHealthSnapshot::new("gw")
            .with_rate_limiter(50.0, 200.0);
        assert_eq!(snapshot.rate_limiter_tokens_remaining, 50.0);
        assert_eq!(snapshot.rate_limiter_capacity, 200.0);
    }

    #[test]
    fn test_with_symbols() {
        let symbols = vec!["AAPL".to_string(), "SPY".to_string()];
        let snapshot = GatewayHealthSnapshot::new("gw")
            .with_symbols(symbols.clone());
        assert_eq!(snapshot.symbols_subscribed, symbols);
    }

    #[test]
    fn test_snapshot_serializes() {
        let snapshot = GatewayHealthSnapshot::new("gw")
            .with_kill_switch(false)
            .with_operation_mode("paper")
            .with_symbols(vec!["AAPL".to_string()]);
        let json = serde_json::to_string(&snapshot).expect("serialize");
        assert!(json.contains("broker_gateway") || json.contains("gw"));
        assert!(json.contains("healthy"));
    }
}
