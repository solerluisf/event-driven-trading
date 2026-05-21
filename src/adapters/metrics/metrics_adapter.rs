// adapters/metrics/metrics_adapter.rs

use std::sync::atomic::{AtomicU64, Ordering};
use crate::core::ports::observability::IObservability;

pub struct MetricsAdapter {
    pub orchestrator_commands_received: AtomicU64,
    pub orchestrator_commands_succeeded: AtomicU64,
    pub orchestrator_commands_failed: AtomicU64,
    pub health_published_total: AtomicU64,
    pub circuit_breaker_state_changes: AtomicU64,
}

impl Default for MetricsAdapter {
    fn default() -> Self {
        Self {
            orchestrator_commands_received: AtomicU64::new(0),
            orchestrator_commands_succeeded: AtomicU64::new(0),
            orchestrator_commands_failed: AtomicU64::new(0),
            health_published_total: AtomicU64::new(0),
            circuit_breaker_state_changes: AtomicU64::new(0),
        }
    }
}

impl MetricsAdapter {
    pub fn record_orch_command_received(&self) {
        self.orchestrator_commands_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_orch_command_succeeded(&self) {
        self.orchestrator_commands_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_orch_command_failed(&self) {
        self.orchestrator_commands_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_health_published(&self) {
        self.health_published_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_circuit_breaker_state_change(&self) {
        self.circuit_breaker_state_changes.fetch_add(1, Ordering::Relaxed);
    }
}

impl IObservability for MetricsAdapter {
    fn emit(&self, _event: String) {}
}
