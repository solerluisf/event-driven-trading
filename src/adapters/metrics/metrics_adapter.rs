// adapters/metrics/metrics_adapter.rs

use crate::core::ports::observability::IObservability;

pub struct MetricsAdapter;

impl IObservability for MetricsAdapter {
    fn emit(&self, _event: String) {}
}