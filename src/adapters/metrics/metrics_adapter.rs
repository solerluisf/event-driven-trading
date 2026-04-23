// metrics_adapter.rs

pub struct MetricsAdapter;

impl IObservability for MetricsAdapter {
    fn emit(&self, _event: String) {}
}