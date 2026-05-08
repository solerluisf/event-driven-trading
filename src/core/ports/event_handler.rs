use async_trait::async_trait;
use crate::adapters::messaging::market_data_publisher::MarketDataEvent;

/// Envelope wrapping every market event with metadata for gap detection
/// and end-to-end latency measurement.
#[derive(Debug, Clone)]
pub struct ReactorEvent {
    pub seq_no:       u64,
    pub ingestion_ts: std::time::Instant,
    pub source:       String,            // e.g. "alpaca/iex"
    pub inner:        MarketDataEvent,
}

#[async_trait]
pub trait EventHandler: Send + Sync + 'static {
    /// Called by the handler task for each event on this handler's symbol.
    /// Must not block; spawn sub-tasks if heavy work is needed.
    async fn on_event(&self, event: &ReactorEvent);

    /// Human-readable name for logs and metrics.
    fn name(&self) -> &str;
}