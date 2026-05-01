// adapters/messaging/market_data_publisher.rs
//
// Actor-based market data publisher using tokio channels.
// Binds a ZeroMQ PUB socket and publishes normalized market data events
// to downstream services (e.g. the Market Data Service).
//
// Socket topology:
//
//   Gateway (PUB, this file)  ──►  Market Data Service (SUB)
//
// Topic format: "market_data.<symbol>" e.g. "market_data.AAPL"
// Payload: JSON-serialized MarketDataEvent

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use zmq::Socket;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataEvent {
    pub symbol: String,
    pub event_type: MarketDataEventType,
    pub timestamp: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataEventType {
    Tick,
    BookUpdate,
    Trade,
    Bar,
}

#[derive(Debug)]
pub enum PublisherError {
    ZmqError(zmq::Error),
    SendError(()),
}

impl From<zmq::Error> for PublisherError {
    fn from(err: zmq::Error) -> Self {
        PublisherError::ZmqError(err)
    }
}

impl From<mpsc::error::SendError<MarketDataEvent>> for PublisherError {
    fn from(_: mpsc::error::SendError<MarketDataEvent>) -> Self {
        PublisherError::SendError(())
    }
}

impl std::fmt::Display for PublisherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublisherError::ZmqError(e) => write!(f, "ZMQ Error: {}", e),
            PublisherError::SendError(_) => write!(f, "Publisher channel closed"),
        }
    }
}

impl std::error::Error for PublisherError {}

pub type Result<T> = std::result::Result<T, PublisherError>;

/// Handle to send market data events to the publisher actor
#[derive(Clone)]
pub struct MarketDataPublisher {
    tx: mpsc::Sender<MarketDataEvent>,
}

impl MarketDataPublisher {
    /// Create a new publisher and spawn the actor task.
    /// Returns a handle for publishing events and a JoinHandle for the actor task.
    pub fn spawn(endpoint: impl Into<String>) -> (Self, tokio::task::JoinHandle<Result<()>>) {
        let endpoint = endpoint.into();
        let (tx, rx) = mpsc::channel(128);

        let handle = tokio::spawn(publisher_actor(endpoint, rx));

        (Self { tx }, handle)
    }

    /// Publish a market data event asynchronously (non-blocking send).
    /// Returns an error only if the actor has shut down.
    pub async fn publish(&self, event: MarketDataEvent) -> Result<()> {
        self.tx.send(event).await?;
        Ok(())
    }
}

/// Actor task that manages the ZMQ socket and processes publish events
async fn publisher_actor(endpoint: String, mut rx: mpsc::Receiver<MarketDataEvent>) -> Result<()> {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::PUB)?;
    socket.bind(&endpoint)?;

    tracing::info!("MarketDataPublisher actor bound on {}", endpoint);

    while let Some(event) = rx.recv().await {
        let topic = format!("market_data.{}", event.symbol);
        let payload = serde_json::to_vec(&event).unwrap_or_default();

        // Send topic frame
        socket.send(&topic, zmq::SNDMORE)?;
        // Send payload frame
        socket.send(&payload, 0)?;

        tracing::debug!(
            "published market_data event symbol={} type={:?}",
            event.symbol,
            event.event_type
        );
    }

    tracing::info!("MarketDataPublisher actor shutting down");
    Ok(())
}