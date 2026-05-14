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
// Payload: MessagePack-serialized MarketDataEvent

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::adapters::messaging::wire_codec::encode_market_data_event;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDataEvent {
    pub symbol: String,
    pub event_type: MarketDataEventType,
    pub timestamp: String,
    pub source: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    /// For testing only — construct a publisher from an existing sender
    /// so tests can intercept events without needing a ZMQ context.
    #[cfg(test)]
    pub fn from_sender(tx: tokio::sync::mpsc::Sender<MarketDataEvent>) -> Self {
        Self { tx }
    }
}

/// Internal message type for the ZMQ send thread.
enum ZmqSendMessage {
    /// Send a message with the given topic and payload
    Send { topic: String, payload: Vec<u8> },
    /// Shutdown the thread
    Shutdown,
}

/// Actor task that manages the ZMQ socket and processes publish events.
///
/// Uses a dedicated thread for ZMQ send operations to avoid blocking tokio worker threads.
/// This is critical because ZMQ socket operations are synchronous and can block under load,
/// causing cascading latency in the async runtime.
///
/// The ZMQ socket is not thread-safe, so it must be owned by a single dedicated thread.
async fn publisher_actor(endpoint: String, mut rx: mpsc::Receiver<MarketDataEvent>) -> Result<()> {
    // Create a channel to communicate with the ZMQ send thread
    let (zmq_tx, zmq_rx) = std::sync::mpsc::channel::<ZmqSendMessage>();

    // Spawn a dedicated thread for ZMQ operations
    let zmq_thread = std::thread::spawn(move || {
        let ctx = zmq::Context::new();
        let socket = match ctx.socket(zmq::PUB) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to create ZMQ socket: {}", e);
                return Err(e);
            }
        };

        if let Err(e) = socket.bind(&endpoint) {
            tracing::error!("Failed to bind ZMQ socket to {}: {}", endpoint, e);
            return Err(e);
        }

        tracing::info!("MarketDataPublisher ZMQ thread bound on {}", endpoint);

        loop {
            match zmq_rx.recv() {
                Ok(ZmqSendMessage::Send { topic, payload }) => {
                    // Send topic frame
                    if let Err(e) = socket.send(&topic, zmq::SNDMORE) {
                        tracing::error!("ZMQ send failed for topic: {}", e);
                        continue;
                    }
                    // Send payload frame
                    if let Err(e) = socket.send(&payload, 0) {
                        tracing::error!("ZMQ send failed for payload: {}", e);
                        continue;
                    }
                }
                Ok(ZmqSendMessage::Shutdown) | Err(_) => {
                    tracing::info!("MarketDataPublisher ZMQ thread shutting down");
                    break;
                }
            }
        }

        Ok(())
    });

    // Process incoming events and forward to ZMQ thread
    while let Some(event) = rx.recv().await {
        let topic = format!("market_data.{}", event.symbol);
        let payload = match encode_market_data_event(&event) {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(
                    "failed to encode market_data event symbol={} type={:?}: {}",
                    event.symbol,
                    event.event_type,
                    err
                );
                continue;
            }
        };

        // Send to ZMQ thread (non-blocking from async perspective)
        if let Err(_) = zmq_tx.send(ZmqSendMessage::Send { topic, payload }) {
            tracing::error!("ZMQ send thread channel closed");
            return Err(PublisherError::SendError(()));
        }

        tracing::debug!(
            "published market_data event symbol={} type={:?}",
            event.symbol,
            event.event_type
        );
    }

    // Signal ZMQ thread to shutdown
    let _ = zmq_tx.send(ZmqSendMessage::Shutdown);

    // Wait for ZMQ thread to finish
    if let Err(e) = zmq_thread.join() {
        tracing::error!("ZMQ thread panicked: {:?}", e);
    }

    tracing::info!("MarketDataPublisher actor shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn market_data_publisher_spawns_and_publishes() {
        let (publisher, handle) = MarketDataPublisher::spawn("inproc://test_market_data");

        let event = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: MarketDataEventType::Trade,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 150.0}),
        };

        // Publish should succeed
        publisher.publish(event).await.expect("publish should succeed");

        // Drop publisher to signal shutdown
        drop(publisher);

        // Wait for actor to complete
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            handle
        ).await;

        assert!(result.is_ok(), "actor should complete");
    }

    #[tokio::test]
    async fn market_data_publisher_publishes_multiple_events() {
        let (publisher, handle) = MarketDataPublisher::spawn("inproc://test_market_data_multi");

        // Publish multiple events
        for i in 0..10 {
            let event = MarketDataEvent {
                symbol: format!("SYM{}", i),
                event_type: MarketDataEventType::Tick,
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                source: "test".to_string(),
                payload: serde_json::json!({"index": i}),
            };
            publisher.publish(event).await.expect("publish should succeed");
        }

        // Drop publisher to signal shutdown
        drop(publisher);

        // Wait for actor to complete
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            handle
        ).await;

        assert!(result.is_ok(), "actor should complete after publishing multiple events");
    }

    #[tokio::test]
    async fn market_data_publisher_concurrent_publishes() {
        let (publisher, handle) = MarketDataPublisher::spawn("inproc://test_market_data_concurrent");

        // Spawn multiple concurrent publish tasks
        let mut handles = vec![];
        for i in 0..5 {
            let pub_clone = publisher.clone();
            let handle = tokio::spawn(async move {
                for j in 0..10 {
                    let event = MarketDataEvent {
                        symbol: format!("SYM{}", i),
                        event_type: MarketDataEventType::BookUpdate,
                        timestamp: "2026-01-01T00:00:00Z".to_string(),
                        source: "test".to_string(),
                        payload: serde_json::json!({"task": i, "seq": j}),
                    };
                    pub_clone.publish(event).await.expect("publish should succeed");
                }
            });
            handles.push(handle);
        }

        // Wait for all publish tasks to complete
        for h in handles {
            h.await.expect("publish task should complete");
        }

        // Drop publisher to signal shutdown
        drop(publisher);

        // Wait for actor to complete
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            handle
        ).await;

        assert!(result.is_ok(), "actor should complete after concurrent publishes");
    }

    #[tokio::test]
    async fn market_data_publisher_handles_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel::<MarketDataEvent>(1);
        let publisher = MarketDataPublisher::from_sender(tx);
        
        // Drop receiver to close channel
        drop(rx);

        let event = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: MarketDataEventType::Trade,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 150.0}),
        };

        // Publish should fail because channel is closed
        let result = publisher.publish(event).await;
        assert!(result.is_err(), "publish should fail when channel is closed");
    }

    #[tokio::test]
    async fn market_data_publisher_from_sender() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let publisher = MarketDataPublisher::from_sender(tx);

        let event = MarketDataEvent {
            symbol: "MSFT".to_string(),
            event_type: MarketDataEventType::Bar,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({}),
        };

        // Clone publisher for multiple sends
        let publisher2 = publisher.clone();
        
        // Send two events
        publisher.publish(event.clone()).await.expect("publish should succeed");
        publisher2.publish(event).await.expect("publish should succeed");

        // Verify events were received
        let received1 = rx.recv().await;
        assert!(received1.is_some(), "first event should be received");
        
        let received2 = rx.recv().await;
        assert!(received2.is_some(), "second event should be received");
    }

    #[test]
    fn market_data_event_type_equality() {
        assert_eq!(MarketDataEventType::Tick, MarketDataEventType::Tick);
        assert_ne!(MarketDataEventType::Tick, MarketDataEventType::Trade);
        assert_ne!(MarketDataEventType::BookUpdate, MarketDataEventType::Bar);
    }

    #[test]
    fn market_data_event_clone() {
        let event = MarketDataEvent {
            symbol: "AAPL".to_string(),
            event_type: MarketDataEventType::Trade,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            source: "test".to_string(),
            payload: serde_json::json!({"price": 150.0}),
        };
        let cloned = event.clone();
        assert_eq!(event.symbol, cloned.symbol);
        assert_eq!(event.event_type, cloned.event_type);
        assert_eq!(event.timestamp, cloned.timestamp);
    }

    #[tokio::test]
    async fn test_publisher_error_display() {
        let error = PublisherError::SendError(());
        assert_eq!(format!("{}", error), "Publisher channel closed");
        
        // Test ZMQ error (simulate)
        let zmq_error = zmq::Error::ENOENT;
        let error = PublisherError::ZmqError(zmq_error);
        assert!(format!("{}", error).contains("ZMQ Error"));
    }

    /// Test that demonstrates the non-blocking nature of the publisher.
    /// Multiple rapid publishes should not block the tokio runtime.
    #[tokio::test]
    async fn market_data_publisher_non_blocking_under_load() {
        let (publisher, handle) = MarketDataPublisher::spawn("inproc://test_market_data_load");

        let start = std::time::Instant::now();
        
        // Rapidly publish many events
        for i in 0..100 {
            let event = MarketDataEvent {
                symbol: format!("SYM{}", i % 10),
                event_type: MarketDataEventType::Tick,
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                source: "test".to_string(),
                payload: serde_json::json!({"seq": i}),
            };
            // Use try_send to verify non-blocking behavior
            publisher.tx.try_send(event).expect("try_send should not block");
        }

        let elapsed = start.elapsed();
        // All 100 sends should complete quickly since they're just channel operations
        // If ZMQ was blocking synchronously, this would take much longer
        assert!(elapsed < Duration::from_millis(500), 
            "Rapid publishes should complete quickly, took {:?}", elapsed);

        // Drop publisher to signal shutdown
        drop(publisher);

        // Wait for actor to complete
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            handle
        ).await;

        assert!(result.is_ok(), "actor should complete after high load");
    }
}