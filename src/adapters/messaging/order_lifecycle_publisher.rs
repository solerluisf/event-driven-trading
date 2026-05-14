// adapters/messaging/order_lifecycle_publisher.rs
//
// Actor-based order lifecycle event publisher using tokio channels.
// Binds a ZeroMQ PUB socket and publishes normalized order lifecycle events
// to downstream services (Risk Service, Journal, Observability).
//
// Socket topology:
//
//   Gateway (PUB, this file)  ──►  Risk Service (SUB)
//                               ──►  Journal Service (SUB)
//                               ──►  Observability Service (SUB)
//
// Topic format: "order_lifecycle.<execution_id>" e.g. "order_lifecycle.123e4567"
// Payload: MessagePack-serialized OrderLifecycleEvent

use tokio::sync::mpsc;

use crate::adapters::messaging::wire_codec::encode_order_lifecycle_event;
use crate::core::domain::order::OrderLifecycleEvent;

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

impl From<mpsc::error::SendError<OrderLifecycleEvent>> for PublisherError {
    fn from(_: mpsc::error::SendError<OrderLifecycleEvent>) -> Self {
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

/// Handle to send order lifecycle events to the publisher actor
#[derive(Clone)]
pub struct OrderLifecyclePublisher {
    tx: mpsc::Sender<OrderLifecycleEvent>,
}

impl OrderLifecyclePublisher {
    /// Create a new publisher and spawn the actor task.
    /// Returns a handle for publishing events and a JoinHandle for the actor task.
    pub fn spawn(endpoint: impl Into<String>) -> (Self, tokio::task::JoinHandle<Result<()>>) {
        let endpoint = endpoint.into();
        let (tx, rx) = mpsc::channel(128);

        let handle = tokio::spawn(publisher_actor(endpoint, rx));

        (Self { tx }, handle)
    }

    /// Publish an order lifecycle event asynchronously (non-blocking send).
    /// Returns an error only if the actor has shut down.
    pub async fn publish(&self, event: OrderLifecycleEvent) -> Result<()> {
        self.tx.send(event).await?;
        Ok(())
    }

    /// Construct a publisher from an existing sender.
    /// This is useful for testing and integration scenarios where you want to
    /// intercept events without needing a ZMQ context.
    pub fn from_sender(tx: tokio::sync::mpsc::Sender<OrderLifecycleEvent>) -> Self {
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
async fn publisher_actor(
    endpoint: String,
    mut rx: mpsc::Receiver<OrderLifecycleEvent>,
) -> Result<()> {
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

        tracing::info!("OrderLifecyclePublisher ZMQ thread bound on {}", endpoint);

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
                    tracing::info!("OrderLifecyclePublisher ZMQ thread shutting down");
                    break;
                }
            }
        }

        Ok(())
    });

    // Process incoming events and forward to ZMQ thread
    while let Some(event) = rx.recv().await {
        let topic = format!("order_lifecycle.{}", event.execution_id);
        let payload = match encode_order_lifecycle_event(&event) {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(
                    "failed to encode order_lifecycle event execution_id={} type={:?}: {}",
                    event.execution_id,
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
            "published order_lifecycle event execution_id={} type={:?}",
            event.execution_id,
            event.event_type
        );
    }

    // Signal ZMQ thread to shutdown
    let _ = zmq_tx.send(ZmqSendMessage::Shutdown);

    // Wait for ZMQ thread to finish
    if let Err(e) = zmq_thread.join() {
        tracing::error!("ZMQ thread panicked: {:?}", e);
    }

    tracing::info!("OrderLifecyclePublisher actor shutting down");
    Ok(())
}

/// Utility function to create a filled event
pub fn create_filled_event(
    execution_id: impl Into<String>,
    symbol: impl Into<String>,
    client_order_id: Option<String>,
    filled_qty: u32,
    filled_price: f64,
) -> OrderLifecycleEvent {
    use crate::core::domain::order::OrderLifecycleEventType;
    use serde_json::json;
    use uuid::Uuid;

    OrderLifecycleEvent {
        event_id: Uuid::new_v4().to_string(),
        execution_id: execution_id.into(),
        client_order_id,
        symbol: symbol.into(),
        event_type: OrderLifecycleEventType::Filled,
        timestamp: chrono::Utc::now().to_rfc3339(),
        payload: json!({
            "filled_qty": filled_qty,
            "filled_price": filled_price,
        }),
    }
}

/// Utility function to create a rejected event
pub fn create_rejected_event(
    execution_id: impl Into<String>,
    symbol: impl Into<String>,
    client_order_id: Option<String>,
    reason: impl Into<String>,
) -> OrderLifecycleEvent {
    use crate::core::domain::order::OrderLifecycleEventType;
    use serde_json::json;
    use uuid::Uuid;

    OrderLifecycleEvent {
        event_id: Uuid::new_v4().to_string(),
        execution_id: execution_id.into(),
        client_order_id,
        symbol: symbol.into(),
        event_type: OrderLifecycleEventType::Rejected,
        timestamp: chrono::Utc::now().to_rfc3339(),
        payload: json!({
            "reason": reason.into(),
        }),
    }
}

/// Utility function to create a cancelled event
pub fn create_cancelled_event(
    execution_id: impl Into<String>,
    symbol: impl Into<String>,
    client_order_id: Option<String>,
) -> OrderLifecycleEvent {
    use crate::core::domain::order::OrderLifecycleEventType;
    use serde_json::json;
    use uuid::Uuid;

    OrderLifecycleEvent {
        event_id: Uuid::new_v4().to_string(),
        execution_id: execution_id.into(),
        client_order_id,
        symbol: symbol.into(),
        event_type: OrderLifecycleEventType::Cancelled,
        timestamp: chrono::Utc::now().to_rfc3339(),
        payload: json!({}),
    }
}

/// Utility function to create a submitted event
pub fn create_submitted_event(
    execution_id: impl Into<String>,
    symbol: impl Into<String>,
    client_order_id: Option<String>,
) -> OrderLifecycleEvent {
    use crate::core::domain::order::OrderLifecycleEventType;
    use serde_json::json;
    use uuid::Uuid;

    OrderLifecycleEvent {
        event_id: Uuid::new_v4().to_string(),
        execution_id: execution_id.into(),
        client_order_id,
        symbol: symbol.into(),
        event_type: OrderLifecycleEventType::Submitted,
        timestamp: chrono::Utc::now().to_rfc3339(),
        payload: json!({}),
    }
}

/// Utility function to create a partial fill event
pub fn create_partial_fill_event(
    execution_id: impl Into<String>,
    symbol: impl Into<String>,
    client_order_id: Option<String>,
    filled_qty: u32,
    filled_price: f64,
    remaining_qty: u32,
) -> OrderLifecycleEvent {
    use crate::core::domain::order::OrderLifecycleEventType;
    use serde_json::json;
    use uuid::Uuid;

    OrderLifecycleEvent {
        event_id: Uuid::new_v4().to_string(),
        execution_id: execution_id.into(),
        client_order_id,
        symbol: symbol.into(),
        event_type: OrderLifecycleEventType::PartialFill,
        timestamp: chrono::Utc::now().to_rfc3339(),
        payload: json!({
            "filled_qty": filled_qty,
            "filled_price": filled_price,
            "remaining_qty": remaining_qty,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::order::OrderLifecycleEventType;

    #[tokio::test]
    async fn order_lifecycle_publisher_spawns_and_publishes() {
        let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_order_lifecycle");

        let event = OrderLifecycleEvent {
            event_id: "evt-123".to_string(),
            execution_id: "exec-456".to_string(),
            client_order_id: Some("client-789".to_string()),
            symbol: "AAPL".to_string(),
            event_type: OrderLifecycleEventType::Filled,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            payload: serde_json::json!({"filled_qty": 100}),
        };

        // Publish should succeed
        publisher.publish(event).await.expect("publish should succeed");

        // Drop publisher to signal shutdown
        drop(publisher);

        // Wait for actor to complete
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle
        ).await;

        assert!(result.is_ok(), "actor should complete");
    }

    #[tokio::test]
    async fn order_lifecycle_publisher_publishes_multiple_events() {
        let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_order_lifecycle_multi");

        // Publish multiple events
        for i in 0..5 {
            let event = OrderLifecycleEvent {
                event_id: format!("evt-{}", i),
                execution_id: format!("exec-{}", i),
                client_order_id: Some(format!("client-{}", i)),
                symbol: "AAPL".to_string(),
                event_type: OrderLifecycleEventType::Filled,
                timestamp: "2026-01-01T00:00:00Z".to_string(),
                payload: serde_json::json!({"filled_qty": i * 100}),
            };
            publisher.publish(event).await.expect("publish should succeed");
        }

        // Drop publisher to signal shutdown
        drop(publisher);

        // Wait for actor to complete
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle
        ).await;

        assert!(result.is_ok(), "actor should complete after publishing multiple events");
    }

    #[tokio::test]
    async fn order_lifecycle_publisher_from_sender() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let publisher = OrderLifecyclePublisher::from_sender(tx);

        let event = OrderLifecycleEvent {
            event_id: "evt-test".to_string(),
            execution_id: "exec-test".to_string(),
            client_order_id: None,
            symbol: "MSFT".to_string(),
            event_type: OrderLifecycleEventType::Submitted,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
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

    #[tokio::test]
    async fn order_lifecycle_publisher_handles_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel::<OrderLifecycleEvent>(1);
        let publisher = OrderLifecyclePublisher::from_sender(tx);
        
        // Drop receiver to close channel
        drop(rx);

        let event = OrderLifecycleEvent {
            event_id: "evt-test".to_string(),
            execution_id: "exec-test".to_string(),
            client_order_id: None,
            symbol: "MSFT".to_string(),
            event_type: OrderLifecycleEventType::Submitted,
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            payload: serde_json::json!({}),
        };

        // Publish should fail because channel is closed
        let result = publisher.publish(event).await;
        assert!(result.is_err(), "publish should fail when channel is closed");
    }

    #[test]
    fn test_create_filled_event() {
        let event = create_filled_event("exec-1", "AAPL", Some("client-1".to_string()), 100, 150.25);
        assert_eq!(event.execution_id, "exec-1");
        assert_eq!(event.symbol, "AAPL");
        assert_eq!(event.event_type, OrderLifecycleEventType::Filled);
        assert!(event.payload.get("filled_qty").is_some());
        assert_eq!(event.payload["filled_qty"], 100);
        assert!(event.payload.get("filled_price").is_some());
        assert_eq!(event.payload["filled_price"], 150.25);
        assert!(event.client_order_id.is_some());
        assert_eq!(event.client_order_id.unwrap(), "client-1");
    }

    #[test]
    fn test_create_filled_event_without_client_order_id() {
        let event = create_filled_event("exec-1", "AAPL", None, 100, 150.25);
        assert!(event.client_order_id.is_none());
    }

    #[test]
    fn test_create_rejected_event() {
        let event = create_rejected_event("exec-1", "AAPL", Some("client-1".to_string()), "insufficient funds");
        assert_eq!(event.execution_id, "exec-1");
        assert_eq!(event.event_type, OrderLifecycleEventType::Rejected);
        assert!(event.payload.get("reason").is_some());
        assert_eq!(event.payload["reason"], "insufficient funds");
    }

    #[test]
    fn test_create_rejected_event_with_string_reason() {
        let reason = String::from("invalid symbol");
        let event = create_rejected_event("exec-2", "INVALID", None, reason);
        assert_eq!(event.payload["reason"], "invalid symbol");
    }

    #[test]
    fn test_create_cancelled_event() {
        let event = create_cancelled_event("exec-1", "AAPL", Some("client-1".to_string()));
        assert_eq!(event.execution_id, "exec-1");
        assert_eq!(event.event_type, OrderLifecycleEventType::Cancelled);
    }

    #[test]
    fn test_create_submitted_event() {
        let event = create_submitted_event("exec-1", "AAPL", Some("client-1".to_string()));
        assert_eq!(event.execution_id, "exec-1");
        assert_eq!(event.event_type, OrderLifecycleEventType::Submitted);
    }

    #[test]
    fn test_create_partial_fill_event() {
        let event = create_partial_fill_event("exec-1", "AAPL", Some("client-1".to_string()), 50, 150.25, 50);
        assert_eq!(event.execution_id, "exec-1");
        assert_eq!(event.event_type, OrderLifecycleEventType::PartialFill);
        assert!(event.payload.get("filled_qty").is_some());
        assert!(event.payload.get("remaining_qty").is_some());
    }

    #[test]
    fn test_event_type_equality() {
        assert_eq!(OrderLifecycleEventType::Submitted, OrderLifecycleEventType::Submitted);
        assert_ne!(OrderLifecycleEventType::Submitted, OrderLifecycleEventType::Filled);
        assert_ne!(OrderLifecycleEventType::Filled, OrderLifecycleEventType::PartialFill);
        assert_ne!(OrderLifecycleEventType::Rejected, OrderLifecycleEventType::Cancelled);
    }

    #[test]
    fn test_event_clone() {
        let event = create_submitted_event("exec-1", "AAPL", Some("client-1".to_string()));
        let cloned = event.clone();
        assert_eq!(event.event_id, cloned.event_id);
        assert_eq!(event.execution_id, cloned.execution_id);
        assert_eq!(event.event_type, cloned.event_type);
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
}
