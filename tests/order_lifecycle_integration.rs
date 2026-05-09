// tests/order_lifecycle_integration.rs
//
// Integration tests for order lifecycle event publishing.
// Tests the end-to-end flow from helper functions through OrderLifecyclePublisher.

use std::time::Duration;
use tokio::time::timeout;

// Import from the crate
use broker_gateway_service::adapters::messaging::order_lifecycle_publisher::{
    OrderLifecyclePublisher, 
    create_submitted_event, 
    create_filled_event, 
    create_cancelled_event,
    create_rejected_event,
    create_partial_fill_event
};
use broker_gateway_service::core::domain::order::{
    OrderLifecycleEvent, OrderLifecycleEventType
};

/// Test that verifies OrderLifecyclePublisher can be created and publishes events
#[tokio::test]
async fn test_order_lifecycle_publisher_spawn_and_publish() {
    let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_lifecycle_integration");

    // Create and publish a submitted event
    let event = create_submitted_event("exec-123", "AAPL", Some("client-456".to_string()));
    publisher.publish(event).await.expect("publish should succeed");

    // Create and publish a filled event
    let event = create_filled_event("exec-123", "AAPL", Some("client-456".to_string()), 100, 150.50);
    publisher.publish(event).await.expect("publish should succeed");

    // Drop publisher to signal shutdown
    drop(publisher);

    // Wait for actor to complete
    let result = timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "actor should complete successfully");
}

/// Test that verifies multiple events can be published
#[tokio::test]
async fn test_order_lifecycle_multiple_events() {
    let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_multiple_events");

    // Publish various event types
    let events = vec![
        create_submitted_event("exec-1", "AAPL", Some("client-1".to_string())),
        create_partial_fill_event("exec-1", "AAPL", Some("client-1".to_string()), 25, 150.0, 75),
        create_partial_fill_event("exec-1", "AAPL", Some("client-1".to_string()), 50, 150.25, 25),
        create_filled_event("exec-1", "AAPL", Some("client-1".to_string()), 25, 150.50),
    ];

    for event in events {
        publisher.publish(event).await.expect("publish should succeed");
    }

    drop(publisher);
    
    let result = timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "actor should complete after multiple events");
}

/// Test helper function: create_submitted_event
#[test]
fn test_create_submitted_event_helper() {
    let event = create_submitted_event("exec-123", "AAPL", Some("client-456".to_string()));
    
    assert_eq!(event.execution_id, "exec-123");
    assert_eq!(event.symbol, "AAPL");
    assert_eq!(event.client_order_id, Some("client-456".to_string()));
    assert_eq!(event.event_type, OrderLifecycleEventType::Submitted);
    assert!(!event.event_id.is_empty(), "event_id should be auto-generated");
    assert!(!event.timestamp.is_empty(), "timestamp should be auto-generated");
}

/// Test helper function: create_filled_event
#[test]
fn test_create_filled_event_helper() {
    let event = create_filled_event("exec-123", "MSFT", Some("client-789".to_string()), 100, 250.75);
    
    assert_eq!(event.execution_id, "exec-123");
    assert_eq!(event.symbol, "MSFT");
    assert_eq!(event.event_type, OrderLifecycleEventType::Filled);
    assert_eq!(event.payload["filled_qty"], 100);
    assert_eq!(event.payload["filled_price"], 250.75);
}

/// Test helper function: create_cancelled_event
#[test]
fn test_create_cancelled_event_helper() {
    let event = create_cancelled_event("exec-123", "TSLA", None);
    
    assert_eq!(event.execution_id, "exec-123");
    assert_eq!(event.symbol, "TSLA");
    assert_eq!(event.event_type, OrderLifecycleEventType::Cancelled);
    assert!(event.client_order_id.is_none());
}

/// Test helper function: create_rejected_event
#[test]
fn test_create_rejected_event_helper() {
    let event = create_rejected_event("exec-123", "AMZN", Some("client-999".to_string()), "insufficient funds");
    
    assert_eq!(event.execution_id, "exec-123");
    assert_eq!(event.symbol, "AMZN");
    assert_eq!(event.event_type, OrderLifecycleEventType::Rejected);
    assert_eq!(event.payload["reason"], "insufficient funds");
}

/// Test helper function: create_partial_fill_event
#[test]
fn test_create_partial_fill_event_helper() {
    let event = create_partial_fill_event("exec-123", "GOOGL", Some("client-111".to_string()), 50, 2800.0, 50);
    
    assert_eq!(event.execution_id, "exec-123");
    assert_eq!(event.symbol, "GOOGL");
    assert_eq!(event.event_type, OrderLifecycleEventType::PartialFill);
    assert_eq!(event.payload["filled_qty"], 50);
    assert_eq!(event.payload["filled_price"], 2800.0);
    assert_eq!(event.payload["remaining_qty"], 50);
}

/// Test event type serialization round-trip
#[test]
fn test_event_type_serialization() {
    use serde_json;
    
    let types = vec![
        OrderLifecycleEventType::Submitted,
        OrderLifecycleEventType::PartialFill,
        OrderLifecycleEventType::Filled,
        OrderLifecycleEventType::Rejected,
        OrderLifecycleEventType::Cancelled,
        OrderLifecycleEventType::Replaced,
        OrderLifecycleEventType::Expired,
        OrderLifecycleEventType::Error,
    ];

    for event_type in types {
        let serialized = serde_json::to_string(&event_type).expect("serialize should succeed");
        let deserialized: OrderLifecycleEventType = serde_json::from_str(&serialized).expect("deserialize should succeed");
        assert_eq!(event_type, deserialized, "event type should round-trip correctly");
    }
}

/// Test event cloning
#[test]
fn test_event_clone() {
    let event = create_filled_event("exec-123", "AAPL", Some("client-456".to_string()), 100, 150.50);
    let cloned = event.clone();
    
    assert_eq!(event.event_id, cloned.event_id);
    assert_eq!(event.execution_id, cloned.execution_id);
    assert_eq!(event.symbol, cloned.symbol);
    assert_eq!(event.event_type, cloned.event_type);
    assert_eq!(event.timestamp, cloned.timestamp);
    assert_eq!(event.payload, cloned.payload);
}

/// Test event comparison
#[test]
fn test_event_comparison() {
    let event1 = create_submitted_event("exec-1", "AAPL", Some("client-1".to_string()));
    let event2 = create_submitted_event("exec-1", "AAPL", Some("client-1".to_string()));
    let event3 = create_filled_event("exec-1", "AAPL", Some("client-1".to_string()), 100, 150.0);
    
    // Events with same data but different generated UUIDs/timestamps should not be equal
    assert_ne!(event1.event_id, event2.event_id, "auto-generated event_ids should be unique");
    
    // But they should have the same execution_id and symbol
    assert_eq!(event1.execution_id, event2.execution_id);
    assert_eq!(event1.symbol, event2.symbol);
    
    // Different event types
    assert_ne!(event1.event_type, event3.event_type);
}

/// Test creating events with string references
#[test]
fn test_create_events_with_string_refs() {
    let exec_id = String::from("exec-123");
    let symbol = String::from("AAPL");
    let client_id = Some(String::from("client-456"));
    
    let event = create_submitted_event(&exec_id, &symbol, client_id);
    assert_eq!(event.execution_id, "exec-123");
    assert_eq!(event.symbol, "AAPL");
}

/// Test empty payload handling
#[test]
fn test_empty_payload() {
    let event = create_submitted_event("exec-123", "AAPL", None);
    
    assert!(event.payload.as_object().unwrap().is_empty());
}

/// Test publisher error types
#[test]
fn test_publisher_error_display() {
    use broker_gateway_service::adapters::messaging::order_lifecycle_publisher::PublisherError;
    
    let send_error = PublisherError::SendError(());
    assert!(send_error.to_string().contains("channel closed"));
    
    let zmq_error = PublisherError::ZmqError(zmq::Error::ENOENT);
    assert!(zmq_error.to_string().contains("ZMQ Error"));
}

/// Test creating events with various quantity and price combinations
#[test]
fn test_event_quantity_price_variations() {
    // Large quantity
    let event = create_filled_event("exec-1", "AAPL", None, 1_000_000, 150.0);
    assert_eq!(event.payload["filled_qty"], 1_000_000);
    
    // Fractional price
    let event = create_filled_event("exec-1", "AAPL", None, 100, 150.999999);
    assert_eq!(event.payload["filled_price"], 150.999999);
    
    // Zero quantity (edge case)
    let event = create_partial_fill_event("exec-1", "AAPL", None, 0, 150.0, 100);
    assert_eq!(event.payload["filled_qty"], 0);
    assert_eq!(event.payload["remaining_qty"], 100);
}

/// Integration test simulating a complete order lifecycle
#[tokio::test]
async fn test_complete_order_lifecycle_simulation() {
    let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_lifecycle_full");
    
    // Simulate a complete order lifecycle
    let execution_id = "sim-exec-123";
    let symbol = "AAPL";
    let client_order_id = Some("sim-client-456".to_string());
    
    // 1. Order submitted
    let event = create_submitted_event(execution_id, symbol, client_order_id.clone());
    publisher.publish(event).await.expect("submit event should publish");
    
    // 2. Partial fill 1
    let event = create_partial_fill_event(execution_id, symbol, client_order_id.clone(), 25, 150.0, 75);
    publisher.publish(event).await.expect("partial fill 1 should publish");
    
    // 3. Partial fill 2
    let event = create_partial_fill_event(execution_id, symbol, client_order_id.clone(), 50, 150.25, 25);
    publisher.publish(event).await.expect("partial fill 2 should publish");
    
    // 4. Final fill
    let event = create_filled_event(execution_id, symbol, client_order_id, 25, 150.50);
    publisher.publish(event).await.expect("final fill should publish");
    
    drop(publisher);
    
    let result = timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "lifecycle simulation should complete");
}

/// Test handling of rejected orders
#[tokio::test]
async fn test_rejected_order_lifecycle() {
    let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_rejected");
    
    // Order submitted
    let event = create_submitted_event("rej-exec-123", "INVALID", Some("rej-client-456".to_string()));
    publisher.publish(event).await.expect("submit event should publish");
    
    // Order rejected
    let event = create_rejected_event("rej-exec-123", "INVALID", Some("rej-client-456".to_string()), "invalid symbol");
    publisher.publish(event).await.expect("rejected event should publish");
    
    drop(publisher);
    
    let result = timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "rejected order lifecycle should complete");
}

/// Test handling of cancelled orders
#[tokio::test]
async fn test_cancelled_order_lifecycle() {
    let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_cancelled");
    
    // Order submitted
    let event = create_submitted_event("can-exec-123", "AAPL", Some("can-client-456".to_string()));
    publisher.publish(event).await.expect("submit event should publish");
    
    // Partial fill
    let event = create_partial_fill_event("can-exec-123", "AAPL", Some("can-client-456".to_string()), 30, 150.0, 70);
    publisher.publish(event).await.expect("partial fill should publish");
    
    // Order cancelled
    let event = create_cancelled_event("can-exec-123", "AAPL", Some("can-client-456".to_string()));
    publisher.publish(event).await.expect("cancelled event should publish");
    
    drop(publisher);
    
    let result = timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "cancelled order lifecycle should complete");
}

/// Test concurrent event publishing
#[tokio::test]
async fn test_concurrent_event_publishing() {
    let (publisher, handle) = OrderLifecyclePublisher::spawn("inproc://test_concurrent");
    
    let mut tasks = vec![];
    
    // Spawn multiple concurrent publishers
    for i in 0..10 {
        let pub_clone = publisher.clone();
        let task = tokio::spawn(async move {
            for j in 0..5 {
                let event = create_submitted_event(
                    format!("concurrent-exec-{}-{}", i, j),
                    "AAPL",
                    Some(format!("client-{}", i))
                );
                pub_clone.publish(event).await.expect("concurrent publish should succeed");
            }
        });
        tasks.push(task);
    }
    
    // Wait for all tasks to complete
    for task in tasks {
        task.await.expect("task should complete");
    }
    
    drop(publisher);
    
    let result = timeout(Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "concurrent publishing should complete");
}

/// Test OrderLifecycleEvent struct directly
#[test]
fn test_order_lifecycle_event_struct() {
    let event = OrderLifecycleEvent {
        event_id: "test-event-id".to_string(),
        execution_id: "test-exec-id".to_string(),
        client_order_id: Some("test-client-id".to_string()),
        symbol: "TEST".to_string(),
        event_type: OrderLifecycleEventType::Filled,
        timestamp: "2026-01-01T00:00:00Z".to_string(),
        payload: serde_json::json!({
            "test_key": "test_value",
            "number": 42
        }),
    };
    
    assert_eq!(event.event_id, "test-event-id");
    assert_eq!(event.execution_id, "test-exec-id");
    assert_eq!(event.client_order_id, Some("test-client-id".to_string()));
    assert_eq!(event.symbol, "TEST");
    assert_eq!(event.event_type, OrderLifecycleEventType::Filled);
    assert_eq!(event.timestamp, "2026-01-01T00:00:00Z");
    assert_eq!(event.payload["test_key"], "test_value");
    assert_eq!(event.payload["number"], 42);
}

/// Test OrderLifecycleEvent with complex nested payload
#[test]
fn test_event_with_complex_payload() {
    let event = OrderLifecycleEvent {
        event_id: "complex-event".to_string(),
        execution_id: "complex-exec".to_string(),
        client_order_id: None,
        symbol: "AAPL".to_string(),
        event_type: OrderLifecycleEventType::PartialFill,
        timestamp: "2026-01-01T00:00:00Z".to_string(),
        payload: serde_json::json!({
            "filled_qty": 100,
            "filled_price": 150.50,
            "remaining_qty": 200,
            "execution_venue": "NYSE",
            "liquidity": "remove",
            "metadata": {
                "algo_id": "algo-123",
                "session_id": "session-abc",
                "tags": ["tag1", "tag2"]
            }
        }),
    };
    
    assert_eq!(event.payload["metadata"]["algo_id"], "algo-123");
    assert_eq!(event.payload["metadata"]["session_id"], "session-abc");
    assert_eq!(event.payload["metadata"]["tags"][0], "tag1");
}
