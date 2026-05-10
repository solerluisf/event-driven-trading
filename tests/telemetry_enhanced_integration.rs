// tests/telemetry_enhanced_integration.rs
//
// Integration tests for enhanced TelemetryDecorator with error tracking
// Verifies that wrap_outbound_result properly captures errors and latency

use std::sync::{Arc, Mutex};
use std::time::Duration;
use broker_gateway_service::core::patterns::telemetry_decorator::{TelemetryDecorator, TelemetryEvent};

#[derive(Debug)]
enum TestBrokerError {
    RateLimited,
    ConnectionFailed(String),
    InvalidOrder(String),
}

impl std::fmt::Display for TestBrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestBrokerError::RateLimited => write!(f, "Rate limit exceeded"),
            TestBrokerError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            TestBrokerError::InvalidOrder(msg) => write!(f, "Invalid order: {}", msg),
        }
    }
}

fn classify_broker_error(e: &TestBrokerError) -> &'static str {
    match e {
        TestBrokerError::RateLimited => "rate_limited",
        TestBrokerError::ConnectionFailed(_) => "connection_error",
        TestBrokerError::InvalidOrder(_) => "invalid_order",
    }
}

#[test]
fn test_wrap_outbound_result_captures_success() {
    let decorator = TelemetryDecorator::new();
    let received = Arc::new(Mutex::new(None));
    let received_clone = received.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    // Successful operation
    let (result, event) = decorator.wrap_outbound_result(
        "submit_order",
        "alpaca",
        || Ok::<i32, TestBrokerError>(42),
        Some(classify_broker_error),
    );
    
    assert_eq!(result.unwrap(), 42);
    assert!(event.success);
    assert!(event.error_type.is_none());
    assert!(event.error_message.is_none());
    assert!(event.latency > Duration::from_nanos(0));
    assert_eq!(event.operation, "submit_order");
    assert_eq!(event.target, "alpaca");
    
    // Verify callback received the success event
    let received_event = received.lock().unwrap().clone().unwrap();
    assert!(received_event.success);
}

#[test]
fn test_wrap_outbound_result_captures_rate_limit_error() {
    let decorator = TelemetryDecorator::new();
    let received = Arc::new(Mutex::new(None));
    let received_clone = received.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    // Rate limited error
    let (result, event) = decorator.wrap_outbound_result(
        "submit_order",
        "alpaca",
        || Err::<i32, TestBrokerError>(TestBrokerError::RateLimited),
        Some(classify_broker_error),
    );
    
    assert!(result.is_err());
    assert!(!event.success);
    assert_eq!(event.error_type, Some("rate_limited".to_string()));
    assert_eq!(event.error_message, Some("Rate limit exceeded".to_string()));
    
    // Verify callback received the error event with classification
    let received_event = received.lock().unwrap().clone().unwrap();
    assert!(!received_event.success);
    assert_eq!(received_event.error_type, Some("rate_limited".to_string()));
}

#[test]
fn test_wrap_outbound_result_captures_connection_error() {
    let decorator = TelemetryDecorator::new();
    
    let (result, event) = decorator.wrap_outbound_result(
        "cancel_order",
        "alpaca",
        || Err::<(), TestBrokerError>(TestBrokerError::ConnectionFailed("timeout".to_string())),
        Some(classify_broker_error),
    );
    
    assert!(result.is_err());
    assert!(!event.success);
    assert_eq!(event.error_type, Some("connection_error".to_string()));
    assert!(event.error_message.as_ref().unwrap().contains("timeout"));
}

#[test]
fn test_wrap_outbound_result_without_classifier() {
    let decorator = TelemetryDecorator::new();
    
    // Without error classifier, error_type should be None but error_message should be set
    let (result, event) = decorator.wrap_outbound_result(
        "modify_order",
        "alpaca",
        || Err::<i32, TestBrokerError>(TestBrokerError::InvalidOrder("Bad price".to_string())),
        None, // No classifier
    );
    
    assert!(result.is_err());
    assert!(!event.success);
    assert!(event.error_type.is_none()); // No classifier = no error type
    assert_eq!(event.error_message, Some("Invalid order: Bad price".to_string()));
}

#[test]
fn test_wrap_outbound_result_measures_latency_for_errors() {
    let decorator = TelemetryDecorator::new();
    
    let (_, event) = decorator.wrap_outbound_result(
        "slow_failed_op",
        "alpaca",
        || {
            std::thread::sleep(Duration::from_millis(50));
            Err::<i32, TestBrokerError>(TestBrokerError::ConnectionFailed("timeout".to_string()))
        },
        Some(classify_broker_error),
    );
    
    // Even for errors, latency should be measured
    assert!(event.latency >= Duration::from_millis(45));
    assert!(!event.success);
}

#[test]
fn test_telemetry_events_are_structured_not_strings() {
    let decorator = TelemetryDecorator::new();
    let received = Arc::new(Mutex::new(None));
    let received_clone = received.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    decorator.wrap_outbound_result(
        "test_op",
        "test_target",
        || Ok::<i32, TestBrokerError>(123),
        None,
    );
    
    let event = received.lock().unwrap().clone().unwrap();
    
    // Verify structured fields are populated
    assert!(!event.trace_id.0.is_empty());
    assert_eq!(event.operation, "test_op");
    assert_eq!(event.target, "test_target");
    assert!(event.latency > Duration::from_nanos(0));
    assert!(event.success);
}

#[test]
fn test_error_categorization_is_accurate() {
    let decorator = TelemetryDecorator::new();
    
    // Test each error type gets correct classification
    let (_, event) = decorator.wrap_outbound_result(
        "test",
        "alpaca",
        || Err::<i32, TestBrokerError>(TestBrokerError::RateLimited),
        Some(classify_broker_error),
    );
    assert_eq!(event.error_type, Some("rate_limited".to_string()));
    
    let (_, event) = decorator.wrap_outbound_result(
        "test",
        "alpaca",
        || Err::<i32, TestBrokerError>(TestBrokerError::ConnectionFailed("error".to_string())),
        Some(classify_broker_error),
    );
    assert_eq!(event.error_type, Some("connection_error".to_string()));
    
    let (_, event) = decorator.wrap_outbound_result(
        "test",
        "alpaca",
        || Err::<i32, TestBrokerError>(TestBrokerError::InvalidOrder("bad".to_string())),
        Some(classify_broker_error),
    );
    assert_eq!(event.error_type, Some("invalid_order".to_string()));
}

#[test]
fn test_multiple_operations_track_independently() {
    let decorator = TelemetryDecorator::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    
    decorator.register_callback(move |event| {
        events_clone.lock().unwrap().push(event);
    });
    
    // Mix of successes and failures
    decorator.wrap_outbound_result(
        "op1",
        "target1",
        || Ok::<i32, TestBrokerError>(1),
        None,
    );
    
    decorator.wrap_outbound_result(
        "op2",
        "target2",
        || Err::<i32, TestBrokerError>(TestBrokerError::RateLimited),
        Some(classify_broker_error),
    );
    
    decorator.wrap_outbound_result(
        "op3",
        "target3",
        || Ok::<i32, TestBrokerError>(3),
        None,
    );
    
    let all_events = events.lock().unwrap();
    assert_eq!(all_events.len(), 3);
    
    // Verify each event has correct fields
    assert!(all_events[0].success);
    assert!(!all_events[1].success);
    assert_eq!(all_events[1].error_type, Some("rate_limited".to_string()));
    assert!(all_events[2].success);
    
    // Verify operations are tracked correctly
    assert_eq!(all_events[0].operation, "op1");
    assert_eq!(all_events[1].operation, "op2");
    assert_eq!(all_events[2].operation, "op3");
}

#[test]
fn test_legacy_wrap_outbound_still_works() {
    let decorator = TelemetryDecorator::new();
    let received = Arc::new(Mutex::new(None));
    let received_clone = received.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    // Legacy API still works for non-Result operations
    let (result, event) = decorator.wrap_outbound("legacy_op", "legacy_target", || {
        std::thread::sleep(Duration::from_millis(10));
        42
    });
    
    assert_eq!(result, 42);
    assert!(event.success); // Legacy always reports success
    assert!(event.latency >= Duration::from_millis(10));
    
    // But it can't capture errors (always success=true)
    let (_, event) = decorator.wrap_outbound("always_success", "target", || {
        // Even if we panic internally, the event would show success
        // because the signature can't capture errors
        0
    });
    assert!(event.success);
}

#[tokio::test]
async fn test_async_version_works() {
    let decorator = TelemetryDecorator::new();
    
    // Async success
    let (result, event) = decorator.wrap_outbound_result_async(
        "async_submit",
        "alpaca",
        || async { Ok::<i32, TestBrokerError>(42) },
        Some(classify_broker_error),
    ).await;
    
    assert_eq!(result.unwrap(), 42);
    assert!(event.success);
    
    // Async error
    let (result, event) = decorator.wrap_outbound_result_async(
        "async_submit",
        "alpaca",
        || async { Err::<i32, TestBrokerError>(TestBrokerError::RateLimited) },
        Some(classify_broker_error),
    ).await;
    
    assert!(result.is_err());
    assert!(!event.success);
    assert_eq!(event.error_type, Some("rate_limited".to_string()));
}

#[test]
fn test_latency_histogram_by_error_type() {
    let decorator = TelemetryDecorator::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    
    decorator.register_callback(move |event| {
        events_clone.lock().unwrap().push(event);
    });
    
    // Simulate different error types with different latencies
    decorator.wrap_outbound_result(
        "fast_error",
        "alpaca",
        || Err::<i32, TestBrokerError>(TestBrokerError::RateLimited),
        Some(classify_broker_error),
    );
    
    std::thread::sleep(Duration::from_millis(20));
    
    decorator.wrap_outbound_result(
        "slow_error",
        "alpaca",
        || {
            std::thread::sleep(Duration::from_millis(50));
            Err::<i32, TestBrokerError>(TestBrokerError::ConnectionFailed("slow".to_string()))
        },
        Some(classify_broker_error),
    );
    
    let all_events = events.lock().unwrap();
    
    // Both should be errors with different latencies
    assert!(!all_events[0].success);
    assert!(!all_events[1].success);
    assert_eq!(all_events[0].error_type, Some("rate_limited".to_string()));
    assert_eq!(all_events[1].error_type, Some("connection_error".to_string()));
    
    // Second error should have higher latency
    assert!(all_events[1].latency > all_events[0].latency);
}
