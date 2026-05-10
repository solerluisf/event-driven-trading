// tests/telemetry_integration.rs
//
// Integration tests for TelemetryDecorator
// Verifies that telemetry events capture all required metrics

use std::sync::{Arc, Mutex};
use std::time::Duration;
use broker_gateway_service::core::patterns::telemetry_decorator::{
    TelemetryDecorator, TelemetryEvent, TelemetryEventBuilder, TraceId
};

#[test]
fn test_telemetry_measures_latency_accurately() {
    let decorator = TelemetryDecorator::new();
    let received_event = Arc::new(Mutex::new(None));
    let received_clone = received_event.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    // Wrap an operation that takes a known amount of time
    let (result, _event) = decorator.wrap_outbound("test_operation", "alpaca", || {
        std::thread::sleep(Duration::from_millis(50));
        "success"
    });
    
    assert_eq!(result, "success");
    
    let event = received_event.lock().unwrap().clone().expect("Event should be received");
    // Allow some tolerance for timing
    assert!(event.latency >= Duration::from_millis(45), 
        "Latency should be at least 45ms, got {:?}", event.latency);
}

#[test]
fn test_telemetry_captures_all_required_fields() {
    let decorator = TelemetryDecorator::new();
    let received_event = Arc::new(Mutex::new(None));
    let received_clone = received_event.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    decorator.wrap_outbound("submit_order", "alpaca", || 42);
    
    let event = received_event.lock().unwrap().clone().expect("Event should be received");
    
    // Verify all required fields are present
    assert!(!event.trace_id.0.is_empty(), "Trace ID should not be empty");
    assert_eq!(event.operation, "submit_order");
    assert_eq!(event.target, "alpaca");
    assert!(event.success);
    
    // Latency should be measured
    assert!(event.latency > Duration::from_nanos(0), "Latency should be measured");
}

#[test]
fn test_telemetry_with_status_code_and_payload_sizes() {
    let decorator = TelemetryDecorator::new();
    let received_event = Arc::new(Mutex::new(None));
    let received_clone = received_event.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    // Use manual operation tracking for full control
    let trace_id = decorator.start_operation("api_call");
    
    decorator.complete_operation(
        trace_id,
        Some(200),           // HTTP status code
        1024,                // Request payload: 1KB
        2048,                // Response payload: 2KB
        None,                // No error
    );
    
    let event = received_event.lock().unwrap().clone().expect("Event should be received");
    
    assert_eq!(event.status_code, Some(200));
    assert_eq!(event.request_payload_size, 1024);
    assert_eq!(event.response_payload_size, 2048);
    assert!(event.success);
}

#[test]
fn test_telemetry_captures_error_information() {
    let decorator = TelemetryDecorator::new();
    let received_event = Arc::new(Mutex::new(None));
    let received_clone = received_event.clone();
    
    decorator.register_callback(move |event| {
        *received_clone.lock().unwrap() = Some(event);
    });
    
    let trace_id = decorator.start_operation("failed_operation");
    
    decorator.complete_operation(
        trace_id,
        Some(500),
        512,
        0,
        Some(("ConnectionTimeout".to_string(), "Connection to broker timed out".to_string())),
    );
    
    let event = received_event.lock().unwrap().clone().expect("Event should be received");
    
    assert!(!event.success);
    assert_eq!(event.status_code, Some(500));
    assert_eq!(event.error_type, Some("ConnectionTimeout".to_string()));
    assert_eq!(event.error_message, Some("Connection to broker timed out".to_string()));
}

#[test]
fn test_event_builder_creates_complete_events() {
    let event = TelemetryEvent::builder("cancel_order", "alpaca")
        .status_code(200)
        .request_payload_size(256)
        .response_payload_size(128)
        .build();
    
    assert_eq!(event.operation, "cancel_order");
    assert_eq!(event.target, "alpaca");
    assert_eq!(event.status_code, Some(200));
    assert_eq!(event.request_payload_size, 256);
    assert_eq!(event.response_payload_size, 128);
    assert!(event.success);
}

#[test]
fn test_event_builder_with_error() {
    let event = TelemetryEvent::builder("modify_order", "fix")
        .status_code(400)
        .request_payload_size(512)
        .failed("InvalidOrderState", "Cannot modify filled order")
        .build();
    
    assert!(!event.success);
    assert_eq!(event.error_type, Some("InvalidOrderState".to_string()));
    assert_eq!(event.error_message, Some("Cannot modify filled order".to_string()));
}

#[test]
fn test_trace_ids_are_unique() {
    let mut ids = std::collections::HashSet::new();
    
    for _ in 0..100 {
        let id = TraceId::new();
        assert!(ids.insert(id), "Duplicate trace ID generated");
    }
}

#[test]
fn test_telemetry_factory_is_reusable() {
    let decorator = TelemetryDecorator::new();
    let event_count = Arc::new(Mutex::new(0));
    let event_count_clone = event_count.clone();
    
    decorator.register_callback(move |_event| {
        *event_count_clone.lock().unwrap() += 1;
    });
    
    // Call wrap_outbound multiple times
    for i in 0..10 {
        let (result, _) = decorator.wrap_outbound(format!("op_{}", i), "alpaca", || i);
        assert_eq!(result, i);
    }
    
    assert_eq!(*event_count.lock().unwrap(), 10);
}

#[test]
fn test_telemetry_thread_safety() {
    use std::thread;
    
    let decorator = Arc::new(TelemetryDecorator::new());
    let event_count = Arc::new(Mutex::new(0));
    let event_count_clone = event_count.clone();
    
    decorator.register_callback(move |_event| {
        *event_count_clone.lock().unwrap() += 1;
    });
    
    let mut handles = vec![];
    
    // Spawn multiple threads
    for i in 0..5 {
        let dec_clone = Arc::clone(&decorator);
        let handle = thread::spawn(move || {
            for j in 0..5 {
                dec_clone.wrap_outbound(format!("thread_{}_op_{}", i, j), "alpaca", || ());
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().expect("Thread should complete");
    }
    
    assert_eq!(*event_count.lock().unwrap(), 25);
}

#[test]
fn test_active_operations_tracking() {
    let decorator = TelemetryDecorator::new();
    
    assert_eq!(decorator.active_operation_count(), 0);
    
    let id1 = decorator.start_operation("op1");
    let id2 = decorator.start_operation("op2");
    let id3 = decorator.start_operation("op3");
    
    assert_eq!(decorator.active_operation_count(), 3);
    
    let trace_ids = decorator.active_trace_ids();
    assert!(trace_ids.contains(&id1));
    assert!(trace_ids.contains(&id2));
    assert!(trace_ids.contains(&id3));
    
    // Complete one operation
    decorator.complete_operation(id1, None, 0, 0, None);
    assert_eq!(decorator.active_operation_count(), 2);
    
    // Complete remaining
    decorator.complete_operation(id2, None, 0, 0, None);
    decorator.complete_operation(id3, None, 0, 0, None);
    assert_eq!(decorator.active_operation_count(), 0);
}

#[test]
fn test_callback_not_called_when_none_registered() {
    let decorator = TelemetryDecorator::new();
    
    // Should not panic even without callback
    let (result, _event) = decorator.wrap_outbound("test", "target", || 42);
    assert_eq!(result, 42);
}

#[test]
fn test_has_callback_returns_correct_state() {
    let decorator = TelemetryDecorator::new();
    assert!(!decorator.has_callback());
    
    decorator.register_callback(|_event| {});
    assert!(decorator.has_callback());
}

#[test]
fn test_different_broker_targets() {
    let decorator = TelemetryDecorator::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    
    decorator.register_callback(move |event| {
        events_clone.lock().unwrap().push(event);
    });
    
    // Simulate calls to different brokers
    decorator.wrap_outbound("submit", "alpaca", || ());
    decorator.wrap_outbound("submit", "fix", || ());
    decorator.wrap_outbound("submit", "websocket", || ());
    
    let all_events = events.lock().unwrap();
    assert_eq!(all_events.len(), 3);
    
    // Verify targets are correctly captured
    let targets: Vec<_> = all_events.iter().map(|e| e.target.clone()).collect();
    assert!(targets.contains(&"alpaca".to_string()));
    assert!(targets.contains(&"fix".to_string()));
    assert!(targets.contains(&"websocket".to_string()));
}
