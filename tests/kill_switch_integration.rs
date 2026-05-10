// tests/kill_switch_integration.rs
//
// Integration tests for kill switch order cancellation functionality.
// Tests the complete flow from order tracking to cancellation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use broker_gateway_service::core::application::kill_switch::KillSwitch;
use broker_gateway_service::core::domain::order::ExecutionId;

#[test]
fn test_kill_switch_starts_disabled() {
    let ks = KillSwitch::new();
    assert!(!ks.is_enabled());
}

#[test]
fn test_kill_switch_enable_disable() {
    let ks = KillSwitch::new();
    
    assert!(!ks.is_enabled());
    
    ks.enable();
    assert!(ks.is_enabled());
    
    ks.disable();
    assert!(!ks.is_enabled());
}

#[test]
fn test_kill_switch_tracks_open_orders() {
    let ks = KillSwitch::new();
    let exec_id = ExecutionId("test-order-1".to_string());
    
    // Initially no orders tracked
    assert_eq!(ks.open_order_count(), 0);
    assert!(!ks.is_order_tracked(&exec_id));
    
    // Track an order
    assert!(ks.track_open_order(&exec_id));
    assert_eq!(ks.open_order_count(), 1);
    assert!(ks.is_order_tracked(&exec_id));
    
    // Tracking same order again returns false
    assert!(!ks.track_open_order(&exec_id));
    assert_eq!(ks.open_order_count(), 1);
}

#[test]
fn test_kill_switch_removes_orders() {
    let ks = KillSwitch::new();
    let exec_id = ExecutionId("test-order-1".to_string());
    
    ks.track_open_order(&exec_id);
    assert_eq!(ks.open_order_count(), 1);
    
    // Remove the order
    assert!(ks.remove_open_order(&exec_id));
    assert_eq!(ks.open_order_count(), 0);
    assert!(!ks.is_order_tracked(&exec_id));
    
    // Removing again returns false
    assert!(!ks.remove_open_order(&exec_id));
}

#[test]
fn test_kill_switch_get_open_orders() {
    let ks = KillSwitch::new();
    let exec_ids = vec![
        ExecutionId("order-1".to_string()),
        ExecutionId("order-2".to_string()),
        ExecutionId("order-3".to_string()),
    ];
    
    for exec_id in &exec_ids {
        ks.track_open_order(exec_id);
    }
    
    let tracked = ks.get_open_orders();
    assert_eq!(tracked.len(), 3);
    
    for exec_id in exec_ids {
        assert!(tracked.contains(&exec_id));
    }
}

#[test]
fn test_kill_switch_cancels_orders_on_enable() {
    let ks = KillSwitch::new();
    let cancelled_count = Arc::new(AtomicUsize::new(0));
    let cancelled_count_clone = cancelled_count.clone();
    
    // Register cancel callback
    ks.register_cancel_callback(move |_exec_id| {
        cancelled_count_clone.fetch_add(1, Ordering::SeqCst);
    });
    
    // Track some orders
    ks.track_open_order(&ExecutionId("order-1".to_string()));
    ks.track_open_order(&ExecutionId("order-2".to_string()));
    ks.track_open_order(&ExecutionId("order-3".to_string()));
    
    assert_eq!(ks.open_order_count(), 3);
    
    // Enable kill switch
    let count = ks.enable();
    assert_eq!(count, 3);
    assert!(ks.is_enabled());
    
    // Verify all orders were cancelled
    assert_eq!(cancelled_count.load(Ordering::SeqCst), 3);
}

#[test]
fn test_kill_switch_enable_returns_count_even_without_callback() {
    let ks = KillSwitch::new();
    
    // Track orders without registering callback
    ks.track_open_order(&ExecutionId("order-1".to_string()));
    ks.track_open_order(&ExecutionId("order-2".to_string()));
    
    let count = ks.enable();
    assert_eq!(count, 2);
    assert!(ks.is_enabled());
}

#[test]
fn test_kill_switch_enable_idempotent() {
    let ks = KillSwitch::new();
    let cancelled_count = Arc::new(AtomicUsize::new(0));
    let cancelled_count_clone = cancelled_count.clone();
    
    ks.register_cancel_callback(move |_exec_id| {
        cancelled_count_clone.fetch_add(1, Ordering::SeqCst);
    });
    
    ks.track_open_order(&ExecutionId("order-1".to_string()));
    
    // First enable
    let count1 = ks.enable();
    assert_eq!(count1, 1);
    assert_eq!(cancelled_count.load(Ordering::SeqCst), 1);
    
    // Second enable should not trigger callback again
    let count2 = ks.enable();
    assert_eq!(count2, 1); // Still returns count but doesn't re-cancel
    assert_eq!(cancelled_count.load(Ordering::SeqCst), 1); // Callback not called again
}

#[test]
fn test_kill_switch_cloning_shares_state() {
    let ks1 = KillSwitch::new();
    let ks2 = ks1.clone();
    
    let exec_id = ExecutionId("test-order".to_string());
    
    ks1.track_open_order(&exec_id);
    assert!(ks2.is_order_tracked(&exec_id));
    
    ks2.enable();
    assert!(ks1.is_enabled());
    assert!(ks2.is_enabled());
}

#[test]
fn test_kill_switch_tracks_correct_order_ids() {
    let ks = KillSwitch::new();
    let cancelled_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let cancelled_ids_clone = cancelled_ids.clone();
    
    ks.register_cancel_callback(move |exec_id| {
        cancelled_ids_clone.lock().unwrap().push(exec_id.0.clone());
    });
    
    let order1 = ExecutionId("order-abc-123".to_string());
    let order2 = ExecutionId("order-def-456".to_string());
    
    ks.track_open_order(&order1);
    ks.track_open_order(&order2);
    
    ks.enable();
    
    let ids = cancelled_ids.lock().unwrap();
    assert!(ids.contains(&"order-abc-123".to_string()));
    assert!(ids.contains(&"order-def-456".to_string()));
}

#[test]
fn test_kill_switch_cancels_all_orders_on_activation() {
    // Integration test simulating real-world scenario
    let ks = KillSwitch::new();
    let cancelled_orders = Arc::new(std::sync::Mutex::new(Vec::new()));
    let cancelled_orders_clone = cancelled_orders.clone();
    
    // Simulate order service registering callback
    ks.register_cancel_callback(move |exec_id| {
        println!("Cancelling order: {}", exec_id.0);
        cancelled_orders_clone.lock().unwrap().push(exec_id.0.clone());
    });
    
    // Simulate submitting 5 orders
    for i in 0..5 {
        let exec_id = ExecutionId(format!("order-{}", i));
        ks.track_open_order(&exec_id);
    }
    
    assert_eq!(ks.open_order_count(), 5);
    
    // Activate kill switch (emergency stop)
    let count = ks.enable();
    
    // Verify
    assert_eq!(count, 5);
    assert!(ks.is_enabled());
    
    let orders = cancelled_orders.lock().unwrap();
    assert_eq!(orders.len(), 5);
    
    // Verify all order IDs are correct
    for i in 0..5 {
        assert!(orders.contains(&format!("order-{}", i)));
    }
}
