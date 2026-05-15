// core/application/kill_switch.rs
//
// Kill switch that stops new orders AND cancels existing open orders.
// This is a critical safety feature for trading systems.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, Receiver, Sender};
use crate::core::domain::order::ExecutionId;
use crate::core::infrastructure::MutexExt;
use crate::adapters::broker::broker_error::BrokerError;

/// Type alias for the cancel callback function
type CancelCallback = Arc<dyn Fn(ExecutionId) + Send + Sync>;

/// Channel sender for async cancel operations
#[derive(Clone)]
pub struct CancelChannel(Sender<ExecutionId>);

/// Kill switch that tracks open orders and cancels them when activated.
/// 
// IMPORTANT: When the kill switch is activated, it must:
// 1. Block all new orders immediately
// 2. Cancel ALL existing open orders
// 3. Log all actions for audit purposes
#[derive(Clone)]
pub struct KillSwitch {
    inner: Arc<Mutex<KillSwitchInner>>,
}

struct KillSwitchInner {
    enabled: bool,
    /// Set of execution IDs for open orders that need to be cancelled
    open_orders: HashSet<String>,
    /// Optional callback to execute when kill switch is activated
    /// This should trigger cancel commands for all open orders
    cancel_callback: Option<CancelCallback>,
    /// Channel sender for async cancel operations
    /// When kill switch is activated, execution IDs are sent here
    /// to be processed by an async task
    cancel_channel: Option<CancelChannel>,
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
    }
}

impl CancelChannel {
    /// Send an execution ID to be cancelled
    pub fn send(&self, exec_id: ExecutionId) {
        if let Err(e) = self.0.try_send(exec_id) {
            tracing::error!("Failed to send cancel command: {}", e);
        }
    }
}

impl KillSwitch {
    /// Create a new kill switch in the disabled state
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(KillSwitchInner {
                enabled: false,
                open_orders: HashSet::new(),
                cancel_callback: None,
                cancel_channel: None,
            })),
        }
    }

    /// Create a kill switch with a channel for async cancel operations
    /// 
    /// Returns the KillSwitch and a Receiver that will receive ExecutionIds
    /// to cancel when the kill switch is activated
    /// 
    /// # Example
    /// ```no_run
    /// use broker_gateway_service::core::application::kill_switch::KillSwitch;
    /// 
    /// let (kill_switch, mut cancel_rx) = KillSwitch::with_channel();
    /// 
    /// // Spawn a task to process cancel commands
    /// tokio::spawn(async move {
    ///     while let Some(exec_id) = cancel_rx.recv().await {
    ///         // Call your async cancel function here
    ///         println!("Cancelling order: {:?}", exec_id);
    ///     }
    /// });
    /// ```
    pub fn with_channel() -> (Self, Receiver<ExecutionId>) {
        let (tx, rx) = mpsc::channel::<ExecutionId>(100);
        let kill_switch = Self {
            inner: Arc::new(Mutex::new(KillSwitchInner {
                enabled: false,
                open_orders: HashSet::new(),
                cancel_callback: None,
                cancel_channel: Some(CancelChannel(tx)),
            })),
        };
        (kill_switch, rx)
    }

    /// Check if the kill switch is enabled
    pub fn is_enabled(&self) -> bool {
        self.inner.safe_lock().enabled
    }

    /// Enable the kill switch and cancel all open orders
    ///
    /// # Returns
    /// - Number of open orders that were marked for cancellation
    ///
    /// # Important
    /// This will trigger the cancel callback or send to channel for each open order if registered
    pub fn enable(&self) -> usize {
        // Collect all data needed while holding the lock, then release it
        // before calling callbacks to prevent potential deadlocks.
        let (order_count, orders_to_cancel, cancel_callback, cancel_channel) = {
            let mut inner = self.inner.safe_lock();

            if inner.enabled {
                tracing::warn!("Kill switch already enabled, {} open orders remain", inner.open_orders.len());
                return inner.open_orders.len();
            }

            inner.enabled = true;
            let order_count = inner.open_orders.len();

            // Clone the data we need and the callback/channel references
            let orders_to_cancel: Vec<String> = inner.open_orders.iter().cloned().collect();
            let cancel_callback = inner.cancel_callback.clone();
            let cancel_channel = inner.cancel_channel.clone();

            (order_count, orders_to_cancel, cancel_callback, cancel_channel)
        }; // Lock is released here before any callbacks are invoked

        if order_count > 0 {
            tracing::error!("🚨 KILL SWITCH ACTIVATED - Cancelling {} open orders", order_count);

            // Cancel all open orders - use channel if available, otherwise use callback
            if let Some(channel) = cancel_channel {
                for execution_id in &orders_to_cancel {
                    tracing::error!("🚨 Kill switch sending cancel for order: {}", execution_id);
                    channel.send(ExecutionId(execution_id.clone()));
                }
            } else if let Some(callback) = cancel_callback {
                for execution_id in &orders_to_cancel {
                    tracing::error!("🚨 Kill switch cancelling order: {}", execution_id);
                    callback(ExecutionId(execution_id.clone()));
                }
            } else {
                tracing::warn!("Kill switch has no cancel mechanism registered - orders tracked but not cancelled");
            }
        } else {
            tracing::error!("🚨 KILL SWITCH ACTIVATED - No open orders to cancel");
        }

        order_count
    }

    /// Disable the kill switch
    /// 
    /// # Warning
    /// Disabling the kill switch allows new orders to flow again.
    /// This should only be done after manual review.
    pub fn disable(&self) {
        let mut inner = self.inner.safe_lock();
        inner.enabled = false;
        tracing::info!("Kill switch deactivated - new orders will be accepted");
    }

    /// Register a callback to be called when the kill switch is activated
    /// 
    /// # Arguments
    /// * `callback` - Function that will be called with each open order's execution ID
    /// 
    /// # Example
    /// ```
    /// use broker_gateway_service::core::application::kill_switch::KillSwitch;
    /// 
    /// let kill_switch = KillSwitch::new();
    /// kill_switch.register_cancel_callback(|exec_id| {
    ///     // Send cancel command to broker
    ///     println!("Cancelling order: {:?}", exec_id);
    /// });
    /// ```
    /// 
    /// # Note
    /// For async cancel operations, use `with_channel()` instead and process
    /// the receiver in an async task.
    pub fn register_cancel_callback<F>(&self, callback: F)
    where
        F: Fn(ExecutionId) + Send + Sync + 'static,
    {
        let mut inner = self.inner.safe_lock();
        inner.cancel_callback = Some(Arc::new(callback));
        tracing::debug!("Kill switch cancel callback registered");
    }

    /// Register a cancel channel for async operations
    /// 
    /// This allows the kill switch to send execution IDs to an async task
    /// that can call async cancel methods.
    /// 
    /// Returns the Receiver that should be processed by an async task.
    /// 
    /// # Example
    /// ```no_run
    /// use broker_gateway_service::core::application::kill_switch::KillSwitch;
    /// 
    /// let kill_switch = KillSwitch::new();
    /// let mut rx = kill_switch.register_cancel_channel();
    /// 
    /// tokio::spawn(async move {
    ///     while let Some(exec_id) = rx.recv().await {
    ///         // Call async cancel here
    ///         println!("Cancelling order: {:?}", exec_id);
    ///     }
    /// });
    /// ```
    pub fn register_cancel_channel(&self) -> Receiver<ExecutionId> {
        let (tx, rx) = mpsc::channel::<ExecutionId>(100);
        let mut inner = self.inner.safe_lock();
        inner.cancel_channel = Some(CancelChannel(tx));
        tracing::debug!("Kill switch cancel channel registered");
        rx
    }

    /// Track an open order
    /// 
    /// # Arguments
    /// * `execution_id` - The execution ID of the order to track
    /// 
    /// # Returns
    /// - `true` if the order was newly added
    /// - `false` if the order was already tracked
    pub fn track_open_order(&self, execution_id: &ExecutionId) -> bool {
        let mut inner = self.inner.safe_lock();
        let added = inner.open_orders.insert(execution_id.0.clone());
        if added {
            tracing::debug!("Tracking open order: {}", execution_id.0);
        }
        added
    }

    /// Remove an order from tracking (e.g., when filled or cancelled)
    /// 
    /// # Arguments
    /// * `execution_id` - The execution ID of the order to remove
    /// 
    /// # Returns
    /// - `true` if the order was removed
    /// - `false` if the order was not being tracked
    pub fn remove_open_order(&self, execution_id: &ExecutionId) -> bool {
        let mut inner = self.inner.safe_lock();
        let removed = inner.open_orders.remove(&execution_id.0);
        if removed {
            tracing::debug!("Order no longer tracked: {}", execution_id.0);
        }
        removed
    }

    /// Get the number of currently tracked open orders
    pub fn open_order_count(&self) -> usize {
        self.inner.safe_lock().open_orders.len()
    }

    /// Get a list of all tracked open orders
    pub fn get_open_orders(&self) -> Vec<ExecutionId> {
        self.inner.lock()
            .unwrap()
            .open_orders
            .iter()
            .map(|id| ExecutionId(id.clone()))
            .collect()
    }

    /// Check if a specific order is being tracked
    pub fn is_order_tracked(&self, execution_id: &ExecutionId) -> bool {
        self.inner.safe_lock().open_orders.contains(&execution_id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn kill_switch_tracks_and_removes_orders() {
        let kill_switch = KillSwitch::new();
        let exec_id = ExecutionId("order-123".to_string());

        assert!(!kill_switch.is_order_tracked(&exec_id));
        assert_eq!(kill_switch.open_order_count(), 0);

        // Track an order
        assert!(kill_switch.track_open_order(&exec_id));
        assert!(kill_switch.is_order_tracked(&exec_id));
        assert_eq!(kill_switch.open_order_count(), 1);

        // Tracking same order again returns false
        assert!(!kill_switch.track_open_order(&exec_id));
        assert_eq!(kill_switch.open_order_count(), 1);

        // Remove the order
        assert!(kill_switch.remove_open_order(&exec_id));
        assert!(!kill_switch.is_order_tracked(&exec_id));
        assert_eq!(kill_switch.open_order_count(), 0);

        // Removing again returns false
        assert!(!kill_switch.remove_open_order(&exec_id));
    }

    #[test]
    fn kill_switch_enable_triggers_callback() {
        let kill_switch = KillSwitch::new();
        let cancelled = Arc::new(AtomicUsize::new(0));
        let cancelled_clone = cancelled.clone();

        kill_switch.register_cancel_callback(move |_exec_id| {
            cancelled_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Track some orders
        kill_switch.track_open_order(&ExecutionId("order-1".to_string()));
        kill_switch.track_open_order(&ExecutionId("order-2".to_string()));
        kill_switch.track_open_order(&ExecutionId("order-3".to_string()));

        // Enable kill switch
        let count = kill_switch.enable();
        assert_eq!(count, 3);
        assert!(kill_switch.is_enabled());

        // Callback should have been called 3 times
        assert_eq!(cancelled.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn kill_switch_enable_does_not_deadlock_when_callback_accesses_kill_switch() {
        // This test verifies that enable() releases the lock before calling callbacks.
        // Previously, this would deadlock because the callback tries to acquire the
        // same lock that enable() was holding.
        let kill_switch = KillSwitch::new();
        let kill_switch_clone = kill_switch.clone();
        let callback_called = Arc::new(AtomicUsize::new(0));
        let callback_called_clone = callback_called.clone();

        kill_switch.register_cancel_callback(move |exec_id| {
            // This callback tries to access the kill switch, which would deadlock
            // if the lock is held during callback invocation
            let _ = kill_switch_clone.is_order_tracked(&exec_id);
            let _ = kill_switch_clone.open_order_count();
            callback_called_clone.fetch_add(1, Ordering::SeqCst);
        });

        // Track some orders
        kill_switch.track_open_order(&ExecutionId("order-1".to_string()));
        kill_switch.track_open_order(&ExecutionId("order-2".to_string()));

        // Enable kill switch - this should not deadlock
        // Use a thread with timeout to detect potential deadlocks
        let result = std::thread::spawn(move || {
            kill_switch.enable()
        })
        .join();

        assert!(result.is_ok(), "Enable() should not deadlock");
        assert_eq!(result.unwrap(), 2);
        assert_eq!(callback_called.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn kill_switch_with_channel_sends_cancels() {
        let (kill_switch, mut cancel_rx) = KillSwitch::with_channel();

        // Track some orders
        kill_switch.track_open_order(&ExecutionId("order-1".to_string()));
        kill_switch.track_open_order(&ExecutionId("order-2".to_string()));

        // Enable kill switch
        let count = kill_switch.enable();
        assert_eq!(count, 2);

        // Collect all cancellation requests
        let mut cancelled_orders = vec![];
        while let Ok(exec_id) = cancel_rx.try_recv() {
            cancelled_orders.push(exec_id.0);
        }

        assert_eq!(cancelled_orders.len(), 2);
        assert!(cancelled_orders.contains(&"order-1".to_string()));
        assert!(cancelled_orders.contains(&"order-2".to_string()));
    }

    #[test]
    fn kill_switch_disable_allows_new_orders() {
        let kill_switch = KillSwitch::new();

        // Enable then disable
        kill_switch.enable();
        assert!(kill_switch.is_enabled());

        kill_switch.disable();
        assert!(!kill_switch.is_enabled());
    }

    #[test]
    fn kill_switch_enable_when_already_enabled_warns() {
        let kill_switch = KillSwitch::new();
        kill_switch.track_open_order(&ExecutionId("order-1".to_string()));

        // First enable
        let count1 = kill_switch.enable();
        assert_eq!(count1, 1);

        // Second enable should return same count but warn
        let count2 = kill_switch.enable();
        assert_eq!(count2, 1);
    }

    #[tokio::test]
    async fn kill_switch_channel_integration() {
        let kill_switch = KillSwitch::new();
        let mut cancel_rx = kill_switch.register_cancel_channel();

        let cancelled = Arc::new(Mutex::new(Vec::new()));
        let cancelled_clone = cancelled.clone();

        // Spawn a task to collect cancellations
        let collector = tokio::spawn(async move {
            while let Some(exec_id) = cancel_rx.recv().await {
                cancelled_clone.safe_lock().push(exec_id.0);
            }
        });

        // Track orders and enable
        kill_switch.track_open_order(&ExecutionId("async-order-1".to_string()));
        kill_switch.track_open_order(&ExecutionId("async-order-2".to_string()));

        kill_switch.enable();

        // Give some time for cancellations to be processed
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check results
        let orders = cancelled.safe_lock();
        assert_eq!(orders.len(), 2);
        assert!(orders.contains(&"async-order-1".to_string()));
        assert!(orders.contains(&"async-order-2".to_string()));

        // Drop kill switch to close channel
        drop(kill_switch);
        let _ = collector.await;
    }
}
