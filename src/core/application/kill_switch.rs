// core/application/kill_switch.rs
//
// Kill switch that stops new orders AND cancels existing open orders.
// This is a critical safety feature for trading systems.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use crate::core::domain::order::ExecutionId;
use crate::adapters::broker::broker_error::BrokerError;

/// Type alias for the cancel callback function
type CancelCallback = Arc<dyn Fn(ExecutionId) + Send + Sync>;

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
}

impl Default for KillSwitch {
    fn default() -> Self {
        Self::new()
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
            })),
        }
    }

    /// Check if the kill switch is enabled
    pub fn is_enabled(&self) -> bool {
        self.inner.lock().unwrap().enabled
    }

    /// Enable the kill switch and cancel all open orders
    /// 
    /// # Returns
    /// - Number of open orders that were marked for cancellation
    /// 
    /// # Important
    /// This will trigger the cancel callback for each open order if registered
    pub fn enable(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        
        if inner.enabled {
            tracing::warn!("Kill switch already enabled, {} open orders remain", inner.open_orders.len());
            return inner.open_orders.len();
        }
        
        inner.enabled = true;
        let order_count = inner.open_orders.len();
        
        if order_count > 0 {
            tracing::error!("🚨 KILL SWITCH ACTIVATED - Cancelling {} open orders", order_count);
            
            // Cancel all open orders
            if let Some(ref callback) = inner.cancel_callback {
                for execution_id in &inner.open_orders {
                    tracing::error!("🚨 Kill switch cancelling order: {}", execution_id);
                    callback(ExecutionId(execution_id.clone()));
                }
            } else {
                tracing::warn!("Kill switch has no cancel callback registered - orders tracked but not cancelled");
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
        let mut inner = self.inner.lock().unwrap();
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
    pub fn register_cancel_callback<F>(&self, callback: F)
    where
        F: Fn(ExecutionId) + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock().unwrap();
        inner.cancel_callback = Some(Arc::new(callback));
        tracing::debug!("Kill switch cancel callback registered");
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
        let mut inner = self.inner.lock().unwrap();
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
        let mut inner = self.inner.lock().unwrap();
        let removed = inner.open_orders.remove(&execution_id.0);
        if removed {
            tracing::debug!("Order no longer tracked: {}", execution_id.0);
        }
        removed
    }

    /// Get the number of currently tracked open orders
    pub fn open_order_count(&self) -> usize {
        self.inner.lock().unwrap().open_orders.len()
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
        self.inner.lock().unwrap().open_orders.contains(&execution_id.0)
    }
}
