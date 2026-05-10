// core/domain/order.rs

use serde::{Deserialize, Serialize};

// --- Enums ---

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum TimeInForce {
    Day,
    Gtc,    // Good Till Canceled
    Ioc,    // Immediate Or Cancel
    Fok,    // Fill Or Kill
}

// --- Value Objects ---

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ExecutionId(pub String);

impl From<String> for ExecutionId {
    fn from(s: String) -> Self {
        ExecutionId(s)
    }
}

impl std::fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// --- Commands ---

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderCmd {
    pub symbol: String,
    pub qty: u32,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub client_order_id: Option<String>,
    pub extended_hours: bool,
    pub notional: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CancelCmd {
    pub execution_id: ExecutionId,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ReplaceCmd {
    pub execution_id: ExecutionId,
    pub symbol: String,
    pub side: OrderSide,
    pub qty: Option<u32>,
    pub limit_price: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StatusQuery {
    pub execution_id: ExecutionId,
}

// --- Order Status Response ---

/// Response returned by `query_status` containing the current order state.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct OrderStatusResponse {
    /// The order's execution ID (from the broker)
    pub execution_id: String,
    /// Current status of the order (e.g., "new", "filled", "canceled", "rejected")
    pub status: String,
    /// Symbol being traded
    pub symbol: String,
    /// Side of the order (Buy/Sell)
    pub side: OrderSide,
    /// Original order quantity
    pub qty: u32,
    /// Filled quantity (if partially or fully filled)
    pub filled_qty: u32,
    /// Average fill price (if filled)
    pub avg_fill_price: Option<f64>,
    /// Remaining quantity to be filled
    pub remaining_qty: u32,
    /// Timestamp of the last status update
    pub updated_at: String,
    /// Raw status from the broker (for debugging/completeness)
    pub raw_status: Option<String>,
}

impl OrderStatusResponse {
    /// Create a new OrderStatusResponse with the required fields
    pub fn new(
        execution_id: impl Into<String>,
        status: impl Into<String>,
        symbol: impl Into<String>,
        side: OrderSide,
        qty: u32,
    ) -> Self {
        Self {
            execution_id: execution_id.into(),
            status: status.into(),
            symbol: symbol.into(),
            side,
            qty,
            filled_qty: 0,
            avg_fill_price: None,
            remaining_qty: qty,
            updated_at: chrono::Utc::now().to_rfc3339(),
            raw_status: None,
        }
    }

    /// Set the fill information
    pub fn with_fill(mut self, filled_qty: u32, avg_fill_price: f64) -> Self {
        self.filled_qty = filled_qty;
        self.avg_fill_price = Some(avg_fill_price);
        self.remaining_qty = self.qty.saturating_sub(filled_qty);
        self
    }

    /// Set the raw broker status
    pub fn with_raw_status(mut self, raw: impl Into<String>) -> Self {
        self.raw_status = Some(raw.into());
        self
    }

    /// Check if the order is in a terminal state (filled, canceled, rejected, expired)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "filled" | "canceled" | "cancelled" | "rejected" | "expired" | "done_for_day"
        )
    }

    /// Check if the order is still open
    pub fn is_open(&self) -> bool {
        matches!(
            self.status.as_str(),
            "new" | "accepted" | "pending" | "partially_filled" | "held"
        )
    }
}

// --- Order Lifecycle Events for PUB/SUB ---

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderLifecycleEvent {
    /// Unique identifier for this event
    pub event_id: String,
    /// The order's execution ID (from the broker)
    pub execution_id: String,
    /// Client-provided order ID if available
    pub client_order_id: Option<String>,
    /// Trading symbol
    pub symbol: String,
    /// Event type (submitted, filled, rejected, cancelled, etc.)
    pub event_type: OrderLifecycleEventType,
    /// Timestamp when the event occurred (ISO 8601)
    pub timestamp: String,
    /// Additional event-specific payload
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderLifecycleEventType {
    /// Order was successfully submitted to the broker
    Submitted,
    /// Order was partially filled
    PartialFill,
    /// Order was completely filled
    Filled,
    /// Order was rejected by the broker
    Rejected,
    /// Order was cancelled
    Cancelled,
    /// Order was replaced/modified
    Replaced,
    /// Order expired
    Expired,
    /// Error occurred during order processing
    Error,
}
