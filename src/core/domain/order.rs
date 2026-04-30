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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplaceCmd {
    pub execution_id: ExecutionId,
    pub qty: Option<u32>,
    pub limit_price: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StatusQuery {
    pub execution_id: ExecutionId,
}
