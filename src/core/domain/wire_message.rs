// core/domain/wire_message.rs
//
// JSON envelope exchanged over the ZeroMQ REQ/REP socket.
// The Execution Service wraps every command in a GatewayRequest and
// unwraps every GatewayResponse on the other side.

use serde::{Deserialize, Serialize};
use crate::core::domain::market_data::MarketSubscription;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery};

/// Back-pressure information included in responses when broker quota is near.
/// This signals to the Execution Service that it should slow down.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct BackPressureInfo {
    /// Broker ID that is near its quota.
    pub broker_id: String,
    /// Current token count remaining.
    pub tokens_remaining: f64,
    /// Maximum token capacity.
    pub capacity: f64,
    /// Percentage of capacity remaining (0.0 - 100.0).
    pub percent_remaining: f64,
    /// True if the broker is near its rate limit.
    pub is_near_limit: bool,
    /// Recommended action for the Execution Service.
    pub recommendation: BackPressureRecommendation,
}

/// Recommendation for the Execution Service on how to handle back-pressure.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackPressureRecommendation {
    /// Continue normal operation.
    Normal,
    /// Slow down request rate.
    SlowDown,
    /// Pause sending requests temporarily.
    Pause,
}

// ── Inbound (Execution Service → Gateway) ────────────────────────────────────

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "command", content = "payload", rename_all = "snake_case")]
pub enum GatewayRequest {
    SubmitOrder(OrderCmd),
    CancelOrder(CancelCmd),
    ReplaceOrder(ReplaceCmd),
    QueryStatus(StatusQuery),
    #[serde(rename = "subscribe")]
    Subscribe(MarketSubscription),
    #[serde(rename = "unsubscribe")]
    Unsubscribe(MarketSubscription),
}

// ── Outbound (Gateway → Execution Service) ───────────────────────────────────

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "status", content = "payload", rename_all = "snake_case")]
pub enum GatewayResponse {
    Ok(ResponsePayload),
    Err(ErrorPayload),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResponsePayload {
    /// Echoed from the request so the caller can correlate.
    pub correlation_id: Option<String>,
    /// Human-readable result or execution ID.
    pub result: String,
    /// Optional back-pressure information when broker quota is near.
    /// When present, the Execution Service should adjust its request rate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub back_pressure: Option<BackPressureInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorPayload {
    pub correlation_id: Option<String>,
    pub code: String,
    pub message: String,
}
