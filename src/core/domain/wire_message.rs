// core/domain/wire_message.rs
//
// JSON envelope exchanged over the ZeroMQ REQ/REP socket.
// The Execution Service wraps every command in a GatewayRequest and
// unwraps every GatewayResponse on the other side.

use serde::{Deserialize, Serialize};
use crate::core::domain::market_data::MarketSubscription;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery};

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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorPayload {
    pub correlation_id: Option<String>,
    pub code: String,
    pub message: String,
}
