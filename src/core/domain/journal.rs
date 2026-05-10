// core/domain/journal.rs

use serde::{Deserialize, Serialize};
use crate::core::domain::request::RequestEnvelope;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RequestRecord {
    pub id: String,
    /// Raw JSON payload of the outbound command
    pub raw_payload: Option<String>,
    /// Optional correlation ID for end-to-end request tracking
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ResponseRecord {
    pub id: String,
    /// Raw JSON payload of the inbound confirmation
    pub raw_payload: Option<String>,
    /// Optional correlation ID echoed back from the request
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ValidatedRequest {
    pub envelope: RequestEnvelope,
}