// market_data.rs

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketSubscription {
    pub symbol: String,
    /// Optional correlation ID for end-to-end request tracking.
    /// If provided, this will be echoed back in the response.
    pub correlation_id: Option<String>,
}

#[derive(Clone, Debug)]
pub enum MarketDataCommand {
    Subscribe(MarketSubscription),
    Unsubscribe(MarketSubscription),
}
