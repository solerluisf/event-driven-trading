// market_data.rs

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketSubscription {
    pub symbol: String,
}

#[derive(Clone, Debug)]
pub enum MarketDataCommand {
    Subscribe(MarketSubscription),
    Unsubscribe(MarketSubscription),
}
