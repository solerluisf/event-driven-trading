// market_data_port.rs

use crate::core::domain::market_data::MarketSubscription;

pub trait IMarketDataPort {
    fn subscribe(&self, sub: MarketSubscription);
    fn unsubscribe(&self, sub: MarketSubscription);
}


