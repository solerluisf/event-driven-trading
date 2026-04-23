// market_data_port.rs


pub trait IMarketDataPort {
    fn subscribe(&self, sub: MarketSubscription);
    fn unsubscribe(&self, sub: MarketSubscription);
}


