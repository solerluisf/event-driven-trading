// adapter_factory.rs

pub struct AdapterFactory;

impl AdapterFactory {
    pub fn create_adapter(&self, config: BrokerConfig) -> Box<dyn IBrokerAdapter> {
        match config.name.as_str() {
            "rest" => Box::new(RestBrokerAdapter::default()),
            "websocket" => Box::new(WebSocketBrokerAdapter::default()),
            "fix" => Box::new(FixBrokerAdapter::default()),
            _ => Box::new(MockAdapter::default()),
        }
    }
}
