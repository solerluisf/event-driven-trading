// adapter_factory.rs


use crate::core::domain::broker_config::BrokerConfig;
use crate::core::ports::execution_port::IExecutionPort;
use crate::adapters::broker::broker_error::BrokerError;
use crate::adapters::broker::alpaca_adapter::AlpacaBrokerAdapter;
use crate::adapters::broker::mock_adapter::MockAdapter;
use crate::adapters::broker::fix_adapter::FixBrokerAdapter;
use crate::adapters::broker::rest_adapter::RestBrokerAdapter;
use crate::adapters::broker::websocket_adapter::WebSocketBrokerAdapter;


use std::sync::Mutex;
use apca::Client;

pub struct AdapterFactory {
    alpaca_client: Mutex<Option<Client>>,
}

impl AdapterFactory {
    pub fn new(alpaca_client: Option<Client>) -> Self {
        Self { 
           alpaca_client: Mutex::new(alpaca_client), 
        }
    }

    pub fn create_adapter(
        &self,
        config: BrokerConfig,
    ) -> Box<dyn IExecutionPort<Error = BrokerError>> {
        match config.name.as_str() {
            "alpaca" => {
                let client = self.alpaca_client
                    .lock()
                    .unwrap()
                    .take()
                    .expect("alpaca client not provided or already used");
                Box::new(AlpacaBrokerAdapter::new(client))
            },
            "fix"       => Box::new(FixBrokerAdapter::default()),
            "rest"      => Box::new(RestBrokerAdapter::default()),
            "websocket" => Box::new(WebSocketBrokerAdapter::default()),
            _           => Box::new(MockAdapter::default()),
        }
    }
}