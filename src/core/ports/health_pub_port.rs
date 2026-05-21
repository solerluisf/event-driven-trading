// core/ports/health_pub_port.rs

use async_trait::async_trait;
use crate::core::domain::gateway_health::GatewayHealthSnapshot;
use crate::adapters::broker::broker_error::BrokerError;

#[async_trait]
pub trait IHealthPublisher: Send + Sync {
    async fn publish_health(&self, snapshot: &GatewayHealthSnapshot) -> Result<(), BrokerError>;
}
