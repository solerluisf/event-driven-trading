// core/ports/orchestration_port.rs

use async_trait::async_trait;
use serde_json::Value;

pub struct OrchestrationMessage {
    pub command_type: String,
    pub payload: Value,
    pub received_ns: u64,
}

#[async_trait]
pub trait IOrchestrationReceiver: Send + Sync {
    async fn recv(&self) -> Option<OrchestrationMessage>;
}
