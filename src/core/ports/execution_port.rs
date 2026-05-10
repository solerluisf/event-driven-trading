// core/ports/execution_port.rs
use async_trait::async_trait;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, ExecutionId, OrderStatusResponse};

#[async_trait]
pub trait IExecutionPort: Send + Sync {
    type Error: std::error::Error + Send + Sync;

    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, Self::Error>;
    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), Self::Error>;
    async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), Self::Error>;
    async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, Self::Error>;
}