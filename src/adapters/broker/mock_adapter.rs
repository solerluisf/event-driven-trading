// adapters/broker/mock_adapter.rs

use async_trait::async_trait;
use crate::core::ports::execution_port::IExecutionPort;
use crate::core::domain::order::{
    OrderCmd,
    CancelCmd,
    ReplaceCmd,
    StatusQuery,
    ExecutionId,
};
use crate::adapters::broker::broker_error::BrokerError;

#[derive(Default)]
pub struct MockAdapter;

#[async_trait]
impl IExecutionPort for MockAdapter {
    type Error = BrokerError;

    async fn submit_order(&self, _cmd: OrderCmd) -> Result<ExecutionId, Self::Error> {
        Ok(ExecutionId("mock-execution-id".into()))
    }

    async fn cancel_order(&self, _cmd: CancelCmd) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn replace_order(&self, _cmd: ReplaceCmd) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn query_status(&self, _query: StatusQuery) -> Result<(), Self::Error> {
        Ok(())
    }
}