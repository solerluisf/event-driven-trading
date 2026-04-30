// adapters/broker/fix_adapter.rs

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
pub struct FixBrokerAdapter;

#[async_trait]
impl IExecutionPort for FixBrokerAdapter {
    type Error = BrokerError;

    async fn submit_order(&self, _cmd: OrderCmd) -> Result<ExecutionId, Self::Error> {
        todo!()
    }

    async fn cancel_order(&self, _cmd: CancelCmd) -> Result<(), Self::Error> {
        todo!()
    }

    async fn replace_order(&self, _cmd: ReplaceCmd) -> Result<(), Self::Error> {
        todo!()
    }

    async fn query_status(&self, _query: StatusQuery) -> Result<(), Self::Error> {
        todo!()
    }
}