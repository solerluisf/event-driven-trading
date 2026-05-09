// src/adapters/broker/alpaca_adapter.rs

use std::sync::Arc;
use std::str::FromStr;
use async_trait::async_trait;
use num_decimal::Num;
use uuid::Uuid;

// apca imports
use apca::Client;
use apca::api::v2::order::{
    Create,
    CreateReqInit,
    Change,
    ChangeReq,
    Delete,
    Get,
    Side,
    Type,
    TimeInForce as ApcaTimeInForce,
    Amount,
    Id as OrderId,
};

// domain types
use crate::core::domain::order::{
    OrderCmd,
    CancelCmd,
    ReplaceCmd,
    StatusQuery,
    ExecutionId,
    OrderSide,
    OrderType,
    TimeInForce,
};

// port trait
use crate::core::ports::execution_port::IExecutionPort;

// unified error
use crate::adapters::broker::broker_error::BrokerError;

pub struct AlpacaBrokerAdapter {
    client: Arc<Client>,
}

impl AlpacaBrokerAdapter {
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    fn parse_order_id(raw: &str) -> Result<OrderId, BrokerError> {
        Uuid::parse_str(raw)
            .map(OrderId)
            .map_err(|_| BrokerError::InvalidOrderId(raw.to_string()))
    }
}

#[async_trait]
impl IExecutionPort for AlpacaBrokerAdapter {
    type Error = BrokerError;

    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, Self::Error> {
        let side = match cmd.side {
            OrderSide::Buy  => Side::Buy,
            OrderSide::Sell => Side::Sell,
        };

        let order_type = match cmd.order_type {
            OrderType::Market    => Type::Market,
            OrderType::Limit     => Type::Limit,
            OrderType::Stop      => Type::Stop,
            OrderType::StopLimit => Type::StopLimit,
        };

        let tif = match cmd.time_in_force {
            TimeInForce::Day => ApcaTimeInForce::Day,
            TimeInForce::Gtc => ApcaTimeInForce::UntilCanceled,
            TimeInForce::Ioc => ApcaTimeInForce::ImmediateOrCancel,
            TimeInForce::Fok => ApcaTimeInForce::FillOrKill,
        };

        let limit_price = cmd.limit_price
            .and_then(|p| Num::from_str(&p.to_string()).ok());
        let stop_price = cmd.stop_price
            .and_then(|p| Num::from_str(&p.to_string()).ok());

        let req = CreateReqInit {
            type_: order_type,
            time_in_force: tif,
            limit_price,
            stop_price,
            ..Default::default()
        }
        .init(&cmd.symbol, side, Amount::quantity(cmd.qty));

        let order = self.client
            .issue::<Create>(&req)
            .await
            .map_err(|e| BrokerError::Unknown(format!("submit failed: {}", e)))?;

        Ok(ExecutionId(order.id.to_string()))
    }

    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), Self::Error> {
        let id = Self::parse_order_id(&cmd.execution_id.0)?;

        self.client
            .issue::<Delete>(&id)
            .await
            .map_err(|e| BrokerError::Unknown(format!("cancel failed: {}", e)))?;

        Ok(())
    }

    async fn replace_order(&self, cmd: ReplaceCmd) -> Result<(), Self::Error> {
        let id = Self::parse_order_id(&cmd.execution_id.0)?;

        // Build the change request with only the fields that should be updated
        let change_req = ChangeReq {
            quantity: cmd.qty.map(|q| Num::from(q)),
            limit_price: cmd.limit_price.and_then(|p| Num::from_str(&p.to_string()).ok()),
            ..Default::default()
        };

        // Use the native PATCH endpoint for atomic order replacement
        // This updates the order in-place without canceling and recreating
        let change_input = (id, change_req);
        
        self.client
            .issue::<Change>(&change_input)
            .await
            .map_err(|e| BrokerError::Unknown(format!("replace failed: {}", e)))?;

        Ok(())
    }

    async fn query_status(&self, query: StatusQuery) -> Result<(), Self::Error> {
        let id = Self::parse_order_id(&query.execution_id.0)?;

        let order = self.client
            .issue::<Get>(&id)
            .await
            .map_err(|e| BrokerError::Unknown(format!("query_status failed: {}", e)))?;

        tracing::info!(
            "order status id={} status={:?}",
            query.execution_id.0,
            order.status
        );

        Ok(())
    }
}
