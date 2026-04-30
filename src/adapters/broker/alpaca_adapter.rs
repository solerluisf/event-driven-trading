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
    Delete,
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
}

#[async_trait]
impl IExecutionPort for AlpacaBrokerAdapter {
    type Error = BrokerError;

    async fn submit_order(&self, cmd: OrderCmd) -> Result<ExecutionId, Self::Error> {
        // map domain side → apca side
        let side = match cmd.side {
            OrderSide::Buy  => Side::Buy,
            OrderSide::Sell => Side::Sell,
        };

        // map domain order type → apca type
        let order_type = match cmd.order_type {
            OrderType::Market    => Type::Market,
            OrderType::Limit     => Type::Limit,
            OrderType::Stop      => Type::Stop,
            OrderType::StopLimit => Type::StopLimit,
        };

        // map domain tif → apca tif
        let tif = match cmd.time_in_force {
            TimeInForce::Day => ApcaTimeInForce::Day,
            TimeInForce::Gtc => ApcaTimeInForce::UntilCanceled,
            TimeInForce::Ioc => ApcaTimeInForce::ImmediateOrCancel,
            TimeInForce::Fok => ApcaTimeInForce::FillOrKill,
        };

        // map optional prices — convert f64 → Num via string
        let limit_price = cmd.limit_price
            .and_then(|p| Num::from_str(&p.to_string()).ok());
        let stop_price = cmd.stop_price
            .and_then(|p| Num::from_str(&p.to_string()).ok());

        // build apca request — symbol, side, amount go into .init()
        let req = CreateReqInit {
            type_: order_type,
            time_in_force: tif,
            limit_price,
            stop_price,
            ..Default::default()
        }
        .init(&cmd.symbol, side, Amount::quantity(cmd.qty));

        // send to apca and return execution id
        let order = self.client
            .issue::<Create>(&req)
            .await
            .map_err(|e| BrokerError::Unknown(format!("Failed to create order: {}", e)))?;

        Ok(ExecutionId(order.id.to_string()))
    }

    async fn cancel_order(&self, cmd: CancelCmd) -> Result<(), Self::Error> {
        // Parse UUID string to create OrderId
        let uuid = Uuid::parse_str(&cmd.execution_id.0)
            .map_err(|_| BrokerError::InvalidOrderId(cmd.execution_id.0.clone()))?;
        
        let id = OrderId(uuid);

        self.client
            .issue::<Delete>(&id)
            .await
            .map_err(|e| BrokerError::Unknown(format!("Failed to delete order: {}", e)))?;

        Ok(())
    }

    async fn replace_order(&self, _cmd: ReplaceCmd) -> Result<(), Self::Error> {
        todo!()
    }

    async fn query_status(&self, _query: StatusQuery) -> Result<(), Self::Error> {
        todo!()
    }
}