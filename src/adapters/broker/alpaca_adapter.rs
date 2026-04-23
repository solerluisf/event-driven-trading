// src/adapters/broker/alpaca_adapter.rs
use super::broker_adapter::BrokerAdapterId;
use apca::client::{Client, Config};
use apca::data::v2::order::{Amount, CreateReqInit, Side, TimeInForce, Type};
use apca::rest::order::Create;
use core::ports::execution_port::{IExecutionPort, OrderSide, OrderType, TimeInForce, ExecutionId};

#[derive(Clone)]
pub struct AlpacaBrokerAdapter {
    adapter_id: BrokerAdapterId,
    apca_client: Client,
}

impl AlpacaBrokerAdapter {
    pub fn new(config: Config, adapter_id: BrokerAdapterId) -> Result<Self, apca::Error> {
        Ok(Self {
            adapter_id,
            apca_client: Client::new(config)?,
        })
    }
}

// Convert your domain types into `apca`‑style enums/structs inline
#[async_trait::async_trait]
impl IExecutionPort for AlpacaBrokerAdapter {
    type Error = apca::Error;  // Or wrap into your own error type

    async fn execute_order(
        &self,
        symbol: String,
        qty: u32,
        side: OrderSide,
        order_type: OrderType,
        tif: TimeInForce,
    ) -> Result<ExecutionId, Self::Error> {
        let (apca_side, apca_type) = match (side, order_type) {
            (OrderSide::Buy, OrderType::Market) => (Side::Buy, Type::Market),
            (OrderSide::Sell, OrderType::Market) => (Side::Sell, Type::Market),
            (OrderSide::Buy, OrderType::Limit) => (Side::Buy, Type::Limit),
            (OrderSide::Sell, OrderType::Limit) => (Side::Sell, Type::Limit),
            _ => unimplemented!(),
        };

        let tif = match tif {
            TimeInForce::Day => TimeInForce::Day,
            TimeInForce::Gtc => TimeInForce::Gtc,
            _ => TimeInForce::Day,
        };

        let req = CreateReqInit {
            account: None,
            symbol,
            qty: Some(qty),
            notional: None,
            side: apca_side,
            r#type: apca_type,
            time_in_force: tif,
            limit_price: None,  // fill this if your `OrderType::Limit` has a price
            stop_price: None,
            stop_loss: None,
            take_profit: None,
            client_order_id: None,
            extended_hours: false,
        };

        let order = self
            .apca_client
            .issue::<Create>(&req.init())
            .await?;

        Ok(order.id.into()) // map `apca::order::Id` → `ExecutionId`
    }
}