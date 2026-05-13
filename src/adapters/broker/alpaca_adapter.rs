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
    OrderStatusResponse,
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

    async fn query_status(&self, query: StatusQuery) -> Result<OrderStatusResponse, Self::Error> {
        let id = Self::parse_order_id(&query.execution_id.0)?;

        let order = self.client
            .issue::<Get>(&id)
            .await
            .map_err(|e| BrokerError::Unknown(format!("query_status failed: {}", e)))?;

        // Map Alpaca status to our domain status
        let status = format!("{:?}", order.status).to_lowercase();
        
        // Extract side
        let side = match order.side {
            Side::Buy => OrderSide::Buy,
            Side::Sell => OrderSide::Sell,
        };

        // Extract filled quantity - Num has to_u64 which returns Option<u64>
        let filled_qty: u32 = order.filled_quantity.to_u64().unwrap_or(0) as u32;
        
        // Extract total quantity from the order amount
        // Amount can be either Quantity or Notional, we handle both cases
        let total_qty: u32 = match &order.amount {
            Amount::Quantity { quantity } => {
                quantity.to_u64().unwrap_or(0) as u32
            }
            Amount::Notional { notional } => {
                // For notional orders, we can't determine exact quantity without price
                // Return 0 as placeholder - in production, you may want to track original order qty separately
                tracing::warn!("Notional order detected (notional={}), using 0 as total qty", notional);
                0
            }
        };
        
        // Extract remaining quantity
        let remaining_qty: u32 = total_qty.saturating_sub(filled_qty);

        // Extract average fill price if available
        let avg_fill_price = order.average_fill_price.as_ref()
            .and_then(|p| p.to_f64());

        let response = OrderStatusResponse::new(
            query.execution_id.0.clone(),
            status.clone(),
            order.symbol.clone(),
            side,
            total_qty,
        )
        .with_fill(filled_qty, avg_fill_price.unwrap_or(0.0))
        .with_raw_status(status.clone());

        // Update remaining_qty
        let mut response = response;
        response.remaining_qty = remaining_qty;

        tracing::info!(
            "order status id={} status={:?} filled={}/{} avg_price={:?}",
            query.execution_id.0,
            order.status,
            filled_qty,
            total_qty,
            avg_fill_price
        );

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that quantity extraction from Amount enum works correctly
    #[test]
    fn test_quantity_extraction_from_amount() {
        // Test Quantity variant
        let qty_amount = Amount::quantity(500u32);
        let extracted_qty: u32 = match &qty_amount {
            Amount::Quantity { quantity } => {
                quantity.to_u64().unwrap_or(0) as u32
            }
            Amount::Notional { .. } => 0,
        };
        assert_eq!(extracted_qty, 500);

        // Test Notional variant
        let notional_amount = Amount::notional(Num::from(1000));
        let extracted_notional: u32 = match &notional_amount {
            Amount::Quantity { quantity } => {
                quantity.to_u64().unwrap_or(0) as u32
            }
            Amount::Notional { .. } => 0,
        };
        assert_eq!(extracted_notional, 0); // Returns 0 for notional orders
    }

    /// Test that remaining quantity calculation works correctly
    #[test]
    fn test_remaining_qty_calculation() {
        let total_qty: u32 = 1000;
        let filled_qty: u32 = 350;
        let remaining_qty: u32 = total_qty.saturating_sub(filled_qty);
        assert_eq!(remaining_qty, 650);

        // Test edge case where filled > total
        let total_qty: u32 = 100;
        let filled_qty: u32 = 150;
        let remaining_qty: u32 = total_qty.saturating_sub(filled_qty);
        assert_eq!(remaining_qty, 0); // saturating_sub should return 0, not underflow
    }

    /// Test quantity extraction with fractional quantities
    #[test]
    fn test_fractional_quantity_extraction() {
        // Alpaca supports fractional shares, so quantity could be like 10.5
        let fractional_qty = Num::from_str("10.5").unwrap();
        let amount = Amount::Quantity { quantity: fractional_qty };
        
        let extracted: u32 = match &amount {
            Amount::Quantity { quantity } => {
                quantity.to_u64().unwrap_or(0) as u32
            }
            Amount::Notional { .. } => 0,
        };
        
        // Fractional part should be truncated when converting to u64
        assert_eq!(extracted, 10);
    }
}
