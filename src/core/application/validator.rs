// validator.rs
//
// Validates order commands before submission to ensure data integrity
// and prevent invalid orders from reaching the broker.

use crate::adapters::broker::broker_error::BrokerError;
use crate::core::domain::order::{OrderCmd, CancelCmd, ReplaceCmd, StatusQuery, OrderType, OrderSide};

/// Validation result type
pub type ValidationResult = Result<(), BrokerError>;

pub struct RequestValidator;

impl RequestValidator {
    pub fn new() -> Self {
        Self
    }

    /// Validate an order submission command
    pub fn validate_order(&self, cmd: &OrderCmd) -> ValidationResult {
        // Symbol validation
        if cmd.symbol.is_empty() {
            return Err(BrokerError::Unknown("symbol cannot be empty".to_string()));
        }
        
        if cmd.symbol.len() > 20 {
            return Err(BrokerError::Unknown("symbol too long (max 20 chars)".to_string()));
        }
        
        // Symbol should only contain alphanumeric characters, dots, and hyphens
        if !cmd.symbol.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '-') {
            return Err(BrokerError::Unknown("symbol contains invalid characters".to_string()));
        }

        // Quantity validation
        if cmd.qty == 0 {
            return Err(BrokerError::Unknown("quantity must be greater than 0".to_string()));
        }
        
        if cmd.qty > 1_000_000 {
            return Err(BrokerError::Unknown("quantity exceeds maximum (1,000,000)".to_string()));
        }

        // Price validation for limit orders
        match cmd.order_type {
            OrderType::Limit | OrderType::StopLimit => {
                if cmd.limit_price.is_none() {
                    return Err(BrokerError::Unknown("limit price required for limit orders".to_string()));
                }
                if let Some(price) = cmd.limit_price {
                    if price <= 0.0 {
                        return Err(BrokerError::Unknown("limit price must be greater than 0".to_string()));
                    }
                    if !price.is_finite() {
                        return Err(BrokerError::Unknown("limit price must be a valid number".to_string()));
                    }
                }
            }
            _ => {}
        }

        // Stop price validation for stop orders
        match cmd.order_type {
            OrderType::Stop | OrderType::StopLimit => {
                if cmd.stop_price.is_none() {
                    return Err(BrokerError::Unknown("stop price required for stop orders".to_string()));
                }
                if let Some(price) = cmd.stop_price {
                    if price <= 0.0 {
                        return Err(BrokerError::Unknown("stop price must be greater than 0".to_string()));
                    }
                    if !price.is_finite() {
                        return Err(BrokerError::Unknown("stop price must be a valid number".to_string()));
                    }
                }
            }
            _ => {}
        }

        // Notional validation (if provided)
        if let Some(notional) = cmd.notional {
            if notional <= 0.0 {
                return Err(BrokerError::Unknown("notional must be greater than 0".to_string()));
            }
            if !notional.is_finite() {
                return Err(BrokerError::Unknown("notional must be a valid number".to_string()));
            }
        }

        // Validate that either qty or notional is provided (but not necessarily both)
        // Both being None would be invalid, but qty defaults to 0 which we already check

        Ok(())
    }

    /// Validate a cancel order command
    pub fn validate_cancel(&self, cmd: &CancelCmd) -> ValidationResult {
        if cmd.execution_id.0.is_empty() {
            return Err(BrokerError::Unknown("execution_id cannot be empty".to_string()));
        }
        
        if cmd.execution_id.0.len() > 100 {
            return Err(BrokerError::Unknown("execution_id too long (max 100 chars)".to_string()));
        }

        Ok(())
    }

    /// Validate a replace order command
    pub fn validate_replace(&self, cmd: &ReplaceCmd) -> ValidationResult {
        // Execution ID validation
        if cmd.execution_id.0.is_empty() {
            return Err(BrokerError::Unknown("execution_id cannot be empty".to_string()));
        }
        
        if cmd.execution_id.0.len() > 100 {
            return Err(BrokerError::Unknown("execution_id too long (max 100 chars)".to_string()));
        }

        // Symbol validation
        if cmd.symbol.is_empty() {
            return Err(BrokerError::Unknown("symbol cannot be empty".to_string()));
        }
        
        if cmd.symbol.len() > 20 {
            return Err(BrokerError::Unknown("symbol too long (max 20 chars)".to_string()));
        }

        // Quantity validation (if provided)
        if let Some(qty) = cmd.qty {
            if qty == 0 {
                return Err(BrokerError::Unknown("quantity must be greater than 0".to_string()));
            }
            if qty > 1_000_000 {
                return Err(BrokerError::Unknown("quantity exceeds maximum (1,000,000)".to_string()));
            }
        }

        // Limit price validation (if provided)
        if let Some(price) = cmd.limit_price {
            if price <= 0.0 {
                return Err(BrokerError::Unknown("limit price must be greater than 0".to_string()));
            }
            if !price.is_finite() {
                return Err(BrokerError::Unknown("limit price must be a valid number".to_string()));
            }
        }

        Ok(())
    }

    /// Validate a status query command
    pub fn validate_query(&self, query: &StatusQuery) -> ValidationResult {
        if query.execution_id.0.is_empty() {
            return Err(BrokerError::Unknown("execution_id cannot be empty".to_string()));
        }
        
        if query.execution_id.0.len() > 100 {
            return Err(BrokerError::Unknown("execution_id too long (max 100 chars)".to_string()));
        }

        Ok(())
    }
}

impl Default for RequestValidator {
    fn default() -> Self {
        Self::new()
    }
}


