// tests/adapter_factory_integration.rs
//
// Integration tests for AdapterFactory reuse behavior
// Tests both the new safe API and the deprecated panic API

use broker_gateway_service::adapters::broker::adapter_factory::{AdapterFactory, AdapterFactoryError};
use broker_gateway_service::core::domain::broker_config::BrokerConfig;

#[test]
fn test_factory_reusable_for_mock_adapters() {
    let factory = AdapterFactory::new(None);
    
    // Factory should be reusable for mock adapters
    for i in 0..10 {
        let config = BrokerConfig {
            name: "mock".to_string(),
        };
        
        let adapter = factory.create_adapter(config);
        assert!(adapter.is_ok(), "Factory should be reusable, failed on iteration {}", i);
    }
}

#[test]
fn test_factory_returns_not_implemented_for_fix_adapters() {
    let factory = AdapterFactory::new(None);
    
    // FIX adapter is not implemented - factory should return error
    for i in 0..5 {
        let config = BrokerConfig {
            name: "fix".to_string(),
        };
        
        let result = factory.create_adapter(config);
        assert!(result.is_err(), "Factory should return error for FIX on iteration {}", i);
        match result {
            Err(AdapterFactoryError::AdapterNotImplemented { broker }) => {
                assert_eq!(broker, "fix");
            }
            _ => panic!("Expected AdapterNotImplemented error for FIX"),
        }
    }
}

#[test]
fn test_alpaca_client_not_available_returns_error() {
    let factory = AdapterFactory::new(None);
    
    assert!(!factory.is_alpaca_client_available());
    
    let config = BrokerConfig {
        name: "alpaca".to_string(),
    };
    
    let result = factory.create_adapter(config);
    assert!(result.is_err());
    
    match result {
        Err(AdapterFactoryError::AlpacaClientAlreadyUsed) => {
            // This is the expected error - client wasn't provided
        }
        _ => panic!("Expected AlpacaClientAlreadyUsed error when client not provided"),
    }
}

#[test]
fn test_error_message_clearly_indicates_problem() {
    let already_used_error = AdapterFactoryError::AlpacaClientAlreadyUsed;
    let error_msg = already_used_error.to_string();
    
    assert!(
        error_msg.contains("already") || error_msg.contains("consumed") || error_msg.contains("used"),
        "Error message should indicate client was already used: got '{}'",
        error_msg
    );
}

#[test]
fn test_factory_can_create_different_adapter_types() {
    let factory = AdapterFactory::new(None);
    
    // Test mock adapter (should succeed)
    let mock_config = BrokerConfig { name: "mock".to_string() };
    assert!(factory.create_adapter(mock_config).is_ok(), "Failed to create mock adapter");
    
    // Test unknown adapter (falls back to mock, should succeed)
    let unknown_config = BrokerConfig { name: "unknown".to_string() };
    assert!(factory.create_adapter(unknown_config).is_ok(), "Failed to create unknown adapter");
    
    // Test unimplemented adapters (should return NotImplemented error)
    let unimplemented_configs = vec![
        BrokerConfig { name: "fix".to_string() },
        BrokerConfig { name: "rest".to_string() },
        BrokerConfig { name: "websocket".to_string() },
    ];
    
    for config in unimplemented_configs {
        let result = factory.create_adapter(config.clone());
        assert!(result.is_err(), "Should fail to create adapter for {}", config.name);
        match result {
            Err(AdapterFactoryError::AdapterNotImplemented { broker }) => {
                assert_eq!(broker, config.name);
            }
            _ => panic!("Expected AdapterNotImplemented error for {}", config.name),
        }
    }
}

#[test]
fn test_alpaca_client_availability_check() {
    // Without client
    let factory_no_client = AdapterFactory::new(None);
    assert!(!factory_no_client.is_alpaca_client_available());
    
    // Note: We can't easily test with a real client in integration tests
    // since creating an apca::Client requires API credentials
}

#[test]
fn test_error_type_equality() {
    let error1 = AdapterFactoryError::AlpacaClientAlreadyUsed;
    let error2 = AdapterFactoryError::AlpacaClientAlreadyUsed;
    let error3 = AdapterFactoryError::AlpacaClientNotProvided;
    
    assert_eq!(error1, error2);
    assert_ne!(error1, error3);
}

#[test]
fn test_error_implements_standard_traits() {
    let error = AdapterFactoryError::AlpacaClientAlreadyUsed;
    
    // Should implement Debug
    let _ = format!("{:?}", error);
    
    // Should implement Display
    let _ = format!("{}", error);
    
    // Should implement Clone
    let cloned = error.clone();
    assert_eq!(error, cloned);
    
    // Should implement PartialEq
    assert!(error == AdapterFactoryError::AlpacaClientAlreadyUsed);
}

#[test]
fn test_factory_is_thread_safe() {
    use std::sync::Arc;
    use std::thread;
    
    let factory = Arc::new(AdapterFactory::new(None));
    let mut handles = vec![];
    
    // Spawn multiple threads trying to create adapters
    for i in 0..5 {
        let factory_clone = Arc::clone(&factory);
        let handle = thread::spawn(move || {
            let config = BrokerConfig {
                name: "mock".to_string(),
            };
            let result = factory_clone.create_adapter(config);
            assert!(result.is_ok(), "Thread {} failed", i);
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_mixed_adapter_creation() {
    let factory = AdapterFactory::new(None);
    
    // Create mock adapters
    for _ in 0..3 {
        let config = BrokerConfig { name: "mock".to_string() };
        assert!(factory.create_adapter(config).is_ok());
    }
    
    // Try to create alpaca (should fail without client)
    let alpaca_config = BrokerConfig { name: "alpaca".to_string() };
    assert!(factory.create_adapter(alpaca_config).is_err());
    
    // Create more mock adapters (should still work)
    for _ in 0..3 {
        let config = BrokerConfig { name: "mock".to_string() };
        assert!(factory.create_adapter(config).is_ok());
    }
}
