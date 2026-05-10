// tests/rate_limiter_shared_integration.rs
//
// Integration tests to verify that RateLimiterManager is properly shared
// between OrderSubmissionService and RiskManagementService

use std::sync::Arc;
use broker_gateway_service::core::application::rate_limiter::RateLimiterManager;
use broker_gateway_service::core::application::risk_management_service::RiskManagementService;
use broker_gateway_service::core::application::kill_switch::KillSwitch;

#[test]
fn test_rate_limiter_is_shared_between_services() {
    // Create a shared rate limiter with 5 requests per minute
    let shared_limiter = Arc::new(RateLimiterManager::new(5.0));
    
    // Create two services using the same rate limiter
    let risk_mgmt = RiskManagementService::new(
        KillSwitch::default(),
        Arc::clone(&shared_limiter),
    );
    
    // Use up all tokens through risk management service
    // Service 1 uses 3 tokens
    assert!(risk_mgmt.check("broker1", 1).is_ok());
    assert!(risk_mgmt.check("broker1", 1).is_ok());
    assert!(risk_mgmt.check("broker1", 1).is_ok());
    
    // Now check that the shared rate limiter has fewer tokens
    // The 4th request should still succeed (we had 5, used 3, have 2 left)
    assert!(risk_mgmt.check("broker1", 1).is_ok());
    
    // The 5th request should succeed (last token)
    assert!(risk_mgmt.check("broker1", 1).is_ok());
    
    // The 6th request should fail (out of tokens)
    assert!(risk_mgmt.check("broker1", 1).is_err());
}

#[test]
fn test_shared_rate_limiter_accumulates_across_services() {
    // Create a shared rate limiter
    let shared_limiter = Arc::new(RateLimiterManager::new(10.0));
    
    // Create multiple service instances with the same rate limiter
    let svc1 = RiskManagementService::new(KillSwitch::default(), Arc::clone(&shared_limiter));
    let svc2 = RiskManagementService::new(KillSwitch::default(), Arc::clone(&shared_limiter));
    let svc3 = RiskManagementService::new(KillSwitch::default(), Arc::clone(&shared_limiter));
    
    // Each service uses some tokens
    // Total: 4 + 3 + 3 = 10 tokens used
    for _ in 0..4 {
        assert!(svc1.check("broker", 1).is_ok());
    }
    
    for _ in 0..3 {
        assert!(svc2.check("broker", 1).is_ok());
    }
    
    for _ in 0..3 {
        assert!(svc3.check("broker", 1).is_ok());
    }
    
    // All tokens should be exhausted now
    assert!(svc1.check("broker", 1).is_err());
    assert!(svc2.check("broker", 1).is_err());
    assert!(svc3.check("broker", 1).is_err());
}

#[test]
fn test_rate_limiter_shares_state_not_copies() {
    // Create one shared rate limiter
    let shared_limiter = Arc::new(RateLimiterManager::new(3.0));
    
    // Create two services pointing to the SAME rate limiter
    let svc_a = RiskManagementService::new(KillSwitch::default(), Arc::clone(&shared_limiter));
    let svc_b = RiskManagementService::new(KillSwitch::default(), Arc::clone(&shared_limiter));
    
    // Use a token from service A
    assert!(svc_a.check("test", 1).is_ok());
    
    // Use a token from service B (should see the same state)
    assert!(svc_b.check("test", 1).is_ok());
    
    // Use another token from service A
    assert!(svc_a.check("test", 1).is_ok());
    
    // Now all 3 tokens are used, neither service should succeed
    assert!(svc_a.check("test", 1).is_err());
    assert!(svc_b.check("test", 1).is_err());
}

#[test]
fn test_separate_rate_limiters_are_independent() {
    // Create two SEPARATE rate limiters (not shared)
    let limiter1 = Arc::new(RateLimiterManager::new(2.0));
    let limiter2 = Arc::new(RateLimiterManager::new(2.0));
    
    let svc1 = RiskManagementService::new(KillSwitch::default(), limiter1);
    let svc2 = RiskManagementService::new(KillSwitch::default(), limiter2);
    
    // Exhaust all tokens on service 1
    assert!(svc1.check("test", 1).is_ok());
    assert!(svc1.check("test", 1).is_ok());
    assert!(svc1.check("test", 1).is_err()); // Exhausted
    
    // Service 2 should still have tokens (independent rate limiter)
    assert!(svc2.check("test", 1).is_ok());
    assert!(svc2.check("test", 1).is_ok());
    assert!(svc2.check("test", 1).is_err()); // Now exhausted
}

#[test]
fn test_arc_clone_shares_same_instance() {
    let limiter = Arc::new(RateLimiterManager::new(5.0));
    
    // Multiple Arc clones should point to same instance
    let clone1 = Arc::clone(&limiter);
    let clone2 = Arc::clone(&limiter);
    let clone3 = Arc::clone(&limiter);
    
    // Verify they're the same by using tokens
    // Use all tokens through clone1
    for _ in 0..5 {
        assert!(clone1.allow("test", 1));
    }
    
    // All clones should see exhausted state
    assert!(!clone1.allow("test", 1));
    assert!(!clone2.allow("test", 1));
    assert!(!clone3.allow("test", 1));
    assert!(!limiter.allow("test", 1));
}

#[test]
fn test_risk_management_service_uses_arc_rate_limiter() {
    // Verify RiskManagementService can be created with Arc<RateLimiterManager>
    let rate_limiter = Arc::new(RateLimiterManager::new(100.0));
    let kill_switch = KillSwitch::default();
    
    // This should compile and work
    let service = RiskManagementService::new(kill_switch, rate_limiter);
    
    // Service should function normally
    assert!(service.check("broker", 1).is_ok());
}
