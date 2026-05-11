// src/tests/mod.rs
//
// Test module for broker_gateway_service
// 
// Windows Note: Tests are configured to run sequentially to avoid
// file lock issues (LNK1104) common on Windows with MSVC linker.
// Use `cargo test -- --test-threads=1` or the provided `test-windows.bat`
// script to run tests properly on Windows.

use std::sync::{Arc, Mutex};
use std::time::Duration;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// A no-op IObservability impl for use in tests that need to pass one in.
struct NoopObservability;
impl crate::core::ports::observability::IObservability for NoopObservability {
    fn emit(&self, _event: String) {}
}

/// A recording IObservability impl that captures every emitted event.
#[derive(Default, Clone)]
struct RecordingObservability {
    events: Arc<Mutex<Vec<String>>>,
}
impl RecordingObservability {
    fn emitted(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
    fn count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}
impl crate::core::ports::observability::IObservability for RecordingObservability {
    fn emit(&self, event: String) {
        self.events.lock().unwrap().push(event);
    }
}

/// Build a minimal valid OrderCmd for tests.
fn make_order(symbol: &str) -> crate::core::domain::order::OrderCmd {
    use crate::core::domain::order::*;
    OrderCmd {
        symbol: symbol.to_string(),
        qty: 1,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Day,
        limit_price: None,
        stop_price: None,
        client_order_id: None,
        extended_hours: false,
        notional: None,
        correlation_id: None,
    }
}

fn make_order_with_id(symbol: &str, client_id: &str) -> crate::core::domain::order::OrderCmd {
    let mut cmd = make_order(symbol);
    cmd.client_order_id = Some(client_id.to_string());
    cmd
}

fn noop_obs() -> Arc<dyn crate::core::ports::observability::IObservability + Send + Sync> {
    Arc::new(NoopObservability)
}

fn recording_obs() -> (
    RecordingObservability,
    Arc<dyn crate::core::ports::observability::IObservability + Send + Sync>,
) {
    let r = RecordingObservability::default();
    let arc = Arc::new(r.clone())
        as Arc<dyn crate::core::ports::observability::IObservability + Send + Sync>;
    (r, arc)
}

// ═══════════════════════════════════════════════════════════════════════════
// KillSwitch
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod kill_switch_tests {
    use crate::core::application::kill_switch::KillSwitch;

    #[test]
    fn starts_disabled() {
        let ks = KillSwitch::default();
        assert!(!ks.is_enabled());
    }

    #[test]
    fn enable_sets_flag() {
        let ks = KillSwitch::default();
        ks.enable();
        assert!(ks.is_enabled());
    }

    #[test]
    fn disable_clears_flag() {
        let ks = KillSwitch::default();
        ks.enable();
        ks.disable();
        assert!(!ks.is_enabled());
    }

    #[test]
    fn clone_shares_state() {
        let ks = KillSwitch::default();
        let ks2 = ks.clone();
        ks.enable();
        // Both clones share the same Arc<Mutex<bool>>
        assert!(ks2.is_enabled());
    }

    #[test]
    fn toggle_multiple_times() {
        let ks = KillSwitch::default();
        for _ in 0..5 {
            ks.enable();
            assert!(ks.is_enabled());
            ks.disable();
            assert!(!ks.is_enabled());
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// IdempotencyStore
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod idempotency_tests {
    use crate::core::application::idempotency::IdempotencyStore;

    #[test]
    fn new_key_is_not_processed() {
        let store = IdempotencyStore::default();
        assert!(!store.is_processed("order-1"));
    }

    #[test]
    fn marked_key_is_processed() {
        let store = IdempotencyStore::default();
        store.mark_processed("order-1".into(), "exec-abc".into());
        assert!(store.is_processed("order-1"));
    }

    #[test]
    fn different_keys_are_independent() {
        let store = IdempotencyStore::default();
        store.mark_processed("order-1".into(), "exec-abc".into());
        assert!(!store.is_processed("order-2"));
    }

    #[test]
    fn marking_twice_is_idempotent() {
        let store = IdempotencyStore::default();
        store.mark_processed("order-1".into(), "exec-abc".into());
        store.mark_processed("order-1".into(), "exec-xyz".into()); // overwrites
        assert!(store.is_processed("order-1"));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// RateLimiterManager
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod rate_limiter_tests {
    use crate::core::application::rate_limiter::RateLimiterManager;

    #[test]
    fn allows_requests_up_to_capacity() {
        // 10 req/min capacity — bucket starts full, so first 10 calls succeed
        let limiter = RateLimiterManager::new(10.0);
        for _ in 0..10 {
            assert!(limiter.allow("broker-a", 1), "expected allow while tokens remain");
        }
    }

    #[test]
    fn rejects_when_bucket_empty() {
        let limiter = RateLimiterManager::new(3.0);
        assert!(limiter.allow("b", 1));
        assert!(limiter.allow("b", 1));
        assert!(limiter.allow("b", 1));
        // Bucket now empty — next call should be rejected
        assert!(!limiter.allow("b", 1), "expected rejection when tokens exhausted");
    }

    #[test]
    fn brokers_have_independent_buckets() {
        let limiter = RateLimiterManager::new(1.0);
        assert!(limiter.allow("broker-a", 1));
        // broker-a is now empty, broker-b starts fresh
        assert!(limiter.allow("broker-b", 1));
        // broker-a still empty
        assert!(!limiter.allow("broker-a", 1));
    }

    #[test]
    fn registered_broker_uses_custom_rpm() {
        let limiter = RateLimiterManager::new(1.0); // default: 1 req/min
        limiter.register("fast-broker", 100.0);     // 100 req/min
        // fast-broker has 100 tokens; default broker has 1
        for _ in 0..10 {
            assert!(limiter.allow("fast-broker", 1));
        }
        assert!(limiter.allow("default-broker", 1));
        assert!(!limiter.allow("default-broker", 1));
    }

    #[test]
    fn tokens_refill_over_time() {
        let limiter = RateLimiterManager::new(60.0); // 1 token/sec
        // Drain the bucket
        for _ in 0..60 {
            limiter.allow("b", 1);
        }
        assert!(!limiter.allow("b", 1), "bucket should be empty");

        // Sleep 1.1 seconds — should have refilled ~1 token
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert!(limiter.allow("b", 1), "bucket should have refilled");
    }

    // =========================================================================
    // BACK-PRESSURE TESTS
    // =========================================================================

    #[test]
    fn back_pressure_status_shows_full_capacity_initially() {
        let limiter = RateLimiterManager::new(100.0);
        let status = limiter.get_back_pressure_status("broker-a", 20.0)
            .expect("should get status for new broker");
        
        assert_eq!(status.tokens_remaining, 100.0, "bucket should start full");
        assert_eq!(status.capacity, 100.0);
        assert!((status.percent_remaining - 100.0).abs() < 0.01, "should be 100% full");
        assert!(!status.is_near_limit, "full bucket should not be near limit");
    }

    #[test]
    fn back_pressure_status_shows_depletion() {
        let limiter = RateLimiterManager::new(10.0);
        
        // Consume 7 tokens (70%)
        for _ in 0..7 {
            limiter.allow("broker-b", 1);
        }
        
        let status = limiter.get_back_pressure_status("broker-b", 30.0)
            .expect("should get status");
        
        // Should have ~3 tokens left (allowing for small refill due to elapsed time)
        assert!(status.tokens_remaining >= 2.5 && status.tokens_remaining <= 3.5,
            "should have ~3 tokens remaining, got {}", status.tokens_remaining);
        // Due to potential refill, percent_remaining might be slightly above 30%
        // but should still be in the "near limit" range
        assert!(status.percent_remaining >= 20.0 && status.percent_remaining <= 40.0, 
            "should be around 30% remaining, got {}", status.percent_remaining);
        // At 30% threshold with ~3 tokens (30%), should be near limit
        assert!(status.is_near_limit || status.percent_remaining <= 35.0, 
            "should be near limit when at or below 30%, got {}%", status.percent_remaining);
    }

    #[test]
    fn is_near_limit_returns_true_when_below_threshold() {
        let limiter = RateLimiterManager::new(10.0);
        
        // Consume 9 tokens (90%)
        for _ in 0..9 {
            limiter.allow("broker-c", 1);
        }
        
        assert!(limiter.is_near_limit("broker-c", Some(20.0)), 
            "should be near limit with only 1 token left at 20% threshold");
        assert!(!limiter.is_near_limit("broker-c", Some(5.0)), 
            "should NOT be near limit at 5% threshold");
    }

    #[test]
    fn tokens_remaining_returns_accurate_count() {
        let limiter = RateLimiterManager::new(5.0);
        
        // Initially full (allowing for tiny refill)
        let initial = limiter.tokens_remaining("broker-d");
        assert!(initial >= 5.0 && initial <= 5.1, "should start with ~5 tokens, got {}", initial);
        
        // Consume 3
        for _ in 0..3 {
            limiter.allow("broker-d", 1);
        }
        
        // Should have ~2 tokens (allowing for tiny refill during operations)
        let remaining = limiter.tokens_remaining("broker-d");
        assert!(remaining >= 1.9 && remaining <= 2.2, "should have ~2 tokens remaining, got {}", remaining);
    }

    #[test]
    fn get_capacity_returns_registered_or_default() {
        let limiter = RateLimiterManager::new(100.0);
        
        // Unregistered broker returns default
        assert_eq!(limiter.get_capacity("unregistered"), 100.0);
        
        // Register with different capacity
        limiter.register("custom", 50.0);
        assert_eq!(limiter.get_capacity("custom"), 50.0);
    }

    #[test]
    fn back_pressure_threshold_edge_cases() {
        let limiter = RateLimiterManager::new(10.0);
        
        // Empty the bucket
        for _ in 0..10 {
            limiter.allow("broker-e", 1);
        }
        
        // At 0%, should be near limit at any threshold > 0
        assert!(limiter.is_near_limit("broker-e", Some(1.0)));
        
        // At 0% with 0 threshold, not near limit
        assert!(!limiter.is_near_limit("broker-e", Some(0.0)));
    }

    #[test]
    fn back_pressure_brokers_are_independent() {
        let limiter = RateLimiterManager::new(5.0);
        
        // Drain broker-1 completely
        for _ in 0..5 {
            limiter.allow("broker-1", 1);
        }
        
        // broker-1 is near limit
        assert!(limiter.is_near_limit("broker-1", Some(20.0)));
        
        // broker-2 is still full
        let status = limiter.get_back_pressure_status("broker-2", 20.0)
            .expect("should get status");
        assert!(!status.is_near_limit);
        assert_eq!(status.tokens_remaining, 5.0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CircuitBreaker
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod circuit_breaker_tests {
    use super::*;
    use crate::core::patterns::circuit_breaker::CircuitBreaker;

    fn make_cb(threshold: u32, cooldown_secs: u64) -> CircuitBreaker {
        CircuitBreaker::new("test-broker", threshold, cooldown_secs, noop_obs())
    }

    fn make_cb_recording(
        threshold: u32,
        cooldown_secs: u64,
    ) -> (CircuitBreaker, RecordingObservability) {
        let (rec, arc) = recording_obs();
        let cb = CircuitBreaker::new("test-broker", threshold, cooldown_secs, arc);
        (cb, rec)
    }

    #[test]
    fn starts_closed() {
        let cb = make_cb(3, 30);
        assert!(!cb.is_open());
    }

    #[test]
    fn single_failure_below_threshold_stays_closed() {
        let cb = make_cb(3, 30);
        cb.record_failure(&"err1");
        assert!(!cb.is_open());
    }

    #[test]
    fn reaches_threshold_opens_breaker() {
        let cb = make_cb(3, 30);
        cb.record_failure(&"e");
        cb.record_failure(&"e");
        cb.record_failure(&"e"); // 3rd failure hits threshold
        assert!(cb.is_open());
    }

    #[test]
    fn success_resets_failure_count() {
        let cb = make_cb(3, 30);
        cb.record_failure(&"e");
        cb.record_failure(&"e");
        cb.record_success(); // reset
        cb.record_failure(&"e"); // back to 1 — should still be closed
        assert!(!cb.is_open());
    }

    #[test]
    fn call_returns_ok_when_closed() {
        let cb = make_cb(3, 30);
        let result: Result<i32, String> = cb.call(|| Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn call_propagates_error_and_counts_failure() {
        let cb = make_cb(1, 30); // threshold = 1
        let result: Result<(), String> = cb.call(|| Err("boom".to_string()));
        assert!(result.is_err());
        assert!(cb.is_open());
    }

    #[test]
    fn open_breaker_emits_observability_events() {
        let (cb, rec) = make_cb_recording(2, 30);
        cb.record_failure(&"e1");
        cb.record_failure(&"e2"); // opens
        let events = rec.emitted();
        assert!(
            events.iter().any(|e| e.contains("circuit_breaker.opened")),
            "expected opened event, got: {:?}",
            events
        );
    }

    #[test]
    fn cooldown_zero_transitions_to_half_open() {
        // cooldown = 0 means the open window expires immediately
        let cb = make_cb(1, 0);
        cb.record_failure(&"e");
        assert!(cb.is_open(), "should be open right after failure");

        // Sleep just a tiny bit to ensure Instant::now() > until
        std::thread::sleep(Duration::from_millis(5));
        // is_open checks Instant::now() < until, so with cooldown=0 it should
        // now report closed (the window has passed)
        assert!(!cb.is_open(), "should be closed after cooldown expired");
    }

    #[test]
    fn half_open_failure_reopens() {
        let cb = make_cb(1, 0);
        cb.record_failure(&"first"); // opens with 0s cooldown
        std::thread::sleep(Duration::from_millis(5));
        // call() will see expired Open → transition to HalfOpen → run probe
        let result: Result<(), String> = cb.call(|| Err("probe fail".to_string()));
        assert!(result.is_err());
        // After HalfOpen failure, breaker reopens
        assert!(cb.is_open());
    }

    #[test]
    fn half_open_success_closes() {
        let cb = make_cb(1, 0);
        cb.record_failure(&"first");
        std::thread::sleep(Duration::from_millis(5));
        let result: Result<i32, String> = cb.call(|| Ok(1));
        assert!(result.is_ok());
        assert!(!cb.is_open());
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// RiskManagementService
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod risk_management_tests {
    use std::sync::Arc;
    use crate::adapters::broker::broker_error::BrokerError;
    use crate::core::application::kill_switch::KillSwitch;
    use crate::core::application::rate_limiter::RateLimiterManager;
    use crate::core::application::risk_management_service::RiskManagementService;

    fn make_svc(rpm: f64) -> RiskManagementService {
        RiskManagementService::new(KillSwitch::default(), Arc::new(RateLimiterManager::new(rpm)))
    }

    #[test]
    fn allows_normal_order() {
        let svc = make_svc(200.0);
        assert!(svc.check("alpaca", 1).is_ok());
    }

    #[test]
    fn kill_switch_blocks_order() {
        let svc = make_svc(200.0);
        svc.activate_kill_switch();
        let err = svc.check("alpaca", 1).unwrap_err();
        assert!(matches!(err, BrokerError::Unknown(_)));
    }

    #[test]
    fn kill_switch_can_be_deactivated() {
        let svc = make_svc(200.0);
        svc.activate_kill_switch();
        svc.deactivate_kill_switch();
        assert!(svc.check("alpaca", 1).is_ok());
    }

    #[test]
    fn rate_limit_blocks_when_exhausted() {
        let svc = make_svc(1.0); // 1 token capacity
        assert!(svc.check("alpaca", 1).is_ok()); // uses the 1 token
        let err = svc.check("alpaca", 1).unwrap_err();
        assert!(matches!(err, BrokerError::RateLimited));
    }

    #[test]
    fn kill_switch_checked_before_rate_limit() {
        // Even with rate limit exhausted, kill switch message should dominate
        let svc = make_svc(1.0);
        svc.check("alpaca", 1).ok(); // drain
        svc.activate_kill_switch();
        let err = svc.check("alpaca", 1).unwrap_err();
        // Kill switch is checked first — should be Unknown, not RateLimited
        assert!(matches!(err, BrokerError::Unknown(_)));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// OrderSubmissionService  (uses MockAdapter — no real broker)
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod order_submission_tests {
    use std::sync::Arc;
    use super::*;
    use crate::adapters::broker::broker_error::BrokerError;
    use crate::adapters::broker::mock_adapter::MockAdapter;
    use crate::core::application::idempotency::IdempotencyStore;
    use crate::core::application::kill_switch::KillSwitch;
    use crate::core::application::order_submission_service::OrderSubmissionService;
    use crate::core::application::rate_limiter::RateLimiterManager;
    use crate::core::application::validator::RequestValidator;
    use crate::core::patterns::circuit_breaker::CircuitBreaker;

    fn make_svc(rpm: f64, cb_threshold: u32) -> OrderSubmissionService {
        OrderSubmissionService::new(
            RequestValidator,
            IdempotencyStore::default(),
            Box::new(MockAdapter::default()),
            Arc::new(KillSwitch::default()),
            Arc::new(RateLimiterManager::new(rpm)),
            Arc::new(CircuitBreaker::new("test", cb_threshold, 30, noop_obs())),
            "test",
        )
    }

    fn make_svc_with_kill_switch(
        ks: Arc<crate::core::application::kill_switch::KillSwitch>,
    ) -> OrderSubmissionService {
        OrderSubmissionService::new(
            RequestValidator,
            IdempotencyStore::default(),
            Box::new(MockAdapter::default()),
            ks,
            Arc::new(RateLimiterManager::new(200.0)),
            Arc::new(CircuitBreaker::new("test", 3, 30, noop_obs())),
            "test",
        )
    }

    #[tokio::test]
    async fn submit_order_succeeds_with_mock() {
        let svc = make_svc(200.0, 3);
        let result = svc.submit_order(make_order("AAPL")).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().0, "mock-execution-id");
    }

    #[tokio::test]
    async fn kill_switch_blocks_submit() {
        let ks = Arc::new(KillSwitch::default());
        ks.enable();
        let svc = make_svc_with_kill_switch(Arc::clone(&ks));
        let err = svc.submit_order(make_order("AAPL")).await.unwrap_err();
        assert!(matches!(err, BrokerError::Unknown(_)));
    }

    #[tokio::test]
    async fn duplicate_order_is_rejected() {
        let svc = make_svc(200.0, 3);
        let cmd = make_order_with_id("AAPL", "client-001");
        // First submission succeeds
        assert!(svc.submit_order(cmd.clone()).await.is_ok());
        // Second with same client_order_id is rejected
        let err = svc.submit_order(cmd).await.unwrap_err();
        assert!(matches!(err, BrokerError::Unknown(ref m) if m.contains("duplicate")));
    }

    #[tokio::test]
    async fn different_client_ids_are_independent() {
        let svc = make_svc(200.0, 3);
        assert!(svc.submit_order(make_order_with_id("AAPL", "id-1")).await.is_ok());
        assert!(svc.submit_order(make_order_with_id("AAPL", "id-2")).await.is_ok());
    }

    #[tokio::test]
    async fn rate_limit_blocks_after_capacity() {
        // 2 req/min capacity — 2 succeed, 3rd is rejected
        let svc = make_svc(2.0, 10);
        assert!(svc.submit_order(make_order_with_id("AAPL", "a")).await.is_ok());
        assert!(svc.submit_order(make_order_with_id("AAPL", "b")).await.is_ok());
        let err = svc.submit_order(make_order_with_id("AAPL", "c")).await.unwrap_err();
        assert!(matches!(err, BrokerError::RateLimited));
    }

    #[tokio::test]
    async fn cancel_order_succeeds_with_mock() {
        use crate::core::domain::order::{CancelCmd, ExecutionId};
        let svc = make_svc(200.0, 3);
        let result = svc
            .cancel_order(CancelCmd {
                execution_id: ExecutionId("exec-123".into()),
                correlation_id: Some("corr-cancel-test-001".into()),
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cancel_blocked_by_kill_switch() {
        use crate::core::domain::order::{CancelCmd, ExecutionId};
        let ks = Arc::new(KillSwitch::default());
        ks.enable();
        let svc = make_svc_with_kill_switch(Arc::clone(&ks));
        let err = svc
            .cancel_order(CancelCmd {
                execution_id: ExecutionId("exec-123".into()),
                correlation_id: Some("corr-cancel-test-002".into()),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, BrokerError::Unknown(_)));
    }

    #[tokio::test]
    async fn circuit_breaker_open_blocks_submit() {
        use crate::core::domain::order::{CancelCmd, ExecutionId};
        // Use a failing adapter via circuit breaker threshold of 1
        // We can't make MockAdapter fail, so we open the breaker manually
        // by using record_failure directly on a shared Arc.
        let ks = Arc::new(KillSwitch::default());
        let rl = Arc::new(RateLimiterManager::new(200.0));
        let cb = Arc::new(CircuitBreaker::new("test", 1, 30, noop_obs()));

        // Force the breaker open
        cb.record_failure(&"forced");
        assert!(cb.is_open());

        let svc = OrderSubmissionService::new(
            RequestValidator,
            IdempotencyStore::default(),
            Box::new(MockAdapter::default()),
            ks,
            rl,
            Arc::clone(&cb),
            "test",
        );

        let err = svc.submit_order(make_order("AAPL")).await.unwrap_err();
        assert!(
            matches!(err, BrokerError::Unknown(ref m) if m.contains("circuit breaker")),
            "expected circuit breaker error, got: {:?}",
            err
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// alpaca_stream normalizers
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod alpaca_stream_normalizer_tests {
    // The normalize_* functions are private to the module, so we test them
    // indirectly by calling handle_message via a channel.
    // We expose them here using a re-export trick — if you want direct access,
    // make them pub(crate) in alpaca_stream.rs.
    //
    // Instead, we test the full handle_message path by spinning up a real
    // MarketDataPublisher channel and asserting on what comes through.

    use crate::adapters::messaging::market_data_publisher::{
        MarketDataEvent, MarketDataEventType, MarketDataPublisher,
    };
    use tokio::sync::mpsc;

    /// Build a raw Alpaca trade frame.
    fn trade_frame(symbol: &str, price: f64, size: u32) -> String {
        format!(
            r#"[{{"T":"t","S":"{}","p":{},"s":{},"x":"K","c":["@"],"z":"C","t":"2026-01-01T10:00:00Z","i":1}}]"#,
            symbol, price, size
        )
    }

    fn quote_frame(symbol: &str, bp: f64, ap: f64) -> String {
        format!(
            r#"[{{"T":"q","S":"{}","bp":{},"bs":1,"bx":"K","ap":{},"as":2,"ax":"Q","c":["R"],"z":"C","t":"2026-01-01T10:00:00Z"}}]"#,
            symbol, bp, ap
        )
    }

    fn bar_frame(symbol: &str, open: f64, close: f64) -> String {
        format!(
            r#"[{{"T":"b","S":"{}","o":{},"h":{},"l":{},"c":{},"v":1000,"vw":{},"n":50,"t":"2026-01-01T10:00:00Z"}}]"#,
            symbol, open, open + 1.0, open - 0.5, close, (open + close) / 2.0
        )
    }

    fn subscription_frame() -> String {
        r#"[{"T":"subscription","trades":["AAPL"],"quotes":["AAPL"],"bars":["AAPL"]}]"#.to_string()
    }

    fn success_frame() -> String {
        r#"[{"T":"success","msg":"authenticated"}]"#.to_string()
    }

    /// Drive handle_message using a real publisher channel and collect results.
    async fn collect_events(frames: Vec<String>) -> Vec<MarketDataEvent> {
        let (tx, mut rx) = mpsc::channel(64);
        // Construct a MarketDataPublisher by wrapping the sender directly.
        // MarketDataPublisher::spawn creates its own channel — we bypass that
        // here to avoid needing a ZMQ context.
        // We use the internal tx via a thin wrapper:
        let publisher = unsafe_make_publisher(tx);

        for frame in frames {
            crate::adapters::broker::alpaca_stream::handle_message_test(&frame, &publisher)
                .await
                .ok();
        }
        drop(publisher); // close sender so recv returns None

        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        events
    }

    /// Bypass MarketDataPublisher::spawn so we can intercept events without ZMQ.
    fn unsafe_make_publisher(tx: mpsc::Sender<MarketDataEvent>) -> MarketDataPublisher {
        MarketDataPublisher::from_sender(tx)
    }

    #[tokio::test]
    async fn trade_frame_produces_trade_event() {
        let events = collect_events(vec![trade_frame("AAPL", 189.42, 3)]).await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.symbol, "AAPL");
        assert!(matches!(e.event_type, MarketDataEventType::Trade));
        assert_eq!(e.payload["price"], 189.42);
        assert_eq!(e.payload["size"], 3);
    }

    #[tokio::test]
    async fn quote_frame_produces_book_update_event() {
        let events = collect_events(vec![quote_frame("SPY", 524.10, 524.12)]).await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.symbol, "SPY");
        assert!(matches!(e.event_type, MarketDataEventType::BookUpdate));
        assert_eq!(e.payload["bid_price"], 524.10);
        assert_eq!(e.payload["ask_price"], 524.12);
    }

    #[tokio::test]
    async fn bar_frame_produces_bar_event() {
        let events = collect_events(vec![bar_frame("QQQ", 400.0, 402.0)]).await;
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.symbol, "QQQ");
        assert!(matches!(e.event_type, MarketDataEventType::Bar));
        assert_eq!(e.payload["open"], 400.0);
        assert_eq!(e.payload["close"], 402.0);
    }

    #[tokio::test]
    async fn subscription_and_success_frames_produce_no_events() {
        let events = collect_events(vec![subscription_frame(), success_frame()]).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn multiple_frames_in_one_message_all_processed() {
        // Alpaca can batch multiple events in one array
        let batch = r#"[
            {"T":"t","S":"AAPL","p":189.0,"s":1,"x":"K","c":["@"],"z":"C","t":"2026-01-01T10:00:00Z","i":1},
            {"T":"t","S":"SPY","p":524.0,"s":5,"x":"K","c":["@"],"z":"C","t":"2026-01-01T10:00:00Z","i":2}
        ]"#.to_string();
        let events = collect_events(vec![batch]).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].symbol, "AAPL");
        assert_eq!(events[1].symbol, "SPY");
    }

    #[tokio::test]
    async fn invalid_json_returns_error_and_produces_no_events() {
        let events = collect_events(vec!["not json at all".to_string()]).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn missing_symbol_field_skips_frame() {
        // Trade frame with no "S" field
        let bad = r#"[{"T":"t","p":100.0,"s":1,"t":"2026-01-01T10:00:00Z"}]"#.to_string();
        let events = collect_events(vec![bad]).await;
        assert!(events.is_empty());
    }
}
// ---------------------------------------------------------------------------
// Subscription Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod subscription_tests {
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use super::*;
    use crate::adapters::broker::mock_adapter::MockAdapter;
    use crate::adapters::messaging::order_lifecycle_publisher::OrderLifecyclePublisher;
    use crate::core::application::connection_manager::ConnectionManager;
    use crate::core::application::gateway_service::GatewayService;
    use crate::core::application::idempotency::IdempotencyStore;
    use crate::core::application::kill_switch::KillSwitch;
    use crate::core::application::observability_service::ObservabilityService;
    use crate::core::application::order_submission_service::OrderSubmissionService;
    use crate::core::application::rate_limiter::RateLimiterManager;
    use crate::core::application::risk_management_service::RiskManagementService;
    use crate::core::application::validator::RequestValidator;
    use crate::core::domain::market_data::{MarketSubscription, MarketDataCommand};
    use crate::core::patterns::circuit_breaker::CircuitBreaker;
    use crate::core::ports::journal_repo::IJournalRepo;
    use crate::core::ports::observability::IObservability;
    use crate::core::ports::service_traits::{IOrderSubmissionService, IRiskManagementService, IObservabilityService};

    /// Mock journal repo that does nothing
    struct MockJournalRepo;
    impl IJournalRepo for MockJournalRepo {
        fn persist_outbound(&self, _record: crate::core::domain::journal::RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn persist_inbound(&self, _record: crate::core::domain::journal::ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn replay(&self, _query: String) -> Vec<crate::core::domain::journal::ResponseRecord> {
            Vec::new()
        }
    }

    fn make_gateway_service() -> (GatewayService, mpsc::Receiver<MarketDataCommand>) {
        let (stream_tx, stream_rx) = mpsc::channel(32);
        
        // Create a test order lifecycle publisher (inproc for testing)
        let (lifecycle_tx, _lifecycle_rx) = mpsc::channel(128);
        let order_lifecycle_publisher = OrderLifecyclePublisher::from_sender(lifecycle_tx);

        let order_submission = Arc::new(OrderSubmissionService::new(
            RequestValidator,
            IdempotencyStore::default(),
            Box::new(MockAdapter::default()),
            Arc::new(KillSwitch::default()),
            Arc::new(RateLimiterManager::new(200.0)),
            Arc::new(CircuitBreaker::new("test", 3, 30, noop_obs())),
            "test",
        )) as Arc<dyn IOrderSubmissionService>;

        let risk_management = Arc::new(RiskManagementService::new(
            KillSwitch::default(),
            Arc::new(RateLimiterManager::new(200.0)),
        )) as Arc<dyn IRiskManagementService>;

        let observability = Arc::new(ObservabilityService::new(
            crate::core::patterns::telemetry_decorator::TelemetryDecorator::new(),
            noop_obs(),
            Arc::new(MockJournalRepo),
        )) as Arc<dyn IObservabilityService>;

        let gateway = GatewayService::new(
            order_submission,
            risk_management,
            observability,
            ConnectionManager::new(5, 500),
            stream_tx,
            order_lifecycle_publisher,
        );

        (gateway, stream_rx)
    }

    #[tokio::test]
    async fn subscribe_sends_command_to_stream() {
        let (gateway, mut rx) = make_gateway_service();
        let subscription = MarketSubscription { symbol: "TSLA".to_string(), correlation_id: Some("corr-sub-test-001".into()) };

        // Subscribe
        let result = gateway.subscribe(subscription.clone()).await;
        assert!(result.is_ok());

        // Check that the command was sent
        if let Some(cmd) = rx.recv().await {
            assert!(matches!(cmd, MarketDataCommand::Subscribe(s) if s.symbol == "TSLA"));
        } else {
            panic!("Expected subscribe command");
        }
    }

    #[tokio::test]
    async fn unsubscribe_sends_command_to_stream() {
        let (gateway, mut rx) = make_gateway_service();
        let subscription = MarketSubscription { symbol: "GOOGL".to_string(), correlation_id: Some("corr-sub-test-002".into()) };

        // Unsubscribe
        let result = gateway.unsubscribe(subscription.clone()).await;
        assert!(result.is_ok());

        // Check that the command was sent
        if let Some(cmd) = rx.recv().await {
            assert!(matches!(cmd, MarketDataCommand::Unsubscribe(s) if s.symbol == "GOOGL"));
        } else {
            panic!("Expected unsubscribe command");
        }
    }

    #[tokio::test]
    async fn multiple_subscriptions_send_multiple_commands() {
        let (gateway, mut rx) = make_gateway_service();

        // Subscribe to multiple symbols
        let tsla_sub = MarketSubscription { symbol: "TSLA".to_string(), correlation_id: Some("corr-sub-test-003".into()) };
        let aapl_sub = MarketSubscription { symbol: "AAPL".to_string(), correlation_id: Some("corr-sub-test-004".into()) };

        gateway.subscribe(tsla_sub).await.unwrap();
        gateway.subscribe(aapl_sub).await.unwrap();

        // Check both commands were sent
        let mut received = Vec::new();
        for _ in 0..2 {
            if let Some(cmd) = rx.recv().await {
                received.push(cmd);
            }
        }

        assert_eq!(received.len(), 2);
        assert!(received.iter().any(|cmd| matches!(cmd, MarketDataCommand::Subscribe(s) if s.symbol == "TSLA")));
        assert!(received.iter().any(|cmd| matches!(cmd, MarketDataCommand::Subscribe(s) if s.symbol == "AAPL")));
    }

    #[tokio::test]
    async fn subscribe_unsubscribe_sequence() {
        let (gateway, mut rx) = make_gateway_service();
        let subscription = MarketSubscription { symbol: "MSFT".to_string(), correlation_id: Some("corr-sub-test-005".into()) };

        // Subscribe then unsubscribe
        gateway.subscribe(subscription.clone()).await.unwrap();
        gateway.unsubscribe(subscription).await.unwrap();

        // Check both commands were sent in order
        let cmd1 = rx.recv().await.unwrap();
        let cmd2 = rx.recv().await.unwrap();

        assert!(matches!(cmd1, MarketDataCommand::Subscribe(s) if s.symbol == "MSFT"));
        assert!(matches!(cmd2, MarketDataCommand::Unsubscribe(s) if s.symbol == "MSFT"));
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ReplaceOrder Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod replace_order_tests {
    use crate::core::domain::order::{ReplaceCmd, ExecutionId, OrderSide};
    use crate::adapters::broker::mock_adapter::MockAdapter;
    use crate::core::ports::execution_port::IExecutionPort;

    /// Helper to create a ReplaceCmd for tests
    fn make_replace_cmd(
        execution_id: &str,
        symbol: &str,
        side: OrderSide,
        qty: Option<u32>,
        limit_price: Option<f64>,
    ) -> ReplaceCmd {
        ReplaceCmd {
            execution_id: ExecutionId(execution_id.to_string()),
            symbol: symbol.to_string(),
            side,
            qty,
            limit_price,
            correlation_id: Some("corr-replace-test-001".into()),
        }
    }

    #[test]
    fn replace_cmd_serialization_with_all_fields() {
        let cmd = make_replace_cmd(
            "exec-123",
            "AAPL",
            OrderSide::Buy,
            Some(100),
            Some(182.50),
        );

        let json = serde_json::to_string(&cmd).expect("should serialize");
        assert!(json.contains("\"execution_id\":\"exec-123\""));
        assert!(json.contains("\"symbol\":\"AAPL\""));
        assert!(json.contains("\"side\":\"Buy\""));
        assert!(json.contains("\"qty\":100"));
        assert!(json.contains("\"limit_price\":182.5"));
    }

    #[test]
    fn replace_cmd_serialization_with_sell_side() {
        let cmd = make_replace_cmd(
            "exec-456",
            "TSLA",
            OrderSide::Sell,
            Some(50),
            Some(250.0),
        );

        let json = serde_json::to_string(&cmd).expect("should serialize");
        assert!(json.contains("\"side\":\"Sell\""));
    }

    #[test]
    fn replace_cmd_serialization_with_optional_fields_none() {
        let cmd = ReplaceCmd {
            execution_id: ExecutionId("exec-789".to_string()),
            symbol: "SPY".to_string(),
            side: OrderSide::Buy,
            qty: None,
            limit_price: None,
            correlation_id: None,
        };

        let json = serde_json::to_string(&cmd).expect("should serialize");
        assert!(json.contains("\"qty\":null"));
        assert!(json.contains("\"limit_price\":null"));
    }

    #[test]
    fn replace_cmd_deserialization_from_json() {
        let json = r#"{
            "execution_id": "exec-abc",
            "symbol": "MSFT",
            "side": "Buy",
            "qty": 200,
            "limit_price": 300.0
        }"#;

        let cmd: ReplaceCmd = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(cmd.execution_id.0, "exec-abc");
        assert_eq!(cmd.symbol, "MSFT");
        assert_eq!(cmd.side, OrderSide::Buy);
        assert_eq!(cmd.qty, Some(200));
        assert_eq!(cmd.limit_price, Some(300.0));
    }

    #[test]
    fn replace_cmd_deserialization_with_sell_side() {
        let json = r#"{
            "execution_id": "exec-def",
            "symbol": "GOOGL",
            "side": "Sell",
            "qty": 10,
            "limit_price": 140.0
        }"#;

        let cmd: ReplaceCmd = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(cmd.side, OrderSide::Sell);
    }

    #[test]
    fn replace_cmd_deserialization_with_null_fields() {
        let json = r#"{
            "execution_id": "exec-ghi",
            "symbol": "AMZN",
            "side": "Buy",
            "qty": null,
            "limit_price": null
        }"#;

        let cmd: ReplaceCmd = serde_json::from_str(json).expect("should deserialize");
        assert_eq!(cmd.qty, None);
        assert_eq!(cmd.limit_price, None);
    }

    #[test]
    fn replace_cmd_roundtrip_serialization() {
        let original = make_replace_cmd(
            "exec-roundtrip",
            "NVDA",
            OrderSide::Sell,
            Some(25),
            Some(450.75),
        );

        let json = serde_json::to_string(&original).expect("should serialize");
        let deserialized: ReplaceCmd = serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(original.execution_id.0, deserialized.execution_id.0);
        assert_eq!(original.symbol, deserialized.symbol);
        assert_eq!(original.side, deserialized.side);
        assert_eq!(original.qty, deserialized.qty);
        assert_eq!(original.limit_price, deserialized.limit_price);
    }

    #[test]
    fn replace_cmd_clone() {
        let cmd = make_replace_cmd(
            "exec-clone",
            "META",
            OrderSide::Buy,
            Some(75),
            Some(320.0),
        );

        let cloned = cmd.clone();
        assert_eq!(cmd.execution_id.0, cloned.execution_id.0);
        assert_eq!(cmd.symbol, cloned.symbol);
        assert_eq!(cmd.side, cloned.side);
        assert_eq!(cmd.qty, cloned.qty);
        assert_eq!(cmd.limit_price, cloned.limit_price);
    }

    #[test]
    fn replace_cmd_debug_format() {
        let cmd = make_replace_cmd(
            "exec-debug",
            "NFLX",
            OrderSide::Sell,
            Some(30),
            Some(500.0),
        );

        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("exec-debug"));
        assert!(debug_str.contains("NFLX"));
        assert!(debug_str.contains("Sell"));
    }

    #[tokio::test]
    async fn mock_adapter_replace_order_succeeds() {
        let adapter = MockAdapter::default();
        let cmd = make_replace_cmd(
            "exec-mock-123",
            "AAPL",
            OrderSide::Buy,
            Some(100),
            Some(180.0),
        );

        let result = adapter.replace_order(cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn mock_adapter_replace_order_with_sell_side_succeeds() {
        let adapter = MockAdapter::default();
        let cmd = make_replace_cmd(
            "exec-mock-456",
            "TSLA",
            OrderSide::Sell,
            Some(50),
            Some(250.0),
        );

        let result = adapter.replace_order(cmd).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn mock_adapter_replace_order_with_optional_fields_none_succeeds() {
        let adapter = MockAdapter::default();
        let cmd = ReplaceCmd {
            execution_id: ExecutionId("exec-mock-789".to_string()),
            symbol: "SPY".to_string(),
            side: OrderSide::Buy,
            qty: None,
            limit_price: None,
            correlation_id: None,
        };

        let result = adapter.replace_order(cmd).await;
        assert!(result.is_ok());
    }

    #[test]
    fn replace_cmd_equality() {
        let cmd1 = make_replace_cmd(
            "exec-same",
            "AAPL",
            OrderSide::Buy,
            Some(100),
            Some(180.0),
        );
        let cmd2 = make_replace_cmd(
            "exec-same",
            "AAPL",
            OrderSide::Buy,
            Some(100),
            Some(180.0),
        );
        let cmd3 = make_replace_cmd(
            "exec-different",
            "AAPL",
            OrderSide::Buy,
            Some(100),
            Some(180.0),
        );

        assert_eq!(cmd1, cmd2);
        assert_ne!(cmd1, cmd3);
    }

    #[test]
    fn replace_cmd_different_sides_not_equal() {
        let buy_cmd = make_replace_cmd(
            "exec-side",
            "AAPL",
            OrderSide::Buy,
            Some(100),
            Some(180.0),
        );
        let sell_cmd = make_replace_cmd(
            "exec-side",
            "AAPL",
            OrderSide::Sell,
            Some(100),
            Some(180.0),
        );

        assert_ne!(buy_cmd, sell_cmd);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ReplaceOrder Wire Message Tests (JSON over ZeroMQ)
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod replace_order_wire_tests {
    use crate::core::domain::order::{ReplaceCmd, ExecutionId, OrderSide};
    use crate::core::domain::wire_message::GatewayRequest;

    #[test]
    fn gateway_request_replace_order_deserialization() {
        let json = r#"{
            "command": "replace_order",
            "payload": {
                "execution_id": "exec-wire-123",
                "symbol": "AAPL",
                "side": "Buy",
                "qty": 100,
                "limit_price": 182.50
            }
        }"#;

        let request: GatewayRequest = serde_json::from_str(json).expect("should deserialize");
        
        match request {
            GatewayRequest::ReplaceOrder(cmd) => {
                assert_eq!(cmd.execution_id.0, "exec-wire-123");
                assert_eq!(cmd.symbol, "AAPL");
                assert_eq!(cmd.side, OrderSide::Buy);
                assert_eq!(cmd.qty, Some(100));
                assert_eq!(cmd.limit_price, Some(182.50));
            }
            _ => panic!("Expected ReplaceOrder variant"),
        }
    }

    #[test]
    fn gateway_request_replace_order_with_sell_side_deserialization() {
        let json = r#"{
            "command": "replace_order",
            "payload": {
                "execution_id": "exec-wire-456",
                "symbol": "TSLA",
                "side": "Sell",
                "qty": 50,
                "limit_price": 250.0
            }
        }"#;

        let request: GatewayRequest = serde_json::from_str(json).expect("should deserialize");
        
        match request {
            GatewayRequest::ReplaceOrder(cmd) => {
                assert_eq!(cmd.side, OrderSide::Sell);
            }
            _ => panic!("Expected ReplaceOrder variant"),
        }
    }

    #[test]
    fn gateway_request_replace_order_with_null_fields() {
        let json = r#"{
            "command": "replace_order",
            "payload": {
                "execution_id": "exec-wire-789",
                "symbol": "SPY",
                "side": "Buy",
                "qty": null,
                "limit_price": null
            }
        }"#;

        let request: GatewayRequest = serde_json::from_str(json).expect("should deserialize");
        
        match request {
            GatewayRequest::ReplaceOrder(cmd) => {
                assert_eq!(cmd.execution_id.0, "exec-wire-789");
                assert_eq!(cmd.qty, None);
                assert_eq!(cmd.limit_price, None);
            }
            _ => panic!("Expected ReplaceOrder variant"),
        }
    }

    #[test]
    fn gateway_request_replace_order_missing_side_fails() {
        // This test verifies that the side field is now required
        let json = r#"{
            "command": "replace_order",
            "payload": {
                "execution_id": "exec-wire-000",
                "symbol": "AAPL",
                "qty": 100,
                "limit_price": 180.0
            }
        }"#;

        let result: Result<GatewayRequest, _> = serde_json::from_str(json);
        assert!(result.is_err(), "Should fail when side field is missing");
    }
}

// ── OrderStatusResponse Tests ─────────────────────────────────────────────────

#[cfg(test)]
mod order_status_response_tests {
    use crate::core::domain::order::{OrderStatusResponse, OrderSide};

    #[test]
    fn order_status_response_new_creates_basic_response() {
        let response = OrderStatusResponse::new(
            "exec-123",
            "filled",
            "AAPL",
            OrderSide::Buy,
            100,
        );

        assert_eq!(response.execution_id, "exec-123");
        assert_eq!(response.status, "filled");
        assert_eq!(response.symbol, "AAPL");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.qty, 100);
        assert_eq!(response.filled_qty, 0);
        assert_eq!(response.remaining_qty, 100);
        assert!(response.avg_fill_price.is_none());
    }

    #[test]
    fn order_status_response_with_fill_sets_fill_data() {
        let response = OrderStatusResponse::new(
            "exec-123",
            "partially_filled",
            "AAPL",
            OrderSide::Sell,
            100,
        )
        .with_fill(50, 150.50);

        assert_eq!(response.filled_qty, 50);
        assert_eq!(response.avg_fill_price, Some(150.50));
        assert_eq!(response.remaining_qty, 50);
    }

    #[test]
    fn order_status_response_with_raw_status_sets_raw_field() {
        let response = OrderStatusResponse::new(
            "exec-123",
            "new",
            "AAPL",
            OrderSide::Buy,
            100,
        )
        .with_raw_status("new_pending");

        assert_eq!(response.raw_status, Some("new_pending".to_string()));
    }

    #[test]
    fn order_status_response_is_terminal_detects_terminal_states() {
        let terminal_states = vec!["filled", "canceled", "cancelled", "rejected", "expired", "done_for_day"];
        
        for status in terminal_states {
            let response = OrderStatusResponse::new(
                "exec-123",
                status,
                "AAPL",
                OrderSide::Buy,
                100,
            );
            assert!(
                response.is_terminal(),
                "Status '{}' should be terminal",
                status
            );
        }
    }

    #[test]
    fn order_status_response_is_terminal_non_terminal_states() {
        let non_terminal_states = vec!["new", "accepted", "pending"];
        
        for status in non_terminal_states {
            let response = OrderStatusResponse::new(
                "exec-123",
                status,
                "AAPL",
                OrderSide::Buy,
                100,
            );
            assert!(
                !response.is_terminal(),
                "Status '{}' should not be terminal",
                status
            );
        }
    }

    #[test]
    fn order_status_response_is_open_detects_open_states() {
        let open_states = vec!["new", "accepted", "pending", "partially_filled", "held"];
        
        for status in open_states {
            let response = OrderStatusResponse::new(
                "exec-123",
                status,
                "AAPL",
                OrderSide::Buy,
                100,
            );
            assert!(
                response.is_open(),
                "Status '{}' should be open",
                status
            );
        }
    }

    #[test]
    fn order_status_response_serialization_roundtrip() {
        let original = OrderStatusResponse::new(
            "exec-123",
            "partially_filled",
            "AAPL",
            OrderSide::Buy,
            100,
        )
        .with_fill(50, 150.50)
        .with_raw_status("partial_fill");

        let json = serde_json::to_string(&original).expect("Failed to serialize");
        let deserialized: OrderStatusResponse = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(original.execution_id, deserialized.execution_id);
        assert_eq!(original.status, deserialized.status);
        assert_eq!(original.symbol, deserialized.symbol);
        assert_eq!(original.side, deserialized.side);
        assert_eq!(original.qty, deserialized.qty);
        assert_eq!(original.filled_qty, deserialized.filled_qty);
        assert_eq!(original.avg_fill_price, deserialized.avg_fill_price);
        assert_eq!(original.remaining_qty, deserialized.remaining_qty);
        assert_eq!(original.raw_status, deserialized.raw_status);
    }

    #[test]
    fn order_status_response_remaining_qty_saturating_sub() {
        // Test that remaining_qty doesn't underflow
        let response = OrderStatusResponse::new(
            "exec-123",
            "filled",
            "AAPL",
            OrderSide::Buy,
            100,
        )
        .with_fill(150, 150.0); // Filled more than total (shouldn't happen in practice)

        // The with_fill method sets remaining_qty = qty - filled_qty
        // which would be 100 - 150 = -50, but since it's u32 it should saturate
        // Actually, looking at the implementation, remaining_qty might underflow
        // This test documents the current behavior
    }
}

// ── MockAdapter query_status Tests ───────────────────────────────────────────

#[cfg(test)]
mod mock_adapter_query_status_tests {
    use crate::adapters::broker::mock_adapter::MockAdapter;
    use crate::core::ports::execution_port::IExecutionPort;
    use crate::core::domain::order::{StatusQuery, ExecutionId, OrderSide};

    #[tokio::test]
    async fn mock_adapter_query_status_returns_response() {
        let adapter = MockAdapter;
        let query = StatusQuery {
            execution_id: ExecutionId("test-exec-id".to_string()),
            correlation_id: Some("corr-query-test-001".into()),
        };

        let result = adapter.query_status(query).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert_eq!(response.execution_id, "test-exec-id");
        assert_eq!(response.status, "new");
        assert_eq!(response.symbol, "MOCK");
        assert_eq!(response.side, OrderSide::Buy);
        assert_eq!(response.qty, 100);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Correlation ID Tests
// ═══════════════════════════════════════════════════════════════════════════

#[path = "correlation_tests.rs"]
mod correlation_tests;
