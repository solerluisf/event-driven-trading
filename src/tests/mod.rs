// src/tests/mod.rs

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
    use crate::adapters::broker::broker_error::BrokerError;
    use crate::core::application::kill_switch::KillSwitch;
    use crate::core::application::rate_limiter::RateLimiterManager;
    use crate::core::application::risk_management_service::RiskManagementService;

    fn make_svc(rpm: f64) -> RiskManagementService {
        RiskManagementService::new(KillSwitch::default(), RateLimiterManager::new(rpm))
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