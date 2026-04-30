// core/application/rate_limiter.rs
//
// Token-bucket rate limiter, one bucket per broker.
// Tokens refill continuously based on elapsed wall-clock time.
// Default capacity: 200 req/min (Alpaca paper trading limit).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// One token bucket for a single broker.
struct Bucket {
    /// Maximum tokens (= burst capacity).
    capacity: f64,
    /// Tokens added per second.
    refill_rate: f64,
    /// Current token count (fractional).
    tokens: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

impl Bucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            refill_rate,
            tokens: capacity, // start full
            last_refill: Instant::now(),
        }
    }

    /// Try to consume `needed` tokens.  Returns true if allowed.
    fn try_consume(&mut self, needed: f64) -> bool {
        // Refill based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= needed {
            self.tokens -= needed;
            true
        } else {
            false
        }
    }
}

pub struct RateLimiterManager {
    /// broker_id → bucket
    buckets: Mutex<HashMap<String, Bucket>>,
    /// Default requests per minute for unknown brokers
    default_rpm: f64,
}

impl Default for RateLimiterManager {
    fn default() -> Self {
        Self::new(200.0) // Alpaca paper: 200 req/min
    }
}

impl RateLimiterManager {
    pub fn new(default_rpm: f64) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            default_rpm,
        }
    }

    /// Register a broker with a specific requests-per-minute limit.
    pub fn register(&self, broker_id: &str, rpm: f64) {
        let refill_rate = rpm / 60.0; // convert to per-second
        self.buckets
            .lock()
            .unwrap()
            .insert(broker_id.to_string(), Bucket::new(rpm, refill_rate));
    }

    /// Returns true if the broker has enough tokens for `tokens` requests.
    pub fn allow(&self, broker_id: &str, tokens: u32) -> bool {
        let mut buckets = self.buckets.lock().unwrap();
        let refill_rate = self.default_rpm / 60.0;
        let bucket = buckets
            .entry(broker_id.to_string())
            .or_insert_with(|| Bucket::new(self.default_rpm, refill_rate));
        bucket.try_consume(tokens as f64)
    }
}