// core/application/rate_limiter.rs
//
// Token-bucket rate limiter, one bucket per broker.
// Tokens refill continuously based on elapsed wall-clock time.
// Default capacity: 200 req/min (Alpaca paper trading limit).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use crate::core::infrastructure::MutexExt;

/// Back-pressure status for a broker's rate limiter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BackPressureStatus {
    /// Current token count (fractional).
    pub tokens_remaining: f64,
    /// Maximum token capacity.
    pub capacity: f64,
    /// Percentage of capacity remaining (0.0 - 100.0).
    pub percent_remaining: f64,
    /// True if tokens are below the configured threshold.
    pub is_near_limit: bool,
}

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

    /// Refill tokens based on elapsed time without consuming.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;
    }

    /// Try to consume `needed` tokens.  Returns true if allowed.
    fn try_consume(&mut self, needed: f64) -> bool {
        // Refill based on elapsed time
        self.refill();

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
        let mut buckets = self.buckets.safe_lock();
        let refill_rate = self.default_rpm / 60.0;
        let bucket = buckets
            .entry(broker_id.to_string())
            .or_insert_with(|| Bucket::new(self.default_rpm, refill_rate));
        bucket.try_consume(tokens as f64)
    }

    /// Get the current back-pressure status for a broker.
    /// Returns None if the broker has no bucket (hasn't been used yet).
    pub fn get_back_pressure_status(&self, broker_id: &str, near_limit_threshold_percent: f64) -> Option<BackPressureStatus> {
        let mut buckets = self.buckets.safe_lock();
        let refill_rate = self.default_rpm / 60.0;
        let bucket = buckets
            .entry(broker_id.to_string())
            .or_insert_with(|| Bucket::new(self.default_rpm, refill_rate));
        
        // Refill before checking status
        bucket.refill();
        
        let percent_remaining = (bucket.tokens / bucket.capacity) * 100.0;
        
        Some(BackPressureStatus {
            tokens_remaining: bucket.tokens,
            capacity: bucket.capacity,
            percent_remaining,
            is_near_limit: percent_remaining < near_limit_threshold_percent,
        })
    }

    /// Check if the broker is near its rate limit.
    /// Returns true if remaining tokens are below the threshold percentage.
    /// Default threshold is 20% if not specified.
    pub fn is_near_limit(&self, broker_id: &str, threshold_percent: Option<f64>) -> bool {
        let threshold = threshold_percent.unwrap_or(20.0);
        self.get_back_pressure_status(broker_id, threshold)
            .map(|status| status.is_near_limit)
            .unwrap_or(false)
    }

    /// Get the current token count for a broker.
    /// Returns 0.0 if the broker has no bucket.
    pub fn tokens_remaining(&self, broker_id: &str) -> f64 {
        let mut buckets = self.buckets.safe_lock();
        let refill_rate = self.default_rpm / 60.0;
        let bucket = buckets
            .entry(broker_id.to_string())
            .or_insert_with(|| Bucket::new(self.default_rpm, refill_rate));
        
        // Refill before returning count
        bucket.refill();
        bucket.tokens
    }

    /// Get the capacity for a broker.
    /// Returns default_rpm if the broker has no bucket.
    pub fn get_capacity(&self, broker_id: &str) -> f64 {
        let buckets = self.buckets.safe_lock();
        buckets
            .get(broker_id)
            .map(|b| b.capacity)
            .unwrap_or(self.default_rpm)
    }
}