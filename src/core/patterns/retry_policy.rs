// core/patterns/retry_policy.rs
//
// Retry policies for both synchronous and asynchronous operations.
// Supports configurable backoff strategies and error filtering.

use std::future::Future;
use std::time::Duration;
use std::pin::Pin;

/// Backoff strategy for retries
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BackoffStrategy {
    /// Fixed interval between retries
    Fixed(Duration),
    /// Exponential backoff: base * 2^attempt
    Exponential {
        /// Initial delay
        base: Duration,
        /// Maximum delay cap
        max: Duration,
    },
    /// Linear backoff: base * attempt
    Linear {
        /// Base delay
        base: Duration,
        /// Maximum delay cap
        max: Duration,
    },
    /// No delay between retries
    None,
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::Exponential {
            base: Duration::from_millis(100),
            max: Duration::from_secs(5),
        }
    }
}

impl BackoffStrategy {
    /// Calculate delay for a specific attempt (0-indexed)
    pub fn delay_for(&self, attempt: usize) -> Duration {
        match self {
            BackoffStrategy::Fixed(duration) => *duration,
            BackoffStrategy::Exponential { base, max } => {
                let delay = base.as_millis() as u64 * 2_u64.pow(attempt as u32);
                let delay_ms = delay.min(max.as_millis() as u64);
                Duration::from_millis(delay_ms)
            }
            BackoffStrategy::Linear { base, max } => {
                let delay = base.as_millis() as u64 * (attempt as u64 + 1);
                let delay_ms = delay.min(max.as_millis() as u64);
                Duration::from_millis(delay_ms)
            }
            BackoffStrategy::None => Duration::from_millis(0),
        }
    }
}

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: usize,
    /// Backoff strategy between retries
    pub backoff: BackoffStrategy,
    /// Optional predicate to determine if an error is retryable
    pub retry_if: Option<fn(&dyn std::fmt::Debug) -> bool>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: BackoffStrategy::default(),
            retry_if: None,
        }
    }
}

impl RetryConfig {
    /// Create a config with no retries
    pub fn no_retries() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Create a config with fixed backoff
    pub fn with_fixed_backoff(interval: Duration) -> Self {
        Self {
            backoff: BackoffStrategy::Fixed(interval),
            ..Default::default()
        }
    }

    /// Create a config with exponential backoff
    pub fn with_exponential_backoff(base: Duration, max: Duration) -> Self {
        Self {
            backoff: BackoffStrategy::Exponential { base, max },
            ..Default::default()
        }
    }

    /// Set custom retry predicate
    pub fn with_retry_if(mut self, predicate: fn(&dyn std::fmt::Debug) -> bool) -> Self {
        self.retry_if = Some(predicate);
        self
    }
}

/// Synchronous retry policy (legacy support)
pub struct RetryPolicy {
    pub retries: usize,
    pub backoff: BackoffStrategy,
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new(retries: usize) -> Self {
        Self {
            retries,
            backoff: BackoffStrategy::None,
        }
    }

    /// Create a retry policy with exponential backoff
    pub fn with_exponential_backoff(retries: usize, base: Duration, max: Duration) -> Self {
        Self {
            retries,
            backoff: BackoffStrategy::Exponential { base, max },
        }
    }

    /// Execute a synchronous operation with retries
    pub fn execute<F, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Result<T, E>,
    {
        let mut last_error = None;
        
        for attempt in 0..=self.retries {
            match f() {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.retries {
                        std::thread::sleep(self.backoff.delay_for(attempt));
                    }
                }
            }
        }
        
        Err(last_error.unwrap())
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(3)
    }
}

/// Asynchronous retry policy for async operations
pub struct AsyncRetryPolicy {
    config: RetryConfig,
}

impl AsyncRetryPolicy {
    /// Create a new async retry policy from config
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    /// Create with default configuration
    pub fn default() -> Self {
        Self::new(RetryConfig::default())
    }

    /// Create a policy with no retries
    pub fn no_retries() -> Self {
        Self::new(RetryConfig::no_retries())
    }

    /// Execute an async operation with retries
    /// 
    /// # Example
    /// ```
    /// use broker_gateway_service::core::patterns::retry_policy::AsyncRetryPolicy;
    /// 
    /// async fn example() {
    ///     let policy = AsyncRetryPolicy::default();
    ///     let result = policy.execute(|| async {
    ///         Ok::<i32, ()>(42)
    ///     }).await;
    ///     assert_eq!(result.unwrap(), 42);
    /// }
    /// ```
    pub async fn execute<F, Fut, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut last_error = None;
        
        for attempt in 0..=self.config.max_retries {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        let delay = self.config.backoff.delay_for(attempt);
                        if delay > Duration::from_millis(0) {
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        }
        
        Err(last_error.unwrap())
    }

    /// Execute with a custom retry condition
    pub async fn execute_with_condition<F, Fut, T, E>(
        &self,
        mut f: F,
        retry_if: impl Fn(&E) -> bool,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut last_error = None;
        
        for attempt in 0..=self.config.max_retries {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    if !retry_if(&e) {
                        return Err(e);
                    }
                    last_error = Some(e);
                    if attempt < self.config.max_retries {
                        let delay = self.config.backoff.delay_for(attempt);
                        if delay > Duration::from_millis(0) {
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        }
        
        Err(last_error.unwrap())
    }
}

/// Retry a future with the given configuration
/// 
/// This is a convenience function for one-off retries
/// 
/// # Example
/// ```
/// use std::time::Duration;
/// use broker_gateway_service::core::patterns::retry_policy::{retry_with_config, RetryConfig};
/// 
/// async fn example() {
///     let result = retry_with_config(
///         RetryConfig::with_exponential_backoff(
///             Duration::from_millis(100),
///             Duration::from_secs(5)
///         ),
///         || async { Ok::<i32, ()>(42) }
///     ).await;
///     assert_eq!(result.unwrap(), 42);
/// }
/// ```
pub async fn retry_with_config<F, Fut, T, E>(
    config: RetryConfig,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let policy = AsyncRetryPolicy::new(config);
    policy.execute(f).await
}

/// Retry a future with default configuration
pub async fn retry<F, Fut, T, E>(f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let policy = AsyncRetryPolicy::default();
    policy.execute(f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // =========================================================================
    // Synchronous Retry Tests
    // =========================================================================

    #[test]
    fn test_sync_retry_success_first_try() {
        let policy = RetryPolicy::new(3);
        let result = policy.execute(|| Ok::<_, ()>(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_sync_retry_eventually_succeeds() {
        let policy = RetryPolicy::new(3);
        let attempts = AtomicUsize::new(0);
        
        let result = policy.execute(|| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err::<i32, &str>("temporary failure")
            } else {
                Ok(42)
            }
        });
        
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_sync_retry_exhausted() {
        let policy = RetryPolicy::new(2);
        let attempts = AtomicUsize::new(0);
        
        let result: Result<i32, &str> = policy.execute(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err("always fails")
        });
        
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Initial + 2 retries
    }

    // =========================================================================
    // Backoff Strategy Tests
    // =========================================================================

    #[test]
    fn test_fixed_backoff() {
        let strategy = BackoffStrategy::Fixed(Duration::from_millis(100));
        assert_eq!(strategy.delay_for(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for(1), Duration::from_millis(100));
        assert_eq!(strategy.delay_for(5), Duration::from_millis(100));
    }

    #[test]
    fn test_exponential_backoff() {
        let strategy = BackoffStrategy::Exponential {
            base: Duration::from_millis(100),
            max: Duration::from_secs(1),
        };
        
        assert_eq!(strategy.delay_for(0), Duration::from_millis(100));   // 100 * 2^0
        assert_eq!(strategy.delay_for(1), Duration::from_millis(200));   // 100 * 2^1
        assert_eq!(strategy.delay_for(2), Duration::from_millis(400));   // 100 * 2^2
        assert_eq!(strategy.delay_for(3), Duration::from_millis(800));   // 100 * 2^3
    }

    #[test]
    fn test_exponential_backoff_with_cap() {
        let strategy = BackoffStrategy::Exponential {
            base: Duration::from_millis(100),
            max: Duration::from_millis(500),
        };
        
        assert_eq!(strategy.delay_for(0), Duration::from_millis(100));
        assert_eq!(strategy.delay_for(1), Duration::from_millis(200));
        assert_eq!(strategy.delay_for(2), Duration::from_millis(400));
        assert_eq!(strategy.delay_for(3), Duration::from_millis(500)); // Capped
        assert_eq!(strategy.delay_for(10), Duration::from_millis(500)); // Capped
    }

    #[test]
    fn test_linear_backoff() {
        let strategy = BackoffStrategy::Linear {
            base: Duration::from_millis(100),
            max: Duration::from_secs(1),
        };
        
        assert_eq!(strategy.delay_for(0), Duration::from_millis(100));  // 100 * 1
        assert_eq!(strategy.delay_for(1), Duration::from_millis(200));  // 100 * 2
        assert_eq!(strategy.delay_for(2), Duration::from_millis(300));  // 100 * 3
    }

    #[test]
    fn test_no_backoff() {
        let strategy = BackoffStrategy::None;
        assert_eq!(strategy.delay_for(0), Duration::from_millis(0));
        assert_eq!(strategy.delay_for(100), Duration::from_millis(0));
    }

    // =========================================================================
    // Async Retry Tests
    // =========================================================================

    #[tokio::test]
    async fn test_async_retry_success_first_try() {
        let policy = AsyncRetryPolicy::default();
        let result = policy.execute(|| async { Ok::<_, ()>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_async_retry_eventually_succeeds() {
        let config = RetryConfig {
            max_retries: 3,
            backoff: BackoffStrategy::None, // No delay for faster test
            retry_if: None,
        };
        let policy = AsyncRetryPolicy::new(config);
        let attempts = AtomicUsize::new(0);
        
        let result = policy.execute(|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err::<i32, &str>("temporary failure")
            } else {
                Ok(42)
            }
        }).await;
        
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_async_retry_exhausted() {
        let config = RetryConfig {
            max_retries: 2,
            backoff: BackoffStrategy::None,
            retry_if: None,
        };
        let policy = AsyncRetryPolicy::new(config);
        let attempts = AtomicUsize::new(0);
        
        let result: Result<i32, &str> = policy.execute(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err("always fails")
        }).await;
        
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Initial + 2 retries
    }

    #[tokio::test]
    async fn test_async_retry_with_condition() {
        let config = RetryConfig {
            max_retries: 5,
            backoff: BackoffStrategy::None,
            retry_if: None,
        };
        let policy = AsyncRetryPolicy::new(config);
        
        // Only retry on "retryable" errors
        let result = policy.execute_with_condition(
            || async { Err::<i32, &str>("fatal error") },
            |e| e.contains("retryable"),
        ).await;
        
        // Should fail immediately since error is not retryable
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_retry_condition_retries_on_match() {
        let config = RetryConfig {
            max_retries: 3,
            backoff: BackoffStrategy::None,
            retry_if: None,
        };
        let policy = AsyncRetryPolicy::new(config);
        let attempts = AtomicUsize::new(0);
        
        let result = policy.execute_with_condition(
            || async {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err::<i32, &str>("retryable error")
                } else {
                    Ok(42)
                }
            },
            |e| e.contains("retryable"),
        ).await;
        
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_with_config_convenience() {
        let config = RetryConfig::with_fixed_backoff(Duration::from_millis(10));
        
        let attempts = AtomicUsize::new(0);
        let result = retry_with_config(config, || async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Err::<i32, &str>("retry")
            } else {
                Ok(42)
            }
        }).await;
        
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_async_retry_no_retries() {
        let policy = AsyncRetryPolicy::no_retries();
        let attempts = AtomicUsize::new(0);
        
        let result: Result<i32, &str> = policy.execute(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err("fails")
        }).await;
        
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // Only one attempt
    }

    // =========================================================================
    // Configuration Tests
    // =========================================================================

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert!(matches!(config.backoff, BackoffStrategy::Exponential { .. }));
    }

    #[test]
    fn test_retry_config_no_retries() {
        let config = RetryConfig::no_retries();
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_retry_config_with_fixed_backoff() {
        let config = RetryConfig::with_fixed_backoff(Duration::from_millis(500));
        assert!(matches!(config.backoff, BackoffStrategy::Fixed(d) if d == Duration::from_millis(500)));
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::default()
            .with_retry_if(|e: &dyn std::fmt::Debug| true);
        
        assert!(config.retry_if.is_some());
    }
}
