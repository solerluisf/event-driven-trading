// core/patterns/circuit_breaker.rs
//
// Three-state circuit breaker: Closed → Open → HalfOpen → Closed.
//
//  Closed   : calls go through; failures are counted.
//  Open     : calls are rejected immediately; a cooldown timer runs.
//  HalfOpen : one probe call is allowed through; success closes it,
//             failure reopens it.
//
// On every state transition the breaker emits to both tracing and
// the IObservability port so MetricsAdapter can record it.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::core::ports::observability::IObservability;

#[derive(Debug, Clone, PartialEq)]
enum State {
    Closed,
    Open { until: Instant },
    HalfOpen,
}

struct Inner {
    state: State,
    failures: u32,
    failure_threshold: u32,
    cooldown: Duration,
}

pub struct CircuitBreaker {
    broker_id: String,
    inner: Mutex<Inner>,
    observability: Arc<dyn IObservability + Send + Sync>,
}

impl CircuitBreaker {
    pub fn new(
        broker_id: impl Into<String>,
        failure_threshold: u32,
        cooldown_secs: u64,
        observability: Arc<dyn IObservability + Send + Sync>,
    ) -> Self {
        Self {
            broker_id: broker_id.into(),
            inner: Mutex::new(Inner {
                state: State::Closed,
                failures: 0,
                failure_threshold,
                cooldown: Duration::from_secs(cooldown_secs),
            }),
            observability,
        }
    }

    /// Wrap a fallible async-compatible closure.
    /// Returns Err immediately if the breaker is Open.
    pub fn call<F, T>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, String>,
    {
        {
            let mut g = self.inner.lock().unwrap();
            match &g.state {
                State::Open { until } => {
                    if Instant::now() < *until {
                        let msg = format!(
                            "circuit_breaker.rejected broker={} state=open",
                            self.broker_id
                        );
                        tracing::warn!("{}", msg);
                        self.observability.emit(msg.clone());
                        return Err(msg);
                    } else {
                        // Cooldown expired → probe
                        g.state = State::HalfOpen;
                        let msg = format!(
                            "circuit_breaker.half_open broker={}",
                            self.broker_id
                        );
                        tracing::info!("{}", msg);
                        self.observability.emit(msg);
                    }
                }
                State::Closed | State::HalfOpen => {}
            }
        }

        match f() {
            Ok(v) => {
                self.record_success();
                Ok(v)
            }
            Err(e) => {
                self.record_failure(&e);
                Err(e)
            }
        }
    }

    pub fn record_success(&self) {
        let mut g = self.inner.lock().unwrap();
        let was_half_open = g.state == State::HalfOpen;
        g.failures = 0;
        g.state = State::Closed;
        if was_half_open {
            let msg = format!("circuit_breaker.closed broker={}", self.broker_id);
            tracing::info!("{}", msg);
            self.observability.emit(msg);
        }
    }

    pub fn record_failure<E: std::fmt::Display>(&self, err: &E) {
        let mut g = self.inner.lock().unwrap();
        g.failures += 1;
        let msg = format!(
            "circuit_breaker.failure broker={} failures={} err={}",
            self.broker_id, g.failures, err
        );
        tracing::warn!("{}", msg);
        self.observability.emit(msg.clone());

        if g.failures >= g.failure_threshold || g.state == State::HalfOpen {
            let cooldown = g.cooldown.max(Duration::from_millis(1));
            let until = Instant::now() + cooldown;
            g.state = State::Open { until };
            let open_msg = format!(
                "circuit_breaker.opened broker={} cooldown_secs={}",
                self.broker_id,
                g.cooldown.as_secs()
            );
            tracing::error!("{}", open_msg);
            self.observability.emit(open_msg);
        }
    }

    pub fn is_open(&self) -> bool {
        let g = self.inner.lock().unwrap();
        matches!(&g.state, State::Open { until } if Instant::now() < *until)
    }
}