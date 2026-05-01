// core/application/connection_manager.rs
//
// Manages broker connections with exponential back-off reconnection.
//
// Back-off schedule (base=500ms, factor=2, max_attempts=5):
//   attempt 1: wait  500ms
//   attempt 2: wait 1000ms
//   attempt 3: wait 2000ms
//   attempt 4: wait 4000ms
//   attempt 5: wait 8000ms  → give up, emit error
//
// All values are configurable via AppConfig.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;

use crate::core::domain::request::{Connection, BrokerId};

pub struct ConnectionManager {
    /// broker_id → active connection (simplified; one connection per broker)
    connections: Mutex<HashMap<String, Connection>>,
    max_attempts: u32,
    base_ms: u64,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new(5, 500)
    }
}

impl ConnectionManager {
    pub fn new(max_attempts: u32, base_ms: u64) -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
            max_attempts,
            base_ms,
        }
    }

    /// Return an existing connection for the broker, if one exists.
    pub fn get_connection(&self, broker_id: BrokerId) -> Option<Connection> {
        self.connections
            .lock()
            .ok()
            .and_then(|conns| conns.get(&broker_id.0).cloned())
    }

    /// Store a connection for a broker.
    pub fn register_connection(&self, broker_id: BrokerId, conn: Connection) {
        if let Ok(mut conns) = self.connections.lock() {
            conns.insert(broker_id.0, conn);
        }
    }

    /// Release (remove) a connection.
    pub fn release_connection(&self, broker_id: &str) {
        if let Ok(mut conns) = self.connections.lock() {
            conns.remove(broker_id);
        }
    }

    /// Attempt to reconnect with exponential back-off.
    ///
    /// `connect_fn` is an async closure that attempts the connection and
    /// returns Ok(Connection) on success or Err(String) on failure.
    ///
    /// Returns Ok(Connection) if reconnection succeeded within max_attempts,
    /// or Err with the last error message if all attempts were exhausted.
    pub async fn reconnect_with_backoff<F, Fut>(
        &self,
        broker_id: &str,
        connect_fn: F,
    ) -> Result<Connection, String>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Connection, String>>,
    {
        let mut delay_ms = self.base_ms;
        let mut last_err = String::from("no attempts made");

        for attempt in 1..=self.max_attempts {
            tracing::info!(
                "reconnect attempt {}/{} for broker={} (delay={}ms)",
                attempt,
                self.max_attempts,
                broker_id,
                if attempt == 1 { 0 } else { delay_ms }
            );

            if attempt > 1 {
                sleep(Duration::from_millis(delay_ms)).await;
                delay_ms *= 2; // exponential back-off
            }

            match connect_fn().await {
                Ok(conn) => {
                    tracing::info!(
                        "reconnect succeeded for broker={} on attempt {}",
                        broker_id,
                        attempt
                    );
                    self.register_connection(BrokerId(broker_id.to_string()), conn.clone());
                    return Ok(conn);
                }
                Err(e) => {
                    tracing::warn!(
                        "reconnect attempt {} failed for broker={}: {}",
                        attempt,
                        broker_id,
                        e
                    );
                    last_err = e;
                }
            }
        }

        tracing::error!(
            "all {} reconnect attempts exhausted for broker={}",
            self.max_attempts,
            broker_id
        );
        Err(last_err)
    }
}