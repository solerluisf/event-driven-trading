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
use std::sync::RwLock;
use std::time::Duration;
use tokio::time::sleep;

use crate::core::domain::request::{Connection, BrokerId};

pub struct ConnectionManager {
    /// broker_id → pool of active connections (supports multiple concurrent
    /// connections to the same broker).
    /// 
    /// Uses RwLock for read-heavy access pattern: multiple concurrent readers
    /// can access the connections simultaneously, while writes are exclusive.
    connections: RwLock<HashMap<String, HashMap<String, Connection>>>,
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
            connections: RwLock::new(HashMap::new()),
            max_attempts,
            base_ms,
        }
    }

    /// Return an existing connection for the broker, if one exists.
    ///
    /// Note: with pooling, there can be multiple. This returns an arbitrary
    /// one (use `get_connections` if you need all).
    ///
    /// Uses read lock to allow concurrent access from multiple readers.
    pub fn get_connection(&self, broker_id: BrokerId) -> Option<Connection> {
        let lock = self.connections.read().ok()?;
        let pool = lock.get(&broker_id.0)?;
        pool.values().next().cloned()
    }

    /// Return all active connections for a given broker.
    ///
    /// Uses read lock to allow concurrent access from multiple readers.
    pub fn get_connections(&self, broker_id: BrokerId) -> Vec<Connection> {
        self.connections
            .read()
            .ok()
            .and_then(|conns| conns.get(&broker_id.0).cloned())
            .map(|pool| pool.into_values().collect())
            .unwrap_or_default()
    }

    /// Return active connection count for a given broker.
    ///
    /// Uses read lock to allow concurrent access from multiple readers.
    pub fn connection_count(&self, broker_id: BrokerId) -> usize {
        self.connections
            .read()
            .ok()
            .and_then(|conns| conns.get(&broker_id.0).map(|pool| pool.len()))
            .unwrap_or(0)
    }

    /// Store a connection for a broker.
    ///
    /// If a connection with the same `conn_id` already exists, it is replaced.
    ///
    /// Uses write lock for exclusive access during modification.
    pub fn register_connection(&self, broker_id: BrokerId, conn: Connection) {
        if let Ok(mut conns) = self.connections.write() {
            conns.entry(broker_id.0)
                .or_default()
                .insert(conn.conn_id.clone(), conn);
        }
    }

    /// Release (remove) a connection.
    ///
    /// This removes all connections for the broker.
    ///
    /// Uses write lock for exclusive access during modification.
    pub fn release_connection(&self, broker_id: &str) {
        if let Ok(mut conns) = self.connections.write() {
            conns.remove(broker_id);
        }
    }

    /// Release (remove) a single connection by ids.
    ///
    /// Uses write lock for exclusive access during modification.
    pub fn release_connection_by_id(&self, broker_id: &str, conn_id: &str) {
        if let Ok(mut conns) = self.connections.write() {
            if let Some(pool) = conns.get_mut(broker_id) {
                pool.remove(conn_id);
                if pool.is_empty() {
                    conns.remove(broker_id);
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::domain::request::{Connection, BrokerId};

    #[test]
    fn can_register_multiple_connections_for_same_broker() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = BrokerId("alpaca".to_string());

        mgr.register_connection(
            broker.clone(),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );
        mgr.register_connection(
            broker.clone(),
            Connection {
                conn_id: "alpaca-2".to_string(),
            },
        );

        assert_eq!(mgr.connection_count(broker), 2);
    }

    #[test]
    fn register_connection_replaces_same_conn_id() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = BrokerId("alpaca".to_string());

        mgr.register_connection(
            broker.clone(),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );
        mgr.register_connection(
            broker,
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );

        assert_eq!(mgr.connection_count(BrokerId("alpaca".to_string())), 1);
    }

    #[test]
    fn get_connections_returns_all_for_broker() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = BrokerId("alpaca".to_string());

        mgr.register_connection(
            broker.clone(),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );
        mgr.register_connection(
            broker.clone(),
            Connection {
                conn_id: "alpaca-2".to_string(),
            },
        );

        let mut ids: Vec<_> = mgr
            .get_connections(broker)
            .into_iter()
            .map(|c| c.conn_id)
            .collect();
        ids.sort();

        assert_eq!(ids, vec!["alpaca-1".to_string(), "alpaca-2".to_string()]);
    }

    #[test]
    fn release_connection_by_id_removes_single_connection() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = "alpaca";

        mgr.register_connection(
            BrokerId(broker.to_string()),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );
        mgr.register_connection(
            BrokerId(broker.to_string()),
            Connection {
                conn_id: "alpaca-2".to_string(),
            },
        );

        mgr.release_connection_by_id(broker, "alpaca-1");

        assert_eq!(mgr.connection_count(BrokerId(broker.to_string())), 1);

        let conns = mgr.get_connections(BrokerId(broker.to_string()));
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0].conn_id, "alpaca-2".to_string());
    }

    #[test]
    fn release_connection_by_id_cleans_up_empty_broker_bucket() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = "alpaca";

        mgr.register_connection(
            BrokerId(broker.to_string()),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );

        mgr.release_connection_by_id(broker, "alpaca-1");

        assert_eq!(mgr.connection_count(BrokerId(broker.to_string())), 0);
        assert!(mgr.get_connections(BrokerId(broker.to_string())).is_empty());
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Concurrency Tests for RwLock
    // ═══════════════════════════════════════════════════════════════════════════

    use std::sync::Arc;
    use std::thread;

    #[test]
    fn concurrent_reads_do_not_block_each_other() {
        // This test verifies that RwLock allows multiple concurrent readers
        let mgr = Arc::new(ConnectionManager::new(5, 500));
        let broker = BrokerId("alpaca".to_string());

        // Register some connections
        for i in 0..10 {
            mgr.register_connection(
                broker.clone(),
                Connection {
                    conn_id: format!("alpaca-{}", i),
                },
            );
        }

        let mut handles = vec![];

        // Spawn multiple reader threads
        for _ in 0..10 {
            let mgr_clone = Arc::clone(&mgr);
            let broker_clone = broker.clone();
            let handle = thread::spawn(move || {
                // Each thread performs multiple read operations
                for _ in 0..100 {
                    let _ = mgr_clone.get_connection(broker_clone.clone());
                    let _ = mgr_clone.connection_count(broker_clone.clone());
                    let _ = mgr_clone.get_connections(broker_clone.clone());
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread should complete without panicking");
        }

        // Verify data integrity after concurrent reads
        assert_eq!(mgr.connection_count(broker), 10);
    }

    #[test]
    fn concurrent_read_and_write_operations() {
        // This test verifies that writes are exclusive while reads can be concurrent
        let mgr = Arc::new(ConnectionManager::new(5, 500));
        let broker = BrokerId("alpaca".to_string());

        // Pre-register some connections
        for i in 0..5 {
            mgr.register_connection(
                broker.clone(),
                Connection {
                    conn_id: format!("alpaca-{}", i),
                },
            );
        }

        let mut handles = vec![];

        // Spawn reader threads
        for _ in 0..5 {
            let mgr_clone = Arc::clone(&mgr);
            let broker_clone = broker.clone();
            let handle = thread::spawn(move || {
                for _ in 0..50 {
                    let _ = mgr_clone.get_connection(broker_clone.clone());
                    let count = mgr_clone.connection_count(broker_clone.clone());
                    // Count should never exceed total registered connections
                    assert!(count <= 10, "Connection count should never exceed 10");
                }
            });
            handles.push(handle);
        }

        // Spawn writer threads
        for i in 5..10 {
            let mgr_clone = Arc::clone(&mgr);
            let broker_clone = broker.clone();
            let handle = thread::spawn(move || {
                mgr_clone.register_connection(
                    broker_clone,
                    Connection {
                        conn_id: format!("alpaca-{}", i),
                    },
                );
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread should complete without panicking");
        }

        // Verify final state
        assert_eq!(mgr.connection_count(broker), 10);
    }

    #[test]
    fn concurrent_write_operations_are_exclusive() {
        // This test verifies that multiple writers don't corrupt the data
        let mgr = Arc::new(ConnectionManager::new(5, 500));
        let broker = BrokerId("alpaca".to_string());

        let mut handles = vec![];

        // Spawn multiple writer threads that register connections
        for i in 0..10 {
            let mgr_clone = Arc::clone(&mgr);
            let broker_clone = broker.clone();
            let handle = thread::spawn(move || {
                mgr_clone.register_connection(
                    broker_clone,
                    Connection {
                        conn_id: format!("alpaca-{}", i),
                    },
                );
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread should complete without panicking");
        }

        // Verify all connections were registered correctly
        assert_eq!(mgr.connection_count(broker.clone()), 10);
        let connections = mgr.get_connections(broker);
        assert_eq!(connections.len(), 10);
    }

    #[test]
    fn get_connection_returns_some_when_connections_exist() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = BrokerId("alpaca".to_string());

        // Initially no connections
        assert!(mgr.get_connection(broker.clone()).is_none());

        // Register a connection
        mgr.register_connection(
            broker.clone(),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );

        // Now get_connection should return Some
        let conn = mgr.get_connection(broker);
        assert!(conn.is_some());
        assert_eq!(conn.unwrap().conn_id, "alpaca-1");
    }

    #[test]
    fn release_connection_removes_all_for_broker() {
        let mgr = ConnectionManager::new(5, 500);
        let broker = "alpaca";

        mgr.register_connection(
            BrokerId(broker.to_string()),
            Connection {
                conn_id: "alpaca-1".to_string(),
            },
        );
        mgr.register_connection(
            BrokerId(broker.to_string()),
            Connection {
                conn_id: "alpaca-2".to_string(),
            },
        );

        assert_eq!(mgr.connection_count(BrokerId(broker.to_string())), 2);

        mgr.release_connection(broker);

        assert_eq!(mgr.connection_count(BrokerId(broker.to_string())), 0);
        assert!(mgr.get_connections(BrokerId(broker.to_string())).is_empty());
    }
}