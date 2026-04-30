// connection_manager.rs
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use crate::core::domain::request::{Connection, BrokerId};


pub struct ConnectionManager {
    pools: Mutex<HashMap<String, VecDeque<Connection>>>,
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self {
            pools: Mutex::new(HashMap::new()),
        }
    }
}

impl ConnectionManager {
    pub fn get_connection(&self, broker_id: BrokerId) -> Option<Connection> {
        self.pools
            .lock()
            .ok()
            .and_then(|mut pools| pools.get_mut(&broker_id.0).and_then(|q| q.pop_front()))
    }

    pub fn release_connection(&self, conn: Connection) {
        let _ = conn;
    }

    pub fn reconnect_if_needed(&self, _conn: &Connection) {}
}