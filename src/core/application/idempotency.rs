// idempotency.rs

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct IdempotencyStore {
    processed: Mutex<HashMap<String, String>>,
}

impl IdempotencyStore {
    pub fn is_processed(&self, key: &str) -> bool {
        self.processed.lock().unwrap().contains_key(key)
    }

    pub fn mark_processed(&self, key: String, result: String) {
        self.processed.lock().unwrap().insert(key, result);
    }
}