// adapters/metrics/health_endpoint.rs

use crate::core::domain::request::Health;

pub struct HealthEndpoint;

impl HealthEndpoint {
    pub fn health_status(&self) -> Health {
        Health { ok: true }
    }
}