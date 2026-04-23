// health_endpoint.rs

pub struct HealthEndpoint;

impl HealthEndpoint {
    pub fn health_status(&self) -> Health {
        Health { ok: true }
    }
}