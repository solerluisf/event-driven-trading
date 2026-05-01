// config/broker_config.rs
//
// Per-broker configuration loaded from env vars with broker-specific prefixes.
// Example for broker named "alpaca":
//   ALPACA_ENDPOINT  (not needed — apca reads APCA_API_BASE_URL directly)
//   ALPACA_RPM       200

use std::env;

#[derive(Clone, Debug)]
pub struct BrokerEnvConfig {
    pub name: String,
    pub rate_limit_rpm: f64,
}

impl BrokerEnvConfig {
    pub fn from_env(broker_name: &str) -> Self {
        let prefix = broker_name.to_uppercase();
        Self {
            name: broker_name.to_string(),
            rate_limit_rpm: env::var(format!("{}_RPM", prefix))
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(200.0),
        }
    }
}