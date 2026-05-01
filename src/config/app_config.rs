// config/app_config.rs
//
// All gateway configuration loaded from environment variables.
// dotenvy already loads .env before this is called from main.rs.
//
// Variables and defaults:
//   GATEWAY_ZMQ_REP_ENDPOINT   tcp://127.0.0.1:5555   inbound commands (REP)
//   GATEWAY_ZMQ_PUB_ENDPOINT   tcp://127.0.0.1:5556   outbound market data (PUB)
//   GATEWAY_BROKER             alpaca
//   GATEWAY_RATE_LIMIT_RPM     200                    requests per minute
//   GATEWAY_CB_THRESHOLD       3                      circuit breaker failure threshold
//   GATEWAY_CB_COOLDOWN_SECS   30                     circuit breaker cooldown
//   GATEWAY_RECONNECT_MAX      5                      max reconnect attempts
//   GATEWAY_RECONNECT_BASE_MS  500                    initial back-off in ms
//   JOURNAL_DB_PATH            journal.db
//   MARKET_DATA_FEED           iex                    "iex", "sip", or "test"
//   MARKET_DATA_SYMBOLS        AAPL,SPY               comma-separated list (or "*")

use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    /// ZeroMQ REP endpoint — receives execution commands
    pub zmq_rep_endpoint: String,
    /// ZeroMQ PUB endpoint — publishes market data
    pub zmq_pub_endpoint: String,
    /// Broker adapter to use
    pub broker: String,
    /// Rate limit in requests per minute
    pub rate_limit_rpm: f64,
    /// Circuit breaker: consecutive failures before opening
    pub cb_failure_threshold: u32,
    /// Circuit breaker: seconds to stay open before probing
    pub cb_cooldown_secs: u64,
    /// Reconnection: maximum attempts before giving up
    pub reconnect_max_attempts: u32,
    /// Reconnection: initial back-off delay in milliseconds
    pub reconnect_base_ms: u64,
    /// SQLite journal file path
    pub journal_db_path: String,
    /// Alpaca market data feed: "iex", "sip", or "test"
    pub market_data_feed: String,
    /// Symbols to stream — use ["*"] for all (requires appropriate plan)
    pub market_data_symbols: Vec<String>,
}

impl AppConfig {
    /// Load config from environment.  Panics on parse errors for numeric fields.
    pub fn from_env() -> Self {
        Self {
            zmq_rep_endpoint: env_str("GATEWAY_ZMQ_REP_ENDPOINT", "tcp://127.0.0.1:5555"),
            zmq_pub_endpoint: env_str("GATEWAY_ZMQ_PUB_ENDPOINT", "tcp://127.0.0.1:5556"),
            broker: env_str("GATEWAY_BROKER", "alpaca"),
            rate_limit_rpm: env_parse("GATEWAY_RATE_LIMIT_RPM", 200.0),
            cb_failure_threshold: env_parse("GATEWAY_CB_THRESHOLD", 3),
            cb_cooldown_secs: env_parse("GATEWAY_CB_COOLDOWN_SECS", 30),
            reconnect_max_attempts: env_parse("GATEWAY_RECONNECT_MAX", 5),
            reconnect_base_ms: env_parse("GATEWAY_RECONNECT_BASE_MS", 500),
            journal_db_path: env_str("JOURNAL_DB_PATH", "journal.db"),
            market_data_feed: env_str("MARKET_DATA_FEED", "iex"),
            market_data_symbols: env_symbol_list("MARKET_DATA_SYMBOLS", &["AAPL", "SPY"]),
        }
    }
}

fn env_str(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr + Copy,
    T::Err: std::fmt::Debug,
{
    match env::var(key) {
        Ok(val) => val.parse::<T>().unwrap_or_else(|e| {
            panic!("Invalid value for {}: {:?}", key, e)
        }),
        Err(_) => default,
    }
}

/// Parse a comma-separated symbol list.  Falls back to `defaults` if the env
/// var is absent.
fn env_symbol_list(key: &str, defaults: &[&str]) -> Vec<String> {
    match env::var(key) {
        Ok(val) if !val.trim().is_empty() => {
            val.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
        _ => defaults.iter().map(|s| s.to_string()).collect(),
    }
}