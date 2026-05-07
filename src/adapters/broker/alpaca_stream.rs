// src/adapters/broker/alpaca_stream.rs
//
// Connects to Alpaca's real-time market data WebSocket, authenticates,
// subscribes to the configured symbols, normalizes inbound events into
// MarketDataEvent, and forwards them to the MarketDataPublisher actor.
//
// Disconnect handling is wired to ConnectionManager::reconnect_with_backoff.
//
// WebSocket endpoint:
//   paper/live:  wss://stream.data.alpaca.markets/v2/{feed}
//   test feed:   wss://stream.data.alpaca.markets/v2/test
//
// Wire protocol (Alpaca sends arrays of message objects):
//   {"T":"success","msg":"connected"}
//   {"T":"success","msg":"authenticated"}
//   {"T":"subscription", ...}
//   {"T":"t", "S":"AAPL", "p":162.92, "s":3, "t":"..."}   <- trade
//   {"T":"q", "S":"AAPL", "bp":162.90, "ap":162.93, "t":"..."} <- quote
//   {"T":"b", "S":"AAPL", "o":162.0, "h":163.0, "l":161.5, "c":162.5, "v":4900, "t":"..."} <- bar

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::adapters::messaging::market_data_publisher::{
    MarketDataEvent, MarketDataEventType, MarketDataPublisher,
};
use crate::core::application::connection_manager::ConnectionManager;
use crate::core::domain::request::Connection;

// ── Raw wire types (Alpaca JSON) ──────────────────────────────────────────────

/// A single message frame as Alpaca sends it.
/// The "T" field determines the variant; we capture the rest as a raw Value
/// so we can handle each type independently.
#[derive(Debug, Deserialize)]
struct AlpacaMsg {
    #[serde(rename = "T")]
    msg_type: String,
    /// Symbol — present on trade / quote / bar frames
    #[serde(rename = "S")]
    symbol: Option<String>,
    /// Timestamp string — present on trade / quote / bar frames
    #[serde(rename = "t")]
    timestamp: Option<String>,
    /// "msg" field — present on success / error frames
    msg: Option<String>,
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Configuration for the Alpaca stream.
#[derive(Clone, Debug)]
pub struct AlpacaStreamConfig {
    /// Alpaca API key id  (reads APCA_API_KEY_ID from env if not set directly)
    pub api_key: String,
    /// Alpaca API secret  (reads APCA_API_SECRET_KEY from env if not set directly)
    pub api_secret: String,
    /// Feed source: "iex", "sip", or "test"
    pub feed: String,
    /// Symbols to subscribe to, e.g. ["AAPL", "SPY"]
    /// Use ["*"] to subscribe to all symbols (requires appropriate plan).
    pub symbols: Vec<String>,
}

impl AlpacaStreamConfig {
    /// Load from environment variables (same vars apca uses).
    pub fn from_env(feed: String, symbols: Vec<String>) -> Self {
        Self {
            api_key: std::env::var("APCA_API_KEY_ID")
                .expect("APCA_API_KEY_ID must be set"),
            api_secret: std::env::var("APCA_API_SECRET_KEY")
                .expect("APCA_API_SECRET_KEY must be set"),
            feed,
            symbols,
        }
    }
}

/// Spawn the market data stream task.
///
/// Returns a JoinHandle that resolves when the task exits (either cleanly or
/// after exhausting all reconnect attempts).
///
/// The task owns an `Arc<ConnectionManager>` so it can call
/// `reconnect_with_backoff` on stream disconnects.
pub fn spawn(
    config: AlpacaStreamConfig,
    publisher: MarketDataPublisher,
    connection_manager: Arc<ConnectionManager>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(stream_loop(config, publisher, connection_manager))
}

// ── Stream loop ───────────────────────────────────────────────────────────────

async fn stream_loop(
    config: AlpacaStreamConfig,
    publisher: MarketDataPublisher,
    connection_manager: Arc<ConnectionManager>,
) {
    let broker_id = "alpaca_stream";

    loop {
        let cfg = config.clone();
        let pub_clone = publisher.clone();

        let result = connection_manager
            .reconnect_with_backoff(broker_id, || {
                let cfg = cfg.clone();
                async move {
                    let url = format!(
                        "wss://stream.data.alpaca.markets/v2/{}",
                        cfg.feed
                    );
                    tracing::info!("alpaca_stream: connecting to {}", url);
                    connect_async(&url)
                        .await
                        .map(|(ws, _)| {
                            // We return a nominal Connection value; the real
                            // WS stream is managed inside run_stream below.
                            let _ = ws; // dropped — we reconnect inside run_stream
                            Connection { conn_id: broker_id.to_string() }
                        })
                        .map_err(|e| format!("WS connect failed: {}", e))
                }
            })
            .await;

        if result.is_err() {
            tracing::error!(
                "alpaca_stream: all reconnect attempts exhausted — stream is dead"
            );
            return;
        }

        // Actually run the stream. We reconnect the WS ourselves here rather
        // than inside the closure above, so we can hold the ws split across
        // the whole session.
        match run_stream(&config, pub_clone).await {
            Ok(()) => {
                tracing::info!("alpaca_stream: stream ended cleanly, reconnecting");
            }
            Err(e) => {
                tracing::warn!("alpaca_stream: stream error: {} — reconnecting", e);
            }
        }
    }
}

/// Connect once and drive the stream until it closes or errors.
async fn run_stream(
    config: &AlpacaStreamConfig,
    publisher: MarketDataPublisher,
) -> Result<(), String> {
    let url = format!(
        "wss://stream.data.alpaca.markets/v2/{}",
        config.feed
    );

    let (ws_stream, _) = connect_async(&url)
        .await
        .map_err(|e| format!("connect failed: {}", e))?;

    let (mut write, mut read) = ws_stream.split();

    // ── Authentication ────────────────────────────────────────────────────────
    let auth = json!({
        "action": "auth",
        "key": config.api_key,
        "secret": config.api_secret,
    });
    write
        .send(Message::Text(auth.to_string()))
        .await
        .map_err(|e| format!("auth send failed: {}", e))?;

    // Wait for authenticated confirmation before subscribing
    wait_for_auth(&mut read).await?;

    // ── Subscription ──────────────────────────────────────────────────────────
    let subscribe_msg = json!({
        "action": "subscribe",
        "trades": config.symbols,
        "quotes": config.symbols,
        "bars":   config.symbols,
    });
    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .map_err(|e| format!("subscribe send failed: {}", e))?;

    tracing::info!(
        "alpaca_stream: subscribed to symbols={:?} feed={}",
        config.symbols,
        config.feed
    );

    // ── Message loop ──────────────────────────────────────────────────────────
    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_message(&text, &publisher).await {
                    tracing::warn!("alpaca_stream: message handling error: {}", e);
                }
            }
            Ok(Message::Ping(data)) => {
                // Respond to server pings to keep the connection alive
                if let Err(e) = write.send(Message::Pong(data)).await {
                    return Err(format!("pong failed: {}", e));
                }
            }
            Ok(Message::Close(frame)) => {
                tracing::info!("alpaca_stream: server closed connection: {:?}", frame);
                return Ok(());
            }
            Ok(_) => {} // Binary or other frames — ignore
            Err(e) => {
                return Err(format!("stream read error: {}", e));
            }
        }
    }

    Ok(())
}

/// Wait for the `{"T":"success","msg":"authenticated"}` frame.
/// Times out after 15 seconds.
async fn wait_for_auth<S>(read: &mut S) -> Result<(), String>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let timeout = tokio::time::Duration::from_secs(15);
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or("auth timeout")?;

        let msg = tokio::time::timeout(remaining, read.next())
            .await
            .map_err(|_| "auth timeout")?
            .ok_or("stream ended before auth")?
            .map_err(|e| format!("stream error during auth: {}", e))?;

        if let Message::Text(text) = msg {
            // Alpaca wraps messages in arrays: [{"T":...}, ...]
            let frames: Vec<Value> = serde_json::from_str(&text)
                .unwrap_or_default();

            for frame in frames {
                let t = frame.get("T").and_then(|v| v.as_str()).unwrap_or("");
                let m = frame.get("msg").and_then(|v| v.as_str()).unwrap_or("");

                if t == "success" && m == "authenticated" {
                    tracing::info!("alpaca_stream: authenticated");
                    return Ok(());
                }
                if t == "error" {
                    let code = frame.get("code").and_then(|v| v.as_i64()).unwrap_or(0);
                    return Err(format!("auth error code={} msg={}", code, m));
                }
            }
        }
    }
}

// ── Message normalizer ────────────────────────────────────────────────────────

/// Parse a raw text frame from Alpaca and publish any trade/quote/bar events.
async fn handle_message(text: &str, publisher: &MarketDataPublisher) -> Result<(), String> {
    // Alpaca wraps all messages in a JSON array
    let frames: Vec<Value> = serde_json::from_str(text)
        .map_err(|e| format!("JSON parse error: {} — raw: {}", e, text))?;

    for frame in frames {
        let msg_type = match frame.get("T").and_then(|v| v.as_str()) {
            Some(t) => t.to_string(),
            None => continue,
        };

        match msg_type.as_str() {
            "t" => {
                // Trade
                if let Some(event) = normalize_trade(&frame) {
                    if let Err(e) = publisher.publish(event).await {
                        tracing::warn!("alpaca_stream: publish failed: {}", e);
                    }
                }
            }
            "q" => {
                // Quote
                if let Some(event) = normalize_quote(&frame) {
                    if let Err(e) = publisher.publish(event).await {
                        tracing::warn!("alpaca_stream: publish failed: {}", e);
                    }
                }
            }
            "b" => {
                // Bar
                if let Some(event) = normalize_bar(&frame) {
                    if let Err(e) = publisher.publish(event).await {
                        tracing::warn!("alpaca_stream: publish failed: {}", e);
                    }
                }
            }
            "subscription" => {
                tracing::info!("alpaca_stream: subscription confirmed: {}", frame);
            }
            "success" | "error" => {
                let m = frame.get("msg").and_then(|v| v.as_str()).unwrap_or("");
                if msg_type == "error" {
                    tracing::error!("alpaca_stream: server error: {}", m);
                } else {
                    tracing::debug!("alpaca_stream: server success: {}", m);
                }
            }
            other => {
                tracing::debug!("alpaca_stream: unhandled message type: {}", other);
            }
        }
    }

    Ok(())
}

// ── Per-type normalizers ──────────────────────────────────────────────────────

fn normalize_trade(frame: &Value) -> Option<MarketDataEvent> {
    let symbol = frame.get("S")?.as_str()?.to_string();
    let timestamp = frame
        .get("t")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Build a clean payload: price, size, exchange, conditions
    let payload = json!({
        "price":      frame.get("p"),
        "size":       frame.get("s"),
        "exchange":   frame.get("x"),
        "conditions": frame.get("c"),
        "tape":       frame.get("z"),
        "trade_id":   frame.get("i"),
    });

    Some(MarketDataEvent {
        symbol,
        event_type: MarketDataEventType::Trade,
        timestamp,
        payload,
    })
}

fn normalize_quote(frame: &Value) -> Option<MarketDataEvent> {
    let symbol = frame.get("S")?.as_str()?.to_string();
    let timestamp = frame
        .get("t")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let payload = json!({
        "bid_price":    frame.get("bp"),
        "bid_size":     frame.get("bs"),
        "bid_exchange": frame.get("bx"),
        "ask_price":    frame.get("ap"),
        "ask_size":     frame.get("as"),
        "ask_exchange": frame.get("ax"),
        "conditions":   frame.get("c"),
        "tape":         frame.get("z"),
    });

    Some(MarketDataEvent {
        symbol,
        event_type: MarketDataEventType::BookUpdate,
        timestamp,
        payload,
    })
}

fn normalize_bar(frame: &Value) -> Option<MarketDataEvent> {
    let symbol = frame.get("S")?.as_str()?.to_string();
    let timestamp = frame
        .get("t")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let payload = json!({
        "open":   frame.get("o"),
        "high":   frame.get("h"),
        "low":    frame.get("l"),
        "close":  frame.get("c"),
        "volume": frame.get("v"),
        "vwap":   frame.get("vw"),
        "trades": frame.get("n"),
    });

    Some(MarketDataEvent {
        symbol,
        event_type: MarketDataEventType::Bar,
        timestamp,
        payload,
    })
}

/// Test-only re-export of the private `handle_message` function.
#[cfg(test)]
pub async fn handle_message_test(
    text: &str,
    publisher: &crate::adapters::messaging::market_data_publisher::MarketDataPublisher,
) -> Result<(), String> {
    handle_message(text, publisher).await
}