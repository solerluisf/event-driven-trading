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
//
// IMPORTANT LIMITATION - Sequence Numbers:
//   Alpaca's WebSocket API does NOT provide sequence numbers in market data
//   messages. This means we cannot detect message-level gaps at the wire.
//   The sequence numbers in ReactorEvent are assigned internally AFTER
//   ingestion and can only detect gaps in our own processing pipeline.
//
//   Gap detection mechanisms we implement:
//   1. Per-symbol internal sequence tracking (best effort)
//   2. Trade ID gap detection for trade messages (trade_id field analysis)
//   3. Time-based gap detection (>10s between messages)
//   4. Silent disconnect detection (>60s no messages)
//
//   If Alpaca drops a message before it reaches our WebSocket connection,
//   there is no way to detect this gap. This is an architectural limitation
//   of Alpaca's streaming API, not a bug in this code.

use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::adapters::broker::broker_error::BrokerError;
use crate::adapters::messaging::market_data_publisher::{
    MarketDataEvent, MarketDataEventType, MarketDataPublisher,
};
use crate::core::application::connection_manager::ConnectionManager;
use crate::core::domain::market_data::MarketDataCommand;
use crate::core::domain::request::Connection;
use crate::core::ports::service_traits::IObservabilityService;

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

// ── Sequence Tracking ─────────────────────────────────────────────────────────

/// Tracks sequence numbers and trade IDs per symbol for best-effort gap detection.
/// 
/// IMPORTANT: This provides INTERNAL sequence tracking only. Alpaca does not
/// provide wire-level sequence numbers, so we cannot detect gaps that occur
/// before messages reach our WebSocket connection.
#[derive(Debug)]
struct SymbolSequenceTracker {
    /// Per-symbol internal sequence counter (for ReactorEvent.seq_no assignment)
    sequences: std::collections::HashMap<String, u64>,
    /// Per-symbol last seen trade ID (for trade gap detection)
    last_trade_ids: std::collections::HashMap<String, i64>,
    /// Gap detection threshold (alert if trade ID gap > this)
    trade_id_gap_threshold: i64,
}

impl SymbolSequenceTracker {
    fn new() -> Self {
        Self {
            sequences: std::collections::HashMap::new(),
            last_trade_ids: std::collections::HashMap::new(),
            trade_id_gap_threshold: 100, // Alert if trade IDs differ by >100
        }
    }

    /// Get the next sequence number for a symbol and increment the counter.
    fn next_seq(&mut self, symbol: &str) -> u64 {
        let counter = self.sequences.entry(symbol.to_string()).or_insert(0);
        *counter += 1;
        *counter
    }

    /// Check for trade ID gaps. Returns true if a significant gap is detected.
    /// Trade IDs are not guaranteed to be sequential, but large gaps may indicate dropped messages.
    fn check_trade_gap(&mut self, symbol: &str, trade_id: i64) -> bool {
        let gap_detected = if let Some(last_id) = self.last_trade_ids.get(symbol) {
            let diff = (trade_id - *last_id).abs();
            diff > self.trade_id_gap_threshold
        } else {
            false
        };
        
        self.last_trade_ids.insert(symbol.to_string(), trade_id);
        gap_detected
    }

    /// Get statistics for monitoring.
    fn get_stats(&self) -> (usize, usize) {
        (self.sequences.len(), self.last_trade_ids.len())
    }
}

/// Information about a detected gap.
#[derive(Debug, Clone)]
struct GapInfo {
    symbol: String,
    gap_type: GapType,
    last_value: i64,
    current_value: i64,
    gap_size: i64,
}

#[derive(Debug, Clone)]
enum GapType {
    TradeIdGap,
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
    /// 
    /// Returns `BrokerError::ConfigError` if required environment variables are not set.
    pub fn from_env(feed: String, symbols: Vec<String>) -> Result<Self, BrokerError> {
        let api_key = std::env::var("APCA_API_KEY_ID")
            .map_err(|_| BrokerError::ConfigError(
                "APCA_API_KEY_ID environment variable must be set".to_string()
            ))?;
        let api_secret = std::env::var("APCA_API_SECRET_KEY")
            .map_err(|_| BrokerError::ConfigError(
                "APCA_API_SECRET_KEY environment variable must be set".to_string()
            ))?;
        
        Ok(Self {
            api_key,
            api_secret,
            feed,
            symbols,
        })
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
    reactor_tx: Sender<MarketDataEvent>,
    connection_manager: Arc<ConnectionManager>,
    stream_command_rx: Receiver<MarketDataCommand>,
    observability: Arc<dyn IObservabilityService>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(stream_loop(config, publisher, reactor_tx, connection_manager, stream_command_rx, observability))
}

// ── Stream loop ───────────────────────────────────────────────────────────────

async fn stream_loop(
    config: AlpacaStreamConfig,
    publisher: MarketDataPublisher,
    reactor_tx: Sender<MarketDataEvent>,
    connection_manager: Arc<ConnectionManager>,
    mut stream_command_rx: Receiver<MarketDataCommand>,
    observability: Arc<dyn IObservabilityService>,
) {
    let broker_id = "alpaca_stream";
    // Stable per stream task, so ConnectionManager can track multiple
    // concurrent WS connections to the same broker.
    let stream_conn_id = format!("{}-{}", broker_id, Uuid::new_v4());

    loop {
        let cfg = config.clone();
        let pub_clone = publisher.clone();

        let result = connection_manager
            .reconnect_with_backoff(broker_id, || {
                let cfg = cfg.clone();
                let stream_conn_id = stream_conn_id.clone();
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
                            Connection {
                                conn_id: stream_conn_id,
                            }
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
        match run_stream(&config, pub_clone, &reactor_tx, &mut stream_command_rx, &observability).await {
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
    reactor_tx: &Sender<MarketDataEvent>,
    stream_command_rx: &mut Receiver<MarketDataCommand>,
    observability: &Arc<dyn IObservabilityService>,
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

    // ── Initial subscription ──────────────────────────────────────────────────
    let mut subscribed_symbols: HashSet<String> = config.symbols.iter().cloned().collect();
    if !subscribed_symbols.is_empty() {
        let symbols: Vec<String> = subscribed_symbols.iter().cloned().collect();
        send_subscription(&mut write, "subscribe", &symbols).await?;
        tracing::info!(
            "alpaca_stream: subscribed to symbols={:?} feed={}",
            subscribed_symbols,
            config.feed
        );
    } else {
        tracing::info!("alpaca_stream: started with no initial market data subscriptions");
    }

    // ── Heartbeat and watchdog setup ──────────────────────────────────────────
    let mut heartbeat = interval(Duration::from_secs(30)); // Send ping every 30s
    let mut watchdog = interval(Duration::from_secs(10)); // Check every 10s
    let mut last_message_time = Instant::now();
    let mut last_event_time = Instant::now();

    // ── Sequence tracking for gap detection ────────────────────────────────────
    // IMPORTANT: This provides best-effort gap detection only. Alpaca does not
    // provide wire-level sequence numbers, so we cannot detect gaps that occur
    // before messages reach our WebSocket connection.
    let mut seq_tracker = SymbolSequenceTracker::new();
    let mut seq_report_counter = 0u64;

    // ── Message loop ──────────────────────────────────────────────────────────
    loop {
        tokio::select! {
            command = stream_command_rx.recv() => {
                match command {
                    Some(cmd) => {
                        handle_stream_command(&mut write, &mut subscribed_symbols, cmd).await?;
                    }
                    None => {
                        tracing::info!("alpaca_stream: stream command channel closed");
                        return Ok(());
                    }
                }
            }
            msg_result = read.next() => {
                match msg_result {
                    Some(Ok(Message::Text(text))) => {
                        last_message_time = Instant::now();
                        if let Err(e) = handle_message(&text, &publisher, Some(reactor_tx), observability, &mut last_event_time, &mut seq_tracker).await {
                            tracing::warn!("alpaca_stream: message handling error: {}", e);
                        }
                        
                        // Periodically log sequence tracking stats
                        seq_report_counter += 1;
                        if seq_report_counter % 10000 == 0 {
                            let (symbol_count, trade_id_count) = seq_tracker.get_stats();
                            tracing::debug!(
                                "alpaca_stream: sequence tracker stats - symbols tracked: {}, trade IDs tracked: {}",
                                symbol_count, trade_id_count
                            );
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        last_message_time = Instant::now();
                        // Respond to server pings to keep the connection alive
                        if let Err(e) = write.send(Message::Pong(data)).await {
                            return Err(format!("pong failed: {}", e));
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!("alpaca_stream: server closed connection: {:?}", frame);
                        return Ok(());
                    }
                    Some(Ok(_)) => {} // Binary or other frames — ignore
                    Some(Err(e)) => {
                        return Err(format!("stream read error: {}", e));
                    }
                    None => {
                        tracing::info!("alpaca_stream: websocket stream ended");
                        return Ok(());
                    }
                }
            }
            _ = heartbeat.tick() => {
                // Send a ping to keep the connection alive
                if let Err(e) = write.send(Message::Ping(vec![])).await {
                    return Err(format!("heartbeat ping failed: {}", e));
                }
                tracing::debug!("alpaca_stream: sent heartbeat ping");
            }
            _ = watchdog.tick() => {
                let now = Instant::now();
                let silent_duration = now.duration_since(last_message_time);
                if silent_duration > Duration::from_secs(60) {
                    observability.emit_event(format!("alpaca_stream: silent disconnect detected ({}s since last message)", silent_duration.as_secs()));
                    return Err(format!("silent disconnect: no messages for {}s", silent_duration.as_secs()));
                }
            }
        }
    }

    // unreachable
}

async fn handle_stream_command<W>(
    write: &mut W,
    subscribed_symbols: &mut HashSet<String>,
    command: MarketDataCommand,
) -> Result<(), String>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match command {
        MarketDataCommand::Subscribe(subscription) => {
            let symbol = subscription.symbol.clone();
            subscribed_symbols.insert(symbol.clone());
            send_subscription(write, "subscribe", &[symbol]).await
        }
        MarketDataCommand::Unsubscribe(subscription) => {
            let symbol = subscription.symbol.clone();
            subscribed_symbols.remove(&symbol);
            send_subscription(write, "unsubscribe", &[symbol]).await
        }
    }
}

async fn send_subscription<W>(
    write: &mut W,
    action: &str,
    symbols: &[String],
) -> Result<(), String>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    if symbols.is_empty() {
        return Ok(());
    }

    let subscribe_msg = json!({
        "action": action,
        "trades": symbols,
        "quotes": symbols,
        "bars": symbols,
    });

    write
        .send(Message::Text(subscribe_msg.to_string()))
        .await
        .map_err(|e| format!("{} send failed: {}", action, e))
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
/// 
/// IMPORTANT: This function implements best-effort gap detection using:
/// - Per-symbol internal sequence tracking (not wire-level)
/// - Trade ID gap detection for trade messages
/// - Time-based gap detection
/// 
/// Since Alpaca does not provide wire-level sequence numbers, we cannot detect
/// gaps that occur before messages reach our WebSocket connection.
async fn handle_message(
    text: &str,
    publisher: &MarketDataPublisher,
    reactor_tx: Option<&Sender<MarketDataEvent>>,
    observability: &Arc<dyn IObservabilityService>,
    last_event_time: &mut Instant,
    seq_tracker: &mut SymbolSequenceTracker,
) -> Result<(), String> {
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
                let symbol = frame.get("S").and_then(|v| v.as_str()).unwrap_or("unknown");
                let trade_id = frame.get("i").and_then(|v| v.as_i64()).unwrap_or(0);
                
                // Check for trade ID gaps (best effort - trade IDs are not guaranteed sequential)
                if trade_id > 0 && seq_tracker.check_trade_gap(symbol, trade_id) {
                    let last_id = seq_tracker.last_trade_ids.get(symbol).copied().unwrap_or(0);
                    observability.emit_event(format!(
                        "alpaca_stream: trade_id gap detected for {}: last={}, current={}, diff={}",
                        symbol,
                        last_id,
                        trade_id,
                        (trade_id - last_id).abs()
                    ));
                }
                
                // Get internal sequence number for this symbol
                let _seq = seq_tracker.next_seq(symbol);
                
                if let Some(event) = normalize_trade(&frame) {
                    let now = Instant::now();
                    let gap = now.duration_since(*last_event_time);
                    if gap > Duration::from_secs(10) {
                        observability.emit_event(format!(
                            "alpaca_stream: time gap detected: {}s since last event (symbol={})",
                            gap.as_secs(),
                            symbol
                        ));
                    }
                    *last_event_time = now;

                    if let Err(e) = publisher.publish(event.clone()).await {
                        tracing::warn!("alpaca_stream: publish failed: {}", e);
                    }
                    if let Some(tx) = reactor_tx {
                        if let Err(err) = tx.try_send(event) {
                            tracing::warn!("alpaca_stream: reactor queue full or closed: {}", err);
                        }
                    }
                }
            }
            "q" => {
                // Quote
                let symbol = frame.get("S").and_then(|v| v.as_str()).unwrap_or("unknown");
                let _seq = seq_tracker.next_seq(symbol); // Track sequence for quotes too
                
                if let Some(event) = normalize_quote(&frame) {
                    let now = Instant::now();
                    let gap = now.duration_since(*last_event_time);
                    if gap > Duration::from_secs(10) {
                        observability.emit_event(format!(
                            "alpaca_stream: time gap detected: {}s since last event (symbol={})",
                            gap.as_secs(),
                            symbol
                        ));
                    }
                    *last_event_time = now;

                    if let Err(e) = publisher.publish(event.clone()).await {
                        tracing::warn!("alpaca_stream: publish failed: {}", e);
                    }
                    if let Some(tx) = reactor_tx {
                        if let Err(err) = tx.try_send(event) {
                            tracing::warn!("alpaca_stream: reactor queue full or closed: {}", err);
                        }
                    }
                }
            }
            "b" => {
                // Bar
                let symbol = frame.get("S").and_then(|v| v.as_str()).unwrap_or("unknown");
                let _seq = seq_tracker.next_seq(symbol); // Track sequence for bars too
                
                if let Some(event) = normalize_bar(&frame) {
                    let now = Instant::now();
                    let gap = now.duration_since(*last_event_time);
                    if gap > Duration::from_secs(10) {
                        observability.emit_event(format!(
                            "alpaca_stream: time gap detected: {}s since last event (symbol={})",
                            gap.as_secs(),
                            symbol
                        ));
                    }
                    *last_event_time = now;

                    if let Err(e) = publisher.publish(event.clone()).await {
                        tracing::warn!("alpaca_stream: publish failed: {}", e);
                    }
                    if let Some(tx) = reactor_tx {
                        if let Err(err) = tx.try_send(event) {
                            tracing::warn!("alpaca_stream: reactor queue full or closed: {}", err);
                        }
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
        source: "alpaca".to_string(),
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
        source: "alpaca".to_string(),
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
        source: "alpaca".to_string(),
        payload,
    })
}

/// Test-only re-export of the private `handle_message` function.
#[cfg(test)]
pub async fn handle_message_test(
    text: &str,
    publisher: &crate::adapters::messaging::market_data_publisher::MarketDataPublisher,
) -> Result<(), String> {
    use std::sync::Arc;
    use tokio::time::Instant;
    use crate::core::ports::service_traits::IObservabilityService;
    use crate::core::domain::journal::{RequestRecord, ResponseRecord};

    struct NoopObs;
    impl IObservabilityService for NoopObs {
        fn record_outbound(&self, _record: RequestRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn record_inbound(&self, _record: ResponseRecord) -> crate::core::ports::journal_repo::JournalResult<()> {
            Ok(())
        }
        fn emit_event(&self, _event: String) {}
    }

    let obs: Arc<dyn IObservabilityService> = Arc::new(NoopObs);
    let mut last_event_time = Instant::now();
    let mut seq_tracker = SymbolSequenceTracker::new();
    handle_message(text, publisher, None, &obs, &mut last_event_time, &mut seq_tracker).await
}

// ═══════════════════════════════════════════════════════════════════════════
// Sequence Tracking Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod sequence_tracker_tests {
    use super::*;

    #[test]
    fn sequence_tracker_starts_at_zero() {
        let tracker = SymbolSequenceTracker::new();
        let (symbols, trade_ids) = tracker.get_stats();
        assert_eq!(symbols, 0);
        assert_eq!(trade_ids, 0);
    }

    #[test]
    fn sequence_tracker_increments_per_symbol() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // First sequence for AAPL should be 1
        let seq1 = tracker.next_seq("AAPL");
        assert_eq!(seq1, 1);
        
        // Second sequence for AAPL should be 2
        let seq2 = tracker.next_seq("AAPL");
        assert_eq!(seq2, 2);
        
        // First sequence for TSLA should be 1 (independent counter)
        let seq3 = tracker.next_seq("TSLA");
        assert_eq!(seq3, 1);
        
        // Third sequence for AAPL should be 3
        let seq4 = tracker.next_seq("AAPL");
        assert_eq!(seq4, 3);
    }

    #[test]
    fn sequence_tracker_tracks_multiple_symbols() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // Track sequences for multiple symbols
        for symbol in &["AAPL", "TSLA", "GOOGL", "MSFT"] {
            for _ in 0..10 {
                let _ = tracker.next_seq(symbol);
            }
        }
        
        let (symbols, _) = tracker.get_stats();
        assert_eq!(symbols, 4);
    }

    #[test]
    fn trade_id_gap_detection_no_gap_on_first_trade() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // First trade for a symbol should never trigger gap detection
        let gap_detected = tracker.check_trade_gap("AAPL", 1000);
        assert!(!gap_detected);
    }

    #[test]
    fn trade_id_gap_detection_no_gap_within_threshold() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // First trade
        let _ = tracker.check_trade_gap("AAPL", 1000);
        
        // Second trade within threshold (gap of 50, threshold is 100)
        let gap_detected = tracker.check_trade_gap("AAPL", 1050);
        assert!(!gap_detected);
    }

    #[test]
    fn trade_id_gap_detection_triggers_on_large_gap() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // First trade
        let _ = tracker.check_trade_gap("AAPL", 1000);
        
        // Second trade with gap > 100 (threshold)
        let gap_detected = tracker.check_trade_gap("AAPL", 1200);
        assert!(gap_detected);
    }

    #[test]
    fn trade_id_gap_detection_per_symbol() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // Set up trade IDs for AAPL
        let _ = tracker.check_trade_gap("AAPL", 1000);
        let _ = tracker.check_trade_gap("AAPL", 1200); // Gap detected
        
        // TSLA should be independent - no gap on first trade
        let gap_detected = tracker.check_trade_gap("TSLA", 5000);
        assert!(!gap_detected);
        
        // AAPL next trade should not detect gap from TSLA's value
        let gap_detected = tracker.check_trade_gap("AAPL", 1205);
        assert!(!gap_detected);
    }

    #[test]
    fn trade_id_gap_detection_handles_decreasing_ids() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // First trade with high ID
        let _ = tracker.check_trade_gap("AAPL", 2000);
        
        // Second trade with lower ID (out of order)
        let gap_detected = tracker.check_trade_gap("AAPL", 1000);
        // Should detect gap because abs(1000 - 2000) = 1000 > 100
        assert!(gap_detected);
    }

    #[test]
    fn sequence_tracker_handles_zero_trade_id() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // Trade ID of 0 should not trigger gap detection (likely missing field)
        let gap_detected = tracker.check_trade_gap("AAPL", 0);
        assert!(!gap_detected);
        
        // Next trade with real ID
        let gap_detected = tracker.check_trade_gap("AAPL", 100);
        // Even though gap from 0 to 100 is 100, it should not trigger
        // because we don't trigger on first trade
        assert!(!gap_detected);
    }

    #[test]
    fn sequence_tracker_stats_accuracy() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // Initially empty
        let (symbols, trade_ids) = tracker.get_stats();
        assert_eq!(symbols, 0);
        assert_eq!(trade_ids, 0);
        
        // Add sequences for 3 symbols
        let _ = tracker.next_seq("AAPL");
        let _ = tracker.next_seq("TSLA");
        let _ = tracker.next_seq("GOOGL");
        
        // Add trade IDs for 2 symbols
        let _ = tracker.check_trade_gap("AAPL", 1000);
        let _ = tracker.check_trade_gap("TSLA", 2000);
        
        let (symbols, trade_ids) = tracker.get_stats();
        assert_eq!(symbols, 3);
        assert_eq!(trade_ids, 2);
    }

    #[test]
    fn sequence_tracker_handles_empty_symbol() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // Should handle empty string symbol
        let seq = tracker.next_seq("");
        assert_eq!(seq, 1);
        
        let gap_detected = tracker.check_trade_gap("", 100);
        assert!(!gap_detected);
    }

    #[test]
    fn sequence_tracker_handles_many_sequences() {
        let mut tracker = SymbolSequenceTracker::new();
        
        // Generate many sequences for a single symbol
        for i in 1..=1000 {
            let seq = tracker.next_seq("AAPL");
            assert_eq!(seq, i);
        }
        
        let (symbols, _) = tracker.get_stats();
        assert_eq!(symbols, 1);
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Alpaca API Limitation Documentation Tests
    // ═══════════════════════════════════════════════════════════════════════════

    /// This test documents the limitation that Alpaca does not provide
    /// wire-level sequence numbers. We can only detect gaps in our
    /// internal processing pipeline, not gaps at the wire level.
    #[test]
    fn documented_limitation_no_wire_sequence_numbers() {
        // Alpaca trade message format (from documentation):
        // {
        //   "T": "t",
        //   "i": 96921,     <- trade_id (not a sequence number)
        //   "S": "AAPL",
        //   "x": "D",
        //   "p": 126.55,
        //   "s": 1,
        //   "t": "2021-02-22T15:51:44.208Z",
        //   "c": ["@", "I"],
        //   "z": "C"
        // }
        //
        // Notice: NO sequence number field!
        // The "i" field is trade_id, not a sequence number.
        //
        // This means if Alpaca drops a message before it reaches us,
        // we have NO WAY to detect that gap at the wire level.
        
        let tracker = SymbolSequenceTracker::new();
        let (symbols, trade_ids) = tracker.get_stats();
        
        // This assertion is just to make the test pass - the real
        // purpose is the documentation above explaining the limitation.
        assert_eq!(symbols, 0);
        assert_eq!(trade_ids, 0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// AlpacaStreamConfig Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn from_env_success_with_valid_env_vars() {
        // Set up environment variables
        unsafe {
            std::env::set_var("APCA_API_KEY_ID", "test_key_id");
            std::env::set_var("APCA_API_SECRET_KEY", "test_secret_key");
        }

        let result = AlpacaStreamConfig::from_env(
            "iex".to_string(),
            vec!["AAPL".to_string(), "TSLA".to_string()],
        );

        // Clean up environment variables immediately after use
        unsafe {
            std::env::remove_var("APCA_API_KEY_ID");
            std::env::remove_var("APCA_API_SECRET_KEY");
        }

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.api_key, "test_key_id");
        assert_eq!(config.api_secret, "test_secret_key");
        assert_eq!(config.feed, "iex");
        assert_eq!(config.symbols, vec!["AAPL", "TSLA"]);
    }

    #[test]
    fn from_env_fails_when_api_key_missing() {
        // Ensure environment variables are not set
        unsafe {
            std::env::remove_var("APCA_API_KEY_ID");
            std::env::set_var("APCA_API_SECRET_KEY", "test_secret_key");
        }

        let result = AlpacaStreamConfig::from_env(
            "iex".to_string(),
            vec!["AAPL".to_string()],
        );

        // Clean up
        unsafe {
            std::env::remove_var("APCA_API_SECRET_KEY");
        }

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = format!("{}", err);
        assert!(err_msg.contains("APCA_API_KEY_ID"));
        assert!(matches!(err, BrokerError::ConfigError(_)));
    }

    #[test]
    fn from_env_fails_when_secret_key_missing() {
        // Set only API key
        unsafe {
            std::env::set_var("APCA_API_KEY_ID", "test_key_id");
            std::env::remove_var("APCA_API_SECRET_KEY");
        }

        let result = AlpacaStreamConfig::from_env(
            "iex".to_string(),
            vec!["AAPL".to_string()],
        );

        // Clean up
        unsafe {
            std::env::remove_var("APCA_API_KEY_ID");
        }

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = format!("{}", err);
        assert!(err_msg.contains("APCA_API_SECRET_KEY"));
        assert!(matches!(err, BrokerError::ConfigError(_)));
    }

    #[test]
    fn from_env_fails_when_both_vars_missing() {
        // Ensure both environment variables are not set
        unsafe {
            std::env::remove_var("APCA_API_KEY_ID");
            std::env::remove_var("APCA_API_SECRET_KEY");
        }

        let result = AlpacaStreamConfig::from_env(
            "iex".to_string(),
            vec!["AAPL".to_string()],
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, BrokerError::ConfigError(_)));
    }

    #[test]
    fn config_allows_empty_symbols_list() {
        // Set up environment variables
        unsafe {
            std::env::set_var("APCA_API_KEY_ID", "test_key_id");
            std::env::set_var("APCA_API_SECRET_KEY", "test_secret_key");
        }

        let result = AlpacaStreamConfig::from_env(
            "test".to_string(),
            vec![], // Empty symbols list
        );

        // Clean up
        unsafe {
            std::env::remove_var("APCA_API_KEY_ID");
            std::env::remove_var("APCA_API_SECRET_KEY");
        }

        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.symbols.is_empty());
    }

    #[test]
    fn config_supports_wildcard_symbol() {
        // Set up environment variables
        unsafe {
            std::env::set_var("APCA_API_KEY_ID", "test_key_id");
            std::env::set_var("APCA_API_SECRET_KEY", "test_secret_key");
        }

        let result = AlpacaStreamConfig::from_env(
            "sip".to_string(),
            vec!["*".to_string()], // Subscribe to all symbols
        );

        // Clean up
        unsafe {
            std::env::remove_var("APCA_API_KEY_ID");
            std::env::remove_var("APCA_API_SECRET_KEY");
        }

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.symbols, vec!["*"]);
        assert_eq!(config.feed, "sip");
    }
}