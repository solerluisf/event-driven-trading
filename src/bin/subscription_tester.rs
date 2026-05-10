// src/bin/subscription_tester.rs
//
// Standalone binary that tests the dynamic market data subscription feature.
// Run this in a terminal while the gateway is running to verify
// subscribe/unsubscribe commands work.
//
// Usage:
//   cargo run --bin subscription_tester
//
// Environment:
//   GATEWAY_ZMQ_ENDPOINT  (default: tcp://127.0.0.1:5555)

use zmq::Context;
use std::sync::{Arc, Mutex};

// We import the shared wire types directly from the library crate.
use broker_gateway_service::core::domain::wire_message::{
    GatewayRequest, GatewayResponse,
};
use broker_gateway_service::adapters::messaging::wire_codec::{
    decode_gateway_response, encode_gateway_request,
};
use broker_gateway_service::core::domain::market_data::MarketSubscription;

#[tokio::main]
async fn main() {
    let endpoint = std::env::var("GATEWAY_ZMQ_ENDPOINT")
        .unwrap_or_else(|_| "tcp://127.0.0.1:5555".into());

    println!("subscription_tester connecting to {}", endpoint);

    let ctx = Context::new();
    let socket = ctx.socket(zmq::REQ).expect("failed to create REQ socket");
    socket.connect(&endpoint).expect("failed to connect REQ socket");
    let socket = Arc::new(Mutex::new(socket));

    // ── Test 1: subscribe to TSLA ─────────────────────────────────────────────
    println!("\n--- Test 1: Subscribe to TSLA ---");
    let subscribe_tsla = GatewayRequest::Subscribe(MarketSubscription {
        symbol: "TSLA".into(),
        correlation_id: Some("corr-tsla-sub".into()),
    });

    let response = send_and_recv(&socket, &subscribe_tsla).await;
    println!("subscribe TSLA response: {:?}", response);

    // ── Test 2: subscribe to GOOGL ───────────────────────────────────────────
    println!("\n--- Test 2: Subscribe to GOOGL ---");
    let subscribe_googl = GatewayRequest::Subscribe(MarketSubscription {
        symbol: "GOOGL".into(),
        correlation_id: Some("corr-googl-sub".into()),
    });

    let response = send_and_recv(&socket, &subscribe_googl).await;
    println!("subscribe GOOGL response: {:?}", response);

    // ── Test 3: unsubscribe from TSLA ────────────────────────────────────────
    println!("\n--- Test 3: Unsubscribe from TSLA ---");
    let unsubscribe_tsla = GatewayRequest::Unsubscribe(MarketSubscription {
        symbol: "TSLA".into(),
        correlation_id: Some("corr-tsla-unsub".into()),
    });

    let response = send_and_recv(&socket, &unsubscribe_tsla).await;
    println!("unsubscribe TSLA response: {:?}", response);

    // ── Test 4: subscribe to MSFT ────────────────────────────────────────────
    println!("\n--- Test 4: Subscribe to MSFT ---");
    let subscribe_msft = GatewayRequest::Subscribe(MarketSubscription {
        symbol: "MSFT".into(),
        correlation_id: Some("corr-msft-sub".into()),
    });

    let response = send_and_recv(&socket, &subscribe_msft).await;
    println!("subscribe MSFT response: {:?}", response);

    println!("\n--- Subscription tests complete ---");
    println!("Run 'cargo run --bin market_data_monitor' in another terminal");
    println!("to see the market data events for GOOGL and MSFT (but not TSLA)");
}

async fn send_and_recv(
    socket: &Arc<Mutex<zmq::Socket>>,
    req: &GatewayRequest,
) -> GatewayResponse {
    let bytes = encode_gateway_request(req).expect("serialize failed");

    // Send in blocking context
    let socket_clone = socket.clone();
    tokio::task::spawn_blocking(move || {
        let socket = socket_clone.lock().unwrap();
        socket.send(&bytes, 0).expect("send failed");
    })
    .await
    .expect("send task failed");

    // Receive in blocking context
    let socket_clone = socket.clone();
    let response_bytes = tokio::task::spawn_blocking(move || {
        let socket = socket_clone.lock().unwrap();
        socket.recv_bytes(0).expect("recv failed")
    })
    .await
    .expect("recv task failed");

    let (response, _) =
        decode_gateway_response(&response_bytes).expect("deserialize response failed");

    response
}