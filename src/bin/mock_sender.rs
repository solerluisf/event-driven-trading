// src/bin/mock_sender.rs
//
// Standalone binary that acts as a minimal Execution Service stub.
// Run this in a second terminal while the gateway is running to verify
// the full REQ → REP round-trip.
//
// Usage:
//   cargo run --bin mock_sender
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
use broker_gateway_service::core::domain::order::{
    OrderCmd, OrderSide, OrderType, TimeInForce,
    CancelCmd, ExecutionId,
};

#[tokio::main]
async fn main() {
    let endpoint = std::env::var("GATEWAY_ZMQ_ENDPOINT")
        .unwrap_or_else(|_| "tcp://127.0.0.1:5555".into());

    println!("mock_sender connecting to {}", endpoint);

    let ctx = Context::new();
    let socket = ctx.socket(zmq::REQ).expect("failed to create REQ socket");
    socket.connect(&endpoint).expect("failed to connect REQ socket");
    let socket = Arc::new(Mutex::new(socket));

    // ── Test 1: submit_order ──────────────────────────────────────────────────
    let submit = GatewayRequest::SubmitOrder(OrderCmd {
        symbol: "AAPL".into(),
        qty: 1,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Day,
        limit_price: None,
        stop_price: None,
        client_order_id: Some("test-idem-001".into()),
        extended_hours: false,
        notional: None,
        correlation_id: Some("corr-submit-001".into()),
    });

    let response = send_and_recv(&socket, &submit).await;
    println!("submit_order response: {:?}", response);

    // ── Test 2: cancel_order ──────────────────────────────────────────────────
    // Use the execution_id returned from submit if it was Ok, else a fake one.
    let exec_id = match &response {
        GatewayResponse::Ok(p) => p.result.clone(),
        _ => "fake-exec-id".into(),
    };

    let cancel = GatewayRequest::CancelOrder(CancelCmd {
        execution_id: ExecutionId(exec_id),
        symbol: "AAPL".to_string(),
        correlation_id: Some("corr-cancel-001".into()),
    });

    let response = send_and_recv(&socket, &cancel).await;
    println!("cancel_order response: {:?}", response);
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
