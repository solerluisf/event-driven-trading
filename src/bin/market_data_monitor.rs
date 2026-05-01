// src/bin/market_data_monitor.rs
//
// Connects to the gateway's ZeroMQ PUB socket and prints all
// market data events as they arrive.
//
// Usage:
//   cargo run --bin market_data_monitor



fn main() {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::SUB).expect("failed to create SUB socket");

    let endpoint = std::env::var("GATEWAY_ZMQ_PUB_ENDPOINT")
        .unwrap_or_else(|_| "tcp://127.0.0.1:5556".to_string());

    socket.connect(&endpoint).expect("failed to connect");

    // Subscribe to all market_data topics
    socket.set_subscribe(b"market_data.").expect("failed to subscribe");

    println!("market_data_monitor connected to {}", endpoint);
    println!("waiting for events...\n");

    loop {
        // Frame 1: topic
        let topic = socket.recv_string(0)
            .expect("recv error")
            .unwrap_or_default();

        // Frame 2: JSON payload
        let payload = socket.recv_string(0)
            .expect("recv error")
            .unwrap_or_default();

        // Pretty-print it
        let pretty = serde_json::from_str::<serde_json::Value>(&payload)
            .map(|v| serde_json::to_string_pretty(&v).unwrap_or(payload.clone()))
            .unwrap_or(payload);

        println!("── {} ──────────────────────────────", topic);
        println!("{}\n", pretty);
    }
}