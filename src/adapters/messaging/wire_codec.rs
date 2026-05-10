use serde::{Serialize, de::DeserializeOwned};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::adapters::messaging::market_data_publisher::MarketDataEvent;
use crate::core::domain::order::OrderLifecycleEvent;
use crate::core::domain::wire_message::{GatewayRequest, GatewayResponse};

const MSGPACK_MAGIC: &[u8; 4] = b"BGW1";

#[derive(Debug, Default)]
pub struct WireCodecMetricsSnapshot {
    pub decode_msgpack_total: u64,
    pub decode_error_total: u64,
    pub encode_error_total: u64,
}

static DECODE_MSGPACK_TOTAL: AtomicU64 = AtomicU64::new(0);
static DECODE_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static ENCODE_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    MessagePack,
}

fn encode_msgpack<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let payload = rmp_serde::to_vec_named(value).map_err(|e| {
        ENCODE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        e.to_string()
    })?;
    let mut framed = Vec::with_capacity(MSGPACK_MAGIC.len() + payload.len());
    framed.extend_from_slice(MSGPACK_MAGIC);
    framed.extend_from_slice(&payload);
    Ok(framed)
}

fn decode_msgpack<T: DeserializeOwned>(bytes: &[u8]) -> Result<(T, WireFormat), String> {
    if !bytes.starts_with(MSGPACK_MAGIC) {
        DECODE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Err(
            format!(
                "Invalid wire format: expected MessagePack ({} prefix), got payload starting with {:?}. " ,
                String::from_utf8_lossy(MSGPACK_MAGIC),
                bytes.get(0..4.min(bytes.len())).unwrap_or(bytes)
            )
        );
    }

    let decoded = rmp_serde::from_slice::<T>(&bytes[MSGPACK_MAGIC.len()..]).map_err(|e| {
        DECODE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        e.to_string()
    })?;
    DECODE_MSGPACK_TOTAL.fetch_add(1, Ordering::Relaxed);
    Ok((decoded, WireFormat::MessagePack))
}

pub fn wire_codec_metrics_snapshot() -> WireCodecMetricsSnapshot {
    WireCodecMetricsSnapshot {
        decode_msgpack_total: DECODE_MSGPACK_TOTAL.load(Ordering::Relaxed),
        decode_error_total: DECODE_ERROR_TOTAL.load(Ordering::Relaxed),
        encode_error_total: ENCODE_ERROR_TOTAL.load(Ordering::Relaxed),
    }
}

pub fn encode_gateway_request(req: &GatewayRequest) -> Result<Vec<u8>, String> {
    encode_msgpack(req)
}

pub fn decode_gateway_request(bytes: &[u8]) -> Result<(GatewayRequest, WireFormat), String> {
    decode_msgpack(bytes)
}

pub fn encode_gateway_response(resp: &GatewayResponse) -> Result<Vec<u8>, String> {
    encode_msgpack(resp)
}

pub fn decode_gateway_response(bytes: &[u8]) -> Result<(GatewayResponse, WireFormat), String> {
    decode_msgpack(bytes)
}

pub fn encode_market_data_event(event: &MarketDataEvent) -> Result<Vec<u8>, String> {
    encode_msgpack(event)
}

pub fn decode_market_data_event(bytes: &[u8]) -> Result<(MarketDataEvent, WireFormat), String> {
    decode_msgpack(bytes)
}

pub fn encode_order_lifecycle_event(event: &OrderLifecycleEvent) -> Result<Vec<u8>, String> {
    encode_msgpack(event)
}

pub fn decode_order_lifecycle_event(bytes: &[u8]) -> Result<(OrderLifecycleEvent, WireFormat), String> {
    decode_msgpack(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::adapters::messaging::market_data_publisher::MarketDataEventType;
    use crate::core::domain::market_data::MarketSubscription;
    use crate::core::domain::wire_message::{ErrorPayload, GatewayRequest, GatewayResponse};

    #[test]
    fn gateway_request_round_trip_msgpack() {
        let req = GatewayRequest::Subscribe(MarketSubscription {
            symbol: "AAPL".to_string(),
            correlation_id: Some("corr-wire-001".into()),
        });

        let encoded = encode_gateway_request(&req).expect("encode should succeed");
        let (decoded, format) = decode_gateway_request(&encoded).expect("decode should succeed");

        assert_eq!(format, WireFormat::MessagePack);
        assert!(matches!(decoded, GatewayRequest::Subscribe(_)));
    }

    #[test]
    fn gateway_response_round_trip_msgpack() {
        let resp = GatewayResponse::Err(ErrorPayload {
            correlation_id: Some("cid-123".to_string()),
            code: "ANY_ERROR".to_string(),
            message: "boom".to_string(),
        });

        let encoded = encode_gateway_response(&resp).expect("encode should succeed");
        let (decoded, format) = decode_gateway_response(&encoded).expect("decode should succeed");

        assert_eq!(format, WireFormat::MessagePack);
        assert!(matches!(decoded, GatewayResponse::Err(_)));
    }

    #[test]
    fn market_data_event_round_trip_msgpack() {
        let event = MarketDataEvent {
            symbol: "MSFT".to_string(),
            event_type: MarketDataEventType::Tick,
            timestamp: "2026-05-08T10:00:00Z".to_string(),
            payload: json!({
                "bid": 412.34,
                "ask": 412.36
            }),
        };

        let encoded = encode_market_data_event(&event).expect("encode should succeed");
        let (decoded, format) =
            decode_market_data_event(&encoded).expect("decode should succeed");

        assert_eq!(format, WireFormat::MessagePack);
        assert_eq!(decoded.symbol, "MSFT");
        assert!(matches!(decoded.event_type, MarketDataEventType::Tick));
    }

    #[test]
    fn decode_invalid_payload_returns_error() {
        let bad_payload = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        assert!(decode_gateway_request(&bad_payload).is_err());
        assert!(decode_gateway_response(&bad_payload).is_err());
        assert!(decode_market_data_event(&bad_payload).is_err());
    }

    #[test]
    fn decode_json_payload_returns_error() {
        // JSON payloads should be rejected - only MessagePack is supported
        let json_bytes = serde_json::to_vec(&GatewayRequest::Subscribe(MarketSubscription {
            symbol: "TSLA".to_string(),
            correlation_id: Some("corr-wire-002".into()),
        }))
        .expect("json serialize should succeed");

        let result = decode_gateway_request(&json_bytes);
        assert!(result.is_err(), "JSON should be rejected");
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Invalid wire format"), "Error should mention invalid format: {}", err_msg);
        assert!(err_msg.contains("BGW1"), "Error should mention expected prefix: {}", err_msg);
    }

    #[test]
    fn decode_bad_msgpack_frame_returns_error() {
        let mut framed = b"BGW1".to_vec();
        framed.extend_from_slice(&[0xC1, 0xC1, 0xC1]); // invalid MessagePack markers

        assert!(decode_gateway_request(&framed).is_err());
        assert!(decode_gateway_response(&framed).is_err());
        assert!(decode_market_data_event(&framed).is_err());
    }

    // --- Order Lifecycle Event Tests ---

    use crate::core::domain::order::{OrderLifecycleEvent, OrderLifecycleEventType};

    #[test]
    fn order_lifecycle_event_round_trip_msgpack() {
        let event = OrderLifecycleEvent {
            event_id: "evt-123".to_string(),
            execution_id: "exec-456".to_string(),
            client_order_id: Some("client-789".to_string()),
            symbol: "AAPL".to_string(),
            event_type: OrderLifecycleEventType::Filled,
            timestamp: "2026-05-08T10:00:00Z".to_string(),
            payload: json!({
                "filled_qty": 100,
                "filled_price": 150.25,
            }),
        };

        let encoded = encode_order_lifecycle_event(&event).expect("encode should succeed");
        let (decoded, format) =
            decode_order_lifecycle_event(&encoded).expect("decode should succeed");

        assert_eq!(format, WireFormat::MessagePack);
        assert_eq!(decoded.event_id, "evt-123");
        assert_eq!(decoded.execution_id, "exec-456");
        assert_eq!(decoded.client_order_id, Some("client-789".to_string()));
        assert_eq!(decoded.symbol, "AAPL");
        assert_eq!(decoded.event_type, OrderLifecycleEventType::Filled);
        assert_eq!(decoded.timestamp, "2026-05-08T10:00:00Z");
        assert_eq!(decoded.payload["filled_qty"], 100);
        assert_eq!(decoded.payload["filled_price"], 150.25);
    }

    #[test]
    fn order_lifecycle_event_round_trip_all_types() {
        let event_types = vec![
            OrderLifecycleEventType::Submitted,
            OrderLifecycleEventType::PartialFill,
            OrderLifecycleEventType::Filled,
            OrderLifecycleEventType::Rejected,
            OrderLifecycleEventType::Cancelled,
            OrderLifecycleEventType::Replaced,
            OrderLifecycleEventType::Expired,
            OrderLifecycleEventType::Error,
        ];

        for event_type in event_types {
            let event = OrderLifecycleEvent {
                event_id: "evt-123".to_string(),
                execution_id: "exec-456".to_string(),
                client_order_id: None,
                symbol: "MSFT".to_string(),
                event_type: event_type.clone(),
                timestamp: "2026-05-08T10:00:00Z".to_string(),
                payload: json!({"test": true}),
            };

            let encoded = encode_order_lifecycle_event(&event).expect("encode should succeed");
            let (decoded, format) =
                decode_order_lifecycle_event(&encoded).expect("decode should succeed");

            assert_eq!(format, WireFormat::MessagePack);
            assert_eq!(decoded.event_type, event_type, "event type should match after round-trip");
        }
    }

    #[test]
    fn order_lifecycle_event_json_payload_returns_error() {
        // JSON payloads should be rejected - only MessagePack is supported
        let event = OrderLifecycleEvent {
            event_id: "evt-json".to_string(),
            execution_id: "exec-json".to_string(),
            client_order_id: Some("client-json".to_string()),
            symbol: "TSLA".to_string(),
            event_type: OrderLifecycleEventType::Submitted,
            timestamp: "2026-05-08T10:00:00Z".to_string(),
            payload: json!({"test": "json"}),
        };

        let json_bytes = serde_json::to_vec(&event).expect("json serialize should succeed");

        let result = decode_order_lifecycle_event(&json_bytes);
        assert!(result.is_err(), "JSON should be rejected");
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("Invalid wire format"), "Error should mention invalid format");
    }

    #[test]
    fn order_lifecycle_event_without_client_order_id() {
        let event = OrderLifecycleEvent {
            event_id: "evt-123".to_string(),
            execution_id: "exec-456".to_string(),
            client_order_id: None,
            symbol: "AAPL".to_string(),
            event_type: OrderLifecycleEventType::Filled,
            timestamp: "2026-05-08T10:00:00Z".to_string(),
            payload: json!({}),
        };

        let encoded = encode_order_lifecycle_event(&event).expect("encode should succeed");
        let (decoded, _) = decode_order_lifecycle_event(&encoded).expect("decode should succeed");

        assert!(decoded.client_order_id.is_none());
    }

    #[test]
    fn decode_invalid_payload_for_order_lifecycle() {
        let bad_payload = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        assert!(decode_order_lifecycle_event(&bad_payload).is_err());
        
        // Test with BGW1 magic but invalid payload
        let mut framed = b"BGW1".to_vec();
        framed.extend_from_slice(&[0xC1, 0xC1, 0xC1]);
        assert!(decode_order_lifecycle_event(&framed).is_err());
    }

    #[test]
    fn order_lifecycle_event_with_complex_payload() {
        let event = OrderLifecycleEvent {
            event_id: "evt-complex".to_string(),
            execution_id: "exec-complex".to_string(),
            client_order_id: Some("client-123".to_string()),
            symbol: "AAPL".to_string(),
            event_type: OrderLifecycleEventType::PartialFill,
            timestamp: "2026-05-08T10:00:00Z".to_string(),
            payload: json!({
                "filled_qty": 50,
                "filled_price": 150.50,
                "remaining_qty": 50,
                "execution_venue": "NYSE",
                "liquidity": "remove",
                "metadata": {
                    "algo_id": "algo-1",
                    "session_id": "session-abc"
                }
            }),
        };

        let encoded = encode_order_lifecycle_event(&event).expect("encode should succeed");
        let (decoded, _) = decode_order_lifecycle_event(&encoded).expect("decode should succeed");

        assert_eq!(decoded.payload["filled_qty"], 50);
        assert_eq!(decoded.payload["execution_venue"], "NYSE");
        assert_eq!(decoded.payload["metadata"]["algo_id"], "algo-1");
    }

}
