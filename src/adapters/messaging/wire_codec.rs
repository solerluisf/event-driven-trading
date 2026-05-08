use serde::{Serialize, de::DeserializeOwned};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::adapters::messaging::market_data_publisher::MarketDataEvent;
use crate::core::domain::wire_message::{GatewayRequest, GatewayResponse};

const MSGPACK_MAGIC: &[u8; 4] = b"BGW1";
static STRICT_MSGPACK_DECODE: OnceLock<bool> = OnceLock::new();

#[derive(Debug, Default)]
pub struct WireCodecMetricsSnapshot {
    pub decode_msgpack_total: u64,
    pub decode_json_total: u64,
    pub decode_error_total: u64,
    pub encode_error_total: u64,
}

static DECODE_MSGPACK_TOTAL: AtomicU64 = AtomicU64::new(0);
static DECODE_JSON_TOTAL: AtomicU64 = AtomicU64::new(0);
static DECODE_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);
static ENCODE_ERROR_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    MessagePack,
    Json,
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

fn decode_with_fallback<T: DeserializeOwned>(bytes: &[u8]) -> Result<(T, WireFormat), String> {
    if bytes.starts_with(MSGPACK_MAGIC) {
        let decoded = rmp_serde::from_slice::<T>(&bytes[MSGPACK_MAGIC.len()..]).map_err(|e| {
            DECODE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
            e.to_string()
        })?;
        DECODE_MSGPACK_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Ok((decoded, WireFormat::MessagePack));
    }

    if strict_msgpack_decode_enabled() {
        DECODE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        return Err(
            "strict MessagePack decode is enabled; refusing non-MessagePack payload".to_string(),
        );
    }

    let decoded = serde_json::from_slice::<T>(bytes).map_err(|e| {
        DECODE_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
        e.to_string()
    })?;
    DECODE_JSON_TOTAL.fetch_add(1, Ordering::Relaxed);
    Ok((decoded, WireFormat::Json))
}

pub fn strict_msgpack_decode_enabled() -> bool {
    *STRICT_MSGPACK_DECODE.get_or_init(|| {
        std::env::var("GATEWAY_STRICT_MSGPACK_DECODE")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    })
}

pub fn wire_codec_metrics_snapshot() -> WireCodecMetricsSnapshot {
    WireCodecMetricsSnapshot {
        decode_msgpack_total: DECODE_MSGPACK_TOTAL.load(Ordering::Relaxed),
        decode_json_total: DECODE_JSON_TOTAL.load(Ordering::Relaxed),
        decode_error_total: DECODE_ERROR_TOTAL.load(Ordering::Relaxed),
        encode_error_total: ENCODE_ERROR_TOTAL.load(Ordering::Relaxed),
    }
}

pub fn encode_gateway_request(req: &GatewayRequest) -> Result<Vec<u8>, String> {
    encode_msgpack(req)
}

pub fn decode_gateway_request(bytes: &[u8]) -> Result<(GatewayRequest, WireFormat), String> {
    decode_with_fallback(bytes)
}

pub fn encode_gateway_response(resp: &GatewayResponse) -> Result<Vec<u8>, String> {
    encode_msgpack(resp)
}

pub fn decode_gateway_response(bytes: &[u8]) -> Result<(GatewayResponse, WireFormat), String> {
    decode_with_fallback(bytes)
}

pub fn encode_market_data_event(event: &MarketDataEvent) -> Result<Vec<u8>, String> {
    encode_msgpack(event)
}

pub fn decode_market_data_event(bytes: &[u8]) -> Result<(MarketDataEvent, WireFormat), String> {
    decode_with_fallback(bytes)
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
    fn gateway_request_decodes_legacy_json_fallback() {
        let json_bytes = serde_json::to_vec(&GatewayRequest::Subscribe(MarketSubscription {
            symbol: "TSLA".to_string(),
        }))
        .expect("json serialize should succeed");

        let (decoded, format) =
            decode_gateway_request(&json_bytes).expect("json fallback decode should succeed");

        assert_eq!(format, WireFormat::Json);
        assert!(matches!(decoded, GatewayRequest::Subscribe(_)));
    }

    #[test]
    fn decode_invalid_payload_returns_error() {
        let bad_payload = vec![0x01, 0x02, 0x03, 0x04, 0x05];
        assert!(decode_gateway_request(&bad_payload).is_err());
        assert!(decode_gateway_response(&bad_payload).is_err());
        assert!(decode_market_data_event(&bad_payload).is_err());
    }

    #[test]
    fn decode_bad_msgpack_frame_returns_error() {
        let mut framed = b"BGW1".to_vec();
        framed.extend_from_slice(&[0xC1, 0xC1, 0xC1]); // invalid MessagePack markers

        assert!(decode_gateway_request(&framed).is_err());
        assert!(decode_gateway_response(&framed).is_err());
        assert!(decode_market_data_event(&framed).is_err());
    }
}
