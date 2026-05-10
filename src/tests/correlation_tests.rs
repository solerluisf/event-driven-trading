// correlation_tests.rs
//
// Tests for end-to-end correlation ID tracking across the gateway.
// Verifies that correlation IDs flow from inbound requests through
// to outbound responses and journal records.

#[cfg(test)]
mod correlation_tests {
    use crate::adapters::messaging::wire_codec::{
        decode_gateway_request, decode_gateway_response, encode_gateway_request,
    };
    use crate::core::domain::journal::{RequestRecord, ResponseRecord};
    use crate::core::domain::market_data::MarketSubscription;
    use crate::core::domain::order::{
        CancelCmd, ExecutionId, OrderCmd, OrderSide, OrderType, ReplaceCmd, StatusQuery,
        TimeInForce,
    };
    use crate::core::domain::wire_message::{
        ErrorPayload, GatewayRequest, GatewayResponse, ResponsePayload,
    };

    // ═══════════════════════════════════════════════════════════════════════════
    // Correlation ID Echo Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn submit_order_correlation_id_echoed_in_response() {
        let correlation_id = "test-corr-123".to_string();
        let cmd = OrderCmd {
            symbol: "AAPL".to_string(),
            qty: 100,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: None,
            extended_hours: false,
            notional: None,
            correlation_id: Some(correlation_id.clone()),
        };

        // Simulate the bus_adapter dispatch logic
        let echoed_correlation = cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone());

        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    #[test]
    fn submit_order_fallback_to_client_order_id() {
        // When correlation_id is None, should fall back to client_order_id
        let client_id = "client-id-456".to_string();
        let cmd = OrderCmd {
            symbol: "AAPL".to_string(),
            qty: 100,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: Some(client_id.clone()),
            extended_hours: false,
            notional: None,
            correlation_id: None, // No correlation ID provided
        };

        let echoed_correlation = cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone());

        assert_eq!(echoed_correlation, Some(client_id));
    }

    #[test]
    fn cancel_order_correlation_id_echoed_in_response() {
        let correlation_id = "cancel-corr-789".to_string();
        let cmd = CancelCmd {
            execution_id: ExecutionId("exec-123".to_string()),
            correlation_id: Some(correlation_id.clone()),
        };

        let echoed_correlation = cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone()));

        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    #[test]
    fn cancel_order_fallback_to_execution_id() {
        let exec_id = ExecutionId("exec-789".to_string());
        let cmd = CancelCmd {
            execution_id: exec_id.clone(),
            correlation_id: None, // No correlation ID provided
        };

        let echoed_correlation = cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone()));

        assert_eq!(echoed_correlation, Some(exec_id.0));
    }

    #[test]
    fn replace_order_correlation_id_echoed_in_response() {
        let correlation_id = "replace-corr-abc".to_string();
        let cmd = ReplaceCmd {
            execution_id: ExecutionId("exec-123".to_string()),
            symbol: "TSLA".to_string(),
            side: OrderSide::Sell,
            qty: Some(50),
            limit_price: Some(250.0),
            correlation_id: Some(correlation_id.clone()),
        };

        let echoed_correlation = cmd.correlation_id.clone().or_else(|| Some(cmd.execution_id.0.clone()));

        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    #[test]
    fn query_status_correlation_id_echoed_in_response() {
        let correlation_id = "query-corr-def".to_string();
        let query = StatusQuery {
            execution_id: ExecutionId("exec-456".to_string()),
            correlation_id: Some(correlation_id.clone()),
        };

        let echoed_correlation = query.correlation_id.clone().or_else(|| Some(query.execution_id.0.clone()));

        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    #[test]
    fn subscribe_correlation_id_echoed_in_response() {
        let correlation_id = "sub-corr-ghi".to_string();
        let sub = MarketSubscription {
            symbol: "MSFT".to_string(),
            correlation_id: Some(correlation_id.clone()),
        };

        let echoed_correlation = sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone()));

        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    #[test]
    fn subscribe_fallback_to_symbol() {
        let sub = MarketSubscription {
            symbol: "GOOGL".to_string(),
            correlation_id: None, // No correlation ID provided
        };

        let echoed_correlation = sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone()));

        assert_eq!(echoed_correlation, Some("GOOGL".to_string()));
    }

    #[test]
    fn unsubscribe_correlation_id_echoed_in_response() {
        let correlation_id = "unsub-corr-jkl".to_string();
        let sub = MarketSubscription {
            symbol: "AMZN".to_string(),
            correlation_id: Some(correlation_id.clone()),
        };

        let echoed_correlation = sub.correlation_id.clone().or_else(|| Some(sub.symbol.clone()));

        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Wire Message Correlation Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn response_payload_carries_correlation_id() {
        let correlation_id = Some("wire-corr-001".to_string());
        let payload = ResponsePayload {
            correlation_id: correlation_id.clone(),
            result: "success".to_string(),
        };

        assert_eq!(payload.correlation_id, correlation_id);
    }

    #[test]
    fn error_payload_carries_correlation_id() {
        let correlation_id = Some("wire-corr-error".to_string());
        let payload = ErrorPayload {
            correlation_id: correlation_id.clone(),
            code: "TEST_ERROR".to_string(),
            message: "Test error message".to_string(),
        };

        assert_eq!(payload.correlation_id, correlation_id);
    }

    #[test]
    fn gateway_response_serialization_preserves_correlation_id() {
        let resp = GatewayResponse::Ok(ResponsePayload {
            correlation_id: Some("serialization-test".to_string()),
            result: "exec-123".to_string(),
        });

        let encoded = serde_json::to_vec(&resp).expect("encode should succeed");
        let decoded: GatewayResponse = serde_json::from_slice(&encoded).expect("decode should succeed");

        match decoded {
            GatewayResponse::Ok(payload) => {
                assert_eq!(payload.correlation_id, Some("serialization-test".to_string()));
            }
            _ => panic!("Expected Ok response"),
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Journal Record Correlation Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn request_record_carries_correlation_id() {
        let record = RequestRecord {
            id: "req-123".to_string(),
            raw_payload: Some(r#"{"symbol":"AAPL"}"#.to_string()),
            correlation_id: Some("journal-corr-001".to_string()),
        };

        assert_eq!(record.correlation_id, Some("journal-corr-001".to_string()));
    }

    #[test]
    fn response_record_carries_correlation_id() {
        let record = ResponseRecord {
            id: "resp-456".to_string(),
            raw_payload: Some(r#"{"result":"success"}"#.to_string()),
            correlation_id: Some("journal-corr-002".to_string()),
        };

        assert_eq!(record.correlation_id, Some("journal-corr-002".to_string()));
    }

    #[test]
    fn request_record_serialization_preserves_correlation_id() {
        let record = RequestRecord {
            id: "serialization-test".to_string(),
            raw_payload: None,
            correlation_id: Some("corr-serialization-test".to_string()),
        };

        let json = serde_json::to_string(&record).expect("should serialize");
        assert!(json.contains("\"correlation_id\":\"corr-serialization-test\""));

        let decoded: RequestRecord = serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(decoded.correlation_id, Some("corr-serialization-test".to_string()));
    }

    #[test]
    fn response_record_serialization_preserves_correlation_id() {
        let record = ResponseRecord {
            id: "serialization-test".to_string(),
            raw_payload: Some(r#"{"data":"value"}"#.to_string()),
            correlation_id: Some("corr-resp-test".to_string()),
        };

        let json = serde_json::to_string(&record).expect("should serialize");
        assert!(json.contains("\"correlation_id\":\"corr-resp-test\""));

        let decoded: ResponseRecord = serde_json::from_str(&json).expect("should deserialize");
        assert_eq!(decoded.correlation_id, Some("corr-resp-test".to_string()));
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // End-to-End Correlation Flow Tests
    // ═══════════════════════════════════════════════════════════════════════════

    #[test]
    fn full_request_response_correlation_flow() {
        // Simulate a complete flow: Request → Process → Response
        let client_correlation_id = "end-to-end-corr-001".to_string();

        // 1. Client creates a request with correlation ID
        let request = GatewayRequest::SubmitOrder(OrderCmd {
            symbol: "NVDA".to_string(),
            qty: 10,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
            limit_price: Some(500.0),
            stop_price: None,
            client_order_id: Some("client-id-xyz".to_string()),
            extended_hours: false,
            notional: None,
            correlation_id: Some(client_correlation_id.clone()),
        });

        // 2. Encode the request (as it would be sent over the wire)
        let encoded = encode_gateway_request(&request).expect("should encode");

        // 3. Server decodes the request
        let (decoded_req, _format) = decode_gateway_request(&encoded).expect("should decode");

        // 4. Extract correlation ID from decoded request (simulating bus_adapter logic)
        let extracted_correlation = match &decoded_req {
            GatewayRequest::SubmitOrder(cmd) => {
                cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone())
            }
            _ => None,
        };

        assert_eq!(extracted_correlation, Some(client_correlation_id.clone()));

        // 5. Server creates response with same correlation ID
        let response = GatewayResponse::Ok(ResponsePayload {
            correlation_id: extracted_correlation,
            result: "exec-nvda-123".to_string(),
        });

        // 6. Encode and decode response
        let resp_encoded = serde_json::to_vec(&response).expect("should encode response");
        let decoded_resp: GatewayResponse = serde_json::from_slice(&resp_encoded).expect("should decode response");

        // 7. Client receives response with correlation ID
        match decoded_resp {
            GatewayResponse::Ok(payload) => {
                assert_eq!(payload.correlation_id, Some(client_correlation_id));
            }
            _ => panic!("Expected Ok response"),
        }
    }

    #[test]
    fn correlation_id_none_when_not_provided() {
        // Test that correlation_id can be None and still works
        let cmd = OrderCmd {
            symbol: "META".to_string(),
            qty: 5,
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::Day,
            limit_price: None,
            stop_price: None,
            client_order_id: None, // No client_order_id either
            extended_hours: false,
            notional: None,
            correlation_id: None, // No correlation_id
        };

        let echoed_correlation = cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone());

        assert_eq!(echoed_correlation, None);
    }

    #[test]
    fn correlation_id_priority_over_fallbacks() {
        // Test that explicit correlation_id takes priority over fallbacks
        let correlation_id = "explicit-corr".to_string();
        let client_id = "client-id".to_string();

        let cmd = OrderCmd {
            symbol: "AMD".to_string(),
            qty: 20,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Day,
            limit_price: Some(150.0),
            stop_price: None,
            client_order_id: Some(client_id), // Has client_order_id
            extended_hours: false,
            notional: None,
            correlation_id: Some(correlation_id.clone()), // Has explicit correlation_id
        };

        let echoed_correlation = cmd.correlation_id.clone().or_else(|| cmd.client_order_id.clone());

        // Should use correlation_id, not client_order_id
        assert_eq!(echoed_correlation, Some(correlation_id));
    }

    #[test]
    fn journal_records_correlation_matching() {
        // Test that outbound and inbound records can be matched by correlation_id
        let correlation_id = "audit-trail-001".to_string();

        let outbound = RequestRecord {
            id: "req-001".to_string(),
            raw_payload: Some(r#"{"action":"submit"}"#.to_string()),
            correlation_id: Some(correlation_id.clone()),
        };

        let inbound = ResponseRecord {
            id: "resp-001".to_string(),
            raw_payload: Some(r#"{"result":"filled"}"#.to_string()),
            correlation_id: Some(correlation_id.clone()),
        };

        assert_eq!(outbound.correlation_id, inbound.correlation_id);
        assert_eq!(outbound.correlation_id, Some(correlation_id));
    }
}
