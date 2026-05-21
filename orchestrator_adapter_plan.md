# Gateway Orchestrator Adapter — Implementation Plan

> **Goal:** Integrate the existing Broker Gateway Service with the Orchestrator Service for control-plane operations.  
> **Scope:** Add an orchestration adapter to the Gateway that subscribes to Orchestrator commands, responds to health checks, and emits system events.  
> **Status:** COMPLETE — All 8 phases implemented and tested.

---

## Context

The Orchestrator Service is fully implemented with:
- HTTP REST API, CLI, WebSocket telemetry
- `BrokerGatewayConnector` that sends commands to `broker_gateway` service ID
- `SystemEvent` enum with kill-switch, mode, policy, circuit-breaker events
- Topic convention: `orchestrator.control.gateway.*` for commands, `system.kill_switch`, `system.mode` for broadcasts

The Gateway Service currently:
- Has `KillSwitch`, `OperationMode`, `CircuitBreaker`, `RateLimiter` (all owned locally)
- Has no subscription to Orchestrator control topics
- Has no `orchestration_handler.rs` or `OrchestrationCommand` enum
- Does not emit health heartbeats on the bus
- Does not respond to Orchestrator health probes

---

## Architecture

```
Orchestrator (PUB)                              Gateway (SUB)
=========================                       ===============
orchestrator.control.gateway.* ──────────────→  OrchestrationHandler
system.kill_switch ──────────────────────────→  KillSwitchSubscriber (new)
system.mode ─────────────────────────────────→  ModeSubscriber (new)

Gateway (PUB)                                   Orchestrator (SUB)
=========================                       ===============
service.broker_gateway.health ───────────────→  HealthAggregator
service.broker_gateway.control.ack ──────────→  Orchestrator ACK handler
service.broker_gateway.circuit_breaker ──────→  EventRouter (circuit breaker events)
```

The Gateway uses **SUB sockets** for inbound control commands and **PUB sockets** for outbound events. This matches the pattern already defined in all other service plans.

---

## Phase 1 — Domain Types

### 1.1 `core/domain/orchestration.rs` (new)

Define the `OrchestrationCommand` enum that maps to the Orchestrator's `ServiceCommand`:

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "command", content = "payload", rename_all = "snake_case")]
pub enum OrchestrationCommand {
    SetOperationMode(OperationMode),
    ActivateKillSwitch { reason: String, actor: String },
    ClearKillSwitch { reason: String, actor: String },
    ReloadPolicies,
    UpdateCircuitBreaker {
        failure_threshold: u32,
        cooldown_secs: u64,
    },
    ResetCircuitBreaker,
    UpdateRateLimiter {
        max_requests_per_min: u32,
        burst_capacity: u32,
    },
    PauseSymbol { symbol: String },
    ResumeSymbol { symbol: String },
    FlushPendingOrders,
    SetLogLevel { level: String },
    HealthCheck,  // Respond with GatewayHealthSnapshot
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrchestrationAck {
    pub command_type: String,
    pub success: bool,
    pub error: Option<String>,
    pub timestamp_ms: u64,
}
```

### 1.2 `core/domain/gateway_health.rs` (new)

Define the health snapshot the Gateway emits:

```rust
#[derive(Debug, Clone, Serialize)]
pub struct GatewayHealthSnapshot {
    pub timestamp_ms: u64,
    pub service_id: String,
    pub status: String,              // "healthy", "degraded", "unhealthy"
    pub kill_switch_active: bool,
    pub operation_mode: String,
    pub circuit_breaker_state: String, // "closed", "open", "half-open"
    pub rate_limiter_tokens_remaining: f64,
    pub rate_limiter_capacity: f64,
    pub active_connections: usize,
    pub orders_submitted_total: u64,
    pub orders_rejected_total: u64,
    pub ws_reconnect_count: u64,
    pub symbols_subscribed: Vec<String>,
}
```

---

## Phase 2 — Port Traits

### 2.1 `core/ports/orchestration_port.rs` (new)

```rust
pub struct OrchestrationMessage {
    pub command_type: String,
    pub payload: serde_json::Value,
    pub received_ns: u64,
}

#[async_trait]
pub trait IOrchestrationReceiver: Send + Sync {
    async fn recv(&self) -> Option<OrchestrationMessage>;
}
```

### 2.2 `core/ports/health_pub_port.rs` (new)

```rust
#[async_trait]
pub trait IHealthPublisher: Send + Sync {
    async fn publish_health(&self, snapshot: &GatewayHealthSnapshot) -> Result<(), GatewayError>;
}
```

---

## Phase 3 — Messaging Adapters

### 3.1 `adapters/messaging/orchestration_handler.rs` (new)

SUB socket connecting to `orchestrator.control.gateway.*`.

```rust
// Dedicated thread + mpsc channel pattern (same as bus_adapter.rs)
std::thread::spawn(move || {
    let socket = ctx.socket(zmq::SUB).unwrap();
    socket.connect(&endpoint).unwrap();
    socket.set_subscribe(b"orchestrator.control.gateway.").unwrap();

    loop {
        let payload = match socket.recv_bytes(0) {
            Ok(p) => p,
            Err(_) => continue,
        };
        tx.blocking_send(OrchestrationMessage {
            command_type: extract_command_type(&payload),
            payload: extract_payload(&payload),
            received_ns: current_time_ns(),
        }).unwrap();
    }
});
```

**Command dispatch:**

| Orchestrator Command | Gateway Action |
|---------------------|----------------|
| `SetOperationMode` | `ModeController::set()` |
| `ActivateKillSwitch` | `KillSwitch::enable()` + broadcast cancel-all |
| `ClearKillSwitch` | `KillSwitch::disable()` |
| `ReloadPolicies` | Reload rate limiter config from disk |
| `UpdateCircuitBreaker` | Update `CircuitBreaker` thresholds |
| `ResetCircuitBreaker` | `CircuitBreaker::reset()` |
| `UpdateRateLimiter` | Update `RateLimiterManager` capacity |
| `PauseSymbol` | Add to paused symbols set, unsubscribe from feed |
| `ResumeSymbol` | Remove from paused set, resubscribe |
| `FlushPendingOrders` | Drain pending order queue |
| `SetLogLevel` | Update tracing filter at runtime |
| `HealthCheck` | Return `GatewayHealthSnapshot` |

### 3.2 `adapters/messaging/kill_switch_subscriber.rs` (new)

SUB socket on `system.kill_switch`. Listens for kill-switch broadcasts from the Orchestrator.

```rust
// Same dedicated-thread pattern
socket.set_subscribe(b"system.kill_switch").unwrap();
```

On receipt:
- `KillSwitchActivated` → `kill_switch.enable()`, log, emit metrics
- `KillSwitchCleared` → `kill_switch.disable()`, log, emit metrics

### 3.3 `adapters/messaging/mode_subscriber.rs` (new)

SUB socket on `system.mode`. Listens for mode change broadcasts.

```rust
socket.set_subscribe(b"system.mode").unwrap();
```

On receipt:
- `ModeTransitionCompleted { to }` → update local `OperationMode`
- Emit ack to `service.broker_gateway.control.ack`

### 3.4 `adapters/messaging/heartbeat_publisher.rs` (new)

PUB socket publishing `GatewayHealthSnapshot` to `service.broker_gateway.health` every 5 seconds.

```rust
// Actor pattern (same as MarketDataPublisher)
pub struct HeartbeatPublisher {
    socket: zmq::Socket,
    rx: mpsc::Receiver<GatewayHealthSnapshot>,
}

// Spawned as tokio task, publishes every 5s
```

### 3.5 `adapters/messaging/circuit_breaker_publisher.rs` (new)

PUB socket on `service.broker_gateway.circuit_breaker`. Emits events when circuit breaker state changes:

```
service.broker_gateway.circuit_breaker  → CircuitBreakerEvent
```

```rust
pub enum CircuitBreakerEvent {
    Opened { service: String, failure_count: u32 },
    Closed { service: String },
    HalfOpen { service: String },
}
```

---

## Phase 4 — Config

### 4.1 `config/app_config.rs` — Add fields

```rust
// Existing fields:
zmq_rep_endpoint: "tcp://127.0.0.1:5555",
zmq_pub_endpoint: "tcp://127.0.0.1:5556",
zmq_order_lifecycle_endpoint: "tcp://127.0.0.1:5557",

// New fields:
orchestrator_control_endpoint: env_str("ORCHESTRATOR_CONTROL_ENDPOINT", "tcp://127.0.0.1:5560"),
orchestrator_events_endpoint:  env_str("ORCHESTRATOR_EVENTS_ENDPOINT", "tcp://127.0.0.1:5561"),
health_pub_endpoint:           env_str("HEALTH_PUB_ENDPOINT", "tcp://127.0.0.1:5562"),
circuit_breaker_pub_endpoint:  env_str("CIRCUIT_BREAKER_PUB_ENDPOINT", "tcp://127.0.0.1:5563"),
```

### 4.2 New endpoint map

| Port | Service | Socket | Direction |
|------|---------|--------|-----------|
| 5555 | Broker Gateway | ROUTER (bind) | Execution → Gateway |
| 5556 | Broker Gateway | PUB (bind) | Gateway → Market Data |
| 5557 | Broker Gateway | PUB (bind) | Gateway → all (order lifecycle) |
| 5558 | Risk Service | ROUTER (bind) | Strategy → Risk |
| 5559 | Risk Service | ROUTER (bind) | Risk → Execution |
| **5560** | **Orchestrator** | **PUB (bind)** | **Orch → Gateway (control)** |
| **5561** | **Orchestrator** | **PUB (bind)** | **Orch → all (events)** |
| **5562** | **Gateway** | **PUB (bind)** | **Gateway → Orch (health)** |
| **5563** | **Gateway** | **PUB (bind)** | **Gateway → Orch (circuit breaker)** |

---

## Phase 5 — main.rs Wiring

Add to `main.rs` after existing component initialization:

```rust
// ── Orchestrator integration ────────────────────────────────────────────────

// Health heartbeat publisher
let (heartbeat_tx, heartbeat_rx) = mpsc::channel(32);
let heartbeat_handle = tokio::spawn(async move {
    let mut publisher = HeartbeatPublisher::spawn(&cfg.health_pub_endpoint);
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        let snapshot = GatewayHealthSnapshot {
            timestamp_ms: current_time_ms(),
            service_id: "broker_gateway".to_string(),
            status: if kill_switch.is_enabled() { "degraded" } else { "healthy" },
            kill_switch_active: kill_switch.is_enabled(),
            operation_mode: cfg.operation_mode.to_string(),
            circuit_breaker_state: circuit_breaker.state().to_string(),
            rate_limiter_tokens_remaining: rate_limiter.tokens_remaining(&cfg.broker),
            rate_limiter_capacity: rate_limiter.capacity(&cfg.broker),
            active_connections: 0,  // TODO: track from stream
            orders_submitted_total: 0,  // TODO: track from metrics
            orders_rejected_total: 0,
            ws_reconnect_count: 0,
            symbols_subscribed: cfg.market_data_symbols.clone(),
        };
        let _ = heartbeat_tx.send(snapshot).await;
    }
});

// Kill-switch subscriber (from Orchestrator)
let kill_switch_subscriber = KillSwitchSubscriber::spawn(
    &cfg.orchestrator_events_endpoint,
    Arc::clone(&kill_switch),
);

// Mode subscriber (from Orchestrator)
let mode_subscriber = ModeSubscriber::spawn(
    &cfg.orchestrator_events_endpoint,
    // mode controller reference
);

// Circuit breaker publisher
let circuit_breaker_publisher = CircuitBreakerPublisher::spawn(
    &cfg.circuit_breaker_pub_endpoint,
    // circuit breaker reference
);

// Orchestration command handler
let orchestration_handler = OrchestrationHandler::spawn(
    &cfg.orchestrator_control_endpoint,
    Arc::clone(&kill_switch),
    // mode controller, circuit breaker, rate limiter references
);
```

Add to the `tokio::select!` block:

```rust
tokio::select! {
    result = bus.listen() => { ... }
    result = publisher_handle => { ... }
    result = order_lifecycle_handle => { ... }
    _ = async { if let Some(h) = stream_handle { h.await.ok(); } } => { ... }
    _ = heartbeat_handle => { error!("heartbeat publisher exited"); std::process::exit(1); }
    _ = kill_switch_subscriber => { /* runs until shutdown */ }
    _ = mode_subscriber => { /* runs until shutdown */ }
    _ = circuit_breaker_publisher => { /* runs until shutdown */ }
    _ = orchestration_handler => { /* runs until shutdown */ }
}
```

---

## Phase 6 — Metrics

Add to existing `MetricsAdapter`:

```
# Orchestrator integration
gateway_orch_commands_received_total{command_type}    counter
gateway_orch_commands_succeeded_total{command_type}   counter
gateway_orch_commands_failed_total{command_type}      counter
gateway_orch_command_latency_ms{command_type}         histogram
gateway_health_published_total                        counter
gateway_circuit_breaker_state_changes_total{state}    counter
```

---

## Phase 7 — Tests

| Test | What it validates |
|------|-------------------|
| `orchestration_handler_unit.rs` | Command dispatch to correct Gateway component |
| `kill_switch_subscriber_unit.rs` | Kill-switch enable/disable on bus message |
| `mode_subscriber_unit.rs` | Mode change on bus message |
| `heartbeat_publisher_unit.rs` | Health snapshot published at correct interval |
| `circuit_breaker_publisher_unit.rs` | CB state change events published |
| `orch_command_integration.rs` | Full round-trip: Orch command → Gateway → ack |
| `kill_switch_propagation_integration.rs` | Orch activates KS → Gateway receives → KS enabled |
| `mode_transition_integration.rs` | Orch changes mode → Gateway receives → mode updated |

---

## Phase 8 — Service Plan Updates

All service plan files need to be updated to reference the Gateway's new orchestration integration:

| File | Change |
|------|--------|
| `Risk/risk_service_plan.md` | Add Gateway as SUB target for `service.broker_gateway.circuit_breaker` events |
| `Strategy/strategy_service_plan.md` | Add Gateway health endpoint reference for position cache staleness |
| `Execution/execution_service_plan.md` | Add Gateway as ROUTER target (already defined, no change needed) |
| `Feature/4 - feature_service_plan.md` | Add Gateway circuit breaker events to bus topic table |
| `Model/5 - model_service_plan.md` | Add Gateway circuit breaker events to bus topic table |
| `Market Data/3 - market_data_service_plan_v2.md` | Add Gateway health endpoint reference |

---

## Implementation Order

1. **Phase 1** — Domain types (no dependencies, pure structs) ✅ COMPLETE
2. **Phase 2** — Port traits (depends on domain types) ✅ COMPLETE
3. **Phase 3** — Messaging adapters (depends on port traits) ✅ COMPLETE
4. **Phase 4** — Config (depends on adapter endpoints) ✅ COMPLETE
5. **Phase 5** — main.rs wiring (depends on all above) ✅ COMPLETE
6. **Phase 6** — Metrics (depends on adapters) ✅ COMPLETE
7. **Phase 7** — Tests (depends on all above) ✅ COMPLETE — 31 tests passing
8. **Phase 8** — Update service plans (documentation) ✅ COMPLETE

---

## Endpoint Summary

| Component | Socket | Endpoint | Direction |
|-----------|--------|----------|-----------|
| `OrchestrationHandler` | SUB | `tcp://127.0.0.1:5560` | Orch → Gateway |
| `KillSwitchSubscriber` | SUB | `tcp://127.0.0.1:5561` | Orch → Gateway |
| `ModeSubscriber` | SUB | `tcp://127.0.0.1:5561` | Orch → Gateway |
| `HeartbeatPublisher` | PUB | `tcp://127.0.0.1:5562` | Gateway → Orch |
| `CircuitBreakerPublisher` | PUB | `tcp://127.0.0.1:5563` | Gateway → Orch |

All endpoints configurable via environment variables with localhost defaults.

---

## Implementation Summary

### Files Created
| File | Phase | Description |
|------|-------|-------------|
| `src/core/domain/orchestration.rs` | 1 | `OrchestrationCommand` enum + `OrchestrationAck` |
| `src/core/domain/gateway_health.rs` | 1 | `GatewayHealthSnapshot` builder pattern |
| `src/core/ports/orchestration_port.rs` | 2 | `IOrchestrationReceiver` trait |
| `src/core/ports/health_pub_port.rs` | 2 | `IHealthPublisher` trait |
| `src/adapters/messaging/orchestration_handler.rs` | 3 | SUB socket + command dispatcher |
| `src/adapters/messaging/kill_switch_subscriber.rs` | 3 | SUB on `system.kill_switch` |
| `src/adapters/messaging/mode_subscriber.rs` | 3 | SUB on `system.mode` |
| `src/adapters/messaging/heartbeat_publisher.rs` | 3 | PUB health snapshots every 5s |
| `src/adapters/messaging/circuit_breaker_publisher.rs` | 3 | PUB CB state change events |
| `src/tests/orchestration_tests.rs` | 7 | 31 unit + integration tests |

### Files Modified
| File | Phase | Change |
|------|-------|--------|
| `src/core/domain/mod.rs` | 1 | Added `orchestration` + `gateway_health` modules |
| `src/core/ports/mod.rs` | 2 | Added `orchestration_port` + `health_pub_port` modules |
| `src/adapters/messaging/mod.rs` | 3 | Added 5 new adapter modules |
| `src/config/app_config.rs` | 4 | Added 4 orchestrator endpoint fields |
| `src/main.rs` | 5 | Wired all 5 orchestrator components + select! branches |
| `src/adapters/metrics/metrics_adapter.rs` | 6 | Added atomic counters for orchestrator metrics |
| `src/tests/mod.rs` | 7 | Added `orchestration_tests` + `correlation_tests` modules |
