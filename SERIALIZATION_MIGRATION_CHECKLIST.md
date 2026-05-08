# MessagePack Migration Checklist

This checklist tracks rollout from legacy JSON to MessagePack on hot paths while keeping service uptime.

## Scope

- `REQ/REP` command channel (Execution Service <-> Gateway)
- `PUB/SUB` market data channel (Gateway -> Market Data consumers)
- Transitional compatibility: MessagePack-first with JSON decode fallback

## Phase 1 - Contract Freeze

- [ ] Freeze wire contracts for `GatewayRequest`, `GatewayResponse`, and `MarketDataEvent`.
- [ ] Confirm all enums use stable serde names (`rename_all`, explicit `rename` where needed).
- [ ] Confirm optional fields are explicitly modeled (`Option<T>`) and documented.
- [ ] Add/change-management rule: any wire contract change requires version note in PR description.

## Phase 2 - Producer First Rollout (Already Started)

- [x] Gateway encodes `GatewayResponse` as MessagePack.
- [x] Gateway decodes `GatewayRequest` as MessagePack with JSON fallback.
- [x] Market data publisher emits MessagePack payloads.
- [x] Add one startup log line showing active wire mode and fallback status.
- [ ] Add metrics counters:
  - [x] `wire_decode_total{format}` (MessagePack / JSON fallback)
  - [x] `wire_decode_error_total`
  - [x] `wire_encode_error_total`

## Phase 3 - Consumer Updates

### Execution Service (REQ client)

- [ ] Switch request encoding to MessagePack via shared codec.
- [ ] Switch response decoding to MessagePack via shared codec.
- [ ] Keep JSON fallback decode for one release window.
- [ ] Add integration test: send MessagePack request and verify round-trip.

### Market Data Service / Subscribers

- [ ] Switch payload decode to MessagePack via shared codec.
- [ ] Keep JSON fallback decode for one release window.
- [ ] Add soak test against live PUB stream (volume + latency sample).

## Phase 4 - Compatibility & Safety

- [ ] Define support window for JSON fallback (example: 2 releases or 30 days).
- [x] Add warning logs when JSON fallback is used (sampled to avoid log spam).
- [ ] Add alert threshold for fallback usage (example: >5% after rollout week 1).
- [x] Add feature flag/env gate for strict MessagePack mode (disable JSON fallback).

## Phase 5 - Performance Validation

- [ ] Baseline current JSON metrics:
  - [ ] payload size p50/p95
  - [ ] encode/decode latency p50/p95
  - [ ] CPU usage under representative load
- [ ] Collect same metrics with MessagePack enabled.
- [ ] Compare and publish delta:
  - [ ] bytes reduction
  - [ ] latency improvement/regression
  - [ ] CPU improvement/regression
- [ ] Validate no increase in decode/validation failures.

## Phase 6 - Cutover & Cleanup

- [ ] Announce final cutover date for removing JSON fallback.
- [ ] Enable strict MessagePack mode in non-prod, then prod.
- [ ] Remove JSON fallback decode branches from all services.
- [ ] Remove JSON-related compatibility logs/metrics no longer needed.
- [ ] Update architecture docs and onboarding notes to MessagePack-only.

## Test Matrix (Minimum)

- [ ] MessagePack request -> MessagePack response (happy path)
- [ ] Legacy JSON request -> MessagePack response (compat path)
- [ ] Invalid/corrupt MessagePack payload -> deterministic error response
- [ ] Invalid JSON payload -> deterministic error response
- [ ] Large payload boundary test (max expected command/event size)
- [ ] Multi-client concurrency test over REQ/REP
- [ ] Long-running PUB/SUB stability test (no decode leaks/crashes)

## Operational Playbook

- [ ] Dashboard panel for wire format mix by channel.
- [ ] Runbook entry: "decode errors spiking" triage steps.
- [ ] Rollback plan:
  - [ ] keep producer MessagePack but retain fallback decode
  - [ ] temporary client downgrade path documented
- [ ] Owner assigned for each dependent service migration task.

## Definition of Done

- [ ] All hot-path clients/consumers use MessagePack by default.
- [ ] JSON fallback usage remains at 0% for agreed stability window.
- [ ] Strict MessagePack mode enabled in production.
- [ ] JSON fallback code removed and docs updated.
