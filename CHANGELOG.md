# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.1] — 2026-07-12

### Fixed

- **Migrated to the `websockets.asyncio` API.** `GatewayClient` and
  `GatewayServer` used the deprecated top-level `websockets.connect` /
  `websockets.server.serve` entry points, which were removed in `websockets`
  14+. They now use `websockets.asyncio.client.connect` and
  `websockets.asyncio.server.serve`, so the package works against current
  `websockets` releases.

### Changed

- **`websockets` extra pinned to `>=14.0,<17`** (was `>=12.0`) — 14.0 is the
  first release with the `asyncio` API; upper bound guards against a future
  breaking major.
- Install hints now point at the `nodus-gateway[websockets]` extra rather than
  bare `nodus-gateway`.

---

## [0.1.0] — 2026-05-31

Initial release.

### Added

- **`HandlerRegistry`** — maps method name strings to async handler functions.
  `register(method, fn)` (decorator and imperative forms). `get(method)`,
  `list_methods()`, `unregister(method)`.

- **`JobResultCache`** — idempotent result cache with TTL. `set(job_id, result)`,
  `get(job_id)` (returns `None` if expired), `evict_expired()`, `len`.
  Thread-safe via `threading.Lock`.

- **`EventBroadcaster`** — fan-out event distribution. `subscribe(handler)`
  returns a subscription ID. `unsubscribe(sub_id)`. `broadcast(event)` delivers
  to all active subscribers. Thread-safe.

- **`GatewayServer`** — WebSocket coordination hub. `start()` / `stop()`.
  Uses `HandlerRegistry` for dispatch, `JobResultCache` for idempotency.
  Requires `websockets>=12.0` (`[websockets]` extra); raises `ImportError`
  gracefully when not installed (`_WS_AVAILABLE` guard).

- **`GatewayClient`** — async WebSocket client. Context manager
  (`async with GatewayClient(url) as client`). `send(method, params)` →
  response dict. `subscribe(handler)` for incoming events.

- **19 tests** in `tests/test_gateway.py`.

- **No required dependencies** — pure stdlib. Optional `[websockets]` extra
  adds `websockets>=12.0` for the transport layer.

[0.1.0]: https://github.com/Masterplanner25/nodus-gateway/releases/tag/v0.1.0
