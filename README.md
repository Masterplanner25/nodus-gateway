# nodus-gateway

**WebSocket coordination hub for Nodus AI systems.**

A typed message exchange point where agents, channels, and tools connect to
send `RequestEnvelope` / `ResponseEnvelope` / `EventEnvelope` messages.
Provides handler dispatch, idempotency caching, event broadcasting, and an
async client. No required dependencies — WebSocket transport is optional.

> **Status:** v0.1.0 — prepared, not yet published.

---

## Install

```bash
pip install nodus-gateway

# With WebSocket transport:
pip install "nodus-gateway[websockets]"
```

---

## What it provides

| Component | Purpose |
|---|---|
| `GatewayServer` | WebSocket server; registers handlers, dispatches requests |
| `HandlerRegistry` | Maps method names → async handler functions |
| `JobResultCache` | Idempotent result cache with TTL (dedup concurrent requests) |
| `EventBroadcaster` | Fan-out execution events to all subscribers |
| `GatewayClient` | Async client for connecting to a GatewayServer |

---

## GatewayServer

```python
from nodus_gateway import GatewayServer, HandlerRegistry

registry = HandlerRegistry()

@registry.register("agent.run")
async def handle_agent_run(params: dict) -> dict:
    return {"status": "ok", "run_id": "run-123"}

server = GatewayServer(host="127.0.0.1", port=18789, registry=registry)
await server.start()   # blocks until stopped
# await server.stop()
```

Requires `pip install "nodus-gateway[websockets]"` for the WebSocket transport.
Without `websockets` installed, `GatewayServer` raises `ImportError` on start.

---

## HandlerRegistry

```python
from nodus_gateway import HandlerRegistry

registry = HandlerRegistry()

# Decorator form
@registry.register("memory.read")
async def handle_memory_read(params: dict) -> dict:
    return {"value": "..."}

# Imperative form
registry.register("memory.write", handle_write)

handler = registry.get("memory.read")   # callable | None
methods = registry.list_methods()        # list[str]
registry.unregister("memory.read")
```

---

## JobResultCache

```python
from nodus_gateway import JobResultCache

cache = JobResultCache(ttl_seconds=300)

# Store and retrieve results by job ID
cache.set("job-abc", {"result": "done"})
result = cache.get("job-abc")    # dict | None (None if expired)
cache.evict_expired()            # remove expired entries
len(cache)
```

Used internally by `GatewayServer` to make concurrent duplicate requests
idempotent — only the first call executes; subsequent calls get the cached result.

---

## EventBroadcaster

```python
from nodus_gateway import EventBroadcaster

broadcaster = EventBroadcaster()

def my_handler(event: dict) -> None:
    print(event["type"], event["payload"])

sub_id = broadcaster.subscribe(my_handler)
broadcaster.broadcast({"type": "flow.completed", "payload": {"id": "f1"}})
broadcaster.unsubscribe(sub_id)
```

---

## GatewayClient

```python
from nodus_gateway import GatewayClient

async with GatewayClient("ws://127.0.0.1:18789") as client:
    response = await client.send("agent.run", {"objective": "hello"})
    # response is the handler's return value

    # Subscribe to events
    client.subscribe(lambda event: print(event))
```

Requires `websockets` for the actual WebSocket connection.

---

## Design

- **No required dependencies.** Core components (HandlerRegistry, JobResultCache,
  EventBroadcaster) are pure stdlib. WebSocket transport is opt-in.
- **Thread-safe.** `JobResultCache` and `EventBroadcaster` use `threading.Lock`.
- **Idempotent dispatch.** `JobResultCache` deduplicates concurrent requests
  with the same job ID.

---

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q
```

---

## License

MIT — see [LICENSE](LICENSE).
