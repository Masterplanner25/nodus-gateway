"""nodus-gateway tests — using in-process WebSocket connections."""
import asyncio
import json
import time
import uuid

import pytest

from nodus_gateway import (
    EventBroadcaster, GatewayClient, GatewayServer,
    HandlerRegistry, JobResultCache,
)


# ── JobResultCache ────────────────────────────────────────────────────────────

def test_cache_set_and_get():
    cache = JobResultCache(ttl_seconds=60)
    cache.set("key-1", {"result": "ok"}, ok=True)
    entry = cache.get("key-1")
    assert entry is not None
    assert entry.ok is True
    assert entry.result == {"result": "ok"}


def test_cache_miss_returns_none():
    cache = JobResultCache()
    assert cache.get("nonexistent") is None


def test_cache_ttl_expiry():
    cache = JobResultCache(ttl_seconds=0.01)
    cache.set("key", "value")
    time.sleep(0.05)
    assert cache.get("key") is None


def test_cache_evict_expired():
    cache = JobResultCache(ttl_seconds=0.01)
    cache.set("a", 1)
    cache.set("b", 2)
    time.sleep(0.05)
    count = cache.evict_expired()
    assert count == 2
    assert len(cache) == 0


def test_cache_max_size_evicts_oldest():
    cache = JobResultCache(ttl_seconds=60, max_size=2)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)   # should evict "a"
    assert len(cache) == 2


# ── HandlerRegistry ───────────────────────────────────────────────────────────

def test_handler_register_and_get():
    reg = HandlerRegistry()
    async def my_handler(params, ctx): return {}
    reg.add("agent.run", my_handler)
    assert reg.get("agent.run") is my_handler


def test_handler_decorator():
    reg = HandlerRegistry()
    @reg.register("session.get")
    async def handler(params, ctx): return {}
    assert "session.get" in reg
    assert reg.get("session.get") is handler


def test_handler_unknown_returns_none():
    assert HandlerRegistry().get("unknown") is None


def test_handler_methods():
    reg = HandlerRegistry()
    reg.add("a.b", lambda p, c: {})
    reg.add("c.d", lambda p, c: {})
    assert set(reg.methods()) == {"a.b", "c.d"}


# ── EventBroadcaster ──────────────────────────────────────────────────────────

def test_broadcast_to_specific_run():
    received = []
    broadcaster = EventBroadcaster()
    broadcaster.subscribe(received.append, run_id="run-1")
    count = broadcaster.broadcast("agent.block", "run-1", {"text": "hello"})
    assert count == 1
    msg = json.loads(received[0])
    assert msg["event_type"] == "agent.block"
    assert msg["run_id"] == "run-1"


def test_broadcast_global_subscriber():
    received = []
    broadcaster = EventBroadcaster()
    broadcaster.subscribe(received.append, run_id=None)   # global
    broadcaster.broadcast("agent.block", "any-run", {})
    assert len(received) == 1


def test_broadcast_no_subscribers_returns_zero():
    assert EventBroadcaster().broadcast("evt", "run-1", {}) == 0


def test_unsubscribe():
    received = []
    broadcaster = EventBroadcaster()
    broadcaster.subscribe(received.append, run_id="r1")
    broadcaster.unsubscribe(received.append, run_id="r1")
    broadcaster.broadcast("evt", "r1", {})
    assert len(received) == 0


def test_subscriber_count():
    b = EventBroadcaster()
    b.subscribe(lambda m: None, run_id="r1")
    b.subscribe(lambda m: None, run_id="r1")
    assert b.subscriber_count("r1") == 2


# ── GatewayServer (in-process) ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_server_handles_registered_method():
    """Test server handler dispatch using direct _handle_message calls."""
    server = GatewayServer()
    results = []

    async def echo_handler(params, ctx):
        results.append(params)
        return {"echo": params.get("message")}

    server.register_handler("echo", echo_handler)

    # Build a valid request JSON matching GatewayServer._handle_message expectations
    request_json = json.dumps({
        "_type": "request",
        "id": "req-1",
        "method": "echo",
        "params": {"message": "hello"},
        "idempotency_key": "idem-1",
        "protocol_version": "1.0",
    })

    response = await server._handle_message(request_json)
    assert response["ok"] is True
    assert response["result"]["echo"] == "hello"
    assert results[0]["message"] == "hello"


@pytest.mark.asyncio
async def test_server_unknown_method_returns_error():
    server = GatewayServer()
    request_json = json.dumps({
        "_type": "request", "id": "req-1",
        "method": "no.such.method", "params": {},
        "idempotency_key": "idem-1", "protocol_version": "1.0",
    })
    response = await server._handle_message(request_json)
    assert response["ok"] is False
    assert "no.such.method" in response["error"]


@pytest.mark.asyncio
async def test_server_idempotency_cache():
    server = GatewayServer()
    call_count = [0]

    async def counting_handler(params, ctx):
        call_count[0] += 1
        return {"count": call_count[0]}

    server.register_handler("count", counting_handler)
    request_json = json.dumps({
        "_type": "request", "id": "req-1",
        "method": "count", "params": {},
        "idempotency_key": "idem-same", "protocol_version": "1.0",
    })

    r1 = await server._handle_message(request_json)
    r2 = await server._handle_message(request_json)   # same idempotency_key

    assert r1["ok"] is True
    assert r1["result"]["count"] == 1
    assert r2.get("_cached") is True    # served from cache
    assert call_count[0] == 1           # handler only called once


@pytest.mark.asyncio
async def test_server_handler_exception_returns_error():
    server = GatewayServer()

    async def failing_handler(params, ctx):
        raise ValueError("deliberate error")

    server.register_handler("fail", failing_handler)
    request_json = json.dumps({
        "_type": "request", "id": "req-1",
        "method": "fail", "params": {},
        "idempotency_key": "idem-fail", "protocol_version": "1.0",
    })
    response = await server._handle_message(request_json)
    assert response["ok"] is False
    assert "deliberate error" in response["error"]


# ── Live WebSocket integration ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_client_server_roundtrip():
    """Start a real server, connect a real client, exchange one message."""
    server = GatewayServer(host="127.0.0.1", port=19999)

    async def greet_handler(params, ctx):
        return {"greeting": f"Hello, {params.get('name', 'world')}!"}

    server.register_handler("greet", greet_handler)
    await server.start()

    try:
        async with GatewayClient("ws://127.0.0.1:19999") as client:
            response = await client.send("greet", {"name": "Nodus"})
        assert response["ok"] is True
        assert response["result"]["greeting"] == "Hello, Nodus!"
    finally:
        await server.stop()
