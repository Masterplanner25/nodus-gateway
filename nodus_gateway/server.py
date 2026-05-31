"""GatewayServer — WebSocket coordination hub."""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

from .broadcast import EventBroadcaster
from .cache import JobResultCache
from .handlers import HandlerRegistry

try:
    import websockets  # noqa: F401
    _WS_AVAILABLE = True
except ImportError:
    _WS_AVAILABLE = False

logger = logging.getLogger(__name__)


class GatewayServer:
    """WebSocket coordination hub for Nodus components.

    All agents, channels, and tools connect here to exchange typed
    ``RequestEnvelope`` / ``ResponseEnvelope`` / ``EventEnvelope`` messages.

    Args:
        host:            Bind host (default: ``"127.0.0.1"``).
        port:            Bind port (default: 18789).
        handler_registry: Pre-configured handler registry.
        cache_ttl:       Job result cache TTL in seconds.
        broadcaster:     Optional EventBroadcaster for live event streaming.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 18789,
        handler_registry: Optional[HandlerRegistry] = None,
        cache_ttl: float = 30.0,
        broadcaster: Optional[EventBroadcaster] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._handlers = handler_registry or HandlerRegistry()
        self._cache = JobResultCache(ttl_seconds=cache_ttl)
        self._broadcaster = broadcaster or EventBroadcaster()
        self._server = None
        self._clients: set = set()

    def register_handler(self, method: str, fn) -> None:
        """Register an async handler for *method*."""
        self._handlers.add(method, fn)

    @property
    def broadcaster(self) -> EventBroadcaster:
        return self._broadcaster

    @property
    def cache(self) -> JobResultCache:
        return self._cache

    async def handle_connection(self, websocket) -> None:
        """Handle one WebSocket connection."""
        self._clients.add(websocket)
        try:
            async for raw_message in websocket:
                response = await self._handle_message(raw_message)
                await websocket.send(json.dumps(response))
        except Exception as exc:
            logger.debug("[GatewayServer] connection error: %s", exc)
        finally:
            self._clients.discard(websocket)

    async def _handle_message(self, raw: str) -> dict[str, Any]:
        try:
            from nodus_protocol import decode, ResponseEnvelope  # noqa: PLC0415
            request = decode(raw)
        except Exception as exc:
            return {"id": "unknown", "ok": False, "error": f"Parse error: {exc}"}

        # Idempotency check
        idem_key = getattr(request, "idempotency_key", None) or request.id
        cached = self._cache.get(idem_key)
        if cached is not None:
            return {
                "id": request.id,
                "ok": cached.ok,
                "result": cached.result,
                "_cached": True,
            }

        handler = self._handlers.get(request.method)
        if handler is None:
            result = {
                "id": request.id,
                "ok": False,
                "error": f"Unknown method: {request.method!r}",
            }
            self._cache.set(idem_key, None, ok=False)
            return result

        try:
            ctx = {"server": self, "request": request}
            handler_result = await handler(request.params, ctx)
            result = {"id": request.id, "ok": True, "result": handler_result}
            self._cache.set(idem_key, handler_result, ok=True)
            return result
        except Exception as exc:
            logger.warning("[GatewayServer] handler error for %r: %s", request.method, exc)
            result = {"id": request.id, "ok": False, "error": str(exc)}
            self._cache.set(idem_key, None, ok=False)
            return result

    async def start(self) -> None:
        """Start the WebSocket server (non-blocking — returns when server is ready)."""
        if not _WS_AVAILABLE:
            raise ImportError(
                "websockets package required. Install with: pip install nodus-gateway"
            )
        import websockets.server  # noqa: PLC0415
        self._server = await websockets.server.serve(
            self.handle_connection,
            self._host,
            self._port,
        )
        logger.info("[GatewayServer] listening on ws://%s:%d", self._host, self._port)

    async def stop(self) -> None:
        """Shut down the WebSocket server gracefully."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
            logger.info("[GatewayServer] stopped")

    @property
    def client_count(self) -> int:
        return len(self._clients)
