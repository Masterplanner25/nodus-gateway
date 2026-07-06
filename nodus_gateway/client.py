"""GatewayClient — connect to a GatewayServer and exchange messages."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class GatewayClient:
    """Async client for the Nodus Gateway WebSocket server.

    Usage::

        async with GatewayClient("ws://127.0.0.1:18789") as client:
            response = await client.send("agent.run", {"objective": "hello"})
    """

    def __init__(
        self,
        url: str = "ws://127.0.0.1:18789",
        *,
        event_handler: Optional[Callable[[dict[str, Any]], None]] = None,
    ) -> None:
        self._url = url
        self._event_handler = event_handler
        self._ws = None
        self._pending: dict[str, asyncio.Future] = {}

    async def connect(self) -> None:
        try:
            import websockets.asyncio.client  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError("websockets required. pip install nodus-gateway[websockets]") from exc
        self._ws = await websockets.asyncio.client.connect(self._url)
        # Start background receiver
        asyncio.create_task(self._receive_loop())
        logger.info("[GatewayClient] connected to %s", self._url)

    async def disconnect(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    async def send(
        self,
        method: str,
        params: dict[str, Any],
        *,
        idempotency_key: Optional[str] = None,
    ) -> dict[str, Any]:
        """Send a request and wait for the response.

        Returns the response dict (``{"id", "ok", "result"/"error"}``).
        """
        if self._ws is None:
            raise RuntimeError("Not connected. Call connect() first.")

        req_id = str(uuid.uuid4())
        idem = idempotency_key or req_id
        message = json.dumps({
            "_type": "request",
            "id": req_id,
            "method": method,
            "params": params,
            "idempotency_key": idem,
            "protocol_version": "1.0",
        })

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[req_id] = future

        await self._ws.send(message)
        return await asyncio.wait_for(future, timeout=30.0)

    def subscribe_events(self, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register *handler* for server-pushed events."""
        self._event_handler = handler

    async def _receive_loop(self) -> None:
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                # Route to pending request or event handler
                req_id = msg.get("id")
                if req_id and req_id in self._pending:
                    future = self._pending.pop(req_id)
                    if not future.done():
                        future.set_result(msg)
                elif self._event_handler is not None:
                    try:
                        self._event_handler(msg)
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("[GatewayClient] receive loop ended: %s", exc)
            # Cancel all pending requests
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(ConnectionError("Connection closed"))
            self._pending.clear()

    async def __aenter__(self) -> "GatewayClient":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.disconnect()
