"""nodus-gateway — WebSocket coordination hub for Nodus components.

Server:
    GatewayServer   — WebSocket server; register handlers, start/stop
    HandlerRegistry — map method names to async handler functions
    JobResultCache  — idempotent result cache with TTL
    EventBroadcaster — distribute execution events to subscribers

Client:
    GatewayClient   — connect, send requests, receive responses, subscribe events
"""
from .broadcast import EventBroadcaster
from .cache import JobResultCache
from .client import GatewayClient
from .handlers import HandlerRegistry
from .server import GatewayServer

__all__ = [
    "GatewayServer",
    "GatewayClient",
    "HandlerRegistry",
    "JobResultCache",
    "EventBroadcaster",
]
