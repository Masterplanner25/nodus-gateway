"""Handler registry — map method names to async handler functions."""
from __future__ import annotations

import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

HandlerFn = Callable[[dict[str, Any], Any], Any]


class HandlerRegistry:
    """Maps gateway method names to async handler functions.

    Handlers have the signature::

        async def my_handler(params: dict, context: Any) -> dict:
            ...

    Usage::

        registry = HandlerRegistry()

        @registry.register("agent.run")
        async def handle_agent_run(params, ctx):
            return {"run_id": "..."}
    """

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFn] = {}

    def register(self, method: str) -> Callable[[HandlerFn], HandlerFn]:
        """Decorator: ``@registry.register("method.name")``."""
        def decorator(fn: HandlerFn) -> HandlerFn:
            self._handlers[method] = fn
            return fn
        return decorator

    def add(self, method: str, fn: HandlerFn) -> None:
        """Imperative registration: ``registry.add("method.name", handler)``."""
        self._handlers[method] = fn

    def get(self, method: str) -> Optional[HandlerFn]:
        return self._handlers.get(method)

    def methods(self) -> list[str]:
        return list(self._handlers.keys())

    def __contains__(self, method: str) -> bool:
        return method in self._handlers
