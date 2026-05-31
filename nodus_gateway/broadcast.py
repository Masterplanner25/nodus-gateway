"""Broadcast execution events to subscribed gateway clients."""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

BroadcastSubscriber = Callable[[str], None]   # receives JSON-encoded event string


class EventBroadcaster:
    """Distribute execution events to subscribed WebSocket clients.

    Clients subscribe by run_id or globally (run_id=None = all events).
    Thread-safe; designed for use with a WebSocket server.
    """

    def __init__(self) -> None:
        self._subscribers: dict[Optional[str], list[BroadcastSubscriber]] = {}
        self._lock = threading.Lock()

    def subscribe(self, subscriber: BroadcastSubscriber, run_id: Optional[str] = None) -> None:
        """Register *subscriber* for events from *run_id* (or all runs if None)."""
        with self._lock:
            if run_id not in self._subscribers:
                self._subscribers[run_id] = []
            self._subscribers[run_id].append(subscriber)

    def unsubscribe(self, subscriber: BroadcastSubscriber, run_id: Optional[str] = None) -> None:
        with self._lock:
            listeners = self._subscribers.get(run_id, [])
            try:
                listeners.remove(subscriber)
            except ValueError:
                pass

    def broadcast(self, event_type: str, run_id: str, payload: dict[str, Any]) -> int:
        """Emit an event to all relevant subscribers.

        Sends to:
        - subscribers registered for *run_id* specifically
        - global subscribers (registered with run_id=None)

        Returns the number of subscribers notified.
        """
        message = json.dumps({
            "event_type": event_type,
            "run_id": run_id,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        with self._lock:
            targets = list(self._subscribers.get(run_id, []))
            targets += list(self._subscribers.get(None, []))

        notified = 0
        for sub in targets:
            try:
                sub(message)
                notified += 1
            except Exception as exc:
                logger.debug("[EventBroadcaster] subscriber error: %s", exc)

        return notified

    def subscriber_count(self, run_id: Optional[str] = None) -> int:
        with self._lock:
            return len(self._subscribers.get(run_id, []))
