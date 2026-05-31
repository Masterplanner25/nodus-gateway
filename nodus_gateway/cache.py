"""Job result cache — idempotent response storage with TTL."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class CacheEntry:
    result: Any
    ok: bool
    created_at: float = field(default_factory=time.monotonic)


class JobResultCache:
    """In-memory cache of recent gateway job results.

    Used for idempotency: when a client retries a request with the same
    ``idempotency_key``, the cached result is returned without re-executing.

    Args:
        ttl_seconds: How long to keep results (default: 30s).
        max_size:    Maximum number of cached entries (default: 10 000).
    """

    def __init__(self, ttl_seconds: float = 30.0, max_size: int = 10_000) -> None:
        self._ttl = ttl_seconds
        self._max = max_size
        self._cache: dict[str, CacheEntry] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[CacheEntry]:
        with self._lock:
            entry = self._cache.get(key)
        if entry is None:
            return None
        if time.monotonic() - entry.created_at > self._ttl:
            with self._lock:
                self._cache.pop(key, None)
            return None
        return entry

    def set(self, key: str, result: Any, *, ok: bool = True) -> None:
        with self._lock:
            if len(self._cache) >= self._max:
                # Evict oldest entry
                oldest = min(self._cache, key=lambda k: self._cache[k].created_at)
                del self._cache[oldest]
            self._cache[key] = CacheEntry(result=result, ok=ok)

    def evict_expired(self) -> int:
        now = time.monotonic()
        with self._lock:
            expired = [k for k, v in self._cache.items() if now - v.created_at > self._ttl]
            for k in expired:
                del self._cache[k]
        return len(expired)

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)
