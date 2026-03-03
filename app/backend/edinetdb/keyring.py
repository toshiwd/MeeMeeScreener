from __future__ import annotations

from threading import Lock
from typing import Any

from app.backend.edinetdb.client import EdinetdbClient, RateLimitStop


class KeyRingClient:
    def __init__(self, clients: list[EdinetdbClient]):
        if not clients:
            raise ValueError("clients must not be empty")
        self._clients = clients
        self._active_idx = 0
        self._lock = Lock()
        self._rate_limited: set[int] = set()

    def _pick_client(self) -> tuple[int, EdinetdbClient] | None:
        with self._lock:
            n = len(self._clients)
            for offset in range(n):
                idx = (self._active_idx + offset) % n
                if idx in self._rate_limited:
                    continue
                self._active_idx = idx
                return idx, self._clients[idx]
            return None

    def _mark_rate_limited(self, idx: int) -> None:
        with self._lock:
            self._rate_limited.add(idx)
            n = len(self._clients)
            for offset in range(1, n + 1):
                nxt = (idx + offset) % n
                if nxt not in self._rate_limited:
                    self._active_idx = nxt
                    return

    def get_json(self, path: str, params: dict[str, Any] | None = None):
        last_rate_limit: RateLimitStop | None = None
        for _ in range(len(self._clients)):
            selected = self._pick_client()
            if selected is None:
                break
            idx, client = selected
            try:
                return client.get_json(path, params)
            except RateLimitStop as exc:
                self._mark_rate_limited(idx)
                last_rate_limit = exc
                continue
        if last_rate_limit is not None:
            raise last_rate_limit
        raise RuntimeError("no_available_client")
