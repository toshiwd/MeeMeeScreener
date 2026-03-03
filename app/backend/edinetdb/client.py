from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable


class ApiError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status = status
        self.body = body


class RetryableApiError(ApiError):
    pass


class RateLimitStop(ApiError):
    pass


@dataclass
class ApiResponse:
    status: int
    payload: Any
    url: str


class EdinetdbClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str,
        timeout_sec: int = 20,
        max_retries: int = 3,
        on_attempt: Callable[[], None] | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._max_retries = max(0, int(max_retries))
        self._on_attempt = on_attempt

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> ApiResponse:
        params = params or {}
        if path.startswith("http://") or path.startswith("https://"):
            base = path
        else:
            p = path if path.startswith("/") else f"/{path}"
            base = f"{self._base_url}{p}"
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{base}?{query}" if query else base
        request = urllib.request.Request(
            url,
            headers={
                "X-API-Key": self._api_key,
                "Accept": "application/json",
                "User-Agent": "MeeMee-EDINETDB/1.0",
            },
        )

        for attempt in range(self._max_retries + 1):
            if self._on_attempt is not None:
                self._on_attempt()
            try:
                with urllib.request.urlopen(request, timeout=self._timeout_sec) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    payload = json.loads(raw) if raw else {}
                    return ApiResponse(status=int(resp.status), payload=payload, url=url)
            except urllib.error.HTTPError as exc:
                status = int(exc.code)
                body = exc.read().decode("utf-8", errors="replace")
                if status == 429:
                    raise RateLimitStop("rate_limited", status=status, body=body) from exc
                if 500 <= status < 600:
                    if attempt < self._max_retries:
                        time.sleep(2**attempt)
                        continue
                    raise RetryableApiError("server_error", status=status, body=body) from exc
                raise ApiError("client_error", status=status, body=body) from exc
            except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
                if attempt < self._max_retries:
                    time.sleep(2**attempt)
                    continue
                raise RetryableApiError(f"network_error:{exc}", status=None, body=None) from exc
            except json.JSONDecodeError as exc:
                raise ApiError("invalid_json_response", status=None, body=None) from exc

        raise RetryableApiError("unexpected_retry_exhaustion", status=None, body=None)
