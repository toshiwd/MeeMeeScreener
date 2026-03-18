from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from threading import Lock
from typing import Any


logger = logging.getLogger(__name__)

_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, list[tuple[int, float, float, float, float, float]]]] = {}
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _enabled() -> bool:
    value = os.getenv("MEEMEE_YF_PROVISIONAL_ENABLED")
    if value is None:
        return True
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _timeout_sec() -> float:
    raw = os.getenv("MEEMEE_YF_TIMEOUT_SEC")
    try:
        value = float(raw) if raw is not None else 1.5
    except (TypeError, ValueError):
        value = 1.5
    return max(0.2, value)


def _cache_ttl_sec() -> float:
    raw = os.getenv("MEEMEE_YF_CACHE_TTL_SEC")
    try:
        value = float(raw) if raw is not None else 120.0
    except (TypeError, ValueError):
        value = 120.0
    return max(1.0, value)


def _history_range() -> str:
    value = str(os.getenv("MEEMEE_YF_HISTORY_RANGE") or "2y").strip()
    return value or "2y"


def _user_agent() -> str:
    value = str(os.getenv("MEEMEE_YF_USER_AGENT") or "").strip()
    return value or _DEFAULT_USER_AGENT


def _code_to_symbol(code: str) -> str | None:
    value = str(code or "").strip()
    if len(value) != 4 or not value.isdigit():
        return None
    return f"{value}.T"


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        iv = int(value)
    except (TypeError, ValueError):
        return None
    if iv >= 1_000_000_000_000:
        return iv // 1000
    return iv


def _normalize_row(row: tuple[Any, ...]) -> tuple[int, float, float, float, float, float] | None:
    if len(row) < 5:
        return None
    ts = _to_int(row[0])
    open_ = _to_float(row[1])
    high = _to_float(row[2])
    low = _to_float(row[3])
    close = _to_float(row[4])
    if ts is None or open_ is None or high is None or low is None or close is None:
        return None
    volume = _to_float(row[5]) if len(row) >= 6 else 0.0
    if volume is None:
        volume = 0.0
    return (ts, open_, high, low, close, volume)


def _fetch_json(url: str, *, timeout_sec: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json,text/plain,*/*",
            "User-Agent": _user_agent(),
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        payload = response.read()
    return json.loads(payload.decode("utf-8"))


def _extract_rows(payload: dict[str, Any]) -> list[tuple[int, float, float, float, float, float]]:
    chart = payload.get("chart") or {}
    results = chart.get("result") or []
    if not results:
        return []
    result = results[0] or {}
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote_list = indicators.get("quote") or []
    quote = quote_list[0] if quote_list else {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []

    rows: list[tuple[int, float, float, float, float, float]] = []
    for idx, ts in enumerate(timestamps):
        row = _normalize_row(
            (
                ts,
                opens[idx] if idx < len(opens) else None,
                highs[idx] if idx < len(highs) else None,
                lows[idx] if idx < len(lows) else None,
                closes[idx] if idx < len(closes) else None,
                volumes[idx] if idx < len(volumes) else 0.0,
            )
        )
        if row is not None:
            rows.append(row)
    return rows


def get_historical_daily_rows_from_chart(
    code: str,
    *,
    range_token: str | None = None,
) -> list[tuple[int, float, float, float, float, float]]:
    if not _enabled():
        return []
    symbol = _code_to_symbol(code)
    if symbol is None:
        return []
    resolved_range = str(range_token or _history_range()).strip() or "2y"
    cache_key = f"{symbol}:{resolved_range}"
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and cached[0] > now:
            return list(cached[1])

    rows: list[tuple[int, float, float, float, float, float]] = []
    try:
        params = urllib.parse.urlencode(
            {
                "interval": "1d",
                "range": resolved_range,
                "includePrePost": "false",
                "events": "div,splits",
            }
        )
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{params}"
        payload = _fetch_json(url, timeout_sec=max(_timeout_sec(), 3.0))
        rows = _extract_rows(payload)
    except Exception as exc:
        logger.debug("Yahoo history fetch failed for %s: %s", symbol, exc)
        rows = []

    with _CACHE_LOCK:
        _CACHE[cache_key] = (time.monotonic() + _cache_ttl_sec(), list(rows))
    return list(rows)
