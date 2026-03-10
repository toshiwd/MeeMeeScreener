from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterable, Sequence


logger = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_BATCH_MAX_SYMBOLS_PER_REQUEST = 50
_SPLIT_FACTOR_CANDIDATES = (1.5, *tuple(float(v) for v in range(2, 31)))
_SPLIT_GAP_MIN = 0.28
_SPLIT_RATIO_TOLERANCE = 0.08
_SPLIT_OHLC_TOLERANCE = 0.20
_CACHE_MISS = object()
_cache_lock = Lock()
_chart_cache: dict[str, tuple[float, tuple[int, float, float, float, float, float] | None]] = {}
_spark_cache: dict[str, tuple[float, tuple[int, float, float, float, float, float] | None]] = {}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float, minimum: float) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except (TypeError, ValueError):
        value = float(default)
    return max(minimum, value)


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, value)


def _enabled() -> bool:
    return _env_bool("MEEMEE_YF_PROVISIONAL_ENABLED", True)


def _timeout_sec() -> float:
    return _env_float("MEEMEE_YF_TIMEOUT_SEC", 1.5, minimum=0.2)


def _cache_ttl_sec() -> float:
    return _env_float("MEEMEE_YF_CACHE_TTL_SEC", 120.0, minimum=1.0)


def _spark_chunk_size() -> int:
    return _env_int("MEEMEE_YF_SPARK_CHUNK_SIZE", 50, minimum=1)


def _chart_max_workers() -> int:
    return _env_int("MEEMEE_YF_CHART_MAX_WORKERS", 8, minimum=1)


def _user_agent() -> str:
    value = str(os.getenv("MEEMEE_YF_USER_AGENT") or "").strip()
    return value or _DEFAULT_USER_AGENT


def code_to_yahoo_symbol(code: str) -> str | None:
    value = str(code or "").strip()
    if len(value) != 4 or not value.isdigit():
        return None
    return f"{value}.T"


def normalize_date_key(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        iv = int(value)
        if iv >= 1_000_000_000_000:
            iv //= 1000
        if iv >= 1_000_000_000:
            try:
                return int(datetime.fromtimestamp(iv, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return None
        if 19_000_101 <= iv <= 21_001_231:
            return iv
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        if len(text) == 8:
            iv = int(text)
            if 19_000_101 <= iv <= 21_001_231:
                return iv
            return None
        if len(text) >= 10:
            try:
                return normalize_date_key(int(text))
            except ValueError:
                return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return int(datetime.strptime(text, fmt).strftime("%Y%m%d"))
        except ValueError:
            continue
    return None


def merge_daily_rows_with_provisional(
    rows: Iterable[Sequence[Any]],
    provisional_row: Sequence[Any] | None,
    *,
    asof_dt: int | None = None,
) -> list[tuple]:
    base_rows = [tuple(row) for row in rows]
    if not provisional_row:
        return base_rows
    if len(provisional_row) < 5:
        return base_rows

    next_key = normalize_date_key(provisional_row[0])
    if next_key is None:
        return base_rows

    asof_key = normalize_date_key(asof_dt) if asof_dt is not None else None
    if asof_key is not None and next_key > asof_key:
        return base_rows

    last_key: int | None = None
    last_idx: int | None = None
    for idx in range(len(base_rows) - 1, -1, -1):
        row = base_rows[idx]
        if not row:
            continue
        last_key = normalize_date_key(row[0])
        if last_key is not None:
            last_idx = idx
            break

    normalized = _normalize_ohlcv_row(provisional_row)
    if normalized is None:
        return base_rows
    provisional_is_close_only = is_close_only_zero_volume_row(normalized)

    if last_key is not None and next_key < last_key:
        return base_rows
    if last_key is not None and next_key == last_key:
        if provisional_is_close_only:
            # Keep current same-day row; close-only payload has no extra OHLC/volume detail.
            return base_rows
        if last_idx is not None and is_close_only_zero_volume_row(base_rows[last_idx]):
            base_rows[last_idx] = normalized
        return base_rows

    base_rows.append(normalized)
    return base_rows


def apply_split_gap_adjustment(rows: Iterable[Sequence[Any]]) -> list[tuple]:
    base_rows = [tuple(row) for row in rows]
    if len(base_rows) < 2:
        return base_rows

    adjusted: list[tuple] = []
    cumulative_factor = 1.0
    prev_ohlc: tuple[float, float, float, float] | None = None
    for row in base_rows:
        if len(row) < 5:
            adjusted.append(row)
            continue

        open_raw = _to_float(row[1])
        high_raw = _to_float(row[2])
        low_raw = _to_float(row[3])
        close_raw = _to_float(row[4])
        if open_raw is None or high_raw is None or low_raw is None or close_raw is None:
            adjusted.append(row)
            continue

        current_ohlc = (
            open_raw * cumulative_factor,
            high_raw * cumulative_factor,
            low_raw * cumulative_factor,
            close_raw * cumulative_factor,
        )
        if prev_ohlc is not None and prev_ohlc[3] > 0 and current_ohlc[3] > 0:
            correction = _estimate_split_correction_factor(prev_ohlc, current_ohlc)
            if correction is not None:
                cumulative_factor *= correction
                current_ohlc = (
                    open_raw * cumulative_factor,
                    high_raw * cumulative_factor,
                    low_raw * cumulative_factor,
                    close_raw * cumulative_factor,
                )

        row_out = list(row)
        row_out[1] = current_ohlc[0]
        row_out[2] = current_ohlc[1]
        row_out[3] = current_ohlc[2]
        row_out[4] = current_ohlc[3]
        adjusted.append(tuple(row_out))
        prev_ohlc = current_ohlc

    return adjusted


def get_provisional_daily_row_from_chart(code: str) -> tuple[int, float, float, float, float, float] | None:
    if not _enabled():
        return None
    symbol = code_to_yahoo_symbol(code)
    if symbol is None:
        return None

    cached = _cache_get(_chart_cache, symbol)
    if cached is not _CACHE_MISS:
        return cached

    row: tuple[int, float, float, float, float, float] | None = None
    try:
        params = urllib.parse.urlencode(
            {
                "interval": "1d",
                "range": "10d",
                "includePrePost": "false",
                "events": "div,splits",
            }
        )
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{params}"
        payload = _fetch_json(url, timeout_sec=_timeout_sec())
        row = _extract_row_from_chart_payload(payload)
    except Exception as exc:
        logger.debug("Yahoo chart provisional fetch failed for %s: %s", symbol, exc)
        row = None

    _cache_set(_chart_cache, symbol, row)
    return row


def get_provisional_daily_rows_from_spark(
    codes: Sequence[str],
    *,
    prefer_chart_ohlc: bool = False,
) -> dict[str, tuple[int, float, float, float, float, float]]:
    if not _enabled():
        return {}
    if not codes:
        return {}

    code_by_symbol: dict[str, str] = {}
    for code in codes:
        symbol = code_to_yahoo_symbol(str(code))
        if symbol is None or symbol in code_by_symbol:
            continue
        code_by_symbol[symbol] = str(code).strip()
    if not code_by_symbol:
        return {}

    if prefer_chart_ohlc:
        return _get_provisional_daily_rows_from_chart_symbols(code_by_symbol)

    resolved: dict[str, tuple[int, float, float, float, float, float]] = {}
    missing_symbols: list[str] = []
    for symbol, code in code_by_symbol.items():
        cached = _cache_get(_spark_cache, symbol)
        if cached is _CACHE_MISS:
            missing_symbols.append(symbol)
            continue
        if cached is not None:
            resolved[code] = cached

    if missing_symbols:
        chunk_size = min(_spark_chunk_size(), _BATCH_MAX_SYMBOLS_PER_REQUEST)
        for start in range(0, len(missing_symbols), chunk_size):
            chunk = missing_symbols[start : start + chunk_size]
            parsed = _fetch_spark_chunk(chunk)
            for symbol in chunk:
                row = parsed.get(symbol)
                _cache_set(_spark_cache, symbol, row)
                if row is not None:
                    resolved[code_by_symbol[symbol]] = row

    return resolved


def _get_provisional_daily_rows_from_chart_symbols(
    code_by_symbol: dict[str, str],
) -> dict[str, tuple[int, float, float, float, float, float]]:
    resolved: dict[str, tuple[int, float, float, float, float, float]] = {}
    missing_symbols: list[str] = []
    for symbol, code in code_by_symbol.items():
        cached = _cache_get(_chart_cache, symbol)
        if cached is _CACHE_MISS:
            missing_symbols.append(symbol)
            continue
        if cached is not None:
            resolved[code] = cached

    if missing_symbols:
        workers = min(_chart_max_workers(), len(missing_symbols))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(_fetch_chart_row_for_symbol, symbol): symbol for symbol in missing_symbols
            }
            for future in as_completed(future_map):
                symbol = future_map[future]
                try:
                    row = future.result()
                except Exception as exc:
                    logger.debug("Yahoo chart batch provisional fetch failed for %s: %s", symbol, exc)
                    row = None
                _cache_set(_chart_cache, symbol, row)
                if row is not None:
                    resolved[code_by_symbol[symbol]] = row

    return resolved


def _normalize_ohlcv_row(row: Sequence[Any]) -> tuple[int, float, float, float, float, float] | None:
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


def is_close_only_zero_volume_row(row: Sequence[Any]) -> bool:
    if len(row) < 6:
        return False
    open_ = _to_float(row[1])
    high = _to_float(row[2])
    low = _to_float(row[3])
    close = _to_float(row[4])
    volume = _to_float(row[5])
    if open_ is None or high is None or low is None or close is None or volume is None:
        return False
    eps = 1e-9
    return (
        abs(open_ - high) <= eps
        and abs(high - low) <= eps
        and abs(low - close) <= eps
        and abs(volume) <= eps
    )


# Backward-compatible private alias.
def _is_close_only_zero_volume_row(row: Sequence[Any]) -> bool:
    return is_close_only_zero_volume_row(row)


def _cache_get(
    cache: dict[str, tuple[float, tuple[int, float, float, float, float, float] | None]],
    key: str,
):
    now = time.monotonic()
    with _cache_lock:
        entry = cache.get(key)
        if not entry:
            return _CACHE_MISS
        expires_at, value = entry
        if expires_at < now:
            cache.pop(key, None)
            return _CACHE_MISS
        return value


def _cache_set(
    cache: dict[str, tuple[float, tuple[int, float, float, float, float, float] | None]],
    key: str,
    value: tuple[int, float, float, float, float, float] | None,
) -> None:
    expires_at = time.monotonic() + _cache_ttl_sec()
    with _cache_lock:
        cache[key] = (expires_at, value)


def _fetch_spark_chunk(
    symbols: Sequence[str],
) -> dict[str, tuple[int, float, float, float, float, float] | None]:
    if not symbols:
        return {}
    symbol_csv = ",".join(symbols)
    params = urllib.parse.urlencode({"symbols": symbol_csv, "range": "1d", "interval": "1d"})
    url = f"https://query1.finance.yahoo.com/v7/finance/spark?{params}"

    rows: dict[str, tuple[int, float, float, float, float, float] | None] = {}
    try:
        payload = _fetch_json(url, timeout_sec=_timeout_sec())
        result = ((payload.get("spark") or {}).get("result") or []) if isinstance(payload, dict) else []
        for item in result:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip()
            if not symbol:
                continue
            rows[symbol] = _extract_row_from_spark_item(item)
    except Exception as exc:
        logger.debug("Yahoo spark provisional fetch failed for %s symbols: %s", len(symbols), exc)
        for symbol in symbols:
            rows[symbol] = None
        return rows

    for symbol in symbols:
        rows.setdefault(symbol, None)
    return rows


def _fetch_chart_row_for_symbol(symbol: str) -> tuple[int, float, float, float, float, float] | None:
    params = urllib.parse.urlencode(
        {
            "interval": "1d",
            "range": "10d",
            "includePrePost": "false",
            "events": "div,splits",
        }
    )
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?{params}"
    try:
        payload = _fetch_json(url, timeout_sec=_timeout_sec())
        return _extract_row_from_chart_payload(payload)
    except Exception as exc:
        logger.debug("Yahoo chart fallback provisional fetch failed for %s: %s", symbol, exc)
        return None


def _extract_row_from_chart_payload(payload: dict[str, Any] | None) -> tuple[int, float, float, float, float, float] | None:
    if not isinstance(payload, dict):
        return None
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    if not isinstance(result, dict):
        return None
    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [None])[0]) or {}
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    closes = quote.get("close") or []
    volumes = quote.get("volume") or []
    for idx in range(len(timestamps) - 1, -1, -1):
        ts = _to_int(_value_at(timestamps, idx))
        open_ = _to_float(_value_at(opens, idx))
        high = _to_float(_value_at(highs, idx))
        low = _to_float(_value_at(lows, idx))
        close = _to_float(_value_at(closes, idx))
        if ts is None or open_ is None or high is None or low is None or close is None:
            continue
        volume = _to_pan_volume_unit(_to_float(_value_at(volumes, idx)))
        return (ts, open_, high, low, close, volume)
    return None


def _extract_row_from_spark_item(item: dict[str, Any]) -> tuple[int, float, float, float, float, float] | None:
    response = (item.get("response") or [None])[0]
    if not isinstance(response, dict):
        return None
    timestamps = response.get("timestamp") or []
    quote = (((response.get("indicators") or {}).get("quote") or [None])[0]) or {}
    closes = quote.get("close") or []
    for idx in range(len(timestamps) - 1, -1, -1):
        ts = _to_int(_value_at(timestamps, idx))
        close = _to_float(_value_at(closes, idx))
        if ts is None or close is None:
            continue
        return (ts, close, close, close, close, 0.0)
    return None



def _fetch_json(url: str, *, timeout_sec: float) -> dict[str, Any] | None:
    req = urllib.request.Request(url, headers={"User-Agent": _user_agent()})
    with urllib.request.urlopen(req, timeout=timeout_sec) as response:
        body = response.read()
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def _value_at(values: Sequence[Any], index: int) -> Any:
    if index < 0 or index >= len(values):
        return None
    return values[index]


def _to_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed > 1_000_000_000_000:
        parsed //= 1000
    return parsed


def _to_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _to_pan_volume_unit(value: float | None) -> float:
    if value is None:
        return 0.0
    # PAN daily_bars.v uses thousand-share unit (千株). Yahoo is shares.
    return float(int(round(float(value) / 1000.0)))


def _estimate_split_correction_factor(
    prev_ohlc: tuple[float, float, float, float],
    current_ohlc: tuple[float, float, float, float],
) -> float | None:
    prev_close = prev_ohlc[3]
    current_close = current_ohlc[3]
    if prev_close != prev_close or current_close != current_close:
        return None
    if prev_close <= 0 or current_close <= 0:
        return None

    ratio = current_close / prev_close
    if ratio != ratio or ratio <= 0:
        return None
    if abs(1.0 - ratio) < _SPLIT_GAP_MIN:
        return None

    best_correction: float | None = None
    best_error: float | None = None
    for factor in _SPLIT_FACTOR_CANDIDATES:
        correction = factor if ratio < 1.0 else (1.0 / factor)
        close_error = abs((ratio * correction) - 1.0)
        if close_error > _SPLIT_RATIO_TOLERANCE:
            continue

        component_errors: list[float] = []
        valid = True
        for idx in range(4):
            prev_value = prev_ohlc[idx]
            curr_value = current_ohlc[idx]
            if prev_value <= 0 or curr_value <= 0:
                valid = False
                break
            component_errors.append(abs(((curr_value * correction) / prev_value) - 1.0))
        if not valid or not component_errors:
            continue
        if max(component_errors) > _SPLIT_OHLC_TOLERANCE:
            continue

        score = close_error + (sum(component_errors) / float(len(component_errors)))
        if best_error is None or score < best_error:
            best_error = score
            best_correction = correction

    return best_correction


def _clear_caches_for_tests() -> None:
    with _cache_lock:
        _chart_cache.clear()
        _spark_cache.clear()
