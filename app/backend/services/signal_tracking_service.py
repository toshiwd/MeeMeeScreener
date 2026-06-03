from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import duckdb
import pandas as pd

from app.backend.services.ml import rankings_cache
from app.core.config import config
from app.db.session import get_conn, get_conn_for_path
from shared.runtime_stock_db_contract import inspect_runtime_stock_db

logger = logging.getLogger(__name__)

WATCH_HORIZON_BARS = 30
COMPLETED_RETENTION_DAYS = 30
DEFAULT_LOOKBACK_DAYS = 120
DEFAULT_BACKFILL_LOOKBACK_DAYS = 252 * 3
DEFAULT_LIMIT = 200
DEFAULT_STATUS = "active"
DEFAULT_BASIS_VERSION = "basis:v1"
DEFAULT_LOGIC_VERSION = "logic:trade:v1"
ACTIVE_LOGIC_VERSION_ALIAS = "latest"
DEFAULT_RANKING_LOGIC_VERSION = "ranking:trade:top50:v1"
ACTIVE_RANKING_LOGIC_VERSION_ALIAS = "latest"
DEFAULT_RANKING_LIMIT = 50
SUPPORTED_STATUSES = {"active", "completed", "archive"}
SUPPORTED_SIDES = {"buy": "up", "sell": "down"}
SUPPORTED_RANKING_DIRS = {"up": "buy", "down": "sell"}
SUPPORTED_TRACKING_OUTCOMES = {"all", "good", "bad", "broken"}
SUPPORTED_TRACKING_SORTS = {"recent", "oldest", "best", "worst"}
DECISION_HORIZONS = (5, 10, 20, 30, 60)
BASIS_PAYLOAD_SCHEMA_VERSION = "signal-basis-payload:v1"
DEFAULT_REGIME_LABEL_VERSION = "v1"
SELL_TIGHTENED_LOGIC_VERSION = "logic:trade:v2-sell-tightened"
PRIMARY_HORIZON_BY_SIDE = {"buy": 20, "sell": 10}
SELL_WEAK_REGIME_TAGS = ("risk_off_trend", "high_vol_chaos", "neutral_range")
SHOCK_TRAILING_HORIZON = 20
SHOCK_DRAWDOWN_THRESHOLD = -0.10
SHOCK_BOTTOM_DECILE = 0.10
SHOCK_LOOKBACK_YEARS = 10
PROHIBITED_BASIS_KEY_TOKENS = (
    "forward_return",
    "future_window",
    "realized",
    "outcome_",
    "ret_h",
    "mfe_h",
    "mae_h",
    "days_to_mfe",
    "days_to_stop",
    "top_1pct",
    "top_3pct",
    "top_5pct",
    "leakage_group_id",
    "embargo_until_date",
    "purge_end_date",
    "label_",
)
_REFRESH_LOCK = threading.Lock()
_REGIME_BUILD_LOCK = threading.Lock()
_REFRESH_STATE = {
    "as_of": None,
    "refreshed_at": None,
}


@dataclass(frozen=True)
class DailyBar:
    date: int
    open: float | None
    high: float | None
    low: float | None
    close: float | None


TrackingProgressCallback = Callable[[dict[str, Any]], None]


def _tracking_heartbeat_at() -> str:
    return _serialize_timestamp(datetime.now(timezone.utc))


def _tracking_progress_event(
    *,
    phase: str,
    status: str,
    processed: int | None = None,
    total: int | None = None,
    current_market_ymd: int | None = None,
    current_market_date: str | None = None,
    detail: str | None = None,
    stage: str = "tracking_refresh",
    substage: str | None = None,
    current_side: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "stage": stage,
        "phase": str(phase),
        "status": str(status),
        "processed": int(processed) if processed is not None else None,
        "total": int(total) if total is not None else None,
        "current_market_ymd": int(current_market_ymd) if current_market_ymd is not None else None,
        "current_market_date": str(current_market_date) if current_market_date is not None else None,
        "current_side": str(current_side) if current_side is not None else None,
        "detail": str(detail) if detail is not None else None,
        "heartbeat_at": _tracking_heartbeat_at(),
    }
    if substage is not None:
        event["substage"] = str(substage)
    return event


def _emit_tracking_progress(
    progress_cb: TrackingProgressCallback | None,
    event: dict[str, Any],
    *,
    throttle_state: dict[str, Any] | None = None,
    force: bool = False,
    min_interval_sec: float = 2.0,
) -> None:
    if progress_cb is None:
        return
    now = time.monotonic()
    if throttle_state is not None:
        last_emit = float(throttle_state.get("last_emit_at") or 0.0)
        last_key = throttle_state.get("last_key")
        key = (
            event.get("stage"),
            event.get("substage"),
            event.get("phase"),
            event.get("status"),
            event.get("processed"),
            event.get("total"),
            event.get("current_market_ymd"),
            event.get("current_side"),
            event.get("detail"),
        )
        if not force and key == last_key and (now - last_emit) < min_interval_sec:
            return
        if not force and event.get("status") == "running" and (now - last_emit) < min_interval_sec:
            return
        throttle_state["last_emit_at"] = now
        throttle_state["last_key"] = key
    progress_cb(dict(event))


@dataclass(frozen=True)
class SignalOccurrence:
    occurrence_id: str
    campaign_id: str | None
    code: str
    side: str
    signal_date: int
    basis_version: str
    logic_version: str
    reason_snapshot_json: str | None
    score_snapshot_json: str | None
    entry_close_price: float | None
    entry_next_open_price: float | None


def _coerce_ymd(value: int | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if 19_000_101 <= value <= 21_001_231 else None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        text = text.replace("-", "")
    if not text.isdigit():
        return None
    ymd = int(text)
    return ymd if 19_000_101 <= ymd <= 21_001_231 else None


def _ymd_to_iso(ymd: int | None) -> str | None:
    if ymd is None:
        return None
    text = f"{int(ymd):08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _ymd_to_date(ymd: int | None) -> date | None:
    iso = _ymd_to_iso(ymd)
    if not iso:
        return None
    return date.fromisoformat(iso)


def _shift_ymd_by_days(ymd: int | None, days: int) -> int | None:
    base = _ymd_to_date(ymd)
    if base is None:
        return None
    return int((base + timedelta(days=int(days))).strftime("%Y%m%d"))


def _shift_ymd_by_years(ymd: int | None, years: int) -> int | None:
    base = _ymd_to_date(ymd)
    if base is None:
        return None
    target_year = base.year - int(years)
    try:
        return int(base.replace(year=target_year).strftime("%Y%m%d"))
    except ValueError:
        # うるう日は 2/28 に寄せる。
        return int(base.replace(year=target_year, month=2, day=28).strftime("%Y%m%d"))


def _json_dump(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_load(value: str | None) -> Any:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return None


def _serialize_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _bulk_insert_or_replace_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    columns: list[str],
    rows: list[list[Any]],
) -> None:
    if not rows:
        return
    frame = pd.DataFrame(rows, columns=columns)
    temp_name = f"_tmp_{table_name}_{threading.get_ident()}"
    conn.register(temp_name, frame)
    try:
        quoted_columns = ", ".join(columns)
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {temp_name}
            """
        )
    finally:
        try:
            conn.unregister(temp_name)
        except Exception:
            pass


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _safe_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if pd.notna(numeric):
            return numeric
    try:
        numeric = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return numeric if pd.notna(numeric) else None


def _safe_median(values: list[int | float]) -> float | None:
    if not values:
        return None
    return float(pd.Series(values).median())


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator / denominator)


def _first_bucket_for_day_index(day_index: int) -> str:
    if day_index <= 4:
        return "0-4"
    if day_index <= 9:
        return "5-9"
    if day_index <= 14:
        return "10-14"
    if day_index <= 19:
        return "15-19"
    return "20-29"


def _build_peak_day_buckets(values: list[int | None]) -> list[dict[str, Any]]:
    counts = {"0-4": 0, "5-9": 0, "10-14": 0, "15-19": 0, "20-29": 0}
    for value in values:
        if value is None:
            continue
        counts[_first_bucket_for_day_index(int(value))] += 1
    total = sum(counts.values())
    return [
        {
            "bucket": bucket,
            "count": count,
            "share": _safe_ratio(count, total),
        }
        for bucket, count in counts.items()
    ]


def _peak_day_metrics_dict(
    favorable_days: list[int | None],
    adverse_days: list[int | None],
) -> dict[str, Any]:
    return {
        "median_days_to_max_favorable_30": _safe_median([int(value) for value in favorable_days if value is not None]),
        "median_days_to_max_adverse_30": _safe_median([int(value) for value in adverse_days if value is not None]),
        "peak_day_buckets": _build_peak_day_buckets(favorable_days),
        "adverse_peak_day_buckets": _build_peak_day_buckets(adverse_days),
    }


def _profit_timing_bucket(day_index: int | None) -> str | None:
    if day_index is None:
        return None
    day_value = int(day_index)
    if day_value <= 10:
        return "10d型"
    if day_value <= 20:
        return "20d型"
    return "30d型"


def _build_profit_timing_patterns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = ("10d型", "20d型", "30d型")
    grouped: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in order}
    for row in rows:
        bucket = _profit_timing_bucket(row.get("days_to_max_favorable_30"))
        if not bucket:
            continue
        grouped.setdefault(bucket, []).append(row)
    total = sum(len(items) for items in grouped.values())
    out: list[dict[str, Any]] = []
    for bucket in order:
        items = grouped.get(bucket) or []
        returns_10 = [float(item["return_10"]) for item in items if isinstance(item.get("return_10"), (int, float))]
        returns_20 = [float(item["return_20"]) for item in items if isinstance(item.get("return_20"), (int, float))]
        returns_30 = [float(item["return_30"]) for item in items if isinstance(item.get("return_30"), (int, float))]
        out.append(
            {
                "bucket": bucket,
                "count": len(items),
                "share": _safe_ratio(len(items), total),
                "directional_hit_rate_10": _safe_ratio(sum(1 for value in returns_10 if value > 0), len(returns_10)),
                "directional_hit_rate_20": _safe_ratio(sum(1 for value in returns_20 if value > 0), len(returns_20)),
                "directional_hit_rate_30": _safe_ratio(sum(1 for value in returns_30 if value > 0), len(returns_30)),
                "average_directional_return_10": _safe_mean(returns_10),
                "average_directional_return_20": _safe_mean(returns_20),
                "average_directional_return_30": _safe_mean(returns_30),
            }
        )
    return out


_SCORE_THRESHOLD_KEYS: tuple[str, ...] = ("tradePriorityScore", "entryScore", "probSide")
_SCORE_THRESHOLD_CANDIDATES: tuple[float, ...] = tuple(round(value / 100.0, 2) for value in range(50, 96, 5))


def _build_score_threshold_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for score_key in _SCORE_THRESHOLD_KEYS:
        scored_rows: list[dict[str, Any]] = []
        for row in rows:
            score_value = row.get(score_key)
            if not isinstance(score_value, (int, float)):
                continue
            scored_rows.append({**row, "score_value": float(score_value)})
        if not scored_rows:
            continue
        total_count = len(scored_rows)
        for threshold in _SCORE_THRESHOLD_CANDIDATES:
            selected_rows = [row for row in scored_rows if float(row["score_value"]) >= threshold]
            returns_30 = [float(row["return_30"]) for row in selected_rows if isinstance(row.get("return_30"), (int, float))]
            baselines_30 = [float(row["baseline_30"]) for row in selected_rows if isinstance(row.get("baseline_30"), (int, float))]
            avg_return_30 = _safe_mean(returns_30)
            avg_baseline_30 = _safe_mean(baselines_30)
            out.append(
                {
                    "score_key": score_key,
                    "threshold": threshold,
                    "count": len(selected_rows),
                    "coverage_rate": _safe_ratio(len(selected_rows), total_count),
                    "directional_hit_rate_30": _safe_ratio(sum(1 for value in returns_30 if value > 0), len(returns_30)),
                    "average_directional_return_30": avg_return_30,
                    "same_date_universe_average_directional_return_30": avg_baseline_30,
                    "lift_vs_same_date_universe_30": (
                        None if avg_return_30 is None or avg_baseline_30 is None else float(avg_return_30 - avg_baseline_30)
                    ),
                }
            )
    return out


def _sorted_break_reason_counts(items: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    grouped: dict[str, int] = {}
    for item in items:
        if str(item.get("break_status") or "") != "broken":
            continue
        key = str(item.get("break_reason") or "unknown")
        grouped[key] = grouped.get(key, 0) + 1
    return [
        {"break_reason": key, "count": int(count)}
        for key, count in sorted(grouped.items(), key=lambda item: (-int(item[1]), item[0]))[:limit]
    ]


def _build_sell_subset_metrics(
    items: list[dict[str, Any]],
    *,
    subset_key: str,
    label: str,
    criteria: dict[str, Any],
    primary_horizon: int,
) -> dict[str, Any]:
    return_key = f"return_{int(primary_horizon)}d"
    returns = [float(item[return_key]) for item in items if isinstance(item.get(return_key), (int, float))]
    baselines = [float(item["baseline_primary"]) for item in items if isinstance(item.get("baseline_primary"), (int, float))]
    signal_mean = _safe_mean(returns)
    baseline_mean = _safe_mean(baselines)
    return {
        "subset_key": subset_key,
        "label": label,
        "criteria": criteria,
        "count": len(items),
        "campaign_count": len({str(item.get("campaign_id") or "") for item in items if str(item.get("campaign_id") or "").strip()}),
        "directional_hit_rate": _safe_ratio(sum(1 for value in returns if value > 0), len(returns)),
        "average_directional_return": signal_mean,
        "same_date_universe_average_directional_return": baseline_mean,
        "lift_vs_same_date_universe": (
            None if signal_mean is None or baseline_mean is None else float(signal_mean - baseline_mean)
        ),
        "break_rate": _safe_ratio(
            sum(1 for item in items if str(item.get("break_status") or "") == "broken"),
            len(items),
        ),
        "median_days_to_max_favorable_30": _safe_median(
            [int(item["days_to_max_favorable_30"]) for item in items if item.get("days_to_max_favorable_30") is not None]
        ),
        "median_days_to_max_adverse_30": _safe_median(
            [int(item["days_to_max_adverse_30"]) for item in items if item.get("days_to_max_adverse_30") is not None]
        ),
        "break_reason_counts": _sorted_break_reason_counts(items),
        "failure_examples": _signal_failure_examples(items, limit=5),
    }


def _build_sell_subset_comparison(
    items: list[dict[str, Any]],
    *,
    primary_horizon: int,
) -> dict[str, Any]:
    universe = list(items)
    subsets = [
        (
            "breakdown_only",
            "breakdown only",
            {"setup_types": ["breakdown"]},
            [item for item in universe if str(item.get("setup_type") or "").strip().lower() == "breakdown"],
        ),
        (
            "repeated_only",
            "repeated only",
            {"signal_count_min": 2},
            [item for item in universe if int(item.get("signal_count") or 1) > 1],
        ),
        (
            "breakdown_repeated",
            "breakdown + repeated",
            {"setup_types": ["breakdown"], "signal_count_min": 2},
            [
                item
                for item in universe
                if str(item.get("setup_type") or "").strip().lower() == "breakdown"
                and int(item.get("signal_count") or 1) > 1
            ],
        ),
        (
            "weak_regime_only",
            "weak regime only",
            {"regime_tags": list(SELL_WEAK_REGIME_TAGS)},
            [item for item in universe if str(item.get("regime_tag") or "") in SELL_WEAK_REGIME_TAGS],
        ),
    ]
    return {
        "version": 1,
        "side": "sell",
        "primary_horizon": int(primary_horizon),
        "rules": {
            "breakdown_setup_types": ["breakdown"],
            "repeated_min_signal_count": 2,
            "weak_regime_tags": list(SELL_WEAK_REGIME_TAGS),
        },
        "universe": _build_sell_subset_metrics(
            universe,
            subset_key="universe",
            label="all sell events",
            criteria={},
            primary_horizon=primary_horizon,
        ),
        "subsets": [
            _build_sell_subset_metrics(rows, subset_key=subset_key, label=label, criteria=criteria, primary_horizon=primary_horizon)
            for subset_key, label, criteria, rows in subsets
        ],
    }


def _safe_directional_return(side: str, exit_price: float | None, basis_price: float | None) -> float | None:
    if exit_price is None or basis_price is None or exit_price <= 0 or basis_price <= 0:
        return None
    if side == "sell":
        return float(basis_price / exit_price - 1.0)
    return float(exit_price / basis_price - 1.0)


def _normalize_side(side: str | None, *, allow_all: bool = False) -> str:
    normalized = str(side or "buy").strip().lower()
    if allow_all and normalized == "all":
        return normalized
    if normalized not in SUPPORTED_SIDES:
        raise ValueError("side must be buy|sell")
    return normalized


def _side_to_direction(side: str) -> str:
    normalized = _normalize_side(side)
    return SUPPORTED_SIDES[normalized]


def _normalize_ranking_dir(direction: str | None) -> str:
    normalized = str(direction or "up").strip().lower()
    if normalized not in SUPPORTED_RANKING_DIRS:
        raise ValueError("dir must be up|down")
    return normalized


def _ranking_dir_to_side(direction: str) -> str:
    return SUPPORTED_RANKING_DIRS[_normalize_ranking_dir(direction)]


def _normalize_tracking_outcome(outcome: str | None) -> str:
    normalized = str(outcome or "all").strip().lower()
    if normalized not in SUPPORTED_TRACKING_OUTCOMES:
        raise ValueError("outcome must be all|good|bad|broken")
    return normalized


def _normalize_tracking_sort(sort: str | None) -> str:
    normalized = str(sort or "recent").strip().lower()
    if normalized not in SUPPORTED_TRACKING_SORTS:
        raise ValueError("sort must be recent|oldest|best|worst")
    return normalized


def _coerce_tracking_limit(limit: int | None) -> int:
    return max(1, min(int(limit or DEFAULT_LIMIT), 500))


def _coerce_tracking_offset(offset: int | None) -> int:
    return max(0, int(offset or 0))


def _tracking_outcome_metric(primary_value: Any, fallback_value: Any) -> float | None:
    numeric = _safe_float(primary_value)
    if numeric is not None:
        return numeric
    return _safe_float(fallback_value)


def _matches_tracking_outcome(*, metric: float | None, break_status: str | None, outcome: str) -> bool:
    if outcome == "all":
        return True
    if outcome == "broken":
        return str(break_status or "").strip().lower() == "broken"
    if metric is None:
        return False
    if outcome == "good":
        return metric > 0
    return metric < 0


def _signal_event_metric(item: dict[str, Any]) -> float | None:
    return _tracking_outcome_metric(item.get("return_30d"), item.get("current_directional_return"))


def _normalize_external_candidate_side(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if normalized in SUPPORTED_SIDES:
        return normalized
    if normalized in SUPPORTED_RANKING_DIRS:
        return _ranking_dir_to_side(normalized)
    if normalized == "long":
        return "buy"
    if normalized == "short":
        return "sell"
    return None


def _hash_json_payload(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _coerce_pred_dt(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        normalized = int(value)
        if 19_000_101 <= normalized <= 21_001_231:
            return normalized
        if normalized >= 1_000_000_000:
            try:
                if normalized >= 1_000_000_000_000:
                    stamp = normalized / 1000.0
                else:
                    stamp = float(normalized)
                return int(datetime.fromtimestamp(stamp, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return None
        return None
    return _coerce_ymd(value)


def _extract_basis_provenance(item: dict[str, Any], *, dt: int) -> dict[str, Any]:
    source_as_of = _coerce_ymd(item.get("asOf") or item.get("as_of") or item.get("dt")) or int(dt)
    pred_dt = _coerce_pred_dt(item.get("pred_dt") or item.get("predDt") or item.get("predDate"))
    model_version = str(item.get("modelVersion") or item.get("model_version") or "").strip() or None
    return {
        "source_as_of": source_as_of,
        "pred_dt": pred_dt,
        "model_version": model_version,
        "basis_source": "rankings_cache.asof_base_cache",
        "source_hash": _hash_json_payload(item),
        "payload_schema_version": BASIS_PAYLOAD_SCHEMA_VERSION,
    }


def _find_prohibited_basis_paths(value: Any, path: str = "") -> list[str]:
    hits: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            lowered = key_text.lower()
            next_path = f"{path}.{key_text}" if path else key_text
            if any(token in lowered for token in PROHIBITED_BASIS_KEY_TOKENS):
                hits.append(next_path)
            hits.extend(_find_prohibited_basis_paths(child, next_path))
        return hits
    if isinstance(value, list):
        for index, child in enumerate(value):
            next_path = f"{path}[{index}]" if path else f"[{index}]"
            hits.extend(_find_prohibited_basis_paths(child, next_path))
    return hits


def _month_key(ymd: int | None) -> str | None:
    if ymd is None:
        return None
    text = f"{int(ymd):08d}"
    return f"{text[:4]}-{text[4:6]}"


def _month_key_sort_key(key: str) -> tuple[int, int]:
    try:
        year_text, month_text = str(key).split("-", 1)
        return int(year_text), int(month_text)
    except Exception:
        return (0, 0)


def _rolling_average_series(
    rows: list[dict[str, Any]],
    *,
    key: str,
    value_key: str,
    window: int = 6,
) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda item: _month_key_sort_key(str(item.get(key) or "")))
    result: list[dict[str, Any]] = []
    for index, item in enumerate(ordered):
        window_rows = ordered[max(0, index - window + 1) : index + 1]
        values = [float(row[value_key]) for row in window_rows if isinstance(row.get(value_key), (int, float))]
        result.append(
            {
                key: item.get(key),
                "window_size": min(window, index + 1),
                value_key: _safe_mean(values),
            }
        )
    return result


def _extract_display_score(item: dict[str, Any]) -> float | None:
    for key in (
        "tradePriorityScore",
        "score",
        "entryScore",
        "hybridScore",
        "winNowScore",
        "mlEv20Net",
        "accumulationScore",
        "breakoutReadiness",
    ):
        value = item.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


@contextmanager
def _open_conn(db_path: str | None = None, *, read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    if db_path:
        with get_conn_for_path(str(db_path), timeout_sec=2.5, read_only=read_only) as conn:
            yield conn
        return
    with get_conn() as conn:
        yield conn


def _resolved_db_path(db_path: str | None = None) -> str:
    target = db_path or str(config.DB_PATH)
    try:
        return str(Path(target).expanduser().resolve(strict=False))
    except Exception:
        return str(target)


def _runtime_stock_db_status(
    *,
    requested_symbol: str | None = None,
    requested_chart_date: int | str | None = None,
    db_path: str | None = None,
    reference_db_path: str | None = None,
) -> dict[str, Any]:
    return inspect_runtime_stock_db(
        runtime_db_path=db_path,
        requested_symbol=requested_symbol,
        requested_chart_date=requested_chart_date,
        reference_db_path=reference_db_path,
    )


def _build_chart_basis_detail_fields(runtime_stock_db_contract: dict[str, Any]) -> dict[str, Any]:
    requested_chart_date = runtime_stock_db_contract.get("requested_chart_date")
    latest_available_global_date = runtime_stock_db_contract.get("latest_available_global_date")
    date_match_status = str(runtime_stock_db_contract.get("date_match_status") or "blocked")
    confirmed_judgment_available = date_match_status == "exact"
    provisional_judgment_available = date_match_status == "lagged_provisional"
    display_basis_classification = "confirmed" if confirmed_judgment_available else "provisional" if provisional_judgment_available else None
    judgment_basis_classification = "confirmed" if confirmed_judgment_available else "provisional" if provisional_judgment_available else None
    overwrite_status = (
        "authoritative_confirmed"
        if confirmed_judgment_available
        else "provisional_only"
        if provisional_judgment_available
        else None
    )
    confirmed_last_available_date = (
        int(latest_available_global_date)
        if isinstance(latest_available_global_date, int)
        else None
    )
    provisional_last_available_date = (
        int(requested_chart_date)
        if provisional_judgment_available and requested_chart_date is not None
        else None
    )
    return {
        "confirmed_chart_source_provider": "chart_gallery_confirmed_source",
        "provisional_chart_source_provider": "yahoo_intraday_unconfirmed_source"
        if provisional_judgment_available
        else None,
        "confirmed_judgment_basis": "chart_gallery_confirmed_source_only"
        if confirmed_judgment_available
        else None,
        "provisional_judgment_basis": "yahoo_intraday_unconfirmed_source_only"
        if provisional_judgment_available
        else None,
        "confirmed_judgment_available": confirmed_judgment_available,
        "provisional_judgment_available": provisional_judgment_available,
        "display_basis_classification": display_basis_classification,
        "judgment_basis_classification": judgment_basis_classification,
        "confirmed_last_available_date": confirmed_last_available_date,
        "provisional_last_available_date": provisional_last_available_date,
        "overwrite_status": overwrite_status,
    }


@contextmanager
def _temporary_stocks_db_path(db_path: str | None) -> Iterator[None]:
    if not db_path:
        yield
        return
    normalized = _resolved_db_path(db_path)
    previous = os.environ.get("STOCKS_DB_PATH")
    os.environ["STOCKS_DB_PATH"] = normalized
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("STOCKS_DB_PATH", None)
        else:
            os.environ["STOCKS_DB_PATH"] = previous


def _table_count(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = ?
            LIMIT 1
            """,
            [str(table_name)],
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_latest_ymd(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> int | None:
    try:
        row = conn.execute(f"SELECT MAX({column_name}) FROM {table_name}").fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _latest_market_ymd(conn: duckdb.DuckDBPyConnection) -> int | None:
    row = conn.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd
            FROM daily_bars
        )
        SELECT MAX(ymd) FROM normalized
        """
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _list_market_dates(
    conn: duckdb.DuckDBPyConnection,
    *,
    from_ymd: int | None = None,
    to_ymd: int | None = None,
) -> list[int]:
    where_parts = ["ymd IS NOT NULL"]
    params: list[Any] = []
    if from_ymd is not None:
        where_parts.append("ymd >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        where_parts.append("ymd <= ?")
        params.append(int(to_ymd))
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT DISTINCT
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd
            FROM daily_bars
        )
        SELECT ymd
        FROM normalized
        WHERE {" AND ".join(where_parts)}
        ORDER BY ymd
        """,
        params,
    ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _fetch_code_bars(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    start_ymd: int,
) -> list[DailyBar]:
    rows = conn.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                o,
                h,
                l,
                c
            FROM daily_bars
            WHERE code = ?
        )
        SELECT ymd, o, h, l, c
        FROM normalized
        WHERE ymd IS NOT NULL AND ymd >= ?
        ORDER BY ymd
        """,
        [str(code), int(start_ymd)],
    ).fetchall()
    return [
        DailyBar(
            date=int(row[0]),
            open=float(row[1]) if row[1] is not None else None,
            high=float(row[2]) if row[2] is not None else None,
            low=float(row[3]) if row[3] is not None else None,
            close=float(row[4]) if row[4] is not None else None,
        )
        for row in rows
    ]


def _bar_price_lookup(bars: list[DailyBar]) -> dict[int, int]:
    return {bar.date: index for index, bar in enumerate(bars)}


def _seed_default_logic_registry(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM signal_logic_registry").fetchone()
    if row and int(row[0] or 0) > 0:
        return
    conn.execute(
        """
        INSERT INTO signal_logic_registry (
            logic_version,
            basis_version,
            label,
            source_hash,
            is_active,
            notes
        )
        VALUES (?, ?, ?, ?, TRUE, ?)
        """,
        [
            DEFAULT_LOGIC_VERSION,
            DEFAULT_BASIS_VERSION,
            "Trade Latest",
            "current_rankings_cache",
            "default active logic version",
        ],
    )


def _seed_default_ranking_logic_registry(conn: duckdb.DuckDBPyConnection) -> None:
    row = conn.execute("SELECT COUNT(*) FROM ranking_logic_registry").fetchone()
    if row and int(row[0] or 0) > 0:
        return
    conn.execute(
        """
        INSERT INTO ranking_logic_registry (
            ranking_logic_version,
            basis_version,
            label,
            contract_json,
            source_hash,
            is_active,
            notes
        )
        VALUES (?, ?, ?, ?, ?, TRUE, ?)
        """,
        [
            DEFAULT_RANKING_LOGIC_VERSION,
            DEFAULT_BASIS_VERSION,
            "Ranking Trade Top50",
            _json_dump(
                {
                    "tf": "D",
                    "which": "latest",
                    "mode": "trade",
                    "risk_mode": "balanced",
                    "limit": DEFAULT_RANKING_LIMIT,
                }
            ),
            "current_rankings_cache",
            "default active ranking history logic",
        ],
    )


def _resolve_logic_version(conn: duckdb.DuckDBPyConnection, logic_version: str | None) -> tuple[str, str]:
    _seed_default_logic_registry(conn)
    requested = str(logic_version or ACTIVE_LOGIC_VERSION_ALIAS).strip() or ACTIVE_LOGIC_VERSION_ALIAS
    if requested == ACTIVE_LOGIC_VERSION_ALIAS:
        row = conn.execute(
            """
            SELECT logic_version, basis_version
            FROM signal_logic_registry
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return str(row[0]), str(row[1] or DEFAULT_BASIS_VERSION)
        return DEFAULT_LOGIC_VERSION, DEFAULT_BASIS_VERSION
    row = conn.execute(
        """
        SELECT logic_version, basis_version
        FROM signal_logic_registry
        WHERE logic_version = ?
        """,
        [requested],
    ).fetchone()
    if row:
        return str(row[0]), str(row[1] or DEFAULT_BASIS_VERSION)
    conn.execute(
        """
        INSERT INTO signal_logic_registry (
            logic_version,
            basis_version,
            label,
            source_hash,
            is_active,
            notes
        )
        VALUES (?, ?, ?, ?, FALSE, ?)
        """,
        [requested, DEFAULT_BASIS_VERSION, requested, "manual", "created during rebuild"],
    )
    return requested, DEFAULT_BASIS_VERSION


def _resolve_ranking_logic_version(
    conn: duckdb.DuckDBPyConnection,
    ranking_logic_version: str | None,
) -> tuple[str, str]:
    _seed_default_ranking_logic_registry(conn)
    requested = str(ranking_logic_version or ACTIVE_RANKING_LOGIC_VERSION_ALIAS).strip() or ACTIVE_RANKING_LOGIC_VERSION_ALIAS
    if requested == ACTIVE_RANKING_LOGIC_VERSION_ALIAS:
        row = conn.execute(
            """
            SELECT ranking_logic_version, basis_version
            FROM ranking_logic_registry
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return str(row[0]), str(row[1] or DEFAULT_BASIS_VERSION)
        return DEFAULT_RANKING_LOGIC_VERSION, DEFAULT_BASIS_VERSION
    row = conn.execute(
        """
        SELECT ranking_logic_version, basis_version
        FROM ranking_logic_registry
        WHERE ranking_logic_version = ?
        """,
        [requested],
    ).fetchone()
    if row:
        return str(row[0]), str(row[1] or DEFAULT_BASIS_VERSION)
    conn.execute(
        """
        INSERT INTO ranking_logic_registry (
            ranking_logic_version,
            basis_version,
            label,
            contract_json,
            source_hash,
            is_active,
            notes
        )
        VALUES (?, ?, ?, ?, ?, FALSE, ?)
        """,
        [
            requested,
            DEFAULT_BASIS_VERSION,
            requested,
            _json_dump({"limit": DEFAULT_RANKING_LIMIT, "mode": "trade"}),
            "manual",
            "created during rebuild",
        ],
    )
    return requested, DEFAULT_BASIS_VERSION


def list_logic_versions(*, db_path: str | None = None) -> dict[str, Any]:
    with _open_conn(db_path) as conn:
        _seed_default_logic_registry(conn)
        rows = conn.execute(
            """
            SELECT logic_version, basis_version, label, source_hash, is_active, notes, created_at
            FROM signal_logic_registry
            ORDER BY is_active DESC, created_at DESC, logic_version ASC
            """
        ).fetchall()
    items = [
        {
            "logic_version": str(row[0]),
            "basis_version": str(row[1] or DEFAULT_BASIS_VERSION),
            "label": str(row[2]) if row[2] is not None else None,
            "source_hash": str(row[3]) if row[3] is not None else None,
            "is_active": bool(row[4]),
            "notes": str(row[5]) if row[5] is not None else None,
            "created_at": _serialize_timestamp(row[6]) if isinstance(row[6], datetime) else None,
        }
        for row in rows
    ]
    active_logic_version = next((item["logic_version"] for item in items if item["is_active"]), DEFAULT_LOGIC_VERSION)
    return {
        "active_logic_version": active_logic_version,
        "items": items,
    }


def list_ranking_logic_versions(*, db_path: str | None = None) -> dict[str, Any]:
    with _open_conn(db_path) as conn:
        _seed_default_ranking_logic_registry(conn)
        rows = conn.execute(
            """
            SELECT
                ranking_logic_version,
                basis_version,
                label,
                contract_json,
                source_hash,
                is_active,
                notes,
                created_at
            FROM ranking_logic_registry
            ORDER BY is_active DESC, created_at DESC, ranking_logic_version ASC
            """
        ).fetchall()
    items = [
        {
            "ranking_logic_version": str(row[0]),
            "basis_version": str(row[1] or DEFAULT_BASIS_VERSION),
            "label": str(row[2]) if row[2] is not None else None,
            "contract": _json_load(str(row[3]) if row[3] is not None else None),
            "source_hash": str(row[4]) if row[4] is not None else None,
            "is_active": bool(row[5]),
            "notes": str(row[6]) if row[6] is not None else None,
            "created_at": _serialize_timestamp(row[7]) if isinstance(row[7], datetime) else None,
        }
        for row in rows
    ]
    active_logic_version = next(
        (item["ranking_logic_version"] for item in items if item["is_active"]),
        DEFAULT_RANKING_LOGIC_VERSION,
    )
    return {
        "active_ranking_logic_version": active_logic_version,
        "items": items,
    }


def get_tracking_runtime_status(*, db_path: str | None = None) -> dict[str, Any]:
    resolved_data_dir = str(config.DATA_DIR)
    resolved_stocks_db_path = _resolved_db_path(db_path)
    runtime_stock_db_contract = _runtime_stock_db_status(db_path=db_path)
    with _open_conn(db_path, read_only=True) as conn:
        signal_occurrence_count = _table_count(conn, "signal_occurrence")
        signal_decision_count = _table_count(conn, "signal_decision_daily")
        ranking_appearance_count = _table_count(conn, "ranking_appearance_daily")
        signal_latest_ymd = _table_latest_ymd(conn, "signal_occurrence", "signal_date")
        ranking_latest_ymd = _table_latest_ymd(conn, "ranking_appearance_daily", "dt")
    return {
        "ok": True,
        "resolved_data_dir": resolved_data_dir,
        "resolved_stocks_db_path": resolved_stocks_db_path,
        "runtime_stock_db_contract": runtime_stock_db_contract,
        "signal_occurrence_count": signal_occurrence_count,
        "signal_decision_count": signal_decision_count,
        "signal_latest_date": signal_latest_ymd,
        "signal_latest_date_iso": _ymd_to_iso(signal_latest_ymd),
        "signal_history_generated": signal_occurrence_count > 0,
        "ranking_appearance_count": ranking_appearance_count,
        "ranking_latest_date": ranking_latest_ymd,
        "ranking_latest_date_iso": _ymd_to_iso(ranking_latest_ymd),
        "ranking_history_generated": ranking_appearance_count > 0,
    }


def activate_logic_version(logic_version: str, *, db_path: str | None = None) -> dict[str, Any]:
    with _open_conn(db_path) as conn:
        _seed_default_logic_registry(conn)
        resolved_logic_version, basis_version = _resolve_logic_version(conn, logic_version)
        conn.execute("UPDATE signal_logic_registry SET is_active = FALSE")
        conn.execute(
            "UPDATE signal_logic_registry SET is_active = TRUE WHERE logic_version = ?",
            [resolved_logic_version],
        )
    return {"ok": True, "logic_version": resolved_logic_version, "basis_version": basis_version}


def activate_ranking_logic_version(
    ranking_logic_version: str,
    *,
    db_path: str | None = None,
) -> dict[str, Any]:
    with _open_conn(db_path) as conn:
        _seed_default_ranking_logic_registry(conn)
        resolved_logic_version, basis_version = _resolve_ranking_logic_version(conn, ranking_logic_version)
        conn.execute("UPDATE ranking_logic_registry SET is_active = FALSE")
        conn.execute(
            "UPDATE ranking_logic_registry SET is_active = TRUE WHERE ranking_logic_version = ?",
            [resolved_logic_version],
        )
    return {"ok": True, "ranking_logic_version": resolved_logic_version, "basis_version": basis_version}


def _build_rank_snapshot(item: dict[str, Any], *, final_rank: int | None, source_rank: int | None) -> dict[str, Any]:
    return {
        "finalRank": final_rank,
        "sourceRank": source_rank,
        "tradePriorityScore": item.get("tradePriorityScore"),
        "tradePriorityProfitScore": item.get("tradePriorityProfitScore"),
        "tradePriorityHitScore": item.get("tradePriorityHitScore"),
        "entryScore": item.get("entryScore"),
        "probSide": item.get("probSide"),
        "asOf": item.get("asOf"),
    }


def _build_reason_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "setupType": item.get("setupType"),
        "monthlyBoxState": item.get("monthlyBoxState"),
        "monthlyBoxPos": item.get("monthlyBoxPos"),
        "monthlyBoxRangePct": item.get("monthlyBoxRangePct"),
        "boxUpper": item.get("boxUpper"),
        "boxLower": item.get("boxLower"),
        "tradeDecisionReasons": item.get("tradeDecisionReasons"),
        "tradeRiskWatch": item.get("tradeRiskWatch"),
        "entryQualified": item.get("entryQualified"),
        "entryQualifiedByFallback": item.get("entryQualifiedByFallback"),
        "entryQualifiedFallbackStage": item.get("entryQualifiedFallbackStage"),
        "recommendedHoldDays": item.get("recommendedHoldDays"),
        "recommendedHoldMinDays": item.get("recommendedHoldMinDays"),
        "recommendedHoldMaxDays": item.get("recommendedHoldMaxDays"),
        "recommendedHoldReason": item.get("recommendedHoldReason"),
        "invalidationTrigger": item.get("invalidationTrigger"),
        "invalidationConservativeAction": item.get("invalidationConservativeAction"),
        "invalidationAggressiveAction": item.get("invalidationAggressiveAction"),
        "invalidationRecommendedAction": item.get("invalidationRecommendedAction"),
        "invalidationDotenRecommended": item.get("invalidationDotenRecommended"),
        "invalidationOppositeHoldDays": item.get("invalidationOppositeHoldDays"),
        "invalidationExpectedDeltaMean": item.get("invalidationExpectedDeltaMean"),
        "invalidationPolicyNote": item.get("invalidationPolicyNote"),
    }


def _build_score_snapshot(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "tradePriorityScore": item.get("tradePriorityScore"),
        "tradePriorityProfitScore": item.get("tradePriorityProfitScore"),
        "tradePriorityHitScore": item.get("tradePriorityHitScore"),
        "tradePriorityQualityScore": item.get("tradePriorityQualityScore"),
        "tradePrioritySafetyScore": item.get("tradePrioritySafetyScore"),
        "entryScore": item.get("entryScore"),
        "hybridScore": item.get("hybridScore"),
        "probSide": item.get("probSide"),
        "probSideCalib": item.get("probSideCalib"),
        "mlEv20Net": item.get("mlEv20Net"),
    }


def _compute_directional_forward_returns(
    side: str,
    *,
    bars: list[DailyBar],
    by_date: dict[int, int],
    signal_date: int,
) -> dict[int, float | None]:
    idx = by_date.get(signal_date)
    if idx is None or idx + 1 >= len(bars):
        return {horizon: None for horizon in DECISION_HORIZONS}
    entry_price = bars[idx + 1].open
    if entry_price is None or entry_price <= 0:
        return {horizon: None for horizon in DECISION_HORIZONS}
    out: dict[int, float | None] = {}
    for horizon in DECISION_HORIZONS:
        exit_idx = idx + horizon
        if exit_idx >= len(bars):
            out[horizon] = None
            continue
        out[horizon] = _safe_directional_return(side, bars[exit_idx].close, entry_price)
    return out


def _apply_logic_version_filters(
    items: list[dict[str, Any]],
    *,
    side: str,
    logic_version: str | None,
) -> list[dict[str, Any]]:
    normalized_logic_version = str(logic_version or "").strip()
    normalized_side = _normalize_side(side)
    if normalized_logic_version != SELL_TIGHTENED_LOGIC_VERSION or normalized_side != "sell":
        return items
    filtered_items: list[dict[str, Any]] = []
    for raw_item in items:
        item = dict(raw_item)
        if bool(item.get("entryQualified")) is True:
            setup_type = str(item.get("setupType") or "").strip().lower()
            is_fallback = bool(item.get("entryQualifiedByFallback")) or bool(str(item.get("entryQualifiedFallbackStage") or "").strip())
            trade_priority_score = _safe_float(item.get("tradePriorityScore"))
            prob_side = _safe_float(item.get("probSide"))
            hit_score = _safe_float(item.get("tradePriorityHitScore"))
            profit_score = _safe_float(item.get("tradePriorityProfitScore"))
            passes = bool(
                setup_type == "breakdown"
                and not is_fallback
                and trade_priority_score is not None
                and trade_priority_score >= 0.90
                and prob_side is not None
                and prob_side >= 0.75
                and hit_score is not None
                and hit_score >= 0.95
                and profit_score is not None
                and profit_score >= 0.97
            )
            if not passes:
                item["entryQualified"] = False
                item["logicFilterReason"] = "sell_tightened_v2"
        filtered_items.append(item)
    return filtered_items


def _call_evaluate_trade_items_from_basis(
    *,
    items: list[dict[str, Any]],
    as_of_int: int,
    side: str,
    logic_version: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int | None, str | None]:
    try:
        return _evaluate_trade_items_from_basis(
            items=items,
            as_of_int=as_of_int,
            side=side,
            logic_version=logic_version,
        )
    except TypeError as exc:
        if "logic_version" not in str(exc):
            raise
        return _evaluate_trade_items_from_basis(
            items=items,
            as_of_int=as_of_int,
            side=side,
        )


def _resolve_observed_window(
    *,
    bars: list[DailyBar],
    start_index: int,
    latest_market_ymd: int | None,
    horizon_bars: int = WATCH_HORIZON_BARS,
) -> dict[str, Any]:
    horizon_end_index = min(start_index + max(1, int(horizon_bars)) - 1, len(bars) - 1)
    observed_end_index = horizon_end_index
    if latest_market_ymd is not None:
        visible_indexes = [index for index, bar in enumerate(bars) if bar.date <= latest_market_ymd]
        if visible_indexes:
            observed_end_index = min(horizon_end_index, visible_indexes[-1])
    elapsed_bars = max(0, observed_end_index - start_index + 1)
    remaining_bars = max(0, max(1, int(horizon_bars)) - elapsed_bars)
    completed_idx = start_index + max(1, int(horizon_bars)) - 1
    completed_ymd = bars[completed_idx].date if completed_idx < len(bars) else None
    status, completed_at, archived_at = _status_for_campaign(
        latest_market_ymd=latest_market_ymd,
        completed_ymd=completed_ymd,
        elapsed_bars=elapsed_bars,
    )
    return {
        "horizon_end_index": horizon_end_index,
        "observed_end_index": observed_end_index,
        "elapsed_bars": elapsed_bars,
        "remaining_bars": remaining_bars,
        "completed_index": completed_idx if completed_idx < len(bars) else None,
        "completed_ymd": completed_ymd,
        "status": status,
        "completed_at": completed_at,
        "archived_at": archived_at,
    }


def _compute_window_metrics(
    *,
    side: str,
    bars: list[DailyBar],
    start_index: int,
    anchor_close_price: float | None,
    anchor_exec_price: float | None,
    latest_market_ymd: int | None,
    horizon_bars: int = WATCH_HORIZON_BARS,
) -> dict[str, Any]:
    window = _resolve_observed_window(
        bars=bars,
        start_index=start_index,
        latest_market_ymd=latest_market_ymd,
        horizon_bars=horizon_bars,
    )
    observed_end_index = int(window["observed_end_index"])
    last_bar = bars[observed_end_index] if bars else None
    max_favorable = None
    max_adverse = None
    days_to_max_favorable = None
    days_to_max_adverse = None
    date_of_max_favorable = None
    date_of_max_adverse = None
    for offset, bar in enumerate(bars[start_index : observed_end_index + 1]):
        favorable_basis = bar.high if side == "buy" else bar.low
        adverse_basis = bar.low if side == "buy" else bar.high
        favorable = _safe_directional_return(side, favorable_basis, anchor_close_price)
        adverse = _safe_directional_return(side, adverse_basis, anchor_close_price)
        if favorable is not None:
            if max_favorable is None or favorable > max_favorable:
                max_favorable = favorable
                days_to_max_favorable = offset
                date_of_max_favorable = bar.date
        if adverse is not None:
            if max_adverse is None or adverse < max_adverse:
                max_adverse = adverse
                days_to_max_adverse = offset
                date_of_max_adverse = bar.date
    completed_index = window.get("completed_index")
    final_return = None
    if isinstance(completed_index, int) and completed_index < len(bars):
        final_return = _safe_directional_return(side, bars[completed_index].close, anchor_close_price)
    price_series = []
    for bar in bars[start_index : observed_end_index + 1]:
        day_index = len(price_series)
        price_series.append(
            {
                "date": bar.date,
                "date_iso": _ymd_to_iso(bar.date),
                "day_index": day_index,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "return_close_basis": _safe_directional_return(side, bar.close, anchor_close_price),
                "return_exec_basis": _safe_directional_return(side, bar.close, anchor_exec_price),
            }
        )
    return {
        **window,
        "last_price": last_bar.close if last_bar is not None else None,
        "current_return_close_basis": _safe_directional_return(
            side,
            last_bar.close if last_bar is not None else None,
            anchor_close_price,
        ),
        "current_return_exec_basis": _safe_directional_return(
            side,
            last_bar.close if last_bar is not None else None,
            anchor_exec_price,
        ),
        "final_return_at_horizon": final_return,
        "max_favorable_return": max_favorable,
        "max_adverse_return": max_adverse,
        "days_to_max_favorable_30": days_to_max_favorable,
        "days_to_max_adverse_30": days_to_max_adverse,
        "date_of_max_favorable_30": date_of_max_favorable,
        "date_of_max_adverse_30": date_of_max_adverse,
        "price_series": price_series,
    }


def _build_basis_rows_for_date(as_of_int: int, *, db_path: str | None = None) -> list[dict[str, Any]]:
    if db_path is None:
        cache = rankings_cache._get_asof_base_cache(as_of_int)
    else:
        with _open_conn(db_path, read_only=True) as conn:
            cache = rankings_cache._build_cache_asof(conn, as_of_int)
    buy_items = list(cache.get(("D", "latest", "up"), []) or [])
    sell_items = list(cache.get(("D", "latest", "down"), []) or [])
    buy_rank = {str(item.get("code") or ""): index + 1 for index, item in enumerate(buy_items) if str(item.get("code") or "").strip()}
    sell_rank = {str(item.get("code") or ""): index + 1 for index, item in enumerate(sell_items) if str(item.get("code") or "").strip()}
    merged: dict[str, dict[str, Any]] = {}
    for item in [*buy_items, *sell_items]:
        code = str(item.get("code") or "").strip()
        if not code:
            continue
        prohibited_paths = _find_prohibited_basis_paths(item)
        if prohibited_paths:
            raise ValueError(
                f"signal basis payload includes prohibited future/outcome fields for {code} {as_of_int}: {', '.join(prohibited_paths[:5])}"
            )
        provenance = _extract_basis_provenance(item, dt=as_of_int)
        merged[code] = {
            "dt": as_of_int,
            "code": code,
            "name": str(item.get("name") or code),
            "source_rank_buy": buy_rank.get(code),
            "source_rank_sell": sell_rank.get(code),
            "source_as_of": provenance["source_as_of"],
            "pred_dt": provenance["pred_dt"],
            "model_version": provenance["model_version"],
            "basis_source": provenance["basis_source"],
            "source_hash": provenance["source_hash"],
            "payload_schema_version": provenance["payload_schema_version"],
            "basis_payload_json": _json_dump(dict(item)),
        }
    return list(merged.values())


def _call_build_basis_rows_for_date(as_of_int: int, *, db_path: str | None = None) -> list[dict[str, Any]]:
    if db_path is None:
        return _build_basis_rows_for_date(as_of_int)
    try:
        return _build_basis_rows_for_date(as_of_int, db_path=db_path)
    except TypeError as exc:
        if "unexpected keyword argument 'db_path'" not in str(exc):
            raise
        return _build_basis_rows_for_date(as_of_int)


def backfill_signal_basis(
    *,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    basis_version: str = DEFAULT_BASIS_VERSION,
    reset_scope: bool = False,
    db_path: str | None = None,
    progress_cb: TrackingProgressCallback | None = None,
) -> dict[str, Any]:
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        effective_to = to_int or latest_market_ymd
        market_dates = _list_market_dates(conn, from_ymd=from_int, to_ymd=effective_to)
    if not market_dates:
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="basis",
                status="done",
                processed=0,
                total=0,
                detail="no market dates",
            ),
            force=True,
        )
        return {
            "ok": True,
            "basis_version": str(basis_version),
            "from": _ymd_to_iso(from_int),
            "to": _ymd_to_iso(to_int),
            "dates_processed": 0,
            "rows_upserted": 0,
        }
    rows: list[list[Any]] = []
    progress_state: dict[str, Any] = {"last_emit_at": 0.0}
    total_dates = len(market_dates)
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="basis",
            status="start",
            processed=0,
            total=total_dates,
            current_market_ymd=market_dates[0],
            current_market_date=_ymd_to_iso(market_dates[0]),
            detail=f"building basis rows (version={basis_version})",
        ),
        throttle_state=progress_state,
        force=True,
    )
    for index, dt in enumerate(market_dates, start=1):
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="basis",
                status="running",
                processed=index,
                total=total_dates,
                current_market_ymd=dt,
                current_market_date=_ymd_to_iso(dt),
                detail="building basis rows",
            ),
            throttle_state=progress_state,
        )
        for row in _call_build_basis_rows_for_date(dt, db_path=db_path):
            rows.append(
                [
                    int(row["dt"]),
                    str(row["code"]),
                    str(basis_version),
                    row.get("name"),
                    row.get("source_rank_buy"),
                    row.get("source_rank_sell"),
                    row.get("source_as_of"),
                    row.get("pred_dt"),
                    row.get("model_version"),
                    row.get("basis_source"),
                    row.get("source_hash"),
                    row.get("payload_schema_version"),
                    row.get("basis_payload_json"),
                    datetime.now(timezone.utc),
                ]
            )
    with _open_conn(db_path) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            if reset_scope:
                delete_where = ["basis_version = ?"]
                delete_params: list[Any] = [str(basis_version)]
                if from_int is not None:
                    delete_where.append("dt >= ?")
                    delete_params.append(int(from_int))
                if effective_to is not None:
                    delete_where.append("dt <= ?")
                    delete_params.append(int(effective_to))
                conn.execute(f"DELETE FROM signal_basis_daily WHERE {' AND '.join(delete_where)}", delete_params)
            _bulk_insert_or_replace_rows(
                conn,
                table_name="signal_basis_daily",
                columns=[
                    "dt",
                    "code",
                    "basis_version",
                    "name",
                    "source_rank_buy",
                    "source_rank_sell",
                    "source_as_of",
                    "pred_dt",
                    "model_version",
                    "basis_source",
                    "source_hash",
                    "payload_schema_version",
                    "basis_payload_json",
                    "updated_at",
                ],
                rows=rows,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="basis",
            status="done",
            processed=total_dates,
            total=total_dates,
            current_market_ymd=market_dates[-1],
            current_market_date=_ymd_to_iso(market_dates[-1]),
            detail=f"rows_upserted={len(rows)}",
        ),
        throttle_state=progress_state,
        force=True,
    )
    return {
        "ok": True,
        "basis_version": str(basis_version),
        "from": _ymd_to_iso(market_dates[0]),
        "to": _ymd_to_iso(market_dates[-1]),
        "dates_processed": len(market_dates),
        "rows_upserted": len(rows),
    }


def backfill_signal_basis_provenance(
    *,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    basis_version: str = DEFAULT_BASIS_VERSION,
    db_path: str | None = None,
) -> dict[str, Any]:
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    where_parts = ["basis_version = ?"]
    params: list[Any] = [str(basis_version)]
    if from_int is not None:
        where_parts.append("dt >= ?")
        params.append(int(from_int))
    if to_int is not None:
        where_parts.append("dt <= ?")
        params.append(int(to_int))
    where_parts.append(
        "("
        "source_as_of IS NULL OR "
        "basis_source IS NULL OR TRIM(basis_source) = '' OR "
        "source_hash IS NULL OR TRIM(source_hash) = '' OR "
        "payload_schema_version IS NULL OR TRIM(payload_schema_version) = '' OR "
        "pred_dt IS NULL OR "
        "model_version IS NULL OR TRIM(model_version) = ''"
        ")"
    )
    with _open_conn(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                dt,
                code,
                basis_version,
                source_as_of,
                pred_dt,
                model_version,
                basis_source,
                source_hash,
                payload_schema_version,
                basis_payload_json
            FROM signal_basis_daily
            WHERE {" AND ".join(where_parts)}
            ORDER BY dt, code
            """,
            params,
        ).fetchall()
        if not rows:
            return {
                "ok": True,
                "basis_version": str(basis_version),
                "candidate_rows": 0,
                "updated_rows": 0,
                "invalid_payload_rows": 0,
                "future_source_as_of_rows": 0,
            }
        pred_dt_candidates = sorted(
            {
                int(row[4])
                for row in rows
                if row[4] is not None and not str(row[5] or "").strip()
            }
        )
        model_version_by_pred_dt: dict[int, str] = {}
        if pred_dt_candidates and _table_exists(conn, "ml_pred_20d"):
            placeholders = ", ".join(["?"] * len(pred_dt_candidates))
            version_rows = conn.execute(
                f"""
                SELECT dt, MIN(model_version) AS model_version
                FROM ml_pred_20d
                WHERE dt IN ({placeholders})
                  AND model_version IS NOT NULL
                  AND TRIM(model_version) <> ''
                GROUP BY dt
                HAVING COUNT(DISTINCT model_version) = 1
                """,
                pred_dt_candidates,
            ).fetchall()
            model_version_by_pred_dt = {
                int(row[0]): str(row[1])
                for row in version_rows
                if row and row[0] is not None and row[1] is not None and str(row[1]).strip()
            }
        updates: list[list[Any]] = []
        invalid_payload_rows = 0
        future_source_as_of_rows = 0
        model_version_join_rows = 0
        for row in rows:
            dt = int(row[0])
            code = str(row[1])
            payload = _json_load(str(row[9]) if row[9] is not None else None)
            if not isinstance(payload, dict):
                invalid_payload_rows += 1
                continue
            provenance = _extract_basis_provenance(payload, dt=dt)
            source_as_of = provenance.get("source_as_of")
            if source_as_of is not None and int(source_as_of) > dt:
                future_source_as_of_rows += 1
                source_as_of = row[3]
            pred_dt = provenance.get("pred_dt") if row[4] is None else row[4]
            model_version = provenance.get("model_version") or row[5]
            if not str(model_version or "").strip() and pred_dt is not None:
                joined_model_version = model_version_by_pred_dt.get(int(pred_dt))
                if joined_model_version:
                    model_version = joined_model_version
                    model_version_join_rows += 1
            basis_source = provenance.get("basis_source") or row[6]
            source_hash = provenance.get("source_hash") or row[7]
            payload_schema_version = provenance.get("payload_schema_version") or row[8]
            updates.append(
                [
                    source_as_of if row[3] is None else row[3],
                    pred_dt,
                    model_version,
                    basis_source,
                    source_hash,
                    payload_schema_version,
                    datetime.now(timezone.utc),
                    dt,
                    code,
                    str(row[2]),
                ]
            )
        if updates:
            frame = pd.DataFrame(
                updates,
                columns=[
                    "source_as_of",
                    "pred_dt",
                    "model_version",
                    "basis_source",
                    "source_hash",
                    "payload_schema_version",
                    "updated_at",
                    "dt",
                    "code",
                    "basis_version",
                ],
            )
            temp_name = f"_tmp_signal_basis_prov_{threading.get_ident()}"
            conn.register(temp_name, frame)
            try:
                conn.execute(
                    f"""
                    UPDATE signal_basis_daily AS tgt
                    SET
                        source_as_of = COALESCE(tgt.source_as_of, src.source_as_of),
                        pred_dt = COALESCE(tgt.pred_dt, src.pred_dt),
                        model_version = COALESCE(NULLIF(tgt.model_version, ''), src.model_version),
                        basis_source = COALESCE(NULLIF(tgt.basis_source, ''), src.basis_source),
                        source_hash = COALESCE(NULLIF(tgt.source_hash, ''), src.source_hash),
                        payload_schema_version = COALESCE(NULLIF(tgt.payload_schema_version, ''), src.payload_schema_version),
                        updated_at = src.updated_at
                    FROM {temp_name} AS src
                    WHERE tgt.dt = src.dt
                      AND tgt.code = src.code
                      AND tgt.basis_version = src.basis_version
                    """
                )
            finally:
                try:
                    conn.unregister(temp_name)
                except Exception:
                    pass
        return {
            "ok": True,
            "basis_version": str(basis_version),
            "candidate_rows": len(rows),
            "updated_rows": len(updates),
            "invalid_payload_rows": invalid_payload_rows,
            "future_source_as_of_rows": future_source_as_of_rows,
            "model_version_join_rows": model_version_join_rows,
            "from": _ymd_to_iso(from_int),
            "to": _ymd_to_iso(to_int),
        }


def _load_basis_items_for_date(
    conn: duckdb.DuckDBPyConnection,
    *,
    dt: int,
    basis_version: str,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int]]:
    rows = conn.execute(
        """
        SELECT code, source_rank_buy, source_rank_sell, basis_payload_json
        FROM signal_basis_daily
        WHERE dt = ? AND basis_version = ?
        ORDER BY COALESCE(source_rank_buy, source_rank_sell, 999999), code
        """,
        [int(dt), str(basis_version)],
    ).fetchall()
    items: list[dict[str, Any]] = []
    buy_rank: dict[str, int] = {}
    sell_rank: dict[str, int] = {}
    for code, source_rank_buy, source_rank_sell, payload_json in rows:
        payload = _json_load(str(payload_json) if payload_json is not None else None)
        if not isinstance(payload, dict):
            continue
        payload["code"] = str(code)
        items.append(payload)
        if source_rank_buy is not None:
            buy_rank[str(code)] = int(source_rank_buy)
        if source_rank_sell is not None:
            sell_rank[str(code)] = int(source_rank_sell)
    return items, buy_rank, sell_rank


def _evaluate_trade_items_from_basis(
    *,
    items: list[dict[str, Any]],
    as_of_int: int,
    side: str,
    logic_version: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int | None, str | None]:
    direction = _side_to_direction(side)
    effective_mode = rankings_cache._resolve_effective_rank_mode("trade")
    risk_mode = "balanced"
    limit = max(1, len(items))
    source_items = rankings_cache._copy_rank_items(items)
    legacy_analysis_disabled = rankings_cache.is_legacy_analysis_disabled()
    pred_dt = None
    model_version = None
    if effective_mode == "trade" and legacy_analysis_disabled:
        out_items = rankings_cache._decorate_rule_items_with_entry_gate(
            source_items,
            direction=direction,
            risk_mode=risk_mode,
        )
    else:
        out_items, pred_dt, model_version = rankings_cache._call_apply_ml_mode(
            source_items,
            direction=direction,
            mode=effective_mode,
            limit=limit,
            risk_mode=risk_mode,
        )
    out_items, pred_dt, model_version = rankings_cache._fallback_down_ml_items_when_empty(
        tf="D",
        direction=direction,
        mode=effective_mode,
        limit=limit,
        risk_mode=risk_mode,
        items=source_items,
        out_items=out_items,
        pred_dt=pred_dt,
        model_version=model_version,
    )
    quality_mode = effective_mode
    out_items = rankings_cache._attach_quality_flags(
        rankings_cache._copy_rank_items(out_items),
        mode=effective_mode,
        direction=direction,
        now_ymd=as_of_int,
    )
    out_items = rankings_cache._attach_swing_fields(out_items, direction=direction)
    if effective_mode == "trade":
        rankings_cache._apply_trade_priority_scores(out_items, direction=direction)
        out_items.sort(
            key=lambda item: (
                item.get("tradePriorityScore") is None,
                -(item.get("tradePriorityScore") or 0.0),
                -(item.get("tradePriorityProfitScore") or 0.0),
                -(item.get("tradePriorityHitScore") or 0.0),
                -(item.get("entryScore") or 0.0),
                -(item.get("probSide") or 0.0),
                item.get("code", ""),
            )
        )
    if pred_dt is not None:
        pred_key = pred_dt
        if pred_key >= 1_000_000_000:
            try:
                pred_key = int(datetime.fromtimestamp(pred_key, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                pred_key = as_of_int
        if pred_key > as_of_int:
            pred_dt = None
            model_version = None
            out_items = rankings_cache._decorate_rule_items_with_entry_gate(
                rankings_cache._copy_rank_items(items),
                direction=direction,
                risk_mode=risk_mode,
            )
            out_items = rankings_cache._attach_quality_flags(
                out_items,
                mode="rule",
                direction=direction,
                now_ymd=as_of_int,
            )
            out_items = rankings_cache._attach_swing_fields(out_items, direction=direction)
            quality_mode = "rule"
    scored_items = rankings_cache._copy_rank_items(out_items)
    if effective_mode == "trade":
        rankings_cache._apply_trade_priority_scores(scored_items, direction=direction)
        scored_items.sort(
            key=lambda item: (
                item.get("tradePriorityScore") is None,
                -(item.get("tradePriorityScore") or 0.0),
                -(item.get("tradePriorityProfitScore") or 0.0),
                -(item.get("tradePriorityHitScore") or 0.0),
                -(item.get("entryScore") or 0.0),
                -(item.get("probSide") or 0.0),
                item.get("code", ""),
            )
        )
    scored_items = _apply_logic_version_filters(
        scored_items,
        side=side,
        logic_version=logic_version,
    )
    qualified_items = rankings_cache._copy_rank_items(scored_items)
    if effective_mode == "trade":
        qualified_items = rankings_cache._filter_tradable_rank_items(qualified_items, direction=direction)
        qualified_items = rankings_cache._filter_strict_trade_rank_items(qualified_items, direction=direction)
        qualified_items.sort(
            key=lambda item: (
                item.get("tradePriorityScore") is None,
                -(item.get("tradePriorityScore") or 0.0),
                -(item.get("tradePriorityProfitScore") or 0.0),
                -(item.get("tradePriorityHitScore") or 0.0),
                -(item.get("entryScore") or 0.0),
                -(item.get("probSide") or 0.0),
                item.get("code", ""),
            )
        )
    filtered_all: list[dict[str, Any]] = []
    for item in scored_items:
        key = rankings_cache._iso_date_to_int(item.get("asOf"))
        if key is not None and key > as_of_int:
            continue
        sanitized = rankings_cache._sanitize_rank_item_for_json(item)
        if quality_mode == "rule":
            sanitized.setdefault("qualityFlags", item.get("qualityFlags"))
        filtered_all.append(sanitized)
    filtered_qualified: list[dict[str, Any]] = []
    for item in qualified_items:
        key = rankings_cache._iso_date_to_int(item.get("asOf"))
        if key is not None and key > as_of_int:
            continue
        filtered_qualified.append(rankings_cache._sanitize_rank_item_for_json(item))
    return filtered_all[:limit], filtered_qualified[:limit], pred_dt, model_version


def rebuild_signal_decisions(
    *,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    logic_version: str | None = None,
    side: str = "all",
    basis_version: str | None = None,
    reset_scope: bool = False,
    db_path: str | None = None,
    progress_cb: TrackingProgressCallback | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side, allow_all=True)
    with _open_conn(db_path) as conn:
        resolved_logic_version, default_basis_version = _resolve_logic_version(conn, logic_version)
    resolved_basis_version = str(basis_version or default_basis_version or DEFAULT_BASIS_VERSION)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    target_sides = list(SUPPORTED_SIDES) if normalized_side == "all" else [normalized_side]
    with _open_conn(db_path, read_only=True) as conn:
        where_parts = ["basis_version = ?"]
        params: list[Any] = [resolved_basis_version]
        if from_int is not None:
            where_parts.append("dt >= ?")
            params.append(int(from_int))
        if to_int is not None:
            where_parts.append("dt <= ?")
            params.append(int(to_int))
        rows = conn.execute(
            f"SELECT DISTINCT dt FROM signal_basis_daily WHERE {' AND '.join(where_parts)} ORDER BY dt",
            params,
        ).fetchall()
        basis_dates = [int(row[0]) for row in rows if row and row[0] is not None]
    decision_rows: list[list[Any]] = []
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
        start_ymd_for_bars = basis_dates[0] if basis_dates else from_int
        total_units = max(1, len(basis_dates) * max(1, len(target_sides)))
        progress_state: dict[str, Any] = {"last_emit_at": 0.0}
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="decisions",
                status="start",
                processed=0,
                total=total_units,
                current_market_ymd=basis_dates[0] if basis_dates else None,
                current_market_date=_ymd_to_iso(basis_dates[0]) if basis_dates else None,
                detail=f"rebuilding decision rows (version={resolved_logic_version})",
            ),
            throttle_state=progress_state,
            force=True,
        )
        completed_units = 0
        for dt in basis_dates:
            items, buy_rank, sell_rank = _load_basis_items_for_date(conn, dt=dt, basis_version=resolved_basis_version)
            if not items:
                continue
            grouped_bars: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
            for basis_item in items:
                code = str(basis_item.get("code") or "").strip()
                if not code or code in grouped_bars:
                    continue
                cached = bars_cache.get(code)
                if cached is None:
                    bars = _fetch_code_bars(
                        conn,
                        code=code,
                        start_ymd=int(start_ymd_for_bars or dt),
                    )
                    cached = (bars, _bar_price_lookup(bars))
                    bars_cache[code] = cached
                grouped_bars[code] = cached
            for target_side in target_sides:
                evaluated_all, evaluated_ranked, pred_dt, model_version = _call_evaluate_trade_items_from_basis(
                    items=items,
                    as_of_int=dt,
                    side=target_side,
                    logic_version=resolved_logic_version,
                )
                source_rank_map = buy_rank if target_side == "buy" else sell_rank
                ranked_code_map = {
                    str(item.get("code") or "").strip(): index + 1
                    for index, item in enumerate(evaluated_ranked, start=1)
                    if str(item.get("code") or "").strip()
                }
                for item in evaluated_all:
                    code = str(item.get("code") or "").strip()
                    if not code:
                        continue
                    bars, by_date = grouped_bars.get(code, ([], {}))
                    forward_returns = _compute_directional_forward_returns(target_side, bars=bars, by_date=by_date, signal_date=dt)
                    idx = by_date.get(dt)
                    entry_close_price = bars[idx].close if idx is not None and idx < len(bars) else None
                    entry_next_open = bars[idx + 1].open if idx is not None and idx + 1 < len(bars) else None
                    window_metrics = (
                        _compute_window_metrics(
                            side=target_side,
                            bars=bars,
                            start_index=idx,
                            anchor_close_price=entry_close_price,
                            anchor_exec_price=entry_next_open,
                            latest_market_ymd=latest_market_ymd,
                        )
                        if idx is not None and idx < len(bars)
                        else {}
                    )
                    reason_snapshot = _build_reason_snapshot(item)
                    score_snapshot = _build_score_snapshot(item)
                    rank_snapshot = _build_rank_snapshot(
                        item,
                        final_rank=ranked_code_map.get(code),
                        source_rank=source_rank_map.get(code),
                    )
                    decision_rows.append(
                        [
                            int(dt),
                            code,
                            target_side,
                            resolved_logic_version,
                            resolved_basis_version,
                            str(item.get("name") or code),
                            bool(item.get("entryQualified") is True),
                            str(item.get("setupType") or "").strip() or None,
                            _json_dump(reason_snapshot),
                            _json_dump(score_snapshot),
                            _json_dump(rank_snapshot),
                            _hash_json_payload(
                                {
                                    "dt": dt,
                                    "code": code,
                                    "side": target_side,
                                    "logic_version": resolved_logic_version,
                                    "pred_dt": pred_dt,
                                    "model_version": model_version,
                                    "reason": reason_snapshot,
                                    "score": score_snapshot,
                                    "rank": rank_snapshot,
                                }
                            ),
                            forward_returns[5],
                            forward_returns[10],
                            forward_returns[20],
                            forward_returns[30],
                            forward_returns[60],
                            window_metrics.get("max_favorable_return"),
                            window_metrics.get("max_adverse_return"),
                            window_metrics.get("days_to_max_favorable_30"),
                            window_metrics.get("days_to_max_adverse_30"),
                            window_metrics.get("date_of_max_favorable_30"),
                            window_metrics.get("date_of_max_adverse_30"),
                            datetime.now(timezone.utc),
                        ]
                    )
                completed_units += 1
                _emit_tracking_progress(
                    progress_cb,
                    _tracking_progress_event(
                        phase="decisions",
                        status="running",
                        processed=completed_units,
                        total=total_units,
                        current_market_ymd=dt,
                        current_market_date=_ymd_to_iso(dt),
                        current_side=target_side,
                        detail=f"dt={dt} side={target_side}",
                    ),
                    throttle_state=progress_state,
                )
    with _open_conn(db_path) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            if reset_scope:
                delete_where = ["logic_version = ?"]
                delete_params: list[Any] = [resolved_logic_version]
                if normalized_side != "all":
                    delete_where.append("side = ?")
                    delete_params.append(normalized_side)
                if from_int is not None:
                    delete_where.append("dt >= ?")
                    delete_params.append(int(from_int))
                if to_int is not None:
                    delete_where.append("dt <= ?")
                    delete_params.append(int(to_int))
                conn.execute(f"DELETE FROM signal_decision_daily WHERE {' AND '.join(delete_where)}", delete_params)
            _bulk_insert_or_replace_rows(
                conn,
                table_name="signal_decision_daily",
                columns=[
                    "dt",
                    "code",
                    "side",
                    "logic_version",
                    "basis_version",
                    "name",
                    "entry_qualified",
                    "setup_type",
                    "reason_snapshot_json",
                    "score_snapshot_json",
                    "rank_snapshot_json",
                    "decision_hash",
                    "forward_return_5",
                    "forward_return_10",
                    "forward_return_20",
                    "forward_return_30",
                    "forward_return_60",
                    "max_favorable_30",
                    "max_adverse_30",
                    "days_to_max_favorable_30",
                    "days_to_max_adverse_30",
                    "date_of_max_favorable_30",
                    "date_of_max_adverse_30",
                    "updated_at",
                ],
                rows=decision_rows,
            )
            conn.execute(
                "UPDATE signal_logic_registry SET basis_version = COALESCE(?, basis_version) WHERE logic_version = ?",
                [resolved_basis_version, resolved_logic_version],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="decisions",
            status="done",
            processed=completed_units,
            total=total_units,
            current_market_ymd=basis_dates[-1] if basis_dates else None,
            current_market_date=_ymd_to_iso(basis_dates[-1]) if basis_dates else None,
            detail=f"decision_rows={len(decision_rows)}",
        ),
        throttle_state=progress_state,
        force=True,
    )
    return {
        "ok": True,
        "logic_version": resolved_logic_version,
        "basis_version": resolved_basis_version,
        "side": normalized_side,
        "dates_processed": len(basis_dates),
        "decision_upserted": len(decision_rows),
        "from": _ymd_to_iso(basis_dates[0]) if basis_dates else _ymd_to_iso(from_int),
        "to": _ymd_to_iso(basis_dates[-1]) if basis_dates else _ymd_to_iso(to_int),
    }


def _status_for_campaign(
    *,
    latest_market_ymd: int | None,
    completed_ymd: int | None,
    elapsed_bars: int,
) -> tuple[str, datetime | None, datetime | None]:
    completed_at = None
    archived_at = None
    if elapsed_bars < WATCH_HORIZON_BARS or completed_ymd is None:
        return "active", completed_at, archived_at
    completed_date = _ymd_to_date(completed_ymd)
    if completed_date is not None:
        completed_at = datetime.combine(completed_date, datetime.min.time(), tzinfo=timezone.utc)
    latest_market_date = _ymd_to_date(latest_market_ymd)
    if latest_market_date is not None and completed_date is not None:
        if latest_market_date >= completed_date + timedelta(days=COMPLETED_RETENTION_DAYS):
            archived_at = datetime.combine(
                completed_date + timedelta(days=COMPLETED_RETENTION_DAYS),
                datetime.min.time(),
                tzinfo=timezone.utc,
            )
            return "archive", completed_at, archived_at
    return "completed", completed_at, archived_at


def rebuild_signal_campaigns(
    *,
    logic_version: str | None = None,
    side: str = "all",
    db_path: str | None = None,
    progress_cb: TrackingProgressCallback | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side, allow_all=True)
    with _open_conn(db_path) as conn:
        resolved_logic_version, _ = _resolve_logic_version(conn, logic_version)
        latest_market_ymd = _latest_market_ymd(conn)
        target_sides = list(SUPPORTED_SIDES) if normalized_side == "all" else [normalized_side]
        decision_rows = conn.execute(
            f"""
            SELECT
                dt,
                code,
                side,
                basis_version,
                logic_version,
                name,
                reason_snapshot_json,
                score_snapshot_json
            FROM signal_decision_daily
            WHERE logic_version = ?
              AND side IN ({", ".join(["?"] * len(target_sides))})
              AND entry_qualified = TRUE
            ORDER BY side, code, dt
            """,
            [resolved_logic_version, *target_sides],
        ).fetchall()
        progress_state: dict[str, Any] = {"last_emit_at": 0.0}
        total_rows = len(decision_rows)
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="campaigns",
                status="start",
                processed=0,
                total=total_rows,
                detail=f"rebuilding campaigns (logic_version={resolved_logic_version})",
            ),
            throttle_state=progress_state,
            force=True,
        )
        conn.execute(
            f"DELETE FROM signal_occurrence WHERE logic_version = ? AND side IN ({', '.join(['?'] * len(target_sides))})",
            [resolved_logic_version, *target_sides],
        )
        conn.execute(
            f"DELETE FROM signal_campaign WHERE logic_version = ? AND side IN ({', '.join(['?'] * len(target_sides))})",
            [resolved_logic_version, *target_sides],
        )
        if not decision_rows:
            _emit_tracking_progress(
                progress_cb,
                _tracking_progress_event(
                    phase="campaigns",
                    status="done",
                    processed=0,
                    total=0,
                    detail="campaigns=0 occurrences=0",
                ),
                throttle_state=progress_state,
                force=True,
            )
            return {
                "ok": True,
                "logic_version": resolved_logic_version,
                "side": normalized_side,
                "occurrence_upserted": 0,
                "campaign_count": 0,
            }
        grouped: dict[tuple[str, str], list[SignalOccurrence]] = {}
        for index, (dt, code, row_side, basis_version, row_logic_version, _name, reason_json, score_json) in enumerate(decision_rows, start=1):
            bars = _fetch_code_bars(conn, code=str(code), start_ymd=int(dt))
            by_date = _bar_price_lookup(bars)
            idx = by_date.get(int(dt))
            close_price = bars[idx].close if idx is not None and idx < len(bars) else None
            next_open_price = bars[idx + 1].open if idx is not None and idx + 1 < len(bars) else None
            occurrence = SignalOccurrence(
                occurrence_id=f"{row_logic_version}:{row_side}:{code}:{int(dt)}",
                campaign_id=None,
                code=str(code),
                side=str(row_side),
                signal_date=int(dt),
                basis_version=str(basis_version or DEFAULT_BASIS_VERSION),
                logic_version=str(row_logic_version),
                reason_snapshot_json=str(reason_json) if reason_json is not None else None,
                score_snapshot_json=str(score_json) if score_json is not None else None,
                entry_close_price=close_price,
                entry_next_open_price=next_open_price,
            )
            grouped.setdefault((occurrence.side, occurrence.code), []).append(occurrence)
            _emit_tracking_progress(
                progress_cb,
                _tracking_progress_event(
                    phase="campaigns",
                    status="running",
                    processed=index,
                    total=total_rows,
                    current_market_ymd=int(dt),
                    current_market_date=_ymd_to_iso(int(dt)),
                    current_side=str(row_side),
                    detail=f"decision_row={index}/{total_rows}",
                ),
                throttle_state=progress_state,
            )
        occurrence_rows: list[list[Any]] = []
        campaign_rows: list[list[Any]] = []
        name_rows = conn.execute("SELECT code, name FROM stock_meta").fetchall()
        names = {str(row[0]): (str(row[1]) if row[1] is not None else None) for row in name_rows}
        for (row_side, code), occurrences in grouped.items():
            occurrences.sort(key=lambda item: (item.signal_date, item.occurrence_id))
            bars = _fetch_code_bars(conn, code=code, start_ymd=occurrences[0].signal_date)
            by_date = _bar_price_lookup(bars)
            current_group: list[SignalOccurrence] = []
            current_completion_index: int | None = None

            def flush_group(group: list[SignalOccurrence]) -> None:
                if not group:
                    return
                first_occurrence = group[0]
                first_index = by_date.get(first_occurrence.signal_date)
                if first_index is None:
                    return
                horizon_end_index = min(first_index + WATCH_HORIZON_BARS - 1, len(bars) - 1)
                observed_end_index = horizon_end_index
                if latest_market_ymd is not None:
                    visible_indexes = [index for index, bar in enumerate(bars) if bar.date <= latest_market_ymd]
                    if visible_indexes:
                        observed_end_index = min(horizon_end_index, visible_indexes[-1])
                elapsed_bars = max(0, observed_end_index - first_index + 1)
                remaining_bars = max(0, WATCH_HORIZON_BARS - elapsed_bars)
                last_bar = bars[observed_end_index] if elapsed_bars > 0 else bars[first_index]
                window_metrics = _compute_window_metrics(
                    side=row_side,
                    bars=bars,
                    start_index=first_index,
                    anchor_close_price=first_occurrence.entry_close_price,
                    anchor_exec_price=first_occurrence.entry_next_open_price,
                    latest_market_ymd=latest_market_ymd,
                )
                completed_idx = first_index + WATCH_HORIZON_BARS - 1
                completed_ymd = bars[completed_idx].date if completed_idx < len(bars) else None
                status, completed_at, archived_at = _status_for_campaign(
                    latest_market_ymd=latest_market_ymd,
                    completed_ymd=completed_ymd,
                    elapsed_bars=elapsed_bars,
                )
                campaign_id = f"{resolved_logic_version}:{row_side}:{code}:{first_occurrence.signal_date}"
                campaign_rows.append(
                    [
                        campaign_id,
                        code,
                        row_side,
                        first_occurrence.basis_version,
                        resolved_logic_version,
                        names.get(code),
                        first_occurrence.signal_date,
                        group[-1].signal_date,
                        WATCH_HORIZON_BARS,
                        elapsed_bars,
                        remaining_bars,
                        status,
                        first_occurrence.entry_close_price,
                        first_occurrence.entry_next_open_price,
                        last_bar.close,
                        _safe_directional_return(row_side, last_bar.close, first_occurrence.entry_close_price),
                        _safe_directional_return(row_side, last_bar.close, first_occurrence.entry_next_open_price),
                        window_metrics.get("max_favorable_return"),
                        window_metrics.get("max_adverse_return"),
                        window_metrics.get("days_to_max_favorable_30"),
                        window_metrics.get("days_to_max_adverse_30"),
                        window_metrics.get("date_of_max_favorable_30"),
                        window_metrics.get("date_of_max_adverse_30"),
                        _safe_directional_return(row_side, bars[completed_idx].close, first_occurrence.entry_close_price)
                        if completed_idx < len(bars)
                        else None,
                        len(group),
                        resolved_logic_version,
                        first_occurrence.reason_snapshot_json,
                        group[-1].reason_snapshot_json,
                        completed_at,
                        archived_at,
                        datetime.now(timezone.utc),
                    ]
                )
                for occurrence in group:
                    occurrence_rows.append(
                        [
                            occurrence.occurrence_id,
                            campaign_id,
                            occurrence.code,
                            occurrence.side,
                            occurrence.signal_date,
                            occurrence.basis_version,
                            occurrence.logic_version,
                            occurrence.reason_snapshot_json,
                            occurrence.score_snapshot_json,
                            occurrence.entry_close_price,
                            occurrence.entry_next_open_price,
                        ]
                    )

            for occurrence in occurrences:
                occ_index = by_date.get(occurrence.signal_date)
                if occ_index is None:
                    continue
                if not current_group:
                    current_group = [occurrence]
                    current_completion_index = occ_index + WATCH_HORIZON_BARS - 1
                    continue
                if current_completion_index is not None and occ_index <= current_completion_index:
                    current_group.append(occurrence)
                    continue
                flush_group(current_group)
                current_group = [occurrence]
                current_completion_index = occ_index + WATCH_HORIZON_BARS - 1
            flush_group(current_group)
        if occurrence_rows:
            conn.executemany(
                """
                INSERT INTO signal_occurrence (
                    occurrence_id,
                    campaign_id,
                    code,
                    side,
                    signal_date,
                    basis_version,
                    logic_version,
                    reason_snapshot_json,
                    score_snapshot_json,
                    entry_close_price,
                    entry_next_open_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                occurrence_rows,
            )
        if campaign_rows:
            conn.executemany(
                """
                INSERT INTO signal_campaign (
                    campaign_id,
                    code,
                    side,
                    basis_version,
                    logic_version,
                    name,
                    first_signal_date,
                    latest_signal_date,
                    watch_horizon_bars,
                    elapsed_bars,
                    remaining_bars,
                    status,
                    anchor_price_close,
                    anchor_price_next_open,
                    last_price,
                    favorable_return_close_basis,
                    favorable_return_exec_basis,
                    max_favorable_return,
                    max_adverse_return,
                    days_to_max_favorable_30,
                    days_to_max_adverse_30,
                    date_of_max_favorable_30,
                    date_of_max_adverse_30,
                    final_return_at_horizon,
                    signal_count,
                    logic_version_first,
                    first_reason_snapshot_json,
                    latest_reason_snapshot_json,
                    completed_at,
                    archived_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                campaign_rows,
            )
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="campaigns",
                status="done",
                processed=total_rows,
                total=total_rows,
                detail=f"campaigns={len(campaign_rows)} occurrences={len(occurrence_rows)}",
            ),
            throttle_state=progress_state,
            force=True,
        )
        return {
            "ok": True,
            "logic_version": resolved_logic_version,
            "side": normalized_side,
            "occurrence_upserted": len(occurrence_rows),
            "campaign_count": len(campaign_rows),
        }


def refresh_signal_tracking(
    *,
    as_of: int | str | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    db_path: str | None = None,
) -> dict[str, Any]:
    as_of_int = _coerce_ymd(as_of)
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
    effective_as_of = as_of_int or latest_market_ymd
    if effective_as_of is None:
        return {
            "ok": True,
            "as_of": None,
            "as_of_int": None,
            "basis_rows_upserted": 0,
            "decision_upserted": 0,
            "campaign_count": 0,
        }
    basis_result = backfill_signal_basis(
        from_ymd=effective_as_of,
        to_ymd=effective_as_of,
        basis_version=DEFAULT_BASIS_VERSION,
        reset_scope=False,
        db_path=db_path,
    )
    decision_result = rebuild_signal_decisions(
        from_ymd=effective_as_of,
        to_ymd=effective_as_of,
        logic_version=ACTIVE_LOGIC_VERSION_ALIAS,
        side="all",
        basis_version=DEFAULT_BASIS_VERSION,
        reset_scope=False,
        db_path=db_path,
    )
    campaign_result = rebuild_signal_campaigns(
        logic_version=ACTIVE_LOGIC_VERSION_ALIAS,
        side="all",
        db_path=db_path,
    )
    ranking_result = rebuild_ranking_appearances(
        from_ymd=effective_as_of,
        to_ymd=effective_as_of,
        ranking_logic_version=ACTIVE_RANKING_LOGIC_VERSION_ALIAS,
        signal_logic_version=ACTIVE_LOGIC_VERSION_ALIAS,
        basis_version=DEFAULT_BASIS_VERSION,
        reset_scope=False,
        db_path=db_path,
    )
    with _REFRESH_LOCK:
        _REFRESH_STATE["as_of"] = effective_as_of
        _REFRESH_STATE["refreshed_at"] = datetime.now(timezone.utc)
    return {
        "ok": True,
        "as_of": _ymd_to_iso(effective_as_of),
        "as_of_int": effective_as_of,
        "basis_rows_upserted": int(basis_result.get("rows_upserted") or 0),
        "decision_upserted": int(decision_result.get("decision_upserted") or 0),
        "campaign_count": int(campaign_result.get("campaign_count") or 0),
        "ranking_appearance_upserted": int(ranking_result.get("appearance_upserted") or 0),
        "lookback_days": int(lookback_days or DEFAULT_LOOKBACK_DAYS),
        "limit": int(limit or DEFAULT_LIMIT),
    }


def refresh_daily_tracking_window(
    *,
    market_day_window: int | None = None,
    db_path: str | None = None,
    progress_cb: TrackingProgressCallback | None = None,
) -> dict[str, Any]:
    started_at = time.monotonic()
    requested_window = max(1, int(market_day_window or (WATCH_HORIZON_BARS + COMPLETED_RETENTION_DAYS)))
    with _open_conn(db_path, read_only=True) as conn:
        market_dates = _list_market_dates(conn)
    logger.info(
        "tracking_refresh start window=%s market_dates=%s db_path=%s",
        requested_window,
        len(market_dates),
        db_path,
    )
    progress_state: dict[str, Any] = {"last_emit_at": 0.0}
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="prepare",
            status="start",
            processed=0,
            total=6,
            current_market_ymd=market_dates[-1] if market_dates else None,
            current_market_date=_ymd_to_iso(market_dates[-1]) if market_dates else None,
            detail=f"window={requested_window} dates={len(market_dates)}",
        ),
        throttle_state=progress_state,
        force=True,
    )
    if not market_dates:
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="prepare",
                status="done",
                processed=0,
                total=6,
                detail="no market dates",
            ),
            throttle_state=progress_state,
            force=True,
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "no_market_dates",
            "market_day_window": requested_window,
            "from": None,
            "to": None,
            "from_int": None,
            "to_int": None,
        }
    from_index = max(0, len(market_dates) - requested_window)
    from_int = int(market_dates[from_index])
    to_int = int(market_dates[-1])

    phase_timers: dict[str, float] = {}

    def _forward_progress(event: dict[str, Any]) -> None:
        phase = str(event.get("phase") or "unknown")
        status = str(event.get("status") or "running")
        substage = f"tracking_refresh.{phase}"
        forwarded = dict(event)
        forwarded["stage"] = "tracking_refresh"
        forwarded["substage"] = substage
        forwarded.setdefault("current_market_ymd", event.get("current_market_ymd"))
        forwarded.setdefault("current_market_date", event.get("current_market_date"))
        forwarded["heartbeat_at"] = _tracking_heartbeat_at()
        if status == "start":
            phase_timers[substage] = time.monotonic()
            logger.info(
                "tracking_refresh substage start: %s detail=%s processed=%s/%s current=%s",
                substage,
                forwarded.get("detail"),
                forwarded.get("processed"),
                forwarded.get("total"),
                forwarded.get("current_market_date"),
            )
        elif status == "done":
            started = phase_timers.pop(substage, None)
            elapsed = time.monotonic() - started if started is not None else None
            logger.info(
                "tracking_refresh substage done: %s elapsed=%s detail=%s processed=%s/%s current=%s",
                substage,
                f"{elapsed:.1f}s" if elapsed is not None else "unknown",
                forwarded.get("detail"),
                forwarded.get("processed"),
                forwarded.get("total"),
                forwarded.get("current_market_date"),
            )
        _emit_tracking_progress(progress_cb, forwarded, throttle_state=progress_state)

    _forward_progress(
        _tracking_progress_event(
            phase="prepare",
            status="start",
            processed=0,
            total=6,
            current_market_ymd=from_int,
            current_market_date=_ymd_to_iso(from_int) if from_int is not None else None,
            detail="preparing backfill",
        )
    )
    result = backfill_signal_tracking(
        from_ymd=from_int,
        to_ymd=to_int,
        logic_version=ACTIVE_LOGIC_VERSION_ALIAS,
        basis_version=DEFAULT_BASIS_VERSION,
        reset_scope=False,
        db_path=db_path,
        progress_cb=_forward_progress,
    )
    with _REFRESH_LOCK:
        _REFRESH_STATE["as_of"] = to_int
        _REFRESH_STATE["refreshed_at"] = datetime.now(timezone.utc)
    _forward_progress(
        _tracking_progress_event(
            phase="finalize",
            status="start",
            processed=5,
            total=6,
            current_market_ymd=to_int,
            current_market_date=_ymd_to_iso(to_int),
            detail="writing refresh state",
        )
    )
    elapsed_sec = time.monotonic() - started_at
    logger.info(
        "tracking_refresh complete window=%s market_dates=%s elapsed=%.1fs",
        requested_window,
        len(market_dates),
        elapsed_sec,
    )
    _forward_progress(
        _tracking_progress_event(
            phase="finalize",
            status="done",
            processed=6,
            total=6,
            current_market_ymd=to_int,
            current_market_date=_ymd_to_iso(to_int),
            detail="tracking refresh completed",
        )
    )
    return {
        "ok": True,
        "market_day_window": requested_window,
        "from": _ymd_to_iso(from_int),
        "to": _ymd_to_iso(to_int),
        "from_int": from_int,
        "to_int": to_int,
        "result": result,
    }


def ensure_signal_tracking_current(
    *,
    as_of: int | str | None = None,
    max_age_sec: int = 300,
    db_path: str | None = None,
) -> dict[str, Any]:
    as_of_int = _coerce_ymd(as_of)
    now = datetime.now(timezone.utc)
    should_refresh = False
    with _REFRESH_LOCK:
        last_refresh = _REFRESH_STATE.get("refreshed_at")
        last_as_of = _REFRESH_STATE.get("as_of")
        should_refresh = (
            last_refresh is None
            or (now - last_refresh).total_seconds() >= max(30, int(max_age_sec))
            or (as_of_int is not None and last_as_of != as_of_int)
        )
    if should_refresh:
        return refresh_signal_tracking(as_of=as_of_int, db_path=db_path)
    return {
        "ok": True,
        "skipped": True,
        "as_of_int": as_of_int,
        "refreshed_at": _serialize_timestamp(_REFRESH_STATE.get("refreshed_at")),
    }


def _campaign_rows_with_additional_dates(
    conn: duckdb.DuckDBPyConnection,
    campaign_rows: list[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    campaign_ids = [str(row[0]) for row in campaign_rows]
    additional_dates_map: dict[str, list[int]] = {campaign_id: [] for campaign_id in campaign_ids}
    if campaign_ids:
        placeholders = ", ".join(["?"] * len(campaign_ids))
        occurrence_rows = conn.execute(
            f"""
            SELECT campaign_id, signal_date
            FROM signal_occurrence
            WHERE campaign_id IN ({placeholders})
            ORDER BY campaign_id, signal_date
            """,
            campaign_ids,
        ).fetchall()
        grouped_dates: dict[str, list[int]] = {}
        for campaign_id, signal_date in occurrence_rows:
            grouped_dates.setdefault(str(campaign_id), []).append(int(signal_date))
        for campaign_id, dates in grouped_dates.items():
            additional_dates_map[campaign_id] = dates[1:] if len(dates) > 1 else []
    items: list[dict[str, Any]] = []
    for row in campaign_rows:
        campaign_id = str(row[0])
        additional_dates = additional_dates_map.get(campaign_id, [])
        items.append(
            {
                "campaign_id": campaign_id,
                "code": str(row[1]),
                "side": str(row[2]),
                "basis_version": str(row[3]) if row[3] is not None else DEFAULT_BASIS_VERSION,
                "logic_version": str(row[4]) if row[4] is not None else DEFAULT_LOGIC_VERSION,
                "name": str(row[5]) if row[5] is not None else None,
                "first_signal_date": int(row[6]),
                "firstSignalDate": _ymd_to_iso(int(row[6])),
                "latest_signal_date": int(row[7]),
                "latestSignalDate": _ymd_to_iso(int(row[7])),
                "watch_horizon_bars": int(row[8]),
                "elapsed_bars": int(row[9]),
                "remaining_bars": int(row[10]),
                "status": str(row[11]),
                "anchor_price_close": float(row[12]) if row[12] is not None else None,
                "anchor_price_next_open": float(row[13]) if row[13] is not None else None,
                "last_price": float(row[14]) if row[14] is not None else None,
                "favorable_return_close_basis": float(row[15]) if row[15] is not None else None,
                "favorable_return_exec_basis": float(row[16]) if row[16] is not None else None,
                "max_favorable_return": float(row[17]) if row[17] is not None else None,
                "max_adverse_return": float(row[18]) if row[18] is not None else None,
                "days_to_max_favorable_30": int(row[19]) if row[19] is not None else None,
                "days_to_max_adverse_30": int(row[20]) if row[20] is not None else None,
                "date_of_max_favorable_30": int(row[21]) if row[21] is not None else None,
                "date_of_max_adverse_30": int(row[22]) if row[22] is not None else None,
                "final_return_at_horizon": float(row[23]) if row[23] is not None else None,
                "signal_count": int(row[24]),
                "logic_version_first": str(row[25]) if row[25] is not None else None,
                "completed_at": _serialize_timestamp(row[26]) if isinstance(row[26], datetime) else None,
                "archived_at": _serialize_timestamp(row[27]) if isinstance(row[27], datetime) else None,
                "updated_at": _serialize_timestamp(row[28]) if isinstance(row[28], datetime) else None,
                "additional_signal_dates": additional_dates,
                "additionalSignalDates": [_ymd_to_iso(value) for value in additional_dates],
            }
        )
    return items


def list_signal_campaigns(
    *,
    status: str = DEFAULT_STATUS,
    side: str = "buy",
    logic_version: str | None = None,
    query: str | None = None,
    limit: int = 100,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_status = str(status or DEFAULT_STATUS).strip().lower()
    if normalized_status not in SUPPORTED_STATUSES:
        raise ValueError("status must be active|completed|archive")
    normalized_side = _normalize_side(side)
    ensure_signal_tracking_current(as_of=as_of, db_path=db_path)
    with _open_conn(db_path) as conn:
        resolved_logic_version, _ = _resolve_logic_version(conn, logic_version)
    search = str(query or "").strip()
    where_parts = ["status = ?", "side = ?", "logic_version = ?"]
    params: list[Any] = [normalized_status, normalized_side, resolved_logic_version]
    if search:
        where_parts.append("(code LIKE ? OR COALESCE(name, '') LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    order_sql = "remaining_bars ASC, first_signal_date ASC, campaign_id ASC" if normalized_status == "active" else "completed_at DESC NULLS LAST, campaign_id ASC"
    with _open_conn(db_path, read_only=True) as conn:
        rows = conn.execute(
            f"""
            SELECT
                campaign_id,
                code,
                side,
                basis_version,
                logic_version,
                name,
                first_signal_date,
                latest_signal_date,
                watch_horizon_bars,
                elapsed_bars,
                remaining_bars,
                status,
                anchor_price_close,
                anchor_price_next_open,
                last_price,
                favorable_return_close_basis,
                favorable_return_exec_basis,
                max_favorable_return,
                max_adverse_return,
                days_to_max_favorable_30,
                days_to_max_adverse_30,
                date_of_max_favorable_30,
                date_of_max_adverse_30,
                final_return_at_horizon,
                signal_count,
                logic_version_first,
                completed_at,
                archived_at,
                updated_at
            FROM signal_campaign
            WHERE {" AND ".join(where_parts)}
            ORDER BY {order_sql}
            LIMIT ?
            """,
            [*params, max(1, min(int(limit or 100), 500))],
        ).fetchall()
        items = _campaign_rows_with_additional_dates(conn, rows)
    return {
        "status": normalized_status,
        "side": normalized_side,
        "logic_version": resolved_logic_version,
        "items": items,
        "count": len(items),
        "query": search or None,
    }


def get_signal_campaign_detail(
    campaign_id: str,
    *,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    campaign_id = str(campaign_id or "").strip()
    if not campaign_id:
        raise ValueError("campaign_id is required")
    ensure_signal_tracking_current(as_of=as_of, db_path=db_path)
    with _open_conn(db_path, read_only=True) as conn:
        row = conn.execute(
            """
            SELECT
                campaign_id,
                code,
                side,
                basis_version,
                logic_version,
                name,
                first_signal_date,
                latest_signal_date,
                watch_horizon_bars,
                elapsed_bars,
                remaining_bars,
                status,
                anchor_price_close,
                anchor_price_next_open,
                last_price,
                favorable_return_close_basis,
                favorable_return_exec_basis,
                max_favorable_return,
                max_adverse_return,
                days_to_max_favorable_30,
                days_to_max_adverse_30,
                date_of_max_favorable_30,
                date_of_max_adverse_30,
                final_return_at_horizon,
                signal_count,
                logic_version_first,
                first_reason_snapshot_json,
                latest_reason_snapshot_json,
                completed_at,
                archived_at,
                updated_at
            FROM signal_campaign
            WHERE campaign_id = ?
            """,
            [campaign_id],
        ).fetchone()
        if not row:
            raise KeyError(campaign_id)
        campaign = {
            "campaign_id": str(row[0]),
            "code": str(row[1]),
            "side": str(row[2]),
            "basis_version": str(row[3]) if row[3] is not None else DEFAULT_BASIS_VERSION,
            "logic_version": str(row[4]) if row[4] is not None else DEFAULT_LOGIC_VERSION,
            "name": str(row[5]) if row[5] is not None else None,
            "first_signal_date": int(row[6]),
            "firstSignalDate": _ymd_to_iso(int(row[6])),
            "latest_signal_date": int(row[7]),
            "latestSignalDate": _ymd_to_iso(int(row[7])),
            "watch_horizon_bars": int(row[8]),
            "elapsed_bars": int(row[9]),
            "remaining_bars": int(row[10]),
            "status": str(row[11]),
            "anchor_price_close": float(row[12]) if row[12] is not None else None,
            "anchor_price_next_open": float(row[13]) if row[13] is not None else None,
            "last_price": float(row[14]) if row[14] is not None else None,
            "favorable_return_close_basis": float(row[15]) if row[15] is not None else None,
            "favorable_return_exec_basis": float(row[16]) if row[16] is not None else None,
            "max_favorable_return": float(row[17]) if row[17] is not None else None,
            "max_adverse_return": float(row[18]) if row[18] is not None else None,
            "days_to_max_favorable_30": int(row[19]) if row[19] is not None else None,
            "days_to_max_adverse_30": int(row[20]) if row[20] is not None else None,
            "date_of_max_favorable_30": int(row[21]) if row[21] is not None else None,
            "date_of_max_adverse_30": int(row[22]) if row[22] is not None else None,
            "final_return_at_horizon": float(row[23]) if row[23] is not None else None,
            "signal_count": int(row[24]),
            "logic_version_first": str(row[25]) if row[25] is not None else None,
            "first_reason_snapshot": _json_load(str(row[26]) if row[26] is not None else None),
            "latest_reason_snapshot": _json_load(str(row[27]) if row[27] is not None else None),
            "completed_at": _serialize_timestamp(row[28]) if isinstance(row[28], datetime) else None,
            "archived_at": _serialize_timestamp(row[29]) if isinstance(row[29], datetime) else None,
            "updated_at": _serialize_timestamp(row[30]) if isinstance(row[30], datetime) else None,
        }
        occurrence_rows = conn.execute(
            """
            SELECT
                occurrence_id,
                signal_date,
                basis_version,
                logic_version,
                reason_snapshot_json,
                score_snapshot_json,
                entry_close_price,
                entry_next_open_price
            FROM signal_occurrence
            WHERE campaign_id = ?
            ORDER BY signal_date, occurrence_id
            """,
            [campaign_id],
        ).fetchall()
        occurrences = []
        signal_dates: list[int] = []
        for occurrence_row in occurrence_rows:
            signal_date = int(occurrence_row[1])
            signal_dates.append(signal_date)
            occurrences.append(
                {
                    "occurrence_id": str(occurrence_row[0]),
                    "signal_date": signal_date,
                    "signalDate": _ymd_to_iso(signal_date),
                    "basis_version": str(occurrence_row[2]) if occurrence_row[2] is not None else DEFAULT_BASIS_VERSION,
                    "logic_version": str(occurrence_row[3]) if occurrence_row[3] is not None else DEFAULT_LOGIC_VERSION,
                    "reason_snapshot": _json_load(str(occurrence_row[4]) if occurrence_row[4] is not None else None),
                    "score_snapshot": _json_load(str(occurrence_row[5]) if occurrence_row[5] is not None else None),
                    "entry_close_price": float(occurrence_row[6]) if occurrence_row[6] is not None else None,
                    "entry_next_open_price": float(occurrence_row[7]) if occurrence_row[7] is not None else None,
                    "is_additional": len(signal_dates) > 1,
                }
            )
        bars = _fetch_code_bars(conn, code=campaign["code"], start_ymd=campaign["first_signal_date"])
        by_date = _bar_price_lookup(bars)
        first_index = by_date.get(campaign["first_signal_date"], 0)
        end_index = min(first_index + campaign["watch_horizon_bars"] - 1, len(bars) - 1) if bars else -1
        price_series = []
        for bar in bars[first_index : end_index + 1] if end_index >= first_index else []:
            price_series.append(
                {
                    "date": bar.date,
                    "date_iso": _ymd_to_iso(bar.date),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "return_close_basis": _safe_directional_return(campaign["side"], bar.close, campaign["anchor_price_close"]),
                    "return_exec_basis": _safe_directional_return(campaign["side"], bar.close, campaign["anchor_price_next_open"]),
                }
            )
        campaign["additional_signal_dates"] = signal_dates[1:]
        campaign["additionalSignalDates"] = [_ymd_to_iso(value) for value in signal_dates[1:]]
    return {"campaign": campaign, "occurrences": occurrences, "price_series": price_series}


def get_signal_tracking_summary(
    *,
    side: str = "buy",
    logic_version: str | None = None,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side)
    ensure_signal_tracking_current(as_of=as_of, db_path=db_path)
    with _open_conn(db_path) as conn:
        resolved_logic_version, _ = _resolve_logic_version(conn, logic_version)
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        rows = conn.execute(
            """
            SELECT status, favorable_return_close_basis, final_return_at_horizon, signal_count
            FROM signal_campaign
            WHERE side = ? AND logic_version = ?
            """,
            [normalized_side, resolved_logic_version],
        ).fetchall()
        event_rows = conn.execute(
            """
            SELECT
                so.occurrence_id,
                so.campaign_id,
                so.code,
                so.side,
                so.signal_date,
                so.basis_version,
                so.logic_version,
                COALESCE(sd.name, sm.name) AS name,
                sd.setup_type,
                so.reason_snapshot_json,
                so.score_snapshot_json,
                so.entry_close_price,
                so.entry_next_open_price
            FROM signal_occurrence AS so
            LEFT JOIN signal_decision_daily AS sd
              ON sd.dt = so.signal_date
             AND sd.code = so.code
             AND sd.side = so.side
             AND sd.logic_version = so.logic_version
            LEFT JOIN stock_meta AS sm
              ON sm.code = so.code
            WHERE so.side = ? AND so.logic_version = ?
            ORDER BY so.signal_date DESC, so.code ASC
            """,
            [normalized_side, resolved_logic_version],
        ).fetchall()
        bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
        decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
        event_items = [
            item
            for row in event_rows
            if (
                item := _build_signal_event_item(
                    conn,
                    occurrence_row=row,
                    latest_market_ymd=latest_market_ymd,
                    bars_cache=bars_cache,
                    decision_rows_cache=decision_rows_cache,
                )
            )
            is not None
        ]
    active_values: list[float] = []
    completed_total = 0
    completed_wins = 0
    total_campaigns = 0
    duplicate_campaigns = 0
    counts = {"active": 0, "completed": 0, "archive": 0}
    for status, favorable_return, final_return, signal_count in rows:
        normalized_status = str(status)
        if normalized_status in counts:
            counts[normalized_status] += 1
        total_campaigns += 1
        if isinstance(signal_count, int) and signal_count > 1:
            duplicate_campaigns += 1
        if normalized_status == "active" and favorable_return is not None:
            active_values.append(float(favorable_return))
        if normalized_status == "completed":
            completed_total += 1
            if final_return is not None and float(final_return) > 0:
                completed_wins += 1
    event_counts = {"active": 0, "completed": 0, "archive": 0}
    event_active_values: list[float] = []
    event_completed_values: list[float] = []
    event_completed_wins = 0
    event_broken = 0
    for item in event_items:
        item_status = str(item["status"])
        event_counts[item_status] = event_counts.get(item_status, 0) + 1
        current_return = item.get("current_directional_return")
        final_return = item.get("return_30d")
        if item_status == "active" and isinstance(current_return, (int, float)):
            event_active_values.append(float(current_return))
        if item_status == "completed" and isinstance(final_return, (int, float)):
            event_completed_values.append(float(final_return))
            if float(final_return) > 0:
                event_completed_wins += 1
        if str(item.get("break_status") or "") == "broken":
            event_broken += 1
    logic_versions = list_logic_versions(db_path=db_path)
    return {
        "side": normalized_side,
        "logic_version": resolved_logic_version,
        "active_logic_version": logic_versions["active_logic_version"],
        "available_logic_versions": logic_versions["items"],
        "active_count": counts["active"],
        "completed_count": counts["completed"],
        "archive_count": counts["archive"],
        "active_average_directional_return": _safe_mean(active_values),
        "completed_win_rate": _safe_ratio(completed_wins, completed_total),
        "duplicate_signal_rate": _safe_ratio(duplicate_campaigns, total_campaigns),
        "event_summary": {
            "active_count": event_counts["active"],
            "completed_count": event_counts["completed"],
            "archive_count": event_counts["archive"],
            "active_average_directional_return": _safe_mean(event_active_values),
            "completed_win_rate": _safe_ratio(event_completed_wins, len(event_completed_values)),
            "break_rate": _safe_ratio(event_broken, len(event_items)),
        },
    }


def _load_signal_event_items_for_validation(
    conn: duckdb.DuckDBPyConnection,
    *,
    side: str,
    logic_version: str,
    from_ymd: int | None,
    to_ymd: int | None,
) -> list[dict[str, Any]]:
    where_parts = ["so.side = ?", "so.logic_version = ?"]
    params: list[Any] = [str(side), str(logic_version)]
    if from_ymd is not None:
        where_parts.append("so.signal_date >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        where_parts.append("so.signal_date <= ?")
        params.append(int(to_ymd))
    occurrence_rows = conn.execute(
        f"""
        SELECT
            so.occurrence_id,
            so.campaign_id,
            so.code,
            so.side,
            so.signal_date,
            so.basis_version,
            so.logic_version,
            COALESCE(sd.name, sm.name) AS name,
            sd.setup_type,
            so.reason_snapshot_json,
            so.score_snapshot_json,
            so.entry_close_price,
            so.entry_next_open_price
        FROM signal_occurrence so
        LEFT JOIN signal_decision_daily sd
          ON sd.dt = so.signal_date
         AND sd.code = so.code
         AND sd.side = so.side
         AND sd.logic_version = so.logic_version
        LEFT JOIN stock_meta sm ON sm.code = so.code
        WHERE {" AND ".join(where_parts)}
        ORDER BY so.signal_date DESC, so.code ASC
        """,
        params,
    ).fetchall()
    latest_market_ymd = _latest_market_ymd(conn)
    bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
    decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
    items: list[dict[str, Any]] = []
    for row in occurrence_rows:
        item = _build_signal_event_item(
            conn,
            occurrence_row=row,
            latest_market_ymd=latest_market_ymd,
            bars_cache=bars_cache,
            decision_rows_cache=decision_rows_cache,
        )
        if item is not None:
            items.append(item)
    return items


def _load_universe_baseline_map(
    conn: duckdb.DuckDBPyConnection,
    *,
    side: str,
    from_ymd: int | None,
    to_ymd: int | None,
    horizon: int = 30,
) -> dict[int, float]:
    clauses: list[str] = []
    params: list[Any] = []
    if from_ymd is not None:
        clauses.append("dt >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        clauses.append("dt <= ?")
        params.append(int(to_ymd))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS dt,
                c AS close,
                c
            FROM daily_bars
        ),
        ordered AS (
            SELECT
                code,
                dt,
                close,
                LEAD(close, {int(horizon)}) OVER (PARTITION BY code ORDER BY dt) AS close_h
            FROM normalized
            {where_sql}
        )
        SELECT
            dt,
            AVG(
                CASE
                    WHEN close IS NULL OR close_h IS NULL OR close <= 0 OR close_h <= 0 THEN NULL
                    WHEN ? = 'sell' THEN (close / close_h) - 1.0
                    ELSE (close_h / close) - 1.0
                END
            ) AS avg_directional_return_h
        FROM ordered
        GROUP BY dt
        """,
        [*params, str(side)],
    ).fetchall()
    return {int(row[0]): float(row[1]) for row in rows if row and row[0] is not None and row[1] is not None}


def _load_market_regime_lookup(
    *,
    db_path: str | None,
    from_ymd: int | None,
    to_ymd: int | None,
) -> dict[int, str]:
    def _materialize_rows() -> None:
        try:
            from app.backend.services.analysis import strategy_backtest_service

            with _REGIME_BUILD_LOCK:
                with _temporary_stocks_db_path(db_path):
                    strategy_backtest_service.build_market_regime_daily(
                        start_dt=from_ymd,
                        end_dt=to_ymd,
                        label_version=DEFAULT_REGIME_LABEL_VERSION,
                    )
        except Exception:
            return

    def _query_rows() -> list[tuple[Any, ...]]:
        with _open_conn(db_path, read_only=True) as inner_conn:
            if not _table_exists(inner_conn, "market_regime_daily"):
                return []
            clauses = ["label_version = ?"]
            params: list[Any] = [DEFAULT_REGIME_LABEL_VERSION]
            if from_ymd is not None:
                clauses.append("dt >= ?")
                params.append(int(from_ymd))
            if to_ymd is not None:
                clauses.append("dt <= ?")
                params.append(int(to_ymd))
            return inner_conn.execute(
                f"""
                SELECT dt, regime_id
                FROM market_regime_daily
                WHERE {" AND ".join(clauses)}
                ORDER BY dt
                """,
                params,
            ).fetchall()

    rows = _query_rows()
    if not rows:
        _materialize_rows()
        rows = _query_rows()
    return {int(row[0]): str(row[1]) for row in rows if row and row[0] is not None and row[1] is not None}


def _summarize_directional_series(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "average_directional_return": _safe_mean(values),
        "directional_hit_rate": _safe_ratio(sum(1 for value in values if value > 0), len(values)),
    }


def get_signal_tracking_validation(
    *,
    logic_version: str | None = None,
    side: str = "buy",
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side)
    ensure_signal_tracking_current(as_of=as_of, db_path=db_path)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    with _open_conn(db_path) as conn:
        resolved_logic_version, resolved_basis_version = _resolve_logic_version(conn, logic_version)
    decision_where = ["side = ?", "logic_version = ?"]
    decision_params: list[Any] = [normalized_side, resolved_logic_version]
    campaign_where = ["side = ?", "logic_version = ?"]
    campaign_params: list[Any] = [normalized_side, resolved_logic_version]
    if from_int is not None:
        decision_where.append("dt >= ?")
        decision_params.append(int(from_int))
        campaign_where.append("first_signal_date >= ?")
        campaign_params.append(int(from_int))
    if to_int is not None:
        decision_where.append("dt <= ?")
        decision_params.append(int(to_int))
        campaign_where.append("first_signal_date <= ?")
        campaign_params.append(int(to_int))
    with _open_conn(db_path, read_only=True) as conn:
        decision_rows = conn.execute(
            f"""
            SELECT
                dt,
                code,
                entry_qualified,
                setup_type,
                forward_return_5,
                forward_return_10,
                forward_return_20,
                forward_return_30,
                forward_return_60,
                days_to_max_favorable_30,
                days_to_max_adverse_30,
                score_snapshot_json
            FROM signal_decision_daily
            WHERE {" AND ".join(decision_where)}
            ORDER BY dt, code
            """,
            decision_params,
        ).fetchall()
        campaign_rows = conn.execute(
            f"""
            SELECT
                code,
                status,
                favorable_return_close_basis,
                max_favorable_return,
                max_adverse_return,
                final_return_at_horizon,
                signal_count,
                first_reason_snapshot_json,
                first_signal_date,
                days_to_max_favorable_30,
                days_to_max_adverse_30
            FROM signal_campaign
            WHERE {" AND ".join(campaign_where)}
            ORDER BY first_signal_date, campaign_id
            """,
            campaign_params,
        ).fetchall()
        event_items = _load_signal_event_items_for_validation(
            conn,
            side=normalized_side,
            logic_version=resolved_logic_version,
            from_ymd=from_int,
            to_ymd=to_int,
        )
        ranking_state = "up" if normalized_side == "buy" else "down"
        ranking_rows = conn.execute(
            """
            SELECT
                dt,
                return_30d,
                signal_state_at_appearance,
                entry_qualified_at_appearance,
                days_to_max_favorable_30,
                days_to_max_adverse_30
            FROM ranking_appearance_daily
            WHERE dir = ?
            ORDER BY dt, rank
            """,
            [ranking_state],
        ).fetchall()
        observed_dates = [int(row[0]) for row in decision_rows if row[0] is not None]
        if observed_dates:
            if from_int is None:
                from_int = min(observed_dates)
            if to_int is None:
                to_int = max(observed_dates)
        regime_lookup = _load_market_regime_lookup(db_path=db_path, from_ymd=from_int, to_ymd=to_int)
        baseline_lookup_10 = _load_universe_baseline_map(
            conn,
            side=normalized_side,
            from_ymd=from_int,
            to_ymd=to_int,
            horizon=10,
        )
        baseline_lookup = _load_universe_baseline_map(
            conn,
            side=normalized_side,
            from_ymd=from_int,
            to_ymd=to_int,
            horizon=30,
        )
    qualified_decision_rows = [row for row in decision_rows if bool(row[2])]
    decision_map_by_key = {
        (int(row[0]), str(row[1])): {
            "code": str(row[1]),
            "return_10d": float(row[5]) if row[5] is not None else None,
            "return_20d": float(row[6]) if row[6] is not None else None,
            "return_30d": float(row[7]) if row[7] is not None else None,
            "setup_type": str(row[3] or "unknown").strip() or "unknown",
            "score_snapshot_json": _json_load(str(row[11]) if row[11] is not None else None) or {},
        }
        for row in qualified_decision_rows
    }
    campaign_signal_count_by_key = {
        (str(row[0]), int(row[8])): int(row[6]) if row[6] is not None else 1
        for row in campaign_rows
        if row[8] is not None
    }
    enriched_event_items: list[dict[str, Any]] = []
    for item in event_items:
        signal_date = item.get("signal_date")
        code = str(item.get("code") or "")
        enriched = dict(item)
        metrics = decision_map_by_key.get((int(signal_date), code)) if signal_date is not None else None
        if metrics:
            enriched["return_10d"] = metrics.get("return_10d")
            enriched["return_20d"] = metrics.get("return_20d")
            enriched["return_30d"] = metrics.get("return_30d")
            enriched["setup_type"] = metrics.get("setup_type") or enriched.get("setup_type")
        else:
            enriched["return_10d"] = None
            enriched["return_20d"] = None
        enriched["signal_count"] = campaign_signal_count_by_key.get(
            (code, int(signal_date)) if signal_date is not None else ("", 0),
            1,
        )
        enriched["regime_tag"] = regime_lookup.get(int(signal_date), "unclassified") if signal_date is not None else "unclassified"
        enriched["baseline_primary"] = baseline_lookup_10.get(int(signal_date)) if signal_date is not None else None
        enriched_event_items.append(enriched)
    qualified_return_5 = [float(row[4]) for row in qualified_decision_rows if row[4] is not None]
    qualified_return_10 = [float(row[5]) for row in qualified_decision_rows if row[5] is not None]
    qualified_return_20 = [float(row[6]) for row in qualified_decision_rows if row[6] is not None]
    qualified_return_30 = [float(row[7]) for row in qualified_decision_rows if row[7] is not None]
    qualified_return_60 = [float(row[8]) for row in qualified_decision_rows if row[8] is not None]
    decision_peak_favorable_days = [int(row[9]) for row in qualified_decision_rows if row[9] is not None]
    decision_peak_adverse_days = [int(row[10]) for row in qualified_decision_rows if row[10] is not None]
    qualified_payload_rows: list[dict[str, Any]] = []
    monthly_groups: dict[str, list[dict[str, Any]]] = {}
    regime_groups: dict[str, list[dict[str, Any]]] = {}
    setup_rows: dict[str, list[tuple[Any, ...]]] = {}
    for row in qualified_decision_rows:
        dt = int(row[0])
        payload = {
            "dt": dt,
            "code": str(row[1]),
            "setup_type": str(row[3] or "unknown").strip() or "unknown",
            "return_10": float(row[5]) if row[5] is not None else None,
            "return_20": float(row[6]) if row[6] is not None else None,
            "return_30": float(row[7]) if row[7] is not None else None,
            "baseline_10": baseline_lookup_10.get(dt),
            "baseline_30": baseline_lookup.get(dt),
            "days_to_max_favorable_30": int(row[9]) if row[9] is not None else None,
            "days_to_max_adverse_30": int(row[10]) if row[10] is not None else None,
            "entryScore": None,
            "tradePriorityScore": None,
            "probSide": None,
        }
        score_snapshot = _json_load(str(row[11]) if len(row) > 11 and row[11] is not None else None) or {}
        if isinstance(score_snapshot, dict):
            for key in _SCORE_THRESHOLD_KEYS:
                value = score_snapshot.get(key)
                if isinstance(value, (int, float)):
                    payload[key] = float(value)
        qualified_payload_rows.append(payload)
        month_key = _month_key(dt)
        if month_key:
            monthly_groups.setdefault(month_key, []).append(payload)
        regime_key = regime_lookup.get(dt, "unclassified")
        regime_groups.setdefault(regime_key, []).append(payload)
        setup_rows.setdefault(str(row[3] or "unknown"), []).append(row)
    decision_level = {
        "total_decisions": len(decision_rows),
        "qualified_decisions": len(qualified_decision_rows),
        "qualified_directional_hit_rate_5": _safe_ratio(sum(1 for value in qualified_return_5 if value > 0), len(qualified_return_5)),
        "qualified_directional_hit_rate_10": _safe_ratio(sum(1 for value in qualified_return_10 if value > 0), len(qualified_return_10)),
        "qualified_directional_hit_rate_20": _safe_ratio(sum(1 for value in qualified_return_20 if value > 0), len(qualified_return_20)),
        "qualified_directional_hit_rate_30": _safe_ratio(sum(1 for value in qualified_return_30 if value > 0), len(qualified_return_30)),
        "qualified_directional_hit_rate_60": _safe_ratio(sum(1 for value in qualified_return_60 if value > 0), len(qualified_return_60)),
        "average_directional_return_5": _safe_mean(qualified_return_5),
        "average_directional_return_10": _safe_mean(qualified_return_10),
        "average_directional_return_20": _safe_mean(qualified_return_20),
        "average_directional_return_30": _safe_mean(qualified_return_30),
        "average_directional_return_60": _safe_mean(qualified_return_60),
        "by_setup_type": [],
        "monthly": [],
        "rolling_6m": [],
        "by_regime": [],
        "lift_vs_same_date_universe_10": None,
        "lift_vs_same_date_universe_30": None,
        "same_date_universe_average_directional_return_10": None,
        "same_date_universe_average_directional_return_30": None,
        "failure_examples": [],
        "by_break_reason": [],
        "profit_timing_patterns": [],
        "shock_analysis": None,
    }
    decision_level.update(_peak_day_metrics_dict(decision_peak_favorable_days, decision_peak_adverse_days))
    decision_level["profit_timing_patterns"] = _build_profit_timing_patterns(qualified_payload_rows)
    decision_level["qualified_hit_rate_5"] = decision_level["qualified_directional_hit_rate_5"]
    decision_level["qualified_hit_rate_10"] = decision_level["qualified_directional_hit_rate_10"]
    decision_level["qualified_hit_rate_20"] = decision_level["qualified_directional_hit_rate_20"]
    decision_level["qualified_hit_rate_30"] = decision_level["qualified_directional_hit_rate_30"]
    decision_level["qualified_hit_rate_60"] = decision_level["qualified_directional_hit_rate_60"]
    decision_level["average_forward_return_5"] = decision_level["average_directional_return_5"]
    decision_level["average_forward_return_10"] = decision_level["average_directional_return_10"]
    decision_level["average_forward_return_20"] = decision_level["average_directional_return_20"]
    decision_level["average_forward_return_30"] = decision_level["average_directional_return_30"]
    decision_level["average_forward_return_60"] = decision_level["average_directional_return_60"]
    for setup_type, rows in sorted(setup_rows.items(), key=lambda item: (-len(item[1]), item[0])):
        decision_level["by_setup_type"].append(
            {
                "setup_type": setup_type,
                "qualified_decisions": len(rows),
                "directional_hit_rate_10": _safe_ratio(
                    sum(1 for row in rows if row[5] is not None and float(row[5]) > 0),
                    sum(1 for row in rows if row[5] is not None),
                ),
                "directional_hit_rate_20": _safe_ratio(
                    sum(1 for row in rows if row[6] is not None and float(row[6]) > 0),
                    sum(1 for row in rows if row[6] is not None),
                ),
                "directional_hit_rate_30": _safe_ratio(
                    sum(1 for row in rows if row[7] is not None and float(row[7]) > 0),
                    sum(1 for row in rows if row[7] is not None),
                ),
                "average_directional_return_10": _safe_mean([float(row[5]) for row in rows if row[5] is not None]),
                "average_directional_return_20": _safe_mean([float(row[6]) for row in rows if row[6] is not None]),
                "average_directional_return_30": _safe_mean([float(row[7]) for row in rows if row[7] is not None]),
                **_peak_day_metrics_dict(
                    [int(row[9]) for row in rows if row[9] is not None],
                    [int(row[10]) for row in rows if row[10] is not None],
                ),
            }
        )
        decision_level["by_setup_type"][-1]["hit_rate_10"] = decision_level["by_setup_type"][-1]["directional_hit_rate_10"]
        decision_level["by_setup_type"][-1]["hit_rate_20"] = decision_level["by_setup_type"][-1]["directional_hit_rate_20"]
        decision_level["by_setup_type"][-1]["hit_rate_30"] = decision_level["by_setup_type"][-1]["directional_hit_rate_30"]
        decision_level["by_setup_type"][-1]["average_forward_return_10"] = decision_level["by_setup_type"][-1]["average_directional_return_10"]
        decision_level["by_setup_type"][-1]["average_forward_return_20"] = decision_level["by_setup_type"][-1]["average_directional_return_20"]
        decision_level["by_setup_type"][-1]["average_forward_return_30"] = decision_level["by_setup_type"][-1]["average_directional_return_30"]
    monthly_rows: list[dict[str, Any]] = []
    for month_key, rows in sorted(monthly_groups.items(), key=lambda item: _month_key_sort_key(item[0])):
        signal_returns_10 = [float(row["return_10"]) for row in rows if isinstance(row.get("return_10"), (int, float))]
        signal_returns = [float(row["return_30"]) for row in rows if isinstance(row.get("return_30"), (int, float))]
        baseline_returns_10 = [float(row["baseline_10"]) for row in rows if isinstance(row.get("baseline_10"), (int, float))]
        baseline_returns = [float(row["baseline_30"]) for row in rows if isinstance(row.get("baseline_30"), (int, float))]
        monthly_rows.append(
            {
                "month": month_key,
                "qualified_decisions": len(rows),
                "average_directional_return_10": _safe_mean(signal_returns_10),
                "directional_hit_rate_10": _safe_ratio(sum(1 for value in signal_returns_10 if value > 0), len(signal_returns_10)),
                "average_directional_return_30": _safe_mean(signal_returns),
                "directional_hit_rate_30": _safe_ratio(sum(1 for value in signal_returns if value > 0), len(signal_returns)),
                "same_date_universe_average_directional_return_10": _safe_mean(baseline_returns_10),
                "same_date_universe_average_directional_return_30": _safe_mean(baseline_returns),
                "lift_vs_same_date_universe_10": None
                if _safe_mean(signal_returns_10) is None or _safe_mean(baseline_returns_10) is None
                else float(_safe_mean(signal_returns_10) - _safe_mean(baseline_returns_10)),
                "lift_vs_same_date_universe_30": None
                if _safe_mean(signal_returns) is None or _safe_mean(baseline_returns) is None
                else float(_safe_mean(signal_returns) - _safe_mean(baseline_returns)),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in rows],
                    [item.get("days_to_max_adverse_30") for item in rows],
                ),
            }
        )
    decision_level["monthly"] = monthly_rows
    decision_level["rolling_6m"] = _rolling_average_series(
        monthly_rows,
        key="month",
        value_key="average_directional_return_30",
        window=6,
    )
    score_threshold_rows = _build_score_threshold_rows(qualified_payload_rows)
    decision_level["score_threshold_rows"] = score_threshold_rows
    shock_analysis = None
    shock_to = to_int if to_int is not None else (max(observed_dates) if observed_dates else None)
    if shock_to is not None:
        shock_from = from_int if from_int is not None else _shift_ymd_by_years(shock_to, SHOCK_LOOKBACK_YEARS)
        if shock_from is None:
            shock_from = shock_to
        with _open_conn(db_path, read_only=True) as shock_conn:
            trailing_return_map, trailing_return_threshold = _load_trailing_return_map(
                shock_conn,
                from_ymd=int(shock_from),
                to_ymd=int(shock_to),
                horizon=SHOCK_TRAILING_HORIZON,
            )
        shock_analysis = _build_shock_analysis_rows(
            side=normalized_side,
            from_ymd=int(shock_from),
            to_ymd=int(shock_to),
            qualified_rows=qualified_payload_rows,
            signal_events=enriched_event_items,
            trailing_return_map=trailing_return_map,
            trailing_return_threshold=trailing_return_threshold,
            regime_lookup=regime_lookup,
        )
    decision_level["shock_analysis"] = shock_analysis
    regime_rows = []
    for regime_key, rows in sorted(regime_groups.items(), key=lambda item: (-len(item[1]), item[0])):
        signal_returns_10 = [float(row["return_10"]) for row in rows if isinstance(row.get("return_10"), (int, float))]
        signal_returns = [float(row["return_30"]) for row in rows if isinstance(row.get("return_30"), (int, float))]
        baseline_returns_10 = [float(row["baseline_10"]) for row in rows if isinstance(row.get("baseline_10"), (int, float))]
        baseline_returns = [float(row["baseline_30"]) for row in rows if isinstance(row.get("baseline_30"), (int, float))]
        signal_mean_10 = _safe_mean(signal_returns_10)
        signal_mean = _safe_mean(signal_returns)
        baseline_mean_10 = _safe_mean(baseline_returns_10)
        baseline_mean = _safe_mean(baseline_returns)
        regime_rows.append(
            {
                "regime": regime_key,
                "qualified_decisions": len(rows),
                "average_directional_return_10": signal_mean_10,
                "directional_hit_rate_10": _safe_ratio(sum(1 for value in signal_returns_10 if value > 0), len(signal_returns_10)),
                "average_directional_return_30": signal_mean,
                "directional_hit_rate_30": _safe_ratio(sum(1 for value in signal_returns if value > 0), len(signal_returns)),
                "same_date_universe_average_directional_return_10": baseline_mean_10,
                "same_date_universe_average_directional_return_30": baseline_mean,
                "lift_vs_same_date_universe_10": None if signal_mean_10 is None or baseline_mean_10 is None else float(signal_mean_10 - baseline_mean_10),
                "lift_vs_same_date_universe_30": None if signal_mean is None or baseline_mean is None else float(signal_mean - baseline_mean),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in rows],
                    [item.get("days_to_max_adverse_30") for item in rows],
                ),
            }
        )
    decision_level["by_regime"] = regime_rows
    all_baselines_10 = [float(value) for value in baseline_lookup_10.values()]
    all_baselines = [float(value) for value in baseline_lookup.values()]
    signal_mean_10 = decision_level["average_directional_return_10"]
    signal_mean_30 = decision_level["average_directional_return_30"]
    baseline_mean_10 = _safe_mean(all_baselines_10)
    baseline_mean_30 = _safe_mean(all_baselines)
    decision_level["same_date_universe_average_directional_return_10"] = baseline_mean_10
    decision_level["lift_vs_same_date_universe_10"] = (
        None if signal_mean_10 is None or baseline_mean_10 is None else float(signal_mean_10 - baseline_mean_10)
    )
    decision_level["same_date_universe_average_directional_return_30"] = baseline_mean_30
    decision_level["lift_vs_same_date_universe_30"] = (
        None if signal_mean_30 is None or baseline_mean_30 is None else float(signal_mean_30 - baseline_mean_30)
    )
    status_counts = {"active": 0, "completed": 0, "archive": 0}
    evaluated_count = 0
    evaluated_wins = 0
    duplicate_campaigns = 0
    final_returns: list[float] = []
    max_favorable_values: list[float] = []
    max_adverse_values: list[float] = []
    active_snapshot_values: list[float] = []
    campaign_peak_favorable_days: list[int] = []
    campaign_peak_adverse_days: list[int] = []
    by_break_reason: dict[str, dict[str, Any]] = {}
    by_signal_count = {
        "single": {"campaign_count": 0, "evaluated_count": 0, "wins": 0, "final_returns": [], "peak_favorable_days": [], "peak_adverse_days": []},
        "repeated": {"campaign_count": 0, "evaluated_count": 0, "wins": 0, "final_returns": [], "peak_favorable_days": [], "peak_adverse_days": []},
    }
    by_setup_type_campaign: dict[str, dict[str, Any]] = {}
    for row in campaign_rows:
        code = str(row[0])
        status = str(row[1])
        favorable_return = float(row[2]) if row[2] is not None else None
        max_favorable = float(row[3]) if row[3] is not None else None
        max_adverse = float(row[4]) if row[4] is not None else None
        final_return = float(row[5]) if row[5] is not None else None
        signal_count = int(row[6]) if row[6] is not None else 0
        reason_snapshot = _json_load(str(row[7]) if row[7] is not None else None) or {}
        first_signal_date = int(row[8]) if row[8] is not None else None
        days_to_max_favorable_30 = int(row[9]) if row[9] is not None else None
        days_to_max_adverse_30 = int(row[10]) if row[10] is not None else None
        setup_type = str(reason_snapshot.get("setupType") or "unknown").strip() or "unknown"
        status_counts[status] = status_counts.get(status, 0) + 1
        if signal_count > 1:
            duplicate_campaigns += 1
        if status == "active" and favorable_return is not None:
            active_snapshot_values.append(favorable_return)
        signal_bucket = by_signal_count["repeated" if signal_count > 1 else "single"]
        signal_bucket["campaign_count"] += 1
        if days_to_max_favorable_30 is not None:
            campaign_peak_favorable_days.append(days_to_max_favorable_30)
            signal_bucket["peak_favorable_days"].append(days_to_max_favorable_30)
        if days_to_max_adverse_30 is not None:
            campaign_peak_adverse_days.append(days_to_max_adverse_30)
            signal_bucket["peak_adverse_days"].append(days_to_max_adverse_30)
        setup_bucket = by_setup_type_campaign.setdefault(
            setup_type,
            {"campaign_count": 0, "evaluated_count": 0, "wins": 0, "final_returns": [], "peak_favorable_days": [], "peak_adverse_days": []},
        )
        setup_bucket["campaign_count"] += 1
        if days_to_max_favorable_30 is not None:
            setup_bucket["peak_favorable_days"].append(days_to_max_favorable_30)
        if days_to_max_adverse_30 is not None:
            setup_bucket["peak_adverse_days"].append(days_to_max_adverse_30)
        if final_return is None:
            continue
        evaluated_count += 1
        final_returns.append(final_return)
        if max_favorable is not None:
            max_favorable_values.append(max_favorable)
        if max_adverse is not None:
            max_adverse_values.append(max_adverse)
        signal_bucket["evaluated_count"] += 1
        signal_bucket["final_returns"].append(final_return)
        setup_bucket["evaluated_count"] += 1
        setup_bucket["final_returns"].append(final_return)
        if final_return > 0:
            evaluated_wins += 1
            signal_bucket["wins"] += 1
            setup_bucket["wins"] += 1
        matching_event = next(
            (
                item
                for item in event_items
                if item.get("signal_date") == first_signal_date
                and str(item.get("code") or "") == code
            ),
            None,
        )
        break_reason = str((matching_event or {}).get("break_reason") or "completed_clean")
        break_bucket = by_break_reason.setdefault(
            break_reason,
            {"count": 0, "evaluated_count": 0, "wins": 0, "final_returns": []},
        )
        break_bucket["count"] += 1
        break_bucket["evaluated_count"] += 1
        break_bucket["final_returns"].append(final_return)
        if final_return > 0:
            break_bucket["wins"] += 1
    failure_events = [
        item
        for item in event_items
        if (isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) <= 0)
        or str(item.get("break_status") or "") == "broken"
    ]
    failure_events.sort(
        key=lambda item: (
            float(item["return_30d"]) if isinstance(item.get("return_30d"), (int, float)) else float("inf"),
            item.get("signal_date") or 0,
            str(item.get("code") or ""),
        )
    )
    decision_level["failure_examples"] = [
        {
            "event_id": item.get("event_id"),
            "code": item.get("code"),
            "name": item.get("name"),
            "signal_date": item.get("signalDate"),
            "setup_type": item.get("setup_type"),
            "return_30d": item.get("return_30d"),
            "max_adverse_30": item.get("max_adverse_30"),
            "max_favorable_30": item.get("max_favorable_30"),
            "break_status": item.get("break_status"),
            "break_reason": item.get("break_reason"),
            "reason_summary": item.get("reason_summary"),
        }
        for item in failure_events[:10]
    ]
    decision_level["by_break_reason"] = [
        {
            "break_reason": key,
            "count": int(payload["count"]),
            "evaluated_count": int(payload["evaluated_count"]),
            "directional_win_rate": _safe_ratio(int(payload["wins"]), int(payload["evaluated_count"])),
            "average_directional_return_30": _safe_mean([float(value) for value in payload["final_returns"]]),
        }
        for key, payload in sorted(by_break_reason.items(), key=lambda item: (-int(item[1]["count"]), item[0]))
    ]
    campaign_level = {
        "total_campaigns": len(campaign_rows),
        "active_count": status_counts["active"],
        "completed_count": status_counts["completed"],
        "archive_count": status_counts["archive"],
        "evaluated_count": evaluated_count,
        "evaluated_directional_win_rate": _safe_ratio(evaluated_wins, evaluated_count),
        "average_final_directional_return": _safe_mean(final_returns),
        "average_max_favorable_return": _safe_mean(max_favorable_values),
        "average_max_adverse_return": _safe_mean(max_adverse_values),
        "active_average_directional_return": _safe_mean(active_snapshot_values),
        "duplicate_signal_rate": _safe_ratio(duplicate_campaigns, len(campaign_rows)),
        "by_signal_count": [
            {
                "bucket": bucket,
                "campaign_count": int(payload["campaign_count"]),
                "evaluated_count": int(payload["evaluated_count"]),
                "directional_win_rate": _safe_ratio(int(payload["wins"]), int(payload["evaluated_count"])),
                "average_final_directional_return": _safe_mean(list(payload["final_returns"])),
                **_peak_day_metrics_dict(
                    list(payload["peak_favorable_days"]),
                    list(payload["peak_adverse_days"]),
                ),
            }
            for bucket, payload in by_signal_count.items()
        ],
        "by_setup_type": [
            {
                "setup_type": setup_type,
                "campaign_count": int(payload["campaign_count"]),
                "evaluated_count": int(payload["evaluated_count"]),
                "directional_win_rate": _safe_ratio(int(payload["wins"]), int(payload["evaluated_count"])),
                "average_final_directional_return": _safe_mean(list(payload["final_returns"])),
                **_peak_day_metrics_dict(
                    list(payload["peak_favorable_days"]),
                    list(payload["peak_adverse_days"]),
                ),
            }
            for setup_type, payload in sorted(by_setup_type_campaign.items(), key=lambda item: (-int(item[1]["campaign_count"]), item[0]))
        ],
        "by_break_reason": decision_level["by_break_reason"],
    }
    campaign_level.update(_peak_day_metrics_dict(campaign_peak_favorable_days, campaign_peak_adverse_days))
    campaign_level["evaluated_win_rate"] = campaign_level["evaluated_directional_win_rate"]
    campaign_level["average_final_return"] = campaign_level["average_final_directional_return"]
    for item in campaign_level["by_signal_count"]:
        item["win_rate"] = item["directional_win_rate"]
        item["average_final_return"] = item["average_final_directional_return"]
    for item in campaign_level["by_setup_type"]:
        item["win_rate"] = item["directional_win_rate"]
        item["average_final_return"] = item["average_final_directional_return"]
    ranking_filtered = []
    ranking_peak_favorable_days: list[int] = []
    ranking_peak_adverse_days: list[int] = []
    for dt, return_30d, signal_state, entry_qualified, days_to_max_favorable_30, days_to_max_adverse_30 in ranking_rows:
        dt_int = _coerce_ymd(dt)
        if from_int is not None and dt_int is not None and dt_int < from_int:
            continue
        if to_int is not None and dt_int is not None and dt_int > to_int:
            continue
        if days_to_max_favorable_30 is not None:
            ranking_peak_favorable_days.append(int(days_to_max_favorable_30))
        if days_to_max_adverse_30 is not None:
            ranking_peak_adverse_days.append(int(days_to_max_adverse_30))
        ranking_filtered.append(
            {
                "dt": dt_int,
                "return_30d": float(return_30d) if return_30d is not None else None,
                "signal_state": str(signal_state or "wait"),
                "entry_qualified": bool(entry_qualified),
                "baseline_30": baseline_lookup.get(dt_int) if dt_int is not None else None,
                "days_to_max_favorable_30": int(days_to_max_favorable_30) if days_to_max_favorable_30 is not None else None,
                "days_to_max_adverse_30": int(days_to_max_adverse_30) if days_to_max_adverse_30 is not None else None,
            }
        )
    ranking_returns = [float(item["return_30d"]) for item in ranking_filtered if isinstance(item.get("return_30d"), (int, float))]
    ranking_baselines = [float(item["baseline_30"]) for item in ranking_filtered if isinstance(item.get("baseline_30"), (int, float))]
    ranking_level = {
        "count": len(ranking_filtered),
        "average_directional_return_30": _safe_mean(ranking_returns),
        "directional_win_rate_30": _safe_ratio(sum(1 for value in ranking_returns if value > 0), len(ranking_returns)),
        "same_date_universe_average_directional_return_30": _safe_mean(ranking_baselines),
        "lift_vs_same_date_universe_30": None
        if _safe_mean(ranking_returns) is None or _safe_mean(ranking_baselines) is None
        else float(_safe_mean(ranking_returns) - _safe_mean(ranking_baselines)),
    }
    ranking_level.update(_peak_day_metrics_dict(ranking_peak_favorable_days, ranking_peak_adverse_days))
    ranking_level["win_rate_30d"] = ranking_level["directional_win_rate_30"]
    ranking_level["average_return_30d"] = ranking_level["average_directional_return_30"]
    leakage_audit = get_signal_tracking_leakage_audit(
        side=normalized_side,
        logic_version=resolved_logic_version,
        from_ymd=from_int,
        to_ymd=to_int,
        db_path=db_path,
    )
    sell_subset_comparison = (
        _build_sell_subset_comparison(
            enriched_event_items,
            primary_horizon=PRIMARY_HORIZON_BY_SIDE["sell"],
        )
        if normalized_side == "sell"
        else None
    )
    return {
        "generated_at": _serialize_timestamp(datetime.now(timezone.utc)),
        "side": normalized_side,
        "logic_version": resolved_logic_version,
        "basis_version": resolved_basis_version,
        "primary_horizon": PRIMARY_HORIZON_BY_SIDE.get(normalized_side, 30),
        "decision_level": decision_level,
        "campaign_level": campaign_level,
        "ranking_level": ranking_level,
        "sell_subset_comparison": sell_subset_comparison,
        "audit": leakage_audit,
        "summary": {
            "total_campaigns": campaign_level["total_campaigns"],
            "active_count": campaign_level["active_count"],
            "completed_count": campaign_level["completed_count"],
            "archive_count": campaign_level["archive_count"],
            "evaluated_count": campaign_level["evaluated_count"],
            "evaluated_directional_win_rate": campaign_level["evaluated_directional_win_rate"],
            "average_final_directional_return": campaign_level["average_final_directional_return"],
            "average_max_favorable_return": campaign_level["average_max_favorable_return"],
            "average_max_adverse_return": campaign_level["average_max_adverse_return"],
            "active_average_directional_return": campaign_level["active_average_directional_return"],
            "duplicate_signal_rate": campaign_level["duplicate_signal_rate"],
        },
    }


def get_signal_tracking_analysis(
    *,
    logic_version: str | None = None,
    side: str = "buy",
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    payload = get_signal_tracking_validation(
        logic_version=logic_version,
        side=side,
        from_ymd=from_ymd,
        to_ymd=to_ymd,
        as_of=as_of,
        db_path=db_path,
    )
    decision_level = dict(payload.get("decision_level") or {})
    campaign_level = dict(payload.get("campaign_level") or {})
    ranking_level = dict(payload.get("ranking_level") or {})
    normalized_side = _normalize_side(payload.get("side"))
    primary_horizon = int(payload.get("primary_horizon") or PRIMARY_HORIZON_BY_SIDE.get(normalized_side, 30))
    return {
        "generated_at": payload.get("generated_at"),
        "side": payload.get("side"),
        "logic_version": payload.get("logic_version"),
        "basis_version": payload.get("basis_version"),
        "primary_horizon": primary_horizon,
        "summary": {
            "qualified_decisions": decision_level.get("qualified_decisions"),
            "primary_directional_hit_rate": decision_level.get(f"qualified_directional_hit_rate_{primary_horizon}"),
            "primary_average_directional_return": decision_level.get(f"average_directional_return_{primary_horizon}"),
            "primary_same_date_universe_average_directional_return": decision_level.get(
                f"same_date_universe_average_directional_return_{primary_horizon}"
            ),
            "primary_lift_vs_same_date_universe": decision_level.get(f"lift_vs_same_date_universe_{primary_horizon}"),
            "directional_hit_rate_30": decision_level.get("qualified_directional_hit_rate_30"),
            "average_directional_return_30": decision_level.get("average_directional_return_30"),
            "same_date_universe_average_directional_return_30": decision_level.get("same_date_universe_average_directional_return_30"),
            "lift_vs_same_date_universe_30": decision_level.get("lift_vs_same_date_universe_30"),
            "median_days_to_max_favorable_30": decision_level.get("median_days_to_max_favorable_30"),
            "median_days_to_max_adverse_30": decision_level.get("median_days_to_max_adverse_30"),
            "campaign_directional_win_rate": campaign_level.get("evaluated_directional_win_rate"),
            "campaign_duplicate_signal_rate": campaign_level.get("duplicate_signal_rate"),
            "ranking_directional_win_rate_30": ranking_level.get("directional_win_rate_30"),
            "ranking_lift_vs_same_date_universe_30": ranking_level.get("lift_vs_same_date_universe_30"),
        },
        "rolling_6m": decision_level.get("rolling_6m") or [],
        "monthly": decision_level.get("monthly") or [],
        "by_regime": decision_level.get("by_regime") or [],
        "by_setup_type": decision_level.get("by_setup_type") or [],
        "by_break_reason": decision_level.get("by_break_reason") or [],
        "by_signal_count": campaign_level.get("by_signal_count") or [],
        "peak_day_buckets": decision_level.get("peak_day_buckets") or [],
        "profit_timing_patterns": decision_level.get("profit_timing_patterns") or [],
        "failure_examples": decision_level.get("failure_examples") or [],
        "sell_subset_comparison": payload.get("sell_subset_comparison"),
        "ranking_level": ranking_level,
        "audit": payload.get("audit") or {},
    }


def _metric_comparison(base: Any, target: Any) -> dict[str, Any]:
    delta = None
    if isinstance(base, (int, float)) and isinstance(target, (int, float)):
        delta = float(target - base)
    return {"base": base, "target": target, "delta": delta}


def get_signal_tracking_comparison(
    *,
    side: str = "sell",
    base_logic_version: str = DEFAULT_LOGIC_VERSION,
    target_logic_version: str = SELL_TIGHTENED_LOGIC_VERSION,
    primary_horizon: int | None = None,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side)
    horizon = int(primary_horizon or PRIMARY_HORIZON_BY_SIDE.get(normalized_side, 30))
    base_validation = get_signal_tracking_validation(
        side=normalized_side,
        logic_version=base_logic_version,
        from_ymd=from_ymd,
        to_ymd=to_ymd,
        as_of=as_of,
        db_path=db_path,
    )
    target_validation = get_signal_tracking_validation(
        side=normalized_side,
        logic_version=target_logic_version,
        from_ymd=from_ymd,
        to_ymd=to_ymd,
        as_of=as_of,
        db_path=db_path,
    )
    base_decision = dict(base_validation.get("decision_level") or {})
    target_decision = dict(target_validation.get("decision_level") or {})
    base_campaign = dict(base_validation.get("campaign_level") or {})
    target_campaign = dict(target_validation.get("campaign_level") or {})
    decision_hit_key = f"qualified_directional_hit_rate_{horizon}"
    decision_return_key = f"average_directional_return_{horizon}"
    decision_lift_key = f"lift_vs_same_date_universe_{horizon}"
    return {
        "generated_at": target_validation.get("generated_at") or base_validation.get("generated_at"),
        "side": normalized_side,
        "primary_horizon": horizon,
        "base_logic_version": base_validation.get("logic_version"),
        "target_logic_version": target_validation.get("logic_version"),
        "decision": {
            "qualified_decisions": _metric_comparison(
                base_decision.get("qualified_decisions"),
                target_decision.get("qualified_decisions"),
            ),
            "directional_hit_rate": _metric_comparison(
                base_decision.get(decision_hit_key),
                target_decision.get(decision_hit_key),
            ),
            "average_directional_return": _metric_comparison(
                base_decision.get(decision_return_key),
                target_decision.get(decision_return_key),
            ),
            "lift_vs_same_date_universe": _metric_comparison(
                base_decision.get(decision_lift_key),
                target_decision.get(decision_lift_key),
            ),
            "median_days_to_max_favorable_30": _metric_comparison(
                base_decision.get("median_days_to_max_favorable_30"),
                target_decision.get("median_days_to_max_favorable_30"),
            ),
            "median_days_to_max_adverse_30": _metric_comparison(
                base_decision.get("median_days_to_max_adverse_30"),
                target_decision.get("median_days_to_max_adverse_30"),
            ),
        },
        "campaign": {
            "total_campaigns": _metric_comparison(
                base_campaign.get("total_campaigns"),
                target_campaign.get("total_campaigns"),
            ),
            "evaluated_directional_win_rate": _metric_comparison(
                base_campaign.get("evaluated_directional_win_rate"),
                target_campaign.get("evaluated_directional_win_rate"),
            ),
            "average_final_directional_return": _metric_comparison(
                base_campaign.get("average_final_directional_return"),
                target_campaign.get("average_final_directional_return"),
            ),
            "duplicate_signal_rate": _metric_comparison(
                base_campaign.get("duplicate_signal_rate"),
                target_campaign.get("duplicate_signal_rate"),
            ),
        },
        "by_setup_type": {
            "base": base_decision.get("by_setup_type") or [],
            "target": target_decision.get("by_setup_type") or [],
        },
        "by_regime": {
            "base": base_decision.get("by_regime") or [],
            "target": target_decision.get("by_regime") or [],
        },
    }


def _load_code_decision_rows(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    logic_version: str,
    from_ymd: int,
    to_ymd: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT dt, side, entry_qualified, score_snapshot_json
        FROM signal_decision_daily
        WHERE code = ? AND logic_version = ? AND dt >= ? AND dt <= ?
        ORDER BY dt, side
        """,
        [str(code), str(logic_version), int(from_ymd), int(to_ymd)],
    ).fetchall()
    return [
        {
            "dt": int(row[0]),
            "side": str(row[1]),
            "entry_qualified": bool(row[2]),
            "score_snapshot": _json_load(str(row[3]) if row[3] is not None else None) or {},
        }
        for row in rows
    ]


def _collect_directional_stats(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "directional_hit_rate": _safe_ratio(sum(1 for value in values if value > 0), len(values)),
        "average_directional_return": _safe_mean(values),
        "median_directional_return": (float(pd.Series(values).median()) if values else None),
    }


def _maybe_ensure_market_regime_rows(
    *,
    from_ymd: int | None,
    to_ymd: int | None,
    db_path: str | None,
) -> None:
    if db_path is not None:
        return
    try:
        from app.backend.services.analysis import strategy_backtest_service

        strategy_backtest_service.build_market_regime_daily(
            start_dt=from_ymd,
            end_dt=to_ymd,
            label_version=DEFAULT_REGIME_LABEL_VERSION,
        )
    except Exception:
        return


def _load_market_regime_map(
    conn: duckdb.DuckDBPyConnection,
    *,
    from_ymd: int | None,
    to_ymd: int | None,
) -> dict[int, dict[str, Any]]:
    if not _table_exists(conn, "market_regime_daily"):
        return {}
    where_parts = ["label_version = ?"]
    params: list[Any] = [DEFAULT_REGIME_LABEL_VERSION]
    if from_ymd is not None:
        where_parts.append("dt >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        where_parts.append("dt <= ?")
        params.append(int(to_ymd))
    try:
        rows = conn.execute(
            f"""
            SELECT dt, regime_id, regime_score
            FROM market_regime_daily
            WHERE {" AND ".join(where_parts)}
            ORDER BY dt ASC
            """,
            params,
        ).fetchall()
    except Exception:
        return {}
    return {
        int(row[0]): {
            "regime_tag": str(row[1] or "unknown"),
            "regime_score": float(row[2]) if row[2] is not None else None,
        }
        for row in rows
        if row and row[0] is not None
    }


def _build_signal_event_items_for_analysis(
    conn: duckdb.DuckDBPyConnection,
    *,
    side: str,
    logic_version: str,
    from_ymd: int | None,
    to_ymd: int | None,
) -> list[dict[str, Any]]:
    where_parts = ["so.side = ?", "so.logic_version = ?"]
    params: list[Any] = [str(side), str(logic_version)]
    if from_ymd is not None:
        where_parts.append("so.signal_date >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        where_parts.append("so.signal_date <= ?")
        params.append(int(to_ymd))
    latest_market_ymd = _latest_market_ymd(conn)
    rows = conn.execute(
        f"""
        SELECT
            so.occurrence_id,
            so.campaign_id,
            so.code,
            so.side,
            so.signal_date,
            so.basis_version,
            so.logic_version,
            COALESCE(sd.name, sm.name) AS name,
            sd.setup_type,
            so.reason_snapshot_json,
            so.score_snapshot_json,
            so.entry_close_price,
            so.entry_next_open_price
        FROM signal_occurrence AS so
        LEFT JOIN signal_decision_daily AS sd
          ON sd.dt = so.signal_date
         AND sd.code = so.code
         AND sd.side = so.side
         AND sd.logic_version = so.logic_version
        LEFT JOIN stock_meta AS sm
          ON sm.code = so.code
        WHERE {" AND ".join(where_parts)}
        ORDER BY so.signal_date DESC, so.code ASC
        """,
        params,
    ).fetchall()
    bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
    decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
    items: list[dict[str, Any]] = []
    for row in rows:
        item = _build_signal_event_item(
            conn,
            occurrence_row=row,
            latest_market_ymd=latest_market_ymd,
            bars_cache=bars_cache,
            decision_rows_cache=decision_rows_cache,
        )
        if item is not None:
            items.append(item)
    return items


def _build_decision_baseline_map(
    conn: duckdb.DuckDBPyConnection,
    *,
    side: str,
    logic_version: str,
    from_ymd: int | None,
    to_ymd: int | None,
) -> dict[int, float]:
    where_parts = ["side = ?", "logic_version = ?", "forward_return_30 IS NOT NULL"]
    params: list[Any] = [str(side), str(logic_version)]
    if from_ymd is not None:
        where_parts.append("dt >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        where_parts.append("dt <= ?")
        params.append(int(to_ymd))
    rows = conn.execute(
        f"""
        SELECT dt, AVG(forward_return_30) AS baseline_return_30
        FROM signal_decision_daily
        WHERE {" AND ".join(where_parts)}
        GROUP BY dt
        ORDER BY dt ASC
        """,
        params,
    ).fetchall()
    return {
        int(row[0]): float(row[1])
        for row in rows
        if row and row[0] is not None and row[1] is not None
    }


def _build_group_metric_rows(
    rows: dict[str, list[dict[str, Any]]],
    *,
    label_key: str,
    return_key: str,
    lift_key: str | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for label, payloads in sorted(rows.items(), key=lambda item: (-len(item[1]), item[0])):
        returns = [float(item[return_key]) for item in payloads if isinstance(item.get(return_key), (int, float))]
        lifts = [float(item[lift_key]) for item in payloads if lift_key and isinstance(item.get(lift_key), (int, float))]
        row = {
            label_key: label,
            "count": len(payloads),
            "directional_hit_rate_30": _safe_ratio(sum(1 for value in returns if value > 0), len(returns)),
            "average_directional_return_30": _safe_mean(returns),
        }
        if lift_key is not None:
            row["average_lift_vs_universe_30"] = _safe_mean(lifts)
        items.append(row)
    return items


def _load_trailing_return_map(
    conn: duckdb.DuckDBPyConnection,
    *,
    from_ymd: int,
    to_ymd: int,
    horizon: int = SHOCK_TRAILING_HORIZON,
) -> tuple[dict[tuple[int, str], float], float | None]:
    query_from = _shift_ymd_by_days(from_ymd, -120) or int(from_ymd)
    rows = conn.execute(
        f"""
        WITH normalized AS (
            SELECT
                code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS dt,
                c AS close
            FROM daily_bars
        ),
        ordered AS (
            SELECT
                code,
                dt,
                close,
                LAG(close, {int(horizon)}) OVER (PARTITION BY code ORDER BY dt) AS close_h
            FROM normalized
            WHERE dt IS NOT NULL
              AND dt >= ?
              AND dt <= ?
        )
        SELECT
            dt,
            code,
            CASE
                WHEN close IS NULL OR close_h IS NULL OR close <= 0 OR close_h <= 0 THEN NULL
                ELSE (close / close_h) - 1.0
            END AS trailing_return
        FROM ordered
        WHERE close_h IS NOT NULL
        ORDER BY dt, code
        """,
        [int(query_from), int(to_ymd)],
    ).fetchall()
    trailing_return_map: dict[tuple[int, str], float] = {}
    window_returns: list[float] = []
    for row in rows:
        dt = int(row[0])
        code = str(row[1])
        trailing_return = float(row[2]) if row[2] is not None else None
        if trailing_return is None:
            continue
        trailing_return_map[(dt, code)] = trailing_return
        if dt >= int(from_ymd):
            window_returns.append(trailing_return)
    threshold = float(pd.Series(window_returns).quantile(SHOCK_BOTTOM_DECILE)) if window_returns else None
    return trailing_return_map, threshold


def _build_shock_analysis_rows(
    *,
    side: str,
    from_ymd: int,
    to_ymd: int,
    qualified_rows: list[dict[str, Any]],
    signal_events: list[dict[str, Any]],
    trailing_return_map: dict[tuple[int, str], float],
    trailing_return_threshold: float | None,
    regime_lookup: dict[int, str],
) -> dict[str, Any]:
    event_by_key = {
        (int(item["signal_date"]), str(item["code"])): item
        for item in signal_events
        if item.get("signal_date") is not None and item.get("code") is not None
    }
    cohort_groups: dict[str, list[dict[str, Any]]] = {
        "insufficient_history": [],
        "normal": [],
        "bottom_decile_only": [],
        "drop_10pct_only": [],
        "both": [],
    }
    shock_rows: list[dict[str, Any]] = []
    for row in qualified_rows:
        dt = int(row["dt"])
        code = str(row["code"])
        trailing_return_20 = trailing_return_map.get((dt, code))
        baseline_30 = row.get("baseline_30")
        return_30 = row.get("return_30")
        setup_type = str(row.get("setup_type") or "unknown").strip() or "unknown"
        regime_tag = str(regime_lookup.get(dt, "unclassified") or "unclassified")
        event = event_by_key.get((dt, code))
        payload = {
            "dt": dt,
            "code": code,
            "setup_type": setup_type,
            "regime_tag": regime_tag,
            "return_30": return_30,
            "baseline_30": baseline_30,
            "lift_vs_universe_30": (
                None
                if not isinstance(return_30, (int, float)) or not isinstance(baseline_30, (int, float))
                else float(return_30) - float(baseline_30)
            ),
            "days_to_max_favorable_30": row.get("days_to_max_favorable_30"),
            "days_to_max_adverse_30": row.get("days_to_max_adverse_30"),
            "trailing_return_20": trailing_return_20,
            "break_status": str((event or {}).get("break_status") or "") or None,
            "break_reason": str((event or {}).get("break_reason") or "") or None,
            "name": (event or {}).get("name"),
        }
        if trailing_return_20 is None:
            cohort_groups["insufficient_history"].append(payload)
            continue
        shock_drop = trailing_return_20 <= SHOCK_DRAWDOWN_THRESHOLD
        shock_bottom = trailing_return_threshold is not None and trailing_return_20 <= trailing_return_threshold
        if shock_drop and shock_bottom:
            cohort_groups["both"].append(payload)
            shock_rows.append(payload)
        elif shock_drop:
            cohort_groups["drop_10pct_only"].append(payload)
            shock_rows.append(payload)
        elif shock_bottom:
            cohort_groups["bottom_decile_only"].append(payload)
            shock_rows.append(payload)
        else:
            cohort_groups["normal"].append(payload)
    shock_setup_groups: dict[str, list[dict[str, Any]]] = {}
    shock_regime_groups: dict[str, list[dict[str, Any]]] = {}
    shock_break_reason_groups: dict[str, list[dict[str, Any]]] = {}
    for payload in shock_rows:
        shock_setup_groups.setdefault(str(payload.get("setup_type") or "unknown"), []).append(payload)
        shock_regime_groups.setdefault(str(payload.get("regime_tag") or "unclassified"), []).append(payload)
        if str(payload.get("break_status") or "") == "broken":
            shock_break_reason_groups.setdefault(str(payload.get("break_reason") or "unknown"), []).append(payload)

    def _build_cohort_row(cohort_key: str, label: str, items: list[dict[str, Any]]) -> dict[str, Any]:
        returns_30 = [float(item["return_30"]) for item in items if isinstance(item.get("return_30"), (int, float))]
        baselines_30 = [float(item["baseline_30"]) for item in items if isinstance(item.get("baseline_30"), (int, float))]
        trailing_returns_20 = [float(item["trailing_return_20"]) for item in items if isinstance(item.get("trailing_return_20"), (int, float))]
        avg_return_30 = _safe_mean(returns_30)
        avg_baseline_30 = _safe_mean(baselines_30)
        return {
            "cohort_key": cohort_key,
            "label": label,
            "count": len(items),
            "share": _safe_ratio(len(items), len(qualified_rows)),
            "directional_hit_rate_30": _safe_ratio(sum(1 for value in returns_30 if value > 0), len(returns_30)),
            "average_directional_return_30": avg_return_30,
            "same_date_universe_average_directional_return_30": avg_baseline_30,
            "lift_vs_same_date_universe_30": None if avg_return_30 is None or avg_baseline_30 is None else float(avg_return_30 - avg_baseline_30),
            "average_trailing_return_20": _safe_mean(trailing_returns_20),
            "median_trailing_return_20": _safe_median(trailing_returns_20),
        }

    cohort_rows = [
        _build_cohort_row("insufficient_history", "insufficient history", cohort_groups["insufficient_history"]),
        _build_cohort_row("normal", "normal", cohort_groups["normal"]),
        _build_cohort_row("bottom_decile_only", "bottom decile only", cohort_groups["bottom_decile_only"]),
        _build_cohort_row("drop_10pct_only", f"20d <= {SHOCK_DRAWDOWN_THRESHOLD:.0%} only", cohort_groups["drop_10pct_only"]),
        _build_cohort_row("both", f"{SHOCK_DRAWDOWN_THRESHOLD:.0%} && bottom decile", cohort_groups["both"]),
    ]
    shock_examples = sorted(
        shock_rows,
        key=lambda item: (
            float(item["trailing_return_20"]) if isinstance(item.get("trailing_return_20"), (int, float)) else float("inf"),
            float(item["return_30"]) if isinstance(item.get("return_30"), (int, float)) else float("inf"),
            int(item["dt"]),
            str(item["code"]),
        ),
    )[:10]
    return {
        "side": side,
        "window": {
            "from_ymd": int(from_ymd),
            "to_ymd": int(to_ymd),
            "lookback_years": SHOCK_LOOKBACK_YEARS,
            "trailing_horizon": SHOCK_TRAILING_HORIZON,
            "drop_threshold": SHOCK_DRAWDOWN_THRESHOLD,
            "bottom_decile_threshold": trailing_return_threshold,
        },
        "qualified_decisions": len(qualified_rows),
        "qualified_with_trailing_return": len([row for row in qualified_rows if trailing_return_map.get((int(row["dt"]), str(row["code"]))) is not None]),
        "cohort_rows": cohort_rows,
        "by_setup_type": _build_group_metric_rows(shock_setup_groups, label_key="setup_type", return_key="return_30", lift_key="lift_vs_universe_30"),
        "by_regime": _build_group_metric_rows(shock_regime_groups, label_key="regime_tag", return_key="return_30", lift_key="lift_vs_universe_30"),
        "by_break_reason": _build_group_metric_rows(shock_break_reason_groups, label_key="break_reason", return_key="return_30", lift_key="lift_vs_universe_30"),
        "shock_examples": [
            {
                "dt": item["dt"],
                "code": item["code"],
                "name": item.get("name"),
                "setup_type": item.get("setup_type"),
                "regime_tag": item.get("regime_tag"),
                "trailing_return_20": item.get("trailing_return_20"),
                "return_30": item.get("return_30"),
                "lift_vs_universe_30": item.get("lift_vs_universe_30"),
                "break_status": item.get("break_status"),
                "break_reason": item.get("break_reason"),
            }
            for item in shock_examples
        ],
    }


def _signal_failure_examples(
    items: list[dict[str, Any]],
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    failures = [
        item
        for item in items
        if str(item.get("break_status") or "") == "broken"
        or (isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) <= 0)
    ]
    failures.sort(
        key=lambda item: (
            0 if str(item.get("break_status") or "") == "broken" else 1,
            float(item.get("return_30d")) if isinstance(item.get("return_30d"), (int, float)) else 999.0,
            str(item.get("signalDate") or ""),
            str(item.get("code") or ""),
        )
    )
    return [
        {
            "event_id": str(item.get("event_id") or ""),
            "code": str(item.get("code") or ""),
            "name": item.get("name"),
            "signal_date": item.get("signal_date"),
            "signalDate": item.get("signalDate"),
            "setup_type": item.get("setup_type"),
            "return_30d": item.get("return_30d"),
            "max_adverse_30": item.get("max_adverse_30"),
            "max_favorable_30": item.get("max_favorable_30"),
            "break_status": item.get("break_status"),
            "break_reason": item.get("break_reason"),
        }
        for item in failures[: max(1, int(limit))]
    ]


def _build_signal_analysis_payload(
    conn: duckdb.DuckDBPyConnection,
    *,
    side: str,
    logic_version: str,
    from_ymd: int | None,
    to_ymd: int | None,
) -> dict[str, Any]:
    decision_where = ["side = ?", "logic_version = ?"]
    decision_params: list[Any] = [str(side), str(logic_version)]
    if from_ymd is not None:
        decision_where.append("dt >= ?")
        decision_params.append(int(from_ymd))
    if to_ymd is not None:
        decision_where.append("dt <= ?")
        decision_params.append(int(to_ymd))
    decision_rows = conn.execute(
        f"""
        SELECT
            dt,
            code,
            entry_qualified,
            setup_type,
            forward_return_5,
            forward_return_20,
            forward_return_30,
            forward_return_60
        FROM signal_decision_daily
        WHERE {" AND ".join(decision_where)}
        ORDER BY dt ASC, code ASC
        """,
        decision_params,
    ).fetchall()
    decision_payloads = [
        {
            "dt": int(row[0]),
            "code": str(row[1]),
            "entry_qualified": bool(row[2]),
            "setup_type": str(row[3] or "unknown").strip() or "unknown",
            "forward_return_5": float(row[4]) if row[4] is not None else None,
            "forward_return_20": float(row[5]) if row[5] is not None else None,
            "forward_return_30": float(row[6]) if row[6] is not None else None,
            "forward_return_60": float(row[7]) if row[7] is not None else None,
        }
        for row in decision_rows
    ]
    qualified_decisions = [row for row in decision_payloads if row["entry_qualified"]]
    baseline_by_date = _build_decision_baseline_map(
        conn,
        side=side,
        logic_version=logic_version,
        from_ymd=from_ymd,
        to_ymd=to_ymd,
    )
    _maybe_ensure_market_regime_rows(from_ymd=from_ymd, to_ymd=to_ymd, db_path=None)
    regime_by_date = _load_market_regime_map(conn, from_ymd=from_ymd, to_ymd=to_ymd)
    setup_groups: dict[str, list[dict[str, Any]]] = {}
    month_groups: dict[str, list[dict[str, Any]]] = {}
    regime_groups: dict[str, list[dict[str, Any]]] = {}
    for row in qualified_decisions:
        dt = int(row["dt"])
        row["baseline_return_30"] = baseline_by_date.get(dt)
        row["lift_vs_universe_30"] = (
            float(row["forward_return_30"]) - float(row["baseline_return_30"])
            if isinstance(row.get("forward_return_30"), (int, float)) and isinstance(row.get("baseline_return_30"), (int, float))
            else None
        )
        month_key = _month_key(dt) or "unknown"
        regime_tag = str((regime_by_date.get(dt) or {}).get("regime_tag") or "unknown")
        row["month_key"] = month_key
        row["regime_tag"] = regime_tag
        setup_groups.setdefault(str(row["setup_type"]), []).append(row)
        month_groups.setdefault(month_key, []).append(row)
        regime_groups.setdefault(regime_tag, []).append(row)
    signal_events = _build_signal_event_items_for_analysis(
        conn,
        side=side,
        logic_version=logic_version,
        from_ymd=from_ymd,
        to_ymd=to_ymd,
    )
    signal_count_groups: dict[str, list[dict[str, Any]]] = {"single": [], "repeated": []}
    break_reason_groups: dict[str, list[dict[str, Any]]] = {}
    for item in signal_events:
        row = dict(item)
        dt = int(item["signal_date"])
        row["baseline_return_30"] = baseline_by_date.get(dt)
        row["lift_vs_universe_30"] = (
            float(item["return_30d"]) - float(row["baseline_return_30"])
            if isinstance(item.get("return_30d"), (int, float)) and isinstance(row.get("baseline_return_30"), (int, float))
            else None
        )
        bucket = "repeated" if item.get("campaign_id") else "single"
        signal_count_groups.setdefault(bucket, []).append(row)
        if str(item.get("break_status") or "") == "broken":
            reason_key = str(item.get("break_reason") or "unknown")
            break_reason_groups.setdefault(reason_key, []).append(row)
    monthly_rows = _build_group_metric_rows(month_groups, label_key="month", return_key="forward_return_30", lift_key="lift_vs_universe_30")
    by_setup_type_rows = _build_group_metric_rows(setup_groups, label_key="setup_type", return_key="forward_return_30", lift_key="lift_vs_universe_30")
    by_regime_rows = _build_group_metric_rows(regime_groups, label_key="regime_tag", return_key="forward_return_30", lift_key="lift_vs_universe_30")
    by_break_reason_rows = _build_group_metric_rows(break_reason_groups, label_key="break_reason", return_key="return_30d", lift_key="lift_vs_universe_30")
    by_signal_count_rows = _build_group_metric_rows(signal_count_groups, label_key="bucket", return_key="return_30d", lift_key="lift_vs_universe_30")
    overall_returns_30 = [float(row["forward_return_30"]) for row in qualified_decisions if isinstance(row.get("forward_return_30"), (int, float))]
    baseline_returns_30 = [float(row["baseline_return_30"]) for row in qualified_decisions if isinstance(row.get("baseline_return_30"), (int, float))]
    lifts_30 = [float(row["lift_vs_universe_30"]) for row in qualified_decisions if isinstance(row.get("lift_vs_universe_30"), (int, float))]
    campaign_completed = [item for item in signal_events if item.get("status") == "completed"]
    completed_returns = [float(item["return_30d"]) for item in campaign_completed if isinstance(item.get("return_30d"), (int, float))]
    broken_count = sum(1 for item in signal_events if str(item.get("break_status") or "") == "broken")
    return {
        "generated_at": _serialize_timestamp(datetime.now(timezone.utc)),
        "side": side,
        "logic_version": logic_version,
        "summary": {
            "qualified_decisions": len(qualified_decisions),
            "directional_hit_rate_30": _safe_ratio(sum(1 for value in overall_returns_30 if value > 0), len(overall_returns_30)),
            "average_directional_return_30": _safe_mean(overall_returns_30),
            "baseline_average_return_30": _safe_mean(baseline_returns_30),
            "average_lift_vs_universe_30": _safe_mean(lifts_30),
            "campaign_directional_win_rate": _safe_ratio(sum(1 for value in completed_returns if value > 0), len(completed_returns)),
            "campaign_break_rate": _safe_ratio(broken_count, len(signal_events)),
        },
        "rolling_6m": _rolling_average_series(monthly_rows, key="month", value_key="average_directional_return_30", window=6),
        "monthly": monthly_rows,
        "by_setup_type": by_setup_type_rows,
        "by_regime": by_regime_rows,
        "by_break_reason": by_break_reason_rows,
        "by_signal_count": by_signal_count_rows,
        "failure_examples": _signal_failure_examples(signal_events, limit=12),
    }


def _build_basis_leakage_audit(
    conn: duckdb.DuckDBPyConnection,
    *,
    basis_version: str,
    from_ymd: int | None,
    to_ymd: int | None,
) -> dict[str, Any]:
    where_parts = ["basis_version = ?"]
    params: list[Any] = [str(basis_version)]
    if from_ymd is not None:
        where_parts.append("dt >= ?")
        params.append(int(from_ymd))
    if to_ymd is not None:
        where_parts.append("dt <= ?")
        params.append(int(to_ymd))
    rows = conn.execute(
        f"""
        SELECT
            dt,
            code,
            source_as_of,
            pred_dt,
            model_version,
            basis_source,
            source_hash,
            payload_schema_version,
            basis_payload_json
        FROM signal_basis_daily
        WHERE {" AND ".join(where_parts)}
        ORDER BY dt DESC, code ASC
        """,
        params,
    ).fetchall()
    provenance_missing = 0
    future_source_as_of = 0
    future_pred_dt = 0
    prohibited_payload_rows = 0
    prohibited_samples: list[dict[str, Any]] = []
    for row in rows:
        payload = _json_load(str(row[8]) if row[8] is not None else None)
        has_missing = any(row[index] in (None, "") for index in (2, 5, 6, 7))
        if has_missing:
            provenance_missing += 1
        if row[2] is not None and int(row[2]) > int(row[0]):
            future_source_as_of += 1
            if len(prohibited_samples) < 10:
                prohibited_samples.append({"dt": int(row[0]), "code": str(row[1]), "violation": "source_as_of_gt_dt"})
        if row[3] is not None and int(row[3]) > int(row[0]):
            future_pred_dt += 1
            if len(prohibited_samples) < 10:
                prohibited_samples.append({"dt": int(row[0]), "code": str(row[1]), "violation": "pred_dt_gt_dt"})
        paths = _find_prohibited_basis_paths(payload)
        if paths:
            prohibited_payload_rows += 1
            if len(prohibited_samples) < 10:
                prohibited_samples.append(
                    {
                        "dt": int(row[0]),
                        "code": str(row[1]),
                        "violation": "prohibited_payload_keys",
                        "paths": paths[:5],
                    }
                )
    return {
        "basis_version": basis_version,
        "row_count": len(rows),
        "provenance_missing_count": provenance_missing,
        "future_source_as_of_count": future_source_as_of,
        "future_pred_dt_count": future_pred_dt,
        "prohibited_payload_row_count": prohibited_payload_rows,
        "violations": prohibited_samples,
    }


def _build_latest_signal_parity_audit(
    conn: duckdb.DuckDBPyConnection,
    *,
    logic_version: str,
    basis_version: str,
    side: str,
) -> dict[str, Any]:
    latest_dt = _table_latest_ymd(conn, "signal_basis_daily", "dt")
    if latest_dt is None:
        return {"status": "unavailable", "latest_dt": None, "sample_count": 0, "mismatch_count": 0, "samples": []}
    items, _buy_rank, _sell_rank = _load_basis_items_for_date(conn, dt=int(latest_dt), basis_version=basis_version)
    if not items:
        return {"status": "unavailable", "latest_dt": int(latest_dt), "sample_count": 0, "mismatch_count": 0, "samples": []}
    evaluated_all, _evaluated_ranked, _pred_dt, _model_version = _evaluate_trade_items_from_basis(
        items=items,
        as_of_int=int(latest_dt),
        side=side,
    )
    live_by_code = {
        str(item.get("code") or "").strip(): {
            "entry_qualified": bool(item.get("entryQualified") is True),
            "setup_type": str(item.get("setupType") or "").strip() or None,
        }
        for item in evaluated_all
        if str(item.get("code") or "").strip()
    }
    stored_rows = conn.execute(
        """
        SELECT code, entry_qualified, setup_type
        FROM signal_decision_daily
        WHERE dt = ? AND side = ? AND logic_version = ?
        ORDER BY code ASC
        """,
        [int(latest_dt), str(side), str(logic_version)],
    ).fetchall()
    stored_by_code = {
        str(row[0]): {
            "entry_qualified": bool(row[1]),
            "setup_type": str(row[2]).strip() if row[2] is not None and str(row[2]).strip() else None,
        }
        for row in stored_rows
    }
    mismatches: list[dict[str, Any]] = []
    for code in sorted(set(live_by_code) | set(stored_by_code)):
        live = live_by_code.get(code)
        stored = stored_by_code.get(code)
        if live == stored:
            continue
        mismatches.append({"code": code, "live": live, "stored": stored})
    return {
        "status": "ok" if not mismatches else "mismatch",
        "latest_dt": int(latest_dt),
        "latestDate": _ymd_to_iso(int(latest_dt)),
        "sample_count": len(stored_by_code),
        "mismatch_count": len(mismatches),
        "samples": mismatches[:10],
    }


def _build_external_label_audit() -> dict[str, Any]:
    try:
        from external_analysis.contracts.paths import resolve_label_db_path
    except Exception as exc:
        return {"status": "unavailable", "reason": f"label_path_unavailable:{exc.__class__.__name__}"}
    label_db_path = resolve_label_db_path()
    if not label_db_path.exists():
        return {"status": "unavailable", "db_path": str(label_db_path), "reason": "label_db_missing"}
    try:
        label_conn = duckdb.connect(str(label_db_path), read_only=True)
    except Exception as exc:
        return {"status": "unavailable", "db_path": str(label_db_path), "reason": f"label_db_open_failed:{exc.__class__.__name__}"}
    try:
        row = label_conn.execute(
            """
            SELECT
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE embargo_until_date IS NULL) AS embargo_missing_count,
                COUNT(*) FILTER (WHERE leakage_group_id IS NULL OR leakage_group_id = '') AS leakage_group_missing_count,
                MIN(policy_version) AS min_policy_version,
                MAX(policy_version) AS max_policy_version
            FROM label_daily_h20
            """
        ).fetchone()
    except Exception as exc:
        label_conn.close()
        return {"status": "unavailable", "db_path": str(label_db_path), "reason": f"label_daily_h20_unavailable:{exc.__class__.__name__}"}
    finally:
        try:
            label_conn.close()
        except Exception:
            pass
    return {
        "status": "ok",
        "db_path": str(label_db_path),
        "row_count": int(row[0] or 0) if row else 0,
        "embargo_missing_count": int(row[1] or 0) if row else 0,
        "leakage_group_missing_count": int(row[2] or 0) if row else 0,
        "policy_version_min": str(row[3]) if row and row[3] is not None else None,
        "policy_version_max": str(row[4]) if row and row[4] is not None else None,
    }


def get_signal_tracking_leakage_audit(
    *,
    side: str = "buy",
    logic_version: str | None = None,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    with _open_conn(db_path) as conn:
        resolved_logic_version, basis_version = _resolve_logic_version(conn, logic_version)
    with _open_conn(db_path, read_only=True) as conn:
        basis_audit = _build_basis_leakage_audit(
            conn,
            basis_version=basis_version,
            from_ymd=from_int,
            to_ymd=to_int,
        )
        latest_parity = _build_latest_signal_parity_audit(
            conn,
            logic_version=resolved_logic_version,
            basis_version=basis_version,
            side=normalized_side,
        )
    external_labels = _build_external_label_audit()
    return {
        "generated_at": _serialize_timestamp(datetime.now(timezone.utc)),
        "side": normalized_side,
        "logic_version": resolved_logic_version,
        "basis_version": basis_version,
        "basis_audit": basis_audit,
        "latest_parity": latest_parity,
        "external_labels": external_labels,
        "replay_parity": {
            "status": "not_run",
            "reason": "historical replay sample parity is not executed inline; use leakage audit artifact generation for offline checks",
        },
    }


def _classify_break_status(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    side: str,
    logic_version: str,
    signal_date: int,
    reason_snapshot: dict[str, Any] | None,
    bars: list[DailyBar],
    start_index: int,
    anchor_close_price: float | None,
    observed_end_index: int,
    status: str,
    decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if start_index < 0 or observed_end_index < start_index:
        return {"break_status": "alive", "first_break_date": None, "break_reason": None}
    reason_snapshot = reason_snapshot or {}
    invalidation_trigger = str(reason_snapshot.get("invalidationTrigger") or "").strip().lower()
    horizon_end_ymd = bars[observed_end_index].date
    opposite_side = "sell" if side == "buy" else "buy"

    def finish(date_ymd: int | None, reason: str | None) -> dict[str, Any]:
        if date_ymd is not None and reason:
            return {
                "break_status": "broken",
                "first_break_date": date_ymd,
                "break_reason": reason,
            }
        if status == "active":
            return {"break_status": "alive", "first_break_date": None, "break_reason": None}
        return {"break_status": "completed_clean", "first_break_date": None, "break_reason": None}

    if anchor_close_price is not None and anchor_close_price > 0:
        for bar in bars[start_index : observed_end_index + 1]:
            if invalidation_trigger == "stop3":
                adverse_price = bar.low if side == "buy" else bar.high
                adverse = _safe_directional_return(side, adverse_price, anchor_close_price)
                if adverse is not None and adverse <= -0.03:
                    return finish(bar.date, "invalidation:stop3")
            elif invalidation_trigger == "stop5":
                adverse_price = bar.low if side == "buy" else bar.high
                adverse = _safe_directional_return(side, adverse_price, anchor_close_price)
                if adverse is not None and adverse <= -0.05:
                    return finish(bar.date, "invalidation:stop5")
            elif invalidation_trigger == "box_break" and side == "buy":
                box_lower = reason_snapshot.get("boxLower")
                if isinstance(box_lower, (int, float)) and bar.low is not None and bar.low < float(box_lower):
                    return finish(bar.date, "invalidation:box_break")
            elif invalidation_trigger == "box_reclaim" and side == "sell":
                box_upper = reason_snapshot.get("boxUpper")
                if isinstance(box_upper, (int, float)) and bar.high is not None and bar.high > float(box_upper):
                    return finish(bar.date, "invalidation:box_reclaim")

    cache_key = (str(code), str(logic_version), int(signal_date), int(horizon_end_ymd))
    if decision_rows_cache is not None and cache_key in decision_rows_cache:
        decision_rows = decision_rows_cache[cache_key]
    else:
        decision_rows = _load_code_decision_rows(
            conn,
            code=code,
            logic_version=logic_version,
            from_ymd=signal_date,
            to_ymd=horizon_end_ymd,
        )
        if decision_rows_cache is not None:
            decision_rows_cache[cache_key] = decision_rows

    opposite_signal_date = next(
        (
            int(row["dt"])
            for row in decision_rows
            if row["side"] == opposite_side and row["entry_qualified"] and int(row["dt"]) > int(signal_date)
        ),
        None,
    )
    if opposite_signal_date is not None:
        return finish(opposite_signal_date, f"opposite_signal:{opposite_side}")

    tone_by_date: dict[int, dict[str, float | None]] = {}
    for row in decision_rows:
        score_snapshot = row.get("score_snapshot") or {}
        score_value = score_snapshot.get("tradePriorityScore")
        tone_by_date.setdefault(int(row["dt"]), {})[str(row["side"])] = (
            float(score_value) if isinstance(score_value, (int, float)) else None
        )
    consecutive = 0
    for dt in sorted(tone_by_date):
        scores = tone_by_date[dt]
        buy_score = scores.get("buy")
        sell_score = scores.get("sell")
        dominant_side = None
        if buy_score is not None or sell_score is not None:
            if buy_score is None:
                dominant_side = "sell"
            elif sell_score is None:
                dominant_side = "buy"
            elif buy_score > sell_score:
                dominant_side = "buy"
            elif sell_score > buy_score:
                dominant_side = "sell"
        if dominant_side == opposite_side and dt > int(signal_date):
            consecutive += 1
            if consecutive >= 2:
                return finish(dt, f"opposite_tone_flip:{opposite_side}")
        else:
            consecutive = 0
    return finish(None, None)


def _build_signal_event_item(
    conn: duckdb.DuckDBPyConnection,
    *,
    occurrence_row: tuple[Any, ...],
    latest_market_ymd: int | None,
    bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]],
    decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    (
        occurrence_id,
        campaign_id,
        code,
        side,
        signal_date,
        basis_version,
        logic_version,
        name,
        setup_type,
        reason_json,
        score_json,
        entry_close_price,
        entry_next_open_price,
    ) = occurrence_row
    code = str(code)
    side = str(side)
    signal_date = int(signal_date)
    cache_key = code
    if cache_key not in bars_cache:
        bars = _fetch_code_bars(conn, code=code, start_ymd=signal_date)
        bars_cache[cache_key] = (bars, _bar_price_lookup(bars))
    bars, by_date = bars_cache[cache_key]
    start_index = by_date.get(signal_date)
    if start_index is None:
        return None
    reason_snapshot = _json_load(str(reason_json) if reason_json is not None else None) or {}
    score_snapshot = _json_load(str(score_json) if score_json is not None else None) or {}
    window_metrics = _compute_window_metrics(
        side=side,
        bars=bars,
        start_index=start_index,
        anchor_close_price=float(entry_close_price) if entry_close_price is not None else None,
        anchor_exec_price=float(entry_next_open_price) if entry_next_open_price is not None else None,
        latest_market_ymd=latest_market_ymd,
    )
    break_info = _classify_break_status(
        conn,
        code=code,
        side=side,
        logic_version=str(logic_version),
        signal_date=signal_date,
        reason_snapshot=reason_snapshot,
        bars=bars,
        start_index=start_index,
        anchor_close_price=float(entry_close_price) if entry_close_price is not None else None,
        observed_end_index=int(window_metrics["observed_end_index"]),
        status=str(window_metrics["status"]),
        decision_rows_cache=decision_rows_cache,
    )
    reason_list = reason_snapshot.get("tradeDecisionReasons")
    if not isinstance(reason_list, list):
        reason_list = []
    return {
        "event_id": str(occurrence_id),
        "occurrence_id": str(occurrence_id),
        "campaign_id": str(campaign_id) if campaign_id is not None else None,
        "code": code,
        "name": str(name) if name is not None else None,
        "side": side,
        "signal_date": signal_date,
        "signalDate": _ymd_to_iso(signal_date),
        "basis_version": str(basis_version or DEFAULT_BASIS_VERSION),
        "logic_version": str(logic_version or DEFAULT_LOGIC_VERSION),
        "setup_type": str(setup_type) if setup_type is not None else reason_snapshot.get("setupType"),
        "anchor_price_close": float(entry_close_price) if entry_close_price is not None else None,
        "anchor_price_next_open": float(entry_next_open_price) if entry_next_open_price is not None else None,
        "current_directional_return": window_metrics["current_return_close_basis"],
        "current_exec_directional_return": window_metrics["current_return_exec_basis"],
        "return_30d": window_metrics["final_return_at_horizon"],
        "max_favorable_30": window_metrics["max_favorable_return"],
        "max_adverse_30": window_metrics["max_adverse_return"],
        "days_to_max_favorable_30": window_metrics.get("days_to_max_favorable_30"),
        "days_to_max_adverse_30": window_metrics.get("days_to_max_adverse_30"),
        "date_of_max_favorable_30": window_metrics.get("date_of_max_favorable_30"),
        "date_of_max_adverse_30": window_metrics.get("date_of_max_adverse_30"),
        "status": window_metrics["status"],
        "elapsed_bars": int(window_metrics["elapsed_bars"]),
        "remaining_bars": int(window_metrics["remaining_bars"]),
        "completed_at": _serialize_timestamp(window_metrics.get("completed_at")),
        "archived_at": _serialize_timestamp(window_metrics.get("archived_at")),
        "break_status": break_info["break_status"],
        "first_break_date": break_info["first_break_date"],
        "firstBreakDate": _ymd_to_iso(break_info["first_break_date"]),
        "break_reason": break_info["break_reason"],
        "reason_summary": list(reason_list[:3]),
        "reason_snapshot": reason_snapshot,
        "score_snapshot": score_snapshot,
        "priority_score": score_snapshot.get("tradePriorityScore"),
    }


def list_signal_events(
    *,
    status: str = DEFAULT_STATUS,
    side: str = "buy",
    logic_version: str | None = None,
    code: str | None = None,
    query: str | None = None,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    limit: int = 200,
    offset: int = 0,
    sort: str = "recent",
    outcome: str = "all",
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_status = str(status or DEFAULT_STATUS).strip().lower()
    if normalized_status not in SUPPORTED_STATUSES:
        raise ValueError("status must be active|completed|archive")
    normalized_side = _normalize_side(side)
    normalized_sort = _normalize_tracking_sort(sort)
    normalized_outcome = _normalize_tracking_outcome(outcome)
    normalized_limit = _coerce_tracking_limit(limit)
    normalized_offset = _coerce_tracking_offset(offset)
    ensure_signal_tracking_current(as_of=as_of, db_path=db_path)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    search = str(query or "").strip()
    code_filter = str(code or "").strip()
    with _open_conn(db_path) as conn:
        resolved_logic_version, _ = _resolve_logic_version(conn, logic_version)
    where_parts = ["so.side = ?", "so.logic_version = ?"]
    params: list[Any] = [normalized_side, resolved_logic_version]
    if from_int is not None:
        where_parts.append("so.signal_date >= ?")
        params.append(int(from_int))
    if to_int is not None:
        where_parts.append("so.signal_date <= ?")
        params.append(int(to_int))
    if code_filter:
        where_parts.append("so.code = ?")
        params.append(code_filter)
    elif search:
        where_parts.append("(so.code LIKE ? OR COALESCE(sd.name, sm.name, '') LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        rows = conn.execute(
            f"""
            SELECT
                so.occurrence_id,
                so.campaign_id,
                so.code,
                so.side,
                so.signal_date,
                so.basis_version,
                so.logic_version,
                COALESCE(sd.name, sm.name) AS name,
                sd.setup_type,
                so.reason_snapshot_json,
                so.score_snapshot_json,
                so.entry_close_price,
                so.entry_next_open_price
            FROM signal_occurrence AS so
            LEFT JOIN signal_decision_daily AS sd
              ON sd.dt = so.signal_date
             AND sd.code = so.code
             AND sd.side = so.side
             AND sd.logic_version = so.logic_version
            LEFT JOIN stock_meta AS sm
              ON sm.code = so.code
            WHERE {" AND ".join(where_parts)}
            ORDER BY so.signal_date DESC, so.code ASC
            """,
            params,
        ).fetchall()
        bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
        decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
        items = []
        for row in rows:
            item = _build_signal_event_item(
                conn,
                occurrence_row=row,
                latest_market_ymd=latest_market_ymd,
                bars_cache=bars_cache,
                decision_rows_cache=decision_rows_cache,
            )
            if not item or item["status"] != normalized_status:
                continue
            metric = _signal_event_metric(item)
            if not _matches_tracking_outcome(
                metric=metric,
                break_status=str(item.get("break_status") or "").strip().lower() or None,
                outcome=normalized_outcome,
            ):
                continue
            items.append(item)
    if normalized_sort == "oldest":
        items.sort(
            key=lambda item: (
                int(item["signal_date"]),
                -(float(item["priority_score"]) if isinstance(item.get("priority_score"), (int, float)) else float("-inf")),
                str(item["event_id"]),
            )
        )
    elif normalized_sort == "best":
        items.sort(
            key=lambda item: (
                -(_signal_event_metric(item) if _signal_event_metric(item) is not None else float("-inf")),
                -int(item["signal_date"]),
                str(item["event_id"]),
            )
        )
    elif normalized_sort == "worst":
        items.sort(
            key=lambda item: (
                _signal_event_metric(item) if _signal_event_metric(item) is not None else float("inf"),
                -int(item["signal_date"]),
                str(item["event_id"]),
            )
        )
    else:
        items.sort(
            key=lambda item: (
                -int(item["signal_date"]),
                -(float(item["priority_score"]) if isinstance(item.get("priority_score"), (int, float)) else float("-inf")),
                str(item["event_id"]),
            )
        )
    total_count = len(items)
    page_items = items[normalized_offset : normalized_offset + normalized_limit]
    next_offset = normalized_offset + len(page_items)
    return {
        "status": normalized_status,
        "side": normalized_side,
        "logic_version": resolved_logic_version,
        "items": page_items,
        "count": total_count,
        "offset": normalized_offset,
        "limit": normalized_limit,
        "sort": normalized_sort,
        "outcome": normalized_outcome,
        "has_more": next_offset < total_count,
        "next_offset": next_offset if next_offset < total_count else None,
        "query": search or code_filter or None,
    }


def get_signal_event_detail(
    event_id: str,
    *,
    as_of: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_event_id = str(event_id or "").strip()
    if not normalized_event_id:
        raise ValueError("event_id is required")
    ensure_signal_tracking_current(as_of=as_of, db_path=db_path)
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        row = conn.execute(
            """
            SELECT
                so.occurrence_id,
                so.campaign_id,
                so.code,
                so.side,
                so.signal_date,
                so.basis_version,
                so.logic_version,
                COALESCE(sd.name, sm.name) AS name,
                sd.setup_type,
                so.reason_snapshot_json,
                so.score_snapshot_json,
                so.entry_close_price,
                so.entry_next_open_price
            FROM signal_occurrence AS so
            LEFT JOIN signal_decision_daily AS sd
              ON sd.dt = so.signal_date
             AND sd.code = so.code
             AND sd.side = so.side
             AND sd.logic_version = so.logic_version
            LEFT JOIN stock_meta AS sm
              ON sm.code = so.code
            WHERE so.occurrence_id = ?
            """,
            [normalized_event_id],
        ).fetchone()
        if not row:
            raise KeyError(normalized_event_id)
        event = _build_signal_event_item(
            conn,
            occurrence_row=row,
            latest_market_ymd=latest_market_ymd,
            bars_cache={},
            decision_rows_cache={},
        )
        if event is None:
            raise KeyError(normalized_event_id)
        campaign = None
        if event.get("campaign_id"):
            try:
                campaign = get_signal_campaign_detail(str(event["campaign_id"]), db_path=db_path)
            except Exception:
                campaign = None
        bars = _fetch_code_bars(conn, code=event["code"], start_ymd=event["signal_date"])
        by_date = _bar_price_lookup(bars)
        start_index = by_date.get(event["signal_date"])
        price_series = []
        if start_index is not None:
            price_series = _compute_window_metrics(
                side=event["side"],
                bars=bars,
                start_index=start_index,
                anchor_close_price=event["anchor_price_close"],
                anchor_exec_price=event["anchor_price_next_open"],
                latest_market_ymd=latest_market_ymd,
            )["price_series"]
    runtime_stock_db_contract = _runtime_stock_db_status(
        requested_symbol=event["code"],
        requested_chart_date=event["signal_date"],
        db_path=db_path,
    )
    chart_basis_fields = _build_chart_basis_detail_fields(runtime_stock_db_contract)
    artifact_source_last_date = runtime_stock_db_contract.get("latest_available_global_date")
    date_match_status = str(runtime_stock_db_contract.get("date_match_status") or "blocked")
    if date_match_status not in {"exact", "lagged_provisional", "blocked"}:
        date_match_status = "blocked"
    judgment_validity_status = date_match_status
    return {
        "event": event,
        "campaign": campaign["campaign"] if isinstance(campaign, dict) and "campaign" in campaign else campaign,
        "occurrences": campaign["occurrences"] if isinstance(campaign, dict) and "occurrences" in campaign else [],
        "price_series": price_series,
        "requested_chart_date": event["signal_date"],
        "requested_chart_date_iso": _ymd_to_iso(event["signal_date"]),
        "artifact_source_last_date": artifact_source_last_date,
        "artifact_source_last_date_iso": _ymd_to_iso(int(artifact_source_last_date)) if isinstance(artifact_source_last_date, int) else None,
        "date_gap_days": runtime_stock_db_contract.get("date_gap_days"),
        "date_match_status": date_match_status,
        "judgment_validity_status": judgment_validity_status,
        "runtime_db_path": runtime_stock_db_contract.get("runtime_db_path"),
        "resolution_reason": runtime_stock_db_contract.get("resolution_reason"),
        "source_freshness_status": runtime_stock_db_contract.get("source_freshness_status"),
        "runtime_stock_db_contract": runtime_stock_db_contract,
        **chart_basis_fields,
    }


def get_signal_markers(
    *,
    code: str,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    logic_version: str | None = None,
    ranking_logic_version: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    code = str(code or "").strip()
    if not code:
        raise ValueError("code is required")
    with _open_conn(db_path) as conn:
        resolved_logic_version, _ = _resolve_logic_version(conn, logic_version)
        resolved_ranking_logic_version, _ = _resolve_ranking_logic_version(conn, ranking_logic_version)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    signal_payload = list_signal_events(
        status="active",
        side="buy",
        logic_version=resolved_logic_version,
        code=code,
        from_ymd=from_int,
        to_ymd=to_int,
        limit=500,
        db_path=db_path,
    )
    sell_payload = list_signal_events(
        status="active",
        side="sell",
        logic_version=resolved_logic_version,
        code=code,
        from_ymd=from_int,
        to_ymd=to_int,
        limit=500,
        db_path=db_path,
    )
    status_payloads = [signal_payload["items"], sell_payload["items"]]
    for status_name in ("completed", "archive"):
        status_payloads.append(
            list_signal_events(
                status=status_name,
                side="buy",
                logic_version=resolved_logic_version,
                code=code,
                from_ymd=from_int,
                to_ymd=to_int,
                limit=500,
                db_path=db_path,
            )["items"]
        )
        status_payloads.append(
            list_signal_events(
                status=status_name,
                side="sell",
                logic_version=resolved_logic_version,
                code=code,
                from_ymd=from_int,
                to_ymd=to_int,
                limit=500,
                db_path=db_path,
            )["items"]
        )
    marker_items = {
        str(item["event_id"]): item
        for payload_items in status_payloads
        for item in payload_items
    }
    with _open_conn(db_path, read_only=True) as conn:
        where_parts = ["code = ?", "ranking_logic_version = ?"]
        params: list[Any] = [code, resolved_ranking_logic_version]
        if from_int is not None:
            where_parts.append("dt >= ?")
            params.append(int(from_int))
        if to_int is not None:
            where_parts.append("dt <= ?")
            params.append(int(to_int))
        ranking_rows = conn.execute(
            f"""
            SELECT appearance_id, dt, dir, rank, break_status
            FROM ranking_appearance_daily
            WHERE {" AND ".join(where_parts)}
            ORDER BY dt DESC, rank ASC
            """,
            params,
        ).fetchall()
    ranking_items = [
        {
            "appearance_id": str(row[0]),
            "date": int(row[1]),
            "date_iso": _ymd_to_iso(int(row[1])),
            "dir": str(row[2]),
            "rank": int(row[3]),
            "break_status": str(row[4]) if row[4] is not None else None,
        }
        for row in ranking_rows
    ]
    return {
        "code": code,
        "logic_version": resolved_logic_version,
        "ranking_logic_version": resolved_ranking_logic_version,
        "signal_events": sorted(marker_items.values(), key=lambda item: (item["signal_date"], item["side"])),
        "ranking_appearances": ranking_items,
    }


def rebuild_ranking_appearances(
    *,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    ranking_logic_version: str | None = None,
    signal_logic_version: str | None = None,
    basis_version: str | None = None,
    reset_scope: bool = False,
    db_path: str | None = None,
    progress_cb: TrackingProgressCallback | None = None,
) -> dict[str, Any]:
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    with _open_conn(db_path) as conn:
        resolved_ranking_logic_version, default_basis_version = _resolve_ranking_logic_version(conn, ranking_logic_version)
        resolved_signal_logic_version, _ = _resolve_logic_version(conn, signal_logic_version)
    resolved_basis_version = str(basis_version or default_basis_version or DEFAULT_BASIS_VERSION)
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        where_parts = ["basis_version = ?"]
        params: list[Any] = [resolved_basis_version]
        if from_int is not None:
            where_parts.append("dt >= ?")
            params.append(int(from_int))
        if to_int is not None:
            where_parts.append("dt <= ?")
            params.append(int(to_int))
        basis_dates = [
            int(row[0])
            for row in conn.execute(
                f"SELECT DISTINCT dt FROM signal_basis_daily WHERE {' AND '.join(where_parts)} ORDER BY dt",
                params,
            ).fetchall()
            if row and row[0] is not None
        ]
    appearance_rows: list[list[Any]] = []
    with _open_conn(db_path, read_only=True) as conn:
        bars_cache: dict[str, tuple[list[DailyBar], dict[int, int]]] = {}
        decision_rows_cache: dict[tuple[str, str, int, int], list[dict[str, Any]]] = {}
        total_dates = len(basis_dates)
        progress_state: dict[str, Any] = {"last_emit_at": 0.0}
        _emit_tracking_progress(
            progress_cb,
            _tracking_progress_event(
                phase="ranking_appearances",
                status="start",
                processed=0,
                total=total_dates,
                detail=f"rebuilding ranking appearances (logic_version={resolved_ranking_logic_version})",
            ),
            throttle_state=progress_state,
            force=True,
        )
        for index, dt in enumerate(basis_dates, start=1):
            items, _buy_rank, _sell_rank = _load_basis_items_for_date(conn, dt=dt, basis_version=resolved_basis_version)
            if not items:
                continue
            buy_all, buy_ranked, _buy_pred_dt, _buy_model_version = _evaluate_trade_items_from_basis(
                items=items,
                as_of_int=dt,
                side="buy",
            )
            sell_all, sell_ranked, _sell_pred_dt, _sell_model_version = _evaluate_trade_items_from_basis(
                items=items,
                as_of_int=dt,
                side="sell",
            )
            buy_map = {str(item.get("code") or "").strip(): item for item in buy_all}
            sell_map = {str(item.get("code") or "").strip(): item for item in sell_all}
            buy_ranked_codes = {str(item.get("code") or "").strip() for item in buy_ranked}
            sell_ranked_codes = {str(item.get("code") or "").strip() for item in sell_ranked}
            for direction, side_name, scored_items in (("up", "buy", buy_all), ("down", "sell", sell_all)):
                for index, item in enumerate(scored_items[:DEFAULT_RANKING_LIMIT], start=1):
                    code = str(item.get("code") or "").strip()
                    if not code:
                        continue
                    peer_item = sell_map.get(code) if side_name == "buy" else buy_map.get(code)
                    this_ranked = code in (buy_ranked_codes if side_name == "buy" else sell_ranked_codes)
                    opposite_ranked = code in (sell_ranked_codes if side_name == "buy" else buy_ranked_codes)
                    if this_ranked and opposite_ranked:
                        signal_state = "both"
                    elif this_ranked:
                        signal_state = side_name
                    elif opposite_ranked:
                        signal_state = "sell" if side_name == "buy" else "buy"
                    else:
                        signal_state = "wait"
                    if code not in bars_cache:
                        bars = _fetch_code_bars(conn, code=code, start_ymd=dt)
                        bars_cache[code] = (bars, _bar_price_lookup(bars))
                    bars, by_date = bars_cache[code]
                    start_index = by_date.get(dt)
                    if start_index is None:
                        continue
                    entry_close_price = bars[start_index].close if start_index < len(bars) else None
                    entry_next_open_price = bars[start_index + 1].open if start_index + 1 < len(bars) else None
                    window_metrics = _compute_window_metrics(
                        side=side_name,
                        bars=bars,
                        start_index=start_index,
                        anchor_close_price=entry_close_price,
                        anchor_exec_price=entry_next_open_price,
                        latest_market_ymd=latest_market_ymd,
                    )
                    reason_snapshot = _build_reason_snapshot(item)
                    break_info = _classify_break_status(
                        conn,
                        code=code,
                        side=side_name,
                        logic_version=resolved_signal_logic_version,
                        signal_date=dt,
                        reason_snapshot=reason_snapshot,
                        bars=bars,
                        start_index=start_index,
                        anchor_close_price=entry_close_price,
                        observed_end_index=int(window_metrics["observed_end_index"]),
                        status=str(window_metrics["status"]),
                        decision_rows_cache=decision_rows_cache,
                    )
                    payload = {
                        "ranking_item": item,
                        "peer_item": peer_item,
                        "signal_state_at_appearance": signal_state,
                        "dir": direction,
                        "rank": index,
                    }
                    appearance_rows.append(
                        [
                            f"{resolved_ranking_logic_version}:{direction}:{code}:{dt}:{index}",
                            int(dt),
                            direction,
                            index,
                            code,
                            resolved_ranking_logic_version,
                            resolved_signal_logic_version,
                            resolved_basis_version,
                            str(item.get("name") or code),
                            _extract_display_score(item),
                            signal_state,
                            bool(item.get("entryQualified") is True),
                            str(item.get("setupType") or "").strip() or None,
                            entry_close_price,
                            entry_next_open_price,
                            window_metrics["current_return_close_basis"],
                            window_metrics["final_return_at_horizon"],
                            window_metrics["max_favorable_return"],
                            window_metrics["max_adverse_return"],
                            window_metrics.get("days_to_max_favorable_30"),
                            window_metrics.get("days_to_max_adverse_30"),
                            window_metrics.get("date_of_max_favorable_30"),
                            window_metrics.get("date_of_max_adverse_30"),
                            str(window_metrics["status"]),
                            break_info["break_status"],
                            break_info["first_break_date"],
                            break_info["break_reason"],
                            window_metrics.get("completed_at"),
                            window_metrics.get("archived_at"),
                            _json_dump(payload),
                            datetime.now(timezone.utc),
                        ]
                    )
            _emit_tracking_progress(
                progress_cb,
                _tracking_progress_event(
                    phase="ranking_appearances",
                    status="running",
                    processed=index,
                    total=total_dates,
                    current_market_ymd=dt,
                    current_market_date=_ymd_to_iso(dt),
                    detail=f"dt={dt} appearances={len(appearance_rows)}",
                ),
                throttle_state=progress_state,
            )
    with _open_conn(db_path) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            if reset_scope:
                delete_where = ["ranking_logic_version = ?"]
                delete_params: list[Any] = [resolved_ranking_logic_version]
                if from_int is not None:
                    delete_where.append("dt >= ?")
                    delete_params.append(int(from_int))
                if to_int is not None:
                    delete_where.append("dt <= ?")
                    delete_params.append(int(to_int))
                conn.execute(
                    f"DELETE FROM ranking_appearance_daily WHERE {' AND '.join(delete_where)}",
                    delete_params,
                )
            _bulk_insert_or_replace_rows(
                conn,
                table_name="ranking_appearance_daily",
                columns=[
                    "appearance_id",
                    "dt",
                    "dir",
                    "rank",
                    "code",
                    "ranking_logic_version",
                    "signal_logic_version",
                    "basis_version",
                    "name",
                    "display_score",
                    "signal_state_at_appearance",
                    "entry_qualified_at_appearance",
                    "setup_type_at_appearance",
                    "anchor_price_close",
                    "anchor_price_next_open",
                    "current_directional_return",
                    "return_30d",
                    "max_favorable_30",
                    "max_adverse_30",
                    "days_to_max_favorable_30",
                    "days_to_max_adverse_30",
                    "date_of_max_favorable_30",
                    "date_of_max_adverse_30",
                    "status",
                    "break_status",
                    "first_break_date",
                    "break_reason",
                    "completed_at",
                    "archived_at",
                    "payload_json",
                    "updated_at",
                ],
                rows=appearance_rows,
            )
            conn.execute(
                """
                UPDATE ranking_logic_registry
                SET basis_version = COALESCE(?, basis_version)
                WHERE ranking_logic_version = ?
                """,
                [resolved_basis_version, resolved_ranking_logic_version],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="ranking_appearances",
            status="done",
            processed=total_dates,
            total=total_dates,
            current_market_ymd=basis_dates[-1] if basis_dates else None,
            current_market_date=_ymd_to_iso(basis_dates[-1]) if basis_dates else None,
            detail=f"appearance_upserted={len(appearance_rows)}",
        ),
        throttle_state=progress_state,
        force=True,
    )
    return {
        "ok": True,
        "ranking_logic_version": resolved_ranking_logic_version,
        "signal_logic_version": resolved_signal_logic_version,
        "basis_version": resolved_basis_version,
        "dates_processed": len(basis_dates),
        "appearance_upserted": len(appearance_rows),
        "from": _ymd_to_iso(basis_dates[0]) if basis_dates else _ymd_to_iso(from_int),
        "to": _ymd_to_iso(basis_dates[-1]) if basis_dates else _ymd_to_iso(to_int),
    }


def list_ranking_appearances(
    *,
    status: str = DEFAULT_STATUS,
    direction: str = "up",
    ranking_logic_version: str | None = None,
    code: str | None = None,
    query: str | None = None,
    rank_bucket: str | None = None,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    limit: int = 200,
    offset: int = 0,
    sort: str = "recent",
    outcome: str = "all",
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_status = str(status or DEFAULT_STATUS).strip().lower()
    if normalized_status not in SUPPORTED_STATUSES:
        raise ValueError("status must be active|completed|archive")
    normalized_dir = _normalize_ranking_dir(direction)
    normalized_sort = _normalize_tracking_sort(sort)
    normalized_outcome = _normalize_tracking_outcome(outcome)
    normalized_limit = _coerce_tracking_limit(limit)
    normalized_offset = _coerce_tracking_offset(offset)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    search = str(query or "").strip()
    code_filter = str(code or "").strip()
    with _open_conn(db_path) as conn:
        resolved_ranking_logic_version, _ = _resolve_ranking_logic_version(conn, ranking_logic_version)
    where_parts = ["status = ?", "dir = ?", "ranking_logic_version = ?"]
    params: list[Any] = [normalized_status, normalized_dir, resolved_ranking_logic_version]
    score_expr = "COALESCE(return_30d, current_directional_return)"
    if from_int is not None:
        where_parts.append("dt >= ?")
        params.append(int(from_int))
    if to_int is not None:
        where_parts.append("dt <= ?")
        params.append(int(to_int))
    if code_filter:
        where_parts.append("code = ?")
        params.append(code_filter)
    elif search:
        where_parts.append("(code LIKE ? OR COALESCE(name, '') LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    if rank_bucket == "1-5":
        where_parts.append("rank BETWEEN 1 AND 5")
    elif rank_bucket == "6-10":
        where_parts.append("rank BETWEEN 6 AND 10")
    elif rank_bucket == "11-20":
        where_parts.append("rank BETWEEN 11 AND 20")
    elif rank_bucket == "21-50":
        where_parts.append("rank BETWEEN 21 AND 50")
    if normalized_outcome == "good":
        where_parts.append(f"{score_expr} > 0")
    elif normalized_outcome == "bad":
        where_parts.append(f"{score_expr} < 0")
    elif normalized_outcome == "broken":
        where_parts.append("break_status = 'broken'")
    if normalized_sort == "oldest":
        order_clause = "dt ASC, rank ASC, appearance_id ASC"
    elif normalized_sort == "best":
        order_clause = f"{score_expr} DESC NULLS LAST, dt DESC, rank ASC, appearance_id ASC"
    elif normalized_sort == "worst":
        order_clause = f"{score_expr} ASC NULLS LAST, dt DESC, rank ASC, appearance_id ASC"
    else:
        order_clause = "dt DESC, rank ASC, appearance_id ASC"
    with _open_conn(db_path, read_only=True) as conn:
        total_count = int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM ranking_appearance_daily
                WHERE {" AND ".join(where_parts)}
                """,
                params,
            ).fetchone()[0]
        )
        rows = conn.execute(
            f"""
            SELECT
                appearance_id,
                dt,
                dir,
                rank,
                code,
                ranking_logic_version,
                signal_logic_version,
                basis_version,
                name,
                display_score,
                signal_state_at_appearance,
                entry_qualified_at_appearance,
                setup_type_at_appearance,
                anchor_price_close,
                anchor_price_next_open,
                current_directional_return,
                return_30d,
                max_favorable_30,
                max_adverse_30,
                days_to_max_favorable_30,
                days_to_max_adverse_30,
                date_of_max_favorable_30,
                date_of_max_adverse_30,
                status,
                break_status,
                first_break_date,
                break_reason,
                completed_at,
                archived_at
            FROM ranking_appearance_daily
            WHERE {" AND ".join(where_parts)}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
            """,
            [*params, normalized_limit + 1, normalized_offset],
        ).fetchall()
    has_more = len(rows) > normalized_limit
    if has_more:
        rows = rows[:normalized_limit]
    items = [
        {
            "appearance_id": str(row[0]),
            "date": int(row[1]),
            "date_iso": _ymd_to_iso(int(row[1])),
            "dir": str(row[2]),
            "rank": int(row[3]),
            "code": str(row[4]),
            "ranking_logic_version": str(row[5]),
            "signal_logic_version": str(row[6]) if row[6] is not None else DEFAULT_LOGIC_VERSION,
            "basis_version": str(row[7]) if row[7] is not None else DEFAULT_BASIS_VERSION,
            "name": str(row[8]) if row[8] is not None else None,
            "display_score": float(row[9]) if row[9] is not None else None,
            "signal_state_at_appearance": str(row[10]) if row[10] is not None else "wait",
            "entry_qualified_at_appearance": bool(row[11]),
            "setup_type_at_appearance": str(row[12]) if row[12] is not None else None,
            "anchor_price_close": float(row[13]) if row[13] is not None else None,
            "anchor_price_next_open": float(row[14]) if row[14] is not None else None,
            "current_directional_return": float(row[15]) if row[15] is not None else None,
            "return_30d": float(row[16]) if row[16] is not None else None,
            "max_favorable_30": float(row[17]) if row[17] is not None else None,
            "max_adverse_30": float(row[18]) if row[18] is not None else None,
            "days_to_max_favorable_30": int(row[19]) if row[19] is not None else None,
            "days_to_max_adverse_30": int(row[20]) if row[20] is not None else None,
            "date_of_max_favorable_30": int(row[21]) if row[21] is not None else None,
            "date_of_max_adverse_30": int(row[22]) if row[22] is not None else None,
            "status": str(row[23]),
            "break_status": str(row[24]) if row[24] is not None else None,
            "first_break_date": int(row[25]) if row[25] is not None else None,
            "firstBreakDate": _ymd_to_iso(int(row[25])) if row[25] is not None else None,
            "break_reason": str(row[26]) if row[26] is not None else None,
            "completed_at": _serialize_timestamp(row[27]) if isinstance(row[27], datetime) else None,
            "archived_at": _serialize_timestamp(row[28]) if isinstance(row[28], datetime) else None,
        }
        for row in rows
    ]
    with _open_conn(db_path, read_only=True) as conn:
        bars_by_code: dict[str, list[DailyBar]] = {}
        for item in items:
            code_value = str(item["code"])
            bars = bars_by_code.get(code_value)
            if bars is None:
                bars = _fetch_code_bars(conn, code=code_value, start_ymd=int(item["date"]))
                bars_by_code[code_value] = bars
            if not bars:
                continue
            anchor_price = item.get("anchor_price_close")
            latest_bar = bars[-1]
            item["current_directional_return"] = _safe_directional_return(
                "sell" if item["dir"] == "down" else "buy",
                latest_bar.close,
                anchor_price,
            )
            for bar in bars:
                favorable_basis = bar.low if item["dir"] == "down" else bar.high
                adverse_basis = bar.high if item["dir"] == "down" else bar.low
                favorable = _safe_directional_return("sell" if item["dir"] == "down" else "buy", favorable_basis, anchor_price)
                adverse = _safe_directional_return("sell" if item["dir"] == "down" else "buy", adverse_basis, anchor_price)
                if favorable is not None and (item["max_favorable_30"] is None or favorable > item["max_favorable_30"]):
                    item["max_favorable_30"] = favorable
                if adverse is not None and (item["max_adverse_30"] is None or adverse < item["max_adverse_30"]):
                    item["max_adverse_30"] = adverse
    return {
        "status": normalized_status,
        "dir": normalized_dir,
        "ranking_logic_version": resolved_ranking_logic_version,
        "items": items,
        "count": total_count,
        "offset": normalized_offset,
        "limit": normalized_limit,
        "sort": normalized_sort,
        "outcome": normalized_outcome,
        "has_more": has_more,
        "next_offset": (normalized_offset + len(items)) if has_more else None,
        "query": search or code_filter or None,
        "rank_bucket": rank_bucket,
    }


def get_ranking_appearance_detail(
    appearance_id: str,
    *,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_id = str(appearance_id or "").strip()
    if not normalized_id:
        raise ValueError("appearance_id is required")
    with _open_conn(db_path, read_only=True) as conn:
        latest_market_ymd = _latest_market_ymd(conn)
        row = conn.execute(
            """
            SELECT
                appearance_id,
                dt,
                dir,
                rank,
                code,
                ranking_logic_version,
                signal_logic_version,
                basis_version,
                name,
                display_score,
                signal_state_at_appearance,
                entry_qualified_at_appearance,
                setup_type_at_appearance,
                anchor_price_close,
                anchor_price_next_open,
                current_directional_return,
                return_30d,
                max_favorable_30,
                max_adverse_30,
                days_to_max_favorable_30,
                days_to_max_adverse_30,
                date_of_max_favorable_30,
                date_of_max_adverse_30,
                status,
                break_status,
                first_break_date,
                break_reason,
                payload_json
            FROM ranking_appearance_daily
            WHERE appearance_id = ?
            """,
            [normalized_id],
        ).fetchone()
        if not row:
            raise KeyError(normalized_id)
        appearance = {
            "appearance_id": str(row[0]),
            "date": int(row[1]),
            "date_iso": _ymd_to_iso(int(row[1])),
            "dir": str(row[2]),
            "rank": int(row[3]),
            "code": str(row[4]),
            "ranking_logic_version": str(row[5]),
            "signal_logic_version": str(row[6]) if row[6] is not None else DEFAULT_LOGIC_VERSION,
            "basis_version": str(row[7]) if row[7] is not None else DEFAULT_BASIS_VERSION,
            "name": str(row[8]) if row[8] is not None else None,
            "display_score": float(row[9]) if row[9] is not None else None,
            "signal_state_at_appearance": str(row[10]) if row[10] is not None else "wait",
            "entry_qualified_at_appearance": bool(row[11]),
            "setup_type_at_appearance": str(row[12]) if row[12] is not None else None,
            "anchor_price_close": float(row[13]) if row[13] is not None else None,
            "anchor_price_next_open": float(row[14]) if row[14] is not None else None,
            "current_directional_return": float(row[15]) if row[15] is not None else None,
            "return_30d": float(row[16]) if row[16] is not None else None,
            "max_favorable_30": float(row[17]) if row[17] is not None else None,
            "max_adverse_30": float(row[18]) if row[18] is not None else None,
            "days_to_max_favorable_30": int(row[19]) if row[19] is not None else None,
            "days_to_max_adverse_30": int(row[20]) if row[20] is not None else None,
            "date_of_max_favorable_30": int(row[21]) if row[21] is not None else None,
            "date_of_max_adverse_30": int(row[22]) if row[22] is not None else None,
            "status": str(row[23]),
            "break_status": str(row[24]) if row[24] is not None else None,
            "first_break_date": int(row[25]) if row[25] is not None else None,
            "firstBreakDate": _ymd_to_iso(int(row[25])) if row[25] is not None else None,
            "break_reason": str(row[26]) if row[26] is not None else None,
            "payload": _json_load(str(row[27]) if row[27] is not None else None),
        }
        bars = _fetch_code_bars(conn, code=appearance["code"], start_ymd=appearance["date"])
        by_date = _bar_price_lookup(bars)
        start_index = by_date.get(appearance["date"])
        price_series = []
        if start_index is not None:
            price_series = _compute_window_metrics(
                side=_ranking_dir_to_side(appearance["dir"]),
                bars=bars,
                start_index=start_index,
                anchor_close_price=appearance["anchor_price_close"],
                anchor_exec_price=appearance["anchor_price_next_open"],
                latest_market_ymd=latest_market_ymd,
            )["price_series"]
    runtime_stock_db_contract = _runtime_stock_db_status(
        requested_symbol=appearance["code"],
        requested_chart_date=appearance["date"],
        db_path=db_path,
    )
    chart_basis_fields = _build_chart_basis_detail_fields(runtime_stock_db_contract)
    artifact_source_last_date = runtime_stock_db_contract.get("latest_available_global_date")
    date_match_status = str(runtime_stock_db_contract.get("date_match_status") or "blocked")
    if date_match_status not in {"exact", "lagged_provisional", "blocked"}:
        date_match_status = "blocked"
    judgment_validity_status = date_match_status
    return {
        "appearance": appearance,
        "price_series": price_series,
        "requested_chart_date": appearance["date"],
        "requested_chart_date_iso": _ymd_to_iso(appearance["date"]),
        "artifact_source_last_date": artifact_source_last_date,
        "artifact_source_last_date_iso": _ymd_to_iso(int(artifact_source_last_date)) if isinstance(artifact_source_last_date, int) else None,
        "date_gap_days": runtime_stock_db_contract.get("date_gap_days"),
        "date_match_status": date_match_status,
        "judgment_validity_status": judgment_validity_status,
        "runtime_db_path": runtime_stock_db_contract.get("runtime_db_path"),
        "resolution_reason": runtime_stock_db_contract.get("resolution_reason"),
        "source_freshness_status": runtime_stock_db_contract.get("source_freshness_status"),
        "runtime_stock_db_contract": runtime_stock_db_contract,
        **chart_basis_fields,
    }


def get_ranking_history_summary(
    *,
    direction: str = "up",
    ranking_logic_version: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_dir = _normalize_ranking_dir(direction)
    with _open_conn(db_path) as conn:
        resolved_ranking_logic_version, _ = _resolve_ranking_logic_version(conn, ranking_logic_version)
    with _open_conn(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT status, current_directional_return, return_30d, break_status, signal_state_at_appearance, entry_qualified_at_appearance
            FROM ranking_appearance_daily
            WHERE dir = ? AND ranking_logic_version = ?
            """,
            [normalized_dir, resolved_ranking_logic_version],
        ).fetchall()
    counts = {"active": 0, "completed": 0, "archive": 0}
    active_values: list[float] = []
    completed_values: list[float] = []
    completed_wins = 0
    break_count = 0
    qualified_true = 0
    signal_present = 0
    for row in rows:
        status = str(row[0])
        counts[status] = counts.get(status, 0) + 1
        if row[1] is not None and status == "active":
            active_values.append(float(row[1]))
        if row[2] is not None and status == "completed":
            completed_values.append(float(row[2]))
            if float(row[2]) > 0:
                completed_wins += 1
        if str(row[3] or "") == "broken":
            break_count += 1
        if bool(row[5]):
            qualified_true += 1
        if str(row[4] or "wait") != "wait":
            signal_present += 1
    logic_versions = list_ranking_logic_versions(db_path=db_path)
    total = len(rows)
    return {
        "dir": normalized_dir,
        "ranking_logic_version": resolved_ranking_logic_version,
        "active_ranking_logic_version": logic_versions["active_ranking_logic_version"],
        "available_ranking_logic_versions": logic_versions["items"],
        "active_count": counts["active"],
        "completed_count": counts["completed"],
        "archive_count": counts["archive"],
        "active_average_directional_return": _safe_mean(active_values),
        "completed_win_rate": _safe_ratio(completed_wins, len(completed_values)),
        "break_rate": _safe_ratio(break_count, total),
        "entry_qualified_rate": _safe_ratio(qualified_true, total),
        "signal_present_rate": _safe_ratio(signal_present, total),
    }


def get_ranking_history_analysis(
    *,
    ranking_logic_version: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    with _open_conn(db_path) as conn:
        resolved_ranking_logic_version, _ = _resolve_ranking_logic_version(conn, ranking_logic_version)
    with _open_conn(db_path, read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT
                dt,
                dir,
                rank,
                signal_state_at_appearance,
                entry_qualified_at_appearance,
                return_30d,
                break_status,
                break_reason,
                days_to_max_favorable_30,
                days_to_max_adverse_30
            FROM ranking_appearance_daily
            WHERE ranking_logic_version = ?
            """,
            [resolved_ranking_logic_version],
        ).fetchall()
        observed_dates = [int(row[0]) for row in rows if row[0] is not None]
        regime_lookup = _load_market_regime_lookup(
            db_path=db_path,
            from_ymd=min(observed_dates) if observed_dates else None,
            to_ymd=max(observed_dates) if observed_dates else None,
        )
        baseline_lookup_up = _load_universe_baseline_map(
            conn,
            side="buy",
            from_ymd=min(observed_dates) if observed_dates else None,
            to_ymd=max(observed_dates) if observed_dates else None,
            horizon=30,
        )
        baseline_lookup_down = _load_universe_baseline_map(
            conn,
            side="sell",
            from_ymd=min(observed_dates) if observed_dates else None,
            to_ymd=max(observed_dates) if observed_dates else None,
            horizon=30,
        )
    def _bucket_for_rank(rank: int) -> str:
        if rank <= 5:
            return "1-5"
        if rank <= 10:
            return "6-10"
        if rank <= 20:
            return "11-20"
        return "21-50"
    by_dir: dict[str, list[dict[str, Any]]] = {"up": [], "down": []}
    by_bucket: dict[str, list[dict[str, Any]]] = {}
    by_entry_qualified: dict[str, list[dict[str, Any]]] = {"true": [], "false": []}
    by_signal_state: dict[str, list[dict[str, Any]]] = {}
    monthly_groups: dict[str, list[dict[str, Any]]] = {}
    regime_groups: dict[str, list[dict[str, Any]]] = {}
    break_reason_counts: dict[str, int] = {}
    all_peak_favorable_days: list[int] = []
    all_peak_adverse_days: list[int] = []
    for dt, direction, rank, signal_state, entry_qualified, return_30d, break_status, break_reason, days_to_max_favorable_30, days_to_max_adverse_30 in rows:
        dt_int = int(dt)
        if return_30d is not None:
            value = float(return_30d)
            peak_payload = {
                "return_30d": value,
                "days_to_max_favorable_30": int(days_to_max_favorable_30) if days_to_max_favorable_30 is not None else None,
                "days_to_max_adverse_30": int(days_to_max_adverse_30) if days_to_max_adverse_30 is not None else None,
            }
            by_dir.setdefault(str(direction), []).append(peak_payload)
            by_bucket.setdefault(_bucket_for_rank(int(rank)), []).append(peak_payload)
            by_entry_qualified["true" if bool(entry_qualified) else "false"].append(peak_payload)
            by_signal_state.setdefault(str(signal_state or "wait"), []).append(peak_payload)
            baseline_map = baseline_lookup_down if str(direction) == "down" else baseline_lookup_up
            payload = {
                "return_30d": value,
                "baseline_30": baseline_map.get(dt_int),
                "days_to_max_favorable_30": peak_payload["days_to_max_favorable_30"],
                "days_to_max_adverse_30": peak_payload["days_to_max_adverse_30"],
            }
            month_key = _month_key(dt_int)
            if month_key:
                monthly_groups.setdefault(month_key, []).append(payload)
            regime_key = regime_lookup.get(dt_int, "unclassified")
            regime_groups.setdefault(regime_key, []).append(payload)
        if days_to_max_favorable_30 is not None:
            all_peak_favorable_days.append(int(days_to_max_favorable_30))
        if days_to_max_adverse_30 is not None:
            all_peak_adverse_days.append(int(days_to_max_adverse_30))
        if str(break_status or "") == "broken":
            reason_key = str(break_reason or "unknown")
            break_reason_counts[reason_key] = break_reason_counts.get(reason_key, 0) + 1
    monthly_rows: list[dict[str, Any]] = []
    for month_key, values in sorted(monthly_groups.items(), key=lambda item: _month_key_sort_key(item[0])):
        returns = [float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]
        baselines = [float(item["baseline_30"]) for item in values if isinstance(item.get("baseline_30"), (int, float))]
        avg_return = _safe_mean(returns)
        avg_baseline = _safe_mean(baselines)
        monthly_rows.append(
            {
                "month": month_key,
                "count": len(values),
                "average_directional_return_30": avg_return,
                "directional_win_rate_30": _safe_ratio(sum(1 for value in returns if value > 0), len(returns)),
                "same_date_universe_average_directional_return_30": avg_baseline,
                "lift_vs_same_date_universe_30": None if avg_return is None or avg_baseline is None else float(avg_return - avg_baseline),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in values],
                    [item.get("days_to_max_adverse_30") for item in values],
                ),
            }
        )
    regime_rows: list[dict[str, Any]] = []
    for regime_key, values in sorted(regime_groups.items(), key=lambda item: (-len(item[1]), item[0])):
        returns = [float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]
        baselines = [float(item["baseline_30"]) for item in values if isinstance(item.get("baseline_30"), (int, float))]
        avg_return = _safe_mean(returns)
        avg_baseline = _safe_mean(baselines)
        regime_rows.append(
            {
                "regime": regime_key,
                "count": len(values),
                "average_directional_return_30": avg_return,
                "directional_win_rate_30": _safe_ratio(sum(1 for value in returns if value > 0), len(returns)),
                "same_date_universe_average_directional_return_30": avg_baseline,
                "lift_vs_same_date_universe_30": None if avg_return is None or avg_baseline is None else float(avg_return - avg_baseline),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in values],
                    [item.get("days_to_max_adverse_30") for item in values],
                ),
            }
        )
    all_returns = [float(row[5]) for row in rows if row[5] is not None]
    all_baselines = [
        float((baseline_lookup_down if str(row[1]) == "down" else baseline_lookup_up).get(int(row[0])))
        for row in rows
        if row[5] is not None and (baseline_lookup_down if str(row[1]) == "down" else baseline_lookup_up).get(int(row[0])) is not None
    ]
    return {
        "generated_at": _serialize_timestamp(datetime.now(timezone.utc)),
        "ranking_logic_version": resolved_ranking_logic_version,
        "by_dir": [
            {
                "dir": key,
                "count": len(values),
                "average_directional_return_30": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "directional_win_rate_30": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                "average_return_30d": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "win_rate_30d": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in values],
                    [item.get("days_to_max_adverse_30") for item in values],
                ),
            }
            for key, values in by_dir.items()
        ],
        "by_rank_bucket": [
            {
                "bucket": key,
                "count": len(values),
                "average_directional_return_30": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "directional_win_rate_30": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                "average_return_30d": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "win_rate_30d": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in values],
                    [item.get("days_to_max_adverse_30") for item in values],
                ),
            }
            for key, values in sorted(by_bucket.items())
        ],
        "by_entry_qualified": [
            {
                "entry_qualified": key == "true",
                "count": len(values),
                "average_directional_return_30": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "directional_win_rate_30": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                "average_return_30d": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "win_rate_30d": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in values],
                    [item.get("days_to_max_adverse_30") for item in values],
                ),
            }
            for key, values in by_entry_qualified.items()
        ],
        "by_signal_state": [
            {
                "signal_state": key,
                "count": len(values),
                "average_directional_return_30": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "directional_win_rate_30": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                "average_return_30d": _safe_mean([float(item["return_30d"]) for item in values if isinstance(item.get("return_30d"), (int, float))]),
                "win_rate_30d": _safe_ratio(
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float)) and float(item["return_30d"]) > 0),
                    sum(1 for item in values if isinstance(item.get("return_30d"), (int, float))),
                ),
                **_peak_day_metrics_dict(
                    [item.get("days_to_max_favorable_30") for item in values],
                    [item.get("days_to_max_adverse_30") for item in values],
                ),
            }
            for key, values in sorted(by_signal_state.items())
        ],
        "monthly": monthly_rows,
        "rolling_6m": _rolling_average_series(
            monthly_rows,
            key="month",
            value_key="average_directional_return_30",
            window=6,
        ),
        "by_regime": regime_rows,
        "peak_day_buckets": _build_peak_day_buckets(all_peak_favorable_days),
        "adverse_peak_day_buckets": _build_peak_day_buckets(all_peak_adverse_days),
        "median_days_to_max_favorable_30": _safe_median(all_peak_favorable_days),
        "median_days_to_max_adverse_30": _safe_median(all_peak_adverse_days),
        "same_date_universe_average_directional_return_30": _safe_mean(all_baselines),
        "lift_vs_same_date_universe_30": None
        if _safe_mean(all_returns) is None or _safe_mean(all_baselines) is None
        else float(_safe_mean(all_returns) - _safe_mean(all_baselines)),
        "break_reason_counts": [
            {"break_reason": key, "count": value}
            for key, value in sorted(break_reason_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
    }


def _compute_latest_signal_parity(
    *,
    db_path: str | None,
    logic_version: str,
    basis_version: str,
) -> dict[str, Any]:
    with _open_conn(db_path, read_only=True) as conn:
        latest_row = conn.execute(
            """
            SELECT MAX(dt)
            FROM signal_decision_daily
            WHERE logic_version = ?
            """,
            [str(logic_version)],
        ).fetchone()
        latest_dt = int(latest_row[0]) if latest_row and latest_row[0] is not None else None
        if latest_dt is None:
            return {"available": False, "reason": "signal_decision_daily is empty for requested logic_version"}
        items, _buy_rank, _sell_rank = _load_basis_items_for_date(conn, dt=latest_dt, basis_version=basis_version)
        if not items:
            return {"available": False, "reason": "signal_basis_daily is empty for latest decision date"}
        per_side: list[dict[str, Any]] = []
        mismatch_samples: list[dict[str, Any]] = []
        for target_side in SUPPORTED_SIDES:
            expected_all, _expected_ranked, pred_dt, model_version = _evaluate_trade_items_from_basis(
                items=items,
                as_of_int=latest_dt,
                side=target_side,
            )
            actual_rows = conn.execute(
                """
                SELECT code, entry_qualified, setup_type
                FROM signal_decision_daily
                WHERE dt = ? AND logic_version = ? AND side = ?
                """,
                [int(latest_dt), str(logic_version), str(target_side)],
            ).fetchall()
            actual_map = {
                str(code): {
                    "entry_qualified": bool(entry_qualified),
                    "setup_type": str(setup_type or "").strip() or None,
                }
                for code, entry_qualified, setup_type in actual_rows
            }
            matched_qualified = 0
            matched_setup = 0
            compared = 0
            for item in expected_all:
                code = str(item.get("code") or "").strip()
                if not code:
                    continue
                compared += 1
                expected_qualified = bool(item.get("entryQualified") is True)
                expected_setup = str(item.get("setupType") or "").strip() or None
                actual = actual_map.get(code)
                actual_qualified = bool((actual or {}).get("entry_qualified"))
                actual_setup = (actual or {}).get("setup_type")
                if actual and actual_qualified == expected_qualified:
                    matched_qualified += 1
                if actual and actual_setup == expected_setup:
                    matched_setup += 1
                if actual and (actual_qualified != expected_qualified or actual_setup != expected_setup) and len(mismatch_samples) < 10:
                    mismatch_samples.append(
                        {
                            "dt": latest_dt,
                            "side": target_side,
                            "code": code,
                            "expected_entry_qualified": expected_qualified,
                            "actual_entry_qualified": actual_qualified,
                            "expected_setup_type": expected_setup,
                            "actual_setup_type": actual_setup,
                        }
                    )
            per_side.append(
                {
                    "side": target_side,
                    "dt": latest_dt,
                    "compared_codes": compared,
                    "qualified_match_rate": _safe_ratio(matched_qualified, compared),
                    "setup_match_rate": _safe_ratio(matched_setup, compared),
                    "pred_dt": pred_dt,
                    "model_version": model_version,
                }
            )
    return {
        "available": True,
        "dt": latest_dt,
        "per_side": per_side,
        "mismatch_samples": mismatch_samples,
    }


def _load_label_policy_audit() -> dict[str, Any]:
    try:
        from external_analysis.contracts.paths import resolve_label_db_path
    except Exception as exc:
        return {"available": False, "reason": f"failed to resolve label db path: {exc}"}
    label_db_path = resolve_label_db_path()
    if not Path(label_db_path).exists():
        return {"available": False, "path": str(label_db_path), "reason": "label db not found"}
    conn = duckdb.connect(str(label_db_path), read_only=True)
    try:
        if not _table_exists(conn, "label_generation_runs"):
            return {"available": False, "path": str(label_db_path), "reason": "label schema missing"}
        table_summaries: list[dict[str, Any]] = []
        for horizon in DECISION_HORIZONS:
            table_name = f"label_daily_h{int(horizon)}"
            if not _table_exists(conn, table_name):
                continue
            row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN policy_version IS NULL OR TRIM(policy_version) = '' THEN 1 ELSE 0 END) AS missing_policy_version,
                    SUM(CASE WHEN leakage_group_id IS NULL OR TRIM(leakage_group_id) = '' THEN 1 ELSE 0 END) AS missing_leakage_group_id,
                    SUM(CASE WHEN purge_end_date IS NULL THEN 1 ELSE 0 END) AS missing_purge_end_date,
                    SUM(CASE WHEN embargo_until_date IS NULL THEN 1 ELSE 0 END) AS missing_embargo_until_date
                FROM {table_name}
                """
            ).fetchone()
            policy_versions = [
                str(value[0])
                for value in conn.execute(f"SELECT DISTINCT policy_version FROM {table_name} ORDER BY policy_version").fetchall()
                if value and value[0] is not None
            ]
            table_summaries.append(
                {
                    "table": table_name,
                    "horizon": int(horizon),
                    "total_rows": int(row[0] or 0),
                    "missing_policy_version": int(row[1] or 0),
                    "missing_leakage_group_id": int(row[2] or 0),
                    "missing_purge_end_date": int(row[3] or 0),
                    "missing_embargo_until_date": int(row[4] or 0),
                    "policy_versions": policy_versions,
                }
            )
        latest_run = conn.execute(
            """
            SELECT run_id, started_at, finished_at, policy_version
            FROM label_generation_runs
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        return {
            "available": True,
            "path": str(label_db_path),
            "tables": table_summaries,
            "latest_run": {
                "run_id": None if latest_run is None else str(latest_run[0]),
                "started_at": None if latest_run is None else _serialize_timestamp(latest_run[1]),
                "completed_at": None if latest_run is None else _serialize_timestamp(latest_run[2]),
                "dependency_version": None if latest_run is None else str(latest_run[3]),
            },
        }
    finally:
        conn.close()


def _load_external_replay_audit(*, logic_version: str, db_path: str | None) -> dict[str, Any]:
    try:
        from external_analysis.contracts.paths import resolve_ops_db_path, resolve_result_db_path
    except Exception as exc:
        return {"available": False, "reason": f"failed to resolve external analysis paths: {exc}"}
    ops_db_path = resolve_ops_db_path()
    result_db_path = resolve_result_db_path()
    if not Path(ops_db_path).exists():
        return {"available": False, "ops_db_path": str(ops_db_path), "reason": "ops db not found"}
    ops_conn = duckdb.connect(str(ops_db_path), read_only=True)
    try:
        if not _table_exists(ops_conn, "external_replay_runs") or not _table_exists(ops_conn, "external_replay_days"):
            return {"available": False, "ops_db_path": str(ops_db_path), "reason": "replay tables missing"}
        latest_replay = ops_conn.execute(
            """
            SELECT replay_id, status, start_as_of_date, end_as_of_date, finished_at
            FROM external_replay_runs
            ORDER BY COALESCE(finished_at, started_at, created_at) DESC
            LIMIT 1
            """
        ).fetchone()
        latest_day = ops_conn.execute(
            """
            SELECT replay_id, CAST(as_of_date AS VARCHAR), publish_id, status
            FROM external_replay_days
            WHERE status = 'success'
            ORDER BY COALESCE(finished_at, started_at) DESC, as_of_date DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        ops_conn.close()
    candidate_summary: dict[str, Any] | None = None
    if latest_day and Path(result_db_path).exists():
        result_conn = duckdb.connect(str(result_db_path), read_only=True)
        try:
            if _table_exists(result_conn, "candidate_daily"):
                publish_id = str(latest_day[2] or "")
                rows = result_conn.execute(
                    """
                    SELECT code, side, rank_position
                    FROM candidate_daily
                    WHERE publish_id = ?
                    ORDER BY side, rank_position
                    LIMIT 20
                    """,
                    [publish_id],
                ).fetchall()
                as_of_day = _coerce_ymd(str(latest_day[1]).replace("-", "")) if latest_day[1] is not None else None
                signal_matches = 0
                side_counts: dict[str, dict[str, int]] = {
                    "buy": {"candidates": 0, "qualified_matches": 0},
                    "sell": {"candidates": 0, "qualified_matches": 0},
                }
                with _open_conn(db_path, read_only=True) as source_conn:
                    for code, candidate_side, _rank_position in rows:
                        normalized_candidate_side = _normalize_external_candidate_side(candidate_side)
                        if normalized_candidate_side is None:
                            continue
                        side_counts[normalized_candidate_side]["candidates"] += 1
                        found = source_conn.execute(
                            """
                            SELECT entry_qualified
                            FROM signal_decision_daily
                            WHERE dt = ? AND code = ? AND side = ? AND logic_version = ?
                            """,
                            [int(as_of_day or 0), str(code), str(normalized_candidate_side), str(logic_version)],
                        ).fetchone()
                        if found and bool(found[0]):
                            signal_matches += 1
                            side_counts[normalized_candidate_side]["qualified_matches"] += 1
                candidate_summary = {
                    "publish_id": publish_id,
                    "as_of_date": latest_day[1],
                    "candidate_count_sampled": len(rows),
                    "qualified_overlap_rate": _safe_ratio(signal_matches, len(rows)),
                    "per_side": [
                        {
                            "side": side_name,
                            "candidate_count": payload["candidates"],
                            "qualified_overlap_rate": _safe_ratio(payload["qualified_matches"], payload["candidates"]),
                        }
                        for side_name, payload in side_counts.items()
                        if payload["candidates"] > 0
                    ],
                    "setup_parity_available": False,
                }
        finally:
            result_conn.close()
    return {
        "available": True,
        "ops_db_path": str(ops_db_path),
        "result_db_path": str(result_db_path),
        "latest_replay": None
        if latest_replay is None
        else {
            "replay_id": str(latest_replay[0]),
            "status": str(latest_replay[1]),
            "start_as_of_date": str(latest_replay[2]),
            "end_as_of_date": str(latest_replay[3]),
            "finished_at": _serialize_timestamp(latest_replay[4]),
        },
        "latest_success_day": None
        if latest_day is None
        else {
            "replay_id": str(latest_day[0]),
            "as_of_date": str(latest_day[1]),
            "publish_id": None if latest_day[2] is None else str(latest_day[2]),
            "status": str(latest_day[3]),
        },
        "sample_parity": candidate_summary,
    }


def get_signal_tracking_leakage_audit(
    *,
    side: str = "buy",
    logic_version: str | None = None,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_side = _normalize_side(side)
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    with _open_conn(db_path) as conn:
        resolved_logic_version, basis_version = _resolve_logic_version(conn, logic_version)
    resolved_basis_version = str(basis_version or DEFAULT_BASIS_VERSION)
    where_parts = ["basis_version = ?"]
    params: list[Any] = [resolved_basis_version]
    if from_int is not None:
        where_parts.append("dt >= ?")
        params.append(int(from_int))
    if to_int is not None:
        where_parts.append("dt <= ?")
        params.append(int(to_int))
    with _open_conn(db_path, read_only=True) as conn:
        basis_rows = conn.execute(
            f"""
            SELECT
                dt,
                code,
                source_as_of,
                pred_dt,
                model_version,
                basis_source,
                source_hash,
                payload_schema_version,
                basis_payload_json
            FROM signal_basis_daily
            WHERE {" AND ".join(where_parts)}
            ORDER BY dt, code
            """,
            params,
        ).fetchall()
    total_rows = len(basis_rows)
    future_source_as_of_count = 0
    future_pred_dt_count = 0
    missing_source_as_of_count = 0
    missing_model_version_count = 0
    missing_basis_source_count = 0
    missing_source_hash_count = 0
    missing_payload_schema_version_count = 0
    prohibited_payload_count = 0
    excluded_from_validation_count = 0
    violation_samples: list[dict[str, Any]] = []
    for dt, code, source_as_of, pred_dt, model_version, basis_source, source_hash, payload_schema_version, payload_json in basis_rows:
        payload = _json_load(str(payload_json) if payload_json is not None else None) or {}
        prohibited_paths = _find_prohibited_basis_paths(payload)
        if prohibited_paths:
            prohibited_payload_count += 1
            if len(violation_samples) < 10:
                violation_samples.append(
                    {
                        "dt": int(dt),
                        "code": str(code),
                        "type": "prohibited_payload_fields",
                        "paths": prohibited_paths[:5],
                    }
                )
        if source_as_of is None:
            missing_source_as_of_count += 1
            excluded_from_validation_count += 1
        elif int(source_as_of) > int(dt):
            future_source_as_of_count += 1
            if len(violation_samples) < 10:
                violation_samples.append(
                    {
                        "dt": int(dt),
                        "code": str(code),
                        "type": "source_as_of_gt_dt",
                        "source_as_of": int(source_as_of),
                    }
                )
        if pred_dt is not None and int(pred_dt) > int(dt):
            future_pred_dt_count += 1
            if len(violation_samples) < 10:
                violation_samples.append(
                    {
                        "dt": int(dt),
                        "code": str(code),
                        "type": "pred_dt_gt_dt",
                        "pred_dt": int(pred_dt),
                    }
                )
        if not str(model_version or "").strip():
            missing_model_version_count += 1
        if not str(basis_source or "").strip():
            missing_basis_source_count += 1
        if not str(source_hash or "").strip():
            missing_source_hash_count += 1
        if not str(payload_schema_version or "").strip():
            missing_payload_schema_version_count += 1
    return {
        "generated_at": _serialize_timestamp(datetime.now(timezone.utc)),
        "side": normalized_side,
        "logic_version": resolved_logic_version,
        "basis_version": resolved_basis_version,
        "basis_provenance": {
            "total_rows": total_rows,
            "missing_source_as_of_count": missing_source_as_of_count,
            "missing_model_version_count": missing_model_version_count,
            "missing_basis_source_count": missing_basis_source_count,
            "missing_source_hash_count": missing_source_hash_count,
            "missing_payload_schema_version_count": missing_payload_schema_version_count,
            "future_source_as_of_count": future_source_as_of_count,
            "future_pred_dt_count": future_pred_dt_count,
            "prohibited_payload_count": prohibited_payload_count,
            "excluded_from_validation_count": excluded_from_validation_count,
        },
        "latest_signal_parity": _compute_latest_signal_parity(
            db_path=db_path,
            logic_version=resolved_logic_version,
            basis_version=resolved_basis_version,
        ),
        "label_policy_audit": _load_label_policy_audit(),
        "external_replay_audit": _load_external_replay_audit(
            logic_version=resolved_logic_version,
            db_path=db_path,
        ),
        "violation_samples": violation_samples,
    }


def backfill_signal_tracking(
    *,
    from_ymd: int | str | None = None,
    to_ymd: int | str | None = None,
    lookback_days: int = DEFAULT_BACKFILL_LOOKBACK_DAYS,
    logic_version: str | None = None,
    side: str = "all",
    basis_version: str = DEFAULT_BASIS_VERSION,
    reset_scope: bool = False,
    db_path: str | None = None,
    progress_cb: TrackingProgressCallback | None = None,
) -> dict[str, Any]:
    from_int = _coerce_ymd(from_ymd)
    to_int = _coerce_ymd(to_ymd)
    if from_int is None or to_int is None:
        with _open_conn(db_path, read_only=True) as conn:
            market_dates = _list_market_dates(conn)
        if market_dates:
            if to_int is None:
                to_int = market_dates[-1]
            if from_int is None:
                lookback = max(20, int(lookback_days or DEFAULT_BACKFILL_LOOKBACK_DAYS))
                from_int = market_dates[max(0, len(market_dates) - lookback)]
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="prepare",
            status="start",
            processed=0,
            total=4,
            current_market_ymd=to_int,
            current_market_date=_ymd_to_iso(to_int) if to_int is not None else None,
            detail=f"backfill window={_ymd_to_iso(from_int) if from_int is not None else None}..{_ymd_to_iso(to_int) if to_int is not None else None}",
        ),
        force=True,
    )
    basis_result = backfill_signal_basis(
        from_ymd=from_int,
        to_ymd=to_int,
        basis_version=basis_version,
        reset_scope=reset_scope,
        db_path=db_path,
        progress_cb=progress_cb,
    )
    decision_result = rebuild_signal_decisions(
        from_ymd=from_int,
        to_ymd=to_int,
        logic_version=logic_version,
        side=side,
        basis_version=basis_version,
        reset_scope=reset_scope,
        db_path=db_path,
        progress_cb=progress_cb,
    )
    campaign_result = rebuild_signal_campaigns(
        logic_version=logic_version,
        side=side,
        db_path=db_path,
        progress_cb=progress_cb,
    )
    ranking_result = rebuild_ranking_appearances(
        from_ymd=from_int,
        to_ymd=to_int,
        ranking_logic_version=ACTIVE_RANKING_LOGIC_VERSION_ALIAS,
        signal_logic_version=logic_version,
        basis_version=basis_version,
        reset_scope=reset_scope,
        db_path=db_path,
        progress_cb=progress_cb,
    )
    _emit_tracking_progress(
        progress_cb,
        _tracking_progress_event(
            phase="prepare",
            status="done",
            processed=4,
            total=4,
            current_market_ymd=to_int,
            current_market_date=_ymd_to_iso(to_int) if to_int is not None else None,
            detail="backfill completed",
        ),
        force=True,
    )
    return {
        "ok": True,
        "basis": basis_result,
        "decisions": decision_result,
        "campaigns": campaign_result,
        "ranking": ranking_result,
    }
