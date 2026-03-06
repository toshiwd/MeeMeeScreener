from __future__ import annotations

from collections import OrderedDict
import csv
from datetime import datetime, timedelta, timezone
import json
import math
import logging
import os
import time
from pathlib import Path
from threading import Condition, Lock
from typing import Any, Literal
from zoneinfo import ZoneInfo

import duckdb

from app.core.config import config as core_config
from app.db.session import get_conn, is_transient_duckdb_error
from app.backend.core.text_encoding import repair_cp932_mojibake
from app.backend.domain.screening.metrics import _calc_liquidity_20d
from app.backend.services.edinet_rank_features import load_edinet_rank_features
from app.backend.services.bar_aggregation import merge_monthly_rows_with_daily
from app.backend.services.jpx_calendar import get_intraday_refresh_end_minute, get_jpx_session_info
from app.backend.services.ml_config import load_ml_config
from app.backend.services.ml_service import select_top_n_ml
from app.backend.services.ranking_analysis_quality import get_latest_prob_up_gates
from app.backend.services import swing_plan_service
from app.backend.services.yahoo_provisional import (
    get_provisional_daily_rows_from_spark,
    merge_daily_rows_with_provisional,
    normalize_date_key,
)

RankTimeframe = Literal["D", "W", "M"]
RankWhich = Literal["latest", "prev"]
RankDir = Literal["up", "down"]
RankMode = Literal["rule", "ml", "hybrid", "turn"]
RankRiskMode = Literal["defensive", "balanced", "aggressive"]

_CACHE: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
_LAST_UPDATED: datetime | None = None
_LAST_DB_MTIME: float | None = None
_LAST_CACHE_DAILY_ASOF_INT: int | None = None
_LAST_CACHE_PAN_DAILY_ASOF_INT: int | None = None
_LOCK = Lock()
_REFRESH_COND = Condition(_LOCK)
_REFRESH_IN_PROGRESS = False
_REFRESH_LAST_ERROR: Exception | None = None
logger = logging.getLogger(__name__)
_DAILY_PROB_CALIB_CACHE: dict[tuple[int, RankDir], dict[str, Any]] = {}
_DAILY_PROB_CALIB_CACHE_LOCK = Lock()
_ASOF_BASE_CACHE_LOCK = Lock()
_ASOF_BASE_CACHE: OrderedDict[int, dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]]] = OrderedDict()
_ASOF_BASE_CACHE_MAX = max(8, int(os.getenv("MEEMEE_RANK_ASOF_BASE_CACHE_MAX", "32")))
_TRACE_CACHE_LOCK = Lock()
_TRACE_CACHE: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
_TRACE_CACHE_MAX = 16
_YF_PROVISIONAL_RANK_REFRESH_SEC = max(30, int(os.getenv("MEEMEE_YF_PROVISIONAL_RANK_REFRESH_SEC", "300")))
_JST = ZoneInfo("Asia/Tokyo")
_INTRADAY_REFRESH_START_MIN = 9 * 60
_INTRADAY_REFRESH_END_MIN = 15 * 60 + 40

_DAILY_LIMIT = 1260
_MONTHLY_LIMIT = 60
_ENTRY_MIN_EV_NET_UP = 0.003
_ENTRY_MAX_EV_NET_DOWN = 0.005
_ENTRY_MAX_DIST_MA20 = 0.12
_ENTRY_MIN_PROB_DOWN_STRICT = 0.56
_ENTRY_MIN_RULE_SIGNAL_DOWN = 0.002
_ENTRY_MAX_COUNTER_MOVE_DOWN = 0.01
# Current model outputs are concentrated around 0.47-0.60 for p_up_5.
# A hard 0.70 gate eliminates all symbols.
_ENTRY_MIN_PROB_UP_5D = 0.58
_ENTRY_PROB_CURVE_EPS = 0.02
_ENTRY_CANDLE_PATTERN_WEIGHTS_UP: tuple[tuple[str, float], ...] = (
    ("shootingStarLike", 0.01),
    ("threeWhiteSoldiers", 0.01),
    ("bullEngulfing", 0.01),
    # Off by default. Promote only after walk-forward verification.
    ("morningStar", 0.0),
    ("threeBlackCrows", 0.0),
)
_ENTRY_CANDLE_PATTERN_COMBO_WEIGHTS_UP: tuple[tuple[str, str, str, float], ...] = (
    # Experimental, regime-conditioned combo validated only under risk-off breadth.
    ("bearMarubozu+threeBlackCrows@riskOff", "bearMarubozu", "threeBlackCrows", 0.003),
)
_MARKET_BREADTH_RISK_ON_MIN_ADV = 0.55
_MARKET_BREADTH_RISK_OFF_MAX_ADV = 0.45
_ENTRY_BONUS_BOX_BOTTOM = 0.03
_ENTRY_BONUS_MTF_SYNERGY = 0.02
_ENTRY_BONUS_STRICT_STACK = 0.02
_ENTRY_BONUS_MA_STREAK_BALANCED = 0.015
_ENTRY_BONUS_BREAKOUT_STACK_STREAK = 0.02
_ENTRY_BONUS_PATTERN_A1_MATURED_BREAKOUT = 0.025
_ENTRY_BONUS_PATTERN_A2_BOX_TREND = 0.012
_ENTRY_BONUS_PATTERN_A3_CAPITULATION_REBOUND = 0.01
_ENTRY_PENALTY_60V_STRONG = 0.01
_ENTRY_PENALTY_WEAK_EARLY_STREAK = 0.03
_ENTRY_PENALTY_BOX_BOTTOM_WEAK = 0.02
_ENTRY_PENALTY_PATTERN_S1_WEAK_BREAKDOWN = 0.03
_ENTRY_PENALTY_PATTERN_S2_WEAK_BOX = 0.02
_ENTRY_PENALTY_PATTERN_S3_LATE_BREAKOUT = 0.02
_ENTRY_BONUS_PATTERN_D1_SHORT_BREAKDOWN = 0.02
_ENTRY_BONUS_PATTERN_D2_SHORT_MIXED_FAR = 0.015
_ENTRY_BONUS_PATTERN_D3_SHORT_NA_BELOW = 0.01
_ENTRY_BONUS_PATTERN_D4_SHORT_DOUBLE_TOP = 0.018
_ENTRY_BONUS_PATTERN_D5_SHORT_HEAD_SHOULDERS = 0.02
_ENTRY_PENALTY_PATTERN_DTRAP_STACKDOWN_FAR = 0.025
_ENTRY_PENALTY_PATTERN_DTRAP_OVERHEAT_MOMENTUM = 0.03
_ENTRY_PENALTY_PATTERN_DTRAP_TOP_FAKEOUT = 0.025
_MONTHLY_ABS_GATE_DEFAULT = 0.30
_MONTHLY_SIDE_GATE_DEFAULT = 0.30
_MONTHLY_ABS_GATE_MIN = 0.15
_MONTHLY_SIDE_GATE_MIN = 0.10
_MONTHLY_GATE_MIN_CANDIDATES = 5
_MONTHLY_ABS_RELAX_STEPS: tuple[float, ...] = (0.35, 0.32, 0.30, 0.28, 0.25, 0.22, 0.20, 0.18, 0.15)
_MONTHLY_SIDE_RELAX_STEPS: tuple[float, ...] = (0.30, 0.25, 0.22, 0.20, 0.18, 0.16, 0.14, 0.12, 0.10)
_MONTHLY_REGIME_BONUS = 0.04
_MONTHLY_RANGE_PENALTY = 0.03
_MONTHLY_TARGET20_GATE_MIN_UP = 0.11
_MONTHLY_TARGET20_GATE_MIN_DOWN = 0.08
_DAILY_PROB_CALIB_LOOKBACK_DAYS = 540
_DAILY_PROB_CALIB_MIN_SAMPLES = 300
_DAILY_PROB_CALIB_MIN_BIN_SAMPLES = 24
_DAILY_PROB_CALIB_MAX_BINS = 10
_DAILY_SCORE_RULE_WEIGHT = 0.45
_DAILY_SCORE_EV_WEIGHT = 0.20
_DAILY_SCORE_PROB_WEIGHT = 0.35
_DAILY_RISK_WEIGHT = 0.08
_DAILY_REV_RISK_PENALTY_WEIGHT = 0.08
_DAILY_TAIL_RISK_PENALTY_WEIGHT = 0.04
_DAILY_ENTRY_SCORE_GATE_STRICT = 0.85
_DAILY_FALLBACK_HYBRID_SCORE_GATE_UP = 0.79
_DAILY_FALLBACK_HYBRID_SCORE_GATE_DOWN = 0.80
_DAILY_FALLBACK_TURN_SCORE_GATE_UP = 0.76
_DAILY_FALLBACK_TURN_SCORE_GATE_DOWN = 0.78
_DAILY_RULE_GATE_MIN_PROB = 0.53
_DAILY_RULE_GATE_MIN_BREAKOUT = 0.52
_DAILY_RULE_GATE_MIN_ENTRY_SCORE = 0.48
_MONTHLY_PRED_REPAIR_COOLDOWN_SEC = 300
_MONTHLY_PRED_REPAIR_LAST_ATTEMPT: datetime | None = None
_ENTRY_POLICY_VERSION = "2026-02-27"
_ENTRY_POLICY_DELTA_LONG_BOX_EXIT = -0.0012
_ENTRY_POLICY_DELTA_SHORT_BOX_DOTEN_OPT = 0.0003
_ENTRY_BONUS_PLAYBOOK_LONG_STRONG = 0.01
_ENTRY_BONUS_PLAYBOOK_LONG_REBOUND = 0.004
_ENTRY_BONUS_PLAYBOOK_SHORT_STRONG = 0.012
_ENTRY_PENALTY_PLAYBOOK_TRAP = 0.015
_ENTRY_SHORT_MIN_PROB_DEFENSIVE = 0.60
_ENTRY_SHORT_MIN_PROB_BALANCED = 0.55
_ENTRY_SHORT_MIN_PROB_AGGRESSIVE = 0.57
_ENTRY_SHORT_MIN_TURN_DEFENSIVE = 0.62
_ENTRY_SHORT_MIN_TURN_BALANCED = 0.59
_ENTRY_SHORT_MIN_TURN_AGGRESSIVE = 0.60
_ENTRY_SHORT_OVERHEAT_DIST = 0.03
_ENTRY_SHORT_OVERHEAT_STRONG_PROB = 0.65
_ENTRY_SHORT_OVERHEAT_STRONG_TURN = 0.63
_ENTRY_SHORT_PRESSURE_SCORE_DEFENSIVE = 0.86
_ENTRY_SHORT_PRESSURE_SCORE_BALANCED = 0.80
_ENTRY_SHORT_PRESSURE_SCORE_AGGRESSIVE = 0.80
_ENTRY_SHORT_PRESSURE_PROB_EXTRA = 0.02
_ENTRY_SHORT_PRESSURE_MAX_EV_DEFENSIVE = -0.004
_ENTRY_SHORT_PRESSURE_MAX_EV_BALANCED = -0.0005
_ENTRY_SHORT_PRESSURE_MAX_EV_AGGRESSIVE = -0.001
_RESEARCH_PRIOR_TTL_SEC = 300
_RESEARCH_PRIOR_BONUS_UP = 0.015
_RESEARCH_PRIOR_BONUS_DOWN = 0.025
_EDINET_SCORE_BONUS_SCALE = 0.06
_EDINET_MONITOR_MIN_SAMPLES = 20
_RESEARCH_PRIOR_CACHE_LOCK = Lock()
_RESEARCH_PRIOR_CACHE: dict[str, Any] = {"loaded_at": None, "payload": None}
_MONTHLY_EDINET_AUDIT_PERSIST_COOLDOWN_SEC = max(
    30,
    int(os.getenv("MEEMEE_EDINET_AUDIT_PERSIST_COOLDOWN_SEC", "300")),
)
_MONTHLY_EDINET_AUDIT_REALIZED_REFRESH_COOLDOWN_SEC = max(
    60,
    int(os.getenv("MEEMEE_EDINET_AUDIT_REALIZED_REFRESH_COOLDOWN_SEC", "900")),
)
_MONTHLY_EDINET_AUDIT_LOCK = Lock()
_MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO: dict[tuple[int, str, str, str, str, str], float] = {}
_MONTHLY_EDINET_AUDIT_LAST_REALIZED_REFRESH_MONO = 0.0
_EDINET_ITEM_DEFAULTS: dict[str, Any] = {
    "edinetStatus": None,
    "edinetMapped": None,
    "edinetFreshnessDays": None,
    "edinetMetricCount": None,
    "edinetQualityScore": None,
    "edinetDataScore": None,
    "edinetScoreBonus": 0.0,
    "edinetFeatureFlagApplied": None,
    "edinetEbitdaMetric": None,
    "edinetRoe": None,
    "edinetEquityRatio": None,
    "edinetDebtRatio": None,
    "edinetOperatingCfMargin": None,
    "edinetRevenueGrowthYoy": None,
}


def _parse_date_value(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw >= 1_000_000_000:
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    raw_str = str(raw).zfill(8)
    if len(raw_str) == 8:
        try:
            year = int(raw_str[:4])
            month = int(raw_str[4:6])
            day = int(raw_str[6:8])
            return datetime(year, month, day)
        except ValueError:
            return None
    if len(raw_str) == 6:
        try:
            year = int(raw_str[:4])
            month = int(raw_str[4:6])
            return datetime(year, month, 1)
        except ValueError:
            return None
    return None


def _format_date(value: datetime | None) -> str | None:
    if not value:
        return None
    return value.date().isoformat()


def _iso_date_to_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        raw = value.replace("-", "")
        if len(raw) != 8 or not raw.isdigit():
            return None
        return int(raw)
    except Exception:
        return None


def _ymd_int_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    text = str(int(value)).zfill(8)
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    except Exception:
        return None


def _coerce_as_of_int(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        if 19_000_101 <= value <= 21_001_231:
            return int(value)
        if value >= 1_000_000_000:
            try:
                return int(datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return None
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return _coerce_as_of_int(int(text))
    return _iso_date_to_int(text)


def _is_edinet_bonus_enabled() -> bool:
    return str(os.getenv("MEEMEE_RANK_EDINET_BONUS_ENABLED", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _apply_edinet_defaults(item: dict, *, flag_applied: bool) -> dict:
    for key, default_value in _EDINET_ITEM_DEFAULTS.items():
        item.setdefault(key, default_value)
    item["edinetFeatureFlagApplied"] = bool(flag_applied)
    if not isinstance(item.get("edinetScoreBonus"), (int, float)):
        item["edinetScoreBonus"] = 0.0
    elif not math.isfinite(float(item.get("edinetScoreBonus"))):
        item["edinetScoreBonus"] = 0.0
    else:
        item["edinetScoreBonus"] = float(item["edinetScoreBonus"])
    return item


def _get_asof_base_cache(as_of_int: int) -> dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]]:
    with _ASOF_BASE_CACHE_LOCK:
        cached = _ASOF_BASE_CACHE.get(as_of_int)
        if cached is not None:
            _ASOF_BASE_CACHE.move_to_end(as_of_int)
            return cached

    with get_conn() as conn:
        built = _build_cache_asof(conn, as_of_int)

    with _ASOF_BASE_CACHE_LOCK:
        _ASOF_BASE_CACHE[as_of_int] = built
        _ASOF_BASE_CACHE.move_to_end(as_of_int)
        while len(_ASOF_BASE_CACHE) > _ASOF_BASE_CACHE_MAX:
            _ASOF_BASE_CACHE.popitem(last=False)
    return built


def _as_of_int_to_utc_epoch(value: int) -> int:
    year = value // 10_000
    month = (value // 100) % 100
    day = value % 100
    dt = datetime(year, month, day, tzinfo=timezone.utc)
    return int(dt.timestamp())


# Alias: identical implementation to _as_of_int_to_utc_epoch.
_as_of_month_int_to_utc_epoch = _as_of_int_to_utc_epoch


def _db_mtime() -> float | None:
    try:
        return os.path.getmtime(str(core_config.DB_PATH))
    except OSError:
        return None


def _is_jpx_intraday_window(now_utc: datetime | None = None) -> bool:
    now = now_utc if isinstance(now_utc, datetime) else datetime.now(timezone.utc)
    now_jst = now.astimezone(_JST)
    session = get_jpx_session_info(now_jst)
    if not session.is_trading_day:
        return False
    now_minutes = now_jst.hour * 60 + now_jst.minute
    refresh_end_min = min(_INTRADAY_REFRESH_END_MIN, get_intraday_refresh_end_minute(now_jst))
    return _INTRADAY_REFRESH_START_MIN <= now_minutes <= refresh_end_min


def _should_intraday_provisional_timer_refresh() -> bool:
    if not _is_jpx_intraday_window():
        return False
    today_jst = int(datetime.now(_JST).strftime("%Y%m%d"))
    latest_pan_cached = _LAST_CACHE_PAN_DAILY_ASOF_INT
    # Once PAN close data for today is in cache, age-based provisional refresh is unnecessary.
    if latest_pan_cached is not None and latest_pan_cached >= today_jst:
        return False
    return True


def _resolve_latest_daily_asof_int(cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]]) -> int | None:
    latest: int | None = None
    for which in ("latest", "prev"):
        for direction in ("up", "down"):
            candidates = cache.get(("D", which, direction)) or []
            for item in candidates:
                as_of_int = _iso_date_to_int(str(item.get("asOf") or ""))
                if as_of_int is None:
                    continue
                if latest is None or as_of_int > latest:
                    latest = as_of_int
    return latest


def _resolve_latest_pan_daily_asof_int(conn: duckdb.DuckDBPyConnection) -> int | None:
    try:
        row = conn.execute(
            """
            SELECT
                MAX(
                    CASE
                        WHEN date BETWEEN 19000101 AND 20991231 THEN date
                        WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                        WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                        ELSE NULL
                    END
                ) AS max_pan_ymd
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
            """
        ).fetchone()
    except Exception as exc:
        logger.debug("failed to resolve latest PAN asOf from daily_bars: %s", exc)
        return None
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _cache_needs_refresh(db_mtime: float | None) -> bool:
    if not _CACHE or _LAST_UPDATED is None:
        return True
    yf_enabled = str(os.getenv("MEEMEE_YF_PROVISIONAL_ENABLED", "1")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if yf_enabled and _LAST_UPDATED is not None:
        age_sec = (datetime.now(timezone.utc) - _LAST_UPDATED).total_seconds()
        if age_sec >= float(_YF_PROVISIONAL_RANK_REFRESH_SEC) and _should_intraday_provisional_timer_refresh():
            return True
    if db_mtime is None:
        return False
    if _LAST_DB_MTIME is None:
        return False
    return db_mtime > (_LAST_DB_MTIME + 1e-6)


def _store_built_cache(
    cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]],
    *,
    refreshed_at: datetime,
    db_mtime: float | None,
    latest_pan_daily_asof_int: int | None,
) -> None:
    global _CACHE, _LAST_UPDATED, _LAST_DB_MTIME, _LAST_CACHE_DAILY_ASOF_INT, _LAST_CACHE_PAN_DAILY_ASOF_INT
    latest_daily_asof_int = _resolve_latest_daily_asof_int(cache)
    with _LOCK:
        _CACHE = cache
        _LAST_UPDATED = refreshed_at
        _LAST_DB_MTIME = db_mtime
        _LAST_CACHE_DAILY_ASOF_INT = latest_daily_asof_int
        _LAST_CACHE_PAN_DAILY_ASOF_INT = latest_pan_daily_asof_int
    with _ASOF_BASE_CACHE_LOCK:
        _ASOF_BASE_CACHE.clear()
    with _TRACE_CACHE_LOCK:
        _TRACE_CACHE.clear()


def _refresh_cache_singleflight(*, force: bool) -> None:
    global _REFRESH_IN_PROGRESS, _REFRESH_LAST_ERROR
    db_mtime = _db_mtime()
    with _LOCK:
        if not force and not _cache_needs_refresh(db_mtime):
            return
        while _REFRESH_IN_PROGRESS:
            _REFRESH_COND.wait()
            # Propagate leader failure to concurrent waiters.
            if _REFRESH_LAST_ERROR is not None:
                raise _REFRESH_LAST_ERROR
            db_mtime = _db_mtime()
            if not force and not _cache_needs_refresh(db_mtime):
                return
        _REFRESH_IN_PROGRESS = True
        _REFRESH_LAST_ERROR = None
    try:
        cache, latest_pan_daily_asof_int = _build_cache()
        refreshed_at = datetime.now(timezone.utc)
        _store_built_cache(
            cache,
            refreshed_at=refreshed_at,
            db_mtime=_db_mtime(),
            latest_pan_daily_asof_int=latest_pan_daily_asof_int,
        )
        with _LOCK:
            _REFRESH_LAST_ERROR = None
    except Exception as exc:
        with _LOCK:
            _REFRESH_LAST_ERROR = exc
        raise
    finally:
        with _LOCK:
            _REFRESH_IN_PROGRESS = False
            _REFRESH_COND.notify_all()


def _ensure_cache_fresh() -> None:
    _refresh_cache_singleflight(force=False)


def _ensure_cache_fresh_stale_ok(*, key: tuple[RankTimeframe, RankWhich, RankDir] | None = None) -> None:
    try:
        _ensure_cache_fresh()
    except Exception as exc:
        if not is_transient_duckdb_error(exc):
            raise
        with _LOCK:
            has_any_cache = bool(_CACHE)
            has_key_cache = key is not None and _CACHE.get(key) is not None
        if has_any_cache or has_key_cache:
            logger.warning("rankings cache refresh skipped due to transient DB lock: %s", exc)
            return
        raise


def _shift_yyyymmdd(value: int, *, days: int) -> int:
    raw = value
    if raw >= 1_000_000_000:
        try:
            return int((datetime.fromtimestamp(raw, tz=timezone.utc) + timedelta(days=days)).strftime("%Y%m%d"))
        except Exception:
            return int(raw)
    try:
        base = datetime.strptime(str(raw), "%Y%m%d")
    except ValueError:
        return int(raw)
    return int((base + timedelta(days=days)).strftime("%Y%m%d"))


def _to_yyyymmdd_int(value: int) -> int:
    raw = int(value)
    if 19_000_101 <= raw <= 21_001_231:
        return raw
    if raw >= 1_000_000_000:
        div = 1000 if raw >= 1_000_000_000_000 else 1
        try:
            return int(datetime.fromtimestamp(raw / div, tz=timezone.utc).strftime("%Y%m%d"))
        except Exception:
            return raw
    return raw


def _default_daily_prob_lookup() -> dict[str, Any]:
    return {
        "baseline_rate": 0.5,
        "bins": [],
        "samples": 0,
        "source": "default",
    }


def _load_daily_prob_lookup(
    conn: duckdb.DuckDBPyConnection,
    *,
    pred_dt: int,
    direction: RankDir,
) -> dict[str, Any]:
    cache_key = (int(pred_dt), direction)
    with _DAILY_PROB_CALIB_CACHE_LOCK:
        cached = _DAILY_PROB_CALIB_CACHE.get(cache_key)
        if isinstance(cached, dict):
            return cached

    pred_dt_ymd = _to_yyyymmdd_int(int(pred_dt))
    min_dt = _shift_yyyymmdd(int(pred_dt_ymd), days=-_DAILY_PROB_CALIB_LOOKBACK_DAYS)
    rows = conn.execute(
        """
        WITH bars AS (
            SELECT
                code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS dt_key,
                c AS close
            FROM daily_bars
            WHERE c IS NOT NULL
        ),
        bars_next AS (
            SELECT
                code,
                dt_key,
                close,
                LEAD(close) OVER (PARTITION BY code ORDER BY dt_key) AS next_close
            FROM bars
            WHERE dt_key IS NOT NULL
        ),
        preds AS (
            SELECT
                code,
                dt,
                CASE
                    WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                    WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt / 1000), '%Y%m%d') AS INTEGER)
                    WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS dt_key,
                COALESCE(p_up_5, p_up_10, p_up) AS p_up_short,
                COALESCE(p_down, 1.0 - COALESCE(p_up_5, p_up_10, p_up), 1.0 - p_up) AS p_down_short
            FROM ml_pred_20d
            WHERE (
                CASE
                    WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                    WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt / 1000), '%Y%m%d') AS INTEGER)
                    WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END
            ) BETWEEN ? AND ?
        )
        SELECT
            CASE WHEN ? = 'up' THEN preds.p_up_short ELSE preds.p_down_short END AS prob,
            CASE
                WHEN bars_next.close IS NULL OR bars_next.next_close IS NULL OR bars_next.close <= 0 THEN NULL
                WHEN ? = 'up' THEN CASE WHEN (bars_next.next_close / bars_next.close - 1.0) > 0 THEN 1 ELSE 0 END
                ELSE CASE WHEN (bars_next.next_close / bars_next.close - 1.0) < 0 THEN 1 ELSE 0 END
            END AS label
        FROM preds
        INNER JOIN bars_next
            ON bars_next.code = preds.code
           AND bars_next.dt_key = preds.dt_key
        WHERE bars_next.next_close IS NOT NULL
        """,
        [int(min_dt), int(pred_dt_ymd), direction, direction],
    ).fetchall()

    samples: list[tuple[float, int]] = []
    for row in rows:
        if not row or len(row) < 2:
            continue
        prob = _first_finite(row[0])
        label = row[1]
        if prob is None:
            continue
        if not isinstance(label, (int, float)):
            continue
        p = float(max(0.0, min(1.0, prob)))
        y = 1 if float(label) >= 0.5 else 0
        samples.append((p, y))

    if len(samples) < _DAILY_PROB_CALIB_MIN_SAMPLES:
        baseline = float(sum(y for _p, y in samples) / max(1, len(samples))) if samples else 0.5
        lookup = {
            "baseline_rate": baseline,
            "bins": [],
            "samples": len(samples),
            "source": "insufficient_samples",
        }
        with _DAILY_PROB_CALIB_CACHE_LOCK:
            _DAILY_PROB_CALIB_CACHE[cache_key] = lookup
        return lookup

    samples.sort(key=lambda item: item[0])
    n = len(samples)
    num_bins = max(2, min(_DAILY_PROB_CALIB_MAX_BINS, n // _DAILY_PROB_CALIB_MIN_BIN_SAMPLES))
    if num_bins <= 1:
        num_bins = 2
    bins: list[dict[str, float]] = []
    start = 0
    for idx in range(num_bins):
        end = int(round((idx + 1) * n / num_bins))
        if idx == num_bins - 1:
            end = n
        if end <= start:
            continue
        bucket = samples[start:end]
        low = bucket[0][0]
        high = bucket[-1][0]
        rate = float(sum(y for _p, y in bucket) / len(bucket))
        bins.append(
            {
                "min_prob": float(low),
                "max_prob": float(high),
                "event_rate": float(max(0.0, min(1.0, rate))),
                "samples": float(len(bucket)),
            }
        )
        start = end

    bins = sorted(bins, key=lambda row: (float(row.get("min_prob") or 0.0), float(row.get("max_prob") or 0.0)))
    running = 0.0
    for row in bins:
        running = max(running, float(row.get("event_rate") or 0.0))
        row["event_rate"] = float(max(0.0, min(1.0, running)))

    baseline = float(sum(y for _p, y in samples) / n)
    lookup = {
        "baseline_rate": float(max(0.0, min(1.0, baseline))),
        "bins": bins,
        "samples": int(n),
        "source": "daily_bin_calibration",
    }
    with _DAILY_PROB_CALIB_CACHE_LOCK:
        _DAILY_PROB_CALIB_CACHE[cache_key] = lookup
    return lookup


def _calibrate_daily_probability(prob_side: float | None, lookup: dict[str, Any]) -> float | None:
    if prob_side is None or not math.isfinite(float(prob_side)):
        return None
    p = float(max(0.0, min(1.0, prob_side)))
    baseline = _first_finite((lookup or {}).get("baseline_rate")) or 0.5
    bins = (lookup or {}).get("bins")
    if not isinstance(bins, list) or not bins:
        return float(max(0.0, min(1.0, 0.75 * p + 0.25 * baseline)))
    fallback = float(max(0.0, min(1.0, 0.70 * p + 0.30 * baseline)))
    for idx, row in enumerate(bins):
        if not isinstance(row, dict):
            continue
        low = _first_finite(row.get("min_prob"))
        high = _first_finite(row.get("max_prob"))
        rate = _first_finite(row.get("event_rate"))
        if low is None or high is None or rate is None:
            continue
        in_bin = (p >= low and p < high) if idx < len(bins) - 1 else (p >= low and p <= high)
        if in_bin:
            return float(max(0.0, min(1.0, 0.75 * rate + 0.25 * p)))
    return fallback


def _estimate_daily_downside_risk(
    *,
    direction: RankDir,
    turn_risk: float | None,
    tail_prob: float | None,
) -> float:
    rev = float(max(0.0, min(1.0, turn_risk))) if turn_risk is not None and math.isfinite(float(turn_risk)) else 0.5
    tail = float(max(0.0, min(1.0, tail_prob))) if tail_prob is not None and math.isfinite(float(tail_prob)) else rev
    return float(max(0.0, min(1.0, 0.60 * rev + 0.40 * tail)))


def _load_research_prior_snapshot() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    with _RESEARCH_PRIOR_CACHE_LOCK:
        loaded_at = _RESEARCH_PRIOR_CACHE.get("loaded_at")
        payload = _RESEARCH_PRIOR_CACHE.get("payload")
        if (
            isinstance(loaded_at, datetime)
            and isinstance(payload, dict)
            and (now - loaded_at).total_seconds() <= float(_RESEARCH_PRIOR_TTL_SEC)
        ):
            return payload

    latest_dir = Path(core_config.REPO_ROOT) / "published" / "latest"
    out: dict[str, Any] = {
        "run_id": None,
        "up": {"asof": None, "codes": [], "rank_map": {}},
        "down": {"asof": None, "codes": [], "rank_map": {}},
    }

    manifest_path = latest_dir / "manifest.json"
    if manifest_path.exists():
        try:
            with manifest_path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            run_id = str(manifest.get("run_id") or "").strip()
            out["run_id"] = run_id or None
        except Exception:
            pass

    def _read_latest_codes(path: Path) -> tuple[str | None, list[str]]:
        if not path.exists():
            return None, []
        rows_by_asof: dict[str, list[str]] = {}
        latest_asof: str | None = None
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    if not isinstance(row, dict):
                        continue
                    asof = str(row.get("asof_date") or "").strip()
                    code = str(row.get("code") or "").strip()
                    if not asof or not code:
                        continue
                    rows_by_asof.setdefault(asof, []).append(code)
                    if latest_asof is None or asof > latest_asof:
                        latest_asof = asof
        except Exception:
            return None, []
        if latest_asof is None:
            return None, []
        seen: set[str] = set()
        codes: list[str] = []
        for code in rows_by_asof.get(latest_asof, []):
            if code in seen:
                continue
            seen.add(code)
            codes.append(code)
        return latest_asof, codes

    for side, name in (("up", "long_top20.csv"), ("down", "short_top20.csv")):
        asof, codes = _read_latest_codes(latest_dir / name)
        rank_map = {code: idx + 1 for idx, code in enumerate(codes)}
        out[side] = {"asof": asof, "codes": codes, "rank_map": rank_map}

    with _RESEARCH_PRIOR_CACHE_LOCK:
        _RESEARCH_PRIOR_CACHE["loaded_at"] = now
        _RESEARCH_PRIOR_CACHE["payload"] = out
    return out


def _calc_research_prior_bonus(
    *,
    item: dict[str, Any],
    direction: RankDir,
    code: str,
    prior_snapshot: dict[str, Any] | None,
) -> float:
    side_key = "up" if direction == "up" else "down"
    side_payload = (
        prior_snapshot.get(side_key)
        if isinstance(prior_snapshot, dict) and isinstance(prior_snapshot.get(side_key), dict)
        else {}
    )
    rank_map = side_payload.get("rank_map") if isinstance(side_payload.get("rank_map"), dict) else {}
    codes = side_payload.get("codes") if isinstance(side_payload.get("codes"), list) else []
    rank_raw = rank_map.get(code)
    rank = int(rank_raw) if isinstance(rank_raw, int) else None
    n = int(len(codes))
    aligned = rank is not None
    bonus = 0.0
    if aligned:
        base = float(_RESEARCH_PRIOR_BONUS_UP if direction == "up" else _RESEARCH_PRIOR_BONUS_DOWN)
        strength = 1.0 if n <= 1 else float(max(0.0, min(1.0, 1.0 - ((rank - 1) / max(1, n - 1)))))
        bonus = float(base * (0.60 + 0.40 * strength))

    item["researchPriorRunId"] = (
        str(prior_snapshot.get("run_id") or "") if isinstance(prior_snapshot, dict) else ""
    ) or None
    item["researchPriorAsOf"] = str(side_payload.get("asof") or "") or None
    item["researchPriorAligned"] = bool(aligned)
    item["researchPriorRank"] = int(rank) if rank is not None else None
    item["researchPriorUniverse"] = int(n)
    item["researchPriorBonus"] = float(bonus)
    return float(bonus)


def _decorate_rule_items_with_entry_gate(
    items: list[dict],
    *,
    direction: RankDir,
    risk_mode: RankRiskMode = "balanced",
) -> list[dict]:
    decorated: list[dict] = []
    research_prior = _load_research_prior_snapshot()
    for base in items:
        item = dict(base)
        code = str(item.get("code") or "")
        change = _first_finite(item.get("changePct"))
        weekly_breakout = _first_finite(
            item.get("weeklyBreakoutUpProb") if direction == "up" else item.get("weeklyBreakoutDownProb")
        )
        monthly_breakout = _first_finite(
            item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
        )
        monthly_range = _first_finite(item.get("monthlyRangeProb"))
        candle = _first_finite(item.get("candleTripletUp") if direction == "up" else item.get("candleTripletDown"))
        liquidity = _first_finite(item.get("liquidity20d"))
        prob_proxy = _first_finite(
            weekly_breakout,
            monthly_breakout,
            candle,
            abs(change) if change is not None else None,
        )
        if prob_proxy is None:
            prob_proxy = 0.0
        rule_signal = 0.0
        if change is not None:
            rule_signal = float(change if direction == "up" else -change)
        entry_score = float(
            0.34 * max(0.0, min(1.0, prob_proxy))
            + 0.24 * max(0.0, min(1.0, (rule_signal + 0.06) / 0.14))
            + 0.22 * max(0.0, min(1.0, (weekly_breakout or 0.0)))
            + 0.20 * max(0.0, min(1.0, (monthly_breakout or 0.0)))
        )
        if monthly_range is not None and monthly_range >= 0.72 and (monthly_breakout is None or monthly_breakout < 0.55):
            entry_score -= 0.05
        research_bonus = _calc_research_prior_bonus(
            item=item,
            direction=direction,
            code=code,
            prior_snapshot=research_prior,
        )
        entry_score += float(research_bonus)
        entry_score = float(max(0.0, min(1.0, entry_score)))
        gate_ok = bool(
            liquidity is not None
            and prob_proxy >= _DAILY_RULE_GATE_MIN_PROB
            and (weekly_breakout is not None and weekly_breakout >= _DAILY_RULE_GATE_MIN_BREAKOUT)
            and entry_score >= _DAILY_RULE_GATE_MIN_ENTRY_SCORE
        )
        if gate_ok and (monthly_breakout is not None and monthly_breakout >= 0.60):
            setup_type = "breakout"
        elif gate_ok:
            setup_type = "watch"
        else:
            setup_type = "reject"
        item["hybridScore"] = item.get("hybridScore")
        item["entryScore"] = float(entry_score)
        item["playbookScoreBonus"] = 0.0
        item["probSideRaw"] = float(prob_proxy)
        item["probSideCalib"] = float(prob_proxy)
        item["probSide"] = float(prob_proxy)
        item["entryQualified"] = bool(gate_ok)
        item["setupType"] = setup_type
        _apply_entry_playbook_fields(
            item,
            direction=direction,
            setup_type=setup_type,
            shape_patterns={},
            risk_mode=risk_mode,
        )
        decorated.append(item)

    decorated.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("probSide") or 0.0),
            item.get("code", ""),
        )
    )
    return decorated


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _ensure_ranking_edinet_audit_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ranking_edinet_audit_daily (
            as_of_ymd INTEGER,
            tf TEXT,
            "which" TEXT,
            direction TEXT,
            mode TEXT,
            risk_mode TEXT,
            code TEXT,
            name TEXT,
            rank_position INTEGER,
            entry_score DOUBLE,
            hybrid_score DOUBLE,
            edinet_status TEXT,
            edinet_mapped BOOLEAN,
            edinet_freshness_days INTEGER,
            edinet_metric_count INTEGER,
            edinet_quality_score DOUBLE,
            edinet_data_score DOUBLE,
            edinet_score_bonus DOUBLE,
            edinet_flag_applied BOOLEAN,
            edinet_ebitda_metric DOUBLE,
            edinet_roe DOUBLE,
            edinet_equity_ratio DOUBLE,
            edinet_debt_ratio DOUBLE,
            edinet_operating_cf_margin DOUBLE,
            edinet_revenue_growth_yoy DOUBLE,
            realized_ret_20 DOUBLE,
            realized_win_20 BOOLEAN,
            realized_as_of_ymd INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            realized_updated_at TIMESTAMP,
            PRIMARY KEY(as_of_ymd, tf, "which", direction, mode, risk_mode, code)
        )
        """
    )


def _build_monthly_edinet_audit_signature(
    *,
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    mode: RankMode,
    risk_mode: RankRiskMode,
    items: list[dict],
) -> tuple[int, str, str, str, str, str] | None:
    as_of_values = [_iso_date_to_int(str(item.get("asOf") or "")) for item in items]
    as_of_ymd = max((value for value in as_of_values if isinstance(value, int)), default=None)
    if as_of_ymd is None:
        return None
    return (
        int(as_of_ymd),
        str(tf),
        str(which),
        str(direction),
        str(mode),
        str(risk_mode),
    )


def _acquire_monthly_edinet_audit_persist_window(signature: tuple[int, str, str, str, str, str]) -> bool:
    now_mono = time.monotonic()
    with _MONTHLY_EDINET_AUDIT_LOCK:
        prev = _MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO.get(signature)
        if isinstance(prev, float) and (now_mono - prev) < float(_MONTHLY_EDINET_AUDIT_PERSIST_COOLDOWN_SEC):
            return False
        _MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO[signature] = now_mono
        if len(_MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO) > 256:
            threshold = now_mono - float(_MONTHLY_EDINET_AUDIT_PERSIST_COOLDOWN_SEC) * 2.0
            stale_keys = [
                key
                for key, seen_mono in _MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO.items()
                if seen_mono < threshold
            ]
            for key in stale_keys:
                _MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO.pop(key, None)
    return True


def _release_monthly_edinet_audit_persist_window(signature: tuple[int, str, str, str, str, str]) -> None:
    with _MONTHLY_EDINET_AUDIT_LOCK:
        _MONTHLY_EDINET_AUDIT_LAST_PERSIST_MONO.pop(signature, None)


def _acquire_monthly_edinet_realized_refresh_window() -> bool:
    global _MONTHLY_EDINET_AUDIT_LAST_REALIZED_REFRESH_MONO
    now_mono = time.monotonic()
    with _MONTHLY_EDINET_AUDIT_LOCK:
        elapsed = now_mono - float(_MONTHLY_EDINET_AUDIT_LAST_REALIZED_REFRESH_MONO)
        if elapsed < float(_MONTHLY_EDINET_AUDIT_REALIZED_REFRESH_COOLDOWN_SEC):
            return False
        _MONTHLY_EDINET_AUDIT_LAST_REALIZED_REFRESH_MONO = now_mono
    return True


def _persist_monthly_edinet_audit(
    *,
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    mode: RankMode,
    risk_mode: RankRiskMode,
    items: list[dict],
) -> None:
    if tf != "M" or mode != "hybrid" or not items:
        return
    signature = _build_monthly_edinet_audit_signature(
        tf=tf,
        which=which,
        direction=direction,
        mode=mode,
        risk_mode=risk_mode,
        items=items,
    )
    if signature is None:
        return
    if not _acquire_monthly_edinet_audit_persist_window(signature):
        return
    try:
        with get_conn() as conn:
            _ensure_ranking_edinet_audit_table(conn)
            rows: list[tuple[Any, ...]] = []
            for index, item in enumerate(items, start=1):
                as_of_ymd = _iso_date_to_int(str(item.get("asOf") or ""))
                code = str(item.get("code") or "").strip()
                if as_of_ymd is None or not code:
                    continue
                updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                rows.append(
                    (
                        int(as_of_ymd),
                        str(tf),
                        str(which),
                        str(direction),
                        str(mode),
                        str(risk_mode),
                        code,
                        str(item.get("name") or code),
                        int(index),
                        _first_finite(item.get("entryScore")),
                        _first_finite(item.get("hybridScore")),
                        str(item.get("edinetStatus") or "") or None,
                        bool(item.get("edinetMapped")) if item.get("edinetMapped") is not None else None,
                        int(item["edinetFreshnessDays"])
                        if isinstance(item.get("edinetFreshnessDays"), (int, float))
                        and math.isfinite(float(item.get("edinetFreshnessDays")))
                        else None,
                        int(item["edinetMetricCount"])
                        if isinstance(item.get("edinetMetricCount"), (int, float))
                        and math.isfinite(float(item.get("edinetMetricCount")))
                        else None,
                        _first_finite(item.get("edinetQualityScore")),
                        _first_finite(item.get("edinetDataScore")),
                        _first_finite(item.get("edinetScoreBonus")) or 0.0,
                        bool(item.get("edinetFeatureFlagApplied"))
                        if item.get("edinetFeatureFlagApplied") is not None
                        else None,
                        _first_finite(item.get("edinetEbitdaMetric")),
                        _first_finite(item.get("edinetRoe")),
                        _first_finite(item.get("edinetEquityRatio")),
                        _first_finite(item.get("edinetDebtRatio")),
                        _first_finite(item.get("edinetOperatingCfMargin")),
                        _first_finite(item.get("edinetRevenueGrowthYoy")),
                        updated_at,
                    )
                )
            if rows:
                conn.executemany(
                    """
                    INSERT INTO ranking_edinet_audit_daily (
                        as_of_ymd,
                        tf,
                        "which",
                        direction,
                        mode,
                        risk_mode,
                        code,
                        name,
                        rank_position,
                        entry_score,
                        hybrid_score,
                        edinet_status,
                        edinet_mapped,
                        edinet_freshness_days,
                        edinet_metric_count,
                        edinet_quality_score,
                        edinet_data_score,
                        edinet_score_bonus,
                        edinet_flag_applied,
                        edinet_ebitda_metric,
                        edinet_roe,
                        edinet_equity_ratio,
                        edinet_debt_ratio,
                        edinet_operating_cf_margin,
                        edinet_revenue_growth_yoy,
                        updated_at
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    ON CONFLICT(as_of_ymd, tf, "which", direction, mode, risk_mode, code) DO UPDATE SET
                        name = excluded.name,
                        rank_position = excluded.rank_position,
                        entry_score = excluded.entry_score,
                        hybrid_score = excluded.hybrid_score,
                        edinet_status = excluded.edinet_status,
                        edinet_mapped = excluded.edinet_mapped,
                        edinet_freshness_days = excluded.edinet_freshness_days,
                        edinet_metric_count = excluded.edinet_metric_count,
                        edinet_quality_score = excluded.edinet_quality_score,
                        edinet_data_score = excluded.edinet_data_score,
                        edinet_score_bonus = excluded.edinet_score_bonus,
                        edinet_flag_applied = excluded.edinet_flag_applied,
                        edinet_ebitda_metric = excluded.edinet_ebitda_metric,
                        edinet_roe = excluded.edinet_roe,
                        edinet_equity_ratio = excluded.edinet_equity_ratio,
                        edinet_debt_ratio = excluded.edinet_debt_ratio,
                        edinet_operating_cf_margin = excluded.edinet_operating_cf_margin,
                        edinet_revenue_growth_yoy = excluded.edinet_revenue_growth_yoy,
                        updated_at = excluded.updated_at
                    """,
                    rows,
                )
            _refresh_monthly_edinet_audit_realized_20(conn)
    except Exception as exc:
        _release_monthly_edinet_audit_persist_window(signature)
        logger.debug("ranking_edinet_audit persist skipped: %s", exc)


def _refresh_monthly_edinet_audit_realized_20(conn: duckdb.DuckDBPyConnection) -> None:
    if not _acquire_monthly_edinet_realized_refresh_window():
        return
    if not _table_exists(conn, "ranking_edinet_audit_daily") or not _table_exists(conn, "daily_bars"):
        return
    conn.execute(
        """
        WITH bars AS (
            SELECT
                code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                c
            FROM daily_bars
            WHERE c IS NOT NULL
        ),
        seq AS (
            SELECT
                code,
                ymd,
                c,
                LEAD(ymd, 20) OVER (PARTITION BY code ORDER BY ymd) AS ymd_fwd20,
                LEAD(c, 20) OVER (PARTITION BY code ORDER BY ymd) AS c_fwd20
            FROM bars
            WHERE ymd IS NOT NULL
        ),
        calc AS (
            SELECT
                a.as_of_ymd,
                a.tf,
                a."which",
                a.direction,
                a.mode,
                a.risk_mode,
                a.code,
                s.ymd_fwd20 AS realized_as_of_ymd,
                CASE
                    WHEN s.c IS NULL OR ABS(s.c) <= 1e-12 OR s.c_fwd20 IS NULL THEN NULL
                    ELSE (s.c_fwd20 / s.c) - 1.0
                END AS realized_ret_20
            FROM ranking_edinet_audit_daily a
            JOIN seq s
              ON s.code = a.code
             AND s.ymd = a.as_of_ymd
            WHERE a.tf = 'M'
              AND a.mode = 'hybrid'
              AND a.realized_ret_20 IS NULL
        )
        UPDATE ranking_edinet_audit_daily AS a
           SET realized_ret_20 = calc.realized_ret_20,
               realized_win_20 = CASE
                   WHEN calc.realized_ret_20 IS NULL THEN NULL
                   WHEN calc.realized_ret_20 > 0 THEN TRUE
                   ELSE FALSE
               END,
               realized_as_of_ymd = calc.realized_as_of_ymd,
               realized_updated_at = CURRENT_TIMESTAMP,
               updated_at = CURRENT_TIMESTAMP
        FROM calc
        WHERE a.as_of_ymd = calc.as_of_ymd
          AND a.tf = calc.tf
          AND a."which" = calc."which"
          AND a.direction = calc.direction
          AND a.mode = calc.mode
          AND a.risk_mode = calc.risk_mode
          AND a.code = calc.code
          AND calc.realized_as_of_ymd IS NOT NULL
        """
    )


def get_edinet_monitor(
    *,
    lookback_days: int = 365,
    direction: str = "all",
    risk_mode: str = "all",
    which: str = "latest",
) -> dict[str, Any]:
    lookback = max(30, min(int(lookback_days or 365), 2000))
    from_ymd = int((datetime.now(timezone.utc) - timedelta(days=lookback)).strftime("%Y%m%d"))
    groups: dict[str, dict[str, Any]] = {
        "positive": {
            "count": 0,
            "realized_count": 0,
            "avg_ret20": None,
            "win_rate20": None,
        },
        "negative": {
            "count": 0,
            "realized_count": 0,
            "avg_ret20": None,
            "win_rate20": None,
        },
        "zero": {
            "count": 0,
            "realized_count": 0,
            "avg_ret20": None,
            "win_rate20": None,
        },
    }
    try:
        with get_conn() as conn:
            if not _table_exists(conn, "ranking_edinet_audit_daily"):
                return {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "lookback_days": lookback,
                    "from_ymd": int(from_ymd),
                    "filters": {
                        "tf": "M",
                        "which": str(which),
                        "direction": str(direction),
                        "risk_mode": str(risk_mode),
                        "mode": "hybrid",
                    },
                    "groups": groups,
                    "comparison": {"delta_avg_ret20": None, "delta_win_rate20": None},
                    "insufficient_samples": True,
                    "min_samples": int(_EDINET_MONITOR_MIN_SAMPLES),
                }
            where_parts = [
                "tf = 'M'",
                "mode = 'hybrid'",
                "as_of_ymd >= ?",
            ]
            params: list[Any] = [int(from_ymd)]
            if which in ("latest", "prev"):
                where_parts.append('"which" = ?')
                params.append(str(which))
            if direction in ("up", "down"):
                where_parts.append("direction = ?")
                params.append(str(direction))
            if risk_mode in ("defensive", "balanced", "aggressive"):
                where_parts.append("risk_mode = ?")
                params.append(str(risk_mode))
            where_sql = " AND ".join(where_parts)
            rows = conn.execute(
                f"""
                SELECT
                    CASE
                        WHEN edinet_score_bonus > 1e-12 THEN 'positive'
                        WHEN edinet_score_bonus < -1e-12 THEN 'negative'
                        ELSE 'zero'
                    END AS bucket,
                    COUNT(*) AS cnt,
                    COUNT(realized_ret_20) AS realized_cnt,
                    AVG(realized_ret_20) AS avg_ret20,
                    AVG(
                        CASE
                            WHEN realized_win_20 IS TRUE THEN 1.0
                            WHEN realized_win_20 IS FALSE THEN 0.0
                            ELSE NULL
                        END
                    ) AS win_rate20
                FROM ranking_edinet_audit_daily
                WHERE {where_sql}
                GROUP BY 1
                """,
                params,
            ).fetchall()
            for row in rows:
                bucket = str(row[0] or "")
                if bucket not in groups:
                    continue
                groups[bucket] = {
                    "count": int(row[1] or 0),
                    "realized_count": int(row[2] or 0),
                    "avg_ret20": float(row[3]) if row[3] is not None else None,
                    "win_rate20": float(row[4]) if row[4] is not None else None,
                }
    except Exception as exc:
        logger.debug("edinet monitor query failed: %s", exc)
    pos = groups["positive"]
    neg = groups["negative"]
    delta_ret = None
    if pos["avg_ret20"] is not None and neg["avg_ret20"] is not None:
        delta_ret = float(pos["avg_ret20"] - neg["avg_ret20"])
    delta_win = None
    if pos["win_rate20"] is not None and neg["win_rate20"] is not None:
        delta_win = float(pos["win_rate20"] - neg["win_rate20"])
    insufficient = bool(
        int(pos["realized_count"]) < int(_EDINET_MONITOR_MIN_SAMPLES)
        or int(neg["realized_count"]) < int(_EDINET_MONITOR_MIN_SAMPLES)
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": lookback,
        "from_ymd": int(from_ymd),
        "filters": {
            "tf": "M",
            "which": str(which),
            "direction": str(direction),
            "risk_mode": str(risk_mode),
            "mode": "hybrid",
        },
        "groups": groups,
        "comparison": {"delta_avg_ret20": delta_ret, "delta_win_rate20": delta_win},
        "insufficient_samples": insufficient,
        "min_samples": int(_EDINET_MONITOR_MIN_SAMPLES),
    }


def _to_month_start_int(value: int | None) -> int | None:
    dt = _parse_date_value(value)
    if dt is None:
        return None
    return int(dt.year * 10_000 + dt.month * 100 + 1)


def _build_weekly_bars(daily_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_week = None
    for row in daily_rows:
        if len(row) < 5:
            continue
        date_value, open_, high, low, close = row[:5]
        if open_ is None or high is None or low is None or close is None:
            continue
        dt = _parse_date_value(date_value)
        if not dt:
            continue
        week_start = dt.date() - timedelta(days=dt.weekday())
        if current_week != week_start:
            items.append(
                {
                    "week_start": week_start,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "last_date": dt,
                }
            )
            current_week = week_start
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
            current["last_date"] = dt
    return items


def _drop_incomplete_weekly(weekly: list[dict], last_daily: datetime | None) -> list[dict]:
    if not weekly or not last_daily:
        return weekly
    last_week_start = last_daily.date() - timedelta(days=last_daily.weekday())
    if weekly[-1]["week_start"] == last_week_start and last_daily.weekday() < 4:
        return weekly[:-1]
    return weekly


def _compute_change(
    closes: list[float], dates: list[datetime], which: RankWhich
) -> tuple[float | None, float | None, str | None, float | None, float | None]:
    if which == "latest":
        target_idx = -1
        prev_idx = -2
    else:
        target_idx = -2
        prev_idx = -3
    if len(closes) < abs(prev_idx):
        return None, None, None, None, None
    close = closes[target_idx]
    prev_close = closes[prev_idx]
    if prev_close is None or prev_close == 0:
        return None, None, _format_date(dates[target_idx]), close, prev_close
    change_abs = close - prev_close
    change_pct = change_abs / prev_close
    return change_pct, change_abs, _format_date(dates[target_idx]), close, prev_close


def _clip01(value: float | None) -> float | None:
    if value is None:
        return None
    if not math.isfinite(value):
        return None
    return max(0.0, min(1.0, float(value)))


def _safe_div(num: float, den: float) -> float | None:
    if not math.isfinite(num) or not math.isfinite(den):
        return None
    if abs(den) <= 1e-12:
        return None
    return float(num / den)


def _rolling_sma(values: list[float], period: int) -> list[float | None]:
    if period <= 0:
        return [None for _ in values]
    out: list[float | None] = [None for _ in values]
    running = 0.0
    for idx, value in enumerate(values):
        running += float(value)
        if idx >= period:
            running -= float(values[idx - period])
        if idx >= period - 1:
            out[idx] = float(running / period)
    return out


def _count_up_streak_with_pending(values: list[float], ma_values: list[float | None]) -> int:
    up_count = 0
    down_count = 0
    pending: str | None = None
    for value, ma in zip(values, ma_values):
        if ma is None or not math.isfinite(ma):
            up_count = 0
            down_count = 0
            pending = None
            continue
        if value >= ma:
            if up_count > 0:
                up_count += 1
                pending = None
            elif down_count > 0:
                if pending == "up":
                    up_count = 2
                    down_count = 0
                    pending = None
                else:
                    pending = "up"
            else:
                up_count = 1
                down_count = 0
                pending = None
        else:
            if down_count > 0:
                down_count += 1
                pending = None
            elif up_count > 0:
                if pending == "down":
                    down_count = 2
                    up_count = 0
                    pending = None
                else:
                    pending = "down"
            else:
                down_count = 1
                up_count = 0
                pending = None
    return int(max(0, up_count))


def _calc_60v_signals(daily_rows: list[tuple]) -> dict[str, float]:
    closes: list[float] = []
    for row in daily_rows:
        if len(row) < 5:
            continue
        close = _finite_float(row[4])
        if close is None:
            continue
        closes.append(float(close))

    default = {
        "reclaim60": 0.0,
        "v60Core": 0.0,
        "v60Strong": 0.0,
        "cnt60Up": 0.0,
        "cnt100Up": 0.0,
    }
    if len(closes) < 62:
        return default

    ma20 = _rolling_sma(closes, 20)
    ma60 = _rolling_sma(closes, 60)
    ma100 = _rolling_sma(closes, 100)
    last_idx = len(closes) - 1
    close_now = closes[last_idx]
    ma20_now = ma20[last_idx]
    ma60_now = ma60[last_idx]
    if ma20_now is None or ma60_now is None:
        return default
    cnt60_up = _count_up_streak_with_pending(closes, ma60)
    cnt100_up = _count_up_streak_with_pending(closes, ma100)

    ma20_prev = ma20[last_idx - 1] if last_idx - 1 >= 0 else None
    ma60_prev = ma60[last_idx - 1] if last_idx - 1 >= 0 else None
    ma20_slope = (
        float(ma20_now - ma20_prev)
        if ma20_prev is not None and math.isfinite(ma20_prev) and math.isfinite(ma20_now)
        else None
    )
    ma60_slope = (
        float(ma60_now - ma60_prev)
        if ma60_prev is not None and math.isfinite(ma60_prev) and math.isfinite(ma60_now)
        else None
    )
    dist20 = _safe_div(close_now - ma20_now, ma20_now)
    dist60 = _safe_div(close_now - ma60_now, ma60_now)

    recent_below60 = False
    for idx in range(max(0, last_idx - 15), last_idx):
        ma60_i = ma60[idx]
        if ma60_i is None:
            continue
        if closes[idx] < ma60_i:
            recent_below60 = True
            break

    reclaim60 = bool(ma20_now > ma60_now and close_now >= ma60_now and recent_below60)
    v60_core = bool(reclaim60 and dist20 is not None and float(dist20) >= -0.01)
    v60_strong = bool(
        v60_core
        and dist20 is not None
        and dist60 is not None
        and float(dist20) >= 0.02
        and float(dist60) >= 0.04
        and isinstance(ma20_slope, (int, float))
        and isinstance(ma60_slope, (int, float))
        and float(ma20_slope) > 0
        and float(ma60_slope) > 0
    )
    return {
        "reclaim60": 1.0 if reclaim60 else 0.0,
        "v60Core": 1.0 if v60_core else 0.0,
        "v60Strong": 1.0 if v60_strong else 0.0,
        "cnt60Up": float(cnt60_up),
        "cnt100Up": float(cnt100_up),
    }


def _calc_triplet_candle_signals(daily_rows: list[tuple]) -> dict[str, float | None]:
    bars: list[dict[str, float]] = []
    for row in daily_rows:
        if len(row) < 5:
            continue
        o = _finite_float(row[1])
        h = _finite_float(row[2])
        l = _finite_float(row[3])
        c = _finite_float(row[4])
        if o is None or h is None or l is None or c is None:
            continue
        if h < l:
            continue
        span = h - l
        body = c - o
        body_ratio = abs(body) / span if span > 1e-12 else 0.0
        upper_ratio = (h - max(o, c)) / span if span > 1e-12 else 0.0
        lower_ratio = (min(o, c) - l) / span if span > 1e-12 else 0.0
        bars.append(
            {
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "body": body,
                "body_ratio": max(0.0, min(1.0, body_ratio)),
                "upper_ratio": max(0.0, min(1.0, upper_ratio)),
                "lower_ratio": max(0.0, min(1.0, lower_ratio)),
            }
        )

    if not bars:
        return {
            "candleBodyRatio": None,
            "candleUpperWickRatio": None,
            "candleLowerWickRatio": None,
            "candleTripletUp": None,
            "candleTripletDown": None,
            "shootingStarLike": None,
            "bullMarubozu": None,
            "bearMarubozu": None,
            "threeWhiteSoldiers": None,
            "threeBlackCrows": None,
            "morningStar": None,
            "bullEngulfing": None,
        }

    latest = bars[-1]
    shooting_star_like = 1.0 if (
        float(latest["upper_ratio"]) >= 0.48
        and float(latest["body_ratio"]) <= 0.38
        and float(latest["lower_ratio"]) <= 0.24
    ) else 0.0
    bull_marubozu = 1.0 if (
        float(latest["body"]) > 0
        and float(latest["body_ratio"]) >= 0.70
        and float(latest["upper_ratio"]) <= 0.12
        and float(latest["lower_ratio"]) <= 0.12
    ) else 0.0
    bear_marubozu = 1.0 if (
        float(latest["body"]) < 0
        and float(latest["body_ratio"]) >= 0.70
        and float(latest["upper_ratio"]) <= 0.12
        and float(latest["lower_ratio"]) <= 0.12
    ) else 0.0
    bull_engulfing: float | None = None
    if len(bars) >= 2:
        prev = bars[-2]
        bull_engulfing = 1.0 if (
            float(prev["body"]) < 0
            and float(latest["body"]) > 0
            and float(latest["o"]) <= float(prev["c"])
            and float(latest["c"]) >= float(prev["o"])
        ) else 0.0
    if len(bars) < 3:
        return {
            "candleBodyRatio": latest["body_ratio"],
            "candleUpperWickRatio": latest["upper_ratio"],
            "candleLowerWickRatio": latest["lower_ratio"],
            "candleTripletUp": None,
            "candleTripletDown": None,
            "shootingStarLike": shooting_star_like,
            "bullMarubozu": bull_marubozu,
            "bearMarubozu": bear_marubozu,
            "threeWhiteSoldiers": None,
            "threeBlackCrows": None,
            "morningStar": None,
            "bullEngulfing": bull_engulfing,
        }

    b0, b1, b2 = bars[-3], bars[-2], bars[-1]
    trio = [b0, b1, b2]
    bull_count = sum(1 for b in trio if b["body"] > 0)
    bear_count = sum(1 for b in trio if b["body"] < 0)
    higher_close = 1.0 if (b0["c"] < b1["c"] < b2["c"]) else 0.0
    lower_close = 1.0 if (b0["c"] > b1["c"] > b2["c"]) else 0.0
    move_3 = _safe_div(b2["c"] - b0["c"], b0["c"]) or 0.0

    prev_anchor = bars[-10]["c"] if len(bars) >= 10 else b0["o"]
    prev_anchor = prev_anchor if abs(prev_anchor) > 1e-12 else b0["o"]
    prior_move = _safe_div(b0["c"] - prev_anchor, prev_anchor) or 0.0

    latest_upper = float(b2["upper_ratio"])
    latest_lower = float(b2["lower_ratio"])
    three_white_soldiers = 1.0 if (
        all(float(bar["body"]) > 0 for bar in trio)
        and float(b0["c"]) < float(b1["c"]) < float(b2["c"])
        and min(float(bar["body_ratio"]) for bar in trio) >= 0.45
    ) else 0.0
    three_black_crows = 1.0 if (
        all(float(bar["body"]) < 0 for bar in trio)
        and float(b0["h"]) > float(b1["h"]) > float(b2["h"])
        and float(b0["c"]) > float(b1["c"]) > float(b2["c"])
        and max(float(bar["lower_ratio"]) for bar in trio) <= 0.30
    ) else 0.0
    morning_star = 1.0 if (
        float(b0["body"]) < 0
        and float(b0["body_ratio"]) >= 0.60
        and float(b1["body_ratio"]) <= 0.20
        and float(b2["body"]) > 0
        and float(b2["body_ratio"]) >= 0.60
        and float(b2["c"]) >= (float(b0["o"]) + float(b0["c"])) / 2.0
    ) else 0.0

    # Reversal-style 3-candle block:
    # - Up side: short pullback (bearish trio) after prior uptrend and lower-wick support.
    # - Down side: short squeeze (bullish trio) after prior downtrend and upper-wick pressure.
    up_prob = _clip01(
        0.10
        + 0.26 * (bear_count / 3.0)
        + 0.18 * lower_close
        + 0.16 * (_clip01(((-move_3) - 0.003) / 0.05) or 0.0)
        + 0.12 * (_clip01((prior_move + 0.06) / 0.18) or 0.0)
        + 0.11 * (_clip01((latest_lower + 0.02) / 0.30) or 0.0)
        + 0.07 * (_clip01((0.55 - latest_upper) / 0.55) or 0.0)
    )
    down_prob = _clip01(
        0.10
        + 0.26 * (bull_count / 3.0)
        + 0.18 * higher_close
        + 0.16 * (_clip01((move_3 - 0.003) / 0.05) or 0.0)
        + 0.12 * (_clip01(((-prior_move) + 0.06) / 0.18) or 0.0)
        + 0.11 * (_clip01((latest_upper + 0.02) / 0.30) or 0.0)
        + 0.07 * (_clip01((0.55 - latest_lower) / 0.55) or 0.0)
    )
    return {
        "candleBodyRatio": latest["body_ratio"],
        "candleUpperWickRatio": latest["upper_ratio"],
        "candleLowerWickRatio": latest["lower_ratio"],
        "candleTripletUp": up_prob,
        "candleTripletDown": down_prob,
        "shootingStarLike": shooting_star_like,
        "bullMarubozu": bull_marubozu,
        "bearMarubozu": bear_marubozu,
        "threeWhiteSoldiers": three_white_soldiers,
        "threeBlackCrows": three_black_crows,
        "morningStar": morning_star,
        "bullEngulfing": bull_engulfing,
    }


def _calc_regime_probs(closes: list[float], *, lookback: int) -> dict[str, float | None]:
    need = max(lookback + 1, 4)
    if len(closes) < need:
        return {
            "breakoutUpProb": None,
            "breakoutDownProb": None,
            "rangeProb": None,
            "rangeWidth": None,
            "rangePos": None,
        }
    last = float(closes[-1])
    hist = [float(v) for v in closes[-(lookback + 1) : -1]]
    if not hist:
        return {
            "breakoutUpProb": None,
            "breakoutDownProb": None,
            "rangeProb": None,
            "rangeWidth": None,
            "rangePos": None,
        }
    hi = max(hist)
    lo = min(hist)
    scale = max(abs((hi + lo) / 2.0), abs(hi), abs(lo), 1e-9)
    width = max(0.0, (hi - lo) / scale)
    if hi > lo:
        range_pos = max(0.0, min(1.0, (last - lo) / (hi - lo)))
    else:
        range_pos = 0.5
    compression = _clip01((0.45 - width) / 0.45) or 0.0
    up_break_dist = max(0.0, (last - hi) / max(abs(hi), 1e-9))
    down_break_dist = max(0.0, (lo - last) / max(abs(lo), 1e-9))
    up_break = _clip01(up_break_dist / 0.08) or 0.0
    down_break = _clip01(down_break_dist / 0.08) or 0.0
    midness = max(0.0, 1.0 - abs(range_pos - 0.5) * 2.0)

    up_prob = _clip01(0.55 * compression + 0.35 * range_pos + 0.10 * up_break)
    down_prob = _clip01(0.55 * compression + 0.35 * (1.0 - range_pos) + 0.10 * down_break)
    range_prob = _clip01(0.65 * compression + 0.35 * midness - 0.40 * max(up_break, down_break))
    if up_break > 0.0:
        up_prob = max(up_prob or 0.0, (_clip01(0.72 + 0.28 * up_break) or 0.0))
    if down_break > 0.0:
        down_prob = max(down_prob or 0.0, (_clip01(0.72 + 0.28 * down_break) or 0.0))
    return {
        "breakoutUpProb": up_prob,
        "breakoutDownProb": down_prob,
        "rangeProb": range_prob,
        "rangeWidth": width,
        "rangePos": range_pos,
    }


def _calc_market_breadth_state(daily_map: dict[str, list[tuple]]) -> dict[str, Any]:
    up_counts: dict[int, int] = {}
    total_counts: dict[int, int] = {}
    for rows in daily_map.values():
        prev_close: float | None = None
        for row in rows:
            if len(row) < 5:
                continue
            date_raw = row[0]
            close = _finite_float(row[4])
            if close is None:
                prev_close = None
                continue
            if prev_close is not None and abs(prev_close) > 1e-12:
                try:
                    date_key = int(date_raw)
                except (TypeError, ValueError):
                    prev_close = close
                    continue
                total_counts[date_key] = int(total_counts.get(date_key, 0)) + 1
                if close > prev_close:
                    up_counts[date_key] = int(up_counts.get(date_key, 0)) + 1
            prev_close = close

    if not total_counts:
        return {
            "marketBreadthDate": None,
            "marketBreadthDateIso": None,
            "marketBreadthAdvRatio": None,
            "marketBreadthSampleSize": 0,
            "marketRiskOff": None,
            "marketRiskOn": None,
            "marketRegime": None,
        }

    latest_date = max(total_counts.keys())
    sample_size = int(total_counts.get(latest_date, 0))
    if sample_size <= 0:
        return {
            "marketBreadthDate": latest_date,
            "marketBreadthDateIso": _format_date(_parse_date_value(latest_date)),
            "marketBreadthAdvRatio": None,
            "marketBreadthSampleSize": 0,
            "marketRiskOff": None,
            "marketRiskOn": None,
            "marketRegime": None,
        }

    adv_ratio = float(up_counts.get(latest_date, 0) / sample_size)
    risk_off = bool(adv_ratio <= _MARKET_BREADTH_RISK_OFF_MAX_ADV)
    risk_on = bool(adv_ratio >= _MARKET_BREADTH_RISK_ON_MIN_ADV)
    regime = "risk_off" if risk_off else ("risk_on" if risk_on else "neutral")
    return {
        "marketBreadthDate": latest_date,
        "marketBreadthDateIso": _format_date(_parse_date_value(latest_date)),
        "marketBreadthAdvRatio": adv_ratio,
        "marketBreadthSampleSize": sample_size,
        "marketRiskOff": risk_off,
        "marketRiskOn": risk_on,
        "marketRegime": regime,
    }


def _detect_monthly_body_box(monthly_rows: list[tuple]) -> dict[str, float | bool | int] | None:
    min_months = 3
    max_months = 14
    max_range_pct = 0.20
    wild_wick_pct = 0.10
    if len(monthly_rows) < min_months:
        return None
    bars: list[dict[str, float]] = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month = _finite_float(row[0])
        o = _finite_float(row[1])
        h = _finite_float(row[2])
        l = _finite_float(row[3])
        c = _finite_float(row[4])
        if month is None or o is None or h is None or l is None or c is None:
            continue
        body_high = max(o, c)
        body_low = min(o, c)
        bars.append(
            {
                "time": float(month),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "body_high": float(body_high),
                "body_low": float(body_low),
            }
        )
    if len(bars) < min_months:
        return None
    length_max = min(max_months, len(bars))
    for length in range(length_max, min_months - 1, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > max_range_pct:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * (1.0 + wild_wick_pct) or item["low"] < lower * (1.0 - wild_wick_pct):
                wild = True
                break
        return {
            "start": int(window[0]["time"]),
            "end": int(window[-1]["time"]),
            "upper": float(upper),
            "lower": float(lower),
            "months": int(length),
            "rangePct": float(range_pct),
            "wild": bool(wild),
            "lastClose": float(window[-1]["close"]),
        }
    return None


def _calc_monthly_box_state(
    *,
    entry_close: float | None,
    box: dict[str, float | bool | int] | None,
) -> tuple[str, float | None]:
    if entry_close is None or box is None:
        return "no_box", None
    lower = _first_finite(box.get("lower"))
    upper = _first_finite(box.get("upper"))
    if lower is None or upper is None or upper <= lower:
        return "no_box", None
    pos = (entry_close - lower) / (upper - lower)
    if pos < 0.0:
        return "below_box", float(pos)
    if pos <= 0.25:
        return "box_lower", float(pos)
    if pos <= 0.75:
        return "box_mid", float(pos)
    if pos <= 1.0:
        return "box_upper", float(pos)
    return "breakout_up", float(pos)


def _calc_shape_pattern_flags(
    *,
    direction: RankDir,
    trend_up_strict: bool,
    trend_down_strict: bool,
    monthly_box_state: str | None,
    monthly_box_months: float | None,
    dist_ma20_signed: float | None,
    cnt60_up: float | None,
    cnt100_up: float | None,
    monthly_range_pos: float | None = None,
    monthly_range_prob: float | None = None,
    monthly_breakout_down_prob: float | None = None,
    shooting_star_like: float | None = None,
    bear_marubozu: float | None = None,
    three_black_crows: float | None = None,
) -> dict[str, bool]:
    box_state = str(monthly_box_state or "")
    months = _first_finite(monthly_box_months)
    cnt60 = _first_finite(cnt60_up)
    cnt100 = _first_finite(cnt100_up)
    dist = _first_finite(dist_ma20_signed)
    range_pos = _first_finite(monthly_range_pos)
    range_prob = _first_finite(monthly_range_prob)
    breakout_down_prob = _first_finite(monthly_breakout_down_prob)
    shooting_star = _first_finite(shooting_star_like)
    bear_marubozu_signal = _first_finite(bear_marubozu)
    three_black_crows_signal = _first_finite(three_black_crows)
    bearish_reversal_candle = bool(
        (shooting_star is not None and shooting_star >= 0.5)
        or (bear_marubozu_signal is not None and bear_marubozu_signal >= 0.5)
        or (three_black_crows_signal is not None and three_black_crows_signal >= 0.5)
    )
    is_matured_box = bool(months is not None and months >= 5.0)
    a1_matured_breakout = False
    a2_box_trend = False
    a3_capitulation_rebound = False
    s1_weak_breakdown = False
    s2_weak_box = False
    s3_late_breakout = False
    d1_short_breakdown = False
    d2_short_mixed_far = False
    d3_short_na_below = False
    d4_short_double_top = False
    d5_short_head_shoulders = False
    dtrap_stackdown_far = False
    dtrap_overheat_momentum = False
    dtrap_top_fakeout = False
    if direction == "up":
        a1_matured_breakout = bool(
            is_matured_box
            and box_state == "breakout_up"
            and cnt60 is not None
            and 10.0 <= cnt60 < 60.0
            and cnt100 is not None
            and (cnt100 < 50.0 or (100.0 <= cnt100 < 200.0))
        )
        a2_box_trend = bool(
            is_matured_box
            and box_state in {"box_mid", "box_upper"}
            and trend_up_strict
            and cnt60 is not None
            and cnt60 >= 30.0
            and dist is not None
            and -0.03 <= dist <= 0.12
        )
        a3_capitulation_rebound = bool(
            trend_down_strict
            and box_state in {"below_box", "box_lower"}
            and cnt60 is not None
            and cnt60 < 10.0
            and dist is not None
            and dist <= -0.05
        )
        s1_weak_breakdown = bool(
            (not trend_up_strict)
            and box_state == "below_box"
            and cnt60 is not None
            and cnt100 is not None
            and cnt60 < 10.0
            and cnt100 < 20.0
            and dist is not None
            and dist < 0.0
        )
        s2_weak_box = bool(
            (not trend_up_strict)
            and box_state in {"box_lower", "below_box"}
            and cnt60 is not None
            and cnt100 is not None
            and cnt60 < 10.0
            and cnt100 < 20.0
        )
        s3_late_breakout = bool(
            is_matured_box
            and box_state == "breakout_up"
            and cnt100 is not None
            and cnt100 >= 200.0
        )
    else:
        d1_short_breakdown = bool(
            (not trend_down_strict)
            and box_state == "below_box"
            and dist is not None
            and dist <= -0.05
            and cnt60 is not None
            and cnt100 is not None
            and cnt60 < 10.0
            and cnt100 < 20.0
        )
        d2_short_mixed_far = bool(
            (not trend_up_strict)
            and (not trend_down_strict)
            and box_state == "below_box"
            and dist is not None
            and dist <= -0.05
        )
        d3_short_na_below = bool(
            (not trend_up_strict)
            and (not trend_down_strict)
            and box_state in {"below_box", "box_mid", "box_upper", "no_box"}
            and dist is not None
            and -0.05 < dist < 0.0
            and cnt60 is not None
            and cnt60 < 10.0
            and cnt100 is not None
            and cnt100 < 20.0
        )
        d4_short_double_top = bool(
            is_matured_box
            and box_state in {"box_upper", "breakout_up"}
            and range_pos is not None
            and range_pos >= 0.68
            and cnt60 is not None
            and cnt60 >= 45.0
            and cnt100 is not None
            and cnt100 >= 100.0
            and dist is not None
            and -0.02 <= dist <= 0.08
            and (
                bearish_reversal_candle
                or (breakout_down_prob is not None and breakout_down_prob >= 0.57)
            )
        )
        d5_short_head_shoulders = bool(
            is_matured_box
            and box_state in {"box_mid", "box_upper", "breakout_up"}
            and (not trend_down_strict)
            and range_pos is not None
            and 0.45 <= range_pos <= 0.78
            and cnt60 is not None
            and 20.0 <= cnt60 <= 70.0
            and cnt100 is not None
            and cnt100 >= 80.0
            and dist is not None
            and -0.03 <= dist <= 0.04
            and (
                (breakout_down_prob is not None and breakout_down_prob >= 0.55)
                or bearish_reversal_candle
            )
        )
        dtrap_stackdown_far = bool(
            trend_down_strict
            and dist is not None
            and dist <= -0.05
        )
        dtrap_overheat_momentum = bool(
            trend_up_strict
            and dist is not None
            and dist >= 0.12
        )
        dtrap_top_fakeout = bool(
            (d4_short_double_top or d5_short_head_shoulders)
            and (
                (
                    range_prob is not None
                    and range_prob >= 0.70
                    and (breakout_down_prob is None or breakout_down_prob < 0.55)
                )
                or (
                    trend_up_strict
                    and dist is not None
                    and dist >= 0.03
                    and (not bearish_reversal_candle)
                )
            )
        )
    return {
        "a1MaturedBreakout": a1_matured_breakout,
        "a2BoxTrend": a2_box_trend,
        "a3CapitulationRebound": a3_capitulation_rebound,
        "s1WeakBreakdown": s1_weak_breakdown,
        "s2WeakBox": s2_weak_box,
        "s3LateBreakout": s3_late_breakout,
        "d1ShortBreakdown": d1_short_breakdown,
        "d2ShortMixedFar": d2_short_mixed_far,
        "d3ShortNaBelow": d3_short_na_below,
        "d4ShortDoubleTop": d4_short_double_top,
        "d5ShortHeadShoulders": d5_short_head_shoulders,
        "dTrapStackDownFar": dtrap_stackdown_far,
        "dTrapOverheatMomentum": dtrap_overheat_momentum,
        "dTrapTopFakeout": dtrap_top_fakeout,
    }


def _recommend_holding_days(
    *,
    direction: RankDir,
    shape_patterns: dict[str, bool],
) -> tuple[int, str]:
    if direction == "down":
        if bool(shape_patterns.get("dTrapStackDownFar")):
            return 3, "売られ過ぎ反発リスクが高く短期決済"
        if bool(shape_patterns.get("dTrapOverheatMomentum")):
            return 5, "順行トレンド逆張りは短期決済"
        if bool(shape_patterns.get("d1ShortBreakdown")):
            return 10, "弱形下抜けは10日付近で期待値が高い"
        if bool(shape_patterns.get("d2ShortMixedFar")):
            return 10, "混在崩れは10日付近で優位"
        if bool(shape_patterns.get("d3ShortNaBelow")):
            return 10, "初期弱含みは短中期で利確優位"
        return 10, "ショート標準ホールド"
    if bool(shape_patterns.get("a3CapitulationRebound")):
        return 20, "反発狙いは20日前後で利確"
    if bool(shape_patterns.get("a1MaturedBreakout")):
        return 25, "成熟Box抜けは25日前後のトレンド追随"
    if bool(shape_patterns.get("a2BoxTrend")):
        return 25, "Box上半トレンドは25日前後で保有"
    return 25, "ロング標準ホールド"


def _recommend_holding_range(
    *,
    direction: RankDir,
    setup_type: str | None,
    shape_patterns: dict[str, bool],
    hold_days: int,
) -> tuple[int, int]:
    setup = str(setup_type or "")
    if direction == "down":
        if bool(shape_patterns.get("dTrapStackDownFar")):
            return 3, 5
        if bool(shape_patterns.get("dTrapOverheatMomentum")):
            return 3, 7
        return 7, 12
    if setup in {"rebound", "turn"} or bool(shape_patterns.get("a3CapitulationRebound")):
        return 15, 20
    if setup in {"continuation"}:
        return 20, 25
    return 20, max(25, int(hold_days))


def _recommend_invalidation_policy(
    *,
    direction: RankDir,
    setup_type: str | None,
    shape_patterns: dict[str, bool],
) -> dict[str, Any]:
    setup = str(setup_type or "")
    if direction == "down":
        if (
            bool(shape_patterns.get("dTrapStackDownFar"))
            or bool(shape_patterns.get("dTrapOverheatMomentum"))
            or bool(shape_patterns.get("dTrapTopFakeout"))
        ):
            return {
                "invalidationTrigger": "stop3",
                "invalidationConservativeAction": "exit",
                "invalidationAggressiveAction": "exit",
                "invalidationDotenRecommended": False,
                "invalidationOppositeHoldDays": None,
                "invalidationExpectedDeltaMean": -0.0030,
                "invalidationPolicyNote": "否定時は反発が速く、ドテンより撤退を優先",
            }
        if (
            setup == "breakdown"
            or bool(shape_patterns.get("d1ShortBreakdown"))
            or bool(shape_patterns.get("d2ShortMixedFar"))
            or bool(shape_patterns.get("d3ShortNaBelow"))
            or bool(shape_patterns.get("d4ShortDoubleTop"))
            or bool(shape_patterns.get("d5ShortHeadShoulders"))
        ):
            return {
                "invalidationTrigger": "box_reclaim",
                "invalidationConservativeAction": "exit",
                "invalidationAggressiveAction": "doten_opt",
                "invalidationDotenRecommended": True,
                "invalidationOppositeHoldDays": 25,
                "invalidationExpectedDeltaMean": _ENTRY_POLICY_DELTA_SHORT_BOX_DOTEN_OPT,
                "invalidationPolicyNote": "Box回復で下落否定。守りは撤退、攻めはロングへドテン",
            }
        return {
            "invalidationTrigger": "stop5",
            "invalidationConservativeAction": "exit",
            "invalidationAggressiveAction": "exit",
            "invalidationDotenRecommended": False,
            "invalidationOppositeHoldDays": None,
            "invalidationExpectedDeltaMean": -0.0016,
            "invalidationPolicyNote": "明確な否定時のみ撤退し、ショート継続は避ける",
        }

    if setup in {"rebound", "turn"} or bool(shape_patterns.get("a3CapitulationRebound")):
        return {
            "invalidationTrigger": "stop5",
            "invalidationConservativeAction": "exit",
            "invalidationAggressiveAction": "hold",
            "invalidationDotenRecommended": False,
            "invalidationOppositeHoldDays": None,
            "invalidationExpectedDeltaMean": -0.0055,
            "invalidationPolicyNote": "反発狙い否定時は撤退。ドテン期待値は低い",
        }

    return {
        "invalidationTrigger": "box_break",
        "invalidationConservativeAction": "exit",
        "invalidationAggressiveAction": "hold",
        "invalidationDotenRecommended": False,
        "invalidationOppositeHoldDays": None,
        "invalidationExpectedDeltaMean": _ENTRY_POLICY_DELTA_LONG_BOX_EXIT,
        "invalidationPolicyNote": "長期上昇取りは継続優位。否定時は守りの撤退のみ",
    }


def _calc_playbook_entry_bonus(
    *,
    direction: RankDir,
    shape_patterns: dict[str, bool],
) -> float:
    if direction == "down":
        if (
            bool(shape_patterns.get("dTrapStackDownFar"))
            or bool(shape_patterns.get("dTrapOverheatMomentum"))
            or bool(shape_patterns.get("dTrapTopFakeout"))
        ):
            return -_ENTRY_PENALTY_PLAYBOOK_TRAP
        if (
            bool(shape_patterns.get("d1ShortBreakdown"))
            or bool(shape_patterns.get("d2ShortMixedFar"))
            or bool(shape_patterns.get("d3ShortNaBelow"))
            or bool(shape_patterns.get("d4ShortDoubleTop"))
            or bool(shape_patterns.get("d5ShortHeadShoulders"))
        ):
            return _ENTRY_BONUS_PLAYBOOK_SHORT_STRONG
        return 0.0

    if (
        bool(shape_patterns.get("s1WeakBreakdown"))
        or bool(shape_patterns.get("s2WeakBox"))
        or bool(shape_patterns.get("s3LateBreakout"))
    ):
        return -_ENTRY_PENALTY_PLAYBOOK_TRAP
    if bool(shape_patterns.get("a1MaturedBreakout")) or bool(shape_patterns.get("a2BoxTrend")):
        return _ENTRY_BONUS_PLAYBOOK_LONG_STRONG
    if bool(shape_patterns.get("a3CapitulationRebound")):
        return _ENTRY_BONUS_PLAYBOOK_LONG_REBOUND
    return 0.0


def _resolve_invalidation_recommended_action(
    *,
    policy: dict[str, Any],
    risk_mode: RankRiskMode,
) -> str:
    conservative = str(policy.get("invalidationConservativeAction") or "exit")
    aggressive = str(policy.get("invalidationAggressiveAction") or conservative)
    doten_recommended = bool(policy.get("invalidationDotenRecommended"))
    expected_delta = _first_finite(policy.get("invalidationExpectedDeltaMean"))
    if risk_mode == "defensive":
        return conservative
    if risk_mode == "aggressive":
        return aggressive
    if doten_recommended and expected_delta is not None and expected_delta > 0:
        return aggressive
    return conservative


def _resolve_short_precision_gates(*, risk_mode: RankRiskMode) -> tuple[float, float]:
    if risk_mode == "defensive":
        return _ENTRY_SHORT_MIN_PROB_DEFENSIVE, _ENTRY_SHORT_MIN_TURN_DEFENSIVE
    if risk_mode == "aggressive":
        return _ENTRY_SHORT_MIN_PROB_AGGRESSIVE, _ENTRY_SHORT_MIN_TURN_AGGRESSIVE
    return _ENTRY_SHORT_MIN_PROB_BALANCED, _ENTRY_SHORT_MIN_TURN_BALANCED


def _resolve_short_pressure_score_gate(*, risk_mode: RankRiskMode) -> float:
    if risk_mode == "defensive":
        return _ENTRY_SHORT_PRESSURE_SCORE_DEFENSIVE
    if risk_mode == "aggressive":
        return _ENTRY_SHORT_PRESSURE_SCORE_AGGRESSIVE
    return _ENTRY_SHORT_PRESSURE_SCORE_BALANCED


def _resolve_short_pressure_max_ev(*, risk_mode: RankRiskMode) -> float:
    if risk_mode == "defensive":
        return _ENTRY_SHORT_PRESSURE_MAX_EV_DEFENSIVE
    if risk_mode == "aggressive":
        return _ENTRY_SHORT_PRESSURE_MAX_EV_AGGRESSIVE
    return _ENTRY_SHORT_PRESSURE_MAX_EV_BALANCED


def _apply_entry_playbook_fields(
    item: dict,
    *,
    direction: RankDir,
    setup_type: str | None,
    shape_patterns: dict[str, bool] | None,
    risk_mode: RankRiskMode = "balanced",
) -> None:
    patterns = shape_patterns if isinstance(shape_patterns, dict) else {}
    hold_days, hold_reason = _recommend_holding_days(direction=direction, shape_patterns=patterns)
    hold_min, hold_max = _recommend_holding_range(
        direction=direction,
        setup_type=setup_type,
        shape_patterns=patterns,
        hold_days=hold_days,
    )
    policy = _recommend_invalidation_policy(
        direction=direction,
        setup_type=setup_type,
        shape_patterns=patterns,
    )

    item["recommendedHoldDays"] = int(hold_days)
    item["recommendedHoldMinDays"] = int(hold_min)
    item["recommendedHoldMaxDays"] = int(hold_max)
    item["recommendedHoldReason"] = str(hold_reason)
    item["invalidationPolicyVersion"] = _ENTRY_POLICY_VERSION
    item["invalidationTrigger"] = policy.get("invalidationTrigger")
    item["invalidationConservativeAction"] = policy.get("invalidationConservativeAction")
    item["invalidationAggressiveAction"] = policy.get("invalidationAggressiveAction")
    item["invalidationDotenRecommended"] = bool(policy.get("invalidationDotenRecommended"))
    opposite_hold = _first_finite(policy.get("invalidationOppositeHoldDays"))
    item["invalidationOppositeHoldDays"] = int(opposite_hold) if opposite_hold is not None else None
    delta_mean = _first_finite(policy.get("invalidationExpectedDeltaMean"))
    item["invalidationExpectedDeltaMean"] = float(delta_mean) if delta_mean is not None else None
    item["invalidationPolicyNote"] = str(policy.get("invalidationPolicyNote") or "")
    item["riskMode"] = str(risk_mode)
    item["invalidationRecommendedAction"] = _resolve_invalidation_recommended_action(
        policy=policy,
        risk_mode=risk_mode,
    )


def _sort_items(items: list[dict], direction: RankDir) -> list[dict]:
    def _liquidity(value: float | None) -> float:
        return float(value) if value is not None else -1.0

    def _sort_key(item: dict) -> tuple:
        change = item.get("changePct")
        missing = change is None or not isinstance(change, (int, float)) or not math.isfinite(change)
        liq = _liquidity(item.get("liquidity20d"))
        if direction == "up":
            return (missing, -(change or 0.0), -liq, item.get("code", ""))
        return (missing, (change or 0.0), -liq, item.get("code", ""))

    return sorted(items, key=_sort_key)


def _finite_float(value: object) -> float | None:
    if not isinstance(value, (int, float)):
        return None
    casted = float(value)
    if not math.isfinite(casted):
        return None
    return casted


def _first_finite(*values: object) -> float | None:
    for value in values:
        resolved = _finite_float(value)
        if resolved is not None:
            return resolved
    return None


def _calc_candlestick_pattern_bonus(
    item: dict[str, Any],
    *,
    direction: RankDir,
) -> tuple[float, dict[str, float]]:
    rules: tuple[tuple[str, float], ...] = _ENTRY_CANDLE_PATTERN_WEIGHTS_UP if direction == "up" else ()
    details: dict[str, float] = {}
    bonus = 0.0
    for key, weight in rules:
        active = bool((_first_finite(item.get(key)) or 0.0) >= 0.5)
        contribution = float(weight) if active else 0.0
        details[key] = contribution
        bonus += contribution
    if direction == "up":
        market_risk_off = bool(item.get("marketRiskOff"))
        for combo_key, left_key, right_key, weight in _ENTRY_CANDLE_PATTERN_COMBO_WEIGHTS_UP:
            left_active = bool((_first_finite(item.get(left_key)) or 0.0) >= 0.5)
            right_active = bool((_first_finite(item.get(right_key)) or 0.0) >= 0.5)
            combo_active = bool(market_risk_off and left_active and right_active)
            contribution = float(weight) if combo_active else 0.0
            details[combo_key] = contribution
            bonus += contribution
    return float(bonus), details


def _is_non_increasing_curve(
    value_5d: float | None,
    value_10d: float | None,
    value_20d: float | None,
    *,
    eps: float = _ENTRY_PROB_CURVE_EPS,
) -> bool:
    points = [
        value
        for value in (value_5d, value_10d, value_20d)
        if isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    if len(points) < 2:
        return True
    for left, right in zip(points, points[1:]):
        if float(left) + float(eps) < float(right):
            return False
    return True


def _sanitize_rank_item_for_json(item: dict) -> dict:
    sanitized: dict = {}
    for key, value in item.items():
        if isinstance(value, float) and not math.isfinite(value):
            sanitized[key] = None
            continue
        sanitized[key] = value
    return sanitized


def _freshness_days_from_asof(as_of_value: Any, *, now_ymd: int | None = None) -> int | None:
    as_of_ymd = _iso_date_to_int(str(as_of_value or ""))
    if as_of_ymd is None:
        return None
    try:
        base = datetime.strptime(str(as_of_ymd), "%Y%m%d")
    except ValueError:
        return None
    anchor = (
        int(now_ymd)
        if now_ymd is not None
        else int((datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d"))
    )
    try:
        today = datetime.strptime(str(anchor), "%Y%m%d")
    except ValueError:
        return None
    return max(0, int((today.date() - base.date()).days))


def _attach_quality_flags(
    items: list[dict],
    *,
    mode: RankMode,
    direction: RankDir,
    now_ymd: int | None = None,
) -> list[dict]:
    enriched: list[dict] = []
    for base in items:
        item = dict(base)
        flags: list[str] = []
        if mode != "rule":
            prob_side = _first_finite(
                item.get("mlPUp") if direction == "up" else item.get("mlPDown"),
                item.get("mlPUpShort") if direction == "up" else item.get("mlPDownShort"),
                item.get("probSide"),
            )
            if prob_side is None:
                flags.append("missing_ml_prob")
            if _first_finite(item.get("mlEv20Net"), item.get("mlEvShortNet")) is None:
                flags.append("missing_ml_ev")
            if not str(item.get("modelVersion") or "").strip():
                flags.append("missing_model_version")
        if bool(item.get("entryQualifiedByFallback")) or str(item.get("entryQualifiedFallbackStage") or "").strip():
            flags.append("fallback_rule_applied")
        freshness_days = _freshness_days_from_asof(item.get("asOf"), now_ymd=now_ymd)
        if isinstance(freshness_days, int) and freshness_days >= 5:
            flags.append("low_freshness")
        if bool(item.get("entryQualified")) is False:
            flags.append("entry_not_qualified")
        if not flags:
            flags.append("ok")
        item["qualityFlags"] = sorted(set(flags))
        enriched.append(item)
    return enriched


def _attach_swing_fields(
    items: list[dict],
    *,
    direction: RankDir,
) -> list[dict]:
    enriched: list[dict] = []
    for base in items:
        item = dict(base)
        as_of_ymd = _iso_date_to_int(item.get("asOf"))
        p_up = _first_finite(item.get("mlPUp"), item.get("mlPUpShort"))
        p_down = _first_finite(item.get("mlPDown"), item.get("mlPDownShort"))
        p_turn_up = _first_finite(item.get("mlPTurnUp"))
        p_turn_down = _first_finite(item.get("mlPTurnDown"), item.get("mlPTurnDownShort"))
        ev20_net = _first_finite(item.get("mlEv20Net"), item.get("mlEvShortNet"))
        close = _first_finite(item.get("close"))
        atr_pct = _first_finite(item.get("atrPct"))
        liquidity20d = _first_finite(item.get("liquidity20d"))
        playbook_bonus = _first_finite(item.get("playbookScoreBonus"))
        short_score = _first_finite(
            item.get("shortScore"),
            item.get("shortCandidateScore"),
            item.get("shortPriorityScore"),
        )
        if short_score is None:
            a_score = _first_finite(item.get("aScore"), item.get("aCandidateScore"))
            b_score = _first_finite(item.get("bScore"), item.get("bCandidateScore"))
            if a_score is not None or b_score is not None:
                short_score = float((a_score or 0.0) + (b_score or 0.0))
        setup_type = str(item.get("setupType") or "watch")
        swing_payload = swing_plan_service.build_swing_plan(
            code=str(item.get("code") or ""),
            as_of_ymd=as_of_ymd,
            close=close,
            p_up=p_up,
            p_down=p_down,
            p_turn_up=p_turn_up,
            p_turn_down=p_turn_down,
            ev20_net=ev20_net,
            long_setup_type=setup_type,
            short_setup_type=setup_type,
            playbook_bonus_long=playbook_bonus,
            playbook_bonus_short=playbook_bonus,
            short_score=short_score,
            atr_pct=atr_pct,
            liquidity20d=liquidity20d,
            decision_tone="up" if direction == "up" else "down",
            hold_days_long=_first_finite(item.get("recommendedHoldDays")),
            hold_days_short=_first_finite(item.get("recommendedHoldDays")),
        )
        diagnostics = (
            swing_payload.get("diagnostics")
            if isinstance(swing_payload, dict) and isinstance(swing_payload.get("diagnostics"), dict)
            else {}
        )
        side_key = "long" if direction == "up" else "short"
        side_eval = diagnostics.get(side_key) if isinstance(diagnostics, dict) else {}
        long_eval = diagnostics.get("long") if isinstance(diagnostics, dict) else {}
        short_eval = diagnostics.get("short") if isinstance(diagnostics, dict) else {}
        score = _first_finite((side_eval or {}).get("score"))
        qualified = bool((side_eval or {}).get("qualified"))
        reasons = (side_eval or {}).get("reasons")
        item["swingScore"] = float(score) if score is not None else None
        item["swingQualified"] = bool(qualified)
        item["swingSide"] = side_key if qualified else "none"
        item["swingReasons"] = [str(v) for v in reasons] if isinstance(reasons, list) else []
        # Preserve quick view for consumers that need best side context.
        item["swingLongScore"] = _first_finite((long_eval or {}).get("score"))
        item["swingShortScore"] = _first_finite((short_eval or {}).get("score"))
        item["swingPlanPreview"] = swing_payload.get("plan") if isinstance(swing_payload, dict) else None
        enriched.append(item)
    return enriched


def _fetch_daily_rows(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    return conn.execute(
        """
        SELECT code, date, o, h, l, c, v
        FROM (
            SELECT
                code,
                date,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
        )
        WHERE rn <= ?
        ORDER BY code, date
        """,
        [_DAILY_LIMIT],
    ).fetchall()


def _fetch_daily_rows_asof(conn: duckdb.DuckDBPyConnection, as_of_int: int) -> list[tuple]:
    as_of_epoch = _as_of_int_to_utc_epoch(as_of_int)
    return conn.execute(
        """
        SELECT code, date, o, h, l, c, v
        FROM (
            SELECT
                code,
                date,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
            FROM daily_bars
            WHERE date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END
        )
        WHERE rn <= ?
        ORDER BY code, date
        """,
        [as_of_epoch, as_of_int, _DAILY_LIMIT],
    ).fetchall()


def _fetch_monthly_rows(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    return conn.execute(
        """
        SELECT code, month, o, h, l, c, v
        FROM (
            SELECT
                code,
                month,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
            FROM monthly_bars
        )
        WHERE rn <= ?
        ORDER BY code, month
        """,
        [_MONTHLY_LIMIT],
    ).fetchall()


def _fetch_monthly_rows_asof(conn: duckdb.DuckDBPyConnection, as_of_int: int) -> list[tuple]:
    as_of_month = int((as_of_int // 100) * 100 + 1)
    as_of_month_epoch = _as_of_month_int_to_utc_epoch(as_of_month)
    return conn.execute(
        """
        SELECT code, month, o, h, l, c, v
        FROM (
            SELECT
                code,
                month,
                o,
                h,
                l,
                c,
                v,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY month DESC) AS rn
            FROM monthly_bars
            WHERE month <= CASE WHEN month >= 1000000000 THEN ? ELSE ? END
        )
        WHERE rn <= ?
        ORDER BY code, month
        """,
        [as_of_month_epoch, as_of_month, _MONTHLY_LIMIT],
    ).fetchall()


def _fetch_names(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    rows = conn.execute("SELECT code, name FROM tickers").fetchall()
    return {row[0]: repair_cp932_mojibake(str(row[1] or row[0])) for row in rows}


def _build_cache() -> tuple[dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]], int | None]:
    with get_conn() as conn:
        latest_pan_daily_asof_int = _resolve_latest_pan_daily_asof_int(conn)
        codes = [row[0] for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
        names = _fetch_names(conn)
        daily_rows = _fetch_daily_rows(conn)
        monthly_rows = _fetch_monthly_rows(conn)

    daily_map: dict[str, list[tuple]] = {}
    for row in daily_rows:
        daily_map.setdefault(row[0], []).append(row[1:])
    try:
        provisional_map = get_provisional_daily_rows_from_spark(codes)
        if provisional_map:
            today_key_jst = int((datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y%m%d"))
            for code, provisional_row in provisional_map.items():
                if not provisional_row or normalize_date_key(provisional_row[0]) != today_key_jst:
                    continue
                daily_map[code] = merge_daily_rows_with_provisional(
                    daily_map.get(code, []),
                    provisional_row,
                )
    except Exception as exc:
        logger.debug("rankings provisional merge skipped: %s", exc)
    market_breadth_state = _calc_market_breadth_state(daily_map)

    monthly_map: dict[str, list[tuple]] = {}
    for row in monthly_rows:
        monthly_map.setdefault(row[0], []).append(row[1:])
    for code in codes:
        monthly_map[code] = merge_monthly_rows_with_daily(
            monthly_map.get(code, []),
            daily_map.get(code, []),
        )

    items_by_tf: dict[tuple[RankTimeframe, RankWhich], list[dict]] = {
        ("D", "latest"): [],
        ("D", "prev"): [],
        ("W", "latest"): [],
        ("W", "prev"): [],
        ("M", "latest"): [],
        ("M", "prev"): [],
    }

    for code in codes:
        daily = daily_map.get(code, [])
        if not daily:
            continue
        last_daily_dt = _parse_date_value(daily[-1][0])
        liquidity = _calc_liquidity_20d(daily)

        daily_closes: list[float] = []
        daily_dates: list[datetime] = []
        for row in daily:
            if row[4] is None:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            daily_closes.append(float(row[4]))
            daily_dates.append(dt)

        weekly = _build_weekly_bars(daily)
        weekly = _drop_incomplete_weekly(weekly, last_daily_dt)
        weekly_closes = [float(item["c"]) for item in weekly]
        weekly_dates = [item["last_date"] for item in weekly]

        monthly = monthly_map.get(code, [])
        monthly_closes: list[float] = []
        monthly_dates: list[datetime] = []
        for row in monthly:
            if row[4] is None:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            monthly_closes.append(float(row[4]))
            monthly_dates.append(dt)

        candle_signals = _calc_triplet_candle_signals(daily)
        v60_signals = _calc_60v_signals(daily)
        weekly_regime = _calc_regime_probs(weekly_closes, lookback=20)
        monthly_regime = _calc_regime_probs(monthly_closes, lookback=12)
        monthly_box = _detect_monthly_body_box(monthly)
        entry_close_for_box = (
            float(daily_closes[-1])
            if daily_closes
            else (float(monthly_closes[-1]) if monthly_closes else None)
        )
        monthly_box_state, monthly_box_pos = _calc_monthly_box_state(
            entry_close=entry_close_for_box,
            box=monthly_box,
        )

        common_fields = {
            "candleBodyRatio": candle_signals.get("candleBodyRatio"),
            "candleUpperWickRatio": candle_signals.get("candleUpperWickRatio"),
            "candleLowerWickRatio": candle_signals.get("candleLowerWickRatio"),
            "candleTripletUp": candle_signals.get("candleTripletUp"),
            "candleTripletDown": candle_signals.get("candleTripletDown"),
            "shootingStarLike": candle_signals.get("shootingStarLike"),
            "bullMarubozu": candle_signals.get("bullMarubozu"),
            "bearMarubozu": candle_signals.get("bearMarubozu"),
            "threeWhiteSoldiers": candle_signals.get("threeWhiteSoldiers"),
            "threeBlackCrows": candle_signals.get("threeBlackCrows"),
            "morningStar": candle_signals.get("morningStar"),
            "bullEngulfing": candle_signals.get("bullEngulfing"),
            "marketBreadthDate": market_breadth_state.get("marketBreadthDate"),
            "marketBreadthDateIso": market_breadth_state.get("marketBreadthDateIso"),
            "marketBreadthAdvRatio": market_breadth_state.get("marketBreadthAdvRatio"),
            "marketBreadthSampleSize": market_breadth_state.get("marketBreadthSampleSize"),
            "marketRiskOff": market_breadth_state.get("marketRiskOff"),
            "marketRiskOn": market_breadth_state.get("marketRiskOn"),
            "marketRegime": market_breadth_state.get("marketRegime"),
            "weeklyBreakoutUpProb": weekly_regime.get("breakoutUpProb"),
            "weeklyBreakoutDownProb": weekly_regime.get("breakoutDownProb"),
            "weeklyRangeProb": weekly_regime.get("rangeProb"),
            "monthlyBreakoutUpProb": monthly_regime.get("breakoutUpProb"),
            "monthlyBreakoutDownProb": monthly_regime.get("breakoutDownProb"),
            "monthlyRangeProb": monthly_regime.get("rangeProb"),
            "monthlyRangeWidth": monthly_regime.get("rangeWidth"),
            "monthlyRangePos": monthly_regime.get("rangePos"),
            "monthlyBoxState": monthly_box_state,
            "monthlyBoxPos": monthly_box_pos,
            "monthlyBoxMonths": _first_finite(monthly_box.get("months")) if monthly_box else None,
            "monthlyBoxRangePct": _first_finite(monthly_box.get("rangePct")) if monthly_box else None,
            "monthlyBoxWild": bool(monthly_box.get("wild")) if monthly_box else None,
            "reclaim60": v60_signals.get("reclaim60"),
            "v60Core": v60_signals.get("v60Core"),
            "v60Strong": v60_signals.get("v60Strong"),
            "cnt60Up": v60_signals.get("cnt60Up"),
            "cnt100Up": v60_signals.get("cnt100Up"),
        }

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                daily_closes, daily_dates, which
            )
            items_by_tf[("D", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                weekly_closes, weekly_dates, which
            )
            items_by_tf[("W", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                monthly_closes, monthly_dates, which
            )
            items_by_tf[("M", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

    cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
    for tf in ("D", "W", "M"):
        for which in ("latest", "prev"):
            items = items_by_tf[(tf, which)]
            for direction in ("up", "down"):
                cache[(tf, which, direction)] = _sort_items(items, direction)
    return cache, latest_pan_daily_asof_int


def _build_cache_asof(conn: duckdb.DuckDBPyConnection, as_of_int: int) -> dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]]:
    codes = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT code
            FROM daily_bars
            WHERE date <= CASE WHEN date >= 1000000000 THEN ? ELSE ? END
            ORDER BY code
            """,
            [_as_of_int_to_utc_epoch(as_of_int), as_of_int],
        ).fetchall()
    ]
    names = _fetch_names(conn)
    daily_rows = _fetch_daily_rows_asof(conn, as_of_int)
    monthly_rows = _fetch_monthly_rows_asof(conn, as_of_int)

    daily_map: dict[str, list[tuple]] = {}
    for row in daily_rows:
        daily_map.setdefault(row[0], []).append(row[1:])
    market_breadth_state = _calc_market_breadth_state(daily_map)
    monthly_map: dict[str, list[tuple]] = {}
    for row in monthly_rows:
        monthly_map.setdefault(row[0], []).append(row[1:])
    for code in codes:
        monthly_map[code] = merge_monthly_rows_with_daily(
            monthly_map.get(code, []),
            daily_map.get(code, []),
        )

    items_by_tf: dict[tuple[RankTimeframe, RankWhich], list[dict]] = {
        ("D", "latest"): [],
        ("D", "prev"): [],
        ("W", "latest"): [],
        ("W", "prev"): [],
        ("M", "latest"): [],
        ("M", "prev"): [],
    }

    for code in codes:
        daily = daily_map.get(code, [])
        if not daily:
            continue
        last_daily_dt = _parse_date_value(daily[-1][0])
        liquidity = _calc_liquidity_20d(daily)
        daily_closes: list[float] = []
        daily_dates: list[datetime] = []
        for row in daily:
            if len(row) < 5:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            daily_closes.append(float(row[4]))
            daily_dates.append(dt)

        weekly = _build_weekly_bars(daily)
        weekly = _drop_incomplete_weekly(weekly, last_daily_dt)
        weekly_closes: list[float] = []
        weekly_dates: list[datetime] = []
        for item in weekly:
            weekly_closes.append(float(item["c"]))
            weekly_dates.append(item["last_date"])

        monthly = monthly_map.get(code, [])
        monthly_closes: list[float] = []
        monthly_dates: list[datetime] = []
        for row in monthly:
            if len(row) < 5:
                continue
            dt = _parse_date_value(row[0]) or last_daily_dt
            if not dt:
                continue
            monthly_closes.append(float(row[4]))
            monthly_dates.append(dt)

        candle_signals = _calc_triplet_candle_signals(daily)
        v60_signals = _calc_60v_signals(daily)
        weekly_regime = _calc_regime_probs(weekly_closes, lookback=20)
        monthly_regime = _calc_regime_probs(monthly_closes, lookback=12)
        monthly_box = _detect_monthly_body_box(monthly)
        entry_close_for_box = (
            float(daily_closes[-1])
            if daily_closes
            else (float(monthly_closes[-1]) if monthly_closes else None)
        )
        monthly_box_state, monthly_box_pos = _calc_monthly_box_state(
            entry_close=entry_close_for_box,
            box=monthly_box,
        )

        common_fields = {
            "candleBodyRatio": candle_signals.get("candleBodyRatio"),
            "candleUpperWickRatio": candle_signals.get("candleUpperWickRatio"),
            "candleLowerWickRatio": candle_signals.get("candleLowerWickRatio"),
            "candleTripletUp": candle_signals.get("candleTripletUp"),
            "candleTripletDown": candle_signals.get("candleTripletDown"),
            "shootingStarLike": candle_signals.get("shootingStarLike"),
            "bullMarubozu": candle_signals.get("bullMarubozu"),
            "bearMarubozu": candle_signals.get("bearMarubozu"),
            "threeWhiteSoldiers": candle_signals.get("threeWhiteSoldiers"),
            "threeBlackCrows": candle_signals.get("threeBlackCrows"),
            "morningStar": candle_signals.get("morningStar"),
            "bullEngulfing": candle_signals.get("bullEngulfing"),
            "marketBreadthDate": market_breadth_state.get("marketBreadthDate"),
            "marketBreadthDateIso": market_breadth_state.get("marketBreadthDateIso"),
            "marketBreadthAdvRatio": market_breadth_state.get("marketBreadthAdvRatio"),
            "marketBreadthSampleSize": market_breadth_state.get("marketBreadthSampleSize"),
            "marketRiskOff": market_breadth_state.get("marketRiskOff"),
            "marketRiskOn": market_breadth_state.get("marketRiskOn"),
            "marketRegime": market_breadth_state.get("marketRegime"),
            "weeklyBreakoutUpProb": weekly_regime.get("breakoutUpProb"),
            "weeklyBreakoutDownProb": weekly_regime.get("breakoutDownProb"),
            "weeklyRangeProb": weekly_regime.get("rangeProb"),
            "monthlyBreakoutUpProb": monthly_regime.get("breakoutUpProb"),
            "monthlyBreakoutDownProb": monthly_regime.get("breakoutDownProb"),
            "monthlyRangeProb": monthly_regime.get("rangeProb"),
            "monthlyRangeWidth": monthly_regime.get("rangeWidth"),
            "monthlyRangePos": monthly_regime.get("rangePos"),
            "monthlyBoxState": monthly_box_state,
            "monthlyBoxPos": monthly_box_pos,
            "monthlyBoxMonths": _first_finite(monthly_box.get("months")) if monthly_box else None,
            "monthlyBoxRangePct": _first_finite(monthly_box.get("rangePct")) if monthly_box else None,
            "monthlyBoxWild": bool(monthly_box.get("wild")) if monthly_box else None,
            "reclaim60": v60_signals.get("reclaim60"),
            "v60Core": v60_signals.get("v60Core"),
            "v60Strong": v60_signals.get("v60Strong"),
            "cnt60Up": v60_signals.get("cnt60Up"),
            "cnt100Up": v60_signals.get("cnt100Up"),
        }

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                daily_closes, daily_dates, which
            )
            items_by_tf[("D", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                weekly_closes, weekly_dates, which
            )
            items_by_tf[("W", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

        for which in ("latest", "prev"):
            change_pct, change_abs, as_of, close, prev_close = _compute_change(
                monthly_closes, monthly_dates, which
            )
            items_by_tf[("M", which)].append(
                {
                    "code": code,
                    "name": names.get(code, code),
                    "asOf": as_of,
                    "close": close,
                    "prevClose": prev_close,
                    "changePct": change_pct,
                    "changeAbs": change_abs,
                    "liquidity20d": liquidity,
                    **common_fields,
                }
            )

    cache: dict[tuple[RankTimeframe, RankWhich, RankDir], list[dict]] = {}
    for tf in ("D", "W", "M"):
        for which in ("latest", "prev"):
            items = items_by_tf[(tf, which)]
            for direction in ("up", "down"):
                cache[(tf, which, direction)] = _sort_items(items, direction)
    return cache


def _resolve_prediction_dt(conn: duckdb.DuckDBPyConnection, items: list[dict]) -> int | None:
    as_of_values = sorted(
        {v for v in (_iso_date_to_int(item.get("asOf")) for item in items) if v is not None}
    )
    if as_of_values:
        row = conn.execute(
            """
            SELECT MAX(dt)
            FROM ml_pred_20d
            WHERE (
                CASE
                    WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                    WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt / 1000), '%Y%m%d') AS INTEGER)
                    WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END
            ) <= ?
            """,
            [as_of_values[-1]],
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    row = conn.execute("SELECT MAX(dt) FROM ml_pred_20d").fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _load_ml_pred_map(
    conn: duckdb.DuckDBPyConnection,
    pred_dt: int,
) -> tuple[dict[str, dict], str | None]:
    cols = conn.execute("PRAGMA table_info('ml_pred_20d')").fetchall()
    names = {str(row[1]).lower() for row in cols}
    p_up_5_expr = "p_up_5" if "p_up_5" in names else "NULL AS p_up_5"
    p_up_10_expr = "p_up_10" if "p_up_10" in names else "NULL AS p_up_10"
    turn_up_expr = "p_turn_up" if "p_turn_up" in names else "NULL AS p_turn_up"
    turn_down_expr = "p_turn_down" if "p_turn_down" in names else "NULL AS p_turn_down"
    turn_down_5_expr = "p_turn_down_5" if "p_turn_down_5" in names else "NULL AS p_turn_down_5"
    turn_down_10_expr = "p_turn_down_10" if "p_turn_down_10" in names else "NULL AS p_turn_down_10"
    turn_down_20_expr = "p_turn_down_20" if "p_turn_down_20" in names else "NULL AS p_turn_down_20"
    p_down_expr = "p_down" if "p_down" in names else "NULL AS p_down"
    rank_up_expr = "rank_up_20" if "rank_up_20" in names else "NULL AS rank_up_20"
    rank_down_expr = "rank_down_20" if "rank_down_20" in names else "NULL AS rank_down_20"
    ev5_net_expr = "ev5_net" if "ev5_net" in names else "NULL AS ev5_net"
    ev10_net_expr = "ev10_net" if "ev10_net" in names else "NULL AS ev10_net"
    rows = conn.execute(
        f"""
        SELECT
            code,
            p_up,
            {p_up_5_expr},
            {p_up_10_expr},
            {turn_up_expr},
            {turn_down_expr},
            {turn_down_5_expr},
            {turn_down_10_expr},
            {turn_down_20_expr},
            {p_down_expr},
            {rank_up_expr},
            {rank_down_expr},
            ret_pred20,
            ev20,
            ev20_net,
            {ev5_net_expr},
            {ev10_net_expr},
            model_version
        FROM ml_pred_20d
        WHERE dt = ?
        """,
        [pred_dt],
    ).fetchall()
    pred_map = {
        str(row[0]): {
            "p_up": float(row[1]) if row[1] is not None else None,
            "p_up_5": float(row[2]) if row[2] is not None else None,
            "p_up_10": float(row[3]) if row[3] is not None else None,
            "p_turn_up": float(row[4]) if row[4] is not None else None,
            "p_turn_down": float(row[5]) if row[5] is not None else None,
            "p_turn_down_5": float(row[6]) if row[6] is not None else None,
            "p_turn_down_10": float(row[7]) if row[7] is not None else None,
            "p_turn_down_20": float(row[8]) if row[8] is not None else None,
            "p_down": float(row[9]) if row[9] is not None else None,
            "rank_up_20": float(row[10]) if row[10] is not None else None,
            "rank_down_20": float(row[11]) if row[11] is not None else None,
            "ret_pred20": float(row[12]) if row[12] is not None else None,
            "ev20": float(row[13]) if row[13] is not None else None,
            "ev20_net": float(row[14]) if row[14] is not None else None,
            "ev5_net": float(row[15]) if row[15] is not None else None,
            "ev10_net": float(row[16]) if row[16] is not None else None,
            "model_version": row[17],
        }
        for row in rows
    }
    model_version = None
    for item in pred_map.values():
        model_version = item.get("model_version")
        if model_version:
            break
    return pred_map, model_version


def _resolve_monthly_prediction_dt(conn: duckdb.DuckDBPyConnection, items: list[dict]) -> int | None:
    if not _table_exists(conn, "ml_monthly_pred"):
        return None
    as_of_month_values = sorted(
        {
            month
            for month in (
                _to_month_start_int(_iso_date_to_int(item.get("asOf")))
                for item in items
            )
            if month is not None
        }
    )
    if as_of_month_values:
        row = conn.execute(
            """
            SELECT MAX(dt)
            FROM ml_monthly_pred
            WHERE (
                CASE
                    WHEN dt BETWEEN 190001 AND 209912 THEN dt
                    WHEN dt BETWEEN 19000101 AND 20991231 THEN CAST(dt / 100 AS INTEGER)
                    WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt / 1000), '%Y%m') AS INTEGER)
                    WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m') AS INTEGER)
                    ELSE NULL
                END
            ) <= ?
            """,
            [as_of_month_values[-1]],
        ).fetchone()
        if row and row[0] is not None:
            return int(row[0])
    row = conn.execute("SELECT MAX(dt) FROM ml_monthly_pred").fetchone()
    if row and row[0] is not None:
        return int(row[0])
    return None


def _load_monthly_pred_map(
    conn: duckdb.DuckDBPyConnection,
    pred_dt: int,
) -> tuple[dict[str, dict], str | None]:
    if not _table_exists(conn, "ml_monthly_pred"):
        return {}, None
    rows = conn.execute(
        """
        SELECT
            code,
            p_abs_big,
            p_up_given_big,
            p_up_big,
            p_down_big,
            score_up,
            score_down,
            model_version,
            n_train_abs,
            n_train_dir
        FROM ml_monthly_pred
        WHERE dt = ?
        """,
        [pred_dt],
    ).fetchall()
    pred_map = {
        str(row[0]): {
            "p_abs_big": float(row[1]) if row[1] is not None else None,
            "p_up_given_big": float(row[2]) if row[2] is not None else None,
            "p_up_big": float(row[3]) if row[3] is not None else None,
            "p_down_big": float(row[4]) if row[4] is not None else None,
            "score_up": float(row[5]) if row[5] is not None else None,
            "score_down": float(row[6]) if row[6] is not None else None,
            "model_version": row[7],
            "n_train_abs": int(row[8]) if row[8] is not None else None,
            "n_train_dir": int(row[9]) if row[9] is not None else None,
        }
        for row in rows
    }
    model_version = None
    for item in pred_map.values():
        model_version = item.get("model_version")
        if model_version:
            break
    return pred_map, model_version


def _clamp_monthly_gate(value: float | None, *, low: float, high: float) -> float:
    if value is None or not math.isfinite(float(value)):
        return float(low)
    return float(max(low, min(high, float(value))))


def _default_monthly_ret20_lookup() -> dict[str, dict[str, Any]]:
    return {
        "up": {"baseline_rate": 0.03, "bins": []},
        "down": {"baseline_rate": 0.02, "bins": []},
    }


def _sanitize_monthly_ret20_lookup_dir(raw: object, fallback_baseline: float) -> dict[str, Any]:
    baseline = fallback_baseline
    bins: list[dict[str, float]] = []
    if isinstance(raw, dict):
        baseline = _clamp_monthly_gate(_first_finite(raw.get("baseline_rate")), low=0.0, high=1.0)
        raw_bins = raw.get("bins")
        if isinstance(raw_bins, list):
            for item in raw_bins:
                if not isinstance(item, dict):
                    continue
                low = _first_finite(item.get("min_prob"))
                high = _first_finite(item.get("max_prob"))
                rate = _first_finite(item.get("event_rate"))
                samples = _first_finite(item.get("samples"))
                if (
                    low is None
                    or high is None
                    or rate is None
                    or high < low
                ):
                    continue
                bins.append(
                    {
                        "min_prob": float(max(0.0, min(1.0, low))),
                        "max_prob": float(max(0.0, min(1.0, high))),
                        "event_rate": float(max(0.0, min(1.0, rate))),
                        "samples": float(samples) if samples is not None else 0.0,
                    }
                )
    bins = sorted(bins, key=lambda row: (row.get("min_prob", 0.0), row.get("max_prob", 0.0)))
    running = 0.0
    for row in bins:
        running = max(running, float(row.get("event_rate") or 0.0))
        row["event_rate"] = float(running)
    return {
        "baseline_rate": float(max(0.0, min(1.0, baseline))),
        "bins": bins,
    }


def _load_monthly_gate_recommendation(
    conn: duckdb.DuckDBPyConnection,
    model_version: str | None,
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, Any]]]:
    default = {
        "up": {
            "abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT),
            "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT),
        },
        "down": {
            "abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT),
            "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT),
        },
    }
    default_ret20_lookup = _default_monthly_ret20_lookup()
    if not model_version or not _table_exists(conn, "ml_monthly_model_registry"):
        return default, default_ret20_lookup
    row = conn.execute(
        """
        SELECT metrics_json
        FROM ml_monthly_model_registry
        WHERE model_version = ?
        LIMIT 1
        """,
        [model_version],
    ).fetchone()
    if not row or row[0] is None:
        return default, default_ret20_lookup
    try:
        metrics_json = json.loads(str(row[0]))
    except Exception:
        return default, default_ret20_lookup
    if not isinstance(metrics_json, dict):
        return default, default_ret20_lookup
    rec = metrics_json.get("gate_recommendation")
    ret20_raw = metrics_json.get("ret20_lookup")
    if not isinstance(rec, dict):
        rec = {}
    out = dict(default)
    for direction in ("up", "down"):
        raw = rec.get(direction)
        if not isinstance(raw, dict):
            continue
        out[direction] = {
            "abs_gate": _clamp_monthly_gate(
                _first_finite(raw.get("abs_gate")),
                low=_MONTHLY_ABS_GATE_MIN,
                high=0.60,
            ),
            "side_gate": _clamp_monthly_gate(
                _first_finite(raw.get("side_gate")),
                low=_MONTHLY_SIDE_GATE_MIN,
                high=0.60,
            ),
        }
        target20_gate = _first_finite(raw.get("target20_gate"))
        if target20_gate is not None:
            out[direction]["target20_gate"] = _clamp_monthly_gate(
                target20_gate,
                low=0.02,
                high=0.60,
            )
    ret20_lookup = dict(default_ret20_lookup)
    if isinstance(ret20_raw, dict):
        ret20_lookup["up"] = _sanitize_monthly_ret20_lookup_dir(ret20_raw.get("up"), 0.03)
        ret20_lookup["down"] = _sanitize_monthly_ret20_lookup_dir(ret20_raw.get("down"), 0.02)
    return out, ret20_lookup


def _estimate_monthly_side20_probability(
    prob_side: float | None,
    lookup_dir: dict[str, Any],
) -> float | None:
    if prob_side is None or not math.isfinite(float(prob_side)):
        return None
    p = float(max(0.0, min(1.0, prob_side)))
    baseline = _first_finite(lookup_dir.get("baseline_rate")) or 0.0
    raw_bins = lookup_dir.get("bins")
    bins = raw_bins if isinstance(raw_bins, list) else []
    fallback = baseline * 0.5 + 0.20 * p
    for idx, row in enumerate(bins):
        if not isinstance(row, dict):
            continue
        low = _first_finite(row.get("min_prob"))
        high = _first_finite(row.get("max_prob"))
        rate = _first_finite(row.get("event_rate"))
        if low is None or high is None or rate is None:
            continue
        in_bin = (p >= low and p < high) if idx < len(bins) - 1 else (p >= low and p <= high)
        if in_bin:
            mixed = 0.70 * float(rate) + 0.30 * float(fallback)
            return float(max(0.0, min(1.0, mixed)))
    # Conservative fallback when lookup bins are unavailable.
    return float(max(0.0, min(1.0, fallback)))


def _candidate_monthly_target_dt(items: list[dict]) -> int | None:
    as_of_values = sorted(
        {v for v in (_iso_date_to_int(item.get("asOf")) for item in items) if v is not None}
    )
    if not as_of_values:
        return None
    return _to_month_start_int(as_of_values[-1])


def _try_repair_monthly_prediction(*, pred_dt: int | None, items: list[dict]) -> None:
    global _MONTHLY_PRED_REPAIR_LAST_ATTEMPT
    now = datetime.now(timezone.utc)
    if _MONTHLY_PRED_REPAIR_LAST_ATTEMPT is not None:
        elapsed = (now - _MONTHLY_PRED_REPAIR_LAST_ATTEMPT).total_seconds()
        if elapsed < float(_MONTHLY_PRED_REPAIR_COOLDOWN_SEC):
            return
    _MONTHLY_PRED_REPAIR_LAST_ATTEMPT = now
    target_dt = pred_dt if pred_dt is not None else _candidate_monthly_target_dt(items)
    if target_dt is None:
        return
    try:
        from app.backend.services import ml_service

        result = ml_service.predict_monthly_for_dt(dt=int(target_dt))
        logger.info(
            "monthly prediction repaired: target_dt=%s rows=%s model=%s",
            target_dt,
            result.get("rows"),
            result.get("model_version"),
        )
    except Exception as exc:
        logger.warning("monthly prediction repair failed (target_dt=%s): %s", target_dt, exc)


def _calc_monthly_accumulation_score(item: dict, *, direction: RankDir) -> float:
    range_prob = _first_finite(item.get("monthlyRangeProb"))
    range_width = _first_finite(item.get("monthlyRangeWidth"))
    range_pos = _first_finite(item.get("monthlyRangePos"))
    body_ratio = _first_finite(item.get("candleBodyRatio"))
    change_pct = _first_finite(item.get("changePct"))
    score = 0.0
    if range_prob is not None:
        score += 0.35 * max(0.0, min(1.0, (range_prob - 0.45) / 0.35))
    if range_width is not None:
        score += 0.25 * max(0.0, min(1.0, (0.35 - range_width) / 0.25))
    if range_pos is not None:
        if direction == "up":
            pos_term = (0.55 - range_pos) / 0.35
        else:
            pos_term = (range_pos - 0.45) / 0.35
        score += 0.20 * max(0.0, min(1.0, pos_term))
    if body_ratio is not None:
        score += 0.10 * max(0.0, min(1.0, (0.55 - body_ratio) / 0.25))
    if change_pct is not None:
        score += 0.10 * max(0.0, min(1.0, (0.14 - abs(change_pct)) / 0.14))
    return float(max(0.0, min(1.0, score)))


def _calc_monthly_breakout_readiness_score(item: dict, *, direction: RankDir) -> float:
    monthly_breakout_prob = _first_finite(
        item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
    )
    weekly_breakout_prob = _first_finite(
        item.get("weeklyBreakoutUpProb") if direction == "up" else item.get("weeklyBreakoutDownProb")
    )
    candle_triplet = _first_finite(item.get("candleTripletUp") if direction == "up" else item.get("candleTripletDown"))
    monthly_range_prob = _first_finite(item.get("monthlyRangeProb"))
    score = 0.0
    if monthly_breakout_prob is not None:
        score += 0.55 * monthly_breakout_prob
    if weekly_breakout_prob is not None:
        score += 0.25 * weekly_breakout_prob
    if candle_triplet is not None:
        score += 0.20 * candle_triplet
    if (
        monthly_range_prob is not None
        and monthly_range_prob >= 0.70
        and (monthly_breakout_prob is None or monthly_breakout_prob < 0.55)
    ):
        score -= 0.08
    return float(max(0.0, min(1.0, score)))


def _count_monthly_gate_candidates(
    items: list[dict],
    *,
    direction: RankDir,
    abs_gate: float,
    side_gate: float,
) -> int:
    side_key = "mlPUpBig" if direction == "up" else "mlPDownBig"
    count = 0
    for item in items:
        p_abs_big = _first_finite(item.get("mlPAbsBig"))
        prob_side = _first_finite(item.get(side_key))
        liquidity = _first_finite(item.get("liquidity20d"))
        if (
            p_abs_big is not None
            and p_abs_big >= float(abs_gate)
            and prob_side is not None
            and prob_side >= float(side_gate)
            and liquidity is not None
        ):
            count += 1
    return int(count)


def _relax_monthly_gates_for_coverage(
    items: list[dict],
    *,
    direction: RankDir,
    abs_gate: float,
    side_gate: float,
    limit: int,
) -> tuple[float, float]:
    base_abs = _clamp_monthly_gate(abs_gate, low=_MONTHLY_ABS_GATE_MIN, high=0.60)
    base_side = _clamp_monthly_gate(side_gate, low=_MONTHLY_SIDE_GATE_MIN, high=0.60)
    required = int(max(1, min(limit, _MONTHLY_GATE_MIN_CANDIDATES)))
    if _count_monthly_gate_candidates(
        items,
        direction=direction,
        abs_gate=base_abs,
        side_gate=base_side,
    ) >= required:
        return base_abs, base_side
    abs_steps = sorted(
        {float(base_abs), *[float(v) for v in _MONTHLY_ABS_RELAX_STEPS if float(v) <= float(base_abs) + 1e-12]},
        reverse=True,
    )
    side_steps = sorted(
        {float(base_side), *[float(v) for v in _MONTHLY_SIDE_RELAX_STEPS if float(v) <= float(base_side) + 1e-12]},
        reverse=True,
    )
    for abs_step in abs_steps:
        abs_step = _clamp_monthly_gate(abs_step, low=_MONTHLY_ABS_GATE_MIN, high=0.60)
        for side_step in side_steps:
            side_step = _clamp_monthly_gate(side_step, low=_MONTHLY_SIDE_GATE_MIN, high=0.60)
            count = _count_monthly_gate_candidates(
                items,
                direction=direction,
                abs_gate=abs_step,
                side_gate=side_step,
            )
            if count >= required:
                return abs_step, side_step
    return float(_MONTHLY_ABS_GATE_MIN), float(_MONTHLY_SIDE_GATE_MIN)


def _decorate_items_with_monthly_ml(items: list[dict], pred_map: dict[str, dict]) -> list[dict]:
    enriched: list[dict] = []
    for item in items:
        code = str(item.get("code") or "")
        pred = pred_map.get(code) or {}
        p_up_big = _first_finite(pred.get("p_up_big"))
        p_down_big = _first_finite(pred.get("p_down_big"))
        enriched.append(
            {
                **item,
                "mlPAbsBig": _first_finite(pred.get("p_abs_big")),
                "mlPUpBig": p_up_big,
                "mlPDownBig": p_down_big,
                "mlScoreUp1M": _first_finite(pred.get("score_up")),
                "mlScoreDown1M": _first_finite(pred.get("score_down")),
                # Backward compatible fields.
                "mlPUp": p_up_big,
                "mlPDown": p_down_big,
                "mlRankUp": _first_finite(pred.get("score_up")),
                "mlRankDown": _first_finite(pred.get("score_down")),
                "mlEv20Net": (
                    float(p_up_big - p_down_big)
                    if p_up_big is not None and p_down_big is not None
                    else None
                ),
                "modelVersion": pred.get("model_version"),
                "prob5d": None,
                "prob10d": None,
                "prob20d": None,
                "prob5dAligned": None,
                "probCurveAligned": None,
                "horizonAligned": None,
            }
        )
    return enriched


def _apply_monthly_ml_mode(
    items: list[dict],
    *,
    direction: RankDir,
    limit: int,
    risk_mode: RankRiskMode = "balanced",
) -> tuple[list[dict], int | None, str | None]:
    gate_recommendation = {
        "up": {"abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT), "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT)},
        "down": {"abs_gate": float(_MONTHLY_ABS_GATE_DEFAULT), "side_gate": float(_MONTHLY_SIDE_GATE_DEFAULT)},
    }
    ret20_lookup = _default_monthly_ret20_lookup()
    pred_dt: int | None = None
    pred_map: dict[str, dict] = {}
    model_version: str | None = None
    edinet_feature_map: dict[str, dict[str, Any]] = {}
    edinet_flag_applied = _is_edinet_bonus_enabled()
    target_codes = sorted({str(item.get("code") or "").strip() for item in items if str(item.get("code") or "").strip()})
    asof_candidates = [_iso_date_to_int(str(item.get("asOf") or "")) for item in items]
    anchor_asof_ymd = max((value for value in asof_candidates if isinstance(value, int)), default=None)
    if anchor_asof_ymd is None:
        anchor_asof_ymd = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
    try:
        with get_conn() as conn:
            pred_dt = _resolve_monthly_prediction_dt(conn, items)
            if pred_dt is not None:
                pred_map, model_version = _load_monthly_pred_map(conn, pred_dt)
                gate_recommendation, ret20_lookup = _load_monthly_gate_recommendation(conn, model_version)
            if target_codes:
                edinet_feature_map = load_edinet_rank_features(conn, target_codes, anchor_asof_ymd)
    except Exception as exc:
        logger.debug("monthly ml bootstrap skipped due to DB error: %s", exc)

    if pred_dt is None or not pred_map:
        _try_repair_monthly_prediction(pred_dt=pred_dt, items=items)
        try:
            with get_conn() as conn:
                pred_dt = _resolve_monthly_prediction_dt(conn, items)
                if pred_dt is not None:
                    pred_map, model_version = _load_monthly_pred_map(conn, pred_dt)
                    gate_recommendation, ret20_lookup = _load_monthly_gate_recommendation(conn, model_version)
                if target_codes and not edinet_feature_map:
                    edinet_feature_map = load_edinet_rank_features(conn, target_codes, anchor_asof_ymd)
        except Exception as exc:
            logger.debug("monthly ml repair reload skipped due to DB error: %s", exc)
    if not pred_map:
        for item in items:
            code = str(item.get("code") or "")
            _apply_edinet_defaults(item, flag_applied=edinet_flag_applied)
            edinet_features = edinet_feature_map.get(code) if isinstance(edinet_feature_map.get(code), dict) else None
            if edinet_features:
                for key in _EDINET_ITEM_DEFAULTS.keys():
                    if key in edinet_features:
                        item[key] = edinet_features.get(key)
            item["edinetFeatureFlagApplied"] = bool(edinet_flag_applied)
        return items[:limit], pred_dt, model_version

    enriched = _decorate_items_with_monthly_ml(items, pred_map)
    dir_gate = gate_recommendation.get(direction, {})
    abs_gate, side_gate = _relax_monthly_gates_for_coverage(
        enriched,
        direction=direction,
        abs_gate=_first_finite(dir_gate.get("abs_gate")) or _MONTHLY_ABS_GATE_DEFAULT,
        side_gate=_first_finite(dir_gate.get("side_gate")) or _MONTHLY_SIDE_GATE_DEFAULT,
        limit=limit,
    )
    ret20_dir_lookup = ret20_lookup.get(direction) if isinstance(ret20_lookup, dict) else {}
    if not isinstance(ret20_dir_lookup, dict):
        ret20_dir_lookup = {}
    ret20_baseline = _first_finite(ret20_dir_lookup.get("baseline_rate")) or (0.03 if direction == "up" else 0.02)
    target20_floor = _MONTHLY_TARGET20_GATE_MIN_UP if direction == "up" else _MONTHLY_TARGET20_GATE_MIN_DOWN
    rec_target20_gate = _first_finite(dir_gate.get("target20_gate"))
    if rec_target20_gate is not None:
        target20_gate = float(max(target20_floor, min(0.50, rec_target20_gate)))
        target20_gate_source = "model_backtest"
    else:
        target20_gate = float(max(target20_floor, min(0.35, ret20_baseline * 2.8)))
        target20_gate_source = "baseline"
    qualified: list[dict] = []
    by_code: dict[str, dict] = {}
    research_prior = _load_research_prior_snapshot()
    for item in enriched:
        code = str(item.get("code") or "")
        by_code[code] = item
        _apply_edinet_defaults(item, flag_applied=edinet_flag_applied)
        edinet_features = edinet_feature_map.get(code) if isinstance(edinet_feature_map.get(code), dict) else None
        if edinet_features:
            for key in _EDINET_ITEM_DEFAULTS.keys():
                if key in edinet_features:
                    item[key] = edinet_features.get(key)
        item["edinetFeatureFlagApplied"] = bool(edinet_flag_applied)
        edinet_data_score = _first_finite(item.get("edinetDataScore"))
        edinet_metric_count = _first_finite(item.get("edinetMetricCount")) or 0.0
        edinet_coverage = float(max(0.0, min(1.0, float(edinet_metric_count) / 3.0)))
        bonus_core = 0.0
        if edinet_data_score is not None and edinet_coverage > 0:
            bonus_core = float((float(edinet_data_score) - 0.5) * _EDINET_SCORE_BONUS_SCALE * edinet_coverage)
        edinet_bonus = float(bonus_core if direction == "up" else -bonus_core)
        item["edinetScoreBonus"] = float(edinet_bonus)
        p_abs_big = _first_finite(item.get("mlPAbsBig"))
        prob_side = _first_finite(item.get("mlPUpBig") if direction == "up" else item.get("mlPDownBig"))
        score_side = _first_finite(item.get("mlScoreUp1M") if direction == "up" else item.get("mlScoreDown1M"))
        monthly_breakout_prob = _first_finite(
            item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
        )
        monthly_range_prob = _first_finite(item.get("monthlyRangeProb"))
        p_side20 = _estimate_monthly_side20_probability(prob_side, ret20_dir_lookup)
        accumulation_score = _calc_monthly_accumulation_score(item, direction=direction)
        breakout_readiness = _calc_monthly_breakout_readiness_score(item, direction=direction)
        monthly_range_pos = _first_finite(item.get("monthlyRangePos"))
        monthly_box_state = str(item.get("monthlyBoxState") or "")
        monthly_box_months = _first_finite(item.get("monthlyBoxMonths"))
        cnt60_up = _first_finite(item.get("cnt60Up"))
        cnt100_up = _first_finite(item.get("cnt100Up"))
        dist_ma20_signed = _first_finite(item.get("distMa20Signed"))
        trend_up_strict = bool(item.get("trendUpStrict"))
        trend_down_strict = bool(item.get("trendDownStrict"))
        candlestick_pattern_bonus, candlestick_pattern_bonus_details = _calc_candlestick_pattern_bonus(
            item,
            direction=direction,
        )
        v60_strong = _first_finite(item.get("v60Strong"))
        shape_patterns = _calc_shape_pattern_flags(
            direction=direction,
            trend_up_strict=trend_up_strict,
            trend_down_strict=trend_down_strict,
            monthly_box_state=monthly_box_state,
            monthly_box_months=monthly_box_months,
            dist_ma20_signed=dist_ma20_signed,
            cnt60_up=cnt60_up,
            cnt100_up=cnt100_up,
            monthly_range_pos=monthly_range_pos,
            monthly_range_prob=monthly_range_prob,
            monthly_breakout_down_prob=monthly_breakout_prob if direction == "down" else None,
            shooting_star_like=_first_finite(item.get("shootingStarLike")),
            bear_marubozu=_first_finite(item.get("bearMarubozu")),
            three_black_crows=_first_finite(item.get("threeBlackCrows")),
        )
        range_trap_penalty = 0.0
        if (
            monthly_range_prob is not None
            and monthly_range_prob >= 0.75
            and breakout_readiness < 0.55
        ):
            range_trap_penalty = 0.03
        p_side20_adj = (
            float(
                max(
                    0.0,
                    min(
                        1.0,
                        (p_side20 if p_side20 is not None else ret20_baseline)
                        + 0.07 * breakout_readiness
                        + 0.05 * accumulation_score
                        - range_trap_penalty,
                    ),
                )
            )
            if (p_side20 is not None or math.isfinite(float(ret20_baseline)))
            else None
        )
        regime_bonus = 0.0
        if monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60:
            regime_bonus += _MONTHLY_REGIME_BONUS
        if (
            monthly_range_prob is not None
            and monthly_range_prob >= 0.70
            and (monthly_breakout_prob is None or monthly_breakout_prob < 0.55)
        ):
            regime_bonus -= _MONTHLY_RANGE_PENALTY
        pattern_bonus = 0.0
        box_bottom_ok = bool(
            monthly_range_prob is not None
            and monthly_range_pos is not None
            and monthly_range_prob >= 0.62
            and (
                (direction == "up" and monthly_range_pos <= 0.38)
                or (direction == "down" and monthly_range_pos >= 0.62)
            )
        )
        ma_streak_balanced = bool(
            direction == "up"
            and cnt60_up is not None
            and cnt100_up is not None
            and cnt60_up >= 30
            and cnt100_up >= 20
        )
        weak_early_pattern = bool(
            direction == "up"
            and cnt60_up is not None
            and cnt100_up is not None
            and cnt60_up < 10
            and cnt100_up < 20
            and monthly_range_pos is not None
            and monthly_range_pos <= 0.45
        )
        if direction == "up":
            pattern_bonus += candlestick_pattern_bonus
            if v60_strong is not None and v60_strong >= 0.5:
                pattern_bonus -= _ENTRY_PENALTY_60V_STRONG
            if ma_streak_balanced:
                pattern_bonus += 0.01
            if weak_early_pattern:
                pattern_bonus -= _ENTRY_PENALTY_WEAK_EARLY_STREAK
            if shape_patterns.get("a1MaturedBreakout"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_A1_MATURED_BREAKOUT
            if shape_patterns.get("a2BoxTrend"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_A2_BOX_TREND
            if shape_patterns.get("a3CapitulationRebound"):
                pattern_bonus += (_ENTRY_BONUS_PATTERN_A3_CAPITULATION_REBOUND * 0.6)
            if shape_patterns.get("s1WeakBreakdown"):
                pattern_bonus -= _ENTRY_PENALTY_PATTERN_S1_WEAK_BREAKDOWN
            if shape_patterns.get("s2WeakBox"):
                pattern_bonus -= _ENTRY_PENALTY_PATTERN_S2_WEAK_BOX
            if shape_patterns.get("s3LateBreakout"):
                pattern_bonus -= _ENTRY_PENALTY_PATTERN_S3_LATE_BREAKOUT
        else:
            if shape_patterns.get("d1ShortBreakdown"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_D1_SHORT_BREAKDOWN
            if shape_patterns.get("d2ShortMixedFar"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_D2_SHORT_MIXED_FAR
            if shape_patterns.get("d3ShortNaBelow"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_D3_SHORT_NA_BELOW
            if shape_patterns.get("d4ShortDoubleTop"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_D4_SHORT_DOUBLE_TOP
            if shape_patterns.get("d5ShortHeadShoulders"):
                pattern_bonus += _ENTRY_BONUS_PATTERN_D5_SHORT_HEAD_SHOULDERS
            if shape_patterns.get("dTrapStackDownFar"):
                pattern_bonus -= _ENTRY_PENALTY_PATTERN_DTRAP_STACKDOWN_FAR
            if shape_patterns.get("dTrapOverheatMomentum"):
                pattern_bonus -= _ENTRY_PENALTY_PATTERN_DTRAP_OVERHEAT_MOMENTUM
            if shape_patterns.get("dTrapTopFakeout"):
                pattern_bonus -= _ENTRY_PENALTY_PATTERN_DTRAP_TOP_FAKEOUT
        if box_bottom_ok:
            pattern_bonus += 0.02
            if weak_early_pattern:
                pattern_bonus -= _ENTRY_PENALTY_BOX_BOTTOM_WEAK

        item["hybridScore"] = float(score_side) if score_side is not None else None
        item["probSide"] = float(prob_side) if prob_side is not None else None
        item["mlP20Side1MRaw"] = float(p_side20) if p_side20 is not None else None
        item["mlP20Side1M"] = float(p_side20_adj) if p_side20_adj is not None else None
        item["accumulationScore"] = float(accumulation_score)
        item["breakoutReadiness"] = float(breakout_readiness)
        item["boxBottomAligned"] = bool(box_bottom_ok)
        item["maStreak60Up"] = float(cnt60_up) if cnt60_up is not None else None
        item["maStreak100Up"] = float(cnt100_up) if cnt100_up is not None else None
        item["maStreakAligned"] = bool(ma_streak_balanced)
        item["weakEarlyPattern"] = bool(weak_early_pattern)
        item["patternA1MaturedBreakout"] = bool(shape_patterns.get("a1MaturedBreakout"))
        item["patternA2BoxTrend"] = bool(shape_patterns.get("a2BoxTrend"))
        item["patternA3CapitulationRebound"] = bool(shape_patterns.get("a3CapitulationRebound"))
        item["patternS1WeakBreakdown"] = bool(shape_patterns.get("s1WeakBreakdown"))
        item["patternS2WeakBox"] = bool(shape_patterns.get("s2WeakBox"))
        item["patternS3LateBreakout"] = bool(shape_patterns.get("s3LateBreakout"))
        item["patternD1ShortBreakdown"] = bool(shape_patterns.get("d1ShortBreakdown"))
        item["patternD2ShortMixedFar"] = bool(shape_patterns.get("d2ShortMixedFar"))
        item["patternD3ShortNaBelow"] = bool(shape_patterns.get("d3ShortNaBelow"))
        item["patternD4ShortDoubleTop"] = bool(shape_patterns.get("d4ShortDoubleTop"))
        item["patternD5ShortHeadShoulders"] = bool(shape_patterns.get("d5ShortHeadShoulders"))
        item["patternDTrapStackDownFar"] = bool(shape_patterns.get("dTrapStackDownFar"))
        item["patternDTrapOverheatMomentum"] = bool(shape_patterns.get("dTrapOverheatMomentum"))
        item["patternDTrapTopFakeout"] = bool(shape_patterns.get("dTrapTopFakeout"))
        item["candlestickPatternBonus"] = float(pattern_bonus)
        item["candlestickPatternBonusDetails"] = candlestick_pattern_bonus_details
        item["v60StrongPenalty"] = bool(direction == "up" and v60_strong is not None and v60_strong >= 0.5)
        item["target20Gate"] = float(target20_gate)
        item["target20GateSource"] = target20_gate_source
        item["target20Qualified"] = bool(
            p_side20_adj is not None
            and p_side20_adj >= target20_gate
        )
        item["entryGateAbs"] = float(abs_gate)
        item["entryGateSide"] = float(side_gate)
        playbook_bonus = _calc_playbook_entry_bonus(
            direction=direction,
            shape_patterns=shape_patterns,
        )
        item["entryScore"] = (
            float(
                0.48 * (score_side if score_side is not None else 0.0)
                + 0.32 * (p_side20_adj if p_side20_adj is not None else ret20_baseline)
                + 0.14 * breakout_readiness
                + 0.06 * accumulation_score
                + regime_bonus
                + pattern_bonus
                + playbook_bonus
            )
            if score_side is not None
            else None
        )
        research_bonus = _calc_research_prior_bonus(
            item=item,
            direction=direction,
            code=code,
            prior_snapshot=research_prior,
        )
        item["playbookScoreBonus"] = float(playbook_bonus)
        if item["entryScore"] is not None:
            bonus_total = float(research_bonus)
            if edinet_flag_applied:
                bonus_total += float(edinet_bonus)
            item["entryScore"] = float(max(0.0, min(1.0, float(item["entryScore"]) + bonus_total)))
        item["monthlyRegimeAligned"] = bool(monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60)
        trend_breakout_ok = bool(
            breakout_readiness >= 0.70
            and prob_side is not None
            and prob_side >= max(float(side_gate), 0.25)
        )
        matured_breakout_ok = bool(
            direction == "up"
            and shape_patterns.get("a1MaturedBreakout")
            and prob_side is not None
            and prob_side >= max(0.22, float(side_gate) * 0.90)
        )
        short_breakdown_ok = bool(
            direction == "down"
            and (
                shape_patterns.get("d1ShortBreakdown")
                or shape_patterns.get("d2ShortMixedFar")
                or shape_patterns.get("d3ShortNaBelow")
                or shape_patterns.get("d4ShortDoubleTop")
                or shape_patterns.get("d5ShortHeadShoulders")
            )
            and prob_side is not None
            and prob_side >= max(0.20, float(side_gate) * 0.88)
        )
        accumulation_ok = bool(
            accumulation_score >= 0.70
            and breakout_readiness >= 0.45
            and prob_side is not None
            and prob_side >= max(0.20, float(side_gate) * 0.85)
        )
        target20_ok = bool(p_side20_adj is not None and p_side20_adj >= target20_gate)
        weak_shape_block = bool(
            (
                direction == "up"
                and (
                    shape_patterns.get("s1WeakBreakdown")
                    or shape_patterns.get("s2WeakBox")
                )
            )
            or (
                direction == "down"
                and (
                    shape_patterns.get("dTrapStackDownFar")
                    or shape_patterns.get("dTrapOverheatMomentum")
                    or shape_patterns.get("dTrapTopFakeout")
                )
            )
        )
        if target20_ok and (trend_breakout_ok or matured_breakout_ok):
            setup_type = "breakout20"
        elif short_breakdown_ok:
            setup_type = "breakdown"
        elif matured_breakout_ok or trend_breakout_ok:
            setup_type = "breakout"
        elif accumulation_ok:
            setup_type = "accumulation"
        else:
            setup_type = "watch"
        item["setupType"] = setup_type
        item["entryQualified"] = bool(
            p_abs_big is not None
            and p_abs_big >= float(abs_gate)
            and prob_side is not None
            and prob_side >= float(side_gate)
            and _first_finite(item.get("liquidity20d")) is not None
            and (target20_ok or trend_breakout_ok or matured_breakout_ok or short_breakdown_ok or accumulation_ok)
            and (not weak_shape_block)
            and (
                direction != "up"
                or not bool(shape_patterns.get("s3LateBreakout"))
            )
        )
        _apply_entry_playbook_fields(
            item,
            direction=direction,
            setup_type=setup_type,
            shape_patterns=shape_patterns,
            risk_mode=risk_mode,
        )
        if item["entryQualified"]:
            qualified.append(item)

    qualified.sort(
        key=lambda item: (
            item.get("entryScore") is None,
            -(item.get("entryScore") or 0.0),
            -(item.get("probSide") or 0.0),
            item.get("code", ""),
        )
    )
    if len(qualified) >= limit:
        return qualified[:limit], pred_dt, model_version

    selected: list[dict] = []
    seen: set[str] = set()
    for item in qualified:
        code = str(item.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        selected.append(item)
    for base in items:
        code = str(base.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        candidate = by_code.get(code)
        if candidate is None:
            candidate = {
                **base,
                "mlPAbsBig": None,
                "mlPUpBig": None,
                "mlPDownBig": None,
                "mlScoreUp1M": None,
                "mlScoreDown1M": None,
                "entryQualified": False,
                "setupType": "watch",
            }
            _apply_edinet_defaults(candidate, flag_applied=edinet_flag_applied)
            edinet_features = (
                edinet_feature_map.get(code)
                if isinstance(edinet_feature_map.get(code), dict)
                else None
            )
            if edinet_features:
                for key in _EDINET_ITEM_DEFAULTS.keys():
                    if key in edinet_features:
                        candidate[key] = edinet_features.get(key)
            candidate["edinetFeatureFlagApplied"] = bool(edinet_flag_applied)
            _apply_entry_playbook_fields(
                candidate,
                direction=direction,
                setup_type=str(candidate.get("setupType") or "watch"),
                shape_patterns={},
                risk_mode=risk_mode,
            )
        selected.append(candidate)
        if len(selected) >= limit:
            break
    return selected[:limit], pred_dt, model_version


def _call_apply_monthly_ml_mode(
    items: list[dict],
    *,
    direction: RankDir,
    limit: int,
    risk_mode: RankRiskMode,
) -> tuple[list[dict], int | None, str | None]:
    try:
        return _apply_monthly_ml_mode(
            items,
            direction=direction,
            limit=limit,
            risk_mode=risk_mode,
        )
    except TypeError as exc:
        if "unexpected keyword argument 'risk_mode'" not in str(exc):
            raise
        return _apply_monthly_ml_mode(
            items,
            direction=direction,
            limit=limit,
        )


def _load_daily_snapshot_map(
    conn: duckdb.DuckDBPyConnection,
    anchor_dt: int,
) -> dict[str, dict]:
    anchor_ymd = _to_yyyymmdd_int(anchor_dt)
    rows = conn.execute(
        """
        WITH latest AS (
            SELECT
                b.code,
                b.date,
                b.c,
                m.ma20,
                m.ma60,
                ROW_NUMBER() OVER (PARTITION BY b.code ORDER BY b.date DESC) AS rn
            FROM daily_bars b
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            WHERE b.date <= CASE WHEN b.date >= 1000000000 THEN ? ELSE ? END
        )
        SELECT
            code,
            MAX(CASE WHEN rn = 1 THEN date END) AS snap_dt,
            MAX(CASE WHEN rn = 1 THEN c END) AS snap_close,
            MAX(CASE WHEN rn = 1 THEN ma20 END) AS snap_ma20,
            MAX(CASE WHEN rn = 1 THEN ma60 END) AS snap_ma60,
            MAX(CASE WHEN rn = 2 THEN c END) AS prev_close,
            MAX(CASE WHEN rn = 2 THEN ma20 END) AS prev_ma20,
            MAX(CASE WHEN rn = 2 THEN ma60 END) AS prev_ma60
        FROM latest
        WHERE rn <= 2
        GROUP BY code
        """,
        [int(anchor_dt), int(anchor_ymd)],
    ).fetchall()
    snapshot_map: dict[str, dict] = {}
    for row in rows:
        code = str(row[0])
        close = float(row[2]) if row[2] is not None else None
        ma20 = float(row[3]) if row[3] is not None else None
        ma60 = float(row[4]) if row[4] is not None else None
        prev_close = float(row[5]) if row[5] is not None else None
        prev_ma20 = float(row[6]) if row[6] is not None else None
        prev_ma60 = float(row[7]) if row[7] is not None else None
        dist_ma20 = None
        dist_ma20_signed = None
        dist_ma60_signed = None
        if close is not None and ma20 is not None and ma20 > 0:
            dist_ma20 = abs(close - ma20) / ma20
            dist_ma20_signed = (close - ma20) / ma20
        if close is not None and ma60 is not None and ma60 > 0:
            dist_ma60_signed = (close - ma60) / ma60
        trend_up = (
            close is not None
            and ma20 is not None
            and ma60 is not None
            and close > ma20 > ma60
        )
        trend_down = (
            close is not None
            and ma20 is not None
            and ma60 is not None
            and close < ma20 < ma60
        )
        ma20_slope = (
            (ma20 - prev_ma20)
            if ma20 is not None and prev_ma20 is not None and math.isfinite(ma20) and math.isfinite(prev_ma20)
            else None
        )
        ma60_slope = (
            (ma60 - prev_ma60)
            if ma60 is not None and prev_ma60 is not None and math.isfinite(ma60) and math.isfinite(prev_ma60)
            else None
        )
        trend_up_strict = bool(
            trend_up
            and isinstance(ma20_slope, (int, float))
            and isinstance(ma60_slope, (int, float))
            and ma20_slope > 0
            and ma60_slope > 0
            and isinstance(dist_ma20_signed, (int, float))
            and dist_ma20_signed >= 0.005
        )
        trend_down_strict = bool(
            trend_down
            and isinstance(ma20_slope, (int, float))
            and isinstance(ma60_slope, (int, float))
            and ma20_slope < 0
            and ma60_slope < 0
            and isinstance(dist_ma20_signed, (int, float))
            and dist_ma20_signed <= -0.005
            and isinstance(dist_ma60_signed, (int, float))
            and dist_ma60_signed <= -0.01
        )
        snapshot_map[code] = {
            "snap_dt": int(row[1]) if row[1] is not None else None,
            "snap_close": close,
            "snap_ma20": ma20,
            "snap_ma60": ma60,
            "prev_close": prev_close,
            "prev_ma20": prev_ma20,
            "prev_ma60": prev_ma60,
            "dist_ma20": dist_ma20,
            "dist_ma20_signed": dist_ma20_signed,
            "dist_ma60_signed": dist_ma60_signed,
            "ma20_slope": ma20_slope,
            "ma60_slope": ma60_slope,
            "trend_up": bool(trend_up),
            "trend_down": bool(trend_down),
            "trend_up_strict": trend_up_strict,
            "trend_down_strict": trend_down_strict,
        }
    return snapshot_map


def _decorate_items_with_ml(
    items: list[dict],
    pred_map: dict[str, dict],
    snapshot_map: dict[str, dict],
) -> list[dict]:
    enriched: list[dict] = []
    for item in items:
        code = str(item.get("code") or "")
        pred = pred_map.get(code) or {}
        snap = snapshot_map.get(code) or {}
        p_up_short = _first_finite(pred.get("p_up_5"), pred.get("p_up_10"), pred.get("p_up"))
        p_down_short = _first_finite(
            pred.get("p_down"),
            (1.0 - p_up_short) if p_up_short is not None else None,
        )
        p_turn_down_short = _first_finite(
            pred.get("p_turn_down_5"),
            pred.get("p_turn_down_10"),
            pred.get("p_turn_down_20"),
            pred.get("p_turn_down"),
        )
        ev_short_net = _first_finite(pred.get("ev5_net"), pred.get("ev10_net"), pred.get("ev20_net"))
        enriched.append(
            {
                **item,
                "mlPUp": pred.get("p_up"),
                "mlPUp5": pred.get("p_up_5"),
                "mlPUp10": pred.get("p_up_10"),
                "mlPUpShort": p_up_short,
                "mlPDownShort": p_down_short,
                "mlPDown": pred.get("p_down"),
                "mlPTurnUp": pred.get("p_turn_up"),
                "mlPTurnDown": pred.get("p_turn_down"),
                "mlPTurnDown5": pred.get("p_turn_down_5"),
                "mlPTurnDown10": pred.get("p_turn_down_10"),
                "mlPTurnDown20": pred.get("p_turn_down_20"),
                "mlRankUp": pred.get("rank_up_20"),
                "mlRankDown": pred.get("rank_down_20"),
                "mlPTurnDownShort": p_turn_down_short,
                "mlRetPred20": pred.get("ret_pred20"),
                "mlEv20": pred.get("ev20"),
                "mlEv20Net": pred.get("ev20_net"),
                "mlEv5Net": pred.get("ev5_net"),
                "mlEv10Net": pred.get("ev10_net"),
                "mlEvShortNet": ev_short_net,
                "modelVersion": pred.get("model_version"),
                "hybridScore": None,
                "entryScore": None,
                "trendUp": snap.get("trend_up"),
                "trendDown": snap.get("trend_down"),
                "trendUpStrict": snap.get("trend_up_strict"),
                "trendDownStrict": snap.get("trend_down_strict"),
                "distMa20": snap.get("dist_ma20"),
                "distMa20Signed": snap.get("dist_ma20_signed"),
                "ma20Slope": snap.get("ma20_slope"),
                "ma60Slope": snap.get("ma60_slope"),
            }
        )
    return enriched


def _percent_rank_desc(values: dict[str, float | None]) -> dict[str, float]:
    pairs = [
        (code, float(value))
        for code, value in values.items()
        if value is not None and isinstance(value, (int, float)) and math.isfinite(float(value))
    ]
    if not pairs:
        return {}
    pairs.sort(key=lambda item: (-item[1], item[0]))
    n = len(pairs)
    if n == 1:
        return {pairs[0][0]: 1.0}
    result: dict[str, float] = {}
    idx = 0
    while idx < n:
        value = pairs[idx][1]
        start = idx
        idx += 1
        while idx < n and pairs[idx][1] == value:
            idx += 1
        end = idx - 1
        avg_rank = ((start + 1) + (end + 1)) / 2.0
        pr = 1.0 - ((avg_rank - 1.0) / (n - 1.0))
        for j in range(start, idx):
            result[pairs[j][0]] = pr
    return result


def _apply_ml_mode(
    items: list[dict],
    *,
    direction: RankDir,
    mode: RankMode,
    limit: int,
    risk_mode: RankRiskMode = "balanced",
) -> tuple[list[dict], int | None, str | None]:
    daily_prob_lookup = _default_daily_prob_lookup()
    try:
        with get_conn() as conn:
            pred_dt = _resolve_prediction_dt(conn, items)
            if pred_dt is None:
                return items[:limit], None, None
            pred_map, model_version = _load_ml_pred_map(conn, pred_dt)
            snapshot_map = _load_daily_snapshot_map(conn, pred_dt)
            daily_prob_lookup = _load_daily_prob_lookup(conn, pred_dt=pred_dt, direction=direction)
    except Exception:
        return items[:limit], None, None

    cfg = load_ml_config()
    enriched = _decorate_items_with_ml(items, pred_map, snapshot_map)
    dynamic_up_gates = get_latest_prob_up_gates() if direction == "up" else None

    def _resolve_up_gate(base_value: float) -> float:
        if direction != "up" or not isinstance(dynamic_up_gates, dict):
            return float(base_value)
        dynamic = _first_finite(dynamic_up_gates.get(risk_mode))
        if dynamic is None:
            return float(base_value)
        blended = 0.5 * float(base_value) + 0.5 * float(dynamic)
        return float(max(0.0, min(1.0, blended)))

    def _prob_up_short(item: dict) -> float | None:
        return _first_finite(item.get("mlPUpShort"), item.get("mlPUp"))

    def _prob_down_short(item: dict) -> float | None:
        if isinstance(item.get("mlPDown"), (int, float)):
            return _first_finite(item.get("mlPDownShort"), item.get("mlPDown"))
        return _first_finite(item.get("mlPDownShort"), 1.0 - float(item.get("mlPUp"))) if isinstance(item.get("mlPUp"), (int, float)) else _first_finite(item.get("mlPDownShort"))

    def _turn_down_short(item: dict) -> float | None:
        return _first_finite(item.get("mlPTurnDownShort"), item.get("mlPTurnDown"))

    def _turn_up_short(item: dict) -> float | None:
        down = _turn_down_short(item)
        if down is not None:
            return max(0.0, min(1.0, 1.0 - down))
        return _first_finite(item.get("mlPTurnUp"))

    def _rank_up(item: dict) -> float | None:
        return _first_finite(item.get("mlRankUp"))

    def _rank_down(item: dict) -> float | None:
        return _first_finite(item.get("mlRankDown"))

    if mode == "ml":
        ml_prob_threshold = float(cfg.p_up_threshold if direction == "up" else cfg.min_prob_down)
        ml_prob_threshold = _resolve_up_gate(ml_prob_threshold)
        selected = select_top_n_ml(
            enriched,
            top_n=int(cfg.top_n),
            p_up_threshold=ml_prob_threshold,
            direction=direction,
        )
        if direction == "up":
            selected.sort(
                key=lambda item: (
                    _rank_up(item) is None,
                    -(_rank_up(item) or 0.0),
                    item.get("mlEvShortNet") is None,
                    -(item.get("mlEvShortNet") or 0.0),
                    -(_prob_up_short(item) or 0.0),
                    item.get("code", ""),
                )
            )
        else:
            selected.sort(
                key=lambda item: (
                    _rank_down(item) is None,
                    -(_rank_down(item) or 0.0),
                    item.get("mlEvShortNet") is None,
                    (item.get("mlEvShortNet") or 0.0),
                    -(_prob_down_short(item) or 0.0),
                    item.get("code", ""),
                )
            )
        return selected[: min(limit, int(cfg.top_n))], pred_dt, model_version

    sign = 1.0 if direction == "up" else -1.0
    prob_min = float(cfg.min_prob_up if direction == "up" else cfg.min_prob_down)
    prob_min = _resolve_up_gate(prob_min)
    prob_gate = prob_min if direction == "up" else max(prob_min, _ENTRY_MIN_PROB_DOWN_STRICT)
    fallback_prob_gate = prob_gate if direction == "up" else max(prob_min, 0.52)
    rule_values = {
        str(item.get("code") or ""): (
            float(item["changePct"]) * sign
            if isinstance(item.get("changePct"), (int, float)) and math.isfinite(float(item["changePct"]))
            else None
        )
        for item in enriched
    }
    ev_values = {
        str(item.get("code") or ""): (
            float(item["mlEvShortNet"]) * sign
            if isinstance(item.get("mlEvShortNet"), (int, float)) and math.isfinite(float(item["mlEvShortNet"]))
            else None
        )
        for item in enriched
    }
    prob_values = {
        str(item.get("code") or ""): (
            _prob_up_short(item)
            if direction == "up"
            else _prob_down_short(item)
        )
        for item in enriched
    }
    calibrated_prob_values = {
        code: _calibrate_daily_probability(prob, daily_prob_lookup)
        for code, prob in prob_values.items()
    }
    rank_values = {
        str(item.get("code") or ""): (
            _rank_up(item)
            if direction == "up"
            else _rank_down(item)
        )
        for item in enriched
    }
    turn_values = {
        str(item.get("code") or ""): (
            _turn_up_short(item)
            if direction == "up"
            else _turn_down_short(item)
        )
        for item in enriched
    }
    turn_opp_values = {
        str(item.get("code") or ""): (
            _turn_down_short(item)
            if direction == "up"
            else _turn_up_short(item)
        )
        for item in enriched
    }
    turn_margin_values = {
        code: (
            (turn_values.get(code) - turn_opp_values.get(code))
            if isinstance(turn_values.get(code), (int, float)) and isinstance(turn_opp_values.get(code), (int, float))
            else None
        )
        for code in {str(item.get("code") or "") for item in enriched}
    }
    rule_rank = _percent_rank_desc(rule_values)
    ev_rank = _percent_rank_desc(ev_values)
    prob_rank = _percent_rank_desc(calibrated_prob_values)
    rank_rank = _percent_rank_desc(rank_values)
    turn_rank = _percent_rank_desc(turn_values)
    turn_margin_rank = _percent_rank_desc(turn_margin_values)
    qualified: list[dict] = []
    fallback: list[dict] = []
    research_prior = _load_research_prior_snapshot()
    base_order = {str(item.get("code") or ""): idx for idx, item in enumerate(enriched)}

    def _base_entry_sort_key(item: dict) -> tuple[Any, ...]:
        code = str(item.get("code") or "")
        entry_score = _first_finite(item.get("entryScore"))
        prob_side = _first_finite(item.get("probSide"))
        hybrid_score = _first_finite(item.get("hybridScore"))
        ev_side_raw = _first_finite(item.get("mlEv20Net"))
        ev_side = (
            ev_side_raw
            if ev_side_raw is None
            else (ev_side_raw if direction == "up" else -ev_side_raw)
        )
        return (
            entry_score is None,
            -(entry_score or 0.0),
            prob_side is None,
            -(prob_side or 0.0),
            hybrid_score is None,
            -(hybrid_score or 0.0),
            ev_side is None,
            -(ev_side or 0.0),
            base_order.get(code, 10**9),
            code,
        )

    def _merge_unique(groups: list[list[dict]]) -> list[dict]:
        merged: list[dict] = []
        seen: set[str] = set()
        for group in groups:
            for item in group:
                code = str(item.get("code") or "")
                if not code or code in seen:
                    continue
                merged.append(item)
                seen.add(code)
        return merged

    for item in enriched:
        code = str(item.get("code") or "")
        item["entryQualifiedByFallback"] = False
        item["entryQualifiedFallbackStage"] = None
        rr = rule_rank.get(code)
        er = ev_rank.get(code)
        pr = prob_rank.get(code)
        rkr = rank_rank.get(code)
        tr = turn_rank.get(code)
        tmr = turn_margin_rank.get(code)
        prob_raw = prob_values.get(code)
        prob = calibrated_prob_values.get(code)
        if prob is None:
            prob = prob_raw
        p_up_5d = _first_finite(item.get("mlPUp5"), item.get("mlPUpShort"), item.get("mlPUp"))
        p_up_10d = _first_finite(item.get("mlPUp10"), item.get("mlPUp"))
        p_up_20d = _first_finite(item.get("mlPUp"))
        p_down_5d = _first_finite(
            item.get("mlPDownShort"),
            (1.0 - p_up_5d) if p_up_5d is not None else None,
            item.get("mlPDown"),
        )
        p_down_10d = _first_finite(
            (1.0 - p_up_10d) if p_up_10d is not None else None,
            item.get("mlPDown"),
            item.get("mlPDownShort"),
        )
        p_down_20d = _first_finite(
            item.get("mlPDown"),
            (1.0 - p_up_20d) if p_up_20d is not None else None,
        )
        prob_5d = p_up_5d if direction == "up" else p_down_5d
        prob_10d = p_up_10d if direction == "up" else p_down_10d
        prob_20d = p_up_20d if direction == "up" else p_down_20d
        turn_risk = _turn_down_short(item) if direction == "up" else _turn_up_short(item)
        tail_risk = p_down_5d if direction == "up" else p_up_5d
        downside_risk = _estimate_daily_downside_risk(
            direction=direction,
            turn_risk=turn_risk,
            tail_prob=tail_risk,
        )
        risk_safety = float(max(0.0, min(1.0, 1.0 - downside_risk)))
        if direction == "up":
            prob_5d_gate = _ENTRY_MIN_PROB_UP_5D
            prob_5d_ok = bool(
                isinstance(prob_5d, (int, float))
                and math.isfinite(float(prob_5d))
                and float(prob_5d) >= prob_5d_gate
            )
            prob_curve_ok = _is_non_increasing_curve(prob_5d, prob_10d, prob_20d)
        else:
            prob_5d_ok = True
            prob_curve_ok = True
        horizon_ok = bool(prob_5d_ok and prob_curve_ok)
        if rr is None or er is None or pr is None:
            item["hybridScore"] = None
            item["entryScore"] = None
            continue
        weighted_core = float(
            _DAILY_SCORE_RULE_WEIGHT * rr
            + _DAILY_SCORE_EV_WEIGHT * er
            + _DAILY_SCORE_PROB_WEIGHT * pr
        )
        base_score_raw = float((1.0 - _DAILY_RISK_WEIGHT) * weighted_core + _DAILY_RISK_WEIGHT * risk_safety)
        rank_weight = float(min(0.8, max(0.0, getattr(cfg, "rank_weight", 0.0))))
        base_score = (
            float((1.0 - rank_weight) * base_score_raw + rank_weight * rkr)
            if rkr is not None
            else base_score_raw
        )
        if mode == "turn":
            if tr is None or tmr is None:
                item["hybridScore"] = None
                item["entryScore"] = None
                continue
            if rkr is not None:
                item["hybridScore"] = float(0.55 * tr + 0.25 * tmr + 0.20 * rkr)
            else:
                item["hybridScore"] = float(0.65 * tr + 0.35 * tmr)
        else:
            turn_weight = float(min(0.7, max(0.0, getattr(cfg, "turn_weight", 0.0))))
            if tr is not None and mode == "hybrid":
                item["hybridScore"] = float((1.0 - turn_weight) * base_score + turn_weight * tr)
            else:
                item["hybridScore"] = base_score
        ev_net = item.get("mlEv20Net")
        ev_ok = (
            isinstance(ev_net, (int, float))
            and math.isfinite(float(ev_net))
            and (
                float(ev_net) >= _ENTRY_MIN_EV_NET_UP
                if direction == "up"
                else float(ev_net) <= _ENTRY_MAX_EV_NET_DOWN
            )
        )
        trend_ok = bool(item.get("trendUp")) if direction == "up" else bool(item.get("trendDownStrict"))
        dist_ma20 = item.get("distMa20")
        dist_ok = (
            isinstance(dist_ma20, (int, float))
            and math.isfinite(float(dist_ma20))
            and float(dist_ma20) <= _ENTRY_MAX_DIST_MA20
        )
        rule_signal = rule_values.get(code)
        rule_ok = bool(
            isinstance(rule_signal, (int, float))
            and math.isfinite(float(rule_signal))
            and (
                float(rule_signal) >= 0.0
                if direction == "up"
                else float(rule_signal) >= _ENTRY_MIN_RULE_SIGNAL_DOWN
            )
        )
        counter_move_ok = bool(
            isinstance(rule_signal, (int, float))
            and math.isfinite(float(rule_signal))
            and (
                True
                if direction == "up"
                else float(rule_signal) >= -_ENTRY_MAX_COUNTER_MOVE_DOWN
            )
        )
        turn_prob = turn_values.get(code)
        turn_opp = turn_opp_values.get(code)
        turn_gate = float(cfg.min_turn_prob_up if direction == "up" else cfg.min_turn_prob_down)
        turn_margin_gate = float(cfg.min_turn_margin)
        turn_ok = bool(
            isinstance(turn_prob, (int, float))
            and math.isfinite(float(turn_prob))
            and float(turn_prob) >= turn_gate
            and (
                not isinstance(turn_opp, (int, float))
                or not math.isfinite(float(turn_opp))
                or (float(turn_prob) - float(turn_opp)) >= turn_margin_gate
            )
        )
        candle_prob = _first_finite(
            item.get("candleTripletUp") if direction == "up" else item.get("candleTripletDown")
        )
        weekly_breakout_prob = _first_finite(
            item.get("weeklyBreakoutUpProb") if direction == "up" else item.get("weeklyBreakoutDownProb")
        )
        monthly_breakout_prob = _first_finite(
            item.get("monthlyBreakoutUpProb") if direction == "up" else item.get("monthlyBreakoutDownProb")
        )
        monthly_range_prob = _first_finite(item.get("monthlyRangeProb"))
        monthly_range_pos = _first_finite(item.get("monthlyRangePos"))
        monthly_box_state = str(item.get("monthlyBoxState") or "")
        monthly_box_months = _first_finite(item.get("monthlyBoxMonths"))
        cnt60_up = _first_finite(item.get("cnt60Up"))
        cnt100_up = _first_finite(item.get("cnt100Up"))
        dist_ma20_signed = _first_finite(item.get("distMa20Signed"))
        ma20_slope = _first_finite(item.get("ma20Slope"))
        ma60_slope = _first_finite(item.get("ma60Slope"))
        candle_shape_bonus, candle_shape_bonus_details = _calc_candlestick_pattern_bonus(
            item,
            direction=direction,
        )
        v60_strong = _first_finite(item.get("v60Strong"))
        short_prob_gate, short_turn_gate = _resolve_short_precision_gates(risk_mode=risk_mode)
        short_prob_ok = bool(
            direction != "down"
            or (
                isinstance(prob, (int, float))
                and math.isfinite(float(prob))
                and float(prob) >= float(short_prob_gate)
            )
        )
        short_turn_ok = bool(
            direction != "down"
            or (
                isinstance(turn_prob, (int, float))
                and math.isfinite(float(turn_prob))
                and float(turn_prob) >= float(short_turn_gate)
            )
        )
        trend_strict_ok = bool(item.get("trendUpStrict")) if direction == "up" else bool(item.get("trendDownStrict"))
        trend_down_strict = bool(item.get("trendDownStrict"))
        shape_patterns = _calc_shape_pattern_flags(
            direction=direction,
            trend_up_strict=bool(item.get("trendUpStrict")),
            trend_down_strict=trend_down_strict,
            monthly_box_state=monthly_box_state,
            monthly_box_months=monthly_box_months,
            dist_ma20_signed=dist_ma20_signed,
            cnt60_up=cnt60_up,
            cnt100_up=cnt100_up,
            monthly_range_pos=monthly_range_pos,
            monthly_range_prob=monthly_range_prob,
            monthly_breakout_down_prob=monthly_breakout_prob if direction == "down" else None,
            shooting_star_like=_first_finite(item.get("shootingStarLike")),
            bear_marubozu=_first_finite(item.get("bearMarubozu")),
            three_black_crows=_first_finite(item.get("threeBlackCrows")),
        )
        short_pattern_strong = bool(
            direction == "down"
            and (
                shape_patterns.get("d1ShortBreakdown")
                or shape_patterns.get("d2ShortMixedFar")
                or shape_patterns.get("d3ShortNaBelow")
                or shape_patterns.get("d4ShortDoubleTop")
                or shape_patterns.get("d5ShortHeadShoulders")
            )
        )
        short_overheat_block = bool(
            direction == "down"
            and bool(item.get("trendUpStrict"))
            and dist_ma20_signed is not None
            and dist_ma20_signed >= _ENTRY_SHORT_OVERHEAT_DIST
            and ma20_slope is not None
            and ma20_slope > 0.0
            and ma60_slope is not None
            and ma60_slope > 0.0
        )
        short_overheat_override = bool(
            short_overheat_block
            and short_pattern_strong
            and isinstance(prob, (int, float))
            and math.isfinite(float(prob))
            and float(prob) >= _ENTRY_SHORT_OVERHEAT_STRONG_PROB
            and isinstance(turn_prob, (int, float))
            and math.isfinite(float(turn_prob))
            and float(turn_prob) >= _ENTRY_SHORT_OVERHEAT_STRONG_TURN
        )
        short_precision_gate = bool(
            direction != "down"
            or (
                short_prob_ok
                and short_turn_ok
                and ((not short_overheat_block) or short_overheat_override)
            )
        )
        short_precision_gate_reason = "ok"
        if direction == "down":
            reason_parts: list[str] = []
            if not short_prob_ok:
                reason_parts.append("p_down")
            if not short_turn_ok:
                reason_parts.append("p_turn_down")
            if short_overheat_block and not short_overheat_override:
                reason_parts.append("overheat_uptrend")
            short_precision_gate_reason = "ok" if not reason_parts else ",".join(reason_parts)
        mtf_strong_alignment = bool(
            trend_strict_ok
            and weekly_breakout_prob is not None
            and weekly_breakout_prob >= 0.56
            and monthly_breakout_prob is not None
            and monthly_breakout_prob >= 0.60
        )
        box_bottom_ok = bool(
            monthly_range_prob is not None
            and monthly_range_pos is not None
            and monthly_range_prob >= 0.62
            and (
                (direction == "up" and monthly_range_pos <= 0.38)
                or (direction == "down" and monthly_range_pos >= 0.62)
            )
        )
        ma_streak_balanced = bool(
            direction == "up"
            and cnt60_up is not None
            and cnt100_up is not None
            and cnt60_up >= 30
            and cnt100_up >= 20
        )
        breakout_stack_streak = bool(
            direction == "up"
            and trend_strict_ok
            and monthly_breakout_prob is not None
            and monthly_breakout_prob >= 0.58
            and cnt60_up is not None
            and cnt60_up >= 30
            and cnt60_up < 100
        )
        weak_early_pattern = bool(
            direction == "up"
            and cnt60_up is not None
            and cnt100_up is not None
            and cnt60_up < 10
            and cnt100_up < 20
            and (not trend_ok)
            and dist_ma20_signed is not None
            and dist_ma20_signed < 0.0
        )
        bonus = 0.0
        if trend_ok:
            bonus += 0.08
        if trend_strict_ok:
            bonus += _ENTRY_BONUS_STRICT_STACK
        if ev_ok:
            bonus += 0.05
        if prob is not None and prob >= (prob_gate + 0.03):
            bonus += 0.04
        if dist_ok:
            bonus += 0.03
        if rule_ok:
            bonus += 0.03
        if turn_ok:
            bonus += 0.07
        if prob is not None and prob >= (prob_gate + 0.08):
            bonus += 0.03
        if candle_prob is not None and candle_prob >= 0.58:
            bonus += 0.03
        if weekly_breakout_prob is not None and weekly_breakout_prob >= 0.56:
            bonus += 0.03
        if monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60:
            bonus += 0.05
        if mtf_strong_alignment:
            bonus += _ENTRY_BONUS_MTF_SYNERGY
        if ma_streak_balanced and trend_ok and dist_ok:
            bonus += _ENTRY_BONUS_MA_STREAK_BALANCED
        if breakout_stack_streak:
            bonus += _ENTRY_BONUS_BREAKOUT_STACK_STREAK
        if box_bottom_ok:
            bonus += _ENTRY_BONUS_BOX_BOTTOM
            if weak_early_pattern:
                bonus -= _ENTRY_PENALTY_BOX_BOTTOM_WEAK
        bonus += candle_shape_bonus
        if weak_early_pattern:
            bonus -= _ENTRY_PENALTY_WEAK_EARLY_STREAK
        if direction == "up":
            if shape_patterns.get("a1MaturedBreakout"):
                bonus += _ENTRY_BONUS_PATTERN_A1_MATURED_BREAKOUT
            if shape_patterns.get("a2BoxTrend"):
                bonus += _ENTRY_BONUS_PATTERN_A2_BOX_TREND
            if shape_patterns.get("a3CapitulationRebound"):
                bonus += _ENTRY_BONUS_PATTERN_A3_CAPITULATION_REBOUND
            if shape_patterns.get("s1WeakBreakdown"):
                bonus -= _ENTRY_PENALTY_PATTERN_S1_WEAK_BREAKDOWN
            if shape_patterns.get("s2WeakBox"):
                bonus -= _ENTRY_PENALTY_PATTERN_S2_WEAK_BOX
            if shape_patterns.get("s3LateBreakout"):
                bonus -= _ENTRY_PENALTY_PATTERN_S3_LATE_BREAKOUT
        else:
            if shape_patterns.get("d1ShortBreakdown"):
                bonus += _ENTRY_BONUS_PATTERN_D1_SHORT_BREAKDOWN
            if shape_patterns.get("d2ShortMixedFar"):
                bonus += _ENTRY_BONUS_PATTERN_D2_SHORT_MIXED_FAR
            if shape_patterns.get("d3ShortNaBelow"):
                bonus += _ENTRY_BONUS_PATTERN_D3_SHORT_NA_BELOW
            if shape_patterns.get("d4ShortDoubleTop"):
                bonus += _ENTRY_BONUS_PATTERN_D4_SHORT_DOUBLE_TOP
            if shape_patterns.get("d5ShortHeadShoulders"):
                bonus += _ENTRY_BONUS_PATTERN_D5_SHORT_HEAD_SHOULDERS
            if shape_patterns.get("dTrapStackDownFar"):
                bonus -= _ENTRY_PENALTY_PATTERN_DTRAP_STACKDOWN_FAR
            if shape_patterns.get("dTrapOverheatMomentum"):
                bonus -= _ENTRY_PENALTY_PATTERN_DTRAP_OVERHEAT_MOMENTUM
            if shape_patterns.get("dTrapTopFakeout"):
                bonus -= _ENTRY_PENALTY_PATTERN_DTRAP_TOP_FAKEOUT
        if direction == "up" and v60_strong is not None and v60_strong >= 0.5:
            bonus -= _ENTRY_PENALTY_60V_STRONG
        if (
            monthly_range_prob is not None
            and monthly_range_prob >= 0.68
            and (monthly_breakout_prob is None or monthly_breakout_prob < 0.55)
        ):
            bonus -= 0.02
        rev_risk_penalty = float(
            max(
                0.0,
                _DAILY_REV_RISK_PENALTY_WEIGHT
                * ((float(turn_risk) - 0.45) / 0.55 if isinstance(turn_risk, (int, float)) else 0.0),
            )
        )
        tail_risk_penalty = float(
            max(
                0.0,
                _DAILY_TAIL_RISK_PENALTY_WEIGHT
                * ((float(tail_risk) - 0.50) / 0.50 if isinstance(tail_risk, (int, float)) else 0.0),
            )
        )
        risk_penalty = float(max(0.0, rev_risk_penalty + tail_risk_penalty))
        rule_signal_norm = 0.5
        if isinstance(rule_signal, (int, float)) and math.isfinite(float(rule_signal)):
            if direction == "up":
                rule_signal_norm = float(max(0.0, min(1.0, (float(rule_signal) + 0.04) / 0.12)))
            else:
                rule_signal_norm = float(max(0.0, min(1.0, ((-float(rule_signal)) + 0.04) / 0.12)))
        hybrid_value = float(item.get("hybridScore") or 0.0)
        prob_value = float(prob) if isinstance(prob, (int, float)) and math.isfinite(float(prob)) else 0.0
        item["entryScore"] = float(
            0.55 * hybrid_value
            + 0.25 * rule_signal_norm
            + 0.20 * prob_value
            + bonus
            - risk_penalty
        )
        playbook_bonus = _calc_playbook_entry_bonus(
            direction=direction,
            shape_patterns=shape_patterns,
        )
        item["playbookScoreBonus"] = float(playbook_bonus)
        item["entryScore"] = float(max(0.0, min(1.0, float(item["entryScore"]) + playbook_bonus)))
        research_bonus = _calc_research_prior_bonus(
            item=item,
            direction=direction,
            code=code,
            prior_snapshot=research_prior,
        )
        item["entryScore"] = float(max(0.0, min(1.0, float(item["entryScore"]) + float(research_bonus))))
        item["evAligned"] = bool(ev_ok)
        item["trendAligned"] = bool(trend_ok)
        item["distOk"] = bool(dist_ok)
        item["ruleAligned"] = bool(rule_ok)
        item["counterMoveOk"] = bool(counter_move_ok)
        item["turnAligned"] = bool(turn_ok)
        item["candleAligned"] = bool(candle_prob is not None and candle_prob >= 0.58)
        item["trendStrictAligned"] = bool(trend_strict_ok)
        item["mtfStrongAligned"] = bool(mtf_strong_alignment)
        item["boxBottomAligned"] = bool(box_bottom_ok)
        item["maStreak60Up"] = float(cnt60_up) if cnt60_up is not None else None
        item["maStreak100Up"] = float(cnt100_up) if cnt100_up is not None else None
        item["maStreakAligned"] = bool(ma_streak_balanced)
        item["breakoutStackStreakAligned"] = bool(breakout_stack_streak)
        item["weakEarlyPattern"] = bool(weak_early_pattern)
        item["patternA1MaturedBreakout"] = bool(shape_patterns.get("a1MaturedBreakout"))
        item["patternA2BoxTrend"] = bool(shape_patterns.get("a2BoxTrend"))
        item["patternA3CapitulationRebound"] = bool(shape_patterns.get("a3CapitulationRebound"))
        item["patternS1WeakBreakdown"] = bool(shape_patterns.get("s1WeakBreakdown"))
        item["patternS2WeakBox"] = bool(shape_patterns.get("s2WeakBox"))
        item["patternS3LateBreakout"] = bool(shape_patterns.get("s3LateBreakout"))
        item["patternD1ShortBreakdown"] = bool(shape_patterns.get("d1ShortBreakdown"))
        item["patternD2ShortMixedFar"] = bool(shape_patterns.get("d2ShortMixedFar"))
        item["patternD3ShortNaBelow"] = bool(shape_patterns.get("d3ShortNaBelow"))
        item["patternD4ShortDoubleTop"] = bool(shape_patterns.get("d4ShortDoubleTop"))
        item["patternD5ShortHeadShoulders"] = bool(shape_patterns.get("d5ShortHeadShoulders"))
        item["patternDTrapStackDownFar"] = bool(shape_patterns.get("dTrapStackDownFar"))
        item["patternDTrapOverheatMomentum"] = bool(shape_patterns.get("dTrapOverheatMomentum"))
        item["patternDTrapTopFakeout"] = bool(shape_patterns.get("dTrapTopFakeout"))
        item["candlestickPatternBonus"] = float(candle_shape_bonus)
        item["candlestickPatternBonusDetails"] = candle_shape_bonus_details
        item["v60StrongPenalty"] = bool(direction == "up" and v60_strong is not None and v60_strong >= 0.5)
        item["weeklyRegimeAligned"] = bool(
            weekly_breakout_prob is not None and weekly_breakout_prob >= 0.56
        )
        item["monthlyRegimeAligned"] = bool(
            monthly_breakout_prob is not None and monthly_breakout_prob >= 0.60
        )
        item["probSideRaw"] = float(prob_raw) if prob_raw is not None else None
        item["probSideCalib"] = float(prob) if prob is not None else None
        item["probSide"] = float(prob) if prob is not None else (float(prob_raw) if prob_raw is not None else None)
        item["revRisk"] = float(turn_risk) if isinstance(turn_risk, (int, float)) and math.isfinite(float(turn_risk)) else None
        item["tailRisk"] = float(tail_risk) if isinstance(tail_risk, (int, float)) and math.isfinite(float(tail_risk)) else None
        item["downsideRisk"] = float(downside_risk)
        item["riskPenalty"] = float(risk_penalty)
        item["prob5d"] = float(prob_5d) if isinstance(prob_5d, (int, float)) and math.isfinite(float(prob_5d)) else None
        item["prob10d"] = float(prob_10d) if isinstance(prob_10d, (int, float)) and math.isfinite(float(prob_10d)) else None
        item["prob20d"] = float(prob_20d) if isinstance(prob_20d, (int, float)) and math.isfinite(float(prob_20d)) else None
        item["prob5dAligned"] = bool(prob_5d_ok)
        item["probCurveAligned"] = bool(prob_curve_ok)
        item["horizonAligned"] = bool(horizon_ok)
        item["shortPrecisionProbGate"] = float(short_prob_gate) if direction == "down" else None
        item["shortPrecisionTurnGate"] = float(short_turn_gate) if direction == "down" else None
        item["shortPrecisionProbAligned"] = bool(short_prob_ok) if direction == "down" else None
        item["shortPrecisionTurnAligned"] = bool(short_turn_ok) if direction == "down" else None
        item["shortOverheatBlocked"] = (
            bool(short_overheat_block and not short_overheat_override)
            if direction == "down"
            else None
        )
        item["shortOverheatOverride"] = bool(short_overheat_override) if direction == "down" else None
        item["shortPrecisionGate"] = bool(short_precision_gate) if direction == "down" else None
        item["shortPrecisionGateReason"] = short_precision_gate_reason if direction == "down" else None
        score_gate_ok = bool(
            isinstance(item.get("entryScore"), (int, float))
            and math.isfinite(float(item.get("entryScore")))
            and float(item.get("entryScore")) >= _DAILY_ENTRY_SCORE_GATE_STRICT
        )
        strict_prob_ok = bool(prob is not None and prob >= (prob_gate + 0.02))
        if direction == "down":
            strict_prob_ok = bool(strict_prob_ok and short_prob_ok)
        short_pressure_score_gate = _resolve_short_pressure_score_gate(risk_mode=risk_mode)
        short_pressure_max_ev = _resolve_short_pressure_max_ev(risk_mode=risk_mode)
        weak_shape_block = bool(
            (
                direction == "up"
                and (
                    shape_patterns.get("s1WeakBreakdown")
                    or shape_patterns.get("s2WeakBox")
                )
            )
            or (
                direction == "down"
                and (
                    shape_patterns.get("dTrapStackDownFar")
                    or shape_patterns.get("dTrapOverheatMomentum")
                    or shape_patterns.get("dTrapTopFakeout")
                )
            )
        )
        late_breakout_caution = bool(direction == "up" and shape_patterns.get("s3LateBreakout"))
        short_pattern_setup = bool(
            direction == "down"
            and (
                shape_patterns.get("d1ShortBreakdown")
                or shape_patterns.get("d2ShortMixedFar")
                or shape_patterns.get("d3ShortNaBelow")
                or shape_patterns.get("d4ShortDoubleTop")
                or shape_patterns.get("d5ShortHeadShoulders")
            )
            and turn_ok
            and strict_prob_ok
        )
        short_pressure_setup = bool(
            direction == "down"
            and short_precision_gate
            and turn_ok
            and short_prob_ok
            and counter_move_ok
            and horizon_ok
            and (not weak_shape_block)
            and isinstance(item.get("entryScore"), (int, float))
            and math.isfinite(float(item.get("entryScore")))
            and float(item.get("entryScore")) >= short_pressure_score_gate
            and isinstance(ev_net, (int, float))
            and math.isfinite(float(ev_net))
            and float(ev_net) <= short_pressure_max_ev
            and (
                rule_ok
                or (
                    ev_ok
                    and prob is not None
                    and prob >= (prob_gate + _ENTRY_SHORT_PRESSURE_PROB_EXTRA)
                )
            )
        )
        rebound_setup = bool(
            direction == "up"
            and shape_patterns.get("a3CapitulationRebound")
            and turn_ok
            and horizon_ok
            and downside_risk <= 0.60
        )
        if mode == "turn":
            setup_type = "rebound" if rebound_setup else "turn"
            item["entryQualified"] = bool(
                turn_ok
                and dist_ok
                and counter_move_ok
                and horizon_ok
                and downside_risk <= 0.60
                and (score_gate_ok or (direction == "down" and short_pressure_setup))
                and (not weak_shape_block)
                and (short_precision_gate if direction == "down" else True)
                and (direction == "up" or short_pattern_setup or short_pressure_setup)
            )
        else:
            trend_path_ok = bool(
                prob is not None
                and prob >= prob_gate
                and ev_ok
                and trend_ok
            )
            breakout_setup = bool(
                trend_path_ok
                and turn_ok
                and strict_prob_ok
                and (
                    (weekly_breakout_prob is not None and weekly_breakout_prob >= 0.54)
                    or (monthly_breakout_prob is not None and monthly_breakout_prob >= 0.58)
                )
            )
            matured_breakout_setup = bool(
                direction == "up"
                and shape_patterns.get("a1MaturedBreakout")
                and turn_ok
                and strict_prob_ok
                and (
                    monthly_breakout_prob is None
                    or monthly_breakout_prob >= 0.54
                )
            )
            accumulation_setup = bool(
                turn_ok
                and dist_ok
                and box_bottom_ok
                and not weak_early_pattern
                and not weak_shape_block
                and strict_prob_ok
            )
            continuation_setup = bool(
                trend_path_ok
                and dist_ok
                and horizon_ok
                and strict_prob_ok
            )
            setup_type = (
                "breakout"
                if (breakout_setup or matured_breakout_setup)
                else (
                    "rebound"
                    if rebound_setup
                    else (
                        "breakdown"
                        if short_pattern_setup
                        else ("pressure" if short_pressure_setup else ("accumulation" if accumulation_setup else ("continuation" if continuation_setup else "watch")))
                    )
                )
            )
            item["entryQualified"] = bool(
                (
                    breakout_setup
                    or matured_breakout_setup
                    or rebound_setup
                    or short_pattern_setup
                    or short_pressure_setup
                    or accumulation_setup
                    or continuation_setup
                )
                and (rule_ok if direction == "up" else counter_move_ok)
                and horizon_ok
                and downside_risk <= 0.65
                and (score_gate_ok or (direction == "down" and short_pressure_setup))
                and (not weak_shape_block)
                and (not late_breakout_caution)
                and (short_precision_gate if direction == "down" else True)
            )
        item["setupType"] = setup_type
        _apply_entry_playbook_fields(
            item,
            direction=direction,
            setup_type=setup_type,
            shape_patterns=shape_patterns,
            risk_mode=risk_mode,
        )
        if item["entryQualified"]:
            qualified.append(item)
        else:
            fallback.append(item)

    qualified.sort(key=_base_entry_sort_key)
    if len(qualified) >= limit:
        return qualified[:limit], pred_dt, model_version
    strict_fallback = [
        item
        for item in fallback
        if bool(item.get("evAligned"))
        and (bool(item.get("trendAligned")) or bool(item.get("turnAligned")))
        and bool(item.get("distOk"))
        and bool(item.get("counterMoveOk"))
        and bool(item.get("horizonAligned"))
        and (direction != "down" or bool(item.get("shortPrecisionGate")))
        and (
            mode == "turn"
            or (
                isinstance(item.get("probSide"), (int, float))
                and float(item.get("probSide") or 0.0) >= fallback_prob_gate
            )
        )
    ]
    strict_fallback.sort(key=_base_entry_sort_key)

    promoted_fallback: list[dict] = []
    if direction == "up" and mode == "hybrid" and not qualified:
        stage1_candidates = [
            item
            for item in strict_fallback
            if isinstance(item.get("entryScore"), (int, float))
            and math.isfinite(float(item.get("entryScore")))
            and float(item.get("entryScore")) >= _DAILY_FALLBACK_HYBRID_SCORE_GATE_UP
        ]
        stage1_candidates.sort(key=_base_entry_sort_key)
        if stage1_candidates:
            promoted_fallback = stage1_candidates
            for item in promoted_fallback:
                item["entryQualifiedByFallback"] = True
                item["entryQualifiedFallbackStage"] = "hybrid_relaxed_score"
        else:
            stage2_candidates = [
                item
                for item in fallback
                if bool(item.get("turnAligned"))
                and bool(item.get("distOk"))
                and bool(item.get("counterMoveOk"))
                and bool(item.get("horizonAligned"))
                and isinstance(item.get("downsideRisk"), (int, float))
                and math.isfinite(float(item.get("downsideRisk")))
                and float(item.get("downsideRisk")) <= 0.60
                and isinstance(item.get("entryScore"), (int, float))
                and math.isfinite(float(item.get("entryScore")))
                and float(item.get("entryScore")) >= _DAILY_FALLBACK_TURN_SCORE_GATE_UP
            ]
            stage2_candidates.sort(key=_base_entry_sort_key)
            promoted_fallback = stage2_candidates
            for item in promoted_fallback:
                item["entryQualifiedByFallback"] = True
                item["entryQualifiedFallbackStage"] = "turn_strict_recovery"
    elif direction == "down" and mode == "hybrid" and not qualified:
        stage_short_candidates = [
            item
            for item in strict_fallback
            if (
                bool(item.get("patternD1ShortBreakdown"))
                or bool(item.get("patternD2ShortMixedFar"))
                or bool(item.get("patternD3ShortNaBelow"))
                or bool(item.get("patternD4ShortDoubleTop"))
                or bool(item.get("patternD5ShortHeadShoulders"))
            )
            and (not bool(item.get("patternDTrapStackDownFar")))
            and (not bool(item.get("patternDTrapOverheatMomentum")))
            and (not bool(item.get("patternDTrapTopFakeout")))
            and isinstance(item.get("entryScore"), (int, float))
            and math.isfinite(float(item.get("entryScore")))
            and float(item.get("entryScore")) >= _DAILY_FALLBACK_HYBRID_SCORE_GATE_DOWN
        ]
        stage_short_candidates.sort(key=_base_entry_sort_key)
        if stage_short_candidates:
            promoted_fallback = stage_short_candidates
            for item in promoted_fallback:
                item["entryQualifiedByFallback"] = True
                item["entryQualifiedFallbackStage"] = "short_pattern_recovery"
        else:
            recovery_candidates = [
                item
                for item in fallback
                if (
                    bool(item.get("patternD4ShortDoubleTop"))
                    or bool(item.get("patternD5ShortHeadShoulders"))
                )
                and (not bool(item.get("patternDTrapStackDownFar")))
                and (not bool(item.get("patternDTrapOverheatMomentum")))
                and (not bool(item.get("patternDTrapTopFakeout")))
                and bool(item.get("turnAligned"))
                and bool(item.get("counterMoveOk"))
                and bool(item.get("horizonAligned"))
                and isinstance(item.get("downsideRisk"), (int, float))
                and math.isfinite(float(item.get("downsideRisk")))
                and float(item.get("downsideRisk")) <= 0.60
                and isinstance(item.get("entryScore"), (int, float))
                and math.isfinite(float(item.get("entryScore")))
                and float(item.get("entryScore")) >= _DAILY_FALLBACK_TURN_SCORE_GATE_DOWN
            ]
            recovery_candidates.sort(key=_base_entry_sort_key)
            promoted_fallback = recovery_candidates
            for item in promoted_fallback:
                item["entryQualifiedByFallback"] = True
                item["entryQualifiedFallbackStage"] = "short_turn_recovery"

    min_return = min(limit, 12 if direction == "up" else 8)
    if len(qualified) < min_return:
        selected = _merge_unique([qualified, promoted_fallback, strict_fallback])
        if direction == "up":
            selected = selected[: max(min_return, len(qualified))]
            if len(selected) < min_return:
                selected = _merge_unique([selected, fallback])[:min_return]
        return selected[:limit], pred_dt, model_version
    fallback.sort(
        key=lambda item: (
            not bool(item.get("trendUp")) if direction == "up" else not bool(item.get("trendDown")),
            *_base_entry_sort_key(item),
        )
    )
    selected = _merge_unique([qualified, promoted_fallback, strict_fallback])
    if direction == "up" and len(selected) < limit:
        selected = _merge_unique([selected, fallback])
    return selected[:limit], pred_dt, model_version


def _call_apply_ml_mode(
    items: list[dict],
    *,
    direction: RankDir,
    mode: RankMode,
    limit: int,
    risk_mode: RankRiskMode,
) -> tuple[list[dict], int | None, str | None]:
    try:
        return _apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
            risk_mode=risk_mode,
        )
    except TypeError as exc:
        # Backward-compatible path for tests that monkeypatch a legacy signature.
        if "unexpected keyword argument 'risk_mode'" not in str(exc):
            raise
        return _apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
        )


def _fallback_down_ml_items_when_empty(
    *,
    tf: RankTimeframe,
    direction: RankDir,
    mode: RankMode,
    limit: int,
    risk_mode: RankRiskMode,
    items: list[dict],
    out_items: list[dict],
    pred_dt: int | None,
    model_version: str | None,
) -> tuple[list[dict], int | None, str | None]:
    if tf != "D" or direction != "down":
        return out_items, pred_dt, model_version
    if mode not in {"hybrid", "turn"}:
        return out_items, pred_dt, model_version
    if out_items:
        return out_items, pred_dt, model_version

    ml_items, ml_pred_dt, ml_model_version = _call_apply_ml_mode(
        items,
        direction=direction,
        mode="ml",
        limit=limit,
        risk_mode=risk_mode,
    )
    if not ml_items:
        return out_items, pred_dt, model_version

    promoted: list[dict] = []
    for src in ml_items:
        item = dict(src)
        prob_down = _first_finite(
            item.get("probSide"),
            item.get("probSideCalib"),
            item.get("mlPDownShort"),
            item.get("mlPDown"),
        )
        ev_short = _first_finite(
            item.get("mlEv20Net"),
            item.get("mlEvShortNet"),
            item.get("changePct"),
        )
        turn_down = _first_finite(
            item.get("mlPTurnDownShort"),
            item.get("mlPTurnDown"),
            item.get("mlPDownShort"),
            item.get("mlPDown"),
        )

        qualified = bool(
            isinstance(prob_down, (int, float))
            and math.isfinite(float(prob_down))
            and float(prob_down) >= 0.55
            and isinstance(ev_short, (int, float))
            and math.isfinite(float(ev_short))
            and float(ev_short) <= -0.002
            and (
                turn_down is None
                or (
                    isinstance(turn_down, (int, float))
                    and math.isfinite(float(turn_down))
                    and float(turn_down) <= 0.70
                )
            )
        )

        item["entryQualified"] = bool(qualified)
        item["setupType"] = "ml_fallback_down"
        item["entryQualifiedByFallback"] = True
        item["entryQualifiedFallbackStage"] = "hybrid_to_ml_down_empty"
        if item.get("probSide") is None and prob_down is not None:
            item["probSide"] = float(prob_down)
        promoted.append(item)

    next_pred_dt = ml_pred_dt if ml_pred_dt is not None else pred_dt
    next_model_version = str(ml_model_version or model_version) if (ml_model_version or model_version) else None
    return promoted, next_pred_dt, next_model_version


def refresh_cache() -> None:
    _refresh_cache_singleflight(force=True)


def get_rankings(
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    limit: int,
    *,
    mode: RankMode = "hybrid",
    risk_mode: RankRiskMode = "balanced",
) -> dict:
    cache_key = (tf, which, direction)
    _ensure_cache_fresh_stale_ok(key=cache_key)
    with _LOCK:
        items = _CACHE.get(cache_key)
        last_updated = _LAST_UPDATED
    if items is None:
        try:
            refresh_cache()
        except Exception as exc:
            if not is_transient_duckdb_error(exc):
                raise
            logger.warning("rankings refresh fallback to stale cache due to lock: %s", exc)
        with _LOCK:
            items = _CACHE.get(cache_key, [])
            last_updated = _LAST_UPDATED

    limit = max(1, min(int(limit or 50), 200))
    pred_dt = None
    model_version = None
    if mode == "rule":
        out_items = _decorate_rule_items_with_entry_gate(
            items[:limit],
            direction=direction,
            risk_mode=risk_mode,
        )
    elif tf == "M" and mode == "hybrid":
        out_items, pred_dt, model_version = _call_apply_monthly_ml_mode(
            items,
            direction=direction,
            limit=limit,
            risk_mode=risk_mode,
        )
    else:
        out_items, pred_dt, model_version = _call_apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
            risk_mode=risk_mode,
        )
    out_items, pred_dt, model_version = _fallback_down_ml_items_when_empty(
        tf=tf,
        direction=direction,
        mode=mode,
        limit=limit,
        risk_mode=risk_mode,
        items=items,
        out_items=out_items,
        pred_dt=pred_dt,
        model_version=model_version,
    )
    if tf == "M" and mode == "hybrid":
        flag_applied = _is_edinet_bonus_enabled()
        out_items = [_apply_edinet_defaults(dict(item), flag_applied=flag_applied) for item in out_items]
        _persist_monthly_edinet_audit(
            tf=tf,
            which=which,
            direction=direction,
            mode=mode,
            risk_mode=risk_mode,
            items=out_items,
        )
    out_items = _attach_quality_flags(
        out_items,
        mode=mode,
        direction=direction,
    )
    out_items = _attach_swing_fields(
        out_items,
        direction=direction,
    )
    out_items = [_sanitize_rank_item_for_json(item) for item in out_items]

    try:
        top_items = out_items[:10] if out_items else []
        log_payload = {
            "tag": "rank_request",
            "tf": tf,
            "which": which,
            "direction": direction,
            "mode": mode,
            "risk_mode": risk_mode,
            "limit": limit,
            "pred_dt": pred_dt,
            "model_version": model_version,
            "anchor_date_list": [item.get("asOf") for item in top_items],
            "top": [
                {
                    "code": item.get("code"),
                    "target_dt": item.get("asOf"),
                    "changePct": item.get("changePct"),
                    "mlEv20Net": item.get("mlEv20Net"),
                    "hybridScore": item.get("hybridScore"),
                    "candleTripletUp": item.get("candleTripletUp"),
                    "candleTripletDown": item.get("candleTripletDown"),
                    "monthlyBreakoutUpProb": item.get("monthlyBreakoutUpProb"),
                    "monthlyBreakoutDownProb": item.get("monthlyBreakoutDownProb"),
                    "monthlyRangeProb": item.get("monthlyRangeProb"),
                }
                for item in top_items
            ],
        }
        logger.debug("rank_request %s", json.dumps(log_payload, ensure_ascii=False))
    except Exception as exc:
        logger.debug("rank_request debug logging failed: %s", exc)
    return {
        "tf": tf,
        "which": which,
        "dir": direction,
        "mode": mode,
        "risk_mode": risk_mode,
        "pred_dt": pred_dt,
        "model_version": model_version,
        "last_updated": last_updated.isoformat() if last_updated else None,
        "items": out_items,
    }


def get_rankings_asof(
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    limit: int,
    *,
    as_of: str | int,
    mode: RankMode = "hybrid",
    risk_mode: RankRiskMode = "balanced",
) -> dict:
    as_of_int = _coerce_as_of_int(as_of)
    if as_of_int is None:
        raise ValueError("as_of must be YYYY-MM-DD or YYYYMMDD")

    cache_key = (tf, which, direction)
    _ensure_cache_fresh_stale_ok(key=cache_key)
    try:
        cache = _get_asof_base_cache(as_of_int)
    except Exception as exc:
        if not is_transient_duckdb_error(exc):
            raise
        logger.warning("asof cache build fallback due to transient DB lock: as_of=%s err=%s", as_of_int, exc)
        with _ASOF_BASE_CACHE_LOCK:
            cached = _ASOF_BASE_CACHE.get(as_of_int)
            cache = cached if cached is not None else {}
        if not cache:
            with _LOCK:
                fallback_items = list(_CACHE.get(cache_key, []) or [])
            cache = {cache_key: fallback_items}
    items = cache.get((tf, which, direction), [])
    limit = max(1, min(int(limit or 50), 200))

    pred_dt = None
    model_version = None
    if mode == "rule" or not items:
        out_items = _decorate_rule_items_with_entry_gate(
            items[:limit],
            direction=direction,
            risk_mode=risk_mode,
        )
    elif tf == "M" and mode == "hybrid":
        out_items, pred_dt, model_version = _call_apply_monthly_ml_mode(
            items,
            direction=direction,
            limit=limit,
            risk_mode=risk_mode,
        )
    else:
        out_items, pred_dt, model_version = _call_apply_ml_mode(
            items,
            direction=direction,
            mode=mode,
            limit=limit,
            risk_mode=risk_mode,
        )
    out_items, pred_dt, model_version = _fallback_down_ml_items_when_empty(
        tf=tf,
        direction=direction,
        mode=mode,
        limit=limit,
        risk_mode=risk_mode,
        items=items,
        out_items=out_items,
        pred_dt=pred_dt,
        model_version=model_version,
    )
    if tf == "M" and mode == "hybrid":
        flag_applied = _is_edinet_bonus_enabled()
        out_items = [_apply_edinet_defaults(dict(item), flag_applied=flag_applied) for item in out_items]
    out_items = _attach_quality_flags(
        out_items,
        mode=mode,
        direction=direction,
        now_ymd=as_of_int,
    )
    out_items = _attach_swing_fields(
        out_items,
        direction=direction,
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
            out_items = _decorate_rule_items_with_entry_gate(
                items[:limit],
                direction=direction,
                risk_mode=risk_mode,
            )
            out_items = _attach_quality_flags(
                out_items,
                mode="rule",
                direction=direction,
                now_ymd=as_of_int,
            )
            if tf == "M" and mode == "hybrid":
                flag_applied = _is_edinet_bonus_enabled()
                out_items = [_apply_edinet_defaults(dict(item), flag_applied=flag_applied) for item in out_items]
            out_items = _attach_swing_fields(
                out_items,
                direction=direction,
            )

    filtered: list[dict] = []
    for item in out_items:
        key = _iso_date_to_int(item.get("asOf"))
        if key is not None and key > as_of_int:
            continue
        filtered.append(_sanitize_rank_item_for_json(item))

    return {
        "tf": tf,
        "which": which,
        "dir": direction,
        "mode": mode,
        "risk_mode": risk_mode,
        "requested_as_of": f"{as_of_int:08d}",
        "pred_dt": pred_dt,
        "model_version": model_version,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "items": filtered[:limit],
    }


def _fetch_recent_asof_dates(
    *,
    as_of_int: int | None,
    lookback_days: int,
) -> list[int]:
    where_parts = ["ymd IS NOT NULL"]
    params: list[Any] = []
    if as_of_int is not None:
        where_parts.append("ymd <= ?")
        params.append(int(as_of_int))
    where_clause = "WHERE " + " AND ".join(where_parts)
    query = f"""
        WITH daily_dates AS (
            SELECT DISTINCT
              CASE
                WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt/1000), '%Y%m%d') AS INTEGER)
                WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                ELSE NULL
              END AS ymd
            FROM ml_pred_20d
        )
        SELECT ymd
        FROM daily_dates
        {where_clause}
        ORDER BY ymd DESC
        LIMIT ?
    """
    params.append(int(max(1, lookback_days)))
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def get_last_qualified_trace(
    tf: RankTimeframe,
    which: RankWhich,
    direction: RankDir,
    limit: int,
    *,
    mode: RankMode = "hybrid",
    risk_mode: RankRiskMode = "balanced",
    lookback_days: int = 260,
    recent_hits: int = 10,
    as_of: str | int | None = None,
) -> dict[str, Any]:
    as_of_int = _coerce_as_of_int(as_of) if as_of is not None else None
    lookback_days = max(20, min(int(lookback_days or 260), 1200))
    recent_hits = max(1, min(int(recent_hits or 10), 50))
    limit = max(1, min(int(limit or 50), 200))
    cache_key = (tf, which, direction, mode, risk_mode, limit, lookback_days, recent_hits, as_of_int)
    db_mtime = _db_mtime()

    with _TRACE_CACHE_LOCK:
        cached = _TRACE_CACHE.get(cache_key)
        if cached is not None and cached.get("_db_mtime") == db_mtime:
            _TRACE_CACHE.move_to_end(cache_key)
            cloned = dict(cached)
            cloned.pop("_db_mtime", None)
            return cloned

    _ensure_cache_fresh_stale_ok(key=(tf, which, direction))
    try:
        dates_desc = _fetch_recent_asof_dates(as_of_int=as_of_int, lookback_days=lookback_days)
    except Exception as exc:
        logger.warning("last-qualified trace date fetch failed: %s", exc)
        dates_desc = []

    zero_streak_days = 0
    last_non_zero: dict[str, Any] | None = None
    hits: list[dict[str, Any]] = []

    for ymd in dates_desc:
        try:
            out = get_rankings_asof(
                tf,
                which,
                direction,
                limit,
                as_of=ymd,
                mode=mode,
                risk_mode=risk_mode,
            )
        except Exception as exc:
            logger.warning("last-qualified trace skipped as_of=%s due to error: %s", ymd, exc)
            continue
        items = out.get("items", [])
        qualified = [item for item in items if item.get("entryQualified") is True]
        if not qualified:
            if last_non_zero is None:
                zero_streak_days += 1
            continue
        hit = {
            "date": int(ymd),
            "date_iso": _ymd_int_to_iso(int(ymd)),
            "qualified_count": len(qualified),
            "codes": [str(item.get("code") or "") for item in qualified if item.get("code") is not None],
        }
        if last_non_zero is None:
            last_non_zero = hit
        hits.append(hit)
        if len(hits) >= recent_hits and last_non_zero is not None:
            break

    result: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tf": tf,
        "which": which,
        "dir": direction,
        "mode": mode,
        "risk_mode": risk_mode,
        "limit": limit,
        "lookback_days": lookback_days,
        "inspected_days": len(dates_desc),
        "as_of": _ymd_int_to_iso(as_of_int) if as_of_int is not None else None,
        "as_of_int": int(as_of_int) if as_of_int is not None else None,
        "zero_streak_days": int(zero_streak_days),
        "last_non_zero_date": int(last_non_zero["date"]) if last_non_zero else None,
        "last_non_zero_date_iso": last_non_zero["date_iso"] if last_non_zero else None,
        "last_non_zero_count": int(last_non_zero["qualified_count"]) if last_non_zero else 0,
        "last_non_zero_codes": list(last_non_zero["codes"]) if last_non_zero else [],
        "recent_hits": hits[:recent_hits],
    }

    to_cache = dict(result)
    to_cache["_db_mtime"] = db_mtime
    with _TRACE_CACHE_LOCK:
        _TRACE_CACHE[cache_key] = to_cache
        _TRACE_CACHE.move_to_end(cache_key)
        while len(_TRACE_CACHE) > _TRACE_CACHE_MAX:
            _TRACE_CACHE.popitem(last=False)
    return result
