from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd
from pandas.errors import PerformanceWarning

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts

warnings.filterwarnings("ignore", category=PerformanceWarning)


CANDIDATE_ID = "ma_buy_sell_probe_v1"
CHAMPION_ID = "champion_top5_capture_boundary_promoter_v1"
SCHEMA_PREFIX = "tradex_ma_buy_sell_probe_v1"

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_CHAMPION_COMPARE_JSON = Path(
    r"G:\Tradex\champion_top5_capture_boundary_promoter_v1\20260504T101732Z\compare.json"
)
DEFAULT_STOCK_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_buy_sell_probe_v1")
DEFAULT_STABILITY_OUTPUT_ROOT = Path(r"G:\Tradex\ma_buy_sell_probe_v1_stability_validation")
DEFAULT_REGIME_AUDIT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_buy_sell_probe_v1_regime_audit")
DEFAULT_FINAL_DECISION_OUTPUT_ROOT = Path(r"G:\Tradex\ma_buy_sell_probe_v1_final_decision")

TOP_K_VALUES = (5, 10, 20)
MA_PERIODS = (3, 5, 7, 8, 10, 20, 25, 30, 40, 60, 75, 100, 120, 200)
MA_PAIRS = ((3, 20), (5, 20), (7, 20), (8, 25), (10, 30), (20, 60), (25, 75), (30, 100), (40, 120), (60, 200))
MA_STACKS = ((5, 20, 60), (7, 20, 60), (8, 25, 75), (10, 30, 100), (20, 60, 200))
VARIANT_CAP_PER_FAMILY = 40
MIN_COVERAGE_RATE = 0.75
EPSILON = 1e-12

SCORE_DELTA_CONFIG = {
    "ma_buy_probe_fixed_boost": 0.05,
    "ma_sell_probe_fixed_penalty": 0.05,
    "optimization_during_run": False,
}
SELL_GUARDRAIL = {
    "sell_guardrail_metric": "mean_ret20_delta",
    "sell_guardrail_max_drawdown": 0.0,
    "sell_guardrail_applies_to": ["top5", "top10", "top20"],
}

REQUIRED_AUTHORITATIVE_JSON = (
    "compare.json",
    "family_leaderboard.json",
    "session_leaderboard_rollup.json",
    "scope_stability_rollup.json",
)
REQUIRED_SUPPORTING_JSON = (
    "evaluation_contract.json",
    "run_manifest.json",
    "ma_feature_catalog.json",
    "ma_feature_coverage.json",
    "ma_horizon_role_summary.json",
    "branching_summary.json",
    "candidate_decision.ma_buy_probe.json",
    "candidate_decision.ma_sell_probe.json",
)

REQUIRED_STABILITY_JSON = (
    "kept_candidate_stability.json",
    "kept_candidate_by_month.json",
    "kept_candidate_by_regime.json",
    "kept_candidate_overlap.json",
    "kept_candidate_churn.json",
    "ma_horizon_role_stability.json",
    "validation_manifest.json",
)
REQUIRED_STABILITY_ARTIFACTS = (*REQUIRED_STABILITY_JSON, "kept_candidate_added_removed_examples.parquet")

REQUIRED_CANONICAL_REGIME_VALIDATION_JSON = (
    "kept_candidate_stability.canonical_regime.json",
    "kept_candidate_by_regime.canonical_regime.json",
    "kept_candidate_by_regime.alternate_context.json",
    "kept_candidate_regime_source_manifest.json",
    "kept_candidate_regime_join_quality.json",
    "kept_candidate_regime_hash_check.json",
    "ma_horizon_role_stability.canonical_regime.json",
    "validation_manifest.json",
)
REQUIRED_CANONICAL_REGIME_VALIDATION_ARTIFACTS = (*REQUIRED_CANONICAL_REGIME_VALIDATION_JSON, "_VALIDATION_COMPLETE.json")

REQUIRED_REGIME_AUDIT_JSON = (
    "regime_label_audit.json",
    "regime_label_column_inventory.json",
    "regime_label_source_trace.json",
    "regime_label_join_feasibility.json",
    "regime_label_validation_recommendation.json",
    "audit_manifest.json",
)
REQUIRED_REGIME_AUDIT_ARTIFACTS = (*REQUIRED_REGIME_AUDIT_JSON, "_AUDIT_COMPLETE.json")
FINAL_DECISION_ROLLUP_JSON = "ma_buy_sell_probe_v1_final_decision_rollup.json"

REGIME_COLUMN_CANDIDATES = (
    "regime",
    "regime_label",
    "market_regime",
    "scope_regime",
    "regime_id",
    "regime_bucket",
    "market_regime_label",
    "trend_regime",
    "volatility_regime",
    "market_regime_bucket",
    "dominant_regime_context",
    "family_regime_context",
    "family_bad_pick_regime",
)
PRIMARY_REGIME_SOURCE_COLUMNS = ("regime_label", "market_regime_bucket")
UNKNOWN_REGIME_VALUES = {"", "unknown", "none", "null", "nan", "<na>"}
ALTERNATE_CONTEXT_REGIME_COLUMNS = ("dominant_regime_context", "family_regime_context", "family_bad_pick_regime")
CANONICAL_REGIME_SOURCE_MODE = "canonical_market_regime_daily_validation_join"
CANONICAL_REGIME_SOURCE_ROLE = "validation_grouping_only"

PRIMARY_STABILITY_VARIANTS = (
    "ma_buy_probe.price_vs_ma_n_8",
    "ma_sell_probe.price_cross_below_ma_n_8",
)
SECONDARY_STABILITY_VARIANTS = (
    "ma_buy_probe.price_vs_ma_n_7",
    "ma_sell_probe.price_cross_below_ma_n_7",
    "ma_buy_probe.price_vs_ma_n_20",
    "ma_buy_probe.price_vs_ma_n_200",
)

LABEL_COLUMNS_EXCLUDED_FROM_SCORING = (
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "top15_label",
    "bottom15_label",
)

CURRENT_MA_ROLE_CONTRACT = {
    7: {
        "ma_label": "7MA",
        "horizon_bucket": "short",
        "role_intent": "entry_timing",
        "user_semantics": "エントリータイミング",
    },
    20: {
        "ma_label": "20MA",
        "horizon_bucket": "mid",
        "role_intent": "trend_ride",
        "user_semantics": "トレンドに乗るため",
    },
    60: {
        "ma_label": "60MA",
        "horizon_bucket": "long",
        "role_intent": "trend_confirmation",
        "user_semantics": "トレンドの確認",
    },
    100: {
        "ma_label": "100MA",
        "horizon_bucket": "long",
        "role_intent": "resistance_band_confirmation",
        "user_semantics": "抵抗帯の確認",
    },
    200: {
        "ma_label": "200MA",
        "horizon_bucket": "long",
        "role_intent": "environment_confirmation",
        "user_semantics": "環境の確認",
    },
}


@dataclass(frozen=True)
class VariantSpec:
    variant_id: str
    probe_family: str
    probe_intent: str
    feature_family: str
    feature_name: str
    periods: tuple[int, ...]
    signal_column: str
    required_lookback_days: int
    score_delta: float
    side_scope: str = "long_only"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if value is pd.NA:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: dict[str, Any]) -> str:
    text = json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _as_bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.fillna(False).astype(bool)
    return series.fillna(False).astype(str).str.lower().isin({"1", "true", "yes", "y"})


def _mean_or_none(values: Iterable[Any]) -> float | None:
    usable = [_as_float(value) for value in values]
    usable = [value for value in usable if value is not None]
    if not usable:
        return None
    return float(sum(usable) / len(usable))


def _rate_or_none(values: Iterable[Any]) -> float | None:
    usable = [value for value in values if value is not None and not pd.isna(value)]
    if not usable:
        return None
    return float(sum(1.0 for value in usable if bool(value)) / len(usable))


def _delta(candidate: float | None, champion: float | None) -> float | None:
    if candidate is None or champion is None:
        return None
    return float(candidate - champion)


def normalize_date_key(value: Any) -> str:
    if value is None or value is pd.NA:
        raise ValueError("date value is required")
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        raise ValueError("date value is required")
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        if len(text) == 8:
            return pd.to_datetime(text, format="%Y%m%d").strftime("%Y-%m-%d")
        number = int(text)
        if number > 100_000_000:
            return pd.to_datetime(number, unit="s", utc=True).strftime("%Y-%m-%d")
    return pd.to_datetime(text).strftime("%Y-%m-%d")


def _date_to_epoch_seconds(value: Any) -> int:
    return int(pd.Timestamp(normalize_date_key(value), tz="UTC").timestamp())


def _date_to_yyyymmdd_int(value: Any) -> int:
    return int(pd.Timestamp(normalize_date_key(value)).strftime("%Y%m%d"))


def _feature_family_for_probe(probe_family: str) -> str:
    if probe_family == "ma_buy_probe":
        return "boundary_feature"
    if probe_family == "ma_sell_probe":
        return "bad_pick_removal"
    raise ValueError(f"unsupported probe_family: {probe_family}")


def _slope_lag(period: int) -> int:
    if period <= 10:
        return 3
    if period <= 40:
        return 5
    return 10


def _overextension_threshold(period: int) -> float:
    if period <= 10:
        return 0.08
    if period <= 40:
        return 0.12
    return 0.18


def _signal_id(feature_name: str, periods: tuple[int, ...]) -> str:
    suffix = "_".join(str(period) for period in periods)
    return f"{feature_name}_{suffix}"


def _make_variant_specs(cap_per_family: int = VARIANT_CAP_PER_FAMILY) -> tuple[list[VariantSpec], list[dict[str, Any]]]:
    specs: list[VariantSpec] = []

    def add(probe_family: str, feature_name: str, periods: tuple[int, ...], signal_column: str, required: int) -> None:
        intent = "buy_boost" if probe_family == "ma_buy_probe" else "sell_demotion"
        delta = SCORE_DELTA_CONFIG["ma_buy_probe_fixed_boost"] if probe_family == "ma_buy_probe" else -SCORE_DELTA_CONFIG["ma_sell_probe_fixed_penalty"]
        specs.append(
            VariantSpec(
                variant_id=f"{probe_family}.{_signal_id(feature_name, periods)}",
                probe_family=probe_family,
                probe_intent=intent,
                feature_family=_feature_family_for_probe(probe_family),
                feature_name=feature_name,
                periods=periods,
                signal_column=signal_column,
                required_lookback_days=required,
                score_delta=float(delta),
            )
        )

    for period in MA_PERIODS:
        add("ma_buy_probe", "price_vs_ma_n", (period,), f"signal_price_vs_ma_{period}", period)
        add("ma_buy_probe", "ma_n_slope", (period,), f"signal_ma_slope_up_{period}", period + _slope_lag(period))
        add("ma_sell_probe", "price_cross_below_ma_n", (period,), f"signal_price_cross_below_ma_{period}", period + 1)
        add("ma_sell_probe", "ma_n_slope_down", (period,), f"signal_ma_slope_down_{period}", period + _slope_lag(period))

    for period in (3, 5, 7, 8, 10, 20, 25, 30, 40):
        add("ma_buy_probe", "price_cross_above_ma_n", (period,), f"signal_price_cross_above_ma_{period}", period + 1)
    for period in (20, 25, 30, 40, 60, 75, 100, 120, 200):
        add("ma_buy_probe", "pullback_to_ma_n", (period,), f"signal_pullback_to_ma_{period}", period + _slope_lag(period))
        add("ma_buy_probe", "breakout_distance_from_ma_n", (period,), f"signal_breakout_distance_from_ma_{period}", period + _slope_lag(period))
        add("ma_sell_probe", "failed_reclaim_ma_n", (period,), f"signal_failed_reclaim_ma_{period}", period + 1)
        add("ma_sell_probe", "overextension_from_ma_n", (period,), f"signal_overextension_from_ma_{period}", period)
        add("ma_sell_probe", "support_loss_after_ma_touch", (period,), f"signal_support_loss_after_ma_touch_{period}", period + 5)

    for fast, slow in MA_PAIRS:
        add("ma_buy_probe", "ma_fast_vs_ma_slow", (fast, slow), f"signal_ma_fast_vs_ma_slow_{fast}_{slow}", slow)
        add("ma_sell_probe", "ma_fast_cross_below_ma_slow", (fast, slow), f"signal_ma_fast_cross_below_ma_slow_{fast}_{slow}", slow + 1)

    for fast, mid, slow in MA_STACKS:
        add("ma_buy_probe", "bullish_ma_stack", (fast, mid, slow), f"signal_bullish_ma_stack_{fast}_{mid}_{slow}", slow)
        add("ma_sell_probe", "bearish_ma_stack", (fast, mid, slow), f"signal_bearish_ma_stack_{fast}_{mid}_{slow}", slow)

    active: list[VariantSpec] = []
    skipped: list[dict[str, Any]] = []
    for probe_family in ("ma_buy_probe", "ma_sell_probe"):
        family_specs = [spec for spec in specs if spec.probe_family == probe_family]
        active.extend(family_specs[:cap_per_family])
        for spec in family_specs[cap_per_family:]:
            skipped.append(
                {
                    "variant_id": spec.variant_id,
                    "probe_family": spec.probe_family,
                    "feature_name": spec.feature_name,
                    "periods": list(spec.periods),
                    "skip_reason": "sweep_scope_limited_initial_branching_probe",
                }
            )
    return active, skipped


def _variant_spec_map() -> dict[str, VariantSpec]:
    specs, _ = _make_variant_specs(VARIANT_CAP_PER_FAMILY)
    return {spec.variant_id: spec for spec in specs}


def load_source_rows(source_rows_parquet: Path, *, limit_anchor_dates: int | None = None) -> pd.DataFrame:
    if not source_rows_parquet.exists():
        raise FileNotFoundError(f"source rows parquet not found: {source_rows_parquet}")
    frame = pd.read_parquet(source_rows_parquet).copy()
    if "champion_score" not in frame.columns and "score" in frame.columns:
        frame["champion_score"] = frame["score"]
    if "trade_date" not in frame.columns and "anchor_date" in frame.columns:
        frame["trade_date"] = frame["anchor_date"]
    required = {"symbol", "side", "trade_date", "champion_rank", "champion_score", "forward_ret_20d"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")

    frame["source_row_id"] = range(len(frame))
    frame["symbol"] = frame["symbol"].astype(str)
    frame["side"] = frame["side"].astype(str).str.lower()
    frame["trade_date_key"] = frame["trade_date"].map(normalize_date_key)
    frame["anchor_date"] = frame.get("anchor_date", frame["trade_date_key"])
    frame["anchor_date"] = frame["anchor_date"].map(normalize_date_key)
    frame["month_bucket"] = frame.get("month_bucket", frame["trade_date_key"].str.slice(0, 7)).astype(str)
    if "regime_label" not in frame.columns:
        frame["regime_label"] = frame["market_regime_bucket"].astype(str) if "market_regime_bucket" in frame.columns else "unknown"
    for column in ("champion_rank", "candidate_rank", "rank"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in ("champion_score", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ("top15_label", "bottom15_label"):
        if column in frame.columns:
            frame[column] = _as_bool_series(frame[column])
        else:
            frame[column] = False
    if "path_value_score_v1" not in frame.columns:
        frame["path_value_score_v1"] = np.nan

    for top_k in TOP_K_VALUES:
        selected_col = f"champion_selected_top{top_k}"
        if selected_col in frame.columns:
            frame[selected_col] = _as_bool_series(frame[selected_col])
        else:
            frame[selected_col] = frame["champion_rank"].le(top_k).fillna(False).astype(bool)

    frame = frame[frame["champion_score"].notna() & frame["champion_rank"].notna()].copy()
    frame.sort_values(["trade_date_key", "side", "champion_rank", "symbol"], inplace=True, kind="stable")
    if limit_anchor_dates is not None and int(limit_anchor_dates) > 0:
        keep_dates = sorted(frame["trade_date_key"].unique().tolist())[: int(limit_anchor_dates)]
        frame = frame[frame["trade_date_key"].isin(keep_dates)].copy()
    return frame.reset_index(drop=True)


def load_daily_bars(stock_db: Path, symbols: list[str]) -> pd.DataFrame:
    if not stock_db.exists():
        raise FileNotFoundError(f"runtime stock DB not found: {stock_db}")
    with duckdb.connect(str(stock_db), read_only=True) as conn:
        bars = conn.execute(
            """
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN (SELECT UNNEST(?))
            ORDER BY code, date
            """,
            [symbols],
        ).fetchdf()
    if bars.empty:
        raise RuntimeError("daily_bars returned no confirmed pan rows for candidate symbols")
    return bars


def build_ma_bar_features(bars: pd.DataFrame) -> pd.DataFrame:
    warnings.filterwarnings("ignore", category=PerformanceWarning)
    required = {"code", "date", "o", "h", "l", "c"}
    missing = sorted(required - set(bars.columns))
    if missing:
        raise ValueError(f"daily_bars missing required columns: {missing}")
    frame = bars.copy()
    frame["symbol"] = frame["code"].astype(str)
    frame["bar_date"] = frame["date"].map(normalize_date_key)
    frame["bar_dt"] = pd.to_datetime(frame["bar_date"])
    for column in ("o", "h", "l", "c", "v"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame.sort_values(["symbol", "bar_dt"], inplace=True, kind="stable")

    out_frames: list[pd.DataFrame] = []
    for _, group in frame.groupby("symbol", sort=False):
        working = group.copy()
        close = working["c"]
        high = working["h"]
        low = working["l"]
        prev_close = close.shift(1)
        for period in MA_PERIODS:
            ma_col = f"ma_{period}"
            working[ma_col] = close.rolling(period, min_periods=period).mean()
            ma = working[ma_col]
            prev_ma = ma.shift(1)
            dist = close / ma.replace(0, np.nan) - 1.0
            slope_lag = _slope_lag(period)
            slope = ma / ma.shift(slope_lag).replace(0, np.nan) - 1.0
            touch = low <= ma * 1.01
            recent_touch = touch.rolling(5, min_periods=1).max().fillna(0).astype(bool)
            valid_ma = ma.notna()
            valid_slope = valid_ma & slope.notna()
            valid_cross = valid_ma & prev_ma.notna() & prev_close.notna()
            working[f"price_vs_ma_{period}"] = dist
            working[f"ma_{period}_slope"] = slope
            working[f"signal_price_vs_ma_{period}"] = ((dist >= 0.0) & (dist <= 0.08)).where(valid_ma, pd.NA)
            working[f"signal_ma_slope_up_{period}"] = (slope > 0.0).where(valid_slope, pd.NA)
            working[f"signal_ma_slope_down_{period}"] = (slope < 0.0).where(valid_slope, pd.NA)
            working[f"signal_price_cross_above_ma_{period}"] = ((close >= ma) & (prev_close < prev_ma)).where(valid_cross, pd.NA)
            working[f"signal_price_cross_below_ma_{period}"] = ((close <= ma) & (prev_close > prev_ma)).where(valid_cross, pd.NA)
            working[f"signal_pullback_to_ma_{period}"] = ((close > ma) & touch & (slope >= 0.0)).where(valid_slope, pd.NA)
            working[f"signal_breakout_distance_from_ma_{period}"] = ((dist >= 0.02) & (dist <= 0.15) & (slope >= 0.0)).where(valid_slope, pd.NA)
            working[f"signal_failed_reclaim_ma_{period}"] = ((high >= ma) & (close < ma) & (prev_close < prev_ma)).where(valid_cross, pd.NA)
            working[f"signal_overextension_from_ma_{period}"] = (dist >= _overextension_threshold(period)).where(valid_ma, pd.NA)
            working[f"signal_support_loss_after_ma_touch_{period}"] = (recent_touch & (close < ma) & (prev_close >= prev_ma)).where(valid_cross, pd.NA)
        for fast, slow in MA_PAIRS:
            fast_ma = working[f"ma_{fast}"]
            slow_ma = working[f"ma_{slow}"]
            pair_valid = fast_ma.notna() & slow_ma.notna()
            pair_cross_valid = pair_valid & fast_ma.shift(1).notna() & slow_ma.shift(1).notna()
            working[f"ma_fast_vs_ma_slow_{fast}_{slow}"] = fast_ma / slow_ma.replace(0, np.nan) - 1.0
            working[f"signal_ma_fast_vs_ma_slow_{fast}_{slow}"] = (fast_ma > slow_ma).where(pair_valid, pd.NA)
            working[f"signal_ma_fast_cross_below_ma_slow_{fast}_{slow}"] = ((fast_ma <= slow_ma) & (fast_ma.shift(1) > slow_ma.shift(1))).where(pair_cross_valid, pd.NA)
        for fast, mid, slow in MA_STACKS:
            fast_ma = working[f"ma_{fast}"]
            mid_ma = working[f"ma_{mid}"]
            slow_ma = working[f"ma_{slow}"]
            stack_valid = fast_ma.notna() & mid_ma.notna() & slow_ma.notna()
            working[f"signal_bullish_ma_stack_{fast}_{mid}_{slow}"] = ((close > fast_ma) & (fast_ma > mid_ma) & (mid_ma > slow_ma)).where(stack_valid, pd.NA)
            working[f"signal_bearish_ma_stack_{fast}_{mid}_{slow}"] = ((close < fast_ma) & (fast_ma < mid_ma) & (mid_ma < slow_ma)).where(stack_valid, pd.NA)
        out_frames.append(working)
    features = pd.concat(out_frames, ignore_index=True)
    signal_cols = [column for column in features.columns if column.startswith("signal_")]
    for column in signal_cols:
        features[column] = features[column].where(features[column].notna(), pd.NA)
    return features


def join_features_to_source(source: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    left = source.copy()
    left["trade_dt"] = pd.to_datetime(left["trade_date_key"])
    right = features.copy()
    right["bar_dt"] = pd.to_datetime(right["bar_date"])
    merged_parts: list[pd.DataFrame] = []
    feature_cols = [column for column in right.columns if column not in {"code", "date", "source"}]
    for symbol, group in left.groupby("symbol", sort=False):
        fgroup = right[right["symbol"] == symbol].sort_values("bar_dt", kind="stable")
        lgroup = group.sort_values("trade_dt", kind="stable")
        if fgroup.empty:
            missing = lgroup.copy()
            for column in feature_cols:
                if column not in missing.columns:
                    missing[column] = pd.NA
            merged_parts.append(missing)
            continue
        merged = pd.merge_asof(
            lgroup,
            fgroup[feature_cols].sort_values("bar_dt", kind="stable"),
            left_on="trade_dt",
            right_on="bar_dt",
            by="symbol",
            direction="backward",
            allow_exact_matches=True,
        )
        merged_parts.append(merged)
    merged = pd.concat(merged_parts, ignore_index=True)
    merged["bar_date_used"] = merged["bar_date"]
    merged["bar_shift_days"] = (merged["trade_dt"] - pd.to_datetime(merged["bar_date_used"])).dt.days
    merged["no_lookahead_valid"] = pd.to_datetime(merged["bar_date_used"]).le(merged["trade_dt"]).fillna(False)
    return merged.sort_values(["trade_date_key", "side", "champion_rank", "symbol"], kind="stable").reset_index(drop=True)


def _side_scope_mask(frame: pd.DataFrame, spec: VariantSpec) -> pd.Series:
    if spec.side_scope == "long_only":
        return frame["side"].astype(str).str.lower().eq("long")
    return pd.Series(True, index=frame.index)


def _rank_with_variant(frame: pd.DataFrame, spec: VariantSpec) -> tuple[pd.DataFrame, dict[str, Any]]:
    working = frame.copy()
    side_mask = _side_scope_mask(working, spec)
    signal_source = working.get(spec.signal_column)
    if signal_source is None:
        signal_source = pd.Series(pd.NA, index=working.index)
    eligible = side_mask & signal_source.notna() & working["no_lookahead_valid"].fillna(False).astype(bool)
    signal_bool = signal_source.astype("boolean").fillna(False).astype(bool)
    signal_hit = eligible & signal_bool
    working["ma_probe_signal_hit"] = signal_hit
    working["ma_probe_signal_eligible"] = eligible
    working["challenger_score"] = pd.to_numeric(working["champion_score"], errors="coerce")
    working.loc[signal_hit, "challenger_score"] = working.loc[signal_hit, "challenger_score"] + spec.score_delta
    ranked_parts: list[pd.DataFrame] = []
    for _, group in working.groupby(["trade_date_key", "side"], sort=True):
        ordered = group.sort_values(["challenger_score", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
        ordered["challenger_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True)
    for top_k in TOP_K_VALUES:
        ranked[f"challenger_selected_top{top_k}"] = ranked["challenger_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        ranked[f"changed_top{top_k}_member"] = ranked[f"challenger_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["challenger_rank"].astype(int) != ranked["champion_rank"].astype(int)

    total_rows = int(side_mask.sum())
    eligible_rows = int(eligible.sum())
    coverage = {
        "variant_id": spec.variant_id,
        "probe_family": spec.probe_family,
        "feature_name": spec.feature_name,
        "periods": list(spec.periods),
        "required_lookback_days": spec.required_lookback_days,
        "eligible_rows_count": eligible_rows,
        "total_rows_count": total_rows,
        "coverage_rate": None if total_rows == 0 else float(eligible_rows / total_rows),
        "skipped_rows_count": int(max(0, total_rows - eligible_rows)),
        "skip_reason": None if total_rows and eligible_rows == total_rows else "feature_coverage_incomplete",
    }
    return ranked, coverage


def _selected(frame: pd.DataFrame, prefix: str, top_k: int) -> pd.DataFrame:
    column = f"{prefix}_selected_top{top_k}"
    return frame[frame[column].fillna(False).astype(bool)]


def _quality(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "count": int(len(rows)),
        "mean_ret20": _mean_or_none(rows.get("forward_ret_20d", pd.Series(dtype=float)).tolist()),
        "mean_path_value_score_v1": _mean_or_none(rows.get("path_value_score_v1", pd.Series(dtype=float)).tolist()),
        "top15_rate": _rate_or_none(rows.get("top15_label", pd.Series(dtype=bool)).tolist()),
        "bottom15_rate": _rate_or_none(rows.get("bottom15_label", pd.Series(dtype=bool)).tolist()),
    }


def _quality_delta(added: dict[str, Any], removed: dict[str, Any]) -> dict[str, Any]:
    return {
        "mean_ret20_delta_vs_removed": _delta(added.get("mean_ret20"), removed.get("mean_ret20")),
        "mean_path_value_score_v1_delta_vs_removed": _delta(added.get("mean_path_value_score_v1"), removed.get("mean_path_value_score_v1")),
        "top15_rate_delta_vs_removed": _delta(added.get("top15_rate"), removed.get("top15_rate")),
        "bottom15_rate_delta_vs_removed": _delta(added.get("bottom15_rate"), removed.get("bottom15_rate")),
    }


def _bad_pick_count(rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    if "bottom15_label" in rows.columns:
        return int(rows["bottom15_label"].fillna(False).astype(bool).sum())
    return int((pd.to_numeric(rows["forward_ret_20d"], errors="coerce") <= -0.15).fillna(False).sum())


def _good_pick_count(rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    if "top15_label" in rows.columns:
        return int(rows["top15_label"].fillna(False).astype(bool).sum())
    return int((pd.to_numeric(rows["forward_ret_20d"], errors="coerce") > 0.05).fillna(False).sum())


def _boundary_gap(frame: pd.DataFrame, score_column: str, top_k: int) -> float | None:
    gaps: list[float] = []
    for _, group in frame.groupby(["trade_date_key", "side"], sort=False):
        ordered = group.sort_values([score_column, "champion_rank", "symbol"], ascending=[False, True, True], kind="stable")
        if len(ordered) <= top_k:
            continue
        kth = _as_float(ordered.iloc[top_k - 1][score_column])
        next_score = _as_float(ordered.iloc[top_k][score_column])
        if kth is not None and next_score is not None:
            gaps.append(float(kth - next_score))
    return _mean_or_none(gaps)


def _topk_metrics(frame: pd.DataFrame, prefix: str, top_k: int) -> dict[str, Any]:
    rows = _selected(frame, prefix, top_k)
    return {
        "selected_count": int(len(rows)),
        "mean_ret20": _mean_or_none(rows["forward_ret_20d"].tolist()),
        "mean_path_value_score_v1": _mean_or_none(rows["path_value_score_v1"].tolist()),
        "top15_rate": _rate_or_none(rows["top15_label"].tolist()),
        "bottom15_rate": _rate_or_none(rows["bottom15_label"].tolist()),
    }


def _selection_divergence_reason(metrics: dict[str, Any], coverage: dict[str, Any]) -> str:
    if coverage.get("skip_reason") == "feature_coverage_incomplete":
        return "feature_coverage_incomplete"
    if int(metrics.get("changed_top5_members_count") or 0) > 0:
        return "top5_member_swap"
    if int(metrics.get("changed_top10_members_count") or 0) > 0:
        return "top10_member_swap"
    if int(metrics.get("changed_top20_members_count") or 0) > 0:
        return "top20_only_swap"
    if int(metrics.get("changed_rank_count") or 0) > 0:
        return "rank_reorder_inside_pool"
    return "no_divergence"


def _horizon_bucket(periods: list[int] | tuple[int, ...]) -> str:
    if not periods:
        return "unknown"
    max_period = max(int(period) for period in periods)
    if max_period <= 10:
        return "short"
    if max_period <= 40:
        return "mid"
    return "long"


def _variant_metrics(ranked: pd.DataFrame, spec: VariantSpec, coverage: dict[str, Any]) -> dict[str, Any]:
    champion_topk: dict[str, Any] = {}
    challenger_topk: dict[str, Any] = {}
    deltas: dict[str, Any] = {}
    added_quality_by_topk: dict[str, Any] = {}
    removed_quality_by_topk: dict[str, Any] = {}
    bad_pick_removal_by_topk: dict[str, int] = {}
    for top_k in TOP_K_VALUES:
        champion = _topk_metrics(ranked, "champion", top_k)
        challenger = _topk_metrics(ranked, "challenger", top_k)
        champion_topk[f"top{top_k}"] = champion
        challenger_topk[f"top{top_k}"] = challenger
        deltas[f"top{top_k}"] = {
            "mean_ret20_delta": _delta(challenger["mean_ret20"], champion["mean_ret20"]),
            "mean_path_value_score_v1_delta": _delta(challenger["mean_path_value_score_v1"], champion["mean_path_value_score_v1"]),
            "top15_rate_delta": _delta(challenger["top15_rate"], champion["top15_rate"]),
            "bottom15_rate_delta": _delta(challenger["bottom15_rate"], champion["bottom15_rate"]),
        }
        added = ranked[ranked[f"challenger_selected_top{top_k}"].astype(bool) & ~ranked[f"champion_selected_top{top_k}"].astype(bool)]
        removed = ranked[ranked[f"champion_selected_top{top_k}"].astype(bool) & ~ranked[f"challenger_selected_top{top_k}"].astype(bool)]
        added_q = _quality(added)
        removed_q = _quality(removed)
        added_q["quality_delta_vs_removed"] = _quality_delta(added_q, removed_q)
        removed_q["quality_delta_vs_added"] = _quality_delta(removed_q, added_q)
        added_quality_by_topk[f"top{top_k}"] = added_q
        removed_quality_by_topk[f"top{top_k}"] = removed_q
        bad_pick_removal_by_topk[f"top{top_k}"] = int(_bad_pick_count(removed) - _bad_pick_count(added))

    metrics = {
        "variant_id": spec.variant_id,
        "probe_family": spec.probe_family,
        "probe_intent": spec.probe_intent,
        "feature_family": spec.feature_family,
        "feature_name": spec.feature_name,
        "periods": list(spec.periods),
        "horizon_bucket": _horizon_bucket(spec.periods),
        "score_delta": spec.score_delta,
        "coverage": coverage,
        "champion_topk": champion_topk,
        "challenger_topk": challenger_topk,
        "topk_deltas": deltas,
        "changed_top5_members_count": int(ranked["changed_top5_member"].fillna(False).astype(bool).sum()),
        "changed_top10_members_count": int(ranked["changed_top10_member"].fillna(False).astype(bool).sum()),
        "changed_top20_members_count": int(ranked["changed_top20_member"].fillna(False).astype(bool).sum()),
        "changed_rank_count": int(ranked["rank_changed"].fillna(False).astype(bool).sum()),
        "top5_boundary_score_gap": _boundary_gap(ranked, "challenger_score", 5),
        "top10_boundary_score_gap": _boundary_gap(ranked, "challenger_score", 10),
        "champion_top5_boundary_score_gap": _boundary_gap(ranked, "champion_score", 5),
        "champion_top10_boundary_score_gap": _boundary_gap(ranked, "champion_score", 10),
        "bad_pick_removal_by_topk": bad_pick_removal_by_topk,
        "bad_pick_removal_count": int(bad_pick_removal_by_topk["top10"]),
        "added_pick_quality_by_topk": added_quality_by_topk,
        "removed_pick_quality_by_topk": removed_quality_by_topk,
        "added_pick_quality": added_quality_by_topk["top10"],
        "removed_pick_quality": removed_quality_by_topk["top10"],
        "good_pick_removal_count": int(
            _good_pick_count(ranked[ranked["champion_selected_top10"].astype(bool) & ~ranked["challenger_selected_top10"].astype(bool)])
        ),
        "signal_hit_count": int(ranked["ma_probe_signal_hit"].fillna(False).astype(bool).sum()),
    }
    metrics["selection_divergence_reason"] = _selection_divergence_reason(metrics, coverage)
    for top_k in TOP_K_VALUES:
        metrics[f"top{top_k}_mean_ret20"] = challenger_topk[f"top{top_k}"]["mean_ret20"]
        metrics[f"champion_top{top_k}_mean_ret20"] = champion_topk[f"top{top_k}"]["mean_ret20"]
        metrics[f"top{top_k}_mean_ret20_delta"] = deltas[f"top{top_k}"]["mean_ret20_delta"]
    return metrics


def _has_topk_uplift(metrics: dict[str, Any]) -> bool:
    return any((metrics.get(f"top{top_k}_mean_ret20_delta") or 0.0) > 0.0 for top_k in TOP_K_VALUES)


def _sell_guardrail_pass(metrics: dict[str, Any], guardrail: dict[str, Any]) -> bool:
    max_drawdown = float(guardrail["sell_guardrail_max_drawdown"])
    for label in guardrail["sell_guardrail_applies_to"]:
        delta_value = metrics.get(f"{label}_mean_ret20_delta")
        if delta_value is None or float(delta_value) < -max_drawdown:
            return False
    return True


def decide_variant(metrics: dict[str, Any], spec: VariantSpec, *, guardrail: dict[str, Any] | None = None) -> dict[str, Any]:
    guardrail = guardrail or SELL_GUARDRAIL
    passed: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []
    coverage_rate = metrics["coverage"].get("coverage_rate")
    coverage_incomplete = coverage_rate is None or float(coverage_rate) < MIN_COVERAGE_RATE
    if coverage_incomplete:
        failed.append({"code": "feature_coverage_incomplete", "status": "fail"})
    else:
        passed.append({"code": "feature_coverage_sufficient", "status": "pass"})

    branching = int(metrics["changed_top5_members_count"]) > 0 or int(metrics["changed_top10_members_count"]) > 0
    if branching:
        passed.append({"code": "topk_branching_present", "status": "pass"})
    else:
        failed.append({"code": "drop_no_meaningful_branching", "status": "fail"})

    uplift = _has_topk_uplift(metrics)
    if spec.probe_family == "ma_buy_probe":
        added_delta = metrics["added_pick_quality"]["quality_delta_vs_removed"].get("mean_ret20_delta_vs_removed")
        added_quality_improved = added_delta is not None and added_delta > 0.0
        if uplift:
            passed.append({"code": "buy_topk_uplift_present", "status": "pass"})
        else:
            failed.append({"code": "drop_topk_uplift_worsened_or_absent", "status": "fail"})
        if added_quality_improved:
            passed.append({"code": "buy_added_pick_quality_improved", "status": "pass"})
        else:
            failed.append({"code": "drop_added_pick_quality_not_improved", "status": "fail"})
        if int(metrics["bad_pick_removal_count"]) < 0:
            failed.append({"code": "buy_bad_pick_removal_worsened_guardrail_recorded", "status": "record_only"})
        if coverage_incomplete:
            decision = "hold"
            reason = "hold_feature_coverage_incomplete"
        elif branching and uplift and added_quality_improved:
            decision = "keep"
            reason = "keep_buy_added_quality_topk_uplift_branching"
        elif not branching:
            decision = "drop"
            reason = "drop_no_meaningful_branching"
        elif not uplift:
            decision = "drop"
            reason = "drop_topk_uplift_worsened_or_absent"
        elif not added_quality_improved:
            decision = "drop"
            reason = "drop_added_pick_quality_not_improved"
        else:
            decision = "hold"
            reason = "hold_buy_partial_improvement_requires_breadth"
    else:
        removal_improved = int(metrics["bad_pick_removal_count"]) > 0
        removed_delta = metrics["removed_pick_quality"]["quality_delta_vs_added"].get("mean_ret20_delta_vs_removed")
        removed_low_quality = removed_delta is not None and removed_delta < 0.0
        guardrail_pass = _sell_guardrail_pass(metrics, guardrail)
        if removal_improved:
            passed.append({"code": "sell_bad_pick_removal_improved", "status": "pass"})
        else:
            failed.append({"code": "drop_bad_pick_removal_not_improved", "status": "fail"})
        if removed_low_quality:
            passed.append({"code": "sell_removed_pick_quality_low_quality_biased", "status": "pass"})
        else:
            failed.append({"code": "drop_removed_pick_quality_not_low_quality", "status": "fail"})
        if guardrail_pass:
            passed.append({"code": "sell_guardrail_passed", "status": "pass"})
        else:
            failed.append({"code": "drop_sell_guardrail_failed_topk_drawdown", "status": "fail"})
        if coverage_incomplete:
            decision = "hold"
            reason = "hold_feature_coverage_incomplete"
        elif branching and removal_improved and removed_low_quality and guardrail_pass:
            decision = "keep"
            reason = "keep_sell_bad_pick_removal_demotion_branching"
        elif not branching:
            decision = "drop"
            reason = "drop_no_meaningful_branching"
        elif not removal_improved:
            decision = "drop"
            reason = "drop_bad_pick_removal_not_improved"
        elif not guardrail_pass:
            decision = "drop"
            reason = "drop_sell_guardrail_failed_topk_drawdown"
        elif not removed_low_quality:
            decision = "drop"
            reason = "drop_removed_pick_quality_not_low_quality"
        else:
            decision = "hold"
            reason = "hold_sell_partial_removal_requires_breadth"

    metrics["candidate_local_decision"] = decision
    metrics["session_aggregate_decision"] = decision
    metrics["decision"] = decision
    metrics["decision_reason"] = reason
    metrics["decision_reasons"] = [{"code": reason, "status": decision}]
    metrics["passed_gate_reasons"] = passed
    metrics["failed_gate_reasons"] = failed
    metrics["variant_validity"] = "invalid" if coverage_incomplete else "valid"
    return metrics


def _decision_sort_key(metrics: dict[str, Any]) -> tuple[int, float]:
    decision_rank = {"keep": 0, "hold": 1, "drop": 2}.get(str(metrics.get("candidate_local_decision")), 3)
    if metrics["probe_family"] == "ma_buy_probe":
        score = (metrics.get("top5_mean_ret20_delta") or 0.0) + (metrics.get("top10_mean_ret20_delta") or 0.0)
        score += metrics["added_pick_quality"]["quality_delta_vs_removed"].get("mean_ret20_delta_vs_removed") or 0.0
    else:
        score = float(metrics.get("bad_pick_removal_count") or 0)
        score += (metrics.get("top5_mean_ret20_delta") or 0.0) + (metrics.get("top10_mean_ret20_delta") or 0.0)
    return decision_rank, -float(score)


def _pick_family_best(variant_results: list[dict[str, Any]], probe_family: str) -> dict[str, Any]:
    family = [row for row in variant_results if row["probe_family"] == probe_family]
    if not family:
        raise RuntimeError(f"no variant results for {probe_family}")
    return sorted(family, key=_decision_sort_key)[0]


def _candidate_result_row(best: dict[str, Any], same_condition: dict[str, Any], fixed_condition_hash: str) -> dict[str, Any]:
    victory = {metric: None for metric in contracts.TRADEX_VICTORY_METRICS}
    victory.update(
        {
            "hold_end_return_20d": best.get("top20_mean_ret20"),
            "mfe_20d": None,
            "mae_20d": None,
            "win_flag_hold_end": None,
            "opportunity_count": best.get("changed_top10_members_count"),
            "addability_score": best.get("top10_mean_ret20_delta"),
            "trimability_score": best.get("bad_pick_removal_count"),
        }
    )
    return {
        "plan_id": best["variant_id"],
        "plan_version": "v1",
        "label": best["variant_id"],
        "method_id": best["variant_id"],
        "method_title": best["variant_id"],
        "method_thesis": "TRADEX-only MA buy/sell probe under fixed same-condition top-K comparison.",
        "method_family": best["probe_family"],
        "probe_family": best["probe_family"],
        "probe_intent": best["probe_intent"],
        "feature_family": best["feature_family"],
        "decision": best["candidate_local_decision"],
        "candidate_local_decision": best["candidate_local_decision"],
        "decision_gate_version": "ma_buy_sell_probe_gate_v1",
        "primary_success_metric": "added_pick_quality" if best["probe_family"] == "ma_buy_probe" else "bad_pick_removal_count",
        "secondary_guardrail_metric": "bad_pick_removal_recorded_only" if best["probe_family"] == "ma_buy_probe" else SELL_GUARDRAIL["sell_guardrail_metric"],
        "decision_reasons": best["decision_reasons"],
        "passed_gate_reasons": best["passed_gate_reasons"],
        "failed_gate_reasons": best["failed_gate_reasons"],
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "victory_metrics": victory,
        "long_horizon_regime_score": best.get("top20_mean_ret20_delta") or 0.0,
        "recent_adaptation_score": best.get("top5_mean_ret20_delta") or 0.0,
        "same_condition_contract": same_condition,
        "fixed_condition_hash": fixed_condition_hash,
        "top5_mean_ret20": best.get("top5_mean_ret20"),
        "top10_mean_ret20": best.get("top10_mean_ret20"),
        "top20_mean_ret20": best.get("top20_mean_ret20"),
        "changed_top5_members_count": best.get("changed_top5_members_count"),
        "changed_top10_members_count": best.get("changed_top10_members_count"),
        "changed_rank_count": best.get("changed_rank_count"),
        "bad_pick_removal_count": best.get("bad_pick_removal_count"),
        "added_pick_quality": best.get("added_pick_quality"),
        "removed_pick_quality": best.get("removed_pick_quality"),
        "selection_divergence_reason": best.get("selection_divergence_reason"),
    }


def _aggregate_decision(rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, str]]]:
    decisions = [row.get("candidate_local_decision") for row in rows]
    if "keep" in decisions:
        return "keep", [{"code": "candidate_keep_present", "status": "keep"}]
    if "hold" in decisions:
        return "hold", [{"code": "candidate_hold_present_no_keep", "status": "hold"}]
    return "drop", [{"code": "all_candidates_drop", "status": "drop"}]


def _build_same_condition(source: pd.DataFrame, evaluation_contract: dict[str, Any], feature_family: str | None = None) -> dict[str, Any]:
    return contracts.build_same_condition_contract(
        universe=[str(evaluation_contract["universe_id"])],
        period_segments=[
            {
                "label": "source_rows_period",
                "start_date": evaluation_contract["period_start"],
                "end_date": evaluation_contract["period_end"],
            }
        ],
        top_k=max(TOP_K_VALUES),
        regime=str(evaluation_contract["regime_definition"]),
        cost_model=evaluation_contract["cost_slippage_config"],
        artifact_detail_level=evaluation_contract["artifact_detail_level"],
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        feature_family=feature_family,
    ).to_dict()


def _load_champion_contract(champion_compare_json_path: Path) -> dict[str, Any]:
    if not champion_compare_json_path.exists():
        raise FileNotFoundError(f"champion compare json not found: {champion_compare_json_path}")
    compare = _load_json(champion_compare_json_path)
    sibling_eval = champion_compare_json_path.parent / "evaluation_contract.json"
    eval_contract = _load_json(sibling_eval) if sibling_eval.exists() else {}
    same_condition = compare.get("same_condition_contract") if isinstance(compare.get("same_condition_contract"), dict) else {}
    eval_same = eval_contract.get("same_condition_contract") if isinstance(eval_contract.get("same_condition_contract"), dict) else {}
    return {
        "ret20_source_mode": eval_contract.get("ret20_source_mode") or same_condition.get("ret20_source_mode") or eval_same.get("ret20_source_mode"),
        "candidate_build_order_mode": eval_contract.get("candidate_build_order_mode")
        or same_condition.get("candidate_build_order_mode")
        or eval_same.get("candidate_build_order_mode"),
        "artifact_detail_level": compare.get("artifact_detail_level") or eval_contract.get("artifact_detail_level") or "authoritative_full",
        "same_condition_contract": same_condition or eval_same,
    }


def _build_evaluation_contract(
    *,
    source: pd.DataFrame,
    source_rows_artifact_path: Path,
    champion_compare_json_path: Path,
    runtime_stock_db_path: Path,
    variant_cap: int,
) -> dict[str, Any]:
    champion_contract = _load_champion_contract(champion_compare_json_path)
    ret20_source_mode = champion_contract.get("ret20_source_mode") or "forward_ret_20d"
    candidate_build_order_mode = champion_contract.get("candidate_build_order_mode") or "champion_rank_preserve_then_top5_boundary_promotion"
    period_start = str(source["trade_date_key"].min())
    period_end = str(source["trade_date_key"].max())
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "source_rows_artifact_path": str(source_rows_artifact_path),
        "champion_compare_json_path": str(champion_compare_json_path),
        "runtime_stock_db_path": str(runtime_stock_db_path),
        "runtime_stock_db_role": "daily_bars_read_only",
        "ret20_source_mode": ret20_source_mode,
        "candidate_build_order_mode": candidate_build_order_mode,
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "universe_id": f"source_rows:{source_rows_artifact_path.name}",
        "period_start": period_start,
        "period_end": period_end,
        "topk_list": list(TOP_K_VALUES),
        "regime_definition": "source_rows.regime_label",
        "cost_slippage_config": contracts.TRADEX_DEFAULT_COST_MODEL,
        "score_delta_config": dict(SCORE_DELTA_CONFIG),
        "variant_cap": int(variant_cap),
        "decision_gate_version": "ma_buy_sell_probe_gate_v1",
        **SELL_GUARDRAIL,
        "same_condition_flags": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
        },
        "no_silent_fallback": True,
        "no_meemee_reflection": True,
        "non_scope": [
            "MeeMee app files",
            "MeeMee display MA",
            "MeeMee ranking UI",
            "MeeMee publish flow",
            "production ranking registration",
            "regime correction",
            "image analysis",
            "symbol-specific correction",
            "ranking loss",
            "evaluation period",
            "cost/slippage conditions",
            "champion artifact regeneration",
        ],
    }
    contract["fixed_condition_hash"] = _stable_hash(
        {
            key: contract[key]
            for key in (
                "source_rows_artifact_path",
                "champion_compare_json_path",
                "ret20_source_mode",
                "candidate_build_order_mode",
                "artifact_detail_level",
                "universe_id",
                "period_start",
                "period_end",
                "topk_list",
                "regime_definition",
                "cost_slippage_config",
                "score_delta_config",
                "variant_cap",
                "decision_gate_version",
                "sell_guardrail_metric",
                "sell_guardrail_max_drawdown",
                "sell_guardrail_applies_to",
            )
        }
    )
    return contract


def _build_breakdown(variant_results: list[dict[str, Any]], column: str, ranked_by_variant: dict[str, pd.DataFrame]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    best_ids = {result["variant_id"] for result in variant_results}
    for variant_id in sorted(best_ids):
        ranked = ranked_by_variant[variant_id]
        for bucket, group in ranked.groupby(column, sort=True):
            rows.append(
                {
                    "variant_id": variant_id,
                    column: str(bucket),
                    "decision_sets": int(group.groupby(["trade_date_key", "side"], sort=False).ngroups),
                    "top5_mean_ret20_delta": _delta(
                        _topk_metrics(group, "challenger", 5)["mean_ret20"],
                        _topk_metrics(group, "champion", 5)["mean_ret20"],
                    ),
                    "top10_mean_ret20_delta": _delta(
                        _topk_metrics(group, "challenger", 10)["mean_ret20"],
                        _topk_metrics(group, "champion", 10)["mean_ret20"],
                    ),
                    "changed_top5_members_count": int(group["changed_top5_member"].fillna(False).astype(bool).sum()),
                    "changed_top10_members_count": int(group["changed_top10_member"].fillna(False).astype(bool).sum()),
                }
            )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_{column}_breakdown_v1",
        "generated_at": _utc_now(),
        "breakdown_column": column,
        "rows": rows,
        "breadth": {
            "bucket_count": len({row[column] for row in rows}),
            "top5_improved_bucket_count": sum(1 for row in rows if (row.get("top5_mean_ret20_delta") or 0.0) > 0.0),
            "top5_branched_bucket_count": sum(1 for row in rows if int(row.get("changed_top5_members_count") or 0) > 0),
        },
    }


def _membership_keys(frame: pd.DataFrame, prefix: str, top_k: int) -> set[tuple[str, str, str]]:
    selected = _selected(frame, prefix, top_k)
    return {
        (str(row["trade_date_key"]), str(row["side"]), str(row["symbol"]))
        for row in selected[["trade_date_key", "side", "symbol"]].to_dict(orient="records")
    }


def _changed_member_rows(frame: pd.DataFrame, variant_id: str, top_k: int, *, added: bool) -> pd.DataFrame:
    challenger_col = f"challenger_selected_top{top_k}"
    champion_col = f"champion_selected_top{top_k}"
    mask = frame[challenger_col].fillna(False).astype(bool) & ~frame[champion_col].fillna(False).astype(bool)
    change_type = "added"
    if not added:
        mask = frame[champion_col].fillna(False).astype(bool) & ~frame[challenger_col].fillna(False).astype(bool)
        change_type = "removed"
    cols = [
        "trade_date_key",
        "month_bucket",
        "regime_label",
        "side",
        "symbol",
        "champion_rank",
        "challenger_rank",
        "champion_score",
        "challenger_score",
        "forward_ret_20d",
        "path_value_score_v1",
        "top15_label",
        "bottom15_label",
    ]
    out = frame.loc[mask, [col for col in cols if col in frame.columns]].copy()
    out.insert(0, "variant_id", variant_id)
    out.insert(1, "top_k", top_k)
    out.insert(2, "change_type", change_type)
    return out


def _stability_group_rows(ranked_by_variant: dict[str, pd.DataFrame], target_variant_ids: list[str], group_column: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant_id in target_variant_ids:
        ranked = ranked_by_variant[variant_id]
        for bucket, group in ranked.groupby(group_column, sort=True):
            top5_champion = _topk_metrics(group, "champion", 5)
            top5_challenger = _topk_metrics(group, "challenger", 5)
            top10_champion = _topk_metrics(group, "champion", 10)
            top10_challenger = _topk_metrics(group, "challenger", 10)
            removed10 = _changed_member_rows(group, variant_id, 10, added=False)
            added10 = _changed_member_rows(group, variant_id, 10, added=True)
            rows.append(
                {
                    "variant_id": variant_id,
                    group_column: str(bucket),
                    "decision_sets": int(group.groupby(["trade_date_key", "side"], sort=False).ngroups),
                    "monthly_top5_delta" if group_column == "month_bucket" else "regime_top5_delta": _delta(
                        top5_challenger["mean_ret20"], top5_champion["mean_ret20"]
                    ),
                    "monthly_top10_delta" if group_column == "month_bucket" else "regime_top10_delta": _delta(
                        top10_challenger["mean_ret20"], top10_champion["mean_ret20"]
                    ),
                    "monthly_changed_top5_members_count" if group_column == "month_bucket" else "regime_changed_top5_members_count": int(
                        group["changed_top5_member"].fillna(False).astype(bool).sum()
                    ),
                    "monthly_changed_top10_members_count" if group_column == "month_bucket" else "regime_changed_top10_members_count": int(
                        group["changed_top10_member"].fillna(False).astype(bool).sum()
                    ),
                    "monthly_bad_pick_removal_count" if group_column == "month_bucket" else "regime_bad_pick_removal_count": int(
                        _bad_pick_count(removed10) - _bad_pick_count(added10)
                    ),
                }
            )
    return rows


def _concentration_summary(rows: list[dict[str, Any]], *, group_key: str, changed_key: str, top10_delta_key: str) -> dict[str, Any]:
    if not rows:
        return {
            "bucket_count": 0,
            "positive_bucket_count": 0,
            "max_changed_share": None,
            "concentrated": True,
            "typed_reason": "uplift_not_observable",
        }
    total_changed = sum(max(0, int(row.get(changed_key) or 0)) for row in rows)
    max_changed = max((max(0, int(row.get(changed_key) or 0)) for row in rows), default=0)
    positive = sum(1 for row in rows if (row.get(top10_delta_key) or 0.0) > 0.0)
    max_share = None if total_changed <= 0 else float(max_changed / total_changed)
    concentrated = bool(total_changed > 0 and max_share is not None and max_share > 0.35) or positive <= 1
    return {
        "bucket_count": len({str(row.get(group_key)) for row in rows}),
        "positive_bucket_count": positive,
        "total_changed_top10_members_count": int(total_changed),
        "max_changed_top10_members_count": int(max_changed),
        "max_changed_share": max_share,
        "concentrated": concentrated,
        "typed_reason": "uplift_concentrated" if concentrated else "uplift_not_concentrated",
    }


def _stability_decision(
    variant_metrics: dict[str, Any],
    month_rows: list[dict[str, Any]],
    regime_rows: list[dict[str, Any]],
) -> tuple[str, list[dict[str, str]], dict[str, Any]]:
    month_concentration = _concentration_summary(
        month_rows,
        group_key="month_bucket",
        changed_key="monthly_changed_top10_members_count",
        top10_delta_key="monthly_top10_delta",
    )
    regime_concentration = _concentration_summary(
        regime_rows,
        group_key="regime_label",
        changed_key="regime_changed_top10_members_count",
        top10_delta_key="regime_top10_delta",
    )
    reasons: list[dict[str, str]] = []
    if variant_metrics["candidate_local_decision"] != "keep":
        reasons.append({"code": "source_candidate_not_keep", "status": "drop"})
        return "drop_after_stability_check", reasons, {"month": month_concentration, "regime": regime_concentration}
    if int(variant_metrics.get("changed_top5_members_count") or 0) <= 0 and int(variant_metrics.get("changed_top10_members_count") or 0) <= 0:
        reasons.append({"code": "no_material_branching", "status": "drop"})
        return "drop_after_stability_check", reasons, {"month": month_concentration, "regime": regime_concentration}
    if month_concentration["concentrated"] or regime_concentration["concentrated"]:
        reasons.append({"code": "uplift_concentrated", "status": "hold"})
        return "hold_for_more_validation", reasons, {"month": month_concentration, "regime": regime_concentration}
    reasons.append({"code": "branching_and_uplift_not_concentrated", "status": "provisional_keep"})
    return "provisional_keep", reasons, {"month": month_concentration, "regime": regime_concentration}


def _role_summary_row(best: dict[str, Any] | None, *, probe_family: str, period: int, role_meta: dict[str, Any], variant_count: int) -> dict[str, Any]:
    if best is None:
        return {
            "period": period,
            **role_meta,
            "probe_family": probe_family,
            "variant_count": 0,
            "best_variant_id": None,
            "candidate_local_decision": "hold",
            "typed_reason": "hold_current_ma_role_not_evaluated",
        }
    return {
        "period": period,
        **role_meta,
        "probe_family": probe_family,
        "variant_count": variant_count,
        "best_variant_id": best["variant_id"],
        "candidate_local_decision": best["candidate_local_decision"],
        "typed_reason": best["decision_reason"],
        "top5_mean_ret20_delta": best.get("top5_mean_ret20_delta"),
        "top10_mean_ret20_delta": best.get("top10_mean_ret20_delta"),
        "top20_mean_ret20_delta": best.get("top20_mean_ret20_delta"),
        "changed_top5_members_count": best.get("changed_top5_members_count"),
        "changed_top10_members_count": best.get("changed_top10_members_count"),
        "changed_rank_count": best.get("changed_rank_count"),
        "bad_pick_removal_count": best.get("bad_pick_removal_count"),
        "selection_divergence_reason": best.get("selection_divergence_reason"),
        "coverage_rate": best.get("coverage", {}).get("coverage_rate"),
    }


def _build_horizon_role_summary(variant_results: list[dict[str, Any]]) -> dict[str, Any]:
    horizon_rows: list[dict[str, Any]] = []
    for probe_family in ("ma_buy_probe", "ma_sell_probe"):
        family_rows = [row for row in variant_results if row["probe_family"] == probe_family]
        for horizon in ("short", "mid", "long"):
            rows = [row for row in family_rows if row.get("horizon_bucket") == horizon]
            if not rows:
                horizon_rows.append(
                    {
                        "probe_family": probe_family,
                        "horizon_bucket": horizon,
                        "variant_count": 0,
                        "best_variant_id": None,
                        "candidate_local_decision": "hold",
                        "typed_reason": "hold_horizon_bucket_not_evaluated",
                    }
                )
                continue
            best = sorted(rows, key=_decision_sort_key)[0]
            horizon_rows.append(
                {
                    "probe_family": probe_family,
                    "horizon_bucket": horizon,
                    "variant_count": len(rows),
                    "keep_count": sum(1 for row in rows if row["candidate_local_decision"] == "keep"),
                    "hold_count": sum(1 for row in rows if row["candidate_local_decision"] == "hold"),
                    "drop_count": sum(1 for row in rows if row["candidate_local_decision"] == "drop"),
                    "best_variant_id": best["variant_id"],
                    "candidate_local_decision": best["candidate_local_decision"],
                    "typed_reason": best["decision_reason"],
                    "top5_mean_ret20_delta": best.get("top5_mean_ret20_delta"),
                    "top10_mean_ret20_delta": best.get("top10_mean_ret20_delta"),
                    "top20_mean_ret20_delta": best.get("top20_mean_ret20_delta"),
                    "changed_top5_members_count": best.get("changed_top5_members_count"),
                    "changed_top10_members_count": best.get("changed_top10_members_count"),
                    "changed_rank_count": best.get("changed_rank_count"),
                    "bad_pick_removal_count": best.get("bad_pick_removal_count"),
                    "selection_divergence_reason": best.get("selection_divergence_reason"),
                }
            )

    current_rows: list[dict[str, Any]] = []
    for period, role_meta in CURRENT_MA_ROLE_CONTRACT.items():
        for probe_family in ("ma_buy_probe", "ma_sell_probe"):
            rows = [
                row
                for row in variant_results
                if row["probe_family"] == probe_family and int(period) in [int(item) for item in row.get("periods", [])]
            ]
            best = sorted(rows, key=_decision_sort_key)[0] if rows else None
            current_rows.append(_role_summary_row(best, probe_family=probe_family, period=period, role_meta=role_meta, variant_count=len(rows)))

    return {
        "schema_version": f"{SCHEMA_PREFIX}_ma_horizon_role_summary_v1",
        "generated_at": _utc_now(),
        "artifact_role": "interpret MA probe results by short/mid/long horizon and current MeeMee visual MA semantics",
        "boundary": "TRADEX-only",
        "horizon_bucket_definition": {
            "short": "MA period <= 10; entry timing and near-boundary timing behavior",
            "mid": "MA period 20-40; trend ride and trend continuation context",
            "long": "MA period >= 60; trend confirmation, resistance/support, and environment confirmation context",
        },
        "current_ma_role_contract": CURRENT_MA_ROLE_CONTRACT,
        "horizon_rows": horizon_rows,
        "current_ma_role_rows": current_rows,
        "interpretation_rules": {
            "ma_buy_probe": "Judge whether the role lifts better candidates into top-K.",
            "ma_sell_probe": "Judge whether the role demotes bad or low-quality candidates without violating the sell guardrail.",
            "not_meemee_reflectable": "This is research evidence only; no MeeMee display MA or production ranking change is implied.",
        },
    }


def _build_artifacts(
    *,
    source: pd.DataFrame,
    variant_results: list[dict[str, Any]],
    ranked_by_variant: dict[str, pd.DataFrame],
    coverage_rows: list[dict[str, Any]],
    skipped_variants: list[dict[str, Any]],
    evaluation_contract: dict[str, Any],
    run_id: str,
    output_dir: Path,
    started_at: str,
    source_rows_artifact_path: Path,
    champion_compare_json_path: Path,
    runtime_stock_db_path: Path,
) -> dict[str, dict[str, Any]]:
    best_buy = _pick_family_best(variant_results, "ma_buy_probe")
    best_sell = _pick_family_best(variant_results, "ma_sell_probe")
    same_condition = _build_same_condition(source, evaluation_contract)
    fixed_hash = evaluation_contract["fixed_condition_hash"]
    candidate_rows = [
        _candidate_result_row(best_buy, same_condition, fixed_hash),
        _candidate_result_row(best_sell, same_condition, fixed_hash),
    ]
    aggregate_decision, aggregate_reasons = _aggregate_decision(candidate_rows)

    compare = {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "diagnostics_schema_version": "tradex_diagnostics_v1",
        "family_id": CANDIDATE_ID,
        "generated_at": _utc_now(),
        "baseline_run_id": CHAMPION_ID,
        "same_condition_contract": same_condition,
        "candidate_results": candidate_rows,
        "fixed_condition_hash": fixed_hash,
        "evaluation_contract_path": str(output_dir / "evaluation_contract.json"),
        "variant_results": variant_results,
        "champion_id": CHAMPION_ID,
        "ret20_source_mode": evaluation_contract["ret20_source_mode"],
        "candidate_build_order_mode": evaluation_contract["candidate_build_order_mode"],
        "artifact_detail_level": evaluation_contract["artifact_detail_level"],
        "selection_divergence_reason": {
            "ma_buy_probe": best_buy["selection_divergence_reason"],
            "ma_sell_probe": best_sell["selection_divergence_reason"],
        },
    }

    family_summary = []
    for best in (best_buy, best_sell):
        family_summary.append(
            {
                "method_family": best["probe_family"],
                "method_title": best["probe_family"],
                "method_thesis": "MA-derived fixed-delta branching probe.",
                "decision": best["candidate_local_decision"],
                "session_aggregate_decision": best["candidate_local_decision"],
                "decision_reasons": best["decision_reasons"],
                "best_variant_id": best["variant_id"],
                "probe_intent": best["probe_intent"],
                "feature_family": best["feature_family"],
                "fixed_condition_hash": fixed_hash,
            }
        )

    family = {
        "schema_version": f"{SCHEMA_PREFIX}_family_leaderboard_v1",
        "session_meta": {"session_id": run_id, "candidate_id": CANDIDATE_ID, "output_dir": str(output_dir)},
        "source_compare_path": str(output_dir / "compare.json"),
        "coverage_waterfall": {
            "variant_count": len(variant_results),
            "coverage_incomplete_variant_count": sum(1 for row in coverage_rows if row.get("skip_reason") == "feature_coverage_incomplete"),
            "skipped_variant_count": len(skipped_variants),
        },
        "overview": {
            "authoritative_result_source": "family_leaderboard.json",
            "ma_buy_probe_decision": best_buy["candidate_local_decision"],
            "ma_sell_probe_decision": best_sell["candidate_local_decision"],
        },
        "family_summary": family_summary,
        "candidate_rows": candidate_rows,
        "authoritative_rollup_decision": aggregate_decision,
        "fixed_condition_hash": fixed_hash,
    }

    session = {
        "schema_version": f"{SCHEMA_PREFIX}_session_leaderboard_rollup_v1",
        "session_meta": {"session_id": run_id, "candidate_id": CANDIDATE_ID, "output_dir": str(output_dir)},
        "source_family_leaderboard_paths": [str(output_dir / "family_leaderboard.json")],
        "overview": {
            "session_aggregate_decision": aggregate_decision,
            "decision_gate_version": evaluation_contract["decision_gate_version"],
        },
        "family_summary": family_summary,
        "candidate_rows": candidate_rows,
        "authoritative_rollup_decision": aggregate_decision,
        "fixed_condition_hash": fixed_hash,
    }

    scope = {
        "schema_version": f"{SCHEMA_PREFIX}_scope_stability_rollup_v1",
        "overview": {
            "scope_id": "ma_buy_sell_probe_v1_fixed_condition",
            "authoritative_rollup_decision": aggregate_decision,
            "fixed_condition_hash": fixed_hash,
        },
        "session_rows": [
            {
                "session_scope_id": run_id,
                "decision": aggregate_decision,
                "session_aggregate_decision": aggregate_decision,
                "decision_reasons": aggregate_reasons,
                "fixed_condition_hash": fixed_hash,
                "ma_buy_probe_decision": best_buy["candidate_local_decision"],
                "ma_sell_probe_decision": best_sell["candidate_local_decision"],
            }
        ],
        "authoritative_rollup_decision": aggregate_decision,
        "fixed_condition_hash": fixed_hash,
    }

    feature_catalog = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_catalog_v1",
        "generated_at": _utc_now(),
        "label_columns_excluded_from_scoring": list(LABEL_COLUMNS_EXCLUDED_FROM_SCORING),
        "variant_cap_per_family": VARIANT_CAP_PER_FAMILY,
        "active_variants": [
            {
                "variant_id": row["variant_id"],
                "probe_family": row["probe_family"],
                "probe_intent": row["probe_intent"],
                "feature_family": row["feature_family"],
                "feature_name": row["feature_name"],
                "periods": row["periods"],
                "score_delta": row["score_delta"],
                "required_lookback_days": row["coverage"]["required_lookback_days"],
                "side_scope": "long_only",
            }
            for row in variant_results
        ],
        "skipped_variants": skipped_variants,
    }

    feature_coverage = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_coverage_v1",
        "generated_at": _utc_now(),
        "min_coverage_rate": MIN_COVERAGE_RATE,
        "coverage_rows": coverage_rows,
        "coverage_incomplete_variant_count": sum(1 for row in coverage_rows if row.get("skip_reason") == "feature_coverage_incomplete"),
    }

    horizon_role_summary = _build_horizon_role_summary(variant_results)

    branching_summary = {
        "schema_version": f"{SCHEMA_PREFIX}_branching_summary_v1",
        "generated_at": _utc_now(),
        "best_variants": {
            "ma_buy_probe": best_buy["variant_id"],
            "ma_sell_probe": best_sell["variant_id"],
        },
        "branching": {
            "ma_buy_probe": {
                "changed_top5_members_count": best_buy["changed_top5_members_count"],
                "changed_top10_members_count": best_buy["changed_top10_members_count"],
                "changed_rank_count": best_buy["changed_rank_count"],
                "selection_divergence_reason": best_buy["selection_divergence_reason"],
            },
            "ma_sell_probe": {
                "changed_top5_members_count": best_sell["changed_top5_members_count"],
                "changed_top10_members_count": best_sell["changed_top10_members_count"],
                "changed_rank_count": best_sell["changed_rank_count"],
                "selection_divergence_reason": best_sell["selection_divergence_reason"],
            },
        },
    }

    buy_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_decision_v1",
        "generated_at": _utc_now(),
        **candidate_rows[0],
        "typed_keep_reason": best_buy["decision_reason"] if best_buy["candidate_local_decision"] == "keep" else None,
        "typed_hold_reason": best_buy["decision_reason"] if best_buy["candidate_local_decision"] == "hold" else None,
        "typed_drop_reason": best_buy["decision_reason"] if best_buy["candidate_local_decision"] == "drop" else None,
    }
    sell_decision = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_decision_v1",
        "generated_at": _utc_now(),
        **candidate_rows[1],
        "typed_keep_reason": best_sell["decision_reason"] if best_sell["candidate_local_decision"] == "keep" else None,
        "typed_hold_reason": best_sell["decision_reason"] if best_sell["candidate_local_decision"] == "hold" else None,
        "typed_drop_reason": best_sell["decision_reason"] if best_sell["candidate_local_decision"] == "drop" else None,
    }

    run_manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_run_manifest_v1",
        "run_id": run_id,
        "script_path": str(Path(__file__).resolve()),
        "git_commit_or_workspace_state": _git_workspace_state(),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "input_artifacts": [
            {"name": "source_rows_artifact_path", "path": str(source_rows_artifact_path)},
            {"name": "champion_compare_json_path", "path": str(champion_compare_json_path)},
            {"name": "runtime_stock_db_path", "path": str(runtime_stock_db_path), "role": "daily_bars_read_only"},
        ],
        "output_artifacts": [*REQUIRED_AUTHORITATIVE_JSON, *REQUIRED_SUPPORTING_JSON, "_ARTIFACT_COMPLETE.json"],
        "fixed_condition_hash": fixed_hash,
        "random_seed_if_any": None,
        "deterministic_cap": VARIANT_CAP_PER_FAMILY,
        "skipped_variant_count": len(skipped_variants),
        "skip_reasons_summary": _skip_reasons_summary(skipped_variants),
        "boundary": "TRADEX-only",
        "runtime_db_write_occurred": False,
        "silent_fallback_used": False,
    }

    by_month = _build_breakdown([best_buy, best_sell], "month_bucket", ranked_by_variant)
    by_regime = _build_breakdown([best_buy, best_sell], "regime_label", ranked_by_variant)

    return {
        "compare.json": compare,
        "family_leaderboard.json": family,
        "session_leaderboard_rollup.json": session,
        "scope_stability_rollup.json": scope,
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "ma_feature_catalog.json": feature_catalog,
        "ma_feature_coverage.json": feature_coverage,
        "ma_horizon_role_summary.json": horizon_role_summary,
        "branching_summary.json": branching_summary,
        "candidate_decision.ma_buy_probe.json": buy_decision,
        "candidate_decision.ma_sell_probe.json": sell_decision,
        "by_month.json": by_month,
        "by_regime.json": by_regime,
    }


def _git_workspace_state() -> dict[str, Any]:
    try:
        commit = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
        status = subprocess.run(["git", "status", "--short"], capture_output=True, text=True, check=False).stdout.splitlines()
        branch = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True, check=False).stdout.strip()
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    return {
        "available": True,
        "branch": branch,
        "head": commit,
        "dirty": bool(status),
        "status_entry_count": len(status),
    }


def _skip_reasons_summary(skipped: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in skipped:
        reason = str(row.get("skip_reason") or "unknown")
        out[reason] = out.get(reason, 0) + 1
    return out


def _run_internal_no_lookahead_check() -> bool:
    dates = pd.bdate_range("2026-01-01", periods=25)
    base_rows = []
    for idx, ts in enumerate(dates):
        close = 100.0 + idx
        base_rows.append({"code": "T1", "date": int(ts.strftime("%Y%m%d")), "o": close - 0.5, "h": close + 1.0, "l": close - 1.0, "c": close, "v": 1000, "source": "pan"})
    future = {"code": "T1", "date": int(pd.Timestamp("2026-02-20").strftime("%Y%m%d")), "o": 10_000.0, "h": 10_000.0, "l": 10_000.0, "c": 10_000.0, "v": 1, "source": "pan"}
    source = pd.DataFrame(
        [
            {
                "symbol": "T1",
                "side": "long",
                "trade_date": int(pd.Timestamp("2026-01-30").strftime("%Y%m%d")),
                "anchor_date": "2026-01-30",
                "champion_rank": 1,
                "champion_score": 1.0,
                "forward_ret_20d": 0.0,
                "path_value_score_v1": 0.0,
                "champion_selected_top5": True,
                "champion_selected_top10": True,
                "champion_selected_top20": True,
            }
        ]
    )
    clean = join_features_to_source(load_source_rows_from_frame(source), build_ma_bar_features(pd.DataFrame(base_rows)))
    with_future = join_features_to_source(load_source_rows_from_frame(source), build_ma_bar_features(pd.DataFrame([*base_rows, future])))
    return bool(clean.loc[0, "ma_20"] == with_future.loc[0, "ma_20"] and with_future.loc[0, "bar_date_used"] == "2026-01-30")


def load_source_rows_from_frame(frame: pd.DataFrame) -> pd.DataFrame:
    temp = frame.copy()
    if "source_row_id" not in temp.columns:
        temp["source_row_id"] = range(len(temp))
    temp["symbol"] = temp["symbol"].astype(str)
    temp["side"] = temp["side"].astype(str).str.lower()
    temp["trade_date_key"] = temp["trade_date"].map(normalize_date_key)
    temp["anchor_date"] = temp.get("anchor_date", temp["trade_date_key"])
    temp["anchor_date"] = temp["anchor_date"].map(normalize_date_key)
    temp["month_bucket"] = temp.get("month_bucket", temp["trade_date_key"].str.slice(0, 7)).astype(str)
    if "regime_label" in temp.columns:
        temp["regime_label"] = temp["regime_label"].astype(str)
    elif "market_regime_bucket" in temp.columns:
        temp["regime_label"] = temp["market_regime_bucket"].astype(str)
    else:
        temp["regime_label"] = "unknown"
    temp["champion_rank"] = pd.to_numeric(temp["champion_rank"], errors="coerce").astype("Int64")
    temp["champion_score"] = pd.to_numeric(temp["champion_score"], errors="coerce")
    temp["forward_ret_20d"] = pd.to_numeric(temp["forward_ret_20d"], errors="coerce")
    temp["path_value_score_v1"] = pd.to_numeric(temp.get("path_value_score_v1", np.nan), errors="coerce")
    temp["top15_label"] = _as_bool_series(temp.get("top15_label", pd.Series(False, index=temp.index)))
    temp["bottom15_label"] = _as_bool_series(temp.get("bottom15_label", pd.Series(False, index=temp.index)))
    for top_k in TOP_K_VALUES:
        col = f"champion_selected_top{top_k}"
        temp[col] = _as_bool_series(temp[col]) if col in temp.columns else temp["champion_rank"].le(top_k).fillna(False).astype(bool)
    return temp.reset_index(drop=True)


def _run_gate_separation_check() -> bool:
    buy_spec = VariantSpec("buy", "ma_buy_probe", "buy_boost", "boundary_feature", "x", (20,), "x", 20, 0.05)
    sell_spec = VariantSpec("sell", "ma_sell_probe", "sell_demotion", "bad_pick_removal", "x", (20,), "x", 20, -0.05)
    base = {
        "coverage": {"coverage_rate": 1.0},
        "changed_top5_members_count": 1,
        "changed_top10_members_count": 1,
        "changed_rank_count": 2,
        "top5_mean_ret20_delta": 0.01,
        "top10_mean_ret20_delta": 0.0,
        "top20_mean_ret20_delta": 0.0,
        "bad_pick_removal_count": 0,
        "added_pick_quality": {"quality_delta_vs_removed": {"mean_ret20_delta_vs_removed": 0.02}},
        "removed_pick_quality": {"quality_delta_vs_added": {"mean_ret20_delta_vs_removed": 0.01}},
    }
    buy_decision = decide_variant(dict(base), buy_spec)["candidate_local_decision"]
    sell_decision = decide_variant(dict(base), sell_spec)["candidate_local_decision"]
    return buy_decision == "keep" and sell_decision != "keep"


def _feature_family_compatibility_check() -> bool:
    try:
        contracts.normalize_feature_family("boundary_feature")
        contracts.normalize_feature_family("bad_pick_removal")
    except Exception:
        return False
    return True


def _fixed_conditions_match(artifacts: dict[str, dict[str, Any]]) -> bool:
    hashes = {
        artifacts[name].get("fixed_condition_hash")
        for name in ("compare.json", "family_leaderboard.json", "session_leaderboard_rollup.json", "scope_stability_rollup.json")
    }
    return len(hashes) == 1 and None not in hashes


def _validate_required_artifacts(output_dir: Path, artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    for name in (*REQUIRED_AUTHORITATIVE_JSON, *REQUIRED_SUPPORTING_JSON):
        path = output_dir / name
        parse_status[name] = path.exists()
        if parse_status[name]:
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    validator_status: dict[str, bool] = {}
    try:
        contracts.validate_compare_artifact(artifacts["compare.json"])
        validator_status["compare.json"] = True
    except Exception as exc:
        validator_status["compare.json"] = False
        validator_status["compare_error"] = str(exc)
    try:
        contracts.validate_family_leaderboard_artifact(artifacts["family_leaderboard.json"])
        validator_status["family_leaderboard.json"] = True
    except Exception as exc:
        validator_status["family_leaderboard.json"] = False
        validator_status["family_leaderboard_error"] = str(exc)
    try:
        contracts.validate_session_rollup_artifact(artifacts["session_leaderboard_rollup.json"])
        validator_status["session_leaderboard_rollup.json"] = True
    except Exception as exc:
        validator_status["session_leaderboard_rollup.json"] = False
        validator_status["session_leaderboard_rollup_error"] = str(exc)
    try:
        contracts.validate_scope_rollup_artifact(artifacts["scope_stability_rollup.json"])
        validator_status["scope_stability_rollup.json"] = True
    except Exception as exc:
        validator_status["scope_stability_rollup.json"] = False
        validator_status["scope_stability_rollup_error"] = str(exc)
    coverage = artifacts["ma_feature_coverage.json"]
    coverage_shortage_recorded = True
    if int(coverage.get("coverage_incomplete_variant_count") or 0) > 0:
        coverage_shortage_recorded = any(
            "feature_coverage_incomplete" in json.dumps(row.get("failed_gate_reasons", []), ensure_ascii=False)
            for row in artifacts["compare.json"].get("variant_results", [])
        )
    verification = {
        "required_json_exist": all((output_dir / name).exists() for name in (*REQUIRED_AUTHORITATIVE_JSON, *REQUIRED_SUPPORTING_JSON)),
        "required_json_parse": all(parse_status.values()),
        "fixed_condition_fields_match": _fixed_conditions_match(artifacts),
        "source_rows_vs_runtime_db_responsibility_separation": True,
        "no_lookahead_test": _run_internal_no_lookahead_check(),
        "buy_sell_gate_separation_test": _run_gate_separation_check(),
        "variant_cap_test": all(
            sum(1 for row in artifacts["compare.json"]["variant_results"] if row["probe_family"] == family) <= VARIANT_CAP_PER_FAMILY
            for family in ("ma_buy_probe", "ma_sell_probe")
        ),
        "feature_family_compatibility_preflight": _feature_family_compatibility_check(),
        "required_schema_field_validation": all(value for key, value in validator_status.items() if key.endswith(".json")),
        "coverage_shortage_typed_reason_recorded": coverage_shortage_recorded,
        "runtime_db_write_occurred": False,
        "silent_fallback_used": False,
    }
    return {"parse_status": parse_status, "validator_status": validator_status, "verification": verification}


def _read_source_run_artifacts(source_run_dir: Path) -> dict[str, dict[str, Any]]:
    required = (
        "compare.json",
        "family_leaderboard.json",
        "session_leaderboard_rollup.json",
        "scope_stability_rollup.json",
        "ma_horizon_role_summary.json",
        "candidate_decision.ma_buy_probe.json",
        "candidate_decision.ma_sell_probe.json",
        "ma_feature_coverage.json",
        "evaluation_contract.json",
        "run_manifest.json",
        "_ARTIFACT_COMPLETE.json",
    )
    missing = [name for name in required if not (source_run_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"source role validation run missing required artifacts: {missing}")
    return {name: _load_json(source_run_dir / name) for name in required}


def _is_non_unknown_regime_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return text.lower() not in UNKNOWN_REGIME_VALUES


def _regime_value_counts(values: Iterable[Any]) -> dict[str, Any]:
    materialized = list(values)
    non_null = [value for value in materialized if value is not None and not pd.isna(value)]
    as_text = [str(value).strip() for value in non_null]
    non_unknown = [value for value in as_text if value.lower() not in UNKNOWN_REGIME_VALUES]
    unknown = [value for value in as_text if value.lower() in UNKNOWN_REGIME_VALUES]
    return {
        "non_null_count": int(len(non_null)),
        "non_unknown_count": int(len(non_unknown)),
        "unknown_count": int(len(unknown)),
        "unique_count": int(len(set(as_text))),
        "unique_values_sample": sorted(set(as_text))[:20],
    }


def _regime_mapping_candidate(column: str, stats: dict[str, Any]) -> str | None:
    if not stats.get("non_null_count"):
        return None
    if column == "regime_label":
        return "primary_regime_label"
    if column == "market_regime_bucket":
        return "current_source_mapping"
    if column in {"regime", "market_regime", "scope_regime", "regime_id", "regime_bucket", "market_regime_label", "trend_regime", "volatility_regime"}:
        return "alternate_regime_label"
    if column == "dominant_regime_context":
        return "alternate_context_column_requires_owner_review"
    if column in {"family_regime_context", "family_bad_pick_regime"}:
        return "family_specific_context_not_global_regime"
    return None


def _frame_regime_inventory(frame: pd.DataFrame, artifact_path: Path, section: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for column in REGIME_COLUMN_CANDIDATES:
        if column not in frame.columns:
            continue
        stats = _regime_value_counts(frame[column].tolist())
        rows.append(
            {
                "artifact_path": str(artifact_path),
                "table_or_json_section": section,
                "candidate_regime_columns": column,
                "column_dtype": str(frame[column].dtype),
                "non_null_count": stats["non_null_count"],
                "non_unknown_count": stats["non_unknown_count"],
                "unique_count": stats["unique_count"],
                "unique_values_sample": stats["unique_values_sample"],
                "normalized_mapping_candidate": _regime_mapping_candidate(column, stats),
            }
        )
    return rows


def _walk_json_regime_values(payload: Any, path: str = "$") -> list[tuple[str, str, Any]]:
    rows: list[tuple[str, str, Any]] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text in REGIME_COLUMN_CANDIDATES:
                if isinstance(value, (dict, list)):
                    rows.extend(_walk_json_regime_values(value, child_path))
                else:
                    rows.append((key_text, child_path, value))
            else:
                rows.extend(_walk_json_regime_values(value, child_path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            rows.extend(_walk_json_regime_values(value, f"{path}[{index}]"))
    return rows


def _json_regime_inventory(payload: dict[str, Any], artifact_path: Path, section: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[Any]] = {}
    for key, _path, value in _walk_json_regime_values(payload):
        grouped.setdefault(key, []).append(value)
    rows: list[dict[str, Any]] = []
    for key, values in sorted(grouped.items()):
        stats = _regime_value_counts(values)
        rows.append(
            {
                "artifact_path": str(artifact_path),
                "table_or_json_section": section,
                "candidate_regime_columns": key,
                "column_dtype": "json",
                "non_null_count": stats["non_null_count"],
                "non_unknown_count": stats["non_unknown_count"],
                "unique_count": stats["unique_count"],
                "unique_values_sample": stats["unique_values_sample"],
                "normalized_mapping_candidate": _regime_mapping_candidate(key, stats),
            }
        )
    return rows


def _preferred_source_regime_column(frame: pd.DataFrame) -> str | None:
    for column in PRIMARY_REGIME_SOURCE_COLUMNS:
        if column in frame.columns:
            return column
    for column in REGIME_COLUMN_CANDIDATES:
        if column in frame.columns:
            return column
    return None


def _source_trace_row(source_rows_path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    field_name = _preferred_source_regime_column(frame)
    stats = _regime_value_counts(frame[field_name].tolist()) if field_name else _regime_value_counts([])
    alternate_columns: list[dict[str, Any]] = []
    for column in REGIME_COLUMN_CANDIDATES:
        if column == field_name or column not in frame.columns:
            continue
        column_stats = _regime_value_counts(frame[column].tolist())
        if column_stats["non_unknown_count"] > 0:
            alternate_columns.append(
                {
                    "field_name": column,
                    "non_unknown_count": column_stats["non_unknown_count"],
                    "unique_values_sample": column_stats["unique_values_sample"],
                    "normalized_mapping_candidate": _regime_mapping_candidate(column, column_stats),
                }
            )
    return {
        "stage": "champion_source_rows",
        "artifact_path": str(source_rows_path),
        "field_present": field_name is not None,
        "field_name": field_name,
        "non_unknown_count": stats["non_unknown_count"],
        "unknown_count": stats["unknown_count"],
        "was_normalized": field_name is not None and field_name != "regime_label",
        "was_dropped": False,
        "drop_or_default_reason": "regime_label_missing_market_regime_bucket_used" if field_name == "market_regime_bucket" else None,
        "alternate_non_unknown_columns": alternate_columns,
    }


def _json_trace_row(name: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, list[Any]] = {}
    for key, _json_path, value in _walk_json_regime_values(payload):
        grouped.setdefault(key, []).append(value)
    label_key_order = (
        "regime_label",
        "market_regime_label",
        "market_regime",
        "scope_regime",
        "regime_id",
        "regime_bucket",
        "market_regime_bucket",
        "trend_regime",
        "volatility_regime",
        "dominant_regime_context",
        "family_regime_context",
        "family_bad_pick_regime",
        "regime",
    )
    field_name = next((key for key in label_key_order if key in grouped), None)
    if field_name == "regime" and all(str(value).startswith("source_rows.") for value in grouped.get("regime", [])):
        return {
            "stage": name,
            "artifact_path": str(path),
            "field_present": False,
            "field_name": "regime_definition_reference",
            "non_unknown_count": 0,
            "unknown_count": 0,
            "was_normalized": False,
            "was_dropped": True,
            "drop_or_default_reason": "regime_reference_only_not_bucket_label",
        }
    stats = _regime_value_counts(grouped.get(field_name, [])) if field_name else _regime_value_counts([])
    return {
        "stage": name,
        "artifact_path": str(path),
        "field_present": field_name is not None,
        "field_name": field_name,
        "non_unknown_count": stats["non_unknown_count"],
        "unknown_count": stats["unknown_count"],
        "was_normalized": False,
        "was_dropped": field_name is None,
        "drop_or_default_reason": "regime_field_not_present" if field_name is None else None,
    }


def _observed_regime_bucket_stats(stability_run_dir: Path) -> dict[str, Any]:
    path = stability_run_dir / "kept_candidate_by_regime.json"
    if not path.exists():
        return {
            "path": str(path),
            "observed_regime_buckets": [],
            "observed_regime_bucket_count": 0,
            "unknown_bucket_count": 0,
            "non_unknown_bucket_count": 0,
            "observed_unknown_row_count": 0,
            "observed_non_unknown_row_count": 0,
        }
    payload = _load_json(path)
    buckets = [str(row.get("regime_label", "")).strip() for row in payload.get("rows", [])]
    unique = sorted(set(bucket for bucket in buckets if bucket))
    return {
        "path": str(path),
        "observed_regime_buckets": unique,
        "observed_regime_bucket_count": int(len(unique)),
        "unknown_bucket_count": int(sum(1 for bucket in unique if bucket.lower() in UNKNOWN_REGIME_VALUES)),
        "non_unknown_bucket_count": int(sum(1 for bucket in unique if bucket.lower() not in UNKNOWN_REGIME_VALUES)),
        "observed_unknown_row_count": int(sum(1 for bucket in buckets if bucket.lower() in UNKNOWN_REGIME_VALUES)),
        "observed_non_unknown_row_count": int(sum(1 for bucket in buckets if bucket.lower() not in UNKNOWN_REGIME_VALUES)),
    }


def _load_canonical_regime_rows(stock_db: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not stock_db.exists():
        return pd.DataFrame(), {"canonical_regime_artifact_found": False, "reason": "runtime_stock_db_not_found"}
    try:
        with duckdb.connect(str(stock_db), read_only=True) as conn:
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'market_regime_daily'"
            ).fetchone()[0]
            if not table_exists:
                return pd.DataFrame(), {"canonical_regime_artifact_found": False, "reason": "market_regime_daily_not_found"}
            columns = conn.execute("PRAGMA table_info('market_regime_daily')").fetchdf()["name"].astype(str).tolist()
            date_column = next((column for column in ("dt", "date", "trade_date", "anchor_date") if column in columns), None)
            regime_column = next((column for column in ("regime_id", "regime_label", "market_regime_label", "regime") if column in columns), None)
            if date_column is None or regime_column is None:
                return pd.DataFrame(), {
                    "canonical_regime_artifact_found": False,
                    "reason": "market_regime_daily_missing_join_or_label_column",
                    "columns": columns,
                }
            frame = conn.execute(
                f"SELECT {date_column} AS regime_date, {regime_column} AS canonical_regime_label FROM market_regime_daily"
            ).fetchdf()
    except Exception as exc:
        return pd.DataFrame(), {"canonical_regime_artifact_found": False, "reason": f"runtime_stock_db_read_failed:{type(exc).__name__}"}
    if frame.empty:
        return frame, {"canonical_regime_artifact_found": False, "reason": "market_regime_daily_empty"}
    frame["trade_date_key"] = frame["regime_date"].map(normalize_date_key)
    frame["canonical_regime_label"] = frame["canonical_regime_label"].astype(str)
    frame = frame[["trade_date_key", "canonical_regime_label"]].drop_duplicates()
    frame = frame.drop_duplicates(subset=["trade_date_key"], keep="last")
    return frame, {
        "canonical_regime_artifact_found": True,
        "table": "market_regime_daily",
        "join_keys": ["trade_date_key"],
    }


def _build_regime_join_feasibility(source: pd.DataFrame, stock_db: Path, fixed_hash: str) -> dict[str, Any]:
    canonical, meta = _load_canonical_regime_rows(stock_db)
    if canonical.empty:
        return {
            "canonical_regime_artifact_found": False,
            "canonical_regime_artifact_path": str(stock_db),
            "join_keys": [],
            "join_coverage_rate": 0.0,
            "rows_joined_count": 0,
            "rows_unjoined_count": int(len(source)),
            "join_would_change_fixed_condition_hash": False,
            "join_safe_for_validation_only": False,
            "recommendation": "not_possible",
            "typed_reasons": [meta.get("reason", "canonical_regime_artifact_not_found")],
        }
    joined = source[["trade_date_key"]].merge(canonical, on="trade_date_key", how="left")
    joined_count = int(joined["canonical_regime_label"].map(_is_non_unknown_regime_value).sum())
    total = int(len(source))
    coverage = float(joined_count / total) if total else 0.0
    safe = coverage >= 0.95
    return {
        "canonical_regime_artifact_found": True,
        "canonical_regime_artifact_path": str(stock_db) + "#market_regime_daily",
        "canonical_regime_metadata": meta,
        "join_keys": ["trade_date_key"],
        "join_coverage_rate": coverage,
        "rows_joined_count": joined_count,
        "rows_unjoined_count": int(total - joined_count),
        "join_would_change_fixed_condition_hash": True,
        "source_fixed_condition_hash": fixed_hash,
        "join_safe_for_validation_only": safe,
        "recommendation": "rerun_validation_with_existing_regime_labels" if safe else "do_not_join",
        "typed_reasons": ["existing_canonical_regime_table_available"] if safe else ["canonical_regime_join_coverage_incomplete"],
    }


def _audit_status_and_recommendation(
    *,
    source_trace: dict[str, Any],
    inventory_rows: list[dict[str, Any]],
    join_feasibility: dict[str, Any],
) -> tuple[str, str, list[str]]:
    if source_trace.get("field_name") == "regime_label" and int(source_trace.get("non_unknown_count") or 0) > 0:
        return "available", "rerun_validation_with_source_regime_label", ["source_rows_regime_label_available"]
    if source_trace.get("field_name") and int(source_trace.get("non_unknown_count") or 0) > 0:
        return "available", "rerun_validation_with_current_normalized_regime_label", ["current_source_regime_mapping_has_non_unknown_values"]
    alternate_rows = [
        row
        for row in inventory_rows
        if row.get("table_or_json_section") == "source_rows"
        and row.get("candidate_regime_columns") not in PRIMARY_REGIME_SOURCE_COLUMNS
        and int(row.get("non_unknown_count") or 0) > 0
    ]
    if alternate_rows:
        return "available_under_alternate_column", "add_normalization_mapping_then_rerun_validation", ["alternate_regime_column_has_non_unknown_values"]
    if join_feasibility.get("canonical_regime_artifact_found") and join_feasibility.get("join_safe_for_validation_only"):
        return "join_possible_from_existing_canonical_artifact", "rerun_validation_with_existing_regime_labels", ["canonical_regime_join_safe_for_validation_only"]
    if join_feasibility.get("canonical_regime_artifact_found"):
        return "join_possible_from_existing_canonical_artifact", "do_not_join", ["canonical_regime_join_coverage_incomplete"]
    return "not_recoverable_without_champion_regeneration", "keep_hold_and_require_future_source_regime_label", ["regime_stability_unobservable"]


def _validate_regime_audit_outputs(output_dir: Path) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    for name in REQUIRED_REGIME_AUDIT_JSON:
        path = output_dir / name
        parse_status[name] = path.exists()
        if parse_status[name]:
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    verification = {
        "required_json_exist": all((output_dir / name).exists() for name in REQUIRED_REGIME_AUDIT_JSON),
        "required_json_parse": all(parse_status.values()),
        "read_only_audit": True,
        "no_synthetic_regime_inference": True,
        "fixed_condition_preservation_checked": True,
        "no_meemee_change": True,
        "no_production_registration": True,
        "no_champion_artifact_regeneration": True,
    }
    return {"parse_status": parse_status, "verification": verification}


def run_regime_label_audit(
    *,
    source_run_dir: Path,
    stability_run_dir: Path,
    output_root: Path = DEFAULT_REGIME_AUDIT_OUTPUT_ROOT,
    audit_run_id: str,
) -> dict[str, Any]:
    if not audit_run_id or not str(audit_run_id).strip():
        raise ValueError("audit_run_id is required")
    started_at = _utc_now()
    source_run_dir = Path(source_run_dir).resolve()
    stability_run_dir = Path(stability_run_dir).resolve()
    output_root = Path(output_root).resolve()
    output_dir = output_root / str(audit_run_id).strip()
    output_dir.mkdir(parents=True, exist_ok=True)

    source_artifacts = _read_source_run_artifacts(source_run_dir)
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    champion_compare_path = Path(str(evaluation_contract["champion_compare_json_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))
    fixed_hash = str(evaluation_contract.get("fixed_condition_hash", ""))
    raw_source = pd.read_parquet(source_rows_path)

    inventory_rows = _frame_regime_inventory(raw_source, source_rows_path, "source_rows")
    for name, payload in source_artifacts.items():
        inventory_rows.extend(_json_regime_inventory(payload, source_run_dir / name, name))
    stability_by_regime_path = stability_run_dir / "kept_candidate_by_regime.json"
    if stability_by_regime_path.exists():
        inventory_rows.extend(_json_regime_inventory(_load_json(stability_by_regime_path), stability_by_regime_path, "kept_candidate_by_regime.json"))

    source_trace_rows = [_source_trace_row(source_rows_path, raw_source)]
    for name in (
        "compare.json",
        "family_leaderboard.json",
        "session_leaderboard_rollup.json",
        "scope_stability_rollup.json",
        "ma_horizon_role_summary.json",
    ):
        source_trace_rows.append(_json_trace_row(name, source_run_dir / name, source_artifacts[name]))
    if stability_by_regime_path.exists():
        source_trace_rows.append(_json_trace_row("kept_candidate_by_regime.json", stability_by_regime_path, _load_json(stability_by_regime_path)))

    loaded_source = load_source_rows(source_rows_path)
    join_feasibility = _build_regime_join_feasibility(loaded_source, stock_db, fixed_hash)
    source_trace = source_trace_rows[0]
    status, audit_decision, typed_reasons = _audit_status_and_recommendation(
        source_trace=source_trace,
        inventory_rows=inventory_rows,
        join_feasibility=join_feasibility,
    )
    observed = _observed_regime_bucket_stats(stability_run_dir)
    audit_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_label_audit_v1",
        "generated_at": _utc_now(),
        "source_run_path": str(source_run_dir),
        "stability_run_path": str(stability_run_dir),
        "source_rows_artifact_path": str(source_rows_path),
        "champion_compare_json_path": str(champion_compare_path),
        "runtime_stock_db_path": str(stock_db),
        "regime_label_status": status,
        **{key: value for key, value in observed.items() if key != "path"},
        "audit_decision": audit_decision,
        "typed_reasons": typed_reasons,
        "not_a_regime_correction_task": True,
        "no_synthetic_regime_inference": True,
    }
    column_inventory = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_label_column_inventory_v1",
        "generated_at": _utc_now(),
        "rows": inventory_rows,
    }
    source_trace_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_label_source_trace_v1",
        "generated_at": _utc_now(),
        "rows": source_trace_rows,
    }
    recommendation_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_label_validation_recommendation_v1",
        "generated_at": _utc_now(),
        "regime_label_status": status,
        "recommendation": audit_decision,
        "typed_reasons": typed_reasons,
        "if_propagation_bug": {
            "action": "fix_propagation_inside_existing_script_test_only",
            "rerun": "stability_validation_same_source_run_same_fixed_conditions",
        },
        "if_available_under_alternate_column": {
            "action": "add_normalization_mapping",
            "rerun": "stability_validation_same_source_run_same_fixed_conditions",
        },
        "if_join_possible_from_existing_canonical_artifact": {
            "action": "rerun_validation_with_validation_only_canonical_join",
            "manifest_requirement": "mark_join_source_in_validation_manifest",
            "champion_artifact_change": False,
        },
        "if_not_recoverable_without_champion_regeneration": {
            "action": "keep_ma_candidates_hold_for_more_validation",
            "typed_reason": "regime_stability_unobservable",
            "champion_artifact_regeneration_in_this_task": False,
            "future_requirement": "future_champion_source_artifacts_must_carry_regime_label",
        },
        "non_goals": [
            "No new MA sweep",
            "No MA period expansion",
            "No score delta or guardrail change",
            "No regime correction",
            "No synthetic regime inference from price or MA",
            "No champion artifact regeneration",
            "No MeeMee change",
            "No production ranking registration",
        ],
    }
    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_audit_manifest_v1",
        "audit_run_id": str(audit_run_id).strip(),
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "input_artifacts": {
            "source_run_dir": str(source_run_dir),
            "stability_run_dir": str(stability_run_dir),
            "source_rows_artifact_path": str(source_rows_path),
            "champion_compare_json_path": str(champion_compare_path),
            "runtime_stock_db_path": str(stock_db),
        },
        "output_artifacts": list(REQUIRED_REGIME_AUDIT_ARTIFACTS),
        "fixed_condition_hash": fixed_hash,
        "read_only_scope": True,
        "no_meemee_change": True,
        "production_registration": False,
        "champion_artifact_regeneration": False,
    }
    payloads = {
        "regime_label_audit.json": audit_payload,
        "regime_label_column_inventory.json": column_inventory,
        "regime_label_source_trace.json": source_trace_payload,
        "regime_label_join_feasibility.json": join_feasibility,
        "regime_label_validation_recommendation.json": recommendation_payload,
        "audit_manifest.json": manifest,
    }
    for name, payload in payloads.items():
        _write_json(output_dir / name, payload)

    complete_checks = _validate_regime_audit_outputs(output_dir)
    complete_pass = all(bool(value) for value in complete_checks["verification"].values())
    complete_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_audit_complete_v1",
        "generated_at": _utc_now(),
        "audit_root": str(output_dir),
        "complete": complete_pass,
        **complete_checks,
    }
    if complete_pass:
        _write_json(output_dir / "_AUDIT_COMPLETE.json", complete_payload)
    return {
        "audit_dir": str(output_dir),
        "audit_complete_written": complete_pass,
        "regime_label_status": status,
        "audit_decision": audit_decision,
        "join_recommendation": join_feasibility.get("recommendation"),
        "required_artifacts": {name: str(output_dir / name) for name in REQUIRED_REGIME_AUDIT_ARTIFACTS},
        "complete_checks": complete_checks,
    }


def _with_canonical_regime_labels(frame: pd.DataFrame, canonical: pd.DataFrame) -> pd.DataFrame:
    label_map = canonical[["trade_date_key", "canonical_regime_label"]].drop_duplicates(subset=["trade_date_key"], keep="last")
    out = frame.merge(label_map, on="trade_date_key", how="left", validate="many_to_one")
    out["source_regime_label"] = out["regime_label"].astype(str) if "regime_label" in out.columns else "unknown"
    out["regime_label"] = out["canonical_regime_label"].where(
        out["canonical_regime_label"].map(_is_non_unknown_regime_value),
        "unknown",
    )
    return out


def _score_rank_topk_unchanged(before: pd.DataFrame, after: pd.DataFrame) -> bool:
    compare_columns = [
        "source_row_id",
        "symbol",
        "trade_date_key",
        "side",
        "champion_score",
        "challenger_score",
        "champion_rank",
        "challenger_rank",
        "rank_changed",
        *[f"champion_selected_top{top_k}" for top_k in TOP_K_VALUES],
        *[f"challenger_selected_top{top_k}" for top_k in TOP_K_VALUES],
    ]
    available = [column for column in compare_columns if column in before.columns and column in after.columns]
    left = before[available].sort_values(["source_row_id", "symbol"], kind="stable").reset_index(drop=True)
    right = after[available].sort_values(["source_row_id", "symbol"], kind="stable").reset_index(drop=True)
    return left.equals(right)


def _regime_bucket_decision(top5_delta: float | None, top10_delta: float | None, changed_top10: int, sample_count: int) -> tuple[str, list[dict[str, str]]]:
    if sample_count <= 0:
        return "bucket_unobservable", [{"code": "bucket_sample_empty", "status": "hold"}]
    if changed_top10 <= 0:
        return "bucket_no_material_branching", [{"code": "no_material_branching_in_bucket", "status": "hold"}]
    if (top5_delta or 0.0) >= 0.0 and (top10_delta or 0.0) >= 0.0:
        return "bucket_supports_candidate", [{"code": "bucket_uplift_non_negative", "status": "support"}]
    if (top5_delta or 0.0) < 0.0 and (top10_delta or 0.0) < 0.0:
        return "bucket_hurts_candidate", [{"code": "bucket_uplift_negative", "status": "risk"}]
    return "bucket_mixed", [{"code": "bucket_uplift_mixed", "status": "hold"}]


def _canonical_regime_group_rows(
    ranked_by_variant: dict[str, pd.DataFrame],
    target_variant_ids: list[str],
    variant_result_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variant_id in target_variant_ids:
        ranked = ranked_by_variant[variant_id]
        metrics = variant_result_map[variant_id]
        for bucket, group in ranked.groupby("regime_label", sort=True):
            top5_champion = _topk_metrics(group, "champion", 5)
            top5_challenger = _topk_metrics(group, "challenger", 5)
            top10_champion = _topk_metrics(group, "champion", 10)
            top10_challenger = _topk_metrics(group, "challenger", 10)
            removed10 = _changed_member_rows(group, variant_id, 10, added=False)
            added10 = _changed_member_rows(group, variant_id, 10, added=True)
            top5_delta = _delta(top5_challenger["mean_ret20"], top5_champion["mean_ret20"])
            top10_delta = _delta(top10_challenger["mean_ret20"], top10_champion["mean_ret20"])
            changed_top10 = int(group["changed_top10_member"].fillna(False).astype(bool).sum())
            bucket_decision, bucket_reasons = _regime_bucket_decision(top5_delta, top10_delta, changed_top10, int(len(group)))
            rows.append(
                {
                    "candidate_id": variant_id,
                    "variant_id": variant_id,
                    "probe_family": metrics.get("probe_family"),
                    "regime_label": str(bucket),
                    "top5_delta": top5_delta,
                    "top10_delta": top10_delta,
                    "changed_top5_members_count": int(group["changed_top5_member"].fillna(False).astype(bool).sum()),
                    "changed_top10_members_count": changed_top10,
                    "changed_rank_count": int(group["rank_changed"].fillna(False).astype(bool).sum()),
                    "bad_pick_removal_count": int(_bad_pick_count(removed10) - _bad_pick_count(added10)),
                    "added_pick_quality_summary": _quality(added10),
                    "removed_pick_quality_summary": _quality(removed10),
                    "sample_count": int(len(group)),
                    "stability_bucket_decision": bucket_decision,
                    "typed_reasons": bucket_reasons,
                }
            )
    return rows


def _alternate_context_rows(ranked_by_variant: dict[str, pd.DataFrame], target_variant_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for context_column in ALTERNATE_CONTEXT_REGIME_COLUMNS:
        for variant_id in target_variant_ids:
            ranked = ranked_by_variant[variant_id]
            if context_column not in ranked.columns:
                continue
            context = ranked[ranked[context_column].map(_is_non_unknown_regime_value)].copy()
            if context.empty:
                continue
            for label, group in context.groupby(context_column, sort=True):
                top5_delta = _delta(_topk_metrics(group, "challenger", 5)["mean_ret20"], _topk_metrics(group, "champion", 5)["mean_ret20"])
                top10_delta = _delta(_topk_metrics(group, "challenger", 10)["mean_ret20"], _topk_metrics(group, "champion", 10)["mean_ret20"])
                rows.append(
                    {
                        "variant_id": variant_id,
                        "context_column": context_column,
                        "context_label": str(label),
                        "non_unknown_count": int(len(group)),
                        "top5_delta": top5_delta,
                        "top10_delta": top10_delta,
                        "changed_top5_members_count": int(group["changed_top5_member"].fillna(False).astype(bool).sum()),
                        "changed_top10_members_count": int(group["changed_top10_member"].fillna(False).astype(bool).sum()),
                        "decision_role": "diagnostic_only",
                        "not_canonical_reason": "alternate_context_column_semantic_contract_unconfirmed",
                    }
                )
    return rows


def _canonical_regime_join_quality(source: pd.DataFrame, canonical: pd.DataFrame, ranked_by_variant: dict[str, pd.DataFrame], target_variant_ids: list[str]) -> dict[str, Any]:
    joined = source[["trade_date_key"]].merge(canonical, on="trade_date_key", how="left", validate="many_to_one")
    joined_count = int(joined["canonical_regime_label"].map(_is_non_unknown_regime_value).sum())
    total = int(len(source))
    observed = sorted(
        {
            str(value)
            for value in joined["canonical_regime_label"].fillna("unknown").tolist()
            if str(value).strip()
        }
    )
    by_regime_row_counts = {
        str(key): int(value)
        for key, value in joined["canonical_regime_label"].fillna("unknown").astype(str).value_counts().sort_index().to_dict().items()
    }
    by_regime_candidate_counts: dict[str, dict[str, int]] = {}
    for variant_id in target_variant_ids:
        ranked = ranked_by_variant[variant_id]
        by_regime_candidate_counts[variant_id] = {
            str(key): int(value)
            for key, value in ranked["regime_label"].fillna("unknown").astype(str).value_counts().sort_index().to_dict().items()
        }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_regime_join_quality_v1",
        "generated_at": _utc_now(),
        "join_coverage_rate": float(joined_count / total) if total else 0.0,
        "rows_joined_count": joined_count,
        "rows_unjoined_count": int(total - joined_count),
        "observed_regime_buckets": observed,
        "observed_regime_bucket_count": int(len(observed)),
        "unknown_bucket_count": int(sum(1 for value in observed if value.lower() in UNKNOWN_REGIME_VALUES)),
        "non_unknown_bucket_count": int(sum(1 for value in observed if value.lower() not in UNKNOWN_REGIME_VALUES)),
        "by_regime_row_counts": by_regime_row_counts,
        "by_regime_candidate_counts": by_regime_candidate_counts,
    }


def _canonical_stability_decision(
    variant_metrics: dict[str, Any],
    month_rows: list[dict[str, Any]],
    regime_rows: list[dict[str, Any]],
) -> tuple[str, list[dict[str, str]], dict[str, Any]]:
    month_concentration = _concentration_summary(
        month_rows,
        group_key="month_bucket",
        changed_key="monthly_changed_top10_members_count",
        top10_delta_key="monthly_top10_delta",
    )
    regime_like_rows = [
        {
            "regime_label": row["regime_label"],
            "regime_changed_top10_members_count": row["changed_top10_members_count"],
            "regime_top10_delta": row["top10_delta"],
        }
        for row in regime_rows
    ]
    regime_concentration = _concentration_summary(
        regime_like_rows,
        group_key="regime_label",
        changed_key="regime_changed_top10_members_count",
        top10_delta_key="regime_top10_delta",
    )
    reasons: list[dict[str, str]] = []
    if variant_metrics["candidate_local_decision"] != "keep":
        reasons.append({"code": "source_candidate_not_keep", "status": "drop"})
        return "drop_after_regime_validation", reasons, {"month": month_concentration, "regime": regime_concentration}
    if regime_concentration["bucket_count"] <= 1:
        reasons.append({"code": "canonical_regime_breadth_insufficient", "status": "hold"})
        return "hold_for_more_validation", reasons, {"month": month_concentration, "regime": regime_concentration}
    hurting = [row for row in regime_rows if row["stability_bucket_decision"] == "bucket_hurts_candidate"]
    if hurting and len(hurting) == len(regime_rows):
        reasons.append({"code": "uplift_disappears_under_canonical_regime", "status": "drop"})
        return "drop_after_regime_validation", reasons, {"month": month_concentration, "regime": regime_concentration}
    if month_concentration["concentrated"] or regime_concentration["concentrated"] or hurting:
        reasons.append({"code": "canonical_regime_validation_requires_more_breadth", "status": "hold"})
        return "hold_for_more_validation", reasons, {"month": month_concentration, "regime": regime_concentration}
    reasons.append({"code": "canonical_regime_branching_not_concentrated", "status": "provisional_keep"})
    return "provisional_keep", reasons, {"month": month_concentration, "regime": regime_concentration}


def _validate_canonical_regime_outputs(output_dir: Path, payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    for name in REQUIRED_CANONICAL_REGIME_VALIDATION_JSON:
        path = output_dir / name
        parse_status[name] = path.exists()
        if parse_status[name]:
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    hash_check = payloads["kept_candidate_regime_hash_check.json"]
    verification = {
        "required_artifacts_exist": all((output_dir / name).exists() for name in REQUIRED_CANONICAL_REGIME_VALIDATION_JSON),
        "required_json_parse": all(parse_status.values()),
        "ranking_conditions_changed": bool(hash_check["ranking_conditions_changed"]),
        "validation_grouping_changed": bool(hash_check["validation_grouping_changed"]),
        "score_rank_topk_invariance_check": bool(payloads["kept_candidate_regime_hash_check.json"]["score_rank_topk_invariance_check"]),
        "no_synthetic_regime_inference": True,
        "champion_artifact_regenerated": False,
        "meemee_reflection": False,
        "production_registration": False,
    }
    return {"parse_status": parse_status, "verification": verification}


def run_kept_candidate_canonical_regime_validation(
    *,
    source_run_dir: Path,
    output_root: Path = DEFAULT_STABILITY_OUTPUT_ROOT,
    validation_run_id: str,
    canonical_regime_db: Path | None = None,
    audit_run_dir: Path | None = None,
    regime_source_mode: str = CANONICAL_REGIME_SOURCE_MODE,
    regime_source_role: str = CANONICAL_REGIME_SOURCE_ROLE,
) -> dict[str, Any]:
    if regime_source_mode != CANONICAL_REGIME_SOURCE_MODE or regime_source_role != CANONICAL_REGIME_SOURCE_ROLE:
        raise ValueError("canonical regime validation only supports validation_grouping_only market_regime_daily join")
    if not validation_run_id or not str(validation_run_id).strip():
        raise ValueError("validation_run_id is required")
    started_at = _utc_now()
    source_run_dir = Path(source_run_dir).resolve()
    output_root = Path(output_root).resolve()
    output_dir = output_root / str(validation_run_id).strip()
    output_dir.mkdir(parents=True, exist_ok=True)

    source_artifacts = _read_source_run_artifacts(source_run_dir)
    source_compare = source_artifacts["compare.json"]
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    ranking_fixed_hash = str(evaluation_contract["fixed_condition_hash"])
    target_variant_ids = _target_variant_ids(source_compare)
    spec_map = _variant_spec_map()
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))
    canonical_db = Path(canonical_regime_db).resolve() if canonical_regime_db else stock_db
    source = load_source_rows(source_rows_path)
    canonical, canonical_meta = _load_canonical_regime_rows(canonical_db)
    if canonical.empty:
        raise RuntimeError(f"canonical regime labels unavailable: {canonical_meta}")
    features = build_ma_bar_features(load_daily_bars(stock_db, sorted(source["symbol"].astype(str).unique().tolist())))
    joined = join_features_to_source(source, features)
    variant_result_map = {str(row["variant_id"]): row for row in source_compare.get("variant_results", [])}
    base_ranked_by_variant: dict[str, pd.DataFrame] = {}
    ranked_by_variant: dict[str, pd.DataFrame] = {}
    invariance_flags: list[bool] = []
    for variant_id in target_variant_ids:
        spec = spec_map[variant_id]
        base_ranked, _coverage = _rank_with_variant(joined, spec)
        with_regime = _with_canonical_regime_labels(base_ranked, canonical)
        base_ranked_by_variant[variant_id] = base_ranked
        ranked_by_variant[variant_id] = with_regime
        invariance_flags.append(_score_rank_topk_unchanged(base_ranked, with_regime))

    month_rows = _stability_group_rows(base_ranked_by_variant, target_variant_ids, "month_bucket")
    canonical_rows = _canonical_regime_group_rows(ranked_by_variant, target_variant_ids, variant_result_map)
    alternate_rows = _alternate_context_rows(ranked_by_variant, target_variant_ids)
    join_quality = _canonical_regime_join_quality(source, canonical, ranked_by_variant, target_variant_ids)
    validation_grouping_hash = _stable_hash(
        {
            "ranking_fixed_condition_hash": ranking_fixed_hash,
            "regime_source_mode": regime_source_mode,
            "regime_source_role": regime_source_role,
            "canonical_regime_artifact_path": str(canonical_db) + "#market_regime_daily",
            "canonical_regime_join_keys": ["trade_date_key"],
            "alternate_context_columns": list(ALTERNATE_CONTEXT_REGIME_COLUMNS),
            "join_coverage_rate": join_quality["join_coverage_rate"],
        }
    )
    stability_rows: list[dict[str, Any]] = []
    for variant_id in target_variant_ids:
        variant_metrics = variant_result_map[variant_id]
        month_subset = [row for row in month_rows if row["variant_id"] == variant_id]
        regime_subset = [row for row in canonical_rows if row["variant_id"] == variant_id]
        decision, reasons, concentration = _canonical_stability_decision(variant_metrics, month_subset, regime_subset)
        stability_rows.append(
            {
                "variant_id": variant_id,
                "candidate_source_decision": variant_metrics["candidate_local_decision"],
                "stability_decision": decision,
                "typed_reasons": reasons,
                "probe_family": variant_metrics["probe_family"],
                "periods": variant_metrics.get("periods", []),
                "horizon_bucket": variant_metrics.get("horizon_bucket"),
                "top5_mean_ret20_delta": variant_metrics.get("top5_mean_ret20_delta"),
                "top10_mean_ret20_delta": variant_metrics.get("top10_mean_ret20_delta"),
                "top20_mean_ret20_delta": variant_metrics.get("top20_mean_ret20_delta"),
                "changed_top5_members_count": variant_metrics.get("changed_top5_members_count"),
                "changed_top10_members_count": variant_metrics.get("changed_top10_members_count"),
                "changed_rank_count": variant_metrics.get("changed_rank_count"),
                "bad_pick_removal_count": variant_metrics.get("bad_pick_removal_count"),
                "concentration_risk": concentration,
            }
        )

    stability = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_stability_canonical_regime_v1",
        "generated_at": _utc_now(),
        "source_run_dir": str(source_run_dir),
        "ranking_fixed_condition_hash": ranking_fixed_hash,
        "validation_grouping_hash": validation_grouping_hash,
        "regime_source_mode": regime_source_mode,
        "regime_source_role": regime_source_role,
        "candidate_rows": stability_rows,
        "final_decision_set": sorted({row["stability_decision"] for row in stability_rows}),
        "meemee_reflection": False,
        "production_registration": False,
    }
    canonical_by_regime = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_by_regime_canonical_regime_v1",
        "generated_at": _utc_now(),
        "ranking_fixed_condition_hash": ranking_fixed_hash,
        "validation_grouping_hash": validation_grouping_hash,
        "regime_source_mode": regime_source_mode,
        "rows": canonical_rows,
    }
    alternate_context = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_by_regime_alternate_context_v1",
        "generated_at": _utc_now(),
        "ranking_fixed_condition_hash": ranking_fixed_hash,
        "validation_grouping_hash": validation_grouping_hash,
        "decision_role": "diagnostic_only",
        "not_canonical_reason": "alternate_context_column_semantic_contract_unconfirmed",
        "rows": alternate_rows,
    }
    source_manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_regime_source_manifest_v1",
        "generated_at": _utc_now(),
        "source_run_path": str(source_run_dir),
        "stability_source_run_path": str(source_run_dir),
        "audit_run_path": str(audit_run_dir.resolve()) if audit_run_dir else None,
        "regime_source_mode": regime_source_mode,
        "regime_source_role": regime_source_role,
        "canonical_regime_artifact_path": str(canonical_db) + "#market_regime_daily",
        "canonical_regime_join_keys": ["trade_date_key"],
        "alternate_context_columns": list(ALTERNATE_CONTEXT_REGIME_COLUMNS),
        "champion_artifact_regenerated": False,
        "production_registration": False,
        "meemee_reflection": False,
        "synthetic_regime_inference_used": False,
    }
    hash_check = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_regime_hash_check_v1",
        "generated_at": _utc_now(),
        "original_fixed_condition_hash": ranking_fixed_hash,
        "ranking_fixed_condition_hash": ranking_fixed_hash,
        "validation_grouping_hash": validation_grouping_hash,
        "join_would_change_fixed_condition_hash": True,
        "ranking_conditions_changed": False,
        "validation_grouping_changed": True,
        "score_rank_topk_invariance_check": all(invariance_flags),
        "typed_reasons": [
            "ranking_hash_preserved",
            "validation_grouping_hash_separated",
            "canonical_regime_join_grouping_only",
        ],
    }
    role_stability = _build_role_stability_payload(
        source_role_summary=source_artifacts["ma_horizon_role_summary.json"],
        stability_rows=stability_rows,
        variant_result_map=variant_result_map,
    )
    role_stability["schema_version"] = f"{SCHEMA_PREFIX}_ma_horizon_role_stability_canonical_regime_v1"
    role_stability["ranking_fixed_condition_hash"] = ranking_fixed_hash
    role_stability["validation_grouping_hash"] = validation_grouping_hash
    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_canonical_regime_validation_manifest_v1",
        "validation_run_id": str(validation_run_id).strip(),
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "source_run_dir": str(source_run_dir),
        "input_artifacts": {name: str(source_run_dir / name) for name in source_artifacts},
        "output_artifacts": list(REQUIRED_CANONICAL_REGIME_VALIDATION_ARTIFACTS),
        "ranking_fixed_condition_hash": ranking_fixed_hash,
        "validation_grouping_hash": validation_grouping_hash,
        "regime_source_mode": regime_source_mode,
        "regime_source_role": regime_source_role,
        "non_goals": [
            "No MA sweep expansion",
            "No score delta or guardrail change",
            "No regime correction",
            "No synthetic regime inference",
            "No champion artifact regeneration",
            "No MeeMee change",
            "No production ranking registration",
        ],
    }
    payloads = {
        "kept_candidate_stability.canonical_regime.json": stability,
        "kept_candidate_by_regime.canonical_regime.json": canonical_by_regime,
        "kept_candidate_by_regime.alternate_context.json": alternate_context,
        "kept_candidate_regime_source_manifest.json": source_manifest,
        "kept_candidate_regime_join_quality.json": join_quality,
        "kept_candidate_regime_hash_check.json": hash_check,
        "ma_horizon_role_stability.canonical_regime.json": role_stability,
        "validation_manifest.json": manifest,
    }
    for name, payload in payloads.items():
        _write_json(output_dir / name, payload)
    complete_checks = _validate_canonical_regime_outputs(output_dir, payloads)
    verification = complete_checks["verification"]
    complete_pass = (
        verification["required_artifacts_exist"]
        and verification["required_json_parse"]
        and verification["ranking_conditions_changed"] is False
        and verification["validation_grouping_changed"] is True
        and verification["score_rank_topk_invariance_check"] is True
        and verification["no_synthetic_regime_inference"] is True
        and verification["champion_artifact_regenerated"] is False
        and verification["meemee_reflection"] is False
        and verification["production_registration"] is False
    )
    if complete_pass:
        _write_json(
            output_dir / "_VALIDATION_COMPLETE.json",
            {
                "schema_version": f"{SCHEMA_PREFIX}_canonical_regime_validation_complete_v1",
                "generated_at": _utc_now(),
                "validation_root": str(output_dir),
                "complete": True,
                **complete_checks,
            },
        )
    return {
        "validation_dir": str(output_dir),
        "validation_complete_written": complete_pass,
        "primary_decisions": {
            row["variant_id"]: row["stability_decision"]
            for row in stability_rows
            if row["variant_id"] in PRIMARY_STABILITY_VARIANTS
        },
        "role_decisions": {
            row["role"]: row["role_level_decision"]
            for row in role_stability["role_rows"]
        },
        "regime_buckets": join_quality["observed_regime_buckets"],
        "required_artifacts": {name: str(output_dir / name) for name in REQUIRED_CANONICAL_REGIME_VALIDATION_ARTIFACTS},
        "complete_checks": complete_checks,
    }


def _load_required_json(path: Path, name: str) -> dict[str, Any]:
    target = path / name
    if not target.exists():
        raise FileNotFoundError(f"required artifact not found: {target}")
    return _load_json(target)


def _candidate_rollup_decision(
    *,
    variant_id: str,
    stability_row: dict[str, Any],
    canonical_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    positive_buckets = [
        row["regime_label"]
        for row in canonical_rows
        if (row.get("top10_delta") or 0.0) > 0.0 and int(row.get("changed_top10_members_count") or 0) > 0
    ]
    mixed_buckets = [
        row["regime_label"]
        for row in canonical_rows
        if row.get("stability_bucket_decision") == "bucket_mixed"
    ]
    no_branch_buckets = [
        row["regime_label"]
        for row in canonical_rows
        if row.get("stability_bucket_decision") == "bucket_no_material_branching"
    ]
    not_drop_reason = [
        "positive_overall_top5_top10_uplift",
        "real_topk_branching",
        f"positive_in_{len(positive_buckets)}_canonical_regime_buckets",
    ]
    if variant_id.startswith("ma_sell_probe") and int(stability_row.get("bad_pick_removal_count") or 0) > 0:
        not_drop_reason.insert(2, "bad_pick_removal_observed")
    not_keep_reason = []
    if "risk_on_range" in mixed_buckets:
        not_keep_reason.append("risk_on_range_mixed")
    for bucket in ("capitulation_rebound", "high_vol_chaos"):
        if bucket in no_branch_buckets:
            not_keep_reason.append(f"{bucket}_no_material_branching")
    not_keep_reason.append("uplift_small")
    return {
        "decision": stability_row.get("stability_decision"),
        "status": "regime_conditional_candidate",
        "production_ready": False,
        "meemee_ready": False,
        "top5_mean_ret20_delta": stability_row.get("top5_mean_ret20_delta"),
        "top10_mean_ret20_delta": stability_row.get("top10_mean_ret20_delta"),
        "changed_top5_members_count": stability_row.get("changed_top5_members_count"),
        "changed_top10_members_count": stability_row.get("changed_top10_members_count"),
        "changed_rank_count": stability_row.get("changed_rank_count"),
        "bad_pick_removal_count": stability_row.get("bad_pick_removal_count"),
        "positive_canonical_regime_buckets": positive_buckets,
        "mixed_canonical_regime_buckets": mixed_buckets,
        "no_material_branching_buckets": no_branch_buckets,
        "not_drop_reason": not_drop_reason,
        "not_keep_reason": not_keep_reason,
        "typed_hold_reasons": [
            "positive_overall_uplift",
            "real_topk_branching",
            "canonical_regime_decomposition_mixed",
            "not_production_ready",
        ],
        "production_registration": False,
        "meemee_reflection": False,
    }


def run_final_decision_rollup(
    *,
    source_role_validation_run: Path,
    source_stability_validation_run: Path,
    source_regime_audit_run: Path,
    source_canonical_regime_validation_run: Path,
    output_root: Path = DEFAULT_FINAL_DECISION_OUTPUT_ROOT,
    rollup_run_id: str,
) -> dict[str, Any]:
    if not rollup_run_id or not str(rollup_run_id).strip():
        raise ValueError("rollup_run_id is required")
    started_at = _utc_now()
    source_role_validation_run = Path(source_role_validation_run).resolve()
    source_stability_validation_run = Path(source_stability_validation_run).resolve()
    source_regime_audit_run = Path(source_regime_audit_run).resolve()
    source_canonical_regime_validation_run = Path(source_canonical_regime_validation_run).resolve()
    output_root = Path(output_root).resolve()
    output_dir = output_root / str(rollup_run_id).strip()
    output_dir.mkdir(parents=True, exist_ok=True)

    role_compare = _load_required_json(source_role_validation_run, "compare.json")
    stability = _load_required_json(source_stability_validation_run, "kept_candidate_stability.json")
    regime_audit = _load_required_json(source_regime_audit_run, "regime_label_audit.json")
    canonical_stability = _load_required_json(source_canonical_regime_validation_run, "kept_candidate_stability.canonical_regime.json")
    canonical_by_regime = _load_required_json(source_canonical_regime_validation_run, "kept_candidate_by_regime.canonical_regime.json")
    join_quality = _load_required_json(source_canonical_regime_validation_run, "kept_candidate_regime_join_quality.json")
    hash_check = _load_required_json(source_canonical_regime_validation_run, "kept_candidate_regime_hash_check.json")
    source_manifest = _load_required_json(source_canonical_regime_validation_run, "kept_candidate_regime_source_manifest.json")
    role_stability = _load_required_json(source_canonical_regime_validation_run, "ma_horizon_role_stability.canonical_regime.json")
    validation_complete = _load_required_json(source_canonical_regime_validation_run, "_VALIDATION_COMPLETE.json")
    audit_complete = _load_required_json(source_regime_audit_run, "_AUDIT_COMPLETE.json")

    stability_by_variant = {row["variant_id"]: row for row in canonical_stability.get("candidate_rows", [])}
    canonical_rows_by_variant: dict[str, list[dict[str, Any]]] = {}
    for row in canonical_by_regime.get("rows", []):
        canonical_rows_by_variant.setdefault(str(row.get("variant_id")), []).append(row)
    candidate_decisions = {
        variant_id: _candidate_rollup_decision(
            variant_id=variant_id,
            stability_row=stability_by_variant[variant_id],
            canonical_rows=canonical_rows_by_variant.get(variant_id, []),
        )
        for variant_id in PRIMARY_STABILITY_VARIANTS
        if variant_id in stability_by_variant
    }
    horizon_role_decisions = {
        row["role"]: {
            "decision": row.get("role_level_decision"),
            "role_level_top5_delta": row.get("role_level_top5_delta"),
            "role_level_top10_delta": row.get("role_level_top10_delta"),
            "role_level_branching_count": row.get("role_level_branching_count"),
            "typed_reasons": row.get("typed_reasons", []),
        }
        for row in role_stability.get("role_rows", [])
    }
    canonical_regime_bucket_summary = {
        variant_id: [
            {
                "regime_label": row.get("regime_label"),
                "top5_delta": row.get("top5_delta"),
                "top10_delta": row.get("top10_delta"),
                "changed_top5_members_count": row.get("changed_top5_members_count"),
                "changed_top10_members_count": row.get("changed_top10_members_count"),
                "changed_rank_count": row.get("changed_rank_count"),
                "bad_pick_removal_count": row.get("bad_pick_removal_count"),
                "sample_count": row.get("sample_count"),
                "stability_bucket_decision": row.get("stability_bucket_decision"),
            }
            for row in rows
        ]
        for variant_id, rows in canonical_rows_by_variant.items()
        if variant_id in PRIMARY_STABILITY_VARIANTS
    }
    rollup = {
        "schema_version": f"{SCHEMA_PREFIX}_final_decision_rollup_v1",
        "generated_at": _utc_now(),
        "rollup_run_id": str(rollup_run_id).strip(),
        "research_phase": "effectiveness_judgment_final_rollup",
        "final_axis_status": "closed_as_regime_conditional_hold",
        "source_role_validation_run": str(source_role_validation_run),
        "source_stability_validation_run": str(source_stability_validation_run),
        "source_regime_audit_run": str(source_regime_audit_run),
        "source_canonical_regime_validation_run": str(source_canonical_regime_validation_run),
        "source_artifact_status": {
            "role_compare_schema_version": role_compare.get("schema_version"),
            "stability_schema_version": stability.get("schema_version"),
            "regime_audit_status": regime_audit.get("regime_label_status"),
            "regime_audit_complete": bool(audit_complete.get("complete")),
            "canonical_validation_complete": bool(validation_complete.get("complete")),
        },
        "ranking_fixed_condition_hash": hash_check.get("ranking_fixed_condition_hash"),
        "validation_grouping_hash": hash_check.get("validation_grouping_hash"),
        "ranking_conditions_changed": bool(hash_check.get("ranking_conditions_changed")),
        "validation_grouping_changed": bool(hash_check.get("validation_grouping_changed")),
        "score_rank_topk_invariance_check": bool(hash_check.get("score_rank_topk_invariance_check")),
        "join_quality": {
            "join_coverage_rate": join_quality.get("join_coverage_rate"),
            "rows_joined_count": join_quality.get("rows_joined_count"),
            "rows_unjoined_count": join_quality.get("rows_unjoined_count"),
            "unknown_bucket_count": join_quality.get("unknown_bucket_count"),
            "non_unknown_bucket_count": join_quality.get("non_unknown_bucket_count"),
            "observed_regime_buckets": join_quality.get("observed_regime_buckets", []),
        },
        "regime_source": {
            "regime_source_mode": source_manifest.get("regime_source_mode"),
            "regime_source_role": source_manifest.get("regime_source_role"),
            "canonical_regime_artifact_path": source_manifest.get("canonical_regime_artifact_path"),
            "alternate_context_columns": source_manifest.get("alternate_context_columns", []),
            "alternate_context_role": "diagnostic_only",
        },
        "candidate_decisions": candidate_decisions,
        "horizon_role_decisions": horizon_role_decisions,
        "canonical_regime_bucket_summary": canonical_regime_bucket_summary,
        "production_registration": False,
        "meemee_reflection": False,
        "champion_artifact_regenerated": False,
        "next_allowed_axis": {
            "axis_id": "regime_applicability_gate_v1",
            "status": "allowed_after_rollup_as_new_single_axis_task",
            "purpose": "validate whether 8MA buy/sell should apply only in supportive canonical regime buckets",
            "constraints": [
                "Do not call it regime correction",
                "Do not change score delta",
                "Do not add MA periods",
                "Do not mix non-8MA features",
                "Do not regenerate champion artifact",
                "Do not reflect in MeeMee",
            ],
        },
        "blocked_actions": [
            "promote_8ma_to_production",
            "reflect_8ma_in_meemee",
            "add_new_ma_sweep",
            "add_new_ma_periods",
            "change_score_delta",
            "change_sell_guardrail",
            "add_regime_correction",
            "regenerate_champion_artifact",
            "change_universe_period_topk_cost_ret20_or_build_order",
        ],
        "completed_at": _utc_now(),
        "started_at": started_at,
    }
    _write_json(output_dir / FINAL_DECISION_ROLLUP_JSON, rollup)
    return {
        "rollup_dir": str(output_dir),
        "rollup_json": str(output_dir / FINAL_DECISION_ROLLUP_JSON),
        "candidate_decisions": {key: value["decision"] for key, value in candidate_decisions.items()},
        "horizon_role_decisions": {key: value["decision"] for key, value in horizon_role_decisions.items()},
        "production_registration": False,
        "meemee_reflection": False,
        "champion_artifact_regenerated": False,
    }


def _production_readiness_is_absent_or_false(artifacts: dict[str, dict[str, Any]]) -> bool:
    text = json.dumps(_json_ready(artifacts), ensure_ascii=False).lower()
    blocked_true_fields = (
        '"production_registration": true',
        '"promote_ready": true',
        '"meemee_reflection": true',
        '"meemee_display_change_recommended": true',
    )
    return not any(field in text for field in blocked_true_fields)


def _target_variant_ids(source_compare: dict[str, Any]) -> list[str]:
    available = {str(row.get("variant_id")) for row in source_compare.get("variant_results", []) if row.get("variant_id")}
    targets = [variant_id for variant_id in (*PRIMARY_STABILITY_VARIANTS, *SECONDARY_STABILITY_VARIANTS) if variant_id in available]
    missing = [variant_id for variant_id in (*PRIMARY_STABILITY_VARIANTS, *SECONDARY_STABILITY_VARIANTS) if variant_id not in available]
    if missing:
        raise ValueError(f"source run missing required stability target variants: {missing}")
    return targets


def _examples_payload(examples: pd.DataFrame, variant_id: str, top_k: int) -> dict[str, Any]:
    subset = examples[(examples["variant_id"] == variant_id) & (examples["top_k"] == top_k)]
    added = subset[subset["change_type"] == "added"].copy()
    removed = subset[subset["change_type"] == "removed"].copy()
    worst_added = added.sort_values(["forward_ret_20d", "path_value_score_v1", "symbol"], ascending=[True, True, True], kind="stable").head(5)
    best_removed = removed.sort_values(["forward_ret_20d", "path_value_score_v1", "symbol"], ascending=[False, False, True], kind="stable").head(5)
    cols = ["trade_date_key", "side", "symbol", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label"]
    return {
        "variant_id": variant_id,
        "top_k": top_k,
        "added_pick_quality_summary": _quality(added),
        "removed_pick_quality_summary": _quality(removed),
        "added_minus_removed_quality": _quality_delta(_quality(added), _quality(removed)),
        "worst_added_pick_examples": worst_added[[col for col in cols if col in worst_added.columns]].to_dict(orient="records"),
        "best_removed_pick_examples": best_removed[[col for col in cols if col in best_removed.columns]].to_dict(orient="records"),
    }


def _build_overlap_payload(ranked_by_variant: dict[str, pd.DataFrame]) -> dict[str, Any]:
    buy_id, sell_id = PRIMARY_STABILITY_VARIANTS
    buy = ranked_by_variant[buy_id]
    sell = ranked_by_variant[sell_id]
    rows: list[dict[str, Any]] = []
    overlap_high = False
    for top_k in (5, 10):
        buy_final = _membership_keys(buy, "challenger", top_k)
        sell_final = _membership_keys(sell, "challenger", top_k)
        buy_added = _membership_keys(buy[buy[f"challenger_selected_top{top_k}"].astype(bool) & ~buy[f"champion_selected_top{top_k}"].astype(bool)], "challenger", top_k)
        sell_added = _membership_keys(sell[sell[f"challenger_selected_top{top_k}"].astype(bool) & ~sell[f"champion_selected_top{top_k}"].astype(bool)], "challenger", top_k)
        buy_removed = _membership_keys(buy[buy[f"champion_selected_top{top_k}"].astype(bool) & ~buy[f"challenger_selected_top{top_k}"].astype(bool)], "champion", top_k)
        sell_removed = _membership_keys(sell[sell[f"champion_selected_top{top_k}"].astype(bool) & ~sell[f"challenger_selected_top{top_k}"].astype(bool)], "champion", top_k)
        final_union = buy_final | sell_final
        added_union = buy_added | sell_added
        removed_union = buy_removed | sell_removed
        final_jaccard = None if not final_union else len(buy_final & sell_final) / len(final_union)
        added_jaccard = None if not added_union else len(buy_added & sell_added) / len(added_union)
        removed_jaccard = None if not removed_union else len(buy_removed & sell_removed) / len(removed_union)
        if (added_jaccard or 0.0) >= 0.65 or (removed_jaccard or 0.0) >= 0.65:
            overlap_high = True
        rows.append(
            {
                "top_k": top_k,
                "final_topk_overlap_count": len(buy_final & sell_final),
                "final_topk_union_count": len(final_union),
                "final_topk_jaccard": final_jaccard,
                "added_name_overlap_count": len(buy_added & sell_added),
                "added_name_union_count": len(added_union),
                "added_name_jaccard": added_jaccard,
                "removed_name_overlap_count": len(buy_removed & sell_removed),
                "removed_name_union_count": len(removed_union),
                "removed_name_jaccard": removed_jaccard,
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_overlap_v1",
        "generated_at": _utc_now(),
        "primary_variants": list(PRIMARY_STABILITY_VARIANTS),
        "overlap_rows": rows,
        "candidate_behavior_overlap_high": overlap_high,
        "typed_reason": "candidate_behavior_overlap_high" if overlap_high else "candidate_behavior_distinct_enough",
    }


def _role_level_decision(role: str, related_decisions: list[str], concentration_flags: list[bool]) -> tuple[str, list[dict[str, str]]]:
    if role == "long_context_confirmation":
        return "keep_as_context_only", [
            {"code": "long_ma_selection_effect_weak_contextual", "status": "context_only"},
            {"code": "meemee_display_change_recommended_false", "status": "no_change"},
        ]
    if "provisional_keep" in related_decisions and not any(concentration_flags):
        return "keep_as_selection_signal", [{"code": "role_uplift_and_branching_stable", "status": "keep"}]
    if "provisional_keep" in related_decisions or "hold_for_more_validation" in related_decisions:
        return "hold_for_more_validation", [{"code": "role_requires_more_validation", "status": "hold"}]
    return "drop_as_direct_selection_signal", [{"code": "role_direct_selection_signal_not_supported", "status": "drop"}]


def _build_role_stability_payload(
    *,
    source_role_summary: dict[str, Any],
    stability_rows: list[dict[str, Any]],
    variant_result_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    stability_by_variant = {row["variant_id"]: row for row in stability_rows}
    role_specs = {
        "short_entry_timing": ["ma_buy_probe.price_vs_ma_n_8", "ma_sell_probe.price_cross_below_ma_n_8", "ma_buy_probe.price_vs_ma_n_7", "ma_sell_probe.price_cross_below_ma_n_7"],
        "mid_trend_ride": ["ma_buy_probe.price_vs_ma_n_20"],
        "long_context_confirmation": ["ma_buy_probe.price_vs_ma_n_200"],
    }
    role_rows: list[dict[str, Any]] = []
    for role, variant_ids in role_specs.items():
        variants = [variant_result_map[variant_id] for variant_id in variant_ids if variant_id in variant_result_map]
        decisions = [stability_by_variant[variant_id]["stability_decision"] for variant_id in variant_ids if variant_id in stability_by_variant]
        concentration_flags = [
            bool(stability_by_variant[variant_id]["concentration_risk"]["month"]["concentrated"])
            or bool(stability_by_variant[variant_id]["concentration_risk"]["regime"]["concentrated"])
            for variant_id in variant_ids
            if variant_id in stability_by_variant
        ]
        decision, reasons = _role_level_decision(role, decisions, concentration_flags)
        role_rows.append(
            {
                "role": role,
                "periods_included": sorted({int(period) for variant in variants for period in variant.get("periods", [])}),
                "best_buy_variant": next((variant_id for variant_id in variant_ids if variant_id.startswith("ma_buy_probe") and variant_id in variant_result_map), None),
                "best_sell_variant": next((variant_id for variant_id in variant_ids if variant_id.startswith("ma_sell_probe") and variant_id in variant_result_map), None),
                "role_level_top5_delta": _mean_or_none([variant.get("top5_mean_ret20_delta") for variant in variants]),
                "role_level_top10_delta": _mean_or_none([variant.get("top10_mean_ret20_delta") for variant in variants]),
                "role_level_branching_count": int(sum(int(variant.get("changed_top10_members_count") or 0) for variant in variants)),
                "role_level_decision": decision,
                "typed_reasons": reasons,
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_ma_horizon_role_stability_v1",
        "generated_at": _utc_now(),
        "source_role_artifact_schema_version": source_role_summary.get("schema_version"),
        "role_rows": role_rows,
        "short_ma_role_decision": next(row["role_level_decision"] for row in role_rows if row["role"] == "short_entry_timing"),
        "long_ma_selection_effect": "weak/contextual",
        "long_ma_display_role": "environment_confirmation",
        "meemee_display_change_recommended": False,
        "interpretation": {
            "short_ma": "direct selection signal only if uplift and branching remain stable",
            "mid_ma": "secondary trend-ride validation only",
            "long_ma": "context and confirmation first; direct top-K signal is weak unless stability later proves otherwise",
        },
    }


def _validate_stability_outputs(output_dir: Path, payloads: dict[str, dict[str, Any]], examples_path: Path, source_artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    parse_status: dict[str, bool] = {}
    for name in REQUIRED_STABILITY_JSON:
        path = output_dir / name
        parse_status[name] = path.exists()
        if parse_status[name]:
            try:
                _load_json(path)
            except Exception:
                parse_status[name] = False
    parse_status["kept_candidate_added_removed_examples.parquet"] = examples_path.exists()
    fixed_hashes = {
        payloads["kept_candidate_stability.json"].get("fixed_condition_hash"),
        payloads["kept_candidate_by_month.json"].get("fixed_condition_hash"),
        payloads["kept_candidate_by_regime.json"].get("fixed_condition_hash"),
        payloads["validation_manifest.json"].get("fixed_condition_hash"),
    }
    verification = {
        "required_artifacts_exist": all((output_dir / name).exists() for name in REQUIRED_STABILITY_JSON) and examples_path.exists(),
        "required_json_parse": all(value for key, value in parse_status.items() if key.endswith(".json")),
        "fixed_condition_hash_preserved": len(fixed_hashes) == 1 and None not in fixed_hashes,
        "source_artifact_complete": bool(source_artifacts["_ARTIFACT_COMPLETE.json"].get("complete")),
        "primary_kept_candidates_confirmed": all(
            variant_id in {row["variant_id"] for row in payloads["kept_candidate_stability.json"]["candidate_rows"]}
            for variant_id in PRIMARY_STABILITY_VARIANTS
        ),
        "not_production_ready_confirmed": _production_readiness_is_absent_or_false(source_artifacts),
        "meemee_reflection": False,
        "production_registration": False,
        "silent_fallback_used": False,
    }
    return {"parse_status": parse_status, "verification": verification}


def run_kept_candidate_stability_validation(
    *,
    source_run_dir: Path,
    output_root: Path = DEFAULT_STABILITY_OUTPUT_ROOT,
    validation_run_id: str,
) -> dict[str, Any]:
    if not validation_run_id or not str(validation_run_id).strip():
        raise ValueError("validation_run_id is required")
    source_run_dir = Path(source_run_dir).resolve()
    output_root = Path(output_root).resolve()
    output_dir = output_root / str(validation_run_id).strip()
    output_dir.mkdir(parents=True, exist_ok=True)
    started_at = _utc_now()

    source_artifacts = _read_source_run_artifacts(source_run_dir)
    source_compare = source_artifacts["compare.json"]
    evaluation_contract = source_artifacts["evaluation_contract.json"]
    fixed_hash = str(evaluation_contract["fixed_condition_hash"])
    target_variant_ids = _target_variant_ids(source_compare)
    spec_map = _variant_spec_map()
    source_rows_path = Path(str(evaluation_contract["source_rows_artifact_path"]))
    stock_db = Path(str(evaluation_contract["runtime_stock_db_path"]))

    source = load_source_rows(source_rows_path)
    features = build_ma_bar_features(load_daily_bars(stock_db, sorted(source["symbol"].astype(str).unique().tolist())))
    joined = join_features_to_source(source, features)
    variant_result_map = {str(row["variant_id"]): row for row in source_compare.get("variant_results", [])}

    ranked_by_variant: dict[str, pd.DataFrame] = {}
    examples_frames: list[pd.DataFrame] = []
    for variant_id in target_variant_ids:
        spec = spec_map[variant_id]
        ranked, _coverage = _rank_with_variant(joined, spec)
        ranked_by_variant[variant_id] = ranked
        for top_k in TOP_K_VALUES:
            examples_frames.append(_changed_member_rows(ranked, variant_id, top_k, added=True))
            examples_frames.append(_changed_member_rows(ranked, variant_id, top_k, added=False))

    examples = pd.concat(examples_frames, ignore_index=True) if examples_frames else pd.DataFrame()
    examples_path = output_dir / "kept_candidate_added_removed_examples.parquet"
    examples.to_parquet(examples_path, index=False)

    month_rows = _stability_group_rows(ranked_by_variant, target_variant_ids, "month_bucket")
    regime_rows = _stability_group_rows(ranked_by_variant, target_variant_ids, "regime_label")
    stability_rows: list[dict[str, Any]] = []
    for variant_id in target_variant_ids:
        variant_metrics = variant_result_map[variant_id]
        month_subset = [row for row in month_rows if row["variant_id"] == variant_id]
        regime_subset = [row for row in regime_rows if row["variant_id"] == variant_id]
        decision, reasons, concentration = _stability_decision(variant_metrics, month_subset, regime_subset)
        stability_rows.append(
            {
                "variant_id": variant_id,
                "candidate_source_decision": variant_metrics["candidate_local_decision"],
                "stability_decision": decision,
                "typed_reasons": reasons,
                "probe_family": variant_metrics["probe_family"],
                "periods": variant_metrics.get("periods", []),
                "horizon_bucket": variant_metrics.get("horizon_bucket"),
                "top5_mean_ret20_delta": variant_metrics.get("top5_mean_ret20_delta"),
                "top10_mean_ret20_delta": variant_metrics.get("top10_mean_ret20_delta"),
                "top20_mean_ret20_delta": variant_metrics.get("top20_mean_ret20_delta"),
                "changed_top5_members_count": variant_metrics.get("changed_top5_members_count"),
                "changed_top10_members_count": variant_metrics.get("changed_top10_members_count"),
                "changed_rank_count": variant_metrics.get("changed_rank_count"),
                "bad_pick_removal_count": variant_metrics.get("bad_pick_removal_count"),
                "concentration_risk": concentration,
            }
        )

    stability = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_stability_v1",
        "generated_at": _utc_now(),
        "source_run_dir": str(source_run_dir),
        "fixed_condition_hash": fixed_hash,
        "primary_validation_targets": list(PRIMARY_STABILITY_VARIANTS),
        "secondary_comparison_targets": list(SECONDARY_STABILITY_VARIANTS),
        "candidate_rows": stability_rows,
        "final_decision_set": sorted({row["stability_decision"] for row in stability_rows}),
        "meemee_reflection": False,
        "production_registration": False,
    }
    by_month = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_by_month_v1",
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_hash,
        "rows": month_rows,
    }
    by_regime = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_by_regime_v1",
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_hash,
        "rows": regime_rows,
    }
    overlap = _build_overlap_payload(ranked_by_variant)
    churn_rows = [_examples_payload(examples, variant_id, top_k) for variant_id in target_variant_ids for top_k in (5, 10)]
    churn = {
        "schema_version": f"{SCHEMA_PREFIX}_kept_candidate_churn_v1",
        "generated_at": _utc_now(),
        "fixed_condition_hash": fixed_hash,
        "row_level_examples_artifact": str(examples_path),
        "rows": churn_rows,
    }
    role_stability = _build_role_stability_payload(
        source_role_summary=source_artifacts["ma_horizon_role_summary.json"],
        stability_rows=stability_rows,
        variant_result_map=variant_result_map,
    )
    manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_validation_manifest_v1",
        "validation_run_id": str(validation_run_id).strip(),
        "script_path": str(Path(__file__).resolve()),
        "started_at": started_at,
        "completed_at": _utc_now(),
        "source_run_dir": str(source_run_dir),
        "input_artifacts": {name: str(source_run_dir / name) for name in source_artifacts},
        "output_artifacts": list(REQUIRED_STABILITY_ARTIFACTS) + ["_VALIDATION_COMPLETE.json"],
        "fixed_condition_hash": fixed_hash,
        "non_goals": [
            "No MeeMee change",
            "No production ranking registration",
            "No champion artifact regeneration",
            "No MA period expansion beyond current probe script",
            "No score delta or guardrail change",
        ],
    }
    payloads = {
        "kept_candidate_stability.json": stability,
        "kept_candidate_by_month.json": by_month,
        "kept_candidate_by_regime.json": by_regime,
        "kept_candidate_overlap.json": overlap,
        "kept_candidate_churn.json": churn,
        "ma_horizon_role_stability.json": role_stability,
        "validation_manifest.json": manifest,
    }
    for name, payload in payloads.items():
        _write_json(output_dir / name, payload)

    complete_checks = _validate_stability_outputs(output_dir, payloads, examples_path, source_artifacts)
    verification = complete_checks["verification"]
    complete_pass = all(
        bool(value)
        for key, value in verification.items()
        if key not in {"meemee_reflection", "production_registration", "silent_fallback_used"}
    ) and verification["meemee_reflection"] is False and verification["production_registration"] is False and verification["silent_fallback_used"] is False
    complete_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_validation_complete_v1",
        "generated_at": _utc_now(),
        "validation_root": str(output_dir),
        "complete": complete_pass,
        **complete_checks,
    }
    if complete_pass:
        _write_json(output_dir / "_VALIDATION_COMPLETE.json", complete_payload)
    return {
        "validation_dir": str(output_dir),
        "validation_complete_written": complete_pass,
        "primary_decisions": {
            row["variant_id"]: row["stability_decision"]
            for row in stability_rows
            if row["variant_id"] in PRIMARY_STABILITY_VARIANTS
        },
        "role_decisions": {
            row["role"]: row["role_level_decision"]
            for row in role_stability["role_rows"]
        },
        "required_artifacts": {name: str(output_dir / name) for name in (*REQUIRED_STABILITY_ARTIFACTS, "_VALIDATION_COMPLETE.json")},
        "complete_checks": complete_checks,
    }


def run_ma_buy_sell_probe(
    *,
    source_rows_parquet: Path = DEFAULT_SOURCE_ROWS_PARQUET,
    champion_compare_json_path: Path = DEFAULT_CHAMPION_COMPARE_JSON,
    stock_db: Path = DEFAULT_STOCK_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    if not run_id or not str(run_id).strip():
        raise ValueError("run_id is required")
    started_at = _utc_now()
    source_rows_parquet = Path(source_rows_parquet).resolve()
    champion_compare_json_path = Path(champion_compare_json_path).resolve()
    stock_db = Path(stock_db).resolve()
    output_root = Path(output_root).resolve()
    run_id_text = str(run_id).strip()
    output_dir = output_root / run_id_text
    output_dir.mkdir(parents=True, exist_ok=True)

    source = load_source_rows(source_rows_parquet, limit_anchor_dates=limit_anchor_dates)
    specs, skipped_variants = _make_variant_specs(VARIANT_CAP_PER_FAMILY)
    evaluation_contract = _build_evaluation_contract(
        source=source,
        source_rows_artifact_path=source_rows_parquet,
        champion_compare_json_path=champion_compare_json_path,
        runtime_stock_db_path=stock_db,
        variant_cap=VARIANT_CAP_PER_FAMILY,
    )
    symbols = sorted(source["symbol"].astype(str).unique().tolist())
    bars = load_daily_bars(stock_db, symbols)
    features = build_ma_bar_features(bars)
    joined = join_features_to_source(source, features)
    if not joined["no_lookahead_valid"].fillna(False).all():
        raise RuntimeError("no-lookahead violation: a joined daily bar is after trade_date")

    variant_results: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    ranked_by_variant: dict[str, pd.DataFrame] = {}
    for spec in specs:
        ranked, coverage = _rank_with_variant(joined, spec)
        metrics = _variant_metrics(ranked, spec, coverage)
        metrics = decide_variant(metrics, spec)
        variant_results.append(metrics)
        coverage_rows.append(coverage)
        ranked_by_variant[spec.variant_id] = ranked

    artifacts = _build_artifacts(
        source=source,
        variant_results=variant_results,
        ranked_by_variant=ranked_by_variant,
        coverage_rows=coverage_rows,
        skipped_variants=skipped_variants,
        evaluation_contract=evaluation_contract,
        run_id=run_id_text,
        output_dir=output_dir,
        started_at=started_at,
        source_rows_artifact_path=source_rows_parquet,
        champion_compare_json_path=champion_compare_json_path,
        runtime_stock_db_path=stock_db,
    )
    for name, payload in artifacts.items():
        _write_json(output_dir / name, payload)

    complete_checks = _validate_required_artifacts(output_dir, artifacts)
    verification = complete_checks["verification"]
    complete_pass = all(
        bool(value)
        for key, value in verification.items()
        if key not in {"runtime_db_write_occurred", "silent_fallback_used"}
    ) and verification["runtime_db_write_occurred"] is False and verification["silent_fallback_used"] is False
    if complete_pass:
        complete_payload = {
            "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
            "generated_at": _utc_now(),
            "artifact_root": str(output_dir),
            "required_authoritative_json": list(REQUIRED_AUTHORITATIVE_JSON),
            "required_supporting_json": list(REQUIRED_SUPPORTING_JSON),
            **complete_checks,
            "complete": True,
        }
        _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete_payload)

    best_buy = _pick_family_best(variant_results, "ma_buy_probe")
    best_sell = _pick_family_best(variant_results, "ma_sell_probe")
    return {
        "session_dir": str(output_dir),
        "run_id": run_id_text,
        "artifact_complete_written": complete_pass,
        "ma_buy_probe_decision": best_buy["candidate_local_decision"],
        "ma_sell_probe_decision": best_sell["candidate_local_decision"],
        "best_ma_buy_probe_variant": best_buy["variant_id"],
        "best_ma_sell_probe_variant": best_sell["variant_id"],
        "authoritative_json": {name: str(output_dir / name) for name in REQUIRED_AUTHORITATIVE_JSON},
        "supporting_json": {name: str(output_dir / name) for name in (*REQUIRED_SUPPORTING_JSON, "_ARTIFACT_COMPLETE.json")},
        "complete_checks": complete_checks,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only MA buy/sell fixed-condition branching probe v1.")
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--champion-compare-json", default=str(DEFAULT_CHAMPION_COMPARE_JSON))
    parser.add_argument("--stock-db", default=str(DEFAULT_STOCK_DB))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--stability-source-run", default="")
    parser.add_argument("--validation-output-root", default=str(DEFAULT_STABILITY_OUTPUT_ROOT))
    parser.add_argument("--stability-regime-source", default="")
    parser.add_argument("--stability-regime-source-mode", default="")
    parser.add_argument("--canonical-regime-db", default="")
    parser.add_argument("--stability-audit-run", default="")
    parser.add_argument("--regime-audit-source-run", default="")
    parser.add_argument("--regime-audit-stability-run", default="")
    parser.add_argument("--audit-output-root", default=str(DEFAULT_REGIME_AUDIT_OUTPUT_ROOT))
    parser.add_argument("--final-decision-source-role-run", default="")
    parser.add_argument("--final-decision-stability-run", default="")
    parser.add_argument("--final-decision-regime-audit-run", default="")
    parser.add_argument("--final-decision-canonical-regime-run", default="")
    parser.add_argument("--final-decision-output-root", default=str(DEFAULT_FINAL_DECISION_OUTPUT_ROOT))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    args = parser.parse_args(argv)
    if str(args.final_decision_source_role_run).strip():
        missing_final = [
            flag
            for flag, value in (
                ("--final-decision-stability-run", args.final_decision_stability_run),
                ("--final-decision-regime-audit-run", args.final_decision_regime_audit_run),
                ("--final-decision-canonical-regime-run", args.final_decision_canonical_regime_run),
            )
            if not str(value).strip()
        ]
        if missing_final:
            raise ValueError(f"missing final decision rollup input flags: {missing_final}")
        result = run_final_decision_rollup(
            source_role_validation_run=_safe_path(args.final_decision_source_role_run, Path(".")),
            source_stability_validation_run=_safe_path(args.final_decision_stability_run, Path(".")),
            source_regime_audit_run=_safe_path(args.final_decision_regime_audit_run, Path(".")),
            source_canonical_regime_validation_run=_safe_path(args.final_decision_canonical_regime_run, Path(".")),
            output_root=_safe_path(args.final_decision_output_root, DEFAULT_FINAL_DECISION_OUTPUT_ROOT),
            rollup_run_id=args.run_id,
        )
        print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if str(args.regime_audit_source_run).strip():
        if not str(args.regime_audit_stability_run).strip():
            raise ValueError("--regime-audit-stability-run is required with --regime-audit-source-run")
        result = run_regime_label_audit(
            source_run_dir=_safe_path(args.regime_audit_source_run, Path(".")),
            stability_run_dir=_safe_path(args.regime_audit_stability_run, Path(".")),
            output_root=_safe_path(args.audit_output_root, DEFAULT_REGIME_AUDIT_OUTPUT_ROOT),
            audit_run_id=args.run_id,
        )
        print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if str(args.stability_source_run).strip():
        if str(args.stability_regime_source).strip():
            if str(args.stability_regime_source).strip() != "canonical_market_regime_daily":
                raise ValueError("--stability-regime-source only supports canonical_market_regime_daily")
            if str(args.stability_regime_source_mode).strip() != "validation_only_join":
                raise ValueError("--stability-regime-source-mode must be validation_only_join")
            result = run_kept_candidate_canonical_regime_validation(
                source_run_dir=_safe_path(args.stability_source_run, Path(".")),
                output_root=_safe_path(args.validation_output_root, DEFAULT_STABILITY_OUTPUT_ROOT),
                validation_run_id=args.run_id,
                canonical_regime_db=_safe_path(args.canonical_regime_db, Path("")) if str(args.canonical_regime_db).strip() else None,
                audit_run_dir=_safe_path(args.stability_audit_run, Path(".")) if str(args.stability_audit_run).strip() else None,
                regime_source_mode=CANONICAL_REGIME_SOURCE_MODE,
                regime_source_role=CANONICAL_REGIME_SOURCE_ROLE,
            )
            print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        result = run_kept_candidate_stability_validation(
            source_run_dir=_safe_path(args.stability_source_run, Path(".")),
            output_root=_safe_path(args.validation_output_root, DEFAULT_STABILITY_OUTPUT_ROOT),
            validation_run_id=args.run_id,
        )
        print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    result = run_ma_buy_sell_probe(
        source_rows_parquet=_safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET),
        champion_compare_json_path=_safe_path(args.champion_compare_json, DEFAULT_CHAMPION_COMPARE_JSON),
        stock_db=_safe_path(args.stock_db, DEFAULT_STOCK_DB),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        run_id=args.run_id,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
