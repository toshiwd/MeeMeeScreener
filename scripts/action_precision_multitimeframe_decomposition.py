from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services import tradex_research_os_store as os_store

DEFAULT_OUTPUT_DIR = REPO_ROOT / "artifacts" / "research_inventory"
DEFAULT_SAMPLES_PATH = DEFAULT_OUTPUT_DIR / "action_precision_samples.parquet"
DEFAULT_THRESHOLDS_PATH = DEFAULT_OUTPUT_DIR / "action_precision_thresholds.json"
DEFAULT_HIERARCHICAL_PATHS = (
    REPO_ROOT / "artifacts" / "monthly_shape_memory" / "monthly_labels_hierarchical.parquet",
    REPO_ROOT / "artifacts" / "monthly_shape_memory" / "monthly_labels_hierarchical.production.parquet",
)

MULTITIMEFRAME_SAMPLES_ARTIFACT = "action_precision_multitimeframe_samples.parquet"
MULTITIMEFRAME_LONG_ARTIFACT = "action_precision_multitimeframe_long_decomposition.json"
MULTITIMEFRAME_SHORT_ARTIFACT = "action_precision_multitimeframe_short_decomposition.json"
MULTITIMEFRAME_PAIRWISE_ARTIFACT = "action_precision_multitimeframe_pairwise_effects.json"
MULTITIMEFRAME_TRIPLE_ARTIFACT = "action_precision_multitimeframe_triple_effects.json"
MULTITIMEFRAME_CANDIDATE_MAP_ARTIFACT = "action_precision_multitimeframe_candidate_map.json"
AUTHORITATIVE_DECISION_ARTIFACT = "authoritative_decision.action_precision_multitimeframe.json"

MIN_SAMPLE_THRESHOLD = 30
LONG_DIRECTIONAL_LABELS = ("BUY_STRONG", "BUY_WEAK", "NO_BUY")
SHORT_DIRECTIONAL_LABELS = ("SELL_STRONG", "SELL_WEAK", "NO_SELL")
LONG_TIMING_LABELS = ("BUY_TOO_EARLY", "BUY_ON_TIME", "BUY_TOO_LATE")
SHORT_TIMING_LABELS = ("SELL_TOO_EARLY", "SELL_ON_TIME", "SELL_TOO_LATE")


@dataclass(frozen=True)
class SplitContract:
    train_months: tuple[int, ...]
    tune_months: tuple[int, ...]
    validation_months: tuple[int, ...]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_text(value: Any, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    return out if math.isfinite(out) else fallback


def _parse_json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return os_store.write_json(path, payload)


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        frame.to_parquet(path, index=False)
    except Exception:
        con = duckdb.connect(":memory:")
        try:
            con.register("frame_df", frame)
            con.execute(f"COPY frame_df TO '{path.as_posix()}' (FORMAT PARQUET)")
        finally:
            con.close()
    return path


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing artifact: {path}")
    return os_store.read_json_object_strict(path, artifact_name=path.name)


def _resolve_hierarchical_path(explicit: str | None = None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if path.exists():
            return path
        raise FileNotFoundError(f"hierarchical parquet not found: {path}")
    for candidate in DEFAULT_HIERARCHICAL_PATHS:
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("could not resolve hierarchical parquet path")


def _load_samples(samples_path: Path) -> pd.DataFrame:
    if not samples_path.exists():
        raise FileNotFoundError(f"missing samples parquet: {samples_path}")
    frame = pd.read_parquet(samples_path)
    if frame.empty:
        raise RuntimeError(f"samples parquet is empty: {samples_path}")
    frame = frame.copy()
    frame["code"] = frame["code"].astype(str)
    frame["side"] = frame["side"].astype(str).str.lower()
    frame["signal_month"] = pd.to_numeric(frame["signal_month"], errors="coerce").astype("Int64")
    frame["dt"] = pd.to_numeric(frame["dt"], errors="coerce").astype("Int64")
    return frame


def _load_hierarchical_rows(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if frame.empty:
        raise RuntimeError(f"hierarchical parquet is empty: {path}")
    frame = frame.copy()
    frame["code"] = frame["code"].astype(str)
    frame["sample_month"] = pd.to_numeric(frame["sample_month"], errors="coerce").astype("Int64")
    return frame


def _load_split_contract(thresholds_path: Path) -> SplitContract:
    thresholds = _read_json(thresholds_path)
    split = thresholds.get("split_contract") if isinstance(thresholds.get("split_contract"), dict) else {}
    return SplitContract(
        train_months=tuple(int(month) for month in split.get("train_months") or []),
        tune_months=tuple(int(month) for month in split.get("tune_months") or []),
        validation_months=tuple(int(month) for month in split.get("validation_months") or []),
    )


def _fill_unknown(series: pd.Series) -> pd.Series:
    return series.map(lambda value: _safe_text(value))


def _label_sets(side_role: str) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    if side_role == "long":
        return LONG_DIRECTIONAL_LABELS, LONG_TIMING_LABELS, "BUY_STRONG"
    return SHORT_DIRECTIONAL_LABELS, SHORT_TIMING_LABELS, "SELL_STRONG"


def _count_and_rate_map(series: pd.Series, labels: tuple[str, ...]) -> tuple[dict[str, int], dict[str, float]]:
    if series.empty:
        counts = {label: 0 for label in labels}
        return counts, {label: 0.0 for label in labels}
    value_counts = series.fillna("unknown").astype(str).value_counts(dropna=False)
    counts = {label: int(value_counts.get(label, 0)) for label in labels}
    total = float(sum(counts.values())) or 1.0
    rates = {label: float(count / total) for label, count in counts.items()}
    return counts, rates


def _group_coverage_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "group_count": 0,
            "sample_count_total": 0,
            "usable_group_count": 0,
            "sparse_group_count": 0,
            "unstable_group_count": 0,
            "usable_sample_count": 0,
            "top_groups_by_sample_count": [],
        }
    ordered = sorted(rows, key=lambda row: (row.get("sample_count") or 0, row.get("directional_success_rate") or 0.0), reverse=True)
    return {
        "group_count": len(rows),
        "sample_count_total": int(sum(int(row.get("sample_count") or 0) for row in rows)),
        "usable_group_count": int(sum(1 for row in rows if row.get("coverage_status") == "usable")),
        "sparse_group_count": int(sum(1 for row in rows if row.get("coverage_status") == "sparse")),
        "unstable_group_count": int(sum(1 for row in rows if row.get("coverage_status") == "unstable")),
        "usable_sample_count": int(sum(int(row.get("sample_count") or 0) for row in rows if row.get("coverage_status") == "usable")),
        "top_groups_by_sample_count": [
            {
                "state_combination": row.get("state_combination"),
                "sample_count": int(row.get("sample_count") or 0),
                "directional_success_rate": row.get("directional_success_rate"),
                "timing_on_time_rate": row.get("timing_on_time_rate"),
                "coverage_status": row.get("coverage_status"),
            }
            for row in ordered[:5]
        ],
    }


def _equivalent_context_for_row(row: pd.Series) -> dict[str, Any]:
    basis = _parse_json(row.get("basis_payload"))
    reason = _parse_json(row.get("reason_snapshot"))
    score = _parse_json(row.get("score_snapshot"))

    monthly_up = _safe_float(basis.get("monthlyBreakoutUpProb"))
    monthly_down = _safe_float(basis.get("monthlyBreakoutDownProb"))
    weekly_up = _safe_float(basis.get("weeklyBreakoutUpProb"))
    weekly_down = _safe_float(basis.get("weeklyBreakoutDownProb"))
    monthly_range_prob = _safe_float(basis.get("monthlyRangeProb"))
    weekly_range_prob = _safe_float(basis.get("weeklyRangeProb"))
    monthly_box_state = _safe_text(basis.get("monthlyBoxState"), "monthly_range_mid")
    monthly_box_pos = _safe_float(basis.get("monthlyBoxPos"))
    market_regime = _safe_text(basis.get("marketRegime"), "unknown")
    change_day_score = 50.0 if reason else 50.0
    daily_candle_bull = any(bool(_safe_float(basis.get(key)) or 0.0) >= 0.5 for key in ("bullMarubozu", "morningStar", "reclaim60", "v60Core", "v60Strong"))
    daily_candle_bear = any(bool(_safe_float(basis.get(key)) or 0.0) >= 0.5 for key in ("bearMarubozu", "shootingStarLike"))
    monthly_env = 50.0
    if monthly_up is not None or monthly_down is not None:
        monthly_env = 50.0 + 25.0 * ((monthly_up or 0.0) - (monthly_down or 0.0))
    if monthly_range_prob is not None:
        monthly_env += 15.0 * (monthly_range_prob - 0.5)
    if monthly_box_pos is not None:
        monthly_env += 10.0 * (monthly_box_pos - 0.5)
    monthly_env = float(max(0.0, min(100.0, monthly_env)))
    weekly_trend = 50.0
    if weekly_up is not None or weekly_down is not None:
        weekly_trend = 50.0 + 30.0 * ((weekly_up or 0.0) - (weekly_down or 0.0))
    if weekly_range_prob is not None:
        weekly_trend += 10.0 * (1.0 - weekly_range_prob)
    weekly_trend = float(max(0.0, min(100.0, weekly_trend)))
    daily_exec = 50.0
    if daily_candle_bull:
        daily_exec += 18.0
    if daily_candle_bear:
        daily_exec -= 18.0
    if bool(_safe_float(basis.get("candleSmallBody")) or 0.0) >= 0.5:
        daily_exec += 4.0
    daily_exec = float(max(0.0, min(100.0, daily_exec)))
    monthly_state = "monthly_range_mid"
    if monthly_up is not None or monthly_down is not None:
        gap = (monthly_up or 0.0) - (monthly_down or 0.0)
        if gap >= 0.25:
            monthly_state = "monthly_up_top_warning" if (monthly_box_pos or 0.5) >= 0.55 else "monthly_up_mid"
        elif gap <= -0.25:
            monthly_state = "monthly_down_bottom_warning" if (monthly_box_pos or 0.5) <= 0.45 else "monthly_down_mid"
    weekly_state = "weekly_range_mid"
    if weekly_up is not None or weekly_down is not None:
        gap = (weekly_up or 0.0) - (weekly_down or 0.0)
        if gap >= 0.25:
            weekly_state = "weekly_up_late" if (weekly_range_prob or 0.5) < 0.5 else "weekly_up_mid"
        elif gap <= -0.25:
            weekly_state = "weekly_down_bottom_warning" if (weekly_range_prob or 0.5) > 0.5 else "weekly_down_mid"
    daily_state = "daily_reversal_up_candidate"
    if daily_candle_bear:
        daily_state = "daily_down_mid"
    elif daily_candle_bull:
        daily_state = "daily_up_mid"
    return {
        "join_mode": "equivalent",
        "hierarchical_join_source": "equivalent_mapping_from_confirmed_payload_fields",
        "monthly_main_state_ctx": monthly_state,
        "weekly_main_state_ctx": weekly_state,
        "daily_main_state_ctx": daily_state,
        "monthly_environment_score_ctx": monthly_env,
        "weekly_trend_score_ctx": weekly_trend,
        "daily_execution_score_ctx": daily_exec,
        "change_day_score_ctx": change_day_score,
        "monthly_change_day_flag_ctx": bool(reason.get("monthlyChangeDayFlag")) if isinstance(reason, dict) else False,
        "weekly_change_day_flag_ctx": bool(reason.get("weeklyChangeDayFlag")) if isinstance(reason, dict) else False,
        "daily_change_day_flag_ctx": bool(reason.get("dailyChangeDayFlag")) if isinstance(reason, dict) else False,
        "daily_trigger_flags_ctx": {
            "bull_marubozu": bool(_safe_float(basis.get("bullMarubozu")) or 0.0 >= 0.5),
            "bear_marubozu": bool(_safe_float(basis.get("bearMarubozu")) or 0.0 >= 0.5),
            "morning_star": bool(_safe_float(basis.get("morningStar")) or 0.0 >= 0.5),
            "shooting_star_like": bool(_safe_float(basis.get("shootingStarLike")) or 0.0 >= 0.5),
            "reclaim60": bool(_safe_float(basis.get("reclaim60")) or 0.0 >= 0.5),
            "v60_core": bool(_safe_float(basis.get("v60Core")) or 0.0 >= 0.5),
            "v60_strong": bool(_safe_float(basis.get("v60Strong")) or 0.0 >= 0.5),
        },
        "monthly_price_vs_ma12_state_ctx": None,
        "monthly_price_vs_ma24_state_ctx": None,
        "monthly_ma12_slope_state_ctx": None,
        "monthly_ma24_slope_state_ctx": None,
        "monthly_alignment_state_ctx": None,
        "weekly_price_vs_ma10_state_ctx": None,
        "weekly_price_vs_ma30_state_ctx": None,
        "weekly_price_vs_ma60_state_ctx": None,
        "weekly_ma10_slope_state_ctx": None,
        "weekly_ma30_slope_state_ctx": None,
        "weekly_alignment_state_ctx": None,
        "daily_price_vs_ma7_state_ctx": None,
        "daily_price_vs_ma20_state_ctx": None,
        "daily_price_vs_ma60_state_ctx": None,
        "daily_ma7_slope_state_ctx": None,
        "daily_ma20_slope_state_ctx": None,
        "daily_alignment_state_ctx": None,
        "winner_promotion_score_ctx": _safe_float(score.get("tradePriorityScore"), 50.0),
        "loser_removal_score_ctx": float(max(0.0, min(100.0, 100.0 - (_safe_float(score.get("tradePriorityScore"), 50.0) or 50.0)))),
        "hierarchical_native_exact_match": False,
        "hierarchical_exact_source": None,
        "hierarchical_native_monthly_main_state": None,
        "hierarchical_native_weekly_main_state": None,
        "hierarchical_native_daily_main_state": None,
        "hierarchical_native_monthly_environment_score": None,
        "hierarchical_native_weekly_trend_score": None,
        "hierarchical_native_daily_execution_score": None,
        "hierarchical_native_change_day_score": None,
        "hierarchical_native_monthly_change_day_flag": None,
        "hierarchical_native_weekly_change_day_flag": None,
        "hierarchical_native_daily_change_day_flag": None,
        "hierarchical_native_monthly_price_vs_ma12_state": None,
        "hierarchical_native_monthly_price_vs_ma24_state": None,
        "hierarchical_native_monthly_ma12_slope_state": None,
        "hierarchical_native_monthly_ma24_slope_state": None,
        "hierarchical_native_monthly_alignment_state": None,
        "hierarchical_native_weekly_price_vs_ma10_state": None,
        "hierarchical_native_weekly_price_vs_ma30_state": None,
        "hierarchical_native_weekly_price_vs_ma60_state": None,
        "hierarchical_native_weekly_ma10_slope_state": None,
        "hierarchical_native_weekly_ma30_slope_state": None,
        "hierarchical_native_weekly_alignment_state": None,
        "hierarchical_native_daily_price_vs_ma7_state": None,
        "hierarchical_native_daily_price_vs_ma20_state": None,
        "hierarchical_native_daily_price_vs_ma60_state": None,
        "hierarchical_native_daily_ma7_slope_state": None,
        "hierarchical_native_daily_ma20_slope_state": None,
        "hierarchical_native_daily_alignment_state": None,
        "hierarchical_native_winner_promotion_score": None,
        "hierarchical_native_loser_removal_score": None,
        "hierarchical_native_regime_tag": None,
    }


def _apply_exact_hierarchical_columns(row: pd.Series, hier_row: pd.Series | None) -> dict[str, Any]:
    if hier_row is None:
        return {}
    return {
        "join_mode": "exact",
        "hierarchical_join_source": "monthly_labels_hierarchical.parquet",
        "monthly_main_state_ctx": _safe_text(hier_row.get("monthly_main_state")),
        "weekly_main_state_ctx": _safe_text(hier_row.get("weekly_main_state")),
        "daily_main_state_ctx": _safe_text(hier_row.get("daily_main_state")),
        "monthly_environment_score_ctx": _safe_float(hier_row.get("monthly_environment_score"), 50.0),
        "weekly_trend_score_ctx": _safe_float(hier_row.get("weekly_trend_score"), 50.0),
        "daily_execution_score_ctx": _safe_float(hier_row.get("daily_execution_score"), 50.0),
        "change_day_score_ctx": _safe_float(hier_row.get("change_day_score"), 50.0),
        "monthly_change_day_flag_ctx": bool(hier_row.get("monthly_change_day_flag")),
        "weekly_change_day_flag_ctx": bool(hier_row.get("weekly_change_day_flag")),
        "daily_change_day_flag_ctx": bool(hier_row.get("daily_change_day_flag")),
        "daily_trigger_flags_ctx": {
            "daily_gap_up_flag": bool(hier_row.get("daily_gap_up_flag")),
            "daily_gap_down_flag": bool(hier_row.get("daily_gap_down_flag")),
            "daily_engulfing_bull_flag": bool(hier_row.get("daily_engulfing_bull_flag")),
            "daily_engulfing_bear_flag": bool(hier_row.get("daily_engulfing_bear_flag")),
            "daily_reclaim_ma20_flag": bool(hier_row.get("daily_reclaim_ma20_flag")),
            "daily_lose_ma20_flag": bool(hier_row.get("daily_lose_ma20_flag")),
            "daily_long_lower_wick_flag": bool(hier_row.get("daily_long_lower_wick_flag")),
            "daily_long_upper_wick_flag": bool(hier_row.get("daily_long_upper_wick_flag")),
            "daily_small_body_flag": bool(hier_row.get("daily_small_body_flag")),
        },
        "monthly_price_vs_ma12_state_ctx": _safe_text(hier_row.get("monthly_price_vs_ma12_state")),
        "monthly_price_vs_ma24_state_ctx": _safe_text(hier_row.get("monthly_price_vs_ma24_state")),
        "monthly_ma12_slope_state_ctx": _safe_text(hier_row.get("monthly_ma12_slope_state")),
        "monthly_ma24_slope_state_ctx": _safe_text(hier_row.get("monthly_ma24_slope_state")),
        "monthly_alignment_state_ctx": _safe_text(hier_row.get("monthly_alignment_state")),
        "weekly_price_vs_ma10_state_ctx": _safe_text(hier_row.get("weekly_price_vs_ma10_state")),
        "weekly_price_vs_ma30_state_ctx": _safe_text(hier_row.get("weekly_price_vs_ma30_state")),
        "weekly_price_vs_ma60_state_ctx": _safe_text(hier_row.get("weekly_price_vs_ma60_state")),
        "weekly_ma10_slope_state_ctx": _safe_text(hier_row.get("weekly_ma10_slope_state")),
        "weekly_ma30_slope_state_ctx": _safe_text(hier_row.get("weekly_ma30_slope_state")),
        "weekly_alignment_state_ctx": _safe_text(hier_row.get("weekly_alignment_state")),
        "daily_price_vs_ma7_state_ctx": _safe_text(hier_row.get("daily_price_vs_ma7_state")),
        "daily_price_vs_ma20_state_ctx": _safe_text(hier_row.get("daily_price_vs_ma20_state")),
        "daily_price_vs_ma60_state_ctx": _safe_text(hier_row.get("daily_price_vs_ma60_state")),
        "daily_ma7_slope_state_ctx": _safe_text(hier_row.get("daily_ma7_slope_state")),
        "daily_ma20_slope_state_ctx": _safe_text(hier_row.get("daily_ma20_slope_state")),
        "daily_alignment_state_ctx": _safe_text(hier_row.get("daily_alignment_state")),
        "winner_promotion_score_ctx": _safe_float(hier_row.get("winner_promotion_score"), 50.0),
        "loser_removal_score_ctx": _safe_float(hier_row.get("loser_removal_score"), 50.0),
        "hierarchical_native_exact_match": True,
        "hierarchical_exact_source": "code+signal_month exact join to monthly_labels_hierarchical.parquet",
        "hierarchical_native_monthly_main_state": _safe_text(hier_row.get("monthly_main_state")),
        "hierarchical_native_weekly_main_state": _safe_text(hier_row.get("weekly_main_state")),
        "hierarchical_native_daily_main_state": _safe_text(hier_row.get("daily_main_state")),
        "hierarchical_native_monthly_environment_score": _safe_float(hier_row.get("monthly_environment_score"), 50.0),
        "hierarchical_native_weekly_trend_score": _safe_float(hier_row.get("weekly_trend_score"), 50.0),
        "hierarchical_native_daily_execution_score": _safe_float(hier_row.get("daily_execution_score"), 50.0),
        "hierarchical_native_change_day_score": _safe_float(hier_row.get("change_day_score"), 50.0),
        "hierarchical_native_monthly_change_day_flag": bool(hier_row.get("monthly_change_day_flag")),
        "hierarchical_native_weekly_change_day_flag": bool(hier_row.get("weekly_change_day_flag")),
        "hierarchical_native_daily_change_day_flag": bool(hier_row.get("daily_change_day_flag")),
        "hierarchical_native_monthly_price_vs_ma12_state": _safe_text(hier_row.get("monthly_price_vs_ma12_state")),
        "hierarchical_native_monthly_price_vs_ma24_state": _safe_text(hier_row.get("monthly_price_vs_ma24_state")),
        "hierarchical_native_monthly_ma12_slope_state": _safe_text(hier_row.get("monthly_ma12_slope_state")),
        "hierarchical_native_monthly_ma24_slope_state": _safe_text(hier_row.get("monthly_ma24_slope_state")),
        "hierarchical_native_monthly_alignment_state": _safe_text(hier_row.get("monthly_alignment_state")),
        "hierarchical_native_weekly_price_vs_ma10_state": _safe_text(hier_row.get("weekly_price_vs_ma10_state")),
        "hierarchical_native_weekly_price_vs_ma30_state": _safe_text(hier_row.get("weekly_price_vs_ma30_state")),
        "hierarchical_native_weekly_price_vs_ma60_state": _safe_text(hier_row.get("weekly_price_vs_ma60_state")),
        "hierarchical_native_weekly_ma10_slope_state": _safe_text(hier_row.get("weekly_ma10_slope_state")),
        "hierarchical_native_weekly_ma30_slope_state": _safe_text(hier_row.get("weekly_ma30_slope_state")),
        "hierarchical_native_weekly_alignment_state": _safe_text(hier_row.get("weekly_alignment_state")),
        "hierarchical_native_daily_price_vs_ma7_state": _safe_text(hier_row.get("daily_price_vs_ma7_state")),
        "hierarchical_native_daily_price_vs_ma20_state": _safe_text(hier_row.get("daily_price_vs_ma20_state")),
        "hierarchical_native_daily_price_vs_ma60_state": _safe_text(hier_row.get("daily_price_vs_ma60_state")),
        "hierarchical_native_daily_ma7_slope_state": _safe_text(hier_row.get("daily_ma7_slope_state")),
        "hierarchical_native_daily_ma20_slope_state": _safe_text(hier_row.get("daily_ma20_slope_state")),
        "hierarchical_native_daily_alignment_state": _safe_text(hier_row.get("daily_alignment_state")),
        "hierarchical_native_winner_promotion_score": _safe_float(hier_row.get("winner_promotion_score"), 50.0),
        "hierarchical_native_loser_removal_score": _safe_float(hier_row.get("loser_removal_score"), 50.0),
        "hierarchical_native_regime_tag": _safe_text(hier_row.get("regime_tag")),
    }


def _build_joined_frame(samples: pd.DataFrame, hierarchical: pd.DataFrame) -> pd.DataFrame:
    relevant_hier = hierarchical[
        [
            "code",
            "sample_month",
            "monthly_main_state",
            "weekly_main_state",
            "daily_main_state",
            "monthly_environment_score",
            "weekly_trend_score",
            "daily_execution_score",
            "change_day_score",
            "monthly_change_day_flag",
            "weekly_change_day_flag",
            "daily_change_day_flag",
            "daily_gap_up_flag",
            "daily_gap_down_flag",
            "daily_engulfing_bull_flag",
            "daily_engulfing_bear_flag",
            "daily_reclaim_ma20_flag",
            "daily_lose_ma20_flag",
            "daily_long_lower_wick_flag",
            "daily_long_upper_wick_flag",
            "daily_small_body_flag",
            "monthly_price_vs_ma12_state",
            "monthly_price_vs_ma24_state",
            "monthly_ma12_slope_state",
            "monthly_ma24_slope_state",
            "monthly_alignment_state",
            "weekly_price_vs_ma10_state",
            "weekly_price_vs_ma30_state",
            "weekly_price_vs_ma60_state",
            "weekly_ma10_slope_state",
            "weekly_ma30_slope_state",
            "weekly_alignment_state",
            "daily_price_vs_ma7_state",
            "daily_price_vs_ma20_state",
            "daily_price_vs_ma60_state",
            "daily_ma7_slope_state",
            "daily_ma20_slope_state",
            "daily_alignment_state",
            "winner_promotion_score",
            "loser_removal_score",
            "regime_tag",
        ]
    ].copy()
    relevant_hier["sample_month"] = pd.to_numeric(relevant_hier["sample_month"], errors="coerce").astype("Int64")
    joined = samples.merge(
        relevant_hier,
        left_on=["code", "signal_month"],
        right_on=["code", "sample_month"],
        how="left",
        suffixes=("", "_hier"),
    )
    context_rows = []
    for row in joined.itertuples(index=False):
        row_dict = dict(row._asdict())
        exact_match = pd.notna(row_dict.get("sample_month")) and pd.notna(row_dict.get("monthly_main_state"))
        if exact_match:
            hier_row = pd.Series(
                {
                    "monthly_main_state": row_dict.get("monthly_main_state"),
                    "weekly_main_state": row_dict.get("weekly_main_state"),
                    "daily_main_state": row_dict.get("daily_main_state"),
                    "monthly_environment_score": row_dict.get("monthly_environment_score"),
                    "weekly_trend_score": row_dict.get("weekly_trend_score"),
                    "daily_execution_score": row_dict.get("daily_execution_score"),
                    "change_day_score": row_dict.get("change_day_score"),
                    "monthly_change_day_flag": row_dict.get("monthly_change_day_flag"),
                    "weekly_change_day_flag": row_dict.get("weekly_change_day_flag"),
                    "daily_change_day_flag": row_dict.get("daily_change_day_flag"),
                    "daily_gap_up_flag": row_dict.get("daily_gap_up_flag"),
                    "daily_gap_down_flag": row_dict.get("daily_gap_down_flag"),
                    "daily_engulfing_bull_flag": row_dict.get("daily_engulfing_bull_flag"),
                    "daily_engulfing_bear_flag": row_dict.get("daily_engulfing_bear_flag"),
                    "daily_reclaim_ma20_flag": row_dict.get("daily_reclaim_ma20_flag"),
                    "daily_lose_ma20_flag": row_dict.get("daily_lose_ma20_flag"),
                    "daily_long_lower_wick_flag": row_dict.get("daily_long_lower_wick_flag"),
                    "daily_long_upper_wick_flag": row_dict.get("daily_long_upper_wick_flag"),
                    "daily_small_body_flag": row_dict.get("daily_small_body_flag"),
                    "monthly_price_vs_ma12_state": row_dict.get("monthly_price_vs_ma12_state"),
                    "monthly_price_vs_ma24_state": row_dict.get("monthly_price_vs_ma24_state"),
                    "monthly_ma12_slope_state": row_dict.get("monthly_ma12_slope_state"),
                    "monthly_ma24_slope_state": row_dict.get("monthly_ma24_slope_state"),
                    "monthly_alignment_state": row_dict.get("monthly_alignment_state"),
                    "weekly_price_vs_ma10_state": row_dict.get("weekly_price_vs_ma10_state"),
                    "weekly_price_vs_ma30_state": row_dict.get("weekly_price_vs_ma30_state"),
                    "weekly_price_vs_ma60_state": row_dict.get("weekly_price_vs_ma60_state"),
                    "weekly_ma10_slope_state": row_dict.get("weekly_ma10_slope_state"),
                    "weekly_ma30_slope_state": row_dict.get("weekly_ma30_slope_state"),
                    "weekly_alignment_state": row_dict.get("weekly_alignment_state"),
                    "daily_price_vs_ma7_state": row_dict.get("daily_price_vs_ma7_state"),
                    "daily_price_vs_ma20_state": row_dict.get("daily_price_vs_ma20_state"),
                    "daily_price_vs_ma60_state": row_dict.get("daily_price_vs_ma60_state"),
                    "daily_ma7_slope_state": row_dict.get("daily_ma7_slope_state"),
                    "daily_ma20_slope_state": row_dict.get("daily_ma20_slope_state"),
                    "daily_alignment_state": row_dict.get("daily_alignment_state"),
                    "winner_promotion_score": row_dict.get("winner_promotion_score"),
                    "loser_removal_score": row_dict.get("loser_removal_score"),
                    "regime_tag": row_dict.get("regime_tag"),
                }
            )
            context = _apply_exact_hierarchical_columns(pd.Series(row_dict), hier_row)
        else:
            context = _equivalent_context_for_row(pd.Series(row_dict))
        row_dict.update(context)
        row_dict["exact_join_available"] = bool(exact_match)
        row_dict["hierarchical_join_key"] = f"{_safe_text(row_dict.get('code'))}:{_safe_text(row_dict.get('signal_month'))}"
        row_dict["monthly_main_state_ctx"] = _safe_text(row_dict.get("monthly_main_state_ctx"))
        row_dict["weekly_main_state_ctx"] = _safe_text(row_dict.get("weekly_main_state_ctx"))
        row_dict["daily_main_state_ctx"] = _safe_text(row_dict.get("daily_main_state_ctx"))
        row_dict["side_role"] = "long" if row_dict.get("side") == "buy" else "short"
        row_dict["monthly_state_key"] = f"monthly_main_state={row_dict['monthly_main_state_ctx']}"
        row_dict["weekly_state_key"] = f"weekly_main_state={row_dict['weekly_main_state_ctx']}"
        row_dict["daily_state_key"] = f"daily_main_state={row_dict['daily_main_state_ctx']}"
        row_dict["monthly_weekly_state_key"] = f"monthly_main_state={row_dict['monthly_main_state_ctx']}|weekly_main_state={row_dict['weekly_main_state_ctx']}"
        row_dict["weekly_daily_state_key"] = f"weekly_main_state={row_dict['weekly_main_state_ctx']}|daily_main_state={row_dict['daily_main_state_ctx']}"
        row_dict["monthly_daily_state_key"] = f"monthly_main_state={row_dict['monthly_main_state_ctx']}|daily_main_state={row_dict['daily_main_state_ctx']}"
        row_dict["triple_state_key"] = (
            f"monthly_main_state={row_dict['monthly_main_state_ctx']}|weekly_main_state={row_dict['weekly_main_state_ctx']}|daily_main_state={row_dict['daily_main_state_ctx']}"
        )
        context_rows.append(row_dict)
    frame = pd.DataFrame(context_rows)
    return frame


def _split_months(frame: pd.DataFrame, split: SplitContract) -> pd.Series:
    month = pd.to_numeric(frame["signal_month"], errors="coerce").astype("Int64")
    train = set(split.train_months)
    tune = set(split.tune_months)
    validation = set(split.validation_months)
    out = pd.Series(["other"] * len(frame), index=frame.index)
    out.loc[month.isin(train | tune)] = "train_tune"
    out.loc[month.isin(validation)] = "validation"
    return out


def _group_metrics(frame: pd.DataFrame, *, side_role: str, group_cols: list[str], level_name: str, min_sample_threshold: int, split: SplitContract) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    side_frame = frame.loc[frame["side_role"] == side_role].copy()
    if side_frame.empty:
        return []
    split_bucket = _split_months(side_frame, split)
    side_frame["split_bucket"] = split_bucket
    success_label = "BUY_STRONG" if side_role == "long" else "SELL_STRONG"
    timing_label_on = "BUY_ON_TIME" if side_role == "long" else "SELL_ON_TIME"
    timing_label_early = "BUY_TOO_EARLY" if side_role == "long" else "SELL_TOO_EARLY"
    timing_label_late = "BUY_TOO_LATE" if side_role == "long" else "SELL_TOO_LATE"
    mfe_col = "long_mfe_20" if side_role == "long" else "short_mfe_20"
    mae_col = "long_mae_20" if side_role == "long" else "short_mae_20"
    days_col = "days_to_long_mfe" if side_role == "long" else "days_to_short_mfe"
    timing_score_col = "long_timing_score" if side_role == "long" else "short_timing_score"
    directional_labels, timing_labels, primary_directional_label = _label_sets(side_role)
    timing_col = "long_timing_label" if side_role == "long" else "short_timing_label"

    grouped = side_frame.groupby(group_cols, dropna=False, sort=False)
    rows: list[dict[str, Any]] = []
    for key, group in grouped:
        if not isinstance(key, tuple):
            key = (key,)
        sample_count = int(len(group))
        train_tune = group.loc[group["split_bucket"] == "train_tune"]
        validation = group.loc[group["split_bucket"] == "validation"]
        train_tune_success = float((train_tune["directional_label"] == success_label).mean()) if len(train_tune) else None
        validation_success = float((validation["directional_label"] == success_label).mean()) if len(validation) else None
        success_rate = float((group["directional_label"] == success_label).mean()) if len(group) else None
        on_time_rate = float((group[timing_col] == timing_label_on).mean()) if len(group) else None
        early_rate = float((group[timing_col] == timing_label_early).mean()) if len(group) else None
        late_rate = float((group[timing_col] == timing_label_late).mean()) if len(group) else None
        directional_counts, directional_rates = _count_and_rate_map(group["directional_label"], directional_labels)
        timing_counts, timing_rates = _count_and_rate_map(group[timing_col], timing_labels)
        failure_labels = tuple(sorted({str(value) for value in group["failure_kind"].dropna().astype(str).unique()}))
        failure_kind_counts, failure_kind_rates = _count_and_rate_map(group["failure_kind"], failure_labels)
        row = {
            "signal_side": side_role,
            "grouping_level": level_name,
            "state_combination": "|".join(f"{col}={_safe_text(value)}" for col, value in zip(group_cols, key)),
            "sample_count": sample_count,
            "directional_success_rate": success_rate,
            "timing_on_time_rate": on_time_rate,
            "too_early_rate": early_rate,
            "too_late_rate": late_rate,
            "mfe_20_mean": float(group[mfe_col].mean()) if len(group) else None,
            "mfe_20_median": float(group[mfe_col].median()) if len(group) else None,
            "mae_20_mean": float(group[mae_col].mean()) if len(group) else None,
            "days_to_mfe_mean": float(group[days_col].mean()) if len(group) else None,
            "timing_score_mean": float(group[timing_score_col].mean()) if len(group) else None,
            "monthly_environment_score_mean": float(pd.to_numeric(group["monthly_environment_score_ctx"], errors="coerce").mean()) if len(group) else None,
            "weekly_trend_score_mean": float(pd.to_numeric(group["weekly_trend_score_ctx"], errors="coerce").mean()) if len(group) else None,
            "daily_execution_score_mean": float(pd.to_numeric(group["daily_execution_score_ctx"], errors="coerce").mean()) if len(group) else None,
            "change_day_score_mean": float(pd.to_numeric(group["change_day_score_ctx"], errors="coerce").mean()) if len(group) else None,
            "directional_label_counts": directional_counts,
            "directional_label_rates": directional_rates,
            "timing_label_counts": timing_counts,
            "timing_label_rates": timing_rates,
            "failure_kind_counts": failure_kind_counts,
            "failure_kind_rates": failure_kind_rates,
            "dominant_directional_label": max(directional_counts, key=directional_counts.get) if directional_counts else None,
            "dominant_timing_label": max(timing_counts, key=timing_counts.get) if timing_counts else None,
            "dominant_failure_kind": max(failure_kind_counts, key=failure_kind_counts.get) if failure_kind_counts else None,
            "primary_directional_label": primary_directional_label,
            "train_tune_sample_count": int(len(train_tune)),
            "validation_sample_count": int(len(validation)),
            "train_tune_directional_success_rate": train_tune_success,
            "validation_directional_success_rate": validation_success,
            "stable_sign_match": None if train_tune_success is None or validation_success is None else bool((train_tune_success - 0.5) * (validation_success - 0.5) >= 0.0),
            "coverage_status": "usable"
            if sample_count >= min_sample_threshold and len(train_tune) >= min_sample_threshold and len(validation) >= min_sample_threshold
            else "sparse"
            if sample_count < min_sample_threshold
            else "unstable",
            "sample_threshold": min_sample_threshold,
            "join_exact_rate": float(group["exact_join_available"].mean()) if len(group) else None,
        }
        rows.append(row)
    rows.sort(key=lambda item: (item["sample_count"], item["directional_success_rate"] or 0.0), reverse=True)
    return rows


def _recommend_candidate(side_role: str, family: str, row: dict[str, Any], *, long_soft_gain: float, short_soft_gain: float, min_sample_threshold: int) -> str:
    if int(row.get("sample_count") or 0) < min_sample_threshold:
        return "ignore_sparse"
    if row.get("coverage_status") == "unstable":
        return "diagnose_more"
    success_rate = float(row.get("directional_success_rate") or 0.0)
    timing_on_time_rate = float(row.get("timing_on_time_rate") or 0.0)
    too_late_rate = float(row.get("too_late_rate") or 0.0)
    mfe = float(row.get("mfe_20_mean") or 0.0)
    soft_gain = long_soft_gain if side_role == "long" else short_soft_gain
    if family.endswith("too_late"):
        if too_late_rate >= 0.30 and success_rate < 0.40:
            return "block_candidate"
        if too_late_rate >= 0.18 or timing_on_time_rate < 0.60:
            return "downgrade_candidate"
        return "diagnose_more"
    if success_rate < 0.30 and mfe < soft_gain:
        return "block_candidate"
    if success_rate < 0.50:
        return "downgrade_candidate"
    return "diagnose_more"


def _candidate_priority_score(side_role: str, family: str, row: dict[str, Any], *, long_soft_gain: float, short_soft_gain: float) -> float:
    success_rate = float(row.get("directional_success_rate") or 0.0)
    timing_on_time_rate = float(row.get("timing_on_time_rate") or 0.0)
    too_late_rate = float(row.get("too_late_rate") or 0.0)
    mfe = float(row.get("mfe_20_mean") or 0.0)
    mae = float(row.get("mae_20_mean") or 0.0)
    timing_score = float(row.get("timing_score_mean") or 0.0)
    soft_gain = long_soft_gain if side_role == "long" else short_soft_gain
    mfe_gap = max(0.0, 1.0 - min(1.0, mfe / max(soft_gain, 1e-9)))
    mae_gap = min(1.0, mae / max(soft_gain, 1e-9))
    timing_gap = max(0.0, 1.0 - min(1.0, timing_score / 100.0))
    if family.endswith("too_late"):
        score = 0.38 * too_late_rate + 0.28 * (1.0 - success_rate) + 0.18 * (1.0 - timing_on_time_rate) + 0.16 * mfe_gap
    else:
        score = 0.42 * (1.0 - success_rate) + 0.24 * mfe_gap + 0.16 * (1.0 - timing_on_time_rate) + 0.10 * mae_gap + 0.08 * timing_gap
    return float(max(0.0, min(100.0, score * 100.0)))


def _candidate_pool(rows: list[dict[str, Any]], *, side_role: str, family: str, long_soft_gain: float, short_soft_gain: float, min_sample_threshold: int) -> list[dict[str, Any]]:
    recommendation_priority = {
        "block_candidate": 3,
        "downgrade_candidate": 2,
        "diagnose_more": 1,
        "ignore_sparse": 0,
    }
    output: list[dict[str, Any]] = []
    for row in rows:
        recommendation = _recommend_candidate(
            side_role,
            family,
            row,
            long_soft_gain=long_soft_gain,
            short_soft_gain=short_soft_gain,
            min_sample_threshold=min_sample_threshold,
        )
        enriched = dict(row)
        enriched["recommendation"] = recommendation
        enriched["recommendation_priority"] = recommendation_priority.get(recommendation, 0)
        enriched["candidate_family"] = family
        enriched["candidate_priority_score"] = _candidate_priority_score(
            side_role,
            family,
            row,
            long_soft_gain=long_soft_gain,
            short_soft_gain=short_soft_gain,
        )
        output.append(enriched)
    output.sort(
        key=lambda row: (
            row.get("recommendation_priority") or 0,
            row.get("candidate_priority_score") or 0.0,
            row.get("sample_count") or 0,
        ),
        reverse=True,
    )
    for rank, row in enumerate(output, start=1):
        row["candidate_rank_in_family"] = rank
    return output


def _build_side_artifact(frame: pd.DataFrame, *, side_role: str, split: SplitContract, min_sample_threshold: int, long_soft_gain: float, short_soft_gain: float) -> dict[str, Any]:
    single_levels = {
        "monthly_only": ["monthly_main_state_ctx"],
        "weekly_only": ["weekly_main_state_ctx"],
        "daily_only": ["daily_main_state_ctx"],
    }
    pairwise_levels = {
        "monthly_weekly": ["monthly_main_state_ctx", "weekly_main_state_ctx"],
        "weekly_daily": ["weekly_main_state_ctx", "daily_main_state_ctx"],
        "monthly_daily": ["monthly_main_state_ctx", "daily_main_state_ctx"],
    }
    triple_levels = {"monthly_weekly_daily": ["monthly_main_state_ctx", "weekly_main_state_ctx", "daily_main_state_ctx"]}

    single = {name: _group_metrics(frame, side_role=side_role, group_cols=cols, level_name=name, min_sample_threshold=min_sample_threshold, split=split) for name, cols in single_levels.items()}
    pairwise = {name: _group_metrics(frame, side_role=side_role, group_cols=cols, level_name=name, min_sample_threshold=min_sample_threshold, split=split) for name, cols in pairwise_levels.items()}
    triple = {name: _group_metrics(frame, side_role=side_role, group_cols=cols, level_name=name, min_sample_threshold=min_sample_threshold, split=split) for name, cols in triple_levels.items()}

    candidate_rows = []
    for family_name, rows in [
        (f"{side_role}_too_late_candidates_multitimeframe", triple["monthly_weekly_daily"]),
        (f"{side_role}_weak_direction_candidates_multitimeframe", triple["monthly_weekly_daily"]),
    ]:
        family_rows = _candidate_pool(rows, side_role=side_role, family=family_name, long_soft_gain=long_soft_gain, short_soft_gain=short_soft_gain, min_sample_threshold=min_sample_threshold)
        candidate_rows.extend(family_rows)

    level_summary = {
        "single_timeframe": {name: _group_coverage_summary(rows) for name, rows in single.items()},
        "pairwise": {name: _group_coverage_summary(rows) for name, rows in pairwise.items()},
        "triple": {name: _group_coverage_summary(rows) for name, rows in triple.items()},
    }

    return {
        "schema_version": "tradex_action_precision_multitimeframe_decomposition_v1",
        "generated_at": _utc_now(),
        "signal_side": side_role,
        "minimum_sample_threshold": min_sample_threshold,
        "split_contract": {
            "train_months": list(split.train_months),
            "tune_months": list(split.tune_months),
            "validation_months": list(split.validation_months),
        },
        "level_summary": level_summary,
        "single_timeframe": single,
        "pairwise": pairwise,
        "triple": triple,
        "candidate_families": {
            f"{side_role}_too_late_candidates_multitimeframe": [row for row in candidate_rows if row["candidate_family"] == f"{side_role}_too_late_candidates_multitimeframe"],
            f"{side_role}_weak_direction_candidates_multitimeframe": [row for row in candidate_rows if row["candidate_family"] == f"{side_role}_weak_direction_candidates_multitimeframe"],
        },
    }


def build_multitimeframe_decomposition(*, samples_path: Path, hierarchical_path: Path, thresholds_path: Path) -> dict[str, Any]:
    samples = _load_samples(samples_path)
    hierarchical = _load_hierarchical_rows(hierarchical_path)
    split = _load_split_contract(thresholds_path)
    thresholds = _read_json(thresholds_path).get("thresholds") or {}
    long_soft_gain = float((thresholds.get("baseline") or {}).get("soft_gain_pct_long", 0.05))
    short_soft_gain = float((thresholds.get("baseline") or {}).get("soft_gain_pct_short", 0.05))

    joined = _build_joined_frame(samples, hierarchical)
    joined["join_exact_rate"] = joined["exact_join_available"].astype(float)

    long_frame = joined.loc[joined["side_role"] == "long"].copy()
    short_frame = joined.loc[joined["side_role"] == "short"].copy()

    long_artifact = _build_side_artifact(
        long_frame,
        side_role="long",
        split=split,
        min_sample_threshold=MIN_SAMPLE_THRESHOLD,
        long_soft_gain=long_soft_gain,
        short_soft_gain=short_soft_gain,
    )
    short_artifact = _build_side_artifact(
        short_frame,
        side_role="short",
        split=split,
        min_sample_threshold=MIN_SAMPLE_THRESHOLD,
        long_soft_gain=long_soft_gain,
        short_soft_gain=short_soft_gain,
    )

    pairwise_effects = {
        "schema_version": "tradex_action_precision_multitimeframe_pairwise_effects_v1",
        "generated_at": _utc_now(),
        "long": long_artifact["pairwise"],
        "short": short_artifact["pairwise"],
    }
    triple_effects = {
        "schema_version": "tradex_action_precision_multitimeframe_triple_effects_v1",
        "generated_at": _utc_now(),
        "long": long_artifact["triple"],
        "short": short_artifact["triple"],
    }

    candidate_map = {
        "schema_version": "tradex_action_precision_multitimeframe_candidate_map_v1",
        "generated_at": _utc_now(),
        "long_too_late_candidates_multitimeframe": long_artifact["candidate_families"]["long_too_late_candidates_multitimeframe"],
        "long_weak_direction_candidates_multitimeframe": long_artifact["candidate_families"]["long_weak_direction_candidates_multitimeframe"],
        "short_too_late_candidates_multitimeframe": short_artifact["candidate_families"]["short_too_late_candidates_multitimeframe"],
        "short_weak_direction_candidates_multitimeframe": short_artifact["candidate_families"]["short_weak_direction_candidates_multitimeframe"],
        "join_summary": {
            "total_rows": int(len(joined)),
            "exact_join_rows": int(joined["exact_join_available"].sum()),
            "exact_join_rate": float(joined["exact_join_available"].mean()),
            "equivalent_join_rows": int((~joined["exact_join_available"]).sum()),
            "equivalent_join_rate": float((~joined["exact_join_available"]).mean()),
            "join_contract": {
                "primary_key": ["code", "signal_month"],
                "hierarchical_key": ["code", "sample_month"],
                "exact_join_source": str(hierarchical_path),
                "equivalent_join_source": "confirmed payload fields from basis/reason/score snapshots",
            },
        },
    }

    long_decomposition = {
        "schema_version": "tradex_action_precision_multitimeframe_long_decomposition_v1",
        "generated_at": _utc_now(),
        "join_summary": candidate_map["join_summary"],
        "level_summary": long_artifact["level_summary"],
        "single_timeframe": long_artifact["single_timeframe"],
        "pairwise": long_artifact["pairwise"],
        "triple": long_artifact["triple"],
        "candidate_families": long_artifact["candidate_families"],
    }
    short_decomposition = {
        "schema_version": "tradex_action_precision_multitimeframe_short_decomposition_v1",
        "generated_at": _utc_now(),
        "join_summary": candidate_map["join_summary"],
        "level_summary": short_artifact["level_summary"],
        "single_timeframe": short_artifact["single_timeframe"],
        "pairwise": short_artifact["pairwise"],
        "triple": short_artifact["triple"],
        "candidate_families": short_artifact["candidate_families"],
    }

    decision = "keep_multitimeframe_decomposition_layer"
    decision_reason = "native hierarchical labels were joined for most rows and equivalent mappings were clearly tagged for the remainder"
    if candidate_map["join_summary"]["exact_join_rate"] < 0.80:
        decision = "hold_multitimeframe_decomposition_layer"
        decision_reason = "exact hierarchical join coverage is too low to fully trust the decomposition without stronger native coverage"
    if not any(len(v) for k, v in candidate_map.items() if k.endswith("candidates_multitimeframe")):
        decision = "hold_multitimeframe_decomposition_layer"
        decision_reason = "no usable candidate pools were produced"

    authoritative = {
        "schema_version": "tradex_action_precision_multitimeframe_decision_v1",
        "generated_at": _utc_now(),
        "decision": decision,
        "decision_reason": decision_reason,
        "join_contract": candidate_map["join_summary"]["join_contract"],
        "exact_join_rate": candidate_map["join_summary"]["exact_join_rate"],
        "equivalent_join_rate": candidate_map["join_summary"]["equivalent_join_rate"],
        "long_candidate_pool_sizes": {key: len(value) for key, value in long_decomposition["candidate_families"].items()},
        "short_candidate_pool_sizes": {key: len(value) for key, value in short_decomposition["candidate_families"].items()},
    }

    return {
        "samples": joined,
        "long_decomposition": long_decomposition,
        "short_decomposition": short_decomposition,
        "pairwise_effects": pairwise_effects,
        "triple_effects": triple_effects,
        "candidate_map": candidate_map,
        "authoritative_decision": authoritative,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    samples_path = Path(args.samples_path).expanduser().resolve() if args.samples_path else DEFAULT_SAMPLES_PATH
    thresholds_path = Path(args.thresholds_path).expanduser().resolve() if args.thresholds_path else DEFAULT_THRESHOLDS_PATH
    hierarchical_path = _resolve_hierarchical_path(args.hierarchical_path or None)

    result = build_multitimeframe_decomposition(
        samples_path=samples_path,
        hierarchical_path=hierarchical_path,
        thresholds_path=thresholds_path,
    )

    samples_frame = result["samples"].copy()
    _write_parquet(output_dir / MULTITIMEFRAME_SAMPLES_ARTIFACT, samples_frame)
    _write_json(output_dir / MULTITIMEFRAME_LONG_ARTIFACT, result["long_decomposition"])
    _write_json(output_dir / MULTITIMEFRAME_SHORT_ARTIFACT, result["short_decomposition"])
    _write_json(output_dir / MULTITIMEFRAME_PAIRWISE_ARTIFACT, result["pairwise_effects"])
    _write_json(output_dir / MULTITIMEFRAME_TRIPLE_ARTIFACT, result["triple_effects"])
    _write_json(output_dir / MULTITIMEFRAME_CANDIDATE_MAP_ARTIFACT, result["candidate_map"])
    _write_json(output_dir / AUTHORITATIVE_DECISION_ARTIFACT, result["authoritative_decision"])

    return {
        "ok": True,
        "samples_path": str(samples_path),
        "hierarchical_path": str(hierarchical_path),
        "thresholds_path": str(thresholds_path),
        "output_dir": str(output_dir),
        "decision": result["authoritative_decision"]["decision"],
        "decision_reason": result["authoritative_decision"]["decision_reason"],
        "exact_join_rate": result["authoritative_decision"]["exact_join_rate"],
        "equivalent_join_rate": result["authoritative_decision"]["equivalent_join_rate"],
        "long_candidate_pool_sizes": result["authoritative_decision"]["long_candidate_pool_sizes"],
        "short_candidate_pool_sizes": result["authoritative_decision"]["short_candidate_pool_sizes"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX multi-timeframe action-precision decomposition.")
    parser.add_argument("--samples-path", default=str(DEFAULT_SAMPLES_PATH))
    parser.add_argument("--hierarchical-path", default="")
    parser.add_argument("--thresholds-path", default=str(DEFAULT_THRESHOLDS_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
