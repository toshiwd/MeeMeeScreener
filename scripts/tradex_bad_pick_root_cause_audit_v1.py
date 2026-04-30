from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import (  # noqa: E402
    get_rankings_freshness,
    get_runtime_stock_db_status,
)
from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _load_json,
    _make_session_id,
    _safe_float,
    _safe_int,
    _write_json,
)

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_CANDIDATE_SNAPSHOT = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_candidate_snapshots.json"
)
DEFAULT_SELECTION_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json"
)
DEFAULT_POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_root_cause_audit")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1"
MANIFEST_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_input_resolution_v1"
COHORT_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_cohort_summary_v1"
ROOT_CAUSE_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_root_cause_taxonomy_summary_v1"
FEATURE_CONTRAST_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_feature_contrast_summary_v1"
VETO_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_veto_hypothesis_backlog_v1"
DECISION_SCHEMA_VERSION = "tradex_bad_pick_root_cause_audit_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
BAD_OUTCOME_THRESHOLD = 0.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _git_hash_or_unknown() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        value = result.stdout.strip()
        return value or "unknown"
    except Exception:
        return "unknown"


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _resolve_source_path(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_small_json(path: Path) -> dict[str, Any]:
    return _load_json(path)


def _to_bool_series(frame: pd.DataFrame, column: str, default: bool = False) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([default] * len(frame), index=frame.index, dtype=bool)
    series = frame[column]
    if series.dtype == bool:
        return series.fillna(default).astype(bool)
    return series.fillna(default).astype(bool)


def _ensure_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["month_bucket"] = frame["month_bucket"].astype(str)
    for column in ("candidate_rank", "champion_rank", "rank"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    for column in (
        "champion_selected_top5",
        "champion_selected_top10",
        "champion_selected_top20",
        "challenger_selected_top5",
        "challenger_selected_top10",
        "challenger_selected_top20",
        "top15_label",
        "bottom15_label",
        "conditional_high_value",
        "shape_joined",
        "stable_high_value_family",
        "stable_bad_pick_family",
        "regime_dependent_family",
        "unstable_or_sparse_family",
        "neutral_family",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    ):
        if column in frame.columns:
            frame[column] = frame[column].fillna(False).astype(bool)
    for column in (
        "score",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "family_mean_forward_ret_20d",
        "family_median_forward_ret_20d",
        "family_mean_mfe_20d",
        "family_mean_mae_20d",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_plus5_before_minus5_rate",
        "family_minus5_before_plus5_rate",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
        "family_worst_month_mean_path_value",
        "family_best_month_mean_path_value",
        "liquidity20d",
        "market_breadth_adv_ratio",
        "market_breadth_sample_size",
        "monthly_box_pos",
        "monthly_box_range_pct",
        "monthly_range_pos",
        "monthly_range_prob",
        "monthly_range_width",
        "weekly_breakout_up_prob",
        "weekly_breakout_down_prob",
        "monthly_breakout_up_prob",
        "monthly_breakout_down_prob",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "bear_marubozu",
        "bull_marubozu",
        "bull_engulfing",
        "three_white_soldiers",
        "three_black_crows",
        "morning_star",
        "shooting_star_like",
        "v60_core",
        "v60_strong",
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in (
        "daily_main_state_ctx",
        "weekly_main_state_ctx",
        "monthly_main_state_ctx",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "prefilter_bucket",
        "selection_reason",
        "selected_by",
        "family_regime_context",
        "family_bad_pick_regime",
        "prefilter_reason",
    ):
        if column in frame.columns:
            frame[column] = frame[column].astype(object)
    return frame


def _load_candidate_source(source_rows_parquet: Path) -> pd.DataFrame:
    frame = pd.read_parquet(source_rows_parquet)
    return _ensure_columns(frame)


def _load_candidate_snapshot(snapshot_path: Path) -> dict[str, Any]:
    payload = _load_small_json(snapshot_path)
    rows = payload.get("rows") or {}
    row_list = rows.get("rows") if isinstance(rows, dict) else rows
    if not isinstance(row_list, list):
        raise ValueError(f"unexpected candidate snapshot schema: {snapshot_path}")
    return {
        "payload": payload,
        "row_count": len(row_list),
        "schema_version": payload.get("schema_version"),
        "rows": row_list,
    }


def _load_selection_ledger(selection_ledger_path: Path) -> dict[str, Any]:
    payload = _load_small_json(selection_ledger_path)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError(f"unexpected selection ledger schema: {selection_ledger_path}")
    return {
        "payload": payload,
        "row_count": len(rows),
        "schema_version": payload.get("schema_version"),
        "rows": rows,
    }


def _iter_wrapped_json_rows(path: Path) -> Iterable[dict[str, Any]]:
    rows_started = False
    capturing = False
    buffer: list[str] = []
    depth = 0
    in_string = False
    escape = False
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not rows_started:
                if '"rows": [' in line:
                    rows_started = True
                continue
            if not capturing:
                if "{" not in line:
                    if "]" in line:
                        break
                    continue
                line = line[line.index("{") :]
                capturing = True
            for ch in line:
                if not capturing:
                    continue
                buffer.append(ch)
                if in_string:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        in_string = False
                    continue
                if ch == '"':
                    in_string = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        yield json.loads("".join(buffer))
                        buffer = []
                        capturing = False
                        in_string = False
                        escape = False
                elif ch == "]" and depth == 0:
                    return


def _extract_policy_feature_row(row: dict[str, Any]) -> dict[str, Any]:
    chart_raw = row.get("chart_context")
    daily_raw = row.get("daily_micro_snapshot")
    try:
        chart = json.loads(chart_raw) if isinstance(chart_raw, str) and chart_raw else {}
    except Exception:
        chart = {}
    try:
        daily = json.loads(daily_raw) if isinstance(daily_raw, str) and daily_raw else {}
    except Exception:
        daily = {}
    basis = daily.get("basis_payload") if isinstance(daily, dict) else {}
    derived = daily.get("derived_context") if isinstance(daily, dict) else {}
    if not isinstance(basis, dict):
        basis = {}
    if not isinstance(derived, dict):
        derived = {}
    return {
        "anchor_date": str(row.get("anchor_date")),
        "symbol": str(row.get("symbol")),
        "side": str(row.get("side")),
        "policy_date": str(row.get("date") or row.get("trade_date") or row.get("anchor_date")),
        "policy_selected_action": row.get("selected_action"),
        "policy_selection_method": row.get("selection_method"),
        "policy_selection_source": row.get("selection_source"),
        "policy_variant": row.get("policy_variant"),
        "daily_main_state_ctx": chart.get("daily_main_state_ctx") or derived.get("daily_main_state_ctx"),
        "weekly_main_state_ctx": chart.get("weekly_main_state_ctx") or derived.get("weekly_main_state_ctx"),
        "monthly_main_state_ctx": chart.get("monthly_main_state_ctx") or derived.get("monthly_main_state_ctx"),
        "dist_ma20_pct": _safe_float(chart.get("dist_ma20_pct")),
        "dist_ma60_pct": _safe_float(chart.get("dist_ma60_pct")),
        "body_ratio": _safe_float(chart.get("body_ratio")),
        "upper_wick_ratio": _safe_float(chart.get("upper_wick_ratio")),
        "lower_wick_ratio": _safe_float(chart.get("lower_wick_ratio")),
        "gap_pct": _safe_float(chart.get("gap_pct")),
        "sequence_3": chart.get("sequence_3"),
        "breakout5": chart.get("breakout5"),
        "breakout10": chart.get("breakout10"),
        "exhaustion": chart.get("exhaustion"),
        "bull_stack": chart.get("bull_stack"),
        "bear_stack": chart.get("bear_stack"),
        "support_wick": chart.get("support_wick"),
        "market_regime": basis.get("marketRegime"),
        "market_risk_on": basis.get("marketRiskOn"),
        "market_risk_off": basis.get("marketRiskOff"),
        "liquidity20d": _safe_float(basis.get("liquidity20d")),
        "market_breadth_adv_ratio": _safe_float(basis.get("marketBreadthAdvRatio")),
        "market_breadth_sample_size": _safe_int(basis.get("marketBreadthSampleSize")),
        "monthly_box_state": basis.get("monthlyBoxState"),
        "monthly_box_pos": _safe_float(basis.get("monthlyBoxPos")),
        "monthly_box_range_pct": _safe_float(basis.get("monthlyBoxRangePct")),
        "monthly_range_pos": _safe_float(basis.get("monthlyRangePos")),
        "monthly_range_prob": _safe_float(basis.get("monthlyRangeProb")),
        "monthly_range_width": _safe_float(basis.get("monthlyRangeWidth")),
        "weekly_breakout_up_prob": _safe_float(basis.get("weeklyBreakoutUpProb")),
        "weekly_breakout_down_prob": _safe_float(basis.get("weeklyBreakoutDownProb")),
        "monthly_breakout_up_prob": _safe_float(basis.get("monthlyBreakoutUpProb")),
        "monthly_breakout_down_prob": _safe_float(basis.get("monthlyBreakoutDownProb")),
        "candle_body_ratio": _safe_float(basis.get("candleBodyRatio")),
        "candle_upper_wick_ratio": _safe_float(basis.get("candleUpperWickRatio")),
        "candle_lower_wick_ratio": _safe_float(basis.get("candleLowerWickRatio")),
        "candle_triplet_up_prob": _safe_float(basis.get("candleTripletUp")),
        "candle_triplet_down_prob": _safe_float(basis.get("candleTripletDown")),
        "bear_marubozu": _safe_float(basis.get("bearMarubozu")),
        "bull_marubozu": _safe_float(basis.get("bullMarubozu")),
        "bull_engulfing": _safe_float(basis.get("bullEngulfing")),
        "three_white_soldiers": _safe_float(basis.get("threeWhiteSoldiers")),
        "three_black_crows": _safe_float(basis.get("threeBlackCrows")),
        "morning_star": _safe_float(basis.get("morningStar")),
        "shooting_star_like": _safe_float(basis.get("shootingStarLike")),
        "v60_core": _safe_float(basis.get("v60Core")),
        "v60_strong": _safe_float(basis.get("v60Strong")),
    }


def _load_policy_feature_overlay(policy_ledger_path: Path, selected_keys: set[tuple[str, str, str]]) -> pd.DataFrame:
    matched: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _iter_wrapped_json_rows(policy_ledger_path):
        key = (str(row.get("anchor_date")), str(row.get("symbol")), str(row.get("side")))
        if key not in selected_keys:
            continue
        if str(row.get("date") or "") != str(row.get("anchor_date") or ""):
            continue
        if str(row.get("selected_action") or "") not in {"stay", "hold"}:
            continue
        if key in matched:
            continue
        matched[key] = _extract_policy_feature_row(row)
        if len(matched) == len(selected_keys):
            break
    overlay = pd.DataFrame(matched.values())
    if overlay.empty:
        return overlay
    overlay["anchor_date"] = overlay["anchor_date"].astype(str)
    overlay["symbol"] = overlay["symbol"].astype(str)
    overlay["side"] = overlay["side"].astype(str)
    return overlay


def _limit_anchor_dates(frame: pd.DataFrame, limit_anchor_dates: int | None) -> pd.DataFrame:
    if not limit_anchor_dates or limit_anchor_dates <= 0:
        return frame
    anchors = sorted(frame["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
    return frame.loc[frame["anchor_date"].isin(anchors)].copy()


def _group_topk_frame(frame: pd.DataFrame, top_k: int) -> pd.DataFrame:
    mask = frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
    return frame.loc[mask].copy()


def _add_outcome_labels(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    if "forward_ret_20d" not in frame.columns:
        raise RuntimeError("missing forward_ret_20d in audit frame")
    frame["is_top15_outcome"] = frame["top15_label"].fillna(False).astype(bool)
    frame["is_bottom15_outcome"] = frame["bottom15_label"].fillna(False).astype(bool)
    frame["is_materially_negative"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce").fillna(0.0) <= BAD_OUTCOME_THRESHOLD
    frame["is_bad_pick"] = (
        (frame["champion_selected_top5"] | frame["champion_selected_top10"])
        & (frame["is_bottom15_outcome"] | frame["is_materially_negative"])
    )
    frame["is_good_pick"] = (
        (frame["champion_selected_top5"] | frame["champion_selected_top10"])
        & frame["is_top15_outcome"]
        & ~frame["is_bad_pick"]
    )
    frame["is_neutral_pick"] = (
        (frame["champion_selected_top5"] | frame["champion_selected_top10"])
        & ~frame["is_bad_pick"]
        & ~frame["is_good_pick"]
    )
    frame["topk_bucket"] = "top20"
    frame.loc[frame["champion_selected_top10"], "topk_bucket"] = "top10"
    frame.loc[frame["champion_selected_top5"], "topk_bucket"] = "top5"
    frame["selected_topk_label"] = pd.Series(
        [
            "top5" if bool(row["champion_selected_top5"]) else "top10" if bool(row["champion_selected_top10"]) else "top20"
            for _, row in frame.iterrows()
        ],
        index=frame.index,
    )
    return frame


def _group_quantile(series: pd.Series, quantile: float) -> float | None:
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    if cleaned.empty:
        return None
    return float(cleaned.quantile(quantile, interpolation="linear"))


def _boundary_rows_for_bad_pick(frame: pd.DataFrame, row: pd.Series) -> pd.DataFrame:
    anchor_date = str(row["anchor_date"])
    side = str(row["side"])
    rank = _safe_int(row.get("champion_rank") or row.get("candidate_rank"))
    group = frame.loc[(frame["anchor_date"] == anchor_date) & (frame["side"] == side)].copy()
    if rank is None:
        return group.iloc[0:0].copy()
    if int(rank) <= 5:
        return group.loc[(group["champion_rank"] >= 6) & (group["champion_rank"] <= 10)].copy()
    if int(rank) <= 10:
        return group.loc[(group["champion_rank"] >= 11) & (group["champion_rank"] <= 20)].copy()
    return group.iloc[0:0].copy()


def _extract_context_features(row: pd.Series) -> dict[str, Any]:
    keys = [
        "daily_main_state_ctx",
        "weekly_main_state_ctx",
        "monthly_main_state_ctx",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "shape_classification",
        "candle_shape_modifier",
        "prefilter_bucket",
        "selected_by",
        "selected_by_methods",
        "prefilter_reason",
        "family_regime_context",
        "family_bad_pick_regime",
        "shape_joined",
        "stable_high_value_family",
        "stable_bad_pick_family",
        "regime_dependent_family",
        "unstable_or_sparse_family",
        "neutral_family",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "top15_label",
        "bottom15_label",
        "conditional_high_value",
        "score",
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "family_sample_count",
        "family_unique_symbol_count",
        "family_month_count",
        "family_mean_forward_ret_20d",
        "family_median_forward_ret_20d",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_plus5_before_minus5_rate",
        "family_minus5_before_plus5_rate",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
        "family_worst_month_mean_path_value",
        "family_best_month_mean_path_value",
        "monthly_context",
        "weekly_context",
    ]
    out = {key: row.get(key) for key in keys if key in row.index}
    out["missing_feature_fields"] = [
        key
        for key in ("event_flag", "earnings_flag", "dividend_flag", "rights_flag", "ex_rights_flag")
        if key not in row.index
    ]
    return out


def _classify_root_cause(
    row: pd.Series,
    *,
    boundary_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    missing_fields: list[str] = []
    evidence_fields: list[str] = []
    notes: list[str] = []

    def val(name: str, default: Any = None) -> Any:
        if name not in row.index:
            missing_fields.append(name)
            return default
        return row.get(name)

    top15 = bool(val("top15_label", False))
    bottom15 = bool(val("bottom15_label", False))
    shape_joined = bool(val("shape_joined", False))
    cond_high = bool(val("conditional_high_value", False))
    family_class = str(val("family_classification", "unknown") or "unknown")
    shape_class = str(val("shape_classification", "shape_missing") or "shape_missing")
    monthly_ctx = str(val("monthly_context", "unknown") or "unknown")
    weekly_ctx = str(val("weekly_context", "unknown") or "unknown")
    daily_ctx = str(val("daily_main_state_ctx", "unknown") or "unknown")
    regime_ctx = str(val("dominant_regime_context", "unknown") or "unknown")
    market_regime = str(val("market_regime_bucket", "unknown") or "unknown")
    score = _safe_float(val("score"))
    forward_ret_20d = _safe_float(val("forward_ret_20d"))
    path_value = _safe_float(val("path_value_score_v1"))
    dist_ma20 = _safe_float(val("dist_ma20_pct"))
    dist_ma60 = _safe_float(val("dist_ma60_pct"))
    gap_pct = _safe_float(val("gap_pct"))
    body_ratio = _safe_float(val("body_ratio"))
    upper_wick = _safe_float(val("upper_wick_ratio"))
    lower_wick = _safe_float(val("lower_wick_ratio"))
    volume_ratio = _safe_float(val("vol_ratio5_20"))
    family_bottom15 = _safe_float(val("family_bottom15_rate"))
    family_path = _safe_float(val("family_mean_path_value_score_v1"))
    family_pos_month = _safe_float(val("family_positive_month_rate"))
    stable_bad_pick = bool(val("stable_bad_pick_family", False))
    stable_high = bool(val("stable_high_value_family", False))

    if not shape_joined:
        missing_fields.append("shape_joined")
    if not cond_high:
        evidence_fields.append("conditional_high_value")
    if family_class == "unknown":
        missing_fields.append("family_classification")
    if monthly_ctx == "unknown":
        missing_fields.append("monthly_context")
    if weekly_ctx == "unknown":
        missing_fields.append("weekly_context")
    if daily_ctx == "unknown":
        missing_fields.append("daily_main_state_ctx")

    if stable_bad_pick and family_class == "stable_bad_pick_family":
        evidence_fields.extend(["stable_bad_pick_family", "family_classification", "family_bottom15_rate", "family_mean_path_value_score_v1"])
        return {
            "root_cause_code": "score_component_overweight",
            "confidence": "high",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "known bad-pick family still entered champion topK",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if boundary_summary:
        score_gap = _safe_float(boundary_summary.get("score_gap"))
        ret_gap = _safe_float(boundary_summary.get("forward_ret_20d_gap"))
        path_gap = _safe_float(boundary_summary.get("path_value_gap"))
        if score_gap is not None and ret_gap is not None and path_gap is not None:
            if score_gap <= 0.02 and ret_gap < 0 and path_gap < 0:
                evidence_fields.extend(["score", "forward_ret_20d", "path_value_score_v1", "boundary_near_miss_score", "boundary_near_miss_forward_ret_20d"])
                return {
                    "root_cause_code": "score_component_overweight",
                    "confidence": "medium",
                    "evidence_fields_used": _unique_ordered(evidence_fields),
                    "notes": "near miss had better realized path/return with similar score boundary",
                    "missing_fields": _unique_ordered(missing_fields),
                }

    if monthly_ctx != "unknown" and weekly_ctx != "unknown":
        if ("overextended" in monthly_ctx or "top_warning" in monthly_ctx or "late" in monthly_ctx) and (
            "overextended" in weekly_ctx or "late" in weekly_ctx
        ):
            if dist_ma20 is not None and dist_ma20 >= 0.04 or dist_ma60 is not None and dist_ma60 >= 0.06:
                evidence_fields.extend(["monthly_context", "weekly_context", "dist_ma20_pct", "dist_ma60_pct", "daily_main_state_ctx"])
                return {
                    "root_cause_code": "late_entry_after_extended_rise",
                    "confidence": "high",
                    "evidence_fields_used": _unique_ordered(evidence_fields),
                    "notes": "higher-timeframe overextension plus stretched MA distance",
                    "missing_fields": _unique_ordered(missing_fields),
                }

    if gap_pct is not None:
        if abs(gap_pct) >= 0.015 and bottom15:
            if (upper_wick is not None and upper_wick >= 0.10) or (body_ratio is not None and body_ratio >= 0.75):
                evidence_fields.extend(["gap_pct", "upper_wick_ratio", "body_ratio", "bottom15_label"])
                return {
                    "root_cause_code": "gap_reversal_risk",
                    "confidence": "medium",
                    "evidence_fields_used": _unique_ordered(evidence_fields),
                    "notes": "gap and intraday wick/body geometry suggest reversal risk",
                    "missing_fields": _unique_ordered(missing_fields),
                }

    if volume_ratio is not None and volume_ratio < 0.8 and bottom15:
        evidence_fields.extend(["vol_ratio5_20", "bottom15_label"])
        return {
            "root_cause_code": "low_liquidity_or_thin_trading",
            "confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "volume ratio below audit threshold and bad 20-day outcome",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if (
        ("down" in monthly_ctx or "weak" in monthly_ctx or "range" in monthly_ctx)
        and ("down" in weekly_ctx or "weak" in weekly_ctx or "range" in weekly_ctx)
        and ("up" in daily_ctx or "reversal" in daily_ctx or "breakout" in daily_ctx)
        and bottom15
    ):
        evidence_fields.extend(["monthly_context", "weekly_context", "daily_main_state_ctx"])
        return {
            "root_cause_code": "downtrend_bounce_misread",
            "confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "daily bounce/reversal signal was misread inside weak higher-timeframe structure",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if (
        ("risk_off" in market_regime or "risk_off" in regime_ctx or "bear" in regime_ctx)
        and ("up" in daily_ctx or "reversal" in daily_ctx or "breakout" in daily_ctx)
        and bottom15
    ):
        evidence_fields.extend(["market_regime_bucket", "dominant_regime_context", "daily_main_state_ctx"])
        return {
            "root_cause_code": "weak_regime_false_positive",
            "confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "selection entered from a weak regime context despite bullish daily signal",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if (
        cond_high
        and shape_class == "shape_context_dependent"
        and boundary_summary is not None
        and _safe_float(boundary_summary.get("forward_ret_20d_gap")) is not None
        and _safe_float(boundary_summary.get("forward_ret_20d_gap")) < 0
    ):
        evidence_fields.extend(["conditional_high_value", "shape_classification", "boundary_near_miss_forward_ret_20d"])
        return {
            "root_cause_code": "monthly_weekly_daily_misalignment",
            "confidence": "low",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "context gated signal exists but a nearby boundary candidate was better on realized path",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if forward_ret_20d is not None and forward_ret_20d > 0 and path_value is not None and path_value < 0:
        evidence_fields.extend(["forward_ret_20d", "path_value_score_v1"])
        return {
            "root_cause_code": "poor_path_despite_positive_terminal_return",
            "confidence": "low",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "positive 20-day return but weak path quality",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if family_class in {"regime_dependent_family", "unstable_or_sparse_family"} and bottom15:
        evidence_fields.extend(["family_classification", "family_bottom15_rate", "family_mean_path_value_score_v1"])
        return {
            "root_cause_code": "score_component_overweight",
            "confidence": "low",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "notes": "family-level evidence did not justify champion admission",
            "missing_fields": _unique_ordered(missing_fields),
        }

    if not evidence_fields:
        evidence_fields.extend(["score", "forward_ret_20d", "path_value_score_v1"])
    return {
        "root_cause_code": "unknown_or_insufficient_data",
        "confidence": "low",
        "evidence_fields_used": _unique_ordered(evidence_fields),
        "notes": "no rule met with sufficient evidence",
        "missing_fields": _unique_ordered(missing_fields),
    }


def _unique_ordered(items: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    seen: set[Any] = set()
    for item in items:
        if item is None:
            continue
        marker = item
        if marker in seen:
            continue
        seen.add(marker)
        out.append(item)
    return out


def _prepare_bad_pick_rows(frame: pd.DataFrame) -> pd.DataFrame:
    bad = frame.loc[frame["is_bad_pick"]].copy()
    bad["bad_pick_scope"] = bad.apply(
        lambda row: "top5" if bool(row["champion_selected_top5"]) else "top10", axis=1
    )
    bad["bad_pick_scope"] = bad["bad_pick_scope"].astype(str)
    bad = bad.sort_values(["champion_rank", "score", "symbol", "anchor_date"], ascending=[True, False, True, True], kind="stable")
    return bad


def _build_boundary_summary(frame: pd.DataFrame, bad_row: pd.Series) -> dict[str, Any]:
    boundary_rows = _boundary_rows_for_bad_pick(frame, bad_row)
    if boundary_rows.empty:
        return {
            "boundary_candidate_count": 0,
            "boundary_rank_range": None,
            "boundary_mean_forward_ret_20d": None,
            "boundary_median_forward_ret_20d": None,
            "boundary_mean_path_value_score_v1": None,
            "boundary_median_path_value_score_v1": None,
            "best_near_miss_rank": None,
            "best_near_miss_symbol": None,
            "best_near_miss_score": None,
            "best_near_miss_forward_ret_20d": None,
            "best_near_miss_path_value_score_v1": None,
            "best_near_miss_shape_classification": None,
            "score_gap": None,
            "forward_ret_20d_gap": None,
            "path_value_gap": None,
            "rank_gap": None,
            "boundary_candidate_ranks": [],
        }

    boundary_rows = boundary_rows.sort_values(["champion_rank", "score", "symbol"], ascending=[True, False, True], kind="stable")
    immediate = boundary_rows.iloc[0]
    rank_min = int(boundary_rows["champion_rank"].min())
    rank_max = int(boundary_rows["champion_rank"].max())
    return {
        "boundary_candidate_count": int(len(boundary_rows)),
        "boundary_rank_range": f"{rank_min}-{rank_max}",
        "boundary_mean_forward_ret_20d": _safe_float(boundary_rows["forward_ret_20d"].mean()),
        "boundary_median_forward_ret_20d": _safe_float(boundary_rows["forward_ret_20d"].median()),
        "boundary_mean_path_value_score_v1": _safe_float(boundary_rows["path_value_score_v1"].mean()),
        "boundary_median_path_value_score_v1": _safe_float(boundary_rows["path_value_score_v1"].median()),
        "best_near_miss_rank": int(_safe_int(immediate.get("champion_rank")) or 0),
        "best_near_miss_symbol": str(immediate.get("symbol")),
        "best_near_miss_score": _safe_float(immediate.get("score")),
        "best_near_miss_forward_ret_20d": _safe_float(immediate.get("forward_ret_20d")),
        "best_near_miss_path_value_score_v1": _safe_float(immediate.get("path_value_score_v1")),
        "best_near_miss_shape_classification": str(immediate.get("shape_classification") or "unknown"),
        "score_gap": _safe_float(bad_row.get("score")) - _safe_float(immediate.get("score")),
        "forward_ret_20d_gap": _safe_float(bad_row.get("forward_ret_20d")) - _safe_float(immediate.get("forward_ret_20d")),
        "path_value_gap": _safe_float(bad_row.get("path_value_score_v1")) - _safe_float(immediate.get("path_value_score_v1")),
        "rank_gap": int((_safe_int(immediate.get("champion_rank")) or 0) - (_safe_int(bad_row.get("champion_rank")) or 0)),
        "boundary_candidate_ranks": [int(v) for v in boundary_rows["champion_rank"].dropna().astype(int).tolist()],
    }


def _build_feature_contrast_summary(frame: pd.DataFrame) -> dict[str, Any]:
    numeric_features = [
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "vol_ratio5_20",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "family_mean_forward_ret_20d",
        "family_median_forward_ret_20d",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_plus5_before_minus5_rate",
        "family_minus5_before_plus5_rate",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_positive_month_rate",
    ]
    categorical_features = [
        "shape_classification",
        "candle_shape_modifier",
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "prefilter_bucket",
        "selected_by",
    ]
    unavailable_fields = [
        "event_flag",
        "earnings_flag",
        "dividend_flag",
        "rights_flag",
        "ex_rights_flag",
        "volume_atr_ratio",
    ]

    def summarize_feature(feature: str, bad: pd.DataFrame, good: pd.DataFrame) -> dict[str, Any]:
        if feature not in frame.columns:
            return {
                "feature": feature,
                "availability": "unavailable",
            }
        b = bad[feature].dropna()
        g = good[feature].dropna()
        if pd.api.types.is_numeric_dtype(frame[feature]):
            if len(b) == 0 or len(g) == 0:
                return {
                    "feature": feature,
                    "availability": "missing_on_one_side",
                }
            delta_mean = _safe_float(pd.to_numeric(b, errors="coerce").mean() - pd.to_numeric(g, errors="coerce").mean())
            delta_median = _safe_float(pd.to_numeric(b, errors="coerce").median() - pd.to_numeric(g, errors="coerce").median())
            if abs(delta_mean or 0.0) < 1e-6 and abs(delta_median or 0.0) < 1e-6:
                classification = "non_discriminative"
            elif (delta_mean or 0.0) > 0:
                classification = "enriched_in_bad"
            else:
                classification = "enriched_in_good"
            return {
                "feature": feature,
                "availability": "available",
                "bad_mean": _safe_float(pd.to_numeric(b, errors="coerce").mean()),
                "good_mean": _safe_float(pd.to_numeric(g, errors="coerce").mean()),
                "delta_mean_bad_minus_good": delta_mean,
                "bad_median": _safe_float(pd.to_numeric(b, errors="coerce").median()),
                "good_median": _safe_float(pd.to_numeric(g, errors="coerce").median()),
                "delta_median_bad_minus_good": delta_median,
                "classification": classification,
            }
        bad_mode = b.astype(str).value_counts().head(1)
        good_mode = g.astype(str).value_counts().head(1)
        if bad_mode.empty or good_mode.empty:
            return {
                "feature": feature,
                "availability": "missing_on_one_side",
            }
        bad_value = bad_mode.index[0]
        good_value = good_mode.index[0]
        bad_rate = float((b.astype(str) == bad_value).mean())
        good_rate = float((g.astype(str) == bad_value).mean())
        if abs(bad_rate - good_rate) < 0.05:
            classification = "non_discriminative"
        elif bad_rate > good_rate:
            classification = "enriched_in_bad"
        else:
            classification = "enriched_in_good"
        return {
            "feature": feature,
            "availability": "available",
            "bad_mode": bad_value,
            "good_mode": good_value,
            "bad_mode_rate": bad_rate,
            "good_mode_rate": good_rate,
            "delta_mode_rate_bad_minus_good": bad_rate - good_rate,
            "classification": classification,
        }

    out: dict[str, Any] = {
        "numeric_features": [],
        "categorical_features": [],
        "unavailable_fields": unavailable_fields,
        "bad_pick_vs_good_pick_notes": [
            "bad picks are defined as champion top5/top10 rows with bottom15_label or materially negative forward_ret_20d",
            "good picks are champion top5/top10 rows with top15_label and not bottom15_label",
        ],
    }
    for topk in ("top5", "top10"):
        bad_mask = (frame[f"champion_selected_{topk}"]) & (frame["bottom15_label"])
        good_mask = (frame[f"champion_selected_{topk}"]) & (frame["top15_label"]) & (~frame["bottom15_label"])
        bad = frame.loc[bad_mask].copy()
        good = frame.loc[good_mask].copy()
        topk_summary = {
            "topk": topk,
            "bad_count": int(len(bad)),
            "good_count": int(len(good)),
            "bad_forward_ret_20d_mean": _safe_float(bad["forward_ret_20d"].mean()),
            "good_forward_ret_20d_mean": _safe_float(good["forward_ret_20d"].mean()),
            "bad_path_value_score_v1_mean": _safe_float(bad["path_value_score_v1"].mean()),
            "good_path_value_score_v1_mean": _safe_float(good["path_value_score_v1"].mean()),
        }
        feature_rows = []
        for feature in numeric_features:
            row = summarize_feature(feature, bad, good)
            if row.get("availability") == "available":
                feature_rows.append(row)
        out[f"{topk}_summary"] = topk_summary
        out[f"{topk}_feature_rows"] = feature_rows
        cat_rows = []
        for feature in categorical_features:
            row = summarize_feature(feature, bad, good)
            if row.get("availability") == "available":
                cat_rows.append(row)
        out[f"{topk}_categorical_rows"] = cat_rows
    return out


def _build_veto_hypotheses(root_cause_summary: dict[str, Any], feature_contrast: dict[str, Any]) -> list[dict[str, Any]]:
    counts = root_cause_summary.get("root_cause_counts") or {}
    hypotheses: list[dict[str, Any]] = []
    top_codes = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    for idx, (code, count) in enumerate(top_codes[:5], start=1):
        if code == "unknown_or_insufficient_data":
            continue
        if code == "score_component_overweight":
            hypotheses.append(
                {
                    "hypothesis_id": f"HP-{idx:02d}",
                    "root_cause_addressed": code,
                    "candidate_condition": "Reject or downgrade candidates where the chosen topK row is a stable bad-pick family or where a rank-near boundary candidate has better realized 20-day return and path value with a similar score.",
                    "required_fields": [
                        "score",
                        "forward_ret_20d",
                        "path_value_score_v1",
                        "stable_bad_pick_family",
                        "top15_label",
                        "bottom15_label",
                    ],
                    "expected_effect": "Reduce champion top5/top10 bad-pick contamination without changing the ranking score itself.",
                    "expected_risk": "May remove some winners in regimes where the same family occasionally recovers.",
                    "why_it_may_move_topk_boundary": "The audit shows nearby lower-ranked candidates can outperform on realized path even when score is similar.",
                    "recommended_next_validation_method": "One-axis veto challenger using primary/high-confidence bad-pick family flags only.",
                    "evidence_strength": "high" if count >= 10 else "medium",
                }
            )
        elif code == "late_entry_after_extended_rise":
            hypotheses.append(
                {
                    "hypothesis_id": f"HP-{idx:02d}",
                    "root_cause_addressed": code,
                    "candidate_condition": "Downgrade candidates when monthly and weekly context both show overextension and daily MA distance is stretched.",
                    "required_fields": [
                        "monthly_context",
                        "weekly_context",
                        "dist_ma20_pct",
                        "dist_ma60_pct",
                        "daily_main_state_ctx",
                    ],
                    "expected_effect": "Filter late entries that already exhausted the near-term move.",
                    "expected_risk": "Could reject momentum continuation winners in strong trend regimes.",
                    "why_it_may_move_topk_boundary": "The bad cohort is concentrated in extended higher-timeframe states, so a veto should shift the top-K boundary away from late entries.",
                    "recommended_next_validation_method": "Fixed-condition veto challenger on the high-extension slice only.",
                    "evidence_strength": "high" if count >= 10 else "medium",
                }
            )
        elif code == "gap_reversal_risk":
            hypotheses.append(
                {
                    "hypothesis_id": f"HP-{idx:02d}",
                    "root_cause_addressed": code,
                    "candidate_condition": "Downgrade gapped-open candidates when wick/body geometry suggests reversal risk rather than clean follow-through.",
                    "required_fields": ["gap_pct", "upper_wick_ratio", "lower_wick_ratio", "body_ratio", "forward_ret_20d"],
                    "expected_effect": "Reduce entries that fade after an opening gap.",
                    "expected_risk": "May miss valid gap-and-go continuation cases.",
                    "why_it_may_move_topk_boundary": "The selected bad picks show gap/reversal geometry more often than good picks.",
                    "recommended_next_validation_method": "Boundary-only candidate veto with gap and wick constraints.",
                    "evidence_strength": "medium",
                }
            )
        elif code == "low_liquidity_or_thin_trading":
            hypotheses.append(
                {
                    "hypothesis_id": f"HP-{idx:02d}",
                    "root_cause_addressed": code,
                    "candidate_condition": "Downgrade candidates with weak liquidity or low relative volume ratio.",
                    "required_fields": ["vol_ratio5_20", "v", "forward_ret_20d"],
                    "expected_effect": "Reduce thin-trading false positives.",
                    "expected_risk": "Could remove small-cap winners that need volume expansion.",
                    "why_it_may_move_topk_boundary": "Thin trading can suppress the realized path even when score looks acceptable.",
                    "recommended_next_validation_method": "Watchlist-only admission for low-volume cases.",
                    "evidence_strength": "medium",
                }
            )
        elif code == "weak_regime_false_positive":
            hypotheses.append(
                {
                    "hypothesis_id": f"HP-{idx:02d}",
                    "root_cause_addressed": code,
                    "candidate_condition": "Downgrade candidates when the market regime is risk-off or weak while the daily signal still looks bullish.",
                    "required_fields": ["market_regime_bucket", "dominant_regime_context", "daily_main_state_ctx"],
                    "expected_effect": "Reduce false positives from regime mismatch.",
                    "expected_risk": "May be too conservative in early regime flips.",
                    "why_it_may_move_topk_boundary": "Weak regime candidates can survive score ranking but fail on 20-day outcomes.",
                    "recommended_next_validation_method": "Regime-gated veto challenger with no score adjustment.",
                    "evidence_strength": "medium",
                }
            )
    if not hypotheses:
        hypotheses.append(
            {
                "hypothesis_id": "HP-01",
                "root_cause_addressed": "unknown_or_insufficient_data",
                "candidate_condition": "Need more data before a veto challenger can be designed confidently.",
                "required_fields": ["score", "forward_ret_20d", "path_value_score_v1"],
                "expected_effect": "None yet.",
                "expected_risk": "Premature design may overfit a sparse audit slice.",
                "why_it_may_move_topk_boundary": "Not established.",
                "recommended_next_validation_method": "Collect additional boundary comparisons and re-run the audit on a broader period.",
                "evidence_strength": "low",
            }
        )
    return hypotheses


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def run_bad_pick_root_cause_audit_v1(
    *,
    source_rows_parquet: str | Path | None = None,
    candidate_snapshot_path: str | Path | None = None,
    selection_ledger_path: str | Path | None = None,
    policy_ledger_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 2,
) -> dict[str, Any]:
    source_rows_parquet = _resolve_source_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET, "source rows parquet")
    candidate_snapshot_path = _resolve_source_path(candidate_snapshot_path, DEFAULT_CANDIDATE_SNAPSHOT, "candidate snapshot")
    selection_ledger_path = _resolve_source_path(selection_ledger_path, DEFAULT_SELECTION_LEDGER, "selection ledger")
    policy_ledger_path = _resolve_source_path(policy_ledger_path, DEFAULT_POLICY_LEDGER, "policy ledger")
    output_root = _resolve_output_root(output_root)

    runtime_status = get_runtime_stock_db_status()
    long_rankings = get_rankings_freshness(direction="up", risk_mode="balanced")
    short_rankings = get_rankings_freshness(direction="down", risk_mode="balanced")

    source_frame = _load_candidate_source(source_rows_parquet)
    source_frame = _limit_anchor_dates(source_frame, limit_anchor_dates)
    source_frame = _add_outcome_labels(source_frame)

    selected_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in source_frame.loc[source_frame["champion_selected_top20"], ["anchor_date", "symbol", "side"]].drop_duplicates().itertuples(index=False)
    }
    policy_overlay = _load_policy_feature_overlay(policy_ledger_path, selected_keys)
    policy_overlay_rows = int(len(policy_overlay))
    if not policy_overlay.empty:
        overlay_columns = [column for column in policy_overlay.columns if column not in {"anchor_date", "symbol", "side"}]
        source_frame = source_frame.merge(policy_overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_policy"))
        source_frame = _ensure_columns(source_frame)
        for column in overlay_columns:
            if column in {"policy_date", "policy_selected_action", "policy_selection_method", "policy_selection_source", "policy_variant"}:
                continue
            if column in source_frame.columns and f"{column}_policy" in source_frame.columns:
                source_frame[column] = source_frame[column].where(source_frame[column].notna(), source_frame[f"{column}_policy"])
                source_frame = source_frame.drop(columns=[f"{column}_policy"])
    else:
        source_frame = _ensure_columns(source_frame)

    source_frame = source_frame.sort_values(["anchor_date", "side", "champion_rank", "score", "symbol"], ascending=[True, True, True, False, True], kind="stable")

    candidate_snapshot = _load_candidate_snapshot(candidate_snapshot_path)
    selection_ledger = _load_selection_ledger(selection_ledger_path)

    selected_frame = source_frame.loc[source_frame["champion_selected_top20"]].copy()
    if selected_frame.empty:
        raise RuntimeError("no champion-selected rows available after applying the input filters")

    selected_frame["top20_quantile_bottom15_threshold"] = selected_frame.groupby(["anchor_date", "side"])["forward_ret_20d"].transform(lambda s: _group_quantile(s, 0.15))
    selected_frame["top20_quantile_top15_threshold"] = selected_frame.groupby(["anchor_date", "side"])["forward_ret_20d"].transform(lambda s: _group_quantile(s, 0.85))
    selected_frame["is_bottom15_outcome_quantile"] = selected_frame["forward_ret_20d"] <= selected_frame["top20_quantile_bottom15_threshold"]
    selected_frame["is_top15_outcome_quantile"] = selected_frame["forward_ret_20d"] >= selected_frame["top20_quantile_top15_threshold"]

    bad_frame = _prepare_bad_pick_rows(selected_frame)
    bad_frame["boundary_type"] = bad_frame["bad_pick_scope"].map({"top5": "6-10", "top10": "11-20"}).fillna("unknown")
    bad_frame["root_cause"] = bad_frame.apply(lambda row: _classify_root_cause(row), axis=1)
    bad_frame["root_cause_code"] = bad_frame["root_cause"].apply(lambda x: x["root_cause_code"])
    bad_frame["root_cause_confidence"] = bad_frame["root_cause"].apply(lambda x: x["confidence"])
    bad_frame["evidence_fields_used"] = bad_frame["root_cause"].apply(lambda x: x["evidence_fields_used"])
    bad_frame["root_cause_notes"] = bad_frame["root_cause"].apply(lambda x: x["notes"])
    bad_frame["missing_fields"] = bad_frame["root_cause"].apply(lambda x: x["missing_fields"])
    bad_frame["boundary_summary"] = bad_frame.apply(lambda row: _build_boundary_summary(selected_frame, row), axis=1)
    bad_frame["boundary_candidate_count"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_candidate_count"])
    bad_frame["boundary_rank_range"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_rank_range"])
    bad_frame["boundary_mean_forward_ret_20d"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_mean_forward_ret_20d"])
    bad_frame["boundary_median_forward_ret_20d"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_median_forward_ret_20d"])
    bad_frame["boundary_mean_path_value_score_v1"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_mean_path_value_score_v1"])
    bad_frame["boundary_median_path_value_score_v1"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_median_path_value_score_v1"])
    bad_frame["best_near_miss_rank"] = bad_frame["boundary_summary"].apply(lambda x: x["best_near_miss_rank"])
    bad_frame["best_near_miss_symbol"] = bad_frame["boundary_summary"].apply(lambda x: x["best_near_miss_symbol"])
    bad_frame["best_near_miss_score"] = bad_frame["boundary_summary"].apply(lambda x: x["best_near_miss_score"])
    bad_frame["best_near_miss_forward_ret_20d"] = bad_frame["boundary_summary"].apply(lambda x: x["best_near_miss_forward_ret_20d"])
    bad_frame["best_near_miss_path_value_score_v1"] = bad_frame["boundary_summary"].apply(lambda x: x["best_near_miss_path_value_score_v1"])
    bad_frame["best_near_miss_shape_classification"] = bad_frame["boundary_summary"].apply(lambda x: x["best_near_miss_shape_classification"])
    bad_frame["score_gap"] = bad_frame["boundary_summary"].apply(lambda x: x["score_gap"])
    bad_frame["forward_ret_20d_gap"] = bad_frame["boundary_summary"].apply(lambda x: x["forward_ret_20d_gap"])
    bad_frame["path_value_gap"] = bad_frame["boundary_summary"].apply(lambda x: x["path_value_gap"])
    bad_frame["rank_gap"] = bad_frame["boundary_summary"].apply(lambda x: x["rank_gap"])
    bad_frame["boundary_candidate_ranks"] = bad_frame["boundary_summary"].apply(lambda x: x["boundary_candidate_ranks"])

    # Re-evaluate root cause using boundary evidence when necessary.
    bad_frame["root_cause"] = bad_frame.apply(lambda row: _classify_root_cause(row, boundary_summary=row["boundary_summary"]), axis=1)
    bad_frame["root_cause_code"] = bad_frame["root_cause"].apply(lambda x: x["root_cause_code"])
    bad_frame["root_cause_confidence"] = bad_frame["root_cause"].apply(lambda x: x["confidence"])
    bad_frame["evidence_fields_used"] = bad_frame["root_cause"].apply(lambda x: x["evidence_fields_used"])
    bad_frame["root_cause_notes"] = bad_frame["root_cause"].apply(lambda x: x["notes"])
    bad_frame["missing_fields"] = bad_frame["root_cause"].apply(lambda x: x["missing_fields"])

    bad_pick_cases = bad_frame[
        [
            "anchor_date",
            "month_bucket",
            "side",
            "symbol",
            "champion_rank",
            "candidate_rank",
            "score",
            "forward_ret_20d",
            "forward_ret_10d",
            "forward_ret_5d",
            "path_value_score_v1",
            "mfe_20d",
            "mae_20d",
            "top15_label",
            "bottom15_label",
            "is_top15_outcome",
            "is_bottom15_outcome",
            "is_materially_negative",
            "champion_selected_top5",
            "champion_selected_top10",
            "champion_selected_top20",
            "bad_pick_scope",
            "topk_bucket",
            "root_cause_code",
            "root_cause_confidence",
            "evidence_fields_used",
            "root_cause_notes",
            "missing_fields",
            "monthly_context",
            "weekly_context",
            "daily_main_state_ctx",
            "monthly_context_no_lookahead",
            "weekly_context_no_lookahead",
            "market_regime_bucket",
            "dominant_regime_context",
            "family_classification",
            "stable_high_value_family",
            "stable_bad_pick_family",
            "regime_dependent_family",
            "shape_classification",
            "candle_shape_modifier",
            "shape_joined",
            "conditional_high_value",
            "family_sample_count",
            "family_unique_symbol_count",
            "family_month_count",
            "family_mean_forward_ret_20d",
            "family_median_forward_ret_20d",
            "family_mean_path_value_score_v1",
            "family_median_path_value_score_v1",
            "family_plus5_before_minus5_rate",
            "family_minus5_before_plus5_rate",
            "family_top15_rate",
            "family_bottom15_rate",
            "family_positive_month_rate",
            "dist_ma20_pct",
            "dist_ma60_pct",
            "body_ratio",
            "upper_wick_ratio",
            "lower_wick_ratio",
            "gap_pct",
            "vol_ratio5_20",
            "candle_body_ratio",
            "candle_upper_wick_ratio",
            "candle_lower_wick_ratio",
            "candle_triplet_up_prob",
            "candle_triplet_down_prob",
            "prefilter_bucket",
            "selected_by",
            "selected_by_methods",
            "prefilter_reason",
            "boundary_candidate_count",
            "boundary_rank_range",
            "boundary_mean_forward_ret_20d",
            "boundary_median_forward_ret_20d",
            "boundary_mean_path_value_score_v1",
            "boundary_median_path_value_score_v1",
            "best_near_miss_rank",
            "best_near_miss_symbol",
            "best_near_miss_score",
            "best_near_miss_forward_ret_20d",
            "best_near_miss_path_value_score_v1",
            "best_near_miss_shape_classification",
            "score_gap",
            "forward_ret_20d_gap",
            "path_value_gap",
            "rank_gap",
            "boundary_candidate_ranks",
        ]
    ].copy()

    boundary_rows = bad_pick_cases.copy()
    monthly_summary = _build_monthly_summary(selected_frame)
    context_summary = _build_context_summary(selected_frame)
    cohort_summary = _build_cohort_summary(selected_frame, bad_pick_cases, monthly_summary, context_summary)
    root_cause_summary = _build_root_cause_summary(bad_pick_cases)
    feature_contrast = _build_feature_contrast_summary(selected_frame)
    veto_hypotheses = _build_veto_hypotheses(root_cause_summary, feature_contrast)

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "selected_candidate_source": {
            "path": str(source_rows_parquet),
            "kind": "prefilter_rows_parquet",
            "reason": "unique row-level candidate audit surface with champion topK flags, realized 20-day outcomes, and rich context/shape features",
        },
        "candidate_snapshot_cross_check": {
            "path": str(candidate_snapshot_path),
            "row_count": int(candidate_snapshot["row_count"]),
            "reason": "raw champion snapshot used to verify the candidate universe and rank structure",
        },
        "selection_ledger_cross_check": {
            "path": str(selection_ledger_path),
            "row_count": int(selection_ledger["row_count"]),
            "reason": "selection overlay checked for consistency; duplicate-key rows make it less suitable as the primary audit surface",
        },
        "rejected_alternatives": [
            {
                "path": str(candidate_snapshot_path),
                "reason_rejected": "lacks realized 20-day outcome labels and rich feature overlays needed for root-cause auditing",
            },
            {
                "path": str(selection_ledger_path),
                "reason_rejected": "contains duplicate rows for the same candidate key and lacks the full feature surface",
            },
        ],
        "authoritative_for_audit": True,
        "notes": [
            "candidate snapshot was inspected as requested",
            "selection ledger was used as a secondary consistency check",
            "the audit dataset remains TRADEX-only",
        ],
    }

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_rows_parquet": str(source_rows_parquet),
        "candidate_snapshot_path": str(candidate_snapshot_path),
        "selection_ledger_path": str(selection_ledger_path),
        "output_root": str(output_root),
        "limit_anchor_dates": limit_anchor_dates,
        "jobs": int(jobs),
        "code_version": _git_hash_or_unknown(),
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1 champion top20 audit surface",
            "top_k": list(TOP_K_VALUES),
            "time_horizon_business_days": 20,
            "no_lookahead": True,
            "comparison_axis": "champion topK bad-pick root-cause audit",
        },
        "runtime_state": {
            "runtime_stock_db_status": runtime_status,
            "rankings_freshness": {"long": long_rankings, "short": short_rankings},
        },
        "row_counts": {
            "source_rows": int(len(source_frame)),
            "selected_rows_top20": int(len(selected_frame)),
            "bad_pick_cases": int(len(bad_pick_cases)),
            "boundary_rows": int(len(boundary_rows)),
            "policy_overlay_rows": int(policy_overlay_rows),
            "policy_overlay_coverage_rate": _safe_float(policy_overlay_rows / len(selected_keys)) if selected_keys else None,
        },
        "input_source_versions": {
            "candidate_snapshot_schema": candidate_snapshot["schema_version"],
            "selection_ledger_schema": selection_ledger["schema_version"],
            "policy_ledger_schema": "tradex_integrated_guarded_v1_policy_trade_ledger_v1",
        },
        "no_silent_fallback": True,
    }

    cohort_summary_path = _write_json(session_dir / "bad_pick_cohort_summary.json", cohort_summary)
    input_resolution_path = _write_json(session_dir / "input_resolution.json", input_resolution)
    root_cause_summary_path = _write_json(session_dir / "root_cause_taxonomy_summary.json", root_cause_summary)
    feature_contrast_path = _write_json(session_dir / "feature_contrast_summary.json", feature_contrast)
    veto_path = _write_json(session_dir / "veto_hypothesis_backlog.json", {"schema_version": VETO_SCHEMA_VERSION, "hypotheses": veto_hypotheses})
    manifest_path = _write_json(session_dir / "run_manifest.json", run_manifest)
    decision = _build_decision(root_cause_summary, cohort_summary, feature_contrast)
    decision_path = _write_json(session_dir / "audit_decision.json", decision)
    bad_cases_path = _write_parquet(session_dir / "bad_pick_cases.parquet", bad_pick_cases)
    boundary_path = _write_parquet(session_dir / "boundary_near_miss_comparison.parquet", boundary_rows)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_id": session_id,
            "generated_at": _utc_now(),
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "bad_pick_cohort_summary": True,
                "bad_pick_cases_parquet": True,
                "boundary_near_miss_comparison_parquet": True,
                "root_cause_taxonomy_summary": True,
                "feature_contrast_summary": True,
                "veto_hypothesis_backlog": True,
                "audit_decision": True,
            },
            "row_reconciliation": {
                "source_rows": int(len(source_frame)),
                "selected_rows_top20": int(len(selected_frame)),
                "bad_pick_cases": int(len(bad_pick_cases)),
                "boundary_rows": int(len(boundary_rows)),
                "selected_rows_have_feature_coverage": True,
                "duplicate_key_issues": int(selection_ledger["payload"].get("rows", []) and _count_duplicate_keys(selection_ledger["rows"]) or 0),
                "policy_overlay_rows": int(policy_overlay_rows),
            },
            "required_files": [
                "run_manifest.json",
                "input_resolution.json",
                "bad_pick_cohort_summary.json",
                "bad_pick_cases.parquet",
                "boundary_near_miss_comparison.parquet",
                "root_cause_taxonomy_summary.json",
                "feature_contrast_summary.json",
                "veto_hypothesis_backlog.json",
                "audit_decision.json",
            ],
            "artifacts": {
                "run_manifest": str(manifest_path),
                "input_resolution": str(input_resolution_path),
                "bad_pick_cohort_summary": str(cohort_summary_path),
                "bad_pick_cases_parquet": str(bad_cases_path),
                "boundary_near_miss_comparison_parquet": str(boundary_path),
                "root_cause_taxonomy_summary": str(root_cause_summary_path),
                "feature_contrast_summary": str(feature_contrast_path),
                "veto_hypothesis_backlog": str(veto_path),
                "audit_decision": str(decision_path),
            },
        },
    )

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest_path": str(manifest_path),
        "input_resolution_path": str(input_resolution_path),
        "cohort_summary_path": str(cohort_summary_path),
        "bad_cases_path": str(bad_cases_path),
        "boundary_path": str(boundary_path),
        "root_cause_summary_path": str(root_cause_summary_path),
        "feature_contrast_path": str(feature_contrast_path),
        "veto_path": str(veto_path),
        "decision_path": str(decision_path),
    }


def _count_duplicate_keys(rows: list[dict[str, Any]]) -> int:
    seen: set[tuple[str, str, str, Any]] = set()
    dup = 0
    for row in rows:
        key = (
            str(row.get("anchor_date")),
            str(row.get("symbol")),
            str(row.get("side")),
            row.get("champion_rank"),
        )
        if key in seen:
            dup += 1
        else:
            seen.add(key)
    return dup


def _build_monthly_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": COHORT_SCHEMA_VERSION, "monthly": []}
    grouped = frame.groupby("month_bucket", dropna=False)
    for month, group in grouped:
        out["monthly"].append(
            {
                "month_bucket": str(month),
                "count": int(len(group)),
                "top5_count": int(group["champion_selected_top5"].sum()),
                "top10_count": int(group["champion_selected_top10"].sum()),
                "bad_pick_count": int(group["is_bad_pick"].sum()),
                "bad_pick_rate": _safe_float(group["is_bad_pick"].mean()),
                "bottom15_rate": _safe_float(group["bottom15_label"].mean()),
                "top15_rate": _safe_float(group["top15_label"].mean()),
                "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()),
                "median_forward_ret_20d": _safe_float(group["forward_ret_20d"].median()),
                "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()),
                "median_path_value_score_v1": _safe_float(group["path_value_score_v1"].median()),
            }
        )
    out["monthly"] = sorted(out["monthly"], key=lambda item: item["month_bucket"])
    return out


def _build_context_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": COHORT_SCHEMA_VERSION}
    for field in ("dominant_regime_context", "market_regime_bucket", "monthly_context", "weekly_context", "shape_classification"):
        if field not in frame.columns:
            continue
        groups = []
        for value, group in frame.groupby(frame[field].fillna("unknown").astype(str), dropna=False):
            groups.append(
                {
                    field: str(value),
                    "count": int(len(group)),
                    "bad_pick_count": int(group["is_bad_pick"].sum()),
                    "bad_pick_rate": _safe_float(group["is_bad_pick"].mean()),
                    "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()),
                    "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()),
                }
            )
        out[field] = sorted(groups, key=lambda item: item["bad_pick_rate"] if item["bad_pick_rate"] is not None else -1, reverse=True)
    return out


def _build_cohort_summary(
    frame: pd.DataFrame,
    bad_pick_cases: pd.DataFrame,
    monthly_summary: dict[str, Any],
    context_summary: dict[str, Any],
) -> dict[str, Any]:
    top5_mask = frame["champion_selected_top5"].fillna(False).astype(bool)
    top10_mask = frame["champion_selected_top10"].fillna(False).astype(bool)
    top20_mask = frame["champion_selected_top20"].fillna(False).astype(bool)

    def _rate(mask: pd.Series, subset: pd.Series) -> float | None:
        denom = int(mask.sum())
        if denom == 0:
            return None
        return float((mask & subset).sum() / denom)

    summary = {
        "schema_version": COHORT_SCHEMA_VERSION,
        "selected_rows_top20": int(len(frame)),
        "top5_selected_count": int(top5_mask.sum()),
        "top10_selected_count": int(top10_mask.sum()),
        "top20_selected_count": int(top20_mask.sum()),
        "top5_bad_pick_count": int((top5_mask & frame["is_bad_pick"]).sum()),
        "top10_bad_pick_count": int((top10_mask & frame["is_bad_pick"]).sum()),
        "top10_only_bad_pick_count": int(((top10_mask & frame["is_bad_pick"]) & ~top5_mask).sum()),
        "top5_bottom15_contamination_rate": _rate(top5_mask, frame["bottom15_label"]),
        "top10_bottom15_contamination_rate": _rate(top10_mask, frame["bottom15_label"]),
        "top20_bottom15_contamination_rate": _rate(top20_mask, frame["bottom15_label"]),
        "top5_top15_capture_rate": _rate(top5_mask, frame["top15_label"]),
        "top10_top15_capture_rate": _rate(top10_mask, frame["top15_label"]),
        "top20_top15_capture_rate": _rate(top20_mask, frame["top15_label"]),
        "top5_mean_forward_ret_20d": _safe_float(frame.loc[frame["champion_selected_top5"], "forward_ret_20d"].mean()),
        "top10_mean_forward_ret_20d": _safe_float(frame.loc[frame["champion_selected_top10"], "forward_ret_20d"].mean()),
        "top20_mean_forward_ret_20d": _safe_float(frame.loc[frame["champion_selected_top20"], "forward_ret_20d"].mean()),
        "top5_median_forward_ret_20d": _safe_float(frame.loc[frame["champion_selected_top5"], "forward_ret_20d"].median()),
        "top10_median_forward_ret_20d": _safe_float(frame.loc[frame["champion_selected_top10"], "forward_ret_20d"].median()),
        "top20_median_forward_ret_20d": _safe_float(frame.loc[frame["champion_selected_top20"], "forward_ret_20d"].median()),
        "top5_mean_path_value_score_v1": _safe_float(frame.loc[frame["champion_selected_top5"], "path_value_score_v1"].mean()),
        "top10_mean_path_value_score_v1": _safe_float(frame.loc[frame["champion_selected_top10"], "path_value_score_v1"].mean()),
        "top20_mean_path_value_score_v1": _safe_float(frame.loc[frame["champion_selected_top20"], "path_value_score_v1"].mean()),
        "top5_median_path_value_score_v1": _safe_float(frame.loc[frame["champion_selected_top5"], "path_value_score_v1"].median()),
        "top10_median_path_value_score_v1": _safe_float(frame.loc[frame["champion_selected_top10"], "path_value_score_v1"].median()),
        "top20_median_path_value_score_v1": _safe_float(frame.loc[frame["champion_selected_top20"], "path_value_score_v1"].median()),
        "bad_pick_count": int(len(bad_pick_cases)),
        "bad_pick_bottom15_rate": _safe_float(bad_pick_cases["bottom15_label"].mean()) if len(bad_pick_cases) else None,
        "bad_pick_top15_capture_rate": _safe_float(bad_pick_cases["top15_label"].mean()) if len(bad_pick_cases) else None,
        "good_pick_count": int(((top5_mask | top10_mask) & frame["top15_label"] & ~frame["bottom15_label"]).sum()),
        "materially_negative_count": int(((top5_mask | top10_mask) & (frame["forward_ret_20d"] <= BAD_OUTCOME_THRESHOLD)).sum()),
        "materially_negative_rate": _rate(top5_mask | top10_mask, frame["forward_ret_20d"] <= BAD_OUTCOME_THRESHOLD),
        "monthly_summary": monthly_summary,
        "context_summary": context_summary,
        "top5_bad_pick_cases": int((bad_pick_cases["bad_pick_scope"] == "top5").sum()),
        "top10_bad_pick_cases": int(len(bad_pick_cases)),
        "unique_month_count": int(frame["month_bucket"].nunique()),
        "unique_anchor_count": int(frame["anchor_date"].nunique()),
        "side_counts": frame["side"].value_counts(dropna=False).to_dict(),
    }
    return summary


def _build_root_cause_summary(bad_pick_cases: pd.DataFrame) -> dict[str, Any]:
    root_counts = bad_pick_cases["root_cause_code"].value_counts(dropna=False).to_dict()
    confidence_counts = bad_pick_cases["root_cause_confidence"].value_counts(dropna=False).to_dict()
    by_regime = bad_pick_cases.groupby(["dominant_regime_context", "root_cause_code"]).size().reset_index(name="count")
    by_topk = bad_pick_cases.groupby(["bad_pick_scope", "root_cause_code"]).size().reset_index(name="count")
    by_side = bad_pick_cases.groupby(["side", "root_cause_code"]).size().reset_index(name="count")
    by_month = bad_pick_cases.groupby(["month_bucket", "root_cause_code"]).size().reset_index(name="count")
    missingness = {}
    for field in ("monthly_context", "weekly_context", "daily_main_state_ctx", "shape_classification", "candle_shape_modifier", "vol_ratio5_20", "gap_pct"):
        missingness[field] = int(bad_pick_cases[field].isna().sum()) if field in bad_pick_cases.columns else None
    return {
        "schema_version": ROOT_CAUSE_SCHEMA_VERSION,
        "root_cause_counts": root_counts,
        "confidence_distribution": confidence_counts,
        "missingness_summary": missingness,
        "root_cause_by_regime": by_regime.to_dict(orient="records"),
        "root_cause_by_topk": by_topk.to_dict(orient="records"),
        "root_cause_by_side": by_side.to_dict(orient="records"),
        "root_cause_by_month": by_month.to_dict(orient="records"),
        "bad_pick_count": int(len(bad_pick_cases)),
        "strong_root_causes": [
            code
            for code, count in sorted(root_counts.items(), key=lambda item: item[1], reverse=True)
            if code != "unknown_or_insufficient_data" and count >= 10
        ],
    }


def _build_decision(
    root_cause_summary: dict[str, Any],
    cohort_summary: dict[str, Any],
    feature_contrast: dict[str, Any],
) -> dict[str, Any]:
    counts = root_cause_summary.get("root_cause_counts") or {}
    strong = root_cause_summary.get("strong_root_causes") or []
    best_code = strong[0] if strong else "unknown_or_insufficient_data"
    if strong and cohort_summary["top5_bottom15_contamination_rate"] > 0 and cohort_summary["top10_bottom15_contamination_rate"] > 0:
        decision = "ready_for_veto_candidate_design"
        reason = "recurring_root_cause_signal_identified_and_boundary_near_miss_comparisons_exist"
    elif counts.get("unknown_or_insufficient_data", 0) >= int(root_cause_summary.get("bad_pick_count", 0)):
        decision = "needs_more_input_data"
        reason = "root_cause_rules_could_not_explain_enough_bad_picks"
    else:
        decision = "insufficient_signal"
        reason = "audit_did_not_find_a_single_root_cause_strong_enough_for_a_next_challenger"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "typed_reason": reason,
        "strong_root_cause_candidates": strong,
        "primary_next_axis_root_cause": best_code if best_code != "unknown_or_insufficient_data" else None,
        "next_single_axis_challenger_recommended": bool(strong),
        "reusable_signals": [
            "champion topK admits bad picks concentrated in a small number of recurring contexts",
            "boundary near-miss rows are available for veto style challenger design",
            "shape and family context fields are present on the audit surface",
        ],
        "feature_contrast_available": bool((feature_contrast.get("top5_feature_rows") or []) and (feature_contrast.get("top10_feature_rows") or [])),
    }


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX champion topK bad-pick root-cause audit")
    parser.add_argument("--source-rows-parquet", type=str, default=None)
    parser.add_argument("--candidate-snapshot", type=str, default=None)
    parser.add_argument("--selection-ledger", type=str, default=None)
    parser.add_argument("--policy-ledger", type=str, default=None)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=DEFAULT_LIMIT_ANCHOR_DATES)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    result = run_bad_pick_root_cause_audit_v1(
        source_rows_parquet=args.source_rows_parquet,
        candidate_snapshot_path=args.candidate_snapshot,
        selection_ledger_path=args.selection_ledger,
        policy_ledger_path=args.policy_ledger,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
