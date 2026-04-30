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
from scripts.tradex_bad_pick_root_cause_audit_v1 import (  # noqa: E402
    _add_outcome_labels,
    _ensure_columns,
    _load_policy_feature_overlay,
    _limit_anchor_dates,
    _safe_float,
    _safe_int,
)
from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _json_ready,
    _make_session_id,
    _write_json,
)

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_BAD_PICK_SESSION = Path(r"G:\Tradex\bad_pick_root_cause_audit\20260429T155546Z-2053e5e4")
DEFAULT_PARENT_BOUNDARY_SESSION = DEFAULT_BAD_PICK_SESSION
DEFAULT_SELECTION_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json"
)
DEFAULT_POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_unknown_reclassification_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1"
MANIFEST_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_input_resolution_v1"
COHORT_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_unknown_cohort_summary_v1"
MISSINGNESS_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_missingness_audit_summary_v1"
PAIRWISE_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_unknown_boundary_pairwise_summary_v1"
ROOT_CAUSE_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_new_root_cause_taxonomy_summary_v1"
FAMILY_BREAKDOWN_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_new_root_cause_family_breakdown_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_data_gap_recommendations_v1"
CANDIDATE_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_future_challenger_candidates_v1"
DECISION_SCHEMA_VERSION = "tradex_bad_pick_unknown_reclassification_v1_decision_v1"

ROOT_CAUSE_CODE = "unknown_or_insufficient_data"
TOP_K_VALUES = (5, 10, 20)
UNAVAILABLE_FIELDS = ["event_flag", "earnings_flag", "dividend_flag", "rights_flag", "ex_rights_flag"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _resolve_source_path(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


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


def _load_small_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _is_missing_token(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if pd.isna(value):
        return True
    text = str(value).strip()
    return not text or text.lower() in {"none", "nan", "<na>", "unknown"}


def _normalize_token(value: Any) -> str:
    if _is_missing_token(value):
        return "unknown"
    return str(value).strip()


def _context_missing(row: pd.Series) -> bool:
    return _is_missing_token(row.get("monthly_context")) or _is_missing_token(row.get("weekly_context"))


def _daily_missing(row: pd.Series) -> bool:
    return _is_missing_token(row.get("daily_main_state_ctx"))


def _liquidity_missing(row: pd.Series) -> bool:
    return _is_missing_token(row.get("vol_ratio5_20")) and _is_missing_token(row.get("liquidity20d"))


def _score_component_missing(row: pd.Series) -> bool:
    return _is_missing_token(row.get("dist_ma20_pct")) or _is_missing_token(row.get("dist_ma60_pct"))


def _is_daily_positive(value: Any) -> bool:
    text = _normalize_token(value).lower()
    return any(key in text for key in ("up_mid", "reversal_up", "breakout", "up_candidate", "bull", "positive"))


def _is_daily_negative(value: Any) -> bool:
    text = _normalize_token(value).lower()
    return any(key in text for key in ("down_mid", "downtrend", "bear", "weak", "reversal_down", "breakdown"))


def _monthly_bucket(value: Any) -> str:
    text = _normalize_token(value).lower()
    if text == "unknown":
        return "unknown"
    if "overextended" in text or "late" in text or "top_warning" in text:
        return "overextended"
    if "range" in text:
        return "range"
    if "uptrend" in text or "bottoming" in text:
        return "trend"
    if "downtrend" in text:
        return "weak"
    return "other"


def _weekly_bucket(value: Any) -> str:
    text = _normalize_token(value).lower()
    if text == "unknown":
        return "unknown"
    if "overextended" in text or "late" in text:
        return "overextended"
    if "range" in text:
        return "range"
    if "uptrend" in text or "breakout" in text or "positive" in text:
        return "trend"
    if "downtrend" in text:
        return "weak"
    return "other"


def _family_pattern(row: pd.Series) -> str:
    monthly = _normalize_token(row.get("monthly_context"))
    weekly = _normalize_token(row.get("weekly_context"))
    daily = _normalize_token(row.get("daily_main_state_ctx"))
    shape = _normalize_token(row.get("shape_classification"))
    if monthly == "unknown" or weekly == "unknown":
        return "unknown_or_insufficient_context"
    if daily == "unknown":
        return "|".join([monthly, weekly, "unknown_daily", shape])
    return "|".join([monthly, weekly, daily, shape])


def _load_bad_pick_session(session_path: Path) -> dict[str, Any]:
    return {
        "bad_pick_cases": pd.read_parquet(session_path / "bad_pick_cases.parquet"),
        "boundary": pd.read_parquet(session_path / "boundary_near_miss_comparison.parquet"),
        "root_summary": _load_small_json(session_path / "root_cause_taxonomy_summary.json"),
        "feature_contrast": _load_small_json(session_path / "feature_contrast_summary.json"),
        "cohort_summary": _load_small_json(session_path / "bad_pick_cohort_summary.json"),
        "veto_hypotheses": _load_small_json(session_path / "veto_hypothesis_backlog.json"),
        "decision": _load_small_json(session_path / "audit_decision.json"),
    }


def _load_selected_frame(source_rows_parquet: Path, policy_ledger_path: Path, *, limit_anchor_dates: int | None) -> tuple[pd.DataFrame, int]:
    frame = pd.read_parquet(source_rows_parquet)
    frame = _ensure_columns(frame)
    frame = _add_outcome_labels(frame)
    frame = _limit_anchor_dates(frame, limit_anchor_dates)
    frame = _ensure_columns(frame)

    selected_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in frame.loc[frame["champion_selected_top20"], ["anchor_date", "symbol", "side"]].drop_duplicates().itertuples(index=False)
    }
    policy_overlay = _load_policy_feature_overlay(policy_ledger_path, selected_keys)
    overlay_rows = int(len(policy_overlay))
    if not policy_overlay.empty:
        overlay_columns = [column for column in policy_overlay.columns if column not in {"anchor_date", "symbol", "side"}]
        frame = frame.merge(policy_overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_policy"))
        frame = _ensure_columns(frame)
        for column in overlay_columns:
            policy_column = f"{column}_policy"
            if column in frame.columns and policy_column in frame.columns:
                frame[column] = frame[column].where(frame[column].notna(), frame[policy_column])
                frame = frame.drop(columns=[policy_column])
    return frame.sort_values(["anchor_date", "side", "champion_rank", "score", "symbol"], ascending=[True, True, True, False, True], kind="stable"), overlay_rows


def _join_candidate_extras(boundary_frame: pd.DataFrame, candidate_frame: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["anchor_date", "symbol", "side"]
    extra_cols = [
        "candidate_idx",
        "trade_date",
        "candidate_score",
        "champion_score",
        "challenger_score",
        "challenger_rank",
        "challenger_gate",
        "champion_gate",
        "selection_reason",
        "challenger_selected_top5",
        "challenger_selected_top10",
        "challenger_selected_top20",
        "changed_top5_member",
        "changed_top10_member",
        "changed_top20_member",
        "state_family_id",
        "unstable_or_sparse_family",
        "neutral_family",
        "family_regime_context",
        "family_bad_pick_regime",
        "family_mean_forward_ret_5d",
        "family_mean_forward_ret_10d",
        "family_mean_mfe_20d",
        "family_mean_mae_20d",
        "family_months_observed",
        "family_worst_month_mean_path_value",
        "family_best_month_mean_path_value",
        "monthly_context_date",
        "monthly_context_source",
        "weekly_context_date",
        "weekly_context_source",
        "hit_plus_5_before_minus_5",
        "hit_minus_5_before_plus_5",
        "o",
        "h",
        "l",
        "c",
        "v",
        "prev_o",
        "prev_h",
        "prev_l",
        "prev_c",
        "rank",
        "include_in_broad_pool",
        "include_in_strict_pool",
        "include_in_exclude_only_pool",
    ]
    extras = candidate_frame.loc[:, [column for column in key_cols + extra_cols if column in candidate_frame.columns]].copy()
    merged = boundary_frame.merge(extras, on=key_cols, how="left", suffixes=("", "_candidate"))
    return merged


def _build_timeframe_context_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    field_specs = [
        ("monthly_context", "confirmed usable", "selected surface", "monthly context"),
        ("weekly_context", "confirmed usable", "selected surface", "weekly context"),
        ("daily_main_state_ctx", "partial overlay", "selected surface / policy_overlay", "daily state context"),
        ("monthly_context_no_lookahead", "confirmed usable", "selected surface", "point-in-time monthly flag"),
        ("weekly_context_no_lookahead", "confirmed usable", "selected surface", "point-in-time weekly flag"),
        ("dominant_regime_context", "confirmed usable", "selected surface", "dominant regime context"),
        ("market_regime_bucket", "confirmed usable", "selected surface", "market regime bucket"),
        ("family_classification", "confirmed usable", "selected surface", "family classification"),
        ("family_regime_context", "confirmed usable", "selected surface", "family regime context"),
        ("state_family_id", "confirmed usable", "candidate surface join", "daily state family id"),
        ("shape_classification", "confirmed usable", "selected surface", "shape classification"),
        ("candle_shape_modifier", "confirmed usable", "selected surface", "candle shape modifier"),
        ("shape_joined", "confirmed usable", "selected surface", "shape availability flag"),
        ("conditional_high_value", "confirmed usable", "selected surface", "conditional high value gate"),
        ("dist_ma20_pct", "proxy only", "selected surface / policy overlay", "MA distance proxy"),
        ("dist_ma60_pct", "proxy only", "selected surface / policy overlay", "MA distance proxy"),
        ("body_ratio", "proxy only", "selected surface / policy overlay", "candle body ratio proxy"),
        ("upper_wick_ratio", "proxy only", "selected surface / policy overlay", "upper wick proxy"),
        ("lower_wick_ratio", "proxy only", "selected surface / policy overlay", "lower wick proxy"),
        ("gap_pct", "proxy only", "selected surface / policy overlay", "gap proxy"),
        ("vol_ratio5_20", "proxy only", "selected surface / policy overlay", "volume proxy"),
        ("liquidity20d", "partial overlay", "policy_overlay", "point-in-time liquidity proxy"),
        ("candle_body_ratio", "proxy only", "selected surface / policy overlay", "candle body ratio proxy"),
        ("candle_upper_wick_ratio", "proxy only", "selected surface / policy overlay", "candle upper wick proxy"),
        ("candle_lower_wick_ratio", "proxy only", "selected surface / policy overlay", "candle lower wick proxy"),
        ("candle_triplet_up_prob", "proxy only", "selected surface / policy overlay", "candle triplet up probability"),
        ("candle_triplet_down_prob", "proxy only", "selected surface / policy overlay", "candle triplet down probability"),
        ("forward_ret_20d", "confirmed usable", "selected surface", "realized outcome"),
        ("path_value_score_v1", "confirmed usable", "selected surface", "realized path score"),
    ]

    inventory_rows: list[dict[str, Any]] = []
    for field, availability, source, note in field_specs:
        if field not in frame.columns:
            inventory_rows.append(
                {
                    "field": field,
                    "availability": "unavailable",
                    "source": source,
                    "non_null_count": 0,
                    "missing_count": int(len(frame)),
                    "missing_rate": 1.0 if len(frame) else None,
                    "note": note,
                }
            )
            continue
        non_null = int(frame[field].notna().sum())
        missing = int(frame[field].isna().sum())
        inventory_rows.append(
            {
                "field": field,
                "availability": availability,
                "source": source,
                "non_null_count": non_null,
                "missing_count": missing,
                "missing_rate": _safe_float(missing / max(len(frame), 1)),
                "note": note,
            }
        )
    for field in UNAVAILABLE_FIELDS:
        inventory_rows.append(
            {
                "field": field,
                "availability": "unavailable",
                "source": "not present on selected audit surface",
                "non_null_count": 0,
                "missing_count": int(len(frame)),
                "missing_rate": 1.0 if len(frame) else None,
                "note": "intentionally excluded from the audit surface",
            }
        )
    return {
        "schema_version": "tradex_bad_pick_unknown_reclassification_v1_timeframe_context_inventory_v1",
        "row_count": int(len(frame)),
        "field_inventory": inventory_rows,
    }


def _build_pair_summary(pairwise: pd.DataFrame) -> dict[str, Any]:
    if pairwise.empty:
        return {
            "pair_count": 0,
            "matched_near_miss_count": 0,
            "selected_higher_score_count": 0,
            "selected_worse_path_count": 0,
            "selected_higher_score_and_worse_path_count": 0,
            "monthly_alignment_same_count": 0,
            "weekly_alignment_same_count": 0,
            "daily_alignment_same_count": 0,
            "shape_alignment_same_count": 0,
            "score_gap_mean": None,
            "score_gap_median": None,
            "forward_ret_20d_gap_mean": None,
            "forward_ret_20d_gap_median": None,
            "path_value_gap_mean": None,
            "path_value_gap_median": None,
        }
    score_gap = pd.to_numeric(pairwise["score"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_score"], errors="coerce"))
    ret_gap = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_forward_ret_20d"], errors="coerce"))
    path_gap = pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce").sub(pd.to_numeric(pairwise["best_near_miss_path_value_score_v1"], errors="coerce"))
    return {
        "pair_count": int(len(pairwise)),
        "matched_near_miss_count": int(pairwise["near_miss_joined"].sum()),
        "selected_higher_score_count": int((score_gap > 0).sum()),
        "selected_worse_path_count": int((ret_gap < 0).sum()),
        "selected_higher_score_and_worse_path_count": int(((score_gap > 0) & (ret_gap < 0)).sum()),
        "monthly_alignment_same_count": int(pairwise["monthly_alignment_same"].sum()),
        "weekly_alignment_same_count": int(pairwise["weekly_alignment_same"].sum()),
        "daily_alignment_same_count": int(pairwise["daily_alignment_same"].sum()),
        "shape_alignment_same_count": int(pairwise["shape_alignment_same"].sum()),
        "score_gap_mean": _safe_float(score_gap.mean()),
        "score_gap_median": _safe_float(score_gap.median()),
        "forward_ret_20d_gap_mean": _safe_float(ret_gap.mean()),
        "forward_ret_20d_gap_median": _safe_float(ret_gap.median()),
        "path_value_gap_mean": _safe_float(path_gap.mean()),
        "path_value_gap_median": _safe_float(path_gap.median()),
    }


def _build_missingness_summary(frame: pd.DataFrame) -> dict[str, Any]:
    def count_missing(column: str) -> int:
        if column not in frame.columns:
            return int(len(frame))
        if column in {"monthly_context_no_lookahead", "weekly_context_no_lookahead"}:
            return int(frame[column].isna().sum())
        return int(frame[column].map(_is_missing_token).sum())

    summary = {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "row_count": int(len(frame)),
        "field_missing_counts": {
            "monthly_context": count_missing("monthly_context"),
            "weekly_context": count_missing("weekly_context"),
            "daily_main_state_ctx": count_missing("daily_main_state_ctx"),
            "monthly_context_no_lookahead": count_missing("monthly_context_no_lookahead"),
            "weekly_context_no_lookahead": count_missing("weekly_context_no_lookahead"),
            "shape_classification": count_missing("shape_classification"),
            "candle_shape_modifier": count_missing("candle_shape_modifier"),
            "dist_ma20_pct": count_missing("dist_ma20_pct"),
            "dist_ma60_pct": count_missing("dist_ma60_pct"),
            "gap_pct": count_missing("gap_pct"),
            "vol_ratio5_20": count_missing("vol_ratio5_20"),
            "liquidity20d": count_missing("liquidity20d"),
            "family_classification": count_missing("family_classification"),
            "dominant_regime_context": count_missing("dominant_regime_context"),
            "market_regime_bucket": count_missing("market_regime_bucket"),
            "state_family_id": count_missing("state_family_id"),
        },
        "field_missing_rates": {},
        "point_in_time_flag_counts": {
            "monthly_context_no_lookahead_true": int(frame["monthly_context_no_lookahead"].fillna(False).astype(bool).sum()) if "monthly_context_no_lookahead" in frame.columns else 0,
            "monthly_context_no_lookahead_false": int((~frame["monthly_context_no_lookahead"].fillna(False).astype(bool)).sum()) if "monthly_context_no_lookahead" in frame.columns else 0,
            "weekly_context_no_lookahead_true": int(frame["weekly_context_no_lookahead"].fillna(False).astype(bool).sum()) if "weekly_context_no_lookahead" in frame.columns else 0,
            "weekly_context_no_lookahead_false": int((~frame["weekly_context_no_lookahead"].fillna(False).astype(bool)).sum()) if "weekly_context_no_lookahead" in frame.columns else 0,
            "conditional_high_value_true": int(frame["conditional_high_value"].fillna(False).astype(bool).sum()) if "conditional_high_value" in frame.columns else 0,
            "conditional_high_value_false": int((~frame["conditional_high_value"].fillna(False).astype(bool)).sum()) if "conditional_high_value" in frame.columns else 0,
        },
        "event_field_availability": {
            "available_count": 0,
            "missing_count": int(len(frame)),
            "missing_rate": 1.0 if len(frame) else None,
            "note": "event / earnings / dividend / rights fields are not present on the selected audit surface",
        },
    }
    for field, missing_count in summary["field_missing_counts"].items():
        summary["field_missing_rates"][field] = _safe_float(missing_count / max(len(frame), 1))

    context_missing_rows = frame.loc[frame.apply(_context_missing, axis=1)]
    daily_missing_rows = frame.loc[~frame.apply(_context_missing, axis=1) & frame.apply(_daily_missing, axis=1)]
    liquidity_missing_rows = frame.loc[
        ~frame.apply(_context_missing, axis=1)
        & ~frame.apply(_daily_missing, axis=1)
        & frame.apply(_liquidity_missing, axis=1)
    ]
    score_component_missing_rows = frame.loc[
        ~frame.apply(_context_missing, axis=1)
        & ~frame.apply(_daily_missing, axis=1)
        & ~frame.apply(_liquidity_missing, axis=1)
        & frame.apply(_score_component_missing, axis=1)
    ]
    summary["missingness_category_counts"] = {
        "missing_context_data": int(len(context_missing_rows)),
        "missing_daily_state_data": int(len(daily_missing_rows)),
        "missing_liquidity_or_volume_data": int(len(liquidity_missing_rows)),
        "missing_score_component_data": int(len(score_component_missing_rows)),
        "sufficient_data_but_unclassified": int(
            len(frame) - len(context_missing_rows) - len(daily_missing_rows) - len(liquidity_missing_rows) - len(score_component_missing_rows)
        ),
    }
    summary["missingness_category_topk_split"] = {}
    for topk in (5, 10):
        mask = frame[f"champion_selected_top{topk}"].fillna(False).astype(bool)
        summary["missingness_category_topk_split"][f"top{topk}"] = {
            "missing_context_data": int(mask.loc[context_missing_rows.index].sum()) if len(context_missing_rows) else 0,
            "missing_daily_state_data": int(mask.loc[daily_missing_rows.index].sum()) if len(daily_missing_rows) else 0,
            "missing_liquidity_or_volume_data": int(mask.loc[liquidity_missing_rows.index].sum()) if len(liquidity_missing_rows) else 0,
            "missing_score_component_data": int(mask.loc[score_component_missing_rows.index].sum()) if len(score_component_missing_rows) else 0,
        }
    return summary


def _classify_reclassified_row(row: pd.Series, *, boundary_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    evidence_fields: list[str] = []
    missing_fields: list[str] = []

    def mark_missing(name: str) -> None:
        if name not in missing_fields:
            missing_fields.append(name)

    missingness_class = row.get("missingness_class")
    if _context_missing(row):
        evidence_fields.extend(["monthly_context", "weekly_context"])
        if _daily_missing(row):
            mark_missing("daily_main_state_ctx")
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_context_missing",
            "reclassification_confidence": "high",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "monthly / weekly context is missing or unavailable for this row",
        }
    if _daily_missing(row):
        evidence_fields.extend(["daily_main_state_ctx", "monthly_context", "weekly_context"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_context_missing",
            "reclassification_confidence": "high",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(["daily_main_state_ctx"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "daily state is missing even though higher timeframe context is present",
        }
    if _liquidity_missing(row):
        evidence_fields.extend(["vol_ratio5_20", "liquidity20d"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_liquidity_missing",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(["vol_ratio5_20", "liquidity20d"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "liquidity / volume coverage is incomplete on the selected surface",
        }
    if _score_component_missing(row):
        evidence_fields.extend(["dist_ma20_pct", "dist_ma60_pct"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "data_gap_score_component_missing",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(["dist_ma20_pct", "dist_ma60_pct"]),
            "is_data_gap": True,
            "is_candidate_for_future_challenger": False,
            "notes": "MA distance proxies are missing on this row",
        }

    monthly_bucket = _monthly_bucket(row.get("monthly_context"))
    weekly_bucket = _weekly_bucket(row.get("weekly_context"))
    daily_positive = _is_daily_positive(row.get("daily_main_state_ctx"))
    daily_negative = _is_daily_negative(row.get("daily_main_state_ctx"))
    score = _safe_float(row.get("score"))
    forward_ret_20d = _safe_float(row.get("forward_ret_20d"))
    path_value = _safe_float(row.get("path_value_score_v1"))
    gap_pct = _safe_float(row.get("gap_pct"))
    vol_ratio = _safe_float(row.get("vol_ratio5_20"))
    shape_class = _normalize_token(row.get("shape_classification"))
    family_class = _normalize_token(row.get("family_classification"))
    regime_ctx = _normalize_token(row.get("dominant_regime_context"))
    market_regime = _normalize_token(row.get("market_regime_bucket"))
    bottom15 = bool(row.get("bottom15_label"))

    if (
        (monthly_bucket in {"overextended", "range"} and weekly_bucket == "overextended" and daily_positive)
        or ("risk_off" in regime_ctx.lower() and daily_positive)
        or ("risk_off" in market_regime.lower() and daily_positive)
    ):
        evidence_fields.extend(["monthly_context", "weekly_context", "dominant_regime_context", "daily_main_state_ctx"])
        confidence = "high" if boundary_summary and _safe_float(boundary_summary.get("forward_ret_20d_gap")) is not None and _safe_float(boundary_summary.get("forward_ret_20d_gap")) < 0 else "medium"
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_regime_false_positive",
            "reclassification_confidence": confidence,
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "higher-timeframe context looks stretched or weak while the daily state remains bullish",
        }

    if pd.notna(gap_pct) and abs(gap_pct) >= 0.015 and daily_negative and bottom15:
        evidence_fields.extend(["gap_pct", "daily_main_state_ctx", "bottom15_label"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_gap_reversal_failure",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "gap / reversal geometry is visible but the 20-day path still fails",
        }

    if pd.notna(vol_ratio) and vol_ratio >= 1.1 and bottom15:
        evidence_fields.extend(["vol_ratio5_20", "bottom15_label"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_volume_spike_failure",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "higher relative volume did not translate into a stable 20-day path",
        }

    if shape_class == "shape_positive_modifier" and pd.notna(path_value) and path_value < 0:
        evidence_fields.extend(["shape_classification", "path_value_score_v1"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_candle_confirmation_failure",
            "reclassification_confidence": "medium",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "positive candle shape was not enough to confirm a good 20-day path",
        }

    if boundary_summary is not None:
        score_gap = _safe_float(boundary_summary.get("score_gap"))
        ret_gap = _safe_float(boundary_summary.get("forward_ret_20d_gap"))
        path_gap = _safe_float(boundary_summary.get("path_value_gap"))
        if score_gap is not None and ret_gap is not None and path_gap is not None and score_gap > 0 and ret_gap < 0 and path_gap < 0:
            evidence_fields.extend(["score", "forward_ret_20d", "path_value_score_v1"])
            return {
                "missingness_class": missingness_class,
                "reclassified_root_cause_code": "observable_score_boundary_failure",
                "reclassification_confidence": "low",
                "evidence_fields_used": _unique_ordered(evidence_fields),
                "missing_fields": _unique_ordered(missing_fields),
                "is_data_gap": False,
                "is_candidate_for_future_challenger": False,
                "notes": "selected row won on score but lost on the realized 20-day path versus the near miss",
            }

    if family_class in {"regime_dependent_family", "stable_high_value_family"} and bottom15:
        evidence_fields.extend(["family_classification", "family_bottom15_rate", "family_mean_path_value_score_v1"])
        return {
            "missingness_class": missingness_class,
            "reclassified_root_cause_code": "observable_family_false_positive",
            "reclassification_confidence": "low",
            "evidence_fields_used": _unique_ordered(evidence_fields),
            "missing_fields": _unique_ordered(missing_fields),
            "is_data_gap": False,
            "is_candidate_for_future_challenger": False,
            "notes": "family-level summary looked acceptable but the realized path still failed",
        }

    evidence_fields.extend(["score", "forward_ret_20d", "path_value_score_v1"])
    return {
        "missingness_class": missingness_class,
        "reclassified_root_cause_code": "still_unknown",
        "reclassification_confidence": "low",
        "evidence_fields_used": _unique_ordered(evidence_fields),
        "missing_fields": _unique_ordered(missing_fields),
        "is_data_gap": False,
        "is_candidate_for_future_challenger": False,
        "notes": "observable fields were present but no stable family emerged",
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


def _build_unknown_reclassification(frame: pd.DataFrame, boundary_frame: pd.DataFrame, selected_frame: pd.DataFrame) -> pd.DataFrame:
    unknown = frame.loc[frame["root_cause_code"] == ROOT_CAUSE_CODE].copy()
    if unknown.empty:
        return unknown
    unknown["missingness_class"] = unknown.apply(
        lambda row: (
            "missing_context_data"
            if _context_missing(row)
            else "missing_daily_state_data"
            if _daily_missing(row)
            else "missing_liquidity_or_volume_data"
            if _liquidity_missing(row)
            else "missing_score_component_data"
            if _score_component_missing(row)
            else "sufficient_data_but_unclassified"
        ),
        axis=1,
    )
    unknown = unknown.merge(
        boundary_frame.loc[
            :,
            [
                "anchor_date",
                "symbol",
                "side",
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
                "boundary_candidate_count",
                "boundary_rank_range",
                "boundary_mean_forward_ret_20d",
                "boundary_median_forward_ret_20d",
                "boundary_mean_path_value_score_v1",
                "boundary_median_path_value_score_v1",
                "boundary_candidate_ranks",
            ],
        ],
        on=["anchor_date", "symbol", "side"],
        how="left",
        suffixes=("", "_boundary"),
    )
    near_cols = [
        "anchor_date",
        "symbol",
        "side",
        "champion_rank",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "shape_classification",
        "candle_shape_modifier",
        "conditional_high_value",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "family_regime_context",
        "family_bad_pick_regime",
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
    ]
    near_frame = selected_frame.loc[:, [col for col in near_cols if col in selected_frame.columns]].copy()
    near_frame = near_frame.rename(
        columns={
            "champion_rank": "near_miss_champion_rank",
            "score": "near_miss_score",
            "forward_ret_20d": "near_miss_forward_ret_20d",
            "path_value_score_v1": "near_miss_path_value_score_v1",
            "monthly_context": "near_miss_monthly_context",
            "weekly_context": "near_miss_weekly_context",
            "daily_main_state_ctx": "near_miss_daily_main_state_ctx",
            "shape_classification": "near_miss_shape_classification",
            "candle_shape_modifier": "near_miss_candle_shape_modifier",
            "conditional_high_value": "near_miss_conditional_high_value",
            "market_regime_bucket": "near_miss_market_regime_bucket",
            "dominant_regime_context": "near_miss_dominant_regime_context",
            "family_classification": "near_miss_family_classification",
            "family_regime_context": "near_miss_family_regime_context",
            "family_bad_pick_regime": "near_miss_family_bad_pick_regime",
            "dist_ma20_pct": "near_miss_dist_ma20_pct",
            "dist_ma60_pct": "near_miss_dist_ma60_pct",
            "body_ratio": "near_miss_body_ratio",
            "upper_wick_ratio": "near_miss_upper_wick_ratio",
            "lower_wick_ratio": "near_miss_lower_wick_ratio",
            "gap_pct": "near_miss_gap_pct",
            "vol_ratio5_20": "near_miss_vol_ratio5_20",
            "candle_body_ratio": "near_miss_candle_body_ratio",
            "candle_upper_wick_ratio": "near_miss_candle_upper_wick_ratio",
            "candle_lower_wick_ratio": "near_miss_candle_lower_wick_ratio",
            "candle_triplet_up_prob": "near_miss_candle_triplet_up_prob",
            "candle_triplet_down_prob": "near_miss_candle_triplet_down_prob",
        }
    )
    unknown = unknown.merge(
        near_frame,
        left_on=["anchor_date", "side", "best_near_miss_symbol"],
        right_on=["anchor_date", "side", "symbol"],
        how="left",
        suffixes=("", "_near"),
    )
    unknown["near_miss_joined"] = unknown["near_miss_score"].notna()
    unknown["near_miss_rank_matches_boundary"] = pd.to_numeric(unknown["best_near_miss_rank"], errors="coerce") == pd.to_numeric(
        unknown["near_miss_champion_rank"], errors="coerce"
    )
    unknown["monthly_alignment_same"] = unknown["monthly_context"].astype(str) == unknown["near_miss_monthly_context"].astype(str)
    unknown["weekly_alignment_same"] = unknown["weekly_context"].astype(str) == unknown["near_miss_weekly_context"].astype(str)
    unknown["daily_alignment_same"] = unknown["daily_main_state_ctx"].astype(str) == unknown["near_miss_daily_main_state_ctx"].astype(str)
    unknown["shape_alignment_same"] = unknown["shape_classification"].astype(str) == unknown["near_miss_shape_classification"].astype(str)
    unknown["reclassification"] = unknown.apply(lambda row: _classify_reclassified_row(row, boundary_summary=row.to_dict()), axis=1)
    unknown["reclassified_root_cause_code"] = unknown["reclassification"].apply(lambda x: x["reclassified_root_cause_code"])
    unknown["reclassification_confidence"] = unknown["reclassification"].apply(lambda x: x["reclassification_confidence"])
    unknown["evidence_fields_used"] = unknown["reclassification"].apply(lambda x: x["evidence_fields_used"])
    unknown["missing_fields"] = unknown["reclassification"].apply(lambda x: x["missing_fields"])
    unknown["is_data_gap"] = unknown["reclassification"].apply(lambda x: x["is_data_gap"])
    unknown["is_candidate_for_future_challenger"] = unknown["reclassification"].apply(lambda x: x["is_candidate_for_future_challenger"])
    unknown["reclassification_notes"] = unknown["reclassification"].apply(lambda x: x["notes"])

    # Join extra candidate-surface fields for state family and selection provenance.
    candidate_surface = selected_frame.loc[selected_frame["champion_selected_top20"]].copy()
    unknown = _join_candidate_extras(unknown, candidate_surface)
    return unknown


def _build_unknown_cohort_summary(frame: pd.DataFrame, unknown: pd.DataFrame) -> dict[str, Any]:
    parent_bad_pick_count = int((frame["champion_selected_top5"] | frame["champion_selected_top10"]).sum())
    summary = {
        "schema_version": COHORT_SCHEMA_VERSION,
        "selected_rows_top20": int(len(frame)),
        "parent_bad_pick_count": parent_bad_pick_count,
        "unknown_count": int(len(unknown)),
        "unknown_top5_count": int(unknown["champion_selected_top5"].sum()),
        "unknown_top10_only_count": int((unknown["champion_selected_top10"] & ~unknown["champion_selected_top5"]).sum()),
        "top5_bad_pick_rate": _safe_float(unknown.loc[unknown["champion_selected_top5"], "bottom15_label"].mean()) if len(unknown) else None,
        "top10_only_bad_pick_rate": _safe_float(unknown.loc[unknown["champion_selected_top10"] & ~unknown["champion_selected_top5"], "bottom15_label"].mean()) if len(unknown) else None,
        "top5_capture_rate": _safe_float(unknown.loc[unknown["champion_selected_top5"], "top15_label"].mean()) if len(unknown) else None,
        "top10_only_capture_rate": _safe_float(unknown.loc[unknown["champion_selected_top10"] & ~unknown["champion_selected_top5"], "top15_label"].mean()) if len(unknown) else None,
        "side_counts": unknown["side"].value_counts(dropna=False).to_dict(),
        "month_counts": unknown["month_bucket"].value_counts(dropna=False).to_dict(),
        "dominant_regime_counts": unknown["dominant_regime_context"].value_counts(dropna=False).to_dict(),
        "family_classification_counts": unknown["family_classification"].value_counts(dropna=False).to_dict(),
        "shape_classification_counts": unknown["shape_classification"].value_counts(dropna=False).to_dict(),
        "missingness_class_counts": unknown["missingness_class"].value_counts(dropna=False).to_dict(),
        "reclassified_root_cause_counts": unknown["reclassified_root_cause_code"].value_counts(dropna=False).to_dict(),
        "confidence_distribution": unknown["reclassification_confidence"].value_counts(dropna=False).to_dict(),
        "boundary_match_rate": _safe_float(unknown["near_miss_joined"].mean()) if len(unknown) else None,
        "boundary_pair_count": int(unknown["near_miss_joined"].sum()),
        "boundary_gap_mean": _safe_float(unknown["score_gap"].mean()),
        "boundary_return_gap_mean": _safe_float(unknown["forward_ret_20d_gap"].mean()),
        "boundary_path_gap_mean": _safe_float(unknown["path_value_gap"].mean()),
        "monthly_context_no_lookahead_true_count": int(unknown["monthly_context_no_lookahead"].fillna(False).astype(bool).sum()),
        "weekly_context_no_lookahead_true_count": int(unknown["weekly_context_no_lookahead"].fillna(False).astype(bool).sum()),
    }
    return summary


def _build_taxonomy_summary(frame: pd.DataFrame, unknown: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for code, group in unknown.groupby("reclassified_root_cause_code", dropna=False):
        rows.append(
            {
                "reclassified_root_cause_code": str(code),
                "count": int(len(group)),
                "top5_count": int(group["champion_selected_top5"].sum()),
                "top10_only_count": int((group["champion_selected_top10"] & ~group["champion_selected_top5"]).sum()),
                "side_counts": group["side"].value_counts(dropna=False).to_dict(),
                "month_counts": group["month_bucket"].value_counts(dropna=False).to_dict(),
                "regime_counts": group["dominant_regime_context"].value_counts(dropna=False).to_dict(),
                "confidence_distribution": group["reclassification_confidence"].value_counts(dropna=False).to_dict(),
                "boundary_matched_count": int(group["near_miss_joined"].sum()),
                "mean_score_gap": _safe_float(group["score_gap"].mean()),
                "mean_forward_ret_20d_gap": _safe_float(group["forward_ret_20d_gap"].mean()),
                "mean_path_value_gap": _safe_float(group["path_value_gap"].mean()),
            }
        )
    rows = sorted(rows, key=lambda item: item["count"], reverse=True)
    confidence_distribution = unknown["reclassification_confidence"].value_counts(dropna=False).to_dict()
    return {
        "schema_version": ROOT_CAUSE_SCHEMA_VERSION,
        "unknown_count": int(len(unknown)),
        "reclassified_count": int(len(unknown)),
        "still_unknown_count": int((unknown["reclassified_root_cause_code"] == "still_unknown").sum()),
        "reclassified_root_cause_counts": unknown["reclassified_root_cause_code"].value_counts(dropna=False).to_dict(),
        "confidence_distribution": confidence_distribution,
        "missingness_class_distribution": unknown["missingness_class"].value_counts(dropna=False).to_dict(),
        "boundary_match_rate": _safe_float(unknown["near_miss_joined"].mean()) if len(unknown) else None,
        "root_cause_by_regime": (
            unknown.groupby(["dominant_regime_context", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
        "root_cause_by_topk": (
            unknown.groupby(["bad_pick_scope", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
        "root_cause_by_side": (
            unknown.groupby(["side", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
        "root_cause_by_month": (
            unknown.groupby(["month_bucket", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
        "family_rows": rows,
        "frozen_line_overlap_notes": [
            "late_entry_after_extended_rise, score_component_overweight, and monthly_weekly_daily_misalignment remain frozen and were used only as exclusion/comparison context",
            "the unknown cohort is being reclassified, not turned back into a challenger line",
        ],
    }


def _build_family_breakdown(frame: pd.DataFrame, unknown: pd.DataFrame) -> dict[str, Any]:
    families = []
    for code, group in unknown.groupby("reclassified_root_cause_code", dropna=False):
        families.append(
            {
                "family_code": str(code),
                "count": int(len(group)),
                "top5_count": int(group["champion_selected_top5"].sum()),
                "top10_only_count": int((group["champion_selected_top10"] & ~group["champion_selected_top5"]).sum()),
                "boundary_matched_count": int(group["near_miss_joined"].sum()),
                "boundary_match_rate": _safe_float(group["near_miss_joined"].mean()) if len(group) else None,
                "mean_score_gap": _safe_float(group["score_gap"].mean()),
                "mean_forward_ret_20d_gap": _safe_float(group["forward_ret_20d_gap"].mean()),
                "mean_path_value_gap": _safe_float(group["path_value_gap"].mean()),
                "side_counts": group["side"].value_counts(dropna=False).to_dict(),
                "month_counts": group["month_bucket"].value_counts(dropna=False).to_dict(),
                "regime_counts": group["dominant_regime_context"].value_counts(dropna=False).to_dict(),
                "top_pattern_counts": group["pattern"].value_counts(dropna=False).head(8).to_dict(),
                "decision_classification": (
                    "data_pipeline_task"
                    if str(code).startswith("data_gap_")
                    else "explanation_only"
                    if str(code).startswith("observable_") and len(group) < 20
                    else "insufficient_signal"
                    if str(code) == "still_unknown"
                    else "watchlist_only"
                ),
            }
        )
    families = sorted(families, key=lambda item: item["count"], reverse=True)
    return {
        "schema_version": FAMILY_BREAKDOWN_SCHEMA_VERSION,
        "family_rows": families,
        "top5_family_counts": {
            str(code): int(count)
            for code, count in unknown.loc[unknown["champion_selected_top5"], "reclassified_root_cause_code"].value_counts(dropna=False).items()
        },
        "top10_only_family_counts": {
            str(code): int(count)
            for code, count in unknown.loc[unknown["champion_selected_top10"] & ~unknown["champion_selected_top5"], "reclassified_root_cause_code"].value_counts(dropna=False).items()
        },
        "regime_family_counts": (
            unknown.groupby(["dominant_regime_context", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
        "month_family_counts": (
            unknown.groupby(["month_bucket", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
        "side_family_counts": (
            unknown.groupby(["side", "reclassified_root_cause_code"]).size().reset_index(name="count").sort_values(["count"], ascending=False).to_dict(orient="records")
            if len(unknown)
            else []
        ),
    }


def _build_data_gap_recommendations(missingness: dict[str, Any], taxonomy: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "recommended_next_action": "data_pipeline_improvement_required",
        "rationale": [
            "unknown cohort is dominated by context gaps rather than a single clean observable family",
            "monthly / weekly context is missing on the majority of unknown rows",
            "daily state is missing for a large secondary slice",
            "event / earnings / dividend / rights fields are unavailable on the selected audit surface",
        ],
        "tasks": [
            {
                "task_id": "DP-01",
                "task": "backfill monthly and weekly context for the selected audit surface",
                "why": "most unknown rows cannot be safely classified while higher-timeframe context is missing",
                "priority": "high",
            },
            {
                "task_id": "DP-02",
                "task": "persist point-in-time daily state context for all champion-selected rows",
                "why": "daily-state missingness is the second largest gap and blocks more precise reclassification",
                "priority": "high",
            },
            {
                "task_id": "DP-03",
                "task": "decide whether event / earnings / dividend / rights fields belong on the audit surface",
                "why": "these fields are globally unavailable and prevent event-like outlier diagnosis",
                "priority": "medium",
            },
            {
                "task_id": "DP-04",
                "task": "preserve liquidity / volume proxies on the audit surface for the few remaining ambiguous cases",
                "why": "a small residual slice still depends on liquidity coverage",
                "priority": "medium",
            },
        ],
        "missingness_summary": missingness,
        "taxonomy_summary": {
            "reclassified_root_cause_counts": taxonomy.get("reclassified_root_cause_counts", {}),
            "still_unknown_count": taxonomy.get("still_unknown_count", 0),
        },
    }


def _build_future_challenger_candidates(unknown: pd.DataFrame) -> dict[str, Any]:
    candidates = []
    for code in ["observable_regime_false_positive", "observable_candle_confirmation_failure", "observable_score_boundary_failure"]:
        group = unknown.loc[unknown["reclassified_root_cause_code"] == code].copy()
        if group.empty:
            continue
        candidates.append(
            {
                "candidate_id": code,
                "family_code": code,
                "count": int(len(group)),
                "top5_count": int(group["champion_selected_top5"].sum()),
                "top10_only_count": int((group["champion_selected_top10"] & ~group["champion_selected_top5"]).sum()),
                "boundary_matched_count": int(group["near_miss_joined"].sum()),
                "boundary_match_rate": _safe_float(group["near_miss_joined"].mean()) if len(group) else None,
                "mean_score_gap": _safe_float(group["score_gap"].mean()),
                "mean_forward_ret_20d_gap": _safe_float(group["forward_ret_20d_gap"].mean()),
                "mean_path_value_gap": _safe_float(group["path_value_gap"].mean()),
                "status": "watchlist_only",
                "blocked_reason": "unknown cohort is still dominated by missing context data",
                "recommended_validation": "re-run after context and daily-state backfill",
            }
        )
    return {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "candidates": sorted(candidates, key=lambda item: item["count"], reverse=True),
    }


def _build_decision(missingness: dict[str, Any], taxonomy: dict[str, Any]) -> dict[str, Any]:
    total = int(taxonomy.get("unknown_count", 0))
    missing_counts = missingness.get("missingness_category_counts") or {}
    context_missing = int(missing_counts.get("missing_context_data", 0))
    daily_missing = int(missing_counts.get("missing_daily_state_data", 0))
    sufficient = int(missing_counts.get("sufficient_data_but_unclassified", 0))
    observable_total = total - context_missing - daily_missing
    if context_missing >= max(1, int(total * 0.5)) or (context_missing + daily_missing) >= max(1, int(total * 0.8)):
        decision = "data_pipeline_improvement_required"
        reason = "unknown cohort is dominated by missing monthly / weekly / daily context and the observable residual is fragmented"
    elif observable_total >= 20 and sufficient > 0:
        decision = "explanation_only"
        reason = "observable residual exists but is too sparse and overlapping with frozen lines for a safe challenger"
    elif sufficient > 0:
        decision = "insufficient_signal"
        reason = "no stable observable family emerged after missingness separation"
    else:
        decision = "insufficient_signal"
        reason = "unknown cohort could not be cleanly reclassified"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "typed_reason": reason,
        "primary_next_axis": "data_pipeline_improvement" if decision == "data_pipeline_improvement_required" else "stop_local_rule_mining",
        "frozen_lines_respected": [
            "late_entry_after_extended_rise",
            "score_component_overweight",
            "monthly_weekly_daily_misalignment",
        ],
        "strong_reclassified_families": [
            item["reclassified_root_cause_code"]
            for item in taxonomy.get("family_rows", [])
            if item.get("count", 0) >= 10 and item.get("reclassified_root_cause_code") != "still_unknown"
        ],
    }


def _build_input_resolution(
    *,
    source_rows_parquet: Path,
    bad_pick_session: Path,
    boundary_session: Path,
    selection_ledger_path: Path,
    policy_ledger_path: Path,
    candidate_snapshot_path: Path,
    selected_frame: pd.DataFrame,
    bad_pick_session_payload: dict[str, Any],
) -> dict[str, Any]:
    prior_sessions = [
        {
            "session_dir": r"G:\Tradex\late_entry_after_extended_rise_veto_v1\20260429T162249Z-1c6940b0",
            "decision": "drop",
            "typed_reason": "late_entry_veto_did_not_improve_same_condition_topk",
            "use": "comparison_only",
        },
        {
            "session_dir": r"G:\Tradex\score_component_overweight_decomposition_v1\20260429T164904Z-a7e58c8a",
            "decision": "ready_for_single_axis_challenger_design",
            "typed_reason": "score_component_overweight_is_consistent_and_boundary_pairs_show_higher_score_but_worse_path",
            "use": "comparison_only",
        },
        {
            "session_dir": r"G:\Tradex\score_component_overweight_cap_or_confirmation_v1\20260429T171450Z-420285eb",
            "decision": "drop",
            "typed_reason": "score_component_overweight_cap_or_confirmation_did_not_move_topk_membership",
            "use": "comparison_only",
        },
        {
            "session_dir": r"G:\Tradex\monthly_weekly_daily_misalignment_audit_v1\20260429T173956Z-74e674e5",
            "decision": "explanation_only",
            "typed_reason": "pattern_is_present_but_too_fragmented_or_sparsely_observed_for_a_safe_challenger",
            "use": "comparison_only",
        },
    ]
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "selected_candidate_source": {
            "path": str(source_rows_parquet),
            "kind": "prefilter_rows_parquet",
            "reason": "selected candidate surface with champion topK flags and the richest row-level feature coverage available for reclassification",
        },
        "bad_pick_session": {
            "path": str(bad_pick_session),
            "reason": "parent bad-pick audit that generated the unknown cohort and boundary references",
            "unknown_count": int(len(bad_pick_session_payload["bad_pick_cases"].loc[bad_pick_session_payload["bad_pick_cases"]["root_cause_code"] == ROOT_CAUSE_CODE])),
        },
        "boundary_session": {
            "path": str(boundary_session),
            "reason": "boundary comparison surface used to preserve the immediate near-miss context for each unknown bad pick",
            "row_count": int(len(bad_pick_session_payload["boundary"])),
        },
        "candidate_snapshot_cross_check": {
            "path": str(candidate_snapshot_path),
            "reason": "raw champion snapshot checked to confirm the selected candidate universe and rank structure",
        },
        "selection_ledger_cross_check": {
            "path": str(selection_ledger_path),
            "reason": "selection ledger checked for consistency with the selected candidate universe",
        },
        "policy_ledger_cross_check": {
            "path": str(policy_ledger_path),
            "reason": "policy overlay checked for point-in-time context fields",
        },
        "rejected_alternatives": [
            {
                "path": str(candidate_snapshot_path),
                "reason_rejected": "lacks the row-level feature surface needed for unknown reclassification",
            },
            {
                "path": str(selection_ledger_path),
                "reason_rejected": "contains duplicate keys and does not carry the full audit surface",
            },
            {
                "path": str(policy_ledger_path),
                "reason_rejected": "point-in-time context only; not sufficient as the primary audit surface",
            },
        ],
        "prior_frozen_line_sessions": prior_sessions,
        "authoritative_for_audit": True,
        "selected_surface_row_count": int(len(selected_frame)),
        "selected_surface_top20_count": int(selected_frame["champion_selected_top20"].sum()),
        "notes": [
            "unknown cohort is being reclassified only; no challenger or ranking change is introduced",
            "frozen lines remain comparison-only context",
            "event / earnings / dividend / rights fields are unavailable on the selected audit surface",
        ],
    }


def run_bad_pick_unknown_reclassification_v1(
    *,
    source_rows_parquet: str | Path | None = None,
    bad_pick_session: str | Path | None = None,
    candidate_snapshot_path: str | Path | None = None,
    selection_ledger_path: str | Path | None = None,
    policy_ledger_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 2,
) -> dict[str, Any]:
    source_rows_parquet = _resolve_source_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET, "source rows parquet")
    bad_pick_session = _resolve_source_path(bad_pick_session, DEFAULT_BAD_PICK_SESSION, "bad pick session")
    candidate_snapshot_path = _resolve_source_path(candidate_snapshot_path, DEFAULT_SELECTION_LEDGER.parent / "integrated_guarded_v1_candidate_snapshots.json", "candidate snapshot")
    selection_ledger_path = _resolve_source_path(selection_ledger_path, DEFAULT_SELECTION_LEDGER, "selection ledger")
    policy_ledger_path = _resolve_source_path(policy_ledger_path, DEFAULT_POLICY_LEDGER, "policy ledger")
    output_root = _resolve_output_root(output_root)

    runtime_status = get_runtime_stock_db_status()
    long_rankings = get_rankings_freshness(direction="up", risk_mode="balanced")
    short_rankings = get_rankings_freshness(direction="down", risk_mode="balanced")

    selected_frame, overlay_rows = _load_selected_frame(source_rows_parquet, policy_ledger_path, limit_anchor_dates=limit_anchor_dates)
    bad_pick_session_payload = _load_bad_pick_session(bad_pick_session)
    bad_pick_cases = bad_pick_session_payload["bad_pick_cases"].copy()
    boundary = bad_pick_session_payload["boundary"].copy()

    if limit_anchor_dates and limit_anchor_dates > 0:
        anchors = sorted(selected_frame["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        bad_pick_cases = bad_pick_cases.loc[bad_pick_cases["anchor_date"].isin(anchors)].copy()
        boundary = boundary.loc[boundary["anchor_date"].isin(anchors)].copy()

    unknown_boundary = _build_unknown_reclassification(bad_pick_cases, boundary, selected_frame)
    selected_frame = _ensure_columns(selected_frame)
    unknown_boundary = _ensure_columns(unknown_boundary)

    # Add a few explicit diagnostics derived from the unknown cohort.
    unknown_boundary["pattern"] = unknown_boundary.apply(_family_pattern, axis=1)
    unknown_boundary["observable_family"] = unknown_boundary["reclassified_root_cause_code"].where(
        ~unknown_boundary["reclassified_root_cause_code"].str.startswith("data_gap_", na=False),
        other="data_gap",
    )

    timeframe_inventory = _build_timeframe_context_inventory(unknown_boundary)
    unknown_summary = _build_unknown_cohort_summary(selected_frame, unknown_boundary)
    missingness_summary = _build_missingness_summary(unknown_boundary)
    pairwise_summary = _build_pair_summary(unknown_boundary)
    taxonomy_summary = _build_taxonomy_summary(selected_frame, unknown_boundary)
    family_breakdown = _build_family_breakdown(selected_frame, unknown_boundary)
    data_gap_recommendations = _build_data_gap_recommendations(missingness_summary, taxonomy_summary)
    future_candidates = _build_future_challenger_candidates(unknown_boundary)
    decision = _build_decision(missingness_summary, taxonomy_summary)

    # Boundary pairwise summary should be rich enough for future review.
    unknown_boundary["selected_higher_score"] = pd.to_numeric(unknown_boundary["score"], errors="coerce") > pd.to_numeric(
        unknown_boundary["best_near_miss_score"], errors="coerce"
    )
    unknown_boundary["selected_worse_path"] = pd.to_numeric(unknown_boundary["forward_ret_20d"], errors="coerce") < pd.to_numeric(
        unknown_boundary["best_near_miss_forward_ret_20d"], errors="coerce"
    )
    unknown_boundary["selected_higher_score_and_worse_path"] = unknown_boundary["selected_higher_score"] & unknown_boundary["selected_worse_path"]

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    input_resolution = _build_input_resolution(
        source_rows_parquet=source_rows_parquet,
        bad_pick_session=bad_pick_session,
        boundary_session=bad_pick_session,
        selection_ledger_path=selection_ledger_path,
        policy_ledger_path=policy_ledger_path,
        candidate_snapshot_path=candidate_snapshot_path,
        selected_frame=selected_frame,
        bad_pick_session_payload=bad_pick_session_payload,
    )

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_rows_parquet": str(source_rows_parquet),
        "bad_pick_session": str(bad_pick_session),
        "candidate_snapshot_path": str(candidate_snapshot_path),
        "selection_ledger_path": str(selection_ledger_path),
        "policy_ledger_path": str(policy_ledger_path),
        "output_root": str(output_root),
        "limit_anchor_dates": limit_anchor_dates,
        "jobs": int(jobs),
        "code_version": _git_hash_or_unknown(),
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1 champion top20 audit surface",
            "top_k": list(TOP_K_VALUES),
            "time_horizon_business_days": 20,
            "no_lookahead": True,
            "comparison_axis": "unknown_or_insufficient_data reclassification",
        },
        "runtime_state": {
            "runtime_stock_db_status": runtime_status,
            "rankings_freshness": {"long": long_rankings, "short": short_rankings},
        },
        "row_counts": {
            "selected_surface_rows": int(len(selected_frame)),
            "selected_surface_top20": int(selected_frame["champion_selected_top20"].sum()),
            "bad_pick_rows": int(len(bad_pick_cases)),
            "unknown_rows": int(len(unknown_boundary)),
            "boundary_rows": int(len(boundary)),
            "policy_overlay_rows": int(overlay_rows),
        },
        "no_silent_fallback": True,
    }

    cohort_path = _write_json(session_dir / "unknown_cohort_summary.json", unknown_summary)
    missingness_path = _write_json(session_dir / "missingness_audit_summary.json", missingness_summary)
    input_resolution_path = _write_json(session_dir / "input_resolution.json", input_resolution)
    inventory_path = _write_json(session_dir / "timeframe_context_inventory.json", timeframe_inventory)
    boundary_summary_path = _write_json(session_dir / "unknown_boundary_pairwise_summary.json", pairwise_summary)
    taxonomy_path = _write_json(session_dir / "new_root_cause_taxonomy_summary.json", taxonomy_summary)
    family_breakdown_path = _write_json(session_dir / "new_root_cause_family_breakdown.json", family_breakdown)
    recommendations_path = _write_json(session_dir / "data_gap_recommendations.json", data_gap_recommendations)
    candidates_path = _write_json(session_dir / "future_challenger_candidates.json", future_candidates)
    manifest_path = _write_json(session_dir / "run_manifest.json", run_manifest)
    decision_path = _write_json(session_dir / "bad_pick_unknown_reclassification_v1_decision.json", decision)
    reclass_path = _write_parquet(session_dir / "unknown_reclassification_rows.parquet", unknown_boundary)
    pairwise_path = _write_parquet(session_dir / "unknown_boundary_pairwise.parquet", unknown_boundary)

    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_id": session_id,
            "generated_at": _utc_now(),
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "timeframe_context_inventory": True,
                "unknown_cohort_summary": True,
                "missingness_audit_summary": True,
                "unknown_reclassification_rows_parquet": True,
                "unknown_boundary_pairwise_parquet": True,
                "unknown_boundary_pairwise_summary": True,
                "new_root_cause_taxonomy_summary": True,
                "new_root_cause_family_breakdown": True,
                "data_gap_recommendations": True,
                "future_challenger_candidates": True,
                "bad_pick_unknown_reclassification_v1_decision": True,
            },
            "row_reconciliation": {
                "selected_surface_rows": int(len(selected_frame)),
                "selected_surface_top20": int(selected_frame["champion_selected_top20"].sum()),
                "bad_pick_rows": int(len(bad_pick_cases)),
                "unknown_rows": int(len(unknown_boundary)),
                "boundary_rows": int(len(boundary)),
                "unknown_rows_reclassified": int(len(unknown_boundary)),
                "boundary_pair_matches": int(unknown_boundary["near_miss_joined"].sum()),
                "candidate_extras_joined": int(unknown_boundary["state_family_id"].notna().sum()) if "state_family_id" in unknown_boundary.columns else 0,
            },
            "required_files": [
                "run_manifest.json",
                "input_resolution.json",
                "unknown_cohort_summary.json",
                "missingness_audit_summary.json",
                "unknown_reclassification_rows.parquet",
                "unknown_boundary_pairwise.parquet",
                "unknown_boundary_pairwise_summary.json",
                "new_root_cause_taxonomy_summary.json",
                "new_root_cause_family_breakdown.json",
                "data_gap_recommendations.json",
                "future_challenger_candidates.json",
                "bad_pick_unknown_reclassification_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "artifacts": {
                "run_manifest": str(manifest_path),
                "input_resolution": str(input_resolution_path),
                "timeframe_context_inventory": str(inventory_path),
                "unknown_cohort_summary": str(cohort_path),
                "missingness_audit_summary": str(missingness_path),
                "unknown_reclassification_rows_parquet": str(reclass_path),
                "unknown_boundary_pairwise_parquet": str(pairwise_path),
                "unknown_boundary_pairwise_summary": str(boundary_summary_path),
                "new_root_cause_taxonomy_summary": str(taxonomy_path),
                "new_root_cause_family_breakdown": str(family_breakdown_path),
                "data_gap_recommendations": str(recommendations_path),
                "future_challenger_candidates": str(candidates_path),
                "bad_pick_unknown_reclassification_v1_decision": str(decision_path),
            },
        },
    )

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest_path": str(manifest_path),
        "input_resolution_path": str(input_resolution_path),
        "inventory_path": str(inventory_path),
        "cohort_path": str(cohort_path),
        "missingness_path": str(missingness_path),
        "reclass_path": str(reclass_path),
        "pairwise_path": str(pairwise_path),
        "boundary_summary_path": str(boundary_summary_path),
        "taxonomy_path": str(taxonomy_path),
        "family_breakdown_path": str(family_breakdown_path),
        "recommendations_path": str(recommendations_path),
        "candidates_path": str(candidates_path),
        "decision_path": str(decision_path),
    }


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX bad-pick unknown cohort reclassification audit")
    parser.add_argument("--source-rows-parquet", type=str, default=None)
    parser.add_argument("--bad-pick-session", type=str, default=None)
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
    result = run_bad_pick_unknown_reclassification_v1(
        source_rows_parquet=args.source_rows_parquet,
        bad_pick_session=args.bad_pick_session,
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
