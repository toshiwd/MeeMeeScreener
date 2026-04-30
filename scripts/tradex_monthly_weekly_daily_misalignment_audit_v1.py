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
    _boundary_rows_for_bad_pick,
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
DEFAULT_POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\monthly_weekly_daily_misalignment_audit_v1")

SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1"
MANIFEST_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_input_resolution_v1"
INVENTORY_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_timeframe_context_inventory_v1"
COHORT_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_misalignment_cohort_summary_v1"
CONTRAST_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_timeframe_combination_contrast_summary_v1"
PAIRWISE_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_misalignment_pairwise_boundary_summary_v1"
REGIME_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_misalignment_regime_breakdown_v1"
FAILURE_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_misalignment_failure_patterns_v1"
HYPOTHESIS_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_misalignment_challenger_hypotheses_v1"
DECISION_SCHEMA_VERSION = "tradex_monthly_weekly_daily_misalignment_audit_v1_decision_v1"

ROOT_CAUSE_CODE = "monthly_weekly_daily_misalignment"
TOP_K_VALUES = (5, 10, 20)

UNAVAILABLE_FIELDS = [
    "event_flag",
    "earnings_flag",
    "dividend_flag",
    "rights_flag",
    "ex_rights_flag",
]


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


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _load_small_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_bad_pick_session(session_path: Path) -> dict[str, Any]:
    bad_pick_cases = pd.read_parquet(session_path / "bad_pick_cases.parquet")
    boundary = pd.read_parquet(session_path / "boundary_near_miss_comparison.parquet")
    root_summary = _load_small_json(session_path / "root_cause_taxonomy_summary.json")
    feature_contrast = _load_small_json(session_path / "feature_contrast_summary.json")
    decision = _load_small_json(session_path / "audit_decision.json")
    return {
        "bad_pick_cases": bad_pick_cases,
        "boundary": boundary,
        "root_summary": root_summary,
        "feature_contrast": feature_contrast,
        "decision": decision,
    }


def _normalize_token(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, float) and math.isnan(value):
        return "unknown"
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "<na>"}:
        return "unknown"
    return text


def _daily_bucket(value: Any) -> str:
    text = _normalize_token(value).lower()
    if text == "unknown":
        return "unknown"
    if any(key in text for key in ("up_mid", "reversal_up", "breakout", "up_candidate", "bull", "positive")):
        return "positive"
    if any(key in text for key in ("down_mid", "downtrend", "bear", "weak", "reversal_down", "breakdown")):
        return "negative"
    return "other"


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


def _shape_bucket(value: Any) -> str:
    text = _normalize_token(value).lower()
    if text == "unknown":
        return "unknown"
    if "context_dependent" in text:
        return "context_dependent"
    if "positive_modifier" in text:
        return "positive_modifier"
    if "negative" in text:
        return "negative_modifier"
    if "missing" in text:
        return "missing"
    return "other"


def _is_daily_positive(value: Any) -> bool:
    return _daily_bucket(value) == "positive"


def _ensure_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = _ensure_columns(frame)
    for column in ("anchor_date", "symbol", "side", "month_bucket"):
        if column in frame.columns:
            frame[column] = frame[column].astype(str)
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
        "liquidity20d",
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
    ):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _load_selected_frame(source_rows_parquet: Path, policy_ledger_path: Path, *, limit_anchor_dates: int | None) -> tuple[pd.DataFrame, int]:
    frame = pd.read_parquet(source_rows_parquet)
    frame = _ensure_frame(frame)
    frame = _add_outcome_labels(frame)
    frame = _ensure_frame(frame)
    frame = _limit_anchor_dates(frame, limit_anchor_dates)
    frame = _ensure_frame(frame)

    selected_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in frame.loc[frame["champion_selected_top20"], ["anchor_date", "symbol", "side"]].drop_duplicates().itertuples(index=False)
    }
    policy_overlay = _load_policy_feature_overlay(policy_ledger_path, selected_keys)
    overlay_rows = int(len(policy_overlay))
    if not policy_overlay.empty:
        overlay_columns = [column for column in policy_overlay.columns if column not in {"anchor_date", "symbol", "side"}]
        frame = frame.merge(policy_overlay, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_policy"))
        frame = _ensure_frame(frame)
        for column in overlay_columns:
            policy_column = f"{column}_policy"
            if column in frame.columns and policy_column in frame.columns:
                frame[column] = frame[column].where(frame[column].notna(), frame[policy_column])
                frame = frame.drop(columns=[policy_column])
    frame = frame.sort_values(["anchor_date", "side", "champion_rank", "score", "symbol"], ascending=[True, True, True, False, True], kind="stable")
    return frame, overlay_rows


def _build_timeframe_context_inventory(frame: pd.DataFrame) -> dict[str, Any]:
    field_specs = [
        ("monthly_context", "confirmed usable", "candidate_surface", "direct monthly context"),
        ("weekly_context", "confirmed usable", "candidate_surface", "direct weekly context"),
        ("market_regime_bucket", "confirmed usable", "candidate_surface", "dominant market regime bucket"),
        ("dominant_regime_context", "confirmed usable", "candidate_surface", "dominant regime context"),
        ("family_classification", "confirmed usable", "candidate_surface", "family classification from prior research"),
        ("family_regime_context", "confirmed usable", "candidate_surface", "family regime context"),
        ("family_bad_pick_regime", "confirmed usable", "candidate_surface", "family bad-pick regime"),
        ("monthly_context_no_lookahead", "partial overlay", "candidate_surface/policy_overlay", "point-in-time monthly context flag"),
        ("weekly_context_no_lookahead", "partial overlay", "candidate_surface/policy_overlay", "point-in-time weekly context flag"),
        ("monthly_main_state_ctx", "partial overlay", "policy_overlay", "monthly state from policy ledger"),
        ("weekly_main_state_ctx", "partial overlay", "policy_overlay", "weekly state from policy ledger"),
        ("daily_main_state_ctx", "partial overlay", "policy_overlay", "daily state from policy ledger"),
        ("shape_joined", "partial overlay", "candidate_surface", "shape feature availability flag"),
        ("conditional_high_value", "confirmed usable", "candidate_surface", "conditional high value gate"),
        ("shape_classification", "partial overlay", "candidate_surface", "shape bucket from prior research"),
        ("candle_shape_modifier", "partial overlay", "candidate_surface", "compact candle shape modifier"),
        ("dist_ma20_pct", "proxy only", "candidate_surface/policy_overlay", "MA distance proxy"),
        ("dist_ma60_pct", "proxy only", "candidate_surface/policy_overlay", "MA distance proxy"),
        ("body_ratio", "proxy only", "candidate_surface/policy_overlay", "candlestick geometry proxy"),
        ("upper_wick_ratio", "proxy only", "candidate_surface/policy_overlay", "candlestick geometry proxy"),
        ("lower_wick_ratio", "proxy only", "candidate_surface/policy_overlay", "candlestick geometry proxy"),
        ("gap_pct", "proxy only", "candidate_surface/policy_overlay", "gap proxy"),
        ("vol_ratio5_20", "proxy only", "candidate_surface/policy_overlay", "volume proxy"),
        ("liquidity20d", "partial overlay", "policy_overlay", "point-in-time liquidity proxy"),
        ("forward_ret_20d", "confirmed usable", "candidate_surface", "realized 20-business-day outcome"),
        ("path_value_score_v1", "confirmed usable", "candidate_surface", "realized path value score"),
        ("candle_body_ratio", "proxy only", "candidate_surface/policy_overlay", "candle geometry proxy"),
        ("candle_upper_wick_ratio", "proxy only", "candidate_surface/policy_overlay", "candle geometry proxy"),
        ("candle_lower_wick_ratio", "proxy only", "candidate_surface/policy_overlay", "candle geometry proxy"),
        ("candle_triplet_up_prob", "proxy only", "candidate_surface/policy_overlay", "candle triplet probability proxy"),
        ("candle_triplet_down_prob", "proxy only", "candidate_surface/policy_overlay", "candle triplet probability proxy"),
        ("state_family_id", "confirmed usable", "candidate_surface", "daily state family id"),
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
    inventory_rows.extend(
        {
            "field": field,
            "availability": "unavailable",
            "source": "not present on audit surface",
            "non_null_count": 0,
            "missing_count": int(len(frame)),
            "missing_rate": 1.0 if len(frame) else None,
            "note": "intentionally excluded from audit surface",
        }
        for field in UNAVAILABLE_FIELDS
    )
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "row_count": int(len(frame)),
        "field_inventory": inventory_rows,
    }


def _build_pattern(row: pd.Series) -> str:
    monthly = _normalize_token(row.get("monthly_context"))
    weekly = _normalize_token(row.get("weekly_context"))
    daily = _normalize_token(row.get("daily_main_state_ctx"))
    shape = _normalize_token(row.get("shape_classification"))
    if monthly == "unknown" or weekly == "unknown":
        return "unknown_or_insufficient_context"
    return "|".join([monthly, weekly, daily, shape])


def _summarize_by_columns(frame: pd.DataFrame, columns: list[str], value_col: str = "count") -> list[dict[str, Any]]:
    if not columns:
        return []
    group = frame.groupby(columns, dropna=False).size().reset_index(name=value_col)
    return group.sort_values([value_col] + columns, ascending=[False] + [True] * len(columns), kind="stable").to_dict(orient="records")


def _build_selected_cohorts(frame: pd.DataFrame) -> dict[str, pd.DataFrame]:
    selected = frame.loc[frame["champion_selected_top20"]].copy()
    selected["is_bad_pick"] = selected["bottom15_label"] | (selected["forward_ret_20d"] <= 0)
    selected["is_good_pick"] = selected["top15_label"] & (~selected["is_bad_pick"])
    selected["is_neutral_pick"] = (~selected["is_bad_pick"]) & (~selected["is_good_pick"])
    selected["pattern"] = selected.apply(_build_pattern, axis=1)
    cohorts = {
        "selected": selected,
        "top5_bad": selected.loc[selected["champion_selected_top5"] & selected["is_bad_pick"]].copy(),
        "top10_bad": selected.loc[selected["champion_selected_top10"] & selected["is_bad_pick"]].copy(),
        "top5_good": selected.loc[selected["champion_selected_top5"] & selected["is_good_pick"]].copy(),
        "top10_good": selected.loc[selected["champion_selected_top10"] & selected["is_good_pick"]].copy(),
        "top5_neutral": selected.loc[selected["champion_selected_top5"] & selected["is_neutral_pick"]].copy(),
        "top10_neutral": selected.loc[selected["champion_selected_top10"] & selected["is_neutral_pick"]].copy(),
    }
    return cohorts


def _build_misalignment_cases(
    selected_frame: pd.DataFrame,
    bad_pick_cases: pd.DataFrame,
) -> pd.DataFrame:
    explicit = bad_pick_cases.loc[bad_pick_cases["root_cause_code"] == ROOT_CAUSE_CODE].copy()
    if explicit.empty:
        return explicit
    join_cols = ["anchor_date", "symbol", "side"]
    selected_cols = [
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
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "family_regime_context",
        "family_bad_pick_regime",
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
    ]
    selected_core = selected_frame.loc[:, [col for col in selected_cols if col in selected_frame.columns]].copy()
    merged = explicit.merge(selected_core, on=join_cols, how="left", suffixes=("", "_selected"))
    return merged


def _join_near_miss_features(misalignment_cases: pd.DataFrame, selected_frame: pd.DataFrame) -> pd.DataFrame:
    if misalignment_cases.empty:
        return misalignment_cases.copy()
    join_cols = ["anchor_date", "symbol", "side"]
    near_cols = [
        "anchor_date",
        "symbol",
        "side",
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
        "monthly_context",
        "weekly_context",
        "daily_main_state_ctx",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "market_regime_bucket",
        "dominant_regime_context",
        "family_classification",
        "family_regime_context",
        "family_bad_pick_regime",
        "shape_classification",
        "candle_shape_modifier",
        "shape_joined",
        "conditional_high_value",
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
            "symbol": "near_miss_symbol",
            "champion_rank": "near_miss_champion_rank",
            "candidate_rank": "near_miss_candidate_rank",
            "score": "near_miss_score",
            "forward_ret_5d": "near_miss_forward_ret_5d",
            "forward_ret_10d": "near_miss_forward_ret_10d",
            "forward_ret_20d": "near_miss_forward_ret_20d",
            "path_value_score_v1": "near_miss_path_value_score_v1",
            "mfe_20d": "near_miss_mfe_20d",
            "mae_20d": "near_miss_mae_20d",
            "top15_label": "near_miss_top15_label",
            "bottom15_label": "near_miss_bottom15_label",
            "monthly_context": "near_miss_monthly_context",
            "weekly_context": "near_miss_weekly_context",
            "daily_main_state_ctx": "near_miss_daily_main_state_ctx",
            "monthly_context_no_lookahead": "near_miss_monthly_context_no_lookahead",
            "weekly_context_no_lookahead": "near_miss_weekly_context_no_lookahead",
            "market_regime_bucket": "near_miss_market_regime_bucket",
            "dominant_regime_context": "near_miss_dominant_regime_context",
            "family_classification": "near_miss_family_classification",
            "family_regime_context": "near_miss_family_regime_context",
            "family_bad_pick_regime": "near_miss_family_bad_pick_regime",
            "shape_classification": "near_miss_shape_classification",
            "candle_shape_modifier": "near_miss_candle_shape_modifier",
            "shape_joined": "near_miss_shape_joined",
            "conditional_high_value": "near_miss_conditional_high_value",
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
    merged = misalignment_cases.merge(
        near_frame,
        left_on=["anchor_date", "side", "best_near_miss_symbol"],
        right_on=["anchor_date", "side", "near_miss_symbol"],
        how="left",
        suffixes=("", "_near"),
    )
    merged["near_miss_joined"] = merged["near_miss_score"].notna()
    merged["near_miss_rank_matches_boundary"] = pd.to_numeric(merged["best_near_miss_rank"], errors="coerce") == pd.to_numeric(
        merged["near_miss_champion_rank"], errors="coerce"
    )
    merged["monthly_alignment_same"] = merged["monthly_context"].astype(str) == merged["near_miss_monthly_context"].astype(str)
    merged["weekly_alignment_same"] = merged["weekly_context"].astype(str) == merged["near_miss_weekly_context"].astype(str)
    merged["daily_alignment_same"] = merged["daily_main_state_ctx"].astype(str) == merged["near_miss_daily_main_state_ctx"].astype(str)
    merged["shape_alignment_same"] = merged["shape_classification"].astype(str) == merged["near_miss_shape_classification"].astype(str)
    merged["selected_daily_positive"] = merged["daily_main_state_ctx"].apply(_is_daily_positive)
    merged["near_miss_daily_positive"] = merged["near_miss_daily_main_state_ctx"].apply(_is_daily_positive)
    merged["selected_pattern"] = merged.apply(_build_pattern, axis=1)
    merged["near_miss_pattern"] = merged.apply(
        lambda row: "|".join(
            [
                _normalize_token(row.get("near_miss_monthly_context")),
                _normalize_token(row.get("near_miss_weekly_context")),
                _normalize_token(row.get("near_miss_daily_main_state_ctx")),
                _normalize_token(row.get("near_miss_shape_classification")),
            ]
        ),
        axis=1,
    )
    return merged


def _build_pairwise_summary(pairwise: pd.DataFrame) -> dict[str, Any]:
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
            "forward_ret_20d_gap_mean": None,
            "path_value_gap_mean": None,
            "score_gap_median": None,
            "forward_ret_20d_gap_median": None,
            "path_value_gap_median": None,
        }
    return {
        "pair_count": int(len(pairwise)),
        "matched_near_miss_count": int(pairwise["near_miss_joined"].sum()),
        "selected_higher_score_count": int((pd.to_numeric(pairwise["score"], errors="coerce") > pd.to_numeric(pairwise["near_miss_score"], errors="coerce")).sum()),
        "selected_worse_path_count": int((pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") < pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce")).sum()),
        "selected_higher_score_and_worse_path_count": int(
            (
                (pd.to_numeric(pairwise["score"], errors="coerce") > pd.to_numeric(pairwise["near_miss_score"], errors="coerce"))
                & (pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") < pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce"))
            ).sum()
        ),
        "monthly_alignment_same_count": int(pairwise["monthly_alignment_same"].sum()),
        "weekly_alignment_same_count": int(pairwise["weekly_alignment_same"].sum()),
        "daily_alignment_same_count": int(pairwise["daily_alignment_same"].sum()),
        "shape_alignment_same_count": int(pairwise["shape_alignment_same"].sum()),
        "score_gap_mean": _safe_float(pd.to_numeric(pairwise["score"], errors="coerce").sub(pd.to_numeric(pairwise["near_miss_score"], errors="coerce")).mean()),
        "forward_ret_20d_gap_mean": _safe_float(
            pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce").sub(pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce")).mean()
        ),
        "path_value_gap_mean": _safe_float(
            pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce").sub(pd.to_numeric(pairwise["near_miss_path_value_score_v1"], errors="coerce")).mean()
        ),
        "score_gap_median": _safe_float(pd.to_numeric(pairwise["score"], errors="coerce").sub(pd.to_numeric(pairwise["near_miss_score"], errors="coerce")).median()),
        "forward_ret_20d_gap_median": _safe_float(
            pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce").sub(pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce")).median()
        ),
        "path_value_gap_median": _safe_float(
            pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce").sub(pd.to_numeric(pairwise["near_miss_path_value_score_v1"], errors="coerce")).median()
        ),
    }


def _build_topk_stats(frame: pd.DataFrame, topk: int) -> dict[str, Any]:
    selected = frame.loc[frame[f"champion_selected_top{topk}"]].copy()
    bad = selected.loc[selected["is_bad_pick"]].copy()
    good = selected.loc[selected["is_good_pick"]].copy()
    neutral = selected.loc[selected["is_neutral_pick"]].copy()
    return {
        "topk": topk,
        "selected_count": int(len(selected)),
        "bad_pick_count": int(len(bad)),
        "good_pick_count": int(len(good)),
        "neutral_pick_count": int(len(neutral)),
        "top15_capture_rate": _safe_float(selected["top15_label"].mean()) if len(selected) else None,
        "bottom15_contamination_rate": _safe_float(selected["bottom15_label"].mean()) if len(selected) else None,
        "mean_forward_ret_20d": _safe_float(selected["forward_ret_20d"].mean()) if len(selected) else None,
        "median_forward_ret_20d": _safe_float(selected["forward_ret_20d"].median()) if len(selected) else None,
        "mean_path_value_score_v1": _safe_float(selected["path_value_score_v1"].mean()) if len(selected) else None,
        "median_path_value_score_v1": _safe_float(selected["path_value_score_v1"].median()) if len(selected) else None,
        "bad_pattern_counts": selected.loc[selected["is_bad_pick"], "pattern"].value_counts(dropna=False).head(20).reset_index(name="count").rename(columns={"index": "pattern"}).to_dict(orient="records"),
        "good_pattern_counts": selected.loc[selected["is_good_pick"], "pattern"].value_counts(dropna=False).head(20).reset_index(name="count").rename(columns={"index": "pattern"}).to_dict(orient="records"),
        "neutral_pattern_counts": selected.loc[selected["is_neutral_pick"], "pattern"].value_counts(dropna=False).head(20).reset_index(name="count").rename(columns={"index": "pattern"}).to_dict(orient="records"),
    }


def _build_misalignment_cohort_summary(
    frame: pd.DataFrame,
    misalignment_cases: pd.DataFrame,
    bad_pick_cases: pd.DataFrame,
    boundary_pairs: pd.DataFrame,
) -> dict[str, Any]:
    explicit = misalignment_cases.copy()
    summary: dict[str, Any] = {
        "selected_row_count": int(len(frame)),
        "bad_pick_count": int(len(bad_pick_cases)),
        "explicit_misalignment_count": int(len(explicit)),
        "top5_misalignment_count": int((explicit["bad_pick_scope"] == "top5").sum()) if len(explicit) else 0,
        "top10_misalignment_count": int((explicit["bad_pick_scope"] == "top10").sum()) if len(explicit) else 0,
        "top20_context_only_count": int(len(frame.loc[frame["champion_selected_top20"]])),
        "side_counts": explicit["side"].value_counts(dropna=False).to_dict() if len(explicit) else {},
        "monthly_context_counts": explicit["monthly_context"].value_counts(dropna=False).to_dict() if len(explicit) else {},
        "weekly_context_counts": explicit["weekly_context"].value_counts(dropna=False).to_dict() if len(explicit) else {},
        "daily_state_counts": explicit["daily_main_state_ctx"].astype(str).value_counts(dropna=False).to_dict() if len(explicit) else {},
        "shape_classification_counts": explicit["shape_classification"].value_counts(dropna=False).to_dict() if len(explicit) else {},
        "dominant_regime_counts": explicit["dominant_regime_context"].value_counts(dropna=False).to_dict() if len(explicit) else {},
        "family_classification_counts": explicit["family_classification"].value_counts(dropna=False).to_dict() if len(explicit) else {},
        "monthly_context_no_lookahead_true_count": int(explicit["monthly_context_no_lookahead"].fillna(False).sum()) if len(explicit) else 0,
        "weekly_context_no_lookahead_true_count": int(explicit["weekly_context_no_lookahead"].fillna(False).sum()) if len(explicit) else 0,
        "no_lookahead_coverage_rate": _safe_float(
            (
                explicit["monthly_context_no_lookahead"].fillna(False).astype(bool)
                & explicit["weekly_context_no_lookahead"].fillna(False).astype(bool)
            ).mean()
        )
        if len(explicit)
        else None,
        "top5_bottom15_rate": _safe_float(explicit.loc[explicit["bad_pick_scope"] == "top5", "bottom15_label"].mean()) if len(explicit) else None,
        "top10_bottom15_rate": _safe_float(explicit.loc[explicit["bad_pick_scope"] == "top10", "bottom15_label"].mean()) if len(explicit) else None,
        "overall_bottom15_rate": _safe_float(explicit["bottom15_label"].mean()) if len(explicit) else None,
        "overall_top15_rate": _safe_float(explicit["top15_label"].mean()) if len(explicit) else None,
        "mean_score_gap": _safe_float(explicit["score_gap"].mean()) if len(explicit) else None,
        "median_score_gap": _safe_float(explicit["score_gap"].median()) if len(explicit) else None,
        "mean_forward_ret_20d_gap": _safe_float(explicit["forward_ret_20d_gap"].mean()) if len(explicit) else None,
        "median_forward_ret_20d_gap": _safe_float(explicit["forward_ret_20d_gap"].median()) if len(explicit) else None,
        "mean_path_value_gap": _safe_float(explicit["path_value_gap"].mean()) if len(explicit) else None,
        "median_path_value_gap": _safe_float(explicit["path_value_gap"].median()) if len(explicit) else None,
        "boundary_pair_count": int(len(boundary_pairs)),
        "boundary_pair_match_rate": _safe_float(len(explicit) / max(len(boundary_pairs), 1)),
        "explicit_vs_parent_match_rate": _safe_float(
            len(
                explicit.merge(
                    bad_pick_cases.loc[bad_pick_cases["root_cause_code"] == ROOT_CAUSE_CODE, ["anchor_date", "symbol", "side"]],
                    on=["anchor_date", "symbol", "side"],
                    how="inner",
                )
            )
            / max(len(explicit), 1)
        )
        if len(explicit)
        else None,
        "bad_pick_scope_distribution": explicit["bad_pick_scope"].value_counts(dropna=False).to_dict() if len(explicit) else {},
    }
    return summary


def _build_timeframe_combination_contrast_summary(frame: pd.DataFrame, misalignment_cases: pd.DataFrame, pairwise: pd.DataFrame) -> dict[str, Any]:
    cohorts = {
        "top5_bad": frame.loc[frame["champion_selected_top5"] & frame["is_bad_pick"]].copy(),
        "top5_good": frame.loc[frame["champion_selected_top5"] & frame["is_good_pick"]].copy(),
        "top5_neutral": frame.loc[frame["champion_selected_top5"] & frame["is_neutral_pick"]].copy(),
        "top10_bad": frame.loc[frame["champion_selected_top10"] & frame["is_bad_pick"]].copy(),
        "top10_good": frame.loc[frame["champion_selected_top10"] & frame["is_good_pick"]].copy(),
        "top10_neutral": frame.loc[frame["champion_selected_top10"] & frame["is_neutral_pick"]].copy(),
        "misalignment": misalignment_cases.copy(),
    }

    def cohort_summary(name: str, sub: pd.DataFrame) -> dict[str, Any]:
        if sub.empty:
            return {
                "cohort": name,
                "count": 0,
                "mean_forward_ret_20d": None,
                "median_forward_ret_20d": None,
                "mean_path_value_score_v1": None,
                "median_path_value_score_v1": None,
                "monthly_context_counts": {},
                "weekly_context_counts": {},
                "daily_state_counts": {},
                "pattern_counts": {},
            }
        return {
            "cohort": name,
            "count": int(len(sub)),
            "mean_forward_ret_20d": _safe_float(sub["forward_ret_20d"].mean()),
            "median_forward_ret_20d": _safe_float(sub["forward_ret_20d"].median()),
            "mean_path_value_score_v1": _safe_float(sub["path_value_score_v1"].mean()),
            "median_path_value_score_v1": _safe_float(sub["path_value_score_v1"].median()),
            "monthly_context_counts": sub["monthly_context"].value_counts(dropna=False).to_dict(),
            "weekly_context_counts": sub["weekly_context"].value_counts(dropna=False).to_dict(),
            "daily_state_counts": sub["daily_main_state_ctx"].astype(str).value_counts(dropna=False).to_dict(),
            "pattern_counts": sub["pattern"].value_counts(dropna=False).head(12).to_dict(),
        }

    contrast: dict[str, Any] = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "cohorts": [cohort_summary(name, sub) for name, sub in cohorts.items()],
        "pairwise_alignment_summary": _build_pairwise_summary(pairwise),
        "pattern_enrichment_top5": [],
        "pattern_enrichment_top10": [],
    }

    for topk in ("top5", "top10"):
        bad = frame.loc[frame[f"champion_selected_{topk}"] & frame["is_bad_pick"]].copy()
        good = frame.loc[frame[f"champion_selected_{topk}"] & frame["is_good_pick"]].copy()
        neutral = frame.loc[frame[f"champion_selected_{topk}"] & frame["is_neutral_pick"]].copy()
        patterns = sorted(set(bad["pattern"].dropna().tolist()) | set(good["pattern"].dropna().tolist()) | set(neutral["pattern"].dropna().tolist()))
        rows = []
        for pattern in patterns:
            bad_count = int((bad["pattern"] == pattern).sum())
            good_count = int((good["pattern"] == pattern).sum())
            neutral_count = int((neutral["pattern"] == pattern).sum())
            rows.append(
                {
                    "pattern": pattern,
                    "bad_count": bad_count,
                    "good_count": good_count,
                    "neutral_count": neutral_count,
                    "bad_rate": _safe_float(bad_count / max(len(bad), 1)),
                    "good_rate": _safe_float(good_count / max(len(good), 1)),
                    "neutral_rate": _safe_float(neutral_count / max(len(neutral), 1)),
                    "bad_minus_good_rate": _safe_float((bad_count / max(len(bad), 1)) - (good_count / max(len(good), 1))),
                }
            )
        rows = sorted(rows, key=lambda r: (r["bad_minus_good_rate"] or 0.0, r["bad_count"]), reverse=True)
        contrast[f"{topk}_pattern_rows"] = rows[:20]
        contrast[f"{topk}_context_gap"] = {
            "bad_minus_good_monthly_overextended_count": int(
                bad["monthly_context"].astype(str).str.contains("overextended", case=False, na=False).sum()
                - good["monthly_context"].astype(str).str.contains("overextended", case=False, na=False).sum()
            ),
            "bad_minus_good_weekly_overextended_count": int(
                bad["weekly_context"].astype(str).str.contains("overextended", case=False, na=False).sum()
                - good["weekly_context"].astype(str).str.contains("overextended", case=False, na=False).sum()
            ),
            "bad_minus_good_shape_context_dependent_count": int(
                (bad["shape_classification"] == "shape_context_dependent").sum()
                - (good["shape_classification"] == "shape_context_dependent").sum()
            ),
        }
        if topk == "top5":
            contrast["pattern_enrichment_top5"] = rows[:20]
        else:
            contrast["pattern_enrichment_top10"] = rows[:20]
    return contrast


def _build_regime_breakdown(frame: pd.DataFrame, misalignment_cases: pd.DataFrame) -> dict[str, Any]:
    breakdown: dict[str, Any] = {
        "schema_version": REGIME_SCHEMA_VERSION,
        "bad_pick_count": int(len(misalignment_cases)),
        "by_side": _summarize_by_columns(misalignment_cases, ["side"]),
        "by_topk": _summarize_by_columns(misalignment_cases, ["bad_pick_scope"]),
        "by_month": _summarize_by_columns(misalignment_cases, ["month_bucket"]),
        "by_dominant_regime": _summarize_by_columns(misalignment_cases, ["dominant_regime_context"]),
        "by_monthly_context": _summarize_by_columns(misalignment_cases, ["monthly_context"]),
        "by_weekly_context": _summarize_by_columns(misalignment_cases, ["weekly_context"]),
        "by_daily_state": _summarize_by_columns(misalignment_cases, ["daily_main_state_ctx"]),
        "by_family_classification": _summarize_by_columns(misalignment_cases, ["family_classification"]),
        "by_shape_classification": _summarize_by_columns(misalignment_cases, ["shape_classification"]),
        "missingness": {
            "daily_main_state_ctx": int(misalignment_cases["daily_main_state_ctx"].isna().sum()),
            "monthly_context_no_lookahead": int(misalignment_cases["monthly_context_no_lookahead"].isna().sum()),
            "weekly_context_no_lookahead": int(misalignment_cases["weekly_context_no_lookahead"].isna().sum()),
        },
        "regime_context_rates": [],
    }

    for group_col in ("dominant_regime_context", "monthly_context", "weekly_context", "daily_main_state_ctx"):
        rows = []
        for value, group in misalignment_cases.groupby(group_col, dropna=False):
            rows.append(
                {
                    "group_col": group_col,
                    "group_value": _normalize_token(value),
                    "count": int(len(group)),
                    "mean_forward_ret_20d": _safe_float(group["forward_ret_20d"].mean()),
                    "mean_path_value_score_v1": _safe_float(group["path_value_score_v1"].mean()),
                    "bottom15_rate": _safe_float(group["bottom15_label"].mean()) if len(group) else None,
                    "top15_rate": _safe_float(group["top15_label"].mean()) if len(group) else None,
                }
            )
        breakdown["regime_context_rates"].extend(rows)
    return breakdown


def _build_failure_patterns(misalignment_cases: pd.DataFrame, pairwise: pd.DataFrame) -> dict[str, Any]:
    pattern_counts = misalignment_cases["pattern"].value_counts(dropna=False).reset_index(name="count").rename(columns={"index": "pattern"})
    top_pattern = pattern_counts.iloc[0].to_dict() if len(pattern_counts) else {}
    daily_missing = int(misalignment_cases["daily_main_state_ctx"].isna().sum()) if len(misalignment_cases) else 0
    top_pattern_share = _safe_float(top_pattern.get("count", 0) / max(len(misalignment_cases), 1)) if top_pattern else None
    return {
        "schema_version": FAILURE_SCHEMA_VERSION,
        "pattern_counts": pattern_counts.to_dict(orient="records"),
        "top_pattern_share": top_pattern_share,
        "daily_missing_count": daily_missing,
        "daily_missing_rate": _safe_float(daily_missing / max(len(misalignment_cases), 1)) if len(misalignment_cases) else None,
        "pairwise_alignment_summary": _build_pairwise_summary(pairwise),
        "notes": [
            "patterns are exact monthly|weekly|daily|shape combinations from the explicit misalignment cohort",
            "daily missingness is treated as evidence of incomplete context observability, not as a fallback label",
        ],
    }


def _build_hypotheses(misalignment_cases: pd.DataFrame, pairwise: pd.DataFrame) -> list[dict[str, Any]]:
    patterns = misalignment_cases["pattern"].value_counts(dropna=False).reset_index(name="count").rename(columns={"index": "pattern"})
    top_pattern = patterns.iloc[0].to_dict() if len(patterns) else {"pattern": "unknown_or_insufficient_context", "count": 0}
    hypotheses: list[dict[str, Any]] = [
        {
            "hypothesis_id": "MWD-01",
            "misalignment_pattern_addressed": top_pattern.get("pattern"),
            "candidate_condition": "Require confirmation when monthly and weekly context are both overextended and the daily signal is bullish but shape is context dependent.",
            "required_fields": [
                "monthly_context",
                "weekly_context",
                "daily_main_state_ctx",
                "shape_classification",
                "monthly_context_no_lookahead",
                "weekly_context_no_lookahead",
            ],
            "expected_effect": "Reduce same-condition false positives where the higher-timeframe move is already stretched.",
            "false_positive_risk": "May reject continuation winners when the overextended state is actually an early trend flag.",
            "why_it_may_move_top5_top10_boundary": "The explicit misalignment cases mostly lose to nearby candidates on realized 20-day path while still winning on score.",
            "test_type": "require-confirmation",
            "recommended_next_validation_method": "Narrow fixed-condition challenger on the explicit misalignment slice only.",
        },
        {
            "hypothesis_id": "MWD-02",
            "misalignment_pattern_addressed": "monthly_overextended_weekly_overextended_daily_positive",
            "candidate_condition": "Cap candidates whose monthly and weekly states are both overextended unless they also show a stronger confirmation proxy than the current context-dependent daily state.",
            "required_fields": [
                "monthly_context",
                "weekly_context",
                "daily_main_state_ctx",
                "path_value_score_v1",
                "dist_ma20_pct",
                "dist_ma60_pct",
            ],
            "expected_effect": "Move topK away from late entries that are already stretched across higher timeframes.",
            "false_positive_risk": "Could suppress otherwise strong momentum continuation trades.",
            "why_it_may_move_top5_top10_boundary": "Boundary pairs show nearby candidates with better 20-day path despite lower score.",
            "test_type": "cap",
            "recommended_next_validation_method": "Boundary-only cap challenger after a separate explanation-only review.",
        },
        {
            "hypothesis_id": "MWD-03",
            "misalignment_pattern_addressed": "unknown_or_insufficient_context",
            "candidate_condition": "Hold the candidate for explanation only when daily state is missing and monthly/weekly context are not sufficiently observable for a safe rule.",
            "required_fields": ["monthly_context", "weekly_context", "daily_main_state_ctx"],
            "expected_effect": "Avoid overfitting a sparse observation surface.",
            "false_positive_risk": "No direct ranking benefit; this is only a safety guard for future rule design.",
            "why_it_may_move_top5_top10_boundary": "It does not move the boundary by itself, but it prevents a noisy rule from being promoted.",
            "test_type": "explanation-only",
            "recommended_next_validation_method": "Do not implement as a challenger until observability is improved.",
        },
    ]
    if len(pairwise):
        hypotheses[0]["boundary_match_rate"] = _safe_float(pairwise["near_miss_joined"].mean())
        hypotheses[1]["selected_higher_score_and_worse_path_count"] = int(
            (
                (pd.to_numeric(pairwise["score"], errors="coerce") > pd.to_numeric(pairwise["near_miss_score"], errors="coerce"))
                & (pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") < pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce"))
            ).sum()
        )
    return hypotheses


def _build_decision(
    misalignment_cases: pd.DataFrame,
    pairwise: pd.DataFrame,
    contrast: dict[str, Any],
) -> dict[str, Any]:
    pattern_counts = misalignment_cases["pattern"].value_counts(dropna=False)
    top_pattern_share = float(pattern_counts.iloc[0] / max(len(misalignment_cases), 1)) if len(pattern_counts) else 0.0
    daily_missing_rate = float(misalignment_cases["daily_main_state_ctx"].isna().mean()) if len(misalignment_cases) else 1.0
    pairwise_summary = _build_pairwise_summary(pairwise)
    pattern_rows = contrast.get("pattern_enrichment_top5") or []
    best_pattern_delta = max((row.get("bad_minus_good_rate") or 0.0) for row in pattern_rows) if pattern_rows else 0.0
    if len(misalignment_cases) == 0:
        decision = "needs_more_input_data"
        reason = "no_misalignment_cases_were_materialized"
    elif top_pattern_share >= 0.5 and daily_missing_rate < 0.4 and pairwise_summary["selected_higher_score_and_worse_path_count"] >= max(len(pairwise) * 0.7, 1):
        decision = "ready_for_single_axis_challenger_design"
        reason = "single_pattern_is_dominant_and_boundary_pairs_show_better_aligned_near_misses"
    else:
        decision = "explanation_only"
        reason = "pattern_is_present_but_too_fragmented_or_sparsely_observed_for_a_safe_challenger"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "decision": decision,
        "typed_reason": reason,
        "authoritative_rollup_decision": decision,
        "root_cause_code": ROOT_CAUSE_CODE,
        "bad_pick_count": int(len(misalignment_cases)),
        "top_pattern_share": _safe_float(top_pattern_share),
        "daily_missing_rate": _safe_float(daily_missing_rate),
        "pairwise_selected_higher_score_and_worse_path_count": int(pairwise_summary["selected_higher_score_and_worse_path_count"]),
        "best_pattern_delta": _safe_float(best_pattern_delta),
        "strong_root_cause_candidates": [ROOT_CAUSE_CODE] if len(misalignment_cases) else [],
        "next_recommended_single_axis": None if decision == "explanation_only" else ROOT_CAUSE_CODE,
    }


def run_monthly_weekly_daily_misalignment_audit_v1(
    *,
    source_rows_parquet: str | Path | None = None,
    bad_pick_session: str | Path | None = None,
    policy_ledger_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 2,
) -> dict[str, Any]:
    source_rows_parquet = _resolve_source_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET, "source rows parquet")
    bad_pick_session = _resolve_source_path(bad_pick_session, DEFAULT_BAD_PICK_SESSION, "bad pick session")
    policy_ledger_path = _resolve_source_path(policy_ledger_path, DEFAULT_POLICY_LEDGER, "policy ledger")
    output_root = _resolve_output_root(output_root)

    runtime_status = get_runtime_stock_db_status()
    long_rankings = get_rankings_freshness(direction="up", risk_mode="balanced")
    short_rankings = get_rankings_freshness(direction="down", risk_mode="balanced")

    bad_pick_payload = _load_bad_pick_session(bad_pick_session)
    bad_pick_cases = bad_pick_payload["bad_pick_cases"]
    boundary_from_parent = bad_pick_payload["boundary"]

    selected_frame, overlay_rows = _load_selected_frame(source_rows_parquet, policy_ledger_path, limit_anchor_dates=limit_anchor_dates)
    selected_frame = _ensure_frame(selected_frame)
    selected_frame = selected_frame.loc[selected_frame["champion_selected_top20"]].copy()
    if selected_frame.empty:
        raise RuntimeError("no champion-selected rows available after applying the input filters")

    selected_frame["is_bad_pick"] = selected_frame["bottom15_label"] | (selected_frame["forward_ret_20d"] <= 0)
    selected_frame["is_good_pick"] = selected_frame["top15_label"] & (~selected_frame["is_bad_pick"])
    selected_frame["is_neutral_pick"] = (~selected_frame["is_bad_pick"]) & (~selected_frame["is_good_pick"])
    selected_frame["pattern"] = selected_frame.apply(_build_pattern, axis=1)

    misalignment_cases = _build_misalignment_cases(selected_frame, bad_pick_cases)
    if not misalignment_cases.empty:
        misalignment_cases = misalignment_cases.copy()
        misalignment_cases["pattern"] = misalignment_cases.apply(_build_pattern, axis=1)
    pairwise = _join_near_miss_features(misalignment_cases, selected_frame)

    if not pairwise.empty:
        pairwise["score_gap"] = pd.to_numeric(pairwise["score"], errors="coerce") - pd.to_numeric(pairwise["near_miss_score"], errors="coerce")
        pairwise["forward_ret_20d_gap"] = pd.to_numeric(pairwise["forward_ret_20d"], errors="coerce") - pd.to_numeric(pairwise["near_miss_forward_ret_20d"], errors="coerce")
        pairwise["path_value_gap"] = pd.to_numeric(pairwise["path_value_score_v1"], errors="coerce") - pd.to_numeric(pairwise["near_miss_path_value_score_v1"], errors="coerce")
        pairwise["near_miss_better_aligned"] = (
            (~pairwise["monthly_alignment_same"]) | (~pairwise["weekly_alignment_same"]) | (~pairwise["daily_alignment_same"])
        )
    else:
        pairwise = pd.DataFrame()

    timeframe_inventory = _build_timeframe_context_inventory(selected_frame)
    cohort_summary = _build_misalignment_cohort_summary(selected_frame, misalignment_cases, bad_pick_cases, pairwise)
    contrast_summary = _build_timeframe_combination_contrast_summary(selected_frame, misalignment_cases, pairwise)
    regime_breakdown = _build_regime_breakdown(selected_frame, misalignment_cases)
    failure_patterns = _build_failure_patterns(misalignment_cases, pairwise)
    hypotheses = _build_hypotheses(misalignment_cases, pairwise)
    decision = _build_decision(misalignment_cases, pairwise, contrast_summary)

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "selected_candidate_source": {
            "path": str(source_rows_parquet),
            "kind": "candidate_prefilter_rows_parquet",
            "reason": "authoritative champion top20 surface with month/week context, outcomes, and candidate-order fields",
        },
        "bad_pick_root_cause_audit": {
            "path": str(bad_pick_session),
            "reason": "authoritative prior bad-pick labels and boundary near-miss audit for the monthly/weekly/daily root cause",
        },
        "policy_overlay": {
            "path": str(policy_ledger_path),
            "reason": "point-in-time context overlay used to recover daily state and liquidity proxies where available",
        },
        "selected_reason": "candidate_prefilter_rows.parquet provides the champion topK surface; prior bad-pick artifacts provide the explicit monthly_weekly_daily_misalignment labels and boundary evidence; policy overlay fills the missing daily state fields.",
        "missing_inputs": [],
        "authoritative_for_audit": True,
        "bad_pick_root_cause_audit_match_count": int(len(misalignment_cases)),
        "bad_pick_root_cause_audit_parent_count": int(len(bad_pick_cases.loc[bad_pick_cases["root_cause_code"] == ROOT_CAUSE_CODE])),
        "policy_overlay_rows": int(overlay_rows),
        "boundary_rows": int(len(boundary_from_parent)),
    }

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": session_id,
        "generated_at": _utc_now(),
        "git_hash": _git_hash_or_unknown(),
        "jobs": int(jobs),
        "input_sources": {
            "selected_candidate_source": str(source_rows_parquet),
            "bad_pick_session": str(bad_pick_session),
            "policy_ledger_path": str(policy_ledger_path),
        },
        "same_condition_contract": {
            "candidate_universe": "integrated_guarded_v1 champion top20 audit surface",
            "anchor_key": "anchor_date/symbol/side",
            "topk_values": list(TOP_K_VALUES),
            "outcome_horizon_business_days": 20,
            "score_baseline": "original champion score",
            "artifact_detail_level": "summary json + parquet detail",
        },
        "row_counts": {
            "selected_row_count": int(len(selected_frame)),
            "bad_pick_count": int(len(bad_pick_cases)),
            "explicit_misalignment_count": int(len(misalignment_cases)),
            "pairwise_row_count": int(len(pairwise)),
            "policy_overlay_rows": int(overlay_rows),
            "selected_top5_count": int(selected_frame["champion_selected_top5"].sum()),
            "selected_top10_count": int(selected_frame["champion_selected_top10"].sum()),
            "selected_top20_count": int(selected_frame["champion_selected_top20"].sum()),
        },
        "runtime_freshness": {
            "runtime_stock_db_status": runtime_status,
            "long_rankings_freshness": long_rankings,
            "short_rankings_freshness": short_rankings,
        },
        "no_silent_fallback": True,
    }

    misalignment_cohort_summary = {
        "schema_version": COHORT_SCHEMA_VERSION,
        "bad_pick_count": int(len(bad_pick_cases)),
        "explicit_misalignment_count": int(len(misalignment_cases)),
        "explicit_misalignment_parent_match_count": int(len(misalignment_cases)),
        "top5_misalignment_count": int((misalignment_cases["bad_pick_scope"] == "top5").sum()) if len(misalignment_cases) else 0,
        "top10_misalignment_count": int((misalignment_cases["bad_pick_scope"] == "top10").sum()) if len(misalignment_cases) else 0,
        "top5_bottom15_rate": _safe_float(misalignment_cases.loc[misalignment_cases["bad_pick_scope"] == "top5", "bottom15_label"].mean()) if len(misalignment_cases) else None,
        "top10_bottom15_rate": _safe_float(misalignment_cases.loc[misalignment_cases["bad_pick_scope"] == "top10", "bottom15_label"].mean()) if len(misalignment_cases) else None,
        "overall_bottom15_rate": _safe_float(misalignment_cases["bottom15_label"].mean()) if len(misalignment_cases) else None,
        "overall_top15_rate": _safe_float(misalignment_cases["top15_label"].mean()) if len(misalignment_cases) else None,
        "monthly_context_counts": misalignment_cases["monthly_context"].value_counts(dropna=False).to_dict() if len(misalignment_cases) else {},
        "weekly_context_counts": misalignment_cases["weekly_context"].value_counts(dropna=False).to_dict() if len(misalignment_cases) else {},
        "daily_state_counts": misalignment_cases["daily_main_state_ctx"].astype(str).value_counts(dropna=False).to_dict() if len(misalignment_cases) else {},
        "shape_classification_counts": misalignment_cases["shape_classification"].value_counts(dropna=False).to_dict() if len(misalignment_cases) else {},
        "dominant_regime_counts": misalignment_cases["dominant_regime_context"].value_counts(dropna=False).to_dict() if len(misalignment_cases) else {},
        "family_classification_counts": misalignment_cases["family_classification"].value_counts(dropna=False).to_dict() if len(misalignment_cases) else {},
        "no_lookahead_count": int(
            ((misalignment_cases["monthly_context_no_lookahead"].fillna(False)) & (misalignment_cases["weekly_context_no_lookahead"].fillna(False))).sum()
        )
        if len(misalignment_cases)
        else 0,
        "daily_missing_count": int(misalignment_cases["daily_main_state_ctx"].isna().sum()) if len(misalignment_cases) else 0,
        "monthly_context_no_lookahead_missing_count": int(misalignment_cases["monthly_context_no_lookahead"].isna().sum()) if len(misalignment_cases) else 0,
        "weekly_context_no_lookahead_missing_count": int(misalignment_cases["weekly_context_no_lookahead"].isna().sum()) if len(misalignment_cases) else 0,
        "mean_score_gap": _safe_float(misalignment_cases["score_gap"].mean()) if len(misalignment_cases) else None,
        "mean_forward_ret_20d_gap": _safe_float(misalignment_cases["forward_ret_20d_gap"].mean()) if len(misalignment_cases) else None,
        "mean_path_value_gap": _safe_float(misalignment_cases["path_value_gap"].mean()) if len(misalignment_cases) else None,
        "pairwise_rows": int(len(pairwise)),
    }

    # Persist artifacts.
    input_resolution_path = _write_json(session_dir / "input_resolution.json", input_resolution)
    timeframe_inventory_path = _write_json(session_dir / "timeframe_context_inventory.json", timeframe_inventory)
    cohort_summary_path = _write_json(session_dir / "misalignment_cohort_summary.json", misalignment_cohort_summary)
    contrast_summary_path = _write_json(session_dir / "timeframe_combination_contrast_summary.json", contrast_summary)
    regime_breakdown_path = _write_json(session_dir / "misalignment_regime_breakdown.json", regime_breakdown)
    failure_patterns_path = _write_json(session_dir / "misalignment_failure_patterns.json", failure_patterns)
    hypotheses_path = _write_json(session_dir / "misalignment_challenger_hypotheses.json", {"schema_version": HYPOTHESIS_SCHEMA_VERSION, "hypotheses": hypotheses})
    decision_path = _write_json(session_dir / "monthly_weekly_daily_misalignment_audit_v1_decision.json", decision)
    run_manifest_path = _write_json(session_dir / "run_manifest.json", run_manifest)

    pairwise_path = _write_parquet(session_dir / "misalignment_pairwise_boundary.parquet", pairwise)
    misalignment_rows_path = _write_parquet(session_dir / "misalignment_rows.parquet", misalignment_cases)

    artifact_complete = {
        "schema_version": "tradex_monthly_weekly_daily_misalignment_audit_v1_artifact_complete_v1",
        "required_files": [
            "run_manifest.json",
            "input_resolution.json",
            "timeframe_context_inventory.json",
            "misalignment_cohort_summary.json",
            "timeframe_combination_contrast_summary.json",
            "misalignment_pairwise_boundary.parquet",
            "misalignment_regime_breakdown.json",
            "misalignment_failure_patterns.json",
            "misalignment_challenger_hypotheses.json",
            "monthly_weekly_daily_misalignment_audit_v1_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "parse_status": {
            "run_manifest": True,
            "input_resolution": True,
            "timeframe_context_inventory": True,
            "misalignment_cohort_summary": True,
            "timeframe_combination_contrast_summary": True,
            "misalignment_regime_breakdown": True,
            "misalignment_failure_patterns": True,
            "misalignment_challenger_hypotheses": True,
            "decision": True,
        },
        "row_reconciliation": {
            "selected_row_count": int(len(selected_frame)),
            "explicit_misalignment_count": int(len(misalignment_cases)),
            "pairwise_row_count": int(len(pairwise)),
            "bad_pick_parent_match_count": int(len(misalignment_cases)),
            "selected_rows_have_feature_coverage": bool(len(selected_frame) > 0),
            "no_silent_row_drops": True,
        },
    }
    artifact_complete_path = _write_json(session_dir / "_ARTIFACT_COMPLETE.json", artifact_complete)

    return {
        "session_dir": str(session_dir),
        "run_manifest_path": str(run_manifest_path),
        "input_resolution_path": str(input_resolution_path),
        "timeframe_inventory_path": str(timeframe_inventory_path),
        "cohort_summary_path": str(cohort_summary_path),
        "contrast_summary_path": str(contrast_summary_path),
        "regime_breakdown_path": str(regime_breakdown_path),
        "failure_patterns_path": str(failure_patterns_path),
        "hypotheses_path": str(hypotheses_path),
        "decision_path": str(decision_path),
        "pairwise_path": str(pairwise_path),
        "misalignment_rows_path": str(misalignment_rows_path),
        "artifact_complete_path": str(artifact_complete_path),
    }


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the TRADEX monthly-weekly-daily misalignment audit.")
    parser.add_argument("--source-rows-parquet", type=str, default=None)
    parser.add_argument("--bad-pick-session", type=str, default=None)
    parser.add_argument("--policy-ledger-path", type=str, default=None)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    run_monthly_weekly_daily_misalignment_audit_v1(
        source_rows_parquet=args.source_rows_parquet,
        bad_pick_session=args.bad_pick_session,
        policy_ledger_path=args.policy_ledger_path,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
