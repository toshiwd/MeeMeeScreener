from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_NAME = "tradex_feature_surface_upgrade_plan_v1"
SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_input_resolution_v1"
FEATURE_INVENTORY_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_feature_surface_inventory_v1"
FEATURE_CANDIDATE_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_feature_candidate_design_v1"
BATCH1_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_batch1_recommendation_v1"
BUILD_PLAN_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_build_plan_v1"
VALIDATION_PLAN_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_validation_plan_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_surface_upgrade_plan_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_surface_upgrade_plan_v1")
DEFAULT_FEATURE_SURFACE = Path(
    r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646\candidate_prefilter_rows_context_enriched.parquet"
)
DEFAULT_UNKNOWN_SURFACE = Path(
    r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991\enriched_unknown_reclassification_rows.parquet"
)
DEFAULT_FREEZE_SESSION = Path(
    r"G:\Tradex\research_freeze_summaries\observable_regime_false_positive_require_confirmation\20260501T090354Z-098449"
)
DEFAULT_REBUILD_SESSION = Path(
    r"G:\Tradex\observable_regime_false_positive_bottom15_summary_rebuild_v1\20260501T085017Z-012155"
)
DEFAULT_RECLASSIFICATION_SESSION = Path(
    r"G:\Tradex\bad_pick_unknown_reclassification_enriched_v1\20260501T053110Z-7a584991"
)
DEFAULT_BACKFILL_SESSION = Path(r"G:\Tradex\audit_surface_context_backfill_v1\20260501T051248Z-eba42646")

FORBIDDEN_FUTURE_FIELDS = [
    "forward_ret_20d",
    "ret_5",
    "ret_10",
    "ret_20",
    "path_value_score_v1",
    "realized_pnl",
    "future_pnl",
]

EXISTING_USABLE_FIELDS = [
    "conditional_high_value",
    "stable_high_value_family",
    "monthly_context",
    "weekly_context",
    "monthly_context_no_lookahead",
    "weekly_context_no_lookahead",
    "monthly_main_state_ctx_backfilled",
    "weekly_main_state_ctx_backfilled",
    "daily_main_state_ctx_backfilled",
    "shape_classification",
    "family_classification",
    "family_regime_context",
    "dominant_regime_context",
    "market_regime",
    "market_risk_on",
    "market_risk_off",
    "market_breadth_adv_ratio",
    "market_breadth_sample_size",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "support_wick",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "bull_marubozu",
    "bear_marubozu",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "gap_pct",
    "liquidity20d",
    "monthly_range_pos",
    "monthly_range_prob",
    "monthly_range_width",
    "monthly_box_range_pct",
    "score",
]

SPARSE_FIELDS = [
    "vol_ratio5_20",
    "candle_shape_modifier",
    "market_regime_bucket",
    "regime_dependent_family",
]

DERIVABLE_FIELDS = [
    "entry_strength_score",
    "signal_quality_bucket",
    "decision_candle_quality",
    "false_breakout_risk",
    "volume_participation_bucket",
    "liquidity_quality_bucket",
    "higher_timeframe_headroom_bucket",
    "monthly_headroom_to_box_high",
    "weekly_headroom_to_recent_high",
    "upside_room_bucket",
    "extension_vs_atr_bucket",
    "gap_direction_bucket",
    "gap_fill_risk",
    "candle_reversal_pattern_flag",
    "inside_bar_flag",
    "engulfing_flag",
    "harami_flag",
    "consecutive_candle_quality",
    "trigger_confirmation_count",
    "entry_threshold_distance",
    "body_strength",
    "close_position_in_range_proxy",
    "upper_wick_risk",
    "lower_wick_support",
]

UPSTREAM_FIELDS = [
    "earnings_nearby_flag",
    "ex_rights_nearby_flag",
    "dividend_rights_nearby_flag",
    "dilution_event_flag",
    "major_event_window_flag",
]

FEATURE_GROUPS = [
    {
        "feature_name": "conditional_high_value_flag",
        "category": "entry_strength / signal_quality",
        "plain_language_meaning": "True when the same-day candidate sits inside the verified conditional-high-value multi-timeframe state family.",
        "required_source_fields": [
            "conditional_high_value",
            "monthly_context",
            "weekly_context",
            "daily_main_state_ctx_backfilled",
            "family_classification",
            "shape_classification",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "anchor_date exact same-day or earlier monthly/weekly context; no future alignment",
        "no_lookahead_proof": "Uses only backfilled same-day or earlier context surfaces and the verified conditional-high-value contract; no outcome fields are consulted.",
        "expected_usefulness": "High; this is the cleanest existing multi-timeframe confirmation anchor for the false-positive family.",
        "expected_false_positive_risk": "Low to medium; it may still overlap with added winners, but it is strongly no-lookahead-safe.",
        "expected_coverage": "high",
        "implementation_difficulty": "low",
        "availability": "add_now",
    },
    {
        "feature_name": "entry_strength_score",
        "category": "entry_strength / signal_quality",
        "plain_language_meaning": "A row-local composite strength score that summarizes confirmation quality without using future returns.",
        "required_source_fields": [
            "conditional_high_value_flag",
            "monthly_main_state_ctx_backfilled",
            "weekly_main_state_ctx_backfilled",
            "daily_main_state_ctx_backfilled",
            "decision_candle_quality",
            "higher_timeframe_headroom_bucket",
            "liquidity_quality_bucket",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "anchor_date row-local scoring only; all inputs must be same-day or earlier backfilled context",
        "no_lookahead_proof": "Composite is built only from same-day/backfilled context and candle/liquidity primitives; no forward outcome field is allowed.",
        "expected_usefulness": "High; provides a single audit-friendly summary for winner versus false-positive separation.",
        "expected_false_positive_risk": "Medium; too much smoothing would hide useful substructure, so the formula should stay simple.",
        "expected_coverage": "high",
        "implementation_difficulty": "medium",
        "availability": "add_now",
    },
    {
        "feature_name": "signal_quality_bucket",
        "category": "entry_strength / signal_quality",
        "plain_language_meaning": "A coarse bucket of entry strength that can be used in audit surfaces and human review.",
        "required_source_fields": [
            "entry_strength_score",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "same as entry_strength_score",
        "no_lookahead_proof": "Buckets only the derived entry_strength_score; the bucket boundary never uses future outcome fields.",
        "expected_usefulness": "High; discrete buckets are easier to compare across added winners and losers than a dense raw score.",
        "expected_false_positive_risk": "Low to medium; bucket edges should be stable enough for audit use.",
        "expected_coverage": "high",
        "implementation_difficulty": "low",
        "availability": "add_now",
    },
    {
        "feature_name": "decision_candle_quality",
        "category": "candle_confirmation",
        "plain_language_meaning": "A normalized candle-quality summary that distinguishes clean continuation shapes from weak or noisy reversal bars.",
        "required_source_fields": [
            "body_ratio",
            "upper_wick_ratio",
            "lower_wick_ratio",
            "support_wick",
            "candle_body_ratio",
            "candle_upper_wick_ratio",
            "candle_lower_wick_ratio",
            "candle_triplet_up_prob",
            "candle_triplet_down_prob",
            "bull_marubozu",
            "bear_marubozu",
            "candle_shape_modifier",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "row-local candle geometry only",
        "no_lookahead_proof": "All source fields are same-day candle primitives already present on the audit surface; no forward bars are consulted.",
        "expected_usefulness": "High; the current false-positive family overlaps in daily state, so more candle detail is a direct separator candidate.",
        "expected_false_positive_risk": "Medium; some candle modifiers are sparse and need explicit missing handling.",
        "expected_coverage": "high for primitives, sparse for modifier detail",
        "implementation_difficulty": "medium",
        "availability": "add_now",
    },
    {
        "feature_name": "false_breakout_risk",
        "category": "gap / reversal_risk",
        "plain_language_meaning": "A risk marker for gaps and long upper-wick reversals that frequently precede weak follow-through.",
        "required_source_fields": [
            "gap_pct",
            "upper_wick_ratio",
            "lower_wick_ratio",
            "candle_triplet_down_prob",
            "support_wick",
            "monthly_range_prob",
            "daily_main_state_ctx_backfilled",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "same-day only; no post-close or forward-return signals allowed",
        "no_lookahead_proof": "Constructed from same-day candle and context fields only.",
        "expected_usefulness": "Medium to high; it is a direct candidate for filtering the bottom15 false-positive subpattern.",
        "expected_false_positive_risk": "Medium; if overused it could reject valid continuation names.",
        "expected_coverage": "high",
        "implementation_difficulty": "medium",
        "availability": "add_now",
    },
    {
        "feature_name": "volume_participation_bucket",
        "category": "volume / participation",
        "plain_language_meaning": "A participation bucket that separates meaningful volume-backed candidates from thin or missing participation.",
        "required_source_fields": [
            "vol_ratio5_20",
            "liquidity20d",
            "market_breadth_adv_ratio",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "same-day or earlier liquidity snapshot only",
        "no_lookahead_proof": "Uses same-day liquidity and participation fields only; missingness is kept explicit instead of backfilled from future bars.",
        "expected_usefulness": "High if coverage is repaired; otherwise medium because sparse coverage limits immediate use.",
        "expected_false_positive_risk": "Low to medium; the signal itself is likely useful, but missingness must stay explicit.",
        "expected_coverage": "sparse until vol_ratio5_20 coverage improves",
        "implementation_difficulty": "medium",
        "availability": "add_now_with_explicit_missingness",
    },
    {
        "feature_name": "liquidity_quality_bucket",
        "category": "volume / participation",
        "plain_language_meaning": "A tradability bucket that groups candidates by liquidity and market participation quality.",
        "required_source_fields": [
            "liquidity20d",
            "market_breadth_adv_ratio",
            "market_breadth_sample_size",
            "market_risk_on",
            "market_risk_off",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "same-day only",
        "no_lookahead_proof": "All sources are point-in-time market state or liquidity snapshots already present on the enriched surface.",
        "expected_usefulness": "Medium to high; it can reduce thin-trading false positives without depending on sparse volume ratio coverage.",
        "expected_false_positive_risk": "Low; tradability information is usually stable and auditable.",
        "expected_coverage": "high",
        "implementation_difficulty": "low",
        "availability": "add_now",
    },
    {
        "feature_name": "higher_timeframe_headroom_bucket",
        "category": "higher_timeframe_headroom",
        "plain_language_meaning": "A bucket that says whether the name still has room to extend on the monthly / weekly surface or is already near exhaustion.",
        "required_source_fields": [
            "monthly_range_pos",
            "monthly_range_prob",
            "monthly_range_width",
            "monthly_box_range_pct",
            "dist_ma20_pct",
            "dist_ma60_pct",
            "monthly_main_state_ctx_backfilled",
            "weekly_main_state_ctx_backfilled",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "monthly and weekly values must be on-or-before the decision date, matching the backfill contract",
        "no_lookahead_proof": "Uses only backfilled monthly/weekly context and same-day MA-distance proxies; no future highs or outcomes are used.",
        "expected_usefulness": "High; the frozen false-positive family had broad overlap in state labels, so headroom may separate exhausted winners from exhausted losers.",
        "expected_false_positive_risk": "Medium; a coarse bucket is preferred over a continuous threshold until coverage is validated.",
        "expected_coverage": "high",
        "implementation_difficulty": "medium",
        "availability": "add_now",
    },
    {
        "feature_name": "monthly_headroom_to_box_high",
        "category": "higher_timeframe_headroom",
        "plain_language_meaning": "How much room the name has inside the monthly range box before it becomes overextended.",
        "required_source_fields": [
            "monthly_range_pos",
            "monthly_range_prob",
            "monthly_range_width",
            "monthly_box_range_pct",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "same-day or earlier monthly context only",
        "no_lookahead_proof": "Derived from monthly context only; no future prices or returns are used.",
        "expected_usefulness": "Medium to high; it is a precise monthly-context companion to the coarser headroom bucket.",
        "expected_false_positive_risk": "Medium; it should be interpreted as a proxy, not a hard rule.",
        "expected_coverage": "high",
        "implementation_difficulty": "low",
        "availability": "add_now",
    },
    {
        "feature_name": "weekly_headroom_to_recent_high",
        "category": "higher_timeframe_headroom",
        "plain_language_meaning": "A weekly-context headroom proxy that separates candidates with room to continue from those already extended.",
        "required_source_fields": [
            "weekly_main_state_ctx_backfilled",
            "dist_ma20_pct",
            "dist_ma60_pct",
            "shape_classification",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "same-day or earlier weekly context only",
        "no_lookahead_proof": "Uses weekly backfilled state and same-day MA-distance proxies; no future highs or outcome fields are used.",
        "expected_usefulness": "Medium; weaker than monthly headroom, but still useful for edge-case separation.",
        "expected_false_positive_risk": "Medium.",
        "expected_coverage": "high",
        "implementation_difficulty": "low",
        "availability": "add_now",
    },
    {
        "feature_name": "earnings_nearby_flag",
        "category": "event / earnings / rights",
        "plain_language_meaning": "Flag that the entry is near earnings or another event window that can distort follow-through.",
        "required_source_fields": [
            "earnings_flag",
            "major_event_window_flag",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "point-in-time only",
        "no_lookahead_proof": "Would be point-in-time safe if sourced from an upstream event calendar keyed on decision date; not inferred from future returns.",
        "expected_usefulness": "High, but unavailable in the current surface.",
        "expected_false_positive_risk": "Low if the upstream source is reliable.",
        "expected_coverage": "unavailable",
        "implementation_difficulty": "high",
        "availability": "needs_upstream_source",
    },
    {
        "feature_name": "dividend_rights_nearby_flag",
        "category": "event / earnings / rights",
        "plain_language_meaning": "Flag that captures dividend, rights, or dilution timing that can create false-positive breakouts.",
        "required_source_fields": [
            "dividend_flag",
            "rights_flag",
            "ex_rights_flag",
            "dilution_event_flag",
        ],
        "computation_grain": "candidate row",
        "date_alignment_rule": "point-in-time only",
        "no_lookahead_proof": "Would be point-in-time safe only if sourced from a calendar or corporate-action feed keyed on the decision date.",
        "expected_usefulness": "High, but unavailable in the current surface.",
        "expected_false_positive_risk": "Low if the upstream source is reliable.",
        "expected_coverage": "unavailable",
        "implementation_difficulty": "high",
        "availability": "needs_upstream_source",
    },
]

BASE_BUILD_FEATURES = [
    {
        "feature_name": "conditional_high_value_flag",
        "source": "candidate_prefilter_rows_context_enriched.parquet + conditional_high_value contract",
        "exact_computation": "carry the existing conditional_high_value boolean forward as an explicit surface feature and add a status column",
        "no_lookahead_rule": "same-day or earlier context only; the existing verified gate is already no-lookahead-safe",
        "expected_coverage": "1.0 on the candidate surface",
        "tests_required": [
            "row count preserved",
            "no-lookahead audit passes",
            "feature status is explicit for all rows",
        ],
        "why_first_batch": "This is the cleanest existing confirmation anchor and directly targets the false-positive family.",
    },
    {
        "feature_name": "decision_candle_quality",
        "source": "body_ratio, upper_wick_ratio, lower_wick_ratio, support_wick, candle_body_ratio, candle_upper_wick_ratio, candle_lower_wick_ratio, candle_triplet_up_prob, candle_triplet_down_prob, bull_marubozu, bear_marubozu, candle_shape_modifier",
        "exact_computation": "bucket the row-local candle anatomy into clean, weak, reversal-risk, and exhaustion-risk groups; keep missingness explicit when candle_shape_modifier is absent",
        "no_lookahead_rule": "same-day candle only; no forward bar, outcome, or path fields",
        "expected_coverage": "high for the primitive ratios; sparse for modifier detail",
        "tests_required": [
            "distribution of buckets is stable",
            "missingness is explicit for sparse modifier rows",
            "row counts are unchanged",
        ],
        "why_first_batch": "Candle detail is one of the few observables that can separate the added winners from added losers without touching ranking logic.",
    },
    {
        "feature_name": "higher_timeframe_headroom_bucket",
        "source": "monthly_range_pos, monthly_range_prob, monthly_range_width, monthly_box_range_pct, dist_ma20_pct, dist_ma60_pct, monthly_main_state_ctx_backfilled, weekly_main_state_ctx_backfilled",
        "exact_computation": "bucket monthly and weekly room into tight / normal / extended / exhausted bands using the backfilled context and distance proxies",
        "no_lookahead_rule": "monthly and weekly inputs must be on-or-before the decision date and sourced from the same backfill contract",
        "expected_coverage": "high",
        "tests_required": [
            "backfill no-lookahead contract preserved",
            "coverage flags emitted",
            "row counts preserved",
        ],
        "why_first_batch": "The frozen false-positive family overlaps in state labels, so headroom is the most direct way to reduce the overextended cases.",
    },
    {
        "feature_name": "liquidity_quality_bucket",
        "source": "liquidity20d, market_breadth_adv_ratio, market_breadth_sample_size, market_risk_on, market_risk_off",
        "exact_computation": "bucket tradability using liquidity and market participation quality; keep the bucket explicit even when market_regime_bucket is low entropy",
        "no_lookahead_rule": "same-day market and liquidity snapshots only",
        "expected_coverage": "high",
        "tests_required": [
            "coverage is measured on both candidate and unknown surfaces",
            "low-entropy market regime values are not mistaken for coverage failure",
            "missingness remains explicit",
        ],
        "why_first_batch": "The current surface already has liquidity coverage, so this can be added without new upstream dependencies.",
    },
    {
        "feature_name": "entry_strength_score",
        "source": "conditional_high_value_flag, decision_candle_quality, higher_timeframe_headroom_bucket, liquidity_quality_bucket, volume_participation_bucket",
        "exact_computation": "combine the batch-1 confirmation, candle, headroom, and liquidity buckets into a row-local composite score that does not reference future outcomes",
        "no_lookahead_rule": "composite must only consume same-day or earlier features that are already no-lookahead-safe",
        "expected_coverage": "high if volume is optional; sparse if volume is mandatory",
        "tests_required": [
            "composite stays row-local",
            "no future fields appear in the formula",
            "sample rows can be inspected without hidden fallback",
        ],
        "why_first_batch": "This is the highest-value summary field for later audit and human review.",
    },
    {
        "feature_name": "signal_quality_bucket",
        "source": "entry_strength_score",
        "exact_computation": "bucket the entry_strength_score into a small set of human-readable quality bands",
        "no_lookahead_rule": "bucket only the derived composite score; do not use future outcome labels to set bucket edges",
        "expected_coverage": "high",
        "tests_required": [
            "bucket edges are documented",
            "coverage mirrors entry_strength_score",
            "same rows can be traced back to the raw features",
        ],
        "why_first_batch": "Discrete buckets are easier to validate against the added top15 versus bottom15 splits.",
    },
    {
        "feature_name": "volume_participation_bucket",
        "source": "vol_ratio5_20, liquidity20d",
        "exact_computation": "bucket participation when vol_ratio5_20 is present; otherwise emit missing_reason explicitly and do not approximate",
        "no_lookahead_rule": "same-day participation only; no backfilling from future bars",
        "expected_coverage": "sparse until vol_ratio5_20 coverage is repaired",
        "tests_required": [
            "missingness is explicit",
            "coverage is reported separately from the other batch-1 fields",
            "no silent fallback is used for missing volume ratio",
        ],
        "why_first_batch": "Sparse, but directly aligned with the user's desired coverage repair for the participation signal.",
    },
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, set):
        return [_json_ready(v) for v in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if math.isnan(out):
            return None
        return out
    except Exception:
        return None


def _coverage(frame: pd.DataFrame, field: str) -> dict[str, Any]:
    if field not in frame.columns:
        return {
            "field_name": field,
            "present": False,
            "non_null_count": 0,
            "coverage": 0.0,
            "unique_non_null_count": 0,
        }
    series = frame[field]
    non_null = int(series.notna().sum())
    total = int(len(series))
    return {
        "field_name": field,
        "present": True,
        "non_null_count": non_null,
        "coverage": _safe_float(non_null / max(total, 1)),
        "unique_non_null_count": int(series.dropna().nunique()),
    }


def _classify_field(field: str, coverage: dict[str, Any], *, category_override: str | None = None) -> str:
    if field in FORBIDDEN_FUTURE_FIELDS:
        return "forbidden_future_outcome_fields"
    if category_override:
        return category_override
    if field in UPSTREAM_FIELDS:
        return "missing_and_requiring_new_upstream_source"
    if field in DERIVABLE_FIELDS:
        return "missing_but_derivable"
    if field in SPARSE_FIELDS:
        return "sparse_fields"
    if coverage.get("coverage") is None:
        return "missing_and_requiring_new_upstream_source"
    if float(coverage.get("coverage") or 0.0) < 0.9 or int(coverage.get("unique_non_null_count") or 0) <= 1:
        return "sparse_fields"
    return "existing_usable_fields"


def _field_row(
    field: str,
    candidate: pd.DataFrame,
    unknown: pd.DataFrame,
    *,
    category_override: str | None = None,
    notes: str = "",
) -> dict[str, Any]:
    cand = _coverage(candidate, field)
    unk = _coverage(unknown, field)
    category = _classify_field(field, cand, category_override=category_override)
    return {
        "field_name": field,
        "category": category,
        "candidate_coverage": cand["coverage"],
        "candidate_non_null_count": cand["non_null_count"],
        "candidate_unique_non_null_count": cand["unique_non_null_count"],
        "unknown_coverage": unk["coverage"],
        "unknown_non_null_count": unk["non_null_count"],
        "unknown_unique_non_null_count": unk["unique_non_null_count"],
        "notes": notes,
    }


def _build_feature_inventory(candidate: pd.DataFrame, unknown: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for field in EXISTING_USABLE_FIELDS:
        notes = "confirmed existing field on the enriched surface"
        if field == "market_regime_bucket":
            notes = "present but low-entropy on the current surfaces; treat as sparse / low-information until upstream coverage improves"
        rows.append(_field_row(field, candidate, unknown, notes=notes))
    for field in SPARSE_FIELDS:
        notes = "present but sparse or low-entropy on the current surfaces"
        rows.append(_field_row(field, candidate, unknown, category_override="sparse_fields", notes=notes))
    for field in DERIVABLE_FIELDS:
        notes = "missing as a direct column, but derivable from current same-day / backfilled surface fields without future outcomes"
        rows.append(_field_row(field, candidate, unknown, category_override="missing_but_derivable", notes=notes))
    for field in UPSTREAM_FIELDS:
        notes = "not found in the current audit surface; requires an upstream point-in-time source"
        rows.append(_field_row(field, candidate, unknown, category_override="missing_and_requiring_new_upstream_source", notes=notes))
    for field in FORBIDDEN_FUTURE_FIELDS:
        rows.append(
            _field_row(
                field,
                candidate,
                unknown,
                category_override="forbidden_future_outcome_fields",
                notes="forbidden as a feature input because it is post-decision or outcome-derived",
            )
        )
    rows = sorted(rows, key=lambda item: (item["category"], item["field_name"]))
    candidate_count = int(len(candidate))
    unknown_count = int(len(unknown))
    summary = {
        "candidate_row_count": candidate_count,
        "unknown_row_count": unknown_count,
        "existing_usable_field_count": int(sum(1 for row in rows if row["category"] == "existing_usable_fields")),
        "sparse_field_count": int(sum(1 for row in rows if row["category"] == "sparse_fields")),
        "missing_but_derivable_field_count": int(sum(1 for row in rows if row["category"] == "missing_but_derivable")),
        "missing_and_requiring_new_upstream_source_count": int(sum(1 for row in rows if row["category"] == "missing_and_requiring_new_upstream_source")),
        "forbidden_future_outcome_field_count": int(sum(1 for row in rows if row["category"] == "forbidden_future_outcome_fields")),
    }
    return {
        "schema_version": FEATURE_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "sources": {
            "candidate_surface": {
                "path": str(DEFAULT_FEATURE_SURFACE),
                "row_count": candidate_count,
            },
            "unknown_surface": {
                "path": str(DEFAULT_UNKNOWN_SURFACE),
                "row_count": unknown_count,
            },
        },
        "code_lineage": [
            {
                "path": str(REPO_ROOT / "scripts" / "tradex_bad_pick_root_cause_audit_v1.py"),
                "role": "same-day policy overlay extraction and backfill source",
            },
            {
                "path": str(REPO_ROOT / "scripts" / "tradex_audit_surface_context_backfill_v1.py"),
                "role": "join contract and no-lookahead backfill contract reference",
            },
            {
                "path": str(REPO_ROOT / "scripts" / "tradex_multi_timeframe_conditional_state_value_v1.py"),
                "role": "conditional-high-value semantics and multi-timeframe state family reference",
            },
            {
                "path": str(REPO_ROOT / "scripts" / "tradex_multi_timeframe_context_gated_high_value_boost_v1.py"),
                "role": "conditional-high-value gate semantics and state-family confirmation reference",
            },
            {
                "path": str(REPO_ROOT / "scripts" / "tradex_monthly_context_blind_test_proxy.py"),
                "role": "monthly range width / headroom proxy reference",
            },
        ],
        "field_inventory": rows,
        "summary": summary,
        "notes": [
            "current surfaces already expose enough same-day / backfilled state, candle, liquidity, and headroom fields for a first batch",
            "event / earnings / dividend / rights fields remain upstream-only and are not silently approximated",
            "forbidden future/outcome fields are explicitly excluded from feature construction",
        ],
    }


def _build_candidate_design(candidate: pd.DataFrame, unknown: pd.DataFrame) -> dict[str, Any]:
    inventory_lookup = {row["field_name"]: row for row in _build_feature_inventory(candidate, unknown)["field_inventory"]}
    features: list[dict[str, Any]] = []
    for item in FEATURE_GROUPS:
        required = item["required_source_fields"]
        availability = item["availability"]
        coverage_values = [inventory_lookup[field]["candidate_coverage"] for field in required if field in inventory_lookup]
        numeric_coverages = [value for value in coverage_values if value is not None]
        expected_coverage = None
        if numeric_coverages:
            expected_coverage = min(numeric_coverages)
        if availability == "needs_upstream_source":
            expected_coverage = "unavailable"
        feature = dict(item)
        feature["expected_coverage"] = expected_coverage if expected_coverage is not None else item["expected_coverage"]
        feature["required_source_status"] = [
            {
                "field_name": field,
                "availability": inventory_lookup[field]["category"] if field in inventory_lookup else "missing_and_requiring_new_upstream_source",
                "candidate_coverage": inventory_lookup[field]["candidate_coverage"] if field in inventory_lookup else 0.0,
            }
            for field in required
        ]
        features.append(feature)
    return {
        "schema_version": FEATURE_CANDIDATE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_count": int(len(features)),
        "feature_groups": {
            "entry_strength_signal_quality": [
                "conditional_high_value_flag",
                "entry_strength_score",
                "signal_quality_bucket",
            ],
            "volume_participation": [
                "volume_participation_bucket",
                "liquidity_quality_bucket",
            ],
            "candle_confirmation": [
                "decision_candle_quality",
                "false_breakout_risk",
            ],
            "higher_timeframe_headroom": [
                "higher_timeframe_headroom_bucket",
                "monthly_headroom_to_box_high",
                "weekly_headroom_to_recent_high",
            ],
            "upstream_batch2": [
                "earnings_nearby_flag",
                "dividend_rights_nearby_flag",
            ],
        },
        "features": features,
        "notes": [
            "batch-1 should stay small and be implemented only from existing no-lookahead-safe sources",
            "event / rights features are deliberately deferred because the surface does not currently expose a verified upstream source",
        ],
    }


def _build_batch1_recommendation(candidate_design: dict[str, Any]) -> dict[str, Any]:
    batch1_features = [
        item
        for item in candidate_design["features"]
        if item["feature_name"]
        in {
            "conditional_high_value_flag",
            "decision_candle_quality",
            "higher_timeframe_headroom_bucket",
            "liquidity_quality_bucket",
            "entry_strength_score",
            "signal_quality_bucket",
            "volume_participation_bucket",
        }
    ]
    selected_names = [item["feature_name"] for item in batch1_features if item["availability"] != "needs_upstream_source"]
    return {
        "schema_version": BATCH1_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision_target": "batch1_core_with_sparse_volume_extension",
        "selected_feature_names": selected_names,
        "batch1_core_features": [
            "conditional_high_value_flag",
            "decision_candle_quality",
            "higher_timeframe_headroom_bucket",
            "liquidity_quality_bucket",
            "entry_strength_score",
            "signal_quality_bucket",
        ],
        "batch1_sparse_extension_features": [
            "volume_participation_bucket",
        ],
        "deferred_batch2_features": [
            "earnings_nearby_flag",
            "ex_rights_nearby_flag",
            "dividend_rights_nearby_flag",
            "dilution_event_flag",
            "major_event_window_flag",
        ],
        "feature_details": batch1_features,
        "why_first_batch": [
            "the current surface already contains the multi-timeframe confirmation anchor that proved useful in the frozen require-confirmation line",
            "candle detail, headroom, and liquidity are all present with high coverage and are no-lookahead-safe",
            "volume participation is sparse but should still be surfaced with explicit missingness instead of being silently ignored",
        ],
        "tests_required": [
            "row count preservation on candidate and family slices",
            "no-lookahead audit using the backfill session contract",
            "explicit missing_reason columns for sparse volume features",
            "field-level coverage matrix before and after enrichment",
            "sample row inspection against known added top15 and bottom15 rows",
        ],
    }


def _build_build_plan(candidate_design: dict[str, Any], batch1: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": BUILD_PLAN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "target_surfaces": [
            "candidate_prefilter_rows_feature_enriched_v1.parquet",
            "observable_regime_false_positive_feature_enriched_v1.parquet",
        ],
        "row_preservation_contract": {
            "preserve_original_row_count": True,
            "preserve_original_columns": True,
            "append_new_feature_columns": True,
            "append_feature_status_columns": True,
            "append_missing_reason_columns": True,
            "no_silent_row_drops": True,
        },
        "build_steps": [
            {
                "step": 1,
                "name": "load_current_surfaces",
                "inputs": [str(DEFAULT_FEATURE_SURFACE), str(DEFAULT_UNKNOWN_SURFACE)],
            },
                {
                    "step": 2,
                    "name": "compute_batch1_feature_columns",
                    "features": batch1["selected_feature_names"],
                    "note": "batch1 core features plus sparse volume extension with explicit missingness",
                },
            {
                "step": 3,
                "name": "append_feature_status_contract",
                "status_columns": [
                    "conditional_high_value_flag_feature_status",
                    "decision_candle_quality_feature_status",
                    "higher_timeframe_headroom_bucket_feature_status",
                    "liquidity_quality_bucket_feature_status",
                    "entry_strength_score_feature_status",
                    "signal_quality_bucket_feature_status",
                    "volume_participation_bucket_feature_status",
                ],
            },
            {
                "step": 4,
                "name": "validate_no_lookahead_contract",
                "checks": [
                    "source date <= decision date for all joined context",
                    "future_outcome_fields are excluded",
                    "no_silent_fallback is false",
                ],
            },
            {
                "step": 5,
                "name": "write_enriched_surfaces",
                "outputs": [
                    "candidate_prefilter_rows_feature_enriched_v1.parquet",
                    "observable_regime_false_positive_feature_enriched_v1.parquet",
                ],
            },
        ],
        "implementation_notes": [
            "keep the event / rights fields as batch2 until an upstream point-in-time source is identified",
            "if volume ratio coverage is too sparse for keep-grade use, retain the field with explicit missingness and do not approximate",
            "do not change ranking or the current frozen policy lines while enriching the surface",
        ],
    }


def _build_validation_plan() -> dict[str, Any]:
    return {
        "schema_version": VALIDATION_PLAN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "validation_axes": [
            {
                "name": "row_count_reconciliation",
                "checks": [
                    "candidate surface row count preserved",
                    "focused family slice row count preserved when created",
                    "no silent drops or duplicate key explosions",
                ],
            },
            {
                "name": "no_lookahead_audit",
                "checks": [
                    "re-use the backfill session no_lookahead_context_audit.json as authoritative reference",
                    "confirm source dates are on or before decision date",
                    "confirm future_outcome_fields_used remains false",
                ],
            },
            {
                "name": "field_level_coverage",
                "checks": [
                    "candidate and unknown coverage per feature",
                    "sparse volume coverage reported separately",
                    "missing_reason columns emitted for all sparse features",
                ],
            },
            {
                "name": "before_after_missingness",
                "checks": [
                    "compare original surface missingness against enriched surface missingness",
                    "record rows that remain missing because no safe source exists",
                ],
            },
            {
                "name": "sample_row_inspection",
                "checks": [
                    "inspect a handful of known added top15 rows",
                    "inspect a handful of known added bottom15 rows",
                    "inspect unchanged rows to confirm no unintended drift",
                ],
            },
            {
                "name": "separation_check_against_known_deltas",
                "checks": [
                    "compare feature distributions on added_top15 vs added_bottom15 rows",
                    "compare feature distributions on enriched good picks vs false positives",
                    "do not train a model; keep this as a descriptive audit only",
                ],
            },
        ],
    }


def _build_decision(feature_inventory: dict[str, Any], batch1: dict[str, Any]) -> dict[str, Any]:
    feature_names = set(batch1["selected_feature_names"])
    ready_count = sum(1 for feature in batch1["feature_details"] if feature["availability"] != "needs_upstream_source")
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_implement_feature_surface_batch1",
        "status": "ready_to_implement_feature_surface_batch1",
        "reason": "existing_no_lookahead_safe_fields_support_a_small_high_value_batch_and_the_remaining_event_fields_can_wait_for_upstream_source_discovery",
        "batch1_ready": True,
        "batch1_feature_count": ready_count,
        "batch1_features": sorted(feature_names),
        "deferred_upstream_feature_count": int(sum(1 for feature in feature_inventory["field_inventory"] if feature["category"] == "missing_and_requiring_new_upstream_source")),
        "sparse_feature_caveat": "volume_participation_bucket is implementable now but must keep explicit missingness because vol_ratio5_20 coverage is sparse",
        "no_policy_or_ranking_changes": True,
    }


def _build_manifest(output_root: Path, session_dir: Path, candidate: Path, unknown: Path, freeze: Path, rebuild: Path, reclass: Path, backfill: Path) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "source_paths": {
            "feature_surface": str(candidate),
            "unknown_surface": str(unknown),
            "freeze_session": str(freeze),
            "bottom15_rebuild_session": str(rebuild),
            "reclassification_session": str(reclass),
            "backfill_session": str(backfill),
        },
    }


def _build_input_resolution(candidate: Path, unknown: Path, freeze: Path, rebuild: Path, reclass: Path, backfill: Path) -> dict[str, Any]:
    entries = []
    for label, path in [
        ("feature_surface", candidate),
        ("unknown_surface", unknown),
        ("freeze_session", freeze),
        ("bottom15_rebuild_session", rebuild),
        ("reclassification_session", reclass),
        ("backfill_session", backfill),
    ]:
        entries.append(
            {
                "label": label,
                "requested_path": str(path),
                "resolved_path": str(path.resolve()),
                "exists": path.exists(),
            }
        )
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "inputs": entries,
        "all_paths_exist": all(item["exists"] for item in entries),
        "notes": [
            "source lineage is anchored to the reconciled false-positive freeze and backfill sessions",
            "no path was silently rewritten; each required path is checked explicitly",
        ],
    }


def _write_artifacts(
    *,
    output_root: Path,
    candidate_surface: Path,
    unknown_surface: Path,
    freeze_session: Path,
    rebuild_session: Path,
    reclassification_session: Path,
    backfill_session: Path,
) -> dict[str, Any]:
    candidate = _load_frame(candidate_surface)
    unknown = _load_frame(unknown_surface)

    feature_inventory = _build_feature_inventory(candidate, unknown)
    candidate_design = _build_candidate_design(candidate, unknown)
    batch1 = _build_batch1_recommendation(candidate_design)
    build_plan = _build_build_plan(candidate_design, batch1)
    validation_plan = _build_validation_plan()
    decision = _build_decision(feature_inventory, batch1)
    input_resolution = _build_input_resolution(candidate_surface, unknown_surface, freeze_session, rebuild_session, reclassification_session, backfill_session)

    session_dir = output_root / _make_session_id()
    session_dir.mkdir(parents=True, exist_ok=False)

    _write_json(session_dir / "run_manifest.json", _build_manifest(output_root, session_dir, candidate_surface, unknown_surface, freeze_session, rebuild_session, reclassification_session, backfill_session))
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "feature_surface_inventory.json", feature_inventory)
    _write_json(session_dir / "feature_candidate_design.json", candidate_design)
    _write_json(session_dir / "feature_surface_batch1_recommendation.json", batch1)
    _write_json(session_dir / "feature_surface_build_plan.json", build_plan)
    _write_json(session_dir / "feature_surface_validation_plan.json", validation_plan)
    _write_json(session_dir / "feature_surface_upgrade_plan_v1_decision.json", decision)

    coverage_rows = feature_inventory["field_inventory"]
    coverage_frame = pd.DataFrame(coverage_rows)
    if not coverage_frame.empty:
        coverage_frame = coverage_frame[
            [
                "field_name",
                "category",
                "candidate_coverage",
                "candidate_non_null_count",
                "candidate_unique_non_null_count",
                "unknown_coverage",
                "unknown_non_null_count",
                "unknown_unique_non_null_count",
                "notes",
            ]
        ].copy()
        _write_parquet(session_dir / "field_coverage_matrix.parquet", coverage_frame)

    artifact_names = [
        "run_manifest.json",
        "input_resolution.json",
        "feature_surface_inventory.json",
        "feature_candidate_design.json",
        "feature_surface_batch1_recommendation.json",
        "feature_surface_build_plan.json",
        "feature_surface_validation_plan.json",
        "feature_surface_upgrade_plan_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    if (session_dir / "field_coverage_matrix.parquet").exists():
        artifact_names.append("field_coverage_matrix.parquet")
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_dir": str(session_dir),
            "artifact_count": len(artifact_names),
            "artifacts": artifact_names,
            "decision": decision["decision"],
        },
    )

    return {
        "output_dir": str(session_dir),
        "decision": decision,
        "feature_inventory": feature_inventory,
        "candidate_design": candidate_design,
        "batch1": batch1,
    }


def run_feature_surface_upgrade_plan_v1(
    *,
    output_root: str | Path | None = None,
    feature_surface: str | Path | None = None,
    unknown_surface: str | Path | None = None,
    freeze_session: str | Path | None = None,
    rebuild_session: str | Path | None = None,
    reclassification_session: str | Path | None = None,
    backfill_session: str | Path | None = None,
) -> dict[str, Any]:
    root = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    candidate = _safe_path(feature_surface, DEFAULT_FEATURE_SURFACE)
    unknown = _safe_path(unknown_surface, DEFAULT_UNKNOWN_SURFACE)
    freeze = _safe_path(freeze_session, DEFAULT_FREEZE_SESSION)
    rebuild = _safe_path(rebuild_session, DEFAULT_REBUILD_SESSION)
    reclass = _safe_path(reclassification_session, DEFAULT_RECLASSIFICATION_SESSION)
    backfill = _safe_path(backfill_session, DEFAULT_BACKFILL_SESSION)
    for label, path in [
        ("feature_surface", candidate),
        ("unknown_surface", unknown),
        ("freeze_session", freeze),
        ("bottom15_rebuild_session", rebuild),
        ("reclassification_session", reclass),
        ("backfill_session", backfill),
    ]:
        _ensure_exists(path, label)
    return _write_artifacts(
        output_root=root,
        candidate_surface=candidate,
        unknown_surface=unknown,
        freeze_session=freeze,
        rebuild_session=rebuild,
        reclassification_session=reclass,
        backfill_session=backfill,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write a TRADEX no-lookahead feature-surface upgrade plan.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--feature-surface", type=Path, default=DEFAULT_FEATURE_SURFACE)
    parser.add_argument("--unknown-surface", type=Path, default=DEFAULT_UNKNOWN_SURFACE)
    parser.add_argument("--freeze-session", type=Path, default=DEFAULT_FREEZE_SESSION)
    parser.add_argument("--rebuild-session", type=Path, default=DEFAULT_REBUILD_SESSION)
    parser.add_argument("--reclassification-session", type=Path, default=DEFAULT_RECLASSIFICATION_SESSION)
    parser.add_argument("--backfill-session", type=Path, default=DEFAULT_BACKFILL_SESSION)
    args = parser.parse_args(argv)
    run_feature_surface_upgrade_plan_v1(
        output_root=args.output_root,
        feature_surface=args.feature_surface,
        unknown_surface=args.unknown_surface,
        freeze_session=args.freeze_session,
        rebuild_session=args.rebuild_session,
        reclassification_session=args.reclassification_session,
        backfill_session=args.backfill_session,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
