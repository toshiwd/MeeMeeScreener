"""Inactive non-candle rank-window shadow adapter.

This module is pure and review-only. It computes adjusted ranks from
caller-provided rows, but it does not write runtime storage, mutate active
ranking output, or register production publish state.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "noncandle_rank_window_shadow_adapter_v1"
DEFAULT_CANDIDATE_ID = "cnt60up_rank_window_1_20_v1"
LIQUIDITY_CANDIDATE_ID = "liquidity20d_rank_window_1_20_v1"
MONTHLY_RANGE_CANDIDATE_ID = "monthlyRangeProb_lte_0.05_window_1_10"
ELIGIBLE_MIN_RANK = 1
ELIGIBLE_MAX_RANK = 20
MONTHLY_RANGE_ELIGIBLE_MAX_RANK = 10
CNT60_UP_MAX = 20.0
LIQUIDITY20D_MAX = 300_000.0
MONTHLY_RANGE_PROB_MAX = 0.05


def compute_cnt60up_rank_window_shadow_ranking(
    active_rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str = DEFAULT_CANDIDATE_ID,
    min_rank: int = ELIGIBLE_MIN_RANK,
    max_rank: int = ELIGIBLE_MAX_RANK,
    cnt60_up_max: float = CNT60_UP_MAX,
) -> dict[str, Any]:
    active_input = [dict(row) for row in active_rows]
    shadow_rows: list[dict[str, Any]] = []
    group_indexes: dict[tuple[str, str], list[int]] = defaultdict(list)

    for row in active_input:
        anchor_date = _text(row, "anchor_date", "as_of_date", "dt", "date")
        symbol = _text(row, "symbol", "code", "ticker")
        side = _normalize_side(_text(row, "side", "direction", "dir", default="long"))
        original_rank = _int(row, "rank", "champion_rank", "original_rank", "fresh_runtime_research_watch_rank")
        original_score = _float(row, "display_score", "champion_score", "score", "fresh_runtime_research_watch_score")
        cnt60_up = _optional_float(row.get("cnt60Up"))
        if cnt60_up is None:
            cnt60_up = _optional_float(row.get("cnt60_up"))
        if cnt60_up is None:
            cnt60_up = _optional_float(row.get("cnt_60_up"))

        gate_passed, reason, section = _cnt60up_decision(
            original_rank=original_rank,
            cnt60_up=cnt60_up,
            min_rank=min_rank,
            max_rank=max_rank,
            cnt60_up_max=cnt60_up_max,
        )
        record = {
            "schema_version": SCHEMA_VERSION,
            "source_candidate_id": candidate_id,
            "anchor_date": anchor_date,
            "symbol": symbol,
            "side": side,
            "active_rank": original_rank,
            "active_display_score": original_score,
            "original_rank": original_rank,
            "original_score": original_score,
            "cnt60Up": cnt60_up,
            "cnt60up_gate_passed": gate_passed,
            "shadow_adjusted_rank": None,
            "shadow_decision_reason": reason,
            "_shadow_sort_section": section,
        }
        group_indexes[(anchor_date, side)].append(len(shadow_rows))
        shadow_rows.append(record)

    for indexes in group_indexes.values():
        ordered = sorted(
            indexes,
            key=lambda idx: (
                int(shadow_rows[idx]["_shadow_sort_section"]),
                int(shadow_rows[idx]["original_rank"]),
                str(shadow_rows[idx]["symbol"]),
            ),
        )
        for adjusted_rank, idx in enumerate(ordered, start=1):
            shadow_rows[idx]["shadow_adjusted_rank"] = adjusted_rank

    for row in shadow_rows:
        row.pop("_shadow_sort_section", None)

    return {
        "schema_version": SCHEMA_VERSION,
        "integration_mode": "inactive_shadow_dry_run_only",
        "shadow_rows": shadow_rows,
        "summary": _build_summary(shadow_rows),
        "audit": _build_invariance_audit(active_input, shadow_rows),
    }


def compute_liquidity_rank_window_shadow_ranking(
    active_rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str = LIQUIDITY_CANDIDATE_ID,
    min_rank: int = ELIGIBLE_MIN_RANK,
    max_rank: int = ELIGIBLE_MAX_RANK,
    liquidity20d_max: float = LIQUIDITY20D_MAX,
) -> dict[str, Any]:
    active_input = [dict(row) for row in active_rows]
    shadow_rows: list[dict[str, Any]] = []
    group_indexes: dict[tuple[str, str], list[int]] = defaultdict(list)

    for row in active_input:
        anchor_date = _text(row, "anchor_date", "as_of_date", "dt", "date")
        symbol = _text(row, "symbol", "code", "ticker")
        side = _normalize_side(_text(row, "side", "direction", "dir", default="long"))
        original_rank = _int(row, "rank", "champion_rank", "original_rank", "fresh_runtime_research_watch_rank")
        original_score = _float(row, "display_score", "champion_score", "score", "fresh_runtime_research_watch_score")
        liquidity20d = _optional_float(row.get("liquidity20d"))
        if liquidity20d is None:
            liquidity20d = _optional_float(row.get("liquidity_20d"))
        if liquidity20d is None:
            liquidity20d = _optional_float(row.get("turnover20"))

        gate_passed, reason, section = _liquidity_decision(
            original_rank=original_rank,
            liquidity20d=liquidity20d,
            min_rank=min_rank,
            max_rank=max_rank,
            liquidity20d_max=liquidity20d_max,
        )
        record = {
            "schema_version": SCHEMA_VERSION,
            "source_candidate_id": candidate_id,
            "anchor_date": anchor_date,
            "symbol": symbol,
            "side": side,
            "active_rank": original_rank,
            "active_display_score": original_score,
            "original_rank": original_rank,
            "original_score": original_score,
            "liquidity20d": liquidity20d,
            "liquidity_gate_passed": gate_passed,
            "shadow_adjusted_rank": None,
            "shadow_decision_reason": reason,
            "_shadow_sort_section": section,
        }
        group_indexes[(anchor_date, side)].append(len(shadow_rows))
        shadow_rows.append(record)

    for indexes in group_indexes.values():
        ordered = sorted(
            indexes,
            key=lambda idx: (
                int(shadow_rows[idx]["_shadow_sort_section"]),
                int(shadow_rows[idx]["original_rank"]),
                str(shadow_rows[idx]["symbol"]),
            ),
        )
        for adjusted_rank, idx in enumerate(ordered, start=1):
            shadow_rows[idx]["shadow_adjusted_rank"] = adjusted_rank

    for row in shadow_rows:
        row.pop("_shadow_sort_section", None)

    return {
        "schema_version": SCHEMA_VERSION,
        "integration_mode": "inactive_shadow_dry_run_only",
        "shadow_rows": shadow_rows,
        "summary": _build_liquidity_summary(shadow_rows),
        "audit": _build_invariance_audit(active_input, shadow_rows),
    }


def compute_monthly_range_rank_window_shadow_ranking(
    active_rows: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str = MONTHLY_RANGE_CANDIDATE_ID,
    min_rank: int = ELIGIBLE_MIN_RANK,
    max_rank: int = MONTHLY_RANGE_ELIGIBLE_MAX_RANK,
    monthly_range_prob_max: float = MONTHLY_RANGE_PROB_MAX,
) -> dict[str, Any]:
    active_input = [dict(row) for row in active_rows]
    shadow_rows: list[dict[str, Any]] = []
    group_indexes: dict[tuple[str, str], list[int]] = defaultdict(list)

    for row in active_input:
        anchor_date = _text(row, "anchor_date", "as_of_date", "dt", "date")
        symbol = _text(row, "symbol", "code", "ticker")
        side = _normalize_side(_text(row, "side", "direction", "dir", default="long"))
        original_rank = _int(row, "rank", "champion_rank", "original_rank", "fresh_runtime_research_watch_rank")
        original_score = _float(row, "display_score", "champion_score", "score", "fresh_runtime_research_watch_score")
        monthly_range_prob = _optional_float(row.get("monthlyRangeProb"))
        if monthly_range_prob is None:
            monthly_range_prob = _optional_float(row.get("monthly_range_prob"))

        gate_passed, reason, section = _monthly_range_decision(
            original_rank=original_rank,
            monthly_range_prob=monthly_range_prob,
            min_rank=min_rank,
            max_rank=max_rank,
            monthly_range_prob_max=monthly_range_prob_max,
        )
        record = {
            "schema_version": SCHEMA_VERSION,
            "source_candidate_id": candidate_id,
            "anchor_date": anchor_date,
            "symbol": symbol,
            "side": side,
            "active_rank": original_rank,
            "active_display_score": original_score,
            "original_rank": original_rank,
            "original_score": original_score,
            "monthlyRangeProb": monthly_range_prob,
            "monthly_range_gate_passed": gate_passed,
            "shadow_adjusted_rank": None,
            "shadow_decision_reason": reason,
            "_shadow_sort_section": section,
        }
        group_indexes[(anchor_date, side)].append(len(shadow_rows))
        shadow_rows.append(record)

    for indexes in group_indexes.values():
        ordered = sorted(
            indexes,
            key=lambda idx: (
                int(shadow_rows[idx]["_shadow_sort_section"]),
                int(shadow_rows[idx]["original_rank"]),
                str(shadow_rows[idx]["symbol"]),
            ),
        )
        for adjusted_rank, idx in enumerate(ordered, start=1):
            shadow_rows[idx]["shadow_adjusted_rank"] = adjusted_rank

    for row in shadow_rows:
        row.pop("_shadow_sort_section", None)

    return {
        "schema_version": SCHEMA_VERSION,
        "integration_mode": "inactive_shadow_dry_run_only",
        "shadow_rows": shadow_rows,
        "summary": _build_monthly_range_summary(shadow_rows),
        "audit": _build_invariance_audit(active_input, shadow_rows),
    }


def _cnt60up_decision(
    *,
    original_rank: int,
    cnt60_up: float | None,
    min_rank: int,
    max_rank: int,
    cnt60_up_max: float,
) -> tuple[bool | None, str, int]:
    if original_rank < min_rank:
        return None, "outside_rank_window_before_no_change", 0
    if original_rank > max_rank:
        return None, "outside_rank_window_after_no_change", 3
    if cnt60_up is None:
        return None, "missing_cnt60up_no_silent_fallback", 2
    if cnt60_up <= cnt60_up_max:
        return True, "cnt60up_rank_window_pass", 1
    return False, "cnt60up_rank_window_demoted", 2


def _liquidity_decision(
    *,
    original_rank: int,
    liquidity20d: float | None,
    min_rank: int,
    max_rank: int,
    liquidity20d_max: float,
) -> tuple[bool | None, str, int]:
    if original_rank < min_rank:
        return None, "outside_rank_window_before_no_change", 0
    if original_rank > max_rank:
        return None, "outside_rank_window_after_no_change", 3
    if liquidity20d is None:
        return None, "missing_liquidity20d_no_silent_fallback", 2
    if liquidity20d <= liquidity20d_max:
        return True, "liquidity20d_rank_window_pass", 1
    return False, "liquidity20d_rank_window_demoted", 2


def _monthly_range_decision(
    *,
    original_rank: int,
    monthly_range_prob: float | None,
    min_rank: int,
    max_rank: int,
    monthly_range_prob_max: float,
) -> tuple[bool | None, str, int]:
    if original_rank < min_rank:
        return None, "outside_rank_window_before_no_change", 0
    if original_rank > max_rank:
        return None, "outside_rank_window_after_no_change", 3
    if monthly_range_prob is None:
        return None, "missing_monthly_range_prob_no_silent_fallback", 2
    if monthly_range_prob <= monthly_range_prob_max:
        return True, "monthly_range_prob_rank_window_pass", 1
    return False, "monthly_range_prob_rank_window_demoted", 2


def _build_summary(shadow_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    changed = sum(1 for row in shadow_rows if row.get("shadow_adjusted_rank") != row.get("original_rank"))
    missing = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "missing_cnt60up_no_silent_fallback")
    demoted = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "cnt60up_rank_window_demoted")
    passed = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "cnt60up_rank_window_pass")
    return {
        "row_count": len(shadow_rows),
        "changed_rank_count": changed,
        "cnt60up_pass_count": passed,
        "cnt60up_demoted_count": demoted,
        "missing_feature_row_count": missing,
    }


def _build_liquidity_summary(shadow_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    changed = sum(1 for row in shadow_rows if row.get("shadow_adjusted_rank") != row.get("original_rank"))
    missing = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "missing_liquidity20d_no_silent_fallback")
    demoted = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "liquidity20d_rank_window_demoted")
    passed = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "liquidity20d_rank_window_pass")
    return {
        "row_count": len(shadow_rows),
        "changed_rank_count": changed,
        "liquidity20d_pass_count": passed,
        "liquidity20d_demoted_count": demoted,
        "missing_feature_row_count": missing,
    }


def _build_monthly_range_summary(shadow_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    changed = sum(1 for row in shadow_rows if row.get("shadow_adjusted_rank") != row.get("original_rank"))
    missing = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "missing_monthly_range_prob_no_silent_fallback")
    demoted = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "monthly_range_prob_rank_window_demoted")
    passed = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "monthly_range_prob_rank_window_pass")
    return {
        "row_count": len(shadow_rows),
        "changed_rank_count": changed,
        "monthly_range_prob_pass_count": passed,
        "monthly_range_prob_demoted_count": demoted,
        "missing_feature_row_count": missing,
    }


def _build_invariance_audit(
    active_rows: Sequence[Mapping[str, Any]],
    shadow_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    active_rank_unchanged = True
    active_display_score_unchanged = True
    original_rank_recoverable = True
    adjusted_rank_separate = True
    for active, shadow in zip(active_rows, shadow_rows, strict=True):
        rank = _int(active, "rank", "champion_rank", "original_rank", "fresh_runtime_research_watch_rank")
        score = _float(active, "display_score", "champion_score", "score", "fresh_runtime_research_watch_score")
        active_rank_unchanged = active_rank_unchanged and shadow["active_rank"] == rank
        active_display_score_unchanged = active_display_score_unchanged and shadow["active_display_score"] == score
        original_rank_recoverable = original_rank_recoverable and shadow["original_rank"] == rank
        adjusted_rank_separate = adjusted_rank_separate and "shadow_adjusted_rank" in shadow and "rank" not in shadow
    return {
        "active_ranking_invariance_pass": active_rank_unchanged and active_display_score_unchanged,
        "active_rank_unchanged": active_rank_unchanged,
        "active_display_score_unchanged": active_display_score_unchanged,
        "original_rank_recoverable": original_rank_recoverable,
        "adjusted_rank_separate": adjusted_rank_separate,
        "runtime_duckdb_write_attempted": False,
        "production_registry_write_attempted": False,
    }


def _text(row: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _int(row: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return _int_value(value, 0)
    return 0


def _float(row: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return _float_value(value, 0.0)
    return 0.0


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _int_value(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_value(value: Any, default: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out == out else default


def _normalize_side(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"up", "buy", "long"}:
        return "long"
    if normalized in {"down", "sell", "short"}:
        return "short"
    return normalized
