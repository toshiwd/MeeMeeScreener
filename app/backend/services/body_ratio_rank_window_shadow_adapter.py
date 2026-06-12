"""Inactive body-ratio rank-window shadow adapter.

This module is pure and review-only. It computes adjusted ranks from
caller-provided rows, but it does not write runtime storage, mutate active
ranking output, or register production publish state.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_PLAN_ROOT = Path(
    r"G:\Tradex\shadow_integration_plans\body_ratio_rank_window_6_20_v1"
    r"\20260607T080536Z-body-ratio-rank-window-shadow-integration-plan-v1"
)

SCHEMA_VERSION = "body_ratio_rank_window_shadow_adapter_v1"
DEFAULT_CANDIDATE_ID = "body_ratio_rank_window_6_20_v1"
ELIGIBLE_MIN_RANK = 6
ELIGIBLE_MAX_RANK = 20
BODY_RATIO_MIN = 0.30


class BodyRatioRankWindowPlanError(ValueError):
    """Raised when the reviewed dry-run plan is missing or not approved."""


@dataclass(frozen=True)
class BodyRatioRankWindowPlan:
    plan_root: Path
    shadow_integration_plan: Mapping[str, Any]
    feature_materialization_plan: Mapping[str, Any]
    rank_storage_contract: Mapping[str, Any]
    rollback_plan: Mapping[str, Any]
    artifact_complete: Mapping[str, Any]

    @property
    def source_candidate_id(self) -> str:
        return str(self.shadow_integration_plan.get("candidate_id") or DEFAULT_CANDIDATE_ID)

    @property
    def eligible_min_rank(self) -> int:
        return _int_value(
            ((self.shadow_integration_plan.get("adapter_contract") or {}).get("rank_window") or [ELIGIBLE_MIN_RANK])[0],
            ELIGIBLE_MIN_RANK,
        )

    @property
    def eligible_max_rank(self) -> int:
        return _int_value(
            ((self.shadow_integration_plan.get("adapter_contract") or {}).get("rank_window") or [None, ELIGIBLE_MAX_RANK])[1],
            ELIGIBLE_MAX_RANK,
        )

    @property
    def body_ratio_min(self) -> float:
        return _float_value(
            (self.shadow_integration_plan.get("adapter_contract") or {}).get("body_ratio_min"),
            BODY_RATIO_MIN,
        )


def load_body_ratio_rank_window_plan(
    plan_root: str | Path = DEFAULT_PLAN_ROOT,
) -> BodyRatioRankWindowPlan:
    root = Path(plan_root)
    plan = _read_json(root / "shadow_integration_plan.json")
    feature_plan = _read_json(root / "feature_materialization_plan.json")
    rank_contract = _read_json(root / "rank_storage_contract.json")
    rollback_plan = _read_json(root / "rollback_plan.json")
    artifact_complete = _read_json(root / "_ARTIFACT_COMPLETE.json")

    issues: list[str] = []
    if plan.get("decision") != "approve_shadow_dry_run_implementation_plan":
        issues.append("shadow_dry_run_plan_not_approved")
    if plan.get("active_runtime_selection_change") != "not_allowed":
        issues.append("active_runtime_selection_change_not_forbidden")
    if plan.get("production_registry_change") != "not_allowed":
        issues.append("production_registry_change_not_forbidden")
    if plan.get("runtime_db_write") != "not_allowed":
        issues.append("runtime_db_write_not_forbidden")
    if feature_plan.get("decision") != "ready_for_dry_run_verification":
        issues.append("feature_materialization_not_ready_for_dry_run")
    record_shape = rank_contract.get("record_shape") or {}
    if record_shape.get("adjusted_rank_is_separate") is not True:
        issues.append("adjusted_rank_not_separate")
    if (rank_contract.get("persistence_contract") or {}).get("runtime_db_write") is not False:
        issues.append("rank_storage_runtime_db_write_not_false")
    if rollback_plan.get("runtime_db_rollback_required") is not False:
        issues.append("runtime_db_rollback_required_not_false")
    if artifact_complete.get("complete") is not True:
        issues.append("plan_artifact_not_complete")
    if issues:
        raise BodyRatioRankWindowPlanError(";".join(issues))

    return BodyRatioRankWindowPlan(
        plan_root=root,
        shadow_integration_plan=plan,
        feature_materialization_plan=feature_plan,
        rank_storage_contract=rank_contract,
        rollback_plan=rollback_plan,
        artifact_complete=artifact_complete,
    )


def compute_body_ratio_rank_window_shadow_ranking(
    active_rows: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]] | Mapping[tuple[str, str, str], Mapping[str, Any]] = (),
    plan: BodyRatioRankWindowPlan | None = None,
) -> dict[str, Any]:
    active_input = [dict(row) for row in active_rows]
    feature_lookup = _build_feature_lookup(feature_rows)
    effective_plan = plan or load_body_ratio_rank_window_plan()

    shadow_rows: list[dict[str, Any]] = []
    group_indexes: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in active_input:
        anchor_date = _text(row, "anchor_date", "as_of_date", "dt", "date")
        symbol = _text(row, "symbol", "code", "ticker")
        side = _normalize_side(_text(row, "side", "direction", "dir", default="long"))
        original_rank = _int(row, "rank", "champion_rank", "original_rank", "fresh_runtime_research_watch_rank")
        original_score = _float(row, "display_score", "champion_score", "score", "fresh_runtime_research_watch_score")
        feature = _find_feature(feature_lookup, anchor_date, symbol, side)
        body_ratio = _optional_float(row.get("body_ratio"))
        if body_ratio is None:
            body_ratio = _optional_float(row.get("candleBodyRatio"))
        if body_ratio is None:
            body_ratio = _optional_float(row.get("candle_body_ratio"))
        if body_ratio is None and feature:
            body_ratio = _optional_float(feature.get("body_ratio"))
        if body_ratio is None and feature:
            body_ratio = _optional_float(feature.get("candleBodyRatio"))
        if body_ratio is None and feature:
            body_ratio = _optional_float(feature.get("candle_body_ratio"))

        gate_passed, reason, section = _body_ratio_decision(
            original_rank=original_rank,
            body_ratio=body_ratio,
            min_rank=effective_plan.eligible_min_rank,
            max_rank=effective_plan.eligible_max_rank,
            body_ratio_min=effective_plan.body_ratio_min,
        )
        record = {
            "schema_version": SCHEMA_VERSION,
            "source_candidate_id": effective_plan.source_candidate_id,
            "anchor_date": anchor_date,
            "symbol": symbol,
            "side": side,
            "active_rank": original_rank,
            "active_display_score": original_score,
            "original_rank": original_rank,
            "original_score": original_score,
            "body_ratio": body_ratio,
            "body_ratio_gate_passed": gate_passed,
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
        "plan_root": str(effective_plan.plan_root),
        "shadow_rows": shadow_rows,
        "summary": _build_summary(shadow_rows),
        "audit": _build_invariance_audit(active_input, shadow_rows),
    }


def _body_ratio_decision(
    *,
    original_rank: int,
    body_ratio: float | None,
    min_rank: int,
    max_rank: int,
    body_ratio_min: float,
) -> tuple[bool | None, str, int]:
    if original_rank < min_rank:
        return None, "outside_rank_window_before_no_change", 0
    if original_rank > max_rank:
        return None, "outside_rank_window_after_no_change", 3
    if body_ratio is None:
        return None, "missing_body_ratio_no_silent_fallback", 2
    if body_ratio >= body_ratio_min:
        return True, "body_ratio_rank_window_pass", 1
    return False, "body_ratio_rank_window_demoted", 2


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise BodyRatioRankWindowPlanError(f"missing_required_plan_artifact:{path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise BodyRatioRankWindowPlanError(f"plan_artifact_not_object:{path}")
    return payload


def _build_feature_lookup(
    feature_rows: Sequence[Mapping[str, Any]] | Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    if isinstance(feature_rows, Mapping):
        return {(_key_text(k[0]), _key_text(k[1]), _normalize_side(_key_text(k[2]))): v for k, v in feature_rows.items()}

    lookup: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in feature_rows:
        anchor_date = _text(row, "anchor_date", "as_of_date", "dt", "date")
        symbol = _text(row, "symbol", "code", "ticker")
        side = _normalize_side(_text(row, "side", "direction", "dir", default="long"))
        lookup[(anchor_date, symbol, side)] = row
        lookup.setdefault((anchor_date, symbol, ""), row)
    return lookup


def _find_feature(
    feature_lookup: Mapping[tuple[str, str, str], Mapping[str, Any]],
    anchor_date: str,
    symbol: str,
    side: str,
) -> Mapping[str, Any] | None:
    return feature_lookup.get((anchor_date, symbol, side)) or feature_lookup.get((anchor_date, symbol, ""))


def _build_summary(shadow_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    changed = sum(1 for row in shadow_rows if row.get("shadow_adjusted_rank") != row.get("original_rank"))
    missing = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "missing_body_ratio_no_silent_fallback")
    demoted = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "body_ratio_rank_window_demoted")
    passed = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "body_ratio_rank_window_pass")
    return {
        "row_count": len(shadow_rows),
        "changed_rank_count": changed,
        "body_ratio_pass_count": passed,
        "body_ratio_demoted_count": demoted,
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


def _key_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_side(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"up", "buy", "long"}:
        return "long"
    if normalized in {"down", "sell", "short"}:
        return "short"
    return normalized
