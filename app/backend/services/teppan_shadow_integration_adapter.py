"""Inactive teppan shadow-ranking adapter.

This module is intentionally pure: it reads a reviewed plan artifact and can
compute shadow-only adjusted scores/ranks from caller-provided rows, but it does
not write runtime storage or register any production publish state.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_PLAN_ROOT = Path(
    r"G:\Tradex\shadow_integration_plans\teppan_ranking_meemee_shadow_integration_plan_v1"
    r"\20260514T040000Z-teppan-ranking-meemee-shadow-integration-plan-v1"
)

SCHEMA_VERSION = "teppan_shadow_integration_adapter_v1"
STATIC_SOFT_BOOST = 0.04
ELIGIBLE_MIN_RANK = 6
ELIGIBLE_MAX_RANK = 20


class TeppanShadowPlanError(ValueError):
    """Raised when the reviewed shadow plan is missing or not approved."""


@dataclass(frozen=True)
class TeppanShadowPlan:
    plan_root: Path
    shadow_integration_plan: Mapping[str, Any]
    feature_materialization_plan: Mapping[str, Any]
    rank_storage_contract: Mapping[str, Any]
    rollback_plan: Mapping[str, Any]
    artifact_complete: Mapping[str, Any]

    @property
    def source_candidate_id(self) -> str:
        value = self.shadow_integration_plan.get("source_candidate_id")
        return str(value or "static_teppan_guarded_soft_boost_v1")

    @property
    def boost_value(self) -> float:
        value = self.shadow_integration_plan.get("static_soft_boost_value", STATIC_SOFT_BOOST)
        try:
            return float(value)
        except (TypeError, ValueError):
            return STATIC_SOFT_BOOST


def load_teppan_shadow_plan(plan_root: str | Path = DEFAULT_PLAN_ROOT) -> TeppanShadowPlan:
    root = Path(plan_root)
    plan = _read_json(root / "shadow_integration_plan.json")
    feature_plan = _read_json(root / "feature_materialization_plan.json")
    rank_contract = _read_json(root / "rank_storage_contract.json")
    rollback_plan = _read_json(root / "rollback_plan.json")
    artifact_complete = _read_json(root / "_ARTIFACT_COMPLETE.json")

    issues: list[str] = []
    if plan.get("decision") != "approve_shadow_integration_implementation":
        issues.append("shadow_integration_plan_not_approved")
    if plan.get("active_runtime_ranking_change_allowed") is not False:
        issues.append("active_runtime_ranking_change_not_forbidden")
    if plan.get("runtime_duckdb_write_allowed") is not False:
        issues.append("runtime_duckdb_write_not_forbidden")
    if plan.get("production_publish_registration_allowed") is not False:
        issues.append("production_publish_registration_not_forbidden")
    if plan.get("frontend_or_backend_ui_change_allowed") is not False:
        issues.append("frontend_or_backend_ui_change_not_forbidden")
    if (feature_plan.get("feature_materialization_decision") or feature_plan.get("decision")) != "ready":
        issues.append("feature_materialization_not_ready")

    materialized = {
        item.get("feature") if isinstance(item, Mapping) else item
        for item in (feature_plan.get("materialized_features") or [])
    }
    for required_feature in ("teppan_pattern_match", "teppan_guard_pass"):
        if required_feature not in materialized:
            issues.append(f"missing_materialized_feature:{required_feature}")

    record_shape = rank_contract.get("record_shape") or {}
    if record_shape.get("original_rank_is_recoverable") is not True:
        issues.append("original_rank_not_recoverable")
    if record_shape.get("adjusted_rank_is_separate") is not True:
        issues.append("adjusted_rank_not_separate")
    if artifact_complete.get("complete") is not True:
        issues.append("plan_artifact_not_complete")

    if issues:
        raise TeppanShadowPlanError(";".join(issues))

    return TeppanShadowPlan(
        plan_root=root,
        shadow_integration_plan=plan,
        feature_materialization_plan=feature_plan,
        rank_storage_contract=rank_contract,
        rollback_plan=rollback_plan,
        artifact_complete=artifact_complete,
    )


def compute_teppan_shadow_adjusted_ranking(
    active_rows: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]] | Mapping[tuple[str, str, str], Mapping[str, Any]],
    plan: TeppanShadowPlan | None = None,
) -> dict[str, Any]:
    active_input = [dict(row) for row in active_rows]
    feature_lookup = _build_feature_lookup(feature_rows)
    effective_plan = plan or load_teppan_shadow_plan()

    shadow_rows: list[dict[str, Any]] = []
    group_indexes: dict[tuple[str, str], list[int]] = defaultdict(list)

    for row in active_input:
        anchor_date = _text(row, "anchor_date", "dt", "date")
        symbol = _text(row, "symbol", "code", "ticker")
        side = _normalize_side(_text(row, "side", "direction", "dir", default="long"))
        original_rank = _int(row, "rank", "champion_rank", "original_rank")
        original_score = _float(row, "display_score", "champion_score", "score")
        feature = _find_feature(feature_lookup, anchor_date, symbol, side)

        pattern_match = _optional_bool(feature.get("teppan_pattern_match")) if feature else None
        guard_pass = _optional_bool(feature.get("teppan_guard_pass")) if feature else None
        boost_applied, reason = _boost_decision(
            original_rank=original_rank,
            side=side,
            pattern_match=pattern_match,
            guard_pass=guard_pass,
        )
        boost_value = effective_plan.boost_value if boost_applied else 0.0
        shadow_adjusted_score = original_score + boost_value

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
            "shadow_adjusted_score": shadow_adjusted_score,
            "shadow_adjusted_rank": None,
            "teppan_guarded_boost_applied": boost_applied,
            "teppan_shadow_boost_value": boost_value,
            "teppan_pattern_match": pattern_match,
            "teppan_guard_pass": guard_pass,
            "shadow_decision_reason": reason,
        }
        group_indexes[(anchor_date, side)].append(len(shadow_rows))
        shadow_rows.append(record)

    for indexes in group_indexes.values():
        ordered = sorted(
            indexes,
            key=lambda idx: (
                -float(shadow_rows[idx]["shadow_adjusted_score"]),
                int(shadow_rows[idx]["original_rank"]),
                str(shadow_rows[idx]["symbol"]),
            ),
        )
        for adjusted_rank, idx in enumerate(ordered, start=1):
            shadow_rows[idx]["shadow_adjusted_rank"] = adjusted_rank

    audit = _build_invariance_audit(active_input, shadow_rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "integration_mode": "inactive_shadow_only",
        "plan_root": str(effective_plan.plan_root),
        "shadow_rows": shadow_rows,
        "summary": _build_summary(shadow_rows),
        "audit": audit,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise TeppanShadowPlanError(f"missing_required_plan_artifact:{path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise TeppanShadowPlanError(f"plan_artifact_not_object:{path}")
    return data


def _build_feature_lookup(
    feature_rows: Sequence[Mapping[str, Any]] | Mapping[tuple[str, str, str], Mapping[str, Any]]
) -> dict[tuple[str, str, str], Mapping[str, Any]]:
    if isinstance(feature_rows, Mapping):
        return {(_key_text(k[0]), _key_text(k[1]), _normalize_side(_key_text(k[2]))): v for k, v in feature_rows.items()}

    lookup: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for row in feature_rows:
        anchor_date = _text(row, "anchor_date", "dt", "date")
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


def _boost_decision(
    *,
    original_rank: int,
    side: str,
    pattern_match: bool | None,
    guard_pass: bool | None,
) -> tuple[bool, str]:
    if side not in {"long", "buy", "up"}:
        return False, "non_long_side_not_eligible"
    if original_rank < ELIGIBLE_MIN_RANK or original_rank > ELIGIBLE_MAX_RANK:
        return False, "outside_rank_6_20_shadow_pool"
    if pattern_match is None:
        return False, "missing_teppan_pattern_match_no_silent_fallback"
    if pattern_match is not True:
        return False, "teppan_pattern_match_false"
    if guard_pass is None:
        return False, "missing_teppan_guard_pass_no_silent_fallback"
    if guard_pass is not True:
        return False, "teppan_guard_blocked"
    return True, "static_teppan_guarded_soft_boost_applied"


def _build_invariance_audit(
    active_rows: Sequence[Mapping[str, Any]],
    shadow_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    active_rank_unchanged = True
    active_display_score_unchanged = True
    original_rank_recoverable = True
    original_score_recoverable = True
    adjusted_rank_separate = True

    for active, shadow in zip(active_rows, shadow_rows, strict=True):
        rank = _int(active, "rank", "champion_rank", "original_rank")
        score = _float(active, "display_score", "champion_score", "score")
        active_rank_unchanged = active_rank_unchanged and shadow["active_rank"] == rank
        active_display_score_unchanged = active_display_score_unchanged and shadow["active_display_score"] == score
        original_rank_recoverable = original_rank_recoverable and shadow["original_rank"] == rank
        original_score_recoverable = original_score_recoverable and shadow["original_score"] == score
        adjusted_rank_separate = adjusted_rank_separate and "shadow_adjusted_rank" in shadow and "rank" not in shadow

    return {
        "active_ranking_invariance_pass": active_rank_unchanged and active_display_score_unchanged,
        "active_rank_unchanged": active_rank_unchanged,
        "active_display_score_unchanged": active_display_score_unchanged,
        "original_rank_recoverable": original_rank_recoverable,
        "original_score_recoverable": original_score_recoverable,
        "adjusted_rank_separate": adjusted_rank_separate,
        "runtime_duckdb_write_attempted": False,
        "production_registry_write_attempted": False,
        "no_runtime_write_path": True,
        "shadow_only": True,
    }


def _build_summary(shadow_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    boosted = sum(1 for row in shadow_rows if row.get("teppan_guarded_boost_applied") is True)
    guard_blocked = sum(1 for row in shadow_rows if row.get("shadow_decision_reason") == "teppan_guard_blocked")
    missing_features = sum(
        1 for row in shadow_rows if str(row.get("shadow_decision_reason", "")).startswith("missing_teppan_")
    )
    return {
        "row_count": len(shadow_rows),
        "boosted_row_count": boosted,
        "guard_blocked_row_count": guard_blocked,
        "missing_feature_row_count": missing_features,
        "active_runtime_ranking_changed": False,
        "runtime_duckdb_written": False,
        "production_publish_registered": False,
    }


def _text(row: Mapping[str, Any], *keys: str, default: str | None = None) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return _key_text(value)
    if default is not None:
        return default
    raise ValueError(f"missing_required_text_field:{'/'.join(keys)}")


def _key_text(value: Any) -> str:
    text = str(value)
    if " " in text:
        return text.split(" ", 1)[0]
    return text


def _normalize_side(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"long", "buy", "up", "bull"}:
        return "long"
    if lowered in {"short", "sell", "down", "bear"}:
        return "short"
    return lowered or "long"


def _int(row: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return int(value)
    raise ValueError(f"missing_required_int_field:{'/'.join(keys)}")


def _float(row: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return float(value)
    raise ValueError(f"missing_required_float_field:{'/'.join(keys)}")


def _optional_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    lowered = str(value).strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None
