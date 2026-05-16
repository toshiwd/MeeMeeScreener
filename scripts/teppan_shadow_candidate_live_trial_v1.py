"""Read-only live trial for the inactive teppan shadow adapter."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.ml import rankings_cache
from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)
from scripts.tradex_teppan_ranking_branching_probe_v1 import build_teppan_tags_for_source


AXIS_ID = "teppan_shadow_candidate_live_trial_v1"
DEFAULT_RUN_ID = "20260514T060000Z-teppan-shadow-candidate-live-trial-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\shadow_live_trials\teppan_shadow_candidate_live_trial_v1")
DEFAULT_PATTERN_ROOT = Path(
    r"G:\Tradex\teppan_chart_pattern_discovery_v1"
    r"\20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
)
REQUIRED_OUTPUTS = [
    "live_trial_result.json",
    "active_top5.json",
    "shadow_top5.json",
    "added_by_shadow.json",
    "removed_from_active.json",
    "boosted_candidates.json",
    "loss_guard_blocked_candidates.json",
    "shadow_top5_reason_summary.json",
    "human_review_candidate_list.json",
    "no_mutation_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--pattern-root", type=Path, default=DEFAULT_PATTERN_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--direction", default="up")
    parser.add_argument("--risk-mode", default="balanced")
    args = parser.parse_args()

    run_teppan_shadow_candidate_live_trial_v1(
        plan_root=args.plan_root,
        pattern_root=args.pattern_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        limit=args.limit,
        direction=args.direction,
        risk_mode=args.risk_mode,
    )
    return 0


def run_teppan_shadow_candidate_live_trial_v1(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    pattern_root: Path = DEFAULT_PATTERN_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    limit: int = 20,
    direction: str = "up",
    risk_mode: str = "balanced",
    ranking_payload: Mapping[str, Any] | None = None,
    runtime_status: Mapping[str, Any] | None = None,
    rankings_freshness: Mapping[str, Any] | None = None,
    teppan_tags: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    plan = load_teppan_shadow_plan(plan_root)
    effective_runtime_status = dict(runtime_status or get_runtime_stock_db_status())
    effective_rankings_freshness = dict(
        rankings_freshness
        or get_rankings_freshness(
            tf="D",
            which="latest",
            direction=direction,
            mode="trade",
            risk_mode=risk_mode,
            limit=limit,
        )
    )
    db_path = Path(str(effective_runtime_status.get("selected_runtime_db_path") or ""))
    db_stat_before = _file_stat(db_path)
    effective_ranking_payload = dict(
        ranking_payload
        or rankings_cache.get_rankings("D", "latest", direction, int(limit), mode="trade", risk_mode=risk_mode)
    )
    active_rows, active_index = _active_rows_from_ranking_payload(effective_ranking_payload, direction=direction)
    tags = list(teppan_tags) if teppan_tags is not None else _materialize_live_teppan_tags(active_rows, db_path, pattern_root)
    feature_rows = _feature_rows_from_tags(tags, active_rows)
    shadow_result = compute_teppan_shadow_adjusted_ranking(active_rows, feature_rows, plan)
    enriched_rows = _enrich_shadow_rows(shadow_result["shadow_rows"], active_index, feature_rows)

    active_top5 = _active_top5(active_rows, active_index)
    shadow_top5 = _shadow_top5(enriched_rows)
    active_codes = {str(row["symbol"]) for row in active_top5}
    shadow_codes = {str(row["symbol"]) for row in shadow_top5}
    added_by_shadow = [row for row in shadow_top5 if str(row["symbol"]) not in active_codes]
    removed_from_active = [row for row in active_top5 if str(row["symbol"]) not in shadow_codes]
    boosted_candidates = [row for row in enriched_rows if row.get("teppan_guarded_boost_applied") is True]
    loss_guard_blocked = [
        row
        for row in enriched_rows
        if row.get("teppan_pattern_match") is True and row.get("teppan_guard_pass") is False
    ]
    reason_summary = _reason_summary(shadow_top5, enriched_rows, added_by_shadow, removed_from_active)
    human_review = _human_review_candidate_list(shadow_top5, added_by_shadow, boosted_candidates)
    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(
        db_path=db_path,
        db_stat_before=db_stat_before,
        db_stat_after=db_stat_after,
        adapter_audit=shadow_result["audit"],
    )
    live_trial = {
        "schema_version": "teppan_shadow_candidate_live_trial_v1",
        "axis_id": AXIS_ID,
        "decision": _trial_decision(added_by_shadow, boosted_candidates, human_review),
        "integration_mode": "read_only_inactive_shadow_trial",
        "generated_at_utc": _utc_now(),
        "plan_root": str(plan.plan_root),
        "pattern_root": str(pattern_root),
        "runtime_stock_db_status": effective_runtime_status,
        "rankings_freshness": effective_rankings_freshness,
        "active_top5": active_top5,
        "shadow_top5": shadow_top5,
        "added_by_shadow": added_by_shadow,
        "removed_from_active": removed_from_active,
        "boosted_candidates": boosted_candidates,
        "loss_guard_blocked_candidates": loss_guard_blocked,
        "shadow_top5_reason_summary": reason_summary,
        "human_review_candidate_list": human_review,
        "evaluation": _evaluation(added_by_shadow, shadow_top5, boosted_candidates, loss_guard_blocked, human_review),
        "no_mutation_audit": no_mutation,
        "not_changed": [
            "active_ranking",
            "active_display_score",
            "runtime_duckdb",
            "production_publish_registry",
            "frontend_ui",
            "backend_api_response",
        ],
    }

    _write_json(output_root / "live_trial_result.json", live_trial)
    _write_json(output_root / "active_top5.json", {"active_top5": active_top5})
    _write_json(output_root / "shadow_top5.json", {"shadow_top5": shadow_top5})
    _write_json(output_root / "added_by_shadow.json", {"added_by_shadow": added_by_shadow})
    _write_json(output_root / "removed_from_active.json", {"removed_from_active": removed_from_active})
    _write_json(output_root / "boosted_candidates.json", {"boosted_candidates": boosted_candidates})
    _write_json(output_root / "loss_guard_blocked_candidates.json", {"loss_guard_blocked_candidates": loss_guard_blocked})
    _write_json(output_root / "shadow_top5_reason_summary.json", reason_summary)
    _write_json(output_root / "human_review_candidate_list.json", {"human_review_candidate_list": human_review, "max_user_selection": 3})
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    complete = _artifact_complete(output_root, live_trial)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)

    return {
        "output_root": str(output_root),
        "live_trial_result": live_trial,
        "artifact_complete": complete,
    }


def _active_rows_from_ranking_payload(payload: Mapping[str, Any], *, direction: str) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("runtime_ranking_payload_has_no_items")
    side = "long" if str(direction).lower() == "up" else "short"
    rows: list[dict[str, Any]] = []
    index: dict[str, dict[str, Any]] = {}
    for rank, item in enumerate(items, start=1):
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("code") or item.get("symbol") or "").strip()
        if not symbol:
            continue
        score, score_source = _ranking_score(item, rank=rank)
        row = {
            "anchor_date": str(item.get("asOf") or payload.get("snapshot_as_of") or ""),
            "symbol": symbol,
            "side": side,
            "champion_rank": rank,
            "champion_score": score,
            "score_source": score_source,
            "name": item.get("name"),
        }
        rows.append(row)
        index[symbol] = {
            "symbol": symbol,
            "name": item.get("name"),
            "as_of": row["anchor_date"],
            "active_rank": rank,
            "active_score": score,
            "active_score_source": score_source,
            "runtime_entry_score": _optional_float(item.get("entryScore")),
            "runtime_prob_side": _optional_float(item.get("probSide")),
            "setup_type": item.get("setupType"),
            "trade_entry_class": item.get("tradeEntryClass"),
            "trade_decision_reasons": item.get("tradeDecisionReasons") or [],
            "trade_risk_watch": item.get("tradeRiskWatch") or [],
        }
    if not rows:
        raise ValueError("runtime_ranking_payload_items_missing_symbols")
    return rows, index


def _ranking_score(item: Mapping[str, Any], *, rank: int) -> tuple[float, str]:
    display_score = item.get("displayScore")
    if display_score is not None and display_score != "":
        return float(display_score), "displayScore"
    return 1.0 - (int(rank) * 0.01), "rank_order_surrogate_no_displayScore"


def _materialize_live_teppan_tags(active_rows: Sequence[Mapping[str, Any]], db_path: Path, pattern_root: Path) -> list[dict[str, Any]]:
    source_rows = pd.DataFrame(
        {
            "anchor_date": [row["anchor_date"] for row in active_rows],
            "anchor_ymd": [int(str(row["anchor_date"]).replace("-", "")) for row in active_rows],
            "side": [row["side"] for row in active_rows],
            "symbol": [row["symbol"] for row in active_rows],
            "champion_rank": [row["champion_rank"] for row in active_rows],
            "champion_score": [row["champion_score"] for row in active_rows],
        }
    )
    tags = build_teppan_tags_for_source(source_rows=source_rows, source_db=db_path, pattern_dir=pattern_root)
    return tags.to_dict(orient="records")


def _feature_rows_from_tags(tags: Sequence[Mapping[str, Any]], active_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    date_by_symbol = {str(row["symbol"]): str(row["anchor_date"]) for row in active_rows}
    side_by_symbol = {str(row["symbol"]): str(row["side"]) for row in active_rows}
    out: list[dict[str, Any]] = []
    for tag in tags:
        symbol = str(tag.get("symbol") or "")
        anchor_date = date_by_symbol.get(symbol) or _date_text(tag.get("anchor_ymd"))
        row = dict(tag)
        row["symbol"] = symbol
        row["anchor_date"] = anchor_date
        row["side"] = side_by_symbol.get(symbol, "long")
        out.append(row)
    return out


def _enrich_shadow_rows(
    shadow_rows: Sequence[Mapping[str, Any]],
    active_index: Mapping[str, Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    feature_index = {str(row.get("symbol")): row for row in feature_rows}
    out: list[dict[str, Any]] = []
    for row in shadow_rows:
        symbol = str(row.get("symbol"))
        active = active_index.get(symbol, {})
        feature = feature_index.get(symbol, {})
        enriched = {
            **row,
            "name": active.get("name"),
            "as_of": active.get("as_of"),
            "active_score_source": active.get("active_score_source"),
            "runtime_entry_score": active.get("runtime_entry_score"),
            "runtime_prob_side": active.get("runtime_prob_side"),
            "setup_type": active.get("setup_type"),
            "trade_entry_class": active.get("trade_entry_class"),
            "trade_decision_reasons": active.get("trade_decision_reasons") or [],
            "trade_risk_watch": active.get("trade_risk_watch") or [],
            "best_pattern_family": feature.get("best_pattern_family"),
            "best_pattern_key": feature.get("best_pattern_key"),
            "best_pattern_decision": feature.get("best_pattern_decision"),
            "best_teppan_score": _json_number(feature.get("best_teppan_score")),
            "matched_pattern_count": int(feature.get("matched_pattern_count") or 0),
            "guard_block_reason": feature.get("guard_block_reason"),
        }
        out.append(_compact_candidate(enriched))
    return out


def _active_top5(active_rows: Sequence[Mapping[str, Any]], active_index: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in sorted(active_rows, key=lambda item: int(item["champion_rank"]))[:5]:
        symbol = str(row["symbol"])
        out.append(_compact_candidate({**active_index.get(symbol, {}), "symbol": symbol}))
    return out


def _shadow_top5(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (int(row["shadow_adjusted_rank"]), int(row["original_rank"]), str(row["symbol"])))
    return [_compact_candidate(row) for row in ordered[:5]]


def _reason_summary(
    shadow_top5: Sequence[Mapping[str, Any]],
    all_rows: Sequence[Mapping[str, Any]],
    added_by_shadow: Sequence[Mapping[str, Any]],
    removed_from_active: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "teppan_shadow_top5_reason_summary_v1",
        "shadow_top5_count": len(shadow_top5),
        "teppan_pattern_match_count": sum(1 for row in shadow_top5 if row.get("teppan_pattern_match") is True),
        "teppan_guard_pass_count": sum(1 for row in shadow_top5 if row.get("teppan_guard_pass") is True),
        "boosted_top5_count": sum(1 for row in shadow_top5 if row.get("teppan_guarded_boost_applied") is True),
        "added_by_shadow_count": len(added_by_shadow),
        "removed_from_active_count": len(removed_from_active),
        "decision_reason_counts_all": dict(Counter(str(row.get("shadow_decision_reason")) for row in all_rows)),
        "explanation_available_for_top5": all(
            "teppan_pattern_match" in row and "teppan_guard_pass" in row and row.get("shadow_decision_reason") for row in shadow_top5
        ),
    }


def _human_review_candidate_list(
    shadow_top5: Sequence[Mapping[str, Any]],
    added_by_shadow: Sequence[Mapping[str, Any]],
    boosted_candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for bucket, rows in (("added_by_shadow", added_by_shadow), ("boosted_shadow", boosted_candidates), ("shadow_top5", shadow_top5)):
        for row in rows:
            symbol = str(row.get("symbol"))
            if symbol and symbol not in selected:
                candidate = _compact_candidate(dict(row))
                candidate["human_review_reason"] = bucket
                selected[symbol] = candidate
    ordered = sorted(
        selected.values(),
        key=lambda row: (
            0 if row.get("human_review_reason") == "added_by_shadow" else 1,
            int(row.get("shadow_adjusted_rank") or row.get("active_rank") or 999),
            str(row.get("symbol")),
        ),
    )
    for idx, row in enumerate(ordered[:5], start=1):
        row["review_priority_rank"] = idx
        row["max_user_selection"] = 3
    return ordered[:5]


def _evaluation(
    added_by_shadow: Sequence[Mapping[str, Any]],
    shadow_top5: Sequence[Mapping[str, Any]],
    boosted_candidates: Sequence[Mapping[str, Any]],
    loss_guard_blocked: Sequence[Mapping[str, Any]],
    human_review: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    added_risky = [
        row
        for row in added_by_shadow
        if row.get("teppan_guard_pass") is False or bool(row.get("trade_risk_watch"))
    ]
    return {
        "shadow_top5_added_buyable_candidates": len(added_by_shadow),
        "clearly_bad_candidate_added_count": len(added_risky),
        "teppan_explanation_usable": all(
            "teppan_pattern_match" in row and "teppan_guard_pass" in row and row.get("shadow_decision_reason") for row in shadow_top5
        ),
        "human_can_select_up_to_3": len(human_review) >= 3,
        "boosted_candidate_count": len(boosted_candidates),
        "loss_guard_blocked_candidate_count": len(loss_guard_blocked),
        "judgment": _trial_judgment(added_by_shadow, boosted_candidates, human_review),
    }


def _trial_decision(
    added_by_shadow: Sequence[Mapping[str, Any]],
    boosted_candidates: Sequence[Mapping[str, Any]],
    human_review: Sequence[Mapping[str, Any]],
) -> str:
    if not human_review:
        return "hold_no_human_review_candidates"
    if added_by_shadow or boosted_candidates:
        return "live_trial_ready_for_human_review"
    return "hold_shadow_no_incremental_candidate"


def _trial_judgment(
    added_by_shadow: Sequence[Mapping[str, Any]],
    boosted_candidates: Sequence[Mapping[str, Any]],
    human_review: Sequence[Mapping[str, Any]],
) -> str:
    if not human_review:
        return "hold_no_candidates"
    if added_by_shadow or boosted_candidates:
        return "candidate_list_ready_for_manual_pick"
    return "active_top5_review_only_no_shadow_incremental_value"


def _no_mutation_audit(
    *,
    db_path: Path,
    db_stat_before: Mapping[str, Any],
    db_stat_after: Mapping[str, Any],
    adapter_audit: Mapping[str, Any],
) -> dict[str, Any]:
    db_unchanged = db_stat_before == db_stat_after
    return {
        "schema_version": "teppan_shadow_candidate_live_trial_no_mutation_audit_v1",
        "runtime_duckdb_path": str(db_path),
        "runtime_duckdb_stat_before": dict(db_stat_before),
        "runtime_duckdb_stat_after": dict(db_stat_after),
        "runtime_duckdb_written": not db_unchanged,
        "runtime_duckdb_unchanged": db_unchanged,
        "active_ranking_invariance_pass": bool(adapter_audit.get("active_ranking_invariance_pass")),
        "active_rank_unchanged": bool(adapter_audit.get("active_rank_unchanged")),
        "active_display_score_unchanged": bool(adapter_audit.get("active_display_score_unchanged")),
        "production_publish_registered": False,
        "frontend_changed": False,
        "backend_api_response_changed": False,
        "no_mutation_pass": db_unchanged and bool(adapter_audit.get("active_ranking_invariance_pass")),
        "silent_fallback_used": False,
    }


def _compact_candidate(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = [
        "symbol",
        "name",
        "as_of",
        "anchor_date",
        "active_rank",
        "original_rank",
        "shadow_adjusted_rank",
        "active_score",
        "active_display_score",
        "original_score",
        "shadow_adjusted_score",
        "active_score_source",
        "runtime_entry_score",
        "runtime_prob_side",
        "teppan_guarded_boost_applied",
        "teppan_shadow_boost_value",
        "teppan_pattern_match",
        "teppan_guard_pass",
        "shadow_decision_reason",
        "best_pattern_family",
        "best_pattern_key",
        "best_pattern_decision",
        "best_teppan_score",
        "matched_pattern_count",
        "guard_block_reason",
        "setup_type",
        "trade_entry_class",
        "trade_decision_reasons",
        "trade_risk_watch",
        "human_review_reason",
        "review_priority_rank",
        "max_user_selection",
    ]
    return {key: _json_ready(row.get(key)) for key in keys if key in row and row.get(key) is not None}


def _artifact_complete(output_root: Path, live_trial: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_candidate_live_trial_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": live_trial.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _file_stat(path: Path) -> dict[str, Any]:
    if not path or not str(path):
        return {"exists": False}
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {"exists": True, "path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _date_text(value: Any) -> str:
    if value is None or value == "":
        return ""
    text = str(value)
    if "-" in text:
        return text[:10]
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}" if len(text) >= 8 else text


def _json_number(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _optional_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
