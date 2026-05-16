"""Read-only topK coverage observation for the inactive teppan shadow adapter."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)
from scripts.tradex_teppan_ranking_branching_probe_v1 import build_teppan_tags_for_source


AXIS_ID = "teppan_shadow_runtime_coverage_observation_v1"
DEFAULT_RUN_ID = "20260514T070000Z-teppan-shadow-runtime-coverage-observation-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\shadow_runtime_coverage_observations\teppan_shadow_runtime_coverage_observation_v1")
DEFAULT_PATTERN_ROOT = Path(
    r"G:\Tradex\teppan_chart_pattern_discovery_v1"
    r"\20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
)
TOP_KS = (5, 10, 20)
REQUIRED_OUTPUTS = [
    "coverage_observation_result.json",
    "active_topk.json",
    "shadow_topk.json",
    "topk_comparison.json",
    "coverage_summary.json",
    "boost_promotion_potential.json",
    "candidate_rows.json",
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
    run_teppan_shadow_runtime_coverage_observation_v1(
        plan_root=args.plan_root,
        pattern_root=args.pattern_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        limit=args.limit,
        direction=args.direction,
        risk_mode=args.risk_mode,
    )
    return 0


def run_teppan_shadow_runtime_coverage_observation_v1(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    pattern_root: Path = DEFAULT_PATTERN_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    limit: int = 20,
    direction: str = "up",
    risk_mode: str = "balanced",
    runtime_status: Mapping[str, Any] | None = None,
    rankings_freshness: Mapping[str, Any] | None = None,
    active_rows: Sequence[Mapping[str, Any]] | None = None,
    teppan_tags: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    plan = load_teppan_shadow_plan(plan_root)
    effective_runtime_status = dict(runtime_status or get_runtime_stock_db_status())
    effective_rankings_freshness = dict(
        rankings_freshness
        or get_rankings_freshness(tf="D", which="latest", direction=direction, mode="trade", risk_mode=risk_mode, limit=limit)
    )
    db_path = Path(str(effective_runtime_status.get("selected_runtime_db_path") or ""))
    db_stat_before = _file_stat(db_path)

    source_rows = list(active_rows) if active_rows is not None else _load_active_ranking_rows(db_path, direction=direction, limit=limit)
    tags = list(teppan_tags) if teppan_tags is not None else _materialize_tags(source_rows, db_path, pattern_root)
    feature_rows = _feature_rows_from_tags(tags, source_rows)
    shadow_result = compute_teppan_shadow_adjusted_ranking(source_rows, feature_rows, plan)
    candidate_rows = _enrich_rows(shadow_result["shadow_rows"], source_rows, feature_rows)

    active_topk = {f"top{k}": _topk(candidate_rows, "active_rank", k) for k in TOP_KS}
    shadow_topk = {f"top{k}": _topk(candidate_rows, "shadow_adjusted_rank", k) for k in TOP_KS}
    topk_comparison = _topk_comparison(active_topk, shadow_topk)
    coverage_summary = _coverage_summary(candidate_rows)
    boost_potential = _boost_promotion_potential(candidate_rows)
    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(
        db_path=db_path,
        db_stat_before=db_stat_before,
        db_stat_after=db_stat_after,
        adapter_audit=shadow_result["audit"],
    )
    decision = _decision(coverage_summary, boost_potential)
    result = {
        "schema_version": "teppan_shadow_runtime_coverage_observation_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "integration_mode": "read_only_coverage_observation",
        "generated_at_utc": _utc_now(),
        "source_surface": "runtime_duckdb.ranking_appearance_daily",
        "source_surface_note": "broader active runtime ranking table; differs from candidate-list endpoint used by live_trial_v1",
        "plan_root": str(plan.plan_root),
        "pattern_root": str(pattern_root),
        "runtime_stock_db_status": effective_runtime_status,
        "rankings_freshness": effective_rankings_freshness,
        "active_topk": active_topk,
        "shadow_topk": shadow_topk,
        "topk_comparison": topk_comparison,
        "coverage_summary": coverage_summary,
        "boost_promotion_potential": boost_potential,
        "candidate_rows": candidate_rows,
        "no_mutation_audit": no_mutation,
        "not_changed": [
            "active_rank",
            "display_score",
            "runtime_duckdb",
            "production_publish_registry",
            "frontend_ui",
            "backend_api_response",
        ],
    }

    _write_json(output_root / "coverage_observation_result.json", result)
    _write_json(output_root / "active_topk.json", active_topk)
    _write_json(output_root / "shadow_topk.json", shadow_topk)
    _write_json(output_root / "topk_comparison.json", topk_comparison)
    _write_json(output_root / "coverage_summary.json", coverage_summary)
    _write_json(output_root / "boost_promotion_potential.json", boost_potential)
    _write_json(output_root / "candidate_rows.json", {"candidate_rows": candidate_rows})
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    complete = _artifact_complete(output_root, result)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "coverage_observation_result": result, "artifact_complete": complete}


def _load_active_ranking_rows(db_path: Path, *, direction: str, limit: int) -> list[dict[str, Any]]:
    dir_value = str(direction or "up").lower()
    if dir_value not in {"up", "down"}:
        raise ValueError("direction must be up or down")
    side = "long" if dir_value == "up" else "short"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        latest = conn.execute("SELECT max(dt) FROM ranking_appearance_daily WHERE dir = ?", [dir_value]).fetchone()[0]
        rows = conn.execute(
            """
            SELECT dt, dir, rank, code, name, display_score, signal_state_at_appearance,
                   entry_qualified_at_appearance, setup_type_at_appearance, status
            FROM ranking_appearance_daily
            WHERE dt = ?
              AND dir = ?
              AND rank <= ?
            ORDER BY rank, code
            """,
            [int(latest), dir_value, int(limit)],
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        raise ValueError("ranking_appearance_daily_topk_empty")
    out = []
    for dt, _dir, rank, code, name, display_score, signal_state, entry_qualified, setup_type, status in rows:
        out.append(
            {
                "anchor_date": _date_text(dt),
                "symbol": str(code),
                "name": name,
                "side": side,
                "champion_rank": int(rank),
                "champion_score": float(display_score),
                "display_score": float(display_score),
                "signal_state": signal_state,
                "entry_qualified": bool(entry_qualified),
                "setup_type": setup_type,
                "status": status,
            }
        )
    return out


def _materialize_tags(active_rows: Sequence[Mapping[str, Any]], db_path: Path, pattern_root: Path) -> list[dict[str, Any]]:
    frame = pd.DataFrame(
        {
            "anchor_date": [row["anchor_date"] for row in active_rows],
            "anchor_ymd": [int(str(row["anchor_date"]).replace("-", "")) for row in active_rows],
            "side": [row["side"] for row in active_rows],
            "symbol": [row["symbol"] for row in active_rows],
            "champion_rank": [row["champion_rank"] for row in active_rows],
            "champion_score": [row["champion_score"] for row in active_rows],
        }
    )
    return build_teppan_tags_for_source(source_rows=frame, source_db=db_path, pattern_dir=pattern_root).to_dict(orient="records")


def _feature_rows_from_tags(tags: Sequence[Mapping[str, Any]], active_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    active_index = {str(row["symbol"]): row for row in active_rows}
    out = []
    for tag in tags:
        row = dict(tag)
        active = active_index.get(str(row.get("symbol")), {})
        row["anchor_date"] = active.get("anchor_date") or _date_text(row.get("anchor_ymd"))
        row["side"] = active.get("side") or "long"
        out.append(row)
    return out


def _enrich_rows(
    shadow_rows: Sequence[Mapping[str, Any]],
    active_rows: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    active_index = {str(row["symbol"]): row for row in active_rows}
    feature_index = {str(row.get("symbol")): row for row in feature_rows}
    out = []
    for shadow in shadow_rows:
        symbol = str(shadow["symbol"])
        active = active_index.get(symbol, {})
        feature = feature_index.get(symbol, {})
        row = {
            "symbol": symbol,
            "name": active.get("name"),
            "anchor_date": shadow.get("anchor_date"),
            "active_rank": shadow.get("active_rank"),
            "display_score": shadow.get("active_display_score"),
            "original_rank": shadow.get("original_rank"),
            "original_score": shadow.get("original_score"),
            "shadow_adjusted_rank": shadow.get("shadow_adjusted_rank"),
            "shadow_adjusted_score": shadow.get("shadow_adjusted_score"),
            "teppan_guarded_boost_applied": shadow.get("teppan_guarded_boost_applied"),
            "teppan_shadow_boost_value": shadow.get("teppan_shadow_boost_value"),
            "teppan_pattern_match": shadow.get("teppan_pattern_match"),
            "teppan_guard_pass": shadow.get("teppan_guard_pass"),
            "shadow_decision_reason": shadow.get("shadow_decision_reason"),
            "best_pattern_family": feature.get("best_pattern_family"),
            "best_pattern_key": feature.get("best_pattern_key"),
            "best_pattern_decision": feature.get("best_pattern_decision"),
            "best_teppan_score": _optional_float(feature.get("best_teppan_score")),
            "matched_pattern_count": int(feature.get("matched_pattern_count") or 0),
            "guard_block_reason": feature.get("guard_block_reason"),
            "signal_state": active.get("signal_state"),
            "entry_qualified": active.get("entry_qualified"),
            "setup_type": active.get("setup_type"),
            "status": active.get("status"),
        }
        out.append(_compact(row))
    return sorted(out, key=lambda row: int(row["active_rank"]))


def _topk(rows: Sequence[Mapping[str, Any]], rank_field: str, k: int) -> list[dict[str, Any]]:
    return [_compact(row) for row in sorted(rows, key=lambda row: (int(row[rank_field]), str(row["symbol"])))[:k]]


def _topk_comparison(active_topk: Mapping[str, Sequence[Mapping[str, Any]]], shadow_topk: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    out = {"schema_version": "teppan_shadow_runtime_coverage_topk_comparison_v1"}
    for key, active_rows in active_topk.items():
        shadow_rows = shadow_topk[key]
        active_symbols = {str(row["symbol"]) for row in active_rows}
        shadow_symbols = {str(row["symbol"]) for row in shadow_rows}
        out[key] = {
            "active_symbols": [row["symbol"] for row in active_rows],
            "shadow_symbols": [row["symbol"] for row in shadow_rows],
            "added_by_shadow": [row for row in shadow_rows if str(row["symbol"]) not in active_symbols],
            "removed_from_active": [row for row in active_rows if str(row["symbol"]) not in shadow_symbols],
            "changed_member_count": len(active_symbols ^ shadow_symbols),
        }
    return out


def _coverage_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    summary = {"schema_version": "teppan_shadow_runtime_coverage_summary_v1"}
    for k in TOP_KS:
        subset = [row for row in rows if int(row["active_rank"]) <= k]
        summary[f"top{k}"] = _coverage_bucket(subset)
    summary["all_observed"] = _coverage_bucket(rows)
    summary["decision_reason_counts"] = dict(Counter(str(row.get("shadow_decision_reason")) for row in rows))
    return summary


def _coverage_bucket(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    pattern = sum(1 for row in rows if row.get("teppan_pattern_match") is True)
    guard = sum(1 for row in rows if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in rows if row.get("teppan_pattern_match") is True and row.get("teppan_guard_pass") is False)
    boosted = sum(1 for row in rows if row.get("teppan_guarded_boost_applied") is True)
    return {
        "row_count": count,
        "teppan_pattern_match_count": pattern,
        "teppan_pattern_match_rate": _rate(pattern, count),
        "teppan_guard_pass_count": guard,
        "teppan_guard_pass_rate": _rate(guard, count),
        "loss_guard_blocked_count": blocked,
        "loss_guard_blocked_rate": _rate(blocked, count),
        "boosted_candidate_count": boosted,
        "boosted_candidate_rate": _rate(boosted, count),
    }


def _boost_promotion_potential(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    boosted = [row for row in rows if row.get("teppan_guarded_boost_applied") is True]
    return {
        "schema_version": "teppan_shadow_runtime_boost_promotion_potential_v1",
        "boosted_candidates": boosted,
        "boosted_candidate_count": len(boosted),
        "would_enter_top5": [row for row in boosted if int(row["active_rank"]) > 5 and int(row["shadow_adjusted_rank"]) <= 5],
        "would_enter_top10": [row for row in boosted if int(row["active_rank"]) > 10 and int(row["shadow_adjusted_rank"]) <= 10],
        "would_enter_top20": [row for row in boosted if int(row["active_rank"]) > 20 and int(row["shadow_adjusted_rank"]) <= 20],
        "near_top5_after_shadow": [row for row in boosted if int(row["shadow_adjusted_rank"]) <= 10],
    }


def _decision(coverage_summary: Mapping[str, Any], boost_potential: Mapping[str, Any]) -> str:
    if boost_potential.get("would_enter_top5") or boost_potential.get("would_enter_top10"):
        return "observe_shadow_promotion_potential"
    if (coverage_summary.get("all_observed") or {}).get("teppan_pattern_match_count", 0) > 0:
        return "observe_feature_coverage_without_topk_promotion"
    return "hold_no_live_teppan_coverage"


def _no_mutation_audit(
    *,
    db_path: Path,
    db_stat_before: Mapping[str, Any],
    db_stat_after: Mapping[str, Any],
    adapter_audit: Mapping[str, Any],
) -> dict[str, Any]:
    unchanged = db_stat_before == db_stat_after
    return {
        "schema_version": "teppan_shadow_runtime_coverage_no_mutation_audit_v1",
        "runtime_duckdb_path": str(db_path),
        "runtime_duckdb_stat_before": dict(db_stat_before),
        "runtime_duckdb_stat_after": dict(db_stat_after),
        "runtime_duckdb_unchanged": unchanged,
        "runtime_duckdb_written": not unchanged,
        "active_ranking_invariance_pass": bool(adapter_audit.get("active_ranking_invariance_pass")),
        "active_rank_unchanged": bool(adapter_audit.get("active_rank_unchanged")),
        "display_score_unchanged": bool(adapter_audit.get("active_display_score_unchanged")),
        "production_publish_registered": False,
        "frontend_changed": False,
        "backend_api_response_changed": False,
        "no_mutation_pass": unchanged and bool(adapter_audit.get("active_ranking_invariance_pass")),
        "silent_fallback_used": False,
    }


def _artifact_complete(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_shadow_runtime_coverage_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": result.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _rate(num: int, denom: int) -> float | None:
    return None if denom <= 0 else float(num) / float(denom)


def _date_text(value: Any) -> str:
    text = str(value or "")
    if "-" in text:
        return text[:10]
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}" if len(text) >= 8 else text


def _optional_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _file_stat(path: Path) -> dict[str, Any]:
    if not path or not str(path):
        return {"exists": False}
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {"exists": True, "path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _compact(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _json_ready(value) for key, value in row.items() if value is not None}


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
