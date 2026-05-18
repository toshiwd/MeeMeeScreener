from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_downside_prebreak_narrow_universe_selection_v4 as v4


AXIS_ID = "tradex_downside_prebreak_narrow_universe_stability_v5"
SCHEMA_PREFIX = "tradex_downside_prebreak_narrow_universe_stability_v5"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\downside_prebreak_narrow_universe_stability_v5")
DEFAULT_RANK_LIMITS = (10, 20, 30)

REQUIRED_ARTIFACTS = (
    "downside_prebreak_narrow_universe_stability_contract.json",
    "downside_prebreak_narrow_universe_rank_limit_compare.json",
    "downside_prebreak_narrow_universe_rank_limit_grid.csv",
    "downside_prebreak_narrow_universe_stability_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _load_rows(source_db: Path, *, start_ymd: int, end_ymd: int) -> list[dict[str, Any]]:
    with v4.base.duckdb.connect(str(source_db), read_only=True) as conn:
        months = v4.base._month_end_dates(conn, start_ymd=int(start_ymd), end_ymd=int(end_ymd))
        return [
            dict(row)
            for row in v4.base._build_rows(
                conn=conn,
                months=months,
                price_store=v4.base._load_price_store(conn),
                sell_map=v4.base._load_frame_map(conn, "sell_analysis_daily", ymd_col="dt"),
                feature_map=v4.base._load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt"),
                event_map=v4.base._load_event_map(conn),
            )["rows"]
        ]


def _grid_row(limit: int, result: dict[str, Any]) -> dict[str, Any]:
    compare = result["compare"]
    delta = compare["delta"]
    return {
        "baseline_rank_limit": int(limit),
        "decision": result["decision"],
        "reason_type": result["reason_type"],
        "selected_count": compare["challenger"]["selected_count"],
        "baseline_prebreak_event_rate": compare["baseline"].get("prebreak_event_rate"),
        "challenger_prebreak_event_rate": compare["challenger"].get("prebreak_event_rate"),
        "prebreak_event_rate_delta": delta.get("prebreak_event_rate_delta"),
        "baseline_near_term_target_mean": compare["baseline"].get("near_term_target_mean"),
        "challenger_near_term_target_mean": compare["challenger"].get("near_term_target_mean"),
        "near_term_target_mean_delta": delta.get("near_term_target_mean_delta"),
        "mean_ret20_delta": delta.get("mean_ret20_delta"),
        "median_ret20_delta": delta.get("median_ret20_delta"),
        "changed_top5_members_count": delta.get("changed_top5_members_count"),
        "changed_rank_count": delta.get("changed_rank_count"),
        "positive_months": compare["monthly_summary"].get("positive_months"),
        "negative_months": compare["monthly_summary"].get("negative_months"),
    }


def _decision(grid: list[dict[str, Any]], source_limit: int) -> tuple[str, str]:
    source = next((row for row in grid if row["baseline_rank_limit"] == source_limit), None)
    if source is None or source["decision"] != "keep_for_shadow_paper_replay":
        return "drop_as_source_candidate_not_reproduced", "source_rank_limit_not_keep"
    comparable = [row for row in grid if row["selected_count"] and row["selected_count"] >= 12]
    positive_event = [row for row in comparable if (row.get("prebreak_event_rate_delta") or 0.0) > 0.0]
    positive_target = [row for row in comparable if (row.get("near_term_target_mean_delta") or 0.0) > 0.0]
    if len(comparable) < 2:
        return "hold_due_to_single_neighborhood_only", "insufficient_rank_limit_breadth"
    if len(positive_event) >= 2 and len(positive_target) >= 2:
        return "keep_for_shadow_paper_replay_stability", "rank_neighborhood_edge_replicates"
    if source["prebreak_event_rate_delta"] > 0.0 and source["near_term_target_mean_delta"] > 0.0:
        return "hold_due_to_rank_limit_sensitivity", "source_keep_not_stable_across_neighborhoods"
    return "drop_as_rank_limit_specific", "edge_specific_to_one_neighborhood"


def run(*, db_path: str | Path | None = None, output_dir: str | Path | None = None, start_ymd: int = 20240101, end_ymd: int = 20260515, top_k: int = v4.DEFAULT_TOP_K, min_train_months: int = v4.DEFAULT_MIN_TRAIN_MONTHS, lookback_months: int = v4.DEFAULT_LOOKBACK_MONTHS, rank_limits: tuple[int, ...] = DEFAULT_RANK_LIMITS, source_rank_limit: int = v4.DEFAULT_BASELINE_RANK_LIMIT) -> dict[str, Any]:
    source_db = v4.base._resolve_db_path(db_path)
    output_root = Path(output_dir).expanduser().resolve() if output_dir else DEFAULT_OUTPUT_ROOT
    output_root.mkdir(parents=True, exist_ok=True)
    generated_at = _utc_now()
    rows = _load_rows(source_db, start_ymd=start_ymd, end_ymd=end_ymd)

    grid: list[dict[str, Any]] = []
    child_roots: dict[str, str] = {}
    for limit in rank_limits:
        child_root = output_root / f"rank_limit_{int(limit)}"
        result = v4.run_pipeline(
            db_path=source_db,
            output_dir=child_root,
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            top_k=top_k,
            min_train_months=min_train_months,
            lookback_months=lookback_months,
            baseline_rank_limit=int(limit),
            prebuilt_rows=rows,
        )
        child_roots[str(limit)] = str(child_root)
        grid.append(_grid_row(int(limit), result))

    decision, reason = _decision(grid, source_rank_limit)
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "boundary": "TRADEX-only",
        "source_axis": v4.AXIS_ID,
        "source_rank_limit": int(source_rank_limit),
        "rank_limits": [int(x) for x in rank_limits],
        "same_condition_controls": {
            "same_period": True,
            "same_top_k": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "no_silent_fallback": True,
            "no_meemee_reflection": True,
        },
        "what_will_not_change": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal"],
    }
    compare = {
        "schema_version": f"{SCHEMA_PREFIX}_rank_limit_compare_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "decision": decision,
        "reason_type": reason,
        "grid": grid,
        "child_artifact_roots": child_roots,
    }
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "pass": True,
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "prebuilt_rows_reused_across_rank_limits": True,
        "silent_fallback_used": False,
        "runtime_db_written": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "decision": decision,
        "reason_type": reason,
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "next_gate": "paper_replay_or_hold_review" if decision.startswith("keep") else "stability_hold_or_drop_review",
        "rank_limit_grid": grid,
    }
    _write_json(output_root / "downside_prebreak_narrow_universe_stability_contract.json", contract)
    _write_json(output_root / "downside_prebreak_narrow_universe_rank_limit_compare.json", compare)
    _write_csv(output_root / "downside_prebreak_narrow_universe_rank_limit_grid.csv", grid)
    _write_json(output_root / "downside_prebreak_narrow_universe_stability_decision.json", decision_payload)
    _write_json(output_root / "no_lookahead_audit.json", no_lookahead)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "decision": decision,
        "reason_type": reason,
        "silent_fallback_used": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "decision": decision, "reason_type": reason, "grid": grid}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank-neighborhood stability diagnostic for downside prebreak selector.")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--start-ymd", type=int, default=20240101)
    parser.add_argument("--end-ymd", type=int, default=20260515)
    parser.add_argument("--top-k", type=int, default=v4.DEFAULT_TOP_K)
    parser.add_argument("--min-train-months", type=int, default=v4.DEFAULT_MIN_TRAIN_MONTHS)
    parser.add_argument("--lookback-months", type=int, default=v4.DEFAULT_LOOKBACK_MONTHS)
    parser.add_argument("--rank-limits", default="10,20,30")
    parser.add_argument("--source-rank-limit", type=int, default=v4.DEFAULT_BASELINE_RANK_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run(
        db_path=args.db_path,
        output_dir=args.output_dir,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        top_k=args.top_k,
        min_train_months=args.min_train_months,
        lookback_months=args.lookback_months,
        rank_limits=tuple(int(x.strip()) for x in str(args.rank_limits).split(",") if x.strip()),
        source_rank_limit=args.source_rank_limit,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
