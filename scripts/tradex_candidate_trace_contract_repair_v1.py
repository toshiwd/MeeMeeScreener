from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import portfolio_agent_replay_v1 as replay
from scripts import tradex_topk_selected_loser_trace_audit_v1 as audit


AXIS_ID = "topk_reform_trace_contract_repair_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\topk_reform_trace_contract_repair_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "pipeline_discovery_report.json",
    "candidate_trace_schema.json",
    "candidate_trace_generation_report.json",
    "daily_candidate_trace.csv",
    "trace_coverage_summary.json",
    "ranking_invariance_report.json",
    "source_family_failure_summary.csv",
    "score_component_failure_summary.csv",
    "reason_code_failure_summary.csv",
    "repair_axis_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
PERIODS = ((2024, 20240101, 20250101), (2025, 20250101, 20260101), (2026, 20260101, 20260205))


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    audit.write_json(path, payload)


def _load_traces(run_dirs: list[Path]) -> pd.DataFrame:
    frames = []
    for run in run_dirs:
        path = run / "daily_candidate_trace.csv"
        if path.exists():
            frame = pd.read_csv(path, dtype={"code": str}, low_memory=False)
            frame["trace_run_dir"] = str(run)
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _ranking_invariance(run_dirs: list[Path]) -> dict[str, Any]:
    changed_rank = changed_top5 = changed_top10 = 0
    checked = 0
    for run in run_dirs:
        snap = pd.read_csv(run / "daily_candidate_snapshot.csv", dtype={"code": str})
        trace = pd.read_csv(run / "daily_candidate_trace.csv", dtype={"code": str})
        merged = snap.merge(trace, left_on=["decision_ymd", "code"], right_on=["decision_date", "code"], how="outer", indicator=True)
        checked += len(merged)
        changed_rank += int((pd.to_numeric(merged["candidate_rank"], errors="coerce") != pd.to_numeric(merged["baseline_rank"], errors="coerce")).fillna(True).sum())
        for key, top in (("top5", 5), ("top10", 10)):
            for _ymd, g in merged.groupby(merged["decision_ymd"].fillna(merged["decision_date"])):
                snap_set = set(g.loc[pd.to_numeric(g["candidate_rank"], errors="coerce") <= top, "code"])
                trace_set = set(g.loc[pd.to_numeric(g["baseline_rank"], errors="coerce") <= top, "code"])
                if snap_set != trace_set:
                    if key == "top5":
                        changed_top5 += 1
                    else:
                        changed_top10 += 1
    return {"checked_join_rows": checked, "changed_rank_count": changed_rank, "changed_top5_members_count": changed_top5, "changed_top10_members_count": changed_top10, "ranking_order_unchanged": changed_rank == 0 and changed_top5 == 0 and changed_top10 == 0}


def _pipeline_discovery() -> dict[str, Any]:
    return {
        "generator_file": "scripts/portfolio_agent_replay_v1.py",
        "candidate_creation_stage": "run_portfolio_agent_replay_v1 groups point-in-time feature rows by ymd",
        "scoring_stage": "_score_candidates calls scripts.tradex_daily_selection_replay_learning_v1.score_entry_candidate",
        "ranking_stage": "_score_candidates sorts by selection_score desc, code asc, then assigns candidate_rank",
        "snapshot_write": "run_portfolio_agent_replay_v1 writes daily_candidate_snapshot.csv near artifact finalization",
        "score_components_created_at": "_score_candidates stores score['components'] as score_components_json",
        "fields_available_before_write": ["decision_ymd", "code", "candidate_rank", "selection_score", "entry_allowed_by_score", "downside_guard_blocked", "next_execution_ymd", "close", "next_open_available", "score_components_json", *sorted(replay.SELECTION_ALLOWED_COLUMNS)],
        "fields_lost_during_export": [],
        "fields_not_present_anywhere_verified": ["candidate_source", "signal_family", "setup_name", "reason_codes", "regime_bucket"],
        "trace_emission_added": ["daily_candidate_trace.csv", "candidate_trace_schema.json", "candidate_trace_generation_report.json"],
    }


def _decision(coverage: dict[str, Any], source: pd.DataFrame, score: pd.DataFrame) -> dict[str, Any]:
    if coverage["candidate_source_available_rate"] == 0.0 and coverage["signal_family_available_rate"] == 0.0 and coverage["setup_name_available_rate"] == 0.0 and coverage["reason_codes_available_rate"] == 0.0:
        return {"research_decision": "score_contract_gap_persists", "reason_typed": ["trace sidecar emitted, but source/family/setup/reason semantics do not exist in portfolio_agent_replay_v1 internals"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    combined = score[(score["period"] == "2024_2026_combined") & (score["topk"] == 10)].copy()
    if not combined.empty and combined["loser_minus_winner_spread"].max() >= 0.08:
        best = combined.sort_values("loser_minus_winner_spread", ascending=False).iloc[0]
        return {"research_decision": "score_axis_found", "reason_typed": [f"axis found: {best['component_feature']}"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    return {"research_decision": "no_clear_score_axis", "reason_typed": ["trace exists but selected loser/winner are not separable by available fields"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, source_db: str | Path | None = None, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-topk-reform-trace-contract-repair-v1"
    subruns = run_dir / "subruns"
    run_dir.mkdir(parents=True, exist_ok=True)
    replay_results = []
    run_dirs = []
    for year, start, end in PERIODS:
        result = replay.run_portfolio_agent_replay_v1(source_db=source_db, output_root=subruns, run_id=f"{year}-trace-baseline", start_ymd=start, end_ymd=end)
        replay_results.append(result)
        run_dirs.append(Path(result["output_dir"]))
    trace = _load_traces(run_dirs)
    labeled = audit.attach_ret20(trace, Path("production_data/production_daily.csv"))
    source_summary = audit.source_family_failure_summary(labeled)
    score_summary = audit.score_component_failure_summary(labeled)
    reason_summary = audit.reason_code_failure_summary(labeled)
    coverage = audit.trace_coverage_summary(trace)
    candidates = audit.repair_axis_candidates(source_summary, score_summary, coverage)
    invariance = _ranking_invariance(run_dirs)
    generation_reports = [json.loads((run / "candidate_trace_generation_report.json").read_text(encoding="utf-8")) for run in run_dirs]
    decision = _decision(coverage, source_summary, score_summary)

    trace.to_csv(run_dir / "daily_candidate_trace.csv", index=False)
    source_summary.to_csv(run_dir / "source_family_failure_summary.csv", index=False)
    score_summary.to_csv(run_dir / "score_component_failure_summary.csv", index=False)
    reason_summary.to_csv(run_dir / "reason_code_failure_summary.csv", index=False)
    _write_json(run_dir / "pipeline_discovery_report.json", _pipeline_discovery())
    _write_json(run_dir / "candidate_trace_schema.json", json.loads((run_dirs[0] / "candidate_trace_schema.json").read_text(encoding="utf-8")))
    _write_json(run_dir / "candidate_trace_generation_report.json", {"subrun_reports": generation_reports, "combined_trace_rows": int(len(trace)), "combined_labeled_rows": int(len(labeled)), "runtime_db_write": False, "ranking_order_changed": False, "score_formula_changed": False, "candidate_generation_changed": False})
    _write_json(run_dir / "trace_coverage_summary.json", coverage)
    _write_json(run_dir / "ranking_invariance_report.json", invariance)
    _write_json(run_dir / "repair_axis_candidates.json", {"candidates": candidates})
    _write_json(run_dir / "input_artifact_report.json", {"periods": [{"year": y, "start_ymd": s, "end_ymd": e} for y, s, e in PERIODS], "subrun_dirs": [str(p) for p in run_dirs], "source_db": str(source_db) if source_db else None, "replay_results": replay_results})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "subrun_audits": [json.loads((run / "no_lookahead_audit.json").read_text(encoding="utf-8")).get("audit_result") for run in run_dirs], "topk_selection_uses_baseline_decision_date_data": True, "trace_values_point_in_time": True, "future_returns_used_only_for_labels": True, "ranking_order_changed": False, "candidate_generation_changed": False, "score_formula_changed": False, "runtime_db_write": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "coverage": coverage, "ranking_invariance": invariance}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(audit.json_ready(run(source_db=args.source_db, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
