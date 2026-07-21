from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_three_window_side_rule_probe_v1 import _default_db_path


AXIS_ID = "short_pattern_portfolio_rollup_v1"
DEFAULT_BLOWOFF_PIPELINE = Path(
    r"G:\Tradex\short_watch_to_entry_retest_probe_v1\pipeline\latest_short_blowoff_pipeline.json"
)
DEFAULT_MONTHLY_RECHECK_PIPELINE = Path(
    r"G:\Tradex\short_entry_shape_family_probe_v1\final_shortlist_recheck\latest_final_shortlist_recheck.json"
)
DEFAULT_MONTHLY_RESEARCH = Path(
    r"G:\Tradex\short_entry_shape_family_probe_v1\latest_short_entry_shape_family_probe.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_pattern_portfolio_rollup_v1")

PATTERN_POLICIES = {
    "high_zone_peak_failure_short": {
        "holding_horizon": "20_to_30_sessions",
        "target_policy": "down_5pct_first_touch",
        "risk_policy": "strict_core_peak_high_plus_0.5pct_or_n20_peak_high_plus_1.0pct",
        "evaluation_basis": "target_first_vs_stop_first_model",
        "comparison_note": "high-confidence but rare setup; do not compare directly to 5-session probe rates",
    },
    "monthly_failure_daily_rejection_short": {
        "holding_horizon": "5_sessions_probe",
        "target_policy": "short_term_downside_expectancy",
        "risk_policy": "candidate_high_break_invalidation_with_hard_high_buffer",
        "evaluation_basis": "entry_now_rate_wrong_rate_avg_ret5",
        "comparison_note": "higher-frequency probe setup; judged by wrong-rate control and 5-session average return",
    },
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _blowoff_rows(path: Path) -> list[dict[str, Any]]:
    payload = _read_json(path)
    board_stage = payload.get("stages", {}).get("watch_board", {})
    artifact_path = board_stage.get("artifact_path")
    if not artifact_path:
        return []
    board = _read_json(Path(artifact_path))
    rows = []
    for row in board.get("rows", []):
        rows.append(
            {
                "pattern_id": "high_zone_peak_failure_short",
                "pattern_label": "高値圏ピーク失速ショート",
                "code": row.get("code"),
                "name": row.get("name"),
                "as_of": row.get("entry_date"),
                "priority": 95 if any(rule.get("profile") == "strict_core" for rule in row.get("matched_rules", [])) else 85,
                "verdict": row.get("board_verdict"),
                "actionability": "review_only",
                "policy": PATTERN_POLICIES["high_zone_peak_failure_short"],
                "source_artifact": artifact_path,
                "basis": {
                    "rr_to_low20": row.get("rr_to_low20"),
                    "entry_vs_ma7_pct": row.get("entry_vs_ma7_pct"),
                    "entry_vs_high60_pct": row.get("entry_vs_high60_pct"),
                    "matched_rules": row.get("matched_rules", []),
                },
            }
        )
    return rows


def _monthly_research_stats(path: Path) -> dict[tuple[str, str, str], dict[str, Any]]:
    payload = _read_json(path)
    out = {}
    for row in payload.get("expectancy_leaderboard", []):
        key = (
            str(row.get("shape_family") or ""),
            str(row.get("ma_shape_family") or ""),
            str(row.get("trigger_status") or ""),
        )
        out[key] = {
            "summary": row.get("summary"),
            "score": row.get("score"),
            "stability": row.get("stability"),
        }
    return out


def _monthly_rows(path: Path, research_path: Path) -> list[dict[str, Any]]:
    payload = _read_json(path)
    research_stats = _monthly_research_stats(research_path)
    rows = []
    source_rows = payload.get("keep_strong_rows")
    if source_rows is None:
        source_rows = [
            row
            for row in payload.get("rows", [])
            if row.get("candidate_local_decision") == "keep_strong_review"
        ]
    for row in source_rows:
        trigger = row.get("trigger", {})
        recheck = row.get("recheck", {})
        shape_family = str(row.get("shape_family") or "mixed_or_late")
        ma_shape_family = str(row.get("ma_shape_family") or "ma_not_bearish_enough")
        trigger_status = str(recheck.get("status") or "")
        historical_key = (shape_family, ma_shape_family, trigger_status)
        rows.append(
            {
                "pattern_id": "monthly_failure_daily_rejection_short",
                "pattern_label": "月足失速・日足下抜け確認ショート",
                "code": row.get("code"),
                "name": row.get("name"),
                "as_of": trigger.get("as_of"),
                "priority": 80,
                "verdict": row.get("candidate_local_decision"),
                "actionability": "review_only",
                "policy": PATTERN_POLICIES["monthly_failure_daily_rejection_short"],
                "source_artifact": str(path),
                "basis": {
                    "visual_rank": row.get("visual_rank"),
                    "visual_decision": row.get("visual_decision"),
                    "shape_family": shape_family,
                    "ma_shape_family": ma_shape_family,
                    "trigger_status": trigger_status,
                    "historical_same_status_stats": research_stats.get(historical_key),
                    "trigger": trigger,
                    "recheck": recheck,
                },
            }
        )
    return rows


def _name_map(db_path: Path, codes: list[str]) -> dict[str, str]:
    if not codes:
        return {}
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT code, name FROM tickers WHERE code IN (" + ",".join(["?"] * len(codes)) + ")",
            codes,
        ).fetchall()
    finally:
        con.close()
    return {str(code): str(name) for code, name in rows if name}


def _attach_display_names(rows: list[dict[str, Any]], db_path: Path) -> None:
    names = _name_map(db_path, sorted({str(row.get("code")) for row in rows if row.get("code")}))
    for row in rows:
        code = str(row.get("code") or "")
        if code in names:
            row["display_name"] = names[code]
        else:
            row["display_name"] = row.get("name")


def run(*, db_path: Path, blowoff_pipeline: Path, monthly_recheck_pipeline: Path, monthly_research: Path, output_root: Path) -> Path:
    rows = []
    source_status = []
    for label, path, loader in [
        ("blowoff_pipeline", blowoff_pipeline, _blowoff_rows),
    ]:
        try:
            loaded = loader(path)
            rows.extend(loaded)
            source_status.append({"source": label, "path": str(path), "status": "loaded", "row_count": len(loaded)})
        except FileNotFoundError:
            source_status.append({"source": label, "path": str(path), "status": "missing", "row_count": 0})
    try:
        loaded = _monthly_rows(monthly_recheck_pipeline, monthly_research)
        rows.extend(loaded)
        source_status.append(
            {"source": "monthly_recheck_pipeline", "path": str(monthly_recheck_pipeline), "status": "loaded", "row_count": len(loaded)}
        )
        source_status.append(
            {"source": "monthly_research", "path": str(monthly_research), "status": "loaded", "row_count": None}
        )
    except FileNotFoundError as error:
        source_status.append(
            {"source": "monthly_recheck_pipeline_or_research", "path": str(error.filename), "status": "missing", "row_count": 0}
        )
    _attach_display_names(rows, db_path)
    rows.sort(key=lambda row: (row["priority"], str(row.get("code") or "")), reverse=True)
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "source_status": source_status,
        "pattern_policies": PATTERN_POLICIES,
        "comparison_policy": {
            "ranking_key": "priority_then_code",
            "do_not_compare_raw_win_rates_across_different_horizons": True,
            "review_only": True,
        },
        "row_count": len(rows),
        "rows": rows,
        "decision": {
            "candidate_local_decision": "review_only_short_patterns_present" if rows else "no_review_short_pattern",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "short pattern review rows are aggregated without changing MeeMee or production ranking",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    _write_json(output_dir / "short_pattern_portfolio_rollup.json", report)
    _write_json(output_root / "latest_short_pattern_portfolio_rollup.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--blowoff-pipeline", type=Path, default=DEFAULT_BLOWOFF_PIPELINE)
    parser.add_argument("--monthly-recheck-pipeline", type=Path, default=DEFAULT_MONTHLY_RECHECK_PIPELINE)
    parser.add_argument("--monthly-research", type=Path, default=DEFAULT_MONTHLY_RESEARCH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(
        run(
            blowoff_pipeline=args.blowoff_pipeline,
            db_path=args.db_path,
            monthly_recheck_pipeline=args.monthly_recheck_pipeline,
            monthly_research=args.monthly_research,
            output_root=args.output_root,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
