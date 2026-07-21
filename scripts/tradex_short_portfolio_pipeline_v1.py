from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_blowoff_pipeline_v1 import (
    DEFAULT_CURRENT_ROOT as DEFAULT_BLOWOFF_CURRENT_ROOT,
    DEFAULT_SCREENSHOT_OUTPUT_ROOT,
    run as run_blowoff_pipeline,
)
from scripts.tradex_short_pattern_portfolio_rollup_v1 import (
    DEFAULT_MONTHLY_RECHECK_PIPELINE,
    DEFAULT_MONTHLY_RESEARCH,
    DEFAULT_OUTPUT_ROOT as DEFAULT_ROLLUP_ROOT,
    run as run_pattern_rollup,
)
from scripts.tradex_short_watch_to_entry_retest_probe_v1 import _default_db_path


AXIS_ID = "short_portfolio_pipeline_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_pattern_portfolio_rollup_v1\pipeline")
DEFAULT_BLOWOFF_OUTPUT_ROOT = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\pipeline")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ymd_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    text = str(int(value))
    if len(text) != 8:
        return None
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _stage_summary(path: Path, artifact_name: str) -> dict[str, Any]:
    artifact_path = path / artifact_name
    payload = _read_json(artifact_path)
    nested_stages = payload.get("stages") or {}
    current_scan = nested_stages.get("current_scan") or {}
    watch_board = nested_stages.get("watch_board") or {}
    return {
        "run_root": str(path),
        "artifact_path": str(artifact_path),
        "decision": payload.get("decision"),
        "row_count": payload.get("row_count"),
        "source_status": payload.get("source_status"),
        "stages": payload.get("stages"),
        "current_candidate_count": payload.get("current_candidate_count") or current_scan.get("current_candidate_count"),
        "watch_board_row_count": watch_board.get("row_count"),
        "blocked_near_miss_count": payload.get("blocked_near_miss_count") or current_scan.get("blocked_near_miss_count") or watch_board.get("blocked_near_miss_count"),
        "screenshot_plan": payload.get("screenshot_plan"),
        "production_ranking_changed": payload.get("production_ranking_changed"),
        "runtime_db_write": payload.get("runtime_db_write"),
        "meemee_unchanged": payload.get("meemee_unchanged"),
    }


def _portfolio_summary(rollup_dir: Path) -> dict[str, Any]:
    rollup_artifact = rollup_dir / "short_pattern_portfolio_rollup.json"
    payload = _read_json(rollup_artifact)
    rows = []
    for row in payload.get("rows", []):
        basis = row.get("basis") or {}
        policy = row.get("policy") or {}
        trigger = basis.get("trigger") or {}
        recheck = basis.get("recheck") or {}
        historical = basis.get("historical_same_status_stats") or {}
        historical_summary = historical.get("summary") or {}
        historical_score = historical.get("score") or {}
        stability = historical.get("stability") or {}
        source_bar = trigger.get("source_bar") or {}
        evaluated_bar = recheck.get("evaluated_bar") or {}
        source_close = source_bar.get("close")
        break_low = trigger.get("entry_review_trigger_break_low")
        close_below = trigger.get("entry_review_trigger_close_below")
        invalidate = trigger.get("invalidate_if_high_breaks")
        hard_invalidate = trigger.get("hard_invalidate_if_above")
        avg_ret5 = historical_summary.get("avg_ret5")
        avg_mae5 = historical_summary.get("avg_MAE5")
        avg_mfe5 = historical_summary.get("avg_MFE5")
        entry_reference = evaluated_bar.get("close") or close_below or break_low
        target_reference = None
        adverse_reference = None
        if entry_reference and avg_ret5 is not None:
            target_reference = round(float(entry_reference) * (1.0 + float(avg_ret5)), 2)
        if entry_reference and avg_mfe5 is not None:
            adverse_reference = round(float(entry_reference) * (1.0 + float(avg_mfe5)), 2)
        rows.append(
            {
                "code": row.get("code"),
                "display_name": row.get("display_name") or row.get("name"),
                "pattern_id": row.get("pattern_id"),
                "priority": row.get("priority"),
                "verdict": row.get("verdict"),
                "actionability": row.get("actionability"),
                "as_of": row.get("as_of"),
                "holding_horizon": policy.get("holding_horizon"),
                "target_policy": policy.get("target_policy"),
                "risk_policy": policy.get("risk_policy"),
                "trigger_status": basis.get("trigger_status") or recheck.get("status"),
                "visual_rank": basis.get("visual_rank"),
                "shape_family": basis.get("shape_family"),
                "ma_shape_family": basis.get("ma_shape_family"),
                "trigger_price": {
                    "break_low": trigger.get("entry_review_trigger_break_low"),
                    "close_below": trigger.get("entry_review_trigger_close_below"),
                    "invalidate_if_high_breaks": trigger.get("invalidate_if_high_breaks"),
                    "hard_invalidate_if_above": trigger.get("hard_invalidate_if_above"),
                },
                "source_bar": {
                    "ymd": source_bar.get("ymd"),
                    "source": source_bar.get("source"),
                    "close": source_bar.get("close"),
                    "high": source_bar.get("high"),
                    "low": source_bar.get("low"),
                },
                "evaluated_bar": {
                    "ymd": evaluated_bar.get("ymd"),
                    "source": evaluated_bar.get("source"),
                    "close": evaluated_bar.get("close"),
                    "high": evaluated_bar.get("high"),
                    "low": evaluated_bar.get("low"),
                },
                "historical_same_status": {
                    "n": historical_summary.get("n"),
                    "entry_now_rate": historical_summary.get("entry_now_rate"),
                    "watch_next_rate": historical_summary.get("watch_next_rate"),
                    "wrong_rate": historical_summary.get("wrong_rate"),
                    "avg_ret5": historical_summary.get("avg_ret5"),
                    "avg_MAE5": historical_summary.get("avg_MAE5"),
                    "avg_MFE5": historical_summary.get("avg_MFE5"),
                    "score": historical_score.get("score"),
                    "score_decision": historical_score.get("decision"),
                    "usable_year_count": stability.get("usable_year_count"),
                    "weak_year_count": stability.get("weak_year_count"),
                },
                "entry_management": {
                    "review_only": True,
                    "entry_style": "after_confirmed_downside_rejection_probe",
                    "entry_price_reference": entry_reference,
                    "trigger_break_low": break_low,
                    "trigger_close_below": close_below,
                    "target_policy": "5_session_probe_use_avg_ret5_and_manual_chart_review",
                    "target_price_reference_from_avg_ret5": target_reference,
                    "historical_avg_mae5_reference": avg_mae5,
                    "historical_avg_mfe5_reference": avg_mfe5,
                    "historical_adverse_price_reference_from_avg_mfe5": adverse_reference,
                    "stop_policy": "invalidate_on_trigger_high_break_or_hard_high_break",
                    "stop_price_reference": invalidate,
                    "hard_stop_price_reference": hard_invalidate,
                    "no_entry_if": [
                        "latest_confirmed_or_provisional_bar_reclaims_trigger_high",
                        "risk_to_stop_exceeds_expected_5_session_downside",
                        "manual_chart_review_rejects_monthly_daily_failure_shape",
                    ],
                    "take_profit_review_if": [
                        "price reaches target_price_reference_from_avg_ret5",
                        "5_sessions_elapsed_without_follow_through",
                        "price shows strong reclaim candle above prior breakdown area",
                    ],
                    "source_close_at_signal": source_close,
                },
            }
        )
    return {
        "review_only": True,
        "not_trade_signal": True,
        "row_count": len(rows),
        "summary_policy": {
            "purpose": "shortlist rows for manual chart review without changing MeeMee or production ranking",
            "ranking_key": "priority_then_code",
            "read_first": [
                "pattern_id",
                "trigger_status",
                "trigger_price",
                "historical_same_status",
                "risk_policy",
            ],
        },
        "rows": rows,
    }


def _screenshot_review_plan(portfolio_summary: dict[str, Any], screenshot_output_root: Path) -> dict[str, Any]:
    samples = []
    commands_by_stage = {"judgment": [], "result": []}
    for row in portfolio_summary.get("rows", []):
        code = row.get("code")
        source_ymd = (row.get("source_bar") or {}).get("ymd") or row.get("as_of")
        evaluated_ymd = (row.get("evaluated_bar") or {}).get("ymd")
        for stage, ymd in [("judgment", source_ymd), ("result", evaluated_ymd)]:
            as_of_iso = _ymd_to_iso(ymd)
            if not code or not as_of_iso:
                continue
            samples.append(
                {
                    "code": code,
                    "display_name": row.get("display_name"),
                    "stage": stage,
                    "as_of": int(ymd),
                    "as_of_iso": as_of_iso,
                    "purpose": "entry_judgment_chart" if stage == "judgment" else "post_judgment_result_chart",
                }
            )
            commands_by_stage[stage].append(f"{code}:{as_of_iso}")
    all_samples_arg = ",".join(f"{sample['code']}:{sample['as_of_iso']}" for sample in samples)
    return {
        "review_only": True,
        "requires_running_meemee": True,
        "clean_ui_route": "/detail-shot/:code",
        "centered_on_as_of": True,
        "center_lookback_months": 8,
        "center_lookahead_months": 3,
        "output_root": str(screenshot_output_root),
        "sample_count": len(samples),
        "samples": samples,
        "batch_command": (
            "node scripts/meemee_detail_clean_screenshot_batch_v1.mjs "
            f"--centered --samples {all_samples_arg}"
            if all_samples_arg
            else None
        ),
        "batch_commands_by_stage": {
            stage: (
                "node scripts/meemee_detail_clean_screenshot_batch_v1.mjs "
                f"--centered --samples {','.join(stage_samples)}"
                if stage_samples
                else None
            )
            for stage, stage_samples in commands_by_stage.items()
        },
    }


def run(
    *,
    db_path: Path,
    output_root: Path,
    blowoff_output_root: Path,
    blowoff_current_root: Path,
    rollup_root: Path,
    monthly_recheck_pipeline: Path,
    monthly_research: Path,
    screenshot_output_root: Path,
    start: str,
    end: str,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)

    blowoff_dir = run_blowoff_pipeline(
        db_path=db_path,
        output_root=blowoff_output_root,
        current_root=blowoff_current_root,
        screenshot_output_root=screenshot_output_root,
        start=start,
        end=end,
    )
    blowoff_artifact = blowoff_dir / "short_blowoff_pipeline.json"
    rollup_dir = run_pattern_rollup(
        db_path=db_path,
        blowoff_pipeline=blowoff_artifact,
        monthly_recheck_pipeline=monthly_recheck_pipeline,
        monthly_research=monthly_research,
        output_root=rollup_root,
    )

    stages = {
        "blowoff_pipeline": _stage_summary(blowoff_dir, "short_blowoff_pipeline.json"),
        "pattern_portfolio_rollup": _stage_summary(rollup_dir, "short_pattern_portfolio_rollup.json"),
    }
    portfolio_summary = _portfolio_summary(rollup_dir)
    screenshot_review_plan = _screenshot_review_plan(portfolio_summary, screenshot_output_root)
    rollup_decision = stages["pattern_portfolio_rollup"].get("decision") or {}
    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "stages": stages,
        "portfolio_summary": portfolio_summary,
        "screenshot_review_plan": screenshot_review_plan,
        "decision": {
            "candidate_local_decision": rollup_decision.get("candidate_local_decision", "pipeline_completed"),
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "pipeline refreshed blowoff current scan and aggregated short pattern portfolio rollup",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_json(output_dir / "short_portfolio_pipeline.json", report)
    _write_json(output_root / "latest_short_portfolio_pipeline.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--blowoff-output-root", type=Path, default=DEFAULT_BLOWOFF_OUTPUT_ROOT)
    parser.add_argument("--blowoff-current-root", type=Path, default=DEFAULT_BLOWOFF_CURRENT_ROOT)
    parser.add_argument("--rollup-root", type=Path, default=DEFAULT_ROLLUP_ROOT)
    parser.add_argument("--monthly-recheck-pipeline", type=Path, default=DEFAULT_MONTHLY_RECHECK_PIPELINE)
    parser.add_argument("--monthly-research", type=Path, default=DEFAULT_MONTHLY_RESEARCH)
    parser.add_argument("--screenshot-output-root", type=Path, default=DEFAULT_SCREENSHOT_OUTPUT_ROOT)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="latest")
    args = parser.parse_args()
    print(
        run(
            db_path=args.db_path,
            output_root=args.output_root,
            blowoff_output_root=args.blowoff_output_root,
            blowoff_current_root=args.blowoff_current_root,
            rollup_root=args.rollup_root,
            monthly_recheck_pipeline=args.monthly_recheck_pipeline,
            monthly_research=args.monthly_research,
            screenshot_output_root=args.screenshot_output_root,
            start=args.start,
            end=args.end,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
