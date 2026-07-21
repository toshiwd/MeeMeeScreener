from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_current_summary_v1 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_CURRENT_SUMMARY_ROOT,
    run as run_current_summary,
)
from scripts.tradex_short_multi_pattern_compare_v1 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_MULTI_COMPARE_ROOT,
    run as run_multi_compare,
)
from scripts.tradex_short_portfolio_pipeline_v1 import (
    DEFAULT_BLOWOFF_CURRENT_ROOT,
    DEFAULT_BLOWOFF_OUTPUT_ROOT,
    DEFAULT_MONTHLY_RECHECK_PIPELINE,
    DEFAULT_MONTHLY_RESEARCH,
    DEFAULT_OUTPUT_ROOT as DEFAULT_PORTFOLIO_PIPELINE_ROOT,
    DEFAULT_ROLLUP_ROOT,
    DEFAULT_SCREENSHOT_OUTPUT_ROOT,
    run as run_portfolio_pipeline,
)
from scripts.tradex_short_pattern_portfolio_rollup_v1 import DEFAULT_OUTPUT_ROOT as DEFAULT_PORTFOLIO_ROLLUP_ROOT
from scripts.tradex_short_watch_to_entry_retest_probe_v1 import _default_db_path
from scripts.tradex_runtime_freshness_guard_v1 import (
    DEFAULT_MAX_STALE_CALENDAR_DAYS,
    build_runtime_freshness_guard,
)
from scripts.tradex_meemee_update_client_v1 import (
    DEFAULT_BASE_URL as DEFAULT_MEEMEE_BASE_URL,
    submit_txt_update,
    wait_for_job,
)
from scripts.tradex_short_visual_review_pack_v1 import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_VISUAL_REVIEW_ROOT,
    run as run_visual_review_pack,
)


AXIS_ID = "short_operational_pipeline_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_operational_pipeline_v1")
DEFAULT_SHAPE_FAMILY = Path(r"G:\Tradex\short_entry_shape_family_probe_v1\latest_short_entry_shape_family_probe.json")
DEFAULT_BLOWOFF_RESEARCH = Path(r"G:\Tradex\short_watch_to_entry_retest_probe_v1\latest_short_watch_to_entry_retest_report.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def run(
    *,
    db_path: Path,
    output_root: Path,
    portfolio_pipeline_root: Path,
    multi_compare_root: Path,
    current_summary_root: Path,
    visual_review_root: Path,
    shape_family: Path,
    blowoff_research: Path,
    start: str,
    end: str,
    require_fresh_within_days: int | None,
    min_confirmed_date: str | None,
    require_expected_latest: bool,
    submit_update_if_stale: bool,
    meemee_base_url: str,
    update_wait_seconds: int,
    today: date | None,
) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)

    runtime_freshness_guard = None
    if require_fresh_within_days is not None:
        runtime_freshness_guard = build_runtime_freshness_guard(
            db_path=db_path,
            max_stale_calendar_days=require_fresh_within_days,
            min_confirmed_date=min_confirmed_date,
            require_expected_latest=require_expected_latest,
            today=today,
        )
        if not runtime_freshness_guard["pass"]:
            update_submission = None
            update_wait = None
            if submit_update_if_stale:
                update_submission = submit_txt_update(base_url=meemee_base_url)
                job_id = update_submission.get("job_id")
                if job_id:
                    update_wait = wait_for_job(
                        job_id=str(job_id),
                        base_url=meemee_base_url,
                        wait_seconds=update_wait_seconds,
                    )
            report = {
                "schema_version": f"{AXIS_ID}_freshness_gate_report_v1",
                "generated_at": _utc_now(),
                "axis_id": AXIS_ID,
                "boundary_owner": "TRADEX",
                "db_path": str(db_path),
                "runtime_freshness_guard": runtime_freshness_guard,
                "stale_update_action": {
                    "requested": submit_update_if_stale,
                    "meemee_base_url": meemee_base_url,
                    "submission": update_submission,
                    "wait": update_wait,
                    "rerun_after_update_command": "python scripts\\tradex_short_operational_pipeline_v1.py",
                },
                "decision": {
                    "candidate_local_decision": (
                        "update_submitted_then_aborted_before_candidate_generation"
                        if update_submission and update_submission.get("ok")
                        else "aborted_before_candidate_generation"
                    ),
                    "authoritative_rollup_decision": "research_candidate_not_trade_signal",
                    "reason": "runtime confirmed daily bars are not fresh enough for latest selection",
                },
                "latest_selection_rule": {
                    "enabled": True,
                    "confirmed_source": "pan",
                    "max_stale_calendar_days": require_fresh_within_days,
                    "min_confirmed_date": min_confirmed_date,
                    "today": today.isoformat() if today else None,
                    "require_expected_latest": require_expected_latest,
                    "failure_policy": "abort_before_candidate_generation",
                    "update_owner": "MeeMee",
                },
                "production_ranking_changed": False,
                "runtime_db_write": False,
                "meemee_unchanged": True,
            }
            _write_json(output_dir / "freshness_gate_failed.json", report)
            _write_json(output_root / "latest_short_operational_pipeline.json", {"run_root": str(output_dir), **report})
            raise RuntimeError(
                "runtime freshness gate failed: "
                f"confirmed_max_date={runtime_freshness_guard['confirmed_max_date']}, "
                f"expected_latest_confirmed_date={runtime_freshness_guard['expected_latest_confirmed_date']}, "
                f"reasons={','.join(runtime_freshness_guard['failure_reasons'])}; "
                + (
                    "MeeMee txt update submitted; rerun after update finishes"
                    if update_submission and update_submission.get("ok")
                    else "run MeeMee txt update before TRADEX selection"
                )
            )

    portfolio_dir = run_portfolio_pipeline(
        db_path=db_path,
        output_root=portfolio_pipeline_root,
        blowoff_output_root=DEFAULT_BLOWOFF_OUTPUT_ROOT,
        blowoff_current_root=DEFAULT_BLOWOFF_CURRENT_ROOT,
        rollup_root=DEFAULT_ROLLUP_ROOT,
        monthly_recheck_pipeline=DEFAULT_MONTHLY_RECHECK_PIPELINE,
        monthly_research=DEFAULT_MONTHLY_RESEARCH,
        screenshot_output_root=DEFAULT_SCREENSHOT_OUTPUT_ROOT,
        start=start,
        end=end,
    )
    portfolio_artifact = portfolio_dir / "short_portfolio_pipeline.json"

    multi_dir = run_multi_compare(
        shape_family=shape_family,
        blowoff=blowoff_research,
        portfolio=portfolio_artifact,
        output_root=multi_compare_root,
    )
    multi_artifact = multi_dir / "short_multi_pattern_compare.json"

    current_dir = run_current_summary(
        portfolio=portfolio_artifact,
        multi_compare=multi_artifact,
        output_root=current_summary_root,
    )
    current_artifact = current_dir / "short_current_summary.json"
    current_payload = _read_json(current_artifact)

    report: dict[str, Any] = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "runtime_freshness_guard": runtime_freshness_guard,
        "stages": {
            "portfolio_pipeline": {
                "run_root": str(portfolio_dir),
                "artifact_path": str(portfolio_artifact),
            },
            "multi_pattern_compare": {
                "run_root": str(multi_dir),
                "artifact_path": str(multi_artifact),
            },
            "current_summary": {
                "run_root": str(current_dir),
                "artifact_path": str(current_artifact),
                "row_count": current_payload.get("row_count"),
                "review_short_probe_count": current_payload.get("review_short_probe_count"),
                "decision": current_payload.get("decision"),
            },
        },
        "current_summary_rows": current_payload.get("rows", []),
        "screenshot_batch_command": current_payload.get("screenshot_batch_command"),
        "decision": {
            "candidate_local_decision": current_payload.get("decision", {}).get("candidate_local_decision", "pipeline_completed"),
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "portfolio, multi-pattern compare, and current short summary generated in fixed order",
        },
        "latest_selection_rule": {
            "enabled": require_fresh_within_days is not None,
            "confirmed_source": "pan",
            "max_stale_calendar_days": require_fresh_within_days,
            "min_confirmed_date": min_confirmed_date,
            "today": today.isoformat() if today else None,
            "require_expected_latest": require_expected_latest,
            "failure_policy": "abort_before_candidate_generation",
            "update_owner": "MeeMee",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    operational_artifact = output_dir / "short_operational_pipeline.json"
    _write_json(operational_artifact, report)

    visual_review_dir = run_visual_review_pack(
        operational_path=operational_artifact,
        output_root=visual_review_root,
        decisions_path=None,
        screenshot_manifest_path=None,
    )
    visual_review_artifact = visual_review_dir / "short_visual_review_pack.json"
    visual_review_payload = _read_json(visual_review_artifact)
    report["stages"]["visual_review_pack"] = {
        "run_root": str(visual_review_dir),
        "artifact_path": str(visual_review_artifact),
        "template_row_count": visual_review_payload.get("template_row_count"),
        "decision_row_count": visual_review_payload.get("decision_row_count"),
        "decision": visual_review_payload.get("decision"),
    }
    _write_json(output_dir / "short_operational_pipeline.json", report)
    _write_json(output_root / "latest_short_operational_pipeline.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=_default_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--portfolio-pipeline-root", type=Path, default=DEFAULT_PORTFOLIO_PIPELINE_ROOT)
    parser.add_argument("--multi-compare-root", type=Path, default=DEFAULT_MULTI_COMPARE_ROOT)
    parser.add_argument("--current-summary-root", type=Path, default=DEFAULT_CURRENT_SUMMARY_ROOT)
    parser.add_argument("--visual-review-root", type=Path, default=DEFAULT_VISUAL_REVIEW_ROOT)
    parser.add_argument("--shape-family", type=Path, default=DEFAULT_SHAPE_FAMILY)
    parser.add_argument("--blowoff-research", type=Path, default=DEFAULT_BLOWOFF_RESEARCH)
    parser.add_argument("--start", default="2018-01-01")
    parser.add_argument("--end", default="latest")
    parser.add_argument(
        "--require-fresh-within-days",
        type=int,
        default=DEFAULT_MAX_STALE_CALENDAR_DAYS,
        help="Abort before selection if confirmed pan daily_bars are older than this many calendar days. Use -1 to disable.",
    )
    parser.add_argument(
        "--min-confirmed-date",
        default=None,
        help="Abort before selection if confirmed pan daily_bars max date is before YYYY-MM-DD.",
    )
    parser.add_argument(
        "--today",
        default=None,
        help="Use this YYYY-MM-DD as the freshness reference date instead of the OS date.",
    )
    parser.add_argument(
        "--skip-expected-latest",
        action="store_true",
        help="Do not require confirmed pan daily_bars to reach the previous JPX/TSE business day. Use only for backtest exceptions.",
    )
    parser.add_argument(
        "--submit-update-if-stale",
        action="store_true",
        help="If freshness fails, submit MeeMee txt update through the local backend API before aborting.",
    )
    parser.add_argument("--meemee-base-url", default=DEFAULT_MEEMEE_BASE_URL)
    parser.add_argument(
        "--update-wait-seconds",
        type=int,
        default=0,
        help="After submitting MeeMee txt update, wait this many seconds for the job before aborting.",
    )
    args = parser.parse_args()
    require_fresh_within_days = None if args.require_fresh_within_days < 0 else args.require_fresh_within_days
    today = date.fromisoformat(args.today) if args.today else None
    print(
        run(
            db_path=args.db_path,
            output_root=args.output_root,
            portfolio_pipeline_root=args.portfolio_pipeline_root,
            multi_compare_root=args.multi_compare_root,
            current_summary_root=args.current_summary_root,
            visual_review_root=args.visual_review_root,
            shape_family=args.shape_family,
            blowoff_research=args.blowoff_research,
            start=args.start,
            end=args.end,
            require_fresh_within_days=require_fresh_within_days,
            min_confirmed_date=args.min_confirmed_date,
            require_expected_latest=not args.skip_expected_latest,
            submit_update_if_stale=args.submit_update_if_stale,
            meemee_base_url=args.meemee_base_url,
            update_wait_seconds=args.update_wait_seconds,
            today=today,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
