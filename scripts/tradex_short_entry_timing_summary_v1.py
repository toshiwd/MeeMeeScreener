from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_entry_timing_summary_v1"
DEFAULT_PIPELINE = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\pipeline\latest_short_entry_timing_pipeline.json")
DEFAULT_TRIGGER_BOARD = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates\latest_provisional_trigger_board.json")
DEFAULT_RECHECK = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\current_candidates\latest_trigger_recheck.json")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_entry_timing_rule_probe_v1\summary")
NAME_OVERRIDES = {
    "2471": "エスプール",
    "2897": "日清食品HD",
    "3661": "エムアップHD",
    "3989": "シェアリングテクノロジー",
    "4168": "ヤプリ",
    "8876": "リログループ",
    "9301": "三菱倉庫",
    "9468": "KADOKAWA",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def _row_summary(row: dict[str, Any], recheck_by_code: dict[str, dict[str, Any]]) -> dict[str, Any]:
    code = str(row.get("code")).strip()
    plan = row.get("trigger_plan", {})
    bar = row.get("provisional_bar", {})
    recheck = recheck_by_code.get(code, {}).get("recheck", {})
    rules = row.get("matched_rules", [])
    return {
        "code": code,
        "name": NAME_OVERRIDES.get(code, row.get("name")),
        "verdict": row.get("provisional_board_verdict"),
        "status": recheck.get("trigger_status", "not_rechecked"),
        "bar_source": bar.get("source"),
        "as_of": bar.get("ymd"),
        "close": bar.get("close"),
        "break_low": plan.get("entry_review_trigger_break_low"),
        "close_below": plan.get("entry_review_trigger_close_below"),
        "invalid_high": plan.get("invalidate_if_high_breaks"),
        "hard_invalid": plan.get("hard_invalidate_if_above"),
        "rule_count": len(rules),
        "primary_rule": any(rule.get("review_strength") == "primary" for rule in rules),
        "best_oos_entry_now_rate": max((rule.get("oos_reference", {}).get("entry_now_rate", 0.0) for rule in rules), default=None),
        "best_oos_wrong_rate": min((rule.get("oos_reference", {}).get("wrong_rate", 1.0) for rule in rules), default=None),
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Short Entry Timing Watch Summary",
        "",
        f"- generated_at: `{report['generated_at']}`",
        f"- decision: `{report['decision']['candidate_local_decision']}`",
        f"- source: `{report['source_trigger_board']}`",
        "",
        "| code | name | verdict | status | close | break low | close <= | invalid > | rules |",
        "|---|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in report["rows"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    _fmt(row["code"]),
                    _fmt(row["name"]),
                    _fmt(row["verdict"]),
                    _fmt(row["status"]),
                    _fmt(row["close"]),
                    _fmt(row["break_low"]),
                    _fmt(row["close_below"]),
                    _fmt(row["invalid_high"]),
                    _fmt(row["rule_count"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- This is TRADEX research-only.",
            "- It is not a MeeMee ranking change and not a trade signal.",
            "- `waiting_next_bar` means no bar exists after the provisional trigger bar in the current DB.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(*, pipeline_path: Path, trigger_board_path: Path, recheck_path: Path, output_root: Path) -> Path:
    pipeline = _read_json(pipeline_path)
    trigger_board = _read_json(trigger_board_path)
    recheck = _read_json(recheck_path)
    recheck_by_code = {str(row.get("code")): row for row in recheck.get("rows", [])}
    rows = [_row_summary(row, recheck_by_code) for row in trigger_board.get("rows", [])]
    rows.sort(
        key=lambda row: (
            row["status"] != "triggered_strong_rejection",
            row["status"] != "waiting_next_bar",
            not row["primary_rule"],
            -row["rule_count"],
            row["code"],
        )
    )
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "source_pipeline": str(pipeline_path),
        "source_trigger_board": str(trigger_board_path),
        "source_recheck": str(recheck_path),
        "pipeline_decision": pipeline.get("decision"),
        "status_counts": recheck.get("status_counts"),
        "rows": rows,
        "decision": {
            "candidate_local_decision": pipeline.get("decision", {}).get("candidate_local_decision", "summary_generated"),
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "summary generated from authoritative latest JSON artifacts",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "short_entry_timing_summary.json", report)
    (output_dir / "short_entry_timing_summary.md").write_text(_markdown(report), encoding="utf-8")
    _write_json(output_root / "latest_short_entry_timing_summary.json", {"run_root": str(output_dir), **report})
    (output_root / "latest_short_entry_timing_summary.md").write_text(_markdown(report), encoding="utf-8")
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", type=Path, default=DEFAULT_PIPELINE)
    parser.add_argument("--trigger-board", type=Path, default=DEFAULT_TRIGGER_BOARD)
    parser.add_argument("--recheck", type=Path, default=DEFAULT_RECHECK)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(pipeline_path=args.pipeline, trigger_board_path=args.trigger_board, recheck_path=args.recheck, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
