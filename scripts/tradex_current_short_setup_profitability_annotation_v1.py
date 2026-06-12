from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "current_short_setup_profitability_annotation_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_short_setup_profitability_annotation_v1")
DEFAULT_WATCH_BOARD = Path(
    r"G:\Tradex\current_short_visual_setup_watch_board_v1"
    r"\20260605T023329Z-current_short_visual_setup_watch_board_v1"
    r"\current_short_visual_setup_watch_board.json"
)
DEFAULT_PROFITABILITY_AUDIT = Path(
    r"G:\Tradex\short_setup_profitability_audit_v1"
    r"\20260605T023906Z-short_setup_profitability_audit_v1"
    r"\short_setup_profitability_audit.json"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _policy_lookup(audit: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["policy_id"]): row for row in audit.get("policy_summaries", [])}


def _annotation(row: dict[str, Any], policies: dict[str, dict[str, Any]]) -> dict[str, Any]:
    immediate = policies.get("setup_ready_immediate_next_open", {})
    confirmed = policies.get("setup_ready_then_continuation_permit", {})
    skipped = policies.get("setup_ready_skipped_no_permit", {})
    if row.get("setup_state") != "SetupReady":
        return {
            "setup_profitability_scope": "not_setup_ready",
            "expected_use": "do_not_use_as_setup_profitability_candidate",
        }
    continuation = str(row.get("continuation_status") or "")
    if continuation == "ContinuationPermit":
        expected_use = "confirmed_continuation_review_candidate"
        stat = confirmed
    elif continuation == "ContinuationPending":
        expected_use = "watch_until_continuation_window_available"
        stat = confirmed
    else:
        expected_use = "skip_if_no_continuation_permit"
        stat = skipped
    return {
        "setup_profitability_scope": "SetupReady",
        "expected_use": expected_use,
        "setup_immediate_mean_short_ret": immediate.get("mean_short_ret"),
        "setup_immediate_win_rate": immediate.get("win_rate"),
        "setup_immediate_stop_hit_rate": immediate.get("stop_hit_rate"),
        "setup_confirmed_mean_short_ret": confirmed.get("mean_short_ret"),
        "setup_confirmed_win_rate": confirmed.get("win_rate"),
        "setup_confirmed_p_mfe_ge_6pct": confirmed.get("p_mfe_ge_6pct"),
        "setup_confirmed_stop_hit_rate": confirmed.get("stop_hit_rate"),
        "setup_skipped_mean_short_ret": skipped.get("mean_short_ret"),
        "selected_policy_reference_mean_short_ret": stat.get("mean_short_ret"),
        "selected_policy_reference_win_rate": stat.get("win_rate"),
        "selected_policy_reference_stop_hit_rate": stat.get("stop_hit_rate"),
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Short Setup Profitability Annotation v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- watch_board_path: `{payload['watch_board_path']}`",
        f"- profitability_audit_path: `{payload['profitability_audit_path']}`",
        "",
        "## SetupReady Candidates",
        "",
        "| watch_rank | code | signal | continuation | expected_use | confirmed_mean | confirmed_win | confirmed_stop | skipped_mean |",
        "|---:|---|---:|---|---|---:|---:|---:|---:|",
    ]
    for row in [item for item in payload["candidates"] if item.get("setup_state") == "SetupReady"]:
        lines.append(
            f"| {row['watch_rank']} | {row['code']} | {row['signal_ymd']} | {row.get('continuation_status')} | "
            f"{row['expected_use']} | {float(row.get('setup_confirmed_mean_short_ret') or 0.0):.4f} | "
            f"{float(row.get('setup_confirmed_win_rate') or 0.0):.3f} | "
            f"{float(row.get('setup_confirmed_stop_hit_rate') or 0.0):.3f} | "
            f"{float(row.get('setup_skipped_mean_short_ret') or 0.0):.4f} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- SetupReady is not an immediate-entry recommendation.",
            "- Profitability is strongest after ContinuationPermit confirmation.",
            "- Review-only. No production ranking, MeeMee, or runtime DB changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(watch_board_path: Path, profitability_audit_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    board = _read_json(watch_board_path)
    audit = _read_json(profitability_audit_path)
    policies = _policy_lookup(audit)
    candidates = [{**row, **_annotation(row, policies)} for row in board.get("candidates", [])]
    setup_ready = [row for row in candidates if row.get("setup_state") == "SetupReady"]
    confirmed_now = [row for row in setup_ready if row.get("continuation_status") == "ContinuationPermit"]
    pending = [row for row in setup_ready if row.get("continuation_status") == "ContinuationPending"]
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "watch_board_path": str(watch_board_path),
        "profitability_audit_path": str(profitability_audit_path),
        "profitability_policy_summaries": audit.get("policy_summaries", []),
        "counts": {
            "total_candidates": len(candidates),
            "setup_ready_count": len(setup_ready),
            "setup_ready_continuation_permit_now_count": len(confirmed_now),
            "setup_ready_pending_count": len(pending),
        },
        "candidates": candidates,
        "authoritative_decision": (
            "current_setup_ready_has_confirmed_profitability_candidates"
            if confirmed_now
            else "current_setup_ready_has_watch_only_profitability_candidates"
            if setup_ready
            else "current_no_setup_ready_profitability_candidates"
        ),
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "current_short_setup_profitability_annotation.json", payload)
    (run_dir / "current_short_setup_profitability_annotation_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "current_short_setup_profitability_annotation.json",
                "current_short_setup_profitability_annotation_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch-board-path", type=Path, default=DEFAULT_WATCH_BOARD)
    parser.add_argument("--profitability-audit-path", type=Path, default=DEFAULT_PROFITABILITY_AUDIT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.watch_board_path, args.profitability_audit_path, args.output_root))


if __name__ == "__main__":
    main()
