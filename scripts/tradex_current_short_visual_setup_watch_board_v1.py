from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "current_short_visual_setup_watch_board_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_short_visual_setup_watch_board_v1")
DEFAULT_CURRENT_VISUAL_BOARD = Path(
    r"G:\Tradex\current_short_visual_micro_path_board_v1"
    r"\20260605T010729Z-current_short_visual_micro_path_board_v1"
    r"\current_short_visual_micro_path_board.json"
)
DEFAULT_SETUP_REPLAY = Path(
    r"G:\Tradex\short_visual_setup_to_continuation_replay_v1"
    r"\20260605T023154Z-short_visual_setup_to_continuation_replay_v1"
    r"\visual_setup_to_continuation_replay.json"
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


def _setup_state(row: dict[str, Any]) -> str:
    label = str(row.get("visual_micro_label") or "")
    lower_high = bool(row.get("visual_lower_high_10v10"))
    ma_overhead = bool(row.get("visual_ma_overhead"))
    clean_pressure = bool(row.get("visual_clean_pressure"))
    support_touch = bool(row.get("visual_support_touch_20"))
    too_extended = bool(row.get("visual_too_extended"))
    lower_wick = bool(row.get("visual_lower_wick_reclaim"))
    if label in {"SellableRollover", "CleanContinuationDown"}:
        return "SetupReady"
    if label == "PullbackBeforeBreak":
        return "SetupWaitBreak"
    if lower_high and ma_overhead and clean_pressure and not lower_wick:
        return "SetupPressure"
    if too_extended or support_touch or lower_wick:
        return "SetupRisk"
    return "SetupNoEdge"


def _summary_lookup(setup_replay: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["setup_state"]): row for row in setup_replay.get("setup_state_summary", [])}


def _watch_priority(row: dict[str, Any]) -> int:
    state = str(row.get("setup_state") or "")
    final_status = str(row.get("final_review_status") or "")
    if state == "SetupReady" and final_status != "Avoid":
        return 0
    if state == "SetupReady":
        return 1
    if state == "SetupPressure" and final_status != "Avoid":
        return 2
    if state == "SetupPressure":
        return 3
    if state == "SetupRisk":
        return 5
    return 4


def _watch_score(row: dict[str, Any]) -> float:
    p = float(row.get("setup_to_visual_continuation_permit_rate") or 0.0)
    p6 = float(row.get("setup_p_mfe_ge_6pct") or 0.0)
    stop = float(row.get("setup_stop_hit_rate") or 1.0)
    original = float(row.get("original_score") or 0.0)
    penalty = 0.20 if str(row.get("final_review_status")) == "Avoid" else 0.0
    return original + 0.80 * p + 0.25 * p6 - 0.35 * stop - penalty


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Short Visual Setup Watch Board v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- source_visual_board_path: `{payload['source_visual_board_path']}`",
        f"- setup_replay_path: `{payload['setup_replay_path']}`",
        "",
        "## Counts",
        "",
    ]
    for key, value in payload["counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Watch Top20",
            "",
            "| watch_rank | original_rank | code | signal | final | setup | to_permit | p6 | stop | visual |",
            "|---:|---:|---|---:|---|---|---:|---:|---:|---|",
        ]
    )
    for row in sorted(payload["candidates"], key=lambda item: int(item["watch_rank"]))[:20]:
        lines.append(
            f"| {row['watch_rank']} | {row['original_rank']} | {row['code']} | {row['signal_ymd']} | "
            f"{row.get('final_review_status')} | {row['setup_state']} | "
            f"{float(row.get('setup_to_visual_continuation_permit_rate') or 0.0):.3f} | "
            f"{float(row.get('setup_p_mfe_ge_6pct') or 0.0):.3f} | "
            f"{float(row.get('setup_stop_hit_rate') or 0.0):.3f} | {row.get('visual_micro_label')} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Watch-rank only. This is a setup/monitoring artifact, not an initial short entry recommendation.",
            "- No production ranking, MeeMee, or runtime DB changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(current_visual_board_path: Path, setup_replay_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    board = _read_json(current_visual_board_path)
    setup_replay = _read_json(setup_replay_path)
    lookup = _summary_lookup(setup_replay)
    candidates: list[dict[str, Any]] = []
    for item in board.get("candidates", []):
        state = _setup_state(item)
        stat = lookup.get(state, {})
        enriched = {
            **item,
            "setup_state": state,
            "setup_to_visual_continuation_permit_rate": stat.get("to_visual_continuation_permit_rate"),
            "setup_p_mfe_ge_6pct": stat.get("p_mfe_ge_6pct"),
            "setup_p_mfe_ge_8pct": stat.get("p_mfe_ge_8pct"),
            "setup_stop_hit_rate": stat.get("stop_hit_rate"),
            "setup_sample_n": stat.get("n"),
            "watch_priority_bucket": _watch_priority({**item, "setup_state": state}),
        }
        enriched["watch_score"] = _watch_score(enriched)
        candidates.append(enriched)
    candidates.sort(key=lambda row: (int(row["watch_priority_bucket"]), -float(row["watch_score"]), int(row["original_rank"])))
    for idx, row in enumerate(candidates, start=1):
        row["watch_rank"] = idx
        row["watch_rank_delta"] = int(row["original_rank"]) - idx
    counts = {
        "total_candidates": len(candidates),
        "setup_state_counts": dict(Counter(str(row["setup_state"]) for row in candidates)),
        "top10_setup_state_counts": dict(Counter(str(row["setup_state"]) for row in candidates[:10])),
        "final_status_counts": dict(Counter(str(row.get("final_review_status")) for row in candidates)),
    }
    top10_ready = counts["top10_setup_state_counts"].get("SetupReady", 0)
    decision = (
        "current_board_has_setup_ready_watch_candidates"
        if top10_ready > 0
        else "current_board_has_no_setup_ready_watch_candidates"
    )
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_visual_board_path": str(current_visual_board_path),
        "setup_replay_path": str(setup_replay_path),
        "counts": counts,
        "watch_rank_contract": {
            "purpose": "setup/monitor ranking, not initial short entry ranking",
            "rank_order": "watch_priority_bucket, watch_score, original_rank",
            "production_change_allowed": False,
            "no_trade_recommendation": True,
        },
        "candidates": candidates,
        "authoritative_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "current_short_visual_setup_watch_board.json", payload)
    (run_dir / "current_short_visual_setup_watch_board_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "current_short_visual_setup_watch_board.json",
                "current_short_visual_setup_watch_board_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-visual-board-path", type=Path, default=DEFAULT_CURRENT_VISUAL_BOARD)
    parser.add_argument("--setup-replay-path", type=Path, default=DEFAULT_SETUP_REPLAY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.current_visual_board_path, args.setup_replay_path, args.output_root))


if __name__ == "__main__":
    main()
