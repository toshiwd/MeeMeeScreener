from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_visual_ranking_integration_decision_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_visual_ranking_integration_decision_v1")
DEFAULT_OBSERVABLE_REPLAY = Path(
    r"G:\Tradex\short_observable_visual_label_shadow_rank_replay_v1"
    r"\20260605T014054Z-short_observable_visual_label_shadow_rank_replay_v1"
    r"\observable_visual_label_shadow_rank_replay.json"
)
DEFAULT_DOWNSIDE_STATS = Path(
    r"G:\Tradex\short_visual_micro_path_downside_stats_v1"
    r"\20260605T011408Z-short_visual_micro_path_downside_stats_v1"
    r"\short_visual_micro_path_downside_stats.json"
)
DEFAULT_CURRENT_SHADOW = Path(
    r"G:\Tradex\current_short_visual_shadow_rerank_v1"
    r"\20260605T013316Z-current_short_visual_shadow_rerank_v1"
    r"\current_short_visual_shadow_rerank.json"
)
DEFAULT_READINESS = Path(
    r"G:\Tradex\short_visual_ranking_integration_readiness_v1"
    r"\20260605T013536Z-short_visual_ranking_integration_readiness_v1"
    r"\short_visual_ranking_integration_readiness.json"
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


def _find_action(stats: dict[str, Any], action: str) -> dict[str, Any] | None:
    return next((row for row in stats.get("visual_micro_action_downside_stats", []) if row.get("visual_micro_action") == action), None)


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Visual Ranking Integration Decision v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- initial_ranking_decision: `{payload['initial_ranking_decision']}`",
        f"- monitor_overlay_decision: `{payload['monitor_overlay_decision']}`",
        "",
        "## Evidence",
        "",
        f"- observable replay: `{payload['observable_replay_path']}`",
        f"- downside stats: `{payload['downside_stats_path']}`",
        f"- current shadow: `{payload['current_shadow_path']}`",
        f"- readiness: `{payload['readiness_path']}`",
        "",
        "## Observable Replay Summary",
        "",
        "| variant | top_k | changed | ret_lift | p6_lift | stop_delta |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in payload["observable_replay_best_rows"]:
        lines.append(
            f"| {row['variant_id']} | {row['top_k']} | {row['changed_selected_members_count']} | "
            f"{row['mean_short_ret_lift']:.4f} | {row['p_mfe_ge_6pct_lift']:.4f} | {row['stop_hit_rate_delta']:.4f} |"
        )
    monitor = payload["monitor_overlay_evidence"]
    lines.extend(
        [
            "",
            "## Monitor Overlay Evidence",
            "",
            f"- VisualContinuationPermit n: {monitor.get('n')}",
            f"- P>=6%: {monitor.get('p_mfe_ge_6pct')}",
            f"- P>=8%: {monitor.get('p_mfe_ge_8pct')}",
            f"- stop_hit_rate: {monitor.get('stop_hit_rate')}",
            "",
            "## Contract",
            "",
            "- Do not inject observable visual labels into initial production ranking.",
            "- Keep VisualContinuationPermit as post-signal monitor/review overlay only.",
            "- No production ranking, MeeMee, runtime DB, or candidate-generator change was made.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(
    observable_replay_path: Path,
    downside_stats_path: Path,
    current_shadow_path: Path,
    readiness_path: Path,
    output_root: Path,
) -> Path:
    run_dir = output_root / _run_id()
    observable = _read_json(observable_replay_path)
    stats = _read_json(downside_stats_path)
    current_shadow = _read_json(current_shadow_path)
    readiness = _read_json(readiness_path)
    comparisons = observable.get("comparisons", [])
    best_rows = sorted(
        comparisons,
        key=lambda row: (
            int(row.get("changed_selected_members_count") or 0),
            float(row.get("mean_short_ret_lift") or 0.0),
            float(row.get("p_mfe_ge_6pct_lift") or 0.0),
        ),
        reverse=True,
    )[:6]
    monitor = _find_action(stats, "VisualContinuationPermit") or {}
    initial_ok = observable.get("research_decision", {}).get("authoritative_decision", "").startswith("keep_")
    monitor_ok = (
        monitor.get("downside_confidence") == "high"
        and float(monitor.get("p_mfe_ge_6pct") or 0.0) > float(stats["baseline"].get("p_mfe_ge_6pct") or 0.0)
        and float(monitor.get("stop_hit_rate") or 1.0) <= float(stats["baseline"].get("stop_hit_rate") or 1.0)
    )
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "observable_replay_path": str(observable_replay_path),
        "downside_stats_path": str(downside_stats_path),
        "current_shadow_path": str(current_shadow_path),
        "readiness_path": str(readiness_path),
        "observable_replay_decision": observable.get("research_decision"),
        "readiness_decision": readiness.get("authoritative_decision"),
        "current_shadow_decision": current_shadow.get("authoritative_decision"),
        "observable_replay_best_rows": best_rows,
        "monitor_overlay_evidence": monitor,
        "initial_ranking_decision": "drop_initial_ranking_integration" if not initial_ok else "keep_initial_ranking_candidate",
        "monitor_overlay_decision": "keep_post_signal_monitor_overlay_candidate" if monitor_ok else "drop_monitor_overlay_candidate",
        "authoritative_decision": (
            "ranking_integration_decision_complete_initial_drop_monitor_keep"
            if (not initial_ok and monitor_ok)
            else "ranking_integration_decision_complete_initial_keep"
            if initial_ok
            else "ranking_integration_decision_complete_drop_all"
        ),
        "ranking_integration_contract": {
            "initial_production_ranking_change_allowed": False,
            "initial_ranking_reason": "observable visual_micro_label shadow replay produced no topK branching even at scale 100",
            "monitor_overlay_allowed": bool(monitor_ok),
            "monitor_overlay_reason": "VisualContinuationPermit is post-signal observable and has high downside probability separation",
            "requires_before_any_production_change": [
                "separate monitor-stage implementation plan",
                "no-lookahead UI/API contract",
                "current-board dry-run with post-signal availability checks",
                "explicit user approval to change production behavior",
            ],
        },
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "short_visual_ranking_integration_decision.json", payload)
    (run_dir / "short_visual_ranking_integration_decision_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "short_visual_ranking_integration_decision.json",
                "short_visual_ranking_integration_decision_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--observable-replay-path", type=Path, default=DEFAULT_OBSERVABLE_REPLAY)
    parser.add_argument("--downside-stats-path", type=Path, default=DEFAULT_DOWNSIDE_STATS)
    parser.add_argument("--current-shadow-path", type=Path, default=DEFAULT_CURRENT_SHADOW)
    parser.add_argument("--readiness-path", type=Path, default=DEFAULT_READINESS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.observable_replay_path, args.downside_stats_path, args.current_shadow_path, args.readiness_path, args.output_root))


if __name__ == "__main__":
    main()
