from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "short_visual_ranking_integration_readiness_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_visual_ranking_integration_readiness_v1")
DEFAULT_DOWNSIDE_STATS = Path(
    r"G:\Tradex\short_visual_micro_path_downside_stats_v1"
    r"\20260605T011408Z-short_visual_micro_path_downside_stats_v1"
    r"\short_visual_micro_path_downside_stats.json"
)
DEFAULT_CURRENT_SHADOW_RERANK = Path(
    r"G:\Tradex\current_short_visual_shadow_rerank_v1"
    r"\20260605T013316Z-current_short_visual_shadow_rerank_v1"
    r"\current_short_visual_shadow_rerank.json"
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


def _label_readiness(row: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    n = int(row.get("n") or 0)
    mean_ret = float(row.get("mean_short_ret") or 0.0)
    p6 = float(row.get("p_mfe_ge_6pct") or 0.0)
    p8 = float(row.get("p_mfe_ge_8pct") or 0.0)
    stop = float(row.get("stop_hit_rate") or 1.0)
    ready = (
        n >= 30
        and mean_ret > float(baseline["mean_short_ret"])
        and p6 > float(baseline["p_mfe_ge_6pct"])
        and p8 >= float(baseline["p_mfe_ge_8pct"])
        and stop <= float(baseline["stop_hit_rate"])
    )
    blockers: list[str] = []
    if n < 30:
        blockers.append("sample_below_30")
    if mean_ret <= float(baseline["mean_short_ret"]):
        blockers.append("mean_return_not_above_baseline")
    if p6 <= float(baseline["p_mfe_ge_6pct"]):
        blockers.append("p6_not_above_baseline")
    if p8 < float(baseline["p_mfe_ge_8pct"]):
        blockers.append("p8_below_baseline")
    if stop > float(baseline["stop_hit_rate"]):
        blockers.append("stop_hit_above_baseline")
    return {
        "visual_micro_label": row["visual_micro_label"],
        "ranking_feature_ready": ready,
        "blockers": blockers,
        "n": n,
        "mean_short_ret": mean_ret,
        "p_mfe_ge_6pct": p6,
        "p_mfe_ge_8pct": p8,
        "stop_hit_rate": stop,
        "downside_confidence": row.get("downside_confidence"),
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Visual Ranking Integration Readiness v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- downside_stats_path: `{payload['downside_stats_path']}`",
        f"- current_shadow_rerank_path: `{payload['current_shadow_rerank_path']}`",
        "",
        "## Observable Ranking Features",
        "",
        "| label | ready | n | P>=6% | P>=8% | stop | blockers |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in payload["observable_label_readiness"]:
        lines.append(
            f"| {row['visual_micro_label']} | {row['ranking_feature_ready']} | {row['n']} | "
            f"{row['p_mfe_ge_6pct']:.3f} | {row['p_mfe_ge_8pct']:.3f} | {row['stop_hit_rate']:.3f} | "
            f"{', '.join(row['blockers'])} |"
        )
    lines.extend(
        [
            "",
            "## Contract",
            "",
            "- Initial ranking may use only signal-day observable visual_micro_label statistics.",
            "- VisualContinuationPermit is not signal-day observable and must stay in review/monitoring, not initial ranking.",
            "- Production ranking remains unchanged until a historical same-condition shadow replay proves topK lift.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(downside_stats_path: Path, current_shadow_rerank_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    stats = _read_json(downside_stats_path)
    shadow = _read_json(current_shadow_rerank_path)
    baseline = stats["baseline"]
    label_readiness = [_label_readiness(row, baseline) for row in stats["visual_micro_label_downside_stats"]]
    ready_labels = [row["visual_micro_label"] for row in label_readiness if row["ranking_feature_ready"]]
    action_high = [
        row
        for row in stats["visual_micro_action_downside_stats"]
        if row.get("visual_micro_action") == "VisualContinuationPermit" and row.get("downside_confidence") == "high"
    ]
    if ready_labels:
        decision = "hold_observable_visual_labels_ready_for_historical_shadow_ranking_replay"
        next_step = "Build historical same-condition shadow ranking replay using observable labels only."
    elif action_high:
        decision = "hold_not_ready_for_initial_ranking_use_as_post_signal_monitor_overlay"
        next_step = "Do not inject into initial ranking yet; build monitor-stage ranking after 3-session confirmation."
    else:
        decision = "drop_visual_ranking_integration_no_observable_edge"
        next_step = "Do not integrate; continue feature discovery."
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "downside_stats_path": str(downside_stats_path),
        "current_shadow_rerank_path": str(current_shadow_rerank_path),
        "baseline": baseline,
        "observable_label_readiness": label_readiness,
        "non_observable_but_useful_monitor_signal": action_high,
        "current_shadow_observed_branching": shadow.get("observed_branching"),
        "current_shadow_topK_breakdown": shadow.get("shadow_topK_breakdown"),
        "ranking_integration_contract": {
            "initial_ranking_allowed_features": ["visual_micro_label", "label_level_downside_probability_stats"],
            "initial_ranking_forbidden_features": ["VisualContinuationPermit", "early_bucket", "any post-signal continuation feature"],
            "monitor_overlay_allowed_features": ["VisualContinuationPermit after required sessions are available"],
            "production_change_allowed_now": False,
            "required_before_production": [
                "historical same-condition shadow ranking replay with observable labels only",
                "topK branching without Avoid promotion",
                "mean return or downside probability lift without stop-hit deterioration",
                "current board dry-run remains review-only",
            ],
        },
        "authoritative_decision": decision,
        "next_required_action": next_step,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "short_visual_ranking_integration_readiness.json", payload)
    (run_dir / "short_visual_ranking_integration_readiness_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "short_visual_ranking_integration_readiness.json",
                "short_visual_ranking_integration_readiness_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--downside-stats-path", type=Path, default=DEFAULT_DOWNSIDE_STATS)
    parser.add_argument("--current-shadow-rerank-path", type=Path, default=DEFAULT_CURRENT_SHADOW_RERANK)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.downside_stats_path, args.current_shadow_rerank_path, args.output_root))


if __name__ == "__main__":
    main()
