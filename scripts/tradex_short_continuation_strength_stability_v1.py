from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "short_continuation_strength_stability_v1"
DEFAULT_SOURCE = Path(
    r"G:\Tradex\short_visual_setup_to_continuation_replay_v1"
    r"\20260605T023154Z-short_visual_setup_to_continuation_replay_v1"
    r"\visual_setup_to_continuation_replay.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_continuation_strength_stability_v1")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _summary(rows: list[dict[str, Any]], candidate_id: str) -> dict[str, Any]:
    if not rows:
        return {"candidate_id": candidate_id, "n": 0}
    frame = pd.DataFrame(rows)
    returns = pd.to_numeric(frame["short_ret"], errors="coerce")
    monthly = frame.assign(ret=returns).groupby("month")["ret"].mean()
    symbol_counts = frame["code"].astype(str).value_counts()
    return {
        "candidate_id": candidate_id,
        "n": int(len(frame)),
        "symbols": int(frame["code"].nunique()),
        "months": int(frame["month"].nunique()),
        "mean_short_ret": float(returns.mean()),
        "median_short_ret": float(returns.median()),
        "win_rate": float((returns > 0).mean()),
        "loss_rate": float((returns < 0).mean()),
        "loss_8pct_rate": float((returns <= -0.08).mean()),
        "positive_month_rate": float((monthly > 0).mean()),
        "worst_month_mean_ret": float(monthly.min()),
        "best_month_mean_ret": float(monthly.max()),
        "max_symbol_share": float(symbol_counts.iloc[0] / len(frame)),
        "stop_hit_rate": float(frame["stop_hit"].astype(bool).mean()),
        "target_hit_rate": float(frame["target_hit"].astype(bool).mean()),
    }


def build_payload(source: dict[str, Any], source_path: Path) -> dict[str, Any]:
    baseline = [
        row
        for row in source.get("events", [])
        if row.get("setup_state") == "SetupReady" and row.get("to_visual_continuation_permit")
    ]
    challenger = [row for row in baseline if row.get("early_bucket") == "EarlyImpulse6NoDenial"]
    removed = [row for row in baseline if row.get("early_bucket") != "EarlyImpulse6NoDenial"]
    baseline_summary = _summary(baseline, "setup_ready_then_continuation_permit")
    challenger_summary = _summary(challenger, "continuation_permit_plus_early_impulse6")
    removed_summary = _summary(removed, "removed_non_early_impulse6")
    keep = bool(
        challenger_summary.get("n", 0) >= 15
        and challenger_summary.get("months", 0) >= 10
        and challenger_summary.get("win_rate", 0.0) >= 0.70
        and challenger_summary.get("median_short_ret", -1.0) > 0.0
        and challenger_summary.get("positive_month_rate", 0.0) >= 0.65
        and challenger_summary.get("max_symbol_share", 1.0) <= 0.25
        and challenger_summary.get("mean_short_ret", -1.0) > baseline_summary.get("mean_short_ret", 1.0)
    )
    return {
        "axis_id": AXIS_ID,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_path": str(source_path),
        "research_phase": "effectiveness judgment",
        "changed_axis": "require EarlyImpulse6NoDenial after existing SetupReady and ContinuationPermit",
        "fixed_evaluation_conditions": {
            "setup_state": "SetupReady",
            "continuation_state": "VisualContinuationPermit",
            "entry_exit_and_costs": "inherit source replay unchanged",
            "universe_period_and_artifact_detail": "inherit source replay unchanged",
        },
        "compare": {
            "baseline": baseline_summary,
            "challenger": challenger_summary,
            "removed": removed_summary,
            "changed_member_count": len(removed),
        },
        "research_decision": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_single_axis_for_review_only" if keep else "hold_needs_more_evidence",
            "authoritative_rollup_decision": "KEEP_REVIEW_ONLY" if keep else "HOLD_REVIEW_ONLY",
            "reason": (
                "EarlyImpulse6 confirmation improved profitability and passed stability gates."
                if keep
                else "EarlyImpulse6 confirmation did not pass every stability gate."
            ),
        },
        "challenger_examples": sorted(challenger, key=lambda row: float(row.get("short_ret") or 0.0), reverse=True),
        "removed_examples": sorted(removed, key=lambda row: float(row.get("short_ret") or 0.0)),
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }


def run(source_path: Path, output_root: Path) -> Path:
    source = json.loads(source_path.read_text(encoding="utf-8"))
    payload = build_payload(source, source_path)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    artifact = run_dir / "compare.json"
    artifact.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (run_dir / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps(
            {"status": "complete", "axis_id": AXIS_ID, "authoritative_artifact": "compare.json"},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.source_path, args.output_root))


if __name__ == "__main__":
    main()
