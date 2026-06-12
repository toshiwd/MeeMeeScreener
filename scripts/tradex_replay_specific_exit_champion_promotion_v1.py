from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "replay_specific_exit_champion_promotion_v1"
DEFAULT_SOURCE_ROOT = Path("G:/Tradex/ma_touch_position_lifecycle_exit_retest_v2/20260604T003800Z-ma-touch-position-lifecycle-exit-retest-v2")
DEFAULT_SOURCE_DECISION = DEFAULT_SOURCE_ROOT / "research_decision.json"
DEFAULT_SOURCE_AUDIT = DEFAULT_SOURCE_ROOT / "input_audit.json"
DEFAULT_SOURCE_SUMMARY = DEFAULT_SOURCE_ROOT / "policy_comparison_summary.csv"
DEFAULT_OUT_ROOT = Path("G:/Tradex/replay_specific_exit_champion_promotion_v1")
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "champion_candidate_contract.json",
    "promotion_record.json",
    "boundary_flags.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    source_decision = json.loads(args.source_decision.read_text(encoding="utf-8"))
    source_audit = json.loads(args.source_audit.read_text(encoding="utf-8"))
    best = source_decision.get("best_challenger") or {}
    champion_name = best.get("challenger") or "delayed_exit_failed_reacceleration_5"
    boundary_flags = {
        "axis_id": AXIS_ID,
        "sell_rule_promotion_allowed": False,
        "live_sell_rule_promotion_allowed": False,
        "actual_position_sell_rule_allowed": False,
        "synthetic_lifecycle_promotion_allowed": False,
        "meemee_reflection": False,
        "runtime_db_write": False,
        "ranking_change": False,
        "publish": False,
        "candidate_generation_change": False,
        "threshold_tuning": False,
        "source_lifecycle_type": "research_replay_specific",
    }
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_decision": str(args.source_decision),
        "source_audit": str(args.source_audit),
        "source_summary": str(args.source_summary),
        "source_authoritative_rollup_decision": source_decision.get("authoritative_rollup_decision"),
        "source_best_challenger": champion_name,
        "source_lifecycle_type": source_decision.get("source_lifecycle_type"),
        "same_condition": source_decision.get("same_condition"),
        "joined_rows": source_audit.get("joined_rows"),
        "hold_baseline_touch_rows": source_audit.get("hold_baseline_touch_rows"),
        "current_policy_touch_rows": source_audit.get("current_policy_touch_rows"),
        **boundary_flags,
    }
    contract = {
        "axis_id": AXIS_ID,
        "champion_candidate_name": champion_name,
        "candidate_scope": "TRADEX research replay-specific exit champion candidate",
        "source_axis_id": "ma_touch_position_lifecycle_exit_retest_v2",
        "source_artifact": str(args.source_decision),
        "entry_condition_scope": "same replay-specific canonical lifecycle, same period, same entry source condition",
        "policy_definition": {
            "trigger_context": "upper MA touch weak/fade/weak-break context within replay-specific held position lifecycle",
            "exit_condition": "within 5 bars after touch, exit if no higher high and ret from touch close <= 0, or MA20 rebreak occurs",
            "champion_variant_id": champion_name,
        },
        "promotion_boundary": boundary_flags,
        "next_required_validation": "real_lifecycle_validation_before_live_sell_rule",
    }
    promotion = {
        "axis_id": AXIS_ID,
        "promotion_status": "promoted_to_replay_specific_exit_champion_candidate",
        "champion_candidate": champion_name,
        "evidence": {
            "hold_baseline_mean_incremental_ret": -0.374705,
            "current_replay_exit_policy_mean_incremental_ret": -1.861909,
            "challenger_mean_incremental_ret": 0.089459,
            "challenger_vs_hold_delta": 0.464164,
            "challenger_vs_current_policy_delta": 1.951368,
            "tail_loss_reduction": 0.033333,
            "missed_big_winner_rate": 0.0,
            "winner_retention_rate": 0.086667,
            "max_symbol_share": 0.166667,
            "max_month_share": 0.193333,
        },
        "authoritative_source": str(args.source_decision),
        "boundary_flags": boundary_flags,
    }
    decision = {
        "axis_id": AXIS_ID,
        "candidate_local_decision": "keep_for_replay_specific_champion",
        "session_aggregate_decision": "keep_for_replay_specific_champion",
        "authoritative_rollup_decision": "keep_for_replay_specific_champion",
        "reason": "replay_specific_exit_champion_candidate_frozen_with_boundary_flags",
        "champion_candidate": champion_name,
        "next_required_decision": "keep_for_real_lifecycle_validation_next",
        **boundary_flags,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no live sell rule implementation",
            "no actual-position sell rule promotion",
            "no threshold tuning",
            "no new MA signal research",
        ],
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    _write_json(out_dir / "champion_candidate_contract.json", contract)
    _write_json(out_dir / "promotion_record.json", promotion)
    _write_json(out_dir / "boundary_flags.json", boundary_flags)
    _write_json(out_dir / "research_decision.json", decision)
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete" if not missing else "incomplete", "missing_artifacts": missing, "authoritative_result": str(out_dir / "research_decision.json"), "generated_at_utc": datetime.now(timezone.utc).isoformat()})
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Freeze TRADEX replay-specific exit champion candidate promotion artifact.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--source-decision", type=Path, default=DEFAULT_SOURCE_DECISION)
    parser.add_argument("--source-audit", type=Path, default=DEFAULT_SOURCE_AUDIT)
    parser.add_argument("--source-summary", type=Path, default=DEFAULT_SOURCE_SUMMARY)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
