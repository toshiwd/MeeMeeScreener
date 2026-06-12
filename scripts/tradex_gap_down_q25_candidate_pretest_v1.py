from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import tradex_gap_down_q25_bad_pick_challenger_v1 as challenger


AXIS_ID = "gap_down_q25_candidate_pretest_v1"
DEFAULT_SOURCE = Path("G:/Tradex/gap_down_q25_bad_pick_challenger_v1/20260604T030104Z-gap-down-q25-bad-pick-challenger-v1/final_research_decision.json")
DEFAULT_REPLAY_ROWS = challenger.DEFAULT_REPLAY_ROWS
DEFAULT_SOURCE_DB = challenger.DEFAULT_SOURCE_DB
DEFAULT_OUT_ROOT = Path("G:/Tradex/gap_down_q25_candidate_pretest_v1")
REQUIRED = (
    "final_research_decision.json",
    "candidate_pretest_compare.json",
    "candidate_pretest_effect_summary.json",
    "replacement_quality_summary.json",
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
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _decision(compare: dict[str, Any], effect: dict[str, Any], repl: dict[str, Any]) -> tuple[str, str, bool]:
    top5 = next(d for d in compare["deltas"] if d["topk"] == 5)
    top10 = next(d for d in compare["deltas"] if d["topk"] == 10)
    e5 = next(x for x in effect["topk"] if x["topk"] == 5)
    e10 = next(x for x in effect["topk"] if x["topk"] == 10)
    r5 = next(x for x in repl["topk"] if x["topk"] == 5)
    r10 = next(x for x in repl["topk"] if x["topk"] == 10)
    direction = (
        (top5["bad_pick_rate_delta"] or 0) < 0
        and (top10["bad_pick_rate_delta"] or 0) < 0
        and (top10["severe_loss_rate_delta"] or 0) <= 0
        and (top5["mean_ret20_delta"] or 0) >= 0
        and (top10["mean_ret20_delta"] or 0) >= 0
        and (top5["hit_rate_delta"] or 0) >= 0
        and (top10["hit_rate_delta"] or 0) >= 0
    )
    practical_branching = e5["dates_with_member_branching"] >= 10 and e5["changed_members_count"] >= 25 and e10["changed_members_count"] >= 10
    false_removal_heavy = r5["false_removed_good_pick_count"] > max(10, r5["removed_bad_pick_count"] * 4)
    if direction and practical_branching and not false_removal_heavy:
        return "keep", "gap_down_q25_pretest_reproduced_quality_improvement_with_practical_branching", True
    if direction:
        reasons = []
        if not practical_branching:
            reasons.append("top10_branching_or_changed_member_count_insufficient")
        if false_removal_heavy:
            reasons.append("top5_false_removal_cost_high")
        return "hold", "direction_positive_but_" + "_and_".join(reasons), True
    return "drop", "gap_down_q25_pretest_failed_quality_or_bad_pick_reduction", False


def run(args: argparse.Namespace) -> Path:
    source = _read_json(args.source_artifact)
    if source.get("authoritative_rollup_decision") != "keep_for_candidate_pretest_next":
        raise RuntimeError("source decision is not keep_for_candidate_pretest_next")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    replay = challenger._load_replay(args.replay_rows)
    rows, q25 = challenger._join_gap(replay, args.source_db)
    compare, effect, repl = challenger._compare(rows)
    decision, reason, reproduced = _decision(compare, effect, repl)
    tested = {
        "axis": "gap_down_q25",
        "definition": "gap_pct <= source q25",
        "q25_value": q25,
        "form": "demotion",
        "threshold_tuning": False,
        "rule": "within each as_of_date, non-gap_down_q25 rows first, then gap_down_q25 rows, preserving baseline rank inside each group",
    }
    _write_json(out_dir / "candidate_pretest_compare.json", {"axis_id": AXIS_ID, "tested_challenger": tested, "compare": compare})
    _write_json(out_dir / "candidate_pretest_effect_summary.json", {"axis_id": AXIS_ID, **effect})
    _write_json(out_dir / "replacement_quality_summary.json", {"axis_id": AXIS_ID, **repl})
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "tested_challenger": tested,
        "source_artifact": str(args.source_artifact),
        "prior_improvement_reproduced": reproduced,
        "top5_top10_top20_result": compare,
        "branching_result": effect,
        "false_removal_replacement_quality": repl,
        "keep_hold_drop_reason": reason,
        "next_required_single_step": (
            "run_stability_replay_for_fixed_gap_down_q25_candidate" if decision == "keep"
            else "hold_gap_down_q25_and_do_not_promote_without_stability_or_better_branching" if decision == "hold"
            else "drop_gap_down_q25_candidate_line"
        ),
        "boundary_flags": {
            "tradex_only": True,
            "candidate_pretest_only": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "frozen_exit_champion_changed": False,
            "ma_phase_mixed_in": False,
            "context_risk_not_clean_mixed_in": False,
            "threshold_tuning": False,
            "baseline_rank_source_changed": False,
            "replay_rows_changed": False,
        },
    }
    _write_json(out_dir / "final_research_decision.json", final)
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {
        "axis_id": AXIS_ID,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "required_files": list(REQUIRED),
        "required_files_present": all((out_dir / f).exists() for f in REQUIRED if f != "_ARTIFACT_COMPLETE.json"),
    })
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Candidate pretest for fixed gap_down_q25 demotion challenger.")
    parser.add_argument("--source-artifact", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
