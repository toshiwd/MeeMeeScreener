from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--timing", required=True)
    ap.add_argument("--management", required=True)
    ap.add_argument("--management-ledger", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    timing = json.loads(Path(a.timing).read_text(encoding="utf-8"))
    management = json.loads(Path(a.management).read_text(encoding="utf-8"))
    timing_key = timing["authoritative_result"]["chosen_candidate"]
    timing_metrics = timing["authoritative_result"]["candidates"][timing_key]["metrics"]
    management_key = management["authoritative_result"]["chosen_policy"]
    management_metrics = management["authoritative_result"]["policies"][management_key]
    ledger = pd.read_csv(a.management_ledger)
    selected_returns = ledger.loc[ledger["policy"].eq(management_key), "short_return"]
    profit_ge_5pct_rate = float((selected_returns >= 0.05).mean())
    checks = {
        "mean_return_at_least_3pct": management_metrics["mean_return"] >= 0.03,
        "profit_ge_5pct_rate_at_least_45pct": profit_ge_5pct_rate >= 0.45,
        "severe_loss_under_5pct": management_metrics["severe_loss_le_minus_10pct_rate"] < 0.05,
        "2026_mean_at_least_2pct": timing_metrics["yearly"]["2026"]["mean_return"] >= 0.02,
        "sample_at_least_250": timing_metrics["row_count"] >= 250,
    }
    payload = {
        "schema_version": "tradex_extreme_high_roll_practical_gate_v2.compare.v1",
        "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {
            "entry": "second_down_w5 confirmation close", "exit": "target10 or strict time5 plus setup-high strong-close denial",
            "practical_gates": {"mean_return": 0.03, "profit_ge_5pct_rate": 0.45, "severe_loss_rate": 0.05, "2026_mean_return": 0.02, "sample": 250},
            "costs": "ignored", "production_ranking_changed": False, "runtime_db_write": False, "meemee_reflection_allowed": False,
        },
        "authoritative_result": {"timing_key": timing_key, "management_key": management_key, "timing_metrics": timing_metrics, "management_metrics": management_metrics, "profit_ge_5pct_rate": profit_ge_5pct_rate, "checks": checks},
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "practical gate only; family membership unchanged"},
        "judgment": {"candidate_local_decision": "keep" if all(checks.values()) else "hold", "authoritative_rollup_decision": "keep_practical" if all(checks.values()) else "hold_not_practical", "reason_type": "profit_magnitude_tail_recent_year_practical_gate", "router_reflection_allowed": all(checks.values())},
        "source_artifacts": {"timing": a.timing, "management": a.management, "management_ledger": a.management_ledger},
        "remaining_risks": ["costs ignored", "2017 weak", "target touch execution"],
    }
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
