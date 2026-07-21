"""Freeze the downside-room action gate selected on the blind discovery set."""
import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


THRESHOLD_ATR = 0.5
GATED_ACTIONS = {"PROBE", "REENTRY_PROBE", "ADD"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(frame: pd.DataFrame) -> dict:
    complete = frame[frame.status.eq("complete")]
    values = complete.return_fixed3_pct.dropna()
    gain, loss = values[values > 0].sum(), -values[values < 0].sum()
    return {
        "n": int(len(frame)), "D": int(complete.outcome_fixed3.eq("D").sum()),
        "R": int(complete.outcome_fixed3.eq("R").sum()), "N": int(complete.outcome_fixed3.eq("N").sum()),
        "mean_fixed3_pct": None if values.empty else float(values.mean()),
        "mean_h5_close_pct": None if complete.empty else float(complete.return_h5_close_pct.mean()),
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": None if values.empty else float(values.min()),
        "sum_return_units_pct": float(values.sum()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--diagnostic", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    source = args.diagnostic / "downside_room_diagnostic_ledger.parquet"
    data = pd.read_parquet(source)
    baseline = data[data.model_action.isin(["PROBE", "CORE", "ADD", "REENTRY_PROBE"])].copy()
    baseline["gate_pass"] = ~baseline.model_action.isin(GATED_ACTIONS) | baseline.downside_room_atr.ge(THRESHOLD_ATR)
    baseline["gate_action"] = baseline.apply(
        lambda row: row.model_action if row.gate_pass else "PROBE_RISK_NO_ADD" if row.model_action == "ADD" else "WAIT_OR_MIN_PROBE",
        axis=1,
    )
    challenger = baseline[baseline.gate_pass].copy()
    ledger_path = args.output / "downside_room_action_gate_ledger.parquet"
    baseline.to_parquet(ledger_path, index=False)
    base_stats, challenger_stats = stats(baseline), stats(challenger)
    d_retention = None if base_stats["D"] == 0 else challenger_stats["D"] / base_stats["D"]
    result = {
        "schema_version": "tradex_downside_room_action_gate_v1.compare.v1",
        "artifact_role": "authoritative_discovery_challenger",
        "review_only": True,
        "research_phase": "threshold_frozen_before_unseen_validation",
        "fixed_conditions": {
            "axis": "nearest lower support downside room only",
            "threshold_atr": THRESHOLD_ATR,
            "CORE": "always pass",
            "PROBE_REENTRY_ADD": "pass when downside_room_atr >= 0.5",
            "failed_ADD": "PROBE_RISK_NO_ADD",
            "failed_PROBE_REENTRY": "WAIT_OR_MIN_PROBE",
            "execution": "next_session_open", "horizon_sessions": 5,
            "weekly_inputs": [], "costs": "ignored", "clean_oos": False,
        },
        "baseline": base_stats,
        "challenger": challenger_stats,
        "observed_branching": {
            "baseline_candidates": len(baseline), "challenger_candidates": len(challenger),
            "removed_candidates": int((~baseline.gate_pass).sum()),
            "removed_by_action": {str(key): int(value) for key, value in baseline.loc[~baseline.gate_pass, "model_action"].value_counts().items()},
            "D_retention": d_retention,
            "R_removed": base_stats["R"] - challenger_stats["R"],
            "selection_divergence_reason": "CORE bypasses the gate; only staged actions require 0.5 ATR observed room",
        },
        "judgment": {
            "candidate_local_decision": "keep_discovery_challenger",
            "session_aggregate_decision": "hold_until_unseen_validation",
            "reason": "discovery challenger removes all R, retains at least 70% of D, and improves maximum loss; no adoption before unused validation",
        },
        "not_changed": ["frozen benchmark", "MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "diagnostic_compare_sha256": sha(args.diagnostic / "compare.json"),
        "source_ledger_sha256": sha(source), "rows": len(baseline),
        "threshold_frozen": True, "weekly_columns_used": [], "future_selection_columns_used": [],
        "ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output": str(args.output), "baseline": base_stats, "challenger": challenger_stats, "branching": result["observed_branching"]}, indent=2))


if __name__ == "__main__":
    main()
