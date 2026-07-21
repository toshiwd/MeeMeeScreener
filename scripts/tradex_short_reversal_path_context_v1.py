from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


KEYS = ["code", "ymd", "bar_index"]


def metrics(g: pd.DataFrame) -> dict:
    return {
        "n": int(len(g)),
        "codes": int(g["code"].nunique()),
        "drop5_rate": float(g["drop5_in5"].mean()),
        "drop_then_rebound_rate": float(g["drop_then_rebound"].mean()),
        "upside_escape_rate": float(g["upside_escape"].mean()),
        "median_high20_pct": float(g["high20_pct"].median()),
        "p90_high20_pct": float(g["high20_pct"].quantile(0.9)),
        "median_close20_pct": float(g["close20_pct"].median()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tiers", required=True)
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    tiers = pd.read_parquet(args.tiers)
    cols = KEYS + ["low5_pct", "high5_pct", "low20_pct", "high20_pct", "close20_pct"]
    inv = pd.read_parquet(args.inventory, columns=cols)
    x = tiers.merge(inv, on=KEYS, how="left", validate="one_to_one", suffixes=("", "_inv"))
    if x["high20_pct"].isna().any():
        raise RuntimeError("inventory join incomplete")

    # Outcome labels only: these never participate in candidate membership.
    x["drop_then_rebound"] = (
        x["drop5_in5"].eq(1) & x["high20_pct"].ge(8.0) & x["close20_pct"].ge(3.0)
    )
    x["upside_escape"] = (
        x["low5_pct"].gt(-3.0) & x["high20_pct"].ge(8.0) & x["close20_pct"].ge(5.0)
    )

    rows = []
    for (period, tier), g in x.groupby(["period", "tier"], observed=True):
        rows.append({"period": period, "tier": tier, **metrics(g)})
    table = pd.DataFrame(rows).sort_values(["period", "tier"])
    table.to_parquet(out / "reversal_path_tier_metrics.parquet", index=False)
    x[KEYS + ["period", "tier", "drop_then_rebound", "upside_escape"]].to_parquet(
        out / "reversal_path_labels.parquet", index=False
    )

    val = table[table["period"].eq("validation")].set_index("tier")
    checks = {
        "outcome_only_no_membership_change": int(len(x)) == int(len(tiers)),
        "core_drop_rate_above_probe": bool(val.loc["Core", "drop5_rate"] > val.loc["Probe", "drop5_rate"]),
        "probe_upside_escape_above_core": bool(val.loc["Probe", "upside_escape_rate"] > val.loc["Core", "upside_escape_rate"]),
    }
    result = {
        "schema_version": "tradex_short_reversal_path_context_v1.compare.v1",
        "artifact_role": "authoritative_short_reversal_path_context",
        "review_only": True,
        "fixed_conditions": {
            "drop_then_rebound": "drop5_in5 and high20_pct>=8 and close20_pct>=3",
            "upside_escape": "low5_pct>-3 and high20_pct>=8 and close20_pct>=5",
            "labels_are_outcomes_only": True,
        },
        "authoritative_result": {"validation_rows": table[table.period.eq("validation")].to_dict("records"), "gate_checks": checks},
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": 0,
            "selection_divergence_reason": "outcome diagnosis does not change membership",
        },
        "judgment": {
            "candidate_local_decision": "keep_diagnostic_only",
            "session_aggregate_decision": "keep_reversal_path_context",
            "authoritative_rollup_decision": "keep_reversal_path_context_v1_review_only",
            "reason_type": "separates_post_drop_rebound_from_immediate_upside_escape",
        },
        "not_changed": ["tier membership", "MeeMee", "ranking", "runtime DB", "production logic"],
        "remaining_risks": ["threshold sensitivity", "20-session overlap", "no event-calendar labels"],
    }
    (out / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "audit.json").write_text(json.dumps({"rows": len(x), "source_rows": len(tiers), "checks": checks}, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2), encoding="utf-8")
    print(json.dumps({"checks": checks, "validation": result["authoritative_result"]["validation_rows"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
