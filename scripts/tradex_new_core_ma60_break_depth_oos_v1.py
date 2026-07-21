"""Evaluate one axis: fresh MA60 decisive-break depth at NEW_CORE close."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = (2023, 2024, 2025)
BANDS = {
    "SHALLOW_0P15_TO_0P35_ATR": (0.15, 0.35),
    "CLEAN_0P35_TO_0P80_ATR": (0.35, 0.80),
    "DEEP_GT_0P80_ATR": (0.80, float("inf")),
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def metrics(frame: pd.DataFrame) -> dict:
    down = float(frame.outcome.eq("down_first").mean()) if len(frame) else None
    rebound = float(frame.outcome.eq("rebound_first").mean()) if len(frame) else None
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.nunique()),
        "down_first": down,
        "rebound_first": rebound,
        "neutral": None if frame.empty else float(frame.outcome.str.startswith("neutral").mean()),
        "margin": None if down is None else down - rebound,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    frame = pd.read_parquet(args.state_ledger)
    frame = frame[
        frame.level_type.eq("MA60")
        & frame.state.eq("DECISIVE_BREAK")
        & frame.year.isin(YEARS)
    ].copy()
    frame["break_depth_atr"] = -frame.distance_close_atr

    results = {}
    stable = []
    for name, (lower, upper) in BANDS.items():
        selected = frame[
            frame.break_depth_atr.ge(lower)
            & (frame.break_depth_atr.lt(upper) if upper != float("inf") else True)
        ]
        yearly = {str(year): metrics(selected[selected.year.eq(year)]) for year in YEARS}
        direction_stable = all(
            yearly[str(year)]["down_first"] is not None
            and yearly[str(year)]["down_first"] > yearly[str(year)]["rebound_first"]
            for year in YEARS
        )
        sample_gate = all(yearly[str(year)]["n"] >= 20 for year in YEARS)
        results[name] = {
            "depth_lower_inclusive_atr": lower,
            "depth_upper_exclusive_atr": None if upper == float("inf") else upper,
            "years": yearly,
            "direction_stable": direction_stable,
            "sample_gate_min20_each_year": sample_gate,
        }
        if direction_stable:
            stable.append(name)

    selected_name = "DEEP_GT_0P80_ATR" if "DEEP_GT_0P80_ATR" in stable else None
    selected = results.get(selected_name) if selected_name else None
    decision = (
        "keep" if selected and selected["sample_gate_min20_each_year"]
        else "hold" if selected else "drop"
    )
    reason = {
        "keep": "deep fresh MA60 break passes direction and sample gates in every year",
        "hold": "deep fresh MA60 break is direction-stable but below the minimum yearly sample gate",
        "drop": "no fixed depth band is direction-stable across all years",
    }[decision]

    data = {
        "schema_version": "tradex_new_core_ma60_break_depth_oos_v1.compare.v1",
        "artifact_role": "authoritative_challenger",
        "axis": "fresh MA60 decisive-break depth",
        "fixed_conditions": {
            "action_type": "NEW_CORE",
            "base_state": "MA60 DECISIVE_BREAK",
            "outcome": "fixed3 h5",
            "years": list(YEARS),
            "bands_predeclared_atr": {
                name: {"lower_inclusive": lower, "upper_exclusive": None if upper == float("inf") else upper}
                for name, (lower, upper) in BANDS.items()
            },
            "keep_gate": "down_first>rebound_first and n>=20 in each year",
        },
        "results": results,
        "selected_challenger": selected_name,
        "observed_branching": {
            "base_events": int(len(frame)),
            "selected_events": 0 if not selected_name else int(sum(selected["years"][str(y)]["n"] for y in YEARS)),
            "changed_rank_count": 1 if selected_name else 0,
            "selection_divergence_reason": "fresh MA60 breaks are separated by close depth in ATR",
        },
        "judgment": {"decision": decision, "reason": reason},
        "not_changed": [
            "sequence path", "monthly environment", "candle thresholds", "other MA states",
            "add logic", "profit logic", "MeeMee", "ranking", "runtime DB",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    frame.to_parquet(args.output / "ma60_decisive_break_depth_ledger.parquet", index=False)
    audit = {
        "base_events": int(len(frame)),
        "duplicates": int(frame.duplicated(["code", "action_ymd"]).sum()),
        "future_used_for_selection": False,
        "review_only": True,
        "source_sha256": sha(args.state_ledger),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "selected": selected_name, "result": selected, "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
