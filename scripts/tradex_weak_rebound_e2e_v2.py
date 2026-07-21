"""Evaluate the fixed weak-rebound probe -> core path without inventing a probe stop."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

FORMAL_YEARS = range(2021, 2026)
DIAGNOSTIC_YEARS = range(2019, 2026)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cell(frame: pd.DataFrame) -> dict:
    n = len(frame)
    down = int(frame.core_outcome.eq("down_first").sum())
    rebound = int(frame.core_outcome.eq("rebound_first").sum())
    return {
        "probe_episodes": n,
        "core_entries": n,
        "probe_to_core_rate": None if not n else 1.0,
        "probe_to_core_days_median": None if not n else float(frame.probe_to_core_days.median()),
        "pre_core_move_pct_median": None if not n else float(frame.pre_core_move_pct.median()),
        "down_first_given_core": None if not n else down / n,
        "rebound_first_given_core": None if not n else rebound / n,
        "end_to_end_probe_core_down": None if not n else down / n,
        "down_first": down,
        "rebound_first": rebound,
        "neutral": n - down - rebound,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--score-ledger", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    source = pd.read_parquet(args.score_ledger)
    source["code"] = source.code.astype(str).str.zfill(4)
    selected = source[source.gate_pass & source.year.isin(DIAGNOSTIC_YEARS)].copy()
    daily = pd.read_parquet(args.daily, columns=["code", "ymd", "c"])
    daily["code"] = daily.code.astype(str).str.zfill(4)
    rows = []
    for row in selected.itertuples(index=False):
        path = daily[
            (daily.code == row.code)
            & daily.ymd.between(int(row.erasure_ymd), int(row.action_ymd))
        ].sort_values("ymd")
        probe = path[path.ymd.eq(int(row.erasure_ymd))]
        core = path[path.ymd.eq(int(row.action_ymd))]
        if probe.empty or core.empty:
            continue
        probe_close = float(probe.c.iloc[0])
        core_close = float(core.c.iloc[0])
        rows.append(
            {
                "code": row.code,
                "probe_ymd": int(row.erasure_ymd),
                "core_ymd": int(row.action_ymd),
                "year": int(row.year),
                "probe_to_core_days": max(0, len(path) - 1),
                "probe_close": probe_close,
                "core_close": core_close,
                "pre_core_move_pct": core_close / probe_close - 1.0,
                "core_outcome": row.outcome,
                "e2e_probe_core_down": row.outcome == "down_first",
            }
        )
    episodes = pd.DataFrame(rows)
    results = {str(year): cell(episodes[episodes.year.eq(year)]) for year in DIAGNOSTIC_YEARS}
    formal_direction = all(
        results[str(year)]["down_first"] > results[str(year)]["rebound_first"]
        for year in FORMAL_YEARS
    )
    external_direction = all(
        results[str(year)]["down_first"] > results[str(year)]["rebound_first"]
        for year in (2019, 2020)
    )
    anchor_source = source[(source.code == "9962") & source.action_ymd.eq(20260713)]
    anchor_frame = anchor_source[
        ["erasure_ymd", "action_ymd", "score", "adjusted_score", "gate_pass", "outcome"]
    ]
    anchor = anchor_frame.where(pd.notna(anchor_frame), None).to_dict("records")
    payload = {
        "schema_version": "tradex_weak_rebound_e2e_v2.compare.v1",
        "artifact_role": "authoritative",
        "review_only": True,
        "research_contract": {
            "probe": "erasure close",
            "core_entry": "adjusted score >=2 at MA20 rebreak close",
            "outcome_start": "core t+1",
            "primary": "h5 first-passage after core and end-to-end probe->core->down",
            "probe_stop": "none; existing TRADEX lifecycle contract",
            "formal_years": list(FORMAL_YEARS),
            "external_diagnostic_years": [2019, 2020],
        },
        "year_results": results,
        "human_anchor_9962": anchor,
        "observed_branching": {
            "selected_episodes": int(len(selected)),
            "scored_episodes": int(len(episodes)),
            "selection_change": 0,
            "selection_divergence_reason": "none; evaluation only",
        },
        "judgment": {
            "decision": "keep_evaluation_contract",
            "formal_effectiveness_decision": "keep" if formal_direction else "hold",
            "external_stability_decision": "keep" if external_direction else "drop",
            "formal_h5_down_exceeds_rebound_all_years": formal_direction,
            "external_h5_down_exceeds_rebound_all_years": external_direction,
        },
        "not_changed": [
            "candidate score",
            "probe/core dates",
            "outcomes",
            "add/profit logic",
            "MeeMee",
            "ranking",
            "runtime DB",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    episodes.to_parquet(args.output / "e2e_ledger.parquet", index=False)
    audit = {
        "selected": int(len(selected)),
        "scored": int(len(episodes)),
        "duplicates": int(episodes.duplicated(["code", "probe_ymd", "core_ymd"]).sum()),
        "missing_paths": int(len(selected) - len(episodes)),
        "future_used_for_selection": False,
        "future_used_for_outcome_only": True,
        "input_sha256": {"score_ledger": sha(args.score_ledger), "daily": sha(args.daily)},
    }
    (args.output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "judgment": payload["judgment"], "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
