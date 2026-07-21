"""Generate an early-breakdown challenger directly from episode impulses."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _load_daily
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _add_shape_features
from scripts.tradex_short_episode_timing_axis_v1 import add_episode_features, fixed3_outcome, metrics, timing_state


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    daily = _load_daily(a.db, None)
    events = []
    anchor = []
    for code, group in daily.groupby("code", sort=False):
        g = add_episode_features(_add_shape_features(group))
        episode_impulse_ordinal = {}
        for idx, row in g.iterrows():
            if not bool(row.episode_onset):
                continue
            episode = int(row.episode_id)
            episode_impulse_ordinal[episode] = episode_impulse_ordinal.get(episode, 0) + 1
            base = {
                "code": str(code),
                "signal_ymd": int(row.ymd),
                "episode_start_ymd": int(row.episode_start_ymd),
                "episode_age": int(row.episode_age),
                "episode_drop_pct": 100 * float(row.episode_drop),
                "impulse_ordinal": int(episode_impulse_ordinal[episode]),
                "timing_state": timing_state(row.episode_age),
                "signal_close": float(row.c),
                "ma20": None if pd.isna(row.get("ma20")) else float(row.get("ma20")),
            }
            if str(code) == "6996" and 20260701 <= int(row.ymd) <= 20260716:
                anchor.append(base)
            if int(row.ymd) < 20190101 or idx + 5 >= len(g):
                continue
            outcome, hold_days, exit_reason = fixed3_outcome(g, idx)
            events.append({
                **base,
                "entry_open": float(g.iloc[idx + 1].o),
                "return_fixed3_pct": outcome,
                "hold_days": hold_days,
                "exit_reason": exit_reason,
            })
    ledger = pd.DataFrame(events)
    ledger.to_parquet(a.output / "early_breakdown_event_ledger.parquet", index=False)

    timing = {name: metrics(ledger.loc[ledger.timing_state.eq(name)])
              for name in ["Early", "Continuation", "LateChase"]}
    early = ledger.loc[ledger.timing_state.eq("Early")]
    ordinal = {str(value): metrics(early.loc[early.impulse_ordinal.eq(value)])
               for value in sorted(early.impulse_ordinal.unique())}
    early_first_three = early.loc[early.impulse_ordinal.le(3)]
    anchor_dates = {int(row["signal_ymd"]): row for row in anchor}
    checks = {
        "6996_jul06_early": anchor_dates.get(20260706, {}).get("timing_state") == "Early",
        "6996_jul07_early": anchor_dates.get(20260707, {}).get("timing_state") == "Early",
        "6996_jul13_late": anchor_dates.get(20260713, {}).get("timing_state") == "LateChase",
        "early_n_ge_500": timing["Early"]["n"] >= 500,
        "early_mean_gt_late": timing["Early"]["mean_return"] > timing["LateChase"]["mean_return"],
        "early_stop_rate_le_late": timing["Early"]["stop_rate"] <= timing["LateChase"]["stop_rate"],
        "early_positive_2024plus_years_ge_2": sum(
            row["mean_return"] > 0 for year, row in timing["Early"]["years"].items() if int(year) >= 2024
        ) >= 2,
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_early_breakdown_population_v1.compare.v1",
        "artifact_role": "authoritative_short_early_breakdown_population",
        "review_only": True,
        "research_phase": "branching_generation",
        "fixed_conditions": {
            "population": "episode impulse: ret1<=-2%, bearish body>=45%, close_pos<=35%, and previous-5-low break or ret1<=-5%",
            "axis_changed": "replace late pre-crash distance gate with episode impulse timing",
            "timing_states": {"Early": "age 0-3", "Continuation": "age 4-5", "LateChase": "age >=6"},
            "execution": "next open; 5 sessions; target -3%; stop +3%; same-bar stop-first",
            "period": "2019-2026",
            "costs": "ignored",
            "weekly_inputs": [],
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "all": metrics(ledger),
            "timing_states": timing,
            "early_impulse_ordinal": ordinal,
            "early_first_three": metrics(early_first_three),
            "anchor_6996": anchor,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(ledger)),
            "selection_divergence_reason": "new population begins at breakdown impulse instead of waiting for >=24% distance from prior high",
            "state_counts": {name: timing[name]["n"] for name in timing},
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": "keep_early_breakdown_population" if keep else "hold_needs_context_gates",
            "authoritative_rollup_decision": "keep_early_breakdown_challenger_review_only" if keep else "hold",
            "reason_type": "anchor_and_broad_fixed_outcome_gates_passed" if keep else "broad_population_needs_additional_context",
        },
        "not_changed": ["MA20 gate", "event gate", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {"db": {"path": str(a.db.resolve()), "read_only": True}},
        "events": int(len(ledger)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "ledger_sha256": sha(a.output / "early_breakdown_event_ledger.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "timing": timing, "ordinal": ordinal, "anchor": anchor, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
