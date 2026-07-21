"""Audit whether local earnings dates can support a historical event veto."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pandas as pd


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(a.events)
    con = duckdb.connect(str(a.db), read_only=True)
    earnings = con.execute(
        "select code, strftime(planned_date,'%Y%m%d')::integer event_ymd, kind, source from earnings_planned"
    ).fetchdf()
    con.close()
    earnings.code = earnings.code.astype(str).str.zfill(4)
    coverage_min = int(earnings.event_ymd.min())
    coverage_max = int(earnings.event_ymd.max())
    covered = events.loc[events.signal_ymd.between(coverage_min, coverage_max)].copy()
    matches = []
    for row in covered.itertuples(index=False):
        candidates = earnings.loc[earnings.code.eq(str(row.code))].copy()
        if candidates.empty:
            continue
        signal = pd.Timestamp(str(int(row.signal_ymd)))
        candidates["calendar_delta"] = candidates.event_ymd.map(
            lambda value: (pd.Timestamp(str(int(value))) - signal).days
        )
        near = candidates.loc[candidates.calendar_delta.abs().le(3)]
        for event in near.itertuples(index=False):
            matches.append({
                "code": str(row.code),
                "signal_ymd": int(row.signal_ymd),
                "event_ymd": int(event.event_ymd),
                "calendar_delta": int(event.calendar_delta),
                "kind": str(event.kind),
                "return_fixed3_pct": float(row.return_fixed3_pct),
            })
    anchor = earnings.loc[earnings.code.eq("9381") & earnings.event_ymd.eq(20260715)].to_dict("records")
    checks = {
        "historical_coverage_from_2019": coverage_min <= 20190101,
        "covered_candidate_n_ge_100": len(covered) >= 100,
        "event_near_n_ge_30": len(matches) >= 30,
        "9381_earnings_t_plus1_confirmed": bool(anchor),
    }
    usable = all(checks.values())
    result = {
        "schema_version": "tradex_short_event_axis_coverage_v1.compare.v1",
        "artifact_role": "authoritative_short_event_axis_coverage",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": "early above flat/falling MA20 challenger",
            "axis_changed": "earnings calendar proximity only",
            "event_window": "calendar T-3 through T+3",
            "local_source": "earnings_planned",
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "calendar_coverage": {
                "min_ymd": coverage_min,
                "max_ymd": coverage_max,
                "rows": int(len(earnings)),
                "codes": int(earnings.code.nunique()),
            },
            "covered_candidate_count": int(len(covered)),
            "event_near_matches": matches,
            "anchor_9381": anchor,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(matches)),
            "selection_divergence_reason": "event axis cannot branch historical candidates because local calendar starts in 2026-06",
        },
        "judgment": {
            "candidate_local_decision": "keep" if usable else "hold",
            "session_aggregate_decision": "hold_event_axis_historical_coverage_insufficient",
            "authoritative_rollup_decision": "hold_event_veto_annotation_only",
            "reason_type": "local_calendar_insufficient_for_2019_2026_effectiveness_claim",
        },
        "not_changed": ["candidate membership", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "coverage": result["authoritative_result"]["calendar_coverage"], "covered": len(covered), "matches": matches, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
