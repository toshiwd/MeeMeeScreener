import argparse, hashlib, json
from pathlib import Path

import pandas as pd

YEARS = range(2021, 2026)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def rates(x):
    n = len(x)
    return {
        "n": n,
        "down_first": int(x.outcome.eq("down_first").sum()),
        "rebound_first": int(x.outcome.eq("rebound_first").sum()),
        "neutral_no_hit": int(x.outcome.eq("neutral_no_hit").sum()),
        "down_first_pct": None if not n else 100 * x.outcome.eq("down_first").mean(),
        "rebound_first_pct": None if not n else 100 * x.outcome.eq("rebound_first").mean(),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--union", type=Path, required=True)
    p.add_argument("--strict-try-fail", type=Path, required=True)
    p.add_argument("--full-erasure", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args(); a.output.mkdir(parents=True, exist_ok=True)

    u = pd.read_parquet(a.union)
    rows = []
    for family, lane in [
        ("WEAK_REBOUND_MA20_REBREAK_CORE", "STAGED_CORE"),
        ("BOX_MA200_REJECTION_CORE", "STAGED_CORE"),
        ("POSTBOX_SUPPORT_BREAK_DIRECT_CORE", "DIRECT_CORE"),
    ]:
        q = u[u.source_families.str.split("|").apply(lambda z: family in z)].copy()
        q["lane"] = lane; q["source_family"] = family; q["probe_ymd"] = pd.NA
        rows.append(q[["code", "ymd", "year", "outcome", "lane", "source_family", "probe_ymd"]])

    t = pd.read_parquet(a.strict_try_fail)
    t = t.rename(columns={"ymd": "ymd"})
    t["lane"] = "STAGED_CORE"; t["source_family"] = "UPTREND_CEILING_TRY_FAIL_PRIOR_PROBE_CORE"
    rows.append(t[["code", "ymd", "year", "outcome", "lane", "source_family", "probe_ymd"]])

    e = pd.read_parquet(a.full_erasure)
    e = e[e.branch.eq("UPTREND_WTOP_FULL_ERASURE_DIRECT_CORE")].copy()
    e["ymd"] = e.action_ymd.astype(int); e["year"] = e.action_year.astype(int)
    e["lane"] = "DIRECT_CORE"; e["source_family"] = e.branch; e["probe_ymd"] = pd.NA
    rows.append(e[["code", "ymd", "year", "outcome", "lane", "source_family", "probe_ymd"]])

    raw = pd.concat(rows, ignore_index=True)
    raw["code"] = raw.code.astype(str).str.zfill(4)
    conflicts = (raw.groupby(["code", "ymd", "lane"]).outcome.nunique() > 1).sum()
    ledger = (raw.sort_values(["code", "ymd", "lane", "source_family"])
              .drop_duplicates(["code", "ymd", "lane", "source_family"]))
    results = {lane: {str(y): rates(ledger[(ledger.lane.eq(lane)) & ledger.year.eq(y)])
                      for y in YEARS} for lane in ["STAGED_CORE", "DIRECT_CORE"]}
    gates = {}
    for lane in results:
        r = results[lane]
        gates[lane] = {
            "minimum_30_each_year": all(v["n"] >= 30 for v in r.values()),
            "down_gt_rebound_each_year": all(v["down_first"] > v["rebound_first"] for v in r.values()),
        }
    anchors = {
        "6857_probe_to_core": len(ledger[(ledger.code.eq("6857")) & ledger.probe_ymd.eq(20240827) & ledger.ymd.eq(20240903)]) == 1,
        "2802_direct_core": len(ledger[(ledger.code.eq("2802")) & ledger.ymd.eq(20240206) & ledger.lane.eq("DIRECT_CORE")]) == 1,
        "4755_direct_core": len(ledger[(ledger.code.eq("4755")) & ledger.ymd.eq(20251114) & ledger.lane.eq("DIRECT_CORE")]) == 1,
        "9107_staged_core": len(ledger[(ledger.code.eq("9107")) & ledger.ymd.eq(20241122) & ledger.lane.eq("STAGED_CORE")]) == 1,
    }
    data = {
        "schema_version": "tradex_core_action_path_lane_v1.compare.v1",
        "artifact_role": "authoritative",
        "review_only": True,
        "axis": "action path lane: prior-probe staged core versus direct core",
        "fixed_conditions": {"years": list(YEARS), "outcome": "inherited exact fixed3 h5", "minimum_each_year": 30,
                             "threshold_sweep": False, "costs": "ignored by project rule"},
        "year_results": results, "gates": gates, "human_anchors": anchors,
        "observed_branching": {"raw_rows": len(raw), "ledger_rows": len(ledger),
            "staged_rows": int(ledger.lane.eq("STAGED_CORE").sum()), "direct_rows": int(ledger.lane.eq("DIRECT_CORE").sum()),
            "outcome_conflicts": int(conflicts), "selection_divergence_reason": "observable prior probe separates staged sizing from direct structural break entries"},
        "judgment": {"decision": "keep" if any(v["minimum_30_each_year"] and v["down_gt_rebound_each_year"] for v in gates.values()) else "drop_as_effectiveness_selector_keep_action_contract",
                     "reason": "keep only a lane passing breadth and direction in every validation year"},
        "not_changed": ["source detectors", "fixed3 outcome", "action sizing", "add and profit logic", "MeeMee", "ranking", "runtime DB"],
    }
    cp = a.output / "compare.json"; cp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ledger.to_parquet(a.output / "action_path_lane_ledger.parquet", index=False)
    (a.output / "audit.json").write_text(json.dumps({"duplicate_source_rows": int(ledger.duplicated(["code","ymd","lane","source_family"]).sum()), "outcome_conflicts": int(conflicts), "future_used_for_selection": False, "input_sha256": {"union": sha(a.union), "strict_try_fail": sha(a.strict_try_fail), "full_erasure": sha(a.full_erasure)}}, indent=2)+"\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "year_results": results, "gates": gates, "anchors": anchors, "judgment": data["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__": main()
