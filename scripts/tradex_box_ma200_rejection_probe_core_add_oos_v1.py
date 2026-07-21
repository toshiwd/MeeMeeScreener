"""OOS lifecycle: BOX resistance at MA200 -> second rejection core -> GD low-break add."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = (2023, 2024, 2025)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixed3_h5(g: pd.DataFrame, idx: int) -> str:
    entry = float(g.iloc[idx].c)
    for j in range(idx + 1, min(idx + 6, len(g))):
        down = float(g.iloc[j].l) <= entry * .97
        up = float(g.iloc[j].h) >= entry * 1.03
        if down and up:
            return "neutral_order_unknown"
        if down:
            return "down_first"
        if up:
            return "rebound_first"
    return "neutral_no_hit"


def episodes(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for code, g0 in x.sort_values(["code", "ymd"]).groupby("code", sort=False):
        g = g0.reset_index(drop=True)
        cooldown = -1
        for i, r in g.iterrows():
            if i <= cooldown or not bool(r.ma200_reject):
                continue
            core_i = None
            for j in range(i + 1, min(i + 3, len(g))):
                q = g.iloc[j]
                second_reject = (
                    str(q.environment) == "BOX"
                    and float(q.h) >= float(q.ma200) - .10 * float(q.atr14)
                    and float(q.c) < float(q.ma200)
                    and float(q.c) < float(q.o)
                    and (float(q.upper_wick_ratio) >= .30 or float(q.close_pos) <= .25)
                    and float(q.c) <= float(r.c)
                )
                if second_reject:
                    core_i = j
                    break
            add_i = None
            if core_i is not None:
                prior_low = float(g.iloc[core_i].l)
                for j in range(core_i + 1, min(core_i + 6, len(g))):
                    q = g.iloc[j]
                    gap = float(q.o / g.iloc[j-1].c - 1.0)
                    if gap <= -.005 and float(q.l) < prior_low and float(q.c) < float(q.o) and float(q.close_pos) <= .35:
                        add_i = j
                        break
                    prior_low = min(prior_low, float(q.l))
            core = None if core_i is None else g.iloc[core_i]
            add = None if add_i is None else g.iloc[add_i]
            rows.append({
                "code": str(code).zfill(4), "probe_ymd": int(r.ymd), "year": int(str(int(r.ymd))[:4]),
                "environment": str(r.environment), "probe_outcome_fixed3_h5": fixed3_h5(g, i),
                "core_ymd": None if core is None else int(core.ymd),
                "core_outcome_fixed3_h5": None if core_i is None else fixed3_h5(g, core_i),
                "add_ymd": None if add is None else int(add.ymd),
                "add_outcome_fixed3_h5": None if add_i is None else fixed3_h5(g, add_i),
                "probe_to_core_days": None if core_i is None else int(core_i-i),
                "core_to_add_days": None if add_i is None else int(add_i-core_i),
            })
            cooldown = i + 7
    return pd.DataFrame(rows)


def cell(z: pd.DataFrame) -> dict:
    core = z[z.core_ymd.notna()]
    add = z[z.add_ymd.notna()]
    return {
        "probes": int(len(z)), "codes": int(z.code.nunique()),
        "cores": int(len(core)), "adds": int(len(add)),
        "probe_to_core_rate": None if z.empty else float(len(core)/len(z)),
        "probe_core_add_rate": None if z.empty else float(len(add)/len(z)),
        "core_h5_down_first": None if core.empty else float(core.core_outcome_fixed3_h5.eq("down_first").mean()),
        "core_h5_rebound_first": None if core.empty else float(core.core_outcome_fixed3_h5.eq("rebound_first").mean()),
        "add_h5_down_first": None if add.empty else float(add.add_outcome_fixed3_h5.eq("down_first").mean()),
        "add_h5_rebound_first": None if add.empty else float(add.add_outcome_fixed3_h5.eq("rebound_first").mean()),
        "end_to_end_probe_core_down": None if z.empty else float(core.core_outcome_fixed3_h5.eq("down_first").sum()/len(z)),
        "end_to_end_probe_core_add_down": None if z.empty else float(add.add_outcome_fixed3_h5.eq("down_first").sum()/len(z)),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--monthly-ledger", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    x = pd.read_parquet(args.input).sort_values(["code", "ymd"]).reset_index(drop=True)
    x["code"] = x.code.astype(str).str.zfill(4)
    x["dt"] = pd.to_datetime(x.ymd.astype(str), format="%Y%m%d")
    x["effective_month"] = x.dt.dt.to_period("M").astype(str)
    m = pd.read_parquet(args.monthly_ledger)
    m["code"] = m.code.astype(str).str.zfill(4)
    m["effective_month"] = m.effective_month.astype(str)
    x = x.merge(m[["code", "effective_month", "source_month", "environment"]], on=["code", "effective_month"], how="left", validate="many_to_one")
    prior_c = x.groupby("code", sort=False).c.shift(1)
    prior_ma200 = x.groupby("code", sort=False).ma200.shift(1)
    x["ma200_reject"] = (
        x.environment.eq("BOX")
        & (prior_c < prior_ma200)
        & (x.h >= x.ma200 - .10*x.atr14)
        & (x.c < x.ma200)
        & ((x.h-x.c)/x.atr14 >= .20)
    )
    ep = episodes(x)
    results = {str(y): cell(ep[ep.year.eq(y)]) for y in YEARS}
    anchor = ep[(ep.code == "9107") & ep.probe_ymd.between(20241120, 20241122)].where(pd.notna(ep), None).to_dict("records")
    breadth = all(results[str(y)]["cores"] >= 30 for y in YEARS)
    core_positive = breadth and all(results[str(y)]["core_h5_down_first"] > results[str(y)]["core_h5_rebound_first"] for y in YEARS)
    anchor_hit = any(r["probe_ymd"] == 20241121 and r["core_ymd"] == 20241122 and r["add_ymd"] == 20241126 for r in anchor)
    payload = {
        "schema_version": "tradex_box_ma200_rejection_probe_core_add_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "BOX MA200 first rejection probe, second rejection core, GD running-low break add",
        "fixed_conditions": {
            "probe": "BOX, prior close below MA200, high reaches MA200-0.10ATR, close remains below, rejection >=0.20ATR",
            "core": "second MA200 rejection within two trading days; bearish upper wick>=30% or close_pos<=25%",
            "add": "within five trading days after core; gap<=-0.5%, running-low break, bearish close_pos<=35%",
            "outcome": "exact OHLC symmetric fixed 3 percent first passage t+1 through t+5",
            "years": list(YEARS), "threshold_sweep": False, "costs": "ignored per project rule",
        },
        "year_results": results,
        "human_anchor_9107": anchor,
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None, "selection_divergence_reason": "new resistance-role lifecycle at MA200 inside monthly BOX"},
        "judgment": {
            "decision": "keep" if breadth and core_positive and anchor_hit else "drop",
            "breadth_pass": breadth, "core_h5_down_exceeds_rebound_all_years": core_positive,
            "human_anchor_full_path_match": anchor_hit,
            "reason": "keep requires the complete 9107 path plus >=30 cores and core down-first dominance in every OOS year",
        },
        "not_changed": ["existing families", "monthly classifier", "existing lifecycle", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ep.to_parquet(args.output / "episode_ledger.parquet", index=False)
    audit = {"rows": int(len(x)), "episodes": int(len(ep)), "duplicate_episode": int(ep.duplicated(["code", "probe_ymd"]).sum()), "input_sha256": sha(args.input), "monthly_sha256": sha(args.monthly_ledger), "future_used_for_selection": False, "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "compare_sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "year_results": results, "anchor": anchor, "judgment": payload["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
