"""Test a probe-only MA20 break near rising MA60, followed by retry-failure core."""
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
    down_barrier, up_barrier = entry * .97, entry * 1.03
    for j in range(idx + 1, min(idx + 6, len(g))):
        down = float(g.iloc[j].l) <= down_barrier
        up = float(g.iloc[j].h) >= up_barrier
        if down and up:
            return "neutral_order_unknown"
        if down:
            return "down_first"
        if up:
            return "rebound_first"
    return "neutral_no_hit"


def build_episodes(x: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for code, g0 in x.sort_values(["code", "ymd"]).groupby("code", sort=False):
        g = g0.reset_index(drop=True)
        cooldown_until = -1
        for i, r in g.iterrows():
            if i <= cooldown_until or not bool(r.probe_candidate):
                continue
            core_i = None
            for j in range(i + 1, min(i + 6, len(g))):
                interim = g.iloc[i + 1:j]
                rebounded = len(interim) > 0 and float(interim.c.max()) >= float(r.c) + .5 * float(r.atr14)
                if rebounded and bool(g.iloc[j].failed_try):
                    core_i = j
                    break
            core = g.iloc[core_i] if core_i is not None else None
            rows.append({
                "code": str(code).zfill(4), "probe_ymd": int(r.ymd),
                "probe_year": int(str(int(r.ymd))[:4]), "environment": str(r.environment),
                "probe_c": float(r.c), "probe_ma60_distance_atr": float((r.c-r.ma60)/r.atr14),
                "core_ymd": None if core is None else int(core.ymd),
                "probe_to_core_days": None if core_i is None else int(core_i-i),
                "core_outcome_fixed3_h5": None if core_i is None else fixed3_h5(g, core_i),
            })
            cooldown_until = i + 5
    return pd.DataFrame(rows)


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
    monthly = pd.read_parquet(args.monthly_ledger)
    monthly["code"] = monthly.code.astype(str).str.zfill(4)
    monthly["effective_month"] = monthly.effective_month.astype(str)
    x = x.merge(monthly[["code", "effective_month", "source_month", "environment"]], on=["code", "effective_month"], how="left", validate="many_to_one")
    grp = x.groupby("code", sort=False)
    prior_c, prior_ma20 = grp.c.shift(1), grp.ma20.shift(1)
    x["failed_try"] = (
        ((x.pos20 >= .70) | ((x.resistance20-x.h).abs()/x.atr14 <= .30))
        & (x.c < x.o) & ((x.upper_wick_ratio >= .30) | (x.close_pos <= .20))
    )
    x["probe_candidate"] = (
        (prior_c >= prior_ma20) & (x.c < x.ma20) & (x.c < x.ma7)
        & (x.ma20 > x.ma60) & (x.ma60 > x.ma100)
        & (x.bear_count5 >= 3)
        & (((x.c-x.ma60)/x.atr14).abs() <= .35)
    )
    ep = build_episodes(x)
    results = {}
    for year in YEARS:
        z = ep[ep.probe_year.eq(year)]
        core = z[z.core_ymd.notna()]
        results[str(year)] = {
            "probes": int(len(z)), "codes": int(z.code.nunique()),
            "cores": int(len(core)), "probe_to_core_rate": None if z.empty else float(len(core)/len(z)),
            "h5_down_first": None if core.empty else float(core.core_outcome_fixed3_h5.eq("down_first").mean()),
            "h5_rebound_first": None if core.empty else float(core.core_outcome_fixed3_h5.eq("rebound_first").mean()),
            "end_to_end_probe_core_down": None if z.empty else float(core.core_outcome_fixed3_h5.eq("down_first").sum()/len(z)),
        }
    anchor = ep[(ep.code == "6857") & ep.probe_ymd.between(20240826, 20240828)].where(pd.notna(ep), None).to_dict("records")
    stable = all(results[str(y)]["cores"] >= 30 for y in YEARS)
    positive = stable and all(results[str(y)]["h5_down_first"] > results[str(y)]["h5_rebound_first"] for y in YEARS)
    anchor_hit = any(r["probe_ymd"] == 20240827 and r["core_ymd"] == 20240903 for r in anchor)
    payload = {
        "schema_version": "tradex_support_adjacent_probe_retry_core_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "support-adjacent probe-only MA20 break then five-day retry-failure core",
        "fixed_conditions": {
            "probe": "first close below MA20 and MA7 from above MA20; MA20>MA60>MA100; bear_count5>=3; abs(close-MA60)<=0.35ATR",
            "core": "within next five trading days, prior interim rebound >=0.5ATR then failed_try bearish candle",
            "outcome": "exact OHLC symmetric fixed 3 percent first passage t+1 through t+5",
            "years": list(YEARS), "threshold_sweep": False, "costs": "ignored per project rule",
        },
        "year_results": results,
        "human_anchor_6857": anchor,
        "observed_branching": {
            "changed_top5_members_count": None, "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "new probe-only family permits support-contact risk while postponing core until failed retry",
        },
        "judgment": {
            "decision": "keep" if positive and anchor_hit else "drop",
            "breadth_pass": stable, "h5_down_exceeds_rebound_all_years": positive,
            "human_anchor_path_match": anchor_hit,
            "reason": "keep requires the 6857 path plus >=30 cores and down-first dominance in every OOS year",
        },
        "not_changed": ["existing probe families", "monthly classifier", "existing lifecycle", "MeeMee", "ranking", "runtime DB"],
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
