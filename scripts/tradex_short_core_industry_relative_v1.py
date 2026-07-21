from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


BANDS = [(-0.001, 0.20, "industry_weakest20"), (0.20, 0.40, "industry_weak20_40"), (0.40, 0.60, "industry_middle"), (0.60, 0.80, "industry_strong20_40"), (0.80, 1.001, "industry_strongest20")]


def summarize(frame: pd.DataFrame) -> dict:
    managed = np.where(frame["drop5_in5"].eq(1), 5.0, -frame["close5_pct"])
    years = {}
    for year, part in frame.groupby(frame["ymd"] // 10000):
        ret = np.where(part["drop5_in5"].eq(1), 5.0, -part["close5_pct"])
        years[str(int(year))] = {"n": int(len(part)), "managed_mean_pct": float(np.mean(ret)), "hit_rate": float(part["drop5_in5"].mean())}
    return {
        "n": int(len(frame)), "codes": int(frame["code"].nunique()),
        "hit_rate_5pct_in5": float(frame["drop5_in5"].mean()),
        "clean_hit_rate": float(frame["clean_drop5_in5"].mean()),
        "managed_target5_or_close5_mean_pct": float(np.mean(managed)),
        "close5_short_mean_pct": float((-frame["close5_pct"]).mean()),
        "adverse_high5_ge10_rate": float((frame["high5_pct"] >= 10).mean()),
        "median_adverse_high5_pct": float(frame["high5_pct"].median()),
        "years": years,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--actionability", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--output", required=True)
    a = ap.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    cols = ["code", "ymd", "ret5", "ret20", "drop5_in5", "clean_drop5_in5", "close5_pct", "high5_pct"]
    inventory = pd.read_parquet(a.inventory, columns=cols)
    inventory["code"] = inventory["code"].astype(str)
    action = pd.read_parquet(a.actionability, columns=["code", "ymd", "action_tier", "period"])
    action["code"] = action["code"].astype(str)
    core = action[action["action_tier"].eq("Core")].drop_duplicates(["code", "ymd"])
    with duckdb.connect(a.db, read_only=True) as conn:
        industry = conn.execute("select code,sector33_code,sector33_name from industry_master").df()
    industry["code"] = industry["code"].astype(str)
    ranked = inventory.merge(industry, on="code", how="left", validate="many_to_one")
    ranked = ranked[ranked["sector33_code"].notna() & ranked["sector33_code"].ne("-")].copy()
    group = ranked.groupby(["ymd", "sector33_code"], sort=False)
    ranked["industry_count"] = group["code"].transform("count")
    ranked["industry_ret5_rank"] = group["ret5"].rank(pct=True, method="average")
    ranked["industry_ret20_rank"] = group["ret20"].rank(pct=True, method="average")
    ranked["industry_weakness_composite"] = (ranked["industry_ret5_rank"] + ranked["industry_ret20_rank"]) / 2
    joined = core.merge(ranked, on=["code", "ymd"], how="left", validate="one_to_one")
    eligible = joined[joined["industry_count"].ge(5) & joined["industry_weakness_composite"].notna()].copy()
    labels = pd.Series(index=eligible.index, dtype="object")
    for lo, hi, label in BANDS:
        labels.loc[eligible["industry_weakness_composite"].gt(lo) & eligible["industry_weakness_composite"].le(hi)] = label
    eligible["industry_relative_band"] = labels
    eligible = eligible[eligible["industry_relative_band"].notna()].copy()
    results = []
    for period in ["development", "validation"]:
        for _, _, label in BANDS:
            part = eligible[eligible["period"].eq(period) & eligible["industry_relative_band"].eq(label)]
            results.append({"period": period, "band": label, **summarize(part)})
    validation = {x["band"]: x for x in results if x["period"] == "validation"}
    passed = []
    for band, row in validation.items():
        year_means = [v["managed_mean_pct"] for v in row["years"].values()]
        checks = {
            "n_at_least_500": row["n"] >= 500,
            "hit_rate_at_least_45pct": row["hit_rate_5pct_in5"] >= 0.45,
            "managed_mean_at_least_3pct": row["managed_target5_or_close5_mean_pct"] >= 3.0,
            "adverse10_rate_under_5pct": row["adverse_high5_ge10_rate"] < 0.05,
            "all_validation_years_positive": bool(year_means) and all(x > 0 for x in year_means),
        }
        row["practical_checks"] = checks
        if all(checks.values()):
            passed.append(band)
    chosen = max(passed, key=lambda b: validation[b]["managed_target5_or_close5_mean_pct"]) if passed else None
    decision = "keep" if chosen else "drop"
    payload = {
        "schema_version": "tradex_short_core_industry_relative_v1.compare.v1",
        "artifact_role": "authoritative",
        "fixed_evaluation_conditions": {
            "population": "existing Core actionability events only",
            "changed_axis": "mean of within-sector ret5 and ret20 percentile ranks",
            "sector": "current industry_master sector33; sector-date groups n>=5",
            "development": "2019-2023", "validation": "2024-2026",
            "entry": "next open", "target": "intraday -5% within 5 sessions else fifth close",
            "costs": "ignored", "production_ranking_changed": False, "runtime_db_write": False, "meemee_reflection_allowed": False,
        },
        "authoritative_result": {"bands": results, "chosen_band": chosen, "eligible_rows": int(len(eligible)), "missing_or_small_sector_rows": int(len(joined) - len(eligible))},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": int(len(eligible)), "selection_divergence_reason": "Core split by combined within-industry 5d and 20d relative weakness"},
        "judgment": {"candidate_local_decision": decision, "authoritative_rollup_decision": f"core_industry_relative_{decision}", "reason_type": "fixed_practical_5day_return_hit_tail_year_gates"},
        "remaining_risks": ["current industry mapping applied historically", "delisted names may lack industry mapping", "daily OHLC target touch execution", "costs ignored"],
    }
    eligible.to_parquet(out / "core_industry_relative_ledger.parquet", index=False)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}), encoding="utf-8")
    print(json.dumps(payload["authoritative_result"], ensure_ascii=False))


if __name__ == "__main__":
    main()
