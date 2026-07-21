"""One-axis OOS test: veto core promotion on generic 20-day support break."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

EPISODES = Path(r"G:\Tradex\probe_core_lifecycle_oos_v1\20260715T022954Z-tradex_probe_core_lifecycle_oos_v1\episode_ledger.parquet")
FEATURES = Path(r"G:\Tradex\tradex_nikkei225_daily_assessment_feature_ledger_v1\20260714T040047Z-tradex_nikkei225_daily_assessment_feature_ledger_v1\daily_assessment_features.parquet")
OUT_ROOT = Path(r"G:\Tradex\core_support_break_veto_oos_v1")
YEARS = (2023, 2024, 2025)


def metrics(x: pd.DataFrame, probes: int) -> dict:
    n = len(x)
    down = x.core_label_5.eq(0)
    rebound = x.core_label_5.eq(1)
    return {
        "core_entries": n,
        "coverage_vs_existing_core": None,
        "down_first_h5": None if not n else float(down.mean()),
        "rebound_first_h5": None if not n else float(rebound.mean()),
        "neutral_h5": None if not n else float((~(down | rebound)).mean()),
        "end_to_end_probe_core_down_h5": None if not probes else float(down.sum() / probes),
        "down_2pct_h5": None if not n else float(x.core_down_exc_5.le(-0.02).mean()),
        "mfe_short_h5_median": None if not n else float((-x.core_down_exc_5).median()),
        "mae_short_h5_median": None if not n else float(x.core_up_exc_5.median()),
    }


def main() -> None:
    ep = pd.read_parquet(EPISODES)
    core = ep[ep.core_ymd.notna()].copy()
    core["core_ymd"] = core.core_ymd.astype(int)
    ft = pd.read_parquet(FEATURES, columns=["code", "ymd", "support_break"])
    core = core.merge(ft, left_on=["code", "core_ymd"], right_on=["code", "ymd"], how="left", validate="one_to_one")
    years = {}
    for year in YEARS:
        probes = int(ep.year.eq(year).sum())
        base = core[core.year.eq(year)]
        challenger = base[~base.support_break.eq(1)]
        bm = metrics(base, probes)
        cm = metrics(challenger, probes)
        cm["coverage_vs_existing_core"] = float(len(challenger) / len(base)) if len(base) else None
        years[str(year)] = {
            "champion_existing_core": bm,
            "challenger_support_break_veto": cm,
            "delta_down_first_h5": cm["down_first_h5"] - bm["down_first_h5"],
            "delta_rebound_first_h5": cm["rebound_first_h5"] - bm["rebound_first_h5"],
        }
    pass_all = all(years[str(y)]["challenger_support_break_veto"]["down_first_h5"] > years[str(y)]["challenger_support_break_veto"]["rebound_first_h5"] for y in YEARS)
    payload = {
        "schema_version": "tradex_core_support_break_veto_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "core promotion only: generic support_break veto",
        "fixed_conditions": {"universe": "same episode ledger", "period": "2023-2025", "entry": "add1 close", "horizon": "h5", "probe_and_environment": "unchanged"},
        "year_results": years,
        "observed_branching": {"selection_divergence_reason": "remove add1 dates where support_break==1", "changed_rank_count": None},
        "judgment": {"decision": "keep" if pass_all else "drop", "down_exceeds_rebound_all_years": pass_all,
                     "reason": "a core gate must make h5 down-first exceed rebound-first in every OOS year"},
        "not_changed": ["monthly environment", "probe", "add2", "MeeMee", "ranking", "runtime DB"],
    }
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = OUT_ROOT / f"{stamp}-tradex_core_support_break_veto_oos_v1"
    out.mkdir(parents=True)
    (out / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    audit = {"core_rows": len(core), "support_break_missing": int(core.support_break.isna().sum()), "future_used_for_selection": False}
    (out / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json"}, indent=2), encoding="utf-8")
    print(out)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
