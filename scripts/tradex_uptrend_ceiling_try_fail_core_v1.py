"""Build and evaluate the uptrend/local-ceiling failed-try direct-core branch."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = tuple(range(2019, 2026))
ENVIRONMENTS = {"UPTREND", "POST_BOX_BREAKOUT_CONSOLIDATION"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixed3(group: pd.DataFrame, index: int) -> str:
    close = float(group.iloc[index].c)
    for offset in range(index + 1, min(index + 6, len(group))):
        row = group.iloc[offset]
        down = float(row.l) <= close * 0.97
        rebound = float(row.h) >= close * 1.03
        if down and rebound:
            return "neutral_order_unknown"
        if down:
            return "down_first"
        if rebound:
            return "rebound_first"
    return "neutral_no_hit"


def rates(frame: pd.DataFrame) -> dict:
    return {
        "n": int(len(frame)), "codes": int(frame.code.nunique()),
        "down_first": None if frame.empty else float(frame.outcome.eq("down_first").mean()),
        "rebound_first": None if frame.empty else float(frame.outcome.eq("rebound_first").mean()),
        "neutral": None if frame.empty else float(frame.outcome.str.startswith("neutral").mean()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--monthly-ledger", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    features = pd.read_parquet(args.features).sort_values(["code", "ymd"]).reset_index(drop=True)
    features["code"] = features.code.astype(str).str.zfill(4)
    features["effective_month"] = pd.to_datetime(features.ymd.astype(str), format="%Y%m%d").dt.to_period("M").astype(str)
    monthly = pd.read_parquet(args.monthly_ledger)
    monthly["code"] = monthly.code.astype(str).str.zfill(4)
    monthly["effective_month"] = monthly.effective_month.astype(str)
    monthly_cols = [
        "code", "effective_month", "source_month", "base_regime", "local_box_mature",
        "local_box_top_touch_count", "local_close_location", "local_box_top_close_distance_atr",
    ]
    frame = features.merge(monthly[monthly_cols], on=["code", "effective_month"], how="left", validate="many_to_one")

    eligible = (
        frame.base_regime.isin(ENVIRONMENTS)
        & frame.local_box_mature.fillna(False).astype(bool)
        & frame.local_close_location.eq("AT_LOCAL_CEILING")
    )
    daily_try = frame.h.ge(frame.resistance20 - 0.15 * frame.atr14)
    rejection = (
        frame.c.lt(frame.o)
        & frame.upper_wick_ratio.ge(0.25)
        & frame.close_pos.le(0.35)
        & frame.c.le(frame.resistance20 - 0.50 * frame.atr14)
    )
    frame["challenger"] = eligible & daily_try & rejection

    outcomes = {}
    for code, group in frame.groupby("code", sort=False):
        group = group.reset_index()
        for local_index, row in group[group.challenger].iterrows():
            outcomes[int(row["index"])] = fixed3(group, local_index)
    events = frame[frame.challenger].copy()
    events["outcome"] = events.index.map(outcomes)
    events["year"] = events.ymd.astype(str).str[:4].astype(int)
    events = events[events.year.isin(YEARS)]

    yearly = {str(year): rates(events[events.year.eq(year)]) for year in YEARS}
    direction_pass = all(
        yearly[str(year)]["down_first"] is not None
        and yearly[str(year)]["down_first"] > yearly[str(year)]["rebound_first"]
        for year in YEARS
    )
    sample_pass = all(yearly[str(year)]["n"] >= 20 for year in YEARS)
    anchor = events[(events.code.eq("6857")) & events.ymd.eq(20240903)]
    decision = "keep" if direction_pass and sample_pass and len(anchor) == 1 else "hold" if len(anchor) == 1 else "drop"
    data = {
        "schema_version": "tradex_uptrend_ceiling_try_fail_core_v1.compare.v1",
        "artifact_role": "authoritative",
        "axis": "UPTREND_CEILING_TRY_FAIL_CORE branch",
        "fixed_conditions": {
            "monthly_environment": sorted(ENVIRONMENTS),
            "monthly_structure": "local_box_mature and AT_LOCAL_CEILING",
            "daily_try": "high >= resistance20 - 0.15ATR",
            "daily_rejection": "bearish; upper_wick>=0.25; close_pos<=0.35; close<=resistance20-0.50ATR",
            "action": "CORE_CLOSE",
            "outcome": "exact OHLC symmetric fixed3 h5",
            "years": list(YEARS), "minimum_each_year": 20, "threshold_sweep": False,
        },
        "year_results": yearly,
        "human_anchors": {
            "6857_20240903": {"expected": "CORE_CLOSE", "match": len(anchor) == 1, "rows": anchor.where(pd.notna(anchor), None).to_dict("records")},
            "excluded_other_families": {
                "2802_20240206": "full-erasure/W-top family",
                "4755_20251114": "direct breakdown family",
                "6532_20230623": "full-erasure then GD family",
            },
        },
        "observed_branching": {
            "events": int(len(events)), "changed_rank_count": int(len(events)),
            "selection_divergence_reason": "monthly local-ceiling context is combined with a same-day failed high try",
        },
        "judgment": {
            "decision": decision, "direction_pass_all_years": direction_pass,
            "sample_pass_all_years": sample_pass, "human_anchor_preserved": len(anchor) == 1,
            "reason": "branch passes all gates" if decision == "keep" else "anchor matched; effectiveness or breadth still requires improvement" if decision == "hold" else "human anchor not reproduced",
        },
        "not_changed": ["other entry families", "probe detector", "add logic", "profit logic", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    events.to_parquet(args.output / "try_fail_core_events.parquet", index=False)
    audit = {
        "feature_rows": int(len(features)), "events": int(len(events)),
        "duplicates": int(events.duplicated(["code", "ymd"]).sum()),
        "missing_monthly_environment": int(frame.base_regime.isna().sum()),
        "future_used_for_selection": False, "future_used_for_outcome_only": True, "review_only": True,
        "feature_sha256": sha(args.features), "monthly_sha256": sha(args.monthly_ledger),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({
        "complete": True, "authoritative": "compare.json", "sha256": sha(compare),
    }, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "year_results": yearly, "anchor_match": len(anchor) == 1, "judgment": data["judgment"], "audit": audit}, indent=2))


if __name__ == "__main__":
    main()
