"""Fixed-axis OOS check: require a strong GD for BOX-MA200 ADD."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

YEARS = (2023, 2024, 2025)


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def cell(x: pd.DataFrame) -> dict:
    n = len(x)
    d = int(x.add_outcome_fixed3_h5.eq("down_first").sum())
    r = int(x.add_outcome_fixed3_h5.eq("rebound_first").sum())
    return {"n": n, "down_first": d, "rebound_first": r, "neutral": n-d-r}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--episodes", type=Path, required=True)
    p.add_argument("--daily", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)
    e = pd.read_parquet(a.episodes)
    e = e[e.year.isin(YEARS) & e.add_ymd.notna()].copy()
    e["code"] = e.code.astype(str).str.zfill(4)
    e["add_ymd"] = e.add_ymd.astype(int)
    d = pd.read_parquet(a.daily, columns=["code", "ymd", "o", "c"])
    d["code"] = d.code.astype(str).str.zfill(4)
    d = d.sort_values(["code", "ymd"])
    d["prior_close"] = d.groupby("code", sort=False).c.shift(1)
    x = e.merge(d, left_on=["code", "add_ymd"], right_on=["code", "ymd"], validate="one_to_one")
    x["gap_pct"] = 100 * (x.o / x.prior_close - 1)
    x["strong_gd_pass"] = x.gap_pct.le(-1.0)
    base = {str(y): cell(x[x.year.eq(y)]) for y in YEARS}
    selected = {str(y): cell(x[x.year.eq(y) & x.strong_gd_pass]) for y in YEARS}
    anchor = x[(x.code == "9107") & x.add_ymd.eq(20241126)][
        ["code", "probe_ymd", "core_ymd", "add_ymd", "gap_pct", "strong_gd_pass", "add_outcome_fixed3_h5"]
    ].to_dict("records")
    direction = all(v["n"] > 0 and v["down_first"] > v["rebound_first"] for v in selected.values())
    breadth = all(v["n"] >= 5 for v in selected.values())
    data = {
        "schema_version": "tradex_ma200_add_strong_gd_oos_v1.compare.v1",
        "artifact_role": "authoritative",
        "review_only": True,
        "axis": "raise existing ADD gap requirement from -0.5 percent to fixed -1.0 percent",
        "fixed_conditions": {
            "episode_membership": "existing complete BOX_MA200_REJECTION episodes",
            "baseline_add_gap": "<= -0.5 percent",
            "challenger_add_gap": "<= -1.0 percent; reused fixed strong-GD boundary, no sweep",
            "other_add_conditions": "unchanged",
            "outcome": "existing add-close fixed3 h5 exact OHLC first passage",
            "years": list(YEARS),
            "costs": "ignored per project rule",
        },
        "baseline_year_results": base,
        "challenger_year_results": selected,
        "human_anchor_9107": anchor,
        "observed_branching": {
            "baseline_episodes": int(len(x)),
            "challenger_episodes": int(x.strong_gd_pass.sum()),
            "removed_episodes": int((~x.strong_gd_pass).sum()),
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int((~x.strong_gd_pass).sum()),
            "selection_divergence_reason": "ADD days with GD between -0.5 and -1.0 percent are removed",
        },
        "judgment": {
            "decision": "keep" if direction and breadth and bool(anchor) and anchor[0]["strong_gd_pass"] else "drop",
            "down_exceeds_rebound_all_years": direction,
            "minimum_five_per_year": breadth,
            "human_anchor_preserved": bool(anchor) and bool(anchor[0]["strong_gd_pass"]),
            "reason": "drop if any formal year lacks down-first dominance or minimum breadth",
        },
        "not_changed": ["monthly environment", "probe/core rules", "low-break rule", "position sizing", "MeeMee", "ranking", "runtime DB"],
    }
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    x.to_parquet(a.output / "strong_gd_episode_ledger.parquet", index=False)
    audit = {
        "input_complete_episodes": int(len(e)),
        "joined_rows": int(len(x)),
        "missing_rows": int(len(e)-len(x)),
        "duplicate_rows": int(x.duplicated(["code", "probe_ymd"]).sum()),
        "future_used_for_selection": False,
        "episodes_sha256": sha(a.episodes),
        "daily_sha256": sha(a.daily),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(a.output), "baseline": base, "challenger": selected, "anchor": anchor, "judgment": data["judgment"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
