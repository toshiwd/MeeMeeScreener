"""Review-only sizing sensitivity for complete BOX MA200 probe/core/add episodes."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


YEARS = (2023, 2024, 2025)
RATIOS = {"equal_1_1_1": (1, 1, 1), "staged_1_2_2": (1, 2, 2), "add_heavy_1_2_4": (1, 2, 4)}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def first_passage(g: pd.DataFrame, idx: int, entry: float) -> str:
    for j in range(idx + 1, min(idx + 6, len(g))):
        down = float(g.iloc[j].l) <= entry * .97
        rebound = float(g.iloc[j].h) >= entry * 1.03
        if down and rebound:
            return "neutral_order_unknown"
        if down:
            return "down_first"
        if rebound:
            return "rebound_first"
    return "neutral_no_hit"


def summarize(x: pd.DataFrame) -> dict:
    n = len(x)
    inside = x[x.state_at_add_close.eq("inside_barriers")]
    down = int(inside.outcome.eq("down_first").sum())
    rebound = int(inside.outcome.eq("rebound_first").sum())
    return {
        "n": n,
        "target_already_reached_at_add_close": int(x.state_at_add_close.eq("target_already_reached").sum()),
        "inside_barriers_at_add_close": int(len(inside)),
        "down_first": down,
        "rebound_first": rebound,
        "neutral": int(len(inside) - down - rebound),
        "down_first_rate_of_inside": None if inside.empty else down / len(inside),
        "rebound_first_rate_of_inside": None if inside.empty else rebound / len(inside),
        "median_open_profit_at_add_close_pct": None if not n else float(x.open_profit_at_add_close_pct.median()),
        "median_close_return_h5_pct": None if not n else float(x.close_return_h5_pct.median()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=Path, required=True)
    ap.add_argument("--daily", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    episodes = pd.read_parquet(a.episodes)
    episodes = episodes[
        episodes.year.isin(YEARS)
        & episodes.core_ymd.notna()
        & episodes.add_ymd.notna()
    ].copy()
    daily = pd.read_parquet(a.daily, columns=["code", "ymd", "o", "h", "l", "c"])
    daily["code"] = daily.code.astype(str).str.zfill(4)
    histories = {code: g.sort_values("ymd").reset_index(drop=True) for code, g in daily.groupby("code", sort=False)}

    rows = []
    missing = []
    for ep in episodes.itertuples(index=False):
        code = str(ep.code).zfill(4)
        g = histories.get(code)
        dates = (int(ep.probe_ymd), int(ep.core_ymd), int(ep.add_ymd))
        if g is None:
            missing.append({"code": code, "dates": dates, "reason": "missing_code"})
            continue
        loc = {int(v): i for i, v in enumerate(g.ymd)}
        if any(d not in loc for d in dates):
            missing.append({"code": code, "dates": dates, "reason": "missing_date"})
            continue
        probe_i, core_i, add_i = (loc[d] for d in dates)
        prices = (float(g.iloc[probe_i].c), float(g.iloc[core_i].c), float(g.iloc[add_i].c))
        for ratio_name, ratio in RATIOS.items():
            entry = sum(p * w for p, w in zip(prices, ratio)) / sum(ratio)
            last_i = min(add_i + 5, len(g) - 1)
            open_profit = 100 * (entry - prices[2]) / entry
            rows.append({
                "code": code,
                "episode_id": f"MA200:{code}:{dates[0]}",
                "year": int(ep.year),
                "probe_ymd": dates[0],
                "core_ymd": dates[1],
                "add_ymd": dates[2],
                "ratio": ratio_name,
                "probe_close": prices[0],
                "core_close": prices[1],
                "add_close": prices[2],
                "weighted_entry": entry,
                "open_profit_at_add_close_pct": open_profit,
                "state_at_add_close": "target_already_reached" if open_profit >= 3 else "inside_barriers",
                "outcome": first_passage(g, add_i, entry),
                "close_return_h5_pct": 100 * (entry - float(g.iloc[last_i].c)) / entry,
                "outcome_window_last_ymd": int(g.iloc[last_i].ymd),
            })
    ledger = pd.DataFrame(rows)
    results = {
        ratio: {str(y): summarize(ledger[(ledger.ratio == ratio) & ledger.year.eq(y)]) for y in YEARS}
        for ratio in RATIOS
    }
    all_years_directional = {
        ratio: all(v["inside_barriers_at_add_close"] > 0 and v["down_first"] > v["rebound_first"] for v in years.values())
        for ratio, years in results.items()
    }
    anchor = ledger[
        ledger.code.eq("9107")
        & ledger.probe_ymd.eq(20241121)
        & ledger.core_ymd.eq(20241122)
        & ledger.add_ymd.eq(20241126)
    ].to_dict("records")
    payload = {
        "schema_version": "tradex_ma200_position_sizing_sensitivity_v1.compare.v1",
        "artifact_role": "authoritative_diagnostic",
        "review_only": True,
        "axis": "position sizing only after fixed BOX MA200 probe/core/add selection",
        "fixed_conditions": {
            "episode_membership": "existing complete BOX_MA200_REJECTION episodes unchanged",
            "execution": "review-only close proxy; each tranche enters at its action-date close",
            "ratios": {k: {"share_quantity_ratio": list(v)} for k, v in RATIOS.items()},
            "outcome": "classify whole-position state at ADD close first; only inside-barrier cases use weighted-entry exact OHLC symmetric fixed 3 percent first passage t+1 through t+5",
            "years": list(YEARS),
            "costs": "ignored per project rule",
            "ratio_optimization": False,
        },
        "year_results": results,
        "human_anchor_9107": anchor,
        "observed_branching": {
            "changed_top5_members_count": 0,
            "changed_top10_members_count": 0,
            "changed_rank_count": 0,
            "selection_divergence_reason": "none; the same complete episodes are reweighted only",
        },
        "judgment": {
            "decision": "hold",
            "all_years_down_exceeds_rebound_by_ratio": all_years_directional,
            "reason": "post-ADD position-management diagnostic only; no user-authoritative tranche ratio and small complete-path samples",
        },
        "not_changed": ["episode detector", "action dates", "entry selectors", "monthly classifier", "MeeMee", "ranking", "runtime DB"],
    }
    compare = a.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ledger.to_parquet(a.output / "position_sizing_ledger.parquet", index=False)
    audit = {
        "complete_episodes": int(len(episodes)),
        "ledger_rows": int(len(ledger)),
        "missing_paths": missing,
        "duplicate_rows": int(ledger.duplicated(["episode_id", "ratio"]).sum()),
        "future_used_for_selection": False,
        "episodes_sha256": sha(a.episodes),
        "daily_sha256": sha(a.daily),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "results": results, "anchor": anchor, "judgment": payload["judgment"], "audit": audit}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
