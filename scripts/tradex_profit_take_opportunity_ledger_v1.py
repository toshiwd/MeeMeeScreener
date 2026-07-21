"""Position-independent profit-taking opportunity ledger and human-anchor audit."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def outcome(g: pd.DataFrame, i: int) -> str | None:
    if i + 5 >= len(g):
        return None
    c = float(g.iloc[i].c)
    for j in range(i + 1, i + 6):
        r = g.iloc[j]
        down, up = float(r.l) <= c*.97, float(r.h) >= c*1.03
        if down and up:
            return "neutral_order_unknown"
        if down:
            return "further_down_first"
        if up:
            return "rebound_first"
    return "neutral_no_hit"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    ft = pd.read_parquet(args.features).sort_values(["code", "ymd"]).reset_index(drop=True)
    ft["code"] = ft.code.astype(str).str.zfill(4)
    rows = []
    for code, g0 in ft.groupby("code", sort=False):
        g = g0.reset_index(drop=True).copy()
        below = g.c < g.ma7
        streak = below.groupby(below.ne(below.shift()).cumsum()).cumcount() + 1
        streak = streak.where(below, 0)
        for i, r in g.iterrows():
            reasons = []
            hit_mas = []
            if int(streak.iloc[i]) == 7:
                reasons.append("BELOW_MA7_STREAK7_ONSET")
            for ma in ("ma60", "ma100", "ma200"):
                level = r[ma]
                if pd.notna(level) and float(r.l) <= float(level)+.15*float(r.atr14) and float(r.c) >= float(level):
                    hit_mas.append(ma.upper())
            if hit_mas:
                reasons.append("LONG_MA_TOUCH_HOLD")
            for reason in reasons:
                rows.append({
                    "code": str(code).zfill(4), "ymd": int(r.ymd), "year": int(str(int(r.ymd))[:4]),
                    "exit_reason": reason, "below_ma7_streak": int(streak.iloc[i]),
                    "held_mas": "|".join(hit_mas), "lower_wick_ratio": float(r.lower_wick_ratio),
                    "close_pos": float(r.close_pos), "outcome_fixed3_h5": outcome(g, i),
                    "position_required": True,
                })
    ledger = pd.DataFrame(rows)
    valid = ledger[ledger.outcome_fixed3_h5.notna()]
    results = {}
    for reason in sorted(valid.exit_reason.unique()):
        results[reason] = {}
        for year in range(2019, 2027):
            z = valid[(valid.exit_reason == reason) & valid.year.eq(year)]
            results[reason][str(year)] = {
                "n": int(len(z)),
                "rebound_first": None if z.empty else float(z.outcome_fixed3_h5.eq("rebound_first").mean()),
                "further_down_first": None if z.empty else float(z.outcome_fixed3_h5.eq("further_down_first").mean()),
            }
    b9007 = ledger[(ledger.code == "9007") & ledger.ymd.between(20231004, 20231011)]
    a2802 = ledger[(ledger.code == "2802") & ledger.ymd.eq(20240216)]
    anchors = {
        "9007": {"human_ymd": 20231011, "accepted_prior_trading_window": 5, "signals": b9007.where(pd.notna(b9007), None).to_dict("records"), "match": bool((b9007.exit_reason == "BELOW_MA7_STREAK7_ONSET").any())},
        "2802": {"human_ymd": 20240216, "signals": a2802.where(pd.notna(a2802), None).to_dict("records"), "match": bool((a2802.exit_reason == "LONG_MA_TOUCH_HOLD").any())},
    }
    payload = {
        "schema_version": "tradex_profit_take_opportunity_ledger_v1.compare.v1",
        "artifact_role": "authoritative_diagnostic",
        "review_only": True,
        "contract": {
            "position_independent_detection": True,
            "execution_requires_existing_short": True,
            "reasons_kept_separate": ["BELOW_MA7_STREAK7_ONSET", "LONG_MA_TOUCH_HOLD"],
            "outcome": "exact OHLC symmetric fixed 3 percent first passage t+1 through t+5",
            "threshold_sweep": False,
        },
        "year_results": results, "human_anchors": anchors,
        "judgment": {"decision": "hold", "both_human_anchors_detected": bool(anchors["9007"]["match"] and anchors["2802"]["match"]), "reason": "opportunity detection is separated from entry lifecycle; execution quality remains unproven until position paths are joined"},
        "not_changed": ["entry events", "position lifecycle", "monthly classifier", "MeeMee", "ranking", "runtime DB"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    ledger.to_parquet(args.output / "profit_take_opportunity_ledger.parquet", index=False)
    audit = {"feature_rows": int(len(ft)), "opportunities": int(len(ledger)), "duplicates": int(ledger.duplicated(["code", "ymd", "exit_reason"]).sum()), "feature_sha256": sha(args.features), "future_used_for_selection": False, "review_only": True}
    (args.output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "compare_sha256": sha(compare)}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "anchors": anchors, "judgment": payload["judgment"], "opportunities": len(ledger)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
