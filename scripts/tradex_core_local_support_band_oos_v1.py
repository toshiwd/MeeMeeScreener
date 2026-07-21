"""PIT local-support-band event contract and one-axis core test.

The positive gate applies only to DOWNTREND_SUPPORT_BREAK episodes.  Other
families are deliberately untouched because a top failure does not require a
floor break.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


LOOKBACK = 120
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
MIN_TOUCHES = 2
CLUSTER_ATR = 0.50


def _band_rows(part: pd.DataFrame, target_dates: set[int]) -> pd.DataFrame:
    g = part.sort_values("ymd").reset_index(drop=True).copy()
    lows = g.l.to_numpy(float)
    atrs = g.atr14.to_numpy(float)
    prev_close = g.c.shift(1).to_numpy(float)
    output: list[dict] = []
    for i, row in g.iterrows():
        if int(row.ymd) not in target_dates:
            continue
        enriched = row.to_dict()
        atr = atrs[i]
        if i < 5 or not np.isfinite(atr) or atr <= 0 or not np.isfinite(prev_close[i]):
            enriched.update({"local_band_center":None,"local_band_lower":None,"local_band_upper":None,
                             "local_band_touch_count":0,"local_band_state":"NO_MATURE_BAND",
                             "local_band_decisive_break":False,"local_band_latest_pivot_ymd":None})
            output.append(enriched)
            continue
        # A pivot at p is usable on decision day i only when p+2 <= i-1.
        confirmed: list[int] = []
        for p in range(max(PIVOT_LEFT, i - LOOKBACK), i - PIVOT_RIGHT):
            if lows[p] <= min(lows[p-2:p]) and lows[p] < min(lows[p+1:p+3]):
                confirmed.append(p)
        tol = CLUSTER_ATR * atr
        candidates: list[tuple[float, list[int]]] = []
        for p in confirmed:
            members = [q for q in confirmed if abs(lows[q] - lows[p]) <= tol]
            # Pivot dates are naturally separated by at least three bars under
            # the 2-left/2-right definition; de-duplicate identical clusters.
            if len(members) >= MIN_TOUCHES:
                center = float(np.median(lows[members]))
                if center <= prev_close[i] + tol:
                    candidates.append((center, members))
        if not candidates:
            enriched.update({"local_band_center":None,"local_band_lower":None,"local_band_upper":None,
                             "local_band_touch_count":0,"local_band_state":"NO_MATURE_BAND",
                             "local_band_decisive_break":False,"local_band_latest_pivot_ymd":None})
            output.append(enriched)
            continue
        center, members = min(candidates, key=lambda item: abs(prev_close[i] - item[0]))
        # The zone thickness is the observed distribution of confirmed pivot
        # lows, not a symmetric volatility buffer around their median.
        lower = float(min(lows[q] for q in members))
        upper = float(max(lows[q] for q in members))
        c, o, low = float(row.c), float(row.o), float(row.l)
        body = float(row.body_ratio or 0.0); close_pos = float(row.close_pos or 0.0)
        fresh = prev_close[i] >= center and c < center
        if fresh and c < lower:
            state = "GAP_BREAK_HOLD" if o < lower else "FRESH_CLOSE_BREAK"
        elif fresh:
            state = "CENTER_BREAK_IN_BAND"
        elif low < lower and c >= center:
            state = "INTRADAY_RECLAIM"
        elif c < lower:
            state = "BELOW_BAND"
        elif c <= upper:
            state = "IN_BAND"
        else:
            state = "LIVE_ABOVE"
        is_decisive = bool(c < lower or (fresh and body >= 0.50 and close_pos <= 0.30))
        enriched.update({"local_band_center":center,"local_band_lower":lower,"local_band_upper":upper,
                         "local_band_touch_count":len(members),"local_band_state":state,
                         "local_band_decisive_break":is_decisive,
                         "local_band_latest_pivot_ymd":int(g.loc[max(members), "ymd"])})
        output.append(enriched)
    return pd.DataFrame(output)


def _rates(x: pd.DataFrame) -> dict:
    return {"n": int(len(x)), "down_first_h5": None if x.empty else float(x.core_label_5.eq(0).mean()),
            "rebound_first_h5": None if x.empty else float(x.core_label_5.eq(1).mean())}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", type=Path, required=True)
    ap.add_argument("--features", type=Path, required=True)
    ap.add_argument("--human-annotations", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, required=True)
    args = ap.parse_args()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.output_root / f"{stamp}-tradex-core-local-support-band-envelope-oos-v1"
    out.mkdir(parents=True, exist_ok=False)
    cols = ["code","ymd","o","h","l","c","atr14","body_ratio","close_pos"]
    ep = pd.read_parquet(args.episodes)
    core = ep[ep.core_ymd.notna()].copy(); core["core_ymd"] = core.core_ymd.astype(int)
    annotations = json.loads(args.human_annotations.read_text(encoding="utf-8"))["annotations"]
    target_by_code: dict[str, set[int]] = {}
    for r in core[["code","core_ymd"]].to_dict("records"):
        target_by_code.setdefault(str(r["code"]).zfill(4), set()).add(int(r["core_ymd"]))
    for r in annotations:
        target_by_code.setdefault(str(r["code"]).zfill(4), set()).add(int(r["ymd"]))
    source = pd.read_parquet(args.features, columns=cols).sort_values(["code","ymd"])
    parts = [_band_rows(p, target_by_code.get(str(code).zfill(4), set())) for code, p in source.groupby("code", sort=False)]
    band = pd.concat([p for p in parts if not p.empty], ignore_index=True)
    core = core.merge(band, left_on=["code","core_ymd"], right_on=["code","ymd"], how="left", validate="one_to_one")
    family = core[core.family.eq("DOWNTREND_SUPPORT_BREAK")]
    years = {}
    for year in (2023, 2024, 2025):
        base = family[family.year.eq(year)]
        challenger = base[base.local_band_decisive_break]
        years[str(year)] = {"champion_family": _rates(base), "challenger": _rates(challenger),
                            "coverage": None if base.empty else float(len(challenger)/len(base)),
                            "state_results": {k:_rates(v) for k,v in base.groupby("local_band_state")}}
    human = pd.DataFrame([{"case_id":r["case_id"],"code":str(r["code"]).zfill(4),"ymd":int(r["ymd"]),
                           "human_decision":r["human_decision"],
                           "human_support_break":bool(r.get("concepts",{}).get("gap_down_prior_low_break_add_short",False))}
                          for r in annotations])
    band_h = band.copy(); band_h["code"] = band_h.code.astype(str).str.zfill(4)
    human = human.merge(band_h, on=["code","ymd"], how="left", validate="one_to_one")
    human["machine_support_break"] = human.local_band_decisive_break.fillna(False)
    human["agreement"] = human.human_support_break.eq(human.machine_support_break)
    pass_years = all(years[str(y)]["challenger"]["n"] >= 10 and
                     years[str(y)]["challenger"]["down_first_h5"] > years[str(y)]["challenger"]["rebound_first_h5"]
                     for y in (2023,2024,2025))
    human_rate = float(human.agreement.mean())
    decision = "keep" if pass_years and human_rate >= 0.80 else "drop"
    payload = {
        "schema_version":"tradex_core_local_support_band_envelope_oos_v1.compare.v1", "artifact_role":"authoritative",
        "axis":"observed pivot-low envelope support band for DOWNTREND_SUPPORT_BREAK core only",
        "fixed_conditions":{"years":[2023,2024,2025],"family":"DOWNTREND_SUPPORT_BREAK","horizon":5,"costs_ignored":True},
        "band_contract":{"lookback":LOOKBACK,"pivot_left":PIVOT_LEFT,"pivot_right":PIVOT_RIGHT,
                         "latest_pivot_must_be_lagged_bars":3,"min_touches":MIN_TOUCHES,
                         "cluster_tolerance_atr":CLUSTER_ATR,"band_bounds":"min/max of confirmed member pivot lows"},
        "year_results":years,
        "human_agreement":{"n":int(len(human)),"rate":human_rate,
                           "rows":human[["case_id","code","ymd","human_decision","human_support_break","local_band_center","local_band_lower","local_band_upper","local_band_touch_count","local_band_state","local_band_decisive_break","agreement"]].where(pd.notna(human),None).to_dict("records")},
        "judgment":{"decision":decision,"reason":"must beat rebound in every year with n>=10 and reproduce >=80% of support-break annotations"},
        "not_changed":["other setup families","probe","monthly environment","MA/candle gates","add/full-erasure path","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    core.to_parquet(out/"core_local_support_band_ledger.parquet",index=False)
    human.to_parquet(out/"human_local_support_band_ledger.parquet",index=False)
    audit={"source_rows":int(len(source)),"core_rows":int(len(core)),"family_core_rows":int(len(family)),
           "missing_core_band_state":int(core.local_band_state.isna().sum()),"duplicate_core":int(core.duplicated(["code","probe_ymd"]).sum()),
           "future_used":False,"pivot_confirmation_lag":2,"latest_usable_pivot":"t-3","review_only":True}
    (out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    (out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out); print(json.dumps({"years":years,"human_agreement":human_rate,"judgment":payload["judgment"],"audit":audit},ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
