#!/usr/bin/env python
"""Review-only PIT study of monthly environment, probe short, and staged adds."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

import tradex_nikkei225_first_passage_order_v1 as fp


AXIS_ID = "tradex_monthly_env_probe_add_oos_v1"
YEARS = (2023, 2024, 2025)
HORIZONS = (1, 3, 5, 10)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def monthly_environment(daily: pd.DataFrame) -> pd.DataFrame:
    x = daily.copy()
    x["dt"] = pd.to_datetime(x.ymd.astype(str), format="%Y%m%d")
    x["month"] = x.dt.dt.to_period("M")
    m = (
        x.sort_values(["code", "ymd"])
        .groupby(["code", "month"], as_index=False)
        .agg(o=("o", "first"), h=("h", "max"), l=("l", "min"), c=("c", "last"), bars=("ymd", "size"))
        .sort_values(["code", "month"])
        .reset_index(drop=True)
    )
    parts = []
    for _, g0 in m.groupby("code", sort=False):
        g = g0.copy().reset_index(drop=True)
        pc = g.c.shift(1)
        tr = pd.concat([(g.h-g.l), (g.h-pc).abs(), (g.l-pc).abs()], axis=1).max(axis=1)
        g["matr6"] = tr.rolling(6, min_periods=6).mean()
        g["mma3"] = g.c.rolling(3, min_periods=3).mean()
        g["mma6"] = g.c.rolling(6, min_periods=6).mean()
        g["mma12"] = g.c.rolling(12, min_periods=12).mean()
        g["mma6_slope3_atr"] = (g.mma6-g.mma6.shift(3))/(3*g.matr6)
        g["prior12_high"] = g.h.shift(1).rolling(12, min_periods=10).max()
        g["prior12_low"] = g.l.shift(1).rolling(12, min_periods=10).min()
        g["prior6_high"] = g.h.shift(1).rolling(6, min_periods=5).max()
        g["prior6_low"] = g.l.shift(1).rolling(6, min_periods=5).min()
        g["prior6_close_high"] = g.c.shift(1).rolling(6, min_periods=5).max()
        g["prior6_close_low"] = g.c.shift(1).rolling(6, min_periods=5).min()
        g["range12_atr"] = (g.prior12_high-g.prior12_low)/g.matr6
        g["box_pos"] = (g.c-g.prior12_low)/(g.prior12_high-g.prior12_low)
        local_upper=[]; local_lower=[]; top_touches=[]; local_mature=[]
        for i in range(len(g)):
            w=g.iloc[max(0,i-4):i+1]
            upper=float(w.h.max()); lower=float(w.l.min())
            tol=.25*float(g.loc[i,"matr6"]) if np.isfinite(g.loc[i,"matr6"]) else np.nan
            touches=int((w.h>=upper-tol).sum()) if np.isfinite(tol) else 0
            local_upper.append(upper); local_lower.append(lower); top_touches.append(touches)
            local_mature.append(bool(len(w)>=5 and touches>=2 and (upper-lower)<=3.0*float(g.loc[i,"matr6"])))
        g["local_box_upper"]=local_upper; g["local_box_lower"]=local_lower
        g["local_box_top_touch_count"]=top_touches; g["local_box_mature"]=local_mature
        g["breakout"] = (g.c > g.prior6_close_high + .15*g.matr6) & ((g.prior6_close_high-g.prior6_close_low)/g.matr6 <= 2.5)
        last_i = None
        ceiling = np.nan
        post = []
        ages = []
        reentry = []
        reentry_until = -1
        for i, row in g.iterrows():
            if bool(row.breakout) and np.isfinite(row.prior6_close_high):
                last_i, ceiling = i, float(row.prior6_close_high)
            age = (i-last_i) if last_i is not None else np.nan
            held = last_i is not None and 0 <= age <= 6 and float(row.c) >= ceiling-.20*float(row.matr6)
            post.append(bool(held)); ages.append(age)
            if last_i is not None and float(row.c) < ceiling-.20*float(row.matr6):
                reentry_until = i + 3
                last_i, ceiling = None, np.nan
            reentry.append(i <= reentry_until)
        g["post_box"] = post
        g["box_reentry"] = reentry
        g["breakout_age"] = ages
        box = g.box_reentry | ((g.range12_atr <= 4.0) & (g.mma6_slope3_atr.abs() <= .06) & g.box_pos.between(.05,.95))
        up = (g.c > g.mma6) & (g.mma6 > g.mma12) & (g.mma6_slope3_atr > .015)
        down = (g.c < g.mma6) & (g.mma6 < g.mma12) & (g.mma6_slope3_atr < -.015)
        g["environment"] = np.select(
            [g.post_box, box, up, down],
            ["POST_BOX_BREAKOUT_CONSOLIDATION", "BOX", "UPTREND", "DOWNTREND"],
            default="AMBIGUOUS",
        )
        g["source_month"] = g.month.astype(str)
        g["effective_month"] = g.month + 1
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def add_daily_features(raw: pd.DataFrame, monthly: pd.DataFrame) -> pd.DataFrame:
    x = raw.sort_values(["code", "ymd"]).reset_index(drop=True).copy()
    x["dt"] = pd.to_datetime(x.ymd.astype(str), format="%Y%m%d")
    x["decision_month"] = x.dt.dt.to_period("M")
    keep = ["code","effective_month","source_month","environment","box_pos","prior12_high","prior12_low","matr6","breakout_age","post_box","local_box_upper","local_box_lower","local_box_top_touch_count","local_box_mature"]
    x = x.merge(monthly[keep], left_on=["code","decision_month"], right_on=["code","effective_month"], how="left", validate="many_to_one")
    grp = x.groupby("code", sort=False)
    for n in (1,2,3,5,10):
        for col in ("o","h","l","c","atr14","body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos"):
            x[f"{col}_lag{n}"] = grp[col].shift(n)
    x["gap_pct"] = x.o/grp.c.shift(1)-1
    x["dist_ma100_atr"] = (x.c-x.ma100)/x.atr14
    x["dist_ma200_atr"] = (x.c-x.ma200)/x.atr14
    x["nearest_long_support_atr"] = pd.concat([
        (x.c-x.ma60)/x.atr14,
        (x.c-x.ma100)/x.atr14,
        (x.c-x.ma200)/x.atr14,
        (x.c-x.support20)/x.atr14,
    ], axis=1).where(lambda z: z >= 0).min(axis=1)
    x["top_zone"] = (x.box_pos >= .70) | ((x.resistance20-x.h).abs()/x.atr14 <= .30)
    x["failed_try"] = x.top_zone & (x.c < x.o) & ((x.upper_wick_ratio >= .30) | (x.close_pos <= .20))
    x["strong_retry_failure"] = x.retry_sequence_available.fillna(False) & (x.retry_second_recovery_fraction >= .50) & (x.retry_second_shortfall_atr > 0) & (x.retry_local_high_slope_atr_per_bar <= 0) & (x.c < x.o) & (x.close_pos <= .35)
    x["strong_retry_onset"] = x.strong_retry_failure & (~x.groupby("code",sort=False).strong_retry_failure.shift(1).fillna(False))
    x["monthly_local_box_top"] = x.local_box_mature.fillna(False) & (x.c >= x.local_box_upper-.30*x.matr6) & (x.c <= x.local_box_upper+.30*x.matr6)
    grp = x.groupby("code", sort=False)
    x["monthly_box_top_recent3"] = x.monthly_local_box_top | grp.monthly_local_box_top.shift(1).fillna(False) | grp.monthly_local_box_top.shift(2).fillna(False) | grp.monthly_local_box_top.shift(3).fillna(False)
    bull_lags = []
    for n in (1,2,3):
        bull_lags.append((x[f"c_lag{n}"] > x[f"o_lag{n}"]) & (x[f"body_ratio_lag{n}"] >= .60) & ((x[f"c_lag{n}"]-x[f"o_lag{n}"])/x[f"atr14_lag{n}"] >= .80) & (x[f"close_pos_lag{n}"] >= .75) & (x.c <= x[f"o_lag{n}"]))
    x["impulse_erasure"] = np.logical_or.reduce(bull_lags) & (x.c < x.o) & (x.close_pos <= .35)
    x["support_room_veto"] = x.nearest_long_support_atr <= .35
    x["room_veto"] = x.support_room_veto | (x.oversold_risk == 1)
    x["rejection_veto"] = (x.lower_wick_ratio >= .40) & (x.close_pos >= .55)
    env = x.environment.fillna("AMBIGUOUS")
    box_probe = x.monthly_box_top_recent3 & x.impulse_erasure
    up_probe = (~x.monthly_local_box_top) & env.isin(["UPTREND","POST_BOX_BREAKOUT_CONSOLIDATION"]) & (x.existing_above_ma100_run >= 60) & x.top_zone & x.strong_retry_onset
    down_probe = env.eq("DOWNTREND") & (x.support_break == 1) & (x.close_pos <= .35)
    x["probe_raw"] = box_probe | up_probe | down_probe
    x["probe_allowed"] = x.probe_raw & (~x.room_veto) & (~x.rejection_veto)
    x["probe_family"] = np.select([box_probe,up_probe,down_probe],["BOX_CEILING_ERASURE","UPTREND_TOP_FAILED_TRY","DOWNTREND_SUPPORT_BREAK"],default="NONE")
    return x


def lifecycle(frame: pd.DataFrame) -> pd.DataFrame:
    out = []
    for _, g0 in frame.groupby("code", sort=False):
        g = g0.copy().reset_index(drop=True)
        stage=0; age=999; last_action=-99; running_low=np.nan; entry=np.nan; current_family="NONE"
        probe=[]; add1=[]; add2=[]; status=[]; families=[]
        for i,row in g.iterrows():
            p=a1=a2=False
            if stage==0 and bool(row.probe_allowed):
                p=True; stage=1; age=0; last_action=i; running_low=float(row.l); entry=float(row.c); current_family=str(row.probe_family)
            elif stage>0:
                age += 1; prior_low=running_low; running_low=min(running_low,float(row.l))
                invalid = float(row.c) > max(float(row.ma20), entry + .8*float(row.atr14)) or age>20
                if invalid:
                    stage=0; age=999; running_low=np.nan; entry=np.nan; current_family="NONE"
                else:
                    new_low_close = float(row.c) < prior_low
                    gd_break = float(row.gap_pct) < -.005 and float(row.l) < prior_low
                    eligible = (not bool(row.room_veto)) and (not bool(row.rejection_veto))
                    box_followthrough = current_family=="BOX_CEILING_ERASURE" and age<=2 and float(row.c)<entry and float(row.c)<float(row.ma20)
                    if stage==1 and i-last_action>=1 and (box_followthrough or (eligible and (new_low_close or gd_break or bool(row.support_break)))):
                        a1=True; stage=2; last_action=i
                    elif stage==2 and i-last_action>=2 and ((eligible and (new_low_close or bool(row.failed_try)) and float(row.close_pos)<=.35) or (current_family=="BOX_CEILING_ERASURE" and bool(row.failed_try) and float(row.close_pos)<=.20)):
                        a2=True; stage=3; last_action=i
            probe.append(p); add1.append(a1); add2.append(a2); status.append(stage); families.append(current_family)
        g["probe_event"]=probe; g["add1_event"]=add1; g["add2_event"]=add2; g["position_stage"]=status; g["position_family"]=families
        out.append(g)
    return pd.concat(out, ignore_index=True)


def cluster_bootstrap_delta(event: pd.DataFrame, base: pd.DataFrame, label_col: str, reps: int=400) -> dict:
    rng=np.random.default_rng(20260715)
    ev=event.assign(cluster=event.code.astype(str)+":"+event.decision_month.astype(str))
    ba=base.assign(cluster=base.code.astype(str)+":"+base.decision_month.astype(str))
    emean=ev.groupby("cluster",sort=False)[label_col].mean().to_numpy(dtype=float)
    bmean=ba.groupby("cluster",sort=False)[label_col].mean().to_numpy(dtype=float)
    if len(emean)<10 or len(bmean)<10:
        return {"lo":None,"median":None,"hi":None,"clusters_event":len(emean),"clusters_base":len(bmean)}
    vals=[]
    for _ in range(reps):
        em=float(rng.choice(emean,len(emean),replace=True).mean())
        bm=float(rng.choice(bmean,len(bmean),replace=True).mean())
        vals.append(em-bm)
    lo,med,hi=np.quantile(vals,[.025,.5,.975])
    return {"lo":float(lo),"median":float(med),"hi":float(hi),"clusters_event":len(emean),"clusters_base":len(bmean)}


def evaluate(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    x=frame.copy()
    for h in HORIZONS:
        first=fp.first_passage(x,h)
        x[f"label_{h}"]=first.label.to_numpy()
    rows=[]; details={}
    for kind,col,base_stage in (("probe","probe_event",0),("add1","add1_event",1),("add2","add2_event",2)):
        details[kind]={}
        for year in YEARS:
            yr=x[x.dt.dt.year.eq(year)]
            ev=yr[yr[col]].copy()
            # Same monthly environment and approximate position stage; exclude all action dates.
            allowed_env=set(ev.environment.dropna().unique())
            base=yr[yr.environment.isin(allowed_env) & (~yr.probe_event) & (~yr.add1_event) & (~yr.add2_event)].copy()
            if kind!="probe": base=base[base.position_stage.ge(base_stage)]
            cell={"events":len(ev),"codes":ev.code.nunique(),"months":ev.decision_month.nunique(),"environment_counts":ev.environment.value_counts().to_dict()}
            for h in HORIZONS:
                e_down=float((ev[f"label_{h}"]==0).mean()) if len(ev) else None
                e_up=float((ev[f"label_{h}"]==1).mean()) if len(ev) else None
                b_down=float((base[f"label_{h}"]==0).mean()) if len(base) else None
                b_up=float((base[f"label_{h}"]==1).mean()) if len(base) else None
                ev[f"down_{h}"]=(ev[f"label_{h}"]==0).astype(float); base[f"down_{h}"]=(base[f"label_{h}"]==0).astype(float)
                boot=cluster_bootstrap_delta(ev,base,f"down_{h}")
                cell[f"h{h}"]={"down_first":e_down,"rebound_first":e_up,"baseline_down":b_down,"baseline_rebound":b_up,"down_uplift":None if e_down is None or b_down is None else e_down-b_down,"rebound_delta":None if e_up is None or b_up is None else e_up-b_up,"bootstrap_down_uplift":boot}
            details[kind][str(year)]=cell
            rows.append({"event_kind":kind,"year":year,**cell})
    return x,details


def judgment(details: dict) -> dict:
    result={}
    for kind,cells in details.items():
        signs=[]; breadth=True; ci=[]
        for y in map(str,YEARS):
            c=cells[y]; breadth &= c["events"]>=20 and c["codes"]>=10
            for h in (3,5):
                u=c[f"h{h}"]["down_uplift"]
                signs.append(u is not None and u>0)
                lo=c[f"h{h}"]["bootstrap_down_uplift"]["lo"]
                ci.append(lo is not None and lo>0)
        if breadth and all(signs) and all(ci): decision="keep"
        elif not breadth or any(not s for s in signs): decision="drop"
        else: decision="hold"
        result[kind]={"decision":decision,"breadth_pass":bool(breadth),"year_h3_h5_positive":bool(all(signs)),"bootstrap_lower_positive":bool(all(ci))}
    return result


def family_diagnostics(x: pd.DataFrame) -> dict:
    out={}
    for kind,col in (("probe","probe_event"),("add1","add1_event"),("add2","add2_event")):
        out[kind]={}
        for family in ("BOX_CEILING_ERASURE","UPTREND_TOP_FAILED_TRY","DOWNTREND_SUPPORT_BREAK"):
            out[kind][family]={}
            for year in YEARS:
                z=x[x.dt.dt.year.eq(year)]
                ev=z[z[col] & z.position_family.eq(family)]
                cell={"events":int(len(ev)),"codes":int(ev.code.nunique()),"months":int(ev.decision_month.nunique())}
                for h in (3,5):
                    cell[f"h{h}"]={"down_first":float((ev[f"label_{h}"]==0).mean()) if len(ev) else None,"rebound_first":float((ev[f"label_{h}"]==1).mean()) if len(ev) else None}
                out[kind][family][str(year)]=cell
    return out


def main() -> None:
    ap=argparse.ArgumentParser(); ap.add_argument("--input",type=Path,required=True); ap.add_argument("--retry-features",type=Path,required=True); ap.add_argument("--output-root",type=Path,required=True); args=ap.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); out=args.output_root/f"{stamp}-{AXIS_ID}"; out.mkdir(parents=True,exist_ok=False)
    raw=pd.read_parquet(args.input).sort_values(["code","ymd"]).reset_index(drop=True)
    retry=pd.read_parquet(args.retry_features); retry["ymd"]=pd.to_numeric(retry.ymd,errors="raise").astype(int); retry["code"]=retry.code.astype(str)
    retry_cols=["code","ymd","retry_sequence_available","retry_second_recovery_fraction","retry_second_shortfall_atr","retry_local_high_slope_atr_per_bar","existing_above_ma100_run"]
    raw=raw.merge(retry[retry_cols],on=["code","ymd"],how="left",validate="one_to_one")
    monthly=monthly_environment(raw)
    joined=add_daily_features(raw,monthly)
    state=lifecycle(joined)
    labeled,details=evaluate(state)
    monthly_path=out/"monthly_environment_ledger.parquet"; event_path=out/"probe_add_event_ledger.parquet"
    monthly.to_parquet(monthly_path,index=False); labeled[["code","ymd","source_month","environment","post_box","monthly_local_box_top","local_box_top_touch_count","probe_family","position_family","probe_raw","probe_event","add1_event","add2_event","position_stage"]+[f"label_{h}" for h in HORIZONS]].to_parquet(event_path,index=False)
    no_lookahead=((labeled.source_month.isna()) | (pd.PeriodIndex(labeled.source_month,freq="M") < labeled.decision_month)).all()
    audit={"schema_version":AXIS_ID+".audit.v1","source":{"path":str(args.input),"sha256":sha(args.input)},"retry_source":{"path":str(args.retry_features),"sha256":sha(args.retry_features)},"rows":len(labeled),"codes":labeled.code.nunique(),"monthly_rows":len(monthly),"pit":{"source_month_strictly_before_decision_month":bool(no_lookahead),"duplicate_code_month":int(monthly.duplicated(["code","month"]).sum()),"monthly_environment_uses_future_labels":False},"fixed_conditions":{"universe":"same Nikkei225 daily ledger","oos_years":list(YEARS),"horizons":list(HORIZONS),"label":"existing first-passage order contract","costs":"ignored per project rule"},"boundary":{"owner":"TRADEX","review_only":True,"meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
    compare={"schema_version":AXIS_ID+".compare.v1","artifact_role":"authoritative","axis":"monthly environment then probe/add lifecycle","environment_counts_oos":labeled[labeled.dt.dt.year.isin(YEARS)].environment.value_counts().to_dict(),"event_evaluation":details,"family_diagnostics":family_diagnostics(labeled),"judgment":judgment(details),"selection_divergence_reason":"environment-specific probe families and position-dependent add stages replace cross-environment S4/S8 scoring","changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":None,"not_changed":["MeeMee","production ranking","runtime DB","existing S-state implementation"]}
    audit_path=out/"audit.json"; compare_path=out/"compare.json"; dump(audit_path,audit); dump(compare_path,compare)
    dump(out/"_ARTIFACT_COMPLETE.json",{"complete":True,"compare":str(compare_path),"compare_sha256":sha(compare_path),"audit_sha256":sha(audit_path),"artifacts":{"monthly":str(monthly_path),"events":str(event_path)}})
    print(out)


if __name__=="__main__": main()
