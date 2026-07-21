"""Fixed +/-3% five-day first-passage contract for trade reproduction.

This supplements, rather than replaces, the existing ATR-scaled research
label.  It matches the user's practical interpretation of a roughly 3% move.
"""
from __future__ import annotations
import argparse, json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

def classify(rows:pd.DataFrame, index:int)->tuple[int,str,int]:
    entry=float(rows.iloc[index].c); down=entry*.97; up=entry*1.03
    future=rows.iloc[index+1:index+6]
    for day,(_,r) in enumerate(future.iterrows(),1):
        if float(r.o)<=down:return 0,"down_open_gap",day
        if float(r.o)>=up:return 1,"rebound_open_gap",day
        dh=float(r.l)<=down; uh=float(r.h)>=up
        if dh and uh:return 2,"neutral_same_day_order_unknown",day
        if dh:return 0,"down_intraday",day
        if uh:return 1,"rebound_intraday",day
    return 2,"neutral_no_hit",0

def rates(x:pd.DataFrame,col:str,probe_n:int|None=None)->dict:
    valid=x[x[col].notna()]
    result={"n":int(len(valid)),"down_first":None if valid.empty else float(valid[col].eq(0).mean()),"rebound_first":None if valid.empty else float(valid[col].eq(1).mean()),"neutral":None if valid.empty else float(valid[col].eq(2).mean())}
    if probe_n is not None:result["end_to_end_probe_action_down"]=float(valid[col].eq(0).sum()/probe_n) if probe_n else None
    return result

def main()->None:
    p=argparse.ArgumentParser();p.add_argument("--episodes",type=Path,required=True);p.add_argument("--features",type=Path,required=True);p.add_argument("--output-root",type=Path,required=True);a=p.parse_args()
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=a.output_root/f"{stamp}-tradex-trade-reproduction-h5-fixed3-contract-v2";out.mkdir(parents=True,exist_ok=False)
    ep=pd.read_parquet(a.episodes);ft=pd.read_parquet(a.features,columns=["code","ymd","o","h","l","c"]).sort_values(["code","ymd"])
    lookup={}
    for code,g in ft.groupby("code",sort=False):
        g=g.reset_index(drop=True);lookup[str(code).zfill(4)]=(g,{int(v):i for i,v in enumerate(g.ymd)})
    for stage,datecol in (("core","core_ymd"),("add2","add2_ymd")):
        labels=[];kinds=[];days=[]
        for r in ep.itertuples(index=False):
            value=getattr(r,datecol)
            if pd.isna(value) or str(r.code).zfill(4) not in lookup:
                labels.append(None);kinds.append(None);days.append(None);continue
            g,idx=lookup[str(r.code).zfill(4)]; i=idx.get(int(value))
            if i is None or i+5>=len(g):labels.append(None);kinds.append(None);days.append(None);continue
            label,kind,day=classify(g,i);labels.append(label);kinds.append(kind);days.append(day)
        ep[f"{stage}_fixed3_label_5"]=labels;ep[f"{stage}_fixed3_kind_5"]=kinds;ep[f"{stage}_fixed3_hit_day_5"]=days
        ep[f"{stage}_action_year"] = ep[datecol].apply(lambda v: None if pd.isna(v) else int(str(int(v))[:4]))
    years={};families={}
    for y in (2023,2024,2025):
        cohort=ep[ep.year.eq(y)]
        years[str(y)]={"probe_cohort_n":int(len(cohort)),
                       "core_by_action_year":rates(ep[ep.core_action_year.eq(y)],"core_fixed3_label_5"),
                       "add2_by_action_year":rates(ep[ep.add2_action_year.eq(y)],"add2_fixed3_label_5"),
                       "probe_cohort_core_end_to_end":rates(cohort,"core_fixed3_label_5",len(cohort)),
                       "probe_cohort_add2_end_to_end":rates(cohort,"add2_fixed3_label_5",len(cohort))}
    for fam in sorted(ep.family.dropna().unique()):
        families[fam]={}
        for y in (2023,2024,2025):
            cohort=ep[ep.year.eq(y)&ep.family.eq(fam)]
            families[fam][str(y)]={"probe_cohort_n":int(len(cohort)),
                                   "core_by_action_year":rates(ep[ep.core_action_year.eq(y)&ep.family.eq(fam)],"core_fixed3_label_5"),
                                   "add2_by_action_year":rates(ep[ep.add2_action_year.eq(y)&ep.family.eq(fam)],"add2_fixed3_label_5"),
                                   "probe_cohort_core_end_to_end":rates(cohort,"core_fixed3_label_5",len(cohort)),
                                   "probe_cohort_add2_end_to_end":rates(cohort,"add2_fixed3_label_5",len(cohort))}
    anchor=ep[(ep.code.astype(str).str.zfill(4)=="6532")&ep.probe_ymd.eq(20230623)]
    cols=["code","probe_ymd","core_ymd","add2_ymd","core_fixed3_label_5","core_fixed3_kind_5","core_fixed3_hit_day_5","add2_fixed3_label_5","add2_fixed3_kind_5","add2_fixed3_hit_day_5"]
    data={"schema_version":"tradex_trade_reproduction_h5_fixed3_contract_v2.compare.v1","artifact_role":"authoritative_measurement_contract","contract":{"horizon_trading_days":5,"down_barrier":-0.03,"rebound_barrier":0.03,"scan":"exact OHLC t+1..t+5 earliest hit","same_day_both":"neutral_order_unknown","year_attribution":"stage rates use core/add2 action date year; end-to-end remains probe cohort year","relationship_to_existing_atr_label":"supplement; existing label unchanged"},"year_results":years,"family_results":families,"human_anchor_6532":anchor[cols].where(pd.notna(anchor),None).to_dict("records"),"judgment":{"decision":"adopt_as_secondary_trade_reproduction_metric","reason":"fixed symmetric barrier reflects the user's cited 3% practical move while preserving ATR label as primary research comparison"},"not_changed":["episode selection","probe/core/add2 dates","ATR labels","MeeMee","ranking","runtime DB"]}
    (out/"compare.json").write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ep.to_parquet(out/"episode_fixed3_outcome_ledger.parquet",index=False)
    audit={"episode_rows":int(len(ep)),"duplicate_episode":int(ep.duplicated(["code","probe_ymd"]).sum()),"missing_core_fixed3":int(ep[ep.core_ymd.notna()].core_fixed3_label_5.isna().sum()),"missing_add2_fixed3":int(ep[ep.add2_ymd.notna()].add2_fixed3_label_5.isna().sum()),"future_used_for_selection":False,"future_used_for_outcome_only":True,"review_only":True};(out/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json"},indent=2)+"\n",encoding="utf-8")
    print(out);print(json.dumps({"box":families.get("BOX_CEILING_ERASURE"),"anchor":data["human_anchor_6532"],"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
