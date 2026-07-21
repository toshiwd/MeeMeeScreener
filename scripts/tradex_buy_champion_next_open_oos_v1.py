from __future__ import annotations

import argparse,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import duckdb,pandas as pd

AXIS_ID="tradex_buy_champion_next_open_oos_v1"
EVENTS=Path(r"G:\Tradex\buy_multitimeframe_meemee_shadow_compare_v1\20260618T075125Z-buy_multitimeframe_meemee_shadow_compare_v1\current_meemee_up_top5_events.csv")
DB=Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT=Path(r"G:\Tradex\buy_champion_next_open_oos_v1")
THRESHOLD=.04103479926322488; STOP=.03; HOLD=20

def pf(s:pd.Series)->float|None:
    g=float(s[s>0].sum()); l=float(-s[s<0].sum()); return g/l if l else None
def summary(d:pd.DataFrame,col:str)->dict[str,Any]:
    s=d[col]; return {"n":int(len(s)),"expectancy":float(s.mean()),"profit_factor":pf(s),"win_rate":float((s>0).mean()),"stop_rate":float(d[f"{col}_stop"].mean()),"p05":float(s.quantile(.05))}
def sim(path:pd.DataFrame,entry:float)->tuple[float,bool]:
    if (path.l<=entry*(1-STOP)).any(): return -STOP,True
    return float(path.iloc[-1].c)/entry-1,False

def run(events_path:Path,db_path:Path,out:Path)->Path:
    events=pd.read_csv(events_path,dtype={"code":str}); codes=sorted(events.code.unique()); q=','.join(['?']*len(codes))
    with duckdb.connect(str(db_path),read_only=True) as c:
        bars=c.execute(f"""WITH n AS (SELECT code,CASE WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER) ELSE CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) END ymd,o,h,l,c FROM daily_bars WHERE source='pan' AND code IN ({q})), b AS (SELECT *,avg(c) OVER(PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) ma60 FROM n) SELECT *,ma60/lag(ma60,20) OVER(PARTITION BY code ORDER BY ymd)-1 ma60_slope20 FROM b ORDER BY code,ymd""",codes).fetchdf()
    features=bars[["code","ymd","ma60_slope20"]].copy(); events=events.merge(features,on=["code","ymd"],how="inner"); events=events[events.ma60_slope20>=THRESHOLD].copy()
    by_code={str(k):v.reset_index(drop=True) for k,v in bars.groupby("code")}; rows=[]
    for e in events.to_dict("records"):
        future=by_code[str(e["code"])]; future=future[future.ymd>int(e["ymd"])].head(HOLD)
        if len(future)<HOLD: continue
        close=float(e["close"]); op=float(future.iloc[0].o); cr,cs=sim(future,close); nr,ns=sim(future,op)
        rows.append({"code":str(e["code"]),"ymd":int(e["ymd"]),"year":int(str(e["ymd"])[:4]),"lower_wick_ratio":max(min(float(e["open"]),float(e["close"]))-float(e["low"]),0)/max(float(e["high"])-float(e["low"]),1e-9),"ma60_slope20":e["ma60_slope20"],"gap_pct":op/close-1,"close_return":cr,"close_return_stop":cs,"next_open_return":nr,"next_open_return_stop":ns})
    d=pd.DataFrame(rows); splits={}
    for year,label in ((2024,"train"),(2025,"test")):
        g=d[d.year==year]; splits[label]={"signal_close":summary(g,"close_return"),"next_open":summary(g,"next_open_return")}
    train=splits["train"]; test=splits["test"]
    train_pass=(train["next_open"]["profit_factor"] or 0)>(train["signal_close"]["profit_factor"] or 0) and train["next_open"]["stop_rate"]<train["signal_close"]["stop_rate"]
    test_pass=(test["next_open"]["profit_factor"] or 0)>(test["signal_close"]["profit_factor"] or 0) and test["next_open"]["stop_rate"]<test["signal_close"]["stop_rate"]
    root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True)
    payload={"schema_version":f"{AXIS_ID}_v1","generated_at":datetime.now(timezone.utc).isoformat(),"axis_id":AXIS_ID,"boundary_owner":"TRADEX","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"source":"current MeeMee D/up/trade top5 events","champion_filter":f"ma60_slope20>={THRESHOLD}","changed_axis":"execution only","stop":STOP,"hold":HOLD,"train":2024,"untouched_test":2025,"costs":"not modeled"},"splits":splits,"gates":{"train_pass":train_pass,"test_pass":test_pass},"decision":{"candidate_local_decision":"keep" if train_pass and test_pass else "drop","authoritative_rollup_decision":"research_only","reason":"next-open must improve PF and reduce stop rate in both train and untouched test"},"runtime_db_write":False,"production_ranking_changed":False,"silent_fallback_used":False}
    d.to_csv(root/"events.csv",index=False); (root/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); print(root/"compare.json"); return root

if __name__=="__main__":
    p=argparse.ArgumentParser(); p.add_argument('--events',type=Path,default=EVENTS);p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();run(a.events,a.db,a.out)
