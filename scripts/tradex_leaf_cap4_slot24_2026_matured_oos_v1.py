from __future__ import annotations
import argparse,hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
import duckdb
if __package__ in (None,""):sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.tradex_leaf_order_contract_readiness_v1 import replay

COMPARE=Path(r"G:\Tradex\chart_entry_geometry_research_v1\20260711T104710Z-shallow_high_zone_next_open_execution_v1\compare.json")
NIGHTLY_DB=Path(r"G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb")
INVALID_PREDECESSOR=r"G:\Tradex\leaf_cap4_slot24_2026_matured_oos_v1\20260713T045423Z-tradex_leaf_cap4_slot24_2026_matured_oos_v1"
OUT=Path(r"G:\Tradex\leaf_cap4_slot24_2026_matured_oos_v1")
def h(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def pf(x):
 n=-x[x<0].sum();return None if n==0 else float(x[x>0].sum()/n)
def candidates(db:Path,maximum_gap:float)->pd.DataFrame:
 leads=lambda col:','.join(f'lead({col},{i}) over w {col}{i}' for i in range(1,11))
 dates=','.join(f'lead(date,{i}) over w d{i}' for i in range(1,11))
 hit=lambda col,mul: 'least('+','.join(f'case when {col}{i}>=next_open*{mul} then {i} else 99 end' if col=='h' else f'case when {col}{i}<=next_open*{mul} then {i} else 99 end' for i in range(1,11))+')'
 tp,sl=hit('h',1.08),hit('l',.95)
 sql=f"""with latest as(select max(date) date from daily_bars where source='pan'), eligible as(select code from daily_bars where source='pan' group by code having max(date)=(select date from latest)), b as(select code,date,o,h,l,c,v,lag(h)over w ph1,max(h)over p20 ph20,lag(c,10)over w c10,avg(c)over m20 ma20,avg(c)over m20l ma20_5,avg(c)over m60 ma60,avg(v)over m20 avgv,max(h)over l10 hi10,min(l)over l10 lo10,lead(o)over w next_open,lead(date)over w next_entry_date,lead(date,10)over w horizon_date,lead(c,10)over w close_horizon,{leads('h')},{leads('l')},{dates} from daily_bars join eligible using(code) where source='pan' window w as(partition by code order by date),p20 as(partition by code order by date rows between 20 preceding and 1 preceding),m20 as(partition by code order by date rows between 19 preceding and current row),m20l as(partition by code order by date rows between 24 preceding and 5 preceding),m60 as(partition by code order by date rows between 59 preceding and current row),l10 as(partition by code order by date rows between 9 preceding and current row)),s as(select *,{tp} tpday,{sl} slday,year(to_timestamp(cast(date as bigint))) signal_year,case when c/ma60-1<=.0370751 and ma20/ma20_5-1>.0107939 then 9 when c/ma60-1>.0370751 and hi10/lo10-1<=.0544289 and v/avgv<=.995031 and c/ph1-1>.000984905 then 14 when c/ma60-1>.0370751 and hi10/lo10-1>.0544289 and c/ph1-1>.0233188 then 20 end shape_leaf from b where date between 1420070400 and(select date from latest) and next_open is not null and close_horizon is not null and ph1 is not null and ph20 is not null and ma20 is not null and ma20_5 is not null and ma60 is not null and avgv>0 and h>l and ma20>ma60 and c>=ph20*.95 and l between ma20*.99 and ma20 and c>ma20 and c>o and (c-l)/(h-l)>=.70 and ((c/ma60-1<=.0370751 and ma20/ma20_5-1>.0107939)or(c/ma60-1>.0370751 and hi10/lo10-1<=.0544289 and v/avgv<=.995031 and c/ph1-1>.000984905)or(c/ma60-1>.0370751 and hi10/lo10-1>.0544289 and c/ph1-1>.0233188))) select code,date,next_entry_date,horizon_date,cast(signal_year as int) signal_year,shape_leaf,next_open entry_price,c/ma60-1 tie_gap_ma60,(next_open/c)-1 next_open_gap,case when slday<=10 and slday<=tpday then case slday {' '.join(f'when {i} then d{i}' for i in range(1,11))} end when tpday<=10 then case tpday {' '.join(f'when {i} then d{i}' for i in range(1,11))} end else d10 end exit_date,case when slday<=10 and slday<=tpday then -.05 when tpday<=10 then .08 else close_horizon/next_open-1 end next_open_return,case when signal_year<=2021 then 'train' when signal_year<=2023 then 'validation' else 'test' end split from s where (next_open/c)-1<={maximum_gap}"""
 with duckdb.connect(str(db),read_only=True) as con:return con.execute(sql).fetchdf()
def generate(compare:Path,out:Path,db_override:Path|None=None)->Path:
 d=json.loads(compare.read_text(encoding="utf-8"));f=d["fixed_evaluation_conditions"]
 if (f.get("take_profit"),f.get("stop_loss"),f.get("max_holding_days"))!=(.08,.05,10):raise ValueError("FROZEN_CONTRACT_MISMATCH")
 selected=d.get("train_only_selected_gap_execution",{}).get("selected_maximum_next_open_gap")
 if selected!=0.0:raise ValueError("AUTHORITATIVE_SELECTED_GAP_NOT_ZERO")
 db=db_override or NIGHTLY_DB;x=candidates(db,selected);x["year"]=x.signal_year;z,summary=replay(x,.001);o=z[z.signal_year==2026].copy()
 if len(o) and ((o.shape_leaf.isin([9,14,20])==False).any() or (o.horizon_date>int(x.horizon_date.max())).any()):raise RuntimeError("EVENT_CONTRACT_VIOLATION")
 now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_leaf_cap4_slot24_2026_matured_oos_v1";root.mkdir(parents=True)
 ledger=root/"event_ledger_2026.csv";o.to_csv(ledger,index=False);r=o.next_open_return;days=o.next_entry_date.nunique()
 m={"n":int(len(o)),"signal_days":int(days),"expectancy":float(r.mean()) if len(o) else None,"profit_factor":pf(r),"win_rate":float((r>0).mean()) if len(o) else None,"avg_win":float(r[r>0].mean()) if (r>0).any() else None,"avg_loss":float(r[r<0].mean()) if (r<0).any() else None,"p05":float(r.quantile(.05)) if len(o) else None,"p01":float(r.quantile(.01)) if len(o) else None,"pnl_yen":float(o.pnl_yen.sum())}
 p={"schema_version":"tradex_leaf_cap4_slot24_2026_matured_oos_v1.manifest.v1","artifact_role":"authoritative","generated_at":now.isoformat(),"oos_status":"matured_2026_only","invalid_predecessor":{"path":INVALID_PREDECESSOR,"reason":"WRONG_MAXIMUM_NEXT_OPEN_GAP_0_02"},"frozen_contract":{"leaves":[9,14,20],"maximum_next_open_gap":selected,"entry":"next_session_open","tp":.08,"sl":.05,"horizon_sessions":10,"adverse_fill":.001,"maximum_positions":4,"slot_budget_yen":2400000,"same_day_candidate_cap":3,"ranking":"tie_gap_ma60_desc_then_code"},"maturity_rule":"query requires non-null next_open and 10-session close_horizon; exits use only the following 10 sessions","source_artifacts":[{"path":str(compare),"sha256":h(compare)},{"path":str(db),"latest_confirmed_date":f.get("confirmed_latest_date")}],"candidate_rows_2026":int((x.signal_year==2026).sum()),"ledger":{"path":str(ledger),"sha256":h(ledger)},"metrics":m,"full_replay_summary":summary,"rules_or_thresholds_changed":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False};mp=root/"manifest.json";mp.write_text(json.dumps(p,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");return mp
def main():
 a=argparse.ArgumentParser();a.add_argument("--compare",type=Path,default=COMPARE);a.add_argument("--out",type=Path,default=OUT);a.add_argument("--db",type=Path,default=NIGHTLY_DB);q=a.parse_args();print(generate(q.compare,q.out,q.db))
if __name__=="__main__":main()
