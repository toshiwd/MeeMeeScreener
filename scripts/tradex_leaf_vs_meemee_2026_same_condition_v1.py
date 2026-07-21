from __future__ import annotations
import argparse,hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
if __package__ in (None,""):sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.tradex_leaf_order_contract_readiness_v1 import replay

DB=Path(r"G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb")
LEAF=Path(r"G:\Tradex\leaf_cap4_slot24_2026_matured_oos_v1\20260713T045756Z-tradex_leaf_cap4_slot24_2026_matured_oos_v1\event_ledger_2026.csv")
OUT=Path(r"G:\Tradex\leaf_vs_meemee_2026_same_condition_v1")
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def meemee(db:Path,start:int,end:int)->pd.DataFrame:
 h=','.join(f'lead(h,{i})over w h{i}' for i in range(1,11));l=','.join(f'lead(l,{i})over w l{i}' for i in range(1,11));ds=','.join(f'lead(date,{i})over w d{i}' for i in range(1,11));tp='least('+','.join(f'case when h{i}>=next_open*1.08 then {i} else 99 end' for i in range(1,11))+')';sl='least('+','.join(f'case when l{i}<=next_open*.95 then {i} else 99 end' for i in range(1,11))+')'
 q=f"""with latest as(select max(date) d from daily_bars where source='pan'),u as(select code from daily_bars where source='pan' group by code having max(date)=(select d from latest)),b as(select code,date,cast(strftime(to_timestamp(date),'%Y%m%d')as int)ymd,c,lead(o)over w next_open,lead(date)over w next_entry_date,lead(c,10)over w close_horizon,{h},{l},{ds} from daily_bars join u using(code) where source='pan' window w as(partition by code order by date)),s as(select r.code,r.dt signal_ymd,r.rank,b.* exclude(code),{tp}tpday,{sl}slday from ranking_appearance_daily r join b on b.code=r.code and b.ymd=r.dt where r.dir='up' and r.rank<=3 and r.ranking_logic_version='ranking:trade:top50:v1' and r.dt between {start} and {end} and b.close_horizon is not null and b.next_open/b.c-1<=0)select code,date,next_entry_date,2026 signal_year,'test' split,next_open entry_price,-rank tie_gap_ma60,case when slday<=10 and slday<=tpday then case slday {' '.join(f'when {i} then d{i}' for i in range(1,11))} end when tpday<=10 then case tpday {' '.join(f'when {i} then d{i}' for i in range(1,11))} end else d10 end exit_date,case when slday<=10 and slday<=tpday then -.05 when tpday<=10 then .08 else close_horizon/next_open-1 end next_open_return from s"""
 with duckdb.connect(str(db),read_only=True)as c:return c.execute(q).fetchdf()
def metric(x):
 r=x.pnl_yen/x.invested_yen if len(x) else pd.Series(dtype=float);neg=-r[r<0].sum()
 return {'n':len(x),'signal_days':int(x.next_entry_date.nunique()) if len(x) else 0,'expectancy':float(r.mean()) if len(r) else None,'profit_factor':float(r[r>0].sum()/neg) if neg else None,'win_rate':float((r>0).mean()) if len(r) else None,'pnl_yen':float(x.pnl_yen.sum()) if len(x) else 0.0}
def generate(db:Path,leaf:Path,out:Path)->Path:
 with duckdb.connect(str(db),read_only=True)as c:
  row=c.execute("select min(dt),max(dt),count(distinct dt) from ranking_appearance_daily where dir='up' and rank<=3 and ranking_logic_version='ranking:trade:top50:v1' and dt between 20260101 and 20261231").fetchone()
 start,end,rank_days=map(int,row)
 l=pd.read_csv(leaf);l=l[(l.next_entry_date.apply(lambda x:int(datetime.fromtimestamp(int(x),timezone.utc).strftime('%Y%m%d')))>=start)&(l.next_entry_date.apply(lambda x:int(datetime.fromtimestamp(int(x),timezone.utc).strftime('%Y%m%d')))<=end)].copy()
 mraw=meemee(db,start,end);mraw['year']=mraw.signal_year;m,_=replay(mraw,.001)
 now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_leaf_vs_meemee_2026_same_condition_v1";root.mkdir(parents=True);m.to_csv(root/'meemee_events.csv',index=False);l.to_csv(root/'leaf_events.csv',index=False)
 payload={'schema_version':'tradex_leaf_vs_meemee_2026_same_condition_v1.compare.v1','artifact_role':'authoritative','generated_at':now.isoformat(),'fixed_evaluation_conditions':{'period_intersection':[start,end],'ranking_history_days':rank_days,'universe':'PAN symbols current at snapshot latest date','top_k':3,'entry':'next_session_open_gap_le_0','tp':.08,'sl':.05,'horizon_sessions':10,'adverse_fill':.001,'maximum_positions':4,'slot_budget_yen':2400000,'ranking_logic_version':'ranking:trade:top50:v1'},'leaf':metric(l),'meemee':metric(m),'comparison':{'expectancy_delta_leaf_minus_meemee':None if not len(l) or not len(m) else metric(l)['expectancy']-metric(m)['expectancy'],'pnl_delta_leaf_minus_meemee_yen':metric(l)['pnl_yen']-metric(m)['pnl_yen']},'coverage_limitation':{'typed_reason':'RANKING_HISTORY_ENDS_20260605','excluded_after':end},'source_artifacts':[{'path':str(db)},{'path':str(leaf),'sha256':sha(leaf)}],'decision':{'authoritative_rollup_decision':'research_only_same_condition_2026'},'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return p
def main():
 a=argparse.ArgumentParser();a.add_argument('--db',type=Path,default=DB);a.add_argument('--leaf',type=Path,default=LEAF);a.add_argument('--out',type=Path,default=OUT);x=a.parse_args();print(generate(x.db,x.leaf,x.out))
if __name__=='__main__':main()
