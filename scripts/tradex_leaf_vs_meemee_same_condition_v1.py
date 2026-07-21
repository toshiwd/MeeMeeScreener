from __future__ import annotations
import argparse,json,glob
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics

AXIS_ID='leaf_vs_meemee_same_condition_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf_vs_meemee_same_condition_v1');TP=.08;SL=.05;H=10
def meemee_events(db:Path)->pd.DataFrame:
 highs=', '.join(f'lead(h,{i}) over w h{i}' for i in range(1,H+1));lows=', '.join(f'lead(l,{i}) over w l{i}' for i in range(1,H+1));dates=', '.join(f'lead(date,{i}) over w d{i}' for i in range(1,H+1));tp='least('+','.join(f'case when h{i}>=next_open*1.08 then {i} else 99 end' for i in range(1,H+1))+')';sl='least('+','.join(f'case when l{i}<=next_open*.95 then {i} else 99 end' for i in range(1,H+1))+')'
 q=f"""WITH b AS (SELECT code,date,CAST(strftime(to_timestamp(date),'%Y%m%d') AS INTEGER) ymd,o,h,l,c,lead(o,1) over w next_open,lead(date,1) over w next_entry_date,lead(c,10) over w close_horizon,{highs},{lows},{dates} FROM daily_bars WHERE source='pan' WINDOW w AS(PARTITION BY code ORDER BY date)), s AS (SELECT r.code,r.dt date,r.rank,b.* EXCLUDE(code),{tp} tpday,{sl} slday FROM ranking_appearance_daily r JOIN b ON b.code=r.code AND b.ymd=r.dt WHERE r.dir='up' AND r.rank<=5 AND r.ranking_logic_version='ranking:trade:top50:v1' AND r.dt BETWEEN 20240101 AND 20251231 AND b.next_open/b.c-1<=0 AND b.close_horizon IS NOT NULL) SELECT code,date,next_entry_date,next_open AS entry_price,rank,CASE WHEN slday<=10 AND slday<=tpday THEN CASE slday {''.join(f' when {i} then d{i}' for i in range(1,H+1))} END WHEN tpday<=10 THEN CASE tpday {''.join(f' when {i} then d{i}' for i in range(1,H+1))} END ELSE d10 END exit_date,CASE WHEN slday<=10 AND slday<=tpday THEN -.05 WHEN tpday<=10 THEN .08 ELSE close_horizon/next_open-1 END next_open_return,CASE WHEN date<20250101 THEN 'validation' ELSE 'test' END split,CAST(FLOOR(date/10000) AS INTEGER) year_value FROM s ORDER BY next_entry_date,rank,code"""
 with duckdb.connect(str(db),read_only=True) as c:x=c.execute(q).fetchdf()
 return x.rename(columns={'year_value':'year'})
def cap5(x:pd.DataFrame)->pd.DataFrame:
 accepted=[];active=[]
 for d,g in x.groupby('next_entry_date',sort=True):
  active=[z for z in active if z>=d];avail=5-len(active)
  for i,r in g.sort_values(['rank','code']).head(max(0,avail)).iterrows():accepted.append(i);active.append(r.exit_date)
 return x.loc[accepted].copy()
def budget(x:pd.DataFrame)->dict:
 pnl=x.assign(pnl=x.next_open_return*2_000_000).groupby('exit_date').pnl.sum().sort_index();eq=10_000_000+pnl.cumsum();dd=eq-eq.cummax();splits,yearly=_metrics(x,'next_open_return');return {'trade_count':len(x),'net_pnl_yen':float((x.next_open_return*2_000_000).sum()),'ending_capital_yen':float(10_000_000+(x.next_open_return*2_000_000).sum()),'max_realized_drawdown_yen':float(dd.min()) if len(dd) else 0,'metrics_by_split':splits,'yearly':yearly}
def run(db:Path,out:Path):
 m=cap5(meemee_events(db));leafp=sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\budget_10m_cap5_events.csv'))[-1];leaf=pd.read_csv(leafp);leaf=leaf[leaf.year.between(2024,2025)].copy();leaf['split']=leaf.year.map(lambda y:'validation' if y==2024 else 'test');root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);m.to_csv(root/'meemee_cap5_events.csv',index=False);leaf.to_csv(root/'leaf_cap5_2024_2025_events.csv',index=False);lm=budget(leaf);mm=budget(m);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','boundary_owner':'TRADEX','fixed_evaluation_conditions':{'period':'2024-2025 ranking history intersection','entry':'next session open only when gap<=0','take_profit':TP,'stop_loss':SL,'max_hold_rows':H,'same_day_dual_hit':'stop first','capital_yen':10_000_000,'slot_yen':2_000_000,'max_positions':5,'costs':'not modeled'},'current_meemee_up_top5':mm,'leaf_rule':lm,'comparison':{'net_pnl_delta_leaf_minus_meemee_yen':lm['net_pnl_yen']-mm['net_pnl_yen'],'test_pf_delta_leaf_minus_meemee':lm['metrics_by_split'].get('test',{}).get('profit_factor',0)-mm['metrics_by_split'].get('test',{}).get('profit_factor',0)},'decision':{'candidate_local_decision':'keep' if lm['net_pnl_yen']>mm['net_pnl_yen'] and lm['metrics_by_split']['test']['profit_factor']>mm['metrics_by_split']['test']['profit_factor'] else 'hold','authoritative_rollup_decision':'research_only','reason_type':'same_condition_capital_and_test_pf_comparison'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False}
 (root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--db',type=Path,default=DB);p.add_argument('--out',type=Path,default=OUT);a=p.parse_args();run(a.db,a.out)
