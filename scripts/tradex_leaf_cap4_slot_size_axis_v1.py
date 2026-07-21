from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_shallow_high_zone_next_open_execution_v1 import _metrics
AXIS_ID='leaf_cap4_slot_size_axis_v1';OUT=Path(r'G:\Tradex\leaf_cap4_slot_size_axis_v1');SIZES=(2_000_000,2_200_000,2_300_000,2_400_000,2_500_000);TRAIN_DD_CAP=-1_000_000;FINAL_DD_CAP=-1_500_000
def dd(x,slot):
 p=x.assign(pnl=x.next_open_return*slot).groupby('exit_date').pnl.sum().sort_index();eq=1e7+p.cumsum();return float((eq-eq.cummax()).min())
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\leaf_position_cap_axis_v1\*\selected_events.csv'))[-1]);x=pd.read_csv(source);splits,yearly=_metrics(x,'next_open_return');rows=[]
 for slot in SIZES:rows.append({'slot_yen':slot,'capital_utilization':slot*4/1e7,'train_dd_yen':dd(x[x.year<=2021],slot),'validation_dd_yen':dd(x[x.year.between(2022,2023)],slot),'test_dd_yen':dd(x[x.year>=2024],slot),'all_dd_yen':dd(x,slot),'pnl_2024_2025_yen':float(x[x.year>=2024].next_open_return.sum()*slot),'net_pnl_2019_2025_yen':float(x.next_open_return.sum()*slot),'ending_capital_yen':float(1e7+x.next_open_return.sum()*slot),'annual_pnl_yen':{str(int(y)):float(g.next_open_return.sum()*slot) for y,g in x.groupby('year')},'metrics_by_split':splits,'red_year_count':sum((r['daily_expectancy'] or 0)<=0 for r in yearly)})
 eligible=[r for r in rows if r['train_dd_yen']>=TRAIN_DD_CAP and r['slot_yen']*4<=1e7];ch=max(eligible,key=lambda r:r['slot_yen']) if eligible else None;ok=bool(ch and ch['pnl_2024_2025_yen']>4964781 and ch['metrics_by_split']['test']['profit_factor']>=1.5 and ch['all_dd_yen']>=FINAL_DD_CAP and ch['red_year_count']==0)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf_shape_exit_rank_cap_unchanged':True,'maximum_positions':4,'changed_axis':'fixed slot notional only','sizes':SIZES,'selection':'largest slot with 2019-2021 realized DD no worse than -1m and total notional<=10m','final_dd_floor':FINAL_DD_CAP,'costs':'not modeled'},'variants':rows,'selection':{'selected_slot_yen':ch['slot_yen'] if ch else None,'selected':ch},'target_gate':{'pass':ok,'requirements':'2024-25 pnl>4,964,781; test PF>=1.5; all DD>=-1.5m; no red year'},'decision':{'candidate_local_decision':'keep_for_operational_validation' if ok else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
