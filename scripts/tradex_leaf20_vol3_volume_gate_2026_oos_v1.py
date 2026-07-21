from __future__ import annotations
import glob,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
from tradex_leaf_take_profit_signal_quality_v1 import metric
AXIS_ID='leaf20_vol3_volume_gate_2026_oos_v1';DB=Path(r'C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb');OUT=Path(r'G:\Tradex\leaf20_vol3_volume_gate_2026_oos_v1');FLOORS=(.8,1.0,1.2,1.5,2.0)
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\prehistory_reference_events.csv'))[-1]);x=pd.read_csv(source);x=x[x.shape_leaf==20].copy();x['code']=x.code.astype(str);x['ret']=x.next_open_return
 q="""WITH b AS(SELECT code,date,c/lag(c) over(partition by code order by date)-1 r FROM daily_bars WHERE source='pan') SELECT code,date,stddev_samp(r) over(partition by code order by date rows between 19 preceding and current row) vol20 FROM b"""
 with duckdb.connect(str(DB),read_only=True) as c:v=c.execute(q).fetchdf();v['code']=v.code.astype(str);x=x.merge(v,on=['code','date'],how='left');x=x[x.vol20<.03];dev=x[x.year.between(2019,2025)];reports=[];sets={}
 for floor in FLOORS:
  z=dev[dev.tie_volume_ratio>=floor];sets[floor]=z;m=metric(z);reports.append({'volume_ratio_floor':floor,'development_2019_2025':m,'development_gate_pass':m['n']>=200 and m['expectancy']>0 and m['profit_factor']>=1.2 and m['win_rate']>=.45 and m['payoff_ratio']>=1.1})
 valid=[r for r in reports if r['development_gate_pass']];chosen=max(valid,key=lambda r:(r['development_2019_2025']['geometric_mean'],r['development_2019_2025']['expectancy'])) if valid else None;floor=chosen['volume_ratio_floor'] if chosen else None;oos=x[(x.year==2026)&(x.tie_volume_ratio>=floor)].copy() if floor else x.iloc[:0];oos_m=metric(oos);positive=bool(oos_m['n']>=5 and oos_m['expectancy']>0 and (oos_m['profit_factor'] or 0)>1 and oos_m['win_rate']>=.45)
 root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);oos.to_csv(root/'oos_2026_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'base_rule':'leaf20 vol20<3% next-open gap<=0 TP8 SL5 H10','changed_axis':'signal-day volume/MA20 floor only','floors':FLOORS,'selection_period':'2019-2025 only','untouched_oos':'2026 completed 10-session paths through confirmed 2026-07-10','capital_allocation':'not used'},'development_variants':reports,'selection':{'selected_volume_ratio_floor':floor,'selected':chosen},'oos_2026_metrics':oos_m,'oos_positive_gate_pass':positive,'decision':{'candidate_local_decision':'keep_for_current_selection' if positive else 'drop','authoritative_rollup_decision':'research_only','reason_type':'2019-2025 selected volume floor applied untouched to 2026'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
