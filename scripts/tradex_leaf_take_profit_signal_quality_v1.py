from __future__ import annotations
import glob,json,math
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
AXIS_ID='leaf_take_profit_signal_quality_v1';OUT=Path(r'G:\Tradex\leaf_take_profit_signal_quality_v1');TPS=(.04,.06,.08,.10,.12);SL=.05;H=10
def outcomes(x,tp):
 z=x.copy();vals=[]
 for r in z.itertuples(index=False):
  th=next((i for i in range(1,H+1) if getattr(r,f'h{i}')>=r.entry_price*(1+tp)),99);sh=next((i for i in range(1,H+1) if getattr(r,f'l{i}')<=r.entry_price*(1-SL)),99)
  vals.append(-SL if sh<=th and sh<=H else tp if th<=H else r.close_horizon/r.entry_price-1)
 z['ret']=vals;return z
def metric(x):
 r=x.ret.astype(float);wins=r[r>0];loss=r[r<0];loss_sum=-loss.sum();avgwin=float(wins.mean()) if len(wins) else None;avgloss=float(-loss.mean()) if len(loss) else None;return {'n':len(r),'expectancy':float(r.mean()),'geometric_mean':float(r.map(lambda v:math.log1p(v)).mean()),'profit_factor':float(wins.sum()/loss_sum) if loss_sum else None,'win_rate':float((r>0).mean()),'average_win':avgwin,'average_loss':avgloss,'payoff_ratio':float(avgwin/avgloss) if avgwin and avgloss else None,'p05':float(r.quantile(.05)),'maximum_loss':float(r.min())}
def gates(m,minimum):return m['n']>=minimum and m['expectancy']>0 and m['profit_factor']>=1.3 and m['win_rate']>=.45 and m['payoff_ratio']>=1.2 and m['p05']>=-.05 and m['maximum_loss']>=-.05
def run():
 source=Path(sorted(glob.glob(r'G:\Tradex\chart_entry_geometry_research_v1\*\eligible_execution_events.csv'))[-1]);x=pd.read_csv(source);rows=[];sets={}
 for tp in TPS:
  z=outcomes(x,tp);sets[tp]=z;parts={'train':metric(z[z.year<=2021]),'validation':metric(z[z.year.between(2022,2023)]),'test':metric(z[z.year>=2024])};rows.append({'take_profit':tp,'metrics_by_split':parts,'train_gate_pass':gates(parts['train'],200)})
 e=[r for r in rows if r['train_gate_pass']];ch=max(e,key=lambda r:(r['metrics_by_split']['train']['geometric_mean'],r['metrics_by_split']['train']['expectancy'],r['metrics_by_split']['train']['profit_factor'])) if e else None;tp=ch['take_profit'] if ch else None;oos=bool(ch and gates(ch['metrics_by_split']['validation'],100) and gates(ch['metrics_by_split']['test'],100));root=OUT/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}";root.mkdir(parents=True);(sets[tp] if tp else x.iloc[:0]).to_csv(root/'selected_events.csv',index=False);payload={'schema_version':f'{AXIS_ID}.compare.v1','artifact_role':'authoritative','axis_id':AXIS_ID,'research_phase':'effectiveness_judgment','fixed_evaluation_conditions':{'source':str(source),'leaf_shape_entry_unchanged':True,'changed_axis':'take profit only','take_profit_levels':TPS,'stop_loss':SL,'maximum_holding_sessions':H,'same_day_dual_hit':'stop first','selection':'2019-2021 mean log return among hard-gate pass','validation':'2022-2023','test':'2024-2025','capital_allocation':'not used'},'variants':rows,'selection':{'selected_take_profit':tp,'selected':ch},'oos_gate_pass':oos,'decision':{'candidate_local_decision':'keep_for_stop_axis' if oos else 'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'silent_fallback_used':False};(root/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');print(root/'compare.json')
if __name__=='__main__':run()
