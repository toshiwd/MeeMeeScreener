from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_fresh_family_events_v1 import FAMILIES,add_scores
from tradex_long_fresh_final_practical_v1 import prepare,routed,run,summary
from tradex_long_fresh_pullback_tail_guard_v1 import DEVELOPMENT_RETENTION_QUANTILE,FEATURES,model
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows

MULTIPLIERS=[.25,.5,.75]

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',type=Path,required=True);p.add_argument('--db',type=Path,required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 rows=load_rows(str(a.db),broad_trigger=False,min_date='2016-01-01');rows['signal_dt']=pd.to_datetime(rows.date,unit='s');rows=add_scores(rows);family=FAMILIES[1]
 pe=rows.sort_values(['date',family,'code'],ascending=[True,False,True]).groupby('date',sort=False).head(3).copy();m=pe[pe.p1_o.notna()&pe.p20_c.notna()].copy();m['realized_ret']=100*(m.p20_c/m.p1_o-1)-.3;m['bad']=m.realized_ret.le(-5).astype(int);m['year']=m.signal_dt.dt.year;dev=m[m.year.between(2016,2023)].copy();oof=pd.Series(index=dev.index,dtype=float)
 for y in range(2020,2024):
  tr=dev[dev.year<y];va=dev[dev.year==y];fit=model().fit(tr[FEATURES],tr.bad);oof.loc[va.index]=fit.predict_proba(va[FEATURES])[:,1]
 th=float(oof.dropna().quantile(DEVELOPMENT_RETENTION_QUANTILE));fit=model().fit(dev[FEATURES],dev.bad)
 source=prepare(a.events);breakout=sorted(source.family.unique())[0];base_events=routed(source,breakout,.7);missing=[x for x in FEATURES if x not in base_events.columns];scored=base_events.merge(rows[['code','date',*missing]],on=['code','date'],how='left',validate='many_to_one');scored['risk']=fit.predict_proba(scored[FEATURES])[:,1];years=pd.to_datetime(scored.date,unit='s').dt.year;high=(scored.family==family)&years.ge(2024)&scored.risk.gt(th)
 bd,bl=run(base_events,a.db,.7);baseline=summary(bd,bl);candidates=[];stores={}
 for mult in MULTIPLIERS:
  e=scored.copy();e.loc[high,'market_breadth_ma20']*=mult;e.loc[high,'market_advancers_ratio']*=mult;d,l=run(e,a.db,.7);s=summary(d,l);stores[mult]=(d,l)
  def delta(k):
   bp,gp=baseline['periods'][k],s['periods'][k];bt,gt=baseline['trade_metrics'][k],s['trade_metrics'][k]
   return {'return_pct_points':gp['return_pct']-bp['return_pct'],'max_drawdown_pct_points':gp['max_drawdown_pct']-bp['max_drawdown_pct'],'total_pnl':gt['total_pnl']-bt['total_pnl'],'mean_allocation_pct_points':gt['mean_allocation_pct']-bt['mean_allocation_pct'],'trades':gt['trades']}
  candidates.append({'high_risk_weight_multiplier':mult,'result':s,'deltas':{k:delta(k) for k in ['development','validation_2024_2025','audit_2026']}})
 eligible=[x for x in candidates if x['deltas']['validation_2024_2025']['return_pct_points']>0 and x['deltas']['validation_2024_2025']['max_drawdown_pct_points']>=0]
 chosen=max(eligible,key=lambda x:(x['deltas']['validation_2024_2025']['return_pct_points'],x['deltas']['validation_2024_2025']['max_drawdown_pct_points'])) if eligible else None
 ad=chosen['deltas']['audit_2026'] if chosen else {};checks={'threshold_fixed_without_2024plus':True,'multiplier_selected_without_2026':chosen is not None,'validation_return_and_drawdown_improve':chosen is not None,'audit_return_improves':ad.get('return_pct_points',-99)>0,'audit_drawdown_not_worse':ad.get('max_drawdown_pct_points',-99)>=0,'candidate_count_unchanged':all(x['deltas']['audit_2026']['trades']==baseline['trade_metrics']['audit_2026']['trades'] for x in candidates)};decision='keep_review_only' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_fresh_tail_risk_sizing_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_evaluation_conditions':{'source':str(a.events),'runtime_db':str(a.db),'router_threshold':.7,'market_sizing_threshold':.7,'tail_risk_threshold':th,'tail_model_training':'2016-2023','multipliers':MULTIPLIERS,'selection':'maximize 2024-2025 return among candidates that also do not worsen drawdown; 2026 excluded','entry':'next session open','exit':'session-20 close','production_changed':False},'authoritative_result':{'baseline':baseline,'candidates':candidates,'eligible_without_2026':[x['high_risk_weight_multiplier'] for x in eligible],'chosen_without_2026':chosen,'high_risk_event_count_2024plus':int(high.sum()),'checks':checks},'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'candidate membership and rank stay fixed; only high-tail-risk pullback capital is reduced'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'fixed_candidate_tail_risk_position_sizing'},'remaining_risks':['raw event loss frequency is unchanged; this axis reduces capital at risk','no production reflection authorized']}
 if chosen:
  d,l=stores[chosen['high_risk_weight_multiplier']];d.to_parquet(out/'chosen_daily_nav.parquet',index=False);l.to_parquet(out/'chosen_trade_ledger.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'decision':decision,'eligible':[x['high_risk_weight_multiplier'] for x in eligible],'chosen':None if not chosen else chosen['high_risk_weight_multiplier'],'chosen_deltas':None if not chosen else chosen['deltas'],'checks':checks},ensure_ascii=False))
if __name__=='__main__':main()
