from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_fresh_family_events_v1 import FAMILIES,add_scores
from tradex_long_fresh_final_practical_v1 import prepare,routed,run,summary
from tradex_long_fresh_pullback_tail_guard_v1 import DEVELOPMENT_RETENTION_QUANTILE,FEATURES,model
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows

LOSS_TRIGGERS=[-1.,-2.,-3.,-5.]

def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);p.add_argument('--events',type=Path,required=True);p.add_argument('--db',type=Path,required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False)
 rows=load_rows(str(a.db),broad_trigger=False,min_date='2016-01-01');rows['signal_dt']=pd.to_datetime(rows.date,unit='s');rows=add_scores(rows);family=FAMILIES[1];pe=rows.sort_values(['date',family,'code'],ascending=[True,False,True]).groupby('date',sort=False).head(3).copy();m=pe[pe.p1_o.notna()&pe.p20_c.notna()].copy();m['ret']=100*(m.p20_c/m.p1_o-1)-.3;m['bad']=m.ret.le(-5).astype(int);m['year']=m.signal_dt.dt.year;dev=m[m.year.between(2016,2023)].copy();oof=pd.Series(index=dev.index,dtype=float)
 for y in range(2020,2024):
  tr=dev[dev.year<y];va=dev[dev.year==y];z=model().fit(tr[FEATURES],tr.bad);oof.loc[va.index]=z.predict_proba(va[FEATURES])[:,1]
 threshold=float(oof.dropna().quantile(DEVELOPMENT_RETENTION_QUANTILE));fit=model().fit(dev[FEATURES],dev.bad)
 source=prepare(a.events);breakout=sorted(source.family.unique())[0];base=routed(source,breakout,.7);missing=[x for x in FEATURES if x not in base.columns];extra=rows[['code','date',*missing]].copy();scored=base.merge(extra,on=['code','date'],how='left',validate='many_to_one');scored['risk']=fit.predict_proba(scored[FEATURES])[:,1];years=pd.to_datetime(scored.date,unit='s').dt.year;scored['day5_ret']=100*(scored.p5_c/scored.entry_price-1)-.3;high=(scored.family==family)&years.ge(2024)&scored.risk.gt(threshold)
 bd,bl=run(base,a.db,.7);baseline=summary(bd,bl);candidates=[];stores={}
 for trigger in LOSS_TRIGGERS:
  e=scored.copy();early=high&e.day5_ret.le(trigger)&e.p5_date.notna();e.loc[early,'exit_date']=e.loc[early,'p5_date'];e.loc[early,'exit_price']=e.loc[early,'p5_c'];d,l=run(e,a.db,.7);s=summary(d,l);stores[trigger]=(d,l)
  def delta(k):
   bp,gp=baseline['periods'][k],s['periods'][k];bt,gt=baseline['trade_metrics'][k],s['trade_metrics'][k]
   return {'return_pct_points':gp['return_pct']-bp['return_pct'],'max_drawdown_pct_points':gp['max_drawdown_pct']-bp['max_drawdown_pct'],'raw_mean_return_pct_points':gt['raw_mean_return_pct']-bt['raw_mean_return_pct'],'raw_win_rate_points':gt['raw_win_rate']-bt['raw_win_rate'],'raw_loss5_rate_points':gt['raw_loss5_rate']-bt['raw_loss5_rate'],'trades':gt['trades']}
  candidates.append({'day5_loss_trigger_pct':trigger,'early_exit_events':int(early.sum()),'result':s,'deltas':{k:delta(k) for k in ['development','validation_2024_2025','audit_2026']}})
 eligible=[x for x in candidates if x['deltas']['validation_2024_2025']['return_pct_points']>0 and x['deltas']['validation_2024_2025']['max_drawdown_pct_points']>=0 and x['deltas']['validation_2024_2025']['raw_loss5_rate_points']<0]
 chosen=max(eligible,key=lambda x:(x['deltas']['validation_2024_2025']['return_pct_points'],x['deltas']['validation_2024_2025']['max_drawdown_pct_points'])) if eligible else None;ad={} if not chosen else chosen['deltas']['audit_2026'];checks={'threshold_and_trigger_selected_without_2026':chosen is not None,'validation_all_three_improve':chosen is not None,'audit_return_improves':ad.get('return_pct_points',-99)>0,'audit_drawdown_not_worse':ad.get('max_drawdown_pct_points',-99)>=0,'audit_loss5_rate_improves':ad.get('raw_loss5_rate_points',99)<0};decision='keep_review_only' if all(checks.values()) else 'drop'
 payload={'schema_version':'tradex_long_fresh_tail_guard_day5_exit_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'fixed_evaluation_conditions':{'source':str(a.events),'runtime_db':str(a.db),'tail_model_training':'2016-2023','tail_risk_threshold':threshold,'day5_loss_triggers_pct':LOSS_TRIGGERS,'selection':'2024-2025 return improvement subject to drawdown and loss5 improvement; 2026 excluded','entry':'next session open','baseline_exit':'session-20 close','challenger_exit':'session-5 close only for high-risk pullback still below chosen return trigger','round_trip_cost_pct':.3,'production_changed':False},'authoritative_result':{'baseline':baseline,'candidates':candidates,'eligible_without_2026':[x['day5_loss_trigger_pct'] for x in eligible],'chosen_without_2026':chosen,'checks':checks},'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'entry candidates and ranks stay fixed; only confirmed adverse high-risk pullbacks exit on session 5'},'judgment':{'candidate_local_decision':decision,'authoritative_rollup_decision':decision,'reason_type':'two_stage_risk_and_adverse_path_exit'},'remaining_risks':['session-5 close action requires end-of-day monitoring','no production reflection authorized']}
 if chosen:
  d,l=stores[chosen['day5_loss_trigger_pct']];d.to_parquet(out/'chosen_daily_nav.parquet',index=False);l.to_parquet(out/'chosen_trade_ledger.parquet',index=False)
 (out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'decision':decision,'eligible':[x['day5_loss_trigger_pct'] for x in eligible],'chosen':None if not chosen else chosen['day5_loss_trigger_pct'],'chosen_deltas':None if not chosen else chosen['deltas'],'checks':checks},ensure_ascii=False))
if __name__=='__main__':main()
