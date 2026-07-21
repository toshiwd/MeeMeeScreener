from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
from tradex_long_ordinary_pit_compound_tree_v1 import load_rows,metrics

FAMILIES=['ブレイクアウト','押し目','底反転'];COST=.3
def centered(x,c=.55,w=.45):return 1-(x-c).abs()/w
def add_scores(d):
 cols=['ret1','ret5','ret20','ret60','gap_ma20','gap_ma60','ma20_slope5','ma60_slope5','close_pos','lower_wick_ratio','volume_ratio20','realized_vol20','c'];r=d.groupby('date')[cols].rank(pct=True);z=d.copy();price=centered(r.c)
 z['ブレイクアウト']=.22*r.ret20+.18*r.ret60+.18*r.close_pos+.15*r.volume_ratio20+.12*r.ma20_slope5+.10*r.gap_ma20-.10*r.realized_vol20+.05*price
 z['押し目']=.20*r.ret60+.15*r.ma60_slope5-.20*r.ret5-.10*r.ret1+.15*r.close_pos+.10*r.lower_wick_ratio+.10*centered(r.gap_ma20)-.10*r.realized_vol20+.05*price
 z['底反転']=-.25*r.ret20-.15*r.gap_ma60+.20*r.close_pos+.18*r.lower_wick_ratio+.15*r.volume_ratio20-.08*r.realized_vol20+.05*price
 return z
def main():
 p=argparse.ArgumentParser();p.add_argument('--output',required=True);a=p.parse_args();out=Path(a.output);out.mkdir(parents=True,exist_ok=False);sys.path[:0]=[str(Path.cwd()),str(Path.cwd()/'app')]
 from backend.services.codex_bridge_service import get_runtime_stock_db_status
 runtime=get_runtime_stock_db_status();d=load_rows(runtime['selected_runtime_db_path'],broad_trigger=False,min_date='2015-01-01');d=d[d[['p1_o','p20_c','p20_date']].notna().all(axis=1)].copy();d['signal_dt']=pd.to_datetime(d.date,unit='s');d=d[d.signal_dt.ge('2016-01-01')];d=add_scores(d);frames=[]
 for f in FAMILIES:
  x=d.sort_values(['date',f,'code'],ascending=[True,False,True]).groupby('date',sort=False).head(3).copy();x['family']=f;x['family_score']=x[f];x['family_rank']=x.groupby('date')[f].rank(ascending=False,method='first');frames.append(x)
 e=pd.concat(frames,ignore_index=True);e['year']=e.signal_dt.dt.year;e['realized_ret']=100*(e.p20_c/e.p1_o-1)-COST;family_rows=[]
 for f,g in e.groupby('family'):
  family_rows.append({'family':f,'development_2016_2023':metrics(g[g.year.between(2016,2023)]),'validation_2024_2025':metrics(g[g.year.between(2024,2025)]),'test_2026':metrics(g[g.year.eq(2026)]),'yearly':{str(y):metrics(g[g.year.eq(y)]) for y in range(2016,2027)},'monthly_2026':{str(m):metrics(q) for m,q in g[g.year.eq(2026)].groupby(g[g.year.eq(2026)].signal_dt.dt.to_period('M'))}})
 payload={'schema_version':'tradex_long_fresh_family_events_v1.compare.v1','artifact_role':'authoritative','generated_at':datetime.now(timezone.utc).isoformat(),'runtime':runtime,'fixed_evaluation_conditions':{'universe':'ordinary domestic stocks only: Prime, Standard, Growth','source':'current runtime DB regenerated through latest confirmed date','families':FAMILIES,'family_definition':'continuous multi-feature cross-sectional scores; no single-feature hard exclusion','top_k_per_family_day':3,'entry':'next session open','outcome':'session-20 close','round_trip_cost_pct':COST,'development':'2016-2023','validation':'2024-2025','test':'2026 mature events through latest confirmed data','production_changed':False},'authoritative_result':{'event_count':len(e),'family_metrics':family_rows,'latest_signal_date':str(e.signal_dt.max().date()),'latest_exit_date':str(pd.to_datetime(e.p20_date,unit='s').max().date())},'observed_branching':{'changed_top5_members_count':None,'changed_top10_members_count':None,'changed_rank_count':len(e),'selection_divergence_reason':'fresh ordinary-stock multi-family scores replace stale legacy CSV population'},'judgment':{'candidate_local_decision':'hold_for_portfolio_comparison','authoritative_rollup_decision':'hold_for_portfolio_comparison','reason_type':'fresh_family_event_population_created'},'remaining_risks':['capital slots, overlap, monthly and baseline gates not yet applied']}
 keep=['code','stock_name','date','signal_dt','p1_date','p1_o','p5_c','p5_date','p10_c','p10_date','p20_c','p20_date','family','family_score','family_rank','realized_ret','realized_vol20','market_breadth_ma20','market_mean_ret1','market_advancers_ratio','market_dispersion_ret1'];e[keep].to_parquet(out/'fresh_family_events.parquet',index=False);(out/'compare.json').write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8');(out/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json'}),encoding='utf-8');print(json.dumps({'events':len(e),'latest_signal':payload['authoritative_result']['latest_signal_date'],'latest_exit':payload['authoritative_result']['latest_exit_date'],'families':family_rows},ensure_ascii=False))
if __name__=='__main__':main()
