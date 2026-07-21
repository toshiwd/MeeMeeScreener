import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=range(2021,2026)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rates(x):
 n=len(x);return {'n':n,'down_first':int(x.outcome_fixed3_h5.eq('down_first').sum()),'rebound_first':int(x.outcome_fixed3_h5.eq('rebound_first').sum()),'neutral_no_hit':int(x.outcome_fixed3_h5.eq('neutral_no_hit').sum()),'down_first_pct':None if not n else 100*x.outcome_fixed3_h5.eq('down_first').mean(),'rebound_first_pct':None if not n else 100*x.outcome_fixed3_h5.eq('rebound_first').mean()}
def main():
 p=argparse.ArgumentParser();p.add_argument('--actions',type=Path,required=True);p.add_argument('--features',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
 x=pd.read_parquet(a.actions);x=x[x.action_lanes.str.contains('STAGED_CORE')&~x.research_fallback].copy()
 f=pd.read_parquet(a.features,columns=['code','ymd','bear_count5','upper_supply_count5','dist_ma20_atr','cross_ma20','weekly_lower_high','weekly_close_pos','nearest_downside_support_room_atr','oversold_risk'])
 f.code=f.code.astype(str).str.zfill(4);f=f.rename(columns={'ymd':'action_ymd'});x=x.merge(f,on=['code','action_ymd'],how='left',validate='one_to_one')
 x['confirm_bear_accumulation']=x.bear_count5.ge(3)
 x['confirm_upper_supply']=x.upper_supply_count5.ge(2)
 x['confirm_ma20_weakness']=x.dist_ma20_atr.lt(0)|x.cross_ma20.eq(1)
 x['confirm_higher_timeframe_weakness']=x.weekly_lower_high.eq(1)|x.weekly_close_pos.le(.35)
 x['confirm_downside_room']=x.nearest_downside_support_room_atr.ge(1.0)
 x['confirm_not_oversold']=x.oversold_risk.eq(0)
 cc=[c for c in x if c.startswith('confirm_')];x['confirmation_count']=x[cc].sum(axis=1);x['gate_pass']=x.confirmation_count.ge(4)
 base={str(y):rates(x[x.year.eq(y)]) for y in YEARS};ch={str(y):rates(x[x.year.eq(y)&x.gate_pass]) for y in YEARS}
 direction=all(v['down_first']>v['rebound_first'] for v in ch.values());breadth=all(v['n']>=30 for v in ch.values())
 def anchor(code,ymd):
  z=x[(x.code.eq(code))&x.action_ymd.eq(ymd)][['confirmation_count','gate_pass']]
  return z.where(pd.notna(z),None).to_dict('records')
 anchors={'6857_20240903':anchor('6857',20240903),'9107_20241122':anchor('9107',20241122)}
 data={'schema_version':'tradex_staged_core_confirmation_breadth_v1.compare.v1','artifact_role':'authoritative','review_only':True,'axis':'minimum independent confirmation groups before staged CORE_CLOSE',
  'fixed_conditions':{'base':'normalized STAGED_CORE excluding research-fallback','single_gate':'confirmation_count>=4 of 6','groups':cc,'outcome':'inherited exact fixed3 h5','years':list(YEARS),'minimum_each_year':30,'threshold_sweep':False},
  'baseline_results':base,'challenger_results':ch,'human_anchors':anchors,
  'observed_branching':{'base_events':len(x),'retained_events':int(x.gate_pass.sum()),'removed_events':int((~x.gate_pass).sum()),'missing_feature_rows':int(x.bear_count5.isna().sum()),'changed_rank_count':int((~x.gate_pass).sum()),'selection_divergence_reason':'CORE requires breadth across candle accumulation, rejection, MA state, higher timeframe, room, and oversold risk'},
  'judgment':{'decision':'keep' if direction and breadth else 'drop','direction_pass_all_years':direction,'breadth_pass':breadth,'reason':'adopt only if down-first exceeds rebound-first with at least 30 events in every year'},
  'not_changed':['source families','monthly classifier','individual thresholds','outcome','DIRECT_CORE','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'confirmation_breadth_ledger.parquet',index=False)
 (a.output/'audit.json').write_text(json.dumps({'duplicates':int(x.duplicated(['code','action_ymd']).sum()),'future_used_for_selection':False,'action_sha256':sha(a.actions),'feature_sha256':sha(a.features)},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
 print(json.dumps({'output':str(a.output),'challenger':ch,'anchors':anchors,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
