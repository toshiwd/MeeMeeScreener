import argparse,hashlib,json
from pathlib import Path
import pandas as pd
YEARS=range(2021,2026)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rates(x):
 n=len(x);return {'n':n,'down_first':int(x.outcome.eq('down_first').sum()),'rebound_first':int(x.outcome.eq('rebound_first').sum()),'neutral_no_hit':int(x.outcome.eq('neutral_no_hit').sum()),'down_first_pct':None if not n else 100*x.outcome.eq('down_first').mean(),'rebound_first_pct':None if not n else 100*x.outcome.eq('rebound_first').mean()}
def main():
 p=argparse.ArgumentParser();p.add_argument('--sequence',type=Path,required=True);p.add_argument('--features',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
 s=pd.read_parquet(a.sequence);x=s[s.gd_ymd.notna()&s.ma20_rebreak_ymd.notna()&(s.ma20_rebreak_ymd>s.gd_ymd)&s.weak_rebound&(s.max_consecutive_closes_above_ma7<7)].copy()
 x['action_ymd']=x.ma20_rebreak_ymd.astype(int);x=x.sort_values('erasure_ymd').drop_duplicates(['code','action_ymd'],keep='last');x['year']=x.action_ymd.astype(str).str[:4].astype(int);x['outcome']=x.ma20_rebreak_outcome_fixed3_h5
 f=pd.read_parquet(a.features,columns=['code','ymd','l','c','atr14']);f.code=f.code.astype(str).str.zfill(4)
 pl=f.rename(columns={'ymd':'erasure_ymd','l':'probe_low'})[['code','erasure_ymd','probe_low']];ac=f.rename(columns={'ymd':'action_ymd','c':'action_close','atr14':'action_atr'})[['code','action_ymd','action_close','action_atr']]
 x.code=x.code.astype(str).str.zfill(4);x=x.merge(pl,on=['code','erasure_ymd'],how='left',validate='one_to_one').merge(ac,on=['code','action_ymd'],how='left',validate='one_to_one')
 x['probe_low_break_depth_atr']=(x.probe_low-x.action_close)/x.action_atr;x['gate_pass']=x.action_close.lt(x.probe_low)
 base={str(y):rates(x[x.year.eq(y)]) for y in YEARS};ch={str(y):rates(x[x.year.eq(y)&x.gate_pass]) for y in YEARS};direction=all(v['down_first']>v['rebound_first'] for v in ch.values());breadth=all(v['n']>=30 for v in ch.values())
 anchor=x[(x.code.eq('9962'))&x.action_ymd.between(20260710,20260714)][['erasure_ymd','action_ymd','probe_low','action_close','gate_pass','outcome']];anchor=anchor.where(pd.notna(anchor),None).to_dict('records')
 data={'schema_version':'tradex_weak_rebound_probe_low_break_oos_v1.compare.v1','artifact_role':'authoritative','review_only':True,'axis':'weak-rebound MA20 rebreak must also close below prior probe low',
  'fixed_conditions':{'base_branch':'WEAK_REBOUND_MA20_REBREAK_CORE unchanged','single_gate':'action close < latest erasure/probe low','years':list(YEARS),'minimum_each_year':30,'outcome':'exact fixed3 h5 inherited','threshold_sweep':False},
  'baseline_results':base,'challenger_results':ch,'human_anchor_9962':anchor,'observed_branching':{'base_events':len(x),'retained_events':int(x.gate_pass.sum()),'removed_events':int((~x.gate_pass).sum()),'selection_divergence_reason':'MA20 rebreak is promoted to core only after the observable probe low is broken at close'},
  'judgment':{'decision':'keep' if direction and breadth else 'hold' if direction else 'drop','direction_pass_all_years':direction,'breadth_pass':breadth,'reason':'requires both direction and minimum breadth in every validation year'},'not_changed':['monthly environment','weak rebound sequence','MA20 rebreak','other branches','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'probe_low_break_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'duplicates':int(x.duplicated(['code','action_ymd']).sum()),'missing_prices':int(x.probe_low.isna().sum()+x.action_close.isna().sum()),'future_used_for_selection':False,'input_sha256':{'sequence':sha(a.sequence),'features':sha(a.features)}},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8');print(json.dumps({'output':str(a.output),'challenger':ch,'anchor':anchor,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
