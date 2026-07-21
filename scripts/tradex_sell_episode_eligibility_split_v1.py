"""Separate effectiveness-eligible actions from human-contract-only episode branches."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=range(2021,2026)
ELIGIBLE_FAMILIES={"WEAK_REBOUND_SCORE_CORE"}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rates(x):
 n=len(x);return {'n':n,'down_first':int(x.outcome_fixed3_h5.eq('down_first').sum()),'rebound_first':int(x.outcome_fixed3_h5.eq('rebound_first').sum()),'neutral':int((~x.outcome_fixed3_h5.isin(['down_first','rebound_first'])).sum())}
def main():
 p=argparse.ArgumentParser();p.add_argument('--ledger',type=Path,required=True);p.add_argument('--effectiveness',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 x=pd.read_parquet(a.ledger);x['research_lane']='CONTRACT_ONLY';x.loc[x.source_family.isin(ELIGIBLE_FAMILIES),'research_lane']='EFFECTIVENESS_ELIGIBLE';x.loc[x.action.eq('TAKE_PROFIT'),'research_lane']='PROFIT_CONTRACT_ONLY'
 core=x[(x.research_lane=='EFFECTIVENESS_ELIGIBLE')&(x.action=='CORE_CLOSE')&x.year.isin(YEARS)].copy();results={str(y):rates(core[core.year.eq(y)]) for y in YEARS};direction=all(v['down_first']>v['rebound_first'] for v in results.values());breadth=all(v['n']>=20 for v in results.values())
 family_lanes=x[['source_family','research_lane']].drop_duplicates().sort_values(['research_lane','source_family']).to_dict('records')
 data={'schema_version':'tradex_sell_episode_eligibility_split_v1.compare.v1','artifact_role':'authoritative','review_only':True,'axis':'explicit effectiveness eligibility versus episode-contract preservation','fixed_conditions':{'eligible_families':sorted(ELIGIBLE_FAMILIES),'eligibility_basis':'only family with down-first > rebound-first in every formal year','formal_years':list(YEARS),'minimum_each_year':20,'source_events_changed':False},'family_lanes':family_lanes,'eligible_core_results':results,'observed_branching':{'all_actions':len(x),'eligible_actions':int((x.research_lane=='EFFECTIVENESS_ELIGIBLE').sum()),'contract_only_actions':int((x.research_lane=='CONTRACT_ONLY').sum()),'profit_contract_actions':int((x.research_lane=='PROFIT_CONTRACT_ONLY').sum()),'changed_rank_count':int((x.research_lane!='EFFECTIVENESS_ELIGIBLE').sum()),'selection_divergence_reason':'human path reproduction is retained without treating failed broad branches as actionable core candidates'},'judgment':{'decision':'keep' if direction and breadth else 'hold','eligible_core_down_exceeds_rebound_all_years':direction,'eligible_core_breadth_pass':breadth,'contract_branches_preserved':True,'reason':'only evidence-passing families enter the actionable research lane; all others remain visible'},'not_changed':['source detectors','action dates','outcomes','human anchors','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'sell_episode_eligibility_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'rows':len(x),'duplicates':int(x.duplicated(['episode_id','action','action_ymd','source_family']).sum()),'unassigned_lane':int(x.research_lane.isna().sum()),'future_used_for_selection':False,'input_sha256':{'ledger':sha(a.ledger),'effectiveness':sha(a.effectiveness)}},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8');print(json.dumps({'output':str(a.output),'results':results,'counts':data['observed_branching'],'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
