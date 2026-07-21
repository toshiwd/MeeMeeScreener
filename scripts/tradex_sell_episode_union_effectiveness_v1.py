"""Measure action and family effectiveness on the normalized sell episode union."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS = range(2021, 2026)

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rates(x, desired):
    n=len(x); down=int(x.outcome_fixed3_h5.eq('down_first').sum()); reb=int(x.outcome_fixed3_h5.eq('rebound_first').sum())
    success=down if desired=='down_first' else reb; failure=reb if desired=='down_first' else down
    return {'n':n,'down_first':down,'rebound_first':reb,'neutral':n-down-reb,'desired':desired,'success':success,'failure':failure,'success_pct':None if not n else 100*success/n}

def main():
    p=argparse.ArgumentParser();p.add_argument('--ledger',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.ledger);x=x[x.year.isin(YEARS)&x.outcome_fixed3_h5.notna()].copy()
    desired={'PROBE':'down_first','CORE_CLOSE':'down_first','ADD':'down_first','TAKE_PROFIT':'rebound_first'}
    action_results={act:{str(y):rates(x[(x.action==act)&x.year.eq(y)],want) for y in YEARS} for act,want in desired.items()}
    family_results={}
    for fam in sorted(x.source_family.unique()):
        family_results[fam]={}
        for act in sorted(x[x.source_family==fam].action.unique()):
            family_results[fam][act]={str(y):rates(x[(x.source_family==fam)&(x.action==act)&x.year.eq(y)],desired[act]) for y in YEARS}
    core=x[x.action=='CORE_CLOSE'].sort_values(['code','action_ymd','source_family']).drop_duplicates(['code','action_ymd','action','outcome_fixed3_h5'])
    integrated={str(y):rates(core[core.year.eq(y)],'down_first') for y in YEARS};direction=all(v['down_first']>v['rebound_first'] for v in integrated.values())
    failing=[]
    for fam,acts in family_results.items():
        if 'CORE_CLOSE' not in acts:continue
        active=[v for v in acts['CORE_CLOSE'].values() if v['n']>0]
        if active and any(v['down_first']<=v['rebound_first'] for v in active):failing.append(fam)
    data={'schema_version':'tradex_sell_episode_union_effectiveness_v1.compare.v1','artifact_role':'authoritative','review_only':True,'fixed_conditions':{'ledger':'normalized union unchanged','years':list(YEARS),'desired_direction':desired,'integrated_core_dedupe':'code/action date/outcome','costs':'ignored by project rule','selection_change':False},'action_results':action_results,'family_results':family_results,'integrated_core_results':integrated,'observed_branching':{'core_source_families':int(x[x.action=='CORE_CLOSE'].source_family.nunique()),'failing_core_families':failing,'changed_rank_count':0,'selection_divergence_reason':'none; diagnostic rollup only'},'judgment':{'decision':'keep' if direction else 'hold','integrated_core_down_exceeds_rebound_all_years':direction,'blocking_families':failing,'reason':'integrated CORE_CLOSE must retain down-first dominance in every formal year'},'not_changed':['episode membership','action dates','source outcomes','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');core.to_parquet(a.output/'integrated_core_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'input_rows':len(x),'integrated_core_rows':len(core),'duplicates_after_dedupe':int(core.duplicated(['code','action_ymd','action','outcome_fixed3_h5']).sum()),'future_used_for_selection':False,'ledger_sha256':sha(a.ledger)},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8');print(json.dumps({'output':str(a.output),'integrated_core_results':integrated,'blocking_families':failing,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
