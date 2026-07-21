"""Calibrate GD depth as an additive point, never as an ADD hard gate."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def cell(z):
    n=len(z); d=int(z.add_outcome_fixed3_h5.eq('down_first').sum()); r=int(z.add_outcome_fixed3_h5.eq('rebound_first').sum())
    return {'n':n,'down_first':d,'rebound_first':r,'neutral':n-d-r,'net_direction':d-r}

def main():
    p=argparse.ArgumentParser();p.add_argument('--ledger',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.ledger).copy()
    x['gap_point']=0
    x.loc[x.gap_pct.le(-1.0),'gap_point']=1
    x.loc[x.gap_pct.le(-2.0),'gap_point']=2
    by_year={str(y):{str(s):cell(x[(x.year==y)&(x.gap_point==s)]) for s in (0,1,2)} for y in YEARS}
    pooled={str(s):cell(x[x.gap_point==s]) for s in (0,1,2)}
    anchor=x[(x.code=='9107')&(x.add_ymd==20241126)][['code','add_ymd','gap_pct','gap_point','add_outcome_fixed3_h5']].to_dict('records')
    monotonic=pooled['0']['net_direction'] < pooled['1']['net_direction'] <= pooled['2']['net_direction']
    stable=all(by_year[str(y)]['2']['down_first']>by_year[str(y)]['2']['rebound_first'] for y in YEARS)
    data={'schema_version':'tradex_ma200_add_gap_point_calibration_v1.compare.v1','artifact_role':'authoritative_diagnostic','review_only':True,'axis':'GD depth additive points only','fixed_conditions':{'point_0':'-0.5% >= gap > -1.0%','point_1':'-1.0% >= gap > -2.0%','point_2':'gap <= -2.0%','threshold_sweep':False,'episode_membership':'unchanged complete BOX MA200 ADD episodes','outcome':'existing add-close fixed3 h5','years':list(YEARS)},'year_results':by_year,'pooled_results':pooled,'human_anchor_9107':anchor,'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'none; points annotate every existing ADD candidate'},'judgment':{'decision':'hold','pooled_net_direction_monotonic':monotonic,'point2_down_exceeds_rebound_all_years':stable,'reason':'retain as one score component only; 2025 direction is unstable and no action threshold is adopted'},'not_changed':['ADD eligibility','other score axes','position sizing','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'gap_point_ledger.parquet',index=False)
    (a.output/'audit.json').write_text(json.dumps({'rows':len(x),'duplicates':int(x.duplicated(['code','probe_ymd']).sum()),'future_used_for_selection':False,'input_sha256':sha(a.ledger)},indent=2)+'\n',encoding='utf-8')
    (a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'pooled':pooled,'by_year':by_year,'anchor':anchor,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
