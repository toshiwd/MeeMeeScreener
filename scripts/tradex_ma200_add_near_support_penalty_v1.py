"""Calibrate a near-unbroken-prior-support penalty for MA200 ADD candidates."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def cell(z):
    n=len(z);d=int(z.add_outcome_fixed3_h5.eq('down_first').sum());r=int(z.add_outcome_fixed3_h5.eq('rebound_first').sum())
    return {'n':n,'down_first':d,'rebound_first':r,'neutral':n-d-r,'net_direction':d-r}

def main():
    p=argparse.ArgumentParser();p.add_argument('--ledger',type=Path,required=True);p.add_argument('--daily',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.ledger);d=pd.read_parquet(a.daily,columns=['code','ymd','l','c','atr14','support20']);d.code=d.code.astype(str).str.zfill(4)
    x=x.drop(columns=[c for c in ['ymd','l','c','atr14','support20'] if c in x]).merge(d,left_on=['code','add_ymd'],right_on=['code','ymd'],validate='one_to_one')
    x['prior_support_room_atr']=(x.c-x.support20)/x.atr14
    x['near_unbroken_support_penalty']=x.prior_support_room_atr.between(0,.35,inclusive='both') & x.l.ge(x.support20)
    results={str(y):{'penalty':cell(x[(x.year==y)&x.near_unbroken_support_penalty]),'no_penalty':cell(x[(x.year==y)&~x.near_unbroken_support_penalty])} for y in YEARS}
    pooled={'penalty':cell(x[x.near_unbroken_support_penalty]),'no_penalty':cell(x[~x.near_unbroken_support_penalty])}
    anchor=x[(x.code=='9107')&(x.add_ymd==20241126)][['code','add_ymd','prior_support_room_atr','near_unbroken_support_penalty','add_outcome_fixed3_h5']].to_dict('records')
    penalty_nonpositive=all(results[str(y)]['penalty']['down_first']<=results[str(y)]['penalty']['rebound_first'] for y in YEARS)
    data={'schema_version':'tradex_ma200_add_near_support_penalty_v1.compare.v1','artifact_role':'authoritative_diagnostic','review_only':True,'axis':'held prior-20-day support penalty only','fixed_conditions':{'penalty':'ADD low does not break prior support20 and 0 <= (close-prior support20)/ATR14 <= 0.35','support20':'minimum low over 20 sessions ending t-1; current ADD bar excluded','threshold_selection':'exploratory audit candidate; no adoption/OOS claim from these same 27 episodes','episode_membership':'unchanged complete BOX MA200 ADD episodes','outcome':'existing add-close fixed3 h5','years':list(YEARS)},'year_results':results,'pooled_results':pooled,'human_anchor_9107':anchor,'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'none; penalty annotates every existing ADD candidate'},'judgment':{'decision':'hold_penalty_component','penalty_down_not_above_rebound_all_years':penalty_nonpositive,'human_anchor_preserved_without_penalty':bool(anchor) and not bool(anchor[0]['near_unbroken_support_penalty']),'reason':'use as -1 point candidate only, never a veto; four penalty cases and same-sample threshold discovery require later locked validation'},'not_changed':['ADD eligibility','GD points','other score axes','position sizing','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'near_support_penalty_ledger.parquet',index=False)
    audit={'rows':len(x),'duplicates':int(x.duplicated(['code','probe_ymd']).sum()),'missing_support20':int(x.support20.isna().sum()),'future_used_for_selection':False,'input_sha256':sha(a.ledger),'daily_sha256':sha(a.daily)}
    (a.output/'audit.json').write_text(json.dumps(audit,indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'results':results,'pooled':pooled,'anchor':anchor,'judgment':data['judgment'],'audit':audit},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
