"""Diagnostic extreme-MA20 oversold flag for MA200 ADD candidates."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def cell(z):
    n=len(z);d=int(z.add_outcome_fixed3_h5.eq('down_first').sum());r=int(z.add_outcome_fixed3_h5.eq('rebound_first').sum())
    return {'n':n,'down_first':d,'rebound_first':r,'neutral':n-d-r}

def main():
    p=argparse.ArgumentParser();p.add_argument('--ledger',type=Path,required=True);p.add_argument('--daily',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.ledger);d=pd.read_parquet(a.daily,columns=['code','ymd','dist_ma20_atr']);d.code=d.code.astype(str).str.zfill(4)
    x=x.drop(columns=[c for c in ['ymd','dist_ma20_atr'] if c in x]).merge(d,left_on=['code','add_ymd'],right_on=['code','ymd'],validate='one_to_one')
    x['extreme_ma20_oversold']=x.dist_ma20_atr.le(-4.0)
    results={str(y):{'flag':cell(x[(x.year==y)&x.extreme_ma20_oversold]),'no_flag':cell(x[(x.year==y)&~x.extreme_ma20_oversold])} for y in YEARS}
    anchor=x[(x.code=='9107')&(x.add_ymd==20241126)][['code','add_ymd','dist_ma20_atr','extreme_ma20_oversold','add_outcome_fixed3_h5']].to_dict('records')
    data={'schema_version':'tradex_ma200_add_extreme_ma20_oversold_v1.compare.v1','artifact_role':'authoritative_diagnostic','review_only':True,'axis':'extreme MA20 downside extension flag only','fixed_conditions':{'flag':'(close-MA20)/ATR14 <= -4.0 at ADD close','threshold_selection':'exploratory domain boundary; same 27 episodes, no adoption/OOS claim','episode_membership':'unchanged complete BOX MA200 ADD episodes','outcome':'existing add-close fixed3 h5','years':list(YEARS)},'year_results':results,'flagged_rows':x[x.extreme_ma20_oversold][['code','add_ymd','year','dist_ma20_atr','add_outcome_fixed3_h5']].to_dict('records'),'human_anchor_9107':anchor,'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'none; diagnostic flag only'},'judgment':{'decision':'hold_risk_flag','flagged_count':int(x.extreme_ma20_oversold.sum()),'human_anchor_not_flagged':bool(anchor) and not bool(anchor[0]['extreme_ma20_oversold']),'reason':'one rebound case is directionally correct but insufficient to set a penalty weight'},'not_changed':['ADD eligibility','existing points','position sizing','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'extreme_ma20_oversold_ledger.parquet',index=False)
    (a.output/'audit.json').write_text(json.dumps({'rows':len(x),'duplicates':int(x.duplicated(['code','probe_ymd']).sum()),'future_used_for_selection':False,'input_sha256':sha(a.ledger),'daily_sha256':sha(a.daily)},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'results':results,'flagged':data['flagged_rows'],'anchor':anchor,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
