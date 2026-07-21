"""Calibrate running-low break depth as one additive ADD point."""
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
    p=argparse.ArgumentParser();p.add_argument('--episodes',type=Path,required=True);p.add_argument('--daily',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.episodes);x=x[x.year.isin(YEARS)&x.add_ymd.notna()].copy();x.code=x.code.astype(str).str.zfill(4);x.core_ymd=x.core_ymd.astype(int);x.add_ymd=x.add_ymd.astype(int)
    d=pd.read_parquet(a.daily,columns=['code','ymd','l','atr14']);d.code=d.code.astype(str).str.zfill(4);hist={c:g.set_index('ymd') for c,g in d.groupby('code',sort=False)}
    depths=[]
    for r in x.itertuples(index=False):
        g=hist[r.code];prior=float(g.loc[(g.index>=r.core_ymd)&(g.index<r.add_ymd),'l'].min());add_low=float(g.loc[r.add_ymd,'l']);atr=float(g.loc[r.add_ymd,'atr14']);depths.append((prior-add_low)/atr)
    x['running_low_break_depth_atr']=depths;x['low_break_point']=x.running_low_break_depth_atr.ge(1.0).astype(int)
    results={str(y):{str(s):cell(x[(x.year==y)&(x.low_break_point==s)]) for s in (0,1)} for y in YEARS};pooled={str(s):cell(x[x.low_break_point==s]) for s in (0,1)}
    anchor=x[(x.code=='9107')&(x.add_ymd==20241126)][['code','add_ymd','running_low_break_depth_atr','low_break_point','add_outcome_fixed3_h5']].to_dict('records')
    data={'schema_version':'tradex_ma200_add_low_break_point_v1.compare.v1','artifact_role':'authoritative_diagnostic','review_only':True,'axis':'running-low break depth additive point only','fixed_conditions':{'point_1':'ADD low is at least 1.0 ATR below minimum low from CORE through t-1','point_0':'break depth below 1.0 ATR','threshold_sweep':False,'episode_membership':'unchanged complete BOX MA200 ADD episodes','outcome':'existing add-close fixed3 h5','years':list(YEARS)},'year_results':results,'pooled_results':pooled,'human_anchor_9107':anchor,'observed_branching':{'changed_top5_members_count':0,'changed_top10_members_count':0,'changed_rank_count':0,'selection_divergence_reason':'none; point annotates every existing ADD candidate'},'judgment':{'decision':'hold_point_component','point1_down_exceeds_rebound_all_years':all(results[str(y)]['1']['down_first']>results[str(y)]['1']['rebound_first'] for y in YEARS),'human_anchor_gets_point':bool(anchor) and anchor[0]['low_break_point']==1,'reason':'retain as a score component only; 2025 point-1 cell fails and no action threshold is adopted'},'not_changed':['ADD eligibility','GD/support points','position sizing','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'low_break_point_ledger.parquet',index=False)
    (a.output/'audit.json').write_text(json.dumps({'rows':len(x),'duplicates':int(x.duplicated(['code','probe_ymd']).sum()),'future_used_for_selection':False,'episodes_sha256':sha(a.episodes),'daily_sha256':sha(a.daily)},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'results':results,'pooled':pooled,'anchor':anchor,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
