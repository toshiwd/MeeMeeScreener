"""One-axis CORE gate: meaningful close separation below the first rejection probe."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def passage(g,idx,entry):
    for j in range(idx+1,min(idx+6,len(g))):
        dn=float(g.iloc[j].l)<=entry*.97;up=float(g.iloc[j].h)>=entry*1.03
        if dn and up:return 'neutral_order_unknown'
        if dn:return 'down_first'
        if up:return 'rebound_first'
    return 'neutral_no_hit'
def cell(z,col):
    n=len(z);d=int(z[col].eq('down_first').sum());r=int(z[col].eq('rebound_first').sum());return {'n':n,'down_first':d,'rebound_first':r,'neutral':n-d-r}

def main():
    p=argparse.ArgumentParser();p.add_argument('--episodes',type=Path,required=True);p.add_argument('--daily',type=Path,required=True);p.add_argument('--years',type=int,nargs='+',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False);years=tuple(a.years)
    e=pd.read_parquet(a.episodes);e=e[e.core_ymd.notna()].copy();e.code=e.code.astype(str).str.zfill(4);e.core_ymd=e.core_ymd.astype(int);e['core_year']=e.core_ymd//10000;e=e[e.core_year.isin(years)].copy()
    d=pd.read_parquet(a.daily,columns=['code','ymd','h','l','c','atr14']);d.code=d.code.astype(str).str.zfill(4);hist={c:g.sort_values('ymd').reset_index(drop=True) for c,g in d.groupby('code',sort=False)}
    rows=[]
    for r in e.itertuples(index=False):
        g=hist[r.code];loc={int(v):i for i,v in enumerate(g.ymd)};pi=loc[int(r.probe_ymd)];ci=loc[int(r.core_ymd)];pc=float(g.iloc[pi].c);cc=float(g.iloc[ci].c);atr=float(g.iloc[ci].atr14);sep=(cc-pc)/atr;en11=(pc+cc)/2;en12=(pc+2*cc)/3
        rows.append({'code':r.code,'probe_ymd':int(r.probe_ymd),'core_ymd':int(r.core_ymd),'core_year':int(r.core_year),'probe_close':pc,'core_close':cc,'core_atr14':atr,'core_vs_probe_atr':sep,'separation_gate_pass':sep<=-.30,'core_tranche_outcome':r.core_outcome_fixed3_h5,'whole_position_outcome_1_1':passage(g,ci,en11),'whole_position_outcome_1_2':passage(g,ci,en12),'has_later_add_candidate':pd.notna(r.add_ymd)})
    x=pd.DataFrame(rows);base={str(y):{'core_tranche':cell(x[x.core_year==y],'core_tranche_outcome'),'whole_1_1':cell(x[x.core_year==y],'whole_position_outcome_1_1'),'whole_1_2':cell(x[x.core_year==y],'whole_position_outcome_1_2')} for y in years};sel=x[x.separation_gate_pass];chall={str(y):{'whole_1_1':cell(sel[sel.core_year==y],'whole_position_outcome_1_1'),'whole_1_2':cell(sel[sel.core_year==y],'whole_position_outcome_1_2')} for y in years}
    active=[str(y) for y in years if chall[str(y)]['whole_1_1']['n']>0];direction=bool(active) and all(chall[y]['whole_1_1']['down_first']>chall[y]['whole_1_1']['rebound_first'] and chall[y]['whole_1_2']['down_first']>chall[y]['whole_1_2']['rebound_first'] for y in active);anchor=x[(x.code=='9107')&(x.core_ymd==20241122)].to_dict('records');external=years!=(2023,2024,2025)
    data={'schema_version':'tradex_ma200_core_probe_separation_oos_v1.compare.v1','artifact_role':'authoritative_external_validation' if external else 'authoritative_retrospective_candidate','review_only':True,'axis':'CORE close separation below PROBE close only','fixed_conditions':{'gate':'(CORE close-PROBE close)/CORE ATR14 <= -0.30','outcome':'whole short position fixed3 h5 from weighted probe/core entries','share_ratio_sensitivity':['probe:core 1:1','probe:core 1:2'],'core_population':'all CORE; later ADD candidate not used for selection','years':list(years),'costs':'ignored per project rule','research_design':'locked external validation' if external else 'retrospective candidate to lock before external use'},'baseline_year_results':base,'challenger_year_results':chall,'human_anchor_9107':anchor,'observed_branching':{'baseline_cores':int(len(x)),'selected_cores':int(len(sel)),'removed_cores':int(len(x)-len(sel)),'changed_rank_count':int(len(x)-len(sel)),'selection_divergence_reason':'second rejection CORE must close at least 0.30 ATR below the first rejection close'},'judgment':{'decision':('keep_external' if direction else 'drop_external') if external else 'hold_locked_candidate','whole_position_down_exceeds_rebound_active_years_both_ratios':direction,'active_years':active,'human_anchor_preserved':None if external else bool(anchor) and bool(anchor[0]['separation_gate_pass']),'reason':'external keep requires down-first dominance for both ratio sensitivities in every active year; zero-sample years do not prove breadth'},'not_changed':['probe detector','monthly environment','ADD classifier','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');x.to_parquet(a.output/'core_probe_separation_ledger.parquet',index=False)
    (a.output/'audit.json').write_text(json.dumps({'rows':len(x),'duplicates':int(x.duplicated(['code','probe_ymd']).sum()),'future_used_for_selection':False,'episodes_sha256':sha(a.episodes),'daily_sha256':sha(a.daily)},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'baseline':base,'challenger':chall,'anchor':anchor,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
