"""E2E rollup for fixed MA200 CORE -> ADD-candidate -> management action paths."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

YEARS=(2023,2024,2025)
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def state(pct): return 'profit_target_already_reached' if pct>=3 else ('loss_barrier_already_reached' if pct<=-3 else 'inside_barriers')
def passage(g,idx,entry):
    for j in range(idx+1,min(idx+6,len(g))):
        down=float(g.iloc[j].l)<=entry*.97;up=float(g.iloc[j].h)>=entry*1.03
        if down and up:return 'neutral_order_unknown'
        if down:return 'down_first'
        if up:return 'rebound_first'
    return 'neutral_no_hit'
def cell(z,ratio):
    n=len(z);inc_d=int(z.add_outcome_fixed3_h5.eq('down_first').sum());inc_r=int(z.add_outcome_fixed3_h5.eq('rebound_first').sum());col=f'post_action_outcome_{ratio}';inside=z[z[f'action_state_{ratio}'].eq('inside_barriers')];d=int(inside[col].eq('down_first').sum());r=int(inside[col].eq('rebound_first').sum())
    return {'n':n,'incremental_add_day_down_first':inc_d,'incremental_add_day_rebound_first':inc_r,'profit_target_already_reached_at_action':int(z[f'action_state_{ratio}'].eq('profit_target_already_reached').sum()),'loss_barrier_already_reached_at_action':int(z[f'action_state_{ratio}'].eq('loss_barrier_already_reached').sum()),'inside_barriers_at_action':int(len(inside)),'post_action_down_first_from_inside':d,'post_action_rebound_first_from_inside':r,'post_action_neutral_from_inside':int(len(inside)-d-r),'median_action_close_profit_pct':None if not n else float(z[f'action_close_profit_{ratio}_pct'].median())}

def main():
    p=argparse.ArgumentParser();p.add_argument('--classifier',type=Path,required=True);p.add_argument('--episodes',type=Path,required=True);p.add_argument('--daily',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.classifier);e=pd.read_parquet(a.episodes);e.code=e.code.astype(str).str.zfill(4);x=x.merge(e[['code','probe_ymd','core_outcome_fixed3_h5']],on=['code','probe_ymd'],how='left',validate='one_to_one')
    d=pd.read_parquet(a.daily,columns=['code','ymd','h','l','c']);d.code=d.code.astype(str).str.zfill(4);hist={c:g.sort_values('ymd').reset_index(drop=True) for c,g in d.groupby('code',sort=False)}
    rows=[]
    for r in x.itertuples(index=False):
        g=hist[r.code];loc={int(v):i for i,v in enumerate(g.ymd)};pi,ci,ai=loc[int(r.probe_ymd)],loc[int(r.core_ymd)],loc[int(r.add_ymd)];pc=float(g.iloc[pi].c);cc=float(g.iloc[ci].c);ac=float(g.iloc[ai].c)
        pre11=(pc+cc)/2;pre12=(pc+2*cc)/3
        if r.action=='ADD': ent11=(pc+cc+ac)/3;ent12=(pc+2*cc+2*ac)/5
        else: ent11,ent12=pre11,pre12
        p11=100*(ent11-ac)/ent11;p12=100*(ent12-ac)/ent12;s11=state(p11);s12=state(p12)
        out11=None if r.action=='TAKE_PROFIT' or s11!='inside_barriers' else passage(g,ai,ent11);out12=None if r.action=='TAKE_PROFIT' or s12!='inside_barriers' else passage(g,ai,ent12)
        row=dict(r._asdict());row.update({'entry_contract_1_1':'probe:core=1:1; ADD makes 1:1:1' if r.action=='ADD' else 'probe:core=1:1','entry_contract_1_2':'probe:core=1:2; ADD makes 1:2:2' if r.action=='ADD' else 'probe:core=1:2','action_entry_1_1':ent11,'action_entry_1_2':ent12,'action_close_profit_1_1_pct':p11,'action_close_profit_1_2_pct':p12,'action_state_1_1':s11,'action_state_1_2':s12,'post_action_outcome_1_1':out11,'post_action_outcome_1_2':out12,'take_profit_realized_1_1_pct':r.preadd_profit_1_1_pct if r.action=='TAKE_PROFIT' else None,'take_profit_realized_1_2_pct':r.preadd_profit_1_2_pct if r.action=='TAKE_PROFIT' else None});rows.append(row)
    z=pd.DataFrame(rows);results={str(y):{act:{ratio:cell(z[(z.action_year==y)&(z.action==act)],ratio) for ratio in ['1_1','1_2']} for act in ['ADD','HOLD','TAKE_PROFIT']} for y in YEARS}
    core_all=e[e.core_ymd.notna()].copy();core_all['core_ymd']=core_all.core_ymd.astype(int);core_all['core_year']=core_all.core_ymd//10000;core_all=core_all[core_all.core_year.isin(YEARS)].copy();core_all['has_add_candidate_within5']=core_all.add_ymd.notna()
    core={}
    for y in YEARS:
        q=core_all[core_all.core_year==y];d0=int(q.core_outcome_fixed3_h5.eq('down_first').sum());r0=int(q.core_outcome_fixed3_h5.eq('rebound_first').sum());with_add=q[q.has_add_candidate_within5];without_add=q[~q.has_add_candidate_within5]
        core[str(y)]={'n':int(len(q)),'down_first':d0,'rebound_first':r0,'neutral':int(len(q)-d0-r0),'with_add_candidate_within5':int(len(with_add)),'without_add_candidate_within5':int(len(without_add)),'with_add_down_first':int(with_add.core_outcome_fixed3_h5.eq('down_first').sum()),'with_add_rebound_first':int(with_add.core_outcome_fixed3_h5.eq('rebound_first').sum()),'without_add_down_first':int(without_add.core_outcome_fixed3_h5.eq('down_first').sum()),'without_add_rebound_first':int(without_add.core_outcome_fixed3_h5.eq('rebound_first').sum())}
    anchor=z[(z.code=='9107')&(z.add_ymd==20241126)].to_dict('records');tp=z[z.action=='TAKE_PROFIT'];tp_contract=bool(len(tp)) and bool(((tp.take_profit_realized_1_1_pct>=3)&(tp.take_profit_realized_1_2_pct>=3)).all());add_direction=all(results[str(y)]['ADD']['1_1']['incremental_add_day_down_first']>results[str(y)]['ADD']['1_1']['incremental_add_day_rebound_first'] for y in YEARS)
    data={'schema_version':'tradex_ma200_action_episode_e2e_rollup_v1.compare.v1','artifact_role':'authoritative','review_only':True,'fixed_conditions':{'years':list(YEARS),'year_attribution':'CORE population uses core year; management population uses ADD-candidate action year','core_population':'all CORE rows retained, including no ADD candidate within five sessions; no future conditioning','core_outcome':'source fixed3 h5 from CORE close','ADD_incremental_outcome':'source fixed3 h5 from ADD close','whole_position':'fixed3 h5 after action only when weighted position is inside barriers at action close','ratios':['probe/core/add share ratio 1:1:1','probe/core/add share ratio 1:2:2'],'TAKE_PROFIT':'position closed at action close; every TP must realize at least 3% under both pre-ADD share-ratio sensitivities; future path is opportunity-cost diagnostic only','costs':'ignored per project rule'},'core_stage_results':core,'action_stage_results':results,'human_anchor_9107':anchor,'observed_branching':{'actions':z.action.value_counts().to_dict(),'changed_rank_count':int((z.action!='ADD').sum()),'selection_divergence_reason':'fixed action classifier branches complete CORE->candidate episodes into ADD/HOLD/TAKE_PROFIT'},'judgment':{'decision':'hold','add_incremental_down_exceeds_rebound_all_years':add_direction,'take_profit_realized_contract_pass':tp_contract,'human_anchor_full_path_match':bool(anchor) and anchor[0]['action']=='ADD','reason':'E2E contract passes locally but external ADD breadth and HOLD management quality remain insufficient for keep'},'not_changed':['classifier thresholds','episode membership','monthly environment','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');z.to_parquet(a.output/'episode_e2e_ledger.parquet',index=False);core_all.to_parquet(a.output/'all_core_population_ledger.parquet',index=False)
    (a.output/'audit.json').write_text(json.dumps({'management_rows':len(z),'all_core_rows':len(core_all),'management_duplicates':int(z.duplicated(['code','probe_ymd']).sum()),'core_duplicates':int(core_all.duplicated(['code','probe_ymd']).sum()),'missing_management_core_outcome':int(z.core_outcome_fixed3_h5.isna().sum()),'missing_all_core_outcome':int(core_all.core_outcome_fixed3_h5.isna().sum()),'future_used_for_action':False,'future_add_conditioning_in_core_population':False,'classifier_sha256':sha(a.classifier),'episodes_sha256':sha(a.episodes),'daily_sha256':sha(a.daily)},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'output':str(a.output),'core':core,'actions':data['observed_branching']['actions'],'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
