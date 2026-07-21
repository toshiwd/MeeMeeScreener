"""Evaluate fixed MA200 management actions at next-session open (review-only)."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=range(2020,2027); FORMAL={2023,2024,2025}
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def passage(g,start,entry,pct=.03,h=5):
    down=entry*(1-pct); up=entry*(1+pct)
    for _,q in g.iloc[start:start+h].iterrows():
        if q.o<=down:return 'down_first'
        if q.o>=up:return 'rebound_first'
        d=q.l<=down; r=q.h>=up
        if d and r:return 'neutral_order_unknown'
        if d:return 'down_first'
        if r:return 'rebound_first'
    return 'neutral_no_hit'
def main():
    p=argparse.ArgumentParser();p.add_argument('--classifier',action='append',required=True);p.add_argument('--daily',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.concat([pd.read_parquet(v) for v in a.classifier],ignore_index=True);x.code=x.code.astype(str).str.zfill(4);x.add_ymd=x.add_ymd.astype(int)
    d=pd.read_parquet(a.daily);d.code=d.code.astype(str).str.zfill(4);d.ymd=d.ymd.astype(int);hist={c:g.sort_values('ymd').reset_index(drop=True) for c,g in d.groupby('code')}
    rows=[];missing=[]
    for r in x.itertuples():
        g=hist.get(r.code); loc={} if g is None else {int(v):i for i,v in enumerate(g.ymd)}
        dates=[int(r.probe_ymd),int(r.core_ymd),int(r.add_ymd)]
        if any(v not in loc or loc[v]+1>=len(g) for v in dates):missing.append({'code':r.code,'dates':dates});continue
        pi,ci,ai=[loc[v]+1 for v in dates]; po,co,ao=[float(g.iloc[i].o) for i in (pi,ci,ai)]
        for ratio,w in [('1_1',1),('1_2',2)]:
            before=(po+w*co)/(1+w); after=(po+w*co+w*ao)/(1+2*w) if r.action=='ADD' else before
            profit=100*(before/ao-1); outcome=passage(g,ai,after)
            rows.append({'code':r.code,'action_signal_ymd':int(r.add_ymd),'action_execution_ymd':int(g.iloc[ai].ymd),'execution_year':int(g.iloc[ai].ymd)//10000,'action':r.action,'ratio':ratio,'probe_next_open':po,'core_next_open':co,'action_next_open':ao,'weighted_entry_after_action':after,'take_profit_next_open_pct':profit if r.action=='TAKE_PROFIT' else None,'post_action_next_open_outcome':outcome if r.action!='TAKE_PROFIT' else None,'action_open_gap_pct':100*(ao/float(g.iloc[ai-1].c)-1)})
    z=pd.DataFrame(rows)
    def cell(q):
        return {'n':len(q),'down_first':int(q.post_action_next_open_outcome.eq('down_first').sum()),'rebound_first':int(q.post_action_next_open_outcome.eq('rebound_first').sum()),'neutral':int(q.post_action_next_open_outcome.fillna('').str.startswith('neutral').sum()),'tp_median_pct':None if q.take_profit_next_open_pct.dropna().empty else float(q.take_profit_next_open_pct.median()),'tp_min_pct':None if q.take_profit_next_open_pct.dropna().empty else float(q.take_profit_next_open_pct.min())}
    results={str(y):{act:{ratio:cell(z[(z.execution_year==y)&(z.action==act)&(z.ratio==ratio)]) for ratio in ['1_1','1_2']} for act in ['ADD','HOLD','TAKE_PROFIT']} for y in YEARS}
    anchor=json.loads(z[(z.code=='9107')&(z.action_signal_ymd==20241126)].to_json(orient='records'))
    formal_add=all(results[str(y)]['ADD'][r]['n']==0 or results[str(y)]['ADD'][r]['down_first']>results[str(y)]['ADD'][r]['rebound_first'] for y in FORMAL for r in ['1_1','1_2'])
    tp_pass=all(q['tp_min_pct'] is None or q['tp_min_pct']>=3 for y in results.values() for q in y['TAKE_PROFIT'].values())
    data={'schema_version':'tradex_ma200_management_next_open_execution_v1.compare.v1','artifact_role':'authoritative','review_only':True,'axis':'management execution price only','fixed_conditions':{'actions':'fixed classifier labels unchanged','execution':'PROBE, CORE, and management action at each signal next-session open','outcome':'action execution session inclusive, fixed3 first passage over five sessions','year_attribution':'management execution year','costs':'ignored per project rule'},'year_results':results,'human_anchor_9107':anchor,'observed_branching':{'action_signal_rows':len(x),'execution_rows_per_ratio':len(z)//2,'missing_execution_paths':len(missing),'changed_rank_count':0,'selection_divergence_reason':'none; execution sensitivity only'},'judgment':{'decision':'keep_execution_contract' if formal_add and tp_pass and not missing else 'hold','formal_add_pass':formal_add,'take_profit_min3_pass':tp_pass,'human_anchor_executable':len(anchor)==2},'not_changed':['classifier thresholds','action labels','selector','monthly environment','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');z.to_parquet(a.output/'management_next_open_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'rows':len(z),'missing':missing,'duplicates':int(z.duplicated(['code','action_signal_ymd','ratio']).sum()),'future_used_for_action':False,'classifier_sha256':[sha(v) for v in a.classifier],'daily_sha256':sha(a.daily)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'judgment':data['judgment'],'anchor':anchor,'year_results':results},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
