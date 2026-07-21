"""PIT BOX return-sell state: exhaustion -> reentry -> CORE."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def run_count(g):
 b=g.c.lt(g.ma7);return b.astype(int).groupby((~b).cumsum()).cumsum()
def trade(g,pi,ci,w):
 if pi+1>=len(g):return {'status':'censored'}
 pei=pi+1;entry=float(g.iloc[pei].o);stop=entry*1.03;end=min(pei+5,len(g))
 for j in range(pei,end):
  q=g.iloc[j]
  if q.o>=stop:return {'status':'complete','exit_ymd':int(q.ymd),'ret':100*(entry-q.o)/entry,'reason':'probe_gap_stop'}
  if q.h>=stop:return {'status':'complete','exit_ymd':int(q.ymd),'ret':-3.0,'reason':'probe_stop'}
  if ci is not None and j==ci+1:
   co=float(q.o);avg=(entry+w*co)/(1+w);stop=min(stop,avg*1.03);end=min(j+5,len(g))
   for k in range(j,end):
    z=g.iloc[k]
    if z.o>=stop:return {'status':'complete','exit_ymd':int(z.ymd),'ret':100*(avg-z.o)/avg,'reason':'core_gap_stop','entry':avg}
    if z.h>=stop:return {'status':'complete','exit_ymd':int(z.ymd),'ret':100*(avg-stop)/avg,'reason':'core_stop','entry':avg}
   if end-j<5:return {'status':'censored'}
   z=g.iloc[end-1];return {'status':'complete','exit_ymd':int(z.ymd),'ret':100*(avg-z.c)/avg,'reason':'core_h5','entry':avg}
 if end-pei<5:return {'status':'censored'}
 q=g.iloc[end-1];return {'status':'complete','exit_ymd':int(q.ymd),'ret':100*(entry-q.c)/entry,'reason':'probe_h5','entry':entry}
def stats(q):
 v=q.realized_return_pct.dropna();gp=v[v>0].sum();gl=-v[v<0].sum();return {'n':len(q),'completed':int(v.size),'wins':int((v>0).sum()),'losses':int((v<0).sum()),'mean_pct':None if v.empty else float(v.mean()),'median_pct':None if v.empty else float(v.median()),'worst_pct':None if v.empty else float(v.min()),'profit_factor':None if gl==0 else float(gp/gl),'core_count':int(q.core_signal_ymd.notna().sum())}
def main():
 p=argparse.ArgumentParser();p.add_argument('--daily',required=True);p.add_argument('--monthly',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 d=pd.read_parquet(a.daily);d.code=d.code.astype(str).str.zfill(4);d=d.sort_values(['code','ymd']).copy();d['effective_month']=pd.to_datetime(d.ymd.astype(str)).dt.to_period('M');d['below7_run']=d.groupby('code',group_keys=False).apply(run_count,include_groups=False)
 m=pd.read_parquet(a.monthly);m.code=m.code.astype(str).str.zfill(4);d=d.merge(m[['code','effective_month','base_regime']],on=['code','effective_month'],how='left',validate='many_to_one')
 states=[];episodes=[]
 for code,g in d.groupby('code'):
  g=g.sort_values('ymd').reset_index(drop=True);state='IDLE';exi=None;rei=None;low=None
  for i,r in g.iterrows():
   if state=='IDLE' and r.base_regime in ('BOX','POST_BOX_BREAKOUT_CONSOLIDATION') and r.below7_run>=7 and r.pos20<=.10:
    state='EXHAUSTED';exi=i;low=float(r.l);states.append({'code':code,'ymd':int(r.ymd),'state':'TAKE_PROFIT_FULL_HEDGE'});continue
   if state=='EXHAUSTED':
    low=min(low,float(r.l))
    if i-exi>15:state='IDLE';exi=None;continue
    if r.base_regime in ('BOX','POST_BOX_BREAKOUT_CONSOLIDATION') and r.c>=r.ma20 and 100*(r.c/low-1)>=5 and r.c>r.o and r.close_pos>=.60:
     state='REENTRY';rei=i;states.append({'code':code,'ymd':int(r.ymd),'state':'REENTRY_PROBE'});continue
   if state=='REENTRY':
    if i-rei>5:episodes.append({'code':code,'probe_signal_ymd':int(g.iloc[rei].ymd),'core_signal_ymd':None});state='IDLE';continue
    if r.c<r.o and r.body_ratio>=.55 and r.close_pos<=.10 and r.cross_ma7==1 and r.cross_ma20==1:
     episodes.append({'code':code,'probe_signal_ymd':int(g.iloc[rei].ymd),'core_signal_ymd':int(r.ymd)});states.append({'code':code,'ymd':int(r.ymd),'state':'CORE'});state='IDLE'
  if state=='REENTRY':episodes.append({'code':code,'probe_signal_ymd':int(g.iloc[rei].ymd),'core_signal_ymd':None})
 e=pd.DataFrame(episodes);rows=[]
 for r in e.itertuples():
  g=d[d.code==r.code].sort_values('ymd').reset_index(drop=True);pi=int(g.index[g.ymd==r.probe_signal_ymd][0]);ci=None if pd.isna(r.core_signal_ymd) else int(g.index[g.ymd==int(r.core_signal_ymd)][0])
  for ratio,w in [('1_1',1),('1_2',2)]:
   t=trade(g,pi,ci,w);rows.append({'code':r.code,'probe_signal_ymd':r.probe_signal_ymd,'core_signal_ymd':None if ci is None else int(r.core_signal_ymd),'signal_year':r.probe_signal_ymd//10000,'ratio':ratio,'exit_ymd':t.get('exit_ymd'),'realized_return_pct':t.get('ret'),'exit_reason':t.get('reason'),'status':t['status']})
 q=pd.DataFrame(rows);years={str(y):{ratio:stats(q[(q.signal_year==y)&(q.ratio==ratio)]) for ratio in ['1_1','1_2']} for y in range(2020,2027)};teacher={'tp_20260126':bool(((pd.DataFrame(states).code=='7733')&(pd.DataFrame(states).ymd==20260126)&(pd.DataFrame(states).state=='TAKE_PROFIT_FULL_HEDGE')).any()),'reentry_20260210':bool(((pd.DataFrame(states).code=='7733')&(pd.DataFrame(states).ymd==20260210)&(pd.DataFrame(states).state=='REENTRY_PROBE')).any()),'core_20260213':bool(((pd.DataFrame(states).code=='7733')&(pd.DataFrame(states).ymd==20260213)&(pd.DataFrame(states).state=='CORE')).any())}
 data={'schema_version':'tradex_box_return_sell_state_v1.compare.v1','artifact_role':'authoritative_challenger','review_only':True,'axis':'BOX_RETURN_SELL state only','fixed_conditions':{'exhaustion':'BOX or POST_BOX_BREAKOUT_CONSOLIDATION, below MA7 run>=7, pos20<=0.10','reentry_within15':'BOX or POST_BOX_BREAKOUT_CONSOLIDATION, close>=MA20, rebound from state low>=5%, bullish close_pos>=0.60','core_within5':'bear body>=55%, close_pos<=0.10, cross MA7 and MA20','execution':'signals next-session open; probe/core ratios 1:1 and 1:2','exit':'3% stop, fixed five sessions after CORE or probe','costs':'ignored'},'year_results':years,'teacher_checks':teacher,'observed_branching':{'episodes':len(e),'with_core':int(e.core_signal_ymd.notna().sum()),'state_rows':len(states),'changed_rank_count':len(e),'selection_divergence_reason':'new state sequence, not raw trigger filter'},'judgment':{'decision':'hold_pending_oos_review','teacher_sequence_complete':all(teacher.values())},'not_changed':['raw trigger','existing CORE','management classifier','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8');q.to_parquet(a.output/'episode_ledger.parquet',index=False);pd.DataFrame(states).to_parquet(a.output/'state_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'episodes':len(e),'duplicates':int(e.duplicated(['code','probe_signal_ymd']).sum()),'future_used':False,'daily_sha256':sha(a.daily),'monthly_sha256':sha(a.monthly)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'teachers':teacher,'years':years,'judgment':data['judgment']},ensure_ascii=False))
if __name__=='__main__':main()
