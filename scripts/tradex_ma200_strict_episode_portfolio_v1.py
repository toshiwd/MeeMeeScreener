"""Strict probe-to-exit MA200 episode and daily MTM portfolio research."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

RATIOS={'1_1':1,'1_2':2}; YEARS=range(2020,2027); RISKS=(.005,.0075,.01)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--episodes',required=True);p.add_argument('--core-ledger',action='append',required=True);p.add_argument('--classifier',action='append',required=True);p.add_argument('--daily',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 e=pd.read_parquet(a.episodes);e.code=e.code.astype(str).str.zfill(4);e=e[e.year.isin(YEARS)].sort_values(['probe_ymd','code'])
 c=pd.concat([pd.read_parquet(v) for v in a.core_ledger]);c=c[c.separation_gate_pass].copy();c.code=c.code.astype(str).str.zfill(4);cores={(r.code,int(r.probe_ymd),int(r.core_ymd)) for r in c.itertuples()}
 m=pd.concat([pd.read_parquet(v) for v in a.classifier]);m.code=m.code.astype(str).str.zfill(4);acts={(r.code,int(r.probe_ymd),int(r.core_ymd)):r for r in m.itertuples()}
 d=pd.read_parquet(a.daily,columns=['code','ymd','o','h','l','c']);d.code=d.code.astype(str).str.zfill(4);d.ymd=d.ymd.astype(int);hist={k:g.sort_values('ymd').reset_index(drop=True) for k,g in d.groupby('code')}
 allrows=[];dailyrows=[];supp=[];missing=[]
 for ratio,w in RATIOS.items():
  active_until={}
  for r in e.itertuples():
   g=hist.get(r.code);loc={} if g is None else {int(v):i for i,v in enumerate(g.ymd)};ps=loc.get(int(r.probe_ymd))
   if ps is None or ps+1>=len(g):
    missing.append({'code':r.code,'probe':int(r.probe_ymd),'ratio':ratio});allrows.append({'ratio':ratio,'code':r.code,'probe_signal_ymd':int(r.probe_ymd),'probe_execution_ymd':None,'core_signal_ymd':None,'core_executed':False,'management_action':None,'management_executed':False,'units_deployed':0,'planned_units':1+2*w,'exit_ymd':None,'exit_year':None,'exit_reason':'censored_no_next_open','deployed_return_pct':None,'planned_capital_return_pct':None,'target_touched':False});continue
   pei=ps+1;peymd=int(g.iloc[pei].ymd)
   if active_until.get(r.code,0)>=peymd:supp.append({'code':r.code,'probe':int(r.probe_ymd),'ratio':ratio,'active_until':active_until[r.code]});continue
   core_key=None;cei=None
   if pd.notna(r.core_ymd) and (r.code,int(r.probe_ymd),int(r.core_ymd)) in cores:
    core_key=(r.code,int(r.probe_ymd),int(r.core_ymd));cs=loc.get(int(r.core_ymd));cei=None if cs is None or cs+1>=len(g) else cs+1
   act=acts.get(core_key);aei=None
   if act is not None:
    aa=loc.get(int(act.add_ymd));aei=None if aa is None or aa+1>=len(g) else aa+1
   units=1;planned=1+2*w;entries=float(g.iloc[pei].o);entry_value=entries;avg=entries;stop=avg*1.03;target=avg*.97;target_touched=False;core_done=False;action_done=False;exit_i=None;exit_px=None;reason=None;core_clock_end=None;last_pnl=0
   max_i=min(pei+5,len(g))
   for j in range(pei,len(g)):
    if core_clock_end is None and j>=max_i:break
    if core_clock_end is not None and j>core_clock_end:break
    q=g.iloc[j];op=float(q.o)
    if op<=target:target_touched=True
    if op>=stop:exit_i=j;exit_px=op;reason='gap_stop';break
    if cei==j and not core_done:
     units+=w;entry_value+=w*op;avg=entry_value/units;stop=min(stop,avg*1.03);target=avg*.97;core_done=True;core_clock_end=min(j+4,len(g)-1)
    if aei==j and core_done and not action_done:
     if act.action=='TAKE_PROFIT':exit_i=j;exit_px=op;reason='take_profit_next_open';action_done=True;break
     if act.action=='ADD':units+=w;entry_value+=w*op;avg=entry_value/units;stop=min(stop,avg*1.03);target=avg*.97
     action_done=True
    down=float(q.l)<=target;up=float(q.h)>=stop;target_touched=target_touched or down
    if down and up:exit_i=j;exit_px=stop;reason='target_and_stop_same_bar_stop';break
    if up:exit_i=j;exit_px=stop;reason='stop';break
    pnl=entry_value-units*float(q.c);pr=100*pnl/(planned*entries);dailyrows.append({'ratio':ratio,'code':r.code,'probe_ymd':int(r.probe_ymd),'ymd':int(q.ymd),'planned_return_pct':pr,'delta_planned_return_pct':pr-last_pnl});last_pnl=pr
   if exit_i is None:
    end=(core_clock_end if core_clock_end is not None else max_i-1)
    if end>=len(g) or end<pei or (core_clock_end is None and len(g)-pei<5):reason='censored'
    else:exit_i=end;exit_px=float(g.iloc[end].c);reason='h5_close'
   if reason=='censored':ret=pret=None;exit_ymd=None
   else:
    pnl=entry_value-units*exit_px;ret=100*pnl/entry_value;pret=100*pnl/(planned*entries);exit_ymd=int(g.iloc[exit_i].ymd);dailyrows.append({'ratio':ratio,'code':r.code,'probe_ymd':int(r.probe_ymd),'ymd':exit_ymd,'planned_return_pct':pret,'delta_planned_return_pct':pret-last_pnl})
   active_until[r.code]=exit_ymd or int(g.iloc[-1].ymd)
   allrows.append({'ratio':ratio,'code':r.code,'probe_signal_ymd':int(r.probe_ymd),'probe_execution_ymd':peymd,'core_signal_ymd':None if core_key is None else core_key[2],'core_executed':core_done,'management_action':None if act is None else act.action,'management_executed':action_done,'units_deployed':units,'planned_units':planned,'exit_ymd':exit_ymd,'exit_year':None if exit_ymd is None else exit_ymd//10000,'exit_reason':reason,'deployed_return_pct':ret,'planned_capital_return_pct':pret,'target_touched':target_touched})
 z=pd.DataFrame(allrows);dr=pd.DataFrame(dailyrows)
 def stats(q):
  v=q.planned_capital_return_pct.dropna();loss=q[q.planned_capital_return_pct<0].sort_values('exit_ymd');st=cur=0
  for _,t in q[q.exit_ymd.notna()].sort_values('exit_ymd').iterrows():cur=cur+1 if t.planned_capital_return_pct<0 else 0;st=max(st,cur)
  gp=v[v>0].sum();gl=-v[v<0].sum();return {'episodes':len(q),'completed':int(q.exit_ymd.notna().sum()),'censored':int(q.exit_ymd.isna().sum()),'wins':int((v>0).sum()),'losses':int((v<0).sum()),'mean_pct':None if v.empty else float(v.mean()),'median_pct':None if v.empty else float(v.median()),'worst_pct':None if v.empty else float(v.min()),'tail5_pct':None if v.empty else float(v.quantile(.05)),'profit_factor':None if gl==0 else float(gp/gl),'max_consecutive_losses':st}
 years={str(y):{r:stats(z[(z.exit_year==y)&(z.ratio==r)]) for r in RATIOS} for y in YEARS};portfolio={}
 for ratio in RATIOS:
  q=z[z.ratio==ratio];days=dr[dr.ratio==ratio].groupby('ymd').delta_planned_return_pct.sum().sort_index();con=[]
  for day in sorted(dr[dr.ratio==ratio].ymd.unique()):con.append(int(((q.probe_execution_ymd<=day)&((q.exit_ymd.fillna(99999999))>=day)).sum()))
  portfolio[ratio]={}
  for risk in RISKS:
   sleeve=risk/.03;curve=100+days.mul(sleeve).cumsum();runmax=curve.cummax();dds=100*(curve/runmax-1);md=0 if dds.empty else float(dds.min());trough=None if dds.empty else int(dds.idxmin());peakday=None if trough is None else int(curve.loc[:trough].idxmax());recovery=None
   if trough is not None:
    hit=curve.loc[trough:][curve.loc[trough:]>=curve.loc[peakday]];recovery=None if hit.empty else int(hit.index[0])
   portfolio[ratio][f'{risk*100:.2f}%']={'reserved_capital_per_episode_pct':100*sleeve,'max_concurrent':max(con,default=0),'max_reserved_capital_pct':100*sleeve*max(con,default=0),'daily_mtm_max_drawdown_pct':md,'drawdown_peak_ymd':peakday,'drawdown_trough_ymd':trough,'recovery_ymd':recovery,'total_pnl_pct':0 if curve.empty else float(curve.iloc[-1]-100)}
 data={'schema_version':'tradex_ma200_strict_episode_portfolio_v1.compare.v2','artifact_role':'authoritative','review_only':True,'supersedes':'v1 fixed3 target was incorrectly treated as automatic TAKE_PROFIT and contradicted the locked 9107 ADD path','fixed_conditions':{'population':'all BOX MA200 probes 2020-2026; CORE/management only on fixed passed ledgers','execution':'all signals next-session open','clock':'probe-only five sessions; CORE execution starts one fixed five-session clock; HOLD/ADD do not reset','exit':'open-first 3% stop, locked TAKE_PROFIT next open, or horizon close; 3% downside target is diagnostic only','barriers':'gap stop at actual open; stop never loosened','overlap':'same-code new probe suppressed until exit','returns':'deployed and planned maximum-unit capital reported separately','costs':'ignored'},'year_results_by_exit_year':years,'clean_oos_2026_by_probe_signal_year':{r:stats(z[(z.probe_signal_ymd//10000==2026)&(z.ratio==r)]) for r in RATIOS},'clean_oos_2026_core_executed':{r:stats(z[(z.probe_signal_ymd//10000==2026)&(z.ratio==r)&z.core_executed]) for r in RATIOS},'management_contribution':{r:{act:stats(z[(z.ratio==r)&z.management_executed&(z.management_action==act)]) for act in ['ADD','HOLD','TAKE_PROFIT']} for r in RATIOS},'portfolio_sizing':portfolio,'observed_branching':{'episodes':{k:int(v) for k,v in z.groupby('ratio').size().items()},'suppressed':{k:int(v) for k,v in pd.DataFrame(supp).groupby('ratio').size().items()} if supp else {},'missing':len(missing),'core_executed':{k:int(v) for k,v in z.groupby('ratio').core_executed.sum().items()},'management_executed':{k:int(v) for k,v in z.groupby('ratio').management_executed.sum().items()},'target_touched_diagnostic':{k:int(v) for k,v in z.groupby('ratio').target_touched.sum().items()},'exit_reasons':z.groupby(['ratio','exit_reason']).size().to_dict()},'human_anchor_9107':json.loads(z[(z.code=='9107')&(z.probe_signal_ymd==20241121)].to_json(orient='records')),'judgment':{'decision':'hold','reason':'strict full-population risk and capacity must pass before any live adoption'},'not_changed':['selector','classifier','MeeMee','ranking','runtime DB']}
 # tuple keys are not JSON keys
 data['observed_branching']['exit_reasons']={f'{k[0]}:{k[1]}':int(v) for k,v in data['observed_branching']['exit_reasons'].items()}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8');z.to_parquet(a.output/'episode_ledger.parquet',index=False);dr.to_parquet(a.output/'daily_mtm_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'rows':len(z),'daily_rows':len(dr),'missing':missing,'suppressed':len(supp),'duplicates':int(z.duplicated(['ratio','code','probe_signal_ymd']).sum()),'source_sha256':{'episodes':sha(a.episodes),'core':[sha(v) for v in a.core_ledger],'classifier':[sha(v) for v in a.classifier],'daily':sha(a.daily)}},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'clean_oos':data['clean_oos_2026_by_probe_signal_year'],'portfolio':portfolio,'branching':data['observed_branching'],'anchor':data['human_anchor_9107']},ensure_ascii=False))
if __name__=='__main__':main()
