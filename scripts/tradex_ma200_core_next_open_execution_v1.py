"""Execution sensitivity: fixed MA200 CORE signals filled at each next-day open."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=tuple(range(2020,2027));FORMAL=(2023,2024,2025)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def passage(g,idx,entry):
 for j in range(idx,min(idx+5,len(g))):
  op=float(g.iloc[j].o)
  if op<=entry*.97:return 'down_first'
  if op>=entry*1.03:return 'rebound_first'
  dn=float(g.iloc[j].l)<=entry*.97;up=float(g.iloc[j].h)>=entry*1.03
  if dn and up:return 'neutral_order_unknown'
  if dn:return 'down_first'
  if up:return 'rebound_first'
 return 'neutral_no_hit'
def cell(z,col):
 n=len(z);d=int(z[col].eq('down_first').sum());r=int(z[col].eq('rebound_first').sum());return {'n':n,'down_first':d,'rebound_first':r,'neutral':n-d-r,'median_weighted_entry_change_pct':None if not n else float(z.weighted_entry_change_pct.median())}
def main():
 p=argparse.ArgumentParser();p.add_argument('--core-ledger',type=Path,nargs='+',required=True);p.add_argument('--daily',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 x=pd.concat([pd.read_parquet(v) for v in a.core_ledger],ignore_index=True);x=x[x.separation_gate_pass].copy();d=pd.read_parquet(a.daily,columns=['code','ymd','o','h','l','c']);d.code=d.code.astype(str).str.zfill(4);hist={c:g.sort_values('ymd').reset_index(drop=True) for c,g in d.groupby('code',sort=False)};rows=[];missing=[]
 for r in x.itertuples(index=False):
  g=hist[r.code];loc={int(v):i for i,v in enumerate(g.ymd)};pi=loc[int(r.probe_ymd)];ci=loc[int(r.core_ymd)]
  if pi+1>=len(g) or ci+1>=len(g):missing.append({'code':r.code,'probe_ymd':int(r.probe_ymd),'core_ymd':int(r.core_ymd)});continue
  pei,cei=pi+1,ci+1;probe_open=float(g.iloc[pei].o);core_open=float(g.iloc[cei].o);probe_close=float(g.iloc[pi].c);core_close=float(g.iloc[ci].c)
  for ratio,w in [('1_1',1),('1_2',2)]:
   close_entry=(probe_close+w*core_close)/(1+w);open_entry=(probe_open+w*core_open)/(1+w);rows.append({'code':r.code,'probe_signal_ymd':int(r.probe_ymd),'probe_execution_ymd':int(g.iloc[pei].ymd),'core_signal_ymd':int(r.core_ymd),'core_execution_ymd':int(g.iloc[cei].ymd),'core_signal_year':int(r.core_year),'execution_year':int(g.iloc[cei].ymd)//10000,'ratio':ratio,'probe_signal_close':probe_close,'probe_next_open':probe_open,'core_signal_close':core_close,'core_next_open':core_open,'close_proxy_weighted_entry':close_entry,'next_open_weighted_entry':open_entry,'weighted_entry_change_pct':100*(open_entry/close_entry-1),'probe_open_gap_pct':100*(probe_open/probe_close-1),'core_open_gap_pct':100*(core_open/core_close-1),'next_open_outcome':passage(g,cei,open_entry),'close_proxy_outcome':r.whole_position_outcome_1_1 if ratio=='1_1' else r.whole_position_outcome_1_2})
 z=pd.DataFrame(rows);by=lambda col:{str(y):{ratio:{'close_proxy':cell(z[(z[col]==y)&(z.ratio==ratio)],'close_proxy_outcome'),'next_open':cell(z[(z[col]==y)&(z.ratio==ratio)],'next_open_outcome')} for ratio in ['1_1','1_2']} for y in YEARS};results=by('execution_year');signal_results=by('core_signal_year');formal=all(signal_results[str(y)][r]['next_open']['down_first']>signal_results[str(y)][r]['next_open']['rebound_first'] for y in FORMAL for r in ['1_1','1_2']);external=all(signal_results[str(y)][r]['next_open']['n']==0 or signal_results[str(y)][r]['next_open']['down_first']>signal_results[str(y)][r]['next_open']['rebound_first'] for y in (2020,2021,2022,2026) for r in ['1_1','1_2']);anchor=z[(z.code=='9107')&(z.core_signal_ymd==20241122)].to_dict('records');changes=z[z.close_proxy_outcome!=z.next_open_outcome][['code','core_signal_ymd','ratio','core_open_gap_pct','weighted_entry_change_pct','close_proxy_outcome','next_open_outcome']].to_dict('records');slip={k:float(z.weighted_entry_change_pct.quantile(v)) for k,v in {'p10':.1,'median':.5,'p90':.9,'worst':0}.items()}
 data={'schema_version':'tradex_ma200_core_next_open_execution_v1.compare.v2','artifact_role':'authoritative','review_only':True,'axis':'execution price only: signal close proxy versus next-session open','fixed_conditions':{'signals':'existing CORE probe-separation pass unchanged','execution':'PROBE filled next-session open; CORE filled next-session open','year_attribution':'signal year for OOS; execution year for realized risk','outcome':'open-first, then OHLC fixed3 first passage; CORE execution session inclusive through five sessions','ratios':['1:1','1:2'],'years':list(YEARS),'costs':'ignored per project rule'},'signal_year_results':signal_results,'execution_year_results':results,'human_anchor_9107':anchor,'execution_diagnostics':{'weighted_entry_change_pct':slip,'outcome_changes':changes},'observed_branching':{'signal_rows':int(len(x)),'execution_rows_per_ratio':int(len(z)//2),'missing_execution_paths':len(missing),'cross_year_execution_rows':int((z.core_signal_year!=z.execution_year).sum()),'changed_rank_count':0,'selection_divergence_reason':'none; execution-price sensitivity only'},'judgment':{'decision':'keep_execution_contract' if formal and external and not missing else 'hold','formal_next_open_pass':formal,'external_next_open_pass':external,'human_anchor_executable':len(anchor)==2,'reason':'next-open execution must preserve direction across formal and external years; otherwise remain review-only'},'not_changed':['signal selector','monthly environment','management classifier','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');z.to_parquet(a.output/'next_open_execution_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'signal_rows':len(x),'ledger_rows':len(z),'missing':missing,'duplicates':int(z.duplicated(['code','core_signal_ymd','ratio']).sum()),'future_used_for_signal':False,'daily_sha256':sha(a.daily),'core_sha256':[sha(v) for v in a.core_ledger]},indent=2)+'\n',encoding='utf-8');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n',encoding='utf-8');print(json.dumps({'output':str(a.output),'results':results,'anchor':anchor,'judgment':data['judgment']},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
