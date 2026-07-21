"""Generate and validate PIT BOX_TOP_FAILURE short candidates."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def passage(g,i,entry):
 for j in range(i,min(i+5,len(g))):
  q=g.iloc[j]
  if q.o<=entry*.97:return 'down_first'
  if q.o>=entry*1.03:return 'rebound_first'
  d=q.l<=entry*.97;r=q.h>=entry*1.03
  if d and r:return 'neutral_order_unknown'
  if d:return 'down_first'
  if r:return 'rebound_first'
 return 'neutral_no_hit'
def realized(g,i,entry):
 for j in range(i,min(i+5,len(g))):
  q=g.iloc[j]
  if q.o>=entry*1.03:return j,100*(entry-q.o)/entry,'gap_stop'
  if q.h>=entry*1.03:return j,-3.0,'stop'
 end=min(i+4,len(g)-1)
 if end<i+4:return None,None,'censored'
 return end,100*(entry-g.iloc[end].c)/entry,'h5_close'
def stats(q):
 v=q.realized_return_pct.dropna();gp=v[v>0].sum();gl=-v[v<0].sum();return {'n':len(q),'completed':int(v.size),'down_first':int(q.outcome.eq('down_first').sum()),'rebound_first':int(q.outcome.eq('rebound_first').sum()),'neutral':int(q.outcome.str.startswith('neutral').sum()),'wins':int((v>0).sum()),'losses':int((v<0).sum()),'mean_return_pct':None if v.empty else float(v.mean()),'median_return_pct':None if v.empty else float(v.median()),'worst_return_pct':None if v.empty else float(v.min()),'profit_factor':None if gl==0 else float(gp/gl)}
def main():
 p=argparse.ArgumentParser();p.add_argument('--daily',required=True);p.add_argument('--monthly',required=True);p.add_argument('--require-full-retrace',action='store_true');p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 d=pd.read_parquet(a.daily);d.code=d.code.astype(str).str.zfill(4);d=d.sort_values(['code','ymd']).copy();d['prev_c']=d.groupby('code').c.shift();d['prev_o']=d.groupby('code').o.shift();d['prev_body_ratio']=d.groupby('code').body_ratio.shift();d['gap_pct']=100*(d.o/d.prev_c-1);d['effective_month']=pd.to_datetime(d.ymd.astype(str)).dt.to_period('M');d['full_retrace_prev_bull']=d.prev_c.gt(d.prev_o)&d.prev_body_ratio.ge(.30)&d.c.le(d.prev_o)
 m=pd.read_parquet(a.monthly);m.code=m.code.astype(str).str.zfill(4);m=m[['code','effective_month','base_regime','local_box_mature','local_box_upper','local_box_lower']]
 z=d.merge(m,on=['code','effective_month'],how='left',validate='many_to_one');z['monthly_current_box_pos']=(z.c-z.local_box_lower)/(z.local_box_upper-z.local_box_lower)
 z['candidate']=(z.base_regime.eq('BOX')&z.local_box_mature.fillna(False)&z.monthly_current_box_pos.ge(.55)&z.pos20.ge(.70)&z.ret10.ge(.03)&z.c.lt(z.o)&z.body_ratio.ge(.45)&z.close_pos.le(.35)&z.gap_pct.lt(1.0)&(z.full_retrace_prev_bull if a.require_full_retrace else True))
 hist={c:g.reset_index(drop=True) for c,g in d.groupby('code')};last={};rows=[]
 for r in z[z.candidate].sort_values(['ymd','code']).itertuples():
  g=hist[r.code];i=int(g.index[g.ymd.eq(r.ymd)][0]);
  if i+1>=len(g):continue
  ei=i+1;ey=int(g.iloc[ei].ymd)
  if last.get(r.code,0)>=ey:continue
  entry=float(g.iloc[ei].o);out=passage(g,ei,entry);xi,ret,reason=realized(g,ei,entry);exit_ymd=None if xi is None else int(g.iloc[xi].ymd);last[r.code]=exit_ymd or 99999999
  rows.append({'code':r.code,'signal_ymd':int(r.ymd),'execution_ymd':ey,'signal_year':int(r.ymd)//10000,'exit_ymd':exit_ymd,'entry':entry,'monthly_current_box_pos':float(r.monthly_current_box_pos),'pos20':float(r.pos20),'ret10':float(r.ret10),'gap_pct':float(r.gap_pct),'body_ratio':float(r.body_ratio),'close_pos':float(r.close_pos),'outcome':out,'realized_return_pct':ret,'exit_reason':reason})
 q=pd.DataFrame(rows);years={str(y):stats(q[q.signal_year==y]) for y in range(2020,2027)};teachers={f'{c}:{ymd}':bool(((z.code==c)&(z.ymd==ymd)&z.candidate).any()) for c,ymd in [('3405',20260618),('4208',20260514),('3405',20260630),('9531',20260603)]}
 data={'schema_version':'tradex_box_top_failure_branch_v1.compare.v1','artifact_role':'authoritative_challenger','review_only':True,'axis':'BOX_TOP_FAILURE generation only','fixed_conditions':{'monthly':'last completed calendar month, effective-month PIT join; BOX and mature local box','monthly_current_box_pos':'>=0.55 using prior fixed box bounds and signal close','daily':'pos20>=0.70, ret10>=3%, bearish body>=45%, close_pos<=0.35, GU<1%','full_retrace_prev_bull':'previous bullish body>=30% and signal close<=previous open' if a.require_full_retrace else 'not required','execution':'next-session open','outcome':'fixed3 D/R over five sessions plus 3% stop/h5 close realized return','costs':'ignored'},'year_results':years,'teacher_checks':teachers,'observed_branching':{'candidate_rows':len(q),'codes':int(q.code.nunique()) if len(q) else 0,'changed_rank_count':len(q),'selection_divergence_reason':'new state branch; not a filter of raw trigger'},'judgment':{'decision':'hold_pending_fixed_period_review','teacher_3405_hit':teachers['3405:20260618'],'teacher_4208_rejected':not teachers['4208:20260514']},'not_changed':['raw trigger','CORE','management classifier','MeeMee','ranking','runtime DB']}
 cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8');q.to_parquet(a.output/'candidate_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'candidates':len(q),'duplicates':int(q.duplicated(['code','signal_ymd']).sum()) if len(q) else 0,'future_used':False,'daily_sha256':sha(a.daily),'monthly_sha256':sha(a.monthly)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'years':years,'teachers':teachers,'judgment':data['judgment']},ensure_ascii=False))
if __name__=='__main__':main()
