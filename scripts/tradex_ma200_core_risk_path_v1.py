"""Risk path for fixed MA200 CORE next-open executions (review-only)."""
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

YEARS=range(2020,2027); RATIOS=('1_1','1_2'); RISK=(.005,.0075,.01)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def max_streak(v):
    best=cur=0
    for x in v:cur=cur+1 if x<0 else 0;best=max(best,cur)
    return best
def path(g,start,entry):
    stop=entry*1.03;target=entry*.97;worst=0
    end=min(start+5,len(g))
    for j in range(start,end):
        q=g.iloc[j];worst=min(worst,100*(entry-float(q.o))/entry)
        if q.o<=target:return j,float(q.o),'gap_target',100*(entry-float(q.o))/entry,worst
        if q.o>=stop:return j,float(q.o),'gap_stop',100*(entry-float(q.o))/entry,worst
        worst=min(worst,100*(entry-float(q.h))/entry)
        d=q.l<=target;r=q.h>=stop
        if d and r:return j,stop,'both_loss_first',-3.0,worst
        if d:return j,target,'target',3.0,worst
        if r:return j,stop,'stop',-3.0,worst
    j=end-1;px=float(g.iloc[j].c);return j,px,'h5_close',100*(entry-px)/entry,worst
def dd(vals):
    eq=100.0;peak=eq;m=0
    for v in vals:eq+=v;peak=max(peak,eq);m=min(m,100*(eq/peak-1))
    return m,eq-100
def main():
    p=argparse.ArgumentParser();p.add_argument('--execution-ledger',required=True);p.add_argument('--daily',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    x=pd.read_parquet(a.execution_ledger);d=pd.read_parquet(a.daily,columns=['code','ymd','o','h','l','c']);d.code=d.code.astype(str).str.zfill(4);d.ymd=d.ymd.astype(int);hist={c:g.sort_values('ymd').reset_index(drop=True) for c,g in d.groupby('code')};rows=[];missing=[]
    for r in x.itertuples():
        g=hist.get(r.code);loc={} if g is None else {int(v):i for i,v in enumerate(g.ymd)};start=loc.get(int(r.core_execution_ymd))
        if start is None:missing.append({'code':r.code,'ymd':int(r.core_execution_ymd)});continue
        ei,ep,reason,ret,worst=path(g,start,float(r.next_open_weighted_entry));rows.append({'code':r.code,'ratio':r.ratio,'signal_year':int(r.core_signal_year),'execution_year':int(r.execution_year),'entry_ymd':int(r.core_execution_ymd),'exit_ymd':int(g.iloc[ei].ymd),'entry':float(r.next_open_weighted_entry),'exit':ep,'exit_reason':reason,'return_pct':ret,'max_adverse_pct':worst})
    z=pd.DataFrame(rows).sort_values(['exit_ymd','entry_ymd','code'])
    def stats(q):
        v=q.return_pct.tolist();return {'n':len(q),'positive':int((q.return_pct>0).sum()),'negative':int((q.return_pct<0).sum()),'neutral':int((q.return_pct==0).sum()),'mean_return_pct':None if not len(q) else float(q.return_pct.mean()),'median_return_pct':None if not len(q) else float(q.return_pct.median()),'worst_return_pct':None if not len(q) else float(q.return_pct.min()),'worst_intratrade_adverse_pct':None if not len(q) else float(q.max_adverse_pct.min()),'max_consecutive_losses':max_streak(v)}
    years={str(y):{r:stats(z[(z.execution_year==y)&(z.ratio==r)]) for r in RATIOS} for y in YEARS};sizing={}
    for ratio in RATIOS:
        q=z[z.ratio==ratio];mx=0
        for day in sorted(set(q.entry_ymd)|set(q.exit_ymd)):
            mx=max(mx,int(((q.entry_ymd<=day)&(q.exit_ymd>=day)).sum()))
        sizing[ratio]={}
        for risk in RISK:
            notional=risk/.03; pnl=q.return_pct.mul(notional).tolist();m,total=dd(pnl);sizing[ratio][f'{100*risk:.2f}%']={'notional_per_episode_pct':100*notional,'max_concurrent_positions':mx,'max_gross_exposure_pct':100*notional*mx,'max_drawdown_pct_initial_equity':m,'total_pnl_pct_initial_equity':total,'worst_episode_pnl_pct_initial_equity':min(pnl)}
    data={'schema_version':'tradex_ma200_core_risk_path_v1.compare.v1','artifact_role':'authoritative','review_only':True,'fixed_conditions':{'population':'existing CORE separation-pass only','entry':'fixed next-open weighted entry','exit':'open-first fixed3; same-day both is conservative loss-first; otherwise fifth-session close','costs':'ignored','risk_sizing':'fixed initial-equity risk budget divided by 3% stop distance','dd':'chronological realized PnL on fixed initial equity; no compounding','scope_warning':'probe-only paths without CORE are outside this artifact'},'year_results':years,'sizing_sensitivity':sizing,'clean_oos_2026':years['2026'],'observed_branching':{'rows':len(z),'missing':len(missing),'exit_reasons':z.exit_reason.value_counts().to_dict(),'changed_rank_count':0},'judgment':{'decision':'hold','reason':'risk evidence must be combined with management execution and probe-only population before live adoption'},'not_changed':['selector','management labels','MeeMee','ranking','runtime DB']}
    cp=a.output/'compare.json';cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');z.to_parquet(a.output/'risk_path_ledger.parquet',index=False);(a.output/'audit.json').write_text(json.dumps({'rows':len(z),'missing':missing,'duplicates':int(z.duplicated(['code','entry_ymd','ratio']).sum()),'execution_sha256':sha(a.execution_ledger),'daily_sha256':sha(a.daily)},indent=2)+'\n');(a.output/'_ARTIFACT_COMPLETE.json').write_text(json.dumps({'complete':True,'authoritative':'compare.json','sha256':sha(cp)},indent=2)+'\n');print(json.dumps({'output':str(a.output),'clean_oos_2026':years['2026'],'sizing':sizing},ensure_ascii=False,indent=2))
if __name__=='__main__':main()
