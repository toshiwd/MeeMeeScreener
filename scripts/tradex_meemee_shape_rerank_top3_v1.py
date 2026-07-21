from __future__ import annotations
import argparse,hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
if __package__ in (None,""):sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.tradex_meemee_leaf_consensus_top3_v1 import ranked_events,stats
from scripts.tradex_leaf_order_contract_readiness_v1 import replay
DB=Path(r'G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb');OUT=Path(r'G:\Tradex\meemee_shape_rerank_top3_v1')
VARIANTS={'A_close_position':('close_position',False),'B_ma20_distance':('ma20_distance',True),'C_volume_ratio':('volume_ratio',False)}
def sha(p):
 h=hashlib.sha256();
 with Path(p).open('rb')as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def features(db):
 q="""with u as(select code from daily_bars where source='pan'group by code having max(date)=(select max(date)from daily_bars where source='pan')),x as(select code,date,o,h,l,c,v,avg(c)over(partition by code order by date rows between 19 preceding and current row)ma20,avg(v)over(partition by code order by date rows between 19 preceding and current row)av20 from daily_bars join u using(code)where source='pan')select code,date,(c-l)/nullif(h-l,0)close_position,abs(c/ma20-1)ma20_distance,v/nullif(av20,0)volume_ratio from x"""
 with duckdb.connect(str(db),read_only=True)as c:return c.execute(q).fetchdf()
def top3(x,col,asc):return x.sort_values(['dt',col,'rank','code'],ascending=[True,asc,True,True]).groupby('dt').head(3).copy()
def branch(base,chall):
 rows=[]
 for d in sorted(set(base.dt)|set(chall.dt)):
  a=list(base[base.dt==d].code.astype(str));b=list(chall[chall.dt==d].code.astype(str));u=set(a)|set(b);i=set(a)&set(b);rows.append({'date':int(d),'changed_members_count':len(set(a)^set(b)),'changed_rank_count':sum(x!=y for x,y in zip(a,b)),'jaccard':len(i)/len(u)if u else 1})
 return rows
def daily_pf(z):
 d=z.assign(ret=z.pnl_yen/z.invested_yen).groupby('exit_date').ret.mean();neg=-d[d<0].sum();return None if neg==0 else float(d[d>0].sum()/neg)
def run_lane(raw):
 raw=raw.copy();raw['year']=raw.signal_year;raw['split']=raw.signal_year.map({2024:'train',2025:'validation',2026:'shadow'});raw['tie_gap_ma60']=-raw['rank'];z,_=replay(raw,.001);return z
def generate(db,out):
 pool=ranked_events(db).merge(features(db),on=['code','date'],how='left',validate='many_to_one');base_raw=top3(pool,'rank',True);base=run_lane(base_raw);bm={s:{**stats(base[base.split==s]),'daily_profit_factor':daily_pf(base[base.split==s])}for s in ('train','validation','shadow')};reports=[]
 for vid,(col,asc)in VARIANTS.items():
  raw=top3(pool,col,asc);z=run_lane(raw);met={s:{**stats(z[z.split==s]),'daily_profit_factor':daily_pf(z[z.split==s])}for s in ('train','validation','shadow')};br=branch(base_raw,raw);tr=[r for r in br if str(r['date']).startswith('2024')];rate=sum(r['changed_members_count']>0 for r in tr)/len(tr)if tr else 0;reports.append({'variant':vid,'feature':col,'metrics':met,'branching':{'train_changed_day_rate':rate,'days':br},'train_gate':rate>=.2 and met['train']['n']>=.8*bm['train']['n'],'ledger':z})
 eligible=[r for r in reports if r['train_gate']and r['metrics']['train']['daily_profit_factor']is not None];chosen=max(eligible,key=lambda r:r['metrics']['train']['daily_profit_factor'],default=None);decision='drop_no_train_candidate';gate={}
 if chosen:
  v,b=chosen['metrics']['validation'],bm['validation'];s,sb=chosen['metrics']['shadow'],bm['shadow'];gate={'validation_pf_delta':v['profit_factor']-b['profit_factor'],'validation_expectancy_improved':v['expectancy']>b['expectancy'],'tail_non_worse':v['p05']>=b['p05'],'dd_non_worse':v['max_drawdown_yen']>=b['max_drawdown_yen'],'frequency_ratio':v['signal_days']/b['signal_days']if b['signal_days']else None,'shadow_direction_consistent':s['profit_factor']>=sb['profit_factor']and s['expectancy']>=sb['expectancy']};decision='keep'if gate['validation_pf_delta']>=.1 and gate['validation_expectancy_improved']and gate['tail_non_worse']and gate['dd_non_worse']and gate['frequency_ratio']>=.8 and gate['shadow_direction_consistent']else'drop'
 now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_meemee_shape_rerank_top3_v1";root.mkdir(parents=True);base.to_csv(root/'baseline_events.csv',index=False)
 for r in reports:r['ledger'].to_csv(root/f"{r['variant']}_events.csv",index=False);del r['ledger']
 script=Path(__file__);payload={'schema_version':'tradex_meemee_shape_rerank_top3_v1.compare.v1','artifact_role':'authoritative','fixed_evaluation_conditions':{'pool':'point-in-time MeeMee up top5 after gap<=0 and H10 maturity','top_k':3,'variants':list(VARIANTS),'entry':'next_open','tp':.08,'sl':.05,'horizon':10,'slippage':.001,'maximum_positions':4,'slot_yen':2400000,'splits':{'train':2024,'validation':2025,'shadow':2026},'top5_external_additions':0},'baseline':bm,'reports':reports,'selection':{'protocol':'highest 2024 train daily PF among branch>=20% and n>=80% baseline','selected_variant':chosen['variant']if chosen else None},'gate':gate,'decision':{'candidate_local_decision':decision,'authoritative_rollup_decision':'research_only'},'hashes':{'db_snapshot_sha256':sha(db),'runner_sha256':sha(script),'rule_source_sha256':sha(Path(__file__).with_name('tradex_meemee_leaf_consensus_top3_v1.py'))},'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};p=root/'compare.json';p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+'\n');return p
def main():
 a=argparse.ArgumentParser();a.add_argument('--db',type=Path,default=DB);a.add_argument('--out',type=Path,default=OUT);x=a.parse_args();print(generate(x.db,x.out))
if __name__=='__main__':main()
