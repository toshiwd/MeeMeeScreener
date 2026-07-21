from __future__ import annotations
import argparse,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
if __package__ in (None,""):sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.tradex_leaf_cap4_slot24_2026_matured_oos_v1 import candidates
from scripts.tradex_leaf_order_contract_readiness_v1 import replay
DB=Path(r'G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb');OUT=Path(r'G:\Tradex\meemee_leaf_consensus_top3_v1')
def ranked_events(db:Path)->pd.DataFrame:
 h=','.join(f'lead(h,{i})over w h{i}' for i in range(1,11));l=','.join(f'lead(l,{i})over w l{i}' for i in range(1,11));ds=','.join(f'lead(date,{i})over w d{i}' for i in range(1,11));tp='least('+','.join(f'case when h{i}>=next_open*1.08 then {i} else 99 end' for i in range(1,11))+')';sl='least('+','.join(f'case when l{i}<=next_open*.95 then {i} else 99 end' for i in range(1,11))+')'
 hits=' '.join(f' when {i} then d{i}' for i in range(1,11))
 q=f"""with u as(select code from daily_bars where source='pan' group by code having max(date)=(select max(date)from daily_bars where source='pan')),b as(select code,date,cast(strftime(to_timestamp(date),'%Y%m%d')as int)ymd,c,lead(o)over w next_open,lead(date)over w next_entry,lead(c,10)over w close_horizon,{h},{l},{ds} from daily_bars join u using(code)where source='pan' window w as(partition by code order by date)),s as(select r.code,r.dt,r.rank,b.* exclude(code),{tp}td,{sl}sd from ranking_appearance_daily r join b on b.code=r.code and b.ymd=r.dt where r.dir='up'and r.rank<=5 and r.ranking_logic_version='ranking:trade:top50:v1'and r.dt between 20240101 and 20261231 and b.close_horizon is not null and b.next_open/b.c-1<=0)select code,dt,date,next_entry next_entry_date,case when dt<20250101 then 2024 when dt<20260101 then 2025 else 2026 end signal_year,next_open entry_price,rank,case when sd<=10 and sd<=td then case sd {hits} end when td<=10 then case td {hits} end else d10 end exit_date,case when sd<=10 and sd<=td then -.05 when td<=10 then .08 else close_horizon/next_open-1 end next_open_return from s"""
 with duckdb.connect(str(db),read_only=True)as c:return c.execute(q).fetchdf()
def select_lanes(ranks,leaf):
 keys=set(zip(leaf.code.astype(str),leaf.date.astype('int64')));x=ranks.copy();x['consensus_indicator']=[int((str(c),int(d))in keys)for c,d in zip(x.code,x.date)]
 b=x.sort_values(['dt','rank','code']).groupby('dt').head(3).copy();q=x.sort_values(['dt','consensus_indicator','rank','code'],ascending=[True,False,True,True]).groupby('dt').head(3).copy();return b,q,x
def stats(x):
 r=x.pnl_yen/x.invested_yen if len(x)else pd.Series(dtype=float);loss=r[r<0];neg=-loss.sum();daily=x.groupby('exit_date').pnl_yen.sum().sort_index()if len(x)else pd.Series(dtype=float);eq=daily.cumsum();dd=eq-eq.cummax();weeks=x.next_entry_date.apply(lambda v:datetime.fromtimestamp(int(v),timezone.utc).strftime('%G-W%V')).nunique()if len(x)else 0
 return {'n':len(x),'signal_days':int(x.next_entry_date.nunique())if len(x)else 0,'weekly_frequency':len(x)/weeks if weeks else None,'expectancy':float(r.mean())if len(r)else None,'profit_factor':float(r[r>0].sum()/neg)if neg else None,'win_rate':float((r>0).mean())if len(r)else None,'payoff_ratio':float(r[r>0].mean()/abs(loss.mean()))if len(r[r>0])and len(loss)else None,'p05':float(r.quantile(.05))if len(r)else None,'pnl_yen':float(x.pnl_yen.sum())if len(x)else 0.0,'max_drawdown_yen':float(dd.min())if len(dd)else 0.0}
def generate(db:Path,out:Path)->Path:
 ranks=ranked_events(db);leaf=candidates(db,0.0);base,chal,allrows=select_lanes(ranks,leaf)
 for z,kind in ((base,'baseline'),(chal,'challenger')):z['year']=z.signal_year;z['split']=z.signal_year.map({2024:'train',2025:'validation',2026:'shadow'});z['tie_gap_ma60']=(-z['rank'] if kind=='baseline' else z.consensus_indicator*100-z['rank'])
 br=[]
 for d,g in allrows.groupby('dt'):
  a=base[base.dt==d];b=chal[chal.dt==d];ai=list(a.code.astype(str));bi=list(b.code.astype(str));inter=len(set(ai)&set(bi));changed=len(set(ai)^set(bi));br.append({'date':int(d),'changed_members_count':changed,'changed_rank_count':sum(x!=y for x,y in zip(ai,bi)),'jaccard':inter/len(set(ai)|set(bi)) if set(ai)|set(bi)else 1,'reason':'leaf_consensus_priority'if changed else'no_leaf_consensus_branch'})
 ledgers={};metrics={}
 for name,raw in (('baseline',base),('challenger',chal)):
  z,_=replay(raw,.001);ledgers[name]=z;metrics[name]={s:stats(z[z.split==s])for s in ('train','validation','shadow')}
 branch_rate=sum(r['changed_members_count']>0 for r in br)/len(br)if br else 0;bv=metrics['baseline']['validation'];cv=metrics['challenger']['validation'];bs=metrics['baseline']['shadow'];cs=metrics['challenger']['shadow'];tail_ok=cv['p05']>=bv['p05']and cv['max_drawdown_yen']>=bv['max_drawdown_yen'];vg=cv['profit_factor']is not None and bv['profit_factor']is not None and cv['profit_factor']-bv['profit_factor']>=.10 and cv['expectancy']>bv['expectancy']and branch_rate>=.20 and tail_ok;shadow=(cs['expectancy']or 0)>=(bs['expectancy']or 0)and(cs['profit_factor']or 0)>=(bs['profit_factor']or 0)
 now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_meemee_leaf_consensus_top3_v1";root.mkdir(parents=True)
 for n,z in ledgers.items():z.to_csv(root/f'{n}_events.csv',index=False)
 p={'schema_version':'tradex_meemee_leaf_consensus_top3_v1.compare.v1','artifact_role':'authoritative','fixed_evaluation_conditions':{'universe':'current PAN symbols','ranking_input_top_k':5,'selection_top_k':3,'entry':'next_open_gap_le_0','tp':.08,'sl':.05,'horizon':10,'slippage':.001,'maximum_positions':4,'slot_yen':2400000,'splits':{'train':2024,'validation':2025,'shadow_matured':2026}},'metrics':metrics,'branching':{'changed_day_rate':branch_rate,'days':br},'gate':{'validation_pf_delta':None if cv['profit_factor']is None or bv['profit_factor']is None else cv['profit_factor']-bv['profit_factor'],'validation_expectancy_above':cv['expectancy']>bv['expectancy'],'branching_ge_20pct':branch_rate>=.2,'tail_dd_non_worse':tail_ok,'shadow_direction_consistent':shadow},'decision':{'candidate_local_decision':'keep'if vg and shadow else'drop','authoritative_rollup_decision':'research_only'},'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};path=root/'compare.json';path.write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return path
def main():
 a=argparse.ArgumentParser();a.add_argument('--db',type=Path,default=DB);a.add_argument('--out',type=Path,default=OUT);x=a.parse_args();print(generate(x.db,x.out))
if __name__=='__main__':main()
