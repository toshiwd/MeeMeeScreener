from __future__ import annotations
import argparse,hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
if __package__ in(None,''):sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.tradex_meemee_leaf_consensus_top3_v1 import ranked_events
from scripts.tradex_leaf_order_contract_readiness_v1 import replay
DB=Path(r'G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb');ROOT=Path(r'G:\Tradex\adaptive_rule_router_v1\20260712T123514Z-tradex_adaptive_rule_router_v1');OUT=Path(r'G:\Tradex\pinned_adaptive_vs_meemee_2026_v1')
def sha(p):
 h=hashlib.sha256();
 with Path(p).open('rb')as f:
  for b in iter(lambda:f.read(8<<20),b''):h.update(b)
 return h.hexdigest()
def outcomes(db):
 h=','.join(f'lead(h,{i})over w h{i}'for i in range(1,11));l=','.join(f'lead(l,{i})over w l{i}'for i in range(1,11));ds=','.join(f'lead(date,{i})over w d{i}'for i in range(1,11));tp='least('+','.join(f'case when h{i}>=next_open*1.08 then {i}else 99 end'for i in range(1,11))+')';sl='least('+','.join(f'case when l{i}<=next_open*.95 then {i}else 99 end'for i in range(1,11))+')';hits=' '.join(f'when {i} then d{i}'for i in range(1,11))
 q=f"""with b as(select code,date,cast(strftime(to_timestamp(date),'%Y%m%d')as int)ymd,c,lead(o)over w next_open,lead(date)over w next_entry,lead(c,10)over w close_horizon,{h},{l},{ds} from daily_bars where source='pan' window w as(partition by code order by date)),s as(select *,{tp}td,{sl}sd from b where ymd between 20260101 and 20260605 and close_horizon is not null and next_open/c-1<=0)select code,ymd,date,next_entry next_entry_date,next_open entry_price,case when sd<=10 and sd<=td then case sd {hits} end when td<=10 then case td {hits} end else d10 end exit_date,case when sd<=10 and sd<=td then -.05 when td<=10 then .08 else close_horizon/next_open-1 end next_open_return from s"""
 with duckdb.connect(str(db),read_only=True)as c:x=c.execute(q).fetchdf()
 x['code']=x.code.astype(str)
 return x
def select_adaptive(path,calendar):
 x=pd.read_csv(path,parse_dates=['signal_date']);x=x[(x.side=='buy')&x.signal_date.dt.year.eq(2026)].copy();x['code']=x.code.astype(str);x['ymd']=x.signal_date.dt.strftime('%Y%m%d').astype(int);x=x[x.ymd.isin(calendar)];return x.sort_values(['ymd','router_score','code'],ascending=[True,False,True]).groupby('ymd').head(3)
def select_meemee(db,calendar):
 x=ranked_events(db);x['code']=x.code.astype(str);x=x[x.dt.isin(calendar)];return x.sort_values(['dt','rank','code']).groupby('dt').head(3).rename(columns={'dt':'ymd'})
def metric(x,calendar):
 r=x.pnl_yen/x.invested_yen if len(x)else pd.Series(dtype=float);daily=x.assign(rr=r).groupby('ymd').rr.mean().reindex(calendar,fill_value=0);neg=-daily[daily<0].sum();loss=r[r<0];cut=r.quantile(.1)if len(r)else None;eq=x.groupby('exit_date').pnl_yen.sum().sort_index().cumsum()if len(x)else pd.Series(dtype=float);dd=eq-eq.cummax();weeks=pd.to_datetime(pd.Series(calendar).astype(str)).dt.strftime('%G-W%V').nunique()
 return {'n':len(x),'signal_days':int(x.ymd.nunique()),'events_per_calendar_week':len(x)/weeks,'profit_factor':float(r[r>0].sum()/-r[r<0].sum())if len(r[r<0])else None,'daily_profit_factor':float(daily[daily>0].sum()/neg)if neg else None,'expectancy':float(r.mean())if len(r)else None,'calendar_expectancy':float(daily.mean()),'win_rate':float((r>0).mean())if len(r)else None,'payoff_ratio':float(r[r>0].mean()/abs(loss.mean()))if len(r[r>0])and len(loss)else None,'p05':float(r.quantile(.05))if len(r)else None,'cvar10':float(r[r<=cut].mean())if cut is not None else None,'max_drawdown_yen':float(dd.min())if len(dd)else 0.0}
def generate(db,root,out):
 with duckdb.connect(str(db),read_only=True)as c:calendar=[int(x[0])for x in c.execute("select distinct dt from ranking_appearance_daily where dir='up'and ranking_logic_version='ranking:trade:top50:v1'and dt between 20260101 and 20260605 order by dt").fetchall()]
 oc=outcomes(db);a=select_adaptive(root/'routed_events.csv',calendar).merge(oc,on=['code','ymd']);m=select_meemee(db,calendar).rename(columns={'date':'signal_epoch'});
 for x,key in((a,'router_score'),(m,'rank')):x['year']=2026;x['split']='shadow';x['tie_gap_ma60']=x[key]if key=='router_score'else-x[key]
 aa,_=replay(a,.001);mm,_=replay(m,.001);days=[]
 for d in calendar:
  av=list(a[a.ymd==d].code.astype(str));mv=list(m[m.ymd==d].code.astype(str));u=set(av)|set(mv);memb=len(set(av)^set(mv));days.append({'date':d,'changed_members_count':memb,'changed_rank_count':sum(x!=y for x,y in zip(av,mv)),'jaccard':len(set(av)&set(mv))/len(u)if u else 1,'adaptive_empty':not av,'meemee_empty':not mv,'one_side_empty':bool(av)!=bool(mv)})
 am,mmx=metric(aa,calendar),metric(mm,calendar);branch=sum(x['changed_members_count']>0 for x in days)/len(days);gate={'adaptive_pf_gte_1_3':(am['profit_factor']or 0)>=1.3,'frequency_gte_1_week':am['events_per_calendar_week']>=1,'p05_gte_minus5':(am['p05']or-1)>=-.05,'daily_pf_delta_gte_point1':(am['daily_profit_factor']or 0)-(mmx['daily_profit_factor']or 0)>=.1,'calendar_expectancy_improved':am['calendar_expectancy']>mmx['calendar_expectancy'],'branching_gte_20pct':branch>=.2}
 now=datetime.now(timezone.utc);dst=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_pinned_adaptive_vs_meemee_2026_v1";dst.mkdir(parents=True);aa.to_csv(dst/'adaptive_events.csv',index=False);mm.to_csv(dst/'meemee_events.csv',index=False);p={'schema_version':'tradex_pinned_adaptive_vs_meemee_2026_v1.compare.v1','artifact_role':'authoritative','fixed_conditions':{'period':[calendar[0],calendar[-1]],'calendar_days':len(calendar),'adaptive':'pinned 2026 forward only; router_score desc/code top3','meemee':'up top5 eligible then rank asc top3','entry':'next_open_gap_le_0','tp':.08,'sl':.05,'horizon':10,'slippage':.001,'maximum_positions':4,'slot_yen':2400000,'development_excluded':True},'adaptive':am,'meemee':mmx,'branching':{'changed_day_rate':branch,'days':days},'gate':gate,'decision':{'candidate_local_decision':'keep'if all(gate.values())else'drop','authoritative_rollup_decision':'research_only'},'hashes':{'adaptive_compare':sha(root/'compare.json'),'adaptive_ledger':sha(root/'routed_events.csv'),'db_snapshot':sha(db)},'runtime_db_write':False,'production_ranking_changed':False};q=dst/'compare.json';q.write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n');return q
def main():
 a=argparse.ArgumentParser();a.add_argument('--db',type=Path,default=DB);a.add_argument('--root',type=Path,default=ROOT);a.add_argument('--out',type=Path,default=OUT);x=a.parse_args();print(generate(x.db,x.root,x.out))
if __name__=='__main__':main()
