from __future__ import annotations
import argparse,hashlib,json,sys
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import duckdb

REPO=Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:sys.path.insert(0,str(REPO))
from app.backend.services.tradex_research_contracts import build_run_manifest

AXIS_ID="tradex_phase5_long_cell_path_risk_v1"
SOURCE=Path(r"G:\Tradex\ma_phase_feature_base_v1\20260603T121215Z-ma-phase-feature-base-v1\ma_phase_features.parquet")
OUT=Path(r"G:\Tradex\phase5_long_cell_path_risk_v1")
TRAIN=(20200101,20221231);VALIDATION=(20230101,20241231);TEST=(20250101,20261231)

def _sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()

def _pf(g:Any,l:Any)->float|None:return float(g)/float(l) if l and float(l)>0 else None

def analyze(source:Path)->dict[str,Any]:
 con=duckdb.connect()
 try:
  con.execute(f"""CREATE TEMP VIEW eligible AS SELECT code,ymd,ret_20b,max_drawdown_20b,max_up_20b,bars_since_cross_above_ma20,cast(ymd/100 as integer) month_bucket,strftime(strptime(cast(ymd as varchar),'%Y%m%d'),'%G-W%V') week_bucket FROM read_parquet('{source.as_posix()}') WHERE ymd<={VALIDATION[1]} AND ret_20b IS NOT NULL AND max_drawdown_20b IS NOT NULL AND max_up_20b IS NOT NULL AND lower_support_bucket='none_near'""")
  def pack(condition:str,start:int,end:int)->dict[str,Any]:
   where=f"ymd between {start} and {end} and ({condition})"
   row=con.execute(f"""SELECT count(*) n,count(distinct code) unique_codes,avg(ret_20b) expectancy,avg(cast(ret_20b>0 as integer)) win_rate,avg(ret_20b) filter(where ret_20b>0) avg_win,avg(ret_20b) filter(where ret_20b<0) avg_loss,quantile_cont(ret_20b,.05) loss_tail_p5,quantile_cont(ret_20b,.01) loss_tail_p1,avg(max_drawdown_20b) mae_mean,quantile_cont(max_drawdown_20b,.10) mae_p10,quantile_cont(max_drawdown_20b,.05) mae_p5,avg(max_up_20b) mfe_mean,sum(case when ret_20b>0 then ret_20b else 0 end) gain,-sum(case when ret_20b<0 then ret_20b else 0 end) loss FROM eligible WHERE {where}""").fetchone();cols=[x[0] for x in con.description];d=dict(zip(cols,row));
   daily=con.execute(f"""WITH x AS (SELECT ymd,avg(ret_20b) r FROM eligible WHERE {where} GROUP BY ymd) SELECT count(*) trade_days,avg(r) expectancy,sum(case when r>0 then r else 0 end) gain,-sum(case when r<0 then r else 0 end) loss FROM x""").fetchone();dc=[x[0] for x in con.description];dd=dict(zip(dc,daily))
   weeks=con.execute(f"SELECT week_bucket,count(*) signals FROM eligible WHERE {where} GROUP BY week_bucket ORDER BY week_bucket").fetchall()
   months=con.execute(f"""SELECT month_bucket,count(*) n,avg(ret_20b) expectancy,sum(case when ret_20b>0 then ret_20b else 0 end) gain,-sum(case when ret_20b<0 then ret_20b else 0 end) loss FROM eligible WHERE {where} GROUP BY month_bucket ORDER BY expectancy,month_bucket LIMIT 12""").fetchall()
   avgwin=float(d['avg_win']) if d['avg_win'] is not None else None;avgloss=float(d['avg_loss']) if d['avg_loss'] is not None else None
   return {"n":int(d['n']),"unique_codes":int(d['unique_codes']),"expectancy":float(d['expectancy']) if d['expectancy'] is not None else None,"profit_factor":_pf(d['gain'],d['loss']),"win_rate":float(d['win_rate']) if d['win_rate'] is not None else None,"avg_win":avgwin,"avg_loss":avgloss,"payoff_ratio":avgwin/abs(avgloss) if avgwin is not None and avgloss not in (None,0) else None,"loss_tail_p5":float(d['loss_tail_p5']) if d['loss_tail_p5'] is not None else None,"loss_tail_p1":float(d['loss_tail_p1']) if d['loss_tail_p1'] is not None else None,"mae_mean":float(d['mae_mean']) if d['mae_mean'] is not None else None,"mae_p10":float(d['mae_p10']) if d['mae_p10'] is not None else None,"mae_p5":float(d['mae_p5']) if d['mae_p5'] is not None else None,"mfe_mean":float(d['mfe_mean']) if d['mfe_mean'] is not None else None,"daily_equal_weight":{"days":int(dd['trade_days']),"expectancy":float(dd['expectancy']) if dd['expectancy'] is not None else None,"profit_factor":_pf(dd['gain'],dd['loss'])},"weekly_signal_frequency":{"weeks":len(weeks),"mean":sum(x[1] for x in weeks)/len(weeks) if weeks else None,"max":max((x[1] for x in weeks),default=None),"rows":[{"week":x[0],"signals":int(x[1])} for x in weeks]},"worst_months":[{"month":int(x[0]),"n":int(x[1]),"expectancy":float(x[2]),"profit_factor":_pf(x[3],x[4])} for x in months]}
  candidate="bars_since_cross_above_ma20=0"
  baseline="bars_since_cross_above_ma20 between 0 and 5"
  return {"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment","fixed_candidate":{"candidate_id":"long_bars_since_cross_ma20_x_lower_support:0|none_near","condition":"bars_since_cross_above_ma20=0 AND lower_support_bucket='none_near'","condition_reselection":False,"threshold_reselection":False},"comparison_baseline":{"baseline_id":"early_lower_support_absent_0_5","condition":"bars_since_cross_above_ma20 BETWEEN 0 AND 5 AND lower_support_bucket='none_near'","role":"diagnostic_comparator_only_not_candidate_reselection"},"periods":{"train":TRAIN,"validation":VALIDATION,"test":TEST,"maximum_observed_date":VALIDATION[1]},"candidate":{"train":pack(candidate,*TRAIN),"validation":pack(candidate,*VALIDATION)},"baseline":{"train":pack(baseline,*TRAIN),"validation":pack(baseline,*VALIDATION)},"test_access":{"status":"not_opened","rows_read":False,"metrics":None},"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
 finally:con.close()

def run(source:Path=SOURCE,out:Path=OUT)->Path:
 c=analyze(source);stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ');root=out/f'{stamp}-{AXIS_ID}';root.mkdir(parents=True,exist_ok=False);(root/'compare.json').write_text(json.dumps(c,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
 m=build_run_manifest(session_id=root.name,seed=0,random_seed=0,input_artifacts=[{'path':str(source),'sha256':_sha(source)}],asof=str(VALIDATION[1]),config={'candidate':c['fixed_candidate'],'baseline':c['comparison_baseline'],'test_not_opened':True},universe=['source_parquet_all_codes'],period={'train':TRAIN,'validation':VALIDATION,'test_locked':TEST},horizon='20_business_days',artifact_detail_level='authoritative_full',fallback_status='authoritative');m.update({'artifact_role':'authoritative','compare_path':str(root/'compare.json'),'test_rows_read':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False});(root/'run_manifest.json').write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');return root

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--source',type=Path,default=SOURCE);p.add_argument('--output-root',type=Path,default=OUT);a=p.parse_args();print(run(a.source,a.output_root))
