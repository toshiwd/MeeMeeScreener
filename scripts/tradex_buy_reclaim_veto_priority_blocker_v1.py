from __future__ import annotations
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
SOURCE=Path(r'C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_revision_r4_reclaim_quality_gate.json');BASE=Path(r'G:\Tradex\point_in_time_side_priority_top3_v1\20260713T054227Z-tradex_point_in_time_side_priority_top3_v1\baseline_fixed_interleave_top3.csv');DB=Path(r'G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb');OUT=Path(r'G:\Tradex\buy_reclaim_veto_priority_v1')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def audit(source:Path,base:Path,db:Path)->dict:
 s=json.loads(source.read_text(encoding='utf-8'));gate=s['challenger']['gate_definition'];b=pd.read_csv(base);buy=b[b.side=='buy'];
 with duckdb.connect(str(db),read_only=True)as c:
  cols={r[1] for r in c.execute("pragma table_info('ml_feature_daily')").fetchall()};tag_payload=int(c.execute("select count(*) from ranking_appearance_daily where payload_json like '%MA20_RECLAIM_INITIAL%'").fetchone()[0]);rows=c.execute("select cast(strftime(to_timestamp(dt),'%Y%m%d')as int)ymd,code,diff20_pct,weekly_breakout_up_prob,monthly_breakout_up_prob from ml_feature_daily where cast(strftime(to_timestamp(dt),'%Y%m%d')as int) between 20240101 and 20261231").fetchdf()
 buy=buy.copy();buy['code']=buy.code.astype(str);rows['code']=rows.code.astype(str);matched=buy.merge(rows,left_on=['signal_ymd','code'],right_on=['ymd','code'],how='left');feature_columns={x:x.rsplit('.',1)[-1] for x in gate['features']};coverage={source_name:int(matched[column].notna().sum()) for source_name,column in feature_columns.items()};required=set(feature_columns.values())
 tag_columns=[]
 with duckdb.connect(str(db),read_only=True)as c:
  for table, in c.execute("select table_name from information_schema.tables where table_schema='main'").fetchall():
   for r in c.execute(f"pragma table_info('{table}')").fetchall():
    if 'ma20_reclaim_initial' in str(r[1]).lower():tag_columns.append(f'{table}.{r[1]}')
 return {'source_gate':gate,'baseline_buy_rows':len(buy),'feature_columns_present':sorted(required&cols),'feature_non_null_on_baseline':coverage,'ma20_reclaim_initial_provenance':{'dedicated_columns':tag_columns,'ranking_payload_occurrences':tag_payload,'baseline_column_present':'strategy_id'in buy.columns or 'setup_id'in buy.columns},'blocker':{'status':'blocked','typed_reason':'MA20_RECLAIM_INITIAL_POINT_IN_TIME_PROVENANCE_UNAVAILABLE','detail':'numeric features are reproducible, but the mandatory setup membership is not persisted for corrected unified candidates; applying the veto to all BUY rows would change the authoritative condition'},'fallback_used':False}
def generate(source,base,db,out):
 a=audit(source,base,db);now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_buy_reclaim_veto_priority_blocker_v1";root.mkdir(parents=True);p={'schema_version':'tradex_buy_reclaim_veto_priority_blocker_v1.compare.v1','artifact_role':'authoritative','research_phase':'comparison_stabilization','fixed_intended_change':'priority demotion only; no suppression; BUY MA20_RECLAIM_INITIAL only; SELL unchanged','source_artifacts':[{'path':str(source),'sha256':sha(source)},{'path':str(base),'sha256':sha(base)},{'path':str(db),'sha256':sha(db)}],**a,'implementation_performed':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};q=root/'compare.json';q.write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n');return q
def main():
 a=argparse.ArgumentParser();a.add_argument('--source',type=Path,default=SOURCE);a.add_argument('--base',type=Path,default=BASE);a.add_argument('--db',type=Path,default=DB);a.add_argument('--out',type=Path,default=OUT);x=a.parse_args();print(generate(x.source,x.base,x.db,x.out))
if __name__=='__main__':main()
