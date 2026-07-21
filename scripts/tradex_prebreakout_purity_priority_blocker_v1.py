from __future__ import annotations
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import duckdb,pandas as pd
SOURCE=Path(r'C:\work\meemee-screener\artifacts\research_inventory\tradex_prebreakout_actionability_compare_v2.json');BASE=Path(r'G:\Tradex\point_in_time_side_priority_top3_v1\20260713T054227Z-tradex_point_in_time_side_priority_top3_v1\baseline_fixed_interleave_top3.csv');DB=Path(r'G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb');OUT=Path(r'G:\Tradex\prebreakout_purity_priority_v1')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def audit(source,base,db):
 s=json.loads(Path(source).read_text(encoding='utf-8'));definition=s['challenge_definition'];features=sorted({x for v in definition['feature_map'].values()for x in v.get('features',[])})
 with duckdb.connect(str(db),read_only=True)as c:
  table_cols={};available=set()
  for table, in c.execute("select table_name from information_schema.tables where table_schema='main'").fetchall():
   cols={r[1]for r in c.execute(f"pragma table_info('{table}')").fetchall()};hit=sorted(set(features)&cols)
   if hit:table_cols[table]=hit;available.update(hit)
  ff=c.execute("select cast(strftime(to_timestamp(dt),'%Y%m%d')as int)signal_ymd,cast(code as varchar)code,"+','.join(sorted(available&{'breakout20_up','candle_upper_wick_ratio','diff20_pct','drawdown60','high20_dist','monthly_breakout_up_prob','rebound60','turnover_z20','weekly_breakout_up_prob'}))+" from feature_frame_daily where cast(strftime(to_timestamp(dt),'%Y%m%d')as int)between 20240101 and 20261231").fetchdf()
 b=pd.read_csv(base);b=b[b.side=='buy'].copy();b['code']=b.code.astype(str);m=b.merge(ff,on=['signal_ymd','code'],how='left');coverage={f:int(m[f].notna().sum())if f in m else 0 for f in features}
 missing=sorted(set(features)-available);formula=definition.get('formula',{});component_transforms={k:formula.get(k)for k in definition['feature_map']}
 blockers=[]
 if missing:blockers.append('RAW_POINT_IN_TIME_FEATURES_MISSING')
 if any(v is None for v in component_transforms.values()):blockers.append('RAW_TO_COMPONENT_TRANSFORMS_UNSPECIFIED')
 return {'source_decision':s.get('judgment',{}).get('authoritative_rollup_decision'),'required_features':features,'available_feature_tables':table_cols,'missing_features':missing,'baseline_buy_rows':len(b),'non_null_coverage':coverage,'component_transform_provenance':component_transforms,'blocker':{'status':'blocked','typed_reason':'PREBREAKOUT_PURITY_POINT_IN_TIME_PROVENANCE_INCOMPLETE','subreasons':blockers,'detail':'authoritative v2 names component formulas but does not persist all raw features or deterministic raw-to-component transforms for corrected 2024-2026 candidates'},'fallback_used':False}
def generate(source,base,db,out):
 a=audit(source,base,db);now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_prebreakout_purity_priority_blocker_v1";root.mkdir(parents=True);p={'schema_version':'tradex_prebreakout_purity_priority_blocker_v1.compare.v1','artifact_role':'authoritative','research_phase':'comparison_stabilization','fixed_intended_change':'BUY priority only; no suppression; SELL and execution unchanged','source_artifacts':[{'path':str(source),'sha256':sha(source)},{'path':str(base),'sha256':sha(base)},{'path':str(db),'sha256':sha(db)}],**a,'implementation_performed':False,'runtime_db_write':False,'production_ranking_changed':False,'meemee_changed':False};q=root/'compare.json';q.write_text(json.dumps(p,ensure_ascii=False,indent=2)+'\n');return q
def main():
 a=argparse.ArgumentParser();a.add_argument('--source',type=Path,default=SOURCE);a.add_argument('--base',type=Path,default=BASE);a.add_argument('--db',type=Path,default=DB);a.add_argument('--out',type=Path,default=OUT);x=a.parse_args();print(generate(x.source,x.base,x.db,x.out))
if __name__=='__main__':main()
