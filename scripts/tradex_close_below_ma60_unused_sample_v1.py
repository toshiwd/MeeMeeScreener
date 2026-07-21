"""Freeze an outcome-blind unused sample for the close-below-MA60 short axis."""
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

SEED="close-below-ma60-unused-v1";SELL={"PROBE","CORE","ADD","REENTRY_PROBE"}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def h(*x):return hashlib.sha256("|".join(map(str,x)).encode()).hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--actions",type=Path,required=True);ap.add_argument("--daily",type=Path,required=True);ap.add_argument("--exclude-codes",type=Path,action="append",default=[]);ap.add_argument("--sample-size",type=int,default=64);ap.add_argument("--per-axis",type=int,default=0);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 excluded=set();ex_sources=[]
 for p in a.exclude_codes:
  x=pd.read_parquet(p,columns=["code"]);codes=set(x.code.astype(str).str.zfill(4));excluded|=codes;ex_sources.append({"path":str(p.resolve()),"sha256":sha(p),"codes":len(codes)})
 actions=pd.read_parquet(a.actions);actions.code=actions.code.astype(str).str.zfill(4);actions.ymd=actions.ymd.astype(int);actions=actions[(actions.ymd//10000).between(2020,2025)&actions.action.isin(SELL)&~actions.code.isin(excluded)].copy()
 daily=pd.read_parquet(a.daily,columns=["code","ymd","c","ma60"]);daily.code=daily.code.astype(str).str.zfill(4);daily.ymd=daily.ymd.astype(int)
 pool=actions.merge(daily,on=["code","ymd"],how="inner",validate="many_to_one");pool=pool[pool.c.notna()&pool.ma60.notna()].copy();pool["below_ma60"]=pool.c<pool.ma60;pool["event_hash"]=[h(SEED,"EVENT",c,y) for c,y in zip(pool.code,pool.ymd)]
 if a.per_axis:
  below=pool[pool.below_ma60].sort_values(["event_hash","code","ymd"]).drop_duplicates("code");below["code_hash"]=[h(SEED,"BELOW","CODE",c) for c in below.code];below=below.sort_values(["code_hash","code"]).head(a.per_axis).copy();used=set(below.code)
  above=pool[(~pool.below_ma60)&~pool.code.isin(used)].sort_values(["event_hash","code","ymd"]).drop_duplicates("code");above["code_hash"]=[h(SEED,"ABOVE","CODE",c) for c in above.code];above=above.sort_values(["code_hash","code"]).head(a.per_axis).copy();sample=pd.concat([below,above],ignore_index=True);expected=a.per_axis*2
 else:
  pool=pool.sort_values(["event_hash","code","ymd"]).drop_duplicates("code");pool["code_hash"]=[h(SEED,"CODE",c) for c in pool.code];sample=pool.sort_values(["code_hash","code"]).head(a.sample_size).copy();expected=a.sample_size
 if len(sample)!=expected:raise RuntimeError(f"sample supply {len(sample)}/{expected}")
 sample["axis_distance_pct"]=sample.c/sample.ma60-1;sample["model_action"]=sample.action;sample["year"]=sample.ymd//10000;sample=sample.sort_values(["code_hash","code"]).reset_index(drop=True);sample.insert(0,"case_id",[f"M{i:03d}" for i in range(1,len(sample)+1)])
 board=sample[["case_id","code","ymd","model_action","reason","monthly_state","c","ma60","below_ma60","axis_distance_pct","event_hash","code_hash","year"]].copy();board["outcome_joined"]=False;board.to_parquet(a.output/"unused_sample_frozen.parquet",index=False)
 gates={"rows_match":len(board)==expected,"unique_codes":board.code.nunique()==expected,"unique_events":board[["code","ymd"]].drop_duplicates().shape[0]==expected,"excluded_code_hits_0":not bool(set(board.code)&excluded),"outcome_joined_false":not bool(board.outcome_joined.any()),"future_columns_0":not any(c.startswith(("ret_close_","down_exc_","up_exc_","forward_")) for c in board),"both_axis_sides_present":bool(board.below_ma60.any() and (~board.below_ma60).any()),"per_axis_match":not a.per_axis or int(board.below_ma60.sum())==a.per_axis}
 if not all(gates.values()):raise RuntimeError(gates)
 result={"schema_version":"tradex_close_below_ma60_unused_sample_v1.compare.v1","artifact_role":"authoritative_outcome_blind_unused_sample","review_only":True,"status":"frozen_before_outcome_reveal","fixed_conditions":{"seed":SEED,"years":list(range(2020,2026)),"one_event_per_code":True,"sample_size":expected,"per_axis":a.per_axis or None,"case_control_axis_balance":bool(a.per_axis),"aggregate_pf_is_natural_frequency_estimate":not bool(a.per_axis),"selection_uses_future_outcomes":False,"changed_axis_only":"close < PIT MA60","weekly_inputs":[]},"observed_sample":{"below_ma60":int(board.below_ma60.sum()),"at_or_above_ma60":int((~board.below_ma60).sum()),"by_year":{str(k):int(v) for k,v in board.year.value_counts().sort_index().items()},"by_action":{str(k):int(v) for k,v in board.model_action.value_counts().items()}},"gates":gates,"not_changed":["model rules","MeeMee","ranking","runtime DB","production trading logic"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"sources":{"actions":{"path":str(a.actions.resolve()),"sha256":sha(a.actions)},"daily":{"path":str(a.daily.resolve()),"sha256":sha(a.daily)}},"exclusion_sources":ex_sources,"excluded_unique_codes":len(excluded),"board_sha256":sha(a.output/"unused_sample_frozen.parquet"),"gates":gates};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),**result["observed_sample"],"gates":gates},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
