"""Freeze a second outcome-blind human/model complement review board."""
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

SEED = "human-complement-seven-below-v1"
BASE_EXCLUDED = {"9107","7733","3405","4208","7004","9531","4188","5631"}

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def rank(code,ymd,bucket): return hashlib.sha256(f"{SEED}|{bucket}|{code}|{ymd}".encode()).hexdigest()
def choose(x,bucket,n,used,year=None):
    x=x[~x.code.isin(used)].copy()
    if year is not None: x=x[x.ymd//10000==year]
    x["event_hash"]=[rank(c,y,f"{bucket}:EVENT") for c,y in zip(x.code,x.ymd)]
    x=x.sort_values(["event_hash","code","ymd"]).drop_duplicates("code")
    x["selection_hash"]=[hashlib.sha256(f"{SEED}|{bucket}:CODE|{c}".encode()).hexdigest() for c in x.code]
    out=x.sort_values(["selection_hash","code"]).head(n); used.update(out.code); return out
def choose_years(x,bucket,n,used):
    parts=[]
    for year in range(2020,2026):
        hit=choose(x,f"{bucket}:Y{year}",1,used,year)
        if len(hit)!=1: raise RuntimeError(f"no candidate {bucket} {year}")
        parts.append(hit)
    if n>6: parts.append(choose(x,f"{bucket}:REST",n-6,used))
    out=pd.concat(parts,ignore_index=True)
    if len(out)!=n: raise RuntimeError(f"shortfall {bucket}: {len(out)}")
    return out
def choose_actions(x,bucket,quotas,used):
    parts=[]
    for action,n in quotas.items():
        hit=choose(x[x.model_action.eq(action)],f"{bucket}:{action}",n,used)
        if len(hit)!=n: raise RuntimeError(f"action shortfall {bucket} {action}: {len(hit)}/{n}")
        parts.append(hit)
    return pd.concat(parts,ignore_index=True)

def main():
    ap=argparse.ArgumentParser()
    for name in ["actions","breakdown","monthly-state","daily"]: ap.add_argument("--"+name,required=True)
    ap.add_argument("--exclude-board",action="append",default=[])
    ap.add_argument("--remaining-profile",action="store_true",help="Use PROBE/REENTRY only after CORE supply is exhausted")
    ap.add_argument("--per-bucket",type=int,default=8); ap.add_argument("--output",type=Path,required=True); ap.add_argument("--sealed-output",type=Path,required=True)
    a=ap.parse_args(); a.output.mkdir(parents=True,exist_ok=False); a.sealed_output.mkdir(parents=True,exist_ok=False)
    excluded=set(BASE_EXCLUDED); exclusion_sources=[]
    for path in a.exclude_board:
        e=pd.read_parquet(path); codes=set(e.code.astype(str).str.zfill(4)); excluded |= codes
        exclusion_sources.append({"path":str(Path(path).resolve()),"sha256":sha(path),"codes":len(codes)})
    actions=pd.read_parquet(a.actions); actions.code=actions.code.astype(str).str.zfill(4)
    actions=actions[(actions.ymd//10000).between(2020,2025)&~actions.code.isin(excluded)]
    top=actions[actions.monthly_state.isin(["HIGH_ZONE_FAILURE","MATURE_BOX_UPPER"])&actions.action.isin(["PROBE","CORE","ADD"])].copy(); top["bucket"]="TOP_FAILURE";top["model_action"]=top.action
    ret=actions[(actions.monthly_state.eq("POST_BOX_RETURN_SELL")|actions.reason.isin(["return_sell_cross_ma7_ma20","rebound_to_ma20_after_exhaustion"]))&actions.action.isin(["PROBE","CORE","REENTRY_PROBE"])].copy();ret["bucket"]="RETURN_SELL";ret["model_action"]=ret.action
    br=pd.read_parquet(a.breakdown);br.code=br.code.astype(str).str.zfill(4);br=br[(br.ymd//10000).between(2020,2025)&~br.code.isin(excluded)].copy();br["bucket"]="BREAKDOWN_REJECTED";br["model_action"]="AVOID";br["reason"]="breakdown_branch_dropped"
    mcols=["code","ymd","source_month","effective_month","base_regime","confirmed_environment","monthly_selection_state","selection_age_sessions","new_entry_blocked_reason","current_local_box_position","current_month_body_pct","current_month_close_position"]
    m=pd.read_parquet(a.monthly_state,columns=mcols);m.code=m.code.astype(str).str.zfill(4)
    dcols=["code","ymd","o","h","l","c","ma7","ma20","ma60","ma100","ma200","body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret5","ret10","pos20","support_break","oversold_risk","dist_ma7_atr","dist_ma20_atr","dist_ma60_atr"]
    d=pd.read_parquet(a.daily,columns=dcols);d.code=d.code.astype(str).str.zfill(4)
    avoid=d.merge(m,on=["code","ymd"],validate="one_to_one");avoid=avoid[(avoid.ymd//10000).between(2020,2025)&~avoid.code.isin(excluded)&avoid.new_entry_blocked_reason.notna()&avoid.c.lt(avoid.o)&avoid.body_ratio.ge(.45)&avoid.close_pos.le(.35)].copy();avoid["bucket"]="TEMPTING_AVOID";avoid["model_action"]="AVOID";avoid["reason"]=avoid.new_entry_blocked_reason;avoid["monthly_state"]=avoid.monthly_selection_state
    used=set(); top_quota={"PROBE":8} if a.remaining_profile else {"CORE":4,"PROBE":4}; ret_quota={"REENTRY_PROBE":4,"PROBE":4}
    parts=[
        choose_actions(top,"TOP_FAILURE",top_quota,used),
        choose_actions(ret,"RETURN_SELL",ret_quota,used),
        choose_years(br,"BREAKDOWN_REJECTED",a.per_bucket,used),
        choose_years(avoid,"TEMPTING_AVOID",a.per_bucket,used),
    ]
    keys=pd.concat(parts,ignore_index=True);keys["display_hash"]=[hashlib.sha256(f"{SEED}|DISPLAY|{c}|{y}".encode()).hexdigest() for c,y in zip(keys.code,keys.ymd)];keys=keys.sort_values("display_hash").reset_index(drop=True);keys.insert(0,"case_id",[f"C{i:02d}" for i in range(1,len(keys)+1)])
    sealed=keys[["case_id","code","ymd","bucket","model_action","reason","monthly_state","event_hash","selection_hash","display_hash"]].copy();sealed["source_family"]=sealed.bucket;sealed["contract_version"]="complement_v1";sealed["outcome_joined"]=False
    ctx=d.merge(m,on=["code","ymd"],validate="one_to_one");board=keys[["case_id","code","ymd","display_hash"]].merge(ctx,on=["code","ymd"],how="left",validate="one_to_one");board=board.drop(columns=["monthly_selection_state","new_entry_blocked_reason"]);board["chart_cutoff_ymd"]=board.ymd;board["max_daily_ymd_used"]=board.ymd;board["outcome_joined"]=False
    for col in ["new_entry_decision","existing_short_management","entry_stage","confidence","reason_codes","reviewer_note","reviewed_at"]: board[col]=None
    forbidden=[c for c in board if c.startswith(("ret_close_","down_exc_","up_exc_","weekly_"))]
    gates={"rows_32":len(board)==32,"unique_codes_32":board.code.nunique()==32,"excluded_hits_0":int(board.code.isin(excluded).sum())==0,"future_columns_0":not forbidden,"outcome_joined_false":not bool(board.outcome_joined.any()),"sell_16":int(sealed.model_action.ne("AVOID").sum())==16,"avoid_16":int(sealed.model_action.eq("AVOID").sum())==16,"reentry_4":int(sealed.model_action.eq("REENTRY_PROBE").sum())==4}
    if a.remaining_profile:gates["probe_12"]=int(sealed.model_action.eq("PROBE").sum())==12
    else:gates.update({"core_4":int(sealed.model_action.eq("CORE").sum())==4,"probe_8":int(sealed.model_action.eq("PROBE").sum())==8})
    if not all(gates.values()): raise RuntimeError(gates)
    board.to_parquet(a.output/"blind_review_board.parquet",index=False);board.to_csv(a.output/"blind_review_board.csv",index=False,encoding="utf-8-sig");sealed.to_parquet(a.sealed_output/"machine_annotation_sealed.parquet",index=False)
    result={"schema_version":"tradex_blind_complement_review_sample_v1.compare.v1","artifact_role":"authoritative_outcome_blind_complement_sample","review_only":True,"status":"frozen_pending_human_direction_review","fixed_conditions":{"years":list(range(2020,2026)),"seed":SEED,"excluded_code_count":len(excluded),"one_case_per_code":True,"selection_uses_future_outcomes":False,"weekly_inputs":[],"human_required_fields":["new_entry_decision"]},"bucket_counts":{str(k):int(v) for k,v in sealed.bucket.value_counts().items()},"action_counts":{str(k):int(v) for k,v in sealed.model_action.value_counts().items()},"gates":gates,"not_changed":["model rules","MeeMee","ranking","runtime DB"]}
    cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    audit={"board_sha256":sha(a.output/"blind_review_board.parquet"),"sealed_sha256":sha(a.sealed_output/"machine_annotation_sealed.parquet"),"exclusion_sources":exclusion_sources,"base_excluded":sorted(BASE_EXCLUDED),"forbidden_columns":forbidden,"outcome_columns_present":False,"gates":gates,"source_paths":{"actions":str(Path(a.actions).resolve()),"breakdown":str(Path(a.breakdown).resolve()),"monthly_state":str(Path(a.monthly_state).resolve()),"daily":str(Path(a.daily).resolve())}}
    (a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");(a.sealed_output/"seal_audit.json").write_text(json.dumps({"reviewer_bundle":str(a.output.resolve()),"sealed_sha256":audit["sealed_sha256"],"outcome_joined":False},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(a.output),"sealed":str(a.sealed_output),"actions":result["action_counts"],"buckets":result["bucket_counts"],"gates":gates},ensure_ascii=False,indent=2))

if __name__=="__main__": main()
