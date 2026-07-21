"""Freeze an outcome-blind 32-event board for a monthly-environment short gate."""
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

SEED = "monthly-environment-gate-unused-events-v1"
SELL = {"PROBE", "CORE", "ADD", "REENTRY_PROBE"}

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def h(*parts): return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()

def choose(pool, label, n, used_codes):
    x = pool[~pool.code.isin(used_codes)].copy()
    x["event_hash"] = [h(SEED, label, "EVENT", c, y) for c, y in zip(x.code, x.ymd)]
    x = x.sort_values(["event_hash", "code", "ymd"]).drop_duplicates("code")
    x["selection_hash"] = [h(SEED, label, "CODE", c) for c in x.code]
    out = x.sort_values(["selection_hash", "code", "ymd"]).head(n).copy()
    if len(out) != n: raise RuntimeError(f"{label} supply {len(out)}/{n}")
    used_codes.update(out.code)
    return out

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--actions",type=Path,required=True);ap.add_argument("--monthly-state",type=Path,required=True);ap.add_argument("--daily",type=Path,required=True)
    ap.add_argument("--exclude-events",type=Path,action="append",default=[])
    ap.add_argument("--exclude-codes",type=Path,action="append",default=[])
    ap.add_argument("--output",type=Path,required=True);ap.add_argument("--sealed-output",type=Path,required=True)
    a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False);a.sealed_output.mkdir(parents=True,exist_ok=False)
    excluded=set();excluded_codes=set();sources=[]
    for p in a.exclude_events:
        d=pd.read_parquet(p); keys=set(zip(d.code.astype(str).str.zfill(4),d.ymd.astype(int)))
        excluded|=keys;sources.append({"path":str(p.resolve()),"sha256":sha(p),"event_keys":len(keys)})
    code_sources=[]
    for p in a.exclude_codes:
        d=pd.read_parquet(p); codes=set(d.code.astype(str).str.zfill(4));excluded_codes|=codes
        code_sources.append({"path":str(p.resolve()),"sha256":sha(p),"codes":len(codes)})
    actions=pd.read_parquet(a.actions);actions.code=actions.code.astype(str).str.zfill(4);actions.ymd=actions.ymd.astype(int)
    actions=actions[(actions.ymd//10000).between(2020,2025)&actions.action.isin(SELL)&~actions.code.isin(excluded_codes)].copy()
    actions=actions[[ (c,int(y)) not in excluded for c,y in zip(actions.code,actions.ymd) ]]
    actions["model_action"]=actions.action;used=set()
    parts=[choose(actions[actions.monthly_state.eq("HIGH_ZONE_FAILURE")],"BLOCKED_HIGH_ZONE_FAILURE",16,used),
           choose(actions[actions.monthly_state.eq("MATURE_BOX_UPPER")],"ALLOWED_MATURE_BOX_UPPER",8,used),
           choose(actions[actions.monthly_state.eq("POST_BOX_RETURN_SELL")],"ALLOWED_POST_BOX_RETURN_SELL",8,used)]
    keys=pd.concat(parts,ignore_index=True);keys["gate_expected"] = keys.monthly_state.ne("HIGH_ZONE_FAILURE")
    keys["bucket"] = keys.monthly_state.map({"HIGH_ZONE_FAILURE":"BLOCKED_HIGH_ZONE_FAILURE","MATURE_BOX_UPPER":"ALLOWED_MATURE_BOX_UPPER","POST_BOX_RETURN_SELL":"ALLOWED_POST_BOX_RETURN_SELL"})
    keys["display_hash"]=[h(SEED,"DISPLAY",c,y) for c,y in zip(keys.code,keys.ymd)];keys=keys.sort_values("display_hash").reset_index(drop=True);keys.insert(0,"case_id",[f"E{i:02d}" for i in range(1,33)])
    sealed_cols=["case_id","code","ymd","bucket","model_action","reason","monthly_state","gate_expected","event_hash","selection_hash","display_hash"]
    sealed=keys[sealed_cols].copy();sealed["contract_version"]="monthly_environment_gate_v1";sealed["outcome_joined"]=False
    mcols=["code","ymd","source_month","effective_month","base_regime","confirmed_environment","monthly_selection_state","selection_age_sessions","current_local_box_position","current_month_body_pct","current_month_close_position"]
    dcols=["code","ymd","o","h","l","c","ma7","ma20","ma60","ma100","ma200","body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret5","ret10","pos20","support_break","oversold_risk","dist_ma7_atr","dist_ma20_atr","dist_ma60_atr"]
    m=pd.read_parquet(a.monthly_state,columns=mcols);m.code=m.code.astype(str).str.zfill(4);d=pd.read_parquet(a.daily,columns=dcols);d.code=d.code.astype(str).str.zfill(4)
    ctx=d.merge(m,on=["code","ymd"],validate="one_to_one");board=keys[["case_id","code","ymd","display_hash"]].merge(ctx,on=["code","ymd"],how="left",validate="one_to_one")
    board=board.drop(columns=["monthly_selection_state"]);board["chart_cutoff_ymd"]=board.ymd;board["max_daily_ymd_used"]=board.ymd;board["outcome_joined"]=False
    for c in ["new_entry_decision","existing_short_management","entry_stage","confidence","reason_codes","reviewer_note","reviewed_at"]:board[c]=None
    forbidden=[c for c in board if c.startswith(("ret_close_","down_exc_","up_exc_","weekly_"))]
    gates={"rows_32":len(board)==32,"unique_codes_32":board.code.nunique()==32,"unique_events_32":board[["code","ymd"]].drop_duplicates().shape[0]==32,"excluded_event_hits_0":not any((c,int(y)) in excluded for c,y in zip(board.code,board.ymd)),"excluded_code_hits_0":not bool(set(board.code)&excluded_codes),"blocked_hzf_16":int(sealed.monthly_state.eq("HIGH_ZONE_FAILURE").sum())==16,"allowed_16":int(sealed.gate_expected.sum())==16,"all_model_sell":bool(sealed.model_action.isin(SELL).all()),"future_columns_0":not forbidden,"outcome_joined_false":not bool(board.outcome_joined.any()),"context_complete":not board[["o","h","l","c","base_regime","confirmed_environment"]].isna().any().any()}
    if not all(gates.values()):raise RuntimeError(gates)
    board.to_parquet(a.output/"blind_review_board.parquet",index=False);board.to_csv(a.output/"blind_review_board.csv",index=False,encoding="utf-8-sig");sealed.to_parquet(a.sealed_output/"machine_annotation_sealed.parquet",index=False)
    result={"schema_version":"tradex_blind_monthly_environment_gate_sample_v1.compare.v1","artifact_role":"authoritative_outcome_blind_monthly_environment_gate_sample","review_only":True,"status":"frozen_pending_human_direction_review","fixed_conditions":{"years":list(range(2020,2026)),"seed":SEED,"one_case_per_code":True,"selection_uses_future_outcomes":False,"weekly_inputs":[],"fixed_gate":"block HIGH_ZONE_FAILURE; allow other monthly states","human_required_fields":["new_entry_decision"]},"bucket_counts":{str(k):int(v) for k,v in sealed.bucket.value_counts().items()},"action_counts":{str(k):int(v) for k,v in sealed.model_action.value_counts().items()},"gates":gates,"not_changed":["model rules","MeeMee","ranking","runtime DB"]}
    cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    audit={"sources":{"actions":{"path":str(a.actions.resolve()),"sha256":sha(a.actions)},"monthly_state":{"path":str(a.monthly_state.resolve()),"sha256":sha(a.monthly_state)},"daily":{"path":str(a.daily.resolve()),"sha256":sha(a.daily)}},"event_exclusion_sources":sources,"code_exclusion_sources":code_sources,"excluded_code_count":len(excluded_codes),"year_counts":{str(k):int(v) for k,v in sealed.assign(year=sealed.ymd//10000).year.value_counts().sort_index().items()},"environment_action_counts":{f"{e}:{act}":int(len(g)) for (e,act),g in sealed.groupby(["monthly_state","model_action"])},"sampling_note":"case-control sample enriched to 50% HZF; aggregate PF is not a natural-frequency operational estimate","forbidden_columns":forbidden,"gates":gates,"board_sha256":sha(a.output/"blind_review_board.parquet"),"sealed_sha256":sha(a.sealed_output/"machine_annotation_sealed.parquet")}
    (a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");(a.sealed_output/"seal_audit.json").write_text(json.dumps({"reviewer_bundle":str(a.output.resolve()),"sealed_sha256":audit["sealed_sha256"],"outcome_joined":False},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(a.output),"sealed":str(a.sealed_output),"buckets":result["bucket_counts"],"actions":result["action_counts"],"gates":gates},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
