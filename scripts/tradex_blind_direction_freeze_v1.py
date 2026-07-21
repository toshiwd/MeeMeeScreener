"""Validate and freeze direction-only human annotations without outcomes or model labels."""
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

VALID={"SELL","WAIT","AVOID"}
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--board",type=Path,required=True);ap.add_argument("--annotations",type=Path,required=True);ap.add_argument("--output",type=Path,required=True);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    board=pd.read_parquet(a.board); payload=json.loads(a.annotations.read_text(encoding="utf-8")); ann=pd.DataFrame(payload.get("annotations",[]))
    required={"case_id","code","ymd","new_entry_decision"}; missing=required-set(ann.columns)
    if missing: raise RuntimeError(f"annotation columns missing: {sorted(missing)}")
    ann.code=ann.code.astype(str).str.zfill(4);board.code=board.code.astype(str).str.zfill(4);ann.ymd=ann.ymd.astype(int)
    if ann.case_id.duplicated().any(): raise RuntimeError("duplicate case_id")
    joined=board[["case_id","code","ymd"]].merge(ann,on=["case_id","code","ymd"],how="left",validate="one_to_one")
    invalid=joined[~joined.new_entry_decision.isin(VALID)]
    if len(invalid): raise RuntimeError(f"incomplete/invalid direction cases: {invalid.case_id.tolist()}")
    if len(joined)!=len(board) or len(ann)!=len(board): raise RuntimeError("annotation key parity failed")
    joined["human_direction"]=joined.new_entry_decision.map({"SELL":"SELL","WAIT":"NO_SELL","AVOID":"NO_SELL"})
    ledger=a.output/"human_direction_frozen.parquet";joined.to_parquet(ledger,index=False)
    result={"schema_version":"tradex_blind_direction_freeze_v1.compare.v1","artifact_role":"authoritative_outcome_free_human_direction_freeze","review_only":True,"status":"frozen_before_model_label_and_outcome_join","rows":len(joined),"direction_counts":{str(k):int(v) for k,v in joined.human_direction.value_counts().items()},"raw_counts":{str(k):int(v) for k,v in joined.new_entry_decision.value_counts().items()},"fixed_conditions":{"outcomes_loaded":False,"model_labels_loaded":False,"weekly_inputs":[]},"judgment":{"decision":"keep_frozen_pending_reveal"},"not_changed":["model rules","MeeMee","ranking","runtime DB"]}
    cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"board_sha256":sha(a.board),"annotations_sha256":sha(a.annotations),"ledger_sha256":sha(ledger),"key_parity":True,"invalid_count":0,"outcome_columns_present":False,"model_columns_present":False};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),**result},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
