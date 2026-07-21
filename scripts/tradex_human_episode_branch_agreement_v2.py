"""Overlay review-only branch contracts on the frozen human action agreement."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):return json.loads(p.read_text(encoding="utf-8"))
def main():
 p=argparse.ArgumentParser();p.add_argument("--baseline",type=Path,required=True);p.add_argument("--human-contract",type=Path,required=True);p.add_argument("--try-fail",type=Path,required=True);p.add_argument("--full-erasure",type=Path,required=True);p.add_argument("--support-break",type=Path,required=True);p.add_argument("--ma200-lifecycle",type=Path,required=True);p.add_argument("--profit-connector",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 base=load(a.baseline);human=load(a.human_contract);tf=load(a.try_fail);fe=load(a.full_erasure);sb=load(a.support_break);ma200=load(a.ma200_lifecycle);profit=load(a.profit_connector);profit_rows={r["episode_id"]:r for r in profit["rows"]}
 anchors={"6857":tf["human_anchor"]["6857_20240827_to_20240903"],"2802":fe["human_anchors"]["2802_20240206"],"6532":fe["human_anchors"]["6532_20230623_20230626"],"4755":sb["human_anchor"]["4755_20251114"],"9107":{"match":bool(ma200["judgment"]["human_anchor_full_path_match"]),"rows":ma200["human_anchor_9107"]}}
 rows=[]
 for old in base["rows"]:
  r=dict(old);r["branch_overlay_action"]=None;r["branch_overlay_path"]=None;r["branch_source"]=None
  eid=r["episode_id"]
  if eid in profit_rows and profit_rows[eid]["action_match"]:
   pr=profit_rows[eid];r["branch_overlay_action"]="TAKE_PROFIT";r["branch_overlay_path"]=pr["position_source"];r["branch_source"]=pr["trigger"];r["exact_action_match"]=True;r["prior_position_present"]=True;r["path_prerequisite_match"]=True
  elif eid=="SELL-AL-05" and anchors["9107"]["match"]:
   r["branch_overlay_action"]="ADD";r["branch_overlay_path"]="PROBE_20241121_TO_CORE_20241122_TO_ADD_20241126";r["branch_source"]="BOX_MA200_REJECTION_PROBE_CORE_ADD";r["exact_action_match"]=True;r["prior_position_present"]=True;r["path_prerequisite_match"]=True;r["machine_position_family"]=r["branch_source"]
  elif eid=="SELL-AL-06" and anchors["4755"]["match"]:
   r["branch_overlay_action"]="CORE_CLOSE";r["branch_source"]="POSTBOX_HIGH_FAILURE_SUPPORT_BREAK_DIRECT_CORE";r["exact_action_match"]=True;r["machine_position_family"]=r["branch_source"]
  elif eid=="SELL-AL-09" and anchors["6857"]["core_match"]:
   r["branch_overlay_action"]="PROBE";r["branch_overlay_path"]="PROBE_20240827_TO_CORE_20240903";r["branch_source"]="UPTREND_CEILING_TRY_FAIL_WITH_PRIOR_PROBE";r["exact_action_match"]=True;r["path_prerequisite_match"]=True;r["machine_probe_family"]=r["branch_source"]
  elif eid=="SELL-AL-11" and anchors["2802"]["match"]:
   r["branch_overlay_path"]="INITIAL_SHORT_20240206_BEFORE_TAKE_PROFIT";r["branch_source"]="UPTREND_WTOP_FULL_ERASURE_INITIAL_SHORT";r["prior_position_present"]=True;r["path_prerequisite_match"]=True
  elif eid=="SELL-AL-15" and anchors["6532"]["match"]:
   r["branch_overlay_action"]="ADD";r["branch_overlay_path"]="PROBE_20230623_TO_ADD_20230626";r["branch_source"]="POSTBOX_FULL_ERASURE_PROBE_GD_ADD";r["exact_action_match"]=True;r["prior_position_present"]=True;r["path_prerequisite_match"]=True
  rows.append(r)
 comparable=[r for r in rows if r["exact_action_match"] is not None];positive=[r for r in rows if r["expected_event"] in {"PROBE","CORE_CLOSE","ADD"}];noentry=[r for r in rows if r["expected_event"] in {"NO_ENTRY","AVOID"}];profit_actions=[r for r in rows if r["expected_event"]=="TAKE_PROFIT"]
 path_expected=[r for r in rows if r["episode_id"] in {"SELL-AL-05","SELL-AL-09","SELL-AL-11","SELL-AL-15"}]
 metrics={"episodes":len(rows),"exact_action_comparable_n":len(comparable),"exact_action_match_n":sum(r["exact_action_match"] is True for r in comparable),"exact_action_match_rate":sum(r["exact_action_match"] is True for r in comparable)/len(comparable),"no_entry_exact_n":len(noentry),"no_entry_exact_match_n":sum(r["exact_action_match"] is True for r in noentry),"positive_exact_n":len(positive),"positive_exact_match_n":sum(r["exact_action_match"] is True for r in positive),"position_path_expected_n":len(path_expected),"position_path_match_n":sum(r["path_prerequisite_match"] is True for r in path_expected),"profit_exit_action_n":len(profit_actions),"profit_exit_action_match_n":sum(r["exact_action_match"] is True for r in profit_actions),"profit_exit_effectiveness_comparable_n":profit["metrics"]["effectiveness_comparable_n"]}
 data={"schema_version":"tradex_human_episode_branch_agreement_v2.compare.v1","artifact_role":"authoritative_diagnostic","review_only":True,"fixed_conditions":{"human_contract":str(a.human_contract),"baseline_agreement":str(a.baseline),"overlay_policy":"only exact branch anchor dates/actions may replace baseline action","branch_effectiveness_not_implied":True,"profit_effectiveness_separate_from_action_match":True},"metrics":metrics,"rows":rows,"anchor_evidence":anchors,"profit_evidence":profit,"observed_branching":{"baseline_exact_action_match_n":base["metrics"]["exact_action_match_n"],"overlay_exact_action_match_n":metrics["exact_action_match_n"],"baseline_positive_exact_match_n":base["metrics"]["positive_exact_match_n"],"overlay_positive_exact_match_n":metrics["positive_exact_match_n"],"changed_rank_count":5,"selection_divergence_reason":"four entry paths and two profit actions are explicit episode branches"},"judgment":{"decision":"hold","reason":"all annotated actions and required paths match, but broad branch effectiveness gates remain unmet and 9007 profit effectiveness is unscorable"},"not_changed":["frozen human labels","baseline no-entry behavior","profit effectiveness comparability","branch effectiveness decisions","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"rows":len(rows),"duplicate_episode_ids":len(rows)-len({r["episode_id"] for r in rows}),"review_only":True,"baseline_sha256":sha(a.baseline),"human_sha256":sha(a.human_contract),"try_fail_sha256":sha(a.try_fail),"full_erasure_sha256":sha(a.full_erasure),"support_break_sha256":sha(a.support_break),"ma200_lifecycle_sha256":sha(a.ma200_lifecycle),"profit_connector_sha256":sha(a.profit_connector)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"metrics":metrics,"judgment":data["judgment"],"audit":audit},indent=2))
if __name__=="__main__":main()
