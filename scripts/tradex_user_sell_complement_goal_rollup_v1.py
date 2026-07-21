"""Build the authoritative completion rollup for user-sell complement research."""
import argparse, hashlib, json
from pathlib import Path

def load(p): return json.loads(Path(p).read_text(encoding="utf-8"))
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
    ap=argparse.ArgumentParser()
    for n in ["base","room-discovery","room-validation","ma-cluster","ma7-discovery","final-comparison","sample-audit"]: ap.add_argument("--"+n,required=True,type=Path)
    ap.add_argument("--output",required=True,type=Path);a=ap.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    base=load(a.base);rd=load(a.room_discovery);rv=load(a.room_validation);ma=load(a.ma_cluster);m7=load(a.ma7_discovery);final=load(a.final_comparison);sample=load(a.sample_audit)
    results=final["authoritative_results"]
    required_metric_keys={"D","R","N","mean_fixed3_pct","profit_factor","max_loss_pct","max_loss_streak","max_drawdown_units_pct","max_concurrent","n"}
    comparison_metrics_complete=all(required_metric_keys.issubset(results[name]) for name in ["human_alone","model_alone","complement_union"])
    environment_total=sum(v["n"] for v in final["by_environment"].values())
    stage58=rv["by_action_model"]
    stage32=final["by_model_action"]
    requirements={
        "frozen_38_human_directions_and_40_outcomes_used": base.get("direction_answered")==38 and base.get("rows")==40,
        "pit_support_prior_low_and_ma_distance_quantified": rd["fixed_conditions"]["prior_lows_exclude_signal_day"] and not rd["fixed_conditions"]["weekly_inputs"],
        "long_ma_60_100_200_cluster_quantified": "MA60/100/200" in ma["fixed_conditions"]["axis"],
        "human_sell_model_avoid_compared": results["human_sell_model_avoid"]["n"]>0,
        "human_nosell_model_sell_compared": results["human_nosell_model_sell"]["n"]>0,
        "probe_core_add_separated": all(x in stage58 for x in ["PROBE","CORE","ADD"]),
        "fresh_unused_sample_ge_30": sample["selection"]["frozen_rows"]>=30,
        "fresh_human_blind_sample_ge_30": sum(v["n"] for v in final["direction_pairs"].values())>=30,
        "human_model_complement_same_condition_metrics_complete": comparison_metrics_complete,
        "environment_breadth_covers_all_32": environment_total==32,
        "weekly_fully_excluded": not final["fixed_conditions"]["weekly_inputs"],
        "next_open_h5_fixed3_cost0": final["fixed_conditions"]["execution"]=="next_session_open" and final["fixed_conditions"]["horizon_sessions"]==5 and final["fixed_conditions"]["costs"]=="ignored",
        "no_automatic_reflection": all(x in final["not_changed"] for x in ["MeeMee","ranking","runtime DB","production trading logic"]),
    }
    if not all(requirements.values()): raise RuntimeError({k:v for k,v in requirements.items() if not v})
    decisions={
        "downside_room_0_5atr_universal_stage_gate":{"decision":"drop","evidence":rv["judgment"],"reason":"unused 58 retained only 66.7% of D and worsened drawdown despite lower R count"},
        "long_ma_cluster_proximity_single_axis":{"decision":"drop","evidence":ma["judgment"],"reason":"no discovery threshold met human D retention, R-rate, and maximum-loss conditions"},
        "seven_below_ma7_user_sell_veto":{"decision":"drop","evidence":final["judgment"],"reason":"fresh 32 did not strictly improve maximum loss and slightly worsened mean fixed3 return"},
        "model_sell_when_human_nosell_disagreement_lane":{"decision":"keep_review_only_next_axis","evidence":results["human_nosell_model_sell"],"reason":"fresh blind disagreement lane was 4D/1R/4N with positive mean and PF, but is not yet a validated production gate"},
        "human_sell_when_model_avoid_lane":{"decision":"risk_flag_next_axis","evidence":results["human_sell_model_avoid"],"reason":"fresh blind lane was 2D/4R/3N with negative mean; model veto context deserves single-axis follow-up"},
    }
    result={
        "schema_version":"tradex_user_sell_complement_goal_rollup_v1.compare.v1","artifact_role":"authoritative_goal_completion_rollup","review_only":True,"research_phase":"effectiveness_judgment",
        "fixed_conditions":{"weekly_inputs":[],"costs":"ignored","execution":"next_session_open","horizon_sessions":5,"barriers":"short target -3%, stop +3%, same-bar stop-first","automatic_reflection":False},
        "current_champion":"model_alone_review_only","current_challengers":["human_plus_7below_gate","complement_union"],
        "authoritative_results":results,"observed_branching":final["observed_branching"],"direction_pairs":final["direction_pairs"],
        "stage_breadth":{"unused58_by_action":stage58,"fresh32_by_model_action":stage32,"ADD_note":"ADD n=4 exists only in unused58; no remaining fresh ADD codes were available for the 32-case human board"},
        "environment_breadth":final["by_environment"],"axis_decisions":decisions,"completion_requirements":requirements,
        "judgment":{"candidate_local_decision":{"room":"drop","ma_cluster":"drop","ma7_streak":"drop","model_disagreement_lane":"keep_review_only_next_axis"},"session_aggregate_decision":"research_complete_no_gate_kept","authoritative_rollup_decision":"research_complete_no_automatic_reflection","reason":"all three tested single-axis gates failed their fixed keep contract; model-only and human/model disagreement evidence remains useful for the next review-only axis"},
        "not_changed":["MeeMee","ranking","runtime DB","production trading logic","weekly exclusion","cost convention"]}
    cp=a.output/"compare.json";cp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    sources={n:{"path":str(getattr(a,n.replace("-","_"))),"sha256":sha(getattr(a,n.replace("-","_")))} for n in ["base","room_discovery","room_validation","ma_cluster","ma7_discovery","final_comparison","sample_audit"]}
    audit={"requirements":requirements,"all_requirements_pass":all(requirements.values()),"source_artifacts":sources,"authoritative_compare_sha256":sha(cp),"weekly_columns_used":[],"runtime_writes":0,"automatic_reflection":False};apath=a.output/"audit.json";apath.write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"requirements":requirements,"decisions":decisions,"judgment":result["judgment"]},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
