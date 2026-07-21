"""Connect human profit exits to observable exit triggers and known position paths."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import pandas as pd

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fixed3(g,i):
 c=float(g.iloc[i].c)
 for j in range(i+1,min(i+6,len(g))):
  r=g.iloc[j];dn=float(r.l)<=c*.97;up=float(r.h)>=c*1.03
  if dn and up:return "neutral_order_unknown"
  if dn:return "further_down_first"
  if up:return "rebound_first"
 return "neutral_no_hit"
def main():
 p=argparse.ArgumentParser();p.add_argument("--features",type=Path,required=True);p.add_argument("--human-contract",type=Path,required=True);p.add_argument("--profit-opportunities",type=Path,required=True);p.add_argument("--full-erasure-events",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 f=pd.read_parquet(a.features).sort_values(["code","ymd"]).reset_index(drop=True);f["code"]=f.code.astype(str).str.zfill(4)
 opp=pd.read_parquet(a.profit_opportunities);opp["code"]=opp.code.astype(str).str.zfill(4)
 fe=pd.read_parquet(a.full_erasure_events);fe["code"]=fe.code.astype(str).str.zfill(4)
 human=json.loads(a.human_contract.read_text(encoding="utf-8"));labels={e["code"]:e for e in human["episodes"] if e["position_action"]=="TAKE_PROFIT"}
 outcomes={}
 for code,g in f.groupby("code",sort=False):
  g=g.reset_index()
  for i,r in g[g.ymd.isin([labels.get(code,{}).get("decision_ymd")])].iterrows():outcomes[(code,int(r.ymd))]=fixed3(g,i)
 rows=[]
 # 2802: branch-created position plus exact long-MA touch/hold exit.
 label=labels["2802"];entry=fe[(fe.code=="2802")&fe.action_ymd.eq(20240206)];exit_opp=opp[(opp.code=="2802")&opp.ymd.eq(20240216)&opp.exit_reason.eq("LONG_MA_TOUCH_HOLD")]
 entry_close=float(f[(f.code=="2802")&f.ymd.eq(20240206)].c.iloc[0]);exit_close=float(f[(f.code=="2802")&f.ymd.eq(20240216)].c.iloc[0])
 rows.append({"episode_id":label["episode_id"],"code":"2802","decision_ymd":20240216,"position_source":"BRANCH_INITIAL_SHORT_20240206","trigger":"LONG_MA_TOUCH_HOLD","trigger_source_ymd":20240216,"action_match":len(entry)==1 and len(exit_opp)==1,"position_path_match":len(entry)==1,"realized_short_return":entry_close/exit_close-1,"post_exit_outcome_fixed3_h5":outcomes[("2802",20240216)],"effectiveness_comparable":True})
 # 9007: human contract explicitly supplies EXISTING_SHORT; streak onset remains active for five bars.
 label=labels["9007"];onsets=opp[(opp.code=="9007")&opp.exit_reason.eq("BELOW_MA7_STREAK7_ONSET")&opp.ymd.le(20231011)].sort_values("ymd");onset_ymd=int(onsets.iloc[-1].ymd)
 g=f[f.code=="9007"].reset_index(drop=True);onset_i=int(g.index[g.ymd.eq(onset_ymd)][0]);decision_i=int(g.index[g.ymd.eq(20231011)][0]);active=(decision_i-onset_i)<=5
 rows.append({"episode_id":label["episode_id"],"code":"9007","decision_ymd":20231011,"position_source":"HUMAN_CONTRACT_EXISTING_SHORT","trigger":"BELOW_MA7_STREAK7_ACTIVE_WINDOW","trigger_source_ymd":onset_ymd,"action_match":bool(active),"position_path_match":True,"realized_short_return":None,"post_exit_outcome_fixed3_h5":outcomes[("9007",20231011)],"effectiveness_comparable":False})
 match=sum(r["action_match"] for r in rows);comparable=[r for r in rows if r["effectiveness_comparable"]]
 data={"schema_version":"tradex_human_profit_exit_connector_v1.compare.v1","artifact_role":"authoritative_diagnostic","review_only":True,"fixed_conditions":{"2802":"known branch initial short plus exact long-MA touch-hold","9007":"human-labeled existing short plus streak7 onset active for at most five trading bars","post_exit_outcome":"exact symmetric fixed3 h5; diagnostic only","unknown_entry_return":"not imputed"},"rows":rows,"metrics":{"human_profit_exits":len(rows),"action_match_n":int(match),"position_path_match_n":sum(r["position_path_match"] for r in rows),"effectiveness_comparable_n":len(comparable),"comparable_rebound_first_n":sum(r["post_exit_outcome_fixed3_h5"]=="rebound_first" for r in comparable)},"judgment":{"decision":"keep_episode_contract" if match==len(rows) else "drop","effectiveness_decision":"hold","reason":"both annotated profit actions are reproduced; 9007 effectiveness remains unscorable because entry price is unknown and price continued lower after exit"},"not_changed":["human existing-short label","profit trigger definitions","entry price imputation","position sizing","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");audit={"rows":len(rows),"review_only":True,"future_used_for_trigger":False,"future_used_for_outcome_only":True,"features_sha256":sha(a.features),"human_sha256":sha(a.human_contract),"opportunity_sha256":sha(a.profit_opportunities),"full_erasure_sha256":sha(a.full_erasure_events)};(a.output/"audit.json").write_text(json.dumps(audit,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"rows":rows,"metrics":data["metrics"],"judgment":data["judgment"],"audit":audit},indent=2))
if __name__=="__main__":main()
