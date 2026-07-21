"""Build PIT daily role states for MA60/100/200 and rolling support20."""
from __future__ import annotations
import argparse,hashlib,json
from pathlib import Path
import numpy as np
import pandas as pd

LEVELS=("ma60","ma100","ma200","support20")
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument("--features",type=Path,required=True);p.add_argument("--output",type=Path,required=True);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
 x=pd.read_parquet(a.features).sort_values(["code","ymd"]).reset_index(drop=True);x["code"]=x.code.astype(str).str.zfill(4);grp=x.groupby("code",sort=False);pc=grp.c.shift(1)
 parts=[]
 for level in LEVELS:
  lv=x[level];plv=grp[level].shift(1);tol=.15*x.atr14
  reclaim=(pc<plv)&(x.c>=lv)&((x.close_pos>=.55)|(x.lower_wick_ratio>=.40))
  touch=(x.l<=lv+tol)&(x.c>=lv)
  decisive=(pc>=plv)&(x.c<lv-tol)&(x.close_pos<=.35)
  retest=(pc<plv)&(x.h>=lv-tol)&(x.c<lv)&((x.upper_wick_ratio>=.30)|(x.close_pos<=.35))
  near=(x.c>=lv)&((x.c-lv)/x.atr14<=.35)&(x.l>lv+tol)
  state=np.select([reclaim,touch,decisive,retest,near,x.c>=lv],["BREAK_AND_RECLAIM","TOUCH_AND_HOLD","DECISIVE_BREAK","RETEST_FROM_BELOW","UNTOUCHED_NEAR_SUPPORT","ABOVE_FAR"],default="BELOW_UNRESOLVED")
  part=pd.DataFrame({"code":x.code,"ymd":x.ymd.astype(int),"level_type":level.upper(),"level":lv,"distance_close_atr":(x.c-lv)/x.atr14,"state":state,"touch_count_prior20":0})
  # Count prior contacts only; current day is excluded from the maturity count.
  contact=(x.l<=lv+tol)&(x.h>=lv-tol)
  part["touch_count_prior20"]=contact.groupby(x.code).transform(lambda s:s.shift(1).rolling(20,min_periods=1).sum()).fillna(0).astype(int)
  parts.append(part)
 ledger=pd.concat(parts,ignore_index=True);counts=ledger.state.value_counts().to_dict()
 specs={"2802":20240216,"6532":20230704,"6702":20250311,"6526":20251014};anchors={}
 for code,ymd in specs.items():
  z=ledger[(ledger.code==code)&ledger.ymd.eq(ymd)];anchors[code]={"ymd":ymd,"available":not z.empty,"rows":z.where(pd.notna(z),None).to_dict("records")}
 data={"schema_version":"tradex_daily_support_role_state_ledger_v1.compare.v1","artifact_role":"authoritative_diagnostic","review_only":True,"state_contract":{"tolerance":"0.15ATR","BREAK_AND_RECLAIM":"prior close below prior level; current close at/above level; close_pos>=0.55 or lower_wick>=0.40","TOUCH_AND_HOLD":"intraday low reaches level+0.15ATR and close holds at/above level","DECISIVE_BREAK":"prior close at/above prior level; current close below level-0.15ATR with close_pos<=0.35","RETEST_FROM_BELOW":"prior close below prior level; high reaches level-0.15ATR; close remains below with upper rejection","UNTOUCHED_NEAR_SUPPORT":"close 0 to 0.35ATR above level without touch","ABOVE_FAR":"close above and farther than near-support state","BELOW_UNRESOLVED":"below level without a fresh decisive break or failed retest","touch_count_prior20":"prior trading days only"},"state_counts":counts,"human_anchor_diagnostics":anchors,"judgment":{"decision":"hold","infrastructure_pass":all(v["available"] for v in anchors.values()),"reason":"role-state ledger must be joined to action-specific entry/add/exit episodes before effectiveness judgment"},"not_changed":["support20 calculation","MA values","sequence paths","entry lifecycle","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json";cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");ledger.to_parquet(a.output/"daily_support_role_state.parquet",index=False);audit={"feature_rows":int(len(x)),"state_rows":int(len(ledger)),"expected_rows":int(len(x)*len(LEVELS)),"duplicates":int(ledger.duplicated(["code","ymd","level_type"]).sum()),"missing_level":int(ledger.level.isna().sum()),"future_used_for_state":False,"review_only":True,"feature_sha256":sha(a.features)};(a.output/"audit.json").write_text(json.dumps(audit,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");(a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n",encoding="utf-8");print(json.dumps({"output":str(a.output),"state_counts":counts,"anchors":anchors,"judgment":data["judgment"],"audit":audit},ensure_ascii=False,indent=2))
if __name__=="__main__":main()
