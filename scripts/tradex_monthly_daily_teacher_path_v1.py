"""Join monthly-only selection episodes to PIT daily entry/management states."""
import argparse, hashlib, json
from pathlib import Path
import pandas as pd

PIT = ["code","ymd","o","h","l","c","ma7","ma20","ma60","ma100","ma200",
       "body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret10","pos20",
       "cross_ma7","cross_ma20","support_break"]

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--daily",required=True); ap.add_argument("--monthly-state",required=True); ap.add_argument("--output",type=Path,required=True); a=ap.parse_args(); a.output.mkdir(parents=True,exist_ok=False)
 d=pd.read_parquet(a.daily,columns=PIT); d.code=d.code.astype(str).str.zfill(4); d=d.sort_values(["code","ymd"]).copy()
 d["below7"]=d.c.lt(d.ma7); d["below7_run"]=d.groupby("code").below7.transform(lambda s:s.astype(int).groupby((~s).cumsum()).cumsum())
 mcols=["code","ymd","selection_event","new_entry_eligible","new_entry_blocked_reason","monthly_selection_state","selection_age_sessions","management_state_valid"]
 m=pd.read_parquet(a.monthly_state,columns=mcols); m.code=m.code.astype(str).str.zfill(4)
 z=d.merge(m,on=["code","ymd"],how="left",validate="one_to_one").sort_values(["code","ymd"])
 events=[]
 for code,g in z.groupby("code",sort=False):
  g=g.reset_index(drop=True); episode=None; episode_start=None; exhausted_i=None; reentry_i=None; episode_low=None
  for i,r in g.iterrows():
   event=None; reason=None
   if episode is not None and exhausted_i is None and episode_start is not None and i-episode_start>20:
    episode=None; episode_start=None; reentry_i=None; episode_low=None
   can_start=episode is None or (exhausted_i is None and pd.notna(r.selection_event) and r.selection_event!=episode)
   if (bool(r.new_entry_eligible) if pd.notna(r.new_entry_eligible) else False) and can_start:
    if r.selection_event=="HIGH_ZONE_FAILURE" and r.h>=r.ma200 and r.c<r.ma200 and r.close_pos<=.65:
     event="PROBE"; reason="monthly_high_zone_failure_plus_daily_ma200_failure"; episode=r.selection_event; episode_start=i; exhausted_i=None; reentry_i=None; episode_low=None
    elif r.selection_event=="POST_BOX_RETURN_SELL" and r.c<r.o and r.body_ratio>=.45 and r.cross_ma20==1:
     event="PROBE"; reason="monthly_post_box_plus_daily_ma20_break"; episode=r.selection_event; episode_start=i; exhausted_i=None; reentry_i=None; episode_low=None
    elif r.selection_event=="MATURE_BOX_UPPER" and i>0 and g.iloc[i-1].c>g.iloc[i-1].o and r.c<=g.iloc[i-1].o and r.c<r.o and r.body_ratio>=.45 and r.close_pos<=.35 and r.pos20>=.55:
     if not (episode=="POST_BOX_RETURN_SELL" and exhausted_i is not None):
      event="PROBE"; reason="monthly_box_upper_plus_daily_top_failure"; episode=r.selection_event; episode_start=i; exhausted_i=None; reentry_i=None; episode_low=None
   if episode=="HIGH_ZONE_FAILURE" and i>0 and r.c<g.iloc[i-1].c and r.c<r.ma200 and r.close_pos<=.25:
    event="CORE"; reason="daily_follow_through_below_ma200"; episode=None; episode_start=None
   if episode=="POST_BOX_RETURN_SELL":
    if exhausted_i is None and r.below7_run>=8 and r.pos20<=.10 and r.c>=r.o:
     event="TAKE_PROFIT_FULL_HEDGE"; reason="eight_below_ma7_with_bottoming_positive_candle"; exhausted_i=i; episode_low=float(r.l)
    elif exhausted_i is not None and reentry_i is None:
     episode_low=min(episode_low,float(r.l))
     if i-exhausted_i>15: episode=None; episode_start=None; exhausted_i=None; reentry_i=None; episode_low=None
     elif r.c>=r.ma20 and 100*(r.c/episode_low-1)>=5 and r.c>r.o and r.close_pos>=.60:
      event="REENTRY_PROBE"; reason="rebound_to_ma20_after_exhaustion"; reentry_i=i
    elif reentry_i is not None:
     if i-reentry_i>5: episode=None; episode_start=None; exhausted_i=None; reentry_i=None; episode_low=None
     elif r.c<r.o and r.body_ratio>=.55 and r.close_pos<=.10 and r.cross_ma7==1 and r.cross_ma20==1:
      event="CORE"; reason="return_sell_cross_ma7_ma20"; episode=None; episode_start=None; exhausted_i=None; reentry_i=None; episode_low=None
   if event: events.append({"code":code,"ymd":int(r.ymd),"action":event,"reason":reason,"monthly_state":r.monthly_selection_state})
 e=pd.DataFrame(events)
 teachers={}
 expected=[("9107",20241121,"PROBE"),("9107",20241122,"CORE"),("7733",20260119,"PROBE"),("7733",20260126,"TAKE_PROFIT_FULL_HEDGE"),("7733",20260210,"REENTRY_PROBE"),("7733",20260213,"CORE"),("3405",20260618,"PROBE")]
 for c,y,act in expected: teachers[f"{c}:{y}:{act}"]=bool(((e.code==c)&(e.ymd==y)&(e.action==act)).any())
 avoided={f"{c}:{y}":not bool(((e.code==c)&(e.ymd==y)).any()) for c,y in [("4208",20260514),("7004",20260317),("7004",20260319),("3405",20260630),("9531",20260603)]}
 data={"schema_version":"tradex_monthly_daily_teacher_path_v1.compare.v1","artifact_role":"authoritative_challenger","review_only":True,"research_phase":"comparison_stabilization","fixed_conditions":{"selection":"monthly-only state artifact","entry_and_management":"daily PIT whitelist","weekly_inputs":[],"future_inputs":[],"execution_evaluation":"not_started"},"teacher_expected":teachers,"teacher_avoided":avoided,"observed_branching":{"event_rows":len(e),"codes":int(e.code.nunique()),"action_counts":e.action.value_counts().to_dict()},"judgment":{"decision":"keep_teacher_contract" if all(teachers.values()) and all(avoided.values()) else "hold_teacher_mismatch"},"not_changed":["outcome rules","MeeMee","ranking","runtime DB"]}
 cp=a.output/"compare.json"; cp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); e.to_parquet(a.output/"action_ledger.parquet",index=False)
 (a.output/"audit.json").write_text(json.dumps({"rows":len(e),"duplicates":int(e.duplicated(["code","ymd","action"]).sum()),"daily_columns":PIT,"weekly_columns_used":[],"future_columns_used":[],"daily_sha256":sha(a.daily),"monthly_state_sha256":sha(a.monthly_state)},indent=2)+"\n")
 (a.output/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"authoritative":"compare.json","sha256":sha(cp)},indent=2)+"\n")
 print(json.dumps(data,ensure_ascii=False))
if __name__=="__main__": main()
