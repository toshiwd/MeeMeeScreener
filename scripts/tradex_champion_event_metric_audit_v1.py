from __future__ import annotations

import argparse, csv, hashlib, json, math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BUY = Path(r"G:\Tradex\leaf_operational_readiness_rollup_v1\20260711T140602Z-leaf_operational_readiness_rollup_v1\session_leaderboard_rollup.json")
BUY_EVENTS = Path(r"G:\Tradex\chart_entry_geometry_research_v1\20260711T104710Z-shallow_high_zone_next_open_execution_v1\budget_10m_cap5_events.csv")
SELL = Path(r"G:\Tradex\short_leaf20_final_rollup_v1\latest_final_rollup.json")
OUT = Path(r"G:\Tradex\champion_event_metric_audit_v1")
METRICS = ("n", "signal_days", "weekly_frequency", "expectancy", "profit_factor", "win_rate", "avg_win", "avg_loss", "payoff_ratio", "p05", "p01")

def sha(path: Path) -> str: return hashlib.sha256(path.read_bytes()).hexdigest()
def q(values: list[float], p: float) -> float | None:
    if not values: return None
    x=sorted(values); pos=(len(x)-1)*p; lo=int(math.floor(pos)); hi=int(math.ceil(pos))
    return x[lo] if lo==hi else x[lo]+(x[hi]-x[lo])*(pos-lo)
def iso_week(epoch: str) -> str:
    d=datetime.fromtimestamp(int(float(epoch)), timezone.utc); y,w,_=d.isocalendar(); return f"{y}-W{w:02d}"
def summarize(rows: list[dict], ret_col="next_open_return", date_col="date") -> dict:
    vals=[float(r[ret_col]) for r in rows if r.get(ret_col) not in (None,"")]
    wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]; days={r[date_col] for r in rows if r.get(date_col)}
    weeks=defaultdict(int)
    for r in rows:
        if r.get(date_col): weeks[iso_week(r[date_col])]+=1
    pf=sum(wins)/abs(sum(losses)) if losses else None; aw=sum(wins)/len(wins) if wins else None; al=sum(losses)/len(losses) if losses else None
    return {"n":len(vals),"signal_days":len(days),"weekly_frequency":{"active_weeks":len(weeks),"mean_signals":sum(weeks.values())/len(weeks) if weeks else None,"max_signals":max(weeks.values()) if weeks else None},"expectancy":sum(vals)/len(vals) if vals else None,"profit_factor":pf,"win_rate":len(wins)/len(vals) if vals else None,"avg_win":aw,"avg_loss":al,"payoff_ratio":aw/abs(al) if aw is not None and al else None,"p05":q(vals,.05),"p01":q(vals,.01),"tail":{"loss_count":len(losses),"worst":min(vals) if vals else None},"missing_metric":[]}
def missing(reason: str) -> dict:
    d={k:None for k in METRICS}; d["tail"]=None; d["missing_metric"]=list(METRICS)+["tail"]; d["missing_reason"]=reason; return d
def decision(doc: dict) -> dict:
    return doc.get("decision", {"authoritative_rollup_decision":doc.get("authoritative_rollup_decision")})
def run(buy:Path, buy_events:Path, sell:Path, out:Path)->Path:
    bd=json.loads(buy.read_text(encoding="utf-8")); sd=json.loads(sell.read_text(encoding="utf-8"))
    with buy_events.open(encoding="utf-8",newline="") as f: rows=list(csv.DictReader(f))
    splits={s:summarize([r for r in rows if r.get("split")==s]) for s in ("train","validation","test")}
    payload={"schema_version":"tradex_champion_event_metric_audit_v1.compare.v1","artifact_role":"authoritative","generated_at":datetime.now(timezone.utc).isoformat(),"boundary_owner":"TRADEX","lanes":[
      {"side":"buy","family":"leaf_shallow_high_zone","comparison_lane":"entry_horizon_mismatch","typed_reason":"EVENT_LEDGER_CAP5_SLOT2M_DOES_NOT_MATCH_CHAMPION_CAP4_SLOT2_4M","champion_contract":bd.get("operational_contract"),"event_ledger_contract":{"maximum_positions":5,"slot_budget_yen":2000000,"entry":"next_session_open","horizon_sessions":10},"metrics_by_split":splits,"existing_decision":decision(bd),"source_artifacts":[{"path":str(buy),"sha256":sha(buy)},{"path":str(buy_events),"sha256":sha(buy_events)}]},
      {"side":"sell","family":"support_break_capitulation_breadth40","comparison_lane":"missing_event_ledger","typed_reason":"AUTHORITATIVE_EVENT_LEDGER_NOT_PERSISTED","champion_contract":sd.get("short_rule"),"metrics_by_split":{s:missing("AUTHORITATIVE_EVENT_LEDGER_NOT_PERSISTED") for s in ("train","validation","test")},"existing_decision":decision(sd),"source_artifacts":[{"path":str(sell),"sha256":sha(sell)}]}
    ],"cross_lane_comparison":{"status":"not_comparable","typed_reasons":["BUY_EVENT_LEDGER_CONTRACT_MISMATCH","SELL_EVENT_LEDGER_MISSING","ENTRY_AND_HORIZON_CONTRACTS_NOT_ALIGNED"]},"runner_reexecuted":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    root=out/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-tradex_champion_event_metric_audit_v1"; root.mkdir(parents=True,exist_ok=False); p=root/"compare.json"; p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); return p
def main():
    a=argparse.ArgumentParser(); a.add_argument("--buy",type=Path,default=BUY);a.add_argument("--buy-events",type=Path,default=BUY_EVENTS);a.add_argument("--sell",type=Path,default=SELL);a.add_argument("--out",type=Path,default=OUT);x=a.parse_args();print(run(x.buy,x.buy_events,x.sell,x.out))
if __name__=="__main__": main()
