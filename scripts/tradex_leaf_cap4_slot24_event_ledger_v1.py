from __future__ import annotations

import argparse, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_leaf_order_contract_readiness_v1 import replay

COMPARE=Path(r"G:\Tradex\leaf_order_contract_readiness_v1\20260711T140535Z-leaf_order_contract_readiness_v1\compare.json")
OUT=Path(r"G:\Tradex\leaf_cap4_slot24_event_ledger_v1")

def digest(p:Path)->str:return hashlib.sha256(p.read_bytes()).hexdigest()
def pf(s:pd.Series):
    neg=-s[s<0].sum();return None if neg==0 else float(s[s>0].sum()/neg)
def metrics(x:pd.DataFrame)->dict:
    result={}
    for split in ("train","validation","test"):
        z=x[x.split==split]; r=z.next_open_return
        result[split]={"n":int(len(z)),"signal_days":int(z.next_entry_date.nunique()),"expectancy":float(r.mean()) if len(z) else None,"profit_factor":pf(r),"win_rate":float((r>0).mean()) if len(z) else None,"pnl_yen":float(z.pnl_yen.sum())}
    return result
def generate(compare:Path,out:Path)->Path:
    c=json.loads(compare.read_text(encoding="utf-8")); fixed=c["fixed_evaluation_conditions"]
    source=Path(fixed["source"]); x=pd.read_csv(source)
    if fixed.get("slot_budget_yen")!=2_400_000 or fixed.get("maximum_positions")!=4 or fixed.get("same_day_candidate_cap")!=3: raise ValueError("AUTHORITATIVE_CONTRACT_MISMATCH")
    z,summary=replay(x,.001); expected=c["selected_operational_contract"]
    checks={k:summary[k]==expected[k] for k in ("accepted_trade_count","eligible_top3_rows","unaffordable_top3_count","red_year_count")}
    for k in ("pnl_2024_2025_yen","test_money_profit_factor","max_realized_drawdown_yen","max_concurrent_invested_yen"):checks[k]=abs(summary[k]-expected[k])<1e-8
    if not all(checks.values()):raise RuntimeError("REPLAY_DOES_NOT_MATCH_AUTHORITATIVE_COMPARE")
    z=z.copy();z.insert(0,"event_id",[f"leaf-cap4-slot24-{i:06d}" for i in range(1,len(z)+1)])
    now=datetime.now(timezone.utc);root=out/f"{now.strftime('%Y%m%dT%H%M%SZ')}-tradex_leaf_cap4_slot24_event_ledger_v1";root.mkdir(parents=True)
    ledger=root/"event_ledger.csv";z.to_csv(ledger,index=False)
    payload={"schema_version":"tradex_leaf_cap4_slot24_event_ledger_v1.manifest.v1","artifact_role":"authoritative","generated_at":now.isoformat(),"family":"leaf_shallow_high_zone","contract":{"entry":"next-session opening auction market order","adverse_fill_stress":.001,"slot_budget_yen":2400000,"maximum_positions":4,"same_day_candidate_cap":3,"round_lot":100,"unaffordable_policy":"skip_without_rank4_promotion","horizon_sessions":10},"splits":{"train":[2019,2021],"validation":[2022,2023],"test":[2024,2025]},"no_lookahead":{"status":"pass_by_existing_fixed_replay_contract","selection_order_columns":["next_entry_date","tie_gap_ma60","code"],"future_columns_used_only_for_exit":["exit_date","next_open_return"]},"source_artifacts":[{"path":str(compare),"sha256":digest(compare)},{"path":str(source),"sha256":digest(source)}],"event_ledger":{"path":str(ledger),"sha256":digest(ledger),"rows":len(z)},"metrics_by_split":metrics(z),"authoritative_metric_match":checks,"existing_decision":c.get("decision"),"rules_or_thresholds_changed":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    p=root/"manifest.json";p.write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8");return p
def main():
    a=argparse.ArgumentParser();a.add_argument("--compare",type=Path,default=COMPARE);a.add_argument("--out",type=Path,default=OUT);x=a.parse_args();print(generate(x.compare,x.out))
if __name__=="__main__":main()
