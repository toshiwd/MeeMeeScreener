from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.tradex_short_state_transition_replay_v1 import HORIZONS, load_events
except ModuleNotFoundError:
    from tradex_short_state_transition_replay_v1 import HORIZONS, load_events


AXIS_ID = "tradex_high_zone_initial_exposure_v1"
FAMILY = "high_zone_climax"
POLICIES = ("all100", "high_price25", "tail_price_tier", "price_plus_continuation_risk")


def _value(row: pd.Series, name: str) -> float | None:
    value = row.get(name)
    return None if value is None or pd.isna(value) else float(value)


def price_band(close: float) -> str:
    if close < 900: return "under_900"
    if close < 3000: return "900_to_3000"
    if close < 5000: return "3000_to_5000"
    if close < 10000: return "5000_to_10000"
    return "10000_and_over"


def continuation_risk(row: pd.Series) -> dict[str, Any]:
    close = float(row["c"])
    open1 = _value(row, "o1")
    checks = {
        "ret20_at_least_120pct": float(row["ret20"]) >= 1.20,
        "ma20_distance_at_least_20pct": float(row["dist_ma20"]) >= 0.20,
        "next_open_gap_at_least_5pct": open1 is not None and open1 / close - 1.0 >= 0.05,
    }
    return {"score": sum(checks.values()), "high": sum(checks.values()) >= 2, "checks": checks}


def exposure(row: pd.Series, policy: str) -> float:
    close = float(row["c"])
    band = price_band(close)
    if policy == "all100": return 1.0
    if policy == "high_price25": return 0.25 if band == "10000_and_over" else 1.0
    if policy == "tail_price_tier":
        if band == "under_900": return 0.75
        if band == "10000_and_over": return 0.25
        return 1.0
    risk = continuation_risk(row)["high"]
    if band == "10000_and_over": return 0.25
    if band == "under_900": return 0.50 if risk else 0.75
    return 0.75 if risk else 1.0


def replay(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, source in events.iterrows():
        open1 = _value(source, "o1")
        risk = continuation_risk(source)
        for policy in POLICIES:
            weight = exposure(source, policy)
            record: dict[str, Any] = {
                "family": FAMILY, "code": str(source["code"]), "signal_ymd": int(source["signal_ymd"]),
                "signal_close": float(source["c"]), "price_band": price_band(float(source["c"])),
                "continuation_risk_score": risk["score"], "continuation_risk_high": risk["high"],
                "policy": policy, "state": "entry" if open1 else "unavailable", "exposure": weight,
                "entry_offset": 1 if open1 else None, "entry_price": open1, "wait_days": 1 if open1 else None,
            }
            for horizon in HORIZONS:
                exit_close = _value(source, f"c{1 + horizon}")
                highs = [_value(source, f"h{i}") for i in range(1, 2 + horizon)]
                highs = [x for x in highs if x is not None]
                record[f"ret{horizon}"] = None if open1 is None or exit_close is None else weight * (1 - exit_close / open1)
                record[f"mae{horizon}"] = None if open1 is None or not highs else weight * (1 - max(highs) / open1)
            rows.append(record)
    return pd.DataFrame(rows)


def _pf(values: pd.Series) -> float | None:
    losses = -values[values < 0].sum()
    return None if losses <= 0 else float(values[values > 0].sum() / losses)


def metrics(frame: pd.DataFrame) -> dict[str, Any]:
    entered = frame[frame.state == "entry"]
    result = {"signal_count": int(len(frame)), "entry_count": int(len(entered)), "participation_capture_rate": float(len(entered) / len(frame)), "mean_exposure": float(entered.exposure.mean()), "full_exposure_rate": float((entered.exposure == 1.0).mean()), "high_risk_rate": float(entered.continuation_risk_high.mean())}
    for horizon in HORIZONS:
        values, adverse = entered[f"ret{horizon}"].dropna(), entered[f"mae{horizon}"].dropna()
        result[f"h{horizon}"] = {"n": int(len(values)), "mean": float(values.mean()), "median": float(values.median()), "win_rate": float((values > 0).mean()), "profit_factor": _pf(values), "loss_le_minus5_rate": float((values <= -0.05).mean()), "loss_le_minus10_rate": float((values <= -0.10).mean()), "mean_mae": float(adverse.mean()), "worst_mae": float(adverse.min())}
    return result


def stability(frame: pd.DataFrame) -> dict[str, Any]:
    part = frame.copy(); part["year"] = part.signal_ymd.astype(str).str[:4]; part["month"] = part.signal_ymd.astype(str).str[:6]
    yearly = [{"year": k, **metrics(g)} for k,g in part.groupby("year")]; monthly = [{"month": k, **metrics(g)} for k,g in part.groupby("month")]; bands = [{"price_band": k, **metrics(g)} for k,g in part.groupby("price_band")]
    ey=[x for x in yearly if x["entry_count"]>=5]; em=[x for x in monthly if x["entry_count"]>=5]; eb=[x for x in bands if x["entry_count"]>=10]
    return {"yearly":yearly,"eligible_year_count":len(ey),"positive_mean_ret10_year_rate":sum((x["h10"]["mean"] or 0)>0 for x in ey)/len(ey) if ey else None,"eligible_month_count":len(em),"positive_mean_ret10_month_rate":sum((x["h10"]["mean"] or 0)>0 for x in em)/len(em) if em else None,"by_price_band":bands,"eligible_price_band_count":len(eb),"positive_mean_ret10_price_band_rate":sum((x["h10"]["mean"] or 0)>0 for x in eb)/len(eb) if eb else None}


def _json_ready(value: Any) -> Any:
    if isinstance(value,dict): return {str(k):_json_ready(v) for k,v in value.items()}
    if isinstance(value,list): return [_json_ready(v) for v in value]
    if isinstance(value,Path): return str(value)
    if hasattr(value,"item"): return _json_ready(value.item())
    if isinstance(value,float) and (math.isnan(value) or math.isinf(value)): return None
    return value


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int) -> Path:
    events=load_events(db_path,start_ymd,end_ymd); events=events[events.family==FAMILY].copy(); ledger=replay(events)
    results={p:metrics(ledger[ledger.policy==p]) for p in POLICIES}; stable={p:stability(ledger[ledger.policy==p]) for p in POLICIES}; baseline=results["all100"]
    challengers={}
    for policy in POLICIES[1:]:
        item=results[policy]
        checks={"participation_capture_100pct":item["participation_capture_rate"]==1.0,"mean_ret10_not_worse":item["h10"]["mean"]>=baseline["h10"]["mean"],"pf10_not_worse":(item["h10"]["profit_factor"] or 0)>=(baseline["h10"]["profit_factor"] or 0),"loss10_rate_not_worse":item["h10"]["loss_le_minus10_rate"]<=baseline["h10"]["loss_le_minus10_rate"],"positive_year_rate_at_least_75pct":stable[policy]["positive_mean_ret10_year_rate"]>=0.75,"positive_price_band_rate_at_least_75pct":stable[policy]["positive_mean_ret10_price_band_rate"]>=0.75}
        decision="keep" if all(checks.values()) else ("hold" if checks["participation_capture_100pct"] and checks["pf10_not_worse"] and checks["loss10_rate_not_worse"] else "drop")
        challengers[policy]={"candidate_local_decision":decision,"checks":checks,"metrics":item,"stability":stable[policy]}
    keepers=[p for p,x in challengers.items() if x["candidate_local_decision"]=="keep"]; holds=[p for p,x in challengers.items() if x["candidate_local_decision"]=="hold"]; pool=keepers or holds; leader=max(pool,key=lambda p:results[p]["h10"]["mean"]) if pool else None; decision="keep" if keepers else ("hold" if holds else "drop")
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"universe":"same high_zone_climax top5/day signals","period":{"start_ymd":start_ymd,"end_ymd":end_ymd},"changed_axis":"initial exposure only; all signals entered next open","price_bands":["under_900","900_to_3000","3000_to_5000","5000_to_10000","10000_and_over"],"continuation_risk":"at least two of ret20>=120pct, MA20 distance>=20pct, next-open GU>=5pct","horizons":list(HORIZONS),"costs":"ignored_by_user_request","runtime_db_write":False,"meemee_reflection":False},"source":{"db_path":str(db_path),"event_count":int(len(events)),"ledger_count":int(len(ledger))},"baseline":{"policy":"all100","metrics":baseline,"stability":stable["all100"]},"challengers":challengers,"observed_branching":{"changed_top5_members_count":0,"changed_top10_members_count":0,"changed_rank_count":0,"selection_divergence_reason":"all signal membership and timing fixed; only initial capital exposure changes"},"decision":{"candidate_local_decision":decision,"session_aggregate_decision":decision,"authoritative_rollup_decision":f"{decision}_high_zone_initial_exposure","selected_policy":leader if decision=="keep" else None,"research_leader":leader,"reason_type":"capture_expectancy_tail_and_stability_pass" if decision=="keep" else ("risk_adjusted_quality_improves_but_mean_gate_fails" if decision=="hold" else "initial_exposure_schedule_does_not_improve_tradeoff")},"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    run_dir=output_root/f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run_dir.mkdir(parents=True,exist_ok=False); ledger.to_parquet(run_dir/"initial_exposure_ledger.parquet",index=False); (run_dir/"compare.json").write_text(json.dumps(_json_ready(payload),ensure_ascii=False,indent=2)+"\n",encoding="utf-8"); (run_dir/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"status":"complete","required_files":["compare.json","initial_exposure_ledger.parquet","_ARTIFACT_COMPLETE.json"]},indent=2)+"\n",encoding="utf-8"); return run_dir


def main()->int:
    parser=argparse.ArgumentParser(); parser.add_argument("--db-path",type=Path,required=True); parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_high_zone_initial_exposure_v1")); parser.add_argument("--start-ymd",type=int,default=20160101); parser.add_argument("--end-ymd",type=int,default=20260617); args=parser.parse_args(); print(run(args.db_path,args.output_root,args.start_ymd,args.end_ymd)); return 0


if __name__=="__main__": raise SystemExit(main())
