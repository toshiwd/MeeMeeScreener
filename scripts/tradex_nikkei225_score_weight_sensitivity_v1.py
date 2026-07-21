from __future__ import annotations

import argparse
import ast
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_nikkei225_score_weight_sensitivity_v1"
WEIGHTS = (0.5, 1.0, 1.5, 2.0, 2.5)
THRESHOLD = 6.0
DECAY = 0.75


def _parse(value: str) -> dict[str, float]:
    if not value:
        return {}
    parsed = ast.literal_eval(value)
    return {str(key): float(weight) for key, weight in parsed.items()}


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [row for row in rows if row["ret10"] is not None]
    if not completed:
        return {"n": 0, "codes": 0, "down_close10_rate": None, "down_low5pct10_rate": None, "mean_ret10": None, "rebound5pct10_rate": None}
    return {
        "n": len(completed),
        "codes": len({row["code"] for row in completed}),
        "down_close10_rate": sum(row["ret10"] < 0 for row in completed) / len(completed),
        "down_low5pct10_rate": sum(row["mfe_short10"] >= .05 for row in completed) / len(completed),
        "mean_ret10": sum(row["ret10"] for row in completed) / len(completed),
        "rebound5pct10_rate": sum(row["rebound5"] for row in completed) / len(completed),
    }


def _replay(rows: list[dict[str, Any]], component: str, weight: float) -> list[dict[str, Any]]:
    scores: dict[str, float] = {}
    positions: dict[str, int] = {}
    last_signal: dict[str, int] = {}
    signals: list[dict[str, Any]] = []
    for row in rows:
        code = row["code"]
        pos = positions.get(code, -1) + 1
        positions[code] = pos
        additions = dict(row["additions"])
        deductions = dict(row["deductions"])
        if component in additions:
            additions[component] = weight
        if component in deductions:
            deductions[component] = weight
        previous = scores.get(code, 0.0)
        score = max(-10.0, min(10.0, previous * DECAY + sum(additions.values()) - sum(deductions.values())))
        scores[code] = score
        crossed = previous < THRESHOLD <= score
        if crossed and row["rebound_risk"] < 4 and not row["irregular"] and (code not in last_signal or pos-last_signal[code] > 10):
            signals.append(row)
            last_signal[code] = pos
    return signals


def _utility(metric: dict[str, Any]) -> float:
    if (metric.get("n") or 0) < 30:
        return -999.0
    return float(metric["down_close10_rate"] or 0) + float(metric["down_low5pct10_rate"] or 0) - float(metric["rebound5pct10_rate"] or 0) - max(-.10, min(.10, float(metric["mean_ret10"] or 0)))


def run(input_csv: Path, output_root: Path) -> Path:
    rows: list[dict[str, Any]] = []
    observed: dict[str, list[float]] = {}
    with input_csv.open("r", encoding="utf-8-sig", newline="") as fh:
        for raw in csv.DictReader(fh):
            additions, deductions = _parse(raw["sell_flow_additions"]), _parse(raw["sell_flow_deductions"])
            for key, value in {**additions, **deductions}.items():
                observed.setdefault(key, []).append(value)
            rows.append({
                "code": str(raw["code"]), "ymd": int(raw["ymd"]), "additions": additions, "deductions": deductions,
                "rebound_risk": float(raw["rebound_risk_score"] or 0), "irregular": raw["irregular_event"].lower()=="true",
                "ret5": float(raw["ret5_forward"]) if raw["ret5_forward"] else None,
                "ret10": float(raw["ret10_forward"]) if raw["ret10_forward"] else None,
                "mfe_short10": float(raw["mfe_short_10"]) if raw["mfe_short_10"] else 0.0,
                "rebound5": raw["rebound_high_5pct_10"].lower()=="true" if raw["rebound_high_5pct_10"] else False,
            })
    rows.sort(key=lambda row: (row["code"], row["ymd"]))
    components = sorted(observed)
    leaderboard: list[dict[str, Any]] = []
    decisions: dict[str, Any] = {}
    for component in components:
        variants = []
        for weight in WEIGHTS:
            signals = _replay(rows, component, weight)
            metrics = {
                split: _metrics([row for row in signals if start <= row["ymd"] <= end])
                for split, (start, end) in {"train_2024":(20240101,20241231),"validation_2025":(20250101,20251231),"shadow_2026":(20260101,20261231)}.items()
            }
            record = {"component":component,"weight":weight,"metrics":metrics,"train_utility":_utility(metrics["train_2024"])}
            variants.append(record); leaderboard.append(record)
        selected = max(variants, key=lambda item:(item["train_utility"],-abs(item["weight"]-1.0)))
        base_weight = round(sum(observed[component])/len(observed[component]), 6)
        baseline = min(variants, key=lambda item:abs(item["weight"]-base_weight))
        val, base_val, shadow = selected["metrics"]["validation_2025"], baseline["metrics"]["validation_2025"], selected["metrics"]["shadow_2026"]
        keep = bool((val.get("n") or 0)>=30 and (val.get("down_close10_rate") or 0)>(base_val.get("down_close10_rate") or 0) and (val.get("mean_ret10") or 1)<(base_val.get("mean_ret10") or 1) and (shadow.get("mean_ret10") or 1)<0)
        decisions[component] = {"observed_baseline_weight":base_weight,"selected_train_weight":selected["weight"],"decision":"keep" if keep else "drop","selected_metrics":selected["metrics"],"baseline_metrics":baseline["metrics"]}
    stamp=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ");out=output_root/f"{stamp}-{AXIS_ID}";out.mkdir(parents=True,exist_ok=False)
    payload={"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","research_phase":"effectiveness_judgment","fixed_evaluation_conditions":{"universe":"current Nikkei225 registry; survivorship-biased research slice","train":[20240101,20241231],"validation":[20250101,20251231],"shadow":[20260101,20261231],"changed_axis":"one score component weight at a time","weights":WEIGHTS,"decay":DECAY,"entry":"first upward crossing of sell score 6 with rebound risk <4","cooldown_bars":10,"costs":"ignored by user rule"},"source_ledger":str(input_csv),"component_decisions":decisions,"decision":{"candidate_local_decision":"keep_components_exist" if any(x["decision"]=="keep" for x in decisions.values()) else "drop_all_weight_changes","authoritative_rollup_decision":"review_only"},"boundary":{"owner":"TRADEX","meemee_changed":False,"runtime_db_write":False,"production_ranking_changed":False}}
    (out/"compare.json").write_text(json.dumps(payload,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    with (out/"weight_leaderboard.csv").open("w",encoding="utf-8-sig",newline="") as fh:
        writer=csv.DictWriter(fh,fieldnames=["component","weight","train_utility","metrics_json"]);writer.writeheader();writer.writerows({"component":x["component"],"weight":x["weight"],"train_utility":x["train_utility"],"metrics_json":json.dumps(x["metrics"],ensure_ascii=False)} for x in leaderboard)
    (out/"_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete":True,"compare":str(out/"compare.json")},indent=2)+"\n",encoding="utf-8")
    return out


def main() -> None:
    parser=argparse.ArgumentParser();parser.add_argument("--input-csv",type=Path,required=True);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_score_weight_sensitivity_v1"));args=parser.parse_args();print(run(args.input_csv,args.output_root))


if __name__=="__main__":main()
