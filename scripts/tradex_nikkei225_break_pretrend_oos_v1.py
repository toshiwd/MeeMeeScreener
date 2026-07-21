from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import random
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_nikkei225_break_pretrend_oos_v1"
SPLITS = {"train_2024": (20240101, 20241231), "validation_2025": (20250101, 20251231), "exploratory_2026": (20260101, 20260713)}


def _mapping(value: str) -> dict[str, float]:
    return {str(key): float(score) for key, score in ast.literal_eval(value or "{}").items()}


def _dedupe(rows: list[dict[str, Any]], cooldown: int = 20) -> list[dict[str, Any]]:
    output, positions, last = [], {}, {}
    for row in sorted(rows, key=lambda item: (item["code"], item["ymd"])):
        position = positions.get(row["code"], -1) + 1; positions[row["code"]] = position
        if row["code"] not in last or position - last[row["code"]] > cooldown:
            output.append(row); last[row["code"]] = position
    return output


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows: return {"n": 0, "codes": 0, "months": 0}
    return {"n": len(rows), "codes": len({row["code"] for row in rows}), "months": len({row["ymd"] // 100 for row in rows}), "mean_ret10": sum(row["ret10"] for row in rows) / len(rows), "down_close10_rate": sum(row["ret10"] < 0 for row in rows) / len(rows), "low_minus5pct10_rate": sum(row["mfe_short"] >= 0.05 for row in rows) / len(rows), "rebound_plus5pct10_rate": sum(row["rebound"] for row in rows) / len(rows)}


def _bootstrap(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> dict[str, Any]:
    by_left, by_right = defaultdict(list), defaultdict(list)
    for row in left: by_left[row["code"]].append(row)
    for row in right: by_right[row["code"]].append(row)
    lc, rc = sorted(by_left), sorted(by_right)
    if len(lc) < 5 or len(rc) < 5: return {"samples": 0, "low95": None, "high95": None}
    rng = random.Random(20260714); values = []
    for _ in range(1000):
        a = [row for code in rng.choices(lc, k=len(lc)) for row in by_left[code]]
        b = [row for code in rng.choices(rc, k=len(rc)) for row in by_right[code]]
        values.append(sum(row["ret10"] for row in a) / len(a) - sum(row["ret10"] for row in b) / len(b))
    values.sort(); return {"samples": 1000, "low95": values[24], "high95": values[974]}


def run(input_csv: Path, output_root: Path) -> Path:
    events = []
    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            additions = _mapping(raw["sell_flow_additions"])
            if not ({"ma20_break", "support_break"} & additions.keys()) or raw["irregular_event"].lower() == "true" or not raw["ret10_forward"]:
                continue
            events.append({"code": str(raw["code"]), "ymd": int(raw["ymd"]), "pretrend": raw["pretrend_10"], "ret10": float(raw["ret10_forward"]), "mfe_short": float(raw["mfe_short_10"] or 0), "rebound": raw["rebound_high_5pct_10"].lower() == "true"})
    events = _dedupe(events)
    results = {}
    for split, (start, end) in SPLITS.items():
        rows = [row for row in events if start <= row["ymd"] <= end]
        bins = {name: _metric([row for row in rows if row["pretrend"] == name]) for name in ("up", "flat", "down")}
        up, down = [row for row in rows if row["pretrend"] == "up"], [row for row in rows if row["pretrend"] == "down"]
        left, right = _metric(up), _metric(down)
        results[split] = {"all": _metric(rows), "bins": bins, "up_minus_down": {"mean_ret10": left.get("mean_ret10", 0) - right.get("mean_ret10", 0), "down_close10_rate": left.get("down_close10_rate", 0) - right.get("down_close10_rate", 0), "low_minus5pct10_rate": left.get("low_minus5pct10_rate", 0) - right.get("low_minus5pct10_rate", 0), "rebound_plus5pct10_rate": left.get("rebound_plus5pct10_rate", 0) - right.get("rebound_plus5pct10_rate", 0), "mean_ret10_code_bootstrap95": _bootstrap(up, down)}}
    train, validation = results["train_2024"]["up_minus_down"], results["validation_2025"]["up_minus_down"]
    vb = results["validation_2025"]["bins"]
    sample_ok = vb["up"].get("n", 0) >= 40 and vb["down"].get("n", 0) >= 40
    direction_ok = train["mean_ret10"] < 0 and validation["mean_ret10"] < 0
    effect_ok = validation["mean_ret10"] <= -0.005 and validation["down_close10_rate"] >= 0.05
    rebound_ok = validation["rebound_plus5pct10_rate"] <= 0
    ci = validation["mean_ret10_code_bootstrap95"]; ci_ok = ci["high95"] is not None and ci["high95"] < 0
    decision = "keep_uptrend_exhaustion_context" if all((sample_ok, direction_ok, effect_ok, rebound_ok, ci_ok)) else "drop" if not direction_ok else "hold"
    spec = "event=(ma20_break OR support_break); changed_axis=pretrend10 category up>=3pct, down<=-3pct, else flat"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); output = output_root / f"{stamp}-{AXIS_ID}"; output.mkdir(parents=True, exist_ok=False)
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment", "data_quality": {"source_sha256": hashlib.sha256(input_csv.read_bytes()).hexdigest(), "confirmed_bars_only": True, "2026_leakage_status": "exploratory_only"}, "fixed_evaluation_conditions": {"universe": "current Nikkei225 ledger", "splits": SPLITS, "frozen_event": "ma20_break or support_break", "changed_axis": "pretrend10 only", "cooldown_bars": 20, "horizon": 10, "costs": "ignored by user rule"}, "candidate_spec": {"expression": spec, "spec_hash": hashlib.sha256(spec.encode()).hexdigest(), "discovery_source": "10-case fine reading"}, "results": results, "gate_audit": {"sample_ok": sample_ok, "direction_ok": direction_ok, "effect_ok": effect_ok, "rebound_guardrail": rebound_ok, "validation_ci_excludes_zero": ci_ok}, "observed_branching": {"selected_event_count": len(events), "selection_divergence_reason": "same break event divided only by prior 10-day trend", "changed_rank_count": None}, "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only"}, "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}}
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(output / "compare.json")}, indent=2) + "\n", encoding="utf-8"); return output


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--input-csv", required=True, type=Path); parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_break_pretrend_oos_v1")); args = parser.parse_args(); print(run(args.input_csv, args.output_root))


if __name__ == "__main__": main()
