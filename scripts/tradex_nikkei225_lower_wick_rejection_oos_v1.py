from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import json
import random
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.backend.services.market_watch_tags import NIKKEI_225_CODES


AXIS_ID = "tradex_nikkei225_lower_wick_rejection_oos_v1"
SPLITS = {"train_2024": (20240101, 20241231), "validation_2025": (20250101, 20251231), "exploratory_2026": (20260101, 20260713)}
BINS = {"no_rejection_lt_0_10": (0.0, 0.10), "partial_rejection_0_10_0_25": (0.10, 0.25), "clear_rejection_ge_0_25": (0.25, 1.01)}


def _mapping(value: str) -> dict[str, float]:
    return {str(key): float(score) for key, score in ast.literal_eval(value or "{}").items()}


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0, "codes": 0, "months": 0}
    return {
        "n": len(rows), "codes": len({row["code"] for row in rows}), "months": len({row["ymd"] // 100 for row in rows}),
        "mean_ret10": sum(row["ret10"] for row in rows) / len(rows),
        "down_close10_rate": sum(row["ret10"] < 0 for row in rows) / len(rows),
        "low_minus5pct10_rate": sum(row["mfe_short"] >= 0.05 for row in rows) / len(rows),
        "rebound_plus5pct10_rate": sum(row["rebound"] for row in rows) / len(rows),
    }


def _code_bootstrap_difference(left: list[dict[str, Any]], right: list[dict[str, Any]], field: str, seed: int = 20260714) -> dict[str, Any]:
    left_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    right_by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in left: left_by[row["code"]].append(row)
    for row in right: right_by[row["code"]].append(row)
    left_codes, right_codes = sorted(left_by), sorted(right_by)
    if len(left_codes) < 5 or len(right_codes) < 5:
        return {"samples": 0, "low95": None, "high95": None}
    rng = random.Random(seed)
    values = []
    for _ in range(1000):
        sampled_left = [row for _code in rng.choices(left_codes, k=len(left_codes)) for row in left_by[_code]]
        sampled_right = [row for _code in rng.choices(right_codes, k=len(right_codes)) for row in right_by[_code]]
        values.append(sum(row[field] for row in sampled_left) / len(sampled_left) - sum(row[field] for row in sampled_right) / len(sampled_right))
    values.sort()
    return {"samples": len(values), "low95": values[24], "high95": values[974]}


def _dedupe(rows: list[dict[str, Any]], cooldown: int = 20) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    positions: dict[str, int] = {}
    last_selected: dict[str, int] = {}
    for row in sorted(rows, key=lambda item: (item["code"], item["ymd"])):
        position = positions.get(row["code"], -1) + 1
        positions[row["code"]] = position
        if row["code"] not in last_selected or position - last_selected[row["code"]] > cooldown:
            output.append(row)
            last_selected[row["code"]] = position
    return output


def run(sequence_csv: Path, output_root: Path) -> Path:
    events: list[dict[str, Any]] = []
    with sequence_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            if str(raw["code"]) not in NIKKEI_225_CODES:
                continue
            additions = _mapping(raw["sell_flow_additions"])
            if not ({"ma20_break", "support_break"} & additions.keys()) or str(raw["irregular_event"]).lower() == "true":
                continue
            if not raw["ret10_forward"] or not raw["mfe_short_10"]:
                continue
            lower_wick = float(raw["lower_wick_ratio"] or 0)
            events.append({
                "code": str(raw["code"]), "ymd": int(raw["ymd"]), "lower_wick": lower_wick,
                "ret10": float(raw["ret10_forward"]), "mfe_short": float(raw["mfe_short_10"]),
                "rebound": str(raw["rebound_high_5pct_10"]).lower() == "true",
                "event_family": "support_break" if "support_break" in additions else "ma20_break",
            })
    events = _dedupe(events)
    results: dict[str, Any] = {}
    for split, (start, end) in SPLITS.items():
        split_rows = [row for row in events if start <= row["ymd"] <= end]
        results[split] = {"all": _metric(split_rows), "bins": {}, "event_family": {}}
        for name, (low, high) in BINS.items():
            results[split]["bins"][name] = _metric([row for row in split_rows if low <= row["lower_wick"] < high])
        for family in ("ma20_break", "support_break"):
            results[split]["event_family"][family] = {
                name: _metric([row for row in split_rows if row["event_family"] == family and low <= row["lower_wick"] < high])
                for name, (low, high) in BINS.items()
            }
        no_rejection = [row for row in split_rows if row["lower_wick"] < 0.10]
        clear_rejection = [row for row in split_rows if row["lower_wick"] >= 0.25]
        results[split]["no_rejection_minus_clear_rejection"] = {
            "mean_ret10": _metric(no_rejection).get("mean_ret10", 0) - _metric(clear_rejection).get("mean_ret10", 0),
            "down_close10_rate": _metric(no_rejection).get("down_close10_rate", 0) - _metric(clear_rejection).get("down_close10_rate", 0),
            "low_minus5pct10_rate": _metric(no_rejection).get("low_minus5pct10_rate", 0) - _metric(clear_rejection).get("low_minus5pct10_rate", 0),
            "rebound_plus5pct10_rate": _metric(no_rejection).get("rebound_plus5pct10_rate", 0) - _metric(clear_rejection).get("rebound_plus5pct10_rate", 0),
            "mean_ret10_code_bootstrap95": _code_bootstrap_difference(no_rejection, clear_rejection, "ret10"),
        }
    train, validation = results["train_2024"], results["validation_2025"]
    train_diff, validation_diff = train["no_rejection_minus_clear_rejection"], validation["no_rejection_minus_clear_rejection"]
    validation_bins = validation["bins"]
    sample_ok = validation_bins["no_rejection_lt_0_10"].get("n", 0) >= 40 and validation_bins["clear_rejection_ge_0_25"].get("n", 0) >= 40
    direction_ok = train_diff["mean_ret10"] < 0 and validation_diff["mean_ret10"] < 0
    effect_ok = validation_diff["mean_ret10"] <= -0.005 and validation_diff["down_close10_rate"] >= 0.05
    rebound_guardrail = validation_diff["rebound_plus5pct10_rate"] <= 0
    ci = validation_diff["mean_ret10_code_bootstrap95"]
    ci_ok = ci["high95"] is not None and ci["high95"] < 0
    decision = "keep_sell_deduction_not_buy_addition" if all((sample_ok, direction_ok, effect_ok, rebound_guardrail, ci_ok)) else "drop" if not direction_ok else "hold"
    spec = "event=(ma20_break OR support_break); axis=lower_wick_ratio bins <0.10,0.10-0.25,>=0.25"
    source_hash = hashlib.sha256(sequence_csv.read_bytes()).hexdigest()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(), "research_phase": "effectiveness_judgment",
        "data_quality": {"source_sha256": source_hash, "confirmed_bars_only": True, "current_nikkei225_survivorship_bias": True, "2026_leakage_status": "exploratory_only_due_to_casebook_discovery"},
        "fixed_evaluation_conditions": {"universe": "current Nikkei225 registry", "splits": SPLITS, "changed_axis": "lower_wick_ratio only", "frozen_event": "ma20_break or support_break", "cooldown_bars": 20, "horizon": 10, "costs": "ignored by user rule", "irregular_events": "excluded"},
        "candidate_spec": {"id": "lower_wick_rejection_sell_deduction", "expression": spec, "spec_hash": hashlib.sha256(spec.encode()).hexdigest(), "discovery_source": "fixed 2026 representative casebook", "classification_target": "sell deduction only; never direct buy addition"},
        "results": results,
        "gate_audit": {"sample_ok": sample_ok, "train_validation_direction_ok": direction_ok, "validation_effect_ok": effect_ok, "rebound_guardrail": rebound_guardrail, "validation_ci_excludes_zero": ci_ok},
        "observed_branching": {"selected_event_count": len(events), "selection_divergence_reason": "same break event separated only by lower-wick rejection ratio", "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None, "ranking_metrics_reason": "not a ranking experiment"},
        "decision": {"candidate_local_decision": decision, "session_aggregate_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_condition_oos_gate"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "remaining_risks": ["2026 is exploratory rather than clean shadow", "current constituent survivorship bias", "lower wick and close position are correlated"],
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(output / "compare.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence-csv", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_lower_wick_rejection_oos_v1"))
    args = parser.parse_args()
    print(run(args.sequence_csv, args.output_root))


if __name__ == "__main__":
    main()
