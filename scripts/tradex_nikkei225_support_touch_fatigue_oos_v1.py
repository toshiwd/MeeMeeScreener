from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.backend.services.market_watch_tags import NIKKEI_225_CODES
from tradex_nikkei225_lower_wick_rejection_oos_v1 import _code_bootstrap_difference, _dedupe, _mapping, _metric


AXIS_ID = "tradex_nikkei225_support_touch_fatigue_oos_v1"
SPLITS = {"train_2024": (20240101, 20241231), "validation_2025": (20250101, 20251231), "exploratory_2026": (20260101, 20260713)}
BINS = {"few_touches_0_2": (0, 3), "repeated_touches_3_4": (3, 5), "fatigued_touches_ge_5": (5, 1000)}


def run(sequence_csv: Path, output_root: Path) -> Path:
    events = []
    with sequence_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            if str(raw["code"]) not in NIKKEI_225_CODES:
                continue
            additions = _mapping(raw["sell_flow_additions"])
            if "support_break" not in additions or str(raw["irregular_event"]).lower() == "true":
                continue
            if not raw["ret10_forward"] or not raw["mfe_short_10"]:
                continue
            events.append({
                "code": str(raw["code"]), "ymd": int(raw["ymd"]), "axis": int(raw["support_touch_count_30"] or 0),
                "ret10": float(raw["ret10_forward"]), "mfe_short": float(raw["mfe_short_10"]),
                "rebound": str(raw["rebound_high_5pct_10"]).lower() == "true",
            })
    events = _dedupe(events)
    results = {}
    for split, (start, end) in SPLITS.items():
        split_rows = [row for row in events if start <= row["ymd"] <= end]
        results[split] = {"all": _metric(split_rows), "bins": {}}
        for name, (low, high) in BINS.items():
            results[split]["bins"][name] = _metric([row for row in split_rows if low <= row["axis"] < high])
        few = [row for row in split_rows if row["axis"] <= 2]
        fatigued = [row for row in split_rows if row["axis"] >= 5]
        left, right = _metric(fatigued), _metric(few)
        results[split]["fatigued_minus_few_touches"] = {
            "mean_ret10": left.get("mean_ret10", 0) - right.get("mean_ret10", 0),
            "down_close10_rate": left.get("down_close10_rate", 0) - right.get("down_close10_rate", 0),
            "low_minus5pct10_rate": left.get("low_minus5pct10_rate", 0) - right.get("low_minus5pct10_rate", 0),
            "rebound_plus5pct10_rate": left.get("rebound_plus5pct10_rate", 0) - right.get("rebound_plus5pct10_rate", 0),
            "mean_ret10_code_bootstrap95": _code_bootstrap_difference(fatigued, few, "ret10"),
        }
    train_diff = results["train_2024"]["fatigued_minus_few_touches"]
    validation_diff = results["validation_2025"]["fatigued_minus_few_touches"]
    validation_bins = results["validation_2025"]["bins"]
    sample_ok = validation_bins["few_touches_0_2"].get("n", 0) >= 40 and validation_bins["fatigued_touches_ge_5"].get("n", 0) >= 40
    direction_ok = train_diff["mean_ret10"] < 0 and validation_diff["mean_ret10"] < 0
    effect_ok = validation_diff["mean_ret10"] <= -0.005 and validation_diff["down_close10_rate"] >= 0.05
    rebound_guardrail = validation_diff["rebound_plus5pct10_rate"] <= 0
    ci = validation_diff["mean_ret10_code_bootstrap95"]
    ci_ok = ci["high95"] is not None and ci["high95"] < 0
    decision = "keep_support_fatigue_sell_addition" if all((sample_ok, direction_ok, effect_ok, rebound_guardrail, ci_ok)) else "hold_boundary_not_instrumented" if not sample_ok else "drop" if not direction_ok else "hold"
    spec = "event=support_break; axis=support_touch_count_30 bins 0-2,3-4,>=5"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(), "research_phase": "effectiveness_judgment",
        "data_quality": {"source_sha256": hashlib.sha256(sequence_csv.read_bytes()).hexdigest(), "confirmed_bars_only": True, "current_nikkei225_survivorship_bias": True, "2026_leakage_status": "exploratory_only_due_to_casebook_discovery"},
        "fixed_evaluation_conditions": {"universe": "current Nikkei225 registry", "splits": SPLITS, "changed_axis": "support touch count only", "frozen_event": "support_break", "touch_contract": "existing 30-bar support touch count", "cooldown_bars": 20, "horizon": 10, "costs": "ignored by user rule", "irregular_events": "excluded"},
        "candidate_spec": {"id": "support_touch_fatigue_sell_addition", "expression": spec, "spec_hash": hashlib.sha256(spec.encode()).hexdigest(), "discovery_source": "fixed 2026 representative casebook", "classification_target": "sell continuation addition only"},
        "results": results,
        "gate_audit": {"sample_ok": sample_ok, "train_validation_direction_ok": direction_ok, "validation_effect_ok": effect_ok, "rebound_guardrail": rebound_guardrail, "validation_ci_excludes_zero": ci_ok},
        "observed_branching": {"selected_event_count": len(events), "selection_divergence_reason": "same support-break event separated only by prior support-touch count", "changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None, "ranking_metrics_reason": "not a ranking experiment"},
        "decision": {"candidate_local_decision": decision, "session_aggregate_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_condition_oos_gate"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "remaining_risks": ["2026 is exploratory rather than clean shadow", "current constituent survivorship bias", "touch level uses existing 30-bar detector rather than proposed 0.35ATR proximity"],
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(output / "compare.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence-csv", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_support_touch_fatigue_oos_v1"))
    args = parser.parse_args()
    print(run(args.sequence_csv, args.output_root))


if __name__ == "__main__":
    main()
