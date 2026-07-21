from __future__ import annotations

import argparse
import ast
import csv
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


AXIS_ID = "tradex_nikkei225_representative_casebook_v1"


def _number(value: str) -> float | None:
    return float(value) if value not in (None, "") else None


def _mapping(value: str) -> dict[str, float]:
    return {str(key): float(score) for key, score in ast.literal_eval(value or "{}").items()}


def _distance(row: dict[str, Any], center: dict[str, float]) -> float:
    scales = {"range20": 0.05, "rebound": 2.0, "add_score": 3.0, "volume": 0.5}
    return sum(abs((row[key] if row[key] is not None else center[key]) - center[key]) / scales[key] for key in center)


def _center(rows: list[dict[str, Any]]) -> dict[str, float]:
    fields = ("range20", "rebound", "add_score", "volume")
    return {
        field: statistics.median([row[field] for row in rows if row[field] is not None])
        if any(row[field] is not None for row in rows)
        else 0.0
        for field in fields
    }


def _dedupe(rows: list[dict[str, Any]], gap: int = 20) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    last: dict[str, int] = {}
    positions: dict[str, int] = {}
    for row in sorted(rows, key=lambda item: (item["code"], item["ymd"])):
        position = positions.get(row["code"], -1) + 1
        positions[row["code"]] = position
        if row["code"] not in last or position - last[row["code"]] > gap:
            selected.append(row)
            last[row["code"]] = position
    return selected


def _compact(row: dict[str, Any], role: str, distance: float) -> dict[str, Any]:
    return {
        "role": role,
        "code": row["code"],
        "ymd": row["ymd"],
        "close": row["close"],
        "pretrend_10": row["pretrend"],
        "position_60": row["position"],
        "ma20_side": row["ma20_side"],
        "sideways20_run": row["sideways"],
        "range20_pct": row["range20"],
        "volume_compression_5_20": row["volume"],
        "sell_additions": row["adds"],
        "sell_deductions": row["deducts"],
        "rebound_risk_score": row["rebound"],
        "ret5_forward": row["ret5"],
        "ret10_forward": row["ret10"],
        "mfe_short_10": row["mfe_short"],
        "mfe_long_10": row["mfe_long"],
        "prototype_distance": distance,
    }


def run(input_csv: Path, output_root: Path) -> Path:
    rows: list[dict[str, Any]] = []
    with input_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            ymd = int(raw["ymd"])
            if not 20260101 <= ymd <= 20260713 or raw["irregular_event"].lower() == "true":
                continue
            adds = _mapping(raw["sell_flow_additions"])
            deducts = _mapping(raw["sell_flow_deductions"])
            rows.append(
                {
                    "code": str(raw["code"]), "ymd": ymd, "close": float(raw["c"]),
                    "adds": adds, "deducts": deducts,
                    "add_score": sum(adds.values()), "deduct_score": sum(deducts.values()),
                    "rebound": float(raw["rebound_risk_score"] or 0),
                    "range20": _number(raw["range20_pct"]), "sideways": int(raw["sideways20_run_length"] or 0),
                    "volume": _number(raw["volume_compression_5_20"]), "position": raw["sideways_position"],
                    "ma20_side": raw["sideways_ma20_side"], "pretrend": raw["pretrend_10"],
                    "ret5": _number(raw["ret5_forward"]), "ret10": _number(raw["ret10_forward"]),
                    "mfe_short": _number(raw["mfe_short_10"]), "mfe_long": _number(raw["mfe_long_10"]),
                }
            )

    rules: dict[str, Callable[[dict[str, Any]], bool]] = {
        "high_zone_ma20_break": lambda row: row["position"] == "high" and row["ma20_side"] == "below" and "ma20_break" in row["adds"],
        "support_break": lambda row: "support_break" in row["adds"],
        "uptrend_compression": lambda row: row["pretrend"] == "up" and row["sideways"] == 5,
        "downtrend_compression": lambda row: row["pretrend"] == "down" and row["sideways"] == 5,
        "prolonged_compression": lambda row: row["sideways"] == 10,
    }
    casebook: dict[str, Any] = {}
    used_codes: set[str] = set()
    for archetype, rule in rules.items():
        eligible = _dedupe([row for row in rows if rule(row) and row["ret10"] is not None])
        center = _center(eligible)
        continuation = sorted(
            [row for row in eligible if row["ret10"] < 0], key=lambda row: (_distance(row, center), row["code"], row["ymd"])
        )
        reversal = sorted(
            [row for row in eligible if row["ret10"] > 0], key=lambda row: (_distance(row, center), row["code"], row["ymd"])
        )
        chosen: list[dict[str, Any]] = []
        for role, candidates in (("downside_followthrough", continuation), ("upside_reversal", reversal)):
            candidate = next((row for row in candidates if row["code"] not in used_codes), candidates[0] if candidates else None)
            if candidate is not None:
                chosen.append(_compact(candidate, role, _distance(candidate, center)))
                used_codes.add(candidate["code"])
        casebook[archetype] = {
            "selection_basis": "nearest observable-feature prototype, paired by opposite 10-day outcome; outcome is for case contrast, not signal fitting",
            "eligible_events": len(eligible),
            "eligible_codes": len({row["code"] for row in eligible}),
            "feature_center": center,
            "cases": chosen,
        }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "universe": "current Nikkei225 registry; survivorship-biased research slice",
            "period": [20260101, 20260713],
            "changed_axis": "representative case sampling only",
            "archetypes": list(rules),
            "event_cooldown_bars": 20,
            "selection": "median-centered observable geometry; one downside and one upside outcome per archetype",
            "costs": "ignored by user rule",
        },
        "source_ledger": str(input_csv),
        "casebook": casebook,
        "decision": {
            "candidate_local_decision": "keep_casebook_for_manual_chart_reading",
            "authoritative_rollup_decision": "review_only",
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(output / "compare.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_representative_casebook_v1"))
    args = parser.parse_args()
    print(run(args.input_csv, args.output_root))


if __name__ == "__main__":
    main()
