from __future__ import annotations

import argparse
import ast
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_nikkei225_case_pair_sequence_contrast_v1"


def _bool(value: str) -> bool:
    return str(value).lower() == "true"


def _float(value: str) -> float | None:
    return float(value) if value not in (None, "") else None


def _mapping(value: str) -> dict[str, float]:
    return {str(key): float(score) for key, score in ast.literal_eval(value or "{}").items()}


def _window_features(window: list[dict[str, Any]]) -> dict[str, Any]:
    prior = window[:-1]
    event = window[-1]
    return {
        "prior_bearish_bars": sum(row["c"] < row["o"] for row in prior),
        "prior_bullish_bars": sum(row["c"] > row["o"] for row in prior),
        "prior_upper_wick_supply_bars": sum(row["upper_wick"] >= 0.25 and row["close_pos"] <= 0.55 for row in prior),
        "prior_lower_wick_reversal_bars": sum(row["lower_wick"] >= 0.35 and row["close_pos"] >= 0.60 for row in prior),
        "prior_closes_below_ma7": sum(row["c"] < row["ma7"] for row in prior),
        "prior_closes_below_ma20": sum(row["c"] < row["ma20"] for row in prior),
        "prior_failed_rebound_ma7": sum(row["failed_rebound"] for row in prior),
        "prior_gap_down_2pct": sum(row["gap_down"] for row in prior),
        "prior_support_to_resistance": sum(row["support_to_resistance"] for row in prior),
        "prior_bearish_full_retrace": sum(row["bearish_full_retrace"] for row in prior),
        "prior_sell_add_score": sum(sum(row["adds"].values()) for row in prior),
        "prior_sell_deduct_score": sum(sum(row["deducts"].values()) for row in prior),
        "event_body_ratio": event["body"],
        "event_upper_wick_ratio": event["upper_wick"],
        "event_lower_wick_ratio": event["lower_wick"],
        "event_close_pos": event["close_pos"],
        "event_dist_ma7": event["dist_ma7"],
        "event_dist_ma20": event["dist_ma20"],
        "event_dist_ma60": event["dist_ma60"],
        "event_atr_distance_ma20": (event["ma20"] - event["c"]) / event["atr14"] if event["atr14"] else None,
        "event_volume_ratio20": event["v"] / event["vol20"] if event["vol20"] else None,
        "event_rebound_risk": event["rebound_risk"],
        "event_sell_score": event["sell_score"],
        "event_net_sell_score": event["net_sell_score"],
    }


def run(sequence_csv: Path, casebook_json: Path, output_root: Path) -> Path:
    casebook = json.loads(casebook_json.read_text(encoding="utf-8"))
    targets = {
        (case["code"], int(case["ymd"])): {"archetype": archetype, "role": case["role"], "ret10": case["ret10_forward"]}
        for archetype, group in casebook["casebook"].items()
        for case in group["cases"]
    }
    by_code: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with sequence_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            code = str(raw["code"])
            if code not in {key[0] for key in targets}:
                continue
            by_code[code].append({
                "code": code, "ymd": int(raw["ymd"]), "o": float(raw["o"]), "h": float(raw["h"]),
                "l": float(raw["l"]), "c": float(raw["c"]), "v": float(raw["v"] or 0),
                "ma7": float(raw["ma7"]), "ma20": float(raw["ma20"]), "ma60": float(raw["ma60"]),
                "atr14": float(raw["atr14"] or 0), "vol20": float(raw["vol20"] or 0),
                "body": float(raw["body_ratio"] or 0), "upper_wick": float(raw["upper_wick_ratio"] or 0),
                "lower_wick": float(raw["lower_wick_ratio"] or 0), "close_pos": float(raw["close_pos"] or 0),
                "dist_ma7": float(raw["dist_ma7"] or 0), "dist_ma20": float(raw["dist_ma20"] or 0),
                "dist_ma60": float(raw["dist_ma60"] or 0), "failed_rebound": _bool(raw["weak_rebound_second_break"]),
                "gap_down": _bool(raw["gap_down_2pct"]), "support_to_resistance": _bool(raw["support_to_resistance"]),
                "bearish_full_retrace": _bool(raw["bearish_full_retrace"]), "adds": _mapping(raw["sell_flow_additions"]),
                "deducts": _mapping(raw["sell_flow_deductions"]), "rebound_risk": float(raw["rebound_risk_score"] or 0),
                "sell_score": float(raw["sell_flow_score"] or 0), "net_sell_score": float(raw["net_sell_score"] or 0),
            })
    cases: list[dict[str, Any]] = []
    for code, rows in by_code.items():
        rows.sort(key=lambda row: row["ymd"])
        position = {row["ymd"]: index for index, row in enumerate(rows)}
        for (target_code, ymd), metadata in targets.items():
            if target_code != code or ymd not in position:
                continue
            index = position[ymd]
            window = rows[max(0, index - 10): index + 1]
            if len(window) != 11:
                continue
            cases.append({"code": code, "ymd": ymd, **metadata, "features": _window_features(window)})

    numeric_fields = sorted(cases[0]["features"]) if cases else []
    contrasts: dict[str, Any] = {}
    for field in numeric_fields:
        down = [case["features"][field] for case in cases if case["role"] == "downside_followthrough" and case["features"][field] is not None]
        up = [case["features"][field] for case in cases if case["role"] == "upside_reversal" and case["features"][field] is not None]
        if not down or not up:
            continue
        pair_directions = []
        for archetype in sorted({case["archetype"] for case in cases}):
            left = next((case["features"][field] for case in cases if case["archetype"] == archetype and case["role"] == "downside_followthrough"), None)
            right = next((case["features"][field] for case in cases if case["archetype"] == archetype and case["role"] == "upside_reversal"), None)
            if left is not None and right is not None:
                pair_directions.append(1 if left > right else -1 if left < right else 0)
        contrasts[field] = {
            "downside_mean": sum(down) / len(down), "reversal_mean": sum(up) / len(up),
            "difference": sum(down) / len(down) - sum(up) / len(up),
            "pair_direction_counts": {"downside_higher": pair_directions.count(1), "reversal_higher": pair_directions.count(-1), "tie": pair_directions.count(0)},
        }
    ranked = sorted(
        contrasts,
        key=lambda field: (max(contrasts[field]["pair_direction_counts"]["downside_higher"], contrasts[field]["pair_direction_counts"]["reversal_higher"]), abs(contrasts[field]["difference"])),
        reverse=True,
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative",
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {"sample": "fixed representative casebook", "lookback_bars": 10, "changed_axis": "none; descriptive contrast only", "lookahead_use": "role labels only, never feature computation"},
        "source_casebook": str(casebook_json), "source_sequence_ledger": str(sequence_csv),
        "cases": cases, "feature_contrasts": contrasts, "ranked_consistency": ranked,
        "decision": {"candidate_local_decision": "descriptive_only_choose_one_axis_next", "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(output / "compare.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequence-csv", required=True, type=Path)
    parser.add_argument("--casebook-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_case_pair_sequence_contrast_v1"))
    args = parser.parse_args()
    print(run(args.sequence_csv, args.casebook_json, args.output_root))


if __name__ == "__main__":
    main()
