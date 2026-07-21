from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_2026_decline_reading_validation_v1 import _load_point_in_time_case


AXIS_ID = "tradex_nikkei225_case_fine_reading_v1"


def _period_bars(rows: list[dict[str, Any]], period: str) -> list[dict[str, float]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        date = datetime.strptime(str(row["ymd"]), "%Y%m%d")
        key = date.strftime("%G%V") if period == "week" else date.strftime("%Y%m")
        grouped[key].append(row)
    output = []
    for key in sorted(grouped):
        part = grouped[key]
        high, low = max(float(row["h"]) for row in part), min(float(row["l"]) for row in part)
        op, close = float(part[0]["o"]), float(part[-1]["c"])
        rng = max(high - low, 1e-9)
        output.append({"key": key, "o": op, "h": high, "l": low, "c": close, "v": sum(float(row["v"]) for row in part), "upper": (high - max(op, close)) / rng, "lower": (min(op, close) - low) / rng, "close_pos": (close - low) / rng})
    return output


def _summary(rows: list[dict[str, Any]], event_index: int) -> dict[str, Any]:
    row = rows[event_index]
    prior20 = rows[event_index - 20:event_index]
    prior10 = rows[event_index - 10:event_index]
    prior5 = rows[event_index - 5:event_index]
    atr = float(row["atr14"] or 1e-9)
    high20, low20 = max(float(item["h"]) for item in prior20), min(float(item["l"]) for item in prior20)
    pos20 = (float(row["c"]) - low20) / max(high20 - low20, 1e-9)
    bull = [item for item in prior10 if item["c"] > item["o"]]
    largest_bull = max(bull, key=lambda item: float(item["c"]) - float(item["o"])) if bull else None
    support = low20
    touch20 = sum(abs(float(item["l"]) - support) <= 0.5 * float(item["atr14"] or atr) or abs(float(item["c"]) - support) <= 0.5 * float(item["atr14"] or atr) for item in prior20)
    break_candidates = [index for index in range(max(1, event_index - 5), event_index) if float(rows[index - 1]["c"]) >= float(rows[index - 1]["ma20"]) and float(rows[index]["c"]) < float(rows[index]["ma20"])]
    first_break = break_candidates[0] if break_candidates else None
    failed_rebound_rebreak = False
    support_to_resistance = False
    if first_break is not None:
        broken_ma = float(rows[first_break]["ma20"])
        between = rows[first_break + 1:event_index]
        failed_rebound_rebreak = bool(between and max(float(item["c"]) for item in between) < broken_ma and float(row["c"]) < min(float(item["l"]) for item in rows[first_break:event_index]))
        support_to_resistance = any(float(item["h"]) >= support - 0.5 * float(item["atr14"] or atr) and float(item["c"]) < support for item in between + [row])
    streak60 = 0
    for item in reversed(rows[:event_index]):
        if float(item["c"]) > float(item["ma60"]): streak60 += 1
        else: break
    weeks = _period_bars(rows[:event_index], "week")
    months = _period_bars(rows[:event_index], "month")
    week = weeks[-1] if weeks else None; previous_week = weeks[-2] if len(weeks) >= 2 else None
    month = months[-1] if months else None; previous_months = months[-4:-1]
    monthly_closes = [item["c"] for item in months]
    monthly_ma12 = sum(monthly_closes[-12:]) / 12 if len(monthly_closes) >= 12 else None
    monthly_ma12_prev = sum(monthly_closes[-13:-1]) / 12 if len(monthly_closes) >= 13 else None
    vol_ratio = float(row["v"]) / float(row["vol20"]) if row.get("vol20") else None
    ranges = []
    for index in range(max(19, event_index - 20), event_index + 1):
        window = rows[index - 19:index + 1]
        ranges.append((max(float(item["h"]) for item in window) - min(float(item["l"]) for item in window)) / float(rows[index]["c"]))
    compression_run = 0
    for value in reversed(ranges):
        if value <= 0.10: compression_run += 1
        else: break
    return {
        "01_pos20": pos20,
        "02_pre_ret10": float(row["c"]) / float(rows[event_index - 10]["c"]) - 1,
        "03_largest_bull_full_retrace": bool(largest_bull and float(row["c"]) <= float(largest_bull["o"])),
        "04_bear_count5": sum(item["c"] < item["o"] for item in prior5),
        "05_bear_body5_atr": sum(max(float(item["o"]) - float(item["c"]), 0) for item in prior5) / atr,
        "06_failed_rebound_rebreak": failed_rebound_rebreak,
        "07_week_lower_high": bool(week and previous_week and week["h"] < previous_week["h"] and week["c"] < previous_week["c"]),
        "08_week_upper_supply": bool(week and week["upper"] >= 0.25 and week["close_pos"] <= 0.50),
        "09_week_lower_rejection": bool(week and week["lower"] >= 0.35 and week["close_pos"] >= 0.60),
        "10_month_high_failure": bool(month and previous_months and month["h"] >= max(item["h"] for item in previous_months) and month["c"] < month["o"] and month["close_pos"] <= 0.40),
        "11_month_above_rising_ma12": bool(monthly_ma12 and monthly_ma12_prev and month and month["c"] >= monthly_ma12 >= monthly_ma12_prev),
        "12_support_touch20": touch20,
        "13_support_break_depth_atr": (support - float(row["c"])) / atr,
        "14_support_to_resistance": support_to_resistance,
        "15_support_reclaim": bool(float(rows[event_index - 1]["c"]) < support <= float(row["c"]) and row["close_pos"] >= 0.60),
        "16_above_ma60_streak": streak60,
        "17_cross_ma7": bool(row["cross_ma7"]),
        "18_cross_ma20": bool(row["cross_ma20"]),
        "19_ma7_slope5_atr": (float(row["ma7"]) - float(rows[event_index - 5]["ma7"])) / (5 * atr),
        "20_ma20_slope5_atr": (float(row["ma20"]) - float(rows[event_index - 5]["ma20"])) / (5 * atr),
        "21_ma60_slope5_atr": (float(row["ma60"]) - float(rows[event_index - 5]["ma60"])) / (5 * atr),
        "22_ma7_reclaim": bool(float(rows[event_index - 1]["c"]) < float(rows[event_index - 1]["ma7"]) and float(row["c"]) >= float(row["ma7"])),
        "23_ma20_reclaim": bool(float(rows[event_index - 1]["c"]) < float(rows[event_index - 1]["ma20"]) and float(row["c"]) >= float(row["ma20"])),
        "24_upper_wick_supply": bool(row["upper_wick_ratio"] >= 0.25 and row["close_pos"] <= 0.50),
        "25_lower_wick_reversal": bool(row["lower_wick_ratio"] >= 0.35 and row["close_pos"] >= 0.60),
        "26_volume_ratio20": vol_ratio,
        "27_volume_break": bool(vol_ratio is not None and vol_ratio >= 1.5 and (row["cross_ma7"] or row["cross_ma20"] or float(row["c"]) < support)),
        "28_range20_pct": (max(high20, float(row["h"])) - min(low20, float(row["l"]))) / float(row["c"]),
        "29_compression_run": compression_run,
        "30_oversold_risk": bool(row["ret3"] <= -0.05 or row["ret5"] <= -0.08 or (float(row["c"]) - float(row["ma7"])) / atr <= -1.5 or (float(row["c"]) - float(row["ma20"])) / atr <= -2.0),
    }


def run(db_path: Path, casebook_json: Path, output_root: Path) -> Path:
    casebook = json.loads(casebook_json.read_text(encoding="utf-8"))
    cases = []
    for archetype, group in casebook["casebook"].items():
        for case in group["cases"]:
            rows = _load_point_in_time_case(db_path, case["code"])
            positions = {int(row["ymd"]): index for index, row in enumerate(rows)}
            index = positions.get(int(case["ymd"]))
            if index is None or index < 20 or index + 10 >= len(rows):
                raise ValueError(f"incomplete case window: {case['code']}@{case['ymd']}")
            cases.append({"code": case["code"], "ymd": int(case["ymd"]), "archetype": archetype, "role": case["role"], "ret10_forward": case["ret10_forward"], "features": _summary(rows, index)})
    fields = list(cases[0]["features"])
    contrasts = {}
    for field in fields:
        pairs = []
        for archetype in sorted({case["archetype"] for case in cases}):
            down = next(case for case in cases if case["archetype"] == archetype and case["role"] == "downside_followthrough")
            up = next(case for case in cases if case["archetype"] == archetype and case["role"] == "upside_reversal")
            left, right = down["features"][field], up["features"][field]
            pairs.append({"archetype": archetype, "downside": left, "reversal": right, "direction": "downside_higher" if left > right else "reversal_higher" if left < right else "tie"})
        contrasts[field] = {"pairs": pairs, "direction_counts": {name: sum(pair["direction"] == name for pair in pairs) for name in ("downside_higher", "reversal_higher", "tie")}}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {"cases": 10, "daily_lookback": 20, "daily_outcome_window": 10, "feature_time": "event close or earlier", "outcome_not_used_in_features": True, "weekly_monthly": "completed periods before event date"},
        "source_casebook": str(casebook_json), "runtime_db": str(db_path), "cases": cases, "feature_contrasts": contrasts,
        "quality_gate": {"expected_cases": 10, "actual_cases": len(cases), "features_per_case": len(fields), "all_pass": len(cases) == 10 and len(fields) == 30},
        "decision": {"candidate_local_decision": "fine_reading_complete_choose_one_axis_next", "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    (output / "compare.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": payload["quality_gate"]["all_pass"], "compare": str(output / "compare.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--casebook-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_case_fine_reading_v1"))
    args = parser.parse_args(); print(run(args.db, args.casebook_json, args.output_root))


if __name__ == "__main__": main()
