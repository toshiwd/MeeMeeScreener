from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_2026_decline_reading_validation_v1 import _load_point_in_time_case


AXIS_ID = "tradex_nikkei225_case_window_ledger_v1"


def _support_features(rows: list[dict[str, Any]], index: int) -> dict[str, Any]:
    row = rows[index]
    prior = rows[max(0, index - 30):index]
    atr = float(row["atr14"] or 0)
    prior_lows = [float(item["l"]) for item in prior]
    support = min(prior_lows[-20:]) if prior_lows else None
    touches = 0 if support is None or atr <= 0 else sum(abs(low - support) <= 0.35 * atr for low in prior_lows[-20:])
    return {
        "support_level_20": support,
        "support_touch_count_20_atr035": touches,
        "support_break_close": bool(support is not None and float(row["c"]) < support),
    }


def _compact(rows: list[dict[str, Any]], index: int, relative: int, metadata: dict[str, Any]) -> dict[str, Any]:
    row = rows[index]
    previous = rows[index - 1] if index else row
    prior5 = rows[max(0, index - 5):index]
    prior10 = rows[max(0, index - 10):index]
    support = _support_features(rows, index)
    volume_ratio = float(row["v"]) / float(row["vol20"]) if row.get("vol20") else None
    return {
        "archetype": metadata["archetype"], "role": metadata["role"], "code": row["code"],
        "event_ymd": metadata["ymd"], "ymd": row["ymd"], "relative_bar": relative,
        "source": row["source"], "bar_status": row["bar_status"],
        "o": row["o"], "h": row["h"], "l": row["l"], "c": row["c"], "v": row["v"],
        "candle": "bull" if row["c"] > row["o"] else "bear" if row["c"] < row["o"] else "doji",
        "body_ratio": row["body_ratio"], "upper_wick_ratio": row["upper_wick_ratio"],
        "lower_wick_ratio": row["lower_wick_ratio"], "close_pos": row["close_pos"],
        "gap_pct": float(row["o"]) / float(previous["c"]) - 1 if float(previous["c"]) else None,
        "day_ret": float(row["c"]) / float(previous["c"]) - 1 if float(previous["c"]) else None,
        "range_atr": (float(row["h"]) - float(row["l"])) / float(row["atr14"]) if row.get("atr14") else None,
        "volume_ratio20": volume_ratio,
        "ma7": row["ma7"], "ma20": row["ma20"], "ma60": row["ma60"], "ma100": row["ma100"], "ma200": row["ma200"],
        "dist_ma7": row["dist_ma7"], "dist_ma20": row["dist_ma20"], "dist_ma60": row["dist_ma60"],
        "ma7_slope_pct": float(row["ma7"]) / float(row["ma7_prev"]) - 1 if row.get("ma7_prev") else None,
        "ma20_slope_pct": float(row["ma20"]) / float(row["ma20_prev"]) - 1 if row.get("ma20_prev") else None,
        "ma60_slope_pct": float(row["ma60"]) / float(row["ma60_prev"]) - 1 if row.get("ma60_prev") else None,
        "cross_ma7": row["cross_ma7"], "cross_ma20": row["cross_ma20"], "failed_rebound_ma7": row["failed_rebound_ma7"],
        "break_low20": row["break_low20"], "pos60": row["pos60"], "ret3": row["ret3"], "ret5": row["ret5"], "ret10": row["ret10"],
        "prior5_bear_count": sum(item["c"] < item["o"] for item in prior5),
        "prior10_bear_count": sum(item["c"] < item["o"] for item in prior10),
        "prior5_upper_supply_count": sum(item["upper_wick_ratio"] >= 0.25 and item["close_pos"] <= 0.55 for item in prior5),
        "prior5_lower_rejection_count": sum(item["lower_wick_ratio"] >= 0.35 and item["close_pos"] >= 0.60 for item in prior5),
        "prior5_below_ma20_count": sum(item["c"] < item["ma20"] for item in prior5),
        **support,
    }


def run(db_path: Path, casebook_json: Path, output_root: Path) -> Path:
    casebook = json.loads(casebook_json.read_text(encoding="utf-8"))
    targets = [
        {"code": case["code"], "ymd": int(case["ymd"]), "archetype": archetype, "role": case["role"]}
        for archetype, group in casebook["casebook"].items() for case in group["cases"]
    ]
    ledger: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    for target in targets:
        rows = _load_point_in_time_case(db_path, target["code"])
        positions = {int(row["ymd"]): index for index, row in enumerate(rows)}
        event_index = positions.get(target["ymd"])
        if event_index is None:
            audits.append({**target, "status": "event_missing"})
            continue
        start, end = event_index - 20, event_index + 10
        if start < 0 or end >= len(rows):
            audits.append({**target, "status": "insufficient_window", "available_before": event_index, "available_after": len(rows) - event_index - 1})
            continue
        case_rows = [_compact(rows, index, index - event_index, target) for index in range(start, end + 1)]
        ledger.extend(case_rows)
        audits.append({**target, "status": "complete", "rows": len(case_rows), "confirmed_rows": sum(row["bar_status"] == "confirmed" for row in case_rows), "provisional_rows": sum(row["bar_status"] != "confirmed" for row in case_rows)})
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    if ledger:
        with (output / "case_window_ledger.csv").open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(ledger[0]))
            writer.writeheader(); writer.writerows(ledger)
    complete = len(audits) == 10 and all(item["status"] == "complete" and item["rows"] == 31 for item in audits)
    payload = {
        "schema_version": f"{AXIS_ID}.audit.v1", "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(), "runtime_db": str(db_path),
        "runtime_db_sha256": hashlib.sha256(db_path.read_bytes()).hexdigest(), "source_casebook": str(casebook_json),
        "window_contract": {"before_bars": 20, "event_bar": 1, "after_bars": 10, "point_in_time_features": True, "pan_first_yahoo_fallback": True},
        "case_audits": audits, "requested_cases": 10, "complete_cases": sum(item["status"] == "complete" for item in audits),
        "ledger_rows": len(ledger), "complete": complete,
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    (output / "audit.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": complete, "audit": str(output / "audit.json")}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--casebook-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_case_window_ledger_v1"))
    args = parser.parse_args()
    print(run(args.db, args.casebook_json, args.output_root))


if __name__ == "__main__":
    main()
