"""Build a 20-session path inventory for the selected early short-entry fusion."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _load_daily


HORIZON = 20


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def first_day(values: list[float], predicate) -> int | None:
    for day, value in enumerate(values, start=1):
        if predicate(value):
            return day
    return None


def classify_path(row: dict) -> str:
    if (
        row["low_5d_pct"] <= -3.0
        and row["high_20d_pct"] >= 8.0
        and row["close_20d_pct"] >= 5.0
    ):
        return "ImmediateDropThenReverse"
    if row["low_5d_pct"] <= -3.0:
        return "ImmediateDrop"
    if row["max_high_before_5pct_drop_pct"] >= 3.0 and row["low_20d_pct"] <= -5.0:
        return "ReboundThenDrop"
    if (
        row["close_abs_max_10d_pct"] <= 3.0
        and row["low_day6_20_pct"] <= -5.0
    ):
        return "SidewaysThenDrop"
    if (
        row["low_20d_pct"] > -3.0
        and row["high_20d_pct"] >= 8.0
        and row["close_20d_pct"] >= 5.0
    ):
        return "TrueUpsideReversal"
    if (
        row["low_20d_pct"] > -5.0
        and row["high_20d_pct"] < 5.0
        and abs(row["close_20d_pct"]) <= 3.0
    ):
        return "TimeHold"
    return "OtherPath"


def build(events: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    histories = {
        str(code): group.sort_values("ymd").reset_index(drop=True)
        for code, group in daily.groupby("code", sort=False)
    }
    output = []
    for event in events.itertuples(index=False):
        history = histories.get(str(event.code))
        if history is None:
            continue
        matches = history.index[history.ymd.eq(int(event.signal_ymd))]
        if len(matches) == 0:
            continue
        signal_idx = int(matches[-1])
        future = history.iloc[signal_idx + 1:signal_idx + 1 + HORIZON].copy()
        if len(future) < HORIZON:
            continue
        entry = float(future.iloc[0].o)
        signal = history.iloc[signal_idx]
        previous = history.iloc[signal_idx - 1]
        lows = (100.0 * (future.l.astype(float) / entry - 1.0)).tolist()
        highs = (100.0 * (future.h.astype(float) / entry - 1.0)).tolist()
        closes = (100.0 * (future.c.astype(float) / entry - 1.0)).tolist()
        opens = (100.0 * (future.o.astype(float) / entry - 1.0)).tolist()
        drop5_day = first_day(lows, lambda value: value <= -5.0)
        max_high_before_drop5 = (
            max(highs) if drop5_day is None else max(highs[:drop5_day])
        )
        signal_high = float(signal.h)
        previous_close = float(previous.c)
        close_signal_high_days = [
            day for day, value in enumerate(future.c.astype(float), start=1)
            if value > signal_high
        ]
        close_previous_close_days = [
            day for day, value in enumerate(future.c.astype(float), start=1)
            if value > previous_close
        ]
        consecutive_bullish_higher = None
        for idx in range(1, len(future)):
            first = future.iloc[idx - 1]
            second = future.iloc[idx]
            if (
                float(first.c) > float(first.o)
                and float(second.c) > float(second.o)
                and float(second.h) > float(first.h)
                and float(second.l) > float(first.l)
            ):
                consecutive_bullish_higher = idx + 1
                break
        record = event._asdict()
        record.update({
            "entry_open_20d": entry,
            "signal_high_price": signal_high,
            "previous_close_price": previous_close,
            "future_open_pct": opens,
            "future_high_pct": highs,
            "future_low_pct": lows,
            "future_close_pct": closes,
            "low_5d_pct": float(min(lows[:5])),
            "low_10d_pct": float(min(lows[:10])),
            "low_20d_pct": float(min(lows)),
            "low_day6_20_pct": float(min(lows[5:])),
            "high_5d_pct": float(max(highs[:5])),
            "high_10d_pct": float(max(highs[:10])),
            "high_20d_pct": float(max(highs)),
            "close_5d_pct_20d": float(closes[4]),
            "close_10d_pct": float(closes[9]),
            "close_20d_pct": float(closes[19]),
            "close_abs_max_10d_pct": float(max(abs(value) for value in closes[:10])),
            "first_3pct_drop_day_20d": first_day(lows, lambda value: value <= -3.0),
            "first_5pct_drop_day_20d": drop5_day,
            "first_3pct_rise_day_20d": first_day(highs, lambda value: value >= 3.0),
            "first_5pct_rise_day_20d": first_day(highs, lambda value: value >= 5.0),
            "first_8pct_rise_day_20d": first_day(highs, lambda value: value >= 8.0),
            "max_high_before_5pct_drop_pct": float(max_high_before_drop5),
            "first_close_above_signal_high_day": (
                close_signal_high_days[0] if close_signal_high_days else None
            ),
            "first_close_above_previous_close_day": (
                close_previous_close_days[0] if close_previous_close_days else None
            ),
            "first_two_bullish_higher_day": consecutive_bullish_higher,
        })
        record["path_class"] = classify_path(record)
        output.append(record)
    return pd.DataFrame(output)


def summarize(frame: pd.DataFrame) -> dict:
    rows = {}
    for name, group in frame.groupby("path_class"):
        rows[str(name)] = {
            "n": int(len(group)),
            "rate": float(len(group) / len(frame)),
            "median_low_20d_pct": float(group.low_20d_pct.median()),
            "median_high_20d_pct": float(group.high_20d_pct.median()),
            "median_close_20d_pct": float(group.close_20d_pct.median()),
        }
    years = {
        str(int(year)): {
            "n": int(len(group)),
            "classes": {
                str(name): int(len(rows))
                for name, rows in group.groupby("path_class")
            },
        }
        for year, group in frame.assign(year=frame.signal_ymd // 10000).groupby("year")
    }
    return {"n": int(len(frame)), "classes": rows, "years": years}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    ledger = build(pd.read_parquet(args.events), _load_daily(args.db, None))
    ledger_path = args.output / "path_inventory_20d.parquet"
    ledger.to_parquet(ledger_path, index=False)
    summary = summarize(ledger)
    checks = {
        "n_ge_100": summary["n"] >= 100,
        "immediate_drop_exists": summary["classes"].get("ImmediateDrop", {}).get("n", 0) > 0,
        "delayed_drop_exists": (
            summary["classes"].get("ReboundThenDrop", {}).get("n", 0)
            + summary["classes"].get("SidewaysThenDrop", {}).get("n", 0)
        ) > 0,
        "true_reversal_exists": summary["classes"].get("TrueUpsideReversal", {}).get("n", 0) > 0,
    }
    result = {
        "schema_version": "tradex_short_20d_path_inventory_v1.compare.v1",
        "artifact_role": "authoritative_short_20d_path_inventory",
        "review_only": True,
        "research_phase": "branching_generation",
        "fixed_conditions": {
            "selector": "kept short initial entry fusion v1",
            "entry": "next session open",
            "horizon_sessions": HORIZON,
            "costs": "ignored",
            "future_selection_columns": [],
            "classes": {
                "ImmediateDrop": "low within days1-5 <= -3%",
                "ImmediateDropThenReverse": (
                    "low within days1-5 <=-3%; then 20d high >=8% and close >=5%"
                ),
                "ReboundThenDrop": "not immediate; >=3% rise before <=-5% low by day20",
                "SidewaysThenDrop": "not immediate; first10 closes within +/-3%; day6-20 low <=-5%",
                "TrueUpsideReversal": "20d low >-3%; high >=8%; day20 close >=5%",
                "TimeHold": "20d low >-5%; high <5%; day20 close within +/-3%",
                "OtherPath": "remaining paths",
            },
        },
        "authoritative_result": {"inventory": summary, "gate_checks": checks},
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(ledger)),
            "selection_divergence_reason": "outcome-only 20-session path classification",
            "class_counts": {
                name: row["n"] for name, row in summary["classes"].items()
            },
        },
        "judgment": {
            "candidate_local_decision": "keep" if all(checks.values()) else "hold",
            "session_aggregate_decision": (
                "keep_20d_path_inventory" if all(checks.values()) else "hold_path_inventory"
            ),
            "authoritative_rollup_decision": (
                "keep_short_20d_path_inventory_v1_review_only"
                if all(checks.values()) else "hold"
            ),
            "reason_type": (
                "all_operational_path_classes_are_instrumented"
                if all(checks.values()) else "one_or_more_path_classes_missing"
            ),
        },
        "not_changed": ["selector", "MeeMee", "ranking", "runtime DB", "production logic"],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "db": {"path": str(args.db.resolve()), "read_only": True},
            "events": {"path": str(args.events.resolve()), "sha256": sha(args.events)},
        },
        "compare_sha256": sha(compare),
        "ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
