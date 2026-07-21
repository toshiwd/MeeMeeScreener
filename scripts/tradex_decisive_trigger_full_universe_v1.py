from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_origin5541_decisive_trigger_v1 as micro


AXIS_ID = "decisive_trigger_full_universe_recent_first_v1"
SCHEMA_VERSION = "tradex_decisive_trigger_full_universe_v1.compare.v1"
LOOKBACK_START = "2023-07-01"
EVALUATION_START = "2024-01-01"
CONFIRMED_END = "2026-07-17"
EVENT_TYPES = (
    "BUY_DECISIVE_INITIAL",
    "BUY_DECISIVE_CONTINUATION",
    "SELL_DECISIVE_RETURN_SELL",
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def load_universe(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        frame = conn.execute(
            """
            SELECT code, strftime(to_timestamp(date), '%Y-%m-%d') AS trade_date,
                   o, h, l, c, v, source
            FROM daily_bars
            WHERE source = 'pan'
              AND date BETWEEN epoch(TIMESTAMP '2023-07-01') AND epoch(TIMESTAMP '2026-07-17')
            ORDER BY code, date
            """
        ).fetchdf()
    if frame.empty:
        raise RuntimeError("No confirmed PAN bars for decisive-trigger validation")
    if frame.duplicated(["code", "trade_date"]).any():
        raise RuntimeError("Duplicate code/trade_date rows in confirmed PAN input")
    return frame


def _future_extreme(series: pd.Series, window: int, mode: str) -> pd.Series:
    shifted = series.shift(-1)
    reversed_series = shifted.iloc[::-1]
    roller = reversed_series.rolling(window, min_periods=window)
    result = roller.max() if mode == "max" else roller.min()
    return result.iloc[::-1]


def classify_code(group: pd.DataFrame) -> pd.DataFrame:
    ordered = group.sort_values("trade_date").reset_index(drop=True)
    features = micro.build_features(ordered.drop(columns=["code"]))
    classified = micro.classify(features)
    classified.insert(0, "code", str(ordered.iloc[0]["code"]))
    for horizon in (5, 10, 20):
        classified[f"future_close_{horizon}"] = classified["c"].shift(-horizon)
    classified["future_high_20"] = _future_extreme(classified["h"], 20, "max")
    classified["future_low_20"] = _future_extreme(classified["l"], 20, "min")
    return classified


def build_event_ledger(bars: pd.DataFrame) -> pd.DataFrame:
    classified = pd.concat(
        [classify_code(group) for _, group in bars.groupby("code", sort=False)],
        ignore_index=True,
    )
    events = classified[
        (classified["trade_date"] >= EVALUATION_START)
        & classified["decision"].isin(EVENT_TYPES)
    ].copy()
    events["event_type"] = events["decision"]
    events["side"] = events["event_type"].map(
        {
            "BUY_DECISIVE_INITIAL": "buy",
            "BUY_DECISIVE_CONTINUATION": "buy",
            "SELL_DECISIVE_RETURN_SELL": "sell",
        }
    )
    for horizon in (5, 10, 20):
        long_return = events[f"future_close_{horizon}"] / events["c"] - 1
        short_return = events["c"] / events[f"future_close_{horizon}"] - 1
        events[f"directional_ret{horizon}"] = long_return.where(events["side"] == "buy", short_return)
    buy_mfe = events["future_high_20"] / events["c"] - 1
    sell_mfe = events["c"] / events["future_low_20"] - 1
    buy_adverse = events["future_low_20"] / events["c"] - 1
    sell_adverse = 1 - events["future_high_20"] / events["c"]
    events["directional_mfe20"] = buy_mfe.where(events["side"] == "buy", sell_mfe)
    events["directional_adverse20"] = buy_adverse.where(events["side"] == "buy", sell_adverse)
    events["outcome_complete20"] = events["future_close_20"].notna()
    events["event_year"] = events["trade_date"].str.slice(0, 4).astype(int)
    return events


def summarize(group: pd.DataFrame) -> dict[str, Any]:
    complete = group[group["outcome_complete20"]].copy()
    result: dict[str, Any] = {
        "event_count": int(len(group)),
        "complete20_count": int(len(complete)),
        "incomplete_recent_count": int(len(group) - len(complete)),
        "symbol_count": int(group["code"].nunique()),
    }
    for horizon in (5, 10, 20):
        values = complete[f"directional_ret{horizon}"].dropna()
        result[f"directional_ret{horizon}_mean_pct"] = None if values.empty else float(values.mean() * 100)
        result[f"directional_ret{horizon}_median_pct"] = None if values.empty else float(values.median() * 100)
        result[f"directional_ret{horizon}_win_rate"] = None if values.empty else float((values > 0).mean())
    result["directional_ret20_ge_5pct_rate"] = None if complete.empty else float((complete["directional_ret20"] >= 0.05).mean())
    ret20 = complete["directional_ret20"].dropna()
    if ret20.empty:
        result["directional_ret20_trim5_mean_pct"] = None
        result["directional_ret20_symbol_equal_mean_pct"] = None
        result["directional_ret20_p10_pct"] = None
        result["directional_ret20_p90_pct"] = None
    else:
        lower, upper = ret20.quantile(0.05), ret20.quantile(0.95)
        trimmed = ret20[(ret20 >= lower) & (ret20 <= upper)]
        symbol_equal = complete.groupby("code")["directional_ret20"].mean()
        result["directional_ret20_trim5_mean_pct"] = None if trimmed.empty else float(trimmed.mean() * 100)
        result["directional_ret20_symbol_equal_mean_pct"] = float(symbol_equal.mean() * 100)
        result["directional_ret20_p10_pct"] = float(ret20.quantile(0.10) * 100)
        result["directional_ret20_p90_pct"] = float(ret20.quantile(0.90) * 100)
    result["directional_mfe20_mean_pct"] = None if complete.empty else float(complete["directional_mfe20"].mean() * 100)
    result["directional_adverse20_mean_pct"] = None if complete.empty else float(complete["directional_adverse20"].mean() * 100)
    result["directional_adverse20_p10_pct"] = None if complete.empty else float(complete["directional_adverse20"].quantile(0.10) * 100)
    return result


def decision_for(summary: dict[str, Any]) -> tuple[str, dict[str, bool]]:
    gates = {
        "complete20_count_ge_100": summary["complete20_count"] >= 100,
        "directional_ret20_mean_gt_0": (summary["directional_ret20_mean_pct"] or -999) > 0,
        "directional_ret20_median_gt_0": (summary["directional_ret20_median_pct"] or -999) > 0,
        "directional_ret20_win_rate_ge_0_52": (summary["directional_ret20_win_rate"] or 0) >= 0.52,
        "directional_ret20_trim5_mean_gt_0": (summary["directional_ret20_trim5_mean_pct"] or -999) > 0,
        "directional_ret20_symbol_equal_mean_gt_0": (summary["directional_ret20_symbol_equal_mean_pct"] or -999) > 0,
    }
    return ("keep_review_only" if all(gates.values()) else "drop_or_hold"), gates


def build_compare(bars: pd.DataFrame, events: pd.DataFrame) -> dict[str, Any]:
    overall = {event_type: summarize(events[events["event_type"] == event_type]) for event_type in EVENT_TYPES}
    by_year = {
        event_type: {
            str(year): summarize(group)
            for year, group in events[events["event_type"] == event_type].groupby("event_year")
        }
        for event_type in EVENT_TYPES
    }
    decisions: dict[str, Any] = {}
    for event_type, summary in overall.items():
        local_decision, gates = decision_for(summary)
        years = by_year[event_type]
        completed_years = [row for row in years.values() if row["complete20_count"] >= 20]
        temporal_positive = bool(completed_years) and all((row["directional_ret20_mean_pct"] or -999) > 0 for row in completed_years)
        decisions[event_type] = {
            "candidate_local_decision": local_decision,
            "gates": gates,
            "completed_years_with_n_ge_20_all_positive_mean20": temporal_positive,
            "authoritative_decision": "keep_review_only" if local_decision == "keep_review_only" and temporal_positive else "drop",
        }
    kept = [key for key, value in decisions.items() if value["authoritative_decision"] == "keep_review_only"]
    quality = {
        "input_rows": int(len(bars)),
        "input_symbols": int(bars["code"].nunique()),
        "duplicate_code_date_rows": int(bars.duplicated(["code", "trade_date"]).sum()),
        "null_ohlcv_rows": int(bars[["o", "h", "l", "c", "v"]].isna().any(axis=1).sum()),
        "invalid_ohlcv_rows": int(((bars["h"] < bars[["o", "c", "l"]].max(axis=1)) | (bars["l"] > bars[["o", "c", "h"]].min(axis=1)) | (bars[["o", "h", "l", "c"]] <= 0).any(axis=1) | (bars["v"] < 0)).sum()),
        "event_duplicate_code_date_type_rows": int(events.duplicated(["code", "trade_date", "event_type"]).sum()),
        "confirmed_source_values": sorted(bars["source"].dropna().unique().tolist()),
        "min_date": str(bars["trade_date"].min()),
        "max_date": str(bars["trade_date"].max()),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "axis_id": AXIS_ID,
        "artifact_role": "authoritative_full_universe_recent_first_decisive_trigger_validation",
        "review_only": True,
        "fixed_conditions": {
            "universe": "all symbols with confirmed PAN bars in selected runtime DB",
            "lookback_start": LOOKBACK_START,
            "evaluation_start": EVALUATION_START,
            "confirmed_end": CONFIRMED_END,
            "trigger_thresholds": "unchanged from origin5541_decisive_trigger_v1",
            "execution_reference": "signal close",
            "outcomes": "directional close returns at 5,10,20 sessions plus 20-session favorable/adverse excursion",
            "costs_slippage_borrow": "ignored",
            "future_used_for_trigger": False,
            "future_used_for_outcome_only": True,
        },
        "data_quality": quality,
        "authoritative_results": overall,
        "by_year": by_year,
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "event-family validation, not ranking comparison",
            "event_counts": {event_type: int((events["event_type"] == event_type).sum()) for event_type in EVENT_TYPES},
            "symbol_counts": {event_type: int(events.loc[events["event_type"] == event_type, "code"].nunique()) for event_type in EVENT_TYPES},
        },
        "family_decisions": decisions,
        "judgment": {
            "candidate_local_decision": "keep_families_separately" if kept else "drop_all",
            "kept_families": kept,
            "session_aggregate_decision": "hold_review_only_no_meemee_reflection",
            "authoritative_rollup_decision": "hold_review_only_full_universe_recent_first_complete",
            "reason_type": "family_specific_effectiveness_gates_applied_without_threshold_retuning",
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production trading logic", "position sizing", "hedge ratios", "trigger thresholds"],
        "remaining_risks": [
            "current runtime symbol set creates survivorship bias for historically delisted names",
            "2026 is a partial year and recent events lack full 20-session outcomes",
            "monthly and weekly chart permission are not included",
            "event overlap and same-symbol clustering can overstate independent sample size",
            "outcome validation does not yet test hedge-release sizing or mental-load proxies",
        ],
    }


def run(db_path: Path, output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    bars = load_universe(db_path)
    events = build_event_ledger(bars)
    compare = build_compare(bars, events)
    ledger_path = output / "decisive_event_ledger.parquet"
    compare_path = output / "compare.json"
    audit_path = output / "audit.json"
    events.to_parquet(ledger_path, index=False)
    _write_json(compare_path, compare)
    _write_json(
        audit_path,
        {
            "schema_version": "tradex_decisive_trigger_full_universe_v1.audit.v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "db_path": str(db_path.resolve()),
            "db_read_only": True,
            "quality": compare["data_quality"],
            "event_rows": int(len(events)),
            "future_used_for_trigger": False,
            "review_only": True,
        },
    )
    _write_json(
        output / "_ARTIFACT_COMPLETE.json",
        {
            "complete": True,
            "authoritative": "compare.json",
            "compare_sha256": _sha256(compare_path),
            "audit_sha256": _sha256(audit_path),
            "ledger_sha256": _sha256(ledger_path),
        },
    )
    return {"output": str(output.resolve()), "judgment": compare["judgment"], "family_decisions": compare["family_decisions"]}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(json.dumps(run(args.db, args.output), ensure_ascii=False, indent=2))
