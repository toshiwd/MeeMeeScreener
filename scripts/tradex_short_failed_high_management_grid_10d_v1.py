from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from scripts.tradex_short_collapse_shape_10d_compare_v1 import (
    _load_daily,
    _pct,
    _rolling_max,
    _rolling_mean,
    _rolling_min,
    _round,
)
from scripts.tradex_short_top_failed_high_confirm_10d_compare_v1 import _confirmation_entries
from scripts.tradex_short_top_failed_high_market_weakness_10d_compare_v1 import _regime_rows
from scripts.tradex_short_top_failed_high_rr_10d_compare_v1 import _attach_rr
from scripts.tradex_short_top_first_failure_10d_compare_v1 import _features, _tags


AXIS_ID = "short_failed_high_management_grid_10d_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_failed_high_management_grid_10d_v1")
RR_THRESHOLD = 1.0
HORIZON = 10

MANAGEMENT_POLICIES: list[dict[str, Any]] = [
    {"policy_id": "hold_to_close10", "take_profit_pct": None, "stop_loss_pct": None},
    {"policy_id": "tp3_sl3", "take_profit_pct": 0.03, "stop_loss_pct": 0.03},
    {"policy_id": "tp3_sl5", "take_profit_pct": 0.03, "stop_loss_pct": 0.05},
    {"policy_id": "tp5_sl3", "take_profit_pct": 0.05, "stop_loss_pct": 0.03},
    {"policy_id": "tp5_sl5", "take_profit_pct": 0.05, "stop_loss_pct": 0.05},
    {"policy_id": "tp7_sl5", "take_profit_pct": 0.07, "stop_loss_pct": 0.05},
]

MARKET_FILTERS: dict[str, dict[str, Any]] = {
    "all_rr_ge_1": {"description": "RR>=1.0 only", "fn": lambda r: True},
    "weak_combo": {
        "description": "breadth_above_ma20 <= 0.45 and index_close_vs_ma20 <= 0",
        "fn": lambda r: _f(r.get("breadth_above_ma20"), 1.0) <= 0.45
        and _f(r.get("index_close_vs_ma20"), 1.0) <= 0.0,
    },
    "risk_off_combo": {
        "description": "regime_score <= -0.5 and index_close_vs_ma20 <= 0",
        "fn": lambda r: _f(r.get("regime_score")) <= -0.5 and _f(r.get("index_close_vs_ma20"), 1.0) <= 0.0,
    },
}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _evaluate_path(bars: list[dict[str, float]], signal_index: int, entry: dict[str, Any]) -> dict[str, Any] | None:
    entry_index = int(entry["entry_index"])
    if entry_index + HORIZON >= len(bars):
        return None
    entry_bar = bars[entry_index]
    entry_price = float(entry_bar["c"])
    if entry_price <= 0:
        return None
    future = bars[entry_index + 1 : entry_index + HORIZON + 1]
    stop_price = float(entry["stop_price"])
    lows = [float(row["l"]) for row in future]
    highs = [float(row["h"]) for row in future]
    closes = [float(row["c"]) for row in future]
    return {
        **entry,
        "signal_ymd": int(bars[signal_index]["ymd"]),
        "entry_ymd": int(entry_bar["ymd"]),
        "entry_price": entry_price,
        "stop_pct": stop_price / entry_price - 1,
        "close10_short_ret": (entry_price - closes[-1]) / entry_price,
        "mfe10_short": (entry_price - min(lows)) / entry_price,
        "mae10_short": (entry_price - max(highs)) / entry_price,
        "future_lows": lows,
        "future_highs": highs,
        "future_closes": closes,
    }


def _apply_management(row: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    entry = float(row["entry_price"])
    tp = policy["take_profit_pct"]
    sl = policy["stop_loss_pct"]
    if tp is None and sl is None:
        return {
            **row,
            "policy_id": policy["policy_id"],
            "managed_short_ret": row["close10_short_ret"],
            "managed_exit_day": HORIZON,
            "managed_first_event": "time_close10",
        }
    tp_price = entry * (1.0 - float(tp)) if tp is not None else None
    sl_price = entry * (1.0 + float(sl)) if sl is not None else None
    for day, (low, high) in enumerate(zip(row["future_lows"], row["future_highs"]), start=1):
        # Conservative intraday order: if both touch on the same day, count stop first.
        if sl_price is not None and float(high) >= sl_price:
            return {
                **row,
                "policy_id": policy["policy_id"],
                "managed_short_ret": -float(sl),
                "managed_exit_day": day,
                "managed_first_event": "stop_loss",
            }
        if tp_price is not None and float(low) <= tp_price:
            return {
                **row,
                "policy_id": policy["policy_id"],
                "managed_short_ret": float(tp),
                "managed_exit_day": day,
                "managed_first_event": "take_profit",
            }
    return {
        **row,
        "policy_id": policy["policy_id"],
        "managed_short_ret": row["close10_short_ret"],
        "managed_exit_day": HORIZON,
        "managed_first_event": "time_close10",
    }


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"event_count": 0}
    returns = [float(row["managed_short_ret"]) for row in rows]
    close10 = [float(row["close10_short_ret"]) for row in rows]
    by_month: dict[str, list[float]] = defaultdict(list)
    by_year: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        ymd = str(row["entry_ymd"])
        by_month[ymd[:6]].append(float(row["managed_short_ret"]))
        by_year[ymd[:4]].append(float(row["managed_short_ret"]))
    usable_months = [values for values in by_month.values() if len(values) >= 5]
    usable_years = [values for values in by_year.values() if len(values) >= 10]
    return {
        "event_count": len(rows),
        "unique_code_count": len({row["code"] for row in rows}),
        "managed_ret_mean": _round(mean(returns)),
        "managed_ret_median": _round(median(returns)),
        "managed_positive_rate": _round(sum(1 for value in returns if value > 0) / len(returns)),
        "managed_ret_p10": _round(_pct(returns, 0.10)),
        "managed_ret_p90": _round(_pct(returns, 0.90)),
        "baseline_close10_ret_mean": _round(mean(close10)),
        "delta_vs_close10_mean": _round(mean(returns) - mean(close10)),
        "positive_month_rate": _round(sum(1 for values in usable_months if mean(values) > 0) / len(usable_months)) if usable_months else None,
        "positive_year_rate": _round(sum(1 for values in usable_years if mean(values) > 0) / len(usable_years)) if usable_years else None,
        "usable_month_count": len(usable_months),
        "usable_year_count": len(usable_years),
        "take_profit_first_rate": _round(sum(1 for row in rows if row["managed_first_event"] == "take_profit") / len(rows)),
        "stop_loss_first_rate": _round(sum(1 for row in rows if row["managed_first_event"] == "stop_loss") / len(rows)),
        "time_exit_rate": _round(sum(1 for row in rows if row["managed_first_event"] == "time_close10") / len(rows)),
        "avg_exit_day": _round(mean(float(row["managed_exit_day"]) for row in rows)),
        "mfe10_short_mean": _round(mean(float(row["mfe10_short"]) for row in rows)),
        "mae10_short_mean": _round(mean(float(row["mae10_short"]) for row in rows)),
    }


def _decision(summary: dict[str, Any]) -> tuple[str, str]:
    if summary.get("event_count", 0) < 200:
        return "drop", "insufficient_sample"
    positive = (summary.get("managed_ret_mean") or 0) > 0
    month_ok = (summary.get("positive_month_rate") or 0) >= 0.50
    year_ok = (summary.get("positive_year_rate") or 0) >= 0.60
    stop_ok = (summary.get("stop_loss_first_rate") or 1) <= 0.45
    improvement_ok = (summary.get("delta_vs_close10_mean") or 0) >= 0.005
    if positive and month_ok and year_ok and stop_ok and improvement_ok:
        return "keep", "management_policy_cleared_10d_short_stability_and_improvement_gates"
    if positive and improvement_ok and (month_ok or year_ok):
        return "hold", "management_policy_improved_returns_but_stability_or_stop_gate_incomplete"
    return "drop", "management_policy_did_not_create_trade_ready_10d_short_edge"


def run(*, db_path: Path, output_root: Path, limit_codes: int | None) -> Path:
    db_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        regimes = _regime_rows(conn)
        daily = _load_daily(conn, limit_codes)
    finally:
        conn.close()

    base_events: list[dict[str, Any]] = []
    for code, bars in daily.items():
        closes = [row["c"] for row in bars]
        lows = [row["l"] for row in bars]
        highs = [row["h"] for row in bars]
        vols = [row["v"] for row in bars]
        ma7s = _rolling_mean(closes, 7)
        ma20s = _rolling_mean(closes, 20)
        ma60s = _rolling_mean(closes, 60)
        vol20s = _rolling_mean(vols, 20)
        high20s = _rolling_max(highs, 20)
        high60s = _rolling_max(highs, 60)
        high120s = _rolling_max(highs, 120)
        low60s = _rolling_min(lows, 60)
        for index in range(120, len(bars) - HORIZON - 3):
            feat = _features(bars, index, closes, ma7s, ma20s, ma60s, vol20s, high20s, high60s, high120s, low60s)
            if feat is None:
                continue
            for base_tag in _tags(bars, index, feat):
                for entry in _confirmation_entries(bars, index, base_tag, ma7s, ma20s):
                    metrics = _evaluate_path(bars, index, entry)
                    if metrics is None:
                        continue
                    enriched = _attach_rr(metrics)
                    if enriched.get("rr_to_5pct") is None or float(enriched["rr_to_5pct"]) < RR_THRESHOLD:
                        continue
                    regime = regimes.get(int(enriched["entry_ymd"]), {})
                    base_events.append(
                        {
                            "code": code,
                            "base_tag": base_tag,
                            **{
                                key: _round(feat[key])
                                for key in [
                                    "ret20",
                                    "ret60",
                                    "ret120",
                                    "volume_vs20",
                                    "close_pos",
                                    "upper_wick",
                                    "range60_pos",
                                    "dist_ma7",
                                    "dist_ma20",
                                    "close_vs_high60",
                                ]
                            },
                            **regime,
                            **enriched,
                        }
                    )

    rows: list[dict[str, Any]] = []
    managed_samples: list[dict[str, Any]] = []
    for confirm_tag in sorted({row["confirm_tag"] for row in base_events}):
        tag_rows = [row for row in base_events if row["confirm_tag"] == confirm_tag]
        for filter_id, spec in MARKET_FILTERS.items():
            filtered = [row for row in tag_rows if spec["fn"](row)]
            for policy in MANAGEMENT_POLICIES:
                managed = [_apply_management(row, policy) for row in filtered]
                summary = _summarize(managed)
                decision, reason = _decision(summary)
                rows.append(
                    {
                        "pattern_tag": confirm_tag,
                        "market_filter_id": filter_id,
                        "market_filter_description": spec["description"],
                        "policy_id": policy["policy_id"],
                        "take_profit_pct": policy["take_profit_pct"],
                        "stop_loss_pct": policy["stop_loss_pct"],
                        "decision": decision,
                        "reason": reason,
                        **summary,
                    }
                )
                if len(managed_samples) < 1000:
                    managed_samples.extend(managed[: max(0, 1000 - len(managed_samples))])

    rows.sort(
        key=lambda row: (
            row["decision"] == "keep",
            row["decision"] == "hold",
            row.get("managed_ret_mean") or -1,
            row.get("delta_vs_close10_mean") or -1,
        ),
        reverse=True,
    )
    keep = [row for row in rows if row["decision"] == "keep"]
    hold = [row for row in rows if row["decision"] == "hold"]
    if keep:
        rollup_decision = "keep_failed_high_management_branch"
        reason = "at_least_one_management_policy_cleared_10d_short_stability_and_improvement_gates"
    elif hold:
        rollup_decision = "hold_failed_high_management_branch"
        reason = "management_policy_improved_returns_but_not_trade_ready"
    else:
        rollup_decision = "drop_failed_high_management_branch"
        reason = "no_management_policy_cleared_10d_short_management_gates"

    report = {
        "schema_version": f"{AXIS_ID}_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "db_status": db_status,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "changed_axis": "event_management_only",
            "horizon": HORIZON,
            "base_signal": "top_first_failure_tags",
            "confirmation_window": "1_to_3_sessions_after_base_signal",
            "entry": "confirmation_day_close",
            "rr_threshold": RR_THRESHOLD,
            "market_filters": list(MARKET_FILTERS),
            "management_policies": MANAGEMENT_POLICIES,
            "same_day_take_profit_and_stop_order": "conservative_stop_first",
            "confirmed_non_yahoo_daily_bars": True,
            "gross_only": True,
            "no_runtime_mutation": True,
            "no_meemee_reflection": True,
            "no_provisional_intraday_mix": True,
        },
        "family_leaderboard": rows,
        "decision": {
            "candidate_local_decision": rollup_decision,
            "session_aggregate_decision": rollup_decision,
            "authoritative_rollup_decision": rollup_decision,
            "reason": reason,
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "compare.json", report)
    _write_json(output_dir / "family_leaderboard.json", {"rows": rows})
    _write_json(output_dir / "session_leaderboard_rollup.json", {"decision": report["decision"], "family_leaderboard": rows})
    _write_csv(output_dir / "event_sample.csv", managed_samples[:1000])
    _write_json(output_root / "latest_compare.json", {"run_root": str(output_dir), **report})
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "all_present": True,
            "run_root": str(output_dir),
            "required_files": [
                "compare.json",
                "family_leaderboard.json",
                "session_leaderboard_rollup.json",
                "event_sample.csv",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    print(json.dumps({"output_dir": str(output_dir), "decision": report["decision"], "top_row": rows[0] if rows else None}, ensure_ascii=False, indent=2))
    return output_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-codes", type=int, default=None)
    args = parser.parse_args()
    db_path = Path(args.db_path) if args.db_path else resolve_runtime_stock_db_path()
    run(db_path=db_path, output_root=Path(args.output_root), limit_codes=args.limit_codes)


if __name__ == "__main__":
    main()
