from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_short_holding_duration_exit_feasibility_v1"
PRIOR_ROOT = Path(r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1")
SEGMENTATION_ROOT = Path(r"G:\Tradex\actual_trade_short_loss_segmentation_v1\20260512T015041Z-actual_trade_short_ma20_kept_loss_segmentation_v1")
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_feasibility_v1")


PATH_FIELDS = [
    "normalized_trade_id", "symbol", "side", "entry_date", "actual_exit_date",
    "path_date", "path_day_index", "open", "high", "low", "close", "volume",
    "entry_price", "actual_exit_price", "quantity", "holding_days_actual",
    "gross_pnl_actual", "entry_to_close_return_for_short", "entry_to_open_return_for_short",
    "ma5", "ma7", "ma20", "ma60", "close_vs_ma7_pct", "close_vs_ma20_pct",
    "ma7_slope_5d", "ma20_slope_5d", "unrealized_pnl_close",
    "unrealized_return_close", "max_favorable_return_to_date",
    "max_adverse_return_to_date", "ma7_reclaim_flag_for_short",
    "ma20_reclaim_flag_for_short",
]


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def fnum(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def parse_d(value: str) -> date:
    return datetime.fromisoformat(value).date()


def date_to_epoch(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def epoch_to_date(value: int) -> date:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).date()


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def ma(values: list[float], n: int) -> float | None:
    return avg(values[-n:]) if len(values) >= n else None


def slope(values: list[float], n: int, lag: int = 5) -> float | None:
    if len(values) < n + lag:
        return None
    cur = avg(values[-n:])
    prev = avg(values[-n - lag : -lag])
    return cur / prev - 1.0 if cur is not None and prev not in (None, 0) else None


def fetch_bars(symbols: set[str], min_entry: date, max_exit: date) -> dict[str, list[dict[str, Any]]]:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        placeholders = ",".join(["?"] * len(symbols))
        rows = con.execute(
            f"""
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN ({placeholders})
              AND date <= ?
            ORDER BY code, date
            """,
            [*sorted(symbols), date_to_epoch(max_exit)],
        ).fetchall()
    finally:
        con.close()
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        out[str(r[0])].append({"date": int(r[1]), "d": epoch_to_date(r[1]), "o": r[2], "h": r[3], "l": r[4], "c": r[5], "v": r[6], "source": r[7]})
    return out


def path_for_trade(trade: dict[str, str], bars_by_symbol: dict[str, list[dict[str, Any]]]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    symbol = trade["symbol"]
    entry_date = parse_d(trade["entry_date"])
    exit_date = parse_d(trade["exit_date"])
    entry_price = fnum(trade, "entry_price") or 0.0
    exit_price = fnum(trade, "exit_price") or 0.0
    qty = fnum(trade, "quantity") or 0.0
    all_bars = bars_by_symbol.get(symbol, [])
    prior_entry = [b for b in all_bars if b["d"] <= entry_date]
    prior_exit = [b for b in all_bars if b["d"] <= exit_date]
    if not prior_entry:
        return [], {"normalized_trade_id": trade["normalized_trade_id"], "symbol": symbol, "missing_reason": "entry_bar_missing"}
    if not prior_exit:
        return [], {"normalized_trade_id": trade["normalized_trade_id"], "symbol": symbol, "missing_reason": "exit_bar_missing"}
    entry_bar_date = prior_entry[-1]["d"]
    exit_bar_date = prior_exit[-1]["d"]
    if exit_bar_date < entry_bar_date:
        return [], {"normalized_trade_id": trade["normalized_trade_id"], "symbol": symbol, "missing_reason": "exit_before_entry_bar"}
    start_idx = all_bars.index(prior_entry[-1])
    end_idx = all_bars.index(prior_exit[-1])
    path_bars = all_bars[start_idx : end_idx + 1]
    if not path_bars:
        return [], {"normalized_trade_id": trade["normalized_trade_id"], "symbol": symbol, "missing_reason": "path_empty"}

    rows: list[dict[str, Any]] = []
    max_fav = None
    max_adv = None
    closes_to_date: list[float] = []
    # Include historical closes for MA at each path bar.
    historical = all_bars[:start_idx]
    closes_to_date.extend(float(b["c"]) for b in historical)
    for idx, b in enumerate(path_bars):
        close = float(b["c"])
        open_ = float(b["o"])
        closes_to_date.append(close)
        ma5 = ma(closes_to_date, 5)
        ma7 = ma(closes_to_date, 7)
        ma20 = ma(closes_to_date, 20)
        ma60 = ma(closes_to_date, 60)
        ret_close = entry_price / close - 1.0 if close else None
        ret_open = entry_price / open_ - 1.0 if open_ else None
        if ret_close is not None:
            max_fav = ret_close if max_fav is None else max(max_fav, ret_close)
            max_adv = ret_close if max_adv is None else min(max_adv, ret_close)
        rows.append(
            {
                "normalized_trade_id": trade["normalized_trade_id"],
                "symbol": symbol,
                "side": "short",
                "entry_date": trade["entry_date"],
                "actual_exit_date": trade["exit_date"],
                "path_date": b["d"].isoformat(),
                "path_day_index": idx,
                "open": open_,
                "high": b["h"],
                "low": b["l"],
                "close": close,
                "volume": b["v"],
                "entry_price": entry_price,
                "actual_exit_price": exit_price,
                "quantity": qty,
                "holding_days_actual": int(float(trade.get("holding_days") or 0)),
                "gross_pnl_actual": fnum(trade, "gross_pnl"),
                "entry_to_close_return_for_short": ret_close,
                "entry_to_open_return_for_short": ret_open,
                "ma5": ma5,
                "ma7": ma7,
                "ma20": ma20,
                "ma60": ma60,
                "close_vs_ma7_pct": close / ma7 - 1.0 if ma7 not in (None, 0) else None,
                "close_vs_ma20_pct": close / ma20 - 1.0 if ma20 not in (None, 0) else None,
                "ma7_slope_5d": slope(closes_to_date, 7),
                "ma20_slope_5d": slope(closes_to_date, 20),
                "unrealized_pnl_close": (entry_price - close) * qty,
                "unrealized_return_close": ret_close,
                "max_favorable_return_to_date": max_fav,
                "max_adverse_return_to_date": max_adv,
                "ma7_reclaim_flag_for_short": close > ma7 if ma7 is not None else None,
                "ma20_reclaim_flag_for_short": close > ma20 if ma20 is not None else None,
            }
        )
    return rows, None


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    kept = read_csv(PRIOR_ROOT / "kept_trades.csv")
    kept_shorts = [r for r in kept if r.get("side") == "short" and r.get("counterfactual_action") == "keep" and r.get("tainted_excluded_flag", "").lower() == "false"]
    prior_no_lookahead = json.loads((PRIOR_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    failure_decision = json.loads((SEGMENTATION_ROOT / "kept_short_failure_cluster_decision.json").read_text(encoding="utf-8"))
    symbols = {r["symbol"] for r in kept_shorts}
    min_entry = min(parse_d(r["entry_date"]) for r in kept_shorts)
    max_exit = max(parse_d(r["exit_date"]) for r in kept_shorts)
    bars_by_symbol = fetch_bars(symbols, min_entry, max_exit)

    path_rows: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    per_trade_summary: list[dict[str, Any]] = []
    for trade in kept_shorts:
        rows, miss = path_for_trade(trade, bars_by_symbol)
        if miss:
            missing.append(miss)
            continue
        path_rows.extend(rows)
        final = rows[-1]
        rets = [r["entry_to_close_return_for_short"] for r in rows if r["entry_to_close_return_for_short"] is not None]
        max_adv_val = min(rets) if rets else None
        max_fav_val = max(rets) if rets else None
        per_trade_summary.append(
            {
                "normalized_trade_id": trade["normalized_trade_id"],
                "symbol": trade["symbol"],
                "holding_days_actual": int(float(trade.get("holding_days") or 0)),
                "path_row_count": len(rows),
                "max_adverse_return": max_adv_val,
                "day_of_max_adverse": next((r["path_day_index"] for r in rows if r["entry_to_close_return_for_short"] == max_adv_val), None),
                "max_favorable_return": max_fav_val,
                "day_of_max_favorable": next((r["path_day_index"] for r in rows if r["entry_to_close_return_for_short"] == max_fav_val), None),
                "became_profitable_before_final_loss": bool((float(trade.get("gross_pnl") or 0) < 0) and any((r["entry_to_close_return_for_short"] or 0) > 0 for r in rows)),
                "hit_ma7_reclaim": any(r["ma7_reclaim_flag_for_short"] is True for r in rows),
                "hit_ma20_reclaim": any(r["ma20_reclaim_flag_for_short"] is True for r in rows),
            }
        )

    path_trade_ids = {r["normalized_trade_id"] for r in per_trade_summary}
    missing_reasons = Counter(r["missing_reason"] for r in missing)
    coverage = {
        "kept_short_count": len(kept_shorts),
        "path_available_trade_count": len(path_trade_ids),
        "path_missing_trade_count": len(missing),
        "path_coverage_rate": len(path_trade_ids) / len(kept_shorts) if kept_shorts else 0.0,
        "full_entry_to_exit_path_count": len(path_trade_ids),
        "partial_path_count": 0,
        "missing_reason_counts": dict(missing_reasons),
        "path_row_count": len(path_rows),
    }

    field_missing_count = {
        field: sum(1 for r in path_rows if r.get(field) in (None, ""))
        for field in ["ma7", "ma20", "ma7_slope_5d", "ma20_slope_5d", "entry_to_close_return_for_short", "entry_to_open_return_for_short"]
    }
    families = {
        "fixed_holding_day_exit": {"required_fields": ["path_day_index", "close", "open"], "known_limitation": "requires convention choice close vs next open"},
        "ma7_reclaim_exit": {"required_fields": ["ma7_reclaim_flag_for_short", "ma7"], "known_limitation": "early history may lack MA7"},
        "ma20_reclaim_exit": {"required_fields": ["ma20_reclaim_flag_for_short", "ma20"], "known_limitation": "early history may lack MA20"},
        "max_loss_exit": {"required_fields": ["unrealized_return_close", "unrealized_pnl_close"], "known_limitation": "close-only unless intraday convention is added"},
        "profit_take_exit": {"required_fields": ["unrealized_return_close", "unrealized_pnl_close"], "known_limitation": "close-only unless intraday convention is added"},
        "time_plus_ma_reclaim_exit": {"required_fields": ["path_day_index", "ma7_reclaim_flag_for_short", "ma20_reclaim_flag_for_short"], "known_limitation": "requires explicit priority order"},
    }
    family_out = {}
    for name, spec in families.items():
        missing_count = sum(field_missing_count.get(f, 0) for f in spec["required_fields"])
        available_path_field_ok = missing_count / max(len(path_rows), 1) < 0.1
        family_out[name] = {
            "feasible": coverage["path_coverage_rate"] >= 0.95 and available_path_field_ok,
            "feasible_on_available_path_subset": available_path_field_ok and coverage["path_available_trade_count"] > 0,
            "required_fields_available": spec["required_fields"],
            "missing_field_count": missing_count,
            "known_limitation": (
                spec["known_limitation"]
                if coverage["path_coverage_rate"] >= 0.95
                else f"{spec['known_limitation']}; full-set replay blocked by bar coverage {coverage['path_coverage_rate']:.3f}"
            ),
        }

    long_hold_loss_ids = {
        r["normalized_trade_id"]
        for r in kept_shorts
        if int(float(r.get("holding_days") or 0)) >= 31 and float(r.get("gross_pnl") or 0) < 0
    }
    long_profiles = [r for r in per_trade_summary if r["normalized_trade_id"] in long_hold_loss_ids]
    long_profile = {
        "count": len(long_hold_loss_ids),
        "path_coverage": len(long_profiles) / len(long_hold_loss_ids) if long_hold_loss_ids else None,
        "average_max_adverse_return": mean([r["max_adverse_return"] for r in long_profiles if r["max_adverse_return"] is not None]) if long_profiles else None,
        "average_max_favorable_return": mean([r["max_favorable_return"] for r in long_profiles if r["max_favorable_return"] is not None]) if long_profiles else None,
        "average_day_of_max_adverse_move": mean([r["day_of_max_adverse"] for r in long_profiles if r["day_of_max_adverse"] is not None]) if long_profiles else None,
        "average_day_of_max_favorable_move": mean([r["day_of_max_favorable"] for r in long_profiles if r["day_of_max_favorable"] is not None]) if long_profiles else None,
        "proportion_that_became_profitable_before_final_loss": sum(1 for r in long_profiles if r["became_profitable_before_final_loss"]) / len(long_profiles) if long_profiles else None,
        "proportion_that_hit_ma7_reclaim_before_final_exit": sum(1 for r in long_profiles if r["hit_ma7_reclaim"]) / len(long_profiles) if long_profiles else None,
        "proportion_that_hit_ma20_reclaim_before_final_exit": sum(1 for r in long_profiles if r["hit_ma20_reclaim"]) / len(long_profiles) if long_profiles else None,
    }

    next_open_available = 0
    by_trade_rows = defaultdict(list)
    for r in path_rows:
        by_trade_rows[r["normalized_trade_id"]].append(r)
    for rows in by_trade_rows.values():
        rows = sorted(rows, key=lambda r: r["path_day_index"])
        if len(rows) > 1:
            next_open_available += 1
    execution_ready = {
        "exit_at_close_supported": len(path_rows) > 0,
        "exit_at_next_open_supported": next_open_available / len(path_trade_ids) if path_trade_ids else 0.0,
        "same_day_exit_supported": True,
        "actual_exit_price_comparison_supported": True,
        "trading_day_index_based_exits_supported": True,
    }

    if coverage["path_coverage_rate"] < 0.95:
        decision = "needs_bar_data_repair"
    elif any(not v["feasible"] for v in family_out.values()):
        decision = "needs_path_field_repair"
    else:
        decision = "ready_for_exit_rule_replay"

    feasibility = {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "source_counterfactual_root": str(PRIOR_ROOT),
        "source_segmentation_root": str(SEGMENTATION_ROOT),
        "kept_short_count": len(kept_shorts),
        "path_coverage": coverage,
        "path_parquet_path": None,
        "execution_convention_readiness": execution_ready,
        "exit_rule_family_feasibility": family_out,
        "no_lookahead_boundary_check": {
            "exit_rules_tested": False,
            "post_entry_bars_used_only_for_feasibility_and_descriptive_profiles": True,
            "no_simulated_rule_outcome_selected": True,
            "no_tainted_trades_included": True,
            "no_long_trades_included": True,
            "no_provisional_yahoo_bars_used": True,
            "future_exit_replay_must_use_only_bars_up_to_simulated_exit_date": True,
            "prior_no_lookahead_pass": bool(prior_no_lookahead.get("pass")),
        },
        "prior_failure_cluster_decision": failure_decision.get("decision"),
    }

    write_json(run_root / "short_exit_feasibility.json", feasibility)
    write_json(run_root / "short_trade_path_coverage.json", coverage)
    write_json(run_root / "short_trade_path_summary.json", {"trade_summaries": per_trade_summary, "path_row_count": len(path_rows)})
    write_json(run_root / "short_long_hold_loss_path_profile.json", long_profile)
    write_json(run_root / "exit_rule_family_feasibility.json", family_out)
    write_csv(run_root / "short_trade_path_rows_sample.csv", path_rows[:5000], PATH_FIELDS)
    write_csv(run_root / "short_trade_path_missing.csv", missing)
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision,
        "complete": True,
        "required_artifacts": [
            "short_exit_feasibility.json",
            "short_trade_path_coverage.json",
            "short_trade_path_summary.json",
            "short_long_hold_loss_path_profile.json",
            "exit_rule_family_feasibility.json",
            "short_trade_path_rows_sample.csv",
            "short_trade_path_missing.csv",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": decision, "path_rows": len(path_rows)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
