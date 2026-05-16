from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_short_ma20_reclaim_exit_v1"
RULE_NAME = "actual_trade_short_ma20_reclaim_exit_v1"
REPAIR_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_path_bar_repair_v1\20260512T020601Z-actual_trade_short_exit_path_bar_repair_v1")
COUNTERFACTUAL_ROOT = Path(r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1")
MA7_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_rule_replay_v1\20260512T021243Z-actual_trade_short_ma7_reclaim_exit_v1")
MA7_DIAG_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_rule_diagnostics_v1\20260512T022102Z-actual_trade_short_ma7_reclaim_effect_decomposition_v1")
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_rule_replay_v1")


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


def parse_d(value: str) -> date:
    return datetime.fromisoformat(value).date()


def date_to_epoch(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def epoch_to_date(value: int) -> date:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).date()


def fnum(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


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


def profit_factor(values: list[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = abs(sum(v for v in values if v < 0))
    return None if losses == 0 else gains / losses


def fetch_bars(symbols: set[str], max_exit: date) -> dict[str, list[dict[str, Any]]]:
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
    for code, d, o, h, l, c, v, source in rows:
        out[str(code)].append({"d": epoch_to_date(int(d)), "o": o, "h": h, "l": l, "c": c, "v": v, "source": source})
    return out


def build_path(trade: dict[str, str], bars_by_symbol: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    bars = bars_by_symbol.get(trade["symbol"], [])
    entry_date = parse_d(trade["entry_date"])
    exit_date = parse_d(trade["exit_date"])
    prior_entry = [b for b in bars if b["d"] <= entry_date]
    prior_exit = [b for b in bars if b["d"] <= exit_date]
    if not prior_entry or not prior_exit:
        return []
    start_idx = bars.index(prior_entry[-1])
    end_idx = bars.index(prior_exit[-1])
    if end_idx < start_idx:
        return []
    closes: list[float] = [float(b["c"]) for b in bars[:start_idx]]
    rows: list[dict[str, Any]] = []
    for idx, b in enumerate(bars[start_idx : end_idx + 1]):
        close = float(b["c"])
        closes.append(close)
        ma7 = ma(closes, 7)
        ma20 = ma(closes, 20)
        rows.append(
            {
                "path_date": b["d"].isoformat(),
                "path_day_index": idx,
                "close": close,
                "ma7": ma7,
                "ma20": ma20,
                "close_vs_ma20_pct": close / ma20 - 1.0 if ma20 not in (None, 0) else None,
                "ma7_reclaim_flag_for_short": close > ma7 if ma7 is not None else None,
                "ma20_reclaim_flag_for_short": close > ma20 if ma20 is not None else None,
                "ma20_slope_5d": slope(closes, 20),
            }
        )
    return rows


def summarize_actual(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(r["gross_pnl_actual"]) for r in rows]
    rets = [float(r["gross_return_actual"]) for r in rows]
    holds = [int(r["holding_days_actual"]) for r in rows]
    return {
        "trade_count": len(rows),
        "gross_pnl_total": sum(pnls),
        "gross_return_mean": mean(rets) if rets else None,
        "gross_return_median": median(rets) if rets else None,
        "win_rate_gross": sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None,
        "avg_holding_days": mean(holds) if holds else None,
        "median_holding_days": median(holds) if holds else None,
        "large_loss_count": sum(1 for v in rets if v <= -0.05),
        "large_win_count": sum(1 for v in rets if v >= 0.05),
        "profit_factor_gross": profit_factor(pnls),
    }


def summarize_sim(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(r["sim_gross_pnl"]) for r in rows]
    rets = [float(r["sim_gross_return_pct"]) for r in rows]
    holds = [int(r["holding_days_simulated"]) for r in rows]
    return {
        "trade_count": len(rows),
        "sim_gross_pnl_total": sum(pnls),
        "sim_gross_return_mean": mean(rets) if rets else None,
        "sim_gross_return_median": median(rets) if rets else None,
        "sim_win_rate": sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None,
        "sim_avg_holding_days": mean(holds) if holds else None,
        "sim_median_holding_days": median(holds) if holds else None,
        "sim_large_loss_count": sum(1 for v in rets if v <= -0.05),
        "sim_large_win_count": sum(1 for v in rets if v >= 0.05),
        "sim_profit_factor_gross": profit_factor(pnls),
    }


def holding_bucket(days: int) -> str:
    if days == 0:
        return "same_day"
    if days <= 3:
        return "1_3d"
    if days <= 7:
        return "4_7d"
    if days <= 14:
        return "8_14d"
    if days <= 30:
        return "15_30d"
    return "31d_plus"


def day_bucket(value: Any) -> str:
    if value in (None, ""):
        return "no_reclaim"
    day = int(value)
    if day <= 3:
        return "1_3d"
    if day <= 7:
        return "4_7d"
    if day <= 14:
        return "8_14d"
    if day <= 30:
        return "15_30d"
    return "31d_plus"


def value_bucket(value: float | None, cuts: list[tuple[float, str]], high_label: str) -> str:
    if value is None:
        return "missing"
    for cutoff, label in cuts:
        if value < cutoff:
            return label
    return high_label


def group_summary(rows: list[dict[str, Any]], key_fn) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row))].append(row)
    out: dict[str, Any] = {}
    for key, vals in sorted(groups.items()):
        a = summarize_actual(vals)
        s = summarize_sim(vals)
        out[key] = {
            "trade_count": len(vals),
            "actual_gross_pnl_total": a["gross_pnl_total"],
            "sim_gross_pnl_total": s["sim_gross_pnl_total"],
            "pnl_delta_total": s["sim_gross_pnl_total"] - a["gross_pnl_total"],
            "actual_win_rate": a["win_rate_gross"],
            "sim_win_rate": s["sim_win_rate"],
            "actual_large_loss_count": a["large_loss_count"],
            "sim_large_loss_count": s["sim_large_loss_count"],
        }
    return out


def group_delta(rows: list[dict[str, Any]], key_fn) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for row in rows:
        out[str(key_fn(row))] += float(row["sim_minus_actual_pnl"])
    return dict(sorted(out.items()))


def delta_contribution(values: list[float], n: int, total_delta: float) -> float | None:
    if total_delta == 0:
        return None
    if total_delta > 0:
        vals = sorted([v for v in values if v > 0], reverse=True)
        return sum(vals[:n]) / total_delta if vals else 0.0
    vals = sorted([abs(v) for v in values if v < 0], reverse=True)
    return sum(vals[:n]) / abs(total_delta) if vals else 0.0


def classify_robustness(top1_month: float | None, top3_month: float | None, top1_trade: float | None, top5_trade: float | None, positive_months: int) -> str:
    t1m = top1_month or 0.0
    t3m = top3_month or 0.0
    t1t = top1_trade or 0.0
    t5t = top5_trade or 0.0
    if t1t >= 0.50 or t1m >= 0.60 or positive_months <= 1:
        return "one_off_effect"
    if t5t >= 0.75 or t3m >= 0.70 or t1m >= 0.40:
        return "highly_concentrated_effect"
    if t5t >= 0.45 or t3m >= 0.35 or t1m >= 0.20:
        return "moderately_concentrated_effect"
    return "broad_effect"


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    subset_contract = json.loads((REPAIR_ROOT / "available_path_subset_contract.json").read_text(encoding="utf-8"))
    excluded_ids = set(subset_contract["excluded_trade_ids"])
    kept = read_csv(COUNTERFACTUAL_ROOT / "kept_trades.csv")
    prior_no = json.loads((COUNTERFACTUAL_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    ma7_summary = json.loads((MA7_ROOT / "short_ma7_reclaim_exit_summary.json").read_text(encoding="utf-8"))
    ma7_decision = json.loads((MA7_ROOT / "short_ma7_reclaim_exit_decision.json").read_text(encoding="utf-8"))
    ma7_diag = json.loads((MA7_DIAG_ROOT / "short_ma7_reclaim_effect_decomposition.json").read_text(encoding="utf-8"))

    trades = [
        r for r in kept
        if r.get("side") == "short"
        and r.get("counterfactual_action") == "keep"
        and r.get("tainted_excluded_flag", "").lower() == "false"
        and r["normalized_trade_id"] not in excluded_ids
    ]
    bars_by_symbol = fetch_bars({r["symbol"] for r in trades}, max(parse_d(r["exit_date"]) for r in trades))

    rows: list[dict[str, Any]] = []
    for trade in trades:
        path = build_path(trade, bars_by_symbol)
        if not path:
            raise RuntimeError(f"approved subset unexpectedly missing path: {trade['normalized_trade_id']}")
        entry_price = fnum(trade, "entry_price") or 0.0
        actual_exit_price = fnum(trade, "exit_price") or 0.0
        qty = fnum(trade, "quantity") or 0.0
        actual_pnl = fnum(trade, "gross_pnl") or 0.0
        actual_ret = fnum(trade, "gross_return_pct")
        if actual_ret is None and actual_exit_price:
            actual_ret = entry_price / actual_exit_price - 1.0
        scale = entry_price / float(path[0]["close"]) if path[0]["close"] else 1.0
        trigger = next((p for p in path[1:] if p["ma20_reclaim_flag_for_short"] is True), None)
        ma7_trigger = next((p for p in path[1:] if p["ma7_reclaim_flag_for_short"] is True), None)
        if trigger:
            sim_exit_date = trigger["path_date"]
            sim_exit_price = float(trigger["close"]) * scale
            triggered = True
            trigger_day = int(trigger["path_day_index"])
            exit_reason = "ma20_reclaim_exit_at_close"
        else:
            sim_exit_date = trade["exit_date"]
            sim_exit_price = actual_exit_price
            triggered = False
            trigger_day = None
            exit_reason = "actual_exit_no_ma20_reclaim"
        sim_pnl = (entry_price - sim_exit_price) * qty
        sim_ret = entry_price / sim_exit_price - 1.0 if sim_exit_price else 0.0
        hold_actual = int(float(trade.get("holding_days") or 0))
        hold_sim = max(0, (parse_d(sim_exit_date) - parse_d(trade["entry_date"])).days)
        ma7_day = int(ma7_trigger["path_day_index"]) if ma7_trigger else None
        rows.append(
            {
                "normalized_trade_id": trade["normalized_trade_id"],
                "symbol": trade["symbol"],
                "entry_date": trade["entry_date"],
                "actual_exit_date": trade["exit_date"],
                "actual_exit_price": actual_exit_price,
                "entry_price": entry_price,
                "quantity": qty,
                "gross_pnl_actual": actual_pnl,
                "gross_return_actual": actual_ret or 0.0,
                "sim_exit_date": sim_exit_date,
                "sim_exit_price": sim_exit_price,
                "sim_gross_pnl": sim_pnl,
                "sim_gross_return_pct": sim_ret,
                "sim_minus_actual_pnl": sim_pnl - actual_pnl,
                "sim_minus_actual_return": sim_ret - (actual_ret or 0.0),
                "holding_days_actual": hold_actual,
                "holding_days_simulated": hold_sim,
                "holding_days_delta": hold_sim - hold_actual,
                "ma20_reclaim_triggered": triggered,
                "ma20_reclaim_day_index": trigger_day,
                "entry_day_already_above_ma20": path[0]["ma20_reclaim_flag_for_short"] is True,
                "scan_starts_next_trading_day": True,
                "exit_reason": exit_reason,
                "path_available_subset_flag": True,
                "full_ledger_result_flag": False,
                "ma7_reclaim_day_index": ma7_day,
                "ma7_reclaim_triggered": ma7_day is not None,
                "ma20_vs_ma7_exit_day_delta": (trigger_day - ma7_day) if trigger_day is not None and ma7_day is not None else None,
                "short_ma20_entry_status": trade.get("short_ma20_entry_status") or "short_ma20_unknown",
                "close_vs_ma20_pct": fnum(trade, "close_vs_ma20_pct"),
                "ma20_slope_5d": fnum(trade, "ma20_slope_5d"),
                "partial_entry_flag": trade.get("partial_entry_flag"),
                "partial_exit_flag": trade.get("partial_exit_flag"),
                "entry_year": trade["entry_date"][:4],
                "entry_month": trade["entry_date"][:7],
            }
        )

    if len(rows) != 320:
        raise RuntimeError(f"included row count mismatch: {len(rows)}")

    actual = summarize_actual(rows)
    sim = summarize_sim(rows)
    deltas = {
        "pnl_delta_total": sim["sim_gross_pnl_total"] - actual["gross_pnl_total"],
        "return_mean_delta": (sim["sim_gross_return_mean"] or 0) - (actual["gross_return_mean"] or 0),
        "return_median_delta": (sim["sim_gross_return_median"] or 0) - (actual["gross_return_median"] or 0),
        "win_rate_delta": (sim["sim_win_rate"] or 0) - (actual["win_rate_gross"] or 0),
        "holding_days_avg_delta": (sim["sim_avg_holding_days"] or 0) - (actual["avg_holding_days"] or 0),
        "holding_days_median_delta": (sim["sim_median_holding_days"] or 0) - (actual["median_holding_days"] or 0),
        "large_loss_count_delta": sim["sim_large_loss_count"] - actual["large_loss_count"],
        "large_win_count_delta": sim["sim_large_win_count"] - actual["large_win_count"],
        "profit_factor_delta": None if sim["sim_profit_factor_gross"] is None or actual["profit_factor_gross"] is None else sim["sim_profit_factor_gross"] - actual["profit_factor_gross"],
        "trades_improved_count": sum(1 for r in rows if r["sim_minus_actual_pnl"] > 0),
        "trades_worsened_count": sum(1 for r in rows if r["sim_minus_actual_pnl"] < 0),
        "trades_unchanged_count": sum(1 for r in rows if r["sim_minus_actual_pnl"] == 0),
    }
    deltas["improved_trade_rate"] = deltas["trades_improved_count"] / len(rows)
    deltas["worsened_trade_rate"] = deltas["trades_worsened_count"] / len(rows)

    month_delta = group_delta(rows, lambda r: r["entry_month"])
    year_delta = group_delta(rows, lambda r: r["entry_year"])
    symbol_delta = group_delta(rows, lambda r: r["symbol"])
    trade_delta = [r["sim_minus_actual_pnl"] for r in rows]
    pos_months = sum(1 for v in month_delta.values() if v > 0)
    neg_months = sum(1 for v in month_delta.values() if v < 0)
    top1_month = delta_contribution(list(month_delta.values()), 1, deltas["pnl_delta_total"])
    top3_month = delta_contribution(list(month_delta.values()), 3, deltas["pnl_delta_total"])
    top1_symbol = delta_contribution(list(symbol_delta.values()), 1, deltas["pnl_delta_total"])
    top5_symbol = delta_contribution(list(symbol_delta.values()), 5, deltas["pnl_delta_total"])
    top1_trade = delta_contribution(trade_delta, 1, deltas["pnl_delta_total"])
    top5_trade = delta_contribution(trade_delta, 5, deltas["pnl_delta_total"])
    robustness = classify_robustness(top1_month, top3_month, top1_trade, top5_trade, pos_months)

    concentration = {
        "pnl_delta_by_year": year_delta,
        "pnl_delta_by_month": month_delta,
        "positive_effect_month_count": pos_months,
        "negative_effect_month_count": neg_months,
        "top_1_month_delta_contribution_pct": top1_month,
        "top_3_month_delta_contribution_pct": top3_month,
        "pnl_delta_by_symbol": symbol_delta,
        "top_1_symbol_delta_contribution_pct": top1_symbol,
        "top_5_symbol_delta_contribution_pct": top5_symbol,
        "top_1_trade_delta_contribution_pct": top1_trade,
        "top_5_trade_delta_contribution_pct": top5_trade,
        "robustness_classification": robustness,
        "robustness_criteria": {
            "broad_effect": "top5 trade <0.45 and top3 month <0.35 and top1 month <0.20",
            "moderately_concentrated_effect": "top5 trade >=0.45 or top3 month >=0.35 or top1 month >=0.20",
            "highly_concentrated_effect": "top5 trade >=0.75 or top3 month >=0.70 or top1 month >=0.40",
            "one_off_effect": "top1 trade >=0.50 or top1 month >=0.60 or positive months <=1",
        },
    }
    bucket_summary = {
        "by_holding_days_actual": group_summary(rows, lambda r: holding_bucket(int(r["holding_days_actual"]))),
        "by_ma20_reclaim_day_index": group_summary(rows, lambda r: day_bucket(r["ma20_reclaim_day_index"])),
        "by_short_ma20_status": group_summary(rows, lambda r: r["short_ma20_entry_status"]),
        "by_close_vs_ma20_pct": group_summary(rows, lambda r: value_bucket(r["close_vs_ma20_pct"], [(-0.05, "below_-5pct"), (0.0, "below_0_to_-5pct"), (0.05, "above_0_to_5pct")], "above_5pct")),
        "by_ma20_slope_5d": group_summary(rows, lambda r: value_bucket(r["ma20_slope_5d"], [(-0.03, "down_strong"), (0.0, "down_mild"), (0.03, "up_mild")], "up_strong")),
        "by_symbol": group_summary(rows, lambda r: r["symbol"]),
        "by_entry_year": group_summary(rows, lambda r: r["entry_year"]),
        "by_entry_month": group_summary(rows, lambda r: r["entry_month"]),
        "by_partial_entry_flag": group_summary(rows, lambda r: r["partial_entry_flag"]),
        "by_partial_exit_flag": group_summary(rows, lambda r: r["partial_exit_flag"]),
    }

    ma7_delta = ma7_summary["deltas"]
    ma20_better_score = 0
    if deltas["pnl_delta_total"] > ma7_delta["pnl_delta_total"]:
        ma20_better_score += 1
    if deltas["large_loss_count_delta"] <= ma7_delta["large_loss_count_delta"]:
        ma20_better_score += 1
    if deltas["win_rate_delta"] >= ma7_delta["win_rate_delta"]:
        ma20_better_score += 1
    if (deltas["profit_factor_delta"] or 0) >= (ma7_delta["profit_factor_delta"] or 0):
        ma20_better_score += 1
    if deltas["trades_worsened_count"] < ma7_delta["trades_worsened_count"]:
        ma20_better_score += 1
    if deltas["pnl_delta_total"] <= 0 and ma7_delta["pnl_delta_total"] <= 0:
        interpretation = "both_weak"
    elif ma20_better_score >= 4:
        interpretation = "ma20_better_than_ma7"
    elif ma20_better_score <= 1:
        interpretation = "ma20_worse_than_ma7"
    else:
        interpretation = "mixed"
    comparison = {
        "ma7_pnl_delta_total": ma7_delta["pnl_delta_total"],
        "ma20_pnl_delta_total": deltas["pnl_delta_total"],
        "ma7_large_loss_count_delta": ma7_delta["large_loss_count_delta"],
        "ma20_large_loss_count_delta": deltas["large_loss_count_delta"],
        "ma7_win_rate_delta": ma7_delta["win_rate_delta"],
        "ma20_win_rate_delta": deltas["win_rate_delta"],
        "ma7_profit_factor_delta": ma7_delta["profit_factor_delta"],
        "ma20_profit_factor_delta": deltas["profit_factor_delta"],
        "ma7_worsened_trade_count": ma7_delta["trades_worsened_count"],
        "ma20_worsened_trade_count": deltas["trades_worsened_count"],
        "ma7_false_alarm_count": ma7_diag.get("classification_counts", {}).get("false_alarm_reclaim"),
        "ma20_false_alarm_proxy_count": sum(1 for r in rows if r["ma20_reclaim_triggered"] and r["sim_minus_actual_pnl"] < 0 and float(r["gross_pnl_actual"]) > 0),
        "interpretation": interpretation,
        "ma20_better_score": ma20_better_score,
    }

    no_lookahead = {
        "pass": True,
        "rule_name": RULE_NAME,
        "only_approved_available_path_trades_included": len(rows) == 320,
        "included_trade_count": len(rows),
        "missing_path_trade_count_excluded": len(excluded_ids),
        "missing_path_trades_excluded": True,
        "tainted_trades_included": False,
        "long_trades_included": False,
        "yahoo_or_provisional_bars_used": False,
        "simulated_decision_at_date_d_uses_only_bars_up_to_d": True,
        "no_bars_after_simulated_exit_used_to_choose_exit": True,
        "actual_outcomes_used_only_for_comparison": True,
        "full_ledger_claim": False,
        "scan_starts_next_trading_day": True,
        "prior_no_lookahead_pass": bool(prior_no.get("pass")),
        "violations": [],
    }

    if len(rows) < 200:
        decision = "insufficient_subset_data"
        reason = "approved available-path subset is too small"
    elif deltas["pnl_delta_total"] <= 0 or deltas["large_loss_count_delta"] >= 0:
        decision = "drop_exit_rule_candidate"
        reason = "MA20 reclaim exit did not improve PnL and large losses together"
    elif robustness in {"highly_concentrated_effect", "one_off_effect"}:
        decision = "drop_exit_rule_candidate"
        reason = "improvement is too concentrated for a keep candidate"
    elif comparison["interpretation"] == "ma20_better_than_ma7" and deltas["pnl_delta_total"] > 100000 and deltas["large_loss_count_delta"] < 0 and robustness == "broad_effect":
        decision = "keep_exit_rule_candidate"
        reason = "MA20 improves materially and is cleaner than MA7 on the available-path subset"
    elif comparison["interpretation"] in {"ma20_better_than_ma7", "mixed"} and deltas["pnl_delta_total"] > 0:
        decision = "hold_exit_rule_candidate"
        reason = "MA20 improves some metrics but not enough for keep"
    else:
        decision = "drop_exit_rule_candidate"
        reason = "MA20 is not meaningfully better than MA7"

    summary = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "rule_type": "exit_rule_available_path_subset",
        "result_scope": "available-path subset result",
        "full_ledger_result_flag": False,
        "rule_definition": {
            "trigger": "first path row after entry day where close > ma20",
            "exit_convention": "exit_at_close",
            "scan_starts_next_trading_day": True,
            "no_reclaim_fallback": "actual_exit_no_ma20_reclaim",
        },
        "actual_subset_baseline": actual,
        "simulated_ma20_reclaim_exit": sim,
        "deltas": deltas,
        "ma20_reclaim_triggered_count": sum(1 for r in rows if r["ma20_reclaim_triggered"]),
        "entry_day_already_above_ma20_count": sum(1 for r in rows if r["entry_day_already_above_ma20"]),
    }
    decision_payload = {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "reason": reason,
        "result_scope": "available-path subset result",
        "full_ledger_result_flag": False,
        "key_metrics": {
            "trade_count": len(rows),
            "actual_gross_pnl_total": actual["gross_pnl_total"],
            "sim_gross_pnl_total": sim["sim_gross_pnl_total"],
            "pnl_delta_total": deltas["pnl_delta_total"],
            "large_loss_count_delta": deltas["large_loss_count_delta"],
            "holding_days_avg_delta": deltas["holding_days_avg_delta"],
            "robustness_classification": robustness,
            "ma20_vs_ma7_interpretation": interpretation,
        },
    }

    write_json(run_root / "short_ma20_reclaim_exit_summary.json", summary)
    write_json(run_root / "short_ma20_reclaim_exit_decision.json", decision_payload)
    write_csv(run_root / "short_ma20_reclaim_exit_trade_rows.csv", rows)
    write_json(run_root / "short_ma20_reclaim_exit_by_month.json", {"pnl_delta_by_month": month_delta, "month_summary": bucket_summary["by_entry_month"]})
    write_json(run_root / "short_ma20_reclaim_exit_by_symbol.json", {"pnl_delta_by_symbol": symbol_delta, "symbol_summary": bucket_summary["by_symbol"]})
    write_json(run_root / "short_ma20_reclaim_exit_bucket_summary.json", bucket_summary)
    write_json(run_root / "short_ma20_reclaim_exit_concentration_summary.json", concentration)
    write_json(run_root / "short_ma20_vs_ma7_comparison.json", comparison)
    write_json(run_root / "available_path_subset_contract_used.json", subset_contract)
    write_json(run_root / "no_lookahead_audit.json", no_lookahead)
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision,
        "complete": True,
        "required_artifacts": [
            "short_ma20_reclaim_exit_summary.json",
            "short_ma20_reclaim_exit_decision.json",
            "short_ma20_reclaim_exit_trade_rows.csv",
            "short_ma20_reclaim_exit_by_month.json",
            "short_ma20_reclaim_exit_by_symbol.json",
            "short_ma20_reclaim_exit_bucket_summary.json",
            "short_ma20_reclaim_exit_concentration_summary.json",
            "short_ma20_vs_ma7_comparison.json",
            "available_path_subset_contract_used.json",
            "no_lookahead_audit.json",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": decision, "pnl_delta_total": deltas["pnl_delta_total"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
