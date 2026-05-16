from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "decision_context_reconstruction_v1"
NORMALIZATION_ROOT = Path(r"G:\Tradex\actual_trade_ledger_normalization_v1\20260512T003853Z-actual_trade_ledger_normalization_v1")
AUDIT_ROOT = Path(r"G:\Tradex\actual_trade_manual_mapping_audit_v1\20260512T010326Z-actual_trade_manual_mapping_audit_v1")
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME

BASE_FIELDS = [
    "normalized_trade_id", "source", "broker", "account_type", "symbol", "side",
    "entry_date", "exit_date", "entry_price", "exit_price", "quantity",
    "notional_entry", "notional_exit", "gross_pnl", "gross_return_pct", "holding_days",
    "partial_entry_flag", "partial_exit_flag", "same_day_trade_flag", "source_event_ids",
    "pnl_is_gross_only", "normalization_status", "clean_subset_flag",
    "tainted_excluded_flag", "context_features_computed",
]

CONTEXT_FIELDS = [
    "entry_bar_date_used", "entry_bar_shifted", "entry_bar_shift_days",
    "entry_close", "entry_open", "entry_high", "entry_low", "entry_volume", "prev_close",
    "gap_pct", "entry_day_return_pct", "body_pct", "upper_wick_ratio", "lower_wick_ratio",
    "close_position_in_range", "ma5", "ma7", "ma20", "ma60", "ma100", "ma200",
    "close_vs_ma7_pct", "close_vs_ma20_pct", "close_vs_ma60_pct",
    "close_vs_ma100_pct", "close_vs_ma200_pct", "ma7_slope_5d", "ma20_slope_5d",
    "ma60_slope_5d", "above_ma7_count_recent", "below_ma7_count_recent",
    "above_ma20_count_recent", "below_ma20_count_recent", "above_ma60_count_recent",
    "below_ma60_count_recent", "ret_3d", "ret_5d", "ret_10d", "ret_20d",
    "drawdown_10d", "drawdown_20d", "runup_10d", "runup_20d",
    "distance_from_20d_high_pct", "distance_from_20d_low_pct",
    "new_high_20d_flag", "new_low_20d_flag", "vol_ma5", "vol_ma20",
    "vol_ratio_t_20", "vol_ratio5_20", "weekly_close", "weekly_ma20",
    "weekly_close_vs_ma20_pct", "weekly_ma20_slope", "monthly_close",
    "monthly_ma20", "monthly_close_vs_ma20_pct", "monthly_ma20_slope",
    "weekly_context_available", "monthly_context_available", "context_missing_reason",
    "champion_rank_at_entry", "champion_score_at_entry", "candidate_pool_presence_at_entry",
    "ranking_context_available", "actual_trade_win_flag", "actual_trade_loss_flag",
    "actual_trade_return_bucket", "holding_days_bucket",
]

ALL_FIELDS = BASE_FIELDS + CONTEXT_FIELDS


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] = ALL_FIELDS) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: to_jsonable(row.get(k)) for k in fields})


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=to_jsonable), encoding="utf-8")


def to_jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def ymd_int(value: str) -> int:
    return int(value.replace("-", ""))


def ymd_to_date(value: int) -> date:
    s = str(int(value))
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def db_date_to_date(value: int) -> date:
    value = int(value)
    if value > 30000000:
        return datetime.fromtimestamp(value, tz=timezone.utc).date()
    return ymd_to_date(value)


def date_to_db_epoch(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def fnum(row: dict[str, str], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def boolish(value: str | bool | None) -> bool:
    return str(value).lower() == "true"


def load_clean_trades() -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, Any]]:
    closed = read_csv(NORMALIZATION_ROOT / "normalized_trade_ledger.csv")
    clean_decision = json.loads((AUDIT_ROOT / "clean_subset_decision.json").read_text(encoding="utf-8"))
    tainted = set(clean_decision["tainted_trade_ids"])
    clean = [r for r in closed if r["normalized_trade_id"] not in tainted]
    excluded = [r for r in closed if r["normalized_trade_id"] in tainted]
    if len(clean) != clean_decision["closed_trade_count_clean"] or len(excluded) != clean_decision["closed_trade_count_tainted"]:
        raise RuntimeError("clean subset does not reconcile with clean_subset_decision.json")
    return clean, excluded, clean_decision


def fetch_bars(symbols: set[str], min_entry: int, max_entry: int) -> dict[str, list[dict[str, Any]]]:
    # Fetch enough lookback for MA200 and monthly context.
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        placeholders = ",".join(["?"] * len(symbols))
        max_entry_epoch = date_to_db_epoch(ymd_to_date(max_entry))
        rows = con.execute(
            f"""
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN ({placeholders})
              AND date <= ?
            ORDER BY code, date
            """,
            [*sorted(symbols), max_entry_epoch],
        ).fetchall()
    finally:
        con.close()
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        out[str(r[0])].append({"date": int(r[1]), "o": r[2], "h": r[3], "l": r[4], "c": r[5], "v": r[6], "source": r[7]})
    return out


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def pct(num: float | None, den: float | None) -> float | None:
    if num is None or den in (None, 0):
        return None
    return num / den - 1.0


def safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den in (None, 0):
        return None
    return num / den


def ma(closes: list[float], n: int) -> float | None:
    return avg(closes[-n:]) if len(closes) >= n else None


def slope(closes: list[float], n: int, lag: int = 5) -> float | None:
    if len(closes) < n + lag:
        return None
    cur = avg(closes[-n:])
    prev = avg(closes[-n - lag : -lag])
    if cur is None or prev in (None, 0):
        return None
    return cur / prev - 1.0


def period_context(bars: list[dict[str, Any]], period: str) -> tuple[float | None, float | None, float | None, bool]:
    if not bars:
        return None, None, None, False
    groups: dict[str, dict[str, Any]] = {}
    for b in bars:
        d = db_date_to_date(b["date"])
        key = f"{d.isocalendar().year}-{d.isocalendar().week:02d}" if period == "weekly" else f"{d.year}-{d.month:02d}"
        groups[key] = b
    closes = [float(v["c"]) for _, v in sorted(groups.items())]
    if len(closes) < 20:
        return closes[-1] if closes else None, None, None, False
    ma20 = avg(closes[-20:])
    slope20 = None
    if len(closes) >= 25:
        prev = avg(closes[-25:-5])
        slope20 = ma20 / prev - 1.0 if ma20 is not None and prev not in (None, 0) else None
    return closes[-1], ma20, slope20, True


def build_context(trade: dict[str, str], bars_by_symbol: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    symbol = trade["symbol"]
    entry_ymd = ymd_int(trade["entry_date"])
    entry_epoch = date_to_db_epoch(ymd_to_date(entry_ymd))
    bars = [b for b in bars_by_symbol.get(symbol, []) if b["date"] <= entry_epoch]
    row: dict[str, Any] = {}
    if not bars:
        row["context_missing_reason"] = "no_confirmed_pan_daily_bar_on_or_before_entry"
        row["weekly_context_available"] = False
        row["monthly_context_available"] = False
        return row

    i = len(bars) - 1
    b = bars[i]
    prev = bars[i - 1] if i >= 1 else None
    closes = [float(x["c"]) for x in bars]
    highs = [float(x["h"]) for x in bars]
    lows = [float(x["l"]) for x in bars]
    vols = [float(x["v"] or 0) for x in bars]
    close = float(b["c"])
    high = float(b["h"])
    low = float(b["l"])
    open_ = float(b["o"])
    prev_close = float(prev["c"]) if prev else None
    eps = 1e-12
    rng = max(high - low, eps)
    entry_bar_date = db_date_to_date(b["date"])
    entry_date = datetime.fromisoformat(trade["entry_date"]).date()
    shift_days = (entry_date - entry_bar_date).days

    ma_vals = {n: ma(closes, n) for n in (5, 7, 20, 60, 100, 200)}
    high20 = max(highs[-20:]) if len(highs) >= 20 else None
    low20 = min(lows[-20:]) if len(lows) >= 20 else None
    high10 = max(highs[-10:]) if len(highs) >= 10 else None
    low10 = min(lows[-10:]) if len(lows) >= 10 else None
    weekly_close, weekly_ma20, weekly_slope, weekly_ok = period_context(bars, "weekly")
    monthly_close, monthly_ma20, monthly_slope, monthly_ok = period_context(bars, "monthly")

    row.update(
        {
            "entry_bar_date_used": entry_bar_date.isoformat(),
            "entry_bar_shifted": shift_days != 0,
            "entry_bar_shift_days": shift_days,
            "entry_close": close,
            "entry_open": open_,
            "entry_high": high,
            "entry_low": low,
            "entry_volume": b["v"],
            "prev_close": prev_close,
            "gap_pct": pct(open_, prev_close),
            "entry_day_return_pct": pct(close, prev_close),
            "body_pct": abs(close - open_) / prev_close if prev_close not in (None, 0) else None,
            "upper_wick_ratio": (high - max(open_, close)) / rng,
            "lower_wick_ratio": (min(open_, close) - low) / rng,
            "close_position_in_range": (close - low) / rng,
            "ma5": ma_vals[5],
            "ma7": ma_vals[7],
            "ma20": ma_vals[20],
            "ma60": ma_vals[60],
            "ma100": ma_vals[100],
            "ma200": ma_vals[200],
            "close_vs_ma7_pct": pct(close, ma_vals[7]),
            "close_vs_ma20_pct": pct(close, ma_vals[20]),
            "close_vs_ma60_pct": pct(close, ma_vals[60]),
            "close_vs_ma100_pct": pct(close, ma_vals[100]),
            "close_vs_ma200_pct": pct(close, ma_vals[200]),
            "ma7_slope_5d": slope(closes, 7),
            "ma20_slope_5d": slope(closes, 20),
            "ma60_slope_5d": slope(closes, 60),
            "above_ma7_count_recent": sum(1 for x in bars[-10:] if ma_vals[7] is not None and float(x["c"]) > ma_vals[7]),
            "below_ma7_count_recent": sum(1 for x in bars[-10:] if ma_vals[7] is not None and float(x["c"]) <= ma_vals[7]),
            "above_ma20_count_recent": sum(1 for x in bars[-10:] if ma_vals[20] is not None and float(x["c"]) > ma_vals[20]),
            "below_ma20_count_recent": sum(1 for x in bars[-10:] if ma_vals[20] is not None and float(x["c"]) <= ma_vals[20]),
            "above_ma60_count_recent": sum(1 for x in bars[-10:] if ma_vals[60] is not None and float(x["c"]) > ma_vals[60]),
            "below_ma60_count_recent": sum(1 for x in bars[-10:] if ma_vals[60] is not None and float(x["c"]) <= ma_vals[60]),
            "ret_3d": pct(close, closes[-4] if len(closes) >= 4 else None),
            "ret_5d": pct(close, closes[-6] if len(closes) >= 6 else None),
            "ret_10d": pct(close, closes[-11] if len(closes) >= 11 else None),
            "ret_20d": pct(close, closes[-21] if len(closes) >= 21 else None),
            "drawdown_10d": close / high10 - 1.0 if high10 not in (None, 0) else None,
            "drawdown_20d": close / high20 - 1.0 if high20 not in (None, 0) else None,
            "runup_10d": close / low10 - 1.0 if low10 not in (None, 0) else None,
            "runup_20d": close / low20 - 1.0 if low20 not in (None, 0) else None,
            "distance_from_20d_high_pct": close / high20 - 1.0 if high20 not in (None, 0) else None,
            "distance_from_20d_low_pct": close / low20 - 1.0 if low20 not in (None, 0) else None,
            "new_high_20d_flag": high20 is not None and high >= high20,
            "new_low_20d_flag": low20 is not None and low <= low20,
            "vol_ma5": avg(vols[-5:]) if len(vols) >= 5 else None,
            "vol_ma20": avg(vols[-20:]) if len(vols) >= 20 else None,
            "vol_ratio_t_20": safe_div(float(b["v"] or 0), avg(vols[-20:]) if len(vols) >= 20 else None),
            "vol_ratio5_20": safe_div(avg(vols[-5:]) if len(vols) >= 5 else None, avg(vols[-20:]) if len(vols) >= 20 else None),
            "weekly_close": weekly_close,
            "weekly_ma20": weekly_ma20,
            "weekly_close_vs_ma20_pct": pct(weekly_close, weekly_ma20),
            "weekly_ma20_slope": weekly_slope,
            "monthly_close": monthly_close,
            "monthly_ma20": monthly_ma20,
            "monthly_close_vs_ma20_pct": pct(monthly_close, monthly_ma20),
            "monthly_ma20_slope": monthly_slope,
            "weekly_context_available": weekly_ok,
            "monthly_context_available": monthly_ok,
            "context_missing_reason": "" if weekly_ok and monthly_ok else "weekly_or_monthly_history_insufficient",
            "champion_rank_at_entry": None,
            "champion_score_at_entry": None,
            "candidate_pool_presence_at_entry": None,
            "ranking_context_available": False,
        }
    )
    return row


def return_bucket(ret: float) -> str:
    if ret >= 0.05:
        return "large_win"
    if ret > 0.005:
        return "small_win"
    if ret <= -0.05:
        return "large_loss"
    if ret < -0.005:
        return "small_loss"
    return "flat"


def holding_bucket(days: int) -> str:
    if days == 0:
        return "same_day"
    if days <= 3:
        return "1_3d"
    if days <= 10:
        return "4_10d"
    if days <= 30:
        return "11_30d"
    return "31d_plus"


def bucketize(value: float | None, kind: str) -> str:
    if value is None:
        return "missing"
    if kind in {"close_vs_ma20", "ma20_slope"}:
        if value < -0.05:
            return "below_-5pct"
        if value < 0:
            return "below_0_to_-5pct"
        if value < 0.05:
            return "above_0_to_5pct"
        return "above_5pct"
    if kind == "distance_from_20d_high":
        if value > -0.02:
            return "near_high"
        if value > -0.08:
            return "moderate_below_high"
        return "far_below_high"
    if kind == "drawdown_20d":
        if value > -0.03:
            return "shallow"
        if value > -0.10:
            return "moderate"
        return "deep"
    return "unknown"


def summarize_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    rets = [r["gross_return_pct"] for r in rows if r.get("gross_return_pct") is not None]
    pnls = [r["gross_pnl"] for r in rows if r.get("gross_pnl") is not None]
    return {
        "count": len(rows),
        "win_rate": sum(1 for r in rows if r.get("gross_pnl", 0) > 0) / len(rows) if rows else None,
        "gross_return_mean": mean(rets) if rets else None,
        "gross_return_median": median(rets) if rets else None,
        "gross_pnl_total": sum(pnls) if pnls else 0.0,
    }


def grouped_summary(rows: list[dict[str, Any]], key_fn) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        groups[str(key_fn(r))].append(r)
    return {k: summarize_rows(v) for k, v in sorted(groups.items())}


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    clean_trades, excluded, clean_decision = load_clean_trades()
    symbols = {r["symbol"] for r in clean_trades}
    min_entry = min(ymd_int(r["entry_date"]) for r in clean_trades)
    max_entry = max(ymd_int(r["entry_date"]) for r in clean_trades)
    bars_by_symbol = fetch_bars(symbols, min_entry, max_entry)

    context_rows: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    max_feature_dates: list[int] = []
    for trade in clean_trades:
        base = {k: trade.get(k) for k in BASE_FIELDS if k in trade}
        gross_pnl = fnum(trade, "gross_pnl") or 0.0
        gross_ret = fnum(trade, "gross_return_pct") or 0.0
        holding_days = int(float(trade.get("holding_days") or 0))
        base.update(
            {
                "same_day_trade_flag": holding_days == 0,
                "clean_subset_flag": True,
                "tainted_excluded_flag": False,
                "context_features_computed": True,
                "gross_pnl": gross_pnl,
                "gross_return_pct": gross_ret,
                "holding_days": holding_days,
                "entry_price": fnum(trade, "entry_price"),
                "exit_price": fnum(trade, "exit_price"),
                "quantity": fnum(trade, "quantity"),
                "notional_entry": fnum(trade, "notional_entry"),
                "notional_exit": fnum(trade, "notional_exit"),
                "partial_entry_flag": boolish(trade.get("partial_entry_flag")),
                "partial_exit_flag": boolish(trade.get("partial_exit_flag")),
                "pnl_is_gross_only": boolish(trade.get("pnl_is_gross_only")),
                "actual_trade_win_flag": gross_pnl > 0,
                "actual_trade_loss_flag": gross_pnl < 0,
                "actual_trade_return_bucket": return_bucket(gross_ret),
                "holding_days_bucket": holding_bucket(holding_days),
            }
        )
        context = build_context(trade, bars_by_symbol)
        row = {**base, **context}
        if row.get("entry_bar_date_used"):
            feature_ymd = ymd_int(row["entry_bar_date_used"])
            max_feature_dates.append(feature_ymd)
            if feature_ymd > ymd_int(trade["entry_date"]):
                violations.append({"trade_id": trade["normalized_trade_id"], "reason": "feature_date_after_entry"})
        context_rows.append(row)

    excluded_ids = set(clean_decision["tainted_trade_ids"])
    leaked = [r["normalized_trade_id"] for r in context_rows if r["normalized_trade_id"] in excluded_ids]
    if leaked:
        violations.append({"reason": "tainted_trade_leak", "trade_ids": leaked[:20]})

    context_feature_keys = [k for k in CONTEXT_FIELDS if k not in {"actual_trade_win_flag", "actual_trade_loss_flag", "actual_trade_return_bucket", "holding_days_bucket"}]
    missing_cells = sum(1 for r in context_rows for k in context_feature_keys if r.get(k) in (None, ""))
    total_cells = len(context_rows) * len(context_feature_keys)
    missing_rate = missing_cells / total_cells if total_cells else 1.0
    weekly_cov = sum(1 for r in context_rows if r.get("weekly_context_available")) / len(context_rows)
    monthly_cov = sum(1 for r in context_rows if r.get("monthly_context_available")) / len(context_rows)
    ranking_cov = sum(1 for r in context_rows if r.get("ranking_context_available")) / len(context_rows)

    gross_pnls = [r["gross_pnl"] for r in context_rows]
    gross_rets = [r["gross_return_pct"] for r in context_rows]
    holdings = [r["holding_days"] for r in context_rows]
    decision = "ready_for_counterfactual_audit"
    if violations or len(context_rows) != clean_decision["closed_trade_count_clean"] or leaked:
        decision = "needs_trade_subset_adjustment"
    elif len(context_rows) < 950:
        decision = "insufficient_context_data"
    elif missing_rate > 0.35:
        decision = "needs_context_field_repair"

    summary = {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "input_closed_trade_count": clean_decision["closed_trade_count_total"],
        "clean_trade_count": len(clean_trades),
        "excluded_tainted_trade_count": len(excluded),
        "context_rows_generated": len(context_rows),
        "date_min": min(r["entry_date"] for r in context_rows) if context_rows else None,
        "date_max": max(r["entry_date"] for r in context_rows) if context_rows else None,
        "symbol_count": len({r["symbol"] for r in context_rows}),
        "long_trade_count": sum(1 for r in context_rows if r["side"] == "long"),
        "short_trade_count": sum(1 for r in context_rows if r["side"] == "short"),
        "gross_pnl_total": sum(gross_pnls),
        "gross_return_mean": mean(gross_rets) if gross_rets else None,
        "gross_return_median": median(gross_rets) if gross_rets else None,
        "win_rate_gross": sum(1 for v in gross_pnls if v > 0) / len(gross_pnls) if gross_pnls else None,
        "avg_holding_days": mean(holdings) if holdings else None,
        "median_holding_days": median(holdings) if holdings else None,
        "context_field_missing_rate": missing_rate,
        "weekly_context_coverage": weekly_cov,
        "monthly_context_coverage": monthly_cov,
        "ranking_context_coverage": ranking_cov,
        "ready_for_counterfactual_audit": decision == "ready_for_counterfactual_audit",
        "next_recommended_axis": "actual_trade_counterfactual_rule_audit_v1" if decision == "ready_for_counterfactual_audit" else "context_field_repair_v1",
        "context_features_computed": True,
    }

    side_bucket = {
        "long": summarize_rows([r for r in context_rows if r["side"] == "long"]),
        "short": summarize_rows([r for r in context_rows if r["side"] == "short"]),
        "by_close_vs_ma20_bucket": grouped_summary(context_rows, lambda r: f"{r['side']}|{bucketize(r.get('close_vs_ma20_pct'), 'close_vs_ma20')}"),
        "by_ma20_slope_bucket": grouped_summary(context_rows, lambda r: f"{r['side']}|{bucketize(r.get('ma20_slope_5d'), 'ma20_slope')}"),
        "by_distance_from_20d_high_bucket": grouped_summary(context_rows, lambda r: f"{r['side']}|{bucketize(r.get('distance_from_20d_high_pct'), 'distance_from_20d_high')}"),
        "by_drawdown_20d_bucket": grouped_summary(context_rows, lambda r: f"{r['side']}|{bucketize(r.get('drawdown_20d'), 'drawdown_20d')}"),
        "by_holding_days_bucket": grouped_summary(context_rows, lambda r: f"{r['side']}|{r['holding_days_bucket']}"),
        "by_partial_entry_flag": grouped_summary(context_rows, lambda r: f"{r['side']}|partial_entry={r['partial_entry_flag']}"),
        "by_partial_exit_flag": grouped_summary(context_rows, lambda r: f"{r['side']}|partial_exit={r['partial_exit_flag']}"),
    }

    outcome_summary = {
        "note": "descriptive outcome summaries only; no causal claims",
        "by_return_bucket": grouped_summary(context_rows, lambda r: f"{r['side']}|{r['actual_trade_return_bucket']}"),
        "by_close_vs_ma20_bucket": side_bucket["by_close_vs_ma20_bucket"],
        "by_drawdown_20d_bucket": side_bucket["by_drawdown_20d_bucket"],
        "by_holding_days_bucket": side_bucket["by_holding_days_bucket"],
    }

    field_missing = {
        k: sum(1 for r in context_rows if r.get(k) in (None, "")) / len(context_rows) if context_rows else 1.0
        for k in context_feature_keys
    }
    coverage = {
        "candidate_name": CANDIDATE_NAME,
        "row_count": len(context_rows),
        "field_missing_rates": field_missing,
        "weekly_context_coverage": weekly_cov,
        "monthly_context_coverage": monthly_cov,
        "ranking_context_coverage": ranking_cov,
        "confirmed_bar_source": "daily_bars.source='pan'",
    }

    no_lookahead = {
        "candidate_name": CANDIDATE_NAME,
        "pass": not violations,
        "violations": violations,
        "max_feature_date_by_row_check": {
            "checked_rows": len(max_feature_dates),
            "max_feature_ymd": max(max_feature_dates) if max_feature_dates else None,
            "all_feature_dates_lte_entry_date": not any(v.get("reason") == "feature_date_after_entry" for v in violations if isinstance(v, dict)),
        },
        "tainted_trade_exclusion_check": {
            "excluded_tainted_trade_count_expected": len(excluded_ids),
            "leaked_tainted_trade_count": len(leaked),
            "leaked_tainted_trade_ids": leaked,
        },
        "context_features_computed_from_confirmed_only": True,
        "confirmed_source_filter": "daily_bars.source='pan'",
        "provisional_yahoo_bars_used": False,
        "exit_date_used_for_entry_features": False,
        "future_market_bars_used": False,
    }

    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision,
        "complete": True,
        "required_artifacts": [
            "actual_trade_decision_context.csv",
            "actual_trade_decision_context.json",
            "excluded_tainted_trades.csv",
            "context_reconstruction_summary.json",
            "context_field_coverage.json",
            "no_lookahead_audit.json",
            "side_bucket_summary.json",
            "outcome_summary_by_context_bucket.json",
        ],
    }

    excluded_fields = list(excluded[0].keys()) if excluded else ["normalized_trade_id"]
    write_csv(run_root / "actual_trade_decision_context.csv", context_rows)
    write_json(run_root / "actual_trade_decision_context.json", {"candidate_name": CANDIDATE_NAME, "row_count": len(context_rows), "rows": context_rows})
    write_csv(run_root / "excluded_tainted_trades.csv", excluded, excluded_fields)
    write_json(run_root / "context_reconstruction_summary.json", summary)
    write_json(run_root / "context_field_coverage.json", coverage)
    write_json(run_root / "no_lookahead_audit.json", no_lookahead)
    write_json(run_root / "side_bucket_summary.json", side_bucket)
    write_json(run_root / "outcome_summary_by_context_bucket.json", outcome_summary)
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)

    print(json.dumps({"run_root": str(run_root), "decision": decision, "rows": len(context_rows)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
