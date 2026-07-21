from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import duckdb

from app.backend.services.codex_bridge_service import get_runtime_stock_db_status
from app.backend.services.watchlist import load_watchlist_codes, resolve_watchlist_path

LABELS = {"breakout_watch": "上抜け待ち", "breakout_candidate": "上抜け成立候補", "sell_preparation": "売り準備", "decline_candidate": "下落初動候補", "rebound_warning": "反発警戒", "undetermined": "方向未確定"}


def _ymd(value):
    value = int(value)
    return value if 10_000_000 <= value <= 99_999_999 else int(datetime.fromtimestamp(value, timezone.utc).strftime("%Y%m%d"))


def _sma(values, period):
    return mean(values[-period:]) if len(values) >= period else None


def _period_bars(rows, key):
    groups = defaultdict(list)
    for row in rows:
        groups[key(row["ymd"])].append(row)
    return [{"close": group[-1]["close"], "high": max(x["high"] for x in group), "low": min(x["low"] for x in group)} for _, group in sorted(groups.items())]


def classify_chart_state(rows):
    if len(rows) < 65:
        return {"state": "undetermined", "state_label": LABELS["undetermined"], "confidence": "low", "reasons": ["insufficient_history"], "image_review_required": False}
    closes = [x["close"] for x in rows]
    latest, prior = rows[-1], rows[-2]
    ma7, ma20, ma60 = (_sma(closes, n) for n in (7, 20, 60))
    prior_ma20 = mean(closes[-21:-1])
    high20 = max(x["high"] for x in rows[-21:-1])
    low20 = min(x["low"] for x in rows[-21:-1])
    high60, low60 = max(x["high"] for x in rows[-60:]), min(x["low"] for x in rows[-60:])
    range_pos = (latest["close"] - low60) / max(high60 - low60, 1e-9)
    span = max(latest["high"] - latest["low"], 1e-9)
    close_pos = (latest["close"] - latest["low"]) / span
    upper_wick = (latest["high"] - max(latest["open"], latest["close"])) / span
    volume_ratio = latest["volume"] / max(mean(x["volume"] for x in rows[-21:-1]), 1)
    weekly = _period_bars(rows[-280:], lambda y: datetime.strptime(str(y), "%Y%m%d").isocalendar()[:2])
    monthly = _period_bars(rows[-760:], lambda y: y // 100)
    wc, mc = [x["close"] for x in weekly], [x["close"] for x in monthly]
    w4, w13, m3, m6 = _sma(wc, 4), _sma(wc, 13), _sma(mc, 3), _sma(mc, 6)
    daily_up = latest["close"] > ma20 > ma60
    weekly_up = bool(w4 and w13 and weekly[-1]["close"] > w4 > w13)
    monthly_up = bool(m3 and m6 and monthly[-1]["close"] > m3 > m6)
    if latest["close"] > high20 and close_pos >= .65 and volume_ratio >= 1.2 and weekly_up and monthly_up:
        state, confidence, reasons = "breakout_candidate", "medium", ["20d_high_break", "volume", "weekly_monthly_up"]
    elif daily_up and weekly_up and range_pos >= .8:
        state, confidence, reasons = "breakout_watch", "medium", ["daily_weekly_up", "near_60d_high"]
    elif range_pos >= .75 and latest["close"] < latest["open"] and close_pos < .4 and upper_wick >= .25 and not monthly_up:
        state, confidence, reasons = "sell_preparation", "low", ["high_zone", "bearish_upper_wick", "monthly_not_up"]
    elif prior["close"] >= prior_ma20 and latest["close"] < ma20 and close_pos < .4 and (volume_ratio >= 1 or not weekly_up):
        state, confidence, reasons = "decline_candidate", "low", ["ma20_break", "weak_close", "volume_or_weekly"]
    elif prior["close"] < prior_ma20 and latest["close"] > ma7 and close_pos >= .65 and range_pos <= .45:
        state, confidence, reasons = "rebound_warning", "low", ["low_zone", "ma7_reclaim", "firm_close"]
    else:
        state, confidence, reasons = "undetermined", "low", ["no_composite_match"]
    return {"state": state, "state_label": LABELS[state], "confidence": confidence, "reasons": reasons, "levels": {"breakout_reference": high20, "support_reference": low20, "ma7": ma7, "ma20": ma20, "ma60": ma60}, "context": {"close": latest["close"], "range_pos60": range_pos, "close_pos": close_pos, "upper_wick_ratio": upper_wick, "volume_ratio20": volume_ratio, "daily_up": daily_up, "weekly_up": weekly_up, "monthly_up": monthly_up}, "image_review_required": state != "undetermined" or abs(latest["close"] / high20 - 1) <= .01}


def build_board(db_path, watchlist_path, as_of=None):
    codes = load_watchlist_codes(str(watchlist_path))
    conn = duckdb.connect(str(db_path), read_only=True)
    raw = conn.execute(f"select code,date,o,h,l,c,v,source from daily_bars where code in ({','.join('?' for _ in codes)}) order by code,date", codes).fetchall()
    conn.close()
    grouped = defaultdict(list)
    for code, date, o, h, l, c, v, source in raw:
        ymd = _ymd(date)
        if not as_of or ymd <= as_of:
            grouped[str(code)].append({"ymd": ymd, "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": int(v or 0), "source": source})
    items = [{"code": code, "as_of": grouped[code][-1]["ymd"] if grouped[code] else None, **classify_chart_state(grouped[code])} for code in codes]
    counts = defaultdict(int)
    for item in items: counts[item["state"]] += 1
    return {"schema_version": "tradex_watchlist_chart_state_board_v1", "artifact_role": "authoritative_research_snapshot", "boundary_owner": "TRADEX", "review_only": True, "signal_quality_validated": False, "fixed_conditions": {"universe": str(watchlist_path), "provisional_bars": False}, "summary": {"watchlist_count": len(codes), "classified_count": len(items), "state_counts": dict(counts), "image_review_count": sum(x["image_review_required"] for x in items)}, "items": items, "not_changed": ["MeeMee", "ranking", "runtime_db", "automatic_trade", "intraday_judgment"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path)
    parser.add_argument("--watchlist-path", type=Path, default=Path(resolve_watchlist_path()))
    parser.add_argument("--as-of", type=int)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runtime = get_runtime_stock_db_status() if args.db_path is None else None
    payload = build_board(args.db_path or Path(runtime["selected_runtime_db_path"]), args.watchlist_path, args.as_of)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(args.output)


if __name__ == "__main__": main()
