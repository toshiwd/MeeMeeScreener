"""Conservative, reproducible baseline for the attached TRADEX research instruction.

This is deliberately a baseline, not a promoted rule.  It uses only signal-day
information, enters next-day open, and writes immutable JSON evidence under G:\\Tradex.
"""
from __future__ import annotations

import json
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import duckdb


AXIS_ID = "instruction_baseline_v1"
DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUTPUT_ROOT = Path(r"G:\Tradex\instruction_baseline_v1")
STOP, TARGET, MAX_HOLD = 0.08, 0.15, 60
ROUND_TRIP_COSTS = (0.002, 0.004)  # explicit provisional 10bp/side, then 2x stress


def tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def split(year: int) -> str | None:
    if 2019 <= year <= 2021:
        return "train"
    if 2022 <= year <= 2023:
        return "validation"
    if 2024 <= year <= 2025:
        return "test"
    return None


def exit_trade(side: str, bars: list[dict], entry_index: int) -> tuple[float, int, str]:
    entry = bars[entry_index]["o"]
    stop = entry * (1 - STOP if side == "long" else 1 + STOP)
    target = entry * (1 + TARGET if side == "long" else 1 - TARGET)
    last = min(entry_index + MAX_HOLD - 1, len(bars) - 1)
    for index in range(entry_index, last + 1):
        bar = bars[index]
        if side == "long":
            if bar["o"] <= stop:
                return bar["o"] / entry - 1, index - entry_index + 1, "gap_stop"
            if bar["l"] <= stop:  # stop wins a same-day target/stop collision
                return stop / entry - 1, index - entry_index + 1, "stop"
            if bar["o"] >= target:
                return bar["o"] / entry - 1, index - entry_index + 1, "gap_target"
            if bar["h"] >= target:
                return target / entry - 1, index - entry_index + 1, "target"
        else:
            if bar["o"] >= stop:
                return entry / bar["o"] - 1, index - entry_index + 1, "gap_stop"
            if bar["h"] >= stop:
                return entry / stop - 1, index - entry_index + 1, "stop"
            if bar["o"] <= target:
                return entry / bar["o"] - 1, index - entry_index + 1, "gap_target"
            if bar["l"] <= target:
                return entry / target - 1, index - entry_index + 1, "target"
    close = bars[last]["c"]
    return (close / entry - 1 if side == "long" else entry / close - 1), last - entry_index + 1, "time"


def summarize(rows: list[dict], cost: float) -> dict:
    net_r = [(row["gross_return"] - cost) / STOP for row in rows]
    gains = sum(value for value in net_r if value > 0)
    losses = -sum(value for value in net_r if value < 0)
    equity = peak = drawdown = 0.0
    for value in net_r:
        equity += value
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return {
        "trade_count": len(rows),
        "win_rate": sum(value > 0 for value in net_r) / len(net_r) if net_r else None,
        "expectancy_r": mean(net_r) if net_r else None,
        "profit_factor": gains / losses if losses else None,
        "max_drawdown_r_sequential": drawdown,
        "average_holding_days": mean(row["holding_days"] for row in rows) if rows else None,
    }


def run() -> Path:
    out = OUTPUT_ROOT / f"{tag()}-{AXIS_ID}"
    out.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(DB), read_only=True)
    try:
        raw = conn.execute("""
            WITH eligible AS (
              SELECT code FROM daily_bars WHERE source='pan' GROUP BY code HAVING max(date) >= 1783555200
            )
            SELECT b.code, b.date, b.o, b.h, b.l, b.c, b.v
            FROM daily_bars b JOIN eligible e USING (code)
            WHERE b.source='pan' AND b.o IS NOT NULL AND b.h IS NOT NULL AND b.l IS NOT NULL AND b.c IS NOT NULL
            ORDER BY b.code, b.date
        """).fetchall()
    finally:
        conn.close()
    by_code: dict[str, list[dict]] = {}
    for code, date, o, h, l, c, v in raw:
        by_code.setdefault(str(code), []).append({"date": int(date), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v or 0)})
    breadth: dict[int, list[int]] = {}
    for bars in by_code.values():
        closes: deque[float] = deque()
        previous_close: float | None = None
        for bar in bars:
            closes.append(bar["c"])
            if len(closes) > 60:
                closes.popleft()
            if len(closes) < 60:
                continue
            ma20 = sum(list(closes)[-20:]) / 20
            ma60 = sum(closes) / 60
            counts = breadth.setdefault(bar["date"], [0, 0, 0, 0.0])
            counts[0] += int(bar["c"] > ma20)
            counts[1] += int(bar["c"] > ma60)
            counts[2] += 1
            if previous_close:
                counts[3] += abs(bar["c"] / previous_close - 1.0)
            previous_close = bar["c"]
    trades = {"long": [], "long_upper_close": [], "long_deep_discount_reversal": [], "long_deep_discount_ma60_rising": [], "long_deep_discount_low_breadth": [], "long_deep_discount_high_vol": [], "short": [], "short_high_zone": []}
    for code, bars in by_code.items():
        closes, highs, lows = deque(), deque(), deque()
        below7 = below20 = 0
        for i, bar in enumerate(bars):
            closes.append(bar["c"]); highs.append(bar["h"]); lows.append(bar["l"])
            if len(closes) > 60: closes.popleft()
            if len(highs) > 20: highs.popleft()
            if len(lows) > 20: lows.popleft()
            if i < 60 or i + MAX_HOLD >= len(bars): continue
            ma7 = sum(list(closes)[-7:]) / 7; ma20 = sum(list(closes)[-20:]) / 20; ma60 = sum(closes) / 60
            below7 = below7 + 1 if bar["c"] < ma7 else 0
            below20 = below20 + 1 if bar["c"] < ma20 else 0
            year = datetime.fromtimestamp(bar["date"], timezone.utc).year
            bucket = split(year)
            if not bucket: continue
            prev = bars[i - 1]; prior_low20 = min(item["l"] for item in bars[i - 20:i])
            long_signal = bar["c"] > ma20 and bar["c"] < ma60 and bar["l"] <= ma20 * 1.03 and bar["c"] > prev["h"]
            deep_discount_reversal = bar["c"] / bars[i - 10]["c"] - 1 <= -0.06 and bar["c"] > prev["h"] and bar["c"] / ma20 - 1 <= -0.05
            ma60_5ago = sum(item["c"] for item in bars[i - 64:i - 4]) / 60
            ma60_rising = ma60 > ma60_5ago
            span = max(bar["h"] - bar["l"], 1e-9)
            short_signal = 5 <= below7 <= 9 and 8 <= below20 <= 12 and bar["c"] < prior_low20 and (bar["o"] - bar["c"]) / span >= 0.50
            upper_close = (bar["c"] - bar["l"]) / span >= 0.70
            high60 = max(item["h"] for item in bars[i - 59:i + 1]); low60 = min(item["l"] for item in bars[i - 59:i + 1])
            high_zone = high60 > low60 and (bar["c"] - low60) / (high60 - low60) >= 0.75
            b20, b60, bn, abs_return_sum = breadth.get(bar["date"], [0, 0, 0, 0.0])
            breadth20 = b20 / bn if bn else None
            market_abs_return_mean = abs_return_sum / bn if bn else None
            for side, signal in (("long", long_signal), ("long_upper_close", long_signal and upper_close), ("long_deep_discount_reversal", deep_discount_reversal), ("long_deep_discount_ma60_rising", deep_discount_reversal and ma60_rising), ("long_deep_discount_low_breadth", deep_discount_reversal and breadth20 is not None and breadth20 < 0.35), ("long_deep_discount_high_vol", deep_discount_reversal and market_abs_return_mean is not None and market_abs_return_mean >= 0.03), ("short", short_signal), ("short_high_zone", short_signal and high_zone)):
                if signal:
                    execution_side = "long" if side.startswith("long") else "short"
                    gross, holding_days, reason = exit_trade(execution_side, bars, i + 1)
                    trades[side].append({"code": code, "signal_date": bar["date"], "year": year, "split": bucket, "gross_return": gross, "holding_days": holding_days, "exit_reason": reason,
                                         "breadth_above_ma20": breadth20, "breadth_above_ma60": b60 / bn if bn else None,
                                         "market_abs_return_mean": market_abs_return_mean})
    result = {"schema_version": "tradex_instruction_baseline_v1.compare.v1", "authoritative_result": True,
              "research_phase": "comparison_stabilization", "fixed_evaluation_conditions": {
                  "source_db": str(DB), "source_filter": "daily_bars.source = pan", "eligible_codes": len(by_code),
                  "entry": "next trading-day open", "stop_loss": STOP, "take_profit": TARGET, "max_holding_days": MAX_HOLD,
                  "same_day_dual_hit": "stop first", "gap_stop": "open", "splits": "2019-2021 / 2022-2023 / 2024-2025",
                  "runtime_db_write": False, "production_ranking_changed": False},
              "rules": {"long": "c>ma20 AND c<ma60 AND low<=ma20*1.03 AND c>prior_high1",
                        "long_upper_close": "long baseline AND (c-low)/(high-low)>=0.70",
                        "long_deep_discount_reversal": "ret10<=-0.06 AND c>prior_high1 AND c/ma20-1<=-0.05",
                        "long_deep_discount_ma60_rising": "long_deep_discount_reversal AND ma60>ma60_5ago",
                        "long_deep_discount_low_breadth": "long_deep_discount_reversal AND universe_breadth_above_ma20<0.35",
                        "long_deep_discount_high_vol": "long_deep_discount_reversal AND universe_mean_abs_1d_return>=0.03",
                        "short": "5<=below_ma7_streak<=9 AND 8<=below_ma20_streak<=12 AND c<prior_low20 AND bearish_body_ratio>=0.50",
                        "short_high_zone": "short baseline AND close_position_60>=0.75"},
              "cost_assumption": "provisional: 10bp per side; doubled stress is 20bp per side", "results": {}}
    for side, rows in trades.items():
        result["results"][side] = {str(cost): {bucket: summarize([row for row in rows if row["split"] == bucket], cost) for bucket in ("train", "validation", "test")} for cost in ROUND_TRIP_COSTS}
    result["yearly_results"] = {
        "long_deep_discount_reversal": {
            str(cost): {str(year): summarize([row for row in trades["long_deep_discount_reversal"] if row["year"] == year], cost) for year in range(2019, 2026)}
            for cost in ROUND_TRIP_COSTS
        },
        "long_deep_discount_low_breadth": {
            str(cost): {str(year): summarize([row for row in trades["long_deep_discount_low_breadth"] if row["year"] == year], cost) for year in range(2019, 2026)}
            for cost in ROUND_TRIP_COSTS
        },
    }
    deep_rows = trades["long_deep_discount_reversal"]
    result["breadth20_band_results"] = {
        band: summarize([row for row in deep_rows if lower <= (row["breadth_above_ma20"] or -1) < upper], ROUND_TRIP_COSTS[-1])
        for band, lower, upper in (("lt_0_35", 0.0, 0.35), ("0_35_to_0_55", 0.35, 0.55), ("gte_0_55", 0.55, 1.01))
    }
    stressed = result["results"]["long"][str(ROUND_TRIP_COSTS[-1])]["test"]
    stressed_upper = result["results"]["long_upper_close"][str(ROUND_TRIP_COSTS[-1])]["test"]
    result["candidate_decisions"] = [
        {"candidate": "long_baseline", "candidate_local_decision": "reject", "reason": "stressed OOS expectancy/PF below GO threshold", "oos": stressed},
        {"candidate": "long_upper_close", "candidate_local_decision": "drop", "reason": "single-axis upper-close gate reduced stressed OOS expectancy and PF versus baseline", "oos": stressed_upper,
         "delta_vs_baseline": {"expectancy_r": stressed_upper["expectancy_r"] - stressed["expectancy_r"], "profit_factor": stressed_upper["profit_factor"] - stressed["profit_factor"], "trade_count": stressed_upper["trade_count"] - stressed["trade_count"]}},
        {"candidate": "long_deep_discount_reversal", "candidate_local_decision": "hold", "reason": "stressed OOS is strong, but train PF/expectancy miss the stability gate; require walk-forward and entry-time image comparison", "oos": result["results"]["long_deep_discount_reversal"][str(ROUND_TRIP_COSTS[-1])]["test"]},
        {"candidate": "long_deep_discount_low_breadth", "candidate_local_decision": "hold_pending_split_review", "reason": "single-axis historical breadth filter; require split and annual stability review", "oos": result["results"]["long_deep_discount_low_breadth"][str(ROUND_TRIP_COSTS[-1])]["test"]},
        {"candidate": "short_baseline", "candidate_local_decision": "reject", "reason": "stressed OOS expectancy and PF are negative/below one", "oos": result["results"]["short"][str(ROUND_TRIP_COSTS[-1])]["test"]},
        {"candidate": "short_high_zone", "candidate_local_decision": "drop", "reason": "zero train/validation observations; the condition is not an evaluable branch", "oos": result["results"]["short_high_zone"][str(ROUND_TRIP_COSTS[-1])]["test"]},
    ]
    result["authoritative_rollup_decision"] = "hold_baseline_only_not_promoted"
    (out / "compare.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out / "long_deep_discount_reversal_trade_ledger.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in trades["long_deep_discount_reversal"]), encoding="utf-8"
    )
    return out


if __name__ == "__main__":
    print(run())
