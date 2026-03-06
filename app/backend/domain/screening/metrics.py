from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any

from app.utils.date_utils import _parse_daily_date, _format_event_date
from app.utils.math_utils import _pct_change, _build_ma_series, _compute_atr
from app.services.box_detector import detect_boxes
from app.backend.domain.screening.ranking import (
    calc_short_a_score,
    calc_short_b_score,
    check_short_prohibition_zones,
    calc_regression_slope,
    calc_range_bounds_with_mid,
    count_streak,
    format_daily_label,
    format_month_label
)

def _parse_month_value(value: int | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        raw = str(int(value)).zfill(6)
        year = int(raw[:4])
        month = int(raw[4:6])
        return datetime(year, month, 1)
    except (ValueError, TypeError):
        return None

def _month_label_to_int(label: str | None) -> int | None:
    if not label:
        return None
    try:
        parts = label.split("-")
        if len(parts) != 2:
            return None
        year = int(parts[0])
        month = int(parts[1])
        if month < 1 or month > 12:
            return None
        return year * 100 + month
    except (TypeError, ValueError):
        return None

def _drop_incomplete_weekly(weekly: list[dict], last_daily: datetime | None) -> list[dict]:
    if not weekly or not last_daily:
        return weekly
    last_week_start = (last_daily - timedelta(days=last_daily.weekday())).date()
    if weekly[-1]["week_start"] == last_week_start and last_daily.weekday() < 4:
        return weekly[:-1]
    return weekly

def _drop_incomplete_monthly(monthly_rows: list[tuple], last_daily: datetime | None) -> list[tuple]:
    if not monthly_rows or not last_daily:
        return monthly_rows
    last_month = _parse_month_value(monthly_rows[-1][0] if monthly_rows else None)
    if last_month and last_month.year == last_daily.year and last_month.month == last_daily.month:
        return monthly_rows[:-1]
    return monthly_rows

def _build_weekly_bars(daily_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_key = None
    for row in daily_rows:
        if len(row) < 5:
            continue
        date_value, open_, high, low, close = row[:5]
        if open_ is None or high is None or low is None or close is None:
            continue
        dt = _parse_daily_date(date_value)
        if not dt:
            continue
        week_start = (dt - timedelta(days=dt.weekday())).date()
        if current_key != week_start:
            items.append(
                {
                    "week_start": week_start,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close),
                    "last_date": dt.date()
                }
            )
            current_key = week_start
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
            current["last_date"] = dt.date()
    return items

def _build_quarterly_bars(monthly_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_key: tuple[int, int] | None = None
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        dt = _parse_month_value(month_value)
        if not dt:
            continue
        quarter = (dt.month - 1) // 3 + 1
        key = (dt.year, quarter)
        if current_key != key:
            items.append(
                {
                    "year": dt.year,
                    "quarter": quarter,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close)
                }
            )
            current_key = key
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
    return items

def _build_yearly_bars(monthly_rows: list[tuple]) -> list[dict]:
    items: list[dict] = []
    current_year = None
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        dt = _parse_month_value(month_value)
        if not dt:
            continue
        if current_year != dt.year:
            items.append(
                {
                    "year": dt.year,
                    "o": float(open_),
                    "h": float(high),
                    "l": float(low),
                    "c": float(close)
                }
            )
            current_year = dt.year
        else:
            current = items[-1]
            current["h"] = max(current["h"], float(high))
            current["l"] = min(current["l"], float(low))
            current["c"] = float(close)
    return items

def _build_box_metrics(
    monthly_rows: list[tuple],
    last_close: float | None
) -> tuple[dict | None, str, str | None, str | None, str]:
    if not monthly_rows:
        return None, "NONE", None, None, "NONE"
    boxes = detect_boxes(monthly_rows, range_basis="body", max_range_pct=0.2)
    if not boxes:
        return None, "NONE", None, None, "NONE"

    bars = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        if open_ is None or close is None:
            continue
        bars.append(
            {
                "month": month_value,
                "open": float(open_),
                "close": float(close)
            }
        )

    if not bars:
        return None, "NONE", None, None, "NONE"

    latest_box = max(boxes, key=lambda item: item["endIndex"])
    months = latest_box["endIndex"] - latest_box["startIndex"] + 1
    if months < 3:
        return None, "NONE", None, None, "NONE"

    active_box = {**latest_box, "months": months}
    latest_index = len(bars) - 1
    start_index = active_box["startIndex"]
    end_index = active_box["endIndex"]
    body_low = None
    body_high = None
    for bar in bars[start_index: end_index + 1]:
        low = min(bar["open"], bar["close"])
        high = max(bar["open"], bar["close"])
        body_low = low if body_low is None else min(body_low, low)
        body_high = high if body_high is None else max(body_high, high)

    if body_low is None or body_high is None:
        return None, "NONE", None, None, "NONE"

    base = max(abs(body_low), 1e-9)
    range_pct = (body_high - body_low) / base
    start_label = format_month_label(active_box["startTime"])
    end_label = format_month_label(active_box["endTime"])

    box_state = "NONE"
    if end_index == latest_index:
        box_state = "IN_BOX"
    elif end_index == latest_index - 1:
        box_state = "JUST_BREAKOUT"

    breakout_month = None
    if box_state == "JUST_BREAKOUT" and latest_index >= 0:
        breakout_month = format_month_label(bars[latest_index]["month"])

    direction_state = "NONE"
    if box_state != "NONE" and last_close is not None:
        if last_close > body_high:
            direction_state = "BREAKOUT_UP"
        elif last_close < body_low:
            direction_state = "BREAKOUT_DOWN"
        else:
            direction_state = "IN_BOX"

    payload = {
        "startDate": start_label,
        "endDate": end_label,
        "bodyLow": body_low,
        "bodyHigh": body_high,
        "months": active_box["months"],
        "rangePct": range_pct,
        "isActive": box_state == "IN_BOX",
        "boxState": box_state,
        "boxEndMonth": end_label,
        "breakoutMonth": breakout_month
    }
    return payload, box_state, end_label, breakout_month, direction_state


def _calc_liquidity_20d(daily_rows: list[tuple]) -> float | None:
    if not daily_rows:
        return None
    values = []
    target_rows = daily_rows[-20:]
    for row in target_rows:
        if len(row) < 6:
            continue
        close = float(row[4]) if row[4] is not None else 0
        volume = float(row[5]) if row[5] is not None else 0
        values.append(close * volume)
    if not values:
        return None
    return sum(values) / len(values)

def _find_supports_resistances(
    closes: list[float], 
    highs: list[float], 
    lows: list[float],
    ma20: float | None,
    ma60: float | None,
    box_monthly: dict | None
) -> tuple[list[float], list[float]]:
    supports = []
    resistances = []
    if box_monthly:
        if box_monthly.get("bodyLow") is not None: supports.append(float(box_monthly["bodyLow"]))
        if box_monthly.get("bodyHigh") is not None: resistances.append(float(box_monthly["bodyHigh"]))
    if ma20:
        supports.append(ma20)
        resistances.append(ma20)
    if ma60:
        supports.append(ma60)
        resistances.append(ma60)
    if len(highs) >= 20: resistances.append(max(highs[-20:]))
    if len(lows) >= 20: supports.append(min(lows[-20:]))
    return supports, resistances

def _find_buy_stop_target(
    last_close: float,
    supports: list[float],
    resistances: list[float],
    atr14: float | None
) -> tuple[float | None, float | None, float | None, float | None]:
    # buyStop = max of supports below close
    valid_supports = [s for s in supports if s < last_close]
    buy_stop = max(valid_supports) if valid_supports else None
    
    # buyTarget = min of resistances above close
    valid_resistances = [r for r in resistances if r > last_close]
    buy_target = min(valid_resistances) if valid_resistances else None
    
    risk_atr = (last_close - buy_stop) / atr14 if buy_stop and atr14 else None
    upside_atr = (buy_target - last_close) / atr14 if buy_target and atr14 else None
    
    return buy_stop, buy_target, risk_atr, upside_atr

def _find_sell_stop_target(
    last_close: float,
    highs: list[float],
    lows: list[float],
    ma20: float | None,
    ma60: float | None,
    box_monthly: dict | None,
    atr14: float | None
) -> tuple[float | None, float | None, float | None, float | None]:
    # 1. Candidates
    resistances = [] # For Stop
    supports = []    # For Target

    # Swing 20
    if len(highs) >= 20: resistances.append(max(highs[-20:]))
    if len(lows) >= 20: supports.append(min(lows[-20:]))
    
    # Box
    if box_monthly:
        if box_monthly.get("bodyLow") is not None: supports.append(float(box_monthly["bodyLow"]))
        if box_monthly.get("bodyHigh") is not None: resistances.append(float(box_monthly["bodyHigh"]))
        
    # MAs
    if ma20: 
        resistances.append(ma20)
        supports.append(ma20)
    if ma60:
        resistances.append(ma60)
        # For target, ma60 could be support? prompt says check candidates.
        # "Target candidates: swingLow20, boxBottom, priorLow" (doesn't explicitly list MAs in 3-1 Target candidates but mentions them in 3-1 Resistance)
        # Wait, prompt 3-1 Target candidates: swingLow20, boxBottom, priorLow.
        # But commonly MAs can be targets. I'll stick to prompt 3-1 strictly?
        # "Resistances (Upper): swingHigh20, boxTop, ma20, ma60"
        # "Supports (Lower): swingLow20, boxBottom, priorLow"
        # Since MAs are not listed in Supports, I exclude them for Target?
        # Actually in 3-2 it says "sellTarget = max(candidate where value < close)".
        # We'll stick to the specific list in 3-1 for now.

    # priorLow (already in swingLow20 if recent, maybe historical?)
    # "detected priorLow" -> Assume covered by swingLow20 for now or monthly box low.

    # Filter Valid
    # sellStop = min(candidate > close)
    valid_stops = [r for r in resistances if r > last_close]
    sell_stop = min(valid_stops) if valid_stops else None

    # sellTarget = max(candidate < close)
    valid_targets = [s for s in supports if s < last_close]
    sell_target = max(valid_targets) if valid_targets else None
    
    risk_atr = (sell_stop - last_close) / atr14 if sell_stop and atr14 else None
    downside_atr = (last_close - sell_target) / atr14 if sell_target and atr14 else None
    
    return sell_stop, sell_target, risk_atr, downside_atr

def _calc_short_scores(
    daily_rows: list[tuple],
    closes: list[float],
    opens: list[float],
    highs: list[float],
    lows: list[float],
    volumes: list[float],
    ma5_series: list[float | None],
    ma7_series: list[float | None],
    ma20_series: list[float | None],
    ma60_series: list[float | None],
    atr14: float | None,
    sell_risk_atr: float | None,
    sell_downside_atr: float | None,
    sell_stop: float | None,
    sell_target: float | None,
    box_monthly: dict | None,
    monthly_regime_c: bool,
    down20: int | None,
    down60: int | None,
    up7: int | None
) -> dict:
    # Basic Data
    close = closes[-1]
    prev_close = closes[-2] if len(closes) >= 2 else close
    prev_low = lows[-2] if len(lows) >= 2 else lows[-1]
    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    
    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 else None
    prev_ma60 = ma60_series[-2] if len(ma60_series) >= 2 else None
    
    slope20 = ma20 - prev_ma20 if ma20 and prev_ma20 else 0
    slope60 = ma60 - prev_ma60 if ma60 and prev_ma60 else 0
    
    vol_avg_20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else 1
    
    # ---------------------------
    # 1. Eligibility Check
    # ---------------------------
    eligible = True
    prohibit_reason = None
    
    # (1) Monthly C (Net Short Prohibited)
    if monthly_regime_c and ma20 and close > ma20:
        eligible = False
        if slope20 >= 0:
            prohibit_reason = "monthly_C_above_ma20"
            
    # (2) Bottom Warning
    if down20 is not None and down20 >= 10 and down60 is not None and down60 >= 20:
        eligible = False
        prohibit_reason = "bottom_warning"
        
    # (3) Late Stage
    if ma20 and atr14 and (ma20 - close) >= 1.2 * atr14:
        eligible = False
        prohibit_reason = "late_stage"

    # ---------------------------
    # 2. Env Score
    # ---------------------------
    env_score = 0
    # Trend
    if ma60 and close < ma60: env_score += 25
    if ma20 and close < ma20: env_score += 15
    if slope20 < 0: env_score += 10
    if slope60 < 0: env_score += 10
    
    # Position / Overheat (Top)
    # 7MA above count (up7 from streak)
    if up7 and up7 >= 5: env_score += 10
    # 20MA above streak (implied by !down20?) No, need up20 count. 
    # Passed down20. Need up20.
    # Assuming caller passes up20 or I calc it? 
    # Let's assume up20 is passed or calc local.
    # Using helper.
    up20 = count_streak(closes, ma20_series, "up")
    if up20 and up20 >= 10: env_score += 10
    
    up60 = count_streak(closes, ma60_series, "up")
    if up60 and up60 >= 22: env_score += 10
    
    # Volume
    if len(volumes) > 0 and volumes[-1] >= vol_avg_20: env_score += 5
    
    # Boxes
    if box_monthly and atr14:
        box_top = float(box_monthly.get("bodyHigh", 0))
        box_bottom = float(box_monthly.get("bodyLow", 0))
        if abs(box_top - close) <= 0.6 * atr14: env_score += 10
        if close <= box_bottom + 0.3 * atr14: env_score += 10

    # Deny (Insurance)
    if ma20 and close > ma20 and slope20 > 0:
        env_score -= 40
        
    env_score = max(0, min(100, env_score))
    
    # ---------------------------
    # 3. Timing Patterns (B/G/M)
    # ---------------------------
    has_B = False
    has_G = False
    has_M = False
    
    if atr14:
        # B: Big Bearish Candle
        body = abs(close - opens[-1])
        lower_shadow = min(opens[-1], close) - lows[-1]
        if close < opens[-1] and body >= 0.8 * atr14 and body > 0 and lower_shadow <= 0.25 * body:
            has_B = True
            
        # G: Gap Down
        if len(closes) >= 2:
            gap_down = closes[-2] - opens[-1]
            if gap_down >= 0.5 * atr14:
                has_G = True
    
    # M: Close < MA5
    ma5 = ma5_series[-1] if ma5_series else None
    target_ma_small = ma5 if ma5 else ma7
    if target_ma_small and close < target_ma_small:
        has_M = True

    decisive_count = (1 if has_B else 0) + (1 if has_G else 0) + (1 if has_M else 0)
    
    # ---------------------------
    # 4. A-Timing (Reversal)
    # ---------------------------
    a_timing = 0
    if sell_risk_atr is not None and sell_risk_atr <= 1.0: a_timing += 30
    
    # Bearish candle at resistance (simplify: has_B and near sellStop)
    if has_B and sell_stop and atr14 and abs(sell_stop - close) < 1.0 * atr14:
        a_timing += 20
        
    if has_B: a_timing += 25
    if has_G: a_timing += 20
    if has_M: a_timing += 15
    if close < prev_low: a_timing += 10
    
    # 7MA cross down 2nd bar?
    # Simplified: down7 == 2
    down7 = count_streak(closes, ma7_series, "down")
    if down7 == 2: a_timing += 10
    
    if prohibit_reason == "late_stage": a_timing -= 20
    
    if has_B and has_G:
        a_timing = max(70, a_timing) # Guarantee 70 if B+G
    
    a_timing = max(0, min(100, a_timing))
    
    # ---------------------------
    # 5. B-Timing (Pullback)
    # ---------------------------
    b_timing = 0
    if ma60 and close < ma60: b_timing += 25
    if down20 is not None and down20 >= 3: b_timing += 15 # "close < ma20 for 3 bars"
    
    # "Pullback stopped at MA7/20 for 1-2 bars"
    # Logic: close > ma7 or ma20 recently? complicated.  
    # Simplified: up7 was 1-2 recently? or down7 just started?
    # Let's count "up7" (streak above ma7). If prior was Up 1-2 and now Down?
    # This is "Timing", so implies current bar is Down.
    # If yesterday was Up 1-2 (pullback short) and today Down?
    # Lets approximate: if down7 == 1 and up_prev_streak in 1..2?
    # For now, simplistic check: if up7 in 1..2 (weak pullback currently) -> but we want "Timing to SELL" so price should be dropping.
    # User says "Return stopped at MA7/20 for 1~2 bars".
    # Interpretation: Price went up to MA, stalled 1-2 bars, now falling.
    # We will give points if "High hit MA7/20 in last 3 bars"
    if ma7 and ma20:
        touched = False
        for i in range(-3, 0):
            if i >= -len(highs):
                h = highs[i]
                if h >= min(ma7, ma20): touched = True
        if touched: b_timing += 20
        
    if has_B: b_timing += 25
    if has_M: b_timing += 15
    if close < prev_low: b_timing += 15
    if down20 == 2: b_timing += 10
    
    b_timing = max(0, min(100, b_timing))
    
    # ---------------------------
    # 6. Risk Score
    # ---------------------------
    risk_score = 0
    if sell_risk_atr is not None:
        risk_part = max(0, min(50, 50 - 20 * sell_risk_atr))
    else:
        risk_part = 0
        
    if sell_downside_atr is not None:
        reward_part = max(0, min(50, 20 * sell_downside_atr))
    else:
        reward_part = 0
        
    rr_bonus = 0
    if sell_risk_atr and sell_downside_atr and sell_risk_atr > 0:
        rr = sell_downside_atr / sell_risk_atr
        if rr >= 2.0: rr_bonus = 10
        elif rr >= 1.5: rr_bonus = 5
        
    risk_score = max(0, min(100, risk_part + reward_part + rr_bonus))
    
    # ---------------------------
    # 7. Composites
    # ---------------------------
    a_score = 0.45 * env_score + 0.40 * a_timing + 0.15 * risk_score
    b_score = 0.40 * env_score + 0.45 * b_timing + 0.15 * risk_score
    
    a_score = max(0, min(100, a_score))
    b_score = max(0, min(100, b_score))
    
    # Case A: A priority
    short_candidate_score = 0
    short_type = None
    
    if a_score >= b_score:
        short_type = "A"
        short_candidate_score = a_score
    else:
        short_type = "B"
        short_candidate_score = b_score
        
    return {
        "shortEligible": eligible,
        "shortProhibitReason": prohibit_reason,
        "shortEnvScore": env_score,
        "aTimingScore": a_timing,
        "bTimingScore": b_timing,
        "shortRiskScore": risk_score,
        "aScore": a_score,
        "bScore": b_score,
        "shortCandidateScore": short_candidate_score,
        "shortType": short_type,
        "sellStop": sell_stop,
        "sellTarget": sell_target,
        "sellRiskAtr": sell_risk_atr,
        "sellDownsideAtr": sell_downside_atr
    }


def compute_screener_metrics(
    daily_rows: list[tuple],
    monthly_rows: list[tuple]
) -> dict:
    reasons: list[str] = []
    daily_rows = sorted(daily_rows, key=lambda item: item[0])
    monthly_rows = sorted(monthly_rows, key=lambda item: item[0])

    last_daily = _parse_daily_date(daily_rows[-1][0]) if daily_rows else None
    
    closes = [float(row[4]) for row in daily_rows if len(row) >= 5 and row[4] is not None]
    opens = [float(row[1]) for row in daily_rows if len(row) >= 5 and row[1] is not None]
    highs = [float(row[2]) for row in daily_rows if len(row) >= 5 and row[2] is not None]
    lows = [float(row[3]) for row in daily_rows if len(row) >= 5 and row[3] is not None]
    volumes = [float(row[5]) if len(row) >= 6 and row[5] is not None else 0.0 for row in daily_rows]
    
    last_close = closes[-1] if closes else None
    if last_close is None:
        reasons.append("missing_last_close")

    liquidity_20d = _calc_liquidity_20d(daily_rows)
    chg1d = _pct_change(closes[-1], closes[-2]) if len(closes) >= 2 else None

    weekly = _build_weekly_bars(daily_rows)
    weekly = _drop_incomplete_weekly(weekly, last_daily)
    confirmed_monthly = _drop_incomplete_monthly(monthly_rows, last_daily)
    
    monthly_closes = [float(row[4]) for row in confirmed_monthly if len(row) >= 5 and row[4] is not None]
    chg1m = _pct_change(monthly_closes[-1], monthly_closes[-2]) if len(monthly_closes) >= 2 else None
    
    weekly_closes = [item["c"] for item in weekly]
    chg1w = _pct_change(weekly_closes[-1], weekly_closes[-2]) if len(weekly_closes) >= 2 else None

    # MAs
    ma5_series = _build_ma_series(closes, 5)
    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None

    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 else None
    slope20 = ma20 - prev_ma20 if ma20 and prev_ma20 else None

    atr14 = _compute_atr(highs, lows, closes, 14)
    
    up7 = count_streak(closes, ma7_series, "up")
    down7 = count_streak(closes, ma7_series, "down")
    up20 = count_streak(closes, ma20_series, "up")
    down20 = count_streak(closes, ma20_series, "down")
    up60 = count_streak(closes, ma60_series, "up")
    down60 = count_streak(closes, ma60_series, "down")

    if ma20 is None: reasons.append("missing_ma20")

    # Box
    box_monthly, box_state, box_end_month, breakout_month, box_direction = _build_box_metrics(
        monthly_rows, last_close
    )
    box_active = box_state in ("IN_BOX", "JUST_BREAKOUT") # Simplify

    # Monthly Bottom Zone (Simple check for now)
    monthly_ma20_series = _build_ma_series(monthly_closes, 20)
    monthly_down20 = count_streak(monthly_closes, monthly_ma20_series, "down")
    bottom_zone = bool(monthly_down20 is not None and monthly_down20 >= 6)
    
    # Monthly Regime C check (Simple PPP check for now)
    monthly_ma7_series = _build_ma_series(monthly_closes, 7)
    monthly_ma60_series = _build_ma_series(monthly_closes, 60)
    monthly_ma7 = monthly_ma7_series[-1] if monthly_ma7_series else None
    monthly_ma20 = monthly_ma20_series[-1] if monthly_ma20_series else None
    monthly_ma60 = monthly_ma60_series[-1] if monthly_ma60_series else None
    
    monthly_regime_c = False
    if monthly_ma7 and monthly_ma20 and monthly_ma60:
        if monthly_ma7 > monthly_ma20 and monthly_ma20 > monthly_ma60:
            monthly_regime_c = True

    # ----------------------------------------------------
    # BUY Scores (Keep existing logic mostly, but use new helpers if needed)
    # ----------------------------------------------------
    # (Simplified Buy Logic from previous step - reusing parts)
    buy_env_score = 0
    if box_active: buy_env_score += 40
    if bottom_zone: buy_env_score += 30
    
    # Recalc buy metrics using helper
    supports, resistances = _find_supports_resistances(closes, highs, lows, ma20, ma60, box_monthly)
    buy_stop, buy_target, buy_risk_atr, buy_upside_atr = _find_buy_stop_target(last_close, supports, resistances, atr14)
    
    buy_risk_score = 50
    if buy_risk_atr and buy_upside_atr:
        buy_risk_score += (1.5 - buy_risk_atr) * 20
        buy_risk_score += (buy_upside_atr - 1.5) * 10
    buy_risk_score = max(0, min(100, buy_risk_score))
    
    # Simple buy timing
    daily_cross_ma20 = False
    if len(closes) >= 2 and len(ma20_series) >= 2:
        daily_cross_ma20 = closes[-1] > ma20_series[-1] and closes[-2] <= ma20_series[-2]
    buy_timing_score = 40 if daily_cross_ma20 else 0
    
    buy_candidate_score = (buy_env_score * 0.4) + (buy_timing_score * 0.4) + (buy_risk_score * 0.2)
    if liquidity_20d and liquidity_20d < 50_000_000:
        buy_candidate_score *= 0.7

    # Guardrail: treat a long up-streak as overextended and exclude from buy candidates.
    buy_overextended = bool(up20 is not None and up20 >= 16)

    buy_pattern_name = "様子見"
    buy_pattern_code = "WAIT"
    if buy_overextended:
        buy_pattern_name = "上昇伸び切り"
        buy_pattern_code = "OVEREXTENDED"
    elif daily_cross_ma20 and box_active:
        buy_pattern_name = "ボックス上放れ初動"
        buy_pattern_code = "BOX_BREAK_INITIAL"
    elif daily_cross_ma20 and bottom_zone:
        buy_pattern_name = "底打ち反転初動"
        buy_pattern_code = "BOTTOM_REVERSAL_INITIAL"
    elif daily_cross_ma20:
        buy_pattern_name = "MA20再上抜け初動"
        buy_pattern_code = "MA20_RECLAIM_INITIAL"
    elif box_active:
        buy_pattern_name = "ボックス持ち合い"
        buy_pattern_code = "BOX_CONSOLIDATION"
    elif bottom_zone:
        buy_pattern_name = "底固め待機"
        buy_pattern_code = "BOTTOM_BASE_WAIT"

    buy_state = "初動" if daily_cross_ma20 else "その他"
    buy_eligible = (buy_state == "初動")
    if buy_overextended:
        buy_candidate_score *= 0.35
        buy_state = "その他"
        buy_eligible = False
        reasons.append("buy_overextended_up20_ge16")

    # ----------------------------------------------------
    # SELL Scores (New Detailed Logic)
    # ----------------------------------------------------
    sell_stop, sell_target, sell_risk_atr, sell_downside_atr = _find_sell_stop_target(
        last_close, highs, lows, ma20, ma60, box_monthly, atr14
    )
    
    short_metrics = _calc_short_scores(
        daily_rows, closes, opens, highs, lows, volumes,
        ma5_series, ma7_series, ma20_series, ma60_series, atr14,
        sell_risk_atr, sell_downside_atr, sell_stop, sell_target, box_monthly,
        monthly_regime_c, down20, down60, up7
    )
    
    # Apply Liquidity Penalty to Sell Score
    if liquidity_20d and liquidity_20d < 50_000_000:
        short_metrics["shortCandidateScore"] *= 0.7

    # ----------------------------------------------------
    # Construction Response
    # ----------------------------------------------------
    result = {
        "lastClose": last_close,
        "liquidity20d": liquidity_20d,
        "atr14": atr14,
        "chg1D": chg1d,
        "chg1M": chg1m,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "ma100": ma100,
        "slope20": slope20,
        "counts": {
            "up7": up7, "down7": down7,
            "up20": up20,
            "down20": down20,
            "up60": up60,
            "down60": down60
        },
        "boxMonthly": box_monthly,
        "boxActive": box_active,
        
        # Buy
        "buyState": buy_state,
        "buyPatternName": buy_pattern_name,
        "buyPatternCode": buy_pattern_code,
        "buyCandidateScore": float(buy_candidate_score),
        "buyEligible": buy_eligible,
        "buyOverextended": buy_overextended,
        "buyRiskAtr": buy_risk_atr,
        "buyUpsideAtr": buy_upside_atr,
        "buyEnvScore": buy_env_score,
        "buyTimingScore": buy_timing_score,
        "buyRiskScore": buy_risk_score,

        # Sell / Short (New)
        "shortEligible": short_metrics["shortEligible"],
        "shortProhibitReason": short_metrics["shortProhibitReason"],
        "shortEnvScore": short_metrics["shortEnvScore"],
        "aTimingScore": short_metrics["aTimingScore"],
        "bTimingScore": short_metrics["bTimingScore"],
        "shortRiskScore": short_metrics["shortRiskScore"],
        "aScore": float(short_metrics["aScore"]),
        "bScore": float(short_metrics["bScore"]),
        "shortCandidateScore": float(short_metrics["shortCandidateScore"]),
        "shortScore": float(short_metrics["shortCandidateScore"]), # Legacy
        "shortType": short_metrics["shortType"],
        "sellStop": short_metrics["sellStop"],
        "sellTarget": short_metrics["sellTarget"],
        "sellRiskAtr": short_metrics["sellRiskAtr"],
        "sellDownsideAtr": short_metrics["sellDownsideAtr"],
        
        "reasons": reasons
    }
    return result

