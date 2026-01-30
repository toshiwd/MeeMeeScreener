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
    _count_streak,
    _format_daily_label,
    _format_month_label
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
    boxes = detect_boxes(monthly_rows)
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
    start_label = _format_month_label(active_box["startTime"])
    end_label = _format_month_label(active_box["endTime"])

    box_state = "NONE"
    if end_index == latest_index:
        box_state = "IN_BOX"
    elif end_index == latest_index - 1:
        box_state = "JUST_BREAKOUT"

    breakout_month = None
    if box_state == "JUST_BREAKOUT" and latest_index >= 0:
        breakout_month = _format_month_label(bars[latest_index]["month"])

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


def compute_screener_metrics(
    daily_rows: list[tuple],
    monthly_rows: list[tuple]
) -> dict:
    reasons: list[str] = []
    # Ensure sorted by date
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

    chg1d = _pct_change(closes[-1], closes[-2]) if len(closes) >= 2 else None

    weekly = _build_weekly_bars(daily_rows)
    weekly = _drop_incomplete_weekly(weekly, last_daily)
    weekly_closes = [item["c"] for item in weekly]
    chg1w = _pct_change(weekly_closes[-1], weekly_closes[-2]) if len(weekly_closes) >= 2 else None
    prev_week_chg = _pct_change(weekly_closes[-2], weekly_closes[-3]) if len(weekly_closes) >= 3 else None

    confirmed_monthly = _drop_incomplete_monthly(monthly_rows, last_daily)
    monthly_closes = [float(row[4]) for row in confirmed_monthly if len(row) >= 5 and row[4] is not None]
    chg1m = _pct_change(monthly_closes[-1], monthly_closes[-2]) if len(monthly_closes) >= 2 else None
    prev_month_chg = _pct_change(monthly_closes[-2], monthly_closes[-3]) if len(monthly_closes) >= 3 else None

    quarterly = _build_quarterly_bars(confirmed_monthly)
    quarterly_closes = [item["c"] for item in quarterly]
    chg1q = _pct_change(quarterly_closes[-1], quarterly_closes[-2]) if len(quarterly_closes) >= 2 else None
    prev_quarter_chg = _pct_change(quarterly_closes[-2], quarterly_closes[-3]) if len(quarterly_closes) >= 3 else None

    yearly = _build_yearly_bars(confirmed_monthly)
    yearly_closes = [item["c"] for item in yearly]
    chg1y = _pct_change(yearly_closes[-1], yearly_closes[-2]) if len(yearly_closes) >= 2 else None
    prev_year_chg = _pct_change(yearly_closes[-2], yearly_closes[-3]) if len(yearly_closes) >= 3 else None

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
    slope20 = ma20 - prev_ma20 if ma20 is not None and prev_ma20 is not None else None

    slope20_reg = calc_regression_slope(ma20_series, 5)
    slope60_reg = calc_regression_slope(ma60_series, 5)

    atr14 = _compute_atr(highs, lows, closes, 14)
    volume_avg_20 = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else None

    up7 = _count_streak(closes, ma7_series, "up")
    down7 = _count_streak(closes, ma7_series, "down")
    up20 = _count_streak(closes, ma20_series, "up")
    down20 = _count_streak(closes, ma20_series, "down")
    up60 = _count_streak(closes, ma60_series, "up")
    down60 = _count_streak(closes, ma60_series, "down")
    up100 = _count_streak(closes, ma100_series, "up")
    down100 = _count_streak(closes, ma100_series, "down")

    if ma20 is None:
        reasons.append("missing_ma20")
    if ma60 is None:
        reasons.append("missing_ma60")
    if ma100 is None:
        reasons.append("missing_ma100")
    if chg1m is None:
        reasons.append("missing_chg1m")
    if chg1q is None:
        reasons.append("missing_chg1q")
    if chg1y is None:
        reasons.append("missing_chg1y")

    box_monthly, box_state, box_end_month, breakout_month, box_direction = _build_box_metrics(
        monthly_rows, last_close
    )

    latest_month_label = _format_month_label(confirmed_monthly[-1][0]) if confirmed_monthly else None
    prev_month_label = _format_month_label(confirmed_monthly[-2][0]) if len(confirmed_monthly) >= 2 else None
    latest_month_value = _month_label_to_int(latest_month_label)
    prev_month_value = _month_label_to_int(prev_month_label)
    box_active = False
    if box_monthly:
        box_start_value = _month_label_to_int(box_monthly.get("startDate"))
        box_end_value = _month_label_to_int(box_monthly.get("endDate"))
        if box_start_value is not None and box_end_value is not None:
            if latest_month_value is not None and box_start_value <= latest_month_value <= box_end_value:
                box_active = True
            elif prev_month_value is not None and box_start_value <= prev_month_value <= box_end_value:
                box_active = True

    monthly_ma20_series = _build_ma_series(monthly_closes, 20)
    monthly_down20 = _count_streak(monthly_closes, monthly_ma20_series, "down")
    bottom_zone = bool(monthly_down20 is not None and monthly_down20 >= 6)

    weekly_closes = [item["c"] for item in weekly]
    weekly_highs = [item["h"] for item in weekly]
    weekly_lows = [item["l"] for item in weekly]
    weekly_ma7_series = _build_ma_series(weekly_closes, 7)
    weekly_ma20_series = _build_ma_series(weekly_closes, 20)
    weekly_ma7 = weekly_ma7_series[-1] if weekly_ma7_series else None
    weekly_ma20 = weekly_ma20_series[-1] if weekly_ma20_series else None
    weekly_above_ma7 = (
        weekly_closes[-1] > weekly_ma7 if weekly_ma7 is not None and weekly_closes else False
    )
    weekly_above_ma20 = (
        weekly_closes[-1] > weekly_ma20 if weekly_ma20 is not None and weekly_closes else False
    )

    weekly_low_stop = False
    if len(weekly_lows) >= 6:
        recent_lows = weekly_lows[-6:]
        previous_lows = weekly_lows[:-6]
        if previous_lows:
            weekly_low_stop = min(recent_lows) >= min(previous_lows)

    weekly_range_contraction = False
    if len(weekly_highs) >= 12:
        recent_range = max(weekly_highs[-6:]) - min(weekly_lows[-6:])
        prev_range = max(weekly_highs[-12:-6]) - min(weekly_lows[-12:-6])
        if prev_range > 0 and recent_range <= prev_range * 0.8:
            weekly_range_contraction = True

    daily_cross_ma7 = False
    daily_cross_ma20 = False
    if len(closes) >= 2 and len(ma7_series) >= 2:
        daily_cross_ma7 = closes[-1] > ma7_series[-1] and closes[-2] <= ma7_series[-2]
    if len(closes) >= 2 and len(ma20_series) >= 2:
        daily_cross_ma20 = closes[-1] > ma20_series[-1] and closes[-2] <= ma20_series[-2]

    daily_pre_signal = False
    if daily_rows:
        last_row = daily_rows[-1]
        if len(last_row) >= 5:
            open_ = float(last_row[1]) if last_row[1] is not None else None
            high = float(last_row[2]) if last_row[2] is not None else None
            low = float(last_row[3]) if last_row[3] is not None else None
            close = float(last_row[4]) if last_row[4] is not None else None
            if open_ is not None and high is not None and low is not None and close is not None:
                rng = max(high - low, 1e-9)
                body = abs(close - open_)
                lower_shadow = min(open_, close) - low
                if body / rng <= 0.35 or lower_shadow / rng >= 0.45:
                    daily_pre_signal = True

    daily_low_break = False
    if len(daily_rows) >= 11:
        lows_window = [
            float(row[3])
            for row in daily_rows[-11:-1]
            if len(row) >= 4 and row[3] is not None
        ]
        if lows_window and daily_rows[-1][3] is not None:
            daily_low_break = float(daily_rows[-1][3]) < min(lows_window)

    weekly_low_break = False
    if len(weekly_lows) >= 7:
        weekly_low_break = weekly_lows[-1] < min(weekly_lows[-7:-1])

    falling_knife = daily_low_break or weekly_low_break
    monthly_ok = box_active or bottom_zone

    score_monthly = 0
    if box_active:
        score_monthly += 18
    if bottom_zone:
        score_monthly += 12

    score_weekly = 0
    if weekly_low_stop:
        score_weekly += 15
    if weekly_range_contraction:
        score_weekly += 10
    if weekly_above_ma7:
        score_weekly += 7
    if weekly_above_ma20:
        score_weekly += 8

    score_daily = 0
    if daily_cross_ma7:
        score_daily += 10
    if daily_cross_ma20:
        score_daily += 12
    if daily_pre_signal:
        score_daily += 8

    daily_ma20_down = False
    if len(ma20_series) >= 2:
        daily_ma20_down = ma20_series[-1] < ma20_series[-2]

    buy_state = "その他"
    buy_state_rank = 0
    buy_state_score = 0
    buy_state_reason_parts: list[str] = []

    if monthly_ok and weekly_low_stop and not falling_knife:
        if daily_cross_ma7 or daily_cross_ma20 or daily_pre_signal:
            buy_state = "初動"
            buy_state_rank = 2
            buy_state_score = score_monthly + score_weekly + score_daily
            if daily_ma20_down and ma20 is not None and last_close is not None and last_close < ma20:
                buy_state_score -= 15
        elif weekly_range_contraction:
            buy_state = "底がため"
            buy_state_rank = 1
            buy_state_score = score_monthly + score_weekly + min(score_daily, 10)

    if buy_state_score < 0:
        buy_state_score = 0
    if buy_state == "初動":
        buy_state_score = min(100, buy_state_score)
    elif buy_state == "底がため":
        buy_state_score = min(80, buy_state_score)

    if monthly_ok:
        month_parts = []
        if box_active:
            month_parts.append("箱有")
        if bottom_zone:
            month_parts.append("大底警戒")
        buy_state_reason_parts.append(f"月:{'/'.join(month_parts)}")
    if weekly_low_stop or weekly_range_contraction:
        week_parts = []
        if weekly_low_stop:
            week_parts.append("安値更新停止")
        if weekly_range_contraction:
            week_parts.append("収縮")
        if weekly_above_ma7:
            week_parts.append("7MA上")
        if weekly_above_ma20:
            week_parts.append("20MA上")
        buy_state_reason_parts.append(f"週:{'/'.join(week_parts)}")
    if daily_cross_ma7 or daily_cross_ma20 or daily_pre_signal:
        day_parts = []
        if daily_cross_ma7:
            day_parts.append("7MA上抜け")
        if daily_cross_ma20:
            day_parts.append("20MA上抜け")
        if daily_pre_signal:
            day_parts.append("事前決定打")
        buy_state_reason_parts.append(f"日:{'/'.join(day_parts)}")
    if falling_knife:
        buy_state_reason_parts.append("落ちるナイフ")

    buy_state_reason = " / ".join(buy_state_reason_parts) if buy_state_reason_parts else "N/A"

    buy_risk_distance = None
    if last_close is not None and box_monthly and box_monthly.get("bodyLow") is not None:
        body_low = float(box_monthly["bodyLow"])
        if last_close > 0:
            buy_risk_distance = max(0.0, (last_close - body_low) / last_close * 100)

    status_label = "UNKNOWN"
    essential_missing = last_close is None or ma20 is None or ma60 is None
    if not essential_missing:
        if last_close > ma20 and ma20 > ma60:
            status_label = "UP"
        elif last_close < ma20 and ma20 < ma60:
            status_label = "DOWN"
        else:
            status_label = "RANGE"

    up_score = None
    down_score = None
    overheat_up = None
    overheat_down = None

    if status_label != "UNKNOWN" and last_close is not None and ma20 is not None and ma60 is not None:
        up_score = 0
        down_score = 0

        if last_close > ma20:
            up_score += 10
        if ma20 > ma60:
            up_score += 10
        if slope20 is not None and slope20 > 0:
            up_score += 10

        if up7 is not None:
            if up7 >= 14:
                up_score += 20
            elif up7 >= 7:
                up_score += 10

        if box_state != "NONE":
            if box_direction == "BREAKOUT_UP":
                up_score += 30
            elif box_state == "IN_BOX" and box_monthly and box_monthly.get("months", 0) >= 3:
                up_score += 10

        if chg1m is not None and chg1m > 0:
            up_score += 10
        if chg1q is not None and chg1q > 0:
            up_score += 10

        if last_close < ma20:
            down_score += 10
        if ma20 < ma60:
            down_score += 10
        if slope20 is not None and slope20 < 0:
            down_score += 10

        if down7 is not None:
            if down7 >= 14:
                down_score += 20
            elif down7 >= 7:
                down_score += 10

        if box_state != "NONE" and box_direction == "BREAKOUT_DOWN":
            down_score += 30

        if chg1m is not None and chg1m < 0:
            down_score += 10
        if chg1q is not None and chg1q < 0:
            down_score += 10

        up_score = min(100, max(0, up_score))
        down_score = min(100, max(0, down_score))

        if up20 is not None:
            overheat_up = min(1.0, max(0.0, (up20 - 16) / 4))
        if down20 is not None:
            overheat_down = min(1.0, max(0.0, (down20 - 16) / 4))

    # Short-selling score calculation
    short_score = None
    a_score = None
    b_score = None
    short_type = None
    short_badges: list[str] = []
    short_reasons: list[str] = []
    short_prohibition = None

    if last_close is not None and ma20 is not None and ma60 is not None:
        range_high_60, range_low_60, range_mid_60 = calc_range_bounds_with_mid(highs, lows, 60)
        short_prohibition, zone_penalty = check_short_prohibition_zones(
            last_close, ma20, ma60, slope20_reg, slope60_reg, atr14,
            range_mid_60, range_high_60, range_low_60
        )

        a_score_raw, a_reasons, a_badges = calc_short_a_score(
            closes, opens, lows, ma5_series, ma20_series, atr14,
            volumes, volume_avg_20, down7, highs
        )

        b_score_raw, b_reasons, b_badges = calc_short_b_score(
            closes, opens, lows, ma5_series, ma20_series, ma60_series,
            slope20_reg, slope60_reg, atr14, volumes, volume_avg_20, down20, ma7_series
        )

        if short_prohibition == "Z3":
            a_score_raw = max(0, a_score_raw + zone_penalty)
            b_score_raw = max(0, b_score_raw + zone_penalty)

        if short_prohibition in ("Z1", "Z2"):
            short_score = 0
            a_score = 0
            b_score = 0
            short_type = None
            short_badges = []
            short_reasons = [f"禁止ゾーン: {short_prohibition}"]
        else:
            a_score = a_score_raw
            b_score = b_score_raw
            short_score = max(a_score, b_score)

            if a_score >= b_score and a_score > 0:
                short_type = "A"
                short_badges = a_badges
                short_reasons = a_reasons
            elif b_score > 0:
                short_type = "B"
                short_badges = b_badges
                short_reasons = b_reasons
            else:
                short_type = None
                short_badges = []
                short_reasons = []

    return {
        "lastClose": last_close,
        "chg1D": chg1d,
        "chg1W": chg1w,
        "chg1M": chg1m,
        "chg1Q": chg1q,
        "chg1Y": chg1y,
        "prevWeekChg": prev_week_chg,
        "prevMonthChg": prev_month_chg,
        "prevQuarterChg": prev_quarter_chg,
        "prevYearChg": prev_year_chg,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "ma100": ma100,
        "slope20": slope20,
        "counts": {
            "up7": up7,
            "down7": down7,
            "up20": up20,
            "down20": down20,
            "up60": up60,
            "down60": down60,
            "up100": up100,
            "down100": down100
        },
        "boxMonthly": box_monthly,
        "boxState": box_state,
        "boxEndMonth": box_end_month,
        "breakoutMonth": breakout_month,
        "boxActive": box_active,
        "hasBox": box_active,
        "box_state": box_state,
        "box_end_month": box_end_month,
        "breakout_month": breakout_month,
        "box_active": box_active,
        "buyState": buy_state,
        "buyStateRank": buy_state_rank,
        "buyStateScore": buy_state_score,
        "buyStateReason": buy_state_reason,
        "buyRiskDistance": buy_risk_distance,
        "buy_state": buy_state,
        "buy_state_rank": buy_state_rank,
        "buy_state_score": buy_state_score,
        "buy_state_reason": buy_state_reason,
        "buy_risk_distance": buy_risk_distance,
        "buyStateDetails": {
            "monthly": score_monthly,
            "weekly": score_weekly,
            "daily": score_daily
        },
        "scores": {
            "upScore": up_score,
            "downScore": down_score,
            "overheatUp": overheat_up,
            "overheatDown": overheat_down
        },
        "statusLabel": status_label,
        "reasons": reasons,
        "shortScore": short_score,
        "aScore": a_score,
        "bScore": b_score,
        "shortType": short_type,
        "shortBadges": short_badges,
        "shortReasons": short_reasons,
        "shortProhibition": short_prohibition
    }
