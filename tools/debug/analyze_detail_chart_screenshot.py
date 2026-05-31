from __future__ import annotations

import json
import sys
from pathlib import Path
from statistics import mean

from PIL import Image


def _is_red(pixel: tuple[int, int, int]) -> bool:
    r, g, b = pixel[:3]
    return r >= 190 and g <= 110 and b <= 120


def _is_green(pixel: tuple[int, int, int]) -> bool:
    r, g, b = pixel[:3]
    return r <= 80 and g >= 130 and b <= 130


def _is_candle(pixel: tuple[int, int, int]) -> bool:
    return _is_red(pixel) or _is_green(pixel)


def _bucket_mean_y(points: list[tuple[int, int]], x0: int, x1: int) -> float | None:
    ys = [y for x, y in points if x0 <= x < x1]
    return mean(ys) if ys else None


def _position_from_y(y: float, height: int) -> float:
    if height <= 1:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (y / float(height - 1))))


def _classify_long(features: dict[str, object]) -> tuple[str, str, list[str]]:
    reasons: list[str] = []
    close_position = float(features.get("latest_price_position_pct") or 0.0)
    trend_slope = float(features.get("trend_slope_pct") or 0.0)
    high_rejection = bool(features.get("high_rejection_risk"))
    chase_risk = bool(features.get("chase_risk"))
    tight_high_hold = bool(features.get("tight_high_hold"))

    if trend_slope > 0.08:
        reasons.append("visual_uptrend")
    if close_position >= 0.72:
        reasons.append("visual_high_zone")
    if tight_high_hold:
        reasons.append("visual_tight_high_hold")
    if chase_risk:
        reasons.append("visual_chase_risk")
    if high_rejection:
        reasons.append("visual_high_rejection_risk")

    if high_rejection:
        return "avoid_chase", "wait_for_reclaim_after_upper_rejection", reasons
    if chase_risk and close_position >= 0.78:
        return "probe_only_or_wait", "small_probe_only_or_wait_for_7ma_pullback", reasons
    if tight_high_hold and trend_slope > 0.04:
        return "probe_candidate", "small_probe_then_add_if_high_hold_continues", reasons
    if trend_slope > 0.08 and close_position >= 0.6:
        return "watch_or_probe_small", "wait_for_intraday_hold_or_breakout_retest", reasons
    return "watch", "wait_for_clearer_visual_entry", reasons


def _classify_short(features: dict[str, object]) -> tuple[str, str, list[str]]:
    reasons: list[str] = []
    close_position = float(features.get("latest_price_position_pct") or 0.0)
    trend_slope = float(features.get("trend_slope_pct") or 0.0)
    high_rejection = bool(features.get("high_rejection_risk"))
    breakdown = bool(features.get("breakdown_risk"))

    if trend_slope < -0.06:
        reasons.append("visual_downtrend")
    if high_rejection:
        reasons.append("visual_high_rejection_risk")
    if breakdown:
        reasons.append("visual_breakdown_risk")
    if close_position <= 0.28:
        reasons.append("visual_low_zone")

    if breakdown and trend_slope < -0.04:
        return "short_probe_candidate", "small_short_probe_then_add_on_failed_retest", reasons
    if high_rejection and close_position >= 0.55:
        return "short_probe_candidate", "small_short_probe_below_rejection_high", reasons
    if trend_slope < -0.06:
        return "watch_short", "wait_for_retest_failure", reasons
    return "watch", "wait_for_clearer_visual_short_entry", reasons


def analyze(path: Path) -> dict[str, object]:
    image = Image.open(path).convert("RGB")
    width, height = image.size

    # MeeMee detail view at 1920x900: main daily chart is the upper chart.
    chart_left = int(width * 0.11)
    chart_right = int(width * 0.965)
    chart_top = int(height * 0.06)
    chart_bottom = int(height * 0.62)
    chart_width = chart_right - chart_left
    chart_height = chart_bottom - chart_top

    candle_points: list[tuple[int, int]] = []
    red_points: list[tuple[int, int]] = []
    green_points: list[tuple[int, int]] = []
    for y_abs in range(chart_top, chart_bottom, 2):
        for x_abs in range(chart_left, chart_right, 2):
            pixel = image.getpixel((x_abs, y_abs))
            if _is_candle(pixel):
                x = x_abs - chart_left
                y = y_abs - chart_top
                candle_points.append((x, y))
                if _is_red(pixel):
                    red_points.append((x, y))
                elif _is_green(pixel):
                    green_points.append((x, y))

    if not candle_points:
        return {
            "confirmed": False,
            "reason": "no_candle_pixels_detected",
            "image_size": [width, height],
        }

    left_mean = _bucket_mean_y(candle_points, 0, int(chart_width * 0.33))
    mid_mean = _bucket_mean_y(candle_points, int(chart_width * 0.33), int(chart_width * 0.66))
    right_mean = _bucket_mean_y(candle_points, int(chart_width * 0.66), chart_width)
    recent_mean = _bucket_mean_y(candle_points, int(chart_width * 0.86), chart_width)
    recent_points = [(x, y) for x, y in candle_points if x >= int(chart_width * 0.86)]
    recent_min_y = min((y for _, y in recent_points), default=None)
    recent_max_y = max((y for _, y in recent_points), default=None)

    # Detect the latest price marker on the right side. If absent, use recent candle mean.
    price_strip_left = int(width * 0.94)
    marker_ys: list[int] = []
    for y_abs in range(chart_top, chart_bottom):
        hits = 0
        for x_abs in range(price_strip_left, width - 8):
            if _is_red(image.getpixel((x_abs, y_abs))) or _is_green(image.getpixel((x_abs, y_abs))):
                hits += 1
        if hits >= 8:
            marker_ys.append(y_abs - chart_top)
    latest_y = mean(marker_ys) if marker_ys else recent_mean if recent_mean is not None else right_mean
    latest_position = _position_from_y(float(latest_y), chart_height) if latest_y is not None else None

    trend_slope = 0.0
    if left_mean is not None and right_mean is not None:
        trend_slope = (left_mean - right_mean) / float(chart_height)

    recent_span = ((recent_max_y - recent_min_y) / float(chart_height)) if recent_min_y is not None and recent_max_y is not None else None
    high_rejection = bool(
        recent_min_y is not None
        and latest_y is not None
        and ((float(latest_y) - float(recent_min_y)) / float(chart_height)) >= 0.08
    )
    chase_risk = bool(
        latest_position is not None
        and latest_position >= 0.78
        and mid_mean is not None
        and right_mean is not None
        and ((mid_mean - right_mean) / float(chart_height)) >= 0.07
    )
    tight_high_hold = bool(
        latest_position is not None
        and latest_position >= 0.66
        and recent_span is not None
        and recent_span <= 0.22
        and not high_rejection
    )
    breakdown_risk = bool(
        latest_position is not None
        and latest_position <= 0.35
        and trend_slope <= -0.04
    )

    features: dict[str, object] = {
        "confirmed": True,
        "image_size": [width, height],
        "chart_box": {
            "left": chart_left,
            "top": chart_top,
            "right": chart_right,
            "bottom": chart_bottom,
        },
        "candle_pixel_count": len(candle_points),
        "red_pixel_count": len(red_points),
        "green_pixel_count": len(green_points),
        "latest_price_position_pct": round(float(latest_position), 4) if latest_position is not None else None,
        "trend_slope_pct": round(float(trend_slope), 4),
        "recent_vertical_span_pct": round(float(recent_span), 4) if recent_span is not None else None,
        "high_rejection_risk": high_rejection,
        "chase_risk": chase_risk,
        "tight_high_hold": tight_high_hold,
        "breakdown_risk": breakdown_risk,
    }
    long_decision, long_entry, long_reasons = _classify_long(features)
    short_decision, short_entry, short_reasons = _classify_short(features)
    features["long_visual_review"] = {
        "decision": long_decision,
        "entryMethod": long_entry,
        "reasons": long_reasons,
    }
    features["short_visual_review"] = {
        "decision": short_decision,
        "entryMethod": short_entry,
        "reasons": short_reasons,
    }
    return features


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python tools/debug/analyze_detail_chart_screenshot.py <screenshot.png>", file=sys.stderr)
        return 2
    result = analyze(Path(sys.argv[1]))
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
