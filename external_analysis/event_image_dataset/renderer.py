from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw

from external_analysis.image_rerank.artifacts import sha256_bytes
from external_analysis.image_rerank.renderer import DEFAULT_PALETTE


CONTROL_EVALUATION_BUNDLE_ID = "monthly_event_day120_pil_12lr_v1"
CONTROL_RENDERER_SPEC_ID = "monthly_event_day120_ohlcv_ma20_60_120_v1"
CONTROL_FEATUREIZER_SPEC_ID = "rgb_flatten_12_lr_v1"

FIDELITY_EVALUATION_BUNDLE_ID = "monthly_event_tripane_agg_48lr_v1"
FIDELITY_RENDERER_SPEC_ID = "monthly_event_tripane_d60_w52_m36_512_v1"
FIDELITY_FEATUREIZER_SPEC_ID = "rgb_flatten_48_lr_v1"

CONTROL_LOOKBACK_DAYS = 120
FIDELITY_DAILY_BARS = 60
FIDELITY_WEEKLY_BARS = 52
FIDELITY_MONTHLY_BARS = 36
FIDELITY_IMAGE_SIZE = (512, 512)
CONTROL_IMAGE_SIZE = (224, 224)

FIDELITY_PANE_LAYOUT = {"daily_ratio": 0.62, "weekly_ratio": 0.20, "monthly_ratio": 0.18}
FIDELITY_DAILY_PRICE_VOLUME_SPLIT = {"price_ratio": 0.84, "volume_ratio": 0.16}
FIDELITY_WARMUP_CONTRACT = {
    "indicator_warmup_days": 200,
    "indicator_warmup_weeks": 60,
    "indicator_warmup_months": 24,
}


def strict_agg_available() -> bool:
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as _  # noqa: F401
    except Exception:
        return False
    return True


def _image_bytes(image: Image.Image) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _render_control_pil(
    *,
    bars: list[dict[str, Any]],
    path: Path,
    image_size: tuple[int, int],
    palette: dict[str, str],
) -> dict[str, Any]:
    width, height = image_size
    image = Image.new("RGB", (width, height), palette["background"])
    draw = ImageDraw.Draw(image)
    left = 8
    right = width - 8
    top = 6
    bottom = height - 6
    price_bottom = int(height * 0.82)
    volume_top = price_bottom + 4

    highs = [float(bar["h"]) for bar in bars]
    lows = [float(bar["l"]) for bar in bars]
    volumes = [max(0.0, float(bar.get("v") or 0.0)) for bar in bars]
    price_min = min(lows)
    price_max = max(highs)
    if price_max <= price_min:
        price_max = price_min + 1.0
    volume_max = max(volumes) if volumes else 1.0
    if volume_max <= 0.0:
        volume_max = 1.0
    count = len(bars)
    step = (right - left) / float(max(1, count - 1))
    candle_width = max(1.0, step * 0.58)

    def y_price(value: float) -> float:
        return price_bottom - ((value - price_min) / (price_max - price_min)) * (price_bottom - top)

    def y_volume(value: float) -> float:
        return bottom - ((value / volume_max) * (bottom - volume_top))

    def _draw_ma_line(values: list[float | None], color: str) -> None:
        points: list[tuple[float, float]] = []
        for idx, value in enumerate(values):
            if value is None:
                if len(points) >= 2:
                    draw.line(points, fill=color, width=1)
                points = []
                continue
            points.append((left + (idx * step), y_price(float(value))))
        if len(points) >= 2:
            draw.line(points, fill=color, width=1)

    for idx, bar in enumerate(bars):
        x = left + (idx * step)
        open_price = float(bar["o"])
        high_price = float(bar["h"])
        low_price = float(bar["l"])
        close_price = float(bar["c"])
        volume = max(0.0, float(bar.get("v") or 0.0))
        bullish = close_price >= open_price
        color = palette["up"] if bullish else palette["down"]
        draw.line((x, y_price(high_price), x, y_price(low_price)), fill=palette["wick"], width=1)
        draw.rectangle(
            (
                x - candle_width / 2.0,
                y_price(max(open_price, close_price)),
                x + candle_width / 2.0,
                y_price(min(open_price, close_price)),
            ),
            fill=color,
            outline=color,
        )
        draw.rectangle(
            (
                x - candle_width / 2.0,
                y_volume(volume),
                x + candle_width / 2.0,
                bottom,
            ),
            fill=palette["volume"],
            outline=palette["volume"],
        )

    _draw_ma_line([float(bar["ma20"]) if bar.get("ma20") is not None else None for bar in bars], "#f4a261")
    _draw_ma_line([float(bar["ma60"]) if bar.get("ma60") is not None else None for bar in bars], "#457b9d")
    _draw_ma_line([float(bar["ma120"]) if bar.get("ma120") is not None else None for bar in bars], "#6d597a")

    path.parent.mkdir(parents=True, exist_ok=True)
    png_bytes = _image_bytes(image)
    path.write_bytes(png_bytes)
    return {
        "evaluation_bundle_id": CONTROL_EVALUATION_BUNDLE_ID,
        "renderer_spec_id": CONTROL_RENDERER_SPEC_ID,
        "featureizer_spec_id": CONTROL_FEATUREIZER_SPEC_ID,
        "actual_backend": "pil",
        "requested_backend": "pil",
        "strict_backend": False,
        "backend_fallback_reason": None,
        "image_path": str(path),
        "image_sha256": sha256_bytes(png_bytes),
        "image_size": [int(width), int(height)],
        "normalization_rule": "lookback_high_low_minmax_with_fixed_volume_panel",
        "palette": palette,
    }


def _draw_agg_candles(
    axis: Any,
    *,
    bars: list[dict[str, Any]],
    ma_columns: list[tuple[str, str]],
    palette: dict[str, str],
    show_volume: bool,
) -> None:
    if not bars:
        return
    price_axis = axis
    volume_axis = None
    if show_volume:
        volume_ratio = float(FIDELITY_DAILY_PRICE_VOLUME_SPLIT["volume_ratio"])
        price_ratio = float(FIDELITY_DAILY_PRICE_VOLUME_SPLIT["price_ratio"])
        gutter = 0.01
        volume_height = max(0.05, volume_ratio - gutter)
        price_axis = axis.inset_axes([0.0, volume_ratio, 1.0, price_ratio])
        volume_axis = axis.inset_axes([0.0, 0.0, 1.0, volume_height], sharex=price_axis)
        price_axis.set_facecolor("none")
        volume_axis.set_facecolor("none")
        axis.set_axis_off()
    xs = np.arange(len(bars), dtype=np.float64)
    highs = np.asarray([float(bar["h"]) for bar in bars], dtype=np.float64)
    lows = np.asarray([float(bar["l"]) for bar in bars], dtype=np.float64)
    price_min = float(np.nanmin(lows))
    price_max = float(np.nanmax(highs))
    if price_max <= price_min:
        price_max = price_min + 1.0
    span = price_max - price_min
    candle_width = 0.58
    for idx, bar in enumerate(bars):
        open_price = float(bar["o"])
        high_price = float(bar["h"])
        low_price = float(bar["l"])
        close_price = float(bar["c"])
        bullish = close_price >= open_price
        color = palette["up"] if bullish else palette["down"]
        price_axis.vlines(xs[idx], low_price, high_price, colors=palette["wick"], linewidth=1.0)
        price_axis.bar(
            xs[idx],
            abs(close_price - open_price),
            bottom=min(open_price, close_price),
            width=candle_width,
            color=color,
            edgecolor=color,
            linewidth=0.8,
        )
    for column, color in ma_columns:
        values = np.asarray(
            [float(bar[column]) if bar.get(column) is not None else np.nan for bar in bars],
            dtype=np.float64,
        )
        price_axis.plot(xs, values, color=color, linewidth=1.0)
    price_axis.set_xlim(-1, max(1, len(bars)))
    price_axis.set_ylim(price_min - (span * 0.08), price_max + (span * 0.08))
    price_axis.set_axis_off()
    if show_volume and volume_axis is not None:
        volumes = np.asarray([max(0.0, float(bar.get("v") or 0.0)) for bar in bars], dtype=np.float64)
        volume_max = float(np.nanmax(volumes)) if len(volumes) else 1.0
        if volume_max <= 0.0:
            volume_max = 1.0
        volume_axis.bar(xs, volumes, width=candle_width, color=palette["volume"], edgecolor=palette["volume"], alpha=0.85)
        volume_axis.set_xlim(-1, max(1, len(bars)))
        volume_axis.set_ylim(0.0, volume_max * 1.15)
        volume_axis.set_axis_off()


def _render_fidelity_agg(
    *,
    daily_bars: list[dict[str, Any]],
    weekly_bars: list[dict[str, Any]],
    monthly_bars: list[dict[str, Any]],
    path: Path,
    image_size: tuple[int, int],
    palette: dict[str, str],
) -> dict[str, Any]:
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover - environment-dependent
        raise RuntimeError("strict agg backend is required for fidelity bundle rendering") from exc

    width, height = image_size
    dpi = 144
    figure = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi)
    figure.patch.set_facecolor(palette["background"])
    daily_ratio = float(FIDELITY_PANE_LAYOUT["daily_ratio"])
    weekly_ratio = float(FIDELITY_PANE_LAYOUT["weekly_ratio"])
    monthly_ratio = float(FIDELITY_PANE_LAYOUT["monthly_ratio"])
    gutter = 0.01
    monthly_height = monthly_ratio - gutter
    weekly_height = weekly_ratio - gutter
    daily_height = daily_ratio - gutter
    monthly_ax = figure.add_axes([0.0, 0.0, 1.0, monthly_height])
    weekly_ax = figure.add_axes([0.0, monthly_ratio, 1.0, weekly_height])
    daily_ax = figure.add_axes([0.0, monthly_ratio + weekly_ratio, 1.0, daily_height])
    for axis in (daily_ax, weekly_ax, monthly_ax):
        axis.set_facecolor(palette["background"])

    _draw_agg_candles(
        daily_ax,
        bars=daily_bars,
        ma_columns=[("ma7", "#ff4d4f"), ("ma20", "#2ecc71"), ("ma60", "#2f6bff"), ("ma100", "#a64dff"), ("ma200", "#f59e0b")],
        palette=palette,
        show_volume=True,
    )
    _draw_agg_candles(
        weekly_ax,
        bars=weekly_bars,
        ma_columns=[("ma10", "#2ecc71"), ("ma30", "#2f6bff"), ("ma60", "#f59e0b")],
        palette=palette,
        show_volume=False,
    )
    _draw_agg_candles(
        monthly_ax,
        bars=monthly_bars,
        ma_columns=[("ma6", "#ff4d4f"), ("ma12", "#2ecc71"), ("ma24", "#2f6bff")],
        palette=palette,
        show_volume=False,
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=dpi, facecolor=palette["background"], edgecolor=palette["background"], pad_inches=0)
    plt.close(figure)
    png_bytes = path.read_bytes()
    return {
        "evaluation_bundle_id": FIDELITY_EVALUATION_BUNDLE_ID,
        "renderer_spec_id": FIDELITY_RENDERER_SPEC_ID,
        "featureizer_spec_id": FIDELITY_FEATUREIZER_SPEC_ID,
        "actual_backend": "agg",
        "requested_backend": "agg",
        "strict_backend": True,
        "backend_fallback_reason": None,
        "image_path": str(path),
        "image_sha256": sha256_bytes(png_bytes),
        "image_size": [int(width), int(height)],
        "normalization_rule": "pane_specific_ohlcv_with_fixed_layout",
        "palette": palette,
    }


def render_event_chart(
    *,
    evaluation_bundle_id: str,
    path: Path,
    daily_bars: list[dict[str, Any]],
    weekly_bars: list[dict[str, Any]] | None = None,
    monthly_bars: list[dict[str, Any]] | None = None,
    palette: dict[str, str] | None = None,
) -> dict[str, Any]:
    resolved_palette = dict(palette or DEFAULT_PALETTE)
    if str(evaluation_bundle_id) == CONTROL_EVALUATION_BUNDLE_ID:
        return _render_control_pil(
            bars=daily_bars,
            path=path,
            image_size=CONTROL_IMAGE_SIZE,
            palette=resolved_palette,
        )
    if str(evaluation_bundle_id) == FIDELITY_EVALUATION_BUNDLE_ID:
        if weekly_bars is None or monthly_bars is None:
            raise RuntimeError("weekly/monthly bars are required for fidelity bundle rendering")
        return _render_fidelity_agg(
            daily_bars=daily_bars,
            weekly_bars=weekly_bars,
            monthly_bars=monthly_bars,
            path=path,
            image_size=FIDELITY_IMAGE_SIZE,
            palette=resolved_palette,
        )
    raise RuntimeError(f"unsupported evaluation bundle: {evaluation_bundle_id}")
