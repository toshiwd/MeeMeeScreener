from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw

from external_analysis.image_rerank.artifacts import sha256_bytes


DEFAULT_PALETTE = {
    "background": "#ffffff",
    "grid": "#e8e8e8",
    "up": "#1f8a4c",
    "down": "#d64545",
    "wick": "#222222",
    "volume": "#8d99ae",
}


@dataclass(frozen=True)
class RendererConfig:
    renderer_version: str = "day80_ohlcv_v1"
    backend: str = "auto"
    dpi: int = 144
    canvas_size: tuple[int, int] = (224, 224)
    padding: int = 10
    linewidth: float = 1.5
    feature_lookback_days: int = 80
    palette: dict[str, str] | None = None

    def resolved_palette(self) -> dict[str, str]:
        return dict(self.palette or DEFAULT_PALETTE)


def _image_bytes(image: Image.Image) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _render_pil(*, bars: list[dict[str, Any]], path: Path, config: RendererConfig) -> dict[str, Any]:
    width, height = config.canvas_size
    palette = config.resolved_palette()
    image = Image.new("RGB", (width, height), palette["background"])
    draw = ImageDraw.Draw(image)
    left = top = int(config.padding)
    right = width - int(config.padding)
    bottom = height - int(config.padding)
    price_bottom = bottom - 42
    price_top = top
    volume_top = price_bottom + 4
    closes = [float(bar["c"]) for bar in bars]
    highs = [float(bar["h"]) for bar in bars]
    lows = [float(bar["l"]) for bar in bars]
    volumes = [max(0.0, float(bar.get("v") or 0.0)) for bar in bars]
    price_min = min(lows)
    price_max = max(highs)
    if price_max <= price_min:
        price_max = price_min + 1.0
    price_span = price_max - price_min
    volume_max = max(volumes) if volumes else 1.0
    volume_max = volume_max if volume_max > 0 else 1.0
    count = len(bars)
    step = (right - left) / float(max(1, count - 1))
    candle_width = max(1.0, step * 0.58)

    def y_price(value: float) -> float:
        return price_bottom - ((value - price_min) / price_span) * (price_bottom - price_top)

    def y_volume(value: float) -> float:
        return bottom - ((value / volume_max) * (bottom - volume_top))

    for ratio in (0.25, 0.5, 0.75):
        y = price_top + (price_bottom - price_top) * ratio
        draw.line((left, y, right, y), fill=palette["grid"], width=1)
    for idx, bar in enumerate(bars):
        x = left + (idx * step)
        open_price = float(bar["o"])
        high_price = float(bar["h"])
        low_price = float(bar["l"])
        close_price = float(bar["c"])
        volume = max(0.0, float(bar.get("v") or 0.0))
        bullish = close_price >= open_price
        color = palette["up"] if bullish else palette["down"]
        draw.line((x, y_price(high_price), x, y_price(low_price)), fill=palette["wick"], width=max(1, int(round(config.linewidth))))
        draw.rectangle(
            (x - candle_width / 2.0, y_price(max(open_price, close_price)), x + candle_width / 2.0, y_price(min(open_price, close_price))),
            fill=color,
            outline=color,
        )
        draw.rectangle(
            (x - candle_width / 2.0, y_volume(volume), x + candle_width / 2.0, bottom),
            fill=palette["volume"],
            outline=palette["volume"],
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    png_bytes = _image_bytes(image)
    path.write_bytes(png_bytes)
    return {
        "renderer_version": config.renderer_version,
        "backend": "pil",
        "dpi": int(config.dpi),
        "palette": palette,
        "canvas_size": [width, height],
        "padding": int(config.padding),
        "linewidth": float(config.linewidth),
        "image_sha256": sha256_bytes(png_bytes),
        "image_path": str(path),
    }


def _render_agg(*, bars: list[dict[str, Any]], path: Path, config: RendererConfig) -> dict[str, Any] | None:
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except Exception:
        return None

    palette = config.resolved_palette()
    width, height = config.canvas_size
    dpi = int(config.dpi)
    fig = plt.figure(figsize=(width / dpi, height / dpi), dpi=dpi)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    closes = [float(bar["c"]) for bar in bars]
    highs = [float(bar["h"]) for bar in bars]
    lows = [float(bar["l"]) for bar in bars]
    volumes = [max(0.0, float(bar.get("v") or 0.0)) for bar in bars]
    price_min = min(lows)
    price_max = max(highs)
    if price_max <= price_min:
        price_max = price_min + 1.0
    price_span = price_max - price_min
    volume_max = max(volumes) if volumes else 1.0
    volume_max = volume_max if volume_max > 0 else 1.0
    count = len(bars)
    xs = np.arange(count)
    candle_width = 0.58
    for idx, bar in enumerate(bars):
        open_price = float(bar["o"])
        high_price = float(bar["h"])
        low_price = float(bar["l"])
        close_price = float(bar["c"])
        volume = max(0.0, float(bar.get("v") or 0.0))
        bullish = close_price >= open_price
        color = palette["up"] if bullish else palette["down"]
        ax.vlines(xs[idx], low_price, high_price, colors=palette["wick"], linewidth=float(config.linewidth))
        ax.bar(xs[idx], abs(close_price - open_price), bottom=min(open_price, close_price), width=candle_width, color=color, edgecolor=color)
        ax.bar(xs[idx], (volume / volume_max) * (price_span * 0.18), bottom=price_min - (price_span * 0.22), width=candle_width, color=palette["volume"], edgecolor=palette["volume"], alpha=0.85)
    ax.set_xlim(-1, max(1, count))
    ax.set_ylim(price_min - (price_span * 0.25), price_max + (price_span * 0.1))
    fig.savefig(path, dpi=dpi, facecolor=palette["background"], edgecolor=palette["background"], pad_inches=0)
    plt.close(fig)
    png_bytes = path.read_bytes()
    return {
        "renderer_version": config.renderer_version,
        "backend": "agg",
        "dpi": dpi,
        "palette": palette,
        "canvas_size": [width, height],
        "padding": int(config.padding),
        "linewidth": float(config.linewidth),
        "image_sha256": sha256_bytes(png_bytes),
        "image_path": str(path),
    }


def render_day80_chart(*, bars: list[dict[str, Any]], path: Path, config: RendererConfig) -> dict[str, Any]:
    backend = str(config.backend or "auto").strip().lower()
    if backend == "agg":
        rendered = _render_agg(bars=bars, path=path, config=config)
        if rendered is not None:
            return rendered
    if backend == "pil":
        return _render_pil(bars=bars, path=path, config=config)
    rendered = _render_agg(bars=bars, path=path, config=config)
    if rendered is not None:
        return rendered
    return _render_pil(bars=bars, path=path, config=config)
