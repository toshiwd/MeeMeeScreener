from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from PIL import Image, ImageDraw

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "meemee_equivalent_chart_render_phase0"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_equivalent_chart_render_phase0")
DEFAULT_CODES = ("8714", "1963", "4208", "6326", "4536", "8086", "4042", "7616", "9381", "3186")
SCALES = {"micro": 30, "short": 60, "structure": 120, "macro": 240}
MA_PERIODS = (7, 20, 60, 100, 200)
WIDTH = 1280
HEIGHT = 720
PAD = 6
AXIS_RIGHT_PAD = 46
AXIS_BOTTOM_PAD = 18
VOLUME_BAND_HEIGHT = 16
COLORS = {"up": "#ef4444", "down": "#22c55e"}
MA_COLORS = {7: "#f59e0b", 20: "#3b82f6", 60: "#8b5cf6", 100: "#ec4899", 200: "#64748b"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fetch_rows(conn: duckdb.DuckDBPyConnection, code: str, as_of: int, limit: int = 260) -> list[tuple]:
    return conn.execute(
        """
        SELECT date, o, h, l, c, v
        FROM daily_bars
        WHERE code = ?
          AND date <= CASE WHEN date >= 1000000000 THEN epoch(strptime(CAST(? AS VARCHAR), '%Y%m%d')) ELSE ? END
          AND COALESCE(source, 'pan') <> 'yahoo'
        ORDER BY date DESC
        LIMIT ?
        """,
        [code, as_of, as_of, limit],
    ).fetchall()[::-1]


def _ma_values(rows: list[tuple], period: int) -> list[float | None]:
    values: list[float | None] = []
    closes: list[float] = []
    for row in rows:
        closes.append(float(row[4]))
        values.append(sum(closes[-period:]) / period if len(closes) >= period else None)
    return values


def render_png(rows: list[tuple], *, max_bars: int) -> bytes:
    display = rows[-max_bars:]
    if not display:
        raise ValueError("empty_rows")
    image = Image.new("RGB", (WIDTH, HEIGHT), "white")
    draw = ImageDraw.Draw(image)
    plot_width = WIDTH - AXIS_RIGHT_PAD
    plot_height = HEIGHT - AXIS_BOTTOM_PAD - VOLUME_BAND_HEIGHT
    ma_series = {period: _ma_values(rows, period)[-len(display):] for period in MA_PERIODS}
    prices = [float(value) for row in display for value in row[2:4]]
    prices.extend(value for series in ma_series.values() for value in series if value is not None)
    low, high = min(prices), max(prices)
    span = max(1e-6, high - low)
    step = plot_width / len(display)
    candle_width = max(1.0, min(6.0, step * 0.6))
    volume_max = max(1.0, max(float(row[5] or 0) for row in display))

    def y(price: float) -> float:
        return PAD + (plot_height - PAD * 2) * (1 - (price - low) / span)

    for ratio in (0.0, 0.25, 0.5, 0.75, 1.0):
        yy = PAD + (plot_height - PAD * 2) * ratio
        draw.line((0, yy, plot_width, yy), fill="#e5e7eb", width=1)
    for index, row in enumerate(display):
        _, open_, high_, low_, close, volume = row
        x = step * index + step / 2
        color = COLORS["up"] if close >= open_ else COLORS["down"]
        draw.line((x, y(float(high_)), x, y(float(low_))), fill=color, width=1)
        draw.rectangle((x - candle_width / 2, min(y(open_), y(close)), x + candle_width / 2, max(y(open_), y(close)) or 1), fill=color)
        volume_height = (float(volume or 0) / volume_max) * max(1, VOLUME_BAND_HEIGHT - 2)
        draw.rectangle((x - min(4, step * 0.5) / 2, HEIGHT - AXIS_BOTTOM_PAD - 2 - volume_height, x + min(4, step * 0.5) / 2, HEIGHT - AXIS_BOTTOM_PAD - 2), fill=color)
    for period, values in ma_series.items():
        points = [(step * index + step / 2, y(value)) for index, value in enumerate(values) if value is not None]
        if len(points) > 1:
            draw.line(points, fill=MA_COLORS[period], width=2)
    out = io.BytesIO()
    image.save(out, format="PNG", optimize=False, compress_level=9)
    return out.getvalue()


def run(*, output_root: Path, db_path: Path, as_of: int, codes: tuple[str, ...]) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    image_dir = output_dir / "images"
    image_dir.mkdir()
    contract = {
        "schema_version": "meemee_equivalent_chart_render_contract_v1",
        "boundary_owner": "TRADEX",
        "reference_implementation": "app/frontend/src/utils/chartScreenshot.ts -> ThumbnailCanvas.drawChart",
        "canonical_training_renderer": "browser_reference_export_from_ThumbnailCanvas.drawChart",
        "python_renderer_role": "deterministic_proxy_for_comparison_audit_only",
        "comparison_status": "research_renderer_contract_ready_playwright_pixel_comparison_required",
        "dimensions": {"width": WIDTH, "height": HEIGHT},
        "theme": "light",
        "show_axes": True,
        "show_boxes": False,
        "scales": SCALES,
        "ma_periods": MA_PERIODS,
        "source_policy": "confirmed_non_yahoo_daily_bars_only",
        "non_scope": ["MeeMee UI mutation", "production ranking mutation", "runtime DB write", "provisional Yahoo training rows"],
    }
    manifest: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for code in codes:
            rows = _fetch_rows(conn, code, as_of)
            if not rows:
                manifest.append({"code": code, "as_of": as_of, "render_status": "missing_confirmed_rows"})
                continue
            for scale, bars in SCALES.items():
                png = render_png(rows, max_bars=bars)
                repeat = render_png(rows, max_bars=bars)
                path = image_dir / f"{code}_{as_of}_{scale}_{bars}.png"
                path.write_bytes(png)
                manifest.append({
                    "code": code,
                    "as_of": as_of,
                    "scale": scale,
                    "bars": bars,
                    "available_rows": len(rows),
                    "path": str(path),
                    "sha256": _sha256(png),
                    "deterministic": png == repeat,
                    "bars_payload": [list(row) for row in rows],
                })
    finally:
        conn.close()
    _write_json(output_dir / "meemee_equivalent_chart_render_contract.json", contract)
    (output_dir / "image_manifest.jsonl").write_text("".join(json.dumps(row) + "\n" for row in manifest), encoding="utf-8")
    _write_json(output_dir / "renderer_determinism_report.json", {
        "checked_image_count": sum(1 for row in manifest if row.get("deterministic") is not None),
        "deterministic_image_count": sum(1 for row in manifest if row.get("deterministic") is True),
        "renderer_deterministic": all(row.get("deterministic", True) for row in manifest),
    })
    _write_json(output_dir / "phase0_audit.json", {
        "schema_version": "meemee_equivalent_chart_render_phase0_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "as_of": as_of,
        "representative_codes": list(codes),
        "generated_image_count": len(manifest),
        "rendered_image_count": sum(1 for row in manifest if row.get("deterministic") is not None),
        "expected_image_count": len(codes) * len(SCALES),
        "missing_code_count": sum(1 for row in manifest if row.get("render_status") == "missing_confirmed_rows"),
        "renderer_deterministic": all(row.get("deterministic", True) for row in manifest),
        "judgment": "hold_for_playwright_pixel_comparison",
        "remaining_gate": "MeeMee browser screenshot versus research renderer representative-sample comparison",
    })
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--as-of", type=int, default=20260529)
    parser.add_argument("--codes", nargs="*", default=list(DEFAULT_CODES))
    args = parser.parse_args()
    output = run(
        output_root=args.output_root,
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        as_of=args.as_of,
        codes=tuple(args.codes),
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
