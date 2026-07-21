from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb
from PIL import Image, ImageStat

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "short_shape_image_quality_probe_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_image_quality_probe_v1")
DEFAULT_SCREENSHOT_DIR = Path(
    r"G:\Tradex\pure_down_current_candidate_screenshots_v1"
    r"\20260701T084227Z-meemee_detail_clean_screenshot_dataset_v1"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def _normalize_date_expr() -> str:
    return """
    CASE
        WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
        ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
    END
    """


def _load_ticker_names(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    try:
        rows = conn.execute("SELECT code, name FROM tickers").fetchall()
    except Exception:
        return {}
    return {str(code): str(name or "") for code, name in rows}


def _is_excluded_symbol(code: str, name: str) -> bool:
    upper_name = name.upper()
    if code in {"1001", "1002", "1306", "1308", "1357", "1321", "1570"}:
        return True
    return any(token in upper_name for token in ("ETF", "ETN", "REIT", "TOPIX", "日経", "インバ"))


def _load_bars(db_path: Path) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        ticker_names = _load_ticker_names(conn)
        rows = conn.execute(
            f"""
            SELECT code, {_normalize_date_expr()} AS ymd, o, h, l, c, v
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
              AND o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
            ORDER BY code, ymd
            """
        ).fetchall()
    finally:
        conn.close()
    by_code: dict[str, list[dict[str, Any]]] = {}
    for code, ymd, open_, high, low, close, volume in rows:
        by_code.setdefault(str(code), []).append(
            {
                "date": int(ymd),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": float(volume or 0),
            }
        )
    return by_code, ticker_names


def _moving_average(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = []
    running = 0.0
    for index, value in enumerate(values):
        running += value
        if index >= period:
            running -= values[index - period]
        out.append(running / period if index + 1 >= period else None)
    return out


def _sample_short_shape_candidates(
    bars_by_code: dict[str, list[dict[str, Any]]],
    ticker_names: dict[str, str],
    *,
    max_per_class: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code, bars in bars_by_code.items():
        name = ticker_names.get(code, "")
        if _is_excluded_symbol(code, name):
            continue
        if len(bars) < 260:
            continue
        closes = [row["close"] for row in bars]
        highs = [row["high"] for row in bars]
        lows = [row["low"] for row in bars]
        volumes = [row["volume"] for row in bars]
        ma20 = _moving_average(closes, 20)
        ma60 = _moving_average(closes, 60)
        vol20 = _moving_average(volumes, 20)
        last_taken_index = -999
        for index in range(80, len(bars) - 20):
            if index - last_taken_index < 20:
                continue
            if ma20[index] is None or ma60[index] is None or vol20[index - 1] is None:
                continue
            current = bars[index]
            previous_high20 = max(highs[index - 20 : index])
            candle_range = current["high"] - current["low"]
            if candle_range <= 0:
                continue
            upper_wick_ratio = (current["high"] - max(current["open"], current["close"])) / candle_range
            body_ratio = abs(current["close"] - current["open"]) / candle_range
            volume_ratio20 = current["volume"] / vol20[index - 1] if vol20[index - 1] else None
            low60 = min(lows[index - 59 : index + 1])
            high60 = max(highs[index - 59 : index + 1])
            range_pos60 = (current["close"] - low60) / (high60 - low60) if high60 > low60 else None

            high_zone_wick = (
                current["close"] > previous_high20
                and upper_wick_ratio >= 0.25
                and volume_ratio20 is not None
                and volume_ratio20 < 1.8
            )
            ma_bear_pullback20 = (
                closes[index] < ma20[index]
                and current["high"] >= ma20[index]
                and ma20[index] < ma60[index]
                and current["close"] < current["open"]
            )
            if not high_zone_wick and not ma_bear_pullback20:
                continue

            entry = current["close"]
            future = bars[index + 1 : index + 21]
            ret20 = future[-1]["close"] / entry - 1.0
            best_short_20d = min(row["low"] for row in future) / entry - 1.0
            worst_adverse_20d = max(row["high"] for row in future) / entry - 1.0
            if ret20 <= -0.08 and best_short_20d <= -0.10:
                outcome_class = "good_short_shape"
            elif ret20 >= 0.06 or worst_adverse_20d >= 0.08:
                outcome_class = "bad_short_shape"
            else:
                continue
            rows.append(
                {
                    "sample_key": f"{code}:{current['date']}",
                    "code": code,
                    "name": name,
                    "as_of": int(current["date"]),
                    "outcome_class": outcome_class,
                    "source_setup_family": "high_zone_wick" if high_zone_wick else "ma_bear_pullback20",
                    "ret20": _round(ret20),
                    "best_short_20d": _round(best_short_20d),
                    "worst_adverse_20d": _round(worst_adverse_20d),
                    "upper_wick_ratio": _round(upper_wick_ratio),
                    "body_ratio": _round(body_ratio),
                    "volume_ratio20": _round(volume_ratio20),
                    "range_pos60": _round(range_pos60),
                    "close_vs_ma20": _round(current["close"] / ma20[index] - 1.0 if ma20[index] else None),
                }
            )
            last_taken_index = index

    by_class: dict[str, list[dict[str, Any]]] = {"good_short_shape": [], "bad_short_shape": []}
    for row in rows:
        by_class[row["outcome_class"]].append(row)
    selected: list[dict[str, Any]] = []
    for outcome_class, class_rows in by_class.items():
        class_rows.sort(key=lambda row: (str(row["code"]), int(row["as_of"])))
        rows_by_code: dict[str, list[dict[str, Any]]] = {}
        for row in class_rows:
            rows_by_code.setdefault(str(row["code"]), []).append(row)
        class_selected: list[dict[str, Any]] = []
        while len(class_selected) < max_per_class:
            added = False
            for code in sorted(rows_by_code):
                code_rows = rows_by_code[code]
                if code_rows:
                    class_selected.append(code_rows.pop(0))
                    added = True
                    if len(class_selected) >= max_per_class:
                        break
            if not added:
                break
        selected.extend(class_selected)
    selected.sort(key=lambda row: (row["outcome_class"], row["code"], row["as_of"]))
    return selected


def _image_quality(path: Path) -> dict[str, Any]:
    with Image.open(path) as image:
        rgb = image.convert("RGB")
        gray = image.convert("L")
        width, height = rgb.size
        stat = ImageStat.Stat(gray)
        extrema = gray.getextrema()
        # A simple non-white ratio catches blank or overly cropped screenshots.
        pixels = list(gray.resize((max(1, width // 8), max(1, height // 8))).tobytes())
        non_white_ratio = sum(1 for value in pixels if value < 245) / len(pixels)
        return {
            "image_path": str(path),
            "width": width,
            "height": height,
            "aspect_ratio": _round(width / height),
            "mean_luma": _round(float(stat.mean[0])),
            "std_luma": _round(float(stat.stddev[0])),
            "min_luma": int(extrema[0]),
            "max_luma": int(extrema[1]),
            "non_white_ratio": _round(non_white_ratio),
            "quality_flags": {
                "display_large_enough": width >= 1200 and height >= 800,
                "training_large_enough": width >= 900 and height >= 600,
                "not_blank": non_white_ratio >= 0.03 and float(stat.stddev[0]) >= 8.0,
                "not_too_dark": float(stat.mean[0]) >= 120.0,
            },
        }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def run(
    *,
    db_path: Path,
    output_root: Path,
    screenshot_dir: Path | None,
    max_per_class: int,
) -> Path:
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)

    bars_by_code, ticker_names = _load_bars(db_path)
    samples = _sample_short_shape_candidates(bars_by_code, ticker_names, max_per_class=max_per_class)
    _write_jsonl(output_dir / "balanced_shape_outcome_sample_plan.jsonl", samples)

    quality_rows: list[dict[str, Any]] = []
    if screenshot_dir is not None:
        manifest_path = screenshot_dir / "image_manifest.jsonl"
        if manifest_path.exists():
            for row in _read_jsonl(manifest_path):
                saved_path = row.get("saved_path")
                if not saved_path:
                    continue
                path = Path(str(saved_path))
                if path.exists():
                    quality_rows.append(
                        {
                            "code": row.get("code"),
                            "as_of": row.get("as_of"),
                            "image_relpath": row.get("image_relpath"),
                            **_image_quality(path),
                        }
                    )
    _write_jsonl(output_dir / "image_quality_ledger.jsonl", quality_rows)

    counts: dict[str, int] = {}
    for row in samples:
        counts[row["outcome_class"]] = counts.get(row["outcome_class"], 0) + 1
    quality_pass = [
        row
        for row in quality_rows
        if bool(row.get("quality_flags", {}).get("training_large_enough"))
        and bool(row.get("quality_flags", {}).get("not_blank"))
        and bool(row.get("quality_flags", {}).get("not_too_dark"))
    ]
    std_values = [float(row["std_luma"]) for row in quality_rows if row.get("std_luma") is not None]
    non_white_values = [float(row["non_white_ratio"]) for row in quality_rows if row.get("non_white_ratio") is not None]
    audit = {
        "schema_version": "tradex_short_shape_image_quality_probe_v1_audit",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "runtime_status": runtime_status,
        "sample_plan_rows": len(samples),
        "sample_plan_counts": counts,
        "screenshot_dir": str(screenshot_dir) if screenshot_dir else None,
        "image_quality_rows": len(quality_rows),
        "image_quality_pass_count": len(quality_pass),
        "image_quality_summary": {
            "median_std_luma": _round(median(std_values)) if std_values else None,
            "mean_std_luma": _round(mean(std_values)) if std_values else None,
            "median_non_white_ratio": _round(median(non_white_values)) if non_white_values else None,
            "min_non_white_ratio": _round(min(non_white_values)) if non_white_values else None,
        },
        "authoritative_rollup_decision": (
            "image_quality_sufficient_build_labeled_dataset"
            if quality_rows and len(quality_pass) == len(quality_rows) and samples
            else "hold_needs_screenshots_or_quality_fix"
        ),
        "reason": (
            "existing clean screenshots pass basic nonblank/resolution/contrast gates; next blocker is labeled image dataset generation"
            if quality_rows and len(quality_pass) == len(quality_rows)
            else "quality evidence is incomplete"
        ),
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "non_scope": ["model training", "prediction adoption", "MeeMee reflection", "runtime DB mutation"],
    }
    _write_json(output_dir / "image_quality_probe_audit.json", audit)
    _write_json(output_root / "latest_image_quality_probe_audit.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--screenshot-dir", type=Path, default=DEFAULT_SCREENSHOT_DIR)
    parser.add_argument("--max-per-class", type=int, default=150)
    args = parser.parse_args()
    output_dir = run(
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        output_root=args.output_root,
        screenshot_dir=args.screenshot_dir,
        max_per_class=args.max_per_class,
    )
    print(output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
