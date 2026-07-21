from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUT_ROOT = Path(r"G:\Tradex\shape_first_box_prebreak_scan_v1")
DEFAULT_SCREENSHOT_ROOT = Path(r"G:\Tradex\shape_first_box_prebreak_screenshots_v1")
REPO_ROOT = Path(__file__).resolve().parents[1]


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _date_expr(column: str = "b.date") -> str:
    return f"""
        CASE
            WHEN {column} BETWEEN 19000101 AND 20991231 THEN CAST({column} AS INTEGER)
            WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000), '%Y%m%d') AS INTEGER)
            WHEN {column} >= 1000000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """


def _load_daily(db_path: Path, *, start_ymd: int) -> pd.DataFrame:
    date_expr = _date_expr("b.date")
    with duckdb.connect(str(db_path), read_only=True) as conn:
        tables = {
            str(row[0]).lower()
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "tickers" in tables:
            name_join = "LEFT JOIN tickers t ON t.code = b.code"
            name_expr = "COALESCE(t.name, b.code)"
        elif "stock_meta" in tables:
            name_join = "LEFT JOIN stock_meta t ON t.code = b.code"
            name_expr = "COALESCE(t.name, b.code)"
        else:
            name_join = ""
            name_expr = "b.code"
        frame = conn.execute(
            f"""
            SELECT
                b.code,
                {name_expr} AS name,
                {date_expr} AS ymd,
                b.o,
                b.h,
                b.l,
                b.c,
                b.v,
                m.ma7,
                m.ma20,
                m.ma60,
                COALESCE(b.source, 'pan') AS source
            FROM daily_bars b
            {name_join}
            LEFT JOIN daily_ma m ON m.code = b.code AND m.date = b.date
            WHERE {date_expr} IS NOT NULL
              AND {date_expr} >= ?
              AND COALESCE(b.source, 'pan') <> 'yahoo'
            ORDER BY b.code, ymd
            """,
            [int(start_ymd)],
        ).fetchdf()
    frame["ymd"] = pd.to_numeric(frame["ymd"], errors="coerce")
    frame = frame.dropna(subset=["ymd", "c"]).copy()
    frame["ymd"] = frame["ymd"].astype(int)
    return frame


def _exclude_code_name(code: str, name: str) -> bool:
    text = f"{code} {name}".lower()
    blocked = (
        "etf",
        "reit",
        "etn",
        "next",
        "投信",
        "上場投",
        "投資法人",
        "(投",
        "インデックス",
        "ダブルインバ",
    )
    return any(token in text for token in blocked)


def _linear_slope(values: pd.Series) -> float | None:
    y = pd.to_numeric(values, errors="coerce").dropna()
    if len(y) < 5:
        return None
    x = pd.Series(range(len(y)), dtype="float64")
    y = y.astype("float64").reset_index(drop=True)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    denom = float(((x - x_mean) ** 2).sum())
    if denom <= 0:
        return None
    slope = float(((x - x_mean) * (y - y_mean)).sum() / denom)
    base = float(y.iloc[-1])
    return slope / base if base else None


def _touch_count(frame: pd.DataFrame, upper: float, *, tolerance: float) -> int:
    if upper <= 0:
        return 0
    near = pd.to_numeric(frame["h"], errors="coerce") >= upper * (1.0 - tolerance)
    # Count separated touch clusters, not every adjacent candle.
    count = 0
    previous = False
    for value in near.fillna(False).tolist():
        current = bool(value)
        if current and not previous:
            count += 1
        previous = current
    return count


def _last_breakout_gap(frame: pd.DataFrame, upper: float) -> float | None:
    if upper <= 0:
        return None
    close = pd.to_numeric(frame["c"], errors="coerce")
    max_close = float(close.max()) if close.notna().any() else None
    if max_close is None:
        return None
    return max_close / upper - 1.0


def _shape_score_for_code(group: pd.DataFrame, *, global_latest_ymd: int, max_lag_days: int) -> dict[str, Any] | None:
    g = group.sort_values("ymd").reset_index(drop=True)
    if len(g) < 140:
        return None
    cur = g.iloc[-1]
    latest_ymd = int(cur["ymd"])
    if global_latest_ymd - latest_ymd > max_lag_days:
        return None
    code = str(cur["code"])
    name = str(cur["name"])
    if _exclude_code_name(code, name):
        return None

    w100 = g.tail(100)
    w80 = g.tail(80)
    w60 = g.tail(60)
    w40 = g.tail(40)
    w20 = g.tail(20)
    w10 = g.tail(10)

    close = _safe_float(cur["c"])
    ma7 = _safe_float(cur["ma7"])
    ma20 = _safe_float(cur["ma20"])
    ma60 = _safe_float(cur["ma60"])
    if close is None or ma7 is None or ma20 is None or ma60 is None:
        return None

    high80 = _safe_float(pd.to_numeric(w80["h"], errors="coerce").max())
    low80 = _safe_float(pd.to_numeric(w80["l"], errors="coerce").min())
    high60 = _safe_float(pd.to_numeric(w60["h"], errors="coerce").max())
    low60 = _safe_float(pd.to_numeric(w60["l"], errors="coerce").min())
    high20 = _safe_float(pd.to_numeric(w20["h"], errors="coerce").max())
    low20 = _safe_float(pd.to_numeric(w20["l"], errors="coerce").min())
    if None in (high80, low80, high60, low60, high20, low20) or low80 <= 0 or high80 <= low80:
        return None

    width80 = high80 / low80 - 1.0
    width60 = high60 / low60 - 1.0 if low60 and low60 > 0 else None
    width20 = high20 / low20 - 1.0 if low20 and low20 > 0 else None
    pos80 = (close - low80) / (high80 - low80)
    dist_upper80 = close / high80 - 1.0
    upper_touches = _touch_count(w80, high80, tolerance=0.025)
    lower_slope40 = _linear_slope(pd.to_numeric(w40["l"], errors="coerce").rolling(5).min())
    close_slope20 = _linear_slope(w20["c"])
    ma20_slope20 = _linear_slope(g.tail(25)["ma20"])
    ma_convergence = (max(ma7, ma20, ma60) / min(ma7, ma20, ma60) - 1.0) if min(ma7, ma20, ma60) > 0 else None
    vol20 = _safe_float(pd.to_numeric(w20["v"], errors="coerce").mean())
    vol10 = _safe_float(pd.to_numeric(w10["v"], errors="coerce").mean())
    vol_ratio10_20 = vol10 / vol20 if vol10 is not None and vol20 and vol20 > 0 else None
    last_breakout_gap = _last_breakout_gap(w80.iloc[:-1], high80)

    reasons: list[str] = []
    rejects: list[str] = []

    if not (0.07 <= width80 <= 0.24):
        rejects.append("box_width_not_tight_enough")
    else:
        reasons.append("tight_80d_box")

    if width60 is not None and width20 is not None and width20 <= width60 * 0.72:
        reasons.append("recent_range_compression")
    else:
        rejects.append("no_recent_compression")

    if 0.78 <= pos80 <= 1.03 and -0.055 <= dist_upper80 <= 0.008:
        reasons.append("near_upper_edge_not_extended")
    else:
        rejects.append("not_clean_upper_edge_setup")

    if upper_touches >= 2:
        reasons.append("multiple_upper_touches")
    else:
        rejects.append("upper_touch_count_too_low")

    if lower_slope40 is not None and lower_slope40 >= -0.0008:
        reasons.append("lows_not_falling")
    else:
        rejects.append("lows_still_falling")

    if close >= ma20 and ma7 >= ma20 * 0.985 and ma20 >= ma60 * 0.97:
        reasons.append("ma_support_constructive")
    else:
        rejects.append("ma_support_not_constructive")

    if ma20_slope20 is not None and ma20_slope20 >= -0.0005:
        reasons.append("ma20_flat_to_rising")
    else:
        rejects.append("ma20_declining")

    if ma_convergence is not None and ma_convergence <= 0.13:
        reasons.append("ma_converged")
    else:
        rejects.append("ma_not_converged")

    if vol_ratio10_20 is not None and 0.65 <= vol_ratio10_20 <= 1.45:
        reasons.append("volume_not_exhausted_or_spiking")
    else:
        rejects.append("volume_profile_not_clean")

    if last_breakout_gap is not None and last_breakout_gap <= 0.025:
        reasons.append("not_already_large_breakout")
    else:
        rejects.append("already_broke_or_extended")

    if rejects:
        return None

    score = 0.0
    score += (0.24 - width80) * 8.0
    if width60 is not None and width20 is not None and width60 > 0:
        score += max(0.0, 1.0 - (width20 / width60)) * 1.6
    score += max(0.0, 1.0 - abs(dist_upper80) / 0.055) * 1.5
    score += min(1.2, upper_touches * 0.3)
    score += 0.8 if lower_slope40 is not None and lower_slope40 > 0 else 0.3
    score += 0.8 if ma20_slope20 is not None and ma20_slope20 > 0 else 0.3
    score += 0.6 if ma_convergence is not None and ma_convergence <= 0.08 else 0.2

    return {
        "code": code,
        "name": name,
        "as_of": int(cur["ymd"]),
        "close": round(float(close), 4),
        "shape_score": round(score, 6),
        "shape_class": "box_prebreak_shape_first",
        "visual_review_priority": "high" if score >= 5.0 else "medium",
        "box_width80": round(float(width80), 6),
        "box_width60": round(float(width60), 6) if width60 is not None else None,
        "box_width20": round(float(width20), 6) if width20 is not None else None,
        "box_pos80": round(float(pos80), 6),
        "dist_to_upper80": round(float(dist_upper80), 6),
        "upper80": round(float(high80), 4),
        "lower80": round(float(low80), 4),
        "upper_touch_clusters80": int(upper_touches),
        "lower_slope40": round(float(lower_slope40), 8) if lower_slope40 is not None else None,
        "close_slope20": round(float(close_slope20), 8) if close_slope20 is not None else None,
        "ma20_slope20": round(float(ma20_slope20), 8) if ma20_slope20 is not None else None,
        "ma_convergence_7_20_60": round(float(ma_convergence), 6) if ma_convergence is not None else None,
        "vol_ratio10_20": round(float(vol_ratio10_20), 6) if vol_ratio10_20 is not None else None,
        "reasons": reasons,
        "explicit_non_numeric_first": True,
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _run_screenshots(candidates: list[dict[str, Any]], args: argparse.Namespace, out_dir: Path) -> dict[str, Any]:
    samples = ",".join(f"{row['code']}:{str(row['as_of'])[:4]}-{str(row['as_of'])[4:6]}-{str(row['as_of'])[6:]}" for row in candidates)
    if not samples:
        return {"ran": False, "reason": "no_candidates"}
    command = [
        "node",
        str(REPO_ROOT / "scripts" / "meemee_detail_clean_screenshot_batch_v1.mjs"),
        "--base-url",
        args.base_url,
        "--api-base",
        args.api_base,
        "--output-root",
        str(args.screenshot_root),
        "--samples",
        samples,
        "--viewport",
        args.viewport,
        "--timeout-ms",
        str(args.screenshot_timeout_ms),
    ]
    result = subprocess.run(command, cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=args.screenshot_timeout_ms * max(1, len(candidates)) // 1000 + 60)
    payload = {
        "ran": True,
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    _write_json(out_dir / "screenshot_command_result.json", payload)
    return payload


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = args.out_root / f"{_now_tag()}-shape-first-box-prebreak-scan-v1"
    out_dir.mkdir(parents=True, exist_ok=True)
    daily = _load_daily(args.db_path, start_ymd=args.start_ymd)
    if daily.empty:
        raise RuntimeError("daily_bars source is empty")

    rows: list[dict[str, Any]] = []
    for _, group in daily.groupby("code"):
        row = _shape_score_for_code(group, global_latest_ymd=int(daily["ymd"].max()), max_lag_days=args.max_lag_days)
        if row is not None:
            rows.append(row)
    rows = sorted(rows, key=lambda item: item["shape_score"], reverse=True)
    selected = rows[: args.top_n]
    payload = {
        "schema_version": "tradex_shape_first_box_prebreak_scan_v1",
        "generated_at": _now_iso(),
        "research_phase": "branching_generation",
        "basis": "confirmed_non_yahoo_daily_bars",
        "db_path": str(args.db_path),
        "current_asof_used": int(daily["ymd"].max()),
        "research_fallback": bool(int(daily["ymd"].max()) < 20260630),
        "shape_first_policy": {
            "primary_sort": "shape_score",
            "does_not_use_win_rate_for_selection": True,
            "does_not_use_month_start_statistics": True,
            "visual_review_required_after_scan": True,
        },
        "fixed_shape_conditions": {
            "freshness_guard": f"code latest ymd within {args.max_lag_days} numeric days of global latest ymd",
            "box_width80": "0.07..0.24",
            "recent_compression": "width20 <= 0.72 * width60",
            "upper_edge": "box_pos80 0.78..1.03 and dist_to_upper80 -5.5%..+0.8%",
            "touches": "at least 2 separated upper touch clusters",
            "lows": "40d rolling-low slope not falling materially",
            "ma": "close>=MA20, MA7 near/above MA20, MA20 near/above MA60, MA20 flat-to-rising",
            "volume": "10d average volume not exhausted or spiking versus 20d",
            "extension_guard": "not already large breakout",
        },
        "candidate_count": len(rows),
        "selected_count": len(selected),
        "top_candidates": selected,
        "non_scope": [
            "MeeMee ranking mutation",
            "runtime DB write",
            "win-rate optimization",
            "production adoption",
        ],
    }
    _write_json(out_dir / "shape_first_box_prebreak_scan.json", payload)
    pd.DataFrame(rows).to_csv(out_dir / "shape_first_box_prebreak_candidates.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(selected).to_csv(out_dir / "shape_first_box_prebreak_selected.csv", index=False, encoding="utf-8-sig")
    (out_dir / "screenshot_samples.txt").write_text(
        ",".join(f"{row['code']}:{str(row['as_of'])[:4]}-{str(row['as_of'])[4:6]}-{str(row['as_of'])[6:]}" for row in selected),
        encoding="utf-8",
    )

    screenshot_result = None
    if args.capture_screenshots:
        screenshot_result = _run_screenshots(selected, args, out_dir)
        payload["screenshot_result"] = screenshot_result
        _write_json(out_dir / "shape_first_box_prebreak_scan.json", payload)

    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "current_asof_used": payload["current_asof_used"],
                "research_fallback": payload["research_fallback"],
                "candidate_count": len(rows),
                "selected": [(row["code"], row["name"], row["shape_score"]) for row in selected],
                "screenshot_returncode": None if screenshot_result is None else screenshot_result.get("returncode"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=Path(os.getenv("STOCKS_DB_PATH") or DEFAULT_DB_PATH))
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--start-ymd", type=int, default=20240101)
    parser.add_argument("--top-n", type=int, default=12)
    parser.add_argument("--max-lag-days", type=int, default=5)
    parser.add_argument("--capture-screenshots", action="store_true")
    parser.add_argument("--screenshot-root", type=Path, default=DEFAULT_SCREENSHOT_ROOT)
    parser.add_argument("--base-url", default="http://127.0.0.1:5174")
    parser.add_argument("--api-base", default="http://127.0.0.1:28888/api")
    parser.add_argument("--viewport", default="1440x1000")
    parser.add_argument("--screenshot-timeout-ms", type=int, default=90000)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
