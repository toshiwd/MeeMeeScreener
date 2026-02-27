from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any, TypeVar

import numpy as np
import pandas as pd

from research.config import ResearchConfig, params_hash
from research.storage import ResearchPaths, extract_asof_from_file, now_utc_iso, parse_date, read_csv, write_csv, ymd


FEATURE_COLUMNS: tuple[str, ...] = (
    "ret1",
    "ret3",
    "ret5",
    "ret10",
    "ret20",
    "ret20_sum",
    "atr14",
    "body_wick_ratio",
    "gap_count20",
    "vol_ratio20",
    "vol_spike20",
    "dev_ma7",
    "dev_ma20",
    "dev_ma60",
    "slope_ma7_5",
    "slope_ma20_5",
    "slope_ma60_10",
    "cnt_above_ma7_20",
    "cnt_below_ma7_20",
    "cnt_above_ma20_20",
    "cnt_below_ma20_20",
    "dist_high20",
    "dist_low20",
    "breakout20",
    "breakdown20",
    "weekly_ma13_gap",
    "weekly_ma26_gap",
    "weekly_box_pos",
    "monthly_box_pos",
    "monthly_ma20_gap",
    "monthly_above_ma20",
)
T = TypeVar("T")


def _cache_dir(paths: ResearchPaths, snapshot_id: str, config: ResearchConfig) -> Path:
    return paths.cache_dir(snapshot_id, config.feature_version, config.label_version, params_hash(config))


def _feature_file(paths: ResearchPaths, snapshot_id: str, config: ResearchConfig, asof_date: str) -> Path:
    return _cache_dir(paths, snapshot_id, config) / f"features_{asof_date}.csv"


def _load_snapshot(paths: ResearchPaths, snapshot_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    sdir = paths.snapshot_dir(snapshot_id)
    if not sdir.exists():
        raise FileNotFoundError(f"snapshot not found: {sdir}")
    daily = read_csv(sdir / "daily.csv")
    universe = read_csv(sdir / "universe_monthly.csv")
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    universe["asof_date"] = pd.to_datetime(universe["asof_date"], errors="coerce").dt.normalize()
    daily = daily.dropna(subset=["date", "code", "open", "high", "low", "close"]).copy()
    universe = universe.dropna(subset=["asof_date", "code"]).copy()
    daily["code"] = daily["code"].astype(str).str.strip()
    universe["code"] = universe["code"].astype(str).str.strip()
    return daily, universe


def _compute_aux_features(code_daily: pd.DataFrame, asof_ts: pd.Timestamp) -> dict[str, float]:
    frame = code_daily[code_daily["date"] <= asof_ts].copy()
    if frame.empty:
        return {
            "weekly_ma13_gap": np.nan,
            "weekly_ma26_gap": np.nan,
            "weekly_box_pos": np.nan,
            "monthly_box_pos": np.nan,
            "monthly_ma20_gap": np.nan,
            "monthly_above_ma20": np.nan,
        }

    frame = frame.sort_values("date").set_index("date")
    weekly = frame.resample("W-FRI").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    weekly = weekly.dropna(subset=["open", "high", "low", "close"]).copy()
    weekly_ma13_gap = np.nan
    weekly_ma26_gap = np.nan
    weekly_box_pos = np.nan
    if not weekly.empty:
        weekly["ma13"] = weekly["close"].rolling(13, min_periods=4).mean()
        weekly["ma26"] = weekly["close"].rolling(26, min_periods=6).mean()
        week_last = weekly.iloc[-1]
        close_w = float(week_last["close"])
        ma13 = float(week_last["ma13"]) if not pd.isna(week_last["ma13"]) else np.nan
        ma26 = float(week_last["ma26"]) if not pd.isna(week_last["ma26"]) else np.nan
        if close_w > 0 and not np.isnan(ma13):
            weekly_ma13_gap = abs(close_w - ma13) / close_w
        if close_w > 0 and not np.isnan(ma26):
            weekly_ma26_gap = abs(close_w - ma26) / close_w
        hi20 = float(weekly["high"].rolling(20, min_periods=4).max().iloc[-1])
        lo20 = float(weekly["low"].rolling(20, min_periods=4).min().iloc[-1])
        if close_w > 0 and hi20 > lo20:
            weekly_box_pos = (close_w - lo20) / (hi20 - lo20)

    monthly = frame.resample("ME").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    monthly = monthly.dropna(subset=["open", "high", "low", "close"]).copy()
    monthly_box_pos = np.nan
    monthly_ma20_gap = np.nan
    monthly_above_ma20 = np.nan
    if not monthly.empty:
        body_top = np.maximum(monthly["open"], monthly["close"])
        body_bottom = np.minimum(monthly["open"], monthly["close"])
        box_hi20 = body_top.rolling(20, min_periods=4).max()
        box_lo20 = body_bottom.rolling(20, min_periods=4).min()
        ma20m = monthly["close"].rolling(20, min_periods=4).mean()
        month_last = monthly.iloc[-1]
        close_m = float(month_last["close"])
        hi = float(box_hi20.iloc[-1]) if not pd.isna(box_hi20.iloc[-1]) else np.nan
        lo = float(box_lo20.iloc[-1]) if not pd.isna(box_lo20.iloc[-1]) else np.nan
        ma20 = float(ma20m.iloc[-1]) if not pd.isna(ma20m.iloc[-1]) else np.nan
        if close_m > 0 and not np.isnan(hi) and not np.isnan(lo) and hi > lo:
            monthly_box_pos = (close_m - lo) / (hi - lo)
        if close_m > 0 and not np.isnan(ma20):
            monthly_ma20_gap = (close_m / ma20) - 1.0
            monthly_above_ma20 = 1.0 if close_m >= ma20 else 0.0

    return {
        "weekly_ma13_gap": weekly_ma13_gap,
        "weekly_ma26_gap": weekly_ma26_gap,
        "weekly_box_pos": weekly_box_pos,
        "monthly_box_pos": monthly_box_pos,
        "monthly_ma20_gap": monthly_ma20_gap,
        "monthly_above_ma20": monthly_above_ma20,
    }


def _chunked(items: list[T], chunk_size: int) -> list[list[T]]:
    if chunk_size < 1:
        chunk_size = 1
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _aux_worker_to_file(task: tuple[list[tuple[str, pd.DataFrame]], str, str]) -> str:
    chunk, asof_str, out_csv = task
    asof_ts = parse_date(asof_str)
    out: list[dict[str, Any]] = []
    for code, code_df in chunk:
        aux = _compute_aux_features(code_df, asof_ts)
        aux["code"] = code
        out.append(aux)
    frame = pd.DataFrame(out)
    if frame.empty:
        frame = pd.DataFrame(columns=["code"])
    frame.to_csv(out_csv, index=False, encoding="utf-8")
    return out_csv


def build_features_for_asof(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    asof_date: str,
    force: bool = False,
    workers: int = 1,
    chunk_size: int = 120,
) -> dict[str, Any]:
    asof_ts = parse_date(asof_date)
    asof_str = ymd(asof_ts)
    out_file = _feature_file(paths, snapshot_id, config, asof_str)
    if out_file.exists() and not force:
        cached = read_csv(out_file)
        return {"ok": True, "cached": True, "rows": int(len(cached)), "path": str(out_file)}

    daily, universe = _load_snapshot(paths, snapshot_id)
    universe_codes = set(
        universe.loc[universe["asof_date"] == asof_ts, "code"]
        .astype(str)
        .str.strip()
        .tolist()
    )
    if not universe_codes:
        raise ValueError(f"no universe codes for asof={asof_str}")

    source = daily[(daily["code"].isin(universe_codes)) & (daily["date"] <= asof_ts)].copy()
    if source.empty:
        raise ValueError(f"daily rows are empty for asof={asof_str}")
    source = source.sort_values(["code", "date"]).reset_index(drop=True)

    g = source.groupby("code", sort=False)
    source["close_prev1"] = g["close"].shift(1)
    source["ret1"] = source["close"] / source["close_prev1"] - 1.0
    source["ret3"] = source["close"] / g["close"].shift(3) - 1.0
    source["ret5"] = source["close"] / g["close"].shift(5) - 1.0
    source["ret10"] = source["close"] / g["close"].shift(10) - 1.0
    source["ret20"] = source["close"] / g["close"].shift(20) - 1.0
    source["ret20_sum"] = g["ret1"].transform(lambda s: s.rolling(20, min_periods=4).sum())

    tr1 = source["high"] - source["low"]
    tr2 = (source["high"] - source["close_prev1"]).abs()
    tr3 = (source["low"] - source["close_prev1"]).abs()
    source["tr"] = np.maximum.reduce([tr1.values, tr2.values, tr3.values])
    source["atr14"] = g["tr"].transform(lambda s: s.rolling(14, min_periods=4).mean()) / (source["close"].abs() + 1e-12)

    body = (source["close"] - source["open"]).abs()
    wick = (source["high"] - source["low"] - body).clip(lower=0.0)
    source["body_wick_ratio"] = body / (wick + 1e-6)

    source["gap_flag"] = ((source["open"] / source["close_prev1"] - 1.0).abs() >= 0.01).astype(float)
    source["gap_count20"] = g["gap_flag"].transform(lambda s: s.rolling(20, min_periods=4).sum())

    source["vol_ma20"] = g["volume"].transform(lambda s: s.rolling(20, min_periods=4).mean())
    source["vol_ratio20"] = source["volume"] / (source["vol_ma20"] + 1e-9)
    source["vol_spike20"] = g["vol_ratio20"].transform(lambda s: s.rolling(20, min_periods=4).max())

    source["ma7"] = g["close"].transform(lambda s: s.rolling(7, min_periods=3).mean())
    source["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=4).mean())
    source["ma60"] = g["close"].transform(lambda s: s.rolling(60, min_periods=10).mean())
    source["dev_ma7"] = source["close"] / (source["ma7"] + 1e-9) - 1.0
    source["dev_ma20"] = source["close"] / (source["ma20"] + 1e-9) - 1.0
    source["dev_ma60"] = source["close"] / (source["ma60"] + 1e-9) - 1.0

    source["slope_ma7_5"] = source["ma7"] / (g["ma7"].shift(5) + 1e-9) - 1.0
    source["slope_ma20_5"] = source["ma20"] / (g["ma20"].shift(5) + 1e-9) - 1.0
    source["slope_ma60_10"] = source["ma60"] / (g["ma60"].shift(10) + 1e-9) - 1.0

    source["above_ma7"] = (source["close"] > source["ma7"]).astype(float)
    source["below_ma7"] = (source["close"] < source["ma7"]).astype(float)
    source["above_ma20"] = (source["close"] > source["ma20"]).astype(float)
    source["below_ma20"] = (source["close"] < source["ma20"]).astype(float)
    source["cnt_above_ma7_20"] = g["above_ma7"].transform(lambda s: s.rolling(20, min_periods=4).sum())
    source["cnt_below_ma7_20"] = g["below_ma7"].transform(lambda s: s.rolling(20, min_periods=4).sum())
    source["cnt_above_ma20_20"] = g["above_ma20"].transform(lambda s: s.rolling(20, min_periods=4).sum())
    source["cnt_below_ma20_20"] = g["below_ma20"].transform(lambda s: s.rolling(20, min_periods=4).sum())

    source["high20"] = g["high"].transform(lambda s: s.rolling(20, min_periods=4).max())
    source["low20"] = g["low"].transform(lambda s: s.rolling(20, min_periods=4).min())
    source["dist_high20"] = source["close"] / (source["high20"] + 1e-9) - 1.0
    source["dist_low20"] = source["close"] / (source["low20"] + 1e-9) - 1.0
    source["breakout20"] = source["close"] / (source.groupby("code", sort=False)["high20"].shift(1) + 1e-9) - 1.0
    source["breakdown20"] = source["close"] / (source.groupby("code", sort=False)["low20"].shift(1) + 1e-9) - 1.0

    asof_rows = source[source["date"] == asof_ts].copy()
    if asof_rows.empty:
        raise ValueError(f"asof date not found in daily rows: {asof_str}")
    asof_rows = asof_rows[asof_rows["code"].isin(universe_codes)].copy()
    if asof_rows.empty:
        raise ValueError(f"asof rows empty after universe filter: {asof_str}")

    code_groups = [
        (str(code), code_df.copy())
        for code, code_df in source.groupby("code", sort=False)
        if code in universe_codes
    ]
    aux_rows: list[dict[str, Any]] = []
    resolved_workers = max(1, int(workers))
    used_parallel = resolved_workers > 1 and len(code_groups) > max(2, chunk_size)
    if used_parallel:
        temp_dir = _cache_dir(paths, snapshot_id, config) / f".tmp_features_{asof_str.replace('-', '')}_{datetime.now(timezone.utc).strftime('%H%M%S%f')}"
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        chunked_groups = _chunked(code_groups, chunk_size=max(1, chunk_size))
        tasks: list[tuple[list[tuple[str, pd.DataFrame]], str, str]] = []
        for idx, chunk in enumerate(chunked_groups, start=1):
            out_csv = temp_dir / f"chunk_{idx:05d}.csv"
            tasks.append((chunk, asof_str, str(out_csv)))
        try:
            paths_out: list[str] = []
            with ProcessPoolExecutor(max_workers=resolved_workers) as pool:
                futures = [pool.submit(_aux_worker_to_file, task) for task in tasks]
                for fut in as_completed(futures):
                    paths_out.append(str(fut.result()))
            parts = [read_csv(Path(p)) for p in sorted(paths_out)]
            aux_frame = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["code"])
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        for code, code_df in code_groups:
            aux = _compute_aux_features(code_df, asof_ts)
            aux["code"] = code
            aux_rows.append(aux)
        aux_frame = pd.DataFrame(aux_rows)
    if not aux_frame.empty:
        aux_frame["code"] = aux_frame["code"].astype(str).str.strip()
        aux_frame = aux_frame.sort_values("code").reset_index(drop=True)
    merged = asof_rows.merge(aux_frame, on="code", how="left")

    features = merged[["code"] + list(FEATURE_COLUMNS)].copy()
    features.insert(0, "asof_date", asof_str)
    features["feature_version"] = config.feature_version
    features["created_at"] = now_utc_iso()
    features["snapshot_id"] = snapshot_id

    write_csv(out_file, features)
    return {
        "ok": True,
        "cached": False,
        "rows": int(len(features)),
        "path": str(out_file),
        "workers_used": int(resolved_workers if used_parallel else 1),
    }


def load_feature_history(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    asof_date: str,
) -> pd.DataFrame:
    asof_ts = parse_date(asof_date)
    cache_dir = _cache_dir(paths, snapshot_id, config)
    if not cache_dir.exists():
        return pd.DataFrame(columns=["asof_date", "code", *FEATURE_COLUMNS, "feature_version", "created_at", "snapshot_id"])

    parts: list[pd.DataFrame] = []
    for file in sorted(cache_dir.glob("features_*.csv")):
        file_asof = extract_asof_from_file(file)
        if not file_asof:
            continue
        if parse_date(file_asof) > asof_ts:
            continue
        frame = read_csv(file)
        if not frame.empty:
            parts.append(frame)
    if not parts:
        return pd.DataFrame(columns=["asof_date", "code", *FEATURE_COLUMNS, "feature_version", "created_at", "snapshot_id"])
    return pd.concat(parts, ignore_index=True)
