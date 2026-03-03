from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any, TypeVar

import pandas as pd

from research.config import ResearchConfig, params_hash
from research.storage import ResearchPaths, extract_asof_from_file, now_utc_iso, parse_date, read_csv, write_csv, ymd
T = TypeVar("T")


def _cache_dir(paths: ResearchPaths, snapshot_id: str, config: ResearchConfig) -> Path:
    return paths.cache_dir(snapshot_id, config.feature_version, config.label_version, params_hash(config))


def _label_file(paths: ResearchPaths, snapshot_id: str, config: ResearchConfig, asof_date: str) -> Path:
    return _cache_dir(paths, snapshot_id, config) / f"labels_{asof_date}.csv"


def _load_snapshot(paths: ResearchPaths, snapshot_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sdir = paths.snapshot_dir(snapshot_id)
    if not sdir.exists():
        raise FileNotFoundError(f"snapshot not found: {sdir}")
    daily = read_csv(sdir / "daily.csv")
    universe = read_csv(sdir / "universe_monthly.csv")
    calendar = read_csv(sdir / "calendar_month_ends.csv")
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    universe["asof_date"] = pd.to_datetime(universe["asof_date"], errors="coerce").dt.normalize()
    calendar["asof_date"] = pd.to_datetime(calendar["asof_date"], errors="coerce").dt.normalize()
    daily = daily.dropna(subset=["date", "code", "open", "high", "low", "close"]).copy()
    universe = universe.dropna(subset=["asof_date", "code"]).copy()
    calendar = calendar.dropna(subset=["asof_date"]).drop_duplicates(subset=["asof_date"]).sort_values("asof_date").copy()
    daily["code"] = daily["code"].astype(str).str.strip()
    universe["code"] = universe["code"].astype(str).str.strip()
    return daily, universe, calendar


def _resolve_next_month_end(calendar: pd.DataFrame, asof_ts: pd.Timestamp) -> pd.Timestamp:
    future = calendar.loc[calendar["asof_date"] > asof_ts, "asof_date"].dropna().sort_values()
    if future.empty:
        raise ValueError(f"next month-end is not available in calendar for asof={ymd(asof_ts)}")
    return pd.Timestamp(future.iloc[0]).normalize()


def _cost_drag(config: ResearchConfig) -> float:
    if not config.cost.enabled:
        return 0.0
    return float(config.cost.rate_per_side) * 2.0


def _evaluate_long(
    future: pd.DataFrame,
    entry: float,
    tp_long: float,
    sl_enabled: bool,
    sl_rate: float,
) -> tuple[float, int, str]:
    tp_price = entry * (1.0 + float(tp_long))
    sl_price = entry * (1.0 - float(sl_rate)) if sl_enabled else None

    for row in future.itertuples(index=False):
        high = float(row.high)
        low = float(row.low)
        if sl_price is not None and low <= sl_price:
            return sl_price, 0, "sl"
        if high >= tp_price:
            return tp_price, 1, "tp"
    return float(future.iloc[-1]["close"]), 0, "eom"


def _evaluate_short(
    future: pd.DataFrame,
    entry: float,
    tp_short: float,
    sl_enabled: bool,
    sl_rate: float,
) -> tuple[float, int, str]:
    tp_price = entry * (1.0 - float(tp_short))
    sl_price = entry * (1.0 + float(sl_rate)) if sl_enabled else None

    for row in future.itertuples(index=False):
        high = float(row.high)
        low = float(row.low)
        if sl_price is not None and high >= sl_price:
            return sl_price, 0, "sl"
        if low <= tp_price:
            return tp_price, 1, "tp"
    return float(future.iloc[-1]["close"]), 0, "eom"


def _mae_mfe_long(future: pd.DataFrame, entry: float) -> tuple[float, float]:
    min_low = float(future["low"].min())
    max_high = float(future["high"].max())
    mae = max(0.0, (entry - min_low) / entry)
    mfe = max(0.0, (max_high - entry) / entry)
    return mae, mfe


def _mae_mfe_short(future: pd.DataFrame, entry: float) -> tuple[float, float]:
    max_high = float(future["high"].max())
    min_low = float(future["low"].min())
    mae = max(0.0, (max_high - entry) / entry)
    mfe = max(0.0, (entry - min_low) / entry)
    return mae, mfe


def _chunked(items: list[T], chunk_size: int) -> list[list[T]]:
    if chunk_size < 1:
        chunk_size = 1
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _label_chunk_worker(
    task: tuple[
        list[tuple[str, pd.DataFrame]],
        str,
        str,
        float,
        float,
        bool,
        float,
        float,
    ],
) -> list[dict[str, Any]]:
    chunk, asof_str, next_month_end_str, tp_long, tp_short, sl_enabled, sl_rate, trade_cost = task
    asof_ts = parse_date(asof_str)
    next_month_end = parse_date(next_month_end_str)
    out: list[dict[str, Any]] = []

    for code, code_daily in chunk:
        if code_daily.empty:
            continue
        code_daily = code_daily.sort_values("date")
        entry_row = code_daily[code_daily["date"] == asof_ts]
        if entry_row.empty:
            continue
        entry = float(entry_row.iloc[-1]["close"])
        future = code_daily[(code_daily["date"] > asof_ts) & (code_daily["date"] <= next_month_end)].copy()
        if future.empty:
            continue

        long_exit, long_tp, long_reason = _evaluate_long(
            future=future,
            entry=entry,
            tp_long=tp_long,
            sl_enabled=sl_enabled,
            sl_rate=sl_rate,
        )
        long_mae, long_mfe = _mae_mfe_long(future, entry)
        long_gross = (long_exit / entry) - 1.0
        # --- Sharpeベース品質ラベル ---
        _rv20_long = float(
            code_daily[code_daily["date"] <= asof_ts]["close"]
            .pct_change().tail(20).std() * (20 ** 0.5)
        ) if len(code_daily) >= 20 else 0.05
        _rv20_long = max(_rv20_long, 0.01)
        _lq = float(long_gross) / _rv20_long  # Sharpe類似スコア
        _lhc = int(long_tp == 1 and long_mae < 0.04 and _lq > 0.4)
        out.append(
            {
                "asof_date": asof_str,
                "code": code,
                "side": "long",
                "realized_return": float(long_gross - trade_cost),
                "tp_hit": int(long_tp),
                "mae": float(long_mae),
                "mfe": float(long_mfe),
                "entry_price": float(entry),
                "exit_price": float(long_exit),
                "exit_reason": long_reason,
                "label_quality": float(_lq),
                "label_high_conf": int(_lhc),
                "rv20": float(_rv20_long),
            }
        )

        short_exit, short_tp, short_reason = _evaluate_short(
            future=future,
            entry=entry,
            tp_short=tp_short,
            sl_enabled=sl_enabled,
            sl_rate=sl_rate,
        )
        short_mae, short_mfe = _mae_mfe_short(future, entry)
        short_gross = (entry - short_exit) / entry
        _rv20_short = float(
            code_daily[code_daily["date"] <= asof_ts]["close"]
            .pct_change().tail(20).std() * (20 ** 0.5)
        ) if len(code_daily) >= 20 else 0.05
        _rv20_short = max(_rv20_short, 0.01)
        _sq = float(short_gross) / _rv20_short
        _shc = int(short_tp == 1 and short_mae < 0.04 and _sq > 0.4)
        out.append(
            {
                "asof_date": asof_str,
                "code": code,
                "side": "short",
                "realized_return": float(short_gross - trade_cost),
                "tp_hit": int(short_tp),
                "mae": float(short_mae),
                "mfe": float(short_mfe),
                "entry_price": float(entry),
                "exit_price": float(short_exit),
                "exit_reason": short_reason,
                "label_quality": float(_sq),
                "label_high_conf": int(_shc),
                "rv20": float(_rv20_short),
            }
        )
    return out


def _label_chunk_worker_to_file(
    task: tuple[
        list[tuple[str, pd.DataFrame]],
        str,
        str,
        float,
        float,
        bool,
        float,
        float,
        str,
    ],
) -> str:
    chunk, asof_str, next_month_end_str, tp_long, tp_short, sl_enabled, sl_rate, trade_cost, out_csv = task
    rows = _label_chunk_worker(
        (
            chunk,
            asof_str,
            next_month_end_str,
            tp_long,
            tp_short,
            sl_enabled,
            sl_rate,
            trade_cost,
        )
    )
    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(
            columns=[
                "asof_date",
                "code",
                "side",
                "realized_return",
                "tp_hit",
                "mae",
                "mfe",
                "entry_price",
                "exit_price",
                "exit_reason",
            ]
        )
    frame.to_csv(out_csv, index=False, encoding="utf-8")
    return out_csv


def build_labels_for_asof(
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
    out_file = _label_file(paths, snapshot_id, config, asof_str)
    if out_file.exists() and not force:
        cached = read_csv(out_file)
        return {"ok": True, "cached": True, "rows": int(len(cached)), "path": str(out_file)}

    daily, universe, calendar = _load_snapshot(paths, snapshot_id)
    universe_codes = set(
        universe.loc[universe["asof_date"] == asof_ts, "code"]
        .astype(str)
        .str.strip()
        .tolist()
    )
    if not universe_codes:
        raise ValueError(f"no universe codes for asof={asof_str}")

    next_month_end = _resolve_next_month_end(calendar, asof_ts)
    rows: list[dict[str, Any]] = []
    trade_cost = _cost_drag(config)
    code_groups = [
        (str(code), code_df.copy())
        for code, code_df in daily[daily["code"].isin(universe_codes)].groupby("code", sort=False)
    ]
    resolved_workers = max(1, int(workers))
    used_parallel = resolved_workers > 1 and len(code_groups) > max(2, chunk_size)
    if used_parallel:
        temp_dir = _cache_dir(paths, snapshot_id, config) / f".tmp_labels_{asof_str.replace('-', '')}_{datetime.now(timezone.utc).strftime('%H%M%S%f')}"
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        temp_dir.mkdir(parents=True, exist_ok=True)
        chunked_groups = _chunked(code_groups, chunk_size=max(1, chunk_size))
        tasks: list[tuple[list[tuple[str, pd.DataFrame]], str, str, float, float, bool, float, float, str]] = []
        for idx, chunk in enumerate(chunked_groups, start=1):
            out_csv = temp_dir / f"chunk_{idx:05d}.csv"
            tasks.append(
                (
                    chunk,
                    asof_str,
                    ymd(next_month_end),
                    float(config.tp_long),
                    float(config.tp_short),
                    bool(config.stop_loss.enabled),
                    float(config.stop_loss.rate),
                    float(trade_cost),
                    str(out_csv),
                )
            )
        try:
            out_paths: list[str] = []
            with ProcessPoolExecutor(max_workers=resolved_workers) as pool:
                futures = [pool.submit(_label_chunk_worker_to_file, task) for task in tasks]
                for fut in as_completed(futures):
                    out_paths.append(str(fut.result()))
            parts = [read_csv(Path(p)) for p in sorted(out_paths)]
            labels_from_chunks = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            if not labels_from_chunks.empty:
                rows = labels_from_chunks.to_dict(orient="records")
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        for code, code_df in code_groups:
            rows.extend(
                _label_chunk_worker(
                    (
                        [(code, code_df)],
                        asof_str,
                        ymd(next_month_end),
                        float(config.tp_long),
                        float(config.tp_short),
                        bool(config.stop_loss.enabled),
                        float(config.stop_loss.rate),
                        float(trade_cost),
                    )
                )
            )

    labels = pd.DataFrame(rows)
    if labels.empty:
        labels = pd.DataFrame(
            columns=[
                "asof_date",
                "code",
                "side",
                "realized_return",
                "tp_hit",
                "mae",
                "mfe",
                "entry_price",
                "exit_price",
                "exit_reason",
            ]
        )

    labels["label_version"] = config.label_version
    labels["created_at"] = now_utc_iso()
    labels["snapshot_id"] = snapshot_id
    if not labels.empty:
        labels = labels.sort_values(["asof_date", "code", "side"]).reset_index(drop=True)
    write_csv(out_file, labels)
    return {
        "ok": True,
        "cached": False,
        "rows": int(len(labels)),
        "path": str(out_file),
        "workers_used": int(resolved_workers if used_parallel else 1),
    }


def load_label_history(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    asof_date: str,
) -> pd.DataFrame:
    asof_ts = parse_date(asof_date)
    cache_dir = _cache_dir(paths, snapshot_id, config)
    if not cache_dir.exists():
        return pd.DataFrame(
            columns=[
                "asof_date",
                "code",
                "side",
                "realized_return",
                "tp_hit",
                "mae",
                "mfe",
                "entry_price",
                "exit_price",
                "exit_reason",
                "label_version",
                "created_at",
                "snapshot_id",
            ]
        )

    parts: list[pd.DataFrame] = []
    for file in sorted(cache_dir.glob("labels_*.csv")):
        file_asof = extract_asof_from_file(file)
        if not file_asof:
            continue
        if parse_date(file_asof) > asof_ts:
            continue
        frame = read_csv(file)
        if not frame.empty:
            parts.append(frame)
    if not parts:
        return pd.DataFrame(
            columns=[
                "asof_date",
                "code",
                "side",
                "realized_return",
                "tp_hit",
                "mae",
                "mfe",
                "entry_price",
                "exit_price",
                "exit_reason",
                "label_version",
                "created_at",
                "snapshot_id",
            ]
        )
    return pd.concat(parts, ignore_index=True)
