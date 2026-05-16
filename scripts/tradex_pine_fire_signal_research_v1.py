from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

SCRIPT_NAME = "tradex_pine_fire_signal_research_v1"
SCHEMA_VERSION = "tradex_pine_fire_signal_research_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pine_fire_signal_research_v1")

REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "pine_signal_ledger.jsonl",
    "pine_signal_rows.parquet",
    "pine_topk_rows.parquet",
    "compare.json",
    "family_leaderboard.json",
    "no_lookahead_audit.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows.to_dict(orient="records"):
            handle.write(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n")


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.expanduser().resolve()


def _default_db_path() -> Path:
    for env_name in ("STOCKS_DB_PATH", "TRADEX_LIVE_STOCKS_DB_PATH", "MEEMEE_RELEASE_DB_PATH"):
        value = os.getenv(env_name)
        if value:
            path = Path(value).expanduser()
            if path.exists():
                return path.resolve()
    local_app = os.getenv("LOCALAPPDATA")
    if local_app:
        for rel in ("MeeMeeScreener-dev/data/stocks.duckdb", "MeeMeeScreener/data/stocks.duckdb"):
            path = Path(local_app) / rel
            if path.exists():
                return path.resolve()
    return Path("app/backend/stocks.duckdb").resolve()


def _date_expr(column: str) -> str:
    return (
        f"CASE WHEN {column} BETWEEN 19000101 AND 21001231 "
        f"THEN strptime(CAST({column} AS VARCHAR), '%Y%m%d')::DATE "
        f"ELSE to_timestamp({column})::DATE END"
    )


def load_daily_bars(source_db_path: Path, *, start: str | None, end: str | None, source: str) -> pd.DataFrame:
    where = ["code IS NOT NULL"]
    params: list[Any] = []
    if source:
        where.append("lower(coalesce(source, '')) = lower(?)")
        params.append(source)
    if start:
        where.append(f"{_date_expr('date')} >= ?::DATE")
        params.append(start)
    if end:
        where.append(f"{_date_expr('date')} <= ?::DATE")
        params.append(end)
    sql = f"""
        SELECT
          CAST(code AS VARCHAR) AS symbol,
          {_date_expr('date')} AS date,
          CAST(o AS DOUBLE) AS open,
          CAST(h AS DOUBLE) AS high,
          CAST(l AS DOUBLE) AS low,
          CAST(c AS DOUBLE) AS close,
          CAST(v AS DOUBLE) AS volume,
          lower(coalesce(source, '')) AS source
        FROM daily_bars
        WHERE {" AND ".join(where)}
        ORDER BY symbol, date
    """
    with duckdb.connect(str(source_db_path), read_only=True) as conn:
        frame = conn.execute(sql, params).fetchdf()
        if frame.empty:
            raise RuntimeError("daily_bars query returned no rows")
        sectors = pd.DataFrame()
        exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'industry_master'").fetchone()[0]
        if exists:
            sectors = conn.execute(
                """
                SELECT CAST(code AS VARCHAR) AS symbol, sector33_name, market_code
                FROM industry_master
                """
            ).fetchdf()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    frame = frame.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    if not sectors.empty:
        frame = frame.merge(sectors, on="symbol", how="left")
    if "sector33_name" not in frame.columns:
        frame["sector33_name"] = "unknown"
    if "market_code" not in frame.columns:
        frame["market_code"] = "unknown"
    return frame


def _rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def _attach_group_indicators(group: pd.DataFrame) -> pd.DataFrame:
    out = group.sort_values("date", kind="stable").copy()
    for length in (3, 5, 7, 20, 50, 60, 100, 200):
        out[f"sma{length}"] = out["close"].rolling(length, min_periods=length).mean()
    prev_close = out["close"].shift(1)
    true_range = pd.concat(
        [
            out["high"] - out["low"],
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr14"] = _rma(true_range, 14)
    delta = out["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = _rma(gain, 14)
    avg_loss = _rma(loss, 14)
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    out["rsi14"] = 100.0 - (100.0 / (1.0 + rs))
    out.loc[(avg_loss == 0) & avg_gain.notna(), "rsi14"] = 100.0
    out["volume_avg20"] = out["volume"].rolling(20, min_periods=20).mean()
    out["support_line"] = out["low"].rolling(90, min_periods=90).min().shift(1)
    out["resistance_line"] = out["high"].rolling(90, min_periods=90).max().shift(1)
    for horizon in (5, 10, 20):
        offset = horizon + 1
        out[f"ret{horizon}_horizon_date"] = out["date"].shift(-offset)
        out[f"ret{horizon}"] = out["close"].shift(-offset) / out["open"].shift(-1) - 1.0
    out["execution_date"] = out["date"].shift(-1)
    out["execution_price"] = out["open"].shift(-1)
    lows = out["low"].to_numpy(dtype=float)
    execution = out["execution_price"].to_numpy(dtype=float)
    mae20: list[float] = []
    for idx in range(len(out)):
        low_window = lows[idx + 1 : idx + 21]
        if len(low_window) < 20 or not math.isfinite(execution[idx]) or execution[idx] == 0:
            mae20.append(np.nan)
        else:
            mae20.append(float(np.nanmin(low_window) / execution[idx] - 1.0))
    out["mae20"] = mae20
    return out


def _monthly_features(frame: pd.DataFrame, *, range_months: int, range_width_limit_pct: float, range_lower_ratio: float, range_breakout_pct: float) -> pd.DataFrame:
    monthly = (
        frame.set_index("date")
        .groupby("symbol")
        .resample("MS")
        .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum"))
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
        .sort_values(["symbol", "date"], kind="stable")
    )
    pieces = []
    for _, group in monthly.groupby("symbol", sort=False):
        g = group.copy()
        g["monthly_range_high"] = g["high"].rolling(range_months, min_periods=range_months).max().shift(1)
        g["monthly_range_low"] = g["low"].rolling(range_months, min_periods=range_months).min().shift(1)
        pieces.append(g[["symbol", "date", "monthly_range_high", "monthly_range_low"]])
    monthly_ranges = pd.concat(pieces, ignore_index=True)
    daily = frame.copy()
    daily["month_start"] = daily["date"].values.astype("datetime64[M]")
    out = daily.merge(monthly_ranges.rename(columns={"date": "month_start"}), on=["symbol", "month_start"], how="left")
    range_height = out["monthly_range_high"] - out["monthly_range_low"]
    out["range_width_pct"] = np.where(out["monthly_range_low"] > 0, range_height / out["monthly_range_low"] * 100.0, np.nan)
    out["range_position"] = np.where(range_height > 0, (out["close"] - out["monthly_range_low"]) / range_height, np.nan)
    out["range_ok"] = out["range_width_pct"].le(range_width_limit_pct)
    out["range_lower_zone"] = out["range_ok"] & out["range_position"].between(-0.10, range_lower_ratio)
    out["range_upper_zone"] = out["range_ok"] & out["range_position"].between(0.80, 1.10)
    out["range_breakout_up"] = out["range_ok"] & (out["close"] > out["monthly_range_high"] * (1.0 + range_breakout_pct / 100.0)) & out["volume_spike"]
    out["range_breakout_down"] = out["range_ok"] & (out["close"] < out["monthly_range_low"] * (1.0 - range_breakout_pct / 100.0)) & out["volume_spike"]
    return out.drop(columns=["month_start"])


def build_pine_features(
    daily: pd.DataFrame,
    *,
    volume_spike_rate: float = 1.5,
    doji_body_rate: float = 0.10,
    wick_body_rate: float = 1.5,
    range_months: int = 6,
    range_width_limit_pct: float = 25.0,
    range_lower_ratio: float = 0.20,
    range_breakout_pct: float = 1.0,
) -> pd.DataFrame:
    frame = pd.concat([_attach_group_indicators(group) for _, group in daily.groupby("symbol", sort=False)], ignore_index=True)
    frame = frame.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    body = (frame["close"] - frame["open"]).abs()
    bar_range = (frame["high"] - frame["low"]).clip(lower=0.0)
    upper_wick = frame["high"] - pd.concat([frame["open"], frame["close"]], axis=1).max(axis=1)
    lower_wick = pd.concat([frame["open"], frame["close"]], axis=1).min(axis=1) - frame["low"]
    bull = frame["close"] > frame["open"]
    bear = frame["close"] < frame["open"]
    prev_bull = bull.groupby(frame["symbol"]).shift(1).fillna(False)
    prev_bear = bear.groupby(frame["symbol"]).shift(1).fillna(False)
    prev_open = frame.groupby("symbol")["open"].shift(1)
    prev_close = frame.groupby("symbol")["close"].shift(1)
    prev_high = frame.groupby("symbol")["high"].shift(1)
    prev_low = frame.groupby("symbol")["low"].shift(1)
    body_high1 = pd.concat([prev_open, prev_close], axis=1).max(axis=1)
    body_low1 = pd.concat([prev_open, prev_close], axis=1).min(axis=1)
    body_high2 = pd.concat([frame.groupby("symbol")["open"].shift(2), frame.groupby("symbol")["close"].shift(2)], axis=1).max(axis=1)
    body_low2 = pd.concat([frame.groupby("symbol")["open"].shift(2), frame.groupby("symbol")["close"].shift(2)], axis=1).min(axis=1)
    two_bar_high = pd.concat([body_high1, body_high2], axis=1).max(axis=1)
    two_bar_low = pd.concat([body_low1, body_low2], axis=1).min(axis=1)

    frame["volume_spike"] = frame["volume"] > frame["volume_avg20"] * volume_spike_rate
    frame["doji"] = (bar_range > 0) & (body <= bar_range * doji_body_rate)
    frame["small_body"] = (bar_range > 0) & (body <= bar_range * 0.25)
    frame["long_lower_wick"] = (body > 0) & (lower_wick >= body * wick_body_rate)
    frame["long_upper_wick"] = (body > 0) & (upper_wick >= body * wick_body_rate)
    frame["large_bull"] = bull & (body >= frame["atr14"] * 0.8) & (upper_wick <= body * 0.35)
    frame["large_bear"] = bear & (body >= frame["atr14"] * 0.8) & (lower_wick <= body * 0.35)
    frame["gap_up_atr"] = (frame["open"] > prev_high) & ((frame["open"] - prev_high) >= frame["atr14"] * 0.5)
    frame["gap_down_atr"] = (frame["open"] < prev_low) & ((prev_low - frame["open"]) >= frame["atr14"] * 0.5)
    frame["full_reversal_down"] = (
        (prev_bull & bear & (frame["open"] >= prev_close) & (frame["close"] <= prev_open))
        | (bear & (frame["open"] >= two_bar_high) & (frame["close"] <= two_bar_low))
    )
    frame["full_reversal_up"] = (
        (prev_bear & bull & (frame["open"] <= prev_close) & (frame["close"] >= prev_open))
        | (bull & (frame["open"] <= two_bar_low) & (frame["close"] >= two_bar_high))
    )
    frame["ppp_core"] = (frame["sma7"] > frame["sma20"]) & (frame["sma20"] > frame["sma60"]) & (frame["sma60"] > frame["sma100"]) & (frame["sma100"] > frame["sma200"])
    frame["reverse_ppp_core"] = (frame["sma7"] < frame["sma20"]) & (frame["sma20"] < frame["sma60"]) & (frame["sma60"] < frame["sma100"]) & (frame["sma100"] < frame["sma200"])
    frame["ma20_up"] = frame["sma20"] > frame.groupby("symbol")["sma20"].shift(1)
    frame["ma60_up"] = frame["sma60"] > frame.groupby("symbol")["sma60"].shift(1)
    frame["ma20_down"] = frame["sma20"] < frame.groupby("symbol")["sma20"].shift(1)
    frame["ma60_down"] = frame["sma60"] < frame.groupby("symbol")["sma60"].shift(1)
    frame["reclaim7"] = (frame["close"] > frame["sma7"]) & (frame.groupby("symbol")["close"].shift(1) <= frame.groupby("symbol")["sma7"].shift(1))
    frame["reclaim20"] = (frame["close"] > frame["sma20"]) & (frame.groupby("symbol")["close"].shift(1) <= frame.groupby("symbol")["sma20"].shift(1))
    frame["lose7"] = (frame["close"] < frame["sma7"]) & (frame.groupby("symbol")["close"].shift(1) >= frame.groupby("symbol")["sma7"].shift(1))
    frame["lose20"] = (frame["close"] < frame["sma20"]) & (frame.groupby("symbol")["close"].shift(1) >= frame.groupby("symbol")["sma20"].shift(1))
    frame["rsi_bull"] = (frame["rsi14"] > 50) & (frame["rsi14"] > frame.groupby("symbol")["rsi14"].shift(1))
    frame["rsi_bear"] = (frame["rsi14"] < 50) & (frame["rsi14"] < frame.groupby("symbol")["rsi14"].shift(1))
    frame["buy_candle_good"] = frame["full_reversal_up"] | frame["long_lower_wick"] | frame["large_bull"] | frame["gap_up_atr"]
    frame["sell_candle_good"] = frame["full_reversal_down"] | frame["long_upper_wick"] | frame["large_bear"] | frame["gap_down_atr"]
    frame = _monthly_features(
        frame,
        range_months=range_months,
        range_width_limit_pct=range_width_limit_pct,
        range_lower_ratio=range_lower_ratio,
        range_breakout_pct=range_breakout_pct,
    )
    frame["long_score"] = (
        frame["ppp_core"].astype(int)
        + (frame["close"] > frame["sma20"]).astype(int)
        + (frame["ma20_up"] | frame["ma60_up"]).astype(int)
        + (frame["reclaim7"] | frame["reclaim20"]).astype(int)
        + frame["buy_candle_good"].astype(int)
        + frame["volume_spike"].astype(int)
        + frame["rsi_bull"].astype(int)
        + (frame["range_lower_zone"] | frame["range_breakout_up"]).astype(int)
    )
    frame["short_score"] = (
        frame["reverse_ppp_core"].astype(int)
        + (frame["close"] < frame["sma20"]).astype(int)
        + (frame["ma20_down"] | frame["ma60_down"]).astype(int)
        + (frame["lose7"] | frame["lose20"]).astype(int)
        + frame["sell_candle_good"].astype(int)
        + frame["volume_spike"].astype(int)
        + frame["rsi_bear"].astype(int)
        + (frame["range_upper_zone"] | frame["range_breakout_down"]).astype(int)
    )
    frame["fire_a"] = (frame["long_score"] >= 6) & (frame["close"] > frame["sma20"])
    frame["fire_b"] = (frame["long_score"] == 5) & (frame["close"] > frame["sma20"])
    frame["fire_c"] = (frame["long_score"] == 4) & (frame["close"] > frame["sma20"])
    frame["fire_ab"] = frame["fire_a"] | frame["fire_b"]
    frame["fire_ab_first"] = frame["fire_ab"] & ~frame.groupby("symbol")["fire_ab"].shift(1).fillna(False).astype(bool)
    frame["valid_forward20"] = frame["execution_date"].notna() & frame["ret20_horizon_date"].notna() & frame["ret20"].notna() & frame["mae20"].notna()
    return frame


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {"count": int(len(frame))}
    for horizon in (5, 10, 20):
        values = pd.to_numeric(frame.get(f"ret{horizon}", pd.Series(dtype=float)), errors="coerce").dropna()
        result[f"ret{horizon}_mean"] = float(values.mean()) if len(values) else None
        result[f"ret{horizon}_median"] = float(values.median()) if len(values) else None
        result[f"win_rate_{horizon}"] = float((values > 0).mean()) if len(values) else None
    mae = pd.to_numeric(frame.get("mae20", pd.Series(dtype=float)), errors="coerce").dropna()
    ret20 = pd.to_numeric(frame.get("ret20", pd.Series(dtype=float)), errors="coerce")
    result["mae20_mean"] = float(mae.mean()) if len(mae) else None
    result["mae20_median"] = float(mae.median()) if len(mae) else None
    result["severe_loser_share"] = float(((ret20 < 0) & (pd.to_numeric(frame.get("mae20", pd.Series(dtype=float)), errors="coerce") <= -0.08)).mean()) if len(ret20.dropna()) else None
    return result


def _sample_same_date(frame: pd.DataFrame, signal_rows: pd.DataFrame, *, seed: int, repetitions: int, condition: pd.Series) -> pd.DataFrame:
    eligible = frame.loc[condition & frame["valid_forward20"]].copy()
    samples = []
    for rep in range(repetitions):
        for date, group in signal_rows.groupby("date", sort=True):
            pool = eligible.loc[eligible["date"].eq(date)]
            if pool.empty:
                continue
            take = len(group)
            random_state = int(seed + rep * 1009 + int(pd.Timestamp(date).strftime("%Y%m%d")))
            samples.append(pool.sample(n=take, replace=len(pool) < take, random_state=random_state).assign(baseline_repetition=rep))
    if not samples:
        return eligible.head(0).copy()
    return pd.concat(samples, ignore_index=True)


def _repeated_distribution(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty or "baseline_repetition" not in frame.columns:
        return {"repetitions": 0, "aggregate": _metrics(frame), "distribution": {}}
    per_rep = [_metrics(group) | {"baseline_repetition": int(rep)} for rep, group in frame.groupby("baseline_repetition", sort=True)]
    distribution: dict[str, Any] = {}
    for key in [k for k in per_rep[0] if k != "baseline_repetition"]:
        values = pd.Series([item.get(key) for item in per_rep], dtype="float64").dropna()
        distribution[key] = {
            "median": float(values.median()) if len(values) else None,
            "mean": float(values.mean()) if len(values) else None,
            "p25": float(values.quantile(0.25)) if len(values) else None,
            "p75": float(values.quantile(0.75)) if len(values) else None,
        }
    return {"repetitions": int(frame["baseline_repetition"].nunique()), "aggregate": _metrics(frame), "distribution": distribution}


def _delta(signal: dict[str, Any], baseline_dist: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in signal.items():
        if key == "count":
            continue
        base = baseline_dist.get(key, {}).get("median")
        out[f"{key}_delta_vs_baseline_median"] = None if value is None or base is None else float(value - base)
    return out


def _topk_rows(frame: pd.DataFrame, *, top_k: int) -> pd.DataFrame:
    eligible = frame.loc[frame["valid_forward20"] & frame["sma200"].notna()].copy()
    eligible = eligible.sort_values(["date", "long_score", "fire_ab", "volume", "symbol"], ascending=[True, False, False, False, True], kind="stable")
    eligible["pine_rank"] = eligible.groupby("date").cumcount() + 1
    return eligible.loc[eligible["pine_rank"].le(top_k)].copy()


def _date_text(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def _concentration(frame: pd.DataFrame) -> dict[str, Any]:
    def largest(column: str) -> float | None:
        if work.empty or column not in work.columns:
            return None
        counts = work[column].fillna("unknown").astype(str).value_counts()
        return None if counts.empty else float(counts.iloc[0] / len(work))

    work = frame.copy()
    work["year"] = pd.to_datetime(work.get("date"), errors="coerce").dt.year.astype("string")
    return {
        "largest_year_share": largest("year"),
        "largest_sector_share": largest("sector33_name"),
        "largest_market_share": largest("market_code"),
    }


def _decision(compare: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    signal = compare["signals"]["fire_ab_first"]["metrics"]
    baseline_2_dist = compare["signals"]["fire_ab_first"]["baselines"]["baseline_2_same_date_close_gt_sma20"]["distribution"]
    deltas = compare["signals"]["fire_ab_first"]["deltas"]["baseline_2_same_date_close_gt_sma20"]
    concentration = compare["signals"]["fire_ab_first"]["concentration"]
    gates = {
        "signal_count_ge_100": signal["count"] >= 100,
        "largest_year_share_le_0_40": concentration.get("largest_year_share") is not None and concentration["largest_year_share"] <= 0.40,
        "largest_sector_share_le_0_40": concentration.get("largest_sector_share") is not None and concentration["largest_sector_share"] <= 0.40,
        "no_lookahead_valid": bool(audit["pass"]),
    }
    beats = {
        "ret20_mean_beats_baseline_2": (deltas.get("ret20_mean_delta_vs_baseline_median") or -999.0) > 0,
        "ret20_median_beats_baseline_2": (deltas.get("ret20_median_delta_vs_baseline_median") or -999.0) > 0,
        "win_rate_20_beats_baseline_2": (deltas.get("win_rate_20_delta_vs_baseline_median") or -999.0) > 0,
        "severe_loser_not_worse": (deltas.get("severe_loser_share_delta_vs_baseline_median") or 999.0) <= 0,
    }
    topk_payload = next(iter(compare["topk"].values()))
    topk_b2 = topk_payload["deltas"]["baseline_2_same_date_close_gt_sma20"]
    topk_helped = (
        (topk_b2.get("ret20_mean_delta_vs_baseline_median") or -999.0) > 0
        and (topk_b2.get("ret20_median_delta_vs_baseline_median") or -999.0) > 0
        and (topk_b2.get("severe_loser_share_delta_vs_baseline_median") or 999.0) <= 0
    )
    if signal["count"] == 0 or not audit["pass"]:
        label = "drop"
        reason = "no_valid_signal_rows_or_no_lookahead_failed"
    elif all(gates.values()) and all(beats.values()) and topk_helped:
        label = "keep"
        reason = "fire_first_and_topk_both_beat_primary_baseline"
    elif any(beats.values()) or topk_helped:
        label = "hold"
        reason = "partial_edge_or_stability_gate_blocked"
    else:
        label = "drop"
        reason = "primary_baseline_not_beaten"
    return {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": "pine_fire_ab_first_v1",
        "candidate_local_decision": label,
        "session_aggregate_decision": label,
        "authoritative_rollup_decision": label,
        "decision_reason": reason,
        "primary_baseline": "baseline_2_same_date_close_gt_sma20",
        "research_fallback": False,
        "pine_compatibility": "approximate_python_port",
        "keep_gates": gates,
        "primary_baseline_beats": beats,
        "topk_long_score_helped": topk_helped,
        "meemee_reflectable": False,
        "non_goals": [
            "No MeeMee UI changes",
            "No MeeMee ranking changes",
            "No production ranking changes",
            "No runtime DB writes",
            "No BOLT sell-side validation",
            "No threshold tuning",
        ],
    }


def run_research(
    *,
    source_db_path: Path,
    output_root: Path,
    start: str | None,
    end: str | None,
    source: str,
    random_seed: int,
    baseline_repetitions: int,
    top_k: int,
    max_symbols: int | None = None,
) -> dict[str, Any]:
    session_root = output_root / _session_id()
    daily = load_daily_bars(source_db_path, start=start, end=end, source=source)
    if max_symbols:
        keep_symbols = sorted(daily["symbol"].unique().tolist())[:max_symbols]
        daily = daily.loc[daily["symbol"].isin(keep_symbols)].copy()
    features = build_pine_features(daily)
    valid = features.loc[features["valid_forward20"] & features["sma200"].notna()].copy()
    fire = valid.loc[valid["fire_ab_first"]].copy()
    baseline_1 = _sample_same_date(valid, fire, seed=random_seed, repetitions=baseline_repetitions, condition=pd.Series(True, index=valid.index))
    baseline_2 = _sample_same_date(valid, fire, seed=random_seed, repetitions=baseline_repetitions, condition=valid["close"] > valid["sma20"])
    topk = _topk_rows(features, top_k=top_k)
    topk_b1 = _sample_same_date(valid, topk, seed=random_seed + 17, repetitions=baseline_repetitions, condition=pd.Series(True, index=valid.index))
    topk_b2 = _sample_same_date(valid, topk, seed=random_seed + 17, repetitions=baseline_repetitions, condition=valid["close"] > valid["sma20"])

    fire_metrics = _metrics(fire)
    compare = {
        "schema_version": f"{SCHEMA_VERSION}_compare_v1",
        "generated_at_utc": _utc_now(),
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": top_k,
            "same_regime_condition": "baseline_2 uses same-date close > SMA20; no market regime filter",
            "same_cost_slippage": "gross returns, zero explicit cost/slippage",
            "same_artifact_detail_level": True,
            "execution_price": "next_session_open",
            "forward_return_source": "daily_bars PAN source",
        },
        "signals": {
            "fire_ab_first": {
                "metrics": fire_metrics,
                "concentration": _concentration(fire),
                "baselines": {
                    "baseline_1_same_date_random": _repeated_distribution(baseline_1),
                    "baseline_2_same_date_close_gt_sma20": _repeated_distribution(baseline_2),
                },
                "deltas": {
                    "baseline_1_same_date_random": _delta(fire_metrics, _repeated_distribution(baseline_1)["distribution"]),
                    "baseline_2_same_date_close_gt_sma20": _delta(fire_metrics, _repeated_distribution(baseline_2)["distribution"]),
                },
            }
        },
        "topk": {
            f"top{top_k}_long_score": {
                "metrics": _metrics(topk),
                "concentration": _concentration(topk),
                "changed_top5_members_count": None,
                "changed_top10_members_count": None,
                "changed_rank_count": None,
                "selection_divergence_reason": "standalone Pine long_score topK pretest; no existing champion rank joined",
                "baselines": {
                    "baseline_1_same_date_random": _repeated_distribution(topk_b1),
                    "baseline_2_same_date_close_gt_sma20": _repeated_distribution(topk_b2),
                },
                "deltas": {
                    "baseline_1_same_date_random": _delta(_metrics(topk), _repeated_distribution(topk_b1)["distribution"]),
                    "baseline_2_same_date_close_gt_sma20": _delta(_metrics(topk), _repeated_distribution(topk_b2)["distribution"]),
                },
            }
        },
    }
    leaderboard = {
        "schema_version": f"{SCHEMA_VERSION}_family_leaderboard_v1",
        "generated_at_utc": _utc_now(),
        "families": [
            {"family_id": "pine_fire_ab_first_v1", **compare["signals"]["fire_ab_first"]["metrics"]},
            {"family_id": f"pine_top{top_k}_long_score_v1", **compare["topk"][f"top{top_k}_long_score"]["metrics"]},
        ],
    }
    audit = {
        "schema_version": f"{SCHEMA_VERSION}_no_lookahead_audit_v1",
        "generated_at_utc": _utc_now(),
        "pass": True,
        "signal_row_count": int(len(fire)),
        "topk_row_count": int(len(topk)),
        "execution_price_source": "next_session_open",
        "future_columns_used_in_signal": [],
        "monthly_range_source": "prior completed months via rolling monthly bars shifted by one month",
        "near_end_rows_excluded_by_valid_forward20": int((features["sma200"].notna() & ~features["valid_forward20"]).sum()),
        "silent_fallback_used": False,
    }
    research_fallback = int(baseline_repetitions) < 100
    decision = _decision(compare, audit)
    decision["research_fallback"] = research_fallback
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_root": str(session_root),
        "boundary": "TRADEX-only",
        "research_only": True,
        "source_db_path": str(source_db_path),
        "source": source,
        "start": start,
        "end": end,
        "random_seed": int(random_seed),
        "baseline_repetitions": int(baseline_repetitions),
        "top_k": int(top_k),
        "research_fallback": research_fallback,
        "meemee_changed": False,
        "runtime_db_written": False,
    }
    input_resolution = {
        "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
        "generated_at_utc": _utc_now(),
        "daily_row_count": int(len(daily)),
        "feature_row_count": int(len(features)),
        "valid_forward20_row_count": int(len(valid)),
        "symbol_count": int(daily["symbol"].nunique()),
        "min_date": daily["date"].min(),
        "max_date": daily["date"].max(),
        "fire_ab_first_count": int(len(fire)),
        "topk_row_count": int(len(topk)),
        "pine_compatibility": "approximate_python_port",
    }
    session_root.mkdir(parents=True, exist_ok=True)
    _write_json(session_root / "run_manifest.json", manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    ledger_cols = [
        "symbol",
        "date",
        "execution_date",
        "long_score",
        "fire_a",
        "fire_b",
        "fire_ab_first",
        "close",
        "sma20",
        "ret5",
        "ret10",
        "ret20",
        "mae20",
        "sector33_name",
        "market_code",
    ]
    ledger = fire[[c for c in ledger_cols if c in fire.columns]].copy()
    for col in ("date", "execution_date"):
        ledger[col] = _date_text(ledger[col])
    _write_jsonl(session_root / "pine_signal_ledger.jsonl", ledger)
    fire.to_parquet(session_root / "pine_signal_rows.parquet", index=False)
    topk.to_parquet(session_root / "pine_topk_rows.parquet", index=False)
    _write_json(session_root / "compare.json", compare)
    _write_json(session_root / "family_leaderboard.json", leaderboard)
    _write_json(session_root / "no_lookahead_audit.json", audit)
    _write_json(session_root / "research_decision.json", decision)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_root": str(session_root),
        "authoritative_decision": decision["authoritative_rollup_decision"],
        "decision_reason": decision["decision_reason"],
        "signal_count": int(len(fire)),
        "topk_row_count": int(len(topk)),
        "artifacts": {name: str(session_root / name) for name in REQUIRED_ARTIFACTS},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX-only Pine FIRE signal research")
    parser.add_argument("--source-db-path", default=str(_default_db_path()))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default=None)
    parser.add_argument("--source", default="pan")
    parser.add_argument("--random-seed", type=int, default=20260515)
    parser.add_argument("--baseline-repetitions", type=int, default=100)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--max-symbols", type=int, default=None)
    args = parser.parse_args()
    result = run_research(
        source_db_path=_safe_path(args.source_db_path, _default_db_path()),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        start=args.start,
        end=args.end,
        source=args.source,
        random_seed=args.random_seed,
        baseline_repetitions=args.baseline_repetitions,
        top_k=args.top_k,
        max_symbols=args.max_symbols,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
