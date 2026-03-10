from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from research.config import ResearchConfig
from research.study_storage import dataset_path, init_study_manifest, init_trial_state, load_dataset_meta, study_paths, update_study_manifest, write_dataset_meta
from research.storage import ResearchPaths, parse_date, read_csv, write_csv, ymd


@dataclass(frozen=True)
class TimeframeSpec:
    timeframe: str
    ma_fast: int
    ma_mid: int
    ma_slow: int
    atr_window: int
    breakout_windows: tuple[int, ...]
    pivot_windows: tuple[int, ...]


TIMEFRAME_SPECS: dict[str, TimeframeSpec] = {
    "daily": TimeframeSpec("daily", ma_fast=5, ma_mid=20, ma_slow=60, atr_window=14, breakout_windows=(10, 20, 40), pivot_windows=(3, 5, 7)),
    "weekly": TimeframeSpec("weekly", ma_fast=4, ma_mid=13, ma_slow=26, atr_window=8, breakout_windows=(4, 8, 12), pivot_windows=(2, 3, 4)),
    "monthly": TimeframeSpec("monthly", ma_fast=3, ma_mid=6, ma_slow=12, atr_window=6, breakout_windows=(3, 6, 12), pivot_windows=(1, 2, 3)),
}


def _fallback_industry(codes: pd.Series) -> pd.DataFrame:
    return (
        pd.DataFrame({"code": codes.astype(str).str.strip().drop_duplicates().tolist()})
        .query("code != ''")
        .assign(sector33_code="__NA__", sector33_name="UNCLASSIFIED")
        .sort_values("code")
        .reset_index(drop=True)
    )


def _load_snapshot(paths: ResearchPaths, snapshot_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sdir = paths.snapshot_dir(snapshot_id)
    if not sdir.exists():
        raise FileNotFoundError(f"snapshot not found: {sdir}")
    daily = read_csv(sdir / "daily.csv")
    universe = read_csv(sdir / "universe_monthly.csv")
    industry_path = sdir / "industry_master.csv"
    industry = read_csv(industry_path) if industry_path.exists() else _fallback_industry(daily["code"])

    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    universe["asof_date"] = pd.to_datetime(universe["asof_date"], errors="coerce").dt.normalize()
    daily = daily.dropna(subset=["date", "code", "open", "high", "low", "close"]).copy()
    universe = universe.dropna(subset=["asof_date", "code"]).copy()
    daily["code"] = daily["code"].astype(str).str.strip()
    universe["code"] = universe["code"].astype(str).str.strip()
    industry["code"] = industry["code"].astype(str).str.strip()
    sector_code = industry["sector33_code"] if "sector33_code" in industry.columns else pd.Series("__NA__", index=industry.index)
    sector_name = industry["sector33_name"] if "sector33_name" in industry.columns else pd.Series("UNCLASSIFIED", index=industry.index)
    industry["sector33_code"] = sector_code.astype(str).str.strip().replace("", "__NA__")
    industry["sector33_name"] = sector_name.astype(str).str.strip().replace("", "UNCLASSIFIED")
    industry = industry.drop_duplicates(subset=["code"], keep="last")
    return daily, universe, industry[["code", "sector33_code", "sector33_name"]].copy()


def _resample_bars(daily: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "daily":
        out = daily.rename(columns={"date": "event_date"}).copy()
        out["timeframe"] = timeframe
        return out[["event_date", "code", "open", "high", "low", "close", "volume", "timeframe"]].copy()

    rule = "W-FRI" if timeframe == "weekly" else "ME"
    parts: list[pd.DataFrame] = []
    for code, grp in daily.groupby("code", sort=False):
        tmp = grp.sort_values("date").set_index("date")
        tmp["trade_date"] = tmp.index
        agg = tmp.resample(rule).agg(
            {
                "trade_date": "last",
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        agg = agg.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True).rename(columns={"trade_date": "event_date"})
        agg["code"] = str(code)
        parts.append(agg)
    if not parts:
        return pd.DataFrame(columns=["event_date", "code", "open", "high", "low", "close", "volume", "timeframe"])
    out = pd.concat(parts, ignore_index=True)
    out["timeframe"] = timeframe
    return out[["event_date", "code", "open", "high", "low", "close", "volume", "timeframe"]].copy()


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    return vals.rolling(window, min_periods=max(3, window // 4)).apply(
        lambda w: float(np.sum(w <= w[-1]) / len(w)) if len(w) > 0 else 0.5,
        raw=True,
    )


def _bars_with_local_features(bars: pd.DataFrame, spec: TimeframeSpec) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for code, grp in bars.groupby("code", sort=False):
        tmp = grp.sort_values("event_date").copy()
        gclose = tmp["close"]
        ghigh = tmp["high"]
        glow = tmp["low"]
        gvol = pd.to_numeric(tmp["volume"], errors="coerce").fillna(0.0)
        prev_close = gclose.shift(1)

        tmp["ret1"] = gclose.pct_change()
        tmp["ret3"] = gclose.pct_change(3)
        tmp["ret6"] = gclose.pct_change(6)
        tr = pd.concat(
            [
                (ghigh - glow).abs(),
                (ghigh - prev_close).abs(),
                (glow - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        tmp["atr"] = tr.rolling(spec.atr_window, min_periods=max(2, spec.atr_window // 2)).mean()
        tmp["ma_fast"] = gclose.rolling(spec.ma_fast, min_periods=max(2, spec.ma_fast // 2)).mean()
        tmp["ma_mid"] = gclose.rolling(spec.ma_mid, min_periods=max(3, spec.ma_mid // 2)).mean()
        tmp["ma_slow"] = gclose.rolling(spec.ma_slow, min_periods=max(4, spec.ma_slow // 2)).mean()
        tmp["ma_align_score_local"] = np.where(
            (tmp["ma_fast"] > tmp["ma_mid"]) & (tmp["ma_mid"] > tmp["ma_slow"]),
            1.0,
            np.where(
                (tmp["ma_fast"] < tmp["ma_mid"]) & (tmp["ma_mid"] < tmp["ma_slow"]),
                -1.0,
                0.0,
            ),
        )
        tmp["ma_distance_support"] = np.where(
            gclose.abs() > 1e-9,
            (gclose - tmp["ma_mid"]) / gclose,
            0.0,
        )
        tmp["ma_slope_mid"] = np.where(
            tmp["ma_mid"].shift(3).abs() > 1e-9,
            (tmp["ma_mid"] / tmp["ma_mid"].shift(3)) - 1.0,
            0.0,
        )
        gap = np.where(gclose.abs() > 1e-9, (tmp["ma_fast"] - tmp["ma_mid"]) / gclose, 0.0)
        gap_series = pd.Series(gap, index=tmp.index, dtype=float).fillna(0.0)
        sign_change = gap_series.mul(gap_series.shift(1).fillna(0.0)).lt(0.0).astype(int)
        tmp["cross_structure_raw"] = (
            np.tanh(gap_series * 25.0)
            + 0.25 * np.tanh((gap_series - gap_series.shift(1).fillna(0.0)) * 50.0)
            - 0.10 * sign_change.rolling(6, min_periods=1).sum()
        )

        rng = (ghigh - glow).replace(0.0, np.nan)
        upper_shadow = ghigh - np.maximum(tmp["open"], gclose)
        lower_shadow = np.minimum(tmp["open"], gclose) - glow
        body_ratio = (gclose - tmp["open"]) / rng
        upper_shadow_ratio = upper_shadow / rng
        lower_shadow_ratio = lower_shadow / rng
        tmp["candle_bias_raw"] = (
            body_ratio.fillna(0.0)
            + 0.5 * (lower_shadow_ratio.fillna(0.0) - upper_shadow_ratio.fillna(0.0))
        )

        tmp["vol_ma20"] = gvol.rolling(20, min_periods=3).mean()
        tmp["vol_ratio20"] = np.where(tmp["vol_ma20"].abs() > 1e-9, gvol / tmp["vol_ma20"], 1.0)
        tmp["turnover"] = gclose * gvol
        tmp["liq_med20"] = tmp["turnover"].rolling(20, min_periods=3).median()
        tmp["volume_ma_combo"] = np.sign(tmp["ma_distance_support"].fillna(0.0)) * np.log1p(tmp["vol_ratio20"].clip(lower=0.0))
        tmp["atr_pct60"] = _rolling_percentile(tmp["atr"], 60)

        for window in spec.breakout_windows:
            high_ref = ghigh.shift(1).rolling(window, min_periods=max(2, window // 2)).max()
            low_ref = glow.shift(1).rolling(window, min_periods=max(2, window // 2)).min()
            breakout = np.where(high_ref.abs() > 1e-9, (gclose / high_ref) - 1.0, 0.0)
            breakdown = np.where(gclose.abs() > 1e-9, (low_ref / gclose) - 1.0, 0.0)
            short_range = (ghigh.rolling(max(2, window // 2), min_periods=2).max() - glow.rolling(max(2, window // 2), min_periods=2).min()).replace(0.0, np.nan)
            long_range = (ghigh.rolling(window, min_periods=max(2, window // 2)).max() - glow.rolling(window, min_periods=max(2, window // 2)).min()).replace(0.0, np.nan)
            compression = _safe_series_div(short_range, long_range).fillna(0.0)
            tmp[f"breakout_shape_w{window}"] = pd.Series(breakout, index=tmp.index).fillna(0.0) - pd.Series(breakdown, index=tmp.index).fillna(0.0) - compression
            tmp[f"volume_price_combo_w{window}"] = tmp[f"breakout_shape_w{window}"] * np.log1p(tmp["vol_ratio20"].clip(lower=0.0))

        for window in spec.pivot_windows:
            rolling_low = glow.rolling(window * 2 + 1, min_periods=max(3, window + 1)).min()
            rolling_high = ghigh.rolling(window * 2 + 1, min_periods=max(3, window + 1)).max()
            low_dist = np.where(gclose.abs() > 1e-9, (gclose - rolling_low) / gclose, 0.0)
            high_dist = np.where(gclose.abs() > 1e-9, (rolling_high - gclose) / gclose, 0.0)
            rebound = tmp["ret3"].fillna(0.0)
            double_bottom = np.exp(-np.abs(low_dist) * 30.0) * np.clip(rebound, -1.0, 1.0)
            double_top = np.exp(-np.abs(high_dist) * 30.0) * np.clip(-rebound, -1.0, 1.0)
            tmp[f"pivot_pattern_w{window}"] = double_bottom - double_top
            tmp[f"neckline_gap_w{window}"] = pd.Series(high_dist - low_dist, index=tmp.index).fillna(0.0)
            tmp[f"symmetry_score_w{window}"] = 1.0 - np.clip(np.abs(tmp["ret1"].fillna(0.0) - tmp["ret3"].fillna(0.0)), 0.0, 1.0)
            tmp[f"right_volume_delta_w{window}"] = np.tanh(tmp["vol_ratio20"].fillna(0.0) - tmp["vol_ratio20"].shift(window).fillna(0.0))
        parts.append(tmp)
    return pd.concat(parts, ignore_index=True) if parts else bars.copy()


def _safe_series_div(num: pd.Series, den: pd.Series) -> pd.Series:
    den_num = pd.to_numeric(den, errors="coerce").replace(0.0, np.nan)
    return pd.to_numeric(num, errors="coerce") / den_num


def _context_bias_from_bars(bars: pd.DataFrame, *, fast_window: int, mid_window: int, long_window: int) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for code, grp in bars.groupby("code", sort=False):
        tmp = grp.sort_values("event_date").copy()
        close = tmp["close"]
        ma_fast = close.rolling(fast_window, min_periods=max(2, fast_window // 2)).mean()
        ma_mid = close.rolling(mid_window, min_periods=max(2, mid_window // 2)).mean()
        ma_long = close.rolling(long_window, min_periods=max(3, long_window // 2)).mean()
        align = np.where((ma_fast > ma_mid) & (ma_mid > ma_long), 1.0, np.where((ma_fast < ma_mid) & (ma_mid < ma_long), -1.0, 0.0))
        slope = np.where(ma_mid.shift(2).abs() > 1e-9, (ma_mid / ma_mid.shift(2)) - 1.0, 0.0)
        box_hi = tmp["high"].rolling(long_window, min_periods=max(3, long_window // 2)).max()
        box_lo = tmp["low"].rolling(long_window, min_periods=max(3, long_window // 2)).min()
        box_pos = np.where((box_hi - box_lo).abs() > 1e-9, (close - box_lo) / (box_hi - box_lo), 0.5)
        tmp["context_bias"] = (
            pd.Series(align, index=tmp.index).fillna(0.0)
            + 0.5 * pd.Series(slope, index=tmp.index).fillna(0.0)
            + 0.5 * (pd.Series(box_pos, index=tmp.index).fillna(0.5) - 0.5)
        )
        parts.append(tmp[["event_date", "code", "context_bias"]])
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["event_date", "code", "context_bias"])


def _merge_context(base: pd.DataFrame, context: pd.DataFrame, target_col: str) -> pd.DataFrame:
    if context.empty:
        out = base.copy()
        out[target_col] = 0.0
        return out
    left = base.sort_values(["event_date", "code"]).copy()
    right = context.sort_values(["event_date", "code"]).copy()
    merged = pd.merge_asof(
        left,
        right,
        on="event_date",
        by="code",
        direction="backward",
        allow_exact_matches=True,
    )
    merged[target_col] = pd.to_numeric(merged.get("context_bias"), errors="coerce").fillna(0.0)
    return merged.drop(columns=["context_bias"], errors="ignore")


def _assign_universe(base: pd.DataFrame, universe: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if base.empty:
        return base.copy()
    dates = pd.DataFrame({"event_date": sorted(base["event_date"].dropna().unique().tolist())})
    dates = dates.sort_values("event_date").reset_index(drop=True)
    if timeframe == "monthly":
        dates["universe_asof_date"] = dates["event_date"]
    else:
        uni_dates = universe[["asof_date"]].drop_duplicates().sort_values("asof_date").reset_index(drop=True)
        mapped = pd.merge_asof(
            dates,
            uni_dates.rename(columns={"asof_date": "universe_asof_date"}).sort_values("universe_asof_date"),
            left_on="event_date",
            right_on="universe_asof_date",
            direction="backward",
            allow_exact_matches=True,
        )
        dates["universe_asof_date"] = mapped["universe_asof_date"]
    merged = base.merge(dates, on="event_date", how="left")
    merged = merged.merge(
        universe.rename(columns={"asof_date": "universe_asof_date"}),
        on=["code", "universe_asof_date"],
        how="inner",
    )
    return merged


def _assign_regime_and_clusters(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["month_bucket"] = pd.to_datetime(out["event_date"], errors="coerce").dt.strftime("%Y-%m")
    out["market_align_mean"] = out.groupby("event_date")["ma_align_score_local"].transform("mean").fillna(0.0)
    out["market_ret_mean"] = out.groupby("event_date")["ret1"].transform("mean").fillna(0.0)
    out["market_vol_rank"] = out.groupby("event_date")["atr_pct60"].transform("mean").fillna(0.5)
    out["regime_bias"] = (
        0.5 * out["market_align_mean"].fillna(0.0)
        + 0.3 * out["market_ret_mean"].fillna(0.0)
        - 0.2 * (out["market_vol_rank"].fillna(0.5) - 0.5)
    )
    trend_state = np.where(out["market_align_mean"] > 0.25, 2, np.where(out["market_align_mean"] < -0.25, 0, 1))
    vol_state = np.where(out["market_vol_rank"] > 0.67, 2, np.where(out["market_vol_rank"] < 0.33, 0, 1))
    out["regime_key"] = pd.Series([f"mt{int(mt)}_vr{int(vr)}" for mt, vr in zip(trend_state, vol_state)], index=out.index)
    out["vol_bin"] = pd.cut(out["atr_pct60"].fillna(0.5), bins=[-np.inf, 0.33, 0.67, np.inf], labels=["0", "1", "2"]).astype(str)
    liq_pct = out.groupby("event_date")["liq_med20"].rank(pct=True)
    out["liquidity_bin"] = pd.cut(liq_pct.fillna(0.5), bins=[-np.inf, 0.33, 0.67, np.inf], labels=["0", "1", "2"]).astype(str)
    out["price_band"] = np.where(out["close"] < 300.0, "low", np.where(out["close"] < 1000.0, "mid", "high"))
    rv_ratio = np.where(out["atr"].rolling(20, min_periods=3).mean().abs() > 1e-9, out["atr"] / out["atr"].rolling(20, min_periods=3).mean(), 1.0)
    out["move_type"] = np.where(
        np.abs(out["ma_slope_mid"].fillna(0.0)) > 0.03,
        "trend",
        np.where(pd.Series(rv_ratio, index=out.index).fillna(1.0) > 1.5, "spike", "range"),
    )
    out["cluster_key"] = (
        out["sector33_code"].fillna("__NA__").astype(str)
        + "|v" + out["vol_bin"].astype(str)
        + "|l" + out["liquidity_bin"].astype(str)
        + "|p" + out["price_band"].astype(str)
        + "|m" + out["move_type"].astype(str)
    )
    return out


def _future_metrics_for_code(frame: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    out = frame.sort_values("event_date").copy()
    close = out["close"].to_numpy(dtype=float)
    high = out["high"].to_numpy(dtype=float)
    low = out["low"].to_numpy(dtype=float)
    n = len(out)

    for horizon in horizons:
        ret = np.full(n, np.nan, dtype=float)
        mfe = np.full(n, np.nan, dtype=float)
        mae = np.full(n, np.nan, dtype=float)
        up_hit = np.full(n, np.nan, dtype=float)
        down_hit = np.full(n, np.nan, dtype=float)
        early_neg_up = np.full(n, np.nan, dtype=float)
        early_neg_down = np.full(n, np.nan, dtype=float)
        window_pnl = np.full(n, np.nan, dtype=float)
        if n > horizon:
            future_close = close[horizon:]
            ret[: n - horizon] = (future_close / close[: n - horizon]) - 1.0
            window_pnl[: n - horizon] = ret[: n - horizon]

            high_windows = sliding_window_view(high[1:], horizon)
            low_windows = sliding_window_view(low[1:], horizon)
            mfe[: n - horizon] = (high_windows.max(axis=1) / close[: n - horizon]) - 1.0
            mae[: n - horizon] = (close[: n - horizon] - low_windows.min(axis=1)) / close[: n - horizon]
            up_hit[: n - horizon] = (ret[: n - horizon] > 0.0).astype(float)
            down_hit[: n - horizon] = (ret[: n - horizon] < 0.0).astype(float)

            early_h = max(1, horizon // 2)
            early_high_windows = sliding_window_view(high[1:], early_h)
            early_low_windows = sliding_window_view(low[1:], early_h)
            early_neg_up[: n - early_h] = ((early_low_windows.min(axis=1) / close[: n - early_h]) - 1.0 <= -0.02).astype(float)
            early_neg_down[: n - early_h] = ((early_high_windows.max(axis=1) / close[: n - early_h]) - 1.0 >= 0.02).astype(float)
        out[f"ret_h{horizon}"] = ret
        out[f"mfe_h{horizon}"] = mfe
        out[f"mae_h{horizon}"] = mae
        out[f"up_hit_h{horizon}"] = up_hit
        out[f"down_hit_h{horizon}"] = down_hit
        out[f"early_neg_up_h{horizon}"] = early_neg_up
        out[f"early_neg_down_h{horizon}"] = early_neg_down
        out[f"window_pnl_h{horizon}"] = window_pnl
    return out


def _assign_future_metrics(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    horizons = (5, 10, 20) if timeframe == "daily" else (4, 8, 12) if timeframe == "weekly" else (1, 3, 6)
    parts = [_future_metrics_for_code(grp, horizons) for _, grp in frame.groupby("code", sort=False)]
    return pd.concat(parts, ignore_index=True) if parts else frame.copy()


def build_study_dataset(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    timeframe: str,
    start_date: str,
    end_date: str,
    *,
    study_id: str | None = None,
) -> dict[str, Any]:
    if timeframe not in TIMEFRAME_SPECS:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    start_ts = parse_date(start_date)
    end_ts = parse_date(end_date)
    if end_ts < start_ts:
        raise ValueError("end_date must be >= start_date")

    manifest = init_study_manifest(
        paths=paths,
        config=config,
        snapshot_id=snapshot_id,
        study_id=study_id,
        timeframes=[timeframe],
        families=config.study.families,
        resume=config.study.resume,
    )
    resolved_study_id = str(manifest["study_id"])
    manifest_timeframes = sorted(set([str(x) for x in manifest.get("timeframes", [])] + [timeframe]))
    update_study_manifest(paths, resolved_study_id, {"timeframes": manifest_timeframes})
    init_trial_state(paths, resolved_study_id, timeframes=config.study.timeframes, families=config.study.families)

    daily, universe, industry = _load_snapshot(paths, snapshot_id)
    spec = TIMEFRAME_SPECS[timeframe]
    bars = _resample_bars(daily, timeframe)
    bars = bars[(bars["event_date"] >= start_ts) & (bars["event_date"] <= end_ts)].copy()
    if bars.empty:
        raise ValueError(f"no bars in range for timeframe={timeframe}")

    local = _bars_with_local_features(bars, spec)
    weekly_bars = _bars_with_local_features(_resample_bars(daily, "weekly"), TIMEFRAME_SPECS["weekly"])
    monthly_bars = _bars_with_local_features(_resample_bars(daily, "monthly"), TIMEFRAME_SPECS["monthly"])

    if timeframe == "daily":
        weekly_ctx = _context_bias_from_bars(weekly_bars, fast_window=4, mid_window=13, long_window=26)
        monthly_ctx = _context_bias_from_bars(monthly_bars, fast_window=3, mid_window=6, long_window=12)
        local = _merge_context(local, weekly_ctx, "weekly_context_bias")
        local = _merge_context(local, monthly_ctx, "monthly_context_bias")
    elif timeframe == "weekly":
        monthly_ctx = _context_bias_from_bars(monthly_bars, fast_window=3, mid_window=6, long_window=12)
        local["weekly_context_bias"] = local["ma_align_score_local"].fillna(0.0)
        local = _merge_context(local, monthly_ctx, "monthly_context_bias")
    else:
        long12 = _context_bias_from_bars(monthly_bars, fast_window=3, mid_window=6, long_window=12)
        long24 = _context_bias_from_bars(monthly_bars, fast_window=6, mid_window=12, long_window=24)
        local = _merge_context(local, long12, "weekly_context_bias")
        local = _merge_context(local, long24, "monthly_context_bias")

    local = _assign_universe(local, universe, timeframe)
    local = local.merge(industry, on="code", how="left")
    local["sector33_code"] = local["sector33_code"].fillna("__NA__")
    local["sector33_name"] = local["sector33_name"].fillna("UNCLASSIFIED")
    local = _assign_regime_and_clusters(local)
    local = _assign_future_metrics(local, timeframe)

    local["event_date"] = pd.to_datetime(local["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    local["universe_asof_date"] = pd.to_datetime(local["universe_asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    local["snapshot_id"] = snapshot_id
    local["timeframe"] = timeframe
    out_file = dataset_path(paths, resolved_study_id, timeframe)
    write_csv(out_file, local.sort_values(["event_date", "code"]).reset_index(drop=True))

    meta_path = study_paths(paths, resolved_study_id)["dataset_meta"]
    meta = load_dataset_meta(paths, resolved_study_id) if meta_path.exists() else {
        "study_id": resolved_study_id,
        "snapshot_id": snapshot_id,
        "datasets": {},
    }
    meta.setdefault("datasets", {})
    meta["datasets"][timeframe] = {
        "path": str(out_file),
        "rows": int(len(local)),
        "start_date": ymd(start_ts),
        "end_date": ymd(end_ts),
        "horizons": list((5, 10, 20) if timeframe == "daily" else (4, 8, 12) if timeframe == "weekly" else (1, 3, 6)),
        "columns": list(local.columns),
    }
    write_dataset_meta(paths, resolved_study_id, meta)
    return {
        "ok": True,
        "study_id": resolved_study_id,
        "snapshot_id": snapshot_id,
        "timeframe": timeframe,
        "rows": int(len(local)),
        "dataset_path": str(out_file),
    }
