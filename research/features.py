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
    # --- v1/v2 既存特徴量 ---
    "ret1", "ret3", "ret5", "ret10", "ret20", "ret20_sum",
    "atr14", "body_wick_ratio", "gap_count20",
    "vol_ratio20", "vol_spike20",
    "dev_ma7", "dev_ma20", "dev_ma60",
    "slope_ma7_5", "slope_ma20_5", "slope_ma60_10",
    "cnt_above_ma7_20", "cnt_below_ma7_20",
    "cnt_above_ma20_20", "cnt_below_ma20_20",
    "dist_high20", "dist_low20", "breakout20", "breakdown20",
    "weekly_ma13_gap", "weekly_ma26_gap", "weekly_box_pos",
    "monthly_box_pos", "monthly_ma20_gap", "monthly_above_ma20",
    "ma_align_bull", "ma_align_bear", "ma_align_score",
    "vol_surge5", "vol_up_days3", "vol_spike_cum5",
    "pullback_from_high20", "pullback_rebound3",
    "bull_body_atr", "bear_body_atr",
    "rsi14", "rsi_overbought", "rsi_oversold",
    "macd_hist", "macd_cross_up",
    "ret5_rank20", "overheated25",
    # --- v3 追加: ローソク足パターン ---
    "cs_hammer",               # ハンマー（下影線2倍+小実体）
    "cs_shooting_star",        # シューティングスター（上影線2倍+小実体）
    "cs_engulf_bull",          # 陽線の包み足
    "cs_engulf_bear",          # 陰線の包み足
    "cs_doji",                 # 十字線（実体なし）
    "cs_inside_bar",           # インサイドバー（前日の範囲内）
    "cs_outside_bar",          # アウトサイドバー（前日の範囲を包む）
    "cs_pinbar_bull",          # ピンバー上昇
    "cs_pinbar_bear",          # ピンバー下落
    "cs_marubozu_bull",        # 陽線丸坊主
    "cs_marubozu_bear",        # 陰線丸坊主
    "cs_morning_star",         # モーニングスター（3本底打ち）
    "cs_evening_star",         # イブニングスター（3本天井）
    "cs_three_white_soldiers", # 三白兵（3連続陽線）
    "cs_three_black_crows",    # 三羽烏（3連続陰線）
    # --- v3 追加: MAシーケンス履歴 ---
    "ma_consec_above_ma7",     # MA7上連続日数
    "ma_consec_above_ma20",    # MA20上連続日数
    "ma_consec_above_ma60",    # MA60上連続日数
    "ma7_cross_up_days_ago",   # ゴールデンクロス（7/20）から経過日数
    "ma20_cross_up_days_ago",  # ゴールデンクロス（20/60）から経過日数
    "ma20_slope_accel",        # MA20傾きの加速度
    "price_range_ratio",       # 10日値幅/30日値幅（収縮検知）
    "consec_up_days",          # 連続上昇日数
    "consec_dn_days",          # 連続下落日数
    # --- v3 追加: 相場レジーム ---
    "market_breadth_ma20",     # 市場全体のMA20上比率
    "market_breadth_52wk",     # 52週高値銘柄比率
    "market_ret20_rank",       # 市場20日リターンのパーセンタイル
    "vol_regime",              # ATRパーセンタイルによるボラレジーム(0/1/2)
    "market_trend_state",      # 市場MAトレンド状態(0/1/2)
    # --- v3 追加: クロスセクション強度 ---
    "cs_ret5_vs_market",       # 5日リターン vs 市場平均差分
    "cs_ret20_sector_pct",     # セクター内20日リターンパーセンタイル
    "cs_rel_strength_52w",     # 52週相対強度 vs 市場
    "cs_vol_rank_sector",      # セクター内出来高ランク
    # --- v3 追加: ボラティリティ構造 ---
    "rv5",                     # 5日間実現ボラ
    "rv20",                    # 20日間実現ボラ
    "rv_ratio",                # rv5/rv20（ボラ拡縮）
    "atr_pct_rank60",          # ATR60日パーセンタイルランク
    "upper_shadow_ratio",      # 上ヒゲ/レンジ比（天井抵抗）
    "lower_shadow_ratio",      # 下ヒゲ/レンジ比（下値サポート）
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

    # ---- v2 新特徴量: MAパーフェクトオーダー ----
    ma7_valid = source["ma7"].notna() & (source["ma7"] > 0)
    ma20_valid = source["ma20"].notna() & (source["ma20"] > 0)
    ma60_valid = source["ma60"].notna() & (source["ma60"] > 0)
    ma_all_valid = ma7_valid & ma20_valid & ma60_valid
    source["ma_align_bull"] = np.where(
        ma_all_valid,
        ((source["ma7"] > source["ma20"]) & (source["ma20"] > source["ma60"])).astype(float),
        np.nan,
    )
    source["ma_align_bear"] = np.where(
        ma_all_valid,
        ((source["ma7"] < source["ma20"]) & (source["ma20"] < source["ma60"])).astype(float),
        np.nan,
    )
    source["ma_align_score"] = np.where(
        ma_all_valid,
        (
            source["ma7"] / (source["ma20"] + 1e-9)
            + source["ma20"] / (source["ma60"] + 1e-9)
        ) / 2.0 - 1.0,
        np.nan,
    )

    # ---- v2 新特徴量: 出来高急増 ----
    vol_ma5 = g["volume"].transform(lambda s: s.rolling(5, min_periods=2).max())
    source["vol_surge5"] = vol_ma5 / (source["vol_ma20"] + 1e-9)
    source["_vol_up"] = (source["volume"] > g["volume"].shift(1)).astype(float)
    source["vol_up_days3"] = g["_vol_up"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    source["vol_spike_cum5"] = g["vol_ratio20"].transform(lambda s: s.rolling(5, min_periods=2).sum())

    # ---- v2 新特徴量: 押し目 ----
    source["pullback_from_high20"] = np.where(
        source["high20"] > 0,
        (source["high20"] - source["close"]) / (source["high20"] + 1e-9),
        np.nan,
    )
    source["pullback_rebound3"] = source["close"] / (g["close"].shift(3) + 1e-9) - 1.0

    # ---- v2 新特徴量: 大陽線/大陰線 ----
    bull_body = (source["close"] - source["open"]).clip(lower=0.0)
    bear_body = (source["open"] - source["close"]).clip(lower=0.0)
    atr14_abs = source["atr14"] * source["close"]  # atr14は既にclose比率なので戻す
    source["bull_body_atr"] = bull_body / (source["atr14"] * source["close"] + 1e-9)
    source["bear_body_atr"] = bear_body / (source["atr14"] * source["close"] + 1e-9)

    # ---- v2 新特徴量: RSI14 ----
    def _compute_rsi14(s: pd.Series) -> pd.Series:
        delta = s.diff()
        gain = delta.clip(lower=0.0).rolling(14, min_periods=6).mean()
        loss = (-delta).clip(lower=0.0).rolling(14, min_periods=6).mean()
        rs = gain / (loss + 1e-9)
        rsi = 100.0 - 100.0 / (1.0 + rs)
        return rsi / 100.0  # normalize to 0-1
    source["rsi14"] = g["close"].transform(_compute_rsi14)
    source["rsi_overbought"] = (source["rsi14"] > 0.70).astype(float)
    source["rsi_oversold"] = (source["rsi14"] < 0.30).astype(float)

    # ---- v2 新特徴量: MACD ----
    def _ema(s: pd.Series, span: int) -> pd.Series:
        return s.ewm(span=span, min_periods=max(4, span // 3), adjust=False).mean()
    def _compute_macd_hist(s: pd.Series) -> pd.Series:
        ema12 = _ema(s, 12)
        ema26 = _ema(s, 26)
        macd_line = ema12 - ema26
        signal = _ema(macd_line, 9)
        hist = macd_line - signal
        return hist
    macd_hist_raw = g["close"].transform(_compute_macd_hist)
    source["macd_hist"] = macd_hist_raw / (source["close"] + 1e-9)
    source["_macd_hist_raw"] = macd_hist_raw
    source["macd_cross_up"] = (
        (source["_macd_hist_raw"] > 0) & (g["_macd_hist_raw"].shift(1) <= 0)
    ).astype(float)

    # ---- v2 新特徴量: モメンタムランク ----
    # 各日の過去20日内での5日リターンのパーセンタイルランク
    def _rolling_pct_rank(s: pd.Series) -> pd.Series:
        def _rank_last(window: np.ndarray) -> float:
            if len(window) == 0 or np.isnan(window[-1]):
                return np.nan
            valid = window[~np.isnan(window)]
            if len(valid) == 0:
                return np.nan
            return float(np.sum(valid <= window[-1]) / len(valid))
        return s.rolling(20, min_periods=5).apply(_rank_last, raw=True)
    source["ret5_rank20"] = g["ret5"].transform(_rolling_pct_rank)

    # ---- v2 新特徴量: 過熱検知 ----
    source["overheated25"] = (source["dev_ma60"] > 0.25).astype(float)


    # ================================================================
    # v3_deep 新特徴量: ローソク足パターン
    # ================================================================
    _body = source["close"] - source["open"]             # 実体（+陽、-陰）
    _body_abs = _body.abs()
    _range = (source["high"] - source["low"]).clip(lower=1e-9)
    _upper_wick = source["high"] - source[["close", "open"]].max(axis=1)
    _lower_wick = source[["close", "open"]].min(axis=1) - source["low"]
    _atr_abs = (source["atr14"] * source["close"]).clip(lower=1e-9)

    # ハンマー: 下影線が実体の2倍以上、上影線が短い、実体が小さい
    source["cs_hammer"] = (
        (_lower_wick >= 2.0 * _body_abs) &
        (_upper_wick <= 0.3 * _body_abs) &
        (_body_abs < _atr_abs * 0.5)
    ).astype(float)

    # シューティングスター: 上影線が実体の2倍以上、下影線が短い
    source["cs_shooting_star"] = (
        (_upper_wick >= 2.0 * _body_abs) &
        (_lower_wick <= 0.3 * _body_abs) &
        (_body_abs < _atr_abs * 0.5)
    ).astype(float)

    # 陽線・陰線の包み足
    _prev_body = g["close"].shift(1) - g["open"].shift(1)
    _prev_hi = g["high"].shift(1)
    _prev_lo = g["low"].shift(1)
    source["cs_engulf_bull"] = (
        (_body > 0) &                       # 今日は陽線
        (_prev_body < 0) &                  # 前日は陰線
        (source["close"] > _prev_hi) &      # 今日終値 > 前日高値
        (source["open"] < g["low"].shift(1))# 今日始値 < 前日安値
    ).astype(float)
    source["cs_engulf_bear"] = (
        (_body < 0) &
        (_prev_body > 0) &
        (source["close"] < _prev_lo) &
        (source["open"] > g["high"].shift(1))
    ).astype(float)

    # 十字線: 実体がATRの8%未満
    source["cs_doji"] = (_body_abs < _atr_abs * 0.08).astype(float)

    # インサイドバー: 高値・安値が前日の範囲内
    source["cs_inside_bar"] = (
        (source["high"] <= _prev_hi) & (source["low"] >= _prev_lo)
    ).astype(float)

    # アウトサイドバー: 高値・安値が前日の範囲を包む
    source["cs_outside_bar"] = (
        (source["high"] > _prev_hi) & (source["low"] < _prev_lo)
    ).astype(float)

    # ピンバー: ヒゲが実体の3倍以上で方向が明確
    source["cs_pinbar_bull"] = (
        (_lower_wick >= 3.0 * _body_abs) & (_lower_wick >= _range * 0.55)
    ).astype(float)
    source["cs_pinbar_bear"] = (
        (_upper_wick >= 3.0 * _body_abs) & (_upper_wick >= _range * 0.55)
    ).astype(float)

    # 丸坊主: ヒゲが短く実体が大きい
    _wick_total = _upper_wick + _lower_wick
    source["cs_marubozu_bull"] = (
        (_body > 0) & (_wick_total < _body_abs * 0.1) & (_body_abs > _atr_abs * 0.8)
    ).astype(float)
    source["cs_marubozu_bear"] = (
        (_body < 0) & (_wick_total < _body_abs * 0.1) & (_body_abs > _atr_abs * 0.8)
    ).astype(float)

    # モーニングスター: 陰線→小実体/ドジ→大陽線
    _body2ago = g["close"].shift(2) - g["open"].shift(2)
    _body_abs2ago = _body2ago.abs()
    source["cs_morning_star"] = (
        (_body2ago < -_atr_abs * 0.5) &
        ((g["close"].shift(1) - g["open"].shift(1)).abs() < _atr_abs * 0.3) &
        (_body > _atr_abs * 0.5)
    ).astype(float)

    # イブニングスター: 陽線→小実体→大陰線
    source["cs_evening_star"] = (
        (_body2ago > _atr_abs * 0.5) &
        ((g["close"].shift(1) - g["open"].shift(1)).abs() < _atr_abs * 0.3) &
        (_body < -_atr_abs * 0.5)
    ).astype(float)

    # 三白兵・三羽烏
    _is_bull = (_body > 0).astype(float)
    _is_bear = (_body < 0).astype(float)
    source["cs_three_white_soldiers"] = (
        (_is_bull == 1) &
        (g["_is_bull"].shift(1) == 1) &
        (g["_is_bull"].shift(2) == 1) &
        (source["close"] > g["close"].shift(1)) &
        (g["close"].shift(1) > g["close"].shift(2))
    ).astype(float) if "_is_bull" in source.columns else pd.Series(0.0, index=source.index)
    # シンプルな実装
    source["_is_bull_day"] = (_body > 0).astype(float)
    source["_is_bear_day"] = (_body < 0).astype(float)
    source["cs_three_white_soldiers"] = (
        (source["_is_bull_day"] == 1) &
        (g["_is_bull_day"].shift(1) == 1) &
        (g["_is_bull_day"].shift(2) == 1) &
        (source["close"] > g["close"].shift(1)) &
        (g["close"].shift(1) > g["close"].shift(2))
    ).astype(float)
    source["cs_three_black_crows"] = (
        (source["_is_bear_day"] == 1) &
        (g["_is_bear_day"].shift(1) == 1) &
        (g["_is_bear_day"].shift(2) == 1) &
        (source["close"] < g["close"].shift(1)) &
        (g["close"].shift(1) < g["close"].shift(2))
    ).astype(float)

    # ================================================================
    # v3_deep 新特徴量: MAシーケンス履歴
    # ================================================================
    def _streak(s: pd.Series, cond: pd.Series) -> pd.Series:
        """連続してcondがTrueの日数をカウント."""
        result = pd.Series(0.0, index=s.index)
        count = 0
        for idx in range(len(s)):
            if bool(cond.iloc[idx]):
                count += 1
            else:
                count = 0
            result.iloc[idx] = count
        return result

    # 各銘柄ごとに計算 — source列として保存してからgroupby参照
    source["_above_ma7_b"] = (source["close"] > source["ma7"]).astype(int)
    source["_above_ma20_b"] = (source["close"] > source["ma20"]).astype(int)
    source["_above_ma60_b"] = (source["close"] > source["ma60"]).astype(int)

    def _consec_true(s: pd.Series) -> pd.Series:
        """連続してTrue(1)の日数. False(0)でリセット."""
        group_ids = (s != s.shift()).cumsum()
        counts = s.groupby(group_ids).cumcount() + 1
        return (counts * s).astype(float)

    source["ma_consec_above_ma7"] = g["_above_ma7_b"].transform(_consec_true)
    source["ma_consec_above_ma20"] = g["_above_ma20_b"].transform(_consec_true)
    source["ma_consec_above_ma60"] = g["_above_ma60_b"].transform(_consec_true)

    # ゴールデンクロス経過日数（MA7>MA20 が初めてTrueになってから）
    def _days_since_cross_up(ma_fast: pd.Series, ma_slow: pd.Series) -> pd.Series:
        """MA_fast > MA_slow が True に転じてから何日経過 (クロスが無ければ -1)."""
        cross_up = (ma_fast > ma_slow) & (ma_fast.shift(1) <= ma_slow.shift(1))
        days = pd.Series(-1.0, index=ma_fast.index)
        last_cross = -1
        for i in range(len(ma_fast)):
            if bool(cross_up.iloc[i]):
                last_cross = i
            if last_cross >= 0 and bool(ma_fast.iloc[i] > ma_slow.iloc[i]):
                days.iloc[i] = i - last_cross
        return days

    source["ma7_cross_up_days_ago"] = g.apply(
        lambda df: _days_since_cross_up(df["ma7"], df["ma20"])
    ).reset_index(level=0, drop=True)
    source["ma20_cross_up_days_ago"] = g.apply(
        lambda df: _days_since_cross_up(df["ma20"], df["ma60"])
    ).reset_index(level=0, drop=True)

    # MA20傾きの加速度 (今週の傾き - 先週の傾き)
    source["_ma20_slope_now"] = source["ma20"] / (g["ma20"].shift(5) + 1e-9) - 1.0
    source["_ma20_slope_prev"] = g["ma20"].shift(5) / (g["ma20"].shift(10) + 1e-9) - 1.0
    source["ma20_slope_accel"] = source["_ma20_slope_now"] - source["_ma20_slope_prev"]

    # 値幅収縮: 10日値幅 / 30日値幅
    _range10 = g["high"].transform(lambda s: s.rolling(10, min_periods=3).max()) -                g["low"].transform(lambda s: s.rolling(10, min_periods=3).min())
    _range30 = g["high"].transform(lambda s: s.rolling(30, min_periods=6).max()) -                g["low"].transform(lambda s: s.rolling(30, min_periods=6).min())
    source["price_range_ratio"] = _range10 / (_range30 + 1e-9)

    # 連続上昇/下落日数
    source["_up_day"] = (source["close"] > source["close_prev1"]).astype(int)
    source["_dn_day"] = (source["close"] < source["close_prev1"]).astype(int)
    source["consec_up_days"] = g["_up_day"].transform(
        lambda s: s.groupby((s != s.shift()).cumsum()).cumcount() + 1
    ) * source["_up_day"]
    source["consec_dn_days"] = g["_dn_day"].transform(
        lambda s: s.groupby((s != s.shift()).cumsum()).cumcount() + 1
    ) * source["_dn_day"]

    # ================================================================
    # v3_deep 新特徴量: 相場レジーム (市場全体特徴量)
    # ================================================================
    # 市場全体のMA20上比率 (全銘柄の _above_ma20 の日次平均)
    _above_ma20_float = (source["close"] > source["ma20"]).astype(float)
    source["_above_ma20_f"] = _above_ma20_float
    _market_breadth_by_date = source.groupby("date")["_above_ma20_f"].transform("mean")
    source["market_breadth_ma20"] = _market_breadth_by_date

    # 52週高値比率
    _52wk_high = g["high"].transform(lambda s: s.rolling(252, min_periods=50).max())
    _at_52wk = (source["close"] >= _52wk_high * 0.98).astype(float)
    source["_at_52wk"] = _at_52wk
    source["market_breadth_52wk"] = source.groupby("date")["_at_52wk"].transform("mean")

    # 市場20日リターンのパーセンタイルランク（全銘柄平均ret20）
    _market_ret20_mean = source.groupby("date")["ret20"].transform("mean")
    source["_mrm20"] = _market_ret20_mean
    def _pct_rank_series(s: pd.Series) -> pd.Series:
        return s.rolling(120, min_periods=20).apply(
            lambda w: float(np.sum(w <= w[-1]) / len(w)), raw=True
        )
    # 日次集計後にパーセンタイル計算（銘柄別ではなく日次ユニーク値）
    _daily_mret = source[["date", "_mrm20"]].drop_duplicates("date").set_index("date")["_mrm20"].sort_index()
    _daily_mret_rank = _daily_mret.rolling(120, min_periods=20).apply(
        lambda w: float(np.sum(w <= w[-1]) / len(w)) if len(w) > 0 else 0.5, raw=True
    )
    source["market_ret20_rank"] = source["date"].map(_daily_mret_rank)

    # ボラティリティレジーム: ATR14の60日パーセンタイルで3段階
    _atr_pct_fn = lambda s: s.rolling(60, min_periods=10).apply(
        lambda w: float(np.sum(w <= w[-1]) / len(w)) if len(w) > 0 else 0.5, raw=True
    )
    _atr_pct60 = g["atr14"].transform(_atr_pct_fn)
    source["atr_pct_rank60"] = _atr_pct60
    source["vol_regime"] = pd.cut(
        _atr_pct60.fillna(0.5),
        bins=[-np.inf, 0.33, 0.67, np.inf],
        labels=[0, 1, 2]
    ).astype(float)

    # 市場トレンド状態 (市場全体のma_align_bull/bearから計算)
    _mkt_bull = source.groupby("date")["ma_align_bull"].transform("mean")
    _mkt_bear = source.groupby("date")["ma_align_bear"].transform("mean")
    source["market_trend_state"] = np.where(
        _mkt_bull > 0.5, 2.0,
        np.where(_mkt_bear > 0.5, 0.0, 1.0)
    )

    # ================================================================
    # v3_deep 新特徴量: クロスセクション強度
    # ================================================================
    # 5日リターン vs 市場平均
    _market_ret5_mean = source.groupby("date")["ret5"].transform("mean")
    source["cs_ret5_vs_market"] = source["ret5"] - _market_ret5_mean

    # セクター内20日リターンパーセンタイル
    def _sector_pct_rank(df: pd.DataFrame) -> pd.Series:
        return df["ret20"].rank(pct=True)
    source["cs_ret20_sector_pct"] = source.groupby(["date", "sector"], group_keys=False).apply(
        _sector_pct_rank
    ) if "sector" in source.columns else source.groupby("date")["ret20"].rank(pct=True)

    # 52週相対強度 (vs 市場平均)
    _ret252 = source["close"] / (g["close"].shift(252) + 1e-9) - 1.0
    _market_ret252 = source.groupby("date")["close"].transform(
        lambda s: s
    )  # placeholder
    _market_avg_ret252 = source.groupby("date")["ret20"].transform("mean") * 12.0  # proxy
    source["cs_rel_strength_52w"] = _ret252 - _market_avg_ret252

    # セクター内出来高ランク
    source["cs_vol_rank_sector"] = source.groupby("date")["vol_ratio20"].rank(pct=True)

    # ================================================================
    # v3_deep 新特徴量: ボラティリティ構造
    # ================================================================
    source["rv5"] = g["ret1"].transform(lambda s: s.rolling(5, min_periods=2).std()) * np.sqrt(5)
    source["rv20"] = g["ret1"].transform(lambda s: s.rolling(20, min_periods=5).std()) * np.sqrt(20)
    source["rv_ratio"] = source["rv5"] / (source["rv20"] + 1e-9)

    _total_range = (source["high"] - source["low"]).clip(lower=1e-9)
    _upper_hi = source["high"] - source[["close", "open"]].max(axis=1)
    _lower_lo = source[["close", "open"]].min(axis=1) - source["low"]
    source["upper_shadow_ratio"] = _upper_hi / _total_range
    source["lower_shadow_ratio"] = _lower_lo / _total_range

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
