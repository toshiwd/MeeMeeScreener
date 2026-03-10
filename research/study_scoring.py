from __future__ import annotations

from dataclasses import dataclass
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.config import ResearchConfig
from research.study_search_space import FAMILY_SPECS
from research.study_storage import fold_artifact_dir
from research.storage import ResearchPaths, write_json


def horizons_for_timeframe(timeframe: str) -> tuple[int, ...]:
    if timeframe == "daily":
        return (5, 10, 20)
    if timeframe == "weekly":
        return (4, 8, 12)
    return (1, 3, 6)


def _sorted_months(frame: pd.DataFrame) -> list[str]:
    months = frame["month_bucket"].astype(str).dropna().unique().tolist()
    return sorted(months)


def build_walkforward_windows(frame: pd.DataFrame, config: ResearchConfig) -> list[dict[str, list[str]]]:
    months = _sorted_months(frame)
    train_n = max(1, int(config.split.train_years) * 12)
    valid_n = max(1, int(config.split.valid_months))
    test_n = max(1, int(config.split.test_months))
    required = train_n + valid_n + test_n
    if len(months) < required:
        return []
    windows: list[dict[str, list[str]]] = []
    step = test_n
    for start in range(0, len(months) - required + 1, step):
        train_months = months[start : start + train_n]
        valid_months = months[start + train_n : start + train_n + valid_n]
        test_months = months[start + train_n + valid_n : start + required]
        windows.append(
            {
                "train_months": train_months,
                "valid_months": valid_months,
                "test_months": test_months,
            }
        )
    return windows


def _sorted_array(values: pd.Series) -> np.ndarray:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    if arr.size == 0:
        return np.asarray([0.0], dtype=float)
    arr.sort()
    return arr


def _percentile_score(values: pd.Series, sorted_ref: np.ndarray) -> pd.Series:
    ref = np.asarray(sorted_ref, dtype=float)
    if ref.size == 0:
        ref = np.asarray([0.0], dtype=float)
    vals = pd.to_numeric(values, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    pos = np.searchsorted(ref, vals, side="right") / float(ref.size)
    return pd.Series((2.0 * pos) - 1.0, index=values.index, dtype=float)


def _safe_div(num: float, den: float, default: float = 0.0) -> float:
    if abs(den) <= 1e-12:
        return float(default)
    return float(num / den)


def _max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    peak = equity.cummax()
    dd = (equity / peak) - 1.0
    return float(max(0.0, -dd.min()))


def _profit_factor(returns: pd.Series) -> float:
    vals = pd.to_numeric(returns, errors="coerce").dropna()
    if vals.empty:
        return 0.0
    pos = float(vals[vals > 0.0].sum())
    neg = float(-vals[vals < 0.0].sum())
    if neg <= 1e-12:
        return 999.0 if pos > 0.0 else 0.0
    return float(pos / neg)


def _stability(selected: pd.DataFrame, direction: int, horizons: tuple[int, ...]) -> float:
    horizon_means: list[float] = []
    for horizon in horizons:
        col = f"window_pnl_h{horizon}"
        if col not in selected.columns:
            continue
        vals = pd.to_numeric(selected[col], errors="coerce").dropna()
        if vals.empty:
            continue
        horizon_means.append(float(direction) * float(vals.mean()))
    if not horizon_means:
        return 0.0
    arr = np.asarray(horizon_means, dtype=float)
    pos_ratio = float(np.mean(arr > 0.0))
    dispersion = float(np.std(arr))
    scale = float(np.mean(np.abs(arr))) + 1e-6
    return float(np.clip(pos_ratio * (1.0 / (1.0 + (dispersion / scale))), 0.0, 1.0))


def _cluster_consistency(selected: pd.DataFrame, direction: int) -> float:
    if selected.empty or "cluster_key" not in selected.columns:
        return 0.0
    work = selected.copy()
    work["signed_primary_return"] = pd.to_numeric(work["primary_return"], errors="coerce").fillna(0.0) * float(direction)
    grouped = work.groupby("cluster_key", dropna=False)["signed_primary_return"].agg(["mean", "count"]).reset_index()
    grouped = grouped[grouped["count"] >= 3]
    if grouped.empty:
        return 0.0
    return float(np.mean(grouped["mean"] > 0.0))


def _primary_signed_return(frame: pd.DataFrame, direction: int, horizons: tuple[int, ...]) -> pd.Series:
    cols = [f"window_pnl_h{horizon}" for horizon in horizons if f"window_pnl_h{horizon}" in frame.columns]
    if not cols:
        return pd.Series(0.0, index=frame.index, dtype=float)
    arr = np.column_stack(
        [
            pd.to_numeric(frame[col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
            for col in cols
        ]
    )
    return pd.Series(float(direction) * arr.mean(axis=1), index=frame.index, dtype=float)


def _cluster_prior(train_frame: pd.DataFrame, direction: int, horizons: tuple[int, ...]) -> pd.DataFrame:
    if train_frame.empty or "cluster_key" not in train_frame.columns:
        return pd.DataFrame(columns=["cluster_key", "cluster_prior"])
    work = train_frame[["cluster_key"]].copy()
    work["signed_primary_return"] = _primary_signed_return(train_frame, direction=direction, horizons=horizons)
    grouped = work.groupby("cluster_key", dropna=False)["signed_primary_return"].agg(["mean", "count"]).reset_index()
    if grouped.empty:
        return pd.DataFrame(columns=["cluster_key", "cluster_prior"])
    global_mean = float(work["signed_primary_return"].mean())
    global_std = float(work["signed_primary_return"].std()) if work["signed_primary_return"].std() > 1e-9 else 1.0
    shrink = 5.0
    grouped["cluster_prior"] = (
        ((grouped["mean"] - global_mean) / global_std)
        * (grouped["count"] / (grouped["count"] + shrink))
    )
    return grouped[["cluster_key", "cluster_prior"]].copy()


def _group_raws(
    frame: pd.DataFrame,
    timeframe: str,
    family: str,
    params: dict[str, Any],
    cluster_prior: pd.DataFrame,
) -> pd.DataFrame:
    spec = FAMILY_SPECS[family]
    direction = float(spec.direction)
    out = frame.copy()
    pivot_window = int(params["pivot_window"])
    breakout_window = int(params["breakout_window"])

    out = out.merge(cluster_prior, on="cluster_key", how="left")
    out["cluster_prior"] = pd.to_numeric(out.get("cluster_prior"), errors="coerce").fillna(0.0)

    out["Candle"] = direction * pd.to_numeric(out.get("candle_bias_raw"), errors="coerce").fillna(0.0)
    out["MA"] = direction * (
        0.45 * pd.to_numeric(out.get("ma_align_score_local"), errors="coerce").fillna(0.0)
        + 0.20 * pd.to_numeric(out.get("ma_distance_support"), errors="coerce").fillna(0.0)
        + 0.15 * pd.to_numeric(out.get("ma_slope_mid"), errors="coerce").fillna(0.0)
        + 0.20 * pd.to_numeric(out.get("cross_structure_raw"), errors="coerce").fillna(0.0)
    )
    out["Volume"] = direction * (
        0.60 * pd.to_numeric(out.get(f"volume_price_combo_w{breakout_window}"), errors="coerce").fillna(0.0)
        + 0.40 * pd.to_numeric(out.get("volume_ma_combo"), errors="coerce").fillna(0.0)
    )
    out["WeeklyContext"] = direction * pd.to_numeric(out.get("weekly_context_bias"), errors="coerce").fillna(0.0)
    out["MonthlyContext"] = direction * pd.to_numeric(out.get("monthly_context_bias"), errors="coerce").fillna(0.0)
    out["Regime"] = direction * pd.to_numeric(out.get("regime_bias"), errors="coerce").fillna(0.0)
    out["Cluster"] = pd.to_numeric(out.get("cluster_prior"), errors="coerce").fillna(0.0)
    out["Pivot"] = direction * (
        0.45 * pd.to_numeric(out.get(f"pivot_pattern_w{pivot_window}"), errors="coerce").fillna(0.0)
        + 0.20 * pd.to_numeric(out.get(f"neckline_gap_w{pivot_window}"), errors="coerce").fillna(0.0)
        + 0.20 * pd.to_numeric(out.get(f"symmetry_score_w{pivot_window}"), errors="coerce").fillna(0.0)
        + 0.15 * pd.to_numeric(out.get(f"right_volume_delta_w{pivot_window}"), errors="coerce").fillna(0.0)
    )
    out["BreakoutShape"] = direction * pd.to_numeric(
        out.get(f"breakout_shape_w{breakout_window}"), errors="coerce"
    ).fillna(0.0)
    out["neg_trend_conflict"] = (
        (pd.to_numeric(out.get("weekly_context_bias"), errors="coerce").fillna(0.0) * direction) < 0.0
    ).astype(float)
    out["neg_volume_dry"] = (
        pd.to_numeric(out.get("vol_ratio20"), errors="coerce").fillna(0.0) < float(params["volume_threshold"])
    ).astype(float)
    out["neg_context_conflict"] = (
        (pd.to_numeric(out.get("monthly_context_bias"), errors="coerce").fillna(0.0) * direction) < 0.0
    ).astype(float)
    out["neg_breakout_fail"] = (
        (pd.to_numeric(out.get(f"breakout_shape_w{breakout_window}"), errors="coerce").fillna(0.0) * direction) < 0.0
    ).astype(float)
    return out


def _normalize_groups(work: pd.DataFrame, train_idx: pd.Index, allowed_groups: tuple[str, ...]) -> tuple[pd.DataFrame, dict[str, list[float]]]:
    normalizers: dict[str, list[float]] = {}
    out = work.copy()
    for group in allowed_groups:
        ref = _sorted_array(out.loc[train_idx, group])
        normalizers[group] = ref.tolist()
        out[group] = _percentile_score(out[group], ref)
    return out, normalizers


def _score_rows(frame: pd.DataFrame, allowed_groups: tuple[str, ...], params: dict[str, Any]) -> pd.Series:
    score = pd.Series(0.0, index=frame.index, dtype=float)
    weights = params.get("weights", {})
    for group in allowed_groups:
        score = score + float(weights.get(group, 0.0)) * pd.to_numeric(frame.get(group), errors="coerce").fillna(0.0)
    penalties = params.get("neg_penalties", {})
    score = score - float(penalties.get("trend_conflict", 0.0)) * frame["neg_trend_conflict"]
    score = score - float(penalties.get("volume_dry", 0.0)) * frame["neg_volume_dry"]
    score = score - float(penalties.get("context_conflict", 0.0)) * frame["neg_context_conflict"]
    score = score - float(penalties.get("breakout_fail", 0.0)) * frame["neg_breakout_fail"]
    return score


def _pick_monthly_top(frame: pd.DataFrame, cutoff: float) -> pd.DataFrame:
    picks: list[pd.DataFrame] = []
    for month, grp in frame.groupby("month_bucket", dropna=False):
        if grp.empty:
            continue
        top_n = max(1, int(math.ceil(len(grp) * float(cutoff))))
        picks.append(grp.sort_values("score_total", ascending=False).head(top_n))
    if not picks:
        return pd.DataFrame(columns=frame.columns)
    return pd.concat(picks, ignore_index=True)


@dataclass
class TrialEvaluation:
    summary: dict[str, Any]
    selected_rows: pd.DataFrame
    fold_artifacts: list[dict[str, Any]]


def evaluate_trial(
    frame: pd.DataFrame,
    config: ResearchConfig,
    timeframe: str,
    family: str,
    params: dict[str, Any],
) -> TrialEvaluation:
    spec = FAMILY_SPECS[family]
    horizons = horizons_for_timeframe(timeframe)
    windows = build_walkforward_windows(frame, config)
    if not windows:
        return TrialEvaluation(
            summary={
                "timeframe": timeframe,
                "family": family,
                "folds": 0,
                "samples": 0,
                "fold_months": 0,
                "oos_return": 0.0,
                "profit_factor": 0.0,
                "positive_window_ratio": 0.0,
                "worst_drawdown": 0.0,
                "stability": 0.0,
                "cluster_consistency": 0.0,
            },
            selected_rows=pd.DataFrame(),
            fold_artifacts=[],
        )

    selected_parts: list[pd.DataFrame] = []
    monthly_returns: list[float] = []
    fold_artifacts: list[dict[str, Any]] = []
    total_oos_months = 0

    for fold_idx, window in enumerate(windows, start=1):
        train_mask = frame["month_bucket"].astype(str).isin(window["train_months"])
        valid_mask = frame["month_bucket"].astype(str).isin(window["valid_months"])
        test_mask = frame["month_bucket"].astype(str).isin(window["test_months"])
        train_df = frame[train_mask].copy()
        if train_df.empty:
            continue
        oos_df = frame[valid_mask | test_mask].copy()
        if oos_df.empty:
            continue

        prior_df = _cluster_prior(train_df, direction=spec.direction, horizons=horizons)
        raw_df = _group_raws(
            frame=frame[train_mask | valid_mask | test_mask].copy(),
            timeframe=timeframe,
            family=family,
            params=params,
            cluster_prior=prior_df,
        )
        raw_months = raw_df["month_bucket"].astype(str)
        normalized_df, normalizers = _normalize_groups(
            raw_df,
            train_idx=raw_df.index[raw_months.isin(window["train_months"])],
            allowed_groups=spec.allowed_groups,
        )
        normalized_df["score_total"] = _score_rows(normalized_df, spec.allowed_groups, params)
        normalized_df["primary_return"] = _primary_signed_return(normalized_df, direction=spec.direction, horizons=horizons)

        oos_scored = normalized_df[
            raw_months.isin([*window["valid_months"], *window["test_months"]])
        ].copy()
        picks = _pick_monthly_top(oos_scored, float(params["selection_cutoff"]))
        if picks.empty:
            continue
        picks["fold_id"] = fold_idx
        selected_parts.append(picks)

        month_returns = (
            picks.groupby("month_bucket", as_index=False)["primary_return"]
            .mean()
            .sort_values("month_bucket")
            .reset_index(drop=True)
        )
        monthly_returns.extend(month_returns["primary_return"].tolist())
        total_oos_months += int(month_returns["month_bucket"].nunique())
        fold_artifacts.append(
            {
                "fold_id": fold_idx,
                "train_months": list(window["train_months"]),
                "valid_months": list(window["valid_months"]),
                "test_months": list(window["test_months"]),
                "selection_cutoff": float(params["selection_cutoff"]),
                "cluster_prior": prior_df.to_dict(orient="records"),
                "normalizers": normalizers,
                "params": json.loads(json.dumps(params)),
                "samples": int(len(picks)),
            }
        )

    if not selected_parts:
        return TrialEvaluation(
            summary={
                "timeframe": timeframe,
                "family": family,
                "folds": len(windows),
                "samples": 0,
                "fold_months": 0,
                "oos_return": 0.0,
                "profit_factor": 0.0,
                "positive_window_ratio": 0.0,
                "worst_drawdown": 0.0,
                "stability": 0.0,
                "cluster_consistency": 0.0,
            },
            selected_rows=pd.DataFrame(),
            fold_artifacts=fold_artifacts,
        )

    selected = pd.concat(selected_parts, ignore_index=True)
    monthly_ret_series = pd.Series(monthly_returns, dtype=float)
    summary = {
        "timeframe": timeframe,
        "family": family,
        "folds": len(fold_artifacts),
        "samples": int(len(selected)),
        "fold_months": int(total_oos_months),
        "oos_return": float(monthly_ret_series.mean()) if not monthly_ret_series.empty else 0.0,
        "profit_factor": _profit_factor(monthly_ret_series),
        "positive_window_ratio": float(np.mean(monthly_ret_series > 0.0)) if not monthly_ret_series.empty else 0.0,
        "worst_drawdown": _max_drawdown(monthly_ret_series),
        "stability": _stability(selected, direction=spec.direction, horizons=horizons),
        "cluster_consistency": _cluster_consistency(selected, direction=spec.direction),
    }
    return TrialEvaluation(summary=summary, selected_rows=selected, fold_artifacts=fold_artifacts)


def retention_gate(summary: dict[str, Any], config: ResearchConfig) -> tuple[bool, list[str]]:
    gate = config.study.retention_gates
    reasons: list[str] = []
    if float(summary.get("profit_factor", 0.0)) <= float(gate.min_profit_factor):
        reasons.append("pf")
    if float(summary.get("positive_window_ratio", 0.0)) < float(gate.min_positive_window_ratio):
        reasons.append("positive_window_ratio")
    if float(summary.get("worst_drawdown", 1.0)) > float(gate.max_worst_drawdown):
        reasons.append("worst_drawdown")
    if int(summary.get("samples", 0)) < int(gate.min_samples):
        reasons.append("min_samples")
    return len(reasons) == 0, reasons


def adoption_gate(summary: dict[str, Any], config: ResearchConfig) -> tuple[bool, list[str]]:
    gate = config.study.adoption_gates
    reasons: list[str] = []
    if float(summary.get("oos_return", 0.0)) < float(gate.min_oos_return):
        reasons.append("oos_return")
    if float(summary.get("profit_factor", 0.0)) < float(gate.min_pf):
        reasons.append("profit_factor")
    if float(summary.get("positive_window_ratio", 0.0)) < float(gate.min_positive_window_ratio):
        reasons.append("positive_window_ratio")
    if float(summary.get("worst_drawdown", 1.0)) > float(gate.max_worst_drawdown):
        reasons.append("worst_drawdown")
    if float(summary.get("stability", 0.0)) < float(gate.min_stability):
        reasons.append("stability")
    if float(summary.get("cluster_consistency", 0.0)) < float(gate.min_cluster_consistency):
        reasons.append("cluster_consistency")
    if int(summary.get("fold_months", 0)) < int(gate.min_fold_months):
        reasons.append("fold_months")
    return len(reasons) == 0, reasons


def write_fold_artifacts(
    paths: ResearchPaths,
    study_id: str,
    trial_id: str,
    payloads: list[dict[str, Any]],
) -> None:
    target_dir = fold_artifact_dir(paths, study_id, trial_id)
    for item in payloads:
        fold_id = int(item.get("fold_id", 0))
        target = target_dir / f"fold_{fold_id:03d}.json"
        write_json(target, item)
