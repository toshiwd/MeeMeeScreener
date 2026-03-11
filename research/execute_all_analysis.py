from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all post-hoc analyses for a training run.")
    parser.add_argument("--run-id", required=True, help="Target run id under research_workspace/runs")
    parser.add_argument(
        "--workspace",
        default="research_workspace",
        help="Workspace root containing runs/ and cache/",
    )
    return parser.parse_args()


def _to_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"required file not found: {path}")
    return pd.read_csv(path)


def _load_run_frames(run_dir: Path) -> dict[str, pd.DataFrame]:
    rankings_long = _read_csv(run_dir / "rankings_long.csv")
    rankings_short = _read_csv(run_dir / "rankings_short.csv")
    top20_long = _read_csv(run_dir / "top20_long.csv")
    top20_short = _read_csv(run_dir / "top20_short.csv")
    eval_monthly = _read_csv(run_dir / "evaluation_monthly.csv")

    rankings = pd.concat([rankings_long, rankings_short], ignore_index=True)
    top20 = pd.concat([top20_long, top20_short], ignore_index=True)

    numeric_cols = [
        "score",
        "pred_return",
        "pred_prob_tp",
        "risk_dn",
        "realized_return",
        "tp_hit",
        "mae",
        "mfe",
        "return_at20",
        "hit_at20",
        "drawdown",
    ]
    rankings = _to_numeric(rankings, numeric_cols)
    top20 = _to_numeric(top20, numeric_cols)
    eval_monthly = _to_numeric(eval_monthly, numeric_cols)
    return {
        "rankings": rankings,
        "top20": top20,
        "eval_monthly": eval_monthly,
    }


def _max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return float("nan")
    equity = (1.0 + returns.fillna(0.0)).cumprod()
    peak = equity.cummax()
    dd = (equity / peak) - 1.0
    return float(max(0.0, -dd.min()))


def _return_quality(returns: pd.Series) -> dict[str, float | int | None]:
    vals = pd.to_numeric(returns, errors="coerce").dropna()
    if vals.empty:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "p05": None,
            "p25": None,
            "p75": None,
            "p95": None,
            "std": None,
            "win_rate": None,
            "pos_months": 0,
            "neg_months": 0,
            "sharpe": None,
            "sortino": None,
            "skew": None,
            "max_drawdown": None,
        }

    arr = vals.to_numpy(dtype=float)
    mean = float(np.mean(arr))
    std = float(np.std(arr))
    downside = np.minimum(arr, 0.0)
    downside_std = float(np.sqrt(np.mean(np.square(downside))))
    skew = float(((arr - mean) ** 3).mean() / (std ** 3)) if std > 1e-12 else None
    sharpe = float(mean / std) if std > 1e-12 else None
    sortino = float(mean / downside_std) if downside_std > 1e-12 else None
    return {
        "n": int(len(arr)),
        "mean": mean,
        "median": float(np.median(arr)),
        "p05": float(np.quantile(arr, 0.05)),
        "p25": float(np.quantile(arr, 0.25)),
        "p75": float(np.quantile(arr, 0.75)),
        "p95": float(np.quantile(arr, 0.95)),
        "std": std,
        "win_rate": float(np.mean(arr > 0.0)),
        "pos_months": int(np.sum(arr > 0.0)),
        "neg_months": int(np.sum(arr < 0.0)),
        "sharpe": sharpe,
        "sortino": sortino,
        "skew": skew,
        "max_drawdown": _max_drawdown(pd.Series(arr)),
    }


def evaluate_return_quality(eval_monthly: pd.DataFrame) -> pd.DataFrame:
    src = eval_monthly.copy()
    src = src[src["phase"].isin(["valid", "test"])].copy()
    rows: list[dict[str, Any]] = []

    for (phase, side), grp in src.groupby(["phase", "side"], dropna=False):
        q = _return_quality(grp["return_at20"])
        q.update({"phase": phase, "side": side})
        rows.append(q)

    for phase, grp in src.groupby("phase", dropna=False):
        q = _return_quality(grp["return_at20"])
        q.update({"phase": phase, "side": "all"})
        rows.append(q)

    for side, grp in src.groupby("side", dropna=False):
        q = _return_quality(grp["return_at20"])
        q.update({"phase": "all", "side": side})
        rows.append(q)

    q = _return_quality(src["return_at20"])
    q.update({"phase": "all", "side": "all"})
    rows.append(q)

    out = pd.DataFrame(rows)
    out = out[
        [
            "phase",
            "side",
            "n",
            "mean",
            "median",
            "p05",
            "p25",
            "p75",
            "p95",
            "std",
            "win_rate",
            "pos_months",
            "neg_months",
            "sharpe",
            "sortino",
            "skew",
            "max_drawdown",
        ]
    ]
    return out.sort_values(["phase", "side"]).reset_index(drop=True)


def _resolve_cache_dir(run_dir: Path) -> Path:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cache_key = manifest.get("cache_key")
    if not cache_key:
        raise ValueError("manifest.cache_key is missing")
    cache_dir = run_dir.parent.parent / "cache" / str(cache_key)
    if not cache_dir.exists():
        raise FileNotFoundError(f"cache dir not found: {cache_dir}")
    return cache_dir


def load_feature_subset(cache_dir: Path, asof_dates: list[str]) -> pd.DataFrame:
    usecols = [
        "asof_date",
        "code",
        "overheated25",
        "market_trend_state",
        "vol_regime",
        "dev_ma20",
        "dev_ma60",
        "rsi14",
    ]
    parts: list[pd.DataFrame] = []
    for asof in sorted(set(asof_dates)):
        path = cache_dir / f"features_{asof}.csv"
        if not path.exists():
            continue
        part = pd.read_csv(path, usecols=lambda c: c in set(usecols))
        parts.append(part)

    if not parts:
        return pd.DataFrame(columns=usecols)

    merged = pd.concat(parts, ignore_index=True)
    merged = _to_numeric(merged, ["code", "overheated25", "market_trend_state", "vol_regime", "dev_ma20", "dev_ma60", "rsi14"])
    merged["asof_date"] = merged["asof_date"].astype(str)
    return merged


def build_analysis_frame(top20: pd.DataFrame, feature_subset: pd.DataFrame) -> pd.DataFrame:
    base = top20[top20["phase"].isin(["valid", "test"])].copy()
    base = base.dropna(subset=["realized_return"]).copy()
    base["asof_date"] = base["asof_date"].astype(str)
    base["code"] = pd.to_numeric(base["code"], errors="coerce")
    merged = base.merge(feature_subset, on=["asof_date", "code"], how="left")

    merged["overheated25"] = pd.to_numeric(merged.get("overheated25"), errors="coerce").fillna(0.0)
    merged["market_trend_state"] = pd.to_numeric(merged.get("market_trend_state"), errors="coerce").fillna(1.0).clip(0, 2)
    merged["vol_regime"] = pd.to_numeric(merged.get("vol_regime"), errors="coerce").fillna(1.0).clip(0, 2)

    merged["hot_flag"] = np.where(merged["overheated25"] >= 0.5, "hot", "normal")
    merged["trend_state"] = merged["market_trend_state"].round().astype(int).astype(str)
    merged["vol_state"] = merged["vol_regime"].round().astype(int).astype(str)
    merged["regime"] = "mt" + merged["trend_state"] + "_vr" + merged["vol_state"]
    return merged


def slice_report(analysis_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    bins = np.linspace(0.0, 1.0, 6)
    src = analysis_df.copy()
    src["prob_bin"] = pd.cut(src["pred_prob_tp"].clip(0, 1), bins=bins, include_lowest=True, right=True).astype(str)

    for (side, phase), g0 in src.groupby(["side", "phase"], dropna=False):
        for slice_col, slice_type in [
            ("hot_flag", "overheated"),
            ("trend_state", "trend_state"),
            ("vol_state", "vol_regime"),
            ("prob_bin", "probability_bin"),
        ]:
            for key, g in g0.groupby(slice_col, dropna=False):
                if g.empty:
                    continue
                rr = pd.to_numeric(g["realized_return"], errors="coerce").dropna()
                tp = pd.to_numeric(g["tp_hit"], errors="coerce").dropna()
                rows.append(
                    {
                        "side": side,
                        "phase": phase,
                        "slice_type": slice_type,
                        "slice_key": str(key),
                        "n": int(len(g)),
                        "mean_return": float(rr.mean()) if not rr.empty else None,
                        "median_return": float(rr.median()) if not rr.empty else None,
                        "win_rate": float((rr > 0.0).mean()) if not rr.empty else None,
                        "tp_rate": float(tp.mean()) if not tp.empty else None,
                        "mae_mean": float(pd.to_numeric(g["mae"], errors="coerce").mean()),
                        "mfe_mean": float(pd.to_numeric(g["mfe"], errors="coerce").mean()),
                        "score_mean": float(pd.to_numeric(g["score"], errors="coerce").mean()),
                        "pred_prob_mean": float(pd.to_numeric(g["pred_prob_tp"], errors="coerce").mean()),
                        "risk_mean": float(pd.to_numeric(g["risk_dn"], errors="coerce").mean()),
                    }
                )

    return pd.DataFrame(rows).sort_values(["side", "phase", "slice_type", "slice_key"]).reset_index(drop=True)


def _apply_gate(frame: pd.DataFrame, gate: dict[str, Any]) -> pd.DataFrame:
    sel = (frame["pred_prob_tp"] >= gate["prob_threshold"]) & (frame["risk_dn"] <= gate["risk_threshold"])
    if gate.get("avoid_hot", False):
        sel = sel & (frame["overheated25"] < 0.5)
    return frame[sel].copy()


def _pick_best_gate(valid_df: pd.DataFrame, side: str) -> dict[str, Any]:
    if valid_df.empty:
        return {
            "prob_threshold": 0.0,
            "risk_threshold": float(valid_df["risk_dn"].max()) if "risk_dn" in valid_df.columns else 1.0,
            "avoid_hot": False,
            "objective": float("-inf"),
        }

    prob_vals = pd.to_numeric(valid_df["pred_prob_tp"], errors="coerce").dropna().clip(0, 1)
    risk_vals = pd.to_numeric(valid_df["risk_dn"], errors="coerce").dropna()
    if prob_vals.empty or risk_vals.empty:
        return {
            "prob_threshold": 0.0,
            "risk_threshold": 1e9,
            "avoid_hot": False,
            "objective": float("-inf"),
        }

    prob_grid = np.unique(np.quantile(prob_vals, [0.3, 0.4, 0.5, 0.6, 0.7, 0.8]))
    risk_grid = np.unique(np.quantile(risk_vals, [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]))
    avoid_hot_grid = [False, True] if side == "long" else [False]
    min_count = max(5, int(len(valid_df) * 0.15))

    best: dict[str, Any] | None = None
    for p_thr in prob_grid:
        for r_thr in risk_grid:
            for avoid_hot in avoid_hot_grid:
                gate = {
                    "prob_threshold": float(p_thr),
                    "risk_threshold": float(r_thr),
                    "avoid_hot": bool(avoid_hot),
                }
                picked = _apply_gate(valid_df, gate)
                if len(picked) < min_count:
                    continue

                rets = pd.to_numeric(picked["realized_return"], errors="coerce").dropna()
                if rets.empty:
                    continue
                mean_ret = float(rets.mean())
                std_ret = float(rets.std(ddof=0))
                coverage = float(len(picked) / max(1, len(valid_df)))
                objective = mean_ret - 0.25 * std_ret + 0.002 * coverage

                candidate = {
                    "prob_threshold": float(p_thr),
                    "risk_threshold": float(r_thr),
                    "avoid_hot": bool(avoid_hot),
                    "objective": float(objective),
                }
                if best is None or candidate["objective"] > best["objective"]:
                    best = candidate

    if best is not None:
        return best
    return {
        "prob_threshold": float(prob_vals.min()),
        "risk_threshold": float(risk_vals.max()),
        "avoid_hot": False,
        "objective": float("-inf"),
    }


def regime_gate_optimization(analysis_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    src = analysis_df.copy()

    for side in sorted(src["side"].dropna().unique().tolist()):
        side_df = src[src["side"] == side].copy()
        for regime in sorted(side_df["regime"].dropna().unique().tolist() + ["ALL"]):
            if regime == "ALL":
                work = side_df
            else:
                work = side_df[side_df["regime"] == regime].copy()

            valid_df = work[work["phase"] == "valid"].copy()
            test_df = work[work["phase"] == "test"].copy()
            if valid_df.empty or test_df.empty:
                continue

            gate = _pick_best_gate(valid_df, side=side)
            valid_pick = _apply_gate(valid_df, gate)
            test_pick = _apply_gate(test_df, gate)

            valid_ret = pd.to_numeric(valid_pick["realized_return"], errors="coerce").dropna()
            test_ret = pd.to_numeric(test_pick["realized_return"], errors="coerce").dropna()
            baseline_valid = pd.to_numeric(valid_df["realized_return"], errors="coerce").dropna()
            baseline_test = pd.to_numeric(test_df["realized_return"], errors="coerce").dropna()

            rows.append(
                {
                    "side": side,
                    "regime": regime,
                    "prob_threshold": gate["prob_threshold"],
                    "risk_threshold": gate["risk_threshold"],
                    "avoid_hot": int(gate.get("avoid_hot", False)),
                    "valid_count": int(len(valid_pick)),
                    "valid_total": int(len(valid_df)),
                    "valid_coverage": float(len(valid_pick) / max(1, len(valid_df))),
                    "valid_mean_return": float(valid_ret.mean()) if not valid_ret.empty else None,
                    "valid_baseline_return": float(baseline_valid.mean()) if not baseline_valid.empty else None,
                    "valid_return_delta": (
                        float(valid_ret.mean() - baseline_valid.mean())
                        if (not valid_ret.empty and not baseline_valid.empty)
                        else None
                    ),
                    "test_count": int(len(test_pick)),
                    "test_total": int(len(test_df)),
                    "test_coverage": float(len(test_pick) / max(1, len(test_df))),
                    "test_mean_return": float(test_ret.mean()) if not test_ret.empty else None,
                    "test_baseline_return": float(baseline_test.mean()) if not baseline_test.empty else None,
                    "test_return_delta": (
                        float(test_ret.mean() - baseline_test.mean())
                        if (not test_ret.empty and not baseline_test.empty)
                        else None
                    ),
                    "test_win_rate": float((test_ret > 0).mean()) if not test_ret.empty else None,
                    "test_baseline_win_rate": float((baseline_test > 0).mean()) if not baseline_test.empty else None,
                }
            )

    return pd.DataFrame(rows).sort_values(["side", "regime"]).reset_index(drop=True)


def _brier(y_true: np.ndarray, pred: np.ndarray) -> float | None:
    if len(y_true) == 0:
        return None
    return float(np.mean((pred - y_true) ** 2))


def _ece(y_true: np.ndarray, pred: np.ndarray, n_bins: int = 10) -> tuple[float | None, list[dict[str, Any]]]:
    if len(y_true) == 0:
        return None, []
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    total = len(y_true)
    ece = 0.0
    rows: list[dict[str, Any]] = []
    for i in range(n_bins):
        lo = edges[i]
        hi = edges[i + 1]
        if i == n_bins - 1:
            mask = (pred >= lo) & (pred <= hi)
        else:
            mask = (pred >= lo) & (pred < hi)
        cnt = int(mask.sum())
        if cnt == 0:
            continue
        avg_prob = float(pred[mask].mean())
        emp_rate = float(y_true[mask].mean())
        ece += abs(avg_prob - emp_rate) * (cnt / total)
        rows.append(
            {
                "bin_low": float(lo),
                "bin_high": float(hi),
                "count": cnt,
                "avg_prob": avg_prob,
                "empirical_rate": emp_rate,
            }
        )
    return float(ece), rows


def _fit_isotonic(x: np.ndarray, y: np.ndarray) -> dict[str, np.ndarray] | None:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]
    if len(x) == 0:
        return None

    order = np.argsort(x, kind="mergesort")
    x = x[order]
    y = y[order]

    sum_w: list[float] = []
    sum_y: list[float] = []
    right: list[float] = []
    for xi, yi in zip(x, y):
        sum_w.append(1.0)
        sum_y.append(float(yi))
        right.append(float(xi))

        while len(sum_w) >= 2:
            prev = sum_y[-2] / sum_w[-2]
            curr = sum_y[-1] / sum_w[-1]
            if prev <= curr:
                break
            sum_w[-2] += sum_w[-1]
            sum_y[-2] += sum_y[-1]
            right[-2] = right[-1]
            sum_w.pop()
            sum_y.pop()
            right.pop()

    values = np.array([sy / sw for sy, sw in zip(sum_y, sum_w)], dtype=float)
    values = np.clip(values, 0.0, 1.0)
    right_arr = np.array(right, dtype=float)
    return {"right": right_arr, "value": values}


def _apply_isotonic(model: dict[str, np.ndarray] | None, pred: np.ndarray) -> np.ndarray:
    p = np.asarray(pred, dtype=float)
    p = np.clip(p, 0.0, 1.0)
    if model is None or len(model["right"]) == 0:
        return p
    right = model["right"]
    value = model["value"]
    lo = float(right.min())
    hi = float(right.max())
    p_clip = np.clip(p, lo, hi)
    idx = np.searchsorted(right, p_clip, side="left")
    idx = np.clip(idx, 0, len(value) - 1)
    return value[idx]


def calibration_report(rankings: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    rel_rows: list[dict[str, Any]] = []
    src = rankings.copy()
    src = src[src["phase"].isin(["valid", "test"])].copy()
    src = src.dropna(subset=["tp_hit", "pred_prob_tp"]).copy()
    src["tp_hit"] = pd.to_numeric(src["tp_hit"], errors="coerce").fillna(0.0).clip(0.0, 1.0)
    src["pred_prob_tp"] = pd.to_numeric(src["pred_prob_tp"], errors="coerce").fillna(0.5).clip(0.0, 1.0)

    for side in sorted(src["side"].dropna().unique().tolist()):
        valid = src[(src["side"] == side) & (src["phase"] == "valid")].copy()
        test = src[(src["side"] == side) & (src["phase"] == "test")].copy()
        if valid.empty:
            continue

        model = _fit_isotonic(valid["pred_prob_tp"].to_numpy(), valid["tp_hit"].to_numpy())
        for phase_name, frame in [("valid", valid), ("test", test)]:
            if frame.empty:
                continue
            y_true = frame["tp_hit"].to_numpy(dtype=float)
            p_raw = frame["pred_prob_tp"].to_numpy(dtype=float)
            p_cal = _apply_isotonic(model, p_raw)
            for mode, pred in [("raw", p_raw), ("isotonic", p_cal)]:
                brier = _brier(y_true, pred)
                ece, bins = _ece(y_true, pred, n_bins=10)
                rows.append(
                    {
                        "side": side,
                        "phase": phase_name,
                        "mode": mode,
                        "n": int(len(frame)),
                        "brier": brier,
                        "ece": ece,
                        "avg_pred": float(np.mean(pred)) if len(pred) else None,
                        "emp_rate": float(np.mean(y_true)) if len(y_true) else None,
                    }
                )
                for b in bins:
                    rel_rows.append(
                        {
                            "side": side,
                            "phase": phase_name,
                            "mode": mode,
                            **b,
                        }
                    )

    return (
        pd.DataFrame(rows).sort_values(["side", "phase", "mode"]).reset_index(drop=True),
        pd.DataFrame(rel_rows).sort_values(["side", "phase", "mode", "bin_low"]).reset_index(drop=True),
    )


def rolling_walkforward(analysis_df: pd.DataFrame, min_train_months: int = 12) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    src = analysis_df.copy()
    src = src[src["phase"].isin(["valid", "test"])].copy()

    for side in sorted(src["side"].dropna().unique().tolist()):
        side_df = src[src["side"] == side].copy()
        months = sorted(side_df["asof_date"].dropna().unique().tolist())
        if len(months) <= min_train_months:
            continue

        for idx in range(min_train_months, len(months)):
            test_month = months[idx]
            train_months = months[:idx]
            train_df = side_df[side_df["asof_date"].isin(train_months)].copy()
            test_df = side_df[side_df["asof_date"] == test_month].copy()
            if train_df.empty or test_df.empty:
                continue

            gate = _pick_best_gate(train_df, side=side)
            picked = _apply_gate(test_df, gate)

            test_ret = pd.to_numeric(test_df["realized_return"], errors="coerce").dropna()
            pick_ret = pd.to_numeric(picked["realized_return"], errors="coerce").dropna()
            rows.append(
                {
                    "side": side,
                    "month": test_month,
                    "train_months": int(len(train_months)),
                    "prob_threshold": gate["prob_threshold"],
                    "risk_threshold": gate["risk_threshold"],
                    "avoid_hot": int(gate.get("avoid_hot", False)),
                    "selected_n": int(len(picked)),
                    "total_n": int(len(test_df)),
                    "coverage": float(len(picked) / max(1, len(test_df))),
                    "selected_mean_return": float(pick_ret.mean()) if not pick_ret.empty else None,
                    "baseline_mean_return": float(test_ret.mean()) if not test_ret.empty else None,
                    "return_delta": (
                        float(pick_ret.mean() - test_ret.mean())
                        if (not pick_ret.empty and not test_ret.empty)
                        else None
                    ),
                    "selected_win_rate": float((pick_ret > 0).mean()) if not pick_ret.empty else None,
                    "baseline_win_rate": float((test_ret > 0).mean()) if not test_ret.empty else None,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["side", "month"]).reset_index(drop=True)
    for side in sorted(out["side"].unique().tolist()):
        m = out["side"] == side
        sr = pd.to_numeric(out.loc[m, "selected_mean_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        br = pd.to_numeric(out.loc[m, "baseline_mean_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        out.loc[m, "selected_cum_return"] = (1.0 + pd.Series(sr)).cumprod().to_numpy() - 1.0
        out.loc[m, "baseline_cum_return"] = (1.0 + pd.Series(br)).cumprod().to_numpy() - 1.0
    return out


def summarize_json(
    run_id: str,
    rankings: pd.DataFrame,
    top20: pd.DataFrame,
    quality: pd.DataFrame,
    gates: pd.DataFrame,
    cal: pd.DataFrame,
    wf: pd.DataFrame,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_rows": {
            "rankings": int(len(rankings)),
            "top20": int(len(top20)),
        },
        "quality_rows": int(len(quality)),
        "gate_rows": int(len(gates)),
        "calibration_rows": int(len(cal)),
        "rolling_wf_rows": int(len(wf)),
    }

    q_focus = quality[(quality["phase"].isin(["valid", "test"])) & (quality["side"].isin(["long", "short"]))]
    out["quality_focus"] = q_focus.to_dict(orient="records")

    if not gates.empty:
        gate_focus = (
            gates[gates["regime"] == "ALL"][
                ["side", "test_return_delta", "test_coverage", "test_count", "test_total"]
            ]
            .sort_values("side")
            .to_dict(orient="records")
        )
        out["gate_focus"] = gate_focus
    else:
        out["gate_focus"] = []

    cal_focus: list[dict[str, Any]] = []
    if not cal.empty:
        for side in sorted(cal["side"].dropna().unique().tolist()):
            for phase in ["valid", "test"]:
                part = cal[(cal["side"] == side) & (cal["phase"] == phase)]
                raw = part[part["mode"] == "raw"]
                iso = part[part["mode"] == "isotonic"]
                if raw.empty or iso.empty:
                    continue
                cal_focus.append(
                    {
                        "side": side,
                        "phase": phase,
                        "brier_raw": float(raw["brier"].iloc[0]),
                        "brier_isotonic": float(iso["brier"].iloc[0]),
                        "ece_raw": float(raw["ece"].iloc[0]),
                        "ece_isotonic": float(iso["ece"].iloc[0]),
                    }
                )
    out["calibration_focus"] = cal_focus

    wf_focus: list[dict[str, Any]] = []
    if not wf.empty:
        for side in sorted(wf["side"].dropna().unique().tolist()):
            part = wf[wf["side"] == side]
            wf_focus.append(
                {
                    "side": side,
                    "months": int(len(part)),
                    "mean_return_delta": float(pd.to_numeric(part["return_delta"], errors="coerce").mean()),
                    "final_selected_cum_return": float(pd.to_numeric(part["selected_cum_return"], errors="coerce").iloc[-1]),
                    "final_baseline_cum_return": float(pd.to_numeric(part["baseline_cum_return"], errors="coerce").iloc[-1]),
                }
            )
    out["rolling_wf_focus"] = wf_focus
    return out


def main() -> None:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    run_dir = workspace / "runs" / args.run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"run dir not found: {run_dir}")

    frames = _load_run_frames(run_dir)
    rankings = frames["rankings"]
    top20 = frames["top20"]
    eval_monthly = frames["eval_monthly"]

    quality_df = evaluate_return_quality(eval_monthly)
    quality_df.to_csv(run_dir / "analysis_quality.csv", index=False)

    cache_dir = _resolve_cache_dir(run_dir)
    asof_dates = top20[top20["phase"].isin(["valid", "test"])]["asof_date"].astype(str).dropna().unique().tolist()
    feat_df = load_feature_subset(cache_dir, asof_dates)
    analysis_df = build_analysis_frame(top20, feat_df)

    slices_df = slice_report(analysis_df)
    slices_df.to_csv(run_dir / "analysis_slices.csv", index=False)

    gates_df = regime_gate_optimization(analysis_df)
    gates_df.to_csv(run_dir / "analysis_regime_gates.csv", index=False)

    cal_df, rel_df = calibration_report(rankings)
    cal_df.to_csv(run_dir / "analysis_calibration.csv", index=False)
    rel_df.to_csv(run_dir / "analysis_reliability.csv", index=False)

    wf_df = rolling_walkforward(analysis_df, min_train_months=12)
    wf_df.to_csv(run_dir / "analysis_rolling_wf.csv", index=False)

    summary = summarize_json(
        run_id=args.run_id,
        rankings=rankings,
        top20=top20,
        quality=quality_df,
        gates=gates_df,
        cal=cal_df,
        wf=wf_df,
    )
    (run_dir / "analysis_exec_all.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
