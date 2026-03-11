from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import time
from typing import Any

import numpy as np
import pandas as pd

from research.config import ResearchConfig, from_dict
from research.labels import load_label_history
from research.storage import ResearchPaths, now_utc_iso, read_csv, read_json, write_csv, write_json


BLEND_VALID_WEIGHT = 0.60
HISTORY_LOCK_TIMEOUT_SEC = 180.0
HISTORY_LOCK_POLL_SEC = 0.25


def _drawdown_series(monthly_returns: pd.Series) -> pd.Series:
    if monthly_returns.empty:
        return pd.Series(dtype=float)
    equity = (1.0 + monthly_returns.fillna(0.0)).cumprod()
    running_peak = equity.cummax()
    return equity / running_peak - 1.0


def _max_drawdown(monthly_returns: pd.Series) -> float:
    if monthly_returns.empty:
        return 0.0
    drawdown = _drawdown_series(monthly_returns)
    return float(max(0.0, -drawdown.min()))


def _metrics_for_side(
    side: str,
    rankings: pd.DataFrame,
    labels: pd.DataFrame,
    expected_by_month: dict[str, int],
    top_k: int,
    phase: str,
) -> tuple[dict[str, Any], pd.DataFrame]:
    phase_frame = rankings[rankings["phase"] == phase].copy()
    if phase_frame.empty:
        empty = pd.DataFrame(
            columns=[
                "phase",
                "side",
                "asof_date",
                "hit_at20",
                "return_at20",
                "mae_mean",
                "mae_p90",
                "coverage",
                "expected_symbols",
                "labeled_symbols",
                "predicted_symbols",
                "drawdown",
            ]
        )
        metrics = {
            "phase": phase,
            "side": side,
            "months": 0,
            "hit_at20": 0.0,
            "return_at20": 0.0,
            "mae_mean": 0.0,
            "mae_p90": 0.0,
            "max_drawdown": 0.0,
            "coverage": 0.0,
            "missing_rate": 1.0,
        }
        return metrics, empty

    hit_values: list[float] = []
    ret_values: list[float] = []
    mae_values: list[float] = []
    coverage_values: list[float] = []
    monthly_ret: list[float] = []
    monthly_rows: list[dict[str, Any]] = []

    phase_months = sorted(phase_frame["asof_date"].dropna().unique().tolist())
    for month in phase_months:
        pred_m = phase_frame[phase_frame["asof_date"] == month].sort_values("score", ascending=False).head(top_k)
        label_m = labels[(labels["asof_date"] == month) & (labels["side"] == side)].copy()
        true_top = set(label_m.sort_values("realized_return", ascending=False).head(top_k)["code"].astype(str))
        pred_codes = set(pred_m["code"].astype(str))
        hit = len(pred_codes.intersection(true_top))
        hit_values.append(float(hit))

        pred_eval = pred_m.merge(
            label_m[["code", "realized_return", "mae"]],
            on="code",
            how="left",
            suffixes=("", "_label"),
        )
        month_ret = float(pd.to_numeric(pred_eval["realized_return_label"], errors="coerce").fillna(0.0).mean())
        monthly_ret.append(month_ret)
        ret_values.append(month_ret)

        month_mae = pd.to_numeric(pred_eval["mae_label"], errors="coerce").dropna()
        month_mae_mean = float(month_mae.mean()) if not month_mae.empty else 0.0
        month_mae_p90 = float(np.quantile(month_mae.to_numpy(dtype=float), 0.90)) if not month_mae.empty else 0.0
        if not month_mae.empty:
            mae_values.extend([float(x) for x in month_mae.tolist()])

        expected = int(expected_by_month.get(month, 0))
        labeled_count = int(label_m["code"].astype(str).nunique()) if "code" in label_m.columns else int(len(label_m))
        coverage = float(labeled_count / expected) if expected > 0 else 0.0
        coverage = float(min(max(coverage, 0.0), 1.0))
        coverage_values.append(coverage)
        monthly_rows.append(
            {
                "phase": phase,
                "side": side,
                "asof_date": str(month),
                "hit_at20": float(hit),
                "return_at20": float(month_ret),
                "mae_mean": month_mae_mean,
                "mae_p90": month_mae_p90,
                "coverage": float(coverage),
                "expected_symbols": int(expected),
                "labeled_symbols": int(labeled_count),
                "predicted_symbols": int(len(pred_m)),
            }
        )

    mae_array = np.asarray(mae_values, dtype=float) if mae_values else np.asarray([], dtype=float)
    dd = _drawdown_series(pd.Series(monthly_ret, dtype=float))
    dd_pos = [-float(x) if float(x) < 0 else 0.0 for x in dd.tolist()]
    for idx, row in enumerate(monthly_rows):
        row["drawdown"] = float(dd_pos[idx]) if idx < len(dd_pos) else 0.0

    monthly_df = pd.DataFrame(monthly_rows)
    if monthly_df.empty:
        monthly_df = pd.DataFrame(
            columns=[
                "phase",
                "side",
                "asof_date",
                "hit_at20",
                "return_at20",
                "mae_mean",
                "mae_p90",
                "coverage",
                "expected_symbols",
                "labeled_symbols",
                "predicted_symbols",
                "drawdown",
            ]
        )

    result = {
        "phase": phase,
        "side": side,
        "months": int(len(phase_months)),
        "hit_at20": float(np.mean(hit_values)) if hit_values else 0.0,
        "return_at20": float(np.mean(ret_values)) if ret_values else 0.0,
        "mae_mean": float(mae_array.mean()) if mae_array.size else 0.0,
        "mae_p90": float(np.quantile(mae_array, 0.90)) if mae_array.size else 0.0,
        "max_drawdown": _max_drawdown(pd.Series(monthly_ret, dtype=float)),
        "coverage": float(np.mean(coverage_values)) if coverage_values else 0.0,
        "missing_rate": float(1.0 - np.mean(coverage_values)) if coverage_values else 1.0,
    }
    return result, monthly_df


def _build_expected_counts(universe: pd.DataFrame) -> dict[str, int]:
    if universe.empty:
        return {}
    tmp = universe.copy()
    tmp["asof_date"] = pd.to_datetime(tmp["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    counts = tmp.groupby("asof_date")["code"].nunique().to_dict()
    return {str(k): int(v) for k, v in counts.items()}


def _overall_metrics(long_metrics: dict[str, Any], short_metrics: dict[str, Any]) -> dict[str, float | int]:
    return {
        "months": int(max(int(long_metrics.get("months", 0)), int(short_metrics.get("months", 0)))),
        "hit_at20": float((float(long_metrics["hit_at20"]) + float(short_metrics["hit_at20"])) / 2.0),
        "return_at20": float((float(long_metrics["return_at20"]) + float(short_metrics["return_at20"])) / 2.0),
        "risk_mae_p90": float(max(float(long_metrics["mae_p90"]), float(short_metrics["mae_p90"]))),
        "coverage": float((float(long_metrics["coverage"]) + float(short_metrics["coverage"])) / 2.0),
    }


def _blend_side_metrics(valid: dict[str, Any], test: dict[str, Any], valid_weight: float) -> dict[str, Any]:
    w = float(min(max(valid_weight, 0.0), 1.0))
    tw = 1.0 - w
    return {
        "phase": "valid_test_blend",
        "side": str(valid.get("side") or test.get("side") or "unknown"),
        "months": int(max(int(valid.get("months", 0)), int(test.get("months", 0)))),
        "hit_at20": float(w * float(valid.get("hit_at20", 0.0)) + tw * float(test.get("hit_at20", 0.0))),
        "return_at20": float(w * float(valid.get("return_at20", 0.0)) + tw * float(test.get("return_at20", 0.0))),
        "mae_mean": float(w * float(valid.get("mae_mean", 0.0)) + tw * float(test.get("mae_mean", 0.0))),
        "mae_p90": float(max(float(valid.get("mae_p90", 0.0)), float(test.get("mae_p90", 0.0)))),
        "max_drawdown": float(max(float(valid.get("max_drawdown", 0.0)), float(test.get("max_drawdown", 0.0)))),
        "coverage": float(w * float(valid.get("coverage", 0.0)) + tw * float(test.get("coverage", 0.0))),
        "missing_rate": float(w * float(valid.get("missing_rate", 1.0)) + tw * float(test.get("missing_rate", 1.0))),
    }


def _blend_phase_metrics(valid_phase: dict[str, Any], test_phase: dict[str, Any], valid_weight: float) -> dict[str, Any]:
    long_blend = _blend_side_metrics(valid_phase["long"], test_phase["long"], valid_weight=valid_weight)
    short_blend = _blend_side_metrics(valid_phase["short"], test_phase["short"], valid_weight=valid_weight)
    overall_blend = _overall_metrics(long_blend, short_blend)
    return {
        "long": long_blend,
        "short": short_blend,
        "overall": overall_blend,
    }


def _pareto_group(phase: str) -> str:
    p = str(phase or "").strip().lower()
    if p in {"valid", "test", "valid_test_blend", "blend"}:
        return "core"
    return p if p else "core"


def _compute_pareto(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history

    hist = history.copy()
    if "evaluation_phase" not in hist.columns:
        hist["evaluation_phase"] = "test"
    hist["evaluation_phase"] = hist["evaluation_phase"].fillna("test").astype(str)
    hist.loc[hist["evaluation_phase"].str.strip() == "", "evaluation_phase"] = "test"
    hist["is_pareto"] = False

    for idx, row in hist.iterrows():
        dominated = False
        row_group = _pareto_group(str(row.get("evaluation_phase", "test")))
        for jdx, other in hist.iterrows():
            if idx == jdx:
                continue
            other_group = _pareto_group(str(other.get("evaluation_phase", "test")))
            if other_group != row_group:
                continue
            better_or_equal = (
                float(other["overall_hit_at20"]) >= float(row["overall_hit_at20"])
                and float(other["overall_return_at20"]) >= float(row["overall_return_at20"])
                and float(other["overall_risk_mae_p90"]) <= float(row["overall_risk_mae_p90"])
                and float(other["overall_coverage"]) >= float(row["overall_coverage"])
            )
            strictly_better = (
                float(other["overall_hit_at20"]) > float(row["overall_hit_at20"])
                or float(other["overall_return_at20"]) > float(row["overall_return_at20"])
                or float(other["overall_risk_mae_p90"]) < float(row["overall_risk_mae_p90"])
                or float(other["overall_coverage"]) > float(row["overall_coverage"])
            )
            if better_or_equal and strictly_better:
                dominated = True
                break
        hist.at[idx, "is_pareto"] = not dominated
    return hist


@contextmanager
def _history_file_lock(lock_path: Path, timeout_sec: float = HISTORY_LOCK_TIMEOUT_SEC):
    start = time.time()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            try:
                os.write(fd, str(os.getpid()).encode("utf-8"))
            finally:
                os.close(fd)
            break
        except FileExistsError:
            if (time.time() - start) >= float(timeout_sec):
                raise TimeoutError(f"history lock timeout: {lock_path}")
            time.sleep(float(HISTORY_LOCK_POLL_SEC))

    try:
        yield
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except TypeError:
            if lock_path.exists():
                lock_path.unlink()


def run_evaluate(paths: ResearchPaths, run_id: str) -> dict[str, Any]:
    run_dir = paths.run_dir(run_id)
    if not run_dir.exists():
        raise FileNotFoundError(f"run not found: {run_id}")

    manifest = read_json(run_dir / "manifest.json")
    cfg = from_dict(manifest.get("config") if isinstance(manifest.get("config"), dict) else {})
    snapshot_id = str(manifest.get("data_snapshot_id") or "")
    if not snapshot_id:
        raise ValueError("run manifest missing data_snapshot_id")

    rankings_long = read_csv(run_dir / "rankings_long.csv")
    rankings_short = read_csv(run_dir / "rankings_short.csv")
    top20_long_path = run_dir / "top20_long.csv"
    top20_short_path = run_dir / "top20_short.csv"
    selected_long = read_csv(top20_long_path) if top20_long_path.exists() else rankings_long
    selected_short = read_csv(top20_short_path) if top20_short_path.exists() else rankings_short
    split_info = manifest.get("split", {}) if isinstance(manifest.get("split"), dict) else {}
    label_load_end = split_info.get("test_end") or split_info.get("valid_end") or manifest.get("asof_date")
    labels = load_label_history(paths, cfg, snapshot_id, label_load_end)
    if labels.empty:
        raise ValueError("label history is empty for evaluation")
    labels["asof_date"] = pd.to_datetime(labels["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    universe = read_csv(paths.snapshot_dir(snapshot_id) / "universe_monthly.csv")
    expected_counts = _build_expected_counts(universe)

    metrics_by_phase: dict[str, dict[str, Any]] = {}
    monthly_parts: list[pd.DataFrame] = []
    for phase in ("valid", "test"):
        long_metrics, monthly_long = _metrics_for_side(
            side="long",
            rankings=selected_long,
            labels=labels,
            expected_by_month=expected_counts,
            top_k=cfg.model.top_k,
            phase=phase,
        )
        short_metrics, monthly_short = _metrics_for_side(
            side="short",
            rankings=selected_short,
            labels=labels,
            expected_by_month=expected_counts,
            top_k=cfg.model.top_k,
            phase=phase,
        )
        overall = _overall_metrics(long_metrics, short_metrics)
        metrics_by_phase[phase] = {
            "long": long_metrics,
            "short": short_metrics,
            "overall": overall,
        }
        monthly_parts.extend([monthly_long, monthly_short])

    valid_months = int(metrics_by_phase["valid"]["overall"]["months"])
    test_months = int(metrics_by_phase["test"]["overall"]["months"])
    if valid_months > 0 and test_months > 0:
        selection_phase = "valid_test_blend"
        selected_metrics = _blend_phase_metrics(
            metrics_by_phase["valid"],
            metrics_by_phase["test"],
            valid_weight=float(BLEND_VALID_WEIGHT),
        )
    elif valid_months > 0:
        selection_phase = "valid"
        selected_metrics = metrics_by_phase["valid"]
    else:
        selection_phase = "test"
        selected_metrics = metrics_by_phase["test"]
    overall = selected_metrics["overall"]

    history_file = paths.evaluations_root / "history.csv"
    history_lock = paths.evaluations_root / ".history.lock"
    with _history_file_lock(history_lock):
        if history_file.exists():
            history = read_csv(history_file)
        else:
            history = pd.DataFrame(
                columns=[
                    "run_id",
                    "created_at",
                    "snapshot_id",
                    "evaluation_phase",
                    "overall_hit_at20",
                    "overall_return_at20",
                    "overall_risk_mae_p90",
                    "overall_coverage",
                ]
            )
        if "evaluation_phase" not in history.columns:
            history["evaluation_phase"] = "test"

        current = pd.DataFrame(
            [
                {
                    "run_id": run_id,
                    "created_at": now_utc_iso(),
                    "snapshot_id": snapshot_id,
                    "evaluation_phase": selection_phase,
                    "overall_hit_at20": overall["hit_at20"],
                    "overall_return_at20": overall["return_at20"],
                    "overall_risk_mae_p90": overall["risk_mae_p90"],
                    "overall_coverage": overall["coverage"],
                }
            ]
        )
        if history.empty:
            history = current.copy()
        else:
            history = history[history["run_id"] != run_id]
            history = pd.concat([history, current], ignore_index=True)
        history = _compute_pareto(history)
        write_csv(history_file, history)

    current_row = history[history["run_id"] == run_id]
    is_pareto = bool(current_row["is_pareto"].iloc[0]) if not current_row.empty else False

    monthly = pd.concat(monthly_parts, ignore_index=True)
    if monthly.empty:
        monthly = pd.DataFrame(
            columns=[
                "run_id",
                "snapshot_id",
                "created_at",
                "phase",
                "side",
                "asof_date",
                "hit_at20",
                "return_at20",
                "mae_mean",
                "mae_p90",
                "coverage",
                "expected_symbols",
                "labeled_symbols",
                "predicted_symbols",
                "drawdown",
            ]
        )
    else:
        monthly.insert(0, "created_at", now_utc_iso())
        monthly.insert(0, "snapshot_id", snapshot_id)
        monthly.insert(0, "run_id", run_id)
        monthly = monthly.sort_values(["asof_date", "phase", "side"]).reset_index(drop=True)
    write_csv(run_dir / "evaluation_monthly.csv", monthly)

    summary = {
        "run_id": run_id,
        "created_at": now_utc_iso(),
        "snapshot_id": snapshot_id,
        "selection_phase": selection_phase,
        "selection_params": (
            {
                "valid_weight": float(BLEND_VALID_WEIGHT),
                "test_weight": float(1.0 - BLEND_VALID_WEIGHT),
            }
            if selection_phase == "valid_test_blend"
            else {}
        ),
        "metrics": {
            "long": selected_metrics["long"],
            "short": selected_metrics["short"],
            "overall": overall,
        },
        "metrics_by_phase": metrics_by_phase,
        "artifacts": {
            "evaluation_monthly": "evaluation_monthly.csv",
        },
        "pareto": {
            "is_pareto": is_pareto,
            "history_rows": int(len(history)),
            "pareto_runs": history.loc[history["is_pareto"] == True, "run_id"].astype(str).tolist(),  # noqa: E712
        },
    }
    write_json(run_dir / "evaluation.json", summary)
    write_json(paths.evaluations_root / f"{run_id}.json", summary)
    return {
        "ok": True,
        "run_id": run_id,
        "is_pareto": is_pareto,
        "selection_phase": selection_phase,
        "overall": overall,
    }
