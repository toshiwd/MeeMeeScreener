from __future__ import annotations

from datetime import datetime, timezone
import shutil
from typing import Any

import numpy as np
import pandas as pd

from research.config import ResearchConfig, config_to_dict, params_hash
from research.features import FEATURE_COLUMNS, build_features_for_asof, load_feature_history
from research.labels import build_labels_for_asof, load_label_history
from research.storage import (
    ResearchPaths,
    git_commit,
    now_utc_iso,
    parse_date,
    read_csv,
    read_json,
    write_csv,
    write_json,
    ymd,
)


def _split_months(months: list[str], config: ResearchConfig) -> dict[str, list[str]]:
    sorted_months = sorted(months)
    total = len(sorted_months)
    train_count = max(1, int(config.split.train_years) * 12)
    valid_count = max(1, int(config.split.valid_months))
    test_count = max(1, int(config.split.test_months))
    required = train_count + valid_count + test_count
    if total < required:
        raise ValueError(
            f"insufficient labeled months for fixed walkforward: need {required}, got {total}. "
            f"adjust split config (train_years/valid_months/test_months)."
        )

    test_start = total - test_count
    valid_start = test_start - valid_count
    train_start = valid_start - train_count

    train_months = sorted_months[train_start:valid_start]
    valid_months = sorted_months[valid_start:test_start]
    test_months = sorted_months[test_start:]

    if not train_months or not valid_months or not test_months:
        raise ValueError("split failed to produce train/valid/test")
    return {"train": train_months, "valid": valid_months, "test": test_months}


def _fit_ridge(X: np.ndarray, y: np.ndarray, alpha: float) -> tuple[float, np.ndarray]:
    if X.ndim != 2 or y.ndim != 1:
        raise ValueError("invalid dimensions for ridge fit")
    n = X.shape[0]
    X1 = np.concatenate([np.ones((n, 1), dtype=float), X], axis=1)
    reg = np.eye(X1.shape[1], dtype=float) * float(alpha)
    reg[0, 0] = 0.0
    lhs = X1.T @ X1 + reg
    rhs = X1.T @ y
    beta = np.linalg.solve(lhs, rhs)
    intercept = float(beta[0])
    coef = beta[1:].astype(float)
    return intercept, coef


def _sigmoid(z: np.ndarray) -> np.ndarray:
    clipped = np.clip(z, -35.0, 35.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _fit_logistic_1d(x: np.ndarray, y: np.ndarray, steps: int = 250, lr: float = 0.08, l2: float = 0.01) -> tuple[float, float]:
    w0 = 0.0
    w1 = 0.0
    for _ in range(steps):
        p = _sigmoid(w0 + w1 * x)
        err = p - y
        g0 = float(np.mean(err) + l2 * w0)
        g1 = float(np.mean(err * x) + l2 * w1)
        w0 -= lr * g0
        w1 -= lr * g1
    return float(w0), float(w1)


def _candidate_seed(frame: pd.DataFrame, side: str) -> pd.Series:
    if side == "long":
        return (
            0.45 * frame["ret20"].fillna(0.0)
            + 0.25 * frame["ret5"].fillna(0.0)
            + 0.20 * frame["breakout20"].fillna(0.0)
            + 0.10 * frame["vol_ratio20"].fillna(0.0)
        )
    return (
        0.45 * (-frame["ret20"].fillna(0.0))
        + 0.25 * (-frame["ret5"].fillna(0.0))
        + 0.20 * (-frame["breakdown20"].fillna(0.0))
        + 0.10 * frame["vol_ratio20"].fillna(0.0)
    )


def _topk_by_month(frame: pd.DataFrame, top_k: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    ranked = (
        frame.sort_values(["asof_date", "score"], ascending=[True, False])
        .groupby("asof_date", as_index=False, group_keys=False)
        .head(top_k)
        .reset_index(drop=True)
    )
    return ranked


def _train_side(
    side: str,
    side_df: pd.DataFrame,
    inference_features: pd.DataFrame,
    split: dict[str, list[str]],
    config: ResearchConfig,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    train_months = split["train"]
    valid_months = split["valid"]
    test_months = split["test"]

    train_df = side_df[side_df["asof_date"].isin(train_months)].copy()
    if train_df.empty:
        raise ValueError(f"no train rows for side={side}")

    medians: dict[str, float] = {}
    for col in FEATURE_COLUMNS:
        series = pd.to_numeric(train_df[col], errors="coerce")
        median = float(series.median()) if not series.dropna().empty else 0.0
        medians[col] = median

    def _to_matrix(frame: pd.DataFrame) -> np.ndarray:
        arr = np.zeros((len(frame), len(FEATURE_COLUMNS)), dtype=float)
        for i, col in enumerate(FEATURE_COLUMNS):
            arr[:, i] = pd.to_numeric(frame[col], errors="coerce").fillna(medians[col]).to_numpy(dtype=float)
        return arr

    X_train = _to_matrix(train_df)
    y_train = pd.to_numeric(train_df["realized_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    mean_x = X_train.mean(axis=0)
    std_x = X_train.std(axis=0)
    std_x = np.where(std_x < 1e-9, 1.0, std_x)
    X_train_z = (X_train - mean_x) / std_x

    intercept, coef = _fit_ridge(X_train_z, y_train, alpha=float(config.model.ridge_alpha))
    pred_train = intercept + X_train_z @ coef
    pred_mean = float(pred_train.mean())
    pred_std = float(pred_train.std() if pred_train.std() > 1e-9 else 1.0)
    pred_train_z = (pred_train - pred_mean) / pred_std
    tp_train = pd.to_numeric(train_df["tp_hit"], errors="coerce").fillna(0.0).clip(0.0, 1.0).to_numpy(dtype=float)
    logit_b0, logit_b1 = _fit_logistic_1d(pred_train_z, tp_train)

    risk_by_code = (
        train_df.groupby("code")["mae"].mean().replace([np.inf, -np.inf], np.nan).dropna().to_dict()
    )
    global_risk = float(pd.to_numeric(train_df["mae"], errors="coerce").fillna(0.0).mean())

    def _predict_phase(frame: pd.DataFrame, phase: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["asof_date", "code", "side", "phase", "score", "pred_return", "pred_prob_tp", "risk_dn"])
        tmp = frame.copy()
        tmp["candidate_seed"] = _candidate_seed(tmp, side)
        tmp = (
            tmp.sort_values(["asof_date", "candidate_seed"], ascending=[True, False])
            .groupby("asof_date", as_index=False, group_keys=False)
            .head(config.model.candidate_pool)
            .reset_index(drop=True)
        )
        X = _to_matrix(tmp)
        Xz = (X - mean_x) / std_x
        pred_return = intercept + Xz @ coef
        pred_z = (pred_return - pred_mean) / pred_std
        pred_prob = _sigmoid(logit_b0 + logit_b1 * pred_z)

        tmp["pred_return"] = pred_return.astype(float)
        tmp["pred_prob_tp"] = pred_prob.astype(float)
        tmp["risk_dn"] = tmp["code"].map(risk_by_code).fillna(global_risk).astype(float)
        tmp["score"] = (
            tmp["pred_return"] * (0.7 + 0.3 * tmp["pred_prob_tp"]) - float(config.model.risk_penalty) * tmp["risk_dn"]
        )
        tmp["side"] = side
        tmp["phase"] = phase
        keep = [
            "asof_date",
            "code",
            "side",
            "phase",
            "score",
            "pred_return",
            "pred_prob_tp",
            "risk_dn",
            "realized_return",
            "tp_hit",
            "mae",
            "mfe",
        ]
        for col in keep:
            if col not in tmp.columns:
                tmp[col] = np.nan
        return tmp[keep].copy()

    valid_pred = _predict_phase(side_df[side_df["asof_date"].isin(valid_months)].copy(), phase="valid")
    test_pred = _predict_phase(side_df[side_df["asof_date"].isin(test_months)].copy(), phase="test")
    infer_pred = _predict_phase(inference_features.copy(), phase="inference")

    rankings = pd.concat([valid_pred, test_pred, infer_pred], ignore_index=True)
    top20 = _topk_by_month(rankings, top_k=config.model.top_k)

    model = {
        "side": side,
        "feature_columns": list(FEATURE_COLUMNS),
        "medians": medians,
        "x_mean": mean_x.tolist(),
        "x_std": std_x.tolist(),
        "ridge_intercept": float(intercept),
        "ridge_coef": [float(x) for x in coef.tolist()],
        "pred_mean": float(pred_mean),
        "pred_std": float(pred_std),
        "logit_b0": float(logit_b0),
        "logit_b1": float(logit_b1),
        "risk_by_code_size": int(len(risk_by_code)),
        "global_risk": float(global_risk),
    }
    return model, rankings, top20


def run_train(
    paths: ResearchPaths,
    config: ResearchConfig,
    snapshot_id: str,
    asof_date: str,
    run_id: str,
    workers: int = 1,
    chunk_size: int = 120,
) -> dict[str, Any]:
    asof_ts = parse_date(asof_date)
    asof_str = ymd(asof_ts)

    sdir = paths.snapshot_dir(snapshot_id)
    if not sdir.exists():
        raise FileNotFoundError(f"snapshot not found: {snapshot_id}")
    snapshot_manifest_path = sdir / "manifest.json"
    snapshot_manifest = read_json(snapshot_manifest_path) if snapshot_manifest_path.exists() else {}
    calendar = read_csv(sdir / "calendar_month_ends.csv")
    universe = read_csv(sdir / "universe_monthly.csv")
    daily = read_csv(sdir / "daily.csv")
    calendar["asof_date"] = pd.to_datetime(calendar["asof_date"], errors="coerce").dt.normalize()
    universe["asof_date"] = pd.to_datetime(universe["asof_date"], errors="coerce").dt.normalize()
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce").dt.normalize()
    calendar = calendar.dropna(subset=["asof_date"]).sort_values("asof_date")
    universe = universe.dropna(subset=["asof_date", "code"]).copy()

    universe_asof = set(universe["asof_date"].dropna().drop_duplicates().tolist())
    if asof_ts not in set(calendar["asof_date"].tolist()):
        raise ValueError(f"asof not found in month-end calendar: {asof_str}")
    if asof_ts not in universe_asof:
        raise ValueError(f"asof not found in universe snapshot: {asof_str}")

    all_asof = sorted(
        [
            ts
            for ts in calendar["asof_date"].drop_duplicates().tolist()
            if ts <= asof_ts and ts in universe_asof
        ]
    )
    if not all_asof:
        raise ValueError("no month-end dates up to asof")

    max_daily = pd.to_datetime(daily["date"], errors="coerce").dropna().max()
    calendar_asof = calendar["asof_date"].dropna().drop_duplicates().sort_values().reset_index(drop=True)
    for dt in all_asof:
        build_features_for_asof(
            paths,
            config,
            snapshot_id,
            ymd(dt),
            force=False,
            workers=workers,
            chunk_size=chunk_size,
        )
        next_month_candidates = calendar_asof[calendar_asof > pd.Timestamp(dt)]
        next_month_end = pd.Timestamp(next_month_candidates.iloc[0]).normalize() if not next_month_candidates.empty else None
        if dt < asof_ts and next_month_end is not None and next_month_end <= max_daily:
            label_result = build_labels_for_asof(
                paths,
                config,
                snapshot_id,
                ymd(dt),
                force=False,
                workers=workers,
                chunk_size=chunk_size,
            )
            if int(label_result.get("rows", 0)) == 0:
                build_labels_for_asof(
                    paths,
                    config,
                    snapshot_id,
                    ymd(dt),
                    force=True,
                    workers=workers,
                    chunk_size=chunk_size,
                )

    feature_hist = load_feature_history(paths, config, snapshot_id, asof_str)
    label_hist = load_label_history(paths, config, snapshot_id, asof_str)

    if feature_hist.empty:
        raise ValueError("feature history is empty")
    if label_hist.empty:
        raise ValueError("label history is empty")

    feature_hist["asof_date"] = pd.to_datetime(feature_hist["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    label_hist["asof_date"] = pd.to_datetime(label_hist["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    train_labels = label_hist[label_hist["asof_date"] < asof_str].copy()
    merged = feature_hist.merge(train_labels, on=["asof_date", "code"], how="inner")
    if merged.empty:
        raise ValueError("merged feature/label dataset is empty")

    inference_features = feature_hist[feature_hist["asof_date"] == asof_str][["asof_date", "code", *FEATURE_COLUMNS]].copy()
    if inference_features.empty:
        raise ValueError(f"inference features missing for asof={asof_str}")

    months = sorted(merged["asof_date"].dropna().unique().tolist())
    split = _split_months(months, config)

    long_df = merged[merged["side"] == "long"].copy()
    short_df = merged[merged["side"] == "short"].copy()
    if long_df.empty or short_df.empty:
        raise ValueError("one side has zero training rows")

    model_long, rankings_long, top20_long = _train_side("long", long_df, inference_features, split, config)
    model_short, rankings_short, top20_short = _train_side("short", short_df, inference_features, split, config)

    run_dir = paths.run_dir(run_id)
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    write_json(run_dir / "model_long.json", model_long)
    write_json(run_dir / "model_short.json", model_short)
    write_csv(run_dir / "rankings_long.csv", rankings_long)
    write_csv(run_dir / "rankings_short.csv", rankings_short)
    write_csv(run_dir / "top20_long.csv", top20_long)
    write_csv(run_dir / "top20_short.csv", top20_short)

    split_summary = {
        "mode": "fixed_walkforward",
        "train_months": split["train"],
        "valid_months": split["valid"],
        "test_months": split["test"],
        "train_start": split["train"][0],
        "train_end": split["train"][-1],
        "valid_start": split["valid"][0],
        "valid_end": split["valid"][-1],
        "test_start": split["test"][0],
        "test_end": split["test"][-1],
    }

    manifest = {
        "run_id": run_id,
        "created_at": now_utc_iso(),
        "asof_date": asof_str,
        "data_snapshot_id": snapshot_id,
        "data_snapshot_manifest": snapshot_manifest,
        "git_commit": git_commit(paths.repo_root),
        "params_hash": params_hash(config),
        "cache_key": str(paths.cache_dir(snapshot_id, config.feature_version, config.label_version, params_hash(config)).name),
        "feature_version": config.feature_version,
        "label_version": config.label_version,
        "model_version": config.model_version,
        "execution": {
            "workers": int(max(1, workers)),
            "chunk_size": int(max(1, chunk_size)),
        },
        "config": config_to_dict(config),
        "split": split_summary,
        "row_counts": {
            "merged_total": int(len(merged)),
            "long_rows": int(len(long_df)),
            "short_rows": int(len(short_df)),
            "inference_rows": int(len(inference_features)),
        },
        "artifacts": {
            "model_long": "model_long.json",
            "model_short": "model_short.json",
            "rankings_long": "rankings_long.csv",
            "rankings_short": "rankings_short.csv",
            "top20_long": "top20_long.csv",
            "top20_short": "top20_short.csv",
        },
    }
    write_json(run_dir / "manifest.json", manifest)

    return {
        "ok": True,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "asof_date": asof_str,
        "snapshot_id": snapshot_id,
        "train_months": len(split["train"]),
        "valid_months": len(split["valid"]),
        "test_months": len(split["test"]),
        "top20_long_rows": int(len(top20_long[top20_long["phase"] == "inference"])),
        "top20_short_rows": int(len(top20_short[top20_short["phase"] == "inference"])),
    }
