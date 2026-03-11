from __future__ import annotations

from datetime import datetime, timezone
import itertools
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


def _try_import_lightgbm() -> Any:
    """LightGBMをインポートし、失敗時はNoneを返す."""
    try:
        import lightgbm as lgb  # type: ignore
        return lgb
    except ModuleNotFoundError:
        return None


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



# Regime feature columns (relative subset - market-level features)
REGIME_FEATURE_COLS: tuple[str, ...] = (
    "market_breadth_ma20",
    "market_breadth_52wk",
    "market_ret20_rank",
    "vol_regime",
    "market_trend_state",
    "ma_align_bull",
    "ma_align_bear",
    "rsi14",
    "rv_ratio",
)


def _fit_regime_classifier(
    lgb: Any,
    merged_df: pd.DataFrame,
) -> Any | None:
    """相場レジームを5段階分類するLightGBMモデルを学習.
    
    Regime labels are derived from market_trend_state + vol_regime:
      0=strong_bear, 1=mild_bear, 2=sideways, 3=mild_bull, 4=strong_bull
    
    Returns None if any required col is missing or data is insufficient.
    """
    avail = [c for c in REGIME_FEATURE_COLS if c in merged_df.columns]
    if len(avail) < 3:
        return None

    # Pseudo-label: combine market_trend_state(0/1/2) and vol_regime(0/1/2)
    # Map to 5 classes
    mt = pd.to_numeric(merged_df.get("market_trend_state", 1.0), errors="coerce").fillna(1.0).clip(0, 2)
    vr = pd.to_numeric(merged_df.get("vol_regime", 1.0), errors="coerce").fillna(1.0).clip(0, 2)
    # regime_label: higher = more bullish / less volatile
    regime_raw = mt * 1.5 + (2.0 - vr) * 0.5  # 0..4
    regime_label = pd.cut(
        regime_raw, bins=[-np.inf, 0.8, 1.6, 2.4, 3.2, np.inf], labels=[0, 1, 2, 3, 4]
    ).astype(float).fillna(2.0).astype(int)

    X = np.column_stack([
        pd.to_numeric(merged_df[c], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        for c in avail
    ])
    y = regime_label.to_numpy(dtype=int)

    if len(np.unique(y)) < 2 or len(X) < 50:
        return None

    params = {
        "objective": "multiclass",
        "num_class": 5,
        "n_estimators": 200,
        "learning_rate": 0.05,
        "max_depth": 4,
        "num_leaves": 15,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_lambda": 1.0,
        "random_state": 42,
        "verbose": -1,
        "n_jobs": -1,
    }
    try:
        clf = lgb.LGBMClassifier(**params)
        clf.fit(X, y)
        print(f"    [Regime] trained on {len(X)} rows, {len(avail)} features, {len(np.unique(y))} classes")
        return clf
    except Exception as e:
        print(f"    [Regime] training failed: {e}")
        return None


def _predict_regime_proba(
    regime_clf: Any | None,
    frame: pd.DataFrame,
) -> np.ndarray:
    """regime_clfがあればレジーム確率(5次元)を返す. なければゼロ行列."""
    n = len(frame)
    if regime_clf is None or n == 0:
        return np.zeros((n, 5), dtype=float)
    avail = [c for c in REGIME_FEATURE_COLS if c in frame.columns]
    if not avail:
        return np.zeros((n, 5), dtype=float)
    X = np.column_stack([
        pd.to_numeric(frame[c], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        for c in avail
    ])
    try:
        proba = regime_clf.predict_proba(X).astype(float)
        if proba.shape[1] < 5:
            pad = np.zeros((n, 5 - proba.shape[1]), dtype=float)
            proba = np.concatenate([proba, pad], axis=1)
        return proba[:, :5]
    except Exception:
        return np.zeros((n, 5), dtype=float)


def _get_lgbm_base_params(config: ResearchConfig, objective: str = "regression") -> dict[str, Any]:
    """共通のLightGBMパラメータを返す. config.modelの設定を反映."""
    n_est = getattr(config.model, "lgbm_n_estimators", 1000)
    lr = getattr(config.model, "lgbm_learning_rate", 0.02)
    max_d = getattr(config.model, "lgbm_max_depth", 6)
    return {
        "objective": objective,
        "n_estimators": max(100, int(n_est)),
        "learning_rate": float(lr),
        "max_depth": int(max_d),
        "num_leaves": min(127, 2 ** int(max_d) - 1),
        "min_child_samples": 20,          # 過学習防止: 葉に必要な最小サンプル数
        "min_child_weight": 1e-3,
        "subsample": 0.7,                  # 行サンプリング (bagging)
        "subsample_freq": 1,
        "colsample_bytree": 0.7,           # 列サンプリング
        "reg_alpha": 0.05,                 # L1正則化
        "reg_lambda": 1.0,                 # L2正則化 (強め)
        "path_smooth": 0.1,                # スムージング
        "extra_trees": False,
        "random_state": 42,
        "verbose": -1,
        "n_jobs": -1,
    }


def _fit_with_early_stopping(
    lgb: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    params: dict[str, Any],
    early_stopping_rounds: int = 50,
    is_classifier: bool = False,
) -> tuple[Any, int]:
    """Early stoppingを使ってLightGBMを学習. 最適n_estimatorsを自動検出.

    Returns: (fitted_model, best_n_estimators)
    """
    if is_classifier:
        model = lgb.LGBMClassifier(**params)
    else:
        model = lgb.LGBMRegressor(**params)

    callbacks = [lgb.early_stopping(stopping_rounds=early_stopping_rounds, verbose=False)]
    eval_set = [(X_val, y_val)]
    model.fit(
        X_train,
        y_train,
        eval_set=eval_set,
        callbacks=callbacks,
    )
    best_iter = getattr(model, "best_iteration_", params.get("n_estimators", 500))
    if best_iter <= 0:
        best_iter = params.get("n_estimators", 500)
    return model, int(best_iter)


def _fit_gbdt_regressor(
    lgb: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    config: ResearchConfig,
    X_val: np.ndarray | None = None,
    y_val: np.ndarray | None = None,
) -> Any:
    """LightGBMリグレッサーで期待リターンを学習.
    X_val/y_valが与えられればEarly Stoppingを使用し最適な木の数を自動決定.
    """
    params = _get_lgbm_base_params(config, objective="regression_l1")
    # Huber loss + MAEの組み合わせでリターンの外れ値に頑健に
    # "regression_l1" = MAE (中央値予測、外れ値に強い)
    # "huber" も良い選択肢

    use_dart = getattr(config.model, "lgbm_use_dart", False)
    if use_dart:
        params["boosting_type"] = "dart"
        params["drop_rate"] = 0.1
        params["skip_drop"] = 0.5

    if X_val is not None and y_val is not None and len(X_val) >= 10 and not use_dart:
        model, best_iter = _fit_with_early_stopping(
            lgb, X_train, y_train, X_val, y_val, params,
            early_stopping_rounds=60,
            is_classifier=False,
        )
        print(f"    [LGB-Reg] early_stop @ {best_iter} trees  (n_train={len(X_train)}, n_val={len(X_val)})")
    else:
        model = lgb.LGBMRegressor(**params)
        model.fit(X_train, y_train)
        best_iter = params["n_estimators"]
        print(f"    [LGB-Reg] full fit @ {best_iter} trees  (n_train={len(X_train)})")
    return model


def _fit_gbdt_classifier(
    lgb: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    config: ResearchConfig,
    X_val: np.ndarray | None = None,
    y_val: np.ndarray | None = None,
) -> Any:
    """LightGBMクラシファイアでTP確率を学習.
    クラス不均衡に対応するためscale_pos_weightを自動計算.
    """
    pos = int(np.sum(y_train > 0.5))
    neg = int(len(y_train)) - pos
    scale_pos = max(1.0, neg / max(pos, 1))

    params = _get_lgbm_base_params(config, objective="binary")
    params["scale_pos_weight"] = scale_pos
    params["is_unbalance"] = False  # scale_pos_weightを優先

    use_dart = getattr(config.model, "lgbm_use_dart", False)
    if use_dart:
        params["boosting_type"] = "dart"
        params["drop_rate"] = 0.1

    if X_val is not None and y_val is not None and len(X_val) >= 10 and not use_dart:
        model, best_iter = _fit_with_early_stopping(
            lgb, X_train, y_train.astype(int), X_val, y_val.astype(int), params,
            early_stopping_rounds=60,
            is_classifier=True,
        )
        print(f"    [LGB-Cls] early_stop @ {best_iter} trees  (pos_rate={pos/len(y_train):.2%})")
    else:
        model = lgb.LGBMClassifier(**params)
        model.fit(X_train, y_train.astype(int))
        print(f"    [LGB-Cls] full fit  (pos_rate={pos/len(y_train):.2%})")
    return model


def _try_optuna_tune(
    lgb: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    n_trials: int = 30,
) -> dict[str, Any]:
    """Optunaによるハイパーパラメータ探索. 未インストール時はデフォルト値を返す."""
    try:
        import optuna  # type: ignore
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ModuleNotFoundError:
        return {}

    def _objective(trial: Any) -> float:
        params = {
            "objective": "regression_l1",
            "n_estimators": 2000,
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.1, log=True),
            "max_depth": trial.suggest_int("max_depth", 3, 8),
            "num_leaves": trial.suggest_int("num_leaves", 15, 127),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 60),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "subsample_freq": 1,
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-4, 1.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 0.1, 10.0, log=True),
            "path_smooth": trial.suggest_float("path_smooth", 0.0, 0.5),
            "random_state": 42,
            "verbose": -1,
            "n_jobs": -1,
        }
        model = lgb.LGBMRegressor(**params)
        cb = [lgb.early_stopping(50, verbose=False)]
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], callbacks=cb)
        preds = model.predict(X_val).astype(float)
        # IC (情報係数) = スピアマン相関で最適化
        from scipy.stats import spearmanr  # type: ignore
        corr, _ = spearmanr(y_val, preds)
        return -float(corr) if not np.isnan(corr) else 0.0

    try:
        study = optuna.create_study(direction="minimize")
        study.optimize(_objective, n_trials=n_trials, show_progress_bar=False)
        best = study.best_params
        print(f"    [Optuna] best IC={-study.best_value:.4f} after {n_trials} trials")
        return best
    except Exception as e:
        print(f"    [Optuna] failed: {e}")
        return {}


def _walkforward_oos_stats(
    side_df: pd.DataFrame,
    feature_cols: list[str],
    months: list[str],
    config: ResearchConfig,
) -> dict[str, Any]:
    """Walk-forward OOS検証: 各月を1つのテスト月として順番に評価."""
    lgb = _try_import_lightgbm()
    use_lgb = lgb is not None and getattr(config.model, "use_lightgbm", True)
    min_train = 12  # 最低12ヶ月の学習データが必要
    oos_records: list[dict[str, Any]] = []

    for test_idx in range(min_train, len(months)):
        test_month = months[test_idx]
        train_months_wf = months[:test_idx]
        train_df = side_df[side_df["asof_date"].isin(train_months_wf)].copy()
        test_df = side_df[side_df["asof_date"] == test_month].copy()
        if train_df.empty or test_df.empty:
            continue

        # 訓練データ準備
        medians_wf: dict[str, float] = {}
        arr_parts: list[np.ndarray] = []
        for col in feature_cols:
            series = pd.to_numeric(train_df[col], errors="coerce")
            med = float(series.median()) if not series.dropna().empty else 0.0
            medians_wf[col] = med
            arr_parts.append(pd.to_numeric(train_df[col], errors="coerce").fillna(med).to_numpy(dtype=float))
        X_tr = np.column_stack(arr_parts) if arr_parts else np.zeros((len(train_df), 1))
        y_tr = pd.to_numeric(train_df["realized_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        tp_tr = pd.to_numeric(train_df["tp_hit"], errors="coerce").fillna(0.0).clip(0, 1).to_numpy(dtype=float)

        # テストデータ準備
        arr_test: list[np.ndarray] = []
        for col in feature_cols:
            arr_test.append(pd.to_numeric(test_df[col], errors="coerce").fillna(medians_wf.get(col, 0.0)).to_numpy(dtype=float))
        X_te = np.column_stack(arr_test) if arr_test else np.zeros((len(test_df), 1))

        if use_lgb:
            reg = _fit_gbdt_regressor(lgb, X_tr, y_tr, config)
            pred_ret = reg.predict(X_te).astype(float)
            if len(np.unique(tp_tr)) >= 2:
                clf = _fit_gbdt_classifier(lgb, X_tr, tp_tr.astype(int), config)
                pred_prob = clf.predict_proba(X_te)[:, 1].astype(float)
            else:
                pred_prob = np.full(len(test_df), float(tp_tr.mean()))
        else:
            # Ridge fallback
            mean_x = X_tr.mean(axis=0)
            std_x = np.where(X_tr.std(axis=0) < 1e-9, 1.0, X_tr.std(axis=0))
            Xz_tr = (X_tr - mean_x) / std_x
            intercept, coef = _fit_ridge(Xz_tr, y_tr, alpha=float(config.model.ridge_alpha))
            Xz_te = (X_te - mean_x) / std_x
            pred_ret = (intercept + Xz_te @ coef).astype(float)
            pred_prob = np.full(len(test_df), 0.5)

        actual_ret = pd.to_numeric(test_df["realized_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        win_rate = float(np.mean(actual_ret > 0))
        mean_ret = float(np.mean(actual_ret))
        oos_records.append({
            "month": test_month,
            "n": len(test_df),
            "win_rate": win_rate,
            "mean_ret": mean_ret,
        })

    if not oos_records:
        return {"oos_months": 0, "oos_mean_ret": None, "oos_win_rate": None, "records": []}

    all_wins = [r["win_rate"] for r in oos_records if r["n"] > 0]
    all_rets = [r["mean_ret"] for r in oos_records if r["n"] > 0]
    return {
        "oos_months": len(oos_records),
        "oos_mean_ret": float(np.mean(all_rets)) if all_rets else None,
        "oos_win_rate": float(np.mean(all_wins)) if all_wins else None,
        "records": oos_records,
    }


def _topk_by_month(
    frame: pd.DataFrame,
    top_k: int,
    side: str | None = None,
    short_regime_caps: dict[str, int] | None = None,
    short_month_gate: dict[str, Any] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    src = frame.sort_values(["asof_date", "score"], ascending=[True, False]).copy()
    if side != "short":
        ranked = (
            src.groupby("asof_date", as_index=False, group_keys=False)
            .head(top_k)
            .reset_index(drop=True)
        )
        return ranked

    caps = short_regime_caps if isinstance(short_regime_caps, dict) else {}
    gate = short_month_gate if isinstance(short_month_gate, dict) else {}
    gate_enabled = bool(gate.get("enabled", False))
    allowed_regimes = {str(x).strip() for x in (gate.get("allowed_regimes", []) or []) if str(x).strip()}
    pred_return_max_raw = gate.get("pred_return_max")
    prob_min_raw = gate.get("prob_min")
    risk_max_raw = gate.get("risk_max")
    pred_return_max = float(pred_return_max_raw) if pred_return_max_raw is not None else None
    prob_min = float(prob_min_raw) if prob_min_raw is not None else None
    risk_max = float(risk_max_raw) if risk_max_raw is not None else None

    rows: list[pd.DataFrame] = []
    for asof, grp in src.groupby("asof_date", as_index=False, sort=True):
        k = int(top_k)
        regime_mode = ""
        mt_key = ""
        if "regime_key" in grp.columns and not grp["regime_key"].dropna().empty:
            regime_mode = str(grp["regime_key"].dropna().astype(str).mode().iloc[0])
            mt_key = regime_mode.split("_", 1)[0] if "_" in regime_mode else regime_mode
            cap = caps.get(regime_mode)
            if cap is None:
                cap = caps.get(mt_key)
            if cap is None:
                cap = caps.get("*")
            if cap is not None:
                k = max(0, min(int(top_k), int(cap)))
        if k <= 0:
            continue

        month_pick = grp.head(k).copy()
        if gate_enabled:
            if allowed_regimes:
                matched = (
                    regime_mode in allowed_regimes
                    or mt_key in allowed_regimes
                    or "*" in allowed_regimes
                )
                if not matched:
                    continue
            if pred_return_max is not None and "pred_return" in month_pick.columns:
                mean_pred_return = float(pd.to_numeric(month_pick["pred_return"], errors="coerce").mean())
                if np.isfinite(mean_pred_return) and mean_pred_return > float(pred_return_max):
                    continue
            if prob_min is not None and "pred_prob_tp" in month_pick.columns:
                mean_prob = float(pd.to_numeric(month_pick["pred_prob_tp"], errors="coerce").mean())
                if np.isfinite(mean_prob) and mean_prob < float(prob_min):
                    continue
            if risk_max is not None and "risk_dn" in month_pick.columns:
                mean_risk = float(pd.to_numeric(month_pick["risk_dn"], errors="coerce").mean())
                if np.isfinite(mean_risk) and mean_risk > float(risk_max):
                    continue
        rows.append(month_pick)
    if not rows:
        return src.iloc[0:0].copy().reset_index(drop=True)
    ranked = pd.concat(rows, ignore_index=True).reset_index(drop=True)
    return ranked


def _max_drawdown_returns(returns: pd.Series) -> float:
    vals = pd.to_numeric(returns, errors="coerce").dropna()
    if vals.empty:
        return 0.0
    equity = (1.0 + vals).cumprod()
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(max(0.0, -float(dd.min())))


def _regime_key(frame: pd.DataFrame) -> pd.Series:
    mt = pd.to_numeric(frame.get("market_trend_state", 1.0), errors="coerce").fillna(1.0).clip(0, 2).round().astype(int)
    vr = pd.to_numeric(frame.get("vol_regime", 1.0), errors="coerce").fillna(1.0).clip(0, 2).round().astype(int)
    return "mt" + mt.astype(str) + "_vr" + vr.astype(str)


def _score_objective(
    frame: pd.DataFrame,
    top_k: int,
    side: str,
    short_regime_caps: dict[str, int] | None = None,
    short_month_gate: dict[str, Any] | None = None,
) -> float:
    if frame.empty or "realized_return" not in frame.columns:
        return float("-inf")
    ranked = _topk_by_month(
        frame,
        top_k=top_k,
        side=side,
        short_regime_caps=short_regime_caps,
        short_month_gate=short_month_gate,
    )
    if ranked.empty:
        return float("-inf")
    monthly = (
        ranked.groupby("asof_date", as_index=False)["realized_return"]
        .mean()
        .rename(columns={"realized_return": "ret"})
    )
    ret = pd.to_numeric(monthly["ret"], errors="coerce").dropna()
    if ret.empty:
        return float("-inf")
    ret_series = pd.Series(ret.to_numpy(dtype=float))
    mean_ret = float(ret.mean())
    vol = float(ret.std(ddof=0))
    win_rate = float((ret > 0.0).mean())
    if side == "short":
        # Short-side objective emphasizes downside control to avoid a few large losses
        # dominating expected value even when monthly win-rate looks acceptable.
        p25 = float(np.quantile(ret_series.to_numpy(dtype=float), 0.25))
        mdd = _max_drawdown_returns(ret_series)
        downside_penalty = max(0.0, -p25)
        return float(
            mean_ret
            - 0.30 * vol
            - 0.45 * downside_penalty
            - 0.20 * mdd
            + 0.003 * win_rate
        )
    return float(mean_ret - 0.20 * vol + 0.001 * win_rate)


def _normalize_short_month_gate(raw: dict[str, Any] | None) -> dict[str, Any]:
    gate_raw = raw if isinstance(raw, dict) else {}
    allowed = []
    for item in (gate_raw.get("allowed_regimes", []) or []):
        key = str(item).strip()
        if key:
            allowed.append(key)
    pred_return_max_raw = gate_raw.get("pred_return_max")
    prob_min_raw = gate_raw.get("prob_min")
    risk_max_raw = gate_raw.get("risk_max")
    return {
        "enabled": bool(gate_raw.get("enabled", False)),
        "allowed_regimes": sorted(set(allowed)),
        "pred_return_max": float(pred_return_max_raw) if pred_return_max_raw is not None else None,
        "prob_min": float(prob_min_raw) if prob_min_raw is not None else None,
        "risk_max": float(risk_max_raw) if risk_max_raw is not None else None,
    }


def _learn_short_month_gate(
    valid_frame: pd.DataFrame,
    config: ResearchConfig,
    top_k: int,
    short_regime_caps: dict[str, int] | None,
    base_gate: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    src = valid_frame.copy()
    src = src.dropna(subset=["realized_return"]).copy()
    if src.empty:
        gate = _normalize_short_month_gate(base_gate)
        return gate, {"enabled": False, "auto": True, "reason": "empty_valid"}
    if "regime_key" not in src.columns:
        src["regime_key"] = _regime_key(src)

    base = _normalize_short_month_gate(base_gate)
    min_improve = max(0.0, float(getattr(config.model, "short_month_gate_auto_min_improvement", 0.0002)))
    min_months = max(1, int(getattr(config.model, "short_month_gate_auto_min_months", 6)))
    max_regimes = max(1, int(getattr(config.model, "short_month_gate_auto_max_regimes", 4)))
    months_sorted = sorted(src["asof_date"].astype(str).dropna().unique().tolist())
    holdout_n = max(2, min(12, max(2, len(months_sorted) // 4)))
    holdout_months = set(months_sorted[-holdout_n:]) if len(months_sorted) > holdout_n else set(months_sorted)
    fit_months = set(months_sorted) - holdout_months
    if not fit_months:
        fit_months = set(months_sorted)

    keys_all = [str(k).strip() for k in src["regime_key"].dropna().astype(str).unique().tolist() if str(k).strip()]
    key_counts = src["regime_key"].astype(str).value_counts(dropna=False).to_dict()
    keys = [k for k in sorted(keys_all) if int(key_counts.get(k, 0)) >= 20]
    if not keys:
        return base, {"enabled": bool(base.get("enabled", False)), "auto": True, "reason": "no_regimes"}

    allowed_sets: list[list[str]] = []
    max_r = min(max_regimes, len(keys))
    for r in range(1, max_r + 1):
        for combo in itertools.combinations(keys, r):
            allowed_sets.append(list(combo))
    if base.get("allowed_regimes"):
        allowed_sets.append(list(base.get("allowed_regimes", [])))
    allowed_sets.append(keys)

    def _uniq_num(vals: list[float]) -> list[float]:
        return sorted(set([round(float(v), 6) for v in vals]))

    ret_vals = _uniq_num(
        [
            -0.012,
            -0.010,
            -0.008,
            -0.006,
            float(base["pred_return_max"]) if base.get("pred_return_max") is not None else -0.006,
        ]
    )
    prob_vals = _uniq_num(
        [
            0.20,
            0.22,
            0.23,
            0.24,
            float(base["prob_min"]) if base.get("prob_min") is not None else 0.22,
        ]
    )
    risk_vals = _uniq_num(
        [
            0.050,
            0.055,
            float(base["risk_max"]) if base.get("risk_max") is not None else 0.055,
        ]
    )

    def _gate_month_count(frame: pd.DataFrame, gate: dict[str, Any]) -> int:
        ranked = _topk_by_month(
            frame,
            top_k=top_k,
            side="short",
            short_regime_caps=short_regime_caps,
            short_month_gate=gate,
        )
        if ranked.empty:
            return 0
        return int(ranked["asof_date"].astype(str).nunique())

    def _robust_obj(gate: dict[str, Any]) -> tuple[float, float, float, int, int]:
        fit = src[src["asof_date"].astype(str).isin(fit_months)]
        hold = src[src["asof_date"].astype(str).isin(holdout_months)]
        fit_obj = _score_objective(
            fit,
            top_k=top_k,
            side="short",
            short_regime_caps=short_regime_caps,
            short_month_gate=gate,
        )
        hold_obj = _score_objective(
            hold,
            top_k=top_k,
            side="short",
            short_regime_caps=short_regime_caps,
            short_month_gate=gate,
        )
        robust = float(0.65 * fit_obj + 0.35 * hold_obj)
        fit_n = _gate_month_count(fit, gate)
        hold_n = _gate_month_count(hold, gate)
        return robust, float(fit_obj), float(hold_obj), int(fit_n), int(hold_n)

    baseline_gate = dict(base)
    baseline_gate["enabled"] = bool(base.get("enabled", False))
    base_obj, base_fit_obj, base_hold_obj, base_fit_n, base_hold_n = _robust_obj(baseline_gate)
    best_gate = dict(baseline_gate)
    best_obj = float(base_obj)
    best_fit_obj = float(base_fit_obj)
    best_hold_obj = float(base_hold_obj)
    best_fit_n = int(base_fit_n)
    best_hold_n = int(base_hold_n)
    tried = 0
    accepted = 0

    need_hold = max(1, min_months // 3)
    for allowed in allowed_sets:
        allowed_norm = sorted(set([str(x).strip() for x in allowed if str(x).strip()]))
        if not allowed_norm:
            continue
        for ret_max in ret_vals:
            for prob_min in prob_vals:
                for risk_max in risk_vals:
                    tried += 1
                    trial = {
                        "enabled": True,
                        "allowed_regimes": allowed_norm,
                        "pred_return_max": float(ret_max),
                        "prob_min": float(prob_min),
                        "risk_max": float(risk_max),
                    }
                    obj, fit_obj, hold_obj, fit_n, hold_n = _robust_obj(trial)
                    if fit_n < min_months or hold_n < need_hold:
                        continue
                    if obj < best_obj + min_improve:
                        continue
                    if hold_obj < best_hold_obj - max(0.00025, min_improve * 0.5):
                        continue
                    best_gate = dict(trial)
                    best_obj = float(obj)
                    best_fit_obj = float(fit_obj)
                    best_hold_obj = float(hold_obj)
                    best_fit_n = int(fit_n)
                    best_hold_n = int(hold_n)
                    accepted += 1

    summary = {
        "enabled": bool(best_gate.get("enabled", False)),
        "auto": True,
        "baseline_objective": float(base_obj),
        "baseline_objective_fit": float(base_fit_obj),
        "baseline_objective_holdout": float(base_hold_obj),
        "baseline_fit_months": int(base_fit_n),
        "baseline_holdout_months": int(base_hold_n),
        "final_objective": float(best_obj),
        "final_objective_fit": float(best_fit_obj),
        "final_objective_holdout": float(best_hold_obj),
        "final_fit_months": int(best_fit_n),
        "final_holdout_months": int(best_hold_n),
        "objective_gain": float(best_obj - base_obj),
        "tried": int(tried),
        "accepted_updates": int(accepted),
        "holdout_months": sorted(list(holdout_months)),
        "selected_gate": best_gate,
    }
    return best_gate, summary


def _profile_grid(side: str, default_profile: dict[str, float]) -> list[dict[str, float]]:
    rb0 = float(default_profile["score_return_base"])
    pw0 = float(default_profile["score_prob_weight"])
    pa0 = float(default_profile.get("score_prob_alpha", 0.0))
    rs0 = float(default_profile["score_risk_scale"])
    oh0 = float(default_profile.get("overheat_penalty_scale", 1.0))
    mb0 = float(default_profile.get("short_mt_bear_bonus", 0.0))
    mu0 = float(default_profile.get("short_mt_bull_penalty", 0.0))
    v10 = float(default_profile.get("short_vr1_penalty", 0.0))
    v20 = float(default_profile.get("short_vr2_bonus", 0.0))

    def _uniq(vals: list[float]) -> list[float]:
        return sorted(set([round(float(v), 6) for v in vals]))

    if side == "long":
        rb_vals = _uniq([max(0.20, rb0 - 0.15), rb0, rb0 + 0.15])
        pw_vals = _uniq([max(0.00, pw0 - 0.10), pw0, pw0 + 0.10])
        pa_vals = [0.0]
        rs_vals = _uniq([max(0.30, rs0 * 0.75), rs0, rs0 * 1.25, rs0 * 1.50])
        oh_vals = _uniq([max(0.30, oh0 * 0.75), oh0, oh0 * 1.25, oh0 * 1.50])
        mu_vals = [0.0]
        v1_vals = [0.0]
    else:
        rb_vals = _uniq([max(0.20, rb0 - 0.10), rb0, rb0 + 0.10])
        pw_vals = _uniq([max(0.00, pw0 - 0.10), pw0, pw0 + 0.10])
        pa_vals = _uniq([pa0 - 0.010, pa0, pa0 + 0.010])
        rs_vals = _uniq([max(0.50, rs0 * 0.75), rs0, rs0 * 1.25, rs0 * 1.50])
        oh_vals = [0.0]
        mu_vals = _uniq([max(0.0, mu0 * 0.50), mu0, mu0 * 1.50]) if mu0 > 0 else [0.0]
        v1_vals = _uniq([max(0.0, v10 * 0.50), v10, v10 * 1.50]) if v10 > 0 else [0.0]

    out: list[dict[str, float]] = []
    for rb in rb_vals:
        for pw in pw_vals:
            for pa in pa_vals:
                for rs in rs_vals:
                    for oh in oh_vals:
                        for mu in mu_vals:
                            for v1 in v1_vals:
                                out.append(
                                    {
                                        "score_return_base": float(rb),
                                        "score_prob_weight": float(pw),
                                        "score_prob_alpha": float(pa),
                                        "score_risk_scale": float(rs),
                                        "overheat_penalty_scale": float(oh),
                                        "short_mt_bear_bonus": float(mb0),
                                        "short_mt_bull_penalty": float(mu),
                                        "short_vr1_penalty": float(v1),
                                        "short_vr2_bonus": float(v20),
                                    }
                                )
    return out


def _apply_strategy_score(
    frame: pd.DataFrame,
    side: str,
    risk_penalty: float,
    default_profile: dict[str, float],
    strategy_profiles: dict[str, dict[str, float]] | None = None,
) -> pd.DataFrame:
    if frame.empty:
        out = frame.copy()
        out["regime_key"] = pd.Series(dtype=str)
        out["strategy_profile"] = pd.Series(dtype=str)
        out["score"] = pd.Series(dtype=float)
        return out

    tmp = frame.copy()
    tmp["regime_key"] = _regime_key(tmp)
    profiles = strategy_profiles or {}

    ret_base = pd.Series(float(default_profile["score_return_base"]), index=tmp.index, dtype=float)
    prob_w = pd.Series(float(default_profile["score_prob_weight"]), index=tmp.index, dtype=float)
    risk_scale = pd.Series(float(default_profile["score_risk_scale"]), index=tmp.index, dtype=float)
    overheat_scale = pd.Series(float(default_profile.get("overheat_penalty_scale", 1.0)), index=tmp.index, dtype=float)
    prob_alpha = pd.Series(float(default_profile.get("score_prob_alpha", 0.0)), index=tmp.index, dtype=float)
    short_mt_bear_bonus = pd.Series(float(default_profile.get("short_mt_bear_bonus", 0.0)), index=tmp.index, dtype=float)
    short_mt_bull_penalty = pd.Series(float(default_profile.get("short_mt_bull_penalty", 0.0)), index=tmp.index, dtype=float)
    short_vr1_penalty = pd.Series(float(default_profile.get("short_vr1_penalty", 0.0)), index=tmp.index, dtype=float)
    short_vr2_bonus = pd.Series(float(default_profile.get("short_vr2_bonus", 0.0)), index=tmp.index, dtype=float)

    for key, prof in profiles.items():
        m = tmp["regime_key"] == str(key)
        if not bool(m.any()):
            continue
        ret_base.loc[m] = float(prof.get("score_return_base", ret_base.loc[m].iloc[0]))
        prob_w.loc[m] = float(prof.get("score_prob_weight", prob_w.loc[m].iloc[0]))
        prob_alpha.loc[m] = float(prof.get("score_prob_alpha", prob_alpha.loc[m].iloc[0]))
        risk_scale.loc[m] = float(prof.get("score_risk_scale", risk_scale.loc[m].iloc[0]))
        overheat_scale.loc[m] = float(prof.get("overheat_penalty_scale", overheat_scale.loc[m].iloc[0]))
        short_mt_bear_bonus.loc[m] = float(prof.get("short_mt_bear_bonus", short_mt_bear_bonus.loc[m].iloc[0]))
        short_mt_bull_penalty.loc[m] = float(prof.get("short_mt_bull_penalty", short_mt_bull_penalty.loc[m].iloc[0]))
        short_vr1_penalty.loc[m] = float(prof.get("short_vr1_penalty", short_vr1_penalty.loc[m].iloc[0]))
        short_vr2_bonus.loc[m] = float(prof.get("short_vr2_bonus", short_vr2_bonus.loc[m].iloc[0]))

    if side == "long":
        ma_bonus = 0.01 * pd.to_numeric(
            tmp.get("ma_align_bull", pd.Series(0.0, index=tmp.index)), errors="coerce"
        ).fillna(0.0)
        rsi_bonus = 0.005 * pd.to_numeric(
            tmp.get("rsi_oversold", pd.Series(0.0, index=tmp.index)), errors="coerce"
        ).fillna(0.0)
    else:
        ma_bonus = 0.01 * pd.to_numeric(
            tmp.get("ma_align_bear", pd.Series(0.0, index=tmp.index)), errors="coerce"
        ).fillna(0.0)
        rsi_bonus = 0.005 * pd.to_numeric(
            tmp.get("rsi_overbought", pd.Series(0.0, index=tmp.index)), errors="coerce"
        ).fillna(0.0)

    vol_ratio_col = pd.to_numeric(
        tmp.get("vol_ratio20", pd.Series(0.0, index=tmp.index)), errors="coerce"
    ).fillna(0.0)
    vol_bonus = 0.005 * (vol_ratio_col > 2.0).astype(float)

    overheated = pd.to_numeric(
        tmp.get("overheated25", pd.Series(0.0, index=tmp.index)), errors="coerce"
    ).fillna(0.0)
    if side == "long":
        overheat_penalty = 0.015 * overheat_scale * overheated
        short_regime_term = pd.Series(0.0, index=tmp.index, dtype=float)
    else:
        overheat_penalty = pd.Series(0.0, index=tmp.index, dtype=float)
        mt_raw = pd.to_numeric(tmp.get("market_trend_state", np.nan), errors="coerce")
        vr_raw = pd.to_numeric(tmp.get("vol_regime", np.nan), errors="coerce")
        if mt_raw.isna().any() or vr_raw.isna().any():
            rk = tmp["regime_key"].astype(str)
            mt_from_key = pd.to_numeric(rk.str.extract(r"mt(\d+)")[0], errors="coerce")
            vr_from_key = pd.to_numeric(rk.str.extract(r"vr(\d+)")[0], errors="coerce")
            mt_raw = mt_raw.fillna(mt_from_key)
            vr_raw = vr_raw.fillna(vr_from_key)
        mt = mt_raw.fillna(1.0).clip(0.0, 2.0)
        vr = vr_raw.fillna(1.0).clip(0.0, 2.0)
        mt_bear = (mt <= 0.5).astype(float)
        mt_bull = (mt >= 1.5).astype(float)
        vr1 = ((vr >= 0.5) & (vr < 1.5)).astype(float)
        vr2 = (vr >= 1.5).astype(float)
        short_regime_term = (
            short_mt_bear_bonus * mt_bear
            - short_mt_bull_penalty * mt_bull
            - short_vr1_penalty * vr1
            + short_vr2_bonus * vr2
        )

    tmp["score"] = (
        pd.to_numeric(tmp["pred_return"], errors="coerce").fillna(0.0) * (ret_base + prob_w * pd.to_numeric(tmp["pred_prob_tp"], errors="coerce").fillna(0.0))
        + prob_alpha * pd.to_numeric(tmp["pred_prob_tp"], errors="coerce").fillna(0.0)
        - float(risk_penalty) * risk_scale * pd.to_numeric(tmp["risk_dn"], errors="coerce").fillna(0.0)
        + ma_bonus
        + vol_bonus
        + rsi_bonus
        + short_regime_term
        - overheat_penalty
    )
    tmp["strategy_profile"] = np.where(tmp["regime_key"].isin(list(profiles.keys())), "regime", "default")
    return tmp


def _learn_regime_profiles(
    valid_frame: pd.DataFrame,
    side: str,
    config: ResearchConfig,
    default_profile: dict[str, float],
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    src = valid_frame.copy()
    src = src.dropna(subset=["realized_return"]).copy()
    if src.empty:
        return {}, {"enabled": False, "reason": "empty_valid"}
    if "regime_key" not in src.columns:
        src["regime_key"] = _regime_key(src)

    min_rows = max(20, int(getattr(config.model, "regime_min_rows", 120)))
    min_months = max(2, int(getattr(config.model, "regime_min_months", 4)))
    min_improve = max(0.0, float(getattr(config.model, "regime_min_improvement", 0.0005)))
    top_k = max(1, int(config.model.top_k))
    months_sorted = sorted(src["asof_date"].astype(str).dropna().unique().tolist())
    holdout_n = max(2, min(4, max(2, len(months_sorted) // 3)))
    holdout_months = set(months_sorted[-holdout_n:]) if len(months_sorted) > holdout_n else set(months_sorted)
    fit_months = set(months_sorted) - holdout_months
    if not fit_months:
        fit_months = set(months_sorted)

    def _robust_objective(scored_frame: pd.DataFrame) -> tuple[float, float, float]:
        fit = scored_frame[scored_frame["asof_date"].astype(str).isin(fit_months)]
        hold = scored_frame[scored_frame["asof_date"].astype(str).isin(holdout_months)]
        fit_obj = _score_objective(fit, top_k=top_k, side=side)
        hold_obj = _score_objective(hold, top_k=top_k, side=side)
        robust = float(0.65 * fit_obj + 0.35 * hold_obj)
        return robust, float(fit_obj), float(hold_obj)

    base_scored = _apply_strategy_score(
        src,
        side=side,
        risk_penalty=float(config.model.risk_penalty),
        default_profile=default_profile,
        strategy_profiles={},
    )
    current_obj, current_fit_obj, current_hold_obj = _robust_objective(base_scored)
    accepted_profiles: dict[str, dict[str, float]] = {}
    accepted_meta: list[dict[str, Any]] = []

    counts = src["regime_key"].value_counts(dropna=False)
    keys = [str(k) for k in counts.index.tolist()]
    grid = _profile_grid(side=side, default_profile=default_profile)

    for key in keys:
        sub = src[src["regime_key"] == key]
        if len(sub) < min_rows:
            continue
        month_count = int(sub["asof_date"].nunique())
        if month_count < min_months:
            continue

        best_obj = current_obj
        best_profile: dict[str, float] | None = None
        best_fit_obj = current_fit_obj
        best_hold_obj = current_hold_obj
        for profile in grid:
            trial_profiles = dict(accepted_profiles)
            trial_profiles[key] = dict(profile)
            trial_scored = _apply_strategy_score(
                src,
                side=side,
                risk_penalty=float(config.model.risk_penalty),
                default_profile=default_profile,
                strategy_profiles=trial_profiles,
            )
            obj, fit_obj, hold_obj = _robust_objective(trial_scored)
            if obj > best_obj:
                best_obj = obj
                best_fit_obj = fit_obj
                best_hold_obj = hold_obj
                best_profile = dict(profile)

        # Guard against profile overfit: holdout objective must not degrade.
        if (
            best_profile is not None
            and best_obj >= current_obj + min_improve
            and best_hold_obj >= current_hold_obj - max(0.00025, min_improve * 0.5)
        ):
            accepted_profiles[key] = best_profile
            accepted_meta.append(
                {
                    "regime_key": key,
                    "rows": int(len(sub)),
                    "months": month_count,
                    "objective_after": float(best_obj),
                    "objective_gain": float(best_obj - current_obj),
                    "fit_objective_after": float(best_fit_obj),
                    "holdout_objective_after": float(best_hold_obj),
                }
            )
            current_obj = float(best_obj)
            current_fit_obj = float(best_fit_obj)
            current_hold_obj = float(best_hold_obj)

    summary = {
        "enabled": True,
        "baseline_objective": float(_score_objective(base_scored, top_k=top_k, side=side)),
        "baseline_objective_fit": float(
            _score_objective(
                base_scored[base_scored["asof_date"].astype(str).isin(fit_months)],
                top_k=top_k,
                side=side,
            )
        ),
        "baseline_objective_holdout": float(
            _score_objective(
                base_scored[base_scored["asof_date"].astype(str).isin(holdout_months)],
                top_k=top_k,
                side=side,
            )
        ),
        "final_objective": float(current_obj),
        "final_objective_fit": float(current_fit_obj),
        "final_objective_holdout": float(current_hold_obj),
        "holdout_months": sorted(list(holdout_months)),
        "accepted_profiles": int(len(accepted_profiles)),
        "accepted_meta": accepted_meta,
    }
    return accepted_profiles, summary


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
    valid_df = side_df[side_df["asof_date"].isin(valid_months)].copy()
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
    tp_train = pd.to_numeric(train_df["tp_hit"], errors="coerce").fillna(0.0).clip(0.0, 1.0).to_numpy(dtype=float)

    # バリデーションセット (early stopping + Optuna用)
    X_val: np.ndarray | None = None
    y_val: np.ndarray | None = None
    tp_val: np.ndarray | None = None
    if not valid_df.empty:
        X_val = _to_matrix(valid_df)
        y_val = pd.to_numeric(valid_df["realized_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        tp_val = pd.to_numeric(valid_df["tp_hit"], errors="coerce").fillna(0.0).clip(0.0, 1.0).to_numpy(dtype=float)

    risk_by_code = (
        train_df.groupby("code")["mae"].mean().replace([np.inf, -np.inf], np.nan).dropna().to_dict()
    )
    global_risk = float(pd.to_numeric(train_df["mae"], errors="coerce").fillna(0.0).mean())

    # ---- LightGBM or Ridge ----
    lgb = _try_import_lightgbm()
    use_lgb = lgb is not None and getattr(config.model, "use_lightgbm", True)

    # ---- Stage-1: レジーム分類器 ----
    regime_clf: Any = None
    if use_lgb:
        all_labeled = pd.concat([train_df, valid_df], ignore_index=True) if not valid_df.empty else train_df
        regime_clf = _fit_regime_classifier(lgb, all_labeled)

    def _add_regime_proba(X_base: np.ndarray, frame: pd.DataFrame) -> np.ndarray:
        """regime確率(5次元)をfeature行列に追加."""
        proba = _predict_regime_proba(regime_clf, frame)
        return np.concatenate([X_base, proba], axis=1)

    X_train_aug = _add_regime_proba(X_train, train_df)
    X_val_aug = _add_regime_proba(X_val, valid_df) if X_val is not None and not valid_df.empty else X_val

    feature_importance: dict[str, float] = {}
    optuna_best_params: dict[str, Any] = {}

    if use_lgb:
        print(f"  [train_side] side={side}, train_n={len(X_train)}, val_n={len(X_val) if X_val is not None else 0}")

        # Optuna HPO (有効な場合)
        n_optuna = getattr(config.model, "lgbm_optuna_trials", 0)
        if n_optuna > 0 and X_val_aug is not None and y_val is not None and len(X_val_aug) >= 10:
            print(f"  [Optuna] running {n_optuna} trials for side={side}...")
            optuna_best_params = _try_optuna_tune(lgb, X_train_aug, y_train, X_val_aug, y_val, n_trials=n_optuna)

        gbm_reg = _fit_gbdt_regressor(lgb, X_train_aug, y_train, config, X_val=X_val_aug, y_val=y_val)
        if len(np.unique(tp_train)) >= 2:
            gbm_cls = _fit_gbdt_classifier(
                lgb, X_train_aug, tp_train, config,
                X_val=X_val_aug,
                y_val=tp_val,
            )
        else:
            gbm_cls = None

        imp_vals = gbm_reg.feature_importances_[:len(FEATURE_COLUMNS)]
        for col, imp in zip(list(FEATURE_COLUMNS), imp_vals.tolist()):
            feature_importance[col] = float(imp)
        model_type = "lightgbm"
        mean_x = X_train.mean(axis=0)
        std_x = np.where(X_train.std(axis=0) < 1e-9, 1.0, X_train.std(axis=0))
        intercept, coef = _fit_ridge((X_train - mean_x) / std_x, y_train, alpha=float(config.model.ridge_alpha))
        pred_mean, pred_std = 0.0, 1.0
        logit_b0, logit_b1 = 0.0, 1.0
    else:
        mean_x = X_train.mean(axis=0)
        std_x = X_train.std(axis=0)
        std_x = np.where(std_x < 1e-9, 1.0, std_x)
        X_train_z = (X_train - mean_x) / std_x
        intercept, coef = _fit_ridge(X_train_z, y_train, alpha=float(config.model.ridge_alpha))
        pred_train = intercept + X_train_z @ coef
        pred_mean = float(pred_train.mean())
        pred_std = float(pred_train.std() if pred_train.std() > 1e-9 else 1.0)
        pred_train_z = (pred_train - pred_mean) / pred_std
        logit_b0, logit_b1 = _fit_logistic_1d(pred_train_z, tp_train)
        gbm_reg = None
        gbm_cls = None
        model_type = "ridge"
        for col, c in zip(list(FEATURE_COLUMNS), coef.tolist()):
            feature_importance[col] = abs(float(c))

    # Side-specific base score mix. Regime strategies are learned on top of this baseline.
    if side == "long":
        score_return_base = 0.70
        score_prob_weight = 0.30
        score_prob_alpha = 0.0
        score_risk_scale = 0.75
        overheat_penalty_scale = 1.0
        short_mt_bear_bonus = 0.0
        short_mt_bull_penalty = 0.0
        short_vr1_penalty = 0.0
        short_vr2_bonus = 0.0
    else:
        score_return_base = float(getattr(config.model, "short_score_return_base", 0.45))
        score_prob_weight = float(getattr(config.model, "short_score_prob_weight", 0.20))
        score_prob_alpha = float(getattr(config.model, "short_score_prob_alpha", 0.01))
        score_risk_scale = float(getattr(config.model, "short_score_risk_scale", 1.20))
        overheat_penalty_scale = 0.0
        short_mt_bear_bonus = float(getattr(config.model, "short_mt_bear_bonus", 0.0030))
        short_mt_bull_penalty = float(getattr(config.model, "short_mt_bull_penalty", 0.0060))
        short_vr1_penalty = float(getattr(config.model, "short_vr1_penalty", 0.0040))
        short_vr2_bonus = float(getattr(config.model, "short_vr2_bonus", 0.0010))

    base_profile = {
        "score_return_base": float(score_return_base),
        "score_prob_weight": float(score_prob_weight),
        "score_prob_alpha": float(score_prob_alpha),
        "score_risk_scale": float(score_risk_scale),
        "overheat_penalty_scale": float(overheat_penalty_scale),
        "short_mt_bear_bonus": float(short_mt_bear_bonus),
        "short_mt_bull_penalty": float(short_mt_bull_penalty),
        "short_vr1_penalty": float(short_vr1_penalty),
        "short_vr2_bonus": float(short_vr2_bonus),
    }
    regime_strategy_enabled = bool(getattr(config.model, "regime_strategy_enabled", True))
    if side == "long":
        regime_strategy_enabled = regime_strategy_enabled and bool(
            getattr(config.model, "regime_strategy_long_enabled", True)
        )
    else:
        regime_strategy_enabled = regime_strategy_enabled and bool(
            getattr(config.model, "regime_strategy_short_enabled", True)
        )

    def _predict_phase(
        frame: pd.DataFrame,
        phase: str,
        strategy_profiles: dict[str, dict[str, float]] | None = None,
        keep_aux: bool = False,
    ) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(
                columns=[
                    "asof_date",
                    "code",
                    "side",
                    "phase",
                    "score",
                    "pred_return",
                    "pred_prob_tp",
                    "risk_dn",
                    "regime_key",
                    "strategy_profile",
                ]
            )
        tmp = frame.copy()
        tmp["candidate_seed"] = _candidate_seed(tmp, side)
        tmp = (
            tmp.sort_values(["asof_date", "candidate_seed"], ascending=[True, False])
            .groupby("asof_date", as_index=False, group_keys=False)
            .head(config.model.candidate_pool)
            .reset_index(drop=True)
        )
        X = _to_matrix(tmp)
        # regime probas を追加 (Stage-1の出力)
        X_aug = _add_regime_proba(X, tmp)

        if use_lgb and gbm_reg is not None:
            pred_return = gbm_reg.predict(X_aug).astype(float)
            if gbm_cls is not None:
                pred_prob = gbm_cls.predict_proba(X_aug)[:, 1].astype(float)
            else:
                pred_prob = np.full(len(tmp), float(np.mean(tp_train)))
        else:
            Xz = (X - mean_x) / std_x
            pred_return = (intercept + Xz @ coef).astype(float)
            pred_z = (pred_return - pred_mean) / pred_std
            pred_prob = _sigmoid(logit_b0 + logit_b1 * pred_z)

        tmp["pred_return"] = pred_return.astype(float)
        tmp["pred_prob_tp"] = pred_prob.astype(float)
        tmp["risk_dn"] = tmp["code"].map(risk_by_code).fillna(global_risk).astype(float)
        tmp = _apply_strategy_score(
            tmp,
            side=side,
            risk_penalty=float(config.model.risk_penalty),
            default_profile=base_profile,
            strategy_profiles=strategy_profiles,
        )

        # ---- 信頼度スコアフィルタ ----
        conf_thresh = float(getattr(config.model, "confidence_threshold", 0.0))
        if conf_thresh > 0.0:
            _rv20_col = tmp.get("rv20", pd.Series(0.05, index=tmp.index)).fillna(0.05)
            # Sharpe調整信頼度: |pred_return| / realized_vol
            confidence = tmp["pred_return"].abs() / (_rv20_col + 1e-6)
            # 閾値以下はスコアをゼロに (上位に来なくなる)
            tmp["score"] = np.where(confidence >= conf_thresh, tmp["score"], 0.0)
            tmp["confidence"] = confidence
            high_conf_pct = float((confidence >= conf_thresh).mean())
            if phase == "inference":
                print(f"    [Confidence] threshold={conf_thresh:.2f}, high_conf={high_conf_pct:.1%}")

        tmp["side"] = side
        tmp["phase"] = phase
        if keep_aux:
            return tmp.copy()
        keep = [
            "asof_date",
            "code",
            "side",
            "phase",
            "score",
            "pred_return",
            "pred_prob_tp",
            "risk_dn",
            "regime_key",
            "strategy_profile",
            "realized_return",
            "tp_hit",
            "mae",
            "mfe",
        ]
        for col in keep:
            if col not in tmp.columns:
                tmp[col] = np.nan
        return tmp[keep].copy()

    strategy_profiles: dict[str, dict[str, float]] = {}
    strategy_summary: dict[str, Any] = {"enabled": False, "reason": "disabled"}
    short_regime_caps = getattr(config.model, "short_regime_topk_caps", {})
    short_month_gate = _normalize_short_month_gate(
        {
            "enabled": bool(getattr(config.model, "short_month_gate_enabled", False)),
            "allowed_regimes": list(getattr(config.model, "short_month_gate_allowed_regimes", []) or []),
            "pred_return_max": getattr(config.model, "short_month_gate_pred_return_max", None),
            "prob_min": getattr(config.model, "short_month_gate_prob_min", None),
            "risk_max": getattr(config.model, "short_month_gate_risk_max", None),
        }
    )
    short_month_gate_summary: dict[str, Any] = {
        "enabled": bool(short_month_gate.get("enabled", False)),
        "auto": False,
        "selected_gate": dict(short_month_gate),
    }
    valid_probe_for_gate: pd.DataFrame | None = None
    if regime_strategy_enabled:
        valid_probe = _predict_phase(valid_df.copy(), phase="valid", strategy_profiles=None, keep_aux=True)
        valid_probe_for_gate = valid_probe.copy()
        strategy_profiles, strategy_summary = _learn_regime_profiles(
            valid_probe,
            side=side,
            config=config,
            default_profile=base_profile,
        )
        if strategy_profiles:
            print(f"  [regime_strategy] side={side}, learned_profiles={len(strategy_profiles)}")
    if side == "short" and bool(short_month_gate.get("enabled", False)):
        if bool(getattr(config.model, "short_month_gate_auto", False)):
            train_probe_for_gate = _predict_phase(
                train_df.copy(),
                phase="train_gate",
                strategy_profiles=strategy_profiles,
                keep_aux=True,
            )
            if valid_probe_for_gate is None:
                valid_probe_for_gate = _predict_phase(
                    valid_df.copy(),
                    phase="valid",
                    strategy_profiles=strategy_profiles,
                    keep_aux=True,
                )
            gate_learn_frame = pd.concat(
                [train_probe_for_gate, valid_probe_for_gate],
                ignore_index=True,
            )
            short_month_gate, short_month_gate_summary = _learn_short_month_gate(
                valid_frame=gate_learn_frame,
                config=config,
                top_k=int(config.model.top_k),
                short_regime_caps=short_regime_caps,
                base_gate=short_month_gate,
            )
            short_month_gate_summary["learn_rows"] = int(len(gate_learn_frame))
            short_month_gate_summary["learn_months"] = int(
                gate_learn_frame["asof_date"].astype(str).nunique()
            )
            print(
                "  [short_month_gate] auto-selected "
                f"enabled={bool(short_month_gate.get('enabled', False))}, "
                f"regimes={len(short_month_gate.get('allowed_regimes', []))}, "
                f"ret_max={short_month_gate.get('pred_return_max')}, "
                f"prob_min={short_month_gate.get('prob_min')}, "
                f"risk_max={short_month_gate.get('risk_max')}"
            )
        else:
            short_month_gate_summary = {
                "enabled": True,
                "auto": False,
                "selected_gate": dict(short_month_gate),
            }

    valid_pred = _predict_phase(valid_df.copy(), phase="valid", strategy_profiles=strategy_profiles)
    test_pred = _predict_phase(
        side_df[side_df["asof_date"].isin(test_months)].copy(),
        phase="test",
        strategy_profiles=strategy_profiles,
    )
    infer_pred = _predict_phase(
        inference_features.copy(),
        phase="inference",
        strategy_profiles=strategy_profiles,
    )

    rankings = pd.concat([valid_pred, test_pred, infer_pred], ignore_index=True)
    top20 = _topk_by_month(
        rankings,
        top_k=config.model.top_k,
        side=side,
        short_regime_caps=short_regime_caps,
        short_month_gate=short_month_gate,
    )

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
        "score_return_base": float(score_return_base),
        "score_prob_weight": float(score_prob_weight),
        "score_prob_alpha": float(score_prob_alpha),
        "score_risk_scale": float(score_risk_scale),
        "overheat_penalty_scale": float(overheat_penalty_scale),
        "short_mt_bear_bonus": float(short_mt_bear_bonus),
        "short_mt_bull_penalty": float(short_mt_bull_penalty),
        "short_vr1_penalty": float(short_vr1_penalty),
        "short_vr2_bonus": float(short_vr2_bonus),
        "short_regime_topk_caps": short_regime_caps,
        "short_month_gate_enabled": bool(getattr(config.model, "short_month_gate_enabled", False)),
        "short_month_gate_allowed_regimes": list(getattr(config.model, "short_month_gate_allowed_regimes", []) or []),
        "short_month_gate_pred_return_max": getattr(config.model, "short_month_gate_pred_return_max", None),
        "short_month_gate_prob_min": getattr(config.model, "short_month_gate_prob_min", None),
        "short_month_gate_risk_max": getattr(config.model, "short_month_gate_risk_max", None),
        "short_month_gate_auto": bool(getattr(config.model, "short_month_gate_auto", False)),
        "short_month_gate_auto_max_regimes": int(getattr(config.model, "short_month_gate_auto_max_regimes", 4)),
        "short_month_gate_auto_min_months": int(getattr(config.model, "short_month_gate_auto_min_months", 6)),
        "short_month_gate_auto_min_improvement": float(
            getattr(config.model, "short_month_gate_auto_min_improvement", 0.0002)
        ),
        "short_month_gate_selected": dict(short_month_gate),
        "short_month_gate_summary": short_month_gate_summary,
        "regime_strategy_enabled": bool(regime_strategy_enabled),
        "regime_strategy_profiles": strategy_profiles,
        "regime_strategy_summary": strategy_summary,
        "model_type": model_type,
        "feature_importance": feature_importance,
        "optuna_best_params": optuna_best_params,
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
    # rv20などが重複する場合があるので、suffixesで管理し後でクリーンアップ
    merged = feature_hist.merge(train_labels, on=["asof_date", "code"], how="inner", suffixes=("", "_label"))
    
    # 重複カラムがある場合（rv20など）、ベースの方を残す
    for col in FEATURE_COLUMNS:
        label_col = f"{col}_label"
        if label_col in merged.columns:
            merged = merged.drop(columns=[label_col])
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
