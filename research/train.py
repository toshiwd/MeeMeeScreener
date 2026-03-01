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

    # Side-specific score mix tuned on prod_v3_final candidate rankings
    # while preserving technical bonus residuals.
    if side == "long":
        score_return_base = 0.70
        score_prob_weight = 0.30
        score_risk_scale = 0.75
    else:
        score_return_base = 0.55
        score_prob_weight = 0.20
        score_risk_scale = 1.50

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

        # ---- Technical Bonuses/Penalties ----
        if side == "long":
            ma_bonus = 0.01 * tmp.get("ma_align_bull", pd.Series(0.0, index=tmp.index)).fillna(0.0)
            rsi_bonus = 0.005 * tmp.get("rsi_oversold", pd.Series(0.0, index=tmp.index)).fillna(0.0)
        else:
            ma_bonus = 0.01 * tmp.get("ma_align_bear", pd.Series(0.0, index=tmp.index)).fillna(0.0)
            rsi_bonus = 0.005 * tmp.get("rsi_overbought", pd.Series(0.0, index=tmp.index)).fillna(0.0)

        vol_ratio_col = tmp.get("vol_ratio20", pd.Series(0.0, index=tmp.index)).fillna(0.0)
        vol_bonus = 0.005 * (vol_ratio_col > 2.0).astype(float)

        overheated = tmp.get("overheated25", pd.Series(0.0, index=tmp.index)).fillna(0.0)
        overheat_penalty = (0.015 * overheated if side == "long" else pd.Series(0.0, index=tmp.index))

        tmp["score"] = (
            tmp["pred_return"] * (score_return_base + score_prob_weight * tmp["pred_prob_tp"])
            - float(config.model.risk_penalty) * score_risk_scale * tmp["risk_dn"]
            + ma_bonus
            + vol_bonus
            + rsi_bonus
            - overheat_penalty
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

    valid_pred = _predict_phase(valid_df.copy(), phase="valid")
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
        "score_return_base": float(score_return_base),
        "score_prob_weight": float(score_prob_weight),
        "score_risk_scale": float(score_risk_scale),
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
