"""
analyze_feature_importance.py
=============================
特徴量重要度解析スクリプト（research/ パイプライン経由）

使い方:
    python -m app.backend.analysis.analyze_feature_importance \
        --snapshot <snapshot_id> \
        --asof <YYYY-MM-DD> \
        [--config <path/to/config.json>]

出力:
    feature_importance_<side>.csv  -- 特徴量重要度 CSV
    feature_importance_report.txt  -- 日本語レポート
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# パスを通す
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.config import load_config
from research.features import FEATURE_COLUMNS, build_features_for_asof, load_feature_history
from research.labels import build_labels_for_asof, load_label_history
from research.storage import ResearchPaths, parse_date, ymd


def _try_import_lightgbm():
    try:
        import lightgbm as lgb  # type: ignore
        return lgb
    except ModuleNotFoundError:
        return None


def run_importance_analysis(
    snapshot_id: str,
    asof_date: str,
    config_path: str | None = None,
    out_dir: Path | None = None,
) -> dict:
    """特徴量重要度を計算してレポートを生成する."""
    paths = ResearchPaths(root=ROOT)
    config = load_config(config_path)
    asof_ts = parse_date(asof_date)
    asof_str = ymd(asof_ts)

    print(f"[INFO] Loading features for snapshot={snapshot_id}, asof={asof_str}")
    feature_hist = load_feature_history(paths, config, snapshot_id, asof_str)
    label_hist = load_label_history(paths, config, snapshot_id, asof_str)

    if feature_hist.empty:
        print("[WARN] Feature history is empty. Run build pipeline first.")
        return {}
    if label_hist.empty:
        print("[WARN] Label history is empty. Run build pipeline first.")
        return {}

    feature_hist["asof_date"] = pd.to_datetime(feature_hist["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    label_hist["asof_date"] = pd.to_datetime(label_hist["asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    merged = feature_hist.merge(label_hist, on=["asof_date", "code"], how="inner")
    if merged.empty:
        print("[WARN] Merged dataset is empty.")
        return {}

    print(f"[INFO] Merged rows: {len(merged)}, codes: {merged['code'].nunique()}, months: {merged['asof_date'].nunique()}")

    lgb = _try_import_lightgbm()
    results: dict = {"sides": {}}

    out_dir = out_dir or Path(".")
    out_dir.mkdir(parents=True, exist_ok=True)

    report_lines = [
        "=" * 70,
        "特徴量重要度レポート (Feature Importance Report)",
        f"  snapshot_id : {snapshot_id}",
        f"  asof_date   : {asof_str}",
        f"  model       : {'LightGBM (GBDT)' if lgb else 'Ridge (coef abs)'}",
        f"  features    : {len(FEATURE_COLUMNS)}",
        f"  rows        : {len(merged)}",
        "=" * 70,
    ]

    for side in ["long", "short"]:
        side_df = merged[merged["side"] == side].copy()
        if side_df.empty:
            print(f"[WARN] No rows for side={side}")
            continue

        medians: dict[str, float] = {}
        arrays: list[np.ndarray] = []
        for col in FEATURE_COLUMNS:
            s = pd.to_numeric(side_df[col], errors="coerce")
            med = float(s.median()) if not s.dropna().empty else 0.0
            medians[col] = med
            arrays.append(s.fillna(med).to_numpy(dtype=float))

        X = np.column_stack(arrays)
        y_ret = pd.to_numeric(side_df["realized_return"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        y_tp = pd.to_numeric(side_df["tp_hit"], errors="coerce").fillna(0).clip(0, 1).to_numpy(dtype=int)

        if lgb:
            reg = lgb.LGBMRegressor(n_estimators=200, learning_rate=0.05, max_depth=5, num_leaves=31,
                                     subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1)
            reg.fit(X, y_ret)
            imp_gain = reg.feature_importances_.astype(float)
            imp_source = "lgbm_gain"

            if len(np.unique(y_tp)) >= 2:
                clf = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05, max_depth=5, num_leaves=31,
                                          subsample=0.8, colsample_bytree=0.8, random_state=42, verbose=-1)
                clf.fit(X, y_tp)
                imp_cls = clf.feature_importances_.astype(float)
            else:
                imp_cls = np.zeros_like(imp_gain)
        else:
            # Ridge fallback
            mean_x = X.mean(axis=0)
            std_x = np.where(X.std(axis=0) < 1e-9, 1.0, X.std(axis=0))
            Xz = (X - mean_x) / std_x
            I = np.eye(Xz.shape[1])
            lhs = Xz.T @ Xz + I * 1.0
            rhs = Xz.T @ y_ret
            coef = np.linalg.solve(lhs, rhs)[1:] if Xz.shape[1] == len(FEATURE_COLUMNS) else np.linalg.solve(lhs, rhs)
            imp_gain = np.abs(coef[:len(FEATURE_COLUMNS)])
            imp_cls = np.zeros_like(imp_gain)
            imp_source = "ridge_abscoef"

        # 重要度テーブル
        imp_df = pd.DataFrame({
            "feature": list(FEATURE_COLUMNS),
            "importance_reg": imp_gain.tolist(),
            "importance_cls": imp_cls.tolist(),
            "importance_combined": ((imp_gain + imp_cls) / 2.0).tolist(),
        })
        imp_df = imp_df.sort_values("importance_combined", ascending=False).reset_index(drop=True)
        imp_df["rank"] = range(1, len(imp_df) + 1)

        # CSV出力
        csv_path = out_dir / f"feature_importance_{side}.csv"
        imp_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[INFO] Saved {csv_path}")

        # レポートセクション
        report_lines += [
            "",
            f"■ Side: {side.upper()}  (rows={len(side_df)}, source={imp_source})",
            f"{'Rank':<5} {'Feature':<30} {'RegImp':>10} {'ClsImp':>10} {'Combined':>10}",
            "-" * 70,
        ]
        for _, row in imp_df.head(30).iterrows():
            report_lines.append(
                f"{int(row['rank']):<5} {row['feature']:<30} {row['importance_reg']:>10.2f} "
                f"{row['importance_cls']:>10.2f} {row['importance_combined']:>10.2f}"
            )

        results["sides"][side] = {
            "n_rows": len(side_df),
            "top10": imp_df.head(10)[["feature", "importance_combined"]].to_dict(orient="records"),
        }

    # レポートファイル保存
    report_path = out_dir / "feature_importance_report.txt"
    report_text = "\n".join(report_lines)
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\n[INFO] Report saved to {report_path}")
    print(report_text)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Feature Importance Analysis")
    parser.add_argument("--snapshot", required=True, help="Snapshot ID")
    parser.add_argument("--asof", required=True, help="As-of date YYYY-MM-DD")
    parser.add_argument("--config", default=None, help="Config JSON path")
    parser.add_argument("--out", default=".", help="Output directory")
    args = parser.parse_args()

    run_importance_analysis(
        snapshot_id=args.snapshot,
        asof_date=args.asof,
        config_path=args.config,
        out_dir=Path(args.out),
    )


if __name__ == "__main__":
    main()
