import pandas as pd
import numpy as np
import argparse
import json
from pathlib import Path
from research.storage import ResearchPaths

def convert_np(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def analyze_errors(run_id: str = None, preds_file: Path = None, monthly_eval_file: Path = None):
    """Analyze False Positive vs True Positive for Short Side and output to JSON."""
    paths = ResearchPaths.build()
    
    if run_id:
        run_dir = paths.run_dir(run_id)
        if preds_file is None:
            preds_file = run_dir / "rankings_short.csv"
        if monthly_eval_file is None:
            monthly_eval_file = run_dir / "evaluation_monthly.csv"
    else:
        if preds_file is None or monthly_eval_file is None:
            print("Error: missing_arguments (run_id or both preds/eval required)")
            return
        run_dir = preds_file.parent

    artifact_dir = run_dir / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    out_file = artifact_dir / "short_error_analysis.json"
    
    if not preds_file.exists():
        print(f"Error: missing_predictions ({preds_file})")
        return
    if not monthly_eval_file.exists():
        print(f"Error: missing_evaluation_monthly ({monthly_eval_file})")
        return
        print(f"Error: {preds_file} not found.")
        return

    preds = pd.read_csv(preds_file)
    preds_short = preds[preds["side"] == "short"].copy() if "side" in preds.columns else preds.copy()
    preds_short = preds_short[preds_short["phase"] == "test"].copy() if "phase" in preds_short.columns else preds_short.copy()
    
    if preds_short.empty:
        print("No short test predictions found.")
        return

    # Define Top-K or High Confidence threshold
    k = min(len(preds_short), 20)
    score_col = "pred_prob_tp" if "pred_prob_tp" in preds_short.columns else ("prob_1" if "prob_1" in preds_short.columns else "score")
    if score_col in preds_short.columns:
        score_thresh = preds_short[score_col].quantile(0.9) if len(preds_short) > 20 else preds_short[score_col].min()
        high_prob = preds_short[preds_short[score_col] >= score_thresh].copy()
    else:
        high_prob = preds_short.copy()

    # Determine False Positive vs True Positive
    # TP: Realized Return > 0 (or label_quality > 0.4), FP: Realized Return < 0
    if "tp_hit" in high_prob.columns:
        fp = high_prob[high_prob["tp_hit"] == 0].copy()
        tp = high_prob[high_prob["tp_hit"] == 1].copy()
    elif "realized_return" in high_prob.columns:
        fp = high_prob[high_prob["realized_return"] <= 0].copy()
        tp = high_prob[high_prob["realized_return"] > 0].copy()
    else:
        fp = high_prob.copy()
        tp = high_prob.copy()

    # 1. Counts
    fp_count = int(len(fp))
    tp_count = int(len(tp))

    # 2. Regime Split
    regime_col = "market_trend_state" if "market_trend_state" in high_prob.columns else None
    if regime_col:
        fp_by_regime = fp[regime_col].value_counts().to_dict()
        tp_by_regime = tp[regime_col].value_counts().to_dict()
    else:
        fp_by_regime = {}
        tp_by_regime = {}

    # 3. Distributions
    dist_cols = ["mae", "mfe", "realized_return", "risk_dn"]
    distributions = {}
    for col in dist_cols:
        if col in high_prob.columns:
            distributions[col] = {
                "fp_mean": float(fp[col].mean()) if not fp.empty else 0.0,
                "tp_mean": float(tp[col].mean()) if not tp.empty else 0.0,
                "fp_p90": float(fp[col].quantile(0.9)) if not fp.empty else 0.0,
                "tp_p90": float(tp[col].quantile(0.9)) if not tp.empty else 0.0,
            }

    # 4. Failure Tickers in top-K
    failure_tickers = []
    if score_col in fp.columns:
        top_k_fp = fp.sort_values(by=score_col, ascending=False).head(20)
        disp_cols = [c for c in ["asof_date", "code", score_col, "mae", "mfe", "realized_return"] if c in top_k_fp.columns]
        failure_tickers = top_k_fp[disp_cols].to_dict(orient="records")

    # 5. Feature differences (FP vs TP)
    ignore_cols = {"asof_date", "code", "side", "label", "label_high_conf", "label_quality", "prob_1", "prob_0", "feature_version", "created_at", "snapshot_id", "realized_return", "tp_hit", "exit_price", "exit_reason", "phase", score_col, "score"}
    features = [c for c in high_prob.columns if c not in ignore_cols and np.issubdtype(high_prob[c].dtype, np.number)]
    
    feature_diff = {}
    if fp_count > 0 and tp_count > 0:
        fp_mean = fp[features].mean()
        tp_mean = tp[features].mean()
        for f in features:
            v_fp = fp_mean[f]
            v_tp = tp_mean[f]
            if v_tp != 0 and not np.isnan(v_tp) and not np.isnan(v_fp):
                ratio = float(v_fp / v_tp)
                diff = float(v_fp - v_tp)
                feature_diff[f] = {"fp_mean": float(v_fp), "tp_mean": float(v_tp), "ratio": ratio, "diff": diff}
    
    # Sort feature_diff by absolute difference
    sorted_feature_diff = dict(sorted(feature_diff.items(), key=lambda x: abs(x[1]["diff"]), reverse=True)[:30])

    # 6. Worst Month / Max Drawdown Contribution
    dd_analysis = {}
    if monthly_eval_file.exists():
        mev = pd.read_csv(monthly_eval_file)
        mev_short = mev[(mev["side"] == "short") & (mev["phase"] == "test")].copy() if "side" in mev.columns else mev.copy()
        ret_col = "return_at20" if "return_at20" in mev_short.columns else ("short_return" if "short_return" in mev_short.columns else None)
        if ret_col:
            worst_month_ret = float(mev_short[ret_col].min())
            worst_month_date = str(mev_short.loc[mev_short[ret_col].idxmin(), "asof_date"]) if not mev_short.empty else "N/A"
            cum_ret = (1 + mev_short[ret_col]).cumprod()
            drawdown = cum_ret / cum_ret.cummax() - 1
            max_dd = float(drawdown.min())
            dd_analysis = {
                "worst_month_date": worst_month_date,
                "worst_month_return": worst_month_ret,
                "max_drawdown": max_dd
            }

    # Decide failure mode:
    # regime-driven / mae-driven / pattern-miss-driven
    mode = "pattern-miss-driven"
    if fp_by_regime:
         max_fp_regime = max(fp_by_regime, key=fp_by_regime.get)
         if fp_by_regime[max_fp_regime] > fp_count * 0.6:
             mode = "regime-driven"
    result = {
        "run_id": run_id,
        "counts": {
            "false_positive": fp_count,
            "true_positive": tp_count,
            "total_high_prob": len(high_prob)
        },
        "regime_split": {
            "false_positive": fp_by_regime,
            "true_positive": tp_by_regime
        },
        "distributions": distributions,
        "failure_tickers_topk": failure_tickers,
        "feature_differences": sorted_feature_diff,
        "drawdown_analysis": dd_analysis
    }

    failure_mode = "unknown"
    if "regime_split" in result and "false_positive" in result["regime_split"]:
        fp_by_regime = result["regime_split"]["false_positive"]
        total_fps = sum(fp_by_regime.values()) if fp_by_regime else 0
        up_range_fps = fp_by_regime.get("up", 0) + fp_by_regime.get("range", 0)
        
        if total_fps > 0 and up_range_fps / total_fps > 0.6:
            failure_mode = "regime-driven"
        else:
            mae_diff = result.get("feature_differences", {}).get("mae", {}).get("diff", 0)
            if mae_diff < -0.05:
                failure_mode = "mae-driven"
            else:
                failure_mode = "pattern-miss-driven"
    
    result["failure_mode"] = failure_mode
    
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=convert_np)
        
    print(f"Analysis successfully written to {out_file}")
    print(f"Decided failure mode: {failure_mode}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", type=str, help="Run ID to analyze")
    parser.add_argument("--predictions", type=str, help="Path to rankings_short.csv")
    parser.add_argument("--evaluation-monthly", type=str, help="Path to evaluation_monthly.csv")
    args = parser.parse_args()
    
    preds_path = Path(args.predictions) if args.predictions else None
    eval_path = Path(args.evaluation_monthly) if args.evaluation_monthly else None

    if not args.run_id and not (preds_path and eval_path):
        print("Error: missing_arguments. Provide --run_id or both --predictions and --evaluation_monthly")
    else:
        analyze_errors(
            run_id=args.run_id,
            preds_file=preds_path,
            monthly_eval_file=eval_path
        )
