import json
import argparse
from pathlib import Path
from typing import Any, Dict
from research.storage import ResearchPaths

def load_run_data(run_id: str) -> Dict[str, Any]:
    paths = ResearchPaths.build()
    run_dir = paths.run_dir(run_id)
    
    eval_path = run_dir / "evaluation.json"
    analysis_path = run_dir / "artifacts" / "short_error_analysis.json"
    
    if not eval_path.exists():
        raise FileNotFoundError(f"Evaluation file not found: {eval_path}")
    if not analysis_path.exists():
        raise FileNotFoundError(f"Short error analysis file not found: {analysis_path}")
        
    with open(eval_path, "r", encoding="utf-8") as f:
        eval_data = json.load(f)
    with open(analysis_path, "r", encoding="utf-8") as f:
        analysis_data = json.load(f)
        
    # Extract authoritative metrics
    # Note: short_error_analysis.json is already filtered for 'test' phase by analyze_short_errors.py
    counts = analysis_data.get("counts", {})
    dist = analysis_data.get("distributions", {}).get("mae", {})
    dd_analysis = analysis_data.get("drawdown_analysis", {})
    
    # Max Drawdown from evaluation.json
    # We prefer test phase drawdown if available, otherwise default short metrics
    test_short_metrics = eval_data.get("metrics_by_phase", {}).get("test", {}).get("short", {})
    overall_short_metrics = eval_data.get("metrics", {}).get("short", {})
    
    max_dd = test_short_metrics.get("max_drawdown")
    if max_dd is None:
        max_dd = overall_short_metrics.get("max_drawdown", 0.0)
        
    return {
        "run_id": run_id,
        "fp_count": counts.get("false_positive", 0),
        "tp_count": counts.get("true_positive", 0),
        "mae_fp_mean": dist.get("fp_mean", 0.0),
        "mae_fp_p90": dist.get("fp_p90", 0.0),
        "worst_month_return": dd_analysis.get("worst_month_return", 0.0),
        "max_drawdown": max_dd
    }

def compare_runs(baseline_id: str, challenger_id: str, out_file: Path):
    base = load_run_data(baseline_id)
    chal = load_run_data(challenger_id)
    
    # Decision Logic
    # keep: FP top-K count decreases, MAE mean improves, DD does not worsen (stable)
    fp_reduced = chal["fp_count"] < base["fp_count"]
    mae_improved = chal["mae_fp_mean"] < base["mae_fp_mean"]
    # max_drawdown is non-negative drawdown. chal <= base means it didn't worsen.
    # Added small epsilon for float comparison
    dd_stable = chal["max_drawdown"] <= (base["max_drawdown"] + 1e-6)
    
    decision = "keep" if (fp_reduced and mae_improved and dd_stable) else "drop"
    
    if not dd_stable:
        rationale = "Max Drawdown worsened (DD: baseline={:.4f}, challenger={:.4f})".format(base["max_drawdown"], chal["max_drawdown"])
    elif not fp_reduced and not mae_improved:
        rationale = "No significant improvement in FP count or MAE."
    elif not fp_reduced:
        rationale = "FP count did not decrease (baseline={}, challenger={})".format(base["fp_count"], chal["fp_count"])
    elif not mae_improved:
        rationale = "MAE mean did not improve (baseline={:.4f}, challenger={:.4f})".format(base["mae_fp_mean"], chal["mae_fp_mean"])
    else:
        rationale = "Improvement in short accuracy (FP: {}->{}, MAE: {:.4f}->{:.4f}) with stable DD.".format(
            base["fp_count"], chal["fp_count"], base["mae_fp_mean"], chal["mae_fp_mean"]
        )
        
    report = {
        "baseline_run_id": baseline_id,
        "challenger_run_id": challenger_id,
        "metrics": {
            "baseline": base,
            "challenger": chal
        },
        "delta": {
            "fp_count": chal["fp_count"] - base["fp_count"],
            "tp_count": chal["tp_count"] - base["tp_count"],
            "mae_fp_mean_diff": chal["mae_fp_mean"] - base["mae_fp_mean"],
            "mae_fp_p90_diff": chal["mae_fp_p90"] - base["mae_fp_p90"],
            "max_drawdown_diff": chal["max_drawdown"] - base["max_drawdown"]
        },
        "decision": decision,
        "rationale": rationale
    }
    
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    print(f"Comparison report generated: {out_file}")
    print(f"Decision: {decision.upper()}")
    print(f"Rationale: {rationale}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automate comparison of two short runs.")
    parser.add_argument("--baseline", type=str, required=True, help="Baseline Run ID")
    parser.add_argument("--challenger", type=str, required=True, help="Challenger Run ID")
    args = parser.parse_args()
    
    paths = ResearchPaths.build()
    chal_dir = paths.run_dir(args.challenger)
    artifact_dir = chal_dir / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    out_path = artifact_dir / "short_feature_family_compare.json"
    
    compare_runs(args.baseline, args.challenger, out_path)
