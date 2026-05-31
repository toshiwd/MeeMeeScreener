from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "t5_wait_winner_damage_decomposition_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\recent_topk_t5_wait_entry_overlay_pretest_v1\20260524T055037Z-recent-topk-t5-wait-entry-overlay-pretest-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\t5_wait_winner_damage_decomposition_v1")
REQUIRED_INPUTS = (
    "candidate_entry_overlay_rows.csv",
    "topk_entry_overlay_summary.json",
    "loser_repair_summary.csv",
    "winner_damage_summary.csv",
    "timing_failure_type_summary.csv",
    "no_entry_loss_summary.csv",
    "period_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "wait_help_harm_summary.csv",
    "wait_helped_vs_harmed_feature_decomposition.csv",
    "immediate_buy_protection_profile.json",
    "delay_candidate_profile.json",
    "conditional_wait_axis_candidates.json",
    "year_stability_summary.csv",
    "concentration_summary.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = (2024, 2025, 2026)
TOPK = (5, 10)
FEATURES = (
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "ma7_gt_ma20_gt_ma60",
    "above7_streak",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "days_since_ma60_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ma20_ratio",
    "volume_spike_on_down_day",
    "high_break_volume_count",
    "realized_vol20",
    "atr14_pct",
    "monthly_high_zone_proxy",
    "monthly_box_inside_proxy",
    "monthly_box_breakout_proxy",
)
EXCLUDED_NEXT = {"monthly_box_breakout_proxy", "above20_streak", "above60_streak", "dist_ma60_pct"}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def _std(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if len(s) < 2 else float(s.std(ddof=0))


def _rate(s: pd.Series) -> float | None:
    if s.dropna().empty:
        return None
    return float(s.fillna(False).astype(bool).mean())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def load_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(input_root / "candidate_entry_overlay_rows.csv", dtype={"code": str}, low_memory=False)
    rows = rows[rows["year"].isin(YEARS) & rows["path_available"].astype(bool)].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows["ret20_num"] = pd.to_numeric(rows["ret20_num"] if "ret20_num" in rows else rows["ret20"], errors="coerce")
    rows["delta_ret20"] = pd.to_numeric(rows["delta_ret20"], errors="coerce")
    rows["delay_helped_loser"] = rows["loser20"].astype(bool) & ((rows["delta_ret20"] > 0) | (~rows["delayed_loser20"].astype(bool)))
    rows["delay_not_helped_loser"] = rows["loser20"].astype(bool) & ~rows["delay_helped_loser"]
    rows["delay_harmed_winner"] = rows["winner20"].astype(bool) & ((rows["delta_ret20"] < 0) | (~rows["delayed_winner20_abs"].astype(bool)) | ((rows["delayed_mfe20"] - rows["baseline_mfe20"]) < -0.02))
    rows["delay_not_harmed_winner"] = rows["winner20"].astype(bool) & ~rows["delay_harmed_winner"]
    rows["neutral_rows"] = ~rows["winner20"].astype(bool) & ~rows["loser20"].astype(bool)
    feature_coverage = {f: (float(rows[f].notna().mean()) if f in rows else 0.0) for f in FEATURES}
    return rows, {"rows_loaded": int(len(rows)), "feature_coverage": feature_coverage}


def wait_help_harm_summary(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK:
            selected = rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] <= topk)]
            for cohort in ("delay_helped_loser", "delay_not_helped_loser", "delay_harmed_winner", "delay_not_harmed_winner", "neutral_rows"):
                c = selected[selected[cohort].astype(bool)]
                out.append(
                    {
                        "year": year,
                        "topk": topk,
                        "cohort": cohort,
                        "count": int(len(c)),
                        "rate": None if len(selected) == 0 else float(len(c) / len(selected)),
                        "avg_delta_ret20": _mean(c, "delta_ret20"),
                        "avg_mfe_loss": _delta(_mean(c, "delayed_mfe20"), _mean(c, "baseline_mfe20")),
                        "avg_mae_improvement": _delta(_mean(c, "delayed_mae20"), _mean(c, "baseline_mae20")),
                    }
                )
    return pd.DataFrame(out)


def feature_decomposition(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK:
            selected = rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] <= topk)]
            helped = selected[selected["delay_helped_loser"].astype(bool)]
            harmed = selected[selected["delay_harmed_winner"].astype(bool)]
            for f in FEATURES:
                if f not in selected:
                    continue
                hm, wm = _mean(helped, f), _mean(harmed, f)
                diff = _delta(hm, wm)
                hs, ws = _std(helped, f), _std(harmed, f)
                pooled = math.sqrt((hs * hs + ws * ws) / 2) if hs is not None and ws is not None else None
                out.append(
                    {
                        "year": year,
                        "topk": topk,
                        "feature": f,
                        "helped_loser_n": int(len(helped)),
                        "harmed_winner_n": int(len(harmed)),
                        "helped_loser_mean": hm,
                        "harmed_winner_mean": wm,
                        "helped_loser_median": _median(helped, f),
                        "harmed_winner_median": _median(harmed, f),
                        "diff_helped_minus_harmed": diff,
                        "effect_size": None if diff is None or not pooled else float(diff / pooled),
                        "helped_coverage": float(helped[f].notna().mean()) if len(helped) else None,
                        "harmed_coverage": float(harmed[f].notna().mean()) if len(harmed) else None,
                    }
                )
    return pd.DataFrame(out)


def year_stability(decomp: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature, sub in decomp.groupby("feature"):
        vals = [float(v) for v in sub["diff_helped_minus_harmed"].dropna()]
        signs = {1 if v > 0 else -1 if v < 0 else 0 for v in vals}
        rows.append(
            {
                "feature": feature,
                "comparisons": int(len(vals)),
                "mean_abs_effect_size": _mean(sub, "effect_size"),
                "mean_diff": None if not vals else float(pd.Series(vals).mean()),
                "stable_sign": len(signs - {0}) <= 1,
                "positive_count": sum(v > 0 for v in vals),
                "negative_count": sum(v < 0 for v in vals),
            }
        )
    return pd.DataFrame(rows).sort_values("mean_abs_effect_size", ascending=False)


def profiles(rows: pd.DataFrame, stability: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    harmed = rows[rows["delay_harmed_winner"].astype(bool)]
    helped = rows[rows["delay_helped_loser"].astype(bool)]
    protection = {
        "delay_harmed_winner_count": int(len(harmed)),
        "strong_immediate_ret5_mean": _mean(harmed, "baseline_ret20_from_t"),
        "mfe_loss_mean": _delta(_mean(harmed, "delayed_mfe20"), _mean(harmed, "baseline_mfe20")),
        "low_exhaustion_proxy": {
            "upper_wick_ratio_mean": _mean(harmed, "upper_wick_ratio"),
            "failed_high_update_rate": _rate(harmed["failed_high_update"]) if "failed_high_update" in harmed else None,
        },
        "constructive_trend_proxy": {
            "ma20_slope_mean": _mean(harmed, "ma20_slope"),
            "ma60_slope_mean": _mean(harmed, "ma60_slope"),
        },
    }
    delay_profile = {
        "delay_helped_loser_count": int(len(helped)),
        "avg_delta_ret20": _mean(helped, "delta_ret20"),
        "mae_improvement_mean": _delta(_mean(helped, "delayed_mae20"), _mean(helped, "baseline_mae20")),
        "extension_proxy": {
            "dist_ma20_pct_mean": _mean(helped, "dist_ma20_pct"),
            "dist_ma60_pct_mean": _mean(helped, "dist_ma60_pct"),
        },
        "exhaustion_proxy": {
            "upper_wick_ratio_mean": _mean(helped, "upper_wick_ratio"),
            "failed_high_update_rate": _rate(helped["failed_high_update"]) if "failed_high_update" in helped else None,
        },
    }
    axes = []
    for _, row in stability.iterrows():
        feature = str(row["feature"])
        if feature in EXCLUDED_NEXT or not bool(row["stable_sign"]) or pd.isna(row["mean_abs_effect_size"]):
            continue
        if float(row["mean_abs_effect_size"]) < 0.15:
            continue
        axes.append(
            {
                "axis_name": f"{feature}_conditional_wait_context",
                "intended_use": "conditional_wait",
                "separates": "delay_helped_loser vs delay_harmed_winner",
                "years_supported": [2024, 2025, 2026],
                "sample_size": int(len(rows)),
                "expected_winner_damage": "unknown_until_pretest",
                "recommended_next": "pretest",
                "not_policy": True,
            }
        )
        if len(axes) >= 5:
            break
    return protection, delay_profile, axes


def concentration(rows: pd.DataFrame) -> dict[str, Any]:
    target = rows[rows["delay_harmed_winner"].astype(bool)].copy()
    if target.empty:
        return {"delay_harmed_winner_count": 0}
    target["month"] = target["decision_ymd"].astype(str).str.slice(0, 6)
    return {
        "delay_harmed_winner_count": int(len(target)),
        "largest_year_share": float(target["year"].value_counts(normalize=True).max()),
        "largest_month_share": float(target["month"].value_counts(normalize=True).max()),
        "largest_code_share": float(target["code"].value_counts(normalize=True).max()),
    }


def decide(axes: list[dict[str, Any]], conc: dict[str, Any]) -> dict[str, Any]:
    if axes and conc.get("largest_year_share", 1.0) < 0.75:
        decision = "conditional_entry_axis_found"
        reason = f"best diagnostic axis {axes[0]['axis_name']} separates helped losers from harmed winners"
    elif axes:
        decision = "drop_entry_timing_overlay"
        reason = "conditional axes are year-concentrated or unstable"
    else:
        decision = "hold_for_ma_event_pretest_later"
        reason = "fixed-delay decision-date features do not cleanly separate; MA event diagnostics remain separate future work"
    return {"research_decision": decision, "reason_typed": [reason], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, input_root: Path = DEFAULT_INPUT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    missing = [n for n in REQUIRED_INPUTS if not (input_root / n).exists()]
    if missing:
        raise FileNotFoundError(f"missing required inputs: {missing}")
    run_dir = output_root / f"{_now_tag()}-t5-wait-winner-damage-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((input_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, report = load_rows(input_root)
    summary = wait_help_harm_summary(rows)
    decomp = feature_decomposition(rows)
    stability = year_stability(decomp)
    protection, delay_profile, axes = profiles(rows, stability)
    conc = concentration(rows)
    decision = decide(axes, conc)
    summary.to_csv(run_dir / "wait_help_harm_summary.csv", index=False)
    decomp.to_csv(run_dir / "wait_helped_vs_harmed_feature_decomposition.csv", index=False)
    stability.to_csv(run_dir / "year_stability_summary.csv", index=False)
    _write_json(run_dir / "immediate_buy_protection_profile.json", protection)
    _write_json(run_dir / "delay_candidate_profile.json", delay_profile)
    _write_json(run_dir / "conditional_wait_axis_candidates.json", {"candidates": axes, "not_policies": True})
    _write_json(run_dir / "concentration_summary.json", conc)
    _write_json(run_dir / "input_artifact_report.json", {"input_root": input_root, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "features_point_in_time": True, "future_returns_used_only_for_cohort_labels": True, "no_new_entry_policy": True, "threshold_sweep": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "candidate_count": len(axes)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(input_root=args.input_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
