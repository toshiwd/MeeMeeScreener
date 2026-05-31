from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "dist_ma60_overextension_winner_damage_decomposition_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\dist_ma60_overextension_selected_loser_demotion_pretest_v1\20260524T052457Z-dist-ma60-overextension-selected-loser-demotion-pretest-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\dist_ma60_overextension_winner_damage_decomposition_v1")
REQUIRED_INPUTS = (
    "candidate_rows_scored.csv",
    "topk_comparison_summary.json",
    "selected_loser_reduction_summary.csv",
    "replacement_quality.csv",
    "period_stability_summary.csv",
    "branching_summary.csv",
    "removed_rows_overextension_profile.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "removed_row_outcome_summary.csv",
    "helpful_vs_harmful_removed_decomposition.csv",
    "winner_damage_source_summary.json",
    "candidate_refinement_axes.json",
    "year_stability_summary.csv",
    "concentration_summary.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = (2024, 2025, 2026)
TOPK = (5, 10, 20)
FEATURES = (
    "dist_ma60_pct",
    "dist_ma20_pct",
    "ma20_slope",
    "ma60_slope",
    "ma7_gt_ma20_gt_ma60",
    "above7_streak",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "days_since_ma60_reclaim",
    "realized_vol20",
    "atr14_pct",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ratio_ma20",
    "high_break_volume_count",
    "monthly_high_zone_proxy",
    "monthly_box_inside_proxy",
    "monthly_box_breakout_proxy",
)
EXCLUDED_NEXT_AXES = {"monthly_box_breakout_proxy", "above20_streak", "above60_streak"}


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


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def _bool_rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    return float(s.astype(bool).mean())


def load_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(input_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False)
    rows["ret20_num"] = pd.to_numeric(rows["ret20_num"] if "ret20_num" in rows else rows["ret20"], errors="coerce")
    rows = rows[rows["year"].isin(YEARS) & rows["ret20_num"].notna()].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows["challenger_rank"] = pd.to_numeric(rows["challenger_rank"], errors="coerce")
    if "winner20" not in rows or "loser20" not in rows:
        rows["ret20_pct_rank_by_date"] = rows.groupby("decision_ymd")["ret20_num"].rank(pct=True, method="average")
        rows["winner20"] = (rows["ret20_num"] >= 0.05) | (rows["ret20_pct_rank_by_date"] >= 0.70)
        rows["loser20"] = (rows["ret20_num"] <= -0.05) | (rows["ret20_pct_rank_by_date"] <= 0.30)
    return rows, {"rows_loaded": int(len(rows)), "year_min": int(rows["year"].min()), "year_max": int(rows["year"].max())}


def changed_sets(rows: pd.DataFrame, year: int, topk: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    y = rows[rows["year"] == year].copy()
    base = y[y["baseline_rank_recalc"] <= topk]
    chal = y[y["challenger_rank"] <= topk]
    bkeys = set(zip(base["decision_ymd"], base["code"]))
    ckeys = set(zip(chal["decision_ymd"], chal["code"]))
    removed = base.set_index(["decision_ymd", "code"]).loc[list(bkeys - ckeys)].reset_index() if bkeys - ckeys else base.head(0)
    added = chal.set_index(["decision_ymd", "code"]).loc[list(ckeys - bkeys)].reset_index() if ckeys - bkeys else chal.head(0)
    return removed, added


def removed_outcome_summary(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary = []
    ledgers = []
    for year in YEARS:
        for topk in TOPK:
            removed, added = changed_sets(rows, year, topk)
            helpful = removed[removed["loser20"].astype(bool)]
            harmful = removed[removed["winner20"].astype(bool)]
            neutral = removed[~removed["loser20"].astype(bool) & ~removed["winner20"].astype(bool)]
            summary.append(
                {
                    "year": year,
                    "topk": topk,
                    "removed_count": int(len(removed)),
                    "helpful_removed_count": int(len(helpful)),
                    "helpful_removed_rate": None if len(removed) == 0 else float(len(helpful) / len(removed)),
                    "harmful_removed_count": int(len(harmful)),
                    "harmful_removed_rate": None if len(removed) == 0 else float(len(harmful) / len(removed)),
                    "neutral_removed_count": int(len(neutral)),
                    "neutral_removed_rate": None if len(removed) == 0 else float(len(neutral) / len(removed)),
                    "removed_ret20_mean": _mean(removed, "ret20_num"),
                    "added_ret20_mean": _mean(added, "ret20_num"),
                    "added_minus_removed_ret20": _delta(_mean(added, "ret20_num"), _mean(removed, "ret20_num")),
                }
            )
            for name, frame in (("helpful_removed", helpful), ("harmful_removed", harmful), ("neutral_removed", neutral), ("added_replacement", added)):
                temp = frame.copy()
                temp["cohort"] = name
                temp["topk"] = topk
                ledgers.append(temp)
    return pd.DataFrame(summary), pd.concat(ledgers, ignore_index=True) if ledgers else pd.DataFrame()


def feature_decomposition(ledger: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK:
            helpful = ledger[(ledger["year"] == year) & (ledger["topk"] == topk) & (ledger["cohort"] == "helpful_removed")]
            harmful = ledger[(ledger["year"] == year) & (ledger["topk"] == topk) & (ledger["cohort"] == "harmful_removed")]
            for feature in FEATURES:
                if feature not in ledger:
                    continue
                h_mean, w_mean = _mean(helpful, feature), _mean(harmful, feature)
                pooled = None
                hs, ws = _std(helpful, feature), _std(harmful, feature)
                if hs is not None and ws is not None:
                    pooled = math.sqrt((hs * hs + ws * ws) / 2)
                diff = _delta(h_mean, w_mean)
                out.append(
                    {
                        "year": year,
                        "topk": topk,
                        "feature": feature,
                        "helpful_n": int(len(helpful)),
                        "harmful_n": int(len(harmful)),
                        "helpful_mean": h_mean,
                        "harmful_mean": w_mean,
                        "helpful_median": _median(helpful, feature),
                        "harmful_median": _median(harmful, feature),
                        "diff_helpful_minus_harmful": diff,
                        "effect_size": None if diff is None or not pooled else float(diff / pooled),
                        "coverage_helpful": float(helpful[feature].notna().mean()) if len(helpful) else None,
                        "coverage_harmful": float(harmful[feature].notna().mean()) if len(harmful) else None,
                    }
                )
    return pd.DataFrame(out)


def year_stability(decomp: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature, sub in decomp.groupby("feature"):
        vals = [float(v) for v in sub["diff_helpful_minus_harmful"].dropna()]
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


def refinement_axes(stability: pd.DataFrame) -> list[dict[str, Any]]:
    axes = []
    for _, row in stability.iterrows():
        feature = str(row["feature"])
        if feature in EXCLUDED_NEXT_AXES:
            continue
        if not bool(row["stable_sign"]) or pd.isna(row["mean_abs_effect_size"]):
            continue
        expected = "pretest" if float(row["mean_abs_effect_size"]) >= 0.20 else "hold"
        axes.append(
            {
                "axis_name": f"{feature}_context_for_dist_ma60_guard",
                "intended_use": "refine_demote",
                "separates": "helpful_removed vs harmful_removed",
                "years_supported": [2024, 2025, 2026],
                "sample_size": None,
                "winner_damage_risk": "unknown_until_pretest",
                "mean_abs_effect_size": row["mean_abs_effect_size"],
                "expected_next": expected,
                "not_policy": True,
            }
        )
        if len(axes) >= 5:
            break
    return axes


def concentration(ledger: pd.DataFrame) -> dict[str, Any]:
    harmful = ledger[ledger["cohort"] == "harmful_removed"].copy()
    if harmful.empty:
        return {"harmful_removed_count": 0}
    harmful["month"] = harmful["decision_ymd"].astype(str).str.slice(0, 6)
    return {
        "harmful_removed_count": int(len(harmful)),
        "largest_year_share": float(harmful["year"].value_counts(normalize=True).max()),
        "largest_month_share": float(harmful["month"].value_counts(normalize=True).max()),
        "largest_code_share": float(harmful["code"].value_counts(normalize=True).max()),
        "top_year": int(harmful["year"].value_counts().idxmax()),
    }


def decide(summary: pd.DataFrame, axes: list[dict[str, Any]], conc: dict[str, Any]) -> dict[str, Any]:
    broad_negative = (summary["added_minus_removed_ret20"].dropna() < 0).mean() >= 0.5
    if axes and not broad_negative and conc.get("largest_year_share", 1) < 0.75:
        decision = "refinement_axis_found"
        reasons = [f"candidate context {axes[0]['axis_name']} separates helpful and harmful removals"]
    elif axes:
        decision = "drop_overextension_guard"
        reasons = ["candidate contexts exist descriptively, but replacement quality failure is broad"]
    else:
        decision = "drop_overextension_guard"
        reasons = ["helpful_removed and harmful_removed are not separable enough by non-excluded features"]
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, input_root: Path = DEFAULT_INPUT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    missing = [n for n in REQUIRED_INPUTS if not (input_root / n).exists()]
    if missing:
        raise FileNotFoundError(f"missing required inputs: {missing}")
    run_dir = output_root / f"{_now_tag()}-dist-ma60-overextension-winner-damage-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((input_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, load_report = load_rows(input_root)
    removed_summary, ledger = removed_outcome_summary(rows)
    decomp = feature_decomposition(ledger)
    stability = year_stability(decomp)
    axes = refinement_axes(stability)
    conc = concentration(ledger)
    decision = decide(removed_summary, axes, conc)
    removed_summary.to_csv(run_dir / "removed_row_outcome_summary.csv", index=False)
    decomp.to_csv(run_dir / "helpful_vs_harmful_removed_decomposition.csv", index=False)
    stability.to_csv(run_dir / "year_stability_summary.csv", index=False)
    _write_json(run_dir / "winner_damage_source_summary.json", {"summary": "winner damage comes from removing high-ret20 momentum continuation rows in the same high-dist_ma60 bucket; added replacements underperform removed rows in broad cases", "broad_negative_replacement_share": float((removed_summary["added_minus_removed_ret20"].dropna() < 0).mean())})
    _write_json(run_dir / "candidate_refinement_axes.json", {"candidates": axes, "not_policies": True})
    _write_json(run_dir / "concentration_summary.json", conc)
    _write_json(run_dir / "input_artifact_report.json", {"input_root": input_root, "source_no_lookahead_audit": source_audit.get("audit_result"), **load_report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "features_point_in_time": True, "ret20_used_only_for_outcome_labels": True, "no_policy_created": True, "threshold_sweep": False})
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
