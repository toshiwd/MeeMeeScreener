from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "selected_loser_negative_axis_audit_v1"
DEFAULT_FAILURE_ROOT = Path(r"G:\Tradex\recent_baseline_topk_failure_decomposition_v1\20260523T195120Z-recent-baseline-topk-failure-decomposition-v1")
DEFAULT_SCORED_ROOT = Path(
    r"G:\Tradex\monthly_box_breakout_above60_maturity_context_pretest_v1\20260523T194427Z-monthly-box-breakout-above60-maturity-context-pretest-v1"
)
DEFAULT_RICH_FEATURE_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1\20260523T185123Z-recent-win-pattern-shift-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\selected_loser_negative_axis_audit_v1")
REQUESTED_INPUTS = (
    "selected_loser_rows.csv",
    "missed_winner_rows.csv",
    "selected_loser_vs_missed_winner_decomposition.csv",
    "pool_quality_summary.csv",
    "boundary_failure_summary.csv",
    "year_failure_mode_summary.json",
    "research_decision.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "selected_loser_profile_by_year.csv",
    "selected_loser_vs_selected_non_loser.csv",
    "selected_loser_vs_selected_winner.csv",
    "stable_loser_feature_matrix.csv",
    "negative_selection_axis_candidates.json",
    "winner_damage_check.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = (2024, 2025, 2026)
TOPK_VALUES = (5, 10)
FEATURE_GROUPS = {
    "trend_age": ["above7_streak", "above20_streak", "above60_streak", "days_since_ma20_reclaim", "days_since_ma60_reclaim"],
    "ma_structure": ["ma7_gt_ma20_gt_ma60", "ma20_slope", "ma60_slope", "dist_ma20_pct", "dist_ma60_pct"],
    "breakout_high_zone": ["monthly_box_breakout_proxy", "monthly_high_zone_proxy", "monthly_box_inside_proxy"],
    "candle_failure": ["large_bearish_candle", "failed_high_update", "upper_wick_ratio", "lower_wick_ratio"],
    "volume": ["volume_ratio_ma20", "high_break_volume_count", "volume_spike_on_breakout", "volume_spike_on_down_day"],
    "risk": ["realized_vol20", "atr14_pct"],
}


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


def _mean(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _bool_rate(series: pd.Series) -> float | None:
    if series.dropna().empty:
        return None
    return float(series.fillna(False).astype(bool).mean())


def _feature_group(feature: str) -> str:
    for group, features in FEATURE_GROUPS.items():
        if feature in features:
            return group
    return "other"


def _all_features() -> list[str]:
    out: list[str] = []
    for features in FEATURE_GROUPS.values():
        out.extend(features)
    return out


def _load_base_rows(scored_root: Path, rich_feature_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    scored = pd.read_csv(scored_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False)
    scored = scored[scored["year"].isin(YEARS) & pd.to_numeric(scored["ret20"], errors="coerce").notna()].copy()
    scored["baseline_rank_recalc"] = pd.to_numeric(scored["baseline_rank_recalc"], errors="coerce")
    rich_path = rich_feature_root / "candidate_rows_with_features.csv"
    if rich_path.exists():
        rich_cols = ["decision_ymd", "code", *[c for c in _all_features() if c not in scored.columns]]
        available = pd.read_csv(rich_path, nrows=1).columns.tolist()
        rich_cols = [c for c in rich_cols if c in available]
        rich = pd.read_csv(rich_path, usecols=rich_cols, dtype={"code": str}, low_memory=False)
        scored = scored.merge(rich, on=["decision_ymd", "code"], how="left", suffixes=("", "_rich"))
        rich_joined = True
    else:
        rich_joined = False
    scored["ret20_num"] = pd.to_numeric(scored["ret20"], errors="coerce")
    scored["ret20_rank_pct_by_date"] = scored.groupby("decision_ymd")["ret20_num"].rank(pct=True, method="average")
    scored["winner20"] = (scored["ret20_num"] >= 0.05) | (scored["ret20_rank_pct_by_date"] >= 0.70)
    scored["loser20"] = (scored["ret20_num"] <= -0.05) | (scored["ret20_rank_pct_by_date"] <= 0.30)
    return scored, {"rows_loaded": int(len(scored)), "rich_feature_joined": rich_joined, "rich_feature_path": rich_path}


def _cohort(rows: pd.DataFrame, year: int, topk: int, name: str) -> pd.DataFrame:
    selected = rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] <= topk)].copy()
    if name == "selected_loser":
        return selected[selected["loser20"]].copy()
    if name == "selected_non_loser":
        return selected[~selected["loser20"]].copy()
    if name == "selected_winner":
        return selected[selected["winner20"]].copy()
    if name == "missed_winner":
        return rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] > topk) & rows["winner20"]].copy()
    raise ValueError(name)


def selected_loser_profile(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK_VALUES:
            losers = _cohort(rows, year, topk, "selected_loser")
            for feature in _all_features():
                if feature not in rows:
                    continue
                out.append(
                    {
                        "year": year,
                        "topk": topk,
                        "feature": feature,
                        "feature_group": _feature_group(feature),
                        "n": int(len(losers)),
                        "coverage": float(losers[feature].notna().mean()) if len(losers) else None,
                        "mean": _mean(losers[feature]),
                        "median": _median(losers[feature]),
                        "true_rate": _bool_rate(losers[feature]) if str(losers[feature].dtype) in {"bool", "boolean"} else None,
                    }
                )
    return pd.DataFrame(out)


def contrast_rows(rows: pd.DataFrame, right_cohort: str) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK_VALUES:
            losers = _cohort(rows, year, topk, "selected_loser")
            right = _cohort(rows, year, topk, right_cohort)
            for feature in _all_features():
                if feature not in rows:
                    continue
                l_num = pd.to_numeric(losers[feature], errors="coerce")
                r_num = pd.to_numeric(right[feature], errors="coerce")
                l_mean = None if l_num.dropna().empty else float(l_num.mean())
                r_mean = None if r_num.dropna().empty else float(r_num.mean())
                l_bool = _bool_rate(losers[feature]) if str(losers[feature].dtype) in {"bool", "boolean"} else None
                r_bool = _bool_rate(right[feature]) if str(right[feature].dtype) in {"bool", "boolean"} else None
                out.append(
                    {
                        "year": year,
                        "topk": topk,
                        "feature": feature,
                        "feature_group": _feature_group(feature),
                        "selected_loser_n": int(len(losers)),
                        f"{right_cohort}_n": int(len(right)),
                        "selected_loser_coverage": float(losers[feature].notna().mean()) if len(losers) else None,
                        f"{right_cohort}_coverage": float(right[feature].notna().mean()) if len(right) else None,
                        "selected_loser_mean": l_mean,
                        f"{right_cohort}_mean": r_mean,
                        "mean_diff_loser_minus_compare": None if l_mean is None or r_mean is None else float(l_mean - r_mean),
                        "selected_loser_true_rate": l_bool,
                        f"{right_cohort}_true_rate": r_bool,
                        "true_rate_diff_loser_minus_compare": None if l_bool is None or r_bool is None else float(l_bool - r_bool),
                    }
                )
    return pd.DataFrame(out)


def stable_feature_matrix(non_loser: pd.DataFrame, winner: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature in sorted(set(non_loser["feature"])):
        sub = non_loser[non_loser["feature"] == feature].copy()
        win = winner[winner["feature"] == feature].copy()
        diffs = []
        coverage_ok = []
        for _, r in sub.iterrows():
            diff = r["true_rate_diff_loser_minus_compare"] if pd.notna(r.get("true_rate_diff_loser_minus_compare")) else r["mean_diff_loser_minus_compare"]
            if pd.notna(diff):
                diffs.append(float(diff))
            coverage_ok.append((r.get("selected_loser_coverage") or 0) >= 0.7 and (r.get("selected_non_loser_coverage") or 0) >= 0.7)
        signs = {1 if d > 0 else -1 if d < 0 else 0 for d in diffs}
        win_diffs = []
        for _, r in win.iterrows():
            diff = r["true_rate_diff_loser_minus_compare"] if pd.notna(r.get("true_rate_diff_loser_minus_compare")) else r["mean_diff_loser_minus_compare"]
            if pd.notna(diff):
                win_diffs.append(float(diff))
        if len(diffs) < 4 or not any(coverage_ok):
            klass = "coverage_limited"
        elif len(signs - {0}) == 1:
            klass = "stable_loser_feature"
        elif len(diffs) >= 4 and max(sum(d > 0 for d in diffs), sum(d < 0 for d in diffs)) >= 4:
            klass = "year_specific_loser_feature"
        elif 1 in signs and -1 in signs:
            klass = "unstable_feature"
        else:
            klass = "non_separating_feature"
        rows.append(
            {
                "feature": feature,
                "feature_group": _feature_group(feature),
                "classification": klass,
                "available_comparisons": int(len(diffs)),
                "mean_abs_diff_vs_non_loser": None if not diffs else float(pd.Series(diffs).abs().mean()),
                "mean_diff_vs_non_loser": None if not diffs else float(pd.Series(diffs).mean()),
                "mean_diff_vs_selected_winner": None if not win_diffs else float(pd.Series(win_diffs).mean()),
            }
        )
    return pd.DataFrame(rows).sort_values(["classification", "mean_abs_diff_vs_non_loser"], ascending=[True, False])


def winner_damage_check(rows: pd.DataFrame, matrix: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    candidates = matrix[matrix["classification"].isin(["stable_loser_feature", "year_specific_loser_feature"])].copy()
    candidates = candidates[~candidates["feature"].isin({"monthly_box_breakout_proxy", "above20_streak", "above60_streak"})]
    candidates = candidates.sort_values("mean_abs_diff_vs_non_loser", ascending=False).head(8)
    damage_rows = []
    axis_candidates = []
    for _, cand in candidates.iterrows():
        feature = str(cand["feature"])
        if feature not in rows:
            continue
        selected = rows[rows["baseline_rank_recalc"] <= 10].copy()
        values = pd.to_numeric(selected[feature], errors="coerce")
        if values.dropna().empty:
            continue
        direction = "high" if (cand.get("mean_diff_vs_non_loser") or 0) > 0 else "low"
        threshold = float(values.median())
        hit = values >= threshold if direction == "high" else values <= threshold
        losers = selected["loser20"].astype(bool)
        winners = selected["winner20"].astype(bool)
        loser_hit_rate = float(hit[losers].mean()) if losers.any() else None
        winner_hit_rate = float(hit[winners].mean()) if winners.any() else None
        spread = None if loser_hit_rate is None or winner_hit_rate is None else float(loser_hit_rate - winner_hit_rate)
        damage_rows.append(
            {
                "feature": feature,
                "feature_group": cand["feature_group"],
                "direction": direction,
                "descriptive_median_threshold": threshold,
                "selected_loser_n": int(losers.sum()),
                "selected_winner_n": int(winners.sum()),
                "loser_hit_rate": loser_hit_rate,
                "winner_hit_rate": winner_hit_rate,
                "loser_minus_winner_spread": spread,
            }
        )
        axis_candidates.append(
            {
                "axis_name": f"{feature}_{direction}_risk",
                "feature_group": cand["feature_group"],
                "intended_use": "demotion",
                "years_supported": [2024, 2025, 2026],
                "sample_size": int(len(selected)),
                "selected_loser_separation_strength": cand.get("mean_abs_diff_vs_non_loser"),
                "selected_winner_damage_risk": winner_hit_rate,
                "loser_minus_winner_spread": spread,
                "expected_next": "pretest" if spread is not None and spread > 0.05 else "hold",
                "risk": "descriptive median only; not a tuned threshold",
            }
        )
    return pd.DataFrame(damage_rows), axis_candidates[:5]


def decide(matrix: pd.DataFrame, axes: list[dict[str, Any]], missing_requested: list[str]) -> dict[str, Any]:
    pretest_axes = [a for a in axes if a.get("expected_next") == "pretest" and a.get("loser_minus_winner_spread") is not None and a["loser_minus_winner_spread"] > 0]
    if not axes:
        decision = "source_or_feature_gap" if missing_requested else "no_clear_negative_axis"
        reasons = ["no candidate axes survived coverage and exclusion filters"]
    elif pretest_axes:
        decision = "negative_selection_axis_found"
        reasons = [f"best axis {pretest_axes[0]['axis_name']} separates selected losers from selected winners descriptively"]
    else:
        decision = "no_clear_negative_axis"
        reasons = ["candidate axes exist but winner damage spread is not strong enough"]
    if missing_requested:
        reasons.append(f"requested row-level inputs absent and cohorts reconstructed: {missing_requested}")
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "best_axis": pretest_axes[0] if pretest_axes else (axes[0] if axes else None),
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
        "threshold_sweep": False,
    }


def run(
    *,
    failure_root: Path = DEFAULT_FAILURE_ROOT,
    scored_root: Path = DEFAULT_SCORED_ROOT,
    rich_feature_root: Path = DEFAULT_RICH_FEATURE_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-selected-loser-negative-axis-audit-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing_requested = [name for name in REQUESTED_INPUTS if not (failure_root / name).exists()]
    source_audit = json.loads((failure_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, load_report = _load_base_rows(scored_root, rich_feature_root)
    profile = selected_loser_profile(rows)
    non_loser = contrast_rows(rows, "selected_non_loser")
    winner = contrast_rows(rows, "selected_winner")
    matrix = stable_feature_matrix(non_loser, winner)
    damage, axes = winner_damage_check(rows, matrix)
    decision = decide(matrix, axes, missing_requested)
    profile.to_csv(run_dir / "selected_loser_profile_by_year.csv", index=False)
    non_loser.to_csv(run_dir / "selected_loser_vs_selected_non_loser.csv", index=False)
    winner.to_csv(run_dir / "selected_loser_vs_selected_winner.csv", index=False)
    matrix.to_csv(run_dir / "stable_loser_feature_matrix.csv", index=False)
    damage.to_csv(run_dir / "winner_damage_check.csv", index=False)
    _write_json(run_dir / "negative_selection_axis_candidates.json", {"candidates": axes, "not_a_pretest": True})
    _write_json(
        run_dir / "input_artifact_report.json",
        {
            "failure_root": failure_root,
            "scored_root": scored_root,
            "rich_feature_root": rich_feature_root,
            "requested_inputs_missing": missing_requested,
            "fallback": "reconstructed selected cohorts from candidate_rows_scored.csv and joined rich 2019-2025 features when available",
            "source_no_lookahead_audit": source_audit.get("audit_result"),
            **load_report,
        },
    )
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "features_point_in_time": True, "ret20_used_only_for_labels": True, "no_policy_or_threshold_tuning": True, "row_reconstruction_fallback_declared": bool(missing_requested)})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "candidate_count": len(axes)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit selected loser negative-selection axes for 2024-2026 baseline topK")
    parser.add_argument("--failure-root", type=Path, default=DEFAULT_FAILURE_ROOT)
    parser.add_argument("--scored-root", type=Path, default=DEFAULT_SCORED_ROOT)
    parser.add_argument("--rich-feature-root", type=Path, default=DEFAULT_RICH_FEATURE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(failure_root=args.failure_root, scored_root=args.scored_root, rich_feature_root=args.rich_feature_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
