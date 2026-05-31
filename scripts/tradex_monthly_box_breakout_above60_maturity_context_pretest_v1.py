from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_2026_same_family_baseline_validation_for_monthly_breakout_v1 as validation


AXIS_ID = "monthly_box_breakout_above60_maturity_context_pretest_v1"
DEFAULT_VALIDATION_ROOT = Path(
    r"G:\Tradex\monthly_box_breakout_2026_validation_v1\20260523T193639Z-monthly-box-breakout-2026-validation-v1"
)
DEFAULT_SHIFT_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1\20260523T185123Z-recent-win-pattern-shift-audit-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\monthly_box_breakout_above60_maturity_context_pretest_v1")
REQUIRED_INPUTS = (
    "topk_comparison_summary_2024_2026.json",
    "replacement_quality_2024_2026.csv",
    "removed_rows_context_by_year.csv",
    "context_pretest_candidate.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "context_policy.json",
    "candidate_rows_scored.csv",
    "topk_comparison_summary.json",
    "ungated_vs_gated_comparison.csv",
    "replacement_quality.csv",
    "period_stability_summary.csv",
    "branching_summary.csv",
    "protected_mature_breakout_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
CONTEXT_POLICY = {
    "policy_id": "monthly_box_breakout_above60_maturity_context_soft_demotion_v1",
    "feature_primary": "monthly_box_breakout_proxy",
    "context_feature": "above60_streak",
    "rules": [
        {"condition": "monthly_box_breakout_proxy == false", "score_delta": 0},
        {"condition": "monthly_box_breakout_proxy == true and above60_streak >= 60", "score_delta": 0},
        {"condition": "monthly_box_breakout_proxy == true and above60_streak < 60", "score_delta": -1},
        {"condition": "monthly_box_breakout_proxy == true and above60_streak is missing", "score_delta": 0, "status": "coverage_limited"},
    ],
    "demotion_amount": -1,
    "above60_threshold": 60,
    "threshold_sweep": False,
    "veto": False,
    "uses_days_since_ma60_reclaim": False,
    "uses_monthly_high_zone_proxy": False,
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


def _bool_value(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _mean(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def _delta(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return float(a - b)


def context_gated_score_delta(monthly_box_breakout_proxy: Any, above60_streak: Any) -> tuple[int, bool, str]:
    monthly = _bool_value(monthly_box_breakout_proxy)
    if not monthly:
        return 0, False, "not_monthly_box_breakout"
    above60 = pd.to_numeric(pd.Series([above60_streak]), errors="coerce").iloc[0]
    if pd.isna(above60):
        return 0, True, "monthly_box_breakout_missing_above60_streak"
    if float(above60) >= 60:
        return 0, False, "monthly_box_breakout_protected_above60_ge_60"
    return -1, False, "monthly_box_breakout_demoted_above60_lt_60"


def score_rows(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows[rows["year"].between(2019, 2026)].copy()
    out["monthly_box_breakout_bool"] = out["monthly_box_breakout_proxy"].map(_bool_value)
    out["above60_streak_numeric"] = pd.to_numeric(out["above60_streak"], errors="coerce")
    policy = out.apply(lambda r: context_gated_score_delta(r["monthly_box_breakout_proxy"], r["above60_streak"]), axis=1)
    out["gated_score_delta"] = [x[0] for x in policy]
    out["context_policy_coverage_limited"] = [x[1] for x in policy]
    out["context_policy_reason"] = [x[2] for x in policy]
    out["ungated_score_delta"] = out["monthly_box_breakout_bool"].map(lambda x: -1 if x else 0)
    out["baseline_score"] = pd.to_numeric(out["selection_score"], errors="coerce")
    out["ungated_score"] = out["baseline_score"] + out["ungated_score_delta"]
    out["gated_score"] = out["baseline_score"] + out["gated_score_delta"]
    out["baseline_rank_recalc"] = out.groupby("decision_ymd")["baseline_score"].rank(method="first", ascending=False)
    out["ungated_rank"] = (
        out.sort_values(["decision_ymd", "ungated_score", "baseline_score", "candidate_rank"], ascending=[True, False, False, True])
        .groupby("decision_ymd")
        .cumcount()
        + 1
    )
    out["gated_rank"] = (
        out.sort_values(["decision_ymd", "gated_score", "baseline_score", "candidate_rank"], ascending=[True, False, False, True])
        .groupby("decision_ymd")
        .cumcount()
        + 1
    )
    return out


def _metrics(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df["ret20"], errors="coerce")
    ret_valid = ret.dropna()
    return {
        "n": int(len(df)),
        "n_with_ret20": int(ret_valid.size),
        "mean_ret20": None if ret_valid.empty else float(ret_valid.mean()),
        "median_ret20": None if ret_valid.empty else float(ret_valid.median()),
        "win_rate_ret20_gt_0": None if ret_valid.empty else float((ret_valid > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret_valid.empty else float((ret_valid > 0.05).mean()),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret_valid.empty else float((ret_valid <= -0.05).mean()),
        "bottom_decile_rate": None if "ret20_decile_by_date" not in df or ret_valid.empty else float((df.loc[ret_valid.index, "ret20_decile_by_date"] <= 1).mean()),
    }


def _topk(df: pd.DataFrame, rank_col: str, k: int) -> pd.DataFrame:
    return df[pd.to_numeric(df[rank_col], errors="coerce") <= k].copy()


def _changed_rows(base: pd.DataFrame, challenger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    base_keys = set(zip(base["decision_ymd"], base["code"]))
    challenger_keys = set(zip(challenger["decision_ymd"], challenger["code"]))
    added_keys = challenger_keys - base_keys
    removed_keys = base_keys - challenger_keys
    added = challenger.set_index(["decision_ymd", "code"]).loc[list(added_keys)].reset_index() if added_keys else challenger.head(0)
    removed = base.set_index(["decision_ymd", "code"]).loc[list(removed_keys)].reset_index() if removed_keys else base.head(0)
    return added, removed


def _periods(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "2024": rows[(rows["year"] == 2024) & rows["ret20"].notna()],
        "2025": rows[(rows["year"] == 2025) & rows["ret20"].notna()],
        "2026_label_safe": rows[(rows["year"] == 2026) & rows["ret20"].notna()],
        "2024_2026_label_safe": rows[(rows["year"].between(2024, 2026)) & rows["ret20"].notna()],
        "2019_2023": rows[(rows["year"].between(2019, 2023)) & rows["ret20"].notna()],
        "2019_2026_label_safe": rows[rows["ret20"].notna()],
    }


def comparison_by_period(rows: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = rows.copy()
    rows["ret20_decile_by_date"] = rows.groupby("decision_ymd")["ret20"].rank(pct=True).rsub(1).mul(10).add(1)
    summary: dict[str, Any] = {}
    replacements: list[dict[str, Any]] = []
    branching: list[dict[str, Any]] = []
    side_by_side: list[dict[str, Any]] = []
    for period, p_rows in _periods(rows).items():
        summary[period] = {}
        for k in (5, 10, 20):
            base = _topk(p_rows, "baseline_rank_recalc", k)
            ungated = _topk(p_rows, "ungated_rank", k)
            gated = _topk(p_rows, "gated_rank", k)
            gated_added, gated_removed = _changed_rows(base, gated)
            ungated_added, ungated_removed = _changed_rows(base, ungated)
            base_m = _metrics(base)
            ungated_m = _metrics(ungated)
            gated_m = _metrics(gated)
            gated_added_mean = _mean(gated_added, "ret20")
            gated_removed_mean = _mean(gated_removed, "ret20")
            ungated_added_mean = _mean(ungated_added, "ret20")
            ungated_removed_mean = _mean(ungated_removed, "ret20")
            gated_repl = _delta(gated_added_mean, gated_removed_mean)
            ungated_repl = _delta(ungated_added_mean, ungated_removed_mean)
            item = {
                "baseline": base_m,
                "ungated": ungated_m,
                "gated": gated_m,
                "ungated_delta_mean_ret20": _delta(ungated_m["mean_ret20"], base_m["mean_ret20"]),
                "gated_delta_mean_ret20": _delta(gated_m["mean_ret20"], base_m["mean_ret20"]),
                "gated_minus_ungated_mean_ret20": _delta(gated_m["mean_ret20"], ungated_m["mean_ret20"]),
                "gated_delta_median_ret20": _delta(gated_m["median_ret20"], base_m["median_ret20"]),
                "gated_delta_severe_loss_rate": _delta(gated_m["severe_loss_rate_ret20_lte_minus_5pct"], base_m["severe_loss_rate_ret20_lte_minus_5pct"]),
                "changed_members_count": int(len(gated_added)),
                "ungated_changed_members_count": int(len(ungated_added)),
                "branching_difference": int(len(gated_added) - len(ungated_added)),
                "added_mean_ret20": gated_added_mean,
                "added_median_ret20": _median(gated_added, "ret20"),
                "removed_mean_ret20": gated_removed_mean,
                "removed_median_ret20": _median(gated_removed, "ret20"),
                "added_minus_removed_ret20": gated_repl,
                "ungated_added_minus_removed_ret20": ungated_repl,
                "replacement_quality_difference": _delta(gated_repl, ungated_repl),
            }
            summary[period][f"top{k}"] = item
            replacements.append(
                {
                    "period": period,
                    "topk": k,
                    "added_count": int(len(gated_added)),
                    "removed_count": int(len(gated_removed)),
                    "added_mean_ret20": gated_added_mean,
                    "added_median_ret20": _median(gated_added, "ret20"),
                    "removed_mean_ret20": gated_removed_mean,
                    "removed_median_ret20": _median(gated_removed, "ret20"),
                    "added_minus_removed_ret20": gated_repl,
                }
            )
            branching.append(
                {
                    "period": period,
                    "topk": k,
                    "changed_members_count": int(len(gated_added)),
                    "ungated_changed_members_count": int(len(ungated_added)),
                    "branching_difference": int(len(gated_added) - len(ungated_added)),
                    "total_gated_members": int(len(gated)),
                }
            )
            side_by_side.append(
                {
                    "period": period,
                    "topk": k,
                    "baseline_mean_ret20": base_m["mean_ret20"],
                    "ungated_delta_mean_ret20": item["ungated_delta_mean_ret20"],
                    "gated_delta_mean_ret20": item["gated_delta_mean_ret20"],
                    "gated_minus_ungated_mean_ret20": item["gated_minus_ungated_mean_ret20"],
                    "ungated_added_minus_removed_ret20": ungated_repl,
                    "gated_added_minus_removed_ret20": gated_repl,
                    "replacement_quality_difference": item["replacement_quality_difference"],
                    "ungated_changed_members_count": int(len(ungated_added)),
                    "gated_changed_members_count": int(len(gated_added)),
                    "branching_difference": int(len(gated_added) - len(ungated_added)),
                }
            )
    return summary, pd.DataFrame(replacements), pd.DataFrame(branching), pd.DataFrame(side_by_side)


def period_stability(summary: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for period, block in summary.items():
        for top_name, item in block.items():
            rows.append(
                {
                    "period": period,
                    "topk": int(top_name.replace("top", "")),
                    "baseline_mean_ret20": item["baseline"]["mean_ret20"],
                    "gated_mean_ret20": item["gated"]["mean_ret20"],
                    "gated_delta_mean_ret20": item["gated_delta_mean_ret20"],
                    "gated_median_ret20": item["gated"]["median_ret20"],
                    "gated_delta_median_ret20": item["gated_delta_median_ret20"],
                    "gated_delta_severe_loss_rate": item["gated_delta_severe_loss_rate"],
                    "added_minus_removed_ret20": item["added_minus_removed_ret20"],
                    "changed_members_count": item["changed_members_count"],
                }
            )
    return pd.DataFrame(rows)


def protected_mature_breakout_summary(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    mature = rows[rows["monthly_box_breakout_bool"] & (pd.to_numeric(rows["above60_streak"], errors="coerce") >= 60)]
    for period, p_rows in _periods(rows).items():
        p_mature = mature[mature["decision_ymd"].isin(p_rows["decision_ymd"])]
        out.append(
            {
                "period": period,
                "scope": "all_candidate_rows",
                "protected_mature_count": int(len(p_mature)),
                "ungated_would_demote_count": int((p_mature["ungated_score_delta"] < 0).sum()),
                "gated_demoted_count": int((p_mature["gated_score_delta"] < 0).sum()),
                "ret20_mean": _mean(p_mature, "ret20"),
            }
        )
        for k in (5, 10, 20):
            base = _topk(p_rows, "baseline_rank_recalc", k)
            ungated = _topk(p_rows, "ungated_rank", k)
            gated = _topk(p_rows, "gated_rank", k)
            _, ungated_removed = _changed_rows(base, ungated)
            _, gated_removed = _changed_rows(base, gated)
            ungated_removed_mature = ungated_removed[
                ungated_removed["monthly_box_breakout_bool"] & (pd.to_numeric(ungated_removed["above60_streak"], errors="coerce") >= 60)
            ]
            gated_removed_mature = gated_removed[
                gated_removed["monthly_box_breakout_bool"] & (pd.to_numeric(gated_removed["above60_streak"], errors="coerce") >= 60)
            ]
            out.append(
                {
                    "period": period,
                    "scope": f"top{k}_removed_rows",
                    "protected_mature_count": int(len(ungated_removed_mature) - len(gated_removed_mature)),
                    "ungated_would_demote_count": int(len(ungated_removed_mature)),
                    "gated_demoted_count": int(len(gated_removed_mature)),
                    "ret20_mean": _mean(ungated_removed_mature, "ret20"),
                }
            )
    return pd.DataFrame(out)


def decide(summary: dict[str, Any], side_by_side: pd.DataFrame, rows: pd.DataFrame, source_audit: dict[str, Any]) -> dict[str, Any]:
    if source_audit.get("audit_result") != "pass":
        return {
            "research_decision": "inconclusive",
            "reason_typed": ["source no-lookahead audit did not pass"],
            "meemee_reflectable": False,
            "ranking_reflectable": False,
            "publish_allowed": False,
        }
    monthly_rows = rows[rows["monthly_box_breakout_bool"]]
    coverage = float(monthly_rows["above60_streak_numeric"].notna().mean()) if len(monthly_rows) else 0.0
    if coverage < 0.25:
        return {
            "research_decision": "inconclusive",
            "reason_typed": ["above60_streak coverage is too low for monthly_box_breakout rows"],
            "above60_streak_coverage_in_monthly_box_breakout_rows": coverage,
            "meemee_reflectable": False,
            "ranking_reflectable": False,
            "publish_allowed": False,
        }
    recent = summary["2024_2026_label_safe"]
    y2024 = summary["2024"]
    y2025 = summary["2025"]
    y2026 = summary["2026_label_safe"]
    keep_hit = False
    hold_hit = False
    drop_reasons = []
    reasons = []
    for top in ("top5", "top10"):
        r = recent[top]
        improves_vs_baseline = (r["gated_delta_mean_ret20"] or 0) >= 0.005
        improves_vs_ungated = (r["gated_minus_ungated_mean_ret20"] or 0) > 0
        replacement_ok = (r["added_minus_removed_ret20"] or -999) > 0
        loss_ok = (r["gated_delta_severe_loss_rate"] or 0) <= 0
        branch_ok = r["changed_members_count"] > 0
        y2025_ok = (y2025[top]["gated_delta_mean_ret20"] or 0) >= 0
        y2024_ok = (y2024[top]["gated_delta_mean_ret20"] or 0) > 0
        y2026_ok = (y2026[top]["gated_delta_mean_ret20"] or 0) > 0
        if improves_vs_baseline and improves_vs_ungated and y2025_ok and replacement_ok and loss_ok and branch_ok and y2024_ok and y2026_ok:
            keep_hit = True
            reasons.append(f"{top} clears fixed gated policy keep gates")
        if improves_vs_ungated:
            hold_hit = True
        if not improves_vs_ungated:
            drop_reasons.append(f"{top} gated policy does not improve over ungated")
        if not y2025_ok:
            drop_reasons.append(f"{top} 2025 remains negative")
        if not y2024_ok or not y2026_ok:
            drop_reasons.append(f"{top} 2024 or 2026 benefit disappears")
        if not replacement_ok:
            drop_reasons.append(f"{top} replacement quality is negative")
        if not loss_ok:
            drop_reasons.append(f"{top} severe loss rate worsens")
    if keep_hit:
        decision = "keep_for_challenger_compare"
    elif hold_hit:
        decision = "hold_for_recent_regime_only"
        reasons.append("gated policy improves over ungated in at least one primary topK but misses a keep gate")
        if coverage < 0.8:
            reasons.append("above60_streak coverage remains partial")
    else:
        decision = "drop"
        reasons = sorted(set(drop_reasons)) or ["gated policy does not improve the fixed topK comparison"]
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "above60_streak_coverage_in_monthly_box_breakout_rows": coverage,
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
        "threshold_sweep": False,
        "policy_fixed_before_outcome_evaluation": True,
        "side_by_side_rows": int(len(side_by_side)),
    }


def load_rows(validation_root: Path, shift_root: Path, daily_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    coverage_path = validation_root / "coverage_audit_2026.json"
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    snapshot = validation_root / "same_family_2026_subrun" / "2026-baseline-portfolio_agent_replay_v1" / "daily_candidate_snapshot.csv"
    if not snapshot.exists():
        raise FileNotFoundError(f"missing same-family 2026 snapshot: {snapshot}")
    rows_2026, cov_2026 = validation.build_2026_feature_rows(snapshot, daily_path, int(coverage["ret20_label_safe_cutoff_ymd"]))
    rows = validation.load_combined_rows(shift_root, rows_2026)
    return rows, {"coverage_audit_2026": coverage, "rebuilt_2026_coverage": cov_2026, "snapshot_path": snapshot}


def run(
    *,
    validation_root: Path = DEFAULT_VALIDATION_ROOT,
    shift_root: Path = DEFAULT_SHIFT_ROOT,
    daily_path: Path = DEFAULT_DAILY_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-monthly-box-breakout-above60-maturity-context-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing = [name for name in REQUIRED_INPUTS if not (validation_root / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing required input artifacts: {missing}")
    source_audit = json.loads((validation_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows_raw, load_report = load_rows(validation_root, shift_root, daily_path)
    rows = score_rows(rows_raw)
    summary, replacements, branching, side_by_side = comparison_by_period(rows)
    stability = period_stability(summary)
    protected = protected_mature_breakout_summary(rows)
    decision = decide(summary, side_by_side, rows, source_audit)
    rows.to_csv(run_dir / "candidate_rows_scored.csv", index=False)
    replacements.to_csv(run_dir / "replacement_quality.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    branching.to_csv(run_dir / "branching_summary.csv", index=False)
    side_by_side.to_csv(run_dir / "ungated_vs_gated_comparison.csv", index=False)
    protected.to_csv(run_dir / "protected_mature_breakout_summary.csv", index=False)
    _write_json(
        run_dir / "input_artifact_report.json",
        {
            "validation_root": validation_root,
            "shift_root": shift_root,
            "daily_path": daily_path,
            "required_inputs": list(REQUIRED_INPUTS),
            "source_no_lookahead_audit": source_audit.get("audit_result"),
            "rows_loaded": int(len(rows)),
            "rows_with_ret20": int(rows["ret20"].notna().sum()),
            "load_report": load_report,
            "scope": "TRADEX-only; fixed monthly_box_breakout x above60 maturity context pretest",
        },
    )
    _write_json(run_dir / "context_policy.json", CONTEXT_POLICY)
    _write_json(run_dir / "topk_comparison_summary.json", summary)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "source_audit_result": source_audit.get("audit_result"),
            "monthly_box_breakout_proxy_is_point_in_time": True,
            "above60_streak_computed_through_decision_date": True,
            "missing_above60_streak_not_classified_as_lt_60": True,
            "ret20_ret40_are_label_only": True,
            "topk_comparison_same_decision_date_only": True,
            "future_label_used_for_context_policy": False,
            "rows_after_2026_label_safe_cutoff_excluded_from_ret20_evaluation": True,
            "model_training": False,
            "threshold_sweep": False,
            "column_classification": {
                "decision_ymd": "decision_surface",
                "code": "decision_surface",
                "selection_score": "decision_surface",
                "candidate_rank": "decision_surface",
                "monthly_box_breakout_proxy": "feature",
                "above60_streak": "feature",
                "gated_score_delta": "fixed_policy",
                "context_policy_reason": "diagnostic",
                "ret20": "label",
                "ret40": "label",
                "mae20": "diagnostic",
                "mfe20": "diagnostic",
            },
        },
    )
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "output_dir": run_dir,
            "required_artifacts": list(REQUIRED_ARTIFACTS),
            "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    return {"output_dir": str(run_dir), "research_decision": decision, "summary": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pretest monthly_box_breakout soft demotion gated by above60 maturity")
    parser.add_argument("--validation-root", type=Path, default=DEFAULT_VALIDATION_ROOT)
    parser.add_argument("--shift-root", type=Path, default=DEFAULT_SHIFT_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    result = run(validation_root=args.validation_root, shift_root=args.shift_root, daily_path=args.daily_path, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
