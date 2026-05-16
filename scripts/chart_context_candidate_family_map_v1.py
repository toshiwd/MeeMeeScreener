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
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


AXIS_ID = "chart_context_candidate_family_map_v1"
SCHEMA_PREFIX = "tradex_chart_context_candidate_family_map_v1"
DEFAULT_OUTPUT_DIR_NAME = "chart_context_candidate_family_map_v1"

FAMILY_PRIORITY_ORDER = (
    "true_breakdown_candidate",
    "shakeout_recovery_candidate",
    "gap_up_failure",
    "gap_down_breakdown",
    "gap_down_shakeout",
    "failed_breakout",
    "resistance_breakout",
    "box_breakdown",
    "box_breakout",
    "bearish_full_retrace_warning",
    "bullish_full_retrace_recovery",
    "ma_breakdown_warning",
    "ma_reclaim_pullback",
    "ma_extended_late_entry",
    "sideways_compression",
    "n_wave_continuation",
    "reverse_n_warning",
    "low_volume_score_spike",
    "unknown_or_mixed",
)

REQUIRED_ARTIFACTS = (
    "chart_context_candidate_family_summary.json",
    "chart_context_candidate_family_map.csv",
    "chart_context_candidate_family_contract.json",
    "family_priority_order.json",
    "family_yearly_performance.csv",
    "family_regime_performance.csv",
    "family_bought_vs_rejected.csv",
    "family_failure_cases.csv",
    "family_keep_drop_hold.csv",
    "family_coverage_report.json",
    "no_lookahead_audit.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "family_veto_pretest",
    "family_weighting_pretest",
    "regime_aware_family_filter_pretest",
    "chart_context_candidate_generation_redesign_v1",
    "abandon_current_candidate_surface",
)

FORBIDDEN_FOR_CLASSIFICATION = (
    "post_ret_5",
    "post_ret_10",
    "post_ret_20",
    "post_ret_40",
    "mae_20",
    "mfe_20",
    "outcome_bucket",
    "missed_winner",
    "avoided_bad",
    "future_return",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _as_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _flag(row: pd.Series, name: str) -> bool:
    value = row.get(name)
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).lower() in {"true", "1", "yes"}


def _num(row: pd.Series, name: str, default: float = 0.0) -> float:
    try:
        value = float(row.get(name))
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) else default


def classify_chart_context_family(row: pd.Series) -> tuple[str, str]:
    if _flag(row, "true_breakdown_candidate_flag"):
        return "true_breakdown_candidate", "past_invalidation_followed_by_breakdown_context"
    if _flag(row, "shakeout_recovery_candidate_flag"):
        return "shakeout_recovery_candidate", "past_invalidation_followed_by_recovery_context"
    if _flag(row, "gap_up_flag") and (_flag(row, "gap_fail_same_day_flag") or _flag(row, "denial_of_prior_bull_flag")):
        return "gap_up_failure", "gap_up_failed_by_same_day_close_or_denial"
    if _flag(row, "gap_down_flag") and (_flag(row, "true_breakdown_candidate_flag") or _num(row, "close_below_ma20_count") >= 2):
        return "gap_down_breakdown", "gap_down_with_ma20_breakdown_context"
    if _flag(row, "gap_down_flag") and (_flag(row, "regained_ma7_after_invalidation_flag") or _flag(row, "regained_ma20_after_invalidation_flag")):
        return "gap_down_shakeout", "gap_down_with_observed_reclaim_context"
    if _flag(row, "failed_breakout_flag") or _flag(row, "prior_swing_failure_flag"):
        return "failed_breakout", "resistance_or_prior_swing_breakout_failed"
    if _flag(row, "breakout_above_resistance_flag") or _flag(row, "prior_swing_reclaim_flag"):
        return "resistance_breakout", "close_above_recent_resistance_or_prior_swing"
    if _flag(row, "box_breakdown_flag"):
        return "box_breakdown", "box_lower_breakdown"
    if _flag(row, "box_breakout_flag"):
        return "box_breakout", "box_upper_breakout"
    if _flag(row, "bearish_full_retrace_flag") or _flag(row, "engulfing_bearish_flag") or _flag(row, "denial_of_prior_bull_flag"):
        return "bearish_full_retrace_warning", "bearish_retrace_or_engulfing_warning"
    if _flag(row, "bullish_full_retrace_flag") or _flag(row, "engulfing_bullish_flag") or _flag(row, "denial_of_prior_bear_flag"):
        return "bullish_full_retrace_recovery", "bullish_retrace_or_engulfing_recovery"
    if _num(row, "days_since_ma20_break", 9999.0) <= 3 or _num(row, "close_below_ma20_count") >= 3:
        return "ma_breakdown_warning", "recent_or_continuing_ma20_break"
    if _num(row, "days_since_ma20_reclaim", 9999.0) <= 5 and _num(row, "close_above_ma20_count") <= 8:
        return "ma_reclaim_pullback", "recent_ma20_reclaim_early_lifecycle"
    if _num(row, "close_above_ma20_count") >= 40 or (_num(row, "close_above_ma7_count") >= 20 and _num(row, "ma7_ma20_distance_pct") >= 0.05):
        return "ma_extended_late_entry", "extended_days_above_ma_context"
    if _num(row, "sideways_length_days") >= 15 or _flag(row, "ma_compression_flag"):
        return "sideways_compression", "sideways_or_ma_compression_context"
    if _flag(row, "n_wave_candidate_flag") or _flag(row, "higher_low_confirmed_flag"):
        return "n_wave_continuation", "higher_low_or_n_wave_continuation_context"
    if _flag(row, "reverse_n_candidate_flag") or _flag(row, "lower_high_confirmed_flag"):
        return "reverse_n_warning", "lower_high_or_reverse_n_warning_context"
    if _num(row, "selection_score") >= 15 and _num(row, "volume_compression_ratio", 1.0) <= 0.85:
        return "low_volume_score_spike", "high_score_with_volume_compression"
    return "unknown_or_mixed", "no_priority_family_matched"


def _load_candidate_outcomes(robustness_root: Path) -> pd.DataFrame:
    yearly = pd.read_csv(robustness_root / "yearly_results.csv")
    frames: list[pd.DataFrame] = []
    for _idx, row in yearly.iterrows():
        year = int(row["year"])
        run_dir = Path(str(row["run_dir"]))
        candidates = pd.read_csv(run_dir / "daily_candidate_snapshot.csv")
        outcomes = pd.read_csv(run_dir / "post_run_outcome_labels.csv")
        candidates["year"] = year
        outcomes["year"] = year
        candidates["code"] = candidates["code"].astype(str)
        outcomes["code"] = outcomes["code"].astype(str)
        candidates["decision_ymd"] = candidates["decision_ymd"].astype(int)
        outcomes["decision_ymd"] = outcomes["decision_ymd"].astype(int)
        merged = candidates.merge(outcomes, on=["year", "decision_ymd", "code"], how="left", suffixes=("", "_outcome"))
        frames.append(merged)
    if not frames:
        raise RuntimeError("no candidate snapshots found")
    out = pd.concat(frames, ignore_index=True)
    out["selected_for_buy_bool"] = _as_bool_series(out.get("selected_for_buy", pd.Series(dtype=bool)))
    out["month"] = out["decision_ymd"].astype(int) // 100
    return out


def _load_regimes(robustness_root: Path) -> pd.DataFrame:
    path = robustness_root / "baseline_regime_failure_decomposition_v1" / "monthly_failure_decomposition.csv"
    if not path.exists():
        return pd.DataFrame(columns=["year", "month", "regime_bucket", "benchmark_return"])
    frame = pd.read_csv(path)
    frame["year"] = frame["year"].astype(int)
    frame["month"] = frame["month"].astype(int)
    return frame[["year", "month", "regime_bucket", "benchmark_return", "portfolio_return", "excess_return"]].copy()


def _build_family_map(robustness_root: Path, chart_root: Path) -> pd.DataFrame:
    chart = pd.read_parquet(chart_root / "chart_context_features_daily.parquet")
    chart["code"] = chart["code"].astype(str)
    chart["decision_ymd"] = chart["decision_ymd"].astype(int)
    outcomes = _load_candidate_outcomes(robustness_root)
    classification = chart.apply(classify_chart_context_family, axis=1, result_type="expand")
    chart["chart_context_family"] = classification[0]
    chart["chart_context_family_reason"] = classification[1]
    join_columns = [
        "year",
        "decision_ymd",
        "code",
        "candidate_rank",
        "selected_for_buy_bool",
        "post_ret_20",
        "mae_20",
        "mfe_20",
        "month",
    ]
    joined = chart.merge(outcomes[[column for column in join_columns if column in outcomes.columns]], on=["year", "decision_ymd", "code"], how="left", suffixes=("", "_snapshot"))
    if "candidate_rank_snapshot" in joined.columns:
        joined["candidate_rank"] = joined["candidate_rank"].fillna(joined["candidate_rank_snapshot"])
    if "selected_for_buy_bool" not in joined.columns:
        joined["selected_for_buy_bool"] = False
    return joined


def _yearly_performance(mapped: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    if not regimes.empty:
        mapped = mapped.merge(regimes[["year", "month", "benchmark_return"]], on=["year", "month"], how="left")
    rows: list[dict[str, Any]] = []
    for (family, year), group in mapped.groupby(["chart_context_family", "year"], sort=True):
        bought = group[group["selected_for_buy_bool"].fillna(False).astype(bool)]
        rejected = group[~group["selected_for_buy_bool"].fillna(False).astype(bool)]
        post = pd.to_numeric(group.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rejected_post = pd.to_numeric(rejected.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        mae = pd.to_numeric(group.get("mae_20", pd.Series(dtype=float)), errors="coerce")
        mfe = pd.to_numeric(group.get("mfe_20", pd.Series(dtype=float)), errors="coerce")
        benchmark_return = pd.to_numeric(group.get("benchmark_return", pd.Series(dtype=float)), errors="coerce").dropna()
        rows.append(
            {
                "chart_context_family": family,
                "year": int(year),
                "candidate_count": int(len(group)),
                "bought_count": int(len(bought)),
                "rejected_count": int(len(rejected)),
                "bought_rate": float(len(bought) / len(group)) if len(group) else 0.0,
                "post_ret20_mean": float(post.mean()) if not post.dropna().empty else None,
                "post_ret20_median": float(post.median()) if not post.dropna().empty else None,
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "rejected_post_ret20_mean": float(rejected_post.mean()) if not rejected_post.dropna().empty else None,
                "severe_loser_rate": float(((post <= -0.10) | (mae <= -0.10)).mean()) if len(group) else None,
                "big_winner_rate": float(((post >= 0.10) | (mfe >= 0.15)).mean()) if len(group) else None,
                "benchmark_up_performance": float(bought_post.mean()) if not bought_post.dropna().empty and (benchmark_return.mean() if len(benchmark_return) else 0) >= 0 else None,
                "benchmark_down_performance": float(bought_post.mean()) if not bought_post.dropna().empty and (benchmark_return.mean() if len(benchmark_return) else 0) < 0 else None,
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    best = result.dropna(subset=["bought_post_ret20_mean"]).sort_values("bought_post_ret20_mean").groupby("chart_context_family").tail(1)[["chart_context_family", "year"]].rename(columns={"year": "best_year"})
    worst = result.dropna(subset=["bought_post_ret20_mean"]).sort_values("bought_post_ret20_mean").groupby("chart_context_family").head(1)[["chart_context_family", "year"]].rename(columns={"year": "worst_year"})
    return result.merge(best, on="chart_context_family", how="left").merge(worst, on="chart_context_family", how="left")


def _regime_performance(mapped: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    work = mapped.merge(regimes[["year", "month", "regime_bucket", "benchmark_return"]], on=["year", "month"], how="left") if not regimes.empty else mapped.copy()
    work["regime_bucket"] = work.get("regime_bucket", pd.Series(index=work.index, dtype="object")).fillna("unknown")
    rows: list[dict[str, Any]] = []
    for (family, regime), group in work.groupby(["chart_context_family", "regime_bucket"], sort=True):
        bought = group[group["selected_for_buy_bool"].fillna(False).astype(bool)]
        post = pd.to_numeric(group.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        mae = pd.to_numeric(group.get("mae_20", pd.Series(dtype=float)), errors="coerce")
        rows.append(
            {
                "chart_context_family": family,
                "regime_bucket": regime,
                "candidate_count": int(len(group)),
                "bought_count": int(len(bought)),
                "post_ret20_mean": float(post.mean()) if not post.dropna().empty else None,
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "severe_loser_rate": float(((post <= -0.10) | (mae <= -0.10)).mean()) if len(group) else None,
                "big_winner_rate": float((post >= 0.10).mean()) if len(post.dropna()) else None,
            }
        )
    return pd.DataFrame(rows)


def _bought_vs_rejected(mapped: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, group in mapped.groupby("chart_context_family", sort=True):
        bought = group[group["selected_for_buy_bool"].fillna(False).astype(bool)]
        rejected = group[~group["selected_for_buy_bool"].fillna(False).astype(bool)]
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rejected_post = pd.to_numeric(rejected.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rows.append(
            {
                "chart_context_family": family,
                "bought_count": int(len(bought)),
                "rejected_count": int(len(rejected)),
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "rejected_post_ret20_mean": float(rejected_post.mean()) if not rejected_post.dropna().empty else None,
                "bought_vs_rejected_gap": float(bought_post.mean() - rejected_post.mean()) if not bought_post.dropna().empty and not rejected_post.dropna().empty else None,
            }
        )
    return pd.DataFrame(rows)


def _failure_cases(mapped: pd.DataFrame) -> pd.DataFrame:
    selected = mapped[mapped["selected_for_buy_bool"].fillna(False).astype(bool)].copy()
    post = pd.to_numeric(selected.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
    mae = pd.to_numeric(selected.get("mae_20", pd.Series(dtype=float)), errors="coerce")
    failures = selected[(post <= -0.08) | (mae <= -0.10)].copy()
    keep_cols = [
        "year",
        "decision_ymd",
        "code",
        "chart_context_family",
        "chart_context_family_reason",
        "candidate_rank",
        "selection_score",
        "post_ret_20",
        "mae_20",
        "mfe_20",
        "candidate_rank_snapshot",
    ]
    return failures[[column for column in keep_cols if column in failures.columns]].sort_values(["year", "chart_context_family", "post_ret_20"], kind="stable")


def _keep_drop_hold(yearly: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, group in yearly.groupby("chart_context_family", sort=True):
        bought_years = group[group["bought_count"] >= 3].copy()
        bought_sample = int(group["bought_count"].sum())
        positive_years = int((pd.to_numeric(bought_years["bought_post_ret20_mean"], errors="coerce") > 0).sum())
        negative_years = int((pd.to_numeric(bought_years["bought_post_ret20_mean"], errors="coerce") < 0).sum())
        bought_mean = pd.to_numeric(group["bought_post_ret20_mean"], errors="coerce")
        bought_count = pd.to_numeric(group["bought_count"], errors="coerce").fillna(0.0)
        valid_ret = bought_mean.notna() & (bought_count > 0)
        mean_ret = float((bought_mean[valid_ret] * bought_count[valid_ret]).sum() / bought_count[valid_ret].sum()) if bool(valid_ret.any()) else 0.0
        unweighted_mean_ret = float(bought_mean.mean()) if not bought_mean.dropna().empty else None
        severe = pd.to_numeric(group["severe_loser_rate"], errors="coerce")
        candidate_count = pd.to_numeric(group["candidate_count"], errors="coerce").fillna(0.0)
        valid_severe = severe.notna() & (candidate_count > 0)
        severe_mean = float((severe[valid_severe] * candidate_count[valid_severe]).sum() / candidate_count[valid_severe].sum()) if bool(valid_severe.any()) else 0.0
        year_count = int(group["year"].nunique())
        failure_years = group[group["year"].isin([2019, 2021, 2022])]
        upside_years = group[group["year"].isin([2024, 2025])]
        failure_contribution = float(pd.to_numeric(failure_years["bought_post_ret20_mean"], errors="coerce").mean()) if not failure_years.empty else None
        upside_contribution = float(pd.to_numeric(upside_years["bought_post_ret20_mean"], errors="coerce").mean()) if not upside_years.empty else None
        if bought_sample < 20:
            decision = "unknown_insufficient_sample"
            reason = "bought_sample_lt_20"
        elif positive_years >= 5 and mean_ret > 0.01 and severe_mean <= 0.25:
            decision = "keep_candidate_family"
            reason = "positive_most_years_low_severe_loser_rate"
        elif negative_years >= 4 or mean_ret < -0.015 or severe_mean >= 0.35:
            decision = "drop_unstable_or_harmful"
            reason = "negative_or_severe_loser_profile"
        else:
            decision = "hold_regime_dependent"
            reason = "mixed_years_or_regime_dependent"
        rows.append(
            {
                "chart_context_family": family,
                "family_decision": decision,
                "reason_type": reason,
                "bought_sample": bought_sample,
                "year_count_present": year_count,
                "positive_bought_years": positive_years,
                "negative_bought_years": negative_years,
                "bought_post_ret20_mean": mean_ret,
                "bought_post_ret20_unweighted_year_mean": unweighted_mean_ret,
                "severe_loser_rate_mean": severe_mean,
                "2019_2021_2022_failure_contribution": failure_contribution,
                "2024_2025_upside_contribution": upside_contribution,
            }
        )
    return pd.DataFrame(rows)


def _coverage(mapped: pd.DataFrame) -> dict[str, Any]:
    family_counts = mapped["chart_context_family"].value_counts().to_dict()
    bought_counts = mapped[mapped["selected_for_buy_bool"].fillna(False).astype(bool)]["chart_context_family"].value_counts().to_dict()
    unknown_count = int(family_counts.get("unknown_or_mixed", 0))
    return {
        "schema_version": f"{SCHEMA_PREFIX}_coverage_report_v1",
        "axis_id": AXIS_ID,
        "row_count": int(len(mapped)),
        "family_count": int(mapped["chart_context_family"].nunique()),
        "family_counts": {str(k): int(v) for k, v in family_counts.items()},
        "bought_family_counts": {str(k): int(v) for k, v in bought_counts.items()},
        "unknown_or_mixed_count": unknown_count,
        "unknown_or_mixed_rate": float(unknown_count / len(mapped)) if len(mapped) else 0.0,
    }


def _choose_next_axis(kdh: pd.DataFrame, regime: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    counts = kdh["family_decision"].value_counts().to_dict() if not kdh.empty else {}
    keep = kdh[kdh["family_decision"] == "keep_candidate_family"]
    harmful = kdh[kdh["family_decision"] == "drop_unstable_or_harmful"]
    hold = kdh[kdh["family_decision"] == "hold_regime_dependent"]
    high_spread = 0
    if not regime.empty:
        spread = regime.groupby("chart_context_family")["bought_post_ret20_mean"].agg(lambda s: float(pd.to_numeric(s, errors="coerce").max() - pd.to_numeric(s, errors="coerce").min()) if len(pd.to_numeric(s, errors="coerce").dropna()) >= 2 else 0.0)
        high_spread = int((spread >= 0.08).sum())
    evidence = {
        "family_decision_counts": counts,
        "keep_family_count": int(len(keep)),
        "harmful_family_count": int(len(harmful)),
        "hold_family_count": int(len(hold)),
        "high_regime_spread_family_count": high_spread,
        "keep_families": keep["chart_context_family"].tolist(),
        "harmful_families": harmful["chart_context_family"].tolist(),
        "hold_families": hold["chart_context_family"].tolist(),
    }
    if len(keep) >= 1 and len(harmful) >= 1:
        return "family_veto_pretest", "has_keep_and_harmful_chart_context_families", evidence
    if len(harmful) >= 1:
        return "family_veto_pretest", "harmful_chart_context_family_detected", evidence
    if len(keep) >= 1:
        return "chart_context_candidate_generation_redesign_v1", "stable_chart_context_keep_family_exists", evidence
    if high_spread >= 2 or len(hold) >= 3:
        return "regime_aware_family_filter_pretest", "chart_context_family_effects_are_regime_dependent", evidence
    return "abandon_current_candidate_surface", "no_actionable_chart_context_family_separation", evidence


def _contract(chart_root: Path) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_id": AXIS_ID,
        "chart_context_root": str(chart_root),
        "family_priority_order": list(FAMILY_PRIORITY_ORDER),
        "classification_source": "chart_context_features_daily.parquet",
        "classification_uses_post_run_outcomes": False,
        "classification_uses_future_bars": False,
        "unknown_or_mixed_allowed": True,
        "scope": {"tradex_only": True, "replay_rerun": False, "policy_change": False, "candidate_generation_change": False, "ranking_change": False, "optimization": False, "threshold_sweep": False, "meemee_ui_changed": False, "runtime_db_written": False, "publish_registry_changed": False},
    }


def _no_lookahead_audit(mapped: pd.DataFrame, chart_root: Path) -> dict[str, Any]:
    chart_audit_path = chart_root / "no_lookahead_audit.json"
    chart_audit = _read_json(chart_audit_path) if chart_audit_path.exists() else {"audit_result": "missing"}
    forbidden_present = [column for column in FORBIDDEN_FOR_CLASSIFICATION if column in mapped.columns and column in {"chart_context_family", "chart_context_family_reason"}]
    audit_result = "pass" if chart_audit.get("audit_result") == "pass" and not forbidden_present else "fail"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "audit_result": audit_result,
        "chart_context_no_lookahead_audit": chart_audit.get("audit_result"),
        "classification_uses_only_chart_context_features": True,
        "post_run_outcomes_used_for_family_classification": False,
        "post_run_outcomes_used_for_performance_labels": True,
        "forbidden_columns_used_for_classification": forbidden_present,
        "chart_context_audit_artifact": str(chart_audit_path),
    }


def run_chart_context_candidate_family_map(
    robustness_root: str | Path,
    chart_context_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    chart_root = Path(chart_context_root) if chart_context_root else robustness_root / "chart_context_feature_contract_v1"
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    mapped = _build_family_map(robustness_root, chart_root)
    regimes = _load_regimes(robustness_root)
    yearly = _yearly_performance(mapped, regimes)
    regime = _regime_performance(mapped, regimes)
    bought_vs_rejected = _bought_vs_rejected(mapped)
    failures = _failure_cases(mapped)
    kdh = _keep_drop_hold(yearly)
    coverage = _coverage(mapped)
    decision, reason, evidence = _choose_next_axis(kdh, regime)
    audit = _no_lookahead_audit(mapped, chart_root)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "chart_context_candidate_family_map.csv", mapped)
    _write_csv(output_root / "family_yearly_performance.csv", yearly)
    _write_csv(output_root / "family_regime_performance.csv", regime)
    _write_csv(output_root / "family_bought_vs_rejected.csv", bought_vs_rejected)
    _write_csv(output_root / "family_failure_cases.csv", failures)
    _write_csv(output_root / "family_keep_drop_hold.csv", kdh)
    _write_json(output_root / "chart_context_candidate_family_contract.json", _contract(chart_root))
    _write_json(output_root / "family_priority_order.json", {"schema_version": f"{SCHEMA_PREFIX}_priority_order_v1", "axis_id": AXIS_ID, "family_priority_order": list(FAMILY_PRIORITY_ORDER), "first_match_wins": True})
    _write_json(output_root / "family_coverage_report.json", coverage)
    _write_json(output_root / "no_lookahead_audit.json", audit)
    _write_json(
        output_root / "chart_context_candidate_family_summary.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "robustness_root": str(robustness_root),
            "chart_context_root": str(chart_root),
            "decision": decision,
            "reason_type": reason,
            "metrics": evidence,
            "coverage": {"row_count": coverage["row_count"], "family_count": coverage["family_count"], "unknown_or_mixed_rate": coverage["unknown_or_mixed_rate"]},
            "scope": {"tradex_only": True, "replay_rerun": False, "policy_change": False, "candidate_generation_change": False, "ranking_change": False, "optimization": False, "threshold_sweep": False, "meemee_ui_changed": False, "runtime_db_written": False, "publish_registry_changed": False},
        },
    )
    _write_json(
        output_root / "next_axis_decision.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "decision_candidates": list(DECISIONS),
            "decision": decision,
            "decision_count": 1,
            "reason_type": reason,
            "metrics": evidence,
            "policy_promotion_allowed": False,
            "meemee_reflectable": False,
        },
    )
    _write_json(
        output_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "complete": True,
            "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
            "no_lookahead_audit": audit["audit_result"],
            "decision": decision,
            "decision_count": 1,
            "replay_rerun": False,
            "policy_change": False,
            "candidate_generation_change": False,
            "ranking_change": False,
            "optimization": False,
            "threshold_sweep": False,
            "silent_fallback_used": False,
            "policy_promotion_allowed": False,
            "meemee_reflectable": False,
        },
    )
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence, "no_lookahead_audit": audit["audit_result"]}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map chart context candidate family effectiveness.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--chart-context-root", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_chart_context_candidate_family_map(args.robustness_root, args.chart_context_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
