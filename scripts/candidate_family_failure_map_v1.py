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

from scripts import candidate_selection_quality_decomposition_v1 as quality


AXIS_ID = "candidate_family_failure_map_v1"
SCHEMA_PREFIX = "tradex_candidate_family_failure_map_v1"
DEFAULT_OUTPUT_DIR_NAME = "candidate_family_failure_map_v1"

FAMILIES = (
    "breakout_continuation",
    "pullback_reclaim",
    "volume_expansion",
    "low_volume_score_spike",
    "monthly_uptrend_pullback",
    "weak_reversal",
    "range_break",
    "one_day_score_spike",
    "trend_follow",
    "unknown_or_mixed",
)

REQUIRED_ARTIFACTS = (
    "candidate_family_failure_summary.json",
    "candidate_family_map.csv",
    "candidate_family_yearly_performance.csv",
    "candidate_family_regime_performance.csv",
    "candidate_family_bought_vs_rejected.csv",
    "candidate_family_failure_cases.csv",
    "candidate_family_keep_drop_hold.csv",
    "feature_family_coverage.csv",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "candidate_generation_redesign_v1",
    "family_veto_pretest",
    "family_weighting_pretest",
    "regime_aware_family_filter_pretest",
    "abandon_current_candidate_surface",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
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


def _family_from_components(row: pd.Series) -> tuple[str, str]:
    components = quality._parse_components(row.get("score_components_json"))
    daily_ret = components.get("daily_ret20_state", "")
    candle = components.get("daily_candle_state", "")
    volume = components.get("daily_volume_state", "")
    sequence = components.get("daily_sequence_state", "")
    ma_stack = components.get("daily_ma_stack", "")
    ma60 = components.get("daily_ma60_slope_state", "")
    weekly = components.get("weekly_trend_state", "")
    weekly_ret = components.get("weekly_ret4_state", "")
    monthly = components.get("monthly_trend_state", "")
    monthly_ret = components.get("monthly_ret6_state", "")
    score = float(row.get("selection_score") or 0)
    reasons: list[str] = []

    if "bear" in candle or "mixed" in sequence:
        reasons.append("bearish_or_mixed_daily_signal")
        if volume == "daily_volume_expansion":
            return "weak_reversal", "|".join(reasons + ["volume_expansion_on_weak_candle"])
    if daily_ret == "daily20_strong_up" and candle == "daily_strong_bull" and volume == "daily_volume_expansion":
        return "breakout_continuation", "strong_daily_ret_bull_candle_volume_expansion"
    if volume == "daily_volume_expansion" and weekly == "weekly_uptrend":
        return "volume_expansion", "volume_expansion_in_weekly_uptrend"
    if daily_ret in {"daily20_up", "daily20_strong_up"} and ma_stack == "daily_bull_stack_5_20_60" and ma60 == "daily_ma60_rising" and weekly == "weekly_uptrend":
        if volume == "daily_volume_normal" and score >= 15:
            return "low_volume_score_spike", "high_score_without_volume_expansion"
        return "trend_follow", "bull_stack_rising_ma_weekly_uptrend"
    if monthly in {"monthly_uptrend", "monthly_recovery"} and candle in {"daily_lower_wick_bull", "daily_strong_bull"} and daily_ret not in {"daily20_strong_up"}:
        return "monthly_uptrend_pullback", "monthly_uptrend_daily_reclaim"
    if candle == "daily_lower_wick_bull" or monthly == "monthly_recovery":
        return "pullback_reclaim", "lower_wick_or_monthly_recovery"
    if weekly_ret == "weekly4_strong_up" and daily_ret == "daily20_strong_up":
        return "range_break", "weekly_and_daily_strong_up"
    if score >= 16 and volume == "daily_volume_normal":
        return "one_day_score_spike", "high_score_normal_volume"
    return "unknown_or_mixed", "no_single_family_rule_matched"


def _month_key(ymd: Any) -> int:
    return int(float(ymd)) // 100


def _load_candidates(robustness_root: Path) -> pd.DataFrame:
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
        joined = candidates.merge(outcomes, on=["year", "decision_ymd", "code"], how="left")
        joined["selected_for_buy_bool"] = _as_bool_series(joined.get("selected_for_buy", pd.Series(dtype=bool)))
        joined["month"] = joined["decision_ymd"].map(_month_key)
        family = joined.apply(_family_from_components, axis=1, result_type="expand")
        joined["candidate_family"] = family[0]
        joined["candidate_family_reason"] = family[1]
        components = joined["score_components_json"].map(quality._parse_components)
        for feature in sorted({key for item in components for key in item.keys()}):
            joined[feature] = components.map(lambda item, name=feature: item.get(name, ""))
        frames.append(joined)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_month_regimes(robustness_root: Path) -> pd.DataFrame:
    path = robustness_root / "baseline_regime_failure_decomposition_v1" / "monthly_failure_decomposition.csv"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    frame["month"] = frame["month"].astype(int)
    return frame[["year", "month", "regime_bucket", "portfolio_return", "benchmark_return", "excess_return"]].copy()


def _family_yearly_performance(mapped: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (family, year), group in mapped.groupby(["candidate_family", "year"], sort=True):
        bought = group[group["selected_for_buy_bool"]].copy()
        rejected = group[~group["selected_for_buy_bool"]].copy()
        post = pd.to_numeric(group.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rejected_post = pd.to_numeric(rejected.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        severe = pd.to_numeric(group.get("mae_20", pd.Series(dtype=float)), errors="coerce") <= -0.10
        rows.append(
            {
                "candidate_family": family,
                "year": int(year),
                "candidate_count": int(len(group)),
                "bought_count": int(len(bought)),
                "post_ret20_mean": float(post.mean()) if not post.dropna().empty else None,
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "rejected_post_ret20_mean": float(rejected_post.mean()) if not rejected_post.dropna().empty else None,
                "win_rate": float((post > 0).mean()) if len(post.dropna()) else None,
                "severe_loser_rate": float(severe.mean()) if len(group) else None,
                "benchmark_excess_proxy": float(bought_post.mean() - rejected_post.mean()) if not bought_post.dropna().empty and not rejected_post.dropna().empty else None,
            }
        )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["best_year"] = result.groupby("candidate_family")["bought_post_ret20_mean"].transform(lambda s: result.loc[s.astype(float).idxmax(), "year"] if not s.dropna().empty else None)
    result["worst_year"] = result.groupby("candidate_family")["bought_post_ret20_mean"].transform(lambda s: result.loc[s.astype(float).idxmin(), "year"] if not s.dropna().empty else None)
    return result


def _family_regime_performance(mapped: pd.DataFrame, regimes: pd.DataFrame) -> pd.DataFrame:
    if regimes.empty:
        mapped = mapped.copy()
        mapped["regime_bucket"] = "unknown"
    else:
        mapped = mapped.merge(regimes, on=["year", "month"], how="left", suffixes=("", "_month"))
        mapped["regime_bucket"] = mapped["regime_bucket"].fillna("unknown")
    rows: list[dict[str, Any]] = []
    for (family, regime), group in mapped.groupby(["candidate_family", "regime_bucket"], sort=True):
        bought = group[group["selected_for_buy_bool"]]
        post = pd.to_numeric(group.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rows.append(
            {
                "candidate_family": family,
                "regime_bucket": regime,
                "candidate_count": int(len(group)),
                "bought_count": int(len(bought)),
                "post_ret20_mean": float(post.mean()) if not post.dropna().empty else None,
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "win_rate": float((post > 0).mean()) if len(post.dropna()) else None,
                "severe_loser_rate": float((pd.to_numeric(group.get("mae_20", pd.Series(dtype=float)), errors="coerce") <= -0.10).mean()) if len(group) else None,
            }
        )
    return pd.DataFrame(rows)


def _bought_vs_rejected(mapped: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, group in mapped.groupby("candidate_family", sort=True):
        bought = group[group["selected_for_buy_bool"]]
        rejected = group[~group["selected_for_buy_bool"]]
        bought_post = pd.to_numeric(bought.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rejected_post = pd.to_numeric(rejected.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
        rows.append(
            {
                "candidate_family": family,
                "bought_count": int(len(bought)),
                "rejected_count": int(len(rejected)),
                "bought_post_ret20_mean": float(bought_post.mean()) if not bought_post.dropna().empty else None,
                "rejected_post_ret20_mean": float(rejected_post.mean()) if not rejected_post.dropna().empty else None,
                "bought_vs_rejected_gap": float(bought_post.mean() - rejected_post.mean()) if not bought_post.dropna().empty and not rejected_post.dropna().empty else None,
            }
        )
    return pd.DataFrame(rows)


def _keep_drop_hold(yearly: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, group in yearly.groupby("candidate_family", sort=True):
        bought_years = group[group["bought_count"] >= 3].copy()
        sample = int(group["bought_count"].sum())
        positive_years = int((pd.to_numeric(bought_years["bought_post_ret20_mean"], errors="coerce") > 0).sum())
        negative_years = int((pd.to_numeric(bought_years["bought_post_ret20_mean"], errors="coerce") < 0).sum())
        severe_mean = float(pd.to_numeric(group["severe_loser_rate"], errors="coerce").mean()) if not group.empty else 0.0
        mean_ret = float(pd.to_numeric(group["bought_post_ret20_mean"], errors="coerce").mean()) if not group.empty else 0.0
        if sample < 20:
            decision = "unknown_insufficient_sample"
            reason = "bought_sample_lt_20"
        elif positive_years >= 5 and mean_ret > 0 and severe_mean < 0.25:
            decision = "keep_candidate_family"
            reason = "positive_most_years_low_severe_loser_rate"
        elif negative_years >= 4 or mean_ret < -0.02 or severe_mean >= 0.35:
            decision = "drop_unstable_or_harmful"
            reason = "negative_or_severe_loser_profile"
        else:
            decision = "hold_regime_dependent"
            reason = "mixed_years_or_regime_dependent"
        rows.append(
            {
                "candidate_family": family,
                "family_decision": decision,
                "reason_type": reason,
                "bought_sample": sample,
                "positive_bought_years": positive_years,
                "negative_bought_years": negative_years,
                "bought_post_ret20_mean": mean_ret,
                "severe_loser_rate_mean": severe_mean,
            }
        )
    return pd.DataFrame(rows)


def _failure_cases(mapped: pd.DataFrame) -> pd.DataFrame:
    selected = mapped[mapped["selected_for_buy_bool"]].copy()
    post = pd.to_numeric(selected.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
    mae = pd.to_numeric(selected.get("mae_20", pd.Series(dtype=float)), errors="coerce")
    failures = selected[(post <= -0.05) | (mae <= -0.10)].copy()
    return failures.sort_values(["year", "candidate_family", "post_ret_20"], kind="stable")


def _coverage(mapped: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, group in mapped.groupby("candidate_family", sort=True):
        rows.append(
            {
                "candidate_family": family,
                "candidate_count": int(len(group)),
                "bought_count": int(group["selected_for_buy_bool"].sum()),
                "candidate_share": float(len(group) / len(mapped)) if len(mapped) else 0.0,
                "bought_share": float(group["selected_for_buy_bool"].sum() / mapped["selected_for_buy_bool"].sum()) if mapped["selected_for_buy_bool"].sum() else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _choose_next_axis(kdh: pd.DataFrame, yearly: pd.DataFrame, regime: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    counts = kdh["family_decision"].value_counts().to_dict() if not kdh.empty else {}
    harmful = kdh[kdh["family_decision"] == "drop_unstable_or_harmful"]
    keep = kdh[kdh["family_decision"] == "keep_candidate_family"]
    hold = kdh[kdh["family_decision"] == "hold_regime_dependent"]
    regime_spread = 0
    if not regime.empty and "bought_post_ret20_mean" in regime.columns:
        spreads = regime.groupby("candidate_family")["bought_post_ret20_mean"].agg(lambda s: float(pd.to_numeric(s, errors="coerce").max() - pd.to_numeric(s, errors="coerce").min()) if len(pd.to_numeric(s, errors="coerce").dropna()) >= 2 else 0.0)
        regime_spread = int((spreads >= 0.08).sum())
    evidence = {
        "family_decision_counts": counts,
        "keep_family_count": int(len(keep)),
        "harmful_family_count": int(len(harmful)),
        "regime_dependent_family_count": int(len(hold)),
        "high_regime_spread_family_count": regime_spread,
        "keep_families": keep["candidate_family"].tolist(),
        "harmful_families": harmful["candidate_family"].tolist(),
    }
    if len(keep) >= 1 and len(harmful) >= 1:
        return "family_veto_pretest", "has_keep_families_and_harmful_families_for_single_axis_veto", evidence
    if len(keep) >= 1:
        return "candidate_generation_redesign_v1", "stable_keep_family_exists_for_generation_redesign", evidence
    if regime_spread >= 2 or len(hold) >= 3:
        return "regime_aware_family_filter_pretest", "family_effects_are_regime_dependent", evidence
    if len(harmful) >= 1:
        return "family_veto_pretest", "harmful_family_detected", evidence
    return "abandon_current_candidate_surface", "no_stable_or_actionable_family_separation", evidence


def run_family_map(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    mapped = _load_candidates(robustness_root)
    regimes = _load_month_regimes(robustness_root)
    yearly = _family_yearly_performance(mapped)
    regime_perf = _family_regime_performance(mapped, regimes)
    bvr = _bought_vs_rejected(mapped)
    failures = _failure_cases(mapped)
    kdh = _keep_drop_hold(yearly)
    coverage = _coverage(mapped)
    decision, reason, evidence = _choose_next_axis(kdh, yearly, regime_perf)

    _write_csv(output_root / "candidate_family_map.csv", mapped)
    _write_csv(output_root / "candidate_family_yearly_performance.csv", yearly)
    _write_csv(output_root / "candidate_family_regime_performance.csv", regime_perf)
    _write_csv(output_root / "candidate_family_bought_vs_rejected.csv", bvr)
    _write_csv(output_root / "candidate_family_failure_cases.csv", failures)
    _write_csv(output_root / "candidate_family_keep_drop_hold.csv", kdh)
    _write_csv(output_root / "feature_family_coverage.csv", coverage)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "metrics": evidence,
        "decision": decision,
        "reason_type": reason,
        "family_rules": list(FAMILIES),
        "scope": {"tradex_only": True, "replay_rerun": False, "rule_changed": False, "candidate_generation_changed": False, "ranking_changed": False, "optimization": False, "threshold_sweep": False, "meemee_ui_changed": False, "runtime_db_written": False, "publish_registry_changed": False},
        "diagnostic_only": {"post_run_outcomes_used_for_family_classification": False, "post_run_outcomes_used_for_performance_labels": True},
    }
    _write_json(output_root / "candidate_family_failure_summary.json", summary)
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "replay_rerun": False, "rule_changed": False, "candidate_generation_changed": False, "ranking_changed": False, "optimization": False, "threshold_sweep": False, "silent_fallback_used": False, "meemee_reflectable": False, "policy_promotion_allowed": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map candidate family failures across baseline replay years.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_family_map(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
