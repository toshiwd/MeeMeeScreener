from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "candidate_selection_quality_decomposition_v1"
SCHEMA_PREFIX = "tradex_candidate_selection_quality_decomposition_v1"
DEFAULT_OUTPUT_DIR_NAME = "candidate_selection_quality_decomposition_v1"

REQUIRED_ARTIFACTS = (
    "candidate_selection_quality_summary.json",
    "avoided_bad_entry_cases.csv",
    "missed_winner_due_to_confirmation_cases.csv",
    "strong_continuation_buy_cases.csv",
    "weak_one_day_candidate_cases.csv",
    "signal_date_feature_distribution.csv",
    "year_feature_separability.csv",
    "2025_upside_damage_cases.csv",
    "same_day_candidate_alternatives.csv",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "same_day_selection_filter_pretest",
    "score_component_quality_filter_pretest",
    "candidate_persistence_soft_weight_pretest",
    "candidate_generation_redesign",
    "abandon_entry_confirmation_axis",
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


def _as_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _rank_bucket(rank: Any) -> str:
    try:
        value = int(float(rank))
    except (TypeError, ValueError):
        return "unknown"
    if value <= 3:
        return "rank_1_3"
    if value <= 5:
        return "rank_4_5"
    if value <= 10:
        return "rank_6_10"
    if value <= 20:
        return "rank_11_20"
    if value <= 50:
        return "rank_21_50"
    return "rank_51_100"


def _parse_components(value: Any) -> dict[str, str]:
    if value is None or pd.isna(value):
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    out: dict[str, str] = {}
    if not isinstance(parsed, list):
        return out
    for item in parsed:
        if not isinstance(item, dict):
            continue
        feature = item.get("feature")
        val = item.get("value")
        if feature:
            out[str(feature)] = "" if val is None else str(val)
    return out


def _load_year_frames(robustness_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    yearly = pd.read_csv(robustness_root / "yearly_results.csv")
    candidates: list[pd.DataFrame] = []
    outcomes: list[pd.DataFrame] = []
    rejected: list[pd.DataFrame] = []
    for _idx, row in yearly.iterrows():
        year = int(row["year"])
        run_dir = Path(str(row["run_dir"]))
        cand = pd.read_csv(run_dir / "daily_candidate_snapshot.csv")
        cand["year"] = year
        cand["code"] = cand["code"].astype(str)
        cand["decision_ymd"] = cand["decision_ymd"].astype(int)
        candidates.append(cand)
        out = pd.read_csv(run_dir / "post_run_outcome_labels.csv")
        out["year"] = year
        out["code"] = out["code"].astype(str)
        out["decision_ymd"] = out["decision_ymd"].astype(int)
        outcomes.append(out)
        rejected_path = run_dir / "rejected_candidates.csv"
        if rejected_path.exists():
            rej = pd.read_csv(rejected_path)
            rej["year"] = year
            rej["code"] = rej["code"].astype(str)
            rej["decision_ymd"] = rej["decision_ymd"].astype(int)
            rejected.append(rej)
    return (
        pd.concat(candidates, ignore_index=True) if candidates else pd.DataFrame(),
        pd.concat(outcomes, ignore_index=True) if outcomes else pd.DataFrame(),
        pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame(),
    )


def _enrich_cases(cases: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    if cases.empty:
        return cases
    merged = cases.copy()
    merged["code"] = merged["code"].astype(str)
    merged["original_decision_ymd"] = merged["original_decision_ymd"].astype(int)
    cand_cols = [
        "year",
        "decision_ymd",
        "code",
        "candidate_rank",
        "selection_score",
        "entry_allowed_by_score",
        "downside_guard_blocked",
        "selected_for_buy",
        "reject_reason",
        "score_components_json",
    ]
    enriched = merged.merge(
        candidates[cand_cols],
        left_on=["year", "original_decision_ymd", "code"],
        right_on=["year", "decision_ymd", "code"],
        how="left",
        suffixes=("", "_signal"),
    )
    component_dicts = enriched["score_components_json"].map(_parse_components) if "score_components_json" in enriched.columns else pd.Series([{}] * len(enriched))
    for feature in sorted({key for item in component_dicts for key in item.keys()}):
        enriched[feature] = component_dicts.map(lambda item, name=feature: item.get(name, ""))
    enriched["rank_bucket"] = enriched.get("original_rank", enriched.get("candidate_rank", pd.Series(dtype=float))).map(_rank_bucket)
    enriched["score_delta_confirmation"] = pd.to_numeric(enriched.get("confirmation_score", pd.Series(dtype=float)), errors="coerce") - pd.to_numeric(enriched.get("original_score", pd.Series(dtype=float)), errors="coerce")
    enriched["rank_delta_confirmation"] = pd.to_numeric(enriched.get("confirmation_rank", pd.Series(dtype=float)), errors="coerce") - pd.to_numeric(enriched.get("original_rank", pd.Series(dtype=float)), errors="coerce")
    return enriched


def _feature_distribution(*frames: tuple[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    feature_cols = [
        "daily_ma_stack",
        "daily_ma60_slope_state",
        "daily_ret20_state",
        "daily_candle_state",
        "daily_volume_state",
        "daily_sequence_state",
        "weekly_trend_state",
        "weekly_ret4_state",
        "monthly_trend_state",
        "monthly_ret6_state",
        "rank_bucket",
    ]
    for case_type, frame in frames:
        if frame.empty:
            continue
        for feature in feature_cols:
            if feature not in frame.columns:
                continue
            counts = frame[feature].fillna("").astype(str).replace("", "unknown").value_counts()
            total = int(counts.sum())
            for value, count in counts.items():
                rows.append({"case_type": case_type, "feature": feature, "value": value, "count": int(count), "share": float(count / total) if total else 0.0})
    return pd.DataFrame(rows)


def _strong_and_weak(candidates: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected = candidates[_as_bool(candidates.get("selected_for_buy", pd.Series(dtype=bool)))].copy()
    joined = selected.merge(outcomes, on=["year", "decision_ymd", "code"], how="left")
    comp = joined["score_components_json"].map(_parse_components)
    for feature in sorted({key for item in comp for key in item.keys()}):
        joined[feature] = comp.map(lambda item, name=feature: item.get(name, ""))
    joined["rank_bucket"] = joined["candidate_rank"].map(_rank_bucket)
    post20 = pd.to_numeric(joined.get("post_ret_20", pd.Series(dtype=float)), errors="coerce")
    mfe20 = pd.to_numeric(joined.get("mfe_20", pd.Series(dtype=float)), errors="coerce")
    mae20 = pd.to_numeric(joined.get("mae_20", pd.Series(dtype=float)), errors="coerce")
    strong = joined[(post20 >= 0.08) | (mfe20 >= 0.12)].copy()
    weak = joined[(post20 <= -0.03) | (mae20 <= -0.08)].copy()
    return strong, weak


def _same_day_alternatives(candidates: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    joined = candidates.merge(outcomes, on=["year", "decision_ymd", "code"], how="left")
    joined["selected_bool"] = _as_bool(joined.get("selected_for_buy", pd.Series(dtype=bool)))
    rows: list[dict[str, Any]] = []
    for (year, ymd), group in joined.groupby(["year", "decision_ymd"], sort=True):
        selected = group[group["selected_bool"]].copy()
        rejected = group[~group["selected_bool"]].copy()
        if selected.empty or rejected.empty:
            continue
        selected_best = selected.sort_values("candidate_rank", kind="stable").iloc[0]
        rejected_best = rejected.sort_values("post_ret_20", ascending=False, na_position="last", kind="stable").iloc[0]
        selected_post = float(selected_best.get("post_ret_20")) if pd.notna(selected_best.get("post_ret_20")) else None
        rejected_post = float(rejected_best.get("post_ret_20")) if pd.notna(rejected_best.get("post_ret_20")) else None
        rows.append(
            {
                "year": int(year),
                "decision_ymd": int(ymd),
                "selected_code": str(selected_best["code"]),
                "selected_rank": int(selected_best["candidate_rank"]),
                "selected_score": float(selected_best["selection_score"]),
                "selected_post_ret20": selected_post,
                "alternative_code": str(rejected_best["code"]),
                "alternative_rank": int(rejected_best["candidate_rank"]),
                "alternative_score": float(rejected_best["selection_score"]),
                "alternative_post_ret20": rejected_post,
                "alternative_minus_selected_post_ret20": None if selected_post is None or rejected_post is None else rejected_post - selected_post,
                "alternative_reject_reason": rejected_best.get("reject_reason"),
            }
        )
    return pd.DataFrame(rows)


def _year_separability(avoided: pd.DataFrame, missed: pd.DataFrame, strong: pd.DataFrame, weak: pd.DataFrame) -> pd.DataFrame:
    years = sorted(set(pd.concat([frame[["year"]] for frame in (avoided, missed, strong, weak) if not frame.empty], ignore_index=True)["year"].astype(int).tolist())) if any(not frame.empty for frame in (avoided, missed, strong, weak)) else []
    rows: list[dict[str, Any]] = []
    for year in years:
        av = avoided[avoided["year"].astype(int) == year]
        ms = missed[missed["year"].astype(int) == year]
        st = strong[strong["year"].astype(int) == year]
        wk = weak[weak["year"].astype(int) == year]
        rows.append(
            {
                "year": year,
                "avoided_bad_entry_count": int(len(av)),
                "missed_winner_count": int(len(ms)),
                "strong_continuation_count": int(len(st)),
                "weak_one_day_count": int(len(wk)),
                "avoided_minus_missed": int(len(av) - len(ms)),
                "strong_to_weak_ratio": None if len(wk) == 0 else float(len(st) / len(wk)),
                "missed_winner_2025_damage_proxy": float(pd.to_numeric(ms.get("post_ret_20", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()),
            }
        )
    return pd.DataFrame(rows)


def _choose_next_axis(summary_metrics: dict[str, Any], feature_distribution: pd.DataFrame, same_day: pd.DataFrame) -> tuple[str, str, dict[str, Any]]:
    missed_2025 = int(summary_metrics["missed_winner_2025_count"])
    avoided = int(summary_metrics["avoided_bad_entry_count"])
    missed = int(summary_metrics["missed_winner_count"])
    same_day_big = int((pd.to_numeric(same_day.get("alternative_minus_selected_post_ret20", pd.Series(dtype=float)), errors="coerce") >= 0.08).sum()) if not same_day.empty else 0
    evidence = dict(summary_metrics)
    evidence["same_day_big_alternative_count"] = same_day_big
    if missed_2025 > 20:
        return "score_component_quality_filter_pretest", "2025_missed_winners_need_signal_date_quality_filter_not_delay", evidence
    if same_day_big >= 50:
        return "same_day_selection_filter_pretest", "same_day_alternatives_show_selection_order_improvement_potential", evidence
    if avoided > missed:
        return "candidate_persistence_soft_weight_pretest", "persistence_has_signal_but_hard_confirmation_cuts_winners", evidence
    if int(summary_metrics["weak_one_day_count"]) > int(summary_metrics["strong_continuation_count"]):
        return "candidate_generation_redesign", "weak_one_day_candidates_dominate_selected_pool", evidence
    return "abandon_entry_confirmation_axis", "entry_confirmation_related_features_do_not_separate_cases", evidence


def run_decomposition(robustness_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    robustness_root = Path(robustness_root)
    entry_root = robustness_root / "entry_confirmation_pretest_v1"
    output_root = Path(output_root) if output_root else robustness_root / DEFAULT_OUTPUT_DIR_NAME
    complete = _read_json(entry_root / "_ARTIFACT_COMPLETE.json")
    goal_summary = _read_json(entry_root / "goal_gate_summary.json")
    candidates, outcomes, rejected = _load_year_frames(robustness_root)
    outcome_cases = pd.read_csv(entry_root / "entry_confirmation_outcome_analysis.csv")
    outcome_cases["code"] = outcome_cases["code"].astype(str)
    avoided = _enrich_cases(outcome_cases[outcome_cases["entry_confirmation_outcome_class"].astype(str) == "avoided_bad_entry"].copy(), candidates)
    missed = _enrich_cases(outcome_cases[outcome_cases["entry_confirmation_outcome_class"].astype(str) == "missed_winner_due_to_confirmation"].copy(), candidates)
    strong, weak = _strong_and_weak(candidates, outcomes)
    damage_2025 = missed[missed["year"].astype(int) == 2025].copy()
    same_day = _same_day_alternatives(candidates, outcomes)
    feature_dist = _feature_distribution(("avoided_bad_entry", avoided), ("missed_winner_due_to_confirmation", missed), ("strong_continuation_buy", strong), ("weak_one_day_candidate", weak))
    separability = _year_separability(avoided, missed, strong, weak)
    metrics = {
        "avoided_bad_entry_count": int(len(avoided)),
        "missed_winner_count": int(len(missed)),
        "missed_winner_2025_count": int(len(damage_2025)),
        "strong_continuation_count": int(len(strong)),
        "weak_one_day_count": int(len(weak)),
        "same_day_candidate_alternative_rows": int(len(same_day)),
        "entry_confirmation_source_decision": complete.get("decision"),
        "entry_confirmation_2025_damage": goal_summary.get("return_damage_2025"),
    }
    decision, reason, evidence = _choose_next_axis(metrics, feature_dist, same_day)

    _write_csv(output_root / "avoided_bad_entry_cases.csv", avoided)
    _write_csv(output_root / "missed_winner_due_to_confirmation_cases.csv", missed)
    _write_csv(output_root / "strong_continuation_buy_cases.csv", strong)
    _write_csv(output_root / "weak_one_day_candidate_cases.csv", weak)
    _write_csv(output_root / "signal_date_feature_distribution.csv", feature_dist)
    _write_csv(output_root / "year_feature_separability.csv", separability)
    _write_csv(output_root / "2025_upside_damage_cases.csv", damage_2025)
    _write_csv(output_root / "same_day_candidate_alternatives.csv", same_day)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "robustness_root": str(robustness_root),
        "entry_confirmation_root": str(entry_root),
        "metrics": metrics,
        "decision": decision,
        "reason_type": reason,
        "scope": {"tradex_only": True, "replay_rerun": False, "rule_changed": False, "threshold_sweep": False, "optimization": False, "meemee_ui_changed": False, "runtime_db_written": False, "ranking_changed": False, "publish_registry_changed": False},
        "diagnostic_only": {"post_run_outcomes_used_for_classification": True, "post_run_outcomes_used_for_selection": False},
    }
    _write_json(output_root / "candidate_selection_quality_summary.json", summary)
    _write_json(output_root / "next_axis_decision.json", {"schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "decision_candidates": list(DECISIONS), "decision": decision, "decision_count": 1, "reason_type": reason, "metrics": evidence, "policy_promotion_allowed": False, "meemee_reflectable": False})
    artifact_complete = {"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "axis_id": AXIS_ID, "generated_at": _utc_now(), "complete": True, "required_artifacts_all_present": all((output_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"), "decision": decision, "decision_count": 1, "replay_rerun": False, "rule_changed": False, "threshold_sweep": False, "optimization": False, "silent_fallback_used": False, "meemee_reflectable": False, "policy_promotion_allowed": False}
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", artifact_complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "reason_type": reason, "metrics": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decompose candidate selection quality after entry confirmation pretest.")
    parser.add_argument("--robustness-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(json.dumps(_json_ready(run_decomposition(args.robustness_root, args.output_root)), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
