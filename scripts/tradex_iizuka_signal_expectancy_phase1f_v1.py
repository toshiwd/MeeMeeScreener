from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase1f_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase1f_v1"
DEFAULT_PHASE1C_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c\20260509T050355Z-080636")
DEFAULT_PHASE1E_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1e\20260509T052429Z-010093")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1f")
DEFAULT_EXPOSURE_SOURCE = Path(r"G:\Tradex\min_pool_reject_provenance_v1\20260503T044907Z-341202\min_pool_accepted_candidate_rows.parquet")
CANDIDATE_ID = "monthly_C_mixed_pullback_end_reclaim7_v1"
BOOSTS = {"mixed_signal_boost_light": 0.02, "mixed_signal_boost_medium": 0.05}
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase1f_key_semantics_gate.json",
    "phase1f_candidate_pool_snapshot.json",
    "phase1f_champion_baseline_metrics.json",
    "phase1f_boost_test_results.json",
    "phase1f_branching_analysis.json",
    "phase1f_bad_pick_removal_analysis.json",
    "phase1f_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _norm_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def _date(value: Any) -> str | None:
    parsed = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.strftime("%Y-%m-%d")


def _prepare_mixed(phase1c_root: Path) -> pd.DataFrame:
    rows = pd.read_parquet(phase1c_root / "phase1c_mixed_signal_rows.parquet").copy()
    rows["code_norm"] = rows["symbol"].map(_norm_code)
    rows["decision_date"] = rows["decision_date"].map(_date)
    rows["execution_date"] = rows["execution_date"].map(_date)
    return rows


def _prepare_pool(path: Path) -> pd.DataFrame:
    rows = pd.read_parquet(path).copy()
    rows["code_norm"] = rows["symbol"].map(_norm_code)
    rows["ranking_date"] = rows["anchor_date"].map(_date)
    rows = rows.loc[rows["side"].astype(str).str.lower() == "long"].copy()
    rows["score"] = pd.to_numeric(rows["score"], errors="coerce").fillna(0.0)
    rows["rank"] = pd.to_numeric(rows["rank"], errors="coerce")
    rows["forward_ret_20d"] = pd.to_numeric(rows.get("forward_ret_20d"), errors="coerce")
    return rows


def _nearest(mixed: pd.DataFrame, pool: pd.DataFrame, direction: str) -> pd.DataFrame:
    left = mixed[["code_norm", "decision_date", "execution_date", "ret5", "ret10", "ret20", "mae20", "mixed_internal_combination"]].copy()
    left["decision_ts"] = pd.to_datetime(left["decision_date"], errors="coerce")
    right = pool.copy()
    right["ranking_ts"] = pd.to_datetime(right["ranking_date"], errors="coerce")
    chunks = []
    for code, group in left.groupby("code_norm"):
        r = right.loc[right["code_norm"] == code].sort_values("ranking_ts")
        if r.empty:
            continue
        joined = pd.merge_asof(group.sort_values("decision_ts"), r, by="code_norm", left_on="decision_ts", right_on="ranking_ts", direction=direction, allow_exact_matches=True)
        chunks.append(joined.dropna(subset=["ranking_date"]))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def _exact(mixed: pd.DataFrame, pool: pd.DataFrame, signal_date: str) -> pd.DataFrame:
    return mixed.merge(pool, left_on=["code_norm", signal_date], right_on=["code_norm", "ranking_date"], how="inner")


def _key_gate(mixed: pd.DataFrame, pool: pd.DataFrame) -> dict[str, Any]:
    policies = {
        "code+decision_date": _exact(mixed, pool, "decision_date"),
        "code+execution_date": _exact(mixed, pool, "execution_date"),
        "code+nearest_prior_ranking_date": _nearest(mixed, pool, "backward"),
        "code+nearest_next_ranking_date": _nearest(mixed, pool, "forward"),
    }
    results = {}
    safe_candidates = []
    for name, rows in policies.items():
        has_future_labels = any(col in rows.columns for col in ["forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "top20pct_label"])
        if len(rows):
            deltas = (pd.to_datetime(rows["ranking_date"]) - pd.to_datetime(rows["decision_date"])).dt.days
            min_delta = int(deltas.min())
            max_delta = int(deltas.max())
            future_share = float((deltas > 0).mean())
        else:
            min_delta = max_delta = None
            future_share = None
        safe = name != "code+nearest_next_ranking_date" and not has_future_labels
        # Candidate source carries evaluation-only outcome columns, so no policy is safe for rank-feature use unless we ignore those fields.
        audit_only_safe = name != "code+nearest_next_ranking_date"
        top20 = int((pd.to_numeric(rows.get("rank"), errors="coerce") <= 20).sum()) if len(rows) and "rank" in rows else 0
        results[name] = {
            "matched_signal_rows": int(len(rows)),
            "top20_overlap": top20,
            "ranking_minus_decision_days_min": min_delta,
            "ranking_minus_decision_days_max": max_delta,
            "future_ranking_date_share": future_share,
            "source_contains_future_or_post_horizon_columns": has_future_labels,
            "safe_for_no_lookahead_rank_pretest": audit_only_safe,
            "strict_safe_without_ignoring_outcome_columns": safe,
        }
        if audit_only_safe and len(rows) >= 100:
            safe_candidates.append((name, len(rows), top20))
    if safe_candidates:
        selected = max(safe_candidates, key=lambda x: (x[1], x[2]))[0]
        decision = "pass"
        reason = "safe_non_future_key_policy_has_sufficient_overlap_when_outcome_columns_are_excluded_from_features"
    else:
        selected = None
        decision = "blocked"
        reason = "no_safe_key_policy_has_sufficient_overlap"
    return {
        "schema_version": f"{SCHEMA_VERSION}_key_semantics_gate_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "decision": decision,
        "decision_reason": reason,
        "selected_key_policy": selected,
        "requested_best_policy_from_phase1e": "code+nearest_next_ranking_date",
        "nearest_next_policy_allowed": False,
        "policy_results": results,
        "feature_exclusion_required": ["forward_ret_20d", "path_value_score_v1", "top15_label", "top20pct_label", "bottom15_label"],
    }


def _selected_rows(mixed: pd.DataFrame, pool: pd.DataFrame, policy: str) -> pd.DataFrame:
    if policy == "code+decision_date":
        return _exact(mixed, pool, "decision_date")
    if policy == "code+execution_date":
        return _exact(mixed, pool, "execution_date")
    if policy == "code+nearest_prior_ranking_date":
        return _nearest(mixed, pool, "backward")
    raise ValueError(policy)


def _top_metrics(rows: pd.DataFrame, score_col: str, k: int) -> dict[str, Any]:
    selected = rows.sort_values(["ranking_date", score_col, "code_norm"], ascending=[True, False, True]).groupby("ranking_date").head(k).copy()
    ret5 = pd.to_numeric(selected.get("ret5"), errors="coerce")
    ret10 = pd.to_numeric(selected.get("ret10"), errors="coerce")
    ret20 = pd.to_numeric(selected.get("ret20"), errors="coerce")
    return {
        "count": int(len(selected)),
        "ret5_mean": float(ret5.mean()) if len(selected) else None,
        "ret10_mean": float(ret10.mean()) if len(selected) else None,
        "ret20_mean": float(ret20.mean()) if len(selected) else None,
        "ret5_median": float(ret5.median()) if len(selected) else None,
        "ret10_median": float(ret10.median()) if len(selected) else None,
        "ret20_median": float(ret20.median()) if len(selected) else None,
        "win_rate_20": float((ret20 > 0).mean()) if len(selected) else None,
        "keys": set((selected["ranking_date"].astype(str) + "|" + selected["code_norm"].astype(str)).tolist()),
    }


def _variant(pool: pd.DataFrame, matched_keys: set[str], boost: float) -> pd.DataFrame:
    out = pool.copy()
    out["candidate_key"] = out["ranking_date"].astype(str) + "|" + out["code_norm"].astype(str)
    out["variant_score"] = out["score"] + out["candidate_key"].isin(matched_keys).astype(float) * boost
    return out


def run_phase1f(*, phase1c_root: Path, phase1e_root: Path, exposure_source: Path, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    mixed = _prepare_mixed(phase1c_root)
    pool = _prepare_pool(exposure_source)
    gate = _key_gate(mixed, pool)
    selected_policy = gate["selected_key_policy"]
    pretest_ran = gate["decision"] == "pass" and selected_policy is not None
    if pretest_ran:
        matched = _selected_rows(mixed, pool, selected_policy)
        matched_keys = set((matched["ranking_date"].astype(str) + "|" + matched["code_norm"].astype(str)).tolist())
        eval_pool = pool.merge(mixed[["code_norm", "ret5", "ret10", "ret20"]], on="code_norm", how="left")
        eval_pool["candidate_key"] = eval_pool["ranking_date"].astype(str) + "|" + eval_pool["code_norm"].astype(str)
        baseline_metrics = {f"top{k}": _top_metrics(eval_pool, "score", k) for k in (5, 10, 20)}
        baseline_keys = {k: v.pop("keys") for k, v in baseline_metrics.items()}
        boost_results = {}
        branching = {}
        for name, boost in BOOSTS.items():
            rows = _variant(eval_pool, matched_keys, boost)
            metrics = {f"top{k}": _top_metrics(rows, "variant_score", k) for k in (5, 10, 20)}
            variant_keys = {k: v.pop("keys") for k, v in metrics.items()}
            boost_results[name] = {"boost_value": boost, "metrics": metrics}
            branching[name] = {
                "changed_top5_members_count": len(baseline_keys["top5"] ^ variant_keys["top5"]),
                "changed_top10_members_count": len(baseline_keys["top10"] ^ variant_keys["top10"]),
                "changed_rank_count": len(baseline_keys["top20"] ^ variant_keys["top20"]),
                "top5_overlap_with_champion": len(baseline_keys["top5"] & variant_keys["top5"]) / max(len(baseline_keys["top5"]), 1),
                "top10_overlap_with_champion": len(baseline_keys["top10"] & variant_keys["top10"]) / max(len(baseline_keys["top10"]), 1),
                "ret20_top5_delta": (metrics["top5"]["ret20_mean"] or 0) - (baseline_metrics["top5"]["ret20_mean"] or 0),
                "ret20_top10_delta": (metrics["top10"]["ret20_mean"] or 0) - (baseline_metrics["top10"]["ret20_mean"] or 0),
                "ret20_top20_delta": (metrics["top20"]["ret20_mean"] or 0) - (baseline_metrics["top20"]["ret20_mean"] or 0),
            }
        best_name = max(branching, key=lambda n: (branching[n]["ret20_top10_delta"], branching[n]["ret20_top20_delta"]))
        best = branching[best_name]
        if max(best["ret20_top5_delta"], best["ret20_top10_delta"], best["ret20_top20_delta"]) > 0 and best["changed_top10_members_count"] <= 40:
            decision = "proceed_to_challenger_candidate"
            reason = "fixed_boost_variant_improved_topk_without_excessive_churn"
        elif max(best["ret20_top5_delta"], best["ret20_top10_delta"], best["ret20_top20_delta"]) > 0:
            decision = "hold_for_boost_design"
            reason = "boost_helped_but_churn_needs_separately_versioned_design"
        else:
            decision = "explain_only"
            reason = "rank_movement_did_not_improve_topk"
    else:
        matched = pd.DataFrame()
        baseline_metrics = {}
        boost_results = {}
        branching = {}
        decision = "blocked"
        reason = gate["decision_reason"]

    bad_pick = {
        "schema_version": f"{SCHEMA_VERSION}_bad_pick_removal_analysis_v1",
        "generated_at_utc": _utc_now(),
        "status": "confirmed" if pretest_ran else "blocked",
        "bad_pick_removal_count": None,
        "newly_added_pick_outcome": "available_in_branching_artifact" if pretest_ran else None,
        "removed_pick_outcome": "available_in_branching_artifact" if pretest_ran else None,
        "note": "No explicit bad_pick label was used for optimization; analysis is limited to ret20 deltas and membership churn.",
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "authoritative_decision": decision,
        "decision_reason": reason,
        "key_policy_selected": selected_policy,
        "key_semantics_decision": gate["decision"],
        "ranking_pretest_ran": pretest_ran,
        "boost_variants_tested": BOOSTS,
        "future_ranking_challenger_candidate_allowed": decision == "proceed_to_challenger_candidate",
        "meemee_reflection": "blocked",
        "production_ranking_changed": False,
        "publish_changed": False,
    }
    artifacts = {
        "run_manifest.json": {"schema_version": f"{SCHEMA_VERSION}_manifest_v1", "generated_at_utc": _utc_now(), "script_name": SCRIPT_NAME, "session_root": str(session_root), "boundary": "TRADEX-only", "ranking_challenger_created": False, "meemee_changed": False, "production_ranking_changed": False, "publish_changed": False},
        "input_resolution.json": {"schema_version": f"{SCHEMA_VERSION}_input_resolution_v1", "phase1c_root": str(phase1c_root), "phase1e_root": str(phase1e_root), "exposure_source": str(exposure_source), "mixed_rows": int(len(mixed)), "candidate_pool_rows": int(len(pool))},
        "phase1f_key_semantics_gate.json": gate,
        "phase1f_candidate_pool_snapshot.json": {"schema_version": f"{SCHEMA_VERSION}_candidate_pool_snapshot_v1", "row_count": int(len(pool)), "date_range": {"min": str(pool["ranking_date"].min()), "max": str(pool["ranking_date"].max())}, "score_column": "score", "rank_column": "rank", "future_columns_excluded_from_features": gate["feature_exclusion_required"]},
        "phase1f_champion_baseline_metrics.json": {"schema_version": f"{SCHEMA_VERSION}_champion_baseline_metrics_v1", "metrics": baseline_metrics},
        "phase1f_boost_test_results.json": {"schema_version": f"{SCHEMA_VERSION}_boost_test_results_v1", "boost_results": boost_results},
        "phase1f_branching_analysis.json": {"schema_version": f"{SCHEMA_VERSION}_branching_analysis_v1", "branching": branching},
        "phase1f_bad_pick_removal_analysis.json": bad_pick,
        "phase1f_decision.json": decision_payload,
    }
    for name, payload in artifacts.items():
        _write_json(session_root / name, payload)
    complete = {"schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1", "generated_at_utc": _utc_now(), "session_root": str(session_root), "required_artifacts": REQUIRED_ARTIFACTS, "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json")}
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision, "ranking_pretest_ran": pretest_ran}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase1c-root", default=str(DEFAULT_PHASE1C_ROOT))
    parser.add_argument("--phase1e-root", default=str(DEFAULT_PHASE1E_ROOT))
    parser.add_argument("--exposure-source", default=str(DEFAULT_EXPOSURE_SOURCE))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase1f(
        phase1c_root=_safe_path(args.phase1c_root, DEFAULT_PHASE1C_ROOT),
        phase1e_root=_safe_path(args.phase1e_root, DEFAULT_PHASE1E_ROOT),
        exposure_source=_safe_path(args.exposure_source, DEFAULT_EXPOSURE_SOURCE),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
    )
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
