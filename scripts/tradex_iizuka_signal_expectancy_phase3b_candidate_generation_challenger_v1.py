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

from scripts import tradex_iizuka_signal_expectancy_phase3_candidate_generation_v1 as phase3
from scripts import tradex_iizuka_signal_expectancy_v1 as base

SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase3b_candidate_generation_challenger_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase3b_candidate_generation_challenger_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase3b_candidate_generation_challenger")
PHASE3_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase3_candidate_generation\20260509T073130Z-180617")
PHASE2C3_ROOT = Path(r"G:\Tradex\point_in_time_candidate_pool_contract_v1_phase2c3\20260509T071034Z-875642")
SIGNAL_ROWS = phase3.SIGNAL_ROWS
POOL_ROWS = phase3.POOL_ROWS
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase3b_candidate_generation_snapshot.json",
    "phase3b_additive_pool_rows.parquet",
    "phase3b_same_condition_comparison.json",
    "phase3b_topk_rank_readiness.json",
    "phase3b_severe_loser_audit.json",
    "phase3b_unsafe_classification_audit.json",
    "phase3b_decision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    base._write_json(path, payload)


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    base._write_parquet(path, frame)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    out = frame.copy()
    if "mae20" not in out.columns and "mae_20d" in out.columns:
        out["mae20"] = out["mae_20d"]
    elif "mae20" in out.columns and "mae_20d" in out.columns:
        mae20 = out["mae20"]
        if isinstance(mae20, pd.DataFrame):
            mae20 = mae20.iloc[:, 0]
        out["mae20"] = mae20.where(mae20.notna(), out["mae_20d"])
        out = out.drop(columns=["mae_20d"])
    return phase3._metrics(out)


def _concentration(frame: pd.DataFrame) -> dict[str, Any]:
    return phase3._concentration(frame)


def _delta(a: Any, b: Any) -> float | None:
    return phase3._delta(a, b)


def _make_added_rows(absent: pd.DataFrame, pool_columns: list[str]) -> pd.DataFrame:
    rows = pd.DataFrame(index=absent.index)
    for column in pool_columns:
        rows[column] = pd.NA
    rows["symbol"] = absent["symbol"].astype(str)
    rows["symbol_norm"] = absent["symbol_norm"].astype(str)
    rows["anchor_date"] = absent["decision_date_norm"]
    rows["candidate_date"] = absent["execution_date_norm"]
    rows["candidate_date_norm"] = absent["execution_date_norm"]
    rows["as_of_date"] = absent["decision_date_norm"]
    rows["feature_cutoff_date"] = absent["decision_date_norm"]
    rows["side"] = "long"
    rows["side_norm"] = "long"
    rows["ret5"] = absent["ret5"].to_numpy()
    rows["ret10"] = absent["ret10"].to_numpy()
    rows["ret20"] = absent["ret20"].to_numpy()
    rows["mae20"] = absent["mae20"].to_numpy()
    rows["sector"] = absent.get("sector", pd.Series("unknown", index=absent.index)).to_numpy()
    rows["liquidity_bucket"] = absent.get("liquidity_bucket", pd.Series("unknown", index=absent.index)).to_numpy()
    rows["candidate_source"] = "iizuka_mixed_signal"
    rows["added_by_challenger"] = True
    rows["source_signal"] = "monthly_C_mixed_pullback_end_reclaim7_v1"
    rows["classification_status"] = "absent_from_candidate_pool"
    rows["no_lookahead_status"] = absent["no_lookahead_valid"].map(lambda ok: "signal_no_lookahead_valid" if bool(ok) else "signal_no_lookahead_invalid")
    rows["source_signal_decision_date"] = absent["decision_date_norm"].to_numpy()
    rows["source_signal_execution_date"] = absent["execution_date_norm"].to_numpy()
    rows["mixed_internal_combination"] = absent.get("mixed_internal_combination", pd.Series(pd.NA, index=absent.index)).to_numpy()
    rows["champion_score"] = pd.NA
    rows["champion_rank"] = pd.NA
    rows["top5_membership"] = False
    rows["top10_membership"] = False
    rows["top20_membership"] = False
    rows["top50_membership"] = False
    return rows.reset_index(drop=True)


def _severe_loser_audit(added: pd.DataFrame, champion: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(added["ret20"], errors="coerce")
    mae = pd.to_numeric(added["mae20"], errors="coerce")
    severe = added.loc[(ret20 < 0) & (mae <= -0.08)].copy()
    champ_metrics = _metrics(champion.rename(columns={"mae_20d": "mae20"}))
    added_metrics = _metrics(added)
    def dist(column: str) -> dict[str, int]:
        if column not in severe.columns:
            return {}
        return severe[column].fillna("unknown").astype(str).value_counts().to_dict()
    tmp = severe.copy()
    tmp["year"] = pd.to_datetime(tmp["source_signal_decision_date"], errors="coerce").dt.year.astype("string")
    return {
        "schema_version": f"{SCHEMA_VERSION}_severe_loser_audit_v1",
        "generated_at_utc": _utc_now(),
        "severe_loser_count": int(len(severe)),
        "severe_loser_share": added_metrics["severe_loser_share"],
        "severe_loser_delta_vs_champion_pool": _delta(added_metrics["severe_loser_share"], champ_metrics["severe_loser_share"]),
        "by_year": tmp["year"].fillna("unknown").astype(str).value_counts().to_dict() if len(tmp) else {},
        "by_sector": dist("sector"),
        "by_liquidity_bucket": dist("liquidity_bucket"),
        "by_internal_combination": dist("mixed_internal_combination"),
    }


def run_phase3b(*, output_root: Path) -> dict[str, Any]:
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    phase3_decision = _read_json(PHASE3_ROOT / "phase3_decision.json")
    phase3_membership = _read_json(PHASE3_ROOT / "phase3_signal_pool_membership.json")
    signal = phase3._prep_signal(pd.read_parquet(SIGNAL_ROWS))
    pool = phase3._prep_pool(pd.read_parquet(POOL_ROWS))
    classified, membership = phase3._classify(signal, pool)
    absent = classified.loc[classified["pool_membership_class"].eq("absent_from_candidate_pool")].copy()
    unsafe = classified.loc[classified["pool_membership_class"].eq("unsafe_to_classify")].copy()
    added = _make_added_rows(absent, list(pool.columns))
    champion = pool.copy()
    champion["candidate_source"] = "champion_candidate_pool"
    champion["added_by_challenger"] = False
    champion["classification_status"] = "already_in_candidate_pool"
    additive = pd.concat([champion, added], ignore_index=True, sort=False)

    champion_metrics = _metrics(champion)
    additive_metrics = _metrics(additive)
    added_metrics = _metrics(added)
    comparison = {
        "schema_version": f"{SCHEMA_VERSION}_same_condition_comparison_v1",
        "generated_at_utc": _utc_now(),
        "champion_candidate_pool": champion_metrics,
        "iizuka_mixed_additive_pool": additive_metrics,
        "added_signal_candidates": added_metrics,
        "candidate_pool_count_delta": int(len(additive) - len(champion)),
        "added_signal_candidate_count": int(len(added)),
        "ret20_mean_delta": _delta(additive_metrics["ret20_mean"], champion_metrics["ret20_mean"]),
        "ret20_median_delta": _delta(additive_metrics["ret20_median"], champion_metrics["ret20_median"]),
        "win_rate20_delta": _delta(additive_metrics["win_rate_20"], champion_metrics["win_rate_20"]),
        "severe_loser_delta": _delta(additive_metrics["severe_loser_share"], champion_metrics["severe_loser_share"]),
        "failed_followthrough_delta": _delta(additive_metrics["failed_followthrough_share"], champion_metrics["failed_followthrough_share"]),
        "additive_concentration": _concentration(additive),
        "added_signal_concentration": _concentration(added),
    }
    topk = {
        "schema_version": f"{SCHEMA_VERSION}_topk_rank_readiness_v1",
        "generated_at_utc": _utc_now(),
        "topk_comparison_status": "blocked_added_rows_have_no_comparable_champion_score_or_rank",
        "candidate_generation_pool_improvement_only": True,
        "do_not_fake_ranks": True,
        "added_rows_with_champion_score": int(added["champion_score"].notna().sum()),
        "added_rows_with_champion_rank": int(added["champion_rank"].notna().sum()),
    }
    severe = _severe_loser_audit(added, champion)
    unsafe_audit = {
        "schema_version": f"{SCHEMA_VERSION}_unsafe_classification_audit_v1",
        "generated_at_utc": _utc_now(),
        "unsafe_to_classify_count": int(len(unsafe)),
        "unsafe_reason": "signal execution date outside verified point-in-time pool date range or no safe prior pool date",
        "unsafe_rows_excluded_from_decision_evidence": True,
        "pool_candidate_date_min": membership["pool_candidate_date_min"],
        "pool_candidate_date_max": membership["pool_candidate_date_max"],
        "unsafe_by_year": pd.to_datetime(unsafe["decision_date"], errors="coerce").dt.year.astype("string").fillna("unknown").value_counts().to_dict() if len(unsafe) else {},
    }
    snapshot = {
        "schema_version": f"{SCHEMA_VERSION}_snapshot_v1",
        "generated_at_utc": _utc_now(),
        "champion_candidate_count": int(len(champion)),
        "additive_candidate_count": int(len(additive)),
        "added_signal_candidate_count": int(len(added)),
        "unsafe_to_classify_count": int(len(unsafe)),
        "candidate_pool_count_delta": int(len(additive) - len(champion)),
        "source_phase3_decision": phase3_decision.get("authoritative_decision"),
        "source_phase3_membership": phase3_membership,
    }
    improves = (
        (comparison["ret20_mean_delta"] or -999) > 0
        and (comparison["ret20_median_delta"] or -999) > 0
        and (comparison["win_rate20_delta"] or -999) > 0
    )
    severe_delta = severe["severe_loser_delta_vs_champion_pool"]
    if int(len(added)) == 0:
        decision = "blocked"
        reason = "no_safe_signal_only_rows_to_add"
        allow_next = False
        risk_filter_required = False
    elif improves and severe_delta is not None and severe_delta <= 0.05:
        decision = "proceed_to_versioned_candidate_generation_challenger"
        reason = "additive_pool_improves_with_acceptable_severe_loser_delta"
        allow_next = True
        risk_filter_required = False
    elif improves:
        decision = "hold_for_risk_filter_design"
        reason = "additive_pool_improves_but_severe_loser_delta_requires_fixed_filter"
        allow_next = False
        risk_filter_required = True
    elif int(len(unsafe)) > int(len(added)):
        decision = "hold_for_classification_coverage"
        reason = "safe_rows_insufficient_relative_to_unsafe_rows"
        allow_next = False
        risk_filter_required = False
    else:
        decision = "explain_only"
        reason = "additive_candidate_generation_value_is_weak"
        allow_next = False
        risk_filter_required = False
    decision_payload = {
        "schema_version": f"{SCHEMA_VERSION}_decision_v1",
        "generated_at_utc": _utc_now(),
        "authoritative_decision": decision,
        "decision_reason": reason,
        "champion_candidate_count": int(len(champion)),
        "additive_candidate_count": int(len(additive)),
        "added_signal_candidate_count": int(len(added)),
        "unsafe_to_classify_count": int(len(unsafe)),
        "ret20_mean_delta": comparison["ret20_mean_delta"],
        "ret20_median_delta": comparison["ret20_median_delta"],
        "win_rate20_delta": comparison["win_rate20_delta"],
        "severe_loser_delta": severe_delta,
        "topk_rank_readiness": topk["topk_comparison_status"],
        "future_versioned_candidate_generation_challenger_allowed": allow_next,
        "risk_filter_required_before_candidate_generation": risk_filter_required,
        "meemee_changed": False,
        "production_ranking_changed": False,
        "publish_changed": False,
        "signal_definition_changed": False,
        "thresholds_changed": False,
        "champion_scoring_logic_changed": False,
        "ranking_challenger_created": False,
    }
    artifacts = {
        "run_manifest.json": {
            "schema_version": f"{SCHEMA_VERSION}_manifest_v1",
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_root": str(session_root),
            "boundary": "TRADEX-only",
        },
        "input_resolution.json": {
            "schema_version": f"{SCHEMA_VERSION}_input_resolution_v1",
            "generated_at_utc": _utc_now(),
            "phase3_root": str(PHASE3_ROOT),
            "phase2c3_root": str(PHASE2C3_ROOT),
            "signal_rows": str(SIGNAL_ROWS),
            "candidate_pool_rows": str(POOL_ROWS),
        },
        "phase3b_candidate_generation_snapshot.json": snapshot,
        "phase3b_same_condition_comparison.json": comparison,
        "phase3b_topk_rank_readiness.json": topk,
        "phase3b_severe_loser_audit.json": severe,
        "phase3b_unsafe_classification_audit.json": unsafe_audit,
        "phase3b_decision.json": decision_payload,
    }
    for name, body in artifacts.items():
        _write_json(session_root / name, body)
    _write_parquet(session_root / "phase3b_additive_pool_rows.parquet", additive)
    complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"session_root": str(session_root), "decision": decision, "added_signal_candidate_count": int(len(added))}


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Phase 3b Iizuka candidate-generation challenger pretest")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run_phase3b(output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT))
    print(json.dumps(base._json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
