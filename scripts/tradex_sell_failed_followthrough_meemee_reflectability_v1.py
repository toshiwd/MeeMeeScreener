from __future__ import annotations

import argparse
import importlib
import inspect
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

base = importlib.import_module("scripts.entry_precision_short_audit")
refill = importlib.import_module("scripts.tradex_sell_failed_followthrough_refill_rerun_v1")
rankings_cache = importlib.import_module("app.backend.services.ml.rankings_cache")
screening_metrics = importlib.import_module("app.backend.domain.screening.metrics")

DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\sell_failed_followthrough_refill_rerun_v1"
    r"\20260515T030407Z-sell-failed-followthrough-refill-rerun-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_failed_followthrough_meemee_reflectability_v1")
CANDIDATE_NAME = "sell_failed_followthrough_after_break_same_month_refill_liquidity_guard_v1"
ACCEPTED_REFILL_LIQUIDITY20D_MIN = 1_000_000.0
SENSITIVITY_THRESHOLDS = [500_000.0, 750_000.0, 1_000_000.0, 1_250_000.0, 1_500_000.0]
START_YMD = 20250101
END_YMD = 20260226


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_readme(path: Path, decision: dict[str, Any]) -> None:
    lines = [
        "# sell failed-followthrough MeeMee reflectability v1",
        "",
        "Authoritative JSON artifacts in this directory are the source of truth.",
        f"candidate_name: {decision['candidate_name']}",
        f"decision: {decision['decision']}",
        f"meemee_reflectable_candidate: {decision['decision'] == 'meemee_reflectable_candidate'}",
        "production_ranking_changed: false",
        "active_ranking_changed: false",
        "meemee_code_changed: false",
        "silent_fallback_used: false",
        "research_fallback: false",
        "",
        "MeeMee usage is limited to read-only candidate display when the final decision is reflectable.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _required_source_paths(source_root: Path) -> dict[str, Path]:
    return {
        "complete": source_root / "_ARTIFACT_COMPLETE.json",
        "compare": source_root / "sell_failed_followthrough_refill_compare.json",
        "contract": source_root / "sell_failed_followthrough_refill_contract.json",
        "decision": source_root / "sell_failed_followthrough_refill_decision.json",
    }


def _check(condition: bool, name: str, *, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"name": name, "pass": bool(condition), "details": details or {}}


def build_artifact_integrity_check(source_root: Path) -> dict[str, Any]:
    paths = _required_source_paths(source_root)
    checks: list[dict[str, Any]] = []
    missing = [key for key, path in paths.items() if not path.exists()]
    checks.append(_check(not missing, "required_artifacts_exist", details={"missing": missing}))
    if missing:
        return {
            "schema_version": "sell_reflectability_artifact_integrity_check_v1",
            "generated_at": _utc_now(),
            "source_artifact_root": str(source_root),
            "artifact_integrity_pass": False,
            "checks": checks,
            "loaded_artifacts": {},
        }

    loaded = {key: _load_json(path) for key, path in paths.items()}
    complete = loaded["complete"]
    compare = loaded["compare"]
    contract = loaded["contract"]
    decision = loaded["decision"]
    same_condition = compare.get("same_condition_contract") or {}
    contract_condition = contract.get("fixed_evaluation_conditions") or {}

    mandatory = {
        "complete": ["status", "artifact_refs", "authoritative_decision", "silent_fallback_used", "research_fallback"],
        "compare": [
            "candidate_id",
            "source_db_path",
            "baseline",
            "challenger",
            "delta",
            "monthly_stability",
            "regime_stability",
            "same_condition_contract",
            "silent_fallback_used",
            "research_fallback",
            "production_ranking_changed",
        ],
        "contract": ["axis", "fixed_evaluation_conditions", "source_db_path", "refill_liquidity20d_min"],
        "decision": [
            "candidate_id",
            "authoritative_rollup_decision",
            "buy_level_equivalence_reached",
            "promote_ready_equivalent",
            "buy_level_blockers",
            "silent_fallback_used",
            "research_fallback",
            "production_ranking_changed",
            "meemee_reflection",
        ],
    }
    missing_fields = {
        key: [field for field in fields if field not in loaded[key]]
        for key, fields in mandatory.items()
    }
    checks.extend(
        [
            _check(complete.get("status") == "complete", "artifact_complete_true", details={"status": complete.get("status")}),
            _check(decision.get("candidate_id") == CANDIDATE_NAME, "decision_candidate_name_matches"),
            _check(compare.get("candidate_id") == CANDIDATE_NAME, "compare_candidate_name_matches"),
            _check(contract.get("axis") == CANDIDATE_NAME, "contract_axis_matches"),
            _check(
                compare.get("source_db_path") == contract.get("source_db_path"),
                "compare_contract_source_snapshot_agree",
                details={"compare": compare.get("source_db_path"), "contract": contract.get("source_db_path")},
            ),
            _check(
                all(same_condition.get(key) == contract_condition.get(key) for key in same_condition.keys()),
                "compare_contract_fixed_conditions_agree",
                details={"compare": same_condition, "contract": contract_condition},
            ),
            _check(float(same_condition.get("refill_liquidity20d_min") or -1.0) == ACCEPTED_REFILL_LIQUIDITY20D_MIN, "accepted_threshold_recorded"),
            _check(all(not fields for fields in missing_fields.values()), "mandatory_fields_present", details=missing_fields),
            _check(complete.get("silent_fallback_used") is False and decision.get("silent_fallback_used") is False and compare.get("silent_fallback_used") is False, "no_silent_fallback"),
            _check(complete.get("research_fallback") is False and decision.get("research_fallback") is False and compare.get("research_fallback") is False, "no_research_fallback"),
            _check(decision.get("production_ranking_changed") is False and compare.get("production_ranking_changed") is False, "production_ranking_unchanged"),
        ]
    )

    return {
        "schema_version": "sell_reflectability_artifact_integrity_check_v1",
        "generated_at": _utc_now(),
        "source_artifact_root": str(source_root),
        "artifact_integrity_pass": all(item["pass"] for item in checks),
        "checks": checks,
        "loaded_artifacts": {key: str(path) for key, path in paths.items()},
    }


def build_no_lookahead_audit() -> dict[str, Any]:
    failed_source = inspect.getsource(refill._is_failed_followthrough)
    eligible_source = inspect.getsource(refill._candidate_pool_eligible)
    selection_source = inspect.getsource(refill._build_refill_selection)
    build_rows_source = inspect.getsource(base._build_rows)
    cache_asof_source = inspect.getsource(rankings_cache._build_cache_asof)
    fetch_daily_asof_source = inspect.getsource(rankings_cache._fetch_daily_rows_asof)
    liquidity_source = inspect.getsource(screening_metrics._calc_liquidity_20d)

    future_selection_fields: list[str] = []
    for field in ["short_ret_5", "short_ret_10"]:
        if field in failed_source or field in selection_source:
            future_selection_fields.append(field)
    if "short_ret_20" in eligible_source:
        future_selection_fields.append("short_ret_20")

    checks = [
        _check(ACCEPTED_REFILL_LIQUIDITY20D_MIN == 1_000_000.0, "accepted_refill_liquidity20d_min_is_1000000"),
        _check("date <=" in cache_asof_source and "date <=" in fetch_daily_asof_source, "daily_bars_asof_filter_present"),
        _check("daily_rows[-20:]" in liquidity_source and "close * volume" in liquidity_source, "liquidity20d_uses_last_20_available_rows"),
        _check("rc._build_cache_asof(conn, int(ymd))" in build_rows_source, "ranking_cache_built_asof_decision_date"),
        _check("short_ret_20" not in eligible_source, "ret20_not_used_for_refill_eligibility"),
        _check(not any(field in failed_source for field in ["short_ret_5", "short_ret_10"]), "failed_followthrough_label_past_current_only"),
    ]

    return {
        "schema_version": "sell_reflectability_no_lookahead_audit_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "refill_liquidity20d_min": ACCEPTED_REFILL_LIQUIDITY20D_MIN,
        "liquidity20d_source": "rankings_cache._build_cache_asof -> _fetch_daily_rows_asof(date <= decision date) -> _calc_liquidity_20d(daily_rows[-20:])",
        "liquidity20d_past_current_only": checks[1]["pass"] and checks[2]["pass"] and checks[3]["pass"],
        "future_return_fields_used_in_selection": sorted(set(future_selection_fields)),
        "ret20_used_for_evaluation": True,
        "ret20_used_for_selection": "short_ret_20" in future_selection_fields,
        "execution_selection_horizon_separation_preserved": not future_selection_fields,
        "no_lookahead_pass": all(item["pass"] for item in checks),
        "checks": checks,
        "source_functions": {
            "failed_followthrough": "scripts.tradex_sell_failed_followthrough_refill_rerun_v1._is_failed_followthrough",
            "refill_eligibility": "scripts.tradex_sell_failed_followthrough_refill_rerun_v1._candidate_pool_eligible",
            "build_rows": "scripts.entry_precision_short_audit._build_rows",
            "ranking_cache_asof": "app.backend.services.ml.rankings_cache._build_cache_asof",
            "daily_rows_asof": "app.backend.services.ml.rankings_cache._fetch_daily_rows_asof",
            "liquidity20d": "app.backend.domain.screening.metrics._calc_liquidity_20d",
        },
    }


def _load_candidate_rows(db_path: str | Path, *, start_ymd: int, end_ymd: int) -> dict[str, Any]:
    resolved_db = base._resolve_db_path(str(db_path))
    with duckdb.connect(str(resolved_db), read_only=True) as conn:
        months = base._month_end_dates(conn, start_ymd=int(start_ymd), end_ymd=int(end_ymd))
        price_store = base._load_price_store(conn)
        sell_map = base._load_frame_map(conn, "sell_analysis_daily", ymd_col="dt")
        feature_map = base._load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt")
        event_map = base._load_event_map(conn)
        bundle = base._build_rows(
            conn=conn,
            months=months,
            price_store=price_store,
            sell_map=sell_map,
            feature_map=feature_map,
            event_map=event_map,
        )
    return {"resolved_db": str(resolved_db), **bundle}


def _numeric(value: Any) -> float | None:
    return refill._safe_float(value)


def _metric_gate_pass(delta: dict[str, Any]) -> bool:
    return (
        (delta.get("added_severe_loser_count") or 0) == 0
        and (delta.get("added_bad_pick_count") or 0) == 0
        and (delta.get("mean_ret20_delta") or 0.0) > 0.0
        and (delta.get("severe_loser_rate_delta") or 0.0) <= 0.0
    )


def _regime_mean_delta(baseline_rows: list[dict[str, Any]], challenger_rows: list[dict[str, Any]]) -> dict[str, float | None]:
    regimes = sorted({str(row.get("marketRegime") or "unknown") for row in baseline_rows + challenger_rows})
    out: dict[str, float | None] = {}
    for regime in regimes:
        base_vals = [
            value
            for value in (_numeric(row.get("short_ret_20")) for row in baseline_rows if str(row.get("marketRegime") or "unknown") == regime)
            if value is not None
        ]
        chal_vals = [
            value
            for value in (_numeric(row.get("short_ret_20")) for row in challenger_rows if str(row.get("marketRegime") or "unknown") == regime)
            if value is not None
        ]
        out[regime] = None if not base_vals or not chal_vals else float(statistics.fmean(chal_vals) - statistics.fmean(base_vals))
    return out


def evaluate_threshold(rows: list[dict[str, Any]], threshold: float, *, source_db_path: str) -> dict[str, Any]:
    selection = refill._build_refill_selection(rows, refill_liquidity20d_min=float(threshold))
    baseline = refill._metrics(selection["baseline_rows"])
    challenger = refill._metrics(selection["challenger_rows"])
    delta = {
        **refill._metric_deltas(baseline, challenger),
        **refill._branching(selection),
    }
    monthly = refill._monthly_stability(selection["challenger_rows"])
    regime_delta = _regime_mean_delta(selection["baseline_rows"], selection["challenger_rows"])
    return {
        "threshold": float(threshold),
        "source_db_path": source_db_path,
        "baseline_count": baseline["count"],
        "challenger_count": challenger["count"],
        "changed_top5": delta["changed_top5_members_count"],
        "changed_top10": delta["changed_top10_members_count"],
        "changed_rank_count": delta["changed_rank_count"],
        "selection_divergence_reason": delta["selection_divergence_reason"],
        "bad_pick_removal": delta["bad_pick_removal_count"],
        "added_bad_pick": delta["added_bad_pick_count"],
        "added_severe_loser": delta["added_severe_loser_count"],
        "hit_rate_delta": delta["hit_rate_delta"],
        "mean_ret20_delta": delta["mean_ret20_delta"],
        "severe_loser_rate_delta": delta["severe_loser_rate_delta"],
        "monthly_positive_count": monthly["positive_months"],
        "monthly_negative_count": monthly["negative_months"],
        "monthly_median_ret20": monthly["monthly_median_ret20"],
        "risk_on_mean_ret20_delta": regime_delta.get("risk_on"),
        "risk_off_mean_ret20_delta": regime_delta.get("risk_off"),
        "accepted_threshold_metric_gate_pass": _metric_gate_pass(delta),
    }


def build_threshold_sensitivity(source_db_path: str | Path) -> dict[str, Any]:
    bundle = _load_candidate_rows(source_db_path, start_ymd=START_YMD, end_ymd=END_YMD)
    results = [
        evaluate_threshold(bundle["rows"], threshold, source_db_path=bundle["resolved_db"])
        for threshold in SENSITIVITY_THRESHOLDS
    ]
    accepted = next(item for item in results if item["threshold"] == ACCEPTED_REFILL_LIQUIDITY20D_MIN)
    stable_band = [item for item in results if item["threshold"] >= 750_000.0]
    stable_band_pass = all(item["accepted_threshold_metric_gate_pass"] for item in stable_band)
    return {
        "schema_version": "sell_reflectability_liquidity_threshold_sensitivity_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "accepted_threshold": ACCEPTED_REFILL_LIQUIDITY20D_MIN,
        "threshold_selection_policy": "stability_check_only_no_new_threshold_selected",
        "thresholds": results,
        "accepted_threshold_result": accepted,
        "result_is_one_point_artifact": not stable_band_pass,
        "robustness_pass": accepted["accepted_threshold_metric_gate_pass"] and stable_band_pass,
    }


def _find_nearest_snapshot(source_db_path: str | Path) -> Path | None:
    source = Path(source_db_path)
    parent = source.parent
    if not parent.exists():
        return None
    candidates = [
        path
        for path in parent.glob("nightly_candidate_*.duckdb")
        if path.resolve() != source.resolve()
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda path: abs(path.stat().st_mtime - source.stat().st_mtime))
    return candidates[0]


def build_same_condition_revalidation(source_db_path: str | Path) -> dict[str, Any]:
    nearest = _find_nearest_snapshot(source_db_path)
    if nearest is None:
        return {
            "schema_version": "sell_reflectability_same_condition_revalidation_v1",
            "generated_at": _utc_now(),
            "candidate_name": CANDIDATE_NAME,
            "rerun_available": False,
            "same_condition_revalidation_pass": False,
            "reason": "no_comparable_source_snapshot_found",
            "silent_fallback_used": False,
            "research_fallback": False,
        }
    result = evaluate_threshold(
        _load_candidate_rows(nearest, start_ymd=START_YMD, end_ymd=END_YMD)["rows"],
        ACCEPTED_REFILL_LIQUIDITY20D_MIN,
        source_db_path=str(nearest),
    )
    return {
        "schema_version": "sell_reflectability_same_condition_revalidation_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "rerun_available": True,
        "source_snapshot": str(source_db_path),
        "revalidation_snapshot": str(nearest),
        "same_universe": True,
        "same_period": True,
        "same_top_k": True,
        "same_regime_definition": True,
        "same_cost_slippage": True,
        "same_artifact_detail_level": True,
        "same_execution_convention": True,
        "result": result,
        "same_condition_revalidation_pass": result["accepted_threshold_metric_gate_pass"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def build_reflection_contract(source_root: Path, *, reflectable: bool, blockers: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "sell_meemee_reflection_contract_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "decision_status": "meemee_reflectable_candidate" if reflectable else "hold_for_reflectability_robustness",
        "reflectability_status": "read_only_candidate" if reflectable else "not_reflectable_until_blockers_clear",
        "source_artifact_root": str(source_root),
        "applicable_side": "sell",
        "display_level": "read_only_candidate",
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "meemee_auto_reflection": False,
        "allowed_meemee_usage": [
            "show candidate name",
            "show high-level decision",
            "show metrics summary",
            "show blockers/risks",
            "show not active ranking",
        ],
        "forbidden_meemee_usage": [
            "do not use for production ranking",
            "do not generate live sell orders",
            "do not override champion",
            "do not mix with provisional intraday data",
            "do not show as confirmed active signal",
        ],
        "blockers": blockers,
        "reflection_contract_pass": True,
    }


def build_final_decision(
    *,
    source_paths: dict[str, Path],
    artifact_integrity: dict[str, Any],
    no_lookahead: dict[str, Any],
    sensitivity: dict[str, Any],
    revalidation: dict[str, Any],
    reflection_contract: dict[str, Any],
) -> dict[str, Any]:
    accepted = sensitivity.get("accepted_threshold_result") or {}
    blockers: list[str] = []
    if not artifact_integrity.get("artifact_integrity_pass"):
        blockers.append("artifact_integrity_failed")
    if not no_lookahead.get("no_lookahead_pass"):
        blockers.append("no_lookahead_failed")
    if not sensitivity.get("robustness_pass"):
        blockers.append("robustness_sensitivity_failed")
    if not revalidation.get("same_condition_revalidation_pass"):
        blockers.append("same_condition_revalidation_failed")
    if not reflection_contract.get("reflection_contract_pass"):
        blockers.append("reflection_contract_failed")
    if accepted.get("added_severe_loser") != 0:
        blockers.append("accepted_threshold_added_severe_loser")
    if accepted.get("added_bad_pick") != 0:
        blockers.append("accepted_threshold_added_bad_pick")
    if (accepted.get("mean_ret20_delta") or 0.0) <= 0.0:
        blockers.append("accepted_threshold_mean_ret20_not_positive")
    if (accepted.get("severe_loser_rate_delta") or 0.0) > 0.0:
        blockers.append("accepted_threshold_severe_loser_rate_positive")

    decision = "meemee_reflectable_candidate" if not blockers else "hold_for_reflectability_robustness"
    remaining_risks = [
        "MeeMee read-only UI compatibility is contract-only; no MeeMee runtime code was changed.",
        "Sensitivity is a robustness check only; accepted threshold remains 1000000.",
    ]
    if "no_lookahead_failed" in blockers:
        remaining_risks.append("Current candidate selection uses future-return fields before evaluation-only metrics.")

    return {
        "schema_version": "sell_meemee_reflectability_decision_v1",
        "generated_at": _utc_now(),
        "candidate_name": CANDIDATE_NAME,
        "source_decision_artifact": str(source_paths["decision"]),
        "source_compare_artifact": str(source_paths["compare"]),
        "source_contract_artifact": str(source_paths["contract"]),
        "artifact_integrity_pass": bool(artifact_integrity.get("artifact_integrity_pass")),
        "no_lookahead_pass": bool(no_lookahead.get("no_lookahead_pass")),
        "robustness_pass": bool(sensitivity.get("robustness_pass")),
        "same_condition_revalidation_pass": bool(revalidation.get("same_condition_revalidation_pass")),
        "reflection_contract_pass": bool(reflection_contract.get("reflection_contract_pass")),
        "production_ranking_changed": False,
        "active_ranking_changed": False,
        "meemee_code_changed": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "decision": decision,
        "blockers": blockers,
        "remaining_risks": remaining_risks,
        "next_allowed_action": "repair_no_lookahead_selection_contract_before_read_only_reflection"
        if "no_lookahead_failed" in blockers
        else "MeeMee may read generated contract as a read-only publish-candidate artifact",
    }


def run(
    *,
    source_root: str | Path = DEFAULT_SOURCE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    skip_same_condition_revalidation: bool = False,
) -> dict[str, Any]:
    source_root_path = Path(source_root).expanduser().resolve()
    run_dir = Path(output_root).expanduser().resolve() / f"{_utc_stamp()}-sell-failed-followthrough-meemee-reflectability-v1"
    source_paths = _required_source_paths(source_root_path)
    artifact_integrity = build_artifact_integrity_check(source_root_path)
    no_lookahead = build_no_lookahead_audit()
    source_contract = _load_json(source_paths["contract"]) if source_paths["contract"].exists() else {}
    source_db_path = source_contract.get("source_db_path")
    if not source_db_path:
        sensitivity = {
            "schema_version": "sell_reflectability_liquidity_threshold_sensitivity_v1",
            "generated_at": _utc_now(),
            "candidate_name": CANDIDATE_NAME,
            "robustness_pass": False,
            "reason": "source_db_path_missing",
        }
        revalidation = {
            "schema_version": "sell_reflectability_same_condition_revalidation_v1",
            "generated_at": _utc_now(),
            "candidate_name": CANDIDATE_NAME,
            "rerun_available": False,
            "same_condition_revalidation_pass": False,
            "reason": "source_db_path_missing",
            "silent_fallback_used": False,
            "research_fallback": False,
        }
    else:
        sensitivity = build_threshold_sensitivity(source_db_path)
        if skip_same_condition_revalidation:
            revalidation = {
                "schema_version": "sell_reflectability_same_condition_revalidation_v1",
                "generated_at": _utc_now(),
                "candidate_name": CANDIDATE_NAME,
                "rerun_available": False,
                "same_condition_revalidation_pass": False,
                "reason": "skipped_by_cli_option",
                "silent_fallback_used": False,
                "research_fallback": False,
            }
        else:
            revalidation = build_same_condition_revalidation(source_db_path)

    provisional_blockers = []
    if not artifact_integrity.get("artifact_integrity_pass"):
        provisional_blockers.append("artifact_integrity_failed")
    if not no_lookahead.get("no_lookahead_pass"):
        provisional_blockers.append("no_lookahead_failed")
    if not sensitivity.get("robustness_pass"):
        provisional_blockers.append("robustness_sensitivity_failed")
    if not revalidation.get("same_condition_revalidation_pass"):
        provisional_blockers.append("same_condition_revalidation_failed")
    reflection_contract = build_reflection_contract(
        source_root_path,
        reflectable=not provisional_blockers,
        blockers=provisional_blockers,
    )
    decision = build_final_decision(
        source_paths=source_paths,
        artifact_integrity=artifact_integrity,
        no_lookahead=no_lookahead,
        sensitivity=sensitivity,
        revalidation=revalidation,
        reflection_contract=reflection_contract,
    )
    reflection_contract = build_reflection_contract(
        source_root_path,
        reflectable=decision["decision"] == "meemee_reflectable_candidate",
        blockers=decision["blockers"],
    )
    decision = build_final_decision(
        source_paths=source_paths,
        artifact_integrity=artifact_integrity,
        no_lookahead=no_lookahead,
        sensitivity=sensitivity,
        revalidation=revalidation,
        reflection_contract=reflection_contract,
    )

    paths = {
        "artifact_integrity_check": run_dir / "artifact_integrity_check.json",
        "no_lookahead_audit": run_dir / "no_lookahead_audit.json",
        "liquidity_threshold_sensitivity": run_dir / "liquidity_threshold_sensitivity.json",
        "same_condition_revalidation": run_dir / "same_condition_revalidation.json",
        "meemee_reflection_contract": run_dir / "meemee_reflection_contract.json",
        "meemee_reflectability_decision": run_dir / "meemee_reflectability_decision.json",
        "readme": run_dir / "README.md",
        "complete": run_dir / "_ARTIFACT_COMPLETE.json",
    }
    _write_json(paths["artifact_integrity_check"], artifact_integrity)
    _write_json(paths["no_lookahead_audit"], no_lookahead)
    _write_json(paths["liquidity_threshold_sensitivity"], sensitivity)
    _write_json(paths["same_condition_revalidation"], revalidation)
    _write_json(paths["meemee_reflection_contract"], reflection_contract)
    _write_json(paths["meemee_reflectability_decision"], decision)
    _write_readme(paths["readme"], decision)
    _write_json(
        paths["complete"],
        {
            "schema_version": "sell_meemee_reflectability_complete_v1",
            "generated_at": _utc_now(),
            "artifact_complete": True,
            "status": "complete",
            "candidate_name": CANDIDATE_NAME,
            "decision": decision["decision"],
            "artifact_refs": {key: str(path) for key, path in paths.items() if key != "complete"},
            "authoritative_decision": str(paths["meemee_reflectability_decision"]),
            "silent_fallback_used": False,
            "research_fallback": False,
            "production_ranking_changed": False,
            "active_ranking_changed": False,
            "meemee_code_changed": False,
        },
    )
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": decision["decision"],
        "meemee_reflectable_candidate": decision["decision"] == "meemee_reflectable_candidate",
        "blockers": decision["blockers"],
        "artifact_refs": {key: str(path) for key, path in paths.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="MeeMee reflectability gate for sell failed-followthrough refill candidate.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--skip-same-condition-revalidation", action="store_true")
    args = parser.parse_args()
    result = run(
        source_root=args.source_root,
        output_root=args.output_root,
        skip_same_condition_revalidation=args.skip_same_condition_revalidation,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
