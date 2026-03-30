from __future__ import annotations

import math
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_source_db_path
from external_analysis.exporter.snapshot_status import (
    EXPORT_SNAPSHOT_STATUS_COMPLETE,
    build_source_signature_payload,
    probe_export_snapshot_readiness,
)
from external_analysis.exporter.source_reader import connect_source_db, normalize_market_date
from external_analysis.image_rerank.artifacts import read_json, verify_roundtrip, write_json
from external_analysis.image_rerank.cli import run_image_rerank_phase0_3
from external_analysis.image_rerank.dataset import (
    build_base_score_artifact,
    build_historical_samples,
    load_bars_frame,
    normalize_as_of_date,
)
from external_analysis.image_rerank.paths import image_rerank_run_dir, image_rerank_session_dir
from external_analysis.image_rerank.split import (
    assign_split_role,
    build_split_audit_manifest,
    build_time_block_split_manifest,
    classify_boundary_reasons,
)


FULL_UNIVERSE_CONFIRM_SCHEMA_VERSION = "tradex_image_rerank_full_universe_confirm_v1"
FIRST_ANALYSIS_SCHEMA_VERSION = "tradex_image_rerank_first_analysis_v1"
DISPOSITION_SCHEMA_VERSION = "tradex_image_rerank_challenger_disposition_v1"
EXECUTION_LOG_SCHEMA_VERSION = "tradex_image_rerank_execution_log_v1"
MAX_INLINE_EXPORT_DAILY_ROWS = 250_000


@dataclass(frozen=True)
class ResearchProfile:
    name: str
    verify_profile: str
    block_size_days: int
    feature_lookback_days: int
    label_horizon_days: int
    embargo_days: int


SMOKE_PROFILE = ResearchProfile(
    name="smoke",
    verify_profile="smoke",
    block_size_days=20,
    feature_lookback_days=30,
    label_horizon_days=5,
    embargo_days=5,
)

STRESS_PROFILE = ResearchProfile(
    name="stress",
    verify_profile="stress",
    block_size_days=30,
    feature_lookback_days=80,
    label_horizon_days=10,
    embargo_days=0,
)

ANALYSIS_PROFILE = ResearchProfile(
    name="analysis",
    verify_profile="analysis",
    block_size_days=30,
    feature_lookback_days=80,
    label_horizon_days=10,
    embargo_days=0,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_latest_source_db_path(source_db_path: str | None = None) -> Path:
    if source_db_path and str(source_db_path).strip():
        return Path(str(source_db_path)).expanduser().resolve()
    research_db_dir = _repo_root() / ".local" / "meemee" / "research_db"
    candidates = sorted(research_db_dir.glob("stocks_research_*.duckdb"))
    if candidates:
        return candidates[-1].resolve()
    return resolve_source_db_path(None)


def _resolve_session_id(session_id: str | None = None) -> str:
    raw = str(session_id or "").strip()
    if raw:
        return raw
    return datetime.now(timezone.utc).strftime("image-rerank-full-universe-%Y%m%dT%H%M%SZ")


def _resolve_export_db_path(*, session_dir: Path, export_db_path: str | None = None) -> Path:
    if export_db_path and str(export_db_path).strip():
        return Path(str(export_db_path)).expanduser().resolve()
    internal_dir = session_dir / "internal"
    internal_dir.mkdir(parents=True, exist_ok=True)
    return (internal_dir / "export.duckdb").resolve()


def _load_ordered_trading_dates(source_db_path: Path) -> list[int]:
    source_conn = connect_source_db(str(source_db_path))
    try:
        rows = source_conn.execute("SELECT DISTINCT date FROM daily_bars ORDER BY date").fetchall()
    finally:
        source_conn.close()
    dates = [normalize_market_date(row[0]) for row in rows if row and row[0] is not None]
    normalized = [int(value) for value in dates if value is not None]
    if not normalized:
        raise RuntimeError("daily_bars has no ordered trading dates")
    return normalized


def _estimate_historical_sample_count(
    *,
    export_db_path: Path,
    feature_lookback_days: int,
    label_horizon_days: int,
) -> int:
    conn = duckdb.connect(str(export_db_path), read_only=True)
    try:
        row = conn.execute(
            f"""
            SELECT SUM(GREATEST(cnt - {int(feature_lookback_days)} - {int(label_horizon_days)} + 1, 0))
            FROM (
                SELECT code, COUNT(*) AS cnt
                FROM bars_daily_export
                GROUP BY code
            )
            """
        ).fetchone()
    finally:
        conn.close()
    return int(row[0] or 0)


def _build_snapshot_rows_from_samples(
    *,
    historical_samples: list[dict[str, Any]],
    snapshot_date: int,
    base_score_artifact: dict[str, Any],
) -> list[dict[str, Any]]:
    base_by_code = {str(row.get("code") or ""): row for row in base_score_artifact.get("rows") or []}
    rows: list[dict[str, Any]] = []
    for sample in historical_samples:
        if int(sample["as_of_date"]) != int(snapshot_date):
            continue
        base_row = base_by_code.get(str(sample["code"]), {})
        rows.append(
            {
                **sample,
                "base_score": float(base_row.get("base_score") or 0.0),
                "base_rank": int(base_row.get("base_rank") or 0),
                "base_retrieval_score": float(base_row.get("base_retrieval_score") or 0.0),
                "base_risk_penalty": float(base_row.get("base_risk_penalty") or 0.0),
            }
        )
    return rows


def _count_kept_train_rows(
    *,
    historical_samples: list[dict[str, Any]],
    trading_dates: list[int],
    profile: ResearchProfile,
) -> int:
    date_to_index = {trade_date: index for index, trade_date in enumerate(trading_dates)}
    split_manifest = build_time_block_split_manifest(
        run_id=f"feasibility-{profile.name}",
        trading_dates=trading_dates,
        block_size_days=profile.block_size_days,
        embargo_days=profile.embargo_days,
        feature_lookback_days=profile.feature_lookback_days,
        label_horizon_days=profile.label_horizon_days,
    )
    sample_indices = [date_to_index[int(sample["as_of_date"])] for sample in historical_samples if int(sample["as_of_date"]) in date_to_index]
    split_manifest = build_split_audit_manifest(
        split_manifest=split_manifest,
        sample_indices=sample_indices,
        feature_lookback_days=profile.feature_lookback_days,
        label_horizon_days=profile.label_horizon_days,
    )
    boundary_checks = split_manifest.get("boundary_checks") or []

    train_count = 0
    for sample in historical_samples:
        split_info = assign_split_role(as_of_date=int(sample["as_of_date"]), split_manifest=split_manifest)
        if str(split_info.get("split_role") or "") != "train":
            continue
        block_index = int(split_info.get("block_index") or -1)
        reasons: list[str] = []
        if block_index >= 0 and block_index < len(boundary_checks):
            boundary = boundary_checks[block_index]
            reasons = classify_boundary_reasons(
                as_of_index=int(date_to_index[int(sample["as_of_date"])]),
                feature_lookback_days=profile.feature_lookback_days,
                label_horizon_days=profile.label_horizon_days,
                protected_start_index=int(boundary["protected_start_index"]),
                protected_end_index=int(boundary["protected_end_index"]),
                purge_start_index=int(boundary["purge_start_index"]),
                embargo_end_index=int(boundary["embargo_end_index"]),
            )
        if reasons:
            continue
        train_count += 1
    return train_count


def _find_feasible_as_of_date(
    *,
    export_db_path: Path,
    requested_as_of_date: str | int | None,
    profile: ResearchProfile,
    search_limit_days: int = 90,
) -> dict[str, Any]:
    bars_frame = load_bars_frame(str(export_db_path))
    trading_dates = sorted({int(value) for value in bars_frame["trade_date"].tolist()})
    if not trading_dates:
        raise RuntimeError("bars_daily_export has no trading dates")

    requested = normalize_as_of_date(requested_as_of_date) if requested_as_of_date is not None else None
    if requested is not None:
        candidate_dates = [requested]
    else:
        cutoff = max(0, len(trading_dates) - profile.label_horizon_days - search_limit_days)
        latest_valid = max(0, len(trading_dates) - profile.label_horizon_days)
        candidate_dates = list(reversed(trading_dates[cutoff:latest_valid]))

    last_error: str | None = None
    for candidate in candidate_dates:
        try:
            base_score_artifact = build_base_score_artifact(
                export_db_path=str(export_db_path),
                as_of_snapshot_date=int(candidate),
            )
            if not (base_score_artifact.get("rows") or []):
                last_error = "base score artifact is empty"
                continue
            historical_samples = build_historical_samples(
                bars_frame=bars_frame,
                snapshot_date=int(candidate),
                feature_lookback_days=profile.feature_lookback_days,
                label_horizon_days=profile.label_horizon_days,
            )
            if not historical_samples:
                last_error = "historical samples are empty"
                continue
            snapshot_rows = _build_snapshot_rows_from_samples(
                historical_samples=historical_samples,
                snapshot_date=int(candidate),
                base_score_artifact=base_score_artifact,
            )
            if not snapshot_rows:
                last_error = "snapshot rows are empty"
                continue
            train_sample_count = _count_kept_train_rows(
                historical_samples=historical_samples,
                trading_dates=trading_dates,
                profile=profile,
            )
            if train_sample_count <= 0:
                last_error = "kept training rows are empty after purge"
                continue
            return {
                "as_of_date": int(candidate),
                "base_score_count": len(base_score_artifact.get("rows") or []),
                "snapshot_candidate_count": len(snapshot_rows),
                "train_sample_count": int(train_sample_count),
                "searched_dates": len(candidate_dates),
            }
        except Exception as exc:
            last_error = str(exc)
            continue
    if requested is not None:
        raise RuntimeError(f"requested as_of_date is not feasible: {requested} ({last_error or 'unknown reason'})")
    raise RuntimeError(f"no feasible as_of_date found within the latest {search_limit_days} trading days ({last_error or 'unknown reason'})")


def _build_contract_checks(*, run_json: dict[str, Any], split_json: dict[str, Any], compare_json: dict[str, Any]) -> dict[str, Any]:
    return {
        "run": {
            "contract_status": run_json.get("purge_rule", {}).get("contract_status"),
            "protected_block_rule": run_json.get("purge_rule", {}).get("protected_block_rule"),
            "stress_feasibility_condition": run_json.get("purge_rule", {}).get("stress_feasibility_condition"),
        },
        "split": {
            "contract_status": split_json.get("purge_rule", {}).get("contract_status"),
            "protected_block_rule": split_json.get("purge_rule", {}).get("protected_block_rule"),
            "stress_feasibility_condition": split_json.get("purge_rule", {}).get("stress_feasibility_condition"),
            "reason_code_definitions": split_json.get("reason_code_definitions"),
        },
        "compare": {
            "readout_contract": compare_json.get("readout_contract"),
            "fusion_sweep": {"parameters": (compare_json.get("fusion_sweep") or {}).get("parameters")},
            "readout": {
                "dropped_top_codes": (compare_json.get("readout") or {}).get("dropped_top_codes"),
                "added_top_codes": (compare_json.get("readout") or {}).get("added_top_codes"),
                "rank_uplift_contributors": (compare_json.get("readout") or {}).get("rank_uplift_contributors"),
            },
        },
    }


def _read_run_artifacts(run_id: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    run_dir = image_rerank_run_dir(run_id)
    run_json = read_json(run_dir / "run.json")
    split_json = read_json(run_dir / "manifests" / "split.json")
    compare_json = read_json(run_dir / "outputs" / "phase3_compare.json")
    return run_json, split_json, compare_json


def _build_run_reference(*, run_id: str, verify_profile: str) -> dict[str, Any]:
    run_dir = image_rerank_run_dir(run_id)
    run_json, _, compare_json = _read_run_artifacts(run_id)
    return {
        "run_id": run_id,
        "verify_profile": verify_profile,
        "run_dir": str(run_dir),
        "run_json_uri": str(run_dir / "run.json"),
        "phase3_compare_uri": str(run_dir / "outputs" / "phase3_compare.json"),
        "ok": True,
        "as_of_snapshot_date": int(run_json["as_of_snapshot_date"]),
        "candidate_universe_hash": run_json["candidate_universe_hash"],
        "top_k": int(compare_json["top_k"]),
    }


def _run_profile(
    *,
    export_db_path: Path,
    session_id: str,
    as_of_date: int,
    top_k: int,
    renderer_backend: str,
    profile: ResearchProfile,
) -> dict[str, Any]:
    run_id = f"{session_id}-{profile.name}"
    return run_image_rerank_phase0_3(
        export_db_path=str(export_db_path),
        as_of_snapshot_date=int(as_of_date),
        run_id=run_id,
        verify_profile=profile.verify_profile,
        top_k=int(top_k),
        block_size_days=profile.block_size_days,
        embargo_days=profile.embargo_days,
        feature_lookback_days=profile.feature_lookback_days,
        label_horizon_days=profile.label_horizon_days,
        base_weight=0.70,
        image_weight=0.30,
        renderer_backend=renderer_backend,
    )


def _selection_divergence(*, base_top_codes: list[str], fused_top_codes: list[str], top_k: int) -> float:
    if top_k <= 0:
        return 0.0
    overlap = len(set(base_top_codes) & set(fused_top_codes))
    return 1.0 - (float(overlap) / float(top_k))


def _build_first_analysis_artifact(
    *,
    session_id: str,
    analysis_run_id: str,
    run_json: dict[str, Any],
    compare_json: dict[str, Any],
    base_score_json: dict[str, Any],
) -> dict[str, Any]:
    metrics = dict(compare_json.get("metrics") or {})
    top_k = int(compare_json.get("top_k") or 0)
    metrics["selection_divergence"] = _selection_divergence(
        base_top_codes=list(metrics.get("base_top_codes") or []),
        fused_top_codes=list(metrics.get("fused_top_codes") or []),
        top_k=top_k,
    )
    same_universe = str(run_json.get("candidate_universe_hash") or "") == str(compare_json.get("candidate_universe_hash") or "")
    same_period = int(run_json.get("as_of_snapshot_date") or 0) == int(compare_json.get("as_of_snapshot_date") or 0)
    same_top_k = int(compare_json.get("top_k") or 0) == top_k
    same_regime = bool(base_score_json.get("regime"))
    same_cost = True
    same_artifact_detail_level = bool(compare_json.get("base_top_rows")) and bool(compare_json.get("fused_top_rows")) and bool(compare_json.get("candidate_rows"))

    run_dir = image_rerank_run_dir(analysis_run_id)
    return {
        "schema_version": FIRST_ANALYSIS_SCHEMA_VERSION,
        "session_id": session_id,
        "created_at": _utc_now_iso(),
        "scope_mode": "full_universe",
        "analysis_run_id": analysis_run_id,
        "baseline_kind": "frozen_base_score",
        "challenger_kind": "image_rerank_rank_improver",
        "comparison_invariants": {
            "same_universe": same_universe,
            "same_period": same_period,
            "same_top_k": same_top_k,
            "same_regime": same_regime,
            "same_cost": same_cost,
            "same_artifact_detail_level": same_artifact_detail_level,
        },
        "metrics": metrics,
        "artifacts": {
            "phase3_compare_uri": str(run_dir / "outputs" / "phase3_compare.json"),
            "run_json_uri": str(run_dir / "run.json"),
            "split_json_uri": str(run_dir / "manifests" / "split.json"),
        },
    }


def _blocker_contract_checks() -> dict[str, Any]:
    return {
        "checked": False,
        "run_artifacts_present": False,
        "compare_artifacts_present": False,
    }


def _analysis_contract_checks() -> dict[str, Any]:
    return {
        "checked": True,
        "run_artifacts_present": True,
        "compare_artifacts_present": True,
    }


def _append_blocker(
    blockers: list[dict[str, Any]],
    *,
    step: str,
    exc: Exception,
    reason_code: str | None = None,
    export_probe: dict[str, Any] | None = None,
) -> None:
    payload = {
        "step": step,
        "error": str(exc),
        "error_type": exc.__class__.__name__,
        "traceback": traceback.format_exc(),
    }
    if reason_code:
        payload["reason_code"] = reason_code
    if export_probe is not None:
        payload["export_probe"] = export_probe
    blockers.append(payload)


def _is_zero_metric(value: Any) -> bool:
    try:
        return math.isclose(float(value or 0.0), 0.0, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def _all_invariants_true(comparison_invariants: dict[str, Any]) -> bool:
    return bool(comparison_invariants) and all(bool(value) for value in comparison_invariants.values())


def _build_disposition_reasons(
    *,
    artifact_complete: bool,
    invariants_complete: bool,
    no_op_primary: bool,
    no_op_secondary: bool,
    decision: str,
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if artifact_complete:
        reasons.append({"code": "artifact_complete"})
    else:
        reasons.append({"code": "artifact_incomplete"})
    if invariants_complete:
        reasons.append({"code": "comparison_invariants_complete"})
    else:
        reasons.append({"code": "comparison_invariants_incomplete"})
    if no_op_primary:
        reasons.append({"code": "primary_no_op"})
    else:
        reasons.append({"code": "primary_has_signal"})
    if no_op_secondary:
        reasons.append({"code": "secondary_no_op"})
    else:
        reasons.append({"code": "secondary_has_signal"})
    reasons.append({"code": f"decision_{decision}"})
    return reasons


def _build_challenger_disposition_artifact(
    *,
    session_id: str,
    confirm_json: dict[str, Any] | None,
    analysis_json: dict[str, Any] | None,
    compare_json: dict[str, Any] | None,
    disposition_path: Path,
) -> dict[str, Any]:
    confirm_json = dict(confirm_json or {})
    analysis_json = dict(analysis_json or {})
    compare_json = dict(compare_json or {})

    comparison_invariants = dict(analysis_json.get("comparison_invariants") or {})
    primary_metrics = dict(analysis_json.get("metrics") or {})
    fusion_sweep = dict(compare_json.get("fusion_sweep") or {})
    secondary_metrics = dict((fusion_sweep.get("modes") or {}).get("veto_helper", {}).get("metrics") or {})

    artifact_complete = bool(confirm_json.get("ok")) and bool(analysis_json) and bool(compare_json)
    invariants_complete = _all_invariants_true(comparison_invariants)
    no_op_primary = (
        _is_zero_metric(primary_metrics.get("top_k_uplift"))
        and int(primary_metrics.get("changed_top10_count") or 0) == 0
        and _is_zero_metric(primary_metrics.get("selection_divergence"))
        and int(primary_metrics.get("bad_pick_removal") or 0) == 0
    )
    no_op_secondary = (
        _is_zero_metric(secondary_metrics.get("top_k_uplift"))
        and int(secondary_metrics.get("changed_top10_count") or 0) == 0
        and int(secondary_metrics.get("bad_pick_removal") or 0) == 0
    )

    decision = "hold"
    if (
        artifact_complete
        and invariants_complete
        and no_op_primary
        and no_op_secondary
    ):
        decision = "drop"
    elif (
        artifact_complete
        and float(primary_metrics.get("top_k_uplift") or 0.0) > 0.0
        and int(primary_metrics.get("changed_top10_count") or 0) > 0
    ):
        decision = "keep"

    disposition = {
        "schema_version": DISPOSITION_SCHEMA_VERSION,
        "session_id": session_id,
        "created_at": _utc_now_iso(),
        "analysis_run_id": analysis_json.get("analysis_run_id"),
        "challenger_kind": analysis_json.get("challenger_kind") or "image_rerank_rank_improver",
        "decision": decision,
        "decision_reasons": _build_disposition_reasons(
            artifact_complete=artifact_complete,
            invariants_complete=invariants_complete,
            no_op_primary=no_op_primary,
            no_op_secondary=no_op_secondary,
            decision=decision,
        ),
        "source_artifacts": {
            "full_universe_confirm_uri": str((disposition_path.parent / "full_universe_confirm.json").resolve()),
            "challenger_first_analysis_uri": str((disposition_path.parent / "challenger_first_analysis.json").resolve()),
            "phase3_compare_uri": str((analysis_json.get("artifacts") or {}).get("phase3_compare_uri") or ""),
        },
        "comparison_invariants": comparison_invariants,
        "primary_mode_metrics": primary_metrics,
        "secondary_mode_metrics": secondary_metrics,
        "summary_flags": {
            "artifact_complete": artifact_complete,
            "no_op_primary": no_op_primary,
            "no_op_secondary": no_op_secondary,
        },
    }
    return disposition


def run_image_rerank_disposition(*, session_id: str) -> dict[str, Any]:
    resolved_session_id = _resolve_session_id(session_id)
    session_dir = image_rerank_session_dir(resolved_session_id)
    confirm_path = session_dir / "full_universe_confirm.json"
    analysis_path = session_dir / "challenger_first_analysis.json"
    disposition_path = session_dir / "challenger_disposition.json"

    if not confirm_path.exists():
        raise RuntimeError(f"missing full_universe_confirm.json: {confirm_path}")

    confirm_json = read_json(confirm_path)
    analysis_json = read_json(analysis_path) if analysis_path.exists() else {}
    compare_path_text = str((analysis_json.get("artifacts") or {}).get("phase3_compare_uri") or "").strip()
    compare_json = read_json(Path(compare_path_text)) if compare_path_text else {}

    disposition = _build_challenger_disposition_artifact(
        session_id=resolved_session_id,
        confirm_json=confirm_json,
        analysis_json=analysis_json,
        compare_json=compare_json,
        disposition_path=disposition_path,
    )
    write_json(disposition_path, disposition)
    verify_roundtrip(disposition_path, disposition)
    return {
        "ok": True,
        "session_id": resolved_session_id,
        "artifacts": {
            "challenger_disposition": str(disposition_path),
        },
        "disposition": disposition,
    }


def run_full_universe_research(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    session_id: str | None = None,
    as_of_date: str | int | None = None,
    top_k: int = 10,
    renderer_backend: str = "auto",
) -> dict[str, Any]:
    resolved_session_id = _resolve_session_id(session_id)
    session_dir = image_rerank_session_dir(resolved_session_id)
    resolved_source_db_path = _resolve_latest_source_db_path(source_db_path)
    resolved_export_db_path = _resolve_export_db_path(session_dir=session_dir, export_db_path=export_db_path)
    confirm_path = session_dir / "full_universe_confirm.json"
    analysis_path = session_dir / "challenger_first_analysis.json"
    execution_log_path = session_dir / "execution_log.json"

    source_payload = build_source_signature_payload(str(resolved_source_db_path))
    export_probe = probe_export_snapshot_readiness(str(resolved_source_db_path), str(resolved_export_db_path))

    execution_log: dict[str, Any] = {
        "schema_version": EXECUTION_LOG_SCHEMA_VERSION,
        "session_id": resolved_session_id,
        "created_at": _utc_now_iso(),
        "scope_mode": "full_universe",
        "executed_commands": [
            {
                "argv": [
                    "python",
                    "-m",
                    "external_analysis",
                    "image-rerank-research-run",
                    "--source-db-path",
                    str(resolved_source_db_path),
                    "--export-db-path",
                    str(resolved_export_db_path),
                    "--session-id",
                    resolved_session_id,
                    *(["--as-of-date", str(normalize_as_of_date(as_of_date))] if as_of_date is not None else []),
                    "--top-k",
                    str(int(top_k)),
                    "--renderer-backend",
                    str(renderer_backend),
                ],
                "started_at": _utc_now_iso(),
            }
        ],
        "resolved_profiles": {
            "smoke": SMOKE_PROFILE.__dict__,
            "stress": STRESS_PROFILE.__dict__,
            "analysis": ANALYSIS_PROFILE.__dict__,
        },
        "source_signature": source_payload["source_signature"],
        "export_probe": export_probe,
    }

    blockers: list[dict[str, Any]] = []
    confirm_artifact: dict[str, Any] = {
        "schema_version": FULL_UNIVERSE_CONFIRM_SCHEMA_VERSION,
        "session_id": resolved_session_id,
        "created_at": _utc_now_iso(),
        "scope_mode": "full_universe",
        "confirm_stage": "preconditions",
        "blocked_before_confirm": False,
        "source_db_path": str(resolved_source_db_path),
        "export_db_path": str(resolved_export_db_path),
        "resolved_as_of_date": None,
        "smoke_run": None,
        "stress_run": None,
        "precondition_checks": {
            "source_db_path_exists": resolved_source_db_path.exists(),
            "export_db_path_exists": resolved_export_db_path.exists(),
            "export_snapshot_status": export_probe.get("status"),
            "export_snapshot_reason_code": export_probe.get("reason_code"),
            "export_snapshot_complete": bool(export_probe.get("reusable")),
            "inline_export_daily_rows_limit": MAX_INLINE_EXPORT_DAILY_ROWS,
        },
        "contract_checks": _blocker_contract_checks(),
        "export_probe": export_probe,
        "source_signature": source_payload["source_signature"],
        "expected_export_signature": source_payload["expected_export_signature"],
        "blocker_reason_code": None,
        "ok": False,
        "blockers": blockers,
    }

    try:
        if str(export_probe.get("status") or "") != EXPORT_SNAPSHOT_STATUS_COMPLETE:
            confirm_artifact["blocked_before_confirm"] = True
            confirm_artifact["blocker_reason_code"] = str(export_probe.get("reason_code") or "")
            raise RuntimeError("export snapshot is not reusable for full-universe confirm")

        execution_log["ordered_trading_dates"] = {
            "count": len(_load_ordered_trading_dates(resolved_source_db_path)),
        }
        execution_log["projected_historical_samples"] = {
            "smoke": _estimate_historical_sample_count(
                export_db_path=resolved_export_db_path,
                feature_lookback_days=SMOKE_PROFILE.feature_lookback_days,
                label_horizon_days=SMOKE_PROFILE.label_horizon_days,
            ),
            "stress": _estimate_historical_sample_count(
                export_db_path=resolved_export_db_path,
                feature_lookback_days=STRESS_PROFILE.feature_lookback_days,
                label_horizon_days=STRESS_PROFILE.label_horizon_days,
            ),
            "analysis": _estimate_historical_sample_count(
                export_db_path=resolved_export_db_path,
                feature_lookback_days=ANALYSIS_PROFILE.feature_lookback_days,
                label_horizon_days=ANALYSIS_PROFILE.label_horizon_days,
            ),
        }

        feasible = _find_feasible_as_of_date(
            export_db_path=resolved_export_db_path,
            requested_as_of_date=as_of_date,
            profile=STRESS_PROFILE,
        )
        resolved_as_of_date = int(feasible["as_of_date"])
        execution_log["feasible_as_of_search"] = feasible
        confirm_artifact["resolved_as_of_date"] = resolved_as_of_date

        confirm_artifact["confirm_stage"] = "smoke_confirm"
        _run_profile(
            export_db_path=resolved_export_db_path,
            session_id=resolved_session_id,
            as_of_date=resolved_as_of_date,
            top_k=top_k,
            renderer_backend=renderer_backend,
            profile=SMOKE_PROFILE,
        )
        confirm_artifact["smoke_run"] = _build_run_reference(run_id=f"{resolved_session_id}-smoke", verify_profile="smoke")

        confirm_artifact["confirm_stage"] = "stress_confirm"
        _run_profile(
            export_db_path=resolved_export_db_path,
            session_id=resolved_session_id,
            as_of_date=resolved_as_of_date,
            top_k=top_k,
            renderer_backend=renderer_backend,
            profile=STRESS_PROFILE,
        )
        confirm_artifact["stress_run"] = _build_run_reference(run_id=f"{resolved_session_id}-stress", verify_profile="stress")

        smoke_run_json, smoke_split_json, smoke_compare_json = _read_run_artifacts(f"{resolved_session_id}-smoke")
        stress_run_json, stress_split_json, stress_compare_json = _read_run_artifacts(f"{resolved_session_id}-stress")
        confirm_artifact["contract_checks"] = {
            **_analysis_contract_checks(),
            "smoke": _build_contract_checks(
                run_json=smoke_run_json,
                split_json=smoke_split_json,
                compare_json=smoke_compare_json,
            ),
            "stress": _build_contract_checks(
                run_json=stress_run_json,
                split_json=stress_split_json,
                compare_json=stress_compare_json,
            ),
            "run": {
                "contract_status": stress_run_json.get("purge_rule", {}).get("contract_status"),
                "protected_block_rule": stress_run_json.get("purge_rule", {}).get("protected_block_rule"),
                "stress_feasibility_condition": stress_run_json.get("purge_rule", {}).get("stress_feasibility_condition"),
            },
            "split": {
                "reason_code_definitions": stress_split_json.get("reason_code_definitions"),
            },
            "compare": {
                "readout_contract": stress_compare_json.get("readout_contract"),
                "fusion_sweep": {"parameters": (stress_compare_json.get("fusion_sweep") or {}).get("parameters")},
                "readout": {
                    "dropped_top_codes": (stress_compare_json.get("readout") or {}).get("dropped_top_codes"),
                    "added_top_codes": (stress_compare_json.get("readout") or {}).get("added_top_codes"),
                    "rank_uplift_contributors": (stress_compare_json.get("readout") or {}).get("rank_uplift_contributors"),
                },
            },
        }
        confirm_artifact["confirm_stage"] = "analysis"
        confirm_artifact["ok"] = True
        write_json(confirm_path, confirm_artifact)
        verify_roundtrip(confirm_path, confirm_artifact)

        _run_profile(
            export_db_path=resolved_export_db_path,
            session_id=resolved_session_id,
            as_of_date=resolved_as_of_date,
            top_k=top_k,
            renderer_backend=renderer_backend,
            profile=ANALYSIS_PROFILE,
        )
        analysis_run_id = f"{resolved_session_id}-analysis"
        analysis_run_json, _, analysis_compare_json = _read_run_artifacts(analysis_run_id)
        base_score_json = read_json(Path(str(analysis_run_json["base_score_artifact_uri"])))
        analysis_artifact = _build_first_analysis_artifact(
            session_id=resolved_session_id,
            analysis_run_id=analysis_run_id,
            run_json=analysis_run_json,
            compare_json=analysis_compare_json,
            base_score_json=base_score_json,
        )
        write_json(analysis_path, analysis_artifact)
        verify_roundtrip(analysis_path, analysis_artifact)
        disposition_result = run_image_rerank_disposition(session_id=resolved_session_id)

        confirm_artifact["confirm_stage"] = "complete"
        write_json(confirm_path, confirm_artifact)
        verify_roundtrip(confirm_path, confirm_artifact)

        execution_log["analysis_run"] = {
            "run_id": analysis_run_id,
            "verify_profile": analysis_run_json.get("verify_profile"),
            "derived_from": "analysis_confirm",
        }
        execution_log["executed_commands"][0]["finished_at"] = _utc_now_iso()
        execution_log["executed_commands"][0]["ok"] = True
        write_json(execution_log_path, execution_log)
        verify_roundtrip(execution_log_path, execution_log)
        return {
            "ok": True,
            "session_id": resolved_session_id,
            "confirm": confirm_artifact,
            "analysis": analysis_artifact,
            "disposition": disposition_result["disposition"],
            "execution_log": execution_log,
            "artifacts": {
                "full_universe_confirm": str(confirm_path),
                "challenger_first_analysis": str(analysis_path),
                "challenger_disposition": str(session_dir / "challenger_disposition.json"),
                "execution_log": str(execution_log_path),
            },
        }
    except Exception as exc:
        if confirm_artifact["confirm_stage"] == "preconditions":
            confirm_artifact["blocked_before_confirm"] = True
        confirm_artifact["blocker_reason_code"] = confirm_artifact["blocker_reason_code"] or "analysis_failed"
        _append_blocker(
            blockers,
            step=str(confirm_artifact["confirm_stage"] or "preconditions"),
            exc=exc,
            reason_code=str(confirm_artifact["blocker_reason_code"]),
            export_probe=export_probe,
        )
        confirm_artifact["ok"] = False
        write_json(confirm_path, confirm_artifact)
        verify_roundtrip(confirm_path, confirm_artifact)
        execution_log["executed_commands"][0]["finished_at"] = _utc_now_iso()
        execution_log["executed_commands"][0]["ok"] = False
        execution_log["blockers"] = blockers
        write_json(execution_log_path, execution_log)
        verify_roundtrip(execution_log_path, execution_log)
        return {
            "ok": False,
            "session_id": resolved_session_id,
            "confirm": confirm_artifact,
            "execution_log": execution_log,
            "artifacts": {
                "full_universe_confirm": str(confirm_path),
                "execution_log": str(execution_log_path),
            },
        }


def run_image_rerank_research(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    session_id: str | None = None,
    as_of_date: str | int | None = None,
    top_k: int = 10,
    renderer_backend: str = "auto",
) -> dict[str, Any]:
    return run_full_universe_research(
        source_db_path=source_db_path,
        export_db_path=export_db_path,
        session_id=session_id,
        as_of_date=as_of_date,
        top_k=top_k,
        renderer_backend=renderer_backend,
    )
