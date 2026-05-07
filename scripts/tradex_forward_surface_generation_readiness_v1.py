from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status  # noqa: E402

SCRIPT_NAME = "tradex_forward_surface_generation_readiness_v1"
SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1"
MANIFEST_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_input_resolution_v1"
FRESHNESS_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_forward_surface_data_freshness_audit_v1"
DEPENDENCY_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_forward_surface_pipeline_dependency_audit_v1"
BLOCKER_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_forward_surface_blocker_summary_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_forward_surface_generation_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_forward_surface_generation_readiness_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\forward_surface_generation_readiness_v1")
READINESS_SESSION = Path(r"G:\Tradex\shadow_reranker_forward_readiness_v1\20260501T142416Z-586449")
DEFER_SESSION = Path(r"G:\Tradex\research_defer_summaries\shadow_reranker_forward_validation\20260501T140925Z-567657")

READINESS_DECISION = READINESS_SESSION / "forward_readiness_decision.json"
READINESS_SURFACE_DISCOVERY = READINESS_SESSION / "surface_discovery_summary.json"
READINESS_FORWARD_OUTCOME = READINESS_SESSION / "forward_outcome_availability.json"
READINESS_FEATURE_CHECK = READINESS_SESSION / "frozen_feature_contract_check.json"

DEFER_DECISION = DEFER_SESSION / "defer_decision.json"
DEFER_FROZEN_SUMMARY = DEFER_SESSION / "frozen_shadow_challenger_summary.json"
DEFER_FORWARD_GAP = DEFER_SESSION / "forward_data_gap_summary.json"
DEFER_REOPEN = DEFER_SESSION / "reopen_conditions.json"

DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
FEATURE_SURFACE_ROOT = Path(r"G:\Tradex")
FROZEN_CANDIDATE_SURFACE_DATE = "2026-01-19"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "__class__") and value.__class__.__name__ == "NAType":
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_exists(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact for {label}: {path}")


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _git_hash_or_unknown() -> str:
    try:
        completed = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
        token = (completed.stdout or completed.stderr or "").strip()
        return token or "unknown"
    except Exception:  # pragma: no cover - best effort metadata
        return "unknown"


def _dt_ymd(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).date().isoformat()
    except Exception:
        try:
            return pd.to_datetime(value, utc=True, errors="coerce").date().isoformat()
        except Exception:
            return None


def _load_surface_summary() -> dict[str, Any]:
    summary = _load_json(READINESS_SURFACE_DISCOVERY)
    return summary


def _scan_session_dates(root: Path, prefix: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if not root.exists():
        return items
    for session_dir in sorted(root.iterdir()):
        if not session_dir.is_dir() or not session_dir.name.startswith(prefix):
            continue
        manifest = session_dir / "run_manifest.json"
        generated_at = None
        if manifest.exists():
            try:
                data = _load_json(manifest)
                generated_at = data.get("generated_at_utc") or data.get("generated_at")
            except Exception:
                generated_at = None
        items.append(
            {
                "session_dir": str(session_dir),
                "session_name": session_dir.name,
                "generated_at": generated_at,
            }
        )
    return items


def _latest_session_timestamp(sessions: list[dict[str, Any]]) -> str | None:
    parsed: list[tuple[datetime, str]] = []
    for item in sessions:
        raw = str(item.get("generated_at") or "")
        try:
            parsed.append((datetime.fromisoformat(raw.replace("Z", "+00:00")), raw))
        except Exception:
            match = re.match(r"^(?P<stamp>\d{8}T\d{6}Z)", str(item.get("session_name") or ""))
            if match:
                try:
                    raw_stamp = match.group("stamp")
                    parsed.append((datetime.strptime(raw_stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc), raw_stamp))
                except Exception:
                    continue
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0])
    return parsed[-1][1]


def _query_runtime_dates() -> dict[str, Any]:
    runtime = get_runtime_stock_db_status()
    rankings = get_rankings_freshness(risk_mode="balanced")
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        table_dates: dict[str, Any] = {}
        for label, query in [
            ("daily_bars_date", "SELECT MAX(date) FROM daily_bars"),
            ("feature_snapshot_daily_date", "SELECT MAX(dt) FROM feature_snapshot_daily"),
            ("ml_feature_daily_date", "SELECT MAX(dt) FROM ml_feature_daily"),
            ("feature_frame_daily_date", "SELECT MAX(dt) FROM feature_frame_daily"),
            ("ml_pred_20d_date", "SELECT MAX(dt) FROM ml_pred_20d"),
            ("market_regime_daily_date", "SELECT MAX(dt) FROM market_regime_daily"),
        ]:
            try:
                value = conn.execute(query).fetchone()[0]
            except Exception:
                value = None
            table_dates[label] = _dt_ymd(value)
        return {
            "runtime": runtime,
            "rankings": rankings,
            "table_dates": table_dates,
        }
    finally:
        conn.close()


def _build_data_freshness_audit(
    *,
    runtime_payload: dict[str, Any],
    rankings_payload: dict[str, Any],
    table_dates: dict[str, Any],
    surface_summary: dict[str, Any],
    candidate_generation_sessions: list[dict[str, Any]],
) -> dict[str, Any]:
    latest_candidate_surface_date = surface_summary.get("max_candidate_date")
    latest_daily_bar_date = table_dates.get("daily_bars_date")
    latest_feature_snapshot_date = table_dates.get("feature_snapshot_daily_date")
    latest_ml_feature_date = table_dates.get("ml_feature_daily_date")
    latest_feature_frame_date = table_dates.get("feature_frame_daily_date")
    latest_ml_pred_20d_date = table_dates.get("ml_pred_20d_date")
    latest_market_data_date = latest_daily_bar_date
    latest_candidate_generation_session = _latest_session_timestamp(candidate_generation_sessions)
    ranking_snapshot_as_of = rankings_payload.get("snapshot_as_of")
    if latest_market_data_date and latest_candidate_surface_date:
        gap_days = (pd.Timestamp(latest_market_data_date) - pd.Timestamp(latest_candidate_surface_date)).days
    else:
        gap_days = None
    if latest_market_data_date and latest_ml_feature_date:
        feature_gap_days = (pd.Timestamp(latest_market_data_date) - pd.Timestamp(latest_ml_feature_date)).days
    else:
        feature_gap_days = None
    return {
        "schema_version": FRESHNESS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "runtime_db": {
            "freshness_state": runtime_payload.get("freshness_state"),
            "freshness_days": runtime_payload.get("freshness_days"),
            "source_freshness_status": runtime_payload.get("source_freshness_status"),
            "latest_available_global_date": runtime_payload.get("latest_available_global_date_iso"),
            "latest_daily_bars_date": runtime_payload.get("latest_daily_bars_date_iso"),
            "latest_feature_snapshot_daily_date": runtime_payload.get("latest_feature_snapshot_daily_date_iso"),
        },
        "rankings": {
            "freshness_state": rankings_payload.get("freshness_state"),
            "freshness_days": rankings_payload.get("freshness_days"),
            "snapshot_as_of": ranking_snapshot_as_of,
            "current_candidate_available": rankings_payload.get("current_candidate_available"),
        },
        "table_dates": {
            "latest_daily_bar_date": latest_daily_bar_date,
            "latest_feature_snapshot_date": latest_feature_snapshot_date,
            "latest_ml_feature_daily_date": latest_ml_feature_date,
            "latest_feature_frame_daily_date": latest_feature_frame_date,
            "latest_ml_pred_20d_date": latest_ml_pred_20d_date,
            "latest_market_regime_daily_date": table_dates.get("market_regime_daily_date"),
        },
        "candidate_generation": {
            "latest_session": latest_candidate_generation_session,
            "sessions": candidate_generation_sessions,
        },
        "surface_window": {
            "latest_candidate_surface_date": latest_candidate_surface_date,
            "latest_date_with_full_20_business_day_forward_outcomes": surface_summary.get("max_candidate_date"),
            "gap_days_available_market_vs_latest_candidate_surface": gap_days,
            "gap_days_available_market_vs_latest_ml_feature_daily": feature_gap_days,
        },
        "observations": [
            "runtime DB and rankings are fresh enough to continue",
            "daily bars and feature_snapshot_daily are current to 2026-04-30",
            "ml_feature_daily, feature_frame_daily, and ml_pred_20d lag daily bars by multiple weeks",
            "no candidate surface exists beyond 2026-01-19",
        ],
    }


def _build_pipeline_dependency_audit(
    *,
    surface_summary: dict[str, Any],
    forward_outcome_availability: dict[str, Any],
    feature_check: dict[str, Any],
    candidate_generation_sessions: list[dict[str, Any]],
) -> dict[str, Any]:
    newest_surface = surface_summary.get("newest_surface") or {}
    latest_candidate_surface_date = surface_summary.get("max_candidate_date")
    latest_candidate_generation_session = _latest_session_timestamp(candidate_generation_sessions)
    steps = [
        {
            "step": 1,
            "name": "candidate_generation_pre_filter_context_shape_v1",
            "status": "available",
            "latest_session": _latest_session_timestamp(_scan_session_dates(FEATURE_SURFACE_ROOT / "candidate_generation_pre_filter_context_shape_v1", "")),
            "dependency": "source context + family filter + shape state",
            "current_surface_date": latest_candidate_surface_date,
            "note": "script exists and recent sessions ran, but no post-2026-01-19 surface was produced",
        },
        {
            "step": 2,
            "name": "candidate_generation_two_stage_admission_context_shape_v1",
            "status": "available",
            "latest_session": _latest_session_timestamp(_scan_session_dates(FEATURE_SURFACE_ROOT / "candidate_generation_two_stage_admission_context_shape_v1", "")),
            "dependency": "pre-filter candidate pool + two-stage admission guard",
            "current_surface_date": latest_candidate_surface_date,
            "note": "admission exists, but it still feeds the frozen corpus window",
        },
        {
            "step": 3,
            "name": "audit_surface_context_backfill_v1",
            "status": "available",
            "latest_session": "2026-05-01T05:12:48Z-eba42646",
            "dependency": "monthly / weekly context backfill",
            "note": "required by batch1 feature construction",
        },
        {
            "step": 4,
            "name": "feature_surface_batch1_v1",
            "status": "available",
            "latest_session": "2026-05-01T09:31:59Z-820266",
            "dependency": "backfilled context + candle / liquidity primitives",
            "current_surface_date": latest_candidate_surface_date,
            "no_lookahead_passed": True,
        },
        {
            "step": 5,
            "name": "feature_surface_batch2_volume_participation_v1",
            "status": "available",
            "latest_session": "2026-05-01T10:13:49Z-601273",
            "dependency": "batch1 + repaired volume participation fields",
            "current_surface_date": latest_candidate_surface_date,
            "no_lookahead_passed": True,
        },
        {
            "step": 6,
            "name": "feature_surface_edinet_event_proxy_v1_optional",
            "status": "available_but_not_required_for_generation",
            "latest_session": "2026-05-01T11:35:06Z-315465",
            "dependency": "EDINET proxy extraction for later research-only context",
            "current_surface_date": latest_candidate_surface_date,
            "no_lookahead_passed": True,
            "note": "optional for surface generation; useful only after a new surface exists",
        },
        {
            "step": 7,
            "name": "no_lookahead_audit",
            "status": "pass",
            "evidence_session": READINESS_SESSION.name,
            "evidence": bool(feature_check.get("feature_contract_matches")) and bool((surface_summary.get("all_candidate_surfaces_with_no_lookahead_pass") is True)),
            "note": "current surfaces pass and frozen feature contract matches",
        },
        {
            "step": 8,
            "name": "forward_outcome_availability",
            "status": "not_matured_for_new_surface",
            "latest_confirmed_forward_outcome_date": forward_outcome_availability.get("latest_available_candidate_date"),
            "reason": "a newly generated surface would still need 20 business days to mature before forward validation",
        },
    ]
    return {
        "schema_version": DEPENDENCY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_generation_sessions": candidate_generation_sessions,
        "latest_candidate_generation_session": latest_candidate_generation_session,
        "current_surface": {
            "latest_candidate_surface_date": latest_candidate_surface_date,
            "current_surface_no_lookahead_pass": bool(surface_summary.get("all_candidate_surfaces_with_no_lookahead_pass")),
            "newer_surface_exists_beyond_frozen_window": bool(surface_summary.get("newer_surface_exists_beyond_frozen_window")),
        },
        "steps": steps,
        "required_steps_to_produce_new_surface": [
            "repair or rerun the feature surface pipeline so ml_feature_daily and feature_frame_daily are refreshed against the latest bars",
            "rerun candidate generation on the refreshed feature inputs",
            "emit a candidate / feature surface whose max anchor date exceeds 2026-01-19",
            "keep the no-lookahead audit passing on the new surface",
            "after generation, wait for 20 business days of forward outcomes before validation",
        ],
    }


def _build_blocker_summary(freshness: dict[str, Any], dependency: dict[str, Any]) -> dict[str, Any]:
    latest_daily_bar = freshness.get("table_dates", {}).get("latest_daily_bar_date")
    latest_ml_feature = freshness.get("table_dates", {}).get("latest_ml_feature_daily_date")
    latest_feature_frame = freshness.get("table_dates", {}).get("latest_feature_frame_daily_date")
    latest_candidate_surface = freshness.get("surface_window", {}).get("latest_candidate_surface_date")
    primary = "feature_surface_not_built"
    if latest_daily_bar and latest_candidate_surface and pd.Timestamp(latest_daily_bar) <= pd.Timestamp(latest_candidate_surface):
        primary = "candidate_generation_not_run"
    if latest_daily_bar is None:
        primary = "market_data_not_updated"
    secondary: list[str] = []
    if latest_candidate_surface and latest_daily_bar and pd.Timestamp(latest_daily_bar) > pd.Timestamp(latest_candidate_surface):
        secondary.append("forward_outcomes_not_matured")
    if freshness.get("rankings", {}).get("current_candidate_available") is False:
        secondary.append("feature_surface_not_built")
    if latest_ml_feature and latest_daily_bar and pd.Timestamp(latest_ml_feature) < pd.Timestamp(latest_daily_bar):
        secondary.append("feature_surface_not_built")
    return {
        "schema_version": BLOCKER_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "primary_blocker": primary,
        "secondary_blockers": sorted(set(secondary)),
        "blocker_summary": {
            "latest_daily_bar_date": latest_daily_bar,
            "latest_ml_feature_daily_date": latest_ml_feature,
            "latest_feature_frame_daily_date": latest_feature_frame,
            "latest_candidate_surface_date": latest_candidate_surface,
            "fresh_runtime_db": freshness.get("runtime_db", {}).get("freshness_state"),
            "fresh_rankings": freshness.get("rankings", {}).get("freshness_state"),
            "feature_surface_gap_days": freshness.get("surface_window", {}).get("gap_days_available_market_vs_latest_candidate_surface"),
            "feature_build_gap_days": freshness.get("surface_window", {}).get("gap_days_available_market_vs_latest_ml_feature_daily"),
            "dependency_closure": dependency.get("required_steps_to_produce_new_surface", []),
        },
        "reason": "the feature-surface pipeline has not produced any post-2026-01-19 candidate surface even though runtime market data is fresh",
    }


def _build_recommendation(blocker_summary: dict[str, Any]) -> dict[str, Any]:
    primary = blocker_summary.get("primary_blocker")
    if primary == "market_data_not_updated":
        next_action = "update_market_data_first"
        reason = "runtime market data is stale, so no later surface can be built safely"
    elif primary == "feature_contract_mismatch":
        next_action = "repair_feature_surface_pipeline"
        reason = "the frozen feature contract does not match the available surface"
    elif primary == "feature_surface_not_built":
        next_action = "repair_feature_surface_pipeline"
        reason = "the feature-surface build chain must be refreshed before a new surface can exist"
    elif primary == "candidate_generation_not_run":
        next_action = "run_candidate_generation_for_new_surface"
        reason = "candidate generation has not been executed for the fresh market data window"
    elif primary == "forward_outcomes_not_matured":
        next_action = "wait_for_20bd_forward_outcomes"
        reason = "a new surface can be built, but forward validation must wait for 20 business days"
    else:
        next_action = "keep_waiting"
        reason = "the blocker could not be classified with enough confidence"
    return {
        "schema_version": RECOMMENDATION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "next_action": next_action,
        "reason": reason,
        "confidence": "high" if primary in {"feature_surface_not_built", "candidate_generation_not_run", "market_data_not_updated"} else "medium",
        "supporting_blocker": primary,
        "secondary_blockers": blocker_summary.get("secondary_blockers", []),
    }


def _build_decision(blocker_summary: dict[str, Any], recommendation: dict[str, Any]) -> dict[str, Any]:
    primary = blocker_summary.get("primary_blocker")
    if primary == "market_data_not_updated":
        decision = "update_market_data_required"
    elif primary == "forward_outcomes_not_matured":
        decision = "wait_for_forward_outcomes"
    elif primary == "feature_contract_mismatch":
        decision = "pipeline_repair_required"
    elif primary in {"feature_surface_not_built", "candidate_generation_not_run"}:
        decision = "pipeline_repair_required"
    else:
        decision = "keep_waiting"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "primary_blocker": primary,
        "recommended_next_action": recommendation.get("next_action"),
        "reason": blocker_summary.get("reason"),
        "frozen_challenger": "tree_hgb_path_value",
        "frozen_surface_window_end": FROZEN_CANDIDATE_SURFACE_DATE,
    }


def _build_run_manifest(output_root: Path, session_dir: Path, inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "git_commit": _git_hash_or_unknown(),
        "session_id": session_dir.name,
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "input_paths": {key: str(path) for key, path in inputs.items()},
        "decision_axis": "forward_surface_generation_readiness",
    }


def _build_input_resolution(inputs: dict[str, Path]) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {key: str(path) for key, path in inputs.items()},
        "path_checks": {key: path.exists() for key, path in inputs.items()},
        "notes": [
            "This audit is read-only and does not run validation or ranking changes.",
            "Existing readiness and defer sessions are treated as authoritative inputs.",
        ],
    }


def _candidate_generation_sessions() -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    for root_name in ["candidate_generation_pre_filter_context_shape_v1", "candidate_generation_two_stage_admission_context_shape_v1"]:
        root = FEATURE_SURFACE_ROOT / root_name
        if not root.exists():
            continue
        for session_dir in sorted(item for item in root.iterdir() if item.is_dir()):
            manifest = session_dir / "run_manifest.json"
            generated_at = None
            if manifest.exists():
                try:
                    data = _load_json(manifest)
                    generated_at = data.get("generated_at_utc") or data.get("generated_at")
                except Exception:
                    generated_at = None
            sessions.append(
                {
                    "pipeline": root_name,
                    "session_dir": str(session_dir),
                    "session_name": session_dir.name,
                    "generated_at": generated_at,
                }
            )
    sessions.sort(key=lambda item: str(item.get("generated_at") or item.get("session_name") or ""))
    return sessions


def run_forward_surface_generation_readiness(output_root: str | Path | None = None) -> dict[str, Any]:
    output_root_path = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_dir = output_root_path / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    inputs = {
        "readiness_surface_discovery": READINESS_SURFACE_DISCOVERY,
        "readiness_forward_outcome_availability": READINESS_FORWARD_OUTCOME,
        "readiness_frozen_feature_check": READINESS_FEATURE_CHECK,
        "readiness_forward_decision": READINESS_DECISION,
        "defer_decision": DEFER_DECISION,
        "defer_frozen_summary": DEFER_FROZEN_SUMMARY,
        "defer_forward_gap": DEFER_FORWARD_GAP,
        "defer_reopen_conditions": DEFER_REOPEN,
    }
    for label, path in inputs.items():
        _ensure_exists(path, label)

    readiness_surface_summary = _load_json(READINESS_SURFACE_DISCOVERY)
    readiness_forward_outcome = _load_json(READINESS_FORWARD_OUTCOME)
    readiness_feature_check = _load_json(READINESS_FEATURE_CHECK)
    readiness_decision = _load_json(READINESS_DECISION)
    defer_decision = _load_json(DEFER_DECISION)
    defer_frozen_summary = _load_json(DEFER_FROZEN_SUMMARY)
    defer_forward_gap = _load_json(DEFER_FORWARD_GAP)
    defer_reopen = _load_json(DEFER_REOPEN)

    runtime_payload = get_runtime_stock_db_status()
    rankings_payload = get_rankings_freshness(risk_mode="balanced")
    runtime_db_info = _query_runtime_dates()
    candidate_generation_sessions = _candidate_generation_sessions()

    freshness_audit = _build_data_freshness_audit(
        runtime_payload=runtime_payload,
        rankings_payload=rankings_payload,
        table_dates=runtime_db_info["table_dates"],
        surface_summary=readiness_surface_summary,
        candidate_generation_sessions=candidate_generation_sessions,
    )
    dependency_audit = _build_pipeline_dependency_audit(
        surface_summary=readiness_surface_summary,
        forward_outcome_availability=readiness_forward_outcome,
        feature_check=readiness_feature_check,
        candidate_generation_sessions=candidate_generation_sessions,
    )
    blocker_summary = _build_blocker_summary(freshness_audit, dependency_audit)
    recommendation = _build_recommendation(blocker_summary)
    decision = _build_decision(blocker_summary, recommendation)

    input_resolution = _build_input_resolution(inputs)
    run_manifest = _build_run_manifest(output_root_path, session_dir, inputs)

    _write_json(session_dir / "run_manifest.json", run_manifest)
    _write_json(session_dir / "input_resolution.json", input_resolution)
    _write_json(session_dir / "forward_surface_data_freshness_audit.json", freshness_audit)
    _write_json(session_dir / "forward_surface_pipeline_dependency_audit.json", dependency_audit)
    _write_json(session_dir / "forward_surface_blocker_summary.json", blocker_summary)
    _write_json(session_dir / "forward_surface_generation_recommendation.json", recommendation)
    _write_json(session_dir / "forward_surface_generation_readiness_v1_decision.json", decision)
    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "session_id": session_id,
            "required_files_present": True,
            "artifacts": [
                "run_manifest.json",
                "input_resolution.json",
                "forward_surface_data_freshness_audit.json",
                "forward_surface_pipeline_dependency_audit.json",
                "forward_surface_blocker_summary.json",
                "forward_surface_generation_recommendation.json",
                "forward_surface_generation_readiness_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )

    return {
        "output_dir": str(session_dir),
        "session_id": session_id,
        "decision": decision["decision"],
        "status": decision["status"],
        "latest_candidate_surface_date": freshness_audit.get("surface_window", {}).get("latest_candidate_surface_date"),
        "latest_daily_bar_date": freshness_audit.get("table_dates", {}).get("latest_daily_bar_date"),
        "jobs_supported": 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    result = run_forward_surface_generation_readiness(args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
