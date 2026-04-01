from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.services.analysis.toredex_policy_diagnosis_service import run_toredex_policy_diagnosis
from app.backend.services.tradex_experiment_store import tradex_reports_root, write_json
from app.core.config import config
from app.db.session import try_get_conn_for_path
from external_analysis.event_image_dataset.analysis import build_event_image_rebound_live_monitor
from external_analysis.image_rerank.artifacts import read_json


REBOUND_FULL_VALIDATION_SCHEMA_VERSION = "tradex_rebound_full_validation_v1"
_LOCK_PROCESS_MARKERS = (
    "run_rebound_monitor",
    "run_toredex_policy_diagnosis",
    "run_rebound_full_validation",
    "event-image-dataset-rebound-monitor-run",
    "event-image-dataset-rebound-v3-run",
    "ranking_backtest",
    "meemeescreener",
    "stocks.duckdb",
)


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _report_root(run_id: str) -> Path:
    root = tradex_reports_root() / "rebound_full_validation" / str(run_id)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _render_markdown(payload: dict[str, Any]) -> str:
    monitor = payload["monitor_summary"]
    diagnosis = payload["diagnosis_summary"]
    lock_check = payload["lock_check"]
    lines = [
        "# TRADEX Rebound Full Validation",
        "",
        "## Summary",
        f"- dataset_id: `{payload['dataset_id']}`",
        f"- decision: `{payload['decision']}`",
        f"- recommended_policy: `{payload['recommended_policy']}`",
        f"- wall_clock_seconds: `{payload['wall_clock_seconds']}`",
        "",
        "## Lock Check",
        f"- status: `{lock_check['status']}`",
        f"- db_path: `{lock_check['db_path']}`",
        f"- blocking_processes: `{len(lock_check['blocking_processes'])}`",
        "",
        "## Monitor",
        f"- baseline_variant: `{monitor['baseline_variant']}`",
        f"- candidate_variant: `{monitor['candidate_variant']}`",
        f"- baseline_days_with_bonus: `{monitor['baseline_days_with_bonus']}`",
        f"- candidate_days_with_bonus: `{monitor['candidate_days_with_bonus']}`",
        f"- days_with_bonus_delta_vs_baseline: `{monitor['days_with_bonus_delta_vs_baseline']}`",
        f"- max_entry_rank_changed_count: `{monitor['max_entry_rank_changed_count']}`",
        "",
        "## Diagnosis",
        f"- primary_failure_axis: `{diagnosis['primary_failure_axis']}`",
        f"- base_current.total_return_pct: `{diagnosis['base_current']['total_return_pct']}`",
        f"- holdings_1.total_return_pct: `{diagnosis['holdings_1']['total_return_pct']}`",
        f"- holdings_2.total_return_pct: `{diagnosis['holdings_2']['total_return_pct']}`",
        f"- turnover_tight.total_return_pct: `{diagnosis['turnover_tight']['total_return_pct']}`",
        f"- gate_disabled_for_diagnosis.total_return_pct: `{diagnosis['gate_disabled_for_diagnosis']['total_return_pct']}`",
        "",
        "## Decision Reason",
        f"- monitor: `{payload['decision_reason']['monitor']}`",
        f"- diagnosis: `{payload['decision_reason']['diagnosis']}`",
    ]
    return "\n".join(lines).strip() + "\n"


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_render_markdown(payload), encoding="utf-8")


def _extract_run_id(result: dict[str, Any], artifact_path: Path) -> str:
    run_id = str(result.get("run_id") or "").strip()
    if run_id:
        return run_id
    return artifact_path.parent.name


def _list_blocking_processes(db_path: Path) -> list[dict[str, Any]]:
    normalized = str(db_path.resolve()).lower()
    basename = db_path.name.lower()
    current_pid = int(os.getpid())
    command = (
        "Get-CimInstance Win32_Process | "
        "Where-Object { $_.Name -match '^(python|pythonw|pwsh|powershell|MeeMeeScreener)(\\.exe)?$' } | "
        "Select-Object ProcessId,Name,CommandLine | ConvertTo-Json -Compress -Depth 3"
    )
    try:
        raw = subprocess.check_output(["powershell", "-NoProfile", "-Command", command], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return []
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    rows = payload if isinstance(payload, list) else [payload]
    blocking: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = int(row.get("ProcessId") or 0)
        if pid <= 0 or pid == current_pid:
            continue
        name = str(row.get("Name") or "")
        command_line = str(row.get("CommandLine") or "")
        haystack = f"{name} {command_line}".lower()
        if normalized not in haystack and basename not in haystack and not any(marker in haystack for marker in _LOCK_PROCESS_MARKERS):
            continue
        blocking.append(
            {
                "pid": pid,
                "name": name,
                "command_line": command_line,
            }
        )
    return blocking


def _check_target_db_lock(db_path: Path) -> dict[str, Any]:
    resolved = db_path.expanduser().resolve()
    blocking_processes = _list_blocking_processes(resolved)
    with try_get_conn_for_path(str(resolved), timeout_sec=0.0, read_only=False) as conn:
        if conn is not None:
            return {
                "status": "ok",
                "db_path": str(resolved),
                "blocking_processes": [],
            }
    return {
        "status": "blocked" if blocking_processes else "blocked_unknown",
        "db_path": str(resolved),
        "blocking_processes": blocking_processes,
    }


def _metric_delta(base_value: float | None, candidate_value: float | None) -> float:
    if base_value is None or candidate_value is None:
        return 0.0
    return float(candidate_value) - float(base_value)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _diagnosis_variant_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("variant_results")
    if not isinstance(rows, list):
        raise RuntimeError("toredex policy diagnosis payload missing variant_results")
    mapped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, dict) and str(row.get("variant_name") or "").strip():
            mapped[str(row["variant_name"])] = row
    required = {"base_current", "holdings_1", "holdings_2", "turnover_tight", "gate_disabled_for_diagnosis"}
    missing = sorted(required - set(mapped))
    if missing:
        raise RuntimeError(f"toredex policy diagnosis payload missing variants: {', '.join(missing)}")
    return mapped


def _build_monitor_summary(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise RuntimeError("rebound live monitor payload missing summary")
    baseline = summary.get("baseline")
    candidate = summary.get("candidate")
    comparison = summary.get("comparison")
    if not isinstance(baseline, dict) or not isinstance(candidate, dict) or not isinstance(comparison, dict):
        raise RuntimeError("rebound live monitor payload missing baseline/candidate/comparison summary")
    return {
        "baseline_variant": str(payload.get("baseline_variant") or "baseline_live"),
        "candidate_variant": str(payload.get("candidate_variant") or ""),
        "baseline_days_with_bonus": int(baseline.get("days_with_bonus") or 0),
        "candidate_days_with_bonus": int(candidate.get("days_with_bonus") or 0),
        "days_with_bonus_delta_vs_baseline": int(comparison.get("days_with_bonus_delta_vs_baseline") or 0),
        "median_bonus_candidate_count": float(candidate.get("median_bonus_candidate_count") or 0.0),
        "median_entry_rank_changed_count": float(candidate.get("median_entry_rank_changed_count") or 0.0),
        "max_entry_rank_changed_count": int(candidate.get("max_entry_rank_changed_count") or 0),
    }


def _build_diagnosis_summary(payload: dict[str, Any]) -> dict[str, Any]:
    variant_map = _diagnosis_variant_map(payload)

    def _row(variant_name: str) -> dict[str, float | None]:
        row = variant_map[variant_name]
        return {
            "total_return_pct": _coerce_float(row.get("total_return_pct")),
            "max_drawdown_pct": _coerce_float(row.get("max_drawdown_pct")),
            "worst_month_pct": _coerce_float(row.get("worst_month_pct")),
        }

    return {
        "primary_failure_axis": str(payload.get("primary_failure_axis") or ""),
        "base_current": _row("base_current"),
        "holdings_1": _row("holdings_1"),
        "holdings_2": _row("holdings_2"),
        "turnover_tight": _row("turnover_tight"),
        "gate_disabled_for_diagnosis": _row("gate_disabled_for_diagnosis"),
    }


def _holdings_is_primary_fix(diagnosis_summary: dict[str, Any]) -> bool:
    if str(diagnosis_summary.get("primary_failure_axis") or "") != "holdings":
        return False
    base_return = _coerce_float(diagnosis_summary["base_current"]["total_return_pct"])
    holdings_best = max(
        _metric_delta(base_return, _coerce_float(diagnosis_summary["holdings_1"]["total_return_pct"])),
        _metric_delta(base_return, _coerce_float(diagnosis_summary["holdings_2"]["total_return_pct"])),
    )
    other_best = max(
        _metric_delta(base_return, _coerce_float(diagnosis_summary["turnover_tight"]["total_return_pct"])),
        _metric_delta(base_return, _coerce_float(diagnosis_summary["gate_disabled_for_diagnosis"]["total_return_pct"])),
    )
    return float(holdings_best) > float(other_best) + 1.0


def _decide_validation_outcome(
    *,
    monitor_summary: dict[str, Any],
    diagnosis_summary: dict[str, Any],
) -> dict[str, Any]:
    candidate_variant = str(monitor_summary["candidate_variant"])
    delta = int(monitor_summary["days_with_bonus_delta_vs_baseline"])
    max_rank_change = int(monitor_summary["max_entry_rank_changed_count"])
    median_rank_change = float(monitor_summary["median_entry_rank_changed_count"])
    holdings_primary = _holdings_is_primary_fix(diagnosis_summary)

    if (
        candidate_variant == "soft_bonus_only"
        and delta > 0
        and max_rank_change <= 10
        and not holdings_primary
    ):
        return {
            "decision": "adopt_soft_bonus_only",
            "recommended_policy": "soft_bonus_only",
            "decision_reason": {
                "monitor": "soft_bonus_only increased bonus-active days over baseline while keeping rank changes bounded.",
                "diagnosis": f"primary_failure_axis={diagnosis_summary['primary_failure_axis']} does not block ranking policy adoption first.",
            },
        }
    if holdings_primary:
        return {
            "decision": "fix_holdings_before_policy_change",
            "recommended_policy": "holdings_fix_first",
            "decision_reason": {
                "monitor": f"candidate monitor delta={delta} and max_entry_rank_changed_count={max_rank_change} do not justify policy-first rollout.",
                "diagnosis": "holdings variants improve total return more than turnover/gate variants, so holdings is the primary failure axis.",
            },
        }
    return {
        "decision": "keep_tag_centered",
        "recommended_policy": "tag_only_keep_bonus_as_is",
        "decision_reason": {
            "monitor": f"candidate bonus delta vs baseline stayed at {delta} with median_entry_rank_changed_count={median_rank_change}.",
            "diagnosis": f"primary_failure_axis={diagnosis_summary['primary_failure_axis']} is not dominant enough to force holdings-first, but policy gain is still insufficient.",
        },
    }


def _write_batch_metadata(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)


def run_rebound_full_validation(
    *,
    dataset_id: str = "monthly-event-meemee-registered-sample100-v12",
    monitor_days: int = 60,
    diagnosis_start_date: str | None = None,
    diagnosis_end_date: str | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    run_id = f"rebound_full_validation_{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}"
    root = output_dir.resolve() if output_dir is not None else _report_root(run_id)
    root.mkdir(parents=True, exist_ok=True)
    metadata_path = root / "batch_metadata.json"
    started_at = _utc_now_iso()
    wall_clock_start = time.perf_counter()
    metadata: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": None,
        "status": "running",
        "failed_step": None,
        "failed_reason": None,
    }
    _write_batch_metadata(metadata_path, metadata)
    current_step = "lock_check"
    lock_check: dict[str, Any] = {
        "status": "unknown",
        "db_path": str(config.DB_PATH),
        "blocking_processes": [],
    }
    try:
        lock_check = _check_target_db_lock(config.DB_PATH)
        metadata["lock_check"] = lock_check
        _write_batch_metadata(metadata_path, metadata)
        if str(lock_check["status"]) != "ok":
            raise RuntimeError(f"target DB is locked: {lock_check['status']}")

        current_step = "monitor"
        monitor_result = build_event_image_rebound_live_monitor(
            dataset_id=str(dataset_id),
            days=int(monitor_days),
            output_root=root / "monitor",
        )
        monitor_path = Path(str(monitor_result["rebound_live_monitor_path"])).resolve()
        if not monitor_path.exists():
            raise RuntimeError("monitor artifact was not generated")
        monitor_payload = read_json(monitor_path)

        current_step = "diagnosis"
        diagnosis_result = run_toredex_policy_diagnosis(
            start_date=diagnosis_start_date,
            end_date=diagnosis_end_date,
            output_dir=root / "diagnosis",
        )
        diagnosis_path = Path(str(diagnosis_result["toredex_policy_diagnosis_path"])).resolve()
        if not diagnosis_path.exists():
            raise RuntimeError("diagnosis artifact was not generated")
        diagnosis_payload = read_json(diagnosis_path)

        current_step = "summary"
        monitor_summary = _build_monitor_summary(monitor_payload)
        diagnosis_summary = _build_diagnosis_summary(diagnosis_payload)
        decision_payload = _decide_validation_outcome(
            monitor_summary=monitor_summary,
            diagnosis_summary=diagnosis_summary,
        )
        finished_at = _utc_now_iso()
        summary_payload = {
            "schema_version": REBOUND_FULL_VALIDATION_SCHEMA_VERSION,
            "dataset_id": str(dataset_id),
            "monitor_artifact_path": str(monitor_path),
            "diagnosis_artifact_path": str(diagnosis_path),
            "period": {
                "monitor_days": int(monitor_days),
                "diagnosis_start_date": str(diagnosis_payload["period"]["start_date"]),
                "diagnosis_end_date": str(diagnosis_payload["period"]["end_date"]),
            },
            "monitor_run_id": _extract_run_id(monitor_result, monitor_path),
            "diagnosis_run_id": _extract_run_id(diagnosis_result, diagnosis_path),
            "wall_clock_seconds": round(time.perf_counter() - wall_clock_start, 3),
            "lock_check": lock_check,
            "monitor_summary": monitor_summary,
            "diagnosis_summary": diagnosis_summary,
            "decision": decision_payload["decision"],
            "decision_reason": decision_payload["decision_reason"],
            "recommended_policy": decision_payload["recommended_policy"],
            "generated_at": finished_at,
        }
        summary_json_path = root / "rebound_full_validation_summary.json"
        summary_md_path = root / "rebound_full_validation_summary.md"
        write_json(summary_json_path, summary_payload)
        _write_markdown(summary_md_path, summary_payload)

        metadata.update(
            {
                "finished_at": finished_at,
                "status": "completed",
                "failed_step": None,
                "failed_reason": None,
                "lock_check": lock_check,
                "summary_path": str(summary_json_path),
            }
        )
        _write_batch_metadata(metadata_path, metadata)
        return {
            "run_id": run_id,
            "rebound_full_validation_summary_path": str(summary_json_path),
            "rebound_full_validation_summary_report_path": str(summary_md_path),
            "batch_metadata_path": str(metadata_path),
            "decision": str(summary_payload["decision"]),
            "recommended_policy": str(summary_payload["recommended_policy"]),
        }
    except Exception as exc:
        metadata.update(
            {
                "finished_at": _utc_now_iso(),
                "status": "failed",
                "failed_step": current_step,
                "failed_reason": str(exc),
                "lock_check": lock_check,
            }
        )
        _write_batch_metadata(metadata_path, metadata)
        raise
