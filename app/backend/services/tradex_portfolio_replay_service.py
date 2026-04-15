from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any

from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.services import tradex_research_os_store as os_store
from external_analysis.policy_replay.policy_family import build_policy_family_replay, load_policy_family_cohort, load_policy_family_replay, run_policy_family_cohort
from external_analysis.policy_replay.simulator import build_replay_change_log, build_replay_window


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def replay_root() -> Path:
    root = os_store.research_os_root() / "policy_replay"
    root.mkdir(parents=True, exist_ok=True)
    return root


def replay_runs_root() -> Path:
    root = replay_root() / "runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def replay_suites_root() -> Path:
    root = replay_root() / "suites"
    root.mkdir(parents=True, exist_ok=True)
    return root


def replay_families_root() -> Path:
    root = replay_root() / "families"
    root.mkdir(parents=True, exist_ok=True)
    return root


def replay_run_dir(run_id: str) -> Path:
    path = replay_runs_root() / _text(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def replay_suite_dir(suite_id: str) -> Path:
    path = replay_suites_root() / _text(suite_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def replay_family_dir(family_id: str) -> Path:
    path = replay_families_root() / _text(family_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_artifact(path: Path, payload: dict[str, Any]) -> Path:
    return os_store.write_json(path, payload)


def _store_window_artifacts(run_dir: Path, window: dict[str, Any]) -> None:
    _write_artifact(run_dir / "replay_run_config.json", window["run_config"])
    _write_artifact(run_dir / "replay_daily_selection_snapshot.json", {"schema_version": window["schema_version"], "items": window["daily_selection_snapshot"]})
    _write_artifact(run_dir / "replay_feature_snapshot.json", {"schema_version": window["schema_version"], "items": window["feature_snapshot"]})
    _write_artifact(run_dir / "replay_positions_timeline.json", {"schema_version": window["schema_version"], "items": window["positions_timeline"]})
    _write_artifact(run_dir / "replay_trade_ledger.json", {"schema_version": window["schema_version"], "items": window["trade_ledger"]})
    _write_artifact(run_dir / "replay_daily_equity_curve.json", {"schema_version": window["schema_version"], "items": window["daily_equity_curve"]})
    _write_artifact(run_dir / "replay_benchmark_market.json", window["benchmark_market"])
    _write_artifact(run_dir / "replay_benchmark_universe.json", window["benchmark_universe"])
    _write_artifact(run_dir / "replay_relative_performance.json", window["relative_performance"])
    _write_artifact(run_dir / "replay_window_summary.json", window["window_summary"])
    _write_artifact(run_dir / "replay_selection_rule_change_log.json", {"schema_version": window["schema_version"], "items": window["selection_rule_change_log"]})


def _store_suite_artifacts(suite_dir: Path, payload: dict[str, Any]) -> None:
    _write_artifact(suite_dir / "replay_run_config.json", payload["run_config"])
    _write_artifact(suite_dir / "replay_multiwindow_leaderboard.json", payload["multiwindow_leaderboard"])
    _write_artifact(suite_dir / "replay_selection_rule_change_log.json", {"schema_version": payload["schema_version"], "items": build_replay_change_log(payload["run_config"])})


def _derive_id(prefix: str, payload: dict[str, Any]) -> str:
    base = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(json.dumps({"prefix": prefix, "base": base}, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def run_portfolio_replay(repo: StockRepository, payload: dict[str, Any]) -> dict[str, Any]:
    result = build_replay_window(repo, payload)
    if "window" in result:
        window = result["window"]
        run_id = _text(payload.get("run_id")) or _derive_id("replay", {"window_start_date": window["window_summary"]["window_start_date"], "policy": result["run_config"]["policy_id"], "family_signature": result["run_config"].get("policy_family_signature"), "signatures": result["run_config"]["selection_rule_signatures"]})
        run_dir = replay_run_dir(run_id)
        window["window_summary"]["run_id"] = run_id
        window["window_summary"]["suite_id"] = ""
        _store_window_artifacts(run_dir, window)
        return {"ok": True, "run_id": run_id, "run_dir": str(run_dir), "window": window}
    suite_id = _text(payload.get("suite_id")) or _derive_id("suite", {"window_start_dates": result["run_config"]["window_start_dates"], "policy": result["run_config"]["policy_id"], "family_signature": result["run_config"].get("policy_family_signature"), "signatures": result["run_config"]["selection_rule_signatures"]})
    suite_dir = replay_suite_dir(suite_id)
    _store_suite_artifacts(suite_dir, result)
    return {"ok": True, "suite_id": suite_id, "suite_dir": str(suite_dir), "result": result}


def run_policy_family_replay(repo: StockRepository, payload: dict[str, Any]) -> dict[str, Any]:
    result = build_policy_family_replay(repo, payload)
    family_id = _text(result.get("family_id"))
    family_dir = replay_family_dir(family_id)
    _write_artifact(family_dir / "policy_family_result.json", result)
    return {"ok": True, "family_id": family_id, "family_dir": str(family_dir), "result": result}


def run_policy_family_cohort_replay(repo: StockRepository, payload: dict[str, Any]) -> dict[str, Any]:
    result = run_policy_family_cohort(repo, payload)
    cohort_id = _text(result.get("cohort_id"))
    cohort_dir = replay_root() / "cohorts" / cohort_id
    return {"ok": True, "cohort_id": cohort_id, "cohort_dir": str(cohort_dir), "result": result}


def load_replay_run(run_id: str) -> dict[str, Any]:
    run_dir = replay_runs_root() / _text(run_id)
    return {
        "ok": True,
        "run_id": run_id,
        "run_config": os_store.read_json_object_strict(run_dir / "replay_run_config.json", artifact_name="replay run config"),
        "daily_selection_snapshot": os_store.read_json_object_strict(run_dir / "replay_daily_selection_snapshot.json", artifact_name="replay daily selection snapshot"),
        "feature_snapshot": os_store.read_json_object_strict(run_dir / "replay_feature_snapshot.json", artifact_name="replay feature snapshot"),
        "positions_timeline": os_store.read_json_object_strict(run_dir / "replay_positions_timeline.json", artifact_name="replay positions timeline"),
        "trade_ledger": os_store.read_json_object_strict(run_dir / "replay_trade_ledger.json", artifact_name="replay trade ledger"),
        "daily_equity_curve": os_store.read_json_object_strict(run_dir / "replay_daily_equity_curve.json", artifact_name="replay daily equity curve"),
        "benchmark_market": os_store.read_json_object_strict(run_dir / "replay_benchmark_market.json", artifact_name="replay benchmark market"),
        "benchmark_universe": os_store.read_json_object_strict(run_dir / "replay_benchmark_universe.json", artifact_name="replay benchmark universe"),
        "relative_performance": os_store.read_json_object_strict(run_dir / "replay_relative_performance.json", artifact_name="replay relative performance"),
        "window_summary": os_store.read_json_object_strict(run_dir / "replay_window_summary.json", artifact_name="replay window summary"),
        "selection_rule_change_log": os_store.read_json_object_strict(run_dir / "replay_selection_rule_change_log.json", artifact_name="replay selection rule change log"),
    }


def load_policy_family_run(family_id: str) -> dict[str, Any]:
    family_dir = replay_families_root() / _text(family_id)
    return {
        "ok": True,
        "family_id": family_id,
        "family_dir": str(family_dir),
        "result": load_policy_family_replay(family_id),
    }


def load_policy_family_cohort_run(cohort_id: str) -> dict[str, Any]:
    cohort_dir = replay_root() / "cohorts" / _text(cohort_id)
    return {
        "ok": True,
        "cohort_id": cohort_id,
        "cohort_dir": str(cohort_dir),
        "result": load_policy_family_cohort(cohort_id),
    }
