from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

import duckdb

import tools.mcp.meemee_runtime_mcp as mcp


def _make_runtime_stock_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE daily_bars(code VARCHAR, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE, source VARCHAR)")
        conn.execute(
            "INSERT INTO daily_bars VALUES "
            "('0001', 20260419, 100.0, 102.0, 99.0, 101.0, 1000.0, 'pan'), "
            "('0001', 20260420, 101.0, 103.0, 100.0, 102.0, 1100.0, 'pan'), "
            "('0002', 20260419, 50.0, 51.0, 49.0, 50.5, 900.0, 'pan'), "
            "('0002', 20260420, 50.5, 52.0, 50.0, 51.5, 950.0, 'pan')"
        )
        conn.execute("CREATE TABLE monthly_bars(code VARCHAR, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v DOUBLE)")
        conn.execute(
            "INSERT INTO monthly_bars VALUES "
            "('0001', 20260401, 99.0, 104.0, 98.0, 102.0, 5000.0), "
            "('0002', 20260401, 49.0, 52.0, 48.0, 51.0, 4000.0)"
        )
        conn.execute("CREATE TABLE tickers(code VARCHAR, name VARCHAR)")
        conn.execute("INSERT INTO tickers VALUES ('0001', 'AAA'), ('0002', 'BBB')")
        conn.execute("CREATE TABLE market_regime_daily(date INTEGER)")
        conn.execute("INSERT INTO market_regime_daily VALUES (20260420)")
        conn.execute("CREATE TABLE feature_snapshot_daily(date INTEGER)")
        conn.execute("INSERT INTO feature_snapshot_daily VALUES (20260418)")
        conn.execute("CREATE TABLE ml_pred_20d(dt INTEGER)")
        conn.execute("INSERT INTO ml_pred_20d VALUES (20260417)")
    finally:
        conn.close()


def _create_safe_artifacts(root: Path) -> None:
    inventory = root / "_internal" / "artifacts" / "research_inventory"
    inventory.mkdir(parents=True, exist_ok=True)
    for artifact_name in mcp.MEEMEE_SAFE_ARTIFACT_FILENAMES:
        (inventory / artifact_name).write_text("{}", encoding="utf-8")


def test_tools_registry_lists_expected_tools() -> None:
    tool_names = [tool["name"] for tool in mcp.list_tools()]
    assert tool_names == [
        "get_runtime_stock_db_status",
        "get_rankings_freshness",
        "get_publish_runtime_state",
        "get_meemee_artifact_boundary",
        "get_release_build_status",
    ]


def test_call_tool_wraps_json_text() -> None:
    payload = mcp.call_tool("get_meemee_artifact_boundary", {})
    assert payload["isError"] is False
    text = payload["content"][0]["text"]
    parsed = json.loads(text)
    assert parsed["deny_by_default"] is True
    assert parsed["allowlist_count"] == len(mcp.MEEMEE_SAFE_ARTIFACT_FILENAMES)


def test_jsonrpc_tools_call_without_name_fails_clear() -> None:
    response = mcp._handle_request({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"arguments": {}}})
    assert response is not None
    assert response["error"]["code"] == -32602


def test_get_runtime_stock_db_status_reports_fresh_selected_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _make_runtime_stock_db(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setattr(mcp, "_current_jst_date", lambda: date(2026, 4, 21))
    mcp.resolve_runtime_stock_db_selection.cache_clear()

    before = db_path.stat().st_mtime
    result = mcp.get_runtime_stock_db_status()
    after = db_path.stat().st_mtime

    assert result["selected_runtime_db_path"] == str(db_path.resolve())
    assert result["resolution_source"] == "STOCKS_DB_PATH"
    assert result["resolution_reason"] == "explicit_runtime_override"
    assert result["latest_available_global_date"] == 20260420
    assert result["latest_daily_bars_date"] == 20260420
    assert result["latest_feature_snapshot_daily_date"] == 20260418
    assert result["latest_ml_pred_20d_date"] == 20260417
    assert result["freshness_state"] == "fresh"
    assert result["stale"] is False
    assert before == after


def test_get_rankings_freshness_reports_current_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        mcp.rankings_cache,
        "get_rankings",
        lambda *args, **kwargs: {
            "snapshot_as_of": "2026-04-20",
            "freshness_state": "fresh",
            "freshness_days": 1,
            "stale": False,
            "current_candidate_available": True,
        },
    )
    monkeypatch.setattr(
        mcp,
        "get_runtime_stock_db_status",
        lambda: {
            "stale": False,
            "selected_runtime_db_path": "C:/runtime/stocks.duckdb",
            "freshness_state": "fresh",
            "freshness_days": 1,
        },
    )

    result = mcp.get_rankings_freshness()

    assert result["ranking_endpoint_source_path"] == "app/backend/api/routers/rankings.py"
    assert result["rankings_cache_contract_path"] == "app/backend/services/ml/rankings_cache.py"
    assert result["snapshot_as_of"] == "2026-04-20"
    assert result["freshness_state"] == "fresh"
    assert result["stale"] is False
    assert result["current_candidate_available"] is True
    assert result["note"] is None


def test_get_publish_runtime_state_is_sanitized(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        mcp,
        "build_runtime_selection_snapshot",
        lambda **kwargs: {
            "snapshot_created_at": "2026-04-21T00:00:00Z",
            "selected_logic_override": "sel",
            "default_logic_pointer": "ptr",
            "registry_default_logic_pointer": "reg_ptr",
            "champion_logic_key": "champ",
            "challenger_logic_key": "chall",
            "challenger_logic_keys": ["chall"],
            "selected_source": "selected_source",
            "resolved_source": "resolved_source",
            "selected_pointer_name": "pointer",
            "validation_state": "ok",
            "validation_issues": [],
            "notes": [],
            "source_of_truth": "external_analysis",
            "registry_sync_state": "in_sync",
            "degraded": False,
            "last_sync_time": "2026-04-21T00:00:00Z",
            "registry_version": 2,
            "source_revision": "rv:2",
            "bootstrap_rule": "rule",
            "external_registry_version": 2,
            "local_mirror_version": 2,
            "mirror_schema_version": "publish_registry_v1",
            "mirror_normalized": True,
            "candidate_backfill_last_run": {"ok": True},
            "snapshot_sweep_last_run": {"ok": True},
            "non_promotable_legacy_count": 0,
            "maintenance_degraded": False,
            "maintenance_state": {"summary": "ok"},
            "operator_mutation_observability": {"status": "ok"},
            "shadow_integration_available": True,
            "shadow_only": False,
            "shadow_integration_state": "shadow",
            "shadow_rollout_boundary": {"state": "shadow"},
            "shadow_verify": {"pass_expected": True},
            "shadow_monitoring_contract": {"state": "monitor"},
            "shadow_integration_validation_issues": [],
            "shadow_integration": {"state": "shadow"},
            "publish_registry": {"raw": "compare"},
            "candidate_review": {"raw": "compare"},
            "catalog": {"raw": "catalog"},
        },
    )

    result = mcp.get_publish_runtime_state(config_data_dir=tmp_path)

    assert result["sanitized"] is True
    assert result["runtime_surface_dependency"] == "TRADEX"
    assert result["source_of_truth"] == "external_analysis"
    assert result["runtime_selection"]["selected_logic_override"] == "sel"
    assert result["publish_state"]["source_of_truth"] == "external_analysis"
    assert result["publish_queue"]["ok"] is True
    assert "publish_registry" not in result["runtime_selection"]
    assert "candidate_review" not in result["runtime_selection"]


def test_get_meemee_artifact_boundary_reports_allowlist() -> None:
    result = mcp.get_meemee_artifact_boundary()

    assert result["deny_by_default"] is True
    assert result["boundary_module_path"].endswith("meemee_artifact_boundary.py")
    assert result["allowlisted_meemee_safe_artifacts"] == list(mcp.MEEMEE_SAFE_ARTIFACT_FILENAMES)
    assert result["known_tradex_only_artifacts"] == list(mcp.TRADEX_ONLY_ARTIFACT_FILENAMES)
    assert result["known_blocked_hold_artifacts"] == list(mcp.BLOCKED_HOLD_ARTIFACT_FILENAMES)


def test_get_release_build_status_reports_packaged_safe_artifacts(tmp_path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    release_root = repo_root / "release"
    release_root.mkdir(parents=True, exist_ok=True)
    zip_path = release_root / "MeeMeeScreener-portable.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        for artifact_name in mcp.MEEMEE_SAFE_ARTIFACT_FILENAMES:
            archive.writestr(f"_internal/artifacts/research_inventory/{artifact_name}", "{}")

    desktop_root = tmp_path / "Desktop"
    package_root = desktop_root / "MeeMeeScreener"
    _create_safe_artifacts(package_root)
    exe_path = package_root / "MeeMeeScreener.exe"
    exe_path.write_bytes(b"fake exe")

    monkeypatch.setenv("MEEMEE_RELEASE_PACKAGE_ROOT", str(desktop_root))

    before = {
        "zip": zip_path.stat().st_mtime,
        "exe": exe_path.stat().st_mtime,
    }
    result = mcp.get_release_build_status(repo_root=repo_root)
    after = {
        "zip": zip_path.stat().st_mtime,
        "exe": exe_path.stat().st_mtime,
    }

    assert result["latest_portable_zip_path"] == str(zip_path)
    assert result["latest_exe_path"] == str(exe_path)
    assert result["packaged_safe_artifacts_summary"]["complete"] is True
    assert result["portable_zip_summary"]["complete"] is True
    assert result["allowlisted_meemee_safe_artifacts_bundled"] is True
    assert result["smoke_pass_ready"] is True
    assert before == after


def test_client_smoke_path_works_end_to_end(tmp_path) -> None:
    smoke_script = Path("tools/mcp/meemee_runtime_mcp_smoke.py")
    config_path = Path("tools/mcp/meemee_runtime_mcp.client.json")
    db_path = tmp_path / "stocks.duckdb"
    _make_runtime_stock_db(db_path)
    proc = subprocess.run(
        [sys.executable, str(smoke_script), "--config", str(config_path)],
        cwd=str(Path.cwd()),
        capture_output=True,
        text=True,
        env={**os.environ, "STOCKS_DB_PATH": str(db_path)},
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout.strip())
    assert payload["runtime_stock_db_status"]["selected_runtime_db_path"] == str(db_path.resolve())
    assert payload["runtime_stock_db_status"]["freshness_state"] in {"fresh", "stale"}
    assert payload["rankings_freshness"]["snapshot_as_of"]
    assert payload["rankings_freshness"]["freshness_state"] in {"fresh", "stale"}
    assert payload["rankings_freshness"]["current_candidate_available"] is True
