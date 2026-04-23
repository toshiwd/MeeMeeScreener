from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

import duckdb
import pytest

import app.backend.services.codex_bridge_service as bridge
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


def _fake_runtime_guard(*, stale: bool = False) -> dict[str, object]:
    runtime_status = {
        "stale": stale,
        "freshness_state": "stale" if stale else "fresh",
        "freshness_days": 7 if stale else 1,
        "selected_runtime_db_path": "C:/runtime/stocks.duckdb",
    }
    rankings_status = {
        "long": {
            "stale": stale,
            "freshness_state": "stale" if stale else "fresh",
            "freshness_days": 7 if stale else 1,
            "note": "runtime DB freshness is stale; rankings reflect stale local data" if stale else None,
        },
        "short": {
            "stale": stale,
            "freshness_state": "stale" if stale else "fresh",
            "freshness_days": 7 if stale else 1,
            "note": "runtime DB freshness is stale; rankings reflect stale local data" if stale else None,
        },
    }
    return {
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_status,
        "stale": stale,
    }


def _fake_rank_item(
    code: str,
    *,
    score: float,
    setup_type: str = "breakout",
    entry_qualified: bool = True,
    fallback: bool = False,
) -> dict[str, object]:
    return {
        "code": code,
        "tradePriorityScore": score,
        "tradePriorityHitScore": score - 0.05,
        "tradePriorityQualityScore": score - 0.02,
        "tradePrioritySafetyScore": score - 0.03,
        "setupType": setup_type,
        "entryQualified": entry_qualified,
        "entryQualifiedByFallback": fallback,
        "entryQualifiedFallbackStage": "fallback_stage" if fallback else None,
    }


def _fake_state_eval_row(code: str, *, side: str, judgement: str = "enter", machine_state: str = "enter") -> dict[str, object]:
    return {
        "publish_id": "pub-1",
        "as_of_date": "2026-04-20",
        "code": code,
        "side": side,
        "holding_band": "core",
        "strategy_tags": [],
        "state_action": "enter",
        "decision_3way": "enter",
        "confidence": 0.91,
        "machine_action_state": machine_state,
        "human_readable_judgement": judgement,
        "buy_score": 0.81,
        "environment_score": 0.71,
        "trend_score": 0.76,
        "trigger_score": 0.79,
        "risk_score": 0.12,
        "invalidation_price": 123.4,
        "invalidation_reason_code": None,
        "reason_codes": [],
        "reason_text_top3": [],
        "freshness_state": "fresh",
    }


def _stub_candidate_sections(monkeypatch) -> None:
    monkeypatch.setattr(bridge, "_build_candidate_event_risk", lambda **kwargs: {"available": True, "reason": None, "tdnet_recent": [], "edinet_recent": [], "rights_warning": None})
    monkeypatch.setattr(bridge, "_build_candidate_supply_demand_risk", lambda **kwargs: {"available": True, "reason": None, "taisyaku_snapshot": {"latestFee": None, "latestBalance": None, "restrictions": []}, "borrow_cost_warning": None})
    monkeypatch.setattr(
        bridge,
        "_build_tradex_detail_section",
        lambda *args, **kwargs: {"available": True, "reason": None, "fallback_used": False, "item": {"available": True, "forecast_surface": {"available": True, "reason": None}}},
    )
    monkeypatch.setattr(
        bridge,
        "_build_tradex_similar_cases",
        lambda *args, **kwargs: {"available": False, "reason": "missing", "rows": [], "count": 0},
    )
    monkeypatch.setattr(
        bridge,
        "_build_candidate_tradex_summary",
        lambda **kwargs: {"available": True, "reason": None, "detail_analysis_available": True, "state_eval_available": True, "similar_cases_count": 0, "fallback_used": False},
    )
    monkeypatch.setattr(
        bridge,
        "get_state_eval_rows",
        lambda *args, **kwargs: {
            "rows": [_fake_state_eval_row(kwargs.get("code", "0001"), side="long")],
            "available": True,
            "reason": None,
            "degrade_reason": None,
        },
    )


def _patch_screening_rankings(monkeypatch, *, up_items: list[dict[str, object]], down_items: list[dict[str, object]], stale: bool = False) -> None:
    _stub_candidate_sections(monkeypatch)
    monkeypatch.setattr(bridge, "_build_runtime_guard", lambda **kwargs: _fake_runtime_guard(stale=stale))
    monkeypatch.setattr(
        bridge,
        "_build_runtime_warnings",
        lambda guard: ["runtime DB freshness is stale"] if stale else [],
    )

    def _get_rankings_asof(tf, which, direction, limit, *, as_of, mode="trade", risk_mode="balanced"):
        payload_items = up_items if direction == "up" else down_items
        return {
            "tf": tf,
            "which": which,
            "dir": direction,
            "mode": mode,
            "risk_mode": risk_mode,
            "legacy_analysis_disabled": False,
            "candidate_source": "ml_plus_features",
            "requested_as_of": str(as_of),
            "snapshot_as_of": "2026-04-20",
            "freshness_state": "stale" if stale else "fresh",
            "freshness_days": 7 if stale else 1,
            "stale": stale,
            "current_candidate_available": not stale,
            "items": list(payload_items)[: int(limit)],
        }

    monkeypatch.setattr(bridge.rankings_cache, "get_rankings_asof", _get_rankings_asof)


def test_tools_registry_lists_expected_tools() -> None:
    tool_names = [tool["name"] for tool in mcp.list_tools()]
    assert tool_names == [
        "get_runtime_stock_db_status",
        "get_rankings_freshness",
        "get_publish_runtime_state",
        "get_meemee_artifact_boundary",
        "get_release_build_status",
        "get_stock_analysis_bundle",
        "get_screening_review_bundle",
    ]


def test_call_tool_wraps_json_text() -> None:
    payload = mcp.call_tool("get_meemee_artifact_boundary", {})
    assert payload["isError"] is False
    text = payload["content"][0]["text"]
    parsed = json.loads(text)
    assert parsed["deny_by_default"] is True
    assert parsed["allowlist_count"] == len(mcp.MEEMEE_SAFE_ARTIFACT_FILENAMES)


def test_call_tool_rejects_missing_stock_code() -> None:
    with pytest.raises(ValueError, match="code is required"):
        mcp.call_tool("get_stock_analysis_bundle", {})


def test_jsonrpc_new_tool_validation_returns_invalid_params() -> None:
    response = mcp._handle_request(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                "name": "get_screening_review_bundle",
                "arguments": {"asof": "2026-04-20"},
            },
        }
    )
    assert response is not None
    assert response["error"]["code"] == -32602


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"asof": "2026-04-20"}, "exactly one of top_n or codes is required"),
        ({"asof": "2026-04-20", "top_n": 3, "codes": ["0001"]}, "exactly one of top_n or codes is required"),
        ({"asof": "2026-04-20", "top_n": 0}, "top_n must be between 1 and 20"),
        ({"asof": "2026-04-20", "top_n": 21}, "top_n must be between 1 and 20"),
        ({"asof": "2026-04-20", "codes": []}, "codes must contain at least one code"),
        ({"asof": "2026-04-20", "codes": ["1"] * 21}, "codes must contain at most 20 items"),
        ({"asof": "2026-04-20", "top_n": 1, "side": "invalid"}, "side must be one of long, short, both"),
        ({"asof": "2026-04-20", "top_n": 1, "risk_mode": "invalid"}, "risk_mode must be one of defensive, balanced, aggressive"),
    ],
)
def test_screening_bundle_validation_rejects_invalid_inputs(kwargs, expected) -> None:
    with pytest.raises(ValueError, match=expected):
        bridge.build_screening_review_bundle(**kwargs)


def test_jsonrpc_tools_call_without_name_fails_clear() -> None:
    response = mcp._handle_request({"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"arguments": {}}})
    assert response is not None
    assert response["error"]["code"] == -32602


def test_get_runtime_stock_db_status_reports_fresh_selected_db(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    _make_runtime_stock_db(db_path)
    monkeypatch.setenv("STOCKS_DB_PATH", str(db_path))
    monkeypatch.setattr(bridge, "_current_jst_date", lambda: date(2026, 4, 21))
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
        bridge.rankings_cache,
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
        bridge,
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


def test_stock_bundle_returns_runtime_guard_and_owner_separation(monkeypatch) -> None:
    monkeypatch.setattr(bridge, "_build_runtime_guard", lambda **kwargs: _fake_runtime_guard(stale=True))
    monkeypatch.setattr(bridge, "_build_runtime_warnings", lambda guard: ["runtime DB freshness is stale"])
    monkeypatch.setattr(
        bridge,
        "_build_meemee_bundle",
        lambda *args, **kwargs: {
            "owner": "MeeMee",
            "available": True,
            "reason": None,
            "analysis": {"available": True, "reason": None},
            "sell_analysis": {"available": True, "reason": None},
            "edinet_summary": {"available": False, "reason": "missing"},
            "edinet_financials": {"available": False, "reason": "missing"},
            "tdnet_disclosures": {"available": False, "reason": "missing"},
            "taisyaku_snapshot": {"available": False, "reason": "missing"},
        },
    )
    monkeypatch.setattr(
        bridge,
        "_build_tradex_bundle",
        lambda *args, **kwargs: {
            "owner": "TRADEX",
            "available": True,
            "reason": None,
            "analysis_bridge_status": {"available": True, "reason": None},
            "detail_analysis": {"available": True, "reason": None, "fallback_used": False},
            "forecast_surface": {"available": True, "reason": None, "fallback_used": False},
            "state_eval_rows": {"available": True, "reason": None, "rows": []},
            "similar_cases": {"available": False, "reason": "missing", "rows": [], "count": 0},
            "promotion_review": {"available": True, "reason": None},
        },
    )

    result = bridge.build_stock_analysis_bundle(code="0001", asof="2026-04-20")

    assert result["confirmed"] is True
    assert result["runtime_guard"]["stale"] is True
    assert result["warnings"]
    assert result["meemee"]["owner"] == "MeeMee"
    assert result["tradex"]["owner"] == "TRADEX"
    assert result["meemee"]["analysis"]["available"] is True
    assert result["tradex"]["detail_analysis"]["fallback_used"] is False


def test_mcp_stock_analysis_bundle_call_uses_service(monkeypatch) -> None:
    monkeypatch.setattr(
        bridge,
        "build_stock_analysis_bundle",
        lambda **kwargs: {"confirmed": True, "code": kwargs["code"], "runtime_guard": {}, "warnings": [], "meemee": {"owner": "MeeMee"}, "tradex": {"owner": "TRADEX"}},
    )
    payload = mcp.call_tool("get_stock_analysis_bundle", {"code": "0001"})
    parsed = json.loads(payload["content"][0]["text"])
    assert parsed["code"] == "0001"
    assert parsed["meemee"]["owner"] == "MeeMee"
    assert parsed["tradex"]["owner"] == "TRADEX"


def test_screening_bundle_top_n_boundary_only_changes_boundary_review(monkeypatch) -> None:
    up_items = [
        _fake_rank_item("0001", score=0.92),
        _fake_rank_item("0002", score=0.88),
        _fake_rank_item("0003", score=0.84),
        _fake_rank_item("0004", score=0.80),
    ]
    down_items = [
        _fake_rank_item("9001", score=0.93, setup_type="breakdown"),
        _fake_rank_item("9002", score=0.89, setup_type="breakdown"),
        _fake_rank_item("9003", score=0.83, setup_type="breakdown"),
    ]
    _patch_screening_rankings(monkeypatch, up_items=up_items, down_items=down_items)

    no_boundary = bridge.build_screening_review_bundle(asof="2026-04-20", top_n=1, side="long", include_near_boundary=False)
    with_boundary = bridge.build_screening_review_bundle(asof="2026-04-20", top_n=1, side="long", include_near_boundary=True)

    assert no_boundary["candidates"] == with_boundary["candidates"]
    assert no_boundary["boundary_review"]["near_boundary_codes"] == []
    assert len(with_boundary["boundary_review"]["near_boundary_codes"]) == 3
    assert with_boundary["boundary_review"]["top_boundary_observability"]["long"]["cutoff_code"] == "0001"
    assert with_boundary["candidates"][0]["actionability"]["action_label"] == "buy"
    assert with_boundary["candidates"][0]["actionability"]["confidence_band"] == "high"
    assert with_boundary["candidates"][0]["actionability"]["blocking_flags"] == []


def test_screening_bundle_dual_direction_conflict_is_blocked(monkeypatch) -> None:
    up_items = [
        _fake_rank_item("0001", score=0.91),
        _fake_rank_item("0002", score=0.84),
    ]
    down_items = [
        _fake_rank_item("0001", score=0.95, setup_type="breakdown"),
        _fake_rank_item("9002", score=0.81, setup_type="breakdown"),
    ]
    _patch_screening_rankings(monkeypatch, up_items=up_items, down_items=down_items)

    result = bridge.build_screening_review_bundle(asof="2026-04-20", top_n=2, side="both", include_near_boundary=False)
    conflict = next(candidate for candidate in result["candidates"] if candidate["code"] == "0001")

    assert conflict["selected_direction"] == "short"
    assert "dual_direction_conflict" in conflict["actionability"]["blocking_flags"]
    assert conflict["actionability"]["confidence_band"] == "low"
    assert conflict["actionability"]["action_label"] == "avoid"
    assert result["boundary_review"]["top_boundary_observability"]["long"]["cutoff_code"] == "0002"
    assert result["boundary_review"]["top_boundary_observability"]["short"]["cutoff_code"] == "9002"


def test_screening_bundle_stale_warnings_propagate(monkeypatch) -> None:
    up_items = [_fake_rank_item("0001", score=0.92)]
    down_items = [_fake_rank_item("9001", score=0.91, setup_type="breakdown")]
    _patch_screening_rankings(monkeypatch, up_items=up_items, down_items=down_items, stale=True)

    result = bridge.build_screening_review_bundle(asof="2026-04-20", top_n=1, side="long", include_near_boundary=False)

    assert result["runtime_guard"]["stale"] is True
    assert any("runtime DB freshness is stale" in warning for warning in result["warnings"])
    assert result["screening_source"]["ranking_snapshot_status"]["long"]["stale"] is True


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


@pytest.mark.parametrize(
    "tool_name, arguments_json, expected_key",
    [
        ("get_stock_analysis_bundle", '{"code":"0001"}', "meemee"),
        ("get_screening_review_bundle", '{"asof":"2026-04-20","top_n":1,"side":"both"}', "candidates"),
    ],
)
def test_client_smoke_generic_tool_invocations(tmp_path, tool_name, arguments_json, expected_key) -> None:
    smoke_script = Path("tools/mcp/meemee_runtime_mcp_smoke.py")
    config_path = Path("tools/mcp/meemee_runtime_mcp.client.json")
    db_path = tmp_path / "stocks.duckdb"
    _make_runtime_stock_db(db_path)
    proc = subprocess.run(
        [
            sys.executable,
            str(smoke_script),
            "--config",
            str(config_path),
            "--tool",
            tool_name,
            "--arguments-json",
            arguments_json,
        ],
        cwd=str(Path.cwd()),
        capture_output=True,
        text=True,
        env={**os.environ, "STOCKS_DB_PATH": str(db_path)},
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout.strip())
    assert payload["tool"] == tool_name
    assert expected_key in payload["tool_result"]
