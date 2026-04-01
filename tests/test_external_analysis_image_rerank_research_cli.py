from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.image_rerank.artifacts import read_json

pytestmark = pytest.mark.integration


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_richer_source_db(source_db: str, *, day_count: int = 140) -> list[int]:
    conn = duckdb.connect(source_db)
    dates = _weekday_ints(date(2026, 1, 5), int(day_count))
    codes = [f"13{i:02d}" for i in range(1, 13)]
    slopes = [1.2, 0.8, 0.3, -0.4, -0.9, 1.5, -1.2, 0.6, 0.1, 1.0, -0.5, 0.4]
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute(
            "CREATE TABLE feature_snapshot_daily (dt INTEGER, code TEXT, close DOUBLE, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE, cnt_20_above INTEGER, cnt_7_above INTEGER, day_count INTEGER, candle_flags TEXT)"
        )
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute("CREATE TABLE positions_live (symbol TEXT, spot_qty DOUBLE, margin_long_qty DOUBLE, margin_short_qty DOUBLE, buy_qty DOUBLE, sell_qty DOUBLE, opened_at TIMESTAMP, updated_at TIMESTAMP, has_issue BOOLEAN, issue_note TEXT)")
        conn.execute("CREATE TABLE position_rounds (round_id TEXT, symbol TEXT, opened_at TIMESTAMP, closed_at TIMESTAMP, closed_reason TEXT)")
        conn.execute("CREATE TABLE trade_events (broker TEXT, exec_dt TIMESTAMP, symbol TEXT, action TEXT, qty DOUBLE, price DOUBLE, source_row_hash TEXT)")
        for idx, trade_date in enumerate(dates):
            for code_idx, (code, slope) in enumerate(zip(codes, slopes, strict=True)):
                base = 80.0 + (code_idx * 3.5)
                seasonal = (idx % 7) * 0.12
                close_price = base + (idx * slope) + seasonal
                open_price = close_price - 0.4
                high_price = close_price + 1.0 + (0.05 * code_idx)
                low_price = close_price - 1.2 - (0.03 * code_idx)
                volume = 1000 + (idx * 10) + (code_idx * 75)
                conn.execute(
                    "INSERT INTO daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [code, trade_date, open_price, high_price, low_price, close_price, volume, "pan"],
                )
                ma20 = close_price - (0.8 if slope > 0 else -0.8)
                conn.execute(
                    "INSERT INTO daily_ma VALUES (?, ?, ?, ?, ?)",
                    [code, trade_date, close_price - 0.5, ma20, ma20 - 2.0],
                )
                conn.execute(
                    "INSERT INTO feature_snapshot_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [trade_date, code, close_price, close_price - 0.5, ma20, ma20 - 2.0, 2.5, 0.02, 1.0, 3, 5, 20, "flag"],
                )
        for code in codes:
            conn.execute("INSERT INTO monthly_bars VALUES (?, ?, ?, ?, ?, ?, ?)", [code, 202603, 90, 100, 88, 95, 10000])
    finally:
        conn.close()
    return dates


def _run_main(monkeypatch, argv: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0


def _configure_env(monkeypatch, *, tradex_root: Path, source_db: Path, result_db: Path) -> None:
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))


def test_image_rerank_research_cli_blocks_on_incomplete_export_snapshot(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_blocker.duckdb"
    export_db = tmp_path / "export_blocker.duckdb"
    result_db = tmp_path / "result_blocker.duckdb"
    tradex_root = tmp_path / "tradex_root_blocker"
    session_id = "image-rerank-research-blocker"
    _seed_richer_source_db(str(source_db))
    _configure_env(monkeypatch, tradex_root=tradex_root, source_db=source_db, result_db=result_db)

    _run_main(monkeypatch, ["external_analysis", "export-snapshot-build", "--source-db-path", str(source_db), "--export-db-path", str(export_db)])
    conn = duckdb.connect(str(export_db), read_only=False)
    try:
        conn.execute("DELETE FROM meta_export_runs")
    finally:
        conn.close()

    _run_main(
        monkeypatch,
        [
            "external_analysis",
            "image-rerank-research-run",
            "--source-db-path",
            str(source_db),
            "--export-db-path",
            str(export_db),
            "--session-id",
            session_id,
            "--top-k",
            "10",
            "--renderer-backend",
            "agg",
        ],
    )

    session_dir = tradex_root / "image_rerank" / "research_sessions" / session_id
    confirm_json = read_json(session_dir / "full_universe_confirm.json")
    assert confirm_json["ok"] is False
    assert confirm_json["confirm_stage"] == "preconditions"
    assert confirm_json["blocked_before_confirm"] is True
    assert confirm_json["precondition_checks"]["export_snapshot_complete"] is False
    assert confirm_json["blocker_reason_code"] == "meta_missing"
    assert confirm_json["export_probe"]["status"] == "incomplete"
    assert confirm_json["export_probe"]["reason_code"] == "meta_missing"
    assert "progress_status" in confirm_json["export_probe"]
    assert "progress_path" in confirm_json["export_probe"]
    assert confirm_json["source_signature"]
    assert confirm_json["expected_export_signature"]["pattern_count"] > 0
    assert confirm_json["contract_checks"]["checked"] is False
    assert confirm_json["contract_checks"]["run_artifacts_present"] is False
    assert confirm_json["contract_checks"]["compare_artifacts_present"] is False
    assert not (session_dir / "challenger_first_analysis.json").exists()


def test_image_rerank_research_cli_reaches_first_analysis_with_complete_snapshot(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_success.duckdb"
    export_db = tmp_path / "export_success.duckdb"
    result_db = tmp_path / "result_success.duckdb"
    tradex_root = tmp_path / "tradex_root_success"
    session_id = "image-rerank-research-success"
    _seed_richer_source_db(str(source_db), day_count=140)
    _configure_env(monkeypatch, tradex_root=tradex_root, source_db=source_db, result_db=result_db)

    _run_main(monkeypatch, ["external_analysis", "export-snapshot-build", "--source-db-path", str(source_db), "--export-db-path", str(export_db)])
    _run_main(
        monkeypatch,
        [
            "external_analysis",
            "image-rerank-research-run",
            "--source-db-path",
            str(source_db),
            "--export-db-path",
            str(export_db),
            "--session-id",
            session_id,
            "--as-of-date",
            "20260424",
            "--top-k",
            "10",
            "--renderer-backend",
            "agg",
        ],
    )

    session_dir = tradex_root / "image_rerank" / "research_sessions" / session_id
    confirm_json = read_json(session_dir / "full_universe_confirm.json")
    analysis_json = read_json(session_dir / "challenger_first_analysis.json")
    disposition_json = read_json(session_dir / "challenger_disposition.json")

    assert confirm_json["ok"] is True
    assert confirm_json["confirm_stage"] == "complete"
    assert confirm_json["blocked_before_confirm"] is False
    assert confirm_json["precondition_checks"]["export_snapshot_complete"] is True
    assert confirm_json["export_probe"]["status"] == "complete"
    assert confirm_json["export_probe"]["progress_status"] == "complete"
    assert confirm_json["export_probe"]["last_completed_step"] == "meta_export_runs"
    assert confirm_json["export_probe"]["incomplete_steps"] == []
    assert confirm_json["contract_checks"]["checked"] is True
    assert confirm_json["contract_checks"]["run_artifacts_present"] is True
    assert confirm_json["contract_checks"]["compare_artifacts_present"] is True
    assert confirm_json["contract_checks"]["run"]["contract_status"] == "fixed"
    assert confirm_json["contract_checks"]["run"]["protected_block_rule"]
    assert confirm_json["contract_checks"]["run"]["stress_feasibility_condition"] == "block_size_days > label_horizon_days + embargo_days"
    assert confirm_json["contract_checks"]["split"]["reason_code_definitions"]
    assert confirm_json["contract_checks"]["compare"]["readout_contract"]["primary_fields"]
    assert confirm_json["contract_checks"]["compare"]["fusion_sweep"]["parameters"]
    assert confirm_json["contract_checks"]["compare"]["readout"]["dropped_top_codes"] is not None
    assert confirm_json["contract_checks"]["compare"]["readout"]["added_top_codes"] is not None
    assert confirm_json["contract_checks"]["compare"]["readout"]["rank_uplift_contributors"] is not None

    assert analysis_json["schema_version"] == "tradex_image_rerank_first_analysis_v1"
    assert analysis_json["session_id"] == session_id
    assert analysis_json["scope_mode"] == "full_universe"
    assert analysis_json["baseline_kind"] == "frozen_base_score"
    assert analysis_json["challenger_kind"] == "image_rerank_rank_improver"
    assert analysis_json["comparison_invariants"]["same_universe"] is True
    assert analysis_json["comparison_invariants"]["same_period"] is True
    assert analysis_json["comparison_invariants"]["same_top_k"] is True
    assert analysis_json["comparison_invariants"]["same_regime"] is True
    assert analysis_json["comparison_invariants"]["same_cost"] is True
    assert analysis_json["comparison_invariants"]["same_artifact_detail_level"] is True
    assert "top_k_uplift" in analysis_json["metrics"]
    assert "bad_pick_removal" in analysis_json["metrics"]
    assert "changed_top10_count" in analysis_json["metrics"]
    assert "selection_divergence" in analysis_json["metrics"]
    assert disposition_json["decision"] in {"keep", "drop", "hold"}
    assert disposition_json["summary_flags"]["artifact_complete"] is True
    assert disposition_json["source_artifacts"]["phase3_compare_uri"] == analysis_json["artifacts"]["phase3_compare_uri"]

    run_json_path = Path(analysis_json["artifacts"]["run_json_uri"])
    split_json_path = Path(analysis_json["artifacts"]["split_json_uri"])
    compare_json_path = Path(analysis_json["artifacts"]["phase3_compare_uri"])
    assert run_json_path.exists()
    assert split_json_path.exists()
    assert compare_json_path.exists()
