from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.image_rerank.artifacts import read_json
from external_analysis.image_rerank.cli import run_image_rerank_phase0_3


def _weekday_ints(start: date, count: int) -> list[int]:
    values: list[int] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(int(current.strftime("%Y%m%d")))
        current += timedelta(days=1)
    return values


def _seed_richer_source_db(source_db: str) -> list[int]:
    conn = duckdb.connect(source_db)
    dates = _weekday_ints(date(2026, 1, 5), 130)
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


def _prepare_phase1(monkeypatch, source_db: str, export_db: str, label_db: str, result_db: str, ops_db: str) -> list[int]:
    monkeypatch.setenv("STOCKS_DB_PATH", source_db)
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", result_db)
    commands = [
        ["external_analysis", "init-result-db", "--db-path", result_db],
        ["external_analysis", "init-export-db", "--db-path", export_db],
        ["external_analysis", "init-label-db", "--db-path", label_db],
        ["external_analysis", "init-ops-db", "--db-path", ops_db],
        ["external_analysis", "export-sync", "--source-db-path", source_db, "--export-db-path", export_db],
        ["external_analysis", "label-build", "--export-db-path", export_db, "--label-db-path", label_db],
    ]
    for argv in commands:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0
    return _seed_richer_source_db(source_db)  # keep return shape aligned for snapshot selection


def test_image_rerank_phase0_3_writes_json_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source.duckdb"
    export_db = tmp_path / "export.duckdb"
    label_db = tmp_path / "label.duckdb"
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dates = _seed_richer_source_db(str(source_db))
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    for argv in [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-ops-db", "--db-path", str(ops_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0

    snapshot_date = str(dates[60])
    payload = run_image_rerank_phase0_3(
        export_db_path=str(export_db),
        as_of_snapshot_date=snapshot_date,
        run_id="image-rerank-smoke",
        top_k=10,
        block_size_days=20,
        embargo_days=5,
        feature_lookback_days=30,
        label_horizon_days=5,
    )
    run_path = tradex_root / "image_rerank" / "runs" / "image-rerank-smoke" / "run.json"
    compare_path = tradex_root / "image_rerank" / "runs" / "image-rerank-smoke" / "outputs" / "phase3_compare.json"
    split_path = tradex_root / "image_rerank" / "runs" / "image-rerank-smoke" / "manifests" / "split.json"
    run_json = read_json(run_path)
    compare_json = read_json(compare_path)
    split_json = read_json(split_path)
    assert payload["ok"] is True
    assert run_json["schema_version"] == "tradex_image_rerank_run_v1"
    assert run_json["base_score_artifact_uri"].endswith("base_score.json")
    assert run_json["base_score_artifact_checksum"]
    assert run_json["candidate_universe_hash"]
    assert run_json["as_of_snapshot_date"] == int(snapshot_date)
    assert run_json["verify_profile"] == "smoke"
    assert split_json["purge_rule"]["name"] == "time-block split + purge + embargo"
    assert compare_json["metrics"]["top_k_uplift"] is not None
    assert compare_json["metrics"]["bad_pick_removal"] is not None
    assert compare_json["metrics"]["changed_top10_count"] >= 0
    assert compare_json["verify_profile"] == "smoke"
    assert "dropped_codes" in compare_json["readout"]
    assert "false_veto_count" in compare_json["readout"]
    assert "winner_drop_count" in compare_json["readout"]
    assert "fusion_sweep" in compare_json
    assert set(compare_json["fusion_sweep"]["modes"]) == {"rank_improver", "veto_helper"}
    assert compare_json["base_top_rows"]
    assert compare_json["fused_top_rows"]


def test_image_rerank_cli_command_runs(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source2.duckdb"
    export_db = tmp_path / "export2.duckdb"
    label_db = tmp_path / "label2.duckdb"
    result_db = tmp_path / "result2.duckdb"
    ops_db = tmp_path / "ops2.duckdb"
    tradex_root = tmp_path / "tradex_root2"
    dates = _seed_richer_source_db(str(source_db))
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))
    for argv in [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-ops-db", "--db-path", str(ops_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        assert external_analysis_main() == 0

    argv = [
        "external_analysis",
        "image-rerank-run",
        "--export-db-path",
        str(export_db),
        "--as-of-date",
        str(dates[60]),
        "--run-id",
        "image-rerank-cli",
        "--block-size-days",
        "20",
        "--embargo-days",
        "5",
        "--feature-lookback-days",
        "30",
        "--label-horizon-days",
        "5",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    assert external_analysis_main() == 0
    run_path = tradex_root / "image_rerank" / "runs" / "image-rerank-cli" / "run.json"
    assert run_path.exists()
