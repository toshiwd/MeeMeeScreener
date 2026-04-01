from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from external_analysis.event_image_dataset.analysis import (
    analyze_event_image_dataset_regime,
    build_event_image_pattern_adoption_compare,
    build_event_image_pattern_adoption_policy,
    build_event_image_pattern_adoption,
    build_event_image_pattern_breadth,
    build_event_image_pattern_boundary,
    build_event_image_pattern_combo_rules,
    build_event_image_pattern_gating,
    build_event_image_pattern_library,
    build_event_image_pattern_playbook,
    build_event_image_pattern_playbook_relax_compare,
    build_event_image_pattern_playbook_threshold_compare,
    build_event_image_pattern_veto_ablation,
    build_event_image_pattern_veto_compare,
    build_event_image_pattern_selection_contract,
    build_event_image_pattern_veto_thin_liquidity_compare,
    build_event_image_pattern_sequence_combo,
    build_event_image_rebound_live_monitor,
    build_event_image_rebound_robustness_compare,
    decompose_event_image_pattern,
    run_event_image_dataset_rebound_v3_round,
    run_event_image_dataset_rebound_multi_research_round,
    run_event_image_dataset_robustness_batch,
    run_event_image_dataset_analysis_batch,
)
from external_analysis.event_image_dataset.build import build_event_image_dataset
from external_analysis.event_image_dataset.restricted_universe import build_meemee_registered_sample_universe
from external_analysis.event_image_dataset.renderer import (
    CONTROL_EVALUATION_BUNDLE_ID,
    FIDELITY_EVALUATION_BUNDLE_ID,
    strict_agg_available,
)
from external_analysis.event_image_dataset.storage import read_parquet_frame, write_parquet_frame
from external_analysis.event_image_dataset.train import run_event_image_dataset_repro, train_event_image_dataset
from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.exporter.snapshot_status import (
    EXPORT_SNAPSHOT_SCHEMA_VERSION,
    build_source_signature_payload,
    resolve_snapshot_status_path,
)
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


def _seed_event_source_db(path: Path, *, code_count: int = 48, day_count: int = 720) -> None:
    conn = duckdb.connect(str(path))
    dates = _weekday_ints(date(2022, 1, 4), int(day_count))
    codes = [f"{1300 + idx:04d}" for idx in range(code_count)]
    month_close_rows: list[tuple[str, int, float, float, float, float, int]] = []
    industry_rows: list[tuple[str, str, str, str, str]] = []
    daily_bar_rows: list[tuple[str, int, float, float, float, float, int, str]] = []
    daily_ma_rows: list[tuple[str, int, float, float, float]] = []
    feature_rows: list[tuple[str, int, float, float, float, int, int, int, str]] = []
    try:
        conn.execute("CREATE TABLE daily_bars (code TEXT, date INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT, source TEXT)")
        conn.execute("CREATE TABLE daily_ma (code TEXT, date INTEGER, ma7 DOUBLE, ma20 DOUBLE, ma60 DOUBLE)")
        conn.execute("CREATE TABLE feature_snapshot_daily (code TEXT, dt INTEGER, atr14 DOUBLE, diff20_pct DOUBLE, diff20_atr DOUBLE, cnt_20_above INTEGER, cnt_7_above INTEGER, day_count INTEGER, candle_flags TEXT)")
        conn.execute("CREATE TABLE monthly_bars (code TEXT, month INTEGER, o DOUBLE, h DOUBLE, l DOUBLE, c DOUBLE, v BIGINT)")
        conn.execute("CREATE TABLE industry_master (code TEXT, name TEXT, sector33_code TEXT, sector33_name TEXT, market_code TEXT)")
        for code_idx, code in enumerate(codes):
            industry_rows.append((code, f"name-{code}", f"S{code_idx % 5}", f"sector-{code_idx % 5}", "prime" if code_idx % 2 == 0 else "standard"))
        for code_idx, code in enumerate(codes):
            month_last_seen: dict[int, tuple[float, float, float, float, int]] = {}
            code_bias = (code_idx - (code_count / 2.0)) * 0.01
            for idx, trade_date in enumerate(dates):
                month_key = trade_date // 100
                seasonal = ((idx % 21) - 10) * 0.03
                month_wave = ((trade_date // 100) % 100) * 0.015
                close_price = 80.0 + (idx * 0.18) + code_bias * idx + seasonal + month_wave
                open_price = close_price - 0.4
                high_price = close_price + 1.1
                low_price = close_price - 1.2
                volume = int(150_000 + (code_idx * 10_000) + (idx * 150))
                ma7 = close_price - 0.3
                ma20 = close_price - 0.8
                ma60 = close_price - 1.6
                daily_bar_rows.append((code, trade_date, open_price, high_price, low_price, close_price, volume, "pan"))
                daily_ma_rows.append((code, trade_date, ma7, ma20, ma60))
                feature_rows.append((code, trade_date, 2.5, 0.02, 1.0, 5, 3, idx + 1, "flag"))
                month_last_seen[month_key] = (open_price, high_price, low_price, close_price, volume)
            for month_key, values in sorted(month_last_seen.items()):
                month_close_rows.append((code, month_key, values[0], values[1], values[2], values[3], values[4]))
        conn.register("industry_rows_df", pd.DataFrame(industry_rows, columns=["code", "name", "sector33_code", "sector33_name", "market_code"]))
        conn.register("daily_bars_df", pd.DataFrame(daily_bar_rows, columns=["code", "date", "o", "h", "l", "c", "v", "source"]))
        conn.register("daily_ma_df", pd.DataFrame(daily_ma_rows, columns=["code", "date", "ma7", "ma20", "ma60"]))
        conn.register(
            "feature_rows_df",
            pd.DataFrame(
                feature_rows,
                columns=["code", "dt", "atr14", "diff20_pct", "diff20_atr", "cnt_20_above", "cnt_7_above", "day_count", "candle_flags"],
            ),
        )
        conn.register("monthly_bars_df", pd.DataFrame(month_close_rows, columns=["code", "month", "o", "h", "l", "c", "v"]))
        conn.execute("INSERT INTO industry_master SELECT * FROM industry_rows_df")
        conn.execute("INSERT INTO daily_bars SELECT * FROM daily_bars_df")
        conn.execute("INSERT INTO daily_ma SELECT * FROM daily_ma_df")
        conn.execute("INSERT INTO feature_snapshot_daily SELECT * FROM feature_rows_df")
        conn.execute("INSERT INTO monthly_bars SELECT * FROM monthly_bars_df")
    finally:
        conn.close()


def _seed_event_export_db_from_source(*, source_db: Path, export_db: Path) -> None:
    ensure_export_db(str(export_db))
    source_payload = build_source_signature_payload(str(source_db))
    source_conn = duckdb.connect(str(source_db), read_only=True)
    export_conn = duckdb.connect(str(export_db), read_only=False)
    try:
        daily_rows = source_conn.execute("SELECT code, date, o, h, l, c, v, source FROM daily_bars ORDER BY code, date").fetchall()
        ma_rows = source_conn.execute(
            """
            SELECT
                m.code,
                m.date,
                m.ma7,
                m.ma20,
                m.ma60,
                f.atr14,
                f.diff20_pct,
                f.diff20_atr,
                f.cnt_20_above,
                f.cnt_7_above,
                f.day_count,
                f.candle_flags
            FROM daily_ma m
            INNER JOIN feature_snapshot_daily f
              ON f.code = m.code AND f.dt = m.date
            ORDER BY m.code, m.date
            """
        ).fetchall()
        pattern_rows = source_conn.execute("SELECT code, dt, candle_flags FROM feature_snapshot_daily ORDER BY code, dt").fetchall()
        export_conn.register(
            "bars_export_df",
            pd.DataFrame(
                [
                    [row[0], int(row[1]), row[2], row[3], row[4], row[5], int(row[6]), row[7], f"bars:{row[0]}:{row[1]}", "run-1"]
                    for row in daily_rows
                ],
                columns=["code", "trade_date", "o", "h", "l", "c", "v", "source", "row_hash", "export_run_id"],
            ),
        )
        export_conn.register(
            "indicator_export_df",
            pd.DataFrame(
                [
                    [row[0], int(row[1]), row[2], row[3], row[4], None, None, row[5], row[6], row[7], row[8], row[9], row[10], row[11], f"indicator:{row[0]}:{row[1]}", "run-1"]
                    for row in ma_rows
                ],
                columns=[
                    "code",
                    "trade_date",
                    "ma7",
                    "ma20",
                    "ma60",
                    "ma100",
                    "ma200",
                    "atr14",
                    "diff20_pct",
                    "diff20_atr",
                    "cnt_20_above",
                    "cnt_7_above",
                    "day_count",
                    "candle_flags",
                    "row_hash",
                    "export_run_id",
                ],
            ),
        )
        export_conn.register(
            "pattern_export_df",
            pd.DataFrame(
                [
                    [row[0], int(row[1]), None, None, None, None, None, None, row[2], f"pattern:{row[0]}:{row[1]}", "run-1"]
                    for row in pattern_rows
                ],
                columns=[
                    "code",
                    "trade_date",
                    "ppp_state",
                    "abc_state",
                    "box_state",
                    "box_upper",
                    "box_lower",
                    "ranking_state",
                    "event_flags",
                    "row_hash",
                    "export_run_id",
                ],
            ),
        )
        export_conn.execute("INSERT INTO bars_daily_export SELECT * FROM bars_export_df")
        export_conn.execute("INSERT INTO indicator_daily_export SELECT * FROM indicator_export_df")
        export_conn.execute("INSERT INTO pattern_state_export SELECT * FROM pattern_export_df")
        export_conn.execute(
            """
            INSERT INTO meta_export_runs (
                run_id, started_at, finished_at, status, source_db_path, source_signature, source_max_trade_date, source_row_counts, changed_table_names, diff_reason
            ) VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'success', ?, ?, ?, ?, ?, ?)
            """,
            [
                "run-1",
                str(source_db),
                str(source_payload["source_signature"]),
                int(source_payload["source_max_trade_date"] or 0),
                json.dumps(source_payload["source_counts"], ensure_ascii=False),
                json.dumps(["bars_daily_export", "indicator_daily_export", "pattern_state_export"], ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
            ],
        )
    finally:
        source_conn.close()
        export_conn.close()

    snapshot_payload = {
        "schema_version": EXPORT_SNAPSHOT_SCHEMA_VERSION,
        "source_db_path": str(source_payload["source_db_path"]),
        "export_db_path": str(export_db),
        "source_signature": str(source_payload["source_signature"]),
        "source_counts": dict(source_payload["source_counts"]),
        "expected_export_signature": dict(source_payload["expected_export_signature"]),
        "required_fields": ["bars_count", "indicator_count", "pattern_count", "max_trade_date"],
        "created_at": "2026-03-28T00:00:00+00:00",
        "status": "complete",
        "reason_code": "complete_match",
        "export_run_id": "run-1",
        "export_signature": dict(source_payload["expected_export_signature"]),
        "export_counts": dict(source_payload["expected_export_signature"]),
    }
    resolve_snapshot_status_path(export_db).write_text(json.dumps(snapshot_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _prepare_rebound_pattern_stack(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, dataset_id: str) -> tuple[Path, Path, str]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source_db = tmp_path / f"{dataset_id}_source.duckdb"
    export_db = tmp_path / f"{dataset_id}_export.duckdb"
    tradex_root = tmp_path / "tradex_root"
    bridge_root = tmp_path / "bridge"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(bridge_root))

    build_event_image_dataset(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        dataset_id=dataset_id,
        start_month="202401",
        end_month="202412",
        renderer_backend="agg",
    )
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    month_rows = compare["month_level_results"]
    if len(month_rows) < 2:
        pytest.skip("synthetic fixture did not produce enough months for rebound pattern stack")
    month_rows[0]["regime_tag"] = "rebound_onset"
    month_rows[0]["winner_accuracy"] = "image"
    month_rows[0]["winner_top10_precision"] = "image"
    month_rows[0]["winner_long_short_spread"] = "image"
    month_rows[1]["regime_tag"] = "uptrend"
    month_rows[1]["winner_accuracy"] = "image"
    month_rows[1]["winner_top10_precision"] = "image"
    month_rows[1]["winner_long_short_spread"] = "image"
    compare["regime_summary"] = [
        {
            "regime_tag": "rebound_onset",
            "regime_label": "反発初動",
            "month_count": 1,
            "image_accuracy_mean": 0.6,
            "numeric_accuracy_mean": 0.4,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.6,
            "numeric_top10_precision_mean": 0.4,
            "image_long_short_spread_mean": 0.1,
            "numeric_long_short_spread_mean": -0.1,
        },
        {
            "regime_tag": "uptrend",
            "regime_label": "上昇トレンド",
            "month_count": 1,
            "image_accuracy_mean": 0.55,
            "numeric_accuracy_mean": 0.45,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.55,
            "numeric_top10_precision_mean": 0.45,
            "image_long_short_spread_mean": 0.05,
            "numeric_long_short_spread_mean": -0.02,
        },
    ]
    Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    for regime_tag in ("rebound_onset", "uptrend"):
        decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    build_event_image_pattern_boundary(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend", max_workers=1)
    build_event_image_pattern_gating(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    build_event_image_pattern_combo_rules(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=["rebound_onset", "uptrend"])
    return tradex_root, bridge_root, dataset_id


def _relax_rebound_gating(tradex_root: Path, dataset_id: str) -> None:
    gating_path = tradex_root / "event_image_dataset" / "datasets" / dataset_id / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
    gating = read_json(gating_path)
    gating["candidate_gate_rule"]["rebound_onset"]["price_vs_ma120_min"] = -1.0
    gating["candidate_gate_rule"]["rebound_onset"]["distance_from_60d_high_range"] = [-1.0, 0.0]
    gating["candidate_gate_rule"]["rebound_onset"]["realized_vol20_min"] = 0.0
    gating_path.write_text(json.dumps(gating, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_restricted_universe_artifact(path: Path, *, month_keys: list[int], codes: list[str], universe_name: str = "TOPIX100") -> None:
    rows: list[dict[str, object]] = []
    month_coverage_start = min(int(value) for value in month_keys)
    month_coverage_end = max(int(value) for value in month_keys)
    source_file_sha256 = "synthetic-source-sha256"
    for month_key in month_keys:
        for code in codes:
            rows.append(
                {
                    "month_key": int(month_key),
                    "universe_name": universe_name,
                    "code": str(code),
                    "source_name": "synthetic_topix100_fixture",
                    "source_version": "v1",
                    "source_uri": "file://synthetic-topix100",
                    "file_sha256": source_file_sha256,
                    "sample_seed": 7,
                    "sampling_policy": json.dumps({"kind": "synthetic_fixture"}, ensure_ascii=False, sort_keys=True),
                    "month_coverage_start": int(month_coverage_start),
                    "month_coverage_end": int(month_coverage_end),
                }
            )
    write_parquet_frame(path, pd.DataFrame(rows))


def _write_watchlist_file(path: Path, codes: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(str(code) for code in codes) + "\n", encoding="utf-8")


def test_event_image_dataset_build_requires_complete_export_snapshot(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_blocker.duckdb"
    export_db = tmp_path / "export_blocker.duckdb"
    tradex_root = tmp_path / "tradex_root"
    _seed_event_source_db(source_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    with pytest.raises(RuntimeError):
        build_event_image_dataset(export_db_path=str(export_db), dataset_id="dataset-blocker", source_db_path=str(source_db), start_month="202401", end_month="202412", renderer_backend="agg")


def test_event_image_dataset_build_requires_strict_agg(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_noagg.duckdb"
    export_db = tmp_path / "export_noagg.duckdb"
    tradex_root = tmp_path / "tradex_root"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setattr("external_analysis.event_image_dataset.build.strict_agg_available", lambda: False)
    with pytest.raises(RuntimeError, match="strict agg backend"):
        build_event_image_dataset(export_db_path=str(export_db), dataset_id="dataset-noagg", source_db_path=str(source_db), start_month="202401", end_month="202412", renderer_backend="agg")


def test_event_image_dataset_universe_build_writes_meemee_registered_sample_artifact(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_universe.duckdb"
    output_path = tmp_path / "meemee_registered_sample100.parquet"
    watchlist_path = tmp_path / "tools" / "code.txt"
    tradex_root = tmp_path / "tradex_root"
    _seed_event_source_db(source_db, code_count=24, day_count=520)
    _write_watchlist_file(watchlist_path, [f"{1300 + idx:04d}" for idx in range(18)])
    monkeypatch.setenv("PAN_CODE_TXT_PATH", str(watchlist_path))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))

    result = build_meemee_registered_sample_universe(
        source_db_path=str(source_db),
        output_path=output_path,
        start_month="202401",
        end_month="202412",
        sample_size=12,
        sample_seed=7,
    )

    artifact = read_parquet_frame(output_path)
    manifest = read_json(output_path.with_suffix(".manifest.json"))

    assert output_path.exists()
    assert output_path.with_suffix(".manifest.json").exists()
    assert result["artifact_path"] == str(output_path.resolve())
    assert result["sample_size"] == 12
    assert manifest["universe_name"] == "MeeMeeRegisteredSample100"
    assert manifest["source_name"] == "MeeMeeWatchlist"
    assert manifest["source_uri"] == str(watchlist_path.resolve())
    assert manifest["sample_seed"] == 7
    assert manifest["month_coverage_start"] == 202401
    assert manifest["month_coverage_end"] == 202412
    assert manifest["artifact_file_sha256"]

    required_columns = {
        "month_key",
        "universe_name",
        "code",
        "source_name",
        "source_version",
        "source_uri",
        "file_sha256",
        "sample_seed",
        "sampling_policy",
        "month_coverage_start",
        "month_coverage_end",
        "sector",
        "market",
        "turnover20",
    }
    assert required_columns.issubset(set(artifact.columns))
    assert sorted(artifact["month_key"].unique().tolist()) == [202401, 202402, 202403, 202404, 202405, 202406, 202407, 202408, 202409, 202410, 202411, 202412]
    selected_codes = sorted(artifact.loc[artifact["month_key"] == 202401, "code"].astype(str).tolist())
    assert len(selected_codes) == 12
    assert selected_codes == sorted(manifest["selected_codes"])
    assert set(selected_codes).issubset({f"{1300 + idx:04d}" for idx in range(18)})
    for month_key, month_frame in artifact.groupby("month_key", sort=True):
        assert sorted(month_frame["code"].astype(str).tolist()) == selected_codes, month_key


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_build_generates_fidelity_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_build.duckdb"
    export_db = tmp_path / "export_build.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-build-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")

    dataset_dir = tradex_root / "event_image_dataset" / "datasets" / dataset_id
    manifest = read_json(dataset_dir / "dataset_manifest.json")
    samples = read_parquet_frame(dataset_dir / "samples.parquet")
    render_manifest = read_json(dataset_dir / "render_manifest.json")
    preview_manifest = read_json(dataset_dir / "preview_manifest.json")

    assert manifest["evaluation_bundle_id"] == FIDELITY_EVALUATION_BUNDLE_ID
    assert manifest["renderer_spec_id"] == "monthly_event_tripane_d60_w52_m36_512_v1"
    assert manifest["featureizer_spec_id"] == "rgb_flatten_48_lr_v1"
    assert render_manifest["strict_backend"] is True
    assert render_manifest["actual_backend"] == "agg"
    assert "warmup_contract" in render_manifest
    assert render_manifest["price_volume_split"] == {"price_ratio": 0.84, "volume_ratio": 0.16}
    assert samples["control_available"].all()
    assert samples["fidelity_available"].any()
    assert preview_manifest["selected_months"]
    assert preview_manifest["selected_sample_keys"]
    assert Path(preview_manifest["preview_contact_sheet_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_build_writes_restricted_universe_manifest(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_restricted.duckdb"
    export_db = tmp_path / "export_restricted.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-restricted-v12"
    restricted_universe_path = tmp_path / "topix100_monthly_membership.parquet"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    _write_restricted_universe_artifact(
        restricted_universe_path,
        month_keys=[202401, 202402, 202403, 202404, 202405, 202406, 202407, 202408, 202409, 202410, 202411, 202412],
        codes=[f"{1300 + idx:04d}" for idx in range(10)],
    )
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(
        source_db_path=str(source_db),
        export_db_path=str(export_db),
        dataset_id=dataset_id,
        start_month="202401",
        end_month="202412",
        renderer_backend="agg",
        restricted_universe_path=str(restricted_universe_path),
    )

    dataset_dir = tradex_root / "event_image_dataset" / "datasets" / dataset_id
    manifest = read_json(dataset_dir / "dataset_manifest.json")
    restricted_manifest = read_json(dataset_dir / "restricted_universe_manifest.json")
    samples = read_parquet_frame(dataset_dir / "samples.parquet")

    assert manifest["universe_definition"]["kind"] == "historical_restricted_membership"
    assert manifest["universe_definition"]["restricted_universe_name"] == "TOPIX100"
    assert manifest["artifact_paths"]["restricted_universe_manifest"] == str(dataset_dir / "restricted_universe_manifest.json")
    assert restricted_manifest["universe_name"] == "TOPIX100"
    assert restricted_manifest["source_name"] == "synthetic_topix100_fixture"
    assert restricted_manifest["source_version"] == "v1"
    assert restricted_manifest["source_uri"] == "file://synthetic-topix100"
    assert restricted_manifest["month_coverage_start"] == 202401
    assert restricted_manifest["month_coverage_end"] == 202412
    assert restricted_manifest["file_sha256"]
    assert set(samples["code"].astype(str).tolist()) <= {f"{1300 + idx:04d}" for idx in range(10)}


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_train_outputs_compare_artifact(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_train.duckdb"
    export_db = tmp_path / "export_train.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-train-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)

    dataset_dir = tradex_root / "event_image_dataset" / "datasets" / dataset_id
    baseline = read_json(dataset_dir / "baseline_metrics.json")
    numeric = read_json(dataset_dir / "numeric_baseline_metrics.json")
    compare = read_json(dataset_dir / "fidelity_compare.json")
    manifest = read_json(dataset_dir / "train_eval_manifest.json")
    predictions = read_parquet_frame(dataset_dir / "predictions.parquet")
    samples = read_parquet_frame(dataset_dir / "samples.parquet")
    test_predictions = predictions.loc[predictions["split"] == "test"].copy()
    test_samples = samples.loc[samples["split"] == "test"].copy()

    assert baseline["evaluation_bundle_id"] == FIDELITY_EVALUATION_BUNDLE_ID
    assert numeric["model_family"] == "numeric_only"
    assert manifest["featureizer_spec_id"] == "rgb_flatten_48_lr_v1"
    assert manifest["control_image_feature_size"] == 12
    assert compare["same_sample_keys"] is True
    assert compare["formal_compare_scope"] == "common eligible subset on test split only"
    assert compare["sample_key_definition"] == "as_of_date + code + label"
    assert compare["common_eligible_sample_count"] == len(test_predictions)
    assert compare["dropped_by_warmup_v1_2_count"] == len(test_samples) - len(test_predictions)
    assert compare["full_common_eligible_sample_count"] == len(predictions)
    assert compare["v1_1_image_metrics"]["test_sample_count"] == compare["v1_2_image_metrics"]["test_sample_count"]
    assert compare["v1_2_image_metrics"]["test_sample_count"] == compare["numeric_baseline_metrics"]["test_sample_count"]
    assert "delta_vs_v1_1" in compare
    assert "delta_vs_numeric" in compare
    assert "control_pred_prob_up" in predictions.columns
    assert "image_pred_prob_up" in predictions.columns
    assert "numeric_pred_prob_up" in predictions.columns
    assert set(predictions["split"].tolist()) <= {"train", "validation", "test"}
    assert predictions["sample_key"].is_unique


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_repro_run_writes_seed_isolated_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_repro.duckdb"
    export_db = tmp_path / "export_repro.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-repro-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    result = run_event_image_dataset_repro(dataset_id=dataset_id, seeds=[7, 11, 19], feature_size=48)

    repro_dir = Path(result["repro_dir"])
    manifest = read_json(repro_dir / "repro_manifest.json")
    summary = read_json(repro_dir / "repro_summary.json")
    report_path = repro_dir / "repro_report.md"

    assert manifest["seed_list"] == [7, 11, 19]
    assert manifest["same_sample_keys"] is True
    assert manifest["formal_compare_scope"] == "common eligible subset on test split only"
    assert summary["disposition_recommendation"] in {"keep_strengthened", "hold", "drop_reconsider"}
    assert len(summary["seed_results"]) == 3
    assert report_path.exists()
    common_counts = {int(item["common_eligible_sample_count"]) for item in summary["seed_results"]}
    dropped_counts = {int(item["dropped_by_warmup_v1_2_count"]) for item in summary["seed_results"]}
    assert len(common_counts) == 1
    assert len(dropped_counts) == 1
    for seed in (7, 11, 19):
        seed_dir = repro_dir / "seed_runs" / f"seed_{seed}"
        assert (seed_dir / "baseline_metrics.json").exists()
        assert (seed_dir / "numeric_baseline_metrics.json").exists()
        assert (seed_dir / "fidelity_compare.json").exists()
        assert (seed_dir / "predictions.parquet").exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_regime_analysis_writes_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_regime.duckdb"
    export_db = tmp_path / "export_regime.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-regime-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    result = analyze_event_image_dataset_regime(dataset_id=dataset_id)

    month_index = read_parquet_frame(Path(result["regime_month_index_path"]))
    compare = read_json(Path(result["regime_compare_path"]))
    manifest = read_json(Path(result["regime_gate_manifest_path"]))
    assert Path(result["regime_report_path"]).exists()
    assert manifest["formal_compare_scope"] == "common eligible subset on test split only"
    assert compare["formal_compare_scope"] == "common eligible subset on test split only"
    assert set(month_index["regime_tag"].tolist()) <= {
        "uptrend",
        "sideways_compression",
        "down_high_volatility",
        "rebound_onset",
    }
    assert len(month_index) == int(compare["test_month_count"])


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_decomposition_writes_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_pattern.duckdb"
    export_db = tmp_path / "export_pattern.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-pattern-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    winning_regime = next(
        (
            row["regime_tag"]
            for row in compare["month_level_results"]
            if sum(1 for key in ("winner_accuracy", "winner_top10_precision", "winner_long_short_spread") if row.get(key) == "image") >= 2
        ),
        None,
    )
    if winning_regime is None:
        first_row = compare["month_level_results"][0]
        first_row["winner_accuracy"] = "image"
        first_row["winner_top10_precision"] = "image"
        first_row["winner_long_short_spread"] = "image"
        Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
        winning_regime = str(first_row["regime_tag"])
    result = decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=winning_regime)

    artifact = read_json(Path(result["pattern_decomposition_path"]))
    assert Path(result["pattern_report_path"]).exists()
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["regime_tag"] == winning_regime
    assert artifact["winner_month_count"] >= 1
    assert Path(result["pattern_micro_features_path"]).exists()
    assert Path(result["pattern_micro_features_summary_path"]).exists()
    assert "trend_strength" in artifact
    assert "setup_quality" in artifact
    assert "tradability_risk" in artifact
    assert "candidate_rule" in artifact


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_analysis_batch_run_writes_summary(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_batch.duckdb"
    export_db = tmp_path / "export_batch.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-batch-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    result = run_event_image_dataset_analysis_batch(dataset_ids=[dataset_id], max_workers=1, refresh_train=False, refresh_repro=False, feature_size=48)

    summary = read_json(Path(result["summary_path"]))
    assert result["dataset_count"] == 1
    assert result["error_count"] == 0
    assert summary["dataset_ids"] == [dataset_id]
    assert len(summary["results"]) == 1


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_library_writes_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_library.duckdb"
    export_db = tmp_path / "export_library.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-library-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    regime_tags: list[str] = []
    for row in compare["month_level_results"]:
        wins = sum(1 for key in ("winner_accuracy", "winner_top10_precision", "winner_long_short_spread") if row.get(key) == "image")
        if wins >= 2 and str(row["regime_tag"]) not in regime_tags:
            regime_tags.append(str(row["regime_tag"]))
    if not regime_tags:
        first_row = compare["month_level_results"][0]
        first_row["winner_accuracy"] = "image"
        first_row["winner_top10_precision"] = "image"
        first_row["winner_long_short_spread"] = "image"
        Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
        regime_tags = [str(first_row["regime_tag"])]

    for regime_tag in regime_tags:
        decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    result = build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=regime_tags)

    artifact = read_json(Path(result["pattern_library_path"]))
    assert Path(result["pattern_library_report_path"]).exists()
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["compare_contract"]["same_sample_keys"] is True
    assert len(artifact["pattern_candidates"]) == len(regime_tags)


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_boundary_writes_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_boundary.duckdb"
    export_db = tmp_path / "export_boundary.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-boundary-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    month_rows = compare["month_level_results"]
    if len(month_rows) < 2:
        pytest.skip("synthetic fixture did not produce enough months for boundary compare")
    month_rows[0]["regime_tag"] = "rebound_onset"
    month_rows[0]["winner_accuracy"] = "image"
    month_rows[0]["winner_top10_precision"] = "image"
    month_rows[0]["winner_long_short_spread"] = "image"
    month_rows[1]["regime_tag"] = "uptrend"
    month_rows[1]["winner_accuracy"] = "image"
    month_rows[1]["winner_top10_precision"] = "image"
    month_rows[1]["winner_long_short_spread"] = "image"
    compare["regime_summary"] = [
        {
            "regime_tag": "rebound_onset",
            "regime_label": "反発初動",
            "month_count": 1,
            "image_accuracy_mean": 0.6,
            "numeric_accuracy_mean": 0.4,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.6,
            "numeric_top10_precision_mean": 0.4,
            "image_long_short_spread_mean": 0.1,
            "numeric_long_short_spread_mean": -0.1,
        },
        {
            "regime_tag": "uptrend",
            "regime_label": "上昇トレンド",
            "month_count": 1,
            "image_accuracy_mean": 0.55,
            "numeric_accuracy_mean": 0.45,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.55,
            "numeric_top10_precision_mean": 0.45,
            "image_long_short_spread_mean": 0.05,
            "numeric_long_short_spread_mean": -0.02,
        },
    ]
    Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    for regime_tag in ("rebound_onset", "uptrend"):
        decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    result = build_event_image_pattern_boundary(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend", max_workers=1)

    artifact = read_json(Path(result["pattern_boundary_compare_path"]))
    assert Path(result["pattern_boundary_compare_report_path"]).exists()
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["primary_regime"] == "rebound_onset"
    assert artifact["comparison_regime"] == "uptrend"
    assert "candidate_boundary_rule" in artifact


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_gating_writes_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_gating.duckdb"
    export_db = tmp_path / "export_gating.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-gating-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    month_rows = compare["month_level_results"]
    if len(month_rows) < 2:
        pytest.skip("synthetic fixture did not produce enough months for gating compare")
    month_rows[0]["regime_tag"] = "rebound_onset"
    month_rows[0]["winner_accuracy"] = "image"
    month_rows[0]["winner_top10_precision"] = "image"
    month_rows[0]["winner_long_short_spread"] = "image"
    month_rows[1]["regime_tag"] = "uptrend"
    month_rows[1]["winner_accuracy"] = "image"
    month_rows[1]["winner_top10_precision"] = "image"
    month_rows[1]["winner_long_short_spread"] = "image"
    compare["regime_summary"] = [
        {
            "regime_tag": "rebound_onset",
            "regime_label": "反発初動",
            "month_count": 1,
            "image_accuracy_mean": 0.6,
            "numeric_accuracy_mean": 0.4,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.6,
            "numeric_top10_precision_mean": 0.4,
            "image_long_short_spread_mean": 0.1,
            "numeric_long_short_spread_mean": -0.1,
        },
        {
            "regime_tag": "uptrend",
            "regime_label": "上昇トレンド",
            "month_count": 1,
            "image_accuracy_mean": 0.55,
            "numeric_accuracy_mean": 0.45,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.55,
            "numeric_top10_precision_mean": 0.45,
            "image_long_short_spread_mean": 0.05,
            "numeric_long_short_spread_mean": -0.02,
        },
    ]
    Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    for regime_tag in ("rebound_onset", "uptrend"):
        decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    build_event_image_pattern_boundary(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend", max_workers=1)

    result = build_event_image_pattern_gating(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    artifact = read_json(Path(result["pattern_gating_rule_path"]))
    assert Path(result["pattern_gating_rule_report_path"]).exists()
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["primary_regime"] == "rebound_onset"
    assert artifact["comparison_regime"] == "uptrend"
    assert "candidate_gate_rule" in artifact
    assert "gate_hit_counts" in artifact


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_combo_writes_artifacts(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_combo.duckdb"
    export_db = tmp_path / "export_combo.duckdb"
    tradex_root = tmp_path / "tradex_root"
    dataset_id = "monthly-event-combo-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    month_rows = compare["month_level_results"]
    if len(month_rows) < 2:
        pytest.skip("synthetic fixture did not produce enough months for combo compare")
    month_rows[0]["regime_tag"] = "rebound_onset"
    month_rows[0]["winner_accuracy"] = "image"
    month_rows[0]["winner_top10_precision"] = "image"
    month_rows[0]["winner_long_short_spread"] = "image"
    month_rows[1]["regime_tag"] = "uptrend"
    month_rows[1]["winner_accuracy"] = "image"
    month_rows[1]["winner_top10_precision"] = "image"
    month_rows[1]["winner_long_short_spread"] = "image"
    compare["regime_summary"] = [
        {
            "regime_tag": "rebound_onset",
            "regime_label": "反発初動",
            "month_count": 1,
            "image_accuracy_mean": 0.6,
            "numeric_accuracy_mean": 0.4,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.6,
            "numeric_top10_precision_mean": 0.4,
            "image_long_short_spread_mean": 0.1,
            "numeric_long_short_spread_mean": -0.1,
        },
        {
            "regime_tag": "uptrend",
            "regime_label": "上昇トレンド",
            "month_count": 1,
            "image_accuracy_mean": 0.55,
            "numeric_accuracy_mean": 0.45,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.55,
            "numeric_top10_precision_mean": 0.45,
            "image_long_short_spread_mean": 0.05,
            "numeric_long_short_spread_mean": -0.02,
        },
    ]
    Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    for regime_tag in ("rebound_onset", "uptrend"):
        decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    build_event_image_pattern_boundary(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend", max_workers=1)
    build_event_image_pattern_gating(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")

    result = build_event_image_pattern_combo_rules(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    artifact = read_json(Path(result["pattern_combo_rules_path"]))
    assert Path(result["pattern_combo_rules_report_path"]).exists()
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["primary_regime"] == "rebound_onset"
    assert artifact["comparison_regime"] == "uptrend"
    assert "combo_results" in artifact
    assert "recommended_primary_combo" in artifact
    library_result = build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=["rebound_onset", "uptrend"])
    library_artifact = read_json(Path(library_result["pattern_library_path"]))
    assert library_artifact["source_artifacts"]["combo_compare"] is not None
    assert all("combo_rule_artifact_path" in row for row in library_artifact["pattern_candidates"])


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_adoption_writes_bridge_snapshot(monkeypatch, tmp_path) -> None:
    source_db = tmp_path / "source_adoption.duckdb"
    export_db = tmp_path / "export_adoption.duckdb"
    tradex_root = tmp_path / "tradex_root"
    bridge_root = tmp_path / "bridge"
    dataset_id = "monthly-event-adoption-v12"
    _seed_event_source_db(source_db)
    _seed_event_export_db_from_source(source_db=source_db, export_db=export_db)
    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(source_db))
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(bridge_root))

    build_event_image_dataset(source_db_path=str(source_db), export_db_path=str(export_db), dataset_id=dataset_id, start_month="202401", end_month="202412", renderer_backend="agg")
    train_event_image_dataset(dataset_id=dataset_id, seed=7, feature_size=48)
    compare_result = analyze_event_image_dataset_regime(dataset_id=dataset_id)
    compare = read_json(Path(compare_result["regime_compare_path"]))
    month_rows = compare["month_level_results"]
    if len(month_rows) < 2:
        pytest.skip("synthetic fixture did not produce enough months for adoption flow")
    month_rows[0]["regime_tag"] = "rebound_onset"
    month_rows[0]["winner_accuracy"] = "image"
    month_rows[0]["winner_top10_precision"] = "image"
    month_rows[0]["winner_long_short_spread"] = "image"
    month_rows[1]["regime_tag"] = "uptrend"
    month_rows[1]["winner_accuracy"] = "image"
    month_rows[1]["winner_top10_precision"] = "image"
    month_rows[1]["winner_long_short_spread"] = "image"
    compare["regime_summary"] = [
        {
            "regime_tag": "rebound_onset",
            "regime_label": "蜿咲匱蛻晏虚",
            "month_count": 1,
            "image_accuracy_mean": 0.6,
            "numeric_accuracy_mean": 0.4,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.6,
            "numeric_top10_precision_mean": 0.4,
            "image_long_short_spread_mean": 0.1,
            "numeric_long_short_spread_mean": -0.1,
        },
        {
            "regime_tag": "uptrend",
            "regime_label": "荳頑・繝医Ξ繝ｳ繝・",
            "month_count": 1,
            "image_accuracy_mean": 0.55,
            "numeric_accuracy_mean": 0.45,
            "image_accuracy_win_months": 1,
            "numeric_accuracy_win_months": 0,
            "tie_accuracy_months": 0,
            "image_top10_precision_mean": 0.55,
            "numeric_top10_precision_mean": 0.45,
            "image_long_short_spread_mean": 0.05,
            "numeric_long_short_spread_mean": -0.02,
        },
    ]
    Path(compare_result["regime_compare_path"]).write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")
    for regime_tag in ("rebound_onset", "uptrend"):
        decompose_event_image_pattern(dataset_id=dataset_id, regime_tag=regime_tag)
    build_event_image_pattern_boundary(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend", max_workers=1)
    build_event_image_pattern_gating(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    build_event_image_pattern_combo_rules(dataset_id=dataset_id, primary_regime="rebound_onset", comparison_regime="uptrend")
    build_event_image_pattern_library(dataset_id=dataset_id, regime_tags=["rebound_onset", "uptrend"])
    gating_path = tradex_root / "event_image_dataset" / "datasets" / dataset_id / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
    gating = read_json(gating_path)
    gating["candidate_gate_rule"]["rebound_onset"]["price_vs_ma120_min"] = -1.0
    gating["candidate_gate_rule"]["rebound_onset"]["distance_from_60d_high_range"] = [-1.0, 0.0]
    gating["candidate_gate_rule"]["rebound_onset"]["realized_vol20_min"] = 0.0
    gating_path.write_text(json.dumps(gating, ensure_ascii=False, indent=2), encoding="utf-8")

    result = build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["adoption_artifact_path"]))
    bridge_snapshot = read_json(Path(result["bridge_snapshot_path"]))
    bridge_manifest = read_json(Path(result["bridge_manifest_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["pattern"] == "rebound_onset"
    assert artifact["fit_score_policy"]["threshold_for_bonus"] == 0.50
    assert artifact["tag_candidate_count"] == artifact["candidate_counts"]["tag_candidate_count"]
    assert artifact["bonus_candidate_count"] == artifact["candidate_counts"]["bonus_candidate_count"]
    assert sorted(artifact["bonus_codes"]) == sorted(artifact["bonus_score_map"].keys())
    assert bridge_snapshot["schema_version"] == "tradex_rebound_onset_research_prior_v1"
    assert bridge_snapshot["strategy_id"] == "tradex_rebound_onset_aux_v1"
    assert bridge_snapshot["up"]["bonus_cap"] == 0.03
    assert "fit_score_map" in bridge_snapshot["up"]
    assert "pattern_tag_map" in bridge_snapshot["up"]
    assert "adoption_reason_map" in bridge_snapshot["up"]
    assert bridge_snapshot["up"]["fit_score_map"] == artifact["bonus_score_map"]
    assert bridge_manifest["artifacts"]["research_prior_snapshot.json"]["source_id"] == result["run_id"]


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_adoption_compare_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-adoption-compare-v12",
    )
    gating_path = tradex_root / "event_image_dataset" / "datasets" / dataset_id / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
    gating = read_json(gating_path)
    gating["candidate_gate_rule"]["rebound_onset"]["price_vs_ma120_min"] = -1.0
    gating["candidate_gate_rule"]["rebound_onset"]["distance_from_60d_high_range"] = [-1.0, 0.0]
    gating["candidate_gate_rule"]["rebound_onset"]["realized_vol20_min"] = 0.0
    gating_path.write_text(json.dumps(gating, ensure_ascii=False, indent=2), encoding="utf-8")

    result = build_event_image_pattern_adoption_compare(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_adoption_compare_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["variant_results"]) == 3
    thresholds = {row["variant_name"]: row["threshold_for_bonus"] for row in artifact["variant_results"]}
    assert thresholds["v1_0_strict_bonus"] == pytest.approx(0.60)
    assert thresholds["v1_1_live_bonus"] == pytest.approx(0.50)
    assert thresholds["v1_2_soft_bonus"] == pytest.approx(0.45)
    counts = {row["variant_name"]: row["bonus_candidate_count"] for row in artifact["variant_results"]}
    assert counts["v1_0_strict_bonus"] <= counts["v1_1_live_bonus"] <= counts["v1_2_soft_bonus"]
    assert artifact["recommended_variant"]["variant_name"] in {"v1_0_strict_bonus", "v1_1_live_bonus", "v1_2_soft_bonus"}


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_breadth_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-breadth-v12",
    )
    gating_path = tradex_root / "event_image_dataset" / "datasets" / dataset_id / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
    gating = read_json(gating_path)
    gating["candidate_gate_rule"]["rebound_onset"]["price_vs_ma120_min"] = -1.0
    gating["candidate_gate_rule"]["rebound_onset"]["distance_from_60d_high_range"] = [-1.0, 0.0]
    gating["candidate_gate_rule"]["rebound_onset"]["realized_vol20_min"] = 0.0
    gating_path.write_text(json.dumps(gating, ensure_ascii=False, indent=2), encoding="utf-8")

    result = build_event_image_pattern_breadth(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_breadth_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["candidate_results"]) == 3
    assert artifact["recommended_candidate"] in {"wick_bias_relaxed", "coil_bias_relaxed", "slope_dual_keep"}


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_sequence_combo_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-sequence-combo-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)

    result = build_event_image_pattern_sequence_combo(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_sequence_combo_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["combo_results"]) == 4
    assert artifact["recommended_candidate"]["candidate_name"] in {
        "tail_hold_2bar",
        "two_red_reversal_3bar",
        "nr_bull_break_2bar",
        "higher_lows_support_3bar",
    }


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_adoption_policy_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-adoption-policy-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)

    result = build_event_image_pattern_adoption_policy(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_adoption_policy_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["variant_results"]) == 3
    assert artifact["recommended_variant"]["variant_name"] in {
        "baseline_live",
        "soft_bonus_only",
        "soft_bonus_plus_best_sequence",
    }


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_playbook_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-playbook-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")

    result = build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_playbook_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["variant_results"]) == 3
    assert artifact["recommended_playbook_variant"]["variant_name"] in {
        "environment_heavy",
        "setup_heavy",
        "balanced_playbook",
    }
    assert artifact["environment_features"]
    assert artifact["setup_features"]
    assert artifact["veto_features"]
    assert "feature_effects" in artifact
    assert Path(result["pattern_playbook_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_playbook_relax_compare_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-playbook-relax-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")

    result = build_event_image_pattern_playbook_relax_compare(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_playbook_relax_compare_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["baseline_playbook_variant"]["variant_name"] == "balanced_playbook"
    assert len(artifact["relax_results"]) == 2
    assert artifact["recommended_relax_axis"]["variant_name"] in {"environment_relax", "setup_relax"}
    assert Path(result["pattern_playbook_relax_compare_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_playbook_threshold_compare_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-playbook-threshold-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")

    result = build_event_image_pattern_playbook_threshold_compare(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_playbook_threshold_compare_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert artifact["fixed_playbook_variant"]["variant_name"] == "balanced_playbook"
    assert len(artifact["threshold_results"]) == 5
    assert artifact["recommended_threshold_variant"]["variant_name"] in {
        "current_balanced",
        "cutoff_055",
        "cutoff_050",
        "cutoff_045",
        "cutoff_040",
    }
    assert Path(result["pattern_playbook_threshold_compare_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_veto_compare_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-veto-compare-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")

    result = build_event_image_pattern_veto_compare(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_veto_compare_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["compare_results"]) == 2
    assert artifact["recommended_policy"] in {"core_gate_plus_veto", "analysis_only", "core_gate_only"}
    assert Path(result["pattern_veto_compare_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_veto_ablation_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-veto-ablation-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")

    result = build_event_image_pattern_veto_ablation(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_veto_ablation_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["ablation_results"]) == 6
    assert artifact["recommended_veto_policy"] in {"all_veto_keep", "drop_one_veto_rule", "analysis_only"}
    assert isinstance(artifact["culprit_veto_rules"], list)
    assert Path(result["pattern_veto_ablation_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_pattern_veto_thin_liquidity_compare_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-veto-thin-liquidity-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_veto_ablation(dataset_id=dataset_id, pattern="rebound_onset")

    result = build_event_image_pattern_veto_thin_liquidity_compare(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_veto_thin_liquidity_compare_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["compare_results"]) == 6
    assert artifact["recommended_thin_liquidity_policy"] in {
        "keep_current",
        "analysis_only",
        "weaken_to_weak_1",
        "weaken_to_weak_2",
        "weaken_to_weak_3",
        "weaken_to_off",
    }
    assert Path(result["pattern_veto_thin_liquidity_compare_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_selection_contract_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-selection-contract-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)
    build_event_image_pattern_adoption(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_playbook(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_veto_ablation(dataset_id=dataset_id, pattern="rebound_onset")
    build_event_image_pattern_veto_thin_liquidity_compare(dataset_id=dataset_id, pattern="rebound_onset")

    validation_dir = tradex_root / "reports" / "rebound_full_validation" / "rebound_full_validation_test"
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / "rebound_full_validation_summary.json").write_text(
        json.dumps(
            {
                "decision": "fix_holdings_before_policy_change",
                "recommended_policy": "holdings_fix_first",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    backtest_dir = tradex_root / "reports" / "ranking_backtests" / "ranking_backtest_test"
    backtest_dir.mkdir(parents=True, exist_ok=True)
    (backtest_dir / "ranking_backtest_summary.json").write_text(
        json.dumps(
            {
                "disposition": "watch",
                "raw_ranking": {
                    "top10": {"lift_vs_all_ranked": 0.01},
                    "top10_entryQualified": {"lift_vs_all_ranked": 0.01},
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    result = build_event_image_pattern_selection_contract(dataset_id=dataset_id, pattern="rebound_onset")

    artifact = read_json(Path(result["pattern_selection_contract_path"]))
    assert artifact["formal_compare_scope"] == "common eligible subset on test split only"
    assert len(artifact["selection_stages"]) == 8
    assert artifact["primary_leak_stage"] in {
        "veto_current",
        "thin_liquidity_policy",
        "ranking_selection",
        "entry_qualification",
    }
    assert artifact["recommended_next_action"] in {
        "promote_weak_1_to_live_monitor",
        "keep_current_veto_and_revisit_ranking_selection",
        "keep_veto_analysis_only_and_revisit_entry_qualification",
        "redirect_to_toredex_holdings_fix",
    }
    assert Path(result["pattern_selection_contract_report_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_rebound_live_monitor_writes_artifacts(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-rebound-monitor-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)

    result = build_event_image_rebound_live_monitor(dataset_id=dataset_id, days=5)

    artifact = read_json(Path(result["rebound_live_monitor_path"]))
    assert artifact["baseline_variant"] == "baseline_live"
    assert "candidate" in artifact["summary"]
    assert "baseline" in artifact["summary"]
    assert Path(result["rebound_live_monitor_panel_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_rebound_v3_round_writes_checkpoint(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-rebound-v3-v12",
    )
    _relax_rebound_gating(tradex_root, dataset_id)

    diagnosis_root = tmp_path / "diagnosis"
    diagnosis_root.mkdir(parents=True, exist_ok=True)
    diagnosis_json = diagnosis_root / "toredex_policy_diagnosis.json"
    diagnosis_json.write_text(
        json.dumps(
            {
                "primary_failure_axis": "turnover",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    def _fake_run_toredex_policy_diagnosis(**_kwargs):
        return {
            "toredex_policy_diagnosis_path": str(diagnosis_json),
            "toredex_policy_diagnosis_report_path": str(diagnosis_root / "toredex_policy_diagnosis.md"),
            "primary_failure_axis": "turnover",
        }

    monkeypatch.setattr(
        "app.backend.services.analysis.toredex_policy_diagnosis_service.run_toredex_policy_diagnosis",
        _fake_run_toredex_policy_diagnosis,
    )

    result = run_event_image_dataset_rebound_v3_round(
        dataset_id=dataset_id,
        max_workers=2,
        monitor_days=5,
    )

    library = read_json(Path(result["pattern_library_path"]))
    rebound_row = next(row for row in library["pattern_candidates"] if str(row["regime_tag"]) == "rebound_onset")
    assert rebound_row["recommended_adoption_policy_variant"] in {"baseline_live", "soft_bonus_only", "soft_bonus_plus_best_sequence"}
    assert rebound_row["primary_failure_axis"] == "turnover"
    assert Path(result["checkpoint_path"]).exists()


@pytest.mark.skipif(not strict_agg_available(), reason="matplotlib/agg unavailable")
def test_event_image_dataset_rebound_robustness_batch_and_multi_round(monkeypatch, tmp_path) -> None:
    tradex_root, _bridge_root, dataset_id = _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-robust-a-v12",
    )
    _prepare_rebound_pattern_stack(
        monkeypatch,
        tmp_path,
        dataset_id="monthly-event-robust-b-v12",
    )
    for gating_dataset_id in (dataset_id, "monthly-event-robust-b-v12"):
        gating_path = tradex_root / "event_image_dataset" / "datasets" / gating_dataset_id / "pattern_gating_rule_rebound_onset_vs_uptrend.json"
        gating = read_json(gating_path)
        gating["candidate_gate_rule"]["rebound_onset"]["price_vs_ma120_min"] = -1.0
        gating["candidate_gate_rule"]["rebound_onset"]["distance_from_60d_high_range"] = [-1.0, 0.0]
        gating["candidate_gate_rule"]["rebound_onset"]["realized_vol20_min"] = 0.0
        gating_path.write_text(json.dumps(gating, ensure_ascii=False, indent=2), encoding="utf-8")

    robustness = run_event_image_dataset_robustness_batch(
        dataset_ids=[dataset_id, "monthly-event-robust-b-v12"],
        pattern="rebound_onset",
        max_workers=2,
    )
    summary = read_json(Path(robustness["robustness_batch_summary_path"]))
    assert len(summary["results"]) == 2

    round_result = run_event_image_dataset_rebound_multi_research_round(
        dataset_id=dataset_id,
        robustness_dataset_ids=[dataset_id, "monthly-event-robust-b-v12"],
        max_workers=2,
    )
    library = read_json(Path(round_result["pattern_library_path"]))
    rebound_row = next(row for row in library["pattern_candidates"] if str(row["regime_tag"]) == "rebound_onset")
    assert rebound_row["recommended_adoption_variant"] in {"v1_0_strict_bonus", "v1_1_live_bonus", "v1_2_soft_bonus"}
    assert Path(round_result["checkpoint_path"]).exists()
