from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from external_analysis.image_rerank.artifacts import read_json

pytestmark = pytest.mark.integration


def _build_real_slice_source_db(*, source_db: Path, subset_source_db: Path) -> tuple[list[str], list[int]]:
    conn = duckdb.connect(str(source_db), read_only=True)
    try:
        raw_dates = [int(row[0]) for row in conn.execute("SELECT DISTINCT date FROM daily_bars ORDER BY date DESC LIMIT 130").fetchall()]
        if not raw_dates:
            raise RuntimeError("daily_bars has no dates")
        dates = sorted(int(datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y%m%d")) for value in raw_dates)
        codes = [
            str(row[0])
            for row in conn.execute(
                """
                WITH last_dates AS (
                    SELECT DISTINCT date
                    FROM daily_bars
                    ORDER BY date DESC
                    LIMIT 130
                )
                SELECT code
                FROM daily_bars
                WHERE date IN (SELECT date FROM last_dates)
                GROUP BY code
                HAVING COUNT(*) = 130
                ORDER BY code
                LIMIT 20
                """
            ).fetchall()
        ]
        if not codes:
            raise RuntimeError("no codes with full 130-day coverage")
        code_sql = ", ".join([f"'{code}'" for code in codes])
        date_sql = ", ".join([str(value) for value in raw_dates])
        daily_bars_df = conn.execute(
            f"SELECT * FROM daily_bars WHERE code IN ({code_sql}) AND date IN ({date_sql}) ORDER BY code, date"
        ).fetchdf()
        daily_ma_df = conn.execute(
            f"SELECT * FROM daily_ma WHERE code IN ({code_sql}) AND date IN ({date_sql}) ORDER BY code, date"
        ).fetchdf()
        feature_df = conn.execute(
            f"SELECT * FROM feature_snapshot_daily WHERE code IN ({code_sql}) AND dt IN ({date_sql}) ORDER BY code, dt"
        ).fetchdf()
    finally:
        conn.close()

    if subset_source_db.exists():
        subset_source_db.unlink()
    subset_conn = duckdb.connect(str(subset_source_db))
    try:
        subset_conn.register("daily_bars_df", daily_bars_df)
        subset_conn.execute("CREATE TABLE daily_bars AS SELECT * FROM daily_bars_df")
        subset_conn.unregister("daily_bars_df")
        subset_conn.register("daily_ma_df", daily_ma_df)
        subset_conn.execute("CREATE TABLE daily_ma AS SELECT * FROM daily_ma_df")
        subset_conn.unregister("daily_ma_df")
        subset_conn.register("feature_df", feature_df)
        subset_conn.execute("CREATE TABLE feature_snapshot_daily AS SELECT * FROM feature_df")
        subset_conn.unregister("feature_df")
        subset_conn.execute("CHECKPOINT")
    finally:
        subset_conn.close()
    return codes, dates


def _run_external_analysis(*args: str, cwd: Path) -> str:
    command = ["python", "-m", "external_analysis", *args]
    completed = subprocess.run(command, cwd=str(cwd), capture_output=True, text=True)
    if completed.returncode != 0:
        raise AssertionError(
            json.dumps(
                {
                    "command": command,
                    "returncode": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    return completed.stdout


def test_image_rerank_real_slice_integration_verify(monkeypatch, tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_db = repo_root / ".local" / "meemee" / "research_db" / "stocks_research_20230101_20260226.duckdb"
    if not source_db.exists():
        pytest.skip("real source db is unavailable")

    subset_source_db = tmp_path / "real_slice_source.duckdb"
    export_db = tmp_path / "real_slice_export.duckdb"
    tradex_root = tmp_path / "tradex_root"
    label_db = tmp_path / "real_slice_label.duckdb"
    result_db = tmp_path / "real_slice_result.duckdb"
    ops_db = tmp_path / "real_slice_ops.duckdb"

    if tradex_root.exists():
        shutil.rmtree(tradex_root)
    codes, dates = _build_real_slice_source_db(source_db=source_db, subset_source_db=subset_source_db)
    assert len(codes) == 20
    assert len(dates) == 130

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setenv("STOCKS_DB_PATH", str(subset_source_db))
    monkeypatch.setenv("MEEMEE_RESULT_DB_PATH", str(result_db))

    for argv in [
        ["external_analysis", "init-result-db", "--db-path", str(result_db)],
        ["external_analysis", "init-export-db", "--db-path", str(export_db)],
        ["external_analysis", "init-label-db", "--db-path", str(label_db)],
        ["external_analysis", "init-ops-db", "--db-path", str(ops_db)],
        ["external_analysis", "export-sync", "--source-db-path", str(subset_source_db), "--export-db-path", str(export_db)],
        ["external_analysis", "label-build", "--export-db-path", str(export_db), "--label-db-path", str(label_db)],
    ]:
        _run_external_analysis(*argv[1:], cwd=repo_root)

    as_of_date = dates[-21]
    stdout = _run_external_analysis(
        "image-rerank-run",
        "--export-db-path",
        str(export_db),
        "--as-of-date",
        str(as_of_date),
        "--run-id",
        "image-rerank-real-slice-integration",
        "--top-k",
        "10",
        "--block-size-days",
        "20",
        "--embargo-days",
        "5",
        "--feature-lookback-days",
        "30",
        "--label-horizon-days",
        "5",
        "--renderer-backend",
        "agg",
        cwd=repo_root,
    )
    assert stdout

    run_path = tradex_root / "image_rerank" / "runs" / "image-rerank-real-slice-integration" / "run.json"
    split_path = tradex_root / "image_rerank" / "runs" / "image-rerank-real-slice-integration" / "manifests" / "split.json"
    compare_path = tradex_root / "image_rerank" / "runs" / "image-rerank-real-slice-integration" / "outputs" / "phase3_compare.json"

    run_json = read_json(run_path)
    split_json = read_json(split_path)
    compare_json = read_json(compare_path)

    assert run_json["schema_version"] == "tradex_image_rerank_run_v1"
    assert run_json["split_artifact_uri"].endswith("split.json")
    assert run_json["verify_profile"] == "smoke"
    assert split_json["blocks"][0]["block_start_index"] == 0
    assert split_json["boundary_checks"][0]["protected_block_index"] == 1
    assert split_json["boundary_checks"][0]["reason_codes"]
    assert set(split_json["reason_counts"]) == {
        "feature_overlap",
        "label_overlap",
        "feature_and_label_overlap",
        "embargo_only",
    }
    assert compare_json["metrics"]["top_k_uplift"] is not None
    assert compare_json["metrics"]["bad_pick_removal"] is not None
    assert compare_json["metrics"]["changed_top10_count"] is not None
    assert compare_json["verify_profile"] == "smoke"
    assert compare_json["readout"]["dropped_codes"]
    assert compare_json["readout"]["false_veto_count"] >= 0
    assert "fusion_sweep" in compare_json
    assert set(compare_json["fusion_sweep"]["modes"]) == {"rank_improver", "veto_helper"}
    assert compare_json["metrics"]["base_top_codes"]
    assert compare_json["metrics"]["fused_top_codes"]
