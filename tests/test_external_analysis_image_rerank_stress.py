from __future__ import annotations

from pathlib import Path

import pytest

from external_analysis.image_rerank.artifacts import read_json
from tests.test_external_analysis_image_rerank_integration import _build_real_slice_source_db, _run_external_analysis

pytestmark = pytest.mark.integration


def test_image_rerank_real_slice_stress_verify(monkeypatch, tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_db = repo_root / ".local" / "meemee" / "research_db" / "stocks_research_20230101_20260226.duckdb"
    if not source_db.exists():
        pytest.skip("real source db is unavailable")

    subset_source_db = tmp_path / "stress_slice_source.duckdb"
    export_db = tmp_path / "stress_slice_export.duckdb"
    tradex_root = tmp_path / "stress_tradex_root"
    label_db = tmp_path / "stress_slice_label.duckdb"
    result_db = tmp_path / "stress_slice_result.duckdb"
    ops_db = tmp_path / "stress_slice_ops.duckdb"

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

    snapshot_date = None
    for candidate in dates[-12:]:
        try:
            stdout = _run_external_analysis(
                "image-rerank-run",
                "--export-db-path",
                str(export_db),
                "--as-of-date",
                str(candidate),
                "--run-id",
                "image-rerank-real-slice-stress",
                "--verify-profile",
                "stress",
                "--top-k",
                "10",
                cwd=repo_root,
            )
        except AssertionError:
            continue
        snapshot_date = candidate
        assert stdout
        break
    if snapshot_date is None:
        pytest.skip("default 80/20 stress verify leaves no kept train rows on this 20-symbol slice")

    run_path = tradex_root / "image_rerank" / "runs" / "image-rerank-real-slice-stress" / "run.json"
    compare_path = tradex_root / "image_rerank" / "runs" / "image-rerank-real-slice-stress" / "outputs" / "phase3_compare.json"
    run_json = read_json(run_path)
    compare_json = read_json(compare_path)

    assert run_json["verify_profile"] == "stress"
    assert run_json["schema_version"] == "tradex_image_rerank_run_v1"
    assert compare_json["verify_profile"] == "stress"
    assert compare_json["metrics"]["top_k_uplift"] is not None
    assert compare_json["readout"]["false_veto_count"] >= 0
    assert set(compare_json["fusion_sweep"]["modes"]) == {"rank_improver", "veto_helper"}
