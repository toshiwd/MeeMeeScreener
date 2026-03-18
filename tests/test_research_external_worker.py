from __future__ import annotations

import os
from pathlib import Path
import sys

import duckdb
import pandas as pd
import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.bridge import export_bridge_run, export_bridge_study
from research.publish import run_publish
from research.source_sync import resolve_source_db_path, sync_source_mirror
from research.storage import ResearchPaths, read_csv, read_json, write_csv, write_json
from research.study_storage import study_paths


def _build_paths(tmp_path: Path) -> ResearchPaths:
    return ResearchPaths.build(
        repo_root=tmp_path,
        research_home=tmp_path / "research_home",
        workspace_root=tmp_path / "workspace",
        published_root=tmp_path / "legacy_published",
    )


def _create_source_db(path: Path) -> Path:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE daily_bars (date DATE, code VARCHAR, close DOUBLE)")
        conn.execute("INSERT INTO daily_bars VALUES ('2024-01-31', '1001', 123.4), ('2024-02-01', '2002', 234.5)")
        conn.execute("CREATE TABLE industry_master (code VARCHAR, sector33_code VARCHAR, sector33_name VARCHAR)")
        conn.execute("INSERT INTO industry_master VALUES ('1001', '10', 'Tech'), ('2002', '20', 'Retail')")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
    return path


def _create_run_artifacts(paths: ResearchPaths, run_id: str) -> None:
    run_dir = paths.run_dir(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        run_dir / "manifest.json",
        {
            "run_id": run_id,
            "created_at": "2026-03-12T00:00:00Z",
            "model_version": "m1",
            "feature_version": "f1",
            "label_version": "l1",
        },
    )
    write_json(
        run_dir / "evaluation.json",
        {
            "pareto": {"is_pareto": True},
            "metrics_by_phase": {
                "test": {
                    "overall": {"months": 12, "return_at20": 0.02, "risk_mae_p90": 0.05},
                    "long": {"return_at20": 0.03},
                    "short": {"return_at20": -0.02},
                }
            },
        },
    )
    frame_long = pd.DataFrame(
        [
            {"asof_date": "2026-03-10", "code": "1001", "score": 0.9, "pred_return": 0.02, "pred_prob_tp": 0.6, "risk_dn": 0.1, "phase": "inference"},
            {"asof_date": "2026-03-10", "code": "2002", "score": 0.8, "pred_return": 0.01, "pred_prob_tp": 0.55, "risk_dn": 0.1, "phase": "inference"},
        ]
    )
    frame_short = pd.DataFrame(
        [
            {"asof_date": "2026-03-10", "code": "3003", "score": 0.91, "pred_return": -0.02, "pred_prob_tp": 0.62, "risk_dn": 0.1, "phase": "inference"},
            {"asof_date": "2026-03-10", "code": "4004", "score": 0.82, "pred_return": -0.01, "pred_prob_tp": 0.58, "risk_dn": 0.1, "phase": "inference"},
        ]
    )
    write_csv(run_dir / "top20_long.csv", frame_long)
    write_csv(run_dir / "top20_short.csv", frame_short)


def _create_study_artifacts(paths: ResearchPaths, study_id: str) -> None:
    spaths = study_paths(paths, study_id)
    spaths["root"].mkdir(parents=True, exist_ok=True)
    write_json(
        spaths["adopted_hypotheses"],
        {
            "study_id": study_id,
            "adopted": [
                {
                    "trial_id": "trial_001",
                    "timeframe": "daily",
                    "family": "down_cont",
                    "study_score": 1.23,
                }
            ],
        },
    )


def test_resolve_source_db_path_precedence(monkeypatch, tmp_path: Path) -> None:
    explicit = tmp_path / "explicit.duckdb"
    env_source = tmp_path / "env_source.duckdb"
    env_stock = tmp_path / "env_stock.duckdb"
    fallback_root = tmp_path / "localappdata"

    monkeypatch.setenv("MEEMEE_SOURCE_DB", str(env_source))
    monkeypatch.setenv("STOCKS_DB_PATH", str(env_stock))
    monkeypatch.setenv("LOCALAPPDATA", str(fallback_root))

    assert resolve_source_db_path(str(explicit)) == explicit.resolve()
    assert resolve_source_db_path() == env_source.resolve()

    monkeypatch.delenv("MEEMEE_SOURCE_DB")
    assert resolve_source_db_path() == env_stock.resolve()

    monkeypatch.delenv("STOCKS_DB_PATH")
    assert resolve_source_db_path() == (fallback_root / "MeeMeeScreener" / "data" / "stocks.duckdb").resolve()


def test_sync_source_mirror_copies_tables_and_skips_unchanged(tmp_path: Path) -> None:
    paths = _build_paths(tmp_path)
    source_db = _create_source_db(tmp_path / "source.duckdb")

    first = sync_source_mirror(paths, source_db=str(source_db), force=False)
    assert first["changed"] is True
    assert paths.current_mirror_db.exists()
    manifest = read_json(paths.current_mirror_manifest)
    assert manifest["table_count"] == 2

    second = sync_source_mirror(paths, source_db=str(source_db), force=False)
    assert second["changed"] is False


def test_export_bridge_run_writes_latest_prior_snapshot(tmp_path: Path) -> None:
    paths = _build_paths(tmp_path)
    _create_run_artifacts(paths, "run_alpha")

    result = export_bridge_run(paths, "run_alpha")
    assert result["ok"] is True
    latest_dir = paths.bridge_latest_dir
    prior = read_json(latest_dir / "research_prior_snapshot.json")
    assert prior["run_id"] == "run_alpha"
    assert prior["up"]["codes"] == ["1001", "2002"]
    assert prior["down"]["rank_map"]["3003"] == 1
    manifest = read_json(latest_dir / "bridge_manifest.json")
    assert manifest["artifacts"]["research_prior_snapshot.json"]["source_id"] == "run_alpha"


def test_export_bridge_study_writes_latest_adopted_hypotheses(tmp_path: Path) -> None:
    paths = _build_paths(tmp_path)
    _create_study_artifacts(paths, "study_alpha")

    result = export_bridge_study(paths, "study_alpha")
    assert result["ok"] is True
    latest_dir = paths.bridge_latest_dir
    adopted = read_json(latest_dir / "adopted_hypotheses.json")
    assert adopted["study_id"] == "study_alpha"
    assert adopted["adopted"][0]["trial_id"] == "trial_001"
    manifest = read_json(latest_dir / "bridge_manifest.json")
    assert manifest["artifacts"]["adopted_hypotheses.json"]["source_id"] == "study_alpha"


def test_publish_requires_legacy_flag_for_repo_published_dir(tmp_path: Path) -> None:
    repo_root = tmp_path
    paths = ResearchPaths.build(
        repo_root=repo_root,
        research_home=tmp_path / "research_home",
        workspace_root=tmp_path / "workspace",
        published_root=tmp_path / "published",
    )
    _create_run_artifacts(paths, "run_beta")

    with pytest.raises(RuntimeError):
        run_publish(paths=paths, run_id="run_beta", legacy_publish=False)

    result = run_publish(paths=paths, run_id="run_beta", legacy_publish=True)
    assert result["ok"] is True
    assert read_csv(paths.latest_published_dir / "long_top20.csv")["code"].astype(str).tolist() == ["1001", "2002"]
