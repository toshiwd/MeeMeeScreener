from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from app.backend.infra.files.config_repo import ConfigRepository, PUBLISH_REGISTRY_SCHEMA_VERSION
from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish import publish_result
from external_analysis.results.publish_candidates import (
    approve_publish_candidate_bundle,
    backfill_publish_candidate_bundles,
    build_publish_candidate_bundle,
    sweep_publish_candidate_snapshots,
)
from external_analysis.results.publish_registry import save_publish_registry_state as save_external_publish_registry_state
from external_analysis.results.result_schema import ensure_result_db


def _write_artifact(path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = content.encode("utf-8")
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _build_manifest(*, logic_id: str, logic_version: str, logic_family: str, artifact_path: Path, checksum: str, trained_at: str, published_at: str) -> dict[str, object]:
    return {
        "logic_id": logic_id,
        "logic_version": logic_version,
        "logic_family": logic_family,
        "status": "published",
        "input_schema_version": "v3",
        "output_schema_version": "v3",
        "feature_spec_version": "v3",
        "required_inputs": ["confirmed_market_bars"],
        "scorer_type": "ranking",
        "params": {},
        "thresholds": {},
        "weights": {},
        "output_spec": {"rank_fields": ["code", "score"]},
        "trained_at": trained_at,
        "published_at": published_at,
        "artifact_uri": str(artifact_path),
        "checksum": checksum,
    }


def seed_publish_ops_e2e(*, data_dir: Path, result_db_path: Path, ops_db_path: Path) -> dict[str, object]:
    data_dir.mkdir(parents=True, exist_ok=True)
    result_db_path.parent.mkdir(parents=True, exist_ok=True)
    ops_db_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_result_db(str(result_db_path))
    ensure_ops_db(str(ops_db_path))
    with duckdb.connect(str(result_db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_meta (
                id INTEGER PRIMARY KEY,
                earnings_last_success_at TIMESTAMP,
                rights_last_success_at TIMESTAMP,
                last_error TEXT,
                last_attempt_at TIMESTAMP,
                is_refreshing BOOLEAN DEFAULT FALSE,
                refresh_lock_job_id TEXT,
                refresh_lock_started_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_refresh_jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT,
                reason TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ex_rights (
                code TEXT,
                ex_date DATE,
                record_date DATE,
                category TEXT,
                last_rights_date DATE
                ,
                source TEXT,
                fetched_at TIMESTAMP
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO events_meta (id, is_refreshing) VALUES (1, FALSE)")

    # `events_routes` reads from `config.DB_PATH` (stocks.duckdb by default).
    # Seed the same minimal tables there so real-backend smoke runs do not trip
    # over missing legacy tables.
    stock_db_path = data_dir / "stocks.duckdb"
    with duckdb.connect(str(stock_db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_meta (
                id INTEGER PRIMARY KEY,
                earnings_last_success_at TIMESTAMP,
                rights_last_success_at TIMESTAMP,
                last_error TEXT,
                last_attempt_at TIMESTAMP,
                is_refreshing BOOLEAN DEFAULT FALSE,
                refresh_lock_job_id TEXT,
                refresh_lock_started_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_refresh_jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT,
                reason TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ex_rights (
                code TEXT,
                ex_date DATE,
                record_date DATE,
                category TEXT,
                last_rights_date DATE
                ,
                source TEXT,
                fetched_at TIMESTAMP
            )
            """
        )
        conn.execute("INSERT OR IGNORE INTO events_meta (id, is_refreshing) VALUES (1, FALSE)")

    champion_artifact = data_dir / "fixtures" / "operator_console_e2e" / "logic_family_a_v1.json"
    challenger_artifact = data_dir / "fixtures" / "operator_console_e2e" / "logic_family_a_v2.json"
    champion_checksum = _write_artifact(champion_artifact, '{"logic":"family_a:v1"}\n')
    challenger_checksum = _write_artifact(challenger_artifact, '{"logic":"family_a:v2"}\n')

    champion_publish_id = "pub_2026-03-19_20260319T010000Z_01"
    challenger_publish_id = "pub_2026-03-19_20260319T020000Z_01"

    publish_result(
        db_path=str(result_db_path),
        publish_id=champion_publish_id,
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        bootstrap_champion=True,
        logic_artifact_uri=str(champion_artifact),
        logic_artifact_checksum=champion_checksum,
        logic_manifest=_build_manifest(
            logic_id="logic_family_a",
            logic_version="v1",
            logic_family="family_a",
            artifact_path=champion_artifact,
            checksum=champion_checksum,
            trained_at="2026-03-18T00:00:00Z",
            published_at="2026-03-19T01:00:00Z",
        ),
    )
    publish_result(
        db_path=str(result_db_path),
        publish_id=challenger_publish_id,
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        logic_artifact_uri=str(challenger_artifact),
        logic_artifact_checksum=challenger_checksum,
        logic_manifest=_build_manifest(
            logic_id="logic_family_a",
            logic_version="v2",
            logic_family="family_a",
            artifact_path=challenger_artifact,
            checksum=challenger_checksum,
            trained_at="2026-03-18T12:00:00Z",
            published_at="2026-03-19T02:00:00Z",
        ),
    )

    readiness_payload = {
        "source": "external_analysis_shadow",
        "as_of_date": "2026-03-19",
        "champion_version": "logic_family_a:v1",
        "challenger_version": "logic_family_a:v2",
        "sample_count": 64,
        "expectancy_delta": 0.035,
        "improved_expectancy": True,
        "mae_non_worse": True,
        "adverse_move_non_worse": True,
        "stable_window": True,
        "alignment_ok": True,
        "readiness_pass": True,
        "reason_codes": [],
        "summary": {"champion_selected": 18, "challenger_selected": 21},
        "created_at": "2026-03-19T02:30:00Z",
    }

    build_publish_candidate_bundle(
        db_path=str(result_db_path),
        publish_id=champion_publish_id,
        readiness=readiness_payload,
    )
    build_publish_candidate_bundle(
        db_path=str(result_db_path),
        publish_id=challenger_publish_id,
        readiness=readiness_payload,
    )

    approve_publish_candidate_bundle(
        db_path=str(result_db_path),
        logic_key="logic_family_a:v1",
        source="seed_publish_ops_e2e",
        reason="seed champion approved",
        actor="seed",
    )

    repo = ConfigRepository(str(data_dir))
    mirror_state = {
        "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
        "default_logic_pointer": "logic_family_a:v1",
        "bootstrap_rule": "explicit_champion_flag",
        "champion": {
            "logic_id": "logic_family_a",
            "logic_version": "v1",
            "logic_key": "logic_family_a:v1",
            "logic_family": "family_a",
            "artifact_uri": str(champion_artifact),
            "artifact_checksum": champion_checksum,
            "published_at": "2026-03-19T01:00:00Z",
            "status": "champion",
            "role": "champion",
        },
        "challengers": [
            {
                "logic_id": "logic_family_a",
                "logic_version": "v2",
                "logic_key": "logic_family_a:v2",
                "logic_family": "family_a",
                "artifact_uri": str(challenger_artifact),
                "artifact_checksum": challenger_checksum,
                "published_at": "2026-03-19T02:00:00Z",
                "status": "challenger",
                "role": "challenger",
                "source_publish_id": challenger_publish_id,
                "queue_order": 1,
                "promotion_state": "queued",
                "validation_state": "approved",
            }
        ],
        "challenger": {
            "logic_id": "logic_family_a",
            "logic_version": "v2",
            "logic_key": "logic_family_a:v2",
            "logic_family": "family_a",
            "artifact_uri": str(challenger_artifact),
            "artifact_checksum": challenger_checksum,
            "published_at": "2026-03-19T02:00:00Z",
            "status": "challenger",
            "role": "challenger",
            "source_publish_id": challenger_publish_id,
            "queue_order": 1,
            "promotion_state": "queued",
            "validation_state": "approved",
        },
        "challenger_logic_keys": ["logic_family_a:v2"],
        "previous_champion_logic_key": "logic_family_a:v0",
        "retired_logic_keys": ["logic_family_a:v0"],
        "promotion_history": [],
        "source_of_truth": "local_mirror",
    }
    repo.save_publish_registry_state(mirror_state)
    save_external_publish_registry_state(
        db_path=str(result_db_path),
        state={
            **mirror_state,
            "source_revision": "seed:publish_registry",
            "registry_sync_state": "synced",
            "sync_state": "synced",
            "sync_message": "seeded_external_registry",
        },
        sync_state="synced",
        degraded=False,
        source_revision="seed:publish_registry",
        sync_message="seeded_external_registry",
    )

    backfill_publish_candidate_bundles(db_path=str(result_db_path), dry_run=True)
    sweep_publish_candidate_snapshots(db_path=str(result_db_path), dry_run=True)

    return {
        "data_dir": str(data_dir),
        "result_db_path": str(result_db_path),
        "stock_db_path": str(stock_db_path),
        "ops_db_path": str(ops_db_path),
        "champion_key": "logic_family_a:v1",
        "challenger_key": "logic_family_a:v2",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed real-backend operator-console fixtures")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--result-db", required=True)
    parser.add_argument("--ops-db", required=True)
    args = parser.parse_args()

    summary = seed_publish_ops_e2e(
        data_dir=Path(args.data_dir),
        result_db_path=Path(args.result_db),
        ops_db_path=Path(args.ops_db),
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
