from __future__ import annotations

import hashlib

import duckdb
import pytest

from external_analysis.ops.ops_schema import ensure_ops_db
from external_analysis.results.publish import publish_result
from external_analysis.results.publish_candidates import build_publish_candidate_bundle, load_publish_candidate_bundle
from external_analysis.results.publish_registry import load_publish_registry_state, save_publish_registry_state
from external_analysis.results.result_schema import ensure_result_db
from shared.contracts.logic_artifacts import PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE
from shared.contracts.logic_selection import DEFAULT_LOGIC_POINTER_NAME
from shared.runtime_selection import resolve_runtime_logic_selection

pytestmark = pytest.mark.integration


def _write_artifact(path, content: str) -> str:
    data = content.encode("utf-8")
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def test_meemee_publish_bundle_aligns_with_registry_and_snapshot(tmp_path) -> None:
    result_db = tmp_path / "result.duckdb"
    ops_db = tmp_path / "ops.duckdb"
    artifact_path = tmp_path / "artifacts" / "logic_family_a_v1.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    checksum = _write_artifact(artifact_path, '{"logic":"family_a:v1","mode":"ranking"}\n')

    ensure_result_db(str(result_db))
    ensure_ops_db(str(ops_db))

    publish_result(
        db_path=str(result_db),
        publish_id="pub_2026-03-19_20260319T010000Z_01",
        as_of_date="2026-03-19",
        freshness_state="fresh",
        default_logic_pointer="logic_family_a:v1",
        bootstrap_champion=True,
        logic_artifact_uri=str(artifact_path),
        logic_artifact_checksum=checksum,
        logic_manifest={
            "logic_id": "logic_family_a",
            "logic_version": "v1",
            "logic_family": "family_a",
            "status": "published",
            "input_schema_version": "v3",
            "output_schema_version": "v3",
            "feature_spec_version": "v3",
            "required_inputs": ["confirmed_market_bars"],
            "scorer_type": "ranking",
            "params": {"weight": 1.0},
            "thresholds": {"min_score": 0.2},
            "weights": {"score": 1.0},
            "output_spec": {"rank_fields": ["code", "score"]},
            "trained_at": "2026-03-18T18:00:00Z",
            "published_at": "2026-03-19T01:00:00Z",
            "artifact_uri": str(artifact_path),
            "checksum": checksum,
        },
    )

    conn = duckdb.connect(str(result_db))
    try:
        conn.execute(
            """
            INSERT INTO candidate_daily VALUES
                (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?),
                (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "pub_2026-03-19_20260319T010000Z_01",
                "2026-03-19",
                "1301",
                "buy",
                1,
                0.91,
                20,
                '["momentum", "confirmed"]',
                "confirmed",
                "fresh",
                "pub_2026-03-19_20260319T010000Z_01",
                "2026-03-19",
                "7203",
                "sell",
                2,
                0.77,
                20,
                '["mean_reversion"]',
                "confirmed",
                "fresh",
            ],
        )
    finally:
        conn.close()

    save_publish_registry_state(
        db_path=str(result_db),
        state={
            "source_of_truth": "external_analysis",
            "source_revision": "seed:publish_registry",
            "bootstrap_rule": "explicit_champion_flag",
            "champion": {
                "logic_id": "logic_family_a",
                "logic_version": "v1",
                "logic_key": "logic_family_a:v1",
                "logic_family": "family_a",
                "artifact_uri": str(artifact_path),
                "artifact_checksum": checksum,
                "status": "champion",
                "role": "champion",
            },
            "challenger": {
                "logic_id": "logic_family_a",
                "logic_version": "v2",
                "logic_key": "logic_family_a:v2",
                "logic_family": "family_a",
                "artifact_uri": str(artifact_path),
                "artifact_checksum": checksum,
                "status": "challenger",
                "role": "challenger",
            },
            "challengers": [
                {
                    "logic_id": "logic_family_a",
                    "logic_version": "v2",
                    "logic_key": "logic_family_a:v2",
                    "logic_family": "family_a",
                    "artifact_uri": str(artifact_path),
                    "artifact_checksum": checksum,
                    "status": "challenger",
                    "role": "challenger",
                    "queue_order": 1,
                }
            ],
            "default_logic_pointer": "logic_family_a:v1",
            "previous_stable_champion_logic_key": "logic_family_a:v0",
            "retired_logic_keys": ["logic_family_a:v0"],
            "demoted_logic_keys": [],
        },
        sync_state="synced",
        degraded=False,
        source_revision="seed:publish_registry",
        sync_message="seeded_registry",
    )

    readiness = {
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

    bundle_result = build_publish_candidate_bundle(
        db_path=str(result_db),
        publish_id="pub_2026-03-19_20260319T010000Z_01",
        readiness=readiness,
    )
    assert bundle_result["ok"] is True

    bundle = load_publish_candidate_bundle(db_path=str(result_db), logic_key="logic_family_a:v1")
    assert bundle is not None
    assert bundle["bundle_schema_version"] == "publish_candidate_bundle_v1"
    assert bundle["published_logic_artifact"]["required_inputs"] == ["confirmed_market_bars"]
    assert bundle["published_logic_manifest"]["artifact_uri"] == str(artifact_path)
    assert bundle["published_logic_manifest"]["checksum"] == checksum
    assert bundle["published_ranking_snapshot"]["audit_role"] == PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE
    assert [row["code"] for row in bundle["published_ranking_snapshot"]["rows"]] == ["1301", "7203"]
    assert bundle["validation_summary"]["decision"] == "candidate"
    assert bundle["validation_summary"]["metrics"]["readiness_pass"] is True
    assert bundle["metadata"]["ranking_snapshot_policy"] == "creation_time_if_rows_present"

    registry = load_publish_registry_state(db_path=str(result_db))
    assert registry["champion_logic_key"] == "logic_family_a:v1"
    assert registry["default_logic_pointer"] == "logic_family_a:v1"

    selection = resolve_runtime_logic_selection(
        selected_logic_override=None,
        default_logic_pointer=registry["default_logic_pointer"],
        last_known_good=None,
        available_logic_keys=[bundle["logic_key"]],
        safe_fallback_key="builtin:fallback",
    )
    assert selection["selected_logic_key"] == "logic_family_a:v1"
    assert selection["selected_source"] == DEFAULT_LOGIC_POINTER_NAME

