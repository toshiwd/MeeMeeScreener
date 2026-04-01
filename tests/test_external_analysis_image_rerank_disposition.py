from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from external_analysis.__main__ import main as external_analysis_main
from external_analysis.image_rerank.artifacts import read_json
from external_analysis.image_rerank.research_runner import _build_challenger_disposition_artifact


def _base_analysis(compare_uri: str) -> dict:
    return {
        "analysis_run_id": "analysis-run",
        "challenger_kind": "image_rerank_rank_improver",
        "comparison_invariants": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
        },
        "metrics": {
            "top_k_uplift": 0.0,
            "changed_top10_count": 0,
            "selection_divergence": 0.0,
            "bad_pick_removal": 0,
        },
        "artifacts": {
            "phase3_compare_uri": compare_uri,
        },
    }


def _base_compare() -> dict:
    return {
        "fusion_sweep": {
            "modes": {
                "veto_helper": {
                    "metrics": {
                        "top_k_uplift": 0.0,
                        "changed_top10_count": 0,
                        "bad_pick_removal": 0,
                    }
                }
            }
        }
    }


def test_build_challenger_disposition_drop_for_primary_and_secondary_noop(tmp_path) -> None:
    disposition = _build_challenger_disposition_artifact(
        session_id="session-drop",
        confirm_json={"ok": True},
        analysis_json=_base_analysis("compare.json"),
        compare_json=_base_compare(),
        disposition_path=tmp_path / "challenger_disposition.json",
    )
    assert disposition["decision"] == "drop"
    assert disposition["summary_flags"]["artifact_complete"] is True
    assert disposition["summary_flags"]["no_op_primary"] is True
    assert disposition["summary_flags"]["no_op_secondary"] is True


def test_build_challenger_disposition_hold_when_secondary_has_signal(tmp_path) -> None:
    compare_json = _base_compare()
    compare_json["fusion_sweep"]["modes"]["veto_helper"]["metrics"]["changed_top10_count"] = 1
    disposition = _build_challenger_disposition_artifact(
        session_id="session-hold",
        confirm_json={"ok": True},
        analysis_json=_base_analysis("compare.json"),
        compare_json=compare_json,
        disposition_path=tmp_path / "challenger_disposition.json",
    )
    assert disposition["decision"] == "hold"
    assert disposition["summary_flags"]["no_op_primary"] is True
    assert disposition["summary_flags"]["no_op_secondary"] is False


def test_build_challenger_disposition_hold_when_artifacts_incomplete(tmp_path) -> None:
    disposition = _build_challenger_disposition_artifact(
        session_id="session-incomplete",
        confirm_json={"ok": False},
        analysis_json={},
        compare_json={},
        disposition_path=tmp_path / "challenger_disposition.json",
    )
    assert disposition["decision"] == "hold"
    assert disposition["summary_flags"]["artifact_complete"] is False


def test_build_challenger_disposition_keep_for_positive_primary_signal(tmp_path) -> None:
    analysis_json = _base_analysis("compare.json")
    analysis_json["metrics"]["top_k_uplift"] = 0.01
    analysis_json["metrics"]["changed_top10_count"] = 2
    disposition = _build_challenger_disposition_artifact(
        session_id="session-keep",
        confirm_json={"ok": True},
        analysis_json=analysis_json,
        compare_json=_base_compare(),
        disposition_path=tmp_path / "challenger_disposition.json",
    )
    assert disposition["decision"] == "keep"
    assert disposition["summary_flags"]["no_op_primary"] is False


@pytest.mark.integration
def test_image_rerank_disposition_cli_backfills_existing_session(monkeypatch, tmp_path) -> None:
    tradex_root = tmp_path / "tradex_root"
    session_dir = tradex_root / "image_rerank" / "research_sessions" / "session-drop"
    session_dir.mkdir(parents=True, exist_ok=True)
    compare_path = tmp_path / "compare.json"

    confirm_json = {"ok": True}
    analysis_json = _base_analysis(str(compare_path))
    compare_json = _base_compare()

    (session_dir / "full_universe_confirm.json").write_text(json.dumps(confirm_json), encoding="utf-8")
    (session_dir / "challenger_first_analysis.json").write_text(json.dumps(analysis_json), encoding="utf-8")
    compare_path.write_text(json.dumps(compare_json), encoding="utf-8")

    monkeypatch.setenv("MEEMEE_TRADEX_ROOT", str(tradex_root))
    monkeypatch.setattr(sys, "argv", ["external_analysis", "image-rerank-disposition-run", "--session-id", "session-drop"])

    assert external_analysis_main() == 0
    disposition_json = read_json(session_dir / "challenger_disposition.json")
    assert disposition_json["decision"] == "drop"
    assert disposition_json["source_artifacts"]["full_universe_confirm_uri"].endswith("full_universe_confirm.json")
    assert disposition_json["source_artifacts"]["challenger_first_analysis_uri"].endswith("challenger_first_analysis.json")
    assert disposition_json["source_artifacts"]["phase3_compare_uri"] == str(compare_path)
