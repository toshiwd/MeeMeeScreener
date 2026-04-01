from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.backend.services import rankings_cache
from app.core.config import config as core_config


def _reset_research_prior_cache() -> None:
    with rankings_cache._RESEARCH_PRIOR_CACHE_LOCK:  # type: ignore[attr-defined]
        rankings_cache._RESEARCH_PRIOR_CACHE["loaded_at"] = None  # type: ignore[attr-defined]
        rankings_cache._RESEARCH_PRIOR_CACHE["payload"] = None  # type: ignore[attr-defined]


def test_load_research_prior_snapshot_returns_empty_without_bridge(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))
    _reset_research_prior_cache()

    payload = rankings_cache._load_research_prior_snapshot()

    assert payload["run_id"] is None
    assert payload["schema_version"] is None
    assert payload["strategy_id"] is None
    assert payload["up"]["asof"] is None
    assert payload["up"]["codes"] == []
    assert payload["up"]["rank_map"] == {}
    assert payload["up"]["fit_score_map"] == {}
    assert payload["up"]["bonus_map"] == {}
    assert payload["down"]["codes"] == []
    assert payload["down"]["rank_map"] == {}


def test_load_research_prior_snapshot_prefers_bridge_over_repo_published(monkeypatch, tmp_path: Path) -> None:
    bridge_latest = tmp_path / "bridge" / "latest"
    bridge_latest.mkdir(parents=True, exist_ok=True)
    (bridge_latest / "research_prior_snapshot.json").write_text(
        json.dumps(
            {
                "run_id": "bridge_run",
                "up": {"asof": "2026-03-12", "codes": ["1001", "2002"], "rank_map": {"1001": 1, "2002": 2}},
                "down": {"asof": "2026-03-12", "codes": ["3003"], "rank_map": {"3003": 1}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (bridge_latest / "bridge_manifest.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-12T00:00:00Z",
                "artifacts": {
                    "research_prior_snapshot.json": {
                        "source_type": "run",
                        "source_id": "bridge_run",
                        "generated_at": "2026-03-12T00:00:00Z",
                        "filename": "research_prior_snapshot.json",
                    }
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    fake_repo = tmp_path / "repo"
    fake_published = fake_repo / "published" / "latest"
    fake_published.mkdir(parents=True, exist_ok=True)
    (fake_published / "research_prior_snapshot.json").write_text(
        json.dumps(
            {
                "run_id": "repo_run",
                "up": {"asof": "2025-01-01", "codes": ["9999"], "rank_map": {"9999": 1}},
                "down": {"asof": "2025-01-01", "codes": ["8888"], "rank_map": {"8888": 1}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))
    monkeypatch.setattr(core_config, "REPO_ROOT", fake_repo)
    _reset_research_prior_cache()

    payload = rankings_cache._load_research_prior_snapshot()

    assert payload["run_id"] == "bridge_run"
    assert payload["up"]["codes"] == ["1001", "2002"]
    assert payload["down"]["rank_map"] == {"3003": 1}


def test_load_research_prior_snapshot_parses_rebound_optional_fields(monkeypatch, tmp_path: Path) -> None:
    bridge_latest = tmp_path / "bridge" / "latest"
    bridge_latest.mkdir(parents=True, exist_ok=True)
    (bridge_latest / "research_prior_snapshot.json").write_text(
        json.dumps(
            {
                "schema_version": "tradex_rebound_onset_research_prior_v1",
                "strategy_id": "tradex_rebound_onset_aux_v1",
                "source_dataset_id": "dataset-1",
                "source_artifacts": {"artifact": "x"},
                "run_id": "bridge_run",
                "up": {
                    "asof": "2026-03-28",
                    "codes": ["1001", "2002"],
                    "rank_map": {"1001": 1, "2002": 2},
                    "fit_score_map": {"1001": 0.8},
                    "pattern_tag_map": {"1001": "rebound_onset", "2002": "rebound_onset"},
                    "adoption_reason_map": {"1001": ["120MA上", "下ヒゲ強い"]},
                    "bonus_cap": 0.03,
                    "source_pattern": "rebound_onset",
                    "source_disposition": "keep",
                },
                "down": {"asof": "2026-03-28", "codes": [], "rank_map": {}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))
    _reset_research_prior_cache()

    payload = rankings_cache._load_research_prior_snapshot()

    assert payload["schema_version"] == "tradex_rebound_onset_research_prior_v1"
    assert payload["strategy_id"] == "tradex_rebound_onset_aux_v1"
    assert payload["source_dataset_id"] == "dataset-1"
    assert payload["up"]["fit_score_map"] == {"1001": 0.8}
    assert payload["up"]["pattern_tag_map"]["2002"] == "rebound_onset"
    assert payload["up"]["adoption_reason_map"]["1001"] == ["120MA上", "下ヒゲ強い"]


def test_calc_research_prior_bonus_uses_fit_score_for_rebound_strategy() -> None:
    snapshot = {
        "run_id": "bridge_run",
        "strategy_id": "tradex_rebound_onset_aux_v1",
        "up": {
            "asof": "2026-03-28",
            "codes": ["1001", "2002"],
            "rank_map": {"1001": 1, "2002": 2},
            "fit_score_map": {"1001": 0.8},
            "pattern_tag_map": {"1001": "rebound_onset", "2002": "rebound_onset"},
            "adoption_reason_map": {"1001": ["120MA上"], "2002": ["120MA上"]},
            "bonus_cap": 0.03,
        },
        "down": {"asof": None, "codes": [], "rank_map": {}},
    }

    tagged_only: dict[str, object] = {}
    tagged_bonus = rankings_cache._calc_research_prior_bonus(
        item=tagged_only,
        direction="up",
        code="2002",
        prior_snapshot=snapshot,
    )
    fitted: dict[str, object] = {}
    fitted_bonus = rankings_cache._calc_research_prior_bonus(
        item=fitted,
        direction="up",
        code="1001",
        prior_snapshot=snapshot,
    )

    assert tagged_bonus == 0.0
    assert fitted_bonus == pytest.approx(0.024)
    assert tagged_only["researchPatternTag"] == "rebound_onset"
    assert tagged_only["researchPriorAligned"] is True
    assert tagged_only["reboundOnsetFitScore"] is None
    assert fitted["reboundOnsetFitScore"] == 0.8
    assert fitted["reboundOnsetAdoptionReasons"] == ["120MA上"]


def test_calc_research_prior_bonus_accepts_fit_score_point_five() -> None:
    snapshot = {
        "run_id": "bridge_run",
        "strategy_id": "tradex_rebound_onset_aux_v1",
        "up": {
            "asof": "2026-03-28",
            "codes": ["2002"],
            "rank_map": {"2002": 1},
            "fit_score_map": {"2002": 0.5},
            "pattern_tag_map": {"2002": "rebound_onset"},
            "adoption_reason_map": {"2002": ["120MA上"]},
            "bonus_cap": 0.03,
        },
        "down": {"asof": None, "codes": [], "rank_map": {}},
    }

    item: dict[str, object] = {}
    bonus = rankings_cache._calc_research_prior_bonus(
        item=item,
        direction="up",
        code="2002",
        prior_snapshot=snapshot,
    )

    assert bonus == pytest.approx(0.015)
    assert item["researchPriorBonus"] == pytest.approx(0.015)
    assert item["reboundOnsetFitScore"] == pytest.approx(0.5)


def test_decorate_rule_items_reorders_entry_score_when_rebound_bonus_exists(monkeypatch) -> None:
    snapshot = {
        "run_id": "bridge_run",
        "strategy_id": "tradex_rebound_onset_aux_v1",
        "up": {
            "asof": "2026-03-28",
            "codes": ["2002", "1001"],
            "rank_map": {"2002": 1, "1001": 2},
            "fit_score_map": {"2002": 0.5},
            "pattern_tag_map": {"2002": "rebound_onset"},
            "adoption_reason_map": {"2002": ["120MA上"]},
            "bonus_cap": 0.03,
        },
        "down": {"asof": None, "codes": [], "rank_map": {}},
    }
    monkeypatch.setattr(rankings_cache, "_load_research_prior_snapshot", lambda: snapshot)
    items = [
        {
            "code": "1001",
            "changePct": 0.02,
            "weeklyBreakoutUpProb": 0.61,
            "monthlyBreakoutUpProb": 0.60,
            "monthlyRangeProb": 0.2,
            "liquidity20d": 1000.0,
            "hybridScore": 0.42,
        },
        {
            "code": "2002",
            "changePct": 0.02,
            "weeklyBreakoutUpProb": 0.61,
            "monthlyBreakoutUpProb": 0.60,
            "monthlyRangeProb": 0.2,
            "liquidity20d": 1000.0,
            "hybridScore": 0.42,
        },
    ]

    decorated = rankings_cache._decorate_rule_items_with_entry_gate(items, direction="up")

    assert decorated[0]["code"] == "2002"
    assert decorated[0]["researchPriorBonus"] == pytest.approx(0.015)
    assert decorated[1]["researchPriorBonus"] == 0.0
    assert decorated[0]["entryScore"] > decorated[1]["entryScore"]
    assert decorated[0]["hybridScore"] == decorated[1]["hybridScore"] == pytest.approx(0.42)


def test_load_research_prior_snapshot_parses_decision_signal_fields(monkeypatch, tmp_path: Path) -> None:
    bridge_latest = tmp_path / "bridge" / "latest"
    bridge_latest.mkdir(parents=True, exist_ok=True)
    (bridge_latest / "research_prior_snapshot.json").write_text(
        json.dumps(
            {
                "schema_version": "meemee_decision_signal_prior_v1",
                "strategy_id": "meemee_decision_signal_prior_v1",
                "run_id": "decision_signal_prior_20260331_close",
                "up": {
                    "asof": "2026-03-31",
                    "codes": ["1001"],
                    "rank_map": {"1001": 1},
                    "fit_score_map": {"1001": 0.74},
                    "signal_strength_map": {"1001": 0.82},
                    "pattern_tag_map": {"1001": "UM-N-GU-HB"},
                    "decision_reason_map": {"1001": ["月足box文脈", "週足support維持"]},
                    "risk_watch_map": {"1001": ["値幅荒い"]},
                    "promotion_stage_map": {"1001": "weighted"},
                    "provisional_map": {"1001": False},
                    "hypothesis_family_map": {"1001": "monthly_box_breakout"},
                    "bonus_map": {"1001": 0.018},
                    "bonus_cap": 0.03,
                },
                "down": {"asof": "2026-03-31", "codes": [], "rank_map": {}},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))
    _reset_research_prior_cache()

    payload = rankings_cache._load_research_prior_snapshot()

    assert payload["schema_version"] == "meemee_decision_signal_prior_v1"
    assert payload["up"]["signal_strength_map"] == {"1001": 0.82}
    assert payload["up"]["decision_reason_map"]["1001"] == ["月足box文脈", "週足support維持"]
    assert payload["up"]["promotion_stage_map"] == {"1001": "weighted"}
    assert payload["up"]["bonus_map"] == {"1001": pytest.approx(0.018)}


def test_calc_research_prior_bonus_uses_direct_bonus_map_for_decision_signal_strategy() -> None:
    snapshot = {
        "run_id": "decision_signal_prior_20260331_close",
        "strategy_id": "meemee_decision_signal_prior_v1",
        "up": {
            "asof": "2026-03-31",
            "codes": ["1001"],
            "rank_map": {"1001": 1},
            "fit_score_map": {"1001": 0.74},
            "signal_strength_map": {"1001": 0.82},
            "pattern_tag_map": {"1001": "UM-N-GU-HB"},
            "decision_reason_map": {"1001": ["月足box文脈"]},
            "risk_watch_map": {"1001": ["値幅荒い"]},
            "promotion_stage_map": {"1001": "weighted"},
            "provisional_map": {"1001": True},
            "hypothesis_family_map": {"1001": "monthly_box_breakout"},
            "bonus_map": {"1001": 0.024},
            "bonus_cap": 0.03,
        },
        "down": {"asof": None, "codes": [], "rank_map": {}},
    }

    item: dict[str, object] = {}
    bonus = rankings_cache._calc_research_prior_bonus(
        item=item,
        direction="up",
        code="1001",
        prior_snapshot=snapshot,
    )

    assert bonus == pytest.approx(0.012)
    assert item["researchSignalStrength"] == pytest.approx(0.82)
    assert item["researchPromotionStage"] == "weighted"
    assert item["researchDecisionReasons"] == ["月足box文脈"]
    assert item["researchRiskWatch"] == ["値幅荒い"]
    assert item["researchProvisional"] is True
    assert item["researchHypothesisFamily"] == "monthly_box_breakout"
