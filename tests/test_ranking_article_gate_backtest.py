from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from scripts import ranking_article_gate_backtest as article_backtest


def test_article_gate_selection_prefers_article_rows() -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 1,
                "code": "1111",
                "entryQualified": True,
                "displayScore": 0.90,
                "setupType": "breakout",
                "forward_return_5": 0.03,
                "forward_return_20": 0.05,
                "forward_return_60": 0.07,
                "article_gate": False,
                "article_breakout_gate": False,
                "article_bottom_gate": False,
                "article_breakout_best": False,
                "article_watch_best": False,
                "article_best_gate": False,
                "timing_label": "other",
            },
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 2,
                "code": "2222",
                "entryQualified": True,
                "displayScore": 0.80,
                "setupType": "breakout",
                "forward_return_5": 0.04,
                "forward_return_20": 0.08,
                "forward_return_60": 0.09,
                "article_gate": True,
                "article_breakout_gate": True,
                "article_bottom_gate": False,
                "article_breakout_best": True,
                "article_watch_best": False,
                "article_best_gate": True,
                "timing_label": "month_end_1_3",
            },
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 3,
                "code": "3333",
                "entryQualified": False,
                "displayScore": 0.85,
                "setupType": "watch",
                "forward_return_5": 0.01,
                "forward_return_20": 0.02,
                "forward_return_60": 0.03,
                "article_gate": True,
                "article_breakout_gate": False,
                "article_bottom_gate": True,
                "article_breakout_best": False,
                "article_watch_best": True,
                "article_best_gate": True,
                "timing_label": "month_start_1_3",
            },
        ]
    )

    baseline = article_backtest._select_for_variant(panel, variant="baseline", bucket_size=2)  # type: ignore[attr-defined]
    article_filter = article_backtest._select_for_variant(panel, variant="article_filter", bucket_size=2)  # type: ignore[attr-defined]
    weighted = article_backtest._select_for_variant(panel, variant="article_weighted_v1", bucket_size=2)  # type: ignore[attr-defined]
    weighted_v2 = article_backtest._select_for_variant(panel, variant="article_weighted_v2", bucket_size=2)  # type: ignore[attr-defined]

    assert baseline["code"].tolist() == ["1111", "2222"]
    assert article_filter["code"].tolist() == ["2222", "3333"]
    assert weighted["code"].tolist()[0] == "2222"
    assert weighted["code"].tolist()[1] == "3333"
    assert weighted_v2["code"].tolist()[0] == "2222"


def test_run_ranking_article_gate_backtest_writes_report(monkeypatch, tmp_path: Path) -> None:
    panel = pd.DataFrame(
        [
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 1,
                "code": "1111",
                "entryQualified": True,
                "displayScore": 0.90,
                "setupType": "breakout",
                "forward_return_5": 0.03,
                "forward_return_20": 0.05,
                "forward_return_60": 0.07,
            },
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 2,
                "code": "2222",
                "entryQualified": True,
                "displayScore": 0.80,
                "setupType": "breakout",
                "forward_return_5": 0.04,
                "forward_return_20": 0.08,
                "forward_return_60": 0.09,
            },
        ]
    )
    article = pd.DataFrame(
        [
            {
                "code": "1111",
                "as_of_iso": "2024-01-02",
                "box_active": True,
                "box_zone": "upper",
                "box_month_bucket": "9-12",
                "box_month_index": 10,
                "monthly_context": "box_upper_pressure",
                "weekly_context": "up_support_intact",
                "timing_label": "other",
                "timing_gate": False,
                "daily_pattern_2": "X",
                "daily_pattern_3": "X>X",
                "article_breakout_gate": False,
                "article_bottom_gate": False,
                "article_gate": False,
                "article_breakout_best": False,
                "article_watch_best": False,
                "article_best_gate": False,
            },
            {
                "code": "2222",
                "as_of_iso": "2024-01-02",
                "box_active": True,
                "box_zone": "upper",
                "box_month_bucket": "9-12",
                "box_month_index": 10,
                "monthly_context": "box_upper_pressure",
                "weekly_context": "up_support_intact",
                "timing_label": "month_end_1_3",
                "timing_gate": True,
                "daily_pattern_2": "U",
                "daily_pattern_3": "U>U>U",
                "article_breakout_gate": True,
                "article_bottom_gate": False,
                "article_gate": True,
                "article_breakout_best": True,
                "article_watch_best": False,
                "article_best_gate": True,
            },
        ]
    )

    def _fake_load_panel(_panel_path):
        return panel.copy()

    def _fake_build_article_features(_db_paths):
        return article.copy()

    monkeypatch.setattr(article_backtest, "_load_panel", _fake_load_panel)
    monkeypatch.setattr(article_backtest, "_build_article_features", _fake_build_article_features)

    payload = article_backtest.run_ranking_article_gate_backtest(
        panel_path=Path("dummy.parquet"),
        db_paths=[Path("dummy.duckdb")],
        bucket_sizes=(2,),
    )

    assert payload["schema_version"] == article_backtest.ARTICLE_SCRIPT_SCHEMA_VERSION
    assert payload["best_variant"] in {
        "baseline",
        "article_filter",
        "article_entryQualified_filter",
        "article_best_filter",
        "article_best_entryQualified_filter",
        "article_weighted_v1",
        "article_weighted_v2",
    }
    assert payload["variants"]["article_filter"]["top10"]["sample_count"] > 0

    output_dir = tmp_path / "out"
    output_dir.mkdir()
    (output_dir / "ranking_article_gate_backtest.json").write_text("{}", encoding="utf-8")
    (output_dir / "ranking_article_gate_backtest.md").write_text("", encoding="utf-8")
    assert output_dir.exists()


def test_run_ranking_article_gate_backtest_publishes_bridge_snapshot(monkeypatch, tmp_path: Path) -> None:
    bridge_latest = tmp_path / "bridge" / "latest"
    bridge_latest.mkdir(parents=True)
    existing_snapshot = {
        "schema_version": "existing",
        "strategy_id": "existing_strategy",
        "source_dataset_id": "existing_dataset",
        "source_artifacts": {"old.json": "old.json"},
        "run_id": "existing_run",
        "up": {},
        "down": {
            "asof": "2024-01-01",
            "codes": ["9999"],
            "rank_map": {"9999": 1},
            "fit_score_map": {},
            "signal_strength_map": {},
            "pattern_tag_map": {},
            "decision_reason_map": {},
            "adoption_reason_map": {},
            "risk_watch_map": {},
            "promotion_stage_map": {},
            "provisional_map": {},
            "hypothesis_family_map": {},
            "bonus_map": {"9999": 0.0},
            "bonus_cap": 0.01,
            "source_pattern": None,
            "source_disposition": None,
        },
    }
    (bridge_latest / "research_prior_snapshot.json").write_text(json.dumps(existing_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    (bridge_latest / "bridge_manifest.json").write_text(json.dumps({"generated_at": "2024-01-01T00:00:00Z", "artifacts": {}}, ensure_ascii=False, indent=2), encoding="utf-8")

    panel = pd.DataFrame(
        [
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 1,
                "code": "1111",
                "entryQualified": True,
                "displayScore": 0.70,
                "setupType": "watch",
                "forward_return_5": 0.02,
                "forward_return_20": 0.04,
                "forward_return_60": 0.06,
            },
            {
                "as_of": 20240102,
                "as_of_iso": "2024-01-02",
                "rank": 2,
                "code": "2222",
                "entryQualified": True,
                "displayScore": 0.90,
                "setupType": "breakout",
                "forward_return_5": 0.05,
                "forward_return_20": 0.09,
                "forward_return_60": 0.11,
            },
        ]
    )
    article = pd.DataFrame(
        [
            {
                "code": "1111",
                "as_of_iso": "2024-01-02",
                "box_active": True,
                "box_zone": "mid",
                "box_month_bucket": "6-8",
                "box_month_index": 7,
                "monthly_context": "box_mid",
                "weekly_context": "up_support_intact",
                "timing_label": "other",
                "timing_gate": False,
                "daily_pattern_2": "X",
                "daily_pattern_3": "X>X",
                "article_breakout_gate": False,
                "article_bottom_gate": False,
                "article_gate": False,
                "article_breakout_best": False,
                "article_watch_best": False,
                "article_best_gate": False,
            },
            {
                "code": "2222",
                "as_of_iso": "2024-01-02",
                "box_active": True,
                "box_zone": "upper",
                "box_month_bucket": "9-12",
                "box_month_index": 10,
                "monthly_context": "box_upper_pressure",
                "weekly_context": "up_support_intact",
                "timing_label": "month_end_1_3",
                "timing_gate": True,
                "daily_pattern_2": "U",
                "daily_pattern_3": "U>U>U",
                "article_breakout_gate": True,
                "article_bottom_gate": False,
                "article_gate": True,
                "article_breakout_best": True,
                "article_watch_best": False,
                "article_best_gate": True,
            },
        ]
    )

    def _fake_load_panel(_panel_path):
        return panel.copy()

    def _fake_build_article_features(_db_paths):
        return article.copy()

    monkeypatch.setattr(article_backtest, "_load_panel", _fake_load_panel)
    monkeypatch.setattr(article_backtest, "_build_article_features", _fake_build_article_features)
    monkeypatch.setenv("MEEMEE_RESEARCH_BRIDGE_DIR", str(tmp_path / "bridge"))

    payload = article_backtest.run_ranking_article_gate_backtest(
        panel_path=Path("dummy.parquet"),
        db_paths=[Path("dummy.duckdb")],
        bucket_sizes=(2,),
        publish_prior=True,
        publish_variant="article_best_entryQualified_filter",
        publish_bucket_size=2,
        source_artifacts={
            "ranking_article_gate_backtest.json": str(tmp_path / "ranking_article_gate_backtest.json"),
            "ranking_article_gate_backtest.md": str(tmp_path / "ranking_article_gate_backtest.md"),
        },
    )

    published = payload["published_prior"]
    assert published["strategy_id"] == "ranking_article_gate_prior_article_best_entryQualified_filter"
    snapshot = json.loads((bridge_latest / "research_prior_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["strategy_id"] == "ranking_article_gate_prior_article_best_entryQualified_filter"
    assert snapshot["up"]["codes"] == ["2222"]
    assert snapshot["down"]["codes"] == ["9999"]
    assert snapshot["up"]["bonus_map"]["2222"] > 0.0
    manifest = json.loads((bridge_latest / "bridge_manifest.json").read_text(encoding="utf-8"))
    assert manifest["artifacts"]["research_prior_snapshot.json"]["source_type"] == "ranking_article_gate_backtest"
