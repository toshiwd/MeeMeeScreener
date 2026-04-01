from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from research.bridge import PRIOR_SNAPSHOT_FILE
from research.decision_signal_prior import (
    SCHEMA_VERSION,
    _build_summary_payload,
    _family_summary,
    _stage_from_stats,
    build_decision_signal_prior,
)
from research.storage import ResearchPaths, read_json


def test_stage_from_stats_respects_promotion_gates() -> None:
    row = {
        "sample_n": 52,
        "months_covered": 13,
        "expectancy_20d": 0.018,
        "profit_factor_20d": 1.31,
        "positive_window_ratio": 0.6,
        "mae_worst_gate": -0.11,
        "by_period_stability": 0.7,
        "top_symbol_concentration": 0.24,
    }
    assert _stage_from_stats(row) == "core"


def test_family_summary_assigns_stage_and_bonus_cap() -> None:
    frame = pd.DataFrame(
        [
            {
                "family": "monthly_box_breakout",
                "side": "up",
                "polarity": 1,
                "target_ret_10d": -0.002 if month == 24 else 0.01,
                "target_ret_20d": -0.005 if month == 24 else 0.02,
                "target_mae_20d": -0.06,
                "month_key": f"{2024 + (month // 13)}-{((month - 1) % 12) + 1:02d}",
                "code": f"{1000 + (month % 8)}",
                "period_bucket": "2023_2026" if month <= 12 else "2020_2022",
                "cluster_key": "a",
                "regime_key": "r1",
            }
            for month in range(1, 25)
        ]
    )
    summary = _family_summary(frame)
    assert len(summary) == 1
    assert summary[0]["family"] == "monthly_box_breakout"
    assert summary[0]["promotion_stage"] in {"assist", "weighted", "core"}
    assert summary[0]["bonus_cap"] in {0.01, 0.02, 0.03}


def test_build_decision_signal_prior_payload_can_be_exported(monkeypatch, tmp_path: Path) -> None:
    import research.decision_signal_prior as decision_signal_prior

    sample_payload = {
        "schema_version": SCHEMA_VERSION,
        "strategy_id": "meemee_decision_signal_prior_v1",
        "run_id": "decision_signal_prior_20260331_close",
        "generated_at": "2026-03-31T00:00:00Z",
        "asof": "2026-03-31",
        "provisional": False,
        "source_dataset_id": "db_a,db_b",
        "source_artifacts": {"seed_generators": ["monthly_box_breakout_research"]},
        "summary": _build_summary_payload([], pd.DataFrame(), pd.DataFrame()),
        "up": {
            "asof": "2026-03-31",
            "codes": ["1301"],
            "rank_map": {"1301": 1},
            "fit_score_map": {"1301": 0.8},
            "signal_strength_map": {"1301": 0.9},
            "pattern_tag_map": {"1301": "UM-N-GU-HB"},
            "decision_reason_map": {"1301": ["月足box文脈"]},
            "adoption_reason_map": {"1301": ["月足box文脈"]},
            "risk_watch_map": {"1301": ["値幅荒い"]},
            "promotion_stage_map": {"1301": "weighted"},
            "provisional_map": {"1301": False},
            "hypothesis_family_map": {"1301": "monthly_box_breakout"},
            "bonus_map": {"1301": 0.018},
            "bonus_cap": 0.03,
        },
        "down": {
            "asof": "2026-03-31",
            "codes": [],
            "rank_map": {},
            "fit_score_map": {},
            "signal_strength_map": {},
            "pattern_tag_map": {},
            "decision_reason_map": {},
            "adoption_reason_map": {},
            "risk_watch_map": {},
            "promotion_stage_map": {},
            "provisional_map": {},
            "hypothesis_family_map": {},
            "bonus_map": {},
            "bonus_cap": 0.03,
        },
    }

    monkeypatch.setattr(decision_signal_prior, "build_decision_signal_prior", lambda **_kwargs: sample_payload)
    paths = ResearchPaths.build(repo_root=Path.cwd(), research_home=tmp_path / "research_home")
    out_path = tmp_path / "prior.json"
    result = decision_signal_prior.run_decision_signal_prior(
        paths=paths,
        asof="2026-03-31",
        provisional=False,
        db_paths=[Path("dummy.duckdb")],
        output_json=out_path,
        export_bridge=True,
    )

    assert result["ok"] is True
    assert out_path.exists()
    bridge_payload = read_json(paths.bridge_latest_dir / PRIOR_SNAPSHOT_FILE)
    assert bridge_payload["schema_version"] == SCHEMA_VERSION
    assert bridge_payload["up"]["bonus_map"] == {"1301": 0.018}
