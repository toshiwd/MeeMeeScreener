from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import tradex_downside_prebreak_narrow_universe_stability_v5 as mod


def test_rank_limit_stability_keep_branch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    monkeypatch.setattr(mod.v4.base, "_resolve_db_path", lambda _cli: db_path)
    monkeypatch.setattr(mod, "_load_rows", lambda *_args, **_kwargs: [{"ymd": 20250131, "code": "A"}])

    def fake_run_pipeline(**kwargs):
        limit = int(kwargs["baseline_rank_limit"])
        return {
            "decision": "keep_for_shadow_paper_replay",
            "reason_type": "prebreak_event_selector_improves_same_condition_near_term_breakdown_quality",
            "compare": {
                "baseline": {"selected_count": 15, "prebreak_event_rate": 0.3, "near_term_target_mean": 0.01},
                "challenger": {"selected_count": 15, "prebreak_event_rate": 0.4, "near_term_target_mean": 0.02},
                "delta": {
                    "prebreak_event_rate_delta": 0.1 if limit in {10, 20} else -0.02,
                    "near_term_target_mean_delta": 0.01 if limit in {10, 20} else -0.01,
                    "mean_ret20_delta": -0.001,
                    "median_ret20_delta": 0.0,
                    "changed_top5_members_count": 4,
                    "changed_rank_count": 1,
                },
                "monthly_summary": {"positive_months": 2, "negative_months": 1},
            },
        }

    monkeypatch.setattr(mod.v4, "run_pipeline", fake_run_pipeline)
    out = tmp_path / "out"
    result = mod.run(db_path=db_path, output_dir=out, rank_limits=(10, 20, 30), source_rank_limit=20)

    decision = json.loads((out / "downside_prebreak_narrow_universe_stability_decision.json").read_text(encoding="utf-8"))
    compare = json.loads((out / "downside_prebreak_narrow_universe_rank_limit_compare.json").read_text(encoding="utf-8"))

    assert result["decision"] == "keep_for_shadow_paper_replay_stability"
    assert decision["decision"] == "keep_for_shadow_paper_replay_stability"
    assert len(compare["grid"]) == 3
    assert (out / "_ARTIFACT_COMPLETE.json").exists()


def test_rank_limit_stability_hold_when_source_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "stocks.duckdb"
    db_path.touch()
    monkeypatch.setattr(mod.v4.base, "_resolve_db_path", lambda _cli: db_path)
    monkeypatch.setattr(mod, "_load_rows", lambda *_args, **_kwargs: [{"ymd": 20250131, "code": "A"}])

    def fake_run_pipeline(**kwargs):
        limit = int(kwargs["baseline_rank_limit"])
        event_delta = 0.1 if limit == 20 else -0.1
        return {
            "decision": "keep_for_shadow_paper_replay" if limit == 20 else "drop_as_prebreak_event_edge_insufficient",
            "reason_type": "synthetic",
            "compare": {
                "baseline": {"selected_count": 15, "prebreak_event_rate": 0.3, "near_term_target_mean": 0.01},
                "challenger": {"selected_count": 15, "prebreak_event_rate": 0.4, "near_term_target_mean": 0.02},
                "delta": {
                    "prebreak_event_rate_delta": event_delta,
                    "near_term_target_mean_delta": event_delta,
                    "mean_ret20_delta": -0.001,
                    "median_ret20_delta": 0.0,
                    "changed_top5_members_count": 4,
                    "changed_rank_count": 1,
                },
                "monthly_summary": {"positive_months": 1, "negative_months": 1},
            },
        }

    monkeypatch.setattr(mod.v4, "run_pipeline", fake_run_pipeline)
    result = mod.run(db_path=db_path, output_dir=tmp_path / "out", rank_limits=(10, 20, 30), source_rank_limit=20)

    assert result["decision"] == "hold_due_to_rank_limit_sensitivity"
