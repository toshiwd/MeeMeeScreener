from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_selected_pattern_family_validation_v2 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _roots(tmp_path: Path) -> dict[str, Path]:
    roots = {
        "portfolio_root": tmp_path / "portfolio",
        "wide_strength_root": tmp_path / "wide",
        "ma5_root": tmp_path / "ma5",
        "ma5_trade_ledger": tmp_path / "ma5_source" / "trade_ledger.jsonl",
    }
    _write_portfolio(roots["portfolio_root"])
    _write_wide(roots["wide_strength_root"])
    _write_ma5(roots["ma5_root"], roots["ma5_trade_ledger"])
    return roots


def _write_portfolio(root: Path) -> None:
    _write_json(root / "research_decision.json", {"decision": "keep_candidate"})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    _write_json(
        root / "selected_pattern_families_for_validation.json",
        {
            "selection_count": 2,
            "selected_pattern_families": [
                {
                    "family_id": "momentum_continuation_soft_boost_v1",
                    "overlap_tags": ["wide_pool", "momentum_continuation", "upside_capture"],
                },
                {
                    "family_id": "ma5_reclaim_context::h12_near_bull_ma60_rising",
                    "overlap_tags": ["ma5_reclaim", "h12_near_bull_ma60_rising", "trend_context"],
                },
            ],
        },
    )


def _write_wide(root: Path, *, weak: bool = False) -> None:
    momentum = {
        "family_id": "momentum_continuation_soft_boost_v1",
        "average_candidates_per_day": 9.0,
        "selected_day_count": 2333,
        "selected_event_count": 6595,
        "selected_on_opportunity_days": 2333,
        "selected_top3_avg_ret20": 0.0108 if not weak else 0.001,
        "selected_top3_win_rate20": 0.513,
        "selected_top3_big_winner_ret20_ge_10_capture_rate": 0.365 if not weak else 0.1,
        "selected_top3_future_top10_precision": 0.217,
        "selected_top3_severe_loss_rate20": 0.244,
        "selected_nonwinner_when_winner_available_rate": 0.403,
    }
    random = {
        "family_id": "all_strength_scoreless_random_top3",
        "selected_top3_avg_ret20": 0.0093,
        "selected_top3_big_winner_ret20_ge_10_capture_rate": 0.322,
        "selected_top3_future_top10_precision": 0.201,
        "selected_top3_severe_loss_rate20": 0.210,
        "selected_nonwinner_when_winner_available_rate": 0.444,
    }
    oracle = {"family_id": "all_strength_oracle_top3", "selected_top3_avg_ret20": 0.0838}
    _write_json(root / "research_decision.json", {"decision": "hold"})
    _write_json(root / "score_leaderboard.json", {"rows": [momentum, random, oracle]})
    _write_json(root / "feature_availability_audit.json", {"future_labels_used_in_score_inputs": False})


def _write_ma5(root: Path, trade_ledger: Path, *, no_h12: bool = False) -> None:
    rows: list[dict[str, object]] = [
        {
            "hypothesis_id": "h00_base_all",
            "trade_count": 1000,
            "avg_ret": 0.004,
            "win_rate": 0.322,
            "severe_loss_rate": 0.020,
            "profit_factor": 1.24,
        }
    ]
    if not no_h12:
        rows.append(
            {
                "hypothesis_id": "h12_near_bull_ma60_rising",
                "trade_count": 1397,
                "symbol_count": 576,
                "avg_ret": 0.0117,
                "win_rate": 0.334,
                "severe_loss_rate": 0.0408,
                "avg_mfe": 0.088,
                "avg_mae": -0.037,
                "profit_factor": 1.47,
                "reentry_expansion_modeled": False,
            }
        )
    _write_json(root / "research_decision.json", {"decision": "excellent_hypothesis_found"})
    _write_json(root / "hypothesis_leaderboard.json", {"rows": rows})
    _write_json(root / "feature_availability_audit.json", {"future_labels_used_in_score_inputs": False})
    _write_jsonl(trade_ledger, [{"symbol": "1001", "entry_date": "2020-01-01", "ret": 0.01}])


def test_validation_v2_holds_when_evidence_exists_but_top5_direct_comparison_missing(tmp_path: Path) -> None:
    payload = mod.run_selected_pattern_family_validation_v2(output_parent=tmp_path / "out", run_id="v2", **_roots(tmp_path))

    research = payload["research_decision"]

    assert research["decision"] == "hold"
    assert research["authoritative_research_decision"] == "selected_pattern_family_validation_v2_hold"
    assert research["top5_direct_comparison_available"] is False
    assert research["silent_fallback_used"] is False
    assert research["production_ranking_changed"] is False
    assert payload["top5_candidate_pool_report"]["top5_direct_comparison_available"] is False
    assert payload["momentum_risk_profile_report"]["family_verdict"] == "hold_risk_decomposition_required"
    assert payload["ma5_reclaim_context_report"]["family_verdict"] == "hold_additive_candidate_generation_gap"
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_validation_v2_drops_when_selected_metrics_are_missing(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    _write_json(roots["wide_strength_root"] / "score_leaderboard.json", {"rows": []})
    _write_ma5(roots["ma5_root"], roots["ma5_trade_ledger"], no_h12=True)

    payload = mod.run_selected_pattern_family_validation_v2(output_parent=tmp_path / "out", run_id="v2", **roots)

    assert payload["research_decision"]["decision"] == "hold"
    assert "evidence_exists_but_top5_direct_comparison_unavailable" in payload["research_decision"]["typed_reasons"]
    assert "source_metrics_missing" in payload["momentum_risk_profile_report"]["risk_flags"]
    assert "h12_metrics_missing" in payload["ma5_reclaim_context_report"]["blockers"]
    assert payload["artifact_complete"]["present_outputs"]["_ARTIFACT_COMPLETE.json"] is True


def test_validation_v2_reports_low_family_overlap(tmp_path: Path) -> None:
    payload = mod.run_selected_pattern_family_validation_v2(output_parent=tmp_path / "out", run_id="v2", **_roots(tmp_path))

    overlap = payload["family_overlap_report"]

    assert overlap["pair_count"] == 1
    assert overlap["pairs"][0]["overlap_level"] == "low"
    assert overlap["overlap_assessment"] == "acceptable_low_overlap"
