from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_pattern_family_portfolio_refresh_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _roots(tmp_path: Path) -> dict[str, Path]:
    roots = {
        "pre_strength_overlay_root": tmp_path / "pre_strength_overlay",
        "multi_pattern_root": tmp_path / "multi_pattern",
        "teppan_watch_root": tmp_path / "teppan_watch",
        "source_v2_root": tmp_path / "source_v2",
        "source_mechanism_root": tmp_path / "source_mechanism",
        "hypothesis_map_root": tmp_path / "hypothesis_map",
        "ma5_root": tmp_path / "ma5",
        "wide_strength_root": tmp_path / "wide_strength",
        "selection_risk_root": tmp_path / "selection_risk",
        "r11_artifact": tmp_path / "r11.json",
        "r11_gate": tmp_path / "r11_gate.json",
    }
    _write_pre_strength_context(roots)
    _write_teppan(roots["teppan_watch_root"])
    _write_source(roots["source_v2_root"], roots["source_mechanism_root"], roots["hypothesis_map_root"])
    _write_ma5(roots["ma5_root"])
    _write_wide(roots["wide_strength_root"], roots["selection_risk_root"])
    _write_r11(roots["r11_artifact"], roots["r11_gate"])
    return roots


def _write_pre_strength_context(roots: dict[str, Path]) -> None:
    _write_json(
        roots["pre_strength_overlay_root"] / "research_decision.json",
        {
            "decision": "defensive_overlay_drop",
            "best_variant_id": "defensive_overlay_soft_penalty_v1",
            "variant_decisions": [
                {"metrics": {"top5_changed_members_count_vs_baseline": 400}},
            ],
        },
    )
    _write_json(roots["multi_pattern_root"] / "research_decision.json", {"decision": "pattern_family_portfolio_ready"})


def _write_teppan(root: Path) -> None:
    _write_json(root / "watch_run_result.json", {"decision": "continue_watch_only", "activation_allowed": False})
    _write_json(root / "teppan_watch_metrics.json", {"top100_teppan_pattern_match_count": 0, "boost_eligible_count": 0})


def _write_source(source_v2_root: Path, source_mech_root: Path, hypothesis_root: Path) -> None:
    _write_json(
        source_v2_root / "research_decision.json",
        {
            "authoritative_research_decision": "source_specific_candidate_generation_v2_drop",
            "selected_hypothesis_id": "candidate_generation_map_refresh_hypothesis_1",
        },
    )
    _write_json(source_mech_root / "research_decision.json", {"authoritative_research_decision": "source_mechanism_validation_next_target_ready"})
    _write_json(
        source_mech_root / "hypothesis_validation_readiness_leaderboard.json",
        {
            "rows": [
                {
                    "hypothesis_id": "candidate_generation_map_refresh_hypothesis_1",
                    "expected_mechanism": "source_not_selected_due_to_max3_overfill",
                    "sample_count": 1454,
                    "missed_winner_count": 92,
                    "future_winner_rate": 0.228,
                    "selected_nonwinner_rate": 0.616,
                    "severe_loss_rate20": 0.247,
                    "selected_capture_rate_among_source_winners": 0.485,
                    "time_block_stability": 0.818,
                }
            ]
        },
    )
    _write_json(hypothesis_root / "refreshed_candidate_generation_hypothesis_map.json", {"hypothesis_count": 2})


def _write_ma5(root: Path, *, weak: bool = False) -> None:
    _write_json(
        root / "research_decision.json",
        {
            "authoritative_research_decision": "excellent_hypothesis_found",
            "excellent_hypotheses": [
                {
                    "hypothesis_id": "h12_near_bull_ma60_rising",
                    "hypothesis_decision": "excellent",
                    "thesis": "MA5 reclaim with rising MA60 context",
                    "trade_count": 1397 if not weak else 60,
                    "symbol_count": 576,
                    "avg_ret": 0.0117 if not weak else 0.001,
                    "avg_ret_delta_vs_base": 0.0075 if not weak else 0.0,
                    "win_rate": 0.334,
                    "severe_loss_rate": 0.0408 if not weak else 0.31,
                    "avg_mfe": 0.088,
                    "avg_mae": -0.037,
                    "profit_factor": 1.47,
                }
            ],
        },
    )


def _write_wide(wide_root: Path, risk_root: Path, *, weak: bool = False) -> None:
    metrics = {
        "family_id": "momentum_continuation_soft_boost_v1",
        "selected_day_count": 2333 if not weak else 20,
        "selected_event_count": 6595 if not weak else 40,
        "average_candidates_per_day": 9.18,
        "median_candidates_per_day": 8.0,
        "opportunity_days_total": 2333 if not weak else 20,
        "selected_top3_avg_ret20": 0.0108 if not weak else 0.0,
        "selected_top3_win_rate20": 0.513,
        "selected_top3_severe_loss_rate20": 0.244 if not weak else 0.40,
        "selected_top3_big_winner_ret20_ge_10_capture_rate": 0.365,
        "selected_top3_future_top10_precision": 0.217,
        "improvement_vs_random_top3": 0.00144 if not weak else 0.0,
        "improvement_vs_all_strength_event_average": 0.00213,
    }
    _write_json(
        wide_root / "research_decision.json",
        {
            "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold",
            "best_research_family_metrics": metrics,
            "decision_reasons": [{"code": "time_block_stability", "value": {"positive_time_block_rate": 0.9}}],
        },
    )
    _write_json(
        risk_root / "research_decision.json",
        {"best_risk_family_metrics": {"selected_top3_severe_loss_rate20": 0.225}},
    )


def _write_r11(artifact: Path, gate: Path) -> None:
    _write_json(artifact, {"family_id": "tradex-research-buy-surface-operational-validation-r11-r1-defensive"})
    _write_json(
        gate,
        {
            "authoritative_decision": "keep",
            "metrics": {
                "candidate_symbol_count": 3,
                "changed_top5_members_count": 8,
                "changed_top10_members_count": 13,
                "changed_rank_count": 13,
                "top5_uplift": 0.228,
                "top10_uplift": 0.132,
                "bad_pick_removal": 0.309,
                "worst_regime_delta": -0.066,
            },
        },
    )


def _run(tmp_path: Path, roots: dict[str, Path]) -> dict[str, object]:
    return mod.run_pattern_family_portfolio_refresh_v1(output_parent=tmp_path / "out", run_id="refresh", **roots)


def test_refresh_selects_non_pre_strength_non_teppan_families(tmp_path: Path) -> None:
    payload = _run(tmp_path, _roots(tmp_path))

    research = payload["research_decision"]
    selected = payload["selected_pattern_families_for_validation"]["selected_pattern_families"]
    selected_ids = {row["family_id"] for row in selected}

    assert research["decision"] == "keep_candidate"
    assert research["authoritative_research_decision"] == "pattern_family_portfolio_refreshed_next_validation_ready"
    assert 1 <= research["selected_family_count"] <= 3
    assert "momentum_continuation_soft_boost_v1" in selected_ids
    wide = next(row for row in selected if row["family_id"] == "momentum_continuation_soft_boost_v1")
    assert wide["quality"]["time_block_positive_rate"] == 0.9
    assert any(family_id.startswith("ma5_reclaim_context::") for family_id in selected_ids)
    assert not any("pre_strength" in family_id for family_id in selected_ids)
    assert "teppan_watch_only" not in selected_ids
    assert "r11_weak_liquidity_defensive_operational_lane" not in selected_ids
    assert research["production_ranking_changed"] is False
    assert research["meemee_reflectable"] is False
    assert research["silent_fallback_used"] is False
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_refresh_holds_when_remaining_family_quality_is_below_gate(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    _write_ma5(roots["ma5_root"], weak=True)
    _write_wide(roots["wide_strength_root"], roots["selection_risk_root"], weak=True)

    payload = _run(tmp_path, roots)

    assert payload["research_decision"]["decision"] == "hold"
    assert payload["research_decision"]["authoritative_research_decision"] == "pattern_family_portfolio_refresh_hold"
    assert payload["selected_pattern_families_for_validation"]["selection_count"] == 0
    assert payload["artifact_complete"]["complete"] is True


def test_refresh_drops_when_all_remaining_families_are_excluded(tmp_path: Path) -> None:
    roots = _roots(tmp_path)
    _write_json(roots["wide_strength_root"] / "research_decision.json", {"best_research_family_metrics": {}})
    _write_json(roots["ma5_root"] / "research_decision.json", {"excellent_hypotheses": []})

    payload = _run(tmp_path, roots)

    assert payload["research_decision"]["decision"] == "drop"
    assert payload["next_axis_recommendation"]["next"] == "broader_candidate_generation_design_reset_v1"
    assert payload["artifact_complete"]["present_outputs"]["_ARTIFACT_COMPLETE.json"] is True
