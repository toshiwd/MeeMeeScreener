from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_multi_pattern_candidate_generation_portfolio_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _artifact_roots(tmp_path: Path) -> dict[str, Path]:
    roots = {
        "pre_strength_root": tmp_path / "pre_strength",
        "guard_root": tmp_path / "guard",
        "source_v2_root": tmp_path / "source_v2",
        "iizuka_phase3_root": tmp_path / "iizuka_phase3",
        "iizuka_phase3b_root": tmp_path / "iizuka_phase3b",
        "ma5_root": tmp_path / "ma5",
        "swing_root": tmp_path / "swing",
        "relative_root": tmp_path / "relative",
        "teppan_watch_root": tmp_path / "teppan_watch",
    }
    _write_pre_strength(roots["pre_strength_root"])
    _write_guard(roots["guard_root"])
    _write_source_v2(roots["source_v2_root"])
    _write_iizuka(roots["iizuka_phase3_root"], roots["iizuka_phase3b_root"])
    _write_ma5(roots["ma5_root"])
    _write_swing(roots["swing_root"])
    _write_relative(roots["relative_root"])
    _write_teppan_watch(roots["teppan_watch_root"])
    return roots


def _write_pre_strength(root: Path, *, include_rows: bool = True, weak_rows: bool = False) -> None:
    rows: list[dict[str, object]] = []
    if include_rows:
        scale = 0.1 if weak_rows else 1.0
        rows = [
            {
                "family_id": "pre_reclaim_accumulation",
                "pattern_key": "reclaim_volume_weekly",
                "pattern_decision": "high_return_pre_strength_pattern",
                "event_count": int(280 * scale),
                "month_count": int(70 * scale),
                "symbol_count": 90,
                "avg_ret20": 0.044 if not weak_rows else 0.002,
                "median_ret20": 0.019,
                "win_rate20": 0.61 if not weak_rows else 0.50,
                "profit_factor20": 2.4 if not weak_rows else 1.0,
                "severe_loss_rate20": 0.16 if not weak_rows else 0.31,
                "avg_mfe20": 0.095,
                "avg_mae20": -0.052,
                "positive_month_rate20": 0.66,
                "pattern_features": {"reclaim": True, "volume_expansion": True, "weekly_prior_state": "strong_up"},
            },
            {
                "family_id": "pre_to_event_confirmation",
                "pattern_key": "confirmation_compact",
                "pattern_decision": "high_win_pre_strength_pattern",
                "event_count": int(150 * scale),
                "month_count": int(50 * scale),
                "symbol_count": 60,
                "avg_ret20": 0.031 if not weak_rows else 0.001,
                "median_ret20": 0.014,
                "win_rate20": 0.58 if not weak_rows else 0.49,
                "profit_factor20": 1.9 if not weak_rows else 1.0,
                "severe_loss_rate20": 0.18 if not weak_rows else 0.32,
                "avg_mfe20": 0.083,
                "avg_mae20": -0.048,
                "positive_month_rate20": 0.62,
                "pattern_features": {"event_confirmation": True, "compression": True},
            },
        ]
    _write_json(root / "research_decision.json", {"decision": "promising_pre_strength_patterns_found"})
    _write_json(root / "pattern_leaderboard.json", {"rows": rows})


def _write_guard(root: Path, *, include_safe_full: bool = True, weak: bool = False) -> None:
    rows: list[dict[str, object]] = []
    if include_safe_full:
        rows.append(
            {
                "guard_id": "safe_full",
                "n": 359 if not weak else 25,
                "opportunity_days_count": 70 if not weak else 4,
                "coverage_rate": 0.22,
                "events_per_day_distribution": {"mean": 5.1},
                "avg_ret20": 0.031 if not weak else 0.0,
                "median_ret20": 0.011,
                "win_rate20": 0.573 if not weak else 0.50,
                "severe_loss_rate20": 0.083 if not weak else 0.31,
                "avg_MFE20": 0.074,
                "avg_MAE20": -0.031,
                "severe_loss_improvement_rate_vs_all_strength": 0.58,
            }
        )
    _write_json(root / "research_decision.json", {"decision": "pre_strength_guard_hold"})
    _write_json(root / "guard_leaderboard.json", {"rows": rows})


def _write_source_v2(root: Path) -> None:
    _write_json(root / "research_decision.json", {"authoritative_research_decision": "source_specific_candidate_generation_v2_drop"})
    _write_json(root / "top5_candidate_pool_report.json", {"changed_top5_members_count_vs_previous_best": 201})
    _write_json(
        root / "source_noise_report.json",
        {
            "recovered_missed_winner_count": 51,
            "severe_loser_added_per_recovered_winner": 1.07,
            "nonwinner_added_per_recovered_winner": 2.25,
        },
    )


def _write_iizuka(phase3_root: Path, phase3b_root: Path) -> None:
    _write_json(phase3_root / "phase3_decision.json", {"signal_only_count": 64})
    _write_json(
        phase3b_root / "phase3b_decision.json",
        {
            "added_signal_candidate_count": 64,
            "additive_candidate_count": 2539,
            "ret20_mean_delta": 0.017,
            "ret20_median_delta": 0.006,
            "win_rate20_delta": 0.05,
            "severe_loser_delta": 0.17,
        },
    )


def _write_ma5(root: Path) -> None:
    _write_json(root / "research_decision.json", {"decision": "excellent_hypothesis_found", "reentry_expansion_modeled": False})
    _write_json(
        root / "hypothesis_leaderboard.json",
        {
            "rows": [
                {
                    "hypothesis_id": "h12",
                    "thesis": "MA5 reclaim quality context",
                    "trade_count": 120,
                    "symbol_count": 50,
                    "avg_ret": 0.04,
                    "avg_ret_delta_vs_base": 0.02,
                    "profit_factor": 3.0,
                    "severe_loss_rate": 0.04,
                }
            ]
        },
    )


def _write_swing(root: Path) -> None:
    _write_json(
        root / "family_leaderboard.json",
        {
            "candidate_rows": [
                {
                    "changed_top5_members_count": 42,
                    "top5_hold_end_return_20d_delta": -0.01,
                    "top5_severe_loss_rate_delta": 0.02,
                }
            ]
        },
    )


def _write_relative(root: Path) -> None:
    _write_json(
        root / "relative_strength_family_final_decision.json",
        {
            "final_decision": "drop",
            "key_metrics": {
                "reranker": {
                    "changed_top5_members_count": 64,
                    "top5_return_delta": -0.004,
                    "top5_severe_loser_rate_delta": 0.01,
                }
            },
        },
    )


def _write_teppan_watch(root: Path) -> None:
    _write_json(root / "watch_run_result.json", {"decision": "continue_watch_only", "activation_allowed": False})
    _write_json(root / "teppan_watch_metrics.json", {"top100_teppan_pattern_match_count": 0, "boost_eligible_count": 0})


def test_portfolio_selects_one_to_three_non_teppan_families(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)

    payload = mod.run_multi_pattern_candidate_generation_portfolio_v1(
        output_parent=tmp_path / "out",
        run_id="portfolio",
        **roots,
    )

    decision = payload["research_decision"]
    selected = payload["selected_pattern_families_for_validation"]["selected_pattern_families"]
    selected_ids = {row["family_id"] for row in selected}

    assert decision["decision"] == "pattern_family_portfolio_ready"
    assert 1 <= decision["selected_family_count"] <= 3
    assert any(family_id.startswith("pre_strength::") for family_id in selected_ids)
    assert "defensive_safe_full_guard::safe_full" in selected_ids
    assert "teppan_watch_only" not in selected_ids
    assert "archived_source_specific_candidate_generation_v2" not in selected_ids
    assert decision["activation_allowed"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert payload["pattern_family_overlap_report"]["selected_portfolio_overlap_assessment"] == "acceptable_low_overlap"
    assert payload["artifact_complete"]["complete"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_portfolio_holds_when_candidates_exist_but_quality_is_below_threshold(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)
    _write_pre_strength(roots["pre_strength_root"], weak_rows=True)
    _write_guard(roots["guard_root"], weak=True)

    payload = mod.run_multi_pattern_candidate_generation_portfolio_v1(
        output_parent=tmp_path / "out",
        run_id="portfolio",
        **roots,
    )

    assert payload["research_decision"]["decision"] == "hold"
    assert payload["selected_pattern_families_for_validation"]["selection_count"] == 0
    assert payload["artifact_complete"]["complete"] is True


def test_portfolio_drops_when_no_usable_non_teppan_family_exists(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)
    _write_pre_strength(roots["pre_strength_root"], include_rows=False)
    _write_guard(roots["guard_root"], include_safe_full=False)

    payload = mod.run_multi_pattern_candidate_generation_portfolio_v1(
        output_parent=tmp_path / "out",
        run_id="portfolio",
        **roots,
    )

    assert payload["research_decision"]["decision"] == "drop_redesign"
    assert payload["next_axis_recommendation"]["next"] == "broader_candidate_generation_design_reset_v1"
    inventory_rows = payload["artifact_complete"]["present_outputs"]
    assert inventory_rows["_ARTIFACT_COMPLETE.json"] is True
