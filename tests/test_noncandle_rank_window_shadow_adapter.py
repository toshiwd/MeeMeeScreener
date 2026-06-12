from __future__ import annotations

from app.backend.services.noncandle_rank_window_shadow_adapter import (
    compute_cnt60up_rank_window_shadow_ranking,
    compute_liquidity_rank_window_shadow_ranking,
    compute_monthly_range_rank_window_shadow_ranking,
)


def test_cnt60up_shadow_adapter_reorders_rank_window_without_mutating_active_rows() -> None:
    active_rows = [
        {"anchor_date": "2026-06-05", "symbol": "4825", "side": "long", "rank": 1, "display_score": 0.43, "cnt60Up": 22},
        {"anchor_date": "2026-06-05", "symbol": "9338", "side": "long", "rank": 2, "display_score": 0.08, "cnt60Up": 4},
    ]
    original_rows = [dict(row) for row in active_rows]

    payload = compute_cnt60up_rank_window_shadow_ranking(active_rows)

    assert active_rows == original_rows
    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["9338"]["shadow_adjusted_rank"] == 1
    assert by_symbol["9338"]["shadow_decision_reason"] == "cnt60up_rank_window_pass"
    assert by_symbol["4825"]["shadow_adjusted_rank"] == 2
    assert by_symbol["4825"]["shadow_decision_reason"] == "cnt60up_rank_window_demoted"
    assert payload["summary"]["changed_rank_count"] == 2
    assert payload["summary"]["cnt60up_pass_count"] == 1
    assert payload["summary"]["cnt60up_demoted_count"] == 1
    assert payload["audit"]["active_ranking_invariance_pass"] is True
    assert payload["audit"]["runtime_duckdb_write_attempted"] is False
    assert payload["audit"]["production_registry_write_attempted"] is False
    assert payload["audit"]["adjusted_rank_separate"] is True


def test_cnt60up_shadow_adapter_missing_feature_uses_no_silent_fallback() -> None:
    payload = compute_cnt60up_rank_window_shadow_ranking(
        [
            {"anchor_date": "2026-06-05", "symbol": "1001", "side": "long", "rank": 1, "display_score": 0.9},
            {"anchor_date": "2026-06-05", "symbol": "1002", "side": "long", "rank": 2, "display_score": 0.8, "cnt60Up": 40},
        ]
    )

    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["1001"]["shadow_decision_reason"] == "missing_cnt60up_no_silent_fallback"
    assert by_symbol["1002"]["shadow_decision_reason"] == "cnt60up_rank_window_demoted"
    assert payload["summary"]["missing_feature_row_count"] == 1


def test_liquidity_shadow_adapter_reorders_rank_window_without_mutating_active_rows() -> None:
    active_rows = [
        {"anchor_date": "2026-06-05", "symbol": "7794", "side": "long", "rank": 1, "display_score": 0.99, "liquidity20d": 868580.2},
        {"anchor_date": "2026-06-05", "symbol": "7776", "side": "long", "rank": 2, "display_score": 0.97, "liquidity20d": 90155.9},
    ]
    original_rows = [dict(row) for row in active_rows]

    payload = compute_liquidity_rank_window_shadow_ranking(active_rows)

    assert active_rows == original_rows
    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["7776"]["shadow_adjusted_rank"] == 1
    assert by_symbol["7776"]["shadow_decision_reason"] == "liquidity20d_rank_window_pass"
    assert by_symbol["7794"]["shadow_adjusted_rank"] == 2
    assert by_symbol["7794"]["shadow_decision_reason"] == "liquidity20d_rank_window_demoted"
    assert payload["summary"]["changed_rank_count"] == 2
    assert payload["summary"]["liquidity20d_pass_count"] == 1
    assert payload["summary"]["liquidity20d_demoted_count"] == 1
    assert payload["audit"]["active_ranking_invariance_pass"] is True
    assert payload["audit"]["runtime_duckdb_write_attempted"] is False
    assert payload["audit"]["production_registry_write_attempted"] is False


def test_monthly_range_shadow_adapter_reorders_top10_without_mutating_active_rows() -> None:
    active_rows = [
        {"anchor_date": "2026-06-05", "symbol": "4825", "side": "long", "rank": 1, "display_score": 0.43, "monthlyRangeProb": 0.6746114162581928},
        {"anchor_date": "2026-06-05", "symbol": "9338", "side": "long", "rank": 2, "display_score": 0.08, "monthlyRangeProb": 0.0},
        {"anchor_date": "2026-06-05", "symbol": "9999", "side": "long", "rank": 11, "display_score": 0.07, "monthlyRangeProb": 0.0},
    ]
    original_rows = [dict(row) for row in active_rows]

    payload = compute_monthly_range_rank_window_shadow_ranking(active_rows)

    assert active_rows == original_rows
    by_symbol = {row["symbol"]: row for row in payload["shadow_rows"]}
    assert by_symbol["9338"]["shadow_adjusted_rank"] == 1
    assert by_symbol["9338"]["shadow_decision_reason"] == "monthly_range_prob_rank_window_pass"
    assert by_symbol["4825"]["shadow_adjusted_rank"] == 2
    assert by_symbol["4825"]["shadow_decision_reason"] == "monthly_range_prob_rank_window_demoted"
    assert by_symbol["9999"]["shadow_adjusted_rank"] == 3
    assert by_symbol["9999"]["shadow_decision_reason"] == "outside_rank_window_after_no_change"
    assert payload["summary"]["changed_rank_count"] == 3
    assert payload["summary"]["monthly_range_prob_pass_count"] == 1
    assert payload["summary"]["monthly_range_prob_demoted_count"] == 1
    assert payload["audit"]["active_ranking_invariance_pass"] is True
    assert payload["audit"]["runtime_duckdb_write_attempted"] is False
    assert payload["audit"]["production_registry_write_attempted"] is False
