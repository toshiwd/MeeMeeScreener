from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_oos_v1 import _build_slope_floor_candidates, _oos_decision, _subset_groups, _subset_selected


def test_subset_helpers_filter_dates() -> None:
    groups = {20250101: [{"code": "1"}], 20260301: [{"code": "2"}]}
    selected = {20250101: {"code": "1"}, 20260301: {"code": "2"}}

    assert set(_subset_groups(groups, 20250101, 20251231)) == {20250101}
    assert set(_subset_selected(selected, 20250101, 20251231)) == {20250101}


def test_oos_decision_keeps_stable_positive_without_loser_damage() -> None:
    compare = {
        "top5": {
            "changed_member_count_total": 12,
            "additive_delta": {
                "forward_return_20_mean": 0.003,
                "bad_loser_rate_20": 0.0,
                "severe_loser_rate_20": 0.0,
            },
        },
        "top10": {
            "additive_delta": {
                "forward_return_20_mean": 0.0,
            },
        },
    }
    months = [
        {"month": "202501", "changed_top5_members_count": 2, "top5_delta_mean": 0.01},
        {"month": "202502", "changed_top5_members_count": 2, "top5_delta_mean": 0.01},
        {"month": "202503", "changed_top5_members_count": 2, "top5_delta_mean": -0.001},
    ]
    coverage = {"oos_selected_additive_date_count": 10}

    assert _oos_decision(compare, months, coverage) == {
        "judgment": "keep",
        "reason_type": "oos_top5_improves_without_loser_damage",
    }


def test_build_slope_floor_candidates_skips_too_steep_before_classification(monkeypatch) -> None:
    import scripts.tradex_short_scene_visual_additive_a_phase_slope_floor_oos_v1 as mod

    bars = []
    price = 100.0
    for index in range(182):
        price -= 1.0
        bars.append({"ymd": 20250101 + index, "o": price, "h": price + 1.0, "l": price - 1.0, "c": price, "v": 1000})

    called = {"shape": 0}

    def fake_shape(_bars):
        called["shape"] += 1
        return {"market_scene": "downtrend_a_phase", "action_bias": "sell_rebound_rejection_or_lower_low", "trade_side": "short"}

    monkeypatch.setattr(mod, "_ma20_slope_10", lambda _window: -0.02)
    monkeypatch.setattr(mod, "classify_shape_from_bars", fake_shape)

    result = _build_slope_floor_candidates(
        {"1001": bars},
        sell_codes_by_date={20250170: set()},
        start_dt=20250101,
        end_dt=20250180,
    )

    assert result == []
    assert called["shape"] == 0
