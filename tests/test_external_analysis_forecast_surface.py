from __future__ import annotations

from external_analysis.models.forecast_surface import _SourceContext, _build_surface_row


def test_surface_heuristic_probability_is_side_aware_for_momentum() -> None:
    base_row = {
        "as_of_date": 20260312,
        "code": "1301",
        "ranking_score_long": 0.0,
        "ranking_score_short": 0.0,
        "close_price": 100.0,
        "close_vs_ma20": 0.08,
        "ret_20_past": 0.10,
        "atr_ratio": 0.0,
        "volume_ratio": 1.0,
        "box_state": "none",
        "ppp_state": "none",
        "abc_state": "none",
    }
    source_context = _SourceContext(
        signal={},
        trade={},
        borrow={},
        events={},
        edinet={},
        market={},
        presence={},
    )

    long_row = _build_surface_row(
        base_row,
        side="long",
        source_context=source_context,
        market_context={},
        publish_id="test",
        freshness_state="fresh",
    )
    short_row = _build_surface_row(
        base_row,
        side="short",
        source_context=source_context,
        market_context={},
        publish_id="test",
        freshness_state="fresh",
    )

    assert float(long_row["direction_prob"]) > 0.5
    assert float(short_row["direction_prob"]) < 0.5
    assert float(long_row["direction_prob"]) > float(short_row["direction_prob"])
