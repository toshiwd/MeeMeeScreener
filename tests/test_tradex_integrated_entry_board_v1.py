from scripts.tradex_integrated_entry_board_v1 import build_board


def test_board_separates_actionable_and_watch_buy_candidates() -> None:
    router = {
        "current_as_of": "2026-07-10", "current_regime": "broad_up",
        "current_candidates": [
            {"code": "1111", "rule": "leaf9", "confirmed_close": 100, "router_rule": "leaf9", "router_state": "Active", "router_score": 2.0, "router_priority_rank": 1, "router_verdict": "review_entry"},
            {"code": "2222", "rule": "clean_breakout", "confirmed_close": 200, "router_rule": "clean_breakout", "router_state": "Secondary", "router_score": 1.0, "router_verdict": "watch_not_routed"},
        ],
    }
    result = build_board(router, {"status": "no_newer_provisional_bar", "intraday_available": False})
    assert result["directional_bias"] == "buy_priority"
    assert [row["code"] for row in result["actionable"]] == ["1111"]
    assert [row["code"] for row in result["watch"]] == ["2222"]


def test_provisional_short_is_not_presented_as_official_close_signal() -> None:
    router = {"current_as_of": "2026-07-10", "current_regime": "broad_up", "current_candidates": []}
    preview = {
        "status": "provisional_intraday_preview", "intraday_available": True, "market_gate_pass": True,
        "candidates": [{"code": "3333", "provisional_ymd": 20260713, "price": 90, "volume_vs20": 4.0, "intraday_rank": 1}],
        "near_matches": [],
    }
    result = build_board(router, preview)
    row = result["actionable"][0]
    assert result["directional_bias"] == "two_sided_review"
    assert row["data_state"] == "provisional_intraday"
    assert row["decision"] == "preclose_review"
    assert row["automatic_trade"] is False
