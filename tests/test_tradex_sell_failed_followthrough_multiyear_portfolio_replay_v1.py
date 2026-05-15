from __future__ import annotations

from scripts import tradex_sell_failed_followthrough_multiyear_portfolio_replay_v1 as replay


def _series(values: list[tuple[int, float, float, float, float]]) -> dict:
    return {
        "ymd": replay.np.array([row[0] for row in values], dtype=replay.np.int64),
        "o": replay.np.array([row[1] for row in values], dtype=replay.np.float64),
        "h": replay.np.array([row[2] for row in values], dtype=replay.np.float64),
        "l": replay.np.array([row[3] for row in values], dtype=replay.np.float64),
        "c": replay.np.array([row[4] for row in values], dtype=replay.np.float64),
        "v": replay.np.array([100000.0 for _ in values], dtype=replay.np.float64),
    }


def test_short_replay_stop_reduces_adverse_loss() -> None:
    tail_dates = [
        20250205,
        20250206,
        20250207,
        20250210,
        20250212,
        20250213,
        20250214,
        20250217,
        20250218,
        20250219,
        20250220,
        20250221,
        20250225,
        20250226,
        20250227,
        20250228,
        20250303,
        20250304,
        20250305,
        20250306,
        20250307,
        20250310,
        20250311,
        20250312,
        20250313,
    ]
    rows = [
        {
            "row_id": "20250131:1001",
            "ymd": 20250131,
            "code": "1001",
            "baseline_rank": 1,
            "tradePriorityScore": 100.0,
            "entryScore": 100.0,
            "marketRegime": "risk_on",
        }
    ]
    price_store = {
        "1001": _series(
            [
                (20250131, 100.0, 100.0, 100.0, 100.0),
                (20250203, 100.0, 106.0, 99.0, 104.0),
                (20250204, 104.0, 107.0, 103.0, 106.0),
            ]
            + [(ymd, 106.0, 107.0, 105.0, 106.0) for ymd in tail_dates]
        )
    }
    selection = {"added_rows": [rows[0]], "removed_rows": []}

    fixed = replay._simulate_portfolio(
        rows=rows,
        selection=selection,
        price_store=price_store,
        exit_variant="fixed_horizon_20d_exit",
        start_ymd=20250131,
        end_ymd=20250331,
        label="challenger",
    )
    stop = replay._simulate_portfolio(
        rows=rows,
        selection=selection,
        price_store=price_store,
        exit_variant="stop_loss_at_negative_5pct_or_20d",
        start_ymd=20250131,
        end_ymd=20250331,
        label="challenger",
    )

    assert fixed["summary"]["number_of_trades"] == 1
    assert stop["summary"]["number_of_trades"] == 1
    assert stop["trades"][0]["exit_reason"] == "stop_loss_5pct"
    assert stop["trades"][0]["net_return"] > fixed["trades"][0]["net_return"]


def test_shadow_trade_decision_holds_when_no_exit_variant_survives() -> None:
    variant_results = {
        "fixed_horizon_20d_exit": {
            "challenger": {
                "summary": {
                    "total_return": -0.1,
                    "max_drawdown": -0.2,
                    "severe_loser_count": 3,
                }
            }
        }
    }
    yearly = {
        "fixed_horizon_20d_exit": [
            {"classification": "negative", "return_on_base_capital": -0.1},
        ]
    }
    decision = replay._decide(
        variant_results=variant_results,
        yearly=yearly,
        no_lookahead={"no_lookahead_pass": True},
        severe_loser={"portfolio_replay_severe_loser_controlled": True},
        added_bad_pick={"added_bad_pick_impact": {"fixed_horizon_20d_added_bad_pick_pnl": 0.0}},
    )

    assert decision["decision"] == "drop_after_multiyear_replay"
    assert decision["shadow_trade_candidate"] is False
    assert "no_fixed_exit_variant_survived" in decision["blockers"]
