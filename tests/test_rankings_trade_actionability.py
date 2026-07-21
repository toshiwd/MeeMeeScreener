from __future__ import annotations

import app.backend.services.codex_bridge_service as bridge
from app.backend.services.ml import rankings_cache


def _fake_runtime_guard(*, stale: bool = False) -> dict[str, object]:
    runtime_status = {
        "stale": stale,
        "freshness_state": "stale" if stale else "fresh",
        "freshness_days": 7 if stale else 1,
        "selected_runtime_db_path": "C:/runtime/stocks.duckdb",
    }
    rankings_status = {
        "long": {
            "stale": stale,
            "freshness_state": "stale" if stale else "fresh",
            "freshness_days": 7 if stale else 1,
            "note": "runtime DB freshness is stale; rankings reflect stale local data" if stale else None,
        },
        "short": {
            "stale": stale,
            "freshness_state": "stale" if stale else "fresh",
            "freshness_days": 7 if stale else 1,
            "note": "runtime DB freshness is stale; rankings reflect stale local data" if stale else None,
        },
    }
    return {
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness": rankings_status,
        "stale": stale,
    }


def _stub_screening_bundle_dependencies(monkeypatch, *, stale: bool = False) -> None:
    monkeypatch.setattr(bridge, "_build_runtime_guard", lambda **kwargs: _fake_runtime_guard(stale=stale))
    monkeypatch.setattr(
        bridge,
        "_build_runtime_warnings",
        lambda guard: ["runtime DB freshness is stale"] if stale else [],
    )
    monkeypatch.setattr(
        bridge,
        "_build_candidate_event_risk",
        lambda **kwargs: {"available": False, "reason": "event_risk_missing", "tdnet_recent": [], "edinet_recent": [], "rights_warning": None},
    )
    monkeypatch.setattr(
        bridge,
        "_build_candidate_supply_demand_risk",
        lambda **kwargs: {"available": False, "reason": "taisyaku_snapshot_missing", "taisyaku_snapshot": None, "borrow_cost_warning": None},
    )
    monkeypatch.setattr(
        bridge,
        "_build_tradex_detail_section",
        lambda *args, **kwargs: {"available": True, "reason": None, "fallback_used": False, "item": {"available": True, "forecast_surface": {"available": True, "reason": None}}},
    )
    monkeypatch.setattr(
        bridge,
        "_build_tradex_similar_cases",
        lambda *args, **kwargs: {"available": False, "reason": "missing", "rows": [], "count": 0},
    )
    monkeypatch.setattr(
        bridge,
        "_build_candidate_tradex_summary",
        lambda **kwargs: {"available": True, "reason": None, "detail_analysis_available": True, "state_eval_available": True, "similar_cases_count": 0, "fallback_used": False},
    )
    monkeypatch.setattr(
        bridge,
        "get_state_eval_rows",
        lambda *args, **kwargs: {
            "rows": [
                {
                    "publish_id": "pub-1",
                    "as_of_date": "2026-04-20",
                    "code": kwargs.get("code", "0001"),
                    "side": "long",
                    "holding_band": "core",
                    "strategy_tags": [],
                    "state_action": "enter",
                    "decision_3way": "enter",
                    "confidence": 0.91,
                    "machine_action_state": "enter",
                    "human_readable_judgement": "enter",
                    "buy_score": 0.81,
                    "environment_score": 0.71,
                    "trend_score": 0.76,
                    "trigger_score": 0.79,
                    "risk_score": 0.12,
                    "invalidation_price": 123.4,
                    "invalidation_reason_code": None,
                    "reason_codes": [],
                    "reason_text_top3": [],
                    "freshness_state": "fresh",
                }
            ],
            "available": True,
            "reason": None,
            "degrade_reason": None,
        },
    )


def test_trade_priority_scores_penalize_etf_market_code(monkeypatch) -> None:
    items = [
        {
            "code": "0001",
            "setupType": "breakout",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.70,
            "hybridScore": 0.70,
            "downsideRisk": 0.20,
            "swingScore": 0.60,
            "mlEv5Net": 0.10,
            "mlEv10Net": 0.10,
            "mlEv20Net": 0.10,
        },
        {
            "code": "0002",
            "setupType": "breakout",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.70,
            "hybridScore": 0.70,
            "downsideRisk": 0.20,
            "swingScore": 0.60,
            "mlEv5Net": 0.10,
            "mlEv10Net": 0.10,
            "mlEv20Net": 0.10,
        },
    ]
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {"0001": "PRIME", "0002": rankings_cache._ETF_MARKET_CODE},
    )

    rankings_cache._apply_trade_priority_scores(items, direction="up")  # type: ignore[attr-defined]
    items.sort(key=rankings_cache._trade_priority_sort_key)  # type: ignore[attr-defined]

    assert [item["code"] for item in items] == ["0001", "0002"]
    assert items[0]["tradePriorityScore"] > items[1]["tradePriorityScore"]


def test_trade_buy_candidates_require_entry_fit_without_becoming_gain_ranking(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, *, change_pct: float, upper_wick: float = 0.12, overextended: bool = False) -> dict[str, object]:
        return {
            "code": code,
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.12,
            "mlEv10Net": 0.12,
            "mlEv20Net": 0.12,
            "changePct": change_pct,
            "candleUpperWickRatio": upper_wick,
            "buy_overextended": overextended,
        }

    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            item("1001", change_pct=-0.003),
            item("1002", change_pct=-0.018),
            item("1003", change_pct=0.046, overextended=True),
            item("1004", change_pct=0.006, upper_wick=0.52),
        ]
    )

    buy_codes = {item["code"] for item in buckets["actionable_buy_candidates"]}
    caution_by_code = {item["code"]: item for item in buckets["caution_watch_candidates"]}

    assert "1001" in buy_codes
    assert "1002" not in buy_codes
    assert "1003" not in buy_codes
    assert "1004" not in buy_codes
    assert caution_by_code["1002"]["tradeEntryBlockReasons"] == ["current_weak_close"]
    assert caution_by_code["1003"]["tradeEntryBlockReasons"] == ["overextended_chase_risk"]
    assert caution_by_code["1004"]["tradeEntryBlockReasons"] == ["upper_wick_rejection"]


def test_trade_candidates_block_late_chase_entries(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def buy_item(code: str, **overrides: object) -> dict[str, object]:
        item: dict[str, object] = {
            "code": code,
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.12,
            "mlEv10Net": 0.12,
            "mlEv20Net": 0.12,
            "changePct": 0.006,
            "candleUpperWickRatio": 0.12,
            "buy_overextended": False,
            "breakout20_up": -0.012,
            "distMa20Signed": 0.035,
            "diff20_pct": 0.04,
        }
        item.update(overrides)
        return item

    def short_item(code: str, **overrides: object) -> dict[str, object]:
        item: dict[str, object] = {
            "code": code,
            "setupType": "pressure",
            "tradeEntryClass": "upper_rejection_primary",
            "monthlyBoxState": "box_mid",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": -0.12,
            "mlEv10Net": -0.12,
            "mlEv20Net": -0.12,
            "changePct": -0.010,
            "candleUpperWickRatio": 0.52,
            "candleLowerWickRatio": 0.12,
            "distMa20Signed": -0.035,
        }
        item.update(overrides)
        return item

    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            buy_item("1001"),
            buy_item("1002", breakout20_up=0.082),
            buy_item("1003", distMa20Signed=0.14),
            buy_item("1004", diff20_pct=0.26),
            buy_item("1005", changePct=0.031, candleUpperWickRatio=0.34, distMa20Signed=0.045),
            buy_item("1006", changePct=0.010, distMa20Signed=0.082),
            buy_item(
                "1007",
                changePct=0.004,
                monthlyBoxState="box_upper",
                monthlyBoxPos=1.0,
                cnt_7_above=5,
                momentumFollowThroughScore=0.62,
            ),
            buy_item(
                "1008",
                changePct=0.004,
                monthlyBoxState="box_upper",
                monthlyBoxPos=1.0,
                cnt_7_above=7,
                momentumFollowThroughScore=0.96,
            ),
            short_item("2001"),
            short_item("2002", changePct=-0.052),
            short_item("2003", candleLowerWickRatio=0.58),
            short_item("2004", distMa20Signed=-0.13),
        ]
    )

    buy_codes = {item["code"] for item in buckets["actionable_buy_candidates"]}
    short_codes = {item["code"] for item in buckets["actionable_short_candidates"]}
    caution_by_code = {item["code"]: item for item in buckets["caution_watch_candidates"]}

    assert "1001" in buy_codes
    assert "1002" not in buy_codes
    assert "1003" not in buy_codes
    assert "1004" not in buy_codes
    assert "1005" not in buy_codes
    assert "1006" not in buy_codes
    assert "1007" not in buy_codes
    assert "1008" in buy_codes
    assert "2001" in short_codes
    assert "2002" not in short_codes
    assert "2003" not in short_codes
    assert "2004" not in short_codes
    assert caution_by_code["1002"]["tradeEntryBlockReasons"] == ["breakout_chase_risk"]
    assert caution_by_code["1003"]["tradeEntryBlockReasons"] == ["extended_above_ma20"]
    assert caution_by_code["1004"]["tradeEntryBlockReasons"] == ["already_large_20d_run"]
    assert caution_by_code["1005"]["tradeEntryBlockReasons"] == ["late_buy_chase_risk"]
    assert caution_by_code["1006"]["tradeEntryBlockReasons"] == ["high_zone_extension_risk"]
    assert caution_by_code["1007"]["tradeEntryBlockReasons"] == ["box_top_no_pullback_chase_risk"]
    assert caution_by_code["2002"]["tradeEntryBlockReasons"] == ["late_breakdown_chase_risk"]
    assert caution_by_code["2003"]["tradeEntryBlockReasons"] == ["lower_wick_rebound_risk"]
    assert caution_by_code["2004"]["tradeEntryBlockReasons"] == ["extended_below_ma20"]


def test_rulebook_display_only_warnings_do_not_promote_trade_candidates(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "code": code,
            "setupType": "watch",
            "monthlyBoxState": "box_upper",
            "entryQualified": False,
            "probSideCalib": 0.60,
            "entryScore": 0.50,
            "hybridScore": 0.50,
            "downsideRisk": 0.30,
            "swingScore": 0.50,
            "mlEv5Net": 0.02,
            "mlEv10Net": 0.02,
            "mlEv20Net": 0.02,
            "changePct": 0.0,
            "candleUpperWickRatio": 0.10,
            "distMa20Signed": 0.02,
            "diff20_pct": 0.04,
        }
        base.update(overrides)
        return base

    items = [
        item("9101", changePct=0.12),
        item("9102", changePct=-0.01, diff20_pct=0.30, closePos=0.25, volumeRatio20=1.5),
        item("9103", previousDistMa20Signed=0.10, distMa20Signed=-0.01),
        item("9104", diff20_pct=0.32, distMa20Signed=0.34, candleUpperWickRatio=0.48, closePos=0.40),
    ]

    rankings_cache._apply_trade_priority_scores(items, direction="up")  # type: ignore[attr-defined]

    watch_by_code = {item["code"]: item["tradeRiskWatch"] for item in items}
    assert "rulebook_post_10pct_spike_chase_caution" in watch_by_code["9101"]
    assert "rulebook_extended_run_weak_close_caution" in watch_by_code["9102"]
    assert "rulebook_extended_gain_ma20_break_exit_review" in watch_by_code["9103"]
    assert "rulebook_extended_run_upper_wick_exit_review" in watch_by_code["9104"]

    buckets = rankings_cache._build_trade_candidate_buckets(items)  # type: ignore[attr-defined]
    assert buckets["actionable_buy_candidates"] == []
    assert buckets["actionable_short_candidates"] == []


def test_trade_short_candidates_require_failed_rebound_entry_setup(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def short_item(code: str, **overrides: object) -> dict[str, object]:
        item: dict[str, object] = {
            "code": code,
            "setupType": "failed_high_retest",
            "tradeEntryClass": "failed_high_retest_short",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": -0.12,
            "mlEv10Net": -0.12,
            "mlEv20Net": -0.12,
            "changePct": -0.010,
            "candleUpperWickRatio": 0.52,
            "candleLowerWickRatio": 0.12,
            "distMa20Signed": -0.035,
            "failedHighRetestShort": True,
            "failedHighRetestRetestRatio": 0.98,
            "failedHighRetestAnchorDropPct": 0.006,
        }
        item.update(overrides)
        return item

    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            short_item("3001"),
            short_item("3002", failedHighRetestRetestRatio=0.92),
            short_item("3003", changePct=0.006, candleUpperWickRatio=0.12, failedHighRetestAnchorDropPct=0.001),
            short_item("3005", failedHighRetestRetestRatio=0.945, candleUpperWickRatio=0.62),
            {
                "code": "3004",
                "setupType": "breakdown",
                "tradeEntryClass": "strict_breakdown_secondary",
                "monthlyBoxState": "box_lower",
                "entryQualified": True,
                "probSideCalib": 0.80,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.18,
                "swingScore": 0.60,
                "mlEv5Net": -0.12,
                "mlEv10Net": -0.12,
                "mlEv20Net": -0.12,
                "changePct": -0.010,
                "candleLowerWickRatio": 0.12,
                "distMa20Signed": -0.035,
            },
        ]
    )

    short_codes = {item["code"] for item in buckets["actionable_short_candidates"]}
    caution_by_code = {item["code"]: item for item in buckets["caution_watch_candidates"]}

    assert "3001" in short_codes
    assert "3005" in short_codes
    assert "3002" not in short_codes
    assert "3003" not in short_codes
    assert "3004" not in short_codes
    assert caution_by_code["3002"]["tradeEntryBlockReasons"] == ["not_near_retest_or_resistance"]
    assert caution_by_code["3003"]["tradeEntryBlockReasons"] == ["no_failed_rebound_short_setup"]
    assert caution_by_code["3004"]["tradeEntryBlockReasons"] == ["momentum_down_but_not_actionable_entry"]


def test_trade_short_entry_actionability_lifts_better_failed_retest(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, *, retest_ratio: float, upper_wick: float, lower_wick: float) -> dict[str, object]:
        return {
            "code": code,
            "setupType": "failed_high_retest",
            "tradeEntryClass": "failed_high_retest_short",
            "monthlyBoxState": "box_mid",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": -0.12,
            "mlEv10Net": -0.12,
            "mlEv20Net": -0.12,
            "changePct": 0.002,
            "candleUpperWickRatio": upper_wick,
            "candleLowerWickRatio": lower_wick,
            "failedHighRetestShort": True,
            "failedHighRetestRetestRatio": retest_ratio,
            "failedHighRetestAnchorDropPct": 0.006,
        }

    items = [
        item("3101", retest_ratio=0.985, upper_wick=0.62, lower_wick=0.10),
        item("3102", retest_ratio=0.945, upper_wick=0.36, lower_wick=0.40),
    ]

    rankings_cache._apply_trade_priority_scores(items, direction="down")  # type: ignore[attr-defined]
    items.sort(key=rankings_cache._trade_priority_sort_key)  # type: ignore[attr-defined]

    assert items[0]["code"] == "3101"
    assert items[0]["shortEntryActionabilityScore"] > items[1]["shortEntryActionabilityScore"]
    assert items[0]["tradePriorityScore"] > items[1]["tradePriorityScore"]


def test_trade_short_momentum_follow_through_lifts_downside_continuation(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, *, diff20: float, breakout_down: float, dist_ma20: float, trend_down: bool) -> dict[str, object]:
        return {
            "code": code,
            "setupType": "failed_high_retest",
            "tradeEntryClass": "failed_high_retest_short",
            "monthlyBoxState": "box_mid",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": -0.12,
            "mlEv10Net": -0.12,
            "mlEv20Net": -0.12,
            "changePct": -0.004,
            "candleUpperWickRatio": 0.62,
            "candleLowerWickRatio": 0.10,
            "failedHighRetestShort": True,
            "failedHighRetestRetestRatio": 0.98,
            "failedHighRetestAnchorDropPct": 0.006,
            "diff20_pct": diff20,
            "breakout20_down": breakout_down,
            "distMa20Signed": dist_ma20,
            "trendDown": trend_down,
            "trendDownStrict": trend_down,
            "market_ret20": -0.03,
        }

    items = [
        item("3151", diff20=-0.08, breakout_down=0.045, dist_ma20=-0.045, trend_down=True),
        item("3152", diff20=0.01, breakout_down=0.00, dist_ma20=0.015, trend_down=False),
    ]

    rankings_cache._apply_trade_priority_scores(items, direction="down")  # type: ignore[attr-defined]
    items.sort(key=rankings_cache._trade_priority_sort_key)  # type: ignore[attr-defined]

    assert items[0]["code"] == "3151"
    assert items[0]["shortMomentumFollowThroughV1"] is True
    assert items[0]["shortMomentumFollowThroughScore"] > items[1]["shortMomentumFollowThroughScore"]
    assert items[0]["tradePriorityScore"] > items[1]["tradePriorityScore"]


def test_trade_short_momentum_follow_through_creates_actionable_short_family(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "code": code,
            "setupType": "reject",
            "monthlyBoxState": "no_box",
            "entryQualified": False,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": -0.12,
            "mlEv10Net": -0.12,
            "mlEv20Net": -0.12,
            "changePct": -0.004,
            "candleUpperWickRatio": 0.20,
            "candleLowerWickRatio": 0.10,
            "diff20_pct": -0.08,
            "breakout20_down": 0.045,
            "distMa20Signed": -0.045,
            "trendDown": True,
            "trendDownStrict": True,
            "market_ret20": -0.03,
        }
        base.update(overrides)
        return base

    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            item("3161"),
            item("3162", candleLowerWickRatio=0.58),
            item("3163", distMa20Signed=-0.13),
            item("3164", changePct=-0.052),
            item("3165", mlEv5Net=None, mlEv10Net=None, mlEv20Net=None),
            item("3166", distMa20Signed=-0.08),
        ]
    )

    short_by_code = {item["code"]: item for item in buckets["actionable_short_candidates"]}
    caution_by_code = {item["code"]: item for item in buckets["caution_watch_candidates"]}

    assert short_by_code["3161"]["setupType"] == "breakdown"
    assert short_by_code["3161"]["tradeEntryClass"] == "short_momentum_follow_through"
    assert short_by_code["3161"]["shortMomentumFollowThroughV1"] is True
    assert "3162" not in short_by_code
    assert "3163" not in short_by_code
    assert "3164" not in short_by_code
    assert "3165" not in short_by_code
    assert "3166" not in short_by_code
    assert "lower_wick_rebound_risk" in caution_by_code["3162"]["tradeEntryBlockReasons"]
    assert "extended_below_ma20" in caution_by_code["3163"]["tradeEntryBlockReasons"]
    assert "late_breakdown_chase_risk" in caution_by_code["3164"]["tradeEntryBlockReasons"]
    assert "missing_short_profit_expectancy" in caution_by_code["3165"]["tradeEntryBlockReasons"]
    assert "extended_below_ma20_profit_risk" in caution_by_code["3166"]["tradeEntryBlockReasons"]


def test_short_entry_decision_contract_explains_zero_candidates() -> None:
    contract = rankings_cache._build_short_entry_decision_contract(  # type: ignore[attr-defined]
        {
            "actionable_short_candidates": [],
            "caution_watch_candidates": [
                {"code": "1001", "tradeEntryBlockReasons": ["missing_short_profit_expectancy"]},
                {
                    "code": "1002",
                    "tradeEntryBlockReasons": [
                        "missing_short_profit_expectancy",
                        "extended_below_ma20_profit_risk",
                    ],
                },
            ],
        }
    )

    assert contract["status"] == "no_eligible_short"
    assert contract["actionable_short_count"] == 0
    assert contract["excluded_short_count"] == 2
    assert contract["exclusion_reason_counts"] == {
        "missing_short_profit_expectancy": 2,
        "extended_below_ma20_profit_risk": 1,
    }
    assert "ありません" in contract["message"]


def test_long_candidates_require_profit_expectancy() -> None:
    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            {
                "code": "1101",
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
                "entryQualified": True,
                "probSideCalib": 0.80,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.18,
                "swingScore": 0.60,
                "mlEv5Net": None,
                "mlEv10Net": None,
                "mlEv20Net": None,
                "changePct": 0.004,
                "candleUpperWickRatio": 0.10,
                "distMa20Signed": 0.02,
                "diff20_pct": 0.04,
            }
        ]
    )

    assert buckets["actionable_buy_candidates"] == []
    assert "missing_long_profit_expectancy" in buckets["caution_watch_candidates"][0]["tradeEntryBlockReasons"]
    assert buckets["setup_watch_candidates"][0]["code"] == "1101"
    assert buckets["setup_watch_candidates"][0]["longWatchClass"] == "setup_watch"


def test_trade_buy_quality_blocks_low_priority_and_stale_ml_prediction() -> None:
    low_priority_item = {
        "code": "9434",
        "asOf": "2026-04-24",
        "setupType": "breakout",
        "monthlyBoxState": "box_mid",
        "entryQualified": True,
        "tradePriorityScore": 0.44,
        "modelVersion": "20260314153632",
        "mlPredDt": 1776643200,
        "mlEv5Net": 0.01,
        "mlEv10Net": 0.01,
        "mlEv20Net": 0.027,
        "changePct": 0.004,
        "candleUpperWickRatio": 0.10,
        "distMa20Signed": 0.02,
        "diff20_pct": 0.04,
    }
    stale_item = {
        **low_priority_item,
        "code": "4689",
        "asOf": "2026-05-29",
        "tradePriorityScore": 0.72,
    }

    assert "low_trade_priority_score" in rankings_cache._trade_up_entry_block_reasons(low_priority_item)  # type: ignore[attr-defined]
    assert "stale_profit_expectancy" in rankings_cache._trade_up_entry_block_reasons(stale_item)  # type: ignore[attr-defined]


def test_stale_ml_buy_candidate_is_demoted_to_long_watch() -> None:
    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            {
                "code": "4689",
                "asOf": "2026-05-29",
                "setupType": "breakout",
                "monthlyBoxState": "box_mid",
                "entryQualified": True,
                "probSideCalib": 0.82,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.10,
                "swingScore": 0.70,
                "modelVersion": "20260314153632",
                "mlPredDt": 1776643200,
                "mlEv5Net": 0.01,
                "mlEv10Net": 0.01,
                "mlEv20Net": 0.046,
                "mlPUpShort": 0.89,
                "changePct": 0.004,
                "candleUpperWickRatio": 0.10,
                "distMa20Signed": 0.02,
                "diff20_pct": 0.04,
            }
        ]
    )

    assert buckets["actionable_buy_candidates"] == []
    assert buckets["caution_watch_candidates"][0]["code"] == "4689"
    assert "stale_profit_expectancy" in buckets["caution_watch_candidates"][0]["tradeEntryBlockReasons"]
    assert buckets["setup_watch_candidates"][0]["code"] == "4689"


def test_soft_blocked_momentum_watch_promotes_to_momentum_entry_candidate(monkeypatch) -> None:
    candidates = rankings_cache._build_momentum_entry_candidates(  # type: ignore[attr-defined]
        [
            {
                "code": "5233",
                "asOf": "2026-05-01",
                "setupType": "reject",
                "monthlyBoxState": "box_upper",
                "entryQualified": False,
                "probSideCalib": 0.82,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.10,
                "swingScore": 0.70,
                "modelVersion": "20260314153632",
                "mlPredDt": 1776643200,
                "mlEv5Net": 0.03,
                "mlEv10Net": 0.04,
                "mlEv20Net": 0.06,
                "mlPUpShort": 0.88,
                "changePct": 0.018,
                "candleUpperWickRatio": 0.10,
                "distMa20Signed": 0.035,
                "diff20_pct": 0.11,
                "breakout20_up": 0.03,
                "cnt_20_above": 16,
                "cnt_7_above": 6,
                "market_ret20": 0.03,
                "momentumFollowThroughScore": 0.94,
                "tradeEntryBlockReasons": ["low_trade_priority_score", "stale_profit_expectancy"],
            }
        ]
    )

    assert [row["code"] for row in candidates] == ["5233"]
    assert candidates[0]["momentumEntryState"] == "entry_candidate"


def test_momentum_entry_candidate_rejects_hard_blocked_watch(monkeypatch) -> None:
    candidates = rankings_cache._build_momentum_entry_candidates(  # type: ignore[attr-defined]
        [
            {
                "code": "9991",
                "asOf": "2026-05-01",
                "setupType": "reject",
                "monthlyBoxState": "box_upper",
                "entryQualified": False,
                "probSideCalib": 0.82,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.10,
                "swingScore": 0.70,
                "modelVersion": "20260314153632",
                "mlPredDt": 1776643200,
                "mlEv5Net": -0.01,
                "mlEv10Net": 0.04,
                "mlEv20Net": 0.06,
                "mlPUpShort": 0.88,
                "changePct": 0.018,
                "candleUpperWickRatio": 0.10,
                "distMa20Signed": 0.035,
                "diff20_pct": 0.11,
                "breakout20_up": 0.03,
                "cnt_20_above": 16,
                "cnt_7_above": 6,
                "market_ret20": 0.03,
                "momentumFollowThroughScore": 0.94,
                "tradeEntryBlockReasons": ["low_trade_priority_score", "weak_5d_profit_expectancy"],
            }
        ]
    )

    assert candidates == []


def test_long_watch_candidates_are_tiered_without_promoting_to_actionable() -> None:
    def item(code: str, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "code": code,
            "setupType": "reject",
            "monthlyBoxState": "box_upper",
            "entryQualified": False,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": None,
            "mlEv10Net": None,
            "mlEv20Net": None,
            "changePct": 0.012,
            "candleUpperWickRatio": 0.10,
            "distMa20Signed": 0.02,
            "diff20_pct": 0.04,
        }
        base.update(overrides)
        return base

    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            item("2101", setupType="breakout", entryQualified=True),
            item("2102", distMa20Signed=0.14),
            item("2103"),
        ]
    )

    assert buckets["actionable_buy_candidates"] == []
    assert [row["code"] for row in buckets["setup_watch_candidates"]] == ["2101"]
    assert [row["code"] for row in buckets["pullback_watch_candidates"]] == ["2102"]
    assert [row["code"] for row in buckets["momentum_watch_candidates"]] == ["2103"]


def test_high_zone_chart_reads_separate_trend_follow_from_high_grab() -> None:
    def item(code: str, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "code": code,
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "entryQualifiedByFallback": False,
            "entryQualifiedFallbackStage": None,
            "probSideCalib": 0.82,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.12,
            "swingScore": 0.68,
            "modelVersion": "20260314153632",
            "mlPredDt": 1777680000,
            "asOf": "2026-05-01",
            "mlEv5Net": 0.018,
            "mlEv10Net": 0.022,
            "mlEv20Net": 0.040,
            "changePct": 0.032,
            "candleUpperWickRatio": 0.08,
            "distMa20Signed": 0.082,
            "diff20_pct": 0.12,
            "breakout20_up": 0.03,
            "momentumFollowThroughScore": 0.96,
            "swingReasons": ["LONG score=0.700 edge=0.500 risk=0.200", "setup=watch n=124844 mean=0.0100"],
        }
        base.update(overrides)
        return base

    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            item("4101"),
            item("4102", mlEv5Net=-0.002, mlEv10Net=0.020),
            item("4103", diff20_pct=0.24, candleUpperWickRatio=0.40, mlEv5Net=-0.001, mlEv10Net=-0.002),
        ]
    )

    assert "4101" not in {row["code"] for row in buckets["actionable_buy_candidates"]}
    reads_by_code = {row["code"]: row for row in buckets["high_zone_chart_reads"]}
    assert reads_by_code["4101"]["highZoneChartState"] == "trend_follow"
    assert reads_by_code["4102"]["highZoneChartState"] == "wait_for_pullback"
    assert reads_by_code["4103"]["highZoneChartState"] == "high_grab_risk"
    assert reads_by_code["4101"]["highZoneEvidenceSampleCount"] == 124844
    assert reads_by_code["4101"]["highZoneEvidenceMinSampleCount"] == 100
    assert reads_by_code["4101"]["highZoneEvidenceUsable"] is True
    assert "positive_5d_10d_expectancy" in reads_by_code["4101"]["highZoneChartReasons"]
    assert "weak_short_expectancy" in reads_by_code["4102"]["highZoneChartRiskReasons"]
    assert "already_large_20d_run" in reads_by_code["4103"]["highZoneChartRiskReasons"]
    assert {row["code"] for row in buckets["pullback_watch_candidates"]} == {"4101", "4102", "4103"}
    assert buckets["high_zone_research_candidates"] == []


def test_high_zone_chart_reads_do_not_trend_follow_with_thin_evidence_sample() -> None:
    buckets = rankings_cache._build_trade_candidate_buckets(  # type: ignore[attr-defined]
        [
            {
                "code": "4104",
                "setupType": "breakout",
                "monthlyBoxState": "box_upper",
                "entryQualified": True,
                "entryQualifiedByFallback": False,
                "entryQualifiedFallbackStage": None,
                "probSideCalib": 0.82,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.12,
                "swingScore": 0.68,
                "modelVersion": "20260314153632",
                "mlPredDt": 1777680000,
                "asOf": "2026-05-01",
                "mlEv5Net": 0.018,
                "mlEv10Net": 0.022,
                "mlEv20Net": 0.040,
                "changePct": 0.032,
                "candleUpperWickRatio": 0.08,
                "distMa20Signed": 0.082,
                "diff20_pct": 0.12,
                "breakout20_up": 0.03,
                "momentumFollowThroughScore": 0.96,
                "swingReasons": ["LONG score=0.700 edge=0.500 risk=0.200", "setup=watch n=12 mean=0.0100"],
            }
        ]
    )

    read = buckets["high_zone_chart_reads"][0]

    assert read["highZoneChartState"] == "research_needed"
    assert read["highZoneEvidenceSampleCount"] == 12
    assert read["highZoneEvidenceMinSampleCount"] == 100
    assert read["highZoneEvidenceUsable"] is False
    assert read["highZoneEvidenceResearchRequired"] is True
    assert "research_sample_needed" in read["highZoneChartRiskReasons"]
    research = buckets["high_zone_research_candidates"][0]
    assert research["code"] == "4104"
    assert research["researchOnly"] is True
    assert research["researchCandidateKind"] == "high_zone_chase_sample_gap"
    assert research["researchCandidateReason"] == "high_zone_evidence_sample_below_minimum"
    assert research["researchCandidateSource"] == "meemee_high_zone_chart_read"
    assert research["researchCandidateBoundary"] == "TRADEX_REVIEW_ONLY"


def test_long_watch_fallback_does_not_promote_actionable(monkeypatch) -> None:
    source_items = [
        {
            "code": "3101",
            "setupType": "reject",
            "monthlyBoxState": "box_upper",
            "entryQualified": False,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": None,
            "mlEv10Net": None,
            "mlEv20Net": None,
            "changePct": 0.012,
            "candleUpperWickRatio": 0.10,
            "distMa20Signed": 0.02,
            "diff20_pct": 0.04,
        }
    ]
    empty_buckets = {
        "actionable_buy_candidates": [],
        "actionable_short_candidates": [],
        "caution_watch_candidates": [],
        "momentum_watch_candidates": [],
        "high_zone_chart_reads": [],
        "high_zone_research_candidates": [],
        "pullback_watch_candidates": [],
        "setup_watch_candidates": [],
    }
    monkeypatch.setattr(rankings_cache, "_decorate_rule_items_with_entry_gate", lambda items, direction, risk_mode: [dict(item) for item in items])

    filled = rankings_cache._fill_long_watch_candidates_from_source(  # type: ignore[attr-defined]
        empty_buckets,
        source_items,
        direction="up",
        risk_mode="balanced",
    )

    assert filled["actionable_buy_candidates"] == []
    assert [row["code"] for row in filled["momentum_watch_candidates"]] == ["3101"]


def test_long_entry_decision_contract_explains_zero_candidates() -> None:
    contract = rankings_cache._build_long_entry_decision_contract(  # type: ignore[attr-defined]
        {
            "actionable_buy_candidates": [],
            "caution_watch_candidates": [
                {"code": "1001", "tradeEntryBlockReasons": ["missing_long_profit_expectancy"]},
                {"code": "1002", "tradeEntryBlockReasons": ["missing_long_profit_expectancy", "extended_above_ma20"]},
            ],
        }
    )

    assert contract["status"] == "no_eligible_long"
    assert contract["actionable_long_count"] == 0
    assert contract["excluded_long_count"] == 2
    assert contract["exclusion_reason_counts"] == {
        "missing_long_profit_expectancy": 2,
        "extended_above_ma20": 1,
    }
    assert "ありません" in contract["message"]


def test_failed_high_retest_merge_preserves_short_entry_actionability(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_failed_high_retest_short_candidates",
        lambda source_items: [
            {
                "code": "3201",
                "setupType": "failed_high_retest",
                "tradeEntryClass": "failed_high_retest_short",
                "monthlyBoxState": "box_mid",
                "entryQualified": True,
                "probSideCalib": 0.80,
                "entryScore": 0.72,
                "hybridScore": 0.72,
                "downsideRisk": 0.18,
                "swingScore": 0.60,
                "mlEv5Net": -0.12,
                "mlEv10Net": -0.12,
                "mlEv20Net": -0.12,
                "changePct": -0.004,
                "candleUpperWickRatio": 0.66,
                "candleLowerWickRatio": 0.08,
                "failedHighRetestShort": True,
                "failedHighRetestShortScore": 0.70,
                "failedHighRetestRetestRatio": 0.965,
                "failedHighRetestAnchorDropPct": 0.010,
                "tradePriorityScore": 0.70,
            }
        ],
    )

    merged = rankings_cache._merge_failed_high_retest_short_candidates(  # type: ignore[attr-defined]
        {"actionable_buy_candidates": [], "actionable_short_candidates": [], "caution_watch_candidates": []},
        [{"code": "3201"}],
    )
    candidate = merged["actionable_short_candidates"][0]

    assert candidate["code"] == "3201"
    assert candidate["shortEntryActionabilityScore"] > 0.5
    assert candidate["tradePriorityScore"] >= candidate["shortEntryActionabilityScore"]


def test_trade_theme_leadership_adjusts_priority_without_requalifying_weak_entry(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, *, change_pct: float, ev: float = 0.10) -> dict[str, object]:
        return {
            "code": code,
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": ev,
            "mlEv10Net": ev,
            "mlEv20Net": ev,
            "changePct": change_pct,
            "candleUpperWickRatio": 0.12,
            "buy_overextended": False,
        }

    items = [
        item("2432", change_pct=0.018),
        item("7974", change_pct=0.011),
        item("7832", change_pct=0.010),
        item("9684", change_pct=0.012),
        item("8035", change_pct=-0.018),
        item("6857", change_pct=-0.016),
        item("6920", change_pct=-0.012),
        item("4063", change_pct=-0.011),
        item("6146", change_pct=-0.010),
    ]

    buckets = rankings_cache._build_trade_candidate_buckets(items)  # type: ignore[attr-defined]
    actionable_by_code = {item["code"]: item for item in buckets["actionable_buy_candidates"]}
    caution_by_code = {item["code"]: item for item in buckets["caution_watch_candidates"]}

    assert actionable_by_code["2432"]["themeId"] == "game_content"
    assert actionable_by_code["2432"]["themeLeadershipState"] == "accel"
    assert actionable_by_code["2432"]["themeLeadershipDelta"] > 0
    assert caution_by_code["8035"]["themeId"] == "semiconductor_core"
    assert caution_by_code["8035"]["themeLeadershipState"] == "fade"
    assert caution_by_code["8035"]["themeLeadershipDelta"] < 0
    assert "current_weak_close" in caution_by_code["8035"]["tradeEntryBlockReasons"]
    assert "8035" not in actionable_by_code


def test_trade_theme_leadership_ignores_thin_samples(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {str(code): "PRIME" for code in codes},
    )

    def item(code: str, *, change_pct: float) -> dict[str, object]:
        return {
            "code": code,
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.10,
            "mlEv10Net": 0.10,
            "mlEv20Net": 0.10,
            "changePct": change_pct,
            "candleUpperWickRatio": 0.12,
            "buy_overextended": False,
        }

    items = [
        item("8035", change_pct=0.018),
        item("6857", change_pct=0.016),
        item("6920", change_pct=0.012),
    ]

    buckets = rankings_cache._build_trade_candidate_buckets(items)  # type: ignore[attr-defined]
    actionable_by_code = {item["code"]: item for item in buckets["actionable_buy_candidates"]}
    candidate = actionable_by_code["8035"]

    assert candidate["themeId"] == "semiconductor_core"
    assert candidate["themeMemberCount"] == 3
    assert candidate["themeMinMemberCount"] == 4
    assert candidate["themeLeadershipUsable"] is False
    assert candidate["themeLeadershipState"] == "insufficient_sample"
    assert candidate["themeLeadershipReasons"] == ["sample_below_minimum"]
    assert candidate["themeLeadershipDelta"] == 0.0


def test_trade_priority_scores_promote_momentum_follow_through(monkeypatch) -> None:
    items = [
        {
            "code": "0001",
            "setupType": "breakout",
            "entryQualified": True,
            "probSideCalib": 0.70,
            "entryScore": 0.65,
            "hybridScore": 0.65,
            "downsideRisk": 0.20,
            "swingScore": 0.55,
            "diff20_pct": 0.11,
            "breakout20_up": 0.025,
            "cnt_20_above": 18,
            "cnt_7_above": 7,
            "distMa20Signed": 0.045,
            "market_ret20": 0.03,
        },
        {
            "code": "0002",
            "setupType": "breakout",
            "entryQualified": True,
            "probSideCalib": 0.70,
            "entryScore": 0.65,
            "hybridScore": 0.65,
            "downsideRisk": 0.20,
            "swingScore": 0.55,
            "diff20_pct": 0.00,
            "breakout20_up": -0.05,
            "cnt_20_above": 6,
            "cnt_7_above": 2,
            "distMa20Signed": -0.01,
            "market_ret20": 0.03,
        },
    ]
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {"0001": "PRIME", "0002": "PRIME"},
    )

    rankings_cache._apply_trade_priority_scores(items, direction="up")  # type: ignore[attr-defined]
    items.sort(key=rankings_cache._trade_priority_sort_key)  # type: ignore[attr-defined]

    assert [item["code"] for item in items] == ["0001", "0002"]
    assert items[0]["momentumFollowThroughV1"] is True
    assert items[0]["momentumFollowThroughScore"] > items[1]["momentumFollowThroughScore"]
    assert items[0]["tradePriorityScore"] > items[1]["tradePriorityScore"]
    assert items[0]["monthlyDrawdownGuardedMomentumV1"] is True
    assert items[0]["monthlyDrawdownGuardedMomentumVariantId"] == "monthly_drawdown_guarded_momentum_m+0.02_l-0.02_h-0.02_md-0.005"


def test_monthly_drawdown_guarded_momentum_adjustment_matches_adopted_variant() -> None:
    momentum_risk_off = {
        "code": "1001",
        "entryQualified": True,
        "marketRiskOff": True,
        "monthlyBoxState": "box_upper",
        "momentumFollowThroughV1": True,
        "momentumFollowThroughScore": 1.0,
        "tradeRiskWatch": [],
        "qualityFlags": [],
    }
    quiet_momentum = {
        "code": "1002",
        "entryQualified": True,
        "marketRiskOff": False,
        "monthlyBoxState": "box_upper",
        "momentumFollowThroughV1": True,
        "momentumFollowThroughScore": 1.0,
        "tradeRiskWatch": [],
        "qualityFlags": [],
    }
    drawdown_risk = {
        "code": "1003",
        "entryQualified": False,
        "marketRiskOff": False,
        "monthlyBoxState": "box_mid",
        "momentumFollowThroughV1": False,
        "momentumFollowThroughScore": 0.2,
        "tradeRiskWatch": ["risk"],
        "qualityFlags": ["entry_not_qualified"],
    }

    assert rankings_cache._apply_monthly_drawdown_guarded_momentum_adjustment(momentum_risk_off) == 0.0  # type: ignore[attr-defined]
    assert rankings_cache._apply_monthly_drawdown_guarded_momentum_adjustment(quiet_momentum) == 0.0  # type: ignore[attr-defined]
    assert rankings_cache._apply_monthly_drawdown_guarded_momentum_adjustment(drawdown_risk) == -0.025  # type: ignore[attr-defined]
    assert drawdown_risk["monthlyDrawdownGuardedMomentumFlags"] == {
        "momentum_candidate": False,
        "low_risk_context": False,
        "high_risk_context": True,
        "monthly_drawdown_context": True,
    }


def test_screening_bundle_shape_remains_stable(monkeypatch) -> None:
    _stub_screening_bundle_dependencies(monkeypatch)

    monkeypatch.setattr(
        bridge.rankings_cache,
        "get_rankings_asof",
        lambda tf, which, direction, limit, *, as_of, mode="trade", risk_mode="balanced": {
            "tf": tf,
            "which": which,
            "dir": direction,
            "mode": mode,
            "risk_mode": risk_mode,
            "legacy_analysis_disabled": False,
            "candidate_source": "ml_plus_features",
            "requested_as_of": str(as_of),
            "snapshot_as_of": "2026-04-20",
            "freshness_state": "fresh",
            "freshness_days": 1,
            "stale": False,
            "current_candidate_available": True,
            "items": [
                {
                    "code": "0001",
                    "tradePriorityScore": 0.91,
                    "tradePriorityProfitScore": 0.40,
                    "tradePriorityHitScore": 0.50,
                    "tradePriorityQualityScore": 0.30,
                    "setupType": "breakout",
                    "entryQualified": True,
                    "entryQualifiedByFallback": False,
                    "entryQualifiedFallbackStage": None,
                }
            ],
        },
    )

    bundle = bridge.build_screening_review_bundle(asof="2026-04-20", top_n=1, side="long", include_near_boundary=False)

    assert set(bundle.keys()) == {"confirmed", "asof", "runtime_guard", "screening_source", "warnings", "candidates", "boundary_review"}
    assert set(bundle["screening_source"].keys()) == {"selection_mode", "ranking_snapshot_status"}
    assert set(bundle["screening_source"]["ranking_snapshot_status"].keys()) == {"long", "short"}
    assert set(bundle["candidates"][0].keys()) == {"code", "rank", "selected_direction", "owner", "meemee_summary", "event_risk", "supply_demand_risk", "tradex_summary", "actionability"}
    assert set(bundle["boundary_review"].keys()) == {"top_boundary_observability", "near_boundary_codes"}
    assert "long" in bundle["boundary_review"]["top_boundary_observability"]
    assert bundle["boundary_review"]["near_boundary_codes"] == []


def test_trade_bucket_fixture_demotes_etf_items(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {
            "0001": "PRIME",
            "0002": "STANDARD",
            "9001": rankings_cache._ETF_MARKET_CODE,
            "9002": rankings_cache._ETF_MARKET_CODE,
        },
    )
    items = [
        {
            "code": "0001",
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.12,
            "mlEv10Net": 0.12,
            "mlEv20Net": 0.12,
        },
        {
            "code": "0002",
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.12,
            "mlEv10Net": 0.12,
            "mlEv20Net": 0.12,
        },
        {
            "code": "9001",
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.12,
            "mlEv10Net": 0.12,
            "mlEv20Net": 0.12,
        },
        {
            "code": "9002",
            "setupType": "breakout",
            "monthlyBoxState": "box_upper",
            "entryQualified": True,
            "probSideCalib": 0.80,
            "entryScore": 0.72,
            "hybridScore": 0.72,
            "downsideRisk": 0.18,
            "swingScore": 0.60,
            "mlEv5Net": 0.12,
            "mlEv10Net": 0.12,
            "mlEv20Net": 0.12,
        },
    ]

    buckets = rankings_cache._build_trade_candidate_buckets(items)  # type: ignore[attr-defined]
    buy_codes = [item["code"] for item in buckets["actionable_buy_candidates"]]

    assert buy_codes[:2] == ["0001", "0002"]
    assert all(code not in buy_codes[:2] for code in ["9001", "9002"])
    assert buckets["actionable_buy_candidates"][0]["tradePriorityScore"] > buckets["actionable_buy_candidates"][2]["tradePriorityScore"]


def test_trade_prebreakout_actionability_axis_prefers_compressed_release_setup(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {"1001": "PRIME", "1002": "PRIME"},
    )
    compressed_item = {
        "code": "1001",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.66,
        "monthlyBreakoutUpProb": 0.70,
        "high20_dist": -0.012,
        "breakout20_up": -0.012,
        "diff20_pct": 0.004,
        "drawdown60": -0.045,
        "rebound60": 0.074,
        "turnover_z20": 0.52,
        "liquidity20d": 1_280_000_000.0,
        "monthlyBoxState": "box_upper",
        "monthlyBoxMonths": 7,
        "candleUpperWickRatio": 0.08,
        "buy_overextended": False,
    }
    late_item = {
        "code": "1002",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.66,
        "monthlyBreakoutUpProb": 0.70,
        "high20_dist": -0.052,
        "breakout20_up": -0.052,
        "diff20_pct": 0.018,
        "drawdown60": -0.118,
        "rebound60": 0.012,
        "turnover_z20": -0.63,
        "liquidity20d": 3_500_000.0,
        "monthlyBoxState": "box_upper",
        "monthlyBoxMonths": 11,
        "candleUpperWickRatio": 0.42,
        "buy_overextended": True,
        "patternS3LateBreakout": True,
    }

    compressed_score = rankings_cache._calc_trade_prebreakout_actionability_score(compressed_item, direction="up")  # type: ignore[attr-defined]
    late_score = rankings_cache._calc_trade_prebreakout_actionability_score(late_item, direction="up")  # type: ignore[attr-defined]
    assert compressed_score > late_score

    items = [dict(compressed_item), dict(late_item)]
    rankings_cache._apply_trade_priority_scores(items, direction="up")  # type: ignore[attr-defined]
    items.sort(key=rankings_cache._trade_priority_sort_key)  # type: ignore[attr-defined]

    assert [item["code"] for item in items] == ["1001", "1002"]
    assert items[0]["tradePriorityScore"] > items[1]["tradePriorityScore"]


def test_trade_prebreakout_actionability_axis_v2_prefers_pure_launch_setup(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {"2001": "PRIME", "2002": "PRIME", "2003": "PRIME"},
    )
    pure_item = {
        "code": "2001",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.59,
        "monthlyBreakoutUpProb": 0.66,
        "high20_dist": -0.028,
        "breakout20_up": -0.028,
        "diff20_pct": 0.009,
        "drawdown60": -0.055,
        "rebound60": 0.112,
        "turnover_z20": 0.72,
        "liquidity20d": 1_100_000_000.0,
        "monthlyBoxState": "box_upper",
        "monthlyBoxMonths": 8,
        "candleUpperWickRatio": 0.10,
        "buy_overextended": False,
    }
    late_item = {
        "code": "2002",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.74,
        "monthlyBreakoutUpProb": 0.80,
        "high20_dist": -0.004,
        "breakout20_up": -0.004,
        "diff20_pct": 0.020,
        "drawdown60": -0.020,
        "rebound60": 0.030,
        "turnover_z20": -0.12,
        "liquidity20d": 28_000_000.0,
        "candleUpperWickRatio": 0.48,
        "buy_overextended": True,
        "patternS3LateBreakout": True,
    }
    weak_item = {
        "code": "2003",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.39,
        "monthlyBreakoutUpProb": 0.42,
        "high20_dist": -0.043,
        "breakout20_up": -0.043,
        "diff20_pct": 0.011,
        "drawdown60": -0.142,
        "rebound60": 0.024,
        "turnover_z20": -0.58,
        "liquidity20d": 12_000_000.0,
        "candleUpperWickRatio": 0.18,
        "buy_overextended": False,
    }

    pure_score = rankings_cache._calc_trade_prebreakout_actionability_score(pure_item, direction="up")  # type: ignore[attr-defined]
    late_score = rankings_cache._calc_trade_prebreakout_actionability_score(late_item, direction="up")  # type: ignore[attr-defined]
    weak_score = rankings_cache._calc_trade_prebreakout_actionability_score(weak_item, direction="up")  # type: ignore[attr-defined]

    assert pure_score > late_score
    assert pure_score > weak_score

    items = [dict(pure_item), dict(late_item), dict(weak_item)]
    rankings_cache._apply_trade_priority_scores(items, direction="up")  # type: ignore[attr-defined]
    items.sort(key=rankings_cache._trade_priority_sort_key)  # type: ignore[attr-defined]

    assert items[0]["code"] == "2001"
    assert items[0]["tradePriorityScore"] > items[1]["tradePriorityScore"]
    assert items[0]["tradePriorityScore"] > items[2]["tradePriorityScore"]


def test_trade_prebreakout_actionability_axis_v3_only_penalizes_failed_breakdown_residue(monkeypatch) -> None:
    monkeypatch.setattr(
        rankings_cache,
        "_load_trade_market_code_map",
        lambda codes: {"3001": "PRIME", "3002": "PRIME"},
    )
    pure_launch = {
        "code": "3001",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.61,
        "monthlyBreakoutUpProb": 0.68,
        "high20_dist": -0.026,
        "breakout20_up": -0.026,
        "diff20_pct": 0.008,
        "drawdown60": -0.058,
        "rebound60": 0.109,
        "turnover_z20": 0.66,
        "liquidity20d": 1_020_000_000.0,
        "monthlyBoxState": "box_upper",
        "monthlyBoxMonths": 8,
        "candleUpperWickRatio": 0.09,
        "buy_overextended": False,
    }
    failed_breakdown_residue = {
        "code": "3002",
        "setupType": "breakout",
        "entryQualified": True,
        "probSideCalib": 0.81,
        "probSide": 0.81,
        "prob20d": 0.81,
        "prob10d": 0.81,
        "prob5d": 0.81,
        "entryScore": 0.72,
        "hybridScore": 0.72,
        "downsideRisk": 0.18,
        "swingScore": 0.63,
        "weeklyBreakoutUpProb": 0.44,
        "monthlyBreakoutUpProb": 0.49,
        "high20_dist": -0.041,
        "breakout20_up": -0.041,
        "diff20_pct": 0.011,
        "drawdown60": -0.132,
        "rebound60": 0.026,
        "turnover_z20": -0.55,
        "liquidity20d": 18_000_000.0,
        "candleUpperWickRatio": 0.20,
        "buy_overextended": True,
        "patternS3LateBreakout": False,
    }

    pure_v2 = rankings_cache._calc_trade_prebreakout_actionability_score(pure_launch, direction="up")  # type: ignore[attr-defined]
    pure_v3 = rankings_cache._calc_trade_prebreakout_actionability_score_v3(pure_launch, direction="up")  # type: ignore[attr-defined]
    residue_v2 = rankings_cache._calc_trade_prebreakout_actionability_score(failed_breakdown_residue, direction="up")  # type: ignore[attr-defined]
    residue_v3 = rankings_cache._calc_trade_prebreakout_actionability_score_v3(failed_breakdown_residue, direction="up")  # type: ignore[attr-defined]
    pure_components = rankings_cache._calc_trade_prebreakout_actionability_components(pure_launch, direction="up")  # type: ignore[attr-defined]
    residue_components = rankings_cache._calc_trade_prebreakout_actionability_components(failed_breakdown_residue, direction="up")  # type: ignore[attr-defined]

    assert pure_components["failed_breakdown_residue_penalty"] < residue_components["failed_breakdown_residue_penalty"]
    assert pure_v3 >= pure_v2 - 0.05
    assert residue_v3 < residue_v2
    assert pure_v3 > residue_v3
