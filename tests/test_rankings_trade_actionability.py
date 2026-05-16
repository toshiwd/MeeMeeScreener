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
