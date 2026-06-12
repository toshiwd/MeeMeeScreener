from scripts import tradex_short_early_impulse6_regime_gate_v1 as mod


def test_regime_gate_holds_when_coverage_is_insufficient(monkeypatch, tmp_path) -> None:
    events = [
        {
            "code": str(idx),
            "signal_ymd": 20240101 + idx,
            "month": 202401 + idx,
            "short_ret": 0.05,
            "setup_state": "SetupReady",
            "to_visual_continuation_permit": True,
            "early_bucket": "EarlyImpulse6NoDenial",
            "stop_hit": False,
            "target_hit": True,
            "range_40_20": 0.6,
            "last_vol_ratio": 0.5,
            "dist_prior_80_high": -0.3,
        }
        for idx in range(18)
    ]
    monkeypatch.setattr(mod, "load_regime_rows", lambda dates, path: ({}, {"status": "unavailable"}))

    payload = mod.build_payload({"events": events}, tmp_path / "source.json", tmp_path / "regime.duckdb")

    assert payload["coverage"]["coverage_ratio"] == 0.0
    assert payload["research_decision"]["authoritative_rollup_decision"] == "HOLD_REVIEW_ONLY"
    assert payload["research_decision"]["production_promotion_allowed"] is False
