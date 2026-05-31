from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_historical_operational_replay_v1 as mod


def _bars() -> pd.DataFrame:
    rows = []
    for idx in range(80):
        close = 100.0 + idx
        rows.append({"code": "1001", "bar_date": 20250101 + idx, "high": close + 2, "low": close - 2, "close": close})
    return pd.DataFrame(rows)


def test_build_bar_features_computes_operational_levels() -> None:
    features = mod.build_bar_features(_bars())
    last = features.iloc[-1]
    assert pd.notna(last["ma20"])
    assert pd.notna(last["atr14"])
    assert pd.notna(last["recent_swing_low"])


def test_attach_invalidation_replay_marks_future_hit() -> None:
    bars = _bars()
    selected = pd.DataFrame(
        [{"as_of_date": int(bars.iloc[40]["bar_date"]), "code": "1001", "ret5": 0.05, "ret20": 0.1, "period_bucket": "2025H1"}]
    )
    features = mod.build_bar_features(bars)
    replay = mod.attach_invalidation_replay(selected, features)
    assert "primary_invalidation_level" in replay.columns
    assert replay["invalidation_hit_20d"].notna().all()


def test_metric_payload_reports_return_risk_and_invalidation() -> None:
    rows = pd.DataFrame(
        [
            {"as_of_date": 1, "code": "a", "ret5": 0.02, "ret20": 0.12, "invalidation_hit_20d": False},
            {"as_of_date": 2, "code": "b", "ret5": 0.01, "ret20": -0.06, "invalidation_hit_20d": True},
        ]
    )
    metrics = mod.metric_payload(rows)
    assert metrics["sample_count"] == 2
    assert metrics["winner_rate_ret20_gt_10pct"] == 0.5
    assert metrics["bad_rate_ret20_lt_minus_5pct"] == 0.5
    assert metrics["invalidation_hit_20d_rate"] == 0.5


def test_decide_keep_when_overall_and_current_pass() -> None:
    metrics = {
        "mean_ret20": 0.05,
        "winner_rate_ret20_gt_10pct": 0.25,
        "bad_rate_ret20_lt_minus_5pct": 0.1,
        "severe_rate_ret20_lt_minus_10pct": 0.05,
        "invalidation_hit_20d_rate": 0.1,
    }
    decision, decision_class, reasons = mod.decide(metrics, metrics, {"no_lookahead_pass": True})
    assert decision == "historical_operational_replay_supports_forward_validation"
    assert decision_class == "KEEP"
    assert "same_selector_risk_contract_passed_historical_operational_replay" in reasons
