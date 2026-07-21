from __future__ import annotations

import pandas as pd

from scripts.tradex_adaptive_short_rule_router_v1 import climax_quality_gate, point_in_time_route, state


def _history(codes: list[str], returns: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=len(returns), freq="D")
    return pd.DataFrame({"code": codes, "signal_date": dates, "ret": returns})


def test_high_pf_rule_is_not_active_when_concentrated_in_too_few_codes() -> None:
    result = state(_history([str(1300 + index % 4) for index in range(20)], [.03] * 19 + [-.05]))
    assert result["pf20"] > 1.3
    assert result["concentration_gate_pass"] is False
    assert result["state"] != "Active"


def test_diversified_positive_rule_can_become_active() -> None:
    result = state(_history([str(1300 + index % 10) for index in range(20)], [.03] * 15 + [-.02] * 5))
    assert result["concentration_gate_pass"] is True
    assert result["state"] == "Active"


def test_router_does_not_use_outcome_before_known_date() -> None:
    signal_dates = pd.date_range("2026-01-01", periods=16, freq="D")
    events = pd.DataFrame({
        "code": [str(1400 + index) for index in range(16)],
        "signal_date": signal_dates,
        "entry_date": signal_dates,
        "ret": [.03] * 16,
        "rule": ["rule_a"] * 16,
        "outcome_known_date": signal_dates + pd.Timedelta(days=20),
    })
    _, routed = point_in_time_route(events)
    assert routed.empty


def test_authoritative_decision_describes_three_family_router() -> None:
    from pathlib import Path

    source = Path("scripts/tradex_adaptive_short_rule_router_v1.py").read_text(encoding="utf-8")
    assert '"candidate_local_decision": "keep_three_family_adaptive_router"' in source
    assert '"authoritative_rollup_decision": "review_only"' in source
    assert "two_family_point_in_time_foundation" not in source


def test_climax_quality_gate_excludes_low_close_capitulation() -> None:
    frame = pd.DataFrame({"close_pos": [0.0, 0.0999, 0.10, 0.25]})
    assert climax_quality_gate(frame).tolist() == [False, False, True, True]
