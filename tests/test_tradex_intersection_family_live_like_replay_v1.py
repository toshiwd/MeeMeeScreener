from __future__ import annotations

import pandas as pd

from scripts import tradex_intersection_family_live_like_replay_v1 as mod


def _rows(ret20_values: list[float], period: int = 20260520) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": period + idx,
                "code": f"{1000 + idx}",
                "fresh_runtime_research_watch_rank": idx + 1,
                "buy_entry_qualified": True,
                "variant_b_entry_qualified_top50": True,
                "ret20": value,
            }
            for idx, value in enumerate(ret20_values)
        ]
    )


def test_period_bucket_uses_half_year() -> None:
    assert mod.period_bucket(20260520) == "2026H1"
    assert mod.period_bucket(20260701) == "2026H2"


def test_metric_payload_computes_return_and_risk_rates() -> None:
    metrics = mod.metric_payload(_rows([0.12, 0.04, -0.06, -0.11]))
    assert metrics["sample_count"] == 4
    assert metrics["winner_rate_ret20_gt_10pct"] == 0.25
    assert metrics["bad_rate_ret20_lt_minus_5pct"] == 0.5
    assert metrics["severe_rate_ret20_lt_minus_10pct"] == 0.25


def test_recent_period_gate_passes_only_quality_recent_period() -> None:
    good = _rows([0.12] * 20 + [0.04] * 5, period=20260520)
    metrics = mod.period_metrics(good.assign(period_bucket=good["as_of_date"].map(mod.period_bucket)))
    gate = mod.recent_period_gate(metrics)
    assert gate["gate_pass"] is True

    weak = _rows([0.12] * 5 + [-0.06] * 20, period=20260520)
    weak_metrics = mod.period_metrics(weak.assign(period_bucket=weak["as_of_date"].map(mod.period_bucket)))
    weak_gate = mod.recent_period_gate(weak_metrics)
    assert weak_gate["gate_pass"] is False
    assert "recent_period_return_or_risk_gate_failed" in weak_gate["reason_typed"]


def test_no_lookahead_requires_support_gate_ready() -> None:
    rows = _rows([0.12, 0.04])
    ready = {"research_decision": "intersection_family_ready_for_forward_paper_validation"}
    assert mod.no_lookahead_audit(rows, ready)["no_lookahead_pass"] is True
    blocked = {"research_decision": "drop"}
    assert mod.no_lookahead_audit(rows, blocked)["no_lookahead_pass"] is False
