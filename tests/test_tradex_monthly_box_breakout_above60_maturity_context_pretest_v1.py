from __future__ import annotations

import pandas as pd

from scripts.tradex_monthly_box_breakout_above60_maturity_context_pretest_v1 import (
    comparison_by_period,
    context_gated_score_delta,
    decide,
    score_rows,
)


def test_context_gated_policy_protects_mature_and_missing_above60() -> None:
    assert context_gated_score_delta(False, 3) == (0, False, "not_monthly_box_breakout")
    assert context_gated_score_delta(True, 59) == (-1, False, "monthly_box_breakout_demoted_above60_lt_60")
    assert context_gated_score_delta(True, 60) == (0, False, "monthly_box_breakout_protected_above60_ge_60")
    assert context_gated_score_delta(True, pd.NA) == (0, True, "monthly_box_breakout_missing_above60_streak")


def test_score_rows_uses_gated_and_ungated_rank_separately() -> None:
    rows = pd.DataFrame(
        [
            _row("A", 10.0, True, 70, 0.01),
            _row("B", 9.5, True, 10, 0.03),
            _row("C", 9.0, False, 0, 0.02),
        ]
    )

    scored = score_rows(rows)

    a = scored[scored["code"] == "A"].iloc[0]
    b = scored[scored["code"] == "B"].iloc[0]
    assert a["gated_score_delta"] == 0
    assert a["ungated_score_delta"] == -1
    assert b["gated_score_delta"] == -1
    assert int(scored[scored["code"] == "C"]["gated_rank"].iloc[0]) == 2


def test_comparison_reports_gated_vs_ungated_fields() -> None:
    rows = pd.DataFrame(
        [
            _row("A", 10.0, True, 70, 0.00),
            _row("B", 9.8, True, 10, -0.05),
            _row("C", 9.2, False, 0, 0.08),
            _row("D", 8.9, False, 0, 0.01),
            _row("E", 8.8, False, 0, 0.02),
            _row("F", 8.7, False, 0, 0.03),
        ]
    )
    scored = score_rows(rows)

    summary, replacements, branching, side_by_side = comparison_by_period(scored)

    assert "2024" in summary
    assert "gated_minus_ungated_mean_ret20" in summary["2024"]["top5"]
    assert not replacements.empty
    assert not branching.empty
    assert "replacement_quality_difference" in set(side_by_side.columns)


def test_decide_inconclusive_when_source_audit_fails() -> None:
    rows = pd.DataFrame([_row("A", 10.0, True, 70, 0.01)])
    scored = score_rows(rows)
    summary, _, _, side_by_side = comparison_by_period(scored)

    decision = decide(summary, side_by_side, scored, {"audit_result": "fail"})

    assert decision["research_decision"] == "inconclusive"


def _row(code: str, score: float, monthly: bool, above60: int | None, ret20: float, year: int = 2024) -> dict[str, object]:
    return {
        "decision_ymd": int(f"{year}0105"),
        "code": code,
        "candidate_rank": int(100 - score),
        "selection_score": score,
        "selected_for_buy": True,
        "source_year": year,
        "year": year,
        "monthly_box_breakout_proxy": monthly,
        "monthly_high_zone_proxy": monthly,
        "monthly_box_inside_proxy": not monthly,
        "above60_streak": above60,
        "days_since_ma60_reclaim": above60,
        "ret20": ret20,
        "ret40": ret20,
        "mae20": min(ret20, 0),
        "mfe20": max(ret20, 0),
    }
