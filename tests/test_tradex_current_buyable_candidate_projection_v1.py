from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_candidate_projection_v1 as mod


def _forward_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20260520, "code": "1001", "forward_paper_rank": 1},
            {"as_of_date": 20260520, "code": "1002", "forward_paper_rank": 2},
        ]
    )


def _bars() -> pd.DataFrame:
    rows = []
    for code in ["1001", "1002"]:
        for idx in range(25):
            date = 20260426 + idx
            rows.append({"code": code, "as_of_date": date, "open": 100.0, "high": 101.0, "low": 99.0, "close": 101.0, "volume": 1000})
    rows[-1]["open"] = 102.0
    rows[-1]["close"] = 100.0
    return pd.DataFrame(rows)


def test_build_candle_features_marks_clean_and_bearish_candidates() -> None:
    features = mod.build_candle_features(_bars(), 20260450)
    clean = features[features["code"] == "1001"].iloc[0]
    bearish = features[features["code"] == "1002"].iloc[0]
    assert bool(clean["variant_a_candle_risk_clean"]) is True
    assert bool(bearish["variant_a_candle_risk_clean"]) is False


def test_project_candidates_assigns_research_buyable_rank() -> None:
    features = mod.build_candle_features(_bars(), 20260450)
    projected = mod.project_candidates(_forward_rows().assign(as_of_date=20260450), features)
    selected = projected[projected["research_buyable_candidate"]]
    assert selected["code"].tolist() == ["1001"]
    assert selected["research_buyable_rank"].tolist() == [1.0]
    assert projected["buy_recommendation"].eq(False).all()
    assert projected["validated_buy"].eq(False).all()


def test_no_lookahead_blocks_if_risk_contract_not_keep() -> None:
    projected = mod.project_candidates(_forward_rows().assign(as_of_date=20260450), mod.build_candle_features(_bars(), 20260450))
    forward_decision = {"research_decision": "intersection_family_forward_paper_candidates_frozen"}
    risk_decision = {"research_decision": "drop"}
    audit = mod.no_lookahead_audit(projected, forward_decision, risk_decision)
    assert audit["no_lookahead_pass"] is False


def test_decide_keeps_when_any_candidate_selected() -> None:
    projected = mod.project_candidates(_forward_rows().assign(as_of_date=20260450), mod.build_candle_features(_bars(), 20260450))
    audit = {
        "no_lookahead_pass": True,
    }
    decision, decision_class, reasons = mod.decide(projected, audit)
    assert decision == "current_research_buyable_candidates_selected"
    assert decision_class == "KEEP"
    assert "current_candidates_passed_frozen_intersection_and_candle_risk_projection" in reasons
