from __future__ import annotations

import pandas as pd

from scripts.tradex_dist_ma60_overextension_selected_loser_demotion_pretest_v1 import compare, decide


def test_same_date_highest_quartile_demotion_changes_rank() -> None:
    rows = pd.DataFrame([_row(i, 100 - i, dist=i / 10, ret=-0.1 if i == 1 else 0.1) for i in range(1, 9)])
    rows["dist_ma60_q_by_date"] = rows.groupby("decision_ymd")["dist_ma60_pct"].rank(pct=True)
    rows["dist_ma60_high_risk_q4"] = rows["dist_ma60_q_by_date"] >= 0.75
    rows["dist_ma60_score_delta"] = rows["dist_ma60_high_risk_q4"].map(lambda x: -1 if x else 0)
    rows["challenger_score"] = rows["baseline_score"] + rows["dist_ma60_score_delta"]
    rows["challenger_rank"] = rows.sort_values(["decision_ymd", "challenger_score", "baseline_score", "candidate_rank"], ascending=[True, False, False, True]).groupby("decision_ymd").cumcount() + 1
    rows["winner20"] = rows["ret20_num"] > 0
    rows["loser20"] = rows["ret20_num"] < 0

    summary, loser, repl, branch, _profile = compare(rows)

    assert "2024" in summary
    assert not loser.empty
    assert not repl.empty
    assert not branch.empty


def test_decide_inconclusive_when_coverage_low() -> None:
    decision = decide({}, 0.1)

    assert decision["research_decision"] == "inconclusive"


def _row(rank: int, score: float, dist: float, ret: float) -> dict[str, object]:
    return {
        "decision_ymd": 20240105,
        "code": str(rank),
        "candidate_rank": rank,
        "year": 2024,
        "baseline_rank_recalc": rank,
        "baseline_score": score,
        "selection_score": score,
        "dist_ma60_pct": dist,
        "dist_ma20_pct": dist / 2,
        "ma20_slope": 0.01,
        "ma60_slope": 0.01,
        "realized_vol20": 0.02,
        "atr14_pct": 0.02,
        "upper_wick_ratio": 0.1,
        "ret20_num": ret,
        "ret20": ret,
    }
