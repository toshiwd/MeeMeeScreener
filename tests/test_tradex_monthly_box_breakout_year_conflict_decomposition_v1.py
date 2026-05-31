from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_monthly_box_breakout_year_conflict_decomposition_v1 import (
    REQUIRED_ARTIFACTS,
    build_membership,
    concentration,
    decide,
    run,
)


def test_build_membership_classifies_added_removed_unchanged() -> None:
    rows = pd.DataFrame(
        [
            {"decision_ymd": 20240101, "code": "A", "year": 2024, "baseline_rank_recalc": 1, "challenger_rank": 3, "ret20": -0.05, "monthly_box_breakout_bool": True},
            {"decision_ymd": 20240101, "code": "B", "year": 2024, "baseline_rank_recalc": 2, "challenger_rank": 1, "ret20": 0.05, "monthly_box_breakout_bool": False},
            {"decision_ymd": 20240101, "code": "C", "year": 2024, "baseline_rank_recalc": 3, "challenger_rank": 2, "ret20": 0.01, "monthly_box_breakout_bool": False},
        ]
    )

    members = build_membership(rows, years=(2024,), topks=(2,))
    roles = dict(zip(members["code"], members["replacement_role"]))

    assert roles["A"] == "removed"
    assert roles["B"] == "unchanged"
    assert roles["C"] == "added"


def test_concentration_detects_largest_month_share() -> None:
    members = pd.DataFrame(
        [
            {"eval_year": 2024, "topk": 5, "replacement_role": "added", "decision_ymd": 20240101, "code": "A"},
            {"eval_year": 2024, "topk": 5, "replacement_role": "removed", "decision_ymd": 20240102, "code": "B"},
        ]
    )

    summary = concentration(members)

    assert summary["2024"]["5"]["largest_month"] == "202401"
    assert summary["2024"]["5"]["largest_month_share"] == 1.0


def test_decide_holds_when_context_visible_and_not_concentrated() -> None:
    summary = {"primary_conflict_type": "conflict"}
    candidates = {"best_context_candidate": {"candidate_axis_name": "x"}}
    conc = {"2024": {"10": {"largest_code_share": 0.01, "largest_month_share": 0.2}}}

    decision = decide(summary, candidates, conc)

    assert decision["research_decision"] == "hold_for_2026_validation"


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    pretest = tmp_path / "pretest"
    context = tmp_path / "context"
    output = tmp_path / "output"
    pretest.mkdir()
    context.mkdir()
    rows = []
    ctx = []
    for year in [2024, 2025]:
        for idx in range(1, 8):
            rows.append(
                {
                    "decision_ymd": int(f"{year}0101"),
                    "code": str(1000 + idx),
                    "year": year,
                    "baseline_rank_recalc": idx,
                    "challenger_rank": 8 - idx,
                    "ret20": 0.01 * idx,
                    "ret40": 0.02 * idx,
                    "monthly_box_breakout_bool": idx <= 3,
                    "monthly_box_breakout_proxy": idx <= 3,
                    "monthly_box_inside_proxy": idx > 5,
                    "monthly_high_zone_proxy": idx <= 5,
                }
            )
            ctx.append(
                {
                    "decision_ymd": int(f"{year}0101"),
                    "code": str(1000 + idx),
                    "monthly_high_zone_proxy": idx <= 5,
                    "monthly_box_inside_proxy": idx > 5,
                    "above20_streak": idx,
                    "above60_streak": idx,
                    "days_since_ma20_reclaim": idx,
                    "days_since_ma60_reclaim": idx,
                    "ma7_gt_ma20_gt_ma60": idx % 2 == 0,
                    "dist_ma20_pct": 0.01 * idx,
                    "dist_ma60_pct": 0.02 * idx,
                    "ma20_slope": 0.001 * idx,
                    "ma60_slope": 0.001 * idx,
                    "realized_vol20": 0.01 * idx,
                    "atr14_pct": 0.01 * idx,
                    "upper_wick_ratio": 0.1,
                    "large_bearish_candle": False,
                    "failed_high_update": False,
                    "volume_ratio_ma20": 1.0,
                }
            )
    pd.DataFrame(rows).to_csv(pretest / "candidate_rows_scored.csv", index=False)
    for name in ["replacement_quality.csv", "period_stability_summary.csv", "branching_summary.csv"]:
        (pretest / name).write_text("x\n", encoding="utf-8")
    for name in ["topk_comparison_summary.json", "research_decision.json", "no_lookahead_audit.json"]:
        (pretest / name).write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")
    pd.DataFrame(ctx).to_csv(context / "candidate_rows_with_features.csv", index=False)

    result = run(pretest_root=pretest, context_root=context, output_root=output)

    out = Path(result["output_dir"])
    assert all((out / artifact).exists() for artifact in REQUIRED_ARTIFACTS)
    assert json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))["artifact_complete"] is True
