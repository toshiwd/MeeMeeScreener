from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_monthly_box_breakout_recent_demotion_pretest_v1 import (
    REQUIRED_ARTIFACTS,
    _bool_value,
    load_and_score,
    run,
    score_delta,
)


def test_monthly_breakout_fixed_demotion_policy() -> None:
    assert _bool_value(True) is True
    assert _bool_value("True") is True
    assert _bool_value(False) is False
    assert score_delta(True) == -1
    assert score_delta(False) == 0


def test_load_and_score_demotes_breakout_within_same_date(tmp_path: Path) -> None:
    rows = pd.DataFrame(
        [
            {"decision_ymd": 20240101, "code": "A", "candidate_rank": 1, "selection_score": 10, "selected_for_buy": True, "source_year": 2024, "year": 2024, "period_bucket": "recent_2024_2026", "monthly_box_breakout_proxy": True, "monthly_high_zone_proxy": True, "monthly_box_inside_proxy": False, "ret20": 0.01, "ret40": 0.01, "mae20": -0.01, "mfe20": 0.02, "max_drawdown_20": -0.01},
            {"decision_ymd": 20240101, "code": "B", "candidate_rank": 2, "selection_score": 9.5, "selected_for_buy": True, "source_year": 2024, "year": 2024, "period_bucket": "recent_2024_2026", "monthly_box_breakout_proxy": False, "monthly_high_zone_proxy": False, "monthly_box_inside_proxy": True, "ret20": 0.04, "ret40": 0.05, "mae20": -0.01, "mfe20": 0.06, "max_drawdown_20": -0.01},
        ]
    )
    rows.to_csv(tmp_path / "candidate_rows_with_features.csv", index=False)

    scored = load_and_score(tmp_path)

    a = scored[scored["code"] == "A"].iloc[0]
    b = scored[scored["code"] == "B"].iloc[0]
    assert a["challenger_score"] == 9
    assert b["challenger_rank"] == 1


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    context_root = tmp_path / "context"
    input_root.mkdir()
    context_root.mkdir()
    rows = []
    for year in [2024, 2025, 2023]:
        for day in [1, 2]:
            ymd = int(f"{year}010{day}")
            for idx in range(1, 8):
                rows.append(
                    {
                        "decision_ymd": ymd,
                        "code": str(1000 + idx),
                        "candidate_rank": idx,
                        "selection_score": 20 - idx,
                        "selected_for_buy": idx <= 3,
                        "source_year": year,
                        "year": year,
                        "period_bucket": "recent_2024_2026" if year >= 2024 else "pre_recent_2019_2023",
                        "monthly_box_breakout_proxy": idx <= 3,
                        "monthly_high_zone_proxy": idx <= 5,
                        "monthly_box_inside_proxy": idx > 5,
                        "ret20": 0.01 * idx,
                        "ret40": 0.01 * idx,
                        "mae20": -0.01,
                        "mfe20": 0.02,
                        "max_drawdown_20": -0.01,
                    }
                )
    pd.DataFrame(rows).to_csv(input_root / "candidate_rows_with_features.csv", index=False)
    (input_root / "pattern_shift_matrix.csv").write_text("feature\nmonthly_box_breakout_proxy\n", encoding="utf-8")
    (input_root / "feature_lift_by_period.csv").write_text("feature,period_bucket\nmonthly_box_breakout_proxy,recent_2024_2026\n", encoding="utf-8")
    (input_root / "no_lookahead_audit.json").write_text(json.dumps({"audit_result": "pass"}), encoding="utf-8")
    (context_root / "research_decision.json").write_text(json.dumps({"research_decision": "drop"}), encoding="utf-8")

    result = run(input_root=input_root, output_root=output_root, context_drop_root=context_root)

    out = Path(result["output_dir"])
    assert all((out / artifact).exists() for artifact in REQUIRED_ARTIFACTS)
    assert json.loads((out / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))["artifact_complete"] is True
