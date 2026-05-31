from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_ma60_above_60plus_short_veto_failure_decomposition_v1 import (
    REQUIRED_ARTIFACTS,
    _bool_series,
    _bucket_year,
    recent_degradation,
    run,
    salvageability,
)


def test_bucket_year_groups_recent_and_prior_periods() -> None:
    assert _bucket_year(2020) == "2019-2021"
    assert _bucket_year(2023) == "2022-2023"
    assert _bucket_year(2025) == "2024-2026"
    assert _bucket_year(2018) == "2018"


def test_bool_series_handles_string_false_safely() -> None:
    values = pd.Series(["True", "False", "1", "0", True, False, None])
    assert _bool_series(values).tolist() == [True, False, True, False, True, False, False]


def test_recent_degradation_compares_2024_2026_to_prior_guard_hits() -> None:
    rows = pd.DataFrame(
        [
            {"guard_hit": True, "year": 2020, "ret20_long": 0.10, "helped_veto": True, "source_type": "actual", "guard_anchor_type": "anchor_10"},
            {"guard_hit": True, "year": 2023, "ret20_long": 0.06, "helped_veto": True, "source_type": "actual", "guard_anchor_type": "anchor_20"},
            {"guard_hit": True, "year": 2025, "ret20_long": 0.01, "helped_veto": False, "source_type": "candidate", "guard_anchor_type": "anchor_10"},
            {"guard_hit": "False", "year": 2025, "ret20_long": -0.20, "helped_veto": False, "source_type": "candidate", "guard_anchor_type": "anchor_10"},
        ]
    )

    summary = recent_degradation(rows)

    assert summary["guard_hit_2019_2023_n"] == 2
    assert summary["guard_hit_2024_2026_n"] == 1
    assert summary["recent_degradation_score_ret20"] < 0
    assert summary["recent_degradation_score_helped_rate"] < 0


def test_salvageability_drops_when_recent_subtype_fails_help_gate() -> None:
    rows = pd.DataFrame(
        [
            {
                "guard_hit": True,
                "year": 2025,
                "guard_anchor_type": "anchor_10",
                "source_type": "actual" if idx % 2 else "candidate",
                "ret20_long": 0.02,
                "helped_veto": idx < 10,
                "harmed_veto": False,
            }
            for idx in range(30)
        ]
    )

    summary = salvageability(rows)

    assert summary["salvageability_decision"] == "drop_short_veto"
    assert summary["subtype_candidates"][0]["n"] == 30
    assert summary["subtype_candidates"][0]["salvageable_observation"] is False


def test_run_writes_required_artifacts(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"
    input_root.mkdir()
    rows = pd.DataFrame(
        [
            {
                "source_artifact": "x",
                "source_name": "x",
                "source_type": "actual",
                "code": "1000",
                "decision_ymd": 20250101,
                "raw_side": "short",
                "guard_hit": True,
                "guard_anchor_type": "anchor_10",
                "guard_anchor_ymd": 20241220,
                "guard_active_until_ymd": 20250120,
                "ret20_long": 0.03,
                "ret40_long": 0.05,
                "short_return20": -0.03,
                "short_return40": -0.05,
                "helped_veto": True,
                "harmed_veto": False,
                "neutral_veto": False,
                "ma20_break_within_20d": False,
                "ma60_break_within_20d": False,
                "ma20_and_ma60_break_within_20d": False,
                "regime_proxy": "monthly_breakout_or_high_zone",
                "year": 2025,
                "period_bucket": "2024-2026",
            }
        ]
    )
    rows.to_csv(input_root / "short_veto_rows.csv", index=False)
    for name in [
        "short_veto_summary.json",
        "research_decision.json",
        "no_lookahead_audit.json",
    ]:
        (input_root / name).write_text(json.dumps({"audit_result": "pass", "research_decision": "weak_short_veto"}), encoding="utf-8")
    for name in [
        "period_stability_summary.csv",
        "regime_stability_summary.csv",
        "source_stability_summary.csv",
    ]:
        (input_root / name).write_text("x\n", encoding="utf-8")

    result = run(input_root=input_root, output_root=output_root)

    out = Path(result["output_dir"])
    assert all((out / artifact).exists() for artifact in REQUIRED_ARTIFACTS)
    assert result["research_decision"]["promotion_allowed"] is False
