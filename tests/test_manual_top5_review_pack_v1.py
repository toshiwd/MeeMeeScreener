from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import manual_top5_review_pack_v1 as pack


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _feature_row(day: str, code: str, rank: int) -> dict:
    return {
        "code": code,
        "decision_ymd": int(day.replace("-", "")),
        "candidate_rank": rank,
        "selection_score": 15,
        "ma_stack_state": "ma_bull_stack_7_20_60",
        "prior_high_distance_pct": 0.02,
        "prior_low_distance_pct": 0.08,
        "box_upper_distance_pct": 0.03,
        "box_lower_distance_pct": 0.10,
        "weekly_resistance_distance_pct": 0.04,
        "monthly_resistance_distance_pct": 0.05,
        "breakout_above_resistance_flag": rank == 1,
        "failed_breakout_flag": False,
        "gap_up_flag": rank == 2,
        "gap_down_flag": False,
        "gap_size_atr_ratio": 0.4,
        "gap_fail_same_day_flag": False,
        "gap_fill_3d_flag": False,
        "bullish_full_retrace_flag": False,
        "bearish_full_retrace_flag": False,
        "engulfing_bullish_flag": False,
        "engulfing_bearish_flag": False,
        "denial_of_prior_bull_flag": False,
        "denial_of_prior_bear_flag": False,
        "close_above_ma7_count": 3,
        "close_above_ma20_count": 8,
        "close_below_ma20_count": 0,
        "days_since_ma20_reclaim": 4,
        "days_since_ma20_break": 99,
        "ma7_ma20_distance_pct": 0.01,
        "ma20_slope": 0.02,
        "ma60_slope": 0.01,
        "body_range_pct": 0.02,
        "sideways_length_days": 12,
        "box_length_days": 18,
        "atr_compression_ratio": 0.9,
        "ma_compression_flag": False,
        "box_breakout_flag": rank == 1,
        "box_breakdown_flag": False,
        "volume_compression_ratio": 1.2,
        "volume_confirmed_denial_flag": False,
        "shakeout_recovery_candidate_flag": False,
        "true_breakdown_candidate_flag": False,
        "feature_missing": False,
    }


def _make_chart_root(root: Path) -> None:
    chart_root = root / "chart_context_feature_contract_v1"
    rows = [
        _feature_row("2025-12-25", "5801", 1),
        _feature_row("2025-12-25", "8035", 2),
        _feature_row("2025-02-17", "5210", 1),
    ]
    frame = pd.DataFrame(rows)
    chart_root.mkdir(parents=True)
    for name in [
        "chart_context_features_daily.parquet",
        "chart_context_features_weekly.parquet",
        "chart_context_features_monthly.parquet",
    ]:
        frame.to_parquet(chart_root / name, index=False)


def _make_source_pack(path: Path) -> None:
    _write_json(
        path,
        {
            "examples": {
                "high_human_selectable": [
                    {
                        "event_date": "2025-12-25",
                        "starter_top5": [
                            {
                                "symbol": "5801",
                                "rank": 1,
                                "score": 7.0,
                                "monthly_prior_state": "monthly_prior_uptrend",
                                "ret20_fwd": 0.5,
                            }
                        ],
                    }
                ]
            }
        },
    )


def test_manual_review_pack_emits_required_artifacts_without_outcome_columns(tmp_path: Path) -> None:
    robustness_root = tmp_path / "robustness"
    _make_chart_root(robustness_root)
    source_json = tmp_path / "representative_top5_candidate_lists.json"
    _make_source_pack(source_json)

    output = pack.build_review_pack(
        robustness_root=robustness_root,
        source_representative_json=source_json,
        output_root=tmp_path / "out",
        compute_missing_from_source_db=False,
    )

    complete = json.loads((output / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    summary = json.loads((output / "manual_top5_review_pack_summary.json").read_text(encoding="utf-8"))
    sheet = pd.read_csv(output / "manual_review_sheet.csv")

    assert complete["complete"] is True
    assert complete["required_artifacts_all_present"] is True
    assert set(pack.REQUIRED_ARTIFACTS) == set(complete["required_artifacts"])
    assert summary["target_candidate_count"] == 40
    assert summary["chart_context_available_count"] == 3
    assert summary["post_run_outcome_visible_before_review"] is False
    assert "post_ret_20" not in sheet.columns
    assert "monthly_context" in sheet.columns
    assert "human_selectable_label" in sheet.columns


def test_review_template_and_schema_enforce_max_three_manual_selection(tmp_path: Path) -> None:
    robustness_root = tmp_path / "robustness"
    _make_chart_root(robustness_root)
    source_json = tmp_path / "representative_top5_candidate_lists.json"
    _make_source_pack(source_json)

    output = pack.build_review_pack(
        robustness_root=robustness_root,
        source_representative_json=source_json,
        output_root=tmp_path / "out",
        compute_missing_from_source_db=False,
    )

    schema = json.loads((output / "review_result_schema.json").read_text(encoding="utf-8"))
    audit = json.loads((output / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    template_lines = (output / "human_selection_decision_template.jsonl").read_text(encoding="utf-8").strip().splitlines()
    first = json.loads(template_lines[0])

    assert schema["per_sample_max_selected"] == 3
    assert "select_strong" in schema["review_labels"]
    assert "post_ret" in schema["forbidden_review_inputs"]
    assert audit["audit_result"] == "pass"
    assert audit["post_run_outcome_in_review_sheet"] is False
    assert len(template_lines) == 40
    assert first["max3_selected_flag"] is None
