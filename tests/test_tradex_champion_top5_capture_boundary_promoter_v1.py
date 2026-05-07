from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.tradex_champion_top5_capture_boundary_promoter_v1 import (
    _build_outputs,
    _select_candidate,
)


def _base_group(num_rows: int = 20) -> pd.DataFrame:
    rows = []
    for idx in range(num_rows):
        rank = idx + 1
        rows.append(
            {
                "anchor_date": "2025-01-03",
                "side": "long",
                "month_bucket": "2025-01",
                "symbol": f"S{rank:02d}",
                "score": 100.0 - idx,
                "champion_rank": rank,
                "forward_ret_20d": float(num_rows - idx),
                "champion_selected_top20": True,
                "top15_label": rank <= 15,
                "bottom15_label": rank > 15,
                "monthly_context": "monthly_range",
                "weekly_context": "weekly_range",
                "shape_classification": "shape_positive_modifier",
                "family_regime_context": "C:risk_on_range",
                "gap_pct": 0.01,
                "vol_ratio5_20": 1.2,
                "candle_body_ratio": 0.4,
                "path_value_score_v1": 0.0,
            }
        )
    frame = pd.DataFrame(rows)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").astype("Int64")
    return frame


def test_no_promotion_when_promotion_gate_fails() -> None:
    group = _base_group()
    group.loc[group["champion_rank"].eq(6), "path_value_score_v1"] = 0.05
    group.loc[group["champion_rank"].eq(5), "path_value_score_v1"] = -0.10

    adjusted = _select_candidate(group)

    assert int(adjusted["promotion_applied"].sum()) == 0
    assert int(adjusted["demotion_applied"].sum()) == 0
    assert int(adjusted["changed_top5_member"].sum()) == 0
    assert adjusted.sort_values("adjusted_rank")["champion_rank"].tolist() == list(range(1, 21))


def test_no_promotion_when_demotion_gate_fails() -> None:
    group = _base_group()
    group.loc[group["champion_rank"].eq(6), "path_value_score_v1"] = 0.20
    group.loc[group["champion_rank"].le(5), "path_value_score_v1"] = [0.05, 0.06, 0.07, 0.08, 0.09]

    adjusted = _select_candidate(group)

    assert int(adjusted["promotion_applied"].sum()) == 0
    assert int(adjusted["demotion_applied"].sum()) == 0
    assert int(adjusted["changed_top5_member"].sum()) == 0


def test_one_controlled_promotion_when_both_gates_pass() -> None:
    group = _base_group()
    group.loc[group["champion_rank"].eq(6), "path_value_score_v1"] = 0.20
    group.loc[group["champion_rank"].eq(5), "path_value_score_v1"] = -0.10

    adjusted = _select_candidate(group)
    promoted = adjusted[adjusted["promotion_applied"].fillna(False).astype(bool)]
    demoted = adjusted[adjusted["demotion_applied"].fillna(False).astype(bool)]

    assert int(promoted.shape[0]) == 1
    assert int(demoted.shape[0]) == 1
    assert int(adjusted["changed_top5_member"].sum()) == 2
    assert int(adjusted["candidate_selected_top5"].sum()) == 5
    assert promoted["champion_rank"].iloc[0] == 6
    assert demoted["champion_rank"].iloc[0] == 5
    original_order = adjusted.sort_values("champion_rank")["symbol"].tolist()
    candidate_order = adjusted.sort_values("adjusted_rank")["symbol"].tolist()
    removed = {promoted["symbol"].iloc[0], demoted["symbol"].iloc[0]}
    assert [symbol for symbol in candidate_order if symbol not in removed] == [symbol for symbol in original_order if symbol not in removed]


def test_no_rank_outside_top20_can_enter_top5() -> None:
    group = _base_group(num_rows=21)
    group.loc[group["champion_rank"].eq(21), "path_value_score_v1"] = 0.99
    group.loc[group["champion_rank"].eq(5), "path_value_score_v1"] = -0.10
    group.loc[group["champion_rank"].eq(6), "path_value_score_v1"] = 0.20

    adjusted = _select_candidate(group)

    assert not bool(adjusted.loc[adjusted["champion_rank"].eq(21), "promotion_pool_member"].iloc[0])
    assert not bool(adjusted.loc[adjusted["champion_rank"].eq(21), "candidate_selected_top5"].iloc[0])


def test_artifacts_and_anti_leakage_contract(tmp_path: Path) -> None:
    frame = pd.concat(
        [
            _base_group().assign(anchor_date="2025-01-03", month_bucket="2025-01"),
            _base_group().assign(anchor_date="2025-02-03", month_bucket="2025-02"),
        ],
        ignore_index=True,
    )
    frame.loc[frame["champion_rank"].eq(6), "path_value_score_v1"] = 0.20
    frame.loc[frame["champion_rank"].eq(5), "path_value_score_v1"] = -0.10

    payload = _build_outputs(frame, output_root=tmp_path, source_rows_parquet=Path(r"G:\Tradex\source.parquet"))
    required = {
        "candidate_manifest.json",
        "evaluation_contract.json",
        "branching_probe.json",
        "monthly_top5_capture_summary.json",
        "topk_effectiveness_summary.json",
        "promotion_quality_summary.json",
        "regime_split_summary.json",
        "turnover_summary.json",
        "compare.json",
        "decision_summary.json",
        "meemee_reflectability_assessment.json",
        "anti_leakage_audit.json",
        "static_gate_oos_diagnostic.json",
        "report.md",
        "_ARTIFACT_COMPLETE.json",
    }

    assert required.issubset(payload["paths"].keys())
    for name in required:
        assert Path(payload["paths"][name]).exists()

    anti = json.loads(Path(payload["paths"]["anti_leakage_audit.json"]).read_text(encoding="utf-8"))
    manifest = json.loads(Path(payload["paths"]["candidate_manifest.json"]).read_text(encoding="utf-8"))
    compare = json.loads(Path(payload["paths"]["compare.json"]).read_text(encoding="utf-8"))
    oos = json.loads(Path(payload["paths"]["static_gate_oos_diagnostic.json"]).read_text(encoding="utf-8"))

    assert anti["pass"] is True
    assert anti["used_future_labels_in_scoring"] is False
    assert "forward_ret_20d" in anti["excluded_label_columns"]
    assert "top15_label" in anti["excluded_label_columns"]
    assert manifest["static_gate_mode"] == "static_non_optimized_v1"
    assert manifest["gate_thresholds"]["promotion_path_value_score_v1"] == 0.10
    assert compare["same_condition_contract"]["same_universe"] is True
    assert oos["oos_diagnostic_status"] == "run"
