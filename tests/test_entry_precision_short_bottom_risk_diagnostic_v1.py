from __future__ import annotations

import csv
import json
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import entry_precision_short_bottom_risk_diagnostic_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _build_fixture_roots(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    compare_root = tmp_path / "compare_root"
    family_root = tmp_path / "family_root"
    closepos_path = tmp_path / "closepos" / "entry_precision_short_broad_down_closepos_fix_decision.json"
    monthly_path = tmp_path / "monthly" / "entry_precision_short_broad_down_monthly_fix_decision.json"
    output_dir = tmp_path / "output"

    baseline_rows = [
        {
            "ymd": 20250331,
            "code": "A001",
            "short_ret_20": 0.05,
            "short_ret_10": 0.04,
            "short_ret_5": 0.03,
            "mae20": 0.01,
            "mfe20": 0.08,
            "close_pos": 0.12,
            "dist_low20": 0.004,
            "dist_ma20_signed": -0.04,
            "day_change_pct": -0.02,
            "monthlyRangeProb": 0.25,
            "monthlyRangePos": 0.10,
            "weeklyBreakoutDownProb": 0.7,
            "monthlyBreakoutDownProb": 0.6,
            "marketRiskOff": True,
            "marketRegime": "risk_off",
            "trendDownStrict": True,
            "entryScore": 0.8,
            "tradePriorityScore": 0.7,
            "liquidity20d": 1_000_000.0,
            "baseline_rank": 1,
            "tradeDecisionReasons": ["A", "B"],
            "tradeRiskWatch": [],
        },
        {
            "ymd": 20250331,
            "code": "A002",
            "short_ret_20": -0.03,
            "short_ret_10": -0.01,
            "short_ret_5": -0.02,
            "mae20": 0.02,
            "mfe20": 0.03,
            "close_pos": 0.08,
            "dist_low20": 0.003,
            "dist_ma20_signed": -0.06,
            "day_change_pct": -0.05,
            "monthlyRangeProb": 0.10,
            "monthlyRangePos": 0.02,
            "weeklyBreakoutDownProb": 0.8,
            "monthlyBreakoutDownProb": 0.7,
            "marketRiskOff": True,
            "marketRegime": "risk_off",
            "trendDownStrict": True,
            "entryScore": 0.9,
            "tradePriorityScore": 0.8,
            "liquidity20d": 2_000_000.0,
            "baseline_rank": 2,
            "tradeDecisionReasons": ["A", "B"],
            "tradeRiskWatch": [],
        },
        {
            "ymd": 20250331,
            "code": "A003",
            "short_ret_20": 0.02,
            "short_ret_10": 0.01,
            "short_ret_5": 0.0,
            "mae20": 0.03,
            "mfe20": 0.05,
            "close_pos": 0.11,
            "dist_low20": 0.002,
            "dist_ma20_signed": -0.03,
            "day_change_pct": -0.02,
            "monthlyRangeProb": 0.21,
            "monthlyRangePos": 0.15,
            "weeklyBreakoutDownProb": 0.65,
            "monthlyBreakoutDownProb": 0.55,
            "marketRiskOff": False,
            "marketRegime": "risk_on",
            "trendDownStrict": True,
            "entryScore": 0.6,
            "tradePriorityScore": 0.5,
            "liquidity20d": 1_500_000.0,
            "baseline_rank": 3,
            "tradeDecisionReasons": ["C"],
            "tradeRiskWatch": ["watch"],
        },
        {
            "ymd": 20250331,
            "code": "A004",
            "short_ret_20": -0.01,
            "short_ret_10": -0.02,
            "short_ret_5": -0.01,
            "mae20": 0.04,
            "mfe20": 0.02,
            "close_pos": 0.05,
            "dist_low20": 0.001,
            "dist_ma20_signed": -0.02,
            "day_change_pct": -0.01,
            "monthlyRangeProb": 0.13,
            "monthlyRangePos": 0.07,
            "weeklyBreakoutDownProb": 0.82,
            "monthlyBreakoutDownProb": 0.65,
            "marketRiskOff": False,
            "marketRegime": "risk_on",
            "trendDownStrict": True,
            "entryScore": 0.55,
            "tradePriorityScore": 0.45,
            "liquidity20d": 900_000.0,
            "baseline_rank": 4,
            "tradeDecisionReasons": ["D"],
            "tradeRiskWatch": [],
        },
        {
            "ymd": 20250331,
            "code": "A005",
            "short_ret_20": None,
            "short_ret_10": None,
            "short_ret_5": None,
            "mae20": None,
            "mfe20": None,
            "close_pos": 0.07,
            "dist_low20": 0.005,
            "dist_ma20_signed": -0.05,
            "day_change_pct": -0.03,
            "monthlyRangeProb": 0.32,
            "monthlyRangePos": 0.11,
            "weeklyBreakoutDownProb": 0.75,
            "monthlyBreakoutDownProb": 0.68,
            "marketRiskOff": True,
            "marketRegime": "risk_off",
            "trendDownStrict": True,
            "entryScore": 0.77,
            "tradePriorityScore": 0.66,
            "liquidity20d": 800_000.0,
            "baseline_rank": 5,
            "tradeDecisionReasons": ["E"],
            "tradeRiskWatch": [],
        },
        {
            "ymd": 20250331,
            "code": "A006",
            "short_ret_20": None,
            "short_ret_10": None,
            "short_ret_5": None,
            "mae20": None,
            "mfe20": None,
            "close_pos": 0.14,
            "dist_low20": 0.007,
            "dist_ma20_signed": -0.03,
            "day_change_pct": -0.02,
            "monthlyRangeProb": 0.28,
            "monthlyRangePos": 0.19,
            "weeklyBreakoutDownProb": 0.78,
            "monthlyBreakoutDownProb": 0.61,
            "marketRiskOff": False,
            "marketRegime": "risk_on",
            "trendDownStrict": True,
            "entryScore": 0.49,
            "tradePriorityScore": 0.41,
            "liquidity20d": 700_000.0,
            "baseline_rank": 6,
            "tradeDecisionReasons": ["F"],
            "tradeRiskWatch": [],
        },
    ]

    selected_rows = [baseline_rows[0], baseline_rows[1], baseline_rows[4]]
    _write_json(
        compare_root / "entry_precision_short_challenger_compare.json",
        {
            "schema_version": "tradex_entry_precision_short_compare_v1",
            "session_id": "entry-short-precision-synthetic",
            "generated_at": "2026-05-17T00:00:00+00:00",
            "baseline_id": "current_rule_trade_gate_baseline",
            "long_freeze_confirmed": True,
            "baseline": {"count": 6},
            "taxonomy": {},
            "feature_map": {},
            "variants": {
                "short_cleanup_bottom_risk_v1": {
                    "variant": "short_cleanup_bottom_risk_v1",
                    "target": "neutral suppression",
                    "baseline": {"count": 6, "hit_rate": 0.5, "mean_ret20": 0.01, "median_ret20": 0.0},
                    "challenger": {"count": 3, "hit_rate": 0.6666666666666666, "mean_ret20": 0.03, "median_ret20": 0.05},
                    "delta": {
                        "selected_count_delta": -3,
                        "hit_rate_delta": 0.16666666666666663,
                        "median_ret20_delta": 0.05,
                        "mean_ret20_delta": 0.02,
                        "changed_top5_short_count": 3,
                        "changed_top10_short_count": 3,
                        "changed_rank_short_count": 1,
                        "bad_short_removal_count": 1,
                        "false_neutral_short_recovery_count": 1,
                    },
                    "monthly_rows": [],
                    "selected_codes_by_month": {},
                    "baseline_codes_by_month": {},
                    "selected_rows": selected_rows,
                    "baseline_rows": baseline_rows,
                }
            },
            "decision_rollup": {"overall": "hold", "per_variant": {"short_cleanup_bottom_risk_v1": "keep"}},
            "same_condition_contract": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": True,
                "same_regime": True,
                "same_cost": True,
                "same_artifact_detail_level": True,
                "long_short_separated": True,
                "one_axis_only": True,
                "no_meemee_ui_change": True,
            },
        },
    )
    _write_json(
        family_root / "entry_precision_short_decision.json",
        {
            "overall_decision": "hold",
            "decisions": {"short_cleanup_bottom_risk_v1": {"decision": "keep"}},
        },
    )
    _write_json(closepos_path, {"overall_decision": "drop"})
    _write_json(monthly_path, {"overall_decision": "drop"})
    return compare_root, family_root, closepos_path, monthly_path, output_dir


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_build_confusion_rows_classifies_known_and_missing_groups(tmp_path: Path) -> None:
    compare_root, family_root, closepos_path, monthly_path, output_dir = _build_fixture_roots(tmp_path)
    args = Namespace(
        compare_root=str(compare_root),
        family_decision_root=str(family_root),
        closepos_decision_path=str(closepos_path),
        monthly_decision_path=str(monthly_path),
        output_dir=str(output_dir),
    )
    result = mod.run(args)
    assert result["decision"] == "hold_due_to_small_sample"
    assert Path(result["output_dir"]).exists()

    confusion_rows = _read_csv(Path(result["output_dir"]) / "short_bottom_risk_confusion_groups.csv")
    groups = {row["confusion_group"] for row in confusion_rows}
    assert {"kept_good", "retained_bad", "removed_good", "removed_bad", "retained_unknown", "removed_unknown"}.issubset(groups)
    retained_bad = [row for row in confusion_rows if row["confusion_group"] == "retained_bad"]
    removed_good = [row for row in confusion_rows if row["confusion_group"] == "removed_good"]
    assert len(retained_bad) == 1
    assert len(removed_good) == 1
    assert retained_bad[0]["code"] == "A002"
    assert removed_good[0]["code"] == "A003"


def test_run_against_real_artifacts_writes_requested_bundle(tmp_path: Path) -> None:
    real_compare_root = Path(r"G:\Tradex\entry_precision_short_audit_v1")
    real_family_root = Path(r"G:\Tradex\entry_precision_short_audit_v1")
    real_closepos = Path(r"G:\Tradex\entry_precision_short_broad_down_closepos_audit_v1\entry_precision_short_broad_down_closepos_fix_decision.json")
    real_monthly = Path(r"G:\Tradex\entry_precision_short_broad_down_monthly_fix_audit_v1\entry_precision_short_broad_down_monthly_fix_decision.json")
    assert (real_compare_root / "entry_precision_short_challenger_compare.json").exists()
    assert (real_family_root / "entry_precision_short_decision.json").exists()
    assert real_closepos.exists()
    assert real_monthly.exists()

    output_dir = tmp_path / "real_output"
    args = Namespace(
        compare_root=str(real_compare_root),
        family_decision_root=str(real_family_root),
        closepos_decision_path=str(real_closepos),
        monthly_decision_path=str(real_monthly),
        output_dir=str(output_dir),
    )
    result = mod.run(args)
    out_root = Path(result["output_dir"])
    assert result["decision"] == "hold_due_to_small_sample"
    assert out_root.exists()
    for name in [
        "short_bottom_risk_diagnostic_contract.json",
        "short_bottom_risk_confusion_groups.csv",
        "short_bottom_risk_feature_comparison.json",
        "short_bottom_risk_removed_good_shorts.csv",
        "short_bottom_risk_retained_bad_shorts.csv",
        "short_bottom_risk_failure_diagnosis.json",
        "short_bottom_risk_next_axis_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert (out_root / name).exists()

    failure = json.loads((out_root / "short_bottom_risk_failure_diagnosis.json").read_text(encoding="utf-8"))
    assert failure["decision"] == "hold_due_to_small_sample"
    assert failure["true_bad_pick_removal_visible"] is True
    assert failure["sample_shrinkage_only"] is False


def test_next_axis_decision_matches_small_sample_hold(tmp_path: Path) -> None:
    compare_root, family_root, closepos_path, monthly_path, output_dir = _build_fixture_roots(tmp_path)
    args = Namespace(
        compare_root=str(compare_root),
        family_decision_root=str(family_root),
        closepos_decision_path=str(closepos_path),
        monthly_decision_path=str(monthly_path),
        output_dir=str(output_dir),
    )
    result = mod.run(args)
    next_axis = json.loads((Path(result["output_dir"]) / "short_bottom_risk_next_axis_decision.json").read_text(encoding="utf-8"))
    assert next_axis["decision"] == "hold_due_to_small_sample"
    assert next_axis["next_axis_justified"] is False
    assert next_axis["candidate_axis_hint"] is None
