from __future__ import annotations

import csv
import json
from argparse import Namespace
from pathlib import Path

from scripts import entry_precision_short_bottom_risk_closed_horizon_stability_v1 as mod


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _fixture_row(
    ymd: int,
    code: str,
    *,
    baseline: bool,
    challenger: bool,
    known: bool,
    ret20: float | None,
) -> dict:
    return {
        "ymd": str(ymd),
        "code": code,
        "confusion_group": "kept_good",
        "baseline_selected": str(baseline),
        "challenger_selected": str(challenger),
        "outcome_known": str(known),
        "outcome_positive": "" if ret20 is None else str(ret20 > 0.0),
        "outcome_bucket": "positive" if ret20 is not None and ret20 > 0.0 else "nonpositive" if ret20 is not None else "missing",
        "short_ret_20": "" if ret20 is None else str(ret20),
        "short_ret_10": "" if ret20 is None else str(ret20 / 2.0),
        "short_ret_5": "" if ret20 is None else str(ret20 / 3.0),
        "close_pos": "0.1",
        "dist_low20": "0.002",
        "dist_ma20_signed": "-0.04",
        "day_change_pct": "-0.02",
        "monthlyRangeProb": "0.2",
        "monthlyRangePos": "0.1",
        "weeklyBreakoutDownProb": "0.7",
        "monthlyBreakoutDownProb": "0.6",
        "marketRiskOff": "True",
        "marketRegime": "risk_off",
        "trendDownStrict": "True",
        "entryScore": "0.8",
        "tradePriorityScore": "0.7",
        "liquidity20d": "1000000",
        "mae20": "0.01",
        "mfe20": "0.04",
        "baseline_rank": "1",
        "tradeDecisionReasons": json.dumps(["reason"], ensure_ascii=False),
        "tradeRiskWatch": json.dumps([], ensure_ascii=False),
    }


def _build_fixture_root(tmp_path: Path) -> Path:
    source_root = tmp_path / "source"
    rows = [
        _fixture_row(20250331, "A001", baseline=True, challenger=True, known=True, ret20=0.05),
        _fixture_row(20250331, "A002", baseline=True, challenger=False, known=True, ret20=-0.03),
        _fixture_row(20250331, "A003", baseline=True, challenger=True, known=True, ret20=-0.01),
        _fixture_row(20250430, "A004", baseline=True, challenger=False, known=False, ret20=None),
        _fixture_row(20250430, "A005", baseline=True, challenger=True, known=False, ret20=None),
        _fixture_row(20250430, "A006", baseline=True, challenger=False, known=False, ret20=None),
    ]
    _write_csv(source_root / "short_bottom_risk_confusion_groups.csv", rows)
    _write_json(
        source_root / "short_bottom_risk_diagnostic_contract.json",
        {
            "schema_version": "tradex_entry_precision_short_bottom_risk_diagnostic_contract_v1",
            "session_id": "synthetic",
            "same_condition_contract": {
                "same_universe": True,
                "same_period": True,
                "same_top_k": True,
                "same_regime": True,
                "same_cost": True,
                "same_artifact_detail_level": True,
                "long_logic_frozen": True,
                "one_axis_only": True,
                "no_meemee_ui_change": True,
                "no_production_state_change": True,
            },
        },
    )
    _write_json(
        source_root / "short_bottom_risk_feature_comparison.json",
        {
            "schema_version": "tradex_entry_precision_short_bottom_risk_feature_comparison_v1",
            "session_id": "synthetic",
            "baseline_id": "current_rule_trade_gate_baseline",
            "challenger_id": "short_cleanup_bottom_risk_v1",
        },
    )
    _write_json(source_root / "short_bottom_risk_failure_diagnosis.json", {"decision": "hold_due_to_small_sample"})
    _write_json(source_root / "short_bottom_risk_next_axis_decision.json", {"decision": "hold_due_to_small_sample"})
    return source_root


def test_closed_horizon_stability_bundle_is_written(tmp_path: Path) -> None:
    source_root = _build_fixture_root(tmp_path)
    output_root = tmp_path / "output"
    result = mod.run(Namespace(source_root=str(source_root), output_root=str(output_root)))
    out_root = Path(result["output_dir"])

    assert result["decision"] == "hold_until_unknown_horizon_completes"
    assert result["unknown_materiality"] is True
    assert result["completed_month_count"] == 1

    for name in [
        "short_bottom_risk_closed_horizon_contract.json",
        "short_bottom_risk_closed_horizon_compare.json",
        "short_bottom_risk_monthly_stability.json",
        "short_bottom_risk_unknown_impact.json",
        "short_bottom_risk_stability_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert (out_root / name).exists()

    monthly = json.loads((out_root / "short_bottom_risk_monthly_stability.json").read_text(encoding="utf-8"))
    assert monthly["rollup"]["completed_bucket_count"] == 1
    assert monthly["completed_months"][0]["month"] == "202503"


def test_real_closed_horizon_stability_root_produces_expected_decision(tmp_path: Path) -> None:
    source_root = Path(
        r"G:\Tradex\entry_precision_short_bottom_risk_diagnostic_v1\20260517T024751Z-entry-short-bottom-risk-diagnostic-v1"
    )
    assert (source_root / "short_bottom_risk_confusion_groups.csv").exists()
    assert (source_root / "short_bottom_risk_diagnostic_contract.json").exists()
    assert (source_root / "short_bottom_risk_feature_comparison.json").exists()
    assert (source_root / "short_bottom_risk_failure_diagnosis.json").exists()
    assert (source_root / "short_bottom_risk_next_axis_decision.json").exists()

    output_root = tmp_path / "real_output"
    result = mod.run(Namespace(source_root=str(source_root), output_root=str(output_root)))
    out_root = Path(result["output_dir"])

    assert result["decision"] == "hold_until_unknown_horizon_completes"
    assert result["completed_month_count"] == 5
    assert result["known_baseline_count"] == 14
    assert result["known_challenger_count"] == 8
    assert result["unknown_materiality"] is True

    decision = json.loads((out_root / "short_bottom_risk_stability_decision.json").read_text(encoding="utf-8"))
    compare = json.loads((out_root / "short_bottom_risk_closed_horizon_compare.json").read_text(encoding="utf-8"))
    unknown = json.loads((out_root / "short_bottom_risk_unknown_impact.json").read_text(encoding="utf-8"))
    monthly = json.loads((out_root / "short_bottom_risk_monthly_stability.json").read_text(encoding="utf-8"))

    assert decision["decision"] == "hold_until_unknown_horizon_completes"
    assert compare["closed_horizon_summary"]["delta"]["hit_rate_delta"] > 0
    assert compare["closed_horizon_summary"]["delta"]["mean_ret20_delta"] > 0
    assert unknown["removed_unknown_count"] == 10
    assert monthly["rollup"]["months_with_mean_ret20_gain"] == 1
    assert monthly["rollup"]["months_with_mean_ret20_loss"] == 1


def test_decision_json_mentions_unknown_horizon_and_no_lookahead(tmp_path: Path) -> None:
    source_root = _build_fixture_root(tmp_path)
    output_root = tmp_path / "output"
    result = mod.run(Namespace(source_root=str(source_root), output_root=str(output_root)))
    out_root = Path(result["output_dir"])
    decision = json.loads((out_root / "short_bottom_risk_stability_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((out_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))

    assert "unknown_rows_materially_affect_the_previous_keep_interpretation" in json.dumps(decision, ensure_ascii=False)
    assert audit["no_lookahead_pass"] is True
    assert audit["future_outcome_fields_used_in_selection"] == []
