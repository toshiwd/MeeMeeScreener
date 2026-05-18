from __future__ import annotations

import csv
import json
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import entry_precision_short_bottom_risk_full_recheck_v1 as mod


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


def _build_decision_inputs(
    *,
    baseline_count: int,
    challenger_count: int,
    hit_rate_delta: float,
    mean_ret20_delta: float,
    median_ret20_delta: float,
    removed_good: int,
    removed_bad: int,
    retained_bad: int,
    kept_good: int,
    months_both_sides: int,
    gain_rate_delta: float = 0.0,
    loss_rate_delta: float = 0.0,
    flat_rate_delta: float = 0.0,
    monthly_not_worse: bool = True,
    unresolved_count: int = 0,
) -> tuple[dict, dict, dict, dict]:
    compare_payload = {
        "session_id": "synthetic",
        "full_recheck_summary": {
            "baseline": {"count": baseline_count, "hit_rate": 0.50, "mean_ret20": 0.01, "median_ret20": 0.01},
            "challenger": {
                "count": challenger_count,
                "hit_rate": 0.50 + hit_rate_delta,
                "mean_ret20": 0.01 + mean_ret20_delta,
                "median_ret20": 0.01 + median_ret20_delta,
            },
            "delta": {
                "hit_rate_delta": hit_rate_delta,
                "mean_ret20_delta": mean_ret20_delta,
                "median_ret20_delta": median_ret20_delta,
                "removed_good_known": removed_good,
                "removed_bad_known": removed_bad,
                "retained_bad_known": retained_bad,
                "kept_good_known": kept_good,
            },
        },
    }
    monthly_payload = {
        "rollup": {
            "completed_bucket_count": 6,
            "months_with_both_sides": months_both_sides,
            "months_with_challenger_absent": 1,
            "months_with_mean_ret20_gain": 3,
            "months_with_mean_ret20_loss": 2,
            "months_with_mean_ret20_flat": 1,
            "gain_rate_on_both_sides": 0.5,
            "loss_rate_on_both_sides": 1 / 3,
            "flat_rate_on_both_sides": 1 / 6,
            "mixed_stability": True,
        },
        "source_rollup": {
            "months_with_both_sides": 3,
            "months_with_mean_ret20_gain": 1,
            "months_with_mean_ret20_loss": 1,
            "months_with_mean_ret20_flat": 1,
        },
        "comparison_to_source": {
            "not_worse_than_source": monthly_not_worse,
        },
    }
    unknown_resolution_payload = {
        "unresolved_count": unresolved_count,
        "resolved_outcome_summary": {
            "positive_count": 4,
            "nonpositive_count": 3,
            "retained_unknown_positive_count": 2,
            "retained_unknown_nonpositive_count": 1,
            "removed_unknown_positive_count": 2,
            "removed_unknown_nonpositive_count": 2,
        },
    }
    source_context = {
        "no_lookahead": {"no_lookahead_pass": True},
        "frozen_watch_decision": {"decision": "keep_frozen_watch_candidate"},
        "source_decision": {"decision": "keep_for_stability_replay"},
    }
    return compare_payload, monthly_payload, unknown_resolution_payload, source_context


def test_real_maturity_root_produces_keep_for_stability_replay(tmp_path: Path, monkeypatch) -> None:
    source_root = Path(
        r"G:\Tradex\entry_precision_short_bottom_risk_maturity_gate_v1\20260517T032317Z-entry-short-bottom-risk-maturity-gate-v1"
    )
    assert (source_root / "short_bottom_risk_maturity_gate_contract.json").exists()
    assert (source_root / "short_bottom_risk_unknown_rows.csv").exists()

    runtime_status = {
        "confirmed": True,
        "selected_runtime_db_path": r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb",
        "latest_available_global_date": 20260515,
        "latest_available_global_date_iso": "2026-05-15",
        "freshness_state": "fresh",
    }
    freshness = {
        "freshness_state": "fresh",
        "freshness_days": 1,
        "stale": False,
        "current_candidate_available": True,
        "snapshot_as_of": "2026-05-15",
    }
    monkeypatch.setattr(mod, "_get_runtime_stock_db_status", lambda: dict(runtime_status))
    monkeypatch.setattr(mod, "_get_rankings_freshness", lambda **_kwargs: dict(freshness))

    result = mod.run(Namespace(source_root=str(source_root), output_root=str(tmp_path / "out")))
    out_root = Path(result["output_dir"])

    assert result["decision"] == "keep_for_stability_replay"
    assert result["full_recheck_ready_now"] is True
    assert result["resolved_unknown_count"] == 17
    assert result["unresolved_count"] == 0
    assert result["monthly_not_worse_than_source"] is True

    for name in [
        "short_bottom_risk_full_recheck_contract.json",
        "short_bottom_risk_full_recheck_compare.json",
        "short_bottom_risk_full_recheck_confusion_groups.csv",
        "short_bottom_risk_full_recheck_monthly_stability.json",
        "short_bottom_risk_full_recheck_unknown_resolution.json",
        "short_bottom_risk_full_recheck_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert (out_root / name).exists()

    compare = json.loads((out_root / "short_bottom_risk_full_recheck_compare.json").read_text(encoding="utf-8"))
    monthly = json.loads((out_root / "short_bottom_risk_full_recheck_monthly_stability.json").read_text(encoding="utf-8"))
    unknown = json.loads((out_root / "short_bottom_risk_full_recheck_unknown_resolution.json").read_text(encoding="utf-8"))
    decision = json.loads((out_root / "short_bottom_risk_full_recheck_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((out_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((out_root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert compare["full_recheck_summary"]["baseline"]["count"] == 31
    assert compare["full_recheck_summary"]["challenger"]["count"] == 15
    assert compare["full_recheck_summary"]["delta"]["hit_rate_delta"] > 0
    assert compare["full_recheck_summary"]["delta"]["mean_ret20_delta"] > 0
    assert compare["full_recheck_summary"]["delta"]["median_ret20_delta"] > 0
    assert compare["full_recheck_summary"]["delta"]["removed_bad_known"] == 9
    assert compare["full_recheck_summary"]["delta"]["removed_good_known"] == 7
    assert compare["full_recheck_summary"]["delta"]["retained_bad_known"] == 7
    assert compare["full_recheck_summary"]["delta"]["kept_good_known"] == 8
    assert compare["resolution_summary"]["resolved_row_count"] == 31
    assert compare["resolution_summary"]["unresolved_row_count"] == 0
    assert compare["selection_branching"]["changed_top5_members_count"] == 16
    assert compare["selection_branching"]["changed_top10_members_count"] == 16
    assert compare["selection_branching"]["changed_rank_count"] == 1

    assert monthly["rollup"]["completed_bucket_count"] == 11
    assert monthly["rollup"]["months_with_both_sides"] == 7
    assert monthly["rollup"]["months_with_challenger_absent"] == 4
    assert monthly["rollup"]["months_with_mean_ret20_gain"] == 3
    assert monthly["rollup"]["months_with_mean_ret20_loss"] == 2
    assert monthly["rollup"]["months_with_mean_ret20_flat"] == 2
    assert monthly["comparison_to_source"]["not_worse_than_source"] is True

    assert unknown["all_unknown_rows_matured_now"] is True
    assert unknown["resolved_count"] == 17
    assert unknown["unresolved_count"] == 0
    assert unknown["unknowns_weakened_original_keep_interpretation"] is True

    assert decision["decision"] == "keep_for_stability_replay"
    assert decision["full_recheck_gain_persists"] is True
    assert decision["full_recheck_monthly_not_worse"] is True

    assert audit["no_lookahead_pass"] is True
    assert audit["future_outcome_fields_used_in_selection"] == []
    assert complete["complete"] is True


@pytest.mark.parametrize(
    ("case_name", "kwargs", "expected"),
    [
        (
            "small_sample",
            {
                "baseline_count": 11,
                "challenger_count": 7,
                "hit_rate_delta": 0.10,
                "mean_ret20_delta": 0.02,
                "median_ret20_delta": 0.01,
                "removed_good": 3,
                "removed_bad": 4,
                "retained_bad": 2,
                "kept_good": 3,
                "months_both_sides": 3,
                "monthly_not_worse": True,
            },
            "hold_due_to_small_sample",
        ),
        (
            "removed_good",
            {
                "baseline_count": 20,
                "challenger_count": 12,
                "hit_rate_delta": 0.05,
                "mean_ret20_delta": 0.01,
                "median_ret20_delta": 0.01,
                "removed_good": 5,
                "removed_bad": 2,
                "retained_bad": 2,
                "kept_good": 4,
                "months_both_sides": 5,
                "monthly_not_worse": True,
            },
            "drop_due_to_removed_good_shorts",
        ),
        (
            "retained_bad",
            {
                "baseline_count": 20,
                "challenger_count": 12,
                "hit_rate_delta": 0.05,
                "mean_ret20_delta": 0.01,
                "median_ret20_delta": 0.01,
                "removed_good": 2,
                "removed_bad": 5,
                "retained_bad": 6,
                "kept_good": 4,
                "months_both_sides": 5,
                "monthly_not_worse": True,
            },
            "drop_due_to_retained_bad_shorts",
        ),
        (
            "monthly_mixed",
            {
                "baseline_count": 20,
                "challenger_count": 12,
                "hit_rate_delta": 0.05,
                "mean_ret20_delta": 0.01,
                "median_ret20_delta": 0.01,
                "removed_good": 2,
                "removed_bad": 5,
                "retained_bad": 2,
                "kept_good": 5,
                "months_both_sides": 5,
                "monthly_not_worse": False,
            },
            "hold_due_to_mixed_monthly_stability",
        ),
        (
            "unknown_adjusted",
            {
                "baseline_count": 20,
                "challenger_count": 12,
                "hit_rate_delta": 0.05,
                "mean_ret20_delta": 0.01,
                "median_ret20_delta": 0.01,
                "removed_good": 2,
                "removed_bad": 5,
                "retained_bad": 2,
                "kept_good": 5,
                "months_both_sides": 5,
                "monthly_not_worse": True,
                "unresolved_count": 1,
            },
            "drop_as_unknown_adjusted_edge_insufficient",
        ),
    ],
)
def test_decision_helper_covers_all_labels(case_name: str, kwargs: dict[str, object], expected: str) -> None:
    compare_payload, monthly_payload, unknown_resolution_payload, source_context = _build_decision_inputs(**kwargs)
    compare_payload["session_id"] = f"synthetic-{case_name}"
    decision = mod._build_decision(
        compare_payload=compare_payload,
        monthly_payload=monthly_payload,
        unknown_resolution_payload=unknown_resolution_payload,
        source_context=source_context,
    )
    assert decision["decision"] == expected
