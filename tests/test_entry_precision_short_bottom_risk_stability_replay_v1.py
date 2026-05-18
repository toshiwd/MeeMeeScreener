from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import entry_precision_short_bottom_risk_stability_replay_v1 as mod


def _build_decision_inputs(
    *,
    snapshot_rollup: dict,
    monthly_rollup: dict,
    monthly_comparison: dict,
    borrow_summary: dict,
    regime_summary: dict | None = None,
) -> tuple[dict, dict, dict, dict, dict]:
    source_context = {
        "source_root": Path(r"G:\Tradex\entry_precision_short_bottom_risk_full_recheck_v1\20260517T034734Z-entry-short-bottom-risk-full-recheck-v1"),
        "compare": {
            "full_recheck_summary": {
                "baseline": {"count": 31, "hit_rate": 0.4838709677419355, "mean_ret20": -0.004175985476089884, "median_ret20": -0.004592968007602154},
                "challenger": {"count": 15, "hit_rate": 0.5333333333333333, "mean_ret20": -0.002387015700803758, "median_ret20": 0.010251630941286114},
                "delta": {
                    "hit_rate_delta": 0.04946236559139783,
                    "mean_ret20_delta": 0.0017889697752861257,
                    "median_ret20_delta": 0.014844598948888269,
                },
            }
        },
        "monthly": {
            "rollup": {
                "completed_bucket_count": 11,
                "months_with_both_sides": 7,
                "months_with_challenger_absent": 4,
                "months_with_mean_ret20_gain": 3,
                "months_with_mean_ret20_loss": 2,
                "months_with_mean_ret20_flat": 2,
                "gain_rate_on_both_sides": 3 / 7,
                "loss_rate_on_both_sides": 2 / 7,
                "flat_rate_on_both_sides": 2 / 7,
                "mixed_stability": True,
            }
        },
        "decision": {"decision": "keep_for_stability_replay"},
        "no_lookahead": {"no_lookahead_pass": True},
    }
    if regime_summary is None:
        regime_summary = {
            "positive_bucket_count": 2,
            "negative_bucket_count": 1,
            "broad_down_edge_positive": True,
            "flat_or_mixed_edge_positive": True,
            "upward_or_non_short_favorable_edge_positive": False,
            "edge_is_broad_down_only": False,
        }
    snapshot_payload = {"rollup": snapshot_rollup}
    monthly_payload = {
        "rollup": monthly_rollup,
        "source_rollup": {
            "months_with_both_sides": 3,
            "months_with_mean_ret20_gain": 1,
            "months_with_mean_ret20_loss": 1,
            "months_with_mean_ret20_flat": 1,
        },
        "comparison_to_source": monthly_comparison,
    }
    regime_payload = {"summary": regime_summary}
    borrow_payload = {"summary": borrow_summary}
    return source_context, snapshot_payload, monthly_payload, regime_payload, borrow_payload


def test_real_source_root_produces_hold_due_to_borrow_proxy_gap(tmp_path: Path) -> None:
    source_root = Path(
        r"G:\Tradex\entry_precision_short_bottom_risk_full_recheck_v1\20260517T034734Z-entry-short-bottom-risk-full-recheck-v1"
    )
    assert (source_root / "short_bottom_risk_full_recheck_compare.json").exists()

    runtime_status = {
        "confirmed": True,
        "selected_runtime_db_path": r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb",
        "resolution_source": "LOCALAPPDATA_DEV",
        "resolution_reason": "freshest_local_snapshot",
        "validated": True,
        "db_exists": True,
        "latest_available_global_date": 20260515,
        "latest_available_global_date_iso": "2026-05-15",
        "freshness_state": "fresh",
        "freshness_days": 2,
    }
    freshness = {
        "confirmed": True,
        "freshness_state": "fresh",
        "freshness_days": 3,
        "stale": False,
        "current_candidate_available": True,
        "snapshot_as_of": "2026-05-14",
    }
    original_runtime = mod.get_runtime_stock_db_status
    original_rankings = mod.get_rankings_freshness
    mod.get_runtime_stock_db_status = lambda: dict(runtime_status)
    mod.get_rankings_freshness = lambda **_kwargs: dict(freshness)
    try:
        result = mod.run(source_root=source_root, output_root=tmp_path / "out")
    finally:
        mod.get_runtime_stock_db_status = original_runtime
        mod.get_rankings_freshness = original_rankings
    out_root = Path(result["output_dir"])

    assert result["decision"] == "hold_due_to_borrow_proxy_gap"
    assert result["snapshot_count"] >= 2
    assert result["outside_source_snapshot_count"] >= 1
    assert result["monthly_not_worse_than_source"] is True
    assert result["hard_borrow_gap_event_share"] < 0.1
    assert result["borrow_soft_cost_event_share"] > 0.6
    assert result["no_lookahead_pass"] is True

    for name in [
        "short_bottom_risk_stability_replay_contract.json",
        "short_bottom_risk_snapshot_stability.json",
        "short_bottom_risk_monthly_stability_replay.json",
        "short_bottom_risk_regime_stability.json",
        "short_bottom_risk_borrow_proxy_report.json",
        "short_bottom_risk_stability_replay_decision.json",
        "no_lookahead_audit.json",
        "_ARTIFACT_COMPLETE.json",
    ]:
        assert (out_root / name).exists()

    snapshot = json.loads((out_root / "short_bottom_risk_snapshot_stability.json").read_text(encoding="utf-8"))
    monthly = json.loads((out_root / "short_bottom_risk_monthly_stability_replay.json").read_text(encoding="utf-8"))
    regime = json.loads((out_root / "short_bottom_risk_regime_stability.json").read_text(encoding="utf-8"))
    borrow = json.loads((out_root / "short_bottom_risk_borrow_proxy_report.json").read_text(encoding="utf-8"))
    decision = json.loads((out_root / "short_bottom_risk_stability_replay_decision.json").read_text(encoding="utf-8"))
    audit = json.loads((out_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((out_root / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert snapshot["rollup"]["edge_survives_outside_source_snapshot"] is True
    assert snapshot["rollup"]["no_contrary_snapshot"] is True
    assert monthly["comparison_to_source"]["not_worse_than_source"] is True
    assert regime["summary"]["broad_down_edge_positive"] is False
    assert regime["summary"]["upward_or_non_short_favorable_edge_positive"] is True
    assert borrow["summary"]["soft_borrow_cost_blocked"] is True
    assert decision["decision"] == "hold_due_to_borrow_proxy_gap"
    assert audit["no_lookahead_pass"] is True
    assert complete["complete"] is True


@pytest.mark.parametrize(
    ("case_name", "kwargs", "expected"),
    [
        (
            "keep",
            {
                "snapshot_rollup": {
                    "available_snapshot_count": 3,
                    "outside_source_snapshot_count": 2,
                    "outside_source_positive_count": 2,
                    "outside_source_contrary_count": 0,
                    "edge_survives_outside_source_snapshot": True,
                    "no_contrary_snapshot": True,
                    "single_snapshot_only": False,
                },
                "monthly_rollup": {
                    "completed_bucket_count": 11,
                    "months_with_both_sides": 7,
                    "months_with_challenger_absent": 4,
                },
                "monthly_comparison": {"not_worse_than_source": True},
                "borrow_summary": {
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_borrow_cost_event_share": 0.2,
                    "soft_borrow_cost_code_count": 2,
                    "code_count": 15,
                },
            },
            "keep_for_shadow_paper_replay",
        ),
        (
            "single_snapshot",
            {
                "snapshot_rollup": {
                    "available_snapshot_count": 1,
                    "outside_source_snapshot_count": 0,
                    "outside_source_positive_count": 0,
                    "outside_source_contrary_count": 0,
                    "edge_survives_outside_source_snapshot": False,
                    "no_contrary_snapshot": True,
                    "single_snapshot_only": True,
                },
                "monthly_rollup": {
                    "completed_bucket_count": 11,
                    "months_with_both_sides": 7,
                    "months_with_challenger_absent": 4,
                },
                "monthly_comparison": {"not_worse_than_source": True},
                "borrow_summary": {
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_borrow_cost_event_share": 0.2,
                    "soft_borrow_cost_code_count": 2,
                    "code_count": 15,
                },
            },
            "hold_due_to_single_snapshot_only",
        ),
        (
            "borrow_proxy",
            {
                "snapshot_rollup": {
                    "available_snapshot_count": 3,
                    "outside_source_snapshot_count": 2,
                    "outside_source_positive_count": 2,
                    "outside_source_contrary_count": 0,
                    "edge_survives_outside_source_snapshot": True,
                    "no_contrary_snapshot": True,
                    "single_snapshot_only": False,
                },
                "monthly_rollup": {
                    "completed_bucket_count": 11,
                    "months_with_both_sides": 7,
                    "months_with_challenger_absent": 4,
                },
                "monthly_comparison": {"not_worse_than_source": True},
                "borrow_summary": {
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_borrow_cost_event_share": 0.8666666667,
                    "soft_borrow_cost_code_count": 13,
                    "code_count": 15,
                },
            },
            "hold_due_to_borrow_proxy_gap",
        ),
        (
            "snapshot_specific",
            {
                "snapshot_rollup": {
                    "available_snapshot_count": 3,
                    "outside_source_snapshot_count": 2,
                    "outside_source_positive_count": 1,
                    "outside_source_contrary_count": 1,
                    "edge_survives_outside_source_snapshot": False,
                    "no_contrary_snapshot": False,
                    "single_snapshot_only": False,
                },
                "monthly_rollup": {
                    "completed_bucket_count": 11,
                    "months_with_both_sides": 7,
                    "months_with_challenger_absent": 4,
                },
                "monthly_comparison": {"not_worse_than_source": True},
                "borrow_summary": {
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_borrow_cost_event_share": 0.2,
                    "soft_borrow_cost_code_count": 2,
                    "code_count": 15,
                },
            },
            "drop_as_snapshot_specific",
        ),
        (
            "monthly_unstable",
            {
                "snapshot_rollup": {
                    "available_snapshot_count": 3,
                    "outside_source_snapshot_count": 2,
                    "outside_source_positive_count": 2,
                    "outside_source_contrary_count": 0,
                    "edge_survives_outside_source_snapshot": True,
                    "no_contrary_snapshot": True,
                    "single_snapshot_only": False,
                },
                "monthly_rollup": {
                    "completed_bucket_count": 11,
                    "months_with_both_sides": 7,
                    "months_with_challenger_absent": 4,
                },
                "monthly_comparison": {"not_worse_than_source": False},
                "borrow_summary": {
                    "hard_borrow_gap_event_share": 0.0,
                    "hard_borrow_gap_code_count": 0,
                    "soft_borrow_cost_event_share": 0.2,
                    "soft_borrow_cost_code_count": 2,
                    "code_count": 15,
                },
            },
            "drop_as_unstable_by_month",
        ),
        (
            "borrow_untradable",
            {
                "snapshot_rollup": {
                    "available_snapshot_count": 3,
                    "outside_source_snapshot_count": 2,
                    "outside_source_positive_count": 2,
                    "outside_source_contrary_count": 0,
                    "edge_survives_outside_source_snapshot": True,
                    "no_contrary_snapshot": True,
                    "single_snapshot_only": False,
                },
                "monthly_rollup": {
                    "completed_bucket_count": 11,
                    "months_with_both_sides": 7,
                    "months_with_challenger_absent": 4,
                },
                "monthly_comparison": {"not_worse_than_source": True},
                "borrow_summary": {
                    "hard_borrow_gap_event_share": 0.2,
                    "hard_borrow_gap_code_count": 4,
                    "soft_borrow_cost_event_share": 0.2,
                    "soft_borrow_cost_code_count": 2,
                    "code_count": 15,
                },
            },
            "drop_due_to_borrow_untradable",
        ),
    ],
)
def test_decision_helper_covers_all_labels(case_name: str, kwargs: dict[str, object], expected: str) -> None:
    source_context, snapshot_payload, monthly_payload, regime_payload, borrow_payload = _build_decision_inputs(
        regime_summary={
            "positive_bucket_count": 2,
            "negative_bucket_count": 1,
            "broad_down_edge_positive": True,
            "flat_or_mixed_edge_positive": True,
            "upward_or_non_short_favorable_edge_positive": False,
            "edge_is_broad_down_only": False,
        },
        **kwargs,
    )
    decision = mod._build_decision(
        session_id=f"synthetic-{case_name}",
        source_context=source_context,
        snapshot_payload=snapshot_payload,
        monthly_payload=monthly_payload,
        regime_payload=regime_payload,
        borrow_payload=borrow_payload,
    )
    assert decision["decision"] == expected
