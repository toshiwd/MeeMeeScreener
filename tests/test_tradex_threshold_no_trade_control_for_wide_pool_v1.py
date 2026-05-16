from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_threshold_no_trade_control_for_wide_pool_v1 as mod


def _row(idx: int, *, set_kind: str, year: int, date_idx: int) -> dict[str, object]:
    event_date = f"{year}-{((date_idx % 12) + 1):02d}-{((date_idx % 20) + 1):02d}"
    base = {
        "code": f"{set_kind[:1]}{idx:04d}",
        "event_date": event_date,
        "event_month": event_date[:7],
        "pre_candle_energy_state": "pre_candle_energy_mixed",
        "pre_wick_warning_state": "pre_wicks_clean",
        "pre_volume_state": "pre_volume_normal",
        "pre_compression_state": "pre_range_normal",
        "event_daily_candle_state": "daily_strong_bull",
        "win20": True,
        "severe_loss20": False,
    }
    if set_kind == "continuation_winner":
        base.update(
            {
                "pre_ret20_state": "pre20_strong_up",
                "pre_ret5_state": "pre5_strong_up",
                "pre_ma20_path_state": "pre_ma20_already_extended",
                "pre_ma60_context_state": "pre_ma60_extended_above",
                "pre_volume_state": "pre_volume_expansion",
                "weekly_prior_state": "weekly_prior_strong_up",
                "monthly_prior_state": "monthly_prior_uptrend",
                "event_daily_ret20_state": "daily20_strong_up",
                "event_strength_score": 9,
                "ret20_fwd": 0.16,
                "mfe20": 0.22,
                "mae20": -0.045,
            }
        )
    elif set_kind == "blowoff_loser":
        base.update(
            {
                "pre_ret20_state": "pre20_strong_up",
                "pre_ret5_state": "pre5_strong_up",
                "pre_ma20_path_state": "pre_ma20_already_extended",
                "pre_ma60_context_state": "pre_ma60_extended_above",
                "pre_wick_warning_state": "pre_upper_wick_or_failed_push",
                "pre_volume_state": "pre_volume_expansion",
                "pre_compression_state": "pre_range_wide",
                "weekly_prior_state": "weekly_prior_strong_up",
                "monthly_prior_state": "monthly_prior_strong_up",
                "event_daily_ret20_state": "daily20_strong_up",
                "event_strength_score": 8,
                "ret20_fwd": -0.09,
                "mfe20": 0.05,
                "mae20": -0.12,
                "win20": False,
                "severe_loss20": True,
            }
        )
    elif set_kind == "safe":
        base.update(
            {
                "pre_ret20_state": "pre20_flat",
                "pre_ret5_state": "pre5_flat",
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "pre_ma60_context_state": "pre_ma60_near_or_above",
                "weekly_prior_state": "weekly_prior_mixed",
                "monthly_prior_state": "monthly_prior_uptrend",
                "event_daily_ret20_state": "daily20_up",
                "event_strength_score": 4,
                "ret20_fwd": 0.035,
                "mfe20": 0.07,
                "mae20": -0.035,
            }
        )
    else:
        base.update(
            {
                "pre_ret20_state": "pre20_down",
                "pre_ret5_state": "pre5_down",
                "pre_ma20_path_state": "pre_ma20_below_base",
                "pre_ma60_context_state": "pre_ma60_below",
                "weekly_prior_state": "weekly_prior_downtrend",
                "monthly_prior_state": "monthly_prior_mixed",
                "event_daily_ret20_state": "daily20_down",
                "event_strength_score": 1,
                "ret20_fwd": -0.03,
                "mfe20": 0.03,
                "mae20": -0.06,
                "win20": False,
            }
        )
    return base


def _events() -> pd.DataFrame:
    rows = []
    for idx in range(180):
        year = 2020 + (idx % 6)
        rows.append(_row(idx, set_kind="continuation_winner", year=year, date_idx=idx))
        rows.append(_row(idx + 1000, set_kind="blowoff_loser", year=year, date_idx=idx))
        rows.append(_row(idx + 2000, set_kind="safe", year=year, date_idx=idx))
        rows.append(_row(idx + 3000, set_kind="weak", year=year, date_idx=idx))
        rows.append(_row(idx + 4000, set_kind="weak", year=year, date_idx=idx))
    return pd.DataFrame(rows)


def _write_pattern_source(root: Path) -> None:
    source = root / "pattern" / "pattern-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "silent_fallback_used": False, "authoritative_research_decision": "promising_pre_strength_patterns_found"},
        "evaluation_contract.json": {"axis_id": "pre_strength_pattern_mining_v1", "contract_hash": "pattern-contract", "source_db": ""},
        "run_manifest.json": {"schema_version": "tradex_research_run_manifest_v1"},
        "feature_availability_audit.json": {"used_future_labels_in_pattern_keys": False, "silent_fallback_used": False},
        "research_decision.json": {"authoritative_research_decision": "promising_pre_strength_patterns_found", "silent_fallback_used": False},
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    (source / "pre_strength_event_ledger.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in _events().to_dict(orient="records")),
        encoding="utf-8",
    )


def _write_guard_source(root: Path) -> None:
    source = root / "guard" / "guard-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "silent_fallback_used": False, "authoritative_research_decision": "pre_strength_guard_hold"},
        "evaluation_contract.json": {"axis_id": "pre_strength_guard_validation_v1", "contract_hash": "guard-contract"},
        "source_artifact_refs.json": {"refs": []},
        "positive_guard_report.json": {"primary_positive_guard_id": "safe_full"},
        "negative_guard_report.json": {"primary_negative_guard_id": "already_extended_strong_up_blowoff_veto"},
        "topk_rotation_proxy_metrics.json": {"topk_rotation_proxy_available": False},
        "research_decision.json": {"authoritative_research_decision": "pre_strength_guard_hold", "silent_fallback_used": False},
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_upside_source(root: Path) -> None:
    source = root / "upside" / "upside-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "silent_fallback_used": False},
        "research_decision.json": {"authoritative_research_decision": "upside_capture_failed", "silent_fallback_used": False},
        "ranking_coverage_audit.json": {"complete_champion_ranking_available": False},
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_wide_source(root: Path) -> None:
    source = root / "wide" / "wide-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "decision": "hold", "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold", "silent_fallback_used": False},
        "research_decision.json": {"decision": "hold", "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold", "silent_fallback_used": False},
        "score_leaderboard.json": {"rows": []},
        "top3_selection_report.json": {"rows": []},
        "ranking_coverage_audit.json": {"complete_champion_ranking_available": False},
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_risk_source(root: Path) -> None:
    source = root / "risk" / "risk-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {
            "complete": True,
            "decision": "drop",
            "authoritative_research_decision": "selection_risk_control_drop",
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "research_decision.json": {
            "decision": "drop",
            "authoritative_research_decision": "selection_risk_control_drop",
            "best_risk_family_id": "extended_continuation_vs_blowoff_risk_v1",
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
        "risk_leaderboard.json": {"rows": []},
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _scored_events() -> pd.DataFrame:
    events = mod.risk_mod.wide_mod.upside_mod.guard_mod.add_guard_flags(mod.risk_mod.wide_mod.upside_mod.guard_mod._normalize_event_frame(_events()))
    events["candidate_all_strength_baseline"] = True
    labeled = mod.risk_mod.wide_mod.upside_mod.add_opportunity_labels(events)
    wide_scored = mod.risk_mod.wide_mod.add_research_scores(labeled)
    risk_scored = mod.risk_mod.add_risk_control_scores(wide_scored)
    return mod.add_threshold_inputs(risk_scored)


def test_threshold_inputs_do_not_include_future_labels() -> None:
    events = _scored_events()

    assert mod.THRESHOLD_INPUT_COLUMNS.isdisjoint(mod.FUTURE_LABEL_COLUMNS)
    assert events["threshold_risk_value"].notna().all()
    assert events["same_date_score_rank"].notna().all()


def test_threshold_policy_allows_no_trade_and_variable_positions() -> None:
    events = _scored_events()
    selected, scored, calibration_rows = mod.build_threshold_selection(events)
    date_ledger = mod.build_date_level_ledger(scored, selected)
    reports = mod.build_reports(scored, selected, date_ledger)
    threshold_row = reports["by_id"]["top1_confident_else_no_trade_v1"]
    baseline = reports["by_id"]["baseline_always_select_top3_previous_best"]

    assert calibration_rows
    assert threshold_row["no_trade_days_rate"] > 0.0
    assert threshold_row["avg_positions_per_all_days"] < baseline["avg_positions_per_all_days"]
    assert threshold_row["fewer_than_3_day_rate"] > 0.0
    assert threshold_row["selected_days_count"] < baseline["selected_days_count"]


def test_threshold_no_trade_run_writes_required_artifacts(tmp_path: Path) -> None:
    _write_pattern_source(tmp_path)
    _write_guard_source(tmp_path)
    _write_upside_source(tmp_path)
    _write_wide_source(tmp_path)
    _write_risk_source(tmp_path)
    result = mod.run_threshold_no_trade_control_for_wide_pool_v1(
        source_pattern_run_id="pattern-run",
        source_guard_run_id="guard-run",
        source_upside_run_id="upside-run",
        source_wide_run_id="wide-run",
        source_risk_run_id="risk-run",
        pattern_root=tmp_path / "pattern",
        guard_root=tmp_path / "guard",
        upside_root=tmp_path / "upside",
        wide_root=tmp_path / "wide",
        risk_root=tmp_path / "risk",
        output_root=tmp_path / "out",
        run_id="threshold-smoke",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    feature_audit = json.loads((output_dir / "feature_availability_audit.json").read_text(encoding="utf-8"))
    policy = json.loads((output_dir / "threshold_policy_contract.json").read_text(encoding="utf-8"))
    split = json.loads((output_dir / "split_contract.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["research_fallback_used"] is False
    assert complete["candidate_scoring_created"] is False
    assert complete["threshold_policy_created"] is True
    assert decision["uses_existing_research_score"] is True
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is True
    assert decision["production_ranking_changed"] is False
    assert decision["publish_bundle_created"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["safe_full_used_as_hard_filter"] is False
    assert decision["negative_guard_used_as_hard_veto"] is False
    assert decision["no_trade_allowed"] is True
    assert decision["variable_position_count_allowed"] is True
    assert decision["future_labels_used_in_threshold_inputs"] is False
    assert decision["thresholds_calibrated_train_past_only"] is True
    assert feature_audit["future_labels_used_in_threshold_inputs"] is False
    assert policy["threshold_policy_scope"] == "research_only"
    assert split["thresholds_calibrated_train_past_only"] is True
    assert result["candidate_scoring_created"] is False
