from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_source_specific_timeblock_applicability_audit_v1 as mod


def _frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year, ret_good in [("2020", 0.12), ("2021", -0.04)]:
        for idx in range(1, 11):
            event_date = f"{year}-01-{idx:02d}"
            rows.extend(
                [
                    {
                        "event_date": event_date,
                        "event_ymd": int(event_date.replace("-", "")),
                        "code": f"{year}{idx:02d}A",
                        "ret20_fwd": ret_good,
                        "mfe20": 0.16 if ret_good > 0 else 0.03,
                        "mae20": -0.02 if ret_good > 0 else -0.13,
                        "win20": ret_good > 0,
                        "severe_loss20": ret_good <= 0,
                        "future_winner": ret_good > 0,
                        "source_specific_candidate": True,
                        "baseline_top3": False,
                        "selection_rank": 4,
                        "previous_best_selection_rank": 4,
                        "negative_guard_match": False,
                        "weekly_prior_state": "weekly_prior_uptrend",
                        "monthly_prior_state": "monthly_prior_up",
                        "time_block": year,
                    },
                    {
                        "event_date": event_date,
                        "event_ymd": int(event_date.replace("-", "")),
                        "code": f"{year}{idx:02d}B",
                        "ret20_fwd": -0.02,
                        "mfe20": 0.02,
                        "mae20": -0.04,
                        "win20": False,
                        "severe_loss20": False,
                        "future_winner": False,
                        "source_specific_candidate": False,
                        "baseline_top3": True,
                        "selection_rank": 1,
                        "previous_best_selection_rank": 1,
                        "negative_guard_match": False,
                        "weekly_prior_state": "weekly_prior_mixed",
                        "monthly_prior_state": "monthly_prior_up",
                        "time_block": year,
                    },
                    {
                        "event_date": event_date,
                        "event_ymd": int(event_date.replace("-", "")),
                        "code": f"{year}{idx:02d}C",
                        "ret20_fwd": 0.01,
                        "mfe20": 0.03,
                        "mae20": -0.03,
                        "win20": True,
                        "severe_loss20": False,
                        "future_winner": False,
                        "source_specific_candidate": False,
                        "baseline_top3": True,
                        "selection_rank": 2,
                        "previous_best_selection_rank": 2,
                        "negative_guard_match": False,
                        "weekly_prior_state": "weekly_prior_mixed",
                        "monthly_prior_state": "monthly_prior_up",
                        "time_block": year,
                    },
                    {
                        "event_date": event_date,
                        "event_ymd": int(event_date.replace("-", "")),
                        "code": f"{year}{idx:02d}D",
                        "ret20_fwd": 0.00,
                        "mfe20": 0.01,
                        "mae20": -0.02,
                        "win20": False,
                        "severe_loss20": False,
                        "future_winner": False,
                        "source_specific_candidate": False,
                        "baseline_top3": True,
                        "selection_rank": 3,
                        "previous_best_selection_rank": 3,
                        "negative_guard_match": False,
                        "weekly_prior_state": "weekly_prior_mixed",
                        "monthly_prior_state": "monthly_prior_up",
                        "time_block": year,
                    },
                ]
            )
    return pd.DataFrame(rows)


def _selected(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    primary_rows = []
    base_rows = []
    for event_date, day in frame.groupby("event_date", sort=True):
        source = day[day["source_specific_candidate"]].iloc[0]
        base = day[day["baseline_top3"]].copy()
        primary = pd.concat([base.head(2), source.to_frame().T], ignore_index=True)
        primary_rows.append(primary)
        base_rows.append(base)
    return pd.concat(primary_rows, ignore_index=True), pd.concat(base_rows, ignore_index=True)


def test_post_drop_audit_reports_calendar_overfit_without_proxy() -> None:
    frame = _frame()
    primary, baseline = _selected(frame)
    timeblock = mod.build_source_timeblock_outcome_report(frame, primary, baseline)
    noise = mod.build_source_noise_by_timeblock_report(timeblock)
    recovery = mod.build_source_recovery_by_timeblock_report(timeblock)
    proxy = mod.build_point_in_time_applicability_proxy_report(frame, primary, baseline)
    overfit = mod.build_overfit_risk_report(timeblock, proxy)
    archive = mod.build_source_archive_or_refine_decision(timeblock=timeblock, proxy=proxy, overfit=overfit)

    assert timeblock["positive_top3_delta_time_block_rate"] == 0.5
    assert noise["rows"]
    assert recovery["good_calendar_block_count"] == 1
    assert proxy["point_in_time_proxy_found"] is False
    assert overfit["overfit_risk_high"] is True
    assert archive["classification"] == "source_applicability_hold"


def test_timeblock_applicability_run_writes_required_artifacts(tmp_path: Path, monkeypatch) -> None:
    frame = _frame()
    primary, baseline = _selected(frame)

    def fake_validate_source_validation(_source_validation_dir):
        return {
            "source_validation_dir": tmp_path / "source",
            "_ARTIFACT_COMPLETE.json": {"complete": True},
            "research_decision.json": {"authoritative_research_decision": "source_specific_candidate_generation_drop"},
            "source_generation_contract.json": {"source_family": "test_source"},
            "source_roots": {},
        }

    def fake_load_audit_inputs(_source_status):
        return frame.copy(), primary.copy(), baseline.copy()

    monkeypatch.setattr(mod, "validate_source_validation", fake_validate_source_validation)
    monkeypatch.setattr(mod, "load_audit_inputs", fake_load_audit_inputs)

    result = mod.run_source_specific_timeblock_applicability_audit_v1(
        source_validation_run_id="validation-run",
        source_validation_root=tmp_path / "source",
        output_root=tmp_path / "out",
        run_id="timeblock-audit-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["candidate_generation_challenger_created"] is False
    assert decision["post_drop_diagnostic_only"] is True
    assert decision["candidate_scoring_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["meemee_reflectable"] is False
    assert result["candidate_generation_challenger_created"] is False
