from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_upside_capture_missed_winner_diagnosis_v1 as mod


def _row(idx: int, *, set_kind: str) -> dict[str, object]:
    event_date = f"2024-01-{(idx % 10) + 1:02d}"
    base = {
        "code": f"{set_kind[:1]}{idx:04d}",
        "event_date": event_date,
        "event_month": "2024-01",
        "pre_ma60_context_state": "pre_ma60_near_or_above",
        "pre_candle_energy_state": "pre_candle_energy_warning",
        "pre_wick_warning_state": "pre_wicks_clean",
        "pre_volume_state": "pre_volume_normal",
        "pre_compression_state": "pre_range_normal",
        "event_daily_ret20_state": "daily20_up",
        "event_daily_candle_state": "daily_strong_bull",
        "event_strength_score": 4,
        "mae20": -0.04,
        "win20": True,
        "severe_loss20": False,
    }
    if set_kind == "safe":
        base.update(
            {
                "pre_ret20_state": "pre20_flat",
                "pre_ret5_state": "pre5_flat",
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "weekly_prior_state": "weekly_prior_mixed",
                "monthly_prior_state": "monthly_prior_uptrend",
                "ret20_fwd": 0.04,
                "mfe20": 0.08,
            }
        )
    elif set_kind == "winner":
        base.update(
            {
                "pre_ret20_state": "pre20_up",
                "pre_ret5_state": "pre5_up",
                "pre_ma20_path_state": "pre_ma20_reclaim_base",
                "weekly_prior_state": "weekly_prior_uptrend",
                "monthly_prior_state": "monthly_prior_uptrend",
                "ret20_fwd": 0.15,
                "mfe20": 0.18,
            }
        )
    else:
        base.update(
            {
                "pre_ret20_state": "pre20_strong_up",
                "pre_ret5_state": "pre5_strong_up",
                "pre_ma20_path_state": "pre_ma20_already_extended",
                "pre_ma60_context_state": "pre_ma60_extended_above",
                "weekly_prior_state": "weekly_prior_strong_up",
                "monthly_prior_state": "monthly_prior_strong_up",
                "ret20_fwd": -0.06,
                "mfe20": 0.04,
                "win20": False,
                "severe_loss20": True,
            }
        )
    return base


def _events() -> pd.DataFrame:
    rows = []
    for idx in range(30):
        rows.append(_row(idx, set_kind="safe"))
        rows.append(_row(idx + 100, set_kind="winner"))
        rows.append(_row(idx + 200, set_kind="risk"))
    return pd.DataFrame(rows)


def _write_pattern_source(root: Path) -> Path:
    source = root / "pattern" / "pattern-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {
            "complete": True,
            "silent_fallback_used": False,
            "authoritative_research_decision": "promising_pre_strength_patterns_found",
        },
        "evaluation_contract.json": {
            "axis_id": "pre_strength_pattern_mining_v1",
            "contract_hash": "pattern-contract",
            "source_db": "",
        },
        "run_manifest.json": {"schema_version": "tradex_research_run_manifest_v1"},
        "feature_availability_audit.json": {
            "used_future_labels_in_pattern_keys": False,
            "silent_fallback_used": False,
        },
        "research_decision.json": {
            "authoritative_research_decision": "promising_pre_strength_patterns_found",
            "silent_fallback_used": False,
        },
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    (source / "pre_strength_event_ledger.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in _events().to_dict(orient="records")),
        encoding="utf-8",
    )
    return source


def _write_guard_source(root: Path) -> Path:
    source = root / "guard" / "guard-run"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {
            "complete": True,
            "silent_fallback_used": False,
            "authoritative_research_decision": "pre_strength_guard_hold",
        },
        "evaluation_contract.json": {"axis_id": "pre_strength_guard_validation_v1", "contract_hash": "guard-contract"},
        "source_artifact_refs.json": {"refs": []},
        "positive_guard_report.json": {"primary_positive_guard_id": "safe_full"},
        "negative_guard_report.json": {"primary_negative_guard_id": "already_extended_strong_up_blowoff_veto"},
        "topk_rotation_proxy_metrics.json": {"topk_rotation_proxy_available": False},
        "research_decision.json": {
            "authoritative_research_decision": "pre_strength_guard_hold",
            "silent_fallback_used": False,
        },
    }
    for name, payload in artifacts.items():
        (source / name).write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    return source


def test_opportunity_labels_are_evaluation_only_and_ranked_by_date() -> None:
    events = mod.guard_mod.add_guard_flags(mod.guard_mod._normalize_event_frame(_events()))
    events["candidate_all_strength_baseline"] = True
    labeled = mod.add_opportunity_labels(events)

    assert mod.GUARD_KEY_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)
    assert labeled["is_future_top10_by_ret20"].any()
    assert labeled["is_big_winner_ret20_ge_10pct"].sum() == 30
    assert labeled.groupby("event_date")["opportunity_day_big_ret20"].all().all()


def test_capture_report_shows_safe_full_misses_big_winners() -> None:
    events = mod.guard_mod.add_guard_flags(mod.guard_mod._normalize_event_frame(_events()))
    events["candidate_all_strength_baseline"] = True
    labeled = mod.add_opportunity_labels(events)
    report = mod.build_candidate_set_capture_report(labeled)
    row = {item["candidate_set_id"]: item for item in report["rows"]}["safe_full"]

    assert row["n"] == 30
    assert row["future_top10_recall_by_candidate_set"] == 0.0
    assert row["big_winner_ret20_ge_10_capture_rate"] == 0.0
    assert row["missed_big_winner_ret20_ge_10_count"] == 30


def test_upside_capture_run_writes_required_artifacts(tmp_path: Path) -> None:
    _write_pattern_source(tmp_path)
    _write_guard_source(tmp_path)
    result = mod.run_upside_capture_missed_winner_diagnosis_v1(
        source_pattern_run_id="pattern-run",
        source_guard_run_id="guard-run",
        pattern_root=tmp_path / "pattern",
        guard_root=tmp_path / "guard",
        output_root=tmp_path / "out",
        run_id="upside-smoke",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    ranking = json.loads((output_dir / "ranking_coverage_audit.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["authoritative_research_decision"] == "upside_capture_failed"
    assert decision["future_labels_used_in_guard_key"] is False
    assert ranking["complete_topk_ranking_available"] is False
