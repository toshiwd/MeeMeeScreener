from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import tradex_pre_strength_guard_validation_v1 as mod


def _row(idx: int, *, safe: bool) -> dict[str, object]:
    year = 2020 + (idx % 6)
    month = (idx % 12) + 1
    day = (idx % 20) + 1
    if safe:
        ret = 0.025 if idx % 10 < 6 else -0.005
        return {
            "code": f"80{idx % 80:02d}",
            "event_date": f"{year}-{month:02d}-{day:02d}",
            "event_month": f"{year}-{month:02d}",
            "pre_ret20_state": "pre20_flat",
            "pre_ret5_state": "pre5_flat",
            "pre_ma20_path_state": "pre_ma20_reclaim_base",
            "pre_ma60_context_state": "pre_ma60_near_or_above",
            "pre_candle_energy_state": "pre_candle_energy_warning",
            "pre_wick_warning_state": "pre_wicks_clean",
            "pre_volume_state": "pre_volume_normal",
            "pre_compression_state": "pre_range_normal",
            "weekly_prior_state": "weekly_prior_mixed",
            "monthly_prior_state": "monthly_prior_uptrend",
            "event_daily_ret20_state": "daily20_up",
            "event_daily_candle_state": "daily_strong_bull",
            "event_strength_score": 4,
            "ret20_fwd": ret,
            "mfe20": 0.06,
            "mae20": -0.04,
            "win20": ret > 0.0,
            "severe_loss20": False,
        }
    ret = -0.08 if idx % 3 == 0 else 0.012
    return {
        "code": f"90{idx % 80:02d}",
        "event_date": f"{year}-{month:02d}-{day:02d}",
        "event_month": f"{year}-{month:02d}",
        "pre_ret20_state": "pre20_strong_up",
        "pre_ret5_state": "pre5_strong_up",
        "pre_ma20_path_state": "pre_ma20_already_extended",
        "pre_ma60_context_state": "pre_ma60_extended_above",
        "pre_candle_energy_state": "pre_candle_energy_mixed",
        "pre_wick_warning_state": "pre_upper_wick_or_failed_push",
        "pre_volume_state": "pre_volume_expansion",
        "pre_compression_state": "pre_range_wide",
        "weekly_prior_state": "weekly_prior_strong_up",
        "monthly_prior_state": "monthly_prior_strong_up",
        "event_daily_ret20_state": "daily20_strong_up",
        "event_daily_candle_state": "daily_strong_bull",
        "event_strength_score": 7,
        "ret20_fwd": ret,
        "mfe20": 0.12,
        "mae20": -0.09,
        "win20": ret > 0.0,
        "severe_loss20": ret < -0.05,
    }


def _events() -> pd.DataFrame:
    return pd.DataFrame([_row(idx, safe=True) for idx in range(300)] + [_row(idx, safe=False) for idx in range(300)])


def _write_source_run(root: Path) -> Path:
    source = root / "source"
    source.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "_ARTIFACT_COMPLETE.json": {
            "complete": True,
            "silent_fallback_used": False,
            "authoritative_research_decision": "promising_pre_strength_patterns_found",
        },
        "evaluation_contract.json": {
            "axis_id": "pre_strength_pattern_mining_v1",
            "contract_hash": "source-contract",
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


def test_guard_flags_keep_future_labels_out_of_keys() -> None:
    events = mod.add_guard_flags(mod._normalize_event_frame(_events()))

    assert mod.GUARD_KEY_COLUMNS.isdisjoint(mod.LABEL_COLUMNS)
    assert int(events["guard_safe_full"].sum()) == 300
    assert int(events["negative_guard_match"].sum()) == 300
    assert not bool(events.loc[events["guard_safe_full"], "negative_guard_match"].any())


def test_primary_guard_metrics_improve_against_all_strength() -> None:
    events = mod.add_guard_flags(mod._normalize_event_frame(_events()))
    baseline = events.copy()
    metrics = mod.calculate_metrics(
        events.loc[events["guard_safe_full"]],
        baseline=baseline,
        total_event_days=events["event_date"].nunique(),
        guard_id="safe_full",
    )
    baseline_metrics = mod.calculate_metrics(
        baseline,
        baseline=baseline,
        total_event_days=events["event_date"].nunique(),
        guard_id="all_strength_baseline",
    )

    assert metrics["n"] == 300
    assert metrics["win_rate20"] >= 0.55
    assert metrics["avg_ret20"] > baseline_metrics["avg_ret20"]
    assert metrics["avg_MAE20"] >= -0.05
    assert metrics["severe_loss_rate20"] <= 0.10


def test_pre_strength_guard_run_writes_required_artifacts(tmp_path: Path) -> None:
    source = _write_source_run(tmp_path)
    result = mod.run_pre_strength_guard_validation_v1(
        source_run_dir=source,
        output_root=tmp_path / "out",
        run_id="guard-smoke",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    topk = json.loads((output_dir / "topk_rotation_proxy_metrics.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["candidate_scoring_created"] is False
    assert complete["publish_bundle_created"] is False
    assert decision["decision"] == "hold"
    assert decision["authoritative_research_decision"] == "pre_strength_guard_hold"
    assert decision["future_labels_used_in_pattern_key"] is False
    assert topk["topk_rotation_proxy_available"] is False
