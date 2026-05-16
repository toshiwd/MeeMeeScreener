from __future__ import annotations

import io
import json
from pathlib import Path

import duckdb
import pandas as pd
from PIL import Image

from scripts import tradex_image_assisted_rerank_phase0_1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _event_row(idx: int, *, set_kind: str, event_date: str) -> dict[str, object]:
    prefix = {"continuation_winner": "C", "blowoff_loser": "B", "safe": "S", "weak": "W"}[set_kind]
    base: dict[str, object] = {
        "code": f"{prefix}{idx:04d}",
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
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2020-01-01", periods=120, freq="D")
    for idx, date in enumerate(dates):
        event_date = str(date.date())
        rows.append(_event_row(idx, set_kind="continuation_winner", event_date=event_date))
        rows.append(_event_row(idx + 1_000, set_kind="blowoff_loser", event_date=event_date))
        rows.append(_event_row(idx + 2_000, set_kind="safe", event_date=event_date))
        rows.append(_event_row(idx + 3_000, set_kind="weak", event_date=event_date))
    return pd.DataFrame(rows)


def _build_source_db(path: Path, events: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    bar_rows: list[dict[str, object]] = []
    for event in events.itertuples(index=False):
        event_date = pd.Timestamp(str(event.event_date))
        prefix = str(event.code)[0]
        for offset in range(95, -1, -1):
            day = event_date - pd.Timedelta(days=offset)
            idx = 95 - offset
            drift = 0.35 if prefix == "C" else 0.72 if prefix == "B" else 0.16
            base = 100.0 + idx * drift
            if prefix == "B" and offset == 0:
                o, h, l, c, v = base * 1.02, base * 1.24, base * 0.98, base * 1.04, 900_000
            elif prefix == "C" and offset == 0:
                o, h, l, c, v = base * 0.99, base * 1.035, base * 0.985, base * 1.03, 300_000
            else:
                o, h, l, c, v = base * 0.995, base * 1.015, base * 0.99, base, 250_000 + idx * 500
            bar_rows.append({"code": str(event.code), "date": int(day.strftime("%Y%m%d")), "o": o, "h": h, "l": l, "c": c, "v": v, "source": "pan"})
    bars = pd.DataFrame(bar_rows).drop_duplicates(["code", "date"]).sort_values(["code", "date"])
    bars["ma20"] = bars.groupby("code")["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    bars["ma60"] = bars.groupby("code")["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    ma = bars[["code", "date", "ma20", "ma60"]].copy()
    ma["ma7"] = bars.groupby("code")["c"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE daily_bars AS SELECT * FROM bars")
        conn.execute("CREATE TABLE daily_ma AS SELECT code, date, ma7, ma20, ma60 FROM ma")


def _write_sources(root: Path) -> None:
    events = _events()
    source_db = root / "db" / "stocks.duckdb"
    _build_source_db(source_db, events)
    pattern = root / "pattern" / "pattern-run"
    guard = root / "guard" / "guard-run"
    upside = root / "upside" / "upside-run"
    wide = root / "wide" / "wide-run"
    risk = root / "risk" / "risk-run"
    threshold = root / "threshold" / "threshold-run"
    feature = root / "feature" / "feature-run"
    negative = root / "negative" / "negative-run"
    for directory in [pattern, guard, upside, wide, risk, threshold, feature, negative]:
        directory.mkdir(parents=True, exist_ok=True)
    for name, payload in {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "silent_fallback_used": False},
        "evaluation_contract.json": {"axis_id": "pre_strength_pattern_mining_v1", "contract_hash": "pattern-contract", "source_db": str(source_db)},
        "run_manifest.json": {"schema_version": "tradex_research_run_manifest_v1"},
        "feature_availability_audit.json": {"used_future_labels_in_pattern_keys": False, "silent_fallback_used": False},
        "research_decision.json": {"authoritative_research_decision": "promising_pre_strength_patterns_found", "silent_fallback_used": False},
    }.items():
        _write_json(pattern / name, payload)
    (pattern / "pre_strength_event_ledger.jsonl").write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in events.to_dict(orient="records")),
        encoding="utf-8",
    )
    for name, payload in {
        "_ARTIFACT_COMPLETE.json": {"complete": True, "silent_fallback_used": False},
        "evaluation_contract.json": {"axis_id": "pre_strength_guard_validation_v1", "contract_hash": "guard-contract"},
        "source_artifact_refs.json": {"refs": []},
        "positive_guard_report.json": {"primary_positive_guard_id": "safe_full"},
        "negative_guard_report.json": {"primary_negative_guard_id": "already_extended_strong_up_blowoff_veto"},
        "topk_rotation_proxy_metrics.json": {"topk_rotation_proxy_available": False},
        "research_decision.json": {"authoritative_research_decision": "pre_strength_guard_hold", "silent_fallback_used": False},
    }.items():
        _write_json(guard / name, payload)
    for directory, complete_payload, decision_payload, extra in [
        (
            upside,
            {"complete": True, "silent_fallback_used": False},
            {"authoritative_research_decision": "upside_capture_failed", "silent_fallback_used": False},
            {"ranking_coverage_audit.json": {"complete_champion_ranking_available": False}},
        ),
        (
            wide,
            {"complete": True, "decision": "hold", "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold", "silent_fallback_used": False},
            {"decision": "hold", "authoritative_research_decision": "wide_strength_pool_upside_rerank_hold", "silent_fallback_used": False},
            {"score_leaderboard.json": {"rows": []}, "top3_selection_report.json": {"rows": []}, "ranking_coverage_audit.json": {"complete_champion_ranking_available": False}},
        ),
        (
            risk,
            {"complete": True, "decision": "drop", "authoritative_research_decision": "selection_risk_control_drop", "silent_fallback_used": False, "research_fallback_used": False},
            {"decision": "drop", "authoritative_research_decision": "selection_risk_control_drop", "best_risk_family_id": "extended_continuation_vs_blowoff_risk_v1", "silent_fallback_used": False, "research_fallback_used": False},
            {"risk_leaderboard.json": {"rows": []}},
        ),
        (
            threshold,
            {"complete": True, "decision": "drop", "authoritative_research_decision": "threshold_no_trade_control_drop", "silent_fallback_used": False, "research_fallback_used": False},
            {"decision": "drop", "authoritative_research_decision": "threshold_no_trade_control_drop", "silent_fallback_used": False, "research_fallback_used": False},
            {"threshold_leaderboard.json": {"rows": []}},
        ),
    ]:
        _write_json(directory / "_ARTIFACT_COMPLETE.json", complete_payload)
        _write_json(directory / "research_decision.json", decision_payload)
        for filename, payload in extra.items():
            _write_json(directory / filename, payload)
    _write_json(feature / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "hold", "authoritative_research_decision": "winner_nonwinner_feature_diagnosis_hold", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        feature / "research_decision.json",
        {
            "decision": "hold",
            "authoritative_research_decision": "winner_nonwinner_feature_diagnosis_hold",
            "recommended_feature_count": 8,
            "negative_guard_recommended_feature_count": 0,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    )
    _write_json(feature / "candidate_feature_shortlist.json", {"rows": [{"feature_id": f"feature_{idx}"} for idx in range(8)]})
    _write_json(feature / "negative_guard_decomposition_report.json", {"decomposition_available": False, "continuation_winner_indicator_candidates": [], "blowoff_loser_indicator_candidates": []})
    _write_json(negative / "_ARTIFACT_COMPLETE.json", {"complete": True, "decision": "drop", "authoritative_research_decision": "negative_guard_feature_extraction_failed", "silent_fallback_used": False, "research_fallback_used": False})
    _write_json(
        negative / "research_decision.json",
        {
            "decision": "drop",
            "authoritative_research_decision": "negative_guard_feature_extraction_failed",
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    )
    _write_json(negative / "candidate_feature_shortlist_v2.json", {"new_negative_guard_feature_count": 50, "recommended_feature_count": 0, "recommended_features": []})
    _write_json(negative / "previous_shortlist_retest_report.json", {"previous_shortlist_feature_count": 8, "previous_shortlist_recommended_for_v2_count": 0})
    _write_json(negative / "feature_availability_audit.json", {"usable_feature_count": 50, "unavailable_feature_count": 0, "silently_imputed_feature_count": 0})
    _write_json(negative / "leakage_audit.json", {"future_label_used_in_feature_inputs": False, "future_label_used_in_score_inputs": False, "same_period_label_tuning": False})
    _write_json(negative / "next_axis_recommendation.json", {"next_axis": "image_assisted_rerank_phase0_1"})


def test_renderer_is_deterministic_and_label_free() -> None:
    dates = pd.date_range("2020-01-01", periods=80, freq="D")
    window = pd.DataFrame(
        {
            "ymd": [int(date.strftime("%Y%m%d")) for date in dates],
            "o": [100 + idx * 0.2 for idx in range(80)],
            "h": [101 + idx * 0.2 for idx in range(80)],
            "l": [99 + idx * 0.2 for idx in range(80)],
            "c": [100.5 + idx * 0.2 for idx in range(80)],
            "v": [100_000 + idx * 100 for idx in range(80)],
        }
    )
    first = mod.render_candlestick_volume_png(window)
    second = mod.render_candlestick_volume_png(window)

    assert first == second
    image = Image.open(io.BytesIO(first))
    assert image.size == (224, 224)
    assert not mod.FUTURE_LABEL_COLUMNS.intersection({"o", "h", "l", "c", "v"})


def test_time_block_split_uses_date_only_and_embargo() -> None:
    dates = pd.date_range("2020-01-01", periods=120, freq="D")
    manifest_rows = [
        {
            "image_sample_key": f"k{idx:04d}",
            "symbol": f"C{idx:04d}",
            "event_date": str(date.date()),
            "event_ymd": int(date.strftime("%Y%m%d")),
            "source_candidate_set": mod.SOURCE_CANDIDATE_SET,
            "source_score_family_id": mod.SOURCE_SCORE_FAMILY_ID,
            "negative_guard_matched": idx % 2 == 0,
            "safe_full_tag": idx % 3 == 0,
        }
        for idx, date in enumerate(dates)
    ]
    assignments, split_contract, leakage = mod.assign_time_block_split(manifest_rows)

    assert split_contract["split_created"] is True
    assert leakage["split_leakage_audit_passed"] is True
    assert leakage["labels_used_for_split_assignment"] is False
    assert leakage["same_date_cross_split"] is False
    assert {row["split"] for row in assignments}.issuperset({"train", "validation", "test", "embargo"})


def test_image_assisted_phase0_1_run_writes_required_artifacts(tmp_path: Path) -> None:
    _write_sources(tmp_path)
    result = mod.run_image_assisted_rerank_phase0_1(
        source_pattern_run_id="pattern-run",
        source_guard_run_id="guard-run",
        source_upside_run_id="upside-run",
        source_wide_run_id="wide-run",
        source_risk_run_id="risk-run",
        source_threshold_run_id="threshold-run",
        source_feature_diagnosis_run_id="feature-run",
        source_negative_guard_feature_run_id="negative-run",
        pattern_root=tmp_path / "pattern",
        guard_root=tmp_path / "guard",
        upside_root=tmp_path / "upside",
        wide_root=tmp_path / "wide",
        risk_root=tmp_path / "risk",
        threshold_root=tmp_path / "threshold",
        feature_diagnosis_root=tmp_path / "feature",
        negative_guard_feature_root=tmp_path / "negative",
        output_root=tmp_path / "out",
        run_id="image-phase0-smoke",
    )
    output_dir = Path(result["output_dir"])
    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    readiness = json.loads((output_dir / "phase2_readiness_report.json").read_text(encoding="utf-8"))
    renderer = json.loads((output_dir / "renderer_determinism_report.json").read_text(encoding="utf-8"))
    split_audit = json.loads((output_dir / "split_leakage_audit.json").read_text(encoding="utf-8"))
    candidate_contract = json.loads((output_dir / "candidate_pool_contract.json").read_text(encoding="utf-8"))
    manifest_rows = (output_dir / "image_manifest.jsonl").read_text(encoding="utf-8").strip().splitlines()

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert decision["image_renderer_created"] is True
    assert decision["image_dataset_contract_created"] is True
    assert decision["image_label_contract_created"] is True
    assert decision["image_split_contract_created"] is True
    assert decision["image_model_trained"] is False
    assert decision["fusion_reranker_created"] is False
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["publish_bundle_created"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["future_labels_used_in_image_rendering"] is False
    assert decision["future_labels_used_in_candidate_key"] is False
    assert renderer["renderer_deterministic"] is True
    assert split_audit["split_leakage_audit_passed"] is True
    assert readiness["train_sample_count"] > 0
    assert readiness["validation_sample_count"] > 0
    assert readiness["test_sample_count"] > 0
    assert readiness["image_renderable_event_rate"] == 1.0
    assert candidate_contract["safe_full_used_as_hard_filter"] is False
    assert candidate_contract["negative_guard_used_as_hard_veto"] is False
    assert len(manifest_rows) == 480
    assert result["image_model_trained"] is False
