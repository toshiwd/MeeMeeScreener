from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_negative_guard_missing_feature_extraction_audit_v1 as mod


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
    for idx in range(48):
        year = 2020 + (idx % 4)
        rows.append(_row(idx, set_kind="continuation_winner", year=year, date_idx=idx))
        rows.append(_row(idx + 1000, set_kind="blowoff_loser", year=year, date_idx=idx))
        rows.append(_row(idx + 2000, set_kind="safe", year=year, date_idx=idx))
        rows.append(_row(idx + 3000, set_kind="weak", year=year, date_idx=idx))
    return pd.DataFrame(rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _build_source_db(path: Path, events: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    bar_rows = []
    for event in events.itertuples(index=False):
        event_date = pd.Timestamp(str(event.event_date))
        prefix = str(event.code)[0]
        for offset in range(90, -1, -1):
            day = event_date - pd.Timedelta(days=offset)
            idx = 90 - offset
            base = 100.0 + idx * (0.35 if prefix == "c" else 0.75 if prefix == "b" else 0.12)
            if prefix == "b" and offset == 0:
                o, h, l, c, v = base * 1.02, base * 1.24, base * 0.98, base * 1.04, 900_000
            elif prefix == "c" and offset == 0:
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
    for directory in [pattern, guard, upside, wide, risk, threshold, feature]:
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
        (upside, {"complete": True, "silent_fallback_used": False}, {"authoritative_research_decision": "upside_capture_failed", "silent_fallback_used": False}, {}),
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
    _write_json(upside / "ranking_coverage_audit.json", {"complete_champion_ranking_available": False})
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
    _write_json(
        feature / "candidate_feature_shortlist.json",
        {"rows": [{"feature_id": "same_date_score_rank"}, {"feature_id": "score_extended_continuation_vs_blowoff_risk_v1"}]},
    )
    _write_json(feature / "negative_guard_decomposition_report.json", {"decomposition_available": False, "continuation_winner_indicator_candidates": [], "blowoff_loser_indicator_candidates": []})


def test_extracted_feature_columns_do_not_include_future_labels() -> None:
    assert mod.OHLCV_FEATURE_COLUMNS.isdisjoint(mod.FUTURE_LABEL_COLUMNS)
    assert mod.EVALUATION_ONLY_REPORT_COLUMNS.isdisjoint(mod.OHLCV_FEATURE_COLUMNS)


def test_ohlcv_feature_frame_creates_current_and_past_only_features(tmp_path: Path) -> None:
    events = _events().head(4)
    db = tmp_path / "stocks.duckdb"
    _build_source_db(db, events)
    daily = mod._load_daily_rows(db, codes=events["code"].astype(str).tolist(), start_ymd=20190101, end_ymd=20251231)
    features = mod.build_ohlcv_feature_frame(daily)

    assert "close_vs_ma20_atr" in features.columns
    assert "upper_wick_ratio_event_day" in features.columns
    assert features["close_vs_ma20_atr"].notna().any()
    assert features["upper_wick_ratio_event_day"].notna().any()


def test_negative_guard_missing_feature_extraction_run_writes_required_artifacts(tmp_path: Path) -> None:
    _write_sources(tmp_path)
    result = mod.run_negative_guard_missing_feature_extraction_audit_v1(
        source_pattern_run_id="pattern-run",
        source_guard_run_id="guard-run",
        source_upside_run_id="upside-run",
        source_wide_run_id="wide-run",
        source_risk_run_id="risk-run",
        source_threshold_run_id="threshold-run",
        source_feature_diagnosis_run_id="feature-run",
        pattern_root=tmp_path / "pattern",
        guard_root=tmp_path / "guard",
        upside_root=tmp_path / "upside",
        wide_root=tmp_path / "wide",
        risk_root=tmp_path / "risk",
        threshold_root=tmp_path / "threshold",
        feature_diagnosis_root=tmp_path / "feature",
        output_root=tmp_path / "out",
        run_id="negative-guard-smoke",
    )
    output_dir = Path(result["output_dir"])

    for artifact in mod.REQUIRED_ARTIFACTS:
        assert (output_dir / artifact).exists(), artifact

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "research_decision.json").read_text(encoding="utf-8"))
    leakage = json.loads((output_dir / "leakage_audit.json").read_text(encoding="utf-8"))
    shortlist = json.loads((output_dir / "candidate_feature_shortlist_v2.json").read_text(encoding="utf-8"))
    availability = json.loads((output_dir / "feature_availability_audit.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert complete["silent_fallback_used"] is False
    assert complete["research_fallback_used"] is False
    assert decision["feature_extraction_created"] is True
    assert decision["candidate_feature_shortlist_v2_created"] is True
    assert decision["candidate_scoring_created"] is False
    assert decision["threshold_policy_created"] is False
    assert decision["production_ranking_changed"] is False
    assert decision["publish_bundle_created"] is False
    assert decision["meemee_reflectable"] is False
    assert decision["future_labels_used_in_feature_inputs"] is False
    assert decision["future_labels_used_in_score_inputs"] is False
    assert leakage["future_label_used_in_feature_inputs"] is False
    assert leakage["future_label_used_in_score_inputs"] is False
    assert shortlist["previous_shortlist_feature_count"] == 2
    assert "recommended_features" in shortlist
    assert availability["silently_imputed_feature_count"] == 0
    assert result["candidate_scoring_created"] is False
