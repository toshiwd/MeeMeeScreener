from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_ma_buy_sell_probe_v1 as mod


def _dates(days: int) -> list[pd.Timestamp]:
    return list(pd.bdate_range("2025-01-02", periods=days))


def _build_source_rows(symbols: list[str], trade_date: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for idx, symbol in enumerate(symbols, start=1):
        ret = 0.02 if idx % 3 == 0 else (-0.04 if idx % 5 == 0 else 0.005)
        rows.append(
            {
                "symbol": symbol,
                "side": "long",
                "trade_date": int(trade_date.strftime("%Y%m%d")),
                "anchor_date": trade_date.strftime("%Y-%m-%d"),
                "month_bucket": trade_date.strftime("%Y-%m"),
                "market_regime_bucket": "test_regime",
                "champion_rank": idx,
                "champion_score": 1.0 - idx * 0.01,
                "forward_ret_20d": ret,
                "path_value_score_v1": ret * 2.0,
                "top15_label": idx % 3 == 0,
                "bottom15_label": idx % 5 == 0,
                "champion_selected_top5": idx <= 5,
                "champion_selected_top10": idx <= 10,
                "champion_selected_top20": True,
            }
        )
    return pd.DataFrame(rows)


def _build_bars(symbols: list[str], days: int = 230) -> pd.DataFrame:
    rows = []
    for symbol_index, symbol in enumerate(symbols):
        for idx, ts in enumerate(_dates(days)):
            drift = idx * (0.35 + symbol_index * 0.01)
            close = 50.0 + drift + symbol_index
            if symbol_index % 5 == 0 and idx > days - 10:
                close -= (idx - (days - 10)) * 2.0
            rows.append(
                {
                    "code": symbol,
                    "date": int(ts.strftime("%Y%m%d")),
                    "o": close - 0.2,
                    "h": close + 0.8,
                    "l": close - 0.8,
                    "c": close,
                    "v": 1000 + idx,
                    "source": "pan",
                }
            )
    return pd.DataFrame(rows)


def _write_db(path: Path, bars: pd.DataFrame) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE daily_bars (
                code TEXT,
                date INTEGER,
                o DOUBLE,
                h DOUBLE,
                l DOUBLE,
                c DOUBLE,
                v BIGINT,
                source TEXT
            )
            """
        )
        conn.register("bars_df", bars)
        conn.execute("INSERT INTO daily_bars SELECT * FROM bars_df")
    finally:
        conn.close()


def _write_champion_compare(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "champion_compare_test_v1",
        "artifact_detail_level": "authoritative_full",
        "fallback_status": "authoritative",
        "same_condition_contract": {
            "schema_version": "tradex_research_contract_v1",
            "ret20_source_mode": "forward_ret_20d",
            "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
            "same_top_k": [5, 10, 20],
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_regime_db(path: Path, dates: list[pd.Timestamp] | None = None) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("DROP TABLE IF EXISTS market_regime_daily")
        conn.execute("CREATE TABLE market_regime_daily (dt DATE, regime_id TEXT)")
        if dates:
            frame = pd.DataFrame(
                {
                    "dt": [ts.strftime("%Y-%m-%d") for ts in dates],
                    "regime_id": ["C:risk_on_trend" if idx % 2 == 0 else "C:neutral_range" for idx, _ts in enumerate(dates)],
                }
            )
            conn.register("regime_df", frame)
            conn.execute("INSERT INTO market_regime_daily SELECT * FROM regime_df")
    finally:
        conn.close()


def _write_regime_audit_fixture(tmp_path: Path, source_rows: pd.DataFrame, *, write_regime_table: bool = False) -> tuple[Path, Path, Path]:
    source_run_dir = tmp_path / "source_run"
    stability_run_dir = tmp_path / "stability_run"
    source_run_dir.mkdir(parents=True, exist_ok=True)
    stability_run_dir.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "source.parquet"
    source_rows.to_parquet(source_path, index=False)
    champion_compare = tmp_path / "champion" / "compare.json"
    _write_champion_compare(champion_compare)
    db_path = tmp_path / "stocks.duckdb"
    dates = [pd.Timestamp(value) for value in sorted(source_rows["trade_date"].map(mod.normalize_date_key).unique().tolist())]
    if write_regime_table:
        _write_regime_db(db_path, dates)
    else:
        duckdb.connect(str(db_path)).close()

    fixed_hash = "fixed-test-hash"
    artifacts = {
        "compare.json": {"schema_version": "compare", "fixed_condition_hash": fixed_hash, "variant_results": []},
        "family_leaderboard.json": {"schema_version": "family", "fixed_condition_hash": fixed_hash},
        "session_leaderboard_rollup.json": {"schema_version": "session", "fixed_condition_hash": fixed_hash},
        "scope_stability_rollup.json": {"schema_version": "scope", "fixed_condition_hash": fixed_hash},
        "ma_horizon_role_summary.json": {"schema_version": "role"},
        "candidate_decision.ma_buy_probe.json": {"schema_version": "buy"},
        "candidate_decision.ma_sell_probe.json": {"schema_version": "sell"},
        "ma_feature_coverage.json": {"schema_version": "coverage"},
        "evaluation_contract.json": {
            "schema_version": "contract",
            "source_rows_artifact_path": str(source_path),
            "champion_compare_json_path": str(champion_compare),
            "runtime_stock_db_path": str(db_path),
            "fixed_condition_hash": fixed_hash,
        },
        "run_manifest.json": {"schema_version": "manifest"},
        "_ARTIFACT_COMPLETE.json": {"complete": True},
    }
    for name, payload in artifacts.items():
        (source_run_dir / name).write_text(json.dumps(payload), encoding="utf-8")
    (stability_run_dir / "kept_candidate_by_regime.json").write_text(
        json.dumps(
            {
                "schema_version": "by_regime",
                "rows": [
                    {
                        "variant_id": "ma_buy_probe.price_vs_ma_n_8",
                        "regime_label": "unknown",
                        "regime_top10_delta": 0.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return source_run_dir, stability_run_dir, db_path


def test_date_normalization_accepts_supported_formats() -> None:
    assert mod.normalize_date_key(20250131) == "2025-01-31"
    assert mod.normalize_date_key("2025-01-31") == "2025-01-31"
    assert mod.normalize_date_key(1738281600) == "2025-01-31"


def test_no_lookahead_future_bar_does_not_change_feature() -> None:
    symbols = ["1001"]
    bars = _build_bars(symbols, days=30)
    trade_date = _dates(25)[-1]
    source = mod.load_source_rows_from_frame(_build_source_rows(symbols, trade_date))
    clean = mod.join_features_to_source(source, mod.build_ma_bar_features(bars))

    future = pd.DataFrame(
        [
            {
                "code": "1001",
                "date": int(pd.Timestamp("2026-12-31").strftime("%Y%m%d")),
                "o": 10000.0,
                "h": 10000.0,
                "l": 10000.0,
                "c": 10000.0,
                "v": 1,
                "source": "pan",
            }
        ]
    )
    with_future = mod.join_features_to_source(source, mod.build_ma_bar_features(pd.concat([bars, future], ignore_index=True)))

    assert clean.loc[0, "bar_date_used"] == with_future.loc[0, "bar_date_used"]
    assert clean.loc[0, "ma_20"] == with_future.loc[0, "ma_20"]
    assert bool(with_future.loc[0, "no_lookahead_valid"]) is True


def test_forward_ret20_is_evaluation_only_for_selection() -> None:
    symbols = [f"10{i:02d}" for i in range(12)]
    trade_date = _dates(230)[-1]
    source_a = mod.load_source_rows_from_frame(_build_source_rows(symbols, trade_date))
    source_b = source_a.copy()
    source_b["forward_ret_20d"] = list(reversed(source_b["forward_ret_20d"].tolist()))
    features = mod.build_ma_bar_features(_build_bars(symbols, days=230))
    joined_a = mod.join_features_to_source(source_a, features)
    joined_b = mod.join_features_to_source(source_b, features)
    spec = [item for item in mod._make_variant_specs()[0] if item.probe_family == "ma_buy_probe"][0]

    ranked_a, _ = mod._rank_with_variant(joined_a, spec)
    ranked_b, _ = mod._rank_with_variant(joined_b, spec)

    assert ranked_a[["symbol", "challenger_rank"]].sort_values("symbol").reset_index(drop=True).equals(
        ranked_b[["symbol", "challenger_rank"]].sort_values("symbol").reset_index(drop=True)
    )


def test_feature_family_compatibility_preflight() -> None:
    assert mod._feature_family_for_probe("ma_buy_probe") == "boundary_feature"
    assert mod._feature_family_for_probe("ma_sell_probe") == "bad_pick_removal"
    assert contracts.normalize_feature_family("boundary_feature") == "boundary_feature"
    assert contracts.normalize_feature_family("bad_pick_removal") == "bad_pick_removal"


def test_buy_sell_gate_separation() -> None:
    assert mod._run_gate_separation_check() is True


def test_variant_cap_records_skipped_variants() -> None:
    specs, skipped = mod._make_variant_specs(cap_per_family=5)

    assert sum(1 for spec in specs if spec.probe_family == "ma_buy_probe") == 5
    assert sum(1 for spec in specs if spec.probe_family == "ma_sell_probe") == 5
    assert skipped
    assert {row["skip_reason"] for row in skipped} == {"sweep_scope_limited_initial_branching_probe"}


def test_run_writes_required_json_and_fixed_conditions(tmp_path: Path) -> None:
    symbols = [f"20{i:02d}" for i in range(12)]
    bars = _build_bars(symbols, days=230)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, bars)
    trade_date = _dates(230)[-1]
    source_path = tmp_path / "source.parquet"
    _build_source_rows(symbols, trade_date).to_parquet(source_path, index=False)
    champion_compare = tmp_path / "champion" / "compare.json"
    _write_champion_compare(champion_compare)

    result = mod.run_ma_buy_sell_probe(
        source_rows_parquet=source_path,
        champion_compare_json_path=champion_compare,
        stock_db=db_path,
        output_root=tmp_path / "out",
        run_id="unit",
    )

    session_dir = Path(result["session_dir"])
    for name in (*mod.REQUIRED_AUTHORITATIVE_JSON, *mod.REQUIRED_SUPPORTING_JSON, "_ARTIFACT_COMPLETE.json"):
        assert (session_dir / name).exists(), name

    compare = json.loads((session_dir / "compare.json").read_text(encoding="utf-8"))
    family = json.loads((session_dir / "family_leaderboard.json").read_text(encoding="utf-8"))
    session = json.loads((session_dir / "session_leaderboard_rollup.json").read_text(encoding="utf-8"))
    scope = json.loads((session_dir / "scope_stability_rollup.json").read_text(encoding="utf-8"))
    role_summary = json.loads((session_dir / "ma_horizon_role_summary.json").read_text(encoding="utf-8"))
    complete = json.loads((session_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))

    assert compare["fixed_condition_hash"] == family["fixed_condition_hash"] == session["fixed_condition_hash"] == scope["fixed_condition_hash"]
    assert complete["verification"]["no_lookahead_test"] is True
    assert complete["verification"]["source_rows_vs_runtime_db_responsibility_separation"] is True
    assert complete["verification"]["buy_sell_gate_separation_test"] is True
    assert complete["verification"]["variant_cap_test"] is True
    assert complete["verification"]["feature_family_compatibility_preflight"] is True
    assert {row["probe_family"] for row in compare["candidate_results"]} == {"ma_buy_probe", "ma_sell_probe"}
    assert role_summary["boundary"] == "TRADEX-only"
    assert {"short", "mid", "long"}.issubset({row["horizon_bucket"] for row in role_summary["horizon_rows"]})
    assert {7, 20, 60, 100, 200}.issubset({int(row["period"]) for row in role_summary["current_ma_role_rows"]})
    assert {
        "entry_timing",
        "trend_ride",
        "trend_confirmation",
        "resistance_band_confirmation",
        "environment_confirmation",
    }.issubset({row["role_intent"] for row in role_summary["current_ma_role_rows"]})


def test_coverage_incomplete_is_recorded_for_long_ma(tmp_path: Path) -> None:
    symbols = [f"30{i:02d}" for i in range(12)]
    bars = _build_bars(symbols, days=60)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, bars)
    trade_date = _dates(60)[-1]
    source_path = tmp_path / "source.parquet"
    _build_source_rows(symbols, trade_date).to_parquet(source_path, index=False)
    champion_compare = tmp_path / "champion" / "compare.json"
    _write_champion_compare(champion_compare)

    result = mod.run_ma_buy_sell_probe(
        source_rows_parquet=source_path,
        champion_compare_json_path=champion_compare,
        stock_db=db_path,
        output_root=tmp_path / "out",
        run_id="coverage",
    )

    session_dir = Path(result["session_dir"])
    coverage = json.loads((session_dir / "ma_feature_coverage.json").read_text(encoding="utf-8"))
    compare = json.loads((session_dir / "compare.json").read_text(encoding="utf-8"))

    assert coverage["coverage_incomplete_variant_count"] > 0
    incomplete_rows = [row for row in coverage["coverage_rows"] if row["skip_reason"] == "feature_coverage_incomplete"]
    assert incomplete_rows
    assert "feature_coverage_incomplete" in json.dumps(compare["variant_results"], ensure_ascii=False)


def test_kept_candidate_stability_validation_writes_required_artifacts(tmp_path: Path) -> None:
    symbols = [f"40{i:02d}" for i in range(12)]
    bars = _build_bars(symbols, days=230)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, bars)
    trade_date = _dates(230)[-1]
    source_path = tmp_path / "source.parquet"
    _build_source_rows(symbols, trade_date).to_parquet(source_path, index=False)
    champion_compare = tmp_path / "champion" / "compare.json"
    _write_champion_compare(champion_compare)

    probe = mod.run_ma_buy_sell_probe(
        source_rows_parquet=source_path,
        champion_compare_json_path=champion_compare,
        stock_db=db_path,
        output_root=tmp_path / "probe",
        run_id="probe",
    )
    validation = mod.run_kept_candidate_stability_validation(
        source_run_dir=Path(probe["session_dir"]),
        output_root=tmp_path / "validation",
        validation_run_id="validation",
    )

    validation_dir = Path(validation["validation_dir"])
    for name in (*mod.REQUIRED_STABILITY_ARTIFACTS, "_VALIDATION_COMPLETE.json"):
        assert (validation_dir / name).exists(), name

    stability = json.loads((validation_dir / "kept_candidate_stability.json").read_text(encoding="utf-8"))
    role = json.loads((validation_dir / "ma_horizon_role_stability.json").read_text(encoding="utf-8"))
    complete = json.loads((validation_dir / "_VALIDATION_COMPLETE.json").read_text(encoding="utf-8"))

    assert set(stability["primary_validation_targets"]) == set(mod.PRIMARY_STABILITY_VARIANTS)
    assert stability["meemee_reflection"] is False
    assert stability["production_registration"] is False
    assert complete["verification"]["required_artifacts_exist"] is True
    assert complete["verification"]["not_production_ready_confirmed"] is True
    assert role["meemee_display_change_recommended"] is False
    assert {"short_entry_timing", "mid_trend_ride", "long_context_confirmation"} == {
        row["role"] for row in role["role_rows"]
    }


def test_regime_label_audit_detects_source_regime_label_and_writes_complete(tmp_path: Path) -> None:
    trade_date = _dates(20)[-1]
    source = _build_source_rows(["5001", "5002", "5003"], trade_date)
    source["regime_label"] = ["C:risk_on_trend", "C:neutral_range", "C:risk_on_trend"]
    source_run, stability_run, _db_path = _write_regime_audit_fixture(tmp_path, source)

    result = mod.run_regime_label_audit(
        source_run_dir=source_run,
        stability_run_dir=stability_run,
        output_root=tmp_path / "audit",
        audit_run_id="audit",
    )

    audit_dir = Path(result["audit_dir"])
    for name in mod.REQUIRED_REGIME_AUDIT_ARTIFACTS:
        assert (audit_dir / name).exists(), name
    audit = json.loads((audit_dir / "regime_label_audit.json").read_text(encoding="utf-8"))
    complete = json.loads((audit_dir / "_AUDIT_COMPLETE.json").read_text(encoding="utf-8"))

    assert audit["regime_label_status"] == "available"
    assert audit["audit_decision"] == "rerun_validation_with_source_regime_label"
    assert complete["verification"]["required_json_parse"] is True
    assert complete["verification"]["no_synthetic_regime_inference"] is True


def test_regime_label_audit_detects_alternate_regime_column(tmp_path: Path) -> None:
    trade_date = _dates(20)[-1]
    source = _build_source_rows(["5101", "5102", "5103"], trade_date)
    source["market_regime_bucket"] = "unknown"
    source["market_regime"] = ["C:risk_on_trend", "C:neutral_range", "C:risk_on_trend"]
    source_run, stability_run, _db_path = _write_regime_audit_fixture(tmp_path, source)

    result = mod.run_regime_label_audit(
        source_run_dir=source_run,
        stability_run_dir=stability_run,
        output_root=tmp_path / "audit",
        audit_run_id="audit",
    )

    audit = json.loads((Path(result["audit_dir"]) / "regime_label_audit.json").read_text(encoding="utf-8"))
    recommendation = json.loads((Path(result["audit_dir"]) / "regime_label_validation_recommendation.json").read_text(encoding="utf-8"))

    assert audit["regime_label_status"] == "available_under_alternate_column"
    assert audit["audit_decision"] == "add_normalization_mapping_then_rerun_validation"
    assert recommendation["if_available_under_alternate_column"]["action"] == "add_normalization_mapping"


def test_regime_label_audit_unknown_only_does_not_infer_synthetic_regime(tmp_path: Path) -> None:
    trade_date = _dates(20)[-1]
    source = _build_source_rows(["5201", "5202", "5203"], trade_date)
    source["market_regime_bucket"] = "unknown"
    source_run, stability_run, _db_path = _write_regime_audit_fixture(tmp_path, source)

    result = mod.run_regime_label_audit(
        source_run_dir=source_run,
        stability_run_dir=stability_run,
        output_root=tmp_path / "audit",
        audit_run_id="audit",
    )

    audit = json.loads((Path(result["audit_dir"]) / "regime_label_audit.json").read_text(encoding="utf-8"))
    recommendation = json.loads((Path(result["audit_dir"]) / "regime_label_validation_recommendation.json").read_text(encoding="utf-8"))

    assert audit["regime_label_status"] == "not_recoverable_without_champion_regeneration"
    assert audit["no_synthetic_regime_inference"] is True
    assert recommendation["if_not_recoverable_without_champion_regeneration"]["typed_reason"] == "regime_stability_unobservable"


def test_regime_label_audit_checks_validation_only_join_and_fixed_condition_preservation(tmp_path: Path) -> None:
    trade_date = _dates(20)[-1]
    source = _build_source_rows(["5301", "5302", "5303"], trade_date)
    source["market_regime_bucket"] = "unknown"
    source_run, stability_run, _db_path = _write_regime_audit_fixture(tmp_path, source, write_regime_table=True)

    result = mod.run_regime_label_audit(
        source_run_dir=source_run,
        stability_run_dir=stability_run,
        output_root=tmp_path / "audit",
        audit_run_id="audit",
    )

    join = json.loads((Path(result["audit_dir"]) / "regime_label_join_feasibility.json").read_text(encoding="utf-8"))
    complete = json.loads((Path(result["audit_dir"]) / "_AUDIT_COMPLETE.json").read_text(encoding="utf-8"))

    assert join["canonical_regime_artifact_found"] is True
    assert join["join_safe_for_validation_only"] is True
    assert join["join_would_change_fixed_condition_hash"] is True
    assert join["recommendation"] == "rerun_validation_with_existing_regime_labels"
    assert complete["verification"]["fixed_condition_preservation_checked"] is True


def test_canonical_regime_validation_only_join_preserves_scores_and_splits_hashes(tmp_path: Path) -> None:
    symbols = [f"60{i:02d}" for i in range(12)]
    bars = _build_bars(symbols, days=230)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, bars)
    trade_dates = [_dates(225)[-1], _dates(230)[-1]]
    _write_regime_db(db_path, trade_dates)
    source = pd.concat([_build_source_rows(symbols, trade_date) for trade_date in trade_dates], ignore_index=True)
    source["market_regime_bucket"] = "unknown"
    source["dominant_regime_context"] = ["C:risk_on_trend" if idx % 2 == 0 else "unknown" for idx in range(len(source))]
    source["family_regime_context"] = ["C:neutral_range" if idx % 3 == 0 else None for idx in range(len(source))]
    source["family_bad_pick_regime"] = ["C:risk_off_trend" if idx % 4 == 0 else "unknown" for idx in range(len(source))]
    source_path = tmp_path / "source.parquet"
    source.to_parquet(source_path, index=False)
    champion_compare = tmp_path / "champion" / "compare.json"
    _write_champion_compare(champion_compare)

    probe = mod.run_ma_buy_sell_probe(
        source_rows_parquet=source_path,
        champion_compare_json_path=champion_compare,
        stock_db=db_path,
        output_root=tmp_path / "probe",
        run_id="probe",
    )
    validation = mod.run_kept_candidate_canonical_regime_validation(
        source_run_dir=Path(probe["session_dir"]),
        output_root=tmp_path / "validation",
        validation_run_id="canonical",
        canonical_regime_db=db_path,
        audit_run_dir=tmp_path / "audit",
    )

    validation_dir = Path(validation["validation_dir"])
    for name in mod.REQUIRED_CANONICAL_REGIME_VALIDATION_ARTIFACTS:
        assert (validation_dir / name).exists(), name

    stability = json.loads((validation_dir / "kept_candidate_stability.canonical_regime.json").read_text(encoding="utf-8"))
    canonical = json.loads((validation_dir / "kept_candidate_by_regime.canonical_regime.json").read_text(encoding="utf-8"))
    alternate = json.loads((validation_dir / "kept_candidate_by_regime.alternate_context.json").read_text(encoding="utf-8"))
    manifest = json.loads((validation_dir / "kept_candidate_regime_source_manifest.json").read_text(encoding="utf-8"))
    join_quality = json.loads((validation_dir / "kept_candidate_regime_join_quality.json").read_text(encoding="utf-8"))
    hash_check = json.loads((validation_dir / "kept_candidate_regime_hash_check.json").read_text(encoding="utf-8"))
    complete = json.loads((validation_dir / "_VALIDATION_COMPLETE.json").read_text(encoding="utf-8"))

    assert validation["validation_complete_written"] is True
    assert stability["ranking_fixed_condition_hash"] == hash_check["original_fixed_condition_hash"]
    assert stability["validation_grouping_hash"] == hash_check["validation_grouping_hash"]
    assert hash_check["ranking_conditions_changed"] is False
    assert hash_check["validation_grouping_changed"] is True
    assert hash_check["score_rank_topk_invariance_check"] is True
    assert join_quality["join_coverage_rate"] == 1.0
    assert join_quality["non_unknown_bucket_count"] >= 2
    assert {row["regime_label"] for row in canonical["rows"]} >= {"C:risk_on_trend", "C:neutral_range"}
    assert manifest["regime_source_role"] == mod.CANONICAL_REGIME_SOURCE_ROLE
    assert manifest["champion_artifact_regenerated"] is False
    assert manifest["production_registration"] is False
    assert manifest["meemee_reflection"] is False
    assert manifest["synthetic_regime_inference_used"] is False
    assert alternate["decision_role"] == "diagnostic_only"
    assert {row["decision_role"] for row in alternate["rows"]} == {"diagnostic_only"}
    assert "regime_label" not in {row["context_column"] for row in alternate["rows"]}
    assert complete["verification"]["required_json_parse"] is True
    assert complete["verification"]["score_rank_topk_invariance_check"] is True


def test_final_decision_rollup_freezes_regime_conditional_hold(tmp_path: Path) -> None:
    role_run = tmp_path / "role"
    stability_run = tmp_path / "stability"
    audit_run = tmp_path / "audit"
    canonical_run = tmp_path / "canonical"
    for path in (role_run, stability_run, audit_run, canonical_run):
        path.mkdir(parents=True, exist_ok=True)

    (role_run / "compare.json").write_text(json.dumps({"schema_version": "compare_test"}), encoding="utf-8")
    (stability_run / "kept_candidate_stability.json").write_text(json.dumps({"schema_version": "stability_test"}), encoding="utf-8")
    (audit_run / "regime_label_audit.json").write_text(
        json.dumps({"schema_version": "audit_test", "regime_label_status": "available_under_alternate_column"}),
        encoding="utf-8",
    )
    (audit_run / "_AUDIT_COMPLETE.json").write_text(json.dumps({"complete": True}), encoding="utf-8")

    candidate_rows = [
        {
            "variant_id": "ma_buy_probe.price_vs_ma_n_8",
            "stability_decision": "hold_for_more_validation",
            "top5_mean_ret20_delta": 0.0001936,
            "top10_mean_ret20_delta": 0.0001996,
            "changed_top5_members_count": 28,
            "changed_top10_members_count": 40,
            "changed_rank_count": 400,
            "bad_pick_removal_count": 2,
        },
        {
            "variant_id": "ma_sell_probe.price_cross_below_ma_n_8",
            "stability_decision": "hold_for_more_validation",
            "top5_mean_ret20_delta": 0.0001414,
            "top10_mean_ret20_delta": 0.0001795,
            "changed_top5_members_count": 12,
            "changed_top10_members_count": 26,
            "changed_rank_count": 210,
            "bad_pick_removal_count": 2,
        },
    ]
    bucket_rows = []
    for variant_id in ("ma_buy_probe.price_vs_ma_n_8", "ma_sell_probe.price_cross_below_ma_n_8"):
        for regime, top10_delta, changed, decision in (
            ("neutral_range", 0.0002, 12, "bucket_supports_candidate"),
            ("risk_off_trend", 0.0001, 2, "bucket_supports_candidate"),
            ("risk_on_trend", 0.0003, 8, "bucket_supports_candidate"),
            ("risk_on_range", -0.0001, 4, "bucket_mixed"),
            ("capitulation_rebound", 0.0, 0, "bucket_no_material_branching"),
            ("high_vol_chaos", 0.0, 0, "bucket_no_material_branching"),
        ):
            bucket_rows.append(
                {
                    "variant_id": variant_id,
                    "regime_label": regime,
                    "top5_delta": top10_delta,
                    "top10_delta": top10_delta,
                    "changed_top5_members_count": changed,
                    "changed_top10_members_count": changed,
                    "changed_rank_count": changed * 3,
                    "bad_pick_removal_count": 1 if variant_id.startswith("ma_sell") and changed else 0,
                    "sample_count": 100,
                    "stability_bucket_decision": decision,
                }
            )
    (canonical_run / "kept_candidate_stability.canonical_regime.json").write_text(
        json.dumps(
            {
                "schema_version": "canonical_stability_test",
                "ranking_fixed_condition_hash": "ranking-hash",
                "validation_grouping_hash": "grouping-hash",
                "candidate_rows": candidate_rows,
            }
        ),
        encoding="utf-8",
    )
    (canonical_run / "kept_candidate_by_regime.canonical_regime.json").write_text(
        json.dumps({"schema_version": "canonical_by_regime_test", "rows": bucket_rows}),
        encoding="utf-8",
    )
    (canonical_run / "kept_candidate_regime_join_quality.json").write_text(
        json.dumps(
            {
                "join_coverage_rate": 1.0,
                "rows_joined_count": 2542,
                "rows_unjoined_count": 0,
                "unknown_bucket_count": 0,
                "non_unknown_bucket_count": 6,
                "observed_regime_buckets": [
                    "capitulation_rebound",
                    "high_vol_chaos",
                    "neutral_range",
                    "risk_off_trend",
                    "risk_on_range",
                    "risk_on_trend",
                ],
            }
        ),
        encoding="utf-8",
    )
    (canonical_run / "kept_candidate_regime_hash_check.json").write_text(
        json.dumps(
            {
                "ranking_fixed_condition_hash": "ranking-hash",
                "validation_grouping_hash": "grouping-hash",
                "ranking_conditions_changed": False,
                "validation_grouping_changed": True,
                "score_rank_topk_invariance_check": True,
            }
        ),
        encoding="utf-8",
    )
    (canonical_run / "kept_candidate_regime_source_manifest.json").write_text(
        json.dumps(
            {
                "regime_source_mode": mod.CANONICAL_REGIME_SOURCE_MODE,
                "regime_source_role": mod.CANONICAL_REGIME_SOURCE_ROLE,
                "canonical_regime_artifact_path": "stocks.duckdb#market_regime_daily",
                "alternate_context_columns": list(mod.ALTERNATE_CONTEXT_REGIME_COLUMNS),
            }
        ),
        encoding="utf-8",
    )
    (canonical_run / "ma_horizon_role_stability.canonical_regime.json").write_text(
        json.dumps(
            {
                "role_rows": [
                    {"role": "short_entry_timing", "role_level_decision": "hold_for_more_validation"},
                    {"role": "mid_trend_ride", "role_level_decision": "hold_for_more_validation"},
                    {"role": "long_context_confirmation", "role_level_decision": "keep_as_context_only"},
                ]
            }
        ),
        encoding="utf-8",
    )
    (canonical_run / "_VALIDATION_COMPLETE.json").write_text(json.dumps({"complete": True}), encoding="utf-8")

    result = mod.run_final_decision_rollup(
        source_role_validation_run=role_run,
        source_stability_validation_run=stability_run,
        source_regime_audit_run=audit_run,
        source_canonical_regime_validation_run=canonical_run,
        output_root=tmp_path / "rollup",
        rollup_run_id="rollup",
    )

    rollup = json.loads(Path(result["rollup_json"]).read_text(encoding="utf-8"))

    assert rollup["final_axis_status"] == "closed_as_regime_conditional_hold"
    assert rollup["candidate_decisions"]["ma_buy_probe.price_vs_ma_n_8"]["decision"] == "hold_for_more_validation"
    assert rollup["candidate_decisions"]["ma_sell_probe.price_cross_below_ma_n_8"]["decision"] == "hold_for_more_validation"
    assert "positive_in_3_canonical_regime_buckets" in rollup["candidate_decisions"]["ma_buy_probe.price_vs_ma_n_8"]["not_drop_reason"]
    assert "bad_pick_removal_observed" in rollup["candidate_decisions"]["ma_sell_probe.price_cross_below_ma_n_8"]["not_drop_reason"]
    assert rollup["candidate_decisions"]["ma_buy_probe.price_vs_ma_n_8"]["production_ready"] is False
    assert rollup["candidate_decisions"]["ma_sell_probe.price_cross_below_ma_n_8"]["meemee_ready"] is False
    assert rollup["horizon_role_decisions"]["long_context_confirmation"]["decision"] == "keep_as_context_only"
    assert rollup["next_allowed_axis"]["axis_id"] == "regime_applicability_gate_v1"
    assert rollup["production_registration"] is False
    assert rollup["meemee_reflection"] is False
    assert rollup["champion_artifact_regenerated"] is False
