from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_ma_buy_sell_probe_v1 as ma_probe
from scripts import tradex_top5_safe_bad_pick_removal_v1 as top5_safe


def _dates(days: int) -> list[pd.Timestamp]:
    return list(pd.bdate_range("2025-01-02", periods=days))


def _build_source_rows(symbols: list[str], trade_date: pd.Timestamp) -> pd.DataFrame:
    rows = []
    for idx, symbol in enumerate(symbols, start=1):
        ret = -0.06 if idx in {7, 9} else (0.04 if idx in {11, 12} else 0.004)
        rows.append(
            {
                "symbol": symbol,
                "side": "long",
                "trade_date": int(trade_date.strftime("%Y%m%d")),
                "anchor_date": trade_date.strftime("%Y-%m-%d"),
                "month_bucket": trade_date.strftime("%Y-%m"),
                "market_regime_bucket": "unknown",
                "champion_rank": idx,
                "champion_score": 1.0 - idx * 0.01,
                "forward_ret_20d": ret,
                "path_value_score_v1": ret * 2.0,
                "top15_label": idx in {11, 12},
                "bottom15_label": idx in {7, 9},
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
            close = 50.0 + idx * 0.3 + symbol_index
            if symbol_index in {6, 8} and idx > days - 8:
                close -= (idx - (days - 8)) * 3.0
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
        conn.execute("CREATE TABLE market_regime_daily (dt DATE, regime_id TEXT)")
        dates = sorted({ma_probe.normalize_date_key(value) for value in bars["date"].tolist()})
        regimes = ["neutral_range", "risk_off_trend", "risk_on_trend"]
        regime_frame = pd.DataFrame(
            {
                "dt": dates,
                "regime_id": [regimes[idx % len(regimes)] for idx, _date in enumerate(dates)],
            }
        )
        conn.register("regime_df", regime_frame)
        conn.execute("INSERT INTO market_regime_daily SELECT * FROM regime_df")
    finally:
        conn.close()


def _write_prior_context(tmp_path: Path, source_path: Path, db_path: Path) -> tuple[Path, Path]:
    source_run = tmp_path / "source_run"
    source_run.mkdir(parents=True, exist_ok=True)
    champion_compare = tmp_path / "champion" / "compare.json"
    champion_compare.parent.mkdir(parents=True, exist_ok=True)
    champion_compare.write_text(json.dumps({"schema_version": "champion_compare_test_v1"}), encoding="utf-8")
    fixed_hash = "fixed-test-hash"
    artifacts = {
        "compare.json": {"schema_version": "compare_test", "fixed_condition_hash": fixed_hash},
        "family_leaderboard.json": {"schema_version": "family_test", "fixed_condition_hash": fixed_hash},
        "session_leaderboard_rollup.json": {"schema_version": "session_test", "fixed_condition_hash": fixed_hash},
        "scope_stability_rollup.json": {"schema_version": "scope_test", "fixed_condition_hash": fixed_hash},
        "ma_horizon_role_summary.json": {"schema_version": "role_test"},
        "candidate_decision.ma_buy_probe.json": {"schema_version": "buy_test"},
        "candidate_decision.ma_sell_probe.json": {"schema_version": "sell_test"},
        "ma_feature_coverage.json": {"schema_version": "coverage_test"},
        "evaluation_contract.json": {
            "schema_version": "contract_test",
            "source_rows_artifact_path": str(source_path),
            "champion_compare_json_path": str(champion_compare),
            "runtime_stock_db_path": str(db_path),
            "fixed_condition_hash": fixed_hash,
            "ret20_source_mode": "forward_ret_20d",
            "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
            "artifact_detail_level": "authoritative_full",
            "cost_slippage_config": {"mode": "flat_zero_cost"},
        },
        "run_manifest.json": {"schema_version": "manifest_test"},
        "_ARTIFACT_COMPLETE.json": {"complete": True},
    }
    for name, payload in artifacts.items():
        (source_run / name).write_text(json.dumps(payload), encoding="utf-8")
    ma_rollup = tmp_path / "ma_final_rollup.json"
    ma_rollup.write_text(
        json.dumps(
            {
                "schema_version": "ma_rollup_test",
                "final_axis_status": "closed_as_regime_conditional_hold",
                "source_role_validation_run": str(source_run),
                "production_registration": False,
                "meemee_reflection": False,
                "champion_artifact_regenerated": False,
            }
        ),
        encoding="utf-8",
    )
    regime_rollup = tmp_path / "regime_gate_rollup.json"
    regime_rollup.write_text(
        json.dumps(
            {
                "schema_version": "gate_rollup_test",
                "final_axis_status": "closed_as_noise_control_hold",
                "production_registration": False,
                "meemee_reflection": False,
                "champion_artifact_regenerated": False,
            }
        ),
        encoding="utf-8",
    )
    return ma_rollup, regime_rollup


def _manual_joined_frame() -> pd.DataFrame:
    rows = []
    for idx in range(1, 13):
        rows.append(
            {
                "source_row_id": idx - 1,
                "symbol": f"90{idx:02d}",
                "side": "long",
                "trade_date_key": "2025-01-31",
                "month_bucket": "2025-01",
                "champion_rank": idx,
                "champion_score": 1.0 - idx * 0.01,
                "forward_ret_20d": -0.08 if idx == 7 else 0.01,
                "path_value_score_v1": 0.0,
                "top15_label": False,
                "bottom15_label": idx == 7,
                "champion_selected_top5": idx <= 5,
                "champion_selected_top10": idx <= 10,
                "champion_selected_top20": True,
                "no_lookahead_valid": True,
                "signal_price_cross_below_ma_8": idx == 7,
                "signal_failed_reclaim_ma_8": idx == 7,
                "signal_support_loss_after_ma_touch_8": False,
                "signal_ma_slope_down_8": idx == 7,
            }
        )
    return pd.DataFrame(rows)


def test_top5_safe_gate_never_demotes_top5() -> None:
    frame = _manual_joined_frame()
    variant = top5_safe.VARIANTS[0]
    ranked = top5_safe._rank_with_top5_safe_demotion(frame, variant)

    top5 = ranked[ranked["champion_rank"].le(5)]
    assert not top5["top5_safe_signal_hit"].any()
    assert int(ranked["changed_top5_member"].sum()) == 0


def test_bad_pick_demotion_only_behavior_changes_boundary_rank() -> None:
    frame = _manual_joined_frame()
    ranked = top5_safe._rank_with_top5_safe_demotion(frame, top5_safe.VARIANTS[0])
    metrics = top5_safe._variant_metrics(ranked, top5_safe.VARIANTS[0])

    assert metrics["signal_hit_count"] == 1
    assert ranked.loc[ranked["symbol"] == "9007", "challenger_score"].iloc[0] < ranked.loc[ranked["symbol"] == "9007", "champion_score"].iloc[0]
    assert metrics["bad_pick_removal_top10_count"] >= 1


def test_forward_ret20_is_not_used_for_selection() -> None:
    frame_a = _manual_joined_frame()
    frame_b = frame_a.copy()
    frame_b["forward_ret_20d"] = list(reversed(frame_b["forward_ret_20d"].tolist()))

    ranked_a = top5_safe._rank_with_top5_safe_demotion(frame_a, top5_safe.VARIANTS[0])
    ranked_b = top5_safe._rank_with_top5_safe_demotion(frame_b, top5_safe.VARIANTS[0])

    assert ranked_a[["symbol", "challenger_rank"]].sort_values("symbol").reset_index(drop=True).equals(
        ranked_b[["symbol", "challenger_rank"]].sort_values("symbol").reset_index(drop=True)
    )


def test_run_writes_required_artifacts_and_preserves_boundaries(tmp_path: Path) -> None:
    symbols = [f"91{i:02d}" for i in range(12)]
    trade_date = _dates(230)[-1]
    source_path = tmp_path / "source.parquet"
    _build_source_rows(symbols, trade_date).to_parquet(source_path, index=False)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, _build_bars(symbols))
    ma_rollup, regime_rollup = _write_prior_context(tmp_path, source_path, db_path)

    result = top5_safe.run_top5_safe_bad_pick_removal(
        ma_final_rollup_json=ma_rollup,
        regime_gate_final_rollup_json=regime_rollup,
        output_root=tmp_path / "out",
        run_id="top5safe",
    )

    output_dir = Path(result["output_dir"])
    for name in top5_safe.REQUIRED_JSON:
        assert (output_dir / name).exists(), name
    assert (output_dir / top5_safe.FINAL_ROLLUP_JSON).exists()

    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    decision = json.loads((output_dir / "top5_safe_bad_pick_removal_candidate_decision.json").read_text(encoding="utf-8"))
    contract = json.loads((output_dir / "top5_safe_bad_pick_removal_contract.json").read_text(encoding="utf-8"))
    invariance = json.loads((output_dir / "top5_safe_bad_pick_removal_invariance_check.json").read_text(encoding="utf-8"))
    final_rollup = json.loads((output_dir / top5_safe.FINAL_ROLLUP_JSON).read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert decision["authoritative_decision"] in {"keep", "drop", "hold"}
    assert decision["production_registration"] is False
    assert decision["meemee_reflection"] is False
    assert "MeeMee files" in contract["disallowed_changes"]
    assert invariance["fixed_conditions_preserved"] is True
    assert invariance["forward_ret20_is_evaluation_only"] is True
    assert invariance["top5_protection_check_passed"] is True
    assert final_rollup["production_ready"] is False
    assert final_rollup["meemee_ready"] is False
    assert final_rollup["next_allowed_axis"] == "top5_safe_bad_pick_removal_stability_v1"


def test_stability_validation_writes_required_artifacts(tmp_path: Path) -> None:
    symbols = [f"92{i:02d}" for i in range(12)]
    trade_date = _dates(230)[-1]
    source_path = tmp_path / "source.parquet"
    _build_source_rows(symbols, trade_date).to_parquet(source_path, index=False)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, _build_bars(symbols))
    ma_rollup, regime_rollup = _write_prior_context(tmp_path, source_path, db_path)
    result = top5_safe.run_top5_safe_bad_pick_removal(
        ma_final_rollup_json=ma_rollup,
        regime_gate_final_rollup_json=regime_rollup,
        output_root=tmp_path / "out",
        run_id="top5safe",
    )
    source_final = Path(result["output_dir"]) / top5_safe.FINAL_ROLLUP_JSON
    source_payload = json.loads(source_final.read_text(encoding="utf-8"))
    source_payload["final_axis_status"] = "closed_as_research_keep"
    source_final.write_text(json.dumps(source_payload), encoding="utf-8")

    stability = top5_safe.run_top5_safe_bad_pick_removal_stability(
        source_run_dir=Path(result["output_dir"]),
        output_root=tmp_path / "stability",
        run_id="stability",
    )

    output_dir = Path(stability["output_dir"])
    for name in top5_safe.REQUIRED_STABILITY_ARTIFACTS:
        assert (output_dir / name).exists(), name
    complete = json.loads((output_dir / "_STABILITY_COMPLETE.json").read_text(encoding="utf-8"))
    stability_json = json.loads((output_dir / "top5_safe_bad_pick_removal_stability.json").read_text(encoding="utf-8"))

    assert complete["complete"] is True
    assert stability_json["stability_decision"] in {"keep_for_next_stage", "hold_for_more_validation", "drop_after_stability_check"}
    assert complete["read_back_verification"]["verification"]["required_json_parse"] is True
    assert complete["read_back_verification"]["verification"]["no_meemee_reflection"] is True


def test_rejects_unclosed_prior_rollups(tmp_path: Path) -> None:
    ma_rollup = tmp_path / "ma.json"
    regime_rollup = tmp_path / "gate.json"
    ma_rollup.write_text(json.dumps({"final_axis_status": "open"}), encoding="utf-8")
    regime_rollup.write_text(json.dumps({"final_axis_status": "closed_as_noise_control_hold"}), encoding="utf-8")

    try:
        top5_safe.run_top5_safe_bad_pick_removal(
            ma_final_rollup_json=ma_rollup,
            regime_gate_final_rollup_json=regime_rollup,
            output_root=tmp_path / "out",
            run_id="bad",
        )
    except RuntimeError as exc:
        assert "closed_as_regime_conditional_hold" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
