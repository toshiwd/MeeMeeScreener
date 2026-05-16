from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_ma_buy_sell_probe_v1 as ma_probe
from scripts import tradex_regime_applicability_gate_v1 as gate


def _dates(days: int) -> list[pd.Timestamp]:
    return list(pd.bdate_range("2025-01-02", periods=days))


def _build_source_rows(symbols: list[str], trade_dates: list[pd.Timestamp]) -> pd.DataFrame:
    rows = []
    for trade_date in trade_dates:
        for idx, symbol in enumerate(symbols, start=1):
            ret = 0.03 if idx in {6, 8} else (-0.05 if idx in {4, 9} else 0.004)
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
                    "top15_label": idx in {6, 8},
                    "bottom15_label": idx in {4, 9},
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
            close = 50.0 + idx * (0.35 + symbol_index * 0.01) + symbol_index
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


def _write_db(path: Path, bars: pd.DataFrame, regime_dates: list[pd.Timestamp]) -> None:
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
        regimes = ["C:neutral_range", "C:risk_on_range"]
        frame = pd.DataFrame(
            {
                "dt": [ts.strftime("%Y-%m-%d") for ts in regime_dates],
                "regime_id": [regimes[idx % len(regimes)] for idx, _ts in enumerate(regime_dates)],
            }
        )
        conn.register("regime_df", frame)
        conn.execute("INSERT INTO market_regime_daily SELECT * FROM regime_df")
    finally:
        conn.close()


def _write_source_run(tmp_path: Path, source_path: Path, db_path: Path) -> tuple[Path, Path]:
    source_run = tmp_path / "source_run"
    source_run.mkdir(parents=True, exist_ok=True)
    champion_compare = tmp_path / "champion" / "compare.json"
    champion_compare.parent.mkdir(parents=True, exist_ok=True)
    champion_compare.write_text(
        json.dumps(
            {
                "schema_version": "champion_compare_test_v1",
                "same_condition_contract": {
                    "ret20_source_mode": "forward_ret_20d",
                    "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
                    "same_top_k": [5, 10, 20],
                },
            }
        ),
        encoding="utf-8",
    )
    fixed_hash = "fixed-test-hash"
    artifacts = {
        "compare.json": {
            "schema_version": "compare_test",
            "fixed_condition_hash": fixed_hash,
            "variant_results": [
                {"variant_id": variant_id, "candidate_local_decision": "hold", "probe_family": variant_id.split(".", 1)[0]}
                for variant_id in gate.TARGET_VARIANTS
            ],
        },
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
            "cost_slippage_config": {"mode": "flat_zero_cost"},
        },
        "run_manifest.json": {"schema_version": "manifest_test"},
        "_ARTIFACT_COMPLETE.json": {"complete": True},
    }
    for name, payload in artifacts.items():
        (source_run / name).write_text(json.dumps(payload), encoding="utf-8")
    final_rollup = tmp_path / "final_rollup.json"
    final_rollup.write_text(
        json.dumps(
            {
                "schema_version": "final_rollup_test",
                "final_axis_status": "closed_as_regime_conditional_hold",
                "source_role_validation_run": str(source_run),
                "validation_grouping_hash": "grouping-test-hash",
                "regime_source": {"canonical_regime_artifact_path": str(db_path) + "#market_regime_daily"},
            }
        ),
        encoding="utf-8",
    )
    return source_run, final_rollup


def test_regime_gate_blocks_non_allow_regime_hits(tmp_path: Path) -> None:
    symbols = [f"70{i:02d}" for i in range(12)]
    trade_dates = [_dates(225)[-1], _dates(230)[-1]]
    source = ma_probe.load_source_rows_from_frame(_build_source_rows(symbols, trade_dates))
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, _build_bars(symbols), trade_dates)
    canonical, _meta = ma_probe._load_canonical_regime_rows(db_path)
    features = ma_probe.build_ma_bar_features(ma_probe.load_daily_bars(db_path, symbols))
    joined = ma_probe._with_canonical_regime_labels(ma_probe.join_features_to_source(source, features), canonical)
    spec = ma_probe._variant_spec_map()["ma_buy_probe.price_vs_ma_n_8"]

    ungated, _ungated_coverage = ma_probe._rank_with_variant(joined, spec)
    gated, gated_coverage = gate._rank_with_regime_gate(joined, spec)

    blocked_hits = gated[gated["ma_probe_signal_hit"] & ~gated["regime_gate_allowed"]]
    assert blocked_hits.empty
    assert gated_coverage["regime_gate_blocked_signal_hit_count"] >= 0
    assert int(gated["ma_probe_signal_hit"].sum()) <= int(ungated["ma_probe_signal_hit"].sum())


def test_run_writes_json_and_readback_verification(tmp_path: Path) -> None:
    symbols = [f"80{i:02d}" for i in range(12)]
    trade_dates = [_dates(225)[-1], _dates(230)[-1]]
    source_path = tmp_path / "source.parquet"
    _build_source_rows(symbols, trade_dates).to_parquet(source_path, index=False)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path, _build_bars(symbols), trade_dates)
    _source_run, final_rollup = _write_source_run(tmp_path, source_path, db_path)

    result = gate.run_regime_applicability_gate(
        source_final_rollup_json=final_rollup,
        output_root=tmp_path / "out",
        run_id="gate",
    )

    output_dir = Path(result["output_dir"])
    for name in gate.REQUIRED_JSON:
        assert (output_dir / name).exists(), name

    decision = json.loads((output_dir / "regime_applicability_gate_v1_decision.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    compare = json.loads((output_dir / "regime_applicability_gate_v1_compare.json").read_text(encoding="utf-8"))

    assert decision["authoritative_decision"] in {"keep", "drop", "hold"}
    assert complete["complete"] is True
    assert complete["read_back_verification"]["verification"]["required_json_parse"] is True
    assert {row["variant_id"] for row in compare["variant_summaries"]} == set(gate.TARGET_VARIANTS)
    assert compare["condition_contract"]["same_ma_period"] == 8
    assert compare["condition_contract"]["regime_correction"] is False
