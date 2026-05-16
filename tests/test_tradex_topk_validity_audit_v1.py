from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts import tradex_topk_validity_audit_v1 as topk


def _source_rows() -> pd.DataFrame:
    rows = []
    for month_index, month in enumerate(("2025-01", "2025-02", "2025-03")):
        for rank in range(1, 31):
            if rank <= 5:
                ret = 0.04
            elif rank <= 10:
                ret = 0.02
            elif rank <= 20:
                ret = 0.005
            else:
                ret = -0.01
            rows.append(
                {
                    "symbol": f"{month_index}{rank:04d}",
                    "side": "long",
                    "trade_date": int(month.replace("-", "") + "15"),
                    "anchor_date": f"{month}-15",
                    "month_bucket": month,
                    "champion_rank": rank,
                    "champion_score": 1.0 - rank * 0.01,
                    "forward_ret_20d": ret,
                    "path_value_score_v1": ret * 2,
                    "top15_label": rank <= 5,
                    "bottom15_label": rank >= 25,
                    "champion_selected_top5": rank <= 5,
                    "champion_selected_top10": rank <= 10,
                    "champion_selected_top20": rank <= 20,
                }
            )
    return pd.DataFrame(rows)


def _write_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE market_regime_daily (dt DATE, regime_id TEXT)")
        frame = pd.DataFrame(
            {
                "dt": ["2025-01-15", "2025-02-15", "2025-03-15"],
                "regime_id": ["neutral_range", "risk_off_trend", "risk_on_trend"],
            }
        )
        conn.register("regime_df", frame)
        conn.execute("INSERT INTO market_regime_daily SELECT * FROM regime_df")
    finally:
        conn.close()


def _write_context(tmp_path: Path) -> Path:
    source_path = tmp_path / "source.parquet"
    _source_rows().to_parquet(source_path, index=False)
    db_path = tmp_path / "stocks.duckdb"
    _write_db(db_path)
    source_run = tmp_path / "source_run"
    source_run.mkdir(parents=True, exist_ok=True)
    champion_compare = tmp_path / "champion" / "compare.json"
    champion_compare.parent.mkdir(parents=True, exist_ok=True)
    champion_compare.write_text(json.dumps({"schema_version": "champion"}), encoding="utf-8")
    required = {
        "compare.json": {"schema_version": "compare"},
        "family_leaderboard.json": {"schema_version": "family"},
        "session_leaderboard_rollup.json": {"schema_version": "session"},
        "scope_stability_rollup.json": {"schema_version": "scope"},
        "ma_horizon_role_summary.json": {"schema_version": "role"},
        "candidate_decision.ma_buy_probe.json": {"schema_version": "buy"},
        "candidate_decision.ma_sell_probe.json": {"schema_version": "sell"},
        "ma_feature_coverage.json": {"schema_version": "coverage"},
        "evaluation_contract.json": {
            "schema_version": "contract",
            "source_rows_artifact_path": str(source_path),
            "champion_compare_json_path": str(champion_compare),
            "runtime_stock_db_path": str(db_path),
            "ret20_source_mode": "forward_ret_20d",
            "candidate_build_order_mode": "champion_rank_preserve_then_top5_boundary_promotion",
            "artifact_detail_level": "authoritative_full",
            "cost_slippage_config": {"mode": "flat_zero_cost"},
        },
        "run_manifest.json": {"schema_version": "manifest"},
        "_ARTIFACT_COMPLETE.json": {"complete": True},
    }
    for name, payload in required.items():
        (source_run / name).write_text(json.dumps(payload), encoding="utf-8")
    rollup = tmp_path / "ma_final_rollup.json"
    rollup.write_text(
        json.dumps(
            {
                "schema_version": "final",
                "source_role_validation_run": str(source_run),
                "production_registration": False,
                "meemee_reflection": False,
                "champion_artifact_regenerated": False,
            }
        ),
        encoding="utf-8",
    )
    return rollup


def test_rank_bucket_quality_detects_gradient() -> None:
    rows = topk._rank_bucket_rows(topk.ma_probe.load_source_rows_from_frame(_source_rows()))
    gradient = topk._bucket_gradient_score(rows)

    assert gradient["clear_quality_gradient"] is True
    assert rows[0]["mean_ret20"] > rows[1]["mean_ret20"] > rows[2]["mean_ret20"]


def test_random_baseline_and_k_sensitivity_have_required_k_values() -> None:
    source = topk.ma_probe.load_source_rows_from_frame(_source_rows())
    random_rows = topk._random_baseline_rows(source)
    k_rows = topk._topk_rows(source)

    assert {row["top_k"] for row in random_rows} == set(topk.K_VALUES)
    assert {row["top_k"] for row in k_rows} == set(topk.K_VALUES)
    assert next(row for row in random_rows if row["top_k"] == 5)["champion_minus_baseline_mean_ret20"] > 0


def test_run_writes_required_artifacts_and_preserves_boundaries(tmp_path: Path) -> None:
    rollup = _write_context(tmp_path)

    result = topk.run_topk_validity_audit(
        source_final_rollup_json=rollup,
        output_root=tmp_path / "out",
        run_id="audit",
    )

    output_dir = Path(result["output_dir"])
    for name in topk.REQUIRED_JSON:
        assert (output_dir / name).exists(), name
    audit = json.loads((output_dir / "topk_validity_audit.json").read_text(encoding="utf-8"))
    manifest = json.loads((output_dir / "topk_validity_manifest.json").read_text(encoding="utf-8"))
    complete = json.loads((output_dir / "_TOPK_AUDIT_COMPLETE.json").read_text(encoding="utf-8"))

    assert audit["audit_decision"] in {"topk_valid", "topk_partially_valid", "topk_not_valid"}
    assert audit["current_ma_demotion_research_interpretable"] is True
    assert manifest["ranking_logic_changed"] is False
    assert manifest["meemee_reflection"] is False
    assert manifest["production_registration"] is False
    assert complete["complete"] is True
    assert complete["read_back_verification"]["verification"]["required_json_parse"] is True
