from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts.tradex_ma_bad_pick_surface_refinement import (
    COMPARE_SCHEMA_VERSION,
    DECISION_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    SHADOW_SCHEMA_VERSION,
    WATERFALL_SCHEMA_VERSION,
    run_bad_pick_surface_refinement,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _build_family_rows() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    specs = [
        {
            "state_family_id": "strict_bad",
            "family_regime_context": "C:risk_off_trend",
            "scores": [-0.30, -0.25, -0.20, -0.15],
            "minus5": [1, 1, 1, 1],
            "plus5": [0, 0, 0, 0],
            "mae": [-0.20, -0.19, -0.18, -0.17],
            "forward20": [-0.08, -0.07, -0.06, -0.05],
        },
        {
            "state_family_id": "relaxed_only",
            "family_regime_context": "C:neutral_range",
            "scores": [-0.40, -0.01, 0.02, 0.03],
            "minus5": [0, 0, 0, 1],
            "plus5": [0, 1, 1, 0],
            "mae": [-0.16, -0.17, -0.05, -0.04],
            "forward20": [0.01, 0.02, -0.01, 0.00],
        },
        {
            "state_family_id": "watch_family",
            "family_regime_context": "C:risk_on_range",
            "scores": [-0.20, -0.10, 0.05, 0.10],
            "minus5": [1, 1, 0, 0],
            "plus5": [0, 0, 1, 1],
            "mae": [-0.11, -0.12, -0.08, -0.07],
            "forward20": [-0.02, -0.01, 0.00, 0.01],
        },
        {
            "state_family_id": "regime_family",
            "family_regime_context": "C:risk_on_trend",
            "alternate_regime_context": "C:risk_off_trend",
            "scores": [0.25, 0.15, -0.20, -0.25],
            "minus5": [1, 0, 1, 0],
            "plus5": [0, 1, 0, 1],
            "mae": [-0.13, -0.15, -0.10, -0.12],
            "forward20": [0.04, -0.03, 0.03, -0.02],
        },
        {
            "state_family_id": "mae_endpoint_family",
            "family_regime_context": "C:risk_on_trend",
            "scores": [-0.22, -0.05, 0.01, 0.06],
            "minus5": [1, 1, 1, 0],
            "plus5": [0, 0, 1, 1],
            "mae": [-0.25, -0.24, -0.22, -0.21],
            "forward20": [0.02, 0.01, -0.01, 0.00],
        },
    ]
    months = [202401, 202402, 202403, 202404]
    codes = ["AAA", "BBB"]
    for spec in specs:
        for idx, trade_month in enumerate(months):
            family_regime_context = spec["family_regime_context"]
            if spec["state_family_id"] == "regime_family" and idx >= 2:
                family_regime_context = spec["alternate_regime_context"]
            rows.append(
                {
                    "state_family_id": spec["state_family_id"],
                    "code": codes[idx % 2],
                    "trade_month": trade_month,
                    "family_regime_context": family_regime_context,
                    "path_value_score_v1": float(spec["scores"][idx]),
                    "forward_ret_3d": float(spec["forward20"][idx] / 3.0),
                    "forward_ret_5d": float(spec["forward20"][idx] / 2.0),
                    "forward_ret_10d": float(spec["forward20"][idx] * 0.75),
                    "forward_ret_20d": float(spec["forward20"][idx]),
                    "mfe_20d": float(abs(spec["scores"][idx]) + 0.05),
                    "mae_20d": float(spec["mae"][idx]),
                    "hit_plus_5_before_minus_5": int(spec["plus5"][idx]),
                    "hit_minus_5_before_plus_5": int(spec["minus5"][idx]),
                }
            )
    return pd.DataFrame(rows)


def _build_source_family_session(tmp_path: Path) -> Path:
    source_session = tmp_path / "family_session"
    source_session.mkdir(parents=True, exist_ok=True)
    source_ma_session = tmp_path / "source_ma_session"
    source_ma_session.mkdir(parents=True, exist_ok=True)

    rows = _build_family_rows()
    db_path = source_session / "source.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        conn.register("rows_df", rows)
        conn.execute("CREATE TABLE rows AS SELECT * FROM rows_df")
        conn.execute(f"COPY rows TO '{(source_session / 'state_family_rows.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        conn.close()

    source_session_id = "family-session-001"
    overall_metrics = {
        "mean_path_value_score_v1": 0.0,
        "median_path_value_score_v1": 0.0,
        "mean_forward_ret_20d": 0.0,
        "mean_mae_20d": -0.05,
        "hit_minus_5_before_plus_5_rate": 0.4,
        "hit_plus_5_before_minus_5_rate": 0.4,
    }
    manifest = {
        "schema_version": "tradex_ma_position_path_research_family_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_session_path": str(source_ma_session),
        "overall_metrics": overall_metrics,
        "output_rows_count": int(len(rows)),
        "no_lookahead_check": {"passed": True},
    }
    source_ma_manifest = {
        "schema_version": "tradex_ma_position_path_research_v1",
        "session_id": "source-ma-session-001",
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_db_path": str(source_ma_session / "stocks.duckdb"),
        "output_root": str(source_ma_session),
        "run_mode": "full",
        "limit_symbols": None,
        "date_range": {"start": 20240101, "end": 20240430},
        "overall_metrics": overall_metrics,
        "state_counts": {"total_state_count": 0},
        "sample_rows_count": 0,
        "output_rows_count": int(len(rows)),
        "no_lookahead_check": {"passed": True},
        "study_status": "confirmed",
        "source_frame_summary": {},
        "output_artifacts": {},
    }
    summary = {
        "schema_version": "tradex_ma_state_family_summary_v1",
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_session_id": source_session_id,
        "source_session_path": str(source_session),
        "baseline_metrics": {
            "mean_path_value_score_v1": 0.0,
            "median_path_value_score_v1": 0.0,
            "plus5_before_minus5_rate": 0.4,
            "minus5_before_plus5_rate": 0.4,
            "bottom15_rate": 0.15,
            "top15_rate": 0.15,
            "bottom15_score_threshold": -0.10,
            "top15_score_threshold": 0.10,
        },
        "family_counts": {"total_families": 5},
        "notes": [],
        "source_artifacts": {},
        "state_id_structure": {},
        "family_definition": {},
        "filter_thresholds": {},
        "top_high_value_families": [],
        "top_bad_pick_families": [],
        "top_regime_dependent_families": [],
        "top_unstable_families": [],
        "top_neutral_families": [],
    }
    by_regime = {
        "schema_version": "tradex_ma_state_family_by_regime_v1",
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_session_id": source_session_id,
        "family_regime_row_count": 6,
        "regime_family_summary": [],
        "top_regime_dependent_families": [],
    }
    monthly = {
        "schema_version": "tradex_ma_state_family_monthly_stability_v1",
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_session_id": source_session_id,
        "family_month_row_count": 5,
        "top_stable_families": [],
        "top_unstable_families": [],
    }
    classification = {
        "schema_version": "tradex_ma_state_family_classification_v1",
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_session_id": source_session_id,
        "state_family_classification_counts": {},
        "examples": {},
    }
    decision = {
        "schema_version": "tradex_ma_state_family_filter_decision_v1",
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_session_id": source_session_id,
        "recommendation": "hold",
    }

    _write_json(source_session / "run_manifest.json", manifest)
    _write_json(source_ma_session / "run_manifest.json", source_ma_manifest)
    _write_json(source_session / "state_family_summary.json", summary)
    _write_json(source_session / "state_family_by_regime.json", by_regime)
    _write_json(source_session / "state_family_monthly_stability.json", monthly)
    _write_json(source_session / "state_family_classification.json", classification)
    _write_json(source_session / "state_family_filter_v1_decision.json", decision)
    return source_session


def test_bad_pick_surface_refinement_creates_summary_only_artifacts(tmp_path: Path) -> None:
    source_session = _build_source_family_session(tmp_path)
    output_root = tmp_path / "bad_pick_refinement"

    result = run_bad_pick_surface_refinement(
        source_family_session=source_session,
        output_root=output_root,
        min_sample_count=4,
        min_unique_symbol_count=2,
        min_month_count=4,
        limit_families=10,
        relax_positive_month_rate=0.50,
        relax_mae_margin=0.01,
    )

    session_dir = Path(result["session_dir"])
    assert session_dir.exists()
    assert (session_dir / "_ARTIFACT_COMPLETE.json").exists()

    for key in [
        "waterfall_path",
        "shadow_path",
        "compare_path",
        "rows_path",
        "decision_path",
        "manifest_path",
    ]:
        assert Path(result[key]).exists()

    waterfall = json.loads(Path(result["waterfall_path"]).read_text(encoding="utf-8"))
    shadow = json.loads(Path(result["shadow_path"]).read_text(encoding="utf-8"))
    compare = json.loads(Path(result["compare_path"]).read_text(encoding="utf-8"))
    decision = json.loads(Path(result["decision_path"]).read_text(encoding="utf-8"))
    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))

    assert waterfall["schema_version"] == WATERFALL_SCHEMA_VERSION
    assert shadow["schema_version"] == SHADOW_SCHEMA_VERSION
    assert compare["schema_version"] == COMPARE_SCHEMA_VERSION
    assert decision["schema_version"] == DECISION_SCHEMA_VERSION
    assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert manifest["source_family_session_id"] == "family-session-001"
    assert manifest["no_lookahead_inherited"] is True
    assert decision["recommendation"] == "drop"
    assert decision["pruning_challenger_justified"] is False

    assert waterfall["stages"][0]["count"] == 5
    assert waterfall["stages"][1]["count"] == 5
    assert waterfall["stages"][6]["count"] == 1
    assert waterfall["stages"][7]["count"] >= 2

    assert compare["counts"]["strict_bad_pick_family_count"] == 1
    assert compare["counts"]["relaxed_bad_pick_family_count"] >= 2
    assert compare["counts"]["delta"] >= 1

    assert shadow["shadow_counts"]["bad_pick_watch_family"] >= 1
    assert shadow["shadow_counts"]["regime_bad_pick_family"] >= 1
    assert shadow["shadow_counts"]["mae_risk_family"] >= 1
    assert shadow["shadow_counts"]["endpoint_bad_pick_family"] >= 1

    parquet_frame = pd.read_parquet(Path(result["rows_path"]))
    assert not parquet_frame.empty
    assert {"state_family_id", "strict_bad_pick_family", "relaxed_bad_pick_family", "bad_pick_watch_family", "regime_bad_pick_family", "mae_risk_family", "endpoint_bad_pick_family"}.issubset(parquet_frame.columns)
