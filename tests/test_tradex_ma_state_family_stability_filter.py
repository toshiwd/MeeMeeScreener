from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from scripts.tradex_ma_state_family_stability_filter import (
    CLASSIFICATION_SCHEMA_VERSION,
    DECISION_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    MONTHLY_SCHEMA_VERSION,
    SUMMARY_SCHEMA_VERSION,
    run_state_family_stability_filter,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _position_state_id(
    *,
    c7: str,
    c20: str,
    c60: str,
    stk: str,
    s20: str,
    s60: str,
    st7: str,
    st20: str,
    st60: str,
    cd: str,
    p20: str,
    p60: str,
    vol: str,
) -> str:
    return (
        f"c7={c7}|c20={c20}|c60={c60}|b7=U|b20=U|b60=U|stk={stk}|s7=U|s20={s20}|s60={s60}"
        f"|st7={st7}|st20={st20}|st60={st60}|cd={cd}|p20={p20}|p60={p60}|vol={vol}"
    )


def _family_rows() -> pd.DataFrame:
    specs = [
        {
            "family": "high",
            "position_state_id": _position_state_id(
                c7="G", c20="G", c60="G", stk="U", s20="U", s60="U", st7="0", st20="1", st60="3", cd="LBB", p20="H", p60="H", vol="above_20d_avg"
            ),
            "regimes": [
                ("confirmed_market_regime_daily", "risk_on_trend"),
            ],
            "scores": [0.09, 0.09, 0.09, 0.09],
            "plus5": 1,
            "minus5": 0,
            "plus3": 1,
            "minus3": 0,
            "mfe": 0.12,
            "mae": -0.02,
        },
        {
            "family": "bad",
            "position_state_id": _position_state_id(
                c7="L", c20="L", c60="L", stk="D", s20="D", s60="D", st7="14", st20="14", st60="14", cd="LBR", p20="L", p60="L", vol="below_or_equal_20d_avg"
            ),
            "regimes": [
                ("confirmed_market_regime_daily", "risk_off_trend"),
            ],
            "scores": [-0.09, -0.09, -0.09, -0.09],
            "plus5": 0,
            "minus5": 1,
            "plus3": 0,
            "minus3": 1,
            "mfe": 0.01,
            "mae": -0.12,
        },
        {
            "family": "regime",
            "position_state_id": _position_state_id(
                c7="G", c20="L", c60="G", stk="M", s20="U", s60="D", st7="3", st20="7", st60="14", cd="U", p20="H", p60="L", vol="above_20d_avg"
            ),
            "regimes": [
                ("confirmed_market_regime_daily", "risk_on_range"),
                ("provisional_regime_proxy", "neutral_range"),
            ],
            "scores": [0.02, 0.02, -0.02, -0.02],
            "plus5": 1,
            "minus5": 0,
            "plus3": 1,
            "minus3": 0,
            "mfe": 0.05,
            "mae": -0.04,
        },
        {
            "family": "neutral",
            "position_state_id": _position_state_id(
                c7="G", c20="G", c60="L", stk="U", s20="U", s60="U", st7="1", st20="3", st60="7", cd="U", p20="M", p60="M", vol="above_20d_avg"
            ),
            "regimes": [
                ("confirmed_market_regime_daily", "neutral_range"),
            ],
            "scores": [0.0, 0.0, 0.0, 0.0],
            "plus5": 1,
            "minus5": 0,
            "plus3": 1,
            "minus3": 0,
            "mfe": 0.03,
            "mae": -0.03,
        },
        {
            "family": "sparse",
            "position_state_id": _position_state_id(
                c7="L", c20="G", c60="G", stk="M", s20="D", s60="U", st7="0", st20="1", st60="1", cd="U", p20="L", p60="H", vol="below_or_equal_20d_avg"
            ),
            "regimes": [
                ("confirmed_market_regime_daily", "high_vol_chaos"),
            ],
            "scores": [0.03, -0.03],
            "plus5": [1, 0],
            "minus5": [0, 1],
            "plus3": [1, 0],
            "minus3": [0, 1],
            "mfe": [0.04, 0.02],
            "mae": [-0.02, -0.05],
        },
    ]

    rows: list[dict[str, object]] = []
    trade_dates = [20240105, 20240205, 20240305, 20240405]
    codes = ["AAA", "BBB"]

    for spec in specs:
        scores = spec["scores"]
        plus5_values = spec["plus5"] if isinstance(spec["plus5"], list) else [spec["plus5"]] * len(scores)
        minus5_values = spec["minus5"] if isinstance(spec["minus5"], list) else [spec["minus5"]] * len(scores)
        plus3_values = spec["plus3"] if isinstance(spec["plus3"], list) else [spec["plus3"]] * len(scores)
        minus3_values = spec["minus3"] if isinstance(spec["minus3"], list) else [spec["minus3"]] * len(scores)
        mfe_values = spec["mfe"] if isinstance(spec["mfe"], list) else [spec["mfe"]] * len(scores)
        mae_values = spec["mae"] if isinstance(spec["mae"], list) else [spec["mae"]] * len(scores)
        for idx, score in enumerate(scores):
            regime_source, regime_label = spec["regimes"][idx % len(spec["regimes"])]
            rows.append(
                {
                    "code": codes[idx % len(codes)],
                    "trade_date": trade_dates[idx],
                    "position_state_id": spec["position_state_id"],
                    "regime_source": regime_source,
                    "regime_label": regime_label,
                    "entry_next_open": 100.0 + idx,
                    "entry_day_close": 100.2 + idx,
                    "forward_window_days": 20,
                    "candle_state_code": spec["position_state_id"].split("|")[13].split("=")[1],
                    "volume_condition": "above_20d_avg" if "above" in spec["position_state_id"] else "below_or_equal_20d_avg",
                    "forward_ret_3d": float(score / 3.0),
                    "forward_ret_5d": float(score / 2.0),
                    "forward_ret_10d": float(score * 0.75),
                    "forward_ret_20d": float(score),
                    "mfe_20d": float(mfe_values[idx]),
                    "mae_20d": float(mae_values[idx]),
                    "days_to_mfe_20d": idx + 1,
                    "days_to_mae_20d": idx + 2,
                    "days_to_positive_close": idx if score >= 0 else None,
                    "days_to_plus_3pct": 3 if score > 0 else None,
                    "days_to_plus_5pct": 4 if plus5_values[idx] else None,
                    "days_to_minus_3pct": 3 if score < 0 else None,
                    "days_to_minus_5pct": 4 if minus5_values[idx] else None,
                    "hit_plus_5_before_minus_5": int(plus5_values[idx]),
                    "hit_minus_5_before_plus_5": int(minus5_values[idx]),
                    "hit_plus_3_before_minus_3": int(plus3_values[idx]),
                    "hit_minus_3_before_plus_3": int(minus3_values[idx]),
                    "hit_plus_1atr_before_minus_1atr": int(score >= 0),
                    "mfe_atr_20d": float(mfe_values[idx] / 0.02),
                    "mae_atr_20d": float(mae_values[idx] / 0.02),
                    "close_above_entry_days_20d": 20 if score >= 0 else 0,
                    "close_below_entry_days_20d": 0 if score >= 0 else 20,
                    "path_value_score_v1": float(score),
                    "body_norm_atr": 1.5 if score > 0 else 0.4,
                    "upper_wick_ratio": 0.2,
                    "lower_wick_ratio": 0.6 if score > 0 else 0.3,
                    "volume": 1000 + idx * 10,
                }
            )
    return pd.DataFrame(rows)


def _build_source_session(tmp_path: Path) -> Path:
    source_session = tmp_path / "source_session"
    source_session.mkdir(parents=True, exist_ok=True)
    row_df = _family_rows()
    row_db = source_session / "rows.duckdb"
    conn = duckdb.connect(str(row_db))
    try:
        conn.register("row_df", row_df)
        conn.execute("CREATE TABLE rows AS SELECT * FROM row_df")
        conn.execute(f"COPY rows TO '{(source_session / 'position_state_forward_path_rows.parquet').as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        conn.close()

    overall_metrics = {
        "eligible_row_count": int(len(row_df)),
        "confirmed_regime_row_count": int((row_df["regime_source"] == "confirmed_market_regime_daily").sum()),
        "provisional_regime_row_count": int((row_df["regime_source"] == "provisional_regime_proxy").sum()),
        "symbol_count": int(row_df["code"].nunique()),
        "trade_date_count": int(row_df["trade_date"].nunique()),
        "mean_forward_ret_3d": float(row_df["forward_ret_3d"].mean()),
        "mean_forward_ret_5d": float(row_df["forward_ret_5d"].mean()),
        "mean_forward_ret_10d": float(row_df["forward_ret_10d"].mean()),
        "mean_forward_ret_20d": float(row_df["forward_ret_20d"].mean()),
        "mean_mfe_20d": float(row_df["mfe_20d"].mean()),
        "mean_mae_20d": float(row_df["mae_20d"].mean()),
        "mean_path_value_score_v1": float(row_df["path_value_score_v1"].mean()),
        "median_forward_ret_20d": float(row_df["forward_ret_20d"].median()),
        "median_path_value_score_v1": float(row_df["path_value_score_v1"].median()),
        "hit_plus_5_before_minus_5_rate": float(row_df["hit_plus_5_before_minus_5"].mean()),
        "hit_minus_5_before_plus_5_rate": float(row_df["hit_minus_5_before_plus_5"].mean()),
    }

    source_session_id = "source-session-001"
    manifest = {
        "schema_version": "tradex_ma_position_path_research_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "source_db_path": str(source_session / "stocks.duckdb"),
        "output_root": str(source_session),
        "run_mode": "full",
        "limit_symbols": None,
        "date_range": {"start": 20240105, "end": 20240405},
        "overall_metrics": overall_metrics,
        "state_counts": {"total_state_count": 5},
        "sample_rows_count": 0,
        "output_rows_count": int(len(row_df)),
        "no_lookahead_check": {"passed": True},
        "study_status": "confirmed",
        "source_frame_summary": {},
        "output_artifacts": {
            "run_manifest_json": str(source_session / "run_manifest.json"),
            "position_state_value_summary_json": str(source_session / "position_state_value_summary.json"),
            "position_state_value_by_regime_json": str(source_session / "position_state_value_by_regime.json"),
            "position_state_monthly_stability_json": str(source_session / "position_state_monthly_stability.json"),
            "position_state_classification_json": str(source_session / "position_state_classification.json"),
            "ma_candle_position_value_v1_decision_json": str(source_session / "ma_candle_position_value_v1_decision.json"),
            "position_state_forward_path_rows_parquet": str(source_session / "position_state_forward_path_rows.parquet"),
        },
    }
    summary = {
        "schema_version": "tradex_ma_position_value_summary_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "study_status": "confirmed",
        "overall_metrics": overall_metrics,
        "state_counts": {"total_state_count": 5},
        "top_state_lists": {},
    }
    by_regime = {
        "schema_version": "tradex_ma_position_value_by_regime_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "regime_state_row_count": 5,
        "regime_state_summary": [],
    }
    monthly = {
        "schema_version": "tradex_ma_position_monthly_stability_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "monthly_state_row_count": 5,
        "monthly_state_summary": [],
    }
    classification = {
        "schema_version": "tradex_ma_position_classification_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "state_quality_counts": {},
    }
    decision = {
        "schema_version": "tradex_ma_position_value_decision_v1",
        "session_id": source_session_id,
        "generated_at": "2026-04-29T00:00:00+00:00",
        "recommendation": "keep",
    }

    _write_json(source_session / "run_manifest.json", manifest)
    _write_json(source_session / "position_state_value_summary.json", summary)
    _write_json(source_session / "position_state_value_by_regime.json", by_regime)
    _write_json(source_session / "position_state_monthly_stability.json", monthly)
    _write_json(source_session / "position_state_classification.json", classification)
    _write_json(source_session / "ma_candle_position_value_v1_decision.json", decision)
    return source_session


def test_tradex_ma_state_family_stability_filter_builds_summary_only_family_session(tmp_path: Path) -> None:
    source_session = _build_source_session(tmp_path)
    output_root = tmp_path / "family_filter_output"

    result = run_state_family_stability_filter(
        source_session=source_session,
        output_root=output_root,
        min_sample_count=4,
        min_unique_symbol_count=2,
        min_month_count=4,
        limit_families=10,
    )

    session_dir = Path(result["session_dir"])
    summary_path = Path(result["summary_path"])
    by_regime_path = Path(result["by_regime_path"])
    monthly_path = Path(result["monthly_stability_path"])
    classification_path = Path(result["classification_path"])
    decision_path = Path(result["decision_path"])
    manifest_path = Path(result["manifest_path"])
    detail_path = Path(result["detail_path"])

    assert session_dir.exists()
    assert (session_dir / "_ARTIFACT_COMPLETE.json").exists()
    for path in (summary_path, by_regime_path, monthly_path, classification_path, decision_path, manifest_path, detail_path):
        assert path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    by_regime = json.loads(by_regime_path.read_text(encoding="utf-8"))
    monthly = json.loads(monthly_path.read_text(encoding="utf-8"))
    classification = json.loads(classification_path.read_text(encoding="utf-8"))
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert summary["schema_version"] == SUMMARY_SCHEMA_VERSION
    assert by_regime["schema_version"] == "tradex_ma_state_family_by_regime_v1"
    assert monthly["schema_version"] == MONTHLY_SCHEMA_VERSION
    assert classification["schema_version"] == CLASSIFICATION_SCHEMA_VERSION
    assert decision["schema_version"] == DECISION_SCHEMA_VERSION
    assert manifest["schema_version"] == MANIFEST_SCHEMA_VERSION

    assert manifest["source_session_id"] == "source-session-001"
    assert decision["source_session_id"] == "source-session-001"
    assert summary["source_session_id"] == "source-session-001"

    parquet_frame = pd.read_parquet(detail_path)
    assert not parquet_frame.empty
    assert {"code", "trade_date", "position_state_id", "regime_label", "regime_source", "forward_ret_20d", "path_value_score_v1", "state_family_id"}.issubset(parquet_frame.columns)

    family_counts = result["family_summary"]["family_classification"].value_counts().to_dict()
    assert family_counts == {
        "stable_high_value_family": 1,
        "stable_bad_pick_family": 1,
        "regime_dependent_family": 1,
        "neutral_family": 1,
        "unstable_or_sparse_family": 1,
    }
    assert result["decision"]["recommendation"] == "hold"
    assert manifest["no_lookahead_inherited"] is True
    assert manifest["output_artifacts"]["state_family_rows_parquet"].endswith("state_family_rows.parquet")
    assert manifest["output_artifacts"]["state_family_filter_v1_decision_json"].endswith("state_family_filter_v1_decision.json")
    assert decision["recommendation"] == "hold"

