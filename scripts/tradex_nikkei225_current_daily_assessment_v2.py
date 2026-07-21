from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd


AXIS_ID = "tradex_nikkei225_current_daily_assessment_v2"
HORIZONS = (1, 3, 5, 10)
REFERENCE_YMD = 20260713
EFFECT_SIDES = {"SELL_ADDITION", "SELL_DEDUCTION", "REBOUND_RISK"}
STATE_COLUMNS = (
    "ma20_structure_state",
    "ma7_sequence_state",
    "support_transition_state",
    "compression_state",
    "lower_rejection_state",
    "stretch_state",
    "pressure_state",
)
TF_COLUMNS = (
    "W-SUN_ret_lag0",
    "W-SUN_signed_body_lag0",
    "W-SUN_upper_wick_atr_lag0",
    "W-SUN_lower_wick_atr_lag0",
    "W-SUN_close_pos_lag0",
    "W-SUN_dist_ma20_atr_lag0",
    "W-SUN_volume_pace_lag0",
    "W-SUN_completion_ratio_lag0",
    "W-SUN_ret_lag1",
    "W-SUN_signed_body_lag1",
    "W-SUN_close_pos_lag1",
    "W-SUN_dist_ma20_atr_lag1",
    "M_ret_lag0",
    "M_signed_body_lag0",
    "M_upper_wick_atr_lag0",
    "M_lower_wick_atr_lag0",
    "M_close_pos_lag0",
    "M_dist_ma20_atr_lag0",
    "M_volume_pace_lag0",
    "M_completion_ratio_lag0",
    "M_ret_lag1",
    "M_signed_body_lag1",
    "M_close_pos_lag1",
    "M_dist_ma20_atr_lag1",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _schema_hash(frame: pd.DataFrame) -> str:
    contract = [(str(column), str(dtype)) for column, dtype in frame.dtypes.items()]
    raw = json.dumps(contract, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _native(value: Any) -> Any:
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    return value


def _object(row: pd.Series, columns: Iterable[str]) -> dict[str, Any]:
    return {column: _native(row[column]) for column in columns}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _resolve_parquet(path: Path, filename: str) -> Path:
    resolved = path / filename if path.is_dir() else path
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    return resolved.resolve()


def _latest(path: Path, reference_ymd: int, columns: list[str] | None = None) -> pd.DataFrame:
    projection = "*" if columns is None else ",".join(f'"{column}"' for column in columns)
    connection = duckdb.connect()
    try:
        return connection.execute(
            f"SELECT {projection} FROM read_parquet(?) WHERE ymd=? ORDER BY code",
            [str(path), reference_ymd],
        ).fetchdf()
    finally:
        connection.close()


def _assert_unique(frame: pd.DataFrame, name: str) -> None:
    duplicates = int(frame.duplicated(["code", "ymd"]).sum())
    if duplicates:
        raise ValueError(f"{name} has {duplicates} duplicate code/ymd rows")


def _add_evidence(
    records: list[dict[str, Any]],
    row: pd.Series,
    evidence_id: str,
    category: str,
    effect_side: str,
    value: Any,
    observation: str,
) -> None:
    if effect_side not in EFFECT_SIDES:
        raise ValueError(f"invalid effect_side: {effect_side}")
    records.append(
        {
            "code": str(row.code),
            "reference_ymd": int(row.ymd),
            "evidence_id": evidence_id,
            "category": category,
            "effect_side": effect_side,
            # Keep one stable physical type across numeric and categorical evidence.
            "observation_value_json": _json(_native(value)),
            "observation": observation,
            "evidence_status": "diagnostic_only_unvalidated",
            "source_research_decision": "not_approved_for_action",
            "model_quality_gate_pass": False,
            "actionable_score_contribution": 0.0,
            "ranking_score_effect": 0.0,
        }
    )


def _evidence(row: pd.Series) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    if row.ret10 < 0:
        _add_evidence(records, row, "daily20.ret10_negative", "daily20", "SELL_ADDITION", row.ret10, "10日騰落率が負")
    elif row.ret10 > 0:
        _add_evidence(records, row, "daily20.ret10_positive", "daily20", "SELL_DEDUCTION", row.ret10, "10日騰落率が正")
    if row.bear_count5 >= 3:
        _add_evidence(records, row, "daily20.bear_majority5", "daily20", "SELL_ADDITION", row.bear_count5, "直近5本で陰線優勢")

    signed_body = (row.c - row.o) / row.atr14 if row.atr14 and np.isfinite(row.atr14) else np.nan
    if signed_body < -0.25:
        _add_evidence(records, row, "candle.bear_body", "candle", "SELL_ADDITION", signed_body, "陰線実体がATR比で大きい")
    elif signed_body > 0.25:
        _add_evidence(records, row, "candle.bull_body", "candle", "SELL_DEDUCTION", signed_body, "陽線実体がATR比で大きい")
    if row.upper_wick_ratio >= 0.35:
        _add_evidence(records, row, "candle.upper_wick_supply", "candle", "SELL_ADDITION", row.upper_wick_ratio, "上ヒゲ比率が高い")
    if row.lower_wick_ratio >= 0.35:
        _add_evidence(records, row, "candle.lower_wick_rejection", "candle", "REBOUND_RISK", row.lower_wick_ratio, "下ヒゲ比率が高い")

    if row.dist_ma20_atr < 0:
        _add_evidence(records, row, "ma.below_ma20", "ma", "SELL_ADDITION", row.dist_ma20_atr, "終値が20MA未満")
    else:
        _add_evidence(records, row, "ma.above_ma20", "ma", "SELL_DEDUCTION", row.dist_ma20_atr, "終値が20MA以上")
    if row.cross_ma20 == 1 or row.cross_ma7 == 1:
        _add_evidence(records, row, "ma.fresh_break", "ma", "SELL_ADDITION", int(row.cross_ma20) + int(row.cross_ma7), "7MAまたは20MAを当日割れ")
    if row.reclaim_ma20 == 1 or row.reclaim_ma7 == 1:
        _add_evidence(records, row, "ma.fresh_reclaim", "ma", "SELL_DEDUCTION", int(row.reclaim_ma20) + int(row.reclaim_ma7), "7MAまたは20MAを当日回復")
    if row.ma20_slope5_atr < 0:
        _add_evidence(records, row, "ma.ma20_slope_down", "ma", "SELL_ADDITION", row.ma20_slope5_atr, "20MAの5日傾きが負")

    if row.support_break == 1:
        _add_evidence(records, row, "support_resistance.support_break", "support_resistance", "SELL_ADDITION", row.support_break_depth_atr, "20本支持を終値で割れ")
    if row.pos20 <= 0.15:
        _add_evidence(records, row, "support_resistance.low_zone", "support_resistance", "REBOUND_RISK", row.pos20, "20本レンジ下端に接近")
    elif row.pos20 >= 0.85:
        _add_evidence(records, row, "support_resistance.high_zone", "support_resistance", "SELL_ADDITION", row.pos20, "20本レンジ上端で供給リスク")

    if row.volume_ratio20 >= 1.25 and signed_body < 0:
        _add_evidence(records, row, "volume.bear_expansion", "volume", "SELL_ADDITION", row.volume_ratio20, "陰線日に20日比で出来高増")
    elif row.volume_ratio20 >= 1.25 and signed_body > 0:
        _add_evidence(records, row, "volume.bull_expansion", "volume", "SELL_DEDUCTION", row.volume_ratio20, "陽線日に20日比で出来高増")

    if row.compression_state in {"forming", "prolonged"}:
        side = "SELL_ADDITION" if row.pressure_state == "dominant" else "SELL_DEDUCTION"
        _add_evidence(records, row, "sideways.compression_pressure", "sideways", side, row.compression_state, f"持合い={row.compression_state},圧力={row.pressure_state}")

    if row.oversold_risk == 1 or row.stretch_state != "normal" or row.dist_ma7_atr <= -1.5:
        _add_evidence(records, row, "stretch.oversold", "stretch", "REBOUND_RISK", row.dist_ma7_atr, f"乖離状態={row.stretch_state}")
    if row.lower_rejection_state == "candidate":
        _add_evidence(records, row, "stretch.lower_rejection_candidate", "stretch", "REBOUND_RISK", row.lower_rejection_count5, "下値拒否候補")

    weekly_body = row["W-SUN_signed_body_lag0"]
    monthly_body = row["M_signed_body_lag0"]
    if pd.notna(weekly_body):
        side = "SELL_ADDITION" if weekly_body < 0 else "SELL_DEDUCTION"
        _add_evidence(records, row, "higher_tf.weekly_body", "higher_tf", side, weekly_body, "当週の進行中実体")
    if pd.notna(row["W-SUN_lower_wick_atr_lag0"]) and row["W-SUN_lower_wick_atr_lag0"] >= 0.5:
        _add_evidence(records, row, "higher_tf.weekly_lower_wick", "higher_tf", "REBOUND_RISK", row["W-SUN_lower_wick_atr_lag0"], "当週の下ヒゲが週ATR比で大きい")
    if pd.notna(monthly_body):
        side = "SELL_ADDITION" if monthly_body < 0 else "SELL_DEDUCTION"
        _add_evidence(records, row, "higher_tf.monthly_body", "higher_tf", side, monthly_body, "当月の進行中実体")
    if pd.notna(row["M_lower_wick_atr_lag0"]) and row["M_lower_wick_atr_lag0"] >= 0.5:
        _add_evidence(records, row, "higher_tf.monthly_lower_wick", "higher_tf", "REBOUND_RISK", row["M_lower_wick_atr_lag0"], "当月の下ヒゲが月ATR比で大きい")
    return records


def run(
    feature_parquet: Path,
    state_parquet: Path,
    exact_tf_artifact: Path,
    irregular_artifact: Path,
    output_root: Path,
    reference_ymd: int = REFERENCE_YMD,
) -> Path:
    feature_parquet = feature_parquet.resolve()
    state_parquet = state_parquet.resolve()
    tf_parquet = _resolve_parquet(exact_tf_artifact, "exact_multitimeframe_features.parquet")
    irregular_parquet = _resolve_parquet(irregular_artifact, "irregular_event_three_state_ledger.parquet")
    for path in (feature_parquet, state_parquet):
        if not path.is_file():
            raise FileNotFoundError(path)

    feature = _latest(feature_parquet, reference_ymd)
    states = _latest(state_parquet, reference_ymd, ["code", "ymd", *STATE_COLUMNS])
    tf = _latest(tf_parquet, reference_ymd, ["code", "ymd", *TF_COLUMNS])
    irregular_columns = [
        "code", "ymd", "decision_cutoff", "scheduled_snapshot_status", "tdnet_system_status",
        "event_mask_union", "event_exclude", "event_eligible", "reason_bitset",
        *[f"mask_h{horizon}" for horizon in HORIZONS],
    ]
    irregular = _latest(irregular_parquet, reference_ymd, irregular_columns)
    sources = {"feature": feature, "state": states, "exact_tf": tf, "irregular": irregular}
    for name, frame in sources.items():
        _assert_unique(frame, name)
        if frame.empty:
            raise ValueError(f"{name} has no rows for {reference_ymd}")
    code_sets = {name: set(frame.code.astype(str)) for name, frame in sources.items()}
    if len({frozenset(codes) for codes in code_sets.values()}) != 1:
        raise ValueError({name: len(codes) for name, codes in code_sets.items()})

    merged = feature.merge(states, on=["code", "ymd"], how="left", validate="one_to_one")
    merged = merged.merge(tf, on=["code", "ymd"], how="left", validate="one_to_one")
    merged = merged.merge(irregular, on=["code", "ymd"], how="left", validate="one_to_one")
    if len(merged) != len(feature) or merged.code.nunique() != len(feature):
        raise AssertionError("join cardinality changed")

    evidence_records: list[dict[str, Any]] = []
    technical_by_code: dict[str, dict[str, Any]] = {}
    for _, row in merged.iterrows():
        evidence_records.extend(_evidence(row))
        technical_by_code[str(row.code)] = {
            "daily20": _object(row, ["ret3", "ret5", "ret10", "pre_ret10", "pos20", "range20_pct", "bear_count5", "bear_body5_atr", "upper_supply_count5", "lower_rejection_count5", "low_close_count3"]),
            "candle": _object(row, ["o", "h", "l", "c", "atr14", "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "close_pos"]),
            "ma": _object(row, ["ma7", "ma20", "ma60", "ma100", "ma200", "dist_ma7_atr", "dist_ma20_atr", "dist_ma60_atr", "ma7_slope5_atr", "ma20_slope5_atr", "ma60_slope5_atr", "cross_ma7", "cross_ma20", "reclaim_ma7", "reclaim_ma20"]),
            "support_resistance": _object(row, ["support20", "resistance20", "support_break", "support_break_depth_atr", "pos20"]),
            "volume": _object(row, ["v", "vol20", "volume_ratio20"]),
            "sideways": _object(row, ["compression_state", "pressure_state", "range20_pct"]),
            "stretch": _object(row, ["oversold_risk", "stretch_state", "lower_rejection_state", "dist_ma7_atr"]),
            "higher_tf": _object(row, TF_COLUMNS),
            "sequence_states": _object(row, STATE_COLUMNS),
        }

    evidence = pd.DataFrame(evidence_records).sort_values(["code", "category", "evidence_id"]).reset_index(drop=True)
    if evidence.empty:
        raise AssertionError("evidence ledger unexpectedly empty")
    if set(evidence.effect_side.unique()) - EFFECT_SIDES:
        raise AssertionError("effect_side contract violated")
    if "BUY_ADDITION" in set(evidence.effect_side):
        raise AssertionError("BUY_ADDITION is prohibited")
    if (evidence.actionable_score_contribution != 0).any() or (evidence.ranking_score_effect != 0).any():
        raise AssertionError("diagnostic evidence changed an actionable score")

    rows: list[dict[str, Any]] = []
    for _, row in merged.sort_values("code").iterrows():
        technical = technical_by_code[str(row.code)]
        for horizon in HORIZONS:
            irregular_mask = str(row[f"mask_h{horizon}"])
            irregular_suppresses = irregular_mask != "eligible"
            rows.append(
                {
                    "code": str(row.code),
                    "reference_ymd": int(reference_ymd),
                    "horizon": int(horizon),
                    "input_data_status": "confirmed",
                    "provisional_input_used": False,
                    "model_artifact_status": "not_approved",
                    "model_quality_gate_pass": False,
                    "p_down": pd.NA,
                    "p_rebound": pd.NA,
                    "p_neutral": pd.NA,
                    "model_direction": pd.NA,
                    "irregular_mask": irregular_mask,
                    "irregular_reason_bitset": str(row.reason_bitset),
                    "irregular_suppresses_action": irregular_suppresses,
                    "assessment_state": "unjudgeable_model_quality",
                    "short_review_action": False,
                    "rebound_review_action": False,
                    "any_action": False,
                    "actionable_score": 0.0,
                    "ranking_score_effect": 0.0,
                    "ranking_write": False,
                    "buy_addition_count": 0,
                    "daily20_json": _json(technical["daily20"]),
                    "candle_json": _json(technical["candle"]),
                    "ma_json": _json(technical["ma"]),
                    "support_resistance_json": _json(technical["support_resistance"]),
                    "volume_json": _json(technical["volume"]),
                    "sideways_json": _json(technical["sideways"]),
                    "stretch_json": _json(technical["stretch"]),
                    "higher_tf_json": _json(technical["higher_tf"]),
                    "sequence_states_json": _json(technical["sequence_states"]),
                }
            )
    assessment = pd.DataFrame(rows).sort_values(["code", "horizon"]).reset_index(drop=True)
    for column in ("p_down", "p_rebound", "p_neutral"):
        assessment[column] = assessment[column].astype("Float64")
    assessment["model_direction"] = assessment.model_direction.astype("string")

    probability_nonnull = int(assessment[["p_down", "p_rebound", "p_neutral"]].notna().sum().sum())
    direction_nonnull = int(assessment.model_direction.notna().sum())
    action_count = int(assessment[["short_review_action", "rebound_review_action", "any_action"]].sum().sum())
    if probability_nonnull or direction_nonnull or action_count:
        raise AssertionError("quality-fail rows exposed probability, direction, or action")
    if int(assessment.buy_addition_count.sum()) != 0:
        raise AssertionError("BUY_ADDITION count is nonzero")
    if (assessment.ranking_score_effect != 0).any() or assessment.ranking_write.any():
        raise AssertionError("ranking boundary violated")
    if not assessment.irregular_suppresses_action.all():
        raise AssertionError("irregular unknown/blocked rows did not suppress actions")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    csv_path = output / "current_daily_assessment.csv"
    jsonl_path = output / "current_daily_assessment_by_code.jsonl"
    evidence_path = output / "assessment_evidence_child.parquet"
    assessment.to_csv(csv_path, index=False, encoding="utf-8-sig")
    evidence.to_parquet(evidence_path, index=False)

    evidence_group = {code: group.to_dict("records") for code, group in evidence.groupby("code", sort=True)}
    with jsonl_path.open("w", encoding="utf-8", newline="\n") as handle:
        for code, group in assessment.groupby("code", sort=True):
            payload = {
                "code": str(code),
                "reference_ymd": int(reference_ymd),
                "input_contract": {"status": "confirmed", "provisional_input_used": False},
                "technical": technical_by_code[str(code)],
                "horizons": [
                    {key: _native(value) for key, value in item.items() if not key.endswith("_json") and key not in {"code", "reference_ymd"}}
                    for item in group.to_dict("records")
                ],
                "evidence": [{key: _native(value) for key, value in item.items()} for item in evidence_group.get(str(code), [])],
            }
            handle.write(_json(payload) + "\n")

    source_paths = {
        "feature": feature_parquet,
        "state": state_parquet,
        "exact_tf": tf_parquet,
        "irregular": irregular_parquet,
    }
    audit = {
        "schema_version": f"{AXIS_ID}.audit.v1",
        "artifact_role": "authoritative",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "research_phase": "comparison_stabilization",
        "status": "review_only",
        "reference_ymd": int(reference_ymd),
        "confirmed_provisional_contract": {
            "input_data_status": "confirmed",
            "provisional_input_used": False,
            "provisional_input_rows": 0,
        },
        "model_contract": {
            "approved_model_artifact": None,
            "fitting_performed": False,
            "calibration_performed": False,
            "threshold_search_performed": False,
            "quality_gate_pass": False,
            "probabilities_must_be_null": True,
            "model_direction_must_be_null": True,
        },
        "dimensions": {"rows": int(len(assessment)), "codes": int(assessment.code.nunique()), "horizons": list(HORIZONS)},
        "join_verification": {
            "source_rows_at_reference": {name: int(len(frame)) for name, frame in sources.items()},
            "source_unique_code_ymd": {name: not frame.duplicated(["code", "ymd"]).any() for name, frame in sources.items()},
            "code_sets_equal": True,
            "joined_rows": int(len(merged)),
            "joined_unique_code_ymd": not merged.duplicated(["code", "ymd"]).any(),
        },
        "quality_fail_verification": {
            "nonnull_probability_cells": probability_nonnull,
            "nonnull_model_direction_rows": direction_nonnull,
            "action_true_cells": action_count,
            "actionable_score_nonzero_rows": int((assessment.actionable_score != 0).sum()),
            "buy_addition_rows": int(assessment.buy_addition_count.sum()),
            "ranking_effect_nonzero_rows": int((assessment.ranking_score_effect != 0).sum()),
            "ranking_write_rows": int(assessment.ranking_write.sum()),
        },
        "irregular_event_contract": {
            "unknown_is_not_eligible": True,
            "mask_counts": [
                {"horizon": int(horizon), "mask": str(mask), "n": int(count)}
                for (horizon, mask), count in assessment.groupby(["horizon", "irregular_mask"]).size().items()
            ],
            "suppressed_rows": int(assessment.irregular_suppresses_action.sum()),
        },
        "evidence_contract": {
            "rows": int(len(evidence)),
            "allowed_effect_sides": sorted(EFFECT_SIDES),
            "observed_effect_counts": [{"effect_side": str(side), "n": int(count)} for side, count in evidence.effect_side.value_counts().items()],
            "buy_addition_rows": 0,
            "diagnostic_or_dropped_actionable_contribution_nonzero_rows": int((evidence.actionable_score_contribution != 0).sum()),
            "ranking_effect_nonzero_rows": int((evidence.ranking_score_effect != 0).sum()),
        },
        "source_files": {
            name: {"path": str(path), "sha256": _sha256(path), "schema_sha256": _schema_hash(sources[name])}
            for name, path in source_paths.items()
        },
        "output_schema_sha256": {"assessment": _schema_hash(assessment), "evidence": _schema_hash(evidence)},
        "outputs": {
            "flattened_csv": str(csv_path),
            "per_code_jsonl": str(jsonl_path),
            "evidence_child_parquet": str(evidence_path),
        },
        "decision": {
            "candidate_local_decision": "hold_unjudgeable_no_approved_model_and_irregular_unknown",
            "authoritative_rollup_decision": "review_only",
        },
        "boundary": {
            "owner": "TRADEX",
            "meemee_changed": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "ranking_writes": 0,
        },
    }
    audit_path = output / "audit.json"
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    marker = {
        "schema_version": f"{AXIS_ID}.complete.v1",
        "complete": True,
        "audit": str(audit_path),
        "artifact_hashes": {
            path.name: _sha256(path) for path in (csv_path, jsonl_path, evidence_path, audit_path)
        },
    }
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps(marker, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-parquet", required=True, type=Path)
    parser.add_argument("--state-parquet", required=True, type=Path)
    parser.add_argument("--exact-tf-artifact", required=True, type=Path)
    parser.add_argument("--irregular-artifact", required=True, type=Path)
    parser.add_argument("--reference-ymd", type=int, default=REFERENCE_YMD)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_current_daily_assessment_v2"))
    args = parser.parse_args()
    print(run(args.feature_parquet, args.state_parquet, args.exact_tf_artifact, args.irregular_artifact, args.output_root, args.reference_ymd))


if __name__ == "__main__":
    main()
