from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "actionable_liquidity_turnover_contract_v1"
DEFAULT_ASOF_ROOT = Path(r"G:\Tradex\asof_positive_selection_score_v1\20260525T134008Z-asof-positive-selection-score-v1")
DEFAULT_SOURCE_DB = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\actionable_liquidity_turnover_contract_v1")

OFFLINE_OUTCOME_COLUMNS = [
    "ret5",
    "ret20",
    "winner_ret20_gt_10pct",
    "bad_ret20_lt_minus_5pct",
    "severe_ret20_lt_minus_10pct",
]
REQUIRED_ARTIFACTS = (
    "liquidity_turnover_summary.json",
    "liquidity_turnover_rows.parquet",
    "liquidity_turnover_rows_sample.csv",
    "liquidity_turnover_contract.json",
    "source_feasibility_audit.json",
    "feature_contract.json",
    "liquidity_bucket_metrics.json",
    "offline_outcome_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_asof_rows(asof_root: Path) -> pd.DataFrame:
    path = asof_root / "asof_positive_selection_score_rows.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


def load_turnover_features(source_db: Path, min_ymd: int, max_ymd: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        query = """
            SELECT
                CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) AS as_of_date,
                CAST(code AS VARCHAR) AS code,
                turnover20,
                turnover_z20,
                vol_ratio5_20,
                available_at,
                source_presence_flag
            FROM feature_frame_daily
            WHERE CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER) BETWEEN ? AND ?
        """
        return con.execute(query, [min_ymd, max_ymd]).fetchdf()
    finally:
        con.close()


def source_feasibility(turnover: pd.DataFrame, asof_rows: pd.DataFrame) -> dict[str, Any]:
    merged_keys = asof_rows[["as_of_date", "code"]].merge(turnover[["as_of_date", "code"]], on=["as_of_date", "code"], how="left", indicator=True)
    coverage = float((merged_keys["_merge"] == "both").mean()) if not merged_keys.empty else 0.0
    available_at_safe = True
    if "available_at" in turnover.columns and not turnover.empty:
        available_dates = pd.to_datetime(turnover["available_at"], errors="coerce").dt.strftime("%Y%m%d")
        valid = pd.to_numeric(available_dates, errors="coerce")
        available_at_safe = bool((valid <= turnover["as_of_date"]).fillna(False).mean() > 0.999)
    return {
        "axis_id": AXIS_ID,
        "source_table": "feature_frame_daily",
        "available_fields": [col for col in ["turnover20", "turnover_z20", "vol_ratio5_20", "available_at", "source_presence_flag"] if col in turnover.columns],
        "classification": "available_actionable_point_in_time" if coverage >= 0.95 and available_at_safe else "available_but_not_actionable",
        "row_coverage_rate": coverage,
        "available_at_safe": available_at_safe,
        "turnover_row_count": int(len(turnover)),
        "asof_row_count": int(len(asof_rows)),
        "research_fallback_used": False,
    }


def build_liquidity_flags(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["turnover20_available_flag"] = out["turnover20"].notna()
    out["low_turnover20_pctile_by_date"] = pd.to_numeric(out["turnover20"], errors="coerce").groupby(out["as_of_date"]).rank(pct=True, ascending=True)
    out["low_turnover_risk_flag"] = out["low_turnover20_pctile_by_date"] <= 0.20
    out["turnover_z20_risk_flag"] = pd.to_numeric(out["turnover_z20"], errors="coerce") <= -0.75
    out["volume_ratio_risk_flag"] = pd.to_numeric(out["volume_vs_20d_avg"], errors="coerce") < 0.65
    out["actionable_liquidity_turnover_risk_flag"] = out[
        ["low_turnover_risk_flag", "turnover_z20_risk_flag", "volume_ratio_risk_flag"]
    ].fillna(False).any(axis=1)
    out["actionable_liquidity_turnover_score_v1"] = (
        0.45 * out["low_turnover_risk_flag"].fillna(False).astype(float)
        + 0.35 * out["turnover_z20_risk_flag"].fillna(False).astype(float)
        + 0.20 * out["volume_ratio_risk_flag"].fillna(False).astype(float)
    )
    out["liquidity_bucket"] = out["actionable_liquidity_turnover_score_v1"].map(
        lambda x: "liquidity_high_risk" if x >= 0.55 else ("liquidity_medium_risk" if x > 0 else "liquidity_low_risk")
    )
    reasons = []
    for low, z, ratio in zip(out["low_turnover_risk_flag"], out["turnover_z20_risk_flag"], out["volume_ratio_risk_flag"]):
        parts = []
        if bool(low):
            parts.append("low_turnover20_by_date")
        if bool(z):
            parts.append("low_turnover_z20")
        if bool(ratio):
            parts.append("low_volume_vs_20d_avg")
        reasons.append("|".join(parts))
    out["liquidity_risk_reason_codes"] = reasons
    return out


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame[col], errors="coerce").dropna() if col in frame else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def metric(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if not frame.empty else 0,
        "code_count": int(frame["code"].nunique()) if not frame.empty else 0,
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "winner_rate_ret20_gt_10pct": _rate(frame["winner_ret20_gt_10pct"]) if "winner_ret20_gt_10pct" in frame else None,
        "bad_rate_ret20_lt_minus_5pct": _rate(frame["bad_ret20_lt_minus_5pct"]) if "bad_ret20_lt_minus_5pct" in frame else None,
        "severe_rate_ret20_lt_minus_10pct": _rate(frame["severe_ret20_lt_minus_10pct"]) if "severe_ret20_lt_minus_10pct" in frame else None,
        "outcome_coverage_rate": float(frame["ret20"].notna().mean()) if "ret20" in frame and not frame.empty else None,
    }


def bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {bucket: metric(rows[rows["liquidity_bucket"] == bucket]) for bucket in ["liquidity_low_risk", "liquidity_medium_risk", "liquidity_high_risk"]}


def decide(feasibility: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str, list[str]]:
    if feasibility["classification"] != "available_actionable_point_in_time":
        return "blocked_missing_actionable_liquidity_turnover_contract", "BLOCKED", ["turnover_source_not_point_in_time_actionable_or_coverage_insufficient"]
    low = metrics["liquidity_low_risk"]
    high = metrics["liquidity_high_risk"]
    low_bad = low.get("bad_rate_ret20_lt_minus_5pct") or 0.0
    high_bad = high.get("bad_rate_ret20_lt_minus_5pct") or 0.0
    low_severe = low.get("severe_rate_ret20_lt_minus_10pct") or 0.0
    high_severe = high.get("severe_rate_ret20_lt_minus_10pct") or 0.0
    separates = high_bad >= low_bad + 0.02 or high_severe >= low_severe + 0.01
    if separates:
        return "actionable_liquidity_turnover_contract_ready_for_risk_integration", "KEEP", ["point_in_time_turnover_contract_available_and_risk_buckets_separate_downside"]
    return "actionable_liquidity_turnover_no_risk_edge", "DROP", ["turnover_contract_available_but_risk_buckets_do_not_separate_downside"]


def feature_contract() -> dict[str, Any]:
    fields: dict[str, Any] = {
        "as_of_date": {"classification": "identifier"},
        "code": {"classification": "identifier"},
        "turnover20": {"classification": "available_actionable_point_in_time"},
        "turnover_z20": {"classification": "available_actionable_point_in_time"},
        "vol_ratio5_20": {"classification": "available_actionable_point_in_time"},
        "volume_vs_20d_avg": {"classification": "available_proxy_only"},
        "low_turnover20_pctile_by_date": {"classification": "point_in_time_feature"},
        "low_turnover_risk_flag": {"classification": "point_in_time_feature"},
        "turnover_z20_risk_flag": {"classification": "point_in_time_feature"},
        "volume_ratio_risk_flag": {"classification": "point_in_time_feature"},
        "actionable_liquidity_turnover_risk_flag": {"classification": "point_in_time_feature"},
        "actionable_liquidity_turnover_score_v1": {"classification": "point_in_time_feature"},
        "liquidity_bucket": {"classification": "point_in_time_feature"},
        "liquidity_risk_reason_codes": {"classification": "point_in_time_feature"},
        "ret20_derived_tags": {"classification": "forbidden_future_leak"},
    }
    for col in OFFLINE_OUTCOME_COLUMNS:
        fields[col] = {"classification": "offline_outcome_only"}
    return {"axis_id": AXIS_ID, "fields": fields}


def output_columns(rows: pd.DataFrame) -> list[str]:
    wanted = [
        "as_of_date",
        "code",
        "turnover20",
        "turnover_z20",
        "vol_ratio5_20",
        "volume_vs_20d_avg",
        "low_turnover20_pctile_by_date",
        "low_turnover_risk_flag",
        "turnover_z20_risk_flag",
        "volume_ratio_risk_flag",
        "actionable_liquidity_turnover_risk_flag",
        "actionable_liquidity_turnover_score_v1",
        "liquidity_bucket",
        "liquidity_risk_reason_codes",
        "available_at",
        "source_presence_flag",
        *OFFLINE_OUTCOME_COLUMNS,
    ]
    return [col for col in wanted if col in rows.columns]


def run(asof_root: Path = DEFAULT_ASOF_ROOT, source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    asof = load_asof_rows(asof_root)
    min_ymd, max_ymd = int(asof["as_of_date"].min()), int(asof["as_of_date"].max())
    turnover = load_turnover_features(source_db, min_ymd, max_ymd)
    feasibility = source_feasibility(turnover, asof)
    merged = asof.merge(turnover, on=["as_of_date", "code"], how="left")
    rows = build_liquidity_flags(merged)
    metrics = bucket_metrics(rows)
    decision, decision_class, reasons = decide(feasibility, metrics)
    out = output_root / f"{_now_tag()}-actionable-liquidity-turnover-contract-v1"
    out.mkdir(parents=True, exist_ok=True)

    cols = output_columns(rows)
    rows[cols].to_parquet(out / "liquidity_turnover_rows.parquet", index=False)
    rows[cols].head(25000).to_csv(out / "liquidity_turnover_rows_sample.csv", index=False)
    contract = {
        "axis_id": AXIS_ID,
        "contract_id": "actionable_liquidity_turnover_contract_v1",
        "diagnostic_only": True,
        "actionable_contract_complete": decision_class == "KEEP",
        "source_table": "feature_frame_daily",
        "source_fields": ["turnover20", "turnover_z20", "vol_ratio5_20", "available_at"],
        "risk_fields": [
            "actionable_liquidity_turnover_risk_flag",
            "actionable_liquidity_turnover_score_v1",
            "liquidity_bucket",
            "liquidity_risk_reason_codes",
        ],
        "forbidden_features": ["ret5", "ret10", "ret20", "future outcomes"],
    }
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "row_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()),
        "code_count": int(rows["code"].nunique()),
        "feasibility_classification": feasibility["classification"],
        "liquidity_bucket_metrics": metrics,
    }
    no_lookahead = {
        "audit_result": "pass" if feasibility["available_at_safe"] else "blocked",
        "no_lookahead_pass": bool(feasibility["available_at_safe"]),
        "available_at_checked": True,
        "risk_flags_use_point_in_time_features_only": True,
        "offline_outcomes_used_in_risk_flags": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }
    source_coverage = {
        "axis_id": AXIS_ID,
        "asof_input_root": str(asof_root),
        "source_db": str(source_db),
        "row_count": int(len(rows)),
        "date_count": int(rows["as_of_date"].nunique()),
        "code_count": int(rows["code"].nunique()),
        "turnover_join_coverage_rate": feasibility["row_coverage_rate"],
        "research_fallback_used": False,
    }
    offline_audit = {
        "outcomes_are_offline_only": True,
        "outcome_columns": OFFLINE_OUTCOME_COLUMNS,
        "outcome_coverage_rate": float(rows["ret20"].notna().mean()) if "ret20" in rows else 0.0,
        "liquidity_bucket_metrics_reference": "liquidity_bucket_metrics.json",
    }
    research_decision = {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
        "research_fallback_used": False,
    }
    _write_json(out / "liquidity_turnover_summary.json", summary)
    _write_json(out / "liquidity_turnover_contract.json", contract)
    _write_json(out / "source_feasibility_audit.json", feasibility)
    _write_json(out / "feature_contract.json", feature_contract())
    _write_json(out / "liquidity_bucket_metrics.json", metrics)
    _write_json(out / "offline_outcome_audit.json", offline_audit)
    _write_json(out / "no_lookahead_audit.json", no_lookahead)
    _write_json(out / "source_coverage.json", source_coverage)
    _write_json(out / "research_decision.json", research_decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asof-root", type=Path, default=DEFAULT_ASOF_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.asof_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
