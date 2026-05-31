from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "actionable_event_liquidity_risk_v1"
DEFAULT_ASOF_ROOT = Path(r"G:\Tradex\asof_positive_selection_score_v1\20260525T134008Z-asof-positive-selection-score-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\actionable_event_liquidity_risk_v1")
DEFAULT_SOURCE_DB = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")

OFFLINE_OUTCOME_COLUMNS = {
    "ret5",
    "ret20",
    "winner_ret20_gt_10pct",
    "bad_ret20_lt_minus_5pct",
    "severe_ret20_lt_minus_10pct",
}
PROXY_FEATURE_COLUMNS = [
    "volume_vs_20d_avg",
    "atr14_pct",
    "realized_vol20",
    "gap_up_flag",
    "gap_down_flag",
    "failed_high_flag",
    "weekly_failed_high_flag",
    "upper_wick_ratio",
    "bearish_body_flag",
    "body_ratio",
]
RISK_COLUMNS = [
    "actionable_event_liquidity_risk_v1",
    "event_risk_flag",
    "liquidity_risk_flag",
    "volatility_liquidity_risk_flag",
    "candle_gap_risk_flag",
    "composite_actionable_risk_score_v1",
    "risk_bucket",
    "risk_reason_codes",
    "actionable_contract_complete",
]
REQUIRED_ARTIFACTS = (
    "actionable_event_liquidity_risk_summary.json",
    "actionable_event_liquidity_risk_rows.parquet",
    "actionable_event_liquidity_risk_rows_sample.csv",
    "risk_contract.json",
    "source_feasibility_audit.json",
    "feature_contract.json",
    "risk_flag_metrics.json",
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


def _safe_count(con: duckdb.DuckDBPyConnection, table: str) -> int | None:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except Exception:
        return None


def _safe_columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    try:
        return [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()]
    except Exception:
        return []


def source_feasibility(source_db: Path, source_columns: set[str]) -> dict[str, Any]:
    db_tables: dict[str, Any] = {}
    if source_db.exists():
        con = duckdb.connect(str(source_db), read_only=True)
        try:
            for table in [
                "earnings_planned",
                "ex_rights",
                "tdnet_disclosures",
                "tdnet_disclosure_features",
                "feature_frame_daily",
                "ml_feature_daily",
                "daily_bars",
            ]:
                db_tables[table] = {
                    "exists": bool(_safe_columns(con, table)),
                    "row_count": _safe_count(con, table),
                    "columns": _safe_columns(con, table),
                }
        finally:
            con.close()
    groups = {
        "volume_turnover_proxies": {
            "classification": "available_proxy_only" if {"volume_vs_20d_avg"} & source_columns else "unavailable",
            "fields": sorted({"volume_vs_20d_avg", "turnover20", "turnover_z20"} & source_columns),
            "note": "volume ratio is point-in-time proxy; not actionable venue liquidity/event source",
        },
        "price_liquidity_proxies": {
            "classification": "available_proxy_only" if {"atr14_pct", "realized_vol20"} & source_columns else "unavailable",
            "fields": sorted({"atr14_pct", "realized_vol20"} & source_columns),
            "note": "ATR/realized volatility proxy only",
        },
        "low_or_abnormal_volume_flags": {
            "classification": "available_proxy_only" if "volume_vs_20d_avg" in source_columns else "unavailable",
            "fields": ["volume_vs_20d_avg"] if "volume_vs_20d_avg" in source_columns else [],
        },
        "volatility_atr_risk": {
            "classification": "available_proxy_only" if {"atr14_pct", "realized_vol20"} & source_columns else "unavailable",
            "fields": sorted({"atr14_pct", "realized_vol20"} & source_columns),
        },
        "gap_risk": {
            "classification": "available_proxy_only" if {"gap_up_flag", "gap_down_flag"} & source_columns else "unavailable",
            "fields": sorted({"gap_up_flag", "gap_down_flag"} & source_columns),
        },
        "failed_high_wick_bearish_candle_risk": {
            "classification": "available_proxy_only" if {"failed_high_flag", "upper_wick_ratio", "bearish_body_flag"} & source_columns else "unavailable",
            "fields": sorted({"failed_high_flag", "weekly_failed_high_flag", "upper_wick_ratio", "bearish_body_flag"} & source_columns),
        },
        "earnings_date_planned_disclosure": {
            "classification": "available_but_not_actionable" if (db_tables.get("earnings_planned", {}).get("row_count") or 0) > 0 else "unavailable",
            "fields": db_tables.get("earnings_planned", {}).get("columns", []),
            "note": "snapshot has fetched_at but no historical as-of-known contract for all evaluation dates",
        },
        "ex_rights_dividend_shareholder_benefit": {
            "classification": "available_but_not_actionable" if (db_tables.get("ex_rights", {}).get("row_count") or 0) > 0 else "unavailable",
            "fields": db_tables.get("ex_rights", {}).get("columns", []),
            "note": "snapshot has fetched_at but no historical as-of-known contract for all evaluation dates",
        },
        "event_flags_json": {
            "classification": "unavailable",
            "fields": sorted([c for c in source_columns if c == "event_flags_json"]),
        },
        "liquidity_flags_json": {
            "classification": "unavailable",
            "fields": sorted([c for c in source_columns if c == "liquidity_flags_json"]),
        },
        "tdnet_event_features": {
            "classification": "unavailable" if (db_tables.get("tdnet_disclosure_features", {}).get("row_count") or 0) == 0 else "available_but_not_actionable",
            "fields": db_tables.get("tdnet_disclosure_features", {}).get("columns", []),
            "note": "no rows in inspected snapshot" if (db_tables.get("tdnet_disclosure_features", {}).get("row_count") or 0) == 0 else "requires historical as-of-known validation",
        },
    }
    actionable_groups = [k for k, v in groups.items() if v["classification"] == "available_actionable_point_in_time"]
    return {
        "axis_id": AXIS_ID,
        "source_db": str(source_db),
        "db_tables": db_tables,
        "groups": groups,
        "true_actionable_event_liquidity_available": bool(actionable_groups),
        "actionable_groups": actionable_groups,
        "research_fallback_used": False,
    }


def _pct_by_date(frame: pd.DataFrame, column: str, *, ascending: bool = True) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    return values.groupby(frame["as_of_date"]).rank(pct=True, ascending=ascending)


def build_proxy_risk(frame: pd.DataFrame, actionable_complete: bool = False) -> pd.DataFrame:
    out = frame.copy()
    low_volume = pd.to_numeric(out["volume_vs_20d_avg"], errors="coerce") < 0.55
    abnormal_volume = pd.to_numeric(out["volume_vs_20d_avg"], errors="coerce") > 2.5
    high_atr = _pct_by_date(out, "atr14_pct") >= 0.90
    high_vol = _pct_by_date(out, "realized_vol20") >= 0.90
    upper_wick = _pct_by_date(out, "upper_wick_ratio") >= 0.90
    failed_high = out.get("failed_high_flag", False).fillna(False).astype(bool)
    weekly_failed_high = out.get("weekly_failed_high_flag", False).fillna(False).astype(bool)
    bearish = out.get("bearish_body_flag", False).fillna(False).astype(bool)
    gap_down = out.get("gap_down_flag", False).fillna(False).astype(bool)
    gap_up = out.get("gap_up_flag", False).fillna(False).astype(bool)

    out["event_risk_flag"] = False
    out["liquidity_risk_flag"] = low_volume | abnormal_volume
    out["volatility_liquidity_risk_flag"] = high_atr | high_vol | low_volume
    out["candle_gap_risk_flag"] = failed_high | weekly_failed_high | bearish | upper_wick | gap_down | gap_up
    out["composite_actionable_risk_score_v1"] = (
        0.00 * out["event_risk_flag"].astype(float)
        + 0.28 * out["liquidity_risk_flag"].astype(float)
        + 0.32 * out["volatility_liquidity_risk_flag"].astype(float)
        + 0.40 * out["candle_gap_risk_flag"].astype(float)
    )
    out["risk_bucket"] = out["composite_actionable_risk_score_v1"].map(
        lambda x: "high_risk" if x >= 0.60 else ("medium_risk" if x >= 0.28 else "low_risk")
    )
    reasons = []
    for values in zip(
        out["liquidity_risk_flag"],
        out["volatility_liquidity_risk_flag"],
        out["candle_gap_risk_flag"],
        low_volume,
        abnormal_volume,
        high_atr,
        high_vol,
        failed_high,
        weekly_failed_high,
        bearish,
        upper_wick,
        gap_down,
        gap_up,
    ):
        labels = []
        if values[3]:
            labels.append("low_volume_proxy")
        if values[4]:
            labels.append("abnormal_volume_proxy")
        if values[5]:
            labels.append("high_atr_proxy")
        if values[6]:
            labels.append("high_realized_vol_proxy")
        if values[7]:
            labels.append("failed_high")
        if values[8]:
            labels.append("weekly_failed_high")
        if values[9]:
            labels.append("bearish_body")
        if values[10]:
            labels.append("upper_wick")
        if values[11]:
            labels.append("gap_down")
        if values[12]:
            labels.append("gap_up")
        reasons.append("|".join(labels))
    out["risk_reason_codes"] = reasons
    out["actionable_contract_complete"] = bool(actionable_complete)
    out["actionable_event_liquidity_risk_v1"] = out["risk_bucket"] == "high_risk"
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


def risk_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    return {bucket: metric(rows[rows["risk_bucket"] == bucket]) for bucket in ["low_risk", "medium_risk", "high_risk"]}


def decide(feasibility: dict[str, Any], metrics: dict[str, Any], no_lookahead_pass: bool) -> tuple[str, str, list[str]]:
    if not no_lookahead_pass:
        return "blocked_no_lookahead_violation", "BLOCKED", ["source_no_lookahead_audit_failed"]
    high_bad = metrics["high_risk"].get("bad_rate_ret20_lt_minus_5pct") or 0.0
    low_bad = metrics["low_risk"].get("bad_rate_ret20_lt_minus_5pct") or 0.0
    high_severe = metrics["high_risk"].get("severe_rate_ret20_lt_minus_10pct") or 0.0
    low_severe = metrics["low_risk"].get("severe_rate_ret20_lt_minus_10pct") or 0.0
    separates = high_bad >= low_bad + 0.025 or high_severe >= low_severe + 0.015
    if feasibility["true_actionable_event_liquidity_available"]:
        if separates:
            return "actionable_event_liquidity_risk_ready_for_integration", "KEEP", ["true_actionable_sources_exist_and_risk_buckets_separate_downside"]
        return "no_risk_separation_edge", "DROP", ["actionable_fields_exist_but_risk_buckets_do_not_separate_downside"]
    if separates:
        return "proxy_risk_contract_created_but_not_actionable", "BLOCKED", ["only_proxy_features_exist_even_though_proxy_buckets_separate_some_downside"]
    return "blocked_missing_actionable_event_liquidity_sources", "BLOCKED", ["no_true_actionable_event_liquidity_source_exists"]


def feature_contract(source_columns: set[str], feasibility: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for col in ["as_of_date", "code"]:
        fields[col] = {"classification": "identifier"}
    for col in PROXY_FEATURE_COLUMNS:
        fields[col] = {"classification": "available_proxy_only" if col in source_columns else "unavailable"}
    for col in RISK_COLUMNS:
        fields[col] = {"classification": "available_proxy_only"}
    for col in sorted(OFFLINE_OUTCOME_COLUMNS):
        fields[col] = {"classification": "offline_outcome_only"}
    fields["event_flags_json"] = {"classification": feasibility["groups"]["event_flags_json"]["classification"]}
    fields["liquidity_flags_json"] = {"classification": feasibility["groups"]["liquidity_flags_json"]["classification"]}
    fields["earnings_planned"] = {"classification": feasibility["groups"]["earnings_date_planned_disclosure"]["classification"]}
    fields["ex_rights"] = {"classification": feasibility["groups"]["ex_rights_dividend_shareholder_benefit"]["classification"]}
    fields["ret20_derived_tags"] = {"classification": "forbidden_future_leak"}
    return {"axis_id": AXIS_ID, "fields": fields}


def output_columns(rows: pd.DataFrame) -> list[str]:
    cols = ["as_of_date", "code", *RISK_COLUMNS, *PROXY_FEATURE_COLUMNS]
    cols.extend([c for c in ["source_db", "source_bar_status", "source_lineage"] if c in rows.columns])
    cols.extend([c for c in OFFLINE_OUTCOME_COLUMNS if c in rows.columns])
    return [c for c in cols if c in rows.columns]


def run(asof_root: Path = DEFAULT_ASOF_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT, source_db: Path = DEFAULT_SOURCE_DB) -> Path:
    source_path = asof_root / "asof_positive_selection_score_rows.parquet"
    if not source_path.exists():
        raise FileNotFoundError(source_path)
    source_decision = _load_json(asof_root / "research_decision.json")
    source_no_lookahead = _load_json(asof_root / "no_lookahead_audit.json")
    rows = pd.read_parquet(source_path)
    missing_proxy = [c for c in PROXY_FEATURE_COLUMNS if c not in rows.columns]
    if missing_proxy:
        raise RuntimeError(f"missing proxy features: {missing_proxy}")
    feasibility = source_feasibility(source_db, set(rows.columns))
    actionable_complete = bool(feasibility["true_actionable_event_liquidity_available"])
    risk_rows = build_proxy_risk(rows, actionable_complete=actionable_complete)
    metrics = risk_metrics(risk_rows)
    no_lookahead_pass = source_no_lookahead.get("no_lookahead_pass") is True
    decision, decision_class, reasons = decide(feasibility, metrics, no_lookahead_pass)

    out = output_root / f"{_now_tag()}-actionable-event-liquidity-risk-v1"
    out.mkdir(parents=True, exist_ok=True)
    cols = output_columns(risk_rows)
    risk_rows[cols].to_parquet(out / "actionable_event_liquidity_risk_rows.parquet", index=False)
    risk_rows[cols].head(25000).to_csv(out / "actionable_event_liquidity_risk_rows_sample.csv", index=False)
    risk_contract = {
        "axis_id": AXIS_ID,
        "contract_id": "actionable_event_liquidity_risk_v1",
        "actionable_contract_complete": actionable_complete,
        "diagnostic_only": True,
        "true_actionable_event_liquidity_available": feasibility["true_actionable_event_liquidity_available"],
        "proxy_features_used": PROXY_FEATURE_COLUMNS,
        "risk_columns": RISK_COLUMNS,
        "missing_actionable_source_contracts": [
            "historical_asof_earnings_calendar_contract",
            "historical_asof_ex_rights_dividend_contract",
            "point_in_time_tdnet_event_features_contract",
            "actionable_liquidity_flags_or_turnover_contract",
        ],
        "forbidden_features": ["ret5", "ret10", "ret20", "future outcomes", "future event realization"],
    }
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "actionable_contract_complete": actionable_complete,
        "row_count": int(len(risk_rows)),
        "date_count": int(risk_rows["as_of_date"].nunique()),
        "code_count": int(risk_rows["code"].nunique()),
        "risk_bucket_metrics": metrics,
    }
    no_lookahead = {
        "audit_result": "pass" if no_lookahead_pass else "blocked",
        "no_lookahead_pass": bool(no_lookahead_pass),
        "source_no_lookahead_audit": source_no_lookahead.get("audit_result"),
        "risk_flags_use_point_in_time_or_proxy_features_only": True,
        "offline_outcomes_used_in_risk_flags": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }
    source_coverage = {
        "axis_id": AXIS_ID,
        "input_source_path": str(source_path),
        "input_source_decision": source_decision,
        "source_db": str(source_db),
        "row_count": int(len(risk_rows)),
        "date_count": int(risk_rows["as_of_date"].nunique()),
        "code_count": int(risk_rows["code"].nunique()),
        "missing_proxy_features": missing_proxy,
        "research_fallback_used": False,
    }
    research_decision = {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "actionable_contract_complete": actionable_complete,
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "production_candidate_generator_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
        "research_fallback_used": False,
    }
    offline_audit = {
        "outcomes_are_offline_only": True,
        "outcome_columns": sorted(OFFLINE_OUTCOME_COLUMNS),
        "outcome_coverage_rate": float(risk_rows["ret20"].notna().mean()) if "ret20" in risk_rows else 0.0,
        "risk_bucket_metrics_reference": "risk_flag_metrics.json",
    }

    _write_json(out / "actionable_event_liquidity_risk_summary.json", summary)
    _write_json(out / "risk_contract.json", risk_contract)
    _write_json(out / "source_feasibility_audit.json", feasibility)
    _write_json(out / "feature_contract.json", feature_contract(set(rows.columns), feasibility))
    _write_json(out / "risk_flag_metrics.json", metrics)
    _write_json(out / "offline_outcome_audit.json", offline_audit)
    _write_json(out / "no_lookahead_audit.json", no_lookahead)
    _write_json(out / "source_coverage.json", source_coverage)
    _write_json(out / "research_decision.json", research_decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--asof-root", type=Path, default=DEFAULT_ASOF_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    args = parser.parse_args(argv)
    out = run(args.asof_root, args.output_root, args.source_db)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
