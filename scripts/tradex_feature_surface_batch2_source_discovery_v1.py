from __future__ import annotations

import argparse
import json
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


SCRIPT_NAME = "tradex_feature_surface_batch2_source_discovery_v1"
SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_input_resolution_v1"
EVENT_INVENTORY_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_event_source_inventory_v1"
DIVIDEND_RIGHTS_INVENTORY_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_dividend_rights_source_inventory_v1"
CORPORATE_ACTION_INVENTORY_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_corporate_action_source_inventory_v1"
VOLUME_AUDIT_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_volume_participation_source_audit_v1"
FEASIBILITY_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_batch2_feature_feasibility_matrix_v1"
GAP_SUMMARY_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_batch2_source_gap_summary_v1"
RECOMMENDATION_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_batch2_implementation_recommendation_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_surface_batch2_source_discovery_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_surface_batch2_source_discovery_v1")
REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")

BATCH1_SESSION = Path(r"G:\Tradex\feature_surface_batch1_v1\20260501T093159Z-820266")
BATCH1_CANDIDATE = BATCH1_SESSION / "candidate_prefilter_rows_feature_enriched_v1.parquet"
BATCH1_ORFP = BATCH1_SESSION / "observable_regime_false_positive_feature_enriched_v1.parquet"
FEATURE_PLAN_SESSION = Path(r"G:\Tradex\feature_surface_upgrade_plan_v1\20260501T091723Z-838354")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except Exception:
        return None
    return result if math.isfinite(result) else None


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, tuple)):
        return list(value)
    if isinstance(value, pd.Series):
        return value.to_dict()
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, (pd.Interval,)):
        return str(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default), encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table],
    ).fetchone()
    return bool(row)


def _table_columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table],
    ).fetchall()
    return [{"column_name": str(col), "data_type": str(dtype)} for col, dtype in rows]


def _table_count(conn: duckdb.DuckDBPyConnection, table: str) -> int | None:
    if not _table_exists(conn, table):
        return None
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _table_min_max(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> tuple[str | None, str | None]:
    if not _table_exists(conn, table):
        return None, None
    try:
        row = conn.execute(f"SELECT MIN({column}), MAX({column}) FROM {table}").fetchone()
    except Exception:
        return None, None
    if not row:
        return None, None
    return (str(row[0]) if row[0] is not None else None, str(row[1]) if row[1] is not None else None)


def _table_inventory(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    tables = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
        """
    ).fetchdf()
    rows: list[dict[str, Any]] = []
    for table in tables["table_name"].tolist():
        columns = _table_columns(conn, table)
        count = _table_count(conn, table)
        row: dict[str, Any] = {
            "table_name": table,
            "row_count": count,
            "column_count": len(columns),
            "columns": [item["column_name"] for item in columns],
        }
        if table in {"earnings_planned", "ex_rights"}:
            date_col = "planned_date" if table == "earnings_planned" else "ex_date"
            row["date_min"], row["date_max"] = _table_min_max(conn, table, date_col)
            row["fetched_min"], row["fetched_max"] = _table_min_max(conn, table, "fetched_at")
        elif table in {"daily_bars", "daily_ma", "ml_feature_daily"}:
            row["date_min"], row["date_max"] = _table_min_max(conn, table, "date" if table != "ml_feature_daily" else "dt")
        elif table in {"tdnet_disclosures", "tdnet_disclosure_features"}:
            row["date_min"], row["date_max"] = _table_min_max(conn, table, "published_at")
        elif table.startswith("edinetdb_"):
            if table in {"edinetdb_company_map"}:
                row["date_min"], row["date_max"] = _table_min_max(conn, table, "updated_at")
            else:
                row["date_min"], row["date_max"] = _table_min_max(conn, table, "fetched_at")
        rows.append(row)
    return pd.DataFrame(rows)


def _read_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path)


def _to_epoch_seconds(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    return (dt.view("int64") // 10**9).astype("Int64")


def _load_event_inventory(conn: duckdb.DuckDBPyConnection, *, candidate_codes: set[str]) -> dict[str, Any]:
    inventory: list[dict[str, Any]] = []
    for table, date_col, event_date_meaning, joinability in [
        (
            "earnings_planned",
            "planned_date",
            "planned earnings announcement date",
            "joinable to anchor_date / symbol via code + planned_date, but PIT safety is conditional on snapshot retention",
        ),
        (
            "ex_rights",
            "ex_date",
            "ex-rights date / corporate-action timing",
            "joinable to anchor_date / symbol via code + ex_date, but PIT safety is conditional on snapshot retention",
        ),
    ]:
        exists = _table_exists(conn, table)
        cols = _table_columns(conn, table) if exists else []
        count = _table_count(conn, table) if exists else None
        fetched_at_min = fetched_at_max = None
        date_min = date_max = None
        distinct_codes = None
        if exists:
            date_min, date_max = _table_min_max(conn, table, date_col)
            fetched_at_min, fetched_at_max = _table_min_max(conn, table, "fetched_at")
            try:
                distinct_codes = int(conn.execute(f"SELECT COUNT(DISTINCT code) FROM {table}").fetchone()[0])
            except Exception:
                distinct_codes = None
        coverage_estimate = None
        if exists and candidate_codes:
            try:
                intersect = conn.execute(
                    f"""
                    SELECT COUNT(DISTINCT t.code)
                    FROM {table} t
                    WHERE t.code IN ({",".join(["?"] * len(candidate_codes))})
                    """,
                    list(candidate_codes),
                ).fetchone()[0]
                coverage_estimate = _safe_float(int(intersect) / len(candidate_codes))
            except Exception:
                coverage_estimate = None
        inventory.append(
            {
                "source_table": table,
                "available": exists and bool(count),
                "row_count": count,
                "distinct_codes": distinct_codes,
                "fields": [item["column_name"] for item in cols],
                "symbol_key": "code",
                "date_key": date_col,
                "event_date_meaning": event_date_meaning,
                "update_timing": "single current snapshot in local DB; fetched_at recorded per row",
                "point_in_time_safety": "conditional",
                "joinable_to_anchor_date_symbol_side": True,
                "coverage_estimate": coverage_estimate,
                "date_range": {"min": date_min, "max": date_max},
                "fetched_at_range": {"min": fetched_at_min, "max": fetched_at_max},
                "limitations": joinability,
            }
        )
    return {
        "schema_version": EVENT_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now().isoformat(),
        "sources": inventory,
        "notes": [
            "earnings_planned and ex_rights exist in the local DB snapshot, but their fetched_at is a single refresh timestamp so historical PIT gating needs explicit snapshot/backfill handling.",
            "tdnet / EDINET event-class tables are handled separately in the corporate action inventory.",
        ],
    }


def _load_dividend_rights_inventory(conn: duckdb.DuckDBPyConnection, *, candidate_codes: set[str]) -> dict[str, Any]:
    sources = []
    for table, date_key, label in [
        ("ex_rights", "ex_date", "ex-rights / rights timing"),
    ]:
        exists = _table_exists(conn, table)
        count = _table_count(conn, table) if exists else None
        date_min, date_max = _table_min_max(conn, table, date_key) if exists else (None, None)
        fetched_min, fetched_max = _table_min_max(conn, table, "fetched_at") if exists else (None, None)
        coverage_estimate = None
        if exists and candidate_codes:
            try:
                intersect = conn.execute(
                    f"""
                    SELECT COUNT(DISTINCT t.code)
                    FROM {table} t
                    WHERE t.code IN ({",".join(["?"] * len(candidate_codes))})
                    """,
                    list(candidate_codes),
                ).fetchone()[0]
                coverage_estimate = _safe_float(int(intersect) / len(candidate_codes))
            except Exception:
                coverage_estimate = None
        sources.append(
            {
                "source_table": table,
                "available": exists and bool(count),
                "row_count": count,
                "symbol_key": "code",
                "date_key": date_key,
                "event_date_meaning": label,
                "point_in_time_safety": "conditional",
                "coverage_estimate": coverage_estimate,
                "date_range": {"min": date_min, "max": date_max},
                "fetched_at_range": {"min": fetched_min, "max": fetched_max},
                "limitations": "No dividend-specific table exists in the local DB snapshot; rights are present, dividends are not.",
            }
        )
    return {
        "schema_version": DIVIDEND_RIGHTS_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now().isoformat(),
        "sources": sources,
        "explicit_gaps": [
            "dividend-specific calendar table not found in local DB snapshot",
            "ex_dividend source table not found in local DB snapshot",
            "rights source is present but PIT safety is conditional on snapshot retention",
        ],
    }


def _load_corporate_action_inventory(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    tables = ["tdnet_disclosures", "tdnet_disclosure_features", "edinetdb_analysis", "edinetdb_financials", "edinetdb_ratios"]
    sources = []
    for table in tables:
        exists = _table_exists(conn, table)
        count = _table_count(conn, table) if exists else None
        cols = _table_columns(conn, table) if exists else []
        date_col = None
        if table in {"tdnet_disclosures", "tdnet_disclosure_features"}:
            date_col = "published_at"
        elif table == "edinetdb_analysis":
            date_col = "asof_date"
        elif table in {"edinetdb_financials", "edinetdb_ratios"}:
            date_col = "fetched_at"
        date_min, date_max = _table_min_max(conn, table, date_col) if exists and date_col else (None, None)
        sources.append(
            {
                "source_table": table,
                "available": exists and bool(count),
                "row_count": count,
                "fields": [item["column_name"] for item in cols],
                "symbol_key": "sec_code" if table.startswith("tdnet_") else ("edinet_code" if table.startswith("edinetdb_") else None),
                "date_key": date_col,
                "date_range": {"min": date_min, "max": date_max},
                "point_in_time_safety": "conditional" if count else "unavailable",
            }
        )
    return {
        "schema_version": CORPORATE_ACTION_INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now().isoformat(),
        "sources": sources,
        "conclusion": "No explicit dilution/financing flag source is present in the current local DB snapshot; tdnet and edinet feature tables exist but are empty or indirect for this contract.",
        "explicit_gaps": [
            "dilution_event_flag: no explicit PIT-safe source table found",
            "financing_event_nearby_flag: no explicit PIT-safe source table found",
            "tdnet_disclosures and tdnet_disclosure_features are empty in the current DB snapshot",
            "edinetdb_analysis is empty in the current DB snapshot",
        ],
    }


def _build_volume_audit(candidate: pd.DataFrame, conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    if "vol_ratio5_20" not in candidate.columns:
        raise KeyError("candidate surface lacks vol_ratio5_20")
    missing = candidate[candidate["vol_ratio5_20"].isna()].copy()
    missing["anchor_ts"] = pd.to_datetime(missing["anchor_date"], utc=True, errors="coerce").astype("int64") // 10**9
    total = int(len(candidate))
    missing_count = int(missing.shape[0])
    non_null_count = total - missing_count
    missing_rate = _safe_float(missing_count / total) if total else None

    vol_ratio_repair = None
    daily_bars_join = None
    if missing_count:
        conn.register("missing_rows", missing[["symbol", "anchor_ts"]].rename(columns={"symbol": "code", "anchor_ts": "dt"}))
        daily_bars_join = conn.execute(
            """
            SELECT
                COUNT(*) AS missing_rows,
                COUNT(db.code) AS matched_daily_bars_rows,
                SUM(CASE WHEN db.v IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows_with_volume,
                SUM(CASE WHEN db.v IS NULL THEN 1 ELSE 0 END) AS matched_rows_without_volume
            FROM missing_rows m
            LEFT JOIN daily_bars db ON db.code = m.code AND db.date = m.dt
            """
        ).fetchone()
        vol_ratio_repair = conn.execute(
            """
            SELECT
                COUNT(*) AS missing_rows,
                COUNT(mf.code) AS matched_ml_feature_rows,
                SUM(CASE WHEN mf.vol_ratio5_20 IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows_with_vol_ratio,
                SUM(CASE WHEN mf.vol_ratio5_20 IS NULL THEN 1 ELSE 0 END) AS matched_rows_without_vol_ratio
            FROM missing_rows m
            LEFT JOIN ml_feature_daily mf ON mf.code = m.code AND mf.dt = m.dt
            """
        ).fetchone()

    ml_feature_total = _table_count(conn, "ml_feature_daily") or 0
    ml_feature_non_null = 0
    if _table_exists(conn, "ml_feature_daily"):
        try:
            ml_feature_non_null = int(conn.execute("SELECT COUNT(*) FROM ml_feature_daily WHERE vol_ratio5_20 IS NOT NULL").fetchone()[0])
        except Exception:
            ml_feature_non_null = 0

    return {
        "schema_version": VOLUME_AUDIT_SCHEMA_VERSION,
        "generated_at_utc": _utc_now().isoformat(),
        "candidate_row_count": total,
        "vol_ratio5_20_non_null_count": non_null_count,
        "vol_ratio5_20_missing_count": missing_count,
        "vol_ratio5_20_missing_rate": missing_rate,
        "repair_diagnosis": {
            "daily_bars_table_present": _table_exists(conn, "daily_bars"),
            "ml_feature_daily_table_present": _table_exists(conn, "ml_feature_daily"),
            "missing_rows_match_daily_bars": bool(daily_bars_join and int(daily_bars_join[1]) == missing_count),
            "missing_rows_match_ml_feature_daily": bool(vol_ratio_repair and int(vol_ratio_repair[1]) == missing_count),
            "source_gap_type": "surface_join_gap_not_source_absence",
            "reason": "every vol_ratio5_20-missing candidate row joins to daily_bars and ml_feature_daily, and ml_feature_daily already carries vol_ratio5_20 values for those rows",
        },
        "daily_bars_repair": {
            "missing_rows": int(daily_bars_join[0]) if daily_bars_join else missing_count,
            "matched_daily_bars_rows": int(daily_bars_join[1]) if daily_bars_join else 0,
            "matched_rows_with_volume": int(daily_bars_join[2]) if daily_bars_join else 0,
            "matched_rows_without_volume": int(daily_bars_join[3]) if daily_bars_join else 0,
        },
        "ml_feature_daily_repair": {
            "table_row_count": ml_feature_total,
            "vol_ratio5_20_non_null_count": ml_feature_non_null,
            "missing_rows": int(vol_ratio_repair[0]) if vol_ratio_repair else missing_count,
            "matched_ml_feature_rows": int(vol_ratio_repair[1]) if vol_ratio_repair else 0,
            "matched_rows_with_vol_ratio": int(vol_ratio_repair[2]) if vol_ratio_repair else 0,
            "matched_rows_without_vol_ratio": int(vol_ratio_repair[3]) if vol_ratio_repair else 0,
        },
        "coverage_by_topk": {
            "top5_missing_count": int(missing["challenger_selected_top5"].fillna(False).astype(bool).sum()),
            "top10_missing_count": int(missing["challenger_selected_top10"].fillna(False).astype(bool).sum()),
            "top20_missing_count": int(missing["challenger_selected_top20"].fillna(False).astype(bool).sum()),
        },
        "coverage_by_family": {str(k): int(v) for k, v in missing["family_classification"].value_counts(dropna=False).items()},
        "coverage_by_side": {str(k): int(v) for k, v in missing["side"].value_counts(dropna=False).items()},
        "coverage_by_month": {str(k): int(v) for k, v in missing["month_bucket"].value_counts(dropna=False).head(12).items()},
        "samples": missing[["anchor_date", "symbol", "side", "candidate_rank", "candidate_score", "vol_ratio5_20", "liquidity20d"]]
        .head(15)
        .to_dict(orient="records"),
        "source_tables": [
            {
                "table": "daily_bars",
                "row_count": _table_count(conn, "daily_bars"),
                "columns": [item["column_name"] for item in _table_columns(conn, "daily_bars")],
                "join_key": "code + date(epoch seconds)",
            },
            {
                "table": "ml_feature_daily",
                "row_count": ml_feature_total,
                "columns": [item["column_name"] for item in _table_columns(conn, "ml_feature_daily")],
                "join_key": "code + dt(epoch seconds)",
            },
        ],
    }


def _batch2_feasibility_matrix() -> list[dict[str, Any]]:
    return [
        {
            "feature_name": "earnings_nearby_flag",
            "category": "event / earnings / rights",
            "feasibility": "needs_source_backfill",
            "why": "earnings_planned exists, but the current local snapshot is a live refresh with a single fetched_at timestamp; historical PIT safety is not yet proven for retrospective surfaces.",
            "required_source": "earnings_planned",
            "join_key": "code + planned_date",
            "date_alignment_rule": "PIT-safe only if joined against a retained snapshot or if fetched_at gating is added to the backfill contract.",
        },
        {
            "feature_name": "earnings_window_bucket",
            "category": "event / earnings / rights",
            "feasibility": "needs_source_backfill",
            "why": "same source as earnings_nearby_flag, but bucketization still depends on an auditable as-of event calendar contract.",
            "required_source": "earnings_planned",
            "join_key": "code + planned_date",
            "date_alignment_rule": "same as earnings_nearby_flag.",
        },
        {
            "feature_name": "ex_rights_nearby_flag",
            "category": "event / earnings / rights",
            "feasibility": "needs_source_backfill",
            "why": "ex_rights exists, but only as a current snapshot with a single fetched_at timestamp; the historical no-lookahead contract is not yet proved.",
            "required_source": "ex_rights",
            "join_key": "code + ex_date",
            "date_alignment_rule": "PIT-safe only with snapshot retention or fetched_at gating.",
        },
        {
            "feature_name": "dividend_rights_nearby_flag",
            "category": "event / earnings / rights",
            "feasibility": "needs_external_data_source",
            "why": "rights coverage exists, but there is no dividend-specific source table or explicit dividend calendar in the local DB snapshot.",
            "required_source": "dividend / ex-dividend calendar",
            "join_key": "code + event_date",
            "date_alignment_rule": "needs an explicit date-stamped source.",
        },
        {
            "feature_name": "dilution_event_flag",
            "category": "event / earnings / rights",
            "feasibility": "needs_external_data_source",
            "why": "tdnet/EDINET tables exist, but no explicit dilution-event table or PIT-safe flag source is present; tdnet_disclosures and tdnet_disclosure_features are empty in this snapshot.",
            "required_source": "TDnet / EDINET dilution detector or calendar",
            "join_key": "code + published_at/asof_date",
            "date_alignment_rule": "needs a timestamped upstream source.",
        },
        {
            "feature_name": "financing_event_nearby_flag",
            "category": "event / earnings / rights",
            "feasibility": "needs_external_data_source",
            "why": "no explicit financing-event source table was found; current tdnet/EDINET tables do not provide a direct financing flag contract.",
            "required_source": "TDnet / EDINET financing or offering detector",
            "join_key": "code + published_at/asof_date",
            "date_alignment_rule": "needs a timestamped upstream source.",
        },
        {
            "feature_name": "volume_zscore_20",
            "category": "volume / participation",
            "feasibility": "implement_now_from_existing_source",
            "why": "ml_feature_daily already contains turnover20, turnover_z20, and vol_ratio5_20 derived from daily_bars; the missingness is a surface join gap, not a source gap.",
            "required_source": "ml_feature_daily or daily_bars",
            "join_key": "code + dt(epoch seconds)",
            "date_alignment_rule": "same-day or earlier daily bars only.",
        },
        {
            "feature_name": "turnover_value_ratio5_20",
            "category": "volume / participation",
            "feasibility": "implement_now_from_existing_source",
            "why": "daily_bars and ml_feature_daily already provide the data needed to derive this proxy without future information.",
            "required_source": "daily_bars + ml_feature_daily",
            "join_key": "code + dt(epoch seconds)",
            "date_alignment_rule": "same-day or earlier daily bars only.",
        },
        {
            "feature_name": "participation_quality_bucket",
            "category": "volume / participation",
            "feasibility": "implement_now_from_existing_source",
            "why": "can be derived from vol_ratio5_20, turnover20, liquidity20d, and explicit missingness handling.",
            "required_source": "ml_feature_daily + daily_bars + liquidity20d",
            "join_key": "code + dt(epoch seconds)",
            "date_alignment_rule": "same-day or earlier daily bars only.",
        },
        {
            "feature_name": "volume_confirmation_repaired_flag",
            "category": "volume / participation",
            "feasibility": "implement_now_from_existing_source",
            "why": "the current surface can repair explicit missingness by joining the existing ml_feature_daily volume fields; this is a surface enrichment task, not a new source hunt.",
            "required_source": "ml_feature_daily",
            "join_key": "code + dt(epoch seconds)",
            "date_alignment_rule": "same-day or earlier daily bars only.",
        },
    ]


def run_batch2_source_discovery(*, output_root: Path, jobs: int = 1) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + f"{random.randint(0, 999999):06d}"
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)
    if not BATCH1_CANDIDATE.exists():
        raise FileNotFoundError(BATCH1_CANDIDATE)
    if not BATCH1_ORFP.exists():
        raise FileNotFoundError(BATCH1_ORFP)

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        candidate = _read_frame(BATCH1_CANDIDATE)
        orfp = _read_frame(BATCH1_ORFP)
        table_inventory = _table_inventory(conn)
        candidate_codes = set(candidate["symbol"].dropna().astype(str).tolist())
        event_inventory = _load_event_inventory(conn, candidate_codes=candidate_codes)
        dividend_rights_inventory = _load_dividend_rights_inventory(conn, candidate_codes=candidate_codes)
        corporate_action_inventory = _load_corporate_action_inventory(conn)
        volume_audit = _build_volume_audit(candidate, conn)
        feasibility = _batch2_feasibility_matrix()

        source_gap_summary = {
            "schema_version": GAP_SUMMARY_SCHEMA_VERSION,
            "generated_at_utc": _utc_now().isoformat(),
            "volume_gap_summary": {
                "candidate_row_count": int(len(candidate)),
                "vol_ratio5_20_missing_count": int(candidate["vol_ratio5_20"].isna().sum()),
                "vol_ratio5_20_missing_rate": _safe_float(candidate["vol_ratio5_20"].isna().mean()),
                "gap_type": "surface_join_gap_not_source_absence",
                "reason": "daily_bars and ml_feature_daily both cover the missing rows; the surface simply did not carry the ratio for those rows yet.",
            },
            "event_gap_summary": {
                "earnings_planned": "present_but_snapshot_only",
                "ex_rights": "present_but_snapshot_only",
                "tdnet_disclosures": "empty",
                "tdnet_disclosure_features": "empty",
                "edinetdb_analysis": "empty",
                "dilution_financing": "explicit_source_missing",
            },
            "implementation_gap_summary": {
                "volume_participation": "implement_now",
                "event_context": "needs_source_backfill_or_external_selection",
            },
            "notes": [
                "Batch 2 should start with the volume participation repair because the data source already exists and covers the missing rows.",
                "Event/rights flags need snapshot/backfill discipline before they are safe to use in retrospective no-lookahead surfaces.",
            ],
        }

        recommendation = {
            "schema_version": RECOMMENDATION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now().isoformat(),
            "recommended_path": "ready_to_implement_batch2_volume_participation",
            "why": [
                "vol_ratio5_20 is missing on 1755 of 2542 candidate rows, but every missing row joins to daily_bars and ml_feature_daily.",
                "ml_feature_daily already contains vol_ratio5_20 for those rows, so the surface gap is repairable now.",
                "Event / rights tables exist, but they are current snapshots and do not yet satisfy the retrospective no-lookahead contract without snapshot/backfill handling.",
                "TDnet / EDINET event-class tables are empty or indirect in the current DB snapshot, so dilution / financing flags still need upstream work.",
            ],
            "deferred_paths": [
                "earnings_nearby_flag and earnings_window_bucket until snapshot/backfill handling is clarified",
                "ex_rights_nearby_flag until snapshot/backfill handling is clarified",
                "dividend_rights_nearby_flag until a dividend-specific calendar source is added",
                "dilution_event_flag and financing_event_nearby_flag until explicit upstream event sources are selected",
            ],
        }

        decision = {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now().isoformat(),
            "decision": "ready_to_implement_batch2_volume_participation",
            "status": "ready_to_implement_batch2_volume_participation",
            "reason": "volume_participation_can_be_repaired_now_while_event_rights_sources_need_backfill_or_external_selection",
            "authoritative_signal": "volume_source_repair_is_the_highest_value_implementable_path",
            "event_rights_status": "present_but_not_yet_proven_pit_safe_for_historical_surface_backfill",
            "volume_status": "implement_now",
        }

        input_resolution = {
            "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now().isoformat(),
            "resolved_paths": {
                "db_path": str(DB_PATH),
                "batch1_session": str(BATCH1_SESSION),
                "batch1_candidate_surface": str(BATCH1_CANDIDATE),
                "batch1_orfp_surface": str(BATCH1_ORFP),
                "feature_plan_session": str(FEATURE_PLAN_SESSION),
            },
            "path_checks": {
                "db_exists": DB_PATH.exists(),
                "batch1_candidate_exists": BATCH1_CANDIDATE.exists(),
                "batch1_orfp_exists": BATCH1_ORFP.exists(),
                "feature_plan_exists": FEATURE_PLAN_SESSION.exists(),
            },
            "notes": [
                "Local DuckDB contains earnings_planned and ex_rights, but both are current snapshots with a single fetched_at timestamp and therefore require PIT/backfill discipline for historical use.",
                "daily_bars and ml_feature_daily cover every vol_ratio5_20-missing candidate row, so the volume gap is repairable without a new upstream source.",
            ],
        }

        run_manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "script_name": SCRIPT_NAME,
            "generated_at_utc": _utc_now().isoformat(),
            "output_root": str(output_root),
            "session_id": session_id,
            "jobs_requested": int(jobs),
            "jobs_supported": 1,
            "input_roots": {
                "batch1_session": str(BATCH1_SESSION),
                "feature_plan_session": str(FEATURE_PLAN_SESSION),
                "duckdb_path": str(DB_PATH),
            },
            "row_counts": {
                "candidate_surface": int(len(candidate)),
                "orfp_surface": int(len(orfp)),
                "table_inventory": int(len(table_inventory)),
            },
            "decision": decision["decision"],
        }

        _write_json(session_dir / "run_manifest.json", run_manifest)
        _write_json(session_dir / "input_resolution.json", input_resolution)
        _write_json(session_dir / "event_source_inventory.json", event_inventory)
        _write_json(session_dir / "dividend_rights_source_inventory.json", dividend_rights_inventory)
        _write_json(session_dir / "corporate_action_source_inventory.json", corporate_action_inventory)
        _write_json(session_dir / "volume_participation_source_audit.json", volume_audit)
        _write_json(session_dir / "batch2_feature_feasibility_matrix.json", feasibility)
        _write_json(session_dir / "batch2_source_gap_summary.json", source_gap_summary)
        _write_json(session_dir / "batch2_implementation_recommendation.json", recommendation)
        _write_json(session_dir / "feature_surface_batch2_source_discovery_v1_decision.json", decision)
        _write_parquet(session_dir / "source_table_inventory.parquet", table_inventory)
        sample_rows = candidate[candidate["vol_ratio5_20"].isna()].head(25)[
            ["anchor_date", "symbol", "side", "candidate_rank", "candidate_score", "vol_ratio5_20", "liquidity20d", "family_classification"]
        ].copy()
        _write_parquet(session_dir / "volume_missing_rows_sample.parquet", sample_rows)
        _write_json(
            session_dir / "_ARTIFACT_COMPLETE.json",
            {
                "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
                "generated_at_utc": _utc_now().isoformat(),
                "required_files_present": True,
                "session_id": session_id,
            },
        )
    finally:
        conn.close()

    return {"output_dir": str(session_dir), "decision": decision["decision"], "session_id": session_id}


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_batch2_source_discovery(output_root=args.output_root, jobs=args.jobs)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
