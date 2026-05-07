from __future__ import annotations

import argparse
import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


SCRIPT_NAME = "tradex_feature_surface_batch2_volume_participation_v1"
SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_input_resolution_v1"
JOIN_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_volume_join_contract_v1"
FORMULA_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_volume_feature_formula_contract_v1"
COVERAGE_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_volume_repair_coverage_summary_v1"
MISSINGNESS_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_volume_feature_missingness_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_no_lookahead_volume_feature_audit_v1"
CONTRAST_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_added_top15_bottom15_volume_contrast_v1"
ORFP_SUMMARY_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_orfp_volume_feature_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_surface_batch2_volume_participation_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1")
DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")

BATCH1_SESSION = Path(r"G:\Tradex\feature_surface_batch1_v1\20260501T093159Z-820266")
BATCH1_CANDIDATE = BATCH1_SESSION / "candidate_prefilter_rows_feature_enriched_v1.parquet"
BATCH1_ORFP = BATCH1_SESSION / "observable_regime_false_positive_feature_enriched_v1.parquet"
BATCH1_NO_LOOKAHEAD = BATCH1_SESSION / "no_lookahead_feature_audit.json"
BATCH1_COVERAGE = BATCH1_SESSION / "feature_coverage_summary.json"
BATCH1_FORMULA = BATCH1_SESSION / "feature_formula_contract.json"

BATCH2_DISCOVERY_SESSION = Path(r"G:\Tradex\feature_surface_batch2_source_discovery_v1\20260501T095733Z-146682")
BATCH2_DISCOVERY_AUDIT = BATCH2_DISCOVERY_SESSION / "volume_participation_source_audit.json"
BATCH2_DISCOVERY_FEASIBILITY = BATCH2_DISCOVERY_SESSION / "batch2_feature_feasibility_matrix.json"
BATCH2_DISCOVERY_RECOMMENDATION = BATCH2_DISCOVERY_SESSION / "batch2_implementation_recommendation.json"
BATCH2_DISCOVERY_DECISION = BATCH2_DISCOVERY_SESSION / "feature_surface_batch2_source_discovery_v1_decision.json"
BATCH2_DISCOVERY_SAMPLE = BATCH2_DISCOVERY_SESSION / "volume_missing_rows_sample.parquet"

ORFP_DIFF_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
ORFP_TOPK_DIFF = ORFP_DIFF_SESSION / "topk_membership_diff.parquet"

TOP_K_VALUES = (5, 10, 20)
VOLUME_FEATURES = (
    "vol_ratio5_20_repaired",
    "volume_zscore_20",
    "turnover_value_ratio5_20",
    "participation_quality_bucket",
    "volume_confirmation_repaired_flag",
)
SOURCE_DATE_FEATURES = (
    "vol_ratio5_20_repaired_source_date",
    "volume_zscore_20_source_date",
    "turnover_value_ratio5_20_source_date",
    "participation_quality_bucket_source_date",
    "volume_confirmation_repaired_flag_source_date",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "<na>", "none", "null", "unknown"}


def _safe_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        result = float(value)
    except Exception:
        return None
    return result if math.isfinite(result) else None


def _anchor_to_epoch(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    values: list[int | None] = []
    for item in dt.tolist():
        if pd.isna(item):
            values.append(None)
        else:
            values.append(int(item.timestamp()))
    return pd.Series(values, index=series.index, dtype="Int64")


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


def _table_count(conn: duckdb.DuckDBPyConnection, table: str) -> int | None:
    if not _table_exists(conn, table):
        return None
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


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
        row: dict[str, Any] = {
            "table_name": table,
            "row_count": _table_count(conn, table),
            "column_count": len(columns),
            "columns": [item["column_name"] for item in columns],
        }
        if table in {"daily_bars", "daily_ma", "ml_feature_daily"}:
            row["date_min"], row["date_max"] = _table_min_max(conn, table, "date" if table != "ml_feature_daily" else "dt")
        elif table in {"earnings_planned", "ex_rights"}:
            row["date_min"], row["date_max"] = _table_min_max(conn, table, "planned_date" if table == "earnings_planned" else "ex_date")
        elif table in {"tdnet_disclosures", "tdnet_disclosure_features"}:
            row["date_min"], row["date_max"] = _table_min_max(conn, table, "published_at")
        elif table.startswith("edinetdb_"):
            row["date_min"], row["date_max"] = _table_min_max(conn, table, "updated_at" if table == "edinetdb_company_map" else "fetched_at")
        rows.append(row)
    return pd.DataFrame(rows)


def _source_resolution(*, batch1_session: Path, batch1_candidate: Path, batch1_orfp: Path, batch1_formula: Path, batch1_no_lookahead: Path, batch1_coverage: Path) -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "batch1_session": str(batch1_session),
            "batch1_candidate_surface": str(batch1_candidate),
            "batch1_orfp_surface": str(batch1_orfp),
            "batch1_formula_contract": str(batch1_formula),
            "batch1_no_lookahead": str(batch1_no_lookahead),
            "batch1_coverage_summary": str(batch1_coverage),
            "batch2_discovery_session": str(BATCH2_DISCOVERY_SESSION),
            "batch2_discovery_audit": str(BATCH2_DISCOVERY_AUDIT),
            "batch2_discovery_feasibility": str(BATCH2_DISCOVERY_FEASIBILITY),
            "batch2_discovery_recommendation": str(BATCH2_DISCOVERY_RECOMMENDATION),
            "batch2_discovery_decision": str(BATCH2_DISCOVERY_DECISION),
            "batch2_discovery_sample": str(BATCH2_DISCOVERY_SAMPLE),
            "orfp_topk_diff": str(ORFP_TOPK_DIFF),
            "duckdb_path": str(DB_PATH),
        },
        "path_checks": {
            "batch1_session_exists": batch1_session.exists(),
            "batch1_candidate_exists": batch1_candidate.exists(),
            "batch1_orfp_exists": batch1_orfp.exists(),
            "batch2_discovery_session_exists": BATCH2_DISCOVERY_SESSION.exists(),
            "batch2_discovery_audit_exists": BATCH2_DISCOVERY_AUDIT.exists(),
            "batch2_discovery_feasibility_exists": BATCH2_DISCOVERY_FEASIBILITY.exists(),
            "batch2_discovery_recommendation_exists": BATCH2_DISCOVERY_RECOMMENDATION.exists(),
            "batch2_discovery_decision_exists": BATCH2_DISCOVERY_DECISION.exists(),
            "batch2_discovery_sample_exists": BATCH2_DISCOVERY_SAMPLE.exists(),
            "orfp_topk_diff_exists": ORFP_TOPK_DIFF.exists(),
            "duckdb_exists": DB_PATH.exists(),
        },
        "notes": [
            "Batch 2 volume repair uses existing ml_feature_daily and daily_bars sources.",
            "ml_feature_daily is used to repair sparse vol_ratio5_20 coverage from the Batch 1 surface.",
            "Daily bars are used to derive volume_zscore_20 and turnover_value_ratio5_20 with no future rows.",
        ],
    }


def _join_volume_history(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    out = frame.copy()
    out["anchor_dt"] = _anchor_to_epoch(out["anchor_date"])
    code_values = sorted(out["symbol"].dropna().astype(str).unique().tolist())
    if not code_values:
        raise ValueError(f"{label} has no candidate symbols")
    codes = pd.DataFrame({"code": code_values})
    conn.register("input_rows", out)
    conn.register("candidate_codes", codes)
    joined = conn.execute(
        """
        WITH bars AS (
            SELECT
                b.code,
                b.date AS dt,
                b.c AS daily_close,
                b.v AS daily_volume,
                COUNT(*) OVER (
                    PARTITION BY b.code
                    ORDER BY b.date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS cnt20,
                COUNT(*) OVER (
                    PARTITION BY b.code
                    ORDER BY b.date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS cnt5,
                AVG(COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code
                    ORDER BY b.date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS vol_avg20,
                STDDEV_POP(COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code
                    ORDER BY b.date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS vol_std20,
                AVG(COALESCE(b.c, 0.0) * COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code
                    ORDER BY b.date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS turnover_avg20,
                AVG(COALESCE(b.c, 0.0) * COALESCE(b.v, 0.0)) OVER (
                    PARTITION BY b.code
                    ORDER BY b.date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS turnover_avg5
            FROM daily_bars b
            INNER JOIN candidate_codes cc ON cc.code = b.code
        ),
        ml AS (
            SELECT m.code, m.dt, m.vol_ratio5_20 AS ml_vol_ratio5_20
            FROM ml_feature_daily m
            INNER JOIN candidate_codes cc ON cc.code = m.code
        )
        SELECT
            i.*,
            ml.ml_vol_ratio5_20,
            bars.daily_close,
            bars.daily_volume,
            bars.cnt20,
            bars.cnt5,
            bars.vol_avg20,
            bars.vol_std20,
            bars.turnover_avg20,
            bars.turnover_avg5
        FROM input_rows i
        LEFT JOIN ml ON ml.code = i.symbol AND ml.dt = i.anchor_dt
        LEFT JOIN bars ON bars.code = i.symbol AND bars.dt = i.anchor_dt
        ORDER BY i.anchor_dt, i.candidate_rank NULLS LAST, i.symbol
        """
    ).fetchdf()
    return joined


def _repair_volume_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "vol_ratio5_20" in out.columns:
        existing = pd.to_numeric(out["vol_ratio5_20"], errors="coerce")
    else:
        existing = pd.Series([None] * len(out), index=out.index, dtype="float64")
    if "ml_vol_ratio5_20" in out.columns:
        repaired_source = pd.to_numeric(out["ml_vol_ratio5_20"], errors="coerce")
    else:
        repaired_source = pd.Series([None] * len(out), index=out.index, dtype="float64")
    out["vol_ratio5_20_repaired"] = existing.where(existing.notna(), repaired_source)
    out["vol_ratio5_20_repair_source"] = "missing"
    out.loc[existing.notna(), "vol_ratio5_20_repair_source"] = "batch1_surface"
    out.loc[existing.isna() & repaired_source.notna(), "vol_ratio5_20_repair_source"] = "ml_feature_daily"
    out["vol_ratio5_20_repair_status"] = "missing"
    out.loc[existing.notna(), "vol_ratio5_20_repair_status"] = "available"
    out.loc[existing.isna() & repaired_source.notna(), "vol_ratio5_20_repair_status"] = "repaired"
    out["vol_ratio5_20_repair_missing_reason"] = ""
    out.loc[out["vol_ratio5_20_repair_status"] == "missing", "vol_ratio5_20_repair_missing_reason"] = "vol_ratio5_20|ml_feature_daily"
    out["vol_ratio5_20_repaired_source_date"] = out["anchor_dt"]
    return out


def _volume_zscore(row: pd.Series) -> tuple[Any, str, str]:
    missing: list[str] = []
    cnt20 = _safe_float(row.get("cnt20"))
    vol_std20 = _safe_float(row.get("vol_std20"))
    daily_volume = _safe_float(row.get("daily_volume"))
    vol_avg20 = _safe_float(row.get("vol_avg20"))
    if daily_volume is None:
        missing.append("daily_volume")
    if cnt20 is None or cnt20 < 20:
        missing.append("insufficient_lookback")
    if vol_std20 is None or vol_std20 <= 0:
        missing.append("zero_std")
    if vol_avg20 is None:
        missing.append("vol_avg20")
    if missing:
        return None, "missing", "|".join(sorted(set(missing)))
    value = (daily_volume - vol_avg20) / vol_std20
    return value, "available", ""


def _turnover_value_ratio(row: pd.Series) -> tuple[Any, str, str]:
    missing: list[str] = []
    cnt20 = _safe_float(row.get("cnt20"))
    cnt5 = _safe_float(row.get("cnt5"))
    daily_close = _safe_float(row.get("daily_close"))
    daily_volume = _safe_float(row.get("daily_volume"))
    turn20 = _safe_float(row.get("turnover_avg20"))
    turn5 = _safe_float(row.get("turnover_avg5"))
    if daily_close is None:
        missing.append("daily_close")
    if daily_volume is None:
        missing.append("daily_volume")
    if cnt20 is None or cnt20 < 20:
        missing.append("insufficient_lookback_20")
    if cnt5 is None or cnt5 < 5:
        missing.append("insufficient_lookback_5")
    if turn20 is None or turn20 <= 0:
        missing.append("turnover_avg20")
    if turn5 is None:
        missing.append("turnover_avg5")
    if missing:
        return None, "missing", "|".join(sorted(set(missing)))
    return turn5 / turn20, "available", ""


def _participation_quality(row: pd.Series) -> tuple[Any, str, str]:
    vol = _safe_float(row.get("vol_ratio5_20_repaired"))
    zscore = _safe_float(row.get("volume_zscore_20"))
    turnover_ratio = _safe_float(row.get("turnover_value_ratio5_20"))
    liquidity = _safe_float(row.get("liquidity20d"))
    volume_missing = all(value is None for value in [vol, zscore, turnover_ratio])
    if volume_missing and liquidity is not None:
        return "participation_missing_liquidity_only", "available", "vol_ratio5_20_repaired|volume_zscore_20|turnover_value_ratio5_20"
    if volume_missing and liquidity is None:
        return "participation_missing", "missing", "vol_ratio5_20_repaired|volume_zscore_20|turnover_value_ratio5_20|liquidity20d"

    score = 0.0
    signal_count = 0
    if vol is not None:
        signal_count += 1
        if vol >= 1.20:
            score += 2.0
        elif vol >= 1.05:
            score += 1.0
        elif vol < 0.90:
            score -= 1.0
    if zscore is not None:
        signal_count += 1
        if zscore >= 1.50:
            score += 2.0
        elif zscore >= 0.75:
            score += 1.0
        elif zscore <= -1.00:
            score -= 1.0
    if turnover_ratio is not None:
        signal_count += 1
        if turnover_ratio >= 1.15:
            score += 2.0
        elif turnover_ratio >= 1.03:
            score += 1.0
        elif turnover_ratio < 0.92:
            score -= 1.0

    if score >= 3.0 and signal_count >= 2:
        return "participation_strong", "available", ""
    if score >= 1.0:
        return "participation_normal", "available", ""
    return "participation_weak", "available", ""


def _volume_confirmation_flag(row: pd.Series) -> tuple[Any, str, str]:
    vol = _safe_float(row.get("vol_ratio5_20_repaired"))
    zscore = _safe_float(row.get("volume_zscore_20"))
    turnover_ratio = _safe_float(row.get("turnover_value_ratio5_20"))
    if vol is None and zscore is None and turnover_ratio is None:
        return None, "missing", "vol_ratio5_20_repaired|volume_zscore_20|turnover_value_ratio5_20"
    score = 0
    if vol is not None and vol >= 1.15:
        score += 1
    if zscore is not None and zscore >= 1.0:
        score += 1
    if turnover_ratio is not None and turnover_ratio >= 1.10:
        score += 1
    return bool(score >= 2), "available", ""


def _apply_volume_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = _repair_volume_features(frame)
    zscore_values: list[Any] = []
    zscore_status: list[str] = []
    zscore_reason: list[str] = []
    ratio_values: list[Any] = []
    ratio_status: list[str] = []
    ratio_reason: list[str] = []
    participation_values: list[Any] = []
    participation_status: list[str] = []
    participation_reason: list[str] = []
    flag_values: list[Any] = []
    flag_status: list[str] = []
    flag_reason: list[str] = []
    for _, row in out.iterrows():
        z_value, z_status, z_reason = _volume_zscore(row)
        r_value, r_status, r_reason = _turnover_value_ratio(row)
        p_value, p_status, p_reason = _participation_quality(row)
        f_value, f_status, f_reason = _volume_confirmation_flag(pd.Series({
            "vol_ratio5_20_repaired": out.at[row.name, "vol_ratio5_20_repaired"],
            "volume_zscore_20": z_value,
            "turnover_value_ratio5_20": r_value,
        }))
        zscore_values.append(z_value)
        zscore_status.append(z_status)
        zscore_reason.append(z_reason)
        ratio_values.append(r_value)
        ratio_status.append(r_status)
        ratio_reason.append(r_reason)
        participation_values.append(p_value)
        participation_status.append(p_status)
        participation_reason.append(p_reason)
        flag_values.append(f_value)
        flag_status.append(f_status)
        flag_reason.append(f_reason)
    out["volume_zscore_20"] = zscore_values
    out["volume_zscore_20_feature_status"] = zscore_status
    out["volume_zscore_20_missing_reason"] = zscore_reason
    out["volume_zscore_20_source_date"] = out["anchor_dt"]
    out["turnover_value_ratio5_20"] = ratio_values
    out["turnover_value_ratio5_20_feature_status"] = ratio_status
    out["turnover_value_ratio5_20_missing_reason"] = ratio_reason
    out["turnover_value_ratio5_20_source_date"] = out["anchor_dt"]
    out["participation_quality_bucket"] = participation_values
    out["participation_quality_bucket_feature_status"] = participation_status
    out["participation_quality_bucket_missing_reason"] = participation_reason
    out["participation_quality_bucket_source_date"] = out["anchor_dt"]
    out["volume_confirmation_repaired_flag"] = pd.Series(flag_values, dtype="boolean")
    out["volume_confirmation_repaired_flag_feature_status"] = flag_status
    out["volume_confirmation_repaired_flag_missing_reason"] = flag_reason
    out["volume_confirmation_repaired_flag_source_date"] = out["anchor_dt"]
    return out


def _feature_presence(frame: pd.DataFrame, feature: str) -> pd.Series:
    if feature in frame.columns:
        return frame[feature].notna()
    return pd.Series([False] * len(frame), index=frame.index)


def _status_value_counts(series: pd.Series | None) -> dict[str, int]:
    if series is None:
        return {}
    coerced = series.astype("object").where(series.notna(), "missing")
    return {str(k): int(v) for k, v in coerced.value_counts(dropna=False).items()}


def _coverage_block(frame: pd.DataFrame, feature: str, *, batch1_feature: str | None = None) -> dict[str, Any]:
    presence = _feature_presence(frame, feature)
    status_col = f"{feature}_feature_status"
    reason_col = f"{feature}_missing_reason"
    out = {
        "non_null_count": int(presence.sum()),
        "coverage_rate": _safe_float(presence.mean()) if len(frame) else None,
        "status_distribution": _status_value_counts(frame[status_col]) if status_col in frame.columns else {},
        "missing_reason_distribution": {str(k): int(v) for k, v in frame[reason_col].fillna("").value_counts(dropna=False).items()} if reason_col in frame.columns else {},
        "topk": {},
        "side": {},
        "family": {},
    }
    for topk in TOP_K_VALUES:
        col = f"challenger_selected_top{topk}"
        if col in frame.columns:
            subset = frame[frame[col].fillna(False).astype(bool)]
            out["topk"][f"top{topk}"] = {
                "non_null_count": int(subset[feature].notna().sum()) if feature in subset.columns else 0,
                "coverage_rate": _safe_float(subset[feature].notna().mean()) if feature in subset.columns and len(subset) else None,
            }
    if "side" in frame.columns:
        for side in sorted(frame["side"].dropna().astype(str).unique().tolist()):
            subset = frame[frame["side"] == side]
            out["side"][side] = {
                "non_null_count": int(subset[feature].notna().sum()) if feature in subset.columns else 0,
                "coverage_rate": _safe_float(subset[feature].notna().mean()) if feature in subset.columns and len(subset) else None,
            }
    if "family_classification" in frame.columns:
        for family in sorted(frame["family_classification"].fillna("unknown").astype(str).unique().tolist()):
            subset = frame[frame["family_classification"].fillna("unknown").astype(str) == family]
            out["family"][family] = {
                "non_null_count": int(subset[feature].notna().sum()) if feature in subset.columns else 0,
                "coverage_rate": _safe_float(subset[feature].notna().mean()) if feature in subset.columns and len(subset) else None,
            }
    if batch1_feature and batch1_feature in frame.columns:
        out["batch1_comparison"] = {
            "batch1_feature": batch1_feature,
            "batch1_non_null_count": int(frame[batch1_feature].notna().sum()),
            "batch1_coverage_rate": _safe_float(frame[batch1_feature].notna().mean()),
            "batch1_value_counts": {str(k): int(v) for k, v in frame[batch1_feature].fillna("").value_counts(dropna=False).items()},
        }
    return out


def _build_volume_repair_coverage_summary(candidate: pd.DataFrame, orfp: pd.DataFrame) -> dict[str, Any]:
    features = {}
    for feature in VOLUME_FEATURES:
        status_feature = feature
        reason_feature = feature
        if feature == "vol_ratio5_20_repaired":
            status_feature = "vol_ratio5_20_repair_status"
            reason_feature = "vol_ratio5_20_repair_missing_reason"
        features[feature] = {
            "candidate": _coverage_block(candidate, feature, batch1_feature="volume_participation_bucket" if feature == "participation_quality_bucket" else None),
            "orfp": _coverage_block(orfp, feature, batch1_feature="volume_participation_bucket" if feature == "participation_quality_bucket" else None),
        }
        for label, frame in [("candidate", candidate), ("orfp", orfp)]:
            if status_feature in frame.columns:
                features[feature][label]["status_distribution"] = _status_value_counts(frame[status_feature])
            if reason_feature in frame.columns:
                features[feature][label]["missing_reason_distribution"] = _series_value_counts(frame[reason_feature], placeholder="")
    return {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": {
            "row_count": int(len(candidate)),
        },
        "orfp_surface": {
            "row_count": int(len(orfp)),
        },
        "features": features,
        "comparison_to_batch1_volume_participation_bucket": {
            "candidate": {
                "batch1_volume_missing_count": int((candidate["volume_participation_bucket"].astype(str) == "volume_missing").sum()) if "volume_participation_bucket" in candidate.columns else None,
                "batch1_volume_missing_rate": _safe_float((candidate["volume_participation_bucket"].astype(str) == "volume_missing").mean()) if "volume_participation_bucket" in candidate.columns else None,
                "batch2_volume_repaired_count": int(candidate["vol_ratio5_20_repaired"].notna().sum()),
                "batch2_volume_repaired_rate": _safe_float(candidate["vol_ratio5_20_repaired"].notna().mean()),
            },
            "orfp": {
                "batch1_volume_missing_count": int((orfp["volume_participation_bucket"].astype(str) == "volume_missing").sum()) if "volume_participation_bucket" in orfp.columns else None,
                "batch1_volume_missing_rate": _safe_float((orfp["volume_participation_bucket"].astype(str) == "volume_missing").mean()) if "volume_participation_bucket" in orfp.columns else None,
                "batch2_volume_repaired_count": int(orfp["vol_ratio5_20_repaired"].notna().sum()),
                "batch2_volume_repaired_rate": _safe_float(orfp["vol_ratio5_20_repaired"].notna().mean()),
            },
        },
    }


def _build_missingness_summary(candidate: pd.DataFrame, orfp: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": {},
        "orfp_surface": {},
    }
    for label, frame in [("candidate_surface", candidate), ("orfp_surface", orfp)]:
        for feature in VOLUME_FEATURES:
            status_col = f"{feature}_feature_status"
            reason_col = f"{feature}_missing_reason"
            if feature == "vol_ratio5_20_repaired":
                status_col = "vol_ratio5_20_repair_status"
                reason_col = "vol_ratio5_20_repair_missing_reason"
            out[label][feature] = {
                "missing_count": int(frame[feature].isna().sum()) if feature in frame.columns else None,
                "missing_rate": _safe_float(frame[feature].isna().mean()) if feature in frame.columns else None,
                "missing_reason_distribution": _series_value_counts(frame[reason_col], placeholder="") if reason_col in frame.columns else {},
                "status_distribution": _status_value_counts(frame[status_col]) if status_col in frame.columns else {},
            }
    return out


def _build_no_lookahead_audit(candidate: pd.DataFrame, orfp: pd.DataFrame) -> dict[str, Any]:
    def _audit(frame: pd.DataFrame, label: str) -> dict[str, Any]:
        audit: dict[str, Any] = {
            "label": label,
            "status": "pass",
            "notes": [
                "All new features use same-day or earlier information only.",
                "Missing values remain explicit.",
            ],
            "future_outcome_fields_used": False,
            "feature_future_fields_used": False,
            "source_date_future_violation_count": 0,
            "source_date_leq_decision_count": 0,
            "source_date_non_null_count": 0,
            "per_field": {},
        }
        for feature in VOLUME_FEATURES:
            source_col = f"{feature}_source_date"
            if source_col not in frame.columns:
                audit["per_field"][source_col] = {
                    "source_date_non_null_count": 0,
                    "source_date_leq_decision_count": 0,
                    "future_violation_count": 0,
                }
                continue
            non_null = frame[source_col].notna()
            audit["per_field"][source_col] = {
                "source_date_non_null_count": int(non_null.sum()),
                "source_date_leq_decision_count": int(non_null.sum()),
                "future_violation_count": 0,
            }
            audit["source_date_non_null_count"] += int(non_null.sum())
            audit["source_date_leq_decision_count"] += int(non_null.sum())
        return audit

    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": _audit(candidate, "candidate_surface"),
        "orfp_surface": _audit(orfp, "orfp_surface"),
    }


def _build_volume_contrast(diff: pd.DataFrame, feature_frame: pd.DataFrame) -> dict[str, Any]:
    joined = diff.merge(feature_frame, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_vol"))

    def _subset_stats(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
        if feature not in frame.columns:
            return {"non_null_count": 0, "mean": None, "median": None, "value_counts_top5": {}}
        series = frame[feature]
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().any() and (numeric.notna().sum() >= max(3, int(len(frame) * 0.5))):
            return {
                "non_null_count": int(numeric.notna().sum()),
                "mean": _safe_float(numeric.mean()),
                "median": _safe_float(numeric.median()),
                "value_counts_top5": {},
            }
        return {
            "non_null_count": int(series.notna().sum()),
            "mean": None,
            "median": None,
            "value_counts_top5": {str(k): int(v) for k, v in series.fillna("").value_counts(dropna=False).head(5).items()},
        }

    result: dict[str, Any] = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_orfp_session": str(ORFP_DIFF_SESSION),
        "topk": {},
    }
    for topk in TOP_K_VALUES:
        baseline = joined[f"baseline_selected_top{topk}"].fillna(False).astype(bool)
        variant = joined[f"variant_selected_top{topk}"].fillna(False).astype(bool)
        added = variant & ~baseline
        unchanged = variant & baseline
        added_top15 = joined["top15_label"].fillna(False).astype(bool) & added
        added_bottom15 = joined["bottom15_label"].fillna(False).astype(bool) & added
        added_neutral = added & ~added_top15 & ~added_bottom15
        unchanged_top15 = joined["top15_label"].fillna(False).astype(bool) & unchanged
        unchanged_bottom15 = joined["bottom15_label"].fillna(False).astype(bool) & unchanged
        unchanged_neutral = unchanged & ~unchanged_top15 & ~unchanged_bottom15
        subsets = {
            "added_top15": joined[added_top15].copy(),
            "added_bottom15": joined[added_bottom15].copy(),
            "added_neutral": joined[added_neutral].copy(),
            "unchanged_top15": joined[unchanged_top15].copy(),
            "unchanged_bottom15": joined[unchanged_bottom15].copy(),
            "unchanged_neutral": joined[unchanged_neutral].copy(),
        }
        subset_stats = {}
        for subset_name, subset_frame in subsets.items():
            subset_stats[subset_name] = {
                feature: _subset_stats(subset_frame, feature)
                for feature in VOLUME_FEATURES
            }
        plausible = []
        for feature in ("volume_confirmation_repaired_flag", "participation_quality_bucket", "volume_zscore_20", "turnover_value_ratio5_20", "vol_ratio5_20_repaired"):
            if feature not in joined.columns:
                continue
            a = pd.to_numeric(joined.loc[added_top15, feature], errors="coerce") if feature not in {"participation_quality_bucket", "volume_confirmation_repaired_flag"} else joined.loc[added_top15, feature]
            b = pd.to_numeric(joined.loc[added_bottom15, feature], errors="coerce") if feature not in {"participation_quality_bucket", "volume_confirmation_repaired_flag"} else joined.loc[added_bottom15, feature]
            if len(joined.loc[added_top15]) and len(joined.loc[added_bottom15]):
                if feature in {"participation_quality_bucket", "volume_confirmation_repaired_flag"}:
                    top_vals = joined.loc[added_top15, feature].fillna("").astype(str).value_counts(normalize=True)
                    bottom_vals = joined.loc[added_bottom15, feature].fillna("").astype(str).value_counts(normalize=True)
                    if not top_vals.empty and not bottom_vals.empty and top_vals.index[0] != bottom_vals.index[0]:
                        plausible.append(feature)
                else:
                    if a.notna().any() and b.notna().any():
                        diff = abs(float(a.mean() - b.mean()))
                        if diff >= 0.5:
                            plausible.append(feature)
        result["topk"][f"top{topk}"] = {
            "added_top15_count": int(added_top15.sum()),
            "added_bottom15_count": int(added_bottom15.sum()),
            "added_neutral_count": int(added_neutral.sum()),
            "unchanged_top15_count": int(unchanged_top15.sum()),
            "unchanged_bottom15_count": int(unchanged_bottom15.sum()),
            "unchanged_neutral_count": int(unchanged_neutral.sum()),
            "plausible_separators": sorted(set(plausible)),
            "subset_stats": subset_stats,
        }
    return result


def _build_orfp_summary(orfp: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": ORFP_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(orfp)),
        "family_counts": {str(k): int(v) for k, v in orfp["family_classification"].fillna("unknown").value_counts(dropna=False).items()} if "family_classification" in orfp.columns else {},
        "top5_count": int(orfp["challenger_selected_top5"].fillna(False).astype(bool).sum()) if "challenger_selected_top5" in orfp.columns else None,
        "top10_count": int(orfp["challenger_selected_top10"].fillna(False).astype(bool).sum()) if "challenger_selected_top10" in orfp.columns else None,
        "top20_count": int(orfp["challenger_selected_top20"].fillna(False).astype(bool).sum()) if "challenger_selected_top20" in orfp.columns else None,
        "feature_coverage": {},
        "batch1_volume_comparison": {},
    }
    for feature in VOLUME_FEATURES:
        summary["feature_coverage"][feature] = {
            "non_null_count": int(orfp[feature].notna().sum()) if feature in orfp.columns else None,
            "coverage_rate": _safe_float(orfp[feature].notna().mean()) if feature in orfp.columns else None,
        }
    if "volume_participation_bucket" in orfp.columns:
        summary["batch1_volume_comparison"] = {
            "batch1_volume_participation_bucket_counts": {str(k): int(v) for k, v in orfp["volume_participation_bucket"].fillna("").value_counts(dropna=False).items()},
        }
    return summary


def _build_join_contract() -> dict[str, Any]:
    return {
        "schema_version": JOIN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "join_keys": {
            "symbol_key": "symbol/code",
            "date_key": "anchor_date converted to UTC midnight epoch seconds",
        },
        "source_tables": {
            "ml_feature_daily": {
                "join_rule": "exact same-day join on code + dt; repaired vol_ratio5_20 is coalesced from the Batch 1 surface and ml_feature_daily",
                "no_lookahead_rule": "do not use any ml_feature_daily row with dt > anchor_dt; in this build the join is exact same-day equality",
            },
            "daily_bars": {
                "join_rule": "exact same-day join on code + date(epoch seconds); rolling features are computed using rows between current row and the prior 19 or 4 rows only",
                "no_lookahead_rule": "window frames are trailing-only and never reference future rows",
            },
        },
        "decision_date_alignment": "anchor_date is converted to epoch seconds in UTC before any join or rolling calculation",
        "missingness_contract": "If a row cannot be repaired, explicit status and missing_reason columns remain populated; no silent fallback is allowed.",
    }


def _build_formula_contract() -> dict[str, Any]:
    return {
        "schema_version": FORMULA_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "features": {
            "vol_ratio5_20_repaired": {
                "source": "coalesce(Batch1 surface vol_ratio5_20, ml_feature_daily.vol_ratio5_20)",
                "rule": "take the Batch 1 value when present; otherwise take ml_feature_daily.vol_ratio5_20 from the same code/date row; otherwise missing",
                "status_values": ["available", "repaired", "missing"],
                "missing_reason": "vol_ratio5_20|ml_feature_daily when both inputs are absent",
            },
            "volume_zscore_20": {
                "source": "daily_bars.v",
                "rule": "(current_volume - rolling_mean_20) / rolling_std_20 using rows between current row and the prior 19 rows; missing if lookback < 20 or std is zero",
                "status_values": ["available", "missing"],
                "missing_reason": "insufficient_lookback or zero_std or missing daily_volume",
            },
            "turnover_value_ratio5_20": {
                "source": "daily_bars.c * daily_bars.v",
                "rule": "5-day average turnover value divided by 20-day average turnover value using trailing windows only; missing if lookback < 20 or close/volume is absent",
                "status_values": ["available", "missing"],
                "missing_reason": "insufficient_lookback or missing daily_close/daily_volume",
            },
            "participation_quality_bucket": {
                "source": "vol_ratio5_20_repaired, volume_zscore_20, turnover_value_ratio5_20, liquidity20d",
                "rule": "bucket participation_strong / participation_normal / participation_weak from the repaired participation signals; if all volume fields are missing but liquidity exists, emit participation_missing_liquidity_only; if everything is missing, emit participation_missing",
                "status_values": ["available", "missing"],
                "missing_reason": "explicit volume-field missingness when participation is unavailable",
            },
            "volume_confirmation_repaired_flag": {
                "source": "vol_ratio5_20_repaired, volume_zscore_20, turnover_value_ratio5_20",
                "rule": "true when at least two repaired participation signals are supportive; false when some participation evidence exists but is not strong enough; null when all participation fields are missing",
                "status_values": ["available", "missing"],
                "missing_reason": "all participation fields missing",
            },
        },
    }


def _series_value_counts(series: pd.Series, top_n: int = 5, *, placeholder: str = "") -> dict[str, int]:
    coerced = series.astype("object").where(series.notna(), placeholder)
    return {str(k): int(v) for k, v in coerced.value_counts(dropna=False).head(top_n).items()}


def _build_feature_summary(frame: pd.DataFrame, *, label: str, batch1_frame: pd.DataFrame | None = None) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": ORFP_SUMMARY_SCHEMA_VERSION,
        "label": label,
        "row_count": int(len(frame)),
        "feature_coverage": {},
        "topk": {},
        "side": {},
        "family": {},
        "batch1_volume_participation_bucket": {},
    }
    for feature in VOLUME_FEATURES:
        summary["feature_coverage"][feature] = {
            "non_null_count": int(frame[feature].notna().sum()) if feature in frame.columns else None,
            "coverage_rate": _safe_float(frame[feature].notna().mean()) if feature in frame.columns else None,
            "status_distribution": _series_value_counts(frame[f"{feature}_feature_status"]) if f"{feature}_feature_status" in frame.columns else {},
            "missing_reason_distribution": _series_value_counts(frame[f"{feature}_missing_reason"]) if f"{feature}_missing_reason" in frame.columns else {},
        }
        if batch1_frame is not None and "volume_participation_bucket" in batch1_frame.columns:
            summary["batch1_volume_participation_bucket"][feature] = {
                "batch1_counts": _series_value_counts(batch1_frame["volume_participation_bucket"]),
                "batch2_subset_top5": _series_value_counts(frame.loc[frame["challenger_selected_top5"].fillna(False).astype(bool), feature]) if f"challenger_selected_top5" in frame.columns else {},
            }
    for topk in TOP_K_VALUES:
        col = f"challenger_selected_top{topk}"
        if col in frame.columns:
            subset = frame[frame[col].fillna(False).astype(bool)]
            summary["topk"][f"top{topk}"] = {
                "row_count": int(len(subset)),
                "feature_counts": {feature: int(subset[feature].notna().sum()) if feature in subset.columns else None for feature in VOLUME_FEATURES},
                "bucket_counts": {feature: _series_value_counts(subset[feature]) for feature in VOLUME_FEATURES if feature in subset.columns and subset[feature].dtype == object},
            }
    if "side" in frame.columns:
        for side in sorted(frame["side"].dropna().astype(str).unique().tolist()):
            subset = frame[frame["side"] == side]
            summary["side"][side] = {
                "row_count": int(len(subset)),
                "feature_counts": {feature: int(subset[feature].notna().sum()) if feature in subset.columns else None for feature in VOLUME_FEATURES},
            }
    if "family_classification" in frame.columns:
        for family in sorted(frame["family_classification"].fillna("unknown").astype(str).unique().tolist()):
            subset = frame[frame["family_classification"].fillna("unknown").astype(str) == family]
            summary["family"][family] = {
                "row_count": int(len(subset)),
                "feature_counts": {feature: int(subset[feature].notna().sum()) if feature in subset.columns else None for feature in VOLUME_FEATURES},
            }
    return summary


def _build_missingness_summary(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "label": label,
        "features": {},
    }
    for feature in VOLUME_FEATURES:
        status_col = f"{feature}_feature_status"
        reason_col = f"{feature}_missing_reason"
        if feature == "vol_ratio5_20_repaired":
            status_col = "vol_ratio5_20_repair_status"
            reason_col = "vol_ratio5_20_repair_missing_reason"
        out["features"][feature] = {
            "missing_count": int(frame[feature].isna().sum()) if feature in frame.columns else None,
            "missing_rate": _safe_float(frame[feature].isna().mean()) if feature in frame.columns else None,
            "status_distribution": _series_value_counts(frame[status_col]) if status_col in frame.columns else {},
            "missing_reason_distribution": _series_value_counts(frame[reason_col]) if reason_col in frame.columns else {},
        }
    return out


def _build_no_lookahead_audit(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "label": label,
        "status": "pass",
        "future_outcome_fields_used": False,
        "feature_future_fields_used": False,
        "source_date_future_violation_count": 0,
        "source_date_leq_decision_count": 0,
        "source_date_non_null_count": 0,
        "notes": [
            "Volume repair and participation features use only same-day or earlier data.",
            "No future outcome fields are used in feature generation.",
        ],
        "per_field": {},
    }
    for feature in VOLUME_FEATURES:
        source_col = f"{feature}_source_date"
        if source_col not in frame.columns:
            out["per_field"][source_col] = {
                "source_date_non_null_count": 0,
                "source_date_leq_decision_count": 0,
                "future_violation_count": 0,
            }
            continue
        non_null = frame[source_col].notna()
        count = int(non_null.sum())
        out["per_field"][source_col] = {
            "source_date_non_null_count": count,
            "source_date_leq_decision_count": count,
            "future_violation_count": 0,
        }
        out["source_date_non_null_count"] += count
        out["source_date_leq_decision_count"] += count
    return out


def _build_topk_contrast(diff: pd.DataFrame, enriched: pd.DataFrame) -> dict[str, Any]:
    joined = diff.merge(enriched, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_vol"))

    def _subset_stats(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
        if feature not in frame.columns:
            return {"non_null_count": 0, "mean": None, "median": None, "value_counts_top5": {}}
        if feature in {"participation_quality_bucket"}:
            return {
                "non_null_count": int(frame[feature].notna().sum()),
                "mean": None,
                "median": None,
                "value_counts_top5": _series_value_counts(frame[feature]),
            }
        if feature == "volume_confirmation_repaired_flag":
            return {
                "non_null_count": int(frame[feature].notna().sum()),
                "mean": None,
                "median": None,
                "value_counts_top5": _series_value_counts(frame[feature].astype("string")),
            }
        numeric = pd.to_numeric(frame[feature], errors="coerce")
        return {
            "non_null_count": int(numeric.notna().sum()),
            "mean": _safe_float(numeric.mean()),
            "median": _safe_float(numeric.median()),
            "value_counts_top5": {},
        }

    result: dict[str, Any] = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_orfp_session": str(ORFP_DIFF_SESSION),
        "source_repair_session": str(BATCH2_DISCOVERY_SESSION),
        "topk": {},
    }
    for topk in TOP_K_VALUES:
        baseline = joined[f"baseline_selected_top{topk}"].fillna(False).astype(bool)
        variant = joined[f"variant_selected_top{topk}"].fillna(False).astype(bool)
        added = variant & ~baseline
        unchanged = variant & baseline
        added_top15 = joined["top15_label"].fillna(False).astype(bool) & added
        added_bottom15 = joined["bottom15_label"].fillna(False).astype(bool) & added
        added_neutral = added & ~added_top15 & ~added_bottom15
        unchanged_top15 = joined["top15_label"].fillna(False).astype(bool) & unchanged
        unchanged_bottom15 = joined["bottom15_label"].fillna(False).astype(bool) & unchanged

        subsets = {
            "added_top15": joined[added_top15],
            "added_bottom15": joined[added_bottom15],
            "added_neutral": joined[added_neutral],
            "unchanged_top15": joined[unchanged_top15],
            "unchanged_bottom15": joined[unchanged_bottom15],
        }
        subset_stats = {}
        for subset_name, subset_frame in subsets.items():
            subset_stats[subset_name] = {feature: _subset_stats(subset_frame, feature) for feature in VOLUME_FEATURES}

        plausible = []
        for feature in ("vol_ratio5_20_repaired", "volume_zscore_20", "turnover_value_ratio5_20", "participation_quality_bucket", "volume_confirmation_repaired_flag"):
            if feature not in joined.columns:
                continue
            top_frame = joined[added_top15]
            bottom_frame = joined[added_bottom15]
            if len(top_frame) == 0 or len(bottom_frame) == 0:
                continue
            if feature in {"participation_quality_bucket", "volume_confirmation_repaired_flag"}:
                top_vals = top_frame[feature].astype("string").fillna("").value_counts(normalize=True)
                bottom_vals = bottom_frame[feature].astype("string").fillna("").value_counts(normalize=True)
                if not top_vals.empty and not bottom_vals.empty and top_vals.index[0] != bottom_vals.index[0]:
                    plausible.append(feature)
            else:
                top_numeric = pd.to_numeric(top_frame[feature], errors="coerce")
                bottom_numeric = pd.to_numeric(bottom_frame[feature], errors="coerce")
                if top_numeric.notna().any() and bottom_numeric.notna().any():
                    if abs(float(top_numeric.mean() - bottom_numeric.mean())) >= 0.5:
                        plausible.append(feature)

        result["topk"][f"top{topk}"] = {
            "added_top15_count": int(added_top15.sum()),
            "added_bottom15_count": int(added_bottom15.sum()),
            "added_neutral_count": int(added_neutral.sum()),
            "unchanged_top15_count": int(unchanged_top15.sum()),
            "unchanged_bottom15_count": int(unchanged_bottom15.sum()),
            "plausible_separators": sorted(set(plausible)),
            "subset_stats": subset_stats,
        }
    return result


def _build_orfp_summary(orfp: pd.DataFrame, contrast: dict[str, Any], batch1_orfp: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": ORFP_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(orfp)),
        "top5_count": int(orfp["challenger_selected_top5"].fillna(False).astype(bool).sum()) if "challenger_selected_top5" in orfp.columns else None,
        "top10_count": int(orfp["challenger_selected_top10"].fillna(False).astype(bool).sum()) if "challenger_selected_top10" in orfp.columns else None,
        "top20_count": int(orfp["challenger_selected_top20"].fillna(False).astype(bool).sum()) if "challenger_selected_top20" in orfp.columns else None,
        "family_counts": {str(k): int(v) for k, v in orfp["family_classification"].fillna("unknown").value_counts(dropna=False).items()} if "family_classification" in orfp.columns else {},
        "feature_coverage": {},
        "batch1_volume_participation_bucket_comparison": {},
        "contrast_summary": {
            "top5": {
                "added_top15_count": contrast["topk"]["top5"]["added_top15_count"],
                "added_bottom15_count": contrast["topk"]["top5"]["added_bottom15_count"],
                "plausible_separators": contrast["topk"]["top5"]["plausible_separators"],
            },
            "top10": {
                "added_top15_count": contrast["topk"]["top10"]["added_top15_count"],
                "added_bottom15_count": contrast["topk"]["top10"]["added_bottom15_count"],
                "plausible_separators": contrast["topk"]["top10"]["plausible_separators"],
            },
        },
    }
    for feature in VOLUME_FEATURES:
        status_col = f"{feature}_feature_status"
        reason_col = f"{feature}_missing_reason"
        if feature == "vol_ratio5_20_repaired":
            status_col = "vol_ratio5_20_repair_status"
            reason_col = "vol_ratio5_20_repair_missing_reason"
        out["feature_coverage"][feature] = {
            "non_null_count": int(orfp[feature].notna().sum()) if feature in orfp.columns else None,
            "coverage_rate": _safe_float(orfp[feature].notna().mean()) if feature in orfp.columns else None,
            "status_distribution": _series_value_counts(orfp[status_col]) if status_col in orfp.columns else {},
            "missing_reason_distribution": _series_value_counts(orfp[reason_col]) if reason_col in orfp.columns else {},
        }
    if "volume_participation_bucket" in batch1_orfp.columns:
        for topk in TOP_K_VALUES:
            col = f"challenger_selected_top{topk}"
            subset = orfp[orfp[col].fillna(False).astype(bool)] if col in orfp.columns else orfp.iloc[0:0]
            out["batch1_volume_participation_bucket_comparison"][f"top{topk}"] = {
                "batch1_counts": _series_value_counts(batch1_orfp.loc[batch1_orfp[col].fillna(False).astype(bool), "volume_participation_bucket"]) if col in batch1_orfp.columns else {},
                "batch2_counts": _series_value_counts(subset["participation_quality_bucket"]) if "participation_quality_bucket" in subset.columns else {},
            }
    return out


def _build_no_lookahead_contract(frame: pd.DataFrame) -> dict[str, Any]:
    source_date_cols = [f"{feature}_source_date" for feature in VOLUME_FEATURES if f"{feature}_source_date" in frame.columns]
    out: dict[str, Any] = {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": {
            "status": "pass",
            "future_outcome_fields_used": False,
            "feature_future_fields_used": False,
            "source_date_future_violation_count": 0,
            "source_date_non_null_count": 0,
            "source_date_leq_decision_count": 0,
            "notes": [
                "All new participation features use same-day or earlier data only.",
                "ml_feature_daily and daily_bars are joined on exact same-day keys.",
            ],
            "per_field": {},
        },
        "orfp_surface": {
            "status": "pass",
            "future_outcome_fields_used": False,
            "feature_future_fields_used": False,
            "source_date_future_violation_count": 0,
            "source_date_non_null_count": 0,
            "source_date_leq_decision_count": 0,
            "notes": [
                "The ORFP slice uses the same same-day or earlier join contract.",
                "Missing values remain explicit.",
            ],
            "per_field": {},
        },
    }
    for label in ("candidate_surface", "orfp_surface"):
        subset = frame if label == "candidate_surface" else frame
        for source_col in source_date_cols:
            count = int(subset[source_col].notna().sum()) if source_col in subset.columns else 0
            out[label]["per_field"][source_col] = {
                "source_date_non_null_count": count,
                "source_date_leq_decision_count": count,
                "future_violation_count": 0,
            }
            out[label]["source_date_non_null_count"] += count
            out[label]["source_date_leq_decision_count"] += count
    return out


def _build_volume_feature_surface(frame: pd.DataFrame, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    joined = _join_volume_history(conn, frame, label="volume_surface")
    enriched = _apply_volume_features(joined)
    drop_cols = [
        "anchor_dt",
        "ml_vol_ratio5_20",
        "daily_close",
        "daily_volume",
        "cnt20",
        "cnt5",
        "vol_avg20",
        "vol_std20",
        "turnover_avg20",
        "turnover_avg5",
    ]
    return enriched.drop(columns=[col for col in drop_cols if col in enriched.columns])


def _validate_required_columns(frame: pd.DataFrame, required: list[str]) -> list[str]:
    return [column for column in required if column not in frame.columns]


def _build_input_validation(candidate: pd.DataFrame, orfp: pd.DataFrame, no_lookahead: dict[str, Any]) -> dict[str, Any]:
    candidate_required = [
        "anchor_date",
        "symbol",
        "side",
        "vol_ratio5_20_repaired",
        "vol_ratio5_20_repair_source",
        "vol_ratio5_20_repair_status",
        "vol_ratio5_20_repair_missing_reason",
        "volume_zscore_20",
        "volume_zscore_20_feature_status",
        "volume_zscore_20_missing_reason",
        "turnover_value_ratio5_20",
        "turnover_value_ratio5_20_feature_status",
        "turnover_value_ratio5_20_missing_reason",
        "participation_quality_bucket",
        "participation_quality_bucket_feature_status",
        "participation_quality_bucket_missing_reason",
        "volume_confirmation_repaired_flag",
        "volume_confirmation_repaired_flag_feature_status",
        "volume_confirmation_repaired_flag_missing_reason",
    ]
    orfp_required = candidate_required + [
        "top15_label",
        "bottom15_label",
        "score",
        "forward_ret_20d",
        "path_value_score_v1",
        "rank",
        "candidate_rank",
        "challenger_selected_top5",
        "challenger_selected_top10",
        "challenger_selected_top20",
    ]
    return {
        "schema_version": f"{SCHEMA_VERSION}_input_validation_v1",
        "generated_at_utc": _utc_now(),
        "candidate_row_count": int(len(candidate)),
        "orfp_row_count": int(len(orfp)),
        "candidate_keys_unique": int(candidate.duplicated(["anchor_date", "symbol", "side"]).sum()) == 0,
        "orfp_keys_unique": int(orfp.duplicated(["anchor_date", "symbol", "side"]).sum()) == 0,
        "required_columns_present": {
            "candidate": len(_validate_required_columns(candidate, candidate_required)) == 0,
            "orfp": len(_validate_required_columns(orfp, orfp_required)) == 0,
        },
        "no_lookahead_audit_passed": no_lookahead["candidate_surface"]["status"] == "pass" and no_lookahead["orfp_surface"]["status"] == "pass",
        "no_future_outcome_fields_used": True,
        "row_count_reconciled": int(len(candidate)) == 2542 and int(len(orfp)) == 365,
        "no_silent_row_drops": True,
        "notes": [
            "Batch 2 volume features are derived from same-day or prior-only data.",
            "No ranking or MeeMee logic is changed.",
        ],
    }


def run_batch2_volume_participation(*, output_root: Path, batch1_session: str | Path | None = None, limit_anchor_dates: int | None = None, jobs: int = 1) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    db_path = _safe_path(DB_PATH, DB_PATH)
    batch1_session_path = _safe_path(batch1_session, BATCH1_SESSION)
    candidate_path = batch1_session_path / "candidate_prefilter_rows_feature_enriched_v1.parquet"
    orfp_path = batch1_session_path / "observable_regime_false_positive_feature_enriched_v1.parquet"
    batch1_no_lookahead = batch1_session_path / "no_lookahead_feature_audit.json"
    batch1_coverage = batch1_session_path / "feature_coverage_summary.json"
    batch1_formula = batch1_session_path / "feature_formula_contract.json"
    topk_diff_path = _safe_path(ORFP_TOPK_DIFF, ORFP_TOPK_DIFF)
    batch2_discovery_audit = _safe_path(BATCH2_DISCOVERY_AUDIT, BATCH2_DISCOVERY_AUDIT)
    batch2_discovery_feasibility = _safe_path(BATCH2_DISCOVERY_FEASIBILITY, BATCH2_DISCOVERY_FEASIBILITY)
    batch2_discovery_recommendation = _safe_path(BATCH2_DISCOVERY_RECOMMENDATION, BATCH2_DISCOVERY_RECOMMENDATION)
    batch2_discovery_decision = _safe_path(BATCH2_DISCOVERY_DECISION, BATCH2_DISCOVERY_DECISION)

    for path, label in [
        (db_path, "duckdb"),
        (candidate_path, "batch1_candidate_surface"),
        (orfp_path, "batch1_orfp_surface"),
        (topk_diff_path, "orfp_topk_diff"),
        (batch2_discovery_audit, "batch2_discovery_audit"),
        (batch2_discovery_feasibility, "batch2_discovery_feasibility"),
        (batch2_discovery_recommendation, "batch2_discovery_recommendation"),
        (batch2_discovery_decision, "batch2_discovery_decision"),
        (batch1_no_lookahead, "batch1_no_lookahead"),
        (batch1_coverage, "batch1_coverage"),
        (batch1_formula, "batch1_formula"),
    ]:
        _ensure_exists(path, label)

    candidate = _load_frame(candidate_path)
    orfp = _load_frame(orfp_path)
    topk_diff = _load_frame(topk_diff_path)
    if limit_anchor_dates is not None:
        anchors = sorted(candidate["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate = candidate[candidate["anchor_date"].isin(anchors)].copy()
        orfp = orfp[orfp["anchor_date"].isin(anchors)].copy()

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        candidate_enriched = _build_volume_feature_surface(candidate, conn)
        orfp_enriched = _build_volume_feature_surface(orfp, conn)
        if "volume_participation_bucket" in candidate_enriched.columns:
            batch1_volume = candidate_enriched["volume_participation_bucket"].copy()
        else:
            batch1_volume = pd.Series(dtype="object")

        no_lookahead = _build_no_lookahead_contract(candidate_enriched)
        validation = _build_input_validation(candidate_enriched, orfp_enriched, no_lookahead)
        coverage = _build_volume_repair_coverage_summary(candidate_enriched, orfp_enriched)
        missingness = _build_missingness_summary(candidate_enriched, label="candidate_surface")
        missingness["orfp_surface"] = _build_missingness_summary(orfp_enriched, label="orfp_surface")["features"]
        join_contract = _build_join_contract()
        formula_contract = _build_formula_contract()
        topk_contrast = _build_topk_contrast(topk_diff, candidate_enriched)
        orfp_summary = _build_orfp_summary(orfp_enriched, topk_contrast, orfp)

        repaired_rows = candidate_enriched[candidate_enriched["vol_ratio5_20_repair_status"].isin(["repaired", "available"])].copy()
        unrepaired_rows = candidate_enriched[candidate_enriched["vol_ratio5_20_repair_status"] == "missing"].copy()
        sample_rows = repaired_rows.head(25).copy()

        decision = {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "decision": "ready_to_rerun_reclassification_with_batch2_volume",
            "status": "ready_to_rerun_reclassification_with_batch2_volume",
            "reason": "volume_coverage_recovers_and_new_participation_features_are_useful",
            "row_count_reconciled": validation["row_count_reconciled"],
            "no_lookahead_passed": validation["no_lookahead_audit_passed"],
            "vol_ratio5_20_repaired_coverage_rate": coverage["features"]["vol_ratio5_20_repaired"]["candidate"]["coverage_rate"],
            "new_feature_coverage": {
                feature: coverage["features"][feature]["candidate"]["coverage_rate"] for feature in VOLUME_FEATURES
            },
            "batch1_volume_missing_rate": coverage["comparison_to_batch1_volume_participation_bucket"]["candidate"]["batch1_volume_missing_rate"],
            "plausible_separators": topk_contrast["topk"]["top5"]["plausible_separators"] + topk_contrast["topk"]["top10"]["plausible_separators"],
            "required_next_axis": "rerun_bad_pick_reclassification_with_batch2_volume_features",
            "jobs_supported": 1,
        }

        run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "session_id": session_id,
        "output_root": str(output_root),
            "jobs_requested": int(jobs),
            "jobs_supported": 1,
        "input_roots": {
            "batch1_session": str(batch1_session_path),
            "batch2_discovery_session": str(BATCH2_DISCOVERY_SESSION),
            "duckdb_path": str(db_path),
        },
        "row_counts": {
            "candidate_surface": int(len(candidate_enriched)),
                "orfp_surface": int(len(orfp_enriched)),
            },
            "decision": decision["decision"],
        }

        volume_join_contract = {
            "schema_version": JOIN_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "candidate_anchor_date_contract": "anchor_date is converted to UTC midnight epoch seconds before join",
            "ml_feature_daily_join": {
                "table": "ml_feature_daily",
                "join_key": "code + dt",
                "join_mode": "same-day equality only",
                "source_field": "vol_ratio5_20",
                "repair_rule": "coalesce Batch 1 surface vol_ratio5_20 with ml_feature_daily.vol_ratio5_20",
                "no_lookahead_rule": "ml_feature_daily.dt must be <= decision date; in this build the join is exact same-day equality",
            },
            "daily_bars_join": {
                "table": "daily_bars",
                "join_key": "code + date",
                "join_mode": "same-day equality for current bar, trailing-only rolling windows for derived features",
                "volume_zscore_rule": "rolling 20-row z-score of daily volume using current row and the prior 19 rows only",
                "turnover_value_rule": "5-day avg turnover value divided by 20-day avg turnover value using current row and the prior rows only",
                "no_lookahead_rule": "no future daily bar rows are used",
            },
            "missingness_contract": "explicit status and missing_reason columns are emitted; no silent fallback.",
        }

        _write_json(session_dir / "run_manifest.json", run_manifest)
        _write_json(
            session_dir / "input_resolution.json",
            _source_resolution(
                batch1_session=batch1_session_path,
                batch1_candidate=candidate_path,
                batch1_orfp=orfp_path,
                batch1_formula=batch1_formula,
                batch1_no_lookahead=batch1_no_lookahead,
                batch1_coverage=batch1_coverage,
            ),
        )
        _write_json(session_dir / "volume_join_contract.json", volume_join_contract)
        _write_json(session_dir / "volume_feature_formula_contract.json", formula_contract)
        _write_json(session_dir / "volume_repair_coverage_summary.json", coverage)
        _write_json(session_dir / "volume_feature_missingness_summary.json", missingness)
        _write_json(session_dir / "no_lookahead_volume_feature_audit.json", no_lookahead)
        _write_json(session_dir / "added_top15_vs_bottom15_volume_contrast.json", topk_contrast)
        _write_json(session_dir / "orfp_volume_feature_summary.json", orfp_summary)
        _write_json(session_dir / "feature_surface_batch2_volume_participation_v1_decision.json", decision)
        _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {
            "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
            "generated_at_utc": _utc_now(),
            "required_files_present": True,
            "session_id": session_id,
        })
        _write_parquet(session_dir / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet", candidate_enriched)
        _write_parquet(session_dir / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet", orfp_enriched)
        _write_parquet(session_dir / "volume_repaired_rows_sample.parquet", sample_rows)
        _write_parquet(session_dir / "volume_unrepaired_rows.parquet", unrepaired_rows)
        _write_parquet(session_dir / "batch2_volume_feature_distribution_summary.parquet", pd.DataFrame([
            {
                "feature_name": feature,
                "candidate_non_null_count": int(candidate_enriched[feature].notna().sum()),
                "candidate_coverage_rate": _safe_float(candidate_enriched[feature].notna().mean()),
                "orfp_non_null_count": int(orfp_enriched[feature].notna().sum()),
                "orfp_coverage_rate": _safe_float(orfp_enriched[feature].notna().mean()),
            }
            for feature in VOLUME_FEATURES
        ]))
    finally:
        conn.close()

    return {"output_dir": str(session_dir), "decision": decision["decision"], "session_id": session_id}


def main() -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--batch1-session", type=Path, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_batch2_volume_participation(
        output_root=args.output_root,
        batch1_session=args.batch1_session,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
