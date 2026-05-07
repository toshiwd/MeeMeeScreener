from __future__ import annotations

import argparse
import bisect
import json
import math
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


SCRIPT_NAME = "tradex_feature_surface_edinet_event_proxy_v1"
SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_input_resolution_v1"
SOURCE_PROFILE_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_source_profile_v1"
TAXONOMY_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_proxy_taxonomy_contract_v1"
FORMULA_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_edinet_event_feature_formula_contract_v1"
COVERAGE_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_edinet_event_coverage_summary_v1"
MISSINGNESS_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_edinet_event_missingness_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_no_lookahead_edinet_event_audit_v1"
CONTRAST_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_added_top15_bottom15_edinet_contrast_v1"
ORFP_SUMMARY_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_orfp_edinet_event_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_surface_edinet_event_proxy_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_surface_edinet_event_proxy_v1")
DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")

BATCH2_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
BATCH2_CANDIDATE = BATCH2_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
BATCH2_ORFP = BATCH2_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
BATCH2_NO_LOOKAHEAD = BATCH2_SESSION / "no_lookahead_volume_feature_audit.json"
ORFP_TOPK_DIFF_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
ORFP_TOPK_DIFF = ORFP_TOPK_DIFF_SESSION / "topk_membership_diff.parquet"

SOURCE_SELECTION_SESSION = Path(r"G:\Tradex\external_event_source_selection_v1\20260501T112146Z")
SOURCE_SELECTION_DECISION = SOURCE_SELECTION_SESSION / "external_event_source_selection_v1_decision.json"
SOURCE_SELECTION_COVERAGE = SOURCE_SELECTION_SESSION / "event_coverage_requirement.json"
SOURCE_SELECTION_CANDIDATES = SOURCE_SELECTION_SESSION / "external_event_source_candidates.json"
SOURCE_SELECTION_PIT = SOURCE_SELECTION_SESSION / "external_event_source_pit_assessment.json"
SOURCE_SELECTION_RECOMMENDATION = SOURCE_SELECTION_SESSION / "external_event_source_recommendation.json"

KEY_COLS = ("anchor_date", "symbol", "side")
TOP_K_VALUES = (5, 10, 20)

EDINET_FEATURES = (
    "edinet_recent_filing_flag",
    "edinet_recent_filing_count_20d",
    "edinet_recent_filing_count_5d",
    "edinet_financing_or_dilution_proxy_flag",
    "edinet_security_issuance_proxy_flag",
    "edinet_material_filing_activity_proxy_flag",
    "edinet_event_noise_bucket",
    "days_since_last_edinet_filing",
)

FEATURE_STATUS_COLUMNS = {
    "edinet_recent_filing_flag": ("edinet_recent_filing_flag_feature_status", "edinet_recent_filing_flag_missing_reason"),
    "edinet_recent_filing_count_20d": ("edinet_recent_filing_count_20d_feature_status", "edinet_recent_filing_count_20d_missing_reason"),
    "edinet_recent_filing_count_5d": ("edinet_recent_filing_count_5d_feature_status", "edinet_recent_filing_count_5d_missing_reason"),
    "edinet_financing_or_dilution_proxy_flag": (
        "edinet_financing_or_dilution_proxy_flag_feature_status",
        "edinet_financing_or_dilution_proxy_flag_missing_reason",
    ),
    "edinet_security_issuance_proxy_flag": (
        "edinet_security_issuance_proxy_flag_feature_status",
        "edinet_security_issuance_proxy_flag_missing_reason",
    ),
    "edinet_material_filing_activity_proxy_flag": (
        "edinet_material_filing_activity_proxy_flag_feature_status",
        "edinet_material_filing_activity_proxy_flag_missing_reason",
    ),
    "edinet_event_noise_bucket": (
        "edinet_event_noise_bucket_feature_status",
        "edinet_event_noise_bucket_missing_reason",
    ),
    "days_since_last_edinet_filing": (
        "days_since_last_edinet_filing_feature_status",
        "days_since_last_edinet_filing_missing_reason",
    ),
}

SECURITY_TERMS = (
    "有価証券届出書",
    "有価証券届出書（参照方式）",
    "有価証券届出書（組込方式）",
    "訂正有価証券届出書",
    "発行登録書",
    "発行登録追補書類",
    "訂正発行登録書",
)

FINANCING_TERMS = (
    "第三者割当",
    "新株予約権",
    "新株予約権付社債",
    "新株発行",
    "社債",
    "転換社債",
    "ワラント",
    "公募",
    "売出",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    text = str(value).strip().lower()
    return text in {"", "nan", "<na>", "none", "null"}


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
    if isinstance(value, date):
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(_ensure_exists(path, str(path))).copy()
    for column in KEY_COLS:
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


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


def _normalize_sec_code(value: Any) -> str:
    if _is_missing(value):
        return ""
    text = re.sub(r"[^0-9]", "", str(value).strip())
    if not text:
        return ""
    if len(text) < 4:
        text = text.zfill(4)
    return text[:4]


def _parse_date(value: Any) -> date | None:
    if _is_missing(value):
        return None
    dt = pd.to_datetime(value, errors="coerce", utc=False)
    if pd.isna(dt):
        return None
    return dt.date()


def _parse_timestamp(value: Any) -> pd.Timestamp | None:
    if _is_missing(value):
        return None
    dt = pd.to_datetime(value, errors="coerce", utc=False)
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt)


def _join_text(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        if _is_missing(value):
            continue
        text = str(value).strip()
        if text:
            parts.append(text)
    return " ".join(parts)


def _load_candidate_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    candidate = _load_frame(BATCH2_CANDIDATE)
    orfp = _load_frame(BATCH2_ORFP)
    diff = pd.read_parquet(_ensure_exists(ORFP_TOPK_DIFF, "ORFP topk diff")).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in diff.columns:
            diff[column] = diff[column].astype("string")
    return candidate, orfp, diff


def _load_edinet_documents(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    if not _table_exists(conn, "edinetdb_official_documents"):
        raise FileNotFoundError("edinetdb_official_documents table not available")
    query = """
        SELECT
            doc_id,
            nullif(trim(sec_code), '') AS sec_code,
            nullif(trim(edinet_code), '') AS edinet_code,
            nullif(trim(filer_name), '') AS filer_name,
            nullif(trim(form_code), '') AS form_code,
            nullif(trim(doc_type_code), '') AS doc_type_code,
            nullif(trim(period_start), '') AS period_start,
            nullif(trim(period_end), '') AS period_end,
            nullif(trim(submit_datetime), '') AS submit_datetime,
            nullif(trim(doc_description), '') AS doc_description,
            csv_flag,
            pdf_flag,
            xbrl_flag,
            nullif(trim(legal_status), '') AS legal_status,
            payload_json,
            fetched_at,
            json_extract_string(payload_json, '$.currentReportReason') AS current_report_reason,
            json_extract_string(payload_json, '$.ordinanceCode') AS ordinance_code,
            json_extract_string(payload_json, '$.docDescription') AS payload_doc_description,
            json_extract_string(payload_json, '$.submitDateTime') AS payload_submit_datetime,
            json_extract_string(payload_json, '$.legalStatus') AS payload_legal_status
        FROM edinetdb_official_documents
    """
    frame = conn.execute(query).fetchdf().copy()
    frame["submit_dt"] = pd.to_datetime(frame["submit_datetime"], errors="coerce")
    frame["submit_date"] = frame["submit_dt"].dt.date
    frame["mapped_sec_code_direct"] = frame["sec_code"].map(_normalize_sec_code)
    return frame


def _build_candidate_symbol_map(conn: duckdb.DuckDBPyConnection) -> dict[str, str]:
    frame = conn.execute("SELECT sec_code, edinet_code FROM edinetdb_company_map").fetchdf()
    mapping: dict[str, str] = {}
    for row in frame.itertuples(index=False):
        sec_code = _normalize_sec_code(row.sec_code)
        edinet_code = str(row.edinet_code or "").strip()
        if sec_code and edinet_code:
            mapping[sec_code] = edinet_code
    return mapping


def _classify_document(row: pd.Series) -> str:
    text = _join_text(
        row.get("doc_description"),
        row.get("payload_doc_description"),
        row.get("current_report_reason"),
        row.get("ordinance_code"),
        row.get("form_code"),
        row.get("doc_type_code"),
    )
    if not text:
        return "edinet_unknown_document_proxy"
    if any(term in text for term in SECURITY_TERMS):
        return "edinet_security_issuance_proxy"
    if any(term in text for term in FINANCING_TERMS):
        return "edinet_financing_or_dilution_proxy"
    return "edinet_material_filing_activity_proxy"


def _doc_category_flags(category: str) -> tuple[bool, bool, bool, bool]:
    is_security = category == "edinet_security_issuance_proxy"
    is_financing = category in {"edinet_security_issuance_proxy", "edinet_financing_or_dilution_proxy"}
    is_material = category == "edinet_material_filing_activity_proxy"
    is_unknown = category == "edinet_unknown_document_proxy"
    return is_financing, is_security, is_material, is_unknown


def _build_source_profile(
    conn: duckdb.DuckDBPyConnection,
    docs: pd.DataFrame,
    candidate_symbols: set[str],
    candidate_symbol_map: dict[str, str],
) -> dict[str, Any]:
    raw_docs = docs.copy()
    raw_sec_non_empty = int(raw_docs["sec_code"].notna().sum())
    raw_sec_distinct = int(raw_docs["mapped_sec_code_direct"].replace("", pd.NA).dropna().nunique())
    blank_sec_count = int((raw_docs["sec_code"].isna()).sum())
    docs = docs.copy()
    docs["mapped_sec_code"] = docs["mapped_sec_code_direct"]
    blank_mask = docs["mapped_sec_code"].eq("") | docs["mapped_sec_code"].isna()
    if blank_mask.any():
        docs.loc[blank_mask, "mapped_sec_code"] = docs.loc[blank_mask, "edinet_code"].map(
            lambda value: _normalize_sec_code(next((k for k, v in candidate_symbol_map.items() if v == str(value).strip()), ""))
        )
    docs["mapped_sec_code"] = docs["mapped_sec_code"].map(_normalize_sec_code)
    docs = docs[docs["mapped_sec_code"].ne("")].copy()
    docs["in_candidate_universe"] = docs["mapped_sec_code"].isin(candidate_symbols)
    docs["document_category"] = docs.apply(_classify_document, axis=1)
    docs["is_financing"], docs["is_security"], docs["is_material"], docs["is_unknown"] = zip(
        *docs["document_category"].map(_doc_category_flags)
    )

    candidate_docs = docs[docs["in_candidate_universe"]].copy()
    overlap_rows = int(len(candidate_docs))
    overlap_symbols = int(candidate_docs["mapped_sec_code"].nunique())
    overlap_symbol_rate = overlap_symbols / len(candidate_symbols) if candidate_symbols else None
    overlap_row_rate = overlap_rows / len(docs) if len(docs) else None
    overlap_date_min = str(candidate_docs["submit_dt"].min()) if candidate_docs["submit_dt"].notna().any() else None
    overlap_date_max = str(candidate_docs["submit_dt"].max()) if candidate_docs["submit_dt"].notna().any() else None
    research_start = pd.Timestamp("2024-03-05")
    research_end = pd.Timestamp("2026-01-19")
    overlap_within_research_window = int(
        (
            candidate_docs["submit_dt"].notna()
            & (candidate_docs["submit_dt"] >= research_start)
            & (candidate_docs["submit_dt"] <= research_end)
        ).sum()
    )
    mapped_symbol_count = int(sum(1 for symbol in candidate_symbols if symbol in candidate_symbol_map))

    columns = _table_columns(conn, "edinetdb_official_documents")
    available_fields = [item["column_name"] for item in columns]
    payload_keys_sample = []
    sample_payload = raw_docs["payload_json"].dropna().iloc[0] if raw_docs["payload_json"].notna().any() else None
    if sample_payload:
        try:
            parsed = json.loads(sample_payload)
            if isinstance(parsed, dict):
                payload_keys_sample = list(parsed.keys())[:24]
        except Exception:
            payload_keys_sample = []

    return {
        "schema_version": SOURCE_PROFILE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_table": "edinetdb_official_documents",
        "row_count": int(len(raw_docs)),
        "date_range": {
            "min": str(raw_docs["submit_dt"].min()) if raw_docs["submit_dt"].notna().any() else None,
            "max": str(raw_docs["submit_dt"].max()) if raw_docs["submit_dt"].notna().any() else None,
        },
        "sec_code_coverage": {
            "non_empty_row_count": raw_sec_non_empty,
            "distinct_sec_code_count": raw_sec_distinct,
            "blank_sec_code_count": blank_sec_count,
        },
        "candidate_overlap": {
            "candidate_symbol_count": int(len(candidate_symbols)),
            "candidate_symbol_map_count": mapped_symbol_count,
            "candidate_symbol_map_rate": (mapped_symbol_count / len(candidate_symbols)) if candidate_symbols else None,
            "historical_filing_overlap_row_count": overlap_rows,
            "historical_filing_overlap_symbol_count": overlap_symbols,
            "historical_filing_overlap_symbol_rate": overlap_symbol_rate,
            "historical_filing_overlap_row_rate": overlap_row_rate,
            "historical_filing_overlap_date_range": {
                "min": overlap_date_min,
                "max": overlap_date_max,
            },
            "historical_filing_overlap_within_research_window_row_count": overlap_within_research_window,
            "note": "historical_filing_overlap_row_count counts filings mapped to candidate symbols; distinct symbol count is reported separately",
        },
        "available_fields": available_fields,
        "available_document_type_fields": ["form_code", "doc_type_code", "legal_status", "doc_description", "payload_json"],
        "available_title_form_ordinance_fields": {
            "title_like_fields": ["doc_description", "payload_doc_description", "filer_name"],
            "form_fields": ["form_code", "doc_type_code", "payload_json.docTypeCode"],
            "ordinance_fields": ["payload_json.ordinanceCode", "current_report_reason"],
        },
        "available_submit_datetime_fields": ["submit_datetime", "payload_json.submitDateTime", "fetched_at"],
        "duplicate_handling": {
            "primary_key": "doc_id",
            "dedupe_rule": "keep the latest fetched_at row for the same doc_id if an upstream merge ever introduces duplicates",
            "observed_duplicate_doc_id_count": int(docs.duplicated("doc_id").sum()),
        },
        "missing_sec_code_count": blank_sec_count,
        "symbol_mapping_rule": "normalize sec_code to a 4-digit security code; if sec_code is blank, use edinet_code -> edinetdb_company_map.sec_code when available",
        "pit_safe_timestamp_field": "submit_datetime",
        "payload_keys_sample": payload_keys_sample,
        "document_category_counts": {str(k): int(v) for k, v in docs["document_category"].value_counts(dropna=False).items()},
        "candidate_overlap_doc_ids": candidate_docs["doc_id"].head(30).tolist(),
        "candidate_overlap_symbols_sample": sorted(candidate_docs["mapped_sec_code"].dropna().unique().tolist())[:40],
        "notes": [
            "EDINET filings are joined on submission date only; no future filing is eligible for an earlier anchor_date.",
            "The source is research-grade historical and PIT-safe because submit_datetime is the as-of timestamp.",
            "The current research window ends before the candidate-overlap EDINET docs begin, so usable overlap inside the window may still be zero.",
        ],
    }


def _build_taxonomy_contract() -> dict[str, Any]:
    return {
        "schema_version": TAXONOMY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "taxonomy": {
            "edinet_financing_or_dilution_proxy": {
                "meaning": "EDINET filing that explicitly suggests capital raise, dilution risk, or issuance-related financing activity",
                "matching_terms": list(FINANCING_TERMS),
                "examples": ["第三者割当", "新株予約権", "新株予約権付社債", "社債", "転換社債", "公募", "売出"],
                "exclude": ["ex-dividend", "shareholder-benefit rights", "exact earnings announcement"],
            },
            "edinet_security_issuance_proxy": {
                "meaning": "security registration / amendment / issuance notice or equivalent filing",
                "matching_terms": list(SECURITY_TERMS),
                "examples": ["有価証券届出書", "訂正有価証券届出書", "発行登録書", "発行登録追補書類", "訂正発行登録書"],
                "exclude": ["ex-dividend", "shareholder-benefit rights", "exact earnings announcement"],
            },
            "edinet_material_filing_activity_proxy": {
                "meaning": "any EDINET filing within the lookback window that is not classified as financing or security issuance",
                "matching_terms": ["fallback category for any filing with a parsed description"],
                "examples": ["臨時報告書", "大量保有報告書", "変更報告書", "確認書", "自己株券買付状況報告書"],
                "exclude": ["ex-dividend", "shareholder-benefit rights", "exact earnings announcement"],
            },
            "edinet_unknown_document_proxy": {
                "meaning": "document exists but the conservative classifier could not safely place it into a proxy bucket",
                "matching_terms": ["missing description or parse failure"],
                "examples": ["blank doc_description", "unparsed payload"],
                "exclude": ["ex-dividend", "shareholder-benefit rights", "exact earnings announcement"],
            },
            "edinet_no_event_proxy": {
                "meaning": "no eligible EDINET filing exists in the lookback window",
                "matching_terms": ["no filings within 20 calendar days"],
                "examples": ["no prior filing in lookback"],
                "exclude": [],
            },
        },
        "explicit_non_classifications": [
            "ex_dividend",
            "shareholder_benefit_rights",
            "exact_earnings_announcement",
        ],
        "classification_priority": [
            "edinet_financing_or_dilution_proxy",
            "edinet_security_issuance_proxy",
            "edinet_material_filing_activity_proxy",
            "edinet_unknown_document_proxy",
            "edinet_no_event_proxy",
        ],
        "pit_rule": "only EDINET filings with submit_datetime date in JST on or before the anchor_date are eligible",
        "notes": [
            "This taxonomy is intentionally conservative and does not infer dividend or earnings timing from EDINET without an explicit field.",
            "The security issuance category is intentionally narrower than the broader material filing activity category.",
        ],
    }


def _build_formula_contract() -> dict[str, Any]:
    features = {
        "edinet_recent_filing_flag": {
            "formula": "true if at least one mapped EDINET filing exists for the symbol in the trailing 20 calendar days including anchor_date",
            "window_days": 20,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date <= anchor_date",
            "status_rule": "available when a symbol map exists; missing_no_symbol_map otherwise",
        },
        "edinet_recent_filing_count_20d": {
            "formula": "count of eligible EDINET filings in the trailing 20 calendar days including anchor_date",
            "window_days": 20,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date in [anchor_date - 20d, anchor_date]",
            "status_rule": "available when a symbol map exists; missing_no_symbol_map otherwise",
        },
        "edinet_recent_filing_count_5d": {
            "formula": "count of eligible EDINET filings in the trailing 5 calendar days including anchor_date",
            "window_days": 5,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date in [anchor_date - 5d, anchor_date]",
            "status_rule": "available when a symbol map exists; missing_no_symbol_map otherwise",
        },
        "edinet_financing_or_dilution_proxy_flag": {
            "formula": "true if any eligible filing in the 20-day window is classified as financing_or_dilution or security_issuance",
            "window_days": 20,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date <= anchor_date",
            "status_rule": "available when a symbol map exists; missing_no_symbol_map otherwise",
        },
        "edinet_security_issuance_proxy_flag": {
            "formula": "true if any eligible filing in the 20-day window is classified as security_issuance",
            "window_days": 20,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date <= anchor_date",
            "status_rule": "available when a symbol map exists; missing_no_symbol_map otherwise",
        },
        "edinet_material_filing_activity_proxy_flag": {
            "formula": "true if any eligible filing exists in the 20-day window and at least one is classified as material_activity or unknown",
            "window_days": 20,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date <= anchor_date",
            "status_rule": "available when a symbol map exists; missing_no_symbol_map otherwise",
        },
        "edinet_event_noise_bucket": {
            "formula": "priority bucket over the 20-day window using financing_or_dilution > security_issuance > material_activity > recent_unknown > no_recent_event > missing_symbol_map",
            "window_days": 20,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date <= anchor_date",
            "status_rule": "available for normal rows; unclassified_document when recent filings exist but only unknown documents are present; missing_no_symbol_map when no mapping exists",
        },
        "days_since_last_edinet_filing": {
            "formula": "anchor_date minus the date of the most recent eligible EDINET filing on or before anchor_date",
            "window_days": None,
            "eligible_docs": "docs with mapped_sec_code == symbol and submit_date <= anchor_date",
            "status_rule": "available when a prior filing exists; missing_no_prior_filing when the symbol is mapped but has no eligible prior filing; missing_no_symbol_map when no mapping exists",
        },
    }
    return {
        "schema_version": FORMULA_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "join_contract": {
            "symbol_join_key": "normalized 4-digit sec_code",
            "anchor_date_rule": "anchor_date is treated as a JST calendar date; submit_datetime is compared on its calendar date, not against a future snapshot folder",
            "eligibility_rule": "only filings with submit_date <= anchor_date are eligible",
            "window_rules": {
                "20d": "anchor_date - 20 calendar days through anchor_date inclusive",
                "5d": "anchor_date - 5 calendar days through anchor_date inclusive",
            },
            "dedupe_rule": "doc_id is the primary key; if duplicates ever appear, the latest fetched_at row is retained upstream",
        },
        "features": features,
        "missing_reason_values": [
            "available",
            "missing_no_symbol_map",
            "missing_no_prior_filing",
            "missing_source_unavailable",
            "unclassified_document",
        ],
        "no_lookahead_proof": [
            "no filing with submit_date after anchor_date is eligible",
            "no future-dated EDINET snapshot is consulted because the DB snapshot itself is treated as the immutable source archive",
            "feature values depend only on historical filing dates and classifications",
        ],
    }


def _build_symbol_index(docs: pd.DataFrame) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for symbol, group in docs.groupby("mapped_sec_code", sort=False):
        ordered = group.sort_values(["submit_date", "submit_dt", "doc_id"], kind="mergesort").copy()
        index[str(symbol)] = {
            "dates": ordered["submit_date"].tolist(),
            "categories": ordered["document_category"].tolist(),
            "doc_ids": ordered["doc_id"].tolist(),
        }
    return index


def _select_doc_symbol_map(docs: pd.DataFrame, candidate_symbol_map: dict[str, str]) -> pd.DataFrame:
    frame = docs.copy()
    frame["mapped_sec_code"] = frame["mapped_sec_code_direct"]
    blank_mask = frame["mapped_sec_code"].eq("") | frame["mapped_sec_code"].isna()
    if blank_mask.any():
        inverted = {str(v).strip(): str(k).strip() for k, v in candidate_symbol_map.items() if str(v).strip()}
        frame.loc[blank_mask, "mapped_sec_code"] = frame.loc[blank_mask, "edinet_code"].map(lambda value: inverted.get(str(value).strip(), ""))
    frame["mapped_sec_code"] = frame["mapped_sec_code"].map(_normalize_sec_code)
    frame = frame[frame["mapped_sec_code"].ne("")].copy()
    return frame


def _row_features(
    symbol: str,
    anchor_date: date | None,
    *,
    symbol_map: set[str],
    symbol_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    feature_prefix = "edinet"
    status_values = {feature: ("available", "") for feature in EDINET_FEATURES}
    if _is_missing(symbol) or not anchor_date:
        out = {feature: None for feature in EDINET_FEATURES}
        for feature in EDINET_FEATURES:
            status_col, reason_col = FEATURE_STATUS_COLUMNS[feature]
            out[status_col] = "missing_source_unavailable"
            out[reason_col] = "missing_source_unavailable"
        return out

    symbol_norm = _normalize_sec_code(symbol)
    if not symbol_norm or symbol_norm not in symbol_map:
        out = {
            "edinet_recent_filing_flag": None,
            "edinet_recent_filing_count_20d": None,
            "edinet_recent_filing_count_5d": None,
            "edinet_financing_or_dilution_proxy_flag": None,
            "edinet_security_issuance_proxy_flag": None,
            "edinet_material_filing_activity_proxy_flag": None,
            "edinet_event_noise_bucket": "edinet_missing_symbol_map",
            "days_since_last_edinet_filing": None,
        }
        for feature in EDINET_FEATURES:
            status_col, reason_col = FEATURE_STATUS_COLUMNS[feature]
            if feature == "edinet_event_noise_bucket":
                out[status_col] = "missing_no_symbol_map"
                out[reason_col] = "missing_no_symbol_map"
            else:
                out[status_col] = "missing_no_symbol_map"
                out[reason_col] = "missing_no_symbol_map"
        return out

    docs = symbol_index.get(symbol_norm)
    if not docs:
        out = {
            "edinet_recent_filing_flag": False,
            "edinet_recent_filing_count_20d": 0,
            "edinet_recent_filing_count_5d": 0,
            "edinet_financing_or_dilution_proxy_flag": False,
            "edinet_security_issuance_proxy_flag": False,
            "edinet_material_filing_activity_proxy_flag": False,
            "edinet_event_noise_bucket": "edinet_no_recent_event",
            "days_since_last_edinet_filing": None,
        }
        for feature in EDINET_FEATURES:
            status_col, reason_col = FEATURE_STATUS_COLUMNS[feature]
            if feature == "days_since_last_edinet_filing":
                out[status_col] = "missing_no_prior_filing"
                out[reason_col] = "missing_no_prior_filing"
            elif feature == "edinet_event_noise_bucket":
                out[status_col] = "available"
                out[reason_col] = ""
            else:
                out[status_col] = "available"
                out[reason_col] = ""
        return out

    dates: list[date] = docs["dates"]
    categories: list[str] = docs["categories"]
    right = bisect.bisect_right(dates, anchor_date)
    left20 = bisect.bisect_left(dates, anchor_date - timedelta(days=20))
    left5 = bisect.bisect_left(dates, anchor_date - timedelta(days=5))

    recent_categories = categories[left20:right]
    recent_5_categories = categories[left5:right]
    recent_count_20d = int(max(0, right - left20))
    recent_count_5d = int(max(0, right - left5))

    recent_financing = any(cat in {"edinet_financing_or_dilution_proxy", "edinet_security_issuance_proxy"} for cat in recent_categories)
    recent_security = any(cat == "edinet_security_issuance_proxy" for cat in recent_categories)
    recent_material = any(cat == "edinet_material_filing_activity_proxy" for cat in recent_categories)
    recent_unknown = any(cat == "edinet_unknown_document_proxy" for cat in recent_categories)
    recent_any = recent_count_20d > 0

    if recent_financing:
        bucket = "edinet_financing_or_dilution"
        bucket_status = "available"
        bucket_reason = ""
    elif recent_security:
        bucket = "edinet_security_issuance"
        bucket_status = "available"
        bucket_reason = ""
    elif recent_material:
        bucket = "edinet_material_activity"
        bucket_status = "available"
        bucket_reason = ""
    elif recent_unknown and recent_any:
        bucket = "edinet_recent_unknown"
        bucket_status = "unclassified_document"
        bucket_reason = "unclassified_document"
    else:
        bucket = "edinet_no_recent_event"
        bucket_status = "available"
        bucket_reason = ""

    days_since_last = None
    days_status = "missing_no_prior_filing"
    days_reason = "missing_no_prior_filing"
    if right > 0:
        days_since_last = int((anchor_date - dates[right - 1]).days)
        days_status = "available"
        days_reason = ""

    out = {
        "edinet_recent_filing_flag": bool(recent_any),
        "edinet_recent_filing_count_20d": recent_count_20d,
        "edinet_recent_filing_count_5d": recent_count_5d,
        "edinet_financing_or_dilution_proxy_flag": bool(recent_financing),
        "edinet_security_issuance_proxy_flag": bool(recent_security),
        "edinet_material_filing_activity_proxy_flag": bool(recent_material or recent_unknown),
        "edinet_event_noise_bucket": bucket,
        "days_since_last_edinet_filing": days_since_last,
    }
    for feature in EDINET_FEATURES:
        status_col, reason_col = FEATURE_STATUS_COLUMNS[feature]
        if feature == "edinet_event_noise_bucket":
            out[status_col] = bucket_status
            out[reason_col] = bucket_reason
        elif feature == "days_since_last_edinet_filing":
            out[status_col] = days_status
            out[reason_col] = days_reason
        else:
            out[status_col] = "available"
            out[reason_col] = ""
    return out


def _apply_edinet_features(frame: pd.DataFrame, *, symbol_map: set[str], symbol_index: dict[str, dict[str, Any]]) -> pd.DataFrame:
    out = frame.copy()
    anchor_dates = [_parse_date(value) for value in out["anchor_date"].tolist()]
    feature_rows = []
    for symbol, anchor_date in zip(out["symbol"].tolist(), anchor_dates, strict=False):
        feature_rows.append(_row_features(str(symbol), anchor_date, symbol_map=symbol_map, symbol_index=symbol_index))
    feature_frame = pd.DataFrame(feature_rows, index=out.index)
    for column in feature_frame.columns:
        out[column] = feature_frame[column]
    return out


def _feature_non_null(frame: pd.DataFrame, feature: str) -> int:
    return int(frame[feature].notna().sum()) if feature in frame.columns else 0


def _feature_positive_count(frame: pd.DataFrame, feature: str) -> int:
    if feature not in frame.columns:
        return 0
    if feature.endswith("_flag"):
        return int(frame[feature].fillna(False).astype(bool).sum())
    if feature.endswith("_count_20d") or feature.endswith("_count_5d"):
        return int((pd.to_numeric(frame[feature], errors="coerce").fillna(0) > 0).sum())
    if feature == "days_since_last_edinet_filing":
        return int(frame[feature].notna().sum())
    if feature == "edinet_event_noise_bucket":
        return int(frame[feature].isin(["edinet_financing_or_dilution", "edinet_security_issuance", "edinet_material_activity", "edinet_recent_unknown"]).sum())
    return int(frame[feature].notna().sum())


def _status_counts(frame: pd.DataFrame, feature: str) -> dict[str, int]:
    status_col, _ = FEATURE_STATUS_COLUMNS[feature]
    if status_col not in frame.columns:
        return {}
    return {str(k): int(v) for k, v in frame[status_col].fillna("").value_counts(dropna=False).items()}


def _missing_reason_counts(frame: pd.DataFrame, feature: str) -> dict[str, int]:
    _, reason_col = FEATURE_STATUS_COLUMNS[feature]
    if reason_col not in frame.columns:
        return {}
    return {str(k): int(v) for k, v in frame[reason_col].fillna("").value_counts(dropna=False).items()}


def _group_coverage(frame: pd.DataFrame, feature: str, group_column: str) -> dict[str, Any]:
    if group_column not in frame.columns:
        return {}
    result: dict[str, Any] = {}
    groups = frame[group_column].fillna("missing").astype(str).value_counts(dropna=False).index.tolist()
    for group in groups:
        subset = frame[frame[group_column].fillna("missing").astype(str) == group]
        if not len(subset):
            continue
        result[group] = {
            "row_count": int(len(subset)),
            "non_null_count": _feature_non_null(subset, feature),
            "coverage_rate": (_feature_non_null(subset, feature) / len(subset)) if len(subset) else None,
            "positive_count": _feature_positive_count(subset, feature),
            "status_distribution": _status_counts(subset, feature),
            "missing_reason_distribution": _missing_reason_counts(subset, feature),
        }
    return result


def _build_coverage_summary(
    candidate: pd.DataFrame,
    orfp: pd.DataFrame,
    *,
    mapped_candidate_symbols: int,
    candidate_symbol_count: int,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "features": {},
        "candidate_rows": int(len(candidate)),
        "orfp_rows": int(len(orfp)),
        "candidate_code_mapping_coverage": {
            "candidate_symbols": int(candidate_symbol_count),
            "mapped_candidate_symbols": int(mapped_candidate_symbols),
            "mapped_symbol_rate": (mapped_candidate_symbols / candidate_symbol_count) if candidate_symbol_count else None,
        },
        "notes": [
            "Coverage is measured after explicit symbol mapping and submit_date filtering.",
            "Positive count means flag true or count > 0; the noise bucket counts non-missing event buckets.",
        ],
    }
    for feature in EDINET_FEATURES:
        summary["features"][feature] = {
            "candidate": {
                "non_null_count": _feature_non_null(candidate, feature),
                "coverage_rate": (_feature_non_null(candidate, feature) / len(candidate)) if len(candidate) else None,
                "positive_count": _feature_positive_count(candidate, feature),
                "status_distribution": _status_counts(candidate, feature),
                "missing_reason_distribution": _missing_reason_counts(candidate, feature),
            },
            "orfp": {
                "non_null_count": _feature_non_null(orfp, feature),
                "coverage_rate": (_feature_non_null(orfp, feature) / len(orfp)) if len(orfp) else None,
                "positive_count": _feature_positive_count(orfp, feature),
                "status_distribution": _status_counts(orfp, feature),
                "missing_reason_distribution": _missing_reason_counts(orfp, feature),
            },
            "candidate_topk_coverage": {
                "top5": _group_coverage(candidate, feature, "champion_selected_top5"),
                "top10": _group_coverage(candidate, feature, "champion_selected_top10"),
                "top20": _group_coverage(candidate, feature, "champion_selected_top20"),
            },
            "orfp_topk_coverage": {
                "top5": _group_coverage(orfp, feature, "variant_selected_top5") if "variant_selected_top5" in orfp.columns else {},
                "top10": _group_coverage(orfp, feature, "variant_selected_top10") if "variant_selected_top10" in orfp.columns else {},
                "top20": _group_coverage(orfp, feature, "variant_selected_top20") if "variant_selected_top20" in orfp.columns else {},
            },
            "side_coverage": {
                "candidate": _group_coverage(candidate, feature, "side"),
                "orfp": _group_coverage(orfp, feature, "side"),
            },
            "family_coverage": {
                "candidate": _group_coverage(candidate, feature, "family_classification"),
                "orfp": _group_coverage(orfp, feature, "family_classification"),
            },
        }
    return summary


def _build_missingness_summary(candidate: pd.DataFrame, orfp: pd.DataFrame) -> dict[str, Any]:
    def _summary(frame: pd.DataFrame) -> dict[str, Any]:
        features: dict[str, Any] = {}
        for feature in EDINET_FEATURES:
            _, reason_col = FEATURE_STATUS_COLUMNS[feature]
            status_col, _ = FEATURE_STATUS_COLUMNS[feature]
            features[feature] = {
                "non_null_count": _feature_non_null(frame, feature),
                "missing_count": int(len(frame) - _feature_non_null(frame, feature)),
                "missing_rate": ((len(frame) - _feature_non_null(frame, feature)) / len(frame)) if len(frame) else None,
                "status_distribution": _status_counts(frame, feature),
                "missing_reason_distribution": _missing_reason_counts(frame, feature),
                "status_column": status_col,
                "missing_reason_column": reason_col,
            }
        return features

    return {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate": _summary(candidate),
        "orfp": _summary(orfp),
    }


def _build_no_lookahead_audit(candidate: pd.DataFrame, orfp: pd.DataFrame) -> dict[str, Any]:
    def _audit(frame: pd.DataFrame, label: str) -> dict[str, Any]:
        status = "pass"
        future_violations = 0
        missing_map = 0
        missing_prior = 0
        symbol_map = set(frame["symbol"].astype(str).map(_normalize_sec_code).replace("", pd.NA).dropna().unique().tolist())
        submit_dates = pd.to_datetime(frame["anchor_date"], errors="coerce").dt.date
        for idx, row in frame.iterrows():
            if pd.isna(submit_dates.loc[idx]):
                continue
            if row.get("edinet_recent_filing_flag_feature_status") == "missing_no_symbol_map":
                missing_map += 1
            if row.get("days_since_last_edinet_filing_feature_status") == "missing_no_prior_filing":
                missing_prior += 1
            # The feature builder only considers filings with submit_date <= anchor_date.
            # This audit checks for explicit violations by inspecting the used feature values.
            if row.get("days_since_last_edinet_filing") is not None:
                if int(row.get("days_since_last_edinet_filing")) < 0:
                    future_violations += 1
        if future_violations:
            status = "fail"
        return {
            "status": status,
            "row_count": int(len(frame)),
            "mapped_symbol_count": int(len(symbol_map)),
            "rows_missing_symbol_map": missing_map,
            "rows_missing_prior_filing": missing_prior,
            "future_filing_violations": future_violations,
            "future_outcome_fields_used": False,
            "explicit_missing_rows": int(frame.filter(like="_missing_reason").ne("").any(axis=1).sum()),
            "submit_datetime_rule": "eligible documents were required to satisfy submit_date <= anchor_date",
            "anchor_date_rule": "anchor_date is interpreted as a JST calendar date",
            "violations_sample": [],
        }

    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": _audit(candidate, "candidate_surface"),
        "orfp_surface": _audit(orfp, "orfp_surface"),
    }


def _subset_stats(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
    if feature not in frame.columns:
        return {"non_null_count": 0, "coverage_rate": None, "positive_count": 0, "value_counts_top5": {}}
    series = frame[feature]
    non_null = int(series.notna().sum())
    if feature.endswith("_flag") or feature == "edinet_event_noise_bucket":
        counts = {str(k): int(v) for k, v in series.fillna("").value_counts(dropna=False).head(5).items()}
        return {
            "non_null_count": non_null,
            "coverage_rate": (non_null / len(frame)) if len(frame) else None,
            "positive_count": _feature_positive_count(frame, feature),
            "value_counts_top5": counts,
        }
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        return {
            "non_null_count": non_null,
            "coverage_rate": (non_null / len(frame)) if len(frame) else None,
            "positive_count": _feature_positive_count(frame, feature),
            "mean": float(numeric.mean()) if math.isfinite(float(numeric.mean())) else None,
            "median": float(numeric.median()) if math.isfinite(float(numeric.median())) else None,
            "value_counts_top5": {},
        }
    return {
        "non_null_count": non_null,
        "coverage_rate": (non_null / len(frame)) if len(frame) else None,
        "positive_count": _feature_positive_count(frame, feature),
        "value_counts_top5": {str(k): int(v) for k, v in series.fillna("").value_counts(dropna=False).head(5).items()},
    }


def _build_contrast(diff: pd.DataFrame, feature_frame: pd.DataFrame) -> dict[str, Any]:
    joined = diff.merge(feature_frame, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_edinet"))
    result: dict[str, Any] = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_orfp_session": str(ORFP_TOPK_DIFF_SESSION),
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
        subset_stats = {
            subset_name: {feature: _subset_stats(subset_frame, feature) for feature in EDINET_FEATURES}
            for subset_name, subset_frame in subsets.items()
        }
        result["topk"][str(topk)] = {
            "added_top15_count": int(added_top15.sum()),
            "added_bottom15_count": int(added_bottom15.sum()),
            "added_neutral_count": int(added_neutral.sum()),
            "unchanged_top15_count": int(unchanged_top15.sum()),
            "unchanged_bottom15_count": int(unchanged_bottom15.sum()),
            "unchanged_neutral_count": int(unchanged_neutral.sum()),
            "subset_stats": subset_stats,
        }
    return result


def _build_orfp_summary(orfp: pd.DataFrame, contrast: dict[str, Any]) -> dict[str, Any]:
    top15 = orfp["top15_label"].fillna(False).astype(bool) if "top15_label" in orfp.columns else pd.Series(False, index=orfp.index)
    bottom15 = orfp["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in orfp.columns else pd.Series(False, index=orfp.index)
    family_counts = orfp["family_classification"].fillna("missing").astype(str).value_counts(dropna=False).head(12).to_dict() if "family_classification" in orfp.columns else {}
    summary = {
        "schema_version": ORFP_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(orfp)),
        "top15_count": int(top15.sum()),
        "bottom15_count": int(bottom15.sum()),
        "feature_coverage": {
            feature: {
                "non_null_count": _feature_non_null(orfp, feature),
                "coverage_rate": (_feature_non_null(orfp, feature) / len(orfp)) if len(orfp) else None,
                "positive_count": _feature_positive_count(orfp, feature),
                "status_distribution": _status_counts(orfp, feature),
                "missing_reason_distribution": _missing_reason_counts(orfp, feature),
            }
            for feature in EDINET_FEATURES
        },
        "family_classification_counts_top12": {str(k): int(v) for k, v in family_counts.items()},
        "top15_bottom15_contrast_reference": {
            "top5_added_top15_count": contrast["topk"]["5"]["added_top15_count"],
            "top5_added_bottom15_count": contrast["topk"]["5"]["added_bottom15_count"],
            "top10_added_top15_count": contrast["topk"]["10"]["added_top15_count"],
            "top10_added_bottom15_count": contrast["topk"]["10"]["added_bottom15_count"],
        },
        "usefulness_note": "If financing_or_dilution or security_issuance concentrates in bottom15 rows, this proxy line is plausible for reclassification support.",
    }
    return summary


def _decide(coverage: dict[str, Any], source_profile: dict[str, Any], orfp: pd.DataFrame, candidate: pd.DataFrame) -> dict[str, Any]:
    candidate_symbol_count = int(candidate["symbol"].astype(str).map(_normalize_sec_code).replace("", pd.NA).dropna().nunique())
    mapped_candidate_rows = int(candidate["edinet_recent_filing_flag"].notna().sum())
    candidate_positive = int(candidate["edinet_recent_filing_flag"].fillna(False).astype(bool).sum())
    financing_positive = int(candidate["edinet_financing_or_dilution_proxy_flag"].fillna(False).astype(bool).sum())
    security_positive = int(candidate["edinet_security_issuance_proxy_flag"].fillna(False).astype(bool).sum())
    material_positive = int(candidate["edinet_material_filing_activity_proxy_flag"].fillna(False).astype(bool).sum())
    unknown_buckets = int(candidate["edinet_event_noise_bucket"].eq("edinet_recent_unknown").sum())
    bottom15_rows = int(candidate["bottom15_label"].fillna(False).astype(bool).sum()) if "bottom15_label" in candidate.columns else 0
    top15_rows = int(candidate["top15_label"].fillna(False).astype(bool).sum()) if "top15_label" in candidate.columns else 0
    overlap_rows = int(source_profile["candidate_overlap"]["historical_filing_overlap_row_count"])
    usable_overlap_rows = int(source_profile["candidate_overlap"]["historical_filing_overlap_within_research_window_row_count"])

    decision = "ready_to_rerun_reclassification_with_edinet_features"
    reason = "candidate symbol map is material, at least one proxy feature is active, and EDINET filings are PIT-safe by submit_date"

    if candidate_symbol_count < 100 or usable_overlap_rows == 0 or candidate_positive == 0:
        decision = "insufficient_edinet_coverage"
        reason = "EDINET filings exist, but no usable filings fall inside the current research window or no positive proxy rows were generated"
    elif financing_positive == 0 and security_positive == 0 and material_positive == 0:
        decision = "stop_edinet_event_proxy_line"
        reason = "EDINET filing activity exists but the conservative taxonomy did not produce a usable proxy signal"
    elif unknown_buckets > max(10, int(len(candidate) * 0.10)):
        decision = "needs_edinet_taxonomy_revision"
        reason = "too many recent filings remain unclassified or ambiguous under the conservative taxonomy"
    elif financing_positive + security_positive < max(5, int(len(candidate) * 0.02)):
        decision = "needs_edinet_taxonomy_revision"
        reason = "proxy positives exist but financing/security signals are too sparse for a reclassification rerun"
    else:
        # If the proxy line is present but does not appear to address the bottom15 bottleneck at all, stop it.
        bottom15_mask = candidate["bottom15_label"].fillna(False).astype(bool) if "bottom15_label" in candidate.columns else pd.Series(False, index=candidate.index)
        top15_mask = candidate["top15_label"].fillna(False).astype(bool) if "top15_label" in candidate.columns else pd.Series(False, index=candidate.index)
        bottom15_financing_rate = float(candidate.loc[bottom15_mask, "edinet_financing_or_dilution_proxy_flag"].fillna(False).astype(bool).mean()) if bottom15_mask.any() else 0.0
        top15_financing_rate = float(candidate.loc[top15_mask, "edinet_financing_or_dilution_proxy_flag"].fillna(False).astype(bool).mean()) if top15_mask.any() else 0.0
        if bottom15_financing_rate <= top15_financing_rate and financing_positive < 20:
            decision = "stop_edinet_event_proxy_line"
            reason = "EDINET proxy signal is too broad or too weak to address the observed false-positive bottleneck"
        else:
            decision = "ready_to_rerun_reclassification_with_edinet_features"
            reason = "EDINET proxy signal is material enough to justify a reclassification rerun"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "reason": reason,
        "authoritative_inputs": {
            "source_selection_decision": str(SOURCE_SELECTION_DECISION),
            "source_selection_coverage": str(SOURCE_SELECTION_COVERAGE),
            "source_selection_candidates": str(SOURCE_SELECTION_CANDIDATES),
            "source_selection_pit": str(SOURCE_SELECTION_PIT),
            "source_selection_recommendation": str(SOURCE_SELECTION_RECOMMENDATION),
            "batch2_candidate_surface": str(BATCH2_CANDIDATE),
            "batch2_orfp_surface": str(BATCH2_ORFP),
            "batch2_no_lookahead": str(BATCH2_NO_LOOKAHEAD),
            "orfp_topk_diff": str(ORFP_TOPK_DIFF),
        },
        "observed_metrics": {
            "candidate_symbol_count": candidate_symbol_count,
            "candidate_rows": int(len(candidate)),
            "candidate_positive_rows": candidate_positive,
            "financing_positive_rows": financing_positive,
            "security_positive_rows": security_positive,
            "material_positive_rows": material_positive,
            "unknown_bucket_rows": unknown_buckets,
            "candidate_overlap_rows": overlap_rows,
            "candidate_usable_overlap_rows": usable_overlap_rows,
            "top15_rows": top15_rows,
            "bottom15_rows": bottom15_rows,
        },
    }


def _run_validation_checks(output_dir: Path, expected_files: list[str]) -> dict[str, Any]:
    missing = [name for name in expected_files if not (output_dir / name).exists()]
    json_ok = []
    for name in expected_files:
        if not name.endswith(".json"):
            continue
        try:
            _load_json(output_dir / name)
            json_ok.append(name)
        except Exception:
            pass
    parquet_ok = []
    for name in expected_files:
        if not name.endswith(".parquet"):
            continue
        try:
            pd.read_parquet(output_dir / name)
            parquet_ok.append(name)
        except Exception:
            pass
    return {
        "missing_files": missing,
        "json_parse_ok": json_ok,
        "parquet_read_ok": parquet_ok,
        "all_required_files_present": not missing,
    }


def run_edinet_event_proxy(
    *,
    output_root: Path,
    limit_anchor_dates: int | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    output_root = output_root.resolve()
    candidate, orfp, diff = _load_candidate_inputs()
    if limit_anchor_dates is not None:
        anchor_dates = sorted(candidate["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate = candidate[candidate["anchor_date"].isin(anchor_dates)].copy()
        orfp = orfp[orfp["anchor_date"].isin(anchor_dates)].copy()

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        raw_docs = _load_edinet_documents(conn)
        candidate_symbol_map = _build_candidate_symbol_map(conn)
        candidate_symbols = {str(symbol) for symbol in candidate["symbol"].astype(str).map(_normalize_sec_code).tolist() if _normalize_sec_code(symbol)}
        source_profile = _build_source_profile(conn, raw_docs, candidate_symbols, candidate_symbol_map)
        docs = _select_doc_symbol_map(raw_docs, candidate_symbol_map)
        docs = docs.drop_duplicates("doc_id", keep="last").copy()
        docs["document_category"] = docs.apply(_classify_document, axis=1)
        docs["is_financing"], docs["is_security"], docs["is_material"], docs["is_unknown"] = zip(
            *docs["document_category"].map(_doc_category_flags)
        )
        docs["mapped_sec_code"] = docs["mapped_sec_code"].map(_normalize_sec_code)
        docs = docs[docs["mapped_sec_code"].ne("")].copy()
        docs = docs[docs["mapped_sec_code"].isin(candidate_symbols) | docs["mapped_sec_code"].isin(candidate_symbol_map.keys())].copy()
        taxonomy_contract = _build_taxonomy_contract()
        formula_contract = _build_formula_contract()

        mapped_symbol_set = {symbol for symbol in candidate_symbols if symbol in candidate_symbol_map}
        symbol_index = _build_symbol_index(docs[docs["mapped_sec_code"].isin(mapped_symbol_set)].copy())

        candidate_enriched = _apply_edinet_features(candidate, symbol_map=mapped_symbol_set, symbol_index=symbol_index)
        orfp_enriched = _apply_edinet_features(orfp, symbol_map=mapped_symbol_set, symbol_index=symbol_index)

        coverage = _build_coverage_summary(
            candidate_enriched,
            orfp_enriched,
            mapped_candidate_symbols=len(mapped_symbol_set),
            candidate_symbol_count=len(candidate_symbols),
        )
        missingness = _build_missingness_summary(candidate_enriched, orfp_enriched)
        no_lookahead = _build_no_lookahead_audit(candidate_enriched, orfp_enriched)
        contrast = _build_contrast(diff, candidate_enriched)
        orfp_summary = _build_orfp_summary(orfp_enriched, contrast)
        decision = _decide(coverage, source_profile, orfp_enriched, candidate_enriched)

        session_dir = output_root / _make_session_id()
        session_dir.mkdir(parents=True, exist_ok=True)

        run_manifest = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "script_name": SCRIPT_NAME,
            "session_id": session_dir.name,
            "output_root": str(output_root),
            "session_dir": str(session_dir),
            "jobs_requested": int(jobs),
            "jobs_supported": 1,
            "source_selection_session": str(SOURCE_SELECTION_SESSION),
            "batch2_session": str(BATCH2_SESSION),
            "orfp_diff_session": str(ORFP_TOPK_DIFF_SESSION),
            "input_paths": {
                "candidate_surface": str(BATCH2_CANDIDATE),
                "orfp_surface": str(BATCH2_ORFP),
                "orfp_topk_diff": str(ORFP_TOPK_DIFF),
                "edinet_db": str(DB_PATH),
            },
            "decision": decision["decision"],
            "no_lookahead_passed": no_lookahead["candidate_surface"]["status"] == "pass" and no_lookahead["orfp_surface"]["status"] == "pass",
        }

        input_resolution = {
            "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "resolved_paths": {
                "candidate_surface": str(BATCH2_CANDIDATE),
                "orfp_surface": str(BATCH2_ORFP),
                "orfp_topk_diff": str(ORFP_TOPK_DIFF),
                "batch2_no_lookahead": str(BATCH2_NO_LOOKAHEAD),
                "source_selection_decision": str(SOURCE_SELECTION_DECISION),
                "source_selection_coverage": str(SOURCE_SELECTION_COVERAGE),
                "source_selection_candidates": str(SOURCE_SELECTION_CANDIDATES),
                "source_selection_pit": str(SOURCE_SELECTION_PIT),
                "source_selection_recommendation": str(SOURCE_SELECTION_RECOMMENDATION),
                "duckdb_path": str(DB_PATH),
                "output_root": str(output_root),
            },
            "path_checks": {
                "candidate_surface_exists": BATCH2_CANDIDATE.exists(),
                "orfp_surface_exists": BATCH2_ORFP.exists(),
                "orfp_topk_diff_exists": ORFP_TOPK_DIFF.exists(),
                "edinet_db_exists": DB_PATH.exists(),
                "source_selection_decision_exists": SOURCE_SELECTION_DECISION.exists(),
                "source_selection_coverage_exists": SOURCE_SELECTION_COVERAGE.exists(),
                "source_selection_candidates_exists": SOURCE_SELECTION_CANDIDATES.exists(),
                "source_selection_pit_exists": SOURCE_SELECTION_PIT.exists(),
                "source_selection_recommendation_exists": SOURCE_SELECTION_RECOMMENDATION.exists(),
            },
        }

        _write_json(session_dir / "run_manifest.json", run_manifest)
        _write_json(session_dir / "input_resolution.json", input_resolution)
        _write_json(session_dir / "edinet_source_profile.json", source_profile)
        _write_json(session_dir / "edinet_proxy_taxonomy_contract.json", taxonomy_contract)
        _write_json(session_dir / "edinet_event_feature_formula_contract.json", formula_contract)
        _write_json(session_dir / "edinet_event_coverage_summary.json", coverage)
        _write_json(session_dir / "edinet_event_missingness_summary.json", missingness)
        _write_json(session_dir / "no_lookahead_edinet_event_audit.json", no_lookahead)
        _write_json(session_dir / "added_top15_vs_bottom15_edinet_contrast.json", contrast)
        _write_json(session_dir / "orfp_edinet_event_summary.json", orfp_summary)
        _write_json(session_dir / "feature_surface_edinet_event_proxy_v1_decision.json", decision)
        _write_json(session_dir / "_ARTIFACT_COMPLETE.json", {
            "schema_version": SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "complete": True,
            "required_files": [
                "run_manifest.json",
                "input_resolution.json",
                "edinet_source_profile.json",
                "edinet_proxy_taxonomy_contract.json",
                "edinet_event_feature_formula_contract.json",
                "edinet_event_coverage_summary.json",
                "edinet_event_missingness_summary.json",
                "no_lookahead_edinet_event_audit.json",
                "added_top15_vs_bottom15_edinet_contrast.json",
                "orfp_edinet_event_summary.json",
                "feature_surface_edinet_event_proxy_v1_decision.json",
                "candidate_prefilter_rows_edinet_event_enriched_v1.parquet",
                "observable_regime_false_positive_edinet_event_enriched_v1.parquet",
            ],
        })

        _write_parquet(session_dir / "candidate_prefilter_rows_edinet_event_enriched_v1.parquet", candidate_enriched)
        _write_parquet(session_dir / "observable_regime_false_positive_edinet_event_enriched_v1.parquet", orfp_enriched)
        sample_rows = pd.concat(
            [
                candidate_enriched.head(10),
                candidate_enriched.tail(10),
                orfp_enriched.head(10),
            ],
            ignore_index=True,
        ).drop_duplicates(subset=["anchor_date", "symbol", "side"], keep="first")
        _write_parquet(session_dir / "sample_edinet_event_feature_rows.parquet", sample_rows)

        validation = _run_validation_checks(
            session_dir,
            [
                "run_manifest.json",
                "input_resolution.json",
                "edinet_source_profile.json",
                "edinet_proxy_taxonomy_contract.json",
                "edinet_event_feature_formula_contract.json",
                "edinet_event_coverage_summary.json",
                "edinet_event_missingness_summary.json",
                "no_lookahead_edinet_event_audit.json",
                "added_top15_vs_bottom15_edinet_contrast.json",
                "orfp_edinet_event_summary.json",
                "feature_surface_edinet_event_proxy_v1_decision.json",
                "_ARTIFACT_COMPLETE.json",
                "candidate_prefilter_rows_edinet_event_enriched_v1.parquet",
                "observable_regime_false_positive_edinet_event_enriched_v1.parquet",
            ],
        )

        result = {
            "output_dir": str(session_dir),
            "validation": validation,
            "decision": decision["decision"],
            "source_profile": source_profile,
            "coverage": coverage,
            "no_lookahead": no_lookahead,
        }
        return result
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Build EDINET event proxy feature surface for TRADEX")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()
    result = run_edinet_event_proxy(
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
