from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\event_source_contract_design_v1")
JPX_EARNINGS_ARCHIVE = REPO_ROOT / "data_store" / "raw" / "jpx_financial_announcement"
JPX_RIGHTS_ARCHIVE = REPO_ROOT / "data_store" / "raw" / "jpx_ex_rights"
CANDIDATE_SURFACE = Path(
    r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273\candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


def _table_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(_scalar(con, f"select count(*) from {table}"))


def _archive_stats(root: Path) -> dict[str, Any]:
    folders = sorted([p for p in root.iterdir() if p.is_dir()])
    return {
        "folder_count": len(folders),
        "file_count": sum(len([f for f in p.iterdir() if f.is_file()]) for p in folders),
        "snapshot_dates": [p.name for p in folders],
        "folder_stats": [
            {"snapshot_date": p.name, "file_count": len([f for f in p.iterdir() if f.is_file()])}
            for p in folders
        ],
        "min_snapshot_date": folders[0].name if folders else None,
        "max_snapshot_date": folders[-1].name if folders else None,
    }


def _candidate_codes() -> tuple[pd.DataFrame, int]:
    frame = pd.read_parquet(CANDIDATE_SURFACE)
    code_col = "symbol" if "symbol" in frame.columns else "code"
    if code_col not in frame.columns:
        raise RuntimeError("candidate surface missing symbol/code column")
    codes = frame[[code_col]].drop_duplicates().rename(columns={code_col: "cand_code"})
    return codes, int(len(codes))


def _entry(
    *,
    source_id: str,
    source_kind: str,
    path_or_table: str | None,
    row_count: int | None,
    symbol_key: str | None,
    event_date_key: str | None,
    as_of_key: str | None,
    fetched_at_key: str | None,
    historical_coverage: str,
    snapshot_mode: str,
    pit_status: str,
    joinability: str,
    limitations: str,
    candidate_code_overlap: int | None,
    candidate_code_overlap_rate: float | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "source_id": source_id,
        "source_kind": source_kind,
        "path_or_table": path_or_table,
        "row_count": row_count,
        "symbol_key": symbol_key,
        "event_date_key": event_date_key,
        "as_of_key": as_of_key,
        "fetched_at_key": fetched_at_key,
        "historical_coverage": historical_coverage,
        "snapshot_mode": snapshot_mode,
        "pit_status": pit_status,
        "joinability": joinability,
        "limitations": limitations,
        "candidate_code_overlap": candidate_code_overlap,
        "candidate_code_overlap_rate": candidate_code_overlap_rate,
    }
    if extra:
        payload.update(extra)
    return payload


def _build_all(con: duckdb.DuckDBPyConnection, cand_codes: pd.DataFrame, candidate_code_count: int):
    con.register("cand_df", cand_codes)
    archive_earnings = _archive_stats(JPX_EARNINGS_ARCHIVE)
    archive_rights = _archive_stats(JPX_RIGHTS_ARCHIVE)

    earnings_overlap = int(_scalar(con, "select count(*) from cand_df join earnings_planned e on e.code = cand_df.cand_code"))
    rights_overlap = int(_scalar(con, "select count(*) from cand_df join ex_rights r on r.code = cand_df.cand_code"))
    edinet_overlap = int(
        _scalar(
            con,
            "select count(*) from cand_df join edinetdb_official_documents o on regexp_replace(coalesce(o.sec_code,''),'[^0-9]','','g') = cand_df.cand_code",
        )
    )
    tdnet_overlap = int(_scalar(con, "select count(*) from cand_df join tdnet_disclosures d on d.sec_code = cand_df.cand_code"))
    tdnet_feat_overlap = int(
        _scalar(con, "select count(*) from cand_df join tdnet_disclosure_features f on f.sec_code = cand_df.cand_code")
    )
    company_overlap = int(_scalar(con, "select count(*) from cand_df join edinetdb_company_map m on m.sec_code = cand_df.cand_code"))

    earnings = _entry(
        source_id="jpx_earnings_planned_snapshot_archive",
        source_kind="core_event_source",
        path_or_table=f"{JPX_EARNINGS_ARCHIVE} + earnings_planned",
        row_count=_table_count(con, "earnings_planned"),
        symbol_key="code",
        event_date_key="planned_date",
        as_of_key="snapshot_folder_date + fetched_at",
        fetched_at_key="fetched_at",
        historical_coverage=f"snapshots {archive_earnings['min_snapshot_date']}..{archive_earnings['max_snapshot_date']}",
        snapshot_mode="archived daily/refresh snapshots",
        pit_status="pit_safe_with_backfill",
        joinability="joinable to anchor_date via code and as-of snapshot filter",
        limitations="planned_date is not actual announcement date; future snapshots must be excluded",
        candidate_code_overlap=earnings_overlap,
        candidate_code_overlap_rate=earnings_overlap / candidate_code_count,
    )
    rights = _entry(
        source_id="jpx_ex_rights_snapshot_archive",
        source_kind="core_event_source",
        path_or_table=f"{JPX_RIGHTS_ARCHIVE} + ex_rights",
        row_count=_table_count(con, "ex_rights"),
        symbol_key="code",
        event_date_key="ex_date / last_rights_date",
        as_of_key="snapshot_folder_date + fetched_at",
        fetched_at_key="fetched_at",
        historical_coverage=f"snapshots {archive_rights['min_snapshot_date']}..{archive_rights['max_snapshot_date']}",
        snapshot_mode="archived daily/refresh snapshots",
        pit_status="pit_safe_with_backfill",
        joinability="joinable to anchor_date via code and as-of snapshot filter",
        limitations="coverage is narrow; rights-related rows are a precise but sparse signal",
        candidate_code_overlap=rights_overlap,
        candidate_code_overlap_rate=rights_overlap / candidate_code_count,
    )
    edinet = _entry(
        source_id="edinetdb_official_documents",
        source_kind="core_event_source",
        path_or_table=f"{DB_PATH}::edinetdb_official_documents",
        row_count=_table_count(con, "edinetdb_official_documents"),
        symbol_key="sec_code",
        event_date_key="submit_datetime",
        as_of_key="fetched_at",
        fetched_at_key="fetched_at",
        historical_coverage=f"submit_datetime {_scalar(con, 'select min(submit_datetime) from edinetdb_official_documents')}..{_scalar(con, 'select max(submit_datetime) from edinetdb_official_documents')}",
        snapshot_mode="current immutable filing snapshot",
        pit_status="pit_safe_with_backfill",
        joinability="joinable via sec_code directly or via edinetdb_company_map on edinet_code",
        limitations="event taxonomy is not yet extracted into dedicated flags; useful for financing/dilution proxies",
        candidate_code_overlap=edinet_overlap,
        candidate_code_overlap_rate=edinet_overlap / candidate_code_count,
    )
    tdnet = _entry(
        source_id="tdnet_disclosures",
        source_kind="core_event_source",
        path_or_table=f"{DB_PATH}::tdnet_disclosures",
        row_count=_table_count(con, "tdnet_disclosures"),
        symbol_key="sec_code",
        event_date_key="published_at",
        as_of_key="fetched_at",
        fetched_at_key="fetched_at",
        historical_coverage="empty in current local DB snapshot",
        snapshot_mode="schema present, data absent",
        pit_status="not_usable_for_historical_research",
        joinability="would join on sec_code once populated",
        limitations="no historical disclosure archive is present locally today",
        candidate_code_overlap=tdnet_overlap,
        candidate_code_overlap_rate=tdnet_overlap / candidate_code_count,
    )
    tdnet_feat = _entry(
        source_id="tdnet_disclosure_features",
        source_kind="derived_event_source",
        path_or_table=f"{DB_PATH}::tdnet_disclosure_features",
        row_count=_table_count(con, "tdnet_disclosure_features"),
        symbol_key="sec_code",
        event_date_key="published_at",
        as_of_key="fetched_at",
        fetched_at_key="fetched_at",
        historical_coverage="empty in current local DB snapshot",
        snapshot_mode="schema present, data absent",
        pit_status="not_usable_for_historical_research",
        joinability="would join on sec_code once populated",
        limitations="classification schema exists in code, but no populated rows are available locally",
        candidate_code_overlap=tdnet_feat_overlap,
        candidate_code_overlap_rate=tdnet_feat_overlap / candidate_code_count,
    )
    edinet_analysis = _entry(
        source_id="edinetdb_analysis",
        source_kind="derived_event_source",
        path_or_table=f"{DB_PATH}::edinetdb_analysis",
        row_count=_table_count(con, "edinetdb_analysis"),
        symbol_key="edinet_code",
        event_date_key="asof_date",
        as_of_key="fetched_at",
        fetched_at_key="fetched_at",
        historical_coverage="empty in current local DB snapshot",
        snapshot_mode="schema present, data absent",
        pit_status="not_usable_for_historical_research",
        joinability="would join via edinet_code once populated",
        limitations="no populated rows in the local DB snapshot",
        candidate_code_overlap=0,
        candidate_code_overlap_rate=0.0,
    )
    company_map = _entry(
        source_id="edinetdb_company_map",
        source_kind="support_lookup",
        path_or_table=f"{DB_PATH}::edinetdb_company_map",
        row_count=_table_count(con, "edinetdb_company_map"),
        symbol_key="sec_code",
        event_date_key=None,
        as_of_key="updated_at",
        fetched_at_key="updated_at",
        historical_coverage="static mapping snapshot",
        snapshot_mode="current support lookup",
        pit_status="pit_safe_now",
        joinability="joins sec_code to edinet_code for filing-based event features",
        limitations="mapping only; not an event feed",
        candidate_code_overlap=company_overlap,
        candidate_code_overlap_rate=company_overlap / candidate_code_count,
    )
    meta = _entry(
        source_id="events_meta",
        source_kind="operational_metadata",
        path_or_table=f"{DB_PATH}::events_meta",
        row_count=_table_count(con, "events_meta"),
        symbol_key=None,
        event_date_key=None,
        as_of_key="last_attempt_at / last_success_at",
        fetched_at_key=None,
        historical_coverage="one metadata row",
        snapshot_mode="operational state only",
        pit_status="pit_safe_now",
        joinability="not joinable to candidate rows",
        limitations="refresh metadata only; no direct feature content",
        candidate_code_overlap=None,
        candidate_code_overlap_rate=None,
    )
    jobs = _entry(
        source_id="events_refresh_jobs",
        source_kind="operational_metadata",
        path_or_table=f"{DB_PATH}::events_refresh_jobs",
        row_count=_table_count(con, "events_refresh_jobs"),
        symbol_key=None,
        event_date_key=None,
        as_of_key="started_at / finished_at",
        fetched_at_key=None,
        historical_coverage="job log only",
        snapshot_mode="operational state only",
        pit_status="pit_safe_now",
        joinability="not joinable to candidate rows",
        limitations="operational log only; helpful for refresh diagnostics but not features",
        candidate_code_overlap=None,
        candidate_code_overlap_rate=None,
        extra={
            "status_counts": {k: int(v) for k, v in con.execute("select status, count(*) from events_refresh_jobs group by status order by count(*) desc, status").fetchall()}
        },
    )
    gap_earnings = _entry(
        source_id="missing_earnings_actual_announcement_date_source",
        source_kind="gap",
        path_or_table=None,
        row_count=None,
        symbol_key="code/sec_code",
        event_date_key="actual_announcement_date",
        as_of_key=None,
        fetched_at_key=None,
        historical_coverage="no dedicated local historical source found",
        snapshot_mode="not present locally",
        pit_status="external_source_required",
        joinability="would join on code/sec_code after selecting an external or backfilled announcement feed",
        limitations="current JPX earnings table only exposes planned dates, not actual announcement dates",
        candidate_code_overlap=None,
        candidate_code_overlap_rate=None,
    )
    gap_dilution = _entry(
        source_id="missing_dilution_or_financing_event_source",
        source_kind="gap",
        path_or_table=None,
        row_count=None,
        symbol_key="code/sec_code",
        event_date_key="event_date",
        as_of_key=None,
        fetched_at_key=None,
        historical_coverage="no dedicated local historical feed found",
        snapshot_mode="not present locally",
        pit_status="external_source_required",
        joinability="would need a formal event feed or text-classification archive",
        limitations="public offering, third-party allotment, CB/warrant issuance, and dilution-like events are not exposed as a native local table today",
        candidate_code_overlap=None,
        candidate_code_overlap_rate=None,
    )
    gap_dividend = _entry(
        source_id="missing_ex_dividend_or_shareholder_benefit_source",
        source_kind="gap",
        path_or_table=None,
        row_count=None,
        symbol_key="code/sec_code",
        event_date_key="ex_dividend_or_benefit_date",
        as_of_key=None,
        fetched_at_key=None,
        historical_coverage="no dedicated local historical feed found",
        snapshot_mode="not present locally",
        pit_status="external_source_required",
        joinability="would need a dividend/benefit corporate-action feed or a populated TDnet archive",
        limitations="current TDnet tables are empty in the local DB snapshot",
        candidate_code_overlap=None,
        candidate_code_overlap_rate=None,
    )

    inventory = [earnings, rights, edinet, tdnet, tdnet_feat, edinet_analysis, company_map, meta, jobs, gap_earnings, gap_dilution, gap_dividend]
    pit_audit = []
    for item in inventory:
        audit = dict(item)
        if item["source_id"] == "jpx_earnings_planned_snapshot_archive":
            audit["backfill_discipline"] = {
                "as_of_timestamp": "use the latest snapshot folder dated <= anchor date; never use a future snapshot folder",
                "publication_date": "planned_date",
                "correction_handling": "later snapshot supersedes earlier snapshot for the same code/planned_date/kind/company",
                "stale_snapshot_handling": "fallback to the latest snapshot folder not after anchor date",
                "symbol_mapping": "normalize code to 4-digit sec code",
                "survivorship_delisting": "retain historical rows even if code later disappears from the latest table",
                "date_window_logic": "compare planned_date against anchor date only after as-of snapshot selection",
            }
        elif item["source_id"] == "jpx_ex_rights_snapshot_archive":
            audit["backfill_discipline"] = {
                "as_of_timestamp": "use the latest snapshot folder dated <= anchor date; never use a future snapshot folder",
                "publication_date": "ex_date / last_rights_date",
                "correction_handling": "later snapshot supersedes earlier snapshot for the same code/ex_date/record_date/category",
                "stale_snapshot_handling": "fallback to the latest snapshot folder not after anchor date",
                "symbol_mapping": "normalize code to 4-digit sec code",
                "survivorship_delisting": "retain historical rows even if code later disappears from the latest table",
                "date_window_logic": "compare ex_date or last_rights_date against anchor date after as-of snapshot selection",
            }
        elif item["source_id"] == "edinetdb_official_documents":
            audit["backfill_discipline"] = {
                "as_of_timestamp": "use submit_datetime as event time and fetched_at as load time; never include filings after anchor date",
                "publication_date": "submit_datetime",
                "correction_handling": "upsert by doc_id; preserve legal_status and latest payload for the latest snapshot",
                "stale_snapshot_handling": "use the newest DB snapshot but filter by submit_datetime <= anchor date",
                "symbol_mapping": "prefer sec_code; fall back to edinet_code via edinetdb_company_map when sec_code is blank",
                "survivorship_delisting": "keep filings even if the issuer later delists; event time is the submission time",
                "date_window_logic": "compare submit_datetime to anchor date, with JST normalization if needed",
            }
        elif item["source_kind"] in {"core_event_source", "derived_event_source"} and item["row_count"] == 0:
            audit["pit_status"] = "not_usable_for_historical_research"
            audit["backfill_discipline"] = None
        else:
            audit["backfill_discipline"] = None
        pit_audit.append(audit)

    feature_list = [
        {
            "feature_name": "earnings_nearby_flag",
            "source": "jpx_earnings_planned_snapshot_archive",
            "required_fields": ["code", "planned_date", "snapshot_folder_date", "fetched_at"],
            "computation_rule": "flag true when the as-of snapshot known at anchor_date contains a planned earnings date within the chosen business-day window of the anchor date",
            "no_lookahead_proof": "planned_date comes only from a snapshot folder whose date is <= anchor_date",
            "coverage_estimate": "high by code overlap (445/485 candidate codes, 91.8%)",
            "risk_of_leakage": "low if snapshot-date cutoff is enforced",
            "can_implement_now": True,
            "missing_upstream_requirement": None,
        },
        {
            "feature_name": "earnings_window_bucket",
            "source": "jpx_earnings_planned_snapshot_archive",
            "required_fields": ["code", "planned_date", "snapshot_folder_date", "fetched_at"],
            "computation_rule": "bucket days_to_next_earnings into same-day / near-term / mid-term / far / missing bands using anchor_date and the as-of snapshot",
            "no_lookahead_proof": "the bucket is derived only after selecting the latest snapshot not after anchor_date",
            "coverage_estimate": "same code coverage as earnings_nearby_flag",
            "risk_of_leakage": "low with snapshot discipline",
            "can_implement_now": True,
            "missing_upstream_requirement": None,
        },
        {
            "feature_name": "days_to_next_earnings",
            "source": "jpx_earnings_planned_snapshot_archive",
            "required_fields": ["code", "planned_date", "snapshot_folder_date", "fetched_at"],
            "computation_rule": "planned_date minus anchor_date, expressed in calendar or business days after selecting the as-of snapshot",
            "no_lookahead_proof": "snapshot cutoff prevents future planned dates from entering earlier anchors",
            "coverage_estimate": "high for codes that appear in the JPX schedule archive",
            "risk_of_leakage": "low if as-of filter is strict",
            "can_implement_now": True,
            "missing_upstream_requirement": None,
        },
        {
            "feature_name": "days_since_last_earnings",
            "source": "tdnet_disclosures or an earnings announcement archive",
            "required_fields": ["sec_code", "published_at / actual_announcement_date"],
            "computation_rule": "days from anchor_date back to the most recent actual earnings announcement",
            "no_lookahead_proof": "requires a historical announcement archive with publication timestamps; planned dates are not a substitute",
            "coverage_estimate": "unknown; current TDnet tables are empty",
            "risk_of_leakage": "high until a historical announcement archive exists",
            "can_implement_now": False,
            "missing_upstream_requirement": "populate TDnet or another earnings announcement archive with published-at timestamps",
        },
        {
            "feature_name": "ex_rights_nearby_flag",
            "source": "jpx_ex_rights_snapshot_archive",
            "required_fields": ["code", "ex_date", "last_rights_date", "snapshot_folder_date", "fetched_at"],
            "computation_rule": "flag true when anchor_date is within the chosen business-day window before ex_date or last_rights_date",
            "no_lookahead_proof": "choose the latest rights snapshot folder not after anchor_date; do not use future folder dates",
            "coverage_estimate": "narrow but real (6/485 candidate codes, 1.2% distinct code overlap)",
            "risk_of_leakage": "low with snapshot discipline",
            "can_implement_now": True,
            "missing_upstream_requirement": None,
        },
        {
            "feature_name": "ex_dividend_nearby_flag",
            "source": "TDnet or a dedicated corporate-action feed",
            "required_fields": ["sec_code", "published_at", "dividend/ex-date"],
            "computation_rule": "flag true when dividend ex-date or dividend announcement falls within the chosen window",
            "no_lookahead_proof": "requires a historical published-at feed; current TDnet tables are empty",
            "coverage_estimate": "unknown; zero in current local snapshot",
            "risk_of_leakage": "high until a populated historical feed exists",
            "can_implement_now": False,
            "missing_upstream_requirement": "populate TDnet disclosure tables or bring in a dedicated dividend feed",
        },
        {
            "feature_name": "rights_window_bucket",
            "source": "jpx_ex_rights_snapshot_archive",
            "required_fields": ["code", "ex_date", "last_rights_date", "snapshot_folder_date", "fetched_at"],
            "computation_rule": "bucket the distance to ex_date/last_rights_date into near / mid / far / missing bands",
            "no_lookahead_proof": "as-of snapshot selection prevents future rights rows from entering earlier anchors",
            "coverage_estimate": "same narrow coverage as ex_rights_nearby_flag",
            "risk_of_leakage": "low with backfill discipline",
            "can_implement_now": True,
            "missing_upstream_requirement": None,
        },
        {
            "feature_name": "shareholder_benefit_rights_flag",
            "source": "dedicated rights/corporate-action feed or TDnet disclosure archive",
            "required_fields": ["sec_code", "published_at", "benefit/right classification"],
            "computation_rule": "flag true when a shareholder-benefit rights event is published within the chosen window",
            "no_lookahead_proof": "needs a historical published-at event feed; no such local feed was found",
            "coverage_estimate": "unknown",
            "risk_of_leakage": "high until a dedicated feed exists",
            "can_implement_now": False,
            "missing_upstream_requirement": "select a historical corporate-action feed or populate TDnet disclosures",
        },
        {
            "feature_name": "dilution_event_nearby_flag",
            "source": "edinetdb_official_documents plus a disclosure classifier",
            "required_fields": ["sec_code", "submit_datetime", "form_code", "doc_description", "payload_json"],
            "computation_rule": "flag true when a filing classified as public offering / third-party allotment / CB / warrant issuance / similar financing is published near anchor_date",
            "no_lookahead_proof": "filings are time-stamped by submit_datetime; only documents with submit_datetime <= anchor_date are eligible",
            "coverage_estimate": "moderate code overlap (193/485 distinct candidate codes, 39.8%), but event taxonomy still needs extraction",
            "risk_of_leakage": "medium until classification rules are frozen and publication-time filtering is enforced",
            "can_implement_now": False,
            "missing_upstream_requirement": "write a stable filing classifier for financing/dilution events and backfill filing history",
        },
        {
            "feature_name": "financing_event_nearby_flag",
            "source": "edinetdb_official_documents plus TDnet disclosure archive",
            "required_fields": ["sec_code", "published_at / submit_datetime", "event classifier"],
            "computation_rule": "flag true for public offering, third-party allotment, CB issuance, or warrant issuance published near anchor_date",
            "no_lookahead_proof": "publish timestamp is required; current TDnet tables are empty so the classifier cannot be validated yet",
            "coverage_estimate": "moderate on EDINET codes, but incomplete without TDnet",
            "risk_of_leakage": "medium to high until the classifier is tied to a frozen historical source",
            "can_implement_now": False,
            "missing_upstream_requirement": "add a historical disclosure archive or a frozen filing classifier",
        },
        {
            "feature_name": "tdnet_material_disclosure_nearby_flag",
            "source": "tdnet_disclosures + tdnet_disclosure_features",
            "required_fields": ["sec_code", "published_at", "event_type", "importance_score"],
            "computation_rule": "flag true when a material disclosure classified as earnings/dividend/share split/governance/distress falls near anchor_date",
            "no_lookahead_proof": "published_at is the event time; classification must be derived from a historical TDnet archive",
            "coverage_estimate": "zero in the current local snapshot",
            "risk_of_leakage": "high until the TDnet tables are populated",
            "can_implement_now": False,
            "missing_upstream_requirement": "populate tdnet_disclosures and tdnet_disclosure_features",
        },
        {
            "feature_name": "event_risk_bucket",
            "source": "JPX earnings/rights backfill + TDnet/EDINET event classification",
            "required_fields": ["earnings flags", "rights flags", "TDnet/EDINET disclosure flags"],
            "computation_rule": "combine earnings, rights, dividend, dilution, and material-disclosure signals into a single event-risk bucket",
            "no_lookahead_proof": "requires every component to be timestamped and filtered as-of anchor_date before the bucket is computed",
            "coverage_estimate": "partial now; full bucket coverage is blocked by missing TDnet/event-classification sources",
            "risk_of_leakage": "medium unless every component is frozen to a historical archive",
            "can_implement_now": False,
            "missing_upstream_requirement": "complete JPX backfill first, then add TDnet/EDINET event extraction",
        },
    ]

    inventory = {
        "schema_version": "tradex_event_source_contract_design_v1_event_source_contract_inventory_v1",
        "generated_at_utc": _utc_now(),
        "candidate_distinct_codes": candidate_code_count,
        "sources": [earnings, rights, edinet, tdnet, tdnet_feat, edinet_analysis, company_map, meta, jobs, gap_earnings, gap_dilution, gap_dividend],
        "raw_archive_stats": {
            "jpx_financial_announcement": archive_earnings,
            "jpx_ex_rights": archive_rights,
        },
    }
    pit = {
        "schema_version": "tradex_event_source_contract_design_v1_event_source_pit_safety_audit_v1",
        "generated_at_utc": _utc_now(),
        "sources": pit_audit,
    }
    features = {
        "schema_version": "tradex_event_source_contract_design_v1_event_feature_contract_proposal_v1",
        "generated_at_utc": _utc_now(),
        "feature_groups": {
            "earnings": ["earnings_nearby_flag", "earnings_window_bucket", "days_to_next_earnings", "days_since_last_earnings"],
            "rights": ["ex_rights_nearby_flag", "ex_dividend_nearby_flag", "rights_window_bucket", "shareholder_benefit_rights_flag"],
            "corporate_action": ["dilution_event_nearby_flag", "financing_event_nearby_flag"],
            "tdnet": ["tdnet_material_disclosure_nearby_flag"],
            "composite": ["event_risk_bucket"],
        },
        "features": feature_list,
    }
    recommendation = {
        "schema_version": "tradex_event_source_contract_design_v1_event_axis_next_step_recommendation_v1",
        "generated_at_utc": _utc_now(),
        "recommended_next_path": "implement_earnings_rights_snapshot_backfill",
        "why_this_next": [
            "The JPX earnings and ex-rights archives already exist locally with dated snapshot folders.",
            "They are the clearest point-in-time-safe sources available now for event context.",
            "They can be backfilled without changing ranking or MeeMee.",
            "TDnet disclosure tables are empty today, so they are not the immediate next implementation path.",
        ],
        "what_it_unlocks": [
            "earnings_nearby_flag",
            "earnings_window_bucket",
            "days_to_next_earnings",
            "ex_rights_nearby_flag",
            "rights_window_bucket",
        ],
        "what_stays_later": [
            "days_since_last_earnings",
            "ex_dividend_nearby_flag",
            "shareholder_benefit_rights_flag",
            "dilution_event_nearby_flag",
            "financing_event_nearby_flag",
            "tdnet_material_disclosure_nearby_flag",
            "event_risk_bucket",
        ],
        "implementation_cost": "low to moderate",
        "coverage_note": "earnings coverage is broad on the candidate code set; rights coverage is narrow but still worth preserving as a precise event-risk feature",
    }
    decision = {
        "schema_version": "tradex_event_source_contract_design_v1_decision_v1",
        "generated_at_utc": _utc_now(),
        "decision": "ready_to_implement_pit_event_backfill",
        "status": "ready_to_implement_pit_event_backfill",
        "reason": "local JPX earnings and rights snapshot archives exist and can be made point-in-time-safe with as-of backfill discipline",
        "next_step": "implement_earnings_rights_snapshot_backfill",
        "source_status": {
            "core_available": ["jpx_earnings_planned_snapshot_archive", "jpx_ex_rights_snapshot_archive", "edinetdb_official_documents"],
            "empty_or_not_ready": ["tdnet_disclosures", "tdnet_disclosure_features", "edinetdb_analysis"],
            "gap_sources": ["earnings_actual_announcement_date", "ex_dividend", "shareholder_benefit_rights", "dilution/financing event feed"],
        },
    }
    return inventory, pit, features, recommendation, decision


def run_event_source_contract_design_v1(output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    cand_codes, candidate_code_count = _candidate_codes()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        inventory, pit, features, recommendation, decision = _build_all(con, cand_codes, candidate_code_count)
        session_dir = output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{datetime.now(timezone.utc).microsecond:06d}"
        session_dir.mkdir(parents=True, exist_ok=False)

        run_manifest = {
            "schema_version": "tradex_event_source_contract_design_v1_run_manifest_v1",
            "generated_at_utc": _utc_now(),
            "session_id": session_dir.name,
            "output_root": str(output_root),
            "session_dir": str(session_dir),
            "repo_root": str(REPO_ROOT),
            "db_path": str(DB_PATH),
            "candidate_surface_path": str(CANDIDATE_SURFACE),
            "candidate_distinct_codes": candidate_code_count,
        }
        input_resolution = {
            "schema_version": "tradex_event_source_contract_design_v1_input_resolution_v1",
            "resolved_paths": {
                "db_path": str(DB_PATH),
                "candidate_surface_path": str(CANDIDATE_SURFACE),
                "jpx_earnings_archive_root": str(JPX_EARNINGS_ARCHIVE),
                "jpx_rights_archive_root": str(JPX_RIGHTS_ARCHIVE),
            },
            "resolved_tables": [
                "earnings_planned",
                "ex_rights",
                "edinetdb_official_documents",
                "tdnet_disclosures",
                "tdnet_disclosure_features",
                "edinetdb_analysis",
                "edinetdb_company_map",
                "events_meta",
                "events_refresh_jobs",
            ],
            "resolution_notes": [
                "JPX earnings and ex-rights raw archives exist under data_store/raw and are dated by fetch day.",
                "TDnet disclosure tables exist in schema but are empty in the current local DB snapshot.",
                "EDINET official documents exist with submit_datetime and fetched_at and can support filing-time event features.",
            ],
        }

        _write_json(session_dir / "run_manifest.json", run_manifest)
        _write_json(session_dir / "input_resolution.json", input_resolution)
        _write_json(session_dir / "event_source_contract_inventory.json", inventory)
        _write_json(session_dir / "event_source_pit_safety_audit.json", pit)
        _write_json(session_dir / "event_feature_contract_proposal.json", features)
        _write_json(session_dir / "event_axis_next_step_recommendation.json", recommendation)
        _write_json(session_dir / "event_source_contract_design_v1_decision.json", decision)
        _write_json(
            session_dir / "_ARTIFACT_COMPLETE.json",
            {
                "schema_version": "tradex_event_source_contract_design_v1_artifact_complete_v1",
                "generated_at_utc": _utc_now(),
                "session_dir": str(session_dir),
                "artifact_count": 8,
                "artifacts": [
                    "run_manifest.json",
                    "input_resolution.json",
                    "event_source_contract_inventory.json",
                    "event_source_pit_safety_audit.json",
                    "event_feature_contract_proposal.json",
                    "event_axis_next_step_recommendation.json",
                    "event_source_contract_design_v1_decision.json",
                    "_ARTIFACT_COMPLETE.json",
                ],
            },
        )
        pd.DataFrame(inventory["sources"]).to_parquet(session_dir / "source_table_inventory.parquet", index=False)
        return {"output_dir": str(session_dir), "decision": decision["decision"], "session_id": session_dir.name}
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Design PIT-safe event / earnings / rights source contract.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(json.dumps(run_event_source_contract_design_v1(args.output_root), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
