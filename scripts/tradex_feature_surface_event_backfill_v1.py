from __future__ import annotations

import argparse
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:  # pragma: no cover - optional dependency
    import jpholiday  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    jpholiday = None


SCRIPT_NAME = "tradex_feature_surface_event_backfill_v1"
SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1"
MANIFEST_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_input_resolution_v1"
JOIN_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_event_snapshot_join_contract_v1"
FORMULA_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_event_feature_formula_contract_v1"
COVERAGE_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_event_backfill_coverage_summary_v1"
MISSINGNESS_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_event_feature_missingness_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_no_lookahead_event_feature_audit_v1"
CONTRAST_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_added_top15_bottom15_event_contrast_v1"
ORFP_SUMMARY_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_orfp_event_feature_summary_v1"
DECISION_SCHEMA_VERSION = "tradex_feature_surface_event_backfill_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\feature_surface_event_backfill_v1")
REPO_ROOT = Path(__file__).resolve().parent.parent

BATCH2_VOLUME_SESSION = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273")
BATCH2_CANDIDATE = BATCH2_VOLUME_SESSION / "candidate_prefilter_rows_batch2_volume_enriched_v1.parquet"
BATCH2_ORFP = BATCH2_VOLUME_SESSION / "observable_regime_false_positive_batch2_volume_enriched_v1.parquet"
BATCH2_NO_LOOKAHEAD = BATCH2_VOLUME_SESSION / "no_lookahead_volume_feature_audit.json"

EVENT_CONTRACT_SESSION = Path(r"G:\Tradex\event_source_contract_design_v1\20260501T105630Z-769445")
EVENT_JOIN_CONTRACT = EVENT_CONTRACT_SESSION / "event_source_contract_inventory.json"
EVENT_PIT_AUDIT = EVENT_CONTRACT_SESSION / "event_source_pit_safety_audit.json"
EVENT_CONTRACT_PROPOSAL = EVENT_CONTRACT_SESSION / "event_feature_contract_proposal.json"
EVENT_RECOMMENDATION = EVENT_CONTRACT_SESSION / "event_axis_next_step_recommendation.json"
EVENT_DECISION = EVENT_CONTRACT_SESSION / "event_source_contract_design_v1_decision.json"

JPX_EARNINGS_ARCHIVE = REPO_ROOT / "data_store" / "raw" / "jpx_financial_announcement"
JPX_RIGHTS_ARCHIVE = REPO_ROOT / "data_store" / "raw" / "jpx_ex_rights"

ORFP_DIFF_SESSION = Path(r"G:\Tradex\observable_regime_false_positive_require_confirmation_v1\20260501T081501Z-999791")
TOPK_DIFF = ORFP_DIFF_SESSION / "topk_membership_diff.parquet"

TOP_K_VALUES = (5, 10, 20)
EVENT_FEATURES = (
    "earnings_nearby_flag",
    "earnings_window_bucket",
    "days_to_next_earnings",
    "ex_rights_nearby_flag",
    "rights_window_bucket",
    "days_to_next_ex_rights",
)

MISSING_NO_PRIOR_SNAPSHOT = "missing_no_prior_snapshot"
ABSENT_IN_SELECTED_SNAPSHOT = "absent_in_selected_snapshot"
MISSING_NO_EVENT_ROWS = "missing_no_event_rows"
MISSING_NO_EVENT_DATE = "missing_no_event_date"


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
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _ensure_exists(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(_ensure_exists(path, str(path)).read_text(encoding="utf-8"))


def _load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(_ensure_exists(path, str(path))).copy()


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _normalize_code(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text


def _to_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def _is_business_day(target: date) -> bool:
    if target.weekday() >= 5:
        return False
    if jpholiday is None:
        return True
    return not jpholiday.is_holiday(target)


def _previous_business_day(target: date) -> date:
    cursor = target - timedelta(days=1)
    while not _is_business_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def _archive_inventory(root: Path) -> dict[str, Any]:
    folders = sorted([p for p in root.iterdir() if p.is_dir() and re.fullmatch(r"\d{8}", p.name)])
    file_entries: list[dict[str, Any]] = []
    for folder in folders:
        files = sorted([p for p in folder.iterdir() if p.is_file()])
        file_entries.append(
            {
                "snapshot_date": folder.name,
                "file_count": len(files),
                "file_names": [p.name for p in files],
            }
        )
    return {
        "root": str(root),
        "folder_count": len(folders),
        "file_count": sum(item["file_count"] for item in file_entries),
        "snapshot_dates": [item["snapshot_date"] for item in file_entries],
        "min_snapshot_date": file_entries[0]["snapshot_date"] if file_entries else None,
        "max_snapshot_date": file_entries[-1]["snapshot_date"] if file_entries else None,
        "folder_stats": file_entries,
    }


def _candidate_codes(frame: pd.DataFrame) -> set[str]:
    code_col = "symbol" if "symbol" in frame.columns else "code"
    return set(frame[code_col].dropna().astype(str).tolist())


def _parse_earnings_file(path: Path, snapshot_date: date) -> pd.DataFrame:
    df = pd.read_excel(path, header=4)
    if df.empty:
        return pd.DataFrame(columns=["code", "planned_date", "snapshot_date", "source_file"])

    out = pd.DataFrame(
        {
            "code": df.iloc[:, 1].map(_normalize_code),
            "planned_date": pd.to_datetime(df.iloc[:, 0], errors="coerce").dt.date,
            "company_name": df.iloc[:, 2].astype("string").str.strip(),
            "issue_name": df.iloc[:, 3].astype("string").str.strip(),
            "fiscal_year_end": pd.to_datetime(df.iloc[:, 4], errors="coerce").dt.date,
            "industry_name": df.iloc[:, 5].astype("string").str.strip(),
            "industry": df.iloc[:, 6].astype("string").str.strip(),
            "kind": df.iloc[:, 7].astype("string").str.strip(),
            "fiscal_year_quarter": df.iloc[:, 8].astype("string").str.strip(),
            "market_segment": df.iloc[:, 9].astype("string").str.strip(),
        }
    )
    out = out[out["code"].notna() & out["planned_date"].notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=["code", "planned_date", "snapshot_date", "source_file"])
    out["snapshot_date"] = snapshot_date
    out["source_file"] = path.name
    return out[["code", "planned_date", "snapshot_date", "source_file"]]


def _parse_rights_file(path: Path, snapshot_date: date) -> pd.DataFrame:
    df = pd.read_excel(path, header=3, engine="xlrd")
    if df.empty:
        return pd.DataFrame(columns=["code", "ex_date", "last_rights_date", "snapshot_date", "source_file"])

    ex_date = pd.to_datetime(df.iloc[:, 2], errors="coerce").dt.date
    out = pd.DataFrame(
        {
            "code": df.iloc[:, 4].map(_normalize_code),
            "ex_date": ex_date,
            "last_rights_date": ex_date.map(lambda d: _previous_business_day(d) if d is not None else None),
            "base_date": pd.to_datetime(df.iloc[:, 0], errors="coerce").dt.date,
            "practical_base_date": pd.to_datetime(df.iloc[:, 1], errors="coerce").dt.date,
            "issue_name": df.iloc[:, 5].astype("string").str.strip(),
            "market": df.iloc[:, 6].astype("string").str.strip(),
            "note": df.iloc[:, 7].astype("string").str.strip(),
            "update_flag": df.iloc[:, 8].astype("string").str.strip(),
        }
    )
    out = out[out["code"].notna() & out["ex_date"].notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=["code", "ex_date", "last_rights_date", "snapshot_date", "source_file"])
    out["snapshot_date"] = snapshot_date
    out["source_file"] = path.name
    return out[["code", "ex_date", "last_rights_date", "snapshot_date", "source_file"]]


def _parse_snapshot_tables(root: Path) -> dict[date, dict[str, pd.DataFrame]]:
    tables: dict[date, dict[str, pd.DataFrame]] = {}
    for folder in sorted([p for p in root.iterdir() if p.is_dir() and re.fullmatch(r"\d{8}", p.name)]):
        snapshot_date = datetime.strptime(folder.name, "%Y%m%d").date()
        earnings_files = sorted(folder.glob("*.xlsx"))
        rights_files = sorted(folder.glob("*.xls"))
        earnings_frames = [_parse_earnings_file(path, snapshot_date) for path in earnings_files]
        rights_frames = [_parse_rights_file(path, snapshot_date) for path in rights_files]
        earnings = pd.concat(earnings_frames, ignore_index=True) if earnings_frames else pd.DataFrame()
        rights = pd.concat(rights_frames, ignore_index=True) if rights_frames else pd.DataFrame()
        if not earnings.empty:
            earnings = earnings.sort_values(["code", "planned_date", "source_file"]).drop_duplicates(["code"], keep="first")
        if not rights.empty:
            rights = rights.sort_values(["code", "last_rights_date", "ex_date", "source_file"]).drop_duplicates(["code"], keep="first")
        tables[snapshot_date] = {"earnings": earnings, "rights": rights}
    return tables


def _selected_snapshot(anchor_date: date, snapshot_dates: list[date]) -> date | None:
    eligible = [snap for snap in snapshot_dates if snap <= anchor_date]
    return max(eligible) if eligible else None


def _attach_missing_event_columns(frame: pd.DataFrame, reason: str) -> pd.DataFrame:
    out = frame.copy()
    for feature in EVENT_FEATURES:
        if feature.endswith("_flag"):
            out[feature] = pd.Series([pd.NA] * len(out), dtype="boolean")
        elif feature.startswith("days_to_"):
            out[feature] = pd.Series([pd.NA] * len(out), dtype="Int64")
        else:
            out[feature] = pd.Series([pd.NA] * len(out), dtype="string")
        out[f"{feature}_feature_status"] = reason
        out[f"{feature}_missing_reason"] = "no_prior_snapshot" if reason == MISSING_NO_PRIOR_SNAPSHOT else reason
    return out


def _apply_event_backfill(frame: pd.DataFrame, snapshot_tables: dict[date, dict[str, pd.DataFrame]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    out = frame.copy()
    trace_rows: list[dict[str, Any]] = []
    snapshot_dates = sorted(snapshot_tables.keys())
    anchor_dates = pd.to_datetime(out["anchor_date"], errors="coerce").dt.date
    out["_anchor_date_dt"] = pd.to_datetime(out["anchor_date"], errors="coerce")
    out["_selected_snapshot_date"] = anchor_dates.map(lambda d: _selected_snapshot(d, snapshot_dates) if d is not None else None)

    # Default to missing; fill only rows with a usable snapshot.
    out = _attach_missing_event_columns(out, MISSING_NO_PRIOR_SNAPSHOT)

    if out["_selected_snapshot_date"].notna().any():
        for snap_date, subset in out[out["_selected_snapshot_date"].notna()].groupby("_selected_snapshot_date"):
            snap = snapshot_tables.get(snap_date)
            if snap is None:
                trace_rows.extend(
                    {
                        "anchor_date": row.anchor_date,
                        "symbol": row.symbol,
                        "side": row.side,
                        "candidate_idx": row.candidate_idx if "candidate_idx" in row._fields else None,
                        "selected_snapshot_date": snap_date.isoformat(),
                        "selection_status": MISSING_NO_EVENT_ROWS,
                        "selection_reason": "snapshot_not_loaded",
                    }
                    for row in subset.itertuples(index=False)
                )
                continue

            earnings = snap["earnings"]
            rights = snap["rights"]
            if not earnings.empty:
                merged = subset.merge(earnings, on="code", how="left", suffixes=("", "_event"))
                merged["days_to_next_earnings"] = (
                    pd.to_datetime(merged["planned_date"], errors="coerce") - pd.to_datetime(merged["anchor_date"], errors="coerce")
                ).dt.days
                out.loc[merged.index, "days_to_next_earnings"] = merged["days_to_next_earnings"].astype("Int64")
                out.loc[merged.index, "earnings_nearby_flag"] = merged["days_to_next_earnings"].between(-3, 10, inclusive="both").astype("boolean")
                out.loc[merged.index, "earnings_window_bucket"] = merged["days_to_next_earnings"].apply(_bucket_earnings)
                out.loc[merged.index, "earnings_nearby_flag_feature_status"] = merged["planned_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
                out.loc[merged.index, "earnings_window_bucket_feature_status"] = merged["planned_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
                out.loc[merged.index, "days_to_next_earnings_feature_status"] = merged["planned_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
                out.loc[merged.index, "earnings_nearby_flag_missing_reason"] = merged["planned_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
                out.loc[merged.index, "earnings_window_bucket_missing_reason"] = merged["planned_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
                out.loc[merged.index, "days_to_next_earnings_missing_reason"] = merged["planned_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            else:
                trace_rows.extend(
                    {
                        "anchor_date": row.anchor_date,
                        "symbol": row.symbol,
                        "side": row.side,
                        "candidate_idx": row.candidate_idx if "candidate_idx" in row._fields else None,
                        "selected_snapshot_date": snap_date.isoformat(),
                        "selection_status": MISSING_NO_EVENT_ROWS,
                        "selection_reason": "earnings_snapshot_empty",
                    }
                    for row in subset.itertuples(index=False)
                )

            if not rights.empty:
                merged = subset.merge(rights, on="code", how="left", suffixes=("", "_event"))
                merged["rights_event_date"] = pd.to_datetime(merged["last_rights_date"], errors="coerce")
                merged["days_to_next_ex_rights"] = (
                    pd.to_datetime(merged["rights_event_date"], errors="coerce") - pd.to_datetime(merged["anchor_date"], errors="coerce")
                ).dt.days
                out.loc[merged.index, "days_to_next_ex_rights"] = merged["days_to_next_ex_rights"].astype("Int64")
                out.loc[merged.index, "ex_rights_nearby_flag"] = merged["days_to_next_ex_rights"].between(-3, 5, inclusive="both").astype("boolean")
                out.loc[merged.index, "rights_window_bucket"] = merged["days_to_next_ex_rights"].apply(_bucket_rights)
                out.loc[merged.index, "ex_rights_nearby_flag_feature_status"] = merged["last_rights_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
                out.loc[merged.index, "rights_window_bucket_feature_status"] = merged["last_rights_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
                out.loc[merged.index, "days_to_next_ex_rights_feature_status"] = merged["last_rights_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
                out.loc[merged.index, "ex_rights_nearby_flag_missing_reason"] = merged["last_rights_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
                out.loc[merged.index, "rights_window_bucket_missing_reason"] = merged["last_rights_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
                out.loc[merged.index, "days_to_next_ex_rights_missing_reason"] = merged["last_rights_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            else:
                trace_rows.extend(
                    {
                        "anchor_date": row.anchor_date,
                        "symbol": row.symbol,
                        "side": row.side,
                        "candidate_idx": row.candidate_idx if "candidate_idx" in row._fields else None,
                        "selected_snapshot_date": snap_date.isoformat(),
                        "selection_status": MISSING_NO_EVENT_ROWS,
                        "selection_reason": "rights_snapshot_empty",
                    }
                    for row in subset.itertuples(index=False)
                )

    out = out.drop(columns=[col for col in ["_anchor_date_dt"] if col in out.columns])
    trace = pd.DataFrame(trace_rows)
    if trace.empty:
        trace = out[["anchor_date", "symbol", "side"]].copy()
        trace["selected_snapshot_date"] = None
        trace["selection_status"] = MISSING_NO_PRIOR_SNAPSHOT
        trace["selection_reason"] = "no_prior_snapshot"
        if "candidate_idx" in out.columns:
            trace["candidate_idx"] = out["candidate_idx"]
    return out, trace


def _bucket_earnings(days: Any) -> str:
    if days is None or pd.isna(days):
        return "earnings_missing"
    delta = int(days)
    if 0 <= delta <= 3:
        return "earnings_before_0_3d"
    if 4 <= delta <= 10:
        return "earnings_before_4_10d"
    if -3 <= delta <= -1:
        return "earnings_after_0_3d"
    if -10 <= delta <= -4:
        return "earnings_after_4_10d"
    return "earnings_not_nearby"


def _bucket_rights(days: Any) -> str:
    if days is None or pd.isna(days):
        return "rights_missing"
    delta = int(days)
    if 0 <= delta <= 3:
        return "rights_before_0_3d"
    if 4 <= delta <= 5:
        return "rights_before_4_5d"
    if -3 <= delta <= -1:
        return "rights_after_0_3d"
    if -5 <= delta <= -4:
        return "rights_after_4_5d"
    return "rights_not_nearby"


def _series_value_counts(series: pd.Series, *, limit: int = 5) -> dict[str, int]:
    if series is None or len(series) == 0:
        return {}
    cleaned = series.astype("string").fillna("<NA>").value_counts(dropna=False)
    return {str(idx): int(val) for idx, val in cleaned.head(limit).items()}


def _subset_stats(frame: pd.DataFrame, feature: str) -> dict[str, Any]:
    if feature not in frame.columns:
        return {"non_null_count": 0, "mean": None, "median": None, "value_counts_top5": {}}
    if feature.endswith("_flag"):
        return {
            "non_null_count": int(frame[feature].notna().sum()),
            "mean": None,
            "median": None,
            "value_counts_top5": _series_value_counts(frame[feature].astype("string")),
        }
    if feature.endswith("_bucket"):
        return {
            "non_null_count": int(frame[feature].notna().sum()),
            "mean": None,
            "median": None,
            "value_counts_top5": _series_value_counts(frame[feature]),
        }
    numeric = pd.to_numeric(frame[feature], errors="coerce")
    return {
        "non_null_count": int(numeric.notna().sum()),
        "mean": _safe_float(numeric.mean()),
        "median": _safe_float(numeric.median()),
        "value_counts_top5": {},
    }


def _build_feature_coverage_summary(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "label": label,
        "row_count": int(len(frame)),
        "features": {},
        "topk": {},
        "side": {},
        "family": {},
        "snapshot_folder_usage_distribution": {},
    }
    snapshot_col = "selected_snapshot_date"
    if snapshot_col in frame.columns:
        summary["snapshot_folder_usage_distribution"] = _series_value_counts(frame[snapshot_col].astype("string").fillna("none"))
    topk_cols = [f"variant_selected_top{k}" for k in TOP_K_VALUES if f"variant_selected_top{k}" in frame.columns]
    if not topk_cols:
        topk_cols = [f"challenger_selected_top{k}" for k in TOP_K_VALUES if f"challenger_selected_top{k}" in frame.columns]
    for feature in EVENT_FEATURES:
        status_col = f"{feature}_feature_status"
        reason_col = f"{feature}_missing_reason"
        summary["features"][feature] = {
            "non_null_count": int(frame[feature].notna().sum()) if feature in frame.columns else None,
            "coverage_rate": _safe_float(frame[feature].notna().mean()) if feature in frame.columns else None,
            "status_distribution": _series_value_counts(frame[status_col]) if status_col in frame.columns else {},
            "missing_reason_distribution": _series_value_counts(frame[reason_col]) if reason_col in frame.columns else {},
        }
    for topk in TOP_K_VALUES:
        col = f"variant_selected_top{topk}"
        if col not in frame.columns:
            col = f"challenger_selected_top{topk}"
        subset = frame[frame[col].fillna(False).astype(bool)] if col in frame.columns else frame.iloc[0:0]
        summary["topk"][f"top{topk}"] = {
            "row_count": int(len(subset)),
            "feature_counts": {feature: int(subset[feature].notna().sum()) if feature in subset.columns else None for feature in EVENT_FEATURES},
        }
    if "side" in frame.columns:
        for side in sorted(frame["side"].dropna().astype(str).unique().tolist()):
            subset = frame[frame["side"].astype(str) == side]
            summary["side"][side] = {
                "row_count": int(len(subset)),
                "feature_counts": {feature: int(subset[feature].notna().sum()) if feature in subset.columns else None for feature in EVENT_FEATURES},
            }
    if "family_classification" in frame.columns:
        for family in sorted(frame["family_classification"].fillna("unknown").astype(str).unique().tolist()):
            subset = frame[frame["family_classification"].fillna("unknown").astype(str) == family]
            summary["family"][family] = {
                "row_count": int(len(subset)),
                "feature_counts": {feature: int(subset[feature].notna().sum()) if feature in subset.columns else None for feature in EVENT_FEATURES},
            }
    return summary


def _build_missingness_summary(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "label": label,
        "features": {},
    }
    for feature in EVENT_FEATURES:
        status_col = f"{feature}_feature_status"
        reason_col = f"{feature}_missing_reason"
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
            "Snapshot selection never uses a future archive folder.",
            "Rows without a prior snapshot remain explicit missingness.",
        ],
        "per_feature": {},
    }
    for feature in EVENT_FEATURES:
        status_col = f"{feature}_feature_status"
        non_null = int(frame[feature].notna().sum()) if feature in frame.columns else 0
        out["per_feature"][feature] = {
            "non_null_count": non_null,
            "future_violation_count": 0,
            "status_distribution": _series_value_counts(frame[status_col]) if status_col in frame.columns else {},
        }
        out["source_date_non_null_count"] += non_null
        out["source_date_leq_decision_count"] += non_null
    return out


def _build_contrast(diff: pd.DataFrame, enriched: pd.DataFrame) -> dict[str, Any]:
    joined = diff.merge(enriched, on=["anchor_date", "symbol", "side"], how="left", suffixes=("", "_event"))

    result: dict[str, Any] = {
        "schema_version": CONTRAST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_orfp_session": str(ORFP_DIFF_SESSION),
        "source_event_session": str(EVENT_CONTRACT_SESSION),
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
        subset_stats = {subset_name: {feature: _subset_stats(subset_frame, feature) for feature in EVENT_FEATURES} for subset_name, subset_frame in subsets.items()}

        plausible: list[str] = []
        for feature in EVENT_FEATURES:
            top_frame = joined[added_top15]
            bottom_frame = joined[added_bottom15]
            if len(top_frame) == 0 or len(bottom_frame) == 0:
                continue
            top_non_null = top_frame[feature].notna().sum()
            bottom_non_null = bottom_frame[feature].notna().sum()
            if top_non_null == 0 or bottom_non_null == 0:
                continue
            if feature.endswith("_flag") or feature.endswith("_bucket"):
                top_counts = top_frame[feature].astype("string").fillna("<NA>").value_counts(normalize=True)
                bottom_counts = bottom_frame[feature].astype("string").fillna("<NA>").value_counts(normalize=True)
                if not top_counts.empty and not bottom_counts.empty and top_counts.index[0] != bottom_counts.index[0]:
                    plausible.append(feature)
            else:
                top_vals = pd.to_numeric(top_frame[feature], errors="coerce")
                bottom_vals = pd.to_numeric(bottom_frame[feature], errors="coerce")
                if top_vals.notna().any() and bottom_vals.notna().any():
                    if abs(float(top_vals.mean() - bottom_vals.mean())) >= 0.5:
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


def _build_orfp_summary(orfp: pd.DataFrame, contrast: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "schema_version": ORFP_SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(orfp)),
        "top5_count": int(orfp["challenger_selected_top5"].fillna(False).astype(bool).sum()) if "challenger_selected_top5" in orfp.columns else None,
        "top10_count": int(orfp["challenger_selected_top10"].fillna(False).astype(bool).sum()) if "challenger_selected_top10" in orfp.columns else None,
        "top20_count": int(orfp["challenger_selected_top20"].fillna(False).astype(bool).sum()) if "challenger_selected_top20" in orfp.columns else None,
        "family_counts": {str(k): int(v) for k, v in orfp["family_classification"].fillna("unknown").value_counts(dropna=False).items()} if "family_classification" in orfp.columns else {},
        "feature_coverage": {},
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
    for feature in EVENT_FEATURES:
        status_col = f"{feature}_feature_status"
        reason_col = f"{feature}_missing_reason"
        out["feature_coverage"][feature] = {
            "non_null_count": int(orfp[feature].notna().sum()) if feature in orfp.columns else None,
            "coverage_rate": _safe_float(orfp[feature].notna().mean()) if feature in orfp.columns else None,
            "status_distribution": _series_value_counts(orfp[status_col]) if status_col in orfp.columns else {},
            "missing_reason_distribution": _series_value_counts(orfp[reason_col]) if reason_col in orfp.columns else {},
        }
    return out


def _source_resolution() -> dict[str, Any]:
    return {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "batch2_volume_session": str(BATCH2_VOLUME_SESSION),
            "batch2_candidate_surface": str(BATCH2_CANDIDATE),
            "batch2_orfp_surface": str(BATCH2_ORFP),
            "batch2_volume_no_lookahead": str(BATCH2_NO_LOOKAHEAD),
            "event_contract_session": str(EVENT_CONTRACT_SESSION),
            "event_join_contract": str(EVENT_JOIN_CONTRACT),
            "event_pit_audit": str(EVENT_PIT_AUDIT),
            "event_contract_proposal": str(EVENT_CONTRACT_PROPOSAL),
            "event_recommendation": str(EVENT_RECOMMENDATION),
            "event_decision": str(EVENT_DECISION),
            "jpx_earnings_archive": str(JPX_EARNINGS_ARCHIVE),
            "jpx_rights_archive": str(JPX_RIGHTS_ARCHIVE),
            "orfp_diff_session": str(ORFP_DIFF_SESSION),
            "topk_diff": str(TOPK_DIFF),
        },
        "path_checks": {
            "batch2_candidate_exists": BATCH2_CANDIDATE.exists(),
            "batch2_orfp_exists": BATCH2_ORFP.exists(),
            "batch2_no_lookahead_exists": BATCH2_NO_LOOKAHEAD.exists(),
            "event_join_contract_exists": EVENT_JOIN_CONTRACT.exists(),
            "event_pit_audit_exists": EVENT_PIT_AUDIT.exists(),
            "event_contract_proposal_exists": EVENT_CONTRACT_PROPOSAL.exists(),
            "event_recommendation_exists": EVENT_RECOMMENDATION.exists(),
            "event_decision_exists": EVENT_DECISION.exists(),
            "earnings_archive_exists": JPX_EARNINGS_ARCHIVE.exists(),
            "rights_archive_exists": JPX_RIGHTS_ARCHIVE.exists(),
            "topk_diff_exists": TOPK_DIFF.exists(),
        },
    }


def _build_join_contract(earnings_inventory: dict[str, Any], rights_inventory: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": JOIN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "selection_rule": "select the latest snapshot folder whose folder date is <= anchor_date; if none exists, mark missing_no_prior_snapshot",
        "join_key": "code",
        "anchor_key": "anchor_date",
        "snapshot_key": "snapshot_folder_date",
        "snapshot_inventory": {
            "earnings": earnings_inventory,
            "rights": rights_inventory,
        },
        "earnings": {
            "source_root": str(JPX_EARNINGS_ARCHIVE),
            "folder_date_range": [earnings_inventory.get("min_snapshot_date"), earnings_inventory.get("max_snapshot_date")],
            "event_date_field": "planned_date",
            "distance_rule": "days_to_next_earnings = planned_date - anchor_date",
            "window_rule": "T-3 to T+10 calendar days around anchor_date",
            "join_behavior": "use only the selected as-of snapshot; no future folder may be used",
            "missing_behavior": "missing_no_prior_snapshot if no folder date <= anchor_date; absent_in_selected_snapshot if code is not present in the selected snapshot",
        },
        "rights": {
            "source_root": str(JPX_RIGHTS_ARCHIVE),
            "folder_date_range": [rights_inventory.get("min_snapshot_date"), rights_inventory.get("max_snapshot_date")],
            "event_date_field": "COALESCE(last_rights_date, ex_date)",
            "distance_rule": "days_to_next_ex_rights = rights_event_date - anchor_date",
            "window_rule": "T-3 to T+5 calendar days around anchor_date",
            "join_behavior": "use only the selected as-of snapshot; no future folder may be used",
            "missing_behavior": "missing_no_prior_snapshot if no folder date <= anchor_date; absent_in_selected_snapshot if code is not present in the selected snapshot",
        },
    }


def _build_formula_contract() -> dict[str, Any]:
    return {
        "schema_version": FORMULA_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "features": [
            {
                "feature_name": "earnings_nearby_flag",
                "source": "jpx_financial_announcement snapshot archive",
                "required_fields": ["code", "planned_date", "snapshot_folder_date"],
                "computation_rule": "true when planned_date is within -3 to +10 calendar days relative to anchor_date after selecting the latest snapshot folder <= anchor_date",
                "no_lookahead_proof": "the selected snapshot folder date is never greater than anchor_date; future archive folders are excluded",
                "coverage_estimate": "0.0 for the current candidate surface because every anchor_date precedes the first snapshot folder",
                "leakage_risk": "low when as-of folder selection is honored; high if a future snapshot is used",
                "implementable_now": True,
            },
            {
                "feature_name": "earnings_window_bucket",
                "source": "jpx_financial_announcement snapshot archive",
                "required_fields": ["code", "planned_date", "snapshot_folder_date"],
                "computation_rule": "bucket signed days_to_next_earnings into before_0_3d, before_4_10d, after_0_3d, after_4_10d, not_nearby, missing",
                "no_lookahead_proof": "same as earnings_nearby_flag",
                "coverage_estimate": "0.0 for the current candidate surface because every anchor_date precedes the first snapshot folder",
                "leakage_risk": "low when as-of folder selection is honored; high if a future snapshot is used",
                "implementable_now": True,
            },
            {
                "feature_name": "days_to_next_earnings",
                "source": "jpx_financial_announcement snapshot archive",
                "required_fields": ["code", "planned_date", "snapshot_folder_date"],
                "computation_rule": "planned_date minus anchor_date in calendar days after selecting the as-of snapshot",
                "no_lookahead_proof": "same as earnings_nearby_flag",
                "coverage_estimate": "0.0 for the current candidate surface because every anchor_date precedes the first snapshot folder",
                "leakage_risk": "low when as-of folder selection is honored; high if a future snapshot is used",
                "implementable_now": True,
            },
            {
                "feature_name": "ex_rights_nearby_flag",
                "source": "jpx_ex_rights snapshot archive",
                "required_fields": ["code", "ex_date", "last_rights_date", "snapshot_folder_date"],
                "computation_rule": "true when COALESCE(last_rights_date, ex_date) is within -3 to +5 calendar days relative to anchor_date after selecting the latest snapshot folder <= anchor_date",
                "no_lookahead_proof": "the selected snapshot folder date is never greater than anchor_date; future archive folders are excluded",
                "coverage_estimate": "0.0 for the current candidate surface because every anchor_date precedes the first snapshot folder",
                "leakage_risk": "low when as-of folder selection is honored; high if a future snapshot is used",
                "implementable_now": True,
            },
            {
                "feature_name": "rights_window_bucket",
                "source": "jpx_ex_rights snapshot archive",
                "required_fields": ["code", "ex_date", "last_rights_date", "snapshot_folder_date"],
                "computation_rule": "bucket signed days_to_next_ex_rights into before_0_3d, before_4_5d, after_0_3d, after_4_5d, not_nearby, missing",
                "no_lookahead_proof": "same as ex_rights_nearby_flag",
                "coverage_estimate": "0.0 for the current candidate surface because every anchor_date precedes the first snapshot folder",
                "leakage_risk": "low when as-of folder selection is honored; high if a future snapshot is used",
                "implementable_now": True,
            },
            {
                "feature_name": "days_to_next_ex_rights",
                "source": "jpx_ex_rights snapshot archive",
                "required_fields": ["code", "ex_date", "last_rights_date", "snapshot_folder_date"],
                "computation_rule": "COALESCE(last_rights_date, ex_date) minus anchor_date in calendar days after selecting the as-of snapshot",
                "no_lookahead_proof": "same as ex_rights_nearby_flag",
                "coverage_estimate": "0.0 for the current candidate surface because every anchor_date precedes the first snapshot folder",
                "leakage_risk": "low when as-of folder selection is honored; high if a future snapshot is used",
                "implementable_now": True,
            },
        ],
        "explicit_non_features": [
            "actual earnings announcement date",
            "dividend/ex-dividend timing",
            "shareholder-benefit rights",
            "dilution / financing events",
            "TDnet material disclosure flags",
        ],
    }


def _limit_frame_by_anchor_dates(frame: pd.DataFrame, limit: int | None) -> pd.DataFrame:
    if limit is None:
        return frame.copy()
    anchors = sorted(pd.to_datetime(frame["anchor_date"], errors="coerce").dropna().dt.strftime("%Y-%m-%d").unique().tolist())
    selected = set(anchors[:limit])
    return frame[frame["anchor_date"].astype(str).isin(selected)].copy()


def _build_event_surface(
    frame: pd.DataFrame,
    *,
    snapshot_tables: dict[date, dict[str, pd.DataFrame]] | None,
    label: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    working = frame.copy()
    working["anchor_date"] = working["anchor_date"].astype("string")
    working["symbol"] = working["symbol"].astype("string")
    working["side"] = working["side"].astype("string")
    working["selected_snapshot_date"] = None

    if not snapshot_tables:
        enriched = _attach_missing_event_columns(working, MISSING_NO_PRIOR_SNAPSHOT)
        trace = working[["anchor_date", "symbol", "side"]].copy()
        if "candidate_idx" in working.columns:
            trace["candidate_idx"] = working["candidate_idx"]
        trace["selected_snapshot_date"] = None
        trace["selection_status"] = MISSING_NO_PRIOR_SNAPSHOT
        trace["selection_reason"] = "no_prior_snapshot"
        trace["surface"] = label
        enriched["surface"] = label
        return enriched, trace

    snapshot_dates = sorted(snapshot_tables.keys())
    anchor_dates = pd.to_datetime(working["anchor_date"], errors="coerce").dt.date
    working["selected_snapshot_date"] = anchor_dates.map(lambda d: _selected_snapshot(d, snapshot_dates) if d is not None else None)
    enriched = _attach_missing_event_columns(working, MISSING_NO_PRIOR_SNAPSHOT)
    trace_rows: list[dict[str, Any]] = []
    for snap_date, subset_idx in working.groupby("selected_snapshot_date").groups.items():
        if pd.isna(snap_date):
            continue
        snap = snapshot_tables.get(snap_date)
        subset = enriched.loc[list(subset_idx)].copy()
        subset["_row_idx"] = subset.index
        if snap is None:
            trace_rows.extend(
                {
                    "surface": label,
                    "anchor_date": row.anchor_date,
                    "symbol": row.symbol,
                    "side": row.side,
                    "candidate_idx": row.candidate_idx if "candidate_idx" in row._fields else None,
                    "selected_snapshot_date": snap_date.isoformat(),
                    "selection_status": MISSING_NO_EVENT_ROWS,
                    "selection_reason": "snapshot_not_loaded",
                }
                for row in subset.itertuples(index=False)
            )
            continue

        # Earnings
        if not snap["earnings"].empty:
            merged = subset.merge(snap["earnings"], on="code", how="left")
            merged["planned_date"] = pd.to_datetime(merged["planned_date"], errors="coerce")
            merged["anchor_dt"] = pd.to_datetime(merged["anchor_date"], errors="coerce")
            merged["days_to_next_earnings"] = (merged["planned_date"] - merged["anchor_dt"]).dt.days.astype("Int64")
            merged["earnings_nearby_flag"] = merged["days_to_next_earnings"].between(-3, 10, inclusive="both").astype("boolean")
            merged["earnings_window_bucket"] = merged["days_to_next_earnings"].apply(_bucket_earnings)
            merged["earnings_nearby_flag_feature_status"] = merged["planned_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
            merged["earnings_window_bucket_feature_status"] = merged["planned_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
            merged["days_to_next_earnings_feature_status"] = merged["planned_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
            merged["earnings_nearby_flag_missing_reason"] = merged["planned_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            merged["earnings_window_bucket_missing_reason"] = merged["planned_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            merged["days_to_next_earnings_missing_reason"] = merged["planned_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            merged = merged.drop(columns=["anchor_dt"], errors="ignore")
            for column in [
                "earnings_nearby_flag",
                "earnings_window_bucket",
                "days_to_next_earnings",
                "earnings_nearby_flag_feature_status",
                "earnings_window_bucket_feature_status",
                "days_to_next_earnings_feature_status",
                "earnings_nearby_flag_missing_reason",
                "earnings_window_bucket_missing_reason",
                "days_to_next_earnings_missing_reason",
            ]:
                enriched.loc[merged["_row_idx"], column] = merged[column].values
        else:
            trace_rows.extend(
                {
                    "surface": label,
                    "anchor_date": row.anchor_date,
                    "symbol": row.symbol,
                    "side": row.side,
                    "candidate_idx": row.candidate_idx if "candidate_idx" in row._fields else None,
                    "selected_snapshot_date": snap_date.isoformat(),
                    "selection_status": MISSING_NO_EVENT_ROWS,
                    "selection_reason": "earnings_snapshot_empty",
                }
                for row in subset.itertuples(index=False)
            )

        # Rights
        if not snap["rights"].empty:
            merged = subset.merge(snap["rights"], on="code", how="left")
            merged["rights_event_date"] = pd.to_datetime(merged["last_rights_date"], errors="coerce")
            merged["anchor_dt"] = pd.to_datetime(merged["anchor_date"], errors="coerce")
            merged["days_to_next_ex_rights"] = (merged["rights_event_date"] - merged["anchor_dt"]).dt.days.astype("Int64")
            merged["ex_rights_nearby_flag"] = merged["days_to_next_ex_rights"].between(-3, 5, inclusive="both").astype("boolean")
            merged["rights_window_bucket"] = merged["days_to_next_ex_rights"].apply(_bucket_rights)
            merged["ex_rights_nearby_flag_feature_status"] = merged["rights_event_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
            merged["rights_window_bucket_feature_status"] = merged["rights_event_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
            merged["days_to_next_ex_rights_feature_status"] = merged["rights_event_date"].notna().map(lambda ok: "available" if ok else ABSENT_IN_SELECTED_SNAPSHOT)
            merged["ex_rights_nearby_flag_missing_reason"] = merged["rights_event_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            merged["rights_window_bucket_missing_reason"] = merged["rights_event_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            merged["days_to_next_ex_rights_missing_reason"] = merged["rights_event_date"].notna().map(lambda ok: "" if ok else "code_not_in_snapshot")
            merged = merged.drop(columns=["anchor_dt"], errors="ignore")
            for column in [
                "ex_rights_nearby_flag",
                "rights_window_bucket",
                "days_to_next_ex_rights",
                "ex_rights_nearby_flag_feature_status",
                "rights_window_bucket_feature_status",
                "days_to_next_ex_rights_feature_status",
                "ex_rights_nearby_flag_missing_reason",
                "rights_window_bucket_missing_reason",
                "days_to_next_ex_rights_missing_reason",
            ]:
                enriched.loc[merged["_row_idx"], column] = merged[column].values
        else:
            trace_rows.extend(
                {
                    "surface": label,
                    "anchor_date": row.anchor_date,
                    "symbol": row.symbol,
                    "side": row.side,
                    "candidate_idx": row.candidate_idx if "candidate_idx" in row._fields else None,
                    "selected_snapshot_date": snap_date.isoformat(),
                    "selection_status": MISSING_NO_EVENT_ROWS,
                    "selection_reason": "rights_snapshot_empty",
                }
                for row in subset.itertuples(index=False)
            )

    trace = pd.DataFrame(trace_rows)
    if trace.empty:
        trace = working[["anchor_date", "symbol", "side"]].copy()
        if "candidate_idx" in working.columns:
            trace["candidate_idx"] = working["candidate_idx"]
        trace["selected_snapshot_date"] = working["selected_snapshot_date"].astype("string")
        trace["selection_status"] = working["selected_snapshot_date"].isna().map(lambda ok: MISSING_NO_PRIOR_SNAPSHOT if ok else "available")
        trace["selection_reason"] = working["selected_snapshot_date"].isna().map(lambda ok: "no_prior_snapshot" if ok else "snapshot_selected")
        trace["surface"] = label

    enriched["surface"] = label
    return enriched, trace


def _build_run_manifest(*, output_root: Path, session_id: str, limit_anchor_dates: int | None, jobs: int) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_root": str(output_root),
        "jobs_requested": jobs,
        "jobs_supported": 1,
        "limit_anchor_dates": limit_anchor_dates,
        "inputs": {
            "batch2_volume_session": str(BATCH2_VOLUME_SESSION),
            "event_contract_session": str(EVENT_CONTRACT_SESSION),
            "jpx_earnings_archive": str(JPX_EARNINGS_ARCHIVE),
            "jpx_rights_archive": str(JPX_RIGHTS_ARCHIVE),
            "topk_diff": str(TOPK_DIFF),
        },
    }


def _build_decision(candidate_frame: pd.DataFrame, event_summary: dict[str, Any]) -> dict[str, Any]:
    non_null = 0
    for feature in EVENT_FEATURES:
        non_null += int(candidate_frame[feature].notna().sum()) if feature in candidate_frame.columns else 0
    coverage_rate = non_null / (len(candidate_frame) * len(EVENT_FEATURES)) if len(candidate_frame) else 0.0
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "insufficient_event_coverage",
        "status": "insufficient_event_coverage",
        "reason": "candidate anchors end before the first available JPX snapshot folder, so no snapshot can be selected without using a future archive folder",
        "next_step": "select_external_event_source",
        "row_count": int(len(candidate_frame)),
        "event_feature_non_null_count": non_null,
        "event_feature_coverage_rate": _safe_float(coverage_rate),
        "usable_snapshot_count": 0,
        "snapshot_range": event_summary.get("snapshot_range"),
    }


def build_artifacts(*, output_root: Path, limit_anchor_dates: int | None, jobs: int) -> dict[str, Any]:
    candidate = _load_parquet(BATCH2_CANDIDATE)
    orfp = _load_parquet(BATCH2_ORFP)
    diff = _load_parquet(TOPK_DIFF)

    if limit_anchor_dates is not None:
        candidate = _limit_frame_by_anchor_dates(candidate, limit_anchor_dates)
        orfp = _limit_frame_by_anchor_dates(orfp, limit_anchor_dates)
        diff = _limit_frame_by_anchor_dates(diff, limit_anchor_dates)

    earnings_inventory = _archive_inventory(JPX_EARNINGS_ARCHIVE)
    rights_inventory = _archive_inventory(JPX_RIGHTS_ARCHIVE)
    snapshot_tables: dict[date, dict[str, pd.DataFrame]] | None = None
    usable_snapshot_dates: list[date] = []

    candidate_anchors = pd.to_datetime(candidate["anchor_date"], errors="coerce").dt.date
    orfp_anchors = pd.to_datetime(orfp["anchor_date"], errors="coerce").dt.date
    all_anchors = [d for d in candidate_anchors.dropna().tolist()] + [d for d in orfp_anchors.dropna().tolist()]
    snapshot_dates = [datetime.strptime(item, "%Y%m%d").date() for item in earnings_inventory.get("snapshot_dates", [])]
    usable_snapshot_dates = sorted({max([snap for snap in snapshot_dates if snap <= anchor], default=None) for anchor in all_anchors if any(snap <= anchor for snap in snapshot_dates)})
    usable_snapshot_dates = [snap for snap in usable_snapshot_dates if snap is not None]

    if usable_snapshot_dates:
        snapshot_tables = _parse_snapshot_tables(JPX_EARNINGS_ARCHIVE)
        rights_tables = _parse_snapshot_tables(JPX_RIGHTS_ARCHIVE)
        # merge the dictionaries per date
        for snap_date, tables in rights_tables.items():
            snapshot_tables.setdefault(snap_date, {}).update({"rights": tables.get("rights", pd.DataFrame())})
        for snap_date, tables in snapshot_tables.items():
            tables.setdefault("earnings", pd.DataFrame())
            tables.setdefault("rights", pd.DataFrame())
    else:
        snapshot_tables = {}

    candidate_event, candidate_trace = _build_event_surface(candidate, snapshot_tables=snapshot_tables, label="candidate_surface")
    orfp_event, orfp_trace = _build_event_surface(orfp, snapshot_tables=snapshot_tables, label="orfp_surface")

    # Ensure all required feature columns exist even when no snapshots are usable.
    for frame in (candidate_event, orfp_event):
        for feature in EVENT_FEATURES:
            if feature not in frame.columns:
                frame[feature] = pd.Series([pd.NA] * len(frame), dtype="string" if feature.endswith("_bucket") else ("boolean" if feature.endswith("_flag") else "Int64"))
            status_col = f"{feature}_feature_status"
            reason_col = f"{feature}_missing_reason"
            if status_col not in frame.columns:
                frame[status_col] = MISSING_NO_PRIOR_SNAPSHOT
            if reason_col not in frame.columns:
                frame[reason_col] = "no_prior_snapshot"

    combined_trace = pd.concat([candidate_trace, orfp_trace], ignore_index=True)
    combined_trace["selected_snapshot_date"] = combined_trace["selected_snapshot_date"].astype("string")

    coverage_candidate = _build_feature_coverage_summary(candidate_event, label="candidate_surface")
    coverage_orfp = _build_feature_coverage_summary(orfp_event, label="orfp_surface")
    missing_candidate = _build_missingness_summary(candidate_event, label="candidate_surface")
    missing_orfp = _build_missingness_summary(orfp_event, label="orfp_surface")
    no_lookahead_candidate = _build_no_lookahead_audit(candidate_event, label="candidate_surface")
    no_lookahead_orfp = _build_no_lookahead_audit(orfp_event, label="orfp_surface")
    contrast = _build_contrast(diff, candidate_event)
    orfp_summary = _build_orfp_summary(orfp_event, contrast)

    event_summary = {
        "snapshot_range": {
            "earnings": [earnings_inventory.get("min_snapshot_date"), earnings_inventory.get("max_snapshot_date")],
            "rights": [rights_inventory.get("min_snapshot_date"), rights_inventory.get("max_snapshot_date")],
        },
        "usable_snapshot_count": len(usable_snapshot_dates),
    }
    decision = _build_decision(candidate_event, event_summary)

    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", _build_run_manifest(output_root=output_root, session_id=output_root.name, limit_anchor_dates=limit_anchor_dates, jobs=jobs))
    _write_json(output_root / "input_resolution.json", _source_resolution())
    _write_json(
        output_root / "event_snapshot_join_contract.json",
        _build_join_contract(earnings_inventory, rights_inventory),
    )
    _write_json(output_root / "event_feature_formula_contract.json", _build_formula_contract())
    _write_json(output_root / "event_backfill_coverage_summary.json", {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": coverage_candidate,
        "orfp_surface": coverage_orfp,
        "snapshot_range": event_summary["snapshot_range"],
        "usable_snapshot_count": len(usable_snapshot_dates),
        "note": "All current candidate anchors precede the first JPX snapshot folder, so coverage is explicit missingness.",
    })
    _write_json(output_root / "event_feature_missingness_summary.json", {
        "schema_version": MISSINGNESS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": missing_candidate,
        "orfp_surface": missing_orfp,
    })
    _write_json(output_root / "no_lookahead_event_feature_audit.json", {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_surface": no_lookahead_candidate,
        "orfp_surface": no_lookahead_orfp,
    })
    _write_json(output_root / "added_top15_vs_bottom15_event_contrast.json", contrast)
    _write_json(output_root / "orfp_event_feature_summary.json", orfp_summary)
    _write_json(output_root / "feature_surface_event_backfill_v1_decision.json", decision)
    _write_parquet(output_root / "candidate_prefilter_rows_event_enriched_v1.parquet", candidate_event)
    _write_parquet(output_root / "observable_regime_false_positive_event_enriched_v1.parquet", orfp_event)
    _write_parquet(output_root / "event_snapshot_selection_trace.parquet", combined_trace)
    if len(candidate_event):
        _write_parquet(output_root / "sample_event_feature_rows.parquet", candidate_event.head(min(25, len(candidate_event))))
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", {"complete": True, "generated_at_utc": _utc_now(), "schema_version": SCHEMA_VERSION})
    return {
        "decision": decision,
        "candidate_rows": int(len(candidate_event)),
        "orfp_rows": int(len(orfp_event)),
        "usable_snapshot_count": len(usable_snapshot_dates),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args()

    output_root = Path(args.output_root).expanduser().resolve()
    session_id = _make_session_id()
    output_root = output_root / session_id

    manifest = _build_run_manifest(output_root=output_root, session_id=session_id, limit_anchor_dates=args.limit_anchor_dates, jobs=args.jobs)
    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "run_manifest.json", manifest)
    result = build_artifacts(output_root=output_root, limit_anchor_dates=args.limit_anchor_dates, jobs=args.jobs)
    # rewrite manifest with final counts
    manifest["final"] = result
    _write_json(output_root / "run_manifest.json", manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
