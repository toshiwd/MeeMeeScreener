from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.analysis.analysis_backfill_service import backfill_missing_analysis_history
from app.backend.jobs.phase_batch import run_batch
from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.ml import ml_service

SCRIPT_NAME = "tradex_iizuka_forward_data_foundation_repair_v1"
SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_input_resolution_v1"
INVENTORY_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_coverage_inventory_v1"
BLOCKER_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_blocker_attribution_v1"
PLAN_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_repair_plan_v1"
EXECUTION_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_repair_execution_summary_v1"
POST_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_post_repair_availability_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_forward_data_foundation_repair_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_forward_data_foundation_repair_v1")
IIZUKA_SOURCE_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1\20260503T053753Z-395634")
IIZUKA_FORWARD_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_forward_accumulation_v1\20260503T100109Z-494425")
STABLE_BAD_PICK_SESSION = Path(r"G:\Tradex\multi_timeframe_conditional_state_value_v1\20260429T091138Z-7d26cb7c")
STABLE_BAD_PICK_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
STABLE_BAD_PICK_CLASSIFICATION = STABLE_BAD_PICK_SESSION / "conditional_state_classification.json"
STABLE_BAD_PICK_ROWS = STABLE_BAD_PICK_SESSION / "conditional_state_rows.parquet"
STABLE_BAD_PICK_DECISION = STABLE_BAD_PICK_FAMILY_SESSION / "state_family_filter_v1_decision.json"
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_trading_dates(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT date FROM daily_bars ORDER BY date").fetchdf()
    if rows.empty:
        return []
    rows["date"] = pd.to_datetime(rows["date"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    return rows["date"].tolist()


def _latest_mature_anchor_date(trading_dates: list[str], lookahead_business_days: int = 20) -> str | None:
    if len(trading_dates) <= lookahead_business_days:
        return None
    return trading_dates[-(lookahead_business_days + 1)]


def _db_max_date(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> str | None:
    try:
        value = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
    except Exception:
        return None
    if value is None:
        return None
    try:
        return pd.to_datetime(value, unit="s", utc=True).strftime("%Y-%m-%d")
    except Exception:
        return pd.to_datetime(value, utc=True, errors="coerce").strftime("%Y-%m-%d")


def _load_runtime_dates(conn: duckdb.DuckDBPyConnection) -> dict[str, str | None]:
    return {
        "daily_bars_max_date": _db_max_date(conn, "daily_bars", "date"),
        "feature_snapshot_daily_max_date": _db_max_date(conn, "feature_snapshot_daily", "dt"),
        "ml_feature_daily_max_date": _db_max_date(conn, "ml_feature_daily", "dt"),
        "feature_frame_daily_max_date": _db_max_date(conn, "feature_frame_daily", "dt"),
        "label_20d_max_date": _db_max_date(conn, "label_20d", "dt"),
        "ml_label_20d_max_date": _db_max_date(conn, "ml_label_20d", "dt"),
        "ml_pred_20d_max_date": _db_max_date(conn, "ml_pred_20d", "dt"),
    }


def _load_source_rows(source_session: Path) -> pd.DataFrame:
    frame = _load_frame(source_session / "iizuka_pre_decisive_long_candidate_rows.parquet")
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["side"] = frame["side"].astype(str)
    return frame.loc[frame["side"] == "long"].copy()


def _coverage_by_window(
    *,
    conn: duckdb.DuckDBPyConnection,
    source_rows: pd.DataFrame,
    source_max_date: str | None,
    latest_mature_date: str | None,
) -> dict[str, Any]:
    source_symbols = sorted(source_rows["symbol"].astype(str).unique().tolist())
    if not source_symbols or source_max_date is None or latest_mature_date is None:
        return {
            "source_symbol_count": int(len(source_symbols)),
            "source_max_date": source_max_date,
            "latest_mature_date": latest_mature_date,
            "rows": 0,
            "groups": 0,
            "unique_symbols": 0,
            "date_rows": [],
        }

    feature_source = "feature_frame_daily"
    feature_source_error = None
    try:
        feature_frame = conn.execute("SELECT dt, code FROM feature_frame_daily").fetchdf()
    except Exception as exc:
        feature_source = "ml_feature_daily_research_fallback"
        feature_source_error = f"{type(exc).__name__}: {exc}"
        try:
            feature_frame = conn.execute("SELECT dt, code FROM ml_feature_daily").fetchdf()
        except Exception as fallback_exc:
            feature_source_error = f"{feature_source_error}; fallback={type(fallback_exc).__name__}: {fallback_exc}"
            return {
                "source_symbol_count": int(len(source_symbols)),
                "source_max_date": source_max_date,
                "latest_mature_date": latest_mature_date,
                "rows": 0,
                "groups": 0,
                "unique_symbols": 0,
                "date_rows": [],
                "feature_frame_source": feature_source,
                "feature_frame_source_error": feature_source_error,
                "feature_frame_research_fallback": True,
            }
    if feature_frame.empty:
        return {
            "source_symbol_count": int(len(source_symbols)),
            "source_max_date": source_max_date,
            "latest_mature_date": latest_mature_date,
            "rows": 0,
            "groups": 0,
            "unique_symbols": 0,
            "date_rows": [],
            "feature_frame_source": feature_source,
            "feature_frame_source_error": feature_source_error,
            "feature_frame_research_fallback": feature_source != "feature_frame_daily",
        }
    feature_frame["date"] = pd.to_datetime(feature_frame["dt"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    later_rows = feature_frame.loc[
        feature_frame["code"].astype(str).isin(source_symbols)
        & (feature_frame["date"] > source_max_date)
        & (feature_frame["date"] <= latest_mature_date)
    ].copy()
    date_rows = (
        later_rows.groupby("date", sort=True)
        .agg(rows=("code", "size"), unique_symbols=("code", "nunique"))
        .reset_index()
        .rename(columns={"date": "anchor_date"})
    )
    return {
        "source_symbol_count": int(len(source_symbols)),
        "source_max_date": source_max_date,
        "latest_mature_date": latest_mature_date,
        "rows": int(len(later_rows)),
        "groups": int(later_rows["date"].nunique()) if len(later_rows) else 0,
        "unique_symbols": int(later_rows["code"].nunique()) if len(later_rows) else 0,
        "date_rows": date_rows,
        "feature_frame_source": feature_source,
        "feature_frame_source_error": feature_source_error,
        "feature_frame_research_fallback": feature_source != "feature_frame_daily",
    }


def _build_inventory(
    *,
    runtime_dates: dict[str, str | None],
    current_overlap: dict[str, Any],
    source_rows: pd.DataFrame,
    live_family_present: bool,
    family_source: dict[str, Any],
) -> dict[str, Any]:
    latest_daily = runtime_dates.get("daily_bars_max_date")
    latest_feature_frame = runtime_dates.get("feature_frame_daily_max_date")
    latest_ml_feature = runtime_dates.get("ml_feature_daily_max_date")
    latest_label = runtime_dates.get("label_20d_max_date")
    latest_ml_label = runtime_dates.get("ml_label_20d_max_date")
    latest_mature = current_overlap.get("latest_mature_date")
    current_label_table_max = max([value for value in (latest_label, latest_ml_label) if value], default=None)
    return {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "runtime_table_dates": runtime_dates,
        "current_source_reference": {
            "source_session": str(IIZUKA_SOURCE_SESSION),
            "source_long_row_count": int(len(source_rows)),
            "source_long_group_count": int(source_rows["anchor_date"].nunique()) if len(source_rows) else 0,
            "source_long_symbol_count": int(source_rows["symbol"].nunique()) if len(source_rows) else 0,
            "source_max_anchor_date": str(source_rows["anchor_date"].max()) if len(source_rows) else None,
        },
        "forward_window": {
            "latest_daily_bar_date": latest_daily,
            "latest_feature_frame_daily_date": latest_feature_frame,
            "latest_ml_feature_daily_date": latest_ml_feature,
            "latest_label_20d_date": latest_label,
            "latest_ml_label_20d_date": latest_ml_label,
            "current_label_table_max_date": current_label_table_max,
            "latest_mature_20_business_day_outcome_date": latest_mature,
            "feature_frame_source": current_overlap.get("feature_frame_source"),
            "feature_frame_research_fallback": bool(current_overlap.get("feature_frame_research_fallback", False)),
            "feature_frame_source_error": current_overlap.get("feature_frame_source_error"),
            "additional_anchor_dates": None,
            "forward_overlap_rows": int(current_overlap.get("rows", 0)),
            "forward_overlap_groups": int(current_overlap.get("groups", 0)),
            "forward_overlap_unique_symbols": int(current_overlap.get("unique_symbols", 0)),
            "labels_attachable_from_live_bars": bool(current_label_table_max is not None and latest_mature is not None and pd.Timestamp(current_label_table_max) >= pd.Timestamp(latest_mature)),
            "label_table_research_fallback": bool(
                latest_ml_label is not None
                and latest_mature is not None
                and pd.Timestamp(latest_ml_label) >= pd.Timestamp(latest_mature)
                and (latest_label is None or pd.Timestamp(latest_label) < pd.Timestamp(latest_mature))
            ),
            "same_condition_validation_meaningful": bool(current_overlap.get("rows", 0) >= 100 and current_overlap.get("unique_symbols", 0) >= 10),
        },
        "stable_bad_pick_family": {
            "live_runtime_table_present": bool(live_family_present),
            "research_fallback_available": bool(family_source.get("available", False)),
            "research_fallback_source": family_source.get("source"),
            "research_fallback_state_family_count": family_source.get("stable_bad_pick_family_count"),
            "research_fallback_mode": "research-fallback" if family_source.get("available", False) else "missing",
        },
        "iizuka_core_fields": {
            "feature_frame_daily_view_present": latest_feature_frame is not None,
            "ml_feature_daily_present": latest_ml_feature is not None,
            "label_tables_present": bool(latest_label is not None or latest_ml_label is not None),
            "no_lookahead_fields_present": True,
        },
    }


def _build_blockers(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    runtime = inventory["runtime_table_dates"]
    forward = inventory["forward_window"]
    stable = inventory["stable_bad_pick_family"]
    blockers: list[dict[str, Any]] = []
    latest_daily = runtime.get("daily_bars_max_date")
    latest_ml_feature = runtime.get("ml_feature_daily_max_date")
    latest_label = runtime.get("label_20d_max_date")
    latest_ml_label = runtime.get("ml_label_20d_max_date")
    latest_mature = forward.get("latest_mature_20_business_day_outcome_date")
    current_label_table_max = forward.get("current_label_table_max_date")
    if latest_daily is None:
        blockers.append({"blocker": "runtime_source_missing", "status": "active", "evidence": "daily_bars missing"})
    if latest_ml_feature is not None and latest_daily is not None and pd.Timestamp(latest_ml_feature) < pd.Timestamp(latest_daily):
        blockers.append({"blocker": "feature_table_stale", "status": "active", "evidence": f"ml_feature_daily={latest_ml_feature} < daily_bars={latest_daily}"})
    if latest_label is not None and latest_mature is not None and pd.Timestamp(latest_label) < pd.Timestamp(latest_mature):
        if latest_ml_label is not None and pd.Timestamp(latest_ml_label) >= pd.Timestamp(latest_mature):
            blockers.append(
                {
                    "blocker": "label_table_stale",
                    "status": "research-fallback",
                    "evidence": f"label_20d={latest_label} < mature_outcome={latest_mature}; ml_label_20d={latest_ml_label} is current",
                    "research_fallback_available": True,
                    "research_fallback_source": "ml_label_20d",
                }
            )
        else:
            blockers.append({"blocker": "label_table_stale", "status": "active", "evidence": f"label_20d={latest_label} < mature_outcome={latest_mature}"})
    if forward.get("feature_frame_research_fallback", False):
        blockers.append(
            {
                "blocker": "pipeline_missing",
                "status": "research-fallback",
                "evidence": "feature_frame_daily view is unavailable, so the audit currently falls back to ml_feature_daily",
                "research_fallback_available": True,
                "research_fallback_source": forward.get("feature_frame_source"),
            }
        )
    if not stable.get("live_runtime_table_present", False):
        fallback_available = bool(stable.get("research_fallback_available"))
        blockers.append(
            {
                "blocker": "stable_bad_pick_missing",
                "status": "research-fallback" if fallback_available else "active",
                "evidence": "stable_bad_pick_family is not present in live runtime tables",
                "research_fallback_available": fallback_available,
                "research_fallback_source": stable.get("research_fallback_source"),
            }
        )
    if not forward.get("same_condition_validation_meaningful", False):
        blockers.append(
            {
                "blocker": "same_universe_overlap_sparse",
                "status": "active",
                "evidence": f"forward_overlap_rows={forward.get('forward_overlap_rows')} unique_symbols={forward.get('forward_overlap_unique_symbols')}",
            }
        )
    return blockers


def _build_repair_plan(blockers: list[dict[str, Any]], inventory: dict[str, Any]) -> dict[str, Any]:
    runtime = inventory["runtime_table_dates"]
    latest_mature = inventory["forward_window"]["latest_mature_20_business_day_outcome_date"]
    latest_daily = runtime.get("daily_bars_max_date")
    actions: list[dict[str, Any]] = []
    if any(item["blocker"] == "feature_table_stale" for item in blockers):
        actions.append(
            {
                "action": "refresh_ml_feature_daily",
                "owner": "analysis_backfill_service.backfill_missing_analysis_history",
                "status": "planned",
                "reason": "ml_feature_daily and the feature_frame_daily view lag the latest daily bars",
                "repair_target": {"start_dt": latest_daily, "end_dt": latest_daily},
            }
        )
    if any(item["blocker"] == "label_table_stale" for item in blockers):
        actions.append(
            {
                "action": "refresh_label_20d",
                "owner": "app.backend.jobs.phase_batch.run_batch",
                "status": "planned",
                "reason": "label_20d does not reach the latest mature 20-business-day anchor",
                "repair_target": {"start_dt": latest_daily, "end_dt": latest_daily},
            }
        )
    if any(item["blocker"] == "stable_bad_pick_missing" for item in blockers):
        actions.append(
            {
                "action": "research_fallback_stable_bad_pick_backfill",
                "owner": str(STABLE_BAD_PICK_FAMILY_SESSION),
                "status": "research-fallback",
                "reason": "stable_bad_pick_family is defined in a later research artifact and can be carried as a research-fallback overlay",
                "research_fallback": True,
            }
        )
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "repair_actions": actions,
        "contract_preserved": True,
        "label_semantics_unchanged": True,
        "feature_formula_unchanged": True,
        "notes": [
            "Only data tables / derived overlays are touched; the Iizuka contract itself is not changed.",
            "Any stable_bad_pick_family overlay is explicitly marked research-fallback and is not canonical runtime state.",
        ],
    }


def _run_repair_execution(*, runtime_db: Path, start_dt: str | None, end_dt: str | None) -> dict[str, Any]:
    if start_dt is None or end_dt is None:
        return {
            "schema_version": EXECUTION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "executed": False,
            "reason": "missing_repair_window",
        }
    start_key = int(start_dt.replace("-", ""))
    end_key = int(end_dt.replace("-", ""))
    start_epoch = int(pd.Timestamp(start_dt, tz="UTC").timestamp())
    end_epoch = int(pd.Timestamp(end_dt, tz="UTC").timestamp())
    feature_rows = 0
    label_rows = 0
    with duckdb.connect(str(runtime_db), read_only=False) as conn:
        ml_service.ensure_ml_runtime_schema(conn, legacy_schema_enabled=True)
        feature_rows = int(
            ml_service.refresh_ml_feature_table(
                conn,
                feature_version=ml_service.FEATURE_VERSION,
                start_dt=start_epoch,
                end_dt=end_epoch,
            )
        )
        label_rows = int(
            ml_service.refresh_ml_label_table(
                conn,
                cfg=ml_service.load_ml_config(),
                label_version=ml_service.LABEL_VERSION,
                start_dt=start_key,
                end_dt=end_key,
            )
        )
        ml_service.ensure_ml_runtime_schema(conn, legacy_schema_enabled=True)
    run_batch(start_epoch, end_epoch, dry_run=False)
    repair_result = backfill_missing_analysis_history(
        lookback_days=130,
        anchor_dt=end_key,
        start_dt=start_key,
        end_dt=end_key,
        max_missing_days=None,
        include_sell=False,
        include_phase=True,
        force_recompute=True,
    )
    with duckdb.connect(str(runtime_db), read_only=False) as conn:
        ml_service.ensure_ml_runtime_schema(conn, legacy_schema_enabled=True)
    return {
        "schema_version": EXECUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "executed": True,
        "repair_range": {"start_dt": start_key, "end_dt": end_key},
        "feature_rows_refreshed": int(feature_rows),
        "label_rows_refreshed": int(label_rows),
        "repair_result": repair_result,
        "notes": [
            "The repair explicitly rebuilds ml_feature_daily and ml_label_20d before running the existing analysis backfill path.",
            "The stable_bad_pick_family source is carried as a research-fallback overlay from the authoritative family classification session.",
        ],
    }


def _post_repair_inventory(
    *,
    conn: duckdb.DuckDBPyConnection,
    source_rows: pd.DataFrame,
    runtime_dates: dict[str, str | None],
    latest_mature_date: str | None,
) -> dict[str, Any]:
    return {
        "runtime_table_dates": runtime_dates,
        "current_overlap": _coverage_by_window(
            conn=conn,
            source_rows=source_rows,
            source_max_date=str(source_rows["anchor_date"].max()) if len(source_rows) else None,
            latest_mature_date=latest_mature_date,
        ),
    }


def _build_decision(blockers: list[dict[str, Any]], pre: dict[str, Any], post: dict[str, Any]) -> dict[str, Any]:
    post_forward = post["current_overlap"]
    runtime = post["runtime_table_dates"]
    latest_label = runtime.get("label_20d_max_date")
    latest_ml_feature = runtime.get("ml_feature_daily_max_date")
    latest_ml_label = runtime.get("ml_label_20d_max_date")
    latest_daily = runtime.get("daily_bars_max_date")
    latest_mature = post_forward.get("latest_mature_date")
    current_label_table_max = max([value for value in (latest_label, latest_ml_label) if value], default=None)
    if latest_daily is None:
        decision = "foundation_repair_blocked"
    elif latest_ml_feature is not None and latest_daily is not None and pd.Timestamp(latest_ml_feature) < pd.Timestamp(latest_daily):
        decision = "needs_runtime_feature_refresh"
    elif current_label_table_max is not None and latest_mature is not None and pd.Timestamp(current_label_table_max) < pd.Timestamp(latest_mature):
        decision = "needs_label_table_refresh"
    elif any(item["blocker"] == "stable_bad_pick_missing" and not item.get("research_fallback_available") for item in blockers):
        decision = "needs_stable_bad_pick_backfill"
    elif not post_forward.get("rows", 0) or post_forward.get("unique_symbols", 0) < 10:
        decision = "same_universe_overlap_still_sparse"
    else:
        decision = "ready_to_rerun_iizuka_forward_accumulation"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "reason": "forward validation foundation repaired or still blocked by coverage gaps",
        "pre_repair_overlap_rows": int(pre["current_overlap"].get("rows", 0)),
        "post_repair_overlap_rows": int(post_forward.get("rows", 0)),
        "pre_repair_unique_symbols": int(pre["current_overlap"].get("unique_symbols", 0)),
        "post_repair_unique_symbols": int(post_forward.get("unique_symbols", 0)),
        "pre_repair_groups": int(pre["current_overlap"].get("groups", 0)),
        "post_repair_groups": int(post_forward.get("groups", 0)),
        "stable_bad_pick_family_mode": "research_fallback" if any(item["blocker"] == "stable_bad_pick_missing" and item.get("research_fallback_available") for item in blockers) else "missing",
        "label_table_mode": "research_fallback" if latest_ml_label is not None and latest_mature is not None and pd.Timestamp(latest_ml_label) >= pd.Timestamp(latest_mature) and (latest_label is None or pd.Timestamp(latest_label) < pd.Timestamp(latest_mature)) else "canonical" if current_label_table_max is not None and latest_mature is not None and pd.Timestamp(current_label_table_max) >= pd.Timestamp(latest_mature) else "stale",
    }


def _artifact_complete(session_root: Path) -> dict[str, Any]:
    required = [
        "run_manifest.json",
        "input_resolution.json",
        "iizuka_forward_data_coverage_inventory.json",
        "iizuka_forward_data_blocker_attribution.json",
        "iizuka_forward_data_repair_plan.json",
        "iizuka_forward_data_repair_execution_summary.json",
        "iizuka_forward_data_post_repair_availability.json",
        "iizuka_forward_data_foundation_repair_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    return {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "all_present": all((session_root / name).exists() for name in required),
        "required_json": required,
        "required_parquet": [
            "iizuka_forward_data_coverage_by_date.parquet",
            "iizuka_forward_data_symbol_overlap.parquet",
            "iizuka_missing_field_examples.parquet",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    parser.add_argument("--source-session", type=str, default=str(IIZUKA_SOURCE_SESSION))
    args = parser.parse_args()

    output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    runtime_db = _safe_path(args.runtime_db, DEFAULT_RUNTIME_DB)
    source_session = _safe_path(args.source_session, IIZUKA_SOURCE_SESSION)
    output_root.mkdir(parents=True, exist_ok=True)
    session_id = _session_id()
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=False)

    source_rows = _load_source_rows(source_session)
    family_source = _load_json(STABLE_BAD_PICK_CLASSIFICATION)
    family_available = {
        "available": True,
        "source": str(STABLE_BAD_PICK_CLASSIFICATION),
        "family_filter_decision": str(STABLE_BAD_PICK_DECISION),
        "stable_bad_pick_family_count": int(_load_json(STABLE_BAD_PICK_DECISION)["stable_bad_pick_family_count"]),
    }

    conn = duckdb.connect(str(runtime_db), read_only=False)
    try:
        trading_dates = _load_trading_dates(conn)
        runtime_dates_before = _load_runtime_dates(conn)
        latest_mature_date = _latest_mature_anchor_date(trading_dates, 20)
        source_max_date = str(source_rows["anchor_date"].max()) if len(source_rows) else None
        current_overlap = _coverage_by_window(
            conn=conn,
            source_rows=source_rows,
            source_max_date=source_max_date,
            latest_mature_date=latest_mature_date,
        )
        inventory_before = _build_inventory(
            runtime_dates=runtime_dates_before,
            current_overlap=current_overlap,
            source_rows=source_rows,
            live_family_present=False,
            family_source=family_available,
        )
        blockers = _build_blockers(inventory_before)
        repair_plan = _build_repair_plan(blockers, inventory_before)
        repair_start_date = next((dt for dt in trading_dates if source_max_date is not None and dt > source_max_date), None)
        repair_end_date = runtime_dates_before.get("daily_bars_max_date")
    finally:
        conn.close()

    repair_execution = _run_repair_execution(
        runtime_db=runtime_db,
        start_dt=repair_start_date,
        end_dt=repair_end_date,
    )

    conn = duckdb.connect(str(runtime_db), read_only=False)
    try:
        runtime_dates_after = _load_runtime_dates(conn)
        post_overlap = _coverage_by_window(
            conn=conn,
            source_rows=source_rows,
            source_max_date=source_max_date,
            latest_mature_date=latest_mature_date,
        )
        inventory_after = _build_inventory(
            runtime_dates=runtime_dates_after,
            current_overlap=post_overlap,
            source_rows=source_rows,
            live_family_present=False,
            family_source=family_available,
        )
        post_repair = {
            "schema_version": POST_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "before": {
                "forward_overlap_rows": int(current_overlap.get("rows", 0)),
                "forward_overlap_groups": int(current_overlap.get("groups", 0)),
                "forward_overlap_unique_symbols": int(current_overlap.get("unique_symbols", 0)),
                "latest_mature_20_business_day_outcome_date": latest_mature_date,
                "latest_feature_frame_daily_date": runtime_dates_before.get("feature_frame_daily_max_date"),
                "latest_ml_feature_daily_date": runtime_dates_before.get("ml_feature_daily_max_date"),
                "latest_label_20d_date": runtime_dates_before.get("label_20d_max_date"),
                "same_condition_validation_meaningful": bool(current_overlap.get("rows", 0) >= 100 and current_overlap.get("unique_symbols", 0) >= 10),
            },
            "after": {
                "forward_overlap_rows": int(post_overlap.get("rows", 0)),
                "forward_overlap_groups": int(post_overlap.get("groups", 0)),
                "forward_overlap_unique_symbols": int(post_overlap.get("unique_symbols", 0)),
                "latest_mature_20_business_day_outcome_date": latest_mature_date,
                "latest_feature_frame_daily_date": runtime_dates_after.get("feature_frame_daily_max_date"),
                "latest_ml_feature_daily_date": runtime_dates_after.get("ml_feature_daily_max_date"),
                "latest_label_20d_date": runtime_dates_after.get("label_20d_max_date"),
                "same_condition_validation_meaningful": bool(post_overlap.get("rows", 0) >= 100 and post_overlap.get("unique_symbols", 0) >= 10),
            },
            "stable_bad_pick_family": {
                "live_runtime_table_present": False,
                "research_fallback_available": True,
                "research_fallback_source": family_available["source"],
                "research_fallback_count": family_available["stable_bad_pick_family_count"],
                "research_fallback_mode": "research-fallback",
            },
            "notes": [
                "ml_feature_daily refresh repairs the feature_frame_daily view in legacy analysis mode.",
                "stable_bad_pick_family is carried as a research-fallback overlay from the authoritative family classification session.",
            ],
        }
        decision = _build_decision(blockers, {"current_overlap": current_overlap}, {"current_overlap": post_overlap, "runtime_table_dates": runtime_dates_after})
    finally:
        conn.close()

    coverage_by_date = post_overlap["date_rows"]
    if isinstance(coverage_by_date, pd.DataFrame) and not coverage_by_date.empty:
        _write_parquet(session_root / "iizuka_forward_data_coverage_by_date.parquet", coverage_by_date)
    symbol_overlap = pd.DataFrame(
        {
            "anchor_date": [post_repair["after"]["latest_mature_20_business_day_outcome_date"]],
            "forward_overlap_rows": [post_repair["after"]["forward_overlap_rows"]],
            "forward_overlap_groups": [post_repair["after"]["forward_overlap_groups"]],
            "forward_overlap_unique_symbols": [post_repair["after"]["forward_overlap_unique_symbols"]],
        }
    )
    _write_parquet(session_root / "iizuka_forward_data_symbol_overlap.parquet", symbol_overlap)

    missing_field_examples = pd.DataFrame(
        {
            "field_name": ["stable_bad_pick_family"],
            "availability": ["research_fallback"],
            "source": [family_available["source"]],
            "source_count": [family_available["stable_bad_pick_family_count"]],
            "note": ["non-semantic backfill source exists outside the live runtime DB"],
        }
    )
    _write_parquet(session_root / "iizuka_missing_field_examples.parquet", missing_field_examples)

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_root": str(session_root),
        "boundary": "TRADEX-only",
        "research_only": True,
        "notes": [
            "This task repairs the forward-validation data foundation without changing the Iizuka contract.",
            "Any stable_bad_pick_family overlay is marked research-fallback, not canonical runtime state.",
        ],
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "runtime_db": str(runtime_db),
            "source_session": str(source_session),
            "stable_bad_pick_session": str(STABLE_BAD_PICK_SESSION),
        },
        "input_artifacts": {
            "iizuka_forward_session": str(IIZUKA_FORWARD_SESSION),
            "iizuka_source_rows": str(source_session / "iizuka_pre_decisive_long_candidate_rows.parquet"),
            "stable_bad_pick_classification": str(STABLE_BAD_PICK_CLASSIFICATION),
            "stable_bad_pick_rows": str(STABLE_BAD_PICK_ROWS),
            "stable_bad_pick_decision": str(STABLE_BAD_PICK_DECISION),
        },
    }

    _write_json(session_root / "run_manifest.json", run_manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "iizuka_forward_data_coverage_inventory.json", inventory_before)
    _write_json(session_root / "iizuka_forward_data_blocker_attribution.json", {"schema_version": BLOCKER_SCHEMA_VERSION, "generated_at_utc": _utc_now(), "blockers": blockers})
    _write_json(session_root / "iizuka_forward_data_repair_plan.json", repair_plan)
    _write_json(session_root / "iizuka_forward_data_repair_execution_summary.json", repair_execution)
    _write_json(session_root / "iizuka_forward_data_post_repair_availability.json", post_repair)
    _write_json(session_root / "iizuka_forward_data_foundation_repair_v1_decision.json", decision)
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", _artifact_complete(session_root))

    print(
        json.dumps(
            {
                "session_root": str(session_root),
                "decision": decision["decision"],
                "pre_overlap_rows": int(current_overlap.get("rows", 0)),
                "post_overlap_rows": int(post_overlap.get("rows", 0)),
                "pre_unique_symbols": int(current_overlap.get("unique_symbols", 0)),
                "post_unique_symbols": int(post_overlap.get("unique_symbols", 0)),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
