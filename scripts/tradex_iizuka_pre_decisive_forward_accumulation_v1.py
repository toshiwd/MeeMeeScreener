from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_pre_decisive_forward_accumulation_v1")
DEFAULT_SOURCE_SESSION = Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1\20260503T053753Z-395634")
STABLE_BAD_PICK_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
STABLE_BAD_PICK_DECISION = STABLE_BAD_PICK_FAMILY_SESSION / "state_family_filter_v1_decision.json"
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

SCRIPT_NAME = "tradex_iizuka_pre_decisive_forward_accumulation_v1"
SCHEMA_VERSION = "tradex_iizuka_pre_decisive_forward_accumulation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_forward_accumulation_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_forward_accumulation_v1_input_resolution_v1"
AVAILABILITY_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_forward_accumulation_v1_forward_availability_audit_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_pre_decisive_forward_accumulation_v1_decision_v1"


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
    if isinstance(value, np.generic):  # type: ignore[name-defined]
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _load_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    for column in ("anchor_date", "symbol", "side"):
        if column in frame.columns:
            frame[column] = frame[column].astype("string")
    return frame


def _load_trading_dates(conn: duckdb.DuckDBPyConnection) -> list[str]:
    bars = conn.execute("select distinct date from daily_bars order by date").fetchdf()
    if bars.empty:
        return []
    bars["date"] = pd.to_datetime(bars["date"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    return bars["date"].tolist()


def _load_runtime_tables(conn: duckdb.DuckDBPyConnection) -> dict[str, str | None]:
    def _max_date(table: str, column: str) -> str | None:
        try:
            value = conn.execute(f"select max({column}) from {table}").fetchone()[0]
        except Exception:
            return None
        if value is None:
            return None
        try:
            return pd.to_datetime(value, unit="s", utc=True).strftime("%Y-%m-%d")
        except Exception:
            return pd.to_datetime(value, utc=True, errors="coerce").strftime("%Y-%m-%d")

    return {
        "daily_bars_max_date": _max_date("daily_bars", "date"),
        "feature_frame_daily_max_date": _max_date("feature_frame_daily", "dt"),
        "ml_feature_daily_max_date": _max_date("ml_feature_daily", "dt"),
        "label_20d_max_date": _max_date("label_20d", "dt"),
        "ml_label_20d_max_date": _max_date("ml_label_20d", "dt"),
        "ml_pred_20d_max_date": _max_date("ml_pred_20d", "dt"),
        "feature_snapshot_daily_max_date": _max_date("feature_snapshot_daily", "dt"),
    }


def _latest_mature_anchor_date(trading_dates: list[str], lookahead_business_days: int = 20) -> str | None:
    if len(trading_dates) <= lookahead_business_days:
        return None
    return trading_dates[-(lookahead_business_days + 1)]


def _build_forward_availability_audit(
    *,
    source: pd.DataFrame,
    trading_dates: list[str],
    runtime_dates: dict[str, str | None],
    runtime_status: dict[str, Any],
    ranking_status: dict[str, Any],
) -> dict[str, Any]:
    source_long = source.loc[source["side"].astype(str) == "long"].copy()
    source_symbols = sorted(source_long["symbol"].astype(str).unique().tolist())
    source_max_date = source_long["anchor_date"].max() if len(source_long) else None
    latest_mature_date = _latest_mature_anchor_date(trading_dates, 20)
    additional_anchor_dates = []
    if source_max_date and latest_mature_date:
        additional_anchor_dates = [d for d in trading_dates if d > source_max_date and d <= latest_mature_date]

    feature_frame_source = "feature_frame_daily"
    feature_frame_source_error = None
    conn = duckdb.connect(str(DEFAULT_RUNTIME_DB), read_only=True)
    try:
        try:
            feature_frame = conn.execute("select dt, code from feature_frame_daily").fetchdf()
        except Exception as exc:
            feature_frame_source = "ml_feature_daily_research_fallback"
            feature_frame_source_error = f"{type(exc).__name__}: {exc}"
            feature_frame = conn.execute("select dt, code from ml_feature_daily").fetchdf()
    finally:
        conn.close()
    feature_frame["date"] = pd.to_datetime(feature_frame["dt"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    later_rows = feature_frame[
        feature_frame["code"].astype(str).isin(source_symbols)
        & feature_frame["date"].isin(additional_anchor_dates)
    ].copy()

    label_tables_max_date = max(
        [value for value in (runtime_dates.get("label_20d_max_date"), runtime_dates.get("ml_label_20d_max_date")) if value],
        default=None,
    )
    stable_bad_pick_fallback_available = STABLE_BAD_PICK_DECISION.exists()
    return {
        "schema_version": AVAILABILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "current_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "runtime_freshness": {
            "runtime_db": runtime_status,
            "rankings": ranking_status,
            "table_dates": runtime_dates,
        },
        "current_source_reference": {
            "session_root": str(DEFAULT_SOURCE_SESSION),
            "source_long_row_count": int(len(source_long)),
            "source_long_group_count": int(source_long["anchor_date"].nunique()) if len(source_long) else 0,
            "source_long_symbol_count": int(source_long["symbol"].nunique()) if len(source_long) else 0,
            "source_max_anchor_date": source_max_date,
        },
        "forward_window": {
            "latest_daily_bar_date": runtime_dates.get("daily_bars_max_date"),
            "latest_feature_snapshot_date": runtime_dates.get("feature_frame_daily_max_date"),
            "latest_mature_20_business_day_outcome_date": latest_mature_date,
            "first_additional_anchor_date": additional_anchor_dates[0] if additional_anchor_dates else None,
            "last_additional_anchor_date": additional_anchor_dates[-1] if additional_anchor_dates else None,
            "additional_anchor_dates": len(additional_anchor_dates),
            "feature_frame_source": feature_frame_source,
            "feature_frame_source_error": feature_frame_source_error,
            "feature_frame_research_fallback": feature_frame_source != "feature_frame_daily",
            "expected_candidate_rows": int(len(later_rows)),
            "expected_groups": int(later_rows["date"].nunique()) if len(later_rows) else 0,
            "expected_unique_symbols": int(later_rows["code"].nunique()) if len(later_rows) else 0,
            "same_universe_overlap_rows": int(len(later_rows)),
            "same_universe_overlap_symbol_count": int(later_rows["code"].nunique()) if len(later_rows) else 0,
            "same_universe_overlap_group_count": int(later_rows["date"].nunique()) if len(later_rows) else 0,
            "labels_attachable_from_live_bars": bool(latest_mature_date is not None),
            "labels_attachable_from_current_label_tables": bool(label_tables_max_date is not None and latest_mature_date is not None and label_tables_max_date >= latest_mature_date),
            "current_label_table_max_date": label_tables_max_date,
        },
        "contract_completion": {
            "stable_bad_pick_family_live_table_present": False,
            "stable_bad_pick_family_research_fallback": bool(stable_bad_pick_fallback_available),
            "live_family_source_missing": not bool(stable_bad_pick_fallback_available),
            "same_universe_overlap_too_sparse": bool(len(later_rows) < 100 or later_rows["code"].nunique() < 10),
            "notes": [
                "stable_bad_pick_family is not present in the live runtime tables",
                "stable_bad_pick_family is available as a research-fallback overlay from the authoritative family filter session" if stable_bad_pick_fallback_available else "stable_bad_pick_family fallback is unavailable",
                "the same-universe post-window overlap is only 74 rows across 2 symbols",
                "the accumulated sample is not large enough for same-condition validation",
            ],
        },
        "decision_hint": {
            "decision": "insufficient_forward_data",
            "status": "hold",
            "reason": "post-window dates exist, but the same-universe overlap is too sparse to support a meaningful forward accumulation bundle",
        },
    }


def _build_decision(audit: dict[str, Any]) -> dict[str, Any]:
    forward = audit["forward_window"]
    contract = audit["contract_completion"]
    if (
        not contract["same_universe_overlap_too_sparse"]
        and forward["labels_attachable_from_current_label_tables"]
        and contract.get("stable_bad_pick_family_research_fallback", False)
    ):
        return {
            "schema_version": DECISION_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "decision": "ready_to_rerun_iizuka_forward_accumulation",
            "status": "ready_to_rerun_iizuka_forward_accumulation",
            "reason": "forward foundation is sufficiently repaired for same-condition accumulation",
            "blocked_by": [],
        }
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": "insufficient_forward_data",
        "status": "hold",
        "reason": "post-window dates exist, but the same-universe overlap is too sparse to support a meaningful forward accumulation bundle",
        "blocked_by": [
            "same_universe_overlap_too_sparse" if contract["same_universe_overlap_too_sparse"] else None,
            "stable_bad_pick_family_missing_from_live_runtime_tables" if contract["live_family_source_missing"] else None,
            "current_label_tables_do_not_cover_the_latest_mature_anchor" if not forward["labels_attachable_from_current_label_tables"] else None,
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka pre-decisive forward accumulation v1")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--source-session", type=str, default=str(DEFAULT_SOURCE_SESSION))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    args = parser.parse_args()

    output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    source_session = _safe_path(args.source_session, DEFAULT_SOURCE_SESSION)
    runtime_db = _safe_path(args.runtime_db, DEFAULT_RUNTIME_DB)
    session_id = _session_id()
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=True)

    source = _load_frame(source_session / "iizuka_pre_decisive_long_candidate_rows.parquet")
    conn = duckdb.connect(str(runtime_db), read_only=True)
    try:
        trading_dates = _load_trading_dates(conn)
        runtime_dates = _load_runtime_tables(conn)
    finally:
        conn.close()

    runtime_status = get_runtime_stock_db_status()
    ranking_status = get_rankings_freshness(risk_mode="balanced")
    audit = _build_forward_availability_audit(
        source=source,
        trading_dates=trading_dates,
        runtime_dates=runtime_dates,
        runtime_status=runtime_status,
        ranking_status=ranking_status,
    )
    decision = _build_decision(audit)

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_root": str(session_root),
        "boundary": "TRADEX-only",
        "research_only": True,
        "fixed_contract": "iizuka_pre_decisive_long_candidate_v1",
        "notes": [
            "forward accumulation was stopped because the same-universe post-window sample is too sparse",
            "no threshold changes, model training, reranker retuning, MeeMee changes, production ranking changes, or publish/promotion changes were made",
        ],
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "output_root": str(output_root),
            "session_root": str(session_root),
            "source_session": str(source_session),
            "runtime_db": str(runtime_db),
        },
        "input_artifacts": {
            "iizuka_pre_decisive_long_candidate_rows.parquet": str(source_session / "iizuka_pre_decisive_long_candidate_rows.parquet"),
            "runtime_db": str(runtime_db),
        },
    }

    _write_json(session_root / "run_manifest.json", run_manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "iizuka_forward_availability_audit.json", audit)
    _write_json(session_root / "iizuka_pre_decisive_forward_accumulation_v1_decision.json", decision)
    _write_json(
        session_root / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
            "generated_at_utc": _utc_now(),
            "session_root": str(session_root),
            "all_present": all(
                (session_root / name).exists()
                for name in [
                    "run_manifest.json",
                    "input_resolution.json",
                    "iizuka_forward_availability_audit.json",
                    "iizuka_pre_decisive_forward_accumulation_v1_decision.json",
                ]
            ),
            "required_json": [
                "run_manifest.json",
                "input_resolution.json",
                "iizuka_forward_availability_audit.json",
                "iizuka_pre_decisive_forward_accumulation_v1_decision.json",
            ],
            "required_parquet": [],
        },
    )

    print(
        json.dumps(
            {
                "session_root": str(session_root),
                "decision": decision["decision"],
                "source_long_rows": int(audit["current_source_reference"]["source_long_row_count"]),
                "forward_overlap_rows": int(audit["forward_window"]["same_universe_overlap_rows"]),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
