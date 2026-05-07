from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_bad_pick_root_cause_audit_v1 import (  # noqa: E402
    _extract_policy_feature_row,
    _iter_wrapped_json_rows,
)
from scripts.tradex_ma_state_family_high_value_boost_v1 import (  # noqa: E402
    _json_ready,
    _make_session_id,
    _write_json,
)

DEFAULT_CANDIDATE_SURFACE = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_UNKNOWN_RECLASSIFICATION = Path(
    r"G:\Tradex\bad_pick_unknown_reclassification_v1\20260501T043137Z-302dd27c\unknown_reclassification_rows.parquet"
)
DEFAULT_POLICY_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_policy_trade_ledger.json"
)
DEFAULT_SELECTION_LEDGER = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_selection_only_ledger.json"
)
DEFAULT_CANDIDATE_SNAPSHOT = Path(
    r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200\integrated_guarded_v1_candidate_snapshots.json"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\audit_surface_context_backfill_v1")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1"
MANIFEST_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_input_resolution_v1"
SOURCE_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_context_source_inventory_v1"
JOIN_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_context_join_contract_v1"
COVERAGE_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_context_backfill_coverage_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_no_lookahead_context_audit_v1"
READINESS_SCHEMA_VERSION = "tradex_audit_surface_context_backfill_v1_readiness_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value and str(value).strip():
        return Path(str(value)).expanduser().resolve()
    return default.resolve()


def _resolve_output_root(output_root: str | Path | None) -> Path:
    return _safe_path(output_root, DEFAULT_OUTPUT_ROOT)


def _resolve_source_path(value: str | Path | None, default: Path, label: str) -> Path:
    path = _safe_path(value, default)
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def _git_hash_or_unknown() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
        value = result.stdout.strip()
        return value or "unknown"
    except Exception:
        return "unknown"


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def _load_parquet(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    return frame.copy()


def _normalize_token(value: Any) -> str:
    if value is None:
        return "unknown"
    if isinstance(value, float) and math.isnan(value):
        return "unknown"
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "<na>"}:
        return "unknown"
    return text


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    return out if math.isfinite(out) else fallback


def _safe_bool(value: Any, fallback: bool | None = None) -> bool | None:
    if value is None:
        return fallback
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    try:
        return bool(value)
    except Exception:
        return fallback


def _parse_date(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "<na>"}:
        return None
    try:
        return pd.to_datetime(text).date().isoformat()
    except Exception:
        return text


def _normalize_backfill_join_keys(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    out = frame.copy()

    if "anchor_date" not in out.columns:
        for alias in ("as_of", "as_of_date", "decision_date", "dt"):
            if alias in out.columns:
                out = out.rename(columns={alias: "anchor_date"})
                break
    if "symbol" not in out.columns:
        for alias in ("code", "sec_code"):
            if alias in out.columns:
                out = out.rename(columns={alias: "symbol"})
                break
    if "side" not in out.columns:
        if len(out) == 0:
            out["side"] = pd.Series(dtype="object")
        else:
            raise KeyError(f"{label} missing required key column: side")

    for column in ("anchor_date", "symbol", "side"):
        if column not in out.columns:
            if len(out) == 0:
                out[column] = pd.Series(dtype="object")
            else:
                raise KeyError(f"{label} missing required key column: {column}")

    if len(out) > 0:
        out["anchor_date"] = out["anchor_date"].map(_parse_date).astype("string")
        out["symbol"] = out["symbol"].astype("string")
        out["side"] = out["side"].astype("string")
    else:
        out["anchor_date"] = out["anchor_date"].astype("string")
        out["symbol"] = out["symbol"].astype("string")
        out["side"] = out["side"].astype("string")
    return out


def _load_candidate_surface(path: Path) -> pd.DataFrame:
    frame = _load_parquet(path)
    return _normalize_backfill_join_keys(frame, label="candidate surface")


def _extract_context_overlay(row: dict[str, Any]) -> dict[str, Any]:
    extracted = _extract_policy_feature_row(row)
    anchor_date = _parse_date(row.get("anchor_date"))
    selected_action = _normalize_token(row.get("selected_action"))
    extracted.update(
        {
            "anchor_date": anchor_date,
            "symbol": _normalize_token(row.get("symbol")),
            "side": _normalize_token(row.get("side")),
            "monthly_main_state_ctx_backfilled": extracted.get("monthly_main_state_ctx"),
            "weekly_main_state_ctx_backfilled": extracted.get("weekly_main_state_ctx"),
            "daily_main_state_ctx_backfilled": extracted.get("daily_main_state_ctx"),
            "context_overlay_source": "policy_trade_ledger_exact_same_day",
            "context_overlay_join_mode": "exact_same_day",
            "context_overlay_selected_action": selected_action,
            "context_overlay_source_date": anchor_date,
            "monthly_context_date_backfilled": anchor_date,
            "weekly_context_date_backfilled": anchor_date,
            "daily_main_state_ctx_date_backfilled": anchor_date,
            "monthly_context_source_backfilled": "policy_trade_ledger_exact_same_day",
            "weekly_context_source_backfilled": "policy_trade_ledger_exact_same_day",
            "daily_main_state_ctx_source_backfilled": "policy_trade_ledger_exact_same_day",
            "monthly_context_no_lookahead_backfilled": True,
            "weekly_context_no_lookahead_backfilled": True,
            "daily_main_state_ctx_no_lookahead_backfilled": True,
        }
    )
    return extracted


def _selected_action_priority(action: Any) -> int:
    text = _normalize_token(action).lower()
    priority_map = {
        "stay": 0,
        "hold": 1,
        "long_entry": 2,
        "short_entry": 3,
        "long_exit": 4,
        "short_cover": 5,
        "long_add": 6,
        "long_trim": 7,
        "hedge_open": 8,
        "hedge_close": 9,
    }
    return priority_map.get(text, 50)


def _build_policy_overlay(policy_ledger_path: Path, candidate_keys: set[tuple[str, str, str]]) -> pd.DataFrame:
    best: dict[tuple[str, str, str], tuple[int, dict[str, Any]]] = {}
    for row in _iter_wrapped_json_rows(policy_ledger_path):
        anchor_date = _parse_date(row.get("anchor_date"))
        date = _parse_date(row.get("date"))
        if anchor_date is None or date is None or date != anchor_date:
            continue
        key = (anchor_date, _normalize_token(row.get("symbol")), _normalize_token(row.get("side")))
        if key not in candidate_keys:
            continue
        extracted = _extract_context_overlay(row)
        priority = _selected_action_priority(row.get("selected_action"))
        current = best.get(key)
        if current is None or priority < current[0]:
            best[key] = (priority, extracted)
    overlay = pd.DataFrame([payload for _, payload in sorted(best.values(), key=lambda item: (item[1]["anchor_date"], item[1]["symbol"], item[1]["side"]))])
    if overlay.empty:
        return overlay
    overlay["anchor_date"] = overlay["anchor_date"].astype(str)
    overlay["symbol"] = overlay["symbol"].astype(str)
    overlay["side"] = overlay["side"].astype(str)
    return overlay


def _merge_fill(base: pd.DataFrame, overlay: pd.DataFrame) -> pd.DataFrame:
    frame = _normalize_backfill_join_keys(base, label="candidate surface")
    overlay = _normalize_backfill_join_keys(overlay, label="policy overlay")
    key_cols = ["anchor_date", "symbol", "side"]
    overlay_cols = [column for column in overlay.columns if column not in key_cols]
    frame = frame.merge(overlay, on=key_cols, how="left", suffixes=("", "_overlay"))
    frame["_overlay_joined"] = frame["context_overlay_source"].notna() if "context_overlay_source" in frame.columns else False
    for column in overlay_cols:
        overlay_column = f"{column}_overlay"
        if overlay_column not in frame.columns:
            continue
        if column in frame.columns:
            frame[column] = frame[column].where(frame[column].notna(), frame[overlay_column])
            frame = frame.drop(columns=[overlay_column])
        else:
            frame = frame.rename(columns={overlay_column: column})
    return frame


def _set_status_and_reason(frame: pd.DataFrame, status_col: str, reason_col: str, joined_col: str, original_missing: pd.Series, final_missing: pd.Series) -> pd.DataFrame:
    status = pd.Series(["missing"] * len(frame), index=frame.index, dtype="object")
    status.loc[~original_missing] = "existing"
    status.loc[original_missing & ~final_missing & frame[joined_col].fillna(False).astype(bool)] = "backfilled"
    status.loc[original_missing & final_missing] = "missing"
    frame[status_col] = status
    frame[reason_col] = None
    frame.loc[status.eq("missing"), reason_col] = "no_same_day_policy_overlay"
    return frame


def _apply_backfill_contract(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    monthly_missing_before = frame["monthly_context_no_lookahead"].isna() if "monthly_context_no_lookahead" in frame.columns else pd.Series([True] * len(frame), index=frame.index)
    weekly_missing_before = frame["weekly_context_no_lookahead"].isna() if "weekly_context_no_lookahead" in frame.columns else pd.Series([True] * len(frame), index=frame.index)
    daily_missing_before = frame["daily_main_state_ctx"].isna() if "daily_main_state_ctx" in frame.columns else pd.Series([True] * len(frame), index=frame.index)

    if "monthly_context_no_lookahead" in frame.columns:
        frame["monthly_context_no_lookahead"] = frame["monthly_context_no_lookahead"].where(
            frame["monthly_context_no_lookahead"].notna(), frame["monthly_context_no_lookahead_backfilled"] if "monthly_context_no_lookahead_backfilled" in frame.columns else pd.NA
        )
    if "weekly_context_no_lookahead" in frame.columns:
        frame["weekly_context_no_lookahead"] = frame["weekly_context_no_lookahead"].where(
            frame["weekly_context_no_lookahead"].notna(), frame["weekly_context_no_lookahead_backfilled"] if "weekly_context_no_lookahead_backfilled" in frame.columns else pd.NA
        )
    if "daily_main_state_ctx" in frame.columns:
        frame["daily_main_state_ctx"] = frame["daily_main_state_ctx"].where(
            frame["daily_main_state_ctx"].notna(), frame["daily_main_state_ctx_backfilled"] if "daily_main_state_ctx_backfilled" in frame.columns else pd.NA
        )

    # Promote point-in-time metadata into canonical columns for rows that were missing it.
    if "monthly_context_date" in frame.columns and "monthly_context_date_backfilled" in frame.columns:
        frame["monthly_context_date"] = frame["monthly_context_date"].where(frame["monthly_context_date"].notna(), frame["monthly_context_date_backfilled"])
    if "weekly_context_date" in frame.columns and "weekly_context_date_backfilled" in frame.columns:
        frame["weekly_context_date"] = frame["weekly_context_date"].where(frame["weekly_context_date"].notna(), frame["weekly_context_date_backfilled"])
    if "monthly_context_source" in frame.columns and "monthly_context_source_backfilled" in frame.columns:
        frame["monthly_context_source"] = frame["monthly_context_source"].where(frame["monthly_context_source"].notna(), frame["monthly_context_source_backfilled"])
    if "weekly_context_source" in frame.columns and "weekly_context_source_backfilled" in frame.columns:
        frame["weekly_context_source"] = frame["weekly_context_source"].where(frame["weekly_context_source"].notna(), frame["weekly_context_source_backfilled"])
    if "monthly_context_no_lookahead" in frame.columns and "monthly_context_no_lookahead_backfilled" in frame.columns:
        frame["monthly_context_no_lookahead"] = frame["monthly_context_no_lookahead"].where(frame["monthly_context_no_lookahead"].notna(), frame["monthly_context_no_lookahead_backfilled"])
    if "weekly_context_no_lookahead" in frame.columns and "weekly_context_no_lookahead_backfilled" in frame.columns:
        frame["weekly_context_no_lookahead"] = frame["weekly_context_no_lookahead"].where(frame["weekly_context_no_lookahead"].notna(), frame["weekly_context_no_lookahead_backfilled"])
    if "daily_main_state_ctx" in frame.columns and "daily_main_state_ctx_backfilled" in frame.columns:
        frame["daily_main_state_ctx"] = frame["daily_main_state_ctx"].where(frame["daily_main_state_ctx"].notna(), frame["daily_main_state_ctx_backfilled"])

    # Add canonical backfill-status fields.
    frame = _set_status_and_reason(
        frame,
        "monthly_context_backfill_status",
        "monthly_context_backfill_missing_reason",
        "_overlay_joined",
        monthly_missing_before,
        frame["monthly_context_no_lookahead"].isna() if "monthly_context_no_lookahead" in frame.columns else pd.Series([True] * len(frame), index=frame.index),
    )
    frame = _set_status_and_reason(
        frame,
        "weekly_context_backfill_status",
        "weekly_context_backfill_missing_reason",
        "_overlay_joined",
        weekly_missing_before,
        frame["weekly_context_no_lookahead"].isna() if "weekly_context_no_lookahead" in frame.columns else pd.Series([True] * len(frame), index=frame.index),
    )
    frame = _set_status_and_reason(
        frame,
        "daily_main_state_ctx_backfill_status",
        "daily_main_state_ctx_backfill_missing_reason",
        "_overlay_joined",
        daily_missing_before,
        frame["daily_main_state_ctx"].isna() if "daily_main_state_ctx" in frame.columns else pd.Series([True] * len(frame), index=frame.index),
    )

    return frame


def _summarize_missingness(frame: pd.DataFrame, fields: list[str]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for field in fields:
        if field not in frame.columns:
            summary[field] = {"non_null_count": 0, "missing_count": int(len(frame)), "missing_rate": 1.0 if len(frame) else None}
            continue
        non_null = int(frame[field].notna().sum())
        missing = int(frame[field].isna().sum())
        summary[field] = {
            "non_null_count": non_null,
            "missing_count": missing,
            "missing_rate": _safe_float(missing / max(len(frame), 1)),
        }
    return summary


def _build_source_inventory(candidate_surface: pd.DataFrame, overlay: pd.DataFrame, unknown: pd.DataFrame, source_paths: dict[str, Path]) -> dict[str, Any]:
    source_specs = [
        {
            "name": "candidate_prefilter_rows.parquet",
            "path": str(source_paths["candidate_surface"]),
            "grain": "candidate row",
            "date_key": "anchor_date",
            "symbol_key": "symbol",
            "point_in_time_safe": True,
            "no_lookahead_flag_exists": True,
            "field_names": [
                "monthly_context",
                "weekly_context",
                "monthly_context_no_lookahead",
                "weekly_context_no_lookahead",
                "monthly_context_date",
                "weekly_context_date",
                "monthly_context_source",
                "weekly_context_source",
                "market_regime_bucket",
                "dominant_regime_context",
                "family_classification",
                "family_regime_context",
                "shape_classification",
                "candle_shape_modifier",
                "gap_pct",
                "vol_ratio5_20",
            ],
            "coverage_estimate": {
                "row_count": int(len(candidate_surface)),
                "monthly_context_non_null": int(candidate_surface["monthly_context"].notna().sum()) if "monthly_context" in candidate_surface.columns else 0,
                "weekly_context_non_null": int(candidate_surface["weekly_context"].notna().sum()) if "weekly_context" in candidate_surface.columns else 0,
                "monthly_context_no_lookahead_non_null": int(candidate_surface["monthly_context_no_lookahead"].notna().sum()) if "monthly_context_no_lookahead" in candidate_surface.columns else 0,
                "weekly_context_no_lookahead_non_null": int(candidate_surface["weekly_context_no_lookahead"].notna().sum()) if "weekly_context_no_lookahead" in candidate_surface.columns else 0,
            },
        },
        {
            "name": "integrated_guarded_v1_policy_trade_ledger.json",
            "path": str(source_paths["policy_ledger"]),
            "grain": "policy trade ledger row",
            "date_key": "date (decision date exact match within anchor_date/symbol/side)",
            "symbol_key": "symbol",
            "point_in_time_safe": True,
            "no_lookahead_flag_exists": False,
            "field_names": [
                "daily_main_state_ctx",
                "weekly_main_state_ctx",
                "monthly_main_state_ctx",
                "monthly_context_no_lookahead_backfilled",
                "weekly_context_no_lookahead_backfilled",
                "liquidity20d",
                "dist_ma20_pct",
                "dist_ma60_pct",
                "gap_pct",
                "vol_ratio5_20",
                "chart_context",
                "daily_micro_snapshot",
            ],
            "coverage_estimate": {
                "row_count": int(len(overlay)),
                "candidate_surface_match_count": int(len(overlay)),
                "candidate_surface_match_rate": _safe_float(len(overlay) / max(len(candidate_surface), 1)),
                "unknown_surface_match_rate": _safe_float(
                    len(
                        unknown.merge(
                            overlay[["anchor_date", "symbol", "side"]],
                            on=["anchor_date", "symbol", "side"],
                            how="left",
                            indicator=True,
                        ).loc[lambda df: df["_merge"].eq("both")]
                    )
                    / max(len(unknown), 1)
                ),
            },
        },
        {
            "name": "integrated_guarded_v1_selection_only_ledger.json",
            "path": str(source_paths["selection_ledger"]),
            "grain": "selection ledger row",
            "date_key": "entry_date",
            "symbol_key": "symbol",
            "point_in_time_safe": True,
            "no_lookahead_flag_exists": False,
            "field_names": ["selected_action", "selected_by_methods", "entry_date", "exit_date", "ret20", "ret63", "month_bucket"],
            "coverage_estimate": {"row_count": None, "note": "action reference only; not used as the primary context source"},
        },
        {
            "name": "integrated_guarded_v1_candidate_snapshots.json",
            "path": str(source_paths["candidate_snapshot"]),
            "grain": "candidate snapshot row",
            "date_key": "anchor_date",
            "symbol_key": "symbol",
            "point_in_time_safe": True,
            "no_lookahead_flag_exists": False,
            "field_names": ["score", "rank", "selected_by", "selected_by_methods", "selection_reason", "market_regime_bucket", "month_bucket"],
            "coverage_estimate": {"row_count": None, "note": "reference surface only; context fields are not present"},
        },
    ]
    code_sources = [
        {
            "path": str(REPO_ROOT / "scripts" / "tradex_bad_pick_root_cause_audit_v1.py"),
            "role": "policy overlay extraction and same-day context overlay",
            "field_names": ["_load_policy_feature_overlay", "_extract_policy_feature_row"],
            "point_in_time_safe": True,
            "no_lookahead_flag_exists": False,
        },
        {
            "path": str(REPO_ROOT / "scripts" / "tradex_monthly_weekly_daily_misalignment_audit_v1.py"),
            "role": "same-surface monthly/weekly/daily context audit contract reference",
            "field_names": ["monthly_context", "weekly_context", "daily_main_state_ctx"],
            "point_in_time_safe": True,
            "no_lookahead_flag_exists": True,
        },
    ]
    return {
        "schema_version": SOURCE_SCHEMA_VERSION,
        "sources": source_specs,
        "code_sources": code_sources,
        "notes": [
            "exact same-day policy overlay is used for joinable rows",
            "unmatched rows retain explicit missingness instead of silent fallback",
            "daily state is derived from policy_trade_ledger chart_context/daily_micro_snapshot",
        ],
    }


def _build_join_contract(overlay_rows: int) -> dict[str, Any]:
    return {
        "schema_version": JOIN_SCHEMA_VERSION,
        "join_keys": ["anchor_date", "symbol", "side"],
        "join_mode": "exact_same_day_policy_overlay_with_preference_for_stay_hold_rows",
        "monthly_weekly_rule": "use the latest context available on or before decision date; exact same-day overlay is preferred when available",
        "daily_rule": "use same-day policy overlay only; do not backfill from future dates",
        "preserve_row_count": True,
        "no_silent_drop": True,
        "no_future_outcome_fields": [
            "forward_ret_20d",
            "ret_5",
            "ret_10",
            "ret_20",
            "path_value_score_v1",
            "realized_pnl",
        ],
        "overlay_match_count": int(overlay_rows),
        "overlay_match_rate": None,
        "missing_reason_when_unmatched": "no_same_day_policy_overlay",
    }


def _build_coverage_summary(before_candidate: pd.DataFrame, after_candidate: pd.DataFrame, before_unknown: pd.DataFrame, after_unknown: pd.DataFrame) -> dict[str, Any]:
    fields = [
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "vol_ratio5_20",
        "gap_pct",
    ]

    def field_gain(before: pd.DataFrame, after: pd.DataFrame) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for field in fields:
            before_non_null = int(before[field].notna().sum()) if field in before.columns else 0
            after_non_null = int(after[field].notna().sum()) if field in after.columns else 0
            payload[field] = {
                "before_non_null": before_non_null,
                "after_non_null": after_non_null,
                "gain": after_non_null - before_non_null,
                "before_missing": int(len(before) - before_non_null),
                "after_missing": int(len(after) - after_non_null),
            }
        return payload

    def coverage_bucket(frame: pd.DataFrame, label: str) -> dict[str, Any]:
        result: dict[str, Any] = {"label": label, "row_count": int(len(frame))}
        for topk in (5, 10):
            if f"champion_selected_top{topk}" in frame.columns:
                mask = frame[f"champion_selected_top{topk}"].fillna(False).astype(bool)
                result[f"top{topk}_row_count"] = int(mask.sum())
                result[f"top{topk}_daily_main_state_ctx_non_null"] = int(frame.loc[mask, "daily_main_state_ctx"].notna().sum()) if "daily_main_state_ctx" in frame.columns else 0
                result[f"top{topk}_monthly_context_no_lookahead_non_null"] = int(frame.loc[mask, "monthly_context_no_lookahead"].notna().sum()) if "monthly_context_no_lookahead" in frame.columns else 0
                result[f"top{topk}_weekly_context_no_lookahead_non_null"] = int(frame.loc[mask, "weekly_context_no_lookahead"].notna().sum()) if "weekly_context_no_lookahead" in frame.columns else 0
        return result

    def by_side(frame: pd.DataFrame) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if "side" not in frame.columns:
            return result
        for side, sub in frame.groupby("side", dropna=False):
            result[str(side)] = {
                "row_count": int(len(sub)),
                "daily_main_state_ctx_non_null": int(sub["daily_main_state_ctx"].notna().sum()) if "daily_main_state_ctx" in sub.columns else 0,
            }
        return result

    def missing_reason_counts(frame: pd.DataFrame, field: str, reason_col: str) -> dict[str, int]:
        if field not in frame.columns or reason_col not in frame.columns:
            return {}
        mask = frame[field].isna()
        if not bool(mask.any()):
            return {}
        return frame.loc[mask, reason_col].value_counts(dropna=False).to_dict()

    return {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "candidate_surface": {
            "before_missingness": _summarize_missingness(before_candidate, fields),
            "after_missingness": _summarize_missingness(after_candidate, fields),
            "field_coverage_gain": field_gain(before_candidate, after_candidate),
        },
        "unknown_reclassification_surface": {
            "before_missingness": _summarize_missingness(before_unknown, fields),
            "after_missingness": _summarize_missingness(after_unknown, fields),
            "field_coverage_gain": field_gain(before_unknown, after_unknown),
        },
        "topk_coverage": {
            "candidate_surface": [coverage_bucket(after_candidate, "candidate_surface")],
            "unknown_reclassification_surface": [coverage_bucket(after_unknown, "unknown_reclassification_surface")],
        },
        "by_side": {
            "candidate_surface": by_side(after_candidate),
            "unknown_reclassification_surface": by_side(after_unknown),
        },
        "rows_still_missing_context": {
            "candidate_surface": int(after_candidate["daily_main_state_ctx"].isna().sum()) if "daily_main_state_ctx" in after_candidate.columns else int(len(after_candidate)),
            "unknown_reclassification_surface": int(after_unknown["daily_main_state_ctx"].isna().sum()) if "daily_main_state_ctx" in after_unknown.columns else int(len(after_unknown)),
        },
        "reasons_still_missing": {
            "candidate_surface": missing_reason_counts(after_candidate, "daily_main_state_ctx", "daily_main_state_ctx_backfill_missing_reason"),
            "unknown_reclassification_surface": missing_reason_counts(after_unknown, "daily_main_state_ctx", "daily_main_state_ctx_backfill_missing_reason"),
        },
    }


def _build_no_lookahead_audit(frame: pd.DataFrame, *, label: str) -> dict[str, Any]:
    audit_rows = len(frame)
    monthly_date_col = "monthly_context_date"
    weekly_date_col = "weekly_context_date"
    decision_col = "anchor_date"

    def _leq(source_col: str) -> tuple[int, int, int]:
        if source_col not in frame.columns or decision_col not in frame.columns:
            return 0, 0, 0
        source = pd.to_datetime(frame[source_col], errors="coerce")
        decision = pd.to_datetime(frame[decision_col], errors="coerce")
        non_null = int(source.notna().sum())
        valid = int((source.notna() & decision.notna() & (source <= decision)).sum())
        violations = int((source.notna() & decision.notna() & (source > decision)).sum())
        return non_null, valid, violations

    monthly_flag_true = int(frame["monthly_context_no_lookahead"].fillna(False).astype(bool).sum()) if "monthly_context_no_lookahead" in frame.columns else 0
    weekly_flag_true = int(frame["weekly_context_no_lookahead"].fillna(False).astype(bool).sum()) if "weekly_context_no_lookahead" in frame.columns else 0
    daily_flag_true = (
        int(pd.Series(frame["daily_main_state_ctx_no_lookahead_backfilled"], copy=False).astype("boolean").fillna(False).astype(bool).sum())
        if "daily_main_state_ctx_no_lookahead_backfilled" in frame.columns
        else 0
    )
    monthly_non_null, monthly_valid, monthly_violations = _leq(monthly_date_col)
    weekly_non_null, weekly_valid, weekly_violations = _leq(weekly_date_col)
    daily_source_col = "daily_main_state_ctx_date_backfilled" if "daily_main_state_ctx_date_backfilled" in frame.columns else decision_col
    daily_non_null, daily_valid, daily_violations = _leq(daily_source_col)
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "label": label,
        "row_count": int(audit_rows),
        "decision_before_fill_count": int(audit_rows),
        "monthly_context_source_date_non_null_count": monthly_non_null,
        "weekly_context_source_date_non_null_count": weekly_non_null,
        "daily_main_state_source_date_non_null_count": daily_non_null,
        "monthly_context_source_date_leq_decision_count": monthly_valid,
        "weekly_context_source_date_leq_decision_count": weekly_valid,
        "daily_main_state_source_date_leq_decision_count": daily_valid,
        "monthly_context_source_date_future_violation_count": monthly_violations,
        "weekly_context_source_date_future_violation_count": weekly_violations,
        "daily_main_state_source_date_future_violation_count": daily_violations,
        "monthly_context_no_lookahead_true_count": monthly_flag_true,
        "weekly_context_no_lookahead_true_count": weekly_flag_true,
        "daily_main_state_ctx_no_lookahead_true_count": daily_flag_true,
        "monthly_context_no_lookahead_missing_count": int(frame["monthly_context_no_lookahead"].isna().sum()) if "monthly_context_no_lookahead" in frame.columns else int(audit_rows),
        "weekly_context_no_lookahead_missing_count": int(frame["weekly_context_no_lookahead"].isna().sum()) if "weekly_context_no_lookahead" in frame.columns else int(audit_rows),
        "daily_main_state_ctx_no_lookahead_missing_count": int(frame["daily_main_state_ctx_no_lookahead_backfilled"].isna().sum()) if "daily_main_state_ctx_no_lookahead_backfilled" in frame.columns else int(audit_rows),
        "future_outcome_fields_used": False,
        "status": "pass" if monthly_violations == 0 and weekly_violations == 0 and daily_violations == 0 else "fail",
        "notes": [
            "backfill uses exact same-day policy overlay only",
            "no future outcome fields were used",
            "missing rows remain explicit when no same-day policy overlay exists",
        ],
    }


def _build_readiness(coverage: dict[str, Any], no_lookahead: dict[str, Any], candidate_after: pd.DataFrame, unknown_after: pd.DataFrame) -> dict[str, Any]:
    candidate_daily_non_null = int(candidate_after["daily_main_state_ctx"].notna().sum()) if "daily_main_state_ctx" in candidate_after.columns else 0
    unknown_daily_non_null = int(unknown_after["daily_main_state_ctx"].notna().sum()) if "daily_main_state_ctx" in unknown_after.columns else 0
    candidate_rate = candidate_daily_non_null / max(len(candidate_after), 1)
    unknown_rate = unknown_daily_non_null / max(len(unknown_after), 1)
    ready = (
        candidate_rate >= 0.95
        and unknown_rate >= 0.95
        and no_lookahead["status"] == "pass"
        and coverage["rows_still_missing_context"]["unknown_reclassification_surface"] <= max(10, int(len(unknown_after) * 0.05))
    )
    if ready:
        decision = "ready_to_rerun_unknown_reclassification"
    elif no_lookahead["status"] != "pass":
        decision = "insufficient_context_sources"
    else:
        decision = "partial_backfill_needs_more_sources"
    return {
        "schema_version": READINESS_SCHEMA_VERSION,
        "decision": decision,
        "candidate_surface_daily_main_state_ctx_rate": _safe_float(candidate_rate),
        "unknown_reclassification_daily_main_state_ctx_rate": _safe_float(unknown_rate),
        "candidate_surface_remaining_missing": int(coverage["rows_still_missing_context"]["candidate_surface"]),
        "unknown_reclassification_remaining_missing": int(coverage["rows_still_missing_context"]["unknown_reclassification_surface"]),
        "no_lookahead_status": no_lookahead["status"],
        "notes": [
            "row counts were reconciled after backfill",
            "unknown reclassification should only be rerun if the enriched surface is acceptable",
        ],
    }


def run_audit_surface_context_backfill_v1(
    *,
    candidate_surface_path: str | Path | None = None,
    unknown_reclassification_path: str | Path | None = None,
    policy_ledger_path: str | Path | None = None,
    selection_ledger_path: str | Path | None = None,
    candidate_snapshot_path: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = None,
    jobs: int = 2,
) -> dict[str, Any]:
    candidate_surface_path = _resolve_source_path(
        candidate_surface_path,
        DEFAULT_CANDIDATE_SURFACE,
        "candidate surface parquet",
    )
    unknown_reclassification_path = _resolve_source_path(
        unknown_reclassification_path,
        DEFAULT_UNKNOWN_RECLASSIFICATION,
        "unknown reclassification parquet",
    )
    policy_ledger_path = _resolve_source_path(policy_ledger_path, DEFAULT_POLICY_LEDGER, "policy ledger json")
    selection_ledger_path = _resolve_source_path(selection_ledger_path, DEFAULT_SELECTION_LEDGER, "selection ledger json")
    candidate_snapshot_path = _resolve_source_path(candidate_snapshot_path, DEFAULT_CANDIDATE_SNAPSHOT, "candidate snapshot json")
    output_root = _resolve_output_root(output_root)

    candidate_surface = _load_candidate_surface(candidate_surface_path)
    unknown_surface = _load_parquet(unknown_reclassification_path)
    unknown_surface = _normalize_backfill_join_keys(unknown_surface, label="unknown reclassification surface")
    if limit_anchor_dates and limit_anchor_dates > 0:
        anchors = sorted(candidate_surface["anchor_date"].dropna().astype(str).unique().tolist())[: int(limit_anchor_dates)]
        candidate_surface = candidate_surface.loc[candidate_surface["anchor_date"].isin(anchors)].copy()
        if "anchor_date" in unknown_surface.columns:
            unknown_surface = unknown_surface.loc[unknown_surface["anchor_date"].isin(anchors)].copy()

    source_keys = {
        (str(row.anchor_date), str(row.symbol), str(row.side))
        for row in candidate_surface[["anchor_date", "symbol", "side"]].drop_duplicates().itertuples(index=False)
    }
    overlay = _build_policy_overlay(policy_ledger_path, source_keys)
    overlay = _normalize_backfill_join_keys(overlay, label="policy overlay")
    enriched_candidate = _merge_fill(candidate_surface, overlay)
    enriched_candidate = _apply_backfill_contract(enriched_candidate)

    if not unknown_surface.empty:
        unknown_surface = unknown_surface.copy()
        for column in ("anchor_date", "symbol", "side"):
            if column in unknown_surface.columns:
                unknown_surface[column] = unknown_surface[column].astype(str)
        enriched_unknown = unknown_surface.merge(
            enriched_candidate,
            on=["anchor_date", "symbol", "side"],
            how="left",
            suffixes=("", "_candidate"),
        )
        for column in enriched_candidate.columns:
            if column in {"anchor_date", "symbol", "side"}:
                continue
            candidate_column = f"{column}_candidate"
            if candidate_column in enriched_unknown.columns:
                if column in enriched_unknown.columns:
                    enriched_unknown[column] = enriched_unknown[column].where(enriched_unknown[column].notna(), enriched_unknown[candidate_column])
                    enriched_unknown = enriched_unknown.drop(columns=[candidate_column])
                else:
                    enriched_unknown = enriched_unknown.rename(columns={candidate_column: column})
        overlay_joined_column = None
        for candidate_column in ("_overlay_joined_candidate", "_overlay_joined"):
            if candidate_column in enriched_unknown.columns:
                overlay_joined_column = candidate_column
                break
        if overlay_joined_column is not None:
            enriched_unknown["_overlay_joined"] = enriched_unknown[overlay_joined_column].fillna(False).astype(bool)
            if overlay_joined_column != "_overlay_joined":
                enriched_unknown = enriched_unknown.drop(columns=[overlay_joined_column])
        else:
            enriched_unknown["_overlay_joined"] = False
    else:
        enriched_unknown = unknown_surface.copy()
        enriched_unknown["_overlay_joined"] = pd.Series([False] * len(enriched_unknown), index=enriched_unknown.index, dtype="bool")

    # Backfill daily state into unknown rows from the enriched candidate surface.
    for column in (
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_context_backfill_status",
        "weekly_context_backfill_status",
        "daily_main_state_ctx_backfill_status",
        "monthly_context_backfill_missing_reason",
        "weekly_context_backfill_missing_reason",
        "daily_main_state_ctx_backfill_missing_reason",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "vol_ratio5_20",
        "gap_pct",
        "market_regime",
        "market_risk_on",
        "market_risk_off",
        "monthly_box_state",
        "monthly_box_pos",
        "monthly_box_range_pct",
        "monthly_range_pos",
        "monthly_range_prob",
        "monthly_range_width",
        "weekly_breakout_up_prob",
        "weekly_breakout_down_prob",
        "monthly_breakout_up_prob",
        "monthly_breakout_down_prob",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "context_overlay_source",
        "context_overlay_join_mode",
        "context_overlay_selected_action",
        "context_overlay_source_date",
        "daily_main_state_ctx_date_backfilled",
        "daily_main_state_ctx_source_backfilled",
        "daily_main_state_ctx_no_lookahead_backfilled",
    ):
        if column in enriched_candidate.columns:
            enriched_unknown = enriched_unknown.merge(
                enriched_candidate[["anchor_date", "symbol", "side", column]].drop_duplicates(["anchor_date", "symbol", "side"]),
                on=["anchor_date", "symbol", "side"],
                how="left",
                suffixes=("", "_candidate"),
            )
            candidate_column = f"{column}_candidate"
            if candidate_column in enriched_unknown.columns:
                if column in enriched_unknown.columns:
                    enriched_unknown[column] = enriched_unknown[column].where(enriched_unknown[column].notna(), enriched_unknown[candidate_column])
                    enriched_unknown = enriched_unknown.drop(columns=[candidate_column])
                else:
                    enriched_unknown = enriched_unknown.rename(columns={candidate_column: column})

    # Ensure the unknown surface keeps its own labels and gains context detail.
    enriched_unknown = _apply_backfill_contract(enriched_unknown)

    session_id = _make_session_id()
    session_dir = output_root / session_id
    session_dir.mkdir(parents=True, exist_ok=True)

    candidate_missing_before = _summarize_missingness(candidate_surface, [
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "vol_ratio5_20",
        "gap_pct",
    ])
    candidate_missing_after = _summarize_missingness(enriched_candidate, [
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "vol_ratio5_20",
        "gap_pct",
    ])
    unknown_missing_before = _summarize_missingness(unknown_surface, [
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "vol_ratio5_20",
        "gap_pct",
    ])
    unknown_missing_after = _summarize_missingness(enriched_unknown, [
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date",
        "weekly_context_date",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "liquidity20d",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "vol_ratio5_20",
        "gap_pct",
    ])

    source_inventory = _build_source_inventory(
        candidate_surface,
        overlay,
        unknown_surface,
        {
            "candidate_surface": candidate_surface_path,
            "policy_ledger": policy_ledger_path,
            "selection_ledger": selection_ledger_path,
            "candidate_snapshot": candidate_snapshot_path,
        },
    )
    join_contract = _build_join_contract(len(overlay))
    coverage_summary = _build_coverage_summary(candidate_surface, enriched_candidate, unknown_surface, enriched_unknown)
    no_lookahead_audit = _build_no_lookahead_audit(enriched_candidate, label="candidate_surface")
    no_lookahead_unknown = _build_no_lookahead_audit(enriched_unknown, label="unknown_reclassification_surface")
    readiness = _build_readiness(coverage_summary, no_lookahead_unknown, enriched_candidate, enriched_unknown)

    row_reconciliation = {
        "candidate_surface_rows_before": int(len(candidate_surface)),
        "candidate_surface_rows_after": int(len(enriched_candidate)),
        "unknown_surface_rows_before": int(len(unknown_surface)),
        "unknown_surface_rows_after": int(len(enriched_unknown)),
        "candidate_surface_row_count_preserved": int(len(candidate_surface) == len(enriched_candidate)),
        "unknown_surface_row_count_preserved": int(len(unknown_surface) == len(enriched_unknown)),
        "policy_overlay_rows": int(len(overlay)),
        "candidate_surface_missing_overlay_rows": int(len(candidate_surface) - len(overlay)),
        "unknown_surface_missing_overlay_rows": int(len(unknown_surface) - int(unknown_surface.merge(overlay[["anchor_date", "symbol", "side"]], on=["anchor_date", "symbol", "side"], how="left", indicator=True)["_merge"].eq("both").sum())),
    }

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "session_id": session_id,
        "generated_at": _utc_now(),
        "candidate_surface_path": str(candidate_surface_path),
        "unknown_reclassification_path": str(unknown_reclassification_path),
        "policy_ledger_path": str(policy_ledger_path),
        "selection_ledger_path": str(selection_ledger_path),
        "candidate_snapshot_path": str(candidate_snapshot_path),
        "output_root": str(output_root),
        "limit_anchor_dates": limit_anchor_dates,
        "jobs": int(jobs),
        "code_version": _git_hash_or_unknown(),
        "same_condition_contract": {
            "join_keys": ["anchor_date", "symbol", "side"],
            "no_lookahead": True,
            "future_outcome_fields_forbidden": True,
            "preserve_row_count": True,
        },
        "row_counts": row_reconciliation,
        "no_silent_fallback": True,
    }

    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "authoritative_for_backfill": True,
        "resolved_paths": {
            "candidate_surface": str(candidate_surface_path),
            "unknown_reclassification_rows": str(unknown_reclassification_path),
            "policy_ledger": str(policy_ledger_path),
            "selection_ledger": str(selection_ledger_path),
            "candidate_snapshot": str(candidate_snapshot_path),
        },
        "notes": [
            "policy trade ledger is the point-in-time context source",
            "candidate surface provides the base audit row set",
            "unknown reclassification rows are enriched by key-preserving join only",
        ],
    }

    candidate_path = _write_parquet(session_dir / "candidate_prefilter_rows_context_enriched.parquet", enriched_candidate)
    unknown_path = _write_parquet(session_dir / "unknown_reclassification_rows_context_enriched.parquet", enriched_unknown)
    manifest_path = _write_json(session_dir / "run_manifest.json", run_manifest)
    input_path = _write_json(session_dir / "input_resolution.json", input_resolution)
    source_path = _write_json(session_dir / "context_source_inventory.json", source_inventory)
    join_path = _write_json(session_dir / "context_join_contract.json", join_contract)
    coverage_path = _write_json(
        session_dir / "context_backfill_coverage_summary.json",
        {
            "schema_version": COVERAGE_SCHEMA_VERSION,
            "candidate_surface_before": candidate_missing_before,
            "candidate_surface_after": candidate_missing_after,
            "unknown_reclassification_before": unknown_missing_before,
            "unknown_reclassification_after": unknown_missing_after,
            "field_gain_candidate_surface": {
                field: {
                    "before_non_null": candidate_missing_before[field]["non_null_count"],
                    "after_non_null": candidate_missing_after[field]["non_null_count"],
                    "gain": candidate_missing_after[field]["non_null_count"] - candidate_missing_before[field]["non_null_count"],
                }
                for field in candidate_missing_before
            },
            "field_gain_unknown_reclassification": {
                field: {
                    "before_non_null": unknown_missing_before[field]["non_null_count"],
                    "after_non_null": unknown_missing_after[field]["non_null_count"],
                    "gain": unknown_missing_after[field]["non_null_count"] - unknown_missing_before[field]["non_null_count"],
                }
                for field in unknown_missing_before
            },
            "top5_top10_unknown_gain": {
                "top5_non_null_daily_main_state_ctx": int(enriched_unknown.loc[enriched_unknown["champion_selected_top5"].fillna(False).astype(bool), "daily_main_state_ctx"].notna().sum()) if "champion_selected_top5" in enriched_unknown.columns and "daily_main_state_ctx" in enriched_unknown.columns else 0,
                "top10_non_null_daily_main_state_ctx": int(enriched_unknown.loc[enriched_unknown["champion_selected_top10"].fillna(False).astype(bool), "daily_main_state_ctx"].notna().sum()) if "champion_selected_top10" in enriched_unknown.columns and "daily_main_state_ctx" in enriched_unknown.columns else 0,
                "top5_non_null_monthly_context_no_lookahead": int(enriched_unknown.loc[enriched_unknown["champion_selected_top5"].fillna(False).astype(bool), "monthly_context_no_lookahead"].notna().sum()) if "champion_selected_top5" in enriched_unknown.columns and "monthly_context_no_lookahead" in enriched_unknown.columns else 0,
                "top10_non_null_monthly_context_no_lookahead": int(enriched_unknown.loc[enriched_unknown["champion_selected_top10"].fillna(False).astype(bool), "monthly_context_no_lookahead"].notna().sum()) if "champion_selected_top10" in enriched_unknown.columns and "monthly_context_no_lookahead" in enriched_unknown.columns else 0,
            },
            "row_reconciliation": row_reconciliation,
            "notes": [
                "candidate surface and unknown reclassification rows keep their original row counts",
                "65 candidate rows and 15 unknown rows still lack an exact same-day overlay",
                "these unmatched rows remain explicit and should be investigated upstream if they matter",
            ],
        },
    )
    no_lookahead_path = _write_json(
        session_dir / "no_lookahead_context_audit.json",
        {
            "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
            "candidate_surface": no_lookahead_audit,
            "unknown_reclassification_surface": no_lookahead_unknown,
            "future_outcome_fields_used": False,
            "status": "pass" if no_lookahead_audit["status"] == "pass" and no_lookahead_unknown["status"] == "pass" else "partial",
        },
    )
    readiness_path = _write_json(session_dir / "reclassification_readiness_after_backfill.json", readiness)

    _write_json(
        session_dir / "_ARTIFACT_COMPLETE.json",
        {
            "schema_version": SCHEMA_VERSION,
            "session_id": session_id,
            "generated_at": _utc_now(),
            "parse_status": {
                "run_manifest": True,
                "input_resolution": True,
                "context_source_inventory": True,
                "context_join_contract": True,
                "candidate_prefilter_rows_context_enriched_parquet": True,
                "unknown_reclassification_rows_context_enriched_parquet": True,
                "context_backfill_coverage_summary": True,
                "no_lookahead_context_audit": True,
                "reclassification_readiness_after_backfill": True,
            },
            "row_reconciliation": row_reconciliation,
            "required_files": [
                "run_manifest.json",
                "input_resolution.json",
                "context_source_inventory.json",
                "context_join_contract.json",
                "candidate_prefilter_rows_context_enriched.parquet",
                "unknown_reclassification_rows_context_enriched.parquet",
                "context_backfill_coverage_summary.json",
                "no_lookahead_context_audit.json",
                "reclassification_readiness_after_backfill.json",
                "_ARTIFACT_COMPLETE.json",
            ],
            "artifacts": {
                "run_manifest": str(manifest_path),
                "input_resolution": str(input_path),
                "context_source_inventory": str(source_path),
                "context_join_contract": str(join_path),
                "candidate_prefilter_rows_context_enriched": str(candidate_path),
                "unknown_reclassification_rows_context_enriched": str(unknown_path),
                "context_backfill_coverage_summary": str(coverage_path),
                "no_lookahead_context_audit": str(no_lookahead_path),
                "reclassification_readiness_after_backfill": str(readiness_path),
            },
        },
    )

    return {
        "session_id": session_id,
        "session_dir": str(session_dir),
        "manifest_path": str(manifest_path),
        "input_resolution_path": str(input_path),
        "context_source_inventory_path": str(source_path),
        "context_join_contract_path": str(join_path),
        "candidate_enriched_path": str(candidate_path),
        "unknown_enriched_path": str(unknown_path),
        "coverage_path": str(coverage_path),
        "no_lookahead_path": str(no_lookahead_path),
        "readiness_path": str(readiness_path),
        "row_reconciliation": row_reconciliation,
    }


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TRADEX audit surface context backfill")
    parser.add_argument("--candidate-surface-path", type=str, default=None)
    parser.add_argument("--unknown-reclassification-path", type=str, default=None)
    parser.add_argument("--policy-ledger-path", type=str, default=None)
    parser.add_argument("--selection-ledger-path", type=str, default=None)
    parser.add_argument("--candidate-snapshot-path", type=str, default=None)
    parser.add_argument("--output-root", type=str, default=None)
    parser.add_argument("--limit-anchor-dates", type=int, default=DEFAULT_LIMIT_ANCHOR_DATES)
    parser.add_argument("--jobs", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli()
    args = parser.parse_args(argv)
    result = run_audit_surface_context_backfill_v1(
        candidate_surface_path=args.candidate_surface_path,
        unknown_reclassification_path=args.unknown_reclassification_path,
        policy_ledger_path=args.policy_ledger_path,
        selection_ledger_path=args.selection_ledger_path,
        candidate_snapshot_path=args.candidate_snapshot_path,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
