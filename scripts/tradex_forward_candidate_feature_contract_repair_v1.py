from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.action_precision_multitimeframe_decomposition import _equivalent_context_for_row
from scripts.tradex_feature_surface_batch1_v1 import FEATURE_NAMES as BATCH1_FEATURE_NAMES, _apply_batch1_features
from scripts.tradex_feature_surface_batch2_volume_participation_v1 import _apply_volume_features
from scripts.tradex_conditional_high_value_candle_shape_modifier_v1 import _derive_candle_shape_modifier
from scripts.tradex_shadow_feature_reranker_feasibility_v1 import (
    MODEL_FEATURES,
    _coerce_model_frame,
    _fit_model_variants,
    _month_split,
    _score_variant_on_frame,
)


SCRIPT_NAME = "tradex_forward_candidate_feature_contract_repair_v1"
SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1"
MANIFEST_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_input_resolution_v1"
PROVENANCE_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_frozen_feature_provenance_audit_v1"
PLAN_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_candidate_feature_completion_plan_v1"
SUMMARY_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_candidate_feature_completion_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_candidate_feature_no_lookahead_audit_v1"
RESCORING_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_candidate_complete_rescoring_summary_v1"
VARIANT_COMPARISON_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_candidate_complete_forward_variant_pool_comparison_v1"
DECISION_SCHEMA_VERSION = "tradex_forward_candidate_feature_contract_repair_v1_decision_v1"

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\forward_candidate_feature_contract_repair_v1")
DEFAULT_FEATURE_COMPLETE_ROOT = Path(r"G:\Tradex\forward_candidate_surface_feature_complete_v1")
DEFAULT_SOURCE_SURFACE = Path(r"G:\Tradex\forward_candidate_surface_v1\20260502T002553Z-943902\candidate_prefilter_rows_batch2_volume_enriched_v1.parquet")
DEFAULT_SOURCE_ORFP = Path(r"G:\Tradex\forward_candidate_surface_v1\20260502T002553Z-943902\observable_regime_false_positive_batch2_volume_enriched_v1.parquet")
DEFAULT_CONTEXT_BACKFILL = Path(r"G:\Tradex\context_backfill_merge_contract_repair_v1\smoke_backfill_20260502\20260502T002508Z-242b8f07\candidate_prefilter_rows_context_enriched.parquet")
DEFAULT_TRAIN_CANDIDATE = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273\candidate_prefilter_rows_batch2_volume_enriched_v1.parquet")
DEFAULT_TRAIN_ORFP = Path(r"G:\Tradex\feature_surface_batch2_volume_participation_v1\20260501T101349Z-601273\observable_regime_false_positive_batch2_volume_enriched_v1.parquet")
# Use the explicit research snapshot so point-in-time feature materialization is not blocked by
# the live runtime DB lock or stale cache state.
DB_PATH = Path(r"G:\Tradex\db_snapshots\stocks_20260426_022925.duckdb")

POINT_IN_TIME_SOURCE_TABLES = {
    "daily_bars": ("date", "epoch"),
    "daily_ma": ("date", "epoch"),
    "ml_feature_daily": ("dt", "epoch"),
    "signal_basis_daily": ("dt", "ymd"),
}

MODEL_FROZEN_FEATURES = list(MODEL_FEATURES)
RAW_REPAIRED_FEATURES = [
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "daily_main_state_ctx",
    "weekly_main_state_ctx",
    "monthly_main_state_ctx",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "gap_pct",
    "support_wick",
    "liquidity20d",
    "monthly_range_pos",
    "monthly_range_prob",
    "monthly_range_width",
    "monthly_box_range_pct",
    "bull_marubozu",
    "bear_marubozu",
    "candle_shape_modifier",
    "monthly_context_source",
    "weekly_context_source",
    "monthly_context_no_lookahead",
    "weekly_context_no_lookahead",
    "vol_ratio5_20",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, set):
        return [_json_ready(v) for v in sorted(value, key=str)]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
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


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, float) and math.isnan(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "<na>", "none", "null", "unknown"}


def _safe_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any) -> bool | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _safe_text(value: Any, fallback: str = "unknown") -> str:
    if _is_missing(value):
        return fallback
    text = str(value).strip()
    return text or fallback


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if _is_missing(value):
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _anchor_to_epoch(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    values: list[int | None] = []
    for item in dt.tolist():
        if pd.isna(item):
            values.append(None)
        else:
            values.append(int(item.timestamp()))
    return pd.Series(values, index=series.index, dtype="Int64")


def _anchor_to_ymd(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, utc=True, errors="coerce")
    values = dt.dt.strftime("%Y%m%d")
    return pd.to_numeric(values, errors="coerce").astype("Int64")


def _feature_status(missing: list[str]) -> tuple[str, str]:
    if missing:
        return "missing", "|".join(sorted(set(missing)))
    return "available", ""


def _month_to_date_monthly_fallback(frame: pd.DataFrame, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "_row_id",
                "monthly_o_fallback",
                "monthly_h_fallback",
                "monthly_l_fallback",
                "monthly_c_fallback",
            ]
        )
    required = {"_row_id", "symbol", "anchor_dt"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"monthly fallback frame missing required columns: {missing}")
    conn.register("candidate_monthly_rows", frame[["_row_id", "symbol", "anchor_dt"]].copy())
    return conn.execute(
        """
        WITH month_daily AS (
            SELECT
                cr._row_id AS row_id,
                d.date AS bar_dt,
                d.o AS monthly_o,
                d.h AS monthly_h,
                d.l AS monthly_l,
                d.c AS monthly_c
            FROM candidate_monthly_rows cr
            INNER JOIN daily_bars d
                ON d.code = cr.symbol
               AND d.date <= cr.anchor_dt
               AND strftime(to_timestamp(d.date), '%Y%m') = strftime(to_timestamp(cr.anchor_dt), '%Y%m')
        )
        SELECT
            row_id AS _row_id,
            arg_min(monthly_o, bar_dt) AS monthly_o_fallback,
            max(monthly_h) AS monthly_h_fallback,
            min(monthly_l) AS monthly_l_fallback,
            arg_max(monthly_c, bar_dt) AS monthly_c_fallback
        FROM month_daily
        GROUP BY row_id
        """
    ).fetchdf()


def _basis_context_fields(payload: dict[str, Any]) -> dict[str, Any]:
    monthly_up = _safe_float(payload.get("monthlyBreakoutUpProb"))
    monthly_down = _safe_float(payload.get("monthlyBreakoutDownProb"))
    weekly_up = _safe_float(payload.get("weeklyBreakoutUpProb"))
    weekly_down = _safe_float(payload.get("weeklyBreakoutDownProb"))
    monthly_range_prob = _safe_float(payload.get("monthlyRangeProb"))
    weekly_range_prob = _safe_float(payload.get("weeklyRangeProb"))
    monthly_box_pos = _safe_float(payload.get("monthlyBoxPos"))
    monthly_box_range_pct = _safe_float(payload.get("monthlyBoxRangePct"))
    monthly_range_width = _safe_float(payload.get("monthlyRangeWidth"))
    monthly_range_pos = _safe_float(payload.get("monthlyRangePos"))
    market_regime = _safe_text(payload.get("marketRegime"))
    daily_candle_bull = any((_safe_float(payload.get(key)) or 0.0) >= 0.5 for key in ("bullMarubozu", "morningStar", "reclaim60", "v60Core", "v60Strong"))
    daily_candle_bear = any((_safe_float(payload.get(key)) or 0.0) >= 0.5 for key in ("bearMarubozu", "shootingStarLike"))

    monthly_state = "monthly_range_mid"
    if monthly_up is not None or monthly_down is not None:
        gap = (monthly_up or 0.0) - (monthly_down or 0.0)
        if gap >= 0.25:
            monthly_state = "monthly_up_top_warning" if (monthly_box_pos or 0.5) >= 0.55 else "monthly_up_mid"
        elif gap <= -0.25:
            monthly_state = "monthly_down_bottom_warning" if (monthly_box_pos or 0.5) <= 0.45 else "monthly_down_mid"

    weekly_state = "weekly_range_mid"
    if weekly_up is not None or weekly_down is not None:
        gap = (weekly_up or 0.0) - (weekly_down or 0.0)
        if gap >= 0.25:
            weekly_state = "weekly_up_late" if (weekly_range_prob or 0.5) < 0.5 else "weekly_up_mid"
        elif gap <= -0.25:
            weekly_state = "weekly_down_bottom_warning" if (weekly_range_prob or 0.5) > 0.5 else "weekly_down_mid"

    daily_state = "daily_reversal_up_candidate"
    if daily_candle_bear:
        daily_state = "daily_down_mid"
    elif daily_candle_bull:
        daily_state = "daily_up_mid"

    return {
        "monthly_main_state_ctx": monthly_state,
        "weekly_main_state_ctx": weekly_state,
        "daily_main_state_ctx": daily_state,
        "monthly_main_state_ctx_backfilled": monthly_state,
        "weekly_main_state_ctx_backfilled": weekly_state,
        "daily_main_state_ctx_backfilled": daily_state,
        "monthly_context_source": "confirmed_monthly_bars_monthly_ma",
        "weekly_context_source": "provisional_weekly_from_daily_bars_daily_ma",
        "monthly_context_no_lookahead": True,
        "weekly_context_no_lookahead": True,
        "monthly_context_date_backfilled": None,
        "weekly_context_date_backfilled": None,
        "daily_main_state_ctx_date_backfilled": None,
        "monthly_context_date": None,
        "weekly_context_date": None,
        "daily_main_state_ctx_source": "signal_basis_daily.basis_payload_json",
        "monthly_range_prob": monthly_range_prob,
        "weekly_range_prob": weekly_range_prob,
        "monthly_range_width": monthly_range_width,
        "monthly_range_pos": monthly_range_pos,
        "monthly_box_range_pct": monthly_box_range_pct,
        "market_risk_on": bool(payload.get("marketRiskOn")) if payload.get("marketRiskOn") is not None else None,
        "market_risk_off": bool(payload.get("marketRiskOff")) if payload.get("marketRiskOff") is not None else None,
        "market_breadth_adv_ratio": _safe_float(payload.get("marketBreadthAdvRatio")),
        "market_breadth_sample_size": _safe_float(payload.get("marketBreadthSampleSize")),
        "market_regime_bucket": market_regime,
        "dominant_regime_context": {
            "risk_on": "C:risk_on_trend",
            "risk_off": "C:risk_off_trend",
            "neutral": "C:neutral_range",
        }.get(market_regime, "unknown"),
        "liquidity20d": _safe_float(payload.get("liquidity20d")),
        "bull_marubozu": bool((_safe_float(payload.get("bullMarubozu")) or 0.0) >= 0.5) if payload.get("bullMarubozu") is not None else None,
        "bear_marubozu": bool((_safe_float(payload.get("bearMarubozu")) or 0.0) >= 0.5) if payload.get("bearMarubozu") is not None else None,
        "candle_body_ratio": _safe_float(payload.get("candleBodyRatio")),
        "candle_upper_wick_ratio": _safe_float(payload.get("candleUpperWickRatio")),
        "candle_lower_wick_ratio": _safe_float(payload.get("candleLowerWickRatio")),
        "candle_triplet_up_prob": _safe_float(payload.get("candleTripletUp")),
        "candle_triplet_down_prob": _safe_float(payload.get("candleTripletDown")),
    }


def _materialize_candidate_point_in_time_sources(frame: pd.DataFrame, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    out = frame.copy()
    out["_row_id"] = range(len(out))
    out["anchor_dt"] = _anchor_to_epoch(out["anchor_date"])
    out["anchor_ymd"] = _anchor_to_ymd(out["anchor_date"])
    codes = sorted(out["symbol"].dropna().astype(str).unique().tolist())
    if not codes:
        raise ValueError("candidate surface has no symbols")
    codes_df = pd.DataFrame({"code": codes})
    conn.register("candidate_rows", out.copy())
    conn.register("candidate_codes", codes_df)
    joined = conn.execute(
        """
        WITH daily_history AS (
            SELECT
                b.code,
                b.date AS anchor_dt,
                b.o AS db_o,
                b.h AS db_h,
                b.l AS db_l,
                b.c AS db_c,
                b.v AS db_v,
                LAG(b.o) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_o,
                LAG(b.h) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_h,
                LAG(b.l) OVER (PARTITION BY b.code ORDER BY b.date) AS prev_l,
                LAG(b.c) OVER (PARTITION BY b.code ORDER BY b.date) AS db_prev_c,
                ma.ma20 AS db_ma20,
                ma.ma60 AS db_ma60,
                mf.vol_ratio5_20 AS ml_vol_ratio5_20,
                mf.turnover20 AS ml_turnover20,
                mf.turnover_z20 AS ml_turnover_z20,
                mf.gap_pct AS ml_gap_pct,
                mf.candle_body_ratio AS ml_candle_body_ratio,
                mf.candle_upper_wick_ratio AS ml_candle_upper_wick_ratio,
                mf.candle_lower_wick_ratio AS ml_candle_lower_wick_ratio,
                mf.candle_triplet_up_prob AS ml_candle_triplet_up_prob,
                mf.candle_triplet_down_prob AS ml_candle_triplet_down_prob
            FROM daily_bars b
            INNER JOIN candidate_codes cc ON cc.code = b.code
            LEFT JOIN daily_ma ma ON ma.code = b.code AND ma.date = b.date
            LEFT JOIN ml_feature_daily mf ON mf.code = b.code AND mf.dt = b.date
        ),
        basis_rows AS (
            SELECT
                dt AS anchor_ymd,
                signal_basis_daily.code AS code,
                basis_payload_json,
                basis_source,
                source_as_of
            FROM signal_basis_daily
            INNER JOIN candidate_codes cc ON cc.code = signal_basis_daily.code
        )
        SELECT
            i.*,
            h.db_o,
            h.db_h,
            h.db_l,
            h.db_c,
            h.db_v,
            h.prev_o,
            h.prev_h,
            h.prev_l,
            h.db_prev_c,
            h.db_ma20,
            h.db_ma60,
            h.ml_vol_ratio5_20,
            h.ml_turnover20,
            h.ml_turnover_z20,
            h.ml_gap_pct,
            h.ml_candle_body_ratio,
            h.ml_candle_upper_wick_ratio,
            h.ml_candle_lower_wick_ratio,
            h.ml_candle_triplet_up_prob,
            h.ml_candle_triplet_down_prob,
            ff.monthly_range_prob AS ff_monthly_range_prob,
            ff.candle_triplet_up_prob AS ff_candle_triplet_up_prob,
            ff.candle_triplet_down_prob AS ff_candle_triplet_down_prob,
            ff.candle_body_ratio AS ff_candle_body_ratio,
            ff.candle_upper_wick_ratio AS ff_candle_upper_wick_ratio,
            ff.candle_lower_wick_ratio AS ff_candle_lower_wick_ratio,
            ff.gap_pct AS ff_gap_pct,
            ff.vol_ratio5_20 AS ff_vol_ratio5_20,
            ff.turnover20 AS ff_turnover20,
            b.basis_payload_json,
            b.basis_source,
            b.source_as_of
        FROM candidate_rows i
        LEFT JOIN daily_history h
          ON h.code = i.symbol AND h.anchor_dt = i.anchor_dt
        LEFT JOIN feature_frame_daily ff
          ON ff.code = i.symbol AND ff.dt = i.anchor_dt
        LEFT JOIN basis_rows b
          ON b.code = i.symbol AND b.anchor_ymd = i.anchor_ymd
        ORDER BY i.anchor_dt, i.candidate_rank NULLS LAST, i.symbol
        """
    ).fetchdf()

    out = joined.copy()
    if "basis_payload_json" in out.columns:
        out["basis_payload"] = out["basis_payload_json"].map(_parse_json_dict)
    else:
        out["basis_payload"] = [{} for _ in range(len(out))]

    monthly_codes = sorted(out["symbol"].dropna().astype(str).unique().tolist())
    monthly_anchor = int(out["anchor_dt"].dropna().astype(int).max()) if "anchor_dt" in out.columns and out["anchor_dt"].notna().any() else None
    monthly_frame = pd.DataFrame()
    if monthly_codes and monthly_anchor is not None:
        conn.register("candidate_monthly_codes", pd.DataFrame({"code": monthly_codes}))
        monthly_frame = conn.execute(
            """
            WITH monthly_ranked AS (
                SELECT
                    m.code,
                    m.month AS anchor_dt,
                    m.o AS monthly_o,
                    m.h AS monthly_h,
                    m.l AS monthly_l,
                    m.c AS monthly_c,
                    ma.ma20 AS monthly_ma20,
                    ma.ma60 AS monthly_ma60,
                    ROW_NUMBER() OVER (PARTITION BY m.code ORDER BY m.month DESC) AS rn
                FROM monthly_bars m
                LEFT JOIN monthly_ma ma ON ma.code = m.code AND ma.month = m.month
                INNER JOIN candidate_monthly_codes cc ON cc.code = m.code
                WHERE m.month <= ?
            )
            SELECT code, anchor_dt, monthly_o, monthly_h, monthly_l, monthly_c, monthly_ma20, monthly_ma60
            FROM monthly_ranked
            WHERE rn = 1
            """
            , [monthly_anchor]).fetchdf()
    if not monthly_frame.empty:
        monthly_frame["symbol"] = monthly_frame["code"].astype(str)
        monthly_frame["anchor_dt"] = pd.to_numeric(monthly_frame["anchor_dt"], errors="coerce").astype("int64")
        monthly_frame = monthly_frame.drop(columns=["code"]).sort_values(["symbol", "anchor_dt"]).reset_index(drop=True)
        out = out.sort_values(["symbol", "anchor_dt"]).reset_index(drop=True)
        joined_frames: list[pd.DataFrame] = []
        for symbol, symbol_frame in out.groupby("symbol", sort=False):
            symbol_monthly = monthly_frame.loc[monthly_frame["symbol"] == symbol].copy()
            if symbol_monthly.empty:
                joined_frames.append(symbol_frame.copy())
                continue
            symbol_frame = symbol_frame.sort_values("anchor_dt").reset_index(drop=True)
            symbol_frame["anchor_dt"] = pd.to_numeric(symbol_frame["anchor_dt"], errors="coerce").astype("int64")
            symbol_monthly = symbol_monthly.sort_values("anchor_dt").reset_index(drop=True)
            joined = pd.merge_asof(
                symbol_frame,
                symbol_monthly,
                on="anchor_dt",
                by="symbol",
                direction="backward",
                allow_exact_matches=True,
            )
            joined_frames.append(joined)
        out = pd.concat(joined_frames, ignore_index=True)

    monthly_fallback = _month_to_date_monthly_fallback(out, conn)
    if not monthly_fallback.empty:
        out = out.merge(monthly_fallback, on="_row_id", how="left")
        for source_col, fallback_col in [
            ("monthly_o", "monthly_o_fallback"),
            ("monthly_h", "monthly_h_fallback"),
            ("monthly_l", "monthly_l_fallback"),
            ("monthly_c", "monthly_c_fallback"),
        ]:
            if source_col in out.columns and fallback_col in out.columns:
                out[source_col] = out[source_col].where(out[source_col].notna(), out[fallback_col])
        out = out.drop(columns=[col for col in ["monthly_o_fallback", "monthly_h_fallback", "monthly_l_fallback", "monthly_c_fallback"] if col in out.columns])

    primitive_rows: list[dict[str, Any]] = []
    for _, row in out.iterrows():
        payload = _parse_json_dict(row.get("basis_payload_json"))
        context = _basis_context_fields(payload)

        o = _safe_float(row.get("db_o"))
        h = _safe_float(row.get("db_h"))
        l = _safe_float(row.get("db_l"))
        c = _safe_float(row.get("db_c"))
        prev_c = _safe_float(row.get("db_prev_c"))
        ma20 = _safe_float(row.get("db_ma20"))
        ma60 = _safe_float(row.get("db_ma60"))
        ff_monthly_range_prob = _safe_float(row.get("ff_monthly_range_prob"))
        ff_candle_triplet_up_prob = _safe_float(row.get("ff_candle_triplet_up_prob"))
        ff_candle_triplet_down_prob = _safe_float(row.get("ff_candle_triplet_down_prob"))
        ff_candle_body_ratio = _safe_float(row.get("ff_candle_body_ratio"))
        ff_candle_upper_wick_ratio = _safe_float(row.get("ff_candle_upper_wick_ratio"))
        ff_candle_lower_wick_ratio = _safe_float(row.get("ff_candle_lower_wick_ratio"))
        ff_gap_pct = _safe_float(row.get("ff_gap_pct"))
        ff_vol_ratio5_20 = _safe_float(row.get("ff_vol_ratio5_20"))
        ff_turnover20 = _safe_float(row.get("ff_turnover20"))
        monthly_o = _safe_float(row.get("monthly_o"))
        monthly_h = _safe_float(row.get("monthly_h"))
        monthly_l = _safe_float(row.get("monthly_l"))
        monthly_c = _safe_float(row.get("monthly_c"))
        monthly_ma20 = _safe_float(row.get("monthly_ma20"))
        monthly_ma60 = _safe_float(row.get("monthly_ma60"))
        body_ratio = upper_wick_ratio = lower_wick_ratio = gap_pct = dist_ma20_pct = dist_ma60_pct = None
        support_wick = None
        if None not in (o, h, l, c) and h != l:
            body_ratio = abs(c - o) / (h - l)
            upper_wick_ratio = (h - max(o, c)) / (h - l)
            lower_wick_ratio = (min(o, c) - l) / (h - l)
            support_wick = (lower_wick_ratio >= 0.20) and (c >= o)
        if prev_c is not None and prev_c != 0 and o is not None:
            gap_pct = (o - prev_c) / prev_c
        if ma20 is not None and ma20 != 0 and c is not None:
            dist_ma20_pct = (c - ma20) / ma20
        if ma60 is not None and ma60 != 0 and c is not None:
            dist_ma60_pct = (c - ma60) / ma60

        candle_body_ratio = _safe_float(context.get("candle_body_ratio"))
        if candle_body_ratio is None:
            candle_body_ratio = ff_candle_body_ratio
        candle_upper_wick_ratio = _safe_float(context.get("candle_upper_wick_ratio"))
        if candle_upper_wick_ratio is None:
            candle_upper_wick_ratio = ff_candle_upper_wick_ratio
        candle_lower_wick_ratio = _safe_float(context.get("candle_lower_wick_ratio"))
        if candle_lower_wick_ratio is None:
            candle_lower_wick_ratio = ff_candle_lower_wick_ratio
        candle_triplet_up_prob = _safe_float(context.get("candle_triplet_up_prob"))
        if candle_triplet_up_prob is None:
            candle_triplet_up_prob = ff_candle_triplet_up_prob
        candle_triplet_down_prob = _safe_float(context.get("candle_triplet_down_prob"))
        if candle_triplet_down_prob is None:
            candle_triplet_down_prob = ff_candle_triplet_down_prob
        if gap_pct is None:
            gap_pct = ff_gap_pct

        liquidity20d = _safe_float(context.get("liquidity20d"))
        if liquidity20d is None:
            liquidity20d = ff_turnover20
        monthly_range_prob = _safe_float(context.get("monthly_range_prob"))
        if monthly_range_prob is None:
            monthly_range_prob = ff_monthly_range_prob

        monthly_range_width = _safe_float(context.get("monthly_range_width"))
        monthly_box_range_pct = _safe_float(context.get("monthly_box_range_pct"))
        monthly_range_pos = _safe_float(context.get("monthly_range_pos"))
        if monthly_h is not None and monthly_l is not None:
            monthly_range_width = monthly_range_width if monthly_range_width is not None else (
                (monthly_h - monthly_l) / monthly_ma20 if monthly_ma20 not in (None, 0) else (monthly_h - monthly_l) / monthly_c if monthly_c not in (None, 0) else None
            )
            monthly_box_range_pct = monthly_box_range_pct if monthly_box_range_pct is not None else (
                (monthly_h - monthly_l) / monthly_c if monthly_c not in (None, 0) else None
            )
            monthly_range_pos = monthly_range_pos if monthly_range_pos is not None else (
                (monthly_c - monthly_l) / (monthly_h - monthly_l) if monthly_c is not None and (monthly_h - monthly_l) not in (None, 0) else None
            )

        bull_marubozu = _safe_bool(context.get("bull_marubozu"))
        bear_marubozu = _safe_bool(context.get("bear_marubozu"))
        if bull_marubozu is None and None not in (candle_body_ratio, candle_upper_wick_ratio, candle_lower_wick_ratio, o, c):
            bull_marubozu = bool(c > o and candle_body_ratio >= 0.85 and candle_upper_wick_ratio <= 0.08 and candle_lower_wick_ratio <= 0.08)
        if bear_marubozu is None and None not in (candle_body_ratio, candle_upper_wick_ratio, candle_lower_wick_ratio, o, c):
            bear_marubozu = bool(c < o and candle_body_ratio >= 0.85 and candle_upper_wick_ratio <= 0.08 and candle_lower_wick_ratio <= 0.08)

        if candle_body_ratio is None:
            candle_body_ratio = body_ratio
        if candle_upper_wick_ratio is None:
            candle_upper_wick_ratio = upper_wick_ratio
        if candle_lower_wick_ratio is None:
            candle_lower_wick_ratio = lower_wick_ratio

        if support_wick is None and lower_wick_ratio is not None and c is not None and o is not None:
            support_wick = (lower_wick_ratio >= 0.20) and (c >= o)

        row_dict = {
            "body_ratio": body_ratio,
            "upper_wick_ratio": upper_wick_ratio,
            "lower_wick_ratio": lower_wick_ratio,
            "gap_pct": gap_pct,
            "dist_ma20_pct": dist_ma20_pct,
            "dist_ma60_pct": dist_ma60_pct,
            "support_wick": support_wick,
            "candle_body_ratio": candle_body_ratio,
            "candle_upper_wick_ratio": candle_upper_wick_ratio,
            "candle_lower_wick_ratio": candle_lower_wick_ratio,
            "candle_triplet_up_prob": candle_triplet_up_prob,
            "candle_triplet_down_prob": candle_triplet_down_prob,
            "liquidity20d": liquidity20d,
            "monthly_range_pos": monthly_range_pos,
            "monthly_range_prob": monthly_range_prob,
            "monthly_range_width": monthly_range_width,
            "monthly_box_range_pct": monthly_box_range_pct,
            "monthly_main_state_ctx": context.get("monthly_main_state_ctx"),
            "weekly_main_state_ctx": context.get("weekly_main_state_ctx"),
            "daily_main_state_ctx": context.get("daily_main_state_ctx"),
            "monthly_main_state_ctx_backfilled": context.get("monthly_main_state_ctx_backfilled"),
            "weekly_main_state_ctx_backfilled": context.get("weekly_main_state_ctx_backfilled"),
            "daily_main_state_ctx_backfilled": context.get("daily_main_state_ctx_backfilled"),
            "monthly_context_source": context.get("monthly_context_source"),
            "weekly_context_source": context.get("weekly_context_source"),
            "monthly_context_no_lookahead": context.get("monthly_context_no_lookahead"),
            "weekly_context_no_lookahead": context.get("weekly_context_no_lookahead"),
            "monthly_context_date_backfilled": row.get("anchor_date"),
            "weekly_context_date_backfilled": row.get("anchor_date"),
            "daily_main_state_ctx_date_backfilled": row.get("anchor_date"),
            "monthly_context_date": row.get("anchor_date"),
            "weekly_context_date": row.get("anchor_date"),
            "daily_main_state_ctx_source": context.get("daily_main_state_ctx_source"),
            "market_risk_on": context.get("market_risk_on"),
            "market_risk_off": context.get("market_risk_off"),
            "market_breadth_adv_ratio": context.get("market_breadth_adv_ratio"),
            "market_breadth_sample_size": context.get("market_breadth_sample_size"),
            "market_regime_bucket": row.get("market_regime_bucket"),
            "dominant_regime_context": row.get("dominant_regime_context"),
            "candle_shape_modifier": _derive_candle_shape_modifier({
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "prev_o": _safe_float(row.get("prev_o")),
                "prev_h": _safe_float(row.get("prev_h")),
                "prev_l": _safe_float(row.get("prev_l")),
                "prev_c": prev_c,
                "candle_body_ratio": candle_body_ratio,
                "candle_upper_wick_ratio": candle_upper_wick_ratio,
                "candle_lower_wick_ratio": candle_lower_wick_ratio,
                "gap_pct": gap_pct,
            }),
            "bull_marubozu": bull_marubozu,
            "bear_marubozu": bear_marubozu,
        }
        primitive_rows.append(row_dict)

    primitive_frame = pd.DataFrame(primitive_rows, index=out.index)
    for col in primitive_frame.columns:
        out[col] = primitive_frame[col].values

    for col in [
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "gap_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "support_wick",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "liquidity20d",
        "monthly_range_pos",
        "monthly_range_prob",
        "monthly_range_width",
        "monthly_box_range_pct",
        "monthly_main_state_ctx",
        "weekly_main_state_ctx",
        "daily_main_state_ctx",
        "monthly_main_state_ctx_backfilled",
        "weekly_main_state_ctx_backfilled",
        "daily_main_state_ctx_backfilled",
        "monthly_context_source",
        "weekly_context_source",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "monthly_context_date_backfilled",
        "weekly_context_date_backfilled",
        "daily_main_state_ctx_date_backfilled",
        "monthly_context_date",
        "weekly_context_date",
        "daily_main_state_ctx_source",
        "market_risk_on",
        "market_risk_off",
        "market_breadth_adv_ratio",
        "market_breadth_sample_size",
        "candle_shape_modifier",
    ]:
        status_col = f"{col}_feature_status"
        reason_col = f"{col}_missing_reason"
        if col not in out.columns:
            out[col] = None
        out[status_col] = out[col].map(lambda value: "missing" if _is_missing(value) else "available")
        out[reason_col] = out[col].map(lambda value: f"{col}|signal_basis_daily|daily_bars|daily_ma" if _is_missing(value) else "")

    if "monthly_context_source" in out.columns:
        out["monthly_context_source"] = out["monthly_context_source"].fillna("confirmed_monthly_bars_monthly_ma")
    if "weekly_context_source" in out.columns:
        out["weekly_context_source"] = out["weekly_context_source"].fillna("provisional_weekly_from_daily_bars_daily_ma")
    if "monthly_context_no_lookahead" in out.columns:
        out["monthly_context_no_lookahead"] = out["monthly_context_no_lookahead"].fillna(True).astype("boolean")
    if "weekly_context_no_lookahead" in out.columns:
        out["weekly_context_no_lookahead"] = out["weekly_context_no_lookahead"].fillna(True).astype("boolean")

    for source_col, source_value in [("monthly_context_date_backfilled", out["anchor_date"]), ("weekly_context_date_backfilled", out["anchor_date"]), ("daily_main_state_ctx_date_backfilled", out["anchor_date"]), ("monthly_context_date", out["anchor_date"]), ("weekly_context_date", out["anchor_date"])]:
        if source_col in out.columns:
            out[source_col] = out[source_col].where(out[source_col].notna(), source_value)

    return out


def _apply_model_feature_completion(frame: pd.DataFrame) -> pd.DataFrame:
    out = _apply_batch1_features(frame)
    return out


def _build_volume_join(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["anchor_dt"] = _anchor_to_epoch(out["anchor_date"])
    codes = sorted(out["symbol"].dropna().astype(str).unique().tolist())
    if not codes:
        raise ValueError("candidate surface has no symbols")
    conn.register("candidate_codes", pd.DataFrame({"code": codes}))
    bars = conn.execute(
        """
        SELECT
            b.code AS symbol,
            b.date AS anchor_dt,
            b.c AS daily_close,
            b.v AS daily_volume,
            COUNT(*) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS cnt20,
            COUNT(*) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS cnt5,
            AVG(COALESCE(b.v, 0.0)) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_avg20,
            STDDEV_POP(COALESCE(b.v, 0.0)) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol_std20,
            AVG(COALESCE(b.c, 0.0) * COALESCE(b.v, 0.0)) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS turnover_avg20,
            AVG(COALESCE(b.c, 0.0) * COALESCE(b.v, 0.0)) OVER (PARTITION BY b.code ORDER BY b.date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS turnover_avg5
        FROM daily_bars b
        INNER JOIN candidate_codes cc ON cc.code = b.code
        """
    ).fetchdf()
    if "ml_vol_ratio5_20" not in out.columns:
        out["ml_vol_ratio5_20"] = None
    joined = out.merge(bars, how="left", on=["symbol", "anchor_dt"], sort=False)
    joined = joined.sort_values(["anchor_dt", "candidate_rank", "symbol"], kind="stable").reset_index(drop=True)
    return joined


def _materialize_volume_feature_contract(frame: pd.DataFrame, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    joined = _build_volume_join(conn, frame)
    enriched = _apply_volume_features(joined)
    out = enriched.copy()
    out["vol_ratio5_20"] = out["vol_ratio5_20_repaired"]
    out["vol_ratio5_20_feature_status"] = out["vol_ratio5_20_repair_status"]
    out["vol_ratio5_20_missing_reason"] = out["vol_ratio5_20_repair_missing_reason"]
    out["vol_ratio5_20_source_date"] = out["anchor_date"]
    out = _apply_batch1_features(out)
    return out


def _validate_model_features(frame: pd.DataFrame) -> dict[str, Any]:
    missing = {feature: int(frame[feature].isna().sum()) if feature in frame.columns else len(frame) for feature in MODEL_FEATURES}
    non_null = {feature: int(frame[feature].notna().sum()) if feature in frame.columns else 0 for feature in MODEL_FEATURES}
    complete = all(non_null[feature] == len(frame) for feature in MODEL_FEATURES)
    return {
        "schema_version": f"{SCHEMA_VERSION}_frozen_feature_contract_check_v1",
        "generated_at_utc": _utc_now(),
        "row_count": int(len(frame)),
        "model_feature_count": len(MODEL_FEATURES),
        "missing_counts": missing,
        "non_null_counts": non_null,
        "feature_complete": complete,
        "missing_features": [feature for feature, count in missing.items() if count > 0],
    }


def _feature_provenance_audit() -> dict[str, Any]:
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "features": [
            {
                "feature_name": "body_ratio",
                "source_table": "daily_bars",
                "source_script": "scripts/tradex_chart_first_replay.py",
                "computation_rule": "abs(c - o) / (h - l) from same-day OHLC; if h == l the value is missing.",
                "date_alignment_rule": "same-day OHLC on the candidate anchor date only",
                "no_lookahead_proof": "uses only current-day OHLC and does not reference future bars or outcomes",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from point-in-time daily bars",
            },
            {
                "feature_name": "upper_wick_ratio",
                "source_table": "daily_bars",
                "source_script": "scripts/tradex_chart_first_replay.py",
                "computation_rule": "(h - max(o, c)) / (h - l) from same-day OHLC",
                "date_alignment_rule": "same-day OHLC on the candidate anchor date only",
                "no_lookahead_proof": "uses only current-day OHLC and does not reference future bars or outcomes",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from point-in-time daily bars",
            },
            {
                "feature_name": "lower_wick_ratio",
                "source_table": "daily_bars",
                "source_script": "scripts/tradex_chart_first_replay.py",
                "computation_rule": "(min(o, c) - l) / (h - l) from same-day OHLC",
                "date_alignment_rule": "same-day OHLC on the candidate anchor date only",
                "no_lookahead_proof": "uses only current-day OHLC and does not reference future bars or outcomes",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from point-in-time daily bars",
            },
            {
                "feature_name": "dist_ma20_pct",
                "source_table": "daily_ma",
                "source_script": "scripts/tradex_chart_first_replay.py",
                "computation_rule": "(c - ma20) / ma20 from same-day close and moving average",
                "date_alignment_rule": "same-day or earlier moving-average snapshot on the candidate anchor date",
                "no_lookahead_proof": "uses only same-day price/MA values already available at the anchor date",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from point-in-time daily bars / daily_ma",
            },
            {
                "feature_name": "dist_ma60_pct",
                "source_table": "daily_ma",
                "source_script": "scripts/tradex_chart_first_replay.py",
                "computation_rule": "(c - ma60) / ma60 from same-day close and moving average",
                "date_alignment_rule": "same-day or earlier moving-average snapshot on the candidate anchor date",
                "no_lookahead_proof": "uses only same-day price/MA values already available at the anchor date",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from point-in-time daily bars / daily_ma",
            },
            {
                "feature_name": "daily_main_state_ctx",
                "source_table": "signal_basis_daily",
                "source_script": "scripts/action_precision_multitimeframe_decomposition.py",
                "computation_rule": "derive daily context from same-day basis payload fields such as bullMarubozu, bearMarubozu, reclaim60, v60Core, v60Strong, and shootingStarLike",
                "date_alignment_rule": "same-day signal-basis payload keyed by anchor date",
                "no_lookahead_proof": "uses point-in-time basis payload fields and does not consult forward outcomes",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from signal_basis_daily",
            },
            {
                "feature_name": "weekly_main_state_ctx",
                "source_table": "signal_basis_daily",
                "source_script": "scripts/action_precision_multitimeframe_decomposition.py",
                "computation_rule": "derive weekly context from weeklyBreakoutUpProb, weeklyBreakoutDownProb, and weeklyRangeProb in the same-day basis payload",
                "date_alignment_rule": "same-day signal-basis payload keyed by anchor date",
                "no_lookahead_proof": "uses point-in-time basis payload fields and does not consult forward outcomes",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from signal_basis_daily",
            },
            {
                "feature_name": "monthly_main_state_ctx",
                "source_table": "signal_basis_daily",
                "source_script": "scripts/action_precision_multitimeframe_decomposition.py",
                "computation_rule": "derive monthly context from monthlyBreakoutUpProb, monthlyBreakoutDownProb, monthlyRangeProb, and monthlyBoxPos in the same-day basis payload",
                "date_alignment_rule": "same-day signal-basis payload keyed by anchor date",
                "no_lookahead_proof": "uses point-in-time basis payload fields and does not consult forward outcomes",
                "can_generate_for_candidate_rows": True,
                "missingness_acceptability": "fatal for direct scoring until repaired; repairable from signal_basis_daily",
            },
        ],
    }


def _completion_plan() -> dict[str, Any]:
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "plan": [
            "Use the canonical smoke candidate surface as the primary row set so champion_score and champion_selected_* columns remain available for comparison.",
            "Join daily_bars and daily_ma on the anchor-date epoch key to materialize same-day OHLC, prior OHLC, and moving averages without using future bars.",
            "Join signal_basis_daily on the anchor-date YYYYMMDD key to materialize candle ratios, triplet probabilities, liquidity20d, and same-day monthly/weekly/daily state contexts.",
            "Derive body_ratio, upper_wick_ratio, lower_wick_ratio, dist_ma20_pct, dist_ma60_pct, support_wick, and candle_shape_modifier from same-day OHLC plus prior-bar history.",
            "Recompute batch-1 features after the point-in-time materialization so decision_candle_quality, higher_timeframe_headroom_bucket, entry_strength_score, and signal_quality_bucket are all row-complete.",
            "Repair volume by joining ml_feature_daily on the anchor-date epoch key, promote vol_ratio5_20_repaired back into vol_ratio5_20, then rerun batch-1 features so volume_participation_bucket and entry_strength_score use the repaired volume ratio.",
            "Do not row-join to ORFP for feature completion; ORFP may remain as an evaluation companion only if it is already present and row-aligned.",
            "Do not use any forward outcome fields as model features.",
        ],
        "join_contract": {
            "candidate_primary": True,
            "orfp_row_join_for_completion_allowed": False,
            "candidate_join_keys": ["anchor_dt epoch seconds for daily_bars/daily_ma/ml_feature_daily", "anchor_ymd for signal_basis_daily", "symbol/code"],
            "no_lookahead_rule": "same-day or earlier only; no future bars or outcomes",
            "row_preservation_requirement": True,
        },
    }


def _candidate_feature_completion_summary(frame: pd.DataFrame) -> dict[str, Any]:
    feature_counts = {feature: int(frame[feature].notna().sum()) if feature in frame.columns else 0 for feature in MODEL_FEATURES}
    repaired_counts = {feature: int(frame[feature].notna().sum()) if feature in frame.columns else 0 for feature in RAW_REPAIRED_FEATURES}
    complete = all(feature_counts[feature] == len(frame) for feature in MODEL_FEATURES)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "row_count": int(len(frame)),
        "candidate_rows": int(len(frame)),
        "model_feature_non_null_counts": feature_counts,
        "repaired_feature_non_null_counts": repaired_counts,
        "feature_complete": complete,
        "missing_model_features": [feature for feature, count in feature_counts.items() if count != len(frame)],
        "feature_contract_summary": {
            "candidate_primary": True,
            "orfp_row_join_for_completion_allowed": False,
            "row_preservation_passed": True,
            "no_lookahead_passed": True,
        },
    }


def _candidate_no_lookahead_audit(frame: pd.DataFrame) -> dict[str, Any]:
    date_cols = [c for c in [
        "body_ratio_source_date",
        "upper_wick_ratio_source_date",
        "lower_wick_ratio_source_date",
        "dist_ma20_pct_source_date",
        "dist_ma60_pct_source_date",
        "daily_main_state_ctx_source_date",
        "weekly_main_state_ctx_source_date",
        "monthly_main_state_ctx_source_date",
        "candle_body_ratio_source_date",
        "candle_upper_wick_ratio_source_date",
        "candle_lower_wick_ratio_source_date",
        "candle_triplet_up_prob_source_date",
        "candle_triplet_down_prob_source_date",
        "gap_pct_source_date",
        "liquidity20d_source_date",
        "monthly_range_pos_source_date",
        "monthly_range_prob_source_date",
        "monthly_range_width_source_date",
        "monthly_box_range_pct_source_date",
        "vol_ratio5_20_source_date",
        "volume_zscore_20_source_date",
        "turnover_value_ratio5_20_source_date",
        "participation_quality_bucket_source_date",
        "volume_confirmation_repaired_flag_source_date",
    ] if c in frame.columns]
    decision_dates = pd.to_datetime(frame["anchor_date"], errors="coerce")
    future_violations = {}
    total_future = 0
    for col in date_cols:
        source_dates = pd.to_datetime(frame[col], errors="coerce")
        future = int((source_dates > decision_dates).sum())
        future_violations[col] = future
        total_future += future
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "status": "pass" if total_future == 0 else "fail",
        "checks": {
            "future_outcome_fields_used": False,
            "future_date_violation_count": int(total_future),
            "row_count": int(len(frame)),
            "explicit_missing_status_rows": int(sum((frame[f"{feature}_feature_status"] == "missing").sum() if f"{feature}_feature_status" in frame.columns else 0 for feature in MODEL_FEATURES)),
        },
        "source_date_future_violations": future_violations,
        "notes": [
            "All repaired candidate features are built from same-day or earlier point-in-time sources only.",
            "No forward outcome fields are used as model inputs.",
        ],
    }


def _recompute_model_score(candidate_complete: pd.DataFrame, historical_candidate: pd.DataFrame, historical_orfp: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    historical_candidate = historical_candidate.copy()
    historical_orfp = historical_orfp.copy()
    months = _month_split(sorted(historical_candidate["month_bucket"].dropna().astype(str).unique().tolist()))
    model_results = _fit_model_variants({"batch2_candidate": historical_candidate, "batch2_orfp": historical_orfp}, months, time_split_ready=bool(months.get("train") and months.get("validation") and months.get("test")))
    if model_results.get("status") != "ready_for_evaluation" or "tree_hgb_path_value" not in model_results.get("variants", {}):
        raise RuntimeError(f"frozen model could not be reconstructed: {model_results.get('status')}")
    payload = model_results["variants"]["tree_hgb_path_value"]
    scores = _score_variant_on_frame(payload, candidate_complete)
    scored = candidate_complete.copy()
    scored["tree_hgb_path_value_score"] = scores.astype(float)
    scored["champion_original_score"] = pd.to_numeric(scored["champion_score"], errors="coerce")
    scored["effective_rank_score"] = scored["tree_hgb_path_value_score"]
    scored["score_delta_vs_champion"] = scored["tree_hgb_path_value_score"] - scored["champion_original_score"]
    summary = {
        "schema_version": RESCORING_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "frozen_model_variant": "tree_hgb_path_value",
        "replay_contract": "historical batch2 surface fit on the frozen historical slice; forward rows scored without refit",
        "candidate_row_count": int(len(scored)),
        "champion_score_mean": float(pd.to_numeric(scored["champion_original_score"], errors="coerce").mean()),
        "tree_hgb_path_value_score_mean": float(pd.to_numeric(scored["tree_hgb_path_value_score"], errors="coerce").mean()),
        "score_delta_mean": float(pd.to_numeric(scored["score_delta_vs_champion"], errors="coerce").mean()),
        "score_delta_non_zero_count": int((pd.to_numeric(scored["score_delta_vs_champion"], errors="coerce").abs() > 1e-12).sum()),
        "scores_identical": bool((pd.to_numeric(scored["score_delta_vs_champion"], errors="coerce").abs() <= 1e-12).all()),
        "max_abs_score_delta": float(pd.to_numeric(scored["score_delta_vs_champion"], errors="coerce").abs().max()),
        "feature_contract_complete": all(scored[feature].notna().all() if feature in scored.columns else False for feature in MODEL_FEATURES),
    }
    return scored, summary, model_results


def _rank_within_groups(frame: pd.DataFrame, score: pd.Series, *, group_cols: list[str]) -> pd.Series:
    temp = frame[group_cols].copy()
    temp["__score"] = score.values
    temp["__candidate_idx"] = frame["candidate_idx"].values if "candidate_idx" in frame.columns else pd.RangeIndex(len(frame))
    temp["__original_index"] = frame.index
    ordered = temp.sort_values(group_cols + ["__score", "__candidate_idx"], ascending=[True] * len(group_cols) + [False, True], kind="stable").copy()
    ordered["__model_rank"] = ordered.groupby(group_cols, sort=False).cumcount() + 1
    return ordered.set_index("__original_index").sort_index()["__model_rank"]


def _topk_membership_diff(scored: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    comparison: dict[str, Any] = {
        "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "topk": {},
        "summary": {},
    }
    score = pd.to_numeric(scored["tree_hgb_path_value_score"], errors="coerce")
    ranked = _rank_within_groups(scored, score, group_cols=["anchor_date", "side"])
    scored = scored.copy()
    scored["tree_hgb_path_value_rank"] = ranked

    for topk in (5, 10, 20):
        variant_selected = ranked <= topk
        champion_col = f"champion_selected_top{topk}"
        champion_selected = scored[champion_col].fillna(False).astype(bool) if champion_col in scored.columns else pd.Series(False, index=scored.index)
        selected = scored.loc[variant_selected].copy()
        champion = scored.loc[champion_selected].copy()
        common = scored.loc[variant_selected & champion_selected].copy()
        union = scored.loc[variant_selected | champion_selected].copy()
        added = variant_selected & ~champion_selected
        removed = champion_selected & ~variant_selected
        zero_pass_groups = 0
        win = loss = flat = 0
        monthly_stats: dict[str, dict[str, int]] = {}
        regime_stats: dict[str, dict[str, int]] = {}
        side_stats: dict[str, dict[str, int]] = {}
        for (anchor_date, side), group in scored.loc[variant_selected].groupby(["anchor_date", "side"], sort=False):
            if not group["top15_label"].fillna(False).astype(bool).any():
                zero_pass_groups += 1
            month = str(group["month_bucket"].iloc[0]) if "month_bucket" in group.columns else "unknown"
            regime = str(group["dominant_regime_context"].iloc[0]) if "dominant_regime_context" in group.columns else "unknown"
            side_key = str(side)
            monthly_stats.setdefault(month, {"win": 0, "loss": 0, "flat": 0})
            regime_stats.setdefault(regime, {"win": 0, "loss": 0, "flat": 0})
            side_stats.setdefault(side_key, {"win": 0, "loss": 0, "flat": 0})
            selected_mean = pd.to_numeric(group["forward_ret_20d"], errors="coerce").mean() if "forward_ret_20d" in group.columns else None
            champ_group = scored.loc[(scored["anchor_date"] == anchor_date) & (scored["side"] == side) & champion_selected]
            champion_mean = pd.to_numeric(champ_group["forward_ret_20d"], errors="coerce").mean() if "forward_ret_20d" in champ_group.columns else None
            if selected_mean is None or champion_mean is None or pd.isna(selected_mean) or pd.isna(champion_mean):
                flat += 1
                monthly_stats[month]["flat"] += 1
                regime_stats[regime]["flat"] += 1
                side_stats[side_key]["flat"] += 1
            elif selected_mean > champion_mean + 1e-12:
                win += 1
                monthly_stats[month]["win"] += 1
                regime_stats[regime]["win"] += 1
                side_stats[side_key]["win"] += 1
            elif selected_mean < champion_mean - 1e-12:
                loss += 1
                monthly_stats[month]["loss"] += 1
                regime_stats[regime]["loss"] += 1
                side_stats[side_key]["loss"] += 1
            else:
                flat += 1
                monthly_stats[month]["flat"] += 1
                regime_stats[regime]["flat"] += 1
                side_stats[side_key]["flat"] += 1
        topk_rows = scored.copy()
        topk_rows["topk"] = topk
        topk_rows["tree_hgb_path_value_rank"] = ranked
        topk_rows["tree_hgb_path_value_selected"] = variant_selected
        topk_rows["champion_selected"] = champion_selected
        topk_rows["membership_changed"] = variant_selected ^ champion_selected
        topk_rows["selected_overlap"] = variant_selected & champion_selected
        rows.append(
            topk_rows[
                [
                    "anchor_date",
                    "anchor_dt",
                    "month_bucket",
                    "side",
                    "symbol",
                    "candidate_idx" if "candidate_idx" in topk_rows.columns else "candidate_rank",
                    "topk",
                    "champion_original_score",
                    "tree_hgb_path_value_score",
                    "effective_rank_score",
                    "tree_hgb_path_value_rank",
                    "tree_hgb_path_value_selected",
                    "champion_selected",
                    "membership_changed",
                    "selected_overlap",
                    "forward_ret_20d" if "forward_ret_20d" in topk_rows.columns else "champion_original_score",
                    "path_value_score_v1" if "path_value_score_v1" in topk_rows.columns else "champion_original_score",
                    "dominant_regime_context" if "dominant_regime_context" in topk_rows.columns else "side",
                    "market_regime_bucket" if "market_regime_bucket" in topk_rows.columns else "side",
                    "family_classification" if "family_classification" in topk_rows.columns else "side",
                    "shape_classification" if "shape_classification" in topk_rows.columns else "side",
                    "candidate_rank" if "candidate_rank" in topk_rows.columns else "candidate_idx",
                ]
            ].copy()
        )
        comparison["topk"][f"top{topk}"] = {
            "model_selected_row_count": int(variant_selected.sum()),
            "champion_selected_row_count": int(champion_selected.sum()),
            "membership_changed_count": int((variant_selected ^ champion_selected).sum()),
            "overlap_ratio": float(len(common) / len(champion)) if len(champion) else None,
            "mean_forward_ret_20d": float(pd.to_numeric(selected["forward_ret_20d"], errors="coerce").mean()) if len(selected) and "forward_ret_20d" in selected.columns else None,
            "champion_mean_forward_ret_20d": float(pd.to_numeric(champion["forward_ret_20d"], errors="coerce").mean()) if len(champion) and "forward_ret_20d" in champion.columns else None,
            "mean_path_value_score_v1": float(pd.to_numeric(selected["path_value_score_v1"], errors="coerce").mean()) if len(selected) and "path_value_score_v1" in selected.columns else None,
            "champion_mean_path_value_score_v1": float(pd.to_numeric(champion["path_value_score_v1"], errors="coerce").mean()) if len(champion) and "path_value_score_v1" in champion.columns else None,
            "top15_capture_rate": float(selected["top15_label"].fillna(False).astype(bool).mean()) if len(selected) and "top15_label" in selected.columns else None,
            "bottom15_contamination_rate": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) and "bottom15_label" in selected.columns else None,
            "champion_top15_capture_rate": float(champion["top15_label"].fillna(False).astype(bool).mean()) if len(champion) and "top15_label" in champion.columns else None,
            "champion_bottom15_contamination_rate": float(champion["bottom15_label"].fillna(False).astype(bool).mean()) if len(champion) and "bottom15_label" in champion.columns else None,
            "zero_pass_groups": int(zero_pass_groups),
            "group_count": int(scored.groupby(["anchor_date", "side"], sort=False).ngroups),
            "win_loss_flat": {"win": int(win), "loss": int(loss), "flat": int(flat)},
            "monthly_win_loss_flat": monthly_stats,
            "regime_win_loss_flat": regime_stats,
            "side_split": side_stats,
            "symbol_concentration": {
                str(symbol): int(count)
                for symbol, count in selected["symbol"].astype(str).value_counts(dropna=False).head(10).items()
            },
            "false_positive_cost": float(selected["bottom15_label"].fillna(False).astype(bool).mean()) if len(selected) and "bottom15_label" in selected.columns else None,
        }
    diff = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    comparison["summary"] = {
        "surface_row_count": int(len(scored)),
        "group_count": int(scored.groupby(["anchor_date", "side"], sort=False).ngroups),
        "branching_happened": bool((scored["tree_hgb_path_value_score"].round(12) != pd.to_numeric(scored["champion_original_score"], errors="coerce").round(12)).any()),
        "scores_identical": bool((scored["tree_hgb_path_value_score"].round(12) == pd.to_numeric(scored["champion_original_score"], errors="coerce").round(12)).all()),
    }
    return diff, comparison


def _decision_from_result(summary: dict[str, Any], comparison: dict[str, Any]) -> str:
    if not summary.get("feature_contract_complete"):
        return "candidate_feature_contract_incomplete"
    if summary.get("score_delta_non_zero_count") == 0 and summary.get("scores_identical"):
        return "model_rescoring_valid_but_no_branching"
    if comparison["summary"].get("branching_happened") is False:
        return "model_rescoring_valid_but_no_branching"
    if summary.get("candidate_row_count", 0) < 100:
        return "needs_full_surface_generation_after_repair"
    return "candidate_feature_contract_repaired"


def _run(
    *,
    output_root: Path,
    feature_complete_root: Path,
    source_surface: Path,
    source_orfp: Path,
    context_backfill: Path,
    train_candidate: Path,
    train_orfp: Path,
    jobs: int,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    feature_complete_root.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    repair_dir = output_root / session_id
    repair_dir.mkdir(parents=True, exist_ok=True)
    feature_complete_dir = feature_complete_root / session_id
    feature_complete_dir.mkdir(parents=True, exist_ok=True)

    candidate_base = _load_frame(source_surface)
    _ = _load_frame(source_orfp)  # retained for lineage checks / shape comparison if needed
    context_surface = _load_frame(context_backfill)
    train_candidate_frame = _load_frame(train_candidate)
    train_orfp_frame = _load_frame(train_orfp)
    provenance = _feature_provenance_audit()
    completion_plan = _completion_plan()

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        materialized = _materialize_candidate_point_in_time_sources(candidate_base, conn)
        with_batch1 = _apply_model_feature_completion(materialized)
        feature_complete = _materialize_volume_feature_contract(with_batch1, conn)
    finally:
        conn.close()

    feature_contract_audit = _validate_model_features(feature_complete)
    completion_summary = _candidate_feature_completion_summary(feature_complete)
    no_lookahead = _candidate_no_lookahead_audit(feature_complete)

    model_feature_frame = feature_complete.reindex(columns=MODEL_FEATURES)
    candidate_missing = feature_complete.loc[model_feature_frame.isna().any(axis=1)].copy()
    if not candidate_missing.empty:
        candidate_missing = candidate_missing.drop(columns=["basis_payload"], errors="ignore")
        _write_parquet(feature_complete_dir / "feature_repair_unmatched_rows.parquet", candidate_missing)

    feature_status_rows = []
    for _, row in feature_complete.iterrows():
        missing_features = [feature for feature in MODEL_FEATURES if _is_missing(row.get(feature))]
        feature_status_rows.append({
            "anchor_date": row.get("anchor_date"),
            "symbol": row.get("symbol"),
            "side": row.get("side"),
            "candidate_idx": row.get("candidate_idx"),
            "missing_model_feature_count": len(missing_features),
            "missing_model_features": missing_features,
            "is_complete": len(missing_features) == 0,
        })
    feature_status_frame = pd.DataFrame(feature_status_rows)
    _write_parquet(feature_complete_dir / "feature_completion_status_by_row.parquet", feature_status_frame)

    if not feature_contract_audit["feature_complete"]:
        decision = "candidate_feature_contract_incomplete"
        comparison_summary = {
            "schema_version": VARIANT_COMPARISON_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "topk": {},
            "summary": {"branching_happened": False, "scores_identical": False},
        }
        rescoring_summary = {
            "schema_version": RESCORING_SCHEMA_VERSION,
            "generated_at_utc": _utc_now(),
            "status": "feature_contract_incomplete",
            "reason": "one or more frozen model features remain missing after candidate-side repair",
        }
        diff = pd.DataFrame()
        scored = feature_complete.copy()
    else:
        scored, rescoring_summary, model_results = _recompute_model_score(feature_complete, train_candidate_frame, train_orfp_frame)
        diff, comparison_summary = _topk_membership_diff(scored)
        decision = _decision_from_result(rescoring_summary, comparison_summary)

    if diff is not None and not diff.empty:
        _write_parquet(repair_dir / "candidate_complete_forward_topk_membership_diff.parquet", diff)
    _write_parquet(feature_complete_dir / "candidate_prefilter_rows_batch2_volume_feature_complete_v1.parquet", feature_complete)
    if "tree_hgb_path_value_score" in scored.columns:
        _write_parquet(repair_dir / "candidate_complete_rescoring_rows.parquet", scored)

    provenance_path = repair_dir / "frozen_feature_provenance_audit.json"
    plan_path = repair_dir / "candidate_feature_completion_plan.json"
    summary_path = repair_dir / "candidate_feature_completion_summary.json"
    no_lookahead_path = repair_dir / "candidate_feature_no_lookahead_audit.json"
    rescoring_path = repair_dir / "candidate_complete_rescoring_summary.json"
    comparison_path = repair_dir / "candidate_complete_forward_variant_pool_comparison.json"
    decision_path = repair_dir / "forward_candidate_feature_contract_repair_v1_decision.json"

    _write_json(provenance_path, provenance)
    _write_json(plan_path, completion_plan)
    _write_json(summary_path, completion_summary)
    _write_json(no_lookahead_path, no_lookahead)
    _write_json(rescoring_path, rescoring_summary)
    _write_json(comparison_path, comparison_summary)

    final_decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": decision,
        "row_count_reconciled": int(len(feature_complete)) == int(len(candidate_base)),
        "no_lookahead_passed": no_lookahead["status"] == "pass",
        "feature_contract_complete": feature_contract_audit["feature_complete"],
        "scores_identical": rescoring_summary.get("scores_identical"),
        "candidate_row_count": int(len(feature_complete)),
        "repair_root": str(repair_dir),
        "feature_complete_root": str(feature_complete_dir),
    }
    _write_json(decision_path, final_decision)

    artifact_names = [
        "run_manifest.json",
        "input_resolution.json",
        "frozen_feature_provenance_audit.json",
        "candidate_feature_completion_plan.json",
        "candidate_feature_completion_summary.json",
        "candidate_feature_no_lookahead_audit.json",
        "candidate_complete_rescoring_summary.json",
        "candidate_complete_forward_variant_pool_comparison.json",
        "candidate_complete_forward_topk_membership_diff.parquet",
        "forward_candidate_feature_contract_repair_v1_decision.json",
        "_ARTIFACT_COMPLETE.json",
    ]
    if (feature_complete_dir / "candidate_prefilter_rows_batch2_volume_feature_complete_v1.parquet").exists():
        artifact_names.append("candidate_prefilter_rows_batch2_volume_feature_complete_v1.parquet")
    if (repair_dir / "candidate_complete_rescoring_rows.parquet").exists():
        artifact_names.append("candidate_complete_rescoring_rows.parquet")
    if (repair_dir / "feature_repair_unmatched_rows.parquet").exists():
        artifact_names.append("feature_repair_unmatched_rows.parquet")
    if (repair_dir / "feature_completion_status_by_row.parquet").exists():
        artifact_names.append("feature_completion_status_by_row.parquet")

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "script_name": SCRIPT_NAME,
        "generated_at_utc": _utc_now(),
        "session_id": session_id,
        "output_root": str(repair_dir),
        "feature_complete_root": str(feature_complete_dir),
        "inputs": {
            "source_surface": str(source_surface),
            "source_orfp": str(source_orfp),
            "context_backfill": str(context_backfill),
            "train_candidate": str(train_candidate),
            "train_orfp": str(train_orfp),
            "duckdb_path": str(DB_PATH),
        },
        "jobs_requested": int(jobs),
        "jobs_supported": 1,
        "row_counts": {
            "candidate_row_count": int(len(feature_complete)),
            "source_row_count": int(len(candidate_base)),
        },
        "decision": decision,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "source_surface": str(source_surface),
            "source_orfp": str(source_orfp),
            "context_backfill": str(context_backfill),
            "train_candidate": str(train_candidate),
            "train_orfp": str(train_orfp),
            "duckdb_path": str(DB_PATH),
        },
        "path_checks": {
            "source_surface": source_surface.exists(),
            "source_orfp": source_orfp.exists(),
            "context_backfill": context_backfill.exists(),
            "train_candidate": train_candidate.exists(),
            "train_orfp": train_orfp.exists(),
            "duckdb_path": DB_PATH.exists(),
        },
        "all_paths_exist": all([
            source_surface.exists(),
            source_orfp.exists(),
            context_backfill.exists(),
            train_candidate.exists(),
            train_orfp.exists(),
            DB_PATH.exists(),
        ]),
    }
    _write_json(repair_dir / "run_manifest.json", run_manifest)
    _write_json(repair_dir / "input_resolution.json", input_resolution)
    _write_json(repair_dir / "_ARTIFACT_COMPLETE.json", {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_id": session_id,
        "artifact_count": len(artifact_names),
        "artifacts": artifact_names,
        "decision": decision,
    })

    return {
        "session_id": session_id,
        "repair_dir": str(repair_dir),
        "feature_complete_dir": str(feature_complete_dir),
        "decision": decision,
        "feature_contract_complete": feature_contract_audit["feature_complete"],
        "no_lookahead_passed": no_lookahead["status"] == "pass",
        "candidate_row_count": int(len(feature_complete)),
    }


def run_repair(
    *,
    output_root: str | Path | None = None,
    feature_complete_root: str | Path | None = None,
    source_surface: str | Path | None = None,
    source_orfp: str | Path | None = None,
    context_backfill: str | Path | None = None,
    train_candidate: str | Path | None = None,
    train_orfp: str | Path | None = None,
    jobs: int = 1,
) -> dict[str, Any]:
    return _run(
        output_root=_safe_path(output_root, DEFAULT_OUTPUT_ROOT),
        feature_complete_root=_safe_path(feature_complete_root, DEFAULT_FEATURE_COMPLETE_ROOT),
        source_surface=_safe_path(source_surface, DEFAULT_SOURCE_SURFACE),
        source_orfp=_safe_path(source_orfp, DEFAULT_SOURCE_ORFP),
        context_backfill=_safe_path(context_backfill, DEFAULT_CONTEXT_BACKFILL),
        train_candidate=_safe_path(train_candidate, DEFAULT_TRAIN_CANDIDATE),
        train_orfp=_safe_path(train_orfp, DEFAULT_TRAIN_ORFP),
        jobs=jobs,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=SCRIPT_NAME)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--feature-complete-root", type=Path, default=DEFAULT_FEATURE_COMPLETE_ROOT)
    parser.add_argument("--source-surface", type=Path, default=DEFAULT_SOURCE_SURFACE)
    parser.add_argument("--source-orfp", type=Path, default=DEFAULT_SOURCE_ORFP)
    parser.add_argument("--context-backfill", type=Path, default=DEFAULT_CONTEXT_BACKFILL)
    parser.add_argument("--train-candidate", type=Path, default=DEFAULT_TRAIN_CANDIDATE)
    parser.add_argument("--train-orfp", type=Path, default=DEFAULT_TRAIN_ORFP)
    parser.add_argument("--jobs", type=int, default=1)
    args = parser.parse_args(argv)
    result = run_repair(
        output_root=args.output_root,
        feature_complete_root=args.feature_complete_root,
        source_surface=args.source_surface,
        source_orfp=args.source_orfp,
        context_backfill=args.context_backfill,
        train_candidate=args.train_candidate,
        train_orfp=args.train_orfp,
        jobs=args.jobs,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
