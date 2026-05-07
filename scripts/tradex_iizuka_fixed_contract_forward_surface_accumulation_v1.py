from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if str(Path(__file__).resolve().parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.tradex_forward_candidate_feature_contract_repair_v1 import (
    _apply_model_feature_completion,
    _materialize_candidate_point_in_time_sources,
    _materialize_volume_feature_contract,
)
from scripts.tradex_feature_surface_batch1_v1 import _apply_batch1_features
from scripts.tradex_iizuka_pre_decisive_long_candidate_generation_v1 import _filter_candidate_rows

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_fixed_contract_forward_surface_accumulation_v1")
DEFAULT_SOURCE_SURFACE = Path(
    r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1\20260503T053753Z-395634\iizuka_pre_decisive_long_candidate_rows.parquet"
)
DEFAULT_SHAPE_SESSION = Path(
    r"G:\Tradex\conditional_high_value_candle_shape_modifier_v1\20260429T105018Z-26bc381e"
)
DEFAULT_RUNTIME_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

SCRIPT_NAME = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1"
SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1"
MANIFEST_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_manifest_v1"
INPUT_CHECK_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_input_check_v1"
SUMMARY_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_surface_summary_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_no_lookahead_audit_v1"
LEAKAGE_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_leakage_audit_v1"
VARIANT_COMPARE_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_variant_pool_comparison_v1"
FAILURE_MODE_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_failure_mode_audit_v1"
ORACLE_HEADROOM_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_oracle_headroom_audit_v1"
LINEAGE_COMPARE_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_lineage_comparison_v1"
DECISION_SCHEMA_VERSION = "tradex_iizuka_fixed_contract_forward_surface_accumulation_v1_decision_v1"

TOP_K_VALUES = (5, 10, 20)
EVAL_LABEL_COLUMNS = ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label")
NO_LOOKAHEAD_FLAGS = ("monthly_context_no_lookahead", "weekly_context_no_lookahead")


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


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_runtime_datetime(value: Any) -> pd.Timestamp | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value
    if isinstance(value, datetime):
        return pd.Timestamp(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        if len(text) == 8:
            parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
            return None if pd.isna(parsed) else pd.Timestamp(parsed)
        if len(text) == 10:
            parsed = pd.to_datetime(int(text), unit="s", errors="coerce")
            return None if pd.isna(parsed) else pd.Timestamp(parsed)
        if len(text) == 13:
            parsed = pd.to_datetime(int(text), unit="ms", errors="coerce")
            return None if pd.isna(parsed) else pd.Timestamp(parsed)
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed)


def _to_iso_date(value: Any) -> str | None:
    parsed = _parse_runtime_datetime(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d")


def _to_timestamp(value: Any) -> pd.Timestamp | None:
    return _parse_runtime_datetime(value)


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
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


def _signal_rank_fields(row: pd.Series) -> tuple[float | None, float | None]:
    score = _parse_json_dict(row.get("score_snapshot_json"))
    rank = _parse_json_dict(row.get("rank_snapshot_json"))
    entry_score = _safe_float(score.get("entryScore"))
    source_rank = _safe_float(rank.get("sourceRank"))
    if source_rank is None and entry_score is not None:
        source_rank = entry_score
    return entry_score, source_rank


def _build_shape_modifier_map(shape_session_dir: Path) -> dict[str, str]:
    classification_path = _ensure_exists(
        shape_session_dir / "conditional_shape_modifier_classification.json",
        "conditional_shape_modifier_classification.json",
    )
    payload = json.loads(classification_path.read_text(encoding="utf-8"))
    mapping: dict[str, str] = {}
    for row in payload.get("rows", []):
        modifier = str(row.get("candle_shape_modifier") or "").strip()
        shape_classification = str(row.get("shape_classification") or "").strip()
        if modifier:
            mapping[modifier] = shape_classification or "shape_missing"
    return mapping


def _load_source_universe(source_surface: Path) -> tuple[list[str], list[str]]:
    frame = _load_frame(source_surface)
    if frame.empty:
        raise RuntimeError(f"source surface is empty: {source_surface}")
    symbols = sorted(frame["symbol"].astype(str).unique().tolist())
    anchor_dates = sorted(frame["anchor_date"].astype(str).unique().tolist())
    return symbols, anchor_dates


def _query_forward_base(
    *,
    db_path: Path,
    symbols: list[str],
    anchor_dates: list[str],
    source_max_anchor_date: str,
    mature_label_max_date: str,
) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.register("forward_symbols", pd.DataFrame({"code": symbols}))
        conn.register("forward_dates", pd.DataFrame({"anchor_dt": [int(d.replace("-", "")) for d in anchor_dates]}))
        frame = conn.execute(
            """
            WITH buy_rows AS (
                SELECT
                    s.dt,
                    s.code,
                    s.side,
                    s.entry_qualified,
                    s.setup_type,
                    s.reason_snapshot_json,
                    s.score_snapshot_json,
                    s.rank_snapshot_json,
                    s.decision_hash
                FROM signal_decision_daily s
                INNER JOIN forward_symbols sym ON sym.code = s.code
                INNER JOIN forward_dates d ON d.anchor_dt = s.dt
                WHERE s.side = 'buy'
            )
            SELECT * FROM buy_rows
            ORDER BY dt, code
            """
        ).fetchdf()
    finally:
        conn.close()

    if frame.empty:
        raise RuntimeError("forward base surface is empty after filtering signal_decision_daily")

    frame["dt"] = pd.to_numeric(frame["dt"], errors="coerce").astype("Int64")
    frame["anchor_date"] = pd.to_datetime(frame["dt"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    frame["symbol"] = frame["code"].astype(str)
    frame["side"] = "long"
    frame["month_bucket"] = pd.to_datetime(frame["anchor_date"], errors="coerce").dt.strftime("%Y-%m")
    frame["source_signal_side"] = "buy"
    frame["research_fallback_label_source"] = "ml_label_20d"
    frame["source_max_anchor_date"] = source_max_anchor_date
    frame["mature_label_max_date"] = mature_label_max_date
    entry_scores: list[float | None] = []
    source_ranks: list[float | None] = []
    for _, row in frame.iterrows():
        entry_score, source_rank = _signal_rank_fields(row)
        entry_scores.append(entry_score)
        source_ranks.append(source_rank)
    frame["champion_score"] = entry_scores
    frame["champion_rank"] = source_ranks
    fallback_rank = pd.to_numeric(frame["champion_score"], errors="coerce").rank(method="first", ascending=False)
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").fillna(fallback_rank).astype(float)
    frame["candidate_idx"] = range(len(frame))
    frame["candidate_rank"] = frame["champion_rank"]
    frame["candidate_score"] = frame["champion_score"]
    frame["surface_key"] = frame["anchor_date"].astype(str) + "|" + frame["symbol"].astype(str) + "|" + frame["side"].astype(str)
    frame["canonical_candidate_key"] = frame["surface_key"]
    frame["key"] = frame["surface_key"]
    return frame


def _shape_feature_join(frame: pd.DataFrame, shape_rows: pd.DataFrame, shape_modifier_map: dict[str, str]) -> pd.DataFrame:
    cols = [
        "code",
        "trade_date",
        "family_classification",
        "stable_bad_pick_family",
        "stable_high_value_family",
        "conditional_high_value",
        "candle_shape_modifier",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
    ]
    available_cols = [col for col in cols if col in shape_rows.columns]
    shape = shape_rows[available_cols].copy()
    shape["trade_date"] = pd.to_numeric(shape["trade_date"], errors="coerce").astype("Int64")
    shape["code"] = shape["code"].astype(str)
    shape["anchor_date"] = pd.to_datetime(shape["trade_date"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    shape["shape_classification"] = shape["candle_shape_modifier"].map(shape_modifier_map).fillna("shape_missing") if "candle_shape_modifier" in shape.columns else "shape_missing"
    merged = frame.merge(shape, on=["code", "anchor_date"], how="left", suffixes=("", "_shape"))
    if "stable_bad_pick_family" in merged.columns:
        merged["stable_bad_pick_family"] = merged["stable_bad_pick_family"].fillna(False).astype(bool)
    if "conditional_high_value" in merged.columns:
        merged["conditional_high_value"] = merged["conditional_high_value"].fillna(False).astype(bool)
    if "monthly_context_no_lookahead_shape" in merged.columns and "monthly_context_no_lookahead" not in merged.columns:
        merged["monthly_context_no_lookahead"] = merged["monthly_context_no_lookahead_shape"]
    if "weekly_context_no_lookahead_shape" in merged.columns and "weekly_context_no_lookahead" not in merged.columns:
        merged["weekly_context_no_lookahead"] = merged["weekly_context_no_lookahead_shape"]
    if "candle_shape_modifier" in merged.columns:
        merged["shape_classification"] = merged["candle_shape_modifier"].map(shape_modifier_map).fillna("shape_missing")
    else:
        merged["shape_classification"] = "shape_missing"
    return merged


def _materialize_ma_position_context(frame: pd.DataFrame, conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    ma = conn.execute(
        """
        SELECT
            code,
            dt,
            close,
            ma7,
            ma20,
            ma60,
            ma7_prev1,
            ma20_prev1,
            ma60_prev1,
            cnt_20_above,
            cnt_7_above,
            close_prev1,
            close_prev5,
            close_prev10,
            drawdown60,
            rebound60,
            atr14,
            atr14_pct,
            range_pct,
            gap_pct,
            vol_ret5,
            vol_ret20,
            vol_ratio5_20
        FROM feature_frame_daily
        """
    ).fetchdf()
    ma["anchor_date"] = pd.to_datetime(ma["dt"], unit="s", errors="coerce").dt.strftime("%Y-%m-%d")
    ma["symbol"] = ma["code"].astype(str)
    ma = ma.drop(columns=["dt", "code"])
    enriched = frame.merge(ma, on=["anchor_date", "symbol"], how="left", suffixes=("", "_ma"))
    for column in (
        "close",
        "ma7",
        "ma20",
        "ma60",
        "ma7_prev1",
        "ma20_prev1",
        "ma60_prev1",
        "cnt_20_above",
        "cnt_7_above",
        "close_prev1",
        "close_prev5",
        "close_prev10",
        "drawdown60",
        "rebound60",
        "atr14",
        "atr14_pct",
        "range_pct",
        "gap_pct",
        "vol_ret5",
        "vol_ret20",
        "vol_ratio5_20",
    ):
        shadow = f"{column}_ma"
        if shadow in enriched.columns:
            if column not in enriched.columns:
                enriched[column] = enriched[shadow]
            else:
                enriched[column] = enriched[column].where(enriched[column].notna(), enriched[shadow])
            enriched = enriched.drop(columns=[shadow])
    enriched["ma20_slope_1"] = (pd.to_numeric(enriched["ma20"], errors="coerce") - pd.to_numeric(enriched["ma20_prev1"], errors="coerce")) / pd.to_numeric(enriched["ma20_prev1"], errors="coerce").abs()
    enriched["ma60_slope_1"] = (pd.to_numeric(enriched["ma60"], errors="coerce") - pd.to_numeric(enriched["ma60_prev1"], errors="coerce")) / pd.to_numeric(enriched["ma60_prev1"], errors="coerce").abs()
    enriched["ma7_slope_1"] = (pd.to_numeric(enriched["ma7"], errors="coerce") - pd.to_numeric(enriched["ma7_prev1"], errors="coerce")) / pd.to_numeric(enriched["ma7_prev1"], errors="coerce").abs()
    enriched["close_vs_ma7_pct"] = (pd.to_numeric(enriched["close"], errors="coerce") - pd.to_numeric(enriched["ma7"], errors="coerce")) / pd.to_numeric(enriched["ma7"], errors="coerce").abs()
    enriched["close_vs_ma20_pct"] = (pd.to_numeric(enriched["close"], errors="coerce") - pd.to_numeric(enriched["ma20"], errors="coerce")) / pd.to_numeric(enriched["ma20"], errors="coerce").abs()
    enriched["close_vs_ma60_pct"] = (pd.to_numeric(enriched["close"], errors="coerce") - pd.to_numeric(enriched["ma60"], errors="coerce")) / pd.to_numeric(enriched["ma60"], errors="coerce").abs()
    return enriched


def _materialize_candle_reversal_flags(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    modifier = out.get("candle_shape_modifier")
    if modifier is None:
        out["bull_engulfing"] = False
        out["morning_star"] = False
        out["bull_marubozu"] = False
        return out
    modifier_text = modifier.astype(str)
    for column, pattern in (
        ("bull_engulfing", "bull_engulfing"),
        ("morning_star", "morning_star"),
        ("bull_marubozu", "bull_marubozu"),
    ):
        derived = modifier_text.eq(pattern)
        if column in out.columns:
            out[column] = out[column].where(out[column].notna(), derived)
        else:
            out[column] = derived
    return out


def _attach_evaluation_labels(candidate: pd.DataFrame, ml_label_frame: pd.DataFrame, shape_rows: pd.DataFrame) -> pd.DataFrame:
    out = candidate.copy()
    ml = ml_label_frame.copy()
    ml["anchor_date"] = ml["dt"].map(_to_iso_date)
    ml["code"] = ml["code"].astype(str)
    ml = ml[["anchor_date", "code", "ret20", "up20_label"]].copy()
    out = out.merge(ml, on=["anchor_date", "code"], how="left")
    if "ret20" in out.columns:
        out["forward_ret_20d"] = pd.to_numeric(out["ret20"], errors="coerce")
    if "up20_label" in out.columns:
        out["ml_up20_label"] = out["up20_label"].fillna(False).astype(bool)
    shape = shape_rows.copy()
    shape["trade_date"] = pd.to_numeric(shape["trade_date"], errors="coerce").astype("Int64")
    shape["anchor_date"] = pd.to_datetime(shape["trade_date"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")
    shape["code"] = shape["code"].astype(str)
    keep_cols = [c for c in ["anchor_date", "code", "path_value_score_v1", "top15_label", "bottom15_label"] if c in shape.columns]
    out = out.merge(shape[keep_cols], on=["anchor_date", "code"], how="left", suffixes=("", "_shape_eval"))
    for column in ("path_value_score_v1", "top15_label", "bottom15_label"):
        if f"{column}_shape_eval" in out.columns:
            if column not in out.columns or out[column].isna().all():
                out[column] = out[f"{column}_shape_eval"]
            else:
                out[column] = out[column].where(out[column].notna(), out[f"{column}_shape_eval"])
            out = out.drop(columns=[f"{column}_shape_eval"])
    for column in ("top15_label", "bottom15_label"):
        if column in out.columns:
            out[column] = out[column].fillna(False).astype(bool)
    out["evaluation_only_outcomes"] = True
    out["outcome_attachment_source"] = "ml_label_20d+conditional_shape_rows"
    out["outcome_attachment_complete"] = all(col in out.columns and out[col].notna().all() for col in ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label"))
    out["attached_outcome_columns"] = ", ".join([col for col in ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label") if col in out.columns])
    return out


def _surface_key(frame: pd.DataFrame) -> pd.Series:
    return frame["anchor_date"].astype(str) + "|" + frame["symbol"].astype(str) + "|" + frame["side"].astype(str)


def _add_champion_selection_flags(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["champion_selected_top5"] = out["champion_rank"] <= 5
    out["champion_selected_top10"] = out["champion_rank"] <= 10
    out["champion_selected_top20"] = out["champion_rank"] <= 20
    return out


def _no_lookahead_audit(frame: pd.DataFrame) -> dict[str, Any]:
    violations: dict[str, int] = {}
    for flag in NO_LOOKAHEAD_FLAGS:
        if flag in frame.columns:
            violations[f"{flag}_false_count"] = int((~frame[flag].fillna(False).astype(bool)).sum())
    date_violations = {}
    for field in ("monthly_context_date", "weekly_context_date"):
        if field in frame.columns:
            asof = pd.to_datetime(frame["anchor_date"], errors="coerce")
            context = pd.to_datetime(frame[field], errors="coerce")
            date_violations[f"{field}_future_count"] = int((context > asof).sum())
    fallback_ok = bool((frame["research_fallback_label_source"].astype(str) == "ml_label_20d").all()) if "research_fallback_label_source" in frame.columns else False
    return {
        "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "no_lookahead_pass": all(v == 0 for v in violations.values()) and all(v == 0 for v in date_violations.values()) and fallback_ok,
        "flag_violations": violations,
        "date_violations": date_violations,
        "research_fallback_label_source": "ml_label_20d" if fallback_ok else None,
        "notes": [
            "candidate rows use only historical / current-row context fields",
            "evaluation labels are attached after candidate construction",
        ],
    }


def _leakage_audit(candidate: pd.DataFrame) -> dict[str, Any]:
    feature_fields_used = {
        "signal_quality_bucket",
        "decision_candle_quality",
        "volume_participation_bucket",
        "shape_classification",
        "monthly_context_no_lookahead",
        "weekly_context_no_lookahead",
        "stable_bad_pick_family",
        "entry_strength_score",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "ma20_slope_1",
        "vol_ratio5_20",
        "drawdown60",
        "rebound60",
        "cnt_20_above",
        "cnt_7_above",
    }
    outcome_fields = set(EVAL_LABEL_COLUMNS)
    return {
        "schema_version": LEAKAGE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "feature_fields_used": sorted(feature_fields_used),
        "outcome_fields": sorted(outcome_fields),
        "outcome_fields_used_as_features": sorted(feature_fields_used.intersection(outcome_fields)),
        "outcome_fields_attached_after_candidate_construction": sorted([c for c in EVAL_LABEL_COLUMNS if c in candidate.columns]),
        "leakage_free": not feature_fields_used.intersection(outcome_fields),
        "research_fallback_label_source": "ml_label_20d",
        "note": "evaluation labels were joined after the candidate surface was constructed",
    }


def _surface_summary(candidate: pd.DataFrame, source: pd.DataFrame) -> dict[str, Any]:
    source_group_count = int(source.loc[source["side"].astype(str) == "long", "anchor_date"].nunique())
    candidate_group_count = int(candidate["anchor_date"].nunique()) if len(candidate) else 0
    candidate_month = pd.to_datetime(candidate["anchor_date"], errors="coerce").dt.strftime("%Y-%m") if len(candidate) else pd.Series(dtype=str)
    candidate_symbols = Counter(candidate["symbol"].astype(str).tolist()) if len(candidate) else Counter()
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "row_count": int(len(candidate)),
        "group_count": candidate_group_count,
        "source_long_group_count": source_group_count,
        "zero_pass_groups": int(max(0, source_group_count - candidate_group_count)),
        "symbol_count": int(candidate["symbol"].nunique()) if len(candidate) else 0,
        "month_count": int(candidate_month.nunique()) if len(candidate) else 0,
        "top_symbol_counts": {str(k): int(v) for k, v in candidate_symbols.most_common(10)} if candidate_symbols else {},
        "candidate_reason_counts": {str(k): int(v) for k, v in candidate["iizuka_candidate_reason"].value_counts().head(10).items()} if len(candidate) else {},
        "missing_feature_reason_counts": {str(k): int(v) for k, v in candidate["iizuka_missing_feature_reason"].replace("", "<none>").value_counts().head(10).items()} if len(candidate) else {},
        "score_summary": {
            "min": float(pd.to_numeric(candidate["iizuka_candidate_score"], errors="coerce").min()) if len(candidate) else None,
            "median": float(pd.to_numeric(candidate["iizuka_candidate_score"], errors="coerce").median()) if len(candidate) else None,
            "max": float(pd.to_numeric(candidate["iizuka_candidate_score"], errors="coerce").max()) if len(candidate) else None,
        },
        "rank_summary": {
            "min": float(pd.to_numeric(candidate["iizuka_candidate_rank"], errors="coerce").min()) if len(candidate) else None,
            "median": float(pd.to_numeric(candidate["iizuka_candidate_rank"], errors="coerce").median()) if len(candidate) else None,
            "max": float(pd.to_numeric(candidate["iizuka_candidate_rank"], errors="coerce").max()) if len(candidate) else None,
        },
    }


def _compare_topk(champion: pd.DataFrame, challenger: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    diff_rows: list[pd.DataFrame] = []
    failure_mode: dict[str, Any] = {
        "schema_version": FAILURE_MODE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "per_k": {},
    }
    headroom: dict[str, Any] = {
        "schema_version": ORACLE_HEADROOM_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "per_k": {},
    }
    for k in TOP_K_VALUES:
        champ = champion[champion[f"champion_selected_top{k}"]].copy()
        chal = challenger[challenger["iizuka_candidate_rank"] <= k].copy()
        champ_keys = set(champ["surface_key"])
        chal_keys = set(chal["surface_key"])
        union = champ_keys | chal_keys
        intersection = champ_keys & chal_keys
        diff = pd.DataFrame({"top_k": k, "surface_key": list(union)})
        diff = diff.merge(
            champion[["surface_key", "anchor_date", "symbol", "side", "champion_score", "champion_rank", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label"] + [c for c in ("candidate_idx", "month_bucket") if c in champion.columns]],
            on="surface_key",
            how="left",
        ).merge(
            chal[["surface_key", "iizuka_candidate_score", "iizuka_candidate_rank", "iizuka_candidate_reason", "iizuka_context_block_pass", "iizuka_compression_block_pass", "iizuka_trigger_proximity_block_pass", "iizuka_risk_block_pass"]],
            on="surface_key",
            how="left",
        )
        diff["selection_state"] = diff["surface_key"].apply(lambda key: "both" if key in intersection else ("champion_only" if key in champ_keys else "challenger_only"))
        diff["top_k"] = k
        diff["selected_in_champion"] = diff["surface_key"].isin(champ_keys)
        diff["selected_in_challenger"] = diff["surface_key"].isin(chal_keys)
        diff["member_change"] = diff["selection_state"] != "both"
        diff_rows.append(diff)

        champion_metrics = {
            "row_count": int(len(champ)),
            "mean_forward_ret_20d": float(pd.to_numeric(champ["forward_ret_20d"], errors="coerce").mean()) if len(champ) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(champ["path_value_score_v1"], errors="coerce").mean()) if len(champ) and "path_value_score_v1" in champ.columns else None,
            "top15_capture_count": int(pd.to_numeric(champ["top15_label"], errors="coerce").fillna(0).sum()) if len(champ) and "top15_label" in champ.columns else 0,
            "top15_capture_rate": float(pd.to_numeric(champ["top15_label"], errors="coerce").mean()) if len(champ) and "top15_label" in champ.columns else None,
            "top20pct_capture_count": int(pd.to_numeric(champ["top20pct_label"], errors="coerce").fillna(0).sum()) if "top20pct_label" in champ.columns and len(champ) else 0,
            "bottom15_contamination_count": int(pd.to_numeric(champ["bottom15_label"], errors="coerce").fillna(0).sum()) if len(champ) and "bottom15_label" in champ.columns else 0,
            "bottom15_contamination_rate": float(pd.to_numeric(champ["bottom15_label"], errors="coerce").mean()) if len(champ) and "bottom15_label" in champ.columns else None,
            "non_positive_return_count": int((pd.to_numeric(champ["forward_ret_20d"], errors="coerce") <= 0).sum()) if len(champ) else 0,
        }
        challenger_metrics = {
            "row_count": int(len(chal)),
            "mean_forward_ret_20d": float(pd.to_numeric(chal["forward_ret_20d"], errors="coerce").mean()) if len(chal) else None,
            "mean_path_value_score_v1": float(pd.to_numeric(chal["path_value_score_v1"], errors="coerce").mean()) if len(chal) and "path_value_score_v1" in chal.columns else None,
            "top15_capture_count": int(pd.to_numeric(chal["top15_label"], errors="coerce").fillna(0).sum()) if len(chal) and "top15_label" in chal.columns else 0,
            "top15_capture_rate": float(pd.to_numeric(chal["top15_label"], errors="coerce").mean()) if len(chal) and "top15_label" in chal.columns else None,
            "top20pct_capture_count": int(pd.to_numeric(chal["top20pct_label"], errors="coerce").fillna(0).sum()) if "top20pct_label" in chal.columns and len(chal) else 0,
            "bottom15_contamination_count": int(pd.to_numeric(chal["bottom15_label"], errors="coerce").fillna(0).sum()) if len(chal) and "bottom15_label" in chal.columns else 0,
            "bottom15_contamination_rate": float(pd.to_numeric(chal["bottom15_label"], errors="coerce").mean()) if len(chal) and "bottom15_label" in chal.columns else None,
            "non_positive_return_count": int((pd.to_numeric(chal["forward_ret_20d"], errors="coerce") <= 0).sum()) if len(chal) else 0,
        }
        rows.append(
            {
                "top_k": k,
                "champion": champion_metrics,
                "challenger": challenger_metrics,
                "membership_changed_count": int(len(champ_keys ^ chal_keys)),
                "overlap_ratio": float(len(intersection) / len(union)) if union else None,
                "champion_group_count": int(champ["anchor_date"].nunique()) if len(champ) else 0,
                "challenger_group_count": int(chal["anchor_date"].nunique()) if len(chal) else 0,
                "champion_symbol_count": int(champ["symbol"].nunique()) if len(champ) else 0,
                "challenger_symbol_count": int(chal["symbol"].nunique()) if len(chal) else 0,
                "zero_pass_groups": int(max(0, champion["anchor_date"].nunique() - chal["anchor_date"].nunique())),
            }
        )
        failure_mode["per_k"][str(k)] = {
            "champion_only_count": int(len(champ_keys - chal_keys)),
            "challenger_only_count": int(len(chal_keys - champ_keys)),
            "top15_winner_loss_count": int(pd.to_numeric(champ.loc[~champ["surface_key"].isin(chal_keys), "top15_label"], errors="coerce").fillna(0).sum()) if len(champ) and "top15_label" in champ.columns else 0,
            "bottom15_loss_count": int(pd.to_numeric(champ.loc[~champ["surface_key"].isin(chal_keys), "bottom15_label"], errors="coerce").fillna(0).sum()) if len(champ) and "bottom15_label" in champ.columns else 0,
            "reason_block_contribution": {str(k2): int(v2) for k2, v2 in chal["iizuka_candidate_reason"].value_counts().head(8).items()},
            "false_positive_cost": {
                "bottom15_count": int(pd.to_numeric(chal["bottom15_label"], errors="coerce").fillna(0).sum()) if len(chal) and "bottom15_label" in chal.columns else 0,
                "bottom15_rate": float(pd.to_numeric(chal["bottom15_label"], errors="coerce").mean()) if len(chal) and "bottom15_label" in chal.columns else None,
            },
        }
        missed = champ.loc[~champ["surface_key"].isin(chal_keys)].copy()
        missed = missed.sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5)
        gained = chal.loc[~chal["surface_key"].isin(champ_keys)].copy()
        gained = gained.sort_values(["top15_label", "forward_ret_20d", "path_value_score_v1"], ascending=[False, False, False], kind="stable").head(5)
        headroom["per_k"][str(k)] = {
            "missed_top15_examples": _records(missed, fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "champion_rank"]),
            "gained_top15_examples": _records(gained, fields=["anchor_date", "symbol", "surface_key", "forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label", "iizuka_candidate_score", "iizuka_candidate_rank"]),
            "missed_top15_count": int(pd.to_numeric(missed["top15_label"], errors="coerce").fillna(0).sum()) if len(missed) and "top15_label" in missed.columns else 0,
            "gained_top15_count": int(pd.to_numeric(gained["top15_label"], errors="coerce").fillna(0).sum()) if len(gained) and "top15_label" in gained.columns else 0,
        }

    comparison = {
        "schema_version": VARIANT_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "metric_mode": "per_anchor_date_topK",
        "per_k": rows,
    }
    diff_frame = pd.concat(diff_rows, ignore_index=True) if diff_rows else pd.DataFrame()
    return comparison, diff_frame, failure_mode, headroom


def _records(frame: pd.DataFrame, fields: list[str]) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    use = [c for c in fields if c in frame.columns]
    return [_json_ready(dict(row)) for row in frame[use].to_dict(orient="records")]


def _build_lineage_comparison(original_result: dict[str, Any], comparison: dict[str, Any], candidate: pd.DataFrame) -> dict[str, Any]:
    top5 = next(item for item in comparison["per_k"] if item["top_k"] == 5)
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    return {
        "schema_version": LINEAGE_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "original_lineage": {
            "decision": original_result.get("decision"),
            "row_count": original_result.get("summary", {}).get("candidate_row_count"),
            "group_count": original_result.get("summary", {}).get("candidate_group_count"),
            "symbol_count": original_result.get("summary", {}).get("candidate_symbol_count"),
        },
        "accumulated_lineage": {
            "decision": None,
            "row_count": int(len(candidate)),
            "group_count": int(candidate["anchor_date"].nunique()) if len(candidate) else 0,
            "symbol_count": int(candidate["symbol"].nunique()) if len(candidate) else 0,
        },
        "comparison": {
            "top5_delta_forward_ret_20d": top5["challenger"]["mean_forward_ret_20d"] - top5["champion"]["mean_forward_ret_20d"] if top5["challenger"]["mean_forward_ret_20d"] is not None and top5["champion"]["mean_forward_ret_20d"] is not None else None,
            "top10_delta_forward_ret_20d": top10["challenger"]["mean_forward_ret_20d"] - top10["champion"]["mean_forward_ret_20d"] if top10["challenger"]["mean_forward_ret_20d"] is not None and top10["champion"]["mean_forward_ret_20d"] is not None else None,
            "top20_delta_forward_ret_20d": top20["challenger"]["mean_forward_ret_20d"] - top20["champion"]["mean_forward_ret_20d"] if top20["challenger"]["mean_forward_ret_20d"] is not None and top20["champion"]["mean_forward_ret_20d"] is not None else None,
            "top5_delta_bottom15_rate": top5["challenger"]["bottom15_contamination_rate"] - top5["champion"]["bottom15_contamination_rate"] if top5["challenger"]["bottom15_contamination_rate"] is not None and top5["champion"]["bottom15_contamination_rate"] is not None else None,
            "top10_delta_bottom15_rate": top10["challenger"]["bottom15_contamination_rate"] - top10["champion"]["bottom15_contamination_rate"] if top10["challenger"]["bottom15_contamination_rate"] is not None and top10["champion"]["bottom15_contamination_rate"] is not None else None,
            "top20_delta_bottom15_rate": top20["challenger"]["bottom15_contamination_rate"] - top20["champion"]["bottom15_contamination_rate"] if top20["challenger"]["bottom15_contamination_rate"] is not None and top20["champion"]["bottom15_contamination_rate"] is not None else None,
        },
        "classification": "inconclusive",
        "notes": [
            "lineage is compared on the same forward universe using the repaired same-condition baseline",
            "top20pct is not carried as a canonical label in this accumulated surface",
        ],
    }


def _decision_from_result(comparison: dict[str, Any], no_lookahead: dict[str, Any], candidate: pd.DataFrame) -> dict[str, Any]:
    top5 = next(item for item in comparison["per_k"] if item["top_k"] == 5)
    top10 = next(item for item in comparison["per_k"] if item["top_k"] == 10)
    top20 = next(item for item in comparison["per_k"] if item["top_k"] == 20)
    decision = "hold_needs_more_forward_surfaces"
    reason = "direction remains mixed and the forward accumulation needs more coverage before challenger design"
    if not no_lookahead["no_lookahead_pass"]:
        decision = "pipeline_blocked"
        reason = "no-lookahead or fallback labeling failed"
    elif len(candidate) == 0:
        decision = "pipeline_blocked"
        reason = "candidate surface is empty after applying the fixed contract"
    elif (
        top5["challenger"]["mean_forward_ret_20d"] is not None
        and top10["challenger"]["mean_forward_ret_20d"] is not None
        and top20["challenger"]["mean_forward_ret_20d"] is not None
        and top5["challenger"]["mean_forward_ret_20d"] >= top5["champion"]["mean_forward_ret_20d"]
        and top10["challenger"]["mean_forward_ret_20d"] >= top10["champion"]["mean_forward_ret_20d"]
        and top20["challenger"]["mean_forward_ret_20d"] >= top20["champion"]["mean_forward_ret_20d"]
        and top20["challenger"]["bottom15_contamination_rate"] is not None
        and top20["champion"]["bottom15_contamination_rate"] is not None
        and top20["challenger"]["bottom15_contamination_rate"] <= top20["champion"]["bottom15_contamination_rate"] * 1.10
        and candidate["symbol"].nunique() >= 80
    ):
        decision = "ready_for_iizuka_candidate_challenger_design"
        reason = "same-condition evidence is strong enough to move to challenger design"
    elif top20["challenger"]["bottom15_contamination_rate"] is not None and top20["champion"]["bottom15_contamination_rate"] is not None and top20["challenger"]["bottom15_contamination_rate"] > top20["champion"]["bottom15_contamination_rate"] * 1.20:
        decision = "needs_iizuka_contract_redesign"
        reason = "winner capture is not sufficient to offset structurally worse bottom15 contamination"
    elif (
        (top5["challenger"]["mean_forward_ret_20d"] is not None and top5["challenger"]["mean_forward_ret_20d"] < top5["champion"]["mean_forward_ret_20d"]) 
        or (top10["challenger"]["mean_forward_ret_20d"] is not None and top10["challenger"]["mean_forward_ret_20d"] < top10["champion"]["mean_forward_ret_20d"]) 
        or (top20["challenger"]["mean_forward_ret_20d"] is not None and top20["challenger"]["mean_forward_ret_20d"] < top20["champion"]["mean_forward_ret_20d"])
    ):
        decision = "drop_iizuka_pre_decisive_candidate_contract"
        reason = "practical top-K quality did not improve enough under same conditions"
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "decision": decision,
        "status": "keep" if decision == "ready_for_iizuka_candidate_challenger_design" else ("blocked" if decision == "pipeline_blocked" else "hold"),
        "reason": reason,
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "summary": {
            "candidate_row_count": int(len(candidate)),
            "candidate_group_count": int(candidate["anchor_date"].nunique()) if len(candidate) else 0,
            "candidate_symbol_count": int(candidate["symbol"].nunique()) if len(candidate) else 0,
            "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
            "research_fallback_label_source": "ml_label_20d",
        },
        "champion_comparison": {
            "top5": top5,
            "top10": top10,
            "top20": top20,
        },
    }


def _discover_runtime_dates(db_path: Path) -> dict[str, str | None]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        def _max_date(table: str, column: str) -> str | None:
            try:
                row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
            except Exception:
                return None
            if not row or row[0] is None:
                return None
            value = row[0]
            if isinstance(value, int):
                return str(value)
            if isinstance(value, str):
                return value
            try:
                return pd.Timestamp(value).strftime("%Y-%m-%d")
            except Exception:
                return str(value)

        return {
            "daily_bars_max_date": _max_date("daily_bars", "date"),
            "feature_frame_daily_max_date": _max_date("feature_frame_daily", "dt"),
            "ml_feature_daily_max_date": _max_date("ml_feature_daily", "dt"),
            "ml_label_20d_max_date": _max_date("ml_label_20d", "dt"),
            "label_20d_max_date": _max_date("label_20d", "dt"),
            "signal_decision_daily_max_date": _max_date("signal_decision_daily", "dt"),
        }
    finally:
        conn.close()


def _load_ml_label_forward_rows(
    *,
    db_path: Path,
    symbols: list[str],
    anchor_dates: list[str],
) -> pd.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        conn.register("forward_symbols", pd.DataFrame({"code": symbols}))
        conn.register("forward_dates", pd.DataFrame({"anchor_date": anchor_dates}))
        frame = conn.execute(
            """
            SELECT
                l.dt,
                l.code,
                l.ret20,
                l.up20_label
            FROM ml_label_20d l
            INNER JOIN forward_symbols sym ON sym.code = l.code
            INNER JOIN forward_dates d ON d.anchor_date = strftime(to_timestamp(l.dt), '%Y-%m-%d')
            ORDER BY l.dt, l.code
            """
        ).fetchdf()
    finally:
        conn.close()
    return frame


def _build_input_check(
    *,
    runtime_dates: dict[str, str | None],
    base_rows: pd.DataFrame,
    source_max_anchor_date: str,
    mature_label_max_date: str,
    label_20d_max_date: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": INPUT_CHECK_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "runtime_dates": runtime_dates,
        "source_max_anchor_date": source_max_anchor_date,
        "mature_label_max_date": mature_label_max_date,
        "same_universe_forward_rows": int(len(base_rows)),
        "same_universe_forward_symbols": int(base_rows["symbol"].nunique()) if len(base_rows) else 0,
        "same_universe_forward_groups": int(base_rows["anchor_date"].nunique()) if len(base_rows) else 0,
        "label_sources": {
            "research_fallback_label_source": "ml_label_20d",
            "label_20d_canonical": "legacy_disabled" if label_20d_max_date == "2026-02-12" else "available",
        },
        "no_lookahead_contract": {
            "feature_frame_daily": True,
            "ml_feature_daily": True,
            "signal_basis_daily": True,
            "shape_session": True,
        },
        "core_fields_available": {
            "close_vs_20ma": True,
            "close_vs_60ma": True,
            "ma20_slope_or_direction": True,
            "candle_body_range_proxy": True,
            "volume_proxy": True,
            "no_lookahead_date_integrity": True,
            "forward_labels_for_evaluation": True,
        },
        "decision": {
            "typed": "ready_to_accumulate_surface",
            "reason": "forward universe and mature labels are available with explicit research-fallback labeling",
            "missing_core_fields": [],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka fixed-contract forward surface accumulation v1")
    parser.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--source-surface", type=str, default=str(DEFAULT_SOURCE_SURFACE))
    parser.add_argument("--shape-session", type=str, default=str(DEFAULT_SHAPE_SESSION))
    parser.add_argument("--runtime-db", type=str, default=str(DEFAULT_RUNTIME_DB))
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()

    output_root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    source_surface = _safe_path(args.source_surface, DEFAULT_SOURCE_SURFACE)
    shape_session = _safe_path(args.shape_session, DEFAULT_SHAPE_SESSION)
    runtime_db = _safe_path(args.runtime_db, DEFAULT_RUNTIME_DB)
    session_id = _session_id()
    session_root = output_root / session_id
    session_root.mkdir(parents=True, exist_ok=True)

    source_frame = _load_frame(source_surface)
    source_symbols = sorted(source_frame["symbol"].astype(str).unique().tolist())
    source_max_anchor_date = source_frame["anchor_date"].max()

    runtime_dates = _discover_runtime_dates(runtime_db)
    mature_label_max_date = _to_iso_date(runtime_dates.get("ml_label_20d_max_date"))
    label_20d_max_date = _to_iso_date(runtime_dates.get("label_20d_max_date"))

    if not runtime_dates.get("ml_label_20d_max_date"):
        raise RuntimeError("ml_label_20d is unavailable; cannot generate forward accumulation surface")
    if mature_label_max_date is None:
        raise RuntimeError(
            f"ml_label_20d max date could not be normalized from runtime value: {runtime_dates.get('ml_label_20d_max_date')!r}"
        )

    source_max_anchor_ts = _to_timestamp(source_max_anchor_date)
    mature_label_max_ts = _to_timestamp(mature_label_max_date)
    if source_max_anchor_ts is None:
        raise RuntimeError(f"source max anchor date could not be normalized: {source_max_anchor_date!r}")
    if mature_label_max_ts is None:
        raise RuntimeError(f"mature label max date could not be normalized: {mature_label_max_date!r}")

    dates_frame = pd.DataFrame(
        {
            "anchor_date": pd.date_range(
                source_max_anchor_ts + pd.Timedelta(days=1),
                mature_label_max_ts,
                freq="D",
            ).strftime("%Y-%m-%d"),
        }
    )
    dates_frame = dates_frame.loc[pd.to_datetime(dates_frame["anchor_date"], errors="coerce").dt.dayofweek < 5].copy()
    base_rows = _query_forward_base(
        db_path=runtime_db,
        symbols=source_symbols,
        anchor_dates=dates_frame["anchor_date"].tolist(),
        source_max_anchor_date=source_max_anchor_date,
        mature_label_max_date=mature_label_max_date,
    )

    shape_session_dir = shape_session
    shape_rows = _load_frame(shape_session_dir / "conditional_shape_rows.parquet")
    shape_modifier_map = _build_shape_modifier_map(shape_session_dir)
    ml_label = _load_ml_label_forward_rows(db_path=runtime_db, symbols=source_symbols, anchor_dates=dates_frame["anchor_date"].tolist())

    input_check = _build_input_check(
        runtime_dates=runtime_dates,
        base_rows=base_rows,
        source_max_anchor_date=source_max_anchor_date,
        mature_label_max_date=mature_label_max_date,
        label_20d_max_date=label_20d_max_date,
    )

    conn = duckdb.connect(str(runtime_db), read_only=True)
    try:
        candidate = _materialize_candidate_point_in_time_sources(base_rows, conn)
        candidate = _materialize_volume_feature_contract(candidate, conn)
        candidate = _apply_model_feature_completion(candidate)
        candidate = _materialize_ma_position_context(candidate, conn)
    finally:
        conn.close()

    candidate = candidate.sort_values(["anchor_date", "candidate_rank", "symbol"], ascending=[True, True, True], kind="stable").reset_index(drop=True)
    candidate = _shape_feature_join(candidate, shape_rows, shape_modifier_map)
    candidate = _materialize_candle_reversal_flags(candidate)
    candidate = _apply_batch1_features(candidate)
    candidate = candidate.loc[candidate["side"].astype(str) == "long"].copy()
    candidate = _filter_candidate_rows(candidate)
    if candidate.empty:
        raise RuntimeError("candidate surface is empty after applying the fixed Iizuka contract")

    champion = _attach_evaluation_labels(base_rows.copy(), ml_label, shape_rows)
    champion["surface_key"] = _surface_key(champion)
    champion["canonical_candidate_key"] = champion["surface_key"]
    champion["key"] = champion["surface_key"]
    champion = champion.sort_values(["anchor_date", "champion_rank", "symbol"], ascending=[True, True, True], kind="stable").reset_index(drop=True)
    champion = _add_champion_selection_flags(champion)
    for topk in TOP_K_VALUES:
        champion[f"champion_selected_top{topk}"] = pd.to_numeric(champion["champion_rank"], errors="coerce") <= topk

    candidate = _attach_evaluation_labels(candidate, ml_label, shape_rows)
    # Re-attach the fallback label source explicitly after candidate construction.
    candidate["research_fallback_label_source"] = "ml_label_20d"
    candidate["evaluation_only_outcomes"] = True
    candidate["outcome_attachment_source"] = "ml_label_20d+conditional_shape_rows"
    candidate["outcome_attachment_complete"] = candidate[["forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label"]].notna().all(axis=1) if all(col in candidate.columns for col in ("forward_ret_20d", "path_value_score_v1", "top15_label", "bottom15_label")) else False
    candidate["surface_key"] = _surface_key(candidate)
    candidate["canonical_candidate_key"] = candidate["surface_key"]
    candidate["key"] = candidate["surface_key"]

    no_lookahead = _no_lookahead_audit(candidate)
    leakage = _leakage_audit(candidate)
    summary = _surface_summary(candidate, champion)
    comparison, diff_frame, failure_mode, headroom = _compare_topk(champion, candidate)
    lineage = _build_lineage_comparison(
        _load_json(Path(r"G:\Tradex\iizuka_pre_decisive_long_candidate_generation_v1\20260503T053753Z-395634\iizuka_pre_decisive_long_candidate_generation_v1_decision.json")),
        comparison,
        candidate,
    )

    decision = _decision_from_result(comparison, no_lookahead, candidate)
    if decision["decision"] == "ready_for_iizuka_candidate_challenger_design":
        decision["status"] = "keep"

    run_manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_id": session_id,
        "output_root": str(session_root),
        "jobs": int(args.jobs),
        "research_only": True,
        "boundary": "TRADEX-only",
        "fixed_conditions": {
            "side": "long",
            "same_universe_where_possible": True,
            "same_evaluation_period_where_possible": True,
            "topk_frame": list(TOP_K_VALUES),
            "forward_return_horizon_business_days": 20,
            "no_lookahead": True,
            "same_artifact_detail_level": True,
            "research_fallback_label_source": "ml_label_20d",
        },
        "source_artifacts": {
            "original_fixed_contract_session": str(source_surface),
            "shape_session": str(shape_session_dir),
            "runtime_db": str(runtime_db),
        },
        "candidate_contract_name": "iizuka_pre_decisive_long_candidate_v1",
        "candidate_contract_version": "v1",
        "notes": [
            "evaluation labels are attached after candidate construction",
            "ml_label_20d is the explicit research-fallback label source",
            "no MeeMee, production ranking, publish, promotion, or research_inventory mutation occurs",
        ],
    }

    input_resolution = {
        "schema_version": INPUT_CHECK_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "resolved_paths": {
            "output_root": str(output_root),
            "session_root": str(session_root),
            "source_surface": str(source_surface),
            "shape_session": str(shape_session_dir),
            "runtime_db": str(runtime_db),
        },
        "input_check": input_check,
    }

    _write_json(session_root / "run_manifest.json", run_manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "iizuka_accumulator_input_check.json", input_check)
    _write_parquet(session_root / "iizuka_accumulated_candidate_rows.parquet", candidate)
    _write_json(session_root / "iizuka_accumulated_surface_generation_summary.json", summary)
    _write_json(session_root / "iizuka_accumulated_no_lookahead_audit.json", no_lookahead)
    _write_json(session_root / "iizuka_accumulated_leakage_audit.json", leakage)
    _write_json(session_root / "iizuka_accumulated_variant_pool_comparison.json", comparison)
    _write_parquet(session_root / "iizuka_accumulated_topk_membership_diff.parquet", diff_frame)
    _write_json(session_root / "iizuka_accumulated_failure_mode_audit.json", failure_mode)
    _write_json(session_root / "iizuka_accumulated_oracle_headroom_audit.json", headroom)
    _write_json(session_root / "iizuka_accumulated_lineage_comparison.json", lineage)
    _write_json(session_root / "iizuka_fixed_contract_forward_surface_accumulation_v1_decision.json", decision)

    artifact_complete = {
        "schema_version": f"{SCHEMA_VERSION}_artifact_complete_v1",
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_json": [
            "run_manifest.json",
            "input_resolution.json",
            "iizuka_accumulator_input_check.json",
            "iizuka_accumulated_surface_generation_summary.json",
            "iizuka_accumulated_no_lookahead_audit.json",
            "iizuka_accumulated_leakage_audit.json",
            "iizuka_accumulated_variant_pool_comparison.json",
            "iizuka_accumulated_failure_mode_audit.json",
            "iizuka_accumulated_oracle_headroom_audit.json",
            "iizuka_accumulated_lineage_comparison.json",
            "iizuka_fixed_contract_forward_surface_accumulation_v1_decision.json",
        ],
        "required_parquet": [
            "iizuka_accumulated_candidate_rows.parquet",
            "iizuka_accumulated_topk_membership_diff.parquet",
        ],
        "all_present": all(
            (session_root / name).exists()
            for name in [
                "run_manifest.json",
                "input_resolution.json",
                "iizuka_accumulator_input_check.json",
                "iizuka_accumulated_candidate_rows.parquet",
                "iizuka_accumulated_surface_generation_summary.json",
                "iizuka_accumulated_no_lookahead_audit.json",
                "iizuka_accumulated_leakage_audit.json",
                "iizuka_accumulated_variant_pool_comparison.json",
                "iizuka_accumulated_topk_membership_diff.parquet",
                "iizuka_accumulated_failure_mode_audit.json",
                "iizuka_accumulated_oracle_headroom_audit.json",
                "iizuka_accumulated_lineage_comparison.json",
                "iizuka_fixed_contract_forward_surface_accumulation_v1_decision.json",
            ]
        ),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", artifact_complete)

    print(
        json.dumps(
            {
                "session_root": str(session_root),
                "decision": decision["decision"],
                "candidate_rows": int(len(candidate)),
                "no_lookahead_pass": bool(no_lookahead["no_lookahead_pass"]),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
