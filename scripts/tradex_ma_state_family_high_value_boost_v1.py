from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_ma_state_family_stability_filter import (  # noqa: E402
    CONFIRMED_REGIME_SOURCE,
    _classify_family,
    _family_monthly_frame,
    _family_regime_frame,
    _family_summary_frame,
    _streak_bucket_expr,
)


DEFAULT_SOURCE_FAMILY_SESSION = Path(r"G:\Tradex\ma_position_path_research_family_filter\20260429T062945Z-87844c56")
DEFAULT_CANDIDATE_INPUT_DIR = Path(r"G:\Tradex\sample_replays\tradex_integrated_guarded_v1_stress200")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma_state_family_high_value_boost_v1")
DEFAULT_V1_SESSION = Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1\20260429T071723Z-2a858f13")
DEFAULT_V1_1_SESSION = Path(r"G:\Tradex\ma_state_family_bad_pick_pruner_v1_1_narrow_penalty\20260429T072508Z-bf85d2f9")
DEFAULT_LIMIT_ANCHOR_DATES = None

SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1"
COMPARE_SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1_compare_v1"
DECISION_SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1_decision_v1"
MANIFEST_SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1_manifest_v1"
MONTHLY_SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1_monthly_comparison_v1"
REGIME_SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1_regime_comparison_v1"
PENALTY_COVERAGE_SCHEMA_VERSION = "tradex_ma_state_family_high_value_boost_v1_boost_coverage_v1"

TOP_K_VALUES = (5, 10, 20)
HIGH_VALUE_BOOST = 0.06
MAX_TOTAL_BOOST = 0.06
REGIME_BAD_PICK_REGIME_EXTRACTION = "dominant_regime_context"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any, fallback: float | None = None) -> float | None:
    if value is None:
        return fallback
    try:
        out = float(value)
    except Exception:
        return fallback
    return out if math.isfinite(out) else fallback


def _safe_int(value: Any, fallback: int | None = None) -> int | None:
    if value is None:
        return fallback
    try:
        return int(value)
    except Exception:
        return fallback


def _progress_log(message: str) -> None:
    print(f"[ma_state_family_high_value_boost_v1] {message}", file=sys.stderr, flush=True)


def _make_session_id() -> str:
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _resolve_source_family_session(source_family_session: str | Path | None) -> Path:
    if source_family_session and str(source_family_session).strip():
        path = Path(str(source_family_session)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"source family session not found: {path}")
        return path
    if DEFAULT_SOURCE_FAMILY_SESSION.exists():
        return DEFAULT_SOURCE_FAMILY_SESSION.resolve()
    raise FileNotFoundError("Could not resolve source family session. Pass --source-family-session.")


def _resolve_candidate_input_dir(candidate_input_dir: str | Path | None) -> Path:
    if candidate_input_dir and str(candidate_input_dir).strip():
        path = Path(str(candidate_input_dir)).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"candidate input dir not found: {path}")
        return path
    if DEFAULT_CANDIDATE_INPUT_DIR.exists():
        return DEFAULT_CANDIDATE_INPUT_DIR.resolve()
    raise FileNotFoundError("Could not resolve candidate input dir. Pass --candidate-input-dir.")


def _load_source_family_session(source_family_session: Path) -> dict[str, Any]:
    manifest = _load_json(source_family_session / "run_manifest.json")
    family_summary_report = _load_json(source_family_session / "state_family_summary.json")
    decision = _load_json(source_family_session / "state_family_filter_v1_decision.json")
    classification = _load_json(source_family_session / "state_family_classification.json")
    by_regime = _load_json(source_family_session / "state_family_by_regime.json")
    monthly_stability = _load_json(source_family_session / "state_family_monthly_stability.json")
    row_parquet = source_family_session / "state_family_rows.parquet"
    if not row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {row_parquet}")
    source_thresholds = manifest.get("thresholds") or family_summary_report.get("filter_thresholds") or {}
    top15_threshold = _safe_float(source_thresholds.get("top15_score_threshold"))
    bottom15_threshold = _safe_float(source_thresholds.get("bottom15_score_threshold"))
    if top15_threshold is None or bottom15_threshold is None:
        raise RuntimeError("missing top15/bottom15 thresholds from family filter session")
    conn = duckdb.connect()
    try:
        conn.execute(f"CREATE TEMP VIEW family_rows AS SELECT * FROM read_parquet('{row_parquet.as_posix()}')")
        family_summary_frame = _family_summary_frame(
            conn,
            top15_threshold=float(top15_threshold),
            bottom15_threshold=float(bottom15_threshold),
        )
        family_regime_frame = _family_regime_frame(conn)
        family_monthly_frame = _family_monthly_frame(conn)
    finally:
        conn.close()
    family_regime_rollup = (
        family_regime_frame.groupby("state_family_id", dropna=False)
        .apply(
            lambda group: pd.Series(
                {
                    "regime_count": int(group["family_regime_context"].nunique()),
                    "regime_consistency_score": _safe_float(group["sample_count"].max() / group["sample_count"].sum()),
                    "score_spread": _safe_float(group["score_spread"].max()),
                    "dominant_regime_context": str(group.sort_values("sample_count", ascending=False).iloc[0]["family_regime_context"]),
                }
            ),
            include_groups=False,
        )
        .reset_index()
    )
    family_summary_frame = family_summary_frame.merge(family_monthly_frame, on="state_family_id", how="left").merge(
        family_regime_rollup, on="state_family_id", how="left"
    )
    family_summary_frame["family_classification"] = family_summary_frame.apply(_classify_family, axis=1, thresholds={
        "min_sample_count": int(manifest["thresholds"]["min_sample_count"]),
        "min_unique_symbol_count": int(manifest["thresholds"]["min_unique_symbol_count"]),
        "min_month_count": int(manifest["thresholds"]["min_month_count"]),
        "baseline_mean_path_value_score_v1": _safe_float(manifest["thresholds"].get("baseline_mean_path_value_score_v1")),
        "baseline_median_path_value_score_v1": _safe_float(manifest["thresholds"].get("baseline_median_path_value_score_v1")),
        "baseline_plus5_before_minus5_rate": _safe_float(manifest["thresholds"].get("baseline_plus5_before_minus5_rate")),
        "baseline_minus5_before_plus5_rate": _safe_float(manifest["thresholds"].get("baseline_minus5_before_plus5_rate")),
        "baseline_top15_rate": _safe_float(manifest["thresholds"].get("baseline_top15_rate")),
        "baseline_bottom15_rate": _safe_float(manifest["thresholds"].get("baseline_bottom15_rate")),
    })
    family_summary_frame["stable_high_value_family"] = family_summary_frame["family_classification"].eq("stable_high_value_family")
    family_summary_frame = family_summary_frame.rename(
        columns={
            "sample_count": "family_sample_count",
            "unique_symbol_count": "family_unique_symbol_count",
            "month_count": "family_month_count",
            "mean_forward_ret_5d": "family_mean_forward_ret_5d",
            "mean_forward_ret_10d": "family_mean_forward_ret_10d",
            "mean_forward_ret_20d": "family_mean_forward_ret_20d",
            "median_forward_ret_20d": "family_median_forward_ret_20d",
            "mean_mfe_20d": "family_mean_mfe_20d",
            "mean_mae_20d": "family_mean_mae_20d",
            "mean_path_value_score_v1": "family_mean_path_value_score_v1",
            "median_path_value_score_v1": "family_median_path_value_score_v1",
            "plus5_before_minus5_rate": "family_plus5_before_minus5_rate",
            "minus5_before_plus5_rate": "family_minus5_before_plus5_rate",
            "top15_rate": "family_top15_rate",
            "bottom15_rate": "family_bottom15_rate",
            "months_observed": "family_months_observed",
            "positive_month_rate": "family_positive_month_rate",
            "worst_month_mean_path_value": "family_worst_month_mean_path_value",
            "best_month_mean_path_value": "family_best_month_mean_path_value",
            "mean_monthly_path_value": "family_mean_monthly_path_value",
            "std_monthly_path_value": "family_std_monthly_path_value",
            "month_sample_count": "family_month_sample_count",
            "regime_count": "family_regime_count",
            "regime_consistency_score": "family_regime_consistency_score",
            "score_spread": "family_score_spread",
            "dominant_regime_context": "family_bad_pick_regime",
        }
    )
    stable_high_value_family_ids = {
        str(item)
        for item in family_summary_frame.loc[family_summary_frame["stable_high_value_family"], "state_family_id"].astype(str).tolist()
    }
    source_ma_manifest_path = Path(manifest["source_artifacts"]["run_manifest_json"])
    source_ma_manifest = _load_json(source_ma_manifest_path)
    source_ma_row_parquet = Path(manifest["source_artifacts"]["position_state_forward_path_rows_parquet"])
    if not source_ma_row_parquet.exists():
        raise FileNotFoundError(f"missing required source artifact: {source_ma_row_parquet}")
    return {
        "manifest": manifest,
        "decision": decision,
        "family_summary_report": family_summary_report,
        "family_summary_frame": family_summary_frame,
        "classification": classification,
        "by_regime": by_regime,
        "monthly_stability": monthly_stability,
        "row_parquet": row_parquet,
        "source_ma_manifest_path": source_ma_manifest_path,
        "source_ma_session_path": source_ma_manifest_path.parent,
        "source_ma_manifest": source_ma_manifest,
        "source_ma_row_parquet": source_ma_row_parquet,
        "stable_high_value_family_ids": stable_high_value_family_ids,
    }


def _load_v1_session(v1_session: Path) -> dict[str, Any]:
    compare = _load_json(v1_session / "ma_state_family_bad_pick_pruner_v1_compare.json")
    decision = _load_json(v1_session / "ma_state_family_bad_pick_pruner_v1_decision.json")
    coverage = _load_json(v1_session / "penalty_coverage_summary.json")
    return {
        "compare": compare,
        "decision": decision,
        "coverage": coverage,
    }


def _load_v1_1_session(v1_1_session: Path) -> dict[str, Any]:
    compare = _load_json(v1_1_session / "ma_state_family_bad_pick_pruner_v1_1_compare.json")
    decision = _load_json(v1_1_session / "ma_state_family_bad_pick_pruner_v1_1_decision.json")
    coverage = _load_json(v1_1_session / "penalty_coverage_summary.json")
    return {
        "compare": compare,
        "decision": decision,
        "coverage": coverage,
    }


def _load_prior_pruner_session(prior_pruner_session: Path) -> dict[str, Any]:
    compare = _load_json(prior_pruner_session / "ma_state_family_regime_only_bad_pick_pruner_v1_compare.json")
    decision = _load_json(prior_pruner_session / "ma_state_family_regime_only_bad_pick_pruner_v1_decision.json")
    coverage = _load_json(prior_pruner_session / "penalty_coverage_summary.json")
    return {
        "compare": compare,
        "decision": decision,
        "coverage": coverage,
    }


def _load_candidate_rows(candidate_input_dir: Path) -> pd.DataFrame:
    payload = _load_json(candidate_input_dir / "integrated_guarded_v1_candidate_snapshots.json")
    rows = payload.get("rows")
    if isinstance(rows, dict) and "rows" in rows:
        rows = rows["rows"]
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        raise RuntimeError(f"no candidate rows found in {candidate_input_dir}")
    if "anchor_date" not in frame.columns or "symbol" not in frame.columns or "side" not in frame.columns:
        raise RuntimeError("candidate snapshots missing anchor_date/symbol/side")
    if "score" not in frame.columns:
        raise RuntimeError("candidate snapshots missing score")
    if "rank" not in frame.columns:
        raise RuntimeError("candidate snapshots missing rank")
    frame = frame.drop_duplicates(["anchor_date", "symbol", "side"], keep="first").copy()
    frame["candidate_idx"] = range(len(frame))
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame["rank"] = pd.to_numeric(frame["rank"], errors="coerce")
    frame["trade_date"] = pd.to_numeric(frame["anchor_date"].str.replace("-", "", regex=False), errors="coerce").astype("Int64")
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].str.slice(0, 7)
    else:
        frame["month_bucket"] = frame["month_bucket"].astype(str)
    if "market_regime_bucket" not in frame.columns:
        frame["market_regime_bucket"] = "unknown"
    frame["market_regime_bucket"] = frame["market_regime_bucket"].astype(str)
    return frame.reset_index(drop=True)


def _apply_anchor_limit(frame: pd.DataFrame, limit_anchor_dates: int | None) -> pd.DataFrame:
    if limit_anchor_dates is None:
        return frame
    limit = max(0, int(limit_anchor_dates))
    if limit == 0:
        return frame.iloc[0:0].copy()
    anchor_dates = sorted(frame["anchor_date"].dropna().astype(str).unique().tolist())[:limit]
    return frame.loc[frame["anchor_date"].isin(anchor_dates)].copy().reset_index(drop=True)


def _family_sql_expr() -> tuple[str, list[str]]:
    raw_cols = [
        "regexp_extract(position_state_id, 'c7=([^|]+)', 1) AS raw_c7",
        "regexp_extract(position_state_id, 'c20=([^|]+)', 1) AS raw_c20",
        "regexp_extract(position_state_id, 'c60=([^|]+)', 1) AS raw_c60",
        "regexp_extract(position_state_id, 'stk=([^|]+)', 1) AS raw_stk",
        "regexp_extract(position_state_id, 's20=([^|]+)', 1) AS raw_s20",
        "regexp_extract(position_state_id, 's60=([^|]+)', 1) AS raw_s60",
        "regexp_extract(position_state_id, 'st7=([^|]+)', 1) AS raw_st7",
        "regexp_extract(position_state_id, 'st20=([^|]+)', 1) AS raw_st20",
        "regexp_extract(position_state_id, 'st60=([^|]+)', 1) AS raw_st60",
        "regexp_extract(position_state_id, 'cd=([^|]+)', 1) AS raw_cd",
        "regexp_extract(position_state_id, 'p20=([^|]+)', 1) AS raw_p20",
        "regexp_extract(position_state_id, 'p60=([^|]+)', 1) AS raw_p60",
        "regexp_extract(position_state_id, 'vol=([^|]+)', 1) AS raw_vol",
    ]
    family_expr_parts = [
        "raw_c7",
        "raw_c20",
        "raw_c60",
        "raw_stk",
        "raw_s20",
        "raw_s60",
        _streak_bucket_expr("regexp_extract(position_state_id, 'st7=([^|]+)', 1)"),
        _streak_bucket_expr("regexp_extract(position_state_id, 'st20=([^|]+)', 1)"),
        _streak_bucket_expr("regexp_extract(position_state_id, 'st60=([^|]+)', 1)"),
        "family_candle_strength",
        "family_gap_group",
        "family_price_location",
        "raw_vol",
    ]
    family_expr = " || '|' || ".join(family_expr_parts)
    return family_expr, raw_cols


def _build_enriched_candidates(
    *,
    candidate_rows: pd.DataFrame,
    source_ma_row_parquet: Path,
    family_row_parquet: Path,
) -> pd.DataFrame:
    conn = duckdb.connect()
    try:
        conn.register("candidate_input", candidate_rows)
        family_expr, raw_cols = _family_sql_expr()
        raw_cols_sql = ",\n            ".join(raw_cols)
        sql = f"""
        WITH source_candidates AS (
            SELECT
                c.candidate_idx,
                c.anchor_date,
                c.trade_date,
                c.month_bucket,
                c.side,
                c.symbol,
                c.rank,
                c.score,
                c.market_regime_bucket,
                c.champion_score,
                c.champion_rank,
                s.position_state_id,
                s.regime_source,
                s.regime_label,
                CASE WHEN s.regime_source = '{CONFIRMED_REGIME_SOURCE}' THEN 'C:' || s.regime_label ELSE 'P:' || s.regime_label END AS family_regime_context,
                s.entry_next_open,
                s.entry_day_close,
                s.forward_window_days,
                s.candle_state_code,
                s.volume_condition AS source_volume_condition,
                s.forward_ret_3d,
                s.forward_ret_5d,
                s.forward_ret_10d,
                s.forward_ret_20d,
                s.mfe_20d,
                s.mae_20d,
                s.days_to_mfe_20d,
                s.days_to_mae_20d,
                s.days_to_positive_close,
                s.days_to_plus_3pct,
                s.days_to_plus_5pct,
                s.days_to_minus_3pct,
                s.days_to_minus_5pct,
                s.hit_plus_5_before_minus_5,
                s.hit_minus_5_before_plus_5,
                s.hit_plus_3_before_minus_3,
                s.hit_minus_3_before_plus_3,
                s.hit_plus_1atr_before_minus_1atr,
                s.mfe_atr_20d,
                s.mae_atr_20d,
                s.close_above_entry_days_20d,
                s.close_below_entry_days_20d,
                s.path_value_score_v1,
                s.body_norm_atr,
                s.upper_wick_ratio,
                s.lower_wick_ratio,
                s.volume,
                {raw_cols_sql},
                CASE WHEN regexp_extract(s.position_state_id, 'cd=([^|]+)', 1) = 'LBB' THEN 'large_bullish'
                     WHEN regexp_extract(s.position_state_id, 'cd=([^|]+)', 1) = 'LBR' THEN 'large_bearish'
                     ELSE 'neutral_small' END AS family_candle_strength,
                CASE WHEN regexp_extract(s.position_state_id, 'cd=([^|]+)', 1) = 'GU' THEN 'gap_up'
                     WHEN regexp_extract(s.position_state_id, 'cd=([^|]+)', 1) = 'GD' THEN 'gap_down'
                     ELSE 'no_major_gap' END AS family_gap_group,
                CASE WHEN regexp_extract(s.position_state_id, 'p20=([^|]+)', 1) = 'H' AND regexp_extract(s.position_state_id, 'p60=([^|]+)', 1) = 'H' THEN 'near_high'
                     WHEN regexp_extract(s.position_state_id, 'p20=([^|]+)', 1) = 'L' AND regexp_extract(s.position_state_id, 'p60=([^|]+)', 1) = 'L' THEN 'near_low'
                     ELSE 'middle' END AS family_price_location
            FROM candidate_input c
            LEFT JOIN read_parquet('{source_ma_row_parquet.as_posix()}') s
              ON s.trade_date = c.trade_date AND CAST(s.code AS VARCHAR) = c.symbol
        ),
            enriched AS (
                SELECT
                    *,
                    {family_expr} AS state_family_id
                FROM source_candidates
            )
            SELECT
                e.*
            FROM enriched e
            ORDER BY e.candidate_idx
            """
        frame = conn.execute(sql).fetchdf()
    finally:
        conn.close()
    return frame


def _classify_boost(row: pd.Series) -> tuple[float, list[str], bool]:
    if bool(row.get("stable_high_value_family")):
        return float(HIGH_VALUE_BOOST), ["stable_high_value_family", "high_value_boost"], True
    return 0.0, [], False


def _apply_boost_scores(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    boosts = out.apply(_classify_boost, axis=1, result_type="expand")
    out["score_adjustment"] = pd.to_numeric(boosts[0], errors="coerce").fillna(0.0)
    out["score_adjustment_reason"] = boosts[1].map(lambda value: list(value) if isinstance(value, list) else [])
    out["high_value_boost_applied"] = boosts[2].map(lambda value: bool(value))
    out["challenger_score"] = pd.to_numeric(out["score"], errors="coerce").fillna(-1e9) + out["score_adjustment"]
    return out


def _rank_selection(frame: pd.DataFrame, *, score_col: str, prefix: str) -> pd.DataFrame:
    ranked = frame.copy()
    ranked["_sort_score"] = pd.to_numeric(ranked[score_col], errors="coerce").fillna(-1e9)
    ranked = ranked.sort_values(["anchor_date", "side", "_sort_score", "rank", "symbol", "candidate_idx"], ascending=[True, True, False, True, True, True], kind="stable")
    ranked[f"{prefix}_position"] = ranked.groupby(["anchor_date", "side"], sort=False).cumcount() + 1
    for top_k in TOP_K_VALUES:
        ranked[f"{prefix}_selected_top{top_k}"] = ranked[f"{prefix}_position"].le(top_k)
    return ranked.drop(columns=["_sort_score"])


def _aggregate_selected_rows(
    frame: pd.DataFrame,
    *,
    selected_col: str,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    selected = frame.loc[frame[selected_col] == True].copy()  # noqa: E712
    if selected.empty:
        return {
            "selected_count": 0,
            "selected_anchor_count": 0,
            "mean_forward_ret_20d": None,
            "median_forward_ret_20d": None,
            "mean_forward_ret_10d": None,
            "median_forward_ret_10d": None,
            "mean_path_value_score_v1": None,
            "median_path_value_score_v1": None,
            "mean_mfe_20d": None,
            "mean_mae_20d": None,
            "top15_capture_rate": None,
            "top15_contamination_rate": None,
            "bottom15_contamination_rate": None,
            "bad_pick_family_contamination_rate": None,
            "regime_bad_pick_contamination_rate": None,
            "win_rate": None,
        }
    forward_20 = pd.to_numeric(selected["forward_ret_20d"], errors="coerce")
    forward_10 = pd.to_numeric(selected["forward_ret_10d"], errors="coerce")
    score = pd.to_numeric(selected["path_value_score_v1"], errors="coerce")
    mfe = pd.to_numeric(selected["mfe_20d"], errors="coerce")
    mae = pd.to_numeric(selected["mae_20d"], errors="coerce")
    family_classification = selected["family_classification"].fillna("").astype(str)
    bad_pick_family = family_classification.eq("stable_bad_pick_family")
    return {
        "selected_count": int(len(selected)),
        "selected_anchor_count": int(selected["anchor_date"].nunique()),
        "mean_forward_ret_20d": float(forward_20.mean()),
        "median_forward_ret_20d": float(forward_20.median()),
        "mean_forward_ret_10d": float(forward_10.mean()),
        "median_forward_ret_10d": float(forward_10.median()),
        "mean_path_value_score_v1": float(score.mean()),
        "median_path_value_score_v1": float(score.median()),
        "mean_mfe_20d": float(mfe.mean()),
        "mean_mae_20d": float(mae.mean()),
        "top15_capture_rate": float((score >= top15_threshold).mean()),
        "top15_contamination_rate": float((score < top15_threshold).mean()),
        "bottom15_contamination_rate": float((score <= bottom15_threshold).mean()),
        "bad_pick_family_contamination_rate": float(bad_pick_family.mean()),
        "regime_bad_pick_contamination_rate": float(family_classification.eq("regime_dependent_family").mean()),
        "win_rate": float((forward_20 > 0).mean()),
    }


def _selection_diff_keys(frame: pd.DataFrame, *, selected_col: str) -> set[tuple[str, str, str]]:
    selected = frame.loc[frame[selected_col] == True, ["anchor_date", "symbol", "side"]].copy()  # noqa: E712
    return set(map(tuple, selected.astype(str).values.tolist()))


def _build_compare_payload(
    *,
    frame: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
    source_family_session_id: str,
    source_family_session_path: Path,
) -> dict[str, Any]:
    champion_by_topk: dict[str, dict[str, Any]] = {}
    challenger_by_topk: dict[str, dict[str, Any]] = {}
    compare_by_topk: dict[str, dict[str, Any]] = {}

    for top_k in TOP_K_VALUES:
        champ_col = f"champion_selected_top{top_k}"
        chal_col = f"challenger_selected_top{top_k}"
        champion_by_topk[str(top_k)] = _aggregate_selected_rows(
            frame, selected_col=champ_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold
        )
        challenger_by_topk[str(top_k)] = _aggregate_selected_rows(
            frame, selected_col=chal_col, bottom15_threshold=bottom15_threshold, top15_threshold=top15_threshold
        )
        compare_by_topk[str(top_k)] = {
            "selection_only": {
                "champion": champion_by_topk[str(top_k)],
                "challenger": challenger_by_topk[str(top_k)],
            },
            "delta": {
                "mean_forward_ret_20d": None
                if champion_by_topk[str(top_k)]["mean_forward_ret_20d"] is None or challenger_by_topk[str(top_k)]["mean_forward_ret_20d"] is None
                else float(challenger_by_topk[str(top_k)]["mean_forward_ret_20d"] - champion_by_topk[str(top_k)]["mean_forward_ret_20d"]),
                "median_forward_ret_20d": None
                if champion_by_topk[str(top_k)]["median_forward_ret_20d"] is None or challenger_by_topk[str(top_k)]["median_forward_ret_20d"] is None
                else float(challenger_by_topk[str(top_k)]["median_forward_ret_20d"] - champion_by_topk[str(top_k)]["median_forward_ret_20d"]),
                "mean_path_value_score_v1": None
                if champion_by_topk[str(top_k)]["mean_path_value_score_v1"] is None or challenger_by_topk[str(top_k)]["mean_path_value_score_v1"] is None
                else float(challenger_by_topk[str(top_k)]["mean_path_value_score_v1"] - champion_by_topk[str(top_k)]["mean_path_value_score_v1"]),
                "median_path_value_score_v1": None
                if champion_by_topk[str(top_k)]["median_path_value_score_v1"] is None or challenger_by_topk[str(top_k)]["median_path_value_score_v1"] is None
                else float(challenger_by_topk[str(top_k)]["median_path_value_score_v1"] - champion_by_topk[str(top_k)]["median_path_value_score_v1"]),
                "top15_capture_rate": None
                if champion_by_topk[str(top_k)]["top15_capture_rate"] is None or challenger_by_topk[str(top_k)]["top15_capture_rate"] is None
                else float(challenger_by_topk[str(top_k)]["top15_capture_rate"] - champion_by_topk[str(top_k)]["top15_capture_rate"]),
                "top15_contamination_rate": None
                if champion_by_topk[str(top_k)]["top15_contamination_rate"] is None or challenger_by_topk[str(top_k)]["top15_contamination_rate"] is None
                else float(challenger_by_topk[str(top_k)]["top15_contamination_rate"] - champion_by_topk[str(top_k)]["top15_contamination_rate"]),
                "bottom15_contamination_rate": None
                if champion_by_topk[str(top_k)]["bottom15_contamination_rate"] is None or challenger_by_topk[str(top_k)]["bottom15_contamination_rate"] is None
                else float(challenger_by_topk[str(top_k)]["bottom15_contamination_rate"] - champion_by_topk[str(top_k)]["bottom15_contamination_rate"]),
                "bad_pick_family_contamination_rate": None
                if champion_by_topk[str(top_k)]["bad_pick_family_contamination_rate"] is None or challenger_by_topk[str(top_k)]["bad_pick_family_contamination_rate"] is None
                else float(challenger_by_topk[str(top_k)]["bad_pick_family_contamination_rate"] - champion_by_topk[str(top_k)]["bad_pick_family_contamination_rate"]),
                "regime_bad_pick_contamination_rate": None
                if champion_by_topk[str(top_k)]["regime_bad_pick_contamination_rate"] is None or challenger_by_topk[str(top_k)]["regime_bad_pick_contamination_rate"] is None
                else float(challenger_by_topk[str(top_k)]["regime_bad_pick_contamination_rate"] - champion_by_topk[str(top_k)]["regime_bad_pick_contamination_rate"]),
                "win_rate": None
                if champion_by_topk[str(top_k)]["win_rate"] is None or challenger_by_topk[str(top_k)]["win_rate"] is None
                else float(challenger_by_topk[str(top_k)]["win_rate"] - champion_by_topk[str(top_k)]["win_rate"]),
            },
        }

    champion_top10_keys = _selection_diff_keys(frame, selected_col="champion_selected_top10")
    challenger_top10_keys = _selection_diff_keys(frame, selected_col="challenger_selected_top10")
    champion_top20_keys = _selection_diff_keys(frame, selected_col="champion_selected_top20")
    challenger_top20_keys = _selection_diff_keys(frame, selected_col="challenger_selected_top20")
    champion_top5_keys = _selection_diff_keys(frame, selected_col="champion_selected_top5")
    challenger_top5_keys = _selection_diff_keys(frame, selected_col="challenger_selected_top5")

    intersection_top5 = len(champion_top5_keys & challenger_top5_keys)
    intersection_top10 = len(champion_top10_keys & challenger_top10_keys)
    intersection_top20 = len(champion_top20_keys & challenger_top20_keys)
    union_top5 = len(champion_top5_keys | challenger_top5_keys)
    union_top10 = len(champion_top10_keys | challenger_top10_keys)
    union_top20 = len(champion_top20_keys | challenger_top20_keys)

    changed_top5_members_count = len(champion_top5_keys ^ challenger_top5_keys)
    changed_top10_members_count = len(champion_top10_keys ^ challenger_top10_keys)
    changed_top20_members_count = len(champion_top20_keys ^ challenger_top20_keys)

    rank_changes = frame.loc[
        frame["champion_selected_top20"].fillna(False).astype(bool) & frame["challenger_selected_top20"].fillna(False).astype(bool),
        ["anchor_date", "symbol", "side", "champion_position", "challenger_position"],
    ].copy()
    if rank_changes.empty:
        changed_rank_count = 0
    else:
        changed_rank_count = int(
            (
                pd.to_numeric(rank_changes["champion_position"], errors="coerce").astype("Int64")
                != pd.to_numeric(rank_changes["challenger_position"], errors="coerce").astype("Int64")
            ).sum()
        )

    month_summary_rows = []
    zero_pass_months = 0
    months = sorted(frame["month_bucket"].dropna().astype(str).unique().tolist())
    for month in months:
        month_frame = frame.loc[frame["month_bucket"].astype(str) == month].copy()
        champ_sel = month_frame.loc[month_frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
        chal_sel = month_frame.loc[month_frame["challenger_selected_top20"].fillna(False).astype(bool)].copy()
        if chal_sel.empty:
            zero_pass_months += 1
        champ_metric = _aggregate_selected_rows(
            month_frame,
            selected_col="champion_selected_top20",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
        chal_metric = _aggregate_selected_rows(
            month_frame,
            selected_col="challenger_selected_top20",
            bottom15_threshold=bottom15_threshold,
            top15_threshold=top15_threshold,
        )
        delta_forward = None if champ_metric["mean_forward_ret_20d"] is None or chal_metric["mean_forward_ret_20d"] is None else float(chal_metric["mean_forward_ret_20d"] - champ_metric["mean_forward_ret_20d"])
        month_summary_rows.append(
            {
                "month_bucket": month,
                "champion_selected_count": champ_metric["selected_count"],
                "challenger_selected_count": chal_metric["selected_count"],
                "champion_mean_forward_ret_20d": champ_metric["mean_forward_ret_20d"],
                "challenger_mean_forward_ret_20d": chal_metric["mean_forward_ret_20d"],
                "delta_mean_forward_ret_20d": delta_forward,
                "champion_mean_path_value_score_v1": champ_metric["mean_path_value_score_v1"],
                "challenger_mean_path_value_score_v1": chal_metric["mean_path_value_score_v1"],
                "delta_mean_path_value_score_v1": None
                if champ_metric["mean_path_value_score_v1"] is None or chal_metric["mean_path_value_score_v1"] is None
                else float(chal_metric["mean_path_value_score_v1"] - champ_metric["mean_path_value_score_v1"]),
                "champion_bottom15_contamination_rate": champ_metric["bottom15_contamination_rate"],
                "challenger_bottom15_contamination_rate": chal_metric["bottom15_contamination_rate"],
                "delta_bottom15_contamination_rate": None
                if champ_metric["bottom15_contamination_rate"] is None or chal_metric["bottom15_contamination_rate"] is None
                else float(chal_metric["bottom15_contamination_rate"] - champ_metric["bottom15_contamination_rate"]),
                "champion_bad_pick_family_contamination_rate": champ_metric["bad_pick_family_contamination_rate"],
                "challenger_bad_pick_family_contamination_rate": chal_metric["bad_pick_family_contamination_rate"],
                "delta_bad_pick_family_contamination_rate": None
                if champ_metric["bad_pick_family_contamination_rate"] is None or chal_metric["bad_pick_family_contamination_rate"] is None
                else float(chal_metric["bad_pick_family_contamination_rate"] - champ_metric["bad_pick_family_contamination_rate"]),
            }
        )

    monthly_wins = int(sum(1 for row in month_summary_rows if row["delta_mean_forward_ret_20d"] is not None and row["delta_mean_forward_ret_20d"] > 0))
    monthly_losses = int(sum(1 for row in month_summary_rows if row["delta_mean_forward_ret_20d"] is not None and row["delta_mean_forward_ret_20d"] < 0))
    monthly_flats = int(sum(1 for row in month_summary_rows if row["delta_mean_forward_ret_20d"] is not None and row["delta_mean_forward_ret_20d"] == 0))
    worst_month_delta = None
    best_month_delta = None
    if month_summary_rows:
        deltas = [row["delta_mean_forward_ret_20d"] for row in month_summary_rows if row["delta_mean_forward_ret_20d"] is not None]
        if deltas:
            worst_month_delta = float(min(deltas))
            best_month_delta = float(max(deltas))

    topk_summary = {
        "top5_overlap_ratio": None if union_top5 == 0 else float(intersection_top5 / union_top5),
        "top10_overlap_ratio": None if union_top10 == 0 else float(intersection_top10 / union_top10),
        "top20_overlap_ratio": None if union_top20 == 0 else float(intersection_top20 / union_top20),
        "turnover_proxy": None if union_top20 == 0 else float((changed_top10_members_count + changed_top20_members_count) / max(1, union_top10 + union_top20)),
        "changed_top5_members_count": changed_top5_members_count,
        "changed_top10_members_count": changed_top10_members_count,
        "changed_top20_members_count": changed_top20_members_count,
        "changed_rank_count": changed_rank_count,
        "selection_divergence_reason": "high_value_boost_v1_vs_champion",
        "zero_pass_months": zero_pass_months,
        "monthly_wins": monthly_wins,
        "monthly_losses": monthly_losses,
        "monthly_flats": monthly_flats,
        "worst_month_delta_mean_forward_ret_20d": worst_month_delta,
        "best_month_delta_mean_forward_ret_20d": best_month_delta,
    }

    return {
        "schema_version": COMPARE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family_session_id,
        "source_family_session_path": str(source_family_session_path),
        "same_condition_contract": {
            "universe": "integrated_guarded_v1_candidate_snapshots",
            "period": "matched candidate anchor dates from the verified stress200 compare path",
            "top_k_values": list(TOP_K_VALUES),
            "cost_slippage": "inherited from source compare path; not changed in pruner",
            "artifact_detail_level": "summary json + parquet diff rows",
            "no_silent_fallback": True,
        },
        "boost_formula": {
            "stable_high_value_family_boost": HIGH_VALUE_BOOST,
            "boost_cap": MAX_TOTAL_BOOST,
            "family_high_value_definition": "stable_high_value_family from the verified family filter session",
            "note": "boost uses stable_high_value_family derived from the family row artifact",
        },
        "boost_coverage": {},
        "champion_vs_challenger": {
            "selection_only": compare_by_topk,
            "branching_metrics": topk_summary,
        },
        "topk_summary": topk_summary,
        "candidate_universe": {
            "row_count": int(len(frame)),
            "anchor_count": int(frame["anchor_date"].nunique()),
            "side_count": int(frame["side"].nunique()),
            "market_regime_bucket_count": int(frame["market_regime_bucket"].nunique()),
        },
        "selection_only_edge_preserved": bool(
            compare_by_topk["10"]["delta"]["mean_forward_ret_20d"] is not None
            and compare_by_topk["20"]["delta"]["mean_forward_ret_20d"] is not None
            and compare_by_topk["10"]["delta"]["mean_forward_ret_20d"] >= 0
            and compare_by_topk["20"]["delta"]["mean_forward_ret_20d"] >= 0
        ),
    }


def _build_monthly_comparison(frame: pd.DataFrame, *, bottom15_threshold: float, top15_threshold: float) -> dict[str, Any]:
    by_topk: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        champ_col = f"champion_selected_top{top_k}"
        chal_col = f"challenger_selected_top{top_k}"
        rows = []
        for month in sorted(frame["month_bucket"].dropna().astype(str).unique().tolist()):
            month_frame = frame.loc[frame["month_bucket"].astype(str) == month].copy()
            champ = _aggregate_selected_rows(
                month_frame,
                selected_col=champ_col,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            chal = _aggregate_selected_rows(
                month_frame,
                selected_col=chal_col,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            rows.append(
                {
                    "month_bucket": month,
                    "champion": champ,
                    "challenger": chal,
                    "delta": {
                        "mean_forward_ret_20d": None
                        if champ["mean_forward_ret_20d"] is None or chal["mean_forward_ret_20d"] is None
                        else float(chal["mean_forward_ret_20d"] - champ["mean_forward_ret_20d"]),
                        "mean_path_value_score_v1": None
                        if champ["mean_path_value_score_v1"] is None or chal["mean_path_value_score_v1"] is None
                        else float(chal["mean_path_value_score_v1"] - champ["mean_path_value_score_v1"]),
                        "bottom15_contamination_rate": None
                        if champ["bottom15_contamination_rate"] is None or chal["bottom15_contamination_rate"] is None
                        else float(chal["bottom15_contamination_rate"] - champ["bottom15_contamination_rate"]),
                        "bad_pick_family_contamination_rate": None
                        if champ["bad_pick_family_contamination_rate"] is None or chal["bad_pick_family_contamination_rate"] is None
                        else float(chal["bad_pick_family_contamination_rate"] - champ["bad_pick_family_contamination_rate"]),
                    },
                }
            )
        deltas = [row["delta"]["mean_forward_ret_20d"] for row in rows if row["delta"]["mean_forward_ret_20d"] is not None]
        by_topk[str(top_k)] = {
            "schema_version": MONTHLY_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "top_k": top_k,
            "rows": rows,
            "summary": {
                "month_count": int(len(rows)),
                "win_month_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] > 0)),
                "loss_month_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] < 0)),
                "flat_month_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] == 0)),
                "zero_pass_months": int(sum(1 for row in rows if row["challenger"]["selected_count"] == 0)),
                "worst_month_delta_mean_forward_ret_20d": None if not deltas else float(min(deltas)),
                "best_month_delta_mean_forward_ret_20d": None if not deltas else float(max(deltas)),
            },
        }
    return {
        "schema_version": MONTHLY_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "rows": by_topk,
    }


def _build_regime_comparison(frame: pd.DataFrame, *, bottom15_threshold: float, top15_threshold: float) -> dict[str, Any]:
    by_topk: dict[str, Any] = {}
    regime_col = "dominant_regime_context"
    if regime_col not in frame.columns:
        regime_col = "family_regime_context"
    for top_k in TOP_K_VALUES:
        champ_col = f"champion_selected_top{top_k}"
        chal_col = f"challenger_selected_top{top_k}"
        rows = []
        for regime in sorted(frame[regime_col].fillna("unknown").astype(str).unique().tolist()):
            regime_frame = frame.loc[frame[regime_col].fillna("unknown").astype(str) == regime].copy()
            champ = _aggregate_selected_rows(
                regime_frame,
                selected_col=champ_col,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            chal = _aggregate_selected_rows(
                regime_frame,
                selected_col=chal_col,
                bottom15_threshold=bottom15_threshold,
                top15_threshold=top15_threshold,
            )
            rows.append(
                {
                    "regime_context": regime,
                    "champion": champ,
                    "challenger": chal,
                    "delta": {
                        "mean_forward_ret_20d": None
                        if champ["mean_forward_ret_20d"] is None or chal["mean_forward_ret_20d"] is None
                        else float(chal["mean_forward_ret_20d"] - champ["mean_forward_ret_20d"]),
                        "mean_path_value_score_v1": None
                        if champ["mean_path_value_score_v1"] is None or chal["mean_path_value_score_v1"] is None
                        else float(chal["mean_path_value_score_v1"] - champ["mean_path_value_score_v1"]),
                        "bottom15_contamination_rate": None
                        if champ["bottom15_contamination_rate"] is None or chal["bottom15_contamination_rate"] is None
                        else float(chal["bottom15_contamination_rate"] - champ["bottom15_contamination_rate"]),
                        "bad_pick_family_contamination_rate": None
                        if champ["bad_pick_family_contamination_rate"] is None or chal["bad_pick_family_contamination_rate"] is None
                        else float(chal["bad_pick_family_contamination_rate"] - champ["bad_pick_family_contamination_rate"]),
                    },
                }
            )
        deltas = [row["delta"]["mean_forward_ret_20d"] for row in rows if row["delta"]["mean_forward_ret_20d"] is not None]
        by_topk[str(top_k)] = {
            "schema_version": REGIME_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "top_k": top_k,
            "rows": rows,
            "summary": {
                "regime_count": int(len(rows)),
                "win_regime_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] > 0)),
                "loss_regime_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] < 0)),
                "flat_regime_count": int(sum(1 for row in rows if row["delta"]["mean_forward_ret_20d"] is not None and row["delta"]["mean_forward_ret_20d"] == 0)),
                "worst_regime_delta_mean_forward_ret_20d": None if not deltas else float(min(deltas)),
                "best_regime_delta_mean_forward_ret_20d": None if not deltas else float(max(deltas)),
            },
        }
    return {
        "schema_version": REGIME_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "rows": by_topk,
    }


def _build_penalty_coverage_summary(frame: pd.DataFrame) -> dict[str, Any]:
    matched_source = frame["position_state_id"].notna().sum() if "position_state_id" in frame.columns else 0
    matched_family = frame["state_family_id"].notna().sum() if "state_family_id" in frame.columns else 0
    unmatched_source = int(len(frame) - matched_source)
    unmatched_family = int(len(frame) - matched_family)
    boost_applied = int((frame["score_adjustment"] > 0).sum())
    boost_match = int(frame["high_value_boost_applied"].fillna(False).astype(bool).sum())
    by_regime = frame.copy()
    regime_col = "family_regime_context" if "family_regime_context" in by_regime.columns else "dominant_regime_context"
    by_regime[regime_col] = by_regime[regime_col].fillna("unknown").astype(str)
    candidate_rows_by_regime = {
        regime: int((by_regime[regime_col] == regime).sum())
        for regime in sorted(by_regime[regime_col].unique().tolist())
    }
    boosted_rows_by_regime = {
        regime: int(((by_regime[regime_col] == regime) & (by_regime["score_adjustment"] > 0)).sum())
        for regime in sorted(by_regime[regime_col].unique().tolist())
    }
    shadow_counts = {
        "stable_high_value_family": int(frame["stable_high_value_family"].fillna(False).astype(bool).sum()) if "stable_high_value_family" in frame.columns else 0,
    }
    return {
        "schema_version": PENALTY_COVERAGE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "candidate_rows": int(len(frame)),
        "matched_source_rows": int(matched_source),
        "unmatched_source_rows": unmatched_source,
        "matched_family_rows": int(matched_family),
        "unmatched_family_rows": unmatched_family,
        "matched_source_rate": None if len(frame) == 0 else float(matched_source / len(frame)),
        "matched_family_rate": None if len(frame) == 0 else float(matched_family / len(frame)),
        "boost_applied_rows": boost_applied,
        "boost_match_rows": boost_match,
        "boost_applied_rate": None if len(frame) == 0 else float(boost_applied / len(frame)),
        "max_total_boost": MAX_TOTAL_BOOST,
        "shadow_counts": shadow_counts,
        "family_high_value_definition": "stable_high_value_family from the verified family filter session",
        "coverage_by_side": {
            side: int(((frame["side"].astype(str) == side) & frame["state_family_id"].notna()).sum())
            for side in sorted(frame["side"].astype(str).unique().tolist())
        },
        "coverage_by_month": {
            month: int(((frame["month_bucket"].astype(str) == month) & frame["state_family_id"].notna()).sum())
            for month in sorted(frame["month_bucket"].astype(str).unique().tolist())
        },
        "candidate_rows_by_regime": candidate_rows_by_regime,
        "boosted_rows_by_regime": boosted_rows_by_regime,
    }


def _build_decision_payload(
    *,
    source_family_session_id: str,
    source_family_session_path: Path,
    source_artifacts: dict[str, str],
    compare_payload: dict[str, Any],
    coverage_payload: dict[str, Any],
    monthly_payload: dict[str, Any],
    regime_payload: dict[str, Any],
    frame: pd.DataFrame,
    bottom15_threshold: float,
    top15_threshold: float,
) -> dict[str, Any]:
    compare_topk = compare_payload["champion_vs_challenger"]["selection_only"]
    branching = compare_payload["champion_vs_challenger"]["branching_metrics"]
    top10_delta = compare_topk["10"]["delta"]["mean_forward_ret_20d"]
    top20_delta = compare_topk["20"]["delta"]["mean_forward_ret_20d"]
    bad_pick_delta_top10 = compare_topk["10"]["delta"]["bad_pick_family_contamination_rate"]
    bad_pick_delta_top20 = compare_topk["20"]["delta"]["bad_pick_family_contamination_rate"]
    bottom15_delta_top10 = compare_topk["10"]["delta"]["bottom15_contamination_rate"]
    bottom15_delta_top20 = compare_topk["20"]["delta"]["bottom15_contamination_rate"]
    coverage_rate = _safe_float(coverage_payload.get("matched_family_rate"), 0.0) or 0.0
    monthly_summary_top20 = monthly_payload["rows"]["20"]["summary"]
    regime_summary_top20 = regime_payload["rows"]["20"]["summary"]
    monthly_win_loss_balance = int(monthly_summary_top20.get("win_month_count", 0)) - int(monthly_summary_top20.get("loss_month_count", 0))
    regime_win_loss_balance = int(regime_summary_top20.get("win_regime_count", 0)) - int(regime_summary_top20.get("loss_regime_count", 0))
    contamination_improved = bool(
        (bad_pick_delta_top10 is not None and bad_pick_delta_top10 < 0)
        or (bad_pick_delta_top20 is not None and bad_pick_delta_top20 < 0)
        or (bottom15_delta_top10 is not None and bottom15_delta_top10 < 0)
        or (bottom15_delta_top20 is not None and bottom15_delta_top20 < 0)
    )
    path_quality_flat_or_better = bool(
        (top10_delta is None or top10_delta >= -0.0005)
        and (top20_delta is None or top20_delta >= -0.0005)
        and (
            compare_topk["10"]["delta"]["mean_path_value_score_v1"] is None
            or compare_topk["10"]["delta"]["mean_path_value_score_v1"] >= -0.0005
        )
        and (
            compare_topk["20"]["delta"]["mean_path_value_score_v1"] is None
            or compare_topk["20"]["delta"]["mean_path_value_score_v1"] >= -0.0005
        )
    )
    controlled_turnover = bool(
        (branching["top10_overlap_ratio"] is not None and branching["top10_overlap_ratio"] >= 0.70)
        and (branching["top20_overlap_ratio"] is not None and branching["top20_overlap_ratio"] >= 0.70)
    )
    monthly_stable = bool(monthly_win_loss_balance >= 0 and monthly_summary_top20.get("worst_month_delta_mean_forward_ret_20d") is not None and monthly_summary_top20["worst_month_delta_mean_forward_ret_20d"] >= -0.03)
    regime_stable = bool(regime_win_loss_balance >= 0 and regime_summary_top20.get("worst_regime_delta_mean_forward_ret_20d") is not None and regime_summary_top20["worst_regime_delta_mean_forward_ret_20d"] >= -0.03)

    if coverage_rate < 0.90:
        recommendation = "hold"
        reason = "join_coverage_incomplete"
    elif contamination_improved and path_quality_flat_or_better and controlled_turnover and monthly_stable and regime_stable:
        recommendation = "keep"
        reason = "high_value_boost_improves_topk_without_material_path_regression"
    elif contamination_improved and (path_quality_flat_or_better or controlled_turnover):
        recommendation = "hold"
        reason = "boost_improves_some_topk_metrics_but_results_remain_mixed"
    else:
        recommendation = "drop"
        reason = "high_value_boost_did_not_improve_topk_practical_quality"

    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "source_family_session_id": source_family_session_id,
        "source_family_session_path": str(source_family_session_path),
        "source_artifacts": source_artifacts,
        "high_value_family_definition": "stable_high_value_family from the verified family filter session",
        "high_value_family_definition_provenance": "confirmed from state_family_filter_v1_decision.json and state_family_classification.json",
        "state_family_id_contract": "frozen from the verified family filter session; this challenger only changes score boosts",
        "boost_formula": compare_payload["boost_formula"],
        "same_condition_contract": compare_payload["same_condition_contract"],
        "coverage_summary": {
            "matched_family_rate": coverage_rate,
            "matched_source_rate": _safe_float(coverage_payload.get("matched_source_rate")),
            "unmatched_family_rows": int(coverage_payload.get("unmatched_family_rows") or 0),
            "unmatched_source_rows": int(coverage_payload.get("unmatched_source_rows") or 0),
        },
        "topk_brief": {
            "top5": compare_topk["5"],
            "top10": compare_topk["10"],
            "top20": compare_topk["20"],
            "branching_metrics": branching,
        },
        "monthly_summary_top20": monthly_summary_top20,
        "regime_summary_top20": regime_summary_top20,
        "thresholds": {
            "bottom15_score_threshold": bottom15_threshold,
            "top15_score_threshold": top15_threshold,
        },
        "recommendation": recommendation,
        "authoritative_rollup_decision": recommendation,
        "typed_reasons": [reason],
        "keep_candidate": recommendation == "keep",
        "hold_candidate": recommendation == "hold",
        "drop_candidate": recommendation == "drop",
        "pruning_challenger_justified": bool(recommendation in {"keep", "hold"} and contamination_improved),
        "monthly_win_loss_balance_top20": monthly_win_loss_balance,
        "regime_win_loss_balance_top20": regime_win_loss_balance,
    }


def _build_v1_v1_1_risk_on_trend_delta_payload(
    *,
    current_compare_payload: dict[str, Any],
    current_decision_payload: dict[str, Any],
    current_coverage_payload: dict[str, Any],
    v1_compare_payload: dict[str, Any],
    v1_decision_payload: dict[str, Any],
    v1_coverage_payload: dict[str, Any],
    v1_session_id: str,
    v1_session_path: Path,
    v1_1_compare_payload: dict[str, Any],
    v1_1_decision_payload: dict[str, Any],
    v1_1_coverage_payload: dict[str, Any],
    v1_1_session_id: str,
    v1_1_session_path: Path,
) -> dict[str, Any]:
    def _topk_entry(payload: dict[str, Any], top_k: str) -> dict[str, Any]:
        return payload["champion_vs_challenger"]["selection_only"][top_k]

    topk_deltas: dict[str, Any] = {}
    for top_k in ("5", "10", "20"):
        current_entry = _topk_entry(current_compare_payload, top_k)
        v1_entry = _topk_entry(v1_compare_payload, top_k)
        topk_deltas[top_k] = {
            "current_vs_champion": current_entry["delta"],
            "v1_vs_champion": v1_entry["delta"],
            "delta_vs_v1": {
                "mean_forward_ret_20d": None
                if current_entry["delta"]["mean_forward_ret_20d"] is None or v1_entry["delta"]["mean_forward_ret_20d"] is None
                else float(current_entry["delta"]["mean_forward_ret_20d"] - v1_entry["delta"]["mean_forward_ret_20d"]),
                "median_forward_ret_20d": None
                if current_entry["delta"]["median_forward_ret_20d"] is None or v1_entry["delta"]["median_forward_ret_20d"] is None
                else float(current_entry["delta"]["median_forward_ret_20d"] - v1_entry["delta"]["median_forward_ret_20d"]),
                "mean_path_value_score_v1": None
                if current_entry["delta"]["mean_path_value_score_v1"] is None or v1_entry["delta"]["mean_path_value_score_v1"] is None
                else float(current_entry["delta"]["mean_path_value_score_v1"] - v1_entry["delta"]["mean_path_value_score_v1"]),
                "median_path_value_score_v1": None
                if current_entry["delta"]["median_path_value_score_v1"] is None or v1_entry["delta"]["median_path_value_score_v1"] is None
                else float(current_entry["delta"]["median_path_value_score_v1"] - v1_entry["delta"]["median_path_value_score_v1"]),
                "bottom15_contamination_rate": None
                if current_entry["delta"]["bottom15_contamination_rate"] is None or v1_entry["delta"]["bottom15_contamination_rate"] is None
                else float(current_entry["delta"]["bottom15_contamination_rate"] - v1_entry["delta"]["bottom15_contamination_rate"]),
                "bad_pick_family_contamination_rate": None
                if current_entry["delta"]["bad_pick_family_contamination_rate"] is None or v1_entry["delta"]["bad_pick_family_contamination_rate"] is None
                else float(current_entry["delta"]["bad_pick_family_contamination_rate"] - v1_entry["delta"]["bad_pick_family_contamination_rate"]),
                "regime_bad_pick_contamination_rate": None
                if current_entry["delta"]["regime_bad_pick_contamination_rate"] is None or v1_entry["delta"]["regime_bad_pick_contamination_rate"] is None
                else float(current_entry["delta"]["regime_bad_pick_contamination_rate"] - v1_entry["delta"]["regime_bad_pick_contamination_rate"]),
                "win_rate": None
                if current_entry["delta"]["win_rate"] is None or v1_entry["delta"]["win_rate"] is None
                else float(current_entry["delta"]["win_rate"] - v1_entry["delta"]["win_rate"]),
            },
        }

    branching_vs_v1 = {
        "changed_top5_members_count_delta": int(
            current_compare_payload["topk_summary"]["changed_top5_members_count"] - v1_compare_payload["topk_summary"]["changed_top5_members_count"]
        ),
        "changed_top10_members_count_delta": int(
            current_compare_payload["topk_summary"]["changed_top10_members_count"] - v1_compare_payload["topk_summary"]["changed_top10_members_count"]
        ),
        "changed_top20_members_count_delta": int(
            current_compare_payload["topk_summary"]["changed_top20_members_count"] - v1_compare_payload["topk_summary"]["changed_top20_members_count"]
        ),
        "changed_rank_count_delta": int(
            current_compare_payload["topk_summary"]["changed_rank_count"] - v1_compare_payload["topk_summary"]["changed_rank_count"]
        ),
        "top5_overlap_ratio_delta": None
        if current_compare_payload["topk_summary"]["top5_overlap_ratio"] is None or v1_compare_payload["topk_summary"]["top5_overlap_ratio"] is None
        else float(current_compare_payload["topk_summary"]["top5_overlap_ratio"] - v1_compare_payload["topk_summary"]["top5_overlap_ratio"]),
        "top10_overlap_ratio_delta": None
        if current_compare_payload["topk_summary"]["top10_overlap_ratio"] is None or v1_compare_payload["topk_summary"]["top10_overlap_ratio"] is None
        else float(current_compare_payload["topk_summary"]["top10_overlap_ratio"] - v1_compare_payload["topk_summary"]["top10_overlap_ratio"]),
        "top20_overlap_ratio_delta": None
        if current_compare_payload["topk_summary"]["top20_overlap_ratio"] is None or v1_compare_payload["topk_summary"]["top20_overlap_ratio"] is None
        else float(current_compare_payload["topk_summary"]["top20_overlap_ratio"] - v1_compare_payload["topk_summary"]["top20_overlap_ratio"]),
    }

    current_monthly_top20 = current_decision_payload["monthly_summary_top20"]
    v1_monthly_top20 = v1_decision_payload["monthly_summary_top20"]
    current_regime_top20 = current_decision_payload["regime_summary_top20"]
    v1_regime_top20 = v1_decision_payload["regime_summary_top20"]

    monthly_vs_v1 = {
        "win_month_count_delta": int(current_monthly_top20.get("win_month_count", 0) - v1_monthly_top20.get("win_month_count", 0)),
        "loss_month_count_delta": int(current_monthly_top20.get("loss_month_count", 0) - v1_monthly_top20.get("loss_month_count", 0)),
        "flat_month_count_delta": int(current_monthly_top20.get("flat_month_count", 0) - v1_monthly_top20.get("flat_month_count", 0)),
        "zero_pass_months_delta": int(current_monthly_top20.get("zero_pass_months", 0) - v1_monthly_top20.get("zero_pass_months", 0)),
        "worst_month_delta_mean_forward_ret_20d_delta": None
        if current_monthly_top20.get("worst_month_delta_mean_forward_ret_20d") is None
        or v1_monthly_top20.get("worst_month_delta_mean_forward_ret_20d") is None
        else float(
            current_monthly_top20["worst_month_delta_mean_forward_ret_20d"]
            - v1_monthly_top20["worst_month_delta_mean_forward_ret_20d"]
        ),
        "best_month_delta_mean_forward_ret_20d_delta": None
        if current_monthly_top20.get("best_month_delta_mean_forward_ret_20d") is None
        or v1_monthly_top20.get("best_month_delta_mean_forward_ret_20d") is None
        else float(
            current_monthly_top20["best_month_delta_mean_forward_ret_20d"]
            - v1_monthly_top20["best_month_delta_mean_forward_ret_20d"]
        ),
    }

    v1_1_monthly_top20 = v1_1_decision_payload["monthly_summary_top20"]
    monthly_vs_v1_1 = {
        "win_month_count_delta": int(current_monthly_top20.get("win_month_count", 0) - v1_1_monthly_top20.get("win_month_count", 0)),
        "loss_month_count_delta": int(current_monthly_top20.get("loss_month_count", 0) - v1_1_monthly_top20.get("loss_month_count", 0)),
        "flat_month_count_delta": int(current_monthly_top20.get("flat_month_count", 0) - v1_1_monthly_top20.get("flat_month_count", 0)),
        "zero_pass_months_delta": int(current_monthly_top20.get("zero_pass_months", 0) - v1_1_monthly_top20.get("zero_pass_months", 0)),
        "worst_month_delta_mean_forward_ret_20d_delta": None
        if current_monthly_top20.get("worst_month_delta_mean_forward_ret_20d") is None
        or v1_1_monthly_top20.get("worst_month_delta_mean_forward_ret_20d") is None
        else float(
            current_monthly_top20["worst_month_delta_mean_forward_ret_20d"]
            - v1_1_monthly_top20["worst_month_delta_mean_forward_ret_20d"]
        ),
        "best_month_delta_mean_forward_ret_20d_delta": None
        if current_monthly_top20.get("best_month_delta_mean_forward_ret_20d") is None
        or v1_1_monthly_top20.get("best_month_delta_mean_forward_ret_20d") is None
        else float(
            current_monthly_top20["best_month_delta_mean_forward_ret_20d"]
            - v1_1_monthly_top20["best_month_delta_mean_forward_ret_20d"]
        ),
    }

    regime_vs_v1 = {
        "win_regime_count_delta": int(current_regime_top20.get("win_regime_count", 0) - v1_regime_top20.get("win_regime_count", 0)),
        "loss_regime_count_delta": int(current_regime_top20.get("loss_regime_count", 0) - v1_regime_top20.get("loss_regime_count", 0)),
        "flat_regime_count_delta": int(current_regime_top20.get("flat_regime_count", 0) - v1_regime_top20.get("flat_regime_count", 0)),
        "worst_regime_delta_mean_forward_ret_20d_delta": None
        if current_regime_top20.get("worst_regime_delta_mean_forward_ret_20d") is None
        or v1_regime_top20.get("worst_regime_delta_mean_forward_ret_20d") is None
        else float(
            current_regime_top20["worst_regime_delta_mean_forward_ret_20d"]
            - v1_regime_top20["worst_regime_delta_mean_forward_ret_20d"]
        ),
        "best_regime_delta_mean_forward_ret_20d_delta": None
        if current_regime_top20.get("best_regime_delta_mean_forward_ret_20d") is None
        or v1_regime_top20.get("best_regime_delta_mean_forward_ret_20d") is None
        else float(
            current_regime_top20["best_regime_delta_mean_forward_ret_20d"]
            - v1_regime_top20["best_regime_delta_mean_forward_ret_20d"]
        ),
    }

    v1_1_regime_top20 = v1_1_decision_payload["regime_summary_top20"]
    regime_vs_v1_1 = {
        "win_regime_count_delta": int(current_regime_top20.get("win_regime_count", 0) - v1_1_regime_top20.get("win_regime_count", 0)),
        "loss_regime_count_delta": int(current_regime_top20.get("loss_regime_count", 0) - v1_1_regime_top20.get("loss_regime_count", 0)),
        "flat_regime_count_delta": int(current_regime_top20.get("flat_regime_count", 0) - v1_1_regime_top20.get("flat_regime_count", 0)),
        "worst_regime_delta_mean_forward_ret_20d_delta": None
        if current_regime_top20.get("worst_regime_delta_mean_forward_ret_20d") is None
        or v1_1_regime_top20.get("worst_regime_delta_mean_forward_ret_20d") is None
        else float(
            current_regime_top20["worst_regime_delta_mean_forward_ret_20d"]
            - v1_1_regime_top20["worst_regime_delta_mean_forward_ret_20d"]
        ),
        "best_regime_delta_mean_forward_ret_20d_delta": None
        if current_regime_top20.get("best_regime_delta_mean_forward_ret_20d") is None
        or v1_1_regime_top20.get("best_regime_delta_mean_forward_ret_20d") is None
        else float(
            current_regime_top20["best_regime_delta_mean_forward_ret_20d"]
            - v1_1_regime_top20["best_regime_delta_mean_forward_ret_20d"]
        ),
    }

    return {
        "schema_version": "tradex_ma_state_family_risk_on_trend_bad_pick_pruner_v1_delta_v1",
        "generated_at": _utc_now(),
        "source_v1_session_id": v1_session_id,
        "source_v1_session_path": str(v1_session_path),
        "source_v1_1_session_id": v1_1_session_id,
        "source_v1_1_session_path": str(v1_1_session_path),
        "current_recommendation": current_decision_payload.get("recommendation"),
        "v1_recommendation": v1_decision_payload.get("recommendation"),
        "v1_1_recommendation": v1_1_decision_payload.get("recommendation"),
        "current_authoritative_rollup_decision": current_decision_payload.get("authoritative_rollup_decision"),
        "v1_authoritative_rollup_decision": v1_decision_payload.get("authoritative_rollup_decision"),
        "v1_1_authoritative_rollup_decision": v1_1_decision_payload.get("authoritative_rollup_decision"),
        "current_penalty_coverage": {
            "candidate_rows": int(current_coverage_payload.get("candidate_rows") or 0),
            "matched_family_rows": int(current_coverage_payload.get("matched_family_rows") or 0),
            "penalty_applied_rows": int(current_coverage_payload.get("penalty_applied_rows") or 0),
            "regime_match_penalty_rows": int(current_coverage_payload.get("regime_match_penalty_rows") or 0),
        },
        "v1_penalty_coverage": {
            "candidate_rows": int(v1_coverage_payload.get("candidate_rows") or 0),
            "matched_family_rows": int(v1_coverage_payload.get("matched_family_rows") or 0),
            "penalty_applied_rows": int(v1_coverage_payload.get("penalty_applied_rows") or 0),
            "regime_match_penalty_rows": int(v1_coverage_payload.get("regime_match_penalty_rows") or 0),
        },
        "v1_1_penalty_coverage": {
            "candidate_rows": int(v1_1_coverage_payload.get("candidate_rows") or 0),
            "matched_family_rows": int(v1_1_coverage_payload.get("matched_family_rows") or 0),
            "penalty_applied_rows": int(v1_1_coverage_payload.get("penalty_applied_rows") or 0),
            "regime_match_penalty_rows": int(v1_1_coverage_payload.get("regime_match_penalty_rows") or 0),
        },
        "topk_deltas_vs_v1": topk_deltas,
        "topk_deltas_vs_v1_1": {
            top_k: {
                "delta_vs_v1_1": {
                    "mean_forward_ret_20d": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["mean_forward_ret_20d"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["mean_forward_ret_20d"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["mean_forward_ret_20d"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["mean_forward_ret_20d"]
                    ),
                    "median_forward_ret_20d": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["median_forward_ret_20d"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["median_forward_ret_20d"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["median_forward_ret_20d"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["median_forward_ret_20d"]
                    ),
                    "mean_path_value_score_v1": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["mean_path_value_score_v1"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["mean_path_value_score_v1"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["mean_path_value_score_v1"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["mean_path_value_score_v1"]
                    ),
                    "median_path_value_score_v1": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["median_path_value_score_v1"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["median_path_value_score_v1"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["median_path_value_score_v1"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["median_path_value_score_v1"]
                    ),
                    "bad_pick_family_contamination_rate": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["bad_pick_family_contamination_rate"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["bad_pick_family_contamination_rate"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["bad_pick_family_contamination_rate"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["bad_pick_family_contamination_rate"]
                    ),
                    "bottom15_contamination_rate": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["bottom15_contamination_rate"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["bottom15_contamination_rate"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["bottom15_contamination_rate"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["bottom15_contamination_rate"]
                    ),
                    "regime_bad_pick_contamination_rate": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["regime_bad_pick_contamination_rate"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["regime_bad_pick_contamination_rate"] is None
                    else float(
                        _topk_entry(current_compare_payload, top_k)["delta"]["regime_bad_pick_contamination_rate"]
                        - _topk_entry(v1_1_compare_payload, top_k)["delta"]["regime_bad_pick_contamination_rate"]
                    ),
                    "win_rate": None
                    if _topk_entry(current_compare_payload, top_k)["delta"]["win_rate"] is None
                    or _topk_entry(v1_1_compare_payload, top_k)["delta"]["win_rate"] is None
                    else float(_topk_entry(current_compare_payload, top_k)["delta"]["win_rate"] - _topk_entry(v1_1_compare_payload, top_k)["delta"]["win_rate"]),
                }
            }
            for top_k in ("5", "10", "20")
        },
        "branching_vs_v1": branching_vs_v1,
        "branching_vs_v1_1": {
            "changed_top5_members_count_delta": int(
                current_compare_payload["topk_summary"]["changed_top5_members_count"] - v1_1_compare_payload["topk_summary"]["changed_top5_members_count"]
            ),
            "changed_top10_members_count_delta": int(
                current_compare_payload["topk_summary"]["changed_top10_members_count"] - v1_1_compare_payload["topk_summary"]["changed_top10_members_count"]
            ),
            "changed_top20_members_count_delta": int(
                current_compare_payload["topk_summary"]["changed_top20_members_count"] - v1_1_compare_payload["topk_summary"]["changed_top20_members_count"]
            ),
            "changed_rank_count_delta": int(
                current_compare_payload["topk_summary"]["changed_rank_count"] - v1_1_compare_payload["topk_summary"]["changed_rank_count"]
            ),
            "top5_overlap_ratio_delta": None
            if current_compare_payload["topk_summary"]["top5_overlap_ratio"] is None or v1_1_compare_payload["topk_summary"]["top5_overlap_ratio"] is None
            else float(current_compare_payload["topk_summary"]["top5_overlap_ratio"] - v1_1_compare_payload["topk_summary"]["top5_overlap_ratio"]),
            "top10_overlap_ratio_delta": None
            if current_compare_payload["topk_summary"]["top10_overlap_ratio"] is None or v1_1_compare_payload["topk_summary"]["top10_overlap_ratio"] is None
            else float(current_compare_payload["topk_summary"]["top10_overlap_ratio"] - v1_1_compare_payload["topk_summary"]["top10_overlap_ratio"]),
            "top20_overlap_ratio_delta": None
            if current_compare_payload["topk_summary"]["top20_overlap_ratio"] is None or v1_1_compare_payload["topk_summary"]["top20_overlap_ratio"] is None
            else float(current_compare_payload["topk_summary"]["top20_overlap_ratio"] - v1_1_compare_payload["topk_summary"]["top20_overlap_ratio"]),
        },
        "contamination_reduction_vs_v1": {
            "top5_bad_pick_family_contamination_rate_delta": topk_deltas["5"]["delta_vs_v1"]["bad_pick_family_contamination_rate"],
            "top10_bad_pick_family_contamination_rate_delta": topk_deltas["10"]["delta_vs_v1"]["bad_pick_family_contamination_rate"],
            "top20_bad_pick_family_contamination_rate_delta": topk_deltas["20"]["delta_vs_v1"]["bad_pick_family_contamination_rate"],
        },
        "path_quality_recovery_vs_v1": {
            "top5_mean_forward_ret_20d_delta": topk_deltas["5"]["delta_vs_v1"]["mean_forward_ret_20d"],
            "top10_mean_forward_ret_20d_delta": topk_deltas["10"]["delta_vs_v1"]["mean_forward_ret_20d"],
            "top5_mean_path_value_score_v1_delta": topk_deltas["5"]["delta_vs_v1"]["mean_path_value_score_v1"],
            "top10_mean_path_value_score_v1_delta": topk_deltas["10"]["delta_vs_v1"]["mean_path_value_score_v1"],
        },
        "top20_branching_delta_vs_v1": {
            "changed_top20_members_count_delta": branching_vs_v1["changed_top20_members_count_delta"],
            "top20_overlap_ratio_delta": branching_vs_v1["top20_overlap_ratio_delta"],
        },
        "high_value_family_definition_extraction": "stable_high_value_family from the verified family filter session",
        "monthly_stability_vs_v1": monthly_vs_v1,
        "regime_stability_vs_v1": regime_vs_v1,
        "monthly_stability_vs_v1_1": monthly_vs_v1_1,
        "regime_stability_vs_v1_1": regime_vs_v1_1,
    }


def _finalize_session_dir(session_tmp: Path, session_final: Path) -> None:
    if session_final.exists():
        raise FileExistsError(f"final session output already exists: {session_final}")
    try:
        session_tmp.replace(session_final)
    except Exception:
        shutil.move(str(session_tmp), str(session_final))


def run_ma_state_family_high_value_boost_v1(
    *,
    source_family_session: str | Path | None = None,
    v1_session: str | Path | None = None,
    v1_1_session: str | Path | None = None,
    candidate_input_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    limit_anchor_dates: int | None = DEFAULT_LIMIT_ANCHOR_DATES,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    source_family_session_path = _resolve_source_family_session(source_family_session)
    v1_session_path = Path(str(v1_session)).expanduser().resolve() if v1_session and str(v1_session).strip() else DEFAULT_V1_SESSION.resolve()
    if not v1_session_path.exists():
        raise FileNotFoundError(f"v1 session not found: {v1_session_path}")
    v1_1_session_path = Path(str(v1_1_session)).expanduser().resolve() if v1_1_session and str(v1_1_session).strip() else DEFAULT_V1_1_SESSION.resolve()
    if not v1_1_session_path.exists():
        raise FileNotFoundError(f"v1.1 session not found: {v1_1_session_path}")
    candidate_input_path = _resolve_candidate_input_dir(candidate_input_dir)
    source_payloads = _load_source_family_session(source_family_session_path)
    v1_payloads = _load_v1_session(v1_session_path)
    v1_1_payloads = _load_v1_1_session(v1_1_session_path)
    candidate_rows = _load_candidate_rows(candidate_input_path)
    candidate_rows = _apply_anchor_limit(candidate_rows, limit_anchor_dates)

    output_root_path = Path(output_root).expanduser().resolve() if output_root else DEFAULT_OUTPUT_ROOT.resolve()
    output_root_path.mkdir(parents=True, exist_ok=True)
    session_id = _make_session_id()
    session_tmp = output_root_path / f"{session_id}.tmp"
    session_final = output_root_path / session_id
    session_tmp.mkdir(parents=True, exist_ok=False)

    _progress_log(
        f"start source_family={source_family_session_path} v1_session={v1_session_path} v1_1_session={v1_1_session_path} candidate_input={candidate_input_path} out_root={output_root_path} session={session_id}"
    )

    source_thresholds = source_payloads["manifest"].get("thresholds") or source_payloads["family_summary"].get("filter_thresholds") or {}
    thresholds = {
        "bottom15_score_threshold": _safe_float(source_thresholds.get("bottom15_score_threshold")),
        "top15_score_threshold": _safe_float(source_thresholds.get("top15_score_threshold")),
        "source_family_min_sample_count": 300,
        "source_family_min_unique_symbol_count": 30,
        "source_family_min_month_count": 12,
    }
    if thresholds["bottom15_score_threshold"] is None or thresholds["top15_score_threshold"] is None:
        raise RuntimeError("missing bottom15/top15 thresholds from family summary")

    enriched = _build_enriched_candidates(
        candidate_rows=candidate_rows,
        source_ma_row_parquet=source_payloads["source_ma_row_parquet"],
        family_row_parquet=source_payloads["row_parquet"],
    )
    family_summary_frame = source_payloads["family_summary_frame"].copy()
    enriched = enriched.merge(family_summary_frame, on="state_family_id", how="left", validate="m:1")
    enriched["stable_high_value_family"] = enriched["stable_high_value_family"].fillna(False).astype(bool)
    enriched["family_classification"] = enriched["family_classification"].fillna("neutral_family")
    enriched = _apply_boost_scores(enriched)

    champion_ranked = _rank_selection(enriched, score_col="score", prefix="champion")
    challenger_ranked = _rank_selection(enriched, score_col="challenger_score", prefix="challenger")
    merged = champion_ranked.merge(
        challenger_ranked[
            [
                "candidate_idx",
                "challenger_position",
                "challenger_selected_top5",
                "challenger_selected_top10",
                "challenger_selected_top20",
            ]
        ],
        on="candidate_idx",
        how="left",
    )
    merged["challenger_position"] = pd.to_numeric(merged["challenger_position"], errors="coerce")
    merged["champion_position"] = pd.to_numeric(merged["champion_position"], errors="coerce")
    for top_k in TOP_K_VALUES:
        merged[f"champion_selected_top{top_k}"] = merged[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        merged[f"challenger_selected_top{top_k}"] = merged[f"challenger_selected_top{top_k}"].fillna(False).astype(bool)
    merged["score"] = pd.to_numeric(merged["score"], errors="coerce")
    merged["challenger_score"] = pd.to_numeric(merged["challenger_score"], errors="coerce")

    family_join_cols = [
        "state_family_id",
        "family_sample_count",
        "family_unique_symbol_count",
        "family_month_count",
        "family_mean_forward_ret_3d",
        "family_mean_forward_ret_5d",
        "family_mean_forward_ret_10d",
        "family_mean_forward_ret_20d",
        "family_median_forward_ret_20d",
        "family_mean_mfe_20d",
        "family_mean_mae_20d",
        "family_mean_path_value_score_v1",
        "family_median_path_value_score_v1",
        "family_plus5_before_minus5_rate",
        "family_minus5_before_plus5_rate",
        "family_top15_rate",
        "family_bottom15_rate",
        "family_months_observed",
        "family_positive_month_rate",
        "family_worst_month_mean_path_value",
        "family_best_month_mean_path_value",
        "family_mean_monthly_path_value",
        "family_std_monthly_path_value",
        "family_month_sample_count",
        "family_regime_count",
        "family_regime_consistency_score",
        "family_score_spread",
        "family_min_regime_mean_path_value",
        "family_max_regime_mean_path_value",
        "stable_high_value_family",
        "family_classification",
        "dominant_regime_context",
        "family_bad_pick_regime",
        "sample_qualified",
        "core_bad_pick",
        "strict_bad_pick_family",
        "relaxed_bad_pick_family",
        "stability_hits_3",
        "bad_pick_watch_family",
        "mae_risk_family",
        "endpoint_bad_pick_family",
        "shadow_label_count",
        "family_source_family_session_id",
        "family_source_family_session_path",
        "family_source_row_count",
        "family_strict_relax_gap",
    ]
    family_join_cols = [col for col in family_join_cols if col in merged.columns]
    diff_rows = merged.copy()
    diff_rows["top5_changed_member"] = diff_rows["champion_selected_top5"] ^ diff_rows["challenger_selected_top5"]
    diff_rows["top10_changed_member"] = diff_rows["champion_selected_top10"] ^ diff_rows["challenger_selected_top10"]
    diff_rows["top20_changed_member"] = diff_rows["champion_selected_top20"] ^ diff_rows["challenger_selected_top20"]
    diff_rows["bad_pick_family_contaminant"] = diff_rows["family_classification"].fillna("").astype(str).eq("stable_bad_pick_family")
    diff_rows["bottom15_contaminant"] = pd.to_numeric(diff_rows["path_value_score_v1"], errors="coerce") <= float(thresholds["bottom15_score_threshold"])
    diff_rows["penalty_terms"] = diff_rows["score_adjustment_reason"].map(lambda value: "|".join(value) if isinstance(value, list) else "")

    coverage_payload = _build_penalty_coverage_summary(diff_rows)
    compare_payload = _build_compare_payload(
        frame=diff_rows,
        bottom15_threshold=float(thresholds["bottom15_score_threshold"]),
        top15_threshold=float(thresholds["top15_score_threshold"]),
        source_family_session_id=source_payloads["manifest"]["session_id"],
        source_family_session_path=source_family_session_path,
    )
    monthly_payload = _build_monthly_comparison(
        diff_rows,
        bottom15_threshold=float(thresholds["bottom15_score_threshold"]),
        top15_threshold=float(thresholds["top15_score_threshold"]),
    )
    regime_payload = _build_regime_comparison(
        diff_rows,
        bottom15_threshold=float(thresholds["bottom15_score_threshold"]),
        top15_threshold=float(thresholds["top15_score_threshold"]),
    )
    decision_payload = _build_decision_payload(
        source_family_session_id=source_payloads["manifest"]["session_id"],
        source_family_session_path=source_family_session_path,
        source_artifacts={
            "run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "bad_pick_shadow_classification_json": str(source_family_session_path / "bad_pick_shadow_classification.json"),
            "bad_pick_relaxation_compare_json": str(source_family_session_path / "bad_pick_relaxation_compare.json"),
            "state_family_bad_pick_surface_refinement_v1_decision_json": str(
                source_family_session_path / "state_family_bad_pick_surface_refinement_v1_decision.json"
            ),
            "bad_pick_refinement_rows_parquet": str(source_family_session_path / "bad_pick_refinement_rows.parquet"),
            "source_ma_run_manifest_json": str(source_payloads["source_ma_session_path"] / "run_manifest.json"),
            "source_ma_row_parquet": str(source_payloads["source_ma_row_parquet"]),
            "candidate_input_json": str(candidate_input_path / "integrated_guarded_v1_candidate_snapshots.json"),
        },
        compare_payload=compare_payload,
        coverage_payload=coverage_payload,
        monthly_payload=monthly_payload,
        regime_payload=regime_payload,
        frame=diff_rows,
        bottom15_threshold=float(thresholds["bottom15_score_threshold"]),
        top15_threshold=float(thresholds["top15_score_threshold"]),
    )

    diff_rows_out = diff_rows.copy()
    diff_rows_out["source_family_session_id"] = source_payloads["manifest"]["session_id"]
    diff_rows_out["source_family_session_path"] = str(source_family_session_path)
    diff_rows_out["source_ma_session_path"] = str(source_payloads["source_ma_session_path"])
    diff_rows_out["candidate_input_dir"] = str(candidate_input_path)

    output_files = {
        "run_manifest_json": session_tmp / "run_manifest.json",
        "ma_state_family_high_value_boost_v1_compare_json": session_tmp / "ma_state_family_high_value_boost_v1_compare.json",
        "ma_state_family_high_value_boost_v1_decision_json": session_tmp / "ma_state_family_high_value_boost_v1_decision.json",
        "boost_coverage_summary_json": session_tmp / "boost_coverage_summary.json",
        "topk_membership_diff_parquet": session_tmp / "topk_membership_diff.parquet",
        "monthly_comparison_json": session_tmp / "monthly_comparison.json",
        "regime_comparison_json": session_tmp / "regime_comparison.json",
        "_artifact_complete_json": session_tmp / "_ARTIFACT_COMPLETE.json",
    }

    conn = duckdb.connect()
    try:
        conn.register("diff_rows_out", diff_rows_out)
        conn.execute(f"COPY diff_rows_out TO '{output_files['topk_membership_diff_parquet'].as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        conn.close()

    final_output_artifacts = {key: str(session_final / path.name) for key, path in output_files.items()}
    manifest_payload = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "source_family_session_id": source_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_ma_session_path": str(source_payloads["source_ma_session_path"]),
        "v1_session_id": v1_session_path.name,
        "v1_session_path": str(v1_session_path),
        "v1_1_session_id": v1_1_session_path.name,
        "v1_1_session_path": str(v1_1_session_path),
        "candidate_input_dir": str(candidate_input_path),
        "candidate_input_json": str(candidate_input_path / "integrated_guarded_v1_candidate_snapshots.json"),
        "output_root": str(output_root_path),
        "source_artifacts": {
            "family_filter_run_manifest_json": str(source_family_session_path / "run_manifest.json"),
            "family_filter_decision_json": str(source_family_session_path / "state_family_filter_v1_decision.json"),
            "family_filter_summary_json": str(source_family_session_path / "state_family_summary.json"),
            "family_filter_classification_json": str(source_family_session_path / "state_family_classification.json"),
            "family_filter_by_regime_json": str(source_family_session_path / "state_family_by_regime.json"),
            "family_filter_monthly_stability_json": str(source_family_session_path / "state_family_monthly_stability.json"),
            "family_filter_rows_parquet": str(source_family_session_path / "state_family_rows.parquet"),
            "source_ma_run_manifest_json": str(source_payloads["source_ma_session_path"] / "run_manifest.json"),
            "source_ma_row_parquet": str(source_payloads["source_ma_row_parquet"]),
            "v1_compare_json": str(v1_session_path / "ma_state_family_bad_pick_pruner_v1_compare.json"),
            "v1_decision_json": str(v1_session_path / "ma_state_family_bad_pick_pruner_v1_decision.json"),
            "v1_penalty_coverage_summary_json": str(v1_session_path / "penalty_coverage_summary.json"),
            "v1_1_compare_json": str(v1_1_session_path / "ma_state_family_bad_pick_pruner_v1_1_compare.json"),
            "v1_1_decision_json": str(v1_1_session_path / "ma_state_family_bad_pick_pruner_v1_1_decision.json"),
            "v1_1_penalty_coverage_summary_json": str(v1_1_session_path / "penalty_coverage_summary.json"),
        },
        "output_artifacts": final_output_artifacts,
        "no_lookahead_inherited": True,
        "candidate_row_count": int(len(candidate_rows)),
        "enriched_row_count": int(len(diff_rows_out)),
        "anchor_count": int(diff_rows_out["anchor_date"].nunique()),
        "month_count": int(diff_rows_out["month_bucket"].nunique()),
        "family_join_rate": _safe_float(coverage_payload.get("matched_family_rate")),
        "score_field": "score",
        "challenger_score_field": "challenger_score",
        "score_adjustment_cap": MAX_TOTAL_BOOST,
        "high_value_family_definition": {
            "stable_high_value_family_count": int(source_payloads["decision"].get("stable_high_value_family_count") or 0),
            "note": "stable_high_value_family is sourced from the verified family filter session",
        },
    }
    _write_json(output_files["ma_state_family_high_value_boost_v1_compare_json"], compare_payload)
    _write_json(output_files["ma_state_family_high_value_boost_v1_decision_json"], decision_payload)
    _write_json(output_files["boost_coverage_summary_json"], coverage_payload)
    _write_json(output_files["monthly_comparison_json"], monthly_payload)
    _write_json(output_files["regime_comparison_json"], regime_payload)
    _write_json(output_files["run_manifest_json"], manifest_payload)
    _write_json(output_files["_artifact_complete_json"], {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "session_id": session_id,
        "validated": True,
    })

    _finalize_session_dir(session_tmp, session_final)
    _progress_log(f"finalized session={session_id} elapsed={time.perf_counter() - run_started:.1f}s")

    return {
        "session_id": session_id,
        "source_family_session_id": source_payloads["manifest"]["session_id"],
        "source_family_session_path": str(source_family_session_path),
        "source_ma_session_path": str(source_payloads["source_ma_session_path"]),
        "candidate_input_dir": str(candidate_input_path),
        "compare": compare_payload,
        "decision": decision_payload,
        "coverage": coverage_payload,
        "monthly": monthly_payload,
        "regime": regime_payload,
        "paths": final_output_artifacts,
        "session_dir": str(session_final),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run TRADEX high-value boost challenger against the refined family map.")
    parser.add_argument("--source-family-session", default=str(DEFAULT_SOURCE_FAMILY_SESSION))
    parser.add_argument("--v1-session", default=str(DEFAULT_V1_SESSION))
    parser.add_argument("--v1-1-session", default=str(DEFAULT_V1_1_SESSION))
    parser.add_argument("--candidate-input-dir", default=str(DEFAULT_CANDIDATE_INPUT_DIR))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    args = parser.parse_args(argv)

    run_ma_state_family_high_value_boost_v1(
        source_family_session=args.source_family_session,
        v1_session=args.v1_session,
        v1_1_session=args.v1_1_session,
        candidate_input_dir=args.candidate_input_dir,
        output_root=args.output_root,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
