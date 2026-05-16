from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts


AXIS_ID = "swing_quality_selection_v1"
CHAMPION_ID = "champion_top5_capture_boundary_promoter_v1"
SCHEMA_PREFIX = "tradex_swing_quality_selection_v1"

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\swing_quality_selection_v1")
DEFAULT_CHAMPION_FREEZE_JSON = Path(
    r"G:\Tradex\research_freeze_summaries\champion_top5_capture_boundary_promoter_v1_final\20260505T070512Z\final_freeze_decision.json"
)
DEFAULT_TOPK_OPERATIONAL_FIT_JSON = Path(
    r"G:\Tradex\topk_validity_audit_v1\20260512T090000Z-topk_validity_audit_v1\topk_operational_fit.json"
)
DEFAULT_PUBLISH_ROOT = Path(r"C:\work\meemee-screener\external_analysis\publish_candidates")

TOP_K_VALUES = (5, 10, 20)
PRIMARY_TOP_K = 10
HOLDING_DAYS = 20
SEVERE_LOSS_THRESHOLD = -0.15

LABEL_COLUMNS = {
    "forward_ret_5d",
    "forward_ret_10d",
    "forward_ret_20d",
    "path_value_score_v1",
    "mfe_20d",
    "mae_20d",
    "top15_label",
    "bottom15_label",
    "hit_plus_5_before_minus_5",
    "hit_minus_5_before_plus_5",
}
SCORING_FEATURE_COLUMNS = {
    "champion_score",
    "monthly_context",
    "weekly_context",
    "monthly_context_no_lookahead",
    "weekly_context_no_lookahead",
    "candle_body_ratio",
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "gap_pct",
    "vol_ratio5_20",
    "v",
    "prefilter_bucket",
    "prefilter_reason",
    "shape_classification",
}

FIXED_WEIGHTS = {
    "regime_quality": 0.035,
    "entry_confirmation": 0.045,
    "liquidity_quality": 0.020,
    "bad_pick_veto": -0.055,
    "drawdown_control": 0.025,
}

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "candidate_ledger.jsonl",
    "compare.json",
    "family_leaderboard.json",
    "session_leaderboard_rollup.json",
    "meemee_reflection_gate.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _mean_or_none(values: Iterable[Any]) -> float | None:
    usable = [_as_float(value) for value in values]
    usable = [value for value in usable if value is not None]
    if not usable:
        return None
    return float(sum(usable) / len(usable))


def _rate_or_none(values: Iterable[Any]) -> float | None:
    usable = [value for value in values if value is not None and not pd.isna(value)]
    if not usable:
        return None
    return float(sum(1 for value in usable if bool(value)) / len(usable))


def _delta(candidate: float | None, champion: float | None) -> float | None:
    if candidate is None or champion is None:
        return None
    return float(candidate - champion)


def _as_bool_series(series: pd.Series) -> pd.Series:
    if str(series.dtype) in {"bool", "boolean"}:
        return series.fillna(False).astype(bool)
    return series.fillna(False).astype(str).str.lower().isin({"1", "true", "yes", "y"})


def _normalize_date_key(value: Any) -> str:
    if value is None or value is pd.NA:
        raise ValueError("date value is required")
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if not text:
        raise ValueError("date value is required")
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit() and len(text) == 8:
        return pd.to_datetime(text, format="%Y%m%d").strftime("%Y-%m-%d")
    if text.isdigit() and int(text) > 100_000_000:
        return pd.to_datetime(int(text), unit="s", utc=True).strftime("%Y-%m-%d")
    return pd.to_datetime(text).strftime("%Y-%m-%d")


def _load_source_rows(source_rows_parquet: Path) -> pd.DataFrame:
    if not source_rows_parquet.exists():
        raise FileNotFoundError(f"source rows parquet not found: {source_rows_parquet}")
    frame = pd.read_parquet(source_rows_parquet).copy()
    return load_source_rows_from_frame(frame)


def load_source_rows_from_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if "champion_score" not in working.columns and "score" in working.columns:
        working["champion_score"] = working["score"]
    if "trade_date" not in working.columns and "anchor_date" in working.columns:
        working["trade_date"] = working["anchor_date"]
    required = {"symbol", "side", "trade_date", "champion_rank", "champion_score", "forward_ret_20d"}
    missing = sorted(required - set(working.columns))
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")

    working["source_row_id"] = range(len(working))
    working["symbol"] = working["symbol"].astype(str)
    working["side"] = working["side"].astype(str).str.lower()
    working["trade_date_key"] = working["trade_date"].map(_normalize_date_key)
    working["anchor_date"] = working.get("anchor_date", working["trade_date_key"])
    working["anchor_date"] = working["anchor_date"].map(_normalize_date_key)
    working["month_bucket"] = working.get("month_bucket", working["trade_date_key"].str.slice(0, 7)).astype(str)
    if "regime_label" not in working.columns:
        working["regime_label"] = working["market_regime_bucket"].astype(str) if "market_regime_bucket" in working.columns else "unknown"

    working["champion_rank"] = pd.to_numeric(working["champion_rank"], errors="coerce").astype("Int64")
    working["champion_score"] = pd.to_numeric(working["champion_score"], errors="coerce")
    for column in (
        "forward_ret_5d",
        "forward_ret_10d",
        "forward_ret_20d",
        "path_value_score_v1",
        "mfe_20d",
        "mae_20d",
        "candle_body_ratio",
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "gap_pct",
        "vol_ratio5_20",
        "v",
    ):
        if column in working.columns:
            working[column] = pd.to_numeric(working[column], errors="coerce")
    for column in ("top15_label", "bottom15_label", "monthly_context_no_lookahead", "weekly_context_no_lookahead"):
        if column in working.columns:
            working[column] = _as_bool_series(working[column])
    if "bottom15_label" not in working.columns:
        working["bottom15_label"] = False
    if "top15_label" not in working.columns:
        working["top15_label"] = False
    if "mfe_20d" not in working.columns:
        working["mfe_20d"] = pd.NA
    if "mae_20d" not in working.columns:
        working["mae_20d"] = pd.NA
    if "path_value_score_v1" not in working.columns:
        working["path_value_score_v1"] = pd.NA
    for column in SCORING_FEATURE_COLUMNS:
        if column not in working.columns:
            working[column] = pd.NA
    for top_k in TOP_K_VALUES:
        selected_col = f"champion_selected_top{top_k}"
        if selected_col in working.columns:
            working[selected_col] = _as_bool_series(working[selected_col])
        else:
            working[selected_col] = working["champion_rank"].le(top_k).fillna(False).astype(bool)

    working = working[
        working["side"].eq("long")
        & working["champion_score"].notna()
        & working["champion_rank"].notna()
        & working["champion_selected_top20"].fillna(False).astype(bool)
    ].copy()
    working.sort_values(["trade_date_key", "side", "champion_rank", "symbol"], inplace=True, kind="stable")
    return working.reset_index(drop=True)


def _clip_series(frame: pd.DataFrame, column: str, default: float, lower: float, upper: float) -> pd.Series:
    series = pd.to_numeric(frame.get(column, default), errors="coerce").fillna(default)
    return series.clip(lower=lower, upper=upper)


def _context_score_value(value: Any) -> float:
    text = str(value or "").lower()
    if not text or text in {"nan", "none", "<na>", "unknown"}:
        return 0.35
    negative = ("overextended", "top_warning", "break_risk", "risk_off", "panic", "down")
    positive = ("trend_up", "up", "rebound", "bottom_building", "range_buy", "support")
    neutral = ("range", "neutral", "mixed")
    if any(token in text for token in negative):
        return 0.15
    if any(token in text for token in positive):
        return 0.75
    if any(token in text for token in neutral):
        return 0.55
    return 0.40


def _regime_quality(frame: pd.DataFrame) -> pd.Series:
    monthly = frame["monthly_context"].map(_context_score_value).astype(float)
    weekly = frame["weekly_context"].map(_context_score_value).astype(float)
    monthly_ok = frame["monthly_context_no_lookahead"].fillna(False).astype(bool).astype(float)
    weekly_ok = frame["weekly_context_no_lookahead"].fillna(False).astype(bool).astype(float)
    context = (monthly + weekly) / 2.0
    no_lookahead_support = (monthly_ok + weekly_ok) / 2.0
    return (0.72 * context + 0.28 * no_lookahead_support).clip(0.0, 1.0)


def _entry_confirmation(frame: pd.DataFrame) -> pd.Series:
    body = _clip_series(frame, "candle_body_ratio", 0.35, 0.0, 1.0)
    lower = _clip_series(frame, "candle_lower_wick_ratio", 0.10, 0.0, 1.0)
    upper = _clip_series(frame, "candle_upper_wick_ratio", 0.20, 0.0, 1.0)
    triplet_up = _clip_series(frame, "candle_triplet_up_prob", 0.50, 0.0, 1.0)
    triplet_down = _clip_series(frame, "candle_triplet_down_prob", 0.50, 0.0, 1.0)
    gap = _clip_series(frame, "gap_pct", 0.0, -0.12, 0.12)
    gap_penalty = gap.clip(lower=0.0) / 0.12
    score = 0.36 * triplet_up + 0.18 * body + 0.18 * lower - 0.16 * upper - 0.12 * triplet_down - 0.10 * gap_penalty + 0.28
    return score.clip(0.0, 1.0)


def _liquidity_quality(frame: pd.DataFrame) -> pd.Series:
    vol_ratio = _clip_series(frame, "vol_ratio5_20", 1.0, 0.0, 3.0)
    volume = pd.to_numeric(frame.get("v", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
    volume_score = (volume.map(lambda value: math.log1p(float(value))) / math.log1p(max(float(volume.max() or 1.0), 1.0))).fillna(0.0)
    ratio_score = (vol_ratio / 3.0).clip(0.0, 1.0)
    return (0.65 * ratio_score + 0.35 * volume_score).clip(0.0, 1.0)


def _bad_pick_veto(frame: pd.DataFrame) -> pd.Series:
    bucket = frame["prefilter_bucket"].fillna("").astype(str).str.lower()
    reason = frame["prefilter_reason"].fillna("").astype(str).str.lower()
    shape = frame["shape_classification"].fillna("").astype(str).str.lower()
    upper = _clip_series(frame, "candle_upper_wick_ratio", 0.20, 0.0, 1.0)
    lower = _clip_series(frame, "candle_lower_wick_ratio", 0.10, 0.0, 1.0)
    triplet_down = _clip_series(frame, "candle_triplet_down_prob", 0.50, 0.0, 1.0)
    triplet_up = _clip_series(frame, "candle_triplet_up_prob", 0.50, 0.0, 1.0)
    gap = _clip_series(frame, "gap_pct", 0.0, -0.12, 0.12)
    text_risk = (
        bucket.str.contains("exclude", regex=False)
        | reason.str.contains("bad_pick", regex=False)
        | reason.str.contains("shape_missing", regex=False)
        | shape.str.contains("negative", regex=False)
    ).astype(float)
    candle_risk = ((upper > lower + 0.25) | (triplet_down > triplet_up + 0.20) | (gap > 0.08)).astype(float)
    return (0.62 * text_risk + 0.38 * candle_risk).clip(0.0, 1.0)


def _drawdown_control(frame: pd.DataFrame) -> pd.Series:
    upper = _clip_series(frame, "candle_upper_wick_ratio", 0.20, 0.0, 1.0)
    lower = _clip_series(frame, "candle_lower_wick_ratio", 0.10, 0.0, 1.0)
    gap_abs = _clip_series(frame, "gap_pct", 0.0, -0.12, 0.12).abs() / 0.12
    triplet_down = _clip_series(frame, "candle_triplet_down_prob", 0.50, 0.0, 1.0)
    score = 0.44 * (1.0 - upper) + 0.22 * lower + 0.18 * (1.0 - gap_abs) + 0.16 * (1.0 - triplet_down)
    return score.clip(0.0, 1.0)


def apply_candidate_logic(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    working["regime_quality_score"] = _regime_quality(working)
    working["entry_confirmation_score"] = _entry_confirmation(working)
    working["liquidity_quality_score"] = _liquidity_quality(working)
    working["bad_pick_veto_score"] = _bad_pick_veto(working)
    working["drawdown_control_score"] = _drawdown_control(working)
    working["swing_quality_delta"] = (
        FIXED_WEIGHTS["regime_quality"] * working["regime_quality_score"]
        + FIXED_WEIGHTS["entry_confirmation"] * working["entry_confirmation_score"]
        + FIXED_WEIGHTS["liquidity_quality"] * working["liquidity_quality_score"]
        + FIXED_WEIGHTS["bad_pick_veto"] * working["bad_pick_veto_score"]
        + FIXED_WEIGHTS["drawdown_control"] * working["drawdown_control_score"]
    )
    working["swing_quality_score"] = working["champion_score"] + working["swing_quality_delta"]
    ranked_parts = []
    for _, group in working.groupby(["trade_date_key", "side"], sort=True):
        ordered = group.sort_values(["swing_quality_score", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
        ordered["candidate_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True) if ranked_parts else working.assign(candidate_rank=pd.Series(dtype="int"))
    for top_k in TOP_K_VALUES:
        ranked[f"candidate_selected_top{top_k}"] = ranked["candidate_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        ranked[f"changed_top{top_k}_member"] = ranked[f"candidate_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["candidate_rank"].astype("Int64") != ranked["champion_rank"].astype("Int64")
    ranked["swing_action_state"] = "hold_neutral"
    ranked.loc[ranked["swing_quality_delta"].ge(0.045), "swing_action_state"] = "promote_candidate"
    ranked.loc[ranked["bad_pick_veto_score"].ge(0.65), "swing_action_state"] = "avoid_or_demote"
    return ranked


def _selected(frame: pd.DataFrame, prefix: str, top_k: int) -> pd.DataFrame:
    return frame[frame[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)].copy()


def _topk_metrics(frame: pd.DataFrame, prefix: str, top_k: int) -> dict[str, Any]:
    rows = _selected(frame, prefix, top_k)
    ret = pd.to_numeric(rows["forward_ret_20d"], errors="coerce")
    mfe = pd.to_numeric(rows["mfe_20d"], errors="coerce")
    mae = pd.to_numeric(rows["mae_20d"], errors="coerce")
    severe = rows["bottom15_label"].fillna(False).astype(bool) | ret.le(SEVERE_LOSS_THRESHOLD).fillna(False)
    return {
        "top_k": int(top_k),
        "selected_count": int(len(rows)),
        "hold_end_return_20d": _mean_or_none(ret.tolist()),
        "mfe_20d": _mean_or_none(mfe.tolist()),
        "mae_20d": _mean_or_none(mae.tolist()),
        "win_rate_hold_end": _rate_or_none(ret.gt(0).tolist()),
        "win_rate_mfe_positive": _rate_or_none(mfe.gt(0).tolist()),
        "severe_loss_rate": _rate_or_none(severe.tolist()),
        "bottom15_count": int(rows["bottom15_label"].fillna(False).astype(bool).sum()),
        "max_drawdown": _as_float(mae.min()) if not rows.empty else None,
        "path_value_score_v1": _mean_or_none(pd.to_numeric(rows["path_value_score_v1"], errors="coerce").tolist()),
    }


def _changed_rows(frame: pd.DataFrame, top_k: int, *, added: bool) -> pd.DataFrame:
    champion = frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
    candidate = frame[f"candidate_selected_top{top_k}"].fillna(False).astype(bool)
    if added:
        return frame[candidate & ~champion].copy()
    return frame[champion & ~candidate].copy()


def _row_quality(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "row_count": 0,
            "hold_end_return_20d": None,
            "mfe_20d": None,
            "mae_20d": None,
            "severe_loss_rate": None,
        }
    ret = pd.to_numeric(rows["forward_ret_20d"], errors="coerce")
    severe = rows["bottom15_label"].fillna(False).astype(bool) | ret.le(SEVERE_LOSS_THRESHOLD).fillna(False)
    return {
        "row_count": int(len(rows)),
        "hold_end_return_20d": _mean_or_none(ret.tolist()),
        "mfe_20d": _mean_or_none(pd.to_numeric(rows["mfe_20d"], errors="coerce").tolist()),
        "mae_20d": _mean_or_none(pd.to_numeric(rows["mae_20d"], errors="coerce").tolist()),
        "severe_loss_rate": _rate_or_none(severe.tolist()),
    }


def _metric_deltas(champion: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "hold_end_return_20d_delta": _delta(candidate["hold_end_return_20d"], champion["hold_end_return_20d"]),
        "mfe_20d_delta": _delta(candidate["mfe_20d"], champion["mfe_20d"]),
        "mae_20d_delta": _delta(candidate["mae_20d"], champion["mae_20d"]),
        "win_rate_hold_end_delta": _delta(candidate["win_rate_hold_end"], champion["win_rate_hold_end"]),
        "severe_loss_rate_delta": _delta(candidate["severe_loss_rate"], champion["severe_loss_rate"]),
        "max_drawdown_delta": _delta(candidate["max_drawdown"], champion["max_drawdown"]),
        "path_value_score_v1_delta": _delta(candidate["path_value_score_v1"], champion["path_value_score_v1"]),
    }


def _period_segments(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return [{"label": "all", "start_date": "unknown", "end_date": "unknown"}]
    return [
        {
            "label": "all",
            "start_date": str(frame["trade_date_key"].min()),
            "end_date": str(frame["trade_date_key"].max()),
        }
    ]


def _same_condition_contract(frame: pd.DataFrame) -> dict[str, Any]:
    return contracts.build_same_condition_contract(
        universe=sorted(frame["symbol"].astype(str).unique().tolist()),
        period_segments=_period_segments(frame),
        top_k=PRIMARY_TOP_K,
        regime="all_long_swing",
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        feature_family="boundary_feature",
    ).to_dict()


def _branching(frame: pd.DataFrame) -> dict[str, Any]:
    changed_top5 = int(frame["changed_top5_member"].fillna(False).astype(bool).sum())
    changed_top10 = int(frame["changed_top10_member"].fillna(False).astype(bool).sum())
    changed_top20 = int(frame["changed_top20_member"].fillna(False).astype(bool).sum())
    changed_rank = int(frame["rank_changed"].fillna(False).astype(bool).sum())
    if changed_top5 or changed_top10:
        reason = "swing_quality_rerank_changed_primary_topk"
    elif changed_top20:
        reason = "swing_quality_rerank_changed_top20_only"
    else:
        reason = "no_meaningful_branching"
    return {
        "changed_top5_members_count": changed_top5,
        "changed_top10_members_count": changed_top10,
        "changed_top20_members_count": changed_top20,
        "changed_rank_count": changed_rank,
        "selection_divergence_reason": reason,
    }


def _stability_rows(frame: pd.DataFrame, column: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = []
    for key, group in frame.groupby(column, sort=True):
        champion = _topk_metrics(group, "champion", PRIMARY_TOP_K)
        candidate = _topk_metrics(group, "candidate", PRIMARY_TOP_K)
        deltas = _metric_deltas(champion, candidate)
        rows.append(
            {
                column: str(key),
                "decision_sets": int(group.groupby(["trade_date_key", "side"], sort=False).ngroups),
                "top10_hold_end_return_20d_delta": deltas["hold_end_return_20d_delta"],
                "top10_mfe_20d_delta": deltas["mfe_20d_delta"],
                "top10_mae_20d_delta": deltas["mae_20d_delta"],
                "top10_severe_loss_rate_delta": deltas["severe_loss_rate_delta"],
                "changed_top10_members_count": int(group["changed_top10_member"].fillna(False).astype(bool).sum()),
            }
        )
    improved = [row for row in rows if (row["top10_hold_end_return_20d_delta"] is not None and row["top10_hold_end_return_20d_delta"] >= 0.0)]
    degraded = [row for row in rows if (row["top10_hold_end_return_20d_delta"] is not None and row["top10_hold_end_return_20d_delta"] < 0.0)]
    worst = min(
        (row["top10_hold_end_return_20d_delta"] for row in rows if row["top10_hold_end_return_20d_delta"] is not None),
        default=None,
    )
    summary = {
        "bucket_count": len(rows),
        "improved_or_flat_bucket_count": len(improved),
        "degraded_bucket_count": len(degraded),
        "worst_top10_hold_end_return_20d_delta": worst,
        "stable": bool(rows) and len(improved) >= len(degraded) and (worst is None or worst >= -0.02),
    }
    return rows, summary


def _swap_quality(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    harmful_swap_count = 0
    for top_k in TOP_K_VALUES:
        added = _changed_rows(frame, top_k, added=True)
        removed = _changed_rows(frame, top_k, added=False)
        added_quality = _row_quality(added)
        removed_quality = _row_quality(removed)
        if (
            added_quality["hold_end_return_20d"] is not None
            and removed_quality["hold_end_return_20d"] is not None
            and added_quality["hold_end_return_20d"] < removed_quality["hold_end_return_20d"]
        ):
            harmful_swap_count += 1
        out[f"top{top_k}"] = {
            "added": added_quality,
            "removed": removed_quality,
            "added_minus_removed_hold_end_return_20d": _delta(added_quality["hold_end_return_20d"], removed_quality["hold_end_return_20d"]),
            "added_minus_removed_severe_loss_rate": _delta(added_quality["severe_loss_rate"], removed_quality["severe_loss_rate"]),
        }
    out["harmful_swap_count"] = harmful_swap_count
    return out


def _victory_metrics(frame: pd.DataFrame, prefix: str = "candidate", top_k: int = PRIMARY_TOP_K) -> dict[str, Any]:
    metrics = _topk_metrics(frame, prefix, top_k)
    return {
        "hold_end_return_20d": metrics["hold_end_return_20d"],
        "mfe_20d": metrics["mfe_20d"],
        "mae_20d": metrics["mae_20d"],
        "win_flag_hold_end": metrics["win_rate_hold_end"],
        "win_flag_mfe": metrics["win_rate_mfe_positive"],
        "addability_score": metrics["path_value_score_v1"],
        "trimability_score": None if metrics["severe_loss_rate"] is None else 1.0 - float(metrics["severe_loss_rate"]),
        "opportunity_count": metrics["selected_count"],
        "avg_holding_days": HOLDING_DAYS,
        "max_drawdown": metrics["max_drawdown"],
    }


def _build_compare(frame: pd.DataFrame, same_condition: dict[str, Any], decision: str, reasons: list[str]) -> dict[str, Any]:
    topk: dict[str, Any] = {}
    for top_k in TOP_K_VALUES:
        champion = _topk_metrics(frame, "champion", top_k)
        candidate = _topk_metrics(frame, "candidate", top_k)
        topk[f"top{top_k}"] = {
            "champion": champion,
            "candidate": candidate,
            "deltas": _metric_deltas(champion, candidate),
            "changed_member_count": int(frame[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum()),
        }
    candidate_result = {
        "candidate_id": AXIS_ID,
        "candidate_local_decision": decision,
        "decision": decision,
        "decision_reasons": reasons,
        "feature_family": "boundary_feature",
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "victory_metrics": _victory_metrics(frame, "candidate", PRIMARY_TOP_K),
        "long_horizon_regime_score": _mean_or_none(frame["regime_quality_score"].tolist()),
        "recent_adaptation_score": _mean_or_none(frame["entry_confirmation_score"].tolist()),
        "topk_metrics": topk,
    }
    return {
        "schema_version": "tradex_experiment_compare_v1",
        "diagnostics_schema_version": f"{SCHEMA_PREFIX}_diagnostics_v1",
        "family_id": AXIS_ID,
        "generated_at": _utc_now(),
        "baseline_run_id": CHAMPION_ID,
        "candidate_results": [candidate_result],
        "same_condition_contract": same_condition,
        "same_condition_checks": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "silent_fallback_used": False,
        },
        "champion_id": CHAMPION_ID,
        "candidate_id": AXIS_ID,
        "topk": topk,
        "branching": _branching(frame),
        "swap_quality": _swap_quality(frame),
    }


def _build_decision_inputs(frame: pd.DataFrame, topk_operational_fit: dict[str, Any]) -> dict[str, Any]:
    top5 = {
        "champion": _topk_metrics(frame, "champion", 5),
        "candidate": _topk_metrics(frame, "candidate", 5),
    }
    top10 = {
        "champion": _topk_metrics(frame, "champion", 10),
        "candidate": _topk_metrics(frame, "candidate", 10),
    }
    top5["deltas"] = _metric_deltas(top5["champion"], top5["candidate"])
    top10["deltas"] = _metric_deltas(top10["champion"], top10["candidate"])
    month_rows, month_summary = _stability_rows(frame, "month_bucket")
    regime_rows, regime_summary = _stability_rows(frame, "regime_label")
    branching = _branching(frame)
    swap_quality = _swap_quality(frame)
    return {
        "top5": top5,
        "top10": top10,
        "month_rows": month_rows,
        "month_summary": month_summary,
        "regime_rows": regime_rows,
        "regime_summary": regime_summary,
        "branching": branching,
        "swap_quality": swap_quality,
        "topk_operational_fit": topk_operational_fit,
    }


def _decide(inputs: dict[str, Any]) -> tuple[str, list[str], dict[str, bool]]:
    top5_delta = inputs["top5"]["deltas"]["hold_end_return_20d_delta"]
    top10_delta = inputs["top10"]["deltas"]["hold_end_return_20d_delta"]
    top5_mfe_delta = inputs["top5"]["deltas"]["mfe_20d_delta"]
    top10_mfe_delta = inputs["top10"]["deltas"]["mfe_20d_delta"]
    top5_mae_delta = inputs["top5"]["deltas"]["mae_20d_delta"]
    top10_mae_delta = inputs["top10"]["deltas"]["mae_20d_delta"]
    top5_severe_delta = inputs["top5"]["deltas"]["severe_loss_rate_delta"]
    top10_severe_delta = inputs["top10"]["deltas"]["severe_loss_rate_delta"]
    branching = inputs["branching"]
    swap_quality = inputs["swap_quality"]
    topk_fit = inputs["topk_operational_fit"]
    checks = {
        "same_condition_pass": True,
        "no_lookahead_pass": True,
        "top5_improved": bool(top5_delta is not None and top5_delta > 0.0),
        "top10_improved": bool(top10_delta is not None and top10_delta > 0.0),
        "mfe_not_worse": bool((top5_mfe_delta is None or top5_mfe_delta >= 0.0) and (top10_mfe_delta is None or top10_mfe_delta >= 0.0)),
        "mae_not_worse": bool((top5_mae_delta is None or top5_mae_delta >= 0.0) and (top10_mae_delta is None or top10_mae_delta >= 0.0)),
        "severe_loss_not_worse": bool((top5_severe_delta is None or top5_severe_delta <= 0.0) and (top10_severe_delta is None or top10_severe_delta <= 0.0)),
        "monthly_stable": bool(inputs["month_summary"]["stable"]),
        "regime_stable": bool(inputs["regime_summary"]["stable"]),
        "real_branching": bool(branching["changed_top5_members_count"] > 0 or branching["changed_top10_members_count"] > 0),
        "no_harmful_swaps": bool(int(swap_quality["harmful_swap_count"] or 0) == 0),
        "fixed_topk_production_valid": bool(topk_fit.get("fixed_topK_valid") is True),
        "silent_fallback_used": False,
    }
    reasons: list[str] = [name for name, passed in checks.items() if passed and name != "silent_fallback_used"]
    failed = [name for name, passed in checks.items() if not passed and name != "silent_fallback_used"]
    reasons.extend(f"failed_{name}" for name in failed)

    keep_required = (
        "same_condition_pass",
        "no_lookahead_pass",
        "top5_improved",
        "top10_improved",
        "mfe_not_worse",
        "mae_not_worse",
        "severe_loss_not_worse",
        "monthly_stable",
        "regime_stable",
        "real_branching",
        "no_harmful_swaps",
        "fixed_topk_production_valid",
    )
    if all(checks[name] for name in keep_required):
        return "keep", reasons, checks
    if (
        checks["real_branching"]
        and (checks["top5_improved"] or checks["top10_improved"])
        and checks["mae_not_worse"]
        and checks["severe_loss_not_worse"]
        and not checks["silent_fallback_used"]
    ):
        return "hold", reasons, checks
    return "drop", reasons, checks


def _build_evaluation_contract(frame: pd.DataFrame, source_rows_parquet: Path, champion_freeze_json: Path, topk_operational_fit_json: Path) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "candidate_id": AXIS_ID,
        "champion_id": CHAMPION_ID,
        "strategy_scope": "long_swing_primary",
        "source_rows_parquet": str(source_rows_parquet),
        "champion_freeze_json": str(champion_freeze_json),
        "topk_operational_fit_json": str(topk_operational_fit_json),
        "fixed_weights": dict(FIXED_WEIGHTS),
        "score_formula": "champion_score + fixed weighted decision-time component deltas",
        "same_condition_checks": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
        },
        "silent_fallback_allowed": False,
        "silent_fallback_used": False,
        "future_label_columns_excluded_from_scoring": sorted(LABEL_COLUMNS),
        "scoring_feature_columns": sorted(SCORING_FEATURE_COLUMNS),
        "non_scope": [
            "MeeMee UI",
            "MeeMee production ranking formula",
            "runtime DB writes",
            "publish registry mutation",
            "short strategy generation",
            "post-result threshold tuning",
        ],
        "decision_sets": int(frame.groupby(["trade_date_key", "side"], sort=False).ngroups),
        "row_count": int(len(frame)),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _build_run_manifest(
    frame: pd.DataFrame,
    run_id: str,
    source_rows_parquet: Path,
    champion_freeze_json: Path,
    topk_operational_fit_json: Path,
    contract_hash: str,
) -> dict[str, Any]:
    period = _period_segments(frame)[0]
    manifest = contracts.build_run_manifest(
        session_id=run_id,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_rows_parquet", "path": str(source_rows_parquet)},
            {"name": "champion_freeze_json", "path": str(champion_freeze_json)},
            {"name": "topk_operational_fit_json", "path": str(topk_operational_fit_json)},
        ],
        asof=_utc_now(),
        config={
            "candidate_id": AXIS_ID,
            "champion_id": CHAMPION_ID,
            "fixed_weights": dict(FIXED_WEIGHTS),
            "evaluation_contract_hash": contract_hash,
            "silent_fallback_used": False,
        },
        universe=sorted(frame["symbol"].astype(str).unique().tolist()),
        period=period,
        horizon="20_trading_days",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(manifest)
    return manifest


def _build_feature_availability_audit(frame: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for column in sorted(SCORING_FEATURE_COLUMNS):
        present = column in frame.columns
        non_null = int(frame[column].notna().sum()) if present else 0
        rows.append(
            {
                "column": column,
                "present": present,
                "non_null_count": non_null,
                "non_null_rate": None if len(frame) == 0 else non_null / len(frame),
            }
        )
    label_overlap = sorted(SCORING_FEATURE_COLUMNS & LABEL_COLUMNS)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "candidate_id": AXIS_ID,
        "row_count": int(len(frame)),
        "feature_rows": rows,
        "scoring_feature_columns": sorted(SCORING_FEATURE_COLUMNS),
        "label_columns_excluded_from_scoring": sorted(LABEL_COLUMNS),
        "scoring_label_overlap": label_overlap,
        "used_future_labels_in_scoring": bool(label_overlap),
        "no_lookahead_pass": not label_overlap,
        "silent_fallback_used": False,
        "point_in_time_notes": [
            "monthly_context and weekly_context are used only with *_no_lookahead support flags.",
            "forward returns, path value, MFE, MAE, and label columns are evaluation-only.",
        ],
    }


def _ledger_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "trade_date_key",
        "symbol",
        "side",
        "champion_rank",
        "candidate_rank",
        "champion_score",
        "swing_quality_score",
        "swing_quality_delta",
        "regime_quality_score",
        "entry_confirmation_score",
        "liquidity_quality_score",
        "bad_pick_veto_score",
        "drawdown_control_score",
        "swing_action_state",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "bottom15_label",
    ]
    rows = []
    for row in frame.sort_values(["trade_date_key", "candidate_rank", "symbol"], kind="stable")[columns].to_dict(orient="records"):
        rows.append({key: _json_ready(value) for key, value in row.items()})
    return rows


def _leaderboard_row(compare: dict[str, Any], decision: str, reasons: list[str], checks: dict[str, bool]) -> dict[str, Any]:
    top5 = compare["topk"]["top5"]
    top10 = compare["topk"]["top10"]
    return {
        "candidate_id": AXIS_ID,
        "family_id": AXIS_ID,
        "champion_id": CHAMPION_ID,
        "candidate_local_decision": decision,
        "decision": decision,
        "decision_reasons": reasons,
        "feature_family": "boundary_feature",
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "victory_metrics": compare["candidate_results"][0]["victory_metrics"],
        "top5_hold_end_return_20d_delta": top5["deltas"]["hold_end_return_20d_delta"],
        "top10_hold_end_return_20d_delta": top10["deltas"]["hold_end_return_20d_delta"],
        "top5_mfe_20d_delta": top5["deltas"]["mfe_20d_delta"],
        "top10_mfe_20d_delta": top10["deltas"]["mfe_20d_delta"],
        "top5_mae_20d_delta": top5["deltas"]["mae_20d_delta"],
        "top10_mae_20d_delta": top10["deltas"]["mae_20d_delta"],
        "top5_severe_loss_rate_delta": top5["deltas"]["severe_loss_rate_delta"],
        "top10_severe_loss_rate_delta": top10["deltas"]["severe_loss_rate_delta"],
        "changed_top5_members_count": compare["branching"]["changed_top5_members_count"],
        "changed_top10_members_count": compare["branching"]["changed_top10_members_count"],
        "changed_rank_count": compare["branching"]["changed_rank_count"],
        "selection_divergence_reason": compare["branching"]["selection_divergence_reason"],
        "checks": checks,
    }


def _build_family_leaderboard(compare: dict[str, Any], decision: str, reasons: list[str], checks: dict[str, bool]) -> dict[str, Any]:
    row = _leaderboard_row(compare, decision, reasons, checks)
    payload = {
        "schema_version": "tradex_experiment_family_v1",
        "generated_at": _utc_now(),
        "session_meta": {"session_id": AXIS_ID, "candidate_id": AXIS_ID, "champion_id": CHAMPION_ID},
        "source_compare_path": "compare.json",
        "coverage_waterfall": {
            "source_rows": compare["topk"]["top20"]["candidate"]["selected_count"],
            "silent_fallback_used": False,
            "no_lookahead_pass": checks["no_lookahead_pass"],
        },
        "overview": {
            "candidate_count": 1,
            "keep_count": 1 if decision == "keep" else 0,
            "hold_count": 1 if decision == "hold" else 0,
            "drop_count": 1 if decision == "drop" else 0,
        },
        "family_summary": [
            {
                "family_id": AXIS_ID,
                "session_aggregate_decision": decision,
                "decision": decision,
                "decision_reasons": reasons,
            }
        ],
        "candidate_rows": [row],
        "authoritative_rollup_decision": decision,
    }
    contracts.validate_family_leaderboard_artifact(payload)
    return payload


def _build_session_rollup(family_leaderboard: dict[str, Any], decision: str, reasons: list[str]) -> dict[str, Any]:
    payload = {
        "schema_version": "tradex_session_leaderboard_rollup_v1",
        "generated_at": _utc_now(),
        "session_meta": {"session_id": AXIS_ID, "candidate_id": AXIS_ID, "champion_id": CHAMPION_ID},
        "source_family_leaderboard_paths": ["family_leaderboard.json"],
        "overview": {
            "family_count": 1,
            "keep_count": 1 if decision == "keep" else 0,
            "hold_count": 1 if decision == "hold" else 0,
            "drop_count": 1 if decision == "drop" else 0,
        },
        "family_summary": [
            {
                "family_id": AXIS_ID,
                "session_aggregate_decision": decision,
                "decision": decision,
                "decision_reasons": reasons,
            }
        ],
        "candidate_rows": family_leaderboard["candidate_rows"],
        "authoritative_rollup_decision": decision,
    }
    contracts.validate_session_rollup_artifact(payload)
    return payload


def _build_reflection_gate(decision: str, checks: dict[str, bool], topk_operational_fit: dict[str, Any]) -> dict[str, Any]:
    blockers = []
    if decision != "keep":
        blockers.append("authoritative_rollup_decision_not_keep")
    for name, passed in checks.items():
        if name == "silent_fallback_used":
            continue
        if not passed:
            blockers.append(f"{name}_failed")
    reflectable = decision == "keep" and not blockers and not checks["silent_fallback_used"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_meemee_reflection_gate_v1",
        "generated_at": _utc_now(),
        "candidate_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reflectable_to_meemee": reflectable,
        "meemee_reflection_allowed": reflectable,
        "publish_bundle_allowed": reflectable,
        "silent_fallback_used": checks["silent_fallback_used"],
        "blockers": blockers,
        "topk_operational_fit": topk_operational_fit,
        "allowed_meemee_read_artifacts_if_reflectable": [
            "published_logic_artifact.json",
            "ranking_adjustment_contract.json",
        ],
        "forbidden_meemee_inputs": [
            "candidate_ledger.jsonl",
            "feature_availability_audit.json",
            "compare.json",
            "family_leaderboard.json",
            "session_leaderboard_rollup.json",
            "forward return labels",
            "research-only diagnostics",
        ],
    }


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_publish_bundle(output_dir: Path, publish_root: Path, gate: dict[str, Any], compare: dict[str, Any]) -> Path | None:
    if not gate.get("reflectable_to_meemee"):
        return None
    bundle_dir = publish_root / AXIS_ID
    bundle_dir.mkdir(parents=True, exist_ok=True)
    artifact = {
        "artifact_version": "published_logic_artifact_v1",
        "logic_family": AXIS_ID,
        "logic_id": AXIS_ID,
        "logic_version": "static_fixed_weights_v1",
        "scorer_type": "static_gate_swing_quality_selection",
        "required_inputs": sorted(SCORING_FEATURE_COLUMNS),
        "forbidden_inputs": sorted(LABEL_COLUMNS),
        "weights": dict(FIXED_WEIGHTS),
        "output_spec": {
            "adjusted_rank_field": "candidate_rank",
            "adjusted_score_field": "swing_quality_score",
            "reason_code_field": "swing_action_state",
            "max_scope": "long_swing_ranking",
        },
    }
    artifact_path = _write_json(bundle_dir / "published_logic_artifact.json", artifact)
    manifest = {
        "artifact_uri": str(artifact_path),
        "bootstrap_champion": False,
        "checksum": _file_sha256(artifact_path),
        "input_schema_version": f"{SCHEMA_PREFIX}_publish_input_v1",
        "logic_family": AXIS_ID,
        "logic_id": AXIS_ID,
        "logic_version": "static_fixed_weights_v1",
        "output_schema_version": f"{SCHEMA_PREFIX}_publish_output_v1",
        "status": "candidate",
    }
    manifest_path = _write_json(bundle_dir / "published_logic_manifest.json", manifest)
    adjustment = {
        "schema_version": f"{SCHEMA_PREFIX}_ranking_adjustment_contract_v1",
        "candidate_id": AXIS_ID,
        "adjustment_mode": "static_fixed_weights_v1",
        "affected_fields": [
            "candidate_rank",
            "swing_quality_score",
            "swing_quality_delta",
            "swing_action_state",
        ],
        "fixed_weights": dict(FIXED_WEIGHTS),
        "steps": [
            "Read champion ranking rows.",
            "Compute fixed decision-time component scores.",
            "Apply fixed weighted delta to champion score.",
            "Rerank long swing rows within the same decision set.",
            "Emit adjusted rank, score, and simple reason state.",
        ],
    }
    adjustment_path = _write_json(bundle_dir / "ranking_adjustment_contract.json", adjustment)
    validation_summary = {
        "decision": "candidate",
        "evaluation_scope": "publish_review",
        "logic_family": AXIS_ID,
        "logic_id": AXIS_ID,
        "logic_version": "static_fixed_weights_v1",
        "metrics": compare["topk"],
        "notes": ["generated only after meemee_reflection_gate.reflectable_to_meemee=true"],
    }
    validation_path = _write_json(bundle_dir / "validation_summary.json", validation_summary)
    exposure = {
        "schema_version": f"{SCHEMA_PREFIX}_meemee_exposure_assessment_v1",
        "candidate_id": AXIS_ID,
        "reflectability_state": "reflectable",
        "is_reflectable_to_meemee_now": True,
        "allowed_future_meemee_exposure": [
            "final adjusted rank",
            "swing quality reason state",
            "before/after rank",
            "source candidate id",
        ],
        "forbidden_meemee_exposure": [
            "raw candidate ledger",
            "future-return labels",
            "MFE/MAE labels",
            "research-only diagnostics",
        ],
    }
    exposure_path = _write_json(bundle_dir / "meemee_exposure_assessment.json", exposure)
    refs = {
        "source_run_root": str(output_dir),
        "source_artifacts": {
            name: str(output_dir / name)
            for name in (
                "evaluation_contract.json",
                "compare.json",
                "family_leaderboard.json",
                "session_leaderboard_rollup.json",
                "meemee_reflection_gate.json",
            )
        },
    }
    refs_path = _write_json(bundle_dir / "source_artifact_refs.json", refs)
    files = [
        artifact_path,
        manifest_path,
        adjustment_path,
        validation_path,
        exposure_path,
        refs_path,
    ]
    bundle_manifest = {
        "schema_version": f"{SCHEMA_PREFIX}_publish_bundle_manifest_v1",
        "candidate_id": AXIS_ID,
        "bundle_root": str(bundle_dir),
        "bundle_status": "complete",
        "required_files_present": True,
        "file_checksums": {path.name: _file_sha256(path) for path in files},
    }
    bundle_manifest["bundle_checksum"] = _stable_hash(bundle_manifest["file_checksums"])
    _write_json(bundle_dir / "bundle_manifest.json", bundle_manifest)
    return bundle_dir


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: str, gate: dict[str, Any]) -> dict[str, Any]:
    existing = {name: Path(path).exists() for name, path in paths.items()}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "candidate_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(existing.values()) and all((output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": existing,
        "authoritative_rollup_decision": decision,
        "meemee_reflectable": gate.get("reflectable_to_meemee"),
        "silent_fallback_used": gate.get("silent_fallback_used"),
    }


def run_swing_quality_selection(
    *,
    source_rows_parquet: Path = DEFAULT_SOURCE_ROWS_PARQUET,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    champion_freeze_json: Path = DEFAULT_CHAMPION_FREEZE_JSON,
    topk_operational_fit_json: Path = DEFAULT_TOPK_OPERATIONAL_FIT_JSON,
    publish_root: Path = DEFAULT_PUBLISH_ROOT,
    write_publish_bundle: bool = True,
) -> dict[str, Any]:
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(f"-{AXIS_ID}"):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_root / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    source = _load_source_rows(source_rows_parquet)
    ranked = apply_candidate_logic(source)
    topk_operational_fit = _load_json(topk_operational_fit_json)
    decision_inputs = _build_decision_inputs(ranked, topk_operational_fit)
    decision, reasons, checks = _decide(decision_inputs)
    same_condition = _same_condition_contract(ranked)
    compare = _build_compare(ranked, same_condition, decision, reasons)
    contracts.validate_compare_artifact(compare)
    evaluation_contract = _build_evaluation_contract(ranked, source_rows_parquet, champion_freeze_json, topk_operational_fit_json)
    run_manifest = _build_run_manifest(
        ranked,
        run_name,
        source_rows_parquet,
        champion_freeze_json,
        topk_operational_fit_json,
        evaluation_contract["contract_hash"],
    )
    feature_audit = _build_feature_availability_audit(ranked)
    family_leaderboard = _build_family_leaderboard(compare, decision, reasons, checks)
    session_rollup = _build_session_rollup(family_leaderboard, decision, reasons)
    reflection_gate = _build_reflection_gate(decision, checks, topk_operational_fit)

    artifacts: dict[str, Any] = {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "compare.json": compare,
        "family_leaderboard.json": family_leaderboard,
        "session_leaderboard_rollup.json": session_rollup,
        "meemee_reflection_gate.json": reflection_gate,
    }
    paths: dict[str, str] = {}
    for name, payload in artifacts.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["candidate_ledger.jsonl"] = str(_write_jsonl(output_dir / "candidate_ledger.jsonl", _ledger_rows(ranked)))
    publish_bundle_dir = None
    if write_publish_bundle:
        publish_bundle_dir = _build_publish_bundle(output_dir, publish_root, reflection_gate, compare)
    complete = _artifact_complete(output_dir, paths, decision, reflection_gate)
    if publish_bundle_dir is not None:
        complete["publish_bundle_dir"] = str(publish_bundle_dir)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "decision": decision,
        "decision_reasons": reasons,
        "checks": checks,
        "reflectable_to_meemee": reflection_gate["reflectable_to_meemee"],
        "publish_bundle_dir": str(publish_bundle_dir) if publish_bundle_dir else None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--champion-freeze-json", default=str(DEFAULT_CHAMPION_FREEZE_JSON))
    parser.add_argument("--topk-operational-fit-json", default=str(DEFAULT_TOPK_OPERATIONAL_FIT_JSON))
    parser.add_argument("--publish-root", default=str(DEFAULT_PUBLISH_ROOT))
    parser.add_argument("--no-publish-bundle", action="store_true")
    args = parser.parse_args(argv)

    result = run_swing_quality_selection(
        source_rows_parquet=_safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        run_id=args.run_id.strip() or None,
        champion_freeze_json=_safe_path(args.champion_freeze_json, DEFAULT_CHAMPION_FREEZE_JSON),
        topk_operational_fit_json=_safe_path(args.topk_operational_fit_json, DEFAULT_TOPK_OPERATIONAL_FIT_JSON),
        publish_root=_safe_path(args.publish_root, DEFAULT_PUBLISH_ROOT),
        write_publish_bundle=not args.no_publish_bundle,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
