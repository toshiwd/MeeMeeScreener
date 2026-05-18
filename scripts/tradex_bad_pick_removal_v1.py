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


AXIS_ID = "bad_pick_removal_v1"
CHAMPION_ID = "champion_top5_capture_boundary_promoter_v1"
SCHEMA_PREFIX = "tradex_bad_pick_removal_v1"

DEFAULT_SOURCE_ROWS_PARQUET = Path(
    r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\bad_pick_removal_v1")
DEFAULT_CHAMPION_FREEZE_JSON = Path(
    r"G:\Tradex\research_freeze_summaries\champion_top5_capture_boundary_promoter_v1_final\20260505T070512Z\final_freeze_decision.json"
)
DEFAULT_TOPK_OPERATIONAL_FIT_JSON = Path(
    r"G:\Tradex\topk_validity_audit_v1\20260512T090000Z-topk_validity_audit_v1\topk_operational_fit.json"
)

TOP_K_VALUES = (5, 10, 20)
PRIMARY_TOP_K = 10
HOLDING_DAYS = 20
SEVERE_LOSS_THRESHOLD = -0.15
FIXED_CONDITIONS = {
    "same_universe": True,
    "same_period": True,
    "same_top_k": list(TOP_K_VALUES),
    "same_regime_condition": True,
    "same_cost_slippage": True,
    "same_artifact_detail_level": True,
}

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
    "candle_upper_wick_ratio",
    "candle_lower_wick_ratio",
    "candle_triplet_up_prob",
    "candle_triplet_down_prob",
    "gap_pct",
    "vol_ratio5_20",
    "prefilter_bucket",
    "prefilter_reason",
    "shape_classification",
}

FIXED_GUARD_CONFIG = {
    "max_penalty": 0.085,
    "upper_wick_threshold": 0.58,
    "large_gap_threshold": 0.055,
    "volume_spike_threshold": 2.35,
    "triplet_down_margin": 0.18,
    "guard_activation_score": 0.35,
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


def _text_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return frame.get(column, pd.Series("", index=frame.index)).fillna("").astype(str).str.lower()


def _numeric_series(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(frame.get(column, default), errors="coerce").fillna(default)


def load_source_rows_from_frame(frame: pd.DataFrame, *, limit_anchor_dates: int | None = None) -> pd.DataFrame:
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
        "candle_upper_wick_ratio",
        "candle_lower_wick_ratio",
        "candle_triplet_up_prob",
        "candle_triplet_down_prob",
        "gap_pct",
        "vol_ratio5_20",
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
    if limit_anchor_dates is not None and int(limit_anchor_dates) > 0:
        keep_dates = sorted(working["trade_date_key"].unique().tolist())[: int(limit_anchor_dates)]
        working = working[working["trade_date_key"].isin(keep_dates)].copy()
    working.sort_values(["trade_date_key", "side", "champion_rank", "symbol"], inplace=True, kind="stable")
    return working.reset_index(drop=True)


def _load_source_rows(source_rows_parquet: Path, *, limit_anchor_dates: int | None = None) -> pd.DataFrame:
    if not source_rows_parquet.exists():
        raise FileNotFoundError(f"source rows parquet not found: {source_rows_parquet}")
    return load_source_rows_from_frame(pd.read_parquet(source_rows_parquet), limit_anchor_dates=limit_anchor_dates)


def _load_frame(source_rows_parquet: Path, *, limit_anchor_dates: int | None = None) -> pd.DataFrame:
    frame = _load_source_rows(source_rows_parquet, limit_anchor_dates=limit_anchor_dates)
    if "anchor_date" not in frame.columns and "trade_date_key" in frame.columns:
        frame = frame.copy()
        frame["anchor_date"] = frame["trade_date_key"]
    return frame


def _contains_any(series: pd.Series, terms: tuple[str, ...]) -> pd.Series:
    mask = pd.Series(False, index=series.index)
    for term in terms:
        mask = mask | series.str.contains(term, regex=False)
    return mask


def _guard_reason_lists(frame: pd.DataFrame) -> list[list[str]]:
    upper = _numeric_series(frame, "candle_upper_wick_ratio")
    lower = _numeric_series(frame, "candle_lower_wick_ratio")
    triplet_up = _numeric_series(frame, "candle_triplet_up_prob", 0.5)
    triplet_down = _numeric_series(frame, "candle_triplet_down_prob", 0.5)
    gap = _numeric_series(frame, "gap_pct")
    volume = _numeric_series(frame, "vol_ratio5_20", 1.0)
    monthly = _text_series(frame, "monthly_context")
    weekly = _text_series(frame, "weekly_context")
    shape = _text_series(frame, "shape_classification")
    bucket = _text_series(frame, "prefilter_bucket")
    reason = _text_series(frame, "prefilter_reason")

    reasons: list[list[str]] = []
    for idx in frame.index:
        row_reasons: list[str] = []
        if upper.loc[idx] >= FIXED_GUARD_CONFIG["upper_wick_threshold"] and upper.loc[idx] > lower.loc[idx] + 0.20:
            row_reasons.append("upper_wick_exhaustion")
        if gap.loc[idx] >= FIXED_GUARD_CONFIG["large_gap_threshold"]:
            row_reasons.append("large_gap_up_risk")
        if volume.loc[idx] >= FIXED_GUARD_CONFIG["volume_spike_threshold"] and lower.loc[idx] < 0.18:
            row_reasons.append("volume_spike_without_support")
        if triplet_down.loc[idx] >= triplet_up.loc[idx] + FIXED_GUARD_CONFIG["triplet_down_margin"]:
            row_reasons.append("downside_triplet_pressure")
        context_text = f"{monthly.loc[idx]} {weekly.loc[idx]}"
        if any(token in context_text for token in ("overextended", "top_warning", "break_risk", "risk_off", "down")):
            row_reasons.append("weak_or_overextended_context")
        shape_text = f"{shape.loc[idx]} {bucket.loc[idx]} {reason.loc[idx]}"
        if any(token in shape_text for token in ("negative", "exclude", "bad_pick", "shape_missing")):
            row_reasons.append("prefilter_or_shape_risk")
        reasons.append(row_reasons)
    return reasons


def _guard_score_from_reasons(reasons: list[list[str]]) -> pd.Series:
    weights = {
        "upper_wick_exhaustion": 0.24,
        "large_gap_up_risk": 0.18,
        "volume_spike_without_support": 0.16,
        "downside_triplet_pressure": 0.18,
        "weak_or_overextended_context": 0.16,
        "prefilter_or_shape_risk": 0.22,
    }
    scores = []
    for row_reasons in reasons:
        scores.append(min(1.0, sum(weights.get(reason, 0.0) for reason in row_reasons)))
    return pd.Series(scores)


def apply_candidate_logic(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    reason_lists = _guard_reason_lists(working)
    guard_score = _guard_score_from_reasons(reason_lists)
    guard_score.index = working.index
    active = guard_score.ge(FIXED_GUARD_CONFIG["guard_activation_score"])
    working["bad_pick_guard_score"] = guard_score
    working["bad_pick_guard_active"] = active
    working["bad_pick_guard_reasons"] = [";".join(reasons) if reasons else "no_guard" for reasons in reason_lists]
    working["bad_pick_guard_penalty"] = guard_score * float(FIXED_GUARD_CONFIG["max_penalty"])
    working.loc[~active, "bad_pick_guard_penalty"] = 0.0
    working["challenger_score"] = working["champion_score"] - working["bad_pick_guard_penalty"]
    ranked_parts: list[pd.DataFrame] = []
    for _, group in working.groupby(["trade_date_key", "side"], sort=True):
        ordered = group.sort_values(["challenger_score", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
        ordered["challenger_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True) if ranked_parts else working.assign(challenger_rank=pd.Series(dtype="int"))
    for top_k in TOP_K_VALUES:
        ranked[f"challenger_selected_top{top_k}"] = ranked["challenger_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        ranked[f"changed_top{top_k}_member"] = ranked[f"challenger_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["challenger_rank"].astype("Int64") != ranked["champion_rank"].astype("Int64")
    return ranked


def _variant_masks(frame: pd.DataFrame) -> dict[str, tuple[str, pd.Series, list[str]]]:
    reasons = _guard_reason_lists(frame)
    reason_text = pd.Series([";".join(items) for items in reasons], index=frame.index)
    breakout_trap = reason_text.str.contains("upper_wick_exhaustion", regex=False) | reason_text.str.contains(
        "large_gap_up_risk",
        regex=False,
    )
    return {
        "bad_pick_removal_v1_breakout_trap_only": (
            "Penalize only breakout-trap style exhaustion or large-gap risk rows.",
            breakout_trap.fillna(False).astype(bool),
            ["candle_upper_wick_ratio", "candle_lower_wick_ratio", "gap_pct"],
        )
    }


def _rank_with_penalty(frame: pd.DataFrame, penalty_mask: pd.Series) -> pd.DataFrame:
    working = frame.copy()
    mask = penalty_mask.reindex(working.index, fill_value=False).fillna(False).astype(bool)
    working["bad_pick_guard_score"] = mask.astype(float)
    working["bad_pick_guard_active"] = mask
    working["bad_pick_guard_reasons"] = mask.map(lambda active: "breakout_trap_only" if active else "no_guard")
    working["bad_pick_guard_penalty"] = mask.astype(float) * float(FIXED_GUARD_CONFIG["max_penalty"])
    working["bad_pick_veto"] = mask
    working["challenger_score"] = working["champion_score"] - working["bad_pick_guard_penalty"]
    ranked_parts: list[pd.DataFrame] = []
    for _, group in working.groupby(["trade_date_key", "side"], sort=True):
        ordered = group.sort_values(["challenger_score", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
        ordered["challenger_rank"] = range(1, len(ordered) + 1)
        ranked_parts.append(ordered)
    ranked = pd.concat(ranked_parts, ignore_index=True) if ranked_parts else working.assign(challenger_rank=pd.Series(dtype="int"))
    for top_k in TOP_K_VALUES:
        ranked[f"challenger_selected_top{top_k}"] = ranked["challenger_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
        ranked[f"changed_top{top_k}_member"] = ranked[f"challenger_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["challenger_rank"].astype("Int64") != ranked["champion_rank"].astype("Int64")
    if "anchor_date" not in ranked.columns and "trade_date_key" in ranked.columns:
        ranked["anchor_date"] = ranked["trade_date_key"]
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


def _metric_deltas(champion: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    return {
        "hold_end_return_20d_delta": _delta(challenger["hold_end_return_20d"], champion["hold_end_return_20d"]),
        "mfe_20d_delta": _delta(challenger["mfe_20d"], champion["mfe_20d"]),
        "mae_20d_delta": _delta(challenger["mae_20d"], champion["mae_20d"]),
        "win_rate_hold_end_delta": _delta(challenger["win_rate_hold_end"], champion["win_rate_hold_end"]),
        "severe_loss_rate_delta": _delta(challenger["severe_loss_rate"], champion["severe_loss_rate"]),
        "bottom15_count_delta": int(challenger["bottom15_count"] - champion["bottom15_count"]),
        "bad_pick_removal_count_delta": int(champion["bottom15_count"] - challenger["bottom15_count"]),
        "max_drawdown_delta": _delta(challenger["max_drawdown"], champion["max_drawdown"]),
        "path_value_score_v1_delta": _delta(challenger["path_value_score_v1"], champion["path_value_score_v1"]),
    }


def _changed_rows(frame: pd.DataFrame, top_k: int, *, added: bool) -> pd.DataFrame:
    champion = frame[f"champion_selected_top{top_k}"].fillna(False).astype(bool)
    challenger = frame[f"challenger_selected_top{top_k}"].fillna(False).astype(bool)
    if added:
        return frame[challenger & ~champion].copy()
    return frame[champion & ~challenger].copy()


def _row_quality(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {
            "row_count": 0,
            "hold_end_return_20d": None,
            "mfe_20d": None,
            "mae_20d": None,
            "severe_loss_rate": None,
            "bottom15_count": 0,
        }
    ret = pd.to_numeric(rows["forward_ret_20d"], errors="coerce")
    severe = rows["bottom15_label"].fillna(False).astype(bool) | ret.le(SEVERE_LOSS_THRESHOLD).fillna(False)
    return {
        "row_count": int(len(rows)),
        "hold_end_return_20d": _mean_or_none(ret.tolist()),
        "mfe_20d": _mean_or_none(pd.to_numeric(rows["mfe_20d"], errors="coerce").tolist()),
        "mae_20d": _mean_or_none(pd.to_numeric(rows["mae_20d"], errors="coerce").tolist()),
        "severe_loss_rate": _rate_or_none(severe.tolist()),
        "bottom15_count": int(rows["bottom15_label"].fillna(False).astype(bool).sum()),
    }


def _harmful_swap_summary(frame: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    harmful_count = 0
    for top_k in TOP_K_VALUES:
        added = _changed_rows(frame, top_k, added=True)
        removed = _changed_rows(frame, top_k, added=False)
        added_quality = _row_quality(added)
        removed_quality = _row_quality(removed)
        harmful = bool(
            added_quality["row_count"]
            and removed_quality["row_count"]
            and (
                (
                    added_quality["hold_end_return_20d"] is not None
                    and removed_quality["hold_end_return_20d"] is not None
                    and added_quality["hold_end_return_20d"] < removed_quality["hold_end_return_20d"]
                )
                or (
                    added_quality["mae_20d"] is not None
                    and removed_quality["mae_20d"] is not None
                    and added_quality["mae_20d"] < removed_quality["mae_20d"]
                )
                or (
                    added_quality["severe_loss_rate"] is not None
                    and removed_quality["severe_loss_rate"] is not None
                    and added_quality["severe_loss_rate"] > removed_quality["severe_loss_rate"]
                )
            )
        )
        harmful_count += int(harmful)
        out[f"top{top_k}"] = {
            "added": added_quality,
            "removed": removed_quality,
            "added_minus_removed_hold_end_return_20d": _delta(added_quality["hold_end_return_20d"], removed_quality["hold_end_return_20d"]),
            "added_minus_removed_mae_20d": _delta(added_quality["mae_20d"], removed_quality["mae_20d"]),
            "added_minus_removed_severe_loss_rate": _delta(added_quality["severe_loss_rate"], removed_quality["severe_loss_rate"]),
            "harmful_swap_worse": harmful,
        }
    out["harmful_swap_worse_count"] = harmful_count
    return out


def _branching(frame: pd.DataFrame) -> dict[str, Any]:
    changed_top5 = int(frame["changed_top5_member"].fillna(False).astype(bool).sum())
    changed_top10 = int(frame["changed_top10_member"].fillna(False).astype(bool).sum())
    changed_top20 = int(frame["changed_top20_member"].fillna(False).astype(bool).sum())
    changed_rank = int(frame["rank_changed"].fillna(False).astype(bool).sum())
    if changed_top5 or changed_top10:
        reason = "bad_pick_guard_changed_primary_topk"
    elif changed_top20:
        reason = "bad_pick_guard_changed_top20_only"
    else:
        reason = "no_meaningful_branching"
    return {
        "changed_top5_members_count": changed_top5,
        "changed_top10_members_count": changed_top10,
        "changed_top20_members_count": changed_top20,
        "changed_rank_count": changed_rank,
        "selection_divergence_reason": reason,
    }


def _period_segments(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return [{"label": "all", "start_date": "unknown", "end_date": "unknown"}]
    return [{"label": "all", "start_date": str(frame["trade_date_key"].min()), "end_date": str(frame["trade_date_key"].max())}]


def _same_condition_contract(frame: pd.DataFrame) -> dict[str, Any]:
    return contracts.build_same_condition_contract(
        universe=sorted(frame["symbol"].astype(str).unique().tolist()),
        period_segments=_period_segments(frame),
        top_k=PRIMARY_TOP_K,
        regime="all_long_swing",
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        feature_family="bad_pick_removal",
    ).to_dict()


def _stability_summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    rows = []
    for key, group in frame.groupby(column, sort=True):
        champion = _topk_metrics(group, "champion", PRIMARY_TOP_K)
        challenger = _topk_metrics(group, "challenger", PRIMARY_TOP_K)
        deltas = _metric_deltas(champion, challenger)
        rows.append(
            {
                column: str(key),
                "decision_sets": int(group.groupby(["trade_date_key", "side"], sort=False).ngroups),
                "top10_hold_end_return_20d_delta": deltas["hold_end_return_20d_delta"],
                "top10_mae_20d_delta": deltas["mae_20d_delta"],
                "top10_mfe_20d_delta": deltas["mfe_20d_delta"],
                "top10_severe_loss_rate_delta": deltas["severe_loss_rate_delta"],
                "top10_bad_pick_removal_count_delta": deltas["bad_pick_removal_count_delta"],
            }
        )
    improved = [row for row in rows if (row["top10_hold_end_return_20d_delta"] is not None and row["top10_hold_end_return_20d_delta"] >= 0.0)]
    degraded = [row for row in rows if (row["top10_hold_end_return_20d_delta"] is not None and row["top10_hold_end_return_20d_delta"] < 0.0)]
    worst = min(
        (row["top10_hold_end_return_20d_delta"] for row in rows if row["top10_hold_end_return_20d_delta"] is not None),
        default=None,
    )
    return {
        "rows": rows,
        "bucket_count": len(rows),
        "improved_or_flat_bucket_count": len(improved),
        "degraded_bucket_count": len(degraded),
        "worst_top10_hold_end_return_20d_delta": worst,
        "stable": bool(rows) and len(improved) >= len(degraded) and (worst is None or worst >= -0.02),
    }


def _victory_metrics(frame: pd.DataFrame, prefix: str = "challenger", top_k: int = PRIMARY_TOP_K) -> dict[str, Any]:
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


def _decision_inputs(frame: pd.DataFrame, topk_operational_fit: dict[str, Any]) -> dict[str, Any]:
    topk = {}
    for top_k in TOP_K_VALUES:
        champion = _topk_metrics(frame, "champion", top_k)
        challenger = _topk_metrics(frame, "challenger", top_k)
        topk[f"top{top_k}"] = {
            "champion": champion,
            "challenger": challenger,
            "deltas": _metric_deltas(champion, challenger),
            "changed_member_count": int(frame[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum()),
        }
    return {
        "topk": topk,
        "harmful_swap": _harmful_swap_summary(frame),
        "branching": _branching(frame),
        "month_stability": _stability_summary(frame, "month_bucket"),
        "regime_stability": _stability_summary(frame, "regime_label"),
        "topk_operational_fit": topk_operational_fit,
    }


def _decide(inputs: dict[str, Any]) -> tuple[str, list[str], dict[str, bool]]:
    top5 = inputs["topk"]["top5"]["deltas"]
    top10 = inputs["topk"]["top10"]["deltas"]
    risk_reduced = bool(
        (top5["bad_pick_removal_count_delta"] or 0) > 0
        or (top10["bad_pick_removal_count_delta"] or 0) > 0
        or (top5["severe_loss_rate_delta"] is not None and top5["severe_loss_rate_delta"] < 0.0)
        or (top10["severe_loss_rate_delta"] is not None and top10["severe_loss_rate_delta"] < 0.0)
    )
    checks = {
        "same_condition_pass": True,
        "no_lookahead_pass": True,
        "real_branching": bool(inputs["branching"]["changed_top5_members_count"] > 0 or inputs["branching"]["changed_top10_members_count"] > 0),
        "top5_not_worse": bool(
            (top5["hold_end_return_20d_delta"] is None or top5["hold_end_return_20d_delta"] >= 0.0)
            and (top5["mfe_20d_delta"] is None or top5["mfe_20d_delta"] >= 0.0)
            and (top5["mae_20d_delta"] is None or top5["mae_20d_delta"] >= 0.0)
        ),
        "top10_not_primary_reason_only": bool((top5["hold_end_return_20d_delta"] or 0.0) >= 0.0 or risk_reduced),
        "mae_not_worse": bool((top5["mae_20d_delta"] is None or top5["mae_20d_delta"] >= 0.0) and (top10["mae_20d_delta"] is None or top10["mae_20d_delta"] >= 0.0)),
        "mfe_not_worse": bool((top5["mfe_20d_delta"] is None or top5["mfe_20d_delta"] >= 0.0) and (top10["mfe_20d_delta"] is None or top10["mfe_20d_delta"] >= 0.0)),
        "severe_loss_not_worse": bool(
            (top5["severe_loss_rate_delta"] is None or top5["severe_loss_rate_delta"] <= 0.0)
            and (top10["severe_loss_rate_delta"] is None or top10["severe_loss_rate_delta"] <= 0.0)
        ),
        "harmful_swap_not_worse": bool(int(inputs["harmful_swap"]["harmful_swap_worse_count"] or 0) == 0),
        "bad_pick_risk_reduced": risk_reduced,
        "monthly_stable": bool(inputs["month_stability"]["stable"]),
        "regime_stable": bool(inputs["regime_stability"]["stable"]),
        "fixed_topk_production_valid": bool(inputs["topk_operational_fit"].get("fixed_topK_valid") is True),
        "silent_fallback_used": False,
    }
    reasons = [name for name, passed in checks.items() if passed and name != "silent_fallback_used"]
    failed = [name for name, passed in checks.items() if not passed and name != "silent_fallback_used"]
    reasons.extend(f"failed_{name}" for name in failed)

    keep_required = (
        "same_condition_pass",
        "no_lookahead_pass",
        "real_branching",
        "top5_not_worse",
        "mae_not_worse",
        "mfe_not_worse",
        "severe_loss_not_worse",
        "harmful_swap_not_worse",
        "bad_pick_risk_reduced",
        "monthly_stable",
        "regime_stable",
        "fixed_topk_production_valid",
    )
    if all(checks[name] for name in keep_required):
        return "keep", reasons, checks
    if (
        checks["real_branching"]
        and checks["top5_not_worse"]
        and checks["mae_not_worse"]
        and checks["severe_loss_not_worse"]
        and checks["harmful_swap_not_worse"]
        and checks["bad_pick_risk_reduced"]
    ):
        return "hold", reasons, checks
    return "drop", reasons, checks


def _build_compare(frame: pd.DataFrame, same_condition: dict[str, Any], decision: str, reasons: list[str], inputs: dict[str, Any]) -> dict[str, Any]:
    candidate_result = {
        "candidate_id": AXIS_ID,
        "candidate_local_decision": decision,
        "decision": decision,
        "decision_reasons": reasons,
        "feature_family": "bad_pick_removal",
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "victory_metrics": _victory_metrics(frame, "challenger", PRIMARY_TOP_K),
        "long_horizon_regime_score": 0.0,
        "recent_adaptation_score": _mean_or_none(frame["bad_pick_guard_score"].tolist()),
        "topk_metrics": inputs["topk"],
        "harmful_swap": inputs["harmful_swap"],
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
        "topk": inputs["topk"],
        "harmful_swap": inputs["harmful_swap"],
        "branching": inputs["branching"],
        "month_stability": inputs["month_stability"],
        "regime_stability": inputs["regime_stability"],
    }


def _build_evaluation_contract(frame: pd.DataFrame, source_rows_parquet: Path, champion_freeze_json: Path, topk_operational_fit_json: Path) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "candidate_id": AXIS_ID,
        "champion_id": CHAMPION_ID,
        "strategy_scope": "long_swing_bad_pick_removal_only",
        "source_rows_parquet": str(source_rows_parquet),
        "champion_freeze_json": str(champion_freeze_json),
        "topk_operational_fit_json": str(topk_operational_fit_json),
        "fixed_guard_config": dict(FIXED_GUARD_CONFIG),
        "score_formula": "champion_score - fixed downside-risk guard penalty",
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
        "single_axis": "downside_risk_guard",
        "non_scope": [
            "MeeMee UI",
            "MeeMee production ranking formula",
            "runtime DB writes",
            "publish registry mutation",
            "publish bundle generation",
            "momentum boost",
            "quality boost",
            "regime redesign",
            "image rerank",
            "symbol-specific adjustment",
        ],
        "decision_sets": int(frame.groupby(["trade_date_key", "side"], sort=False).ngroups),
        "row_count": int(len(frame)),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _build_run_manifest(frame: pd.DataFrame, run_id: str, source_rows_parquet: Path, champion_freeze_json: Path, topk_operational_fit_json: Path, contract_hash: str) -> dict[str, Any]:
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
            "fixed_guard_config": dict(FIXED_GUARD_CONFIG),
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
        rows.append({"column": column, "present": present, "non_null_count": non_null, "non_null_rate": None if len(frame) == 0 else non_null / len(frame)})
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
    }


def _ledger_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "trade_date_key",
        "symbol",
        "side",
        "champion_rank",
        "challenger_rank",
        "champion_score",
        "challenger_score",
        "bad_pick_guard_score",
        "bad_pick_guard_active",
        "bad_pick_guard_penalty",
        "bad_pick_guard_reasons",
        "forward_ret_20d",
        "mfe_20d",
        "mae_20d",
        "bottom15_label",
    ]
    rows = []
    for row in frame.sort_values(["trade_date_key", "challenger_rank", "symbol"], kind="stable")[columns].to_dict(orient="records"):
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
        "feature_family": "bad_pick_removal",
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
        "top5_bad_pick_removal_count_delta": top5["deltas"]["bad_pick_removal_count_delta"],
        "top10_bad_pick_removal_count_delta": top10["deltas"]["bad_pick_removal_count_delta"],
        "harmful_swap_worse_count": compare["harmful_swap"]["harmful_swap_worse_count"],
        "changed_top5_members_count": compare["branching"]["changed_top5_members_count"],
        "changed_top10_members_count": compare["branching"]["changed_top10_members_count"],
        "changed_rank_count": compare["branching"]["changed_rank_count"],
        "selection_divergence_reason": compare["branching"]["selection_divergence_reason"],
        "checks": checks,
    }


def _build_family_leaderboard(compare: dict[str, Any], decision: str, reasons: list[str], checks: dict[str, bool]) -> dict[str, Any]:
    payload = {
        "schema_version": "tradex_experiment_family_v1",
        "generated_at": _utc_now(),
        "session_meta": {"session_id": AXIS_ID, "candidate_id": AXIS_ID, "champion_id": CHAMPION_ID},
        "source_compare_path": "compare.json",
        "coverage_waterfall": {
            "source_rows": compare["topk"]["top20"]["challenger"]["selected_count"],
            "silent_fallback_used": False,
            "no_lookahead_pass": checks["no_lookahead_pass"],
        },
        "overview": {
            "candidate_count": 1,
            "keep_count": 1 if decision == "keep" else 0,
            "hold_count": 1 if decision == "hold" else 0,
            "drop_count": 1 if decision == "drop" else 0,
        },
        "family_summary": [{"family_id": AXIS_ID, "session_aggregate_decision": decision, "decision": decision, "decision_reasons": reasons}],
        "candidate_rows": [_leaderboard_row(compare, decision, reasons, checks)],
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
        "family_summary": [{"family_id": AXIS_ID, "session_aggregate_decision": decision, "decision": decision, "decision_reasons": reasons}],
        "candidate_rows": family_leaderboard["candidate_rows"],
        "authoritative_rollup_decision": decision,
    }
    contracts.validate_session_rollup_artifact(payload)
    return payload


def _build_reflection_gate(decision: str, checks: dict[str, bool], topk_operational_fit: dict[str, Any]) -> dict[str, Any]:
    blockers = ["publish_bundle_creation_forbidden_this_run"]
    if decision != "keep":
        blockers.append("authoritative_rollup_decision_not_keep")
    for name, passed in checks.items():
        if name == "silent_fallback_used":
            continue
        if not passed:
            blockers.append(f"{name}_failed")
    theoretical_reflectable = decision == "keep" and checks["fixed_topk_production_valid"] and not checks["silent_fallback_used"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_meemee_reflection_gate_v1",
        "generated_at": _utc_now(),
        "candidate_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reflectable_to_meemee": False,
        "theoretical_reflectable_if_publish_allowed": theoretical_reflectable,
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": checks["silent_fallback_used"],
        "blockers": blockers,
        "topk_operational_fit": topk_operational_fit,
        "allowed_meemee_read_artifacts_if_future_reflectable": [
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
        "publish_bundle_created": False,
    }


def run_bad_pick_removal_v1(
    *,
    source_rows_parquet: str | Path = DEFAULT_SOURCE_ROWS_PARQUET,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    champion_freeze_json: str | Path = DEFAULT_CHAMPION_FREEZE_JSON,
    topk_operational_fit_json: str | Path = DEFAULT_TOPK_OPERATIONAL_FIT_JSON,
    limit_anchor_dates: int | None = None,
) -> dict[str, Any]:
    source_path = _safe_path(source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    champion_path = _safe_path(champion_freeze_json, DEFAULT_CHAMPION_FREEZE_JSON)
    topk_path = _safe_path(topk_operational_fit_json, DEFAULT_TOPK_OPERATIONAL_FIT_JSON)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    source = _load_source_rows(source_path, limit_anchor_dates=limit_anchor_dates)
    ranked = apply_candidate_logic(source)
    topk_operational_fit = _load_json(topk_path)
    inputs = _decision_inputs(ranked, topk_operational_fit)
    decision, reasons, checks = _decide(inputs)
    same_condition = _same_condition_contract(ranked)
    compare = _build_compare(ranked, same_condition, decision, reasons, inputs)
    contracts.validate_compare_artifact(compare)
    evaluation_contract = _build_evaluation_contract(ranked, source_path, champion_path, topk_path)
    run_manifest = _build_run_manifest(ranked, run_name, source_path, champion_path, topk_path, evaluation_contract["contract_hash"])
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
    complete = _artifact_complete(output_dir, paths, decision, reflection_gate)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "decision": decision,
        "decision_reasons": reasons,
        "checks": checks,
        "reflectable_to_meemee": reflection_gate["reflectable_to_meemee"],
        "publish_bundle_created": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--champion-freeze-json", default=str(DEFAULT_CHAMPION_FREEZE_JSON))
    parser.add_argument("--topk-operational-fit-json", default=str(DEFAULT_TOPK_OPERATIONAL_FIT_JSON))
    parser.add_argument("--limit-anchor-dates", type=int, default=None)
    args = parser.parse_args(argv)

    result = run_bad_pick_removal_v1(
        source_rows_parquet=args.source_rows_parquet,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        champion_freeze_json=args.champion_freeze_json,
        topk_operational_fit_json=args.topk_operational_fit_json,
        limit_anchor_dates=args.limit_anchor_dates,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
