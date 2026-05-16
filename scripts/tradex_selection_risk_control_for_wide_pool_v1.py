from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_wide_strength_pool_upside_rerank_v1 as wide_mod


AXIS_ID = "selection_risk_control_for_wide_pool_v1"
SCHEMA_PREFIX = "tradex_selection_risk_control_for_wide_pool_v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\selection_risk_control_for_wide_pool_v1")

TOP_K = 3
RANDOM_SEED = 20260513
BASE_FAMILY_ID = "momentum_continuation_soft_boost_v1"
EMBARGO_CALENDAR_DAYS = 32
MIN_KEY_HISTORY = 12
MIN_PARENT_HISTORY = 25
MIN_GLOBAL_HISTORY = 25

RISK_FAMILIES = (
    "baseline_momentum_continuation_soft_boost_v1",
    "severe_loss_soft_penalty_v1",
    "extended_continuation_vs_blowoff_risk_v1",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "risk_control_contract.json",
    "feature_availability_audit.json",
    "split_contract.json",
    "risk_family_contract.json",
    "selected_event_risk_ledger.jsonl",
    "date_level_risk_selection_ledger.jsonl",
    "risk_leaderboard.json",
    "top3_risk_report.json",
    "upside_preservation_report.json",
    "negative_guard_decomposition_report.json",
    "safe_full_soft_tag_report.json",
    "oracle_regret_report.json",
    "time_block_stability.json",
    "threshold_calibration_report.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

SCORE_INPUT_COLUMNS = set(wide_mod.SCORE_INPUT_COLUMNS) | {
    "past_only_severe_loss_risk_estimate",
    "past_only_bad_pick_risk_estimate",
    "past_only_mae_abs_risk_estimate",
    "continuation_signal_score",
    "blowoff_signal_score",
}
TRAINING_LABEL_COLUMNS = {
    "ret20_fwd",
    "mfe20",
    "mae20",
    "severe_loss20",
    "is_future_top10_by_ret20",
    "is_big_winner_ret20_ge_10pct",
    "is_big_winner_MFE20_ge_15pct",
}
FUTURE_LABEL_COLUMNS = wide_mod.FUTURE_LABEL_COLUMNS


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
    return json.loads(path.read_text(encoding="utf-8"))


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return _safe_path(root, default_root) / run_id


def _safe_rate(count: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return float(count) / float(total)


def _state_score(series: pd.Series, mapping: dict[str, float], default: float = 0.0) -> pd.Series:
    return series.astype(str).map(mapping).fillna(default).astype(float)


def _new_stats() -> dict[str, float]:
    return {"n": 0.0, "severe": 0.0, "mae_abs": 0.0, "bad_pick": 0.0}


def _add_stats(stats: dict[str, float], row: Any) -> None:
    stats["n"] += 1.0
    stats["severe"] += 1.0 if bool(row.severe_loss20) else 0.0
    stats["mae_abs"] += abs(float(row.mae20))
    stats["bad_pick"] += 1.0 if bool(row.severe_loss20) or float(row.ret20_fwd) <= -0.05 else 0.0


def _stats_estimate(stats: dict[str, float]) -> tuple[float, float, float]:
    n = max(float(stats["n"]), 1.0)
    return float(stats["severe"]) / n, float(stats["bad_pick"]) / n, float(stats["mae_abs"]) / n


def _risk_key(row: Any) -> tuple[Any, ...]:
    return (
        bool(row.negative_guard_match),
        str(row.pre_ret20_state),
        str(row.pre_ret5_state),
        str(row.pre_ma20_path_state),
        str(row.pre_ma60_context_state),
        str(row.weekly_prior_state),
        str(row.monthly_prior_state),
        str(row.pre_wick_warning_state),
        str(row.pre_volume_state),
    )


def _parent_key(row: Any) -> tuple[Any, ...]:
    return (
        bool(row.negative_guard_match),
        str(row.pre_ma20_path_state),
        str(row.pre_wick_warning_state),
        str(row.pre_volume_state),
    )


def add_past_only_risk_estimates(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy().sort_values(["event_ymd", "code"]).reset_index(drop=True)
    frame["event_dt"] = pd.to_datetime(frame["event_date"])
    source = frame.sort_values(["event_dt", "code"]).reset_index(drop=True)
    key_stats: defaultdict[tuple[Any, ...], dict[str, float]] = defaultdict(_new_stats)
    parent_stats: defaultdict[tuple[Any, ...], dict[str, float]] = defaultdict(_new_stats)
    global_stats = _new_stats()
    add_pointer = 0
    rows: list[dict[str, Any]] = []
    for event_dt, group in frame.groupby("event_dt", sort=True):
        cutoff = event_dt - pd.Timedelta(days=EMBARGO_CALENDAR_DAYS)
        while add_pointer < len(source) and source.loc[add_pointer, "event_dt"] <= cutoff:
            row = source.loc[add_pointer]
            row_tuple = row.to_frame().T.itertuples(index=False).__next__()
            _add_stats(global_stats, row_tuple)
            _add_stats(key_stats[_risk_key(row_tuple)], row_tuple)
            _add_stats(parent_stats[_parent_key(row_tuple)], row_tuple)
            add_pointer += 1
        for row in group.itertuples(index=False):
            exact = key_stats[_risk_key(row)]
            parent = parent_stats[_parent_key(row)]
            if exact["n"] >= MIN_KEY_HISTORY:
                severe, bad_pick, mae_abs = _stats_estimate(exact)
                estimate_source = "exact_key_past_embargo"
                training_n = int(exact["n"])
            elif parent["n"] >= MIN_PARENT_HISTORY:
                severe, bad_pick, mae_abs = _stats_estimate(parent)
                estimate_source = "parent_key_past_embargo"
                training_n = int(parent["n"])
            elif global_stats["n"] >= MIN_GLOBAL_HISTORY:
                severe, bad_pick, mae_abs = _stats_estimate(global_stats)
                estimate_source = "global_past_embargo"
                training_n = int(global_stats["n"])
            else:
                severe, bad_pick, mae_abs = 0.20, 0.24, 0.065
                estimate_source = "neutral_prior_no_train_history"
                training_n = 0
            rows.append(
                {
                    "event_ymd": int(row.event_ymd),
                    "code": str(row.code),
                    "past_only_severe_loss_risk_estimate": severe,
                    "past_only_bad_pick_risk_estimate": bad_pick,
                    "past_only_mae_abs_risk_estimate": mae_abs,
                    "risk_estimate_source": estimate_source,
                    "risk_estimate_training_n": training_n,
                }
            )
    risk = pd.DataFrame(rows)
    out = frame.merge(risk, on=["event_ymd", "code"], how="left")
    out = out.drop(columns=["event_dt"])
    return out


def add_risk_control_scores(events: pd.DataFrame) -> pd.DataFrame:
    frame = add_past_only_risk_estimates(events)
    base = pd.to_numeric(frame["score_momentum_continuation_soft_boost_v1"], errors="coerce").fillna(0.0)
    continuation = (
        _state_score(frame["pre_ret20_state"], {"pre20_strong_up": 0.7, "pre20_up": 0.4, "pre20_flat": 0.0, "pre20_down": -0.3})
        + _state_score(frame["pre_ret5_state"], {"pre5_strong_up": 0.6, "pre5_up": 0.35, "pre5_flat": 0.0, "pre5_down": -0.25})
        + _state_score(frame["weekly_prior_state"], {"weekly_prior_strong_up": 0.45, "weekly_prior_uptrend": 0.35, "weekly_prior_mixed": 0.0, "weekly_prior_downtrend": -0.4})
        + _state_score(frame["monthly_prior_state"], {"monthly_prior_uptrend": 0.35, "monthly_prior_strong_up": 0.2, "monthly_prior_mixed": 0.0, "monthly_prior_down_or_drawdown": -0.35})
        + _state_score(frame["pre_volume_state"], {"pre_volume_expansion": 0.35, "pre_volume_normal": 0.0, "pre_volume_dry": -0.2})
        + pd.to_numeric(frame.get("event_strength_score", 0.0), errors="coerce").fillna(0.0) * 0.04
    )
    blowoff = (
        _state_score(frame["pre_wick_warning_state"], {"pre_upper_wick_or_failed_push": 0.8, "pre_wicks_clean": 0.0, "pre_lower_wick_support": -0.1})
        + _state_score(frame["pre_compression_state"], {"pre_range_wide": 0.55, "pre_range_normal": 0.0, "pre_range_compressed": -0.1})
        + _state_score(frame["pre_ma60_context_state"], {"pre_ma60_extended_above": 0.35, "pre_ma60_near_or_above": 0.0, "pre_ma60_below": 0.15})
        + _state_score(frame["monthly_prior_state"], {"monthly_prior_strong_up": 0.25, "monthly_prior_uptrend": 0.0, "monthly_prior_down_or_drawdown": 0.15})
    )
    severe = pd.to_numeric(frame["past_only_severe_loss_risk_estimate"], errors="coerce").fillna(0.20)
    bad_pick = pd.to_numeric(frame["past_only_bad_pick_risk_estimate"], errors="coerce").fillna(0.24)
    mae_abs = pd.to_numeric(frame["past_only_mae_abs_risk_estimate"], errors="coerce").fillna(0.065)
    negative_factor = frame["negative_guard_match"].astype(float) * 0.7 + 0.3
    frame["continuation_signal_score"] = continuation
    frame["blowoff_signal_score"] = blowoff
    frame["score_baseline_momentum_continuation_soft_boost_v1"] = base
    frame["score_severe_loss_soft_penalty_v1"] = base - severe * 3.5 - bad_pick * 1.1 - (mae_abs - 0.055).clip(lower=0.0) * 9.0
    frame["score_extended_continuation_vs_blowoff_risk_v1"] = (
        base
        + continuation * 0.45
        - blowoff * negative_factor * 0.55
        - severe * negative_factor * 2.8
        - bad_pick * negative_factor * 0.8
        - (mae_abs - 0.055).clip(lower=0.0) * negative_factor * 7.0
    )
    return frame


def _select_by_score(events: pd.DataFrame, family_id: str, score_column: str) -> pd.DataFrame:
    selected = events.copy()
    selected["risk_family_id"] = family_id
    selected["research_score"] = pd.to_numeric(selected[score_column], errors="coerce").fillna(-9999.0)
    selected["selection_rank"] = selected.groupby("event_date")["research_score"].rank(method="first", ascending=False)
    selected = selected[selected["selection_rank"] <= TOP_K].copy()
    selected["threshold_mode"] = "always_select_top3"
    return selected


def build_selection_ledger(events: pd.DataFrame) -> pd.DataFrame:
    frames = [
        _select_by_score(events, "baseline_momentum_continuation_soft_boost_v1", "score_baseline_momentum_continuation_soft_boost_v1"),
        _select_by_score(events, "severe_loss_soft_penalty_v1", "score_severe_loss_soft_penalty_v1"),
        _select_by_score(events, "extended_continuation_vs_blowoff_risk_v1", "score_extended_continuation_vs_blowoff_risk_v1"),
    ]
    selected = pd.concat(frames, ignore_index=True)
    columns = [
        "risk_family_id",
        "event_date",
        "code",
        "selection_rank",
        "research_score",
        "threshold_mode",
        "ret20_fwd",
        "mfe20",
        "mae20",
        "win20",
        "severe_loss20",
        "is_future_top10_by_ret20",
        "is_future_top5_by_ret20",
        "is_big_winner_ret20_ge_10pct",
        "is_big_winner_MFE20_ge_15pct",
        "guard_safe_full",
        "negative_guard_match",
        "past_only_severe_loss_risk_estimate",
        "past_only_bad_pick_risk_estimate",
        "past_only_mae_abs_risk_estimate",
        "risk_estimate_source",
        "risk_estimate_training_n",
        "continuation_signal_score",
        "blowoff_signal_score",
        "pre_ret20_state",
        "pre_ret5_state",
        "pre_ma20_path_state",
        "pre_ma60_context_state",
        "pre_wick_warning_state",
        "pre_volume_state",
        "weekly_prior_state",
        "monthly_prior_state",
    ]
    return selected[columns].copy()


def build_date_level_ledger(selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (family_id, event_date), group in selected.groupby(["risk_family_id", "event_date"], sort=True):
        top3 = group[group["selection_rank"].le(3.0)]
        rows.append(
            {
                "risk_family_id": family_id,
                "event_date": event_date,
                "selected_count": int(len(top3)),
                "top3_day_avg_ret20": float(top3["ret20_fwd"].mean()) if len(top3) else 0.0,
                "top3_day_worst_ret20": float(top3["ret20_fwd"].min()) if len(top3) else 0.0,
                "top3_day_worst_MAE20": float(top3["mae20"].min()) if len(top3) else 0.0,
                "top3_day_any_severe_loss": bool(top3["severe_loss20"].any()) if len(top3) else False,
                "top3_day_all_negative": bool((top3["ret20_fwd"] < 0.0).all()) if len(top3) else False,
                "top3_day_all_severe_loss": bool(top3["severe_loss20"].all()) if len(top3) else False,
            }
        )
    return pd.DataFrame(rows)


def _selection_metrics(selected: pd.DataFrame, events: pd.DataFrame, date_ledger: pd.DataFrame, *, family_id: str) -> dict[str, Any]:
    group = selected[selected["risk_family_id"].eq(family_id)]
    dates = date_ledger[date_ledger["risk_family_id"].eq(family_id)]
    top1 = group[group["selection_rank"].eq(1.0)]
    top3 = group[group["selection_rank"].le(3.0)]
    opportunity_dates = set(events.loc[events["opportunity_day_top15"], "event_date"].unique().tolist())
    selected_dates = set(top3["event_date"].unique().tolist())
    winner_available_dates = set(events.loc[events["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    selected_top10_dates = set(top3.loc[top3["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    total_big_ret20 = int(events["is_big_winner_ret20_ge_10pct"].sum())
    total_big_mfe = int(events["is_big_winner_MFE20_ge_15pct"].sum())
    neg_selected = top3[top3["negative_guard_match"]]
    safe_selected = top3[top3["guard_safe_full"]]
    bad_pick = top3["severe_loss20"] | top3["ret20_fwd"].le(-0.05)
    return {
        "family_id": family_id,
        "selected_event_count": int(len(top3)),
        "selected_day_count": int(len(selected_dates)),
        "selected_top1_avg_ret20": float(top1["ret20_fwd"].mean()) if len(top1) else 0.0,
        "selected_top3_avg_ret20": float(top3["ret20_fwd"].mean()) if len(top3) else 0.0,
        "selected_top3_win_rate20": float(top3["win20"].mean()) if len(top3) else 0.0,
        "selected_top3_avg_MFE20": float(top3["mfe20"].mean()) if len(top3) else 0.0,
        "selected_top3_avg_MAE20": float(top3["mae20"].mean()) if len(top3) else 0.0,
        "selected_top3_severe_loss_rate20": float(top3["severe_loss20"].mean()) if len(top3) else 0.0,
        "selected_top3_any_severe_loss_day_rate": float(dates["top3_day_any_severe_loss"].mean()) if len(dates) else 0.0,
        "selected_top3_worst_MAE20_avg_by_day": float(dates["top3_day_worst_MAE20"].mean()) if len(dates) else 0.0,
        "selected_top3_worst_ret20_avg_by_day": float(dates["top3_day_worst_ret20"].mean()) if len(dates) else 0.0,
        "selected_top3_bad_pick_rate20": float(bad_pick.mean()) if len(top3) else 0.0,
        "top3_day_all_negative_rate": float(dates["top3_day_all_negative"].mean()) if len(dates) else 0.0,
        "top3_day_all_severe_loss_rate": float(dates["top3_day_all_severe_loss"].mean()) if len(dates) else 0.0,
        "risk_adjusted_ret20_report_only": (float(top3["ret20_fwd"].mean()) + float(top3["mae20"].mean()) - float(top3["severe_loss20"].mean()) * 0.05) if len(top3) else 0.0,
        "selected_top3_big_winner_ret20_ge_10_capture_rate": _safe_rate(int(top3["is_big_winner_ret20_ge_10pct"].sum()), total_big_ret20),
        "selected_top3_big_winner_MFE20_ge_15_capture_rate": _safe_rate(int(top3["is_big_winner_MFE20_ge_15pct"].sum()), total_big_mfe),
        "selected_nonwinner_when_winner_available_rate": _safe_rate(len(winner_available_dates - selected_top10_dates), len(winner_available_dates)),
        "opportunity_days_total": int(len(opportunity_dates)),
        "selected_on_opportunity_days": int(len(opportunity_dates & selected_dates)),
        "no_trade_on_opportunity_day_rate": _safe_rate(len(opportunity_dates - selected_dates), len(opportunity_dates)),
        "weak_selection_on_opportunity_day_rate": _safe_rate(len(winner_available_dates - selected_top10_dates), len(opportunity_dates)),
        "selected_negative_guard_matched_rate": float(top3["negative_guard_match"].mean()) if len(top3) else 0.0,
        "negative_guard_winner_selected_count": int((neg_selected["is_big_winner_ret20_ge_10pct"]).sum()) if len(neg_selected) else 0,
        "negative_guard_loser_selected_count": int((neg_selected["severe_loss20"]).sum()) if len(neg_selected) else 0,
        "selected_safe_full_tag_rate": float(top3["guard_safe_full"].mean()) if len(top3) else 0.0,
        "safe_full_winner_selected_count": int((safe_selected["is_big_winner_ret20_ge_10pct"]).sum()) if len(safe_selected) else 0,
        "safe_full_loser_selected_count": int((safe_selected["severe_loss20"]).sum()) if len(safe_selected) else 0,
    }


def build_reports(events: pd.DataFrame, selected: pd.DataFrame, date_ledger: pd.DataFrame) -> dict[str, Any]:
    rows = [_selection_metrics(selected, events, date_ledger, family_id=family_id) for family_id in RISK_FAMILIES]
    by_id = {row["family_id"]: row for row in rows}
    base = by_id["baseline_momentum_continuation_soft_boost_v1"]
    oracle_selected = wide_mod._select_by_score(events, "all_strength_oracle_top3", "score_all_strength_oracle_top3")
    oracle_top3_ret = float(oracle_selected["ret20_fwd"].mean()) if len(oracle_selected) else 0.0
    oracle_top3_mfe = float(oracle_selected["mfe20"].mean()) if len(oracle_selected) else 0.0
    for row in rows:
        row["big_winner_capture_delta_vs_previous_best"] = row["selected_top3_big_winner_ret20_ge_10_capture_rate"] - base["selected_top3_big_winner_ret20_ge_10_capture_rate"]
        row["avg_ret20_delta_vs_previous_best"] = row["selected_top3_avg_ret20"] - base["selected_top3_avg_ret20"]
        row["severe_loss_delta_vs_previous_best"] = row["selected_top3_severe_loss_rate20"] - base["selected_top3_severe_loss_rate20"]
        row["MAE_delta_vs_previous_best"] = row["selected_top3_avg_MAE20"] - base["selected_top3_avg_MAE20"]
        row["oracle_top3_gap_ret20"] = row["selected_top3_avg_ret20"] - oracle_top3_ret
        row["oracle_top3_gap_MFE20"] = row["selected_top3_avg_MFE20"] - oracle_top3_mfe
        row["oracle_gap_delta_vs_previous_best"] = row["oracle_top3_gap_ret20"] - (base["selected_top3_avg_ret20"] - oracle_top3_ret)
    leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_risk_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": sorted(rows, key=lambda row: (row["selected_top3_severe_loss_rate20"], -row["selected_top3_avg_ret20"])),
    }
    preservation = {
        "schema_version": f"{SCHEMA_PREFIX}_upside_preservation_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "base_family_id": "baseline_momentum_continuation_soft_boost_v1",
        "rows": [
            {
                "family_id": row["family_id"],
                "big_winner_capture_delta_vs_previous_best": row["big_winner_capture_delta_vs_previous_best"],
                "avg_ret20_delta_vs_previous_best": row["avg_ret20_delta_vs_previous_best"],
                "severe_loss_delta_vs_previous_best": row["severe_loss_delta_vs_previous_best"],
                "MAE_delta_vs_previous_best": row["MAE_delta_vs_previous_best"],
                "oracle_gap_delta_vs_previous_best": row["oracle_gap_delta_vs_previous_best"],
            }
            for row in rows
        ],
    }
    return {
        "rows": rows,
        "by_id": by_id,
        "risk_leaderboard": leaderboard,
        "top3_risk_report": {
            "schema_version": f"{SCHEMA_PREFIX}_top3_risk_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": rows,
        },
        "upside_preservation_report": preservation,
        "oracle_regret_report": {
            "schema_version": f"{SCHEMA_PREFIX}_oracle_regret_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "oracle_top3_avg_ret20": oracle_top3_ret,
            "oracle_top3_avg_MFE20": oracle_top3_mfe,
            "rows": [
                {
                    "family_id": row["family_id"],
                    "oracle_top3_gap_ret20": row["oracle_top3_gap_ret20"],
                    "oracle_top3_gap_MFE20": row["oracle_top3_gap_MFE20"],
                    "oracle_gap_delta_vs_previous_best": row["oracle_gap_delta_vs_previous_best"],
                }
                for row in rows
            ],
        },
    }


def build_negative_guard_decomposition_report(reports: dict[str, Any]) -> dict[str, Any]:
    base = reports["by_id"]["baseline_momentum_continuation_soft_boost_v1"]
    rows = []
    for row in reports["rows"]:
        rows.append(
            {
                "family_id": row["family_id"],
                "selected_negative_guard_matched_rate": row["selected_negative_guard_matched_rate"],
                "negative_guard_winner_selected_count": row["negative_guard_winner_selected_count"],
                "negative_guard_loser_selected_count": row["negative_guard_loser_selected_count"],
                "negative_guard_winner_retained_rate": _safe_rate(row["negative_guard_winner_selected_count"], base["negative_guard_winner_selected_count"]),
                "negative_guard_loser_reduced_rate": 1.0 - _safe_rate(row["negative_guard_loser_selected_count"], base["negative_guard_loser_selected_count"]),
                "extended_continuation_winner_count": row["negative_guard_winner_selected_count"],
                "extended_blowoff_loser_count": row["negative_guard_loser_selected_count"],
                "decomposition_available": True,
                "negative_guard_used_as_hard_veto": False,
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_negative_guard_decomposition_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }


def build_safe_full_soft_tag_report(reports: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_safe_full_soft_tag_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "safe_full_used_as_hard_filter": False,
        "rows": [
            {
                "family_id": row["family_id"],
                "selected_safe_full_tag_rate": row["selected_safe_full_tag_rate"],
                "safe_full_winner_selected_count": row["safe_full_winner_selected_count"],
                "safe_full_loser_selected_count": row["safe_full_loser_selected_count"],
            }
            for row in reports["rows"]
        ],
    }


def build_time_block_stability(selected: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for family_id, group in selected.groupby("risk_family_id", sort=True):
        temp = group.copy()
        temp["year"] = temp["event_date"].astype(str).str.slice(0, 4)
        by_year = temp.groupby("year").agg(avg_ret20=("ret20_fwd", "mean"), severe_loss_rate20=("severe_loss20", "mean"), avg_MAE20=("mae20", "mean")).reset_index()
        rows.append(
            {
                "family_id": family_id,
                "time_block_count": int(len(by_year)),
                "positive_ret_time_block_rate": float((by_year["avg_ret20"] > 0.0).mean()) if len(by_year) else 0.0,
                "severe_loss_under_225_time_block_rate": float((by_year["severe_loss_rate20"] <= 0.225).mean()) if len(by_year) else 0.0,
                "stable_enough": bool(len(by_year) >= 5 and float((by_year["avg_ret20"] > 0.0).mean()) >= 0.55),
                "rows": by_year.to_dict(orient="records"),
            }
        )
    return {"schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows}


def build_source_refs(pattern_dir: Path, guard_dir: Path, upside_dir: Path, wide_dir: Path) -> dict[str, Any]:
    refs = []
    for source_name, root, names in [
        ("pattern", pattern_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "pre_strength_event_ledger.jsonl", "research_decision.json"]),
        ("guard", guard_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "research_decision.json"]),
        ("upside", upside_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "ranking_coverage_audit.json"]),
        ("wide", wide_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "score_leaderboard.json", "top3_selection_report.json", "research_decision.json"]),
    ]:
        for name in names:
            path = root / name
            item = {"source": source_name, "name": name, "path": str(path), "exists": path.exists()}
            if path.exists() and path.suffix == ".json":
                item["content_hash"] = _stable_hash(_load_json(path))
            refs.append(item)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "pattern_artifact_root": str(pattern_dir),
        "guard_artifact_root": str(guard_dir),
        "upside_artifact_root": str(upside_dir),
        "wide_artifact_root": str(wide_dir),
        "refs": refs,
    }


def load_source_artifacts(pattern_dir: Path, guard_dir: Path, wide_dir: Path) -> dict[str, Any]:
    wide_required = ["_ARTIFACT_COMPLETE.json", "research_decision.json", "score_leaderboard.json", "top3_selection_report.json", "ranking_coverage_audit.json"]
    missing = [name for name in wide_required if not (wide_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"wide source missing required artifacts: {missing} at {wide_dir}")
    wide_json = {name: _load_json(wide_dir / name) for name in wide_required}
    if wide_json["_ARTIFACT_COMPLETE.json"].get("complete") is not True:
        raise RuntimeError("wide source artifact is not complete")
    if wide_json["_ARTIFACT_COMPLETE.json"].get("silent_fallback_used") is not False or wide_json["research_decision.json"].get("silent_fallback_used") is not False:
        raise RuntimeError("wide source artifact used silent fallback")
    inputs = wide_mod.upside_mod.load_inputs(pattern_dir, guard_dir)
    events = wide_mod.add_research_scores(inputs["events"])
    events = add_risk_control_scores(events)
    return {"events": events, "wide_json": wide_json}


def build_contracts(
    *,
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    wide_dir: Path,
    events: pd.DataFrame,
    wide_json: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_refs = build_source_refs(pattern_dir, guard_dir, upside_dir, wide_dir)
    risk_control_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_risk_control_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "base_family_id": BASE_FAMILY_ID,
        "risk_control_policy": "wide pool preserved; safe_full and negative_guard are soft tags; risk is a train-past-only soft penalty",
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "risk_control_used_as_soft_penalty": True,
        "embargo_calendar_days": EMBARGO_CALENDAR_DAYS,
        "min_key_history": MIN_KEY_HISTORY,
        "min_parent_history": MIN_PARENT_HISTORY,
    }
    feature_audit = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "score_input_columns": sorted(SCORE_INPUT_COLUMNS),
        "training_label_columns": sorted(TRAINING_LABEL_COLUMNS),
        "future_label_columns": sorted(FUTURE_LABEL_COLUMNS),
        "future_labels_used_for_training_or_evaluation_only": True,
        "future_labels_used_in_score_inputs": bool(SCORE_INPUT_COLUMNS.intersection(FUTURE_LABEL_COLUMNS)),
        "risk_estimate_source_counts": events["risk_estimate_source"].value_counts().to_dict(),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    split_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_split_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_policy": "rolling past-only risk estimates with calendar embargo; fixed predeclared penalty weights",
        "training_used": True,
        "embargo_applied": True,
        "embargo_calendar_days": EMBARGO_CALENDAR_DAYS,
        "weights_learned_from_full_period": False,
        "research_fallback_used": False,
        "keep_candidate_allowed_from_split": True,
    }
    risk_family_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_risk_family_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "families": {
            "baseline_momentum_continuation_soft_boost_v1": "previous wide-pool best score, unchanged",
            "severe_loss_soft_penalty_v1": "base score minus past-only severe/bad-pick/MAE risk estimates",
            "extended_continuation_vs_blowoff_risk_v1": "base score plus current continuation signal minus blowoff/risk soft penalties for negative-guard decomposition",
        },
        "future_labels_used_in_score_inputs": False,
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "selection_risk_control_for_wide_pool",
        "boundary": "TRADEX-only",
        "axis_moved": "selection_risk_control_for_wide_pool",
        "source_wide_pool_decision": wide_json["research_decision.json"].get("authoritative_research_decision"),
        "pattern_artifact_root": str(pattern_dir),
        "guard_artifact_root": str(guard_dir),
        "upside_artifact_root": str(upside_dir),
        "wide_artifact_root": str(wide_dir),
        "period": {"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max()))},
        "event_count": int(len(events)),
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": TOP_K,
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "cost_slippage_evaluated": False,
            "cost_slippage_ignored_by_user_intent": True,
        },
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "risk_control_contract.json": risk_control_contract,
        "feature_availability_audit.json": feature_audit,
        "split_contract.json": split_contract,
        "risk_family_contract.json": risk_family_contract,
    }


def build_threshold_calibration_report() -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_threshold_calibration_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "threshold_family_created": False,
        "threshold_mode": "not_created",
        "reason": "always_select_top3 risk control only; no train-calibrated no-trade threshold in this axis",
        "research_fallback_used": False,
    }


def build_research_decision(
    *,
    reports: dict[str, Any],
    time_stability: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    base = reports["by_id"]["baseline_momentum_continuation_soft_boost_v1"]
    candidates = [reports["by_id"]["severe_loss_soft_penalty_v1"], reports["by_id"]["extended_continuation_vs_blowoff_risk_v1"]]
    best = min(candidates, key=lambda row: (row["selected_top3_severe_loss_rate20"], -row["selected_top3_avg_ret20"]))
    stability_by_id = {row["family_id"]: row for row in time_stability["rows"]}
    stable = bool(stability_by_id.get(best["family_id"], {}).get("stable_enough") is True)
    safe_filter_recreated = best["selected_safe_full_tag_rate"] > 0.50
    negative_veto_recreated = best["selected_negative_guard_matched_rate"] < 0.05
    severe_pass = best["selected_top3_severe_loss_rate20"] <= 0.20
    mae_pass = best["selected_top3_avg_MAE20"] > base["selected_top3_avg_MAE20"] + 0.0025
    ret_pass = best["selected_top3_avg_ret20"] >= 0.010
    capture_pass = best["selected_top3_big_winner_ret20_ge_10_capture_rate"] >= 0.33
    nonwinner_pass = best["selected_nonwinner_when_winner_available_rate"] < base["selected_nonwinner_when_winner_available_rate"]
    oracle_pass = best["oracle_top3_gap_ret20"] > base["oracle_top3_gap_ret20"]
    no_leak = not bool(SCORE_INPUT_COLUMNS.intersection(FUTURE_LABEL_COLUMNS))
    keep_pass = all([artifact_complete, severe_pass, mae_pass, ret_pass, capture_pass, nonwinner_pass, oracle_pass, stable, no_leak, not safe_filter_recreated, not negative_veto_recreated])
    drop_pass = (
        best["selected_top3_severe_loss_rate20"] > 0.225
        or best["selected_top3_avg_MAE20"] <= -0.070
        or best["selected_top3_big_winner_ret20_ge_10_capture_rate"] < 0.30
        or safe_filter_recreated
        or negative_veto_recreated
        or best["oracle_top3_gap_ret20"] < base["oracle_top3_gap_ret20"] - 0.002
        or not no_leak
        or not artifact_complete
    )
    if keep_pass:
        decision = "keep_candidate"
        authoritative = "selection_risk_control_keep_candidate"
    elif drop_pass:
        decision = "drop"
        authoritative = "selection_risk_control_drop"
    else:
        decision = "hold"
        authoritative = "selection_risk_control_hold"
    typed_reasons = [
        "research_only_risk_control_created",
        "soft_penalty_not_hard_filter",
        "severe_loss_improved" if best["selected_top3_severe_loss_rate20"] < base["selected_top3_severe_loss_rate20"] else "severe_loss_not_improved",
        "upside_capture_preserved" if capture_pass else "upside_capture_not_preserved",
        "oracle_gap_improved" if oracle_pass else "oracle_gap_not_improved",
    ]
    if safe_filter_recreated:
        typed_reasons.append("scorer_recreates_safe_full_hard_filter_behavior")
    if negative_veto_recreated:
        typed_reasons.append("scorer_recreates_negative_guard_veto_behavior")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "selection_risk_control_for_wide_pool",
        "boundary": "TRADEX-only",
        "axis_moved": "selection_risk_control_for_wide_pool",
        "source_wide_pool_decision": "wide_strength_pool_upside_rerank_hold",
        "base_family_id": BASE_FAMILY_ID,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "best_risk_family_id": best["family_id"],
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "risk_control_used_as_soft_penalty": True,
        "future_labels_used_for_training_or_evaluation_only": True,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "typed_reasons": typed_reasons,
        "decision_reasons": [
            {"code": "selected_top3_severe_loss_rate20_le_20pct", "status": "pass" if severe_pass else "fail", "value": best["selected_top3_severe_loss_rate20"], "threshold": 0.20},
            {"code": "selected_top3_avg_MAE20_improves", "status": "pass" if mae_pass else "fail", "value": best["selected_top3_avg_MAE20"], "baseline": base["selected_top3_avg_MAE20"]},
            {"code": "selected_top3_avg_ret20_ge_1pct", "status": "pass" if ret_pass else "fail", "value": best["selected_top3_avg_ret20"], "threshold": 0.010},
            {"code": "big_winner_capture_ge_33pct", "status": "pass" if capture_pass else "fail", "value": best["selected_top3_big_winner_ret20_ge_10_capture_rate"], "threshold": 0.33},
            {"code": "selected_nonwinner_when_winner_available_improves", "status": "pass" if nonwinner_pass else "fail", "value": best["selected_nonwinner_when_winner_available_rate"], "baseline": base["selected_nonwinner_when_winner_available_rate"]},
            {"code": "oracle_top3_gap_ret20_improves", "status": "pass" if oracle_pass else "fail", "value": best["oracle_top3_gap_ret20"], "baseline": base["oracle_top3_gap_ret20"]},
            {"code": "time_block_stability", "status": "pass" if stable else "fail", "value": stability_by_id.get(best["family_id"], {})},
            {"code": "safe_full_not_hard_filter", "status": "pass" if not safe_filter_recreated else "fail", "value": best["selected_safe_full_tag_rate"]},
            {"code": "negative_guard_not_hard_veto", "status": "pass" if not negative_veto_recreated else "fail", "value": best["selected_negative_guard_matched_rate"]},
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
        ],
        "best_risk_family_metrics": best,
        "base_family_metrics": base,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required.values()),
        "required_artifacts": required,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_selection_risk_control_for_wide_pool_v1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    guard_dir = _run_dir(guard_root, source_guard_run_id, DEFAULT_GUARD_ROOT)
    upside_dir = _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    loaded = load_source_artifacts(pattern_dir, guard_dir, wide_dir)
    events = loaded["events"]
    selected = build_selection_ledger(events)
    date_ledger = build_date_level_ledger(selected)
    reports = build_reports(events, selected, date_ledger)
    time_stability = build_time_block_stability(selected)
    contracts_payload = build_contracts(pattern_dir=pattern_dir, guard_dir=guard_dir, upside_dir=upside_dir, wide_dir=wide_dir, events=events, wide_json=loaded["wide_json"])
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=[
            {"name": "source_pattern_artifact_root", "path": str(pattern_dir)},
            {"name": "source_guard_artifact_root", "path": str(guard_dir)},
            {"name": "source_upside_artifact_root", "path": str(upside_dir)},
            {"name": "source_wide_artifact_root", "path": str(wide_dir)},
            {"name": "evaluation_contract", "contract_hash": contracts_payload["evaluation_contract.json"]["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={"axis_id": AXIS_ID, "top_k": TOP_K, "candidate_scoring_scope": "research_only", "risk_control_used_as_soft_penalty": True},
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "selection_risk_control_for_wide_pool"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    paths: dict[str, str] = {}
    for name, payload in {
        **contracts_payload,
        "run_manifest.json": run_manifest,
        "risk_leaderboard.json": reports["risk_leaderboard"],
        "top3_risk_report.json": reports["top3_risk_report"],
        "upside_preservation_report.json": reports["upside_preservation_report"],
        "negative_guard_decomposition_report.json": build_negative_guard_decomposition_report(reports),
        "safe_full_soft_tag_report.json": build_safe_full_soft_tag_report(reports),
        "oracle_regret_report.json": reports["oracle_regret_report"],
        "time_block_stability.json": time_stability,
        "threshold_calibration_report.json": build_threshold_calibration_report(),
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["selected_event_risk_ledger.jsonl"] = str(_write_jsonl(output_dir / "selected_event_risk_ledger.jsonl", selected.to_dict(orient="records")))
    paths["date_level_risk_selection_ledger.jsonl"] = str(_write_jsonl(output_dir / "date_level_risk_selection_ledger.jsonl", date_ledger.to_dict(orient="records")))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(reports=reports, time_stability=time_stability, artifact_complete=bool(pre_complete["complete"]))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "best_risk_family_id": decision["best_risk_family_id"],
        "best_risk_family_metrics": decision["best_risk_family_metrics"],
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_selection_risk_control_for_wide_pool_v1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        source_upside_run_id=args.source_upside_run_id,
        source_wide_run_id=args.source_wide_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        upside_root=args.upside_root,
        wide_root=args.wide_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
