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
from scripts import tradex_selection_risk_control_for_wide_pool_v1 as risk_mod


AXIS_ID = "threshold_no_trade_control_for_wide_pool_v1"
SCHEMA_PREFIX = "tradex_threshold_no_trade_control_for_wide_pool_v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_RISK_RUN_ID = "20260513T040000Z-selection-risk-control-for-wide-pool-v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_RISK_ROOT = Path(r"G:\Tradex\selection_risk_control_for_wide_pool_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\threshold_no_trade_control_for_wide_pool_v1")

TOP_K = 3
RANDOM_SEED = 20260513
EMBARGO_CALENDAR_DAYS = risk_mod.EMBARGO_CALENDAR_DAYS
BASE_SCORE_COLUMN = "score_extended_continuation_vs_blowoff_risk_v1"

THRESHOLD_FAMILIES = (
    "baseline_always_select_top3_previous_best",
    "score_quantile_no_trade_v1",
    "risk_quantile_no_trade_v1",
    "hybrid_score_risk_threshold_v1",
    "top1_confident_else_no_trade_v1",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "threshold_policy_contract.json",
    "feature_availability_audit.json",
    "split_contract.json",
    "threshold_family_contract.json",
    "threshold_calibration_ledger.jsonl",
    "selected_event_threshold_ledger.jsonl",
    "date_level_threshold_selection_ledger.jsonl",
    "threshold_leaderboard.json",
    "top1_threshold_report.json",
    "variable_position_threshold_report.json",
    "no_trade_report.json",
    "opportunity_miss_report.json",
    "risk_report.json",
    "upside_preservation_report.json",
    "oracle_regret_report.json",
    "tag_behavior_report.json",
    "time_block_stability.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

THRESHOLD_INPUT_COLUMNS = {
    BASE_SCORE_COLUMN,
    "past_only_severe_loss_risk_estimate",
    "past_only_bad_pick_risk_estimate",
    "past_only_mae_abs_risk_estimate",
    "threshold_risk_value",
    "same_date_score_rank",
    "score_margin_to_next_candidate",
}
FUTURE_LABEL_COLUMNS = risk_mod.FUTURE_LABEL_COLUMNS
TRAINING_LABEL_COLUMNS = risk_mod.TRAINING_LABEL_COLUMNS


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


def add_threshold_inputs(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    for column in ("threshold_risk_value", "same_date_score_rank", "score_margin_to_next_candidate"):
        if column in frame.columns:
            frame = frame.drop(columns=[column])
    frame["threshold_risk_value"] = (
        pd.to_numeric(frame["past_only_severe_loss_risk_estimate"], errors="coerce").fillna(0.20)
        + pd.to_numeric(frame["past_only_bad_pick_risk_estimate"], errors="coerce").fillna(0.24) * 0.45
        + (pd.to_numeric(frame["past_only_mae_abs_risk_estimate"], errors="coerce").fillna(0.065) - 0.055).clip(lower=0.0) * 2.0
    )
    frame["same_date_score_rank"] = frame.groupby("event_date")[BASE_SCORE_COLUMN].rank(method="first", ascending=False)
    sorted_scores = frame.sort_values(["event_date", BASE_SCORE_COLUMN], ascending=[True, False]).copy()
    sorted_scores["next_score"] = sorted_scores.groupby("event_date")[BASE_SCORE_COLUMN].shift(-1)
    sorted_scores["score_margin_to_next_candidate"] = pd.to_numeric(sorted_scores[BASE_SCORE_COLUMN], errors="coerce") - pd.to_numeric(sorted_scores["next_score"], errors="coerce")
    margins = sorted_scores[["event_ymd", "code", "score_margin_to_next_candidate"]]
    frame = frame.merge(margins, on=["event_ymd", "code"], how="left")
    frame["score_margin_to_next_candidate"] = frame["score_margin_to_next_candidate"].fillna(0.0)
    return frame


def _select_top(events: pd.DataFrame, family_id: str, mask: pd.Series, *, top_k: int = TOP_K) -> pd.DataFrame:
    candidate = events[mask].copy()
    if candidate.empty:
        return candidate.assign(threshold_family_id=family_id, selection_rank=pd.Series(dtype=float), threshold_mode=pd.Series(dtype=str))
    candidate["threshold_family_id"] = family_id
    candidate["selection_rank"] = candidate.groupby("event_date")[BASE_SCORE_COLUMN].rank(method="first", ascending=False)
    candidate = candidate[candidate["selection_rank"] <= top_k].copy()
    candidate["threshold_mode"] = "select_0_to_3" if top_k == TOP_K else "top1_confident_else_no_trade"
    return candidate


def _day_eval(events: pd.DataFrame, selected: pd.DataFrame, family_id: str) -> pd.DataFrame:
    rows = []
    selected_by_date = {date: group for date, group in selected.groupby("event_date", sort=False)}
    for event_date, day in events.groupby("event_date", sort=True):
        group = selected_by_date.get(event_date, selected.iloc[0:0])
        day_had_future_winner = bool(day["is_future_top10_by_ret20"].any())
        selected_future_winner = bool(group["is_future_top10_by_ret20"].any()) if len(group) else False
        rows.append(
            {
                "threshold_family_id": family_id,
                "event_date": event_date,
                "day_selected_count": int(len(group)),
                "day_avg_ret20": float(group["ret20_fwd"].mean()) if len(group) else 0.0,
                "day_worst_ret20": float(group["ret20_fwd"].min()) if len(group) else 0.0,
                "day_worst_MAE20": float(group["mae20"].min()) if len(group) else 0.0,
                "day_any_severe_loss": bool(group["severe_loss20"].any()) if len(group) else False,
                "day_all_negative": bool((group["ret20_fwd"] < 0.0).all()) if len(group) else False,
                "day_all_severe_loss": bool(group["severe_loss20"].all()) if len(group) else False,
                "day_had_future_winner": day_had_future_winner,
                "day_selected_future_winner": selected_future_winner,
                "day_no_trade_despite_future_winner": bool(day_had_future_winner and len(group) == 0),
                "opportunity_day_top15": bool(day["opportunity_day_top15"].any()),
                "opportunity_day_big_ret20": bool(day["opportunity_day_big_ret20"].any()),
            }
        )
    return pd.DataFrame(rows)


def _selection_objective(train_selected: pd.DataFrame, train_events: pd.DataFrame, *, no_trade_penalty: float = 0.010) -> float:
    if train_selected.empty:
        return -999.0
    day_eval = _day_eval(train_events, train_selected, "train")
    avg_ret = float(train_selected["ret20_fwd"].mean())
    severe = float(train_selected["severe_loss20"].mean())
    mae = float(train_selected["mae20"].mean())
    winner_dates = set(train_events.loc[train_events["is_future_top10_by_ret20"], "event_date"].unique())
    selected_winner_dates = set(train_selected.loc[train_selected["is_future_top10_by_ret20"], "event_date"].unique())
    nonwinner_rate = _safe_rate(len(winner_dates - selected_winner_dates), len(winner_dates))
    no_trade_opp = float(day_eval.loc[day_eval["opportunity_day_top15"], "day_selected_count"].eq(0).mean()) if day_eval["opportunity_day_top15"].any() else 0.0
    capture = _safe_rate(int(train_selected["is_big_winner_ret20_ge_10pct"].sum()), int(train_events["is_big_winner_ret20_ge_10pct"].sum()))
    return avg_ret - severe * 0.07 + mae * 0.25 + capture * 0.025 - nonwinner_rate * 0.020 - no_trade_opp * no_trade_penalty


def _calibrate_family(train: pd.DataFrame, family_id: str, eval_year: int) -> dict[str, Any]:
    if train.empty:
        return {
            "eval_block": str(eval_year),
            "threshold_family_id": family_id,
            "train_sample_count": 0,
            "threshold_status": "no_prior_train_no_trade",
            "score_quantile": None,
            "risk_quantile": None,
            "margin_quantile": None,
            "score_threshold": None,
            "risk_threshold": None,
            "margin_threshold": None,
            "train_objective": None,
        }
    best: dict[str, Any] | None = None

    def consider(params: dict[str, Any], selected: pd.DataFrame) -> None:
        nonlocal best
        objective = _selection_objective(selected, train)
        row = {
            "eval_block": str(eval_year),
            "threshold_family_id": family_id,
            "train_sample_count": int(len(train)),
            "threshold_status": "train_past_only_calibrated",
            "train_selected_count": int(len(selected)),
            "train_selected_day_count": int(selected["event_date"].nunique()) if len(selected) else 0,
            "train_objective": objective,
            **params,
        }
        if best is None or objective > float(best["train_objective"]):
            best = row

    if family_id == "score_quantile_no_trade_v1":
        for score_q in (0.45, 0.55, 0.65, 0.75, 0.85):
            score_threshold = float(train[BASE_SCORE_COLUMN].quantile(score_q))
            selected = _select_top(train, family_id, train[BASE_SCORE_COLUMN].ge(score_threshold))
            consider({"score_quantile": score_q, "risk_quantile": None, "margin_quantile": None, "score_threshold": score_threshold, "risk_threshold": None, "margin_threshold": None}, selected)
    elif family_id == "risk_quantile_no_trade_v1":
        for risk_q in (0.35, 0.45, 0.55, 0.65, 0.75):
            risk_threshold = float(train["threshold_risk_value"].quantile(risk_q))
            selected = _select_top(train, family_id, train["threshold_risk_value"].le(risk_threshold))
            consider({"score_quantile": None, "risk_quantile": risk_q, "margin_quantile": None, "score_threshold": None, "risk_threshold": risk_threshold, "margin_threshold": None}, selected)
    elif family_id == "hybrid_score_risk_threshold_v1":
        for score_q in (0.45, 0.55, 0.65, 0.75):
            score_threshold = float(train[BASE_SCORE_COLUMN].quantile(score_q))
            for risk_q in (0.45, 0.55, 0.65, 0.75):
                risk_threshold = float(train["threshold_risk_value"].quantile(risk_q))
                mask = train[BASE_SCORE_COLUMN].ge(score_threshold) & train["threshold_risk_value"].le(risk_threshold)
                selected = _select_top(train, family_id, mask)
                consider({"score_quantile": score_q, "risk_quantile": risk_q, "margin_quantile": None, "score_threshold": score_threshold, "risk_threshold": risk_threshold, "margin_threshold": None}, selected)
    elif family_id == "top1_confident_else_no_trade_v1":
        top_train = train[train["same_date_score_rank"].eq(1.0)].copy()
        for score_q in (0.55, 0.65, 0.75, 0.85):
            score_threshold = float(train[BASE_SCORE_COLUMN].quantile(score_q))
            for risk_q in (0.45, 0.55, 0.65, 0.75):
                risk_threshold = float(train["threshold_risk_value"].quantile(risk_q))
                for margin_q in (0.35, 0.50, 0.65):
                    margin_threshold = float(top_train["score_margin_to_next_candidate"].quantile(margin_q)) if len(top_train) else 0.0
                    mask = (
                        train["same_date_score_rank"].eq(1.0)
                        & train[BASE_SCORE_COLUMN].ge(score_threshold)
                        & train["threshold_risk_value"].le(risk_threshold)
                        & train["score_margin_to_next_candidate"].ge(margin_threshold)
                    )
                    selected = _select_top(train, family_id, mask, top_k=1)
                    consider({"score_quantile": score_q, "risk_quantile": risk_q, "margin_quantile": margin_q, "score_threshold": score_threshold, "risk_threshold": risk_threshold, "margin_threshold": margin_threshold}, selected)
    else:
        return {
            "eval_block": str(eval_year),
            "threshold_family_id": family_id,
            "train_sample_count": int(len(train)),
            "threshold_status": "baseline_no_threshold",
            "score_quantile": None,
            "risk_quantile": None,
            "margin_quantile": None,
            "score_threshold": None,
            "risk_threshold": None,
            "margin_threshold": None,
            "train_objective": None,
        }
    assert best is not None
    return best


def _apply_threshold(events: pd.DataFrame, family_id: str, calibration: dict[str, Any]) -> pd.DataFrame:
    if family_id == "baseline_always_select_top3_previous_best":
        return _select_top(events, family_id, pd.Series(True, index=events.index))
    if calibration.get("threshold_status") == "no_prior_train_no_trade":
        return events.iloc[0:0].copy().assign(threshold_family_id=family_id, selection_rank=pd.Series(dtype=float), threshold_mode=pd.Series(dtype=str))
    mask = pd.Series(True, index=events.index)
    if calibration.get("score_threshold") is not None:
        mask &= events[BASE_SCORE_COLUMN].ge(float(calibration["score_threshold"]))
    if calibration.get("risk_threshold") is not None:
        mask &= events["threshold_risk_value"].le(float(calibration["risk_threshold"]))
    top_k = TOP_K
    if family_id == "top1_confident_else_no_trade_v1":
        top_k = 1
        mask &= events["same_date_score_rank"].eq(1.0)
        if calibration.get("margin_threshold") is not None:
            mask &= events["score_margin_to_next_candidate"].ge(float(calibration["margin_threshold"]))
    return _select_top(events, family_id, mask, top_k=top_k)


def build_threshold_selection(events: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    frame = add_threshold_inputs(events)
    frame["event_year"] = frame["event_date"].astype(str).str.slice(0, 4).astype(int)
    selected_frames = []
    calibration_rows = []
    for year, eval_events in frame.groupby("event_year", sort=True):
        cutoff = pd.Timestamp(f"{year}-01-01") - pd.Timedelta(days=EMBARGO_CALENDAR_DAYS)
        train = frame[pd.to_datetime(frame["event_date"]).le(cutoff)].copy()
        for family_id in THRESHOLD_FAMILIES:
            calibration = _calibrate_family(train, family_id, int(year))
            calibration["eval_sample_count"] = int(len(eval_events))
            calibration_rows.append(calibration)
            selected_frames.append(_apply_threshold(eval_events, family_id, calibration))
    selected = pd.concat(selected_frames, ignore_index=True) if selected_frames else frame.iloc[0:0].copy()
    selected_columns = [
        "threshold_family_id",
        "event_date",
        "code",
        "selection_rank",
        "threshold_mode",
        BASE_SCORE_COLUMN,
        "threshold_risk_value",
        "same_date_score_rank",
        "score_margin_to_next_candidate",
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
    ]
    return selected[selected_columns].copy(), frame, calibration_rows


def build_date_level_ledger(events: pd.DataFrame, selected: pd.DataFrame) -> pd.DataFrame:
    frames = [_day_eval(events, selected[selected["threshold_family_id"].eq(family_id)], family_id) for family_id in THRESHOLD_FAMILIES]
    return pd.concat(frames, ignore_index=True)


def _family_metrics(selected: pd.DataFrame, date_ledger: pd.DataFrame, events: pd.DataFrame, *, family_id: str, base: dict[str, Any] | None = None) -> dict[str, Any]:
    group = selected[selected["threshold_family_id"].eq(family_id)]
    dates = date_ledger[date_ledger["threshold_family_id"].eq(family_id)]
    top1 = group[group["selection_rank"].eq(1.0)]
    all_dates = set(events["event_date"].unique().tolist())
    selected_dates = set(group["event_date"].unique().tolist())
    opportunity_dates = set(events.loc[events["opportunity_day_top15"], "event_date"].unique().tolist())
    winner_dates = set(events.loc[events["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    selected_winner_dates = set(group.loc[group["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    total_big_ret20 = int(events["is_big_winner_ret20_ge_10pct"].sum())
    total_big_mfe = int(events["is_big_winner_MFE20_ge_15pct"].sum())
    bad_pick = group["severe_loss20"] | group["ret20_fwd"].le(-0.05) if len(group) else pd.Series(dtype=bool)
    selected_avg = float(group["ret20_fwd"].mean()) if len(group) else 0.0
    row = {
        "family_id": family_id,
        "selected_days_count": int(len(selected_dates)),
        "no_trade_days_count": int(len(all_dates - selected_dates)),
        "no_trade_days_rate": _safe_rate(len(all_dates - selected_dates), len(all_dates)),
        "selected_events_count": int(len(group)),
        "avg_positions_per_selected_day": _safe_rate(len(group), len(selected_dates)),
        "avg_positions_per_all_days": _safe_rate(len(group), len(all_dates)),
        "capital_utilization_proxy": _safe_rate(len(group), len(all_dates) * TOP_K),
        "fewer_than_3_day_rate": float(dates["day_selected_count"].lt(TOP_K).mean()) if len(dates) else 0.0,
        "selected_avg_ret20": selected_avg,
        "selected_top1_avg_ret20": float(top1["ret20_fwd"].mean()) if len(top1) else 0.0,
        "selected_top3_or_variable_avg_ret20": selected_avg,
        "selected_win_rate20": float(group["win20"].mean()) if len(group) else 0.0,
        "selected_avg_MFE20": float(group["mfe20"].mean()) if len(group) else 0.0,
        "selected_big_winner_ret20_ge_10_capture_rate": _safe_rate(int(group["is_big_winner_ret20_ge_10pct"].sum()), total_big_ret20),
        "selected_big_winner_MFE20_ge_15_capture_rate": _safe_rate(int(group["is_big_winner_MFE20_ge_15pct"].sum()), total_big_mfe),
        "selected_avg_MAE20": float(group["mae20"].mean()) if len(group) else 0.0,
        "selected_severe_loss_rate20": float(group["severe_loss20"].mean()) if len(group) else 0.0,
        "selected_any_severe_loss_day_rate": float(dates["day_any_severe_loss"].mean()) if len(dates) else 0.0,
        "selected_worst_MAE20_avg_by_day": float(dates["day_worst_MAE20"].mean()) if len(dates) else 0.0,
        "selected_worst_ret20_avg_by_day": float(dates["day_worst_ret20"].mean()) if len(dates) else 0.0,
        "selected_bad_pick_rate20": float(bad_pick.mean()) if len(group) else 0.0,
        "opportunity_days_total": int(len(opportunity_dates)),
        "no_trade_on_opportunity_day_count": int(len(opportunity_dates - selected_dates)),
        "no_trade_on_opportunity_day_rate": _safe_rate(len(opportunity_dates - selected_dates), len(opportunity_dates)),
        "selected_on_opportunity_days": int(len(opportunity_dates & selected_dates)),
        "weak_selection_on_opportunity_day_rate": _safe_rate(len(winner_dates - selected_winner_dates), len(opportunity_dates)),
        "selected_nonwinner_when_winner_available_rate": _safe_rate(len(winner_dates - selected_winner_dates), len(winner_dates)),
        "missed_big_winner_due_to_no_trade_count": int(dates["day_no_trade_despite_future_winner"].sum()) if len(dates) else 0,
        "missed_big_winner_due_to_threshold_count": int(len(winner_dates - selected_winner_dates)),
        "selected_negative_guard_matched_rate": float(group["negative_guard_match"].mean()) if len(group) else 0.0,
        "negative_guard_winner_selected_count": int((group["negative_guard_match"] & group["is_big_winner_ret20_ge_10pct"]).sum()) if len(group) else 0,
        "negative_guard_severe_loser_selected_count": int((group["negative_guard_match"] & group["severe_loss20"]).sum()) if len(group) else 0,
        "selected_safe_full_tag_rate": float(group["guard_safe_full"].mean()) if len(group) else 0.0,
        "safe_full_winner_selected_count": int((group["guard_safe_full"] & group["is_big_winner_ret20_ge_10pct"]).sum()) if len(group) else 0,
    }
    if base:
        row["big_winner_capture_delta_vs_previous_best"] = row["selected_big_winner_ret20_ge_10_capture_rate"] - base["selected_big_winner_ret20_ge_10_capture_rate"]
        row["severe_loss_delta_vs_previous_best"] = row["selected_severe_loss_rate20"] - base["selected_severe_loss_rate20"]
        row["any_severe_loss_day_delta_vs_previous_best"] = row["selected_any_severe_loss_day_rate"] - base["selected_any_severe_loss_day_rate"]
        row["avg_ret20_delta_vs_previous_best"] = row["selected_avg_ret20"] - base["selected_avg_ret20"]
    else:
        row["big_winner_capture_delta_vs_previous_best"] = 0.0
        row["severe_loss_delta_vs_previous_best"] = 0.0
        row["any_severe_loss_day_delta_vs_previous_best"] = 0.0
        row["avg_ret20_delta_vs_previous_best"] = 0.0
    return row


def build_reports(events: pd.DataFrame, selected: pd.DataFrame, date_ledger: pd.DataFrame) -> dict[str, Any]:
    base = _family_metrics(selected, date_ledger, events, family_id="baseline_always_select_top3_previous_best")
    rows = [base]
    for family_id in THRESHOLD_FAMILIES[1:]:
        rows.append(_family_metrics(selected, date_ledger, events, family_id=family_id, base=base))
    by_id = {row["family_id"]: row for row in rows}
    oracle_rows = []
    for family_id, dates in date_ledger.groupby("threshold_family_id", sort=True):
        selected_days = set(dates.loc[dates["day_selected_count"].gt(0), "event_date"])
        gaps_selected = []
        gaps_all = []
        for event_date, day in events.groupby("event_date", sort=True):
            oracle_top = float(day["ret20_fwd"].sort_values(ascending=False).head(TOP_K).mean())
            family_day = dates[dates["event_date"].eq(event_date)]
            actual = float(family_day["day_avg_ret20"].iloc[0]) if len(family_day) else 0.0
            gaps_all.append(actual - oracle_top)
            if event_date in selected_days:
                gaps_selected.append(actual - oracle_top)
        by_id[family_id]["oracle_top3_gap_ret20_on_selected_days"] = float(pd.Series(gaps_selected).mean()) if gaps_selected else 0.0
        by_id[family_id]["oracle_gap_all_days_with_no_trade_penalty"] = float(pd.Series(gaps_all).mean()) if gaps_all else 0.0
        by_id[family_id]["oracle_gap_delta_vs_previous_best"] = by_id[family_id]["oracle_gap_all_days_with_no_trade_penalty"] - by_id["baseline_always_select_top3_previous_best"]["oracle_gap_all_days_with_no_trade_penalty"]
        oracle_rows.append(
            {
                "family_id": family_id,
                "oracle_top1_gap_ret20_on_selected_days": by_id[family_id]["oracle_top3_gap_ret20_on_selected_days"],
                "oracle_top3_gap_ret20_on_selected_days": by_id[family_id]["oracle_top3_gap_ret20_on_selected_days"],
                "oracle_gap_all_days_with_no_trade_penalty": by_id[family_id]["oracle_gap_all_days_with_no_trade_penalty"],
                "oracle_gap_delta_vs_previous_best": by_id[family_id]["oracle_gap_delta_vs_previous_best"],
            }
        )
    rows = [by_id[family_id] for family_id in THRESHOLD_FAMILIES]
    leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_threshold_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": sorted(rows, key=lambda row: (row["selected_severe_loss_rate20"], -row["selected_avg_ret20"])),
    }
    return {
        "rows": rows,
        "by_id": by_id,
        "threshold_leaderboard": leaderboard,
        "top1_threshold_report": {"schema_version": f"{SCHEMA_PREFIX}_top1_threshold_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("family_id", "selected_top1_avg_ret20", "selected_win_rate20", "selected_severe_loss_rate20")} for row in rows]},
        "variable_position_threshold_report": {"schema_version": f"{SCHEMA_PREFIX}_variable_position_threshold_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows},
        "no_trade_report": {"schema_version": f"{SCHEMA_PREFIX}_no_trade_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("family_id", "selected_days_count", "no_trade_days_count", "no_trade_days_rate", "avg_positions_per_selected_day", "avg_positions_per_all_days", "capital_utilization_proxy", "fewer_than_3_day_rate", "no_trade_on_opportunity_day_rate")} for row in rows]},
        "opportunity_miss_report": {"schema_version": f"{SCHEMA_PREFIX}_opportunity_miss_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("family_id", "opportunity_days_total", "no_trade_on_opportunity_day_count", "no_trade_on_opportunity_day_rate", "selected_nonwinner_when_winner_available_rate", "missed_big_winner_due_to_no_trade_count", "missed_big_winner_due_to_threshold_count")} for row in rows]},
        "risk_report": {"schema_version": f"{SCHEMA_PREFIX}_risk_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("family_id", "selected_avg_MAE20", "selected_severe_loss_rate20", "selected_any_severe_loss_day_rate", "selected_worst_MAE20_avg_by_day", "selected_worst_ret20_avg_by_day", "selected_bad_pick_rate20", "severe_loss_delta_vs_previous_best", "any_severe_loss_day_delta_vs_previous_best")} for row in rows]},
        "upside_preservation_report": {"schema_version": f"{SCHEMA_PREFIX}_upside_preservation_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("family_id", "selected_avg_ret20", "selected_big_winner_ret20_ge_10_capture_rate", "selected_big_winner_MFE20_ge_15_capture_rate", "big_winner_capture_delta_vs_previous_best", "avg_ret20_delta_vs_previous_best")} for row in rows]},
        "oracle_regret_report": {"schema_version": f"{SCHEMA_PREFIX}_oracle_regret_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": oracle_rows},
        "tag_behavior_report": {"schema_version": f"{SCHEMA_PREFIX}_tag_behavior_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": [{k: row[k] for k in ("family_id", "selected_negative_guard_matched_rate", "negative_guard_winner_selected_count", "negative_guard_severe_loser_selected_count", "selected_safe_full_tag_rate", "safe_full_winner_selected_count")} for row in rows]},
    }


def build_time_block_stability(date_ledger: pd.DataFrame, selected: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for family_id, dates in date_ledger.groupby("threshold_family_id", sort=True):
        temp = dates.copy()
        temp["year"] = temp["event_date"].astype(str).str.slice(0, 4)
        by_year = temp.groupby("year").agg(
            selected_days=("day_selected_count", lambda s: int((s > 0).sum())),
            no_trade_days=("day_selected_count", lambda s: int((s == 0).sum())),
            avg_day_ret20=("day_avg_ret20", "mean"),
            any_severe_loss_day_rate=("day_any_severe_loss", "mean"),
        ).reset_index()
        rows.append(
            {
                "family_id": family_id,
                "time_block_count": int(len(by_year)),
                "positive_time_block_rate": float((by_year["avg_day_ret20"] > 0.0).mean()) if len(by_year) else 0.0,
                "threshold_selection_rate_by_time_block": by_year.to_dict(orient="records"),
                "threshold_family_stability": bool(len(by_year) >= 5 and float((by_year["avg_day_ret20"] > 0.0).mean()) >= 0.55),
            }
        )
    return {"schema_version": f"{SCHEMA_PREFIX}_time_block_stability_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows}


def build_source_refs(pattern_dir: Path, guard_dir: Path, upside_dir: Path, wide_dir: Path, risk_dir: Path) -> dict[str, Any]:
    refs = []
    for source, root, names in [
        ("pattern", pattern_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "pre_strength_event_ledger.jsonl", "research_decision.json"]),
        ("guard", guard_dir, ["_ARTIFACT_COMPLETE.json", "evaluation_contract.json", "research_decision.json"]),
        ("upside", upside_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "ranking_coverage_audit.json"]),
        ("wide", wide_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "score_leaderboard.json"]),
        ("risk", risk_dir, ["_ARTIFACT_COMPLETE.json", "research_decision.json", "risk_leaderboard.json"]),
    ]:
        for name in names:
            path = root / name
            item = {"source": source, "name": name, "path": str(path), "exists": path.exists()}
            if path.exists() and path.suffix == ".json":
                item["content_hash"] = _stable_hash(_load_json(path))
            refs.append(item)
    return {"schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "refs": refs}


def load_source_artifacts(pattern_dir: Path, guard_dir: Path, wide_dir: Path, risk_dir: Path) -> dict[str, Any]:
    required = ["_ARTIFACT_COMPLETE.json", "research_decision.json", "risk_leaderboard.json"]
    missing = [name for name in required if not (risk_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"risk source missing required artifacts: {missing} at {risk_dir}")
    risk_json = {name: _load_json(risk_dir / name) for name in required}
    if risk_json["_ARTIFACT_COMPLETE.json"].get("complete") is not True:
        raise RuntimeError("risk source artifact is not complete")
    if risk_json["_ARTIFACT_COMPLETE.json"].get("silent_fallback_used") is not False or risk_json["research_decision.json"].get("silent_fallback_used") is not False:
        raise RuntimeError("risk source artifact used silent fallback")
    if risk_json["_ARTIFACT_COMPLETE.json"].get("research_fallback_used") is not False or risk_json["research_decision.json"].get("research_fallback_used") is not False:
        raise RuntimeError("risk source artifact used research fallback")
    loaded = risk_mod.load_source_artifacts(pattern_dir, guard_dir, wide_dir)
    return {"events": loaded["events"], "risk_json": risk_json}


def build_contracts(
    *,
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    wide_dir: Path,
    risk_dir: Path,
    events: pd.DataFrame,
    risk_json: dict[str, Any],
    calibration_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    source_refs = build_source_refs(pattern_dir, guard_dir, upside_dir, wide_dir, risk_dir)
    threshold_policy_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_threshold_policy_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "selection_policy": "existing research score plus train-past-only thresholds; select 0 to 3 per day",
        "uses_existing_research_score": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": True,
        "threshold_policy_scope": "research_only",
        "no_trade_allowed": True,
        "variable_position_count_allowed": True,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
    }
    feature_audit = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "threshold_input_columns": sorted(THRESHOLD_INPUT_COLUMNS),
        "training_label_columns": sorted(TRAINING_LABEL_COLUMNS),
        "future_label_columns": sorted(FUTURE_LABEL_COLUMNS),
        "future_labels_used_for_training_or_evaluation_only": True,
        "future_labels_used_in_threshold_inputs": bool(THRESHOLD_INPUT_COLUMNS.intersection(FUTURE_LABEL_COLUMNS)),
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    split_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_split_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_policy": "calendar-year expanding train-past-only threshold calibration",
        "training_used": True,
        "thresholds_calibrated_train_past_only": True,
        "embargo_applied": True,
        "embargo_calendar_days": EMBARGO_CALENDAR_DAYS,
        "research_fallback_used": False,
        "first_block_without_prior_history_uses_no_trade_for_threshold_families": True,
    }
    threshold_family_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_threshold_family_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "families": {
            "baseline_always_select_top3_previous_best": "previous risk-family score, no threshold, always top3",
            "score_quantile_no_trade_v1": "train-past-only score quantile threshold, 0 to 3 selections",
            "risk_quantile_no_trade_v1": "train-past-only risk quantile threshold, 0 to 3 selections",
            "hybrid_score_risk_threshold_v1": "train-past-only score and risk thresholds, 0 to 3 selections",
            "top1_confident_else_no_trade_v1": "train-past-only score/risk/margin threshold, top1 or no_trade",
        },
        "candidate_scoring_created": False,
        "uses_existing_research_score": True,
    }
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "threshold_no_trade_control_for_wide_pool",
        "boundary": "TRADEX-only",
        "axis_moved": "threshold_no_trade_control_for_wide_pool",
        "source_selection_risk_decision": risk_json["research_decision.json"].get("authoritative_research_decision"),
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
        "uses_existing_research_score": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": True,
        "threshold_policy_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    threshold_policy_contract["threshold_calibration_blocks"] = sorted({row["eval_block"] for row in calibration_rows})
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "threshold_policy_contract.json": threshold_policy_contract,
        "feature_availability_audit.json": feature_audit,
        "split_contract.json": split_contract,
        "threshold_family_contract.json": threshold_family_contract,
    }


def build_research_decision(*, reports: dict[str, Any], time_stability: dict[str, Any], artifact_complete: bool) -> dict[str, Any]:
    base = reports["by_id"]["baseline_always_select_top3_previous_best"]
    candidates = [reports["by_id"][family_id] for family_id in THRESHOLD_FAMILIES[1:]]
    best = max(
        candidates,
        key=lambda row: (
            row["selected_avg_ret20"] >= base["selected_avg_ret20"],
            row["selected_severe_loss_rate20"] <= 0.225,
            row["selected_big_winner_ret20_ge_10_capture_rate"],
            -row["selected_nonwinner_when_winner_available_rate"],
            -row["selected_severe_loss_rate20"],
        ),
    )
    stable_map = {row["family_id"]: row for row in time_stability["rows"]}
    stable = bool(stable_map.get(best["family_id"], {}).get("threshold_family_stability") is True)
    safe_filter_like = best["selected_safe_full_tag_rate"] > 0.50 or best["no_trade_on_opportunity_day_rate"] > 0.75
    negative_veto_like = best["selected_negative_guard_matched_rate"] < 0.05
    severe_keep = best["selected_severe_loss_rate20"] <= 0.20
    any_day_keep = best["selected_any_severe_loss_day_rate"] < base["selected_any_severe_loss_day_rate"] - 0.05
    mae_keep = best["selected_avg_MAE20"] > base["selected_avg_MAE20"] + 0.0025
    ret_keep = best["selected_avg_ret20"] >= base["selected_avg_ret20"]
    capture_keep = best["selected_big_winner_ret20_ge_10_capture_rate"] >= 0.32
    nonwinner_keep = best["selected_nonwinner_when_winner_available_rate"] < base["selected_nonwinner_when_winner_available_rate"]
    opp_no_trade_ok = best["no_trade_on_opportunity_day_rate"] <= 0.40
    oracle_keep = best["oracle_gap_all_days_with_no_trade_penalty"] > base["oracle_gap_all_days_with_no_trade_penalty"]
    no_leak = not bool(THRESHOLD_INPUT_COLUMNS.intersection(FUTURE_LABEL_COLUMNS))
    keep_pass = all([artifact_complete, severe_keep, any_day_keep, mae_keep, ret_keep, capture_keep, nonwinner_keep, opp_no_trade_ok, oracle_keep, stable, no_leak, not safe_filter_like, not negative_veto_like])
    drop_pass = (
        best["selected_severe_loss_rate20"] > 0.225
        or best["selected_any_severe_loss_day_rate"] > base["selected_any_severe_loss_day_rate"] - 0.01
        or best["selected_nonwinner_when_winner_available_rate"] > base["selected_nonwinner_when_winner_available_rate"]
        or best["oracle_gap_all_days_with_no_trade_penalty"] < base["oracle_gap_all_days_with_no_trade_penalty"]
        or best["no_trade_on_opportunity_day_rate"] > 0.75
        or safe_filter_like
        or negative_veto_like
        or not no_leak
        or not artifact_complete
    )
    if keep_pass:
        decision = "keep_candidate"
        authoritative = "threshold_no_trade_control_keep_candidate"
    elif drop_pass:
        decision = "drop"
        authoritative = "threshold_no_trade_control_drop"
    else:
        decision = "hold"
        authoritative = "threshold_no_trade_control_hold"
    typed_reasons = [
        "threshold_policy_created",
        "uses_existing_research_score",
        "thresholds_calibrated_train_past_only",
        "no_trade_allowed",
        "severe_loss_improved" if best["selected_severe_loss_rate20"] < base["selected_severe_loss_rate20"] else "severe_loss_not_improved",
        "nonwinner_selection_improved" if nonwinner_keep else "nonwinner_selection_not_improved",
        "oracle_gap_improved" if oracle_keep else "oracle_gap_not_improved",
    ]
    if safe_filter_like:
        typed_reasons.append("threshold_policy_too_opportunity_sparse_or_safe_filter_like")
    if negative_veto_like:
        typed_reasons.append("threshold_policy_recreates_negative_guard_veto_behavior")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "threshold_no_trade_control_for_wide_pool",
        "boundary": "TRADEX-only",
        "axis_moved": "threshold_no_trade_control_for_wide_pool",
        "source_selection_risk_decision": "selection_risk_control_drop",
        "uses_existing_research_score": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": True,
        "threshold_policy_scope": "research_only",
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "best_threshold_family_id": best["family_id"],
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "no_trade_allowed": True,
        "variable_position_count_allowed": True,
        "future_labels_used_for_training_or_evaluation_only": True,
        "future_labels_used_in_threshold_inputs": False,
        "thresholds_calibrated_train_past_only": True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "typed_reasons": typed_reasons,
        "decision_reasons": [
            {"code": "selected_severe_loss_rate20_le_20pct", "status": "pass" if severe_keep else "fail", "value": best["selected_severe_loss_rate20"], "threshold": 0.20},
            {"code": "selected_any_severe_loss_day_rate_improves", "status": "pass" if any_day_keep else "fail", "value": best["selected_any_severe_loss_day_rate"], "baseline": base["selected_any_severe_loss_day_rate"]},
            {"code": "selected_avg_MAE20_improves", "status": "pass" if mae_keep else "fail", "value": best["selected_avg_MAE20"], "baseline": base["selected_avg_MAE20"]},
            {"code": "selected_avg_ret20_ge_previous_best", "status": "pass" if ret_keep else "fail", "value": best["selected_avg_ret20"], "baseline": base["selected_avg_ret20"]},
            {"code": "big_winner_capture_ge_32pct", "status": "pass" if capture_keep else "fail", "value": best["selected_big_winner_ret20_ge_10_capture_rate"], "threshold": 0.32},
            {"code": "selected_nonwinner_when_winner_available_improves", "status": "pass" if nonwinner_keep else "fail", "value": best["selected_nonwinner_when_winner_available_rate"], "baseline": base["selected_nonwinner_when_winner_available_rate"]},
            {"code": "no_trade_on_opportunity_day_rate_not_excessive", "status": "pass" if opp_no_trade_ok else "fail", "value": best["no_trade_on_opportunity_day_rate"], "threshold": 0.40},
            {"code": "oracle_gap_all_days_with_no_trade_penalty_improves", "status": "pass" if oracle_keep else "fail", "value": best["oracle_gap_all_days_with_no_trade_penalty"], "baseline": base["oracle_gap_all_days_with_no_trade_penalty"]},
            {"code": "threshold_family_stability", "status": "pass" if stable else "fail", "value": stable_map.get(best["family_id"], {})},
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
        ],
        "best_threshold_family_metrics": best,
        "baseline_metrics": base,
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
        "candidate_scoring_created": False,
        "uses_existing_research_score": True,
        "threshold_policy_created": True,
        "threshold_policy_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_threshold_no_trade_control_for_wide_pool_v1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_risk_run_id: str = DEFAULT_RISK_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    risk_root: str | Path = DEFAULT_RISK_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    guard_dir = _run_dir(guard_root, source_guard_run_id, DEFAULT_GUARD_ROOT)
    upside_dir = _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    risk_dir = _run_dir(risk_root, source_risk_run_id, DEFAULT_RISK_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    loaded = load_source_artifacts(pattern_dir, guard_dir, wide_dir, risk_dir)
    selected, events, calibration_rows = build_threshold_selection(loaded["events"])
    date_ledger = build_date_level_ledger(events, selected)
    reports = build_reports(events, selected, date_ledger)
    time_stability = build_time_block_stability(date_ledger, selected)
    contracts_payload = build_contracts(
        pattern_dir=pattern_dir,
        guard_dir=guard_dir,
        upside_dir=upside_dir,
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        events=events,
        risk_json=loaded["risk_json"],
        calibration_rows=calibration_rows,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=[
            {"name": "source_pattern_artifact_root", "path": str(pattern_dir)},
            {"name": "source_guard_artifact_root", "path": str(guard_dir)},
            {"name": "source_upside_artifact_root", "path": str(upside_dir)},
            {"name": "source_wide_artifact_root", "path": str(wide_dir)},
            {"name": "source_risk_artifact_root", "path": str(risk_dir)},
            {"name": "evaluation_contract", "contract_hash": contracts_payload["evaluation_contract.json"]["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={"axis_id": AXIS_ID, "top_k": TOP_K, "candidate_scoring_created": False, "threshold_policy_created": True},
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "threshold_no_trade_control_for_wide_pool"},
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
        "threshold_leaderboard.json": reports["threshold_leaderboard"],
        "top1_threshold_report.json": reports["top1_threshold_report"],
        "variable_position_threshold_report.json": reports["variable_position_threshold_report"],
        "no_trade_report.json": reports["no_trade_report"],
        "opportunity_miss_report.json": reports["opportunity_miss_report"],
        "risk_report.json": reports["risk_report"],
        "upside_preservation_report.json": reports["upside_preservation_report"],
        "oracle_regret_report.json": reports["oracle_regret_report"],
        "tag_behavior_report.json": reports["tag_behavior_report"],
        "time_block_stability.json": time_stability,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["threshold_calibration_ledger.jsonl"] = str(_write_jsonl(output_dir / "threshold_calibration_ledger.jsonl", calibration_rows))
    paths["selected_event_threshold_ledger.jsonl"] = str(_write_jsonl(output_dir / "selected_event_threshold_ledger.jsonl", selected.to_dict(orient="records")))
    paths["date_level_threshold_selection_ledger.jsonl"] = str(_write_jsonl(output_dir / "date_level_threshold_selection_ledger.jsonl", date_ledger.to_dict(orient="records")))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(reports=reports, time_stability=time_stability, artifact_complete=bool(pre_complete["complete"]))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "best_threshold_family_id": decision["best_threshold_family_id"],
        "best_threshold_family_metrics": decision["best_threshold_family_metrics"],
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "candidate_scoring_created": False,
        "uses_existing_research_score": True,
        "threshold_policy_created": True,
        "threshold_policy_scope": "research_only",
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-guard-run-id", default=DEFAULT_GUARD_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-risk-run-id", default=DEFAULT_RISK_RUN_ID)
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--risk-root", default=str(DEFAULT_RISK_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_threshold_no_trade_control_for_wide_pool_v1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        source_upside_run_id=args.source_upside_run_id,
        source_wide_run_id=args.source_wide_run_id,
        source_risk_run_id=args.source_risk_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        upside_root=args.upside_root,
        wide_root=args.wide_root,
        risk_root=args.risk_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
