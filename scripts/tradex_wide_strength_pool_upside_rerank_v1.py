from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_upside_capture_missed_winner_diagnosis_v1 as upside_mod


AXIS_ID = "wide_strength_pool_upside_rerank_v1"
SCHEMA_PREFIX = "tradex_wide_strength_pool_upside_rerank_v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_GUARD_RUN_ID = "20260513T010000Z-pre-strength-guard-validation-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\pre_strength_guard_validation_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")

RANDOM_REPEATS = 32
RANDOM_SEED = 20260513
TOP_K = 3

SCORING_FAMILIES = (
    "all_strength_scoreless_random_top3",
    "all_strength_oracle_top3",
    "momentum_continuation_soft_boost_v1",
    "hybrid_reclaim_momentum_soft_risk_v1",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "candidate_pool_contract.json",
    "feature_availability_audit.json",
    "scoring_family_contract.json",
    "split_contract.json",
    "date_level_selection_ledger.jsonl",
    "score_leaderboard.json",
    "top1_selection_report.json",
    "top3_selection_report.json",
    "oracle_regret_report.json",
    "missed_winner_selection_report.json",
    "safe_full_soft_tag_report.json",
    "negative_guard_soft_tag_report.json",
    "ranking_coverage_audit.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

SCORE_INPUT_COLUMNS = {
    "pre_ret20_state",
    "pre_ret5_state",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "pre_candle_energy_state",
    "pre_wick_warning_state",
    "pre_volume_state",
    "pre_compression_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "event_daily_ret20_state",
    "event_daily_candle_state",
    "event_strength_score",
    "guard_safe_full",
    "negative_guard_match",
}

FUTURE_LABEL_COLUMNS = upside_mod.LABEL_COLUMNS


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


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_rate(count: int | float, total: int | float) -> float:
    if not total:
        return 0.0
    return float(count) / float(total)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _run_dir(root: str | Path, run_id: str, default_root: Path) -> Path:
    return _safe_path(root, default_root) / run_id


def _state_score(series: pd.Series, mapping: dict[str, float], default: float = 0.0) -> pd.Series:
    return series.astype(str).map(mapping).fillna(default).astype(float)


def add_research_scores(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    event_strength = pd.to_numeric(frame.get("event_strength_score", 0.0), errors="coerce").fillna(0.0)
    momentum = (
        _state_score(frame["pre_ret20_state"], {"pre20_strong_up": 2.0, "pre20_up": 1.2, "pre20_flat": 0.2, "pre20_down": -0.8, "pre20_strong_down": -1.2})
        + _state_score(frame["pre_ret5_state"], {"pre5_strong_up": 1.8, "pre5_up": 1.0, "pre5_flat": 0.1, "pre5_down": -0.6, "pre5_strong_down": -1.0})
        + _state_score(frame["weekly_prior_state"], {"weekly_prior_strong_up": 1.4, "weekly_prior_uptrend": 1.0, "weekly_prior_recovery": 0.6, "weekly_prior_mixed": 0.0, "weekly_prior_downtrend": -1.0})
        + _state_score(frame["monthly_prior_state"], {"monthly_prior_strong_up": 1.1, "monthly_prior_uptrend": 0.8, "monthly_prior_recovery": 0.4, "monthly_prior_mixed": 0.0, "monthly_prior_down_or_drawdown": -1.0})
        + _state_score(frame["pre_volume_state"], {"pre_volume_expansion": 0.6, "pre_volume_normal": 0.1, "pre_volume_dry": -0.2})
        + _state_score(frame["event_daily_ret20_state"], {"daily20_strong_up": 1.2, "daily20_up": 0.8, "daily20_flat": 0.0, "daily20_down": -0.3, "daily20_strong_down": -0.8})
    )
    reclaim_safety = (
        _state_score(frame["pre_ma20_path_state"], {"pre_ma20_reclaim_base": 1.4, "pre_ma20_near": 0.4, "pre_ma20_below_base": -0.2, "pre_ma20_already_extended": 0.3})
        + _state_score(frame["pre_candle_energy_state"], {"pre_candle_energy_positive": 0.8, "pre_candle_energy_mixed": 0.2, "pre_candle_energy_warning": -0.1})
        + _state_score(frame["pre_wick_warning_state"], {"pre_wicks_clean": 0.5, "pre_lower_wick_support": 0.4, "pre_upper_wick_or_failed_push": -0.25})
        + _state_score(frame["pre_compression_state"], {"pre_range_compressed": 0.4, "pre_range_normal": 0.1, "pre_range_wide": -0.4})
        + frame["guard_safe_full"].astype(float) * 0.8
    )
    risk_soft = (
        frame["negative_guard_match"].astype(float) * 0.8
        + _state_score(frame["pre_ma60_context_state"], {"pre_ma60_extended_above": 0.4, "pre_ma60_near_or_above": 0.0, "pre_ma60_below": 0.3})
        + _state_score(frame["pre_wick_warning_state"], {"pre_upper_wick_or_failed_push": 0.3}, default=0.0)
    )
    frame["score_momentum_continuation_soft_boost_v1"] = momentum + event_strength * 0.15 - risk_soft * 0.25 + frame["guard_safe_full"].astype(float) * 0.15
    frame["score_hybrid_reclaim_momentum_soft_risk_v1"] = momentum * 0.55 + reclaim_safety * 0.75 + event_strength * 0.10 - risk_soft * 0.45
    frame["score_all_strength_oracle_top3"] = frame["ret20_fwd"]
    return frame


def _stable_random_score(event_date: str, code: str, repeat: int) -> float:
    seed = int(hashlib.sha256(f"{RANDOM_SEED}|{repeat}|{event_date}|{code}".encode("utf-8")).hexdigest()[:16], 16)
    return random.Random(seed).random()


def _select_by_score(events: pd.DataFrame, family_id: str, score_column: str, *, top_k: int = TOP_K) -> pd.DataFrame:
    selected = events.copy()
    selected["research_family_id"] = family_id
    selected["research_score"] = pd.to_numeric(selected[score_column], errors="coerce").fillna(-9999.0)
    selected["selection_rank"] = selected.groupby("event_date")["research_score"].rank(method="first", ascending=False)
    selected = selected[selected["selection_rank"] <= top_k].copy()
    selected["selected_top1"] = selected["selection_rank"].eq(1.0)
    selected["selected_top3"] = True
    selected["threshold_mode"] = "always_select_top3"
    return selected


def build_selection_ledger(events: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    selected_frames = [
        _select_by_score(events, "all_strength_oracle_top3", "score_all_strength_oracle_top3"),
        _select_by_score(events, "momentum_continuation_soft_boost_v1", "score_momentum_continuation_soft_boost_v1"),
        _select_by_score(events, "hybrid_reclaim_momentum_soft_risk_v1", "score_hybrid_reclaim_momentum_soft_risk_v1"),
    ]
    random_reports = []
    random_selected = []
    for repeat in range(RANDOM_REPEATS):
        temp = events.copy()
        temp["score_random"] = [
            _stable_random_score(str(row.event_date), str(row.code), repeat)
            for row in temp[["event_date", "code"]].itertuples(index=False)
        ]
        picked = _select_by_score(temp, f"all_strength_scoreless_random_top3_repeat_{repeat}", "score_random")
        random_reports.append(_selection_metrics(picked, events, family_id=f"all_strength_scoreless_random_top3_repeat_{repeat}"))
        if repeat == 0:
            picked["research_family_id"] = "all_strength_scoreless_random_top3"
            random_selected.append(picked)
    if random_selected:
        selected_frames.extend(random_selected)
    selected = pd.concat(selected_frames, ignore_index=True)
    ledger_columns = [
        "research_family_id",
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
        "pre_ret20_state",
        "pre_ret5_state",
        "pre_ma20_path_state",
        "weekly_prior_state",
        "monthly_prior_state",
    ]
    return selected[ledger_columns].copy(), random_reports


def _selection_metrics(selected: pd.DataFrame, events: pd.DataFrame, *, family_id: str) -> dict[str, Any]:
    top1 = selected[selected["selection_rank"].eq(1.0)]
    top3 = selected[selected["selection_rank"].le(3.0)]
    opportunity_dates = set(events.loc[events["opportunity_day_top15"], "event_date"].unique().tolist())
    selected_dates = set(top3["event_date"].unique().tolist())
    winner_available_dates = set(events.loc[events["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    selected_top10_dates = set(top3.loc[top3["is_future_top10_by_ret20"], "event_date"].unique().tolist())
    weak_selection_dates = winner_available_dates - selected_top10_dates
    total_big_ret20 = int(events["is_big_winner_ret20_ge_10pct"].sum())
    total_big_mfe = int(events["is_big_winner_MFE20_ge_15pct"].sum())
    return {
        "family_id": family_id,
        "selected_event_count": int(len(top3)),
        "selected_day_count": int(len(selected_dates)),
        "selected_top1_avg_ret20": float(top1["ret20_fwd"].mean()) if len(top1) else 0.0,
        "selected_top3_avg_ret20": float(top3["ret20_fwd"].mean()) if len(top3) else 0.0,
        "selected_top1_win_rate20": float(top1["win20"].mean()) if len(top1) else 0.0,
        "selected_top3_win_rate20": float(top3["win20"].mean()) if len(top3) else 0.0,
        "selected_top3_avg_MFE20": float(top3["mfe20"].mean()) if len(top3) else 0.0,
        "selected_top3_avg_MAE20": float(top3["mae20"].mean()) if len(top3) else 0.0,
        "selected_top3_severe_loss_rate20": float(top3["severe_loss20"].mean()) if len(top3) else 0.0,
        "selected_top3_future_top10_precision": float(top3["is_future_top10_by_ret20"].mean()) if len(top3) else 0.0,
        "selected_top3_future_top5_precision": float(top3["is_future_top5_by_ret20"].mean()) if len(top3) else 0.0,
        "selected_top3_big_winner_ret20_ge_10_capture_rate": _safe_rate(int(top3["is_big_winner_ret20_ge_10pct"].sum()), total_big_ret20),
        "selected_top3_big_winner_MFE20_ge_15_capture_rate": _safe_rate(int(top3["is_big_winner_MFE20_ge_15pct"].sum()), total_big_mfe),
        "selected_nonwinner_when_winner_available_rate": _safe_rate(len(weak_selection_dates), len(winner_available_dates)),
        "opportunity_days_total": int(len(opportunity_dates)),
        "selected_on_opportunity_days": int(len(opportunity_dates & selected_dates)),
        "no_trade_on_opportunity_day_rate": _safe_rate(len(opportunity_dates - selected_dates), len(opportunity_dates)),
        "weak_selection_on_opportunity_day_rate": _safe_rate(len(weak_selection_dates), len(opportunity_dates)),
        "selected_negative_guard_matched_rate": float(top3["negative_guard_match"].mean()) if len(top3) else 0.0,
        "selected_safe_full_tag_rate": float(top3["guard_safe_full"].mean()) if len(top3) else 0.0,
        "negative_guard_winner_selected_count": int((top3["negative_guard_match"] & top3["is_big_winner_ret20_ge_10pct"]).sum()),
        "negative_guard_loser_selected_count": int((top3["negative_guard_match"] & top3["severe_loss20"]).sum()),
        "safe_full_winner_selected_count": int((top3["guard_safe_full"] & top3["is_big_winner_ret20_ge_10pct"]).sum()),
    }


def summarize_random_baseline(random_reports: list[dict[str, Any]]) -> dict[str, Any]:
    frame = pd.DataFrame(random_reports)
    out: dict[str, Any] = {"family_id": "all_strength_scoreless_random_top3", "repeat_count": len(random_reports)}
    for column in frame.columns:
        if column == "family_id":
            continue
        if pd.api.types.is_numeric_dtype(frame[column]):
            out[column] = float(frame[column].mean())
    return out


def build_reports(events: pd.DataFrame, selected: pd.DataFrame, random_reports: list[dict[str, Any]]) -> dict[str, Any]:
    random_summary = summarize_random_baseline(random_reports)
    family_reports = [random_summary]
    for family_id in ("all_strength_oracle_top3", "momentum_continuation_soft_boost_v1", "hybrid_reclaim_momentum_soft_risk_v1"):
        family_reports.append(_selection_metrics(selected[selected["research_family_id"].eq(family_id)], events, family_id=family_id))
    by_id = {row["family_id"]: row for row in family_reports}
    oracle = by_id["all_strength_oracle_top3"]
    random_base = by_id["all_strength_scoreless_random_top3"]
    all_event_avg = float(events["ret20_fwd"].mean())
    safe_full_avg = float(events.loc[events["guard_safe_full"], "ret20_fwd"].mean()) if events["guard_safe_full"].any() else 0.0
    candidate_counts = events.groupby("event_date").size()
    for row in family_reports:
        row["average_candidates_per_day"] = float(candidate_counts.mean())
        row["median_candidates_per_day"] = float(candidate_counts.median())
        row["p95_candidates_per_day"] = float(candidate_counts.quantile(0.95))
        row["oracle_top1_gap_ret20"] = row["selected_top1_avg_ret20"] - oracle["selected_top1_avg_ret20"]
        row["oracle_top3_gap_ret20"] = row["selected_top3_avg_ret20"] - oracle["selected_top3_avg_ret20"]
        row["oracle_top3_gap_MFE20"] = row["selected_top3_avg_MFE20"] - oracle["selected_top3_avg_MFE20"]
        row["regret_vs_all_strength_oracle_top3"] = oracle["selected_top3_avg_ret20"] - row["selected_top3_avg_ret20"]
        row["improvement_vs_random_top3"] = row["selected_top3_avg_ret20"] - random_base["selected_top3_avg_ret20"]
        row["improvement_vs_safe_full_hard_filter"] = row["selected_top3_avg_ret20"] - safe_full_avg
        row["improvement_vs_all_strength_event_average"] = row["selected_top3_avg_ret20"] - all_event_avg
        row["complete_champion_ranking_available"] = False
    score_leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_score_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": sorted(family_reports, key=lambda row: (row["family_id"] == "all_strength_oracle_top3", row["selected_top3_avg_ret20"]), reverse=True),
    }
    return {
        "family_reports": family_reports,
        "by_id": by_id,
        "score_leaderboard": score_leaderboard,
        "top1_selection_report": {
            "schema_version": f"{SCHEMA_PREFIX}_top1_selection_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": [
                {key: row[key] for key in ("family_id", "selected_top1_avg_ret20", "selected_top1_win_rate20", "oracle_top1_gap_ret20", "improvement_vs_random_top3")}
                for row in family_reports
            ],
        },
        "top3_selection_report": {
            "schema_version": f"{SCHEMA_PREFIX}_top3_selection_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": family_reports,
        },
        "oracle_regret_report": {
            "schema_version": f"{SCHEMA_PREFIX}_oracle_regret_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "rows": [
                {
                    "family_id": row["family_id"],
                    "oracle_top1_gap_ret20": row["oracle_top1_gap_ret20"],
                    "oracle_top3_gap_ret20": row["oracle_top3_gap_ret20"],
                    "oracle_top3_gap_MFE20": row["oracle_top3_gap_MFE20"],
                    "regret_vs_all_strength_oracle_top3": row["regret_vs_all_strength_oracle_top3"],
                }
                for row in family_reports
            ],
        },
    }


def build_soft_tag_reports(events: pd.DataFrame, selected: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    rows_safe = []
    rows_negative = []
    for family_id, group in selected.groupby("research_family_id", sort=True):
        if family_id.startswith("all_strength_scoreless_random_top3_repeat_"):
            continue
        top3 = group[group["selection_rank"].le(3.0)]
        safe = top3[top3["guard_safe_full"]]
        neg = top3[top3["negative_guard_match"]]
        rows_safe.append(
            {
                "family_id": family_id,
                "selected_safe_full_tag_rate": float(top3["guard_safe_full"].mean()) if len(top3) else 0.0,
                "safe_full_winner_selected_count": int((safe["is_big_winner_ret20_ge_10pct"]).sum()) if len(safe) else 0,
                "safe_full_avg_ret20_selected": float(safe["ret20_fwd"].mean()) if len(safe) else None,
                "safe_full_used_as_hard_filter": False,
            }
        )
        rows_negative.append(
            {
                "family_id": family_id,
                "selected_negative_guard_matched_rate": float(top3["negative_guard_match"].mean()) if len(top3) else 0.0,
                "negative_guard_winner_selected_count": int((neg["is_big_winner_ret20_ge_10pct"]).sum()) if len(neg) else 0,
                "negative_guard_loser_selected_count": int((neg["severe_loss20"]).sum()) if len(neg) else 0,
                "negative_guard_avg_ret20_selected": float(neg["ret20_fwd"].mean()) if len(neg) else None,
                "negative_guard_used_as_hard_veto": False,
                "decomposition_available": True,
            }
        )
    return (
        {"schema_version": f"{SCHEMA_PREFIX}_safe_full_soft_tag_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows_safe},
        {"schema_version": f"{SCHEMA_PREFIX}_negative_guard_soft_tag_report_v1", "generated_at": _utc_now(), "axis_id": AXIS_ID, "rows": rows_negative},
    )


def build_missed_winner_selection_report(events: pd.DataFrame, selected: pd.DataFrame) -> dict[str, Any]:
    rows = []
    winner_dates = set(events.loc[events["is_future_top10_by_ret20"], "event_date"].unique())
    for family_id, group in selected.groupby("research_family_id", sort=True):
        if family_id.startswith("all_strength_scoreless_random_top3_repeat_"):
            continue
        top3 = group[group["selection_rank"].le(3.0)]
        selected_top10_dates = set(top3.loc[top3["is_future_top10_by_ret20"], "event_date"].unique())
        rows.append(
            {
                "family_id": family_id,
                "selected_nonwinner_when_winner_available_rate": _safe_rate(len(winner_dates - selected_top10_dates), len(winner_dates)),
                "selected_top3_future_top10_precision": float(top3["is_future_top10_by_ret20"].mean()) if len(top3) else 0.0,
                "selected_top3_future_top5_precision": float(top3["is_future_top5_by_ret20"].mean()) if len(top3) else 0.0,
                "selection_miss_available": True,
                "complete_champion_ranking_available": False,
            }
        )
    return {
        "schema_version": f"{SCHEMA_PREFIX}_missed_winner_selection_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }


def build_ranking_coverage_audit(upside_dir: Path) -> dict[str, Any]:
    source = upside_dir / "ranking_coverage_audit.json"
    if not source.exists():
        return {
            "schema_version": f"{SCHEMA_PREFIX}_ranking_coverage_audit_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "complete_champion_ranking_available": False,
            "reason": "source_upside_ranking_coverage_audit_missing",
        }
    data = _load_json(source)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_ranking_coverage_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "complete_champion_ranking_available": bool(data.get("complete_topk_ranking_available") is True),
        "source_reason": data.get("reason"),
        "ranking_score_coverage_rate": data.get("ranking_score_coverage_rate"),
        "ranking_score_covered_count": data.get("ranking_score_covered_count"),
        "event_count": data.get("event_count"),
        "sparse_ranking_used_as_champion_proof": False,
    }


def build_contract_artifacts(
    *,
    pattern_dir: Path,
    guard_dir: Path,
    upside_dir: Path,
    events: pd.DataFrame,
    ranking_audit: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    candidate_pool_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_pool_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_pool_id": "all_strength_baseline",
        "candidate_pool_policy": "wide pool from pre_strength_event_ledger; safe_full and negative_guard are soft tags only",
        "candidate_count": int(len(events)),
        "candidate_day_count": int(events["event_date"].nunique()),
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
    }
    feature_availability = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "score_input_columns": sorted(SCORE_INPUT_COLUMNS),
        "future_label_columns": sorted(FUTURE_LABEL_COLUMNS),
        "future_labels_used_in_score_inputs": bool(SCORE_INPUT_COLUMNS.intersection(FUTURE_LABEL_COLUMNS)),
        "silent_fallback_used": False,
    }
    scoring_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_scoring_family_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "scoring_families": {
            "all_strength_scoreless_random_top3": "seeded repeated random top3 lower-bound reference",
            "all_strength_oracle_top3": "future-label oracle upper bound; evaluation-only",
            "momentum_continuation_soft_boost_v1": "fixed predeclared momentum continuation score using past/current state columns",
            "hybrid_reclaim_momentum_soft_risk_v1": "fixed predeclared reclaim/momentum blend using safe/risk tags softly",
        },
        "weights_learned_from_full_period": False,
        "future_labels_used_in_score_inputs": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
    }
    split_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_split_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "split_policy": "fixed predeclared weights; no label training performed; report time-block stability by calendar year",
        "training_used": False,
        "embargo_required": False,
        "research_fallback_used": False,
        "keep_candidate_allowed_from_split": True,
    }
    source_refs = upside_mod.build_source_artifact_refs(pattern_dir, guard_dir)
    source_refs["schema_version"] = f"{SCHEMA_PREFIX}_source_artifact_refs_v1"
    source_refs["source_upside_artifact_root"] = str(upside_dir)
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "wide_strength_pool_upside_rerank",
        "boundary": "TRADEX-only",
        "axis_moved": "wide_strength_pool_upside_rerank",
        "pattern_artifact_root": str(pattern_dir),
        "guard_artifact_root": str(guard_dir),
        "upside_artifact_root": str(upside_dir),
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": TOP_K,
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "cost_slippage_evaluated": False,
            "cost_slippage_ignored_by_user_intent": True,
        },
        "complete_champion_ranking_available": ranking_audit["complete_champion_ranking_available"],
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
        "candidate_pool_contract.json": candidate_pool_contract,
        "feature_availability_audit.json": feature_availability,
        "scoring_family_contract.json": scoring_contract,
        "split_contract.json": split_contract,
    }


def _time_block_stability(events: pd.DataFrame, selected: pd.DataFrame, family_id: str) -> dict[str, Any]:
    group = selected[selected["research_family_id"].eq(family_id)].copy()
    if group.empty:
        return {"family_id": family_id, "time_block_count": 0, "positive_time_block_rate": 0.0, "stable_enough": False}
    group["year"] = group["event_date"].astype(str).str.slice(0, 4)
    rows = group.groupby("year")["ret20_fwd"].mean()
    return {
        "family_id": family_id,
        "time_block_count": int(len(rows)),
        "positive_time_block_rate": float((rows > 0).mean()) if len(rows) else 0.0,
        "stable_enough": bool(len(rows) >= 5 and float((rows > 0).mean()) >= 0.55),
    }


def build_research_decision(
    *,
    reports: dict[str, Any],
    selected: pd.DataFrame,
    events: pd.DataFrame,
    ranking_audit: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    by_id = reports["by_id"]
    candidates = [by_id["momentum_continuation_soft_boost_v1"], by_id["hybrid_reclaim_momentum_soft_risk_v1"]]
    best = max(candidates, key=lambda row: row["selected_top3_avg_ret20"])
    random_base = by_id["all_strength_scoreless_random_top3"]
    safe_avg = float(events.loc[events["guard_safe_full"], "ret20_fwd"].mean()) if events["guard_safe_full"].any() else 0.0
    time_stability = _time_block_stability(events, selected, best["family_id"])
    complete_champion = bool(ranking_audit.get("complete_champion_ranking_available") is True)
    metric_pass = (
        best["selected_top3_avg_ret20"] > random_base["selected_top3_avg_ret20"]
        and best["selected_top3_avg_ret20"] > float(events["ret20_fwd"].mean())
        and best["selected_top3_big_winner_ret20_ge_10_capture_rate"] > by_id["all_strength_scoreless_random_top3"]["selected_top3_big_winner_ret20_ge_10_capture_rate"]
        and best["selected_nonwinner_when_winner_available_rate"] < random_base["selected_nonwinner_when_winner_available_rate"]
        and best["selected_negative_guard_matched_rate"] > 0.0
        and time_stability["stable_enough"]
    )
    recreates_safe_filter = best["selected_safe_full_tag_rate"] > 0.50
    vetoes_negative_guard = best["selected_negative_guard_matched_rate"] < 0.05
    if not artifact_complete or recreates_safe_filter or vetoes_negative_guard:
        decision = "drop"
        authoritative = "wide_strength_pool_upside_rerank_drop"
    elif metric_pass:
        decision = "keep_candidate" if complete_champion else "hold"
        authoritative = "wide_strength_pool_upside_rerank_keep_candidate" if complete_champion else "wide_strength_pool_upside_rerank_hold"
    else:
        decision = "hold"
        authoritative = "wide_strength_pool_upside_rerank_hold"
    typed_reasons = [
        "research_only_scoring_created",
        "complete_champion_ranking_unavailable" if not complete_champion else "complete_champion_ranking_available",
        "top3_metric_gate_passed" if metric_pass else "top3_metric_gate_incomplete",
    ]
    if recreates_safe_filter:
        typed_reasons.append("scorer_recreates_safe_full_hard_filter_behavior")
    if vetoes_negative_guard:
        typed_reasons.append("scorer_recreates_negative_guard_veto_behavior")
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "wide_strength_pool_upside_rerank",
        "boundary": "TRADEX-only",
        "axis_moved": "wide_strength_pool_upside_rerank",
        "source_upside_decision": "upside_capture_failed",
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "best_research_family_id": best["family_id"],
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "cost_slippage_evaluated": False,
        "cost_slippage_ignored_by_user_intent": True,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "safe_full_used_as_soft_tag": True,
        "negative_guard_used_as_soft_tag": True,
        "future_labels_used_for_training_or_evaluation_only": True,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "complete_champion_ranking_available": complete_champion,
        "typed_reasons": typed_reasons,
        "decision_reasons": [
            {"code": "selected_top3_avg_ret20_gt_random", "status": "pass" if best["selected_top3_avg_ret20"] > random_base["selected_top3_avg_ret20"] else "fail", "value": best["selected_top3_avg_ret20"] - random_base["selected_top3_avg_ret20"]},
            {"code": "selected_top3_avg_ret20_gt_event_average", "status": "pass" if best["selected_top3_avg_ret20"] > float(events["ret20_fwd"].mean()) else "fail", "value": best["selected_top3_avg_ret20"] - float(events["ret20_fwd"].mean())},
            {"code": "selected_nonwinner_when_winner_available_improves", "status": "pass" if best["selected_nonwinner_when_winner_available_rate"] < random_base["selected_nonwinner_when_winner_available_rate"] else "fail", "value": best["selected_nonwinner_when_winner_available_rate"]},
            {"code": "negative_guard_winners_not_discarded", "status": "pass" if best["selected_negative_guard_matched_rate"] > 0.0 else "fail", "value": best["selected_negative_guard_matched_rate"]},
            {"code": "safe_full_not_hard_filter", "status": "pass" if not recreates_safe_filter else "fail", "value": best["selected_safe_full_tag_rate"]},
            {"code": "time_block_stability", "status": "pass" if time_stability["stable_enough"] else "fail", "value": time_stability},
            {"code": "complete_champion_ranking_available", "status": "pass" if complete_champion else "hold_blocker", "value": complete_champion},
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
        ],
        "best_research_family_metrics": best,
        "random_baseline_metrics": random_base,
        "safe_full_hard_filter_avg_ret20_reference": safe_avg,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "complete": all(required_existing.values()),
        "required_artifacts": required_existing,
        "paths": paths,
        "decision": decision.get("decision") if decision else None,
        "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
        "silent_fallback_used": False,
        "candidate_scoring_created": True,
        "candidate_scoring_scope": "research_only",
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def run_wide_strength_pool_upside_rerank_v1(
    *,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_guard_run_id: str = DEFAULT_GUARD_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    guard_root: str | Path = DEFAULT_GUARD_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    guard_dir = _run_dir(guard_root, source_guard_run_id, DEFAULT_GUARD_ROOT)
    upside_dir = _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT)
    inputs = upside_mod.load_inputs(pattern_dir, guard_dir)
    events = add_research_scores(inputs["events"])
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    ranking_audit = build_ranking_coverage_audit(upside_dir)
    contracts_payload = build_contract_artifacts(pattern_dir=pattern_dir, guard_dir=guard_dir, upside_dir=upside_dir, events=events, ranking_audit=ranking_audit)
    selected, random_reports = build_selection_ledger(events)
    reports = build_reports(events, selected, random_reports)
    safe_tag_report, negative_tag_report = build_soft_tag_reports(events, selected)
    missed_report = build_missed_winner_selection_report(events, selected)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=RANDOM_SEED,
        random_seed=RANDOM_SEED,
        input_artifacts=[
            {"name": "source_pattern_artifact_root", "path": str(pattern_dir)},
            {"name": "source_guard_artifact_root", "path": str(guard_dir)},
            {"name": "source_upside_artifact_root", "path": str(upside_dir)},
            {"name": "evaluation_contract", "contract_hash": contracts_payload["evaluation_contract.json"]["contract_hash"]},
        ],
        asof=str(int(events["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "top_k": TOP_K,
            "random_repeats": RANDOM_REPEATS,
            "candidate_scoring_created": True,
            "candidate_scoring_scope": "research_only",
            "production_ranking_changed": False,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "wide_strength_pool_upside_rerank"},
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
        "score_leaderboard.json": reports["score_leaderboard"],
        "top1_selection_report.json": reports["top1_selection_report"],
        "top3_selection_report.json": reports["top3_selection_report"],
        "oracle_regret_report.json": reports["oracle_regret_report"],
        "missed_winner_selection_report.json": missed_report,
        "safe_full_soft_tag_report.json": safe_tag_report,
        "negative_guard_soft_tag_report.json": negative_tag_report,
        "ranking_coverage_audit.json": ranking_audit,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["date_level_selection_ledger.jsonl"] = str(_write_jsonl(output_dir / "date_level_selection_ledger.jsonl", selected.to_dict(orient="records")))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        reports=reports,
        selected=selected,
        events=events,
        ranking_audit=ranking_audit,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "best_research_family_id": decision["best_research_family_id"],
        "best_research_family_metrics": decision["best_research_family_metrics"],
        "complete_champion_ranking_available": decision["complete_champion_ranking_available"],
        "silent_fallback_used": False,
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
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--guard-root", default=str(DEFAULT_GUARD_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    result = run_wide_strength_pool_upside_rerank_v1(
        source_pattern_run_id=args.source_pattern_run_id,
        source_guard_run_id=args.source_guard_run_id,
        source_upside_run_id=args.source_upside_run_id,
        pattern_root=args.pattern_root,
        guard_root=args.guard_root,
        upside_root=args.upside_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
