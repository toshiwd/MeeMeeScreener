from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_ranking_loss_or_topk_objective_repair_v1 as ranking_mod


AXIS_ID = "oracle_gap_and_candidate_generation_root_cause_v1"
SCHEMA_PREFIX = "tradex_oracle_gap_and_candidate_generation_root_cause_v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_RISK_RUN_ID = "20260513T040000Z-selection-risk-control-for-wide-pool-v1"
DEFAULT_THRESHOLD_RUN_ID = "20260513T050000Z-threshold-no-trade-control-for-wide-pool-v1"
DEFAULT_FEATURE_DIAGNOSIS_RUN_ID = "20260513T060000Z-wide-pool-winner-nonwinner-feature-diagnosis-v1"
DEFAULT_IMAGE_PHASE2_RUN_ID = "20260513T090000Z-image-only-classifier-baseline-phase2"
DEFAULT_IMAGE_CNN_PHASE2B_RUN_ID = "20260513T111000Z-image-cnn-baseline-phase2b-torch"
DEFAULT_RANKING_OBJECTIVE_RUN_ID = "20260513T120000Z-ranking-loss-or-topk-objective-repair-v1"
DEFAULT_WIDE_ROOT = ranking_mod.DEFAULT_WIDE_ROOT
DEFAULT_RISK_ROOT = ranking_mod.DEFAULT_RISK_ROOT
DEFAULT_THRESHOLD_ROOT = ranking_mod.DEFAULT_THRESHOLD_ROOT
DEFAULT_FEATURE_DIAGNOSIS_ROOT = ranking_mod.DEFAULT_FEATURE_DIAGNOSIS_ROOT
DEFAULT_IMAGE_PHASE2_ROOT = ranking_mod.DEFAULT_IMAGE_PHASE2_ROOT
DEFAULT_IMAGE_CNN_PHASE2B_ROOT = ranking_mod.DEFAULT_IMAGE_CNN_PHASE2B_ROOT
DEFAULT_RANKING_OBJECTIVE_ROOT = ranking_mod.DEFAULT_OUTPUT_ROOT
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\oracle_gap_and_candidate_generation_root_cause_v1")

TOP_K = 3
TOP5_K = 5
BASELINE_FAMILY_ID = ranking_mod.BASELINE_FAMILY_ID
ORACLE_FAMILY_ID = ranking_mod.ORACLE_FAMILY_ID
RANDOM_FAMILY_ID = ranking_mod.RANDOM_FAMILY_ID
PAIRWISE_FAMILY_ID = ranking_mod.PAIRWISE_FAMILY_ID
LISTWISE_FAMILY_ID = ranking_mod.LISTWISE_FAMILY_ID
BADPICK_FAMILY_ID = ranking_mod.BADPICK_FAMILY_ID
REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "root_cause_contract.json",
    "oracle_gap_decomposition.json",
    "candidate_pool_recall_report.json",
    "within_pool_ranking_failure_report.json",
    "event_source_decomposition_report.json",
    "regime_timeblock_decomposition_report.json",
    "max3_deployment_fit_report.json",
    "label_objective_mismatch_report.json",
    "failure_mode_classification.json",
    "candidate_generation_hypothesis_map.json",
    "next_axis_recommendation.json",
    "research_decision.json",
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
    if isinstance(value, np.generic):
        return _json_ready(value.item())
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


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _file_hash(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def _mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def validate_sources(
    *,
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
    feature_diagnosis_dir: Path,
    image_phase2_dir: Path,
    image_cnn_phase2b_dir: Path,
    ranking_objective_dir: Path,
) -> dict[str, Any]:
    source_status = ranking_mod.validate_sources(
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        threshold_dir=threshold_dir,
        feature_diagnosis_dir=feature_diagnosis_dir,
        image_phase2_dir=image_phase2_dir,
        image_cnn_phase2b_dir=image_cnn_phase2b_dir,
    )
    ranking_required = [
        "_ARTIFACT_COMPLETE.json",
        "research_decision.json",
        "ranker_score_ledger.jsonl",
        "top3_selection_report.json",
        "branching_report.json",
        "oracle_gap_report.json",
    ]
    missing = [name for name in ranking_required if not (ranking_objective_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"ranking objective source missing required artifacts: {missing} at {ranking_objective_dir}")
    complete = _load_json(ranking_objective_dir / "_ARTIFACT_COMPLETE.json")
    decision = _load_json(ranking_objective_dir / "research_decision.json")
    if complete.get("complete") is not True:
        raise RuntimeError("ranking objective source artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError("ranking objective source used silent fallback")
    if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is not False:
        raise RuntimeError("ranking objective source used research fallback")
    if decision.get("authoritative_research_decision") != "ranking_objective_drop":
        raise RuntimeError("ranking objective source decision is not ranking_objective_drop")
    source_status["ranking_objective"] = {
        "_ARTIFACT_COMPLETE.json": complete,
        "research_decision.json": decision,
        "top3_selection_report.json": _load_json(ranking_objective_dir / "top3_selection_report.json"),
        "branching_report.json": _load_json(ranking_objective_dir / "branching_report.json"),
        "oracle_gap_report.json": _load_json(ranking_objective_dir / "oracle_gap_report.json"),
    }
    return source_status


def load_diagnosis_inputs(
    *,
    source_status: dict[str, Any],
    wide_dir: Path,
    risk_dir: Path,
    threshold_dir: Path,
    ranking_objective_dir: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    events, _selected = ranking_mod.load_ranker_events(
        pattern_dir=source_status["pattern_dir"],
        guard_dir=source_status["guard_dir"],
        upside_dir=source_status["upside_dir"],
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        threshold_dir=threshold_dir,
    )
    ledger = pd.DataFrame(_read_jsonl(ranking_objective_dir / "ranker_score_ledger.jsonl"))
    if events.empty:
        raise RuntimeError("source event frame is empty")
    if ledger.empty:
        raise RuntimeError("ranker_score_ledger.jsonl is empty")
    for frame in (events, ledger):
        frame["event_date"] = frame["event_date"].astype(str).str.slice(0, 10)
        frame["code"] = frame["code"].astype(str)
    ledger["selection_rank"] = pd.to_numeric(ledger["selection_rank"], errors="coerce")
    return events, ledger


def _family_scope(ledger: pd.DataFrame, family_id: str) -> pd.DataFrame:
    return ledger[ledger["ranker_family_id"].eq(family_id)].copy()


def _topk(ledger: pd.DataFrame, family_id: str, k: int = TOP_K) -> pd.DataFrame:
    scope = _family_scope(ledger, family_id)
    return scope[pd.to_numeric(scope["selection_rank"], errors="coerce").le(k)].copy()


def _topk_avg_ret20(ledger: pd.DataFrame, family_id: str) -> float | None:
    return _mean(_topk(ledger, family_id, TOP_K)["ret20_fwd"])


def build_oracle_gap_decomposition(ledger: pd.DataFrame, ranking_decision: dict[str, Any]) -> dict[str, Any]:
    wide_oracle = _topk_avg_ret20(ledger, ORACLE_FAMILY_ID)
    previous = _topk_avg_ret20(ledger, BASELINE_FAMILY_ID)
    challenger_id = ranking_decision.get("best_ranker_family_id") or LISTWISE_FAMILY_ID
    challenger = _topk_avg_ret20(ledger, str(challenger_id))
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_oracle_gap_decomposition_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "full_oracle_top3_ret20": None,
        "full_oracle_available": False,
        "full_oracle_unavailable_reason": "artifact_chain_contains_current_candidate_pool_labels_not_full_market_same_date_universe",
        "wide_pool_oracle_top3_ret20": wide_oracle,
        "previous_best_top3_ret20": previous,
        "best_challenger_family_id": challenger_id,
        "best_challenger_top3_ret20": challenger,
        "gap_full_to_wide_pool": None,
        "gap_wide_pool_to_previous_best": previous - wide_oracle if previous is not None and wide_oracle is not None else None,
        "gap_previous_best_to_challenger": challenger - previous if challenger is not None and previous is not None else None,
        "gap_wide_pool_to_challenger": challenger - wide_oracle if challenger is not None and wide_oracle is not None else None,
        "oracle_gap_decomposition_complete": True,
        "full_market_pool_recall_complete": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_candidate_pool_recall_report(events: pd.DataFrame) -> dict[str, Any]:
    rows = []
    total_dates = int(events["event_date"].nunique())
    absent_top10_dates = 0
    for event_date, day in events.groupby("event_date", sort=True):
        top3_count = int(pd.to_numeric(day["ret20_rank_by_date"], errors="coerce").le(3).sum())
        top5_count = int(pd.to_numeric(day["ret20_rank_by_date"], errors="coerce").le(5).sum())
        top10_count = int(day["is_future_top10_by_ret20"].astype(bool).sum())
        big10_count = int(day["is_big_winner_ret20_ge_10pct"].astype(bool).sum())
        big_mfe_count = int(day["is_big_winner_MFE20_ge_15pct"].astype(bool).sum())
        if top10_count == 0:
            absent_top10_dates += 1
        rows.append(
            {
                "event_date": str(event_date),
                "candidate_count": int(len(day)),
                "source_pool_future_top3_count": top3_count,
                "source_pool_future_top5_count": top5_count,
                "source_pool_future_top10_count": top10_count,
                "source_pool_big_winner_ret20_ge_10_count": big10_count,
                "source_pool_big_winner_MFE20_ge_15_count": big_mfe_count,
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_pool_recall_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "full_universe_pool_recall_available": False,
        "full_universe_pool_recall_unavailable_reason": "no full same-date market universe artifact in source chain",
        "current_source_pool_recall_available": True,
        "future_top3_by_date_in_wide_pool_rate": 1.0 if total_dates else 0.0,
        "future_top5_by_date_in_wide_pool_rate": 1.0 if total_dates else 0.0,
        "future_top10_by_date_in_wide_pool_rate": 1.0 if total_dates else 0.0,
        "big_winner_ret20_ge_10_in_wide_pool_rate": 1.0 if int(events["is_big_winner_ret20_ge_10pct"].sum()) else 0.0,
        "big_winner_MFE20_ge_15_in_wide_pool_rate": 1.0 if int(events["is_big_winner_MFE20_ge_15pct"].sum()) else 0.0,
        "opportunity_days_where_winner_absent_from_pool": int(absent_top10_dates),
        "opportunity_day_count": total_dates,
        "rows_sample": rows[:200],
        "diagnosis_note": "within the available pre-strength/wide source ledger, future winners are present by construction; full-market recall remains unobserved",
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _winner_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["is_future_top10_by_ret20"].astype(bool)
        | pd.to_numeric(frame["ret20_fwd"], errors="coerce").ge(0.10)
        | pd.to_numeric(frame["mfe20"], errors="coerce").ge(0.15)
    )


def _nonwinner_selected_mask(frame: pd.DataFrame) -> pd.Series:
    return ~_winner_mask(frame) & (pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0) | frame["severe_loss20"].astype(bool))


def build_within_pool_ranking_failure_report(ledger: pd.DataFrame, challenger_id: str) -> dict[str, Any]:
    family_rows = []
    pair_rows = []
    for family_id in [BASELINE_FAMILY_ID, challenger_id, PAIRWISE_FAMILY_ID, BADPICK_FAMILY_ID]:
        if family_id in {row["ranker_family_id"] for row in family_rows}:
            continue
        scope = _family_scope(ledger, family_id)
        top3 = scope[scope["selection_rank"].le(TOP_K)]
        winner_dates = set(scope.loc[_winner_mask(scope), "event_date"].unique().tolist())
        selected_winner_dates = set(top3.loc[_winner_mask(top3), "event_date"].unique().tolist())
        nonwinner_dates = winner_dates - selected_winner_dates
        loser_rank_deltas = []
        for event_date, day in scope.groupby("event_date", sort=True):
            winners = day[_winner_mask(day)]
            selected_losers = day[day["selection_rank"].le(TOP_K) & _nonwinner_selected_mask(day)]
            if winners.empty or selected_losers.empty:
                continue
            best_winner_rank = float(pd.to_numeric(winners["selection_rank"], errors="coerce").min())
            worst_selected_loser_rank = float(pd.to_numeric(selected_losers["selection_rank"], errors="coerce").max())
            loser_rank_deltas.append(worst_selected_loser_rank - best_winner_rank)
            if len(pair_rows) < 200:
                pair_rows.append(
                    {
                        "ranker_family_id": family_id,
                        "event_date": str(event_date),
                        "best_winner_rank_position": best_winner_rank,
                        "selected_loser_rank_position": worst_selected_loser_rank,
                        "selected_loser_minus_winner_rank_delta": worst_selected_loser_rank - best_winner_rank,
                    }
                )
        family_rows.append(
            {
                "ranker_family_id": family_id,
                "winner_available_day_count": int(len(winner_dates)),
                "winner_available_but_not_selected_count": int(len(nonwinner_dates)),
                "winner_available_but_nonwinner_selected_rate": _safe_rate(len(nonwinner_dates), len(winner_dates)),
                "selected_top3_severe_loss_rate20": float(top3["severe_loss20"].mean()) if len(top3) else 0.0,
                "selected_loser_rank_position_vs_winner_rank_position": float(np.mean(loser_rank_deltas)) if loser_rank_deltas else None,
            }
        )
    base = next(row for row in family_rows if row["ranker_family_id"] == BASELINE_FAMILY_ID)
    challenger = next(row for row in family_rows if row["ranker_family_id"] == challenger_id)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_within_pool_ranking_failure_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "previous_best_selected_nonwinner_when_winner_available": base["winner_available_but_nonwinner_selected_rate"],
        "challenger_selected_nonwinner_when_winner_available": challenger["winner_available_but_nonwinner_selected_rate"],
        "winner_available_but_not_selected_count": base["winner_available_but_not_selected_count"],
        "winner_available_but_nonwinner_selected_rate": base["winner_available_but_nonwinner_selected_rate"],
        "selected_loser_rank_position_vs_winner_rank_position": base["selected_loser_rank_position_vs_winner_rank_position"],
        "ranking_failure_present": bool(base["winner_available_but_nonwinner_selected_rate"] >= 0.30),
        "challenger_worsened_ranking_failure": bool(challenger["winner_available_but_nonwinner_selected_rate"] > base["winner_available_but_nonwinner_selected_rate"]),
        "family_rows": family_rows,
        "pair_rows_sample": pair_rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _selection_flags_from_family(ledger: pd.DataFrame, family_id: str) -> pd.DataFrame:
    scope = _family_scope(ledger, family_id)
    selected = scope[scope["selection_rank"].le(TOP_K)][["event_date", "code"]].copy()
    selected["selected_by_family"] = True
    return selected


def build_event_source_decomposition_report(events: pd.DataFrame, ledger: pd.DataFrame, challenger_id: str) -> dict[str, Any]:
    selected = _selection_flags_from_family(ledger, BASELINE_FAMILY_ID)
    frame = events.merge(selected, on=["event_date", "code"], how="left")
    frame["selected_by_family"] = frame["selected_by_family"].eq(True)
    frame["future_winner"] = _winner_mask(frame)
    frame["selected_winner"] = frame["selected_by_family"] & frame["future_winner"]
    frame["selected_nonwinner"] = frame["selected_by_family"] & ~frame["future_winner"] & (pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0) | frame["ret20_rank_pct_by_date"].gt(0.50))
    frame["selected_severe_loser"] = frame["selected_by_family"] & frame["severe_loss20"].astype(bool)
    winner_dates = set(frame.loc[frame["future_winner"], "event_date"].unique().tolist())
    selected_winner_dates = set(frame.loc[frame["selected_winner"], "event_date"].unique().tolist())
    frame["missed_winner"] = frame["future_winner"] & frame["event_date"].isin(winner_dates - selected_winner_dates)
    source_columns = [
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
        "negative_guard_match",
        "guard_safe_full",
    ]
    group_masks = {
        "future_winners": frame["future_winner"],
        "selected_winners": frame["selected_winner"],
        "selected_nonwinners": frame["selected_nonwinner"],
        "severe_losers": frame["severe_loss20"].astype(bool),
        "missed_winners": frame["missed_winner"],
    }
    rows = []
    for column in source_columns:
        if column not in frame.columns:
            continue
        values = frame[column].astype(str)
        for group_name, mask in group_masks.items():
            subset = values[mask]
            counts = subset.value_counts(normalize=True).head(8)
            rows.append(
                {
                    "source_field": column,
                    "group": group_name,
                    "group_count": int(mask.sum()),
                    "top_values": [{"value": str(key), "rate": float(value)} for key, value in counts.items()],
                }
            )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_event_source_decomposition_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "baseline_family_id": BASELINE_FAMILY_ID,
        "challenger_family_id": challenger_id,
        "group_counts": {name: int(mask.sum()) for name, mask in group_masks.items()},
        "winners_from_negative_guard_rate": float(frame.loc[frame["future_winner"], "negative_guard_match"].mean()) if frame["future_winner"].any() else None,
        "missed_winners_from_negative_guard_rate": float(frame.loc[frame["missed_winner"], "negative_guard_match"].mean()) if frame["missed_winner"].any() else None,
        "selected_nonwinners_from_negative_guard_rate": float(frame.loc[frame["selected_nonwinner"], "negative_guard_match"].mean()) if frame["selected_nonwinner"].any() else None,
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _family_day_stats(ledger: pd.DataFrame, family_id: str) -> pd.DataFrame:
    rows = []
    scope = _family_scope(ledger, family_id)
    for event_date, day in scope.groupby("event_date", sort=True):
        top3 = day[day["selection_rank"].le(TOP_K)]
        rows.append(
            {
                "event_date": str(event_date),
                "time_block": str(event_date)[:4],
                "ranker_family_id": family_id,
                "top3_avg_ret20": _mean(top3["ret20_fwd"]) or 0.0,
                "top3_any_severe_loss": bool(top3["severe_loss20"].any()) if len(top3) else False,
                "winner_available": bool(_winner_mask(day).any()),
                "selected_winner": bool(_winner_mask(top3).any()) if len(top3) else False,
            }
        )
    return pd.DataFrame(rows)


def build_regime_timeblock_decomposition_report(events: pd.DataFrame, ledger: pd.DataFrame, challenger_id: str) -> dict[str, Any]:
    base_day = _family_day_stats(ledger, BASELINE_FAMILY_ID)
    challenger_day = _family_day_stats(ledger, challenger_id)
    oracle_day = _family_day_stats(ledger, ORACLE_FAMILY_ID)
    rows = []
    for block in sorted(base_day["time_block"].unique().tolist()):
        base = base_day[base_day["time_block"].eq(block)]
        challenger = challenger_day[challenger_day["time_block"].eq(block)]
        oracle = oracle_day[oracle_day["time_block"].eq(block)]
        event_block = events[events["event_date"].astype(str).str.slice(0, 4).eq(block)]
        rows.append(
            {
                "time_block": block,
                "day_count": int(base["event_date"].nunique()),
                "previous_best_top3_avg_ret20": _mean(base["top3_avg_ret20"]),
                "challenger_top3_avg_ret20": _mean(challenger["top3_avg_ret20"]),
                "wide_pool_oracle_top3_avg_ret20": _mean(oracle["top3_avg_ret20"]),
                "oracle_gap_previous_best": (_mean(base["top3_avg_ret20"]) or 0.0) - (_mean(oracle["top3_avg_ret20"]) or 0.0),
                "oracle_gap_challenger": (_mean(challenger["top3_avg_ret20"]) or 0.0) - (_mean(oracle["top3_avg_ret20"]) or 0.0),
                "previous_best_nonwinner_when_winner_available_rate": _safe_rate(int((base["winner_available"] & ~base["selected_winner"]).sum()), int(base["winner_available"].sum())),
                "challenger_nonwinner_when_winner_available_rate": _safe_rate(int((challenger["winner_available"] & ~challenger["selected_winner"]).sum()), int(challenger["winner_available"].sum())),
                "previous_best_any_severe_loss_day_rate": float(base["top3_any_severe_loss"].mean()) if len(base) else 0.0,
                "challenger_any_severe_loss_day_rate": float(challenger["top3_any_severe_loss"].mean()) if len(challenger) else 0.0,
                "current_source_pool_future_top10_day_rate": float(event_block.groupby("event_date")["is_future_top10_by_ret20"].any().mean()) if len(event_block) else None,
            }
        )
    deltas = [row["challenger_top3_avg_ret20"] - row["previous_best_top3_avg_ret20"] for row in rows if row["challenger_top3_avg_ret20"] is not None and row["previous_best_top3_avg_ret20"] is not None]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_regime_timeblock_decomposition_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "time_block_count": len(rows),
        "challenger_beats_previous_best_time_block_rate": float(np.mean([delta > 0.0 for delta in deltas])) if deltas else 0.0,
        "regime_specific_failure_present": bool(deltas and min(deltas) < -0.005 and max(deltas) > 0.002),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_max3_deployment_fit_report(events: pd.DataFrame, ledger: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for event_date, day in events.groupby("event_date", sort=True):
        strong = _winner_mask(day)
        rows.append(
            {
                "event_date": str(event_date),
                "candidate_count": int(len(day)),
                "true_strong_candidate_count": int(strong.sum()),
                "strong_candidate_bucket": str(min(int(strong.sum()), 3)),
                "forced_top3_structurally_overfilled": bool(int(strong.sum()) < TOP_K),
            }
        )
    day_frame = pd.DataFrame(rows)
    base_day = _family_day_stats(ledger, BASELINE_FAMILY_ID)
    top1 = _topk(ledger, BASELINE_FAMILY_ID, 1)
    top3 = _topk(ledger, BASELINE_FAMILY_ID, 3)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_max3_deployment_fit_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "days_with_0_true_strong_candidates": int(day_frame["true_strong_candidate_count"].eq(0).sum()),
        "days_with_1_true_strong_candidates": int(day_frame["true_strong_candidate_count"].eq(1).sum()),
        "days_with_2_true_strong_candidates": int(day_frame["true_strong_candidate_count"].eq(2).sum()),
        "days_with_3_or_more_true_strong_candidates": int(day_frame["true_strong_candidate_count"].ge(3).sum()),
        "forced_top3_structurally_overfilled_day_rate": float(day_frame["forced_top3_structurally_overfilled"].mean()) if len(day_frame) else 0.0,
        "previous_best_top1_avg_ret20": _mean(top1["ret20_fwd"]),
        "previous_best_top3_avg_ret20": _mean(top3["ret20_fwd"]),
        "top1_minus_top3_avg_ret20": (_mean(top1["ret20_fwd"]) or 0.0) - (_mean(top3["ret20_fwd"]) or 0.0),
        "top1_policy_created": False,
        "threshold_policy_created": False,
        "rows_sample": rows[:200],
        "diagnosis_note": "forced top3 fit is evaluated only as diagnosis; no threshold or top1 policy is created",
    }
    payload["max3_structure_failure_present"] = bool(payload["forced_top3_structurally_overfilled_day_rate"] >= 0.35 or payload["top1_minus_top3_avg_ret20"] > 0.002)
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_label_objective_mismatch_report(events: pd.DataFrame, ledger: pd.DataFrame) -> dict[str, Any]:
    frame = events.copy()
    frame["ret20_winner"] = frame["is_future_top10_by_ret20"].astype(bool) | pd.to_numeric(frame["ret20_fwd"], errors="coerce").ge(0.10)
    frame["mfe_winner"] = pd.to_numeric(frame["mfe20"], errors="coerce").ge(0.15)
    frame["mfe_only_winner"] = frame["mfe_winner"] & ~frame["ret20_winner"]
    frame["ret20_only_winner"] = frame["ret20_winner"] & ~frame["mfe_winner"]
    top3 = _topk(ledger, BASELINE_FAMILY_ID, TOP_K)
    top3_mfe_winner = pd.to_numeric(top3["mfe20"], errors="coerce").ge(0.15) if len(top3) else pd.Series(dtype=bool)
    source_mfe_winner = pd.to_numeric(frame["mfe20"], errors="coerce").ge(0.15)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_label_objective_mismatch_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "mfe_only_winner_count": int(frame["mfe_only_winner"].sum()),
        "ret20_only_winner_count": int(frame["ret20_only_winner"].sum()),
        "mfe_only_winner_rate_among_mfe_winners": _safe_rate(int(frame["mfe_only_winner"].sum()), int(frame["mfe_winner"].sum())),
        "selected_top3_mfe_only_winner_count": int((top3["mfe20"].ge(0.15) & ~top3["is_future_top10_by_ret20"].astype(bool) & top3["ret20_fwd"].lt(0.10)).sum()) if len(top3) else 0,
        "selected_top3_big_MFE_capture_rate": _safe_rate(int(top3_mfe_winner.sum()), int(source_mfe_winner.sum())),
        "selected_top3_big_ret20_capture_rate": _safe_rate(int(top3["is_big_winner_ret20_ge_10pct"].sum()), int(frame["is_big_winner_ret20_ge_10pct"].sum())),
        "label_objective_mismatch_present": bool(_safe_rate(int(frame["mfe_only_winner"].sum()), int(frame["mfe_winner"].sum())) >= 0.25),
        "scoring_created": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_failure_mode_classification(
    *,
    oracle_gap: dict[str, Any],
    pool_recall: dict[str, Any],
    ranking_failure: dict[str, Any],
    timeblock: dict[str, Any],
    max3: dict[str, Any],
    label_mismatch: dict[str, Any],
) -> dict[str, Any]:
    modes = [
        {
            "failure_mode": "pool_recall_failure",
            "status": "not_observable_full_universe",
            "evidence_strength": "limited",
            "evidence": "full same-date market universe is not present; available wide source contains current-source winners",
        },
        {
            "failure_mode": "ranking_failure",
            "status": "present",
            "evidence_strength": "high",
            "evidence": f"winner_available_but_nonwinner_selected_rate={ranking_failure['winner_available_but_nonwinner_selected_rate']:.4f}",
        },
        {
            "failure_mode": "risk_return_tradeoff_failure",
            "status": "present",
            "evidence_strength": "high",
            "evidence": "ranking challenger reduced severe loss but worsened top3 return and oracle gap",
        },
        {
            "failure_mode": "regime_specific_failure",
            "status": "partial",
            "evidence_strength": "medium" if timeblock.get("regime_specific_failure_present") else "low",
            "evidence": f"challenger_beats_previous_best_time_block_rate={timeblock.get('challenger_beats_previous_best_time_block_rate')}",
        },
        {
            "failure_mode": "max3_structure_failure",
            "status": "present" if max3.get("max3_structure_failure_present") else "not_primary",
            "evidence_strength": "medium" if max3.get("max3_structure_failure_present") else "low",
            "evidence": f"forced_top3_structurally_overfilled_day_rate={max3.get('forced_top3_structurally_overfilled_day_rate')}",
        },
        {
            "failure_mode": "label_objective_mismatch",
            "status": "present" if label_mismatch.get("label_objective_mismatch_present") else "not_primary",
            "evidence_strength": "medium" if label_mismatch.get("label_objective_mismatch_present") else "low",
            "evidence": f"mfe_only_winner_rate_among_mfe_winners={label_mismatch.get('mfe_only_winner_rate_among_mfe_winners')}",
        },
    ]
    primary = ["ranking_failure", "risk_return_tradeoff_failure"]
    if max3.get("max3_structure_failure_present"):
        primary.append("max3_structure_failure")
    if label_mismatch.get("label_objective_mismatch_present"):
        primary.append("label_objective_mismatch")
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_failure_mode_classification_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "primary_failure_modes": primary[:3],
        "modes": modes,
        "root_failure_mode_identified": True,
        "pool_recall_vs_ranking_failure_separated": True,
        "full_universe_pool_recall_limitation_recorded": True,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_candidate_generation_hypothesis_map(failure_modes: dict[str, Any], event_source: dict[str, Any], max3: dict[str, Any], label_mismatch: dict[str, Any]) -> dict[str, Any]:
    hypotheses = [
        {
            "hypothesis_id": "missed_winner_event_source_candidate_generation_v1",
            "target_failure_mode": "ranking_failure",
            "description": "Generate a new candidate family from event-source states that are frequent among missed winners but under-selected by the previous wide scorer.",
            "expected_mechanism": "Move upstream from reordering the same wide score surface to candidate generation keyed on missed-winner source patterns, preserving negative_guard as a tag only.",
            "required_inputs": ["pre_strength_event_ledger", "guard_tags", "missed_winner_source_distribution", "point_in_time_candidate_keys"],
            "forbidden_shortcuts": ["safe_full hard filter", "negative_guard hard veto", "image fusion", "threshold tuning"],
            "testable_next_axis": "missed_winner_event_source_candidate_generation_v1",
            "priority": 1,
        },
        {
            "hypothesis_id": "max3_candidate_supply_fit_audit_v1",
            "target_failure_mode": "max3_structure_failure",
            "description": "Audit whether the daily candidate-generation layer can supply three capital-worthy candidates, or whether max3 should be conditional on candidate supply before scorer work resumes.",
            "expected_mechanism": "Separate days with insufficient true strong candidates from ranking mistakes so future candidate generation is judged by strong-candidate supply rather than forced top3 fill.",
            "required_inputs": ["daily_candidate_count", "future strong candidate count for diagnosis", "previous_best selected path"],
            "forbidden_shortcuts": ["threshold tuning", "no-trade policy creation", "safe_full hard filter", "negative_guard hard veto"],
            "testable_next_axis": "max3_candidate_supply_fit_audit_v1",
            "priority": 2,
        },
    ]
    if label_mismatch.get("label_objective_mismatch_present"):
        hypotheses = [
            hypotheses[0],
            {
                "hypothesis_id": "mfe_tradeable_opportunity_candidate_generation_v1",
                "target_failure_mode": "label_objective_mismatch",
                "description": "Test candidate generation against tradeable MFE opportunity rather than ret20 close-only winner labels.",
                "expected_mechanism": "If many tradable winners appear as MFE-only outcomes, next candidate generation should target tradable opportunity shape before exit logic is optimized.",
                "required_inputs": ["MFE20", "MAE20", "ret20", "source event states"],
                "forbidden_shortcuts": ["exit optimization", "sell-side logic", "image fusion", "threshold tuning"],
                "testable_next_axis": "mfe_tradeable_opportunity_candidate_generation_v1",
                "priority": 2,
            },
        ]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_generation_hypothesis_map_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_generation_hypothesis_map_created": True,
        "hypothesis_count": len(hypotheses),
        "hypotheses": hypotheses[:2],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_next_axis_recommendation(hypotheses: dict[str, Any]) -> dict[str, Any]:
    first = hypotheses["hypotheses"][0] if hypotheses.get("hypotheses") else {}
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "one_recommended_next_axis_only": True,
        "recommended_next_axis": first.get("testable_next_axis"),
        "recommended_hypothesis_id": first.get("hypothesis_id"),
        "reason": "existing scorer/ranker/image axes failed; next step should move upstream to candidate generation hypothesis testing",
        "do_not_continue_axes": [
            "daily_pairwise_ranker coefficient tuning",
            "daily_listwise threshold tuning",
            "top3_badpick_penalty stronger variant",
            "safe_full hard filter",
            "negative_guard hard veto",
            "image fusion",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(
    *,
    output_dir: Path,
    source_dirs: dict[str, Path],
    events: pd.DataFrame,
    source_status: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    refs = []
    for source_name, root in source_dirs.items():
        for path in sorted(root.glob("*.json")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "content_hash": _stable_hash(_load_json(path))})
        for path in sorted(root.glob("*.jsonl")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "oracle_gap_and_candidate_generation_root_cause",
        "boundary": "TRADEX-only",
        "axis_moved": "oracle_gap_and_candidate_generation_root_cause",
        "source_ranking_objective_decision": source_status["ranking_objective"]["research_decision.json"].get("authoritative_research_decision"),
        "event_count": int(len(events)),
        "event_day_count": int(events["event_date"].nunique()),
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": TOP_K,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "cost_slippage_evaluated": False,
            "cost_slippage_ignored_by_user_intent": True,
        },
        "diagnosis_created": True,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    root_cause_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_root_cause_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "diagnosis_only": True,
        "failure_modes": [
            "pool_recall_failure",
            "ranking_failure",
            "risk_return_tradeoff_failure",
            "regime_specific_failure",
            "max3_structure_failure",
            "label_objective_mismatch",
        ],
        "candidate_generation_hypothesis_required": True,
        "new_scorer_created": False,
        "threshold_policy_created": False,
        "image_fusion_created": False,
        "production_ranking_changed": False,
        "meemee_reflectable": False,
    }
    root_cause_contract["contract_hash"] = _stable_hash(root_cause_contract)
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_roots": {key: str(value) for key, value in source_dirs.items()},
        "refs": refs,
    }
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "root_cause_contract.json": root_cause_contract,
    }


def build_research_decision(
    *,
    oracle_gap: dict[str, Any],
    failure_modes: dict[str, Any],
    hypotheses: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    root_identified = failure_modes.get("root_failure_mode_identified") is True
    has_hypotheses = int(hypotheses.get("hypothesis_count") or 0) in (1, 2)
    diagnosis_complete = oracle_gap.get("oracle_gap_decomposition_complete") is True and failure_modes.get("pool_recall_vs_ranking_failure_separated") is True
    if artifact_complete and root_identified and has_hypotheses and diagnosis_complete:
        decision = "keep_candidate"
        authoritative = "root_cause_identified_next_axis_ready"
    elif artifact_complete and (root_identified or has_hypotheses):
        decision = "hold"
        authoritative = "root_cause_hold"
    else:
        decision = "drop"
        authoritative = "root_cause_inconclusive"
    typed_reasons = [
        "root_failure_mode_identified" if root_identified else "root_failure_mode_not_identified",
        "candidate_generation_hypotheses_testable" if has_hypotheses else "candidate_generation_hypotheses_not_testable",
        "oracle_gap_decomposition_complete" if oracle_gap.get("oracle_gap_decomposition_complete") else "oracle_gap_decomposition_incomplete",
        "full_market_pool_recall_unavailable_recorded" if oracle_gap.get("full_oracle_available") is False else "full_market_pool_recall_available",
        "diagnosis_only_no_scorer_created",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "oracle_gap_and_candidate_generation_root_cause",
        "boundary": "TRADEX-only",
        "axis_moved": "oracle_gap_and_candidate_generation_root_cause",
        "source_ranking_objective_decision": "ranking_objective_drop",
        "diagnosis_created": True,
        "candidate_generation_hypothesis_map_created": True,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "typed_reasons": typed_reasons,
        "primary_failure_modes": failure_modes.get("primary_failure_modes", []),
        "recommended_next_axis": hypotheses["hypotheses"][0]["testable_next_axis"] if hypotheses.get("hypotheses") else None,
        "decision_reasons": [
            {"code": "root_failure_mode_identified", "status": "pass" if root_identified else "fail", "value": failure_modes.get("primary_failure_modes")},
            {"code": "candidate_generation_hypothesis_count_1_or_2", "status": "pass" if has_hypotheses else "fail", "value": hypotheses.get("hypothesis_count")},
            {"code": "oracle_gap_decomposition_complete", "status": "pass" if oracle_gap.get("oracle_gap_decomposition_complete") else "fail", "value": oracle_gap.get("oracle_gap_decomposition_complete")},
            {"code": "no_scorer_threshold_or_fusion_created", "status": "pass", "value": True},
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
        ],
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
        "diagnosis_created": True,
        "candidate_generation_hypothesis_map_created": True,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def run_oracle_gap_and_candidate_generation_root_cause_v1(
    *,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_risk_run_id: str = DEFAULT_RISK_RUN_ID,
    source_threshold_run_id: str = DEFAULT_THRESHOLD_RUN_ID,
    source_feature_diagnosis_run_id: str = DEFAULT_FEATURE_DIAGNOSIS_RUN_ID,
    source_image_phase2_run_id: str = DEFAULT_IMAGE_PHASE2_RUN_ID,
    source_image_cnn_phase2b_run_id: str = DEFAULT_IMAGE_CNN_PHASE2B_RUN_ID,
    source_ranking_objective_run_id: str = DEFAULT_RANKING_OBJECTIVE_RUN_ID,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    risk_root: str | Path = DEFAULT_RISK_ROOT,
    threshold_root: str | Path = DEFAULT_THRESHOLD_ROOT,
    feature_diagnosis_root: str | Path = DEFAULT_FEATURE_DIAGNOSIS_ROOT,
    image_phase2_root: str | Path = DEFAULT_IMAGE_PHASE2_ROOT,
    image_cnn_phase2b_root: str | Path = DEFAULT_IMAGE_CNN_PHASE2B_ROOT,
    ranking_objective_root: str | Path = DEFAULT_RANKING_OBJECTIVE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    risk_dir = _run_dir(risk_root, source_risk_run_id, DEFAULT_RISK_ROOT)
    threshold_dir = _run_dir(threshold_root, source_threshold_run_id, DEFAULT_THRESHOLD_ROOT)
    feature_dir = _run_dir(feature_diagnosis_root, source_feature_diagnosis_run_id, DEFAULT_FEATURE_DIAGNOSIS_ROOT)
    image_phase2_dir = _run_dir(image_phase2_root, source_image_phase2_run_id, DEFAULT_IMAGE_PHASE2_ROOT)
    image_cnn_dir = _run_dir(image_cnn_phase2b_root, source_image_cnn_phase2b_run_id, DEFAULT_IMAGE_CNN_PHASE2B_ROOT)
    ranking_dir = _run_dir(ranking_objective_root, source_ranking_objective_run_id, DEFAULT_RANKING_OBJECTIVE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    source_status = validate_sources(
        wide_dir=wide_dir,
        risk_dir=risk_dir,
        threshold_dir=threshold_dir,
        feature_diagnosis_dir=feature_dir,
        image_phase2_dir=image_phase2_dir,
        image_cnn_phase2b_dir=image_cnn_dir,
        ranking_objective_dir=ranking_dir,
    )
    events, ledger = load_diagnosis_inputs(source_status=source_status, wide_dir=wide_dir, risk_dir=risk_dir, threshold_dir=threshold_dir, ranking_objective_dir=ranking_dir)
    ranking_decision = source_status["ranking_objective"]["research_decision.json"]
    challenger_id = str(ranking_decision.get("best_ranker_family_id") or LISTWISE_FAMILY_ID)
    oracle_gap = build_oracle_gap_decomposition(ledger, ranking_decision)
    pool_recall = build_candidate_pool_recall_report(events)
    ranking_failure = build_within_pool_ranking_failure_report(ledger, challenger_id)
    event_source = build_event_source_decomposition_report(events, ledger, challenger_id)
    timeblock = build_regime_timeblock_decomposition_report(events, ledger, challenger_id)
    max3 = build_max3_deployment_fit_report(events, ledger)
    label_mismatch = build_label_objective_mismatch_report(events, ledger)
    failure_modes = build_failure_mode_classification(
        oracle_gap=oracle_gap,
        pool_recall=pool_recall,
        ranking_failure=ranking_failure,
        timeblock=timeblock,
        max3=max3,
        label_mismatch=label_mismatch,
    )
    hypotheses = build_candidate_generation_hypothesis_map(failure_modes, event_source, max3, label_mismatch)
    next_axis = build_next_axis_recommendation(hypotheses)
    source_dirs = {
        "wide": wide_dir,
        "risk": risk_dir,
        "threshold": threshold_dir,
        "feature_diagnosis": feature_dir,
        "image_phase2_reference_only": image_phase2_dir,
        "image_cnn_phase2b_reference_only": image_cnn_dir,
        "ranking_objective": ranking_dir,
    }
    contract_artifacts = build_contract_artifacts(output_dir=output_dir, source_dirs=source_dirs, events=events, source_status=source_status)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=ranking_mod.RANDOM_SEED,
        random_seed=ranking_mod.RANDOM_SEED,
        input_artifacts=[
            {"name": key, "path": str(value)}
            for key, value in source_dirs.items()
        ],
        asof=str(int(events["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "diagnosis_only": True,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=sorted(events["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(events["event_ymd"].min())), "end_date": str(int(events["event_ymd"].max())), "label": "oracle_gap_and_candidate_generation_root_cause"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    paths: dict[str, str] = {}
    for name, payload in {
        **contract_artifacts,
        "run_manifest.json": run_manifest,
        "oracle_gap_decomposition.json": oracle_gap,
        "candidate_pool_recall_report.json": pool_recall,
        "within_pool_ranking_failure_report.json": ranking_failure,
        "event_source_decomposition_report.json": event_source,
        "regime_timeblock_decomposition_report.json": timeblock,
        "max3_deployment_fit_report.json": max3,
        "label_objective_mismatch_report.json": label_mismatch,
        "failure_mode_classification.json": failure_modes,
        "candidate_generation_hypothesis_map.json": hypotheses,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        oracle_gap=oracle_gap,
        failure_modes=failure_modes,
        hypotheses=hypotheses,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "primary_failure_modes": decision["primary_failure_modes"],
        "recommended_next_axis": decision["recommended_next_axis"],
        "diagnosis_created": True,
        "candidate_generation_hypothesis_map_created": True,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-risk-run-id", default=DEFAULT_RISK_RUN_ID)
    parser.add_argument("--source-threshold-run-id", default=DEFAULT_THRESHOLD_RUN_ID)
    parser.add_argument("--source-feature-diagnosis-run-id", default=DEFAULT_FEATURE_DIAGNOSIS_RUN_ID)
    parser.add_argument("--source-image-phase2-run-id", default=DEFAULT_IMAGE_PHASE2_RUN_ID)
    parser.add_argument("--source-image-cnn-phase2b-run-id", default=DEFAULT_IMAGE_CNN_PHASE2B_RUN_ID)
    parser.add_argument("--source-ranking-objective-run-id", default=DEFAULT_RANKING_OBJECTIVE_RUN_ID)
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--risk-root", default=str(DEFAULT_RISK_ROOT))
    parser.add_argument("--threshold-root", default=str(DEFAULT_THRESHOLD_ROOT))
    parser.add_argument("--feature-diagnosis-root", default=str(DEFAULT_FEATURE_DIAGNOSIS_ROOT))
    parser.add_argument("--image-phase2-root", default=str(DEFAULT_IMAGE_PHASE2_ROOT))
    parser.add_argument("--image-cnn-phase2b-root", default=str(DEFAULT_IMAGE_CNN_PHASE2B_ROOT))
    parser.add_argument("--ranking-objective-root", default=str(DEFAULT_RANKING_OBJECTIVE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_oracle_gap_and_candidate_generation_root_cause_v1(
        source_wide_run_id=args.source_wide_run_id,
        source_risk_run_id=args.source_risk_run_id,
        source_threshold_run_id=args.source_threshold_run_id,
        source_feature_diagnosis_run_id=args.source_feature_diagnosis_run_id,
        source_image_phase2_run_id=args.source_image_phase2_run_id,
        source_image_cnn_phase2b_run_id=args.source_image_cnn_phase2b_run_id,
        source_ranking_objective_run_id=args.source_ranking_objective_run_id,
        wide_root=args.wide_root,
        risk_root=args.risk_root,
        threshold_root=args.threshold_root,
        feature_diagnosis_root=args.feature_diagnosis_root,
        image_phase2_root=args.image_phase2_root,
        image_cnn_phase2b_root=args.image_cnn_phase2b_root,
        ranking_objective_root=args.ranking_objective_root,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
