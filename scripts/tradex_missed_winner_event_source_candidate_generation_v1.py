from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_oracle_gap_and_candidate_generation_root_cause_v1 as root_mod
from scripts import tradex_ranking_loss_or_topk_objective_repair_v1 as ranking_mod


AXIS_ID = "missed_winner_event_source_candidate_generation_v1"
SCHEMA_PREFIX = "tradex_missed_winner_event_source_candidate_generation_v1"
DEFAULT_ROOT_CAUSE_RUN_ID = "20260513T130000Z-oracle-gap-and-candidate-generation-root-cause-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_FEATURE_DIAGNOSIS_RUN_ID = "20260513T060000Z-wide-pool-winner-nonwinner-feature-diagnosis-v1"
DEFAULT_ROOT_CAUSE_ROOT = Path(r"G:\Tradex\oracle_gap_and_candidate_generation_root_cause_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_PATTERN_ROOT = Path(r"G:\Tradex\pre_strength_pattern_mining_v1")
DEFAULT_UPSIDE_ROOT = Path(r"G:\Tradex\upside_capture_missed_winner_diagnosis_v1")
DEFAULT_FEATURE_DIAGNOSIS_ROOT = Path(r"G:\Tradex\wide_pool_winner_nonwinner_feature_diagnosis_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\missed_winner_event_source_candidate_generation_v1")

TOP_K = 3
BASELINE_FAMILY_ID = root_mod.BASELINE_FAMILY_ID
LISTWISE_FAMILY_ID = root_mod.LISTWISE_FAMILY_ID
REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "event_source_contract.json",
    "missed_winner_source_decomposition.json",
    "selected_nonwinner_source_decomposition.json",
    "event_source_quality_leaderboard.json",
    "same_date_source_miss_report.json",
    "max3_source_structure_report.json",
    "time_block_source_stability.json",
    "source_failure_mode_classification.json",
    "candidate_generation_hypothesis_map.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
SOURCE_COLUMNS = (
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


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _median(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.median())


def validate_sources(
    *,
    root_cause_dir: Path,
    wide_dir: Path,
    pattern_dir: Path,
    upside_dir: Path,
    feature_diagnosis_dir: Path,
) -> dict[str, Any]:
    required_by_source = {
        "root_cause": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "source_artifact_refs.json",
            "candidate_generation_hypothesis_map.json",
            "oracle_gap_decomposition.json",
            "failure_mode_classification.json",
        ],
        "wide": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "source_artifact_refs.json"],
        "pattern": ["_ARTIFACT_COMPLETE.json"],
        "upside": ["_ARTIFACT_COMPLETE.json"],
        "feature_diagnosis": ["_ARTIFACT_COMPLETE.json", "candidate_feature_shortlist.json", "research_decision.json"],
    }
    dirs = {
        "root_cause": root_cause_dir,
        "wide": wide_dir,
        "pattern": pattern_dir,
        "upside": upside_dir,
        "feature_diagnosis": feature_diagnosis_dir,
    }
    status: dict[str, Any] = {}
    for source_name, names in required_by_source.items():
        root = dirs[source_name]
        missing = [name for name in names if not (root / name).exists()]
        if missing:
            raise FileNotFoundError(f"{source_name} source missing required artifacts: {missing} at {root}")
        complete = _load_json(root / "_ARTIFACT_COMPLETE.json")
        if complete.get("complete") is not True:
            raise RuntimeError(f"{source_name} source artifact is not complete")
        if complete.get("silent_fallback_used") is not False:
            raise RuntimeError(f"{source_name} source used silent fallback")
        status[source_name] = {"_ARTIFACT_COMPLETE.json": complete}

    root_decision = _load_json(root_cause_dir / "research_decision.json")
    if root_decision.get("authoritative_research_decision") != "root_cause_identified_next_axis_ready":
        raise RuntimeError("root cause source is not ready for missed-winner event-source axis")
    if root_decision.get("recommended_next_axis") != AXIS_ID:
        raise RuntimeError("root cause source does not recommend missed_winner_event_source_candidate_generation_v1")
    if root_decision.get("research_fallback_used") is not False:
        raise RuntimeError("root cause source used research fallback")

    wide_refs_payload = _load_json(wide_dir / "source_artifact_refs.json")
    wide_refs = wide_refs_payload.get("source_roots", {})
    root_refs = _load_json(root_cause_dir / "source_artifact_refs.json").get("source_roots", {})
    inferred_guard = wide_refs.get("guard") or wide_refs.get("source_guard") or wide_refs_payload.get("guard_artifact_root")
    inferred_risk = root_refs.get("risk") or str(ranking_mod.DEFAULT_RISK_ROOT / ranking_mod.DEFAULT_RISK_RUN_ID)
    inferred_threshold = root_refs.get("threshold") or str(ranking_mod.DEFAULT_THRESHOLD_ROOT / ranking_mod.DEFAULT_THRESHOLD_RUN_ID)
    inferred_ranking_objective = root_refs.get("ranking_objective") or str(ranking_mod.DEFAULT_OUTPUT_ROOT / ranking_mod.DEFAULT_RANKING_OBJECTIVE_RUN_ID)
    if not inferred_guard:
        raise RuntimeError("wide source_artifact_refs.json does not expose guard source root")

    status["root_cause"]["research_decision.json"] = root_decision
    status["root_cause"]["candidate_generation_hypothesis_map.json"] = _load_json(root_cause_dir / "candidate_generation_hypothesis_map.json")
    status["root_cause"]["oracle_gap_decomposition.json"] = _load_json(root_cause_dir / "oracle_gap_decomposition.json")
    status["root_cause"]["failure_mode_classification.json"] = _load_json(root_cause_dir / "failure_mode_classification.json")
    status["wide"]["research_decision.json"] = _load_json(wide_dir / "research_decision.json")
    status["wide"]["source_artifact_refs.json"] = _load_json(wide_dir / "source_artifact_refs.json")
    status["feature_diagnosis"]["research_decision.json"] = _load_json(feature_diagnosis_dir / "research_decision.json")
    status["feature_diagnosis"]["candidate_feature_shortlist.json"] = _load_json(feature_diagnosis_dir / "candidate_feature_shortlist.json")
    status["pattern_dir"] = pattern_dir
    status["guard_dir"] = Path(inferred_guard)
    status["upside_dir"] = upside_dir
    status["risk_dir"] = Path(inferred_risk)
    status["threshold_dir"] = Path(inferred_threshold)
    status["ranking_objective_dir"] = Path(inferred_ranking_objective)
    return status


def load_diagnosis_inputs(*, source_status: dict[str, Any], wide_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    events, ledger = root_mod.load_diagnosis_inputs(
        source_status={
            "pattern_dir": source_status["pattern_dir"],
            "guard_dir": source_status["guard_dir"],
            "upside_dir": source_status["upside_dir"],
            "ranking_objective": {
                "research_decision.json": {
                    "authoritative_research_decision": "ranking_objective_drop",
                    "best_ranker_family_id": LISTWISE_FAMILY_ID,
                }
            },
        },
        wide_dir=wide_dir,
        risk_dir=source_status["risk_dir"],
        threshold_dir=source_status["threshold_dir"],
        ranking_objective_dir=source_status["ranking_objective_dir"],
    )
    return prepare_event_source_frame(events, ledger), ledger


def _winner_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["is_future_top10_by_ret20"].astype(bool)
        | pd.to_numeric(frame["ret20_fwd"], errors="coerce").ge(0.10)
        | pd.to_numeric(frame["mfe20"], errors="coerce").ge(0.15)
    )


def _selected_nonwinner_mask(frame: pd.DataFrame) -> pd.Series:
    return ~_winner_mask(frame) & (
        pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0)
        | pd.to_numeric(frame.get("ret20_rank_pct_by_date", pd.Series(index=frame.index, dtype=float)), errors="coerce").gt(0.50)
        | frame["severe_loss20"].astype(bool)
    )


def _baseline_selection_flags(ledger: pd.DataFrame) -> pd.DataFrame:
    scope = ledger[ledger["ranker_family_id"].eq(BASELINE_FAMILY_ID)].copy()
    scope["selection_rank"] = pd.to_numeric(scope["selection_rank"], errors="coerce")
    out = scope[["event_date", "code", "selection_rank"]].copy()
    out["selected_by_previous_best_top3"] = out["selection_rank"].le(TOP_K)
    out["selected_by_previous_best_top5"] = out["selection_rank"].le(5)
    return out


def _source_value(row: pd.Series, columns: tuple[str, ...]) -> str:
    parts = []
    for column in columns:
        if column in row:
            value = row[column]
            if pd.notna(value):
                parts.append(f"{column}={value}")
    return "|".join(parts) if parts else "source_unknown"


def prepare_event_source_frame(events: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    frame["event_date"] = frame["event_date"].astype(str).str.slice(0, 10)
    frame["code"] = frame["code"].astype(str)
    selected = _baseline_selection_flags(ledger)
    frame = frame.merge(selected, on=["event_date", "code"], how="left")
    frame["selection_rank"] = pd.to_numeric(frame["selection_rank"], errors="coerce")
    frame["selected_by_previous_best_top3"] = frame["selected_by_previous_best_top3"].eq(True)
    frame["selected_by_previous_best_top5"] = frame["selected_by_previous_best_top5"].eq(True)
    frame["future_winner"] = _winner_mask(frame)
    frame["selected_winner"] = frame["selected_by_previous_best_top3"] & frame["future_winner"]
    frame["selected_nonwinner"] = frame["selected_by_previous_best_top3"] & _selected_nonwinner_mask(frame)
    frame["selected_severe_loser"] = frame["selected_by_previous_best_top3"] & frame["severe_loss20"].astype(bool)
    selected_winner_dates = set(frame.loc[frame["selected_winner"], "event_date"].unique().tolist())
    frame["missed_winner"] = frame["future_winner"] & ~frame["event_date"].isin(selected_winner_dates)
    frame["future_top3_by_date"] = pd.to_numeric(frame.get("ret20_rank_by_date"), errors="coerce").le(3)
    frame["time_block"] = frame["event_date"].astype(str).str.slice(0, 4)
    for column in SOURCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = "missing_source_field"
    frame["event_source"] = frame.apply(
        lambda row: _source_value(
            row,
            (
                "pre_ret20_state",
                "pre_ma20_path_state",
                "pre_ma60_context_state",
                "weekly_prior_state",
                "monthly_prior_state",
                "negative_guard_match",
            ),
        ),
        axis=1,
    )
    frame["source_family"] = frame.apply(
        lambda row: _source_value(
            row,
            ("pre_ma20_path_state", "pre_ma60_context_state", "weekly_prior_state", "negative_guard_match"),
        ),
        axis=1,
    )
    frame["setup_family"] = frame.apply(
        lambda row: _source_value(row, ("pre_ret5_state", "pre_wick_warning_state", "pre_volume_state", "pre_compression_state")),
        axis=1,
    )
    return frame


def _source_group_rows(frame: pd.DataFrame, source_column: str = "source_family") -> list[dict[str, Any]]:
    rows = []
    total_missed = int(frame["missed_winner"].sum())
    for source, group in frame.groupby(source_column, dropna=False, sort=True):
        future_winners = group["future_winner"].astype(bool)
        missed = group["missed_winner"].astype(bool)
        selected = group["selected_by_previous_best_top3"].astype(bool)
        selected_nonwinner = group["selected_nonwinner"].astype(bool)
        severe = group["severe_loss20"].astype(bool)
        rows.append(
            {
                "event_source": str(source),
                "sample_count": int(len(group)),
                "day_count": int(group["event_date"].nunique()),
                "missed_winner_count": int(missed.sum()),
                "missed_winner_rate": _safe_rate(int(missed.sum()), int(future_winners.sum())),
                "missed_winner_share": _safe_rate(int(missed.sum()), total_missed),
                "future_winner_count": int(future_winners.sum()),
                "future_winner_rate": float(future_winners.mean()) if len(group) else 0.0,
                "future_top10_count": int(group["is_future_top10_by_ret20"].astype(bool).sum()),
                "big_winner_ret20_ge_10_count": int(group["is_big_winner_ret20_ge_10pct"].astype(bool).sum()),
                "big_winner_MFE20_ge_15_count": int(group["is_big_winner_MFE20_ge_15pct"].astype(bool).sum()),
                "selected_top3_count": int(selected.sum()),
                "selected_capture_count": int((selected & future_winners).sum()),
                "selected_capture_rate_among_source_winners": _safe_rate(int((selected & future_winners).sum()), int(future_winners.sum())),
                "selected_nonwinner_count": int(selected_nonwinner.sum()),
                "selected_nonwinner_rate": _safe_rate(int(selected_nonwinner.sum()), int(selected.sum())),
                "severe_loser_count": int(severe.sum()),
                "severe_loss_rate20": float(severe.mean()) if len(group) else 0.0,
                "avg_ret20": _mean(group["ret20_fwd"]),
                "median_ret20": _median(group["ret20_fwd"]),
                "win_rate20": float(pd.to_numeric(group["ret20_fwd"], errors="coerce").gt(0.0).mean()) if len(group) else 0.0,
                "avg_MFE20": _mean(group["mfe20"]),
                "avg_MAE20": _mean(group["mae20"]),
                "MFE_to_MAE_profile": (_mean(group["mfe20"]) or 0.0) / abs(_mean(group["mae20"]) or 1.0),
                "negative_guard_rate": float(group["negative_guard_match"].astype(bool).mean()) if len(group) else 0.0,
                "safe_full_rate": float(group["guard_safe_full"].astype(bool).mean()) if len(group) else 0.0,
            }
        )
    rows.sort(key=lambda row: (row["missed_winner_count"], row["future_winner_rate"], row["avg_ret20"] or 0.0), reverse=True)
    return rows


def build_missed_winner_source_decomposition(frame: pd.DataFrame) -> dict[str, Any]:
    rows = _source_group_rows(frame)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_missed_winner_source_decomposition_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "missed_winner_total_count": int(frame["missed_winner"].sum()),
        "future_winner_total_count": int(frame["future_winner"].sum()),
        "event_source_count": len(rows),
        "rows": rows,
        "top_missed_winner_sources": rows[:20],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_selected_nonwinner_source_decomposition(frame: pd.DataFrame) -> dict[str, Any]:
    rows = _source_group_rows(frame)
    rows.sort(key=lambda row: (row["selected_nonwinner_count"], row["severe_loser_count"], row["selected_nonwinner_rate"]), reverse=True)
    selected = frame[frame["selected_by_previous_best_top3"]]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_selected_nonwinner_source_decomposition_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "selected_top3_count": int(len(selected)),
        "selected_nonwinner_count": int(frame["selected_nonwinner"].sum()),
        "selected_severe_loser_count": int(frame["selected_severe_loser"].sum()),
        "selected_nonwinner_rate": _safe_rate(int(frame["selected_nonwinner"].sum()), int(len(selected))),
        "selected_severe_loser_rate": _safe_rate(int(frame["selected_severe_loser"].sum()), int(len(selected))),
        "MAE20_profile_by_event_source": [
            {"event_source": row["event_source"], "avg_MAE20": row["avg_MAE20"], "severe_loss_rate20": row["severe_loss_rate20"]}
            for row in rows[:50]
        ],
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_event_source_quality_leaderboard(frame: pd.DataFrame) -> dict[str, Any]:
    overall_severe = float(frame["severe_loss20"].astype(bool).mean()) if len(frame) else 0.0
    rows = _source_group_rows(frame)
    for row in rows:
        row["quality_score_diagnostic_only"] = (
            row["future_winner_rate"]
            + max(row["avg_ret20"] or 0.0, -0.10)
            + 0.25 * row["selected_capture_rate_among_source_winners"]
            - max(row["severe_loss_rate20"] - overall_severe, 0.0)
        )
        row["diagnostic_only_not_score_input"] = True
    rows.sort(key=lambda row: row["quality_score_diagnostic_only"], reverse=True)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_event_source_quality_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "overall_severe_loss_rate20": overall_severe,
        "diagnostic_only_quality_score_created": True,
        "candidate_scoring_created": False,
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_same_date_source_miss_report(frame: pd.DataFrame) -> dict[str, Any]:
    rows = []
    winner_days = 0
    mismatch_days = 0
    under_ranked_days = 0
    for event_date, day in frame.groupby("event_date", sort=True):
        winners = day[day["future_winner"]]
        selected_nonwinners = day[day["selected_nonwinner"]]
        if winners.empty:
            continue
        winner_days += 1
        selected_winners = day[day["selected_winner"]]
        best_winner_rank = pd.to_numeric(winners["selection_rank"], errors="coerce").min()
        source_mismatch = bool(
            not selected_nonwinners.empty
            and set(winners["source_family"].astype(str).tolist()).isdisjoint(set(selected_nonwinners["source_family"].astype(str).tolist()))
        )
        winner_under_ranked = bool(pd.notna(best_winner_rank) and best_winner_rank > TOP_K)
        mismatch_days += int(source_mismatch)
        under_ranked_days += int(winner_under_ranked)
        if len(rows) < 500:
            rows.append(
                {
                    "event_date": str(event_date),
                    "winner_count": int(len(winners)),
                    "selected_winner_count": int(len(selected_winners)),
                    "selected_nonwinner_count": int(len(selected_nonwinners)),
                    "winner_sources": sorted(set(winners["source_family"].astype(str).tolist())),
                    "selected_nonwinner_sources": sorted(set(selected_nonwinners["source_family"].astype(str).tolist())),
                    "source_mismatch_explains_miss": source_mismatch,
                    "winner_source_present_but_under_ranked": winner_under_ranked,
                    "best_winner_selection_rank": float(best_winner_rank) if pd.notna(best_winner_rank) else None,
                }
            )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_same_date_source_miss_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "winner_available_day_count": winner_days,
        "source_mismatch_explains_miss_day_count": mismatch_days,
        "source_mismatch_explains_miss_rate": _safe_rate(mismatch_days, winner_days),
        "winner_source_present_but_under_ranked_day_count": under_ranked_days,
        "winner_source_present_but_under_ranked_rate": _safe_rate(under_ranked_days, winner_days),
        "same_date_rows_sample": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_max3_source_structure_report(frame: pd.DataFrame) -> dict[str, Any]:
    rows = []
    overfill_by_mix: dict[str, dict[str, Any]] = {}
    for event_date, day in frame.groupby("event_date", sort=True):
        strong = day[day["future_winner"]]
        selected = day[day["selected_by_previous_best_top3"]]
        source_mix = "+".join(sorted(set(selected["source_family"].astype(str).tolist()))) or "none"
        overfilled = int(len(strong)) < TOP_K
        rec = overfill_by_mix.setdefault(source_mix, {"source_mix": source_mix, "day_count": 0, "overfill_day_count": 0, "avg_selected_ret20_values": []})
        rec["day_count"] += 1
        rec["overfill_day_count"] += int(overfilled)
        rec["avg_selected_ret20_values"].append(_mean(selected["ret20_fwd"]) or 0.0)
        rows.append(
            {
                "event_date": str(event_date),
                "strong_source_count": int(strong["source_family"].nunique()),
                "strong_candidate_count": int(len(strong)),
                "selected_source_count": int(selected["source_family"].nunique()),
                "forced_top3_overfilled": bool(overfilled),
                "winner_source_mix": sorted(set(strong["source_family"].astype(str).tolist())),
                "selected_source_mix": sorted(set(selected["source_family"].astype(str).tolist())),
            }
        )
    mix_rows = []
    for rec in overfill_by_mix.values():
        mix_rows.append(
            {
                "source_mix": rec["source_mix"],
                "day_count": rec["day_count"],
                "overfill_day_count": rec["overfill_day_count"],
                "overfill_day_rate": _safe_rate(rec["overfill_day_count"], rec["day_count"]),
                "avg_selected_top3_ret20": float(np.mean(rec["avg_selected_ret20_values"])) if rec["avg_selected_ret20_values"] else None,
            }
        )
    mix_rows.sort(key=lambda row: (row["overfill_day_count"], -1.0 * (row["avg_selected_top3_ret20"] or 0.0)), reverse=True)
    day_frame = pd.DataFrame(rows)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_max3_source_structure_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "days_with_1_strong_source_only": int(day_frame["strong_source_count"].eq(1).sum()) if len(day_frame) else 0,
        "days_with_2_strong_sources": int(day_frame["strong_source_count"].eq(2).sum()) if len(day_frame) else 0,
        "days_with_3_or_more_strong_sources": int(day_frame["strong_source_count"].ge(3).sum()) if len(day_frame) else 0,
        "forced_top3_overfill_day_count": int(day_frame["forced_top3_overfilled"].sum()) if len(day_frame) else 0,
        "forced_top3_overfill_day_rate": float(day_frame["forced_top3_overfilled"].mean()) if len(day_frame) else 0.0,
        "source_mix_overfill_rows": mix_rows[:50],
        "rows_sample": rows[:500],
        "threshold_policy_created": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_time_block_source_stability(frame: pd.DataFrame) -> dict[str, Any]:
    rows = []
    source_rows = _source_group_rows(frame)
    top_sources = {row["event_source"] for row in source_rows[:25]}
    for (source, block), group in frame[frame["source_family"].isin(top_sources)].groupby(["source_family", "time_block"], sort=True):
        rows.append(
            {
                "event_source": str(source),
                "time_block": str(block),
                "sample_count": int(len(group)),
                "future_winner_rate": float(group["future_winner"].mean()) if len(group) else 0.0,
                "missed_winner_count": int(group["missed_winner"].sum()),
                "avg_ret20": _mean(group["ret20_fwd"]),
                "severe_loss_rate20": float(group["severe_loss20"].astype(bool).mean()) if len(group) else 0.0,
            }
        )
    stability_rows = []
    for source, group in pd.DataFrame(rows).groupby("event_source", sort=True) if rows else []:
        valid = group[group["sample_count"].ge(5)]
        winner_positive_rate = float(valid["future_winner_rate"].gt(0.0).mean()) if len(valid) else 0.0
        ret_positive_rate = float(pd.to_numeric(valid["avg_ret20"], errors="coerce").gt(0.0).mean()) if len(valid) else 0.0
        stability_rows.append(
            {
                "event_source": str(source),
                "time_block_count": int(len(valid)),
                "winner_presence_stability": winner_positive_rate,
                "ret20_positive_block_rate": ret_positive_rate,
                "passes_time_block_stability": bool(len(valid) >= 2 and winner_positive_rate >= 0.50),
            }
        )
    stability_rows.sort(key=lambda row: (row["passes_time_block_stability"], row["winner_presence_stability"], row["time_block_count"]), reverse=True)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_time_block_source_stability_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "source_stability_rows": stability_rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_failure_mode_classification(
    *,
    missed: dict[str, Any],
    quality: dict[str, Any],
    same_date: dict[str, Any],
    max3: dict[str, Any],
    stability: dict[str, Any],
) -> dict[str, Any]:
    quality_by_source = {row["event_source"]: row for row in quality["rows"]}
    stability_by_source = {row["event_source"]: row for row in stability.get("source_stability_rows", [])}
    rows = []
    for row in missed["rows"]:
        source = row["event_source"]
        quality_row = quality_by_source.get(source, row)
        stability_row = stability_by_source.get(source, {})
        modes = []
        if row["missed_winner_count"] >= 5 and row["selected_capture_rate_among_source_winners"] < 0.40:
            modes.append("source_under_ranked")
        if row["future_winner_count"] >= 5 and row["severe_loss_rate20"] >= quality.get("overall_severe_loss_rate20", 0.0) + 0.03:
            modes.append("source_over_noisy")
        if max3.get("forced_top3_overfill_day_rate", 0.0) >= 0.35 and row["missed_winner_count"] >= 5:
            modes.append("source_not_selected_due_to_max3_overfill")
        if stability_row and not stability_row.get("passes_time_block_stability"):
            modes.append("source_regime_specific")
        if (quality_row.get("big_winner_MFE20_ge_15_count") or 0) > (quality_row.get("big_winner_ret20_ge_10_count") or 0) * 1.25:
            modes.append("source_label_mismatch")
        rows.append(
            {
                "event_source": source,
                "sample_count": row["sample_count"],
                "missed_winner_count": row["missed_winner_count"],
                "future_winner_rate": row["future_winner_rate"],
                "severe_loss_rate20": row["severe_loss_rate20"],
                "source_failure_modes": modes or ["source_not_primary"],
                "primary_source_failure_mode": modes[0] if modes else "source_not_primary",
                "same_date_under_ranked_supported": bool(same_date.get("winner_source_present_but_under_ranked_rate", 0.0) > 0.20),
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_failure_mode_classification_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "classified_source_count": len(rows),
        "source_under_ranked_count": int(sum("source_under_ranked" in row["source_failure_modes"] for row in rows)),
        "source_over_noisy_count": int(sum("source_over_noisy" in row["source_failure_modes"] for row in rows)),
        "source_not_selected_due_to_max3_overfill_count": int(sum("source_not_selected_due_to_max3_overfill" in row["source_failure_modes"] for row in rows)),
        "source_regime_specific_count": int(sum("source_regime_specific" in row["source_failure_modes"] for row in rows)),
        "source_label_mismatch_count": int(sum("source_label_mismatch" in row["source_failure_modes"] for row in rows)),
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_candidate_generation_hypothesis_map(
    *,
    missed: dict[str, Any],
    quality: dict[str, Any],
    same_date: dict[str, Any],
    stability: dict[str, Any],
    source_failure: dict[str, Any],
) -> dict[str, Any]:
    overall_severe = quality.get("overall_severe_loss_rate20", 0.0)
    stability_by_source = {row["event_source"]: row for row in stability.get("source_stability_rows", [])}
    failure_by_source = {row["event_source"]: row for row in source_failure.get("rows", [])}
    candidate_rows = []
    for row in quality["rows"]:
        source = row["event_source"]
        failure_modes = failure_by_source.get(source, {}).get("source_failure_modes", [])
        if row["sample_count"] < 20 or row["missed_winner_count"] < 3:
            continue
        if row["future_winner_rate"] <= 0.0:
            continue
        if row["severe_loss_rate20"] > overall_severe + 0.12:
            continue
        if "source_under_ranked" not in failure_modes and "source_not_selected_due_to_max3_overfill" not in failure_modes:
            continue
        candidate_rows.append(
            {
                "row": row,
                "stability": stability_by_source.get(source, {}),
                "failure_modes": failure_modes,
                "priority_score": row["missed_winner_share"] + row["future_winner_rate"] - max(row["severe_loss_rate20"] - overall_severe, 0.0),
            }
        )
    candidate_rows.sort(key=lambda item: item["priority_score"], reverse=True)
    hypotheses = []
    for idx, item in enumerate(candidate_rows[:2], start=1):
        row = item["row"]
        failure_modes = item["failure_modes"]
        target_failure = "source_under_ranked" if "source_under_ranked" in failure_modes else "source_not_selected_due_to_max3_overfill"
        hypotheses.append(
            {
                "hypothesis_id": f"source_specific_candidate_generation_v{idx}",
                "target_failure_mode": target_failure,
                "source_family": row["event_source"],
                "description": "Create or promote candidates from this source family before ranking.",
                "expected_mechanism": "This source contains missed winners that previous_best under-ranks while keeping severe loss within the diagnosed source-risk bound.",
                "required_inputs": [
                    "pre_strength_event_source_fields",
                    "guard_tags_as_features_only",
                    "wide_pool_candidate_key",
                    "point_in_time_event_date",
                ],
                "forbidden_shortcuts": [
                    "safe_full hard filter",
                    "negative_guard hard veto",
                    "threshold tuning",
                    "image fusion",
                ],
                "testable_next_axis": "source_specific_candidate_generation_validation_v1",
                "priority": idx,
                "evidence": {
                    "sample_count": row["sample_count"],
                    "missed_winner_count": row["missed_winner_count"],
                    "future_winner_rate": row["future_winner_rate"],
                    "severe_loss_rate20": row["severe_loss_rate20"],
                    "same_date_under_ranked_rate": same_date.get("winner_source_present_but_under_ranked_rate"),
                    "time_block_stability": item["stability"].get("winner_presence_stability"),
                },
            }
        )
    if not hypotheses and quality["rows"]:
        row = quality["rows"][0]
        hypotheses.append(
            {
                "hypothesis_id": "source_specific_candidate_generation_requires_validation_v1",
                "target_failure_mode": "source_under_ranked",
                "source_family": row["event_source"],
                "description": "Top source has missed winners but does not pass all source-quality gates; validate with stricter point-in-time candidate-generation coverage before scorer work.",
                "expected_mechanism": "The source may explain missed winners, but severe-loss/sample/stability evidence is insufficient for direct challenger creation.",
                "required_inputs": ["additional point-in-time source coverage", "source-specific candidate ledger"],
                "forbidden_shortcuts": ["safe_full hard filter", "negative_guard hard veto", "threshold tuning", "image fusion"],
                "testable_next_axis": "source_specific_candidate_generation_validation_v1",
                "priority": 1,
                "evidence": {
                    "sample_count": row["sample_count"],
                    "missed_winner_count": row["missed_winner_count"],
                    "future_winner_rate": row["future_winner_rate"],
                    "severe_loss_rate20": row["severe_loss_rate20"],
                },
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_candidate_generation_hypothesis_map_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_generation_hypothesis_map_created": True,
        "hypothesis_count": len(hypotheses[:2]),
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
        "recommended_source_family": first.get("source_family"),
        "reason": "event-source diagnosis must be converted into a source-specific candidate-generation validation before scorer work resumes",
        "do_not_continue_axes": [
            "previous_best coefficient tuning",
            "daily_listwise re-tuning",
            "threshold/no-trade",
            "safe_full hard filter",
            "negative_guard hard veto",
            "image fusion",
            "CNN/ResNet deepening",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(*, source_dirs: dict[str, Path], frame: pd.DataFrame, source_status: dict[str, Any]) -> dict[str, dict[str, Any]]:
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
        "research_phase": "missed_winner_event_source_candidate_generation",
        "boundary": "TRADEX-only",
        "axis_moved": "missed_winner_event_source_candidate_generation",
        "source_root_cause_decision": source_status["root_cause"]["research_decision.json"].get("authoritative_research_decision"),
        "event_count": int(len(frame)),
        "event_day_count": int(frame["event_date"].nunique()),
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
        "event_source_decomposition_created": True,
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
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    event_source_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_event_source_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "diagnosis_only": True,
        "event_source_fields": list(SOURCE_COLUMNS),
        "event_source_key": "source_family",
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
        "candidate_generation_hypothesis_required": True,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "image_fusion_created": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
    }
    event_source_contract["contract_hash"] = _stable_hash(event_source_contract)
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
        "event_source_contract.json": event_source_contract,
    }


def build_research_decision(
    *,
    missed: dict[str, Any],
    same_date: dict[str, Any],
    hypotheses: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    has_missed_sources = int(missed.get("missed_winner_total_count") or 0) > 0 and int(missed.get("event_source_count") or 0) > 0
    has_hypotheses = int(hypotheses.get("hypothesis_count") or 0) in (1, 2)
    same_date_supported = same_date.get("winner_source_present_but_under_ranked_rate", 0.0) > 0.0 or same_date.get("source_mismatch_explains_miss_rate", 0.0) > 0.0
    first_hypothesis = hypotheses["hypotheses"][0] if hypotheses.get("hypotheses") else {}
    strict_source_ready = bool(first_hypothesis and first_hypothesis.get("hypothesis_id") != "source_specific_candidate_generation_requires_validation_v1")
    if artifact_complete and has_missed_sources and has_hypotheses and same_date_supported and strict_source_ready:
        decision = "keep_candidate"
        authoritative = "missed_winner_source_hypothesis_ready"
    elif artifact_complete and has_missed_sources and has_hypotheses:
        decision = "hold"
        authoritative = "missed_winner_source_hold"
    else:
        decision = "drop"
        authoritative = "missed_winner_source_inconclusive"
    typed_reasons = [
        "missed_winner_source_families_identified" if has_missed_sources else "missed_winner_sources_not_identified",
        "candidate_generation_hypotheses_testable" if has_hypotheses else "candidate_generation_hypotheses_not_testable",
        "same_date_source_miss_supported" if same_date_supported else "same_date_source_miss_not_supported",
        "diagnosis_only_no_scorer_created",
        "artifact_complete" if artifact_complete else "artifact_incomplete",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "missed_winner_event_source_candidate_generation",
        "boundary": "TRADEX-only",
        "axis_moved": "missed_winner_event_source_candidate_generation",
        "source_root_cause_decision": "root_cause_identified_next_axis_ready",
        "diagnosis_created": True,
        "event_source_decomposition_created": True,
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
        "recommended_next_axis": first_hypothesis.get("testable_next_axis"),
        "recommended_source_family": first_hypothesis.get("source_family"),
        "decision_reasons": [
            {"code": "missed_winner_source_families_identified", "status": "pass" if has_missed_sources else "fail", "value": missed.get("missed_winner_total_count")},
            {"code": "candidate_generation_hypothesis_count_1_or_2", "status": "pass" if has_hypotheses else "fail", "value": hypotheses.get("hypothesis_count")},
            {"code": "same_date_source_miss_supported", "status": "pass" if same_date_supported else "fail", "value": same_date.get("winner_source_present_but_under_ranked_rate")},
            {"code": "no_scorer_threshold_image_or_fusion_created", "status": "pass", "value": True},
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
        "event_source_decomposition_created": True,
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


def run_missed_winner_event_source_candidate_generation_v1(
    *,
    source_root_cause_run_id: str = DEFAULT_ROOT_CAUSE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    source_feature_diagnosis_run_id: str = DEFAULT_FEATURE_DIAGNOSIS_RUN_ID,
    root_cause_root: str | Path = DEFAULT_ROOT_CAUSE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    feature_diagnosis_root: str | Path = DEFAULT_FEATURE_DIAGNOSIS_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    root_cause_dir = _run_dir(root_cause_root, source_root_cause_run_id, DEFAULT_ROOT_CAUSE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    upside_dir = _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT)
    feature_dir = _run_dir(feature_diagnosis_root, source_feature_diagnosis_run_id, DEFAULT_FEATURE_DIAGNOSIS_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    source_status = validate_sources(
        root_cause_dir=root_cause_dir,
        wide_dir=wide_dir,
        pattern_dir=pattern_dir,
        upside_dir=upside_dir,
        feature_diagnosis_dir=feature_dir,
    )
    frame, ledger = load_diagnosis_inputs(source_status=source_status, wide_dir=wide_dir)
    missed = build_missed_winner_source_decomposition(frame)
    selected_nonwinner = build_selected_nonwinner_source_decomposition(frame)
    quality = build_event_source_quality_leaderboard(frame)
    same_date = build_same_date_source_miss_report(frame)
    max3 = build_max3_source_structure_report(frame)
    stability = build_time_block_source_stability(frame)
    source_failure = build_source_failure_mode_classification(
        missed=missed,
        quality=quality,
        same_date=same_date,
        max3=max3,
        stability=stability,
    )
    hypotheses = build_candidate_generation_hypothesis_map(
        missed=missed,
        quality=quality,
        same_date=same_date,
        stability=stability,
        source_failure=source_failure,
    )
    next_axis = build_next_axis_recommendation(hypotheses)
    source_dirs = {
        "root_cause": root_cause_dir,
        "wide": wide_dir,
        "pattern": pattern_dir,
        "upside": upside_dir,
        "feature_diagnosis": feature_dir,
        "risk_reference_from_root_cause": source_status["risk_dir"],
        "threshold_reference_from_root_cause": source_status["threshold_dir"],
        "ranking_objective_reference_from_root_cause": source_status["ranking_objective_dir"],
    }
    contract_artifacts = build_contract_artifacts(source_dirs=source_dirs, frame=frame, source_status=source_status)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=ranking_mod.RANDOM_SEED,
        random_seed=ranking_mod.RANDOM_SEED,
        input_artifacts=[{"name": key, "path": str(value)} for key, value in source_dirs.items()],
        asof=str(int(frame["event_ymd"].max())),
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
        universe=sorted(frame["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(frame["event_ymd"].min())), "end_date": str(int(frame["event_ymd"].max())), "label": "missed_winner_event_source_candidate_generation"},
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
        "missed_winner_source_decomposition.json": missed,
        "selected_nonwinner_source_decomposition.json": selected_nonwinner,
        "event_source_quality_leaderboard.json": quality,
        "same_date_source_miss_report.json": same_date,
        "max3_source_structure_report.json": max3,
        "time_block_source_stability.json": stability,
        "source_failure_mode_classification.json": source_failure,
        "candidate_generation_hypothesis_map.json": hypotheses,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        missed=missed,
        same_date=same_date,
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
        "recommended_next_axis": decision["recommended_next_axis"],
        "recommended_source_family": decision["recommended_source_family"],
        "diagnosis_created": True,
        "event_source_decomposition_created": True,
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
    parser.add_argument("--source-root-cause-run-id", default=DEFAULT_ROOT_CAUSE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--source-feature-diagnosis-run-id", default=DEFAULT_FEATURE_DIAGNOSIS_RUN_ID)
    parser.add_argument("--root-cause-root", default=str(DEFAULT_ROOT_CAUSE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--feature-diagnosis-root", default=str(DEFAULT_FEATURE_DIAGNOSIS_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_missed_winner_event_source_candidate_generation_v1(
        source_root_cause_run_id=args.source_root_cause_run_id,
        source_wide_run_id=args.source_wide_run_id,
        source_pattern_run_id=args.source_pattern_run_id,
        source_upside_run_id=args.source_upside_run_id,
        source_feature_diagnosis_run_id=args.source_feature_diagnosis_run_id,
        root_cause_root=args.root_cause_root,
        wide_root=args.wide_root,
        pattern_root=args.pattern_root,
        upside_root=args.upside_root,
        feature_diagnosis_root=args.feature_diagnosis_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
