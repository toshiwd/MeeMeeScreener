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
from scripts import tradex_source_specific_candidate_generation_validation_v1 as validation_mod
from scripts import tradex_ranking_loss_or_topk_objective_repair_v1 as ranking_mod


AXIS_ID = "source_specific_timeblock_applicability_audit_v1"
SCHEMA_PREFIX = "tradex_source_specific_timeblock_applicability_audit_v1"
DEFAULT_SOURCE_VALIDATION_RUN_ID = "20260513T150000Z-source-specific-candidate-generation-validation-v1"
DEFAULT_SOURCE_VALIDATION_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\source_specific_timeblock_applicability_audit_v1")
TOP_K = 3
REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "applicability_audit_contract.json",
    "source_timeblock_outcome_report.json",
    "source_noise_by_timeblock_report.json",
    "source_recovery_by_timeblock_report.json",
    "point_in_time_applicability_proxy_report.json",
    "overfit_risk_report.json",
    "source_archive_or_refine_decision.json",
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


def validate_source_validation(source_validation_dir: Path) -> dict[str, Any]:
    required = [
        "_ARTIFACT_COMPLETE.json",
        "research_decision.json",
        "source_artifact_refs.json",
        "source_generation_contract.json",
        "source_candidate_ledger.jsonl",
        "time_block_source_validation.json",
        "baseline_comparison_report.json",
        "validation_outcome_classification.json",
    ]
    missing = [name for name in required if not (source_validation_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"source validation artifact missing required files: {missing} at {source_validation_dir}")
    complete = _load_json(source_validation_dir / "_ARTIFACT_COMPLETE.json")
    decision = _load_json(source_validation_dir / "research_decision.json")
    if complete.get("complete") is not True:
        raise RuntimeError("source validation artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is not False:
        raise RuntimeError("source validation artifact used silent fallback")
    if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is not False:
        raise RuntimeError("source validation artifact used research fallback")
    if decision.get("authoritative_research_decision") != "source_specific_candidate_generation_drop":
        raise RuntimeError("source validation source is not the frozen dropped validation run")
    refs = _load_json(source_validation_dir / "source_artifact_refs.json").get("source_roots", {})
    return {
        "source_validation_dir": source_validation_dir,
        "_ARTIFACT_COMPLETE.json": complete,
        "research_decision.json": decision,
        "source_generation_contract.json": _load_json(source_validation_dir / "source_generation_contract.json"),
        "time_block_source_validation.json": _load_json(source_validation_dir / "time_block_source_validation.json"),
        "baseline_comparison_report.json": _load_json(source_validation_dir / "baseline_comparison_report.json"),
        "validation_outcome_classification.json": _load_json(source_validation_dir / "validation_outcome_classification.json"),
        "source_roots": {key: Path(value) for key, value in refs.items()},
    }


def load_audit_inputs(source_status: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source_roots = source_status["source_roots"]
    validation_source_status = validation_mod.validate_sources(
        missed_winner_dir=source_roots["missed_winner"],
        root_cause_dir=source_roots["root_cause"],
        wide_dir=source_roots["wide"],
        pattern_dir=source_roots["pattern"],
        upside_dir=source_roots["upside"],
    )
    frame, _ledger = validation_mod.load_validation_inputs(source_status=validation_source_status, wide_dir=source_roots["wide"])
    source_family = source_status["source_generation_contract.json"]["source_family"]
    frame = validation_mod._source_frame(frame, source_family)
    primary_ledger = validation_mod.build_selection_ledger(frame, max_source_slots=1, family_id=validation_mod.PRIMARY_FAMILY_ID)
    primary_selected = validation_mod._selected_from_ledger(primary_ledger)
    baseline_selected = validation_mod._baseline_selected(frame, TOP_K)
    return frame, primary_selected, baseline_selected


def _newly_selected_source(primary_selected: pd.DataFrame, frame: pd.DataFrame) -> pd.DataFrame:
    baseline_keys = set(zip(frame.loc[frame["baseline_top3"], "event_date"].astype(str), frame.loc[frame["baseline_top3"], "code"].astype(str)))
    selected = primary_selected[primary_selected["source_specific_candidate"]].copy()
    if not selected.empty:
        selected["event_date"] = selected["event_date"].astype(str)
        selected["code"] = selected["code"].astype(str)
        frame_lookup = frame.copy()
        frame_lookup["event_date"] = frame_lookup["event_date"].astype(str)
        frame_lookup["code"] = frame_lookup["code"].astype(str)
        missing_columns = [column for column in frame_lookup.columns if column not in selected.columns and column not in {"event_date", "code"}]
        selected = selected.merge(frame_lookup[["event_date", "code", *missing_columns]], on=["event_date", "code"], how="left")
    if selected.empty:
        selected["newly_selected_vs_previous_best"] = []
        return selected
    selected["newly_selected_vs_previous_best"] = [
        (str(row["event_date"]), str(row["code"])) not in baseline_keys for _, row in selected.iterrows()
    ]
    return selected[selected["newly_selected_vs_previous_best"]].copy()


def _nonwinner_mask(frame: pd.DataFrame) -> pd.Series:
    if "future_winner" in frame.columns:
        return ~frame["future_winner"].astype(bool) & (
            pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0)
            | frame["severe_loss20"].astype(bool)
        )
    return validation_mod._nonwinner_mask(frame)


def _date_delta_frame(frame: pd.DataFrame, primary_selected: pd.DataFrame, baseline_selected: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for event_date, day in frame.groupby("event_date", sort=True):
        block = str(event_date)[:4]
        primary_day = primary_selected[primary_selected["event_date"].astype(str).eq(str(event_date))]
        base_day = baseline_selected[baseline_selected["event_date"].astype(str).eq(str(event_date))]
        rows.append(
            {
                "event_date": str(event_date),
                "time_block": block,
                "source_candidate_present": bool(day["source_specific_candidate"].any()),
                "top3_delta_vs_previous_best": (_mean(primary_day["ret20_fwd"]) or 0.0) - (_mean(base_day["ret20_fwd"]) or 0.0),
            }
        )
    return pd.DataFrame(rows)


def build_source_timeblock_outcome_report(frame: pd.DataFrame, primary_selected: pd.DataFrame, baseline_selected: pd.DataFrame) -> dict[str, Any]:
    added = _newly_selected_source(primary_selected, frame)
    date_delta = _date_delta_frame(frame, primary_selected, baseline_selected)
    rows = []
    for block in sorted(frame["time_block"].astype(str).unique().tolist()):
        block_added = added[added["event_date"].astype(str).str.slice(0, 4).eq(block)]
        block_delta = date_delta[date_delta["time_block"].eq(block)]
        rows.append(
            {
                "time_block": block,
                "source_selected_count": int(block_added.shape[0]),
                "source_selected_avg_ret20": _mean(block_added["ret20_fwd"]),
                "recovered_winners": int(block_added["future_winner"].astype(bool).sum()) if len(block_added) else 0,
                "added_nonwinners": int(_nonwinner_mask(block_added).sum()) if len(block_added) else 0,
                "added_severe_losers": int(block_added["severe_loss20"].astype(bool).sum()) if len(block_added) else 0,
                "top3_delta_vs_previous_best": _mean(block_delta["top3_delta_vs_previous_best"]),
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_timeblock_outcome_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "positive_top3_delta_time_block_rate": float(np.mean([(row["top3_delta_vs_previous_best"] or 0.0) > 0.0 for row in rows])) if rows else 0.0,
        "calendar_timeblock_only": True,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_noise_by_timeblock_report(timeblock: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for row in timeblock["rows"]:
        recovered = int(row["recovered_winners"])
        rows.append(
            {
                "time_block": row["time_block"],
                "recovered_winners": recovered,
                "added_nonwinners": row["added_nonwinners"],
                "added_severe_losers": row["added_severe_losers"],
                "nonwinner_added_per_recovered_winner": row["added_nonwinners"] / recovered if recovered else None,
                "severe_loser_added_per_recovered_winner": row["added_severe_losers"] / recovered if recovered else None,
            }
        )
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_noise_by_timeblock_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_recovery_by_timeblock_report(timeblock: dict[str, Any]) -> dict[str, Any]:
    rows = [
        {
            "time_block": row["time_block"],
            "source_selected_count": row["source_selected_count"],
            "recovered_winners": row["recovered_winners"],
            "source_selected_avg_ret20": row["source_selected_avg_ret20"],
            "top3_delta_vs_previous_best": row["top3_delta_vs_previous_best"],
        }
        for row in timeblock["rows"]
    ]
    rows.sort(key=lambda row: (row["recovered_winners"], row["top3_delta_vs_previous_best"] or -999.0), reverse=True)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_recovery_by_timeblock_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "good_calendar_block_count": int(sum((row["top3_delta_vs_previous_best"] or 0.0) > 0.0 and row["recovered_winners"] > 0 for row in rows)),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _rank_bucket(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "rank_missing"
    if numeric <= 3:
        return "rank_1_3"
    if numeric <= 5:
        return "rank_4_5"
    if numeric <= 10:
        return "rank_6_10"
    return "rank_11_plus"


def _count_bucket(value: int) -> str:
    if value <= 5:
        return "candidate_count_1_5"
    if value <= 10:
        return "candidate_count_6_10"
    if value <= 20:
        return "candidate_count_11_20"
    return "candidate_count_21_plus"


def _score_bucket(series: pd.Series, value: Any) -> str:
    numeric = pd.to_numeric(series, errors="coerce")
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val) or numeric.dropna().empty:
        return "score_missing"
    pct = float((numeric <= val).mean())
    if pct >= 0.80:
        return "score_top20pct"
    if pct >= 0.60:
        return "score_60_80pct"
    if pct >= 0.40:
        return "score_40_60pct"
    return "score_bottom40pct"


def build_point_in_time_applicability_proxy_report(frame: pd.DataFrame, primary_selected: pd.DataFrame, baseline_selected: pd.DataFrame) -> dict[str, Any]:
    added = _newly_selected_source(primary_selected, frame)
    if added.empty:
        payload = {
            "schema_version": f"{SCHEMA_PREFIX}_point_in_time_applicability_proxy_report_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "proxy_rows": [],
            "best_proxy": None,
            "point_in_time_proxy_found": False,
        }
        payload["contract_hash"] = _stable_hash(payload)
        return payload

    candidate_counts = frame.groupby("event_date")["code"].count().to_dict()
    added["candidate_count_same_day_bucket"] = added["event_date"].map(candidate_counts).fillna(0).astype(int).map(_count_bucket)
    added["source_candidate_relative_rank_bucket"] = added["previous_best_selection_rank"].map(_rank_bucket)
    score_col = ranking_mod.BASE_SCORE_COLUMN
    if score_col in frame.columns and score_col in added.columns:
        added["source_candidate_score_bucket"] = [_score_bucket(frame[score_col], value) for value in added[score_col]]
    else:
        added["source_candidate_score_bucket"] = "score_unavailable"
    added["weekly_monthly_prior_state_mix"] = added["weekly_prior_state"].astype(str) + "|" + added["monthly_prior_state"].astype(str)
    added["negative_guard_false_stability"] = np.where(added["negative_guard_match"].astype(bool), "negative_guard_true", "negative_guard_false")
    added["market_regime_proxy"] = added["event_date"].astype(str).str.slice(0, 4)

    date_delta = _date_delta_frame(frame, primary_selected, baseline_selected)
    date_delta_map = dict(zip(date_delta["event_date"].astype(str), date_delta["top3_delta_vs_previous_best"]))
    added["date_top3_delta_vs_previous_best"] = added["event_date"].astype(str).map(date_delta_map)
    proxy_columns = [
        "candidate_count_same_day_bucket",
        "source_candidate_score_bucket",
        "source_candidate_relative_rank_bucket",
        "weekly_monthly_prior_state_mix",
        "negative_guard_false_stability",
    ]
    rows = []
    overall_recovered = int(added["future_winner"].astype(bool).sum())
    overall_nonwinner = int(_nonwinner_mask(added).sum())
    overall_severe = int(added["severe_loss20"].astype(bool).sum())
    overall_nonwinner_ratio = overall_nonwinner / overall_recovered if overall_recovered else None
    overall_severe_ratio = overall_severe / overall_recovered if overall_recovered else None
    for proxy in proxy_columns:
        for value, group in added.groupby(proxy, dropna=False, sort=True):
            recovered = int(group["future_winner"].astype(bool).sum())
            nonwinner = int(_nonwinner_mask(group).sum())
            severe = int(group["severe_loss20"].astype(bool).sum())
            top3_delta = _mean(group["date_top3_delta_vs_previous_best"])
            nonwinner_ratio = nonwinner / recovered if recovered else None
            severe_ratio = severe / recovered if recovered else None
            rows.append(
                {
                    "proxy_name": proxy,
                    "proxy_value": str(value),
                    "sample_count": int(len(group)),
                    "time_block_count": int(group["event_date"].astype(str).str.slice(0, 4).nunique()),
                    "recovered_winners": recovered,
                    "added_nonwinners": nonwinner,
                    "added_severe_losers": severe,
                    "nonwinner_added_per_recovered_winner": nonwinner_ratio,
                    "severe_loser_added_per_recovered_winner": severe_ratio,
                    "avg_ret20": _mean(group["ret20_fwd"]),
                    "top3_delta_vs_previous_best": top3_delta,
                    "proxy_observable_point_in_time": True,
                    "material_noise_improvement": bool(
                        recovered >= 10
                        and nonwinner_ratio is not None
                        and severe_ratio is not None
                        and (overall_nonwinner_ratio is None or nonwinner_ratio <= overall_nonwinner_ratio * 0.75)
                        and (overall_severe_ratio is None or severe_ratio <= overall_severe_ratio * 0.75)
                    ),
                    "proxy_candidate_refine_ready": False,
                }
            )
    for row in rows:
        row["proxy_candidate_refine_ready"] = bool(
            row["sample_count"] >= 30
            and row["time_block_count"] >= 2
            and row["recovered_winners"] >= 10
            and row["top3_delta_vs_previous_best"] is not None
            and row["top3_delta_vs_previous_best"] > 0.0
            and row["material_noise_improvement"]
        )
    rows.sort(
        key=lambda row: (
            row["proxy_candidate_refine_ready"],
            row["top3_delta_vs_previous_best"] if row["top3_delta_vs_previous_best"] is not None else -999.0,
            row["recovered_winners"],
        ),
        reverse=True,
    )
    best = rows[0] if rows else None
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_point_in_time_applicability_proxy_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "overall_nonwinner_added_per_recovered_winner": overall_nonwinner_ratio,
        "overall_severe_loser_added_per_recovered_winner": overall_severe_ratio,
        "proxy_rows": rows,
        "best_proxy": best,
        "point_in_time_proxy_found": bool(any(row["proxy_candidate_refine_ready"] for row in rows)),
        "calendar_proxy_reported_for_overfit_only": True,
        "future_labels_used_for_evaluation_only": True,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_overfit_risk_report(timeblock: dict[str, Any], proxy: dict[str, Any]) -> dict[str, Any]:
    calendar_good_blocks = int(sum((row["top3_delta_vs_previous_best"] or 0.0) > 0.0 and row["recovered_winners"] > 0 for row in timeblock["rows"]))
    point_in_time_proxy_found = proxy.get("point_in_time_proxy_found") is True
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_overfit_risk_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "calendar_good_block_count": calendar_good_blocks,
        "positive_top3_delta_time_block_rate": timeblock.get("positive_top3_delta_time_block_rate"),
        "point_in_time_proxy_found": point_in_time_proxy_found,
        "calendar_only_improvement": bool(calendar_good_blocks > 0 and not point_in_time_proxy_found),
        "overfit_risk_high": bool(calendar_good_blocks > 0 and not point_in_time_proxy_found),
        "trading_rule_from_calendar_slicing_recommended": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_archive_or_refine_decision(
    *,
    timeblock: dict[str, Any],
    proxy: dict[str, Any],
    overfit: dict[str, Any],
) -> dict[str, Any]:
    point_in_time_proxy_found = proxy.get("point_in_time_proxy_found") is True
    best_proxy = proxy.get("best_proxy") or {}
    positive_block_rate = float(timeblock.get("positive_top3_delta_time_block_rate") or 0.0)
    if point_in_time_proxy_found and not overfit.get("overfit_risk_high"):
        classification = "source_applicability_refine_ready"
        reasons = ["point_in_time_proxy_found", "proxy_noise_improves", "proxy_top3_delta_positive", "no_calendar_only_rule"]
    elif positive_block_rate > 0.0 and not point_in_time_proxy_found:
        classification = "source_applicability_hold"
        reasons = ["useful_calendar_pockets_exist", "point_in_time_proxy_weak_or_absent", "calendar_only_rule_forbidden"]
    else:
        classification = "source_family_archive"
        reasons = ["no_point_in_time_proxy_exists", "source_remains_noisy_or_unusable", "top3_return_not_repaired"]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_archive_or_refine_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "classification": classification,
        "source_family_archive": classification == "source_family_archive",
        "source_applicability_hold": classification == "source_applicability_hold",
        "source_applicability_refine_ready": classification == "source_applicability_refine_ready",
        "best_proxy": best_proxy,
        "typed_reasons": reasons,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_next_axis_recommendation(archive_decision: dict[str, Any]) -> dict[str, Any]:
    classification = archive_decision.get("classification")
    if classification == "source_applicability_refine_ready":
        next_axis = "regime_limited_source_generation_validation_v1"
        reason = "point-in-time applicability proxy exists; validate a regime-limited source hypothesis before any starter-entry work"
    elif classification == "source_applicability_hold":
        next_axis = "candidate_generation_hypothesis_map_second_hypothesis_review_v1"
        reason = "calendar pockets exist but no strong point-in-time proxy; move to second hypothesis after recording limitations"
    else:
        next_axis = "candidate_generation_hypothesis_map_second_hypothesis_review_v1"
        reason = "source should be archived; move to the second missed-winner candidate-generation hypothesis"
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "one_recommended_next_axis_only": True,
        "recommended_next_axis": next_axis,
        "reason": reason,
        "do_not_continue_axes": [
            "source_specific_candidate_generation_v1_max1slot rescue",
            "learned scorer",
            "threshold/no-trade",
            "safe_full hard filter",
            "negative_guard hard veto",
            "image fusion",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(*, source_validation_dir: Path, source_status: dict[str, Any], frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    refs = []
    for path in sorted(source_validation_dir.glob("*.json")):
        refs.append({"source": "source_validation", "name": path.name, "path": str(path), "exists": path.exists(), "content_hash": _stable_hash(_load_json(path))})
    for path in sorted(source_validation_dir.glob("*.jsonl")):
        refs.append({"source": "source_validation", "name": path.name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    evaluation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "research_phase": "source_specific_timeblock_applicability_audit",
        "boundary": "TRADEX-only",
        "axis_moved": "source_specific_timeblock_applicability_audit",
        "source_validation_decision": source_status["research_decision.json"].get("authoritative_research_decision"),
        "event_count": int(len(frame)),
        "event_day_count": int(frame["event_date"].nunique()),
        "post_drop_diagnostic_only": True,
        "candidate_generation_challenger_created": False,
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
    applicability_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_applicability_audit_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "post_drop_diagnostic_only": True,
        "source_family": source_status["source_generation_contract.json"].get("source_family"),
        "calendar_slicing_trading_rule_forbidden": True,
        "point_in_time_observable_proxy_required_for_refine": True,
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
        "production_ranking_changed": False,
    }
    applicability_contract["contract_hash"] = _stable_hash(applicability_contract)
    source_refs = {
        "schema_version": f"{SCHEMA_PREFIX}_source_artifact_refs_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_roots": {"source_validation": str(source_validation_dir)},
        "refs": refs,
    }
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": source_refs,
        "applicability_audit_contract.json": applicability_contract,
    }


def build_research_decision(
    *,
    archive_decision: dict[str, Any],
    overfit: dict[str, Any],
    proxy: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    classification = archive_decision.get("classification")
    if artifact_complete and classification == "source_applicability_refine_ready":
        decision = "keep_candidate"
        authoritative = "source_applicability_refine_ready"
    elif artifact_complete and classification == "source_applicability_hold":
        decision = "hold"
        authoritative = "source_applicability_hold"
    elif artifact_complete:
        decision = "drop"
        authoritative = "source_family_archive"
    else:
        decision = "drop"
        authoritative = "source_applicability_inconclusive"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "source_specific_timeblock_applicability_audit",
        "boundary": "TRADEX-only",
        "axis_moved": "source_specific_timeblock_applicability_audit",
        "source_validation_decision": "source_specific_candidate_generation_drop",
        "post_drop_diagnostic_only": True,
        "candidate_generation_challenger_created": False,
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
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_score_inputs": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "source_archive_or_refine_classification": classification,
        "overfit_risk_high": overfit.get("overfit_risk_high"),
        "point_in_time_proxy_found": proxy.get("point_in_time_proxy_found"),
        "typed_reasons": archive_decision.get("typed_reasons", []),
        "decision_reasons": [
            {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
            {"code": "point_in_time_proxy_found", "status": "pass" if proxy.get("point_in_time_proxy_found") else "fail", "value": proxy.get("point_in_time_proxy_found")},
            {"code": "overfit_risk_high", "status": "fail" if overfit.get("overfit_risk_high") else "pass", "value": overfit.get("overfit_risk_high")},
            {"code": "post_drop_diagnostic_only", "status": "pass", "value": True},
            {"code": "no_scorer_threshold_or_promotion_created", "status": "pass", "value": True},
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
        "post_drop_diagnostic_only": True,
        "candidate_generation_challenger_created": False,
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


def run_source_specific_timeblock_applicability_audit_v1(
    *,
    source_validation_run_id: str = DEFAULT_SOURCE_VALIDATION_RUN_ID,
    source_validation_root: str | Path = DEFAULT_SOURCE_VALIDATION_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    source_validation_dir = _run_dir(source_validation_root, source_validation_run_id, DEFAULT_SOURCE_VALIDATION_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    source_status = validate_source_validation(source_validation_dir)
    frame, primary_selected, baseline_selected = load_audit_inputs(source_status)
    timeblock = build_source_timeblock_outcome_report(frame, primary_selected, baseline_selected)
    noise_timeblock = build_source_noise_by_timeblock_report(timeblock)
    recovery_timeblock = build_source_recovery_by_timeblock_report(timeblock)
    proxy = build_point_in_time_applicability_proxy_report(frame, primary_selected, baseline_selected)
    overfit = build_overfit_risk_report(timeblock, proxy)
    archive_decision = build_source_archive_or_refine_decision(timeblock=timeblock, proxy=proxy, overfit=overfit)
    next_axis = build_next_axis_recommendation(archive_decision)
    contract_artifacts = build_contract_artifacts(source_validation_dir=source_validation_dir, source_status=source_status, frame=frame)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=ranking_mod.RANDOM_SEED,
        random_seed=ranking_mod.RANDOM_SEED,
        input_artifacts=[{"name": "source_validation", "path": str(source_validation_dir)}],
        asof=str(int(frame["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "post_drop_diagnostic_only": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=sorted(frame["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(frame["event_ymd"].min())), "end_date": str(int(frame["event_ymd"].max())), "label": "source_specific_timeblock_applicability_audit"},
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
        "source_timeblock_outcome_report.json": timeblock,
        "source_noise_by_timeblock_report.json": noise_timeblock,
        "source_recovery_by_timeblock_report.json": recovery_timeblock,
        "point_in_time_applicability_proxy_report.json": proxy,
        "overfit_risk_report.json": overfit,
        "source_archive_or_refine_decision.json": archive_decision,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        archive_decision=archive_decision,
        overfit=overfit,
        proxy=proxy,
        artifact_complete=bool(pre_complete["complete"]),
    )
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "source_archive_or_refine_classification": decision["source_archive_or_refine_classification"],
        "recommended_next_axis": next_axis.get("recommended_next_axis"),
        "post_drop_diagnostic_only": True,
        "candidate_generation_challenger_created": False,
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
    parser.add_argument("--source-validation-run-id", default=DEFAULT_SOURCE_VALIDATION_RUN_ID)
    parser.add_argument("--source-validation-root", default=str(DEFAULT_SOURCE_VALIDATION_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_source_specific_timeblock_applicability_audit_v1(
        source_validation_run_id=args.source_validation_run_id,
        source_validation_root=args.source_validation_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
