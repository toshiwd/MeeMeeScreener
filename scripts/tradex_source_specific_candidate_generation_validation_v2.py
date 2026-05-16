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
from scripts import tradex_missed_winner_event_source_candidate_generation_v1 as source_mod
from scripts import tradex_source_specific_candidate_generation_validation_v1 as v1_mod


AXIS_ID = "source_specific_candidate_generation_validation_v2"
SCHEMA_PREFIX = "tradex_source_specific_candidate_generation_validation_v2"

DEFAULT_MECHANISM_VALIDATION_RUN_ID = "20260513T190000Z-candidate-generation-source-mechanism-validation-v1"
DEFAULT_HYPOTHESIS_REFRESH_RUN_ID = "20260513T180000Z-candidate-generation-hypothesis-map-refresh-v1"
DEFAULT_SECOND_REVIEW_RUN_ID = "20260513T170000Z-candidate-generation-hypothesis-map-second-hypothesis-review-v1"
DEFAULT_MISSED_WINNER_RUN_ID = "20260513T140000Z-missed-winner-event-source-candidate-generation-v1"
DEFAULT_ROOT_CAUSE_RUN_ID = "20260513T130000Z-oracle-gap-and-candidate-generation-root-cause-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"

DEFAULT_MECHANISM_VALIDATION_ROOT = Path(r"G:\Tradex\candidate_generation_source_mechanism_validation_v1")
DEFAULT_HYPOTHESIS_REFRESH_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_refresh_v1")
DEFAULT_SECOND_REVIEW_ROOT = Path(r"G:\Tradex\candidate_generation_hypothesis_map_second_hypothesis_review_v1")
DEFAULT_MISSED_WINNER_ROOT = Path(r"G:\Tradex\missed_winner_event_source_candidate_generation_v1")
DEFAULT_ROOT_CAUSE_ROOT = Path(r"G:\Tradex\oracle_gap_and_candidate_generation_root_cause_v1")
DEFAULT_WIDE_ROOT = Path(r"G:\Tradex\wide_strength_pool_upside_rerank_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v2")

TOP_K = 3
TOP5_K = 5
BASELINE_FAMILY_ID = source_mod.BASELINE_FAMILY_ID
PRIMARY_FAMILY_ID = "source_specific_candidate_generation_validation_v2_max1slot"
DIAGNOSTIC_FAMILY_ID = "source_specific_candidate_generation_validation_v2_max2slot_diagnostic"
REQUIRED_CONTEXT_FIELDS = ("pre_ma20_path_state", "pre_ma60_context_state", "weekly_prior_state", "negative_guard_match", "source_family")

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "selected_target_readback.json",
    "source_generation_contract_v2.json",
    "source_candidate_ledger.jsonl",
    "source_context_availability_audit.json",
    "same_date_support_limitation_report.json",
    "source_overlap_audit.json",
    "source_oracle_diagnostic.json",
    "top5_candidate_pool_report.json",
    "top3_selection_report.json",
    "source_recovery_report.json",
    "source_noise_report.json",
    "baseline_comparison_report.json",
    "branching_report.json",
    "time_block_source_validation.json",
    "validation_outcome_classification.json",
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


def _stamp(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    payload["schema_version"] = f"{SCHEMA_PREFIX}_{artifact_id}_v1"
    payload["generated_at"] = _utc_now()
    payload["axis_id"] = AXIS_ID
    payload["contract_hash"] = _stable_hash({key: value for key, value in payload.items() if key != "contract_hash"})
    return payload


def validate_sources(
    *,
    mechanism_validation_dir: Path,
    hypothesis_refresh_dir: Path,
    second_review_dir: Path,
    missed_winner_dir: Path,
    root_cause_dir: Path,
    wide_dir: Path,
) -> dict[str, Any]:
    required_by_source = {
        "mechanism_validation": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "selected_next_validation_target.json",
            "per_source_same_date_support_report.json",
        ],
        "hypothesis_refresh": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "refreshed_candidate_generation_hypothesis_map.json"],
        "second_review": ["_ARTIFACT_COMPLETE.json", "research_decision.json"],
        "missed_winner": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "source_artifact_refs.json"],
        "root_cause": ["_ARTIFACT_COMPLETE.json", "research_decision.json"],
        "wide": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "date_level_selection_ledger.jsonl"],
    }
    dirs = {
        "mechanism_validation": mechanism_validation_dir,
        "hypothesis_refresh": hypothesis_refresh_dir,
        "second_review": second_review_dir,
        "missed_winner": missed_winner_dir,
        "root_cause": root_cause_dir,
        "wide": wide_dir,
    }
    status: dict[str, Any] = {}
    for source_name, names in required_by_source.items():
        root = dirs[source_name]
        missing = [name for name in names if not (root / name).exists()]
        if missing:
            raise FileNotFoundError(f"{source_name} missing required artifacts: {missing} at {root}")
        complete = _load_json(root / "_ARTIFACT_COMPLETE.json")
        decision = _load_json(root / "research_decision.json")
        if complete.get("complete") is not True:
            raise RuntimeError(f"{source_name} artifact is not complete")
        if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is True:
            raise RuntimeError(f"{source_name} used silent fallback")
        if complete.get("research_fallback_used") is True or decision.get("research_fallback_used") is True:
            raise RuntimeError(f"{source_name} used research fallback")
        status[source_name] = {"_ARTIFACT_COMPLETE.json": complete, "research_decision.json": decision}
        for name in names:
            if name in {"_ARTIFACT_COMPLETE.json", "research_decision.json"}:
                continue
            path = root / name
            status[source_name][name] = _load_json(path) if path.suffix == ".json" else {"path": str(path), "exists": True, "file_hash": _file_hash(path)}

    mechanism_decision = status["mechanism_validation"]["research_decision.json"]
    if mechanism_decision.get("authoritative_research_decision") != "source_mechanism_validation_next_target_ready":
        raise RuntimeError("source mechanism validation is not next-target-ready")
    selection = status["mechanism_validation"]["selected_next_validation_target.json"]
    if selection.get("selected_hypothesis_id") != "candidate_generation_map_refresh_hypothesis_1":
        raise RuntimeError("selected target is not candidate_generation_map_refresh_hypothesis_1")
    if selection.get("selected_next_axis") != AXIS_ID:
        raise RuntimeError("selected target does not point to source_specific_candidate_generation_validation_v2")
    if status["hypothesis_refresh"]["research_decision.json"].get("authoritative_research_decision") != "hypothesis_map_refreshed_next_validation_ready":
        raise RuntimeError("hypothesis refresh source is not next-validation-ready")
    if status["second_review"]["research_decision.json"].get("authoritative_research_decision") != "second_hypothesis_drop":
        raise RuntimeError("second hypothesis source is not dropped")
    return status


def _derived_source_dirs(status: dict[str, Any], *, root_cause_dir: Path, wide_dir: Path) -> dict[str, Path]:
    refs = status["missed_winner"]["source_artifact_refs.json"].get("source_roots") or {}
    required = ("pattern", "upside", "feature_diagnosis")
    missing = [name for name in required if not refs.get(name)]
    if missing:
        raise RuntimeError(f"missed_winner source refs missing derived reconstruction roots: {missing}")
    return {
        "root_cause": root_cause_dir,
        "wide": wide_dir,
        "pattern": Path(refs["pattern"]),
        "upside": Path(refs["upside"]),
        "feature_diagnosis": Path(refs["feature_diagnosis"]),
    }


def load_validation_frame(*, status: dict[str, Any], root_cause_dir: Path, wide_dir: Path) -> pd.DataFrame:
    derived = _derived_source_dirs(status, root_cause_dir=root_cause_dir, wide_dir=wide_dir)
    source_status = source_mod.validate_sources(
        root_cause_dir=derived["root_cause"],
        wide_dir=derived["wide"],
        pattern_dir=derived["pattern"],
        upside_dir=derived["upside"],
        feature_diagnosis_dir=derived["feature_diagnosis"],
    )
    frame, _ledger = source_mod.load_diagnosis_inputs(source_status=source_status, wide_dir=wide_dir)
    return frame


def selected_hypothesis(status: dict[str, Any]) -> dict[str, Any]:
    selected_id = status["mechanism_validation"]["selected_next_validation_target.json"].get("selected_hypothesis_id")
    hypotheses = status["hypothesis_refresh"]["refreshed_candidate_generation_hypothesis_map.json"].get("hypotheses") or []
    for hypothesis in hypotheses:
        if hypothesis.get("hypothesis_id") == selected_id:
            return dict(hypothesis)
    raise RuntimeError(f"selected hypothesis not found in refreshed map: {selected_id}")


def build_selected_target_readback(status: dict[str, Any], hypothesis: dict[str, Any]) -> dict[str, Any]:
    selection = status["mechanism_validation"]["selected_next_validation_target.json"]
    return _stamp(
        {
            "selected_target_source": "selected_next_validation_target.json",
            "selected_hypothesis_id": selection.get("selected_hypothesis_id"),
            "selected_next_axis": selection.get("selected_next_axis"),
            "selected_source_family": selection.get("selected_source_family"),
            "source_family_from_refreshed_map": hypothesis.get("source_family"),
            "source_definition_unchanged": selection.get("selected_source_family") == hypothesis.get("source_family"),
            "selected_reason": selection.get("reason"),
            "per_source_same_date_support_available": status["mechanism_validation"]["per_source_same_date_support_report.json"].get("per_source_same_date_support_available"),
        },
        "selected_target_readback",
    )


def build_source_context_availability_audit(frame: pd.DataFrame, hypothesis: dict[str, Any], status: dict[str, Any], *, wide_dir: Path) -> dict[str, Any]:
    source = str(hypothesis.get("source_family") or "")
    available = {field: field in frame.columns for field in REQUIRED_CONTEXT_FIELDS}
    source_count = int(frame["source_family"].astype(str).eq(source).sum()) if "source_family" in frame.columns else 0
    missing = [field for field, present in available.items() if not present]
    ledger_path = wide_dir / "date_level_selection_ledger.jsonl"
    ledger_fields: set[str] = set()
    if ledger_path.exists():
        with ledger_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    row = json.loads(line)
                    if isinstance(row, dict):
                        ledger_fields.update(str(key) for key in row.keys())
                    break
    return _stamp(
        {
            "selected_hypothesis_id": hypothesis.get("hypothesis_id"),
            "selected_source_family": source,
            "source_definition_applicable_point_in_time": bool(not missing and source_count > 0),
            "required_context_fields": list(REQUIRED_CONTEXT_FIELDS),
            "available_context_fields": available,
            "missing_required_context_fields": missing,
            "source_candidate_count": source_count,
            "same_date_ledger_path": str(ledger_path),
            "same_date_ledger_observed_fields": sorted(ledger_fields),
            "same_date_ledger_missing_required_context_fields": sorted({"pre_ma60_context_state"} - ledger_fields),
            "per_source_same_date_support_available": status["mechanism_validation"]["per_source_same_date_support_report.json"].get("per_source_same_date_support_available"),
        },
        "source_context_availability_audit",
    )


def build_same_date_support_limitation_report(status: dict[str, Any]) -> dict[str, Any]:
    source_report = status["mechanism_validation"]["per_source_same_date_support_report.json"]
    missing = source_report.get("missing_required_source_fields") or []
    if "pre_ma60_context_state" not in missing:
        missing = [*missing, "pre_ma60_context_state"]
    return _stamp(
        {
            "per_source_same_date_support_available": False,
            "same_date_support_not_faked": True,
            "validation_conclusion_relies_on_per_source_same_date_support": False,
            "missing_required_context_fields": sorted(set(missing)),
            "source_complete_same_date_support_blocker": "available same-date ledger does not include pre_ma60_context_state",
            "aggregate_same_date_support_only": source_report.get("aggregate_same_date_support_only"),
        },
        "same_date_support_limitation_report",
    )


def _source_frame(frame: pd.DataFrame, source_family: str) -> pd.DataFrame:
    return v1_mod._source_frame(frame, source_family)


def _selected_from_ledger(ledger: pd.DataFrame) -> pd.DataFrame:
    return ledger[ledger["selected_topk"].eq(True)].copy()


def build_selection_ledger(frame: pd.DataFrame, *, max_source_slots: int, family_id: str, k: int = TOP_K) -> pd.DataFrame:
    return v1_mod.build_selection_ledger(frame, max_source_slots=max_source_slots, family_id=family_id, k=k)


def build_source_candidate_ledger(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return v1_mod.build_source_candidate_ledger(frame)


def build_source_overlap_audit(frame: pd.DataFrame) -> dict[str, Any]:
    payload = v1_mod.build_source_overlap_audit(frame)
    return _stamp({key: value for key, value in payload.items() if key not in {"schema_version", "generated_at", "axis_id", "contract_hash"}}, "source_overlap_audit")


def build_source_oracle_diagnostic(frame: pd.DataFrame) -> dict[str, Any]:
    payload = v1_mod.build_source_oracle_diagnostic(frame)
    return _stamp({key: value for key, value in payload.items() if key not in {"schema_version", "generated_at", "axis_id", "contract_hash"}}, "source_oracle_diagnostic")


def _baseline_selected(frame: pd.DataFrame, k: int = TOP_K) -> pd.DataFrame:
    out = frame[frame["selection_rank"].le(k)].copy()
    out["ranker_family_id"] = BASELINE_FAMILY_ID
    out["selected_topk"] = True
    return out


def _topk_metrics(selected: pd.DataFrame, frame: pd.DataFrame, *, family_id: str) -> dict[str, Any]:
    return v1_mod._topk_metrics(selected, frame, family_id=family_id)


def _top5_pool_metrics(selected: pd.DataFrame, frame: pd.DataFrame, *, family_id: str) -> dict[str, Any]:
    winner_total = int(frame["future_winner"].astype(bool).sum()) if "future_winner" in frame.columns else 0
    future_top10_total = int(frame["is_future_top10_by_ret20"].astype(bool).sum()) if "is_future_top10_by_ret20" in frame.columns else 0
    ret20 = pd.to_numeric(selected.get("ret20_fwd", pd.Series(index=selected.index, dtype=float)), errors="coerce")
    severe = selected["severe_loss20"].astype(bool) if "severe_loss20" in selected.columns else pd.Series(False, index=selected.index)
    human_selectable = ret20.gt(0.0) & ~severe
    nonwinner = v1_mod._nonwinner_mask(selected) if len(selected) else pd.Series(dtype=bool)
    big_winner_count = int(selected["future_winner"].astype(bool).sum()) if "future_winner" in selected.columns else 0
    future_top10_count = int(selected["is_future_top10_by_ret20"].astype(bool).sum()) if "is_future_top10_by_ret20" in selected.columns else 0

    day_rows = []
    for event_date, _day in frame.groupby("event_date", sort=True):
        selected_day = selected[selected["event_date"].astype(str).eq(str(event_date))]
        day_ret20 = pd.to_numeric(selected_day.get("ret20_fwd", pd.Series(index=selected_day.index, dtype=float)), errors="coerce")
        day_severe = selected_day["severe_loss20"].astype(bool) if "severe_loss20" in selected_day.columns else pd.Series(False, index=selected_day.index)
        day_selectable = day_ret20.gt(0.0) & ~day_severe
        day_rows.append(
            {
                "event_date": str(event_date),
                "top5_candidate_count": int(len(selected_day)),
                "top5_human_selectable_candidate_count": int(day_selectable.sum()),
                "top5_has_at_least_3_human_selectable_candidates": int(day_selectable.sum()) >= 3,
                "top5_unique_source_family_count": int(selected_day["source_family"].astype(str).nunique()) if "source_family" in selected_day.columns and len(selected_day) else 0,
                "top5_severe_loss_count": int(day_severe.sum()) if len(selected_day) else 0,
            }
        )
    day_frame = pd.DataFrame(day_rows)
    source_counts = selected["source_family"].astype(str).value_counts() if "source_family" in selected.columns and len(selected) else pd.Series(dtype=int)
    top_source_share = float(source_counts.iloc[0] / len(selected)) if len(selected) and len(source_counts) else 0.0
    return {
        "ranker_family_id": family_id,
        "evaluation_role": "primary_top5_candidate_pool_quality",
        "final_max3_selection_owner": "human_user",
        "forced_top3_is_primary": False,
        "top5_candidate_count": int(len(selected)),
        "top5_avg_ret20": _mean(selected["ret20_fwd"]) if len(selected) else None,
        "top5_win_rate20": float(ret20.gt(0.0).mean()) if len(selected) else 0.0,
        "top5_big_winner_capture_rate": _safe_rate(big_winner_count, winner_total),
        "top5_future_top10_capture_rate": _safe_rate(future_top10_count, future_top10_total),
        "top5_severe_loss_rate": float(severe.mean()) if len(selected) else 0.0,
        "top5_bad_pick_count": int(nonwinner.sum()) if len(selected) else 0,
        "top5_bad_pick_rate": float(nonwinner.mean()) if len(selected) else 0.0,
        "top5_candidate_diversity": _mean(day_frame["top5_unique_source_family_count"]) if len(day_frame) else None,
        "top5_top_source_share": top_source_share,
        "top5_human_selectable_candidate_count": int(human_selectable.sum()) if len(selected) else 0,
        "top5_human_selectable_candidate_day_count": int(day_frame["top5_has_at_least_3_human_selectable_candidates"].sum()) if len(day_frame) else 0,
        "top5_human_selectable_candidate_day_rate": float(day_frame["top5_has_at_least_3_human_selectable_candidates"].mean()) if len(day_frame) else 0.0,
        "top5_candidate_pool_quality_diluted": bool(len(selected) and (_mean(selected["ret20_fwd"]) or 0.0) <= 0.0),
        "rows_sample": day_rows[:500],
    }


def _oracle_top3_avg(frame: pd.DataFrame) -> float | None:
    return v1_mod._oracle_top3_avg(frame)


def _selection_sets(selected: pd.DataFrame) -> dict[str, set[str]]:
    return v1_mod._selection_sets(selected)


def build_top3_selection_report(frame: pd.DataFrame, primary_selected: pd.DataFrame, diagnostic_selected: pd.DataFrame) -> dict[str, Any]:
    baseline = _baseline_selected(frame, TOP_K)
    oracle_avg = _oracle_top3_avg(frame)
    base_metrics = _topk_metrics(baseline, frame, family_id=BASELINE_FAMILY_ID)
    primary_metrics = _topk_metrics(primary_selected, frame, family_id=PRIMARY_FAMILY_ID)
    diagnostic_metrics = _topk_metrics(diagnostic_selected, frame, family_id=DIAGNOSTIC_FAMILY_ID)
    for metrics in (base_metrics, primary_metrics, diagnostic_metrics):
        metrics["oracle_top3_gap_ret20"] = (metrics["selected_top3_avg_ret20"] or 0.0) - (oracle_avg or 0.0)
    base_sets = _selection_sets(baseline)
    primary_sets = _selection_sets(primary_selected)
    changed_top3 = sum(len(base_sets.get(day, set()).symmetric_difference(primary_sets.get(day, set()))) for day in sorted(set(base_sets) | set(primary_sets)))
    baseline_top5 = _baseline_selected(frame, TOP5_K)
    primary_top5_ledger = build_selection_ledger(frame, max_source_slots=1, family_id=PRIMARY_FAMILY_ID, k=TOP5_K)
    primary_top5 = _selected_from_ledger(primary_top5_ledger)
    base5_sets = _selection_sets(baseline_top5)
    primary5_sets = _selection_sets(primary_top5)
    changed_top5 = sum(len(base5_sets.get(day, set()).symmetric_difference(primary5_sets.get(day, set()))) for day in sorted(set(base5_sets) | set(primary5_sets)))
    return _stamp(
        {
            "wide_pool_oracle_top3_avg_ret20": oracle_avg,
            "previous_best": base_metrics,
            "source_specific_v2_max1slot": primary_metrics,
            "source_specific_v2_max2slot_diagnostic": diagnostic_metrics,
            "changed_top3_members_count_vs_previous_best": int(changed_top3),
            "changed_top5_members_count_vs_previous_best": int(changed_top5),
            "candidate_scoring_created": False,
            "threshold_policy_created": False,
        },
        "top3_selection_report",
    )


def build_top5_candidate_pool_report(frame: pd.DataFrame) -> dict[str, Any]:
    baseline_top5 = _baseline_selected(frame, TOP5_K)
    primary_top5_ledger = build_selection_ledger(frame, max_source_slots=1, family_id=PRIMARY_FAMILY_ID, k=TOP5_K)
    diagnostic_top5_ledger = build_selection_ledger(frame, max_source_slots=2, family_id=DIAGNOSTIC_FAMILY_ID, k=TOP5_K)
    primary_top5 = _selected_from_ledger(primary_top5_ledger)
    diagnostic_top5 = _selected_from_ledger(diagnostic_top5_ledger)
    base_metrics = _top5_pool_metrics(baseline_top5, frame, family_id=BASELINE_FAMILY_ID)
    primary_metrics = _top5_pool_metrics(primary_top5, frame, family_id=PRIMARY_FAMILY_ID)
    diagnostic_metrics = _top5_pool_metrics(diagnostic_top5, frame, family_id=DIAGNOSTIC_FAMILY_ID)
    base_sets = _selection_sets(baseline_top5)
    primary_sets = _selection_sets(primary_top5)
    changed_top5 = sum(len(base_sets.get(day, set()).symmetric_difference(primary_sets.get(day, set()))) for day in sorted(set(base_sets) | set(primary_sets)))
    return _stamp(
        {
            "evaluation_role": "primary_top5_candidate_pool_quality",
            "goal": "produce about five buy candidates with enough quality for the user to choose up to three",
            "final_max3_selection_owner": "human_user",
            "forced_top3_is_primary": False,
            "candidate_pool_size_target": TOP5_K,
            "previous_best": base_metrics,
            "source_specific_v2_max1slot": primary_metrics,
            "source_specific_v2_max2slot_diagnostic": diagnostic_metrics,
            "changed_top5_members_count_vs_previous_best": int(changed_top5),
            "candidate_scoring_created": False,
            "threshold_policy_created": False,
        },
        "top5_candidate_pool_report",
    )


def build_source_recovery_report(frame: pd.DataFrame, primary_selected: pd.DataFrame) -> dict[str, Any]:
    payload = v1_mod.build_source_recovery_report(frame, primary_selected)
    return _stamp({key: value for key, value in payload.items() if key not in {"schema_version", "generated_at", "axis_id", "contract_hash"}}, "source_recovery_report")


def build_source_noise_report(recovery: dict[str, Any]) -> dict[str, Any]:
    payload = v1_mod.build_source_noise_report(recovery)
    return _stamp({key: value for key, value in payload.items() if key not in {"schema_version", "generated_at", "axis_id", "contract_hash"}}, "source_noise_report")


def build_time_block_source_validation(frame: pd.DataFrame, primary_selected: pd.DataFrame) -> dict[str, Any]:
    payload = v1_mod.build_time_block_source_validation(frame, primary_selected)
    return _stamp({key: value for key, value in payload.items() if key not in {"schema_version", "generated_at", "axis_id", "contract_hash"}}, "time_block_source_validation")


def build_baseline_comparison_report(top3_report: dict[str, Any], top5_report: dict[str, Any], recovery: dict[str, Any], noise: dict[str, Any], timeblock: dict[str, Any]) -> dict[str, Any]:
    base = top3_report["previous_best"]
    primary = top3_report["source_specific_v2_max1slot"]
    base5 = top5_report["previous_best"]
    primary5 = top5_report["source_specific_v2_max1slot"]
    return _stamp(
        {
            "champion": "previous_best_wide_score_baseline",
            "challenger": "source_specific_candidate_generation_validation_v2",
            "primary_metric_scope": "top5_candidate_pool_quality",
            "secondary_metric_scope": "top3_operating_guardrail",
            "forced_top3_is_primary": False,
            "final_max3_selection_owner": "human_user",
            "top5_avg_ret20_delta_vs_previous_best": (primary5.get("top5_avg_ret20") or 0.0) - (base5.get("top5_avg_ret20") or 0.0),
            "top5_win_rate20_delta_vs_previous_best": (primary5.get("top5_win_rate20") or 0.0) - (base5.get("top5_win_rate20") or 0.0),
            "top5_big_winner_capture_rate_delta_vs_previous_best": (primary5.get("top5_big_winner_capture_rate") or 0.0) - (base5.get("top5_big_winner_capture_rate") or 0.0),
            "top5_future_top10_capture_rate_delta_vs_previous_best": (primary5.get("top5_future_top10_capture_rate") or 0.0) - (base5.get("top5_future_top10_capture_rate") or 0.0),
            "top5_severe_loss_rate_delta_vs_previous_best": (primary5.get("top5_severe_loss_rate") or 0.0) - (base5.get("top5_severe_loss_rate") or 0.0),
            "top5_bad_pick_count_delta_vs_previous_best": int(primary5.get("top5_bad_pick_count") or 0) - int(base5.get("top5_bad_pick_count") or 0),
            "top5_candidate_diversity_delta_vs_previous_best": (primary5.get("top5_candidate_diversity") or 0.0) - (base5.get("top5_candidate_diversity") or 0.0),
            "top5_human_selectable_candidate_day_rate_delta_vs_previous_best": (primary5.get("top5_human_selectable_candidate_day_rate") or 0.0) - (base5.get("top5_human_selectable_candidate_day_rate") or 0.0),
            "top5_top_source_share_delta_vs_previous_best": (primary5.get("top5_top_source_share") or 0.0) - (base5.get("top5_top_source_share") or 0.0),
            "selected_top3_avg_ret20_delta_vs_previous_best": (primary.get("selected_top3_avg_ret20") or 0.0) - (base.get("selected_top3_avg_ret20") or 0.0),
            "oracle_top3_gap_delta_vs_previous_best": (primary.get("oracle_top3_gap_ret20") or 0.0) - (base.get("oracle_top3_gap_ret20") or 0.0),
            "selected_nonwinner_when_winner_available_delta_vs_previous_best": (primary.get("selected_nonwinner_when_winner_available_rate") or 0.0) - (base.get("selected_nonwinner_when_winner_available_rate") or 0.0),
            "selected_top3_severe_loss_rate_delta_vs_previous_best": (primary.get("selected_top3_severe_loss_rate20") or 0.0) - (base.get("selected_top3_severe_loss_rate20") or 0.0),
            "changed_top3_members_count_vs_previous_best": top3_report.get("changed_top3_members_count_vs_previous_best"),
            "changed_top5_members_count_vs_previous_best": top3_report.get("changed_top5_members_count_vs_previous_best"),
            "recovered_missed_winner_count": recovery.get("recovered_missed_winner_count"),
            "nonwinner_added_per_recovered_winner": noise.get("nonwinner_added_per_recovered_winner"),
            "severe_loser_added_per_recovered_winner": noise.get("severe_loser_added_per_recovered_winner"),
            "effect_stable_across_time_blocks": timeblock.get("effect_stable_across_time_blocks"),
        },
        "baseline_comparison_report",
    )


def build_branching_report(top3_report: dict[str, Any]) -> dict[str, Any]:
    return _stamp(
        {
            "changed_top3_members_count_vs_previous_best": top3_report.get("changed_top3_members_count_vs_previous_best"),
            "changed_top5_members_count_vs_previous_best": top3_report.get("changed_top5_members_count_vs_previous_best"),
            "changed_rank_count_if_available": None,
            "selection_divergence_reason": "selected source candidates are evaluated primarily as top5 pool additions; top3 movement is a secondary guardrail",
            "real_branching_observed": int(top3_report.get("changed_top5_members_count_vs_previous_best") or 0) > 0,
        },
        "branching_report",
    )


def build_validation_outcome_classification(
    *,
    overlap: dict[str, Any],
    comparison: dict[str, Any],
    recovery: dict[str, Any],
    noise: dict[str, Any],
    timeblock: dict[str, Any],
    context: dict[str, Any],
    same_date: dict[str, Any],
) -> dict[str, Any]:
    top5_improved = comparison.get("top5_avg_ret20_delta_vs_previous_best", 0.0) > 0.0
    top5_capture_improved = comparison.get("top5_big_winner_capture_rate_delta_vs_previous_best", 0.0) > 0.0
    top5_severe_ok = comparison.get("top5_severe_loss_rate_delta_vs_previous_best", 0.0) <= 0.0
    top5_noise_ok = comparison.get("top5_bad_pick_count_delta_vs_previous_best", 0) <= 0
    top3_improved = comparison.get("selected_top3_avg_ret20_delta_vs_previous_best", 0.0) > 0.0
    top3_not_fatal = comparison.get("selected_top3_avg_ret20_delta_vs_previous_best", 0.0) >= -0.02
    recovered = int(recovery.get("recovered_missed_winner_count") or 0)
    severe_ratio = noise.get("severe_loser_added_per_recovered_winner")
    nonwinner_ratio = noise.get("nonwinner_added_per_recovered_winner")
    source_approx = not bool(context.get("source_definition_applicable_point_in_time"))
    fake_support = not bool(same_date.get("same_date_support_not_faked"))
    timeblock_specific = not bool(timeblock.get("effect_stable_across_time_blocks"))
    if source_approx or fake_support:
        outcome = "source_definition_blocked_or_fake_support"
    elif recovered > 0 and top5_improved and top5_capture_improved and top5_severe_ok and top5_noise_ok and top3_not_fatal:
        outcome = "source_improves_top5_candidate_pool"
    elif recovered > 0 and top5_improved and top3_not_fatal:
        outcome = "source_improves_top5_but_needs_human_selection_features"
    elif recovered > 0 and ((severe_ratio is not None and severe_ratio > 1.0) or (nonwinner_ratio is not None and nonwinner_ratio > 4.0) or not top5_noise_ok):
        outcome = "source_recovers_winners_but_too_noisy"
    elif timeblock_specific and recovered > 0:
        outcome = "source_timeblock_specific"
    else:
        outcome = "source_under_ranked_but_unusable"
    return _stamp(
        {
            "validation_outcome": outcome,
            "source_improves_top5_candidate_pool": outcome == "source_improves_top5_candidate_pool",
            "source_improves_top5_but_needs_human_selection_features": outcome == "source_improves_top5_but_needs_human_selection_features",
            "source_recovers_winners_cleanly": outcome == "source_improves_top5_candidate_pool",
            "source_recovers_winners_but_too_noisy": outcome == "source_recovers_winners_but_too_noisy",
            "source_under_ranked_but_unusable": outcome == "source_under_ranked_but_unusable",
            "source_timeblock_specific": outcome == "source_timeblock_specific",
            "source_definition_blocked_or_fake_support": outcome == "source_definition_blocked_or_fake_support",
            "evidence": {
                "top5_avg_ret20_improved": top5_improved,
                "top5_big_winner_capture_improved": top5_capture_improved,
                "top5_severe_loss_not_worse": top5_severe_ok,
                "top5_bad_pick_count_not_worse": top5_noise_ok,
                "top3_improved": top3_improved,
                "top3_not_fatal": top3_not_fatal,
                "recovered_missed_winner_count": recovered,
                "severe_loser_added_per_recovered_winner": severe_ratio,
                "nonwinner_added_per_recovered_winner": nonwinner_ratio,
                "source_candidate_overlap_with_previous_best_top3_rate": overlap.get("source_candidate_overlap_with_previous_best_top3_rate"),
                "effect_stable_across_time_blocks": timeblock.get("effect_stable_across_time_blocks"),
                "per_source_same_date_support_available": same_date.get("per_source_same_date_support_available"),
            },
        },
        "validation_outcome_classification",
    )


def build_next_axis_recommendation(outcome: dict[str, Any]) -> dict[str, Any]:
    if outcome.get("source_improves_top5_candidate_pool"):
        next_axis = "source_specific_candidate_generation_challenger_v2_contract"
        reason = "candidate-generation validation improved top5 pool quality without unacceptable severe-loss or bad-pick dilution"
    elif outcome.get("source_improves_top5_but_needs_human_selection_features"):
        next_axis = "source_specific_candidate_generation_v2_explanation_features"
        reason = "top5 pool improved, but human selection needs better explanation features before keep"
    elif outcome.get("source_recovers_winners_but_too_noisy"):
        next_axis = "source_specific_candidate_generation_v2_noise_decomposition"
        reason = "source recovers winners but added nonwinner/severe risk is too high"
    elif outcome.get("source_timeblock_specific"):
        next_axis = "source_specific_candidate_generation_v2_timeblock_applicability_audit"
        reason = "source has winner recovery but effect is not stable across time blocks"
    else:
        next_axis = "candidate_generation_hypothesis_map_refresh_followup_v2"
        reason = "selected source mechanism did not improve previous_best max3 operating quality"
    return _stamp(
        {
            "one_recommended_next_axis_only": True,
            "recommended_next_axis": next_axis,
            "reason": reason,
            "do_not_continue_axes": [
                "learned scorer",
                "ranking objective",
                "threshold/no-trade",
                "image fusion",
                "production ranking",
                "MeeMee reflection",
                "safe_full hard filter",
                "negative_guard hard veto",
            ],
        },
        "next_axis_recommendation",
    )


def build_contract_artifacts(
    *,
    source_dirs: dict[str, Path],
    frame: pd.DataFrame,
    hypothesis: dict[str, Any],
    selected: dict[str, Any],
    context: dict[str, Any],
    same_date: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    refs = []
    for source_name, root in source_dirs.items():
        for path in sorted(root.glob("*.json")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "content_hash": _stable_hash(_load_json(path))})
        for path in sorted(root.glob("*.jsonl")):
            refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    evaluation_contract = _stamp(
        {
            "research_phase": "source_specific_candidate_generation_validation_v2",
            "boundary": "TRADEX-only",
            "axis_moved": "source_specific_candidate_generation_validation_v2",
            "goal": "TRADEX produces about five buy candidates with high-quality features; user selects up to three positions",
            "source_mechanism_validation_decision": "source_mechanism_validation_next_target_ready",
            "champion": "previous_best_wide_score_baseline",
            "challenger": "source_specific_candidate_generation_validation_v2",
            "same_condition_controls": {
                "same_period": True,
                "same_wide_pool_baseline_context": True,
                "same_top5_candidate_pool_evaluation": True,
                "same_top3_guardrail_evaluation": True,
                "future_labels_for_evaluation_only": True,
            },
            "primary_metrics": [
                "top5_avg_ret20",
                "top5_win_rate20",
                "top5_big_winner_capture_rate",
                "top5_future_top10_capture_rate",
                "top5_severe_loss_rate",
                "top5_bad_pick_count",
                "top5_candidate_diversity",
                "top5_human_selectable_candidate_day_rate",
            ],
            "secondary_metrics": [
                "selected_top3_avg_ret20",
                "selected_top3_severe_loss_rate20",
                "oracle_top3_gap_ret20",
                "selected_nonwinner_when_winner_available_rate",
            ],
            "forced_top3_is_primary": False,
            "final_max3_selection_owner": "human_user",
            "event_count": int(len(frame)),
            "event_day_count": int(frame["event_date"].nunique()) if "event_date" in frame.columns else 0,
            "candidate_generation_challenger_created": True,
            "candidate_generation_scope": "research_only",
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
        },
        "evaluation_contract",
    )
    generation_contract = _stamp(
        {
            "primary_challenger": "source_specific_candidate_generation_validation_v2",
            "selected_hypothesis_id": selected.get("selected_hypothesis_id"),
            "source_family": hypothesis.get("source_family"),
            "source_definition_unchanged": context.get("source_definition_applicable_point_in_time"),
            "candidate_pool_size_target": TOP5_K,
            "final_max3_selection_owner": "human_user",
            "forced_top3_is_primary": False,
            "max_source_slots_per_day_primary": 1,
            "max_source_slots_per_day_diagnostic": 2,
            "candidate_generation_challenger_created": True,
            "candidate_generation_scope": "research_only",
            "candidate_scoring_created": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_generation": False,
            "safe_full_used_as_hard_filter": False,
            "negative_guard_used_as_hard_veto": False,
            "per_source_same_date_support_available": same_date.get("per_source_same_date_support_available"),
            "same_date_support_not_faked": same_date.get("same_date_support_not_faked"),
        },
        "source_generation_contract_v2",
    )
    refs_payload = _stamp({"source_roots": {key: str(value) for key, value in source_dirs.items()}, "refs": refs}, "source_artifact_refs")
    return {
        "evaluation_contract.json": evaluation_contract,
        "source_artifact_refs.json": refs_payload,
        "source_generation_contract_v2.json": generation_contract,
    }


def build_research_decision(
    *,
    comparison: dict[str, Any],
    recovery: dict[str, Any],
    noise: dict[str, Any],
    branching: dict[str, Any],
    timeblock: dict[str, Any],
    context: dict[str, Any],
    same_date: dict[str, Any],
    outcome: dict[str, Any],
    artifact_complete: bool,
    selected_hypothesis_id: str,
) -> dict[str, Any]:
    top5_return_improved = comparison.get("top5_avg_ret20_delta_vs_previous_best", 0.0) > 0.0
    top5_winner_capture_improved = comparison.get("top5_big_winner_capture_rate_delta_vs_previous_best", 0.0) > 0.0
    top5_future_top10_capture_not_worse = comparison.get("top5_future_top10_capture_rate_delta_vs_previous_best", 0.0) >= 0.0
    top5_severe_ok = comparison.get("top5_severe_loss_rate_delta_vs_previous_best", 0.0) <= 0.0
    top5_bad_pick_ok = comparison.get("top5_bad_pick_count_delta_vs_previous_best", 0) <= 0
    top5_human_selectable_ok = comparison.get("top5_human_selectable_candidate_day_rate_delta_vs_previous_best", 0.0) >= 0.0
    top5_diversity_ok = comparison.get("top5_top_source_share_delta_vs_previous_best", 0.0) <= 0.25
    top3_return_delta = comparison.get("selected_top3_avg_ret20_delta_vs_previous_best", 0.0)
    top3_not_fatal = top3_return_delta >= -0.02
    oracle_improved = comparison.get("oracle_top3_gap_delta_vs_previous_best", 0.0) > 0.0
    nonwinner_delta = comparison.get("selected_nonwinner_when_winner_available_delta_vs_previous_best", 0.0)
    nonwinner_ok = nonwinner_delta <= 0.0
    severe_delta = comparison.get("selected_top3_severe_loss_rate_delta_vs_previous_best", 0.0)
    severe_ok = severe_delta <= 0.02
    recovered = int(recovery.get("recovered_missed_winner_count") or 0)
    nonwinner_ratio = noise.get("nonwinner_added_per_recovered_winner")
    severe_ratio = noise.get("severe_loser_added_per_recovered_winner")
    noise_ok = (nonwinner_ratio is not None and nonwinner_ratio <= 4.0) and (severe_ratio is not None and severe_ratio <= 1.0)
    branching_ok = branching.get("real_branching_observed") is True
    stable = timeblock.get("effect_stable_across_time_blocks") is True
    source_applicable = context.get("source_definition_applicable_point_in_time") is True
    no_fake_same_date = same_date.get("same_date_support_not_faked") is True
    top5_primary_ok = top5_return_improved and top5_winner_capture_improved and top5_future_top10_capture_not_worse and top5_severe_ok and top5_bad_pick_ok and top5_human_selectable_ok and top5_diversity_ok
    if artifact_complete and source_applicable and no_fake_same_date and top5_primary_ok and top3_not_fatal and recovered > 0 and noise_ok and branching_ok and stable:
        decision = "keep_candidate"
        authoritative = "source_specific_candidate_generation_v2_keep_candidate"
    elif artifact_complete and source_applicable and no_fake_same_date and top5_return_improved and recovered > 0 and branching_ok:
        decision = "hold"
        authoritative = "source_specific_candidate_generation_v2_hold"
    else:
        decision = "drop"
        authoritative = "source_specific_candidate_generation_v2_drop"
    typed_reasons = [
        "top5_return_improved" if top5_return_improved else "top5_return_not_improved",
        "top5_big_winner_capture_improved" if top5_winner_capture_improved else "top5_big_winner_capture_not_improved",
        "top5_future_top10_capture_not_worse" if top5_future_top10_capture_not_worse else "top5_future_top10_capture_worse",
        "top5_severe_loss_not_worse" if top5_severe_ok else "top5_severe_loss_worse",
        "top5_bad_pick_count_not_worse" if top5_bad_pick_ok else "top5_bad_pick_count_worse",
        "top5_human_selectable_pool_not_worse" if top5_human_selectable_ok else "top5_human_selectable_pool_worse",
        "top5_source_diversity_not_overconcentrated" if top5_diversity_ok else "top5_source_diversity_overconcentrated",
        "top3_guardrail_not_fatal" if top3_not_fatal else "top3_guardrail_fatal",
        "oracle_gap_improved_secondary" if oracle_improved else "oracle_gap_not_improved_secondary",
        "nonwinner_when_winner_available_not_worse_secondary" if nonwinner_ok else "nonwinner_when_winner_available_worse_secondary",
        "top3_severe_loss_not_materially_worse_secondary" if severe_ok else "top3_severe_loss_materially_worse_secondary",
        "recovered_missed_winners_meaningful" if recovered > 0 else "recovered_missed_winners_too_few",
        "noise_ratio_acceptable" if noise_ok else "noise_ratio_not_acceptable",
        "real_branching_observed" if branching_ok else "no_real_branching",
        "time_block_effect_stable" if stable else "time_block_effect_not_stable",
        "same_date_support_not_faked" if no_fake_same_date else "same_date_support_fake_or_unknown",
        "artifact_complete" if artifact_complete else "artifact_incomplete",
    ]
    return _stamp(
        {
            "research_phase": "source_specific_candidate_generation_validation_v2",
            "boundary": "TRADEX-only",
            "axis_moved": "source_specific_candidate_generation_validation_v2",
            "source_mechanism_validation_decision": "source_mechanism_validation_next_target_ready",
            "selected_hypothesis_id": selected_hypothesis_id,
            "selected_target_source": "selected_next_validation_target.json",
            "goal": "top5_candidate_pool_quality_for_human_max3_selection",
            "primary_metric_scope": "top5_candidate_pool_quality",
            "secondary_metric_scope": "top3_operating_guardrail",
            "forced_top3_is_primary": False,
            "final_max3_selection_owner": "human_user",
            "candidate_generation_challenger_created": True,
            "candidate_generation_scope": "research_only",
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
            "per_source_same_date_support_available": False,
            "same_date_support_not_faked": no_fake_same_date,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_candidate_generation": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "typed_reasons": typed_reasons,
            "validation_outcome": outcome.get("validation_outcome"),
            "recommended_next_axis": None,
            "decision_reasons": [
                {"code": "top5_avg_ret20_improves_vs_previous_best", "status": "pass" if top5_return_improved else "fail", "value": comparison.get("top5_avg_ret20_delta_vs_previous_best")},
                {"code": "top5_big_winner_capture_rate_improves_vs_previous_best", "status": "pass" if top5_winner_capture_improved else "fail", "value": comparison.get("top5_big_winner_capture_rate_delta_vs_previous_best")},
                {"code": "top5_future_top10_capture_rate_not_worse", "status": "pass" if top5_future_top10_capture_not_worse else "fail", "value": comparison.get("top5_future_top10_capture_rate_delta_vs_previous_best")},
                {"code": "top5_severe_loss_rate_not_worse", "status": "pass" if top5_severe_ok else "fail", "value": comparison.get("top5_severe_loss_rate_delta_vs_previous_best")},
                {"code": "top5_bad_pick_count_not_worse", "status": "pass" if top5_bad_pick_ok else "fail", "value": comparison.get("top5_bad_pick_count_delta_vs_previous_best")},
                {"code": "top5_human_selectable_candidate_day_rate_not_worse", "status": "pass" if top5_human_selectable_ok else "fail", "value": comparison.get("top5_human_selectable_candidate_day_rate_delta_vs_previous_best")},
                {"code": "top5_source_concentration_not_worse", "status": "pass" if top5_diversity_ok else "fail", "value": comparison.get("top5_top_source_share_delta_vs_previous_best")},
                {"code": "selected_top3_avg_ret20_guardrail_not_fatal", "status": "pass" if top3_not_fatal else "fail", "value": top3_return_delta},
                {"code": "oracle_top3_gap_ret20_secondary", "status": "pass" if oracle_improved else "fail", "value": comparison.get("oracle_top3_gap_delta_vs_previous_best")},
                {"code": "selected_nonwinner_when_winner_available_secondary", "status": "pass" if nonwinner_ok else "fail", "value": nonwinner_delta},
                {"code": "selected_top3_severe_loss_rate_secondary", "status": "pass" if severe_ok else "fail", "value": severe_delta},
                {"code": "recovered_missed_winner_count", "status": "pass" if recovered > 0 else "fail", "value": recovered},
                {"code": "changed_top5_members_count_vs_previous_best", "status": "pass" if branching_ok else "fail", "value": branching.get("changed_top5_members_count_vs_previous_best")},
                {"code": "same_date_support_not_faked", "status": "pass" if no_fake_same_date else "fail", "value": no_fake_same_date},
                {"code": "artifact_complete", "status": "pass" if artifact_complete else "fail", "value": artifact_complete},
            ],
        },
        "research_decision",
    )


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return _stamp(
        {
            "artifact_root": str(output_dir),
            "complete": all(required.values()),
            "required_artifacts": required,
            "paths": paths,
            "decision": decision.get("decision") if decision else None,
            "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
            "primary_metric_scope": "top5_candidate_pool_quality",
            "secondary_metric_scope": "top3_operating_guardrail",
            "forced_top3_is_primary": False,
            "final_max3_selection_owner": "human_user",
            "candidate_generation_challenger_created": True,
            "candidate_generation_scope": "research_only",
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
        },
        "artifact_complete",
    )


def run_source_specific_candidate_generation_validation_v2(
    *,
    source_mechanism_validation_run_id: str = DEFAULT_MECHANISM_VALIDATION_RUN_ID,
    source_hypothesis_refresh_run_id: str = DEFAULT_HYPOTHESIS_REFRESH_RUN_ID,
    source_second_hypothesis_review_run_id: str = DEFAULT_SECOND_REVIEW_RUN_ID,
    source_missed_winner_run_id: str = DEFAULT_MISSED_WINNER_RUN_ID,
    source_root_cause_run_id: str = DEFAULT_ROOT_CAUSE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    mechanism_validation_root: str | Path = DEFAULT_MECHANISM_VALIDATION_ROOT,
    hypothesis_refresh_root: str | Path = DEFAULT_HYPOTHESIS_REFRESH_ROOT,
    second_review_root: str | Path = DEFAULT_SECOND_REVIEW_ROOT,
    missed_winner_root: str | Path = DEFAULT_MISSED_WINNER_ROOT,
    root_cause_root: str | Path = DEFAULT_ROOT_CAUSE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    mechanism_dir = _run_dir(mechanism_validation_root, source_mechanism_validation_run_id, DEFAULT_MECHANISM_VALIDATION_ROOT)
    refresh_dir = _run_dir(hypothesis_refresh_root, source_hypothesis_refresh_run_id, DEFAULT_HYPOTHESIS_REFRESH_ROOT)
    second_dir = _run_dir(second_review_root, source_second_hypothesis_review_run_id, DEFAULT_SECOND_REVIEW_ROOT)
    missed_dir = _run_dir(missed_winner_root, source_missed_winner_run_id, DEFAULT_MISSED_WINNER_ROOT)
    root_dir = _run_dir(root_cause_root, source_root_cause_run_id, DEFAULT_ROOT_CAUSE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    status = validate_sources(
        mechanism_validation_dir=mechanism_dir,
        hypothesis_refresh_dir=refresh_dir,
        second_review_dir=second_dir,
        missed_winner_dir=missed_dir,
        root_cause_dir=root_dir,
        wide_dir=wide_dir,
    )
    hypothesis = selected_hypothesis(status)
    frame = load_validation_frame(status=status, root_cause_dir=root_dir, wide_dir=wide_dir)
    target_readback = build_selected_target_readback(status, hypothesis)
    context = build_source_context_availability_audit(frame, hypothesis, status, wide_dir=wide_dir)
    same_date = build_same_date_support_limitation_report(status)
    if context.get("source_definition_applicable_point_in_time") is True:
        frame = _source_frame(frame, str(hypothesis["source_family"]))
    else:
        frame = _source_frame(frame, "__blocked_no_applicable_source_definition__")
    primary_ledger = build_selection_ledger(frame, max_source_slots=1, family_id=PRIMARY_FAMILY_ID)
    diagnostic_ledger = build_selection_ledger(frame, max_source_slots=2, family_id=DIAGNOSTIC_FAMILY_ID)
    primary_selected = _selected_from_ledger(primary_ledger)
    diagnostic_selected = _selected_from_ledger(diagnostic_ledger)
    source_candidate_rows = build_source_candidate_ledger(frame)
    overlap = build_source_overlap_audit(frame)
    source_oracle = build_source_oracle_diagnostic(frame)
    top3_report = build_top3_selection_report(frame, primary_selected, diagnostic_selected)
    top5_report = build_top5_candidate_pool_report(frame)
    recovery = build_source_recovery_report(frame, primary_selected)
    noise = build_source_noise_report(recovery)
    timeblock = build_time_block_source_validation(frame, primary_selected)
    comparison = build_baseline_comparison_report(top3_report, top5_report, recovery, noise, timeblock)
    branching = build_branching_report(top3_report)
    outcome = build_validation_outcome_classification(
        overlap=overlap,
        comparison=comparison,
        recovery=recovery,
        noise=noise,
        timeblock=timeblock,
        context=context,
        same_date=same_date,
    )
    next_axis = build_next_axis_recommendation(outcome)
    derived = _derived_source_dirs(status, root_cause_dir=root_dir, wide_dir=wide_dir)
    source_dirs = {
        "mechanism_validation": mechanism_dir,
        "hypothesis_refresh": refresh_dir,
        "second_review": second_dir,
        "missed_winner": missed_dir,
        "root_cause": root_dir,
        "wide": wide_dir,
        "derived_pattern": derived["pattern"],
        "derived_upside": derived["upside"],
        "derived_feature_diagnosis": derived["feature_diagnosis"],
    }
    contract_artifacts = build_contract_artifacts(
        source_dirs=source_dirs,
        frame=frame,
        hypothesis=hypothesis,
        selected=status["mechanism_validation"]["selected_next_validation_target.json"],
        context=context,
        same_date=same_date,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[{"name": key, "path": str(value)} for key, value in source_dirs.items()],
        asof=str(int(frame["event_ymd"].max())) if "event_ymd" in frame.columns and len(frame) else "20260513",
        config={
            "axis_id": AXIS_ID,
            "selected_hypothesis_id": hypothesis.get("hypothesis_id"),
            "source_family": hypothesis.get("source_family"),
            "goal": "top5 candidate pool quality for human max3 selection",
            "primary_metric_scope": "top5_candidate_pool_quality",
            "secondary_metric_scope": "top3_operating_guardrail",
            "forced_top3_is_primary": False,
            "final_max3_selection_owner": "human_user",
            "candidate_generation_challenger_created": True,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=sorted(frame["code"].astype(str).unique().tolist()) if "code" in frame.columns else [],
        period={
            "start_date": str(int(frame["event_ymd"].min())) if "event_ymd" in frame.columns and len(frame) else "unknown",
            "end_date": str(int(frame["event_ymd"].max())) if "event_ymd" in frame.columns and len(frame) else "unknown",
            "label": "source_specific_candidate_generation_validation_v2",
        },
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
        "selected_target_readback.json": target_readback,
        "source_context_availability_audit.json": context,
        "same_date_support_limitation_report.json": same_date,
        "source_overlap_audit.json": overlap,
        "source_oracle_diagnostic.json": source_oracle,
        "top5_candidate_pool_report.json": top5_report,
        "top3_selection_report.json": top3_report,
        "source_recovery_report.json": recovery,
        "source_noise_report.json": noise,
        "baseline_comparison_report.json": comparison,
        "branching_report.json": branching,
        "time_block_source_validation.json": timeblock,
        "validation_outcome_classification.json": outcome,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["source_candidate_ledger.jsonl"] = str(_write_jsonl(output_dir / "source_candidate_ledger.jsonl", source_candidate_rows))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        comparison=comparison,
        recovery=recovery,
        noise=noise,
        branching=branching,
        timeblock=timeblock,
        context=context,
        same_date=same_date,
        outcome=outcome,
        artifact_complete=bool(pre_complete["complete"]),
        selected_hypothesis_id=str(hypothesis.get("hypothesis_id")),
    )
    decision["recommended_next_axis"] = next_axis.get("recommended_next_axis")
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "validation_outcome": decision["validation_outcome"],
        "selected_hypothesis_id": decision["selected_hypothesis_id"],
        "recommended_next_axis": decision["recommended_next_axis"],
        "primary_metric_scope": "top5_candidate_pool_quality",
        "secondary_metric_scope": "top3_operating_guardrail",
        "forced_top3_is_primary": False,
        "final_max3_selection_owner": "human_user",
        "candidate_generation_challenger_created": True,
        "candidate_generation_scope": "research_only",
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "same_date_support_not_faked": True,
        "per_source_same_date_support_available": False,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-mechanism-validation-run-id", default=DEFAULT_MECHANISM_VALIDATION_RUN_ID)
    parser.add_argument("--source-hypothesis-refresh-run-id", default=DEFAULT_HYPOTHESIS_REFRESH_RUN_ID)
    parser.add_argument("--source-second-hypothesis-review-run-id", default=DEFAULT_SECOND_REVIEW_RUN_ID)
    parser.add_argument("--source-missed-winner-run-id", default=DEFAULT_MISSED_WINNER_RUN_ID)
    parser.add_argument("--source-root-cause-run-id", default=DEFAULT_ROOT_CAUSE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--mechanism-validation-root", default=str(DEFAULT_MECHANISM_VALIDATION_ROOT))
    parser.add_argument("--hypothesis-refresh-root", default=str(DEFAULT_HYPOTHESIS_REFRESH_ROOT))
    parser.add_argument("--second-review-root", default=str(DEFAULT_SECOND_REVIEW_ROOT))
    parser.add_argument("--missed-winner-root", default=str(DEFAULT_MISSED_WINNER_ROOT))
    parser.add_argument("--root-cause-root", default=str(DEFAULT_ROOT_CAUSE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_source_specific_candidate_generation_validation_v2(
        source_mechanism_validation_run_id=args.source_mechanism_validation_run_id,
        source_hypothesis_refresh_run_id=args.source_hypothesis_refresh_run_id,
        source_second_hypothesis_review_run_id=args.source_second_hypothesis_review_run_id,
        source_missed_winner_run_id=args.source_missed_winner_run_id,
        source_root_cause_run_id=args.source_root_cause_run_id,
        source_wide_run_id=args.source_wide_run_id,
        mechanism_validation_root=args.mechanism_validation_root,
        hypothesis_refresh_root=args.hypothesis_refresh_root,
        second_review_root=args.second_review_root,
        missed_winner_root=args.missed_winner_root,
        root_cause_root=args.root_cause_root,
        wide_root=args.wide_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
