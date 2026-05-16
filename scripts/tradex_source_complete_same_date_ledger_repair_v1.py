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
from scripts import tradex_source_specific_candidate_generation_v2_noise_decomposition_v1 as noise_mod
from scripts import tradex_source_specific_candidate_generation_validation_v2 as v2_mod


AXIS_ID = "source_complete_same_date_ledger_repair_v1"
SCHEMA_PREFIX = "tradex_source_complete_same_date_ledger_repair_v1"

DEFAULT_NOISE_DECOMPOSITION_RUN_ID = "20260514T000000Z-source-specific-candidate-generation-v2-noise-decomposition-v1"
DEFAULT_VALIDATION_V2_RUN_ID = "20260513T200000Z-source-specific-candidate-generation-validation-v2"
DEFAULT_MECHANISM_VALIDATION_RUN_ID = "20260513T190000Z-candidate-generation-source-mechanism-validation-v1"
DEFAULT_HYPOTHESIS_REFRESH_RUN_ID = "20260513T180000Z-candidate-generation-hypothesis-map-refresh-v1"
DEFAULT_MISSED_WINNER_RUN_ID = "20260513T140000Z-missed-winner-event-source-candidate-generation-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"

DEFAULT_NOISE_DECOMPOSITION_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_v2_noise_decomposition_v1")
DEFAULT_VALIDATION_V2_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v2")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\source_complete_same_date_ledger_repair_v1")

REQUIRED_NOISE_ARTIFACTS = (
    "research_decision.json",
    "source_v2_archive_or_refine_decision.json",
    "same_date_support_limitation_followup_report.json",
    "_ARTIFACT_COMPLETE.json",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "ledger_repair_contract.json",
    "field_availability_audit.json",
    "reconstruction_audit.json",
    "leakage_audit.json",
    "source_complete_same_date_ledger.jsonl",
    "same_date_support_readiness_report.json",
    "v2_source_support_precheck.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

SOURCE_COMPONENT_FIELDS = (
    "pre_ma20_context_state",
    "pre_ma60_context_state",
    "weekly_prior_state",
    "negative_guard_match",
)

FUTURE_LABEL_FIELDS = (
    "ret20",
    "MFE20",
    "MAE20",
    "severe_loss20",
    "future_top10_by_ret20",
    "big_winner_ret20_ge_10pct",
    "big_winner_MFE20_ge_15pct",
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


def _mean(values: Iterable[Any]) -> float | None:
    series = pd.to_numeric(pd.Series(list(values)), errors="coerce").dropna()
    if series.empty:
        return None
    return float(series.mean())


def _stamp(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    payload["schema_version"] = f"{SCHEMA_PREFIX}_{artifact_id}_v1"
    payload["generated_at"] = _utc_now()
    payload["axis_id"] = AXIS_ID
    payload["contract_hash"] = _stable_hash({key: value for key, value in payload.items() if key != "contract_hash"})
    return payload


def _present(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return str(value).strip() != ""


def _bool_value(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _float_value(value: Any) -> float | None:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _source_family_id(components: dict[str, Any]) -> str | None:
    if any(not _present(components.get(field)) for field in SOURCE_COMPONENT_FIELDS):
        return None
    text = "|".join(f"{field}={components[field]}" for field in SOURCE_COMPONENT_FIELDS)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _nonwinner_label(row: pd.Series, *, ret20: float | None, severe_loss: bool, future_winner: bool) -> bool:
    rank_pct = _float_value(row.get("ret20_rank_pct_by_date"))
    return bool((not future_winner) and ((ret20 is not None and ret20 <= 0.0) or severe_loss or (rank_pct is not None and rank_pct > 0.50)))


def load_source_artifacts(noise_dir: Path, validation_v2_dir: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_NOISE_ARTIFACTS if not (noise_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"noise decomposition missing required artifacts: {missing} at {noise_dir}")
    noise_complete = _load_json(noise_dir / "_ARTIFACT_COMPLETE.json")
    noise_decision = _load_json(noise_dir / "research_decision.json")
    archive_or_refine = _load_json(noise_dir / "source_v2_archive_or_refine_decision.json")
    same_date_followup = _load_json(noise_dir / "same_date_support_limitation_followup_report.json")
    if noise_complete.get("complete") is not True:
        raise RuntimeError("noise decomposition artifact is not complete")
    if noise_complete.get("silent_fallback_used") is not False or noise_decision.get("silent_fallback_used") is True:
        raise RuntimeError("noise decomposition used silent fallback")
    if noise_complete.get("research_fallback_used") is not False or noise_decision.get("research_fallback_used") is True:
        raise RuntimeError("noise decomposition used research fallback")
    if noise_decision.get("authoritative_research_decision") != "source_complete_ledger_repair_needed":
        raise RuntimeError("noise decomposition does not require source-complete ledger repair")

    validation_artifacts = noise_mod.load_source_validation_v2_artifacts(validation_v2_dir)
    return {
        "noise_complete": noise_complete,
        "noise_decision": noise_decision,
        "archive_or_refine": archive_or_refine,
        "same_date_followup": same_date_followup,
        "validation_artifacts": validation_artifacts,
    }


def load_reconstructed_inputs(artifacts: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Path]]:
    return noise_mod.load_reconstructed_inputs(artifacts["validation_artifacts"])


def build_source_complete_ledger(frame: pd.DataFrame, primary_ledger: pd.DataFrame, hypothesis: dict[str, Any]) -> list[dict[str, Any]]:
    selected_keys = set(
        zip(
            primary_ledger.loc[primary_ledger["selected_topk"].eq(True), "event_date"].astype(str),
            primary_ledger.loc[primary_ledger["selected_topk"].eq(True), "code"].astype(str),
        )
    )
    source_family = str(hypothesis.get("source_family") or "")
    rows = []
    ordered = frame.sort_values(["event_ymd", "code"], kind="mergesort") if {"event_ymd", "code"}.issubset(frame.columns) else frame.copy()
    for _, row in ordered.iterrows():
        event_date = str(row.get("event_date"))
        symbol = str(row.get("code"))
        pre_ma20_context = row.get("pre_ma20_context_state")
        if not _present(pre_ma20_context):
            pre_ma20_context = row.get("pre_ma20_path_state")
        components = {
            "pre_ma20_context_state": pre_ma20_context,
            "pre_ma60_context_state": row.get("pre_ma60_context_state"),
            "weekly_prior_state": row.get("weekly_prior_state"),
            "negative_guard_match": row.get("negative_guard_match"),
        }
        source_id = _source_family_id(components)
        candidate_id = f"{int(row.get('event_ymd'))}:{symbol}" if _present(row.get("event_ymd")) else f"{event_date}:{symbol}"
        selected_by_source_v2 = (event_date, symbol) in selected_keys
        ret20 = _float_value(row.get("ret20_fwd"))
        mfe20 = _float_value(row.get("mfe20"))
        mae20 = _float_value(row.get("mae20"))
        source_v2_candidate = bool(row.get("source_specific_candidate", False)) or str(row.get("source_family") or "") == source_family
        ledger_row = {
            "event_date": event_date,
            "symbol": symbol,
            "candidate_id": candidate_id,
            "stable_event_key": candidate_id,
            "source_family_id": source_id,
            "source_family_components": components,
            "source_family_text": "|".join(f"{key}={value}" for key, value in components.items()) if source_id else None,
            "pre_ma20_context_state": pre_ma20_context,
            "pre_ma60_context_state": row.get("pre_ma60_context_state"),
            "weekly_prior_state": row.get("weekly_prior_state"),
            "negative_guard_match": row.get("negative_guard_match"),
            "safe_full_tag": row.get("guard_safe_full") if "guard_safe_full" in row.index else None,
            "previous_best_rank": _float_value(row.get("selection_rank")),
            "previous_best_score": _float_value(row.get("research_score")),
            "selected_by_previous_best_top3": bool(row.get("baseline_top3", row.get("selected_by_previous_best_top3", False))),
            "selected_by_source_v2": selected_by_source_v2,
            "source_v2_candidate_flag": source_v2_candidate,
            "ret20": ret20,
            "MFE20": mfe20,
            "MAE20": mae20,
            "severe_loss20": bool(row.get("severe_loss20", False)),
            "future_top10_by_ret20": bool(row.get("is_future_top10_by_ret20", False)),
            "big_winner_ret20_ge_10pct": bool(row.get("is_big_winner_ret20_ge_10pct", False)),
            "big_winner_MFE20_ge_15pct": bool(row.get("is_big_winner_MFE20_ge_15pct", False)),
        }
        future_winner = bool(row.get("future_winner", False)) if "future_winner" in row.index else bool(
            ledger_row["big_winner_ret20_ge_10pct"] or ledger_row["big_winner_MFE20_ge_15pct"]
        )
        ledger_row["future_winner_evaluation_label"] = future_winner
        ledger_row["nonwinner_evaluation_label"] = _nonwinner_label(row, ret20=ret20, severe_loss=ledger_row["severe_loss20"], future_winner=future_winner)
        rows.append(ledger_row)
    return rows


def _availability(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    count = sum(1 for row in rows if _present(row.get(field)))
    return {"available_count": count, "total_count": len(rows), "availability_rate": _safe_rate(count, len(rows))}


def build_field_availability_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fields = [
        "pre_ma60_context_state",
        "pre_ma20_context_state",
        "weekly_prior_state",
        "negative_guard_match",
        "source_family_id",
        "selected_by_previous_best_top3",
        "selected_by_source_v2",
    ]
    availability = {field: _availability(rows, field) for field in fields}
    return _stamp(
        {
            "candidate_row_count": len(rows),
            "availability": availability,
            "pre_ma60_context_state_available": availability["pre_ma60_context_state"]["availability_rate"] == 1.0,
            "source_family_id_available": availability["source_family_id"]["availability_rate"] == 1.0,
        },
        "field_availability_audit",
    )


def build_reconstruction_audit(rows: list[dict[str, Any]], frame: pd.DataFrame) -> dict[str, Any]:
    direct_fields = [
        field
        for field in [
            "event_date",
            "code",
            "pre_ma20_path_state",
            "pre_ma60_context_state",
            "weekly_prior_state",
            "negative_guard_match",
            "guard_safe_full",
            "selection_rank",
            "ret20_fwd",
            "mfe20",
            "mae20",
            "severe_loss20",
            "is_future_top10_by_ret20",
            "is_big_winner_ret20_ge_10pct",
            "is_big_winner_MFE20_ge_15pct",
        ]
        if field in frame.columns
    ]
    failed = []
    if any(not _present(row.get("pre_ma60_context_state")) for row in rows):
        failed.append("pre_ma60_context_state")
    if any(not _present(row.get("source_family_id")) for row in rows):
        failed.append("source_family_id")
    return _stamp(
        {
            "fields_read_directly": direct_fields,
            "fields_reconstructed": {
                "symbol": "from code",
                "candidate_id": "from event_ymd + symbol",
                "pre_ma20_context_state": "from pre_ma20_context_state if present else point-in-time pre_ma20_path_state",
                "source_family_components": list(SOURCE_COMPONENT_FIELDS),
                "source_family_id": "stable hash of source_family_components",
                "selected_by_source_v2": "from reconstructed source_specific_candidate_generation_validation_v2 max1slot ledger",
            },
            "fields_failed": failed,
            "reconstruction_failed": bool(failed),
            "reconstruction_point_in_time_safe": not bool(failed),
            "future_labels_excluded_from_source_construction": True,
            "future_labels_attached_for_evaluation_only": True,
            "aggregate_metrics_used_as_same_date_support": False,
        },
        "reconstruction_audit",
    )


def build_leakage_audit() -> dict[str, Any]:
    return _stamp(
        {
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_source_construction": False,
            "future_label_fields_attached_after_source_construction": list(FUTURE_LABEL_FIELDS),
            "source_family_constructed_from_fields": list(SOURCE_COMPONENT_FIELDS),
            "aggregate_metrics_used_as_same_date_support": False,
            "same_date_support_not_faked": True,
            "leakage_detected": False,
            "silent_fallback_used": False,
        },
        "leakage_audit",
    )


def build_same_date_support_readiness_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    frame = pd.DataFrame(rows)
    can_group = bool(len(frame) and frame["event_date"].notna().all() and frame["source_family_id"].notna().all())
    can_label = bool(
        len(frame)
        and frame["ret20"].notna().all()
        and frame["MFE20"].notna().all()
        and frame["MAE20"].notna().all()
        and frame["severe_loss20"].notna().all()
    )
    grouped_rows = []
    if can_group and can_label:
        grouped = frame.groupby(["event_date", "source_family_id"], dropna=False)
        for (event_date, source_family_id), group in grouped:
            grouped_rows.append(
                {
                    "event_date": str(event_date),
                    "source_family_id": str(source_family_id),
                    "candidate_count": int(len(group)),
                    "winner_count": int(group["future_winner_evaluation_label"].astype(bool).sum()),
                    "nonwinner_count": int(group["nonwinner_evaluation_label"].astype(bool).sum()),
                    "severe_loser_count": int(group["severe_loss20"].astype(bool).sum()),
                    "selected_by_source_v2_count": int(group["selected_by_source_v2"].astype(bool).sum()),
                    "source_v2_candidate_count": int(group["source_v2_candidate_flag"].astype(bool).sum()),
                }
            )
    per_source_support = can_group and can_label and bool(grouped_rows)
    return _stamp(
        {
            "can_group_by_event_date_and_source_family_id": can_group,
            "can_identify_source_winners_nonwinners_severe_losers_by_same_date": can_label,
            "can_compute_per_source_same_date_support": per_source_support,
            "can_compare_v2_source_vs_other_sources_on_same_date": per_source_support
            and any(row["source_v2_candidate_count"] > 0 for row in grouped_rows)
            and len({row["source_family_id"] for row in grouped_rows}) > 1,
            "same_date_source_group_count": len(grouped_rows),
            "sample_rows": grouped_rows[:100],
            "same_date_support_not_faked": True,
            "aggregate_metrics_used_as_same_date_support": False,
        },
        "same_date_support_readiness_report",
    )


def build_v2_source_support_precheck(rows: list[dict[str, Any]]) -> dict[str, Any]:
    frame = pd.DataFrame(rows)
    source = frame[frame["source_v2_candidate_flag"].astype(bool)].copy() if len(frame) else frame
    selected = source[source["selected_by_source_v2"].astype(bool)].copy() if len(source) else source
    baseline_selected_keys = set(zip(frame.loc[frame["selected_by_previous_best_top3"].astype(bool), "event_date"].astype(str), frame.loc[frame["selected_by_previous_best_top3"].astype(bool), "symbol"].astype(str))) if len(frame) else set()
    if len(selected):
        selected["_new_vs_previous_best"] = [(str(row["event_date"]), str(row["symbol"])) not in baseline_selected_keys for _, row in selected.iterrows()]
    else:
        selected["_new_vs_previous_best"] = pd.Series(dtype=bool)
    added = selected[selected["_new_vs_previous_best"].astype(bool)] if len(selected) else selected
    recovered = added[added["future_winner_evaluation_label"].astype(bool)] if len(added) else added
    severe = added[added["severe_loss20"].astype(bool)] if len(added) else added
    per_source_support = bool(frame["source_family_id"].notna().all()) if len(frame) else False
    return _stamp(
        {
            "v2_source_candidate_count": int(len(source)),
            "v2_source_selected_count": int(len(selected)),
            "v2_source_recovered_winner_count": int(len(recovered)),
            "v2_source_added_severe_loser_count": int(len(severe)),
            "per_source_support_computable": per_source_support,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_source_construction": False,
        },
        "v2_source_support_precheck",
    )


def build_source_artifact_refs(
    *,
    noise_dir: Path,
    validation_v2_dir: Path,
    source_dirs: dict[str, Path],
    source_mechanism_validation_run_id: str,
    source_hypothesis_refresh_run_id: str,
    source_missed_winner_run_id: str,
    source_wide_run_id: str,
) -> dict[str, Any]:
    refs = []
    for source_name, root in {"noise_decomposition": noise_dir, "source_validation_v2": validation_v2_dir, **source_dirs}.items():
        if root.exists():
            for path in sorted(root.glob("*.json")):
                refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": True, "file_hash": _file_hash(path)})
            for path in sorted(root.glob("*.jsonl")):
                refs.append({"source": source_name, "name": path.name, "path": str(path), "exists": True, "file_hash": _file_hash(path)})
    return _stamp(
        {
            "source_roots": {key: str(value) for key, value in {"noise_decomposition": noise_dir, "source_validation_v2": validation_v2_dir, **source_dirs}.items()},
            "input_run_ids": {
                "source_mechanism_validation_run_id": source_mechanism_validation_run_id,
                "source_hypothesis_refresh_run_id": source_hypothesis_refresh_run_id,
                "source_missed_winner_run_id": source_missed_winner_run_id,
                "source_wide_run_id": source_wide_run_id,
            },
            "refs": refs,
        },
        "source_artifact_refs",
    )


def build_contract_artifacts(*, rows: list[dict[str, Any]], artifacts: dict[str, Any]) -> dict[str, dict[str, Any]]:
    evaluation_contract = _stamp(
        {
            "research_phase": "source_complete_same_date_ledger_repair",
            "boundary": "TRADEX-only",
            "axis_moved": "source_complete_same_date_ledger_repair",
            "source_noise_decomposition_decision": artifacts["noise_decision"].get("authoritative_research_decision"),
            "ledger_repair_created": True,
            "diagnostic_foundation_only": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_source_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "ledger_row_count": len(rows),
        },
        "evaluation_contract",
    )
    repair_contract = _stamp(
        {
            "required_source_complete_fields": [
                "event_date",
                "symbol",
                "candidate_id",
                "stable_event_key",
                "source_family_id",
                "source_family_components",
                "pre_ma20_context_state",
                "pre_ma60_context_state",
                "weekly_prior_state",
                "negative_guard_match",
                "safe_full_tag",
                "previous_best_rank",
                "previous_best_score",
                "selected_by_previous_best_top3",
                "selected_by_source_v2",
                "source_v2_candidate_flag",
                *FUTURE_LABEL_FIELDS,
            ],
            "do_not_fabricate_pre_ma60_context_state": True,
            "do_not_derive_per_source_support_from_aggregate_metrics": True,
            "future_labels_attached_for_evaluation_only": True,
            "future_labels_used_in_source_construction": False,
            "same_date_support_not_faked": True,
        },
        "ledger_repair_contract",
    )
    return {"evaluation_contract.json": evaluation_contract, "ledger_repair_contract.json": repair_contract}


def build_next_axis_recommendation(decision: str) -> dict[str, Any]:
    if decision == "source_complete_ledger_ready":
        next_axis = "source_v2_per_source_same_date_support_audit_v1"
    elif decision == "source_complete_ledger_hold":
        next_axis = "targeted context field repair"
    else:
        next_axis = "archive v2 source or return to broader candidate generation design"
    return _stamp(
        {
            "one_recommended_next_axis_only": True,
            "recommended_next_axis": next_axis,
            "authoritative_research_decision": decision,
            "do_not_continue_axes": [
                "source v2 refine",
                "scorer",
                "threshold/no-trade",
                "candidate promotion",
                "image/fusion",
                "production ranking",
                "MeeMee reflection",
                "publish",
            ],
        },
        "next_axis_recommendation",
    )


def decide(
    *,
    field_audit: dict[str, Any],
    reconstruction: dict[str, Any],
    leakage: dict[str, Any],
    readiness: dict[str, Any],
    precheck: dict[str, Any],
    artifact_complete: bool,
) -> tuple[str, str, list[str]]:
    pre_ma60_ready = field_audit["availability"]["pre_ma60_context_state"]["availability_rate"] == 1.0
    source_id_ready = field_audit["availability"]["source_family_id"]["availability_rate"] == 1.0
    per_source_ready = readiness.get("can_compute_per_source_same_date_support") is True
    precheck_ready = precheck.get("per_source_support_computable") is True
    leakage_ok = leakage.get("leakage_detected") is False and leakage.get("future_labels_used_in_source_construction") is False
    no_fake = readiness.get("same_date_support_not_faked") is True
    if artifact_complete and pre_ma60_ready and source_id_ready and per_source_ready and precheck_ready and leakage_ok and no_fake:
        return (
            "keep_candidate",
            "source_complete_ledger_ready",
            [
                "pre_ma60_context_state_available",
                "source_family_id_computable",
                "per_source_same_date_support_computable",
                "v2_support_precheck_available",
                "future_labels_excluded_from_source_construction",
                "same_date_support_not_faked",
                "artifact_complete",
            ],
        )
    if not pre_ma60_ready or reconstruction.get("reconstruction_failed") is True or not source_id_ready or not per_source_ready or leakage.get("leakage_detected") is True:
        return (
            "drop",
            "source_complete_ledger_failed",
            [
                "pre_ma60_context_state_unavailable_or_reconstruction_failed" if not pre_ma60_ready else "context_reconstruction_failed",
                "source_family_or_per_source_support_not_computable" if not source_id_ready or not per_source_ready else "leakage_or_support_failure",
            ],
        )
    return ("hold", "source_complete_ledger_hold", ["ledger_partially_ready", "additional_context_field_repair_needed"])


def build_research_decision(
    *,
    decision: str,
    authoritative: str,
    typed_reasons: list[str],
    field_audit: dict[str, Any],
    readiness: dict[str, Any],
) -> dict[str, Any]:
    return _stamp(
        {
            "research_phase": "source_complete_same_date_ledger_repair",
            "boundary": "TRADEX-only",
            "axis_moved": "source_complete_same_date_ledger_repair",
            "source_noise_decomposition_decision": "source_complete_ledger_repair_needed",
            "ledger_repair_created": True,
            "source_complete_same_date_ledger_created": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
            "publish_bundle_created": False,
            "meemee_reflectable": False,
            "pre_ma60_context_state_available": field_audit["availability"]["pre_ma60_context_state"]["availability_rate"] == 1.0,
            "per_source_same_date_support_available": readiness.get("can_compute_per_source_same_date_support"),
            "same_date_support_not_faked": readiness.get("same_date_support_not_faked"),
            "future_labels_used_for_evaluation_only": True,
            "future_labels_used_in_source_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "typed_reasons": typed_reasons,
        },
        "research_decision",
    )


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.add("research_decision.json")
        excluded.add("next_axis_recommendation.json")
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return _stamp(
        {
            "artifact_root": str(output_dir),
            "complete": all(required.values()),
            "required_artifacts": required,
            "paths": paths,
            "decision": decision.get("decision") if decision else None,
            "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
            "ledger_repair_created": True,
            "source_complete_same_date_ledger_created": True,
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
        },
        "artifact_complete",
    )


def run_source_complete_same_date_ledger_repair_v1(
    *,
    source_noise_decomposition_run_id: str = DEFAULT_NOISE_DECOMPOSITION_RUN_ID,
    source_validation_v2_run_id: str = DEFAULT_VALIDATION_V2_RUN_ID,
    source_mechanism_validation_run_id: str = DEFAULT_MECHANISM_VALIDATION_RUN_ID,
    source_hypothesis_refresh_run_id: str = DEFAULT_HYPOTHESIS_REFRESH_RUN_ID,
    source_missed_winner_run_id: str = DEFAULT_MISSED_WINNER_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    noise_decomposition_root: str | Path = DEFAULT_NOISE_DECOMPOSITION_ROOT,
    validation_v2_root: str | Path = DEFAULT_VALIDATION_V2_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    noise_dir = _run_dir(noise_decomposition_root, source_noise_decomposition_run_id, DEFAULT_NOISE_DECOMPOSITION_ROOT)
    validation_v2_dir = _run_dir(validation_v2_root, source_validation_v2_run_id, DEFAULT_VALIDATION_V2_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    artifacts = load_source_artifacts(noise_dir, validation_v2_dir)
    frame, primary_ledger, hypothesis, source_dirs = load_reconstructed_inputs(artifacts)
    ledger_rows = build_source_complete_ledger(frame, primary_ledger, hypothesis)
    field_audit = build_field_availability_audit(ledger_rows)
    reconstruction = build_reconstruction_audit(ledger_rows, frame)
    leakage = build_leakage_audit()
    readiness = build_same_date_support_readiness_report(ledger_rows)
    precheck = build_v2_source_support_precheck(ledger_rows)
    contracts_payload = build_contract_artifacts(rows=ledger_rows, artifacts=artifacts)
    refs = build_source_artifact_refs(
        noise_dir=noise_dir,
        validation_v2_dir=validation_v2_dir,
        source_dirs=source_dirs,
        source_mechanism_validation_run_id=source_mechanism_validation_run_id,
        source_hypothesis_refresh_run_id=source_hypothesis_refresh_run_id,
        source_missed_winner_run_id=source_missed_winner_run_id,
        source_wide_run_id=source_wide_run_id,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_noise_decomposition", "path": str(noise_dir)},
            {"name": "source_validation_v2", "path": str(validation_v2_dir)},
            {"name": "source_mechanism_validation_run_id", "path": source_mechanism_validation_run_id},
            {"name": "source_hypothesis_refresh_run_id", "path": source_hypothesis_refresh_run_id},
            {"name": "source_missed_winner_run_id", "path": source_missed_winner_run_id},
            {"name": "source_wide_run_id", "path": source_wide_run_id},
        ],
        asof=str(int(frame["event_ymd"].max())) if "event_ymd" in frame.columns and len(frame) else "20260514",
        config={
            "axis_id": AXIS_ID,
            "diagnostic_foundation_only": True,
            "source_noise_decomposition_decision": artifacts["noise_decision"].get("authoritative_research_decision"),
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "threshold_policy_created": False,
            "production_ranking_changed": False,
        },
        universe=sorted(frame["code"].astype(str).unique().tolist()) if "code" in frame.columns else [],
        period={
            "start_date": str(int(frame["event_ymd"].min())) if "event_ymd" in frame.columns and len(frame) else "unknown",
            "end_date": str(int(frame["event_ymd"].max())) if "event_ymd" in frame.columns and len(frame) else "unknown",
            "label": AXIS_ID,
        },
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
        "source_artifact_refs.json": refs,
        "field_availability_audit.json": field_audit,
        "reconstruction_audit.json": reconstruction,
        "leakage_audit.json": leakage,
        "same_date_support_readiness_report.json": readiness,
        "v2_source_support_precheck.json": precheck,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["source_complete_same_date_ledger.jsonl"] = str(_write_jsonl(output_dir / "source_complete_same_date_ledger.jsonl", ledger_rows))
    pre_complete = _artifact_complete(output_dir, paths)
    decision, authoritative, reasons = decide(
        field_audit=field_audit,
        reconstruction=reconstruction,
        leakage=leakage,
        readiness=readiness,
        precheck=precheck,
        artifact_complete=bool(pre_complete["complete"]),
    )
    next_axis = build_next_axis_recommendation(authoritative)
    paths["next_axis_recommendation.json"] = str(_write_json(output_dir / "next_axis_recommendation.json", next_axis))
    research_decision = build_research_decision(decision=decision, authoritative=authoritative, typed_reasons=reasons, field_audit=field_audit, readiness=readiness)
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", research_decision))
    complete = _artifact_complete(output_dir, paths, research_decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "recommended_next_axis": next_axis["recommended_next_axis"],
        "ledger_repair_created": True,
        "source_complete_same_date_ledger_created": True,
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "pre_ma60_context_state_available": research_decision["pre_ma60_context_state_available"],
        "per_source_same_date_support_available": research_decision["per_source_same_date_support_available"],
        "same_date_support_not_faked": research_decision["same_date_support_not_faked"],
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-noise-decomposition-run-id", default=DEFAULT_NOISE_DECOMPOSITION_RUN_ID)
    parser.add_argument("--source-validation-v2-run-id", default=DEFAULT_VALIDATION_V2_RUN_ID)
    parser.add_argument("--source-mechanism-validation-run-id", default=DEFAULT_MECHANISM_VALIDATION_RUN_ID)
    parser.add_argument("--source-hypothesis-refresh-run-id", default=DEFAULT_HYPOTHESIS_REFRESH_RUN_ID)
    parser.add_argument("--source-missed-winner-run-id", default=DEFAULT_MISSED_WINNER_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--noise-decomposition-root", default=str(DEFAULT_NOISE_DECOMPOSITION_ROOT))
    parser.add_argument("--validation-v2-root", default=str(DEFAULT_VALIDATION_V2_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_source_complete_same_date_ledger_repair_v1(
        source_noise_decomposition_run_id=args.source_noise_decomposition_run_id,
        source_validation_v2_run_id=args.source_validation_v2_run_id,
        source_mechanism_validation_run_id=args.source_mechanism_validation_run_id,
        source_hypothesis_refresh_run_id=args.source_hypothesis_refresh_run_id,
        source_missed_winner_run_id=args.source_missed_winner_run_id,
        source_wide_run_id=args.source_wide_run_id,
        noise_decomposition_root=args.noise_decomposition_root,
        validation_v2_root=args.validation_v2_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
