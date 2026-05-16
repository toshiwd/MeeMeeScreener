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
from scripts import tradex_source_specific_candidate_generation_validation_v1 as v1_mod
from scripts import tradex_source_specific_candidate_generation_validation_v2 as v2_mod


AXIS_ID = "source_specific_candidate_generation_v2_noise_decomposition_v1"
SCHEMA_PREFIX = "tradex_source_specific_candidate_generation_v2_noise_decomposition_v1"

DEFAULT_VALIDATION_V2_RUN_ID = "20260513T200000Z-source-specific-candidate-generation-validation-v2"
DEFAULT_MECHANISM_VALIDATION_RUN_ID = "20260513T190000Z-candidate-generation-source-mechanism-validation-v1"
DEFAULT_HYPOTHESIS_REFRESH_RUN_ID = "20260513T180000Z-candidate-generation-hypothesis-map-refresh-v1"

DEFAULT_VALIDATION_V2_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v2")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_v2_noise_decomposition_v1")

REQUIRED_SOURCE_ARTIFACTS = (
    "research_decision.json",
    "source_candidate_ledger.jsonl",
    "source_recovery_report.json",
    "source_noise_report.json",
    "baseline_comparison_report.json",
    "branching_report.json",
    "same_date_support_limitation_report.json",
    "time_block_source_validation.json",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "noise_decomposition_contract.json",
    "source_v2_grouped_outcome_report.json",
    "recovered_winner_vs_added_nonwinner_report.json",
    "recovered_winner_vs_added_severe_loser_report.json",
    "point_in_time_noise_proxy_report.json",
    "time_block_noise_report.json",
    "max1slot_displacement_error_report.json",
    "same_date_support_limitation_followup_report.json",
    "source_v2_archive_or_refine_decision.json",
    "next_axis_recommendation.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

FUTURE_LABEL_FIELDS = {
    "ret20_fwd",
    "mfe20",
    "mae20",
    "win20",
    "severe_loss20",
    "future_winner",
    "nonwinner",
    "is_future_top10_by_ret20",
    "is_future_top5_by_ret20",
    "is_big_winner_ret20_ge_10pct",
    "is_big_winner_MFE20_ge_15pct",
    "ret20_rank_pct_by_date",
}

OBSERVABLE_FEATURES = (
    "previous_best_rank_bucket",
    "pre_ma20_path_state",
    "pre_ma60_context_state",
    "weekly_prior_state",
    "monthly_prior_state",
    "pre_ret20_state",
    "pre_ret5_state",
    "negative_guard_match",
    "guard_safe_full",
    "source_candidate_count_bucket",
    "baseline_top5",
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


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


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


def _rank_bucket(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unranked"
    if numeric <= 3:
        return "rank_1_3"
    if numeric <= 5:
        return "rank_4_5"
    if numeric <= 10:
        return "rank_6_10"
    if numeric <= 20:
        return "rank_11_20"
    return "rank_gt20"


def _count_bucket(value: Any) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return "unknown"
    if numeric <= 1:
        return "source_count_1"
    if numeric <= 3:
        return "source_count_2_3"
    if numeric <= 6:
        return "source_count_4_6"
    return "source_count_gt6"


def load_source_validation_v2_artifacts(validation_v2_dir: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_SOURCE_ARTIFACTS if not (validation_v2_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"source validation v2 missing required artifacts: {missing} at {validation_v2_dir}")
    complete_path = validation_v2_dir / "_ARTIFACT_COMPLETE.json"
    refs_path = validation_v2_dir / "source_artifact_refs.json"
    if not complete_path.exists():
        raise FileNotFoundError(f"source validation v2 missing _ARTIFACT_COMPLETE.json at {validation_v2_dir}")
    if not refs_path.exists():
        raise FileNotFoundError(f"source validation v2 missing source_artifact_refs.json at {validation_v2_dir}")

    complete = _load_json(complete_path)
    if complete.get("complete") is not True:
        raise RuntimeError("source validation v2 artifact is not complete")
    if complete.get("silent_fallback_used") is not False:
        raise RuntimeError("source validation v2 used silent fallback")
    if complete.get("research_fallback_used") is not False:
        raise RuntimeError("source validation v2 used research fallback")

    artifacts: dict[str, Any] = {
        "_ARTIFACT_COMPLETE.json": complete,
        "source_artifact_refs.json": _load_json(refs_path),
    }
    for name in REQUIRED_SOURCE_ARTIFACTS:
        path = validation_v2_dir / name
        artifacts[name] = _load_json(path) if path.suffix == ".json" else _load_jsonl(path)

    decision = artifacts["research_decision.json"]
    if decision.get("authoritative_research_decision") != "source_specific_candidate_generation_v2_hold":
        raise RuntimeError("source validation v2 decision is not source_specific_candidate_generation_v2_hold")
    if decision.get("same_date_support_not_faked") is not True:
        raise RuntimeError("source validation v2 same-date support was not explicitly non-faked")
    return artifacts


def load_reconstructed_inputs(artifacts: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Path]]:
    source_roots = artifacts["source_artifact_refs.json"].get("source_roots") or {}
    required_roots = ("mechanism_validation", "hypothesis_refresh", "second_review", "missed_winner", "root_cause", "wide")
    missing = [name for name in required_roots if not source_roots.get(name)]
    if missing:
        raise RuntimeError(f"source_artifact_refs.json missing source_roots: {missing}")
    source_dirs = {name: Path(source_roots[name]) for name in source_roots}
    status = v2_mod.validate_sources(
        mechanism_validation_dir=source_dirs["mechanism_validation"],
        hypothesis_refresh_dir=source_dirs["hypothesis_refresh"],
        second_review_dir=source_dirs["second_review"],
        missed_winner_dir=source_dirs["missed_winner"],
        root_cause_dir=source_dirs["root_cause"],
        wide_dir=source_dirs["wide"],
    )
    hypothesis = v2_mod.selected_hypothesis(status)
    frame = v2_mod.load_validation_frame(status=status, root_cause_dir=source_dirs["root_cause"], wide_dir=source_dirs["wide"])
    frame = v2_mod._source_frame(frame, str(hypothesis["source_family"]))
    primary_ledger = v2_mod.build_selection_ledger(frame, max_source_slots=1, family_id=v2_mod.PRIMARY_FAMILY_ID)
    return frame, primary_ledger, hypothesis, source_dirs


def _prepare_selected_source(frame: pd.DataFrame, primary_ledger: pd.DataFrame) -> pd.DataFrame:
    baseline_codes = set(zip(frame.loc[frame["baseline_top3"], "event_date"].astype(str), frame.loc[frame["baseline_top3"], "code"].astype(str)))
    source_counts = frame[frame["source_specific_candidate"]].groupby("event_date")["code"].count().to_dict()
    selected = primary_ledger[primary_ledger["selected_topk"].eq(True) & primary_ledger["source_specific_candidate"].eq(True)].copy()
    selected["key"] = list(zip(selected["event_date"].astype(str), selected["code"].astype(str)))
    selected["newly_selected_vs_previous_best"] = [key not in baseline_codes for key in selected["key"]]
    selected["recovered_missed_winner"] = selected["newly_selected_vs_previous_best"] & selected["future_winner"].astype(bool)
    selected["added_nonwinner"] = selected["newly_selected_vs_previous_best"] & v1_mod._nonwinner_mask(selected)
    selected["added_severe_loser"] = selected["newly_selected_vs_previous_best"] & selected["severe_loss20"].astype(bool)
    selected["neutral_other"] = selected["newly_selected_vs_previous_best"] & ~(
        selected["recovered_missed_winner"] | selected["added_nonwinner"] | selected["added_severe_loser"]
    )
    selected["exclusive_outcome_group"] = np.select(
        [
            selected["recovered_missed_winner"],
            selected["added_severe_loser"],
            selected["added_nonwinner"],
            selected["neutral_other"],
        ],
        ["recovered_missed_winner", "added_severe_loser", "added_nonwinner", "neutral_other"],
        default="selected_existing_or_other",
    )
    selected["previous_best_rank_bucket"] = selected["previous_best_selection_rank"].map(_rank_bucket)
    selected["source_candidate_count_by_date"] = selected["event_date"].map(source_counts).fillna(0).astype(int)
    selected["source_candidate_count_bucket"] = selected["source_candidate_count_by_date"].map(_count_bucket)
    selected["baseline_top5"] = selected["previous_best_selection_rank"].le(5).fillna(False).astype(bool).astype(str)
    if "time_block" not in selected.columns:
        selected["time_block"] = selected["event_date"].astype(str).str.slice(0, 4)
    return selected


def _group_metrics(group: pd.DataFrame) -> dict[str, Any]:
    return {
        "count": int(len(group)),
        "avg_ret20": _mean(group.get("ret20_fwd", [])),
        "avg_mfe20": _mean(group.get("mfe20", [])),
        "avg_mae20": _mean(group.get("mae20", [])),
        "win_rate20": float(pd.to_numeric(group.get("ret20_fwd", pd.Series(dtype=float)), errors="coerce").gt(0.0).mean()) if len(group) else 0.0,
        "future_winner_rate": float(group.get("future_winner", pd.Series(dtype=bool)).astype(bool).mean()) if len(group) else 0.0,
        "severe_loss_rate20": float(group.get("severe_loss20", pd.Series(dtype=bool)).astype(bool).mean()) if len(group) else 0.0,
        "high_mfe_weak_ret20_count": int(
            (
                pd.to_numeric(group.get("mfe20", pd.Series(dtype=float)), errors="coerce").ge(0.15)
                & pd.to_numeric(group.get("ret20_fwd", pd.Series(dtype=float)), errors="coerce").lt(0.10)
            ).sum()
        )
        if len(group)
        else 0,
    }


def build_source_v2_grouped_outcome_report(selected_source: pd.DataFrame, artifacts: dict[str, Any]) -> dict[str, Any]:
    newly = selected_source[selected_source["newly_selected_vs_previous_best"]].copy()
    groups = {
        "recovered_missed_winner": selected_source[selected_source["recovered_missed_winner"]],
        "added_nonwinner": selected_source[selected_source["added_nonwinner"]],
        "added_severe_loser": selected_source[selected_source["added_severe_loser"]],
        "neutral_other": selected_source[selected_source["neutral_other"]],
    }
    report = {
        "selected_source_candidate_count": int(len(selected_source)),
        "newly_selected_source_candidate_count": int(len(newly)),
        "groups": {name: _group_metrics(group) for name, group in groups.items()},
        "exclusive_group_counts": {str(key): int(value) for key, value in newly["exclusive_outcome_group"].value_counts().sort_index().items()},
        "reconciles_with_source_recovery_report": {
            "recovered_missed_winner_count_matches": int(groups["recovered_missed_winner"].shape[0])
            == int(artifacts["source_recovery_report.json"].get("recovered_missed_winner_count") or 0),
            "added_nonwinner_count_matches": int(groups["added_nonwinner"].shape[0])
            == int(artifacts["source_recovery_report.json"].get("source_candidate_added_nonwinner_count") or 0),
            "added_severe_loser_count_matches": int(groups["added_severe_loser"].shape[0])
            == int(artifacts["source_recovery_report.json"].get("source_candidate_added_severe_loser_count") or 0),
        },
        "source_validation_v2_decision": artifacts["research_decision.json"].get("authoritative_research_decision"),
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
    }
    return _stamp(report, "source_v2_grouped_outcome_report")


def _feature_contrast(group_a: pd.DataFrame, group_b: pd.DataFrame, *, group_a_name: str, group_b_name: str) -> dict[str, Any]:
    rows = []
    for feature in OBSERVABLE_FEATURES:
        if feature not in group_a.columns and feature not in group_b.columns:
            continue
        values = sorted(set(group_a.get(feature, pd.Series(dtype=object)).astype(str).dropna()) | set(group_b.get(feature, pd.Series(dtype=object)).astype(str).dropna()))
        for value in values:
            a_count = int(group_a.get(feature, pd.Series(dtype=object)).astype(str).eq(value).sum()) if feature in group_a else 0
            b_count = int(group_b.get(feature, pd.Series(dtype=object)).astype(str).eq(value).sum()) if feature in group_b else 0
            rows.append(
                {
                    "feature": feature,
                    "value": value,
                    f"{group_a_name}_count": a_count,
                    f"{group_b_name}_count": b_count,
                    f"{group_a_name}_rate": _safe_rate(a_count, len(group_a)),
                    f"{group_b_name}_rate": _safe_rate(b_count, len(group_b)),
                    "absolute_rate_delta": abs(_safe_rate(a_count, len(group_a)) - _safe_rate(b_count, len(group_b))),
                }
            )
    rows = sorted(rows, key=lambda row: (row["absolute_rate_delta"], row[f"{group_a_name}_count"] + row[f"{group_b_name}_count"]), reverse=True)
    max_delta = rows[0]["absolute_rate_delta"] if rows else 0.0
    return {
        "group_a": group_a_name,
        "group_b": group_b_name,
        "group_a_metrics": _group_metrics(group_a),
        "group_b_metrics": _group_metrics(group_b),
        "observable_features_checked": [feature for feature in OBSERVABLE_FEATURES if feature in group_a.columns or feature in group_b.columns],
        "future_label_fields_excluded_from_feature_contrast": sorted(FUTURE_LABEL_FIELDS),
        "max_observable_bucket_rate_delta": max_delta,
        "meaningful_observable_separation": bool(max_delta >= 0.25 and min(len(group_a), len(group_b)) >= 10),
        "top_feature_deltas": rows[:25],
    }


def build_recovered_winner_vs_added_nonwinner_report(selected_source: pd.DataFrame) -> dict[str, Any]:
    report = _feature_contrast(
        selected_source[selected_source["recovered_missed_winner"]],
        selected_source[selected_source["added_nonwinner"]],
        group_a_name="recovered_winner",
        group_b_name="added_nonwinner",
    )
    return _stamp(report, "recovered_winner_vs_added_nonwinner_report")


def build_recovered_winner_vs_added_severe_loser_report(selected_source: pd.DataFrame) -> dict[str, Any]:
    report = _feature_contrast(
        selected_source[selected_source["recovered_missed_winner"]],
        selected_source[selected_source["added_severe_loser"]],
        group_a_name="recovered_winner",
        group_b_name="added_severe_loser",
    )
    return _stamp(report, "recovered_winner_vs_added_severe_loser_report")


def build_displacement_pairs(frame: pd.DataFrame, primary_ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    baseline = frame[frame["baseline_top3"]].copy()
    primary_selected = primary_ledger[primary_ledger["selected_topk"].eq(True)].copy()
    for event_date in sorted(set(frame["event_date"].astype(str))):
        baseline_day = baseline[baseline["event_date"].astype(str).eq(event_date)]
        primary_day = primary_selected[primary_selected["event_date"].astype(str).eq(event_date)]
        base_codes = set(baseline_day["code"].astype(str))
        primary_codes = set(primary_day["code"].astype(str))
        added = primary_day[primary_day["code"].astype(str).isin(primary_codes - base_codes)]
        displaced = baseline_day[baseline_day["code"].astype(str).isin(base_codes - primary_codes)]
        for _, added_row in added.iterrows():
            if not bool(added_row.get("source_specific_candidate", False)):
                continue
            displaced_row = displaced.sort_values(["selection_rank", "code"], ascending=[False, False]).head(1)
            d = displaced_row.iloc[0].to_dict() if len(displaced_row) else {}
            rows.append(
                {
                    "event_date": event_date,
                    "added_code": str(added_row["code"]),
                    "added_ret20": float(added_row["ret20_fwd"]),
                    "added_mfe20": float(added_row["mfe20"]),
                    "added_mae20": float(added_row["mae20"]),
                    "added_future_winner": bool(added_row["future_winner"]),
                    "added_nonwinner": bool(v1_mod._nonwinner_mask(pd.DataFrame([added_row])).iloc[0]),
                    "added_severe_loser": bool(added_row["severe_loss20"]),
                    "added_previous_best_rank_bucket": _rank_bucket(added_row.get("previous_best_selection_rank")),
                    "added_source_candidate_count_bucket": _count_bucket(frame[frame["event_date"].astype(str).eq(event_date)]["source_specific_candidate"].sum()),
                    "displaced_code": str(d.get("code")) if d else None,
                    "displaced_ret20": float(d.get("ret20_fwd")) if d else None,
                    "displaced_severe_loser": bool(d.get("severe_loss20")) if d else None,
                    "ret20_delta_added_minus_displaced": float(added_row["ret20_fwd"]) - float(d.get("ret20_fwd")) if d else None,
                }
            )
    return pd.DataFrame(rows)


def build_max1slot_displacement_error_report(displacements: pd.DataFrame, artifacts: dict[str, Any]) -> dict[str, Any]:
    severe = displacements[displacements.get("added_severe_loser", pd.Series(dtype=bool)).astype(bool)] if len(displacements) else displacements
    recovered = displacements[displacements.get("added_future_winner", pd.Series(dtype=bool)).astype(bool)] if len(displacements) else displacements
    report = {
        "max1slot_policy_diagnostic_only": True,
        "displacement_pair_count": int(len(displacements)),
        "recovered_winner_displacement_count": int(len(recovered)),
        "severe_loser_displacement_count": int(len(severe)),
        "avg_ret20_delta_added_minus_displaced": _mean(displacements.get("ret20_delta_added_minus_displaced", [])),
        "recovered_avg_ret20_delta_added_minus_displaced": _mean(recovered.get("ret20_delta_added_minus_displaced", [])),
        "severe_avg_ret20_delta_added_minus_displaced": _mean(severe.get("ret20_delta_added_minus_displaced", [])),
        "previous_best_displacement_error_observed": bool(_mean(displacements.get("ret20_delta_added_minus_displaced", [])) is not None and (_mean(displacements.get("ret20_delta_added_minus_displaced", [])) or 0.0) < 0.0),
        "changed_top3_members_count_vs_previous_best": artifacts["branching_report.json"].get("changed_top3_members_count_vs_previous_best"),
        "sample_rows": displacements.sort_values("ret20_delta_added_minus_displaced").head(50).to_dict("records") if len(displacements) else [],
    }
    return _stamp(report, "max1slot_displacement_error_report")


def build_point_in_time_noise_proxy_report(selected_source: pd.DataFrame, displacements: pd.DataFrame) -> dict[str, Any]:
    newly = selected_source[selected_source["newly_selected_vs_previous_best"]].copy()
    candidate_rows = []
    for feature in OBSERVABLE_FEATURES:
        if feature not in newly.columns:
            continue
        for value, bucket in newly.groupby(feature, dropna=False):
            recovered = int(bucket["recovered_missed_winner"].sum())
            severe = int(bucket["added_severe_loser"].sum())
            nonwinner = int(bucket["added_nonwinner"].sum())
            if recovered <= 0:
                continue
            disp_bucket = displacements[displacements["added_code"].isin(bucket["code"].astype(str))] if len(displacements) else pd.DataFrame()
            candidate_rows.append(
                {
                    "feature": feature,
                    "value": str(value),
                    "selected_source_added_count": int(len(bucket)),
                    "recovered_missed_winner_count": recovered,
                    "added_nonwinner_count": nonwinner,
                    "added_severe_loser_count": severe,
                    "nonwinner_added_per_recovered_winner": nonwinner / recovered,
                    "severe_loser_added_per_recovered_winner": severe / recovered,
                    "avg_ret20": _mean(bucket["ret20_fwd"]),
                    "avg_ret20_delta_added_minus_displaced": _mean(disp_bucket.get("ret20_delta_added_minus_displaced", [])),
                    "point_in_time_observable": feature not in FUTURE_LABEL_FIELDS,
                    "calendar_only_rule": False,
                }
            )
    candidate_rows = sorted(
        candidate_rows,
        key=lambda row: (
            row["severe_loser_added_per_recovered_winner"],
            row["nonwinner_added_per_recovered_winner"],
            -(row["avg_ret20_delta_added_minus_displaced"] or -999),
        ),
    )
    best = candidate_rows[0] if candidate_rows else None
    meaningful = bool(
        best
        and best["recovered_missed_winner_count"] >= 5
        and best["severe_loser_added_per_recovered_winner"] < 0.5
        and best["nonwinner_added_per_recovered_winner"] < 2.0
        and (best["avg_ret20_delta_added_minus_displaced"] or 0.0) > 0.0
    )
    report = {
        "point_in_time_observable_fields_checked": [feature for feature in OBSERVABLE_FEATURES if feature in newly.columns],
        "future_label_fields_excluded_from_proxy_inputs": sorted(FUTURE_LABEL_FIELDS),
        "calendar_block_used_as_trading_rule": False,
        "meaningful_refine_proxy_found": meaningful,
        "best_observable_proxy": best,
        "candidate_proxy_rows": candidate_rows[:100],
    }
    return _stamp(report, "point_in_time_noise_proxy_report")


def build_time_block_noise_report(selected_source: pd.DataFrame, timeblock_artifact: dict[str, Any]) -> dict[str, Any]:
    rows = []
    newly = selected_source[selected_source["newly_selected_vs_previous_best"]].copy()
    for block, group in newly.groupby("time_block", sort=True):
        recovered = int(group["recovered_missed_winner"].sum())
        severe = int(group["added_severe_loser"].sum())
        nonwinner = int(group["added_nonwinner"].sum())
        rows.append(
            {
                "time_block": str(block),
                "selected_source_added_count": int(len(group)),
                "recovered_missed_winner_count": recovered,
                "added_nonwinner_count": nonwinner,
                "added_severe_loser_count": severe,
                "severe_loser_added_per_recovered_winner": severe / recovered if recovered else None,
                "nonwinner_added_per_recovered_winner": nonwinner / recovered if recovered else None,
                "avg_ret20": _mean(group["ret20_fwd"]),
            }
        )
    positive_rows = [row for row in timeblock_artifact.get("rows", []) if (row.get("top3_avg_ret20_delta") or 0.0) > 0.0]
    report = {
        "calendar_block_diagnostic_only": True,
        "calendar_block_used_as_trading_rule": False,
        "effect_stable_across_time_blocks": timeblock_artifact.get("effect_stable_across_time_blocks"),
        "positive_top3_delta_time_block_rate": timeblock_artifact.get("positive_top3_delta_time_block_rate"),
        "positive_top3_delta_time_blocks": [row.get("time_block") for row in positive_rows],
        "rows": rows,
        "time_block_only_pocket_or_overfit_risk": bool(timeblock_artifact.get("effect_stable_across_time_blocks") is False),
    }
    return _stamp(report, "time_block_noise_report")


def build_same_date_support_limitation_followup_report(artifacts: dict[str, Any]) -> dict[str, Any]:
    limitation = artifacts["same_date_support_limitation_report.json"]
    missing = limitation.get("missing_required_context_fields") or []
    blocked = "pre_ma60_context_state" in missing and limitation.get("per_source_same_date_support_available") is False
    report = {
        "per_source_same_date_support_available": limitation.get("per_source_same_date_support_available"),
        "same_date_support_not_faked": limitation.get("same_date_support_not_faked"),
        "missing_required_context_fields": missing,
        "pre_ma60_context_state_absent_from_same_date_support": "pre_ma60_context_state" in missing,
        "source_complete_same_date_ledger_repair_recommended": blocked,
        "do_not_fake_pre_ma60_context_state": True,
        "do_not_derive_per_source_support_from_aggregate_metrics": True,
        "aggregate_same_date_support_only": limitation.get("aggregate_same_date_support_only"),
        "blocking_assessment": "blocks_reliable_same_date_per_source_diagnosis" if blocked else "not_blocking_available_diagnosis",
    }
    return _stamp(report, "same_date_support_limitation_followup_report")


def build_archive_or_refine_decision(
    *,
    artifacts: dict[str, Any],
    grouped: dict[str, Any],
    nonwinner_report: dict[str, Any],
    severe_report: dict[str, Any],
    proxy: dict[str, Any],
    timeblock: dict[str, Any],
    displacement: dict[str, Any],
    same_date: dict[str, Any],
) -> dict[str, Any]:
    source_noise = artifacts["source_noise_report.json"]
    comparison = artifacts["baseline_comparison_report.json"]
    severe_ratio = source_noise.get("severe_loser_added_per_recovered_winner")
    nonwinner_ratio = source_noise.get("nonwinner_added_per_recovered_winner")
    top3_delta = comparison.get("selected_top3_avg_ret20_delta_vs_previous_best")
    proxy_found = proxy.get("meaningful_refine_proxy_found") is True
    separation = bool(
        severe_report.get("meaningful_observable_separation") is True
        or nonwinner_report.get("meaningful_observable_separation") is True
    )
    same_date_blocked = same_date.get("source_complete_same_date_ledger_repair_recommended") is True
    calendar_only = timeblock.get("time_block_only_pocket_or_overfit_risk") is True and not proxy_found

    if same_date_blocked and not proxy_found:
        authoritative = "source_complete_ledger_repair_needed"
        decision = "hold"
        reasons = ["source_complete_same_date_support_missing", "pre_ma60_context_state_missing_from_same_date_ledger"]
    elif proxy_found:
        authoritative = "source_v2_noise_decomposition_refine_ready"
        decision = "keep_candidate"
        reasons = ["point_in_time_observable_proxy_found", "recovered_winner_severe_loser_separation_meaningful"]
    elif (
        not separation
        and calendar_only
        and severe_ratio is not None
        and severe_ratio >= 1.0
        and top3_delta is not None
        and top3_delta < 0.0
    ):
        authoritative = "source_v2_archive"
        decision = "drop"
        reasons = ["recovered_winners_and_severe_losers_not_actionably_separable", "calendar_only_pocket_risk", "top3_return_worse"]
    else:
        authoritative = "source_v2_noise_decomposition_hold"
        decision = "hold"
        reasons = ["separation_weak_or_incomplete", "additional_source_context_required"]

    report = {
        "decision": decision,
        "authoritative_research_decision": authoritative,
        "source_validation_v2_decision": artifacts["research_decision.json"].get("authoritative_research_decision"),
        "severe_loser_added_per_recovered_winner": severe_ratio,
        "nonwinner_added_per_recovered_winner": nonwinner_ratio,
        "selected_top3_avg_ret20_delta_vs_previous_best": top3_delta,
        "oracle_top3_gap_delta_vs_previous_best": comparison.get("oracle_top3_gap_delta_vs_previous_best"),
        "recovered_winner_vs_added_nonwinner_meaningful_separation": nonwinner_report.get("meaningful_observable_separation"),
        "recovered_winner_vs_added_severe_loser_meaningful_separation": severe_report.get("meaningful_observable_separation"),
        "meaningful_refine_proxy_found": proxy_found,
        "best_observable_proxy": proxy.get("best_observable_proxy"),
        "previous_best_displacement_error_observed": displacement.get("previous_best_displacement_error_observed"),
        "source_complete_same_date_ledger_repair_recommended": same_date.get("source_complete_same_date_ledger_repair_recommended"),
        "calendar_block_used_as_trading_rule": False,
        "future_labels_used_for_diagnosis_only": True,
        "future_labels_used_in_score_inputs": False,
        "typed_reasons": reasons,
    }
    return _stamp(report, "source_v2_archive_or_refine_decision")


def build_contract_artifacts(
    *,
    validation_v2_dir: Path,
    output_dir: Path,
    artifacts: dict[str, Any],
    frame: pd.DataFrame,
    source_dirs: dict[str, Path],
) -> dict[str, dict[str, Any]]:
    source_refs = []
    for name in REQUIRED_SOURCE_ARTIFACTS:
        path = validation_v2_dir / name
        source_refs.append({"source": "source_validation_v2", "name": name, "path": str(path), "exists": path.exists(), "file_hash": _file_hash(path)})
    evaluation_contract = _stamp(
        {
            "research_phase": "source_specific_candidate_generation_v2_noise_decomposition",
            "boundary": "TRADEX-only",
            "axis_moved": "source_specific_candidate_generation_v2_noise_decomposition",
            "source_validation_v2_decision": "source_specific_candidate_generation_v2_hold",
            "fixed_evaluation_conditions": {
                "same_source_validation_v2_run": True,
                "same_universe": True,
                "same_period": True,
                "same_top_k": 3,
                "same_cost_slippage_liquidity": "not_changed_not_evaluated",
                "same_artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            },
            "diagnosis_only": True,
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
            "event_count": int(len(frame)),
            "event_day_count": int(frame["event_date"].nunique()) if "event_date" in frame.columns else 0,
        },
        "evaluation_contract",
    )
    noise_contract = _stamp(
        {
            "diagnostic_groups": ["recovered_missed_winner", "added_nonwinner", "added_severe_loser", "neutral_other"],
            "future_labels_used_for_diagnostic_grouping_only": True,
            "future_labels_used_in_score_inputs": False,
            "point_in_time_observable_fields_only_for_proxy": True,
            "excluded_future_label_fields": sorted(FUTURE_LABEL_FIELDS),
            "no_new_scorer": True,
            "no_threshold_tuning": True,
            "no_candidate_promotion": True,
            "no_calendar_block_trading_rule": True,
            "same_date_support_not_faked": artifacts["same_date_support_limitation_report.json"].get("same_date_support_not_faked"),
        },
        "noise_decomposition_contract",
    )
    refs = _stamp(
        {
            "source_validation_v2_root": str(validation_v2_dir),
            "output_root": str(output_dir),
            "source_roots": {key: str(value) for key, value in source_dirs.items()},
            "refs": source_refs,
        },
        "source_artifact_refs",
    )
    return {
        "evaluation_contract.json": evaluation_contract,
        "noise_decomposition_contract.json": noise_contract,
        "source_artifact_refs.json": refs,
    }


def build_next_axis_recommendation(decision: dict[str, Any]) -> dict[str, Any]:
    authoritative = decision.get("authoritative_research_decision")
    if authoritative == "source_v2_noise_decomposition_refine_ready":
        next_axis = "refine v2 with observable condition"
    elif authoritative == "source_v2_archive":
        next_axis = "archive v2"
    elif authoritative == "source_complete_ledger_repair_needed":
        next_axis = "repair source-complete ledger"
    else:
        next_axis = "hold due to insufficient information"
    return _stamp(
        {
            "one_recommended_next_axis_only": True,
            "recommended_next_axis": next_axis,
            "authoritative_research_decision": authoritative,
            "do_not_continue_axes": [
                "scorer",
                "threshold/no-trade",
                "image/fusion",
                "production ranking",
                "MeeMee reflection",
                "publish",
                "sell-side",
                "buy-more/core",
                "exit optimization",
                "cost/slippage/liquidity filters",
            ],
        },
        "next_axis_recommendation",
    )


def build_research_decision(
    *,
    archive_or_refine: dict[str, Any],
    same_date: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    return _stamp(
        {
            "research_phase": "source_specific_candidate_generation_v2_noise_decomposition",
            "boundary": "TRADEX-only",
            "axis_moved": "source_specific_candidate_generation_v2_noise_decomposition",
            "source_validation_v2_decision": "source_specific_candidate_generation_v2_hold",
            "diagnosis_created": True,
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
            "per_source_same_date_support_available": same_date.get("per_source_same_date_support_available"),
            "same_date_support_not_faked": same_date.get("same_date_support_not_faked"),
            "future_labels_used_for_diagnosis_only": True,
            "future_labels_used_in_score_inputs": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": archive_or_refine.get("decision"),
            "authoritative_research_decision": archive_or_refine.get("authoritative_research_decision"),
            "typed_reasons": archive_or_refine.get("typed_reasons") or [],
            "artifact_complete": artifact_complete,
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
            "diagnosis_created": True,
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


def run_source_specific_candidate_generation_v2_noise_decomposition_v1(
    *,
    source_validation_v2_run_id: str = DEFAULT_VALIDATION_V2_RUN_ID,
    source_mechanism_validation_run_id: str = DEFAULT_MECHANISM_VALIDATION_RUN_ID,
    source_hypothesis_refresh_run_id: str = DEFAULT_HYPOTHESIS_REFRESH_RUN_ID,
    validation_v2_root: str | Path = DEFAULT_VALIDATION_V2_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    validation_v2_dir = _run_dir(validation_v2_root, source_validation_v2_run_id, DEFAULT_VALIDATION_V2_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    artifacts = load_source_validation_v2_artifacts(validation_v2_dir)
    frame, primary_ledger, hypothesis, source_dirs = load_reconstructed_inputs(artifacts)

    expected_mechanism = DEFAULT_VALIDATION_V2_ROOT.parent / "candidate_generation_source_mechanism_validation_v1" / source_mechanism_validation_run_id
    expected_refresh = DEFAULT_VALIDATION_V2_ROOT.parent / "candidate_generation_hypothesis_map_refresh_v1" / source_hypothesis_refresh_run_id
    source_root_mismatch = {
        "mechanism_validation": str(source_dirs.get("mechanism_validation")) != str(expected_mechanism),
        "hypothesis_refresh": str(source_dirs.get("hypothesis_refresh")) != str(expected_refresh),
    }

    selected_source = _prepare_selected_source(frame, primary_ledger)
    displacements = build_displacement_pairs(frame, primary_ledger)
    grouped = build_source_v2_grouped_outcome_report(selected_source, artifacts)
    nonwinner = build_recovered_winner_vs_added_nonwinner_report(selected_source)
    severe = build_recovered_winner_vs_added_severe_loser_report(selected_source)
    proxy = build_point_in_time_noise_proxy_report(selected_source, displacements)
    timeblock = build_time_block_noise_report(selected_source, artifacts["time_block_source_validation.json"])
    displacement = build_max1slot_displacement_error_report(displacements, artifacts)
    same_date = build_same_date_support_limitation_followup_report(artifacts)
    archive_or_refine = build_archive_or_refine_decision(
        artifacts=artifacts,
        grouped=grouped,
        nonwinner_report=nonwinner,
        severe_report=severe,
        proxy=proxy,
        timeblock=timeblock,
        displacement=displacement,
        same_date=same_date,
    )
    next_axis = build_next_axis_recommendation(archive_or_refine)

    contracts_payload = build_contract_artifacts(
        validation_v2_dir=validation_v2_dir,
        output_dir=output_dir,
        artifacts=artifacts,
        frame=frame,
        source_dirs=source_dirs,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_validation_v2", "path": str(validation_v2_dir)},
            {"name": "source_mechanism_validation_run_id", "path": source_mechanism_validation_run_id},
            {"name": "source_hypothesis_refresh_run_id", "path": source_hypothesis_refresh_run_id},
        ],
        asof=str(int(frame["event_ymd"].max())) if "event_ymd" in frame.columns and len(frame) else "20260513",
        config={
            "axis_id": AXIS_ID,
            "selected_hypothesis_id": hypothesis.get("hypothesis_id"),
            "source_family": hypothesis.get("source_family"),
            "diagnosis_only": True,
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "threshold_policy_created": False,
            "source_root_mismatch": source_root_mismatch,
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
        "source_v2_grouped_outcome_report.json": grouped,
        "recovered_winner_vs_added_nonwinner_report.json": nonwinner,
        "recovered_winner_vs_added_severe_loser_report.json": severe,
        "point_in_time_noise_proxy_report.json": proxy,
        "time_block_noise_report.json": timeblock,
        "max1slot_displacement_error_report.json": displacement,
        "same_date_support_limitation_followup_report.json": same_date,
        "source_v2_archive_or_refine_decision.json": archive_or_refine,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(archive_or_refine=archive_or_refine, same_date=same_date, artifact_complete=bool(pre_complete["complete"]))
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", decision))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": decision["decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "recommended_next_axis": next_axis["recommended_next_axis"],
        "diagnosis_created": True,
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "same_date_support_not_faked": same_date["same_date_support_not_faked"],
        "per_source_same_date_support_available": same_date["per_source_same_date_support_available"],
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-validation-v2-run-id", default=DEFAULT_VALIDATION_V2_RUN_ID)
    parser.add_argument("--source-mechanism-validation-run-id", default=DEFAULT_MECHANISM_VALIDATION_RUN_ID)
    parser.add_argument("--source-hypothesis-refresh-run-id", default=DEFAULT_HYPOTHESIS_REFRESH_RUN_ID)
    parser.add_argument("--validation-v2-root", default=str(DEFAULT_VALIDATION_V2_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_source_specific_candidate_generation_v2_noise_decomposition_v1(
        source_validation_v2_run_id=args.source_validation_v2_run_id,
        source_mechanism_validation_run_id=args.source_mechanism_validation_run_id,
        source_hypothesis_refresh_run_id=args.source_hypothesis_refresh_run_id,
        validation_v2_root=args.validation_v2_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
