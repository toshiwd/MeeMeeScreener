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


AXIS_ID = "source_v2_per_source_same_date_support_audit_v1"
SCHEMA_PREFIX = "tradex_source_v2_per_source_same_date_support_audit_v1"

DEFAULT_LEDGER_REPAIR_RUN_ID = "20260514T010000Z-source-complete-same-date-ledger-repair-v1"
DEFAULT_NOISE_DECOMPOSITION_RUN_ID = "20260514T000000Z-source-specific-candidate-generation-v2-noise-decomposition-v1"
DEFAULT_VALIDATION_V2_RUN_ID = "20260513T200000Z-source-specific-candidate-generation-validation-v2"
DEFAULT_LEDGER_REPAIR_ROOT = Path(r"G:\Tradex\source_complete_same_date_ledger_repair_v1")
DEFAULT_NOISE_DECOMPOSITION_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_v2_noise_decomposition_v1")
DEFAULT_VALIDATION_V2_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v2")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\source_v2_per_source_same_date_support_audit_v1")

REQUIRED_LEDGER_REPAIR_ARTIFACTS = (
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
    "source_complete_same_date_ledger.jsonl",
    "same_date_support_readiness_report.json",
    "v2_source_support_precheck.json",
)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "per_source_same_date_support_contract.json",
    "per_source_same_date_support_ledger.jsonl",
    "v2_source_relative_support_report.json",
    "v2_source_selected_vs_nonselected_report.json",
    "same_date_source_rank_report.json",
    "same_date_failure_mode_report.json",
    "v2_archive_or_refine_decision.json",
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


def _stamp(payload: dict[str, Any], artifact_id: str) -> dict[str, Any]:
    payload["schema_version"] = f"{SCHEMA_PREFIX}_{artifact_id}_v1"
    payload["generated_at"] = _utc_now()
    payload["axis_id"] = AXIS_ID
    payload["contract_hash"] = _stable_hash({key: value for key, value in payload.items() if key != "contract_hash"})
    return payload


def load_inputs(ledger_repair_dir: Path, noise_dir: Path, validation_v2_dir: Path) -> dict[str, Any]:
    missing = [name for name in REQUIRED_LEDGER_REPAIR_ARTIFACTS if not (ledger_repair_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"ledger repair missing required artifacts: {missing} at {ledger_repair_dir}")
    complete = _load_json(ledger_repair_dir / "_ARTIFACT_COMPLETE.json")
    decision = _load_json(ledger_repair_dir / "research_decision.json")
    if complete.get("complete") is not True:
        raise RuntimeError("ledger repair artifact is not complete")
    if complete.get("silent_fallback_used") is not False or decision.get("silent_fallback_used") is True:
        raise RuntimeError("ledger repair used silent fallback")
    if complete.get("research_fallback_used") is not False or decision.get("research_fallback_used") is True:
        raise RuntimeError("ledger repair used research fallback")
    if decision.get("authoritative_research_decision") != "source_complete_ledger_ready":
        raise RuntimeError("source complete ledger is not ready")
    if decision.get("per_source_same_date_support_available") is not True:
        raise RuntimeError("per-source same-date support is not available")
    if decision.get("same_date_support_not_faked") is not True:
        raise RuntimeError("same-date support is not explicitly non-faked")
    if decision.get("future_labels_used_in_source_construction") is not False:
        raise RuntimeError("ledger repair used future labels in source construction")
    ledger_rows = _load_jsonl(ledger_repair_dir / "source_complete_same_date_ledger.jsonl")
    if not ledger_rows:
        raise RuntimeError("source_complete_same_date_ledger.jsonl is empty")
    optional_refs = {}
    for source_name, root in {"noise_decomposition": noise_dir, "source_validation_v2": validation_v2_dir}.items():
        optional_refs[source_name] = {"path": str(root), "exists": root.exists()}
        for name in ("research_decision.json", "_ARTIFACT_COMPLETE.json"):
            if (root / name).exists():
                optional_refs[source_name][name] = _load_json(root / name)
    return {
        "ledger_repair_complete": complete,
        "ledger_repair_decision": decision,
        "ledger_readiness": _load_json(ledger_repair_dir / "same_date_support_readiness_report.json"),
        "v2_precheck": _load_json(ledger_repair_dir / "v2_source_support_precheck.json"),
        "ledger_rows": ledger_rows,
        "optional_refs": optional_refs,
    }


def _frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    for col in ("ret20", "MFE20", "MAE20"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    for col in (
        "future_winner_evaluation_label",
        "nonwinner_evaluation_label",
        "severe_loss20",
        "selected_by_previous_best_top3",
        "selected_by_source_v2",
        "source_v2_candidate_flag",
    ):
        frame[col] = frame[col].astype(bool)
    return frame


def _support_metrics(group: pd.DataFrame) -> dict[str, Any]:
    count = int(len(group))
    return {
        "candidate_count": count,
        "future_winner_count": int(group["future_winner_evaluation_label"].sum()),
        "severe_loser_count": int(group["severe_loss20"].sum()),
        "nonwinner_count": int(group["nonwinner_evaluation_label"].sum()),
        "avg_ret20": _mean(group["ret20"]),
        "median_ret20": _median(group["ret20"]),
        "avg_MFE20": _mean(group["MFE20"]),
        "avg_MAE20": _mean(group["MAE20"]),
        "severe_loss_rate20": _safe_rate(int(group["severe_loss20"].sum()), count),
        "future_winner_rate": _safe_rate(int(group["future_winner_evaluation_label"].sum()), count),
        "selected_by_previous_best_top3_count": int(group["selected_by_previous_best_top3"].sum()),
        "selected_by_source_v2_count": int(group["selected_by_source_v2"].sum()),
    }


def build_per_source_same_date_support_ledger(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for (event_date, source_family_id), group in frame.groupby(["event_date", "source_family_id"], dropna=False, sort=True):
        metrics = _support_metrics(group)
        rows.append(
            {
                "event_date": str(event_date),
                "source_family_id": str(source_family_id),
                "source_family_text": str(group["source_family_text"].dropna().iloc[0]) if "source_family_text" in group.columns and group["source_family_text"].notna().any() else None,
                "source_v2_family": bool(group["source_v2_candidate_flag"].any()),
                **metrics,
            }
        )
    return rows


def _rank_rows(support_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = []
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in support_rows:
        by_date.setdefault(row["event_date"], []).append(row)
    for event_date, rows in by_date.items():
        winner_sorted = sorted(rows, key=lambda item: (-float(item["future_winner_rate"]), -float(item["avg_ret20"] or -999), item["source_family_id"]))
        severe_sorted = sorted(rows, key=lambda item: (float(item["severe_loss_rate20"]), -float(item["avg_ret20"] or -999), item["source_family_id"]))
        ret_sorted = sorted(rows, key=lambda item: (-(float(item["avg_ret20"]) if item["avg_ret20"] is not None else -999), item["source_family_id"]))
        winner_rank = {row["source_family_id"]: idx + 1 for idx, row in enumerate(winner_sorted)}
        severe_rank = {row["source_family_id"]: idx + 1 for idx, row in enumerate(severe_sorted)}
        ret_rank = {row["source_family_id"]: idx + 1 for idx, row in enumerate(ret_sorted)}
        for row in rows:
            ranked.append(
                {
                    **row,
                    "same_date_source_family_count": len(rows),
                    "winner_rate_rank": winner_rank[row["source_family_id"]],
                    "severe_loss_rate_rank": severe_rank[row["source_family_id"]],
                    "avg_ret20_rank": ret_rank[row["source_family_id"]],
                }
            )
    return sorted(ranked, key=lambda row: (row["event_date"], row["source_family_id"]))


def build_v2_source_relative_support_report(ranked_rows: list[dict[str, Any]]) -> dict[str, Any]:
    frame = pd.DataFrame(ranked_rows)
    v2 = frame[frame["source_v2_family"].astype(bool)].copy()
    rows = []
    for _, row in v2.iterrows():
        same = frame[frame["event_date"].astype(str).eq(str(row["event_date"]))]
        other = same[~same["source_v2_family"].astype(bool)]
        other_weight = float(other["candidate_count"].sum())
        other_winners = float(other["future_winner_count"].sum())
        other_severe = float(other["severe_loser_count"].sum())
        other_avg_ret = _mean(other["avg_ret20"]) if len(other) else None
        baseline_winner_rate = other_winners / other_weight if other_weight else None
        baseline_severe_rate = other_severe / other_weight if other_weight else None
        rows.append(
            {
                "event_date": str(row["event_date"]),
                "v2_source_family_id": str(row["source_family_id"]),
                "v2_candidate_count": int(row["candidate_count"]),
                "v2_future_winner_rate": float(row["future_winner_rate"]),
                "same_date_other_source_winner_rate": baseline_winner_rate,
                "winner_rate_delta_vs_other_sources": float(row["future_winner_rate"]) - baseline_winner_rate if baseline_winner_rate is not None else None,
                "v2_severe_loss_rate20": float(row["severe_loss_rate20"]),
                "same_date_other_source_severe_loss_rate20": baseline_severe_rate,
                "severe_loss_rate_delta_vs_other_sources": float(row["severe_loss_rate20"]) - baseline_severe_rate if baseline_severe_rate is not None else None,
                "v2_avg_ret20": row["avg_ret20"],
                "same_date_source_group_avg_ret20": other_avg_ret,
                "avg_ret20_delta_vs_same_date_source_group": float(row["avg_ret20"]) - other_avg_ret if row["avg_ret20"] is not None and other_avg_ret is not None else None,
                "v2_winner_rate_rank": int(row["winner_rate_rank"]),
                "v2_severe_loss_rate_rank": int(row["severe_loss_rate_rank"]),
                "v2_avg_ret20_rank": int(row["avg_ret20_rank"]),
                "same_date_source_family_count": int(row["same_date_source_family_count"]),
            }
        )
    rel = pd.DataFrame(rows)
    report = {
        "v2_same_date_count": int(len(rows)),
        "v2_positive_winner_rate_delta_rate": float(pd.to_numeric(rel.get("winner_rate_delta_vs_other_sources", pd.Series(dtype=float)), errors="coerce").gt(0.0).mean()) if len(rel) else 0.0,
        "v2_nonworse_severe_rate_delta_rate": float(pd.to_numeric(rel.get("severe_loss_rate_delta_vs_other_sources", pd.Series(dtype=float)), errors="coerce").le(0.0).mean()) if len(rel) else 0.0,
        "v2_positive_avg_ret20_delta_rate": float(pd.to_numeric(rel.get("avg_ret20_delta_vs_same_date_source_group", pd.Series(dtype=float)), errors="coerce").gt(0.0).mean()) if len(rel) else 0.0,
        "avg_winner_rate_delta_vs_other_sources": _mean(rel.get("winner_rate_delta_vs_other_sources", pd.Series(dtype=float))) if len(rel) else None,
        "avg_severe_loss_rate_delta_vs_other_sources": _mean(rel.get("severe_loss_rate_delta_vs_other_sources", pd.Series(dtype=float))) if len(rel) else None,
        "avg_ret20_delta_vs_same_date_source_group": _mean(rel.get("avg_ret20_delta_vs_same_date_source_group", pd.Series(dtype=float))) if len(rel) else None,
        "rows": rows,
        "rows_sample": rows[:500],
    }
    return _stamp(report, "v2_source_relative_support_report")


def build_v2_source_selected_vs_nonselected_report(frame: pd.DataFrame) -> dict[str, Any]:
    v2 = frame[frame["source_v2_candidate_flag"].astype(bool)].copy()
    selected = v2[v2["selected_by_source_v2"].astype(bool)].copy()
    nonselected = v2[~v2["selected_by_source_v2"].astype(bool)].copy()
    recovered = selected[~selected["selected_by_previous_best_top3"].astype(bool) & selected["future_winner_evaluation_label"].astype(bool)]
    added_nonwinner = selected[~selected["selected_by_previous_best_top3"].astype(bool) & selected["nonwinner_evaluation_label"].astype(bool)]
    added_severe = selected[~selected["selected_by_previous_best_top3"].astype(bool) & selected["severe_loss20"].astype(bool)]
    recovered_count = int(len(recovered))
    selected_metrics = _support_metrics(selected) if len(selected) else _support_metrics(v2.iloc[0:0])
    nonselected_metrics = _support_metrics(nonselected) if len(nonselected) else _support_metrics(v2.iloc[0:0])
    return _stamp(
        {
            "v2_source_candidate_count": int(len(v2)),
            "selected_by_source_v2": selected_metrics,
            "not_selected_by_source_v2": nonselected_metrics,
            "selected_by_previous_best_top3_count": int(v2["selected_by_previous_best_top3"].sum()) if len(v2) else 0,
            "not_selected_by_previous_best_top3_count": int((~v2["selected_by_previous_best_top3"]).sum()) if len(v2) else 0,
            "recovered_missed_winner_count": recovered_count,
            "added_nonwinner_count": int(len(added_nonwinner)),
            "added_severe_loser_count": int(len(added_severe)),
            "severe_loser_added_per_recovered_winner": len(added_severe) / recovered_count if recovered_count else None,
            "nonwinner_added_per_recovered_winner": len(added_nonwinner) / recovered_count if recovered_count else None,
        },
        "v2_source_selected_vs_nonselected_report",
    )


def build_same_date_source_rank_report(ranked_rows: list[dict[str, Any]]) -> dict[str, Any]:
    v2_rows = [row for row in ranked_rows if row["source_v2_family"]]
    return _stamp(
        {
            "same_date_source_rank_rows": len(ranked_rows),
            "v2_ranked_same_date_rows": len(v2_rows),
            "v2_avg_winner_rate_rank": _mean(pd.Series([row["winner_rate_rank"] for row in v2_rows])),
            "v2_avg_severe_loss_rate_rank": _mean(pd.Series([row["severe_loss_rate_rank"] for row in v2_rows])),
            "v2_avg_ret20_rank": _mean(pd.Series([row["avg_ret20_rank"] for row in v2_rows])),
            "v2_top_half_winner_rate_rank_rate": float(np.mean([row["winner_rate_rank"] <= max(row["same_date_source_family_count"] / 2, 1) for row in v2_rows])) if v2_rows else 0.0,
            "rows_sample": v2_rows[:500],
        },
        "same_date_source_rank_report",
    )


def build_same_date_failure_mode_report(relative: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for row in relative.get("rows", []):
        winner_delta = row.get("winner_rate_delta_vs_other_sources")
        severe_delta = row.get("severe_loss_rate_delta_vs_other_sources")
        ret_delta = row.get("avg_ret20_delta_vs_same_date_source_group")
        if winner_delta is not None and winner_delta > 0 and severe_delta is not None and severe_delta <= 0 and ret_delta is not None and ret_delta > 0:
            mode = "v2_source_best_available"
        elif winner_delta is not None and winner_delta > 0 and severe_delta is not None and severe_delta > 0:
            mode = "v2_source_good_but_over_noisy"
        elif winner_delta is not None and winner_delta < 0 and ret_delta is not None and ret_delta < 0:
            mode = "v2_source_worse_than_other_sources"
        elif row.get("v2_avg_ret20_rank") and int(row["v2_avg_ret20_rank"]) > 1 and int(row.get("v2_candidate_count") or 0) > 0:
            mode = "v2_source_selected_when_better_source_available" if int(row.get("v2_candidate_count") or 0) > 0 else "v2_source_no_clear_advantage"
        else:
            mode = "v2_source_no_clear_advantage"
        rows.append({"event_date": row["event_date"], "failure_mode": mode, **row})
    counts = pd.Series([row["failure_mode"] for row in rows]).value_counts().to_dict() if rows else {}
    return _stamp(
        {
            "classified_v2_same_date_count": len(rows),
            "failure_mode_counts": {str(key): int(value) for key, value in counts.items()},
            "failure_mode_rates": {str(key): _safe_rate(value, len(rows)) for key, value in counts.items()},
            "rows_sample": rows[:500],
        },
        "same_date_failure_mode_report",
    )


def build_contract_artifacts(*, rows: list[dict[str, Any]], ledger_repair_decision: dict[str, Any]) -> dict[str, dict[str, Any]]:
    evaluation_contract = _stamp(
        {
            "research_phase": "source_v2_per_source_same_date_support_audit",
            "boundary": "TRADEX-only",
            "axis_moved": "source_v2_per_source_same_date_support_audit",
            "source_complete_ledger_decision": ledger_repair_decision.get("authoritative_research_decision"),
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
            "safe_full_used_as_hard_filter": False,
            "negative_guard_used_as_hard_veto": False,
            "future_labels_used_for_diagnosis_only": True,
            "future_labels_used_in_source_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "source_complete_ledger_row_count": len(rows),
        },
        "evaluation_contract",
    )
    support_contract = _stamp(
        {
            "source_complete_same_date_ledger_is_authoritative": True,
            "per_source_group_key": ["event_date", "source_family_id"],
            "v2_source_identifier": "source_v2_candidate_flag",
            "future_labels_for_diagnosis_only": [
                "ret20",
                "MFE20",
                "MAE20",
                "severe_loss20",
                "future_winner_evaluation_label",
                "nonwinner_evaluation_label",
            ],
            "future_labels_used_in_source_construction": False,
            "same_date_support_not_faked": True,
        },
        "per_source_same_date_support_contract",
    )
    return {"evaluation_contract.json": evaluation_contract, "per_source_same_date_support_contract.json": support_contract}


def build_source_artifact_refs(ledger_repair_dir: Path, noise_dir: Path, validation_v2_dir: Path) -> dict[str, Any]:
    refs = []
    for source, root in {"ledger_repair": ledger_repair_dir, "noise_decomposition": noise_dir, "source_validation_v2": validation_v2_dir}.items():
        for path in sorted(root.glob("*.json")) if root.exists() else []:
            refs.append({"source": source, "name": path.name, "path": str(path), "exists": True, "file_hash": _file_hash(path)})
        for path in sorted(root.glob("*.jsonl")) if root.exists() else []:
            refs.append({"source": source, "name": path.name, "path": str(path), "exists": True, "file_hash": _file_hash(path)})
    return _stamp(
        {
            "source_roots": {
                "ledger_repair": str(ledger_repair_dir),
                "noise_decomposition": str(noise_dir),
                "source_validation_v2": str(validation_v2_dir),
            },
            "refs": refs,
        },
        "source_artifact_refs",
    )


def decide(relative: dict[str, Any], selected: dict[str, Any], failure: dict[str, Any], artifact_complete: bool) -> tuple[str, str, list[str]]:
    winner_delta = relative.get("avg_winner_rate_delta_vs_other_sources")
    severe_delta = relative.get("avg_severe_loss_rate_delta_vs_other_sources")
    ret_delta = relative.get("avg_ret20_delta_vs_same_date_source_group")
    severe_ratio = selected.get("severe_loser_added_per_recovered_winner")
    nonwinner_ratio = selected.get("nonwinner_added_per_recovered_winner")
    best_rate = failure.get("failure_mode_rates", {}).get("v2_source_best_available", 0.0)
    worse_rate = failure.get("failure_mode_rates", {}).get("v2_source_worse_than_other_sources", 0.0)
    better_available_rate = failure.get("failure_mode_rates", {}).get("v2_source_selected_when_better_source_available", 0.0)
    if (
        artifact_complete
        and winner_delta is not None
        and winner_delta > 0.0
        and severe_delta is not None
        and severe_delta <= 0.0
        and ret_delta is not None
        and ret_delta > 0.0
        and severe_ratio is not None
        and severe_ratio < 0.75
        and nonwinner_ratio is not None
        and nonwinner_ratio < 2.0
        and best_rate >= 0.45
    ):
        return (
            "keep_candidate",
            "source_v2_refine_ready",
            ["positive_same_date_relative_support", "winner_rate_exceeds_same_date_baseline", "severe_rate_not_worse", "noise_ratios_materially_improved", "artifact_complete"],
        )
    if (
        winner_delta is not None
        and winner_delta <= 0.0
        and ret_delta is not None
        and ret_delta <= 0.0
        and (severe_ratio is None or severe_ratio >= 1.0)
        and (worse_rate + better_available_rate) >= 0.50
    ):
        return (
            "drop",
            "source_v2_archive",
            ["no_positive_same_date_advantage", "severe_noise_remains_high", "v2_often_worse_or_replaces_better_sources"],
        )
    return (
        "hold",
        "source_v2_support_hold",
        ["partial_or_mixed_same_date_advantage", "refine_requires_point_in_time_applicability_rule_or_more_source_decomposition"],
    )


def build_archive_or_refine_decision(relative: dict[str, Any], selected: dict[str, Any], failure: dict[str, Any], artifact_complete: bool) -> dict[str, Any]:
    decision, authoritative, reasons = decide(relative, selected, failure, artifact_complete)
    return _stamp(
        {
            "decision": decision,
            "authoritative_research_decision": authoritative,
            "avg_winner_rate_delta_vs_other_sources": relative.get("avg_winner_rate_delta_vs_other_sources"),
            "avg_severe_loss_rate_delta_vs_other_sources": relative.get("avg_severe_loss_rate_delta_vs_other_sources"),
            "avg_ret20_delta_vs_same_date_source_group": relative.get("avg_ret20_delta_vs_same_date_source_group"),
            "severe_loser_added_per_recovered_winner": selected.get("severe_loser_added_per_recovered_winner"),
            "nonwinner_added_per_recovered_winner": selected.get("nonwinner_added_per_recovered_winner"),
            "failure_mode_rates": failure.get("failure_mode_rates"),
            "future_labels_used_for_diagnosis_only": True,
            "future_labels_used_in_source_construction": False,
            "same_date_support_not_faked": True,
            "typed_reasons": reasons,
        },
        "v2_archive_or_refine_decision",
    )


def build_next_axis_recommendation(authoritative: str) -> dict[str, Any]:
    if authoritative == "source_v2_refine_ready":
        next_axis = "limited-condition v2 validation"
    elif authoritative == "source_v2_archive":
        next_axis = "archive v2 source"
    else:
        next_axis = "additional source decomposition before v2 refine"
    return _stamp(
        {
            "one_recommended_next_axis_only": True,
            "recommended_next_axis": next_axis,
            "authoritative_research_decision": authoritative,
            "do_not_continue_axes": ["scorer", "threshold/no-trade", "candidate promotion", "image/fusion", "production ranking", "MeeMee reflection", "publish"],
        },
        "next_axis_recommendation",
    )


def build_research_decision(archive_or_refine: dict[str, Any]) -> dict[str, Any]:
    return _stamp(
        {
            "research_phase": "source_v2_per_source_same_date_support_audit",
            "boundary": "TRADEX-only",
            "axis_moved": "source_v2_per_source_same_date_support_audit",
            "source_complete_ledger_decision": "source_complete_ledger_ready",
            "per_source_same_date_support_audit_created": True,
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
            "pre_ma60_context_state_available": True,
            "per_source_same_date_support_available": True,
            "same_date_support_not_faked": True,
            "future_labels_used_for_diagnosis_only": True,
            "future_labels_used_in_source_construction": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
            "decision": archive_or_refine.get("decision"),
            "authoritative_research_decision": archive_or_refine.get("authoritative_research_decision"),
            "typed_reasons": archive_or_refine.get("typed_reasons") or [],
        },
        "research_decision",
    )


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any] | None = None) -> dict[str, Any]:
    excluded = {"_ARTIFACT_COMPLETE.json"}
    if decision is None:
        excluded.update({"research_decision.json", "next_axis_recommendation.json"})
    required = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name not in excluded}
    return _stamp(
        {
            "artifact_root": str(output_dir),
            "complete": all(required.values()),
            "required_artifacts": required,
            "paths": paths,
            "decision": decision.get("decision") if decision else None,
            "authoritative_research_decision": decision.get("authoritative_research_decision") if decision else None,
            "per_source_same_date_support_audit_created": True,
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


def run_source_v2_per_source_same_date_support_audit_v1(
    *,
    source_ledger_repair_run_id: str = DEFAULT_LEDGER_REPAIR_RUN_ID,
    source_noise_decomposition_run_id: str = DEFAULT_NOISE_DECOMPOSITION_RUN_ID,
    source_validation_v2_run_id: str = DEFAULT_VALIDATION_V2_RUN_ID,
    ledger_repair_root: str | Path = DEFAULT_LEDGER_REPAIR_ROOT,
    noise_decomposition_root: str | Path = DEFAULT_NOISE_DECOMPOSITION_ROOT,
    validation_v2_root: str | Path = DEFAULT_VALIDATION_V2_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    ledger_repair_dir = _run_dir(ledger_repair_root, source_ledger_repair_run_id, DEFAULT_LEDGER_REPAIR_ROOT)
    noise_dir = _run_dir(noise_decomposition_root, source_noise_decomposition_run_id, DEFAULT_NOISE_DECOMPOSITION_ROOT)
    validation_v2_dir = _run_dir(validation_v2_root, source_validation_v2_run_id, DEFAULT_VALIDATION_V2_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    inputs = load_inputs(ledger_repair_dir, noise_dir, validation_v2_dir)
    frame = _frame(inputs["ledger_rows"])
    support_rows = build_per_source_same_date_support_ledger(frame)
    ranked_rows = _rank_rows(support_rows)
    relative = build_v2_source_relative_support_report(ranked_rows)
    selected = build_v2_source_selected_vs_nonselected_report(frame)
    rank_report = build_same_date_source_rank_report(ranked_rows)
    failure = build_same_date_failure_mode_report(relative)
    contracts_payload = build_contract_artifacts(rows=inputs["ledger_rows"], ledger_repair_decision=inputs["ledger_repair_decision"])
    refs = build_source_artifact_refs(ledger_repair_dir, noise_dir, validation_v2_dir)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_ledger_repair", "path": str(ledger_repair_dir)},
            {"name": "source_noise_decomposition", "path": str(noise_dir)},
            {"name": "source_validation_v2", "path": str(validation_v2_dir)},
        ],
        asof=str(max(frame["event_date"].astype(str))) if len(frame) else "20260514",
        config={
            "axis_id": AXIS_ID,
            "diagnosis_only": True,
            "source_complete_ledger_decision": inputs["ledger_repair_decision"].get("authoritative_research_decision"),
            "candidate_generation_challenger_created": False,
            "candidate_scoring_created": False,
            "threshold_policy_created": False,
            "production_ranking_changed": False,
        },
        universe=sorted(frame["symbol"].astype(str).unique().tolist()) if "symbol" in frame.columns else [],
        period={"start_date": str(min(frame["event_date"].astype(str))) if len(frame) else "unknown", "end_date": str(max(frame["event_date"].astype(str))) if len(frame) else "unknown", "label": AXIS_ID},
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
        "v2_source_relative_support_report.json": relative,
        "v2_source_selected_vs_nonselected_report.json": selected,
        "same_date_source_rank_report.json": rank_report,
        "same_date_failure_mode_report.json": failure,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["per_source_same_date_support_ledger.jsonl"] = str(_write_jsonl(output_dir / "per_source_same_date_support_ledger.jsonl", ranked_rows))
    pre_complete = _artifact_complete(output_dir, paths)
    archive_or_refine = build_archive_or_refine_decision(relative, selected, failure, bool(pre_complete["complete"]))
    paths["v2_archive_or_refine_decision.json"] = str(_write_json(output_dir / "v2_archive_or_refine_decision.json", archive_or_refine))
    next_axis = build_next_axis_recommendation(str(archive_or_refine["authoritative_research_decision"]))
    paths["next_axis_recommendation.json"] = str(_write_json(output_dir / "next_axis_recommendation.json", next_axis))
    research_decision = build_research_decision(archive_or_refine)
    paths["research_decision.json"] = str(_write_json(output_dir / "research_decision.json", research_decision))
    complete = _artifact_complete(output_dir, paths, research_decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))
    return {
        "output_dir": str(output_dir),
        "decision": research_decision["decision"],
        "authoritative_research_decision": research_decision["authoritative_research_decision"],
        "recommended_next_axis": next_axis["recommended_next_axis"],
        "per_source_same_date_support_audit_created": True,
        "candidate_generation_challenger_created": False,
        "candidate_scoring_created": False,
        "ranking_objective_created": False,
        "threshold_policy_created": False,
        "image_score_used": False,
        "fusion_reranker_created": False,
        "production_ranking_changed": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
        "same_date_support_not_faked": True,
        "silent_fallback_used": False,
        "research_fallback_used": False,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-ledger-repair-run-id", default=DEFAULT_LEDGER_REPAIR_RUN_ID)
    parser.add_argument("--source-noise-decomposition-run-id", default=DEFAULT_NOISE_DECOMPOSITION_RUN_ID)
    parser.add_argument("--source-validation-v2-run-id", default=DEFAULT_VALIDATION_V2_RUN_ID)
    parser.add_argument("--ledger-repair-root", default=str(DEFAULT_LEDGER_REPAIR_ROOT))
    parser.add_argument("--noise-decomposition-root", default=str(DEFAULT_NOISE_DECOMPOSITION_ROOT))
    parser.add_argument("--validation-v2-root", default=str(DEFAULT_VALIDATION_V2_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_source_v2_per_source_same_date_support_audit_v1(
        source_ledger_repair_run_id=args.source_ledger_repair_run_id,
        source_noise_decomposition_run_id=args.source_noise_decomposition_run_id,
        source_validation_v2_run_id=args.source_validation_v2_run_id,
        ledger_repair_root=args.ledger_repair_root,
        noise_decomposition_root=args.noise_decomposition_root,
        validation_v2_root=args.validation_v2_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
