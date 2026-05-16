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
from scripts import tradex_ranking_loss_or_topk_objective_repair_v1 as ranking_mod


AXIS_ID = "source_specific_candidate_generation_validation_v1"
SCHEMA_PREFIX = "tradex_source_specific_candidate_generation_validation_v1"
DEFAULT_MISSED_WINNER_RUN_ID = "20260513T140000Z-missed-winner-event-source-candidate-generation-v1"
DEFAULT_ROOT_CAUSE_RUN_ID = "20260513T130000Z-oracle-gap-and-candidate-generation-root-cause-v1"
DEFAULT_WIDE_RUN_ID = "20260513T030000Z-wide-strength-pool-upside-rerank-v1"
DEFAULT_PATTERN_RUN_ID = "20260513T000000Z-pre-strength-pattern-mining-v1"
DEFAULT_UPSIDE_RUN_ID = "20260513T020000Z-upside-capture-missed-winner-diagnosis-v1"
DEFAULT_MISSED_WINNER_ROOT = Path(r"G:\Tradex\missed_winner_event_source_candidate_generation_v1")
DEFAULT_ROOT_CAUSE_ROOT = source_mod.DEFAULT_ROOT_CAUSE_ROOT
DEFAULT_WIDE_ROOT = source_mod.DEFAULT_WIDE_ROOT
DEFAULT_PATTERN_ROOT = source_mod.DEFAULT_PATTERN_ROOT
DEFAULT_UPSIDE_ROOT = source_mod.DEFAULT_UPSIDE_ROOT
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\source_specific_candidate_generation_validation_v1")

TOP_K = 3
TOP5_K = 5
BASELINE_FAMILY_ID = source_mod.BASELINE_FAMILY_ID
PRIMARY_FAMILY_ID = "source_specific_candidate_generation_v1_max1slot"
DIAGNOSTIC_FAMILY_ID = "source_specific_candidate_generation_v1_max2slot_diagnostic"
SOURCE_ORACLE_FAMILY_ID = "source_only_oracle_diagnostic"
REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "source_artifact_refs.json",
    "source_generation_contract.json",
    "source_candidate_ledger.jsonl",
    "source_overlap_audit.json",
    "source_oracle_diagnostic.json",
    "top3_selection_report.json",
    "source_recovery_report.json",
    "source_noise_report.json",
    "max3_source_slot_report.json",
    "time_block_source_validation.json",
    "baseline_comparison_report.json",
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


def validate_sources(
    *,
    missed_winner_dir: Path,
    root_cause_dir: Path,
    wide_dir: Path,
    pattern_dir: Path,
    upside_dir: Path,
) -> dict[str, Any]:
    required_by_source = {
        "missed_winner": [
            "_ARTIFACT_COMPLETE.json",
            "research_decision.json",
            "candidate_generation_hypothesis_map.json",
            "source_artifact_refs.json",
        ],
        "root_cause": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "source_artifact_refs.json"],
        "wide": ["_ARTIFACT_COMPLETE.json", "research_decision.json", "source_artifact_refs.json"],
        "pattern": ["_ARTIFACT_COMPLETE.json"],
        "upside": ["_ARTIFACT_COMPLETE.json"],
    }
    dirs = {
        "missed_winner": missed_winner_dir,
        "root_cause": root_cause_dir,
        "wide": wide_dir,
        "pattern": pattern_dir,
        "upside": upside_dir,
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

    missed_decision = _load_json(missed_winner_dir / "research_decision.json")
    if missed_decision.get("authoritative_research_decision") != "missed_winner_source_hypothesis_ready":
        raise RuntimeError("missed-winner source artifact is not ready for source-specific validation")
    if missed_decision.get("recommended_next_axis") != AXIS_ID:
        raise RuntimeError("missed-winner source artifact does not recommend source_specific_candidate_generation_validation_v1")
    if missed_decision.get("research_fallback_used") is not False:
        raise RuntimeError("missed-winner source artifact used research fallback")

    missed_refs = _load_json(missed_winner_dir / "source_artifact_refs.json").get("source_roots", {})
    feature_dir = Path(missed_refs.get("feature_diagnosis") or source_mod.DEFAULT_FEATURE_DIAGNOSIS_ROOT / source_mod.DEFAULT_FEATURE_DIAGNOSIS_RUN_ID)
    source_status = source_mod.validate_sources(
        root_cause_dir=root_cause_dir,
        wide_dir=wide_dir,
        pattern_dir=pattern_dir,
        upside_dir=upside_dir,
        feature_diagnosis_dir=feature_dir,
    )
    status.update(
        {
            "missed_winner": {
                **status["missed_winner"],
                "research_decision.json": missed_decision,
                "candidate_generation_hypothesis_map.json": _load_json(missed_winner_dir / "candidate_generation_hypothesis_map.json"),
            },
            "source_mod_status": source_status,
            "feature_diagnosis_dir": feature_dir,
        }
    )
    return status


def load_validation_inputs(*, source_status: dict[str, Any], wide_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    return source_mod.load_diagnosis_inputs(source_status=source_status["source_mod_status"], wide_dir=wide_dir)


def _winner_mask(frame: pd.DataFrame) -> pd.Series:
    return source_mod._winner_mask(frame)


def _nonwinner_mask(frame: pd.DataFrame) -> pd.Series:
    return ~_winner_mask(frame) & (
        pd.to_numeric(frame["ret20_fwd"], errors="coerce").le(0.0)
        | pd.to_numeric(frame.get("ret20_rank_pct_by_date", pd.Series(index=frame.index, dtype=float)), errors="coerce").gt(0.50)
        | frame["severe_loss20"].astype(bool)
    )


def _source_hypothesis(source_status: dict[str, Any]) -> dict[str, Any]:
    hypotheses = source_status["missed_winner"]["candidate_generation_hypothesis_map.json"].get("hypotheses", [])
    if not hypotheses:
        raise RuntimeError("candidate_generation_hypothesis_map.json has no hypotheses")
    return dict(hypotheses[0])


def _source_frame(frame: pd.DataFrame, source_family: str) -> pd.DataFrame:
    out = frame.copy()
    out["source_specific_candidate"] = out["source_family"].astype(str).eq(source_family)
    out["baseline_top3"] = out["selected_by_previous_best_top3"].astype(bool)
    out["baseline_top5"] = out["selected_by_previous_best_top5"].astype(bool)
    out["future_winner"] = _winner_mask(out)
    out["nonwinner"] = _nonwinner_mask(out)
    out["selection_rank"] = pd.to_numeric(out["selection_rank"], errors="coerce")
    return out


def _ordered_day(day: pd.DataFrame) -> pd.DataFrame:
    return day.assign(_rank_sort=day["selection_rank"].fillna(1_000_000)).sort_values(["_rank_sort", "code"], kind="mergesort")


def _policy_selection_for_day(day: pd.DataFrame, *, k: int, max_source_slots: int) -> list[str]:
    ordered = _ordered_day(day)
    selected_codes = ordered[ordered["selection_rank"].le(k)]["code"].astype(str).tolist()
    source_codes_in_selected = set(ordered[ordered["code"].isin(selected_codes) & ordered["source_specific_candidate"]]["code"].astype(str).tolist())
    available_slots = max(max_source_slots - len(source_codes_in_selected), 0)
    if available_slots <= 0:
        return selected_codes[:k]

    source_candidates = ordered[ordered["source_specific_candidate"] & ~ordered["code"].isin(selected_codes)]
    if source_candidates.empty:
        return selected_codes[:k]

    selected = selected_codes[:k]
    for code in source_candidates["code"].astype(str).tolist()[:available_slots]:
        if len(selected) < k:
            selected.append(code)
            continue
        selected_frame = ordered[ordered["code"].isin(selected)].copy()
        selected_frame["_is_source"] = selected_frame["source_specific_candidate"].astype(bool)
        replace_pool = selected_frame[~selected_frame["_is_source"]]
        if replace_pool.empty:
            break
        replace_code = str(replace_pool.sort_values(["_rank_sort", "code"], ascending=[False, False]).iloc[0]["code"])
        selected = [item for item in selected if item != replace_code] + [code]
    return selected[:k]


def build_selection_ledger(frame: pd.DataFrame, *, max_source_slots: int, family_id: str, k: int = TOP_K) -> pd.DataFrame:
    rows = []
    for event_date, day in frame.groupby("event_date", sort=True):
        selected_codes = _policy_selection_for_day(day, k=k, max_source_slots=max_source_slots)
        selected_order = {code: idx + 1 for idx, code in enumerate(selected_codes)}
        for _, row in day.iterrows():
            code = str(row["code"])
            rows.append(
                {
                    "ranker_family_id": family_id,
                    "event_date": str(event_date),
                    "event_ymd": int(row["event_ymd"]),
                    "code": code,
                    "selection_rank": selected_order.get(code),
                    "selected_topk": code in selected_order,
                    "previous_best_selection_rank": float(row["selection_rank"]) if pd.notna(row["selection_rank"]) else None,
                    "source_specific_candidate": bool(row["source_specific_candidate"]),
                    "source_family": str(row["source_family"]),
                    "ret20_fwd": float(row["ret20_fwd"]),
                    "mfe20": float(row["mfe20"]),
                    "mae20": float(row["mae20"]),
                    "win20": bool(row["win20"]),
                    "severe_loss20": bool(row["severe_loss20"]),
                    "future_winner": bool(row["future_winner"]),
                    "is_future_top10_by_ret20": bool(row["is_future_top10_by_ret20"]),
                    "is_big_winner_ret20_ge_10pct": bool(row["is_big_winner_ret20_ge_10pct"]),
                    "is_big_winner_MFE20_ge_15pct": bool(row["is_big_winner_MFE20_ge_15pct"]),
                }
            )
    return pd.DataFrame(rows)


def _baseline_selected(frame: pd.DataFrame, k: int = TOP_K) -> pd.DataFrame:
    out = frame[frame["selection_rank"].le(k)].copy()
    out["ranker_family_id"] = BASELINE_FAMILY_ID
    out["selected_topk"] = True
    return out


def _selected_from_ledger(ledger: pd.DataFrame) -> pd.DataFrame:
    return ledger[ledger["selected_topk"].eq(True)].copy()


def _topk_metrics(selected: pd.DataFrame, frame: pd.DataFrame, *, family_id: str) -> dict[str, Any]:
    winner_total = int(frame["future_winner"].sum())
    day_rows = []
    for event_date, day in frame.groupby("event_date", sort=True):
        selected_day = selected[selected["event_date"].astype(str).eq(str(event_date))]
        winner_available = bool(day["future_winner"].any())
        selected_winner = bool(selected_day["future_winner"].any()) if len(selected_day) else False
        day_rows.append(
            {
                "event_date": str(event_date),
                "winner_available": winner_available,
                "selected_winner": selected_winner,
                "top3_any_severe_loss": bool(selected_day["severe_loss20"].any()) if len(selected_day) else False,
                "top3_worst_MAE20": float(pd.to_numeric(selected_day["mae20"], errors="coerce").min()) if len(selected_day) else None,
                "top3_avg_ret20": _mean(selected_day["ret20_fwd"]) if len(selected_day) else None,
            }
        )
    day_frame = pd.DataFrame(day_rows)
    return {
        "ranker_family_id": family_id,
        "selected_top3_count": int(len(selected)),
        "selected_top3_avg_ret20": _mean(selected["ret20_fwd"]),
        "selected_top3_win_rate20": float(pd.to_numeric(selected["ret20_fwd"], errors="coerce").gt(0.0).mean()) if len(selected) else 0.0,
        "selected_top3_avg_MFE20": _mean(selected["mfe20"]),
        "selected_top3_avg_MAE20": _mean(selected["mae20"]),
        "selected_top3_severe_loss_rate20": float(selected["severe_loss20"].astype(bool).mean()) if len(selected) else 0.0,
        "selected_top3_big_winner_capture_rate": _safe_rate(int(selected["future_winner"].sum()), winner_total),
        "selected_nonwinner_when_winner_available_rate": _safe_rate(
            int((day_frame["winner_available"] & ~day_frame["selected_winner"]).sum()),
            int(day_frame["winner_available"].sum()),
        ),
        "top3_day_any_severe_loss_rate": float(day_frame["top3_any_severe_loss"].mean()) if len(day_frame) else 0.0,
        "top3_day_worst_MAE20": _mean(day_frame["top3_worst_MAE20"]) if len(day_frame) else None,
    }


def _oracle_top3_avg(frame: pd.DataFrame) -> float | None:
    values = []
    for _event_date, day in frame.groupby("event_date", sort=True):
        values.extend(pd.to_numeric(day["ret20_fwd"], errors="coerce").sort_values(ascending=False).head(TOP_K).tolist())
    if not values:
        return None
    return float(np.mean(values))


def build_source_candidate_ledger(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    source = frame[frame["source_specific_candidate"]].copy()
    for _, row in source.iterrows():
        rows.append(
            {
                "event_date": str(row["event_date"]),
                "event_ymd": int(row["event_ymd"]),
                "code": str(row["code"]),
                "source_family": str(row["source_family"]),
                "source_specific_candidate": True,
                "previous_best_selection_rank": float(row["selection_rank"]) if pd.notna(row["selection_rank"]) else None,
                "overlap_with_previous_best_top3": bool(row["baseline_top3"]),
                "under_ranked_for_top3": bool(pd.isna(row["selection_rank"]) or row["selection_rank"] > TOP_K),
                "future_winner": bool(row["future_winner"]),
                "nonwinner": bool(row["nonwinner"]),
                "severe_loss20": bool(row["severe_loss20"]),
                "ret20_fwd": float(row["ret20_fwd"]),
                "mfe20": float(row["mfe20"]),
                "mae20": float(row["mae20"]),
            }
        )
    return rows


def build_source_overlap_audit(frame: pd.DataFrame) -> dict[str, Any]:
    source = frame[frame["source_specific_candidate"]]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_overlap_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_candidate_count": int(len(source)),
        "source_candidate_days": int(source["event_date"].nunique()),
        "source_candidate_overlap_with_previous_best_top3": int(source["baseline_top3"].sum()),
        "source_candidate_overlap_with_previous_best_top3_rate": _safe_rate(int(source["baseline_top3"].sum()), int(len(source))),
        "source_candidate_under_ranked_count": int((source["selection_rank"].isna() | source["selection_rank"].gt(TOP_K)).sum()),
        "source_candidate_under_ranked_rate": _safe_rate(int((source["selection_rank"].isna() | source["selection_rank"].gt(TOP_K)).sum()), int(len(source))),
        "source_already_captured": bool(_safe_rate(int(source["baseline_top3"].sum()), int(len(source))) >= 0.60),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_oracle_diagnostic(frame: pd.DataFrame) -> dict[str, Any]:
    rows = []
    source = frame[frame["source_specific_candidate"]]
    for event_date, day in source.groupby("event_date", sort=True):
        ordered = day.sort_values("ret20_fwd", ascending=False)
        top1 = ordered.head(1)
        top3 = ordered.head(TOP_K)
        rows.append(
            {
                "event_date": str(event_date),
                "source_candidate_count": int(len(day)),
                "source_oracle_top1_ret20": _mean(top1["ret20_fwd"]),
                "source_oracle_top3_ret20": _mean(top3["ret20_fwd"]),
                "source_oracle_top3_severe_loss_rate20": float(top3["severe_loss20"].astype(bool).mean()) if len(top3) else 0.0,
            }
        )
    day_frame = pd.DataFrame(rows)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_oracle_diagnostic_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_only_oracle_evaluation_only": True,
        "source_oracle_top1_avg_ret20": _mean(day_frame["source_oracle_top1_ret20"]) if len(day_frame) else None,
        "source_oracle_top3_avg_ret20": _mean(day_frame["source_oracle_top3_ret20"]) if len(day_frame) else None,
        "source_oracle_top3_severe_loss_rate20": _mean(day_frame["source_oracle_top3_severe_loss_rate20"]) if len(day_frame) else None,
        "rows_sample": rows[:500],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def _selection_sets(selected: pd.DataFrame) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for event_date, day in selected.groupby("event_date", sort=True):
        out[str(event_date)] = set(day["code"].astype(str).tolist())
    return out


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
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_top3_selection_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "wide_pool_oracle_top3_avg_ret20": oracle_avg,
        "previous_best": base_metrics,
        "source_specific_max1slot": primary_metrics,
        "source_specific_max2slot_diagnostic": diagnostic_metrics,
        "changed_top3_members_count_vs_previous_best": int(changed_top3),
        "changed_top5_members_count_vs_previous_best": int(changed_top5),
        "candidate_scoring_created": False,
        "threshold_policy_created": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_recovery_report(frame: pd.DataFrame, primary_selected: pd.DataFrame) -> dict[str, Any]:
    baseline_codes = set(zip(frame.loc[frame["baseline_top3"], "event_date"].astype(str), frame.loc[frame["baseline_top3"], "code"].astype(str)))
    selected_source = primary_selected[primary_selected["source_specific_candidate"]].copy()
    selected_source["newly_selected_vs_previous_best"] = [
        (str(row["event_date"]), str(row["code"])) not in baseline_codes for _, row in selected_source.iterrows()
    ]
    recovered = selected_source[selected_source["newly_selected_vs_previous_best"] & selected_source["future_winner"].astype(bool)]
    added_nonwinner = selected_source[selected_source["newly_selected_vs_previous_best"] & _nonwinner_mask(selected_source)]
    added_severe = selected_source[selected_source["newly_selected_vs_previous_best"] & selected_source["severe_loss20"].astype(bool)]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_recovery_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_candidate_selected_count": int(len(selected_source)),
        "source_candidate_selected_win_rate20": float(pd.to_numeric(selected_source["ret20_fwd"], errors="coerce").gt(0.0).mean()) if len(selected_source) else 0.0,
        "source_candidate_selected_avg_ret20": _mean(selected_source["ret20_fwd"]),
        "source_candidate_selected_severe_loss_rate20": float(selected_source["severe_loss20"].astype(bool).mean()) if len(selected_source) else 0.0,
        "source_candidate_recovered_winner_count": int(len(recovered)),
        "recovered_missed_winner_count": int(len(recovered)),
        "source_candidate_added_nonwinner_count": int(len(added_nonwinner)),
        "source_candidate_added_severe_loser_count": int(len(added_severe)),
        "source_candidate_added_count": int(selected_source["newly_selected_vs_previous_best"].sum()) if len(selected_source) else 0,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_source_noise_report(recovery: dict[str, Any]) -> dict[str, Any]:
    recovered = int(recovery.get("recovered_missed_winner_count") or 0)
    severe_added = int(recovery.get("source_candidate_added_severe_loser_count") or 0)
    nonwinner_added = int(recovery.get("source_candidate_added_nonwinner_count") or 0)
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_source_noise_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "recovered_missed_winner_count": recovered,
        "source_candidate_added_nonwinner_count": nonwinner_added,
        "source_candidate_added_severe_loser_count": severe_added,
        "severe_loser_added_per_recovered_winner": severe_added / recovered if recovered else None,
        "nonwinner_added_per_recovered_winner": nonwinner_added / recovered if recovered else None,
        "source_recovers_winners_but_too_noisy": bool(recovered > 0 and severe_added / recovered > 1.0),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_max3_source_slot_report(top3_report: dict[str, Any]) -> dict[str, Any]:
    primary = top3_report["source_specific_max1slot"]
    diagnostic = top3_report["source_specific_max2slot_diagnostic"]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_max3_source_slot_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "max_source_slots_per_day_primary": 1,
        "max_source_slots_per_day_diagnostic": 2,
        "primary_selected_top3_avg_ret20": primary.get("selected_top3_avg_ret20"),
        "diagnostic_selected_top3_avg_ret20": diagnostic.get("selected_top3_avg_ret20"),
        "primary_selected_top3_severe_loss_rate20": primary.get("selected_top3_severe_loss_rate20"),
        "diagnostic_selected_top3_severe_loss_rate20": diagnostic.get("selected_top3_severe_loss_rate20"),
        "all_top3_source_replacement_created": False,
        "threshold_policy_created": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_time_block_source_validation(frame: pd.DataFrame, primary_selected: pd.DataFrame) -> dict[str, Any]:
    baseline = _baseline_selected(frame, TOP_K)
    rows = []
    for block in sorted(frame["time_block"].astype(str).unique().tolist()):
        block_frame = frame[frame["time_block"].astype(str).eq(block)]
        block_base = baseline[baseline["time_block"].astype(str).eq(block)] if "time_block" in baseline.columns else baseline[baseline["event_date"].astype(str).str.slice(0, 4).eq(block)]
        block_primary = primary_selected[primary_selected["event_date"].astype(str).str.slice(0, 4).eq(block)]
        source_selected = block_primary[block_primary["source_specific_candidate"]]
        rows.append(
            {
                "time_block": block,
                "event_count": int(block_frame["event_date"].nunique()),
                "previous_best_top3_avg_ret20": _mean(block_base["ret20_fwd"]),
                "source_specific_top3_avg_ret20": _mean(block_primary["ret20_fwd"]),
                "top3_avg_ret20_delta": (_mean(block_primary["ret20_fwd"]) or 0.0) - (_mean(block_base["ret20_fwd"]) or 0.0),
                "source_candidate_selected_count": int(len(source_selected)),
                "source_candidate_selected_avg_ret20": _mean(source_selected["ret20_fwd"]),
                "source_candidate_selected_severe_loss_rate20": float(source_selected["severe_loss20"].astype(bool).mean()) if len(source_selected) else 0.0,
            }
        )
    deltas = [row["top3_avg_ret20_delta"] for row in rows]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_time_block_source_validation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "rows": rows,
        "time_block_count": len(rows),
        "positive_top3_delta_time_block_rate": float(np.mean([delta > 0.0 for delta in deltas])) if deltas else 0.0,
        "effect_stable_across_time_blocks": bool(deltas and np.mean([delta > 0.0 for delta in deltas]) >= 0.50),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_baseline_comparison_report(top3_report: dict[str, Any], recovery: dict[str, Any], noise: dict[str, Any], timeblock: dict[str, Any]) -> dict[str, Any]:
    base = top3_report["previous_best"]
    primary = top3_report["source_specific_max1slot"]
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_baseline_comparison_report_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "selected_top3_avg_ret20_delta_vs_previous_best": (primary.get("selected_top3_avg_ret20") or 0.0) - (base.get("selected_top3_avg_ret20") or 0.0),
        "oracle_top3_gap_delta_vs_previous_best": (primary.get("oracle_top3_gap_ret20") or 0.0) - (base.get("oracle_top3_gap_ret20") or 0.0),
        "selected_nonwinner_when_winner_available_delta_vs_previous_best": (primary.get("selected_nonwinner_when_winner_available_rate") or 0.0) - (base.get("selected_nonwinner_when_winner_available_rate") or 0.0),
        "selected_top3_severe_loss_rate_delta_vs_previous_best": (primary.get("selected_top3_severe_loss_rate20") or 0.0) - (base.get("selected_top3_severe_loss_rate20") or 0.0),
        "changed_top3_members_count_vs_previous_best": top3_report.get("changed_top3_members_count_vs_previous_best"),
        "changed_top5_members_count_vs_previous_best": top3_report.get("changed_top5_members_count_vs_previous_best"),
        "recovered_missed_winner_count": recovery.get("recovered_missed_winner_count"),
        "severe_loser_added_per_recovered_winner": noise.get("severe_loser_added_per_recovered_winner"),
        "effect_stable_across_time_blocks": timeblock.get("effect_stable_across_time_blocks"),
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_validation_outcome_classification(overlap: dict[str, Any], comparison: dict[str, Any], recovery: dict[str, Any], noise: dict[str, Any], timeblock: dict[str, Any]) -> dict[str, Any]:
    top3_improved = comparison.get("selected_top3_avg_ret20_delta_vs_previous_best", 0.0) > 0.0
    oracle_improved = comparison.get("oracle_top3_gap_delta_vs_previous_best", 0.0) > 0.0
    recovered = int(recovery.get("recovered_missed_winner_count") or 0)
    severe_ratio = noise.get("severe_loser_added_per_recovered_winner")
    source_already_captured = overlap.get("source_already_captured") is True
    too_noisy = bool(severe_ratio is not None and severe_ratio > 1.0)
    timeblock_specific = not bool(timeblock.get("effect_stable_across_time_blocks"))
    if source_already_captured:
        outcome = "source_already_captured"
    elif recovered > 0 and top3_improved and oracle_improved and not too_noisy:
        outcome = "source_recovers_winners_cleanly"
    elif recovered > 0 and (too_noisy or comparison.get("selected_top3_severe_loss_rate_delta_vs_previous_best", 0.0) > 0.02):
        outcome = "source_recovers_winners_but_too_noisy"
    elif recovered > 0 and not top3_improved:
        outcome = "source_under_ranked_but_unusable"
    elif timeblock_specific:
        outcome = "source_timeblock_specific"
    else:
        outcome = "source_under_ranked_but_unusable"
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_validation_outcome_classification_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "validation_outcome": outcome,
        "source_recovers_winners_cleanly": outcome == "source_recovers_winners_cleanly",
        "source_recovers_winners_but_too_noisy": outcome == "source_recovers_winners_but_too_noisy",
        "source_already_captured": source_already_captured,
        "source_under_ranked_but_unusable": outcome == "source_under_ranked_but_unusable",
        "source_timeblock_specific": timeblock_specific,
        "evidence": {
            "top3_improved": top3_improved,
            "oracle_gap_improved": oracle_improved,
            "recovered_missed_winner_count": recovered,
            "severe_loser_added_per_recovered_winner": severe_ratio,
            "source_candidate_overlap_with_previous_best_top3_rate": overlap.get("source_candidate_overlap_with_previous_best_top3_rate"),
            "effect_stable_across_time_blocks": timeblock.get("effect_stable_across_time_blocks"),
        },
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_next_axis_recommendation(outcome: dict[str, Any]) -> dict[str, Any]:
    if outcome.get("source_recovers_winners_cleanly"):
        next_axis = "source_specific_candidate_generation_challenger_v2"
        reason = "source validation improved max3 quality without unacceptable noise; next step can build a bounded challenger contract"
    elif outcome.get("source_recovers_winners_but_too_noisy"):
        next_axis = "source_noise_decomposition_before_candidate_generation_v2"
        reason = "source recovers winners but adds too many bad picks; decompose source noise before promotion"
    elif outcome.get("source_timeblock_specific"):
        next_axis = "source_specific_timeblock_applicability_audit_v1"
        reason = "source effect is regime/time-block specific"
    else:
        next_axis = "candidate_generation_hypothesis_rebuild_v1"
        reason = "source validation did not improve previous_best max3 operating quality"
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_recommendation_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "one_recommended_next_axis_only": True,
        "recommended_next_axis": next_axis,
        "reason": reason,
        "do_not_continue_axes": [
            "learned scorer",
            "previous_best coefficient tuning",
            "ranking objective re-tuning",
            "threshold/no-trade",
            "safe_full hard filter",
            "negative_guard hard veto",
            "image fusion",
        ],
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_contract_artifacts(*, source_dirs: dict[str, Path], frame: pd.DataFrame, source_status: dict[str, Any], hypothesis: dict[str, Any]) -> dict[str, dict[str, Any]]:
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
        "research_phase": "source_specific_candidate_generation_validation",
        "boundary": "TRADEX-only",
        "axis_moved": "source_specific_candidate_generation_validation",
        "source_missed_winner_decision": source_status["missed_winner"]["research_decision.json"].get("authoritative_research_decision"),
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
    }
    evaluation_contract["contract_hash"] = _stable_hash(evaluation_contract)
    source_generation_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_source_generation_contract_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "primary_challenger": "source_specific_candidate_generation_v1",
        "source_family": hypothesis.get("source_family"),
        "max_source_slots_per_day_primary": 1,
        "max_source_slots_per_day_diagnostic": 2,
        "candidate_generation_challenger_created": True,
        "candidate_scoring_created": False,
        "future_labels_used_for_evaluation_only": True,
        "future_labels_used_in_candidate_generation": False,
        "safe_full_used_as_hard_filter": False,
        "negative_guard_used_as_hard_veto": False,
    }
    source_generation_contract["contract_hash"] = _stable_hash(source_generation_contract)
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
        "source_generation_contract.json": source_generation_contract,
    }


def build_research_decision(
    *,
    top3_report: dict[str, Any],
    comparison: dict[str, Any],
    recovery: dict[str, Any],
    noise: dict[str, Any],
    timeblock: dict[str, Any],
    outcome: dict[str, Any],
    artifact_complete: bool,
) -> dict[str, Any]:
    return_improved = comparison.get("selected_top3_avg_ret20_delta_vs_previous_best", 0.0) > 0.0
    oracle_improved = comparison.get("oracle_top3_gap_delta_vs_previous_best", 0.0) > 0.0
    nonwinner_delta = comparison.get("selected_nonwinner_when_winner_available_delta_vs_previous_best", 0.0)
    nonwinner_ok = nonwinner_delta <= 0.0
    severe_delta = comparison.get("selected_top3_severe_loss_rate_delta_vs_previous_best", 0.0)
    severe_ok = severe_delta <= 0.02
    recovered = int(recovery.get("recovered_missed_winner_count") or 0)
    severe_ratio = noise.get("severe_loser_added_per_recovered_winner")
    noise_ok = severe_ratio is not None and severe_ratio <= 1.0
    branching = int(top3_report.get("changed_top3_members_count_vs_previous_best") or 0) > 0
    stable = timeblock.get("effect_stable_across_time_blocks") is True
    if artifact_complete and return_improved and oracle_improved and nonwinner_ok and severe_ok and recovered > 0 and noise_ok and branching and stable:
        decision = "keep_candidate"
        authoritative = "source_specific_candidate_generation_keep_candidate"
    elif artifact_complete and recovered > 0 and branching and (return_improved or oracle_improved):
        decision = "hold"
        authoritative = "source_specific_candidate_generation_hold"
    else:
        decision = "drop"
        authoritative = "source_specific_candidate_generation_drop"
    typed_reasons = [
        "top3_return_improved" if return_improved else "top3_return_not_improved",
        "oracle_gap_improved" if oracle_improved else "oracle_gap_not_improved",
        "nonwinner_when_winner_available_not_worse" if nonwinner_ok else "nonwinner_when_winner_available_worse",
        "severe_loss_not_materially_worse" if severe_ok else "severe_loss_materially_worse",
        "recovered_missed_winners_meaningful" if recovered > 0 else "recovered_missed_winners_too_few",
        "real_branching_observed" if branching else "no_real_branching",
        "time_block_effect_stable" if stable else "time_block_effect_not_stable",
        "diagnosis_no_scorer_created",
    ]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "research_phase": "source_specific_candidate_generation_validation",
        "boundary": "TRADEX-only",
        "axis_moved": "source_specific_candidate_generation_validation",
        "source_missed_winner_decision": "missed_winner_source_hypothesis_ready",
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
        "max_source_slots_per_day_primary": 1,
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
            {"code": "selected_top3_avg_ret20_improves_vs_previous_best", "status": "pass" if return_improved else "fail", "value": comparison.get("selected_top3_avg_ret20_delta_vs_previous_best")},
            {"code": "oracle_top3_gap_ret20_improves_vs_previous_best", "status": "pass" if oracle_improved else "fail", "value": comparison.get("oracle_top3_gap_delta_vs_previous_best")},
            {"code": "selected_nonwinner_when_winner_available_not_worse", "status": "pass" if nonwinner_ok else "fail", "value": nonwinner_delta},
            {"code": "selected_top3_severe_loss_rate_not_materially_worse", "status": "pass" if severe_ok else "fail", "value": severe_delta},
            {"code": "recovered_missed_winner_count", "status": "pass" if recovered > 0 else "fail", "value": recovered},
            {"code": "changed_top3_members_count_vs_previous_best", "status": "pass" if branching else "fail", "value": top3_report.get("changed_top3_members_count_vs_previous_best")},
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
    }


def run_source_specific_candidate_generation_validation_v1(
    *,
    source_missed_winner_run_id: str = DEFAULT_MISSED_WINNER_RUN_ID,
    source_root_cause_run_id: str = DEFAULT_ROOT_CAUSE_RUN_ID,
    source_wide_run_id: str = DEFAULT_WIDE_RUN_ID,
    source_pattern_run_id: str = DEFAULT_PATTERN_RUN_ID,
    source_upside_run_id: str = DEFAULT_UPSIDE_RUN_ID,
    missed_winner_root: str | Path = DEFAULT_MISSED_WINNER_ROOT,
    root_cause_root: str | Path = DEFAULT_ROOT_CAUSE_ROOT,
    wide_root: str | Path = DEFAULT_WIDE_ROOT,
    pattern_root: str | Path = DEFAULT_PATTERN_ROOT,
    upside_root: str | Path = DEFAULT_UPSIDE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
) -> dict[str, Any]:
    missed_dir = _run_dir(missed_winner_root, source_missed_winner_run_id, DEFAULT_MISSED_WINNER_ROOT)
    root_cause_dir = _run_dir(root_cause_root, source_root_cause_run_id, DEFAULT_ROOT_CAUSE_ROOT)
    wide_dir = _run_dir(wide_root, source_wide_run_id, DEFAULT_WIDE_ROOT)
    pattern_dir = _run_dir(pattern_root, source_pattern_run_id, DEFAULT_PATTERN_ROOT)
    upside_dir = _run_dir(upside_root, source_upside_run_id, DEFAULT_UPSIDE_ROOT)
    output_dir = _safe_path(output_root, DEFAULT_OUTPUT_ROOT) / (run_id.strip() if isinstance(run_id, str) and run_id.strip() else _default_run_id())
    source_status = validate_sources(
        missed_winner_dir=missed_dir,
        root_cause_dir=root_cause_dir,
        wide_dir=wide_dir,
        pattern_dir=pattern_dir,
        upside_dir=upside_dir,
    )
    frame, _ledger = load_validation_inputs(source_status=source_status, wide_dir=wide_dir)
    hypothesis = _source_hypothesis(source_status)
    frame = _source_frame(frame, str(hypothesis["source_family"]))
    primary_ledger = build_selection_ledger(frame, max_source_slots=1, family_id=PRIMARY_FAMILY_ID)
    diagnostic_ledger = build_selection_ledger(frame, max_source_slots=2, family_id=DIAGNOSTIC_FAMILY_ID)
    primary_selected = _selected_from_ledger(primary_ledger)
    diagnostic_selected = _selected_from_ledger(diagnostic_ledger)
    source_candidate_rows = build_source_candidate_ledger(frame)
    overlap = build_source_overlap_audit(frame)
    source_oracle = build_source_oracle_diagnostic(frame)
    top3_report = build_top3_selection_report(frame, primary_selected, diagnostic_selected)
    recovery = build_source_recovery_report(frame, primary_selected)
    noise = build_source_noise_report(recovery)
    max3_slots = build_max3_source_slot_report(top3_report)
    timeblock = build_time_block_source_validation(frame, primary_selected)
    comparison = build_baseline_comparison_report(top3_report, recovery, noise, timeblock)
    outcome = build_validation_outcome_classification(overlap, comparison, recovery, noise, timeblock)
    next_axis = build_next_axis_recommendation(outcome)
    source_dirs = {
        "missed_winner": missed_dir,
        "root_cause": root_cause_dir,
        "wide": wide_dir,
        "pattern": pattern_dir,
        "upside": upside_dir,
    }
    contract_artifacts = build_contract_artifacts(source_dirs=source_dirs, frame=frame, source_status=source_status, hypothesis=hypothesis)
    run_manifest = contracts.build_run_manifest(
        session_id=output_dir.name,
        seed=ranking_mod.RANDOM_SEED,
        random_seed=ranking_mod.RANDOM_SEED,
        input_artifacts=[{"name": key, "path": str(value)} for key, value in source_dirs.items()],
        asof=str(int(frame["event_ymd"].max())),
        config={
            "axis_id": AXIS_ID,
            "source_family": hypothesis.get("source_family"),
            "candidate_generation_challenger_created": True,
            "candidate_scoring_created": False,
            "ranking_objective_created": False,
            "threshold_policy_created": False,
            "image_score_used": False,
            "fusion_reranker_created": False,
            "production_ranking_changed": False,
        },
        universe=sorted(frame["code"].astype(str).unique().tolist()),
        period={"start_date": str(int(frame["event_ymd"].min())), "end_date": str(int(frame["event_ymd"].max())), "label": "source_specific_candidate_generation_validation"},
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
        "source_overlap_audit.json": overlap,
        "source_oracle_diagnostic.json": source_oracle,
        "top3_selection_report.json": top3_report,
        "source_recovery_report.json": recovery,
        "source_noise_report.json": noise,
        "max3_source_slot_report.json": max3_slots,
        "time_block_source_validation.json": timeblock,
        "baseline_comparison_report.json": comparison,
        "validation_outcome_classification.json": outcome,
        "next_axis_recommendation.json": next_axis,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["source_candidate_ledger.jsonl"] = str(_write_jsonl(output_dir / "source_candidate_ledger.jsonl", source_candidate_rows))
    pre_complete = _artifact_complete(output_dir, paths)
    decision = build_research_decision(
        top3_report=top3_report,
        comparison=comparison,
        recovery=recovery,
        noise=noise,
        timeblock=timeblock,
        outcome=outcome,
        artifact_complete=bool(pre_complete["complete"]),
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
        "recommended_next_axis": decision["recommended_next_axis"],
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
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-missed-winner-run-id", default=DEFAULT_MISSED_WINNER_RUN_ID)
    parser.add_argument("--source-root-cause-run-id", default=DEFAULT_ROOT_CAUSE_RUN_ID)
    parser.add_argument("--source-wide-run-id", default=DEFAULT_WIDE_RUN_ID)
    parser.add_argument("--source-pattern-run-id", default=DEFAULT_PATTERN_RUN_ID)
    parser.add_argument("--source-upside-run-id", default=DEFAULT_UPSIDE_RUN_ID)
    parser.add_argument("--missed-winner-root", default=str(DEFAULT_MISSED_WINNER_ROOT))
    parser.add_argument("--root-cause-root", default=str(DEFAULT_ROOT_CAUSE_ROOT))
    parser.add_argument("--wide-root", default=str(DEFAULT_WIDE_ROOT))
    parser.add_argument("--pattern-root", default=str(DEFAULT_PATTERN_ROOT))
    parser.add_argument("--upside-root", default=str(DEFAULT_UPSIDE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = run_source_specific_candidate_generation_validation_v1(
        source_missed_winner_run_id=args.source_missed_winner_run_id,
        source_root_cause_run_id=args.source_root_cause_run_id,
        source_wide_run_id=args.source_wide_run_id,
        source_pattern_run_id=args.source_pattern_run_id,
        source_upside_run_id=args.source_upside_run_id,
        missed_winner_root=args.missed_winner_root,
        root_cause_root=args.root_cause_root,
        wide_root=args.wide_root,
        pattern_root=args.pattern_root,
        upside_root=args.upside_root,
        output_root=args.output_root,
        run_id=args.run_id,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
