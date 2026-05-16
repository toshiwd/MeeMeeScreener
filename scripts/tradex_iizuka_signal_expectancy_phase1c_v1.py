from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_signal_expectancy_phase1b_v1 as phase1b
from scripts import tradex_iizuka_signal_expectancy_v1 as base

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1c")
SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase1c_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase1c_v1"
MANIFEST_SCHEMA_VERSION = f"{SCHEMA_VERSION}_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = f"{SCHEMA_VERSION}_input_resolution_v1"
DEFINITION_SCHEMA_VERSION = f"{SCHEMA_VERSION}_candidate_definition_snapshot_v1"
MIXED_COMPARE_SCHEMA_VERSION = f"{SCHEMA_VERSION}_mixed_expectancy_compare_v1"
COMBINATION_COMPARE_SCHEMA_VERSION = f"{SCHEMA_VERSION}_internal_combination_compare_v1"
LOSING_SCHEMA_VERSION = f"{SCHEMA_VERSION}_losing_pattern_inspection_v1"
KOMA_SCHEMA_VERSION = f"{SCHEMA_VERSION}_reference_koma_comparison_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = f"{SCHEMA_VERSION}_no_lookahead_audit_v1"
DECISION_SCHEMA_VERSION = f"{SCHEMA_VERSION}_signal_decision_v1"
COMPLETE_SCHEMA_VERSION = f"{SCHEMA_VERSION}_artifact_complete_v1"

CANDIDATE_ID = "monthly_C_mixed_pullback_end_reclaim7_v1"
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase1c_candidate_definition_snapshot.json",
    "phase1c_mixed_signal_rows.parquet",
    "phase1c_mixed_expectancy_compare.json",
    "phase1c_internal_combination_compare.json",
    "phase1c_losing_pattern_inspection.json",
    "phase1c_reference_koma_comparison.json",
    "phase1c_no_lookahead_audit.json",
    "phase1c_signal_decision.json",
    "_ARTIFACT_COMPLETE.json",
]
COMBINATIONS = (
    "lower_wick+koma",
    "lower_wick+horizontal",
    "koma+horizontal",
    "lower_wick+koma+horizontal",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    return base._json_ready(value)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return base._write_json(path, payload)


def _write_parquet(path: Path, frame: pd.DataFrame) -> Path:
    return base._write_parquet(path, frame)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _combination(row: pd.Series) -> str | None:
    lower = bool(row.get("lower_wick_flag"))
    koma = bool(row.get("koma_flag"))
    horizontal = bool(row.get("horizontal_flag"))
    if lower and koma and horizontal:
        return "lower_wick+koma+horizontal"
    if lower and koma:
        return "lower_wick+koma"
    if lower and horizontal:
        return "lower_wick+horizontal"
    if koma and horizontal:
        return "koma+horizontal"
    return None


def _compare_against_baseline(
    *,
    rows: pd.DataFrame,
    eligible_b2: pd.DataFrame,
    random_seed: int,
    repetitions: int,
    research_fallback: bool,
) -> dict[str, Any]:
    baseline = phase1b._repeated_baseline_distribution(
        eligible_b2,
        rows,
        random_seed=random_seed,
        repetitions=repetitions,
    )
    metrics = base._metrics(rows)
    concentration = phase1b._concentration(rows)
    decision = phase1b._decision_for_metrics(
        signal_metrics=metrics,
        baseline_distribution=baseline["distribution"],
        concentration=concentration,
        no_lookahead_pass=True,
        research_fallback=research_fallback,
    )
    return {
        "signal": metrics,
        "baseline_2": baseline,
        "baseline_2_deltas": base._delta(metrics, baseline["distribution"]),
        "concentration": concentration,
        "decision": decision,
    }


def _losing_patterns(mixed_rows: pd.DataFrame) -> dict[str, Any]:
    work = mixed_rows.copy()
    work["ret20_loser"] = pd.to_numeric(work["ret20"], errors="coerce") < 0
    work["severe_loser"] = work["ret20_loser"] & (pd.to_numeric(work["mae20"], errors="coerce") <= -0.08)
    work["failed_followthrough"] = (pd.to_numeric(work["ret5"], errors="coerce") <= 0) & (pd.to_numeric(work["ret20"], errors="coerce") <= 0)
    work["adverse_first"] = pd.to_numeric(work["mae20"], errors="coerce") <= -0.05
    work["year"] = work["decision_date"].astype(str).str.slice(0, 4)

    def bucket(mask_col: str) -> dict[str, Any]:
        subset = work.loc[work[mask_col].fillna(False)].copy()
        return {
            "count": int(len(subset)),
            "share_of_mixed_rows": float(len(subset) / len(work)) if len(work) else None,
            "combination_distribution": subset["mixed_internal_combination"].value_counts(dropna=False).to_dict(),
            "year_distribution": subset["year"].value_counts(dropna=False).to_dict(),
            "sector_distribution": subset["sector"].value_counts(dropna=False).head(20).to_dict() if "sector" in subset.columns else {},
            "liquidity_or_volume_bucket_distribution": subset["liquidity_bucket"].value_counts(dropna=False).to_dict() if "liquidity_bucket" in subset.columns else {},
            "largest_combination_share": base._largest_share(subset, "mixed_internal_combination"),
            "largest_year_share": base._largest_share(subset, "year"),
            "largest_sector_share": base._largest_share(subset, "sector"),
            "largest_liquidity_or_volume_bucket_share": base._largest_share(subset, "liquidity_bucket"),
        }

    return {
        "schema_version": LOSING_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "diagnostic_only": True,
        "definitions": {
            "ret20_loser": "ret20 < 0",
            "severe_loser": "ret20 < 0 and MAE_through_20_sessions <= -0.08",
            "failed_followthrough": "ret5 <= 0 and ret20 <= 0",
            "adverse_first": "MAE_through_20_sessions <= -0.05 before ret20_horizon_date",
        },
        "ret20_loser": bucket("ret20_loser"),
        "severe_loser": bucket("severe_loser"),
        "failed_followthrough": bucket("failed_followthrough"),
        "adverse_first": bucket("adverse_first"),
    }


def run_phase1c(
    *,
    source_rows_parquet: Path,
    output_root: Path,
    random_seed: int = 20260509,
    baseline_repetitions: int = 100,
) -> dict[str, Any]:
    started = time.perf_counter()
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    source = pd.read_parquet(source_rows_parquet)
    normalized = base._normalize_source_frame(source)
    evaluated_frame = base._attach_forward_outcomes(base._attach_signal_flags(normalized))
    signal_rows = base._build_signal_rows(normalized)
    eligible_b2 = base._eligible_rows(evaluated_frame, "baseline_2")
    repetitions = max(1, int(baseline_repetitions))
    research_fallback = repetitions < 100

    mixed = signal_rows.loc[signal_rows["signal_subtype"].astype(str) == "mixed_reclaim7"].copy().reset_index(drop=True)
    mixed["mixed_internal_combination"] = mixed.apply(_combination, axis=1)
    koma = signal_rows.loc[signal_rows["signal_subtype"].astype(str) == "koma_reclaim7"].copy().reset_index(drop=True)
    no_lookahead = base._build_no_lookahead_audit(mixed, list(source.columns))

    mixed_compare = _compare_against_baseline(
        rows=mixed,
        eligible_b2=eligible_b2,
        random_seed=random_seed,
        repetitions=repetitions,
        research_fallback=research_fallback,
    )
    combination_payload: dict[str, Any] = {}
    strongest: dict[str, Any] | None = None
    weakest: dict[str, Any] | None = None
    for combo in COMBINATIONS:
        subset = mixed.loc[mixed["mixed_internal_combination"] == combo].copy()
        payload = _compare_against_baseline(
            rows=subset,
            eligible_b2=eligible_b2,
            random_seed=random_seed + sum(ord(ch) for ch in combo),
            repetitions=repetitions,
            research_fallback=research_fallback,
        )
        combination_payload[combo] = payload
        ret20_median = payload["signal"].get("ret20_median")
        item = {
            "combination": combo,
            "ret20_median": ret20_median,
            "decision": payload["decision"]["decision"],
            "signal_count": payload["signal"]["count"],
        }
        if ret20_median is not None and (strongest is None or ret20_median > strongest["ret20_median"]):
            strongest = item
        if ret20_median is not None and (weakest is None or ret20_median < weakest["ret20_median"]):
            weakest = item

    reference_koma = _compare_against_baseline(
        rows=koma,
        eligible_b2=eligible_b2,
        random_seed=random_seed + 911,
        repetitions=repetitions,
        research_fallback=research_fallback,
    )
    reference_koma["reference_only"] = True
    reference_koma["promotion_allowed"] = False
    reference_koma["promotion_block_reason"] = "koma_reclaim7 count remains below keep gate and is diagnostic-only in Phase 1c"

    decision = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "candidate_local_decision": mixed_compare["decision"]["decision"],
        "session_aggregate_decision": mixed_compare["decision"]["decision"],
        "authoritative_rollup_decision": mixed_compare["decision"]["decision"],
        "decision_reason": mixed_compare["decision"]["decision_reason"],
        "research_fallback": research_fallback,
        "baseline_repeat_count": repetitions,
        "signal": mixed_compare["signal"],
        "baseline_2_deltas": mixed_compare["baseline_2_deltas"],
        "concentration": mixed_compare["concentration"],
        "primary_baseline_beats": mixed_compare["decision"]["primary_baseline_beats"],
        "keep_gates": mixed_compare["decision"]["keep_gates"],
        "mae_median_worse_ratio_vs_baseline_2": mixed_compare["decision"]["mae_median_worse_ratio_vs_baseline_2"],
        "strongest_internal_combination": strongest,
        "weakest_internal_combination": weakest,
        "ranking_challenger_eligible_future_review": mixed_compare["decision"]["decision"] == "keep",
        "meemee_reflection": "blocked",
        "ranking_challenger": "blocked",
        "publish": "blocked",
    }
    definition_snapshot = {
        "schema_version": DEFINITION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "definition_source": "existing mixed_reclaim7 rows emitted by Phase 1b-compatible evaluator",
        "signal_definition_changed": False,
        "thresholds_changed": False,
        "mixed_definition": "at least two of lower_wick_flag, koma_flag, horizontal_flag are true within monthly_C_pullback_end_reclaim7_v1",
        "internal_combinations": list(COMBINATIONS),
        "non_goals": [
            "No threshold changes",
            "No horizontal redefinition",
            "No koma count expansion",
            "No sell-side research",
            "No unknown pattern discovery",
            "No MeeMee reflection",
            "No ranking challenger",
            "No publish",
        ],
    }
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_root": str(session_root),
        "candidate_id": CANDIDATE_ID,
        "boundary": "TRADEX-only",
        "research_only": True,
        "baseline_repeat_count": repetitions,
        "random_seed": int(random_seed),
        "research_fallback": research_fallback,
        "runtime_seconds": float(time.perf_counter() - started),
        "signal_definition_changed": False,
        "thresholds_changed": False,
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_rows_parquet": str(source_rows_parquet),
        "source_row_count": int(len(source)),
        "normalized_row_count": int(len(normalized)),
        "eligible_baseline_2_row_count": int(len(eligible_b2)),
        "raw_signal_row_count": int(signal_rows.attrs.get("raw_signal_row_count", len(signal_rows))),
        "all_signal_row_count": int(len(signal_rows)),
        "mixed_signal_row_count": int(len(mixed)),
        "reference_koma_row_count": int(len(koma)),
        "excluded_no_lookahead_invalid_count": int(signal_rows.attrs.get("excluded_no_lookahead_invalid_count", 0)),
    }
    internal_compare = {
        "schema_version": COMBINATION_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "baseline_id": "baseline_2",
        "baseline_repeat_count": repetitions,
        "research_fallback": research_fallback,
        "combinations": combination_payload,
    }
    mixed_expectancy = {
        "schema_version": MIXED_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "baseline_id": "baseline_2",
        "baseline_repeat_count": repetitions,
        "research_fallback": research_fallback,
        "mixed_reclaim7": mixed_compare,
    }

    _write_json(session_root / "run_manifest.json", manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "phase1c_candidate_definition_snapshot.json", definition_snapshot)
    _write_parquet(session_root / "phase1c_mixed_signal_rows.parquet", mixed)
    _write_json(session_root / "phase1c_mixed_expectancy_compare.json", mixed_expectancy)
    _write_json(session_root / "phase1c_internal_combination_compare.json", internal_compare)
    _write_json(session_root / "phase1c_losing_pattern_inspection.json", _losing_patterns(mixed))
    _write_json(session_root / "phase1c_reference_koma_comparison.json", {**reference_koma, "schema_version": KOMA_SCHEMA_VERSION})
    _write_json(session_root / "phase1c_no_lookahead_audit.json", {**no_lookahead, "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION})
    _write_json(session_root / "phase1c_signal_decision.json", decision)
    complete = {
        "schema_version": COMPLETE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "session_root": str(session_root),
        "required_artifacts": REQUIRED_ARTIFACTS,
        "all_present": all((session_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
    }
    _write_json(session_root / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "session_root": str(session_root),
        "baseline_repeat_count": repetitions,
        "research_fallback": research_fallback,
        "decision": decision["authoritative_rollup_decision"],
        "artifact_refs": {artifact: str(session_root / artifact) for artifact in REQUIRED_ARTIFACTS},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka Phase 1c mixed reclaim7 candidate evaluation")
    parser.add_argument("--source-rows-parquet", required=True)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--random-seed", type=int, default=20260509)
    parser.add_argument("--baseline-repetitions", type=int, default=100)
    args = parser.parse_args()
    result = run_phase1c(
        source_rows_parquet=_safe_path(args.source_rows_parquet, Path(args.source_rows_parquet)),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        random_seed=args.random_seed,
        baseline_repetitions=args.baseline_repetitions,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
