from __future__ import annotations

import argparse
import json
import math
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

from scripts import tradex_iizuka_signal_expectancy_v1 as base

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\iizuka_signal_expectancy_v1_phase1b")
SCRIPT_NAME = "tradex_iizuka_signal_expectancy_phase1b_v1"
SCHEMA_VERSION = "tradex_iizuka_signal_expectancy_phase1b_v1"
MANIFEST_SCHEMA_VERSION = f"{SCHEMA_VERSION}_manifest_v1"
INPUT_RESOLUTION_SCHEMA_VERSION = f"{SCHEMA_VERSION}_input_resolution_v1"
BASELINE_STABILITY_SCHEMA_VERSION = f"{SCHEMA_VERSION}_baseline_stability_v1"
SUBTYPE_COMPARE_SCHEMA_VERSION = f"{SCHEMA_VERSION}_subtype_expectancy_compare_v1"
SUBTYPE_BREAKDOWNS_SCHEMA_VERSION = f"{SCHEMA_VERSION}_subtype_breakdowns_v1"
NO_LOOKAHEAD_SCHEMA_VERSION = f"{SCHEMA_VERSION}_no_lookahead_audit_v1"
REDECISION_SCHEMA_VERSION = f"{SCHEMA_VERSION}_signal_redecision_v1"
COMPLETE_SCHEMA_VERSION = f"{SCHEMA_VERSION}_artifact_complete_v1"

SUBTYPES = ("horizontal_reclaim7", "mixed_reclaim7", "lower_wick_reclaim7", "koma_reclaim7")
REQUIRED_ARTIFACTS = [
    "run_manifest.json",
    "input_resolution.json",
    "phase1b_baseline_stability.json",
    "phase1b_subtype_expectancy_compare.json",
    "phase1b_subtype_breakdowns.json",
    "phase1b_no_lookahead_audit.json",
    "phase1b_signal_redecision.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{now.microsecond:06d}"


def _json_ready(value: Any) -> Any:
    return base._json_ready(value)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    return base._write_json(path, payload)


def _safe_path(value: str | Path | None, default: Path) -> Path:
    return base._safe_path(value, default)


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    return base._metrics(frame)


def _distribution(per_rep: list[dict[str, Any]]) -> dict[str, Any]:
    if not per_rep:
        return {}
    out: dict[str, Any] = {}
    for key in [item for item in per_rep[0].keys() if item != "baseline_repetition"]:
        series = pd.Series([row.get(key) for row in per_rep], dtype="float64").dropna()
        out[key] = {
            "mean": float(series.mean()) if len(series) else None,
            "median": float(series.median()) if len(series) else None,
            "p25": float(series.quantile(0.25)) if len(series) else None,
            "p75": float(series.quantile(0.75)) if len(series) else None,
        }
    return out


def _sample_group_indices(pool_indices: np.ndarray, take: int, *, seed: int, replace: bool) -> np.ndarray:
    rng = np.random.default_rng(seed)
    if len(pool_indices) == 0 or take <= 0:
        return np.array([], dtype=np.int64)
    return rng.choice(pool_indices, size=take, replace=replace)


def _repeated_baseline_distribution(
    eligible: pd.DataFrame,
    signal_rows: pd.DataFrame,
    *,
    random_seed: int,
    repetitions: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    if signal_rows.empty:
        return {
            "repetitions": int(repetitions),
            "aggregate": _metrics(signal_rows),
            "distribution": {},
            "runtime_seconds": 0.0,
        }

    eligible_by_date = {
        str(date): group.index.to_numpy(dtype=np.int64)
        for date, group in eligible.groupby("date", sort=False)
    }
    counts_by_date = signal_rows.groupby("decision_date", sort=True).size().to_dict()
    per_rep: list[dict[str, Any]] = []
    all_sample_indices: list[np.ndarray] = []
    for repetition in range(repetitions):
        sampled_parts: list[np.ndarray] = []
        for date, count in counts_by_date.items():
            pool = eligible_by_date.get(str(date), np.array([], dtype=np.int64))
            if len(pool) == 0:
                continue
            seed = int(random_seed + repetition * 1_000_003 + sum(ord(ch) for ch in str(date)))
            sampled_parts.append(_sample_group_indices(pool, int(count), seed=seed, replace=len(pool) < int(count)))
        sample_idx = np.concatenate(sampled_parts) if sampled_parts else np.array([], dtype=np.int64)
        sample = eligible.loc[sample_idx].copy() if len(sample_idx) else eligible.head(0).copy()
        metric = _metrics(sample)
        metric["baseline_repetition"] = repetition
        per_rep.append(metric)
        all_sample_indices.append(sample_idx)
    aggregate_indices = np.concatenate(all_sample_indices) if all_sample_indices else np.array([], dtype=np.int64)
    aggregate = eligible.loc[aggregate_indices].copy() if len(aggregate_indices) else eligible.head(0).copy()
    return {
        "repetitions": int(repetitions),
        "aggregate": _metrics(aggregate),
        "distribution": _distribution(per_rep),
        "runtime_seconds": float(time.perf_counter() - started),
    }


def _delta(signal_metrics: dict[str, Any], baseline_distribution: dict[str, Any]) -> dict[str, Any]:
    return base._delta(signal_metrics, baseline_distribution)


def _largest_share(frame: pd.DataFrame, column: str) -> float | None:
    return base._largest_share(frame, column)


def _concentration(frame: pd.DataFrame) -> dict[str, Any]:
    work = frame.copy()
    if "year" not in work.columns:
        work["year"] = work["decision_date"].astype(str).str.slice(0, 4)
    return {
        "largest_year_share": _largest_share(work, "year"),
        "largest_sector_share": _largest_share(work, "sector"),
        "largest_liquidity_or_volume_bucket_share": _largest_share(work, "liquidity_bucket"),
    }


def _mae_ratio(signal_mae: float | None, baseline_mae: float | None) -> float | None:
    return base._worse_mae_ratio(signal_mae, baseline_mae)


def _decision_for_metrics(
    *,
    signal_metrics: dict[str, Any],
    baseline_distribution: dict[str, Any],
    concentration: dict[str, Any],
    no_lookahead_pass: bool,
    research_fallback: bool,
) -> dict[str, Any]:
    beats = {
        "ret20_mean_beats_baseline_2": signal_metrics.get("ret20_mean") is not None
        and baseline_distribution.get("ret20_mean", {}).get("median") is not None
        and signal_metrics["ret20_mean"] > baseline_distribution["ret20_mean"]["median"],
        "ret20_median_beats_baseline_2": signal_metrics.get("ret20_median") is not None
        and baseline_distribution.get("ret20_median", {}).get("median") is not None
        and signal_metrics["ret20_median"] > baseline_distribution["ret20_median"]["median"],
        "win_rate_20_beats_baseline_2": signal_metrics.get("win_rate_20") is not None
        and baseline_distribution.get("win_rate_20", {}).get("median") is not None
        and signal_metrics["win_rate_20"] > baseline_distribution["win_rate_20"]["median"],
    }
    mae_ratio = _mae_ratio(signal_metrics.get("mae20_median"), baseline_distribution.get("mae20_median", {}).get("median"))
    gates = {
        "signal_count_ge_100": int(signal_metrics.get("count") or 0) >= 100,
        "largest_year_share_le_0_40": concentration.get("largest_year_share") is not None and concentration["largest_year_share"] <= 0.40,
        "largest_sector_share_le_0_40": concentration.get("largest_sector_share") is not None and concentration["largest_sector_share"] <= 0.40,
        "largest_liquidity_share_le_0_50": concentration.get("largest_liquidity_or_volume_bucket_share") is not None and concentration["largest_liquidity_or_volume_bucket_share"] <= 0.50,
        "mae_median_not_worse_than_1_2x_baseline_2": mae_ratio is not None and mae_ratio <= 1.2,
        "no_lookahead_valid": bool(no_lookahead_pass),
    }
    if int(signal_metrics.get("count") or 0) == 0 or not no_lookahead_pass:
        decision = "blocked" if not no_lookahead_pass else "drop"
        reason = "no_signal_rows_or_no_lookahead_failed"
    elif all(gates.values()) and all(beats.values()) and not research_fallback:
        decision = "keep"
        reason = "beats_stabilized_baseline_2_and_keep_gates_pass"
    elif all(beats.values()):
        decision = "hold"
        reason = "beats_baseline_2_but_not_keep_grade_due_to_fallback_or_gate"
    elif any(beats.values()):
        decision = "hold"
        reason = "partial_baseline_2_improvement"
    else:
        decision = "analysis_only"
        reason = "diagnostic_signal_but_baseline_2_not_beaten"
    return {
        "decision": decision,
        "decision_reason": reason,
        "primary_baseline_beats": beats,
        "keep_gates": gates,
        "mae_median_worse_ratio_vs_baseline_2": mae_ratio,
    }


def _breakdown_summary(frame: pd.DataFrame) -> dict[str, Any]:
    work = frame.copy()
    work["year"] = work["decision_date"].astype(str).str.slice(0, 4)
    return {
        "year": base._breakdown(work, "year"),
        "sector": base._breakdown(work, "sector"),
        "liquidity_or_volume_bucket": base._breakdown(work, "liquidity_bucket"),
        "monthly_regime": base._breakdown(work, "monthly_C_regime"),
        "concentration": _concentration(work),
    }


def run_phase1b(
    *,
    source_rows_parquet: Path,
    output_root: Path,
    random_seed: int = 20260509,
    baseline_repetitions: int = 100,
) -> dict[str, Any]:
    run_started = time.perf_counter()
    session_root = output_root / _session_id()
    session_root.mkdir(parents=True, exist_ok=True)
    source = pd.read_parquet(source_rows_parquet)
    normalized = base._normalize_source_frame(source)
    evaluated_frame = base._attach_forward_outcomes(base._attach_signal_flags(normalized))
    signal_rows = base._build_signal_rows(normalized)
    eligible_b2 = base._eligible_rows(evaluated_frame, "baseline_2")
    repetitions = max(1, int(baseline_repetitions))
    research_fallback = repetitions < 100

    full_baseline = _repeated_baseline_distribution(
        eligible_b2,
        signal_rows,
        random_seed=random_seed,
        repetitions=repetitions,
    )
    subtype_payload: dict[str, Any] = {}
    subtype_breakdowns: dict[str, Any] = {}
    strongest: dict[str, Any] | None = None
    weakest: dict[str, Any] | None = None
    for subtype in SUBTYPES:
        subset = signal_rows.loc[signal_rows["signal_subtype"].astype(str) == subtype].copy()
        baseline = _repeated_baseline_distribution(
            eligible_b2,
            subset,
            random_seed=random_seed + sum(ord(ch) for ch in subtype),
            repetitions=repetitions,
        )
        metrics = _metrics(subset)
        concentration = _concentration(subset)
        decision = _decision_for_metrics(
            signal_metrics=metrics,
            baseline_distribution=baseline["distribution"],
            concentration=concentration,
            no_lookahead_pass=True,
            research_fallback=research_fallback,
        )
        subtype_payload[subtype] = {
            "signal": metrics,
            "baseline_2": baseline,
            "baseline_2_deltas": _delta(metrics, baseline["distribution"]),
            "concentration": concentration,
            "decision": decision,
        }
        subtype_breakdowns[subtype] = _breakdown_summary(subset)
        rank_value = metrics.get("ret20_median")
        item = {"subtype": subtype, "ret20_median": rank_value, "decision": decision["decision"], "signal_count": metrics["count"]}
        if rank_value is not None and (strongest is None or rank_value > strongest["ret20_median"]):
            strongest = item
        if rank_value is not None and (weakest is None or rank_value < weakest["ret20_median"]):
            weakest = item

    full_metrics = _metrics(signal_rows)
    full_concentration = _concentration(signal_rows)
    no_lookahead = base._build_no_lookahead_audit(signal_rows, list(source.columns))
    full_decision = _decision_for_metrics(
        signal_metrics=full_metrics,
        baseline_distribution=full_baseline["distribution"],
        concentration=full_concentration,
        no_lookahead_pass=bool(no_lookahead["pass"]),
        research_fallback=research_fallback,
    )
    full_redecision = {
        "schema_version": REDECISION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": base.CANDIDATE_ID,
        "candidate_local_decision": full_decision["decision"],
        "session_aggregate_decision": full_decision["decision"],
        "authoritative_rollup_decision": full_decision["decision"],
        "decision_reason": full_decision["decision_reason"],
        "research_fallback": research_fallback,
        "baseline_repeat_count": repetitions,
        "signal": full_metrics,
        "baseline_2_deltas": _delta(full_metrics, full_baseline["distribution"]),
        "concentration": full_concentration,
        "primary_baseline_beats": full_decision["primary_baseline_beats"],
        "keep_gates": full_decision["keep_gates"],
        "mae_median_worse_ratio_vs_baseline_2": full_decision["mae_median_worse_ratio_vs_baseline_2"],
        "strongest_subtype": strongest,
        "weakest_subtype": weakest,
    }
    baseline_stability = {
        "schema_version": BASELINE_STABILITY_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": base.CANDIDATE_ID,
        "baseline_id": "baseline_2",
        "baseline_repeat_count": repetitions,
        "random_seed": int(random_seed),
        "research_fallback": research_fallback,
        "fallback_reason": None if not research_fallback else "100 baseline repetitions exceeded runtime in Phase 1; Phase 1b used explicit reduced repeat count",
        "full_signal_baseline_2": full_baseline,
    }
    subtype_compare = {
        "schema_version": SUBTYPE_COMPARE_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": base.CANDIDATE_ID,
        "baseline_id": "baseline_2",
        "baseline_repeat_count": repetitions,
        "research_fallback": research_fallback,
        "subtypes": subtype_payload,
    }
    breakdowns = {
        "schema_version": SUBTYPE_BREAKDOWNS_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "candidate_id": base.CANDID_ID if hasattr(base, "CANDID_ID") else base.CANDIDATE_ID,
        "full_signal": _breakdown_summary(signal_rows),
        "subtypes": subtype_breakdowns,
    }
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "script_name": SCRIPT_NAME,
        "session_root": str(session_root),
        "candidate_id": base.CANDIDATE_ID,
        "boundary": "TRADEX-only",
        "research_only": True,
        "signal_definition_changed": False,
        "thresholds_changed": False,
        "baseline_repeat_count": repetitions,
        "random_seed": int(random_seed),
        "research_fallback": research_fallback,
        "runtime_seconds": float(time.perf_counter() - run_started),
    }
    input_resolution = {
        "schema_version": INPUT_RESOLUTION_SCHEMA_VERSION,
        "generated_at_utc": _utc_now(),
        "source_rows_parquet": str(source_rows_parquet),
        "source_row_count": int(len(source)),
        "normalized_row_count": int(len(normalized)),
        "eligible_baseline_2_row_count": int(len(eligible_b2)),
        "raw_signal_row_count": int(signal_rows.attrs.get("raw_signal_row_count", len(signal_rows))),
        "signal_row_count": int(len(signal_rows)),
        "excluded_no_lookahead_invalid_count": int(signal_rows.attrs.get("excluded_no_lookahead_invalid_count", 0)),
    }

    _write_json(session_root / "run_manifest.json", manifest)
    _write_json(session_root / "input_resolution.json", input_resolution)
    _write_json(session_root / "phase1b_baseline_stability.json", baseline_stability)
    _write_json(session_root / "phase1b_subtype_expectancy_compare.json", subtype_compare)
    _write_json(session_root / "phase1b_subtype_breakdowns.json", breakdowns)
    _write_json(session_root / "phase1b_no_lookahead_audit.json", {**no_lookahead, "schema_version": NO_LOOKAHEAD_SCHEMA_VERSION})
    _write_json(session_root / "phase1b_signal_redecision.json", full_redecision)
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
        "decision": full_redecision["authoritative_rollup_decision"],
        "artifact_refs": {artifact: str(session_root / artifact) for artifact in REQUIRED_ARTIFACTS},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="TRADEX Iizuka signal expectancy Phase 1b subtype and baseline stability evaluation")
    parser.add_argument("--source-rows-parquet", required=True)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--random-seed", type=int, default=20260509)
    parser.add_argument("--baseline-repetitions", type=int, default=100)
    args = parser.parse_args()
    result = run_phase1b(
        source_rows_parquet=_safe_path(args.source_rows_parquet, Path(args.source_rows_parquet)),
        output_root=_safe_path(args.output_root, DEFAULT_OUTPUT_ROOT),
        random_seed=args.random_seed,
        baseline_repetitions=args.baseline_repetitions,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
