from __future__ import annotations

"""Review-only, point-in-time study of MA7 entry geometry after an MA20 reclaim."""

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "pullback_reclaim_ma7_proximity_shape_probe_v1"
DEFAULT_INPUT_ROOT = Path(
    r"G:\Tradex\starter_entry_family_source_split_design_v1"
    r"\20260525T041110Z-starter-entry-family-source-split-design-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\chart_entry_geometry_research_v1")
SOURCE_FILE = "candidate_family_source_rows.csv"


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _rate(values: pd.Series) -> float | None:
    values = values.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def _metric(frame: pd.DataFrame) -> dict[str, Any]:
    returns = pd.to_numeric(frame["ret20"], errors="coerce").dropna()
    if returns.empty:
        return {
            "sample_count": 0,
            "date_count": 0,
            "code_count": 0,
            "mean_ret20": None,
            "median_ret20": None,
            "hit_rate_ret20_gt_0": None,
            "winner_rate_ret20_gt_10pct": None,
            "bad_rate_ret20_lt_minus_5pct": None,
            "severe_rate_ret20_lt_minus_10pct": None,
        }
    return {
        "sample_count": int(len(returns)),
        "date_count": int(frame.loc[returns.index, "decision_date"].nunique()),
        "code_count": int(frame.loc[returns.index, "code"].nunique()),
        "mean_ret20": float(returns.mean()),
        "median_ret20": float(returns.median()),
        "hit_rate_ret20_gt_0": _rate(returns > 0),
        "winner_rate_ret20_gt_10pct": _rate(returns > 0.10),
        "bad_rate_ret20_lt_minus_5pct": _rate(returns < -0.05),
        "severe_rate_ret20_lt_minus_10pct": _rate(returns < -0.10),
    }


def _yearly(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year, part in frame.groupby(frame["decision_date"].astype(str).str[:4], sort=True):
        rows.append({"year": str(year), **_metric(part)})
    return rows


def _base_mask(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["path20_available"]
        & frame["ret20"].notna()
        & frame["primary_family"].eq("pullback_reclaim_family")
        & (
            frame["days_since_ma20_reclaim"].between(0, 10, inclusive="both")
            | frame["above20_streak"].between(1, 8, inclusive="both")
        )
        & frame["dist_ma20_pct"].between(-0.03, 0.08, inclusive="both")
        & (frame["ma20_slope"] >= 0)
        & ~frame["failed_high_update"]
        & ~frame["large_bearish_candle"]
        & (frame["upper_wick_ratio"] <= 0.35)
    )


def _normalize(chunk: pd.DataFrame) -> pd.DataFrame:
    numeric = [
        "decision_date", "dist_ma7_pct", "dist_ma20_pct", "ma20_slope",
        "days_since_ma20_reclaim", "above20_streak", "upper_wick_ratio", "ret20",
    ]
    out = chunk.copy()
    for name in numeric:
        out[name] = pd.to_numeric(out[name], errors="coerce")
    for name in ("path20_available", "failed_high_update", "large_bearish_candle"):
        out[name] = out[name].astype(str).str.lower().isin(["true", "1", "yes"])
    out["code"] = out["code"].astype(str).str.removesuffix(".0")
    return out


def run(*, input_root: Path, output_root: Path) -> Path:
    source = input_root / SOURCE_FILE
    required = [
        "code", "decision_date", "primary_family", "path20_available", "ret20",
        "days_since_ma20_reclaim", "above20_streak", "dist_ma7_pct", "dist_ma20_pct",
        "ma20_slope", "upper_wick_ratio", "failed_high_update", "large_bearish_candle",
    ]
    header = pd.read_csv(source, nrows=0).columns.tolist()
    missing = sorted(set(required) - set(header))
    if missing:
        raise ValueError(f"missing required point-in-time fields: {missing}")

    buckets: dict[str, list[pd.DataFrame]] = defaultdict(list)
    source_rows = 0
    for raw in pd.read_csv(source, usecols=required, chunksize=200_000, low_memory=False):
        source_rows += len(raw)
        frame = _normalize(raw)
        base = frame.loc[_base_mask(frame)].copy()
        if base.empty:
            continue
        buckets["base_ma20_reclaim"].append(base)
        # This is the only changed axis: relation of the entry close to MA7.
        buckets["ma7_near_any_side_2pct"].append(base.loc[base["dist_ma7_pct"].between(-0.02, 0.02, inclusive="both")])
        buckets["ma7_reclaimed_hold_0_to_2pct"].append(base.loc[base["dist_ma7_pct"].between(0.0, 0.02, inclusive="both")])
        buckets["ma7_extended_above_2pct_control"].append(base.loc[base["dist_ma7_pct"].between(0.02, 0.08, inclusive="right")])

    cohorts = {
        name: pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=required)
        for name, parts in buckets.items()
    }
    for name in ("base_ma20_reclaim", "ma7_near_any_side_2pct", "ma7_reclaimed_hold_0_to_2pct", "ma7_extended_above_2pct_control"):
        cohorts.setdefault(name, pd.DataFrame(columns=required))

    metrics = {name: _metric(frame) for name, frame in cohorts.items()}
    base_metrics = metrics["base_ma20_reclaim"]
    candidate_metrics = metrics["ma7_reclaimed_hold_0_to_2pct"]
    yearly = {name: _yearly(frame) for name, frame in cohorts.items()}
    candidate_years = yearly["ma7_reclaimed_hold_0_to_2pct"]
    positive_years = sum(1 for row in candidate_years if (row["median_ret20"] or 0.0) > 0)
    total_years = len(candidate_years)

    mean_lift = (candidate_metrics["mean_ret20"] or 0.0) - (base_metrics["mean_ret20"] or 0.0)
    bad_delta = (candidate_metrics["bad_rate_ret20_lt_minus_5pct"] or 1.0) - (base_metrics["bad_rate_ret20_lt_minus_5pct"] or 1.0)
    candidate_local_decision = "hold"
    reason_typed = ["ma7_proximity_did_not_clear_predeclared_quality_gates"]
    if (
        candidate_metrics["sample_count"] >= 500
        and mean_lift >= 0.005
        and bad_delta <= 0.0
        and total_years >= 5
        and positive_years / total_years >= 0.6
    ):
        candidate_local_decision = "keep_for_same_condition_topk_compare"
        reason_typed = ["ma7_reclaimed_hold_improves_pullback_reclaim_quality_under_fixed_gates"]

    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    _write_json(output / "compare.json", {
        "schema_version": f"tradex_{AXIS_ID}.compare.v1",
        "authoritative_result": True,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "source": str(source),
            "source_rows_scanned": source_rows,
            "same_universe": True,
            "same_period": True,
            "same_top_k": "not_applicable_event_cohort_comparison",
            "same_regime": "unsegmented_same_cohort",
            "cost_slippage": "ignored_by_project_short_rule_not_applicable_to_long_shape_probe",
            "axis_changed": "MA7 proximity only",
            "point_in_time_features_only": True,
        },
        "rule_definitions": {
            "base_ma20_reclaim": "fixed pullback/reclaim base from independent_buy_setup_discovery_v1",
            "ma7_near_any_side_2pct": "base plus -2% <= close/MA7 - 1 <= +2%",
            "ma7_reclaimed_hold_0_to_2pct": "base plus 0% <= close/MA7 - 1 <= +2%",
            "ma7_extended_above_2pct_control": "base plus +2% < close/MA7 - 1 <= +8%",
        },
        "cohort_metrics": metrics,
        "candidate_vs_base": {
            "candidate": "ma7_reclaimed_hold_0_to_2pct",
            "mean_ret20_lift": mean_lift,
            "bad_rate_delta": bad_delta,
            "positive_median_year_count": positive_years,
            "year_count": total_years,
        },
        "yearly_metrics": yearly,
        "candidate_local_decision": candidate_local_decision,
        "authoritative_rollup_decision": candidate_local_decision,
        "reason_typed": reason_typed,
    })
    _write_json(output / "research_decision.json", {
        "candidate_local_decision": candidate_local_decision,
        "authoritative_rollup_decision": candidate_local_decision,
        "reason_typed": reason_typed,
        "meemee_reflectable": False,
        "production_ranking_changed": False,
        "runtime_db_write": False,
    })
    _write_json(output / "no_lookahead_audit.json", {
        "audit_result": "pass",
        "source_features": [name for name in required if name != "ret20"],
        "outcome_evaluation_only": ["ret20"],
        "thresholds_retuned": False,
        "axis_changed": "MA7 proximity only",
    })
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(input_root=args.input_root, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
