"""Test old typical-pattern membership inside the kept early short population."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_short_exit_profit_take_v1 import _load_daily
from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import (
    TYPICAL_PATTERNS,
    _add_shape_features,
)
from scripts.tradex_pre_crash_shape_pattern_discovery_v1 import _classify_shape


FEATURES = [
    "ret_80_40", "ret_40_20", "ret_20_0", "ret_60_0",
    "range_40_20", "range_20_0", "dist_prior_80_high",
    "dist_prior_80_low", "late_high_break", "last_vol_ratio",
    "red_cluster_10", "weak_close_cluster_10",
]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def add_old_pattern(events: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for code, group in daily.groupby("code", sort=False):
        shaped = _add_shape_features(group)
        shaped["old_pattern"] = [
            _classify_shape({
                key: None if pd.isna(row.get(key)) else float(row.get(key))
                for key in FEATURES
            })
            for _, row in shaped.iterrows()
        ]
        shaped["old_typical_pattern"] = shaped.old_pattern.isin(TYPICAL_PATTERNS)
        parts.append(shaped[["code", "ymd", "old_pattern", "old_typical_pattern", *FEATURES]])
    context = pd.concat(parts, ignore_index=True)
    merged = events.merge(
        context,
        left_on=["code", "signal_ymd"],
        right_on=["code", "ymd"],
        how="left",
        validate="many_to_one",
    )
    return merged


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0}
    years = {}
    for year, rows in frame.assign(year=frame.signal_ymd // 10000).groupby("year"):
        years[str(int(year))] = {
            "n": int(len(rows)),
            "target_3pct_hit_rate": float(rows.target_3pct_hit.mean()),
            "fast_clean_target_3pct_hit_rate": float(rows.fast_clean_target_3pct_hit.mean()),
        }
    return {
        "n": int(len(frame)),
        "codes": int(frame.code.astype(str).nunique()),
        "target_3pct_hit_rate": float(frame.target_3pct_hit.mean()),
        "clean_target_3pct_hit_rate": float(frame.clean_target_3pct_hit.mean()),
        "fast_target_3pct_hit_rate": float(frame.fast_target_3pct_hit.mean()),
        "fast_clean_target_3pct_hit_rate": float(frame.fast_clean_target_3pct_hit.mean()),
        "median_adverse_before_hit_pct": float(frame.max_adverse_before_3pct_hit_pct.median()),
        "median_max_high_5d_pct": float(frame.max_high_5d_pct.median()),
        "median_min_low_5d_pct": float(frame.min_low_5d_pct.median()),
        "positive_years_fast_clean_gt_40": int(
            sum(row["fast_clean_target_3pct_hit_rate"] > 0.40 for row in years.values())
        ),
        "year_count": int(len(years)),
        "years": years,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(args.events)
    fused = add_old_pattern(events, _load_daily(args.db, None))
    fused.to_parquet(args.output / "early_old_pattern_fusion_ledger.parquet", index=False)
    baseline = metrics(fused)
    typical = fused.loc[fused.old_typical_pattern.fillna(False)].copy()
    non_typical = fused.loc[~fused.old_typical_pattern.fillna(False)].copy()
    typical_metrics = metrics(typical)
    non_typical_metrics = metrics(non_typical)
    pattern_metrics = {
        str(pattern): metrics(rows)
        for pattern, rows in fused.groupby("old_pattern")
    }
    checks = {
        "typical_n_ge_100": typical_metrics["n"] >= 100,
        "typical_fast_clean_gt_baseline": (
            typical_metrics.get("fast_clean_target_3pct_hit_rate", 0)
            > baseline["fast_clean_target_3pct_hit_rate"]
        ),
        "typical_target3_not_worse_than_baseline": (
            typical_metrics.get("target_3pct_hit_rate", 0)
            >= baseline["target_3pct_hit_rate"]
        ),
        "typical_positive_years_ge_5": (
            typical_metrics.get("positive_years_fast_clean_gt_40", 0) >= 5
        ),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_early_old_pattern_fusion_v1.compare.v1",
        "artifact_role": "authoritative_early_old_pattern_fusion",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": (
                "episode Early age0-3; close>=MA20; MA20 slope5<=0; "
                "900<=close<5000"
            ),
            "axis_changed": "old typical pre-crash pattern membership only",
            "entry": "next session open",
            "primary_win": "3% decline within 5 sessions regardless of prior rebound",
            "timing_quality": "3% decline within 3 sessions before 3% adverse move",
            "period": "2019-2026",
            "costs": "ignored",
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "baseline_early": baseline,
            "old_typical_pattern_fusion": typical_metrics,
            "non_typical": non_typical_metrics,
            "pattern_metrics": pattern_metrics,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(fused) - len(typical)),
            "selection_divergence_reason": (
                "keeps only early candidates whose same-day 80-session shape "
                "belongs to the old selector typical-pattern family"
            ),
            "baseline_members": int(len(fused)),
            "fused_members": int(len(typical)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "drop",
            "session_aggregate_decision": (
                "keep_old_pattern_fusion" if keep else "drop_old_pattern_fusion"
            ),
            "authoritative_rollup_decision": (
                "keep_early_old_pattern_fusion_v1_review_only"
                if keep else "drop_old_pattern_membership_axis"
            ),
            "reason_type": (
                "same_condition_timing_quality_and_breadth_gates_passed"
                if keep else "old_pattern_membership_did_not_pass_fixed_gates"
            ),
        },
        "not_changed": [
            "old distance gate", "old range gates", "entry execution",
            "MeeMee", "ranking", "runtime DB", "production logic",
        ],
        "remaining_risks": [
            "pattern taxonomy may be too broad",
            "small individual pattern samples",
            "discretionary exits remain unsimulated",
            "event history remains insufficient",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "db": {"path": str(args.db.resolve()), "read_only": True},
            "events": {"path": str(args.events.resolve()), "sha256": sha(args.events)},
        },
        "compare_sha256": sha(compare),
        "ledger_sha256": sha(args.output / "early_old_pattern_fusion_ledger.parquet"),
    }
    (args.output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps(
            {"complete": True, "authoritative": "compare.json", "sha256": sha(compare)},
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
