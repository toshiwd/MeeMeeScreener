"""Select and validate a practical early short-entry fusion without recent-period tuning."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd


RET_THRESHOLDS = (-0.03, -0.04, -0.05, -0.06)
RANGE_THRESHOLDS = (0.15, 0.20, 0.25, 0.30, 0.35, 0.40)
DEVELOPMENT_END_YEAR = 2023
VALIDATION_START_YEAR = 2024
MIN_DEVELOPMENT_N = 80


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def add_entry_features(events: pd.DataFrame, db: Path) -> pd.DataFrame:
    sql = """
    WITH bars AS (
      SELECT
        code::VARCHAR AS code,
        CASE
          WHEN date > 1000000000
            THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
          ELSE CAST(date AS INTEGER)
        END AS ymd,
        CAST(c AS DOUBLE) AS signal_close_db,
        lag(CAST(c AS DOUBLE)) OVER (PARTITION BY code ORDER BY date) AS previous_close
      FROM daily_bars
      WHERE lower(coalesce(source, '')) = 'pan'
    )
    SELECT * FROM bars
    """
    bars = duckdb.connect(str(db), read_only=True).execute(sql).df()
    out = events.merge(
        bars,
        left_on=["code", "signal_ymd"],
        right_on=["code", "ymd"],
        how="left",
        validate="many_to_one",
    )
    out["signal_ret1"] = out.signal_close_db / out.previous_close - 1.0
    out["entry_gap_pct"] = out.entry_open_recomputed / out.signal_close - 1.0
    out["year"] = out.signal_ymd // 10000
    return out


def metrics(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"n": 0}
    years = {}
    for year, rows in frame.groupby("year"):
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
        "fast_clean_target_3pct_hit_rate": float(frame.fast_clean_target_3pct_hit.mean()),
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
    ap.add_argument("--old-events", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    data = add_entry_features(pd.read_parquet(args.events), args.db)
    old = pd.read_parquet(args.old_events)
    development = data.loc[data.year.le(DEVELOPMENT_END_YEAR)]
    validation = data.loc[data.year.ge(VALIDATION_START_YEAR)]
    dev_baseline = metrics(development)
    grid = {}
    eligible = []
    for ret_threshold in RET_THRESHOLDS:
        for range_threshold in RANGE_THRESHOLDS:
            name = f"ret_le_{abs(int(ret_threshold * 100))}_range20_ge_{int(range_threshold * 100)}"
            dev_rows = development.loc[
                development.signal_ret1.le(ret_threshold)
                & development.range_20_0.ge(range_threshold)
            ]
            row = metrics(dev_rows)
            grid[name] = {
                "signal_ret1_max": ret_threshold,
                "range20_min": range_threshold,
                "development": row,
            }
            if (
                row["n"] >= MIN_DEVELOPMENT_N
                and row["target_3pct_hit_rate"] >= dev_baseline["target_3pct_hit_rate"]
                and row["positive_years_fast_clean_gt_40"] >= 3
            ):
                eligible.append((name, row))
    selected_name, _ = max(
        eligible,
        key=lambda item: (
            item[1]["fast_clean_target_3pct_hit_rate"],
            item[1]["target_3pct_hit_rate"],
            item[1]["n"],
        ),
        default=(None, None),
    )
    if selected_name is None:
        raise RuntimeError("no development candidate passed the predeclared gates")
    selected_definition = grid[selected_name]
    ret_threshold = float(selected_definition["signal_ret1_max"])
    range_threshold = float(selected_definition["range20_min"])
    selected_mask = (
        data.signal_ret1.le(ret_threshold)
        & data.range_20_0.ge(range_threshold)
    )
    selected = data.loc[selected_mask].copy()
    selected.to_parquet(args.output / "selected_entry_fusion_ledger.parquet", index=False)
    selected_dev = selected.loc[selected.year.le(DEVELOPMENT_END_YEAR)]
    selected_validation = selected.loc[selected.year.ge(VALIDATION_START_YEAR)]
    gap_overlay = selected.loc[selected.entry_gap_pct.ge(0.01) & selected.entry_gap_pct.lt(0.03)]

    old_metrics = metrics(
        old.assign(
            year=old.signal_ymd // 10000,
            target_3pct_hit=old.target_3pct_hit.astype(bool),
            clean_target_3pct_hit=old.clean_target_3pct_hit.astype(bool),
            fast_clean_target_3pct_hit=old.fast_clean_target_3pct_hit.astype(bool),
        )
    )
    full_metrics = metrics(selected)
    dev_metrics = metrics(selected_dev)
    validation_metrics = metrics(selected_validation)
    overlay_metrics = metrics(gap_overlay)
    validation_years = validation_metrics["years"]
    checks = {
        "development_selected_without_2024plus": selected_name is not None,
        "full_n_ge_100": full_metrics["n"] >= 100,
        "full_fast_clean_gt_0_55": full_metrics["fast_clean_target_3pct_hit_rate"] > 0.55,
        "full_target3_gt_0_70": full_metrics["target_3pct_hit_rate"] > 0.70,
        "validation_n_ge_50": validation_metrics["n"] >= 50,
        "validation_fast_clean_gt_0_55": validation_metrics["fast_clean_target_3pct_hit_rate"] > 0.55,
        "all_validation_years_fast_clean_ge_0_50": all(
            row["fast_clean_target_3pct_hit_rate"] >= 0.50
            for row in validation_years.values()
        ),
        "better_than_old_fast_clean": (
            full_metrics["fast_clean_target_3pct_hit_rate"]
            > old_metrics["fast_clean_target_3pct_hit_rate"]
        ),
    }
    keep = all(checks.values())
    result = {
        "schema_version": "tradex_short_initial_entry_fusion_rollup_v1.compare.v1",
        "artifact_role": "authoritative_initial_entry_fusion_rollup",
        "review_only": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "parent_population": (
                "episode Early age0-3; close>=MA20; MA20 slope5<=0; "
                "900<=close<5000"
            ),
            "development_period": "2020-2023",
            "validation_period": "2024-2026",
            "minimum_development_samples": MIN_DEVELOPMENT_N,
            "selection_axes": ["signal_ret1", "range_20_0"],
            "entry": "next session open",
            "primary_win": "3% decline within 5 sessions regardless of prior rebound",
            "timing_quality": "3% decline within 3 sessions before 3% adverse move",
            "costs": "ignored",
            "future_selection_columns": [],
        },
        "authoritative_result": {
            "development_baseline": dev_baseline,
            "development_grid": grid,
            "selected_name": selected_name,
            "selected_definition": {
                "signal_ret1_max": ret_threshold,
                "range20_min": range_threshold,
            },
            "selected_development": dev_metrics,
            "selected_validation": validation_metrics,
            "selected_full": full_metrics,
            "gap_1_to_3pct_precision_overlay_full": overlay_metrics,
            "old_selector_full": old_metrics,
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": int(len(data) - len(selected)),
            "selection_divergence_reason": (
                "development-only selection combines initial drop severity "
                "with recent 20-session range"
            ),
            "parent_members": int(len(data)),
            "selected_members": int(len(selected)),
        },
        "judgment": {
            "candidate_local_decision": "keep" if keep else "hold",
            "session_aggregate_decision": (
                "keep_initial_entry_fusion" if keep else "hold_initial_entry_fusion"
            ),
            "authoritative_rollup_decision": (
                "keep_short_initial_entry_fusion_v1_review_only"
                if keep else "hold_continue_entry_fusion_research"
            ),
            "reason_type": (
                "development_selected_candidate_passed_recent_validation_gates"
                if keep else "one_or_more_recent_validation_gates_failed"
            ),
        },
        "not_changed": ["MeeMee", "ranking", "runtime DB", "production logic"],
        "remaining_risks": [
            "2020-2023 development samples remain regime-dependent",
            "gap overlay is diagnostic and was not development-selected",
            "discretionary exits remain unsimulated",
            "current anchors lack complete future windows",
        ],
    }
    compare = args.output / "compare.json"
    compare.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "db": {"path": str(args.db.resolve()), "read_only": True},
            "events": {"path": str(args.events.resolve()), "sha256": sha(args.events)},
            "old_events": {"path": str(args.old_events.resolve()), "sha256": sha(args.old_events)},
        },
        "compare_sha256": sha(compare),
        "ledger_sha256": sha(args.output / "selected_entry_fusion_ledger.parquet"),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
