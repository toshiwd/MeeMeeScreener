from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "pullback_retest_confirmation_source_pretest_v1"
DEFAULT_ENRICHED_ROOT = Path(
    r"G:\Tradex\pullback_retest_sequence_invalidation_enrichment_v1"
    r"\20260602T103306Z-pullback_retest_sequence_invalidation_enrichment_v1"
)
DEFAULT_SOURCE_PARQUET = Path(
    r"G:\Tradex\pattern_family_source_rows_v1"
    r"\20260525T101220Z-pattern-family-source-rows-v1"
    r"\pattern_family_source_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\pullback_retest_confirmation_source_pretest_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _metrics(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "row_count": len(frame),
        "ret20_covered_count": int(frame["ret20"].notna().sum()),
        "ret20_mean": float(frame["ret20"].mean()),
        "winner_ret20_gt_10pct_rate": float(frame["winner_ret20_gt_10pct"].mean()),
        "bad_ret20_lt_minus_5pct_rate": float(frame["bad_ret20_lt_minus_5pct"].mean()),
        "severe_ret20_lt_minus_10pct_rate": float(frame["severe_ret20_lt_minus_10pct"].mean()),
    }


def run_pretest(*, enriched_root: Path, source_parquet: Path, output_root: Path) -> Path:
    events = pd.read_parquet(enriched_root / "pullback_retest_sequence_events_enriched.parquet")
    events = events.loc[events["fixed_condition_pretest_eligible"]].copy()
    events["confirmation_as_of"] = events["confirmation_as_of"].astype(int)
    source = pd.read_parquet(
        source_parquet,
        columns=[
            "as_of_date",
            "code",
            "ret20",
            "winner_ret20_gt_10pct",
            "bad_ret20_lt_minus_5pct",
            "severe_ret20_lt_minus_10pct",
        ],
    )
    source["as_of_date"] = source["as_of_date"].astype(int)
    source["code"] = source["code"].astype(str)
    events["code"] = events["code"].astype(str)
    outcomes = source.rename(columns={"as_of_date": "confirmation_as_of"})
    event_outcomes = events.merge(outcomes, on=["confirmation_as_of", "code"], how="left", validate="one_to_one")
    baseline = outcomes.loc[outcomes["confirmation_as_of"].isin(event_outcomes["confirmation_as_of"].unique())].copy()
    event_metrics = _metrics(event_outcomes)
    baseline_metrics = _metrics(baseline)
    event_metrics["ret20_mean_delta_vs_same_day_baseline"] = event_metrics["ret20_mean"] - baseline_metrics["ret20_mean"]
    event_metrics["winner_rate_delta_vs_same_day_baseline"] = (
        event_metrics["winner_ret20_gt_10pct_rate"] - baseline_metrics["winner_ret20_gt_10pct_rate"]
    )
    event_metrics["bad_rate_delta_vs_same_day_baseline"] = (
        event_metrics["bad_ret20_lt_minus_5pct_rate"] - baseline_metrics["bad_ret20_lt_minus_5pct_rate"]
    )
    event_outcomes["year"] = event_outcomes["confirmation_as_of"].astype(str).str[:4].astype(int)
    yearly = []
    for year, event_year in event_outcomes.groupby("year", sort=True):
        baseline_year = baseline.loc[baseline["confirmation_as_of"].astype(str).str[:4].astype(int) == year]
        event_year_metrics = _metrics(event_year)
        baseline_year_metrics = _metrics(baseline_year)
        yearly.append(
            {
                "year": int(year),
                "event": event_year_metrics,
                "same_day_baseline": baseline_year_metrics,
                "ret20_mean_delta": event_year_metrics["ret20_mean"] - baseline_year_metrics["ret20_mean"],
            }
        )
    covered = event_metrics["ret20_covered_count"] == event_metrics["row_count"]
    positive_year_count = sum(row["ret20_mean_delta"] > 0 for row in yearly)
    stable_positive = bool(
        covered
        and event_metrics["row_count"] >= 500
        and event_metrics["ret20_mean_delta_vs_same_day_baseline"] > 0
        and event_metrics["winner_rate_delta_vs_same_day_baseline"] > 0
        and event_metrics["bad_rate_delta_vs_same_day_baseline"] <= 0
        and positive_year_count >= max(1, int(len(yearly) * 0.6))
    )
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    event_outcomes.to_parquet(output_dir / "pullback_retest_confirmation_event_outcomes.parquet", index=False)
    pd.DataFrame(yearly).to_json(output_dir / "yearly_metrics.json", orient="records", indent=2)
    compare = {
        "axis_id": AXIS_ID,
        "evaluation_condition": "event outcomes vs all source rows on identical confirmation dates",
        "event": event_metrics,
        "same_day_baseline": baseline_metrics,
        "yearly": yearly,
        "positive_year_count": positive_year_count,
        "year_count": len(yearly),
        "changed_top5_members_count": None,
        "changed_top10_members_count": None,
        "changed_rank_count": None,
        "selection_divergence_reason": "sequence_confirmation_source_vs_same_day_source_rows",
    }
    decision = {
        "axis_id": AXIS_ID,
        "decision_class": "KEEP" if stable_positive else "DROP",
        "research_decision": (
            "keep_pullback_retest_confirmation_source_for_next_research_comparison"
            if stable_positive
            else "drop_pullback_retest_confirmation_source_no_stable_same_day_edge"
        ),
        "candidate_generation_changed": False,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "validated_buy_count": 0,
    }
    _write_json(output_dir / "compare.json", compare)
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", decision)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enriched-root", type=Path, default=DEFAULT_ENRICHED_ROOT)
    parser.add_argument("--source-parquet", type=Path, default=DEFAULT_SOURCE_PARQUET)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run_pretest(enriched_root=args.enriched_root, source_parquet=args.source_parquet, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
