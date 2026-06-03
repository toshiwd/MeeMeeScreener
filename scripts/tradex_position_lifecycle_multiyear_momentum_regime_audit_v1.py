from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "position_lifecycle_multiyear_momentum_regime_audit_v1"
DEFAULT_SOURCE = Path(
    r"G:\Tradex\pattern_family_source_rows_v1"
    r"\20260525T101220Z-pattern-family-source-rows-v1\pattern_family_source_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\position_lifecycle_multiyear_momentum_regime_audit_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _regime(row: pd.Series) -> str:
    if row["breadth_above_ma20"] >= 0.62 and row["breadth_above_ma60"] >= 0.58 and row["trend_participation"] >= 0.08:
        return "broad_momentum"
    if row["breadth_above_ma20"] <= 0.40 or row["breadth_above_ma60"] <= 0.42:
        return "risk_off_or_low_breadth"
    if row["overextension_share"] >= 0.08:
        return "overextended_momentum"
    return "mixed_range"


def _entry_state(row: pd.Series) -> str:
    if bool(row["early_trend_reclaim_controlled_extension_candidate"]) or bool(row["monthly_weekly_supportive_daily_confirmation_candidate"]):
        return "Starter"
    if bool(row["volatility_compression_breakout_preparation_candidate"]) or bool(row["constructive_pullback_support_bullish_confirmation_reference_match"]):
        return "Accumulate"
    return "Wait"


def _held_state(row: pd.Series) -> str:
    if bool(row["failed_high_flag"]) or row["close_vs_ma20_pct"] < -0.06:
        return "ExitReview"
    if not bool(row["weekly_supportive_flag"]) or row["close_vs_ma20_pct"] < -0.02:
        return "HoldCaution"
    return "Hold"


def _cohort(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"row_count": 0}
    return {
        "row_count": len(frame),
        "ret5_mean": float(frame["ret5"].mean()),
        "ret20_mean": float(frame["ret20"].mean()),
        "ret20_median": float(frame["ret20"].median()),
        "positive_ret20_rate": float((frame["ret20"] > 0).mean()),
        "winner_ret20_rate": float(frame["winner_ret20_gt_10pct"].mean()),
        "bad_ret20_rate": float(frame["bad_ret20_lt_minus_5pct"].mean()),
        "severe_ret20_rate": float(frame["severe_ret20_lt_minus_10pct"].mean()),
    }


def run(*, source_path: Path, output_root: Path) -> Path:
    rows = pd.read_parquet(source_path)
    rows["as_of_date"] = rows["as_of_date"].astype(int)
    daily = rows.groupby("as_of_date", sort=True).agg(
        breadth_above_ma20=("close_above_ma20", "mean"),
        breadth_above_ma60=("close_above_ma60", "mean"),
        trend_participation=("monthly_weekly_supportive_daily_confirmation_candidate", "mean"),
        overextension_share=("close_vs_ma20_pct", lambda s: float((s > 0.12).mean())),
    ).reset_index()
    daily["market_momentum_regime"] = daily.apply(_regime, axis=1)
    rows = rows.merge(daily, on="as_of_date", how="left", validate="many_to_one")
    rows["entry_state"] = rows.apply(_entry_state, axis=1)
    rows["held_position_review_state"] = rows.apply(_held_state, axis=1)
    rows["year"] = rows["as_of_date"].astype(str).str[:4].astype(int)
    yearly = []
    for year, part in rows.groupby("year", sort=True):
        yearly.append({
            "year": int(year),
            "row_count": len(part),
            "date_count": int(part["as_of_date"].nunique()),
            "regime_date_distribution": daily.loc[daily["as_of_date"].astype(str).str[:4].astype(int) == year, "market_momentum_regime"].value_counts().sort_index().to_dict(),
            "by_entry_state": {state: _cohort(group) for state, group in part.groupby("entry_state", sort=True)},
            "by_held_position_review_state": {state: _cohort(group) for state, group in part.groupby("held_position_review_state", sort=True)},
        })
    by_regime = {}
    for regime, part in rows.groupby("market_momentum_regime", sort=True):
        by_regime[regime] = {
            "row_count": len(part),
            "date_count": int(part["as_of_date"].nunique()),
            "by_entry_state": {state: _cohort(group) for state, group in part.groupby("entry_state", sort=True)},
            "by_held_position_review_state": {state: _cohort(group) for state, group in part.groupby("held_position_review_state", sort=True)},
        }
    latest_year = int(rows["year"].max())
    prior = rows.loc[rows["year"] < latest_year]
    current = rows.loc[rows["year"] == latest_year]
    starter_prior = _cohort(prior.loc[prior["entry_state"] == "Starter"])
    starter_current = _cohort(current.loc[current["entry_state"] == "Starter"])
    summary = {
        "axis_id": AXIS_ID,
        "contract": {"state_thresholds_fixed": True, "market_regime_point_in_time_only": True, "offline_outcomes_evaluation_only": True, "runtime_db_write": False},
        "period": {"start_as_of": int(rows["as_of_date"].min()), "end_as_of": int(rows["as_of_date"].max()), "year_count": int(rows["year"].nunique())},
        "daily_regime_distribution": daily["market_momentum_regime"].value_counts().sort_index().to_dict(),
        "yearly": yearly,
        "by_market_momentum_regime": by_regime,
        "latest_year_shift": {
            "latest_year": latest_year,
            "starter_latest_year": starter_current,
            "starter_prior_years": starter_prior,
            "starter_ret20_mean_delta_latest_vs_prior": starter_current.get("ret20_mean", 0) - starter_prior.get("ret20_mean", 0),
            "starter_positive_rate_delta_latest_vs_prior": starter_current.get("positive_ret20_rate", 0) - starter_prior.get("positive_ret20_rate", 0),
        },
    }
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    rows.to_parquet(output / "position_lifecycle_multiyear_regime_rows.parquet", index=False)
    daily.to_csv(output / "market_momentum_regime_daily.csv", index=False)
    _write_json(output / "position_lifecycle_multiyear_regime_audit.json", summary)
    _write_json(output / "research_decision.json", {"decision_class": "READY_REVIEW_ONLY", "research_decision": "multiyear_momentum_regime_audit_ready_for_manual_policy_review", "state_thresholds_changed": False, "automatic_trade_action": False, "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "validated_buy_count": 0})
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(source_path=args.source_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
