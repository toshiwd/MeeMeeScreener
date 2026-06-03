from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


AXIS_ID = "position_lifecycle_state_machine_v1"
DEFAULT_SURFACE = Path(
    r"G:\Tradex\practical_decision_support_bundle_v1"
    r"\20260602T105354Z-practical_decision_support_bundle_v1\decision_support_surface.parquet"
)
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\position_lifecycle_state_machine_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _entry_state(row: pd.Series) -> str:
    if row["avoid_level"] == "avoid":
        return "Avoid"
    if row["review_bucket"] == "Starter":
        return "Starter"
    if bool(row["volatility_compression_breakout_preparation_candidate"]) or bool(row["constructive_pullback_support_bullish_confirmation_reference_match"]):
        return "Accumulate"
    return "Wait"


def _held_state(row: pd.Series) -> str:
    if bool(row["failed_high_flag"]) or row["close_vs_ma20_pct"] < -0.06 or row["downside_risk_probability_20d"] >= 0.68:
        return "ExitReview"
    if row["avoid_level"] == "avoid" or row["downside_risk_probability_20d"] >= 0.55 or not bool(row["weekly_supportive_flag"]):
        return "HoldCaution"
    return "Hold"


def _reasons(row: pd.Series) -> list[str]:
    reasons = []
    if bool(row["volatility_compression_breakout_preparation_candidate"]):
        reasons.append("compression_breakout_preparation")
    if bool(row["constructive_pullback_support_bullish_confirmation_reference_match"]):
        reasons.append("constructive_pullback_confirmation")
    if bool(row["early_trend_reclaim_controlled_extension_candidate"]):
        reasons.append("early_trend_reclaim")
    if bool(row["monthly_weekly_supportive_daily_confirmation_candidate"]):
        reasons.append("higher_timeframe_daily_confirmation")
    if bool(row["failed_high_flag"]):
        reasons.append("failed_high")
    if row["close_vs_ma20_pct"] < -0.06:
        reasons.append("ma20_breakdown")
    if row["downside_risk_probability_20d"] >= 0.68:
        reasons.append("high_downside_probability")
    if not bool(row["weekly_supportive_flag"]):
        reasons.append("weekly_not_supportive")
    return reasons


def _bars(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(
            """
            SELECT CAST(code AS VARCHAR) AS code,
                   CASE
                     WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                     WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                     ELSE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                   END AS as_of_date,
                   CAST(h AS DOUBLE) AS high,
                   CAST(l AS DOUBLE) AS low,
                   CAST(c AS DOUBLE) AS close
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
            ORDER BY code, as_of_date
            """
        ).fetchdf()


def _attach_path(surface: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.sort_values(["code", "as_of_date"]).copy()
    group = work.groupby("code", sort=False)
    for horizon in (5, 20):
        future_highs = [group["high"].shift(-step) for step in range(1, horizon + 1)]
        future_lows = [group["low"].shift(-step) for step in range(1, horizon + 1)]
        work[f"mfe{horizon}"] = pd.concat(future_highs, axis=1).max(axis=1) / work["close"] - 1.0
        work[f"mae{horizon}"] = pd.concat(future_lows, axis=1).min(axis=1) / work["close"] - 1.0
    return surface.merge(work[["code", "as_of_date", "mfe5", "mae5", "mfe20", "mae20"]], on=["code", "as_of_date"], how="left", validate="one_to_one")


def _cohort(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"row_count": 0}
    return {
        "row_count": len(frame),
        "ret5_mean": float(frame["ret5"].mean()),
        "ret20_mean": float(frame["ret20"].mean()),
        "mfe5_mean": float(frame["mfe5"].mean()),
        "mae5_mean": float(frame["mae5"].mean()),
        "mfe20_mean": float(frame["mfe20"].mean()),
        "mae20_mean": float(frame["mae20"].mean()),
        "bad_ret20_rate": float(frame["bad_ret20_lt_minus_5pct"].mean()),
        "severe_ret20_rate": float(frame["severe_ret20_lt_minus_10pct"].mean()),
        "winner_ret20_rate": float(frame["winner_ret20_gt_10pct"].mean()),
    }


def run(*, surface_path: Path, db_path: Path, output_root: Path) -> Path:
    surface = pd.read_parquet(surface_path)
    surface["entry_state"] = surface.apply(_entry_state, axis=1)
    surface["held_position_review_state"] = surface.apply(_held_state, axis=1)
    surface["lifecycle_reason_codes"] = surface.apply(_reasons, axis=1).map(json.dumps)
    surface = surface.sort_values(["code", "as_of_date"]).copy()
    surface["previous_entry_state"] = surface.groupby("code")["entry_state"].shift(1)
    surface["previous_held_position_review_state"] = surface.groupby("code")["held_position_review_state"].shift(1)
    surface["entry_transition"] = surface["previous_entry_state"].fillna("INITIAL") + "->" + surface["entry_state"]
    surface["held_transition"] = surface["previous_held_position_review_state"].fillna("INITIAL") + "->" + surface["held_position_review_state"]
    surface = _attach_path(surface, _bars(db_path))
    by_entry = {state: _cohort(part) for state, part in surface.groupby("entry_state", sort=True)}
    by_held = {state: _cohort(part) for state, part in surface.groupby("held_position_review_state", sort=True)}
    by_transition = {state: _cohort(part) for state, part in surface.groupby("held_transition", sort=True)}
    yearly = []
    for year, part in surface.groupby(surface["as_of_date"].astype(str).str[:4].astype(int), sort=True):
        yearly.append({"year": int(year), "by_held_position_review_state": {state: _cohort(group) for state, group in part.groupby("held_position_review_state", sort=True)}})
    exit_review = surface.loc[surface["held_position_review_state"] == "ExitReview"]
    hold = surface.loc[surface["held_position_review_state"] == "Hold"]
    metrics = {
        "axis_id": AXIS_ID,
        "contract": {"position_ledger_used": False, "entry_state_and_held_position_review_state_are_separate": True, "automatic_trade_action": False, "runtime_db_write": False},
        "by_entry_state": by_entry,
        "by_held_position_review_state": by_held,
        "by_held_transition": by_transition,
        "exit_review_diagnostics": {
            "bad_rate_delta_exit_review_vs_hold": _cohort(exit_review).get("bad_ret20_rate", 0) - _cohort(hold).get("bad_ret20_rate", 0),
            "severe_rate_delta_exit_review_vs_hold": _cohort(exit_review).get("severe_ret20_rate", 0) - _cohort(hold).get("severe_ret20_rate", 0),
            "winner_rate_exit_review": _cohort(exit_review).get("winner_ret20_rate"),
            "mfe20_mean_exit_review": _cohort(exit_review).get("mfe20_mean"),
            "early_exit_opportunity_risk_note": "winner_rate and mfe20 quantify upside that could be missed by automatic exit",
        },
        "yearly": yearly,
    }
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    surface.to_parquet(output / "position_lifecycle_replay.parquet", index=False)
    surface.head(1000).to_csv(output / "position_lifecycle_replay_sample.csv", index=False)
    _write_json(output / "position_lifecycle_metrics.json", metrics)
    _write_json(output / "research_decision.json", {"decision_class": "READY_REVIEW_ONLY", "research_decision": "position_lifecycle_state_machine_ready_for_manual_review_not_automatic_execution", "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "automatic_trade_action": False, "validated_buy_count": 0})
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--surface-path", type=Path, default=DEFAULT_SURFACE)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(surface_path=args.surface_path, db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
