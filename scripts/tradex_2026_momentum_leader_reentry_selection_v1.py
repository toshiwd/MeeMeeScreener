from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "2026_momentum_leader_reentry_selection_v1"
DEFAULT_SOURCE = Path(
    r"G:\Tradex\position_lifecycle_multiyear_momentum_regime_audit_v1"
    r"\20260602T121800Z-position_lifecycle_multiyear_momentum_regime_audit_v1"
    r"\position_lifecycle_multiyear_regime_rows.parquet"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\2026_momentum_leader_reentry_selection_v1")
MOMENTUM_REGIMES = {"broad_momentum", "overextended_momentum"}


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _classify(row: pd.Series) -> str:
    if not bool(row["momentum_regime_flag"]) or not bool(row["leader_flag"]):
        return "NonLeader"
    if bool(row["failed_high_flag"]) or row["close_vs_ma20_pct"] < -0.06 or row["ma20_slope_10d"] < 0:
        return "TrendBroken"
    if row["close_vs_ma20_pct"] > 0.12 or row["recent_high_distance_pct"] > -0.005 or row["volume_vs_20d_avg"] > 2.5:
        return "ChaseAvoid"
    if row["close_vs_ma20_pct"] <= 0.06 and row["ma7_slope_5d"] > 0 and row["ma20_slope_10d"] > 0 and row["volume_vs_20d_avg"] >= 0.75 and not bool(row["bearish_body_flag"]):
        return "ReentryReady"
    return "LeaderWatch"


def _cohort(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"row_count": 0}
    return {
        "row_count": len(frame),
        "date_count": int(frame["as_of_date"].nunique()),
        "code_count": int(frame["code"].nunique()),
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
    rows["year"] = rows["as_of_date"].astype(str).str[:4].astype(int)
    rows["momentum_regime_flag"] = rows["market_momentum_regime"].isin(MOMENTUM_REGIMES)
    rows["relative_strength_score"] = (
        rows["close_vs_ma20_pct"].rank(pct=True)
        + rows["close_vs_ma60_pct"].rank(pct=True)
        + rows["weekly_close_vs_ma20_pct"].rank(pct=True)
        + rows["monthly_close_vs_ma20_pct"].rank(pct=True)
    ) / 4
    rows["relative_strength_percentile_same_day"] = rows.groupby("as_of_date")["relative_strength_score"].rank(pct=True)
    rows["leader_flag"] = rows["relative_strength_percentile_same_day"] >= 0.85
    rows["momentum_leader_state"] = rows.apply(_classify, axis=1)
    reference = rows.loc[rows["year"] <= 2025]
    validation = rows.loc[rows["year"] == 2026]
    by_period = {
        "reference_2020_2025": {state: _cohort(part) for state, part in reference.groupby("momentum_leader_state", sort=True)},
        "validation_2026": {state: _cohort(part) for state, part in validation.groupby("momentum_leader_state", sort=True)},
    }
    validation_states = by_period["validation_2026"]
    reentry = validation_states.get("ReentryReady", {"row_count": 0})
    chase = validation_states.get("ChaseAvoid", {"row_count": 0})
    ready = bool(
        reentry.get("row_count", 0) >= 50
        and reentry.get("ret20_mean", 0) > chase.get("ret20_mean", 0)
        and reentry.get("bad_ret20_rate", 1) < chase.get("bad_ret20_rate", 1)
    )
    latest_as_of = int(rows["as_of_date"].max())
    latest = rows.loc[rows["as_of_date"] == latest_as_of].copy()
    order = {"ReentryReady": 0, "LeaderWatch": 1, "ChaseAvoid": 2, "TrendBroken": 3, "NonLeader": 4}
    latest["state_sort_order"] = latest["momentum_leader_state"].map(order)
    latest = latest.sort_values(["state_sort_order", "relative_strength_percentile_same_day"], ascending=[True, False])
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    rows.to_parquet(output / "momentum_leader_reentry_rows.parquet", index=False)
    latest.to_csv(output / "latest_historical_review_board.csv", index=False)
    _write_json(output / "momentum_leader_reentry_compare.json", {"axis_id": AXIS_ID, "contract": {"theme_name_used": False, "hindsight_sector_label_used": False, "point_in_time_features_only": True, "state_thresholds_fixed_before_2026_validation": True}, "by_period": by_period, "latest_historical_as_of": latest_as_of})
    _write_json(output / "research_decision.json", {"decision_class": "KEEP_REVIEW_ONLY" if ready else "DROP", "research_decision": "keep_2026_momentum_leader_reentry_for_manual_support" if ready else "drop_2026_momentum_leader_reentry_no_fixed_condition_edge", "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "automatic_trade_action": False, "validated_buy_count": 0})
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "KEEP_REVIEW_ONLY" if ready else "DROP"})
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
