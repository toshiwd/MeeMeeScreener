from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_2026_momentum_leader_reentry_selection_v1 as selection
from scripts import tradex_pattern_family_source_rows_v1 as source
from scripts import tradex_position_lifecycle_multiyear_momentum_regime_audit_v1 as regime


AXIS_ID = "2026_momentum_leader_reentry_current_board_v1"
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\2026_momentum_leader_reentry_current_board_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(*, db_path: Path, output_root: Path) -> Path:
    raw = source.load_confirmed_daily_bars(db_path, 20200101, 20991231)
    latest_as_of = int(raw["as_of_date"].max())
    featured = source.add_family_flags(source.attach_period_features(source.add_daily_features(raw)))
    current = featured.loc[featured["as_of_date"] == latest_as_of].copy()
    breadth = {
        "breadth_above_ma20": float(current["close_above_ma20"].mean()),
        "breadth_above_ma60": float(current["close_above_ma60"].mean()),
        "trend_participation": float(current["monthly_weekly_supportive_daily_confirmation_candidate"].mean()),
        "overextension_share": float((current["close_vs_ma20_pct"] > 0.12).mean()),
    }
    market_regime = regime._regime(pd.Series(breadth))
    current["market_momentum_regime"] = market_regime
    current["momentum_regime_flag"] = current["market_momentum_regime"].isin(selection.MOMENTUM_REGIMES)
    current["relative_strength_score"] = (
        current["close_vs_ma20_pct"].rank(pct=True)
        + current["close_vs_ma60_pct"].rank(pct=True)
        + current["weekly_close_vs_ma20_pct"].rank(pct=True)
        + current["monthly_close_vs_ma20_pct"].rank(pct=True)
    ) / 4
    current["relative_strength_percentile_same_day"] = current["relative_strength_score"].rank(pct=True)
    current["leader_flag"] = current["relative_strength_percentile_same_day"] >= 0.85
    current["momentum_leader_state"] = current.apply(selection._classify, axis=1)
    order = {"ReentryReady": 0, "LeaderWatch": 1, "ChaseAvoid": 2, "TrendBroken": 3, "NonLeader": 4}
    current["state_sort_order"] = current["momentum_leader_state"].map(order)
    board = current.sort_values(["state_sort_order", "relative_strength_percentile_same_day"], ascending=[True, False])
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    board.to_csv(output / "current_momentum_leader_reentry_board.csv", index=False)
    board.to_json(output / "current_momentum_leader_reentry_board.json", orient="records", indent=2)
    _write_json(output / "current_momentum_leader_reentry_board_audit.json", {"axis_id": AXIS_ID, "latest_as_of": latest_as_of, "market_regime": market_regime, "breadth": breadth, "row_count": len(board), "state_distribution": board["momentum_leader_state"].value_counts().sort_index().to_dict(), "theme_name_used": False, "hindsight_sector_label_used": False, "runtime_db_write": False, "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False})
    _write_json(output / "research_decision.json", {"decision_class": "READY_REVIEW_ONLY", "research_decision": "current_momentum_leader_reentry_board_ready_for_manual_support", "automatic_trade_action": False, "validated_buy_count": 0, "runtime_db_write": False, "meemee_unchanged": True, "production_ranking_changed": False, "candidate_generation_changed": False})
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
