from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import tradex_position_lifecycle_state_machine_v1 as lifecycle


AXIS_ID = "position_lifecycle_current_board_v1"
DEFAULT_CURRENT_BOARD = Path(
    r"G:\Tradex\practical_current_review_board_v1"
    r"\20260602T105634Z-practical_current_review_board_v1\current_review_board.csv"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\position_lifecycle_current_board_v1")


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(*, current_board: Path, output_root: Path) -> Path:
    rows = pd.read_csv(current_board)
    required = ["failed_high_flag", "close_vs_ma20_pct", "weekly_supportive_flag", "volatility_compression_breakout_preparation_candidate", "constructive_pullback_support_bullish_confirmation_reference_match", "early_trend_reclaim_controlled_extension_candidate", "monthly_weekly_supportive_daily_confirmation_candidate"]
    missing = [column for column in required if column not in rows]
    if missing:
        raise ValueError(f"current board missing lifecycle columns: {missing}")
    for column in ["weekly_supportive_flag", "failed_high_flag", "volatility_compression_breakout_preparation_candidate", "constructive_pullback_support_bullish_confirmation_reference_match", "early_trend_reclaim_controlled_extension_candidate", "monthly_weekly_supportive_daily_confirmation_candidate"]:
        rows[column] = rows[column].astype(str).str.lower().eq("true")
    rows["entry_state"] = rows.apply(lifecycle._entry_state, axis=1)
    rows["held_position_review_state"] = rows.apply(lifecycle._held_state, axis=1)
    rows["lifecycle_reason_codes"] = rows.apply(lifecycle._reasons, axis=1).map(json.dumps)
    rows["entry_state_sort_order"] = rows["entry_state"].map({"Starter": 0, "Accumulate": 1, "Wait": 2, "Avoid": 3})
    rows = rows.sort_values(["entry_state_sort_order", "entry_actionability_score"], ascending=[True, False])
    output = output_root / f"{_tag()}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    rows.to_csv(output / "current_position_lifecycle_board.csv", index=False)
    rows.to_json(output / "current_position_lifecycle_board.json", orient="records", indent=2)
    _write_json(output / "current_position_lifecycle_board_audit.json", {
        "axis_id": AXIS_ID, "latest_as_of": int(rows["as_of_date"].max()), "row_count": len(rows),
        "entry_state_distribution": rows["entry_state"].value_counts().sort_index().to_dict(),
        "held_position_review_state_distribution": rows["held_position_review_state"].value_counts().sort_index().to_dict(),
        "position_ledger_used": False, "automatic_trade_action": False,
        "interpretation": "entry_state is for unheld candidates; held_position_review_state is for positions already held",
        "meemee_unchanged": True, "production_ranking_changed": False, "runtime_db_write": False,
    })
    _write_json(output / "research_decision.json", {"decision_class": "READY_REVIEW_ONLY", "research_decision": "current_position_lifecycle_board_ready_for_manual_support", "automatic_trade_action": False, "validated_buy_count": 0, "meemee_unchanged": True, "production_ranking_changed": False, "runtime_db_write": False})
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output), "decision_class": "READY_REVIEW_ONLY"})
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-board", type=Path, default=DEFAULT_CURRENT_BOARD)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(current_board=args.current_board, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
