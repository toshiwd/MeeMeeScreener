from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


OUT_ROOT = Path(r"G:\Tradex\portfolio_policy_10m_v1")
LONG_ROOT = Path(r"G:\Tradex\shape_entry_current_board_v1")
SHORT_ROLLUP = Path(r"G:\Tradex\short_10d_big_goal_research_rollup_v1\latest_big_goal_rollup.json")
CAPITAL = 10_000_000.0
MAX_POSITIONS = 10
MAX_SINGLE_NOTIONAL = CAPITAL / MAX_POSITIONS
LONG_STOP_PCT = 0.05


def _latest_board() -> Path:
    boards = list(LONG_ROOT.glob("*/shape_entry_current_board.json"))
    if not boards:
        raise FileNotFoundError("No long current board artifact")
    return max(boards, key=lambda path: path.stat().st_mtime)


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = OUT_ROOT / f"{stamp}-portfolio_policy_10m_v1"
    output.mkdir(parents=True, exist_ok=False)
    long_path = _latest_board()
    long_board = _read(long_path)
    short_rollup = _read(SHORT_ROLLUP)
    capacity = long_board["portfolio_capacity"]
    short_decision = short_rollup["decision_summary"]["authoritative_rollup_decision"]
    short_admitted = short_decision.startswith("adopt")
    payload = {
        "schema_version": "tradex_portfolio_policy_10m_v1.board.v1",
        "authoritative_result": True,
        "boundary": "TRADEX review-only; no MeeMee mutation or order placement",
        "capital_contract": {
            "gross_cap_yen": CAPITAL,
            "leverage": "not used",
            "gross_definition": "long notional plus short notional; netting long and short does not create new capacity",
            "maximum_positions": MAX_POSITIONS,
            "maximum_single_notional_yen": MAX_SINGLE_NOTIONAL,
            "same_day_maximum_new_positions": 3,
            "maximum_total_modeled_stop_loss_yen": CAPITAL * LONG_STOP_PCT,
            "risk_contract": "Each admitted position reserves notional times its validated stop distance. The sum of reserved stop loss may not exceed 500,000 yen; a future short must consume this same shared risk budget rather than receive a separate hedge allowance.",
        },
        "long_admission": {
            "source_board": str(long_path),
            "requires": ["confirmed PAN adopted shallow-high-zone shape", "not already held", "top three same-day close/MA60 priority", "next open not above confirmed close", "gross and position capacity available"],
            "current_review_entry_count": long_board["actionable_entry_count"],
        },
        "short_admission": {
            "source_rollup": str(SHORT_ROLLUP),
            "authoritative_rollup_decision": short_decision,
            "current_allocation_allowed": short_admitted,
            "policy": "zero short allocation unless a separately current, PAN-confirmed short artifact has an adoption decision; review-only or hold short artifacts are not sufficient.",
        },
        "hedge_contract": {
            "allowed": short_admitted,
            "policy": "a hedge requires both an admitted long and an admitted short. Do not create a short merely to reduce net exposure.",
        },
        "current_capacity": capacity,
        "current_portfolio_decision": (
            "freeze_new_entries_reduce_gross_to_cap"
            if float(capacity["gross_overage_yen"]) > 0 or not bool(capacity["account_wide_capacity_available"])
            else "review_new_entries_only_under_side_admission_rules"
        ),
        "what_not_changed": ["positions_live", "trade_events", "MeeMee ranking", "order placement"],
    }
    (output / "portfolio_policy_board.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "research_decision.json").write_text(json.dumps({
        "authoritative_rollup_decision": payload["current_portfolio_decision"],
        "short_allocation_allowed": short_admitted,
        "runtime_db_write": False,
        "production_ranking_changed": False,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


if __name__ == "__main__":
    print(run())
