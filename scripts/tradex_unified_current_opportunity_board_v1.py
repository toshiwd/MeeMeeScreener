from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


AXIS_ID = "tradex_unified_current_opportunity_board_v1"
OUT = Path(r"G:\Tradex\unified_current_opportunity_board_v1")
SHAPE_ROOT = Path(r"G:\Tradex\shape_entry_current_board_v1")
MOMENTUM_ROOT = Path(r"G:\Tradex\2026_momentum_leader_reentry_current_board_v1")
RESEARCH_ROOT = Path(r"G:\Tradex\momentum_reentry_h10_union_v1")


def latest(root: Path, name: str) -> Path:
    files = sorted(root.glob(f"*/{name}"), key=lambda path: path.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"{name} not found under {root}")
    return files[-1]


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def run() -> Path:
    shape_path = latest(SHAPE_ROOT, "shape_entry_current_board.json")
    momentum_path = latest(MOMENTUM_ROOT, "current_momentum_leader_reentry_board_audit.json")
    research_path = latest(RESEARCH_ROOT, "compare.json")
    shape = read(shape_path)
    momentum = read(momentum_path)
    research = read(research_path)

    candidates = []
    for row in shape.get("board", []):
        if row.get("tradex_shape_eligible") and row.get("new_entry_verdict") == "review_entry":
            candidates.append({
                "side": "buy", "code": str(row["code"]), "rule": row["shape_signal"],
                "signal_date": shape["confirmed_signal_date"], "confirmed_close": row["confirmed_close"],
                "entry_condition": "next_session_open_at_or_below_confirmed_close",
                "verdict": "review_entry", "automatic_trade": False,
            })
    momentum_count = int(momentum.get("state_distribution", {}).get("ReentryReady", 0))
    breadth_below = 1.0 - float(momentum["breadth"]["breadth_above_ma20"])
    short_market_gate = breadth_below >= .40
    for rank, row in enumerate(candidates, start=1):
        row["unified_rank"] = rank

    current_metrics = research["reports"]["current_2026"]
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    payload = {
        "schema_version": f"{AXIS_ID}.board.v1", "artifact_role": "authoritative",
        "confirmed_signal_date": shape["confirmed_signal_date"],
        "market_state": momentum["market_regime"],
        "market_breadth": {"above_ma20": momentum["breadth"]["breadth_above_ma20"], "below_ma20": breadth_below},
        "side_availability": {
            "shape_buy_count": len(candidates), "momentum_reentry_buy_count": momentum_count,
            "conditional_short_market_gate_pass": short_market_gate,
        },
        "candidate_count": len(candidates), "candidates": candidates,
        "default_verdict": "review_entry" if candidates else "wait",
        "validated_union_2026": {
            "event_count": current_metrics["combined"]["event_count"],
            "daily_profit_factor": current_metrics["combined"]["daily_profit_factor"],
            "daily_expectancy": current_metrics["combined"]["daily_expectancy"],
            "average_events_per_calendar_week": current_metrics["weekly_coverage"]["average_events_per_calendar_week"],
            "weeks_with_trade": current_metrics["weekly_coverage"]["weeks_with_trade"],
            "calendar_weeks": current_metrics["weekly_coverage"]["calendar_weeks"],
            "maximum_consecutive_empty_weeks": current_metrics["weekly_coverage"]["maximum_consecutive_empty_weeks"],
        },
        "sources": {"shape_board": str(shape_path), "momentum_board": str(momentum_path), "research_compare": str(research_path)},
        "boundary": "TRADEX selection; MeeMee display only", "holdings": "ignored", "capital_allocation": "not used",
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path = output / "current_opportunity_board.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
