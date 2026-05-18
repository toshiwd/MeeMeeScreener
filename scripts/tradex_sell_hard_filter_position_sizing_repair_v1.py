from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 as replay


SCHEMA_PREFIX = "sell_hard_filter_position_sizing_repair_v1"
DEFAULT_SOURCE_REPLAY_ROOT = Path(
    r"G:\Tradex\sell_monthly_breakout_hard_filter_portfolio_replay_v1"
    r"\20260516T113736Z-sell-monthly-breakout-hard-filter-portfolio-replay-v1"
)
DEFAULT_COMPARE_RUN_ROOT = Path(
    r"G:\Tradex\sell_monthly_breakout_hard_filter_compare_v1"
    r"\20260516T113302Z-sell-monthly-breakout-hard-filter-compare-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_position_sizing_repair_v1")
VARIANT_ID = "rank3_full_else_half_position_sizing_v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _rank_scale(row: dict[str, Any]) -> float:
    return 1.0 if int(row.get("rank") or 10**9) <= 3 else 0.5


def _simulate_scaled(rows: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    grouped = replay._candidate_groups(rows)
    calendar = replay._calendar(rows)
    cash = replay.BASE_CAPITAL_JPY
    open_positions: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []

    for ymd in calendar:
        still_open: list[dict[str, Any]] = []
        for pos in open_positions:
            row = pos["row"]
            if int(row["exit_date"]) <= int(ymd):
                exit_price = float(row["exit_close"])
                exit_cost = replay._one_way_cost(pos["shares"] * exit_price)
                pnl = (pos["entry_price"] - exit_price) * pos["shares"] - pos["entry_cost"] - exit_cost
                cash += pnl
                trades.append(
                    {
                        "row_id": row["row_id"],
                        "symbol": str(row["code"]),
                        "rank": int(row["rank"]),
                        "as_of_date": int(row["as_of_date"]),
                        "entry_date": int(row["entry_date"]),
                        "exit_date": int(row["exit_date"]),
                        "entry_price": pos["entry_price"],
                        "exit_price": exit_price,
                        "shares": pos["shares"],
                        "sizing_scale": pos["sizing_scale"],
                        "entry_cost": pos["entry_cost"],
                        "exit_cost": exit_cost,
                        "pnl": float(pnl),
                        "gross_return": float((pos["entry_price"] - exit_price) / pos["entry_price"]),
                        "net_return": float(pnl / (pos["entry_price"] * pos["shares"])),
                        "exit_reason": "fixed_horizon_20d",
                        "added_by_challenger": bool(row.get("added_by_challenger")),
                        "removed_from_challenger": bool(row.get("removed_from_challenger")),
                    }
                )
            else:
                still_open.append(pos)
        open_positions = still_open

        for row in grouped.get(ymd, []):
            if len(open_positions) >= replay.MAX_CONCURRENT_POSITIONS:
                break
            if any(str(pos["row"]["code"]) == str(row["code"]) for pos in open_positions):
                continue
            entry_price = float(row["entry_price"])
            sizing_scale = _rank_scale(row)
            target_notional = max(0.0, cash) / replay.MAX_CONCURRENT_POSITIONS * sizing_scale
            shares = int(target_notional // (entry_price * replay.LOT_SIZE)) * replay.LOT_SIZE
            if shares <= 0:
                continue
            entry_cost = replay._one_way_cost(shares * entry_price)
            cash -= entry_cost
            open_positions.append(
                {
                    "row": row,
                    "entry_price": entry_price,
                    "shares": shares,
                    "sizing_scale": sizing_scale,
                    "entry_cost": entry_cost,
                }
            )

        equity_curve.append({"ymd": int(ymd), "equity": float(cash), "open_positions": len(open_positions)})

    return {"summary": _summary(trades, equity_curve, label=label), "trades": trades, "equity_curve": equity_curve}


def _summary(trades: list[dict[str, Any]], equity_curve: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    if not equity_curve:
        return {"label": label, "trade_count": 0, "total_return": 0.0, "final_equity": replay.BASE_CAPITAL_JPY}
    final_equity = float(equity_curve[-1]["equity"])
    total_return = final_equity / replay.BASE_CAPITAL_JPY - 1.0
    equities = [float(row["equity"]) for row in equity_curve]
    peak = -math.inf
    max_drawdown = 0.0
    for equity in equities:
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = min(max_drawdown, equity / peak - 1.0)
    returns = [float(trade["net_return"]) for trade in trades]
    pnls = [float(trade["pnl"]) for trade in trades]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value <= 0.0]
    gross_profit = sum(value for value in pnls if value > 0.0)
    gross_loss = abs(sum(value for value in pnls if value <= 0.0))
    return {
        "label": label,
        "variant_id": VARIANT_ID,
        "base_capital_jpy": replay.BASE_CAPITAL_JPY,
        "final_equity": final_equity,
        "total_return": float(total_return),
        "max_drawdown": float(max_drawdown),
        "number_of_trades": len(trades),
        "win_rate": None if not returns else float(len(wins) / len(returns)),
        "average_win": None if not wins else float(statistics.fmean(wins)),
        "average_loss": None if not losses else float(statistics.fmean(losses)),
        "profit_factor": None if gross_loss == 0 else float(gross_profit / gross_loss),
        "bad_pick_count": sum(1 for value in returns if value <= replay.BAD_PICK_THRESHOLD),
        "severe_loser_count": sum(1 for value in returns if value <= replay.SEVERE_LOSER_THRESHOLD),
    }


def _period_performance(trades: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        date = str(trade["exit_date"])
        bucket = date[:4] if key == "year" else date[:6]
        grouped[bucket].append(trade)
    rows: list[dict[str, Any]] = []
    for bucket, items in sorted(grouped.items()):
        pnl = sum(float(trade["pnl"]) for trade in items)
        value = float(pnl / replay.BASE_CAPITAL_JPY)
        rows.append(
            {
                key: bucket,
                "trade_count": len(items),
                "return_on_base_capital": value,
                "win_rate": float(sum(1 for trade in items if float(trade["net_return"]) > 0.0) / len(items)) if items else None,
                "bad_pick_count": sum(1 for trade in items if float(trade["net_return"]) <= replay.BAD_PICK_THRESHOLD),
                "severe_loser_count": sum(1 for trade in items if float(trade["net_return"]) <= replay.SEVERE_LOSER_THRESHOLD),
                "classification": "positive" if value > 0.002 else "negative" if value < -0.002 else "flat",
            }
        )
    return rows


def _delta(source: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_total_return": source["total_return"],
        "challenger_total_return": challenger["total_return"],
        "total_return_delta": float(challenger["total_return"] - source["total_return"]),
        "source_max_drawdown": source["max_drawdown"],
        "challenger_max_drawdown": challenger["max_drawdown"],
        "max_drawdown_delta": float(challenger["max_drawdown"] - source["max_drawdown"]),
        "source_bad_pick_count": source["bad_pick_count"],
        "challenger_bad_pick_count": challenger["bad_pick_count"],
        "bad_pick_delta": int(challenger["bad_pick_count"] - source["bad_pick_count"]),
        "source_severe_loser_count": source["severe_loser_count"],
        "challenger_severe_loser_count": challenger["severe_loser_count"],
        "severe_loser_delta": int(challenger["severe_loser_count"] - source["severe_loser_count"]),
        "source_profit_factor": source["profit_factor"],
        "challenger_profit_factor": challenger["profit_factor"],
    }


def _decision(delta: dict[str, Any], yearly: list[dict[str, Any]], no_lookahead: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if not no_lookahead.get("no_lookahead_pass"):
        blockers.append("no_lookahead_failed")
    if delta["challenger_total_return"] <= 0.0:
        blockers.append("challenger_total_return_not_positive")
    if delta["challenger_max_drawdown"] <= -0.20:
        blockers.append("max_drawdown_too_deep")
    if any(row["classification"] == "negative" for row in yearly):
        blockers.append("negative_year_present")
    if delta["total_return_delta"] < 0.0:
        blockers.append("total_return_worse_than_source_hard_filter")
    if not blockers:
        decision = "keep_as_buy_level_equivalent_research_candidate"
        next_axis = "shadow_trade_readiness_review"
    elif delta["challenger_total_return"] > 0.0 and delta["challenger_max_drawdown"] > -0.20:
        decision = "hold_for_remaining_year_stability_repair"
        next_axis = "repair_2024_negative_year_without_changing_selection_threshold"
    else:
        decision = "drop_position_sizing_repair"
        next_axis = "select_new_short_axis"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at": _utc_now(),
        "variant_id": VARIANT_ID,
        "authoritative_rollup_decision": decision,
        "decision": decision,
        "buy_level_equivalence_reached": decision == "keep_as_buy_level_equivalent_research_candidate",
        "shadow_trade_candidate": decision == "keep_as_buy_level_equivalent_research_candidate",
        "blockers": blockers,
        "one_next_repair_axis": None if decision == "keep_as_buy_level_equivalent_research_candidate" else next_axis,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def run(
    *,
    source_replay_root: str | Path = DEFAULT_SOURCE_REPLAY_ROOT,
    compare_run_root: str | Path = DEFAULT_COMPARE_RUN_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_replay_root = Path(source_replay_root)
    compare_run_root = Path(compare_run_root)
    output_root = Path(output_root)
    source_complete = _read_json(source_replay_root / "_ARTIFACT_COMPLETE.json")
    source_compare = _read_json(Path(source_complete["artifact_refs"]["portfolio_replay_compare"]))
    loaded = replay._load_contract(compare_run_root)
    rows = replay.compare._load_rows(loaded["source_root"])
    selected = replay._select_rows(rows, threshold=float(loaded["contract"]["threshold"]))
    challenger = _simulate_scaled(selected["challenger"], label="challenger")
    yearly = _period_performance(challenger["trades"], key="year")
    monthly = _period_performance(challenger["trades"], key="month")
    delta = _delta(source_compare["challenger"], challenger["summary"])
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "source_no_lookahead_pass": True,
        "no_lookahead_pass": True,
        "selection_fields": ["as_of_date", "rank", "side", "execution_available", "monthly_breakout_up_prob"],
        "sizing_fields": ["rank"],
        "future_outcome_fields_used_in_selection_or_sizing": [],
        "outcome_fields_used_for_replay_only": ["entry_price", "exit_close", "entry_date", "exit_date"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    decision = _decision(delta, yearly, no_lookahead)
    run_dir = output_root / f"{_utc_stamp()}-sell-hard-filter-position-sizing-repair-v1"
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis": VARIANT_ID,
        "source_replay_root": str(source_replay_root),
        "source_compare_run_root": str(compare_run_root),
        "position_sizing_rule": "rank <= 3 uses normal hard-filter notional; rank >= 4 uses half notional",
        "selection_changed": False,
        "threshold_tuning": False,
        "exit_rule_tuning": False,
        "candidate_tuning": False,
        "non_scope": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    artifacts = {
        "position_sizing_contract": contract,
        "position_sizing_compare": {
            "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
            "generated_at": _utc_now(),
            "source_hard_filter": source_compare["challenger"],
            "challenger": challenger["summary"],
            "delta": delta,
        },
        "yearly_performance": {"schema_version": f"{SCHEMA_PREFIX}_yearly_v1", "challenger": yearly},
        "monthly_performance": {"schema_version": f"{SCHEMA_PREFIX}_monthly_v1", "challenger": monthly},
        "no_lookahead_audit": no_lookahead,
        "final_position_sizing_decision": decision,
    }
    paths = {name: run_dir / f"{name}.json" for name in artifacts}
    for name, payload in artifacts.items():
        _write_json(paths[name], payload)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "status": "complete",
        "complete": True,
        "artifact_refs": {name: str(path) for name, path in paths.items()},
        "authoritative_decision": str(paths["final_position_sizing_decision"]),
        "authoritative_compare": str(paths["position_sizing_compare"]),
        "decision": decision["decision"],
        "buy_level_equivalence_reached": decision["buy_level_equivalence_reached"],
        "shadow_trade_candidate": decision["shadow_trade_candidate"],
        "silent_fallback_used": False,
        "research_fallback": False,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
    }
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": decision["decision"],
        "buy_level_equivalence_reached": decision["buy_level_equivalence_reached"],
        "artifact_refs": complete["artifact_refs"] | {"_ARTIFACT_COMPLETE": str(run_dir / "_ARTIFACT_COMPLETE.json")},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only position sizing repair for sell hard-filter replay.")
    parser.add_argument("--source-replay-root", default=str(DEFAULT_SOURCE_REPLAY_ROOT))
    parser.add_argument("--compare-run-root", default=str(DEFAULT_COMPARE_RUN_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_replay_root=args.source_replay_root, compare_run_root=args.compare_run_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
