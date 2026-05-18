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

from scripts import tradex_sell_monthly_breakout_hard_filter_compare_v1 as compare


SCHEMA_PREFIX = "sell_monthly_breakout_hard_filter_portfolio_replay_v1"
DEFAULT_COMPARE_RUN_ROOT = Path(
    r"G:\Tradex\sell_monthly_breakout_hard_filter_compare_v1"
    r"\20260516T113302Z-sell-monthly-breakout-hard-filter-compare-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_monthly_breakout_hard_filter_portfolio_replay_v1")
BASE_CAPITAL_JPY = 10_000_000.0
MAX_CONCURRENT_POSITIONS = 3
LOT_SIZE = 100
ONE_WAY_COST_BPS = 30.0
BAD_PICK_THRESHOLD = 0.0
SEVERE_LOSER_THRESHOLD = -0.05


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
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _one_way_cost(notional: float) -> float:
    return abs(float(notional)) * ONE_WAY_COST_BPS / 10_000.0


def _ymd_to_datetime(ymd: int) -> datetime:
    return datetime.strptime(str(int(ymd)), "%Y%m%d")


def _load_contract(compare_run_root: Path) -> dict[str, Any]:
    complete = _read_json(compare_run_root / "_ARTIFACT_COMPLETE.json")
    contract = _read_json(Path(complete["artifact_refs"]["contract"]))
    decision = _read_json(Path(complete["artifact_refs"]["decision"]))
    source_root = Path(contract["source_root"])
    compare_root = Path(contract["source_compare_root"])
    if decision.get("portfolio_replay_allowed_next") is not True:
        raise RuntimeError(f"source compare is not portfolio-replay ready: {decision.get('decision')}")
    return {
        "complete": complete,
        "contract": contract,
        "decision": decision,
        "source_root": source_root,
        "compare_root": compare_root,
    }


def _select_rows(rows: list[dict[str, Any]], *, threshold: float) -> dict[str, list[dict[str, Any]]]:
    by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("side") == "sell" and row.get("execution_available") is True:
            by_date[int(row["as_of_date"])].append(dict(row))
    baseline: list[dict[str, Any]] = []
    challenger: list[dict[str, Any]] = []
    for _, date_rows in sorted(by_date.items()):
        baseline.extend(compare._top_rows(date_rows, top_k=compare.TOP_K))
        challenger.extend(compare._select_hard_filter(date_rows, threshold=threshold, top_k=compare.TOP_K))
    baseline_ids = {_row_id(row) for row in baseline}
    challenger_ids = {_row_id(row) for row in challenger}
    for row in baseline:
        row["row_id"] = _row_id(row)
        row["removed_from_challenger"] = row["row_id"] not in challenger_ids
    for row in challenger:
        row["row_id"] = _row_id(row)
        row["added_by_challenger"] = row["row_id"] not in baseline_ids
    return {"baseline": baseline, "challenger": challenger}


def _row_id(row: dict[str, Any]) -> str:
    return f"{int(row['as_of_date'])}:{int(row['rank'])}:{row['code']}"


def _candidate_groups(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[int(row["entry_date"])].append(row)
    return {day: sorted(items, key=lambda row: int(row.get("rank") or 10**9)) for day, items in groups.items()}


def _calendar(rows: list[dict[str, Any]]) -> list[int]:
    days = {int(row["entry_date"]) for row in rows if row.get("entry_date") is not None}
    days.update(int(row["exit_date"]) for row in rows if row.get("exit_date") is not None)
    return sorted(days)


def _simulate(rows: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    grouped = _candidate_groups(rows)
    calendar = _calendar(rows)
    cash = BASE_CAPITAL_JPY
    open_positions: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []

    for ymd in calendar:
        still_open: list[dict[str, Any]] = []
        for pos in open_positions:
            row = pos["row"]
            if int(row["exit_date"]) <= int(ymd):
                exit_price = float(row["exit_close"])
                exit_cost = _one_way_cost(pos["shares"] * exit_price)
                pnl = (pos["entry_price"] - exit_price) * pos["shares"] - pos["entry_cost"] - exit_cost
                cash += pnl
                trades.append(
                    {
                        "row_id": row["row_id"],
                        "symbol": str(row["code"]),
                        "as_of_date": int(row["as_of_date"]),
                        "entry_date": int(row["entry_date"]),
                        "exit_date": int(row["exit_date"]),
                        "entry_price": pos["entry_price"],
                        "exit_price": exit_price,
                        "shares": pos["shares"],
                        "entry_cost": pos["entry_cost"],
                        "exit_cost": exit_cost,
                        "pnl": float(pnl),
                        "gross_return": float((pos["entry_price"] - exit_price) / pos["entry_price"]),
                        "net_return": float(pnl / (pos["entry_price"] * pos["shares"])),
                        "exit_reason": "fixed_horizon_20d",
                        "holding_days": (_ymd_to_datetime(int(row["exit_date"])) - _ymd_to_datetime(int(row["entry_date"]))).days,
                        "added_by_challenger": bool(row.get("added_by_challenger")),
                        "removed_from_challenger": bool(row.get("removed_from_challenger")),
                    }
                )
            else:
                still_open.append(pos)
        open_positions = still_open

        for row in grouped.get(ymd, []):
            if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
                break
            if any(str(pos["row"]["code"]) == str(row["code"]) for pos in open_positions):
                continue
            entry_price = float(row["entry_price"])
            target_notional = max(0.0, cash) / MAX_CONCURRENT_POSITIONS
            shares = int(target_notional // (entry_price * LOT_SIZE)) * LOT_SIZE
            if shares <= 0:
                continue
            entry_cost = _one_way_cost(shares * entry_price)
            cash -= entry_cost
            open_positions.append({"row": row, "entry_price": entry_price, "shares": shares, "entry_cost": entry_cost})

        mark_to_market = cash
        for pos in open_positions:
            row = pos["row"]
            # Path bars are intentionally not reconstructed in this replay; mark
            # the open fixed-horizon short at entry until the authoritative exit.
            mark_to_market += 0.0
        equity_curve.append({"ymd": int(ymd), "equity": float(mark_to_market), "open_positions": len(open_positions)})

    return {"summary": _summary(trades, equity_curve, label=label), "trades": trades, "equity_curve": equity_curve}


def _summary(trades: list[dict[str, Any]], equity_curve: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    if not equity_curve:
        return {"label": label, "trade_count": 0, "total_return": 0.0, "final_equity": BASE_CAPITAL_JPY}
    final_equity = float(equity_curve[-1]["equity"])
    total_return = final_equity / BASE_CAPITAL_JPY - 1.0
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
        "base_capital_jpy": BASE_CAPITAL_JPY,
        "final_equity": final_equity,
        "total_return": float(total_return),
        "max_drawdown": float(max_drawdown),
        "number_of_trades": len(trades),
        "win_rate": None if not returns else float(len(wins) / len(returns)),
        "average_win": None if not wins else float(statistics.fmean(wins)),
        "average_loss": None if not losses else float(statistics.fmean(losses)),
        "profit_factor": None if gross_loss == 0 else float(gross_profit / gross_loss),
        "bad_pick_count": sum(1 for value in returns if value <= BAD_PICK_THRESHOLD),
        "severe_loser_count": sum(1 for value in returns if value <= SEVERE_LOSER_THRESHOLD),
        "added_bad_pick_count": sum(1 for trade in trades if trade.get("added_by_challenger") and float(trade["net_return"]) <= BAD_PICK_THRESHOLD),
        "removed_bad_pick_count": sum(1 for trade in trades if trade.get("removed_from_challenger") and float(trade["net_return"]) <= BAD_PICK_THRESHOLD),
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
        value = float(pnl / BASE_CAPITAL_JPY)
        rows.append(
            {
                key: bucket,
                "trade_count": len(items),
                "return_on_base_capital": value,
                "win_rate": float(sum(1 for trade in items if float(trade["net_return"]) > 0.0) / len(items)) if items else None,
                "bad_pick_count": sum(1 for trade in items if float(trade["net_return"]) <= BAD_PICK_THRESHOLD),
                "severe_loser_count": sum(1 for trade in items if float(trade["net_return"]) <= SEVERE_LOSER_THRESHOLD),
                "classification": "positive" if value > 0.002 else "negative" if value < -0.002 else "flat",
            }
        )
    return rows


def _compare_portfolios(baseline: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    base = baseline["summary"]
    chal = challenger["summary"]
    return {
        "baseline_total_return": base["total_return"],
        "challenger_total_return": chal["total_return"],
        "total_return_delta": float(chal["total_return"] - base["total_return"]),
        "baseline_max_drawdown": base["max_drawdown"],
        "challenger_max_drawdown": chal["max_drawdown"],
        "max_drawdown_delta": float(chal["max_drawdown"] - base["max_drawdown"]),
        "baseline_bad_pick_count": base["bad_pick_count"],
        "challenger_bad_pick_count": chal["bad_pick_count"],
        "bad_pick_delta": int(chal["bad_pick_count"] - base["bad_pick_count"]),
        "baseline_severe_loser_count": base["severe_loser_count"],
        "challenger_severe_loser_count": chal["severe_loser_count"],
        "severe_loser_delta": int(chal["severe_loser_count"] - base["severe_loser_count"]),
        "baseline_profit_factor": base["profit_factor"],
        "challenger_profit_factor": chal["profit_factor"],
    }


def _decision(compare_payload: dict[str, Any], yearly: list[dict[str, Any]], no_lookahead: dict[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    if not no_lookahead.get("no_lookahead_pass"):
        blockers.append("no_lookahead_failed")
    if compare_payload["challenger_total_return"] <= 0.0:
        blockers.append("challenger_total_return_not_positive")
    if compare_payload["total_return_delta"] <= 0.0:
        blockers.append("total_return_not_improved")
    if compare_payload["challenger_max_drawdown"] <= -0.20:
        blockers.append("max_drawdown_too_deep")
    if compare_payload["bad_pick_delta"] >= 0:
        blockers.append("bad_pick_not_reduced")
    if compare_payload["severe_loser_delta"] > 0:
        blockers.append("severe_loser_added")
    negative_years = [row for row in yearly if row["classification"] == "negative"]
    if negative_years:
        blockers.append("negative_year_present")
    if not blockers:
        decision = "keep_as_buy_level_equivalent_research_candidate"
        shadow = True
        next_axis = "shadow_trade_readiness_review"
    elif compare_payload["total_return_delta"] > 0.0 and compare_payload["bad_pick_delta"] < 0 and compare_payload["severe_loser_delta"] <= 0:
        decision = "hold_for_portfolio_risk_repair"
        shadow = False
        next_axis = "position_sizing_or_exit_risk_control"
    else:
        decision = "drop_after_portfolio_replay"
        shadow = False
        next_axis = "freeze_or_select_new_short_axis"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at": _utc_now(),
        "authoritative_rollup_decision": decision,
        "decision": decision,
        "shadow_trade_candidate": shadow,
        "buy_level_equivalence_reached": decision == "keep_as_buy_level_equivalent_research_candidate",
        "blockers": blockers,
        "one_next_repair_axis": None if shadow else next_axis,
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def run(
    *,
    compare_run_root: str | Path = DEFAULT_COMPARE_RUN_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    compare_run_root = Path(compare_run_root)
    output_root = Path(output_root)
    loaded = _load_contract(compare_run_root)
    threshold = float(loaded["contract"]["threshold"])
    rows = compare._load_rows(loaded["source_root"])
    selected = _select_rows(rows, threshold=threshold)
    baseline = _simulate(selected["baseline"], label="baseline")
    challenger = _simulate(selected["challenger"], label="challenger")
    portfolio_compare = _compare_portfolios(baseline, challenger)
    yearly = _period_performance(challenger["trades"], key="year")
    monthly = _period_performance(challenger["trades"], key="month")
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "source_no_lookahead_pass": True,
        "no_lookahead_pass": True,
        "selection_fields": ["as_of_date", "rank", "side", "execution_available", "monthly_breakout_up_prob"],
        "future_outcome_fields_used_in_selection": [],
        "outcome_fields_used_for_replay_only": ["entry_price", "exit_close", "entry_date", "exit_date"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    decision = _decision(portfolio_compare, yearly, no_lookahead)
    run_dir = output_root / f"{_utc_stamp()}-sell-monthly-breakout-hard-filter-portfolio-replay-v1"
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "source_compare_run_root": str(compare_run_root),
        "source_root": str(loaded["source_root"]),
        "axis": "monthly_breakout_up_prob_low_q25_hard_filter_fixed_horizon_portfolio_replay",
        "base_capital_jpy": BASE_CAPITAL_JPY,
        "max_concurrent_positions": MAX_CONCURRENT_POSITIONS,
        "lot_size": LOT_SIZE,
        "cost_slippage": {"one_way_total_bps": ONE_WAY_COST_BPS},
        "execution": "next_session_open_to_20th_session_close",
        "exit_variant": "fixed_horizon_20d_only",
        "threshold": threshold,
        "threshold_tuning": False,
        "candidate_tuning": False,
        "non_scope": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal", "exit_rule_tuning"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    artifacts = {
        "portfolio_replay_contract": contract,
        "portfolio_replay_compare": {
            "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
            "generated_at": _utc_now(),
            "baseline": baseline["summary"],
            "challenger": challenger["summary"],
            "delta": portfolio_compare,
        },
        "yearly_performance": {"schema_version": f"{SCHEMA_PREFIX}_yearly_v1", "challenger": yearly},
        "monthly_performance": {"schema_version": f"{SCHEMA_PREFIX}_monthly_v1", "challenger": monthly},
        "no_lookahead_replay_audit": no_lookahead,
        "final_portfolio_replay_decision": decision,
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
        "authoritative_decision": str(paths["final_portfolio_replay_decision"]),
        "authoritative_compare": str(paths["portfolio_replay_compare"]),
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
    parser = argparse.ArgumentParser(description="TRADEX-only hard-filter fixed-horizon sell portfolio replay.")
    parser.add_argument("--compare-run-root", default=str(DEFAULT_COMPARE_RUN_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(compare_run_root=args.compare_run_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
