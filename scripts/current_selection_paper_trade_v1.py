from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


AXIS_ID = "current_selection_paper_trade_v1"
SCHEMA_PREFIX = "tradex_current_selection_paper_trade_v1"
DEFAULT_OUTPUT_PARENT = Path("G:/Tradex/paper_trades/current_selection_paper_trade_v1")
DEFAULT_INITIAL_CASH = 10_000_000
DEFAULT_MAX_POSITIONS = 3
DEFAULT_LOT_SIZE = 100
DEFAULT_ONE_WAY_COST_BPS = 30

REQUIRED_ARTIFACTS = (
    "paper_trade_summary.json",
    "runtime_state_check.json",
    "selection_contract.json",
    "current_candidate_snapshot.csv",
    "paper_trade_orders.csv",
    "paper_trade_order_intents.jsonl",
    "position_sizing_summary.json",
    "no_mutation_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_ready(dict(row)), ensure_ascii=False, sort_keys=True) + "\n")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _file_stat(path_text: str | None) -> dict[str, Any]:
    if not path_text:
        return {"path": None, "exists": False, "bytes": None, "mtime_ns": None}
    path = Path(path_text)
    if not path.exists():
        return {"path": str(path), "exists": False, "bytes": None, "mtime_ns": None}
    stat = path.stat()
    return {"path": str(path), "exists": True, "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _next_weekday_iso(as_of: str) -> str:
    day = datetime.strptime(as_of, "%Y-%m-%d").date() + timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day.isoformat()


def _ranking_score(item: Mapping[str, Any], rank: int) -> float:
    return _safe_float(item.get("tradePriorityScore"), _safe_float(item.get("entryScore"), 1.0 - rank * 0.01))


def _compact_candidate(item: Mapping[str, Any], rank: int) -> dict[str, Any]:
    risk_watch = item.get("tradeRiskWatch") if isinstance(item.get("tradeRiskWatch"), list) else []
    decision_reasons = item.get("tradeDecisionReasons") if isinstance(item.get("tradeDecisionReasons"), list) else []
    quality_flags = item.get("qualityFlags") if isinstance(item.get("qualityFlags"), list) else []
    return {
        "rank": rank,
        "code": str(item.get("code") or item.get("symbol") or "").strip(),
        "name": item.get("name"),
        "as_of": item.get("asOf"),
        "close": _safe_float(item.get("close")),
        "prev_close": _safe_float(item.get("prevClose")),
        "change_pct": _safe_float(item.get("changePct")),
        "liquidity20d": _safe_float(item.get("liquidity20d")),
        "trade_priority_score": _ranking_score(item, rank),
        "entry_score": _safe_float(item.get("entryScore")),
        "prob_side": _safe_float(item.get("probSide")),
        "entry_qualified": _safe_bool(item.get("entryQualified")),
        "setup_type": item.get("setupType"),
        "trade_entry_class": item.get("tradeEntryClass"),
        "market_regime": item.get("marketRegime"),
        "market_risk_off": _safe_bool(item.get("marketRiskOff")),
        "monthly_box_state": item.get("monthlyBoxState"),
        "momentum_follow_through": _safe_bool(item.get("momentumFollowThroughV1")),
        "momentum_follow_through_score": _safe_float(item.get("momentumFollowThroughScore")),
        "recommended_hold_days": item.get("recommendedHoldDays"),
        "invalidation_trigger": item.get("invalidationTrigger"),
        "invalidation_recommended_action": item.get("invalidationRecommendedAction"),
        "decision_reasons": ";".join(str(x) for x in decision_reasons),
        "risk_watch": ";".join(str(x) for x in risk_watch),
        "quality_flags": ";".join(str(x) for x in quality_flags),
    }


def _paper_orders(
    candidates: list[dict[str, Any]],
    *,
    initial_cash: int,
    max_positions: int,
    lot_size: int,
    one_way_cost_bps: int,
    signal_date: str,
    planned_execution_date: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected = candidates[:max_positions]
    per_slot_cash = initial_cash / max(1, len(selected))
    orders: list[dict[str, Any]] = []
    used_cash = 0.0
    for candidate in selected:
        close = _safe_float(candidate.get("close"))
        if close <= 0:
            quantity = 0
            notional = 0.0
            reject_reason = "missing_close_price"
        else:
            raw_qty = math.floor(per_slot_cash / close / lot_size) * lot_size
            quantity = max(0, int(raw_qty))
            notional = float(quantity * close)
            reject_reason = "" if quantity > 0 else "slot_cash_below_lot_size"
        estimated_cost = notional * (one_way_cost_bps / 10_000.0)
        used_cash += notional + estimated_cost
        orders.append(
            {
                "intent_id": f"{signal_date.replace('-', '')}-{candidate['rank']:02d}-{candidate['code']}",
                "signal_date": signal_date,
                "planned_execution_date": planned_execution_date,
                "execution_policy": "next_session_open_planned",
                "side": "buy",
                "order_type": "paper_market_on_open",
                "code": candidate["code"],
                "name": candidate["name"],
                "rank": candidate["rank"],
                "reference_close": close,
                "quantity": quantity,
                "lot_size": lot_size,
                "estimated_notional_at_reference_close": notional,
                "estimated_one_way_cost": estimated_cost,
                "estimated_total_cash_required": notional + estimated_cost,
                "paper_status": "planned_not_submitted" if quantity > 0 else "blocked",
                "blocked_reason": reject_reason,
                "trade_priority_score": candidate["trade_priority_score"],
                "entry_score": candidate["entry_score"],
                "prob_side": candidate["prob_side"],
                "setup_type": candidate["setup_type"],
                "trade_entry_class": candidate["trade_entry_class"],
                "market_regime": candidate["market_regime"],
                "market_risk_off": candidate["market_risk_off"],
                "decision_reasons": candidate["decision_reasons"],
                "risk_watch": candidate["risk_watch"],
            }
        )
    sizing = {
        "initial_cash": initial_cash,
        "max_positions": max_positions,
        "selected_count": len(selected),
        "lot_size": lot_size,
        "one_way_cost_bps": one_way_cost_bps,
        "per_slot_cash": per_slot_cash,
        "estimated_cash_required_total": used_cash,
        "estimated_cash_remaining": initial_cash - used_cash,
        "all_orders_planned": all(order["paper_status"] == "planned_not_submitted" for order in orders),
    }
    return orders, sizing


def run_current_selection_paper_trade_v1(
    *,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str | None = None,
    initial_cash: int = DEFAULT_INITIAL_CASH,
    max_positions: int = DEFAULT_MAX_POSITIONS,
    lot_size: int = DEFAULT_LOT_SIZE,
    one_way_cost_bps: int = DEFAULT_ONE_WAY_COST_BPS,
    limit: int = 20,
) -> dict[str, Any]:
    from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
    from app.backend.services.ml import rankings_cache

    generated_at = _utc_now()
    run_id = run_id or f"{generated_at.replace(':', '').replace('-', '').replace('+0000', 'Z')}-{AXIS_ID}"
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    runtime_before = get_runtime_stock_db_status()
    db_stat_before = _file_stat(runtime_before.get("selected_runtime_db_path"))
    freshness = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=limit)
    ranking_payload = rankings_cache.get_rankings("D", "latest", "up", limit, mode="trade", risk_mode="balanced")
    runtime_after = get_runtime_stock_db_status()
    db_stat_after = _file_stat(runtime_after.get("selected_runtime_db_path"))

    items = [item for item in list(ranking_payload.get("items") or []) if str(item.get("code") or item.get("symbol") or "").strip()]
    candidates = [_compact_candidate(item, rank) for rank, item in enumerate(items, start=1)]
    signal_date = str(freshness.get("snapshot_as_of") or ranking_payload.get("snapshot_as_of") or "")
    planned_execution_date = _next_weekday_iso(signal_date) if signal_date else "next_session_open_after_signal"
    orders, sizing = _paper_orders(
        candidates,
        initial_cash=initial_cash,
        max_positions=max_positions,
        lot_size=lot_size,
        one_way_cost_bps=one_way_cost_bps,
        signal_date=signal_date,
        planned_execution_date=planned_execution_date,
    )
    no_mutation = {
        "schema_version": f"{SCHEMA_PREFIX}_no_mutation_audit_v1",
        "axis_id": AXIS_ID,
        "runtime_db_stat_before": db_stat_before,
        "runtime_db_stat_after": db_stat_after,
        "no_mutation_pass": db_stat_before == db_stat_after,
        "broker_api_called": False,
        "runtime_db_written": False,
        "ranking_changed": False,
        "publish_registry_changed": False,
        "frontend_changed": False,
    }
    selection_contract = {
        "schema_version": f"{SCHEMA_PREFIX}_selection_contract_v1",
        "axis_id": AXIS_ID,
        "selection_source": "MeeMee runtime D/latest/up/trade/balanced ranking items",
        "paper_trade_only": True,
        "broker_api_allowed": False,
        "actual_order_submission_allowed": False,
        "max_positions": max_positions,
        "long_only": True,
        "cash_only": True,
        "initial_cash": initial_cash,
        "cost_model": {"one_way_cost_bps": one_way_cost_bps},
        "execution_policy": "signal after close; planned entry at next session open; no same-day close fill",
        "position_sizing": "equal_cash_slots_floor_to_lot_size",
        "lot_size": lot_size,
        "manual_review_required_before_real_trade": True,
    }
    runtime_state = {
        "schema_version": f"{SCHEMA_PREFIX}_runtime_state_check_v1",
        "axis_id": AXIS_ID,
        "runtime_stock_db_status_before": runtime_before,
        "rankings_freshness": freshness,
        "runtime_stock_db_status_after": runtime_after,
        "runtime_db_stat_before": db_stat_before,
        "runtime_db_stat_after": db_stat_after,
        "ranking_item_count": len(items),
        "candidate_available": bool(items),
    }
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "output_root": str(output_root),
        "decision": "paper_trade_ticket_created" if orders else "hold_no_current_candidates",
        "signal_date": signal_date,
        "planned_execution_date": planned_execution_date,
        "runtime_freshness_state": runtime_after.get("freshness_state"),
        "rankings_freshness_state": freshness.get("freshness_state"),
        "ranking_item_count": len(items),
        "selected_count": len(orders),
        "selected_codes": [order["code"] for order in orders if order["paper_status"] == "planned_not_submitted"],
        "paper_trade_only": True,
        "broker_api_called": False,
        "actual_orders_submitted": False,
        "same_day_close_fill": False,
        "no_mutation_pass": no_mutation["no_mutation_pass"],
        "sizing": sizing,
        "not_changed": [
            "MeeMee UI",
            "runtime DB",
            "ranking",
            "publish registry",
            "broker API",
            "replay policy",
        ],
    }

    pd.DataFrame(candidates).to_csv(output_root / "current_candidate_snapshot.csv", index=False)
    pd.DataFrame(orders).to_csv(output_root / "paper_trade_orders.csv", index=False)
    _write_jsonl(output_root / "paper_trade_order_intents.jsonl", orders)
    _write_json(output_root / "paper_trade_summary.json", summary)
    _write_json(output_root / "runtime_state_check.json", runtime_state)
    _write_json(output_root / "selection_contract.json", selection_contract)
    _write_json(output_root / "position_sizing_summary.json", sizing)
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": False,
        "required_artifacts": REQUIRED_ARTIFACTS,
        "required_artifacts_all_present": False,
        "paper_trade_only": True,
        "broker_api_called": False,
        "actual_orders_submitted": False,
        "silent_fallback_used": False,
        "artifacts": {},
    }
    for artifact in REQUIRED_ARTIFACTS:
        path = output_root / artifact
        complete["artifacts"][artifact] = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0}
    complete["required_artifacts_all_present"] = all(
        item["exists"] and item["bytes"] > 0
        for name, item in complete["artifacts"].items()
        if name != "_ARTIFACT_COMPLETE.json"
    )
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
        "exists": (output_root / "_ARTIFACT_COMPLETE.json").exists(),
        "bytes": (output_root / "_ARTIFACT_COMPLETE.json").stat().st_size,
    }
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    complete["required_artifacts_all_present"] = complete["complete"]
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "summary": summary, "runtime_state_check": runtime_state, "orders": orders}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--initial-cash", type=int, default=DEFAULT_INITIAL_CASH)
    parser.add_argument("--max-positions", type=int, default=DEFAULT_MAX_POSITIONS)
    parser.add_argument("--lot-size", type=int, default=DEFAULT_LOT_SIZE)
    parser.add_argument("--one-way-cost-bps", type=int, default=DEFAULT_ONE_WAY_COST_BPS)
    parser.add_argument("--limit", type=int, default=20)
    return parser


def main() -> int:
    result = run_current_selection_paper_trade_v1(**vars(_parser().parse_args()))
    print(json.dumps(_json_ready(result["summary"]), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
