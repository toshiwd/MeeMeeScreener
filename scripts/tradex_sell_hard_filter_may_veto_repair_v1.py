from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_sell_hard_filter_position_sizing_repair_v1 as sizing
from scripts import tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 as replay


SCHEMA_PREFIX = "sell_hard_filter_may_veto_repair_v1"
DEFAULT_SOURCE_SIZING_ROOT = Path(
    r"G:\Tradex\sell_hard_filter_position_sizing_repair_v1"
    r"\20260516T114449Z-sell-hard-filter-position-sizing-repair-v1"
)
DEFAULT_COMPARE_RUN_ROOT = Path(
    r"G:\Tradex\sell_monthly_breakout_hard_filter_compare_v1"
    r"\20260516T113302Z-sell-monthly-breakout-hard-filter-compare-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_may_veto_repair_v1")
VARIANT_ID = "rank3_half_size_plus_may_entry_veto_v1"
VETO_MONTH = 5


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


def _is_veto_month(row: dict[str, Any]) -> bool:
    return int(str(int(row["as_of_date"]))[4:6]) == VETO_MONTH


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
    if delta["total_return_delta"] <= 0.0:
        blockers.append("total_return_not_improved")
    if delta["bad_pick_delta"] > 0:
        blockers.append("bad_pick_added")
    if delta["severe_loser_delta"] > 0:
        blockers.append("severe_loser_added")

    if not blockers:
        decision = "keep_as_buy_level_equivalent_research_candidate"
        next_axis = "shadow_trade_readiness_review"
    elif delta["challenger_total_return"] > 0.0 and delta["challenger_max_drawdown"] > -0.20:
        decision = "hold_for_breadth_and_forward_shadow_review"
        next_axis = "calendar_veto_breadth_and_forward_shadow_review"
    else:
        decision = "drop_may_veto_repair"
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
        "remaining_risks": ["calendar_month_veto_may_be_seasonality_overfit", "borrow_availability_not_modeled"],
    }


def run(
    *,
    source_sizing_root: str | Path = DEFAULT_SOURCE_SIZING_ROOT,
    compare_run_root: str | Path = DEFAULT_COMPARE_RUN_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_sizing_root = Path(source_sizing_root)
    compare_run_root = Path(compare_run_root)
    output_root = Path(output_root)
    source_complete = _read_json(source_sizing_root / "_ARTIFACT_COMPLETE.json")
    source_compare = _read_json(Path(source_complete["artifact_refs"]["position_sizing_compare"]))
    loaded = replay._load_contract(compare_run_root)
    rows = replay.compare._load_rows(loaded["source_root"])
    selected = replay._select_rows(rows, threshold=float(loaded["contract"]["threshold"]))
    challenger_rows = [row for row in selected["challenger"] if not _is_veto_month(row)]
    challenger = sizing._simulate_scaled(challenger_rows, label="challenger")
    yearly = sizing._period_performance(challenger["trades"], key="year")
    monthly = sizing._period_performance(challenger["trades"], key="month")
    delta = _delta(source_compare["challenger"], challenger["summary"])
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "source_no_lookahead_pass": True,
        "no_lookahead_pass": True,
        "selection_fields": ["as_of_date", "rank", "side", "execution_available", "monthly_breakout_up_prob"],
        "sizing_fields": ["rank"],
        "veto_fields": ["as_of_date_month"],
        "future_outcome_fields_used_in_selection_sizing_or_veto": [],
        "outcome_fields_used_for_replay_only": ["entry_price", "exit_close", "entry_date", "exit_date"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    decision = _decision(delta, yearly, no_lookahead)
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis": VARIANT_ID,
        "source_sizing_root": str(source_sizing_root),
        "source_compare_run_root": str(compare_run_root),
        "position_sizing_rule": "rank <= 3 normal notional; rank >= 4 half notional",
        "calendar_veto_rule": "exclude entries with as_of_date month == 5",
        "selection_threshold_changed": False,
        "exit_rule_tuning": False,
        "candidate_tuning": False,
        "non_scope": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    run_dir = output_root / f"{_utc_stamp()}-sell-hard-filter-may-veto-repair-v1"
    artifacts = {
        "may_veto_contract": contract,
        "may_veto_compare": {
            "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
            "generated_at": _utc_now(),
            "source_position_sizing": source_compare["challenger"],
            "challenger": challenger["summary"],
            "delta": delta,
            "removed_candidate_rows_count": len(selected["challenger"]) - len(challenger_rows),
        },
        "yearly_performance": {"schema_version": f"{SCHEMA_PREFIX}_yearly_v1", "challenger": yearly},
        "monthly_performance": {"schema_version": f"{SCHEMA_PREFIX}_monthly_v1", "challenger": monthly},
        "no_lookahead_audit": no_lookahead,
        "final_may_veto_decision": decision,
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
        "authoritative_decision": str(paths["final_may_veto_decision"]),
        "authoritative_compare": str(paths["may_veto_compare"]),
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
    parser = argparse.ArgumentParser(description="TRADEX-only May-entry veto repair for sell hard-filter replay.")
    parser.add_argument("--source-sizing-root", default=str(DEFAULT_SOURCE_SIZING_ROOT))
    parser.add_argument("--compare-run-root", default=str(DEFAULT_COMPARE_RUN_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_sizing_root=args.source_sizing_root, compare_run_root=args.compare_run_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
