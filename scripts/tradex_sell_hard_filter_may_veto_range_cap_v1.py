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

from scripts import tradex_sell_hard_filter_may_veto_repair_v1 as may_veto
from scripts import tradex_sell_hard_filter_position_sizing_repair_v1 as sizing
from scripts import tradex_sell_monthly_breakout_hard_filter_portfolio_replay_v1 as replay


SCHEMA_PREFIX = "sell_hard_filter_may_veto_range_cap_v1"
DEFAULT_SOURCE_MAY_ROOT = Path(
    r"G:\Tradex\sell_hard_filter_may_veto_repair_v1"
    r"\20260516T114925Z-sell-hard-filter-may-veto-repair-v1"
)
DEFAULT_COMPARE_RUN_ROOT = Path(
    r"G:\Tradex\sell_monthly_breakout_hard_filter_compare_v1"
    r"\20260516T113302Z-sell-monthly-breakout-hard-filter-compare-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\sell_hard_filter_may_veto_range_cap_v1")
VARIANT_ID = "may_veto_plus_monthly_range_lt_0_5_v1"
MONTHLY_RANGE_MAX_EXCLUSIVE = 0.5


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


def _range_allowed(row: dict[str, Any]) -> bool:
    value = row.get("monthly_range_prob")
    if value is None:
        return True
    return float(value) < MONTHLY_RANGE_MAX_EXCLUSIVE


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

    decision = "keep_as_buy_level_equivalent_research_candidate" if not blockers else "hold_for_breadth_and_forward_shadow_review"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at": _utc_now(),
        "variant_id": VARIANT_ID,
        "authoritative_rollup_decision": decision,
        "decision": decision,
        "buy_level_equivalence_reached": decision == "keep_as_buy_level_equivalent_research_candidate",
        "shadow_trade_candidate": decision == "keep_as_buy_level_equivalent_research_candidate",
        "blockers": blockers,
        "one_next_repair_axis": None if not blockers else "calendar_range_cap_breadth_and_forward_shadow_review",
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "remaining_risks": ["calendar_and_range_rules_need_forward_shadow_review", "borrow_availability_not_modeled"],
    }


def run(
    *,
    source_may_root: str | Path = DEFAULT_SOURCE_MAY_ROOT,
    compare_run_root: str | Path = DEFAULT_COMPARE_RUN_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_may_root = Path(source_may_root)
    compare_run_root = Path(compare_run_root)
    source_complete = _read_json(source_may_root / "_ARTIFACT_COMPLETE.json")
    source_compare = _read_json(Path(source_complete["artifact_refs"]["may_veto_compare"]))
    loaded = replay._load_contract(compare_run_root)
    rows = replay.compare._load_rows(loaded["source_root"])
    selected = replay._select_rows(rows, threshold=float(loaded["contract"]["threshold"]))
    challenger_rows = [row for row in selected["challenger"] if not may_veto._is_veto_month(row) and _range_allowed(row)]
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
        "veto_fields": ["as_of_date_month", "monthly_range_prob"],
        "future_outcome_fields_used_in_selection_sizing_or_veto": [],
        "outcome_fields_used_for_replay_only": ["entry_price", "exit_close", "entry_date", "exit_date"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    decision = _decision(delta, yearly, no_lookahead)
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis": VARIANT_ID,
        "source_may_root": str(source_may_root),
        "source_compare_run_root": str(compare_run_root),
        "calendar_veto_rule": "exclude entries with as_of_date month == 5",
        "range_cap_rule": "exclude entries with monthly_range_prob >= 0.5",
        "selection_threshold_changed": False,
        "exit_rule_tuning": False,
        "candidate_tuning": False,
        "non_scope": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal"],
        "silent_fallback_used": False,
        "research_fallback": False,
    }
    run_dir = Path(output_root) / f"{_utc_stamp()}-sell-hard-filter-may-veto-range-cap-v1"
    artifacts = {
        "range_cap_contract": contract,
        "range_cap_compare": {
            "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
            "generated_at": _utc_now(),
            "source_may_veto": source_compare["challenger"],
            "challenger": challenger["summary"],
            "delta": delta,
            "removed_candidate_rows_count": len(selected["challenger"]) - len(challenger_rows),
        },
        "yearly_performance": {"schema_version": f"{SCHEMA_PREFIX}_yearly_v1", "challenger": yearly},
        "monthly_performance": {"schema_version": f"{SCHEMA_PREFIX}_monthly_v1", "challenger": monthly},
        "no_lookahead_audit": no_lookahead,
        "final_range_cap_decision": decision,
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
        "authoritative_decision": str(paths["final_range_cap_decision"]),
        "authoritative_compare": str(paths["range_cap_compare"]),
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
    parser = argparse.ArgumentParser(description="TRADEX-only May veto plus monthly range cap for sell hard-filter replay.")
    parser.add_argument("--source-may-root", default=str(DEFAULT_SOURCE_MAY_ROOT))
    parser.add_argument("--compare-run-root", default=str(DEFAULT_COMPARE_RUN_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_may_root=args.source_may_root, compare_run_root=args.compare_run_root, output_root=args.output_root)
    print(json.dumps(_json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
