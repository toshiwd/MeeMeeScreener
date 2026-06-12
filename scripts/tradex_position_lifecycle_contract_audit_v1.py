from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "position_lifecycle_contract_audit_v1"
DEFAULT_OUT_ROOT = Path("G:/Tradex/position_lifecycle_contract_audit_v1")
DEFAULT_TOUCH_PRETEST_DECISION = Path("G:/Tradex/ma_touch_position_lifecycle_exit_pretest_v1/20260603T160348Z-ma-touch-position-lifecycle-exit-pretest-v1/research_decision.json")
DEFAULT_TOUCH_PRETEST_AUDIT = Path("G:/Tradex/ma_touch_position_lifecycle_exit_pretest_v1/20260603T160348Z-ma-touch-position-lifecycle-exit-pretest-v1/input_audit.json")
DEFAULT_FEATURE_AUDIT = Path("G:/Tradex/ma_phase_feature_base_v1/20260603T121215Z-ma-phase-feature-base-v1/input_audit.json")
REQUIRED_ARTIFACTS = (
    "input_audit.json",
    "lifecycle_source_inventory.csv",
    "candidate_source_schema_audit.csv",
    "canonical_position_lifecycle_contract.json",
    "gap_analysis.json",
    "next_implementation_plan.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


CANDIDATE_SOURCES = [
    {
        "source_name": "position_lifecycle_state_machine_v1",
        "artifact_path": Path("G:/Tradex/position_lifecycle_state_machine_v1/20260602T111028Z-position_lifecycle_state_machine_v1/position_lifecycle_replay.parquet"),
        "decision_path": Path("G:/Tradex/position_lifecycle_state_machine_v1/20260602T111028Z-position_lifecycle_state_machine_v1/research_decision.json"),
        "kind": "review_state_daily_surface",
    },
    {
        "source_name": "position_management_policy_pretest_v1_latest",
        "artifact_path": Path("G:/Tradex/position_management_policy_pretest_v1/20260526T022045Z-position-management-policy-pretest-v1/position_policy_daily_ledger.csv"),
        "decision_path": Path("G:/Tradex/position_management_policy_pretest_v1/20260526T022045Z-position-management-policy-pretest-v1/research_decision.json"),
        "contract_path": Path("G:/Tradex/position_management_policy_pretest_v1/20260526T022045Z-position-management-policy-pretest-v1/entry_source_contract.json"),
        "kind": "research_policy_daily_ledger",
    },
    {
        "source_name": "position_management_policy_pretest_v1_prior",
        "artifact_path": Path("G:/Tradex/position_management_policy_pretest_v1/20260526T021953Z-position-management-policy-pretest-v1/position_policy_daily_ledger.csv"),
        "decision_path": Path("G:/Tradex/position_management_policy_pretest_v1/20260526T021953Z-position-management-policy-pretest-v1/research_decision.json"),
        "contract_path": Path("G:/Tradex/position_management_policy_pretest_v1/20260526T021953Z-position-management-policy-pretest-v1/entry_source_contract.json"),
        "kind": "research_policy_daily_ledger",
    },
    {
        "source_name": "actual_trade_counterfactual_rule_audit_v1",
        "artifact_path": Path("G:/Tradex/actual_trade_counterfactual_rule_audit_v1/20260512T012005Z-actual_trade_entry_ma20_regime_filter_v1/baseline_trade_summary.json"),
        "decision_path": Path("G:/Tradex/actual_trade_counterfactual_rule_audit_v1/20260512T012005Z-actual_trade_entry_ma20_regime_filter_v1/counterfactual_decision.json"),
        "kind": "actual_trade_summary_not_daily_lifecycle",
    },
]


REQUIRED_CONTRACT_FIELDS = {
    "position_id",
    "code",
    "bar_date",
    "entry_date",
    "entry_price",
    "close_price",
    "position_side",
    "is_open_at_bar",
    "holding_age_bars",
    "unrealized_return_pct",
    "source_entry_event_id",
}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _read_columns(path: Path) -> tuple[int | None, list[str], str | None]:
    if not path.exists():
        return None, [], "missing_artifact"
    try:
        if path.suffix.lower() == ".parquet":
            frame = pd.read_parquet(path)
        elif path.suffix.lower() == ".csv":
            frame = pd.read_csv(path)
        elif path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            keys = list(data.keys()) if isinstance(data, dict) else []
            return 1, keys, None
        else:
            return None, [], f"unsupported_suffix:{path.suffix}"
    except Exception as exc:
        return None, [], f"read_error:{type(exc).__name__}:{exc}"
    return int(len(frame)), list(frame.columns), None


def _audit_source(source: dict[str, Any]) -> dict[str, Any]:
    rows, cols, error = _read_columns(source["artifact_path"])
    cols_set = set(cols)
    missing = sorted(REQUIRED_CONTRACT_FIELDS - cols_set)
    present = sorted(REQUIRED_CONTRACT_FIELDS & cols_set)
    has_entry_price = "entry_price" in cols_set or "unrealized_pnl_per_entry_unit" in cols_set
    has_bar_grain = bool({"bar_date", "as_of_date", "ymd"}.intersection(cols_set))
    has_position_identity = bool({"position_id", "event_id"}.intersection(cols_set))
    has_open_state = bool({"units", "is_open_at_bar", "action"}.intersection(cols_set))
    event_grain_status = "candidate"
    if error:
        event_grain_status = "unavailable"
    elif not has_bar_grain:
        event_grain_status = "not_daily_bar_grain"
    elif not has_position_identity:
        event_grain_status = "missing_position_identity"
    elif not has_entry_price:
        event_grain_status = "missing_entry_price"
    elif not has_open_state:
        event_grain_status = "missing_open_position_state"
    contract = {}
    contract_path = source.get("contract_path")
    if contract_path and Path(contract_path).exists():
        try:
            contract = json.loads(Path(contract_path).read_text(encoding="utf-8"))
        except Exception as exc:
            contract = {"read_error": f"{type(exc).__name__}:{exc}"}
    source_type = contract.get("source_type") if isinstance(contract, dict) else None
    generalizable_status = "unknown"
    if source_type == "historical_research_candidate_events":
        generalizable_status = "research_event_specific_not_canonical"
    elif source["kind"] == "review_state_daily_surface":
        generalizable_status = "review_state_surface_not_position_ledger"
    elif source["kind"].startswith("actual_trade"):
        generalizable_status = "actual_trade_summary_not_touch_event_grain"
    return {
        "source_name": source["source_name"],
        "kind": source["kind"],
        "artifact_path": str(source["artifact_path"]),
        "decision_path": str(source.get("decision_path")),
        "contract_path": str(source.get("contract_path")) if source.get("contract_path") else None,
        "artifact_exists": source["artifact_path"].exists(),
        "row_count": rows,
        "column_count": len(cols),
        "read_error": error,
        "present_required_fields": present,
        "missing_required_fields": missing,
        "has_bar_grain": has_bar_grain,
        "has_position_identity": has_position_identity,
        "has_entry_price_or_pnl_proxy": has_entry_price,
        "has_open_position_state": has_open_state,
        "event_grain_status": event_grain_status,
        "entry_source": contract.get("entry_source") if isinstance(contract, dict) else None,
        "source_type": source_type,
        "generalizable_status": generalizable_status,
        "is_authoritative_for_ma_touch_exit": False,
    }


def _contract() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "contract_name": "canonical_tradex_position_lifecycle_ledger_v1",
        "purpose": "point-in-time daily open-position state that can be joined to MA touch events by code and bar_date before any exit-policy validation",
        "required_grain": "one row per open position per confirmed daily bar",
        "required_keys": ["position_id", "code", "bar_date"],
        "required_fields": {
            "position_id": "stable unique id for one simulated or actual long position lifecycle",
            "code": "stock code as string",
            "bar_date": "confirmed daily bar date as yyyymmdd int",
            "entry_date": "position entry date as yyyymmdd int",
            "entry_price": "point-in-time entry execution price used for unrealized return",
            "close_price": "confirmed close at bar_date",
            "position_side": "long or short; MA touch exit pretest requires long",
            "is_open_at_bar": "true when position is open at bar_date before any tested exit action",
            "holding_age_bars": "trading bars since entry at bar_date",
            "unrealized_return_pct": "close_price / entry_price - 1 for long, percent units",
            "source_entry_event_id": "id/path of the candidate or actual trade event that opened the position",
        },
        "required_audit_fields": {
            "confirmed_bars_only": True,
            "point_in_time_entry": True,
            "no_future_exit_labels_as_input": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
        },
        "non_goals": [
            "no sell rule implementation",
            "no synthetic lifecycle promotion",
            "no MeeMee display contract",
            "no ranking or candidate generation mutation",
        ],
    }


def _gap_analysis(audits: list[dict[str, Any]]) -> dict[str, Any]:
    authoritative = [row for row in audits if row["is_authoritative_for_ma_touch_exit"]]
    blockers = []
    for row in audits:
        blockers.append(
            {
                "source_name": row["source_name"],
                "blocker": row["event_grain_status"],
                "generalizable_status": row["generalizable_status"],
                "missing_required_fields": row["missing_required_fields"],
            }
        )
    return {
        "axis_id": AXIS_ID,
        "authoritative_lifecycle_source_found": bool(authoritative),
        "authoritative_sources": authoritative,
        "primary_blocker": "no_canonical_position_lifecycle_ledger_at_ma_touch_event_grain",
        "blockers_by_source": blockers,
        "blocked_prior_axis": str(DEFAULT_TOUCH_PRETEST_DECISION),
        "prior_axis_decision": "hold_due_to_synthetic_lifecycle",
        "synthetic_lifecycle_promotion_allowed": False,
    }


def _next_plan() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "recommended_next_script": "scripts/tradex_canonical_position_lifecycle_ledger_v1.py",
        "recommended_artifact_root": "G:/Tradex/canonical_position_lifecycle_ledger_v1/<timestamp>/",
        "implementation_steps": [
            "choose one frozen entry-event source or actual-trade source and record it in entry_source_contract.json",
            "build one row per open position per confirmed daily bar with entry_price, holding_age_bars, unrealized_return_pct, and source_entry_event_id",
            "write lifecycle_ledger.parquet plus lifecycle_contract.json and no_lookahead_audit.json",
            "rerun ma_touch_position_lifecycle_exit_pretest_v1 against the canonical ledger without synthetic fallback",
        ],
        "required_outputs": [
            "input_audit.json",
            "lifecycle_contract.json",
            "lifecycle_ledger.parquet",
            "source_entry_events.csv",
            "no_lookahead_audit.json",
            "research_decision.json",
            "_ARTIFACT_COMPLETE.json",
        ],
        "do_not_do": [
            "do not infer entries from MA20 runs for sell-rule validation",
            "do not use future reaction labels as lifecycle inputs",
            "do not mutate runtime DB, ranking, candidate generation, publish, or MeeMee",
        ],
    }


def _decision(audits: list[dict[str, Any]]) -> dict[str, Any]:
    found = any(row["is_authoritative_for_ma_touch_exit"] for row in audits)
    decision = "hold"
    reason = "canonical_position_lifecycle_contract_missing"
    next_decision = "build_canonical_lifecycle_ledger_before_exit_policy_retest"
    if found:
        decision = "keep_for_exit_policy_retest_next"
        reason = "canonical_position_lifecycle_source_found"
        next_decision = "rerun_ma_touch_position_lifecycle_exit_pretest_without_synthetic_fallback"
    return {
        "axis_id": AXIS_ID,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "next_required_decision": next_decision,
        "sell_rule_promotion_allowed": False,
        "synthetic_lifecycle_promotion_allowed": False,
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB write",
            "no ranking change",
            "no publish",
            "no candidate generation change",
            "no live sell rule implementation",
            "no exit threshold tuning",
            "no new MA signal research",
            "no synthetic lifecycle promotion",
        ],
    }


def run(args: argparse.Namespace) -> Path:
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    feature_audit = json.loads(args.feature_audit.read_text(encoding="utf-8"))
    prior_audit = json.loads(args.touch_pretest_audit.read_text(encoding="utf-8"))
    prior_decision = json.loads(args.touch_pretest_decision.read_text(encoding="utf-8"))
    audits = [_audit_source(source) for source in CANDIDATE_SOURCES]
    inventory = pd.DataFrame(audits)
    schema_audit = inventory[
        [
            "source_name",
            "kind",
            "artifact_exists",
            "row_count",
            "event_grain_status",
            "generalizable_status",
            "present_required_fields",
            "missing_required_fields",
            "is_authoritative_for_ma_touch_exit",
        ]
    ].copy()
    input_audit = {
        "axis_id": AXIS_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "feature_audit": str(args.feature_audit),
        "blocked_touch_pretest_decision": str(args.touch_pretest_decision),
        "blocked_touch_pretest_audit": str(args.touch_pretest_audit),
        "blocked_touch_pretest_authoritative_rollup_decision": prior_decision.get("authoritative_rollup_decision"),
        "blocked_touch_pretest_research_fallback_used": prior_decision.get("research_fallback_used") or prior_audit.get("research_fallback_used"),
        "candidate_source_count": len(audits),
        "authoritative_lifecycle_source_found": False,
        "confirmed_bars_only_inherited": bool(feature_audit.get("confirmed_bars_only")),
        "runtime_db_write": False,
        "meemee_reflection": False,
        "ranking_change": False,
        "publish": False,
        "silent_fallback_used": False,
    }
    _write_json(out_dir / "input_audit.json", input_audit)
    inventory.to_csv(out_dir / "lifecycle_source_inventory.csv", index=False, encoding="utf-8")
    schema_audit.to_csv(out_dir / "candidate_source_schema_audit.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "canonical_position_lifecycle_contract.json", _contract())
    _write_json(out_dir / "gap_analysis.json", _gap_analysis(audits))
    _write_json(out_dir / "next_implementation_plan.json", _next_plan())
    _write_json(out_dir / "research_decision.json", _decision(audits))
    missing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and not (out_dir / name).exists()]
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete" if not missing else "incomplete",
            "missing_artifacts": missing,
            "authoritative_result": str(out_dir / "research_decision.json"),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    if missing:
        raise RuntimeError(f"missing artifacts: {missing}")
    return out_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX read-only position lifecycle contract audit.")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--touch-pretest-decision", type=Path, default=DEFAULT_TOUCH_PRETEST_DECISION)
    parser.add_argument("--touch-pretest-audit", type=Path, default=DEFAULT_TOUCH_PRETEST_AUDIT)
    parser.add_argument("--feature-audit", type=Path, default=DEFAULT_FEATURE_AUDIT)
    return parser.parse_args()


def main() -> None:
    print(run(parse_args()))


if __name__ == "__main__":
    main()
