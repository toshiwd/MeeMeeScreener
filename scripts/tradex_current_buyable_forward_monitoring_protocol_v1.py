from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "current_buyable_forward_monitoring_protocol_v1"
DEFAULT_FORWARD_ROOT = Path(
    r"G:\Tradex\current_buyable_forward_paper_validation_v1\20260526T010838Z-current-buyable-forward-paper-validation-v1"
)
DEFAULT_INVALIDATION_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_contract_v2_apply\20260526T014806Z-current-buyable-invalidation-contract-v2-apply"
)
DEFAULT_TRACKING_ROOT = Path(
    r"G:\Tradex\current_buyable_invalidation_tracking_status_v1\20260526T013647Z-current-buyable-invalidation-tracking-status-v1"
)
DEFAULT_READINESS_ROOT = Path(
    r"G:\Tradex\current_buyable_operational_readiness_gate_v1\20260526T013407Z-current-buyable-operational-readiness-gate-v1"
)
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_buyable_forward_monitoring_protocol_v1")
REQUIRED_ARTIFACTS = (
    "monitoring_protocol_summary.json",
    "monitoring_status.json",
    "rerun_commands.json",
    "promotion_blockers.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_forward_summary(forward_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    summary_path = forward_root / "forward_paper_validation_summary.json"
    decision_path = forward_root / "research_decision.json"
    if not summary_path.exists():
        raise FileNotFoundError(summary_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    return _load_json(summary_path), _load_json(decision_path)


def confirmed_future_sessions(source_db: Path, codes: list[str], as_of_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        return con.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code,
                   COUNT(*) FILTER (WHERE {expr} > ?) AS future_sessions,
                   MAX({expr}) AS latest_bar
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
            GROUP BY CAST(code AS VARCHAR)
            ORDER BY code
            """,
            [int(as_of_date), codes],
        ).fetchdf()
    finally:
        con.close()


def monitoring_status(forward_summary: dict[str, Any], sessions: pd.DataFrame, tracking_decision: dict[str, Any], readiness_decision: dict[str, Any]) -> dict[str, Any]:
    min_sessions = int(sessions["future_sessions"].min()) if not sessions.empty else 0
    ret5_ready = min_sessions >= 5
    ret20_ready = min_sessions >= 20
    return {
        "axis_id": AXIS_ID,
        "selected_as_of_date": forward_summary.get("selected_as_of_date"),
        "selected_codes": forward_summary.get("selected_codes", []),
        "minimum_future_confirmed_sessions": min_sessions,
        "ret5_ready_to_rerun": ret5_ready,
        "ret20_ready_to_rerun": ret20_ready,
        "latest_bar_by_code": sessions.to_dict(orient="records"),
        "latest_tracking_decision": tracking_decision.get("research_decision"),
        "latest_readiness_decision": readiness_decision.get("research_decision"),
        "validated_buy_count": 0,
    }


def rerun_commands() -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "commands_when_new_confirmed_bars_arrive": [
            "python scripts\\tradex_current_buyable_forward_paper_validation_v1.py",
            "python scripts\\tradex_current_buyable_invalidation_tracking_status_v1.py",
            "python scripts\\tradex_current_buyable_operational_readiness_gate_v1.py --invalidation-root \"G:\\Tradex\\current_buyable_invalidation_contract_v2_apply\\20260526T014806Z-current-buyable-invalidation-contract-v2-apply\"",
        ],
        "do_not_run_if": [
            "candidate list differs from 8086,9831",
            "runtime DB lacks confirmed daily_bars beyond previous latest date",
            "provisional bars are the only new data",
        ],
        "promotion_boundary": {
            "meemee_reflection_allowed": False,
            "runtime_db_write_allowed": False,
            "production_ranking_change_allowed": False,
            "validated_buy_count_must_remain": 0,
        },
    }


def blockers(status: dict[str, Any], readiness_decision: dict[str, Any]) -> dict[str, Any]:
    rows = []
    if not status["ret5_ready_to_rerun"]:
        rows.append({"contract": "forward_ret5_confirmed_outcome", "status": "pending", "reason": "minimum_future_confirmed_sessions_below_5"})
    if not status["ret20_ready_to_rerun"]:
        rows.append({"contract": "forward_ret20_confirmed_outcome", "status": "pending", "reason": "minimum_future_confirmed_sessions_below_20"})
    if readiness_decision.get("production_ready") is not True:
        rows.append({"contract": "operational_readiness_gate", "status": "not_passed", "reason": readiness_decision.get("research_decision")})
    return {"axis_id": AXIS_ID, "blocking_contract_count": len(rows), "blocking_contracts": rows}


def no_lookahead_audit(forward_decision: dict[str, Any], tracking_decision: dict[str, Any]) -> dict[str, Any]:
    forward_ok = forward_decision.get("research_decision") == "forward_validation_pending_more_confirmed_bars"
    tracking_ok = tracking_decision.get("research_decision") in {
        "current_candidates_active_no_invalidation_hit",
        "current_candidate_invalidation_hit_close_or_review",
    }
    return {
        "audit_result": "pass" if forward_ok and tracking_ok else "blocked",
        "no_lookahead_pass": bool(forward_ok and tracking_ok),
        "protocol_uses_status_and_bar_counts_only": True,
        "future_outcomes_used_for_selection": False,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(status: dict[str, Any], block: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["monitoring_inputs_failed_no_lookahead_contract"]
    if status["ret20_ready_to_rerun"]:
        return "monitoring_protocol_ret20_ready_to_rerun_validation", "HOLD_UNDERPOWERED", ["confirmed_future_sessions_reached_ret20_window"]
    if status["ret5_ready_to_rerun"]:
        return "monitoring_protocol_ret5_ready_to_rerun_validation", "HOLD_UNDERPOWERED", ["confirmed_future_sessions_reached_ret5_window"]
    return "monitoring_protocol_wait_for_more_confirmed_bars", "HOLD_UNDERPOWERED", ["confirmed_future_sessions_below_ret5_window"]


def run(
    forward_root: Path = DEFAULT_FORWARD_ROOT,
    invalidation_root: Path = DEFAULT_INVALIDATION_ROOT,
    tracking_root: Path = DEFAULT_TRACKING_ROOT,
    readiness_root: Path = DEFAULT_READINESS_ROOT,
    source_db: Path = DEFAULT_SOURCE_DB,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> Path:
    forward_summary, forward_decision = load_forward_summary(forward_root)
    tracking_decision = _load_json(tracking_root / "research_decision.json")
    readiness_decision = _load_json(readiness_root / "research_decision.json")
    selected_codes = [str(code) for code in forward_summary.get("selected_codes", [])]
    selected_as_of_date = int(forward_summary["selected_as_of_date"])
    sessions = confirmed_future_sessions(source_db, selected_codes, selected_as_of_date)
    status = monitoring_status(forward_summary, sessions, tracking_decision, readiness_decision)
    block = blockers(status, readiness_decision)
    audit = no_lookahead_audit(forward_decision, tracking_decision)
    decision, decision_class, reasons = decide(status, block, audit)

    out = output_root / f"{_now_tag()}-current-buyable-forward-monitoring-protocol-v1"
    out.mkdir(parents=True, exist_ok=True)
    _write_json(
        out / "monitoring_protocol_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "minimum_future_confirmed_sessions": status["minimum_future_confirmed_sessions"],
            "ret5_ready_to_rerun": status["ret5_ready_to_rerun"],
            "ret20_ready_to_rerun": status["ret20_ready_to_rerun"],
            "validated_buy_count": 0,
            "production_ready": False,
        },
    )
    _write_json(out / "monitoring_status.json", status)
    _write_json(out / "rerun_commands.json", rerun_commands())
    _write_json(out / "promotion_blockers.json", block)
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(
        out / "source_coverage.json",
        {
            "axis_id": AXIS_ID,
            "source_db": str(source_db),
            "forward_root": str(forward_root),
            "invalidation_root": str(invalidation_root),
            "tracking_root": str(tracking_root),
            "readiness_root": str(readiness_root),
            "candidate_count": len(selected_codes),
            "latest_bar_by_code": sessions.to_dict(orient="records"),
            "research_fallback_used": False,
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "research_decision": decision,
            "decision_class": decision_class,
            "reason_typed": reasons,
            "research_watch_only": True,
            "production_ready": False,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "publish_allowed": False,
            "validated_buy_count": 0,
            "active_gate_created": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--forward-root", type=Path, default=DEFAULT_FORWARD_ROOT)
    parser.add_argument("--invalidation-root", type=Path, default=DEFAULT_INVALIDATION_ROOT)
    parser.add_argument("--tracking-root", type=Path, default=DEFAULT_TRACKING_ROOT)
    parser.add_argument("--readiness-root", type=Path, default=DEFAULT_READINESS_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.forward_root, args.invalidation_root, args.tracking_root, args.readiness_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
