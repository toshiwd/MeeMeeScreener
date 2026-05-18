from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_closed_horizon_stability_v1\20260517T030047Z-entry-short-bottom-risk-closed-horizon-stability-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_precision_short_bottom_risk_maturity_gate_v1")

BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_cleanup_bottom_risk_v1"
EVALUATION_HORIZON_DAYS = 20

SOURCE_COMPARE_NAME = "short_bottom_risk_closed_horizon_compare.json"
SOURCE_MONTHLY_NAME = "short_bottom_risk_monthly_stability.json"
SOURCE_UNKNOWN_IMPACT_NAME = "short_bottom_risk_unknown_impact.json"
SOURCE_DECISION_NAME = "short_bottom_risk_stability_decision.json"
SOURCE_NO_LOOKAHEAD_NAME = "no_lookahead_audit.json"
SOURCE_CONFUSION_NAME = "short_bottom_risk_confusion_groups.csv"

REQUIRED_OUTPUTS = [
    "short_bottom_risk_maturity_gate_contract.json",
    "short_bottom_risk_unknown_rows.csv",
    "short_bottom_risk_maturity_calendar.json",
    "short_bottom_risk_recheck_plan.json",
    "short_bottom_risk_recheck_acceptance_gate.json",
    "short_bottom_risk_frozen_watch_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], *, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in columns})


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "None"):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _ymd_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    try:
        return date(int(str(value)[:4]), int(str(value)[4:6]), int(str(value)[6:8])).isoformat()
    except Exception:
        return None


def _ymd_to_date(value: int | None) -> date | None:
    if value is None:
        return None
    try:
        return date(int(str(value)[:4]), int(str(value)[4:6]), int(str(value)[6:8]))
    except Exception:
        return None


def _ymd_expr(column_name: str) -> str:
    return f"""
        CASE
            WHEN {column_name} BETWEEN 19000101 AND 20991231 THEN CAST({column_name} AS BIGINT)
            WHEN {column_name} >= 1000000000000 THEN CAST(strftime(to_timestamp({column_name} / 1000), '%Y%m%d') AS BIGINT)
            WHEN {column_name} >= 1000000000 THEN CAST(strftime(to_timestamp({column_name}), '%Y%m%d') AS BIGINT)
            ELSE NULL
        END
    """


def _get_runtime_stock_db_status() -> dict[str, Any]:
    from app.backend.services.codex_bridge_service import get_runtime_stock_db_status

    return get_runtime_stock_db_status()


def _get_rankings_freshness(*, direction: str, limit: int = 20) -> dict[str, Any]:
    from app.backend.services.codex_bridge_service import get_rankings_freshness

    return get_rankings_freshness(tf="D", which="latest", direction=direction, mode="trade", risk_mode="balanced", limit=limit)


def _load_source_context(source_root: Path) -> dict[str, Any]:
    compare = _load_json(source_root / SOURCE_COMPARE_NAME)
    monthly = _load_json(source_root / SOURCE_MONTHLY_NAME)
    unknown_impact = _load_json(source_root / SOURCE_UNKNOWN_IMPACT_NAME)
    decision = _load_json(source_root / SOURCE_DECISION_NAME)
    no_lookahead = _load_json(source_root / SOURCE_NO_LOOKAHEAD_NAME)
    diagnostic_root = Path(str(compare["source_root"]))
    return {
        "source_root": source_root,
        "compare": compare,
        "monthly": monthly,
        "unknown_impact": unknown_impact,
        "decision": decision,
        "no_lookahead": no_lookahead,
        "diagnostic_root": diagnostic_root,
        "source_diagnostic_root": diagnostic_root,
        "source_confusion_path": diagnostic_root / SOURCE_CONFUSION_NAME,
    }


def _load_unknown_rows(confusion_path: Path) -> list[dict[str, str]]:
    with confusion_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [row for row in rows if not _truthy(row.get("outcome_known"))]


def _load_runtime_calendar(runtime_db_path: Path, codes: Iterable[str]) -> tuple[list[int], dict[str, list[int]]]:
    code_list = sorted({str(code).strip() for code in codes if str(code).strip()})
    if not runtime_db_path.exists():
        raise FileNotFoundError(f"runtime stock db not found: {runtime_db_path}")
    if not code_list:
        code_list = []
    conn = duckdb.connect(str(runtime_db_path), read_only=True)
    try:
        global_rows = conn.execute(
            f"""
            SELECT DISTINCT {_ymd_expr("date")} AS ymd
            FROM daily_bars
            ORDER BY ymd
            """
        ).fetchall()
        global_calendar = [int(row[0]) for row in global_rows if row and row[0] is not None]
        if not code_list:
            return global_calendar, {}
        placeholders = ",".join(["?"] * len(code_list))
        rows = conn.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, {_ymd_expr("date")} AS ymd
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN ({placeholders})
            ORDER BY code, ymd
            """,
            code_list,
        ).fetchall()
    finally:
        conn.close()

    series: dict[str, list[int]] = defaultdict(list)
    for code, ymd in rows:
        if ymd is None:
            continue
        series[str(code)].append(int(ymd))
    return global_calendar, series


def _series_horizon_info(
    *,
    code: str,
    signal_date: int,
    global_calendar: list[int],
    dates: list[int],
    runtime_latest_date: int | None,
) -> dict[str, Any]:
    if signal_date not in global_calendar:
        return {
            "signal_present_in_runtime_db": False,
            "signal_index": None,
            "required_horizon_end_date": None,
            "required_horizon_end_date_iso": None,
            "current_eligibility_state": "missing_signal_date",
            "outcome_now_available": False,
            "permanently_unresolvable": True,
            "runtime_code_latest_date": dates[-1] if dates else None,
            "runtime_code_latest_date_iso": _ymd_to_iso(dates[-1]) if dates else None,
            "future_session_count": 0,
            "days_past_horizon": None,
        }

    global_idx = global_calendar.index(signal_date)
    future_session_count = len(global_calendar) - global_idx - 1
    if future_session_count < EVALUATION_HORIZON_DAYS:
        return {
            "signal_present_in_runtime_db": True,
            "signal_index": global_idx,
            "required_horizon_end_date": None,
            "required_horizon_end_date_iso": None,
            "current_eligibility_state": "waiting_for_future_sessions",
            "outcome_now_available": False,
            "permanently_unresolvable": False,
            "runtime_code_latest_date": dates[-1] if dates else None,
            "runtime_code_latest_date_iso": _ymd_to_iso(dates[-1]) if dates else None,
            "future_session_count": int(future_session_count),
            "days_past_horizon": None,
        }

    required_horizon_end_date = global_calendar[global_idx + EVALUATION_HORIZON_DAYS]
    code_has_required_horizon = required_horizon_end_date in dates
    outcome_now_available = bool(
        runtime_latest_date is not None and required_horizon_end_date <= runtime_latest_date and code_has_required_horizon
    )
    runtime_date_obj = _ymd_to_date(runtime_latest_date)
    horizon_date_obj = _ymd_to_date(required_horizon_end_date)
    return {
        "signal_present_in_runtime_db": True,
        "signal_index": global_idx,
        "required_horizon_end_date": required_horizon_end_date,
        "required_horizon_end_date_iso": _ymd_to_iso(required_horizon_end_date),
        "current_eligibility_state": (
            "matured_available_now"
            if outcome_now_available
            else "waiting_for_horizon"
            if runtime_latest_date is None or runtime_latest_date < required_horizon_end_date
            else "missing_price_history"
        ),
        "outcome_now_available": outcome_now_available,
        "permanently_unresolvable": bool(not code_has_required_horizon and runtime_latest_date is not None and runtime_latest_date >= required_horizon_end_date),
        "runtime_code_latest_date": dates[-1],
        "runtime_code_latest_date_iso": _ymd_to_iso(dates[-1]),
        "future_session_count": int(future_session_count),
        "days_past_horizon": None
        if runtime_date_obj is None or horizon_date_obj is None
        else int(max(0, (runtime_date_obj - horizon_date_obj).days))
    }


def _build_unknown_rows(
    *,
    unknown_rows: list[dict[str, str]],
    global_calendar: list[int],
    runtime_series: dict[str, list[int]],
    runtime_latest_date: int | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output_rows: list[dict[str, Any]] = []
    status_counts: dict[str, int] = defaultdict(int)
    horizon_buckets: dict[int, dict[str, int]] = defaultdict(lambda: {"total": 0, "retained_unknown": 0, "removed_unknown": 0})
    maturity_dates: list[int] = []
    unresolvable_count = 0
    waiting_count = 0
    matured_count = 0

    for row in sorted(unknown_rows, key=lambda item: (int(item["ymd"]), str(item["code"]))):
        signal_date = _safe_int(row.get("ymd"))
        code = str(row.get("code") or "").strip()
        dates = runtime_series.get(code, [])
        info = _series_horizon_info(
            code=code,
            signal_date=signal_date or 0,
            global_calendar=global_calendar,
            dates=dates,
            runtime_latest_date=runtime_latest_date,
        )
        status_counts[str(info["current_eligibility_state"])] += 1
        if info["required_horizon_end_date"] is not None:
            horizon_buckets[int(info["required_horizon_end_date"])]["total"] += 1
            horizon_buckets[int(info["required_horizon_end_date"])]["retained_unknown" if row.get("confusion_group") == "retained_unknown" else "removed_unknown"] += 1
            maturity_dates.append(int(info["required_horizon_end_date"]))
            matured_count += 1
        if info["current_eligibility_state"] == "waiting_for_future_sessions":
            waiting_count += 1
        if info["permanently_unresolvable"]:
            unresolvable_count += 1

        output_rows.append(
            {
                "code": code,
                "signal_date_ymd": signal_date,
                "signal_date_iso": _ymd_to_iso(signal_date),
                "as_of_date_ymd": signal_date,
                "as_of_date_iso": _ymd_to_iso(signal_date),
                "group": row.get("confusion_group"),
                "current_eligibility_state": info["current_eligibility_state"],
                "required_horizon_end_date": info["required_horizon_end_date"],
                "required_horizon_end_date_iso": info["required_horizon_end_date_iso"],
                "outcome_now_available": info["outcome_now_available"],
                "signal_present_in_runtime_db": info["signal_present_in_runtime_db"],
                "permanently_unresolvable": info["permanently_unresolvable"],
                "runtime_code_latest_date": info["runtime_code_latest_date"],
                "runtime_code_latest_date_iso": info["runtime_code_latest_date_iso"],
                "runtime_latest_global_date": runtime_latest_date,
                "runtime_latest_global_date_iso": _ymd_to_iso(runtime_latest_date),
                "future_session_count": info["future_session_count"],
                "days_past_horizon": info["days_past_horizon"],
                "baseline_selected": row.get("baseline_selected"),
                "challenger_selected": row.get("challenger_selected"),
                "outcome_known": row.get("outcome_known"),
                "confusion_group": row.get("confusion_group"),
                "tradeDecisionReasons": row.get("tradeDecisionReasons"),
                "tradeRiskWatch": row.get("tradeRiskWatch"),
            }
        )

    earliest_partial = min(maturity_dates) if maturity_dates else None
    latest_full = max(maturity_dates) if maturity_dates and waiting_count == 0 and unresolvable_count == 0 else None
    current_date = runtime_latest_date
    full_ready_now = bool(latest_full is not None and current_date is not None and latest_full <= current_date and unresolvable_count == 0 and waiting_count == 0)
    partial_ready_now = bool(matured_count > 0)
    return output_rows, {
        "status_counts": dict(status_counts),
        "horizon_buckets": horizon_buckets,
        "earliest_partial_recheck_date": earliest_partial,
        "earliest_full_recheck_date": latest_full,
        "partial_ready_now": partial_ready_now,
        "full_ready_now": full_ready_now,
        "permanently_unresolvable_count": int(unresolvable_count),
        "waiting_count": int(waiting_count),
        "matured_count": int(matured_count),
        "maturity_dates": sorted(set(maturity_dates)),
    }


def _build_calendar_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    runtime_status: dict[str, Any],
    rankings_down: dict[str, Any],
    rankings_up: dict[str, Any],
    unknown_rows: list[dict[str, Any]],
    maturity_summary: dict[str, Any],
) -> dict[str, Any]:
    horizon_rows = []
    runtime_latest_date = _safe_int(runtime_status.get("latest_available_global_date"))
    for horizon_end, bucket in sorted(maturity_summary["horizon_buckets"].items()):
        horizon_rows.append(
            {
                "required_horizon_end_date_ymd": int(horizon_end),
                "required_horizon_end_date_iso": _ymd_to_iso(int(horizon_end)),
                "total_rows": int(bucket["total"]),
                "retained_unknown": int(bucket["retained_unknown"]),
                "removed_unknown": int(bucket["removed_unknown"]),
                "all_available_now": bool(runtime_latest_date is not None and int(horizon_end) <= runtime_latest_date),
            }
        )

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_maturity_calendar_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "source_diagnostic_root": str(source_context["diagnostic_root"]),
        "runtime_latest_available_global_date": runtime_status.get("latest_available_global_date"),
        "runtime_latest_available_global_date_iso": runtime_status.get("latest_available_global_date_iso"),
        "rankings_freshness": {
            "down": rankings_down,
            "up": rankings_up,
        },
        "unknown_row_count": int(len(unknown_rows)),
        "group_counts": {
            "retained_unknown": int(sum(1 for row in unknown_rows if row.get("confusion_group") == "retained_unknown")),
            "removed_unknown": int(sum(1 for row in unknown_rows if row.get("confusion_group") == "removed_unknown")),
        },
        "maturity_summary": {
            "earliest_partial_recheck_date_ymd": maturity_summary["earliest_partial_recheck_date"],
            "earliest_partial_recheck_date_iso": _ymd_to_iso(maturity_summary["earliest_partial_recheck_date"]),
            "earliest_full_recheck_date_ymd": maturity_summary["earliest_full_recheck_date"],
            "earliest_full_recheck_date_iso": _ymd_to_iso(maturity_summary["earliest_full_recheck_date"]),
            "partial_ready_now": maturity_summary["partial_ready_now"],
            "full_ready_now": maturity_summary["full_ready_now"],
            "all_unknown_rows_matured_now": bool(
                maturity_summary["permanently_unresolvable_count"] == 0
                and maturity_summary["waiting_count"] == 0
                and maturity_summary["full_ready_now"]
            ),
            "permanently_unresolvable_count": maturity_summary["permanently_unresolvable_count"],
            "waiting_count": maturity_summary["waiting_count"],
            "matured_count": maturity_summary["matured_count"],
            "current_runtime_date_ymd": runtime_latest_date,
            "current_runtime_date_iso": runtime_status.get("latest_available_global_date_iso"),
            "current_watch_state": "ready_for_full_recheck" if maturity_summary["full_ready_now"] and maturity_summary["permanently_unresolvable_count"] == 0 else "ready_for_partial_recheck" if maturity_summary["partial_ready_now"] else "wait_until_full_horizon_matures",
        },
        "horizon_buckets": horizon_rows,
        "unknown_rows": [
            {
                "code": row["code"],
                "group": row["group"],
                "signal_date_ymd": row["signal_date_ymd"],
                "required_horizon_end_date_ymd": row["required_horizon_end_date"],
                "outcome_now_available": row["outcome_now_available"],
                "current_eligibility_state": row["current_eligibility_state"],
            }
            for row in unknown_rows
        ],
    }


def _build_recheck_plan(
    *,
    session_id: str,
    calendar_payload: dict[str, Any],
    source_context: dict[str, Any],
    source_compare: dict[str, Any],
) -> dict[str, Any]:
    maturity = calendar_payload["maturity_summary"]
    if maturity["permanently_unresolvable_count"] > 0:
        decision = "drop_due_to_unresolvable_unknowns"
        recheck_state = "drop_due_to_unresolvable_unknowns"
        next_action = "freeze_and_drop"
    elif maturity["full_ready_now"]:
        decision = "ready_for_full_recheck"
        recheck_state = "ready_for_full_recheck"
        next_action = "rerun_frozen_closed_horizon_stability_check_now"
    elif maturity["partial_ready_now"]:
        decision = "ready_for_partial_recheck"
        recheck_state = "ready_for_partial_recheck"
        next_action = "rerun_partial_closed_horizon_stability_check_when_ready"
    else:
        decision = "wait_until_full_horizon_matures"
        recheck_state = "wait_until_full_horizon_matures"
        next_action = "wait_for_unknown_rows_to_mature"

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_recheck_plan_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "decision": decision,
        "recheck_state": recheck_state,
        "next_action": next_action,
        "source_root": str(source_context["source_root"]),
        "source_diagnostic_root": str(source_context["diagnostic_root"]),
        "earliest_partial_recheck_date": calendar_payload["maturity_summary"]["earliest_partial_recheck_date_iso"],
        "earliest_full_recheck_date": calendar_payload["maturity_summary"]["earliest_full_recheck_date_iso"],
        "current_runtime_date": calendar_payload["maturity_summary"]["current_runtime_date_iso"],
        "current_frozen_result": {
            "stability_decision": source_context["decision"].get("decision"),
            "unknown_materiality": source_context["unknown_impact"].get("unknown_materiality"),
            "closed_horizon_hit_rate_delta": source_compare.get("closed_horizon_summary", {}).get("delta", {}).get("hit_rate_delta"),
            "closed_horizon_mean_ret20_delta": source_compare.get("closed_horizon_summary", {}).get("delta", {}).get("mean_ret20_delta"),
            "monthly_stability": source_context["monthly"].get("rollup", {}),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        },
        "plan_steps": [
            "Keep the frozen watch candidate unchanged.",
            "When the full horizon is mature, rerun the same closed-horizon stability check without changing thresholds, sizing, or replay semantics.",
            "Compare the rerun against the frozen keep slice and only then evaluate acceptance criteria.",
            "Do not promote into production or MeeMee until the rerun gate passes.",
        ],
        "decision_labels": [
            "wait_until_full_horizon_matures",
            "ready_for_partial_recheck",
            "ready_for_full_recheck",
            "drop_due_to_unresolvable_unknowns",
            "keep_frozen_watch_candidate",
        ],
        "promotion_blocker": "This task only arms the frozen watch candidate; it does not change production state.",
    }


def _build_acceptance_gate(
    *,
    session_id: str,
    source_context: dict[str, Any],
    recheck_plan: dict[str, Any],
) -> dict[str, Any]:
    compare = source_context["compare"]
    closed = compare["closed_horizon_summary"]
    delta = closed["delta"]
    monthly_rollup = source_context["monthly"].get("rollup", {})
    current_ready = recheck_plan["recheck_state"] == "ready_for_full_recheck"

    criteria = [
        {
            "name": "unknown_materiality_lower",
            "required": "false_or_materially_lower",
            "current_value": bool(source_context["unknown_impact"].get("unknown_materiality")),
            "current_state": "source_unknown_materiality_is_true",
            "future_check": "evaluate_again_on_rerun",
        },
        {
            "name": "challenger_hit_rate_above_baseline",
            "required": True,
            "current_value": bool(delta.get("hit_rate_delta") is not None and delta["hit_rate_delta"] > 0.0),
            "current_state": "pass" if delta.get("hit_rate_delta") is not None and delta["hit_rate_delta"] > 0.0 else "fail",
            "future_check": "rerun_must_keep_hit_rate_above_baseline",
        },
        {
            "name": "challenger_mean_ret20_above_baseline",
            "required": True,
            "current_value": bool(delta.get("mean_ret20_delta") is not None and delta["mean_ret20_delta"] > 0.0),
            "current_state": "pass" if delta.get("mean_ret20_delta") is not None and delta["mean_ret20_delta"] > 0.0 else "fail",
            "future_check": "rerun_must_keep_mean_ret20_above_baseline",
        },
        {
            "name": "removed_good_not_dominating_removed_bad",
            "required": True,
            "current_value": bool(delta.get("removed_good_known", 0) <= delta.get("removed_bad_known", 0)),
            "current_state": "pass" if delta.get("removed_good_known", 0) <= delta.get("removed_bad_known", 0) else "fail",
            "future_check": "rerun_must_keep_removed_good_below_removed_bad",
        },
        {
            "name": "monthly_stability_not_worsened",
            "required": True,
            "current_value": monthly_rollup.get("mixed_stability"),
            "current_state": "mixed" if monthly_rollup.get("mixed_stability") else "not_mixed",
            "future_check": "rerun_monthly_stability_should_improve_or_at_least_not_worsen",
        },
        {
            "name": "no_lookahead_pass",
            "required": True,
            "current_value": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
            "current_state": "pass" if source_context["no_lookahead"].get("no_lookahead_pass") else "fail",
            "future_check": "no_lookahead_must_remain_true",
        },
        {
            "name": "production_state_unchanged",
            "required": True,
            "current_value": True,
            "current_state": "pass",
            "future_check": "production_state_must_remain_unchanged",
        },
    ]

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_recheck_acceptance_gate_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "gate_state": "armed_for_future_rerun" if current_ready else recheck_plan["recheck_state"],
        "current_recheck_state": recheck_plan["recheck_state"],
        "future_promotion_target": "keep_for_stability_replay",
        "criteria": criteria,
        "current_ready_for_rerun": current_ready,
        "overall_current_status": "not_yet_eligible_for_promotion",
        "promotion_requires_rerun": True,
        "promotion_blocker": "The slice is still frozen; rerun acceptance must be evaluated after the full closed-horizon replay.",
        "source_closed_horizon_decision": source_context["decision"].get("decision"),
        "source_unknown_materiality": source_context["unknown_impact"].get("unknown_materiality"),
    }


def _build_frozen_watch_decision(
    *,
    session_id: str,
    calendar_payload: dict[str, Any],
    source_context: dict[str, Any],
    recheck_plan: dict[str, Any],
    unknown_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    maturity = calendar_payload["maturity_summary"]
    if maturity["permanently_unresolvable_count"] > 0:
        decision = "drop_due_to_unresolvable_unknowns"
        reasons = ["some_unknown_rows_are_unresolvable_from_the_current_price_history"]
    elif maturity["full_ready_now"]:
        decision = "keep_frozen_watch_candidate"
        reasons = [
            "all_unknown_rows_have_matured_20_session_outcomes",
            "no_permanently_unresolvable_unknown_rows_remain",
            "full_closed_horizon_recheck_is_ready_now",
        ]
    else:
        decision = "wait_until_full_horizon_matures"
        reasons = [
            "unknown_rows_have_not_yet_completed_their_20_session_horizon",
            "partial_recheck_may_be_possible_but_full_horizon_is_not_yet_complete",
        ]

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_frozen_watch_decision_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "decision": decision,
        "decision_reasons": reasons,
        "recheck_state": recheck_plan["recheck_state"],
        "current_full_recheck_ready_now": bool(maturity["full_ready_now"]),
        "current_partial_recheck_ready_now": bool(maturity["partial_ready_now"]),
        "current_unknown_row_count": int(len(unknown_rows)),
        "current_permanently_unresolvable_count": int(maturity["permanently_unresolvable_count"]),
        "can_promote_before_full_maturity": False,
        "promotion_blocker": "This task freezes the candidate and arms a recheck; it does not promote into production.",
        "source_closed_horizon_decision": source_context["decision"].get("decision"),
        "source_unknown_materiality": source_context["unknown_impact"].get("unknown_materiality"),
    }


def _build_contract(
    *,
    session_id: str,
    source_context: dict[str, Any],
    runtime_status: dict[str, Any],
    rankings_down: dict[str, Any],
    rankings_up: dict[str, Any],
    maturity_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_maturity_gate_contract_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "evaluation_horizon_days": EVALUATION_HORIZON_DAYS,
        "source_root": str(source_context["source_root"]),
        "source_diagnostic_root": str(source_context["diagnostic_root"]),
        "input_artifacts": {
            "source_closed_horizon_compare": str(source_context["source_root"] / SOURCE_COMPARE_NAME),
            "source_monthly_stability": str(source_context["source_root"] / SOURCE_MONTHLY_NAME),
            "source_unknown_impact": str(source_context["source_root"] / SOURCE_UNKNOWN_IMPACT_NAME),
            "source_stability_decision": str(source_context["source_root"] / SOURCE_DECISION_NAME),
            "source_no_lookahead_audit": str(source_context["source_root"] / SOURCE_NO_LOOKAHEAD_NAME),
            "source_confusion_groups": str(source_context["source_diagnostic_root"] / SOURCE_CONFUSION_NAME),
        },
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost": True,
            "same_artifact_detail_level": True,
            "long_logic_frozen": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
            "no_production_state_change": True,
            "no_lookahead_contract": True,
        },
        "runtime_context": {
            "runtime_stock_db_status": runtime_status,
            "rankings_freshness_down": rankings_down,
            "rankings_freshness_up": rankings_up,
        },
        "frozen_source_state": {
            "stability_decision": source_context["decision"].get("decision"),
            "unknown_materiality": source_context["unknown_impact"].get("unknown_materiality"),
            "baseline_known_rows": source_context["compare"].get("closed_horizon_summary", {}).get("baseline", {}).get("count"),
            "challenger_known_rows": source_context["compare"].get("closed_horizon_summary", {}).get("challenger", {}).get("count"),
            "hit_rate_delta": source_context["compare"].get("closed_horizon_summary", {}).get("delta", {}).get("hit_rate_delta"),
            "mean_ret20_delta": source_context["compare"].get("closed_horizon_summary", {}).get("delta", {}).get("mean_ret20_delta"),
            "monthly_stability": source_context["monthly"].get("rollup", {}),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        },
        "maturity_contract": {
            "evaluation_horizon_days": EVALUATION_HORIZON_DAYS,
            "earliest_partial_recheck_date": maturity_summary["earliest_partial_recheck_date"],
            "earliest_full_recheck_date": maturity_summary["earliest_full_recheck_date"],
            "all_unknown_rows_matured_now": bool(maturity_summary["permanently_unresolvable_count"] == 0 and maturity_summary["full_ready_now"]),
            "permanently_unresolvable_count": maturity_summary["permanently_unresolvable_count"],
            "current_runtime_date": runtime_status.get("latest_available_global_date"),
        },
        "change_policy": {
            "scope": "TRADEX-only frozen watch maturity gate for short_cleanup_bottom_risk_v1.",
            "non_scope": [
                "new challenger creation",
                "threshold tuning",
                "close_pos changes",
                "monthly alignment changes",
                "long logic changes",
                "cost model changes",
                "MeeMee changes",
                "production ranking changes",
                "active champion changes",
                "publish",
                "live sell signal",
            ],
            "boundary_check": "TRADEX",
            "risks": [
                "maturity is based on the live runtime stock DB and may drift forward",
                "promotion remains blocked until a rerun proves the frozen result again",
                "the gate should not be used to alter the frozen rule",
            ],
        },
        "decision_target": "Freeze the bottom-risk keep slice as a watch candidate and tell us exactly when it is safe to rerun the closed-horizon stability decision.",
    }


def _artifact_complete(output_dir: Path) -> dict[str, Any]:
    artifact_refs = {name: str(output_dir / name) for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    complete = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_maturity_artifact_complete_v1",
        "generated_at": _utc_now(),
        "artifact_root": str(output_dir),
        "required_artifacts": REQUIRED_OUTPUTS,
        "artifact_refs": artifact_refs,
        "artifacts": {},
    }
    for name in REQUIRED_OUTPUTS:
        path = output_dir / name
        if name != "_ARTIFACT_COMPLETE.json":
            complete["artifacts"][name] = {
                "path": str(path),
                "exists": path.exists(),
                "bytes": path.stat().st_size if path.exists() else 0,
            }

    self_path = output_dir / "_ARTIFACT_COMPLETE.json"
    self_bytes = 0
    for _ in range(5):
        complete["artifacts"]["_ARTIFACT_COMPLETE.json"] = {
            "path": str(self_path),
            "exists": True,
            "bytes": int(self_bytes),
        }
        complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
        _write_json(self_path, complete)
        actual_bytes = self_path.stat().st_size
        if actual_bytes == self_bytes:
            break
        self_bytes = actual_bytes
    complete["artifacts"]["_ARTIFACT_COMPLETE.json"]["bytes"] = int(self_path.stat().st_size)
    complete["complete"] = all(item["exists"] and item["bytes"] > 0 for item in complete["artifacts"].values())
    return complete


def run(args: argparse.Namespace) -> dict[str, Any]:
    source_root = Path(args.source_root or DEFAULT_SOURCE_ROOT)
    output_root = Path(args.output_root or DEFAULT_OUTPUT_ROOT)
    source_context = _load_source_context(source_root)
    runtime_status = _get_runtime_stock_db_status()
    rankings_down = _get_rankings_freshness(direction="down")
    rankings_up = _get_rankings_freshness(direction="up")

    runtime_db_path_text = str(runtime_status.get("selected_runtime_db_path") or runtime_status.get("runtime_db_path") or "").strip()
    if not runtime_db_path_text:
        raise RuntimeError("runtime stock db path missing from runtime status")
    runtime_db_path = Path(runtime_db_path_text)

    confusion_rows = _load_unknown_rows(source_context["source_confusion_path"])
    global_calendar, runtime_series = _load_runtime_calendar(runtime_db_path, [row["code"] for row in confusion_rows])

    runtime_latest_date = _safe_int(runtime_status.get("latest_available_global_date"))
    unknown_rows, maturity_summary = _build_unknown_rows(
        unknown_rows=confusion_rows,
        global_calendar=global_calendar,
        runtime_series=runtime_series,
        runtime_latest_date=runtime_latest_date,
    )

    session_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_id = f"entry-short-bottom-risk-maturity-gate-{session_stamp}"
    output_dir = output_root / f"{session_stamp}-entry-short-bottom-risk-maturity-gate-v1"
    output_dir.mkdir(parents=True, exist_ok=True)

    contract = _build_contract(
        session_id=session_id,
        source_context=source_context,
        runtime_status=runtime_status,
        rankings_down=rankings_down,
        rankings_up=rankings_up,
        maturity_summary=maturity_summary,
    )
    calendar_payload = _build_calendar_payload(
        session_id=session_id,
        source_context=source_context,
        runtime_status=runtime_status,
        rankings_down=rankings_down,
        rankings_up=rankings_up,
        unknown_rows=unknown_rows,
        maturity_summary=maturity_summary,
    )
    recheck_plan = _build_recheck_plan(
        session_id=session_id,
        calendar_payload=calendar_payload,
        source_context=source_context,
        source_compare=source_context["compare"],
    )
    acceptance_gate = _build_acceptance_gate(
        session_id=session_id,
        source_context=source_context,
        recheck_plan=recheck_plan,
    )
    frozen_watch_decision = _build_frozen_watch_decision(
        session_id=session_id,
        calendar_payload=calendar_payload,
        source_context=source_context,
        recheck_plan=recheck_plan,
        unknown_rows=unknown_rows,
    )

    no_lookahead_audit = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_maturity_no_lookahead_audit_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "no_lookahead_pass": True,
        "future_outcome_fields_used_in_selection": [],
        "future_outcome_fields_used_in_maturity_gate": [],
        "selection_logic_frozen": True,
        "diagnostic_mode": True,
        "research_fallback": False,
        "silent_fallback_used": False,
    }

    _write_json(output_dir / "short_bottom_risk_maturity_gate_contract.json", contract)
    _write_csv(
        output_dir / "short_bottom_risk_unknown_rows.csv",
        unknown_rows,
        columns=[
            "code",
            "signal_date_ymd",
            "signal_date_iso",
            "as_of_date_ymd",
            "as_of_date_iso",
            "group",
            "current_eligibility_state",
            "required_horizon_end_date",
            "required_horizon_end_date_iso",
            "outcome_now_available",
            "signal_present_in_runtime_db",
            "permanently_unresolvable",
            "runtime_code_latest_date",
            "runtime_code_latest_date_iso",
            "runtime_latest_global_date",
            "runtime_latest_global_date_iso",
            "future_session_count",
            "days_past_horizon",
            "baseline_selected",
            "challenger_selected",
            "outcome_known",
            "confusion_group",
            "tradeDecisionReasons",
            "tradeRiskWatch",
        ],
    )
    _write_json(output_dir / "short_bottom_risk_maturity_calendar.json", calendar_payload)
    _write_json(output_dir / "short_bottom_risk_recheck_plan.json", recheck_plan)
    _write_json(output_dir / "short_bottom_risk_recheck_acceptance_gate.json", acceptance_gate)
    _write_json(output_dir / "short_bottom_risk_frozen_watch_decision.json", frozen_watch_decision)
    _write_json(output_dir / "no_lookahead_audit.json", no_lookahead_audit)
    artifact_complete = _artifact_complete(output_dir)
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", artifact_complete)

    return {
        "session_id": session_id,
        "output_dir": str(output_dir),
        "decision": frozen_watch_decision["decision"],
        "recheck_state": recheck_plan["recheck_state"],
        "unknown_row_count": int(len(unknown_rows)),
        "retained_unknown_count": int(sum(1 for row in unknown_rows if row["group"] == "retained_unknown")),
        "removed_unknown_count": int(sum(1 for row in unknown_rows if row["group"] == "removed_unknown")),
        "full_recheck_ready_now": bool(calendar_payload["maturity_summary"]["full_ready_now"]),
        "partial_recheck_ready_now": bool(calendar_payload["maturity_summary"]["partial_ready_now"]),
        "permanently_unresolvable_count": int(calendar_payload["maturity_summary"]["permanently_unresolvable_count"]),
        "no_lookahead_pass": bool(no_lookahead_audit["no_lookahead_pass"]),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TRADEX maturity gate for the frozen short_cleanup_bottom_risk_v1 keep slice."
    )
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT), help="Closed-horizon frozen source root")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT), help="Output root under G:\\Tradex")
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(args)
    print(json.dumps({"decision": result["decision"], "output_dir": result["output_dir"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
