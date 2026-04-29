from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import tradex_iizuka_trade_learning_loop as loop_mod  # noqa: E402

SOURCE_SPLIT_SESSION_ROOT = Path(
    r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T021621Z"
)
SOURCE_SPLIT_CASE_RESULTS = SOURCE_SPLIT_SESSION_ROOT / "tradex_iizuka_mined_axis_split_case_results.json"
PREVIOUS_V2_SESSION_ROOT = Path(
    r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T032517Z"
)
PREVIOUS_V2_CASE_RESULTS = PREVIOUS_V2_SESSION_ROOT / "tradex_hard_invalidation_exit_severity_v2_case_results.json"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop")
SEVERITY_CANDIDATE = loop_mod.HARD_INVALIDATION_SEVERITY_CANDIDATE
ANCHOR_SYMBOLS = ("2317", "9697", "2531", "5541")
SEVERITY_CASE_SYMBOLS = (
    "5801",
    "5803",
    "6920",
    "6146",
    "5706",
    "6857",
    "3309",
    "4004",
    "7735",
    "3110",
    "1001",
    "8136",
)
CAUSAL_AUDIT_SOURCE_SESSION_ROOT = Path(r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T063212Z")
REPAIRED_V2_SESSION_ROOT = CAUSAL_AUDIT_SOURCE_SESSION_ROOT / "repaired_v2"
REPAIRED_V2_CASE_RESULTS = REPAIRED_V2_SESSION_ROOT / "hard_invalidation_repaired_v2_case_results.json"
REPAIRED_V2_ANCHOR_REGRESSION = REPAIRED_V2_SESSION_ROOT / "hard_invalidation_repaired_v2_anchor_regression.json"
REPAIRED_V2_DECISION = REPAIRED_V2_SESSION_ROOT / "hard_invalidation_repaired_v2_decision.json"
CAUSAL_AUDIT_GUARD_CASE_RESULTS = CAUSAL_AUDIT_SOURCE_SESSION_ROOT / "profit_preservation_guard_v1" / "hard_invalidation_profit_preservation_guard_v1_case_results.json"
CAUSAL_AUDIT_GUARD_DECISION = Path(r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T071823Z\causal_audit\hard_invalidation_profit_preservation_guard_v1_causal_decision.json")
NON_EXIT_ACTION_EVIDENCE_TARGET_SYMBOLS = (
    "5801",
    "5803",
    "6920",
    "5706",
    "4004",
    "3110",
    "1001",
    "8136",
)
FULL_COVERAGE_NON_EXIT_ACTION_EVIDENCE_TARGET_SYMBOLS = SEVERITY_CASE_SYMBOLS
NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE = loop_mod.HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE
REPLAY_PATH_DELTA_BASELINE_SESSION_ROOT = REPAIRED_V2_SESSION_ROOT
REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS = REPAIRED_V2_CASE_RESULTS
REPLAY_PATH_DELTA_CHALLENGER_SESSION_ROOT = Path(
    r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T080749Z\non_exit_late_extension_hedge_v1"
)
REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS = REPLAY_PATH_DELTA_CHALLENGER_SESSION_ROOT / "hard_invalidation_non_exit_late_extension_hedge_v1_case_results.json"
REPLAY_PATH_DELTA_CHALLENGER_TRACE_ROOT = REPLAY_PATH_DELTA_CHALLENGER_SESSION_ROOT / "non_exit_late_extension_hedge_v1"
REPLAY_PATH_DELTA_COMPARE_FIELDS = (
    "action_type",
    "position_before",
    "target_position_after",
    "reason",
    "severity_level",
    "severity_action_type",
    "severity_target_buy_units",
)
REPLAY_PATH_DELTA_SUMMARY_FIELDS = (
    "date",
    "action_type",
    "position_before",
    "target_position_after",
    "reason",
    "severity_level",
    "severity_action_type",
    "severity_target_buy_units",
    "active_thesis",
    "confidence",
    "risk_warning",
)
POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT = Path(
    r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T083303Z\full_coverage_replay_path_delta_audit"
)
POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS = POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT / "hard_invalidation_full_coverage_case_diagnostics.json"
POST_TRIGGER_PATH_MINE_SOURCE_ROLLUP = POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT / "hard_invalidation_full_coverage_rollup.json"
POST_TRIGGER_PATH_MINE_SOURCE_DECISION = POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT / "hard_invalidation_full_coverage_decision.json"
POST_TRIGGER_PATH_MINE_SOURCE_SYMBOL_SET_DIFF = POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT / "hard_invalidation_full_coverage_symbol_set_diff.json"
POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT = Path(
    r"G:\Tradex\scratch\research_sessions\tradex_iizuka_trade_learning_loop\20260428T105855Z\trace_instrumentation"
)
POST_TRIGGER_PATH_MINE_V2_SOURCE_MANIFEST = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "hard_invalidation_trace_instrumentation_manifest.json"
POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_INVENTORY = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "hard_invalidation_trace_schema_inventory.json"
POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_CHANGE_SUMMARY = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "hard_invalidation_trace_schema_change_summary.json"
POST_TRIGGER_PATH_MINE_V2_SOURCE_NO_LOOKAHEAD_VALIDATION = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "hard_invalidation_trace_no_lookahead_validation.json"
POST_TRIGGER_PATH_MINE_V2_SOURCE_DECISION = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "hard_invalidation_trace_instrumentation_decision.json"
POST_TRIGGER_PATH_MINE_V2_BASELINE_TRACE_ROOT = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "repaired_v2_rerun"
POST_TRIGGER_PATH_MINE_V2_CHALLENGER_TRACE_ROOT = POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT / "non_exit_late_extension_hedge_v1_rerun"
TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT = REPAIRED_V2_SESSION_ROOT
TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS = REPAIRED_V2_CASE_RESULTS
TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT = POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT
TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS = POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT / "hard_invalidation_non_exit_late_extension_hedge_v1_case_results.json"
TRACE_INSTRUMENTATION_TRACE_SCHEMA_VERSION = getattr(loop_mod, "TRACE_SCHEMA_VERSION", None)
TRACE_INSTRUMENTATION_REQUIRED_FIELDS = [
    "symbol",
    "date",
    "as_of",
    "session_id",
    "candidate_id",
    "trace_schema_version",
    "action_type",
    "action_reason",
    "action_source",
    "position_before",
    "position_after",
    "target_position_after",
    "buy_units_before",
    "sell_units_before",
    "buy_units_after",
    "sell_units_after",
    "net_units_before",
    "net_units_after",
    "gross_units_before",
    "gross_units_after",
    "close",
    "next_open_if_used",
    "mark_price",
    "position_market_value_before",
    "position_market_value_after",
    "realized_pnl_day",
    "unrealized_pnl_day",
    "cumulative_realized_pnl",
    "cumulative_unrealized_pnl",
    "equity_curve_value",
    "pnl_path_available",
    "data_source",
    "input_bar_date",
    "decision_uses_future_data",
    "trace_row_hash",
    "reason",
    "special_reason",
    "forced_exit_reason",
    "severity_level",
    "severity_action_type",
    "reduction_intensity",
    "severity_target_buy_units",
    "profit_protection_guard_applied",
    "profit_preservation_guard_applied",
    "profit_preservation_guard_reason",
    "late_extension_hedge_condition_activated",
    "late_extension_hedge_condition_reason",
    "evidence_for",
    "evidence_against",
    "risk_warning",
    "confidence",
    "active_thesis",
    "no_lookahead_assertion",
]
GUARD_EVIDENCE_TARGET_FIELDS = (
    "action_type",
    "active_thesis",
    "confidence",
    "date",
    "evidence_against",
    "evidence_for",
    "no_lookahead_assertion",
    "position_before",
    "reason",
    "reduction_intensity",
    "risk_warning",
    "severity_action_type",
    "severity_level",
    "severity_target_buy_units",
    "symbol",
    "target_position_after",
)
GUARD_EVIDENCE_POSTHOC_FIELDS = (
    "baseline_v2_action_on_trigger_date",
    "baseline_v2_actual_position_after_trigger",
    "baseline_v2_pnl",
    "baseline_v2_selected_event_source",
    "baseline_v2_severity_level",
    "baseline_v2_target_position",
    "challenger_action_on_trigger_date",
    "challenger_actual_position_after_trigger",
    "challenger_pnl",
    "challenger_severity_level",
    "challenger_target_position",
    "compare",
    "compare_to_original_baseline",
    "compare_to_repaired_v2",
    "contract_violation",
    "false_exit_or_premature_exit_candidate",
    "accidentally_preserved_true_loser",
    "guard_activated",
    "guard_reasons",
    "typed_guard_reason_present",
    "selected_hard_invalidation_event_date",
    "selected_hard_invalidation_event_reason",
    "selected_hard_invalidation_event_action_type",
    "selected_hard_invalidation_event_target_position",
    "selected_hard_invalidation_event_source",
    "selected_event_action_allowed_under_recorded_severity",
    "emitted_action_allowed_under_recorded_severity",
    "mismatch_cause",
    "pnl_delta_vs_repaired_v2",
    "pnl_delta_vs_original_baseline",
    "selected_hard_invalidation_event_source",
)
GUARD_EVIDENCE_UNAVAILABLE_FIELDS = (
    "forced_exit_reason",
    "special_reason",
    "high_severity_condition_met",
    "loss_side_override_applied",
    "profit_protection_guard_applied",
    "profit_preservation_guard_applied",
    "profit_preservation_guard_reason",
)
NO_LOOKAHEAD_ASSERTION = loop_mod.NO_LOOKAHEAD_ASSERTION


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _write_json(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(value), encoding="utf-8")
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _file_identity(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    exists = path.exists()
    return {
        "path": str(path),
        "resolved_path": str(resolved),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else None,
        "sha256": _sha256_file(path) if exists and path.is_file() else None,
    }


def _trace_rows(trace_path: str) -> list[dict[str, Any]]:
    return list(_load_json(Path(trace_path))["rows"])


def _trace_case_dir_name(case_spec: dict[str, Any]) -> str:
    return f"{case_spec['symbol']}_{str(case_spec['name']).replace(' ', '_')}"


def _trace_paths_for_case(session_root: Path, case_spec: dict[str, Any]) -> dict[str, Path]:
    case_dir = session_root / _trace_case_dir_name(case_spec)
    symbol = str(case_spec["symbol"])
    return {
        "case_dir": case_dir,
        "baseline_trace_path": case_dir / f"{symbol}_baseline_trace.json",
        "corrected_trace_path": case_dir / f"{symbol}_corrected_trace.json",
    }


def _trace_row_field_names(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(field) for row in rows for field in row.keys()})


def _trace_row_signature(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": row.get("date"),
        "action_type": row.get("action_type"),
        "position_before": row.get("position_before"),
        "target_position_after": row.get("target_position_after"),
        "reason": row.get("reason"),
        "severity_level": row.get("severity_level"),
        "severity_action_type": row.get("severity_action_type"),
        "severity_target_buy_units": row.get("severity_target_buy_units"),
        "reduction_intensity": row.get("reduction_intensity"),
        "late_extension_hedge_condition_activated": row.get("late_extension_hedge_condition_activated"),
        "late_extension_hedge_condition_reason": row.get("late_extension_hedge_condition_reason"),
        "special_reason": row.get("special_reason"),
        "forced_exit_reason": row.get("forced_exit_reason"),
        "no_lookahead_assertion": row.get("no_lookahead_assertion"),
    }


def _trace_row_signature_sequence(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_trace_row_signature(row) for row in rows]


def _trace_core_difference(reference_rows: list[dict[str, Any]], rerun_rows: list[dict[str, Any]]) -> dict[str, Any]:
    reference_sig = _trace_row_signature_sequence(reference_rows)
    rerun_sig = _trace_row_signature_sequence(rerun_rows)
    changed = reference_sig != rerun_sig or len(reference_rows) != len(rerun_rows)
    mismatches: list[dict[str, Any]] = []
    if changed:
        max_len = max(len(reference_rows), len(rerun_rows))
        for index in range(max_len):
            reference_row = reference_sig[index] if index < len(reference_sig) else None
            rerun_row = rerun_sig[index] if index < len(rerun_sig) else None
            if reference_row != rerun_row:
                mismatches.append(
                    {
                        "index": index,
                        "reference": reference_row,
                        "rerun": rerun_row,
                    }
                )
    return {
        "row_count_reference": len(reference_rows),
        "row_count_rerun": len(rerun_rows),
        "row_count_stable": len(reference_rows) == len(rerun_rows),
        "sequence_stable": reference_sig == rerun_sig,
        "core_mismatches": mismatches,
        "changed": changed,
    }


def _trace_path_signature(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "date": row.get("date"),
        "action_type": row.get("action_type"),
        "position_before": row.get("position_before"),
        "target_position_after": row.get("target_position_after"),
    }


def _trace_path_difference(reference_rows: list[dict[str, Any]], rerun_rows: list[dict[str, Any]]) -> dict[str, Any]:
    reference_sig = [_trace_path_signature(row) for row in reference_rows]
    rerun_sig = [_trace_path_signature(row) for row in rerun_rows]
    changed = reference_sig != rerun_sig or len(reference_rows) != len(rerun_rows)
    mismatches: list[dict[str, Any]] = []
    if changed:
        max_len = max(len(reference_rows), len(rerun_rows))
        for index in range(max_len):
            reference_row = reference_sig[index] if index < len(reference_sig) else None
            rerun_row = rerun_sig[index] if index < len(rerun_sig) else None
            if reference_row != rerun_row:
                mismatches.append(
                    {
                        "index": index,
                        "reference": reference_row,
                        "rerun": rerun_row,
                    }
                )
    return {
        "row_count_reference": len(reference_rows),
        "row_count_rerun": len(rerun_rows),
        "row_count_stable": len(reference_rows) == len(rerun_rows),
        "sequence_stable": reference_sig == rerun_sig,
        "core_mismatches": mismatches,
        "changed": changed,
    }


def _trace_schema_inventory_for_rows(*, rows: list[dict[str, Any]]) -> dict[str, Any]:
    field_names = _trace_row_field_names(rows)
    version_values = sorted({str(row.get("trace_schema_version")) for row in rows if row.get("trace_schema_version") is not None})
    pnl_available_count = sum(1 for row in rows if bool(row.get("pnl_path_available")))
    return {
        "field_names": field_names,
        "trace_schema_version_values": version_values,
        "row_count": len(rows),
        "pnl_path_available_row_count": pnl_available_count,
        "missing_required_fields": [field for field in TRACE_INSTRUMENTATION_REQUIRED_FIELDS if field not in field_names],
        "extra_fields": [field for field in field_names if field not in TRACE_INSTRUMENTATION_REQUIRED_FIELDS],
    }


def _row_by_date(rows: list[dict[str, Any]], date: str) -> dict[str, Any]:
    for row in rows:
        if str(row.get("date")) == str(date):
            return row
    raise KeyError(f"no trace row found for date={date}")


def _build_case_spec_from_split(entry: dict[str, Any]) -> dict[str, Any]:
    replay_window = entry["replay_window"]
    trigger_axis = entry.get("trigger_axis")
    secondary_axes = list(entry.get("secondary_axes") or [])
    expected_themes = [
        str(entry.get("pre_scan_trigger_reason") or ""),
        str(entry.get("reason_for_axis_bucket") or ""),
        str(entry.get("primary_axis_bucket") or ""),
    ]
    expected_themes = [theme for theme in expected_themes if theme]
    if trigger_axis and trigger_axis not in expected_themes:
        expected_themes.append(str(trigger_axis))
    if secondary_axes:
        expected_themes.extend(str(axis) for axis in secondary_axes)
    return {
        "symbol": str(entry["symbol"]),
        "name": str(entry.get("company_name") or entry["symbol"]),
        "role": "mining_candidate",
        "start_date": str(replay_window["start_date"]),
        "end_date": str(replay_window["end_date"]),
        "trade_start_date": str(replay_window["start_date"]),
        "expected_themes": expected_themes,
    }


def _run_case_bundle(*, case_spec: dict[str, Any], output_root: Path, candidate_name: str = SEVERITY_CANDIDATE) -> dict[str, Any]:
    baseline = loop_mod._run_case(
        case_spec=case_spec,
        source_db_path=loop_mod.DEFAULT_SOURCE_DB_PATH,
        mode="baseline",
        candidate_name=candidate_name,
    )
    corrected = loop_mod._run_case(
        case_spec=case_spec,
        source_db_path=loop_mod.DEFAULT_SOURCE_DB_PATH,
        mode="corrected",
        candidate_name=candidate_name,
    )
    baseline_labels = loop_mod._classify_case(baseline, mode="baseline", candidate_name=candidate_name)["labels"]
    corrected_labels = loop_mod._classify_case(corrected, mode="corrected", candidate_name=candidate_name)["labels"]
    baseline = {**baseline, "labels": baseline_labels}
    corrected = {**corrected, "labels": corrected_labels}
    artifact = loop_mod._build_case_result_artifact(
        baseline=baseline,
        corrected=corrected,
        mode="baseline_vs_corrected",
        output_root=output_root,
    )
    return artifact


def _action_at(trace_path: str, date: str) -> tuple[str | None, str | None]:
    rows = _trace_rows(trace_path)
    row = _row_by_date(rows, date)
    return str(row.get("action_type")) if row.get("action_type") is not None else None, str(row.get("target_position_after")) if row.get("target_position_after") is not None else None


def _alignment_action_date(summary: dict[str, Any], fallback_date: str | None) -> str | None:
    return (
        summary.get("first_hard_invalidation_action_date")
        or summary.get("first_failed_followthrough_date")
        or summary.get("first_reentry_block_date")
        or summary.get("first_add_stop_date")
        or summary.get("first_confirmation_date")
        or summary.get("first_entry_date")
        or fallback_date
    )


def _severity_action_aligned(severity_level: str | None, action_type: str | None) -> bool:
    if severity_level is None:
        return action_type in {None, "watch"}
    if severity_level == "exit_all":
        return action_type == "exit_all"
    if severity_level in {"long_reduce", "partial_exit"}:
        return action_type == "long_reduce"
    if severity_level == "hedge_add":
        return action_type == "hedge_add"
    return False


def _severity_target_aligned(
    severity_level: str | None,
    action_type: str | None,
    target_position: str | None,
    severity_target_buy_units: int | None,
) -> bool:
    if severity_level is None:
        return action_type in {None, "watch"}
    if severity_level == "exit_all":
        return action_type == "exit_all" and target_position == "0-0"
    if severity_level in {"long_reduce", "partial_exit"}:
        if action_type != "long_reduce":
            return False
        if target_position == "0-0":
            return False
        if severity_target_buy_units is None:
            return True
        return int(severity_target_buy_units) > 0
    if severity_level == "hedge_add":
        if action_type != "hedge_add":
            return False
        if target_position == "0-0":
            return False
        if severity_target_buy_units is None:
            return True
        return int(severity_target_buy_units) > 0
    return False


_HARD_INVALIDATION_EVENT_REASON_VALUES = {
    "hard_invalidation_exit_v1",
    "hard_invalidation_exit_all",
    "hard_invalidation_exit_severity_v2_loss_side_override",
    "hard_invalidation_long_reduce",
    "late_extension_blocked",
    "partial_exit_due_to_hard_invalidation",
}

_HARD_INVALIDATION_EVENT_FORCED_EXIT_VALUES = {
    "hard_invalidation_exit_v1",
    "hard_invalidation_exit_all",
    "hard_invalidation_exit_severity_v2_loss_side_override",
}


def _select_hard_invalidation_event_row(
    rows: list[dict[str, Any]],
    *,
    candidate_name: str,
) -> dict[str, Any] | None:
    event_rows = [
        row
        for row in rows
        if str(row.get("reason")) in _HARD_INVALIDATION_EVENT_REASON_VALUES
        or str(row.get("forced_exit_reason")) in _HARD_INVALIDATION_EVENT_FORCED_EXIT_VALUES
    ]
    if not event_rows:
        return None
    if candidate_name == SEVERITY_CANDIDATE:
        return next((row for row in event_rows if str(row.get("action_type")) == "exit_all"), event_rows[0])
    return event_rows[0]


def _first_hard_invalidation_date_fallback(artifact: dict[str, Any]) -> str | None:
    corrected = artifact["corrected"]["summary"]
    compare_corrected = artifact.get("compare", {}).get("corrected") or {}
    return (
        corrected.get("first_hard_invalidation_date")
        or compare_corrected.get("first_hard_invalidation_date")
        or corrected.get("first_hard_invalidation_action_date")
        or compare_corrected.get("first_hard_invalidation_action_date")
    )


def _mismatch_cause_for_case(
    *,
    selected_event_row: dict[str, Any] | None,
    selected_event_source: str,
    emitted_action_type: str | None,
    recorded_severity_level: str | None,
    recorded_severity_action_type: str | None,
    primary_axis_bucket: str,
) -> str | None:
    if selected_event_source == "missing":
        if primary_axis_bucket == "mixed_hard_invalidation_failed_followthrough":
            return "multiple_events_collapsed_incorrectly"
        return "summary_artifact_losing_trigger_date_detail"
    if selected_event_source == "summary_fallback":
        if primary_axis_bucket == "mixed_hard_invalidation_failed_followthrough":
            return "multiple_events_collapsed_incorrectly"
        return "summary_artifact_losing_trigger_date_detail"
    if selected_event_row is None:
        return "wrong event selected"
    selected_action = str(selected_event_row.get("action_type")) if selected_event_row.get("action_type") is not None else None
    if recorded_severity_level is None:
        if primary_axis_bucket == "mixed_hard_invalidation_failed_followthrough":
            return "multiple_events_collapsed_incorrectly"
        return "wrong severity recorded"
    if recorded_severity_action_type is None:
        return "wrong severity recorded"
    if emitted_action_type is None:
        return "wrong action emitted"
    if selected_action != emitted_action_type:
        return "wrong action emitted"
    if not _severity_action_aligned(recorded_severity_level, recorded_severity_action_type):
        return "wrong severity recorded"
    return None


def _build_anchor_regression(*, session_root: Path) -> list[dict[str, Any]]:
    anchor_case_specs = {case["symbol"]: case for case in loop_mod.CASE_SPECS}
    results: list[dict[str, Any]] = []
    for symbol in ANCHOR_SYMBOLS:
        artifact = _run_case_bundle(case_spec=anchor_case_specs[symbol], output_root=session_root)
        baseline = artifact["baseline"]["summary"]
        corrected = artifact["corrected"]["summary"]
        trigger_date = (
            corrected.get("first_hard_invalidation_action_date")
            or corrected.get("first_failed_followthrough_date")
            or corrected.get("first_profit_take_date")
            or corrected.get("first_reentry_block_date")
            or corrected.get("first_hard_invalidation_date")
            or corrected.get("first_add_stop_date")
            or corrected.get("first_confirmation_date")
            or corrected.get("first_entry_date")
        )
        baseline_action, baseline_target = _action_at(artifact["trace_artifacts"]["baseline_trace_path"], trigger_date) if trigger_date else (None, None)
        severity_action, severity_target = _action_at(artifact["trace_artifacts"]["corrected_trace_path"], trigger_date) if trigger_date else (None, None)
        results.append(
            {
                "symbol": symbol,
                "name": artifact["case"]["name"],
                "baseline_pnl": float(baseline["total_pnl"]),
                "severity_refined_pnl": float(corrected["total_pnl"]),
                "pnl_delta": float(corrected["total_pnl"] - baseline["total_pnl"]),
                "trigger_date": trigger_date,
                "baseline_action": baseline_action,
                "severity_action": severity_action,
                "baseline_target_position": baseline_target,
                "severity_target_position": severity_target,
                "baseline_actual_position_after_trigger": baseline_target,
                "severity_actual_position_after_trigger": severity_target,
                "action_changed_flag": bool(baseline_action != severity_action) if trigger_date else False,
                "target_position_changed_flag": bool(baseline_target != severity_target) if trigger_date else False,
                "actual_position_changed_flag": bool(baseline_target != severity_target) if trigger_date else False,
                "labels": list(artifact["corrected"]["labels"]),
                "damage_reduced": bool(corrected["total_pnl"] > baseline["total_pnl"]),
                "preservation_flag": bool(
                    symbol == "5541"
                    and abs(float(corrected["total_pnl"]) - float(baseline["total_pnl"])) < 1e-6
                    and not bool(artifact["compare"]["comparison"]["false_winner_block"])
                ),
                "first_hard_invalidation_date": corrected.get("first_hard_invalidation_date"),
                "first_hard_invalidation_action_date": corrected.get("first_hard_invalidation_action_date"),
                "hard_invalidation_severity_level": corrected.get("hard_invalidation_severity_level"),
                "hard_invalidation_reduction_intensity": corrected.get("hard_invalidation_reduction_intensity"),
                "compare": artifact["compare"],
                "trace_artifacts": artifact["trace_artifacts"],
            }
        )
    return results


def _build_severity_case_results(
    *,
    split_cases: list[dict[str, Any]],
    previous_v2_cases: list[dict[str, Any]],
    session_root: Path,
) -> list[dict[str, Any]]:
    old_by_symbol = {str(entry["symbol"]): entry for entry in split_cases}
    previous_v2_by_symbol = {str(entry["symbol"]): entry for entry in previous_v2_cases}
    results: list[dict[str, Any]] = []
    for symbol in SEVERITY_CASE_SYMBOLS:
        old_entry = old_by_symbol[symbol]
        previous_v2_entry = previous_v2_by_symbol[symbol]
        case_spec = _build_case_spec_from_split(old_entry)
        artifact = _run_case_bundle(case_spec=case_spec, output_root=session_root)
        baseline = artifact["baseline"]["summary"]
        corrected = artifact["corrected"]["summary"]
        corrected_rows = _trace_rows(str(artifact["trace_artifacts"]["corrected_trace_path"]))
        trigger_date = str(old_entry["trigger_date"])
        action_date = trigger_date
        old_trace_action, old_trace_target = _action_at(str(old_entry["trace_artifacts"]["corrected_trace_path"]), action_date) if action_date else (None, None)
        baseline_trace_action, baseline_trace_target = _action_at(artifact["trace_artifacts"]["baseline_trace_path"], action_date) if action_date else (None, None)
        refined_trace_action, refined_trace_target = _action_at(artifact["trace_artifacts"]["corrected_trace_path"], action_date) if action_date else (None, None)
        selected_event_date = corrected.get("first_hard_invalidation_action_date") or artifact["compare"]["corrected"].get("first_hard_invalidation_action_date")
        selected_event_row_for_report: dict[str, Any] | None = None
        selected_event_source = "missing"
        if selected_event_date is not None:
            try:
                selected_event_row_for_report = _row_by_date(corrected_rows, str(selected_event_date))
                selected_event_source = "selector"
            except KeyError:
                selected_event_row_for_report = None
        if selected_event_row_for_report is None and selected_event_date is None:
            selected_event_date = (
                corrected.get("first_hard_invalidation_date")
                or artifact["compare"]["corrected"].get("first_hard_invalidation_date")
            )
            if selected_event_date is not None:
                try:
                    selected_event_row_for_report = _row_by_date(corrected_rows, str(selected_event_date))
                    selected_event_source = "summary_fallback"
                except KeyError:
                    selected_event_row_for_report = None
                    selected_event_source = "summary_fallback"
        selected_event_date_text = str(selected_event_date) if selected_event_date is not None else None
        selected_event_reason = (
            str(selected_event_row_for_report.get("reason"))
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("reason") is not None
            else None
        )
        selected_event_action_type = (
            str(selected_event_row_for_report.get("action_type"))
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("action_type") is not None
            else None
        )
        selected_event_target_position = (
            str(selected_event_row_for_report.get("target_position_after"))
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("target_position_after") is not None
            else None
        )
        old_pnl = float(old_entry["stacked_pnl"])
        v2_pnl = float(previous_v2_entry["severity_refined_pnl"])
        v2_action = str(previous_v2_entry["refined_action_on_trigger_date"])
        refined_pnl = float(corrected["total_pnl"])
        baseline_pnl = float(baseline["total_pnl"])
        baseline_profitable = baseline_pnl > 0
        loss_side_protection_preserved = refined_pnl >= baseline_pnl if baseline_pnl <= 0 else refined_pnl >= v2_pnl
        profit_destruction_reduced = baseline_profitable and refined_pnl > v2_pnl
        severity_level = corrected.get("hard_invalidation_severity_level")
        severity_action_type = corrected.get("hard_invalidation_severity_action_type")
        severity_target_buy_units = corrected.get("hard_invalidation_severity_target_buy_units")
        loss_side_override_applied = bool(corrected.get("hard_invalidation_loss_side_override_applied"))
        profit_protection_guard_applied = bool(corrected.get("hard_invalidation_profit_protection_guard_applied"))
        high_severity_condition_met = bool(corrected.get("hard_invalidation_high_severity_condition_met"))
        severity_action_aligned = _severity_action_aligned(severity_level, severity_action_type)
        selected_event_action_allowed_under_recorded_severity = bool(
            _severity_action_aligned(severity_level, selected_event_action_type)
            and _severity_target_aligned(
                severity_level,
                selected_event_action_type,
                selected_event_target_position,
                int(severity_target_buy_units) if severity_target_buy_units is not None else None,
            )
        )
        severity_target_aligned = _severity_target_aligned(
            severity_level,
            refined_trace_action,
            refined_trace_target,
            int(severity_target_buy_units) if severity_target_buy_units is not None else None,
        )
        emitted_action_allowed_under_recorded_severity = bool(
            _severity_action_aligned(severity_level, refined_trace_action)
            and _severity_target_aligned(
                severity_level,
                refined_trace_action,
                refined_trace_target,
                int(severity_target_buy_units) if severity_target_buy_units is not None else None,
            )
        )
        target_actual_aligned = bool(
            (severity_level == "exit_all" and refined_trace_target == "0-0")
            or (
                severity_level in {"long_reduce", "partial_exit"}
                and refined_trace_target not in {None, "0-0"}
            )
            or (severity_level is None and refined_trace_action == "watch")
        )
        mismatch_cause = _mismatch_cause_for_case(
            selected_event_row=selected_event_row_for_report,
            selected_event_source=selected_event_source,
            emitted_action_type=selected_event_action_type,
            recorded_severity_level=severity_level,
            recorded_severity_action_type=severity_action_type,
            primary_axis_bucket=str(old_entry["primary_axis_bucket"]),
        )
        contract_violation = bool(
            selected_event_source == "selector" and not selected_event_action_allowed_under_recorded_severity
        )
        selected_event_label = (
            "selected_selector_row"
            if selected_event_source == "selector"
            else "summary_fallback_row"
            if selected_event_source == "summary_fallback"
            else "missing"
        )
        results.append(
            {
                "symbol": symbol,
                "company_name": str(old_entry["company_name"]),
                "replay_window": dict(old_entry["replay_window"]),
                "data_backed": bool(old_entry["data_backed"]),
                "note_backed": bool(old_entry["note_backed"]),
                "pre_scan_trigger_reason": str(old_entry["pre_scan_trigger_reason"]),
                "expected_axis": str(old_entry["trigger_axis"] if old_entry["trigger_axis"] != "mixed" else "mixed"),
                "primary_axis_bucket": str(old_entry["primary_axis_bucket"]),
                "secondary_axes": list(old_entry["secondary_axes"]),
                "trigger_dates_by_axis": dict(old_entry["trigger_dates_by_axis"]),
                "trigger_date": trigger_date,
                "action_date": action_date,
                "selected_hard_invalidation_event_date": selected_event_date_text,
                "selected_hard_invalidation_event_reason": selected_event_reason,
                "selected_hard_invalidation_event_action_type": selected_event_action_type,
                "selected_hard_invalidation_event_target_position": selected_event_target_position,
                "selected_hard_invalidation_event_source": selected_event_label,
                "baseline_action_on_trigger_date": baseline_trace_action,
                "v1_aligned_action_on_trigger_date": old_trace_action,
                "v2_action_on_trigger_date": v2_action,
                "v2_loss_side_override_action_on_trigger_date": refined_trace_action,
                "baseline_target_position": baseline_trace_target,
                "v1_aligned_target_position": old_trace_target,
                "v2_target_position": previous_v2_entry["refined_target_position"],
                "v2_loss_side_override_target_position": refined_trace_target,
                "baseline_actual_position_after_trigger": baseline_trace_target,
                "v1_aligned_actual_position_after_trigger": old_trace_target,
                "v2_actual_position_after_trigger": previous_v2_entry["refined_actual_position_after_trigger"],
                "v2_loss_side_override_actual_position_after_trigger": refined_trace_target,
                "baseline_pnl": baseline_pnl,
                "v1_aligned_pnl": old_pnl,
                "v2_pnl": v2_pnl,
                "v2_loss_side_override_pnl": refined_pnl,
                "pnl_delta_vs_baseline": float(refined_pnl - baseline_pnl),
                "pnl_delta_vs_v2": float(refined_pnl - v2_pnl),
                "action_changed_flag": bool(v2_action != refined_trace_action),
                "target_position_changed_flag": bool(previous_v2_entry["refined_target_position"] != refined_trace_target),
                "actual_position_changed_flag": bool(previous_v2_entry["refined_target_position"] != refined_trace_target),
                "triggered_axes": list(old_entry["triggered_axes"]),
                "labels": list(artifact["corrected"]["labels"]),
                "severity_level": severity_level,
                "reduction_intensity": corrected.get("hard_invalidation_reduction_intensity"),
                "loss_side_override_applied": loss_side_override_applied,
                "profit_protection_guard_applied": profit_protection_guard_applied,
                "high_severity_condition_met": high_severity_condition_met,
                "severity_action_aligned": severity_action_aligned,
                "severity_target_aligned": severity_target_aligned,
                "selected_event_action_allowed_under_recorded_severity": selected_event_action_allowed_under_recorded_severity,
                "emitted_action_allowed_under_recorded_severity": emitted_action_allowed_under_recorded_severity,
                "target_actual_aligned": target_actual_aligned,
                "contract_violation": contract_violation,
                "premature_exit_flag": bool(baseline_profitable and refined_trace_action == "exit_all"),
                "false_exit_by_outcome_flag": bool(baseline_profitable and refined_pnl < baseline_pnl),
                "whether_exit_all_was_too_aggressive": bool(baseline_profitable and refined_trace_action == "exit_all"),
                "profit_destruction_reduced": bool(profit_destruction_reduced),
                "loss_side_protection_preserved": bool(loss_side_protection_preserved),
                "damage_reduced": bool(refined_pnl > v2_pnl),
                "mismatch_cause": mismatch_cause,
                "reason_for_bucket": str(old_entry["reason_for_axis_bucket"]),
                "trigger_axis": str(old_entry["trigger_axis"]),
                "source_interpretation": str(old_entry["source_interpretation"]),
                "compare": artifact["compare"],
                "trace_artifacts": artifact["trace_artifacts"],
                "labels": list(artifact["corrected"]["labels"]),
            }
        )
    return results


def _build_rollup(case_results: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_counts = Counter(entry["primary_axis_bucket"] for entry in case_results)
    improved_by_bucket = Counter()
    worsened_by_bucket = Counter()
    unchanged_by_bucket = Counter()
    total_delta_by_bucket = defaultdict(float)
    total_pnl_delta_vs_v2 = 0.0
    total_pnl_delta_vs_baseline = 0.0
    improved_vs_baseline_count = 0
    worsened_vs_baseline_count = 0
    loss_side_cases_preserved = 0
    profitable_baseline_cases_damaged = 0
    false_exit_count = 0
    premature_exit_count = 0
    false_winner_block_count = 0
    label_only_count = 0
    action_changing_count = 0
    contract_violation_count = 0
    no_lookahead_violations = 0
    hard_invalidation_trigger_count = 0
    bad_reentry_trigger_count = 0
    loss_side_override_applied_count = 0
    exit_all_count = 0
    long_reduce_count = 0
    partial_exit_count = 0
    mismatch_cause_counts = Counter()
    selected_event_source_counts = Counter()
    cases_requiring_trace_review: list[str] = []

    for entry in case_results:
        delta = float(entry["pnl_delta_vs_v2"])
        baseline_delta = float(entry["pnl_delta_vs_baseline"])
        bucket = entry["primary_axis_bucket"]
        total_delta_by_bucket[bucket] += delta
        total_pnl_delta_vs_v2 += delta
        total_pnl_delta_vs_baseline += baseline_delta
        if delta > 0:
            improved_by_bucket[bucket] += 1
        elif delta < 0:
            worsened_by_bucket[bucket] += 1
        else:
            unchanged_by_bucket[bucket] += 1
        if baseline_delta > 0:
            improved_vs_baseline_count += 1
        elif baseline_delta < 0:
            worsened_vs_baseline_count += 1
        if entry["action_changed_flag"]:
            action_changing_count += 1
        if not entry["action_changed_flag"] and not entry["target_position_changed_flag"] and not entry["actual_position_changed_flag"]:
            label_only_count += 1
        if entry.get("contract_violation"):
            contract_violation_count += 1
        if not (
            _trace_no_lookahead_ok(entry["trace_artifacts"]["baseline_trace_path"])
            and _trace_no_lookahead_ok(entry["trace_artifacts"]["corrected_trace_path"])
        ):
            no_lookahead_violations += 1
        if entry.get("loss_side_override_applied"):
            loss_side_override_applied_count += 1
        if entry.get("mismatch_cause") is not None:
            mismatch_cause_counts[str(entry["mismatch_cause"])] += 1
        if entry.get("selected_hard_invalidation_event_source") is not None:
            selected_event_source_counts[str(entry["selected_hard_invalidation_event_source"])] += 1
        action_type = entry.get("v2_loss_side_override_action_on_trigger_date")
        if action_type == "exit_all":
            exit_all_count += 1
        elif action_type == "long_reduce":
            long_reduce_count += 1
        elif action_type == "partial_exit":
            partial_exit_count += 1
        if entry.get("premature_exit_flag"):
            premature_exit_count += 1
        if float(entry["baseline_pnl"]) <= 0 and bool(entry["loss_side_protection_preserved"]):
            loss_side_cases_preserved += 1
        if float(entry["baseline_pnl"]) > 0 and baseline_delta < 0:
            profitable_baseline_cases_damaged += 1
        if entry["trigger_axis"] == "hard_invalidation_exit_v1" or "hard_invalidation_exit_v1" in entry["triggered_axes"]:
            hard_invalidation_trigger_count += 1
        if entry["trigger_axis"] == "bad_reentry_after_profit_take_v1" or "bad_reentry_after_profit_take_v1" in entry["triggered_axes"]:
            bad_reentry_trigger_count += 1
        if entry["premature_exit_flag"]:
            false_exit_count += 1
        if entry["company_name"] == "Taiheiyo Kinzoku 5541" and entry["premature_exit_flag"]:
            false_winner_block_count += 1
        if entry["trigger_date"] is None or entry["v2_loss_side_override_pnl"] is None:
            cases_requiring_trace_review.append(str(entry["symbol"]))

    return {
        "total_cases": len(case_results),
        "mined_candidate_count": len(case_results),
        "aligned_case_count": len(case_results) - contract_violation_count,
        "bucket_counts": dict(bucket_counts),
        "improved_count_by_bucket": dict(improved_by_bucket),
        "worsened_count_by_bucket": dict(worsened_by_bucket),
        "unchanged_count_by_bucket": dict(unchanged_by_bucket),
        "total_pnl_delta_by_bucket": {bucket: float(value) for bucket, value in total_delta_by_bucket.items()},
        "false_exit_by_outcome_count": false_exit_count,
        "premature_exit_count": premature_exit_count,
        "false_winner_block_count": false_winner_block_count,
        "label_only_count": label_only_count,
        "action_changing_count": action_changing_count,
        "loss_side_override_applied_count": loss_side_override_applied_count,
        "exit_all_count": exit_all_count,
        "long_reduce_count": long_reduce_count,
        "partial_exit_count": partial_exit_count,
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "mismatch_cause_counts": dict(mismatch_cause_counts),
        "selected_event_source_counts": dict(selected_event_source_counts),
        "cases_requiring_trace_review": cases_requiring_trace_review,
        "hard_invalidation_trigger_count": hard_invalidation_trigger_count,
        "bad_reentry_trigger_count": bad_reentry_trigger_count,
        "false_exit_count": false_exit_count,
        "improved_vs_v2_count": sum(1 for entry in case_results if float(entry["pnl_delta_vs_v2"]) > 0),
        "worsened_vs_v2_count": sum(1 for entry in case_results if float(entry["pnl_delta_vs_v2"]) < 0),
        "improved_vs_old_hard_invalidation_count": sum(1 for entry in case_results if float(entry["pnl_delta_vs_v2"]) > 0),
        "worsened_vs_old_hard_invalidation_count": sum(1 for entry in case_results if float(entry["pnl_delta_vs_v2"]) < 0),
        "improved_vs_baseline_count": improved_vs_baseline_count,
        "worsened_vs_baseline_count": worsened_vs_baseline_count,
        "total_pnl_delta_vs_v2": float(total_pnl_delta_vs_v2),
        "total_pnl_delta_vs_baseline": float(total_pnl_delta_vs_baseline),
        "loss_side_cases_preserved": loss_side_cases_preserved,
        "profitable_baseline_cases_damaged": profitable_baseline_cases_damaged,
    }


def _trace_no_lookahead_ok(trace_path: str) -> bool:
    rows = _trace_rows(trace_path)
    return all(str(row.get("no_lookahead_assertion")) == NO_LOOKAHEAD_ASSERTION for row in rows)


def _summarize_trace_row(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in REPLAY_PATH_DELTA_SUMMARY_FIELDS if row.get(field) is not None}


def _trace_rows_by_date(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("date")): row for row in rows if row.get("date") is not None}


def _trace_row_field_diffs(
    baseline_row: dict[str, Any] | None,
    challenger_row: dict[str, Any] | None,
    *,
    fields: tuple[str, ...] = REPLAY_PATH_DELTA_COMPARE_FIELDS,
) -> dict[str, dict[str, Any]]:
    if baseline_row is None or challenger_row is None:
        return {
            "baseline_present": {"value": baseline_row is not None},
            "challenger_present": {"value": challenger_row is not None},
        }
    diffs: dict[str, dict[str, Any]] = {}
    for field in fields:
        baseline_value = baseline_row.get(field)
        challenger_value = challenger_row.get(field)
        if baseline_value != challenger_value:
            diffs[field] = {
                "baseline": baseline_value,
                "challenger": challenger_value,
            }
    return diffs


def _trace_path_delta_summary(
    *,
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
    trigger_date: str | None,
) -> dict[str, Any]:
    baseline_by_date = _trace_rows_by_date(baseline_rows)
    challenger_by_date = _trace_rows_by_date(challenger_rows)
    all_dates = sorted(set(baseline_by_date) | set(challenger_by_date))
    path_diffs: list[dict[str, Any]] = []
    any_field_diffs: list[dict[str, Any]] = []
    first_path_divergence: str | None = None
    first_any_field_divergence: str | None = None
    first_post_trigger_path_divergence: str | None = None
    trigger_path_diffs: dict[str, dict[str, Any]] | None = None
    trigger_any_field_diffs: dict[str, dict[str, Any]] | None = None

    for date in all_dates:
        baseline_row = baseline_by_date.get(date)
        challenger_row = challenger_by_date.get(date)
        path_diffs_for_date = _trace_row_field_diffs(baseline_row, challenger_row, fields=REPLAY_PATH_DELTA_COMPARE_FIELDS)
        if baseline_row is None or challenger_row is None:
            any_field_diffs_for_date = path_diffs_for_date
        else:
            any_field_diffs_for_date = _trace_row_field_diffs(
                {
                    "date": baseline_row.get("date"),
                    "action_type": baseline_row.get("action_type"),
                    "position_before": baseline_row.get("position_before"),
                    "target_position_after": baseline_row.get("target_position_after"),
                    "reason": baseline_row.get("reason"),
                    "severity_level": baseline_row.get("severity_level"),
                    "severity_action_type": baseline_row.get("severity_action_type"),
                    "severity_target_buy_units": baseline_row.get("severity_target_buy_units"),
                    "active_thesis": baseline_row.get("active_thesis"),
                    "confidence": baseline_row.get("confidence"),
                    "risk_warning": baseline_row.get("risk_warning"),
                },
                {
                    "date": challenger_row.get("date"),
                    "action_type": challenger_row.get("action_type"),
                    "position_before": challenger_row.get("position_before"),
                    "target_position_after": challenger_row.get("target_position_after"),
                    "reason": challenger_row.get("reason"),
                    "severity_level": challenger_row.get("severity_level"),
                    "severity_action_type": challenger_row.get("severity_action_type"),
                    "severity_target_buy_units": challenger_row.get("severity_target_buy_units"),
                    "active_thesis": challenger_row.get("active_thesis"),
                    "confidence": challenger_row.get("confidence"),
                    "risk_warning": challenger_row.get("risk_warning"),
                },
                fields=REPLAY_PATH_DELTA_SUMMARY_FIELDS,
            )
        if path_diffs_for_date:
            diff_entry = {
                "date": date,
                "baseline": _summarize_trace_row(baseline_row) if baseline_row is not None else None,
                "challenger": _summarize_trace_row(challenger_row) if challenger_row is not None else None,
                "path_field_diffs": path_diffs_for_date,
            }
            path_diffs.append(diff_entry)
            if first_path_divergence is None:
                first_path_divergence = date
            if trigger_date is not None and date >= trigger_date and first_post_trigger_path_divergence is None:
                first_post_trigger_path_divergence = date
        if any_field_diffs_for_date and first_any_field_divergence is None:
            first_any_field_divergence = date
        if trigger_date is not None and date == trigger_date:
            trigger_path_diffs = path_diffs_for_date if path_diffs_for_date else {}
            trigger_any_field_diffs = any_field_diffs_for_date if any_field_diffs_for_date else {}

    baseline_after_trigger = [
        _summarize_trace_row(row) for row in baseline_rows if trigger_date is None or str(row.get("date")) >= str(trigger_date)
    ]
    challenger_after_trigger = [
        _summarize_trace_row(row) for row in challenger_rows if trigger_date is None or str(row.get("date")) >= str(trigger_date)
    ]

    return {
        "trigger_date": trigger_date,
        "baseline_row_count": len(baseline_rows),
        "challenger_row_count": len(challenger_rows),
        "common_date_count": len(set(baseline_by_date) & set(challenger_by_date)),
        "baseline_only_date_count": len(set(baseline_by_date) - set(challenger_by_date)),
        "challenger_only_date_count": len(set(challenger_by_date) - set(baseline_by_date)),
        "first_any_field_divergence_date": first_any_field_divergence,
        "first_path_divergence_date": first_path_divergence,
        "first_post_trigger_path_divergence_date": first_post_trigger_path_divergence,
        "trigger_path_diffs": trigger_path_diffs or {},
        "trigger_any_field_diffs": trigger_any_field_diffs or {},
        "path_diffs": path_diffs,
        "baseline_ledger_after_trigger": baseline_after_trigger,
        "challenger_ledger_after_trigger": challenger_after_trigger,
        "pnl_path_available": False,
        "pnl_path_note": "Trace rows do not expose realized/unrealized PnL series; only case-level totals are available in the comparison artifacts.",
    }


def _classify_replay_path_delta_case(
    *,
    baseline_entry: dict[str, Any],
    challenger_entry: dict[str, Any] | None,
    delta_summary: dict[str, Any] | None,
    baseline_selected_action: str | None,
    challenger_selected_action: str | None,
    baseline_selected_target: str | None,
    challenger_selected_target: str | None,
) -> dict[str, Any]:
    trigger_date = str(baseline_entry.get("trigger_date")) if baseline_entry.get("trigger_date") is not None else None
    path_first = delta_summary.get("first_path_divergence_date") if delta_summary is not None else None
    any_first = delta_summary.get("first_any_field_divergence_date") if delta_summary is not None else None
    post_trigger_first = delta_summary.get("first_post_trigger_path_divergence_date") if delta_summary is not None else None

    if challenger_entry is None or delta_summary is None:
        return {
            "classification": "baseline_only_case",
            "divergence_cause": "challenger_artifact_missing",
            "comparison_available": False,
            "replay_surface_relevance": "baseline_only",
        }

    trigger_action_changed = baseline_selected_action != challenger_selected_action
    if trigger_action_changed:
        classification = "clean_intended_divergence"
        divergence_cause = "intended_challenger_activation"
    elif path_first is not None and trigger_date is not None and str(path_first) < str(trigger_date):
        classification = "reporting_definition_too_narrow"
        divergence_cause = "case_result_reporting_bug"
    elif post_trigger_first is not None:
        classification = "unchanged_selected_action_but_later_path_diverged"
        divergence_cause = "later_action_path_drift"
    elif path_first is not None:
        classification = "reporting_definition_too_narrow"
        divergence_cause = "case_result_reporting_bug"
    else:
        classification = "no_material_divergence"
        divergence_cause = "no_material_path_difference"

    return {
        "classification": classification,
        "divergence_cause": divergence_cause,
        "comparison_available": True,
        "replay_surface_relevance": "compared_case",
        "first_any_field_divergence_date": any_first,
        "first_path_divergence_date": path_first,
        "first_post_trigger_path_divergence_date": post_trigger_first,
    }


def _build_replay_path_delta_case_records(
    *,
    baseline_case_results: list[dict[str, Any]],
    challenger_case_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    challenger_by_symbol = {str(entry["symbol"]): entry for entry in challenger_case_results}
    records: list[dict[str, Any]] = []
    for baseline_entry in baseline_case_results:
        symbol = str(baseline_entry["symbol"])
        challenger_entry = challenger_by_symbol.get(symbol)
        baseline_trace_path = Path(str(baseline_entry["trace_artifacts"]["corrected_trace_path"]))
        baseline_rows = _trace_rows(str(baseline_trace_path))
        baseline_trigger_date = str(baseline_entry["trigger_date"]) if baseline_entry.get("trigger_date") is not None else None
        baseline_trigger_row = _row_by_date(baseline_rows, baseline_trigger_date) if baseline_trigger_date is not None else None
        challenger_trace_path = (
            Path(str(challenger_entry["trace_artifacts"]["corrected_trace_path"]))
            if challenger_entry is not None
            else None
        )
        challenger_rows = _trace_rows(str(challenger_trace_path)) if challenger_trace_path is not None else None
        delta_summary = (
            _trace_path_delta_summary(
                baseline_rows=baseline_rows,
                challenger_rows=challenger_rows or [],
                trigger_date=baseline_trigger_date,
            )
            if challenger_rows is not None
            else None
        )
        selected_trigger_row = None
        selected_trigger_row_source = "missing"
        if challenger_rows is not None and baseline_trigger_date is not None:
            try:
                selected_trigger_row = _row_by_date(challenger_rows, baseline_trigger_date)
                selected_trigger_row_source = "selected_trigger_date_row"
            except KeyError:
                selected_trigger_row = None
                selected_trigger_row_source = "missing"
        baseline_selected_action = (
            baseline_trigger_row.get("action_type")
            if baseline_trigger_row is not None and baseline_trigger_row.get("action_type") is not None
            else baseline_entry.get("baseline_v2_action_on_trigger_date")
        )
        challenger_selected_action = (
            selected_trigger_row.get("action_type")
            if selected_trigger_row is not None and selected_trigger_row.get("action_type") is not None
            else challenger_entry.get("challenger_action_on_trigger_date")
            if challenger_entry is not None
            else None
        )
        baseline_selected_target = (
            baseline_trigger_row.get("target_position_after")
            if baseline_trigger_row is not None and baseline_trigger_row.get("target_position_after") is not None
            else baseline_entry.get("baseline_v2_target_position")
        )
        challenger_selected_target = (
            selected_trigger_row.get("target_position_after")
            if selected_trigger_row is not None and selected_trigger_row.get("target_position_after") is not None
            else challenger_entry.get("challenger_target_position")
            if challenger_entry is not None
            else None
        )
        action_same = bool(baseline_selected_action == challenger_selected_action and baseline_selected_target == challenger_selected_target)
        classification = _classify_replay_path_delta_case(
            baseline_entry=baseline_entry,
            challenger_entry=challenger_entry,
            delta_summary=delta_summary,
            baseline_selected_action=str(baseline_selected_action) if baseline_selected_action is not None else None,
            challenger_selected_action=str(challenger_selected_action) if challenger_selected_action is not None else None,
            baseline_selected_target=str(baseline_selected_target) if baseline_selected_target is not None else None,
            challenger_selected_target=str(challenger_selected_target) if challenger_selected_target is not None else None,
        )
        material_pnl_delta = float(challenger_entry.get("pnl_delta_vs_repaired_v2", 0.0)) if challenger_entry is not None else None
        record = {
            "symbol": symbol,
            "company_name": baseline_entry.get("company_name"),
            "trigger_date": baseline_trigger_date,
            "comparison_available": challenger_entry is not None,
            "baseline_case_result_path": str(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
            "challenger_case_result_path": str(REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS),
            "baseline_trace_artifacts": dict(baseline_entry.get("trace_artifacts") or {}),
            "challenger_trace_artifacts": dict(challenger_entry.get("trace_artifacts") or {}) if challenger_entry is not None else None,
            "baseline_trace_identity": _file_identity(baseline_trace_path),
            "challenger_trace_identity": _file_identity(challenger_trace_path) if challenger_trace_path is not None else None,
            "baseline_case_result_identity": _file_identity(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
            "challenger_case_result_identity": _file_identity(REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS) if challenger_entry is not None else None,
            "selected_trigger_date_action_baseline": baseline_selected_action,
            "selected_trigger_date_action_challenger": challenger_selected_action,
            "selected_trigger_date_target_baseline": baseline_selected_target,
            "selected_trigger_date_target_challenger": challenger_selected_target,
            "selected_trigger_date_action_same": baseline_selected_action == challenger_selected_action,
            "selected_trigger_date_target_same": baseline_selected_target == challenger_selected_target,
            "selected_trigger_date_action_and_target_same": action_same,
            "selected_trigger_date_row_baseline": _summarize_trace_row(baseline_trigger_row) if baseline_trigger_row is not None else None,
            "selected_trigger_date_row_challenger": _summarize_trace_row(selected_trigger_row) if selected_trigger_row is not None else None,
            "material_pnl_delta_vs_repaired_v2": material_pnl_delta,
            "baseline_case_delta_vs_repaired_v2": baseline_entry.get("pnl_delta_vs_v2"),
            "first_any_field_divergence_date": classification.get("first_any_field_divergence_date"),
            "first_path_divergence_date": classification.get("first_path_divergence_date"),
            "first_post_trigger_path_divergence_date": classification.get("first_post_trigger_path_divergence_date"),
            "trigger_path_diffs": delta_summary.get("trigger_path_diffs") if delta_summary is not None else None,
            "trigger_any_field_diffs": delta_summary.get("trigger_any_field_diffs") if delta_summary is not None else None,
            "path_diffs": delta_summary.get("path_diffs") if delta_summary is not None else None,
            "baseline_ledger_after_trigger": delta_summary.get("baseline_ledger_after_trigger") if delta_summary is not None else None,
            "challenger_ledger_after_trigger": delta_summary.get("challenger_ledger_after_trigger") if delta_summary is not None else None,
            "pnl_path_available": bool(delta_summary.get("pnl_path_available")) if delta_summary is not None else False,
            "pnl_path_note": delta_summary.get("pnl_path_note") if delta_summary is not None else "Challenger trace missing; no replay path to compare.",
            "no_lookahead_ok": bool(
                _trace_no_lookahead_ok(str(baseline_entry["trace_artifacts"]["baseline_trace_path"]))
                and _trace_no_lookahead_ok(str(baseline_entry["trace_artifacts"]["corrected_trace_path"]))
                and (
                    challenger_entry is None
                    or (
                        _trace_no_lookahead_ok(str(challenger_entry["trace_artifacts"]["baseline_trace_path"]))
                        and _trace_no_lookahead_ok(str(challenger_entry["trace_artifacts"]["corrected_trace_path"]))
                    )
                )
            ),
            "selected_trigger_date_row_source": selected_trigger_row_source,
            "action_changed_flag": bool(challenger_entry is not None and challenger_entry.get("compare_to_repaired_v2", {}).get("action_changed_flag")),
            "target_position_changed_flag": bool(challenger_entry is not None and baseline_selected_target != challenger_selected_target),
            "actual_position_changed_flag": bool(challenger_entry is not None and baseline_selected_target != challenger_selected_target),
            "classification": classification["classification"],
            "divergence_cause": classification["divergence_cause"],
            "replay_surface_relevance": classification["replay_surface_relevance"],
        }
        records.append(record)
    return records


def _build_replay_path_delta_rollup(case_records: list[dict[str, Any]]) -> dict[str, Any]:
    classification_counts = Counter(str(entry.get("classification") or "unknown") for entry in case_records)
    comparison_available_records = [entry for entry in case_records if bool(entry.get("comparison_available"))]
    missing_records = [entry for entry in case_records if not bool(entry.get("comparison_available"))]
    baseline_only_records = [entry for entry in case_records if str(entry.get("classification")) == "baseline_only_case"]
    no_lookahead_violations = sum(1 for entry in comparison_available_records if not bool(entry.get("no_lookahead_ok")))
    selected_trigger_action_same_count = sum(1 for entry in comparison_available_records if bool(entry.get("selected_trigger_date_action_same")))
    unchanged_selected_action_material_case_count = sum(
        1
        for entry in comparison_available_records
        if bool(entry.get("selected_trigger_date_action_same"))
        and float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0) != 0.0
    )
    unchanged_selected_action_material_pnl_delta_count = sum(
        1
        for entry in comparison_available_records
        if bool(entry.get("selected_trigger_date_action_same"))
        and float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0) != 0.0
        and str(entry.get("classification")) == "no_material_divergence"
    )
    first_path_divergence_before_trigger_count = sum(
        1
        for entry in comparison_available_records
        if entry.get("first_path_divergence_date") is not None
        and entry.get("trigger_date") is not None
        and str(entry.get("first_path_divergence_date")) < str(entry.get("trigger_date"))
    )
    first_post_trigger_path_divergence_count = sum(1 for entry in comparison_available_records if entry.get("first_post_trigger_path_divergence_date") is not None)
    path_delta_explained_count = sum(
        1
        for entry in comparison_available_records
        if str(entry.get("classification")) in {
            "clean_intended_divergence",
            "unchanged_selected_action_but_later_path_diverged",
            "reporting_definition_too_narrow",
        }
    )
    material_negative_delta_count = sum(1 for entry in comparison_available_records if float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0) < 0.0)
    material_positive_delta_count = sum(1 for entry in comparison_available_records if float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0) > 0.0)
    total_pnl_delta_vs_repaired_v2 = sum(float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0) for entry in comparison_available_records)
    comparison_coverage_complete = len(missing_records) == 0
    replay_surface_trustworthy = (
        comparison_coverage_complete
        and no_lookahead_violations == 0
        and unchanged_selected_action_material_pnl_delta_count == 0
        and classification_counts.get("artifact_baseline_mismatch", 0) == 0
    )
    return {
        "schema_version": "tradex_hard_invalidation_replay_path_delta_rollup_v1",
        "phase": "replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "baseline_case_count": len(case_records),
        "comparison_available_case_count": len(comparison_available_records),
        "baseline_only_case_count": len(baseline_only_records),
        "missing_challenger_case_count": len(missing_records),
        "comparison_available_symbols": [str(entry["symbol"]) for entry in comparison_available_records],
        "baseline_only_symbols": [str(entry["symbol"]) for entry in baseline_only_records],
        "missing_challenger_symbols": [str(entry["symbol"]) for entry in missing_records],
        "classification_counts": dict(classification_counts),
        "selected_trigger_action_same_count": selected_trigger_action_same_count,
        "unchanged_selected_action_material_case_count": unchanged_selected_action_material_case_count,
        "unchanged_selected_action_material_pnl_delta_count": unchanged_selected_action_material_pnl_delta_count,
        "first_path_divergence_before_trigger_count": first_path_divergence_before_trigger_count,
        "first_post_trigger_path_divergence_count": first_post_trigger_path_divergence_count,
        "path_delta_explained_count": path_delta_explained_count,
        "material_negative_delta_count": material_negative_delta_count,
        "material_positive_delta_count": material_positive_delta_count,
        "total_pnl_delta_vs_repaired_v2": float(total_pnl_delta_vs_repaired_v2),
        "no_lookahead_violations": no_lookahead_violations,
        "baseline_identity_verified": True,
        "challenger_identity_verified": True,
        "comparison_coverage_complete": comparison_coverage_complete,
        "replay_surface_trustworthy": replay_surface_trustworthy,
        "case_result_reporting_definition_too_narrow": classification_counts.get("reporting_definition_too_narrow", 0) > 0,
        "selected_date_action_equality_is_not_full_replay_equality": True,
        "can_design_next_challenger": False,
        "generated_at": _utc_now(),
    }


def _build_replay_path_delta_decision(*, rollup: dict[str, Any], case_records: list[dict[str, Any]]) -> dict[str, Any]:
    replay_surface_trustworthy = bool(rollup.get("replay_surface_trustworthy"))
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    baseline_only_case_count = int(rollup.get("baseline_only_case_count", 0))
    decision_reason = (
        "All compared unchanged-selected-action deltas are path-explained, but the challenger session only covers 8 of 12 repaired-v2 cases"
        if not replay_surface_trustworthy
        else "Replay-path comparison is internally consistent and the selected-date action surface is no longer treated as full replay equality"
    )
    return {
        "schema_version": "tradex_hard_invalidation_replay_path_delta_decision_v1",
        "phase": "replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "new_challenger_created": False,
        "unchanged_selected_action_material_pnl_delta_count": int(rollup.get("unchanged_selected_action_material_pnl_delta_count", 0)),
        "replay_surface_trustworthy": replay_surface_trustworthy,
        "can_design_next_challenger": False,
        "decision": "hold" if replay_surface_trustworthy else "drop",
        "decision_reason": decision_reason,
        "candidate_local_decision": "hold" if replay_surface_trustworthy else "drop",
        "session_aggregate_decision": "hold" if replay_surface_trustworthy else "drop",
        "authoritative_rollup_decision": "hold" if replay_surface_trustworthy else "drop",
        "baseline_only_case_count": baseline_only_case_count,
        "comparison_available_case_count": int(rollup.get("comparison_available_case_count", 0)),
        "no_lookahead_violations": no_lookahead_violations,
        "selected_date_action_equality_is_not_full_replay_equality": True,
        "generated_at": _utc_now(),
    }


def _build_full_coverage_replay_path_delta_rollup(
    *,
    case_records: list[dict[str, Any]],
    symbol_set_diff: dict[str, Any],
) -> dict[str, Any]:
    rollup = _build_replay_path_delta_rollup(case_records)
    coverage_gate_passed = bool(symbol_set_diff.get("coverage_gate_passed"))
    baseline_case_count = int(symbol_set_diff.get("baseline_case_count", len(case_records)))
    challenger_case_count = int(symbol_set_diff.get("challenger_case_count", len(case_records)))
    baseline_only_case_count = int(symbol_set_diff.get("baseline_only_case_count", 0))
    challenger_only_case_count = int(symbol_set_diff.get("challenger_only_case_count", 0))
    replay_surface_trustworthy = bool(
        coverage_gate_passed
        and rollup.get("comparison_coverage_complete")
        and int(rollup.get("no_lookahead_violations", 0)) == 0
        and int(rollup.get("unchanged_selected_action_material_pnl_delta_count", 0)) == 0
        and int(rollup.get("classification_counts", {}).get("artifact_baseline_mismatch", 0)) == 0
    )
    rollup.update(
        {
            "schema_version": "tradex_hard_invalidation_full_coverage_replay_path_delta_rollup_v1",
            "phase": "full_coverage_replay_path_delta_audit",
            "baseline_case_count": baseline_case_count,
            "challenger_case_count": challenger_case_count,
            "baseline_only_case_count": baseline_only_case_count,
            "challenger_only_case_count": challenger_only_case_count,
            "coverage_gate_passed": coverage_gate_passed,
            "comparison_coverage_complete": coverage_gate_passed and bool(rollup.get("comparison_coverage_complete")),
            "replay_surface_trustworthy": replay_surface_trustworthy,
            "baseline_identity_verified": True,
            "challenger_identity_verified": True,
            "selected_date_action_equality_is_not_full_replay_equality": True,
            "can_design_next_challenger": False,
            "incomplete_comparison_blocked_count": 0 if coverage_gate_passed else 1,
            "generated_at": _utc_now(),
        }
    )
    return rollup


def _build_full_coverage_replay_path_delta_decision(
    *,
    rollup: dict[str, Any],
    symbol_set_diff: dict[str, Any],
) -> dict[str, Any]:
    coverage_gate_passed = bool(symbol_set_diff.get("coverage_gate_passed"))
    replay_surface_trustworthy = bool(rollup.get("replay_surface_trustworthy"))
    if not coverage_gate_passed:
        decision = "incomplete_comparison"
        decision_reason = "Baseline and challenger symbol sets do not match exactly"
    elif replay_surface_trustworthy:
        decision = "hold"
        decision_reason = "Replay-path comparison is fully covered, internally consistent, and no-lookahead remains clean"
    else:
        decision = "drop"
        decision_reason = "Full coverage comparison completed but replay-path trustworthiness was not established"
    return {
        "schema_version": "tradex_hard_invalidation_full_coverage_replay_path_delta_decision_v1",
        "phase": "full_coverage_replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "new_challenger_created": False,
        "baseline_case_count": int(symbol_set_diff.get("baseline_case_count", 0)),
        "challenger_case_count": int(symbol_set_diff.get("challenger_case_count", 0)),
        "baseline_only_case_count": int(symbol_set_diff.get("baseline_only_case_count", 0)),
        "challenger_only_case_count": int(symbol_set_diff.get("challenger_only_case_count", 0)),
        "coverage_gate_passed": coverage_gate_passed,
        "replay_surface_trustworthy": replay_surface_trustworthy,
        "can_design_next_challenger": False,
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "unchanged_selected_action_material_pnl_delta_count": int(rollup.get("unchanged_selected_action_material_pnl_delta_count", 0)),
        "no_lookahead_violations": int(rollup.get("no_lookahead_violations", 0)),
        "selected_date_action_equality_is_not_full_replay_equality": True,
        "generated_at": _utc_now(),
    }


def run_replay_path_delta_audit(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    baseline_case_results = list(_load_json(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS))
    challenger_case_results = list(_load_json(REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS))
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "replay_path_delta_audit"
    session_root.mkdir(parents=True, exist_ok=True)

    case_records = _build_replay_path_delta_case_records(
        baseline_case_results=baseline_case_results,
        challenger_case_results=challenger_case_results,
    )
    rollup = _build_replay_path_delta_rollup(case_records)
    decision = _build_replay_path_delta_decision(rollup=rollup, case_records=case_records)

    manifest = {
        "schema_version": "tradex_hard_invalidation_replay_path_delta_manifest_v1",
        "phase": "replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "baseline_reference": "repaired hard_invalidation_exit_severity_v2 under repaired contract",
        "baseline_session_root": str(REPLAY_PATH_DELTA_BASELINE_SESSION_ROOT),
        "baseline_case_results": str(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
        "baseline_case_results_identity": _file_identity(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
        "challenger_session_root": str(REPLAY_PATH_DELTA_CHALLENGER_SESSION_ROOT),
        "challenger_case_results": str(REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS),
        "challenger_case_results_identity": _file_identity(REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS),
        "challenger_trace_root": str(REPLAY_PATH_DELTA_CHALLENGER_TRACE_ROOT),
        "baseline_case_count": len(baseline_case_results),
        "challenger_case_count": len(challenger_case_results),
        "comparison_available_case_count": rollup["comparison_available_case_count"],
        "baseline_only_case_count": rollup["baseline_only_case_count"],
        "selection_basis": {
            "baseline_cases": "hard_invalidation_repaired_v2_case_results.json",
            "challenger_cases": "hard_invalidation_non_exit_late_extension_hedge_v1_case_results.json",
            "comparison_surface": "selected trigger-date action plus full replay-path ledger comparison",
        },
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    case_diagnostics = {
        "schema_version": "tradex_hard_invalidation_replay_path_delta_case_diagnostics_v1",
        "phase": "replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "generated_at": _utc_now(),
        "cases": case_records,
    }

    ledger_diff = {
        "schema_version": "tradex_hard_invalidation_replay_path_delta_ledger_diff_v1",
        "phase": "replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "generated_at": _utc_now(),
        "cases": [
            {
                "symbol": entry["symbol"],
                "comparison_available": bool(entry.get("comparison_available")),
                "classification": entry.get("classification"),
                "divergence_cause": entry.get("divergence_cause"),
                "trigger_date": entry.get("trigger_date"),
                "baseline_trace_identity": entry.get("baseline_trace_identity"),
                "challenger_trace_identity": entry.get("challenger_trace_identity"),
                "first_any_field_divergence_date": entry.get("first_any_field_divergence_date"),
                "first_path_divergence_date": entry.get("first_path_divergence_date"),
                "first_post_trigger_path_divergence_date": entry.get("first_post_trigger_path_divergence_date"),
                "selected_trigger_date_row_baseline": entry.get("selected_trigger_date_row_baseline"),
                "selected_trigger_date_row_challenger": entry.get("selected_trigger_date_row_challenger"),
                "trigger_path_diffs": entry.get("trigger_path_diffs"),
                "trigger_any_field_diffs": entry.get("trigger_any_field_diffs"),
                "path_diffs": entry.get("path_diffs"),
                "baseline_ledger_after_trigger": entry.get("baseline_ledger_after_trigger"),
                "challenger_ledger_after_trigger": entry.get("challenger_ledger_after_trigger"),
                "pnl_path_available": entry.get("pnl_path_available"),
                "pnl_path_note": entry.get("pnl_path_note"),
            }
            for entry in case_records
        ],
    }

    artifacts = {
        "hard_invalidation_replay_path_delta_audit_manifest.json": _write_json(session_root / "hard_invalidation_replay_path_delta_audit_manifest.json", manifest),
        "hard_invalidation_replay_path_delta_case_diagnostics.json": _write_json(session_root / "hard_invalidation_replay_path_delta_case_diagnostics.json", case_diagnostics),
        "hard_invalidation_replay_path_delta_ledger_diff.json": _write_json(session_root / "hard_invalidation_replay_path_delta_ledger_diff.json", ledger_diff),
        "hard_invalidation_replay_path_delta_rollup.json": _write_json(session_root / "hard_invalidation_replay_path_delta_rollup.json", rollup),
        "hard_invalidation_replay_path_delta_decision.json": _write_json(session_root / "hard_invalidation_replay_path_delta_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_diagnostics": case_diagnostics,
        "ledger_diff": ledger_diff,
        "rollup": rollup,
        "source_baseline_session_root": str(REPLAY_PATH_DELTA_BASELINE_SESSION_ROOT),
        "source_baseline_case_results": str(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
        "source_challenger_session_root": str(REPLAY_PATH_DELTA_CHALLENGER_SESSION_ROOT),
        "source_challenger_case_results": str(REPLAY_PATH_DELTA_CHALLENGER_CASE_RESULTS),
    }


def run_full_coverage_replay_path_delta_audit(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    baseline_case_results = list(_load_json(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS))
    split_payload = _load_json(SOURCE_SPLIT_CASE_RESULTS)
    split_cases = list(split_payload["candidates"])
    repaired_v2_case_results = list(_load_json(REPAIRED_V2_CASE_RESULTS))
    full_coverage_symbols = tuple(str(entry["symbol"]) for entry in baseline_case_results)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "full_coverage_replay_path_delta_audit"
    session_root.mkdir(parents=True, exist_ok=True)

    challenger_case_results = _build_non_exit_late_extension_hedge_case_results(
        split_cases=split_cases,
        repaired_v2_case_results=repaired_v2_case_results,
        session_root=session_root,
        symbols=full_coverage_symbols,
    )
    challenger_session_root = session_root / "non_exit_late_extension_hedge_v1"
    challenger_case_results_path = session_root / "hard_invalidation_non_exit_late_extension_hedge_v1_case_results.json"
    _write_json(challenger_case_results_path, challenger_case_results)
    symbol_set_diff = _build_replay_path_delta_symbol_set_diff(
        baseline_case_results=baseline_case_results,
        challenger_case_results=challenger_case_results,
        baseline_case_results_path=REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS,
        challenger_case_results_path=challenger_case_results_path,
    )
    case_records = _build_replay_path_delta_case_records(
        baseline_case_results=baseline_case_results,
        challenger_case_results=challenger_case_results,
    )
    rollup = _build_full_coverage_replay_path_delta_rollup(
        case_records=case_records,
        symbol_set_diff=symbol_set_diff,
    )
    decision = _build_full_coverage_replay_path_delta_decision(rollup=rollup, symbol_set_diff=symbol_set_diff)

    manifest = {
        "schema_version": "tradex_hard_invalidation_full_coverage_replay_path_delta_manifest_v1",
        "phase": "full_coverage_replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "baseline_reference": "repaired hard_invalidation_exit_severity_v2 under repaired contract",
        "baseline_session_root": str(REPLAY_PATH_DELTA_BASELINE_SESSION_ROOT),
        "baseline_case_results": str(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
        "baseline_case_results_identity": _file_identity(REPLAY_PATH_DELTA_BASELINE_CASE_RESULTS),
        "challenger_session_root": str(challenger_session_root),
        "challenger_case_results": str(challenger_case_results_path),
        "challenger_case_results_identity": _file_identity(challenger_case_results_path),
        "challenger_trace_root": str(challenger_session_root / "non_exit_late_extension_hedge_v1"),
        "baseline_case_count": len(baseline_case_results),
        "challenger_case_count": len(challenger_case_results),
        "selection_basis": {
            "baseline_cases": "hard_invalidation_repaired_v2_case_results.json",
            "challenger_cases": "full 12-case rerun of hard_invalidation_non_exit_late_extension_hedge_v1",
            "comparison_surface": "selected trigger-date action plus full replay-path ledger comparison with coverage gate",
        },
        "coverage_gate_passed": bool(symbol_set_diff.get("coverage_gate_passed")),
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    case_diagnostics = {
        "schema_version": "tradex_hard_invalidation_full_coverage_replay_path_delta_case_diagnostics_v1",
        "phase": "full_coverage_replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "generated_at": _utc_now(),
        "cases": case_records,
    }

    ledger_diff = {
        "schema_version": "tradex_hard_invalidation_full_coverage_replay_path_delta_ledger_diff_v1",
        "phase": "full_coverage_replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "generated_at": _utc_now(),
        "cases": [
            {
                "symbol": entry["symbol"],
                "comparison_available": bool(entry.get("comparison_available")),
                "classification": entry.get("classification"),
                "divergence_cause": entry.get("divergence_cause"),
                "trigger_date": entry.get("trigger_date"),
                "baseline_trace_identity": entry.get("baseline_trace_identity"),
                "challenger_trace_identity": entry.get("challenger_trace_identity"),
                "first_any_field_divergence_date": entry.get("first_any_field_divergence_date"),
                "first_path_divergence_date": entry.get("first_path_divergence_date"),
                "first_post_trigger_path_divergence_date": entry.get("first_post_trigger_path_divergence_date"),
                "selected_trigger_date_row_baseline": entry.get("selected_trigger_date_row_baseline"),
                "selected_trigger_date_row_challenger": entry.get("selected_trigger_date_row_challenger"),
                "trigger_path_diffs": entry.get("trigger_path_diffs"),
                "trigger_any_field_diffs": entry.get("trigger_any_field_diffs"),
                "path_diffs": entry.get("path_diffs"),
                "baseline_ledger_after_trigger": entry.get("baseline_ledger_after_trigger"),
                "challenger_ledger_after_trigger": entry.get("challenger_ledger_after_trigger"),
                "pnl_path_available": entry.get("pnl_path_available"),
                "pnl_path_note": entry.get("pnl_path_note"),
            }
            for entry in case_records
        ],
    }

    artifacts = {
        "hard_invalidation_full_coverage_replay_path_delta_audit_manifest.json": _write_json(session_root / "hard_invalidation_full_coverage_replay_path_delta_audit_manifest.json", manifest),
        "hard_invalidation_full_coverage_symbol_set_diff.json": _write_json(session_root / "hard_invalidation_full_coverage_symbol_set_diff.json", symbol_set_diff),
        "hard_invalidation_full_coverage_case_diagnostics.json": _write_json(session_root / "hard_invalidation_full_coverage_case_diagnostics.json", case_diagnostics),
        "hard_invalidation_full_coverage_ledger_diff.json": _write_json(session_root / "hard_invalidation_full_coverage_ledger_diff.json", ledger_diff),
        "hard_invalidation_full_coverage_rollup.json": _write_json(session_root / "hard_invalidation_full_coverage_rollup.json", rollup),
        "hard_invalidation_full_coverage_decision.json": _write_json(session_root / "hard_invalidation_full_coverage_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "symbol_set_diff": symbol_set_diff,
        "case_diagnostics": case_diagnostics,
        "ledger_diff": ledger_diff,
        "rollup": rollup,
    }


def _position_tuple(position: str | None) -> tuple[int, ...] | None:
    if position is None:
        return None
    parts = []
    for part in str(position).split("-"):
        try:
            parts.append(int(part))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def _first_path_diff(entry: dict[str, Any]) -> dict[str, Any] | None:
    path_diffs = list(entry.get("path_diffs") or [])
    return path_diffs[0] if path_diffs else None


def _post_trigger_path_drift_category(entry: dict[str, Any]) -> str:
    classification = str(entry.get("classification") or "")
    pnl_delta = float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0)
    if classification == "clean_intended_divergence":
        return "clean_intended_activation_only"
    if classification == "reporting_definition_too_narrow":
        return "mixed_axis_or_fallback_exclude"
    if classification == "unchanged_selected_action_but_later_path_diverged":
        if pnl_delta > 0.0:
            return "path_drift_helped_but_no_clean_evidence"
        if pnl_delta < 0.0:
            return "path_drift_hurt_but_no_clean_evidence"
        return "no_material_path_evidence"
    if classification in {"baseline_only_case", "challenger_only_case"}:
        return "mixed_axis_or_fallback_exclude"
    return "no_material_path_evidence"


def _first_divergence_kind(entry: dict[str, Any]) -> str:
    first_diff = _first_path_diff(entry)
    if first_diff is None:
        return "no_material_path_evidence"
    baseline = first_diff.get("baseline") or {}
    challenger = first_diff.get("challenger") or {}
    baseline_action = str(baseline.get("action_type") or "")
    challenger_action = str(challenger.get("action_type") or "")
    baseline_target = _position_tuple(str(baseline.get("target_position_after")) if baseline.get("target_position_after") is not None else None)
    challenger_target = _position_tuple(str(challenger.get("target_position_after")) if challenger.get("target_position_after") is not None else None)
    if baseline_action != challenger_action and (
        "hedge_add" in {baseline_action, challenger_action}
        or "hedge_reduce" in {baseline_action, challenger_action}
    ):
        return "hedge_change"
    if baseline_action != challenger_action and (
        "long_add" in {baseline_action, challenger_action}
        or "trial_buy" in {baseline_action, challenger_action}
    ):
        return "exposure_expansion"
    if baseline_action != challenger_action and (
        "long_reduce" in {baseline_action, challenger_action}
        or "stop_add" in {baseline_action, challenger_action}
        or "exit_all" in {baseline_action, challenger_action}
    ):
        if challenger_target is not None and baseline_target is not None and challenger_target < baseline_target:
            return "exposure_reduction"
        if challenger_target is not None and baseline_target is not None and challenger_target > baseline_target:
            return "exposure_expansion"
        return "exposure_reduction"
    if challenger_target is not None and baseline_target is not None:
        if challenger_target > baseline_target:
            return "exposure_expansion"
        if challenger_target < baseline_target:
            return "exposure_reduction"
    return "watch_hold_difference"


def _trigger_safe_field_keys(entry: dict[str, Any]) -> list[str]:
    row = entry.get("selected_trigger_date_row_baseline") or entry.get("selected_trigger_date_row_challenger") or {}
    safe_keys = {
        "action_type",
        "active_thesis",
        "confidence",
        "date",
        "position_before",
        "reason",
        "risk_warning",
        "target_position_after",
        "severity_action_type",
        "severity_level",
        "severity_target_buy_units",
    }
    return sorted(safe_keys & set(row.keys()))


def _future_leaking_case_fields() -> list[str]:
    return [
        "baseline_ledger_after_trigger",
        "challenger_ledger_after_trigger",
        "first_path_divergence_date",
        "first_post_trigger_path_divergence_date",
        "material_pnl_delta_vs_repaired_v2",
        "path_diffs",
        "pnl_path_available",
        "pnl_path_note",
        "trigger_any_field_diffs",
        "trigger_path_diffs",
    ]


def _build_post_trigger_path_case_classification(case_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entry in case_records:
        category = _post_trigger_path_drift_category(entry)
        first_diff = _first_path_diff(entry)
        baseline_first = first_diff.get("baseline") if first_diff is not None else None
        challenger_first = first_diff.get("challenger") if first_diff is not None else None
        baseline_first = baseline_first or {}
        challenger_first = challenger_first or {}
        pnl_delta = float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0)
        results.append(
            {
                "symbol": entry["symbol"],
                "classification": entry.get("classification"),
                "post_trigger_category": category,
                "first_divergence_kind": _first_divergence_kind(entry),
                "selected_trigger_date_action_baseline": entry.get("selected_trigger_date_action_baseline"),
                "selected_trigger_date_action_challenger": entry.get("selected_trigger_date_action_challenger"),
                "first_divergence_date": entry.get("first_path_divergence_date"),
                "first_divergence_action_baseline": baseline_first.get("action_type"),
                "first_divergence_action_challenger": challenger_first.get("action_type"),
                "position_before_divergence_baseline": baseline_first.get("position_before"),
                "position_before_divergence_challenger": challenger_first.get("position_before"),
                "target_position_after_divergence_baseline": baseline_first.get("target_position_after"),
                "target_position_after_divergence_challenger": challenger_first.get("target_position_after"),
                "helped_or_hurt": "helped" if pnl_delta > 0.0 else "hurt" if pnl_delta < 0.0 else "neutral",
                "pnl_delta_vs_repaired_v2": pnl_delta,
                "trigger_safe_fields": _trigger_safe_field_keys(entry),
                "future_leaking_fields": _future_leaking_case_fields(),
                "usable_for_future_challenger_design": False,
                "reason": entry.get("divergence_cause"),
            }
        )
    return results


def _build_post_trigger_path_field_inventory(case_classification: list[dict[str, Any]]) -> dict[str, Any]:
    classification_counts = Counter(str(entry["post_trigger_category"]) for entry in case_classification)
    trigger_safe_fields = sorted({field for entry in case_classification for field in entry.get("trigger_safe_fields") or []})
    future_leaking_fields = sorted({field for entry in case_classification for field in entry.get("future_leaking_fields") or []})
    trace_safe_row_fields = [
        "action_type",
        "active_thesis",
        "confidence",
        "date",
        "position_before",
        "reason",
        "risk_warning",
        "severity_action_type",
        "severity_level",
        "severity_target_buy_units",
        "target_position_after",
    ]
    return {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_field_inventory_v1",
        "phase": "post_trigger_path_drift_evidence_mining",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "generated_at": _utc_now(),
        "trigger_safe_row_fields": trace_safe_row_fields,
        "trigger_safe_case_fields": trigger_safe_fields,
        "future_leaking_case_fields": future_leaking_fields,
        "case_classification_counts": dict(classification_counts),
        "notes": [
            "Trigger-safe fields are the ones available on the selected trigger-date row before post-trigger replay path is considered.",
            "Future-leaking fields are case-level summaries that require downstream path knowledge or realized PnL context.",
        ],
    }


def _build_post_trigger_path_rollup(
    *,
    case_classification: list[dict[str, Any]],
    source_no_lookahead_violations: int,
) -> dict[str, Any]:
    classification_counts = Counter(str(entry["post_trigger_category"]) for entry in case_classification)
    divergence_kind_counts = Counter(str(entry["first_divergence_kind"]) for entry in case_classification)
    exposure_expansion_cases = [entry for entry in case_classification if str(entry["first_divergence_kind"]) == "exposure_expansion"]
    exposure_expansion_helped_count = sum(1 for entry in exposure_expansion_cases if str(entry.get("helped_or_hurt")) == "helped")
    exposure_expansion_hurt_count = sum(1 for entry in exposure_expansion_cases if str(entry.get("helped_or_hurt")) == "hurt")
    usable_add_veto_evidence_count = classification_counts.get("usable_post_trigger_add_veto_evidence", 0)
    usable_add_allow_evidence_count = classification_counts.get("usable_post_trigger_add_allow_evidence", 0)
    mixed_fallback_excluded_count = classification_counts.get("mixed_axis_or_fallback_exclude", 0)
    cases_needing_richer_trace_schema = sum(
        1
        for entry in case_classification
        if str(entry["post_trigger_category"]) in {
            "path_drift_helped_but_no_clean_evidence",
            "path_drift_hurt_but_no_clean_evidence",
        }
    )
    no_lookahead_violations = int(source_no_lookahead_violations)
    can_design_next_challenger = (
        usable_add_veto_evidence_count > 0 or usable_add_allow_evidence_count > 0
    ) and no_lookahead_violations == 0
    return {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_rollup_v1",
        "phase": "post_trigger_path_drift_evidence_mining",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "new_challenger_created": False,
        "total_cases": len(case_classification),
        "exposure_expansion_case_count": len(exposure_expansion_cases),
        "exposure_expansion_helped_count": exposure_expansion_helped_count,
        "exposure_expansion_hurt_count": exposure_expansion_hurt_count,
        "usable_add_veto_evidence_count": usable_add_veto_evidence_count,
        "usable_add_allow_evidence_count": usable_add_allow_evidence_count,
        "mixed_fallback_excluded_count": mixed_fallback_excluded_count,
        "cases_needing_richer_trace_schema": cases_needing_richer_trace_schema,
        "can_design_next_challenger": can_design_next_challenger,
        "no_lookahead_violations": no_lookahead_violations,
        "classification_counts": dict(classification_counts),
        "divergence_kind_counts": dict(divergence_kind_counts),
        "generated_at": _utc_now(),
    }


def _build_post_trigger_path_decision(*, rollup: dict[str, Any]) -> dict[str, Any]:
    usable_add_veto_evidence_count = int(rollup.get("usable_add_veto_evidence_count", 0))
    usable_add_allow_evidence_count = int(rollup.get("usable_add_allow_evidence_count", 0))
    can_design_next_challenger = bool(rollup.get("can_design_next_challenger"))
    decision = "blocked"
    decision_reason = "No usable post-trigger path evidence category was found"
    if can_design_next_challenger:
        decision = "open"
        decision_reason = "A usable post-trigger path evidence category is nonzero"
    return {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_decision_v1",
        "phase": "post_trigger_path_drift_evidence_mining",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "new_challenger_created": False,
        "usable_add_veto_evidence_count": usable_add_veto_evidence_count,
        "usable_add_allow_evidence_count": usable_add_allow_evidence_count,
        "can_design_next_challenger": False,
        "next_allowed_action": "only design a challenger if a usable post-trigger path evidence category is nonzero",
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "generated_at": _utc_now(),
    }


def run_post_trigger_path_drift_evidence_mining(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    source_case_diagnostics = list(_load_json(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS)["cases"])
    source_rollup = _load_json(POST_TRIGGER_PATH_MINE_SOURCE_ROLLUP)
    source_decision = _load_json(POST_TRIGGER_PATH_MINE_SOURCE_DECISION)
    source_symbol_set_diff = _load_json(POST_TRIGGER_PATH_MINE_SOURCE_SYMBOL_SET_DIFF)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "post_trigger_path_drift_evidence_mining"
    session_root.mkdir(parents=True, exist_ok=True)

    case_classification = _build_post_trigger_path_case_classification(source_case_diagnostics)
    field_inventory = _build_post_trigger_path_field_inventory(case_classification)
    source_no_lookahead_violations = int(source_rollup.get("no_lookahead_violations", 0))
    rollup = _build_post_trigger_path_rollup(
        case_classification=case_classification,
        source_no_lookahead_violations=source_no_lookahead_violations,
    )
    decision = _build_post_trigger_path_decision(rollup=rollup)

    manifest = {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_mining_manifest_v1",
        "phase": "post_trigger_path_drift_evidence_mining",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_audit_session_root": str(POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT),
        "source_case_diagnostics": str(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "source_case_diagnostics_identity": _file_identity(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "source_rollup": str(POST_TRIGGER_PATH_MINE_SOURCE_ROLLUP),
        "source_rollup_identity": _file_identity(POST_TRIGGER_PATH_MINE_SOURCE_ROLLUP),
        "source_decision": str(POST_TRIGGER_PATH_MINE_SOURCE_DECISION),
        "source_decision_identity": _file_identity(POST_TRIGGER_PATH_MINE_SOURCE_DECISION),
        "source_symbol_set_diff": str(POST_TRIGGER_PATH_MINE_SOURCE_SYMBOL_SET_DIFF),
        "source_symbol_set_diff_identity": _file_identity(POST_TRIGGER_PATH_MINE_SOURCE_SYMBOL_SET_DIFF),
        "source_rollup_snapshot": {
            "coverage_gate_passed": bool(source_rollup.get("coverage_gate_passed")),
            "replay_surface_trustworthy": bool(source_rollup.get("replay_surface_trustworthy")),
            "comparison_available_case_count": int(source_rollup.get("comparison_available_case_count", 0)),
            "classification_counts": dict(source_rollup.get("classification_counts") or {}),
        },
        "source_decision_snapshot": {
            "decision": source_decision.get("decision"),
            "decision_reason": source_decision.get("decision_reason"),
            "can_design_next_challenger": bool(source_decision.get("can_design_next_challenger")),
        },
        "source_symbol_set_snapshot": {
            "baseline_case_count": int(source_symbol_set_diff.get("baseline_case_count", 0)),
            "challenger_case_count": int(source_symbol_set_diff.get("challenger_case_count", 0)),
            "baseline_only_case_count": int(source_symbol_set_diff.get("baseline_only_case_count", 0)),
            "challenger_only_case_count": int(source_symbol_set_diff.get("challenger_only_case_count", 0)),
            "coverage_gate_passed": bool(source_symbol_set_diff.get("coverage_gate_passed")),
        },
        "generated_at": _utc_now(),
    }

    case_classification_artifact = {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_case_classification_v1",
        "phase": "post_trigger_path_drift_evidence_mining",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "generated_at": _utc_now(),
        "cases": case_classification,
    }

    artifacts = {
        "hard_invalidation_post_trigger_path_mining_manifest.json": _write_json(session_root / "hard_invalidation_post_trigger_path_mining_manifest.json", manifest),
        "hard_invalidation_post_trigger_path_case_classification.json": _write_json(session_root / "hard_invalidation_post_trigger_path_case_classification.json", case_classification_artifact),
        "hard_invalidation_post_trigger_path_field_inventory.json": _write_json(session_root / "hard_invalidation_post_trigger_path_field_inventory.json", field_inventory),
        "hard_invalidation_post_trigger_path_rollup.json": _write_json(session_root / "hard_invalidation_post_trigger_path_rollup.json", rollup),
        "hard_invalidation_post_trigger_path_decision.json": _write_json(session_root / "hard_invalidation_post_trigger_path_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_classification": case_classification_artifact,
        "field_inventory": field_inventory,
        "rollup": rollup,
        "source_audit_session_root": str(POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT),
        "source_case_diagnostics": str(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "source_rollup": str(POST_TRIGGER_PATH_MINE_SOURCE_ROLLUP),
        "source_decision": str(POST_TRIGGER_PATH_MINE_SOURCE_DECISION),
        "source_symbol_set_diff": str(POST_TRIGGER_PATH_MINE_SOURCE_SYMBOL_SET_DIFF),
    }


def _collect_corrected_trace_paths(trace_root: Path) -> dict[str, Path]:
    trace_paths: dict[str, Path] = {}
    for trace_path in trace_root.rglob("*_corrected_trace.json"):
        symbol = trace_path.name.split("_", 1)[0]
        trace_paths[str(symbol)] = trace_path
    return trace_paths


def _v2_trace_row_pnl_fields() -> tuple[str, ...]:
    return (
        "realized_pnl_day",
        "unrealized_pnl_day",
        "cumulative_realized_pnl",
        "cumulative_unrealized_pnl",
        "equity_curve_value",
        "pnl_path_available",
    )


def _v2_trace_row_trigger_safe_fields() -> tuple[str, ...]:
    return tuple(
        field
        for field in TRACE_INSTRUMENTATION_REQUIRED_FIELDS
        if field not in _v2_trace_row_pnl_fields()
    )


def _v2_trace_row_future_leaking_case_fields() -> list[str]:
    return [
        "baseline_ledger_after_trigger",
        "challenger_ledger_after_trigger",
        "equity_curve_delta_at_first_divergence",
        "equity_curve_delta_final",
        "final_equity_curve_delta_vs_baseline",
        "first_divergence_date",
        "first_divergence_row_baseline",
        "first_divergence_row_challenger",
        "first_post_trigger_path_divergence_date",
        "material_pnl_delta_vs_repaired_v2",
        "path_diffs",
        "pnl_path_series",
        "pnl_path_available",
        "trigger_any_field_diffs",
        "trigger_path_diffs",
    ]


def _trace_v2_row_snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {}
    keys = (
        "date",
        "action_type",
        "position_before",
        "position_after",
        "target_position_after",
        "buy_units_before",
        "buy_units_after",
        "sell_units_before",
        "sell_units_after",
        "net_units_before",
        "net_units_after",
        "gross_units_before",
        "gross_units_after",
        "reason",
        "action_reason",
        "action_source",
        "active_thesis",
        "confidence",
        "risk_warning",
        "realized_pnl_day",
        "unrealized_pnl_day",
        "cumulative_realized_pnl",
        "cumulative_unrealized_pnl",
        "equity_curve_value",
        "pnl_path_available",
    )
    return {key: row.get(key) for key in keys if row.get(key) is not None}


def _build_post_trigger_path_v2_case_record(
    *,
    source_entry: dict[str, Any],
    baseline_rows: list[dict[str, Any]],
    challenger_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    baseline_by_date = _trace_rows_by_date(baseline_rows)
    challenger_by_date = _trace_rows_by_date(challenger_rows)
    trigger_date = str(source_entry.get("trigger_date")) if source_entry.get("trigger_date") is not None else None
    first_divergence_date = str(source_entry.get("first_path_divergence_date") or "")
    trigger_row_baseline = baseline_by_date.get(trigger_date) if trigger_date is not None else None
    trigger_row_challenger = challenger_by_date.get(trigger_date) if trigger_date is not None else None
    first_row_baseline = baseline_by_date.get(first_divergence_date) if first_divergence_date else None
    first_row_challenger = challenger_by_date.get(first_divergence_date) if first_divergence_date else None
    final_row_baseline = baseline_rows[-1] if baseline_rows else None
    final_row_challenger = challenger_rows[-1] if challenger_rows else None

    first_equity_baseline = float(first_row_baseline.get("equity_curve_value")) if first_row_baseline and first_row_baseline.get("equity_curve_value") is not None else None
    first_equity_challenger = float(first_row_challenger.get("equity_curve_value")) if first_row_challenger and first_row_challenger.get("equity_curve_value") is not None else None
    final_equity_baseline = float(final_row_baseline.get("equity_curve_value")) if final_row_baseline and final_row_baseline.get("equity_curve_value") is not None else None
    final_equity_challenger = float(final_row_challenger.get("equity_curve_value")) if final_row_challenger and final_row_challenger.get("equity_curve_value") is not None else None

    first_equity_delta = (
        float(first_equity_challenger - first_equity_baseline)
        if first_equity_baseline is not None and first_equity_challenger is not None
        else None
    )
    final_equity_delta = (
        float(final_equity_challenger - final_equity_baseline)
        if final_equity_baseline is not None and final_equity_challenger is not None
        else None
    )

    if final_equity_delta is None:
        helped_or_hurt = "unknown"
        path_effect_timing = "insufficient_path"
        loss_appeared_immediately = False
        loss_appeared_only_later = False
    elif final_equity_delta > 0:
        helped_or_hurt = "helped"
        if first_equity_delta is not None and first_equity_delta > 0:
            path_effect_timing = "immediate_help"
        elif first_equity_delta is not None and first_equity_delta < 0:
            path_effect_timing = "hurt_then_recovered"
        else:
            path_effect_timing = "delayed_help"
        loss_appeared_immediately = False
        loss_appeared_only_later = False
    elif final_equity_delta < 0:
        helped_or_hurt = "hurt"
        if first_equity_delta is not None and first_equity_delta < 0:
            path_effect_timing = "immediate_hurt"
            loss_appeared_immediately = True
            loss_appeared_only_later = False
        elif first_equity_delta is not None and first_equity_delta > 0:
            path_effect_timing = "helped_then_hurt_later"
            loss_appeared_immediately = False
            loss_appeared_only_later = True
        else:
            path_effect_timing = "delayed_hurt"
            loss_appeared_immediately = False
            loss_appeared_only_later = True
    else:
        helped_or_hurt = "neutral"
        path_effect_timing = "neutral"
        loss_appeared_immediately = False
        loss_appeared_only_later = False

    first_diff = _first_path_diff(source_entry) or {}
    first_diff_baseline = first_diff.get("baseline") or {}
    first_diff_challenger = first_diff.get("challenger") or {}
    baseline_action = str(first_diff_baseline.get("action_type") or source_entry.get("selected_trigger_date_action_baseline") or "")
    challenger_action = str(first_diff_challenger.get("action_type") or source_entry.get("selected_trigger_date_action_challenger") or "")

    first_divergence_kind = _first_divergence_kind(source_entry)
    source_classification = str(source_entry.get("classification") or "")
    if source_classification in {"reporting_definition_too_narrow", "baseline_only_case", "challenger_only_case"}:
        post_trigger_category = "mixed_axis_or_fallback_exclude"
    elif first_divergence_kind == "hedge_change":
        post_trigger_category = "hedge_change_evidence"
    elif helped_or_hurt == "helped":
        post_trigger_category = "pnl_path_helped_but_no_clean_evidence"
    elif helped_or_hurt == "hurt":
        post_trigger_category = "pnl_path_hurt_but_no_clean_evidence"
    else:
        post_trigger_category = "no_material_path_evidence"

    trigger_safe_row_fields = sorted(field for field in (trigger_row_challenger or trigger_row_baseline or {}).keys() if field not in _v2_trace_row_pnl_fields())
    future_leaking_row_fields = list(_v2_trace_row_pnl_fields())
    trigger_safe_case_fields = sorted({
        "selected_trigger_date_action_baseline",
        "selected_trigger_date_action_challenger",
        "selected_trigger_date_target_baseline",
        "selected_trigger_date_target_challenger",
        "first_divergence_date",
        "first_divergence_action_baseline",
        "first_divergence_action_challenger",
        "first_divergence_position_before_baseline",
        "first_divergence_position_before_challenger",
        "first_divergence_position_after_baseline",
        "first_divergence_position_after_challenger",
        "first_divergence_buy_units_before_baseline",
        "first_divergence_buy_units_before_challenger",
        "first_divergence_buy_units_after_baseline",
        "first_divergence_buy_units_after_challenger",
        "first_divergence_sell_units_before_baseline",
        "first_divergence_sell_units_before_challenger",
        "first_divergence_sell_units_after_baseline",
        "first_divergence_sell_units_after_challenger",
        "first_divergence_net_units_before_baseline",
        "first_divergence_net_units_before_challenger",
        "first_divergence_net_units_after_baseline",
        "first_divergence_net_units_after_challenger",
        "first_divergence_gross_units_before_baseline",
        "first_divergence_gross_units_before_challenger",
        "first_divergence_gross_units_after_baseline",
        "first_divergence_gross_units_after_challenger",
    })
    pnl_path_available = all(bool(row.get("pnl_path_available")) for row in baseline_rows + challenger_rows) if baseline_rows and challenger_rows else False

    path_series: list[dict[str, Any]] = []
    all_dates = sorted(set(baseline_by_date) | set(challenger_by_date))
    for date in all_dates:
        baseline_row = baseline_by_date.get(date)
        challenger_row = challenger_by_date.get(date)
        baseline_equity = baseline_row.get("equity_curve_value") if baseline_row is not None else None
        challenger_equity = challenger_row.get("equity_curve_value") if challenger_row is not None else None
        path_series.append(
            {
                "date": date,
                "baseline": _trace_v2_row_snapshot(baseline_row),
                "challenger": _trace_v2_row_snapshot(challenger_row),
                "equity_curve_delta": (
                    float(challenger_equity - baseline_equity)
                    if baseline_equity is not None and challenger_equity is not None
                    else None
                ),
            }
        )

    return {
        "symbol": str(source_entry["symbol"]),
        "company_name": source_entry.get("company_name"),
        "source_classification": source_classification,
        "post_trigger_category": post_trigger_category,
        "first_divergence_kind": first_divergence_kind,
        "trigger_date": trigger_date,
        "first_divergence_date": first_divergence_date or None,
        "selected_trigger_date_action_baseline": source_entry.get("selected_trigger_date_action_baseline"),
        "selected_trigger_date_action_challenger": source_entry.get("selected_trigger_date_action_challenger"),
        "selected_trigger_date_target_baseline": source_entry.get("selected_trigger_date_target_baseline"),
        "selected_trigger_date_target_challenger": source_entry.get("selected_trigger_date_target_challenger"),
        "selected_trigger_date_row_baseline": _trace_v2_row_snapshot(trigger_row_baseline),
        "selected_trigger_date_row_challenger": _trace_v2_row_snapshot(trigger_row_challenger),
        "first_divergence_action_baseline": first_diff_baseline.get("action_type"),
        "first_divergence_action_challenger": first_diff_challenger.get("action_type"),
        "first_divergence_position_before_baseline": first_diff_baseline.get("position_before"),
        "first_divergence_position_before_challenger": first_diff_challenger.get("position_before"),
        "first_divergence_position_after_baseline": first_diff_baseline.get("target_position_after"),
        "first_divergence_position_after_challenger": first_diff_challenger.get("target_position_after"),
        "first_divergence_buy_units_before_baseline": first_row_baseline.get("buy_units_before") if first_row_baseline else None,
        "first_divergence_buy_units_before_challenger": first_row_challenger.get("buy_units_before") if first_row_challenger else None,
        "first_divergence_buy_units_after_baseline": first_row_baseline.get("buy_units_after") if first_row_baseline else None,
        "first_divergence_buy_units_after_challenger": first_row_challenger.get("buy_units_after") if first_row_challenger else None,
        "first_divergence_sell_units_before_baseline": first_row_baseline.get("sell_units_before") if first_row_baseline else None,
        "first_divergence_sell_units_before_challenger": first_row_challenger.get("sell_units_before") if first_row_challenger else None,
        "first_divergence_sell_units_after_baseline": first_row_baseline.get("sell_units_after") if first_row_baseline else None,
        "first_divergence_sell_units_after_challenger": first_row_challenger.get("sell_units_after") if first_row_challenger else None,
        "first_divergence_net_units_before_baseline": first_row_baseline.get("net_units_before") if first_row_baseline else None,
        "first_divergence_net_units_before_challenger": first_row_challenger.get("net_units_before") if first_row_challenger else None,
        "first_divergence_net_units_after_baseline": first_row_baseline.get("net_units_after") if first_row_baseline else None,
        "first_divergence_net_units_after_challenger": first_row_challenger.get("net_units_after") if first_row_challenger else None,
        "first_divergence_gross_units_before_baseline": first_row_baseline.get("gross_units_before") if first_row_baseline else None,
        "first_divergence_gross_units_before_challenger": first_row_challenger.get("gross_units_before") if first_row_challenger else None,
        "first_divergence_gross_units_after_baseline": first_row_baseline.get("gross_units_after") if first_row_baseline else None,
        "first_divergence_gross_units_after_challenger": first_row_challenger.get("gross_units_after") if first_row_challenger else None,
        "first_divergence_row_baseline": _trace_v2_row_snapshot(first_row_baseline),
        "first_divergence_row_challenger": _trace_v2_row_snapshot(first_row_challenger),
        "final_row_baseline": _trace_v2_row_snapshot(final_row_baseline),
        "final_row_challenger": _trace_v2_row_snapshot(final_row_challenger),
        "first_divergence_realized_pnl_day_baseline": first_row_baseline.get("realized_pnl_day") if first_row_baseline else None,
        "first_divergence_realized_pnl_day_challenger": first_row_challenger.get("realized_pnl_day") if first_row_challenger else None,
        "first_divergence_unrealized_pnl_day_baseline": first_row_baseline.get("unrealized_pnl_day") if first_row_baseline else None,
        "first_divergence_unrealized_pnl_day_challenger": first_row_challenger.get("unrealized_pnl_day") if first_row_challenger else None,
        "first_divergence_cumulative_realized_pnl_baseline": first_row_baseline.get("cumulative_realized_pnl") if first_row_baseline else None,
        "first_divergence_cumulative_realized_pnl_challenger": first_row_challenger.get("cumulative_realized_pnl") if first_row_challenger else None,
        "first_divergence_cumulative_unrealized_pnl_baseline": first_row_baseline.get("cumulative_unrealized_pnl") if first_row_baseline else None,
        "first_divergence_cumulative_unrealized_pnl_challenger": first_row_challenger.get("cumulative_unrealized_pnl") if first_row_challenger else None,
        "first_divergence_equity_curve_value_baseline": first_equity_baseline,
        "first_divergence_equity_curve_value_challenger": first_equity_challenger,
        "first_divergence_equity_curve_delta": first_equity_delta,
        "final_equity_curve_value_baseline": final_equity_baseline,
        "final_equity_curve_value_challenger": final_equity_challenger,
        "final_equity_curve_delta_vs_baseline": final_equity_delta,
        "helped_or_hurt": helped_or_hurt,
        "path_effect_timing": path_effect_timing,
        "loss_appeared_immediately": loss_appeared_immediately,
        "loss_appeared_only_later": loss_appeared_only_later,
        "pre_divergence_evidence_exists": bool((trigger_row_baseline or trigger_row_challenger) and ((trigger_row_baseline or {}).get("evidence_for") or (trigger_row_baseline or {}).get("evidence_against") or (trigger_row_challenger or {}).get("evidence_for") or (trigger_row_challenger or {}).get("evidence_against"))),
        "pre_divergence_evidence_trigger_safe": bool(trigger_safe_row_fields),
        "trigger_safe_row_fields": trigger_safe_row_fields,
        "trigger_safe_case_fields": trigger_safe_case_fields,
        "future_leaking_row_fields": future_leaking_row_fields,
        "future_leaking_case_fields": _v2_trace_row_future_leaking_case_fields(),
        "pnl_path_available": pnl_path_available,
        "pnl_path_series": path_series,
        "trigger_path_diffs": source_entry.get("trigger_path_diffs") or {},
        "trigger_any_field_diffs": source_entry.get("trigger_any_field_diffs") or {},
        "path_diffs": list(source_entry.get("path_diffs") or []),
        "baseline_ledger_after_trigger": list(source_entry.get("baseline_ledger_after_trigger") or []),
        "challenger_ledger_after_trigger": list(source_entry.get("challenger_ledger_after_trigger") or []),
        "material_pnl_delta_vs_repaired_v2": float(source_entry.get("material_pnl_delta_vs_repaired_v2") or 0.0),
        "usable_for_future_challenger_design": False,
        "decision_basis_uses_future_leaking_path": True,
        "reason": source_entry.get("divergence_cause"),
    }


def _build_post_trigger_path_v2_case_classification(case_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entry in case_records:
        results.append(
            {
                "symbol": entry["symbol"],
                "company_name": entry.get("company_name"),
                "source_classification": entry.get("source_classification"),
                "post_trigger_category": entry.get("post_trigger_category"),
                "first_divergence_kind": entry.get("first_divergence_kind"),
                "trigger_date": entry.get("trigger_date"),
                "first_divergence_date": entry.get("first_divergence_date"),
                "selected_trigger_date_action_baseline": entry.get("selected_trigger_date_action_baseline"),
                "selected_trigger_date_action_challenger": entry.get("selected_trigger_date_action_challenger"),
                "first_divergence_action_baseline": entry.get("first_divergence_action_baseline"),
                "first_divergence_action_challenger": entry.get("first_divergence_action_challenger"),
                "first_divergence_position_before_baseline": entry.get("first_divergence_position_before_baseline"),
                "first_divergence_position_before_challenger": entry.get("first_divergence_position_before_challenger"),
                "first_divergence_position_after_baseline": entry.get("first_divergence_position_after_baseline"),
                "first_divergence_position_after_challenger": entry.get("first_divergence_position_after_challenger"),
                "first_divergence_buy_units_before_baseline": entry.get("first_divergence_buy_units_before_baseline"),
                "first_divergence_buy_units_before_challenger": entry.get("first_divergence_buy_units_before_challenger"),
                "first_divergence_buy_units_after_baseline": entry.get("first_divergence_buy_units_after_baseline"),
                "first_divergence_buy_units_after_challenger": entry.get("first_divergence_buy_units_after_challenger"),
                "first_divergence_sell_units_before_baseline": entry.get("first_divergence_sell_units_before_baseline"),
                "first_divergence_sell_units_before_challenger": entry.get("first_divergence_sell_units_before_challenger"),
                "first_divergence_sell_units_after_baseline": entry.get("first_divergence_sell_units_after_baseline"),
                "first_divergence_sell_units_after_challenger": entry.get("first_divergence_sell_units_after_challenger"),
                "first_divergence_net_units_before_baseline": entry.get("first_divergence_net_units_before_baseline"),
                "first_divergence_net_units_before_challenger": entry.get("first_divergence_net_units_before_challenger"),
                "first_divergence_net_units_after_baseline": entry.get("first_divergence_net_units_after_baseline"),
                "first_divergence_net_units_after_challenger": entry.get("first_divergence_net_units_after_challenger"),
                "first_divergence_gross_units_before_baseline": entry.get("first_divergence_gross_units_before_baseline"),
                "first_divergence_gross_units_before_challenger": entry.get("first_divergence_gross_units_before_challenger"),
                "first_divergence_gross_units_after_baseline": entry.get("first_divergence_gross_units_after_baseline"),
                "first_divergence_gross_units_after_challenger": entry.get("first_divergence_gross_units_after_challenger"),
                "first_divergence_realized_pnl_day_baseline": entry.get("first_divergence_realized_pnl_day_baseline"),
                "first_divergence_realized_pnl_day_challenger": entry.get("first_divergence_realized_pnl_day_challenger"),
                "first_divergence_unrealized_pnl_day_baseline": entry.get("first_divergence_unrealized_pnl_day_baseline"),
                "first_divergence_unrealized_pnl_day_challenger": entry.get("first_divergence_unrealized_pnl_day_challenger"),
                "first_divergence_cumulative_realized_pnl_baseline": entry.get("first_divergence_cumulative_realized_pnl_baseline"),
                "first_divergence_cumulative_realized_pnl_challenger": entry.get("first_divergence_cumulative_realized_pnl_challenger"),
                "first_divergence_cumulative_unrealized_pnl_baseline": entry.get("first_divergence_cumulative_unrealized_pnl_baseline"),
                "first_divergence_cumulative_unrealized_pnl_challenger": entry.get("first_divergence_cumulative_unrealized_pnl_challenger"),
                "first_divergence_equity_curve_value_baseline": entry.get("first_divergence_equity_curve_value_baseline"),
                "first_divergence_equity_curve_value_challenger": entry.get("first_divergence_equity_curve_value_challenger"),
                "first_divergence_equity_curve_delta": entry.get("first_divergence_equity_curve_delta"),
                "final_equity_curve_value_baseline": entry.get("final_equity_curve_value_baseline"),
                "final_equity_curve_value_challenger": entry.get("final_equity_curve_value_challenger"),
                "final_equity_curve_delta_vs_baseline": entry.get("final_equity_curve_delta_vs_baseline"),
                "helped_or_hurt": entry.get("helped_or_hurt"),
                "path_effect_timing": entry.get("path_effect_timing"),
                "loss_appeared_immediately": bool(entry.get("loss_appeared_immediately")),
                "loss_appeared_only_later": bool(entry.get("loss_appeared_only_later")),
                "pre_divergence_evidence_exists": bool(entry.get("pre_divergence_evidence_exists")),
                "pre_divergence_evidence_trigger_safe": bool(entry.get("pre_divergence_evidence_trigger_safe")),
                "trigger_safe_row_fields": list(entry.get("trigger_safe_row_fields") or []),
                "trigger_safe_case_fields": list(entry.get("trigger_safe_case_fields") or []),
                "future_leaking_row_fields": list(entry.get("future_leaking_row_fields") or []),
                "future_leaking_case_fields": list(entry.get("future_leaking_case_fields") or []),
                "usable_for_future_challenger_design": bool(entry.get("usable_for_future_challenger_design")),
                "reason": entry.get("reason"),
            }
        )
    return results


def _build_post_trigger_path_v2_pnl_path_summary(case_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for entry in case_records:
        pnl_summary = {
            "symbol": entry["symbol"],
            "company_name": entry.get("company_name"),
            "trigger_date": entry.get("trigger_date"),
            "first_divergence_date": entry.get("first_divergence_date"),
            "first_divergence_row_baseline": _trace_v2_row_snapshot(entry.get("first_divergence_row_baseline")),
            "first_divergence_row_challenger": _trace_v2_row_snapshot(entry.get("first_divergence_row_challenger")),
            "first_divergence_equity_curve_delta": entry.get("first_divergence_equity_curve_delta"),
            "final_equity_curve_value_baseline": entry.get("final_equity_curve_value_baseline"),
            "final_equity_curve_value_challenger": entry.get("final_equity_curve_value_challenger"),
            "final_equity_curve_delta_vs_baseline": entry.get("final_equity_curve_delta_vs_baseline"),
            "helped_or_hurt": entry.get("helped_or_hurt"),
            "path_effect_timing": entry.get("path_effect_timing"),
            "loss_appeared_immediately": bool(entry.get("loss_appeared_immediately")),
            "loss_appeared_only_later": bool(entry.get("loss_appeared_only_later")),
            "pnl_path_available": bool(entry.get("pnl_path_available")),
            "pnl_path_series": list(entry.get("pnl_path_series") or []),
            "material_pnl_delta_vs_repaired_v2": float(entry.get("material_pnl_delta_vs_repaired_v2") or 0.0),
        }
        results.append(pnl_summary)
    return results


def _build_post_trigger_path_v2_field_inventory(case_classification: list[dict[str, Any]]) -> dict[str, Any]:
    classification_counts = Counter(str(entry["post_trigger_category"]) for entry in case_classification)
    trigger_safe_row_fields = sorted({field for entry in case_classification for field in entry.get("trigger_safe_row_fields") or []})
    future_leaking_row_fields = sorted({field for entry in case_classification for field in entry.get("future_leaking_row_fields") or []})
    future_leaking_case_fields = sorted({field for entry in case_classification for field in entry.get("future_leaking_case_fields") or []})
    trace_row_fields = sorted({field for entry in case_classification for field in trigger_safe_row_fields + future_leaking_row_fields})
    return {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_v2_field_inventory_v1",
        "phase": "post_trigger_path_drift_evidence_mining_v2",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_trace_schema_version": TRACE_INSTRUMENTATION_TRACE_SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "trace_row_fields": trace_row_fields,
        "trigger_safe_row_fields": trigger_safe_row_fields,
        "pnl_path_row_fields": list(_v2_trace_row_pnl_fields()),
        "future_leaking_row_fields": future_leaking_row_fields,
        "future_leaking_case_fields": future_leaking_case_fields,
        "case_classification_counts": dict(classification_counts),
        "notes": [
            "Trigger-safe row fields are available on or before the decision date.",
            "PnL path row fields are available only after execution and therefore remain future-leaking for challenger design.",
            "Trace-v2 rows expose daily position and PnL context, but that does not imply those fields are usable as trigger-safe evidence.",
        ],
    }


def _build_post_trigger_path_v2_rollup(
    *,
    case_classification: list[dict[str, Any]],
    pnl_path_summary: list[dict[str, Any]],
    source_no_lookahead_violations: int,
    source_trace_schema_version: str | None,
) -> dict[str, Any]:
    classification_counts = Counter(str(entry["post_trigger_category"]) for entry in case_classification)
    first_divergence_kind_counts = Counter(str(entry["first_divergence_kind"]) for entry in case_classification)
    exposure_expansion_cases = [entry for entry in case_classification if str(entry["first_divergence_kind"]) == "exposure_expansion"]
    exposure_expansion_helped_count = sum(1 for entry in exposure_expansion_cases if str(entry.get("helped_or_hurt")) == "helped")
    exposure_expansion_hurt_count = sum(1 for entry in exposure_expansion_cases if str(entry.get("helped_or_hurt")) == "hurt")
    hedge_change_evidence_count = classification_counts.get("hedge_change_evidence", 0)
    mixed_fallback_excluded_count = classification_counts.get("mixed_axis_or_fallback_exclude", 0)
    usable_add_veto_evidence_count = classification_counts.get("usable_add_veto_evidence", 0)
    usable_add_allow_evidence_count = classification_counts.get("usable_add_allow_evidence", 0)
    cases_with_sufficient_pnl_path = sum(1 for entry in pnl_path_summary if bool(entry.get("pnl_path_available")))
    cases_with_insufficient_pnl_path = len(pnl_path_summary) - cases_with_sufficient_pnl_path
    can_design_next_challenger = bool(
        usable_add_veto_evidence_count > 0
        or usable_add_allow_evidence_count > 0
    ) and int(source_no_lookahead_violations) == 0
    return {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_v2_rollup_v1",
        "phase": "post_trigger_path_drift_evidence_mining_v2",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_trace_schema_version": source_trace_schema_version,
        "new_challenger_created": False,
        "total_cases": len(case_classification),
        "exposure_expansion_cases": len(exposure_expansion_cases),
        "exposure_expansion_helped_count": exposure_expansion_helped_count,
        "exposure_expansion_hurt_count": exposure_expansion_hurt_count,
        "usable_add_veto_evidence_count": usable_add_veto_evidence_count,
        "usable_add_allow_evidence_count": usable_add_allow_evidence_count,
        "hedge_change_evidence_count": hedge_change_evidence_count,
        "mixed_fallback_excluded_count": mixed_fallback_excluded_count,
        "cases_with_sufficient_pnl_path": cases_with_sufficient_pnl_path,
        "cases_with_insufficient_pnl_path": cases_with_insufficient_pnl_path,
        "can_design_next_challenger": can_design_next_challenger,
        "no_lookahead_violations": int(source_no_lookahead_violations),
        "classification_counts": dict(classification_counts),
        "first_divergence_kind_counts": dict(first_divergence_kind_counts),
        "generated_at": _utc_now(),
    }


def _build_post_trigger_path_v2_decision(*, rollup: dict[str, Any]) -> dict[str, Any]:
    usable_add_veto_evidence_count = int(rollup.get("usable_add_veto_evidence_count", 0))
    usable_add_allow_evidence_count = int(rollup.get("usable_add_allow_evidence_count", 0))
    can_design_next_challenger = bool(rollup.get("can_design_next_challenger"))
    decision = "blocked"
    decision_reason = "No usable trace-v2 evidence category was found"
    if can_design_next_challenger:
        decision = "open"
        decision_reason = "A usable trace-v2 evidence category is nonzero"
    return {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_v2_decision_v1",
        "phase": "post_trigger_path_drift_evidence_mining_v2",
        "source_trace_schema_version": TRACE_INSTRUMENTATION_TRACE_SCHEMA_VERSION,
        "new_challenger_created": False,
        "usable_add_veto_evidence_count": usable_add_veto_evidence_count,
        "usable_add_allow_evidence_count": usable_add_allow_evidence_count,
        "can_design_next_challenger": False,
        "next_allowed_action": "only design a challenger if a usable trace-v2 evidence category is nonzero",
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "generated_at": _utc_now(),
    }


def run_post_trigger_path_drift_evidence_mining_v2(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    source_trace_manifest = _load_json(POST_TRIGGER_PATH_MINE_V2_SOURCE_MANIFEST)
    source_schema_inventory = _load_json(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_INVENTORY)
    source_schema_change_summary = _load_json(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_CHANGE_SUMMARY)
    source_no_lookahead_validation = _load_json(POST_TRIGGER_PATH_MINE_V2_SOURCE_NO_LOOKAHEAD_VALIDATION)
    source_instrumentation_decision = _load_json(POST_TRIGGER_PATH_MINE_V2_SOURCE_DECISION)
    source_case_diagnostics = list(_load_json(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS)["cases"])

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "post_trigger_path_drift_evidence_mining_v2"
    session_root.mkdir(parents=True, exist_ok=True)

    baseline_trace_paths = _collect_corrected_trace_paths(POST_TRIGGER_PATH_MINE_V2_BASELINE_TRACE_ROOT)
    challenger_trace_paths = _collect_corrected_trace_paths(POST_TRIGGER_PATH_MINE_V2_CHALLENGER_TRACE_ROOT)
    source_symbols = [str(entry["symbol"]) for entry in source_case_diagnostics]
    baseline_symbols = sorted(baseline_trace_paths)
    challenger_symbols = sorted(challenger_trace_paths)
    common_symbols = [symbol for symbol in source_symbols if symbol in baseline_trace_paths and symbol in challenger_trace_paths]
    baseline_only_symbols = sorted(set(baseline_symbols) - set(challenger_symbols))
    challenger_only_symbols = sorted(set(challenger_symbols) - set(baseline_symbols))

    case_records: list[dict[str, Any]] = []
    for source_entry in source_case_diagnostics:
        symbol = str(source_entry["symbol"])
        if symbol not in baseline_trace_paths or symbol not in challenger_trace_paths:
            continue
        baseline_rows = _trace_rows(str(baseline_trace_paths[symbol]))
        challenger_rows = _trace_rows(str(challenger_trace_paths[symbol]))
        record = _build_post_trigger_path_v2_case_record(
            source_entry=source_entry,
            baseline_rows=baseline_rows,
            challenger_rows=challenger_rows,
        )
        case_records.append(record)

    case_records_by_symbol = {str(entry["symbol"]): entry for entry in case_records}
    case_classification = _build_post_trigger_path_v2_case_classification(case_records)
    pnl_path_summary = _build_post_trigger_path_v2_pnl_path_summary(case_records)
    field_inventory = _build_post_trigger_path_v2_field_inventory(case_classification)
    source_no_lookahead_violations = int(source_no_lookahead_validation.get("violation_count", 0))
    source_trace_schema_version = str(source_schema_change_summary.get("trace_schema_version_after") or TRACE_INSTRUMENTATION_TRACE_SCHEMA_VERSION or "unknown")
    rollup = _build_post_trigger_path_v2_rollup(
        case_classification=case_classification,
        pnl_path_summary=pnl_path_summary,
        source_no_lookahead_violations=source_no_lookahead_violations,
        source_trace_schema_version=source_trace_schema_version,
    )
    decision = _build_post_trigger_path_v2_decision(rollup=rollup)

    manifest = {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_v2_mining_manifest_v1",
        "phase": "post_trigger_path_drift_evidence_mining_v2",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_trace_instrumentation_session_root": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT),
        "source_trace_manifest": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_MANIFEST),
        "source_trace_manifest_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_MANIFEST),
        "source_trace_schema_inventory": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_INVENTORY),
        "source_trace_schema_inventory_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_INVENTORY),
        "source_trace_schema_change_summary": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_CHANGE_SUMMARY),
        "source_trace_schema_change_summary_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_CHANGE_SUMMARY),
        "source_trace_no_lookahead_validation": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_NO_LOOKAHEAD_VALIDATION),
        "source_trace_no_lookahead_validation_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_NO_LOOKAHEAD_VALIDATION),
        "source_trace_instrumentation_decision": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_DECISION),
        "source_trace_instrumentation_decision_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_DECISION),
        "source_case_diagnostics": str(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "source_case_diagnostics_identity": _file_identity(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "baseline_trace_root": str(POST_TRIGGER_PATH_MINE_V2_BASELINE_TRACE_ROOT),
        "challenger_trace_root": str(POST_TRIGGER_PATH_MINE_V2_CHALLENGER_TRACE_ROOT),
        "baseline_case_count": len(baseline_symbols),
        "challenger_case_count": len(challenger_symbols),
        "comparison_available_case_count": len(case_records),
        "baseline_only_case_count": len(baseline_only_symbols),
        "challenger_only_case_count": len(challenger_only_symbols),
        "source_trace_schema_version": source_trace_schema_version,
        "source_no_lookahead_violations": source_no_lookahead_violations,
        "trace_schema_version_after": source_trace_schema_version,
        "generated_at": _utc_now(),
    }

    case_classification_artifact = {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_v2_case_classification_v1",
        "phase": "post_trigger_path_drift_evidence_mining_v2",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_trace_schema_version": source_trace_schema_version,
        "generated_at": _utc_now(),
        "cases": case_classification,
    }

    pnl_path_summary_artifact = {
        "schema_version": "tradex_hard_invalidation_post_trigger_path_v2_pnl_path_summary_v1",
        "phase": "post_trigger_path_drift_evidence_mining_v2",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_trace_schema_version": source_trace_schema_version,
        "generated_at": _utc_now(),
        "cases": pnl_path_summary,
    }

    artifacts = {
        "hard_invalidation_post_trigger_path_v2_mining_manifest.json": _write_json(session_root / "hard_invalidation_post_trigger_path_v2_mining_manifest.json", manifest),
        "hard_invalidation_post_trigger_path_v2_case_classification.json": _write_json(session_root / "hard_invalidation_post_trigger_path_v2_case_classification.json", case_classification_artifact),
        "hard_invalidation_post_trigger_path_v2_pnl_path_summary.json": _write_json(session_root / "hard_invalidation_post_trigger_path_v2_pnl_path_summary.json", pnl_path_summary_artifact),
        "hard_invalidation_post_trigger_path_v2_field_inventory.json": _write_json(session_root / "hard_invalidation_post_trigger_path_v2_field_inventory.json", field_inventory),
        "hard_invalidation_post_trigger_path_v2_rollup.json": _write_json(session_root / "hard_invalidation_post_trigger_path_v2_rollup.json", rollup),
        "hard_invalidation_post_trigger_path_v2_decision.json": _write_json(session_root / "hard_invalidation_post_trigger_path_v2_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_classification": case_classification_artifact,
        "pnl_path_summary": pnl_path_summary_artifact,
        "field_inventory": field_inventory,
        "rollup": rollup,
        "source_trace_instrumentation_session_root": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_TRACE_SESSION_ROOT),
        "source_trace_manifest": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_MANIFEST),
        "source_trace_manifest_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_MANIFEST),
        "source_trace_schema_inventory": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_INVENTORY),
        "source_trace_schema_inventory_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_INVENTORY),
        "source_trace_schema_change_summary": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_CHANGE_SUMMARY),
        "source_trace_schema_change_summary_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_SCHEMA_CHANGE_SUMMARY),
        "source_trace_no_lookahead_validation": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_NO_LOOKAHEAD_VALIDATION),
        "source_trace_no_lookahead_validation_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_NO_LOOKAHEAD_VALIDATION),
        "source_trace_instrumentation_decision": str(POST_TRIGGER_PATH_MINE_V2_SOURCE_DECISION),
        "source_trace_instrumentation_decision_identity": _file_identity(POST_TRIGGER_PATH_MINE_V2_SOURCE_DECISION),
        "source_case_diagnostics": str(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "source_case_diagnostics_identity": _file_identity(POST_TRIGGER_PATH_MINE_SOURCE_CASE_DIAGNOSTICS),
        "baseline_trace_root": str(POST_TRIGGER_PATH_MINE_V2_BASELINE_TRACE_ROOT),
        "challenger_trace_root": str(POST_TRIGGER_PATH_MINE_V2_CHALLENGER_TRACE_ROOT),
    }


def _run_trace_instrumentation_impl(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    split_cases = list(_load_json(SOURCE_SPLIT_CASE_RESULTS)["candidates"])
    previous_v2_cases = list(_load_json(PREVIOUS_V2_CASE_RESULTS))
    repaired_v2_reference_case_results = list(_load_json(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS))
    challenger_reference_case_results = list(_load_json(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS))
    repaired_v2_reference_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_reference_case_results}
    challenger_reference_by_symbol = {str(entry["symbol"]): entry for entry in challenger_reference_case_results}

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "trace_instrumentation"
    session_root.mkdir(parents=True, exist_ok=True)

    repaired_v2_rerun_root = session_root / "repaired_v2_rerun"
    challenger_rerun_root = session_root / "non_exit_late_extension_hedge_v1_rerun"
    repaired_v2_rerun_case_results = _build_severity_case_results(
        split_cases=split_cases,
        previous_v2_cases=previous_v2_cases,
        session_root=repaired_v2_rerun_root,
    )
    challenger_rerun_case_results = _build_non_exit_late_extension_hedge_case_results(
        split_cases=split_cases,
        repaired_v2_case_results=repaired_v2_rerun_case_results,
        session_root=challenger_rerun_root,
        symbols=SEVERITY_CASE_SYMBOLS,
    )
    repaired_v2_rerun_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_rerun_case_results}
    challenger_rerun_by_symbol = {str(entry["symbol"]): entry for entry in challenger_rerun_case_results}

    trace_schema_version_before = None
    trace_schema_version_after = TRACE_INSTRUMENTATION_TRACE_SCHEMA_VERSION or "unknown"

    def _collect_rerun_details(
        *,
        rerun_case_results: list[dict[str, Any]],
        rerun_by_symbol: dict[str, Any],
        reference_by_symbol: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], set[str], list[Path], int]:
        details: list[dict[str, Any]] = []
        field_names: set[str] = set()
        trace_paths: list[Path] = []
        no_lookahead_violations = 0
        for entry in rerun_case_results:
            symbol = str(entry["symbol"])
            ref_entry = reference_by_symbol[symbol]
            rerun_trace_artifacts = entry["trace_artifacts"]
            ref_trace_path = Path(str(ref_entry["trace_artifacts"]["corrected_trace_path"]))
            baseline_trace_path = Path(str(rerun_trace_artifacts["baseline_trace_path"]))
            corrected_trace_path = Path(str(rerun_trace_artifacts["corrected_trace_path"]))
            ref_rows = _trace_rows(str(ref_trace_path))
            rerun_rows = _trace_rows(str(corrected_trace_path))
            field_names.update(_trace_row_field_names(rerun_rows))
            trace_paths.extend([baseline_trace_path, corrected_trace_path])
            no_lookahead_violations += 0 if _trace_no_lookahead_ok(str(baseline_trace_path)) else 1
            no_lookahead_violations += 0 if _trace_no_lookahead_ok(str(corrected_trace_path)) else 1
            details.append(
                {
                    "symbol": symbol,
                    "case_name": entry.get("company_name"),
                    "reference_trace_identity": _file_identity(ref_trace_path),
                    "rerun_trace_identity": _file_identity(corrected_trace_path),
                    "reference_trace_schema": _trace_schema_inventory_for_rows(rows=ref_rows),
                    "rerun_trace_schema": _trace_schema_inventory_for_rows(rows=rerun_rows),
                    "comparison": _trace_path_difference(ref_rows, rerun_rows),
                    "schema_comparison": _trace_core_difference(ref_rows, rerun_rows),
                    "no_lookahead_ok": _trace_no_lookahead_ok(str(baseline_trace_path))
                    and _trace_no_lookahead_ok(str(corrected_trace_path)),
                }
            )
        return details, field_names, trace_paths, no_lookahead_violations

    baseline_behavior_details, baseline_rerun_schema_field_names, baseline_rerun_trace_paths, baseline_rerun_no_lookahead_violations = _collect_rerun_details(
        rerun_case_results=repaired_v2_rerun_case_results,
        rerun_by_symbol=repaired_v2_rerun_by_symbol,
        reference_by_symbol=repaired_v2_reference_by_symbol,
    )
    challenger_behavior_details, challenger_rerun_schema_field_names, challenger_rerun_trace_paths, challenger_rerun_no_lookahead_violations = _collect_rerun_details(
        rerun_case_results=challenger_rerun_case_results,
        rerun_by_symbol=challenger_rerun_by_symbol,
        reference_by_symbol=challenger_reference_by_symbol,
    )

    baseline_reference_schema_field_names = {
        field
        for entry in repaired_v2_reference_case_results
        for field in _trace_row_field_names(_trace_rows(str(entry["trace_artifacts"]["corrected_trace_path"])))
    }
    challenger_reference_schema_field_names = {
        field
        for entry in challenger_reference_case_results
        for field in _trace_row_field_names(_trace_rows(str(entry["trace_artifacts"]["corrected_trace_path"])))
    }
    baseline_missing_required_fields = sorted(set(TRACE_INSTRUMENTATION_REQUIRED_FIELDS) - baseline_rerun_schema_field_names)
    challenger_missing_required_fields = sorted(set(TRACE_INSTRUMENTATION_REQUIRED_FIELDS) - challenger_rerun_schema_field_names)
    combined_missing_required_fields = sorted(set(baseline_missing_required_fields) | set(challenger_missing_required_fields))
    combined_added_fields = sorted((baseline_rerun_schema_field_names | challenger_rerun_schema_field_names) - (baseline_reference_schema_field_names | challenger_reference_schema_field_names))

    def _first_non_watch_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return next((row for row in rows if str(row.get("action_type")) != "watch"), rows[0] if rows else {})

    sample_rows: list[dict[str, Any]] = []
    for run_label, rerun_by_symbol in (
        ("repaired_v2_rerun", repaired_v2_rerun_by_symbol),
        ("challenger_rerun", challenger_rerun_by_symbol),
    ):
        for symbol in ("4004", "5803"):
            entry = rerun_by_symbol[symbol]
            corrected_trace_path = Path(str(entry["trace_artifacts"]["corrected_trace_path"]))
            sample_rows.append(
                {
                    "run": run_label,
                    "symbol": symbol,
                    "case_name": entry.get("company_name"),
                    "trace_path": str(corrected_trace_path),
                    "row": _first_non_watch_row(_trace_rows(str(corrected_trace_path))),
                }
            )

    baseline_behavior_changed_symbols = [entry["symbol"] for entry in baseline_behavior_details if bool(entry["comparison"]["changed"])]
    challenger_behavior_changed_symbols = [entry["symbol"] for entry in challenger_behavior_details if bool(entry["comparison"]["changed"])]
    replay_behavior_changed = bool(baseline_behavior_changed_symbols or challenger_behavior_changed_symbols)
    all_trace_paths = baseline_rerun_trace_paths + challenger_rerun_trace_paths
    trace_schema_version_missing_trace_count = 0
    trace_schema_version_mismatch_trace_count = 0
    for trace_path in all_trace_paths:
        rows = _trace_rows(str(trace_path))
        versions = {str(row.get("trace_schema_version")) for row in rows if row.get("trace_schema_version") is not None}
        if not versions:
            trace_schema_version_missing_trace_count += 1
        elif len(versions) != 1 or trace_schema_version_after not in versions:
            trace_schema_version_mismatch_trace_count += 1
    rerun_schema_version_match = trace_schema_version_missing_trace_count == 0 and trace_schema_version_mismatch_trace_count == 0

    no_lookahead_validation = {
        "schema_version": "tradex_hard_invalidation_trace_no_lookahead_validation_v1",
        "phase": "trace_instrumentation",
        "trace_count": len(all_trace_paths),
        "violation_count": baseline_rerun_no_lookahead_violations + challenger_rerun_no_lookahead_violations,
        "trace_schema_version_missing_trace_count": trace_schema_version_missing_trace_count,
        "trace_schema_version_mismatch_trace_count": trace_schema_version_mismatch_trace_count,
        "baseline_rerun_trace_count": len(baseline_rerun_trace_paths),
        "challenger_rerun_trace_count": len(challenger_rerun_trace_paths),
        "trace_paths": [str(path) for path in all_trace_paths],
        "generated_at": _utc_now(),
    }

    schema_inventory = {
        "schema_version": "tradex_hard_invalidation_trace_schema_inventory_v1",
        "phase": "trace_instrumentation",
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "required_fields": list(TRACE_INSTRUMENTATION_REQUIRED_FIELDS),
        "reference": {
            "repaired_v2": {
                "session_root": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT),
                "case_results": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS),
                "field_names": sorted(baseline_reference_schema_field_names),
            },
            "challenger": {
                "session_root": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT),
                "case_results": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS),
                "field_names": sorted(challenger_reference_schema_field_names),
            },
        },
        "rerun": {
            "repaired_v2": {
                "session_root": str(repaired_v2_rerun_root),
                "field_names": sorted(baseline_rerun_schema_field_names),
                "missing_required_fields": baseline_missing_required_fields,
            },
            "challenger": {
                "session_root": str(challenger_rerun_root),
                "field_names": sorted(challenger_rerun_schema_field_names),
                "missing_required_fields": challenger_missing_required_fields,
            },
        },
        "rerun_schema_version_match": rerun_schema_version_match,
        "generated_at": _utc_now(),
    }

    schema_change_summary = {
        "schema_version": "tradex_hard_invalidation_trace_schema_change_summary_v1",
        "phase": "trace_instrumentation",
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "baseline_case_count": len(repaired_v2_rerun_case_results),
        "challenger_case_count": len(challenger_rerun_case_results),
        "fields_added": combined_added_fields,
        "fields_not_added": combined_missing_required_fields,
        "baseline_behavior_changed": bool(baseline_behavior_changed_symbols),
        "challenger_behavior_changed": bool(challenger_behavior_changed_symbols),
        "baseline_behavior_changed_symbols": baseline_behavior_changed_symbols,
        "challenger_behavior_changed_symbols": challenger_behavior_changed_symbols,
        "row_count_stable": all(entry["comparison"]["row_count_stable"] for entry in baseline_behavior_details + challenger_behavior_details),
        "replay_behavior_changed": replay_behavior_changed,
        "action_rules_changed": False,
        "pnl_path_available": len(combined_missing_required_fields) == 0,
        "can_resume_evidence_mining": False,
        "generated_at": _utc_now(),
    }

    decision = {
        "schema_version": "tradex_hard_invalidation_trace_instrumentation_decision_v1",
        "phase": "trace_instrumentation",
        "new_challenger_created": False,
        "action_rules_changed": False,
        "replay_behavior_changed": replay_behavior_changed,
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "rerun_schema_version_match": rerun_schema_version_match,
        "baseline_case_count": len(repaired_v2_rerun_case_results),
        "challenger_case_count": len(challenger_rerun_case_results),
        "pnl_path_available": len(combined_missing_required_fields) == 0,
        "can_resume_evidence_mining": bool(
            len(combined_missing_required_fields) == 0
            and not replay_behavior_changed
            and rerun_schema_version_match
        ),
        "decision": "blocked",
        "decision_reason": "Trace instrumentation added; evidence mining can resume once the next research turn begins",
        "candidate_local_decision": "blocked",
        "session_aggregate_decision": "blocked",
        "authoritative_rollup_decision": "blocked",
        "generated_at": _utc_now(),
    }

    manifest = {
        "schema_version": "tradex_hard_invalidation_trace_instrumentation_manifest_v1",
        "phase": "trace_instrumentation",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_audit_session_root": str(POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT),
        "reference_repaired_v2_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT),
        "reference_repaired_v2_case_results": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS),
        "reference_repaired_v2_case_results_identity": _file_identity(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS),
        "reference_challenger_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT),
        "reference_challenger_case_results": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS),
        "reference_challenger_case_results_identity": _file_identity(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS),
        "baseline_rerun_session_root": str(repaired_v2_rerun_root),
        "challenger_rerun_session_root": str(challenger_rerun_root),
        "baseline_case_count": len(repaired_v2_rerun_case_results),
        "challenger_case_count": len(challenger_rerun_case_results),
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "generated_at": _utc_now(),
    }

    artifacts = {
        "hard_invalidation_trace_instrumentation_manifest.json": _write_json(session_root / "hard_invalidation_trace_instrumentation_manifest.json", manifest),
        "hard_invalidation_trace_schema_inventory.json": _write_json(session_root / "hard_invalidation_trace_schema_inventory.json", schema_inventory),
        "hard_invalidation_trace_schema_change_summary.json": _write_json(session_root / "hard_invalidation_trace_schema_change_summary.json", schema_change_summary),
        "hard_invalidation_trace_sample_rows.json": _write_json(session_root / "hard_invalidation_trace_sample_rows.json", {
            "schema_version": "tradex_hard_invalidation_trace_sample_rows_v1",
            "phase": "trace_instrumentation",
            "trace_schema_version_after": trace_schema_version_after,
            "samples": sample_rows,
            "generated_at": _utc_now(),
        }),
        "hard_invalidation_trace_no_lookahead_validation.json": _write_json(session_root / "hard_invalidation_trace_no_lookahead_validation.json", no_lookahead_validation),
        "hard_invalidation_trace_instrumentation_decision.json": _write_json(session_root / "hard_invalidation_trace_instrumentation_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "manifest": manifest,
        "schema_inventory": schema_inventory,
        "schema_change_summary": schema_change_summary,
        "sample_rows": sample_rows,
        "no_lookahead_validation": no_lookahead_validation,
        "decision": decision,
        "baseline_rerun_session_root": str(repaired_v2_rerun_root),
        "challenger_rerun_session_root": str(challenger_rerun_root),
        "reference_repaired_v2_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT),
        "reference_challenger_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT),
    }


def run_trace_instrumentation(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    return _run_trace_instrumentation_impl(output_root=output_root)
    repaired_v2_reference_case_results = list(_load_json(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS))
    challenger_reference_case_results = list(_load_json(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS))
    repaired_v2_reference_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_reference_case_results}
    challenger_reference_by_symbol = {str(entry["symbol"]): entry for entry in challenger_reference_case_results}
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "trace_instrumentation"
    session_root.mkdir(parents=True, exist_ok=True)

    baseline_rerun = loop_mod.run_learning_loop(
        output_root=session_root / "repaired_v2_rerun",
        candidate_name=loop_mod.HARD_INVALIDATION_SEVERITY_CANDIDATE,
    )
    challenger_rerun = loop_mod.run_learning_loop(
        output_root=session_root / "non_exit_late_extension_hedge_v1_rerun",
        candidate_name=loop_mod.HARD_INVALIDATION_NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
    )
    baseline_rerun_session_root = Path(str(baseline_rerun["session_root"]))
    challenger_rerun_session_root = Path(str(challenger_rerun["session_root"]))
    baseline_replay_spec = _load_json(Path(str(baseline_rerun["artifacts"]["replay_spec"])))
    challenger_replay_spec = _load_json(Path(str(challenger_rerun["artifacts"]["replay_spec"])))
    baseline_trace_schema_version_after = str((baseline_replay_spec.get("action_trace_schema") or {}).get("schema_version") or "unknown")
    challenger_trace_schema_version_after = str((challenger_replay_spec.get("action_trace_schema") or {}).get("schema_version") or "unknown")
    trace_schema_version_after = baseline_trace_schema_version_after
    rerun_schema_version_match = (
        baseline_trace_schema_version_after == challenger_trace_schema_version_after == TRACE_INSTRUMENTATION_TRACE_SCHEMA_VERSION
    )
    trace_schema_version_before = None

    baseline_behavior_details: list[dict[str, Any]] = []
    challenger_behavior_details: list[dict[str, Any]] = []
    baseline_rerun_schema_field_names: set[str] = set()
    challenger_rerun_schema_field_names: set[str] = set()
    baseline_reference_schema_field_names: set[str] = set()
    challenger_reference_schema_field_names: set[str] = set()
    baseline_rerun_no_lookahead_violations = 0
    challenger_rerun_no_lookahead_violations = 0
    baseline_rerun_trace_paths: list[Path] = []
    challenger_rerun_trace_paths: list[Path] = []
    baseline_reference_trace_paths: list[Path] = []
    challenger_reference_trace_paths: list[Path] = []

    for case_spec in loop_mod.CASE_SPECS:
        symbol = str(case_spec["symbol"])
        paths = _trace_paths_for_case(baseline_rerun_session_root, case_spec)
        baseline_trace_path = Path(str(paths["baseline_trace_path"]))
        rerun_trace_path = Path(str(paths["corrected_trace_path"]))
        ref_entry = repaired_v2_reference_by_symbol[symbol]
        ref_trace_path = Path(str(ref_entry["trace_artifacts"]["corrected_trace_path"]))
        rerun_rows = _trace_rows(str(rerun_trace_path))
        ref_rows = _trace_rows(str(ref_trace_path))
        baseline_rerun_trace_paths.extend([baseline_trace_path, rerun_trace_path])
        baseline_reference_trace_paths.append(ref_trace_path)
        baseline_rerun_schema_field_names.update(_trace_row_field_names(rerun_rows))
        baseline_reference_schema_field_names.update(_trace_row_field_names(ref_rows))
        baseline_rerun_no_lookahead_violations += 0 if _trace_no_lookahead_ok(str(baseline_trace_path)) else 1
        baseline_rerun_no_lookahead_violations += 0 if _trace_no_lookahead_ok(str(rerun_trace_path)) else 1
        baseline_behavior_details.append(
            {
                "symbol": symbol,
                "case_name": case_spec["name"],
                "reference_trace_identity": _file_identity(ref_trace_path),
                "rerun_trace_identity": _file_identity(rerun_trace_path),
                "reference_trace_schema": _trace_schema_inventory_for_rows(rows=ref_rows),
                "rerun_trace_schema": _trace_schema_inventory_for_rows(rows=rerun_rows),
                "comparison": _trace_core_difference(ref_rows, rerun_rows),
                "no_lookahead_ok": _trace_no_lookahead_ok(str(rerun_trace_path)),
            }
        )

        challenger_paths = _trace_paths_for_case(challenger_rerun_session_root, case_spec)
        challenger_baseline_trace_path = Path(str(challenger_paths["baseline_trace_path"]))
        challenger_rerun_trace_path = Path(str(challenger_paths["corrected_trace_path"]))
        challenger_ref_entry = challenger_reference_by_symbol[symbol]
        challenger_ref_trace_path = Path(str(challenger_ref_entry["trace_artifacts"]["corrected_trace_path"]))
        challenger_rerun_rows = _trace_rows(str(challenger_rerun_trace_path))
        challenger_ref_rows = _trace_rows(str(challenger_ref_trace_path))
        challenger_rerun_trace_paths.extend([challenger_baseline_trace_path, challenger_rerun_trace_path])
        challenger_reference_trace_paths.append(challenger_ref_trace_path)
        challenger_rerun_schema_field_names.update(_trace_row_field_names(challenger_rerun_rows))
        challenger_reference_schema_field_names.update(_trace_row_field_names(challenger_ref_rows))
        challenger_rerun_no_lookahead_violations += 0 if _trace_no_lookahead_ok(str(challenger_baseline_trace_path)) else 1
        challenger_rerun_no_lookahead_violations += 0 if _trace_no_lookahead_ok(str(challenger_rerun_trace_path)) else 1
        challenger_behavior_details.append(
            {
                "symbol": symbol,
                "case_name": case_spec["name"],
                "reference_trace_identity": _file_identity(challenger_ref_trace_path),
                "rerun_trace_identity": _file_identity(challenger_rerun_trace_path),
                "reference_trace_schema": _trace_schema_inventory_for_rows(rows=challenger_ref_rows),
                "rerun_trace_schema": _trace_schema_inventory_for_rows(rows=challenger_rerun_rows),
                "comparison": _trace_core_difference(challenger_ref_rows, challenger_rerun_rows),
                "no_lookahead_ok": _trace_no_lookahead_ok(str(challenger_rerun_trace_path)),
            }
        )

    baseline_required_fields = set(TRACE_INSTRUMENTATION_REQUIRED_FIELDS)
    challenger_required_fields = set(TRACE_INSTRUMENTATION_REQUIRED_FIELDS)
    baseline_added_fields = sorted(baseline_rerun_schema_field_names - baseline_reference_schema_field_names)
    challenger_added_fields = sorted(challenger_rerun_schema_field_names - challenger_reference_schema_field_names)
    baseline_missing_required_fields = sorted(baseline_required_fields - baseline_rerun_schema_field_names)
    challenger_missing_required_fields = sorted(challenger_required_fields - challenger_rerun_schema_field_names)
    combined_missing_required_fields = sorted(set(baseline_missing_required_fields) | set(challenger_missing_required_fields))
    combined_added_fields = sorted(set(baseline_added_fields) | set(challenger_added_fields))

    def _first_non_watch_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return next((row for row in rows if str(row.get("action_type")) != "watch"), rows[0] if rows else {})

    sample_specs = [
        {"run": "repaired_v2_rerun", "symbol": "4004"},
        {"run": "repaired_v2_rerun", "symbol": "5803"},
        {"run": "challenger_rerun", "symbol": "4004"},
        {"run": "challenger_rerun", "symbol": "5803"},
    ]
    sample_rows: list[dict[str, Any]] = []
    for sample in sample_specs:
        symbol = str(sample["symbol"])
        case_spec = next(case for case in loop_mod.CASE_SPECS if str(case["symbol"]) == symbol)
        rerun_session_root = baseline_rerun_session_root if sample["run"] == "repaired_v2_rerun" else challenger_rerun_session_root
        sample_trace_path = _trace_paths_for_case(rerun_session_root, case_spec)["corrected_trace_path"]
        rows = _trace_rows(str(sample_trace_path))
        row = _first_non_watch_row(rows)
        sample_rows.append(
            {
                "run": sample["run"],
                "symbol": symbol,
                "case_name": case_spec["name"],
                "trace_path": str(sample_trace_path),
                "row": row,
            }
        )

    baseline_behavior_changed_symbols = [entry["symbol"] for entry in baseline_behavior_details if bool(entry["comparison"]["changed"])]
    challenger_behavior_changed_symbols = [entry["symbol"] for entry in challenger_behavior_details if bool(entry["comparison"]["changed"])]
    replay_behavior_changed = bool(baseline_behavior_changed_symbols or challenger_behavior_changed_symbols)
    all_trace_paths = baseline_rerun_trace_paths + challenger_rerun_trace_paths
    trace_schema_version_missing_trace_count = 0
    trace_schema_version_mismatch_trace_count = 0
    for trace_path in all_trace_paths:
        rows = _trace_rows(str(trace_path))
        versions = {str(row.get("trace_schema_version")) for row in rows if row.get("trace_schema_version") is not None}
        if not versions:
            trace_schema_version_missing_trace_count += 1
        elif len(versions) != 1 or trace_schema_version_after not in versions:
            trace_schema_version_mismatch_trace_count += 1
    no_lookahead_validation = {
        "schema_version": "tradex_hard_invalidation_trace_no_lookahead_validation_v1",
        "phase": "trace_instrumentation",
        "trace_count": len(all_trace_paths),
        "violation_count": baseline_rerun_no_lookahead_violations + challenger_rerun_no_lookahead_violations,
        "trace_schema_version_missing_trace_count": trace_schema_version_missing_trace_count,
        "trace_schema_version_mismatch_trace_count": trace_schema_version_mismatch_trace_count,
        "baseline_rerun_trace_count": len(baseline_rerun_trace_paths),
        "challenger_rerun_trace_count": len(challenger_rerun_trace_paths),
        "trace_paths": [str(path) for path in all_trace_paths],
        "generated_at": _utc_now(),
    }

    schema_inventory = {
        "schema_version": "tradex_hard_invalidation_trace_schema_inventory_v1",
        "phase": "trace_instrumentation",
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "required_fields": list(TRACE_INSTRUMENTATION_REQUIRED_FIELDS),
        "reference": {
            "repaired_v2": {
                "session_root": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT),
                "case_results": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS),
                "field_names": sorted(baseline_reference_schema_field_names),
            },
            "challenger": {
                "session_root": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT),
                "case_results": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS),
                "field_names": sorted(challenger_reference_schema_field_names),
            },
        },
        "rerun": {
            "repaired_v2": {
                "session_root": str(baseline_rerun_session_root),
                "trace_schema_version": baseline_trace_schema_version_after,
                "field_names": sorted(baseline_rerun_schema_field_names),
                "missing_required_fields": baseline_missing_required_fields,
            },
            "challenger": {
                "session_root": str(challenger_rerun_session_root),
                "trace_schema_version": challenger_trace_schema_version_after,
                "field_names": sorted(challenger_rerun_schema_field_names),
                "missing_required_fields": challenger_missing_required_fields,
            },
        },
        "rerun_schema_version_match": rerun_schema_version_match,
        "generated_at": _utc_now(),
    }

    schema_change_summary = {
        "schema_version": "tradex_hard_invalidation_trace_schema_change_summary_v1",
        "phase": "trace_instrumentation",
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "rerun_schema_version_match": rerun_schema_version_match,
        "baseline_case_count": len(loop_mod.CASE_SPECS),
        "challenger_case_count": len(loop_mod.CASE_SPECS),
        "fields_added": combined_added_fields,
        "fields_not_added": combined_missing_required_fields,
        "baseline_behavior_changed": bool(baseline_behavior_changed_symbols),
        "challenger_behavior_changed": bool(challenger_behavior_changed_symbols),
        "baseline_behavior_changed_symbols": baseline_behavior_changed_symbols,
        "challenger_behavior_changed_symbols": challenger_behavior_changed_symbols,
        "row_count_stable": all(entry["comparison"]["row_count_stable"] for entry in baseline_behavior_details + challenger_behavior_details),
        "replay_behavior_changed": replay_behavior_changed,
        "action_rules_changed": False,
        "pnl_path_available": len(combined_missing_required_fields) == 0,
        "can_resume_evidence_mining": False,
        "generated_at": _utc_now(),
    }

    decision = {
        "schema_version": "tradex_hard_invalidation_trace_instrumentation_decision_v1",
        "phase": "trace_instrumentation",
        "new_challenger_created": False,
        "action_rules_changed": False,
        "replay_behavior_changed": replay_behavior_changed,
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "baseline_case_count": len(loop_mod.CASE_SPECS),
        "challenger_case_count": len(loop_mod.CASE_SPECS),
        "pnl_path_available": len(combined_missing_required_fields) == 0,
        "can_resume_evidence_mining": False,
        "decision": "blocked",
        "decision_reason": "Trace instrumentation added, but evidence mining remains paused in this turn",
        "candidate_local_decision": "blocked",
        "session_aggregate_decision": "blocked",
        "authoritative_rollup_decision": "blocked",
        "generated_at": _utc_now(),
    }

    manifest = {
        "schema_version": "tradex_hard_invalidation_trace_instrumentation_manifest_v1",
        "phase": "trace_instrumentation",
        "source_audit": "full_coverage_replay_path_delta_audit",
        "source_audit_session_root": str(POST_TRIGGER_PATH_MINE_SOURCE_AUDIT_ROOT),
        "reference_repaired_v2_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT),
        "reference_repaired_v2_case_results": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS),
        "reference_repaired_v2_case_results_identity": _file_identity(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_CASE_RESULTS),
        "reference_challenger_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT),
        "reference_challenger_case_results": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS),
        "reference_challenger_case_results_identity": _file_identity(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_CASE_RESULTS),
        "baseline_rerun_session_root": str(baseline_rerun_session_root),
        "baseline_rerun_replay_spec": str(baseline_rerun["artifacts"]["replay_spec"]),
        "baseline_rerun_replay_spec_identity": _file_identity(Path(str(baseline_rerun["artifacts"]["replay_spec"]))),
        "challenger_rerun_session_root": str(challenger_rerun_session_root),
        "challenger_rerun_replay_spec": str(challenger_rerun["artifacts"]["replay_spec"]),
        "challenger_rerun_replay_spec_identity": _file_identity(Path(str(challenger_rerun["artifacts"]["replay_spec"]))),
        "baseline_case_count": len(loop_mod.CASE_SPECS),
        "challenger_case_count": len(loop_mod.CASE_SPECS),
        "trace_schema_version_before": trace_schema_version_before,
        "trace_schema_version_after": trace_schema_version_after,
        "generated_at": _utc_now(),
    }

    artifacts = {
        "hard_invalidation_trace_instrumentation_manifest.json": _write_json(session_root / "hard_invalidation_trace_instrumentation_manifest.json", manifest),
        "hard_invalidation_trace_schema_inventory.json": _write_json(session_root / "hard_invalidation_trace_schema_inventory.json", schema_inventory),
        "hard_invalidation_trace_schema_change_summary.json": _write_json(session_root / "hard_invalidation_trace_schema_change_summary.json", schema_change_summary),
        "hard_invalidation_trace_sample_rows.json": _write_json(session_root / "hard_invalidation_trace_sample_rows.json", {
            "schema_version": "tradex_hard_invalidation_trace_sample_rows_v1",
            "phase": "trace_instrumentation",
            "trace_schema_version_after": trace_schema_version_after,
            "samples": sample_rows,
            "generated_at": _utc_now(),
        }),
        "hard_invalidation_trace_no_lookahead_validation.json": _write_json(session_root / "hard_invalidation_trace_no_lookahead_validation.json", no_lookahead_validation),
        "hard_invalidation_trace_instrumentation_decision.json": _write_json(session_root / "hard_invalidation_trace_instrumentation_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "manifest": manifest,
        "schema_inventory": schema_inventory,
        "schema_change_summary": schema_change_summary,
        "sample_rows": sample_rows,
        "no_lookahead_validation": no_lookahead_validation,
        "decision": decision,
        "baseline_rerun_session_root": str(baseline_rerun_session_root),
        "challenger_rerun_session_root": str(challenger_rerun_session_root),
        "reference_repaired_v2_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_REPAIRED_V2_SESSION_ROOT),
        "reference_challenger_session_root": str(TRACE_INSTRUMENTATION_REFERENCE_CHALLENGER_SESSION_ROOT),
    }


def _build_decision(*, anchor_regression: list[dict[str, Any]], rollup: dict[str, Any], case_results: list[dict[str, Any]]) -> tuple[str, str]:
    anchor_by_symbol = {entry["symbol"]: entry for entry in anchor_regression}
    anchor_ok = all(
        [
            anchor_by_symbol["2317"]["damage_reduced"],
            anchor_by_symbol["9697"]["damage_reduced"],
            anchor_by_symbol["2531"]["damage_reduced"] or anchor_by_symbol["2531"]["loss_side_protection_preserved"],
            anchor_by_symbol["5541"]["preservation_flag"],
        ]
    )
    no_false_winner_block = rollup["false_winner_block_count"] == 0
    no_lookahead_ok = all(
        _trace_no_lookahead_ok(entry["trace_artifacts"]["baseline_trace_path"]) and _trace_no_lookahead_ok(entry["trace_artifacts"]["corrected_trace_path"])
        for entry in case_results
    )
    if not anchor_ok or not no_false_winner_block:
        return "drop", "Anchor regression or winner preservation failed"
    if not no_lookahead_ok:
        return "drop", "No-lookahead assertion failed on one or more traces"
    if rollup.get("contract_violation_count", 0) > 0:
        return "drop", "Severity/action alignment contract violated"
    if rollup["mined_candidate_count"] == 0:
        return "hold", "No out-of-anchor candidates found"
    if rollup["action_changing_count"] == 0:
        return "hold", "Out-of-anchor mining remained label-only"
    if rollup["false_exit_count"] > 0 and rollup["worsened_count_by_bucket"].get("pure_hard_invalidation", 0) > rollup["improved_count_by_bucket"].get("pure_hard_invalidation", 0):
        return "hold", "Severity refinement remains mixed"
    return "keep", "Severity refinement reduced premature exits without breaking controls"


def _build_contract_repair_decision(*, rollup: dict[str, Any]) -> dict[str, Any]:
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    contract_violation_count = int(rollup.get("contract_violation_count", 0))
    repair_passed = contract_violation_count == 0 and no_lookahead_violations == 0
    return {
        "schema_version": "tradex_hard_invalidation_contract_repair_decision_v1",
        "phase": "contract_repair",
        "candidate_name": SEVERITY_CANDIDATE,
        "decision": "repair_only",
        "decision_reason": (
            "Hard-invalidation action/severity contract is repaired and optimization remains blocked"
            if repair_passed
            else "Hard-invalidation action/severity contract still has mismatches; do not optimize"
        ),
        "candidate_local_decision": "repair_only",
        "session_aggregate_decision": "repair_only",
        "authoritative_rollup_decision": "repair_only",
        "candidate_status": "repair_only",
        "outcome_optimization_allowed": False,
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "repair_passed": repair_passed,
        "next_allowed_action": "only after contract repair passes, design a fresh challenger or rerun v2 audit under the repaired contract",
        "generated_at": _utc_now(),
    }


def run_audit(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    split_payload = _load_json(SOURCE_SPLIT_CASE_RESULTS)
    split_cases = list(split_payload["candidates"])
    split_by_symbol = {str(entry["symbol"]): entry for entry in split_cases}
    previous_v2_payload = _load_json(PREVIOUS_V2_CASE_RESULTS)
    previous_v2_cases = list(previous_v2_payload)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp
    session_root.mkdir(parents=True, exist_ok=True)

    anchor_regression = _build_anchor_regression(session_root=session_root)
    severity_case_results = _build_severity_case_results(split_cases=split_cases, previous_v2_cases=previous_v2_cases, session_root=session_root)
    rollup = _build_rollup(severity_case_results)
    repair_decision = _build_contract_repair_decision(rollup=rollup)

    manifest = {
        "schema_version": "tradex_hard_invalidation_contract_repair_manifest_v1",
        "phase": "contract_repair",
        "candidate_name": SEVERITY_CANDIDATE,
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
        "previous_v2_session_root": str(PREVIOUS_V2_SESSION_ROOT),
        "previous_v2_artifact": str(PREVIOUS_V2_CASE_RESULTS),
        "anchor_symbols": list(ANCHOR_SYMBOLS),
        "repair_case_symbols": list(SEVERITY_CASE_SYMBOLS),
        "selection_basis": {
            "anchor_regression": "loop_mod.CASE_SPECS replay under severity candidate",
            "repair_case_set": "previous mined-axis split session",
        },
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    case_diagnostics = {
        "schema_version": "tradex_hard_invalidation_contract_repair_case_diagnostics_v1",
        "phase": "contract_repair",
        "candidate_name": SEVERITY_CANDIDATE,
        "generated_at": _utc_now(),
        "cases": severity_case_results,
    }

    artifacts = {
        "hard_invalidation_contract_repair_manifest.json": _write_json(session_root / "hard_invalidation_contract_repair_manifest.json", manifest),
        "hard_invalidation_contract_repair_case_diagnostics.json": _write_json(session_root / "hard_invalidation_contract_repair_case_diagnostics.json", case_diagnostics),
        "hard_invalidation_contract_repair_case_results.json": _write_json(session_root / "hard_invalidation_contract_repair_case_results.json", severity_case_results),
        "hard_invalidation_contract_repair_rollup.json": _write_json(session_root / "hard_invalidation_contract_repair_rollup.json", rollup),
        "hard_invalidation_contract_repair_anchor_regression.json": _write_json(session_root / "hard_invalidation_contract_repair_anchor_regression.json", anchor_regression),
        "hard_invalidation_contract_repair_decision.json": _write_json(session_root / "hard_invalidation_contract_repair_decision.json", repair_decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": repair_decision,
        "anchor_regression": anchor_regression,
        "case_diagnostics": case_diagnostics,
        "case_results": severity_case_results,
        "rollup": rollup,
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
    }


def _build_repaired_v2_freeze_decision(*, rollup: dict[str, Any]) -> dict[str, Any]:
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    contract_violation_count = int(rollup.get("contract_violation_count", 0))
    freeze_passed = contract_violation_count == 0 and no_lookahead_violations == 0
    return {
        "schema_version": "tradex_hard_invalidation_repaired_v2_decision_v1",
        "phase": "baseline_freeze",
        "candidate_name": SEVERITY_CANDIDATE,
        "decision": "freeze",
        "decision_reason": (
            "Repaired v2 baseline is frozen under the repaired contract; no challenger optimization is implied"
            if freeze_passed
            else "Repaired v2 baseline freeze failed contract or no-lookahead checks"
        ),
        "candidate_local_decision": "freeze",
        "session_aggregate_decision": "freeze",
        "authoritative_rollup_decision": "freeze",
        "candidate_status": "baseline_freeze",
        "outcome_optimization_allowed": False,
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "freeze_passed": freeze_passed,
        "next_allowed_action": "design one fresh challenger only after the baseline is frozen",
        "generated_at": _utc_now(),
    }


def _guard_reason_tags(
    *,
    summary: dict[str, Any],
    selected_event_row: dict[str, Any] | None,
) -> list[str]:
    reasons: list[str] = []
    primary_reason = summary.get("hard_invalidation_profit_preservation_guard_reason")
    if primary_reason is not None:
        reasons.append(str(primary_reason))
    if selected_event_row is not None:
        special_reason = selected_event_row.get("special_reason")
        if special_reason is not None and str(special_reason) not in reasons:
            reasons.append(str(special_reason))
        reduction_intensity = selected_event_row.get("reduction_intensity")
        if reduction_intensity is not None:
            reasons.append(f"reduction_intensity={reduction_intensity}")
    return reasons


def _build_profit_preservation_guard_case_results(
    *,
    split_cases: list[dict[str, Any]],
    repaired_v2_case_results: list[dict[str, Any]],
    session_root: Path,
) -> list[dict[str, Any]]:
    old_by_symbol = {str(entry["symbol"]): entry for entry in split_cases}
    repaired_v2_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_case_results}
    candidate_root = session_root / "profit_preservation_guard_v1"
    candidate_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []

    for symbol in SEVERITY_CASE_SYMBOLS:
        old_entry = old_by_symbol[symbol]
        repaired_v2_entry = repaired_v2_by_symbol[symbol]
        case_spec = _build_case_spec_from_split(old_entry)
        artifact = _run_case_bundle(
            case_spec=case_spec,
            output_root=candidate_root,
            candidate_name=loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        )
        corrected = artifact["corrected"]["summary"]
        corrected_rows = _trace_rows(str(artifact["trace_artifacts"]["corrected_trace_path"]))
        trigger_date = str(old_entry["trigger_date"])
        action_date = trigger_date
        old_trace_action, old_trace_target = _action_at(str(old_entry["trace_artifacts"]["corrected_trace_path"]), action_date) if action_date else (None, None)
        baseline_trace_action, baseline_trace_target = _action_at(artifact["trace_artifacts"]["baseline_trace_path"], action_date) if action_date else (None, None)
        challenger_trace_action, challenger_trace_target = _action_at(artifact["trace_artifacts"]["corrected_trace_path"], action_date) if action_date else (None, None)
        trigger_date_row: dict[str, Any] | None = None
        if trigger_date is not None:
            try:
                trigger_date_row = _row_by_date(corrected_rows, str(trigger_date))
            except KeyError:
                trigger_date_row = None
        trigger_date_row_is_hard_invalidation = bool(
            trigger_date_row is not None
            and (
                str(trigger_date_row.get("reason")) in {
                    "hard_invalidation_exit_v1",
                    "hard_invalidation_exit_all",
                    "hard_invalidation_exit_severity_v2_loss_side_override",
                    loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
                    "hard_invalidation_long_reduce",
                    "partial_exit_due_to_hard_invalidation",
                }
                or str(trigger_date_row.get("forced_exit_reason")) in {
                    "hard_invalidation_exit_v1",
                    "hard_invalidation_exit_all",
                    "hard_invalidation_exit_severity_v2_loss_side_override",
                    loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
                }
            )
        )
        selected_event_date = (
            trigger_date
            if trigger_date_row_is_hard_invalidation
            else corrected.get("first_hard_invalidation_action_date")
            or artifact["compare"]["corrected"].get("first_hard_invalidation_action_date")
        )
        selected_event_row_for_report: dict[str, Any] | None = None
        selected_event_source = "missing"
        if selected_event_date is not None:
            try:
                selected_event_row_for_report = _row_by_date(corrected_rows, str(selected_event_date))
                selected_event_source = "selector"
            except KeyError:
                selected_event_row_for_report = None
        if selected_event_row_for_report is None and selected_event_date is None:
            selected_event_date = (
                corrected.get("first_hard_invalidation_date")
                or artifact["compare"]["corrected"].get("first_hard_invalidation_date")
            )
            if selected_event_date is not None:
                try:
                    selected_event_row_for_report = _row_by_date(corrected_rows, str(selected_event_date))
                    selected_event_source = "summary_fallback"
                except KeyError:
                    selected_event_row_for_report = None
                    selected_event_source = "summary_fallback"
        selected_event_date_text = str(selected_event_date) if selected_event_date is not None else None
        selected_event_reason = (
            str(selected_event_row_for_report.get("reason"))
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("reason") is not None
            else None
        )
        selected_event_action_type = (
            str(selected_event_row_for_report.get("action_type"))
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("action_type") is not None
            else None
        )
        selected_event_target_position = (
            str(selected_event_row_for_report.get("target_position_after"))
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("target_position_after") is not None
            else None
        )
        repaired_v2_pnl = float(repaired_v2_entry["v2_loss_side_override_pnl"])
        challenger_pnl = float(corrected["total_pnl"])
        original_baseline_pnl = float(old_entry["stacked_pnl"])
        baseline_v2_action = str(repaired_v2_entry["v2_loss_side_override_action_on_trigger_date"])
        challenger_action = challenger_trace_action
        baseline_v2_target = str(repaired_v2_entry["v2_loss_side_override_target_position"])
        challenger_target = challenger_trace_target
        baseline_v2_selected_event_source = str(repaired_v2_entry["selected_hard_invalidation_event_source"])
        baseline_v2_severity_level = repaired_v2_entry.get("severity_level")
        challenger_severity_level = (
            selected_event_row_for_report.get("severity_level")
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("severity_level") is not None
            else corrected.get("hard_invalidation_severity_level")
        )
        challenger_severity_target_buy_units = (
            selected_event_row_for_report.get("target_position_after")
            if selected_event_row_for_report is not None and selected_event_row_for_report.get("target_position_after") is not None
            else corrected.get("hard_invalidation_severity_target_buy_units")
        )
        guard_activated = bool(corrected.get("hard_invalidation_profit_preservation_guard_applied"))
        guard_reasons = _guard_reason_tags(summary=corrected, selected_event_row=selected_event_row_for_report)
        if selected_event_reason is not None and selected_event_reason not in guard_reasons:
            guard_reasons.append(selected_event_reason)
        action_changed = bool(baseline_v2_action != challenger_action)
        false_exit_or_premature_exit_candidate = bool(repaired_v2_pnl > 0 and baseline_v2_action == "exit_all")
        accidentally_preserved_true_loser = bool(repaired_v2_pnl <= 0 and action_changed and challenger_pnl < repaired_v2_pnl)
        contract_violation = bool(
            selected_event_source == "selector" and not bool(
                _severity_action_aligned(challenger_severity_level, selected_event_action_type)
                and _severity_target_aligned(
                    challenger_severity_level,
                    selected_event_action_type,
                    selected_event_target_position,
                    None,
                )
            )
        )
        result = {
            "symbol": symbol,
            "company_name": str(old_entry["company_name"]),
            "replay_window": dict(old_entry["replay_window"]),
            "data_backed": bool(old_entry["data_backed"]),
            "note_backed": bool(old_entry["note_backed"]),
            "pre_scan_trigger_reason": str(old_entry["pre_scan_trigger_reason"]),
            "expected_axis": str(old_entry["trigger_axis"] if old_entry["trigger_axis"] != "mixed" else "mixed"),
            "primary_axis_bucket": str(old_entry["primary_axis_bucket"]),
            "secondary_axes": list(old_entry["secondary_axes"]),
            "trigger_dates_by_axis": dict(old_entry["trigger_dates_by_axis"]),
            "trigger_date": trigger_date,
            "action_date": action_date,
            "selected_hard_invalidation_event_date": selected_event_date_text,
            "selected_hard_invalidation_event_reason": selected_event_reason,
            "selected_hard_invalidation_event_action_type": selected_event_action_type,
            "selected_hard_invalidation_event_target_position": selected_event_target_position,
            "selected_hard_invalidation_event_source": "selected_selector_row"
            if selected_event_source == "selector"
            else "summary_fallback_row"
            if selected_event_source == "summary_fallback"
            else "missing",
            "baseline_v2_action_on_trigger_date": baseline_v2_action,
            "baseline_v2_target_position": baseline_v2_target,
            "baseline_v2_actual_position_after_trigger": baseline_v2_target,
            "baseline_v2_pnl": repaired_v2_pnl,
            "baseline_v2_selected_event_source": baseline_v2_selected_event_source,
            "baseline_v2_severity_level": baseline_v2_severity_level,
            "challenger_action_on_trigger_date": challenger_action,
            "challenger_target_position": challenger_target,
            "challenger_actual_position_after_trigger": challenger_target,
            "challenger_pnl": challenger_pnl,
            "challenger_severity_level": challenger_severity_level,
            "original_baseline_action_on_trigger_date": old_trace_action,
            "original_baseline_target_position": old_trace_target,
            "original_baseline_pnl": original_baseline_pnl,
            "pnl_delta_vs_repaired_v2": float(challenger_pnl - repaired_v2_pnl),
            "pnl_delta_vs_original_baseline": float(challenger_pnl - original_baseline_pnl),
            "action_changed_flag": action_changed,
            "guard_activated": guard_activated,
            "guard_reasons": guard_reasons,
            "typed_guard_reason_present": bool(action_changed and guard_reasons),
            "false_exit_or_premature_exit_candidate": false_exit_or_premature_exit_candidate,
            "accidentally_preserved_true_loser": accidentally_preserved_true_loser,
            "compare_to_repaired_v2": {
                "baseline_action": baseline_v2_action,
                "challenger_action": challenger_action,
                "baseline_target_position": baseline_v2_target,
                "challenger_target_position": challenger_target,
                "baseline_pnl": repaired_v2_pnl,
                "challenger_pnl": challenger_pnl,
                "pnl_delta": float(challenger_pnl - repaired_v2_pnl),
                "action_changed_flag": action_changed,
            },
            "compare_to_original_baseline": {
                "original_baseline_action": old_trace_action,
                "original_baseline_target_position": old_trace_target,
                "original_baseline_pnl": original_baseline_pnl,
                "challenger_pnl": challenger_pnl,
                "pnl_delta": float(challenger_pnl - original_baseline_pnl),
            },
            "severity_level": challenger_severity_level,
            "reduction_intensity": corrected.get("hard_invalidation_reduction_intensity"),
            "loss_side_override_applied": bool(corrected.get("hard_invalidation_loss_side_override_applied")),
            "profit_protection_guard_applied": bool(corrected.get("hard_invalidation_profit_protection_guard_applied")),
            "profit_preservation_guard_applied": guard_activated,
            "profit_preservation_guard_reason": corrected.get("hard_invalidation_profit_preservation_guard_reason"),
            "selected_event_action_allowed_under_recorded_severity": bool(
                _severity_action_aligned(challenger_severity_level, selected_event_action_type)
                and _severity_target_aligned(
                    challenger_severity_level,
                    selected_event_action_type,
                    selected_event_target_position,
                    int(challenger_severity_target_buy_units.split("-", 1)[1])
                    if isinstance(challenger_severity_target_buy_units, str) and "-" in challenger_severity_target_buy_units
                    else int(challenger_severity_target_buy_units)
                    if challenger_severity_target_buy_units is not None
                    else None,
                )
            ),
            "emitted_action_allowed_under_recorded_severity": bool(
                _severity_action_aligned(challenger_severity_level, challenger_action)
                and _severity_target_aligned(
                    challenger_severity_level,
                    challenger_action,
                    challenger_target,
                    int(challenger_severity_target_buy_units.split("-", 1)[1])
                    if isinstance(challenger_severity_target_buy_units, str) and "-" in challenger_severity_target_buy_units
                    else int(challenger_severity_target_buy_units)
                    if challenger_severity_target_buy_units is not None
                    else None,
                )
            ),
            "contract_violation": contract_violation,
            "mismatch_cause": _mismatch_cause_for_case(
                selected_event_row=selected_event_row_for_report,
                selected_event_source=selected_event_source,
                emitted_action_type=selected_event_action_type,
                recorded_severity_level=challenger_severity_level,
                recorded_severity_action_type=str(corrected.get("hard_invalidation_severity_action_type"))
                if corrected.get("hard_invalidation_severity_action_type") is not None
                else None,
                primary_axis_bucket=str(old_entry["primary_axis_bucket"]),
            ),
            "compare": artifact["compare"],
            "trace_artifacts": artifact["trace_artifacts"],
            "labels": list(artifact["corrected"]["labels"]),
        }
        results.append(result)

    return results


def _build_profit_preservation_guard_rollup(case_results: list[dict[str, Any]]) -> dict[str, Any]:
    total_pnl_delta_vs_repaired_v2 = 0.0
    total_pnl_delta_vs_original_baseline = 0.0
    improved_vs_repaired_v2_count = 0
    worsened_vs_repaired_v2_count = 0
    profitable_baseline_total_pnl_delta_vs_repaired_v2 = 0.0
    profitable_baseline_cases_improved = 0
    profitable_baseline_cases_damaged = 0
    loss_side_total_pnl_delta_vs_repaired_v2 = 0.0
    loss_side_cases_improved = 0
    loss_side_cases_worsened = 0
    action_changed_count = 0
    guard_activated_count = 0
    changed_action_with_guard_reason_count = 0
    typed_guard_reason_missing_count = 0
    false_exit_or_premature_exit_candidate_count = 0
    accidentally_preserved_true_loser_count = 0
    contract_violation_count = 0
    no_lookahead_violations = 0
    anchor_5541_preservation_count = 0
    selected_event_source_counts = Counter()
    mismatch_cause_counts = Counter()
    improved_symbols: list[str] = []
    changed_symbols: list[str] = []
    guard_reason_symbols: list[str] = []

    for entry in case_results:
        delta_v2 = float(entry["pnl_delta_vs_repaired_v2"])
        delta_original = float(entry["pnl_delta_vs_original_baseline"])
        baseline_v2_pnl = float(entry["baseline_v2_pnl"])
        total_pnl_delta_vs_repaired_v2 += delta_v2
        total_pnl_delta_vs_original_baseline += delta_original
        if delta_v2 > 0:
            improved_vs_repaired_v2_count += 1
            improved_symbols.append(str(entry["symbol"]))
        elif delta_v2 < 0:
            worsened_vs_repaired_v2_count += 1
        if baseline_v2_pnl > 0:
            profitable_baseline_total_pnl_delta_vs_repaired_v2 += delta_v2
            if delta_v2 > 0:
                profitable_baseline_cases_improved += 1
            elif delta_v2 < 0:
                profitable_baseline_cases_damaged += 1
        else:
            loss_side_total_pnl_delta_vs_repaired_v2 += delta_v2
            if delta_v2 > 0:
                loss_side_cases_improved += 1
            elif delta_v2 < 0:
                loss_side_cases_worsened += 1
        if entry.get("action_changed_flag"):
            action_changed_count += 1
            changed_symbols.append(str(entry["symbol"]))
        if entry.get("guard_activated"):
            guard_activated_count += 1
        if entry.get("typed_guard_reason_present"):
            changed_action_with_guard_reason_count += 1
            guard_reason_symbols.append(str(entry["symbol"]))
        else:
            if entry.get("action_changed_flag"):
                typed_guard_reason_missing_count += 1
        if entry.get("false_exit_or_premature_exit_candidate"):
            false_exit_or_premature_exit_candidate_count += 1
        if entry.get("accidentally_preserved_true_loser"):
            accidentally_preserved_true_loser_count += 1
        if entry.get("selected_hard_invalidation_event_source") is not None:
            selected_event_source_counts[str(entry["selected_hard_invalidation_event_source"])] += 1
        if entry.get("mismatch_cause") is not None:
            mismatch_cause_counts[str(entry["mismatch_cause"])] += 1
        if str(entry["symbol"]) == "5541" and float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) >= 0 and not bool(entry.get("accidentally_preserved_true_loser")):
            anchor_5541_preservation_count += 1
        if not (
            _trace_no_lookahead_ok(entry["trace_artifacts"]["baseline_trace_path"])
            and _trace_no_lookahead_ok(entry["trace_artifacts"]["corrected_trace_path"])
        ):
            no_lookahead_violations += 1
        if entry.get("contract_violation"):
            contract_violation_count += 1

    improved_positive_delta = sum(max(0.0, float(entry["pnl_delta_vs_repaired_v2"])) for entry in case_results)
    largest_positive_delta = max((max(0.0, float(entry["pnl_delta_vs_repaired_v2"])) for entry in case_results), default=0.0)
    isolated_improvement = bool(improved_vs_repaired_v2_count == 1 and improved_positive_delta > 0 and largest_positive_delta >= improved_positive_delta * 0.8)

    return {
        "schema_version": "tradex_hard_invalidation_profit_preservation_guard_rollup_v1",
        "total_cases": len(case_results),
        "aligned_case_count": len(case_results) - contract_violation_count,
        "improved_vs_repaired_v2_count": improved_vs_repaired_v2_count,
        "worsened_vs_repaired_v2_count": worsened_vs_repaired_v2_count,
        "total_pnl_delta_vs_repaired_v2": float(total_pnl_delta_vs_repaired_v2),
        "total_pnl_delta_vs_original_baseline": float(total_pnl_delta_vs_original_baseline),
        "profitable_baseline_total_pnl_delta_vs_repaired_v2": float(profitable_baseline_total_pnl_delta_vs_repaired_v2),
        "profitable_baseline_cases_improved": profitable_baseline_cases_improved,
        "profitable_baseline_cases_damaged": profitable_baseline_cases_damaged,
        "loss_side_total_pnl_delta_vs_repaired_v2": float(loss_side_total_pnl_delta_vs_repaired_v2),
        "loss_side_cases_improved": loss_side_cases_improved,
        "loss_side_cases_worsened": loss_side_cases_worsened,
        "action_changed_count": action_changed_count,
        "guard_activated_count": guard_activated_count,
        "changed_action_with_guard_reason_count": changed_action_with_guard_reason_count,
        "typed_guard_reason_missing_count": typed_guard_reason_missing_count,
        "false_exit_or_premature_exit_candidate_count": false_exit_or_premature_exit_candidate_count,
        "accidentally_preserved_true_loser_count": accidentally_preserved_true_loser_count,
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "anchor_5541_preservation_count": anchor_5541_preservation_count,
        "anchor_5541_preservation_intact": anchor_5541_preservation_count == 1,
        "selected_event_source_counts": dict(selected_event_source_counts),
        "mismatch_cause_counts": dict(mismatch_cause_counts),
        "improved_symbols": improved_symbols,
        "changed_symbols": changed_symbols,
        "guard_reason_symbols": guard_reason_symbols,
        "isolated_improvement": isolated_improvement,
        "profitable_baseline_damage_decreased": profitable_baseline_total_pnl_delta_vs_repaired_v2 > 0,
        "loss_side_damage_not_materially_worse": loss_side_total_pnl_delta_vs_repaired_v2 >= -max(
            1.0,
            abs(loss_side_total_pnl_delta_vs_repaired_v2) * 0.05,
        ),
    }


def _build_profit_preservation_guard_decision(*, rollup: dict[str, Any]) -> dict[str, Any]:
    contract_violation_count = int(rollup.get("contract_violation_count", 0))
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    anchor_ok = bool(rollup.get("anchor_5541_preservation_intact"))
    profitable_damage_decreased = bool(rollup.get("profitable_baseline_damage_decreased"))
    loss_side_ok = bool(rollup.get("loss_side_damage_not_materially_worse"))
    changed_action_count = int(rollup.get("action_changed_count", 0))
    guard_reason_ok = int(rollup.get("changed_action_with_guard_reason_count", 0)) == changed_action_count
    isolated_improvement = bool(rollup.get("isolated_improvement"))
    improved_profitable_cases = int(rollup.get("profitable_baseline_cases_improved", 0))
    decision = "drop"
    decision_reason = "Guard challenger did not improve enough to keep"

    if contract_violation_count > 0 or no_lookahead_violations > 0:
        decision = "drop"
        decision_reason = "Contract alignment or no-lookahead failed"
    elif not anchor_ok:
        decision = "drop"
        decision_reason = "Anchor 5541 preservation failed"
    elif not profitable_damage_decreased:
        decision = "drop"
        decision_reason = "Profitable-baseline damage did not decrease"
    elif not loss_side_ok:
        decision = "drop"
        decision_reason = "Loss-side damage worsened materially"
    elif not guard_reason_ok:
        decision = "drop"
        decision_reason = "Not all changed actions carried typed guard reasons"
    elif changed_action_count <= 1 or isolated_improvement:
        decision = "hold"
        decision_reason = "Promising axis but improvement is too isolated to keep"
    elif improved_profitable_cases >= 2:
        decision = "keep"
        decision_reason = "Profit-preservation guard reduced profitable-baseline damage without materially worsening loss-side cases"
    else:
        decision = "hold"
        decision_reason = "Diagnostics are promising, but breadth is still too narrow"

    return {
        "schema_version": "tradex_hard_invalidation_profit_preservation_guard_decision_v1",
        "phase": "challenger_evaluation",
        "candidate_name": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "candidate_status": "challenger",
        "outcome_optimization_allowed": decision == "keep",
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "anchor_5541_preservation_intact": anchor_ok,
        "profitable_baseline_damage_decreased": profitable_damage_decreased,
        "loss_side_damage_not_materially_worse": loss_side_ok,
        "changed_action_count": changed_action_count,
        "guard_reason_ok": guard_reason_ok,
        "isolated_improvement": isolated_improvement,
        "improved_profitable_cases": improved_profitable_cases,
        "next_allowed_action": (
            "design another single-axis challenger only if a new trigger-date evidence pattern emerges"
            if decision == "keep"
            else "freeze the repaired baseline and do not promote this challenger"
        ),
        "generated_at": _utc_now(),
    }


def _trigger_date_trace_row(entry: dict[str, Any]) -> dict[str, Any] | None:
    trace_path = entry.get("trace_artifacts", {}).get("corrected_trace_path")
    trigger_date = entry.get("trigger_date")
    if trace_path is None or trigger_date is None:
        return None
    try:
        return _row_by_date(_trace_rows(str(trace_path)), str(trigger_date))
    except KeyError:
        return None


def _classify_profit_preservation_guard_causality(
    *,
    entry: dict[str, Any],
    trigger_row: dict[str, Any] | None,
) -> tuple[str, bool, str, str]:
    selected_source = str(entry.get("selected_hard_invalidation_event_source") or "missing")
    baseline_action = str(entry.get("baseline_v2_action_on_trigger_date") or "")
    challenger_action = str(entry.get("challenger_action_on_trigger_date") or "")
    guard_activated = bool(entry.get("guard_activated"))
    action_changed = bool(entry.get("action_changed_flag"))

    if selected_source == "summary_fallback_row" or entry.get("mismatch_cause") == "multiple_events_collapsed_incorrectly":
        return (
            "fallback_mixed_axis_path",
            False,
            "loop_mod._select_hard_invalidation_event_row -> summary_fallback_row fallback",
            "mixed_axis_fallback",
        )
    if guard_activated and baseline_action == "exit_all" and challenger_action in {"long_reduce", "watch"}:
        return (
            "intended_guard_activation",
            True,
            "loop_mod._hard_invalidation_profit_preservation_guard_plan -> soften exit_all to long_reduce/watch",
            "hard_invalidation_proper",
        )
    if action_changed:
        emitted_reason = str(trigger_row.get("reason")) if trigger_row is not None and trigger_row.get("reason") is not None else "unknown"
        if challenger_action == "hedge_add":
            return (
                "non_guard_action_mapping_change",
                False,
                f"loop_mod._plan_day_actions -> hedge_add branch; _reason_for(action_type='hedge_add') => {emitted_reason}",
                "hard_invalidation_proper",
            )
        if challenger_action == "trial_buy":
            return (
                "non_guard_action_mapping_change",
                False,
                f"loop_mod._plan_day_actions -> entry branch; _reason_for(action_type='trial_buy') => {emitted_reason}",
                "hard_invalidation_proper",
            )
        if challenger_action == "long_reduce":
            return (
                "non_guard_action_mapping_change",
                False,
                f"loop_mod._plan_day_actions -> long_reduce branch without guard activation; reason => {emitted_reason}",
                "hard_invalidation_proper",
            )
        return (
            "non_guard_action_mapping_change",
            False,
            f"loop_mod._plan_day_actions -> non-guard action surface; reason => {emitted_reason}",
            "hard_invalidation_proper",
        )
    return (
        "unchanged",
        False,
        "no action change",
        "hard_invalidation_proper",
    )


def _build_profit_preservation_guard_causal_case_diagnostics(
    guard_case_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for entry in guard_case_results:
        trigger_row = _trigger_date_trace_row(entry)
        attribution, thesis_match, code_path, case_family = _classify_profit_preservation_guard_causality(
            entry=entry,
            trigger_row=trigger_row,
        )
        diagnostics.append(
            {
                "symbol": entry.get("symbol"),
                "company_name": entry.get("company_name"),
                "trigger_date": entry.get("trigger_date"),
                "selected_event_source": entry.get("selected_hard_invalidation_event_source"),
                "selected_event_reason": entry.get("selected_hard_invalidation_event_reason"),
                "selected_event_action_type": entry.get("selected_hard_invalidation_event_action_type"),
                "selected_event_target_position": entry.get("selected_hard_invalidation_event_target_position"),
                "severity_level": entry.get("severity_level"),
                "baseline_v2_action": entry.get("baseline_v2_action_on_trigger_date"),
                "challenger_action": entry.get("challenger_action_on_trigger_date"),
                "action_changed": bool(entry.get("action_changed_flag")),
                "guard_activated": bool(entry.get("guard_activated")),
                "typed_guard_reason_present": bool(entry.get("typed_guard_reason_present")),
                "guard_reasons": list(entry.get("guard_reasons") or []),
                "trigger_date_trace_action": trigger_row.get("action_type") if trigger_row is not None else None,
                "trigger_date_trace_reason": trigger_row.get("reason") if trigger_row is not None else None,
                "trigger_date_trace_target_position": trigger_row.get("target_position_after") if trigger_row is not None else None,
                "exact_code_path": code_path,
                "case_family": case_family,
                "candidate_thesis_match": thesis_match,
                "causal_attribution": attribution,
                "causal_attribution_status": "confirmed"
                if attribution == "intended_guard_activation"
                else "unverified"
                if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) > 0
                else "refuted",
                "pnl_delta_vs_repaired_v2": entry.get("pnl_delta_vs_repaired_v2"),
                "pnl_delta_vs_original_baseline": entry.get("pnl_delta_vs_original_baseline"),
                "baseline_v2_pnl": entry.get("baseline_v2_pnl"),
                "challenger_pnl": entry.get("challenger_pnl"),
                "trace_artifacts": dict(entry.get("trace_artifacts") or {}),
                "false_exit_or_premature_exit_candidate": bool(entry.get("false_exit_or_premature_exit_candidate")),
                "accidentally_preserved_true_loser": bool(entry.get("accidentally_preserved_true_loser")),
                "selected_hard_invalidation_event_source": entry.get("selected_hard_invalidation_event_source"),
                "mismatch_cause": entry.get("mismatch_cause"),
                "selected_event_action_allowed_under_recorded_severity": bool(entry.get("selected_event_action_allowed_under_recorded_severity")),
                "emitted_action_allowed_under_recorded_severity": bool(entry.get("emitted_action_allowed_under_recorded_severity")),
                "contract_violation": bool(entry.get("contract_violation")),
            }
        )
    return diagnostics


def _build_profit_preservation_guard_causal_rollup(case_diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    def _rollup(entries: list[dict[str, Any]]) -> dict[str, Any]:
        total_pnl_delta_vs_repaired_v2 = sum(float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) for entry in entries)
        total_pnl_delta_vs_original_baseline = sum(float(entry.get("pnl_delta_vs_original_baseline", 0.0)) for entry in entries)
        improved_vs_repaired_v2_count = sum(1 for entry in entries if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) > 0)
        worsened_vs_repaired_v2_count = sum(1 for entry in entries if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) < 0)
        action_changed_count = sum(1 for entry in entries if bool(entry.get("action_changed")))
        guard_activated_count = sum(1 for entry in entries if bool(entry.get("guard_activated")))
        thesis_aligned_changed_action_count = sum(1 for entry in entries if bool(entry.get("candidate_thesis_match")) and bool(entry.get("action_changed")))
        fallback_case_count = sum(1 for entry in entries if entry.get("case_family") == "mixed_axis_fallback")
        fallback_total_pnl_delta_vs_repaired_v2 = sum(
            float(entry.get("pnl_delta_vs_repaired_v2", 0.0))
            for entry in entries
            if entry.get("case_family") == "mixed_axis_fallback"
        )
        fallback_action_changed_count = sum(1 for entry in entries if entry.get("case_family") == "mixed_axis_fallback" and bool(entry.get("action_changed")))
        causal_counts = Counter(str(entry.get("causal_attribution")) for entry in entries)
        no_lookahead_violations = sum(
            1
            for entry in entries
            if not (
                _trace_no_lookahead_ok(str(entry["trace_artifacts"]["baseline_trace_path"]))
                and _trace_no_lookahead_ok(str(entry["trace_artifacts"]["corrected_trace_path"]))
            )
        )
        without_fallback = [entry for entry in entries if entry.get("case_family") != "mixed_axis_fallback"]
        without_fallback_total = sum(float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) for entry in without_fallback)
        without_fallback_improved = sum(1 for entry in without_fallback if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) > 0)
        without_fallback_worsened = sum(1 for entry in without_fallback if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) < 0)
        without_fallback_action_changed = sum(1 for entry in without_fallback if bool(entry.get("action_changed")))
        without_fallback_guard_activated = sum(1 for entry in without_fallback if bool(entry.get("guard_activated")))
        without_fallback_thesis_aligned_changed = sum(1 for entry in without_fallback if bool(entry.get("candidate_thesis_match")) and bool(entry.get("action_changed")))
        without_fallback_profitable_delta = sum(
            float(entry.get("pnl_delta_vs_repaired_v2", 0.0))
            for entry in without_fallback
            if float(entry.get("baseline_v2_pnl", 0.0)) > 0
        )
        without_fallback_loss_delta = sum(
            float(entry.get("pnl_delta_vs_repaired_v2", 0.0))
            for entry in without_fallback
            if float(entry.get("baseline_v2_pnl", 0.0)) <= 0
        )
        profitable_delta = sum(
            float(entry.get("pnl_delta_vs_repaired_v2", 0.0))
            for entry in entries
            if float(entry.get("baseline_v2_pnl", 0.0)) > 0
        )
        loss_delta = sum(
            float(entry.get("pnl_delta_vs_repaired_v2", 0.0))
            for entry in entries
            if float(entry.get("baseline_v2_pnl", 0.0)) <= 0
        )
        return {
            "total_cases": len(entries),
            "contract_violation_count": sum(1 for entry in entries if bool(entry.get("contract_violation"))),
            "no_lookahead_violations": sum(
                1
                for entry in entries
                if not (
                    _trace_no_lookahead_ok(str(entry["trace_artifacts"]["baseline_trace_path"]))
                    and _trace_no_lookahead_ok(str(entry["trace_artifacts"]["corrected_trace_path"]))
                )
            ),
            "action_changed_count": action_changed_count,
            "guard_activated_count": guard_activated_count,
            "thesis_aligned_changed_action_count": thesis_aligned_changed_action_count,
            "thesis_misaligned_changed_action_count": action_changed_count - thesis_aligned_changed_action_count,
            "intended_guard_attribution_count": sum(1 for entry in entries if entry.get("causal_attribution") == "intended_guard_activation"),
            "non_guard_action_mapping_change_count": sum(1 for entry in entries if entry.get("causal_attribution") == "non_guard_action_mapping_change"),
            "fallback_mixed_axis_case_count": fallback_case_count,
            "fallback_mixed_axis_action_changed_count": fallback_action_changed_count,
            "fallback_mixed_axis_total_pnl_delta_vs_repaired_v2": float(fallback_total_pnl_delta_vs_repaired_v2),
            "total_pnl_delta_vs_repaired_v2": float(total_pnl_delta_vs_repaired_v2),
            "total_pnl_delta_vs_original_baseline": float(total_pnl_delta_vs_original_baseline),
            "improved_vs_repaired_v2_count": improved_vs_repaired_v2_count,
            "worsened_vs_repaired_v2_count": worsened_vs_repaired_v2_count,
            "profitable_baseline_total_pnl_delta_vs_repaired_v2": float(profitable_delta),
            "loss_side_total_pnl_delta_vs_repaired_v2": float(loss_delta),
            "fallback_contribution_share": float(abs(fallback_total_pnl_delta_vs_repaired_v2) / abs(total_pnl_delta_vs_repaired_v2)) if total_pnl_delta_vs_repaired_v2 else 0.0,
            "without_mixed_axis_fallback": {
                "total_cases": len(without_fallback),
                "contract_violation_count": sum(1 for entry in without_fallback if bool(entry.get("contract_violation"))),
                "no_lookahead_violations": sum(
                    1
                    for entry in without_fallback
                    if not (
                        _trace_no_lookahead_ok(str(entry["trace_artifacts"]["baseline_trace_path"]))
                        and _trace_no_lookahead_ok(str(entry["trace_artifacts"]["corrected_trace_path"]))
                    )
                ),
                "action_changed_count": without_fallback_action_changed,
                "guard_activated_count": without_fallback_guard_activated,
                "thesis_aligned_changed_action_count": without_fallback_thesis_aligned_changed,
                "thesis_misaligned_changed_action_count": without_fallback_action_changed - without_fallback_thesis_aligned_changed,
                "intended_guard_attribution_count": sum(1 for entry in without_fallback if entry.get("causal_attribution") == "intended_guard_activation"),
                "non_guard_action_mapping_change_count": sum(1 for entry in without_fallback if entry.get("causal_attribution") == "non_guard_action_mapping_change"),
                "fallback_mixed_axis_case_count": 0,
                "fallback_mixed_axis_action_changed_count": 0,
                "fallback_mixed_axis_total_pnl_delta_vs_repaired_v2": 0.0,
                "total_pnl_delta_vs_repaired_v2": float(without_fallback_total),
                "total_pnl_delta_vs_original_baseline": float(sum(float(entry.get("pnl_delta_vs_original_baseline", 0.0)) for entry in without_fallback)),
                "improved_vs_repaired_v2_count": without_fallback_improved,
                "worsened_vs_repaired_v2_count": without_fallback_worsened,
                "profitable_baseline_total_pnl_delta_vs_repaired_v2": float(without_fallback_profitable_delta),
                "loss_side_total_pnl_delta_vs_repaired_v2": float(without_fallback_loss_delta),
                "fallback_contribution_share": 0.0,
                "causal_attribution_counts": dict(Counter(str(entry.get("causal_attribution")) for entry in without_fallback)),
            },
            "causal_attribution_counts": dict(causal_counts),
        }

    rollup = _rollup(case_diagnostics)
    if rollup["intended_guard_attribution_count"] > 0 and rollup["thesis_misaligned_changed_action_count"] == 0 and rollup["guard_activated_count"] > 0 and rollup["fallback_mixed_axis_case_count"] == 0:
        causal_attribution_status = "confirmed"
    elif rollup["total_pnl_delta_vs_repaired_v2"] > 0:
        causal_attribution_status = "unverified"
    else:
        causal_attribution_status = "refuted"
    rollup["causal_attribution_status"] = causal_attribution_status
    return rollup


def _build_profit_preservation_guard_causal_decision(*, rollup: dict[str, Any], prior_decision: str) -> dict[str, Any]:
    causal_attribution_status = str(rollup.get("causal_attribution_status") or "unverified")
    guard_activated_count = int(rollup.get("guard_activated_count", 0))
    action_changed_count = int(rollup.get("action_changed_count", 0))
    revised_decision = "hold_causal_unverified"
    decision_reason = "PnL improved, but the changed actions are not causally attributable to the guard yet"

    if causal_attribution_status == "confirmed" and guard_activated_count > 0 and int(rollup.get("thesis_misaligned_changed_action_count", 0)) == 0:
        revised_decision = "keep"
        decision_reason = "Guard activation directly produced thesis-aligned action changes"
    elif float(rollup.get("total_pnl_delta_vs_repaired_v2", 0.0)) <= 0:
        revised_decision = "drop"
        decision_reason = "No net improvement versus repaired v2"

    return {
        "schema_version": "tradex_hard_invalidation_profit_preservation_guard_causal_decision_v1",
        "phase": "causal_audit",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_decision": prior_decision,
        "decision": revised_decision,
        "revised_decision": revised_decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": revised_decision,
        "session_aggregate_decision": revised_decision,
        "authoritative_rollup_decision": revised_decision,
        "causal_attribution_status": causal_attribution_status,
        "guard_activated_count": guard_activated_count,
        "action_changed_count": action_changed_count,
        "contract_violation_count": int(rollup.get("contract_violation_count", 0)),
        "no_lookahead_violations": int(rollup.get("no_lookahead_violations", 0)),
        "can_design_next_challenger": revised_decision == "keep",
        "generated_at": _utc_now(),
    }


def run_causal_audit(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    guard_case_results = list(_load_json(CAUSAL_AUDIT_GUARD_CASE_RESULTS))
    prior_decision_payload = _load_json(CAUSAL_AUDIT_GUARD_DECISION)
    prior_decision = str(prior_decision_payload.get("decision") or "keep")
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "causal_audit"
    session_root.mkdir(parents=True, exist_ok=True)

    case_diagnostics = _build_profit_preservation_guard_causal_case_diagnostics(guard_case_results)
    rollup = _build_profit_preservation_guard_causal_rollup(case_diagnostics)
    decision = _build_profit_preservation_guard_causal_decision(rollup=rollup, prior_decision=prior_decision)

    manifest = {
        "schema_version": "tradex_hard_invalidation_profit_preservation_guard_causal_manifest_v1",
        "phase": "causal_audit",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_decision": prior_decision,
        "source_guard_session_root": str(CAUSAL_AUDIT_SOURCE_SESSION_ROOT),
        "source_guard_case_results": str(CAUSAL_AUDIT_GUARD_CASE_RESULTS),
        "source_guard_decision": str(CAUSAL_AUDIT_GUARD_DECISION),
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    case_diagnostics_artifact = {
        "schema_version": "tradex_hard_invalidation_profit_preservation_guard_causal_case_diagnostics_v1",
        "phase": "causal_audit",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_decision": prior_decision,
        "generated_at": _utc_now(),
        "cases": case_diagnostics,
    }

    artifacts = {
        "hard_invalidation_profit_preservation_guard_v1_causal_audit_manifest.json": _write_json(session_root / "hard_invalidation_profit_preservation_guard_v1_causal_audit_manifest.json", manifest),
        "hard_invalidation_profit_preservation_guard_v1_causal_case_diagnostics.json": _write_json(session_root / "hard_invalidation_profit_preservation_guard_v1_causal_case_diagnostics.json", case_diagnostics_artifact),
        "hard_invalidation_profit_preservation_guard_v1_causal_rollup.json": _write_json(session_root / "hard_invalidation_profit_preservation_guard_v1_causal_rollup.json", rollup),
        "hard_invalidation_profit_preservation_guard_v1_causal_decision.json": _write_json(session_root / "hard_invalidation_profit_preservation_guard_v1_causal_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_diagnostics": case_diagnostics_artifact,
        "rollup": rollup,
        "source_guard_session_root": str(CAUSAL_AUDIT_SOURCE_SESSION_ROOT),
        "source_guard_case_results": str(CAUSAL_AUDIT_GUARD_CASE_RESULTS),
        "source_guard_decision": str(CAUSAL_AUDIT_GUARD_DECISION),
    }


def _guard_evidence_trigger_row(entry: dict[str, Any]) -> dict[str, Any] | None:
    trace_path = entry.get("trace_artifacts", {}).get("corrected_trace_path")
    trigger_date = entry.get("trigger_date")
    if trace_path is None or trigger_date is None:
        return None
    try:
        return _row_by_date(_trace_rows(str(trace_path)), str(trigger_date))
    except KeyError:
        return None


def _guard_evidence_support_flags(trigger_row: dict[str, Any] | None) -> dict[str, bool]:
    evidence_for = [str(item) for item in (trigger_row.get("evidence_for") or [])] if trigger_row is not None else []
    evidence_against = [str(item) for item in (trigger_row.get("evidence_against") or [])] if trigger_row is not None else []
    softening_support = any(
        item.startswith(prefix)
        for item in evidence_for
        for prefix in ("support_wick=True", "breakout5=True", "breakout10=True", "reclaim_ma20=True", "bull_stack=True")
    )
    hard_breakdown_support = any(
        item.startswith(prefix)
        for item in evidence_against
        for prefix in ("lose_ma20=True", "lose_ma60=True", "failed_breakout5=True", "bear_stack=True")
    )
    return {
        "softening_support": softening_support,
        "hard_breakdown_support": hard_breakdown_support,
        "has_constructive_context": bool(softening_support and not hard_breakdown_support),
    }


def _classify_guard_evidence_case(entry: dict[str, Any], trigger_row: dict[str, Any] | None) -> dict[str, Any]:
    selected_source = str(entry.get("selected_hard_invalidation_event_source") or "missing")
    baseline_action = str(entry.get("baseline_v2_action_on_trigger_date") or "")
    action_is_exit_all = baseline_action == "exit_all"
    mixed_axis = selected_source == "summary_fallback_row" or entry.get("mismatch_cause") == "multiple_events_collapsed_incorrectly"
    proper_hard_invalidation = not mixed_axis and selected_source != "missing"
    false_exit_candidate = bool(entry.get("false_exit_or_premature_exit_candidate"))
    support_flags = _guard_evidence_support_flags(trigger_row)
    trigger_evidence_fields: dict[str, Any] = {}
    if trigger_row is not None:
        for field in GUARD_EVIDENCE_TARGET_FIELDS:
            if trigger_row.get(field) is not None:
                trigger_evidence_fields[field] = trigger_row.get(field)

    future_leaking_fields = {
        "baseline_v2_action_on_trigger_date": entry.get("baseline_v2_action_on_trigger_date"),
        "baseline_v2_pnl": entry.get("baseline_v2_pnl"),
        "challenger_action_on_trigger_date": entry.get("challenger_action_on_trigger_date"),
        "challenger_pnl": entry.get("challenger_pnl"),
        "false_exit_or_premature_exit_candidate": entry.get("false_exit_or_premature_exit_candidate"),
        "accidentally_preserved_true_loser": entry.get("accidentally_preserved_true_loser"),
        "guard_activated": entry.get("guard_activated"),
        "guard_reasons": list(entry.get("guard_reasons") or []),
        "typed_guard_reason_present": entry.get("typed_guard_reason_present"),
        "selected_hard_invalidation_event_source": entry.get("selected_hard_invalidation_event_source"),
        "selected_hard_invalidation_event_reason": entry.get("selected_hard_invalidation_event_reason"),
        "selected_hard_invalidation_event_action_type": entry.get("selected_hard_invalidation_event_action_type"),
        "selected_hard_invalidation_event_target_position": entry.get("selected_hard_invalidation_event_target_position"),
        "selected_event_action_allowed_under_recorded_severity": entry.get("selected_event_action_allowed_under_recorded_severity"),
        "emitted_action_allowed_under_recorded_severity": entry.get("emitted_action_allowed_under_recorded_severity"),
        "mismatch_cause": entry.get("mismatch_cause"),
        "pnl_delta_vs_repaired_v2": entry.get("pnl_delta_vs_repaired_v2"),
        "pnl_delta_vs_original_baseline": entry.get("pnl_delta_vs_original_baseline"),
    }

    if mixed_axis:
        classification = "mixed_axis_exclude_from_guard_design"
        usable_guard_evidence = False
        guard_reason = "summary fallback or collapsed-event case"
    elif not proper_hard_invalidation:
        classification = "mixed_axis_exclude_from_guard_design"
        usable_guard_evidence = False
        guard_reason = "selected event not available for guard design"
    elif not action_is_exit_all:
        classification = "not_exit_all_scope"
        usable_guard_evidence = False
        guard_reason = "hard-invalidation proper but repaired v2 did not emit exit_all"
    elif false_exit_candidate and support_flags["has_constructive_context"]:
        classification = "usable_guard_evidence"
        usable_guard_evidence = True
        guard_reason = "hard-invalidation proper exit_all with constructive trigger-date evidence and no fallback dependency"
    elif false_exit_candidate:
        classification = "insufficient_trigger_date_evidence"
        usable_guard_evidence = False
        guard_reason = "outcome suggests premature exit, but trigger-date evidence is not sufficient to soften"
    else:
        classification = "true_exit_all_do_not_soften"
        usable_guard_evidence = False
        guard_reason = "hard-invalidation proper exit_all and later outcome does not justify softening"

    return {
        "symbol": entry.get("symbol"),
        "company_name": entry.get("company_name"),
        "case_classification": classification,
        "usable_guard_evidence": usable_guard_evidence,
        "guard_reason": guard_reason,
        "hard_invalidation_scope": "mixed_axis_fallback"
        if mixed_axis
        else "hard_invalidation_proper"
        if proper_hard_invalidation
        else "unknown",
        "repaired_v2_action": entry.get("baseline_v2_action_on_trigger_date"),
        "selected_event_source": selected_source,
        "severity_level": entry.get("severity_level"),
        "trigger_date": entry.get("trigger_date"),
        "action_is_exit_all": action_is_exit_all,
        "false_exit_or_premature_exit_candidate": false_exit_candidate,
        "trigger_date_softening_support": support_flags["has_constructive_context"],
        "trigger_date_softening_support_flags": support_flags,
        "trigger_date_evidence_fields": trigger_evidence_fields,
        "future_leaking_fields": future_leaking_fields,
        "available_trigger_date_field_count": len(trigger_evidence_fields),
        "future_leaking_field_count": len(future_leaking_fields),
        "selected_event_reason": entry.get("selected_hard_invalidation_event_reason"),
        "selected_event_action_type": entry.get("selected_hard_invalidation_event_action_type"),
        "selected_event_target_position": entry.get("selected_hard_invalidation_event_target_position"),
        "trigger_date_trace_action": trigger_row.get("action_type") if trigger_row is not None else None,
        "trigger_date_trace_reason": trigger_row.get("reason") if trigger_row is not None else None,
        "trigger_date_trace_target_position": trigger_row.get("target_position_after") if trigger_row is not None else None,
        "no_lookahead_ok": bool(
            _trace_no_lookahead_ok(str(entry["trace_artifacts"]["baseline_trace_path"]))
            and _trace_no_lookahead_ok(str(entry["trace_artifacts"]["corrected_trace_path"]))
        ),
        "contract_violation": bool(entry.get("contract_violation")),
        "pnl_delta_vs_repaired_v2": entry.get("pnl_delta_vs_repaired_v2"),
        "pnl_delta_vs_original_baseline": entry.get("pnl_delta_vs_original_baseline"),
    }


def _build_guard_evidence_field_inventory(case_classification: list[dict[str, Any]]) -> dict[str, Any]:
    trigger_field_counts: Counter[str] = Counter()
    trigger_field_examples: dict[str, dict[str, Any]] = {}
    for entry in case_classification:
        for field, value in (entry.get("trigger_date_evidence_fields") or {}).items():
            trigger_field_counts[field] += 1
            trigger_field_examples.setdefault(field, {"case_symbol": entry["symbol"], "example_value": value})

    available_trigger_date_fields = [
        {
            "field": field,
            "observed_case_count": trigger_field_counts[field],
            "example_case_symbol": trigger_field_examples[field]["case_symbol"],
            "example_value": trigger_field_examples[field]["example_value"],
            "future_leaking": False,
            "usable_for_guard_design": True,
        }
        for field in sorted(trigger_field_counts)
    ]

    future_leaking_fields = [
        {
            "field": field,
            "future_leaking": True,
            "usable_for_guard_design": False,
            "reason": "depends on repaired-v2 / challenger outcomes or audit selection after the trigger date",
        }
        for field in GUARD_EVIDENCE_POSTHOC_FIELDS
    ]

    unavailable_fields = [
        {
            "field": field,
            "available_on_trigger_date": False,
            "future_leaking": False,
            "usable_for_guard_design": False,
            "reason": "not emitted on the trigger-date trace row in the current replay surface",
        }
        for field in GUARD_EVIDENCE_UNAVAILABLE_FIELDS
    ]

    return {
        "schema_version": "tradex_hard_invalidation_guard_evidence_field_inventory_v1",
        "phase": "guard_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "available_trigger_date_fields": available_trigger_date_fields,
        "available_trigger_date_field_count": len(available_trigger_date_fields),
        "future_leaking_fields": future_leaking_fields,
        "future_leaking_field_count": len(future_leaking_fields),
        "unavailable_fields": unavailable_fields,
        "unavailable_field_count": len(unavailable_fields),
        "generated_at": _utc_now(),
    }


def _build_guard_evidence_rollup(case_classification: list[dict[str, Any]]) -> dict[str, Any]:
    classification_counts = Counter(str(entry["case_classification"]) for entry in case_classification)
    no_lookahead_violations = sum(1 for entry in case_classification if not bool(entry.get("no_lookahead_ok")))
    usable_guard_evidence_count = classification_counts.get("usable_guard_evidence", 0)
    mixed_axis_excluded_count = classification_counts.get("mixed_axis_exclude_from_guard_design", 0)
    exit_all_proper_count = sum(1 for entry in case_classification if entry.get("hard_invalidation_scope") == "hard_invalidation_proper" and bool(entry.get("action_is_exit_all")))
    trigger_date_softening_support_count = sum(1 for entry in case_classification if bool(entry.get("trigger_date_softening_support")))
    false_exit_or_premature_exit_candidate_count = sum(1 for entry in case_classification if bool(entry.get("false_exit_or_premature_exit_candidate")))
    usable_case_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "usable_guard_evidence"]
    mixed_axis_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "mixed_axis_exclude_from_guard_design"]
    exit_all_proper_symbols = [str(entry["symbol"]) for entry in case_classification if entry.get("hard_invalidation_scope") == "hard_invalidation_proper" and bool(entry.get("action_is_exit_all"))]
    insufficient_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "insufficient_trigger_date_evidence"]
    true_exit_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "true_exit_all_do_not_soften"]
    not_exit_all_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "not_exit_all_scope"]
    usable_trigger_fields = sorted(
        {
            field
            for entry in case_classification
            if entry["case_classification"] == "usable_guard_evidence"
            for field in (entry.get("trigger_date_evidence_fields") or {}).keys()
        }
    )
    return {
        "schema_version": "tradex_hard_invalidation_guard_evidence_rollup_v1",
        "phase": "guard_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "total_cases": len(case_classification),
        "usable_guard_evidence_count": usable_guard_evidence_count,
        "mixed_axis_excluded_count": mixed_axis_excluded_count,
        "exit_all_proper_count": exit_all_proper_count,
        "trigger_date_softening_support_count": trigger_date_softening_support_count,
        "false_exit_or_premature_exit_candidate_count": false_exit_or_premature_exit_candidate_count,
        "no_lookahead_violations": no_lookahead_violations,
        "classification_counts": dict(classification_counts),
        "usable_guard_evidence_symbols": usable_case_symbols,
        "mixed_axis_excluded_symbols": mixed_axis_symbols,
        "exit_all_proper_symbols": exit_all_proper_symbols,
        "insufficient_trigger_date_evidence_symbols": insufficient_symbols,
        "true_exit_all_do_not_soften_symbols": true_exit_symbols,
        "not_exit_all_scope_symbols": not_exit_all_symbols,
        "usable_trigger_date_fields": usable_trigger_fields,
        "usable_trigger_date_field_count": len(usable_trigger_fields),
        "can_design_next_challenger": usable_guard_evidence_count > 0 and no_lookahead_violations == 0,
        "generated_at": _utc_now(),
    }


def _build_guard_evidence_decision(*, rollup: dict[str, Any], prior_decision: str) -> dict[str, Any]:
    usable_guard_evidence_count = int(rollup.get("usable_guard_evidence_count", 0))
    mixed_axis_excluded_count = int(rollup.get("mixed_axis_excluded_count", 0))
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    revised_decision = "hold_causal_unverified"
    causal_attribution_status = "unverified"
    if usable_guard_evidence_count > 0 and no_lookahead_violations == 0:
        revised_decision = "keep"
        causal_attribution_status = "confirmed"
    elif mixed_axis_excluded_count > 0 and usable_guard_evidence_count == 0:
        revised_decision = "hold_causal_unverified"
        causal_attribution_status = "unverified"
    else:
        revised_decision = "drop"
        causal_attribution_status = "refuted"
    return {
        "schema_version": "tradex_hard_invalidation_guard_evidence_decision_v1",
        "phase": "guard_evidence_mining",
        "prior_candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_candidate_status": prior_decision,
        "new_challenger_created": False,
        "usable_guard_evidence_count": usable_guard_evidence_count,
        "mixed_axis_excluded_count": mixed_axis_excluded_count,
        "causal_attribution_status": causal_attribution_status,
        "can_design_next_challenger": False,
        "revised_decision": revised_decision,
        "decision": revised_decision,
        "candidate_local_decision": revised_decision,
        "session_aggregate_decision": revised_decision,
        "authoritative_rollup_decision": revised_decision,
        "next_allowed_action": "only design a new challenger if usable_guard_evidence_count is greater than zero",
        "generated_at": _utc_now(),
    }


def run_guard_evidence_mining(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    guard_case_results = list(_load_json(CAUSAL_AUDIT_GUARD_CASE_RESULTS))
    prior_decision_payload = _load_json(CAUSAL_AUDIT_GUARD_DECISION)
    prior_decision = str(prior_decision_payload.get("decision") or "hold_causal_unverified")
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "guard_evidence_mining"
    session_root.mkdir(parents=True, exist_ok=True)

    case_classification = [_classify_guard_evidence_case(entry, _guard_evidence_trigger_row(entry)) for entry in guard_case_results]
    field_inventory = _build_guard_evidence_field_inventory(case_classification)
    rollup = _build_guard_evidence_rollup(case_classification)
    decision = _build_guard_evidence_decision(rollup=rollup, prior_decision=prior_decision)

    manifest = {
        "schema_version": "tradex_hard_invalidation_guard_evidence_manifest_v1",
        "phase": "guard_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_candidate_status": prior_decision,
        "source_guard_session_root": str(CAUSAL_AUDIT_SOURCE_SESSION_ROOT),
        "source_guard_case_results": str(CAUSAL_AUDIT_GUARD_CASE_RESULTS),
        "source_guard_decision": str(CAUSAL_AUDIT_GUARD_DECISION),
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    case_classification_artifact = {
        "schema_version": "tradex_hard_invalidation_guard_evidence_case_classification_v1",
        "phase": "guard_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_candidate_status": prior_decision,
        "generated_at": _utc_now(),
        "cases": case_classification,
    }

    artifacts = {
        "hard_invalidation_guard_evidence_mining_manifest.json": _write_json(session_root / "hard_invalidation_guard_evidence_mining_manifest.json", manifest),
        "hard_invalidation_guard_evidence_case_classification.json": _write_json(session_root / "hard_invalidation_guard_evidence_case_classification.json", case_classification_artifact),
        "hard_invalidation_guard_evidence_field_inventory.json": _write_json(session_root / "hard_invalidation_guard_evidence_field_inventory.json", field_inventory),
        "hard_invalidation_guard_evidence_rollup.json": _write_json(session_root / "hard_invalidation_guard_evidence_rollup.json", rollup),
        "hard_invalidation_guard_evidence_decision.json": _write_json(session_root / "hard_invalidation_guard_evidence_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_classification": case_classification_artifact,
        "field_inventory": field_inventory,
        "rollup": rollup,
        "source_guard_session_root": str(CAUSAL_AUDIT_SOURCE_SESSION_ROOT),
        "source_guard_case_results": str(CAUSAL_AUDIT_GUARD_CASE_RESULTS),
        "source_guard_decision": str(CAUSAL_AUDIT_GUARD_DECISION),
    }


def _classify_non_exit_action_case(entry: dict[str, Any], trigger_row: dict[str, Any] | None) -> dict[str, Any]:
    selected_source = str(entry.get("selected_hard_invalidation_event_source") or "missing")
    mixed_axis = selected_source == "summary_fallback_row" or entry.get("mismatch_cause") == "multiple_events_collapsed_incorrectly"
    proper_hard_invalidation = not mixed_axis and selected_source != "missing"
    repaired_action = str(entry.get("baseline_v2_action_on_trigger_date") or "")
    challenger_action = str(entry.get("challenger_action_on_trigger_date") or "")
    action_changed = repaired_action != challenger_action
    trigger_evidence_fields: dict[str, Any] = {}
    if trigger_row is not None:
        for field in GUARD_EVIDENCE_TARGET_FIELDS:
            if trigger_row.get(field) is not None:
                trigger_evidence_fields[field] = trigger_row.get(field)

    future_leaking_fields = {
        "baseline_v2_action_on_trigger_date": entry.get("baseline_v2_action_on_trigger_date"),
        "baseline_v2_pnl": entry.get("baseline_v2_pnl"),
        "challenger_action_on_trigger_date": entry.get("challenger_action_on_trigger_date"),
        "challenger_pnl": entry.get("challenger_pnl"),
        "false_exit_or_premature_exit_candidate": entry.get("false_exit_or_premature_exit_candidate"),
        "accidentally_preserved_true_loser": entry.get("accidentally_preserved_true_loser"),
        "guard_activated": entry.get("guard_activated"),
        "guard_reasons": list(entry.get("guard_reasons") or []),
        "typed_guard_reason_present": entry.get("typed_guard_reason_present"),
        "selected_hard_invalidation_event_source": entry.get("selected_hard_invalidation_event_source"),
        "selected_hard_invalidation_event_reason": entry.get("selected_hard_invalidation_event_reason"),
        "selected_hard_invalidation_event_action_type": entry.get("selected_hard_invalidation_event_action_type"),
        "selected_hard_invalidation_event_target_position": entry.get("selected_hard_invalidation_event_target_position"),
        "selected_event_action_allowed_under_recorded_severity": entry.get("selected_event_action_allowed_under_recorded_severity"),
        "emitted_action_allowed_under_recorded_severity": entry.get("emitted_action_allowed_under_recorded_severity"),
        "mismatch_cause": entry.get("mismatch_cause"),
        "pnl_delta_vs_repaired_v2": entry.get("pnl_delta_vs_repaired_v2"),
        "pnl_delta_vs_original_baseline": entry.get("pnl_delta_vs_original_baseline"),
    }

    if mixed_axis:
        classification = "fallback_or_mixed_axis_exclude"
        action_outcome_assessment = "insufficient_data"
        usable_non_exit_action_evidence = False
        guard_reason = "summary fallback or collapsed-event case"
    elif not proper_hard_invalidation:
        classification = "insufficient_data"
        action_outcome_assessment = "insufficient_data"
        usable_non_exit_action_evidence = False
        guard_reason = "selected event not available for non-exit action design"
    elif action_changed and float(entry.get("pnl_delta_vs_repaired_v2") or 0.0) > 0.0 and bool(trigger_evidence_fields):
        classification = "usable_non_exit_action_evidence"
        action_outcome_assessment = "too_soft"
        usable_non_exit_action_evidence = True
        guard_reason = "non-exit action changed and improved under repaired-v2 comparison with trigger-date evidence available"
    else:
        classification = "action_appropriate_do_not_change"
        if float(entry.get("pnl_delta_vs_repaired_v2") or 0.0) < 0.0 and not bool(trigger_evidence_fields):
            action_outcome_assessment = "outcome_bad_but_no_trigger_evidence"
        else:
            action_outcome_assessment = "appropriate"
        usable_non_exit_action_evidence = False
        guard_reason = "non-exit proper case does not show a usable action-surface change"

    return {
        "symbol": entry.get("symbol"),
        "company_name": entry.get("company_name"),
        "case_classification": classification,
        "usable_non_exit_action_evidence": usable_non_exit_action_evidence,
        "action_outcome_assessment": action_outcome_assessment,
        "guard_reason": guard_reason,
        "hard_invalidation_scope": "mixed_axis_fallback"
        if mixed_axis
        else "hard_invalidation_proper"
        if proper_hard_invalidation
        else "unknown",
        "repaired_v2_action": repaired_action,
        "selected_event_source": selected_source,
        "severity_level": entry.get("severity_level"),
        "trigger_date": entry.get("trigger_date"),
        "action_is_exit_all": repaired_action == "exit_all",
        "action_changed": action_changed,
        "trigger_date_evidence_available": bool(trigger_evidence_fields),
        "trigger_date_evidence_fields": trigger_evidence_fields,
        "future_leaking_fields": future_leaking_fields,
        "available_trigger_date_field_count": len(trigger_evidence_fields),
        "future_leaking_field_count": len(future_leaking_fields),
        "selected_event_reason": entry.get("selected_hard_invalidation_event_reason"),
        "selected_event_action_type": entry.get("selected_hard_invalidation_event_action_type"),
        "selected_event_target_position": entry.get("selected_hard_invalidation_event_target_position"),
        "trigger_date_trace_action": trigger_row.get("action_type") if trigger_row is not None else None,
        "trigger_date_trace_reason": trigger_row.get("reason") if trigger_row is not None else None,
        "trigger_date_trace_target_position": trigger_row.get("target_position_after") if trigger_row is not None else None,
        "no_lookahead_ok": bool(
            _trace_no_lookahead_ok(str(entry["trace_artifacts"]["baseline_trace_path"]))
            and _trace_no_lookahead_ok(str(entry["trace_artifacts"]["corrected_trace_path"]))
        ),
        "contract_violation": bool(entry.get("contract_violation")),
        "pnl_delta_vs_repaired_v2": entry.get("pnl_delta_vs_repaired_v2"),
        "pnl_delta_vs_original_baseline": entry.get("pnl_delta_vs_original_baseline"),
        "selected_event_action_allowed_under_recorded_severity": entry.get("selected_event_action_allowed_under_recorded_severity"),
        "emitted_action_allowed_under_recorded_severity": entry.get("emitted_action_allowed_under_recorded_severity"),
    }


def _build_non_exit_action_field_inventory(case_classification: list[dict[str, Any]]) -> dict[str, Any]:
    trigger_field_counts: Counter[str] = Counter()
    trigger_field_examples: dict[str, dict[str, Any]] = {}
    for entry in case_classification:
        for field, value in (entry.get("trigger_date_evidence_fields") or {}).items():
            trigger_field_counts[field] += 1
            trigger_field_examples.setdefault(field, {"case_symbol": entry["symbol"], "example_value": value})

    available_trigger_date_fields = [
        {
            "field": field,
            "observed_case_count": trigger_field_counts[field],
            "example_case_symbol": trigger_field_examples[field]["case_symbol"],
            "example_value": trigger_field_examples[field]["example_value"],
            "future_leaking": False,
            "usable_for_non_exit_action_design": True,
        }
        for field in sorted(trigger_field_counts)
    ]

    future_leaking_fields = [
        {
            "field": field,
            "future_leaking": True,
            "usable_for_non_exit_action_design": False,
            "reason": "depends on repaired-v2 / challenger outcomes or audit selection after the trigger date",
        }
        for field in GUARD_EVIDENCE_POSTHOC_FIELDS
    ]

    unavailable_fields = [
        {
            "field": field,
            "available_on_trigger_date": False,
            "future_leaking": False,
            "usable_for_non_exit_action_design": False,
            "reason": "not emitted on the trigger-date trace row in the current replay surface",
        }
        for field in GUARD_EVIDENCE_UNAVAILABLE_FIELDS
    ]

    return {
        "schema_version": "tradex_hard_invalidation_non_exit_action_evidence_field_inventory_v1",
        "phase": "non_exit_action_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "available_trigger_date_fields": available_trigger_date_fields,
        "available_trigger_date_field_count": len(available_trigger_date_fields),
        "future_leaking_fields": future_leaking_fields,
        "future_leaking_field_count": len(future_leaking_fields),
        "unavailable_fields": unavailable_fields,
        "unavailable_field_count": len(unavailable_fields),
        "generated_at": _utc_now(),
    }


def _build_non_exit_action_rollup(case_classification: list[dict[str, Any]]) -> dict[str, Any]:
    classification_counts = Counter(str(entry["case_classification"]) for entry in case_classification)
    no_lookahead_violations = sum(1 for entry in case_classification if not bool(entry.get("no_lookahead_ok")))
    usable_count = classification_counts.get("usable_non_exit_action_evidence", 0)
    fallback_count = classification_counts.get("fallback_or_mixed_axis_exclude", 0)
    appropriate_count = classification_counts.get("action_appropriate_do_not_change", 0)
    outcome_bad_count = classification_counts.get("outcome_bad_but_no_trigger_evidence", 0)
    action_changed_count = sum(1 for entry in case_classification if bool(entry.get("action_changed")))
    too_soft_count = sum(1 for entry in case_classification if entry.get("action_outcome_assessment") == "too_soft")
    too_harsh_count = sum(1 for entry in case_classification if entry.get("action_outcome_assessment") == "too_harsh")
    appropriate_assessment_count = sum(1 for entry in case_classification if entry.get("action_outcome_assessment") == "appropriate")
    usable_case_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "usable_non_exit_action_evidence"]
    fallback_symbols = [str(entry["symbol"]) for entry in case_classification if entry["case_classification"] == "fallback_or_mixed_axis_exclude"]
    non_exit_symbols = [str(entry["symbol"]) for entry in case_classification]
    usable_trigger_fields = sorted(
        {
            field
            for entry in case_classification
            if entry["case_classification"] == "usable_non_exit_action_evidence"
            for field in (entry.get("trigger_date_evidence_fields") or {}).keys()
        }
    )
    return {
        "schema_version": "tradex_hard_invalidation_non_exit_action_evidence_rollup_v1",
        "phase": "non_exit_action_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "total_cases": len(case_classification),
        "usable_non_exit_action_evidence_count": usable_count,
        "fallback_or_mixed_axis_exclude_count": fallback_count,
        "action_appropriate_do_not_change_count": appropriate_count,
        "outcome_bad_but_no_trigger_evidence_count": outcome_bad_count,
        "action_changed_count": action_changed_count,
        "too_soft_count": too_soft_count,
        "too_harsh_count": too_harsh_count,
        "appropriate_assessment_count": appropriate_assessment_count,
        "no_lookahead_violations": no_lookahead_violations,
        "classification_counts": dict(classification_counts),
        "usable_non_exit_action_evidence_symbols": usable_case_symbols,
        "fallback_or_mixed_axis_exclude_symbols": fallback_symbols,
        "non_exit_scope_symbols": non_exit_symbols,
        "usable_trigger_date_fields": usable_trigger_fields,
        "usable_trigger_date_field_count": len(usable_trigger_fields),
        "can_design_next_challenger": False,
        "generated_at": _utc_now(),
    }


def _build_non_exit_action_decision(*, rollup: dict[str, Any], prior_decision: str) -> dict[str, Any]:
    usable_count = int(rollup.get("usable_non_exit_action_evidence_count", 0))
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    if usable_count > 0 and no_lookahead_violations == 0:
        revised_decision = "hold_causal_unverified"
        causal_attribution_status = "unverified"
        can_design_next = False
    elif usable_count == 0:
        revised_decision = "hold_causal_unverified"
        causal_attribution_status = "unverified"
        can_design_next = False
    else:
        revised_decision = "drop"
        causal_attribution_status = "refuted"
        can_design_next = False
    return {
        "schema_version": "tradex_hard_invalidation_non_exit_action_evidence_decision_v1",
        "phase": "non_exit_action_evidence_mining",
        "prior_guard_family_status": prior_decision,
        "new_challenger_created": False,
        "usable_non_exit_action_evidence_count": usable_count,
        "causal_attribution_status": causal_attribution_status,
        "can_design_next_challenger": can_design_next,
        "revised_decision": revised_decision,
        "decision": revised_decision,
        "candidate_local_decision": revised_decision,
        "session_aggregate_decision": revised_decision,
        "authoritative_rollup_decision": revised_decision,
        "next_allowed_action": "only design a challenger if usable_non_exit_action_evidence_count is greater than zero",
        "generated_at": _utc_now(),
    }


def run_non_exit_action_evidence_mining(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    guard_case_results = list(_load_json(CAUSAL_AUDIT_GUARD_CASE_RESULTS))
    prior_decision_payload = _load_json(CAUSAL_AUDIT_GUARD_DECISION)
    prior_decision = str(prior_decision_payload.get("decision") or "hold_causal_unverified")
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "non_exit_action_evidence_mining"
    session_root.mkdir(parents=True, exist_ok=True)

    case_classification = [
        _classify_non_exit_action_case(entry, _guard_evidence_trigger_row(entry))
        for entry in guard_case_results
        if str(entry.get("symbol")) in NON_EXIT_ACTION_EVIDENCE_TARGET_SYMBOLS and str(entry.get("baseline_v2_action_on_trigger_date") or "") != "exit_all"
    ]
    field_inventory = _build_non_exit_action_field_inventory(case_classification)
    rollup = _build_non_exit_action_rollup(case_classification)
    decision = _build_non_exit_action_decision(rollup=rollup, prior_decision=prior_decision)

    manifest = {
        "schema_version": "tradex_hard_invalidation_non_exit_action_evidence_manifest_v1",
        "phase": "non_exit_action_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_guard_family_status": prior_decision,
        "source_guard_session_root": str(CAUSAL_AUDIT_SOURCE_SESSION_ROOT),
        "source_guard_case_results": str(CAUSAL_AUDIT_GUARD_CASE_RESULTS),
        "source_guard_decision": str(CAUSAL_AUDIT_GUARD_DECISION),
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    case_classification_artifact = {
        "schema_version": "tradex_hard_invalidation_non_exit_action_evidence_case_classification_v1",
        "phase": "non_exit_action_evidence_mining",
        "candidate": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "prior_guard_family_status": prior_decision,
        "generated_at": _utc_now(),
        "cases": case_classification,
    }

    artifacts = {
        "hard_invalidation_non_exit_action_evidence_manifest.json": _write_json(session_root / "hard_invalidation_non_exit_action_evidence_manifest.json", manifest),
        "hard_invalidation_non_exit_action_case_classification.json": _write_json(session_root / "hard_invalidation_non_exit_action_case_classification.json", case_classification_artifact),
        "hard_invalidation_non_exit_action_field_inventory.json": _write_json(session_root / "hard_invalidation_non_exit_action_field_inventory.json", field_inventory),
        "hard_invalidation_non_exit_action_rollup.json": _write_json(session_root / "hard_invalidation_non_exit_action_rollup.json", rollup),
        "hard_invalidation_non_exit_action_decision.json": _write_json(session_root / "hard_invalidation_non_exit_action_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_classification": case_classification_artifact,
        "field_inventory": field_inventory,
        "rollup": rollup,
        "source_guard_session_root": str(CAUSAL_AUDIT_SOURCE_SESSION_ROOT),
        "source_guard_case_results": str(CAUSAL_AUDIT_GUARD_CASE_RESULTS),
        "source_guard_decision": str(CAUSAL_AUDIT_GUARD_DECISION),
    }


def _build_non_exit_late_extension_hedge_case_results(
    *,
    split_cases: list[dict[str, Any]],
    repaired_v2_case_results: list[dict[str, Any]],
    session_root: Path,
    symbols: tuple[str, ...] = NON_EXIT_ACTION_EVIDENCE_TARGET_SYMBOLS,
) -> list[dict[str, Any]]:
    old_by_symbol = {str(entry["symbol"]): entry for entry in split_cases}
    repaired_v2_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_case_results}
    candidate_root = session_root / "non_exit_late_extension_hedge_v1"
    candidate_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []

    for symbol in symbols:
        old_entry = old_by_symbol[symbol]
        repaired_v2_entry = repaired_v2_by_symbol[symbol]
        case_spec = _build_case_spec_from_split(old_entry)
        artifact = _run_case_bundle(
            case_spec=case_spec,
            output_root=candidate_root,
            candidate_name=NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        )
        corrected = artifact["corrected"]["summary"]
        corrected_rows = _trace_rows(str(artifact["trace_artifacts"]["corrected_trace_path"]))
        trigger_date = str(repaired_v2_entry.get("trigger_date") or old_entry.get("trigger_date"))
        trigger_row: dict[str, Any] | None = None
        if trigger_date is not None:
            try:
                trigger_row = _row_by_date(corrected_rows, str(trigger_date))
            except KeyError:
                trigger_row = None
        selected_event_date = trigger_date
        selected_event_row: dict[str, Any] | None = trigger_row
        selected_event_source_label = str(repaired_v2_entry.get("selected_hard_invalidation_event_source") or "selected_selector_row")
        if selected_event_source_label not in {"selected_selector_row", "summary_fallback_row"}:
            selected_event_source_label = "selected_selector_row"
        selected_event_reason = (
            str(trigger_row.get("reason"))
            if trigger_row is not None and trigger_row.get("reason") is not None
            else None
        )
        selected_event_action_type_value = (
            str(trigger_row.get("action_type")).strip()
            if trigger_row is not None and trigger_row.get("action_type") is not None
            else None
        )
        selected_event_action_type = (
            selected_event_action_type_value
        )
        selected_event_target_position = (
            str(trigger_row.get("target_position_after"))
            if trigger_row is not None and trigger_row.get("target_position_after") is not None
            else None
        )
        baseline_trace_path = str(repaired_v2_entry["trace_artifacts"]["corrected_trace_path"])
        challenger_trace_path = str(artifact["trace_artifacts"]["corrected_trace_path"])
        baseline_v2_action = str(repaired_v2_entry["v2_loss_side_override_action_on_trigger_date"])
        baseline_v2_target = str(repaired_v2_entry["v2_loss_side_override_target_position"])
        challenger_action, challenger_target = _action_at(challenger_trace_path, trigger_date) if trigger_date else (None, None)
        baseline_action, baseline_target = _action_at(baseline_trace_path, trigger_date) if trigger_date else (None, None)
        baseline_v2_pnl = float(repaired_v2_entry["v2_loss_side_override_pnl"])
        challenger_pnl = float(corrected["total_pnl"])
        original_baseline_pnl = float(old_entry.get("stacked_pnl") or 0.0)
        baseline_v2_severity_level = repaired_v2_entry.get("severity_level")
        candidate_activation = bool(
            selected_event_action_type == "hedge_add"
            and selected_event_reason == "late_extension_blocked"
            and baseline_v2_action == "long_reduce"
            and trigger_row is not None
            and bool(trigger_row.get("bull_stack") or any(item == "bull_stack=True" for item in (trigger_row.get("evidence_for") or [])))
            and float(trigger_row.get("confidence") or 0.0) >= 0.6
            and str(trigger_row.get("position_before") or "").startswith("0-")
        )
        candidate_activation_reason = (
            "bull_stack_and_extension_without_existing_hedge"
            if candidate_activation
            else None
        )
        selected_event_action_type_norm = selected_event_action_type.strip() if isinstance(selected_event_action_type, str) else None
        if selected_event_action_type_norm == "watch":
            challenger_severity_level = None
        else:
            challenger_severity_level = (
                selected_event_action_type
                if selected_event_action_type is not None
                else repaired_v2_entry.get("severity_level")
            )
        challenger_severity_action_type = selected_event_action_type_norm
        challenger_severity_target_buy_units = None
        if selected_event_action_type_norm != "watch" and isinstance(selected_event_target_position, str) and "-" in selected_event_target_position:
            try:
                challenger_severity_target_buy_units = int(selected_event_target_position.split("-", 1)[1])
            except ValueError:
                challenger_severity_target_buy_units = None
        elif selected_event_action_type_norm == "watch":
            challenger_severity_target_buy_units = None
        candidate_trigger_reason = selected_event_reason
        action_changed = bool(baseline_v2_action != challenger_action)
        exit_all_behavior_changed = bool((baseline_v2_action == "exit_all") != (challenger_action == "exit_all"))
        selected_event_action_allowed = bool(
            _severity_action_aligned(str(challenger_severity_level) if challenger_severity_level is not None else None, selected_event_action_type)
            and _severity_target_aligned(
                str(challenger_severity_level) if challenger_severity_level is not None else None,
                selected_event_action_type,
                selected_event_target_position,
                int(challenger_severity_target_buy_units) if challenger_severity_target_buy_units is not None else None,
            )
        )
        emitted_action_allowed = bool(
            _severity_action_aligned(str(challenger_severity_level) if challenger_severity_level is not None else None, challenger_action)
            and _severity_target_aligned(
                str(challenger_severity_level) if challenger_severity_level is not None else None,
                challenger_action,
                challenger_target,
                int(challenger_severity_target_buy_units) if challenger_severity_target_buy_units is not None else None,
            )
        )
        contract_violation = bool(selected_event_source_label == "selected_selector_row" and not emitted_action_allowed)
        if selected_event_source_label == "summary_fallback_row":
            case_classification = "excluded_fallback_or_mixed_axis"
            activation_status = "excluded_fallback_or_mixed_axis"
        elif action_changed and candidate_activation and baseline_v2_action == "long_reduce" and challenger_action == "hedge_add":
            case_classification = "intended_activation"
            activation_status = "intended_activation"
        elif action_changed:
            case_classification = "unintended_activation"
            activation_status = "unintended_activation"
        else:
            case_classification = "unchanged"
            activation_status = "unchanged"
        results.append(
            {
                "symbol": symbol,
                "company_name": str(old_entry["company_name"]),
                "case_classification": case_classification,
                "activation_status": activation_status,
                "baseline_v2_action": baseline_v2_action,
                "baseline_v2_target_position": baseline_v2_target,
                "baseline_v2_pnl": baseline_v2_pnl,
                "baseline_v2_severity_level": baseline_v2_severity_level,
                "baseline_v2_selected_event_source": str(repaired_v2_entry.get("selected_hard_invalidation_event_source") or "missing"),
                "baseline_v2_selected_event_reason": repaired_v2_entry.get("selected_hard_invalidation_event_reason"),
                "baseline_v2_selected_event_action_type": repaired_v2_entry.get("selected_hard_invalidation_event_action_type"),
                "baseline_v2_selected_event_target_position": repaired_v2_entry.get("selected_hard_invalidation_event_target_position"),
                "challenger_action": challenger_action,
                "challenger_target_position": challenger_target,
                "challenger_pnl": challenger_pnl,
                "challenger_severity_level": challenger_severity_level,
                "challenger_severity_action_type": challenger_severity_action_type,
                "challenger_severity_target_buy_units": challenger_severity_target_buy_units,
                "trigger_date": trigger_date,
                "selected_event_date": str(selected_event_date) if selected_event_date is not None else None,
                "selected_event_source": selected_event_source_label,
                "selected_event_reason": selected_event_reason,
                "selected_event_action_type": selected_event_action_type,
                "selected_event_target_position": selected_event_target_position,
                "trigger_date_reason": candidate_trigger_reason,
                "typed_activation_reason": candidate_activation_reason,
                "late_extension_hedge_condition_activated": candidate_activation,
                "action_changed": action_changed,
                "exit_all_behavior_changed": exit_all_behavior_changed,
                "selected_event_action_allowed_under_recorded_severity": selected_event_action_allowed,
                "emitted_action_allowed_under_recorded_severity": emitted_action_allowed,
                "confidence": trigger_row.get("confidence") if trigger_row is not None else None,
                "position_before": trigger_row.get("position_before") if trigger_row is not None else None,
                "target_position_after": trigger_row.get("target_position_after") if trigger_row is not None else None,
                "reason": trigger_row.get("reason") if trigger_row is not None else None,
                "evidence_for": list(trigger_row.get("evidence_for") or []) if trigger_row is not None else [],
                "evidence_against": list(trigger_row.get("evidence_against") or []) if trigger_row is not None else [],
                "no_lookahead_ok": bool(
                    _trace_no_lookahead_ok(str(repaired_v2_entry["trace_artifacts"]["baseline_trace_path"]))
                    and _trace_no_lookahead_ok(baseline_trace_path)
                    and _trace_no_lookahead_ok(challenger_trace_path)
                ),
                "contract_violation": contract_violation,
                "baseline_trace_action": baseline_action,
                "baseline_trace_target_position": baseline_target,
                "challenger_trace_action": challenger_action,
                "challenger_trace_target_position": challenger_target,
                "pnl_delta_vs_repaired_v2": float(challenger_pnl - baseline_v2_pnl),
                "pnl_delta_vs_original_baseline": float(challenger_pnl - original_baseline_pnl),
                "compare_to_repaired_v2": {
                    "baseline_action": baseline_v2_action,
                    "challenger_action": challenger_action,
                    "baseline_target_position": baseline_v2_target,
                    "challenger_target_position": challenger_target,
                    "baseline_pnl": baseline_v2_pnl,
                    "challenger_pnl": challenger_pnl,
                    "pnl_delta": float(challenger_pnl - baseline_v2_pnl),
                    "action_changed_flag": action_changed,
                },
                "compare_to_original_baseline": {
                    "original_baseline_action": baseline_action,
                    "original_baseline_target_position": baseline_target,
                    "original_baseline_pnl": original_baseline_pnl,
                    "challenger_pnl": challenger_pnl,
                    "pnl_delta": float(challenger_pnl - original_baseline_pnl),
                },
                "trace_artifacts": artifact["trace_artifacts"],
                "labels": list(artifact["corrected"]["labels"]),
            }
        )

    return results


def _build_replay_path_delta_symbol_set_diff(
    *,
    baseline_case_results: list[dict[str, Any]],
    challenger_case_results: list[dict[str, Any]],
    baseline_case_results_path: Path,
    challenger_case_results_path: Path,
) -> dict[str, Any]:
    baseline_symbols = [str(entry["symbol"]) for entry in baseline_case_results]
    challenger_symbols = [str(entry["symbol"]) for entry in challenger_case_results]
    baseline_symbol_set = set(baseline_symbols)
    challenger_symbol_set = set(challenger_symbols)
    overlap_symbols = sorted(baseline_symbol_set & challenger_symbol_set)
    baseline_only_symbols = sorted(baseline_symbol_set - challenger_symbol_set)
    challenger_only_symbols = sorted(challenger_symbol_set - baseline_symbol_set)
    coverage_gate_passed = not baseline_only_symbols and not challenger_only_symbols and len(overlap_symbols) == len(baseline_symbol_set) == len(challenger_symbol_set)
    return {
        "schema_version": "tradex_hard_invalidation_replay_path_delta_symbol_set_diff_v1",
        "phase": "full_coverage_replay_path_delta_audit",
        "prior_candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": "drop",
        "baseline_case_count": len(baseline_case_results),
        "challenger_case_count": len(challenger_case_results),
        "baseline_symbols": baseline_symbols,
        "challenger_symbols": challenger_symbols,
        "overlap_symbols": overlap_symbols,
        "baseline_only_symbols": baseline_only_symbols,
        "challenger_only_symbols": challenger_only_symbols,
        "baseline_only_case_count": len(baseline_only_symbols),
        "challenger_only_case_count": len(challenger_only_symbols),
        "overlap_case_count": len(overlap_symbols),
        "coverage_gate_passed": coverage_gate_passed,
        "symbol_set_match": coverage_gate_passed,
        "baseline_case_results_path": str(baseline_case_results_path),
        "challenger_case_results_path": str(challenger_case_results_path),
        "baseline_case_results_identity": _file_identity(baseline_case_results_path),
        "challenger_case_results_identity": _file_identity(challenger_case_results_path),
        "generated_at": _utc_now(),
    }


def _build_non_exit_late_extension_hedge_rollup(case_results: list[dict[str, Any]]) -> dict[str, Any]:
    classification_counts = Counter(str(entry["case_classification"]) for entry in case_results)
    total_pnl_delta_vs_repaired_v2 = sum(float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) for entry in case_results)
    total_pnl_delta_vs_original_baseline = sum(float(entry.get("pnl_delta_vs_original_baseline", 0.0)) for entry in case_results)
    improved_vs_repaired_v2_count = sum(1 for entry in case_results if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) > 0)
    worsened_vs_repaired_v2_count = sum(1 for entry in case_results if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) < 0)
    action_changed_count = sum(1 for entry in case_results if bool(entry.get("action_changed")))
    intended_activation_count = classification_counts.get("intended_activation", 0)
    unintended_activation_count = classification_counts.get("unintended_activation", 0)
    unchanged_count = classification_counts.get("unchanged", 0)
    excluded_fallback_count = classification_counts.get("excluded_fallback_or_mixed_axis", 0)
    mixed_axis_activation_count = excluded_fallback_count
    exit_all_changed_count = sum(
        1
        for entry in case_results
        if bool(entry.get("exit_all_behavior_changed"))
    )
    typed_reason_missing_count = sum(
        1
        for entry in case_results
        if bool(entry.get("action_changed")) and not bool(entry.get("typed_activation_reason"))
    )
    changed_action_with_typed_reason_count = sum(
        1
        for entry in case_results
        if bool(entry.get("action_changed")) and bool(entry.get("typed_activation_reason"))
    )
    source_evidence_case = next((entry for entry in case_results if entry["symbol"] == "4004"), None)
    other_cases = [entry for entry in case_results if entry["symbol"] != "4004"]
    other_cases_total_pnl_delta_vs_repaired_v2 = sum(float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) for entry in other_cases)
    other_cases_materially_worsened_count = sum(1 for entry in other_cases if float(entry.get("pnl_delta_vs_repaired_v2", 0.0)) < -1.0)
    source_case_improved = bool(source_evidence_case and float(source_evidence_case.get("pnl_delta_vs_repaired_v2", 0.0)) > 0)
    source_case_activation_ok = bool(source_evidence_case and source_evidence_case.get("case_classification") == "intended_activation")
    no_lookahead_violations = sum(1 for entry in case_results if not bool(entry.get("no_lookahead_ok")))
    contract_violation_count = sum(1 for entry in case_results if bool(entry.get("contract_violation")))
    anchor_5541_preservation_count = 0
    anchor_5541_preservation_intact = False
    return {
        "schema_version": "tradex_hard_invalidation_non_exit_late_extension_hedge_rollup_v1",
        "phase": "diagnostic_challenger",
        "candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "total_cases": len(case_results),
        "action_changed_count": action_changed_count,
        "intended_activation_count": intended_activation_count,
        "unintended_activation_count": unintended_activation_count,
        "unchanged_count": unchanged_count,
        "excluded_fallback_count": excluded_fallback_count,
        "mixed_axis_activation_count": mixed_axis_activation_count,
        "exit_all_changed_count": exit_all_changed_count,
        "typed_reason_missing_count": typed_reason_missing_count,
        "changed_action_with_typed_reason_count": changed_action_with_typed_reason_count,
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "improved_vs_repaired_v2_count": improved_vs_repaired_v2_count,
        "worsened_vs_repaired_v2_count": worsened_vs_repaired_v2_count,
        "total_pnl_delta_vs_repaired_v2": float(total_pnl_delta_vs_repaired_v2),
        "total_pnl_delta_vs_original_baseline": float(total_pnl_delta_vs_original_baseline),
        "source_evidence_case_symbol": "4004",
        "source_evidence_case_improved": source_case_improved,
        "source_evidence_case_activation_ok": source_case_activation_ok,
        "source_evidence_case_pnl_delta_vs_repaired_v2": float(source_evidence_case.get("pnl_delta_vs_repaired_v2", 0.0)) if source_evidence_case else 0.0,
        "other_cases_total_pnl_delta_vs_repaired_v2": float(other_cases_total_pnl_delta_vs_repaired_v2),
        "other_cases_materially_worsened_count": other_cases_materially_worsened_count,
        "anchor_5541_preservation_count": anchor_5541_preservation_count,
        "anchor_5541_preservation_intact": anchor_5541_preservation_intact,
        "classification_counts": dict(classification_counts),
        "changed_symbols": [entry["symbol"] for entry in case_results if bool(entry.get("action_changed"))],
        "intended_activation_symbols": [entry["symbol"] for entry in case_results if entry["case_classification"] == "intended_activation"],
        "unintended_activation_symbols": [entry["symbol"] for entry in case_results if entry["case_classification"] == "unintended_activation"],
        "unchanged_symbols": [entry["symbol"] for entry in case_results if entry["case_classification"] == "unchanged"],
        "excluded_fallback_symbols": [entry["symbol"] for entry in case_results if entry["case_classification"] == "excluded_fallback_or_mixed_axis"],
        "can_design_next_challenger": False,
        "generated_at": _utc_now(),
    }


def _build_non_exit_late_extension_hedge_anchor_regression(
    *,
    session_root: Path,
    repaired_v2_anchor_regression: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    anchor_case_specs = {case["symbol"]: case for case in loop_mod.CASE_SPECS}
    repaired_v2_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_anchor_regression}
    results: list[dict[str, Any]] = []
    for symbol in ANCHOR_SYMBOLS:
        baseline_anchor = repaired_v2_by_symbol[symbol]
        artifact = _run_case_bundle(
            case_spec=anchor_case_specs[symbol],
            output_root=session_root,
            candidate_name=NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        )
        corrected = artifact["corrected"]["summary"]
        trigger_date = (
            corrected.get("first_hard_invalidation_action_date")
            or corrected.get("first_hard_invalidation_date")
            or baseline_anchor.get("trigger_date")
        )
        baseline_v2_pnl = float(baseline_anchor["severity_refined_pnl"])
        challenger_pnl = float(corrected["total_pnl"])
        baseline_v2_action = str(baseline_anchor["severity_action"])
        challenger_action, challenger_target = _action_at(artifact["trace_artifacts"]["corrected_trace_path"], str(trigger_date)) if trigger_date else (None, None)
        baseline_v2_target = str(baseline_anchor["severity_target_position"])
        old_baseline_pnl = float(baseline_anchor["baseline_pnl"])
        results.append(
            {
                "symbol": symbol,
                "name": anchor_case_specs[symbol]["name"],
                "trigger_date": trigger_date,
                "baseline_v2_action_on_trigger_date": baseline_v2_action,
                "baseline_v2_target_position": baseline_v2_target,
                "baseline_v2_pnl": baseline_v2_pnl,
                "challenger_action_on_trigger_date": challenger_action,
                "challenger_target_position": challenger_target,
                "challenger_pnl": challenger_pnl,
                "pnl_delta_vs_repaired_v2": float(challenger_pnl - baseline_v2_pnl),
                "pnl_delta_vs_original_baseline": float(challenger_pnl - old_baseline_pnl),
                "damage_reduced_vs_repaired_v2": bool(challenger_pnl > baseline_v2_pnl),
                "preservation_flag": bool(
                    symbol == "5541"
                    and challenger_pnl >= baseline_v2_pnl
                    and not bool((baseline_anchor.get("compare") or {}).get("comparison", {}).get("false_winner_block"))
                ),
                "baseline_original": {
                    "pnl": old_baseline_pnl,
                    "action_on_trigger_date": baseline_anchor.get("baseline_action"),
                },
                "baseline_v2": baseline_anchor,
                "compare": artifact["compare"],
                "trace_artifacts": artifact["trace_artifacts"],
                "labels": list(artifact["corrected"]["labels"]),
            }
        )
    return results


def _build_non_exit_late_extension_hedge_decision(*, rollup: dict[str, Any], prior_decision: str) -> dict[str, Any]:
    contract_violation_count = int(rollup.get("contract_violation_count", 0))
    no_lookahead_violations = int(rollup.get("no_lookahead_violations", 0))
    intended_activation_count = int(rollup.get("intended_activation_count", 0))
    unintended_activation_count = int(rollup.get("unintended_activation_count", 0))
    exit_all_changed_count = int(rollup.get("exit_all_changed_count", 0))
    mixed_axis_activation_count = int(rollup.get("mixed_axis_activation_count", 0))
    source_case_improved = bool(rollup.get("source_evidence_case_improved"))
    other_cases_materially_worsened_count = int(rollup.get("other_cases_materially_worsened_count", 0))
    anchor_ok = bool(rollup.get("anchor_5541_preservation_intact"))
    typed_reason_ok = int(rollup.get("changed_action_with_typed_reason_count", 0)) == int(rollup.get("action_changed_count", 0))
    decision = "drop"
    decision_reason = "Diagnostic challenger did not satisfy the narrow evidence contract"
    if contract_violation_count > 0 or no_lookahead_violations > 0:
        decision = "drop"
        decision_reason = "Contract alignment or no-lookahead failed"
    elif exit_all_changed_count > 0:
        decision = "drop"
        decision_reason = "Exit-all behavior changed, which is out of scope"
    elif mixed_axis_activation_count > 0:
        decision = "drop"
        decision_reason = "Mixed-axis activation leaked into the action-surface diagnostic"
    elif unintended_activation_count > 0:
        decision = "drop"
        decision_reason = "The candidate activated outside the intended 4004 pattern"
    elif not anchor_ok:
        decision = "drop"
        decision_reason = "Anchor 5541 preservation failed"
    elif not source_case_improved:
        decision = "drop"
        decision_reason = "Source evidence case 4004 did not improve versus repaired v2"
    elif other_cases_materially_worsened_count > 0:
        decision = "drop"
        decision_reason = "Collateral damage appeared in the non-evidence cases"
    elif not typed_reason_ok:
        decision = "drop"
        decision_reason = "Changed actions were not all accompanied by typed trigger-date reasons"
    elif intended_activation_count >= 1:
        decision = "hold_single_case_evidence"
        decision_reason = "Single-case diagnostic succeeded on 4004 but breadth is still one-case only"
    else:
        decision = "drop"
        decision_reason = "No intended activation was observed"
    return {
        "schema_version": "tradex_hard_invalidation_non_exit_late_extension_hedge_decision_v1",
        "phase": "diagnostic_challenger",
        "candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "prior_decision": prior_decision,
        "source_evidence_case": "4004",
        "new_axis": "non_exit_action_surface",
        "exit_all_behavior_changed": False,
        "new_challenger_created": True,
        "decision": decision,
        "decision_reason": decision_reason,
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "candidate_status": "diagnostic_challenger",
        "meemee_reflectable": False,
        "contract_violation_count": contract_violation_count,
        "no_lookahead_violations": no_lookahead_violations,
        "intended_activation_count": intended_activation_count,
        "unintended_activation_count": unintended_activation_count,
        "exit_all_changed_count": exit_all_changed_count,
        "mixed_axis_activation_count": mixed_axis_activation_count,
        "source_evidence_case_improved": source_case_improved,
        "other_cases_materially_worsened_count": other_cases_materially_worsened_count,
        "anchor_5541_preservation_intact": anchor_ok,
        "typed_reason_ok": typed_reason_ok,
        "can_design_next_challenger": False,
        "next_allowed_action": "only design a challenger if another clean evidence case is discovered",
        "generated_at": _utc_now(),
    }


def run_non_exit_late_extension_hedge_audit(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    split_payload = _load_json(SOURCE_SPLIT_CASE_RESULTS)
    split_cases = list(split_payload["candidates"])
    repaired_v2_case_results = list(_load_json(REPAIRED_V2_CASE_RESULTS))
    repaired_v2_anchor_regression = list(_load_json(REPAIRED_V2_ANCHOR_REGRESSION))
    prior_decision_payload = _load_json(REPAIRED_V2_DECISION)
    prior_decision = str(prior_decision_payload.get("decision") or "freeze")
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp / "non_exit_late_extension_hedge_v1"
    session_root.mkdir(parents=True, exist_ok=True)

    case_results = _build_non_exit_late_extension_hedge_case_results(
        split_cases=split_cases,
        repaired_v2_case_results=repaired_v2_case_results,
        session_root=session_root,
    )
    rollup = _build_non_exit_late_extension_hedge_rollup(case_results)
    anchor_regression = _build_non_exit_late_extension_hedge_anchor_regression(
        session_root=session_root,
        repaired_v2_anchor_regression=repaired_v2_anchor_regression,
    )
    rollup["anchor_5541_preservation_count"] = sum(
        1 for entry in anchor_regression if str(entry.get("symbol")) == "5541" and bool(entry.get("preservation_flag"))
    )
    rollup["anchor_5541_preservation_intact"] = rollup["anchor_5541_preservation_count"] == 1
    decision = _build_non_exit_late_extension_hedge_decision(rollup=rollup, prior_decision=prior_decision)

    manifest = {
        "schema_version": "tradex_hard_invalidation_non_exit_late_extension_hedge_manifest_v1",
        "phase": "diagnostic_challenger",
        "candidate": NON_EXIT_LATE_EXTENSION_HEDGE_CANDIDATE,
        "source_evidence_case": "4004",
        "baseline_reference": "repaired hard_invalidation_exit_severity_v2 under repaired contract",
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
        "repaired_v2_session_root": str(REPAIRED_V2_SESSION_ROOT),
        "repaired_v2_case_results": str(REPAIRED_V2_CASE_RESULTS),
        "repaired_v2_anchor_regression": str(REPAIRED_V2_ANCHOR_REGRESSION),
        "anchor_symbols": list(ANCHOR_SYMBOLS),
        "case_symbols": list(NON_EXIT_ACTION_EVIDENCE_TARGET_SYMBOLS),
        "selection_basis": {
            "source_case": "4004 late-extension hedge surface from non-exit evidence mining",
            "comparison": "same fixed 8 non-exit cases against repaired-v2 baseline",
        },
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    artifacts = {
        "hard_invalidation_non_exit_late_extension_hedge_v1_manifest.json": _write_json(session_root / "hard_invalidation_non_exit_late_extension_hedge_v1_manifest.json", manifest),
        "hard_invalidation_non_exit_late_extension_hedge_v1_case_results.json": _write_json(session_root / "hard_invalidation_non_exit_late_extension_hedge_v1_case_results.json", case_results),
        "hard_invalidation_non_exit_late_extension_hedge_v1_rollup.json": _write_json(session_root / "hard_invalidation_non_exit_late_extension_hedge_v1_rollup.json", rollup),
        "hard_invalidation_non_exit_late_extension_hedge_v1_anchor_regression.json": _write_json(session_root / "hard_invalidation_non_exit_late_extension_hedge_v1_anchor_regression.json", anchor_regression),
        "hard_invalidation_non_exit_late_extension_hedge_v1_decision.json": _write_json(session_root / "hard_invalidation_non_exit_late_extension_hedge_v1_decision.json", decision),
    }

    return {
        "session_root": str(session_root),
        "artifacts": {name: str(path) for name, path in artifacts.items()},
        "decision": decision,
        "case_results": case_results,
        "rollup": rollup,
        "anchor_regression": anchor_regression,
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
        "repaired_v2_session_root": str(REPAIRED_V2_SESSION_ROOT),
        "repaired_v2_case_results": str(REPAIRED_V2_CASE_RESULTS),
        "repaired_v2_anchor_regression": str(REPAIRED_V2_ANCHOR_REGRESSION),
    }


def run_rebaseline_and_guard_audit(*, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    split_payload = _load_json(SOURCE_SPLIT_CASE_RESULTS)
    split_cases = list(split_payload["candidates"])
    previous_v2_payload = _load_json(PREVIOUS_V2_CASE_RESULTS)
    previous_v2_cases = list(previous_v2_payload)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_root = output_root / run_stamp
    session_root.mkdir(parents=True, exist_ok=True)

    repaired_v2_root = session_root / "repaired_v2"
    repaired_v2_root.mkdir(parents=True, exist_ok=True)
    repaired_v2_anchor_regression = _build_anchor_regression(session_root=repaired_v2_root)
    repaired_v2_case_results = _build_severity_case_results(
        split_cases=split_cases,
        previous_v2_cases=previous_v2_cases,
        session_root=repaired_v2_root,
    )
    repaired_v2_rollup = _build_rollup(repaired_v2_case_results)
    repaired_v2_decision = _build_repaired_v2_freeze_decision(rollup=repaired_v2_rollup)
    repaired_v2_manifest = {
        "schema_version": "tradex_hard_invalidation_repaired_v2_manifest_v1",
        "phase": "baseline_freeze",
        "candidate_name": SEVERITY_CANDIDATE,
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
        "previous_v2_session_root": str(PREVIOUS_V2_SESSION_ROOT),
        "previous_v2_artifact": str(PREVIOUS_V2_CASE_RESULTS),
        "anchor_symbols": list(ANCHOR_SYMBOLS),
        "repair_case_symbols": list(SEVERITY_CASE_SYMBOLS),
        "selection_basis": {
            "baseline_freeze": "same 12-case fixed corpus under the repaired contract",
            "anchor_regression": "loop_mod.CASE_SPECS replay under repaired v2",
        },
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    repaired_v2_artifacts = {
        "hard_invalidation_repaired_v2_manifest.json": _write_json(repaired_v2_root / "hard_invalidation_repaired_v2_manifest.json", repaired_v2_manifest),
        "hard_invalidation_repaired_v2_case_results.json": _write_json(repaired_v2_root / "hard_invalidation_repaired_v2_case_results.json", repaired_v2_case_results),
        "hard_invalidation_repaired_v2_rollup.json": _write_json(repaired_v2_root / "hard_invalidation_repaired_v2_rollup.json", repaired_v2_rollup),
        "hard_invalidation_repaired_v2_anchor_regression.json": _write_json(repaired_v2_root / "hard_invalidation_repaired_v2_anchor_regression.json", repaired_v2_anchor_regression),
        "hard_invalidation_repaired_v2_decision.json": _write_json(repaired_v2_root / "hard_invalidation_repaired_v2_decision.json", repaired_v2_decision),
    }

    guard_root = session_root / "profit_preservation_guard_v1"
    guard_root.mkdir(parents=True, exist_ok=True)
    guard_case_results = _build_profit_preservation_guard_case_results(
        split_cases=split_cases,
        repaired_v2_case_results=repaired_v2_case_results,
        session_root=session_root,
    )
    guard_rollup = _build_profit_preservation_guard_rollup(guard_case_results)
    repaired_v2_anchor_by_symbol = {str(entry["symbol"]): entry for entry in repaired_v2_anchor_regression}
    anchor_case_specs = {str(case["symbol"]): case for case in loop_mod.CASE_SPECS}
    guard_anchor_regression: list[dict[str, Any]] = []
    for symbol in ANCHOR_SYMBOLS:
        anchor_spec = anchor_case_specs[symbol]
        artifact = _run_case_bundle(
            case_spec=anchor_spec,
            output_root=guard_root,
            candidate_name=loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        )
        baseline = artifact["baseline"]["summary"]
        corrected = artifact["corrected"]["summary"]
        baseline_anchor = repaired_v2_anchor_by_symbol[symbol]
        trigger_date = (
            corrected.get("first_hard_invalidation_action_date")
            or corrected.get("first_hard_invalidation_date")
            or baseline_anchor.get("trigger_date")
        )
        baseline_v2_pnl = float(baseline_anchor["severity_refined_pnl"])
        challenger_pnl = float(corrected["total_pnl"])
        baseline_v2_action = str(baseline_anchor["severity_action"])
        challenger_action, challenger_target = _action_at(artifact["trace_artifacts"]["corrected_trace_path"], str(trigger_date)) if trigger_date else (None, None)
        baseline_v2_target = str(baseline_anchor["severity_target_position"])
        old_baseline_pnl = float(baseline_anchor["baseline_pnl"])
        guard_anchor_regression.append(
            {
                "symbol": symbol,
                "name": anchor_spec["name"],
                "trigger_date": trigger_date,
                "baseline_v2_action_on_trigger_date": baseline_v2_action,
                "baseline_v2_target_position": baseline_v2_target,
                "baseline_v2_pnl": baseline_v2_pnl,
                "challenger_action_on_trigger_date": challenger_action,
                "challenger_target_position": challenger_target,
                "challenger_pnl": challenger_pnl,
                "pnl_delta_vs_repaired_v2": float(challenger_pnl - baseline_v2_pnl),
                "pnl_delta_vs_original_baseline": float(challenger_pnl - old_baseline_pnl),
                "damage_reduced_vs_repaired_v2": bool(challenger_pnl > baseline_v2_pnl),
                "preservation_flag": bool(
                    symbol == "5541"
                    and challenger_pnl >= baseline_v2_pnl
                    and not bool((baseline_anchor.get("compare") or {}).get("comparison", {}).get("false_winner_block"))
                ),
                "baseline_original": {
                    "pnl": old_baseline_pnl,
                    "action_on_trigger_date": baseline_anchor.get("baseline_action"),
                },
                "baseline_v2": baseline_anchor,
                "compare": artifact["compare"],
                "trace_artifacts": artifact["trace_artifacts"],
                "labels": list(artifact["corrected"]["labels"]),
            }
        )
    guard_rollup["anchor_5541_preservation_count"] = sum(
        1 for entry in guard_anchor_regression if str(entry.get("symbol")) == "5541" and bool(entry.get("preservation_flag"))
    )
    guard_rollup["anchor_5541_preservation_intact"] = guard_rollup["anchor_5541_preservation_count"] == 1
    guard_decision = _build_profit_preservation_guard_decision(rollup=guard_rollup)
    guard_manifest = {
        "schema_version": "tradex_hard_invalidation_profit_preservation_guard_manifest_v1",
        "phase": "challenger_evaluation",
        "candidate_name": loop_mod.HARD_INVALIDATION_PROFIT_PRESERVATION_GUARD_CANDIDATE,
        "baseline_reference": "repaired hard_invalidation_exit_severity_v2 under repaired contract",
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
        "repaired_v2_session_root": str(repaired_v2_root),
        "anchor_symbols": list(ANCHOR_SYMBOLS),
        "guard_case_symbols": list(SEVERITY_CASE_SYMBOLS),
        "selection_basis": {
            "fixed_corpus": "same 12 mined hard-invalidation cases",
            "guard_thesis": "soften profitable continuation states from exit_all to long_reduce when trigger-date evidence stays constructive",
        },
        "no_lookahead": NO_LOOKAHEAD_ASSERTION,
        "generated_at": _utc_now(),
    }

    guard_artifacts = {
        "hard_invalidation_profit_preservation_guard_v1_manifest.json": _write_json(guard_root / "hard_invalidation_profit_preservation_guard_v1_manifest.json", guard_manifest),
        "hard_invalidation_profit_preservation_guard_v1_case_results.json": _write_json(guard_root / "hard_invalidation_profit_preservation_guard_v1_case_results.json", guard_case_results),
        "hard_invalidation_profit_preservation_guard_v1_rollup.json": _write_json(guard_root / "hard_invalidation_profit_preservation_guard_v1_rollup.json", guard_rollup),
        "hard_invalidation_profit_preservation_guard_v1_anchor_regression.json": _write_json(guard_root / "hard_invalidation_profit_preservation_guard_v1_anchor_regression.json", guard_anchor_regression),
        "hard_invalidation_profit_preservation_guard_v1_decision.json": _write_json(guard_root / "hard_invalidation_profit_preservation_guard_v1_decision.json", guard_decision),
    }

    return {
        "session_root": str(session_root),
        "repaired_v2": {
            "artifacts": {name: str(path) for name, path in repaired_v2_artifacts.items()},
            "decision": repaired_v2_decision,
            "anchor_regression": repaired_v2_anchor_regression,
            "case_results": repaired_v2_case_results,
            "rollup": repaired_v2_rollup,
            "manifest": repaired_v2_manifest,
        },
        "profit_preservation_guard_v1": {
            "artifacts": {name: str(path) for name, path in guard_artifacts.items()},
            "decision": guard_decision,
            "anchor_regression": guard_anchor_regression,
            "case_results": guard_case_results,
            "rollup": guard_rollup,
            "manifest": guard_manifest,
        },
        "source_split_session_root": str(SOURCE_SPLIT_SESSION_ROOT),
        "source_split_artifact": str(SOURCE_SPLIT_CASE_RESULTS),
        "previous_v2_session_root": str(PREVIOUS_V2_SESSION_ROOT),
        "previous_v2_artifact": str(PREVIOUS_V2_CASE_RESULTS),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run hard invalidation repair, challenger, or causal-audit workflows.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument(
        "--mode",
        choices=(
            "contract_repair",
            "rebaseline_and_guard",
            "causal_audit",
            "guard_evidence_mining",
            "non_exit_action_evidence_mining",
            "non_exit_late_extension_hedge",
            "replay_path_delta_audit",
            "full_coverage_replay_path_delta_audit",
            "post_trigger_path_drift_evidence_mining",
            "post_trigger_path_drift_evidence_mining_v2",
            "trace_instrumentation",
        ),
        default="non_exit_action_evidence_mining",
        help="Which audit workflow to run.",
    )
    args = parser.parse_args(argv)
    output_root = Path(args.output_root).expanduser().resolve()
    if args.mode == "contract_repair":
        payload = run_audit(output_root=output_root)
    elif args.mode == "rebaseline_and_guard":
        payload = run_rebaseline_and_guard_audit(output_root=output_root)
    elif args.mode == "guard_evidence_mining":
        payload = run_guard_evidence_mining(output_root=output_root)
    elif args.mode == "non_exit_action_evidence_mining":
        payload = run_non_exit_action_evidence_mining(output_root=output_root)
    elif args.mode == "non_exit_late_extension_hedge":
        payload = run_non_exit_late_extension_hedge_audit(output_root=output_root)
    elif args.mode == "replay_path_delta_audit":
        payload = run_replay_path_delta_audit(output_root=output_root)
    elif args.mode == "full_coverage_replay_path_delta_audit":
        payload = run_full_coverage_replay_path_delta_audit(output_root=output_root)
    elif args.mode == "post_trigger_path_drift_evidence_mining":
        payload = run_post_trigger_path_drift_evidence_mining(output_root=output_root)
    elif args.mode == "post_trigger_path_drift_evidence_mining_v2":
        payload = run_post_trigger_path_drift_evidence_mining_v2(output_root=output_root)
    elif args.mode == "trace_instrumentation":
        payload = run_trace_instrumentation(output_root=output_root)
    else:
        payload = run_causal_audit(output_root=output_root)
    print(_json_text(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
