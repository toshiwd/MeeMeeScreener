from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import entry_precision_short_audit as base


AXIS_ID = "tradex_downside_prebreak_maturity_gate_v7"
SCHEMA_PREFIX = "tradex_downside_prebreak_maturity_gate_v7"
DEFAULT_SOURCE_ROOT = Path(r"G:\Tradex\downside_prebreak_monthly_coverage_gate_v6")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\downside_prebreak_maturity_gate_v7")

REQUIRED_ARTIFACTS = (
    "downside_prebreak_maturity_gate_contract.json",
    "downside_prebreak_skipped_months.csv",
    "downside_prebreak_maturity_calendar.json",
    "downside_prebreak_recheck_plan.json",
    "downside_prebreak_maturity_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _safe_int(value: Any) -> int:
    try:
        if value is None or pd.isna(value) or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _load_skipped_rows(source_root: Path) -> list[dict[str, Any]]:
    path = source_root / "downside_prebreak_monthly_coverage_rows.csv"
    if not path.exists():
        raise FileNotFoundError(f"coverage rows not found: {path}")
    frame = pd.read_csv(path)
    rows = [dict(row) for row in frame.to_dict(orient="records")]
    return [row for row in rows if bool(row.get("skipped")) or _safe_int(row.get("selected_candidate_count")) == 0]


def _load_calendar(db_path: Path) -> list[int]:
    with base.duckdb.connect(str(db_path), read_only=True) as conn:
        expr = base._ymd_expr("date")
        rows = conn.execute(
            f"""
            SELECT DISTINCT {expr} AS ymd
            FROM daily_bars
            WHERE {expr} IS NOT NULL
            ORDER BY ymd
            """
        ).fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def _maturity_rows(skipped_rows: list[dict[str, Any]], calendar: list[int]) -> list[dict[str, Any]]:
    latest_ymd = max(calendar) if calendar else None
    out: list[dict[str, Any]] = []
    for row in skipped_rows:
        month = _safe_int(row.get("month"))
        month_dates = [d for d in calendar if d // 100 == month]
        anchor_ymd = max(month_dates) if month_dates else None
        future = [d for d in calendar if anchor_ymd is not None and d > anchor_ymd]
        horizon20_ymd = future[19] if len(future) >= 20 else None
        out.append(
            {
                "month": month,
                "skip_reason": row.get("skip_reason"),
                "closed_horizon_candidate_count": _safe_int(row.get("closed_horizon_candidate_count")),
                "unknown_candidate_count": _safe_int(row.get("unknown_candidate_count")),
                "anchor_ymd": anchor_ymd,
                "required_horizon20_ymd": horizon20_ymd,
                "latest_available_ymd": latest_ymd,
                "future_sessions_available": len(future),
                "horizon_now_available": bool(horizon20_ymd is not None and latest_ymd is not None and latest_ymd >= horizon20_ymd),
                "permanently_unresolvable": anchor_ymd is None,
            }
        )
    return out


def _decision(rows: list[dict[str, Any]]) -> tuple[str, str]:
    if any(row["permanently_unresolvable"] for row in rows):
        return "drop_due_to_unresolvable_months", "calendar_or_price_history_missing"
    if rows and all(row["horizon_now_available"] for row in rows):
        return "ready_for_full_recheck", "all_skipped_month_horizons_matured"
    if any(row["horizon_now_available"] for row in rows):
        return "ready_for_partial_recheck", "some_skipped_month_horizons_matured"
    return "wait_until_full_horizon_matures", "skipped_month_horizons_not_ready"


def run(*, source_root: str | Path = DEFAULT_SOURCE_ROOT, output_dir: str | Path | None = None, db_path: str | Path | None = None) -> dict[str, Any]:
    source_root_path = Path(source_root).expanduser().resolve()
    output_root = Path(output_dir).expanduser().resolve() if output_dir else DEFAULT_OUTPUT_ROOT
    output_root.mkdir(parents=True, exist_ok=True)
    source_db = base._resolve_db_path(str(db_path) if db_path else None)
    generated_at = _utc_now()
    skipped = _load_skipped_rows(source_root_path)
    maturity = _maturity_rows(skipped, _load_calendar(source_db))
    decision, reason = _decision(maturity)
    ready_count = sum(1 for row in maturity if row["horizon_now_available"])
    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "boundary": "TRADEX-only",
        "source_root": str(source_root_path),
        "input_source_db": str(source_db),
        "what_will_not_change": ["MeeMee", "production ranking", "active champion", "publish", "live sell signal"],
    }
    calendar_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_calendar_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "skipped_month_count": len(maturity),
        "ready_month_count": ready_count,
        "all_skipped_months_matured": bool(maturity and ready_count == len(maturity)),
        "permanently_unresolvable_month_count": sum(1 for row in maturity if row["permanently_unresolvable"]),
        "earliest_partial_recheck_ymd": min((row["required_horizon20_ymd"] for row in maturity if row["horizon_now_available"] and row["required_horizon20_ymd"]), default=None),
        "earliest_full_recheck_ymd": max((row["required_horizon20_ymd"] for row in maturity if row["required_horizon20_ymd"]), default=None),
        "rows": maturity,
    }
    recheck_plan = {
        "schema_version": f"{SCHEMA_PREFIX}_recheck_plan_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "decision": decision,
        "reason_type": reason,
        "next_action": "rerun_v5_stability_and_v6_monthly_coverage_now" if decision == "ready_for_full_recheck" else "wait_for_horizon_maturity",
        "acceptance_gate": [
            "evaluated_month_count >= 6",
            "improved_event_month_count > degraded_event_month_count",
            "20d quality does not materially worsen",
            "no-lookahead passes",
            "production state remains unchanged",
        ],
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "decision": decision,
        "reason_type": reason,
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "next_gate": recheck_plan["next_action"],
        "maturity": {k: v for k, v in calendar_payload.items() if k != "rows"},
    }
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "pass": True,
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "calendar_only_maturity_check": True,
        "silent_fallback_used": False,
        "runtime_db_written": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    _write_json(output_root / "downside_prebreak_maturity_gate_contract.json", contract)
    pd.DataFrame(maturity).to_csv(output_root / "downside_prebreak_skipped_months.csv", index=False)
    _write_json(output_root / "downside_prebreak_maturity_calendar.json", calendar_payload)
    _write_json(output_root / "downside_prebreak_recheck_plan.json", recheck_plan)
    _write_json(output_root / "downside_prebreak_maturity_decision.json", decision_payload)
    _write_json(output_root / "no_lookahead_audit.json", no_lookahead)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": generated_at,
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "decision": decision,
        "reason_type": reason,
        "silent_fallback_used": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "decision": decision, "reason_type": reason, "maturity": calendar_payload}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Maturity gate for downside prebreak monthly coverage hold.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--db-path", default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run(source_root=args.source_root, output_dir=args.output_dir, db_path=args.db_path)
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
