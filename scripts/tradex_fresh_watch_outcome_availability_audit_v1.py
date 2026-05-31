from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "fresh_watch_outcome_availability_audit_v1"
DEFAULT_WATCH_ROOT = Path(r"G:\Tradex\fresh_runtime_watch_validation_protocol_v1\20260525T143854Z-fresh-runtime-watch-validation-protocol-v1")
DEFAULT_SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\fresh_watch_outcome_availability_audit_v1")
REQUIRED_ARTIFACTS = (
    "fresh_outcome_availability_summary.json",
    "fresh_watch_outcome_availability_rows.csv",
    "outcome_window_status.json",
    "future_evaluation_readiness.json",
    "source_coverage.json",
    "no_lookahead_audit.json",
    "lineage.json",
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


def load_watch_rows(watch_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows_path = watch_root / "fresh_watch_rows.csv"
    decision_path = watch_root / "research_decision.json"
    if not rows_path.exists():
        raise FileNotFoundError(rows_path)
    if not decision_path.exists():
        raise FileNotFoundError(decision_path)
    rows = pd.read_csv(rows_path, dtype={"code": str})
    rows["as_of_date"] = pd.to_numeric(rows["as_of_date"], errors="coerce").astype("Int64")
    return rows, _load_json(decision_path)


def load_bar_calendar(source_db: Path, codes: list[str], min_date: int) -> pd.DataFrame:
    if not source_db.exists():
        raise FileNotFoundError(source_db)
    con = duckdb.connect(str(source_db), read_only=True)
    try:
        expr = "CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)"
        query = f"""
            SELECT CAST(code AS VARCHAR) AS code, {expr} AS bar_date, c AS close
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN (SELECT * FROM UNNEST(?))
              AND {expr} >= ?
            ORDER BY code, bar_date
        """
        return con.execute(query, [codes, int(min_date)]).fetchdf()
    finally:
        con.close()


def build_availability_rows(watch_rows: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    out = watch_rows.copy()
    future_counts: list[int] = []
    ret5_ready: list[bool] = []
    ret20_ready: list[bool] = []
    latest_dates: list[int | None] = []
    future_date_text: list[str] = []
    by_code = {str(code): grp.sort_values("bar_date") for code, grp in bars.groupby("code")}
    for row in out.itertuples(index=False):
        code = str(row.code)
        as_of = int(row.as_of_date)
        grp = by_code.get(code, pd.DataFrame(columns=["bar_date"]))
        future = grp[pd.to_numeric(grp["bar_date"], errors="coerce") > as_of]
        dates = pd.to_numeric(future["bar_date"], errors="coerce").dropna().astype(int).tolist()
        future_counts.append(len(dates))
        ret5_ready.append(len(dates) >= 5)
        ret20_ready.append(len(dates) >= 20)
        latest_dates.append(max(dates) if dates else None)
        future_date_text.append(",".join(str(d) for d in dates[:20]))
    out["available_future_session_count"] = future_counts
    out["ret5_evaluation_ready"] = ret5_ready
    out["ret20_evaluation_ready"] = ret20_ready
    out["latest_available_future_bar_date"] = latest_dates
    out["available_future_bar_dates"] = future_date_text
    out["buyable_selection_ready"] = False
    out["validated_buy"] = False
    return out


def outcome_window_status(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "axis_id": AXIS_ID,
        "watch_row_count": int(len(rows)),
        "minimum_available_future_sessions": int(rows["available_future_session_count"].min()) if not rows.empty else 0,
        "median_available_future_sessions": float(rows["available_future_session_count"].median()) if not rows.empty else 0.0,
        "maximum_available_future_sessions": int(rows["available_future_session_count"].max()) if not rows.empty else 0,
        "ret5_ready_count": int(rows["ret5_evaluation_ready"].sum()) if not rows.empty else 0,
        "ret20_ready_count": int(rows["ret20_evaluation_ready"].sum()) if not rows.empty else 0,
        "ret5_all_rows_ready": bool(rows["ret5_evaluation_ready"].all()) if not rows.empty else False,
        "ret20_all_rows_ready": bool(rows["ret20_evaluation_ready"].all()) if not rows.empty else False,
    }


def no_lookahead_audit(rows: pd.DataFrame, watch_decision: dict[str, Any]) -> dict[str, Any]:
    forbidden = {"ret5", "ret10", "ret20", "winner_ret20_gt_10pct", "bad_ret20_lt_minus_5pct", "severe_ret20_lt_minus_10pct"}
    present = sorted(forbidden & set(rows.columns))
    source_ok = watch_decision.get("research_decision") == "fresh_runtime_watch_protocol_ready_for_future_outcome_validation"
    passed = not present and source_ok
    return {
        "audit_result": "pass" if passed else "blocked",
        "no_lookahead_pass": passed,
        "watch_rows_frozen_before_outcome_join": True,
        "future_outcome_values_joined": False,
        "forbidden_outcome_columns_present": present,
        "source_watch_protocol_ready": source_ok,
        "runtime_db_write": False,
        "research_fallback_used": False,
    }


def decide(status: dict[str, Any], audit: dict[str, Any]) -> tuple[str, str, list[str]]:
    if not audit["no_lookahead_pass"]:
        return "blocked_no_lookahead_violation", "BLOCKED", ["fresh_watch_protocol_or_rows_failed_no_lookahead"]
    if status["ret20_all_rows_ready"]:
        return "fresh_watch_ret20_outcomes_ready_for_evaluation", "HOLD_UNDERPOWERED", ["future_ret20_window_available_for_all_fresh_watch_rows"]
    if status["ret5_all_rows_ready"]:
        return "fresh_watch_ret5_ready_ret20_pending", "HOLD_UNDERPOWERED", ["short_horizon_ready_but_starter_entry_ret20_pending"]
    return "fresh_watch_outcomes_pending_more_confirmed_bars", "HOLD_UNDERPOWERED", ["insufficient_future_confirmed_sessions_for_fresh_ret5_ret20_evaluation"]


def run(watch_root: Path = DEFAULT_WATCH_ROOT, source_db: Path = DEFAULT_SOURCE_DB, output_root: Path = DEFAULT_OUTPUT_ROOT) -> Path:
    watch_rows, watch_decision = load_watch_rows(watch_root)
    min_date = int(pd.to_numeric(watch_rows["as_of_date"], errors="coerce").min())
    codes = sorted(watch_rows["code"].astype(str).unique().tolist())
    bars = load_bar_calendar(source_db, codes, min_date)
    availability = build_availability_rows(watch_rows, bars)
    status = outcome_window_status(availability)
    audit = no_lookahead_audit(availability, watch_decision)
    decision, decision_class, reasons = decide(status, audit)

    out = output_root / f"{_now_tag()}-fresh-watch-outcome-availability-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    availability.to_csv(out / "fresh_watch_outcome_availability_rows.csv", index=False)
    summary = {
        "axis_id": AXIS_ID,
        "decision": decision,
        "decision_class": decision_class,
        "reason_typed": reasons,
        "watch_root": str(watch_root),
        "source_db": str(source_db),
        "as_of_date": min_date,
        **status,
        "buyable_selection_ready": False,
        "validated_buy_count": 0,
    }
    _write_json(out / "fresh_outcome_availability_summary.json", summary)
    _write_json(out / "outcome_window_status.json", status)
    _write_json(
        out / "future_evaluation_readiness.json",
        {
            "axis_id": AXIS_ID,
            "ret5_ready": status["ret5_all_rows_ready"],
            "ret20_ready": status["ret20_all_rows_ready"],
            "current_available_future_sessions": status["minimum_available_future_sessions"],
            "required_future_sessions": {"ret5": 5, "ret20": 20},
            "next_valid_action": "wait_for_more_confirmed_bars_then_run_fresh_watch_forward_outcome_evaluation",
            "buyable_selection_ready": False,
            "validated_buy_count": 0,
        },
    )
    _write_json(out / "source_coverage.json", {"axis_id": AXIS_ID, "source_db": str(source_db), "watch_root": str(watch_root), "watch_row_count": int(len(watch_rows)), "bar_row_count_for_watch_codes": int(len(bars)), "latest_bar_date_for_watch_codes": int(bars["bar_date"].max()) if not bars.empty else None, "research_fallback_used": False})
    _write_json(out / "no_lookahead_audit.json", audit)
    _write_json(out / "lineage.json", {"axis_id": AXIS_ID, "watch_root": str(watch_root), "watch_research_decision": watch_decision, "source_db": str(source_db)})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "decision_class": decision_class, "reason_typed": reasons, "research_watch_only": True, "buyable_selection_ready": False, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "production_candidate_generator_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False, "research_fallback_used": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch-root", type=Path, default=DEFAULT_WATCH_ROOT)
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.watch_root, args.source_db, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
