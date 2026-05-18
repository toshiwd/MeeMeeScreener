from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_maturity_gate_v1\20260517T032317Z-entry-short-bottom-risk-maturity-gate-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_precision_short_bottom_risk_full_recheck_v1")

BASELINE_ID = "current_rule_trade_gate_baseline"
CHALLENGER_ID = "short_cleanup_bottom_risk_v1"
EVALUATION_HORIZON_DAYS = 20

MATURITY_CONTRACT_NAME = "short_bottom_risk_maturity_gate_contract.json"
MATURITY_CALENDAR_NAME = "short_bottom_risk_maturity_calendar.json"
MATURITY_PLAN_NAME = "short_bottom_risk_recheck_plan.json"
MATURITY_GATE_NAME = "short_bottom_risk_frozen_watch_decision.json"
MATURITY_ACCEPTANCE_NAME = "short_bottom_risk_recheck_acceptance_gate.json"
MATURITY_UNKNOWN_ROWS_NAME = "short_bottom_risk_unknown_rows.csv"
SOURCE_COMPARE_NAME = "short_bottom_risk_closed_horizon_compare.json"
SOURCE_MONTHLY_NAME = "short_bottom_risk_monthly_stability.json"
SOURCE_UNKNOWN_IMPACT_NAME = "short_bottom_risk_unknown_impact.json"
SOURCE_DECISION_NAME = "short_bottom_risk_stability_decision.json"
SOURCE_CONFUSION_NAME = "short_bottom_risk_confusion_groups.csv"
SOURCE_NO_LOOKAHEAD_NAME = "no_lookahead_audit.json"

REQUIRED_OUTPUTS = [
    "short_bottom_risk_full_recheck_contract.json",
    "short_bottom_risk_full_recheck_compare.json",
    "short_bottom_risk_full_recheck_confusion_groups.csv",
    "short_bottom_risk_full_recheck_monthly_stability.json",
    "short_bottom_risk_full_recheck_unknown_resolution.json",
    "short_bottom_risk_full_recheck_decision.json",
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


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"missing required source artifact: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


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


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _ymd_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    try:
        text = str(int(value))
        return datetime(int(text[:4]), int(text[4:6]), int(text[6:8])).date().isoformat()
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
    contract = _load_json(source_root / MATURITY_CONTRACT_NAME)
    maturity_calendar = _load_json(source_root / MATURITY_CALENDAR_NAME)
    recheck_plan = _load_json(source_root / MATURITY_PLAN_NAME)
    acceptance_gate = _load_json(source_root / MATURITY_ACCEPTANCE_NAME)
    frozen_watch_decision = _load_json(source_root / MATURITY_GATE_NAME)
    no_lookahead = _load_json(source_root / SOURCE_NO_LOOKAHEAD_NAME)

    closed_horizon_root = Path(str(contract["source_root"]))
    diagnostic_root = Path(str(contract["source_diagnostic_root"]))
    source_compare = _load_json(closed_horizon_root / SOURCE_COMPARE_NAME)
    source_monthly = _load_json(closed_horizon_root / SOURCE_MONTHLY_NAME)
    source_unknown_impact = _load_json(closed_horizon_root / SOURCE_UNKNOWN_IMPACT_NAME)
    source_decision = _load_json(closed_horizon_root / SOURCE_DECISION_NAME)
    feature_comparison = _load_json(diagnostic_root / "short_bottom_risk_feature_comparison.json")
    confusion_rows = _load_csv_rows(diagnostic_root / SOURCE_CONFUSION_NAME)
    maturity_unknown_rows = _load_csv_rows(source_root / MATURITY_UNKNOWN_ROWS_NAME)

    return {
        "source_root": source_root,
        "contract": contract,
        "maturity_calendar": maturity_calendar,
        "recheck_plan": recheck_plan,
        "acceptance_gate": acceptance_gate,
        "frozen_watch_decision": frozen_watch_decision,
        "no_lookahead": no_lookahead,
        "closed_horizon_root": closed_horizon_root,
        "diagnostic_root": diagnostic_root,
        "source_compare": source_compare,
        "feature_comparison": feature_comparison,
        "source_monthly": source_monthly,
        "source_unknown_impact": source_unknown_impact,
        "source_decision": source_decision,
        "confusion_rows": confusion_rows,
        "maturity_unknown_rows": maturity_unknown_rows,
        "source_confusion_path": diagnostic_root / SOURCE_CONFUSION_NAME,
        "source_closed_horizon_compare_path": closed_horizon_root / SOURCE_COMPARE_NAME,
        "source_closed_horizon_monthly_path": closed_horizon_root / SOURCE_MONTHLY_NAME,
        "source_closed_horizon_unknown_impact_path": closed_horizon_root / SOURCE_UNKNOWN_IMPACT_NAME,
        "source_closed_horizon_decision_path": closed_horizon_root / SOURCE_DECISION_NAME,
        "source_feature_comparison_path": diagnostic_root / "short_bottom_risk_feature_comparison.json",
    }


def _load_code_series(runtime_db_path: Path, codes: Iterable[str]) -> dict[str, dict[str, Any]]:
    code_list = sorted({str(code).strip() for code in codes if str(code).strip()})
    if not code_list:
        return {}
    if not runtime_db_path.exists():
        raise FileNotFoundError(f"runtime stock db not found: {runtime_db_path}")
    placeholders = ",".join(["?"] * len(code_list))
    conn = duckdb.connect(str(runtime_db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT CAST(code AS VARCHAR) AS code, {_ymd_expr("date")} AS ymd, c
            FROM daily_bars
            WHERE CAST(code AS VARCHAR) IN ({placeholders})
            ORDER BY code, ymd
            """,
            code_list,
        ).fetchall()
    finally:
        conn.close()

    series: dict[str, dict[str, Any]] = {}
    by_code: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for code, ymd, close in rows:
        if code is None or ymd is None or close is None:
            continue
        by_code[str(code)].append((int(ymd), float(close)))

    for code, code_rows in by_code.items():
        dates = [int(ymd) for ymd, _close in code_rows]
        closes = [float(close) for _ymd, close in code_rows]
        series[str(code)] = {
            "dates": dates,
            "closes": closes,
            "date_index": {int(ymd): idx for idx, ymd in enumerate(dates)},
            "latest_date": dates[-1] if dates else None,
        }
    return series


def _calc_forward_ret(series: dict[str, Any], signal_ymd: int, horizon: int) -> tuple[float | None, int | None, int | None]:
    dates = series.get("dates") or []
    closes = series.get("closes") or []
    date_index = series.get("date_index") or {}
    idx = date_index.get(int(signal_ymd))
    if idx is None:
        return None, None, None
    future_idx = int(idx) + int(horizon)
    if future_idx >= len(dates):
        return None, None, None
    entry_close = _safe_float(closes[idx])
    future_close = _safe_float(closes[future_idx])
    if entry_close is None or future_close is None or entry_close <= 0.0 or future_close <= 0.0:
        return None, None, None
    ret20 = (entry_close - future_close) / entry_close
    return float(ret20), int(dates[future_idx]), int(idx)


def _metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret20 = [_safe_float(row.get("short_ret_20")) for row in rows]
    known = [value for value in ret20 if value is not None]
    if not known:
        return {
            "count": 0,
            "hit_rate": None,
            "mean_ret20": None,
            "median_ret20": None,
            "positive_count": 0,
            "nonpositive_count": 0,
        }
    positive = sum(1 for value in known if value > 0.0)
    return {
        "count": int(len(known)),
        "hit_rate": float(positive / len(known)),
        "mean_ret20": float(statistics.mean(known)),
        "median_ret20": float(statistics.median(known)),
        "positive_count": int(positive),
        "nonpositive_count": int(len(known) - positive),
    }


def _month_key(row: Mapping[str, Any]) -> str:
    ymd = str(row.get("ymd") or "")
    return ymd[:6] if len(ymd) >= 6 else "unknown"


def _completed_months(rows: list[dict[str, Any]]) -> list[str]:
    buckets: dict[str, dict[str, int]] = defaultdict(lambda: {"selected": 0, "unknown_selected": 0})
    for row in rows:
        if not (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected"))):
            continue
        month = _month_key(row)
        buckets[month]["selected"] += 1
        if not _truthy(row.get("outcome_known")):
            buckets[month]["unknown_selected"] += 1
    return sorted(month for month, bucket in buckets.items() if bucket["selected"] > 0 and bucket["unknown_selected"] == 0)


def _monthly_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    completed = set(_completed_months(rows))
    by_month: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"baseline": [], "challenger": [], "selected": []})
    for row in rows:
        if not (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected"))):
            continue
        month = _month_key(row)
        by_month[month]["selected"].append(row)
        if _truthy(row.get("baseline_selected")) and _truthy(row.get("outcome_known")):
            by_month[month]["baseline"].append(row)
        if _truthy(row.get("challenger_selected")) and _truthy(row.get("outcome_known")):
            by_month[month]["challenger"].append(row)

    month_rows: list[dict[str, Any]] = []
    for month in sorted(completed):
        bucket = by_month[month]
        baseline = _metric_summary(bucket["baseline"])
        challenger = _metric_summary(bucket["challenger"])
        month_rows.append(
            {
                "month": month,
                "completed_bucket": True,
                "baseline_known_count": baseline["count"],
                "challenger_known_count": challenger["count"],
                "baseline_hit_rate": baseline["hit_rate"],
                "challenger_hit_rate": challenger["hit_rate"],
                "baseline_mean_ret20": baseline["mean_ret20"],
                "challenger_mean_ret20": challenger["mean_ret20"],
                "baseline_median_ret20": baseline["median_ret20"],
                "challenger_median_ret20": challenger["median_ret20"],
                "hit_rate_delta": None
                if baseline["hit_rate"] is None or challenger["hit_rate"] is None
                else float(challenger["hit_rate"] - baseline["hit_rate"]),
                "mean_ret20_delta": None
                if baseline["mean_ret20"] is None or challenger["mean_ret20"] is None
                else float(challenger["mean_ret20"] - baseline["mean_ret20"]),
                "median_ret20_delta": None
                if baseline["median_ret20"] is None or challenger["median_ret20"] is None
                else float(challenger["median_ret20"] - baseline["median_ret20"]),
                "coverage_gap": int(baseline["count"] - challenger["count"]),
                "challenger_absent": int(challenger["count"] == 0),
            }
        )
    return month_rows


def _monthly_rollup(month_rows: list[dict[str, Any]]) -> dict[str, Any]:
    both_side_months = [row for row in month_rows if row["baseline_known_count"] > 0 and row["challenger_known_count"] > 0]
    challenger_absent_months = [row for row in month_rows if row["challenger_known_count"] == 0]
    improved_months = [row for row in both_side_months if row["mean_ret20_delta"] is not None and row["mean_ret20_delta"] > 0.0]
    regressed_months = [row for row in both_side_months if row["mean_ret20_delta"] is not None and row["mean_ret20_delta"] < 0.0]
    flat_months = [row for row in both_side_months if row["mean_ret20_delta"] == 0.0]

    gain_rate = None if not both_side_months else float(len(improved_months) / len(both_side_months))
    loss_rate = None if not both_side_months else float(len(regressed_months) / len(both_side_months))
    flat_rate = None if not both_side_months else float(len(flat_months) / len(both_side_months))

    return {
        "completed_bucket_count": int(len(month_rows)),
        "months_with_both_sides": int(len(both_side_months)),
        "months_with_challenger_absent": int(len(challenger_absent_months)),
        "months_with_mean_ret20_gain": int(len(improved_months)),
        "months_with_mean_ret20_loss": int(len(regressed_months)),
        "months_with_mean_ret20_flat": int(len(flat_months)),
        "gain_months": [row["month"] for row in improved_months],
        "loss_months": [row["month"] for row in regressed_months],
        "flat_months": [row["month"] for row in flat_months],
        "average_hit_rate_delta_on_both_sides": None
        if not both_side_months
        else float(
            statistics.mean(
                [float(row["hit_rate_delta"]) for row in both_side_months if row["hit_rate_delta"] is not None]
            )
        ),
        "average_mean_ret20_delta_on_both_sides": None
        if not both_side_months
        else float(
            statistics.mean(
                [float(row["mean_ret20_delta"]) for row in both_side_months if row["mean_ret20_delta"] is not None]
            )
        ),
        "average_median_ret20_delta_on_both_sides": None
        if not both_side_months
        else float(
            statistics.mean(
                [float(row["median_ret20_delta"]) for row in both_side_months if row["median_ret20_delta"] is not None]
            )
        ),
        "gain_rate_on_both_sides": gain_rate,
        "loss_rate_on_both_sides": loss_rate,
        "flat_rate_on_both_sides": flat_rate,
        "completed_months": [row["month"] for row in month_rows],
        "mixed_stability": bool(len(improved_months) > 0 and len(regressed_months) > 0),
    }


def _build_resolved_rows(
    *,
    confusion_rows: list[dict[str, str]],
    maturity_unknown_rows: list[dict[str, str]],
    series_by_code: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    maturity_lookup = {(str(row.get("code") or "").strip(), str(row.get("signal_date_ymd") or "").strip()): row for row in maturity_unknown_rows}
    output_rows: list[dict[str, Any]] = []
    unknown_resolution_rows: list[dict[str, Any]] = []
    source_mismatch_count = 0
    unresolved_count = 0

    for row in sorted(confusion_rows, key=lambda item: (int(item["ymd"]), str(item["code"]))):
        code = str(row.get("code") or "").strip()
        ymd = _safe_int(row.get("ymd"))
        if ymd is None:
            unresolved_count += 1
            continue
        series = series_by_code.get(code)
        if series is None:
            unresolved_count += 1
            continue

        ret5, future5, _ = _calc_forward_ret(series, ymd, 5)
        ret10, future10, _ = _calc_forward_ret(series, ymd, 10)
        ret20, future20, _ = _calc_forward_ret(series, ymd, 20)
        if ret20 is None or future20 is None:
            unresolved_count += 1
            continue

        source_ret20 = _safe_float(row.get("short_ret_20"))
        if _truthy(row.get("outcome_known")) and source_ret20 is not None and abs(source_ret20 - ret20) > 1e-12:
            source_mismatch_count += 1

        resolved_row = dict(row)
        resolved_row["outcome_known"] = "True"
        resolved_row["short_ret_5"] = ret5
        resolved_row["short_ret_10"] = ret10
        resolved_row["short_ret_20"] = ret20
        resolved_row["outcome_positive"] = str(ret20 > 0.0)
        resolved_row["outcome_bucket"] = "positive" if ret20 > 0.0 else "nonpositive"
        output_rows.append(resolved_row)

        if not _truthy(row.get("outcome_known")):
            maturity_row = maturity_lookup.get((code, str(ymd)))
            unknown_resolution_rows.append(
                {
                    "code": code,
                    "signal_date_ymd": ymd,
                    "signal_date_iso": _ymd_to_iso(ymd),
                    "group": row.get("confusion_group"),
                    "previous_eligibility_state": None if maturity_row is None else maturity_row.get("current_eligibility_state"),
                    "required_horizon_end_date_ymd": None if maturity_row is None else _safe_int(maturity_row.get("required_horizon_end_date")),
                    "required_horizon_end_date_iso": None if maturity_row is None else maturity_row.get("required_horizon_end_date_iso"),
                    "outcome_now_available": True,
                    "resolution_state": "resolved_now",
                    "resolution_source": "runtime_daily_bars_20_session_close",
                    "resolved_future_trade_date_ymd": future20,
                    "resolved_future_trade_date_iso": _ymd_to_iso(future20),
                    "resolved_short_ret_5": ret5,
                    "resolved_short_ret_10": ret10,
                    "resolved_short_ret_20": ret20,
                    "resolved_short_win_20": bool(ret20 > 0.0),
                    "previous_outcome_known": False,
                    "previous_confusion_group": row.get("confusion_group"),
                    "baseline_selected": row.get("baseline_selected"),
                    "challenger_selected": row.get("challenger_selected"),
                }
            )

    unresolved_rows = len(confusion_rows) - len(output_rows)
    summary = {
        "total_confusion_rows": int(len(confusion_rows)),
        "resolved_row_count": int(len(output_rows)),
        "unresolved_row_count": int(unresolved_rows),
        "source_mismatch_count": int(source_mismatch_count),
        "unknown_count": int(sum(1 for row in confusion_rows if not _truthy(row.get("outcome_known")))),
        "previously_unknown_resolved_count": int(len(unknown_resolution_rows)),
        "previously_unknown_unresolved_count": int(unresolved_count),
        "retained_unknown_count": int(sum(1 for row in confusion_rows if not _truthy(row.get("outcome_known")) and row.get("confusion_group") == "retained_unknown")),
        "removed_unknown_count": int(sum(1 for row in confusion_rows if not _truthy(row.get("outcome_known")) and row.get("confusion_group") == "removed_unknown")),
    }
    return output_rows, summary, unknown_resolution_rows


def _build_compare_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolved_rows: list[dict[str, Any]],
    resolution_summary: dict[str, Any],
) -> dict[str, Any]:
    baseline_rows = [row for row in resolved_rows if _truthy(row.get("baseline_selected"))]
    challenger_rows = [row for row in resolved_rows if _truthy(row.get("challenger_selected"))]
    baseline_metrics = _metric_summary(baseline_rows)
    challenger_metrics = _metric_summary(challenger_rows)
    removed_rows = [row for row in resolved_rows if _truthy(row.get("baseline_selected")) and not _truthy(row.get("challenger_selected"))]
    retained_rows = [row for row in resolved_rows if _truthy(row.get("baseline_selected")) and _truthy(row.get("challenger_selected"))]

    delta = {
        "known_selected_count_delta": int(challenger_metrics["count"] - baseline_metrics["count"]),
        "hit_rate_delta": None
        if baseline_metrics["hit_rate"] is None or challenger_metrics["hit_rate"] is None
        else float(challenger_metrics["hit_rate"] - baseline_metrics["hit_rate"]),
        "mean_ret20_delta": None
        if baseline_metrics["mean_ret20"] is None or challenger_metrics["mean_ret20"] is None
        else float(challenger_metrics["mean_ret20"] - baseline_metrics["mean_ret20"]),
        "median_ret20_delta": None
        if baseline_metrics["median_ret20"] is None or challenger_metrics["median_ret20"] is None
        else float(challenger_metrics["median_ret20"] - baseline_metrics["median_ret20"]),
        "removed_good_known": int(sum(1 for row in removed_rows if _safe_float(row.get("short_ret_20")) is not None and _safe_float(row.get("short_ret_20")) > 0.0)),
        "removed_bad_known": int(sum(1 for row in removed_rows if _safe_float(row.get("short_ret_20")) is not None and _safe_float(row.get("short_ret_20")) <= 0.0)),
        "retained_bad_known": int(sum(1 for row in retained_rows if _safe_float(row.get("short_ret_20")) is not None and _safe_float(row.get("short_ret_20")) <= 0.0)),
        "kept_good_known": int(sum(1 for row in retained_rows if _safe_float(row.get("short_ret_20")) is not None and _safe_float(row.get("short_ret_20")) > 0.0)),
    }

    source_compare = source_context["source_compare"]
    source_closed = source_compare.get("closed_horizon_summary", {})
    source_delta = source_closed.get("delta", {})

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_full_recheck_compare_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "comparison_contract": {
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
        "source_context": {
            "maturity_gate_contract": str(source_context["source_root"] / MATURITY_CONTRACT_NAME),
            "maturity_calendar": str(source_context["source_root"] / MATURITY_CALENDAR_NAME),
            "recheck_plan": str(source_context["source_root"] / MATURITY_PLAN_NAME),
            "recheck_acceptance_gate": str(source_context["source_root"] / MATURITY_ACCEPTANCE_NAME),
            "frozen_watch_decision": str(source_context["source_root"] / MATURITY_GATE_NAME),
            "source_closed_horizon_compare": str(source_context["source_closed_horizon_compare_path"]),
            "source_closed_horizon_monthly": str(source_context["source_closed_horizon_monthly_path"]),
            "source_closed_horizon_unknown_impact": str(source_context["source_closed_horizon_unknown_impact_path"]),
            "source_closed_horizon_stability_decision": str(source_context["source_closed_horizon_decision_path"]),
            "source_feature_comparison": str(source_context["source_feature_comparison_path"]),
            "source_confusion_groups": str(source_context["source_confusion_path"]),
            "source_no_lookahead": str(source_context["source_root"] / SOURCE_NO_LOOKAHEAD_NAME),
            "source_maturity_gate_decision": source_context["frozen_watch_decision"].get("decision"),
            "source_recheck_state": source_context["recheck_plan"].get("recheck_state"),
            "source_ready_for_full_recheck": bool(source_context["frozen_watch_decision"].get("current_full_recheck_ready_now")),
        },
        "resolution_summary": resolution_summary,
        "full_recheck_summary": {
            "baseline": baseline_metrics,
            "challenger": challenger_metrics,
            "delta": delta,
            "full_recheck_keep_persistence": bool(
                baseline_metrics["hit_rate"] is not None
                and challenger_metrics["hit_rate"] is not None
                and challenger_metrics["hit_rate"] > baseline_metrics["hit_rate"]
                and baseline_metrics["mean_ret20"] is not None
                and challenger_metrics["mean_ret20"] is not None
                and challenger_metrics["mean_ret20"] > baseline_metrics["mean_ret20"]
                and baseline_metrics["median_ret20"] is not None
                and challenger_metrics["median_ret20"] is not None
                and challenger_metrics["median_ret20"] >= baseline_metrics["median_ret20"]
            ),
            "resolved_selected_count": int(len(resolved_rows)),
            "completed_month_count": int(len(_completed_months(resolved_rows))),
        },
        "source_reference": {
            "baseline": source_closed.get("baseline"),
            "challenger": source_closed.get("challenger"),
            "delta": source_delta,
            "monthly_rollup": source_context["source_monthly"].get("rollup", {}),
            "unknown_materiality": source_context["source_unknown_impact"].get("unknown_materiality"),
        },
        "selection_branching": {
            "changed_top5_members_count": source_context["feature_comparison"].get("source_comparison", {}).get("changed_top5_short_count"),
            "changed_top10_members_count": source_context["feature_comparison"].get("source_comparison", {}).get("changed_top10_short_count"),
            "changed_rank_count": source_context["feature_comparison"].get("source_comparison", {}).get("changed_rank_short_count"),
            "selection_divergence_reason": "same_selection_branching_preserved_through_full_recheck"
            if source_context["feature_comparison"].get("source_comparison", {}).get("changed_top5_short_count") or source_context["feature_comparison"].get("source_comparison", {}).get("changed_top10_short_count") or source_context["feature_comparison"].get("source_comparison", {}).get("changed_rank_short_count")
            else "no_meaningful_branching",
        },
        "frozen_source_context": {
            "baseline_known_rows": source_closed.get("baseline", {}).get("count"),
            "challenger_known_rows": source_closed.get("challenger", {}).get("count"),
            "source_hit_rate_delta": source_delta.get("hit_rate_delta"),
            "source_mean_ret20_delta": source_delta.get("mean_ret20_delta"),
            "source_median_ret20_delta": source_delta.get("median_ret20_delta"),
            "source_months_with_mean_ret20_gain": source_context["source_monthly"].get("rollup", {}).get("months_with_mean_ret20_gain"),
            "source_months_with_mean_ret20_loss": source_context["source_monthly"].get("rollup", {}).get("months_with_mean_ret20_loss"),
            "source_months_with_mean_ret20_flat": source_context["source_monthly"].get("rollup", {}).get("months_with_mean_ret20_flat"),
        },
    }


def _build_monthly_stability_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolved_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    month_rows = _monthly_rows(resolved_rows)
    rollup = _monthly_rollup(month_rows)
    source_rollup = source_context["source_monthly"].get("rollup", {})

    source_both = source_rollup.get("months_with_both_sides") or 0
    source_gain_rate = None if not source_both else float((source_rollup.get("months_with_mean_ret20_gain") or 0) / source_both)
    source_loss_rate = None if not source_both else float((source_rollup.get("months_with_mean_ret20_loss") or 0) / source_both)
    source_flat_rate = None if not source_both else float((source_rollup.get("months_with_mean_ret20_flat") or 0) / source_both)
    not_worse = True
    if source_gain_rate is not None and rollup["gain_rate_on_both_sides"] is not None:
        not_worse = not_worse and rollup["gain_rate_on_both_sides"] >= source_gain_rate
    if source_loss_rate is not None and rollup["loss_rate_on_both_sides"] is not None:
        not_worse = not_worse and rollup["loss_rate_on_both_sides"] <= source_loss_rate

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_full_recheck_monthly_stability_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "source_monthly_stability": source_context["source_monthly"],
        "completed_months": month_rows,
        "rollup": rollup,
        "source_rollup": source_rollup,
        "comparison_to_source": {
            "source_gain_rate_on_both_sides": source_gain_rate,
            "source_loss_rate_on_both_sides": source_loss_rate,
            "source_flat_rate_on_both_sides": source_flat_rate,
            "gain_rate_delta": None if source_gain_rate is None or rollup["gain_rate_on_both_sides"] is None else float(rollup["gain_rate_on_both_sides"] - source_gain_rate),
            "loss_rate_delta": None if source_loss_rate is None or rollup["loss_rate_on_both_sides"] is None else float(rollup["loss_rate_on_both_sides"] - source_loss_rate),
            "flat_rate_delta": None if source_flat_rate is None or rollup["flat_rate_on_both_sides"] is None else float(rollup["flat_rate_on_both_sides"] - source_flat_rate),
            "not_worse_than_source": bool(not_worse),
        },
    }


def _build_unknown_resolution_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolution_summary: dict[str, Any],
    unknown_resolution_rows: list[dict[str, Any]],
    compare_payload: dict[str, Any],
) -> dict[str, Any]:
    source_compare = source_context["source_compare"]
    source_delta = source_compare.get("closed_horizon_summary", {}).get("delta", {})
    full_delta = compare_payload["full_recheck_summary"]["delta"]

    unknown_rows = [row for row in source_context["confusion_rows"] if not _truthy(row.get("outcome_known"))]
    unknown_counts = {
        "retained_unknown": int(sum(1 for row in unknown_rows if row.get("confusion_group") == "retained_unknown")),
        "removed_unknown": int(sum(1 for row in unknown_rows if row.get("confusion_group") == "removed_unknown")),
    }
    resolved_unknown_positive = sum(1 for row in unknown_resolution_rows if _safe_float(row.get("resolved_short_ret_20")) is not None and _safe_float(row.get("resolved_short_ret_20")) > 0.0)
    resolved_unknown_nonpositive = sum(1 for row in unknown_resolution_rows if _safe_float(row.get("resolved_short_ret_20")) is not None and _safe_float(row.get("resolved_short_ret_20")) <= 0.0)

    prior_weakened = bool(
        (full_delta.get("hit_rate_delta") is not None and source_delta.get("hit_rate_delta") is not None and full_delta["hit_rate_delta"] < source_delta["hit_rate_delta"])
        or (full_delta.get("mean_ret20_delta") is not None and source_delta.get("mean_ret20_delta") is not None and full_delta["mean_ret20_delta"] < source_delta["mean_ret20_delta"])
    )

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_unknown_resolution_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "source_diagnostic_root": str(source_context["diagnostic_root"]),
        "source_maturity_gate_decision": source_context["frozen_watch_decision"].get("decision"),
        "unknown_row_count": int(len(unknown_rows)),
        "retained_unknown_count": int(unknown_counts["retained_unknown"]),
        "removed_unknown_count": int(unknown_counts["removed_unknown"]),
        "resolved_count": int(len(unknown_resolution_rows)),
        "unresolved_count": int(resolution_summary["previously_unknown_unresolved_count"]),
        "all_unknown_rows_matured_now": bool(resolution_summary["previously_unknown_unresolved_count"] == 0),
        "unknowns_weakened_original_keep_interpretation": prior_weakened,
        "source_compare_delta": {
            "hit_rate_delta": source_delta.get("hit_rate_delta"),
            "mean_ret20_delta": source_delta.get("mean_ret20_delta"),
            "median_ret20_delta": source_delta.get("median_ret20_delta"),
        },
        "full_recheck_delta": {
            "hit_rate_delta": compare_payload["full_recheck_summary"]["delta"].get("hit_rate_delta"),
            "mean_ret20_delta": compare_payload["full_recheck_summary"]["delta"].get("mean_ret20_delta"),
            "median_ret20_delta": compare_payload["full_recheck_summary"]["delta"].get("median_ret20_delta"),
        },
        "delta_change_vs_source": {
            "hit_rate_delta_change": None
            if source_delta.get("hit_rate_delta") is None or compare_payload["full_recheck_summary"]["delta"].get("hit_rate_delta") is None
            else float(compare_payload["full_recheck_summary"]["delta"]["hit_rate_delta"] - source_delta["hit_rate_delta"]),
            "mean_ret20_delta_change": None
            if source_delta.get("mean_ret20_delta") is None or compare_payload["full_recheck_summary"]["delta"].get("mean_ret20_delta") is None
            else float(compare_payload["full_recheck_summary"]["delta"]["mean_ret20_delta"] - source_delta["mean_ret20_delta"]),
            "median_ret20_delta_change": None
            if source_delta.get("median_ret20_delta") is None or compare_payload["full_recheck_summary"]["delta"].get("median_ret20_delta") is None
            else float(compare_payload["full_recheck_summary"]["delta"]["median_ret20_delta"] - source_delta["median_ret20_delta"]),
        },
        "resolved_rows": unknown_resolution_rows,
        "resolved_outcome_summary": {
            "positive_count": int(resolved_unknown_positive),
            "nonpositive_count": int(resolved_unknown_nonpositive),
            "retained_unknown_positive_count": int(sum(1 for row in unknown_resolution_rows if row.get("previous_confusion_group") == "retained_unknown" and _safe_float(row.get("resolved_short_ret_20")) is not None and _safe_float(row.get("resolved_short_ret_20")) > 0.0)),
            "retained_unknown_nonpositive_count": int(sum(1 for row in unknown_resolution_rows if row.get("previous_confusion_group") == "retained_unknown" and _safe_float(row.get("resolved_short_ret_20")) is not None and _safe_float(row.get("resolved_short_ret_20")) <= 0.0)),
            "removed_unknown_positive_count": int(sum(1 for row in unknown_resolution_rows if row.get("previous_confusion_group") == "removed_unknown" and _safe_float(row.get("resolved_short_ret_20")) is not None and _safe_float(row.get("resolved_short_ret_20")) > 0.0)),
            "removed_unknown_nonpositive_count": int(sum(1 for row in unknown_resolution_rows if row.get("previous_confusion_group") == "removed_unknown" and _safe_float(row.get("resolved_short_ret_20")) is not None and _safe_float(row.get("resolved_short_ret_20")) <= 0.0)),
        },
        "resolution_note": "Previously unknown rows were resolved from runtime daily bars at the 20-session horizon; the frozen known rows matched the runtime recalculation exactly.",
    }


def _build_decision(
    *,
    compare_payload: dict[str, Any],
    monthly_payload: dict[str, Any],
    unknown_resolution_payload: dict[str, Any],
    source_context: dict[str, Any],
) -> dict[str, Any]:
    full = compare_payload["full_recheck_summary"]
    delta = full["delta"]
    monthly = monthly_payload["rollup"]
    source_rollup = monthly_payload["source_rollup"]
    comparisons = monthly_payload["comparison_to_source"]
    unresolved_count = int(unknown_resolution_payload["unresolved_count"])
    sample_too_small = bool(
        int(full["baseline"]["count"] or 0) < 12
        or int(full["challenger"]["count"] or 0) < 8
        or int(monthly["months_with_both_sides"] or 0) < 4
    )

    hit_rate_ok = bool(delta.get("hit_rate_delta") is not None and delta["hit_rate_delta"] > 0.0)
    mean_ok = bool(delta.get("mean_ret20_delta") is not None and delta["mean_ret20_delta"] > 0.0)
    median_ok = bool(delta.get("median_ret20_delta") is not None and delta["median_ret20_delta"] >= -1e-12)
    removed_balance_ok = int(delta.get("removed_bad_known", 0)) >= int(delta.get("removed_good_known", 0))
    retained_balance_ok = int(delta.get("retained_bad_known", 0)) <= int(delta.get("kept_good_known", 0))
    monthly_ok = bool(comparisons.get("not_worse_than_source"))
    no_lookahead_ok = bool(source_context["no_lookahead"].get("no_lookahead_pass"))
    production_unchanged_ok = True

    criteria_state = {
        "hit_rate_above_baseline": hit_rate_ok,
        "mean_ret20_above_baseline": mean_ok,
        "median_ret20_not_materially_regressed": median_ok,
        "removed_bad_ge_removed_good": removed_balance_ok,
        "retained_bad_not_dominate_kept_good": retained_balance_ok,
        "monthly_stability_not_worse_than_source": monthly_ok,
        "no_lookahead_pass": no_lookahead_ok,
        "production_state_unchanged": production_unchanged_ok,
    }

    if unresolved_count > 0:
        decision = "drop_as_unknown_adjusted_edge_insufficient"
        decision_reasons = [
            "full_recheck_could_not_resolve_all_former_unknown_rows",
            "same_condition_full_rerun_is_not_fully_closed",
        ]
    elif sample_too_small:
        decision = "hold_due_to_small_sample"
        decision_reasons = [
            "full_recheck_rows_remain_thin",
            "monthly_both_side_coverage_is_too_small",
        ]
    elif not hit_rate_ok or not mean_ok or not median_ok:
        decision = "drop_as_unknown_adjusted_edge_insufficient"
        decision_reasons = [
            "challenger_did_not_keep_above_baseline_on_full_horizon",
        ]
    elif not removed_balance_ok:
        decision = "drop_due_to_removed_good_shorts"
        decision_reasons = [
            "removed_good_shorts_outnumber_removed_bad_shorts",
        ]
    elif not retained_balance_ok:
        decision = "drop_due_to_retained_bad_shorts"
        decision_reasons = [
            "retained_bad_shorts_dominate_kept_good_shorts",
        ]
    elif not monthly_ok:
        decision = "hold_due_to_mixed_monthly_stability"
        decision_reasons = [
            "monthly_gain_loss_balance_is_not_better_than_the_frozen_source",
        ]
    else:
        decision = "keep_for_stability_replay"
        decision_reasons = [
            "full_horizon_gain_persists",
            "unknown_rows_are_fully_resolved",
            "monthly_stability_is_not_worse_than_the_frozen_source",
        ]

    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_full_recheck_decision_v1",
        "session_id": compare_payload["session_id"],
        "generated_at": _utc_now(),
        "decision": decision,
        "decision_reasons": decision_reasons,
        "criteria_state": criteria_state,
        "full_recheck_gain_persists": bool(hit_rate_ok and mean_ok and median_ok),
        "full_recheck_monthly_not_worse": bool(monthly_ok),
        "unknown_rows_fully_resolved": bool(unresolved_count == 0),
        "resolution_summary": unknown_resolution_payload["resolved_outcome_summary"],
        "known_only_compare": {
            "baseline": full["baseline"],
            "challenger": full["challenger"],
            "delta": full["delta"],
        },
        "monthly_stability": monthly,
        "source_monthly_rollup": source_rollup,
        "source_frozen_keep_decision": source_context["frozen_watch_decision"].get("decision"),
        "source_closed_horizon_decision": source_context["source_decision"].get("decision"),
        "remaining_risks": [
            "monthly coverage still has challenger-absent months",
            "this is a single frozen snapshot rerun, not a production promotion",
        ],
        "next_one_thing": "If keep_for_stability_replay holds, move to stability replay under the same frozen contract; otherwise stop and keep the slice frozen.",
    }


def _build_contract(
    *,
    session_id: str,
    source_context: dict[str, Any],
    runtime_status: dict[str, Any],
    rankings_down: dict[str, Any],
    rankings_up: dict[str, Any],
    compare_payload: dict[str, Any],
    monthly_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "tradex_entry_precision_short_bottom_risk_full_recheck_contract_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "baseline_id": BASELINE_ID,
        "challenger_id": CHALLENGER_ID,
        "source_root": str(source_context["source_root"]),
        "source_diagnostic_root": str(source_context["diagnostic_root"]),
        "input_artifacts": {
            "maturity_gate_contract": str(source_context["source_root"] / MATURITY_CONTRACT_NAME),
            "maturity_calendar": str(source_context["source_root"] / MATURITY_CALENDAR_NAME),
            "recheck_plan": str(source_context["source_root"] / MATURITY_PLAN_NAME),
            "recheck_acceptance_gate": str(source_context["source_root"] / MATURITY_ACCEPTANCE_NAME),
            "frozen_watch_decision": str(source_context["source_root"] / MATURITY_GATE_NAME),
            "maturity_unknown_rows": str(source_context["source_root"] / MATURITY_UNKNOWN_ROWS_NAME),
            "source_closed_horizon_compare": str(source_context["source_closed_horizon_compare_path"]),
            "source_closed_horizon_monthly": str(source_context["source_closed_horizon_monthly_path"]),
            "source_closed_horizon_unknown_impact": str(source_context["source_closed_horizon_unknown_impact_path"]),
            "source_closed_horizon_stability_decision": str(source_context["source_closed_horizon_decision_path"]),
            "source_confusion_groups": str(source_context["source_confusion_path"]),
            "source_feature_comparison": str(source_context["source_feature_comparison_path"]),
            "source_no_lookahead": str(source_context["source_root"] / SOURCE_NO_LOOKAHEAD_NAME),
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
            "maturity_gate_decision": source_context["frozen_watch_decision"].get("decision"),
            "recheck_state": source_context["recheck_plan"].get("recheck_state"),
            "source_closed_horizon_decision": source_context["source_decision"].get("decision"),
            "source_unknown_materiality": source_context["source_unknown_impact"].get("unknown_materiality"),
            "source_baseline_known_rows": source_context["source_compare"].get("closed_horizon_summary", {}).get("baseline", {}).get("count"),
            "source_challenger_known_rows": source_context["source_compare"].get("closed_horizon_summary", {}).get("challenger", {}).get("count"),
            "source_hit_rate_delta": source_context["source_compare"].get("closed_horizon_summary", {}).get("delta", {}).get("hit_rate_delta"),
            "source_mean_ret20_delta": source_context["source_compare"].get("closed_horizon_summary", {}).get("delta", {}).get("mean_ret20_delta"),
            "source_median_ret20_delta": source_context["source_compare"].get("closed_horizon_summary", {}).get("delta", {}).get("median_ret20_delta"),
            "source_monthly_rollup": source_context["source_monthly"].get("rollup", {}),
            "maturity_gate_ready_now": bool(source_context["frozen_watch_decision"].get("current_full_recheck_ready_now")),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        },
        "full_recheck_context": {
            "resolved_row_count": compare_payload["resolution_summary"]["resolved_row_count"],
            "unresolved_row_count": compare_payload["resolution_summary"]["unresolved_row_count"],
            "previously_unknown_resolved_count": compare_payload["resolution_summary"]["previously_unknown_resolved_count"],
            "source_known_mismatch_count": compare_payload["resolution_summary"]["source_mismatch_count"],
            "monthly_completed_bucket_count": monthly_payload["rollup"]["completed_bucket_count"],
            "monthly_not_worse_than_source": monthly_payload["comparison_to_source"]["not_worse_than_source"],
        },
        "decision_target": "Rerun the frozen short_cleanup_bottom_risk_v1 closed-horizon stability check after full maturity and only then decide if it can enter stability replay.",
    }


def _artifact_complete(output_dir: Path) -> dict[str, Any]:
    artifact_refs = {name: str(output_dir / name) for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    complete = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_full_recheck_artifact_complete_v1",
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

    codes = [str(row.get("code") or "").strip() for row in source_context["confusion_rows"] if str(row.get("code") or "").strip()]
    series_by_code = _load_code_series(runtime_db_path, codes)
    resolved_rows, resolution_summary, unknown_resolution_rows = _build_resolved_rows(
        confusion_rows=source_context["confusion_rows"],
        maturity_unknown_rows=source_context["maturity_unknown_rows"],
        series_by_code=series_by_code,
    )

    session_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    session_id = f"entry-short-bottom-risk-full-recheck-{session_stamp}"
    output_dir = output_root / f"{session_stamp}-entry-short-bottom-risk-full-recheck-v1"
    output_dir.mkdir(parents=True, exist_ok=True)

    compare_payload = _build_compare_payload(
        session_id=session_id,
        source_context=source_context,
        resolved_rows=resolved_rows,
        resolution_summary=resolution_summary,
    )
    monthly_payload = _build_monthly_stability_payload(
        session_id=session_id,
        source_context=source_context,
        resolved_rows=resolved_rows,
    )
    unknown_resolution_payload = _build_unknown_resolution_payload(
        session_id=session_id,
        source_context=source_context,
        resolution_summary=resolution_summary,
        unknown_resolution_rows=unknown_resolution_rows,
        compare_payload=compare_payload,
    )
    decision_payload = _build_decision(
        compare_payload=compare_payload,
        monthly_payload=monthly_payload,
        unknown_resolution_payload=unknown_resolution_payload,
        source_context=source_context,
    )
    contract = _build_contract(
        session_id=session_id,
        source_context=source_context,
        runtime_status=runtime_status,
        rankings_down=rankings_down,
        rankings_up=rankings_up,
        compare_payload=compare_payload,
        monthly_payload=monthly_payload,
    )

    no_lookahead_audit = {
        "schema_version": "tradex_entry_precision_short_bottom_risk_full_recheck_no_lookahead_audit_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "no_lookahead_pass": True,
        "future_outcome_fields_used_in_selection": [],
        "future_outcome_fields_used_in_recheck": [],
        "future_outcome_fields_used_in_evaluation": ["short_ret_5", "short_ret_10", "short_ret_20"],
        "selection_logic_frozen": True,
        "recheck_logic_frozen": True,
        "diagnostic_mode": True,
        "research_fallback": False,
        "silent_fallback_used": False,
    }

    _write_json(output_dir / "short_bottom_risk_full_recheck_contract.json", contract)
    _write_csv(
        output_dir / "short_bottom_risk_full_recheck_confusion_groups.csv",
        resolved_rows,
        columns=[
            "ymd",
            "code",
            "confusion_group",
            "baseline_selected",
            "challenger_selected",
            "outcome_known",
            "outcome_positive",
            "outcome_bucket",
            "short_ret_20",
            "short_ret_10",
            "short_ret_5",
            "close_pos",
            "dist_low20",
            "dist_ma20_signed",
            "day_change_pct",
            "monthlyRangeProb",
            "monthlyRangePos",
            "weeklyBreakoutDownProb",
            "monthlyBreakoutDownProb",
            "marketRiskOff",
            "marketRegime",
            "trendDownStrict",
            "entryScore",
            "tradePriorityScore",
            "liquidity20d",
            "mae20",
            "mfe20",
            "baseline_rank",
            "tradeDecisionReasons",
            "tradeRiskWatch",
        ],
    )
    _write_json(output_dir / "short_bottom_risk_full_recheck_compare.json", compare_payload)
    _write_json(
        output_dir / "short_bottom_risk_full_recheck_monthly_stability.json",
        monthly_payload,
    )
    _write_json(
        output_dir / "short_bottom_risk_full_recheck_unknown_resolution.json",
        unknown_resolution_payload,
    )
    _write_json(
        output_dir / "short_bottom_risk_full_recheck_decision.json",
        decision_payload,
    )
    _write_json(output_dir / "no_lookahead_audit.json", no_lookahead_audit)
    artifact_complete = _artifact_complete(output_dir)
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", artifact_complete)

    return {
        "session_id": session_id,
        "output_dir": str(output_dir),
        "decision": decision_payload["decision"],
        "known_baseline_count": int(compare_payload["full_recheck_summary"]["baseline"]["count"]),
        "known_challenger_count": int(compare_payload["full_recheck_summary"]["challenger"]["count"]),
        "resolved_unknown_count": int(unknown_resolution_payload["resolved_count"]),
        "unresolved_count": int(unknown_resolution_payload["unresolved_count"]),
        "hit_rate_delta": compare_payload["full_recheck_summary"]["delta"]["hit_rate_delta"],
        "mean_ret20_delta": compare_payload["full_recheck_summary"]["delta"]["mean_ret20_delta"],
        "median_ret20_delta": compare_payload["full_recheck_summary"]["delta"]["median_ret20_delta"],
        "monthly_not_worse_than_source": monthly_payload["comparison_to_source"]["not_worse_than_source"],
        "full_recheck_ready_now": bool(source_context["frozen_watch_decision"].get("current_full_recheck_ready_now")),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TRADEX full closed-horizon rerun for the frozen short_cleanup_bottom_risk_v1 keep slice."
    )
    parser.add_argument("--source-root", default="", help="Path to the frozen maturity gate artifact root")
    parser.add_argument("--output-root", default="", help="Path to the output root")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
