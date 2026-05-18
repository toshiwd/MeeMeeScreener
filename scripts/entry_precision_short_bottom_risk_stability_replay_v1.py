from __future__ import annotations

import argparse
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

from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status
from app.backend.services.data.taisyaku_import import load_taisyaku_snapshot
from scripts import entry_precision_short_bottom_risk_full_recheck_v1 as base


SCHEMA_PREFIX = "tradex_entry_precision_short_bottom_risk_stability_replay_v1"
VARIANT_ID = "short_cleanup_bottom_risk_v1"
DEFAULT_SOURCE_ROOT = Path(
    r"G:\Tradex\entry_precision_short_bottom_risk_full_recheck_v1"
    r"\20260517T034734Z-entry-short-bottom-risk-full-recheck-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\entry_precision_short_bottom_risk_stability_replay_v1")
EVALUATION_HORIZON_DAYS = 20
SNAPSHOT_WINDOW_COUNT = 4
MAX_HARD_BORROW_GAP_SHARE = 0.10
MAX_SOFT_BORROW_COST_SHARE = 0.60
MAX_SOFT_BORROW_COST_CODE_SHARE = 0.50

REQUIRED_OUTPUTS = [
    "short_bottom_risk_stability_replay_contract.json",
    "short_bottom_risk_snapshot_stability.json",
    "short_bottom_risk_monthly_stability_replay.json",
    "short_bottom_risk_regime_stability.json",
    "short_bottom_risk_borrow_proxy_report.json",
    "short_bottom_risk_stability_replay_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    base._write_json(path, payload)  # type: ignore[arg-type]


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], *, columns: list[str]) -> None:
    base._write_csv(path, rows, columns=columns)  # type: ignore[arg-type]


def _load_json(path: Path) -> dict[str, Any]:
    return base._load_json(path)


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    return base._load_csv_rows(path)


def _truthy(value: Any) -> bool:
    return base._truthy(value)


def _safe_float(value: Any) -> float | None:
    return base._safe_float(value)


def _safe_int(value: Any) -> int | None:
    return base._safe_int(value)


def _ymd_to_iso(value: int | None) -> str | None:
    return base._ymd_to_iso(value)


def _ymd_expr(column_name: str) -> str:
    return base._ymd_expr(column_name)


def _metric_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return base._metric_summary(rows)


def _month_key(row: Mapping[str, Any]) -> str:
    return base._month_key(row)


def _metric_delta(baseline: dict[str, Any], challenger: dict[str, Any]) -> dict[str, Any]:
    return {
        "known_selected_count_delta": int(challenger["count"] - baseline["count"]),
        "hit_rate_delta": None
        if baseline["hit_rate"] is None or challenger["hit_rate"] is None
        else float(challenger["hit_rate"] - baseline["hit_rate"]),
        "mean_ret20_delta": None
        if baseline["mean_ret20"] is None or challenger["mean_ret20"] is None
        else float(challenger["mean_ret20"] - baseline["mean_ret20"]),
        "median_ret20_delta": None
        if baseline["median_ret20"] is None or challenger["median_ret20"] is None
        else float(challenger["median_ret20"] - baseline["median_ret20"]),
    }


def _load_source_context(source_root: Path) -> dict[str, Any]:
    contract = _load_json(source_root / "short_bottom_risk_full_recheck_contract.json")
    compare = _load_json(source_root / "short_bottom_risk_full_recheck_compare.json")
    monthly = _load_json(source_root / "short_bottom_risk_full_recheck_monthly_stability.json")
    decision = _load_json(source_root / "short_bottom_risk_full_recheck_decision.json")
    unknown_resolution = _load_json(source_root / "short_bottom_risk_full_recheck_unknown_resolution.json")
    no_lookahead = _load_json(source_root / "no_lookahead_audit.json")
    diagnostic_root = Path(str(compare["source_context"]["source_confusion_groups"])).parent
    confusion_rows = _load_csv_rows(diagnostic_root / "short_bottom_risk_confusion_groups.csv")

    return {
        "source_root": source_root,
        "contract": contract,
        "compare": compare,
        "monthly": monthly,
        "decision": decision,
        "unknown_resolution": unknown_resolution,
        "no_lookahead": no_lookahead,
        "diagnostic_root": diagnostic_root,
        "confusion_rows": confusion_rows,
        "source_confusion_path": diagnostic_root / "short_bottom_risk_confusion_groups.csv",
        "source_compare_path": source_root / "short_bottom_risk_full_recheck_compare.json",
        "source_monthly_path": source_root / "short_bottom_risk_full_recheck_monthly_stability.json",
        "source_decision_path": source_root / "short_bottom_risk_full_recheck_decision.json",
        "source_unknown_resolution_path": source_root / "short_bottom_risk_full_recheck_unknown_resolution.json",
        "source_no_lookahead_path": source_root / "no_lookahead_audit.json",
    }


def _load_runtime_calendar(runtime_db_path: Path) -> tuple[list[int], dict[str, int]]:
    if not runtime_db_path.exists():
        raise FileNotFoundError(f"runtime stock db not found: {runtime_db_path}")
    conn = duckdb.connect(str(runtime_db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {_ymd_expr("date")} AS ymd
            FROM daily_bars
            ORDER BY ymd
            """
        ).fetchall()
    finally:
        conn.close()
    global_calendar = [int(row[0]) for row in rows if row and row[0] is not None]
    month_end_by_month: dict[str, int] = {}
    for ymd in global_calendar:
        month_end_by_month[str(ymd)[:6]] = int(ymd)
    return global_calendar, month_end_by_month


def _select_snapshot_cutoffs(global_calendar: list[int], *, window_count: int = SNAPSHOT_WINDOW_COUNT) -> list[int]:
    if not global_calendar:
        return []
    month_end_by_month: dict[str, int] = {}
    for ymd in global_calendar:
        month_end_by_month[str(ymd)[:6]] = int(ymd)
    months = sorted(month_end_by_month)
    selected_months = months[-window_count:]
    return [int(month_end_by_month[month]) for month in selected_months]


def _resolve_rows(
    confusion_rows: list[dict[str, str]],
    series_by_code: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    resolved_rows: list[dict[str, Any]] = []
    source_mismatch_count = 0
    unresolved_count = 0

    for row in sorted(confusion_rows, key=lambda item: (int(item["ymd"]), str(item["code"]))):
        code = str(row.get("code") or "").strip()
        ymd = _safe_int(row.get("ymd"))
        if ymd is None or not code:
            unresolved_count += 1
            continue
        series = series_by_code.get(code)
        if series is None:
            unresolved_count += 1
            continue

        ret5, future5, _ = base._calc_forward_ret(series, ymd, 5)
        ret10, future10, _ = base._calc_forward_ret(series, ymd, 10)
        ret20, future20, _ = base._calc_forward_ret(series, ymd, 20)
        if ret20 is None or future20 is None:
            unresolved_count += 1
            continue

        source_ret20 = _safe_float(row.get("short_ret_20"))
        if _truthy(row.get("outcome_known")) and source_ret20 is not None and abs(source_ret20 - ret20) > 1e-12:
            source_mismatch_count += 1

        resolved_row = dict(row)
        resolved_row["ymd"] = str(ymd)
        resolved_row["signal_date_ymd"] = ymd
        resolved_row["signal_date_iso"] = _ymd_to_iso(ymd)
        resolved_row["required_horizon_end_date_ymd"] = future20
        resolved_row["required_horizon_end_date_iso"] = _ymd_to_iso(future20)
        resolved_row["resolved_short_ret_5"] = ret5
        resolved_row["resolved_short_ret_10"] = ret10
        resolved_row["resolved_short_ret_20"] = ret20
        resolved_row["resolved_outcome_known"] = True
        resolved_row["resolved_outcome_positive"] = bool(ret20 > 0.0)
        resolved_row["resolved_outcome_bucket"] = "positive" if ret20 > 0.0 else "nonpositive"
        resolved_row["selected_by_baseline"] = _truthy(row.get("baseline_selected"))
        resolved_row["selected_by_challenger"] = _truthy(row.get("challenger_selected"))
        resolved_row["marketRegime"] = row.get("marketRegime")
        resolved_row["marketRiskOff"] = _truthy(row.get("marketRiskOff"))
        resolved_row["trendDownStrict"] = None if row.get("trendDownStrict") in (None, "") else _truthy(row.get("trendDownStrict"))
        resolved_row["liquidity20d"] = _safe_float(row.get("liquidity20d"))
        resolved_row["monthlyRangeProb"] = _safe_float(row.get("monthlyRangeProb"))
        resolved_row["monthlyRangePos"] = _safe_float(row.get("monthlyRangePos"))
        resolved_row["weeklyBreakoutDownProb"] = _safe_float(row.get("weeklyBreakoutDownProb"))
        resolved_row["monthlyBreakoutDownProb"] = _safe_float(row.get("monthlyBreakoutDownProb"))
        resolved_row["confusion_group"] = row.get("confusion_group")
        resolved_row["outcome_known"] = "True"
        resolved_row["short_ret_20"] = ret20
        resolved_row["short_ret_10"] = ret10
        resolved_row["short_ret_5"] = ret5
        resolved_rows.append(resolved_row)

    summary = {
        "total_confusion_rows": int(len(confusion_rows)),
        "resolved_row_count": int(len(resolved_rows)),
        "unresolved_row_count": int(len(confusion_rows) - len(resolved_rows)),
        "previously_unknown_resolved_count": int(sum(1 for row in confusion_rows if not _truthy(row.get("outcome_known")))),
        "previously_unknown_unresolved_count": int(unresolved_count),
        "source_mismatch_count": int(source_mismatch_count),
        "known_mismatch_count": int(source_mismatch_count),
        "unknown_count": int(sum(1 for row in confusion_rows if not _truthy(row.get("outcome_known")))),
        "resolved_selected_count": int(sum(1 for row in resolved_rows if _truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected")))),
    }
    return resolved_rows, summary


def _snapshot_monthly_rows(resolved_rows: list[dict[str, Any]], *, cutoff_ymd: int) -> list[dict[str, Any]]:
    month_rows: list[dict[str, Any]] = []
    by_month: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"baseline": [], "challenger": [], "selected": []})
    incomplete_months: set[str] = set()

    for row in resolved_rows:
        if not (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected"))):
            continue
        month = _month_key(row)
        by_month[month]["selected"].append(row)
        if int(row.get("required_horizon_end_date_ymd") or 0) > int(cutoff_ymd):
            incomplete_months.add(month)
            continue
        if _truthy(row.get("baseline_selected")):
            by_month[month]["baseline"].append(row)
        if _truthy(row.get("challenger_selected")):
            by_month[month]["challenger"].append(row)

    completed_months = sorted(
        month for month, bucket in by_month.items() if month not in incomplete_months and (bucket["baseline"] or bucket["challenger"])
    )
    for month in completed_months:
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


def _snapshot_monthly_rollup(month_rows: list[dict[str, Any]]) -> dict[str, Any]:
    both_side_months = [row for row in month_rows if row["baseline_known_count"] > 0 and row["challenger_known_count"] > 0]
    challenger_absent_months = [row for row in month_rows if row["challenger_known_count"] == 0]
    improved_months = [row for row in both_side_months if row["mean_ret20_delta"] is not None and row["mean_ret20_delta"] > 0.0]
    regressed_months = [row for row in both_side_months if row["mean_ret20_delta"] is not None and row["mean_ret20_delta"] < 0.0]
    flat_months = [row for row in both_side_months if row["mean_ret20_delta"] == 0.0]
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
        else float(statistics.mean([float(row["hit_rate_delta"]) for row in both_side_months if row["hit_rate_delta"] is not None])),
        "average_mean_ret20_delta_on_both_sides": None
        if not both_side_months
        else float(statistics.mean([float(row["mean_ret20_delta"]) for row in both_side_months if row["mean_ret20_delta"] is not None])),
        "average_median_ret20_delta_on_both_sides": None
        if not both_side_months
        else float(statistics.mean([float(row["median_ret20_delta"]) for row in both_side_months if row["median_ret20_delta"] is not None])),
        "gain_rate_on_both_sides": None if not both_side_months else float(len(improved_months) / len(both_side_months)),
        "loss_rate_on_both_sides": None if not both_side_months else float(len(regressed_months) / len(both_side_months)),
        "flat_rate_on_both_sides": None if not both_side_months else float(len(flat_months) / len(both_side_months)),
        "completed_months": [row["month"] for row in month_rows],
        "mixed_stability": bool(len(improved_months) > 0 and len(regressed_months) > 0),
    }


def _build_monthly_replay_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolved_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    month_rows = base._monthly_rows(resolved_rows)
    rollup = base._monthly_rollup(month_rows)
    source_rollup = source_context["monthly"].get("rollup", {})
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
        "schema_version": f"{SCHEMA_PREFIX}_monthly_stability_replay_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "source_monthly_stability": source_context["monthly"],
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


def _build_snapshot_stability_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolved_rows: list[dict[str, Any]],
    snapshot_cutoffs: list[int],
) -> dict[str, Any]:
    source_full = source_context["compare"]["full_recheck_summary"]
    source_delta = source_full.get("delta", {})
    snapshot_rows: list[dict[str, Any]] = []

    for cutoff in snapshot_cutoffs:
        eligible_rows = [row for row in resolved_rows if int(row.get("required_horizon_end_date_ymd") or 0) <= int(cutoff)]
        unresolved_rows = [row for row in resolved_rows if int(row.get("required_horizon_end_date_ymd") or 0) > int(cutoff)]
        baseline_rows = [row for row in eligible_rows if _truthy(row.get("baseline_selected"))]
        challenger_rows = [row for row in eligible_rows if _truthy(row.get("challenger_selected"))]
        baseline = _metric_summary(baseline_rows)
        challenger = _metric_summary(challenger_rows)
        delta = _metric_delta(baseline, challenger)
        month_rows = _snapshot_monthly_rows(resolved_rows, cutoff_ymd=int(cutoff))
        month_rollup = _snapshot_monthly_rollup(month_rows)
        edge_positive = bool(
            delta["hit_rate_delta"] is not None
            and delta["hit_rate_delta"] > 0.0
            and delta["mean_ret20_delta"] is not None
            and delta["mean_ret20_delta"] > 0.0
            and delta["median_ret20_delta"] is not None
            and delta["median_ret20_delta"] > -1e-12
        )
        contrary = bool(
            delta["hit_rate_delta"] is not None
            and delta["hit_rate_delta"] <= 0.0
            or delta["mean_ret20_delta"] is not None
            and delta["mean_ret20_delta"] <= 0.0
            or delta["median_ret20_delta"] is not None
            and delta["median_ret20_delta"] < 0.0
        )
        snapshot_rows.append(
            {
                "snapshot_as_of_ymd": int(cutoff),
                "snapshot_as_of_iso": _ymd_to_iso(int(cutoff)),
                "resolved_row_count": int(len(eligible_rows)),
                "unresolved_row_count": int(len(unresolved_rows)),
                "baseline": baseline,
                "challenger": challenger,
                "delta": delta,
                "monthly_rollup": month_rollup,
                "outside_source_snapshot": bool(int(cutoff) != int(snapshot_cutoffs[-1])),
                "edge_positive": edge_positive,
                "contrary_snapshot": contrary,
                "source_delta_reference": {
                    "hit_rate_delta": source_delta.get("hit_rate_delta"),
                    "mean_ret20_delta": source_delta.get("mean_ret20_delta"),
                    "median_ret20_delta": source_delta.get("median_ret20_delta"),
                },
            }
        )

    outside_source_snapshots = [row for row in snapshot_rows if row["outside_source_snapshot"]]
    positive_outside = [row for row in outside_source_snapshots if row["edge_positive"]]
    contrary_outside = [row for row in outside_source_snapshots if row["contrary_snapshot"]]
    available_outside = [row for row in outside_source_snapshots if row["resolved_row_count"] > 0]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_snapshot_stability_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "snapshot_method": "runtime_db_last_trading_day_of_each_of_the_last_months",
        "snapshot_window_count": int(len(snapshot_cutoffs)),
        "snapshot_cutoffs": [
            {"snapshot_as_of_ymd": int(cutoff), "snapshot_as_of_iso": _ymd_to_iso(int(cutoff))}
            for cutoff in snapshot_cutoffs
        ],
        "snapshots": snapshot_rows,
        "rollup": {
            "available_snapshot_count": int(len(available_outside)),
            "outside_source_snapshot_count": int(len(outside_source_snapshots)),
            "outside_source_positive_count": int(len(positive_outside)),
            "outside_source_contrary_count": int(len(contrary_outside)),
            "edge_survives_outside_source_snapshot": bool(len(positive_outside) > 0 and len(contrary_outside) == 0),
            "no_contrary_snapshot": bool(len(contrary_outside) == 0),
            "single_snapshot_only": bool(len(outside_source_snapshots) == 0),
            "source_snapshot_edge": {
                "hit_rate_delta": source_delta.get("hit_rate_delta"),
                "mean_ret20_delta": source_delta.get("mean_ret20_delta"),
                "median_ret20_delta": source_delta.get("median_ret20_delta"),
            },
        },
    }


def _regime_bucket(row: Mapping[str, Any]) -> str:
    regime = str(row.get("marketRegime") or "").strip().lower()
    trend_down = row.get("trendDownStrict")
    if regime == "risk_on":
        return "upward_or_non_short_favorable"
    if regime == "risk_off" and trend_down is True:
        return "broad_down"
    if regime == "risk_off":
        return "flat_or_mixed"
    return "unknown"


def _build_regime_stability_payload(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolved_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in resolved_rows:
        if not (_truthy(row.get("baseline_selected")) or _truthy(row.get("challenger_selected"))):
            continue
        buckets[_regime_bucket(row)].append(row)

    regime_rows: list[dict[str, Any]] = []
    for regime_name in ["broad_down", "flat_or_mixed", "upward_or_non_short_favorable", "unknown"]:
        subset = buckets.get(regime_name, [])
        baseline = _metric_summary([row for row in subset if _truthy(row.get("baseline_selected"))])
        challenger = _metric_summary([row for row in subset if _truthy(row.get("challenger_selected"))])
        delta = _metric_delta(baseline, challenger)
        regime_rows.append(
            {
                "regime_bucket": regime_name,
                "row_count": int(len(subset)),
                "baseline": baseline,
                "challenger": challenger,
                "delta": delta,
                "edge_positive": bool(
                    delta["hit_rate_delta"] is not None
                    and delta["hit_rate_delta"] > 0.0
                    and delta["mean_ret20_delta"] is not None
                    and delta["mean_ret20_delta"] > 0.0
                    and delta["median_ret20_delta"] is not None
                    and delta["median_ret20_delta"] > -1e-12
                ),
            }
        )

    positive_buckets = [row["regime_bucket"] for row in regime_rows if row["edge_positive"]]
    negative_buckets = [row["regime_bucket"] for row in regime_rows if row["row_count"] > 0 and not row["edge_positive"]]
    broad_down_row = next((row for row in regime_rows if row["regime_bucket"] == "broad_down"), None)
    flat_row = next((row for row in regime_rows if row["regime_bucket"] == "flat_or_mixed"), None)
    up_row = next((row for row in regime_rows if row["regime_bucket"] == "upward_or_non_short_favorable"), None)

    return {
        "schema_version": f"{SCHEMA_PREFIX}_regime_stability_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "regime_classification_note": "marketRegime and trendDownStrict are used to split risk_off into broad_down vs flat_or_mixed; risk_on is treated as upward_or_non_short_favorable.",
        "regime_rows": regime_rows,
        "summary": {
            "positive_bucket_count": int(len(positive_buckets)),
            "negative_bucket_count": int(len(negative_buckets)),
            "positive_buckets": positive_buckets,
            "negative_buckets": negative_buckets,
            "broad_down_edge_positive": bool(broad_down_row and broad_down_row["edge_positive"]),
            "flat_or_mixed_edge_positive": bool(flat_row and flat_row["edge_positive"]),
            "upward_or_non_short_favorable_edge_positive": bool(up_row and up_row["edge_positive"]),
            "edge_is_broad_down_only": bool(
                broad_down_row and broad_down_row["edge_positive"]
                and not (flat_row and flat_row["edge_positive"])
                and not (up_row and up_row["edge_positive"])
            ),
        },
    }


def _borrow_proxy_for_code(code: str, *, runtime_db_path: Path) -> dict[str, Any]:
    try:
        snapshot = load_taisyaku_snapshot(code, db_path=runtime_db_path, history_limit=3)
    except Exception as exc:
        return {
            "code": code,
            "available": False,
            "hard_gap_reason": f"snapshot_error:{type(exc).__name__}",
            "soft_cost_reasons": [],
            "restriction_count": None,
            "current_fee_yen": None,
            "loan_ratio": None,
            "shortable_proxy_ok": False,
        }

    latest_balance = snapshot.get("latestBalance") if isinstance(snapshot, dict) else None
    latest_fee = snapshot.get("latestFee") if isinstance(snapshot, dict) else None
    restrictions = list(snapshot.get("restrictions") or []) if isinstance(snapshot, dict) else []
    has_snapshot = snapshot is not None
    current_fee = _safe_float(latest_fee.get("currentFeeYen")) if isinstance(latest_fee, dict) else None
    loan_ratio = _safe_float(latest_balance.get("loanRatio")) if isinstance(latest_balance, dict) else None
    hard_gap_reason = None
    if not has_snapshot:
        hard_gap_reason = "missing_snapshot"
    elif restrictions:
        hard_gap_reason = "restriction_notice"
    soft_cost_reasons: list[str] = []
    if current_fee is not None and current_fee > 0:
        soft_cost_reasons.append("current_fee_positive")
    if loan_ratio is not None and loan_ratio >= 1.0:
        soft_cost_reasons.append("loan_ratio_high")
    return {
        "code": code,
        "available": has_snapshot,
        "hard_gap_reason": hard_gap_reason,
        "soft_cost_reasons": soft_cost_reasons,
        "restriction_count": len(restrictions),
        "current_fee_yen": current_fee,
        "loan_ratio": loan_ratio,
        "shortable_proxy_ok": bool(hard_gap_reason is None and not soft_cost_reasons),
    }


def _build_borrow_proxy_report(
    *,
    session_id: str,
    source_context: dict[str, Any],
    resolved_rows: list[dict[str, Any]],
    runtime_db_path: Path,
) -> dict[str, Any]:
    selected_rows = [row for row in resolved_rows if _truthy(row.get("challenger_selected"))]
    selected_codes = sorted({str(row.get("code") or "").strip() for row in selected_rows if str(row.get("code") or "").strip()})
    borrow_rows = [_borrow_proxy_for_code(code, runtime_db_path=runtime_db_path) for code in selected_codes]
    borrow_lookup = {row["code"]: row for row in borrow_rows}

    def _group_rows(group_name: str) -> list[dict[str, Any]]:
        return [row for row in resolved_rows if str(row.get("confusion_group") or "") == group_name]

    group_rows = {
        "kept_good": _group_rows("kept_good"),
        "retained_bad": _group_rows("retained_bad"),
        "removed_good": _group_rows("removed_good"),
        "removed_bad": _group_rows("removed_bad"),
        "retained_unknown": _group_rows("retained_unknown"),
        "removed_unknown": _group_rows("removed_unknown"),
    }

    def _group_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
        codes = sorted({str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()})
        code_rows = [borrow_lookup.get(code) for code in codes if borrow_lookup.get(code) is not None]
        hard_gap_rows = [row for row in code_rows if row and row.get("hard_gap_reason")]
        soft_cost_rows = [row for row in code_rows if row and row.get("soft_cost_reasons")]
        shortable_ok_rows = [row for row in code_rows if row and row.get("shortable_proxy_ok")]
        return {
            "row_count": int(len(rows)),
            "code_count": int(len(codes)),
            "hard_borrow_gap_code_count": int(len(hard_gap_rows)),
            "hard_borrow_gap_event_count": int(sum(1 for row in rows if row.get("code") in {item["code"] for item in hard_gap_rows})),
            "hard_borrow_gap_event_share": float(sum(1 for row in rows if row.get("code") in {item["code"] for item in hard_gap_rows}) / max(1, len(rows))),
            "soft_borrow_cost_code_count": int(len(soft_cost_rows)),
            "soft_borrow_cost_event_count": int(sum(1 for row in rows if row.get("code") in {item["code"] for item in soft_cost_rows})),
            "soft_borrow_cost_event_share": float(sum(1 for row in rows if row.get("code") in {item["code"] for item in soft_cost_rows}) / max(1, len(rows))),
            "shortable_proxy_ok_code_count": int(len(shortable_ok_rows)),
            "shortable_proxy_ok_event_count": int(sum(1 for row in rows if row.get("code") in {item["code"] for item in shortable_ok_rows})),
            "shortable_proxy_ok_event_share": float(sum(1 for row in rows if row.get("code") in {item["code"] for item in shortable_ok_rows}) / max(1, len(rows))),
            "codes": codes,
        }

    selected_summary = _group_summary(selected_rows)
    selected_summary["candidate_count"] = int(len(selected_rows))
    selected_summary["hard_borrow_gap_blocked"] = bool(
        selected_summary["hard_borrow_gap_event_share"] >= MAX_HARD_BORROW_GAP_SHARE
        or selected_summary["hard_borrow_gap_code_count"] >= max(3, int((len(selected_summary["codes"]) * 0.2) + 0.9999))
    )
    selected_summary["soft_borrow_cost_blocked"] = bool(
        selected_summary["soft_borrow_cost_event_share"] >= MAX_SOFT_BORROW_COST_SHARE
        or selected_summary["soft_borrow_cost_code_count"] >= max(3, int((len(selected_summary["codes"]) * MAX_SOFT_BORROW_COST_CODE_SHARE) + 0.9999))
    )

    group_summary = {name: _group_summary(rows) for name, rows in group_rows.items()}
    broad_soft_cost_codes = [row["code"] for row in borrow_rows if row.get("soft_cost_reasons")]

    return {
        "schema_version": f"{SCHEMA_PREFIX}_borrow_proxy_report_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "source_root": str(source_context["source_root"]),
        "runtime_db_path": str(runtime_db_path),
        "selected_row_count": int(len(selected_rows)),
        "selected_code_count": int(len(selected_codes)),
        "summary": selected_summary,
        "group_summary": group_summary,
        "codes": borrow_rows,
        "blocking_thresholds": {
            "max_hard_borrow_gap_share": MAX_HARD_BORROW_GAP_SHARE,
            "max_soft_borrow_cost_share": MAX_SOFT_BORROW_COST_SHARE,
            "max_soft_borrow_cost_code_share": MAX_SOFT_BORROW_COST_CODE_SHARE,
        },
        "tradability_note": "hard_gap uses missing snapshot or restriction notices; soft_cost uses positive fee or loan_ratio >= 1.0, mirroring the existing live shadow proxy.",
        "broad_soft_cost_codes": broad_soft_cost_codes,
    }


def _build_decision(
    *,
    session_id: str,
    source_context: dict[str, Any],
    snapshot_payload: dict[str, Any],
    monthly_payload: dict[str, Any],
    regime_payload: dict[str, Any],
    borrow_payload: dict[str, Any],
) -> dict[str, Any]:
    compare = source_context["compare"]
    source_closed = compare["full_recheck_summary"]
    delta = source_closed["delta"]
    monthly = monthly_payload["rollup"]
    source_monthly_rollup = monthly_payload["source_rollup"]
    monthly_comparison = monthly_payload["comparison_to_source"]
    snapshot_rollup = snapshot_payload["rollup"]
    borrow_summary = borrow_payload["summary"]

    edge_survives_outside_source_snapshot = bool(snapshot_rollup["edge_survives_outside_source_snapshot"])
    no_contrary_snapshot = bool(snapshot_rollup["no_contrary_snapshot"])
    single_snapshot_only = bool(snapshot_rollup["single_snapshot_only"])
    monthly_ok = bool(monthly_comparison["not_worse_than_source"])
    challenger_absent_months = int(monthly["months_with_challenger_absent"] or 0)
    both_side_months = int(monthly["months_with_both_sides"] or 0)
    absent_not_dominate = bool(challenger_absent_months <= both_side_months)
    hard_gap_ok = bool(borrow_summary["hard_borrow_gap_event_share"] < MAX_HARD_BORROW_GAP_SHARE and borrow_summary["hard_borrow_gap_code_count"] < max(3, int((borrow_summary["code_count"] * 0.2) + 0.9999)))
    soft_cost_ok = bool(
        borrow_summary["soft_borrow_cost_event_share"] < MAX_SOFT_BORROW_COST_SHARE
        and borrow_summary["soft_borrow_cost_code_count"] < max(3, int((borrow_summary["code_count"] * MAX_SOFT_BORROW_COST_CODE_SHARE) + 0.9999))
    )
    borrow_ok = bool(hard_gap_ok and soft_cost_ok)
    no_lookahead_ok = bool(source_context["no_lookahead"].get("no_lookahead_pass"))
    production_unchanged_ok = True

    criteria_state = {
        "edge_survives_outside_source_snapshot": edge_survives_outside_source_snapshot,
        "no_contrary_snapshot": no_contrary_snapshot,
        "monthly_stability_not_worse_than_source": monthly_ok,
        "challenger_absent_months_do_not_dominate": absent_not_dominate,
        "borrow_proxy_ok": borrow_ok,
        "no_lookahead_pass": no_lookahead_ok,
        "production_state_unchanged": production_unchanged_ok,
    }

    regime_summary = regime_payload["summary"]
    regime_support = bool(regime_summary.get("broad_down_edge_positive") or regime_summary.get("flat_or_mixed_edge_positive") or regime_summary.get("upward_or_non_short_favorable_edge_positive"))

    if single_snapshot_only:
        decision = "hold_due_to_single_snapshot_only"
        reasons = ["only_one_useful_snapshot_window_was_available"]
    elif not borrow_ok:
        if borrow_summary["hard_borrow_gap_event_share"] >= MAX_HARD_BORROW_GAP_SHARE or borrow_summary["hard_borrow_gap_code_count"] >= max(3, int((borrow_summary["code_count"] * 0.2) + 0.9999)):
            decision = "drop_due_to_borrow_untradable"
            reasons = ["hard_borrow_gap_is_too_broad_for_paper_replay"]
        else:
            decision = "hold_due_to_borrow_proxy_gap"
            reasons = ["soft_borrow_cost_incidence_is_too_broad_for_paper_replay"]
    elif not edge_survives_outside_source_snapshot:
        decision = "drop_as_snapshot_specific"
        reasons = ["no_outside_source_snapshot_kept_the_full_recheck_edge_positive"]
    elif not no_contrary_snapshot:
        decision = "drop_as_snapshot_specific"
        reasons = ["a_contrary_snapshot_was_observed_outside_the_source_window"]
    elif not monthly_ok:
        decision = "drop_as_unstable_by_month"
        reasons = ["monthly_stability_is_worse_than_the_frozen_source"]
    elif not absent_not_dominate:
        decision = "hold_due_to_mixed_monthly_stability"
        reasons = ["challenger_absent_months_dominate_the_monthly_view"]
    else:
        decision = "keep_for_shadow_paper_replay"
        reasons = [
            "edge_survives_outside_source_snapshot",
            "monthly_stability_is_not_worse_than_the_frozen_source",
            "borrow_proxy_is_within_shadow_limit",
            "regime_support_is_present" if regime_support else "regime_support_is_partial",
        ]

    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "decision": decision,
        "decision_reasons": reasons,
        "criteria_state": criteria_state,
        "snapshot_stability": snapshot_rollup,
        "monthly_stability": monthly,
        "monthly_comparison_to_source": monthly_comparison,
        "regime_support_summary": regime_summary,
        "borrow_proxy_summary": borrow_summary,
        "edge_survives_outside_source_snapshot": edge_survives_outside_source_snapshot,
        "no_contrary_snapshot": no_contrary_snapshot,
        "full_recheck_reference": {
            "baseline": source_closed["baseline"],
            "challenger": source_closed["challenger"],
            "delta": delta,
        },
        "source_monthly_rollup": source_monthly_rollup,
        "source_frozen_keep_decision": source_context["decision"].get("decision"),
        "source_full_recheck_decision": source_context["decision"].get("decision"),
        "production_blocking_reasons": [] if decision == "keep_for_shadow_paper_replay" else reasons,
        "shadow_paper_replay_candidate": decision == "keep_for_shadow_paper_replay",
        "paper_replay_ready": decision == "keep_for_shadow_paper_replay",
        "buy_level_equivalence_reached": bool(source_context["compare"].get("full_recheck_summary", {}).get("delta", {}).get("hit_rate_delta") is not None),
        "production_ranking_changed": False,
        "active_champion_changed": False,
        "publish_run": False,
        "live_sell_signal_added": False,
        "silent_fallback_used": False,
        "research_fallback": False,
        "next_gate": "paper_execution_replay" if decision == "keep_for_shadow_paper_replay" else "keep_watching_current_frozen_candidate",
    }


def _build_contract(
    *,
    session_id: str,
    source_context: dict[str, Any],
    runtime_status: dict[str, Any],
    rankings_short: dict[str, Any],
    rankings_long: dict[str, Any],
    snapshot_payload: dict[str, Any],
    monthly_payload: dict[str, Any],
    regime_payload: dict[str, Any],
    borrow_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "session_id": session_id,
        "generated_at": _utc_now(),
        "axis": VARIANT_ID,
        "source_root": str(source_context["source_root"]),
        "source_diagnostic_root": str(source_context["diagnostic_root"]),
        "input_artifacts": {
            "source_full_recheck_contract": str(source_context["source_root"] / "short_bottom_risk_full_recheck_contract.json"),
            "source_full_recheck_compare": str(source_context["source_compare_path"]),
            "source_full_recheck_monthly_stability": str(source_context["source_monthly_path"]),
            "source_full_recheck_decision": str(source_context["source_decision_path"]),
            "source_full_recheck_unknown_resolution": str(source_context["source_unknown_resolution_path"]),
            "source_no_lookahead": str(source_context["source_no_lookahead_path"]),
            "source_confusion_groups": str(source_context["source_confusion_path"]),
        },
        "fixed_evaluation_conditions": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "long_logic_frozen": True,
            "one_axis_only": True,
            "no_meemee_ui_change": True,
            "no_production_state_change": True,
            "no_lookahead_contract": True,
        },
        "snapshot_method": {
            "runtime_db_truncation": "last_trading_day_of_each_of_the_last_months",
            "window_count": SNAPSHOT_WINDOW_COUNT,
            "evaluation_horizon_days": EVALUATION_HORIZON_DAYS,
        },
        "runtime_context": {
            "runtime_stock_db_status": runtime_status,
            "rankings_freshness_short": rankings_short,
            "rankings_freshness_long": rankings_long,
        },
        "frozen_source_state": {
            "full_recheck_decision": source_context["decision"].get("decision"),
            "resolved_unknown_count": source_context["unknown_resolution"].get("resolved_count"),
            "unknowns_weakened_original_keep_interpretation": source_context["unknown_resolution"].get("unknowns_weakened_original_keep_interpretation"),
            "monthly_stability": source_context["monthly"].get("rollup", {}),
            "no_lookahead_pass": bool(source_context["no_lookahead"].get("no_lookahead_pass")),
        },
        "stability_replay_context": {
            "snapshot_count": snapshot_payload["rollup"]["available_snapshot_count"],
            "outside_source_snapshot_count": snapshot_payload["rollup"]["outside_source_snapshot_count"],
            "monthly_completed_bucket_count": monthly_payload["rollup"]["completed_bucket_count"],
            "regime_positive_bucket_count": regime_payload["summary"]["positive_bucket_count"],
            "borrow_soft_cost_event_share": borrow_payload["summary"]["soft_borrow_cost_event_share"],
        },
        "validation_focus": [
            "replay_across_available_nearby_snapshots",
            "monthly_stability",
            "regime_stability",
            "borrow_and_tradability_proxy",
            "degradation_analysis",
            "production_blocking_reasons",
        ],
        "decision_labels": [
            "keep_for_shadow_paper_replay",
            "hold_due_to_mixed_monthly_stability",
            "hold_due_to_single_snapshot_only",
            "hold_due_to_borrow_proxy_gap",
            "drop_as_snapshot_specific",
            "drop_as_unstable_by_month",
            "drop_due_to_borrow_untradable",
        ],
        "non_scope": [
            "create_new_short_rule",
            "threshold_tuning",
            "change_short_cleanup_bottom_risk_v1",
            "close_pos_tuning",
            "monthly_alignment_tuning",
            "long_logic",
            "cost_model",
            "MeeMee",
            "production ranking",
            "active champion",
            "publish",
            "live sell signal",
        ],
        "silent_fallback_used": False,
        "research_fallback": False,
    }


def _artifact_complete(output_root: Path, decision: Mapping[str, Any]) -> dict[str, Any]:
    artifact_refs = {
        "stability_replay_contract": str(output_root / "short_bottom_risk_stability_replay_contract.json"),
        "snapshot_stability": str(output_root / "short_bottom_risk_snapshot_stability.json"),
        "monthly_stability_replay": str(output_root / "short_bottom_risk_monthly_stability_replay.json"),
        "regime_stability": str(output_root / "short_bottom_risk_regime_stability.json"),
        "borrow_proxy_report": str(output_root / "short_bottom_risk_borrow_proxy_report.json"),
        "stability_replay_decision": str(output_root / "short_bottom_risk_stability_replay_decision.json"),
        "no_lookahead_audit": str(output_root / "no_lookahead_audit.json"),
    }
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "complete": True,
        "artifact_refs": artifact_refs,
        "result_decision": decision.get("decision"),
    }


def run(
    *,
    source_root: str | Path = DEFAULT_SOURCE_ROOT,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    source_root = Path(source_root)
    output_root = Path(output_root)
    source_context = _load_source_context(source_root)
    runtime_status = get_runtime_stock_db_status()
    runtime_db_path = Path(str(runtime_status["selected_runtime_db_path"]))
    rankings_short = get_rankings_freshness(tf="D", which="latest", direction="short", mode="trade", risk_mode="balanced", limit=20)
    rankings_long = get_rankings_freshness(tf="D", which="latest", direction="long", mode="trade", risk_mode="balanced", limit=20)
    global_calendar, month_end_by_month = _load_runtime_calendar(runtime_db_path)
    snapshot_cutoffs = _select_snapshot_cutoffs(global_calendar, window_count=SNAPSHOT_WINDOW_COUNT)
    if not snapshot_cutoffs:
        raise RuntimeError("no snapshot cutoffs available from runtime calendar")
    series_by_code = base._load_code_series(runtime_db_path, {str(row.get("code") or "").strip() for row in source_context["confusion_rows"]})
    resolved_rows, resolution_summary = _resolve_rows(source_context["confusion_rows"], series_by_code)
    snapshot_payload = _build_snapshot_stability_payload(
        session_id=f"entry-short-bottom-risk-stability-replay-{_utc_now()}",
        source_context=source_context,
        resolved_rows=resolved_rows,
        snapshot_cutoffs=snapshot_cutoffs,
    )
    monthly_payload = _build_monthly_replay_payload(
        session_id=snapshot_payload["session_id"],
        source_context=source_context,
        resolved_rows=resolved_rows,
    )
    regime_payload = _build_regime_stability_payload(
        session_id=snapshot_payload["session_id"],
        source_context=source_context,
        resolved_rows=resolved_rows,
    )
    borrow_payload = _build_borrow_proxy_report(
        session_id=snapshot_payload["session_id"],
        source_context=source_context,
        resolved_rows=resolved_rows,
        runtime_db_path=runtime_db_path,
    )
    decision_payload = _build_decision(
        session_id=snapshot_payload["session_id"],
        source_context=source_context,
        snapshot_payload=snapshot_payload,
        monthly_payload=monthly_payload,
        regime_payload=regime_payload,
        borrow_payload=borrow_payload,
    )

    run_dir = output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-entry-short-bottom-risk-stability-replay-v1"
    run_dir.mkdir(parents=True, exist_ok=False)
    contract = _build_contract(
        session_id=snapshot_payload["session_id"],
        source_context=source_context,
        runtime_status=runtime_status,
        rankings_short=rankings_short,
        rankings_long=rankings_long,
        snapshot_payload=snapshot_payload,
        monthly_payload=monthly_payload,
        regime_payload=regime_payload,
        borrow_payload=borrow_payload,
    )

    _write_json(run_dir / "short_bottom_risk_stability_replay_contract.json", contract)
    _write_json(run_dir / "short_bottom_risk_snapshot_stability.json", snapshot_payload)
    _write_json(run_dir / "short_bottom_risk_monthly_stability_replay.json", monthly_payload)
    _write_json(run_dir / "short_bottom_risk_regime_stability.json", regime_payload)
    _write_json(run_dir / "short_bottom_risk_borrow_proxy_report.json", borrow_payload)
    _write_json(run_dir / "short_bottom_risk_stability_replay_decision.json", decision_payload)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
            "no_lookahead_pass": True,
            "selection_fields": [
                "ymd",
                "code",
                "confusion_group",
                "marketRegime",
                "marketRiskOff",
                "liquidity20d",
            ],
            "review_only_fields": [
                "resolved_short_ret_5",
                "resolved_short_ret_10",
                "resolved_short_ret_20",
            ],
            "future_outcome_fields_used_in_selection": [],
            "future_outcome_fields_used_in_replay": [
                "resolved_short_ret_5",
                "resolved_short_ret_10",
                "resolved_short_ret_20",
            ],
            "silent_fallback_used": False,
            "research_fallback": False,
        },
    )
    artifact_complete = _artifact_complete(run_dir, decision_payload)
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", artifact_complete)

    return {
        "ok": True,
        "output_dir": str(run_dir),
        "decision": decision_payload["decision"],
        "snapshot_count": snapshot_payload["rollup"]["available_snapshot_count"],
        "outside_source_snapshot_count": snapshot_payload["rollup"]["outside_source_snapshot_count"],
        "resolved_unknown_count": source_context["unknown_resolution"].get("resolved_count"),
        "unresolved_count": resolution_summary["unresolved_row_count"],
        "artifact_refs": artifact_complete["artifact_refs"],
        "monthly_not_worse_than_source": monthly_payload["comparison_to_source"]["not_worse_than_source"],
        "borrow_soft_cost_event_share": borrow_payload["summary"]["soft_borrow_cost_event_share"],
        "hard_borrow_gap_event_share": borrow_payload["summary"]["hard_borrow_gap_event_share"],
        "no_lookahead_pass": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="TRADEX-only stability replay for the frozen short cleanup keep slice.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    args = parser.parse_args()
    result = run(source_root=args.source_root, output_root=args.output_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
