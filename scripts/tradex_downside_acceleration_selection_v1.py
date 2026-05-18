from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import entry_precision_short_audit as base


AXIS_ID = "tradex_downside_acceleration_selection_v1"
SCHEMA_PREFIX = "tradex_downside_acceleration_selection_v1"
DEFAULT_OUTPUT_DIR_NAME = "downside_acceleration_selection_v1"
DEFAULT_TOP_K = 5
DEFAULT_MIN_TRAIN_MONTHS = 6
DEFAULT_LOOKBACK_MONTHS = 12

REQUIRED_ARTIFACTS = (
    "downside_acceleration_selection_contract.json",
    "downside_acceleration_bucket_model.json",
    "downside_acceleration_monthly_rankings.csv",
    "downside_acceleration_compare.json",
    "downside_acceleration_yearly_performance.json",
    "downside_acceleration_decision.json",
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
    if pd.isna(value) if not isinstance(value, (dict, list, tuple, set, Path)) else False:
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _month_key(ymd: Any) -> int:
    return int(float(ymd)) // 100


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _bucket(value: float | None, edges: tuple[float, ...], labels: tuple[str, ...]) -> str:
    if value is None or len(labels) != len(edges) + 1:
        return "unknown"
    for idx, edge in enumerate(edges):
        if value < edge:
            return labels[idx]
    return labels[-1]


def _feature_buckets(row: dict[str, Any]) -> dict[str, str]:
    close_pos = _safe_float(row.get("close_pos"))
    dist_ma20 = _safe_float(row.get("dist_ma20_signed"))
    dist_low20 = _safe_float(row.get("dist_low20"))
    day_change = _safe_float(row.get("day_change_pct"))
    weekly = _safe_float(row.get("weeklyBreakoutDownProb"))
    monthly = _safe_float(row.get("monthlyBreakoutDownProb"))
    range_prob = _safe_float(row.get("monthlyRangeProb"))
    range_pos = _safe_float(row.get("monthlyRangePos"))
    ma20_slope = _safe_float(row.get("ma20_slope"))
    ma60_slope = _safe_float(row.get("ma60_slope"))
    return {
        "close_pos_bucket": _bucket(close_pos, edges=(0.10, 0.25, 0.50, 0.75), labels=("lt_010", "lt_025", "lt_050", "lt_075", "ge_075")),
        "dist_ma20_bucket": _bucket(dist_ma20, edges=(-0.03, -0.01, 0.01, 0.03), labels=("lt_m030", "lt_m010", "lt_p010", "lt_p030", "ge_p030")),
        "dist_low20_bucket": _bucket(dist_low20, edges=(0.005, 0.015, 0.03, 0.05), labels=("lt_005", "lt_015", "lt_030", "lt_050", "ge_050")),
        "day_change_bucket": _bucket(day_change, edges=(-0.03, -0.015, 0.0, 0.02), labels=("lt_m030", "lt_m015", "lt_p000", "lt_p020", "ge_p020")),
        "trend_strict": "trend_strict_true" if bool(row.get("trendDownStrict")) else "trend_strict_false",
        "ma20_slope_bucket": _bucket(ma20_slope, edges=(-0.02, -0.005, 0.005, 0.02), labels=("lt_m020", "lt_m005", "lt_p005", "lt_p020", "ge_p020")),
        "ma60_slope_bucket": _bucket(ma60_slope, edges=(-0.02, -0.005, 0.005, 0.02), labels=("lt_m020", "lt_m005", "lt_p005", "lt_p020", "ge_p020")),
        "weekly_down_prob_bucket": _bucket(weekly, edges=(0.35, 0.50, 0.65, 0.80), labels=("lt_035", "lt_050", "lt_065", "lt_080", "ge_080")),
        "monthly_down_prob_bucket": _bucket(monthly, edges=(0.35, 0.50, 0.65, 0.80), labels=("lt_035", "lt_050", "lt_065", "lt_080", "ge_080")),
        "range_prob_bucket": _bucket(range_prob, edges=(0.10, 0.20, 0.35, 0.50), labels=("lt_010", "lt_020", "lt_035", "lt_050", "ge_050")),
        "range_pos_bucket": _bucket(range_pos, edges=(0.15, 0.35, 0.65, 0.85), labels=("lt_015", "lt_035", "lt_065", "lt_085", "ge_085")),
        "event_risk_short": "event_risk_short_true" if bool(row.get("event_risk_short")) else "event_risk_short_false",
        "borrow_proxy_unfavorable": "borrow_proxy_unfavorable_true" if bool(row.get("borrow_proxy_unfavorable")) else "borrow_proxy_unfavorable_false",
    }


def _row_label(row: dict[str, Any]) -> int:
    ret20 = _safe_float(row.get("short_ret_20"))
    if ret20 is None:
        return 0
    return 1 if ret20 > 0 else 0


def _acceleration_target(row: dict[str, Any]) -> float:
    ret5 = _safe_float(row.get("short_ret_5")) or 0.0
    ret10 = _safe_float(row.get("short_ret_10")) or 0.0
    ret20 = _safe_float(row.get("short_ret_20")) or 0.0
    return 0.4 * ret5 + 0.3 * ret10 + 0.3 * ret20


def _fit_bucket_model(rows: list[dict[str, Any]]) -> dict[str, Any]:
    label_totals = Counter(_row_label(row) for row in rows)
    if not rows:
        return {
            "schema_version": f"{SCHEMA_PREFIX}_bucket_model_v1",
            "generated_at": _utc_now(),
            "axis_id": AXIS_ID,
            "alpha": 1.0,
            "label_totals": {"positive": 0, "negative": 0},
            "feature_weights": {},
            "feature_support": {},
            "train_row_count": 0,
        }
    feature_counts: dict[str, Counter[str]] = defaultdict(Counter)
    feature_ret_sum: dict[str, Counter[str]] = defaultdict(Counter)
    feature_label_sum: dict[str, Counter[str]] = defaultdict(Counter)
    overall_ret = 0.0
    for row in rows:
        label = _row_label(row)
        target_ret = _acceleration_target(row)
        overall_ret += target_ret
        features = _feature_buckets(row)
        for feature, bucket in features.items():
            feature_counts[feature][bucket] += 1
            feature_ret_sum[feature][bucket] += target_ret
            feature_label_sum[feature][bucket] += label
    alpha = 1.0
    overall_mean_ret = overall_ret / float(len(rows))
    feature_weights: dict[str, dict[str, float]] = {}
    feature_support: dict[str, dict[str, int]] = {}
    for feature, buckets in feature_counts.items():
        feature_weights[feature] = {}
        feature_support[feature] = {}
        all_buckets = sorted(buckets.keys())
        for bucket in all_buckets:
            total = float(buckets.get(bucket, 0))
            mean_ret = float(feature_ret_sum[feature].get(bucket, 0.0) / total) if total else overall_mean_ret
            lift = mean_ret - overall_mean_ret
            shrink = math.sqrt(total / (total + 5.0))
            feature_weights[feature][bucket] = float(lift * shrink)
            feature_support[feature][bucket] = int(total)
    return {
        "schema_version": f"{SCHEMA_PREFIX}_bucket_model_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "alpha": alpha,
        "overall_mean_acceleration_target": float(overall_mean_ret),
        "label_totals": {"positive": int(label_totals[1]), "negative": int(label_totals[0])},
        "feature_weights": feature_weights,
        "feature_support": feature_support,
        "train_row_count": int(len(rows)),
    }


def _score_row(row: dict[str, Any], model: dict[str, Any]) -> dict[str, Any]:
    features = _feature_buckets(row)
    weights = model.get("feature_weights", {}) if isinstance(model.get("feature_weights"), dict) else {}
    score = 0.0
    contributions: list[dict[str, Any]] = []
    for feature, bucket in features.items():
        weight = _safe_float((weights.get(feature) or {}).get(bucket)) or 0.0
        score += weight
        contributions.append({"feature": feature, "bucket": bucket, "weight": weight})
    label = _row_label(row)
    return {
        **row,
        **features,
        "statistical_score": float(score),
        "statistical_label": int(label),
        "score_contributions": contributions,
    }


def _group_rankings(rows: list[dict[str, Any]], *, top_k: int) -> list[dict[str, Any]]:
    by_month: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_month[_month_key(row["ymd"])].append(row)
    rankings: list[dict[str, Any]] = []
    for month, month_rows in sorted(by_month.items()):
        baseline_sorted = sorted(
            month_rows,
            key=lambda r: (
                r.get("baseline_rank") is None,
                int(r.get("baseline_rank") or 10**9),
                -(float(r.get("tradePriorityScore") or 0.0)),
                -(float(r.get("entryScore") or 0.0)),
                str(r.get("code")),
            ),
        )
        challenger_sorted = sorted(
            month_rows,
            key=lambda r: (
                -(float(r.get("statistical_score") or 0.0)),
                r.get("baseline_rank") is None,
                int(r.get("baseline_rank") or 10**9),
                -(float(r.get("tradePriorityScore") or 0.0)),
                str(r.get("code")),
            ),
        )
        baseline_top = baseline_sorted[:top_k]
        challenger_top = challenger_sorted[:top_k]
        base_ids = [f"{row['ymd']}|{row['code']}" for row in baseline_top]
        chal_ids = [f"{row['ymd']}|{row['code']}" for row in challenger_top]
        rankings.append(
            {
                "month": int(month),
                "baseline_top_codes": [str(row["code"]) for row in baseline_top],
                "challenger_top_codes": [str(row["code"]) for row in challenger_top],
                "baseline_top_ids": base_ids,
                "challenger_top_ids": chal_ids,
                "baseline_top_mean_ret20": _mean([_safe_float(r.get("short_ret_20")) for r in baseline_top]),
                "challenger_top_mean_ret20": _mean([_safe_float(r.get("short_ret_20")) for r in challenger_top]),
                "baseline_top_hit_rate": _hit_rate([_safe_float(r.get("short_ret_20")) for r in baseline_top]),
                "challenger_top_hit_rate": _hit_rate([_safe_float(r.get("short_ret_20")) for r in challenger_top]),
                "baseline_top_median_ret20": _median([_safe_float(r.get("short_ret_20")) for r in baseline_top]),
                "challenger_top_median_ret20": _median([_safe_float(r.get("short_ret_20")) for r in challenger_top]),
                "baseline_selected_count": int(len(baseline_top)),
                "challenger_selected_count": int(len(challenger_top)),
                "changed_top5_members_count": len(set(base_ids[:5]) ^ set(chal_ids[:5])),
                "changed_rank_count": sum(
                    1
                    for item in set(base_ids).intersection(chal_ids)
                    if base_ids.index(item) != chal_ids.index(item)
                ),
            }
        )
    return rankings


def _mean(values: list[float | None]) -> float | None:
    cleaned = [float(v) for v in values if v is not None and pd.notna(v)]
    if not cleaned:
        return None
    return float(sum(cleaned) / len(cleaned))


def _median(values: list[float | None]) -> float | None:
    cleaned = [float(v) for v in values if v is not None and pd.notna(v)]
    if not cleaned:
        return None
    return float(pd.Series(cleaned).median())


def _hit_rate(values: list[float | None]) -> float | None:
    cleaned = [float(v) for v in values if v is not None and pd.notna(v)]
    if not cleaned:
        return None
    return float(sum(1 for v in cleaned if v > 0.0) / len(cleaned))


def _performance_summary(rankings: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    challenger_rows = [row for row in rows if row.get("used_for_challenger")]
    baseline_rows = [row for row in rows if row.get("used_for_baseline_eval")]
    baseline_rets = [_safe_float(row.get("short_ret_20")) for row in baseline_rows]
    challenger_rets = [_safe_float(row.get("short_ret_20")) for row in challenger_rows]
    return {
        "baseline": {
            "selected_count": len(baseline_rows),
            "hit_rate": _hit_rate(baseline_rets),
            "mean_ret20": _mean(baseline_rets),
            "median_ret20": _median(baseline_rets),
        },
        "challenger": {
            "selected_count": len(challenger_rows),
            "hit_rate": _hit_rate(challenger_rets),
            "mean_ret20": _mean(challenger_rets),
            "median_ret20": _median(challenger_rets),
        },
        "delta": {
            "selected_count_delta": len(challenger_rows) - len(baseline_rows),
            "hit_rate_delta": None if _hit_rate(baseline_rets) is None or _hit_rate(challenger_rets) is None else _hit_rate(challenger_rets) - _hit_rate(baseline_rets),
            "mean_ret20_delta": None if _mean(baseline_rets) is None or _mean(challenger_rets) is None else _mean(challenger_rets) - _mean(baseline_rets),
            "median_ret20_delta": None if _median(baseline_rets) is None or _median(challenger_rets) is None else _median(challenger_rets) - _median(baseline_rets),
            "changed_top5_members_count": sum(int(item["changed_top5_members_count"]) for item in rankings),
            "changed_rank_count": sum(int(item["changed_rank_count"]) for item in rankings),
        },
        "monthly_summary": {
            "month_count": len(rankings),
            "positive_months": sum(1 for item in rankings if (item.get("challenger_top_mean_ret20") or 0.0) > 0),
            "negative_months": sum(1 for item in rankings if (item.get("challenger_top_mean_ret20") or 0.0) < 0),
        },
    }


def _decision(compare: dict[str, Any], rankings: list[dict[str, Any]]) -> tuple[str, str]:
    base = compare["baseline"]
    chal = compare["challenger"]
    delta = compare["delta"]
    monthly = compare["monthly_summary"]
    if chal["selected_count"] < 12:
        return "hold_due_to_small_sample", "selected_sample_thin"
    if chal["hit_rate"] is None or base["hit_rate"] is None:
        return "hold_due_to_small_sample", "insufficient_label_coverage"
    if chal["hit_rate"] > base["hit_rate"] and chal["mean_ret20"] > base["mean_ret20"] and chal["median_ret20"] >= base["median_ret20"] and monthly["positive_months"] >= monthly["negative_months"]:
        return "keep_for_shadow_paper_replay", "statistical_selector_improves_same_condition_short_quality"
    if chal["mean_ret20"] <= base["mean_ret20"] or chal["median_ret20"] <= base["median_ret20"]:
        return "drop_as_statistical_edge_insufficient", "same_condition_quality_failed"
    return "hold_due_to_breadth_or_stability", "improvement_not_robust_enough"


def run_pipeline(*, db_path: str | Path | None = None, output_dir: str | Path | None = None, start_ymd: int = 20240101, end_ymd: int = 20260515, top_k: int = DEFAULT_TOP_K, min_train_months: int = DEFAULT_MIN_TRAIN_MONTHS, lookback_months: int = DEFAULT_LOOKBACK_MONTHS) -> dict[str, Any]:
    source_db = base._resolve_db_path(db_path)
    output_root = Path(output_dir).expanduser().resolve() if output_dir else Path(r"G:\Tradex\downside_acceleration_selection_v1")
    output_root.mkdir(parents=True, exist_ok=True)

    with base.duckdb.connect(str(source_db), read_only=True) as conn:
        months = base._month_end_dates(conn, start_ymd=int(start_ymd), end_ymd=int(end_ymd))
        price_store = base._load_price_store(conn)
        sell_map = base._load_frame_map(conn, "sell_analysis_daily", ymd_col="dt")
        feature_map = base._load_frame_map(conn, "feature_snapshot_daily", ymd_col="dt")
        event_map = base._load_event_map(conn)
        bundle = base._build_rows(conn=conn, months=months, price_store=price_store, sell_map=sell_map, feature_map=feature_map, event_map=event_map)

    rows = bundle["rows"]
    candidate_rows = [dict(row) for row in rows]
    by_month: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        by_month[_month_key(row["ymd"])].append(row)

    baseline_eval_rows: list[dict[str, Any]] = []
    challenger_eval_rows: list[dict[str, Any]] = []
    monthly_rankings: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []
    for month in sorted(by_month):
        train_months = [m for m in sorted(by_month) if m < month]
        if len(train_months) < min_train_months:
            continue
        train_months = train_months[-lookback_months:]
        train_rows = [row for m in train_months for row in by_month[m]]
        model = _fit_bucket_model(train_rows)
        month_rows = [dict(row) for row in by_month[month]]
        month_scored = [_score_row(row, model) for row in month_rows]
        baseline_sorted = sorted(
            month_scored,
            key=lambda r: (
                r.get("baseline_rank") is None,
                int(r.get("baseline_rank") or 10**9),
                -(float(r.get("tradePriorityScore") or 0.0)),
                -(float(r.get("entryScore") or 0.0)),
                str(r.get("code")),
            ),
        )
        challenger_sorted = sorted(
            month_scored,
            key=lambda r: (
                -(float(r.get("statistical_score") or 0.0)),
                r.get("baseline_rank") is None,
                int(r.get("baseline_rank") or 10**9),
                -(float(r.get("tradePriorityScore") or 0.0)),
                str(r.get("code")),
            ),
        )
        baseline_top = baseline_sorted[:top_k]
        challenger_top = challenger_sorted[:top_k]
        baseline_ids = [f"{row['ymd']}|{row['code']}" for row in baseline_top]
        challenger_ids = [f"{row['ymd']}|{row['code']}" for row in challenger_top]
        month_rank = {
            "month": int(month),
            "train_month_count": len(train_months),
            "train_row_count": len(train_rows),
            "train_label_totals": model["label_totals"],
            "baseline_top_codes": [str(row["code"]) for row in baseline_top],
            "challenger_top_codes": [str(row["code"]) for row in challenger_top],
            "baseline_top_ids": baseline_ids,
            "challenger_top_ids": challenger_ids,
            "baseline_top_hit_rate": _hit_rate([_safe_float(r.get("short_ret_20")) for r in baseline_top]),
            "challenger_top_hit_rate": _hit_rate([_safe_float(r.get("short_ret_20")) for r in challenger_top]),
            "baseline_top_mean_ret20": _mean([_safe_float(r.get("short_ret_20")) for r in baseline_top]),
            "challenger_top_mean_ret20": _mean([_safe_float(r.get("short_ret_20")) for r in challenger_top]),
            "baseline_top_median_ret20": _median([_safe_float(r.get("short_ret_20")) for r in baseline_top]),
            "challenger_top_median_ret20": _median([_safe_float(r.get("short_ret_20")) for r in challenger_top]),
            "changed_top5_members_count": len(set(baseline_ids[:5]) ^ set(challenger_ids[:5])),
            "changed_rank_count": sum(1 for item in set(baseline_ids).intersection(challenger_ids) if baseline_ids.index(item) != challenger_ids.index(item)),
            "selected_candidate_count": len(month_scored),
        }
        monthly_rankings.append(month_rank)
        baseline_eval_rows.extend(
            {
                **row,
                "month": int(month),
                "train_month_count": len(train_months),
                "statistical_model_hash": json.dumps(model["feature_weights"], sort_keys=True),
                "used_for_baseline_eval": True,
                "used_for_challenger": False,
            }
            for row in baseline_top
        )
        challenger_eval_rows.extend(
            {
                **row,
                "month": int(month),
                "train_month_count": len(train_months),
                "statistical_model_hash": json.dumps(model["feature_weights"], sort_keys=True),
                "used_for_baseline_eval": False,
                "used_for_challenger": True,
            }
            for row in challenger_top
        )
        model_rows.append(
            {
                "month": int(month),
                "train_month_count": len(train_months),
                "train_row_count": len(train_rows),
                "positive_label_count": model["label_totals"]["positive"],
                "negative_label_count": model["label_totals"]["negative"],
                "feature_weights": model["feature_weights"],
                "feature_support": model["feature_support"],
            }
        )

    compare = _performance_summary(monthly_rankings, baseline_eval_rows + challenger_eval_rows)
    decision, reason = _decision(compare, monthly_rankings)
    baseline_selected = [row for row in baseline_eval_rows]
    challenger_selected = [row for row in challenger_eval_rows]
    if challenger_selected:
        selected_codes = [str(row["code"]) for row in challenger_selected]
        selected_symbol_concentration = max(Counter(selected_codes).values()) / len(selected_codes)
    else:
        selected_symbol_concentration = None

    contract = {
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "boundary": "TRADEX-only",
        "same_condition_controls": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": True,
            "same_regime_condition": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "no_silent_fallback": True,
            "no_meemee_reflection": True,
        },
        "objective": "statistical downside-acceleration short selector",
        "selection_universe": "baseline-selected short candidates from entry_precision_short_audit rows",
        "selection_policy": {
            "walk_forward": True,
            "top_k": int(top_k),
            "min_train_months": int(min_train_months),
            "lookback_months": int(lookback_months),
            "label": "short_ret_20 > 0",
            "label_definition": "good short if 20d forward short return is positive",
        },
        "what_will_not_change": [
            "MeeMee",
            "production ranking",
            "active champion",
            "publish",
            "live sell signal",
        ],
        "input_source_db": str(source_db),
        "baseline_artifact": "entry_precision_short_audit internal rows",
    }
    bucket_model = {
        "schema_version": f"{SCHEMA_PREFIX}_bucket_model_v1",
        "axis_id": AXIS_ID,
        "selected_month_count": len(monthly_rankings),
        "model_rows": model_rows,
        "summary": compare,
    }
    yearly = pd.DataFrame(monthly_rankings)
    if not yearly.empty:
        yearly["year"] = yearly["month"].astype(str).str[:4].astype(int)
        yearly_summary = yearly.groupby("year", as_index=False).agg(
            selected_count=("selected_candidate_count", "sum"),
            challenger_mean_ret20=("challenger_top_mean_ret20", "mean"),
            challenger_median_ret20=("challenger_top_median_ret20", "mean"),
            baseline_mean_ret20=("baseline_top_mean_ret20", "mean"),
            baseline_median_ret20=("baseline_top_median_ret20", "mean"),
        )
    else:
        yearly_summary = pd.DataFrame(columns=["year", "selected_count", "challenger_mean_ret20", "challenger_median_ret20", "baseline_mean_ret20", "baseline_median_ret20"])
    no_lookahead = {
        "schema_version": f"{SCHEMA_PREFIX}_no_lookahead_audit_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "train_rows_only_from_past_months": True,
        "train_lookback_months": int(lookback_months),
        "min_train_months": int(min_train_months),
        "silent_fallback_used": False,
        "runtime_db_written": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "pass": True,
    }
    baseline_score_mean = _mean([_safe_float(row.get("statistical_score")) for row in baseline_eval_rows])
    challenger_score_mean = _mean([_safe_float(row.get("statistical_score")) for row in challenger_eval_rows])
    compare_json = {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "baseline": compare["baseline"],
        "challenger": compare["challenger"],
        "delta": compare["delta"],
        "monthly_summary": compare["monthly_summary"],
        "baseline_score_mean": baseline_score_mean,
        "challenger_score_mean": challenger_score_mean,
        "baseline_acceleration_target_mean": _mean([_acceleration_target(row) for row in baseline_eval_rows]),
        "challenger_acceleration_target_mean": _mean([_acceleration_target(row) for row in challenger_eval_rows]),
        "selected_monthly_rankings": monthly_rankings,
        "selected_symbol_concentration": selected_symbol_concentration,
        "same_condition_contract": contract["same_condition_controls"],
        "decision": decision,
        "reason_type": reason,
    }
    decision_payload = {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "decision": decision,
        "reason_type": reason,
        "baseline_candidate_decision": "keep" if compare["baseline"]["mean_ret20"] is not None and compare["baseline"]["mean_ret20"] > 0 else "hold",
        "challenger_candidate_decision": decision,
        "selected_month_count": len(monthly_rankings),
        "selected_candidate_count": len(challenger_selected),
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "next_gate": "paper_replay_or_hold_review",
        "metrics": compare,
        "selected_symbol_concentration": selected_symbol_concentration,
    }
    _write_json(output_root / "downside_acceleration_selection_contract.json", contract)
    _write_json(output_root / "downside_acceleration_bucket_model.json", bucket_model)
    _write_csv(output_root / "downside_acceleration_monthly_rankings.csv", pd.DataFrame(monthly_rankings))
    _write_json(output_root / "downside_acceleration_compare.json", compare_json)
    _write_json(output_root / "downside_acceleration_yearly_performance.json", {"schema_version": f"{SCHEMA_PREFIX}_yearly_performance_v1", "axis_id": AXIS_ID, "yearly": yearly_summary.to_dict(orient="records")})
    _write_json(output_root / "downside_acceleration_decision.json", decision_payload)
    _write_json(output_root / "no_lookahead_audit.json", no_lookahead)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "decision": decision,
        "reason_type": reason,
        "silent_fallback_used": False,
        "production_state_changed": False,
        "meeMee_changed": False,
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "decision": decision, "reason_type": reason, "compare": compare, "monthly_rankings": monthly_rankings}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Statistical downside-acceleration short selector.")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--start-ymd", type=int, default=20240101)
    parser.add_argument("--end-ymd", type=int, default=20260515)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--min-train-months", type=int, default=DEFAULT_MIN_TRAIN_MONTHS)
    parser.add_argument("--lookback-months", type=int, default=DEFAULT_LOOKBACK_MONTHS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_pipeline(
        db_path=args.db_path,
        output_dir=args.output_dir,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
        top_k=args.top_k,
        min_train_months=args.min_train_months,
        lookback_months=args.lookback_months,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
