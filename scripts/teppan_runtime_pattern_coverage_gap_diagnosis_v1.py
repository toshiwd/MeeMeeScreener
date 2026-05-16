"""Diagnosis-only runtime pattern coverage gap audit for teppan shadow."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.codex_bridge_service import get_runtime_stock_db_status
from app.backend.services.teppan_shadow_integration_adapter import (
    DEFAULT_PLAN_ROOT,
    compute_teppan_shadow_adjusted_ranking,
    load_teppan_shadow_plan,
)
from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery
from scripts import tradex_teppan_loss_guard_v1 as loss_guard
from scripts.tradex_teppan_ranking_branching_probe_v1 import build_teppan_tags_for_source


AXIS_ID = "teppan_runtime_pattern_coverage_gap_diagnosis_v1"
DEFAULT_RUN_ID = "20260514T080000Z-teppan-runtime-pattern-coverage-gap-diagnosis-v1"
DEFAULT_OUTPUT_PARENT = Path(r"G:\Tradex\runtime_pattern_coverage_gap_diagnosis\teppan_runtime_pattern_coverage_gap_diagnosis_v1")
DEFAULT_PATTERN_ROOT = Path(
    r"G:\Tradex\teppan_chart_pattern_discovery_v1"
    r"\20260514T000000Z-current-runtime-teppan-discovery-v1-teppan_chart_pattern_discovery_v1"
)
TOP_KS = (20, 50, 100)
REQUIRED_RANKING_FIELDS = {"dt", "dir", "rank", "code", "name", "display_score"}
REQUIRED_OUTPUTS = [
    "coverage_gap_diagnosis_result.json",
    "runtime_coverage_by_topk_and_date.json",
    "pattern_condition_pass_rates.json",
    "guard_coverage_report.json",
    "historical_runtime_condition_diff.json",
    "ranking_appearance_field_audit.json",
    "materialization_false_diagnostic.json",
    "candidate_rows.json",
    "no_mutation_audit.json",
    "_ARTIFACT_COMPLETE.json",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--pattern-root", type=Path, default=DEFAULT_PATTERN_ROOT)
    parser.add_argument("--output-parent", type=Path, default=DEFAULT_OUTPUT_PARENT)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    parser.add_argument("--direction", default="up")
    parser.add_argument("--recent-dates", type=int, default=10)
    parser.add_argument("--rank-limit", type=int, default=100)
    args = parser.parse_args()
    run_teppan_runtime_pattern_coverage_gap_diagnosis_v1(
        plan_root=args.plan_root,
        pattern_root=args.pattern_root,
        output_parent=args.output_parent,
        run_id=args.run_id,
        direction=args.direction,
        recent_dates=args.recent_dates,
        rank_limit=args.rank_limit,
    )
    return 0


def run_teppan_runtime_pattern_coverage_gap_diagnosis_v1(
    *,
    plan_root: Path = DEFAULT_PLAN_ROOT,
    pattern_root: Path = DEFAULT_PATTERN_ROOT,
    output_parent: Path = DEFAULT_OUTPUT_PARENT,
    run_id: str = DEFAULT_RUN_ID,
    direction: str = "up",
    recent_dates: int = 10,
    rank_limit: int = 100,
    runtime_status: Mapping[str, Any] | None = None,
    active_rows: Sequence[Mapping[str, Any]] | None = None,
    anchor_features: Sequence[Mapping[str, Any]] | None = None,
    teppan_tags: Sequence[Mapping[str, Any]] | None = None,
    ranking_field_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    output_root = output_parent / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    plan = load_teppan_shadow_plan(plan_root)
    effective_runtime_status = dict(runtime_status or get_runtime_stock_db_status())
    db_path = Path(str(effective_runtime_status.get("selected_runtime_db_path") or ""))
    db_stat_before = _file_stat(db_path)
    field_audit = dict(ranking_field_audit or _ranking_appearance_field_audit(db_path))
    candidates = _load_teppan_candidates(pattern_root)
    candidate_lookup = _candidate_lookup(candidates)

    source_rows = list(active_rows) if active_rows is not None else _load_recent_runtime_rows(
        db_path,
        direction=direction,
        recent_dates=recent_dates,
        rank_limit=rank_limit,
    )
    anchors = list(anchor_features) if anchor_features is not None else _build_live_safe_anchor_features(source_rows, db_path)
    tags = list(teppan_tags) if teppan_tags is not None else build_teppan_tags_for_source(
        source_rows=pd.DataFrame(source_rows),
        source_db=db_path,
        pattern_dir=pattern_root,
    ).to_dict(orient="records")
    source_rows = _merge_anchor_features(source_rows, anchors)
    feature_rows = _feature_rows_from_tags(tags, source_rows)
    shadow_result = compute_teppan_shadow_adjusted_ranking(source_rows, feature_rows, plan)
    candidate_rows = _enrich_candidate_rows(shadow_result["shadow_rows"], source_rows, feature_rows, anchors, candidate_lookup)

    coverage_by_topk_date = _coverage_by_topk_date(candidate_rows)
    condition_pass_rates = _condition_pass_rates(anchors, candidates)
    guard_report = _guard_coverage_report(candidate_rows)
    historical_diff = _historical_runtime_condition_diff(anchors, candidates)
    materialization_diag = _materialization_false_diagnostic(anchors, tags, candidate_lookup, active_row_count=len(source_rows))
    db_stat_after = _file_stat(db_path)
    no_mutation = _no_mutation_audit(
        db_path=db_path,
        db_stat_before=db_stat_before,
        db_stat_after=db_stat_after,
        adapter_audit=shadow_result["audit"],
    )
    decision = _decision(field_audit, materialization_diag, coverage_by_topk_date)
    result = {
        "schema_version": "teppan_runtime_pattern_coverage_gap_diagnosis_v1",
        "axis_id": AXIS_ID,
        "decision": decision,
        "generated_at_utc": _utc_now(),
        "diagnosis_only": True,
        "source_surface": "runtime_duckdb.ranking_appearance_daily",
        "runtime_stock_db_status": effective_runtime_status,
        "plan_root": str(plan.plan_root),
        "pattern_root": str(pattern_root),
        "recent_dates": recent_dates,
        "rank_limit": rank_limit,
        "ranking_appearance_field_audit": field_audit,
        "runtime_coverage_by_topk_and_date": coverage_by_topk_date,
        "pattern_condition_pass_rates": condition_pass_rates,
        "guard_coverage_report": guard_report,
        "historical_runtime_condition_diff": historical_diff,
        "materialization_false_diagnostic": materialization_diag,
        "candidate_rows": candidate_rows,
        "no_mutation_audit": no_mutation,
        "not_changed": [
            "active_rank",
            "display_score",
            "runtime_duckdb",
            "production_publish_registry",
            "frontend_ui",
            "backend_api_response",
        ],
    }

    _write_json(output_root / "coverage_gap_diagnosis_result.json", result)
    _write_json(output_root / "runtime_coverage_by_topk_and_date.json", coverage_by_topk_date)
    _write_json(output_root / "pattern_condition_pass_rates.json", condition_pass_rates)
    _write_json(output_root / "guard_coverage_report.json", guard_report)
    _write_json(output_root / "historical_runtime_condition_diff.json", historical_diff)
    _write_json(output_root / "ranking_appearance_field_audit.json", field_audit)
    _write_json(output_root / "materialization_false_diagnostic.json", materialization_diag)
    _write_json(output_root / "candidate_rows.json", {"candidate_rows": candidate_rows})
    _write_json(output_root / "no_mutation_audit.json", no_mutation)
    complete = _artifact_complete(output_root, result)
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"output_root": str(output_root), "coverage_gap_diagnosis_result": result, "artifact_complete": complete}


def _ranking_appearance_field_audit(db_path: Path) -> dict[str, Any]:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute("DESCRIBE ranking_appearance_daily").fetchall()
    finally:
        conn.close()
    columns = {str(row[0]) for row in rows}
    missing = sorted(REQUIRED_RANKING_FIELDS - columns)
    return {
        "schema_version": "teppan_runtime_ranking_appearance_field_audit_v1",
        "table": "ranking_appearance_daily",
        "required_fields": sorted(REQUIRED_RANKING_FIELDS),
        "present_fields": sorted(columns),
        "missing_required_fields": missing,
        "pass": not missing,
    }


def _load_recent_runtime_rows(db_path: Path, *, direction: str, recent_dates: int, rank_limit: int) -> list[dict[str, Any]]:
    dir_value = str(direction or "up").lower()
    side = "long" if dir_value == "up" else "short"
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        dates = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT dt FROM ranking_appearance_daily WHERE dir = ? ORDER BY dt DESC LIMIT ?",
                [dir_value, int(recent_dates)],
            ).fetchall()
        ]
        rows = conn.execute(
            """
            SELECT dt, rank, code, name, display_score, signal_state_at_appearance,
                   entry_qualified_at_appearance, setup_type_at_appearance, status
            FROM ranking_appearance_daily
            WHERE dir = ?
              AND dt IN (SELECT UNNEST(?))
              AND rank <= ?
              AND display_score IS NOT NULL
            ORDER BY dt DESC, rank, code
            """,
            [dir_value, dates, int(rank_limit)],
        ).fetchall()
    finally:
        conn.close()
    frame = pd.DataFrame(
        rows,
        columns=[
            "dt",
            "runtime_rank",
            "code",
            "name",
            "display_score",
            "signal_state",
            "entry_qualified",
            "setup_type",
            "status",
        ],
    )
    frame = frame.sort_values(["dt", "runtime_rank", "display_score", "code"], ascending=[False, True, False, True], kind="stable")
    frame = frame.drop_duplicates(["dt", "code"], keep="first").copy()
    frame["observation_rank"] = frame.groupby("dt", sort=False).cumcount() + 1
    frame = frame[frame["observation_rank"] <= int(rank_limit)].copy()
    out = []
    for row in frame.itertuples(index=False):
        out.append(
            {
                "anchor_date": _date_text(row.dt),
                "anchor_ymd": int(row.dt),
                "symbol": str(row.code),
                "name": row.name,
                "side": side,
                "champion_rank": int(row.observation_rank),
                "runtime_rank": int(row.runtime_rank),
                "champion_score": float(row.display_score),
                "display_score": float(row.display_score),
                "signal_state": row.signal_state,
                "entry_qualified": bool(row.entry_qualified),
                "setup_type": row.setup_type,
                "status": row.status,
            }
        )
    if not out:
        raise ValueError("recent_runtime_ranking_rows_empty")
    return out


def _build_live_safe_anchor_features(active_rows: Sequence[Mapping[str, Any]], db_path: Path) -> list[dict[str, Any]]:
    frame = pd.DataFrame(active_rows)
    frame["anchor_ymd"] = frame["anchor_date"].map(lambda value: int(str(value).replace("-", ""))).astype(int)
    source_path = discovery._resolve_source_db(db_path)
    min_ymd = int(frame["anchor_ymd"].min())
    max_ymd = int(frame["anchor_ymd"].max())
    data_start = int((pd.to_datetime(str(min_ymd), format="%Y%m%d") - pd.DateOffset(days=520)).strftime("%Y%m%d"))
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        daily = discovery._load_daily_rows(conn, start_ymd=data_start, end_ymd=max_ymd)
        monthly = discovery._load_monthly_rows(conn, start_ymd=data_start, end_ymd=max_ymd)
    finally:
        conn.close()
    anchors = _build_live_safe_signal_features(daily, monthly, anchor_start_ymd=min_ymd)
    wanted = set(zip(frame["symbol"].astype(str), frame["anchor_ymd"].astype(int)))
    anchors = anchors[anchors.apply(lambda row: (str(row["code"]), int(row["ymd"])) in wanted, axis=1)].copy()
    anchors["symbol"] = anchors["code"].astype(str)
    anchors["anchor_ymd"] = anchors["ymd"].astype(int)
    anchors["anchor_date"] = anchors["anchor_ymd"].map(_date_text)
    return anchors.to_dict(orient="records")


def _build_live_safe_signal_features(daily: pd.DataFrame, monthly: pd.DataFrame, *, anchor_start_ymd: int) -> pd.DataFrame:
    frame = daily.sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["history_days"] = grouped.cumcount() + 1
    frame["ma5"] = grouped["c"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    if "ma20" not in frame.columns or frame["ma20"].isna().all():
        frame["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in frame.columns or frame["ma60"].isna().all():
        frame["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    frame["ret5"] = grouped["c"].transform(lambda s: s / s.shift(5) - 1.0)
    frame["ret20"] = grouped["c"].transform(lambda s: s / s.shift(20) - 1.0)
    frame["ma60_slope_20d"] = grouped["ma60"].transform(lambda s: s / s.shift(20) - 1.0)
    vol5 = grouped["v"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    vol20 = grouped["v"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    frame["vol_ratio5_20"] = discovery._safe_div(vol5, vol20)
    frame["daily_ma_stack"] = "daily_stack_mixed"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_bull_stack_5_20_60"
    frame.loc[(frame["ma5"] > frame["ma20"]) & (frame["ma20"] <= frame["ma60"]), "daily_ma_stack"] = "daily_near_bull_5_over_20_under_60"
    frame.loc[(frame["ma5"] <= frame["ma20"]) & (frame["ma20"] > frame["ma60"]), "daily_ma_stack"] = "daily_pullback_20_over_60"
    frame.loc[(frame["ma5"] < frame["ma20"]) & (frame["ma20"] < frame["ma60"]), "daily_ma_stack"] = "daily_bear_stack_5_20_60"
    frame["daily_ma60_slope_state"] = "daily_ma60_flat"
    frame.loc[frame["ma60_slope_20d"] >= 0.02, "daily_ma60_slope_state"] = "daily_ma60_rising"
    frame.loc[frame["ma60_slope_20d"] <= -0.02, "daily_ma60_slope_state"] = "daily_ma60_falling"
    frame["daily_ret20_state"] = discovery._bucket_return(frame["ret20"], strong_down=-0.08, down=-0.03, up=0.03, strong_up=0.08, prefix="daily20")
    frame["daily_candle_state"] = discovery._candle_state(frame["o"], frame["h"], frame["l"], frame["c"], prefix="daily")
    frame["daily_volume_state"] = "daily_volume_normal"
    frame.loc[frame["vol_ratio5_20"] >= 1.6, "daily_volume_state"] = "daily_volume_expansion"
    frame.loc[frame["vol_ratio5_20"] <= 0.7, "daily_volume_state"] = "daily_volume_dry"
    strong_bull = frame["daily_candle_state"].isin({"daily_strong_bull", "daily_lower_wick_bull"})
    weak_bear = frame["daily_candle_state"].isin({"daily_strong_bear", "daily_upper_wick_warning"})
    frame["strong_bull_count_5"] = strong_bull.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(5, min_periods=5).sum())
    frame["weak_bear_count_5"] = weak_bear.astype(float).groupby(frame["code"], sort=False).transform(lambda s: s.rolling(5, min_periods=5).sum())
    frame["daily_sequence_state"] = "daily_sequence_mixed"
    frame.loc[(frame["strong_bull_count_5"] >= 2) & (frame["weak_bear_count_5"] <= 1), "daily_sequence_state"] = "daily_sequence_bullish"
    frame.loc[(frame["weak_bear_count_5"] >= 2), "daily_sequence_state"] = "daily_sequence_warning"
    frame["anchor_month"] = frame["date"].dt.to_period("M").astype(str)
    frame["week_key"] = frame["date"].dt.to_period("W-FRI").astype(str)
    frame["month_key"] = frame["date"].dt.to_period("M").astype(str)
    eligible = frame[(frame["ymd"] >= int(anchor_start_ymd)) & (frame["history_days"] >= discovery.MIN_HISTORY_DAYS)].copy()
    weekly_features = discovery.build_weekly_feature_frame(daily)
    monthly_features = discovery.build_monthly_feature_frame(monthly)
    eligible = eligible.merge(weekly_features, left_on=["code", "week_key"], right_on=["code", "effective_week_key"], how="left")
    eligible = eligible.merge(monthly_features, left_on=["code", "month_key"], right_on=["code", "effective_month_key"], how="left")
    for column in discovery.SIGNAL_FEATURE_COLUMNS:
        if column in eligible.columns:
            eligible[column] = eligible[column].fillna(f"{column}_unknown").astype(str)
    return eligible


def _load_teppan_candidates(pattern_root: Path) -> list[dict[str, Any]]:
    payload = json.loads((pattern_root / "teppan_candidates.json").read_text(encoding="utf-8"))
    return [dict(row) for row in payload.get("candidates") or [] if isinstance(row, dict)]


def _candidate_lookup(candidates: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
    lookup = {}
    for row in candidates:
        lookup[(str(row.get("pattern_family") or ""), str(row.get("pattern_key") or ""))] = row
    return lookup


def _merge_anchor_features(source_rows: Sequence[Mapping[str, Any]], anchors: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    anchor_index = {(str(row.get("symbol")), int(row.get("anchor_ymd") or 0)): row for row in anchors}
    out = []
    for row in source_rows:
        merged = dict(row)
        anchor = anchor_index.get((str(row.get("symbol")), int(str(row.get("anchor_date")).replace("-", ""))))
        if anchor:
            for column in discovery.SIGNAL_FEATURE_COLUMNS:
                merged[column] = anchor.get(column)
        out.append(merged)
    return out


def _feature_rows_from_tags(tags: Sequence[Mapping[str, Any]], active_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    active_index = {str(row.get("symbol")) + ":" + str(row.get("anchor_date")): row for row in active_rows}
    out = []
    for tag in tags:
        row = dict(tag)
        symbol = str(row.get("symbol") or "")
        anchor_date = _date_text(row.get("anchor_ymd"))
        active = active_index.get(symbol + ":" + anchor_date, {})
        row["anchor_date"] = active.get("anchor_date") or anchor_date
        row["side"] = active.get("side") or "long"
        out.append(row)
    return out


def _enrich_candidate_rows(
    shadow_rows: Sequence[Mapping[str, Any]],
    active_rows: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, Any]],
    anchors: Sequence[Mapping[str, Any]],
    candidate_lookup: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    active_index = {(str(row.get("symbol")), str(row.get("anchor_date"))): row for row in active_rows}
    feature_index = {(str(row.get("symbol")), str(row.get("anchor_date"))): row for row in feature_rows}
    anchor_index = {(str(row.get("symbol")), str(row.get("anchor_date"))): row for row in anchors}
    out = []
    for shadow in shadow_rows:
        key = (str(shadow.get("symbol")), str(shadow.get("anchor_date")))
        active = active_index.get(key, {})
        feature = feature_index.get(key, {})
        anchor = anchor_index.get(key, {})
        independent = _independent_exact_matches(anchor, candidate_lookup)
        out.append(
            _compact(
                {
                    "anchor_date": shadow.get("anchor_date"),
                    "symbol": shadow.get("symbol"),
                    "name": active.get("name"),
                    "active_rank": shadow.get("active_rank"),
                    "runtime_rank": active.get("runtime_rank"),
                    "display_score": shadow.get("active_display_score"),
                    "shadow_adjusted_rank": shadow.get("shadow_adjusted_rank"),
                    "shadow_adjusted_score": shadow.get("shadow_adjusted_score"),
                    "teppan_pattern_match": shadow.get("teppan_pattern_match"),
                    "teppan_guard_pass": shadow.get("teppan_guard_pass"),
                    "teppan_guarded_boost_applied": shadow.get("teppan_guarded_boost_applied"),
                    "shadow_decision_reason": shadow.get("shadow_decision_reason"),
                    "guard_block_reason": feature.get("guard_block_reason"),
                    "matched_pattern_count": feature.get("matched_pattern_count"),
                    "independent_exact_match_count": len(independent),
                    "independent_best_pattern_family": independent[0]["pattern_family"] if independent else None,
                    "independent_best_pattern_key": independent[0]["pattern_key"] if independent else None,
                    "signal_state": active.get("signal_state"),
                    "entry_qualified": active.get("entry_qualified"),
                    "setup_type": active.get("setup_type"),
                }
            )
        )
    return sorted(out, key=lambda row: (str(row["anchor_date"]), int(row["active_rank"]), str(row["symbol"])))


def _coverage_by_topk_date(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {"schema_version": "teppan_runtime_coverage_by_topk_and_date_v1", "by_topk": {}, "by_date": {}}
    for k in TOP_KS:
        subset = [row for row in rows if int(row["active_rank"]) <= k]
        out["by_topk"][f"top{k}"] = _coverage_bucket(subset)
    for date in sorted({str(row["anchor_date"]) for row in rows}):
        date_rows = [row for row in rows if str(row["anchor_date"]) == date]
        out["by_date"][date] = {f"top{k}": _coverage_bucket([row for row in date_rows if int(row["active_rank"]) <= k]) for k in TOP_KS}
    return out


def _coverage_bucket(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    pattern = sum(1 for row in rows if row.get("teppan_pattern_match") is True)
    guard = sum(1 for row in rows if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in rows if row.get("teppan_pattern_match") is True and row.get("teppan_guard_pass") is False)
    boosted = sum(1 for row in rows if row.get("teppan_guarded_boost_applied") is True)
    independent = sum(1 for row in rows if int(row.get("independent_exact_match_count") or 0) > 0)
    return {
        "row_count": total,
        "teppan_pattern_match_count": pattern,
        "teppan_pattern_match_rate": _rate(pattern, total),
        "teppan_guard_pass_count": guard,
        "teppan_guard_pass_rate": _rate(guard, total),
        "loss_guard_blocked_count": blocked,
        "loss_guard_blocked_rate": _rate(blocked, total),
        "boosted_candidate_count": boosted,
        "boosted_candidate_rate": _rate(boosted, total),
        "independent_exact_match_count": independent,
        "independent_exact_match_rate": _rate(independent, total),
    }


def _condition_pass_rates(anchors: Sequence[Mapping[str, Any]], candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    approved_values: dict[str, set[str]] = defaultdict(set)
    for row in candidates:
        features = row.get("pattern_features") if isinstance(row.get("pattern_features"), Mapping) else {}
        for column, value in features.items():
            approved_values[str(column)].add(str(value))
    feature_rows = {}
    for column in sorted(discovery.SIGNAL_FEATURE_COLUMNS):
        values = [str(row.get(column)) for row in anchors if row.get(column) is not None]
        approved = approved_values.get(column, set())
        pass_count = sum(1 for value in values if value in approved)
        feature_rows[column] = {
            "runtime_non_null_count": len(values),
            "approved_value_count": len(approved),
            "approved_value_pass_count": pass_count,
            "approved_value_pass_rate": _rate(pass_count, len(values)),
            "top_runtime_values": dict(Counter(values).most_common(10)),
            "approved_values_sample": sorted(approved)[:20],
        }
    family_rows = {}
    for family, columns in discovery.PATTERN_FAMILIES:
        best_scores = []
        exact = 0
        candidate_keys = {
            str(row.get("pattern_key"))
            for row in candidates
            if str(row.get("pattern_family")) == family and row.get("pattern_key") is not None
        }
        for anchor in anchors:
            matched = sum(1 for column in columns if _feature_condition_pass(anchor, column, candidates, family))
            best_scores.append(float(matched) / float(len(columns)))
            key = _pattern_key(anchor, columns)
            if key in candidate_keys:
                exact += 1
        family_rows[family] = {
            "columns": list(columns),
            "candidate_key_count": len(candidate_keys),
            "exact_match_count": exact,
            "exact_match_rate": _rate(exact, len(anchors)),
            "avg_any_candidate_condition_pass_rate": sum(best_scores) / len(best_scores) if best_scores else None,
        }
    return {
        "schema_version": "teppan_runtime_pattern_condition_pass_rates_v1",
        "feature_condition_pass_rates": feature_rows,
        "family_condition_pass_rates": family_rows,
    }


def _feature_condition_pass(anchor: Mapping[str, Any], column: str, candidates: Sequence[Mapping[str, Any]], family: str) -> bool:
    value = str(anchor.get(column))
    for row in candidates:
        if str(row.get("pattern_family")) != family:
            continue
        features = row.get("pattern_features") if isinstance(row.get("pattern_features"), Mapping) else {}
        if str(features.get(column)) == value:
            return True
    return False


def _guard_coverage_report(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "teppan_runtime_guard_coverage_report_v1",
        "all": _guard_bucket(rows),
        "by_topk": {f"top{k}": _guard_bucket([row for row in rows if int(row["active_rank"]) <= k]) for k in TOP_KS},
        "guard_block_reason_counts": dict(Counter(str(row.get("guard_block_reason") or "") for row in rows)),
    }


def _guard_bucket(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    pattern = [row for row in rows if row.get("teppan_pattern_match") is True]
    guard_pass = sum(1 for row in pattern if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in pattern if row.get("teppan_guard_pass") is False)
    return {
        "row_count": total,
        "pattern_match_count": len(pattern),
        "guard_pass_count": guard_pass,
        "guard_pass_rate_among_matches": _rate(guard_pass, len(pattern)),
        "loss_guard_blocked_count": blocked,
        "loss_guard_blocked_rate_among_matches": _rate(blocked, len(pattern)),
    }


def _historical_runtime_condition_diff(anchors: Sequence[Mapping[str, Any]], candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    historical_decisions = dict(Counter(str(row.get("pattern_decision")) for row in candidates))
    runtime_feature_values = {
        column: dict(Counter(str(row.get(column)) for row in anchors if row.get(column) is not None).most_common(20))
        for column in sorted(discovery.SIGNAL_FEATURE_COLUMNS)
    }
    top_candidate_features = [
        {
            "pattern_family": row.get("pattern_family"),
            "pattern_decision": row.get("pattern_decision"),
            "teppan_score": row.get("teppan_score"),
            "pattern_features": row.get("pattern_features"),
            "runtime_exact_match_count": _runtime_exact_count(anchors, row),
        }
        for row in candidates[:20]
    ]
    return {
        "schema_version": "teppan_historical_runtime_condition_diff_v1",
        "historical_candidate_count": len(candidates),
        "historical_decision_counts": historical_decisions,
        "runtime_anchor_count": len(anchors),
        "runtime_feature_value_distributions": runtime_feature_values,
        "top_historical_candidate_runtime_exact_counts": top_candidate_features,
        "main_gap": "no_current_runtime_anchor_matches_any_approved_candidate_key"
        if all(item["runtime_exact_match_count"] == 0 for item in top_candidate_features)
        else "some_top_historical_patterns_exist_in_runtime",
    }


def _materialization_false_diagnostic(
    anchors: Sequence[Mapping[str, Any]],
    tags: Sequence[Mapping[str, Any]],
    candidate_lookup: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    active_row_count: int,
) -> dict[str, Any]:
    independent_by_key = {
        (str(anchor.get("symbol")), str(anchor.get("anchor_date"))): len(_independent_exact_matches(anchor, candidate_lookup))
        for anchor in anchors
    }
    tag_match_by_key = {
        (str(tag.get("symbol")), _date_text(tag.get("anchor_ymd"))): bool(tag.get("teppan_pattern_match"))
        for tag in tags
    }
    independent_count = sum(1 for count in independent_by_key.values() if count > 0)
    tag_count = sum(1 for value in tag_match_by_key.values() if value is True)
    tag_row_coverage_rate = _rate(len(tags), active_row_count)
    label_dependent_gap = len(anchors) > 0 and len(tags) < len(anchors)
    mismatch = []
    for key, independent_count_for_row in independent_by_key.items():
        tag_value = tag_match_by_key.get(key, False)
        if (independent_count_for_row > 0) != tag_value:
            mismatch.append({"symbol": key[0], "anchor_date": key[1], "independent_exact_match_count": independent_count_for_row, "tag_match": tag_value})
    return {
        "schema_version": "teppan_materialization_false_diagnostic_v1",
        "active_row_count": active_row_count,
        "live_safe_anchor_feature_rows": len(anchors),
        "anchor_feature_rows": len(anchors),
        "materialized_tag_rows": len(tags),
        "materialized_tag_row_coverage_rate": tag_row_coverage_rate,
        "independent_exact_match_count": independent_count,
        "materialized_teppan_pattern_match_count": tag_count,
        "mismatch_count": len(mismatch),
        "mismatch_examples": mismatch[:20],
        "label_dependent_current_runtime_gap_suspected": label_dependent_gap,
        "materialization_impl_bug_suspected": len(mismatch) > 0,
        "diagnosis": "materialization_label_dependent_current_runtime_gap"
        if label_dependent_gap
        else "materialization_matches_independent_exact_match_zero"
        if independent_count == 0 and tag_count == 0 and not mismatch
        else "materialization_mismatch_or_runtime_exact_matches_present"
        if mismatch
        else "materialization_matches_independent_exact_matches",
    }


def _independent_exact_matches(anchor: Mapping[str, Any], candidate_lookup: Mapping[tuple[str, str], Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    out = []
    for family, columns in discovery.PATTERN_FAMILIES:
        key = _pattern_key(anchor, columns)
        row = candidate_lookup.get((family, key))
        if row:
            out.append(row)
    return sorted(out, key=lambda row: -float(row.get("teppan_score") or 0.0))


def _runtime_exact_count(anchors: Sequence[Mapping[str, Any]], candidate: Mapping[str, Any]) -> int:
    family = str(candidate.get("pattern_family") or "")
    pattern_key = str(candidate.get("pattern_key") or "")
    columns = next((cols for fam, cols in discovery.PATTERN_FAMILIES if fam == family), ())
    return sum(1 for anchor in anchors if _pattern_key(anchor, columns) == pattern_key)


def _pattern_key(row: Mapping[str, Any], columns: Sequence[str]) -> str:
    return "|".join(f"{column}={row.get(column)}" for column in columns)


def _decision(
    field_audit: Mapping[str, Any],
    materialization_diag: Mapping[str, Any],
    coverage_by_topk_date: Mapping[str, Any],
) -> str:
    if not field_audit.get("pass"):
        return "hold_runtime_ranking_field_gap"
    if materialization_diag.get("label_dependent_current_runtime_gap_suspected"):
        return "hold_materialization_label_dependency_gap"
    if materialization_diag.get("materialization_impl_bug_suspected"):
        return "hold_materialization_impl_gap"
    top100 = ((coverage_by_topk_date.get("by_topk") or {}).get("top100") or {})
    if int(top100.get("teppan_pattern_match_count") or 0) == 0:
        return "gap_confirmed_no_current_runtime_exact_pattern_match"
    return "coverage_present_requires_shadow_reobservation"


def _no_mutation_audit(
    *,
    db_path: Path,
    db_stat_before: Mapping[str, Any],
    db_stat_after: Mapping[str, Any],
    adapter_audit: Mapping[str, Any],
) -> dict[str, Any]:
    unchanged = db_stat_before == db_stat_after
    return {
        "schema_version": "teppan_runtime_pattern_gap_no_mutation_audit_v1",
        "runtime_duckdb_path": str(db_path),
        "runtime_duckdb_stat_before": dict(db_stat_before),
        "runtime_duckdb_stat_after": dict(db_stat_after),
        "runtime_duckdb_unchanged": unchanged,
        "runtime_duckdb_written": not unchanged,
        "active_ranking_invariance_pass": bool(adapter_audit.get("active_ranking_invariance_pass")),
        "active_rank_unchanged": bool(adapter_audit.get("active_rank_unchanged")),
        "display_score_unchanged": bool(adapter_audit.get("active_display_score_unchanged")),
        "production_publish_registered": False,
        "frontend_changed": False,
        "backend_api_response_changed": False,
        "no_mutation_pass": unchanged and bool(adapter_audit.get("active_ranking_invariance_pass")),
        "silent_fallback_used": False,
    }


def _artifact_complete(output_root: Path, result: Mapping[str, Any]) -> dict[str, Any]:
    presence = {name: (output_root / name).exists() for name in REQUIRED_OUTPUTS if name != "_ARTIFACT_COMPLETE.json"}
    presence["_ARTIFACT_COMPLETE.json"] = True
    return {
        "schema_version": "teppan_runtime_pattern_gap_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "decision": result.get("decision"),
        "complete": all(presence.values()),
        "required_outputs": REQUIRED_OUTPUTS,
        "present_outputs": presence,
        "output_root": str(output_root),
        "silent_fallback_used": False,
    }


def _rate(num: int, denom: int) -> float | None:
    return None if denom <= 0 else float(num) / float(denom)


def _date_text(value: Any) -> str:
    text = str(value or "")
    if "-" in text:
        return text[:10]
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}" if len(text) >= 8 else text


def _file_stat(path: Path) -> dict[str, Any]:
    if not path or not str(path):
        return {"exists": False}
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {"exists": True, "path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _compact(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _json_ready(value) for key, value in row.items() if value is not None}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
