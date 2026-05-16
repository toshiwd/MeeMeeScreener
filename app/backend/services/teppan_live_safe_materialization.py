"""Live-safe teppan feature materialization.

This module is read-only and decision-time only. It reconstructs the signal
feature keys needed for teppan pattern matching without requiring future return
labels used by historical evaluation.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

import duckdb
import pandas as pd

from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery
from scripts import tradex_teppan_loss_guard_v1 as loss_guard


FUTURE_LABEL_COLUMNS = {
    "entry_next_open",
    "future_close_20",
    "future_close_40",
    "future_high_20",
    "future_low_20",
    "ret20_fwd",
    "ret40_fwd",
    "mfe20",
    "mae20",
    "win20",
    "win40",
    "severe_loss20",
    "forward_ret_20d",
    "future_return_labels",
}

DECISION_TIME_INPUTS = {
    "daily_bars.o",
    "daily_bars.h",
    "daily_bars.l",
    "daily_bars.c",
    "daily_bars.v",
    "daily_ma.ma20",
    "daily_ma.ma60",
    "monthly_bars.o",
    "monthly_bars.h",
    "monthly_bars.l",
    "monthly_bars.c",
    "monthly_bars.v",
    "monthly_ma.ma20",
    "monthly_ma.ma60",
    "ranking_appearance_daily.dt",
    "ranking_appearance_daily.dir",
    "ranking_appearance_daily.rank",
    "ranking_appearance_daily.code",
    "ranking_appearance_daily.display_score",
}


def load_teppan_candidates(pattern_root: str | Path) -> list[dict[str, Any]]:
    payload = json.loads((Path(pattern_root) / "teppan_candidates.json").read_text(encoding="utf-8"))
    return [dict(row) for row in payload.get("candidates") or [] if isinstance(row, dict)]


def load_recent_runtime_ranking_rows(
    db_path: str | Path,
    *,
    direction: str = "up",
    recent_dates: int = 10,
    rank_limit: int = 100,
) -> list[dict[str, Any]]:
    path = Path(db_path)
    dir_value = str(direction or "up").lower()
    side = "long" if dir_value == "up" else "short"
    conn = duckdb.connect(str(path), read_only=True)
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
    if frame.empty:
        raise ValueError("recent_runtime_ranking_rows_empty")
    frame = frame.sort_values(["dt", "runtime_rank", "display_score", "code"], ascending=[False, True, False, True], kind="stable")
    frame = frame.drop_duplicates(["dt", "code"], keep="first").copy()
    frame["observation_rank"] = frame.groupby("dt", sort=False).cumcount() + 1
    frame = frame[frame["observation_rank"] <= int(rank_limit)].copy()
    return [
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
        for row in frame.itertuples(index=False)
    ]


def build_live_safe_anchor_features(active_rows: Sequence[Mapping[str, Any]], db_path: str | Path) -> list[dict[str, Any]]:
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
    anchors = build_live_safe_signal_features(daily, monthly, anchor_start_ymd=min_ymd)
    wanted = set(zip(frame["symbol"].astype(str), frame["anchor_ymd"].astype(int)))
    anchors = anchors[anchors.apply(lambda row: (str(row["code"]), int(row["ymd"])) in wanted, axis=1)].copy()
    anchors["symbol"] = anchors["code"].astype(str)
    anchors["anchor_ymd"] = anchors["ymd"].astype(int)
    anchors["anchor_date"] = anchors["anchor_ymd"].map(_date_text)
    return anchors.to_dict(orient="records")


def build_live_safe_signal_features(daily: pd.DataFrame, monthly: pd.DataFrame, *, anchor_start_ymd: int) -> pd.DataFrame:
    frame = daily.sort_values(["code", "date"], kind="stable").copy()
    grouped = frame.groupby("code", sort=False)
    frame["history_days"] = grouped.cumcount() + 1
    frame["ma5"] = grouped["c"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    if "ma20" not in frame.columns or frame["ma20"].isna().all():
        frame["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in frame.columns or frame["ma60"].isna().all():
        frame["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
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


def materialize_teppan_features_from_anchors(
    active_rows: Sequence[Mapping[str, Any]],
    anchor_rows: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_lookup = _candidate_lookup(candidates)
    active_index = {(str(row.get("symbol")), str(row.get("anchor_date"))): row for row in active_rows}
    anchor_index = {(str(row.get("symbol")), str(row.get("anchor_date"))): row for row in anchor_rows}
    anchor_frame = pd.DataFrame(anchor_rows)
    risk_by_key: dict[tuple[str, str], bool] = {}
    if not anchor_frame.empty:
        risk = loss_guard._composite_downside_risk(anchor_frame).fillna(False).astype(bool)
        for idx, value in risk.items():
            row = anchor_frame.loc[idx]
            risk_by_key[(str(row.get("symbol")), str(row.get("anchor_date")))] = bool(value)

    rows = []
    for active in active_rows:
        key = (str(active.get("symbol")), str(active.get("anchor_date")))
        anchor = anchor_index.get(key)
        if not anchor:
            rows.append(_empty_row(active, reason="missing_live_safe_anchor_features"))
            continue
        matches = _exact_matches(anchor, candidate_lookup)
        risk_flag = bool(risk_by_key.get(key, False))
        loss_guard_pass = not risk_flag
        pattern_match = bool(matches)
        best = matches[0] if matches else {}
        rows.append(
            {
                "anchor_date": active.get("anchor_date"),
                "anchor_ymd": active.get("anchor_ymd") or int(str(active.get("anchor_date")).replace("-", "")),
                "symbol": active.get("symbol"),
                "name": active.get("name"),
                "side": active.get("side", "long"),
                "active_rank": active.get("champion_rank"),
                "runtime_rank": active.get("runtime_rank"),
                "display_score": active.get("display_score") or active.get("champion_score"),
                "teppan_pattern_match": pattern_match,
                "teppan_guard_pass": bool(pattern_match and loss_guard_pass),
                "loss_guard_pass": bool(loss_guard_pass),
                "loss_guard_blocked": bool(pattern_match and not loss_guard_pass),
                "guard_block_reason": "composite_downside_risk" if pattern_match and risk_flag else "" if pattern_match else "no_teppan_pattern_match",
                "matched_pattern_count": len(matches),
                "best_pattern_family": best.get("pattern_family"),
                "best_pattern_key": best.get("pattern_key"),
                "best_pattern_decision": best.get("pattern_decision"),
                "best_teppan_score": _optional_float(best.get("teppan_score")),
                "signal_features": {column: anchor.get(column) for column in sorted(discovery.SIGNAL_FEATURE_COLUMNS)},
                "future_label_inputs_used": False,
            }
        )
    return {
        "schema_version": "teppan_live_safe_materialization_result_v1",
        "rows": rows,
        "summary": _summary(rows, anchor_rows),
        "input_dependency_audit": build_input_dependency_audit(),
    }


def materialize_teppan_features(
    active_rows: Sequence[Mapping[str, Any]],
    *,
    db_path: str | Path,
    pattern_root: str | Path,
) -> dict[str, Any]:
    candidates = load_teppan_candidates(pattern_root)
    anchors = build_live_safe_anchor_features(active_rows, db_path)
    return materialize_teppan_features_from_anchors(active_rows, anchors, candidates)


def build_input_dependency_audit() -> dict[str, Any]:
    overlap = sorted(DECISION_TIME_INPUTS & FUTURE_LABEL_COLUMNS)
    return {
        "schema_version": "teppan_live_safe_feature_input_dependency_audit_v1",
        "decision_time_inputs": sorted(DECISION_TIME_INPUTS),
        "signal_feature_columns": sorted(discovery.SIGNAL_FEATURE_COLUMNS),
        "future_label_columns_forbidden": sorted(FUTURE_LABEL_COLUMNS),
        "future_label_overlap": overlap,
        "future_labels_used": bool(overlap),
        "historical_evaluation_filter_used": False,
    }


def independent_exact_match_rows(
    materialized_rows: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    candidate_lookup = _candidate_lookup(candidates)
    out = []
    for row in materialized_rows:
        features = row.get("signal_features") if isinstance(row.get("signal_features"), Mapping) else {}
        matches = _exact_matches(features, candidate_lookup)
        best = matches[0] if matches else {}
        out.append(
            {
                "symbol": row.get("symbol"),
                "anchor_date": row.get("anchor_date"),
                "independent_teppan_pattern_match": bool(matches),
                "independent_match_count": len(matches),
                "independent_best_pattern_family": best.get("pattern_family"),
                "independent_best_pattern_key": best.get("pattern_key"),
            }
        )
    return out


def _empty_row(active: Mapping[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "anchor_date": active.get("anchor_date"),
        "anchor_ymd": active.get("anchor_ymd"),
        "symbol": active.get("symbol"),
        "name": active.get("name"),
        "side": active.get("side", "long"),
        "active_rank": active.get("champion_rank"),
        "runtime_rank": active.get("runtime_rank"),
        "display_score": active.get("display_score") or active.get("champion_score"),
        "teppan_pattern_match": False,
        "teppan_guard_pass": False,
        "loss_guard_pass": False,
        "loss_guard_blocked": False,
        "guard_block_reason": reason,
        "matched_pattern_count": 0,
        "future_label_inputs_used": False,
    }


def _candidate_lookup(candidates: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
    return {(str(row.get("pattern_family") or ""), str(row.get("pattern_key") or "")): row for row in candidates}


def _exact_matches(row: Mapping[str, Any], candidate_lookup: Mapping[tuple[str, str], Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    matches = []
    for family, columns in discovery.PATTERN_FAMILIES:
        key = "|".join(f"{column}={row.get(column)}" for column in columns)
        candidate = candidate_lookup.get((family, key))
        if candidate:
            matches.append(candidate)
    return sorted(matches, key=lambda item: -float(item.get("teppan_score") or 0.0))


def _summary(rows: Sequence[Mapping[str, Any]], anchor_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    match = sum(1 for row in rows if row.get("teppan_pattern_match") is True)
    guard = sum(1 for row in rows if row.get("teppan_guard_pass") is True)
    blocked = sum(1 for row in rows if row.get("loss_guard_blocked") is True)
    return {
        "row_count": count,
        "live_safe_anchor_feature_rows": len(anchor_rows),
        "teppan_pattern_match_count": match,
        "teppan_pattern_match_rate": _rate(match, count),
        "teppan_guard_pass_count": guard,
        "teppan_guard_pass_rate": _rate(guard, count),
        "loss_guard_blocked_count": blocked,
        "loss_guard_blocked_rate": _rate(blocked, count),
        "future_label_inputs_used": False,
        "guard_block_reason_counts": dict(Counter(str(row.get("guard_block_reason") or "") for row in rows)),
    }


def _date_text(value: Any) -> str:
    text = str(value or "")
    if "-" in text:
        return text[:10]
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}" if len(text) >= 8 else text


def _optional_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _rate(num: int, denom: int) -> float | None:
    return None if denom <= 0 else float(num) / float(denom)
