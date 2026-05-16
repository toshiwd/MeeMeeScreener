from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.services import tradex_research_contracts as contracts
from scripts import tradex_teppan_chart_pattern_discovery_v1 as discovery


AXIS_ID = "teppan_loss_guard_v1"
SCHEMA_PREFIX = "tradex_teppan_loss_guard_v1"
DEFAULT_SOURCE_DB = discovery.DEFAULT_SOURCE_DB
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\teppan_loss_guard_v1")

DEFAULT_YEARS = 10
PRIMARY_GUARD_ID = "composite_downside_guard_v1"
SELECTED_PATTERN_DECISIONS = {"teppan_candidate", "high_return_candidate", "high_win_rate_candidate"}
MIN_GUARD_TRADES = 250
MIN_GUARD_SYMBOLS = 30
MIN_GUARD_MONTHS = 18

LABEL_COLUMNS = set(discovery.LABEL_COLUMNS)
SIGNAL_FEATURE_COLUMNS = set(discovery.SIGNAL_FEATURE_COLUMNS)

REQUIRED_ARTIFACTS = (
    "evaluation_contract.json",
    "run_manifest.json",
    "feature_availability_audit.json",
    "guard_ledger.jsonl",
    "guard_compare.json",
    "guard_leaderboard.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(_json_text(row) + "\n" for row in rows), encoding="utf-8")
    return path


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_json_text(payload).encode("utf-8")).hexdigest()


def _safe_path(value: str | Path | None, default: Path) -> Path:
    if value is None or not str(value).strip():
        return default.resolve()
    return Path(str(value)).expanduser().resolve()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _profit_factor(ret: pd.Series) -> float | None:
    values = pd.to_numeric(ret, errors="coerce")
    gains = float(values[values > 0.0].sum())
    losses = float(values[values < 0.0].sum())
    if losses == 0.0:
        return None if gains == 0.0 else 999.0
    return float(gains / abs(losses))


def _safe_delta(candidate: float | None, baseline: float | None) -> float | None:
    if candidate is None or baseline is None:
        return None
    return float(candidate - baseline)


def _load_anchor_features(source_path: Path, *, years: int) -> tuple[pd.DataFrame, int, int]:
    conn = duckdb.connect(str(source_path), read_only=True)
    try:
        max_daily_ymd = discovery._load_max_daily_ymd(conn)
        max_daily_ts = discovery._ymd_to_timestamp(max_daily_ymd)
        anchor_start_ts = max_daily_ts - pd.DateOffset(years=int(years))
        data_start_ts = anchor_start_ts - pd.DateOffset(days=520)
        anchor_start_ymd = discovery._timestamp_to_ymd(anchor_start_ts)
        data_start_ymd = discovery._timestamp_to_ymd(data_start_ts)
        daily = discovery._load_daily_rows(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
        monthly = discovery._load_monthly_rows(conn, start_ymd=data_start_ymd, end_ymd=max_daily_ymd)
    finally:
        conn.close()
    anchors = discovery.build_anchor_features(daily, monthly, anchor_start_ymd=anchor_start_ymd)
    return anchors, anchor_start_ymd, max_daily_ymd


def _pattern_key(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series:
    values = [frame[column].astype(str).radd(f"{column}=") for column in columns]
    out = values[0]
    for value in values[1:]:
        out = out + "|" + value
    return out


def build_selected_pattern_opportunities(anchors: pd.DataFrame, pattern_rows: list[dict[str, Any]]) -> pd.DataFrame:
    selected_by_family: dict[str, set[str]] = {}
    selected_meta: dict[tuple[str, str], dict[str, Any]] = {}
    for row in pattern_rows:
        if row.get("pattern_decision") not in SELECTED_PATTERN_DECISIONS:
            continue
        family = str(row["pattern_family"])
        key = str(row["pattern_key"])
        selected_by_family.setdefault(family, set()).add(key)
        selected_meta[(family, key)] = {
            "source_pattern_decision": row.get("pattern_decision"),
            "source_teppan_score": row.get("teppan_score"),
            "source_win_rate20": row.get("win_rate20"),
            "source_avg_ret20": row.get("avg_ret20"),
            "source_severe_loss_rate20": row.get("severe_loss_rate20"),
        }

    base_columns = [
        "code",
        "ymd",
        "anchor_month",
        "ret20_fwd",
        "ret40_fwd",
        "mfe20",
        "mae20",
        "win20",
        "win40",
        "severe_loss20",
        *sorted(SIGNAL_FEATURE_COLUMNS),
    ]
    chunks: list[pd.DataFrame] = []
    for family_id, columns in discovery.PATTERN_FAMILIES:
        wanted = selected_by_family.get(family_id)
        if not wanted:
            continue
        work = anchors[base_columns].copy()
        work["pattern_family"] = family_id
        work["pattern_key"] = _pattern_key(work, columns)
        work = work[work["pattern_key"].isin(wanted)].copy()
        if work.empty:
            continue
        for column_name in (
            "source_pattern_decision",
            "source_teppan_score",
            "source_win_rate20",
            "source_avg_ret20",
            "source_severe_loss_rate20",
        ):
            work[column_name] = [
                selected_meta[(family_id, key)][column_name] for key in work["pattern_key"].astype(str).tolist()
            ]
        work["opportunity_id"] = (
            work["code"].astype(str) + "|" + work["ymd"].astype(str) + "|" + work["pattern_family"].astype(str) + "|" + work["pattern_key"].astype(str)
        )
        chunks.append(work)

    if not chunks:
        return pd.DataFrame(columns=[*base_columns, "pattern_family", "pattern_key", "opportunity_id"])
    return pd.concat(chunks, ignore_index=True)


def _monthly_bull_chase_risk(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["monthly_ret6_state"].eq("monthly6_strong_down")
        & frame["daily_ma_stack"].eq("daily_bull_stack_5_20_60")
        & frame["daily_ret20_state"].isin({"daily20_up", "daily20_strong_up"})
        & frame["daily_candle_state"].isin({"daily_strong_bull", "daily_upper_wick_warning"})
    )


def _higher_frame_conflict_risk(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["monthly_trend_state"].eq("monthly_downtrend")
        & frame["weekly_trend_state"].isin({"weekly_downtrend", "weekly_mixed"})
        & frame["weekly_candle_state"].isin({"weekly_strong_bear", "weekly_upper_wick_warning", "weekly_small_neutral", "weekly_doji"})
    )


def _upper_wick_after_rise_risk(frame: pd.DataFrame) -> pd.Series:
    return frame["daily_candle_state"].eq("daily_upper_wick_warning") & frame["daily_ret20_state"].isin({"daily20_up", "daily20_strong_up"})


def _weekly_breakdown_risk(frame: pd.DataFrame) -> pd.Series:
    return frame["weekly_trend_state"].eq("weekly_downtrend") | frame["weekly_ret4_state"].isin({"weekly4_down", "weekly4_strong_down"})


def _composite_downside_risk(frame: pd.DataFrame) -> pd.Series:
    return (
        _monthly_bull_chase_risk(frame)
        | _higher_frame_conflict_risk(frame)
        | _upper_wick_after_rise_risk(frame)
        | _weekly_breakdown_risk(frame)
    )


GuardFn = Callable[[pd.DataFrame], pd.Series]

GUARD_RULES: tuple[dict[str, Any], ...] = (
    {
        "guard_id": PRIMARY_GUARD_ID,
        "typed_reason": "composite_downside_risk",
        "description": "Union of fixed monthly bull-chase, higher-frame conflict, upper-wick-after-rise, and weekly-breakdown guards.",
        "primary": True,
        "fn": _composite_downside_risk,
    },
    {
        "guard_id": "monthly_bull_chase_guard_v1",
        "typed_reason": "monthly_strong_down_daily_bull_chase",
        "description": "Exclude bull-stack daily chase while monthly 6-month return is strongly down.",
        "primary": False,
        "fn": _monthly_bull_chase_risk,
    },
    {
        "guard_id": "higher_frame_conflict_guard_v1",
        "typed_reason": "monthly_downtrend_weekly_not_confirming",
        "description": "Exclude monthly downtrend with weak or mixed weekly confirmation.",
        "primary": False,
        "fn": _higher_frame_conflict_risk,
    },
    {
        "guard_id": "upper_wick_after_rise_guard_v1",
        "typed_reason": "daily_upper_wick_after_rise",
        "description": "Exclude upper-wick warning after a 20-day up move.",
        "primary": False,
        "fn": _upper_wick_after_rise_risk,
    },
    {
        "guard_id": "weekly_breakdown_guard_v1",
        "typed_reason": "weekly_breakdown",
        "description": "Exclude weekly downtrend or negative 4-week return state.",
        "primary": False,
        "fn": _weekly_breakdown_risk,
    },
)


def summarize_opportunities(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "opportunity_count": 0,
            "unique_anchor_count": 0,
            "pattern_count": 0,
            "symbol_count": 0,
            "month_count": 0,
            "avg_ret20": None,
            "median_ret20": None,
            "win_rate20": None,
            "profit_factor20": None,
            "avg_ret40": None,
            "win_rate40": None,
            "avg_mfe20": None,
            "avg_mae20": None,
            "severe_loss_rate20": None,
            "positive_month_rate20": None,
            "worst_month_mean_ret20": None,
        }
    ret20 = pd.to_numeric(frame["ret20_fwd"], errors="coerce")
    ret40 = pd.to_numeric(frame["ret40_fwd"], errors="coerce")
    monthly_mean = frame.groupby("anchor_month", dropna=False)["ret20_fwd"].mean()
    anchor_id = frame["code"].astype(str) + "|" + frame["ymd"].astype(str)
    pattern_id = frame["pattern_family"].astype(str) + "|" + frame["pattern_key"].astype(str)
    return {
        "opportunity_count": int(len(frame)),
        "unique_anchor_count": int(anchor_id.nunique()),
        "pattern_count": int(pattern_id.nunique()),
        "symbol_count": int(frame["code"].nunique()),
        "month_count": int(frame["anchor_month"].nunique()),
        "avg_ret20": _safe_float(ret20.mean()),
        "median_ret20": _safe_float(ret20.median()),
        "win_rate20": _safe_float(frame["win20"].astype(float).mean()),
        "profit_factor20": _profit_factor(ret20),
        "avg_ret40": _safe_float(ret40.mean()),
        "win_rate40": _safe_float(frame["win40"].astype(float).mean()),
        "avg_mfe20": _safe_float(pd.to_numeric(frame["mfe20"], errors="coerce").mean()),
        "avg_mae20": _safe_float(pd.to_numeric(frame["mae20"], errors="coerce").mean()),
        "severe_loss_rate20": _safe_float(frame["severe_loss20"].astype(float).mean()),
        "positive_month_rate20": _safe_float((monthly_mean > 0.0).mean()),
        "worst_month_mean_ret20": _safe_float(monthly_mean.min()),
    }


def _guard_decision(row: dict[str, Any]) -> str:
    kept = row["kept"]
    if kept["opportunity_count"] < MIN_GUARD_TRADES or kept["symbol_count"] < MIN_GUARD_SYMBOLS or kept["month_count"] < MIN_GUARD_MONTHS:
        return "drop_insufficient_remaining_sample"
    if (
        kept["win_rate20"] is not None
        and kept["avg_ret20"] is not None
        and kept["profit_factor20"] is not None
        and kept["severe_loss_rate20"] is not None
        and kept["positive_month_rate20"] is not None
        and kept["win_rate20"] >= 0.58
        and kept["avg_ret20"] >= 0.015
        and kept["profit_factor20"] >= 1.35
        and kept["severe_loss_rate20"] <= 0.08
        and kept["positive_month_rate20"] >= 0.60
    ):
        return "keep_teppan_loss_guard_candidate"

    deltas = row["delta_vs_baseline"]
    severe_delta = deltas.get("severe_loss_rate20")
    avg_ret_delta = deltas.get("avg_ret20")
    win_delta = deltas.get("win_rate20")
    mae_delta = deltas.get("avg_mae20")
    profit_delta = deltas.get("profit_factor20")
    if (
        severe_delta is not None
        and avg_ret_delta is not None
        and win_delta is not None
        and mae_delta is not None
        and profit_delta is not None
        and severe_delta <= -0.03
        and avg_ret_delta >= -0.005
        and win_delta >= -0.02
        and mae_delta >= 0.0
        and profit_delta >= 0.0
    ):
        return "keep_loss_guard_improves_risk_without_expectancy_harm"
    if (
        severe_delta is not None
        and avg_ret_delta is not None
        and win_delta is not None
        and mae_delta is not None
        and severe_delta <= -0.015
        and avg_ret_delta >= -0.01
        and win_delta >= -0.03
        and mae_delta >= -0.005
    ):
        return "hold_loss_guard_reduces_risk_but_not_teppan"
    return "drop_no_reliable_loss_guard_improvement"


def evaluate_guards(opportunities: pd.DataFrame) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    baseline = summarize_opportunities(opportunities)
    rows: list[dict[str, Any]] = []
    for rule in GUARD_RULES:
        mask = rule["fn"](opportunities).fillna(False).astype(bool)
        kept_frame = opportunities.loc[~mask].copy()
        removed_frame = opportunities.loc[mask].copy()
        kept = summarize_opportunities(kept_frame)
        removed = summarize_opportunities(removed_frame)
        delta = {
            metric: _safe_delta(kept.get(metric), baseline.get(metric))
            for metric in (
                "avg_ret20",
                "median_ret20",
                "win_rate20",
                "profit_factor20",
                "avg_ret40",
                "win_rate40",
                "avg_mfe20",
                "avg_mae20",
                "severe_loss_rate20",
                "positive_month_rate20",
                "worst_month_mean_ret20",
            )
        }
        row = {
            "guard_id": rule["guard_id"],
            "typed_reason": rule["typed_reason"],
            "description": rule["description"],
            "primary": bool(rule["primary"]),
            "baseline": baseline,
            "kept": kept,
            "removed": removed,
            "removed_rate": None if baseline["opportunity_count"] == 0 else removed["opportunity_count"] / baseline["opportunity_count"],
            "delta_vs_baseline": delta,
            "no_lookahead_features": True,
            "silent_fallback_used": False,
        }
        row["decision"] = _guard_decision(row)
        rows.append(row)
    order = {
        "keep_teppan_loss_guard_candidate": 0,
        "keep_loss_guard_improves_risk_without_expectancy_harm": 1,
        "hold_loss_guard_reduces_risk_but_not_teppan": 2,
        "drop_no_reliable_loss_guard_improvement": 3,
        "drop_insufficient_remaining_sample": 4,
    }
    rows = sorted(
        rows,
        key=lambda row: (
            order.get(row["decision"], 9),
            row["delta_vs_baseline"].get("severe_loss_rate20") if row["delta_vs_baseline"].get("severe_loss_rate20") is not None else 99,
            -(row["kept"].get("avg_ret20") or -99),
        ),
    )
    return baseline, rows


def build_feature_availability_audit(opportunities: pd.DataFrame) -> dict[str, Any]:
    rows = []
    for column in sorted(SIGNAL_FEATURE_COLUMNS):
        present = column in opportunities.columns
        non_null = int(opportunities[column].notna().sum()) if present else 0
        rows.append({"column": column, "present": present, "non_null_count": non_null, "non_null_rate": None if len(opportunities) == 0 else non_null / len(opportunities)})
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_availability_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "opportunity_rows": int(len(opportunities)),
        "signal_feature_columns": sorted(SIGNAL_FEATURE_COLUMNS),
        "label_columns_excluded_from_guard_rules": sorted(LABEL_COLUMNS),
        "signal_label_overlap": sorted(SIGNAL_FEATURE_COLUMNS & LABEL_COLUMNS),
        "used_future_labels_in_guard_rules": bool(SIGNAL_FEATURE_COLUMNS & LABEL_COLUMNS),
        "guard_axis": "downside_risk_guard_only",
        "guard_rule_ids": [str(rule["guard_id"]) for rule in GUARD_RULES],
        "primary_guard_id": PRIMARY_GUARD_ID,
        "silent_fallback_used": False,
    }


def build_evaluation_contract(*, source_db: Path, anchor_start_ymd: int, max_daily_ymd: int, years: int) -> dict[str, Any]:
    payload = {
        "schema_version": f"{SCHEMA_PREFIX}_evaluation_contract_v1",
        "axis_id": AXIS_ID,
        "boundary": "TRADEX-only",
        "research_phase": "downside-risk guard over discovered teppan-like chart patterns",
        "source_db": str(source_db),
        "anchor_start_ymd": int(anchor_start_ymd),
        "max_daily_ymd": int(max_daily_ymd),
        "requested_years": int(years),
        "base_axis": discovery.AXIS_ID,
        "selected_pattern_decisions": sorted(SELECTED_PATTERN_DECISIONS),
        "primary_guard_id": PRIMARY_GUARD_ID,
        "guard_axis": "downside_risk_guard_only",
        "entry_convention": "same as base discovery: buy next session open after pattern is observable at anchor close",
        "outcomes": ["ret20", "ret40", "mfe20", "mae20", "win_rate20", "severe_loss_rate20"],
        "future_label_policy": {
            "future_labels_used_for_guard_rules": False,
            "future_labels_used_for_evaluation": True,
        },
        "same_condition_controls": {
            "same_universe_source": "runtime snapshot daily_bars PAN source",
            "same_period": True,
            "same_cost_slippage": contracts.TRADEX_DEFAULT_COST_MODEL,
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        },
        "candidate_scoring_created": False,
        "meemee_reflection_allowed": False,
        "publish_bundle_allowed": False,
        "silent_fallback_used": False,
    }
    payload["contract_hash"] = _stable_hash(payload)
    return payload


def build_research_decision(output_dir: Path, baseline: dict[str, Any], guard_rows: list[dict[str, Any]], opportunities: pd.DataFrame) -> dict[str, Any]:
    primary = next((row for row in guard_rows if row["guard_id"] == PRIMARY_GUARD_ID), None)
    keep_rows = [row for row in guard_rows if row["decision"].startswith("keep_")]
    hold_rows = [row for row in guard_rows if row["decision"].startswith("hold_")]
    if primary and primary["decision"].startswith("keep_"):
        decision = "keep"
        reason = "primary_guard_passed"
    elif primary and primary["decision"].startswith("hold_"):
        decision = "hold"
        reason = "primary_guard_reduced_risk_but_not_teppan"
    elif keep_rows:
        decision = "hold"
        reason = "diagnostic_guard_passed_primary_not_keep"
    elif hold_rows:
        decision = "hold"
        reason = "diagnostic_guard_reduced_risk_primary_not_keep"
    else:
        decision = "drop"
        reason = "no_guard_reduced_loss_without_tradeoff"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_research_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "candidate_local_decision": primary["decision"] if primary else "drop_missing_primary_guard",
        "session_aggregate_decision": decision,
        "authoritative_research_decision": decision,
        "authoritative_reason": reason,
        "primary_guard_id": PRIMARY_GUARD_ID,
        "baseline": baseline,
        "primary_guard": primary,
        "top_guard_rows": guard_rows,
        "sample_summary": {
            "opportunity_rows": int(len(opportunities)),
            "unique_anchor_count": int((opportunities["code"].astype(str) + "|" + opportunities["ymd"].astype(str)).nunique()) if not opportunities.empty else 0,
            "symbol_count": int(opportunities["code"].nunique()) if not opportunities.empty else 0,
            "month_count": int(opportunities["anchor_month"].nunique()) if not opportunities.empty else 0,
            "pattern_count": int((opportunities["pattern_family"].astype(str) + "|" + opportunities["pattern_key"].astype(str)).nunique()) if not opportunities.empty else 0,
        },
        "candidate_scoring_created": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }


def _artifact_complete(output_dir: Path, paths: dict[str, str], decision: dict[str, Any]) -> dict[str, Any]:
    existing = {name: Path(path).exists() for name, path in paths.items()}
    required_existing = {name: (output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "artifact_root": str(output_dir),
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "existing_artifacts": {**existing, **required_existing},
        "complete": all(existing.values()) and all(required_existing.values()),
        "candidate_local_decision": decision["candidate_local_decision"],
        "session_aggregate_decision": decision["session_aggregate_decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def _ledger_rows(guard_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in guard_rows:
        rows.append(
            {
                "guard_id": row["guard_id"],
                "typed_reason": row["typed_reason"],
                "decision": row["decision"],
                "primary": row["primary"],
                "removed_rate": row["removed_rate"],
                "kept_opportunity_count": row["kept"]["opportunity_count"],
                "removed_opportunity_count": row["removed"]["opportunity_count"],
                "baseline_severe_loss_rate20": row["baseline"]["severe_loss_rate20"],
                "kept_severe_loss_rate20": row["kept"]["severe_loss_rate20"],
                "delta_severe_loss_rate20": row["delta_vs_baseline"]["severe_loss_rate20"],
                "baseline_avg_mae20": row["baseline"]["avg_mae20"],
                "kept_avg_mae20": row["kept"]["avg_mae20"],
                "delta_avg_mae20": row["delta_vs_baseline"]["avg_mae20"],
                "baseline_avg_ret20": row["baseline"]["avg_ret20"],
                "kept_avg_ret20": row["kept"]["avg_ret20"],
                "delta_avg_ret20": row["delta_vs_baseline"]["avg_ret20"],
                "baseline_win_rate20": row["baseline"]["win_rate20"],
                "kept_win_rate20": row["kept"]["win_rate20"],
                "delta_win_rate20": row["delta_vs_baseline"]["win_rate20"],
                "no_lookahead_features": True,
                "silent_fallback_used": False,
            }
        )
    return rows


def run_teppan_loss_guard_v1(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    years: int = DEFAULT_YEARS,
) -> dict[str, Any]:
    source_path = discovery._resolve_source_db(source_db)
    output_base = _safe_path(output_root, DEFAULT_OUTPUT_ROOT)
    run_name = run_id.strip() if run_id else _default_run_id()
    if not run_name.endswith(AXIS_ID):
        run_name = f"{run_name}-{AXIS_ID}"
    output_dir = output_base / run_name
    output_dir.mkdir(parents=True, exist_ok=True)

    anchors, anchor_start_ymd, max_daily_ymd = _load_anchor_features(source_path, years=years)
    pattern_rows = discovery.evaluate_pattern_families(anchors)
    opportunities = build_selected_pattern_opportunities(anchors, pattern_rows)
    baseline, guard_rows = evaluate_guards(opportunities)

    evaluation_contract = build_evaluation_contract(
        source_db=source_path,
        anchor_start_ymd=anchor_start_ymd,
        max_daily_ymd=max_daily_ymd,
        years=years,
    )
    run_manifest = contracts.build_run_manifest(
        session_id=run_name,
        seed=0,
        random_seed=0,
        input_artifacts=[
            {"name": "source_db", "path": str(source_path)},
            {"name": "evaluation_contract", "contract_hash": evaluation_contract["contract_hash"]},
        ],
        asof=str(max_daily_ymd),
        config={
            "axis_id": AXIS_ID,
            "base_axis": discovery.AXIS_ID,
            "years": int(years),
            "selected_pattern_decisions": sorted(SELECTED_PATTERN_DECISIONS),
            "primary_guard_id": PRIMARY_GUARD_ID,
            "guard_axis": "downside_risk_guard_only",
            "candidate_scoring_created": False,
        },
        universe=sorted(opportunities["code"].astype(str).unique().tolist()) if not opportunities.empty else [],
        period={"start_date": str(anchor_start_ymd), "end_date": str(max_daily_ymd), "label": "teppan_candidate_guard_compare"},
        horizon="20d_and_40d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )
    contracts.validate_run_manifest(run_manifest)
    feature_audit = build_feature_availability_audit(opportunities)
    decision = build_research_decision(output_dir, baseline, guard_rows, opportunities)
    compare = {
        "schema_version": f"{SCHEMA_PREFIX}_guard_compare_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "baseline": baseline,
        "primary_guard_id": PRIMARY_GUARD_ID,
        "primary_guard": decision["primary_guard"],
        "guard_rows": guard_rows,
        "candidate_scoring_created": False,
        "meemee_reflectable": False,
        "publish_bundle_created": False,
        "silent_fallback_used": False,
    }
    leaderboard = {
        "schema_version": f"{SCHEMA_PREFIX}_guard_leaderboard_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "primary_guard_id": PRIMARY_GUARD_ID,
        "decision": decision["authoritative_research_decision"],
        "rows": guard_rows,
    }

    paths: dict[str, str] = {}
    for name, payload in {
        "evaluation_contract.json": evaluation_contract,
        "run_manifest.json": run_manifest,
        "feature_availability_audit.json": feature_audit,
        "guard_compare.json": compare,
        "guard_leaderboard.json": leaderboard,
        "research_decision.json": decision,
    }.items():
        paths[name] = str(_write_json(output_dir / name, payload))
    paths["guard_ledger.jsonl"] = str(_write_jsonl(output_dir / "guard_ledger.jsonl", _ledger_rows(guard_rows)))
    complete = _artifact_complete(output_dir, paths, decision)
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete))

    return {
        "output_dir": str(output_dir),
        "paths": paths,
        "candidate_local_decision": decision["candidate_local_decision"],
        "session_aggregate_decision": decision["session_aggregate_decision"],
        "authoritative_research_decision": decision["authoritative_research_decision"],
        "primary_guard": decision["primary_guard"],
        "baseline": baseline,
        "silent_fallback_used": False,
        "candidate_scoring_created": False,
        "publish_bundle_created": False,
        "meemee_reflectable": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default="")
    parser.add_argument("--years", type=int, default=DEFAULT_YEARS)
    args = parser.parse_args(argv)
    result = run_teppan_loss_guard_v1(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        run_id=args.run_id.strip() or None,
        years=args.years,
    )
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
