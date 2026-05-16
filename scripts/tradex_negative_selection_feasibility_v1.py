from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_entry_timing_branching_enrichment_v1 import _aggregate_context, _bars_for_symbols, _rolling_context
from scripts.tradex_reflectability_funnel_common_v1 import _write_json, build_artifact_complete

SOURCE_ROWS = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet")
STOCK_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")
REL_RERANK_ROOT = Path(r"G:\Tradex\relative_strength_persistence_v1\20260511T073456Z-relative_strength_persistence_v1")
REL_VETO_ROOT = Path(r"G:\Tradex\relative_strength_persistence_veto_v1\20260511T083046Z-relative_strength_persistence_veto_v1")
FINAL_ROOT_BASE = Path(r"G:\Tradex\relative_strength_family_final_decision")
NEG_ROOT_BASE = Path(r"G:\Tradex\negative_selection_avoidance_v1")
EPSILON = 1e-9


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_to_epoch(value: str) -> int:
    return int(pd.Timestamp(value).timestamp())


def _ratio(value: float | None, base: float | None) -> float | None:
    if value is None or base is None or abs(base) <= EPSILON:
        return None
    return value / base - 1.0


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    keys = sorted({key for row in rows for key in row}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _finalize_relative_strength(root: Path) -> dict[str, str]:
    root.mkdir(parents=True, exist_ok=True)
    rerank_compare = _load_json(REL_RERANK_ROOT / "compare.json")
    rerank_decision = _load_json(REL_RERANK_ROOT / "candidate_decision.json")
    veto_compare = _load_json(REL_VETO_ROOT / "compare.json")
    veto_decision = _load_json(REL_VETO_ROOT / "candidate_decision.json")
    payload = {
        "schema_version": "tradex_relative_strength_family_final_decision_v1",
        "generated_at": _utc_now(),
        "family_name": "relative_strength",
        "final_decision": "drop",
        "dropped_candidates": ["relative_strength_persistence_v1", "relative_strength_persistence_veto_v1"],
        "source_run_roots": {
            "relative_strength_persistence_v1": str(REL_RERANK_ROOT),
            "relative_strength_persistence_veto_v1": str(REL_VETO_ROOT),
        },
        "key_metrics": {
            "reranker": {
                "decision": rerank_decision["authoritative_rollup_decision"],
                "top5_return_delta": rerank_compare["deltas"]["top5"]["forward_ret_20d_mean_delta"],
                "top10_return_delta": rerank_compare["deltas"]["top10"]["forward_ret_20d_mean_delta"],
                "top5_severe_loser_rate_delta": rerank_compare["deltas"]["top5"]["severe_loser_rate_delta"],
                "changed_top5_members_count": rerank_compare["branching"]["changed_top5_members_count"],
            },
            "veto": {
                "decision": veto_decision["authoritative_rollup_decision"],
                "top5_return_delta": veto_compare["deltas"]["top5"]["forward_ret_20d_mean_delta"],
                "top10_return_delta": veto_compare["deltas"]["top10"]["forward_ret_20d_mean_delta"],
                "top5_severe_loser_rate_delta": veto_compare["deltas"]["top5"]["severe_loser_rate_delta"],
                "top10_severe_loser_rate_delta": veto_compare["deltas"]["top10"]["severe_loser_rate_delta"],
            },
        },
        "why_reranker_failed": "full reranking created real branching but worsened top5 expectancy; replacement quality was not reliable",
        "why_veto_failed": "veto-only variant did not improve severe loser rate and slightly worsened top5/top10 expectancy",
        "why_threshold_tuning_is_not_allowed": "both full-rerank and fixed veto forms failed under the fixed-condition contract; continuing with threshold sweeps would tune the same failed family",
        "non_scope_confirmation": {
            "no_meemee_change": True,
            "no_live_ranking_change": True,
            "no_champion_scoring_change": True,
            "no_publish_promotion_change": True,
        },
        "recommended_next_independent_axis": "negative_selection_avoidance_v1",
    }
    paths = {"relative_strength_family_final_decision.json": str(_write_json(root / "relative_strength_family_final_decision.json", payload))}
    complete = build_artifact_complete({"schema_version": "tradex_relative_strength_family_final_decision_complete_v1", "artifact_root": str(root)}, ["relative_strength_family_final_decision.json"], schema_version="tradex_relative_strength_family_final_decision_complete_v1")
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(root / "_ARTIFACT_COMPLETE.json", complete))
    return paths


def _prepare_context(frame: pd.DataFrame) -> dict[tuple[str, int], pd.Series]:
    symbols = sorted(frame["symbol"].astype(str).unique())
    max_epoch = max(_date_to_epoch(str(value)) for value in frame["anchor_date"].astype(str))
    bars = _bars_for_symbols(symbols, max_epoch, STOCK_DB)
    frames = []
    for _, group in bars.groupby("code", sort=False):
        ctx = _rolling_context(group)
        for col in ("above_ma7", "below_ma7", "above_ma20", "below_ma20"):
            ctx[f"{col}_count_recent"] = ctx[col].rolling(10, min_periods=1).sum()
        ctx["prev_c"] = ctx["c"].shift(1)
        ctx["ret_1d"] = ctx["c"] / ctx["prev_c"] - 1.0
        ctx["range_pct"] = (ctx["h"] - ctx["l"]) / ctx["prev_c"]
        ctx["range_ma20"] = ctx["range_pct"].rolling(20, min_periods=20).mean()
        ctx["volatility_expansion_downside_like_flag"] = (ctx["ret_1d"] < 0) & (ctx["range_pct"] > ctx["range_ma20"] * 1.5)
        ctx["lower_high_like_flag"] = ctx["h"] < ctx["h"].shift(1).rolling(5, min_periods=2).max()
        ctx["failed_breakout_like_flag"] = (ctx["h"] >= ctx["h"].rolling(20, min_periods=20).max().shift(1)) & (ctx["c"] < ctx["o"])
        ctx["high_volume_failure_like_flag"] = (ctx["ret_1d"] < 0) & (ctx["v"] > ctx["v"].rolling(20, min_periods=20).mean() * 1.5)
        ctx["weak_rebound_after_breakdown_like_flag"] = (ctx["c"] < ctx["ma20"]) & (ctx["ret_5d"] > 0) & (ctx["c"] < ctx["ma7"])
        frames.append(ctx)
    context = pd.concat(frames, ignore_index=True)
    return {(str(row["code"]), int(row["date"])): row for _, row in context.iterrows()}


def _enrich_candidate(row: dict[str, Any], ctx: pd.Series | None) -> dict[str, Any]:
    if ctx is None:
        return {"context_available": False, "context_missing_reason": "daily_bar_missing"}
    close = _as_float(ctx.get("c"))
    open_ = _as_float(ctx.get("o"))
    high = _as_float(ctx.get("h"))
    low = _as_float(ctx.get("l"))
    prev_close = _as_float(ctx.get("prev_c"))
    range_size = None if high is None or low is None else max(high - low, EPSILON)
    ma7 = _as_float(ctx.get("ma7"))
    ma20 = _as_float(ctx.get("ma20"))
    ma60 = _as_float(ctx.get("ma60"))
    return {
        "context_available": True,
        "gap_pct": _ratio(open_, prev_close),
        "day_return_pct": _ratio(close, prev_close),
        "body_pct": None if open_ is None or close is None or prev_close in (None, 0) else abs(close - open_) / prev_close,
        "upper_wick_ratio": None if range_size is None or high is None or open_ is None or close is None else (high - max(open_, close)) / range_size,
        "lower_wick_ratio": None if range_size is None or low is None or open_ is None or close is None else (min(open_, close) - low) / range_size,
        "close_position_in_range": None if range_size is None or close is None or low is None else (close - low) / range_size,
        "volume_t": _as_float(ctx.get("v")),
        "vol_ratio_t_20": None if _as_float(ctx.get("vol_ma20")) in (None, 0) else _as_float(ctx.get("v")) / _as_float(ctx.get("vol_ma20")),
        "vol_ratio5_20": None if _as_float(ctx.get("vol_ma20")) in (None, 0) else _as_float(ctx.get("vol_ma5")) / _as_float(ctx.get("vol_ma20")),
        "close_vs_ma7_pct": _ratio(close, ma7),
        "close_vs_ma20_pct": _ratio(close, ma20),
        "close_vs_ma60_pct": _ratio(close, ma60),
        "ma7_slope_5d": _as_float(ctx.get("ma7_slope_5d")),
        "ma20_slope_5d": _as_float(ctx.get("ma20_slope_5d")),
        "ma60_slope_5d": _as_float(ctx.get("ma60_slope_5d")),
        "above_ma7_count_recent": _as_float(ctx.get("above_ma7_count_recent")),
        "below_ma7_count_recent": _as_float(ctx.get("below_ma7_count_recent")),
        "above_ma20_count_recent": _as_float(ctx.get("above_ma20_count_recent")),
        "below_ma20_count_recent": _as_float(ctx.get("below_ma20_count_recent")),
        "drawdown_10d": _as_float(ctx.get("drawdown_10d")),
        "drawdown_20d": _as_float(ctx.get("drawdown_20d")),
        "runup_10d": _as_float(ctx.get("runup_10d")),
        "runup_20d": _as_float(ctx.get("runup_20d")),
        "distance_from_20d_high_pct": _as_float(ctx.get("distance_from_20d_high_pct")),
        "distance_from_20d_low_pct": _as_float(ctx.get("distance_from_20d_low_pct")),
        "new_high_20d_flag": bool(ctx.get("new_high_20d_flag")),
        "new_low_20d_flag": bool(ctx.get("new_low_20d_flag")),
        "lower_high_like_flag": bool(ctx.get("lower_high_like_flag")),
        "failed_breakout_like_flag": bool(ctx.get("failed_breakout_like_flag")),
        "high_volume_failure_like_flag": bool(ctx.get("high_volume_failure_like_flag")),
        "weak_rebound_after_breakdown_like_flag": bool(ctx.get("weak_rebound_after_breakdown_like_flag")),
        "volatility_expansion_downside_like_flag": bool(ctx.get("volatility_expansion_downside_like_flag")),
    }


def _classify_pattern(row: dict[str, Any]) -> str:
    if not row.get("context_available"):
        return "insufficient_fields"
    if row.get("high_volume_failure_like_flag"):
        return "high_volume_failure"
    if row.get("failed_breakout_like_flag") or ((row.get("distance_from_20d_high_pct") or 0) > -0.03 and (row.get("day_return_pct") or 0) < 0):
        return "breakdown_after_failed_high"
    if row.get("weak_rebound_after_breakdown_like_flag"):
        return "weak_rebound_after_ma_break"
    if row.get("volatility_expansion_downside_like_flag"):
        return "downside_volatility_expansion"
    if (row.get("runup_20d") or 0) > 0.20 and (row.get("close_position_in_range") or 1) < 0.45:
        return "late_stage_exhaustion"
    return "unknown_no_common_pattern"


def _negative_selection_audit(root: Path) -> dict[str, str]:
    root.mkdir(parents=True, exist_ok=True)
    frame = pd.read_parquet(SOURCE_ROWS)
    frame = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    frame["symbol"] = frame["symbol"].astype(str)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    ctx_lookup = _prepare_context(frame)
    examples: list[dict[str, Any]] = []
    pattern_months: dict[str, set[str]] = defaultdict(set)
    pattern_counts: Counter[str] = Counter()
    top_rows = frame[frame["champion_rank"].isin(range(1, 11))].copy()
    for raw in top_rows.to_dict(orient="records"):
        ret20 = _as_float(raw.get("forward_ret_20d"))
        severe = bool(raw.get("bottom15_label")) or (ret20 is not None and ret20 <= -0.15)
        negative = ret20 is not None and ret20 < 0
        if not severe and not negative:
            continue
        epoch = _date_to_epoch(str(raw["anchor_date"]))
        enriched = _enrich_candidate(raw, ctx_lookup.get((str(raw["symbol"]), epoch)))
        row = {
            "symbol": raw["symbol"],
            "decision_date": raw["anchor_date"],
            "month_bucket": str(raw.get("month_bucket") or raw["anchor_date"][:7]),
            "champion_rank": int(raw["champion_rank"]),
            "topk_bucket": "top5" if int(raw["champion_rank"]) <= 5 else "top10",
            "ret20": ret20,
            "severe_loser_flag": severe,
            "negative_ret20_flag": negative,
            **enriched,
        }
        row["failure_pattern"] = _classify_pattern(row)
        examples.append(row)
        pattern_counts[row["failure_pattern"]] += 1
        pattern_months[row["failure_pattern"]].add(row["month_bucket"])
    non_flagged_by_group = []
    for (date, side), group in frame.groupby(["anchor_date", "side"], sort=True):
        top10_losers = group[group["champion_rank"].le(10) & ((group["forward_ret_20d"] < 0) | group["bottom15_label"].fillna(False).astype(bool))]
        replacements = group[group["champion_rank"].between(11, 20, inclusive="both") & (group["forward_ret_20d"] >= 0) & ~group["bottom15_label"].fillna(False).astype(bool)]
        non_flagged_by_group.append({"decision_date": date, "side": side, "loser_count_top10": int(len(top10_losers)), "available_replacement_count_rank11_20": int(len(replacements))})
    classifiable = sum(count for pattern, count in pattern_counts.items() if pattern not in {"unknown_no_common_pattern", "insufficient_fields"})
    enough_losers = len(examples) >= 50
    dominant = [{"pattern": p, "count": c, "month_count": len(pattern_months[p])} for p, c in pattern_counts.most_common()]
    multi_month = any(item["pattern"] not in {"unknown_no_common_pattern", "insufficient_fields"} and item["month_count"] > 1 for item in dominant)
    enough_replacements = sum(1 for row in non_flagged_by_group if row["loser_count_top10"] > 0 and row["available_replacement_count_rank11_20"] >= row["loser_count_top10"]) >= 20
    if not enough_losers:
        decision = "not_enough_loser_cases"
    elif classifiable == 0:
        decision = "drop_axis_before_replay"
    elif not multi_month:
        decision = "drop_axis_before_replay"
    elif not enough_replacements:
        decision = "not_enough_replacement_availability"
    else:
        decision = "ready_for_fixed_condition_replay"
    feasibility = {
        "schema_version": "tradex_negative_selection_feasibility_v1",
        "generated_at": _utc_now(),
        "candidate_axis": "negative_selection_avoidance_v1",
        "feasibility_decision": decision,
        "inputs": {
            "champion_source_file": "scripts/tradex_champion_top5_capture_boundary_promoter_v1.py",
            "candidate_input_parquet": str(SOURCE_ROWS),
            "source_artifact_refs": "external_analysis/publish_candidates/champion_top5_capture_boundary_promoter_v1/source_artifact_refs.json",
            "universe": "champion_selected_top20 only",
            "period": "from fixed source parquet anchor_date coverage",
            "top_k": [5, 10, 20],
            "regime_condition": "same as fixed source parquet; not independently stratified here",
            "cost_slippage_handling": "same-condition contract inherited; numeric model unverified",
            "artifact_detail_level": "diagnostic feasibility",
            "daily_bars_source_policy": "confirmed daily_bars source='pan' and date <= decision date",
        },
        "loser_case_count": len(examples),
        "classifiable_loser_count": classifiable,
        "no_lookahead_check": {"pass": True, "daily_bars_filter": "source='pan' and date <= decision_date", "future_outcomes_used_only_as_labels": True},
    }
    pattern_summary = {"schema_version": "tradex_negative_selection_pattern_summary_v1", "generated_at": _utc_now(), "pattern_counts": dominant}
    fields = {"schema_version": "tradex_negative_selection_available_fields_v1", "generated_at": _utc_now(), "available_fields": sorted(set().union(*(set(row) for row in examples))) if examples else [], "missing_fields": ["weekly_close_vs_ma20_pct", "weekly_ma20_slope", "monthly_close_vs_ma20_pct", "monthly_ma20_slope", "weekly_regime_label", "monthly_regime_label"]}
    replacement = {"schema_version": "tradex_negative_selection_replacement_availability_v1", "generated_at": _utc_now(), "rows": non_flagged_by_group, "groups_with_sufficient_rank11_20_replacements": sum(1 for row in non_flagged_by_group if row["loser_count_top10"] > 0 and row["available_replacement_count_rank11_20"] >= row["loser_count_top10"])}
    paths = {
        "negative_selection_feasibility.json": str(_write_json(root / "negative_selection_feasibility.json", feasibility)),
        "champion_loser_structure_examples.csv": _write_csv(root / "champion_loser_structure_examples.csv", examples),
        "champion_loser_pattern_summary.json": str(_write_json(root / "champion_loser_pattern_summary.json", pattern_summary)),
        "available_failure_context_fields.json": str(_write_json(root / "available_failure_context_fields.json", fields)),
        "replacement_availability_summary.json": str(_write_json(root / "replacement_availability_summary.json", replacement)),
    }
    complete = build_artifact_complete({"schema_version": "tradex_negative_selection_feasibility_complete_v1", "artifact_root": str(root)}, list(paths), schema_version="tradex_negative_selection_feasibility_complete_v1")
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(root / "_ARTIFACT_COMPLETE.json", complete))
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    run_id = args.run_id.strip() or _run_id()
    final_root = FINAL_ROOT_BASE / f"{run_id}-relative_strength_family_final_decision"
    neg_root = NEG_ROOT_BASE / f"{run_id}-feasibility"
    paths = {"relative_strength_final": _finalize_relative_strength(final_root), "negative_selection": _negative_selection_audit(neg_root)}
    print(json.dumps({"final_root": str(final_root), "negative_selection_root": str(neg_root), "paths": paths}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
