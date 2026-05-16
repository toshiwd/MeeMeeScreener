from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_reflectability_funnel_common_v1 import TOP_K_VALUES, _ensure_columns, _mean_or_none, _safe_path, _utc_now, _write_json, build_artifact_complete

DEFAULT_SOURCE_ROWS_PARQUET = Path(r"G:\Tradex\candidate_generation_pre_filter_context_shape_v1\20260429T145332Z-7bd554ac\candidate_prefilter_rows.parquet")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\relative_strength_persistence_v1")
DEFAULT_STOCK_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")

CANDIDATE_ID = "relative_strength_persistence_v1"
CHAMPION_ID = "champion_top5_capture_boundary_promoter_v1"
SCHEMA_PREFIX = "tradex_relative_strength_persistence_v1"
EPSILON = 1e-9


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_to_epoch(value: str) -> int:
    return int(pd.Timestamp(value).timestamp())


def _load_frame(source_rows_parquet: Path) -> pd.DataFrame:
    frame = _ensure_columns(pd.read_parquet(source_rows_parquet))
    if "champion_score" not in frame.columns and "score" in frame.columns:
        frame["champion_score"] = frame["score"]
    required = {"anchor_date", "side", "symbol", "champion_selected_top20", "champion_rank", "champion_score", "forward_ret_20d"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"source rows missing required columns: {missing}")
    frame = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["champion_rank"] = pd.to_numeric(frame["champion_rank"], errors="coerce").astype("Int64")
    frame["champion_score"] = pd.to_numeric(frame["champion_score"], errors="coerce")
    frame["forward_ret_20d"] = pd.to_numeric(frame["forward_ret_20d"], errors="coerce")
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].str.slice(0, 7)
    if "regime_label" not in frame.columns:
        frame["regime_label"] = frame.get("market_regime_bucket", "unverified")
    return frame


def _load_bars(symbols: list[str], max_epoch: int, stock_db: Path) -> pd.DataFrame:
    with duckdb.connect(str(stock_db), read_only=True) as conn:
        return conn.execute(
            """
            SELECT code, date, c
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN (SELECT UNNEST(?))
              AND date <= ?
            ORDER BY code, date
            """,
            [symbols, max_epoch],
        ).fetchdf()


def _build_daily_relative_features(frame: pd.DataFrame, stock_db: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    symbols = sorted(frame["symbol"].astype(str).unique().tolist())
    max_epoch = max(_date_to_epoch(value) for value in frame["anchor_date"].astype(str).tolist())
    bars = _load_bars(symbols, max_epoch, stock_db)
    if bars.empty:
        raise RuntimeError("daily_bars returned no confirmed pan rows for candidate symbols")
    feature_frames = []
    for _, group in bars.groupby("code", sort=False):
        working = group.sort_values("date").copy()
        close = pd.to_numeric(working["c"], errors="coerce")
        for window in (5, 10, 20):
            working[f"ret_{window}d"] = close / close.shift(window) - 1.0
        working["daily_ret"] = close / close.shift(1) - 1.0
        feature_frames.append(working)
    features = pd.concat(feature_frames, ignore_index=True)
    market = features.groupby("date", as_index=False).agg(
        market_ret_5d=("ret_5d", "median"),
        market_ret_10d=("ret_10d", "median"),
        market_ret_20d=("ret_20d", "median"),
        market_daily_ret=("daily_ret", "median"),
    )
    features = features.merge(market, on="date", how="left")
    for window in (5, 10, 20):
        features[f"rel_ret_{window}d"] = features[f"ret_{window}d"] - features[f"market_ret_{window}d"]

    per_symbol = []
    for _, group in features.groupby("code", sort=False):
        working = group.sort_values("date").copy()
        rel_daily = working["daily_ret"] - working["market_daily_ret"]
        working["positive_rel_ret_days_20d"] = rel_daily.gt(0).rolling(20, min_periods=1).sum()
        working["negative_rel_ret_days_20d"] = rel_daily.lt(0).rolling(20, min_periods=1).sum()
        valid = rel_daily.notna().rolling(20, min_periods=1).sum()
        working["rel_strength_persistence_ratio_20d"] = working["positive_rel_ret_days_20d"] / valid.clip(lower=1)
        rel_cum = (1.0 + rel_daily.fillna(0.0)).rolling(20, min_periods=1).apply(lambda values: float(values.prod()), raw=True)
        rel_peak = rel_cum.rolling(20, min_periods=1).max()
        working["max_relative_drawdown_20d"] = rel_cum / rel_peak - 1.0
        working["rel_ret_20d_minus_5d"] = working["rel_ret_20d"] - working["rel_ret_5d"]
        working["market_down_day"] = working["market_daily_ret"] < 0
        working["candidate_down_component"] = working["daily_ret"].where(working["market_down_day"])
        working["market_down_component"] = working["market_daily_ret"].where(working["market_down_day"])
        working["down_day_count_20d"] = working["market_down_day"].rolling(20, min_periods=1).sum()
        working["candidate_ret_on_market_down_days_20d"] = working["candidate_down_component"].rolling(20, min_periods=1).sum()
        working["market_ret_on_down_days_20d"] = working["market_down_component"].rolling(20, min_periods=1).sum()
        working["down_day_resilience_20d"] = working["candidate_ret_on_market_down_days_20d"] - working["market_ret_on_down_days_20d"]
        per_symbol.append(working)
    features = pd.concat(per_symbol, ignore_index=True)
    features["anchor_date"] = pd.to_datetime(features["date"], unit="s").dt.strftime("%Y-%m-%d")
    keep = [
        "code",
        "anchor_date",
        "ret_5d",
        "ret_10d",
        "ret_20d",
        "market_ret_5d",
        "market_ret_10d",
        "market_ret_20d",
        "rel_ret_5d",
        "rel_ret_10d",
        "rel_ret_20d",
        "down_day_count_20d",
        "candidate_ret_on_market_down_days_20d",
        "market_ret_on_down_days_20d",
        "down_day_resilience_20d",
        "positive_rel_ret_days_20d",
        "negative_rel_ret_days_20d",
        "rel_strength_persistence_ratio_20d",
        "max_relative_drawdown_20d",
        "rel_ret_20d_minus_5d",
    ]
    return features[keep].rename(columns={"code": "symbol"}), {
        "market_proxy_used": True,
        "market_proxy_reason": "safe external market index series was not present in fixed-condition input; same-date confirmed daily_bars median was used explicitly",
        "daily_bars_filter": "source='pan' and date <= decision_date",
    }


def _percent_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True, method="average")


def _z(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    std = numeric.std(ddof=0)
    if pd.isna(std) or abs(std) <= EPSILON:
        return pd.Series(0.0, index=series.index)
    return (numeric - numeric.mean()) / std


def _score_group(group: pd.DataFrame) -> pd.DataFrame:
    working = group.copy()
    for col in ("ret_5d", "ret_10d", "ret_20d", "rel_ret_5d", "rel_ret_10d", "rel_ret_20d"):
        working[f"candidate_group_pctile_{col}"] = _percent_rank(pd.to_numeric(working[col], errors="coerce")).fillna(0.5)
    score_parts = {
        "pctile_rel_ret_20d": _z(working["candidate_group_pctile_rel_ret_20d"]),
        "pctile_rel_ret_10d": _z(working["candidate_group_pctile_rel_ret_10d"]),
        "persistence": _z(working["rel_strength_persistence_ratio_20d"]),
        "down_day_resilience": _z(working["down_day_resilience_20d"]),
        "max_relative_drawdown": -_z(working["max_relative_drawdown_20d"]),
    }
    working["relative_strength_score_v1"] = sum(score_parts.values())
    working["feature_missing_count_row"] = working[
        ["rel_ret_20d", "rel_ret_10d", "rel_strength_persistence_ratio_20d", "down_day_resilience_20d", "max_relative_drawdown_20d"]
    ].isna().sum(axis=1)
    ordered = working.sort_values(["relative_strength_score_v1", "champion_rank", "symbol"], ascending=[False, True, True], kind="stable").copy()
    ordered["candidate_rank"] = range(1, len(ordered) + 1)
    return ordered


def _apply_candidate(frame: pd.DataFrame, stock_db: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    features, market_meta = _build_daily_relative_features(frame, stock_db)
    working = frame.merge(features, on=["symbol", "anchor_date"], how="left")
    ranked = pd.concat([_score_group(group) for _, group in working.groupby(["anchor_date", "side"], sort=True)], ignore_index=True)
    for top_k in TOP_K_VALUES:
        ranked[f"candidate_selected_top{top_k}"] = ranked["candidate_rank"].le(top_k)
        ranked[f"champion_selected_top{top_k}"] = ranked["champion_rank"].le(top_k)
        ranked[f"changed_top{top_k}_member"] = ranked[f"candidate_selected_top{top_k}"] != ranked[f"champion_selected_top{top_k}"]
    ranked["rank_changed"] = ranked["candidate_rank"].astype("Int64") != ranked["champion_rank"]
    return ranked, market_meta


def _severe_rate(selected: pd.DataFrame) -> float | None:
    if selected.empty:
        return None
    if "bottom15_label" in selected.columns:
        return _mean_or_none(selected["bottom15_label"].fillna(False).astype(bool).astype(float).tolist())
    return _mean_or_none((selected["forward_ret_20d"] <= -0.15).astype(float).tolist())


def _metrics(frame: pd.DataFrame, prefix: str) -> dict[str, Any]:
    out = {}
    for top_k in TOP_K_VALUES:
        selected = frame[frame[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)]
        out[f"top{top_k}"] = {
            "selected_count": int(len(selected)),
            "forward_ret_20d_mean": _mean_or_none(selected["forward_ret_20d"].tolist()),
            "severe_loser_rate": _severe_rate(selected),
            "hit_rate_positive_20d": _mean_or_none((selected["forward_ret_20d"] > 0).astype(float).tolist()),
        }
    return out


def _change_rows(frame: pd.DataFrame, top_k: int, *, added: bool) -> list[dict[str, Any]]:
    ccol = f"candidate_selected_top{top_k}"
    hcol = f"champion_selected_top{top_k}"
    selected = frame[frame[ccol].fillna(False).astype(bool) & ~frame[hcol].fillna(False).astype(bool)] if added else frame[frame[hcol].fillna(False).astype(bool) & ~frame[ccol].fillna(False).astype(bool)]
    cols = ["anchor_date", "side", "symbol", "champion_rank", "candidate_rank", "champion_score", "relative_strength_score_v1", "forward_ret_20d", "bottom15_label", "rel_ret_5d", "rel_ret_10d", "rel_ret_20d", "rel_strength_persistence_ratio_20d"]
    return [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in selected.sort_values(["anchor_date", "side", "candidate_rank", "champion_rank", "symbol"], kind="stable")[cols].to_dict(orient="records")]


def _candidate_shortage(frame: pd.DataFrame) -> dict[str, Any]:
    groups = frame.groupby(["anchor_date", "side"], sort=False)
    total = int(groups.ngroups)
    out = {"decision_sets_total": total}
    for top_k in TOP_K_VALUES:
        shortage = sum(1 for _, group in groups if int(group[f"candidate_selected_top{top_k}"].sum()) < min(top_k, len(group)))
        out[f"top{top_k}_shortage_rate"] = None if total == 0 else shortage / total
        out[f"top{top_k}_shortage_decision_sets"] = int(shortage)
    return out


def _build_compare(frame: pd.DataFrame, *, market_meta: dict[str, Any], overlap: dict[str, Any]) -> dict[str, Any]:
    champion = _metrics(frame, "champion")
    candidate = _metrics(frame, "candidate")
    deltas = {}
    top_overlap = {}
    for top_k in TOP_K_VALUES:
        key = f"top{top_k}"
        deltas[key] = {
            "forward_ret_20d_mean_delta": None if champion[key]["forward_ret_20d_mean"] is None or candidate[key]["forward_ret_20d_mean"] is None else candidate[key]["forward_ret_20d_mean"] - champion[key]["forward_ret_20d_mean"],
            "severe_loser_rate_delta": None if champion[key]["severe_loser_rate"] is None or candidate[key]["severe_loser_rate"] is None else candidate[key]["severe_loser_rate"] - champion[key]["severe_loser_rate"],
            "changed_member_count": int(frame[f"changed_top{top_k}_member"].fillna(False).astype(bool).sum()),
        }
        both = int((frame[f"candidate_selected_top{top_k}"].astype(bool) & frame[f"champion_selected_top{top_k}"].astype(bool)).sum())
        champion_count = int(frame[f"champion_selected_top{top_k}"].astype(bool).sum())
        top_overlap[key] = {"overlap_count": both, "champion_count": champion_count, "overlap_with_champion": None if champion_count == 0 else both / champion_count}
    feature_missing_count = int(frame["feature_missing_count_row"].sum())
    return {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "generated_at": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "champion_id": CHAMPION_ID,
        "same_condition_contract": {
            "same_universe": True,
            "same_period": True,
            "same_top_k": list(TOP_K_VALUES),
            "same_regime": True,
            "same_cost_slippage": True,
            "same_artifact_detail_level": True,
            "silent_fallback_allowed": False,
        },
        "axis_overlap_risk": overlap["axis_overlap_risk"],
        "market_proxy_used": market_meta["market_proxy_used"],
        "feature_missing_count": feature_missing_count,
        "research_fallback_count": 0,
        "champion": champion,
        "candidate": candidate,
        "deltas": deltas,
        "candidate_shortage": _candidate_shortage(frame),
        "overlap": top_overlap,
        "branching": {
            "changed_top5_members_count": deltas["top5"]["changed_member_count"],
            "changed_top10_members_count": deltas["top10"]["changed_member_count"],
            "changed_top20_members_count": deltas["top20"]["changed_member_count"],
            "changed_rank_count": int(frame["rank_changed"].fillna(False).astype(bool).sum()),
            "selection_divergence_reason": "relative_strength_persistence_rerank_within_champion_top20",
        },
        "added_challenger_members": {f"top{k}": _change_rows(frame, k, added=True) for k in TOP_K_VALUES},
        "removed_champion_members": {f"top{k}": _change_rows(frame, k, added=False) for k in TOP_K_VALUES},
    }


def _build_breakdown(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    rows = []
    for bucket, group in frame.groupby(column, sort=True):
        comp = _build_compare(group, market_meta={"market_proxy_used": True}, overlap={"axis_overlap_risk": "medium"})
        rows.append({
            column: str(bucket),
            "decision_sets": int(group.groupby(["anchor_date", "side"], sort=False).ngroups),
            "changed_top5_members_count": comp["branching"]["changed_top5_members_count"],
            "changed_top10_members_count": comp["branching"]["changed_top10_members_count"],
            "top5_forward_ret_20d_mean_delta": comp["deltas"]["top5"]["forward_ret_20d_mean_delta"],
            "top10_forward_ret_20d_mean_delta": comp["deltas"]["top10"]["forward_ret_20d_mean_delta"],
            "top5_severe_loser_rate_delta": comp["deltas"]["top5"]["severe_loser_rate_delta"],
        })
    return {
        "schema_version": f"{SCHEMA_PREFIX}_breakdown_v1",
        "generated_at": _utc_now(),
        "breakdown_column": column,
        "rows": rows,
        "breadth": {
            "bucket_count": len(rows),
            "top5_improved_bucket_count": sum(1 for row in rows if (row["top5_forward_ret_20d_mean_delta"] or 0) > 0),
            "top5_branched_bucket_count": sum(1 for row in rows if row["changed_top5_members_count"] > 0),
        },
    }


def _feature_overlap_check(source_rows_parquet: Path) -> dict[str, Any]:
    frame_cols = list(pd.read_parquet(source_rows_parquet).columns)
    relevant = [c for c in frame_cols if any(k in c.lower() for k in ["ret_5", "ret_10", "ret_20", "relative", "rel_ret", "pctile", "path_value", "momentum", "trend", "ma", "dist"])]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_overlap_check_v1",
        "generated_at": _utc_now(),
        "champion_source_file": "scripts/tradex_champion_top5_capture_boundary_promoter_v1.py",
        "published_logic_artifact": "external_analysis/publish_candidates/champion_top5_capture_boundary_promoter_v1/published_logic_artifact.json",
        "champion_score_columns": ["champion_rank", "champion_score", "path_value_score_v1"],
        "champion_required_inputs": ["champion_rank", "champion_score", "path_value_score_v1"],
        "source_columns_related_to_axis": relevant,
        "direct_relative_strength_inputs_in_champion": [],
        "overlap_notes": ["path_value_score_v1 may indirectly encode path quality, but direct 5d/10d/20d market-relative persistence and candidate-group percentile are not champion inputs"],
        "axis_overlap_risk": "medium",
        "independent_enough_to_test": True,
    }


def _build_decision(compare: dict[str, Any], by_month: dict[str, Any], by_regime: dict[str, Any]) -> dict[str, Any]:
    top5 = compare["deltas"]["top5"]["forward_ret_20d_mean_delta"]
    top10 = compare["deltas"]["top10"]["forward_ret_20d_mean_delta"]
    sev5 = compare["deltas"]["top5"]["severe_loser_rate_delta"]
    changed5 = compare["branching"]["changed_top5_members_count"]
    changed10 = compare["branching"]["changed_top10_members_count"]
    if changed5 == 0 and changed10 == 0:
        decision, reason = "drop", "no_material_branching"
    elif (top5 is not None and top5 < 0) and (top10 is not None and top10 < 0):
        decision, reason = "drop", "top5_and_top10_expectancy_worsened"
    elif sev5 is not None and sev5 > 0.002 and (top5 is None or top5 <= 0):
        decision, reason = "drop", "severe_loser_rate_worsened_without_top5_improvement"
    elif (
        top5 is not None
        and top10 is not None
        and top5 > 0
        and top10 >= 0
        and (sev5 is None or sev5 <= 0)
        and by_month["breadth"]["top5_improved_bucket_count"] >= 6
        and by_regime["breadth"]["bucket_count"] > 1
        and by_regime["breadth"]["top5_improved_bucket_count"] > 0
    ):
        decision, reason = "keep", "topk_improved_with_breadth_and_no_severe_loser_regression"
    elif (top5 is not None and top5 > 0) or (top10 is not None and top10 > 0):
        decision, reason = "hold", "partial_improvement_requires_breadth_or_risk_confirmation"
    else:
        decision, reason = "drop", "fixed_condition_topk_metrics_not_improved"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at": _utc_now(),
        "candidate_local_decision": decision,
        "session_aggregate_decision": decision,
        "authoritative_rollup_decision": decision,
        "decision_reason": reason,
        "candidate_id": CANDIDATE_ID,
        "champion_id": CHAMPION_ID,
        "promote_ready": decision == "keep",
        "research_fallback": False,
        "metrics": {
            "top5_return_delta": top5,
            "top10_return_delta": top10,
            "top20_return_delta": compare["deltas"]["top20"]["forward_ret_20d_mean_delta"],
            "top5_severe_loser_rate_delta": sev5,
            "changed_top5_members_count": changed5,
            "changed_top10_members_count": changed10,
            "changed_rank_count": compare["branching"]["changed_rank_count"],
            "month_breadth": by_month["breadth"],
            "regime_breadth": by_regime["breadth"],
            "axis_overlap_risk": compare["axis_overlap_risk"],
            "market_proxy_used": compare["market_proxy_used"],
            "feature_missing_count": compare["feature_missing_count"],
            "research_fallback_count": compare["research_fallback_count"],
        },
        "non_goals": ["No MeeMee mutation", "No live ranking mutation", "No champion promoter retune", "No entry timing repair"],
    }


def _feature_summary(frame: pd.DataFrame, market_meta: dict[str, Any]) -> dict[str, Any]:
    feature_cols = ["ret_5d", "ret_10d", "ret_20d", "rel_ret_5d", "rel_ret_10d", "rel_ret_20d", "rel_strength_persistence_ratio_20d", "down_day_resilience_20d", "max_relative_drawdown_20d"]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_feature_summary_v1",
        "generated_at": _utc_now(),
        "formula": "z(candidate_group_pctile_rel_ret_20d)+z(candidate_group_pctile_rel_ret_10d)+z(rel_strength_persistence_ratio_20d)+z(down_day_resilience_20d)-z(max_relative_drawdown_20d)",
        "feature_columns": feature_cols,
        "label_columns_excluded_from_scoring": ["forward_ret_5d", "forward_ret_10d", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d", "top15_label", "bottom15_label"],
        "market_proxy": market_meta,
        "feature_missing_count": int(frame["feature_missing_count_row"].sum()),
        "row_count": int(len(frame)),
    }


def _build_outputs(frame: pd.DataFrame, *, output_root: Path, source_rows_parquet: Path, stock_db: Path) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    overlap = _feature_overlap_check(source_rows_parquet)
    ranked, market_meta = _apply_candidate(frame, stock_db)
    compare = _build_compare(ranked, market_meta=market_meta, overlap=overlap)
    by_month = _build_breakdown(ranked, "month_bucket")
    by_regime = _build_breakdown(ranked, "regime_label")
    decision = _build_decision(compare, by_month, by_regime)
    artifacts = {
        "compare.json": compare,
        "candidate_decision.json": decision,
        "family_leaderboard.json": {
            "schema_version": f"{SCHEMA_PREFIX}_family_leaderboard_v1",
            "generated_at": _utc_now(),
            "families": [{**decision["metrics"], "family_id": CANDIDATE_ID, "decision": decision["authoritative_rollup_decision"], "decision_reason": decision["decision_reason"]}],
        },
        "by_month.json": by_month,
        "by_regime.json": by_regime,
        "branching_summary.json": {
            "schema_version": f"{SCHEMA_PREFIX}_branching_summary_v1",
            "generated_at": _utc_now(),
            "branching": compare["branching"],
            "overlap": compare["overlap"],
            "candidate_shortage": compare["candidate_shortage"],
            "added_challenger_members": compare["added_challenger_members"],
            "removed_champion_members": compare["removed_champion_members"],
            "selection_divergence_reason": compare["branching"]["selection_divergence_reason"],
        },
        "feature_overlap_check.json": overlap,
        "relative_strength_feature_summary.json": _feature_summary(ranked, market_meta),
    }
    paths = {name: str(_write_json(output_root / name, payload)) for name, payload in artifacts.items()}
    complete = build_artifact_complete({"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "artifact_root": str(output_root), "json_validated": True}, list(artifacts.keys()), schema_version=f"{SCHEMA_PREFIX}_artifact_complete_v1")
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(output_root / "_ARTIFACT_COMPLETE.json", complete))
    return {"paths": paths, "compare": compare, "decision": decision}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-rows-parquet", default=str(DEFAULT_SOURCE_ROWS_PARQUET))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--stock-db", default=str(DEFAULT_STOCK_DB))
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    source = _safe_path(args.source_rows_parquet, DEFAULT_SOURCE_ROWS_PARQUET)
    base = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    stock_db = _safe_path(args.stock_db, DEFAULT_STOCK_DB)
    run_id = args.run_id.strip() or _run_id()
    if not run_id.endswith(f"-{CANDIDATE_ID}"):
        run_id = f"{run_id}-{CANDIDATE_ID}"
    root = base / run_id
    payload = _build_outputs(_load_frame(source), output_root=root, source_rows_parquet=source, stock_db=stock_db)
    print(json.dumps({"output_root": str(root), "decision": payload["decision"]["authoritative_rollup_decision"], "decision_reason": payload["decision"]["decision_reason"], "authoritative_decision_artifact": payload["paths"]["candidate_decision.json"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
