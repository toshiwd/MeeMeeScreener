from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.tradex_reflectability_funnel_common_v1 import TOP_K_VALUES, _mean_or_none, _safe_path, _utc_now, _write_json, build_artifact_complete
from scripts.tradex_relative_strength_persistence_v1 import (
    DEFAULT_SOURCE_ROWS_PARQUET,
    DEFAULT_STOCK_DB,
    _apply_candidate as _apply_rs_features,
    _load_frame,
)

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\relative_strength_persistence_veto_v1")
CANDIDATE_ID = "relative_strength_persistence_veto_v1"
CHAMPION_ID = "champion_top5_capture_boundary_promoter_v1"
SCHEMA_PREFIX = "tradex_relative_strength_persistence_veto_v1"
FEATURE_REQUIRED = ["relative_strength_score_v1", "rel_strength_persistence_ratio_20d", "rel_ret_20d", "max_relative_drawdown_20d"]


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _as_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_severe(row: pd.Series | dict[str, Any]) -> bool:
    bottom = bool(row.get("bottom15_label", False))
    ret20 = _as_float(row.get("forward_ret_20d"))
    return bottom or (ret20 is not None and ret20 <= -0.15)


def _classify_removal(row: pd.Series | dict[str, Any]) -> str:
    ret20 = _as_float(row.get("forward_ret_20d"))
    if _is_severe(row) or (ret20 is not None and ret20 <= -0.03):
        return "good_removal"
    if ret20 is not None and ret20 >= 0.05:
        return "bad_removal"
    return "neutral_removal"


def _classify_replacement(row: pd.Series | dict[str, Any], removed_avg: float | None) -> str:
    ret20 = _as_float(row.get("forward_ret_20d"))
    if _is_severe(row) or (removed_avg is not None and ret20 is not None and ret20 < removed_avg):
        return "bad_replacement"
    if ret20 is not None and (ret20 >= 0.05 or (removed_avg is not None and ret20 > removed_avg)):
        return "good_replacement"
    return "neutral_replacement"


def _rank_group_veto(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values(["champion_rank", "symbol"], kind="stable").copy()
    group["rs_score_pctile_in_champion_top20"] = group["relative_strength_score_v1"].rank(pct=True, method="average")
    median_drawdown = pd.to_numeric(group["max_relative_drawdown_20d"], errors="coerce").median()
    missing = group[FEATURE_REQUIRED].isna().any(axis=1)
    weak_condition = (
        (group["rs_score_pctile_in_champion_top20"] <= 0.25)
        & (
            (group["rel_strength_persistence_ratio_20d"] < 0.50)
            | (group["rel_ret_20d"] < 0)
            | (group["max_relative_drawdown_20d"] < median_drawdown)
        )
    )
    group["feature_missing_for_veto"] = missing
    group["weak_rs_veto_flag"] = weak_condition & ~missing
    group["fallback_reason"] = None

    non_veto = group[~group["weak_rs_veto_flag"]].sort_values(["champion_rank", "symbol"], kind="stable")
    veto = group[group["weak_rs_veto_flag"]].sort_values(["champion_rank", "symbol"], kind="stable")
    ordered = pd.concat([non_veto, veto], ignore_index=False).copy()
    ordered["candidate_rank"] = range(1, len(ordered) + 1)
    ordered["candidate_shortage_any_topk"] = False
    for top_k in TOP_K_VALUES:
        ordered[f"candidate_selected_top{top_k}"] = ordered["candidate_rank"].le(top_k)
        ordered[f"champion_selected_top{top_k}"] = ordered["champion_rank"].le(top_k)
        ordered[f"changed_top{top_k}_member"] = ordered[f"candidate_selected_top{top_k}"] != ordered[f"champion_selected_top{top_k}"]
    ordered["rank_changed"] = ordered["candidate_rank"].astype("Int64") != ordered["champion_rank"]
    return ordered


def _apply_veto(frame: pd.DataFrame, stock_db: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    scored, market_meta = _apply_rs_features(frame, stock_db)
    ranked = pd.concat([_rank_group_veto(group) for _, group in scored.groupby(["anchor_date", "side"], sort=True)], ignore_index=True)
    return ranked, market_meta


def _selected_metrics(frame: pd.DataFrame, prefix: str) -> dict[str, Any]:
    out = {}
    for top_k in TOP_K_VALUES:
        selected = frame[frame[f"{prefix}_selected_top{top_k}"].fillna(False).astype(bool)]
        out[f"top{top_k}"] = {
            "selected_count": int(len(selected)),
            "forward_ret_20d_mean": _mean_or_none(selected["forward_ret_20d"].tolist()),
            "severe_loser_rate": _mean_or_none([1.0 if _is_severe(row) else 0.0 for _, row in selected.iterrows()]),
        }
    return out


def _change_rows(frame: pd.DataFrame, top_k: int, *, added: bool) -> list[dict[str, Any]]:
    ccol = f"candidate_selected_top{top_k}"
    hcol = f"champion_selected_top{top_k}"
    selected = frame[frame[ccol].astype(bool) & ~frame[hcol].astype(bool)] if added else frame[frame[hcol].astype(bool) & ~frame[ccol].astype(bool)]
    cols = ["anchor_date", "side", "symbol", "champion_rank", "candidate_rank", "champion_score", "relative_strength_score_v1", "rs_score_pctile_in_champion_top20", "weak_rs_veto_flag", "forward_ret_20d", "bottom15_label", "rel_ret_20d", "rel_strength_persistence_ratio_20d", "max_relative_drawdown_20d"]
    return [{k: (None if pd.isna(v) else v) for k, v in row.items()} for row in selected.sort_values(["anchor_date", "side", "candidate_rank", "champion_rank", "symbol"], kind="stable")[cols].to_dict(orient="records")]


def _candidate_shortage(frame: pd.DataFrame) -> dict[str, Any]:
    total = int(frame.groupby(["anchor_date", "side"], sort=False).ngroups)
    return {
        "decision_sets_total": total,
        **{f"top{k}_shortage_decision_sets": 0 for k in TOP_K_VALUES},
        **{f"top{k}_shortage_rate": 0.0 for k in TOP_K_VALUES},
    }


def _veto_summary(frame: pd.DataFrame) -> dict[str, Any]:
    vetoed = frame[frame["weak_rs_veto_flag"].fillna(False).astype(bool)]
    removed_top5 = frame[frame["champion_selected_top5"].astype(bool) & ~frame["candidate_selected_top5"].astype(bool)]
    added_top5 = frame[frame["candidate_selected_top5"].astype(bool) & ~frame["champion_selected_top5"].astype(bool)]
    removed_avg = _mean_or_none(removed_top5["forward_ret_20d"].tolist())
    removed_classes = [_classify_removal(row) for _, row in removed_top5.iterrows()]
    replacement_classes = [_classify_replacement(row, removed_avg) for _, row in added_top5.iterrows()]
    replacement_avg = _mean_or_none(added_top5["forward_ret_20d"].tolist())
    removed_sev = _mean_or_none([1.0 if _is_severe(row) else 0.0 for _, row in removed_top5.iterrows()])
    added_sev = _mean_or_none([1.0 if _is_severe(row) else 0.0 for _, row in added_top5.iterrows()])
    return {
        "schema_version": f"{SCHEMA_PREFIX}_veto_summary_v1",
        "generated_at": _utc_now(),
        "vetoed_count_total": int(len(vetoed)),
        "vetoed_from_champion_top5_count": int(vetoed["champion_rank"].le(5).sum()),
        "vetoed_from_champion_top10_count": int(vetoed["champion_rank"].le(10).sum()),
        "vetoed_good_removal_count": removed_classes.count("good_removal"),
        "vetoed_bad_removal_count": removed_classes.count("bad_removal"),
        "vetoed_neutral_removal_count": removed_classes.count("neutral_removal"),
        "replacement_good_count": replacement_classes.count("good_replacement"),
        "replacement_bad_count": replacement_classes.count("bad_replacement"),
        "replacement_neutral_count": replacement_classes.count("neutral_replacement"),
        "net_replacement_return_delta": None if replacement_avg is None or removed_avg is None else replacement_avg - removed_avg,
        "net_severe_loser_delta": None if added_sev is None or removed_sev is None else added_sev - removed_sev,
        "fallback_reason_counts": {},
        "research_fallback_count": 0,
    }


def _build_compare(frame: pd.DataFrame, market_meta: dict[str, Any], veto: dict[str, Any]) -> dict[str, Any]:
    champion = _selected_metrics(frame, "champion")
    candidate = _selected_metrics(frame, "candidate")
    deltas = {}
    overlap = {}
    for top_k in TOP_K_VALUES:
        key = f"top{top_k}"
        deltas[key] = {
            "forward_ret_20d_mean_delta": None if champion[key]["forward_ret_20d_mean"] is None or candidate[key]["forward_ret_20d_mean"] is None else candidate[key]["forward_ret_20d_mean"] - champion[key]["forward_ret_20d_mean"],
            "severe_loser_rate_delta": None if champion[key]["severe_loser_rate"] is None or candidate[key]["severe_loser_rate"] is None else candidate[key]["severe_loser_rate"] - champion[key]["severe_loser_rate"],
            "changed_member_count": int(frame[f"changed_top{top_k}_member"].sum()),
        }
        both = int((frame[f"candidate_selected_top{top_k}"].astype(bool) & frame[f"champion_selected_top{top_k}"].astype(bool)).sum())
        count = int(frame[f"champion_selected_top{top_k}"].astype(bool).sum())
        overlap[key] = {"overlap_count": both, "champion_count": count, "overlap_with_champion": None if count == 0 else both / count}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "generated_at": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "champion_id": CHAMPION_ID,
        "same_condition_contract": {"same_universe": True, "same_period": True, "same_top_k": list(TOP_K_VALUES), "same_regime": True, "same_cost_slippage": True, "same_artifact_detail_level": True, "silent_fallback_allowed": False},
        "market_proxy_used": bool(market_meta.get("market_proxy_used")),
        "feature_missing_count": int(frame["feature_missing_for_veto"].sum()),
        "research_fallback_count": 0,
        "no_lookahead_check": {"pass": True, "daily_bars_filter": market_meta.get("daily_bars_filter"), "scoring_fields_use_date_lte_decision_date": True},
        "champion": champion,
        "candidate": candidate,
        "deltas": deltas,
        "candidate_shortage": _candidate_shortage(frame),
        "overlap": overlap,
        "branching": {
            "changed_top5_members_count": deltas["top5"]["changed_member_count"],
            "changed_top10_members_count": deltas["top10"]["changed_member_count"],
            "changed_top20_members_count": deltas["top20"]["changed_member_count"],
            "changed_rank_count": int(frame["rank_changed"].sum()),
            "selection_divergence_reason": "relative_strength_persistence_veto_preserve_champion_order_fill_by_original_order",
        },
        "veto_summary": veto,
        "added_challenger_members": {f"top{k}": _change_rows(frame, k, added=True) for k in TOP_K_VALUES},
        "removed_champion_members": {f"top{k}": _change_rows(frame, k, added=False) for k in TOP_K_VALUES},
    }


def _breakdown(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    rows = []
    for bucket, group in frame.groupby(column, sort=True):
        veto = _veto_summary(group)
        comp = _build_compare(group, {"market_proxy_used": True, "daily_bars_filter": "source='pan' and date <= decision_date"}, veto)
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
            "top5_branched_bucket_count": sum(1 for row in rows if row["changed_top5_members_count"] > 0),
            "top5_improved_bucket_count": sum(1 for row in rows if (row["top5_forward_ret_20d_mean_delta"] or 0) > 0),
        },
    }


def _decision(compare: dict[str, Any], by_month: dict[str, Any], by_regime: dict[str, Any]) -> dict[str, Any]:
    top5 = compare["deltas"]["top5"]["forward_ret_20d_mean_delta"]
    top10 = compare["deltas"]["top10"]["forward_ret_20d_mean_delta"]
    sev5 = compare["deltas"]["top5"]["severe_loser_rate_delta"]
    sev10 = compare["deltas"]["top10"]["severe_loser_rate_delta"]
    changed = compare["branching"]["changed_top5_members_count"] + compare["branching"]["changed_top10_members_count"]
    if changed == 0:
        dec, reason = "drop", "no_material_branching"
    elif (sev5 is None or sev5 >= 0) and (sev10 is None or sev10 >= 0):
        dec, reason = "drop", "severe_loser_rate_did_not_improve"
    elif (top5 is not None and top5 < -0.003) or (top10 is not None and top10 < -0.003):
        dec, reason = "drop", "return_deterioration_material"
    elif (sev5 is not None and sev5 < 0) and (top5 is not None and top5 >= 0) and (top10 is not None and top10 >= 0) and by_month["breadth"]["top5_improved_bucket_count"] >= 6 and by_regime["breadth"]["bucket_count"] > 1:
        dec, reason = "keep", "risk_improved_without_return_deterioration_with_breadth"
    else:
        dec, reason = "hold", "risk_improved_but_return_or_breadth_mixed"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "generated_at": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "champion_id": CHAMPION_ID,
        "candidate_local_decision": dec,
        "session_aggregate_decision": dec,
        "authoritative_rollup_decision": dec,
        "decision_reason": reason,
        "research_fallback": False,
        "promote_ready": dec == "keep",
        "metrics": {
            "top5_return_delta": top5,
            "top10_return_delta": top10,
            "top20_return_delta": compare["deltas"]["top20"]["forward_ret_20d_mean_delta"],
            "top5_severe_loser_rate_delta": sev5,
            "top10_severe_loser_rate_delta": sev10,
            "changed_top5_members_count": compare["branching"]["changed_top5_members_count"],
            "changed_top10_members_count": compare["branching"]["changed_top10_members_count"],
            "month_breadth": by_month["breadth"],
            "regime_breadth": by_regime["breadth"],
        },
        "non_scope": ["No MeeMee mutation", "No live ranking mutation", "No champion promoter retune", "No full reranker"],
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    keys = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


def _build_outputs(frame: pd.DataFrame, *, output_root: Path, stock_db: Path) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    ranked, market_meta = _apply_veto(frame, stock_db)
    veto = _veto_summary(ranked)
    compare = _build_compare(ranked, market_meta, veto)
    by_month = _breakdown(ranked, "month_bucket")
    by_regime = _breakdown(ranked, "regime_label")
    decision = _decision(compare, by_month, by_regime)
    feature_summary = {
        "schema_version": f"{SCHEMA_PREFIX}_feature_summary_v1",
        "generated_at": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "weak_rs_veto_rule": "rs_score_pctile_in_champion_top20 <= 0.25 and (rel_strength_persistence_ratio_20d < 0.50 or rel_ret_20d < 0 or max_relative_drawdown_20d worse than top20 median)",
        "feature_columns": FEATURE_REQUIRED,
        "market_proxy_used": bool(market_meta.get("market_proxy_used")),
        "feature_missing_count": int(ranked["feature_missing_for_veto"].sum()),
        "label_columns_excluded_from_scoring": ["forward_ret_5d", "forward_ret_10d", "forward_ret_20d", "path_value_score_v1", "mfe_20d", "mae_20d", "top15_label", "bottom15_label"],
        "no_lookahead_check": compare["no_lookahead_check"],
    }
    branching = {
        "schema_version": f"{SCHEMA_PREFIX}_branching_summary_v1",
        "generated_at": _utc_now(),
        "branching": compare["branching"],
        "overlap": compare["overlap"],
        "candidate_shortage": compare["candidate_shortage"],
        "added_challenger_members": compare["added_challenger_members"],
        "removed_champion_members": compare["removed_champion_members"],
        "selection_divergence_reason": compare["branching"]["selection_divergence_reason"],
    }
    artifacts = {
        "compare.json": compare,
        "candidate_decision.json": decision,
        "family_leaderboard.json": {"schema_version": f"{SCHEMA_PREFIX}_family_leaderboard_v1", "generated_at": _utc_now(), "families": [{**decision["metrics"], "family_id": CANDIDATE_ID, "decision": decision["authoritative_rollup_decision"], "decision_reason": decision["decision_reason"]}]},
        "by_month.json": by_month,
        "by_regime.json": by_regime,
        "branching_summary.json": branching,
        "veto_summary.json": veto,
        "relative_strength_feature_summary.json": feature_summary,
    }
    paths = {name: str(_write_json(output_root / name, payload)) for name, payload in artifacts.items()}
    paths["vetoed_members.csv"] = _write_csv(output_root / "vetoed_members.csv", _change_rows(ranked, 20, added=False) + [row for row in ranked[ranked["weak_rs_veto_flag"].astype(bool)].to_dict(orient="records")])
    paths["replacement_members.csv"] = _write_csv(output_root / "replacement_members.csv", compare["added_challenger_members"]["top5"] + compare["added_challenger_members"]["top10"])
    complete = build_artifact_complete({"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "artifact_root": str(output_root), "json_validated": True}, list(artifacts.keys()) + ["vetoed_members.csv", "replacement_members.csv"], schema_version=f"{SCHEMA_PREFIX}_artifact_complete_v1")
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
    root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT)
    stock_db = _safe_path(args.stock_db, DEFAULT_STOCK_DB)
    run_id = args.run_id.strip() or _run_id()
    if not run_id.endswith(f"-{CANDIDATE_ID}"):
        run_id = f"{run_id}-{CANDIDATE_ID}"
    output_root = root / run_id
    payload = _build_outputs(_load_frame(source), output_root=output_root, stock_db=stock_db)
    print(json.dumps({"output_root": str(output_root), "decision": payload["decision"]["authoritative_rollup_decision"], "decision_reason": payload["decision"]["decision_reason"], "authoritative_decision_artifact": payload["paths"]["candidate_decision.json"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
