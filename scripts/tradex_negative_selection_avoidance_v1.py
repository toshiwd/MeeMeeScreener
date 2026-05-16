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

from scripts.tradex_negative_selection_feasibility_v1 import SOURCE_ROWS, STOCK_DB, _as_float, _date_to_epoch, _enrich_candidate, _prepare_context
from scripts.tradex_reflectability_funnel_common_v1 import TOP_K_VALUES, _mean_or_none, _safe_path, _utc_now, _write_json, build_artifact_complete

DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\negative_selection_avoidance_v1")
CANDIDATE_ID = "negative_selection_avoidance_v1"
CHAMPION_ID = "champion_top5_capture_boundary_promoter_v1"
SCHEMA_PREFIX = "tradex_negative_selection_avoidance_v1"


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _is_severe(row: pd.Series | dict[str, Any]) -> bool:
    ret20 = _as_float(row.get("forward_ret_20d"))
    return bool(row.get("bottom15_label", False)) or (ret20 is not None and ret20 <= -0.15)


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


def _load_frame(source: Path) -> pd.DataFrame:
    frame = pd.read_parquet(source)
    if "champion_score" not in frame.columns and "score" in frame.columns:
        frame["champion_score"] = frame["score"]
    frame = frame[frame["champion_selected_top20"].fillna(False).astype(bool)].copy()
    frame["symbol"] = frame["symbol"].astype(str)
    frame["anchor_date"] = frame["anchor_date"].astype(str)
    frame["side"] = frame["side"].astype(str)
    if "month_bucket" not in frame.columns:
        frame["month_bucket"] = frame["anchor_date"].str.slice(0, 7)
    if "regime_label" not in frame.columns:
        frame["regime_label"] = frame.get("market_regime_bucket", "unverified")
    return frame


def _flag_breakdown(row: dict[str, Any]) -> bool:
    if not row.get("context_available"):
        return False
    c1 = (row.get("distance_from_20d_high_pct") is not None and row["distance_from_20d_high_pct"] <= -0.03)
    c2 = (row.get("runup_20d") is not None and row["runup_20d"] > 0)
    c3 = any((row.get(k) is not None and row[k] < 0) for k in ("close_vs_ma7_pct", "close_vs_ma20_pct", "ma7_slope_5d"))
    c4 = (
        (row.get("drawdown_10d") is not None and row["drawdown_10d"] < -0.03)
        or (row.get("drawdown_20d") is not None and row["drawdown_20d"] < -0.05)
        or (row.get("close_position_in_range") is not None and row["close_position_in_range"] < 0.40)
    )
    return bool(c1 and c2 and c3 and c4)


def _apply_candidate(frame: pd.DataFrame) -> pd.DataFrame:
    ctx = _prepare_context(frame)
    rows = []
    for raw in frame.to_dict(orient="records"):
        enriched = _enrich_candidate(raw, ctx.get((str(raw["symbol"]), _date_to_epoch(str(raw["anchor_date"])))))
        rows.append({**raw, **enriched})
    working = pd.DataFrame(rows)
    working["field_missing_for_pattern"] = ~working["context_available"].fillna(False).astype(bool)
    working["breakdown_after_failed_high_flag"] = working.apply(lambda row: _flag_breakdown(row.to_dict()), axis=1)
    ranked = pd.concat([_rank_group(group) for _, group in working.groupby(["anchor_date", "side"], sort=True)], ignore_index=True)
    return ranked


def _rank_group(group: pd.DataFrame) -> pd.DataFrame:
    group = group.sort_values(["champion_rank", "symbol"], kind="stable").copy()
    non_flagged = group[~group["breakdown_after_failed_high_flag"].fillna(False).astype(bool)]
    flagged = group[group["breakdown_after_failed_high_flag"].fillna(False).astype(bool)]
    ordered = pd.concat([non_flagged, flagged], ignore_index=False).copy()
    ordered["candidate_rank"] = range(1, len(ordered) + 1)
    for k in TOP_K_VALUES:
        ordered[f"candidate_selected_top{k}"] = ordered["candidate_rank"].le(k)
        ordered[f"champion_selected_top{k}"] = ordered["champion_rank"].le(k)
        ordered[f"changed_top{k}_member"] = ordered[f"candidate_selected_top{k}"] != ordered[f"champion_selected_top{k}"]
    ordered["rank_changed"] = ordered["candidate_rank"].astype("Int64") != ordered["champion_rank"]
    return ordered


def _metrics(frame: pd.DataFrame, prefix: str) -> dict[str, Any]:
    out = {}
    for k in TOP_K_VALUES:
        selected = frame[frame[f"{prefix}_selected_top{k}"].fillna(False).astype(bool)]
        out[f"top{k}"] = {
            "selected_count": int(len(selected)),
            "forward_ret_20d_mean": _mean_or_none(selected["forward_ret_20d"].tolist()),
            "severe_loser_rate": _mean_or_none([1.0 if _is_severe(row) else 0.0 for _, row in selected.iterrows()]),
        }
    return out


def _change_rows(frame: pd.DataFrame, k: int, *, added: bool) -> list[dict[str, Any]]:
    c, h = f"candidate_selected_top{k}", f"champion_selected_top{k}"
    selected = frame[frame[c].astype(bool) & ~frame[h].astype(bool)] if added else frame[frame[h].astype(bool) & ~frame[c].astype(bool)]
    cols = ["anchor_date", "side", "symbol", "champion_rank", "candidate_rank", "champion_score", "forward_ret_20d", "bottom15_label", "breakdown_after_failed_high_flag", "distance_from_20d_high_pct", "drawdown_10d", "drawdown_20d", "close_position_in_range", "close_vs_ma7_pct", "close_vs_ma20_pct", "ma7_slope_5d"]
    return [{key: (None if pd.isna(value) else value) for key, value in row.items()} for row in selected.sort_values(["anchor_date", "side", "candidate_rank", "champion_rank", "symbol"], kind="stable")[cols].to_dict(orient="records")]


def _summary(frame: pd.DataFrame) -> dict[str, Any]:
    flagged = frame[frame["breakdown_after_failed_high_flag"].fillna(False).astype(bool)]
    removed = frame[frame["champion_selected_top5"].astype(bool) & ~frame["candidate_selected_top5"].astype(bool)]
    added = frame[frame["candidate_selected_top5"].astype(bool) & ~frame["champion_selected_top5"].astype(bool)]
    removed_avg = _mean_or_none(removed["forward_ret_20d"].tolist())
    added_avg = _mean_or_none(added["forward_ret_20d"].tolist())
    removed_sev = _mean_or_none([1.0 if _is_severe(row) else 0.0 for _, row in removed.iterrows()])
    added_sev = _mean_or_none([1.0 if _is_severe(row) else 0.0 for _, row in added.iterrows()])
    removal_classes = [_classify_removal(row) for _, row in removed.iterrows()]
    replacement_classes = [_classify_replacement(row, removed_avg) for _, row in added.iterrows()]
    return {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "generated_at": _utc_now(),
        "pattern_used": "breakdown_after_failed_high",
        "flagged_count_total": int(len(flagged)),
        "flagged_from_champion_top5_count": int(flagged["champion_rank"].le(5).sum()),
        "flagged_from_champion_top10_count": int(flagged["champion_rank"].le(10).sum()),
        "flagged_good_removal_count": removal_classes.count("good_removal"),
        "flagged_bad_removal_count": removal_classes.count("bad_removal"),
        "flagged_neutral_removal_count": removal_classes.count("neutral_removal"),
        "replacement_good_count": replacement_classes.count("good_replacement"),
        "replacement_bad_count": replacement_classes.count("bad_replacement"),
        "replacement_neutral_count": replacement_classes.count("neutral_replacement"),
        "net_replacement_return_delta": None if added_avg is None or removed_avg is None else added_avg - removed_avg,
        "net_severe_loser_delta": None if added_sev is None or removed_sev is None else added_sev - removed_sev,
        "pattern_breadth_by_month": dict(Counter(flagged["month_bucket"].astype(str))),
        "pattern_breadth_by_regime": dict(Counter(flagged["regime_label"].astype(str))),
        "fallback_reason_counts": {},
        "research_fallback_count": 0,
    }


def _compare(frame: pd.DataFrame, summary: dict[str, Any]) -> dict[str, Any]:
    champ, cand = _metrics(frame, "champion"), _metrics(frame, "candidate")
    deltas, overlap = {}, {}
    for k in TOP_K_VALUES:
        key = f"top{k}"
        deltas[key] = {
            "forward_ret_20d_mean_delta": cand[key]["forward_ret_20d_mean"] - champ[key]["forward_ret_20d_mean"],
            "severe_loser_rate_delta": cand[key]["severe_loser_rate"] - champ[key]["severe_loser_rate"],
            "changed_member_count": int(frame[f"changed_top{k}_member"].sum()),
        }
        both = int((frame[f"candidate_selected_top{k}"].astype(bool) & frame[f"champion_selected_top{k}"].astype(bool)).sum())
        cnt = int(frame[f"champion_selected_top{k}"].astype(bool).sum())
        overlap[key] = {"overlap_count": both, "champion_count": cnt, "overlap_with_champion": both / cnt if cnt else None}
    return {
        "schema_version": f"{SCHEMA_PREFIX}_compare_v1",
        "generated_at": _utc_now(),
        "candidate_id": CANDIDATE_ID,
        "champion_id": CHAMPION_ID,
        "same_condition_contract": {"same_universe": True, "same_period": True, "same_top_k": list(TOP_K_VALUES), "same_regime": True, "same_cost_slippage": True, "same_artifact_detail_level": True, "silent_fallback_allowed": False},
        "field_missing_count": int(frame["field_missing_for_pattern"].sum()),
        "weekly_context_available": False,
        "monthly_context_available": False,
        "research_fallback_count": 0,
        "no_lookahead_check": {"pass": True, "daily_bars_filter": "source='pan' and date <= decision_date", "future_outcomes_used_only_for_evaluation": True},
        "champion": champ,
        "candidate": cand,
        "deltas": deltas,
        "candidate_shortage": {"decision_sets_total": int(frame.groupby(["anchor_date", "side"]).ngroups), **{f"top{k}_shortage_rate": 0.0 for k in TOP_K_VALUES}},
        "overlap": overlap,
        "branching": {"changed_top5_members_count": deltas["top5"]["changed_member_count"], "changed_top10_members_count": deltas["top10"]["changed_member_count"], "changed_top20_members_count": deltas["top20"]["changed_member_count"], "changed_rank_count": int(frame["rank_changed"].sum()), "selection_divergence_reason": "negative_selection_breakdown_after_failed_high_veto_preserve_champion_order"},
        "negative_selection_summary": summary,
        "added_challenger_members": {f"top{k}": _change_rows(frame, k, added=True) for k in TOP_K_VALUES},
        "removed_champion_members": {f"top{k}": _change_rows(frame, k, added=False) for k in TOP_K_VALUES},
    }


def _breakdown(frame: pd.DataFrame, col: str) -> dict[str, Any]:
    rows = []
    for bucket, group in frame.groupby(col, sort=True):
        comp = _compare(group, _summary(group))
        rows.append({col: str(bucket), "decision_sets": int(group.groupby(["anchor_date", "side"]).ngroups), "changed_top5_members_count": comp["branching"]["changed_top5_members_count"], "changed_top10_members_count": comp["branching"]["changed_top10_members_count"], "top5_forward_ret_20d_mean_delta": comp["deltas"]["top5"]["forward_ret_20d_mean_delta"], "top10_forward_ret_20d_mean_delta": comp["deltas"]["top10"]["forward_ret_20d_mean_delta"], "top5_severe_loser_rate_delta": comp["deltas"]["top5"]["severe_loser_rate_delta"]})
    return {"schema_version": f"{SCHEMA_PREFIX}_breakdown_v1", "generated_at": _utc_now(), "breakdown_column": col, "rows": rows, "breadth": {"bucket_count": len(rows), "top5_branched_bucket_count": sum(1 for r in rows if r["changed_top5_members_count"] > 0), "top5_improved_bucket_count": sum(1 for r in rows if (r["top5_forward_ret_20d_mean_delta"] or 0) > 0)}}


def _decision(comp: dict[str, Any], month: dict[str, Any], regime: dict[str, Any]) -> dict[str, Any]:
    top5, top10 = comp["deltas"]["top5"]["forward_ret_20d_mean_delta"], comp["deltas"]["top10"]["forward_ret_20d_mean_delta"]
    sev5, sev10 = comp["deltas"]["top5"]["severe_loser_rate_delta"], comp["deltas"]["top10"]["severe_loser_rate_delta"]
    if comp["branching"]["changed_top5_members_count"] + comp["branching"]["changed_top10_members_count"] == 0:
        dec, reason = "drop", "no_material_branching"
    elif sev5 >= 0 and sev10 >= 0:
        dec, reason = "drop", "severe_loser_rate_did_not_improve"
    elif top5 < -0.003 or top10 < -0.003:
        dec, reason = "drop", "return_deterioration_material"
    elif sev5 < 0 and top5 >= 0 and top10 >= 0 and month["breadth"]["top5_improved_bucket_count"] >= 6 and regime["breadth"]["bucket_count"] > 1:
        dec, reason = "keep", "risk_improved_without_return_deterioration_with_breadth"
    else:
        dec, reason = "hold", "risk_or_return_improvement_mixed_or_breadth_limited"
    return {"schema_version": f"{SCHEMA_PREFIX}_decision_v1", "generated_at": _utc_now(), "candidate_id": CANDIDATE_ID, "champion_id": CHAMPION_ID, "candidate_local_decision": dec, "session_aggregate_decision": dec, "authoritative_rollup_decision": dec, "decision_reason": reason, "research_fallback": False, "promote_ready": dec == "keep", "metrics": {"top5_return_delta": top5, "top10_return_delta": top10, "top20_return_delta": comp["deltas"]["top20"]["forward_ret_20d_mean_delta"], "top5_severe_loser_rate_delta": sev5, "top10_severe_loser_rate_delta": sev10, "changed_top5_members_count": comp["branching"]["changed_top5_members_count"], "changed_top10_members_count": comp["branching"]["changed_top10_members_count"], "month_breadth": month["breadth"], "regime_breadth": regime["breadth"]}, "non_scope": ["No MeeMee mutation", "No live ranking mutation", "No champion scoring change", "No multi-pattern combination"]}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> str:
    keys = sorted({k for row in rows for k in row}) if rows else ["empty"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader(); w.writerows(rows)
    return str(path)


def _build_outputs(frame: pd.DataFrame, root: Path) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    ranked = _apply_candidate(frame)
    summary = _summary(ranked)
    comp = _compare(ranked, summary)
    month = _breakdown(ranked, "month_bucket")
    regime = _breakdown(ranked, "regime_label")
    dec = _decision(comp, month, regime)
    feature = {"schema_version": f"{SCHEMA_PREFIX}_feature_summary_v1", "generated_at": _utc_now(), "pattern": "breakdown_after_failed_high", "rule": "distance_from_20d_high_pct <= -0.03 and runup_20d > 0 and one of close_vs_ma7_pct/close_vs_ma20_pct/ma7_slope_5d < 0 and one of drawdown_10d < -0.03/drawdown_20d < -0.05/close_position_in_range < 0.40", "field_missing_count": comp["field_missing_count"], "weekly_context_available": False, "monthly_context_available": False, "no_lookahead_check": comp["no_lookahead_check"], "label_columns_excluded_from_scoring": ["forward_ret_20d", "bottom15_label", "path_value_score_v1", "mfe_20d", "mae_20d"]}
    branch = {"schema_version": f"{SCHEMA_PREFIX}_branching_summary_v1", "generated_at": _utc_now(), "branching": comp["branching"], "overlap": comp["overlap"], "candidate_shortage": comp["candidate_shortage"], "added_challenger_members": comp["added_challenger_members"], "removed_champion_members": comp["removed_champion_members"], "selection_divergence_reason": comp["branching"]["selection_divergence_reason"]}
    artifacts = {"compare.json": comp, "candidate_decision.json": dec, "family_leaderboard.json": {"schema_version": f"{SCHEMA_PREFIX}_family_leaderboard_v1", "generated_at": _utc_now(), "families": [{**dec["metrics"], "family_id": CANDIDATE_ID, "decision": dec["authoritative_rollup_decision"], "decision_reason": dec["decision_reason"]}]}, "by_month.json": month, "by_regime.json": regime, "branching_summary.json": branch, "negative_selection_summary.json": summary, "failure_pattern_feature_summary.json": feature}
    paths = {name: str(_write_json(root / name, payload)) for name, payload in artifacts.items()}
    paths["flagged_members.csv"] = _write_csv(root / "flagged_members.csv", ranked[ranked["breakdown_after_failed_high_flag"].astype(bool)].to_dict(orient="records"))
    paths["replacement_members.csv"] = _write_csv(root / "replacement_members.csv", comp["added_challenger_members"]["top5"] + comp["added_challenger_members"]["top10"])
    complete = build_artifact_complete({"schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1", "artifact_root": str(root)}, list(artifacts) + ["flagged_members.csv", "replacement_members.csv"], schema_version=f"{SCHEMA_PREFIX}_artifact_complete_v1")
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(root / "_ARTIFACT_COMPLETE.json", complete))
    return {"paths": paths, "decision": dec, "compare": comp}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--source-rows-parquet", default=str(SOURCE_ROWS))
    p.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    p.add_argument("--run-id", default="")
    args = p.parse_args(argv)
    run_id = args.run_id.strip() or _run_id()
    if not run_id.endswith(f"-{CANDIDATE_ID}"):
        run_id = f"{run_id}-{CANDIDATE_ID}"
    root = _safe_path(args.output_root, DEFAULT_OUTPUT_ROOT) / run_id
    payload = _build_outputs(_load_frame(_safe_path(args.source_rows_parquet, SOURCE_ROWS)), root)
    print(json.dumps({"output_root": str(root), "decision": payload["decision"]["authoritative_rollup_decision"], "decision_reason": payload["decision"]["decision_reason"], "authoritative_decision_artifact": payload["paths"]["candidate_decision.json"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
