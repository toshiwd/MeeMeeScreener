from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "bad_pick_single_axis_gate_selection_audit_v1"
DEFAULT_SOURCE_ROOT = Path("G:/Tradex/independent_bad_pick_removal_axis_discovery_v1/20260604T021818Z-independent-bad-pick-removal-axis-discovery-v1")
DEFAULT_REPLAY_ROWS = Path("G:/Tradex/current_buyable_historical_operational_replay_v1/20260526T014356Z-current-buyable-historical-operational-replay-v1/historical_operational_replay_rows.csv")
DEFAULT_OUT_ROOT = Path("G:/Tradex/bad_pick_single_axis_gate_selection_audit_v1")
TOPKS = (5, 10, 20)
REQUIRED = (
    "final_research_decision.json",
    "single_axis_gate_selection.json",
    "leakage_contract_audit.json",
    "axis_overlap_matrix.json",
    "axis_incremental_value_summary.json",
    "false_removal_decomposition.csv",
    "selected_axis_candidate_card.json",
    "rejected_axis_reason_summary.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _mean(s: pd.Series) -> float | None:
    v = pd.to_numeric(s, errors="coerce").dropna()
    return None if v.empty else float(v.mean())


def _rate(s: pd.Series) -> float | None:
    v = s.dropna()
    return None if v.empty else float(v.astype(bool).mean())


def _load_rows(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["code"] = df["code"].astype(str)
    df["as_of_date"] = df["as_of_date"].astype(str)
    df["baseline_rank"] = pd.to_numeric(df["fresh_runtime_research_watch_rank"], errors="coerce")
    df["baseline_top_rank"] = df.groupby("as_of_date")["baseline_rank"].rank(method="first")
    df["ret20"] = pd.to_numeric(df["ret20"], errors="coerce")
    df["hit_flag"] = df["ret20"] > 0
    df["bottom_return_flag"] = df["ret20"] <= -0.05
    df["severe_loss_flag"] = df["ret20"] <= -0.10
    df["bad_pick_flag"] = df["bottom_return_flag"] | df["severe_loss_flag"]
    df["winner_quality_bucket"] = "loss_or_flat"
    df.loc[(df["ret20"] > 0) & (df["ret20"] < 0.03), "winner_quality_bucket"] = "marginal_winner_0_3pct"
    df.loc[(df["ret20"] >= 0.03) & (df["ret20"] < 0.10), "winner_quality_bucket"] = "solid_winner_3_10pct"
    df.loc[df["ret20"] >= 0.10, "winner_quality_bucket"] = "high_quality_winner_10pct_plus"
    for col in ["monthly_supportive_flag", "weekly_supportive_flag", "variant_b_volatility_extension_clean", "variant_c_combined_context_risk_clean"]:
        df[col] = df[col].fillna(False).astype(bool)
    df["invalidation_hit_20d"] = df["invalidation_hit_20d"].fillna(False).astype(bool)
    df["support_distance_pct"] = pd.to_numeric(df["entry_reference_close"], errors="coerce") / pd.to_numeric(df["recent_swing_low"], errors="coerce") - 1.0
    for col in ["upper_wick_ratio", "recent_high_distance_pct", "atr14_pct", "realized_vol20"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _axis_defs(df: pd.DataFrame) -> list[dict[str, Any]]:
    q = lambda col, pct: float(df[col].quantile(pct))
    defs = [
        ("invalidation_hit_proxy", "drawdown/invalidation proxy", "outcome_proxy_or_label_only", "invalidation_hit_20d == true; uses future 20d invalidation outcome", df["invalidation_hit_20d"]),
        ("far_from_recent_support_q75", "distance from support/resistance", "deployable_pre_entry_feature", "support_distance_pct >= source q75", df["support_distance_pct"] >= q("support_distance_pct", 0.75)),
        ("context_risk_not_clean", "combined non-MA context risk", "deployable_pre_entry_feature", "variant_c_combined_context_risk_clean == false", ~df["variant_c_combined_context_risk_clean"]),
        ("volatility_expansion_atr_q75", "volatility expansion after exhaustion", "deployable_pre_entry_feature", "atr14_pct >= source q75", df["atr14_pct"] >= q("atr14_pct", 0.75)),
        ("realized_volatility_q75", "volatility expansion after exhaustion", "deployable_pre_entry_feature", "realized_vol20 >= source q75", df["realized_vol20"] >= q("realized_vol20", 0.75)),
        ("high_position_recent_range_q75", "high position in recent range", "deployable_pre_entry_feature", "recent_high_distance_pct >= source q75", df["recent_high_distance_pct"] >= q("recent_high_distance_pct", 0.75)),
        ("weak_candle_upper_shadow_q75", "weak candle / upper shadow", "deployable_pre_entry_feature", "upper_wick_ratio >= source q75", df["upper_wick_ratio"] >= q("upper_wick_ratio", 0.75)),
        ("weekly_monthly_regime_mismatch", "monthly/weekly regime mismatch", "deployable_pre_entry_feature", "weekly_supportive_flag == false or monthly_supportive_flag == false", (~df["weekly_supportive_flag"]) | (~df["monthly_supportive_flag"])),
        ("monthly_regime_mismatch", "monthly/weekly regime mismatch", "deployable_pre_entry_feature", "monthly_supportive_flag == false", ~df["monthly_supportive_flag"]),
        ("volatility_extension_not_clean", "volatility expansion after exhaustion", "deployable_pre_entry_feature", "variant_b_volatility_extension_clean == false", ~df["variant_b_volatility_extension_clean"]),
    ]
    axes = []
    for axis_id, family, status, definition, mask in defs:
        df[axis_id] = mask.fillna(False).astype(bool)
        axes.append({"axis_id": axis_id, "axis_family": family, "leakage_status": status, "definition": definition})
    return axes


def _quality(part: pd.DataFrame) -> dict[str, Any]:
    return {
        "sample_count": int(len(part)),
        "mean_ret20": _mean(part["ret20"]),
        "hit_rate": _rate(part["hit_flag"]),
        "severe_loss_rate": _rate(part["severe_loss_flag"]),
        "bad_pick_rate": _rate(part["bad_pick_flag"]),
    }


def _metrics(df: pd.DataFrame, axis: dict[str, Any], stronger: set[str]) -> dict[str, Any]:
    col = axis["axis_id"]
    inside = df[df[col]]
    outside = df[~df[col]]
    prior_mask = df[list(stronger)].any(axis=1) if stronger else pd.Series(False, index=df.index)
    incremental = df[df[col] & ~prior_mask]
    row = {
        **axis,
        "sample_count": int(len(inside)),
        "inside": _quality(inside),
        "outside": _quality(outside),
        "mean_ret20_inside_minus_outside": (_mean(inside["ret20"]) - _mean(outside["ret20"])) if _mean(inside["ret20"]) is not None and _mean(outside["ret20"]) is not None else None,
        "severe_loss_inside_minus_outside": (_rate(inside["severe_loss_flag"]) - _rate(outside["severe_loss_flag"])) if _rate(inside["severe_loss_flag"]) is not None and _rate(outside["severe_loss_flag"]) is not None else None,
        "incremental_bad_pick_capture_count": int(incremental["bad_pick_flag"].sum()),
        "incremental_good_pick_false_removal_count": int((incremental["ret20"] > 0).sum()),
        "topk": [],
    }
    for topk in TOPKS:
        top = df[df["baseline_top_rank"] <= topk]
        ti = top[top[col]]
        to = top[~top[col]]
        prior_top = top[list(stronger)].any(axis=1) if stronger else pd.Series(False, index=top.index)
        inc = top[top[col] & ~prior_top]
        bad_total = int(top["bad_pick_flag"].sum())
        good_total = int((top["ret20"] > 0).sum())
        row["topk"].append({
            "topk": topk,
            "coverage": float(len(ti) / len(top)) if len(top) else None,
            "sample_count": int(len(ti)),
            "bad_pick_capture_count": int(ti["bad_pick_flag"].sum()),
            "bad_pick_capture_rate": float(ti["bad_pick_flag"].sum() / bad_total) if bad_total else 0.0,
            "good_pick_false_removal_count": int((ti["ret20"] > 0).sum()),
            "good_pick_false_removal_rate": float((ti["ret20"] > 0).sum() / good_total) if good_total else 0.0,
            "inside": _quality(ti),
            "outside": _quality(to),
            "incremental_bad_pick_capture_count": int(inc["bad_pick_flag"].sum()),
            "incremental_good_pick_false_removal_count": int((inc["ret20"] > 0).sum()),
            "net_replacement_potential": (_mean(to["ret20"]) - _mean(ti["ret20"])) if _mean(to["ret20"]) is not None and _mean(ti["ret20"]) is not None else None,
            "topK_branching_potential": int(len(ti)),
        })
    return row


def _overlap(df: pd.DataFrame, axes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for a in axes:
        for b in axes:
            am = df[a["axis_id"]]
            bm = df[b["axis_id"]]
            inter = int((am & bm).sum())
            union = int((am | bm).sum())
            rows.append({
                "axis_a": a["axis_id"],
                "axis_b": b["axis_id"],
                "overlap_count": inter,
                "jaccard_overlap": float(inter / union) if union else None,
                "a_covered_by_b_rate": float(inter / am.sum()) if int(am.sum()) else None,
                "conditional_bad_pick_capture_a_given_b_count": int(df[am & bm]["bad_pick_flag"].sum()),
                "conditional_good_false_removal_a_given_b_count": int((df[am & bm]["ret20"] > 0).sum()),
            })
    return rows


def _false_removals(df: pd.DataFrame, axes: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for axis in axes:
        col = axis["axis_id"]
        for topk in TOPKS:
            part = df[(df["baseline_top_rank"] <= topk) & df[col] & (df["ret20"] > 0)]
            for bucket, g in part.groupby("winner_quality_bucket", dropna=False):
                rows.append({
                    "axis_id": col,
                    "leakage_status": axis["leakage_status"],
                    "topk": topk,
                    "winner_quality_bucket": bucket,
                    "false_removed_good_picks_count": int(len(g)),
                    "average_ret20_of_false_removals": _mean(g["ret20"]),
                    "hit_rate_of_false_removals": _rate(g["hit_flag"]),
                    "top5_count": int((g["baseline_top_rank"] <= 5).sum()),
                    "top10_count": int((g["baseline_top_rank"] <= 10).sum()),
                    "top20_count": int((g["baseline_top_rank"] <= 20).sum()),
                })
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> Path:
    source_decision = _read_json(args.source_root / "final_research_decision.json")
    if source_decision.get("authoritative_rollup_decision") != "keep_diagnostic_only":
        raise RuntimeError("source decision is not keep_diagnostic_only")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    df = _load_rows(args.replay_rows)
    axes = _axis_defs(df)
    leakage_audit = {
        "axis_contracts": [
            {
                **axis,
                "as_of_date_observable": axis["leakage_status"] == "deployable_pre_entry_feature",
                "promotion_allowed": axis["leakage_status"] == "deployable_pre_entry_feature",
                "leakage_reason": "uses future 20d invalidation/outcome label" if axis["axis_id"] == "invalidation_hit_proxy" else None,
            }
            for axis in axes
        ],
        "unavailable_axis": [
            "volume abnormality without follow-through",
            "liquidity/thin-sample instability",
            "gap failure with open/previous close fields",
        ],
    }
    deployable = [a for a in axes if a["leakage_status"] == "deployable_pre_entry_feature"]
    overlap_rows = _overlap(df, deployable)
    metric_rows = []
    stronger: set[str] = set()
    for axis in sorted(deployable, key=lambda a: -int(df[a["axis_id"]].sum())):
        metric_rows.append(_metrics(df, axis, stronger))
        stronger.add(axis["axis_id"])
    false_df = _false_removals(df, axes)
    false_df.to_csv(out_dir / "false_removal_decomposition.csv", index=False, encoding="utf-8")

    # Rank by deployable, low false removal, bad-pick enrichment, and incremental value.
    ranked = []
    for row in metric_rows:
        top10 = next(x for x in row["topk"] if x["topk"] == 10)
        score = 0.0
        score += 4.0 * top10["bad_pick_capture_rate"]
        score -= 3.0 * top10["good_pick_false_removal_rate"]
        score += 8.0 * (top10["net_replacement_potential"] or 0)
        score += 0.08 * top10["incremental_bad_pick_capture_count"]
        score -= 0.03 * top10["incremental_good_pick_false_removal_count"]
        ranked.append({**row, "single_axis_usefulness_score": score})
    ranked.sort(key=lambda x: x["single_axis_usefulness_score"], reverse=True)
    best = ranked[0] if ranked else None
    second = ranked[1] if len(ranked) > 1 else None
    clear_gap = bool(best and (not second or best["single_axis_usefulness_score"] - second["single_axis_usefulness_score"] >= 0.35))
    best_top10 = next((x for x in best["topk"] if x["topk"] == 10), {}) if best else {}
    best_passes = bool(best and best_top10.get("bad_pick_capture_count", 0) >= 8 and best_top10.get("good_pick_false_removal_rate", 1) <= 0.25 and best_top10.get("incremental_bad_pick_capture_count", 0) >= 3)
    if best_passes and clear_gap:
        decision, reason = "propose_single_next_challenger", "one_deployable_axis_clearly_best_after_leakage_overlap_false_removal_audit"
        selected = best
    elif ranked:
        decision, reason = "keep_diagnostic_only", "deployable_axes_remain_too_close_or_overlap_requires_more_contract_work"
        selected = None
    else:
        decision, reason = "drop_axis_family", "all_strong_axes_are_outcome_proxy_unavailable_or_not_deployable"
        selected = None

    rejected = []
    for row in ranked:
        reasons = []
        if selected and row["axis_id"] == selected["axis_id"]:
            continue
        top10 = next(x for x in row["topk"] if x["topk"] == 10)
        if top10["good_pick_false_removal_rate"] > 0.25:
            reasons.append("good_pick_false_removal_too_high")
        if top10["incremental_bad_pick_capture_count"] < 3:
            reasons.append("low_incremental_bad_pick_capture_after_overlap")
        if selected is None:
            reasons.append("not_selected_because_no_clear_single_axis_gap")
        rejected.append({"axis_id": row["axis_id"], "reasons": reasons or ["weaker_than_selected_axis"], "single_axis_usefulness_score": row["single_axis_usefulness_score"]})
    rejected.append({"axis_id": "invalidation_hit_proxy", "reasons": ["outcome_proxy_or_label_only", "uses_future_20d_invalidation_hit", "promotion_disallowed"], "single_axis_usefulness_score": None})

    _write_json(out_dir / "leakage_contract_audit.json", {"axis_id": AXIS_ID, **leakage_audit})
    _write_json(out_dir / "axis_overlap_matrix.json", {"axis_id": AXIS_ID, "overlap_matrix": overlap_rows})
    _write_json(out_dir / "axis_incremental_value_summary.json", {"axis_id": AXIS_ID, "deployable_axes": ranked})
    _write_json(out_dir / "single_axis_gate_selection.json", {"axis_id": AXIS_ID, "decision": decision, "reason": reason, "ranked_deployable_axes": ranked})
    _write_json(out_dir / "selected_axis_candidate_card.json", {"axis_id": AXIS_ID, "selected_axis": selected, "challenger_implemented": False})
    _write_json(out_dir / "rejected_axis_reason_summary.json", {"axis_id": AXIS_ID, "rejected_axes": rejected})
    final = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": decision,
        "reason": reason,
        "source_artifact": str(args.source_root / "final_research_decision.json"),
        "selected_single_axis": selected["axis_id"] if selected else None,
        "invalidation_hit_proxy_status": "outcome_proxy_or_label_only",
        "invalidation_hit_proxy_promotion_allowed": False,
        "deployable_axes": [a["axis_id"] for a in deployable],
        "selected_axis_candidate_card": selected,
        "boundary_flags": {
            "tradex_only": True,
            "read_only_diagnostic": True,
            "challenger_implemented": False,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "ma_phase_mixed_in": False,
            "ma_threshold_tuning": False,
            "exit_threshold_tuning": False,
            "baseline_rank_source_changed": False,
            "replay_rows_changed": False,
        },
        "next_required_single_step": "build_single_read_only_challenger_for_selected_axis" if selected else "resolve_axis_overlap_or_expand_pre_entry_contract_before_challenger",
    }
    _write_json(out_dir / "final_research_decision.json", final)
    _write_json(out_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "status": "complete", "generated_at_utc": datetime.now(timezone.utc).isoformat(), "required_files": list(REQUIRED), "required_files_present": all((out_dir / f).exists() for f in REQUIRED if f != "_ARTIFACT_COMPLETE.json")})
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Select one deployable bad-pick removal axis after leakage and overlap audit.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    print(run(parser.parse_args()))


if __name__ == "__main__":
    main()
