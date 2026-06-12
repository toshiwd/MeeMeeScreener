from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "independent_bad_pick_removal_axis_discovery_v1"
DEFAULT_REPLAY_ROOT = Path("G:/Tradex/current_buyable_historical_operational_replay_v1/20260526T014356Z-current-buyable-historical-operational-replay-v1")
DEFAULT_REPLAY_ROWS = DEFAULT_REPLAY_ROOT / "historical_operational_replay_rows.csv"
DEFAULT_FROZEN_MA_DECISION = Path("G:/Tradex/ma_phase_context_branching_leverage_audit_v1/20260604T014827Z-ma-phase-context-branching-leverage-audit-v1/final_research_decision.json")
DEFAULT_OUT_ROOT = Path("G:/Tradex/independent_bad_pick_removal_axis_discovery_v1")
REQUIRED = (
    "final_research_decision.json",
    "bad_pick_axis_leaderboard.json",
    "bad_pick_decomposition_detail.csv",
    "topk_bad_pick_summary.json",
    "feature_slice_quality_summary.json",
    "candidate_axis_gate_summary.json",
    "_ARTIFACT_COMPLETE.json",
)
TOPKS = (5, 10, 20)


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


def _mean(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.mean())


def _median(series: pd.Series) -> float | None:
    valid = pd.to_numeric(series, errors="coerce").dropna()
    return None if valid.empty else float(valid.median())


def _rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    return None if valid.empty else float(valid.astype(bool).mean())


def _load_rows(path: Path) -> pd.DataFrame:
    rows = pd.read_csv(path)
    rows["code"] = rows["code"].astype(str)
    rows["as_of_date"] = rows["as_of_date"].astype(str)
    rows["baseline_rank"] = pd.to_numeric(rows["fresh_runtime_research_watch_rank"], errors="coerce")
    rows["baseline_top_rank"] = rows.groupby("as_of_date")["baseline_rank"].rank(method="first")
    rows["ret20"] = pd.to_numeric(rows["ret20"], errors="coerce")
    rows["ret5"] = pd.to_numeric(rows["ret5"], errors="coerce")
    rows["hit_flag"] = rows["ret20"] > 0
    rows["failed_hit_flag"] = rows["ret20"] <= 0
    rows["bottom_return_flag"] = rows["ret20"] <= -0.05
    rows["severe_loss_flag"] = rows["ret20"] <= -0.10
    rows["invalidation_hit_20d"] = rows["invalidation_hit_20d"].fillna(False).astype(bool)
    rows["support_distance_pct"] = (
        pd.to_numeric(rows["entry_reference_close"], errors="coerce") / pd.to_numeric(rows["recent_swing_low"], errors="coerce") - 1.0
    )
    rows["atr14_pct"] = pd.to_numeric(rows["atr14_pct"], errors="coerce")
    rows["realized_vol20"] = pd.to_numeric(rows["realized_vol20"], errors="coerce")
    rows["upper_wick_ratio"] = pd.to_numeric(rows["upper_wick_ratio"], errors="coerce")
    rows["recent_high_distance_pct"] = pd.to_numeric(rows["recent_high_distance_pct"], errors="coerce")
    for col in [
        "monthly_supportive_flag",
        "weekly_supportive_flag",
        "variant_b_volatility_extension_clean",
        "variant_c_combined_context_risk_clean",
        "variant_a_candle_risk_clean",
    ]:
        rows[col] = rows[col].fillna(False).astype(bool)
    return rows


def _add_axes(rows: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], list[str]]:
    df = rows.copy()
    unavailable = [
        "volume abnormality without follow-through: source replay rows do not include volume or turnover columns",
        "liquidity/thin-sample instability: source replay rows do not include liquidity, market cap, spread, or trade-count columns",
        "gap failure: source replay rows do not include open/previous close gap columns; failed_high_flag is constant false in this slice",
    ]
    q = lambda col, pct: float(df[col].quantile(pct))
    upper_wick_q75 = q("upper_wick_ratio", 0.75)
    recent_high_q75 = q("recent_high_distance_pct", 0.75)
    atr_q75 = q("atr14_pct", 0.75)
    vol_q75 = q("realized_vol20", 0.75)
    support_q75 = q("support_distance_pct", 0.75)
    axes = [
        {
            "axis_id": "weak_candle_upper_shadow_q75",
            "axis_family": "weak candle / upper shadow",
            "definition": f"upper_wick_ratio >= source q75 ({upper_wick_q75:.6f})",
            "flag": df["upper_wick_ratio"] >= upper_wick_q75,
        },
        {
            "axis_id": "high_position_recent_range_q75",
            "axis_family": "high position in recent range",
            "definition": f"recent_high_distance_pct >= source q75 ({recent_high_q75:.6f})",
            "flag": df["recent_high_distance_pct"] >= recent_high_q75,
        },
        {
            "axis_id": "volatility_expansion_atr_q75",
            "axis_family": "volatility expansion after exhaustion",
            "definition": f"atr14_pct >= source q75 ({atr_q75:.6f})",
            "flag": df["atr14_pct"] >= atr_q75,
        },
        {
            "axis_id": "realized_volatility_q75",
            "axis_family": "volatility expansion after exhaustion",
            "definition": f"realized_vol20 >= source q75 ({vol_q75:.6f})",
            "flag": df["realized_vol20"] >= vol_q75,
        },
        {
            "axis_id": "volatility_extension_not_clean",
            "axis_family": "volatility expansion after exhaustion",
            "definition": "variant_b_volatility_extension_clean == false",
            "flag": ~df["variant_b_volatility_extension_clean"],
        },
        {
            "axis_id": "weekly_monthly_regime_mismatch",
            "axis_family": "monthly/weekly regime mismatch",
            "definition": "weekly_supportive_flag == false or monthly_supportive_flag == false",
            "flag": (~df["weekly_supportive_flag"]) | (~df["monthly_supportive_flag"]),
        },
        {
            "axis_id": "monthly_regime_mismatch",
            "axis_family": "monthly/weekly regime mismatch",
            "definition": "monthly_supportive_flag == false",
            "flag": ~df["monthly_supportive_flag"],
        },
        {
            "axis_id": "far_from_recent_support_q75",
            "axis_family": "distance from support/resistance",
            "definition": f"entry_reference_close / recent_swing_low - 1 >= source q75 ({support_q75:.6f})",
            "flag": df["support_distance_pct"] >= support_q75,
        },
        {
            "axis_id": "context_risk_not_clean",
            "axis_family": "combined non-MA context risk",
            "definition": "variant_c_combined_context_risk_clean == false",
            "flag": ~df["variant_c_combined_context_risk_clean"],
        },
        {
            "axis_id": "invalidation_hit_proxy",
            "axis_family": "drawdown/invalidation proxy",
            "definition": "invalidation_hit_20d == true",
            "flag": df["invalidation_hit_20d"],
        },
    ]
    for axis in axes:
        df[axis["axis_id"]] = axis["flag"].fillna(False).astype(bool)
        axis["flag_column"] = axis["axis_id"]
        axis.pop("flag")
    return df, axes, unavailable


def _quality(part: pd.DataFrame) -> dict[str, Any]:
    return {
        "sample_count": int(len(part)),
        "unique_date_count": int(part["as_of_date"].nunique()) if not part.empty else 0,
        "unique_symbol_count": int(part["code"].nunique()) if not part.empty else 0,
        "mean_ret20": _mean(part["ret20"]),
        "median_ret20": _median(part["ret20"]),
        "hit_rate": _rate(part["hit_flag"]),
        "failed_hit_rate": _rate(part["failed_hit_flag"]),
        "bottom_return_rate": _rate(part["bottom_return_flag"]),
        "severe_loss_rate": _rate(part["severe_loss_flag"]),
        "invalidation_hit_rate": _rate(part["invalidation_hit_20d"]),
    }


def _topk_bad_pick_summary(rows: pd.DataFrame) -> dict[str, Any]:
    payload = {"topk": []}
    for topk in TOPKS:
        top = rows[rows["baseline_top_rank"] <= topk]
        payload["topk"].append(
            {
                "topk": topk,
                **_quality(top),
                "bad_pick_count_bottom_or_severe": int((top["bottom_return_flag"] | top["severe_loss_flag"]).sum()),
                "failed_hit_count": int(top["failed_hit_flag"].sum()),
            }
        )
    return payload


def _axis_metrics(rows: pd.DataFrame, axes: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    leaderboard: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    for axis in axes:
        col = axis["flag_column"]
        inside = rows[rows[col]]
        outside = rows[~rows[col]]
        inside_quality = _quality(inside)
        outside_quality = _quality(outside)
        row = {
            "axis_id": axis["axis_id"],
            "axis_family": axis["axis_family"],
            "definition": axis["definition"],
            "sample_count": int(len(inside)),
            "coverage_all": float(len(inside) / len(rows)) if len(rows) else None,
            "inside": inside_quality,
            "outside": outside_quality,
            "severe_loss_rate_lift": (
                inside_quality["severe_loss_rate"] - outside_quality["severe_loss_rate"]
                if inside_quality["severe_loss_rate"] is not None and outside_quality["severe_loss_rate"] is not None
                else None
            ),
            "mean_ret20_gap_inside_minus_outside": (
                inside_quality["mean_ret20"] - outside_quality["mean_ret20"]
                if inside_quality["mean_ret20"] is not None and outside_quality["mean_ret20"] is not None
                else None
            ),
            "hit_rate_gap_inside_minus_outside": (
                inside_quality["hit_rate"] - outside_quality["hit_rate"]
                if inside_quality["hit_rate"] is not None and outside_quality["hit_rate"] is not None
                else None
            ),
            "topk": [],
        }
        total_score = 0.0
        for topk in TOPKS:
            top = rows[rows["baseline_top_rank"] <= topk]
            top_inside = top[top[col]]
            top_outside = top[~top[col]]
            bad_capture = int((top_inside["bottom_return_flag"] | top_inside["severe_loss_flag"]).sum())
            good_false = int((top_inside["ret20"] > 0).sum())
            bad_total = int((top["bottom_return_flag"] | top["severe_loss_flag"]).sum())
            good_total = int((top["ret20"] > 0).sum())
            inside_q = _quality(top_inside)
            outside_q = _quality(top_outside)
            replacement_pool = top_outside.sort_values(["as_of_date", "baseline_top_rank"], kind="stable")
            replacement_quality = _mean(replacement_pool["ret20"])
            net_replacement_potential = (
                replacement_quality - inside_q["mean_ret20"]
                if replacement_quality is not None and inside_q["mean_ret20"] is not None
                else None
            )
            top_payload = {
                "topk": topk,
                "coverage": float(len(top_inside) / len(top)) if len(top) else None,
                "sample_count": int(len(top_inside)),
                "bad_pick_capture_count": bad_capture,
                "bad_pick_capture_rate": float(bad_capture / bad_total) if bad_total else 0.0,
                "good_pick_false_removal_count": good_false,
                "good_pick_false_removal_rate": float(good_false / good_total) if good_total else 0.0,
                "inside": inside_q,
                "outside": outside_q,
                "net_replacement_potential": net_replacement_potential,
                "branching_potential": int(len(top_inside)),
            }
            row["topk"].append(top_payload)
            if topk in (5, 10):
                score = 0.0
                score += 3.0 * top_payload["bad_pick_capture_rate"]
                score -= 1.5 * top_payload["good_pick_false_removal_rate"]
                if net_replacement_potential is not None:
                    score += 5.0 * net_replacement_potential
                if inside_q["severe_loss_rate"] is not None and outside_q["severe_loss_rate"] is not None:
                    score += 2.0 * (inside_q["severe_loss_rate"] - outside_q["severe_loss_rate"])
                total_score += score
        row["axis_score"] = total_score
        leaderboard.append(row)
        for _, r in rows[rows[col]].iterrows():
            detail_rows.append(
                {
                    "axis_id": axis["axis_id"],
                    "axis_family": axis["axis_family"],
                    "as_of_date": r["as_of_date"],
                    "code": r["code"],
                    "baseline_top_rank": r["baseline_top_rank"],
                    "ret20": r["ret20"],
                    "ret5": r["ret5"],
                    "hit_flag": r["hit_flag"],
                    "failed_hit_flag": r["failed_hit_flag"],
                    "bottom_return_flag": r["bottom_return_flag"],
                    "severe_loss_flag": r["severe_loss_flag"],
                    "invalidation_hit_20d": r["invalidation_hit_20d"],
                    "upper_wick_ratio": r["upper_wick_ratio"],
                    "recent_high_distance_pct": r["recent_high_distance_pct"],
                    "atr14_pct": r["atr14_pct"],
                    "realized_vol20": r["realized_vol20"],
                    "support_distance_pct": r["support_distance_pct"],
                    "monthly_supportive_flag": r["monthly_supportive_flag"],
                    "weekly_supportive_flag": r["weekly_supportive_flag"],
                }
            )
    leaderboard.sort(key=lambda x: x["axis_score"], reverse=True)
    return leaderboard, pd.DataFrame(detail_rows)


def _gate(leaderboard: list[dict[str, Any]]) -> dict[str, Any]:
    gated = []
    for axis in leaderboard:
        top10 = next(item for item in axis["topk"] if item["topk"] == 10)
        top5 = next(item for item in axis["topk"] if item["topk"] == 5)
        inside = top10["inside"]
        outside = top10["outside"]
        passes = (
            top10["sample_count"] >= 8
            and top10["bad_pick_capture_count"] >= 2
            and top10["bad_pick_capture_rate"] >= 0.25
            and top10["good_pick_false_removal_rate"] <= 0.25
            and inside["mean_ret20"] is not None
            and outside["mean_ret20"] is not None
            and inside["mean_ret20"] < outside["mean_ret20"]
            and inside["severe_loss_rate"] is not None
            and outside["severe_loss_rate"] is not None
            and inside["severe_loss_rate"] > outside["severe_loss_rate"]
            and top5["good_pick_false_removal_rate"] <= 0.35
        )
        reasons = []
        if top10["sample_count"] < 8:
            reasons.append("top10_sample_too_small")
        if top10["bad_pick_capture_count"] < 2 or top10["bad_pick_capture_rate"] < 0.25:
            reasons.append("bad_pick_capture_insufficient")
        if top10["good_pick_false_removal_rate"] > 0.25:
            reasons.append("good_pick_false_removal_too_high")
        if not (inside["mean_ret20"] is not None and outside["mean_ret20"] is not None and inside["mean_ret20"] < outside["mean_ret20"]):
            reasons.append("inside_return_not_worse_than_outside")
        if not (inside["severe_loss_rate"] is not None and outside["severe_loss_rate"] is not None and inside["severe_loss_rate"] > outside["severe_loss_rate"]):
            reasons.append("severe_loss_not_enriched")
        if top5["good_pick_false_removal_rate"] > 0.35:
            reasons.append("top5_false_removal_risk")
        gated.append(
            {
                "axis_id": axis["axis_id"],
                "axis_family": axis["axis_family"],
                "axis_score": axis["axis_score"],
                "passes_gate": passes,
                "gate_reasons": reasons if not passes else ["passed_single_axis_gate"],
                "top10_key_metrics": {
                    "sample_count": top10["sample_count"],
                    "bad_pick_capture_count": top10["bad_pick_capture_count"],
                    "bad_pick_capture_rate": top10["bad_pick_capture_rate"],
                    "good_pick_false_removal_count": top10["good_pick_false_removal_count"],
                    "good_pick_false_removal_rate": top10["good_pick_false_removal_rate"],
                    "inside_mean_ret20": inside["mean_ret20"],
                    "outside_mean_ret20": outside["mean_ret20"],
                    "inside_severe_loss_rate": inside["severe_loss_rate"],
                    "outside_severe_loss_rate": outside["severe_loss_rate"],
                    "net_replacement_potential": top10["net_replacement_potential"],
                    "branching_potential": top10["branching_potential"],
                },
            }
        )
    passed = [g for g in gated if g["passes_gate"]]
    if len(passed) == 1:
        decision = "propose_single_next_challenger"
        reason = "exactly_one_independent_axis_passed_bad_pick_removal_gate"
    elif len(passed) > 1:
        decision = "keep_diagnostic_only"
        reason = "multiple_axes_passed_or_overlap_requires_single_axis_selection_before_challenger"
    elif any(g["top10_key_metrics"]["bad_pick_capture_count"] > 0 for g in gated):
        decision = "keep_diagnostic_only"
        reason = "patterns_exist_but_not_strong_enough_for_single_challenger"
    else:
        decision = "drop_axis_family"
        reason = "no_independent_bad_pick_concentration_found"
    return {"decision": decision, "reason": reason, "passed_axes": passed, "gate_rows": gated}


def run(args: argparse.Namespace) -> Path:
    frozen = _read_json(args.frozen_ma_decision)
    if frozen.get("authoritative_rollup_decision") != "park_low_priority":
        raise RuntimeError("MA phase frozen decision is not park_low_priority")
    out_dir = args.out_root / f"{_now_tag()}-{AXIS_ID.replace('_', '-')}"
    out_dir.mkdir(parents=True, exist_ok=False)
    rows = _load_rows(args.replay_rows)
    rows, axes, unavailable = _add_axes(rows)
    leaderboard, detail = _axis_metrics(rows, axes)
    topk_summary = _topk_bad_pick_summary(rows)
    gate = _gate(leaderboard)
    best = leaderboard[0] if leaderboard else None

    detail.to_csv(out_dir / "bad_pick_decomposition_detail.csv", index=False, encoding="utf-8")
    _write_json(out_dir / "bad_pick_axis_leaderboard.json", {"axis_id": AXIS_ID, "axes": leaderboard})
    _write_json(out_dir / "topk_bad_pick_summary.json", {"axis_id": AXIS_ID, **topk_summary})
    _write_json(
        out_dir / "feature_slice_quality_summary.json",
        {
            "axis_id": AXIS_ID,
            "available_axis_count": len(axes),
            "unavailable_requested_axes": unavailable,
            "feature_slices": [
                {
                    "axis_id": axis["axis_id"],
                    "axis_family": axis["axis_family"],
                    "definition": axis["definition"],
                    "quality": _quality(rows[rows[axis["flag_column"]]]),
                    "outside_quality": _quality(rows[~rows[axis["flag_column"]]]),
                }
                for axis in axes
            ],
        },
    )
    _write_json(out_dir / "candidate_axis_gate_summary.json", {"axis_id": AXIS_ID, **gate})

    decision_payload = {
        "axis_id": AXIS_ID,
        "authoritative_rollup_decision": gate["decision"],
        "reason": gate["reason"],
        "frozen_ma_phase_source": str(args.frozen_ma_decision),
        "ma_phase_frozen_confirmed": True,
        "source_replay_rows": str(args.replay_rows),
        "same_condition": {
            "same_replay_rows": True,
            "same_baseline_rank_source": "fresh_runtime_research_watch_rank",
            "same_period": True,
            "same_topk_evaluation": True,
            "candidate_set_changed": False,
        },
        "best_bad_pick_removal_axis": {
            "axis_id": best["axis_id"] if best else None,
            "axis_family": best["axis_family"] if best else None,
            "axis_score": best["axis_score"] if best else None,
            "definition": best["definition"] if best else None,
            "top10": next((item for item in best["topk"] if item["topk"] == 10), None) if best else None,
        },
        "gate_summary": gate,
        "do_not_change_confirmed": [
            "MeeMee",
            "runtime DB",
            "ranking",
            "publish",
            "production candidate generation",
            "live buy/sell rules",
            "frozen replay-specific exit champion",
            "MA thresholds",
            "exit thresholds",
            "baseline rank source",
            "replay rows",
            "period",
            "top-K evaluation",
        ],
        "unavailable_requested_axes": unavailable,
        "next_required_single_step": (
            "build_exactly_one_read_only_challenger_for_passed_axis"
            if gate["decision"] == "propose_single_next_challenger"
            else "keep_as_diagnostic_and_move_to_next_independent_bad_pick_family_or_expand_source_contract"
        ),
        "boundary_flags": {
            "tradex_only": True,
            "read_only_diagnostic": True,
            "runtime_db_write": False,
            "meemee_reflection": False,
            "ranking_change": False,
            "publish": False,
            "production_candidate_generation_change": False,
            "live_rule_promotion_allowed": False,
            "ma_phase_mixed_in": False,
            "ma_threshold_tuning": False,
            "exit_threshold_tuning": False,
            "challenger_implemented": False,
        },
    }
    _write_json(out_dir / "final_research_decision.json", decision_payload)
    _write_json(
        out_dir / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "status": "complete",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "required_files": list(REQUIRED),
            "required_files_present": all((out_dir / name).exists() for name in REQUIRED if name != "_ARTIFACT_COMPLETE.json"),
        },
    )
    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover independent non-MA bad-pick removal axes under fixed replay conditions.")
    parser.add_argument("--replay-rows", type=Path, default=DEFAULT_REPLAY_ROWS)
    parser.add_argument("--frozen-ma-decision", type=Path, default=DEFAULT_FROZEN_MA_DECISION)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    args = parser.parse_args()
    print(run(args))


if __name__ == "__main__":
    main()
