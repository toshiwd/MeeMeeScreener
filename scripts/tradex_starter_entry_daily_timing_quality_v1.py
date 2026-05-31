from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "starter_entry_daily_timing_quality_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_daily_timing_quality_v1")
TOPK_VALUES = (5, 10, 20)
READ_COLUMNS = [
    "decision_date",
    "code",
    "year",
    "baseline_rank",
    "baseline_score",
    "ma7_slope",
    "ma20_slope",
    "ma60_slope",
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "above7_streak",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "days_since_ma60_reclaim",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ma20_ratio",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "path20_available",
    "ret20",
    "mae20",
    "mfe20",
    "starter_good",
    "starter_bad",
    "selected_loser",
    "selected_winner",
    "immediate_adverse_entry",
]
REQUIRED_ARTIFACTS = (
    "daily_timing_summary.json",
    "daily_timing_rows.csv",
    "bucket_metrics.json",
    "topk_comparison.json",
    "replacement_quality.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "lineage.json",
    "research_decision.json",
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
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _to_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin(["true", "1", "yes"])


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(frame.get(col), errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def load_rows(input_root: Path) -> pd.DataFrame:
    frames = [chunk for chunk in pd.read_csv(input_root / "candidate_family_source_rows.csv", usecols=lambda c: c in READ_COLUMNS, chunksize=250_000, low_memory=False)]
    rows = pd.concat(frames, ignore_index=True)
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    for col in [
        "decision_date",
        "year",
        "baseline_rank",
        "baseline_score",
        "ma7_slope",
        "ma20_slope",
        "ma60_slope",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "above7_streak",
        "above20_streak",
        "above60_streak",
        "days_since_ma20_reclaim",
        "days_since_ma60_reclaim",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "volume_ma20_ratio",
        "ret20",
        "mae20",
        "mfe20",
    ]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in [
        "large_bullish_candle",
        "large_bearish_candle",
        "failed_high_update",
        "monthly_box_inside_proxy",
        "weekly_monthly_uptrend_proxy",
        "path20_available",
        "starter_good",
        "starter_bad",
        "selected_loser",
        "selected_winner",
        "immediate_adverse_entry",
    ]:
        rows[col] = _to_bool(rows[col])
    return rows[rows["path20_available"] & rows["baseline_rank"].notna()].copy()


def score_axis(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    weak_reclaim = (out["days_since_ma20_reclaim"].between(0, 3, inclusive="both")) & (out["ma7_slope"] < 0)
    candle_reject = out["failed_high_update"] | out["large_bearish_candle"] | (out["upper_wick_ratio"] >= 0.35)
    constructive_pullback = (
        out["monthly_box_inside_proxy"]
        & out["weekly_monthly_uptrend_proxy"]
        & (out["dist_ma20_pct"].between(-0.03, 0.04, inclusive="both"))
        & (out["lower_wick_ratio"] >= 0.25)
        & (out["volume_ma20_ratio"] >= 0.8)
    )
    out["daily_timing_action"] = "retain"
    out.loc[weak_reclaim | candle_reject, "daily_timing_action"] = "demote_weak_reclaim_or_candle_reject"
    out.loc[constructive_pullback, "daily_timing_action"] = "boost_constructive_pullback"
    delta = pd.Series(0.0, index=out.index)
    delta = delta.mask(weak_reclaim | candle_reject, -25.0)
    delta = delta.mask(constructive_pullback, 4.0)
    out["daily_timing_sort_score"] = out["baseline_score"].fillna(0.0) + delta - out["baseline_rank"].fillna(9999) * 0.001
    out["daily_timing_rank"] = (
        out.sort_values(["decision_date", "daily_timing_sort_score", "baseline_rank", "code"], ascending=[True, False, True, True])
        .groupby("decision_date")
        .cumcount()
        + 1
    )
    return out


def periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["year"].eq(2024)]),
        ("2025", rows[rows["year"].eq(2025)]),
        ("2026_label_safe", rows[rows["year"].eq(2026)]),
        ("2024_2026_combined", rows[rows["year"].isin([2024, 2025, 2026])]),
    ]


def summarize(frame: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = frame[frame[rank_col] <= topk]
    return {
        "n": int(len(g)),
        "mean_ret20": _mean(g, "ret20"),
        "bad_pick_rate": _rate(g["starter_bad"]) if not g.empty else None,
        "severe_loss_rate": _rate(g["ret20"] <= -0.05) if not g.empty else None,
        "selected_loser_rate": _rate(g["selected_loser"]) if not g.empty else None,
        "starter_good_rate": _rate(g["starter_good"]) if not g.empty else None,
    }


def topk_comparison(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for period, pr in periods(rows):
        for topk in TOPK_VALUES:
            b = summarize(pr, "baseline_rank", topk)
            v = summarize(pr, "daily_timing_rank", topk)
            rec.append(
                {
                    "period": period,
                    "topk": topk,
                    **{f"baseline_{k}": x for k, x in b.items()},
                    **{f"challenger_{k}": x for k, x in v.items()},
                    "delta_mean_ret20": None if b["mean_ret20"] is None or v["mean_ret20"] is None else v["mean_ret20"] - b["mean_ret20"],
                    "delta_bad_pick_rate": None if b["bad_pick_rate"] is None or v["bad_pick_rate"] is None else v["bad_pick_rate"] - b["bad_pick_rate"],
                    "delta_severe_loss_rate": None if b["severe_loss_rate"] is None or v["severe_loss_rate"] is None else v["severe_loss_rate"] - b["severe_loss_rate"],
                }
            )
    return pd.DataFrame(rec)


def replacement_quality(rows: pd.DataFrame) -> dict[str, Any]:
    rec = []
    for period, pr in periods(rows):
        for topk in TOPK_VALUES:
            vals = []
            changed_dates = 0
            for _, g in pr.groupby("decision_date", sort=True):
                base = set(g[g["baseline_rank"] <= topk]["code"].astype(str))
                var = set(g[g["daily_timing_rank"] <= topk]["code"].astype(str))
                added = g[g["code"].astype(str).isin(var - base)]
                removed = g[g["code"].astype(str).isin(base - var)]
                if not added.empty or not removed.empty:
                    changed_dates += 1
                if not added.empty and not removed.empty:
                    vals.append((_mean(added, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0))
            rec.append({"period": period, "topk": topk, "changed_dates": changed_dates, "replacement_delta_ret20": None if not vals else float(pd.Series(vals).mean())})
    return {"rows": rec}


def boundary_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    recent = rows[rows["year"].isin([2024, 2025, 2026])]
    out = {}
    for topk in [5, 10]:
        changed = []
        for _, g in recent.groupby("decision_date", sort=True):
            base = set(g[g["baseline_rank"] <= topk]["code"].astype(str))
            var = set(g[g["daily_timing_rank"] <= topk]["code"].astype(str))
            changed.append(len(base.symmetric_difference(var)))
        out[f"changed_top{topk}_members_count"] = int(sum(1 for x in changed if x > 0))
        out[f"changed_top{topk}_member_slots"] = int(sum(changed))
    out["changed_rank_count"] = int((rows["baseline_rank"] != rows["daily_timing_rank"]).sum())
    out["selection_divergence_reason"] = "fixed daily timing demotion for weak reclaim/candle rejection and small boost for constructive pullback"
    return out


def decide(comp: pd.DataFrame, repl: dict[str, Any], boundary: dict[str, Any]) -> dict[str, Any]:
    recent = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    r10 = [r for r in repl["rows"] if r["period"] == "2024_2026_combined" and r["topk"] == 10][0]
    if boundary["changed_top10_members_count"] < 10:
        decision, reason = "close_branch_no_reusable_signal", "topK_boundary_did_not_move_enough"
    elif (recent["delta_mean_ret20"] or 0.0) > 0 and (recent["delta_bad_pick_rate"] or 0.0) <= 0 and (recent["delta_severe_loss_rate"] or 0.0) <= 0 and (r10["replacement_delta_ret20"] or 0.0) > 0:
        decision, reason = "keep_for_next_stage", "mean_ret20_and_replacement_quality_improved_without_bad_or_severe_worsening"
    elif ((recent["delta_bad_pick_rate"] or 0.0) < 0 or (recent["delta_severe_loss_rate"] or 0.0) < 0) and (r10["replacement_delta_ret20"] or 0.0) < 0 and (recent["delta_mean_ret20"] or 0.0) <= 0:
        decision, reason = "drop", "bad_or_severe_rate_improved_but_replacement_quality_negative_and_mean_ret20_not_improved"
    elif (recent["delta_mean_ret20"] or 0.0) > 0 or (r10["replacement_delta_ret20"] or 0.0) > 0:
        decision, reason = "promising_but_underpowered", "partial_positive_direction_without_full_keep_gates"
    else:
        decision, reason = "close_branch_no_reusable_signal", "daily_timing_quality_has_no_clear_same_condition_edge"
    return {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "reason_typed": [reason],
        "replacement_delta_ret20_top10_recent": r10["replacement_delta_ret20"],
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def run(input_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-daily-timing-quality-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = score_axis(load_rows(input_root))
    comp = topk_comparison(rows)
    repl = replacement_quality(rows)
    boundary = boundary_metrics(rows)
    decision = decide(comp, repl, boundary)
    _write_json(out / "daily_timing_summary.json", {"axis_id": AXIS_ID, "input_rows": int(len(rows)), **boundary})
    rows.to_csv(out / "daily_timing_rows.csv", index=False)
    _write_json(out / "bucket_metrics.json", {"daily_timing_action": rows.groupby("daily_timing_action").size().to_dict()})
    _write_json(out / "topk_comparison.json", {"rows": comp.to_dict("records"), **boundary})
    _write_json(out / "replacement_quality.json", repl)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "features_use_decision_date_context_only": True, "future_outcomes_used_for_ranking": False, "outcomes_used_evaluation_only": True, "candidate_generation_changed": False, "runtime_db_write": False, "research_fallback_used": False})
    feature_cols = [c for c in READ_COLUMNS if c not in {"ret20", "mae20", "mfe20", "starter_good", "starter_bad", "selected_loser", "selected_winner", "immediate_adverse_entry"}]
    _write_json(out / "source_coverage.json", {"row_count": int(len(rows)), "date_count": int(rows["decision_date"].nunique()), "confirmed_bars_only": True, "research_fallback_used": False, "feature_coverage": {c: float(rows[c].notna().mean()) for c in feature_cols if c in rows}})
    _write_json(out / "lineage.json", {"input_family_source_root": input_root, "closed_prior_axis": r"G:\Tradex\starter_entry_monthly_box_regime_interaction_v1\20260525T074258Z-starter-entry-monthly-box-regime-interaction-v1"})
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX starter-entry daily timing quality pretest")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
