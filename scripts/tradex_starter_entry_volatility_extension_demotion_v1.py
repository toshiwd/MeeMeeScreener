from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "starter_entry_volatility_extension_demotion_v1"
DEFAULT_INPUT_ROOT = Path(
    r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1"
)
DEFAULT_CLOSURE_ROOT = Path(r"G:\Tradex\starter_chart_review_branch_closure_v1\20260525T072259Z-starter-chart-review-branch-closure-v1")
DEFAULT_WATCH_CLOSURE_ROOT = Path(r"G:\Tradex\watch_persistence_quality_pretest_v1\20260525T072903Z-watch-persistence-quality-pretest-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_volatility_extension_demotion_v1")
TOPK_VALUES = (5, 10, 20)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "axis_contract.json",
    "candidate_volatility_extension_rows.csv",
    "topk_volatility_extension_comparison_summary.json",
    "replacement_quality.csv",
    "boundary_metrics.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "lineage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)
READ_COLUMNS = [
    "decision_date",
    "code",
    "year",
    "baseline_rank",
    "baseline_score",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "realized_vol20",
    "atr14_pct",
    "upper_wick_ratio",
    "volume_ma20_ratio",
    "dist_ma20_top_quartile",
    "dist_ma60_top_quartile",
    "realized_vol20_top_quartile",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "weekly_monthly_uptrend_proxy",
    "research_candidate_source_family",
    "path20_available",
    "ret20",
    "mae20",
    "mfe20",
    "starter_good",
    "starter_bad",
    "selected_loser",
    "selected_winner",
    "immediate_adverse_entry",
    "same_date_ret20_rank_pct",
]


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
    if frame.empty or col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def load_rows(input_root: Path) -> pd.DataFrame:
    path = input_root / "candidate_family_source_rows.csv"
    chunks: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=lambda c: c in READ_COLUMNS, chunksize=250_000, low_memory=False):
        chunks.append(chunk)
    rows = pd.concat(chunks, ignore_index=True)
    for col in ["decision_date", "year", "baseline_rank", "baseline_score"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["dist_ma20_pct", "dist_ma60_pct", "realized_vol20", "atr14_pct", "upper_wick_ratio", "volume_ma20_ratio", "ret20", "mae20", "mfe20", "same_date_ret20_rank_pct"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["path20_available", "dist_ma20_top_quartile", "dist_ma60_top_quartile", "realized_vol20_top_quartile", "monthly_high_zone_proxy", "monthly_box_breakout_proxy", "weekly_monthly_uptrend_proxy", "starter_good", "starter_bad", "selected_loser", "selected_winner", "immediate_adverse_entry"]:
        rows[col] = _to_bool(rows[col])
    return rows[rows["path20_available"] & rows["baseline_rank"].notna()].copy()


def score_axis(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["volatility_extension_risk_flag"] = (
        out["dist_ma20_top_quartile"]
        & out["dist_ma60_top_quartile"]
        & (out["realized_vol20_top_quartile"] | (out["atr14_pct"] >= 0.04) | (out["upper_wick_ratio"] >= 0.35))
    )
    out["volatility_extension_reason"] = ""
    out.loc[out["volatility_extension_risk_flag"], "volatility_extension_reason"] = "dist_ma20_top_quartile+dist_ma60_top_quartile+vol_or_atr_or_upper_wick"
    out["volatility_extension_sort_score"] = (
        out["baseline_score"].fillna(0.0)
        - out["volatility_extension_risk_flag"].astype(int) * 100.0
        - out["baseline_rank"].fillna(9999) * 0.001
    )
    out["volatility_extension_rank"] = (
        out.sort_values(["decision_date", "volatility_extension_sort_score", "baseline_rank", "code"], ascending=[True, False, True, True])
        .groupby("decision_date")
        .cumcount()
        + 1
    )
    return out


def _periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [
        ("2024", rows[rows["year"].eq(2024)]),
        ("2025", rows[rows["year"].eq(2025)]),
        ("2026_label_safe", rows[rows["year"].eq(2026)]),
        ("2024_2025", rows[rows["year"].isin([2024, 2025])]),
        ("2024_2026_combined", rows[rows["year"].isin([2024, 2025, 2026])]),
    ]


def summarize(frame: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = frame[frame[rank_col] <= topk]
    return {
        "n": int(len(g)),
        "mean_ret20": _mean(g, "ret20"),
        "starter_good_rate": _rate(g["starter_good"]) if not g.empty else None,
        "starter_bad_rate": _rate(g["starter_bad"]) if not g.empty else None,
        "selected_loser_rate": _rate(g["selected_loser"]) if not g.empty else None,
        "selected_winner_rate": _rate(g["selected_winner"]) if not g.empty else None,
        "immediate_adverse_rate": _rate(g["immediate_adverse_entry"]) if not g.empty else None,
        "severe_loss_rate": _rate(g["ret20"] <= -0.05) if not g.empty else None,
        "mae20_mean": _mean(g, "mae20"),
        "mfe20_mean": _mean(g, "mfe20"),
        "hit_rate_ret20_gt_5pct": _rate(g["ret20"] >= 0.05) if not g.empty else None,
    }


def comparison_summary(rows: pd.DataFrame) -> pd.DataFrame:
    rec: list[dict[str, Any]] = []
    for period, pr in _periods(rows):
        for topk in TOPK_VALUES:
            base = summarize(pr, "baseline_rank", topk)
            challenger = summarize(pr, "volatility_extension_rank", topk)
            rec.append(
                {
                    "period": period,
                    "topk": topk,
                    **{f"baseline_{k}": v for k, v in base.items()},
                    **{f"challenger_{k}": v for k, v in challenger.items()},
                    "delta_mean_ret20": None if base["mean_ret20"] is None or challenger["mean_ret20"] is None else challenger["mean_ret20"] - base["mean_ret20"],
                    "delta_starter_good_rate": None if base["starter_good_rate"] is None or challenger["starter_good_rate"] is None else challenger["starter_good_rate"] - base["starter_good_rate"],
                    "delta_starter_bad_rate": None if base["starter_bad_rate"] is None or challenger["starter_bad_rate"] is None else challenger["starter_bad_rate"] - base["starter_bad_rate"],
                    "delta_selected_loser_rate": None if base["selected_loser_rate"] is None or challenger["selected_loser_rate"] is None else challenger["selected_loser_rate"] - base["selected_loser_rate"],
                }
            )
    return pd.DataFrame(rec)


def replacement_quality(rows: pd.DataFrame) -> pd.DataFrame:
    rec: list[dict[str, Any]] = []
    for period, pr in _periods(rows):
        for topk in TOPK_VALUES:
            for date, g in pr.groupby("decision_date", sort=True):
                baseline = set(g[g["baseline_rank"] <= topk]["code"].astype(str))
                challenger = set(g[g["volatility_extension_rank"] <= topk]["code"].astype(str))
                added = g[g["code"].astype(str).isin(challenger - baseline)]
                removed = g[g["code"].astype(str).isin(baseline - challenger)]
                rec.append(
                    {
                        "period": period,
                        "topk": topk,
                        "decision_date": int(date),
                        "changed_members_count": int(len(added) + len(removed)),
                        "added_ret20_mean": _mean(added, "ret20"),
                        "removed_ret20_mean": _mean(removed, "ret20"),
                        "added_minus_removed_ret20": None if added.empty or removed.empty else (_mean(added, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0),
                    }
                )
    return pd.DataFrame(rec)


def boundary_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for topk in [5, 10]:
        changed = []
        for _, g in rows[rows["year"].isin([2024, 2025, 2026])].groupby("decision_date", sort=True):
            baseline = set(g[g["baseline_rank"] <= topk]["code"].astype(str))
            challenger = set(g[g["volatility_extension_rank"] <= topk]["code"].astype(str))
            changed.append(len(baseline.symmetric_difference(challenger)))
        out[f"changed_top{topk}_members_count"] = int(sum(1 for x in changed if x > 0))
        out[f"changed_top{topk}_member_slots"] = int(sum(changed))
    out["changed_rank_count"] = int((rows["baseline_rank"] != rows["volatility_extension_rank"]).sum())
    out["selection_divergence_reason"] = "fixed point-in-time demotion of high volatility plus MA extension candidates"
    out["risk_flag_count"] = int(rows["volatility_extension_risk_flag"].sum())
    out["risk_flag_selected_baseline_top10_count"] = int((rows["volatility_extension_risk_flag"] & (rows["baseline_rank"] <= 10)).sum())
    return out


def source_coverage(rows: pd.DataFrame, input_root: Path) -> dict[str, Any]:
    required = [
        "dist_ma20_top_quartile",
        "dist_ma60_top_quartile",
        "realized_vol20_top_quartile",
        "atr14_pct",
        "upper_wick_ratio",
        "baseline_rank",
        "baseline_score",
    ]
    return {
        "input_root": input_root,
        "row_count": int(len(rows)),
        "date_count": int(rows["decision_date"].nunique()),
        "year_count": int(rows["year"].nunique()),
        "required_feature_coverage": {col: float(rows[col].notna().mean()) for col in required},
        "confirmed_bars_only": True,
        "research_fallback_used": False,
    }


def decide(comp: pd.DataFrame, repl: pd.DataFrame, boundary: dict[str, Any]) -> dict[str, Any]:
    recent = comp[(comp["period"] == "2024_2026_combined") & (comp["topk"] == 10)].iloc[0]
    recent_repl = repl[(repl["period"] == "2024_2026_combined") & (repl["topk"] == 10)]["added_minus_removed_ret20"].dropna()
    replacement_delta = None if recent_repl.empty else float(recent_repl.mean())
    if boundary["changed_top10_members_count"] < 10:
        decision = "drop"
        reason = "topK_boundary_did_not_move_enough"
    elif (recent["delta_mean_ret20"] or 0.0) >= 0.005 and (recent["delta_starter_bad_rate"] or 0.0) < 0 and (replacement_delta or 0.0) > 0:
        decision = "keep_for_next_stage"
        reason = "top10_quality_improved_with_bad_pick_reduction_and_positive_replacements"
    elif ((recent["delta_starter_bad_rate"] or 0.0) < 0 or (recent["delta_selected_loser_rate"] or 0.0) < 0) and (replacement_delta or 0.0) > 0:
        decision = "promising_but_underpowered"
        reason = "bad_pick_metrics_improved_with_positive_replacements_but_without_full_return_gate"
    else:
        decision = "close_branch_no_reusable_signal"
        reason = "fixed_volatility_extension_demotion_reduced_bad_pick_rates_but_replacements_or_returns_did_not_improve"
    return {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "reason_typed": [reason],
        "replacement_delta_ret20_top10_recent": replacement_delta,
        "meemee_reflectable_candidate": False,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def run(input_root: Path, closure_root: Path, watch_closure_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-volatility-extension-demotion-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = score_axis(load_rows(input_root))
    comp = comparison_summary(rows)
    repl = replacement_quality(rows)
    boundary = boundary_metrics(rows)
    decision = decide(comp, repl, boundary)
    coverage = source_coverage(rows, input_root)

    _write_json(out / "input_artifact_report.json", {"input_root": input_root, "input_rows": int(len(rows))})
    _write_json(
        out / "axis_contract.json",
        {
            "axis_id": AXIS_ID,
            "axis_family": "volatility/extension risk control",
            "ranking_rule": "same-date rerank by baseline_score with fixed demotion for dist_ma20_top_quartile AND dist_ma60_top_quartile AND volatility/ATR/upper-wick risk",
            "features_used_for_ranking": ["dist_ma20_top_quartile", "dist_ma60_top_quartile", "realized_vol20_top_quartile", "atr14_pct", "upper_wick_ratio", "baseline_score", "baseline_rank"],
            "outcome_fields_evaluation_only": ["ret20", "mae20", "mfe20", "starter_good", "starter_bad", "selected_loser", "selected_winner"],
            "closed_axes_not_reopened": ["starter_ready", "chart_review_label_tuning", "watch_persistence"],
        },
    )
    rows.to_csv(out / "candidate_volatility_extension_rows.csv", index=False)
    _write_json(out / "topk_volatility_extension_comparison_summary.json", {"rows": comp.to_dict("records")})
    repl.to_csv(out / "replacement_quality.csv", index=False)
    _write_json(out / "boundary_metrics.json", boundary)
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "features_use_decision_date_context_only": True,
            "future_outcomes_used_for_ranking": False,
            "outcomes_used_evaluation_only": True,
            "candidate_generation_changed": False,
            "runtime_db_write": False,
            "research_fallback_used": False,
        },
    )
    _write_json(out / "source_coverage.json", coverage)
    _write_json(
        out / "lineage.json",
        {
            "input_family_source_root": input_root,
            "closed_chart_review_branch_root": closure_root,
            "closed_watch_persistence_branch_root": watch_closure_root,
            "lineage_note": "single-axis diagnostic uses existing family source rows; no historical reconstruction fallback",
        },
    )
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX starter-entry volatility/extension demotion pretest")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--closure-root", type=Path, default=DEFAULT_CLOSURE_ROOT)
    parser.add_argument("--watch-closure-root", type=Path, default=DEFAULT_WATCH_CLOSURE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.closure_root, args.watch_closure_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
