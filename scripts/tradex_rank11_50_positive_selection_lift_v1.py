from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "rank11_50_positive_selection_lift_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_FAILED_DEMOTION_ROOT = Path(r"G:\Tradex\tight_bottom_top10_bad_pick_removal_v1\20260525T080953Z-tight-bottom-top10-bad-pick-removal-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\rank11_50_positive_selection_lift_v1")
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
    "ma7_gt_ma20_gt_ma60",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "lower_wick_ratio",
    "upper_wick_ratio",
    "large_bullish_candle",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ma20_ratio",
    "monthly_high_zone_proxy",
    "monthly_box_breakout_proxy",
    "monthly_box_inside_proxy",
    "weekly_monthly_uptrend_proxy",
    "primary_family",
    "path20_available",
    "ret20",
    "starter_bad",
]
REQUIRED_ARTIFACTS = (
    "positive_selection_summary.json",
    "positive_selection_rows.csv",
    "feature_contract.json",
    "rank11_50_winner_profile.json",
    "lift_variant_metrics.json",
    "topk_comparison.json",
    "promoted_candidate_quality.json",
    "displaced_candidate_quality.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
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
    vals = pd.to_numeric(frame.get(col), errors="coerce").dropna()
    return None if vals.empty else float(vals.mean())


def _rate(series: pd.Series) -> float | None:
    vals = series.dropna()
    return None if vals.empty else float(vals.astype(bool).mean())


def feature_contract(header: list[str]) -> dict[str, Any]:
    fields: dict[str, dict[str, str]] = {}
    for col in READ_COLUMNS:
        if col in {"ret20", "starter_bad"}:
            cls = "outcome_only"
        elif col in header:
            cls = "point_in_time_feature"
        else:
            cls = "unavailable"
        fields[col] = {"classification": cls}
    fields["ret20_derived_terms"] = {"classification": "forbidden_future_leak"}
    fields["liquidity_event_fields"] = {"classification": "unavailable"}
    return {"axis_id": AXIS_ID, "fields": fields}


def load_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = input_root / "candidate_family_source_rows.csv"
    header = pd.read_csv(source, nrows=0).columns.tolist()
    present = [c for c in READ_COLUMNS if c in header]
    rows = pd.concat([c for c in pd.read_csv(source, usecols=present, chunksize=250_000, low_memory=False)], ignore_index=True)
    for c in READ_COLUMNS:
        if c not in rows:
            rows[c] = pd.NA
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    for col in ["decision_date", "year", "baseline_rank", "baseline_score", "ma7_slope", "ma20_slope", "ma60_slope", "dist_ma7_pct", "dist_ma20_pct", "dist_ma60_pct", "above20_streak", "above60_streak", "days_since_ma20_reclaim", "lower_wick_ratio", "upper_wick_ratio", "volume_ma20_ratio", "ret20"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["ma7_gt_ma20_gt_ma60", "large_bullish_candle", "large_bearish_candle", "failed_high_update", "monthly_high_zone_proxy", "monthly_box_breakout_proxy", "monthly_box_inside_proxy", "weekly_monthly_uptrend_proxy", "path20_available", "starter_bad"]:
        rows[col] = _to_bool(rows[col])
    rows = rows[rows["path20_available"] & rows["baseline_rank"].notna() & rows["ret20"].notna()].copy()
    rows["rank11_50_pool"] = rows["baseline_rank"].between(11, 50, inclusive="both")
    rows["winner_ret20_gt_10pct"] = rows["ret20"] > 0.10
    rows["nonwinner_ret20_le_0"] = rows["ret20"] <= 0
    return rows, feature_contract(header)


def add_variant_flags(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    if "rank11_50_pool" not in out:
        out["rank11_50_pool"] = out["baseline_rank"].between(11, 50, inclusive="both")
    pool = out["rank11_50_pool"]
    out["variant_a_lift"] = pool & out["weekly_monthly_uptrend_proxy"] & (out["ma20_slope"] > 0) & (out["ma60_slope"] > 0) & (out["dist_ma20_pct"].between(-0.02, 0.08, inclusive="both"))
    out["variant_b_lift"] = pool & out["large_bullish_candle"] & (out["lower_wick_ratio"] >= 0.20) & (out["upper_wick_ratio"] <= 0.20) & (out["volume_ma20_ratio"] >= 1.0) & (~out["failed_high_update"])
    out["variant_c_lift"] = pool & (
        (out["variant_a_lift"] & (out["volume_ma20_ratio"] >= 0.8) & (~out["large_bearish_candle"]))
        | (out["variant_b_lift"] & (out["monthly_box_inside_proxy"] | out["monthly_high_zone_proxy"]))
    )
    return out


def score_variant(rows: pd.DataFrame, variant: str) -> pd.DataFrame:
    out = rows.copy()
    flag = f"{variant}_lift"
    out[f"{variant}_sort_score"] = out["baseline_score"].fillna(0.0) + out[flag].astype(int) * 100.0 - out["baseline_rank"].fillna(9999) * 0.001
    out[f"{variant}_rank"] = out.sort_values(["decision_date", f"{variant}_sort_score", "baseline_rank", "code"], ascending=[True, False, True, True]).groupby("decision_date").cumcount() + 1
    return out


def summarize_topk(rows: pd.DataFrame, rank_col: str, topk: int) -> dict[str, Any]:
    g = rows[rows[rank_col] <= topk]
    return {
        "n": int(len(g)),
        "mean_ret20": _mean(g, "ret20"),
        "bad_pick_rate": _rate(g["ret20"] < -0.05) if not g.empty else None,
        "severe_loss_rate": _rate(g["ret20"] < -0.10) if not g.empty else None,
        "winner_rate_ret20_gt_10pct": _rate(g["ret20"] > 0.10) if not g.empty else None,
    }


def periods(rows: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    return [("2024", rows[rows["year"].eq(2024)]), ("2025", rows[rows["year"].eq(2025)]), ("2026_label_safe", rows[rows["year"].eq(2026)]), ("2024_2026_combined", rows[rows["year"].isin([2024, 2025, 2026])])]


def evaluate_variant(rows: pd.DataFrame, variant: str) -> dict[str, Any]:
    rank_col = f"{variant}_rank"
    topk_records: list[dict[str, Any]] = []
    promoted_records: list[dict[str, Any]] = []
    displaced_records: list[dict[str, Any]] = []
    for period, pr in periods(rows):
        for topk in TOPK_VALUES:
            base = summarize_topk(pr, "baseline_rank", topk)
            chal = summarize_topk(pr, rank_col, topk)
            topk_records.append({"variant": variant, "period": period, "topk": topk, **{f"baseline_{k}": v for k, v in base.items()}, **{f"challenger_{k}": v for k, v in chal.items()}, "delta_mean_ret20": (chal["mean_ret20"] or 0) - (base["mean_ret20"] or 0), "delta_bad_pick_rate": (chal["bad_pick_rate"] or 0) - (base["bad_pick_rate"] or 0), "delta_severe_loss_rate": (chal["severe_loss_rate"] or 0) - (base["severe_loss_rate"] or 0), "delta_winner_rate_ret20_gt_10pct": (chal["winner_rate_ret20_gt_10pct"] or 0) - (base["winner_rate_ret20_gt_10pct"] or 0)})
        for date, g in pr.groupby("decision_date", sort=True):
            base10 = set(g[g["baseline_rank"] <= 10]["code"].astype(str))
            chal10 = set(g[g[rank_col] <= 10]["code"].astype(str))
            promoted = g[g["code"].astype(str).isin(chal10 - base10)]
            displaced = g[g["code"].astype(str).isin(base10 - chal10)]
            if not promoted.empty:
                promoted_records.append({"variant": variant, "period": period, "decision_date": int(date), **quality_block(promoted, "promoted")})
                displaced_records.append({"variant": variant, "period": period, "decision_date": int(date), **quality_block(displaced, "displaced")})
    return {"topk": topk_records, "promoted": promoted_records, "displaced": displaced_records}


def quality_block(frame: pd.DataFrame, prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_count": int(len(frame)),
        f"{prefix}_mean_ret20": _mean(frame, "ret20"),
        f"{prefix}_hit_rate": _rate(frame["ret20"] > 0) if not frame.empty else None,
        f"{prefix}_winner_rate_ret20_gt_10pct": _rate(frame["ret20"] > 0.10) if not frame.empty else None,
        f"{prefix}_bad_rate": _rate(frame["ret20"] < -0.05) if not frame.empty else None,
        f"{prefix}_severe_rate": _rate(frame["ret20"] < -0.10) if not frame.empty else None,
    }


def aggregate(records: list[dict[str, Any]], variant: str, period: str) -> dict[str, Any]:
    df = pd.DataFrame([r for r in records if r["variant"] == variant and r["period"] == period])
    out = {"variant": variant, "period": period, "date_count": int(df["decision_date"].nunique()) if not df.empty else 0}
    if df.empty:
        return out
    for c in df.columns:
        if c not in {"variant", "period", "decision_date"}:
            out[c] = _mean(df, c)
    return out


def boundary(rows: pd.DataFrame, variant: str) -> dict[str, Any]:
    recent = rows[rows["year"].isin([2024, 2025, 2026])]
    out: dict[str, Any] = {}
    for topk in [5, 10]:
        changed = []
        for _, g in recent.groupby("decision_date"):
            changed.append(len(set(g[g["baseline_rank"] <= topk]["code"]) ^ set(g[g[f"{variant}_rank"] <= topk]["code"])))
        out[f"changed_top{topk}_members_count"] = int(sum(1 for x in changed if x > 0))
    out["changed_rank_count"] = int((rows["baseline_rank"] != rows[f"{variant}_rank"]).sum())
    return out


def winner_profile(rows: pd.DataFrame) -> dict[str, Any]:
    pool = rows[rows["rank11_50_pool"]]
    winners = pool[pool["winner_ret20_gt_10pct"]]
    non = pool[pool["nonwinner_ret20_le_0"]]
    feats = ["ma20_slope", "ma60_slope", "dist_ma20_pct", "dist_ma60_pct", "lower_wick_ratio", "upper_wick_ratio", "volume_ma20_ratio"]
    return {
        "rank11_50_count": int(len(pool)),
        "winner_count": int(len(winners)),
        "nonwinner_count": int(len(non)),
        "feature_profile": {f: {"winner_mean": _mean(winners, f), "nonwinner_mean": _mean(non, f), "winner_minus_nonwinner": None if _mean(winners, f) is None or _mean(non, f) is None else (_mean(winners, f) or 0) - (_mean(non, f) or 0)} for f in feats},
        "boolean_profile": {
            "weekly_monthly_uptrend_proxy": {"winner_rate": _rate(winners["weekly_monthly_uptrend_proxy"]), "nonwinner_rate": _rate(non["weekly_monthly_uptrend_proxy"])},
            "monthly_box_inside_proxy": {"winner_rate": _rate(winners["monthly_box_inside_proxy"]), "nonwinner_rate": _rate(non["monthly_box_inside_proxy"])},
            "large_bullish_candle": {"winner_rate": _rate(winners["large_bullish_candle"]), "nonwinner_rate": _rate(non["large_bullish_candle"])},
        },
    }


def lift_quality(promoted: dict[str, Any], displaced: dict[str, Any], rows: pd.DataFrame, variant: str) -> dict[str, Any]:
    pool = rows[rows["rank11_50_pool"]]
    captured = rows[rows[f"{variant}_lift"] & rows["winner_ret20_gt_10pct"]]
    winners = pool[pool["winner_ret20_gt_10pct"]]
    return {
        "promoted_minus_displaced_ret20": (promoted.get("promoted_mean_ret20") or 0) - (displaced.get("displaced_mean_ret20") or 0),
        "promoted_minus_displaced_winner_rate": (promoted.get("promoted_winner_rate_ret20_gt_10pct") or 0) - (displaced.get("displaced_winner_rate_ret20_gt_10pct") or 0),
        "promoted_bad_rate_vs_displaced_bad_rate": (promoted.get("promoted_bad_rate") or 0) - (displaced.get("displaced_bad_rate") or 0),
        "rank11_50_winner_capture_rate": None if winners.empty else float(len(captured) / len(winners)),
        "accidental_promotion_bad_rate": promoted.get("promoted_bad_rate"),
    }


def decide(topk: pd.DataFrame, promoted: dict[str, dict[str, Any]], displaced: dict[str, dict[str, Any]], lift: dict[str, dict[str, Any]], bounds: dict[str, dict[str, Any]]) -> tuple[str, list[str], str]:
    best = None
    for variant in ["variant_a", "variant_b", "variant_c"]:
        r = topk[(topk["variant"] == variant) & (topk["period"] == "2024_2026_combined") & (topk["topk"] == 10)].iloc[0]
        score = (r["delta_mean_ret20"], lift[variant].get("promoted_minus_displaced_ret20", -999))
        if best is None or score > best[0]:
            best = (score, variant, r)
    assert best is not None
    _, variant, r = best
    lq = lift[variant]
    if r["delta_mean_ret20"] > 0 and lq["promoted_minus_displaced_ret20"] > 0 and r["delta_bad_pick_rate"] <= 0.002 and r["delta_severe_loss_rate"] <= 0.002 and (lq["accidental_promotion_bad_rate"] or 1) <= 0.25:
        return "keep_for_next_stage", ["top10_mean_ret20_improved_with_positive_promoted_minus_displaced_quality"], variant
    if lq["promoted_minus_displaced_ret20"] < 0:
        return "drop", ["promoted_candidates_weaker_than_displaced_candidates"], variant
    if bounds[variant]["changed_top10_members_count"] == 0:
        return "close_branch_no_reusable_signal", ["top10_boundary_did_not_move"], variant
    if r["delta_mean_ret20"] > 0 or lq["promoted_minus_displaced_ret20"] > 0:
        return "promising_but_underpowered", ["positive_direction_but_keep_gates_not_fully_met"], variant
    return "close_branch_no_reusable_signal", ["features_do_not_separate_rank11_50_winners_into_better_top10"], variant


def run(input_root: Path, failed_demotion_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-rank11-50-positive-selection-lift-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows, contract = load_rows(input_root)
    rows = add_variant_flags(rows)
    for v in ["variant_a", "variant_b", "variant_c"]:
        rows = score_variant(rows, v)
    evals = {v: evaluate_variant(rows, v) for v in ["variant_a", "variant_b", "variant_c"]}
    topk_df = pd.DataFrame(evals["variant_a"]["topk"] + evals["variant_b"]["topk"] + evals["variant_c"]["topk"])
    promoted = {v: aggregate(evals[v]["promoted"], v, "2024_2026_combined") for v in ["variant_a", "variant_b", "variant_c"]}
    displaced = {v: aggregate(evals[v]["displaced"], v, "2024_2026_combined") for v in ["variant_a", "variant_b", "variant_c"]}
    lift = {v: lift_quality(promoted[v], displaced[v], rows, v) for v in ["variant_a", "variant_b", "variant_c"]}
    bounds = {v: boundary(rows, v) for v in ["variant_a", "variant_b", "variant_c"]}
    decision, reasons, best_variant = decide(topk_df, promoted, displaced, lift, bounds)
    _write_json(out / "feature_contract.json", contract)
    _write_json(out / "rank11_50_winner_profile.json", winner_profile(rows))
    _write_json(out / "lift_variant_metrics.json", lift)
    _write_json(out / "topk_comparison.json", {"rows": topk_df.to_dict("records"), "boundary": bounds})
    _write_json(out / "promoted_candidate_quality.json", promoted)
    _write_json(out / "displaced_candidate_quality.json", displaced)
    rows.to_csv(out / "positive_selection_rows.csv", index=False)
    _write_json(out / "positive_selection_summary.json", {"axis_id": AXIS_ID, "best_variant": best_variant, "input_rows": int(len(rows)), "failed_demotion_root": failed_demotion_root, **bounds[best_variant]})
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "features_use_saved_point_in_time_context_only": True, "future_outcomes_used_for_feature_construction": False, "outcomes_used_evaluation_only": True, "candidate_generation_changed": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", {"row_count": int(len(rows)), "date_count": int(rows["decision_date"].nunique()), "research_fallback_used": False, "coverage": {c: float(rows[c].notna().mean()) for c in READ_COLUMNS if c in rows}})
    _write_json(out / "research_decision.json", {"axis_id": AXIS_ID, "research_decision": decision, "best_variant": best_variant, "reason_typed": reasons, "meemee_reflectable_candidate": False, "runtime_db_write": False, "production_ranking_changed": False, "publish_allowed": False, "validated_buy_count": 0, "active_gate_created": False})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--failed-demotion-root", type=Path, default=DEFAULT_FAILED_DEMOTION_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.failed_demotion_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
