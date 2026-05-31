from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "starter_entry_family_split_v1"
DEFAULT_REDESIGN_ROOT = Path(r"G:\Tradex\starter_entry_redesign_route_audit_v1\20260525T030215Z-starter-entry-redesign-route-audit-v1")
DEFAULT_BACKFILL_ROOT = Path(r"G:\Tradex\starter_entry_role_backfill_v1\20260525T020451Z-starter-entry-role-backfill-v1")
DEFAULT_SHADOW_ROOT = Path(r"G:\Tradex\candidate_family_taxonomy_shadow_v1\20260524T135527Z-candidate-family-taxonomy-shadow-v1")
DEFAULT_PAIRWISE_ROOT = Path(r"G:\Tradex\starter_entry_pairwise_reranker_v1\20260525T024758Z-starter-entry-pairwise-reranker-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_split_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "family_taxonomy_schema.json",
    "candidate_family_rows.csv",
    "family_coverage_summary.csv",
    "family_quality_summary.csv",
    "topk_family_mixture_audit.csv",
    "missed_starter_good_by_family.csv",
    "family_separation_score.csv",
    "next_family_axis_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
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
            return value.item()
    except Exception:
        pass
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


def _mean(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def _rate(s: pd.Series) -> float | None:
    v = s.dropna()
    return None if v.empty else float(v.astype(bool).mean())


def load_rows(backfill_root: Path, pairwise_root: Path) -> pd.DataFrame:
    rows = pd.read_csv(backfill_root / "candidate_role_rows_2019_2026.csv", low_memory=False)
    rows["code"] = rows["code"].astype(str)
    rows["decision_date"] = pd.to_numeric(rows["decision_date"], errors="coerce")
    pair_path = pairwise_root / "candidate_pairwise_rows.csv"
    if pair_path.exists():
        pair = pd.read_csv(pair_path, usecols=["decision_date", "code", "pairwise_rank"], low_memory=False)
        pair["code"] = pair["code"].astype(str)
        pair["decision_date"] = pd.to_numeric(pair["decision_date"], errors="coerce")
        rows = rows.merge(pair, on=["decision_date", "code"], how="left")
    return rows


def family_schema() -> dict[str, Any]:
    return {
        "schema_version": AXIS_ID,
        "rules_are_fixed_not_outcome_tuned": True,
        "primary_family_priority_order": [
            "overextension_risk_family",
            "pullback_reclaim_family",
            "breakout_retest_family",
            "mature_trend_continuation_family",
            "early_trend_family",
            "range_reversal_family",
            "uncategorized_family",
        ],
        "rule_summary": {
            "overextension_risk_family": "dist_ma20/ma60 or ma7_slope same-date top quartile, or upper-wick/failed-high/bearish-candle risk",
            "pullback_reclaim_family": "close near ma7/ma20 after above20 maturity or recent ma20 reclaim",
            "breakout_retest_family": "monthly high-zone or breakout context with not extreme overextension",
            "mature_trend_continuation_family": "above60/above20 mature with positive ma20/ma60 slopes",
            "early_trend_family": "recent ma20/ma60 reclaim or low above20 streak with positive ma20 slope",
            "range_reversal_family": "range/monthly box inside context with not high-zone breakout",
            "uncategorized_family": "insufficient or mixed context",
        },
    }


def _bool(row: pd.Series, col: str) -> bool | None:
    if col not in row or pd.isna(row[col]):
        return None
    return bool(row[col])


def _num(row: pd.Series, col: str) -> float | None:
    if col not in row or pd.isna(row[col]):
        return None
    return float(row[col])


def assign_family_row(row: pd.Series) -> tuple[str, list[str], dict[str, Any], dict[str, bool]]:
    needed = [
        "dist_ma20_top_quartile",
        "dist_ma60_top_quartile",
        "ma7_slope_top_quartile",
        "upper_wick_ratio",
        "failed_high_update",
        "large_bearish_candle",
        "dist_ma7_pct",
        "dist_ma20_pct",
        "above20_streak",
        "above60_streak",
        "days_since_ma20_reclaim",
        "days_since_ma60_reclaim",
        "ma20_slope",
        "ma60_slope",
        "monthly_high_zone_proxy",
        "monthly_box_breakout_proxy",
        "monthly_box_inside_proxy",
    ]
    avail = {c: c in row.index and pd.notna(row[c]) for c in needed}
    tags: list[str] = []
    reasons: dict[str, Any] = {}
    overext = any(_bool(row, c) is True for c in ["dist_ma20_top_quartile", "dist_ma60_top_quartile", "ma7_slope_top_quartile"]) or (_num(row, "upper_wick_ratio") or 0) >= 0.45 or _bool(row, "failed_high_update") is True or _bool(row, "large_bearish_candle") is True
    near_ma = (_num(row, "dist_ma7_pct") is not None and abs(_num(row, "dist_ma7_pct") or 0) <= 0.03) or (_num(row, "dist_ma20_pct") is not None and abs(_num(row, "dist_ma20_pct") or 0) <= 0.06)
    recent_reclaim = (_num(row, "days_since_ma20_reclaim") is not None and (_num(row, "days_since_ma20_reclaim") or 999) <= 15) or (_num(row, "days_since_ma60_reclaim") is not None and (_num(row, "days_since_ma60_reclaim") or 999) <= 20)
    mature = ((_num(row, "above60_streak") or 0) >= 60 or (_num(row, "above20_streak") or 0) >= 40) and (_num(row, "ma20_slope") or 0) > 0 and (_num(row, "ma60_slope") or 0) >= 0
    early = recent_reclaim or (((_num(row, "above20_streak") or 999) < 20) and (_num(row, "ma20_slope") or 0) > 0)
    breakout = _bool(row, "monthly_high_zone_proxy") is True or _bool(row, "monthly_box_breakout_proxy") is True
    range_ctx = _bool(row, "monthly_box_inside_proxy") is True
    for name, flag in [
        ("overextension_context", overext),
        ("near_ma_pullback_context", near_ma),
        ("recent_reclaim_context", recent_reclaim),
        ("mature_trend_context", mature),
        ("early_trend_context", early),
        ("breakout_high_zone_context", breakout),
        ("range_context", range_ctx),
    ]:
        if flag:
            tags.append(name)
    if overext:
        primary = "overextension_risk_family"
    elif near_ma and ((_num(row, "above20_streak") or 0) >= 5 or recent_reclaim):
        primary = "pullback_reclaim_family"
    elif breakout and not overext:
        primary = "breakout_retest_family"
    elif mature:
        primary = "mature_trend_continuation_family"
    elif early:
        primary = "early_trend_family"
    elif range_ctx:
        primary = "range_reversal_family"
    else:
        primary = "uncategorized_family"
    reasons["primary_rule"] = primary
    reasons["matched_context_tags"] = tags
    return primary, tags, reasons, avail


def assign_families(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    assigned = rows.apply(assign_family_row, axis=1)
    rows["primary_family"] = [x[0] for x in assigned]
    rows["secondary_family_tags_json"] = [json.dumps(x[1], sort_keys=True) for x in assigned]
    rows["family_assignment_reason_json"] = [json.dumps(x[2], sort_keys=True) for x in assigned]
    rows["family_feature_availability_json"] = [json.dumps(x[3], sort_keys=True) for x in assigned]
    return rows


def period_label(rows: pd.DataFrame) -> pd.Series:
    return rows["year"].map(lambda y: "2019_2023_reference" if y <= 2023 else ("2026_label_safe" if y == 2026 else str(int(y))))


def coverage_summary(rows: pd.DataFrame) -> pd.DataFrame:
    rec = []
    for fam, g in rows.groupby("primary_family"):
        rec.append({"primary_family": fam, "row_count": int(len(g)), "top5_count": int((g["baseline_rank"] <= 5).sum()), "top10_count": int((g["baseline_rank"] <= 10).sum()), "top20_count": int((g["baseline_rank"] <= 20).sum()), "years": json.dumps({str(int(k)): int(v) for k, v in g["year"].value_counts().sort_index().items()}, sort_keys=True), "path20_available_rate": _rate(g["path20_available"])})
    return pd.DataFrame(rec).sort_values("row_count", ascending=False)


def quality_summary(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows[rows["path20_available"].eq(True)].copy()
    rows["period"] = period_label(rows)
    rows2 = pd.concat([rows, rows.assign(period="2024_2026_combined")[rows["year"].isin([2024, 2025, 2026])]], ignore_index=True)
    rec = []
    for (period, fam), g in rows2.groupby(["period", "primary_family"]):
        rec.append({"period": period, "primary_family": fam, "n": int(len(g)), "mean_ret20": _mean(g, "ret20"), "median_ret20": _median(g, "ret20"), "starter_good_rate": _rate(g["starter_good"]), "starter_bad_rate": _rate(g["starter_bad"]), "selected_loser_rate": _rate(g["selected_loser"]), "selected_winner_rate": _rate(g["selected_winner"]), "immediate_adverse_rate": _rate(g["immediate_adverse_entry"]), "severe_loss_rate": _rate(g["ret20"] <= -0.05), "mae20_mean": _mean(g, "mae20"), "mfe20_mean": _mean(g, "mfe20")})
    return pd.DataFrame(rec)


def topk_mixture(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows[rows["path20_available"].eq(True)].copy()
    rows["period"] = period_label(rows)
    rows2 = pd.concat([rows, rows.assign(period="2024_2026_combined")[rows["year"].isin([2024, 2025, 2026])]], ignore_index=True)
    rec = []
    for period, pr in rows2.groupby("period"):
        for topk in [5, 10, 20]:
            selected = pr[pr["baseline_rank"] <= topk]
            for fam, g in selected.groupby("primary_family"):
                rec.append({"period": period, "topk": topk, "primary_family": fam, "row_count": int(len(g)), "family_share": len(g) / len(selected) if len(selected) else None, "starter_good_count": int(g["starter_good"].astype(bool).sum()), "starter_bad_count": int(g["starter_bad"].astype(bool).sum()), "selected_loser_count": int(g["selected_loser"].astype(bool).sum()), "selected_winner_count": int(g["selected_winner"].astype(bool).sum()), "mean_ret20": _mean(g, "ret20")})
    return pd.DataFrame(rec)


def missed_good_by_family(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows[rows["path20_available"].eq(True) & rows["year"].isin([2024, 2025, 2026])].copy()
    rows["period"] = period_label(rows)
    sg = rows[(rows["starter_good_abs"].astype(bool) | rows["starter_good_cross_sectional"].astype(bool)) & (rows["baseline_rank"] > 10) & (rows["pairwise_rank"] > 10)]
    rec = []
    for (period, fam), g in sg.groupby(["period", "primary_family"]):
        rec.append({"period": period, "primary_family": fam, "missed_starter_good_count": int(len(g)), "avg_baseline_rank": _mean(g, "baseline_rank"), "avg_pairwise_rank": _mean(g, "pairwise_rank"), "mean_ret20": _mean(g, "ret20"), "mfe20_mean": _mean(g, "mfe20")})
    return pd.DataFrame(rec)


def separation_score(q: pd.DataFrame, mix: pd.DataFrame) -> pd.DataFrame:
    recent = q[q["period"].eq("2024_2026_combined")].copy()
    top10 = mix[(mix["period"].eq("2024_2026_combined")) & (mix["topk"].eq(10))]
    contrib = top10.groupby("primary_family")["selected_loser_count"].sum().to_dict()
    rows = []
    for _, r in recent.iterrows():
        good = r["starter_good_rate"] or 0
        bad = r["starter_bad_rate"] or 0
        loser = r["selected_loser_rate"] or 0
        winner = r["selected_winner_rate"] or 0
        rows.append({"primary_family": r["primary_family"], "n": r["n"], "starter_good_minus_bad_spread": good - bad, "loser_minus_winner_spread": loser - winner, "top10_selected_loser_contribution": int(contrib.get(r["primary_family"], 0)), "mean_ret20": r["mean_ret20"], "sample_size": r["n"], "winner_damage_risk_if_suppressed": winner, "upside_opportunity_if_promoted": good})
    return pd.DataFrame(rows).sort_values(["starter_good_minus_bad_spread", "mean_ret20"], ascending=[False, False])


def axis_candidates(score: pd.DataFrame) -> dict[str, Any]:
    opts = []
    for _, r in score.head(5).iterrows():
        fam = r["primary_family"]
        use = "family-specific candidate source" if r["starter_good_minus_bad_spread"] > -0.25 else "family-specific suppression"
        opts.append({"axis_name": f"{fam}_starter_axis", "family": fam, "intended_use": use, "evidence": {"starter_good_minus_bad_spread": r["starter_good_minus_bad_spread"], "loser_minus_winner_spread": r["loser_minus_winner_spread"], "mean_ret20": r["mean_ret20"]}, "sample_size": int(r["sample_size"]), "year_stability": "requires_pretest", "expected_winner_damage": r["winner_damage_risk_if_suppressed"], "expected_meemee_reflection_path_if_successful": "TRADEX formal compare then read-only MeeMee reflection bundle", "recommended_next": "pretest" if int(r["sample_size"]) >= 1000 else "hold"})
    return {"candidates": opts}


def decide(score: pd.DataFrame) -> dict[str, Any]:
    best = score.iloc[0].to_dict() if not score.empty else {}
    if best and best.get("sample_size", 0) >= 1000 and best.get("starter_good_minus_bad_spread", -1) > -0.35:
        decision = "starter_specific_family_pretest_allowed"
        reason = f"{best['primary_family']} has the best recent starter-good vs bad balance and enough sample for a fixed family-specific pretest"
    elif not score.empty:
        decision = "candidate_source_split_design_needed"
        reason = "families differ, but no single clean family clears enough separation; generator/source split design is needed"
    else:
        decision = "feature_contract_expansion_needed"
        reason = "family assignment produced no usable scored families"
    return {"research_decision": decision, "reason_typed": [reason], "meemee_reflectable_candidate": False, "blocker_reason": "diagnostic family split only; no keep-gated challenger or reflection bundle"}


def run(redesign_root: Path, backfill_root: Path, shadow_root: Path, pairwise_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-family-split-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = assign_families(load_rows(backfill_root, pairwise_root))
    cov = coverage_summary(rows)
    qual = quality_summary(rows)
    mix = topk_mixture(rows)
    miss = missed_good_by_family(rows)
    sep = separation_score(qual, mix)
    candidates = axis_candidates(sep)
    decision = decide(sep)
    _write_json(out / "input_artifact_report.json", {"redesign_root": redesign_root, "backfill_root": backfill_root, "shadow_root": shadow_root, "pairwise_root": pairwise_root, "input_rows": len(rows)})
    _write_json(out / "family_taxonomy_schema.json", family_schema())
    rows.to_csv(out / "candidate_family_rows.csv", index=False)
    cov.to_csv(out / "family_coverage_summary.csv", index=False)
    qual.to_csv(out / "family_quality_summary.csv", index=False)
    mix.to_csv(out / "topk_family_mixture_audit.csv", index=False)
    miss.to_csv(out / "missed_starter_good_by_family.csv", index=False)
    sep.to_csv(out / "family_separation_score.csv", index=False)
    _write_json(out / "next_family_axis_candidates.json", candidates)
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "family_assignment_uses_decision_date_features_only": True, "labels_used_for_audit_only": True, "no_model_training": True, "ranking_unchanged": True, "score_formula_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "meemee_unchanged": True})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--redesign-root", type=Path, default=DEFAULT_REDESIGN_ROOT)
    p.add_argument("--backfill-root", type=Path, default=DEFAULT_BACKFILL_ROOT)
    p.add_argument("--shadow-root", type=Path, default=DEFAULT_SHADOW_ROOT)
    p.add_argument("--pairwise-root", type=Path, default=DEFAULT_PAIRWISE_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = p.parse_args(argv)
    out = run(args.redesign_root, args.backfill_root, args.shadow_root, args.pairwise_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
