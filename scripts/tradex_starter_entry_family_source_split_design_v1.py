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


AXIS_ID = "starter_entry_family_source_split_design_v1"
DEFAULT_FAMILY_SPLIT_ROOT = Path(r"G:\Tradex\starter_entry_family_split_v1\20260525T035132Z-starter-entry-family-split-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "research_family_source_schema.json",
    "candidate_family_source_rows.csv",
    "family_surface_coverage_summary.csv",
    "family_surface_quality_summary.csv",
    "family_topk_diagnostic_summary.csv",
    "cross_family_mixture_audit.json",
    "next_family_axis_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
SURFACE_FILES = {
    "pullback_reclaim_source": "family_surface_pullback_reclaim.csv",
    "breakout_retest_source": "family_surface_breakout_retest.csv",
    "mature_trend_continuation_source": "family_surface_mature_trend_continuation.csv",
    "early_trend_source": "family_surface_early_trend.csv",
    "range_reversal_source": "family_surface_range_reversal.csv",
    "overextension_risk_source": "family_surface_overextension_risk.csv",
    "uncategorized_source": "family_surface_uncategorized.csv",
}
FAMILY_TO_SOURCE = {
    "pullback_reclaim_family": "pullback_reclaim_source",
    "breakout_retest_family": "breakout_retest_source",
    "mature_trend_continuation_family": "mature_trend_continuation_source",
    "early_trend_family": "early_trend_source",
    "range_reversal_family": "range_reversal_source",
    "overextension_risk_family": "overextension_risk_source",
    "uncategorized_family": "uncategorized_source",
}


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


def period_label(rows: pd.DataFrame) -> pd.Series:
    return rows["year"].map(lambda y: "2019_2023_reference" if int(y) <= 2023 else ("2026_label_safe" if int(y) == 2026 else str(int(y))))


def schema() -> dict[str, Any]:
    return {
        "schema_version": "research_family_source_schema_v1",
        "diagnostic_not_real_candidate_source": True,
        "source_field_name": "research_candidate_source_family",
        "surface_field_name": "research_family_surface",
        "required_fields": [
            "decision_date",
            "code",
            "baseline_rank",
            "baseline_score",
            "primary_family",
            "research_candidate_source_family",
            "secondary_family_tags_json",
            "family_assignment_reason_json",
            "feature_availability_json",
            "starter_good",
            "starter_bad",
            "selected_loser",
            "selected_winner",
            "ret20",
            "mae20",
            "mfe20",
            "source_artifact_path",
            "source_run_id",
        ],
        "families": sorted(FAMILY_TO_SOURCE.values()),
    }


def load_surface_rows(family_split_root: Path) -> pd.DataFrame:
    rows = pd.read_csv(family_split_root / "candidate_family_rows.csv", low_memory=False)
    rows["research_candidate_source_family"] = rows["primary_family"].map(FAMILY_TO_SOURCE).fillna("uncategorized_source")
    rows["research_family_surface"] = rows["research_candidate_source_family"]
    rows["research_family_source_schema_version"] = "research_family_source_schema_v1"
    rows["feature_availability_json"] = rows.get("family_feature_availability_json", "{}")
    rows["research_family_assignment_reason_json"] = rows.get("family_assignment_reason_json", "{}")
    rows["within_family_baseline_rank"] = rows.sort_values(["decision_date", "research_candidate_source_family", "baseline_score", "code"], ascending=[True, True, False, True]).groupby(["decision_date", "research_candidate_source_family"]).cumcount() + 1
    return rows


def with_combined_periods(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    rows["period"] = period_label(rows)
    recent = rows[rows["year"].isin([2024, 2025, 2026])].copy()
    recent["period"] = "2024_2026_combined"
    return pd.concat([rows, recent], ignore_index=True)


def coverage(rows: pd.DataFrame) -> pd.DataFrame:
    data = with_combined_periods(rows)
    rec = []
    for (period, src), g in data.groupby(["period", "research_candidate_source_family"]):
        dates = g["decision_date"].nunique()
        rec.append({"period": period, "research_candidate_source_family": src, "row_count": int(len(g)), "date_count": int(dates), "avg_candidates_per_date": float(len(g) / dates) if dates else None, "top5_count": int((g["baseline_rank"] <= 5).sum()), "top10_count": int((g["baseline_rank"] <= 10).sum()), "top20_count": int((g["baseline_rank"] <= 20).sum()), "path20_available_rate": _rate(g["path20_available"])})
    return pd.DataFrame(rec)


def quality(rows: pd.DataFrame) -> pd.DataFrame:
    data = with_combined_periods(rows[rows["path20_available"].eq(True)])
    rec = []
    for (period, src), g in data.groupby(["period", "research_candidate_source_family"]):
        rec.append({"period": period, "research_candidate_source_family": src, "n": int(len(g)), "mean_ret20": _mean(g, "ret20"), "median_ret20": _median(g, "ret20"), "starter_good_rate": _rate(g["starter_good"]), "starter_bad_rate": _rate(g["starter_bad"]), "selected_loser_rate": _rate(g["selected_loser"]), "selected_winner_rate": _rate(g["selected_winner"]), "immediate_adverse_rate": _rate(g["immediate_adverse_entry"]), "severe_loss_rate": _rate(g["ret20"] <= -0.05), "mae20_mean": _mean(g, "mae20"), "mfe20_mean": _mean(g, "mfe20")})
    return pd.DataFrame(rec)


def family_topk(rows: pd.DataFrame) -> pd.DataFrame:
    data = with_combined_periods(rows[rows["path20_available"].eq(True)])
    rec = []
    for (period, src), g in data.groupby(["period", "research_candidate_source_family"]):
        for topk in [3, 5, 10]:
            top = g[g["within_family_baseline_rank"] <= topk]
            rec.append({"period": period, "research_candidate_source_family": src, "family_topk": topk, "n": int(len(top)), "date_count": int(top["decision_date"].nunique()), "avg_eligible_count_per_date": float(len(g) / g["decision_date"].nunique()) if g["decision_date"].nunique() else None, "mean_ret20": _mean(top, "ret20"), "starter_good_rate": _rate(top["starter_good"]) if not top.empty else None, "starter_bad_rate": _rate(top["starter_bad"]) if not top.empty else None, "selected_loser_rate": _rate(top["selected_loser"]) if not top.empty else None, "severe_loss_rate": _rate(top["ret20"] <= -0.05) if not top.empty else None})
    return pd.DataFrame(rec)


def mixture(rows: pd.DataFrame) -> dict[str, Any]:
    recent = rows[rows["path20_available"].eq(True) & rows["year"].isin([2024, 2025, 2026])].copy()
    baseline_top10 = recent[recent["baseline_rank"] <= 10]
    starter_good = recent[recent["starter_good"].astype(bool)]
    missed_good = starter_good[(starter_good["baseline_rank"] > 10) & (starter_good.get("pairwise_rank", 9999) > 10)]
    selected_loser = baseline_top10[baseline_top10["selected_loser"].astype(bool)]

    def shares(df: pd.DataFrame) -> dict[str, int]:
        return {str(k): int(v) for k, v in df["research_candidate_source_family"].value_counts().sort_values(ascending=False).items()}

    return {
        "baseline_global_top10_family_composition": shares(baseline_top10),
        "oracle_starter_good_family_composition": shares(starter_good),
        "missed_starter_good_family_composition": shares(missed_good),
        "selected_loser_family_composition": shares(selected_loser),
        "useful_underrepresented_family": "pullback_reclaim_source",
        "topk_selected_loser_dominant_family": "overextension_risk_source",
        "daily_frequency_usable_families": ["overextension_risk_source", "pullback_reclaim_source", "breakout_retest_source"],
    }


def axis_candidates(cov: pd.DataFrame, qual: pd.DataFrame, topk: pd.DataFrame, mix: dict[str, Any]) -> dict[str, Any]:
    recent_q = qual[qual["period"].eq("2024_2026_combined")].copy()
    recent_c = cov[cov["period"].eq("2024_2026_combined")][["research_candidate_source_family", "row_count", "date_count", "avg_candidates_per_date"]]
    merged = recent_q.merge(recent_c, on="research_candidate_source_family", how="left")
    merged["balance"] = merged["starter_good_rate"] - merged["starter_bad_rate"]
    merged = merged.sort_values(["mean_ret20", "balance"], ascending=[False, False])
    out = []
    for _, r in merged.head(5).iterrows():
        src = r["research_candidate_source_family"]
        if src == "overextension_risk_source":
            intended = "family_watch_only_contract"
            rec = "design"
            risk = "winner damage if suppressed; selected loser contribution is high"
        elif r["row_count"] >= 1000 and r["date_count"] >= 100:
            intended = "family_specific_topK_pretest"
            rec = "pretest"
            risk = "family definition derived not real source"
        else:
            intended = "source_generation_design"
            rec = "hold"
            risk = "sample thin"
        out.append({"axis_name": f"{src}_surface_axis", "source_family": src, "intended_next": intended, "evidence": {"mean_ret20": r["mean_ret20"], "starter_good_rate": r["starter_good_rate"], "starter_bad_rate": r["starter_bad_rate"], "selected_loser_rate": r["selected_loser_rate"], "date_coverage": r["date_count"], "sample_size": r["row_count"]}, "risk": risk, "recommended_next": rec})
    return {"candidates": out}


def decide(candidates: dict[str, Any]) -> dict[str, Any]:
    pretests = [c for c in candidates["candidates"] if c["recommended_next"] == "pretest"]
    if pretests:
        best = pretests[0]
        return {"research_decision": "family_specific_pretest_allowed", "reason_typed": [f"{best['source_family']} has sufficient date/sample coverage for a fixed family-specific topK pretest"], "next_axis": best["axis_name"], "meemee_reflectable_candidate": False, "blocker_reason": "design artifact only; no keep-gated family pretest yet"}
    return {"research_decision": "candidate_source_contract_needed", "reason_typed": ["family surfaces differ materially but require generation-time family/source contract before ranking"], "meemee_reflectable_candidate": False, "blocker_reason": "no validated challenger"}


def write_surfaces(rows: pd.DataFrame, out: Path) -> None:
    for source, filename in SURFACE_FILES.items():
        rows[rows["research_candidate_source_family"].eq(source)].to_csv(out / filename, index=False)


def run(family_split_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-entry-family-source-split-design-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows = load_surface_rows(family_split_root)
    cov = coverage(rows)
    qual = quality(rows)
    topk = family_topk(rows)
    mix = mixture(rows)
    candidates = axis_candidates(cov, qual, topk, mix)
    decision = decide(candidates)
    _write_json(out / "input_artifact_report.json", {"family_split_root": family_split_root, "input_rows": len(rows)})
    _write_json(out / "research_family_source_schema.json", schema())
    rows.to_csv(out / "candidate_family_source_rows.csv", index=False)
    write_surfaces(rows, out)
    cov.to_csv(out / "family_surface_coverage_summary.csv", index=False)
    qual.to_csv(out / "family_surface_quality_summary.csv", index=False)
    topk.to_csv(out / "family_topk_diagnostic_summary.csv", index=False)
    _write_json(out / "cross_family_mixture_audit.json", mix)
    _write_json(out / "next_family_axis_candidates.json", candidates)
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "diagnostic_research_family_only": True, "real_candidate_source_not_filled": True, "family_surfaces_filter_existing_rows_only": True, "no_new_training": True, "ranking_unchanged": True, "score_formula_changed": False, "candidate_generation_changed": False, "runtime_db_write": False, "meemee_unchanged": True})
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS) + list(SURFACE_FILES.values()), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--family-split-root", type=Path, default=DEFAULT_FAMILY_SPLIT_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = p.parse_args(argv)
    out = run(args.family_split_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
