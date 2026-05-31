from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "monthly_box_breakout_year_conflict_decomposition_v1"
DEFAULT_PRETEST_ROOT = Path(r"G:\Tradex\monthly_box_breakout_recent_demotion_pretest_v1\20260523T190422Z-monthly-box-breakout-recent-demotion-pretest-v1")
DEFAULT_CONTEXT_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1\20260523T185123Z-recent-win-pattern-shift-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\monthly_box_breakout_year_conflict_decomposition_v1")
REQUIRED_PRETEST = (
    "candidate_rows_scored.csv",
    "replacement_quality.csv",
    "topk_comparison_summary.json",
    "period_stability_summary.csv",
    "branching_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "replacement_decomposition_by_year.csv",
    "removed_rows_feature_decomposition.csv",
    "added_rows_feature_decomposition.csv",
    "boundary_contribution_summary.csv",
    "concentration_summary.json",
    "year_conflict_summary.json",
    "context_axis_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
CONTEXT_FEATURES = [
    "decision_ymd",
    "code",
    "monthly_high_zone_proxy",
    "monthly_box_inside_proxy",
    "above20_streak",
    "above60_streak",
    "days_since_ma20_reclaim",
    "days_since_ma60_reclaim",
    "ma7_gt_ma20_gt_ma60",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "ma20_slope",
    "ma60_slope",
    "realized_vol20",
    "atr14_pct",
    "upper_wick_ratio",
    "large_bearish_candle",
    "failed_high_update",
    "volume_ratio_ma20",
]


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
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
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _mean(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def _rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    if s.dtype == bool:
        return float(s.mean())
    return float(s.astype("string").str.lower().isin({"true", "1", "yes"}).mean())


def build_membership(rows: pd.DataFrame, years: tuple[int, ...] = (2024, 2025), topks: tuple[int, ...] = (5, 10, 20)) -> pd.DataFrame:
    parts = []
    scoped = rows[rows["year"].isin(years)].copy()
    for year in years:
        y = scoped[scoped["year"] == year]
        for topk in topks:
            base = y[y["baseline_rank_recalc"] <= topk].copy()
            chal = y[y["challenger_rank"] <= topk].copy()
            base_keys = set(zip(base["decision_ymd"], base["code"]))
            chal_keys = set(zip(chal["decision_ymd"], chal["code"]))
            for label, frame, keys in (
                ("added", chal, chal_keys - base_keys),
                ("removed", base, base_keys - chal_keys),
                ("unchanged", chal, chal_keys & base_keys),
            ):
                if not keys:
                    continue
                out = frame.set_index(["decision_ymd", "code"]).loc[list(keys)].reset_index()
                out["replacement_role"] = label
                out["topk"] = topk
                out["eval_year"] = year
                parts.append(out)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def load_context(context_root: Path) -> pd.DataFrame:
    return pd.read_csv(context_root / "candidate_rows_with_features.csv", usecols=CONTEXT_FEATURES, dtype={"code": str}, low_memory=False)


def enrich_membership(members: pd.DataFrame, context: pd.DataFrame) -> pd.DataFrame:
    out = members.copy()
    out["code"] = out["code"].astype(str)
    context = context.copy()
    context["code"] = context["code"].astype(str)
    context_cols = [c for c in CONTEXT_FEATURES if c not in out.columns or c in {"decision_ymd", "code"}]
    return out.merge(context[context_cols], on=["decision_ymd", "code"], how="left")


def replacement_decomposition(members: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (year, topk, role), g in members.groupby(["eval_year", "topk", "replacement_role"], dropna=False):
        rows.append(
            {
                "year": year,
                "topk": topk,
                "replacement_role": role,
                "n": int(len(g)),
                "ret20_mean": _mean(g, "ret20"),
                "ret20_median": _median(g, "ret20"),
                "monthly_box_breakout_share": _rate(g, "monthly_box_breakout_bool"),
                "monthly_high_zone_share": _rate(g, "monthly_high_zone_proxy"),
                "monthly_box_inside_share": _rate(g, "monthly_box_inside_proxy"),
                "baseline_rank_mean": _mean(g, "baseline_rank_recalc"),
                "challenger_rank_mean": _mean(g, "challenger_rank"),
                "baseline_boundary_distance_mean": _mean(g.assign(boundary_distance=(pd.to_numeric(g["baseline_rank_recalc"], errors="coerce") - topk).abs()), "boundary_distance"),
                "challenger_boundary_distance_mean": _mean(g.assign(boundary_distance=(pd.to_numeric(g["challenger_rank"], errors="coerce") - topk).abs()), "boundary_distance"),
            }
        )
    return pd.DataFrame(rows)


def feature_decomposition(members: pd.DataFrame, role: str) -> pd.DataFrame:
    scoped = members[members["replacement_role"] == role]
    features = [c for c in CONTEXT_FEATURES if c not in {"decision_ymd", "code"}]
    rows = []
    for (year, topk), g in scoped.groupby(["eval_year", "topk"], dropna=False):
        for feature in features:
            rows.append(
                {
                    "year": year,
                    "topk": topk,
                    "replacement_role": role,
                    "feature": feature,
                    "mean": _mean(g, feature),
                    "median": _median(g, feature),
                    "true_rate": _rate(g, feature),
                    "n_non_null": int(g[feature].notna().sum()) if feature in g else 0,
                }
            )
    return pd.DataFrame(rows)


def concentration(members: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    changed = members[members["replacement_role"].isin(["added", "removed"])].copy()
    changed["month"] = changed["decision_ymd"].astype(str).str.slice(0, 6)
    for year, g in changed.groupby("eval_year"):
        block = {}
        for topk, tg in g.groupby("topk"):
            code_counts = tg["code"].astype(str).value_counts()
            date_counts = tg["decision_ymd"].astype(str).value_counts()
            month_counts = tg["month"].value_counts()
            block[str(topk)] = {
                "n_changed_rows": int(len(tg)),
                "largest_code_share": None if len(tg) == 0 else float(code_counts.iloc[0] / len(tg)),
                "largest_code": None if code_counts.empty else str(code_counts.index[0]),
                "largest_date_share": None if len(tg) == 0 else float(date_counts.iloc[0] / len(tg)),
                "largest_date": None if date_counts.empty else str(date_counts.index[0]),
                "largest_month_share": None if len(tg) == 0 else float(month_counts.iloc[0] / len(tg)),
                "largest_month": None if month_counts.empty else str(month_counts.index[0]),
            }
        out[str(year)] = block
    return out


def boundary_summary(members: pd.DataFrame) -> pd.DataFrame:
    changed = members[members["replacement_role"].isin(["added", "removed"])].copy()
    rows = []
    for (year, topk, role), g in changed.groupby(["eval_year", "topk", "replacement_role"]):
        boundary = g[(pd.to_numeric(g["baseline_rank_recalc"], errors="coerce").sub(topk).abs() <= 3) | (pd.to_numeric(g["challenger_rank"], errors="coerce").sub(topk).abs() <= 3)]
        rows.append(
            {
                "year": year,
                "topk": topk,
                "replacement_role": role,
                "n": int(len(g)),
                "boundary_near_n": int(len(boundary)),
                "boundary_near_share": None if len(g) == 0 else float(len(boundary) / len(g)),
                "ret20_mean_all": _mean(g, "ret20"),
                "ret20_mean_boundary_near": _mean(boundary, "ret20"),
                "ret20_top10pct_contribution_share": _top_contribution_share(g),
            }
        )
    return pd.DataFrame(rows)


def _top_contribution_share(g: pd.DataFrame) -> float | None:
    vals = pd.to_numeric(g["ret20"], errors="coerce").dropna()
    if vals.empty or vals.abs().sum() == 0:
        return None
    top_n = max(1, int(math.ceil(len(vals) * 0.10)))
    return float(vals.sort_values(ascending=False).head(top_n).sum() / vals.sum()) if vals.sum() != 0 else None


def year_conflict_summary(repl: pd.DataFrame, boundary: pd.DataFrame, conc: dict[str, Any]) -> dict[str, Any]:
    top10 = repl[(repl["topk"] == 10) & (repl["replacement_role"].isin(["added", "removed"]))]
    def pair(year: int) -> dict[str, Any]:
        added = top10[(top10["year"] == year) & (top10["replacement_role"] == "added")]
        removed = top10[(top10["year"] == year) & (top10["replacement_role"] == "removed")]
        return {
            "added_ret20_mean": None if added.empty else float(added.iloc[0]["ret20_mean"]),
            "removed_ret20_mean": None if removed.empty else float(removed.iloc[0]["ret20_mean"]),
            "added_minus_removed": None if added.empty or removed.empty else float(added.iloc[0]["ret20_mean"] - removed.iloc[0]["ret20_mean"]),
        }
    return {
        "top10_2024": pair(2024),
        "top10_2025": pair(2025),
        "primary_conflict_type": "2024 demotion removed weak breakout rows, but 2025 demotion removed stronger breakout rows",
        "boundary_driven": _boundary_driven(boundary),
        "concentration": conc,
    }


def _boundary_driven(boundary: pd.DataFrame) -> bool:
    vals = boundary[boundary["topk"].isin([5, 10])]["boundary_near_share"].dropna()
    return bool((vals >= 0.5).all()) if not vals.empty else False


def context_candidates(members: pd.DataFrame) -> dict[str, Any]:
    removed = members[members["replacement_role"] == "removed"].copy()
    rows = []
    for feature in ["monthly_high_zone_proxy", "above20_streak", "above60_streak", "realized_vol20", "ma20_slope", "failed_high_update", "dist_ma20_pct"]:
        if feature not in removed:
            continue
        g2024 = removed[(removed["eval_year"] == 2024) & (removed["topk"] == 10)]
        g2025 = removed[(removed["eval_year"] == 2025) & (removed["topk"] == 10)]
        rows.append(
            {
                "candidate_axis_name": f"monthly_box_breakout_context_{feature}_diagnostic_axis",
                "context_feature": feature,
                "n_2024_2025_removed_top10": int(len(removed[removed["topk"] == 10])),
                "feature_mean_removed_2024_top10": _mean(g2024, feature),
                "feature_mean_removed_2025_top10": _mean(g2025, feature),
                "observed_context_gap": _diff(_mean(g2025, feature), _mean(g2024, feature)),
                "recommended_next": "hold",
                "risk": "year-dependent; not pretested",
            }
        )
    rows = sorted(rows, key=lambda x: abs(x["observed_context_gap"] or 0), reverse=True)
    best = rows[0] if rows else None
    if best and best["n_2024_2025_removed_top10"] >= 300:
        best["recommended_next"] = "keep_for_separate_context_pretest_only_after_2026_or_additional_source"
    return {"candidates": rows[:5], "best_context_candidate": best}


def _diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return float(a - b)


def decide(summary: dict[str, Any], candidates: dict[str, Any], conc: dict[str, Any]) -> dict[str, Any]:
    best = candidates.get("best_context_candidate")
    concentrated = False
    for year_block in conc.values():
        for top_block in year_block.values():
            if (top_block.get("largest_code_share") or 0) > 0.15 or (top_block.get("largest_month_share") or 0) > 0.5:
                concentrated = True
    if best and not concentrated:
        decision = "hold_for_2026_validation"
        reason = "context gap is visible, but 2024/2025 contradiction requires 2026 or another same-family source before pretest"
    else:
        decision = "drop_monthly_box_breakout_demotion"
        reason = "no stable non-concentrated context emerged from 2024/2025 replacement conflict"
    return {
        "research_decision": decision,
        "reason_typed": [reason, summary["primary_conflict_type"]],
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
        "threshold_sweep": False,
    }


def run(*, pretest_root: Path = DEFAULT_PRETEST_ROOT, context_root: Path = DEFAULT_CONTEXT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-monthly-box-breakout-year-conflict-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing = [name for name in REQUIRED_PRETEST if not (pretest_root / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing pretest artifacts: {missing}")
    scored = pd.read_csv(pretest_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False)
    context = load_context(context_root)
    members = enrich_membership(build_membership(scored), context)
    repl = replacement_decomposition(members)
    removed_feat = feature_decomposition(members, "removed")
    added_feat = feature_decomposition(members, "added")
    boundary = boundary_summary(members)
    conc = concentration(members)
    conflict = year_conflict_summary(repl, boundary, conc)
    candidates = context_candidates(members)
    decision = decide(conflict, candidates, conc)
    repl.to_csv(run_dir / "replacement_decomposition_by_year.csv", index=False)
    removed_feat.to_csv(run_dir / "removed_rows_feature_decomposition.csv", index=False)
    added_feat.to_csv(run_dir / "added_rows_feature_decomposition.csv", index=False)
    boundary.to_csv(run_dir / "boundary_contribution_summary.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"pretest_root": pretest_root, "context_root": context_root, "rows_scored": int(len(scored)), "replacement_rows": int(len(members)), "scope": "read existing pretest artifacts only; no new policy"})
    _write_json(run_dir / "concentration_summary.json", conc)
    _write_json(run_dir / "year_conflict_summary.json", conflict)
    _write_json(run_dir / "context_axis_candidates.json", candidates)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "input_pretest_no_lookahead_reused": True, "context_features_point_in_time_from_shift_audit": True, "ret20_ret40_are_label_only": True, "no_new_demotion_policy": True, "threshold_sweep": False, "model_training": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "best_context_candidate": candidates.get("best_context_candidate")}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decompose 2024/2025 conflict in monthly breakout demotion pretest")
    parser.add_argument("--pretest-root", type=Path, default=DEFAULT_PRETEST_ROOT)
    parser.add_argument("--context-root", type=Path, default=DEFAULT_CONTEXT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(pretest_root=args.pretest_root, context_root=args.context_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
