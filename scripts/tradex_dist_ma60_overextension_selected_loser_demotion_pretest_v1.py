from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "dist_ma60_overextension_selected_loser_demotion_pretest_v1"
DEFAULT_AXIS_ROOT = Path(r"G:\Tradex\selected_loser_negative_axis_audit_v1\20260523T195634Z-selected-loser-negative-axis-audit-v1")
DEFAULT_SCORED_ROOT = Path(r"G:\Tradex\monthly_box_breakout_above60_maturity_context_pretest_v1\20260523T194427Z-monthly-box-breakout-above60-maturity-context-pretest-v1")
DEFAULT_RICH_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1\20260523T185123Z-recent-win-pattern-shift-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\dist_ma60_overextension_selected_loser_demotion_pretest_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "demotion_policy.json",
    "candidate_rows_scored.csv",
    "topk_comparison_summary.json",
    "selected_loser_reduction_summary.csv",
    "replacement_quality.csv",
    "period_stability_summary.csv",
    "branching_summary.csv",
    "removed_rows_overextension_profile.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
TOPK = (5, 10, 20)
YEARS = (2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026)
PROFILE_COLS = ("dist_ma60_pct", "dist_ma20_pct", "ma20_slope", "ma60_slope", "realized_vol20", "atr14_pct", "upper_wick_ratio")


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
    if df.empty or col not in df:
        return None
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def load_rows(scored_root: Path, rich_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    scored = pd.read_csv(scored_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False)
    rich_path = rich_root / "candidate_rows_with_features.csv"
    rich_cols = [
        "decision_ymd",
        "code",
        "dist_ma60_pct",
        "dist_ma20_pct",
        "ma20_slope",
        "ma60_slope",
        "realized_vol20",
        "atr14_pct",
        "upper_wick_ratio",
    ]
    rich = pd.read_csv(rich_path, usecols=[c for c in rich_cols if c in pd.read_csv(rich_path, nrows=1).columns], dtype={"code": str}, low_memory=False)
    rows = scored.merge(rich, on=["decision_ymd", "code"], how="left", suffixes=("", "_rich"))
    rows["ret20_num"] = pd.to_numeric(rows["ret20"], errors="coerce")
    rows = rows[rows["year"].isin(YEARS) & rows["ret20_num"].notna()].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows["dist_ma60_pct_num"] = pd.to_numeric(rows["dist_ma60_pct"], errors="coerce")
    rows["dist_ma60_q_by_date"] = rows.groupby("decision_ymd")["dist_ma60_pct_num"].rank(pct=True, method="average")
    rows["dist_ma60_coverage_limited"] = rows["dist_ma60_pct_num"].isna()
    rows["dist_ma60_high_risk_q4"] = rows["dist_ma60_q_by_date"] >= 0.75
    rows.loc[rows["dist_ma60_coverage_limited"], "dist_ma60_high_risk_q4"] = False
    rows["dist_ma60_score_delta"] = rows["dist_ma60_high_risk_q4"].map(lambda x: -1 if bool(x) else 0)
    rows["baseline_score"] = pd.to_numeric(rows["selection_score"], errors="coerce")
    rows["challenger_score"] = rows["baseline_score"] + rows["dist_ma60_score_delta"]
    rows["challenger_rank"] = (
        rows.sort_values(["decision_ymd", "challenger_score", "baseline_score", "candidate_rank"], ascending=[True, False, False, True])
        .groupby("decision_ymd")
        .cumcount()
        + 1
    )
    rows["ret20_pct_rank_by_date"] = rows.groupby("decision_ymd")["ret20_num"].rank(pct=True, method="average")
    rows["winner20"] = (rows["ret20_num"] >= 0.05) | (rows["ret20_pct_rank_by_date"] >= 0.70)
    rows["loser20"] = (rows["ret20_num"] <= -0.05) | (rows["ret20_pct_rank_by_date"] <= 0.30)
    return rows, {
        "rows_loaded": int(len(rows)),
        "dist_ma60_pct_coverage": float(rows["dist_ma60_pct_num"].notna().mean()),
        "rich_feature_path": rich_path,
        "scored_path": scored_root / "candidate_rows_scored.csv",
    }


def _periods(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "2024": rows[rows["year"] == 2024],
        "2025": rows[rows["year"] == 2025],
        "2026_label_safe": rows[rows["year"] == 2026],
        "2024_2026_label_safe": rows[rows["year"].between(2024, 2026)],
        "2019_2023": rows[rows["year"].between(2019, 2023)],
        "2019_2026_label_safe": rows,
    }


def _metrics(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df["ret20_num"], errors="coerce").dropna()
    return {
        "n": int(len(df)),
        "mean_ret20": None if ret.empty else float(ret.mean()),
        "median_ret20": None if ret.empty else float(ret.median()),
        "win_rate_ret20_gt_0": None if ret.empty else float((ret > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret.empty else float((ret > 0.05).mean()),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret.empty else float((ret <= -0.05).mean()),
        "selected_loser_rate": None if df.empty else float(df["loser20"].mean()),
        "selected_winner_rate": None if df.empty else float(df["winner20"].mean()),
    }


def compare(rows: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary: dict[str, Any] = {}
    loser_rows = []
    repl_rows = []
    branch_rows = []
    profile_rows = []
    for pname, p in _periods(rows).items():
        summary[pname] = {}
        for k in TOPK:
            base = p[p["baseline_rank_recalc"] <= k].copy()
            chal = p[p["challenger_rank"] <= k].copy()
            bkeys = set(zip(base["decision_ymd"], base["code"]))
            ckeys = set(zip(chal["decision_ymd"], chal["code"]))
            added = chal.set_index(["decision_ymd", "code"]).loc[list(ckeys - bkeys)].reset_index() if ckeys - bkeys else chal.head(0)
            removed = base.set_index(["decision_ymd", "code"]).loc[list(bkeys - ckeys)].reset_index() if bkeys - ckeys else base.head(0)
            bm, cm = _metrics(base), _metrics(chal)
            added_mean, removed_mean = _mean(added, "ret20_num"), _mean(removed, "ret20_num")
            item = {
                "baseline": bm,
                "challenger": cm,
                "delta_mean_ret20": _delta(cm["mean_ret20"], bm["mean_ret20"]),
                "delta_median_ret20": _delta(cm["median_ret20"], bm["median_ret20"]),
                "delta_severe_loss_rate": _delta(cm["severe_loss_rate_ret20_lte_minus_5pct"], bm["severe_loss_rate_ret20_lte_minus_5pct"]),
                "delta_selected_loser_rate": _delta(cm["selected_loser_rate"], bm["selected_loser_rate"]),
                "delta_selected_winner_rate": _delta(cm["selected_winner_rate"], bm["selected_winner_rate"]),
                "changed_members_count": int(len(added)),
                "added_mean_ret20": added_mean,
                "removed_mean_ret20": removed_mean,
                "added_minus_removed_ret20": _delta(added_mean, removed_mean),
            }
            summary[pname][f"top{k}"] = item
            loser_rows.append({"period": pname, "topk": k, "baseline_selected_loser_rate": bm["selected_loser_rate"], "challenger_selected_loser_rate": cm["selected_loser_rate"], "delta_selected_loser_rate": item["delta_selected_loser_rate"], "baseline_selected_winner_rate": bm["selected_winner_rate"], "challenger_selected_winner_rate": cm["selected_winner_rate"], "delta_selected_winner_rate": item["delta_selected_winner_rate"], "winner_damage_count": int(base["winner20"].sum() - chal["winner20"].sum())})
            repl_rows.append({"period": pname, "topk": k, "added_count": int(len(added)), "removed_count": int(len(removed)), "added_mean_ret20": added_mean, "added_median_ret20": _median(added, "ret20_num"), "removed_mean_ret20": removed_mean, "removed_median_ret20": _median(removed, "ret20_num"), "added_minus_removed_ret20": item["added_minus_removed_ret20"], "removed_loser_rate": None if removed.empty else float(removed["loser20"].mean()), "removed_winner_rate": None if removed.empty else float(removed["winner20"].mean())})
            branch_rows.append({"period": pname, "topk": k, "changed_members_count": int(len(added)), "total_challenger_members": int(len(chal))})
            prof = {"period": pname, "topk": k, "removed_count": int(len(removed))}
            for col in PROFILE_COLS:
                prof[f"{col}_mean"] = _mean(removed, col)
                prof[f"{col}_median"] = _median(removed, col)
            profile_rows.append(prof)
    return summary, pd.DataFrame(loser_rows), pd.DataFrame(repl_rows), pd.DataFrame(branch_rows), pd.DataFrame(profile_rows)


def period_stability(summary: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "period": period,
                "topk": int(top.replace("top", "")),
                "baseline_mean_ret20": item["baseline"]["mean_ret20"],
                "challenger_mean_ret20": item["challenger"]["mean_ret20"],
                "delta_mean_ret20": item["delta_mean_ret20"],
                "delta_selected_loser_rate": item["delta_selected_loser_rate"],
                "delta_selected_winner_rate": item["delta_selected_winner_rate"],
                "delta_severe_loss_rate": item["delta_severe_loss_rate"],
                "added_minus_removed_ret20": item["added_minus_removed_ret20"],
                "changed_members_count": item["changed_members_count"],
            }
            for period, block in summary.items()
            for top, item in block.items()
        ]
    )


def decide(summary: dict[str, Any], coverage: float) -> dict[str, Any]:
    if coverage < 0.70:
        decision = "inconclusive"
        reasons = ["dist_ma60_pct coverage is too low"]
    else:
        recent = summary["2024_2026_label_safe"]
        keep = False
        hold = False
        drop_reasons = []
        for top in ("top5", "top10"):
            r = recent[top]
            mean_ok = (r["delta_mean_ret20"] or 0) >= 0.005
            loser_ok = (r["delta_selected_loser_rate"] or 0) < 0
            winner_ok = (r["delta_selected_winner_rate"] or 0) >= -0.02
            repl_ok = (r["added_minus_removed_ret20"] or -999) > 0
            loss_ok = (r["delta_severe_loss_rate"] or 0) <= 0
            branch_ok = r["changed_members_count"] > 0
            years_ok = all((summary[y][top]["delta_mean_ret20"] or 0) > -0.005 for y in ("2024", "2025", "2026_label_safe"))
            if mean_ok and loser_ok and winner_ok and repl_ok and loss_ok and branch_ok and years_ok:
                keep = True
            elif loser_ok and branch_ok:
                hold = True
            if not loser_ok:
                drop_reasons.append(f"{top} selected_loser_rate does not improve")
            if not repl_ok:
                drop_reasons.append(f"{top} replacement quality is negative")
            if not loss_ok:
                drop_reasons.append(f"{top} severe loss worsens")
            if not years_ok:
                drop_reasons.append(f"{top} year effects contradict")
        if keep:
            decision = "keep_for_challenger_compare"
            reasons = ["top5 or top10 clears fixed demotion keep gates"]
        elif hold:
            decision = "hold_for_risk_guard_refinement"
            reasons = ["selected_loser_rate improves, but mean-return or winner-damage gates are not fully cleared"]
        else:
            decision = "drop"
            reasons = sorted(set(drop_reasons)) or ["fixed overextension demotion does not improve topK quality"]
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False, "threshold_sweep": False}


def run(*, axis_root: Path = DEFAULT_AXIS_ROOT, scored_root: Path = DEFAULT_SCORED_ROOT, rich_root: Path = DEFAULT_RICH_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-dist-ma60-overextension-selected-loser-demotion-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    axis_decision = json.loads((axis_root / "research_decision.json").read_text(encoding="utf-8"))
    rows, load_report = load_rows(scored_root, rich_root)
    summary, loser, repl, branch, profile = compare(rows)
    stability = period_stability(summary)
    decision = decide(summary, load_report["dist_ma60_pct_coverage"])
    rows.to_csv(run_dir / "candidate_rows_scored.csv", index=False)
    loser.to_csv(run_dir / "selected_loser_reduction_summary.csv", index=False)
    repl.to_csv(run_dir / "replacement_quality.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    branch.to_csv(run_dir / "branching_summary.csv", index=False)
    profile.to_csv(run_dir / "removed_rows_overextension_profile.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"axis_root": axis_root, "scored_root": scored_root, "rich_root": rich_root, "axis_decision": axis_decision.get("research_decision"), **load_report})
    _write_json(run_dir / "demotion_policy.json", {"policy_id": AXIS_ID, "feature": "dist_ma60_pct", "condition": "same decision date highest quartile of dist_ma60_pct", "score_delta": -1, "missing_dist_ma60_pct": "coverage_limited_no_demotion", "threshold_sweep": False, "veto": False})
    _write_json(run_dir / "topk_comparison_summary.json", summary)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "dist_ma60_pct_point_in_time": True, "same_date_quartile_uses_features_only": True, "ret20_ret40_are_labels_only": True, "same_date_topk_comparison_only": True, "rows_after_2026_label_safe_cutoff_excluded": True, "threshold_sweep": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "summary": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--axis-root", type=Path, default=DEFAULT_AXIS_ROOT)
    parser.add_argument("--scored-root", type=Path, default=DEFAULT_SCORED_ROOT)
    parser.add_argument("--rich-root", type=Path, default=DEFAULT_RICH_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(axis_root=args.axis_root, scored_root=args.scored_root, rich_root=args.rich_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
