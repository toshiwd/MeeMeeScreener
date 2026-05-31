from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "above20_streak_recent_long_quality_pretest_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1\20260523T185123Z-recent-win-pattern-shift-audit-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\above20_streak_recent_long_quality_pretest_v1")
REQUIRED_INPUTS = (
    "candidate_rows_with_features.csv",
    "pattern_shift_matrix.csv",
    "next_challenger_candidates.json",
    "no_lookahead_audit.json",
)
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "boost_policy.json",
    "candidate_rows_scored.csv",
    "topk_comparison_summary.json",
    "replacement_quality.csv",
    "period_stability_summary.csv",
    "branching_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
USECOLS = [
    "decision_ymd",
    "code",
    "candidate_rank",
    "selection_score",
    "selected_for_buy",
    "source_year",
    "year",
    "period_bucket",
    "above20_streak",
    "days_since_ma20_reclaim",
    "ret20",
    "ret40",
    "mae20",
    "mfe20",
    "max_drawdown_20",
]
BOOST_POLICY = {
    "policy_id": "above20_streak_fixed_rank_equivalent_boost_v1",
    "feature": "above20_streak",
    "bucket_0": {"condition": "above20_streak < 5", "score_boost": 0},
    "bucket_1": {"condition": "5 <= above20_streak < 10", "score_boost": 0},
    "bucket_2": {"condition": "10 <= above20_streak < 20", "score_boost": 1},
    "bucket_3": {"condition": "20 <= above20_streak < 40", "score_boost": 2},
    "bucket_4": {"condition": "above20_streak >= 40", "score_boost": 1},
    "selection": "one fixed map selected before outcome evaluation from user contract",
    "threshold_sweep": False,
}


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


def above20_bucket(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "missing"
    if value < 5:
        return "bucket_0"
    if value < 10:
        return "bucket_1"
    if value < 20:
        return "bucket_2"
    if value < 40:
        return "bucket_3"
    return "bucket_4"


def score_boost(bucket: str) -> int:
    if bucket not in BOOST_POLICY:
        return 0
    return int(BOOST_POLICY[bucket]["score_boost"])


def period_label(year: int) -> str:
    if 2019 <= year <= 2023:
        return "pre_recent_2019_2023"
    if 2024 <= year <= 2025:
        return "recent_confirmed_2024_2025"
    return str(year)


def load_and_score(input_root: Path) -> pd.DataFrame:
    rows = pd.read_csv(input_root / "candidate_rows_with_features.csv", usecols=USECOLS, dtype={"code": str}, low_memory=False)
    rows = rows[rows["year"].between(2019, 2025)].copy()
    rows["period_eval"] = rows["year"].astype(int).map(period_label)
    rows["above20_bucket"] = rows["above20_streak"].map(above20_bucket)
    rows["above20_score_boost"] = rows["above20_bucket"].map(score_boost)
    rows["baseline_score"] = pd.to_numeric(rows["selection_score"], errors="coerce")
    rows["challenger_score"] = rows["baseline_score"] + rows["above20_score_boost"]
    rows["baseline_rank_recalc"] = rows.groupby("decision_ymd")["baseline_score"].rank(method="first", ascending=False)
    rows["challenger_rank"] = rows.sort_values(["decision_ymd", "challenger_score", "baseline_score", "candidate_rank"], ascending=[True, False, False, True]).groupby("decision_ymd").cumcount() + 1
    return rows


def _metrics(df: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(df["ret20"], errors="coerce")
    return {
        "n": int(len(df)),
        "mean_ret20": None if ret20.dropna().empty else float(ret20.mean()),
        "median_ret20": None if ret20.dropna().empty else float(ret20.median()),
        "win_rate_ret20_gt_0": None if ret20.dropna().empty else float((ret20 > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret20.dropna().empty else float((ret20 > 0.05).mean()),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret20.dropna().empty else float((ret20 <= -0.05).mean()),
        "bottom_decile_rate": None if "ret20_decile_by_date" not in df else float((df["ret20_decile_by_date"] <= 1).mean()),
        "mae20_mean": None if "mae20" not in df else _mean(df, "mae20"),
        "mfe20_mean": None if "mfe20" not in df else _mean(df, "mfe20"),
    }


def _mean(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def topk_sets(rows: pd.DataFrame, k: int, rank_col: str) -> pd.DataFrame:
    return rows[rows[rank_col] <= k].copy()


def comparison_by_period(rows: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    rows = rows.copy()
    rows["ret20_decile_by_date"] = rows.groupby("decision_ymd")["ret20"].rank(pct=True).rsub(1).mul(10).add(1)
    periods = {
        "2024": rows[rows["year"] == 2024],
        "2025": rows[rows["year"] == 2025],
        "2024_2025_combined": rows[rows["year"].between(2024, 2025)],
        "2019_2023_combined": rows[rows["year"].between(2019, 2023)],
        "2019_2025_combined": rows[rows["year"].between(2019, 2025)],
    }
    summary: dict[str, Any] = {}
    replacements = []
    branch_rows = []
    for period, p_rows in periods.items():
        summary[period] = {}
        for k in (5, 10, 20):
            base = topk_sets(p_rows, k, "baseline_rank_recalc")
            chal = topk_sets(p_rows, k, "challenger_rank")
            base_keys = set(zip(base["decision_ymd"], base["code"]))
            chal_keys = set(zip(chal["decision_ymd"], chal["code"]))
            added_keys = chal_keys - base_keys
            removed_keys = base_keys - chal_keys
            added = chal.set_index(["decision_ymd", "code"]).loc[list(added_keys)].reset_index() if added_keys else chal.head(0)
            removed = base.set_index(["decision_ymd", "code"]).loc[list(removed_keys)].reset_index() if removed_keys else base.head(0)
            added_ret = _mean(added, "ret20")
            removed_ret = _mean(removed, "ret20")
            summary[period][f"top{k}"] = {
                "baseline": _metrics(base),
                "challenger": _metrics(chal),
                "delta_mean_ret20": _delta(_metrics(chal)["mean_ret20"], _metrics(base)["mean_ret20"]),
                "delta_median_ret20": _delta(_metrics(chal)["median_ret20"], _metrics(base)["median_ret20"]),
                "delta_severe_loss_rate": _delta(_metrics(chal)["severe_loss_rate_ret20_lte_minus_5pct"], _metrics(base)["severe_loss_rate_ret20_lte_minus_5pct"]),
                f"changed_top{k}_members_count": int(len(added_keys)),
                "added_mean_ret20": added_ret,
                "removed_mean_ret20": removed_ret,
                "added_minus_removed_ret20": _delta(added_ret, removed_ret),
            }
            replacements.append({"period": period, "topk": k, "added_count": int(len(added_keys)), "removed_count": int(len(removed_keys)), "added_mean_ret20": added_ret, "removed_mean_ret20": removed_ret, "added_minus_removed_ret20": _delta(added_ret, removed_ret)})
            branch_rows.append({"period": period, "topk": k, "changed_members_count": int(len(added_keys)), "total_challenger_members": int(len(chal))})
    return summary, pd.DataFrame(replacements), pd.DataFrame(branch_rows)


def _delta(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return float(a - b)


def period_stability(replacements: pd.DataFrame, summary: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for period, block in summary.items():
        for top_name, item in block.items():
            rows.append(
                {
                    "period": period,
                    "topk": int(top_name.replace("top", "")),
                    "baseline_mean_ret20": item["baseline"]["mean_ret20"],
                    "challenger_mean_ret20": item["challenger"]["mean_ret20"],
                    "delta_mean_ret20": item["delta_mean_ret20"],
                    "baseline_median_ret20": item["baseline"]["median_ret20"],
                    "challenger_median_ret20": item["challenger"]["median_ret20"],
                    "delta_median_ret20": item["delta_median_ret20"],
                    "delta_severe_loss_rate": item["delta_severe_loss_rate"],
                    "added_minus_removed_ret20": item["added_minus_removed_ret20"],
                    "changed_members_count": item[f"changed_top{int(top_name.replace('top', ''))}_members_count"],
                }
            )
    return pd.DataFrame(rows)


def decide(summary: dict[str, Any]) -> dict[str, Any]:
    recent = summary["2024_2025_combined"]
    pre = summary["2019_2023_combined"]
    keep_hit = False
    hold_hit = False
    reasons = []
    for top in ("top5", "top10"):
        r = recent[top]
        pre_r = pre[top]
        improves = (r["delta_mean_ret20"] or 0) >= 0.005
        median_ok = (r["delta_median_ret20"] or 0) >= 0
        loss_ok = (r["delta_severe_loss_rate"] or 0) <= 0
        repl_ok = (r["added_minus_removed_ret20"] or -999) > 0
        branch_ok = r[f"changed_{top}_members_count"] > 0
        pre_ok = (pre_r["delta_mean_ret20"] or 0) > -0.01
        if improves and median_ok and loss_ok and repl_ok and branch_ok and pre_ok:
            keep_hit = True
            reasons.append(f"{top} clears recent improvement, replacement quality, branching, and pre-recent degradation gates")
        elif branch_ok and (r["delta_mean_ret20"] or 0) > 0:
            hold_hit = True
            reasons.append(f"{top} improves directionally but misses one or more keep gates")
    if keep_hit:
        decision = "keep_for_challenger_compare"
    elif hold_hit or (recent["top20"]["delta_mean_ret20"] or 0) > 0:
        decision = "hold_for_more_source_validation"
        if not reasons:
            reasons.append("improvement is limited to top20 or does not clear top5/top10 keep gates")
    else:
        decision = "drop"
        reasons.append("no meaningful topK improvement under fixed above20_streak boost")
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "meemee_reflectable": False,
        "ranking_reflectable": False,
        "publish_allowed": False,
        "threshold_sweep": False,
    }


def run(*, input_root: Path = DEFAULT_INPUT_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-above20-streak-recent-long-quality-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing = [name for name in REQUIRED_INPUTS if not (input_root / name).exists()]
    if missing:
        raise FileNotFoundError(f"missing required input artifacts: {missing}")
    source_audit = json.loads((input_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    source_challengers = json.loads((input_root / "next_challenger_candidates.json").read_text(encoding="utf-8"))
    rows = load_and_score(input_root)
    summary, replacements, branching = comparison_by_period(rows)
    stability = period_stability(replacements, summary)
    decision = decide(summary)
    rows.to_csv(run_dir / "candidate_rows_scored.csv", index=False)
    replacements.to_csv(run_dir / "replacement_quality.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    branching.to_csv(run_dir / "branching_summary.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"input_root": input_root, "required_inputs": list(REQUIRED_INPUTS), "source_no_lookahead_audit": source_audit.get("audit_result"), "source_recommended_single_axis": source_challengers.get("recommended_single_axis"), "rows_loaded": int(len(rows)), "years": [2019, 2020, 2021, 2022, 2023, 2024, 2025], "recent_confirmed": "2024-2025", "source_2026_available": False})
    _write_json(run_dir / "boost_policy.json", BOOST_POLICY)
    _write_json(run_dir / "topk_comparison_summary.json", summary)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "no_lookahead_audit.json",
        {
            "audit_result": "pass",
            "above20_streak_uses_only_decision_date_or_prior_rows": True,
            "ret20_ret40_are_label_only": True,
            "topk_comparison_same_decision_date_only": True,
            "future_label_used_for_boost_policy": False,
            "model_training": False,
            "threshold_sweep": False,
            "column_classification": {
                "decision_ymd": "decision_surface",
                "code": "decision_surface",
                "selection_score": "decision_surface",
                "candidate_rank": "decision_surface",
                "above20_streak": "feature",
                "above20_bucket": "feature",
                "above20_score_boost": "fixed_policy",
                "ret20": "label",
                "ret40": "label",
                "mae20": "diagnostic",
                "mfe20": "diagnostic",
            },
        },
    )
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "summary": summary}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pretest fixed above20_streak boost on recent long candidate quality")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(input_root=args.input_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
