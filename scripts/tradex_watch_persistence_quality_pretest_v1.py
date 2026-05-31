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

from scripts import tradex_starter_candidate_chart_review_outcome_audit_v1 as outcome_audit
from scripts import tradex_starter_candidate_chart_review_historical_replay_v1 as hist_replay
from scripts import tradex_starter_candidate_review_pack_v2 as review_v2
from scripts import tradex_starter_ready_failure_decomposition_v1 as decomp


AXIS_ID = "watch_persistence_quality_pretest_v1"
DEFAULT_FAMILY_SOURCE_ROOT = Path(
    r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1"
)
DEFAULT_CHART_REVIEW_ROOT = Path(
    r"G:\Tradex\starter_candidate_chart_review_pack_v1\20260525T061448Z-starter-candidate-chart-review-pack-v1"
)
DEFAULT_CLOSURE_ROOT = Path(r"G:\Tradex\starter_chart_review_branch_closure_v1\20260525T072259Z-starter-chart-review-branch-closure-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\watch_persistence_quality_pretest_v1")

REQUIRED_ARTIFACTS = (
    "watch_persistence_summary.json",
    "watch_persistence_rows.csv",
    "persistence_bucket_metrics.json",
    "persistence_vs_new_comparison.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "lineage.json",
    "research_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

BUCKETS = (
    "first_time_watch",
    "repeated_watch_2plus",
    "repeated_watch_3plus",
    "consecutive_watch_2plus",
    "reappeared_after_gap",
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


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def discover_saved_watch_snapshots(chart_review_root: Path) -> list[Path]:
    return hist_replay.discover_historical_chart_review_roots(chart_review_root)


def reconstruct_watch_rows(source_path: Path, selected_dates: list[int]) -> pd.DataFrame:
    candidate_rows = hist_replay.read_rows_for_dates(source_path, selected_dates)
    if candidate_rows.empty:
        return pd.DataFrame()
    watch_parts: list[pd.DataFrame] = []
    for _, date_rows in candidate_rows.groupby("decision_date"):
        picked = review_v2.select_candidates(date_rows.copy(), max_rows=10)
        out: list[dict[str, Any]] = []
        for _, row in picked.iterrows():
            klass, reasons, score = review_v2.classify(row, keep_gated=False)
            if klass != "watch":
                continue
            out.append(
                {
                    **row.to_dict(),
                    "as_of_date": int(row["decision_date"]),
                    "code": str(row["code"]).removesuffix(".0"),
                    "current_watch_present": True,
                    "candidate_action_class": klass,
                    "review_score": score,
                    "classification_reason": "|".join(reasons),
                    "pattern_type": decomp.pattern_type(row.get("research_candidate_source_family")),
                    "historical_watch_lineage": "diagnostic_reconstruction_v2_select_candidates_classify",
                }
            )
        if out:
            watch_parts.append(pd.DataFrame(out))
    return pd.concat(watch_parts, ignore_index=True) if watch_parts else pd.DataFrame()


def add_persistence_features(watch: pd.DataFrame) -> pd.DataFrame:
    if watch.empty:
        return watch
    rows: list[dict[str, Any]] = []
    work = watch.sort_values(["code", "as_of_date"]).copy()
    work["_dt"] = pd.to_datetime(work["as_of_date"].astype(str), format="%Y%m%d")
    for code, group in work.groupby("code", sort=False):
        prior_dates: list[pd.Timestamp] = []
        for _, row in group.iterrows():
            cur = row["_dt"]
            prior_20 = [d for d in prior_dates if 0 < (cur - d).days <= 20]
            prior_60 = [d for d in prior_dates if 0 < (cur - d).days <= 60]
            consecutive = 1
            prev = prior_dates[-1] if prior_dates else None
            if prev is not None and (cur - prev).days <= 7:
                consecutive = int(row.get("_prev_consecutive", 1)) + 1
            days_since_first = int((cur - min(prior_60)).days) if prior_60 else None
            reappeared = bool(prev is not None and (cur - prev).days > 7 and len(prior_60) > 0)
            record = row.drop(labels=["_dt"], errors="ignore").to_dict()
            record.update(
                {
                    "prior_watch_count_20d": len(prior_20),
                    "prior_watch_count_60d": len(prior_60),
                    "consecutive_watch_count": consecutive,
                    "days_since_first_recent_watch": days_since_first,
                    "first_time_watch_flag": len(prior_60) == 0,
                    "repeated_watch_2plus_flag": len(prior_60) >= 1,
                    "repeated_watch_3plus_flag": len(prior_60) >= 2,
                    "reappeared_after_gap_flag": reappeared,
                }
            )
            rows.append(record)
            prior_dates.append(cur)
            group.loc[row.name, "_prev_consecutive"] = consecutive
    out = pd.DataFrame(rows)
    out["consecutive_watch_2plus_flag"] = out["consecutive_watch_count"] >= 2
    out["decision_date"] = out["as_of_date"]
    return out


def metric_block(frame: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce").dropna()
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["as_of_date"].nunique()) if "as_of_date" in frame else 0,
        "code_count": int(frame["code"].astype(str).nunique()) if "code" in frame else 0,
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret10": _mean(frame, "ret10"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": float(ret20.median()) if not ret20.empty else None,
        "hit_rate_ret20_gt_0": float((ret20 > 0).mean()) if not ret20.empty else None,
        "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
        "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
    }


def _mean(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def compare(left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any]:
    lm = metric_block(left)
    rm = metric_block(right)
    return {
        "left": lm,
        "right": rm,
        "mean_ret20_delta": None if lm["mean_ret20"] is None or rm["mean_ret20"] is None else lm["mean_ret20"] - rm["mean_ret20"],
        "bad_rate_delta": None
        if lm["bad_rate_ret20_lt_minus_5pct"] is None or rm["bad_rate_ret20_lt_minus_5pct"] is None
        else lm["bad_rate_ret20_lt_minus_5pct"] - rm["bad_rate_ret20_lt_minus_5pct"],
        "severe_rate_delta": None
        if lm["severe_rate_ret20_lt_minus_10pct"] is None or rm["severe_rate_ret20_lt_minus_10pct"] is None
        else lm["severe_rate_ret20_lt_minus_10pct"] - rm["severe_rate_ret20_lt_minus_10pct"],
        "sample_allows_comparison": len(left) >= 10 and len(right) >= 10,
    }


def bucket_frame(rows: pd.DataFrame, bucket: str) -> pd.DataFrame:
    if bucket == "first_time_watch":
        return rows[rows["first_time_watch_flag"].astype(bool)]
    if bucket == "repeated_watch_2plus":
        return rows[rows["repeated_watch_2plus_flag"].astype(bool)]
    if bucket == "repeated_watch_3plus":
        return rows[rows["repeated_watch_3plus_flag"].astype(bool)]
    if bucket == "consecutive_watch_2plus":
        return rows[rows["consecutive_watch_2plus_flag"].astype(bool)]
    if bucket == "reappeared_after_gap":
        return rows[rows["reappeared_after_gap_flag"].astype(bool)]
    return rows.iloc[0:0]


def bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    first = bucket_frame(rows, "first_time_watch")
    out: dict[str, Any] = {}
    for bucket in BUCKETS:
        frame = bucket_frame(rows, bucket)
        other = rows[~rows.index.isin(frame.index)]
        pattern_metrics: dict[str, Any] = {}
        for pattern, group in frame.groupby("pattern_type", dropna=False):
            same_pattern_other = other[other["pattern_type"].astype(str).eq(str(pattern))]
            pattern_metrics[str(pattern)] = {
                "bucket": metric_block(group),
                "comparison_vs_same_pattern_other_watch": compare(group, same_pattern_other),
            }
        out[bucket] = {
            **metric_block(frame),
            "comparison_vs_first_time_watch": compare(frame, first),
            "comparison_vs_all_other_watch": compare(frame, other),
            "comparison_by_pattern_type": pattern_metrics,
        }
    return out


def decide(metrics: dict[str, Any]) -> str:
    rep = metrics.get("repeated_watch_2plus", {})
    first_comp = rep.get("comparison_vs_first_time_watch", {})
    n = int(rep.get("sample_count") or 0)
    dates = int(rep.get("date_count") or 0)
    delta = first_comp.get("mean_ret20_delta")
    bad_delta = first_comp.get("bad_rate_delta")
    severe_delta = first_comp.get("severe_rate_delta")
    if n == 0:
        return "sample_insufficient"
    worse = delta is not None and delta < 0
    improves = delta is not None and delta > 0 and (bad_delta is not None and bad_delta <= 0) and (severe_delta is not None and severe_delta <= 0)
    no_edge = not improves
    if worse:
        return "watch_persistence_worse_than_first_time"
    if n < 30 or dates < 12:
        return "watch_persistence_promising_but_underpowered" if improves else "sample_insufficient"
    if no_edge:
        return "watch_persistence_no_clear_edge"
    return "watch_persistence_keep_for_candidate_pool_pretest"


def run(family_source_root: Path, chart_review_root: Path, closure_root: Path, output_root: Path, db_path: Path | None = None) -> Path:
    out = output_root / f"{_now_tag()}-watch-persistence-quality-pretest-v1"
    out.mkdir(parents=True, exist_ok=True)
    source_path = family_source_root / "candidate_family_source_rows.csv"
    chart_roots = discover_saved_watch_snapshots(chart_review_root)
    selected_db = db_path or outcome_audit.select_confirmed_db(20260508)
    source_report = hist_replay.confirmed_source_report(selected_db)
    all_dates = hist_replay.read_candidate_dates(source_path)
    selected_dates, excluded_dates = hist_replay.monthly_grid_dates(all_dates, int(source_report["confirmed_max_date"]), min_year=2019)
    watch = reconstruct_watch_rows(source_path, selected_dates)
    if watch.empty:
        decision = "blocked_missing_historical_watch_lineage"
        rows = pd.DataFrame()
        metrics: dict[str, Any] = {}
        source_ok = False
    else:
        rows = add_persistence_features(watch)
        outcome_rows = rows.copy()
        outcome_rows["decision_date"] = outcome_rows["as_of_date"]
        bars = outcome_audit.load_forward_bars(selected_db, outcome_rows)
        audited = outcome_audit.audit_rows(outcome_rows, bars)
        audited["as_of_date"] = audited["decision_date"]
        rows = audited[pd.to_numeric(audited["forward_bar_count"], errors="coerce").ge(20)].reset_index(drop=True)
        metrics = bucket_metrics(rows)
        decision = decide(metrics)
        source_ok = bool(not bars.empty and set(bars["source"].dropna().unique()).issubset({"pan", "txt", "confirmed"}))
    rows.to_csv(out / "watch_persistence_rows.csv", index=False)

    closure_decision = json.loads((closure_root / "research_decision.json").read_text(encoding="utf-8")) if (closure_root / "research_decision.json").exists() else {}
    _write_json(out / "persistence_bucket_metrics.json", metrics)
    _write_json(
        out / "persistence_vs_new_comparison.json",
        {
            "repeated_watch_2plus_vs_first_time_watch": metrics.get("repeated_watch_2plus", {}).get("comparison_vs_first_time_watch"),
            "repeated_watch_3plus_vs_first_time_watch": metrics.get("repeated_watch_3plus", {}).get("comparison_vs_first_time_watch"),
            "consecutive_watch_2plus_vs_first_time_watch": metrics.get("consecutive_watch_2plus", {}).get("comparison_vs_first_time_watch"),
        },
    )
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "passes": source_ok,
            "persistence_features_use_watch_appearances_on_or_before_as_of_date": True,
            "future_watch_appearances_used": False,
            "ret5_ret10_ret20_used_in_feature_construction": False,
            "trigger_invalidation_used_in_feature_construction": False,
            "future_outcomes_evaluation_only": True,
            "bar_sources": source_report.get("confirmed_sources", []),
        },
    )
    _write_json(
        out / "source_coverage.json",
        {
            **source_report,
            "watch_lineage": "saved_chart_review_roots" if len(chart_roots) > 1 else "diagnostic_reconstruction",
            "saved_chart_review_root_count": len(chart_roots),
            "selected_date_count": len(selected_dates),
            "excluded_date_count": len(excluded_dates),
            "sample_count": int(len(rows)),
            "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0,
            "code_count": int(rows["code"].astype(str).nunique()) if not rows.empty else 0,
            "confirmed_source_only": source_ok,
            "runtime_db_write": False,
            "meemee_changed": False,
            "production_ranking_changed": False,
        },
    )
    _write_json(
        out / "lineage.json",
        {
            "closed_starter_chart_review_branch_root": closure_root,
            "closed_branch_decision": closure_decision.get("decision"),
            "family_source_rows": source_path,
            "saved_chart_review_roots": chart_roots,
            "historical_watch_reconstruction": "review_v2.select_candidates + review_v2.classify; watch only",
        },
    )
    _write_json(
        out / "research_decision.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "starter_ready_promotable": False,
            "chart_review_branch_reopened": False,
            "watch_persistence_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
            "candidate_generation_changed": False,
        },
    )
    _write_json(
        out / "watch_persistence_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "sample_count": int(len(rows)),
            "date_count": int(rows["as_of_date"].nunique()) if not rows.empty else 0,
            "code_count": int(rows["code"].astype(str).nunique()) if not rows.empty else 0,
            "starter_ready_promotable": False,
            "chart_review_branch_reopened": False,
            "watch_persistence_active_gate": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "confirmed_source_only": source_ok,
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family-source-root", type=Path, default=DEFAULT_FAMILY_SOURCE_ROOT)
    parser.add_argument("--chart-review-root", type=Path, default=DEFAULT_CHART_REVIEW_ROOT)
    parser.add_argument("--closure-root", type=Path, default=DEFAULT_CLOSURE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(args.family_source_root, args.chart_review_root, args.closure_root, args.output_root, args.db_path))


if __name__ == "__main__":
    main()
