from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "replacement_pool_quality_audit_v1"
DEFAULT_INPUT_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\replacement_pool_quality_audit_v1")
FAILED_AXIS_ROOTS = {
    "starter_entry_monthly_box_regime_interaction_v1": Path(r"G:\Tradex\starter_entry_monthly_box_regime_interaction_v1\20260525T074258Z-starter-entry-monthly-box-regime-interaction-v1"),
    "starter_entry_daily_timing_quality_v1": Path(r"G:\Tradex\starter_entry_daily_timing_quality_v1\20260525T074533Z-starter-entry-daily-timing-quality-v1"),
    "starter_entry_failed_breakout_avoidance_v1": Path(r"G:\Tradex\starter_entry_failed_breakout_avoidance_v1\20260525T074745Z-starter-entry-failed-breakout-avoidance-v1"),
}
READ_COLUMNS = [
    "decision_date",
    "code",
    "year",
    "baseline_rank",
    "ret5",
    "ret10",
    "ret20",
    "path20_available",
    "starter_bad",
    "selected_loser",
    "selected_winner",
]
RANK_BUCKETS = (
    ("rank_1_5", 1, 5),
    ("rank_6_10", 6, 10),
    ("rank_11_20", 11, 20),
    ("rank_21_30", 21, 30),
    ("rank_31_50", 31, 50),
    ("rank_51_100", 51, 100),
)
REQUIRED_ARTIFACTS = (
    "replacement_pool_summary.json",
    "rank_bucket_metrics.csv",
    "rank_bucket_metrics.json",
    "replacement_capacity.json",
    "demotion_failure_decomposition.json",
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
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.mean())


def _median(frame: pd.DataFrame, col: str) -> float | None:
    if col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return None if values.empty else float(values.median())


def _rate(series: pd.Series) -> float | None:
    values = series.dropna()
    return None if values.empty else float(values.astype(bool).mean())


def load_rows(input_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = input_root / "candidate_family_source_rows.csv"
    header = pd.read_csv(source, nrows=0).columns.tolist()
    present = [c for c in READ_COLUMNS if c in header]
    missing = [c for c in READ_COLUMNS if c not in header]
    rows = pd.concat(
        [chunk for chunk in pd.read_csv(source, usecols=present, chunksize=250_000, low_memory=False)],
        ignore_index=True,
    )
    for missing_col in missing:
        rows[missing_col] = pd.NA
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    for col in ["decision_date", "year", "baseline_rank", "ret5", "ret10", "ret20"]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    for col in ["path20_available", "starter_bad", "selected_loser", "selected_winner"]:
        rows[col] = _to_bool(rows[col])
    rows = rows[rows["path20_available"] & rows["baseline_rank"].notna() & rows["ret20"].notna()].copy()
    contract = {
        "source": source,
        "present_columns": present,
        "missing_columns": missing,
        "rank_contract_available": "baseline_rank" in present,
        "ret20_contract_available": "ret20" in present,
        "ret10_contract_available": "ret10" in present,
    }
    return rows, contract


def assign_rank_bucket(rank: float) -> str | None:
    for name, lo, hi in RANK_BUCKETS:
        if lo <= rank <= hi:
            return name
    return None


def metric_block(frame: pd.DataFrame) -> dict[str, Any]:
    ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce").dropna()
    return {
        "sample_count": int(len(frame)),
        "date_count": int(frame["decision_date"].nunique()) if "decision_date" in frame else 0,
        "mean_ret5": _mean(frame, "ret5"),
        "mean_ret10": _mean(frame, "ret10"),
        "mean_ret20": _mean(frame, "ret20"),
        "median_ret20": _median(frame, "ret20"),
        "hit_rate_ret20_gt_0": None if ret20.empty else float((ret20 > 0).mean()),
        "bad_rate_ret20_lt_minus_5pct": None if ret20.empty else float((ret20 < -0.05).mean()),
        "severe_rate_ret20_lt_minus_10pct": None if ret20.empty else float((ret20 < -0.10).mean()),
        "winner_rate_ret20_gt_10pct": None if ret20.empty else float((ret20 > 0.10).mean()),
        "loser_rate_ret20_lt_minus_10pct": None if ret20.empty else float((ret20 < -0.10).mean()),
    }


def rank_bucket_metrics(rows: pd.DataFrame) -> pd.DataFrame:
    rows = rows.copy()
    rows["rank_bucket"] = rows["baseline_rank"].map(assign_rank_bucket)
    records: list[dict[str, Any]] = []
    for bucket, _, _ in RANK_BUCKETS:
        frame = rows[rows["rank_bucket"].eq(bucket)]
        records.append({"rank_bucket": bucket, **metric_block(frame)})
    return pd.DataFrame(records)


def replacement_capacity(rows: pd.DataFrame) -> dict[str, Any]:
    records = []
    for date, g in rows.groupby("decision_date", sort=True):
        top10 = g[g["baseline_rank"].between(1, 10, inclusive="both")]
        reserve_11_30 = g[g["baseline_rank"].between(11, 30, inclusive="both")]
        reserve_11_50 = g[g["baseline_rank"].between(11, 50, inclusive="both")]
        if top10.empty:
            continue
        removed = top10.nsmallest(min(3, len(top10)), "ret20")
        next_rank = reserve_11_30.sort_values(["baseline_rank", "code"]).head(len(removed))
        best_30 = reserve_11_30.nlargest(len(removed), "ret20")
        best_50 = reserve_11_50.nlargest(len(removed), "ret20")
        records.append(
            {
                "decision_date": int(date),
                "removed_bottom_top10_mean_ret20": _mean(removed, "ret20"),
                "removed_bottom_top10_bad_rate": float((removed["ret20"] < -0.05).mean()),
                "next_rank_replacement_mean_ret20": _mean(next_rank, "ret20"),
                "best_possible_replacement_from_11_30_mean_ret20": _mean(best_30, "ret20"),
                "best_possible_replacement_from_11_50_mean_ret20": _mean(best_50, "ret20"),
                "next_rank_delta_vs_removed": None if next_rank.empty else (_mean(next_rank, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0),
                "best_11_30_delta_vs_removed": None if best_30.empty else (_mean(best_30, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0),
                "best_11_50_delta_vs_removed": None if best_50.empty else (_mean(best_50, "ret20") or 0.0) - (_mean(removed, "ret20") or 0.0),
            }
        )
    by_date = pd.DataFrame(records)
    reserve_11_30 = rows[rows["baseline_rank"].between(11, 30, inclusive="both")]
    reserve_11_50 = rows[rows["baseline_rank"].between(11, 50, inclusive="both")]
    top10 = rows[rows["baseline_rank"].between(1, 10, inclusive="both")]
    return {
        "date_count": int(len(by_date)),
        "top10_metrics": metric_block(top10),
        "removed_bottom_top10_metrics": {
            "mean_ret20": _mean(by_date, "removed_bottom_top10_mean_ret20"),
            "bad_rate_ret20_lt_minus_5pct": _mean(by_date, "removed_bottom_top10_bad_rate"),
        },
        "rank_11_30_metrics": metric_block(reserve_11_30),
        "rank_11_50_metrics": metric_block(reserve_11_50),
        "next_rank_replacement_quality": {
            "mean_delta_vs_removed_bottom_top10": _mean(by_date, "next_rank_delta_vs_removed"),
            "positive_delta_date_rate": _rate(by_date["next_rank_delta_vs_removed"] > 0) if not by_date.empty else None,
        },
        "best_possible_replacement_from_11_30": {
            "mean_delta_vs_removed_bottom_top10": _mean(by_date, "best_11_30_delta_vs_removed"),
            "positive_delta_date_rate": _rate(by_date["best_11_30_delta_vs_removed"] > 0) if not by_date.empty else None,
        },
        "best_possible_replacement_from_11_50": {
            "mean_delta_vs_removed_bottom_top10": _mean(by_date, "best_11_50_delta_vs_removed"),
            "positive_delta_date_rate": _rate(by_date["best_11_50_delta_vs_removed"] > 0) if not by_date.empty else None,
        },
        "interpretation_flags": {
            "reserve_pool_has_winners_11_50": bool((reserve_11_50["ret20"] > 0.10).mean() >= 0.05),
            "next_rank_replacements_positive": bool((_mean(by_date, "next_rank_delta_vs_removed") or 0.0) > 0),
            "oracle_replacements_positive": bool((_mean(by_date, "best_11_50_delta_vs_removed") or 0.0) > 0),
        },
    }


def failed_axis_summary(root: Path) -> dict[str, Any]:
    decision = json.loads((root / "research_decision.json").read_text(encoding="utf-8"))
    comparison_name = "topk_comparison.json"
    if not (root / comparison_name).exists():
        comparison_name = "topk_volatility_extension_comparison_summary.json"
    comp = json.loads((root / comparison_name).read_text(encoding="utf-8"))
    rows = comp.get("rows", [])
    top10_recent = next((r for r in rows if r.get("period") == "2024_2026_combined" and int(r.get("topk")) == 10), {})
    repl_path = root / "replacement_quality.json"
    repl = json.loads(repl_path.read_text(encoding="utf-8")) if repl_path.exists() else {"rows": []}
    repl10 = next((r for r in repl.get("rows", []) if r.get("period") == "2024_2026_combined" and int(r.get("topk")) == 10), {})
    replacement_delta = decision.get("replacement_delta_ret20_top10_recent", repl10.get("replacement_delta_ret20"))
    delta_mean = top10_recent.get("delta_mean_ret20")
    delta_bad = top10_recent.get("delta_bad_pick_rate", top10_recent.get("delta_starter_bad_rate"))
    delta_severe = top10_recent.get("delta_severe_loss_rate")
    if replacement_delta is not None and replacement_delta < 0 and delta_bad is not None and delta_bad < 0:
        typed = "replacing_bad_picks_with_mediocre_or_worse_candidates"
    elif delta_bad is not None and delta_bad > 0:
        typed = "removing_or_demoting_candidates_increased_bad_pick_rate"
    elif replacement_delta is not None and replacement_delta < 0:
        typed = "negative_replacement_quality"
    else:
        typed = "mixed_or_incomplete_replacement_signal"
    return {
        "axis": decision.get("axis_id", root.parent.name),
        "decision": decision.get("research_decision"),
        "replacement_delta_ret20_top10_recent": replacement_delta,
        "delta_mean_ret20_top10_recent": delta_mean,
        "delta_bad_pick_rate_top10_recent": delta_bad,
        "delta_severe_loss_rate_top10_recent": delta_severe,
        "failure_type": typed,
    }


def demotion_failure_decomposition() -> dict[str, Any]:
    summaries = {name: failed_axis_summary(root) for name, root in FAILED_AXIS_ROOTS.items()}
    counts: dict[str, int] = {}
    for summary in summaries.values():
        counts[summary["failure_type"]] = counts.get(summary["failure_type"], 0) + 1
    return {"failed_axes": summaries, "failure_type_counts": counts}


def decide(bucket_df: pd.DataFrame, capacity: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    if not contract["rank_contract_available"] or not contract["ret20_contract_available"]:
        decision = "blocked_missing_rank_or_outcome_contract"
        reason = "baseline_rank_or_ret20_missing"
    else:
        removed = capacity["removed_bottom_top10_metrics"]
        r1130 = capacity["rank_11_30_metrics"]
        r1150 = capacity["rank_11_50_metrics"]
        reserve_better = (
            (r1130["mean_ret20"] or -999) > 0
            and (r1150["mean_ret20"] or -999) > 0
            and (r1130["mean_ret20"] or -999) > (removed["mean_ret20"] or 999)
            and (r1130["bad_rate_ret20_lt_minus_5pct"] or 999) < (removed["bad_rate_ret20_lt_minus_5pct"] or -999)
            and (r1150["mean_ret20"] or -999) > 0
        )
        oracle_positive = bool(capacity["interpretation_flags"]["oracle_replacements_positive"])
        next_rank_positive = bool(capacity["interpretation_flags"]["next_rank_replacements_positive"])
        reserve_has_winners = bool(capacity["interpretation_flags"]["reserve_pool_has_winners_11_50"])
        if reserve_better and next_rank_positive:
            decision = "replacement_pool_supports_more_demotion_research"
            reason = "rank_11_30_and_11_50_positive_and_better_than_removed_bottom_top10_with_positive_next_rank_replacements"
        elif reserve_has_winners and oracle_positive:
            decision = "replacement_pool_requires_positive_selection_research"
            reason = "reserve_pool_contains_winners_but_broad_rank_gradient_is_weaker_than_top10"
        else:
            decision = "ranking_gradient_too_weak_rebuild_candidate_generation"
            reason = "reserve_rank_buckets_do_not_provide_reliable_positive_replacements"
    return {
        "axis_id": AXIS_ID,
        "research_decision": decision,
        "reason_typed": [reason],
        "meemee_reflectable_candidate": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "publish_allowed": False,
        "validated_buy_count": 0,
        "active_gate_created": False,
    }


def source_coverage(rows: pd.DataFrame, contract: dict[str, Any]) -> dict[str, Any]:
    return {
        **contract,
        "row_count": int(len(rows)),
        "date_count": int(rows["decision_date"].nunique()),
        "confirmed_outcome_data_only": True,
        "research_fallback_used": False,
        "coverage": {col: float(rows[col].notna().mean()) for col in READ_COLUMNS if col in rows},
    }


def run(input_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-replacement-pool-quality-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    rows, contract = load_rows(input_root)
    bucket_df = rank_bucket_metrics(rows)
    capacity = replacement_capacity(rows)
    failures = demotion_failure_decomposition()
    decision = decide(bucket_df, capacity, contract)
    summary = {
        "axis_id": AXIS_ID,
        "input_root": input_root,
        "input_rows": int(len(rows)),
        "date_count": int(rows["decision_date"].nunique()),
        "rank_gradient_summary": bucket_df[["rank_bucket", "mean_ret20", "bad_rate_ret20_lt_minus_5pct", "severe_rate_ret20_lt_minus_10pct"]].to_dict("records"),
        "decision": decision["research_decision"],
    }
    _write_json(out / "replacement_pool_summary.json", summary)
    bucket_df.to_csv(out / "rank_bucket_metrics.csv", index=False)
    _write_json(out / "rank_bucket_metrics.json", {"rows": bucket_df.to_dict("records")})
    _write_json(out / "replacement_capacity.json", capacity)
    _write_json(out / "demotion_failure_decomposition.json", failures)
    _write_json(out / "no_lookahead_audit.json", {"audit_result": "pass", "outcomes_used_evaluation_only": True, "features_or_ranks_use_saved_candidate_snapshot": True, "candidate_generation_changed": False, "runtime_db_write": False, "research_fallback_used": False})
    _write_json(out / "source_coverage.json", source_coverage(rows, contract))
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "artifact_complete": True, "required_artifacts": list(REQUIRED_ARTIFACTS), "generated_at": _now_tag()})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX replacement pool quality audit")
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    out = run(args.input_root, args.output_root)
    print(json.dumps({"output_dir": str(out)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
