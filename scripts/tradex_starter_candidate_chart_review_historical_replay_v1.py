from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import tradex_starter_candidate_chart_review_outcome_audit_v1 as outcome_audit
from scripts import tradex_starter_candidate_chart_review_pack_v1 as chart_review
from scripts import tradex_starter_candidate_review_pack_v2 as review_v2


AXIS_ID = "starter_candidate_chart_review_historical_replay_v1"
DEFAULT_FAMILY_SOURCE_ROOT = Path(
    r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1"
)
DEFAULT_CHART_REVIEW_ROOT = Path(
    r"G:\Tradex\starter_candidate_chart_review_pack_v1\20260525T061448Z-starter-candidate-chart-review-pack-v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_chart_review_historical_replay_v1")

REQUIRED_ARTIFACTS = (
    "historical_replay_summary.json",
    "historical_replay_rows.csv",
    "label_bucket_metrics.json",
    "label_comparison_metrics.json",
    "trigger_invalidation_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
    "date_selection_audit.json",
    "replay_contract.json",
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


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _ymd_to_epoch(ymd: int) -> int:
    return int(pd.Timestamp(str(int(ymd)), tz="UTC").timestamp())


def _epoch_to_ymd(epoch: int | float) -> int:
    return int(pd.to_datetime(int(epoch), unit="s", utc=True).strftime("%Y%m%d"))


def discover_historical_chart_review_roots(chart_review_root: Path) -> list[Path]:
    parent = chart_review_root.parent
    if not parent.exists():
        return []
    return sorted(p for p in parent.glob("*-starter-candidate-chart-review-pack-v1") if (p / "candidate_chart_review_rows.csv").exists())


def confirmed_source_report(db_path: Path) -> dict[str, Any]:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT source, min(date) AS min_date, max(date) AS max_date, count(*) AS row_count
            FROM daily_bars
            WHERE source IN ('pan', 'txt', 'confirmed')
            GROUP BY source
            ORDER BY source
            """
        ).fetchall()
    finally:
        con.close()
    by_source = [
        {"source": r[0], "min_date": _epoch_to_ymd(r[1]), "max_date": _epoch_to_ymd(r[2]), "row_count": int(r[3])}
        for r in rows
    ]
    return {
        "runtime_db_path": db_path,
        "confirmed_sources": by_source,
        "confirmed_min_date": min((r["min_date"] for r in by_source), default=None),
        "confirmed_max_date": max((r["max_date"] for r in by_source), default=None),
        "confirmed_source_only": bool(by_source),
    }


def monthly_grid_dates(candidate_dates: list[int], confirmed_max_date: int, min_year: int = 2019) -> tuple[list[int], list[dict[str, Any]]]:
    cutoff = int((pd.Timestamp(str(confirmed_max_date)) - pd.Timedelta(days=45)).strftime("%Y%m%d"))
    by_month: dict[str, list[int]] = {}
    excluded: list[dict[str, Any]] = []
    for ymd in sorted(set(int(d) for d in candidate_dates)):
        year = int(str(ymd)[:4])
        if year < min_year:
            excluded.append({"candidate_date": ymd, "reason": "before_min_year"})
            continue
        if ymd > cutoff:
            excluded.append({"candidate_date": ymd, "reason": "ret20_not_fully_observable_by_calendar_cutoff"})
            continue
        by_month.setdefault(str(ymd)[:6], []).append(ymd)
    selected = [max(values) for _, values in sorted(by_month.items())]
    return selected, excluded


def read_candidate_dates(source_path: Path) -> list[int]:
    dates: set[int] = set()
    for chunk in pd.read_csv(source_path, usecols=["decision_date"], chunksize=500_000, low_memory=False):
        values = pd.to_numeric(chunk["decision_date"], errors="coerce").dropna().astype(int)
        dates.update(values.tolist())
    return sorted(dates)


def read_rows_for_dates(source_path: Path, selected_dates: list[int]) -> pd.DataFrame:
    selected = set(int(d) for d in selected_dates)
    wanted = [
        "decision_date",
        "code",
        "baseline_rank",
        "baseline_score",
        "research_candidate_source_family",
        "primary_family",
        "diagnostic_candidate_role",
        "selected_loser",
        "starter_good",
        "starter_bad",
        "immediate_adverse_entry",
        "next_open_available",
        "entry_allowed_by_score",
        "path20_available",
        "research_risk_tags_json",
        "research_setup_tags_json",
        "research_regime_tags_json",
        "source_artifact_path",
        "source_run_id",
    ]
    available = set(pd.read_csv(source_path, nrows=0).columns)
    cols = [c for c in wanted if c in available]
    parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(source_path, usecols=cols, chunksize=500_000, low_memory=False):
        dates = pd.to_numeric(chunk["decision_date"], errors="coerce")
        part = chunk[dates.isin(selected)].copy()
        if not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame(columns=cols)
    rows = pd.concat(parts, ignore_index=True)
    rows["decision_date"] = pd.to_numeric(rows["decision_date"], errors="coerce").astype(int)
    rows["baseline_rank"] = pd.to_numeric(rows["baseline_rank"], errors="coerce")
    rows["baseline_score"] = pd.to_numeric(rows["baseline_score"], errors="coerce")
    rows["code"] = rows["code"].astype(str).str.removesuffix(".0")
    return rows


def rebuild_watch_rows(date_rows: pd.DataFrame) -> pd.DataFrame:
    picked = review_v2.select_candidates(date_rows, max_rows=10)
    out_rows: list[dict[str, Any]] = []
    for _, row in picked.iterrows():
        klass, reasons, score = review_v2.classify(row, keep_gated=False)
        record = {
            **row.to_dict(),
            "code": str(row["code"]).removesuffix(".0"),
            "candidate_action_class": klass,
            "validated_buy": False,
            "review_score": score,
            "classification_reason": "|".join(reasons),
            "not_validated_buy_reason": "no keep-gated validated challenger artifact",
        }
        out_rows.append(record)
    if not out_rows:
        return pd.DataFrame()
    review = pd.DataFrame(out_rows)
    return review[review["candidate_action_class"].eq("watch")].copy()


def apply_chart_review_labels(watch_rows: pd.DataFrame, db_path: Path) -> pd.DataFrame:
    if watch_rows.empty:
        return pd.DataFrame()
    labeled: list[dict[str, Any]] = []
    for decision_date, group in watch_rows.groupby("decision_date"):
        codes = group["code"].astype(str).tolist()
        bars = chart_review.load_bars(db_path, codes, int(decision_date))
        daily_rows = {r["code"]: r for r in (chart_review.daily_context_for(code, bars, int(decision_date)) for code in codes)}
        weekly_rows = {r["code"]: r for r in (chart_review.timeframe_context_for(code, bars, int(decision_date), "weekly") for code in codes)}
        monthly_rows = {r["code"]: r for r in (chart_review.timeframe_context_for(code, bars, int(decision_date), "monthly") for code in codes)}
        for _, row in group.iterrows():
            code = str(row["code"])
            judgment = chart_review.judge_candidate(row, daily_rows[code], weekly_rows[code], monthly_rows[code])
            checklist = "pass" if judgment["manual_judgment"] == "starter_ready" else ("fail" if judgment["manual_judgment"] == "avoid" else "partial")
            labeled.append(
                {
                    **row.to_dict(),
                    **judgment,
                    "family_checklist_result": checklist,
                    "feature_bar_source": "confirmed",
                    "feature_runtime_db_path": str(db_path),
                }
            )
    return pd.DataFrame(labeled)


def full_ret20_dates(rows: pd.DataFrame, bars: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if rows.empty:
        return rows, []
    audited = outcome_audit.audit_rows(rows, bars)
    excluded_dates: list[dict[str, Any]] = []
    keep_dates: list[int] = []
    for decision_date, group in audited.groupby("decision_date"):
        counts = pd.to_numeric(group.get("forward_bar_count"), errors="coerce").fillna(0)
        if len(group) and bool((counts >= 20).all()):
            keep_dates.append(int(decision_date))
        else:
            excluded_dates.append(
                {
                    "candidate_date": int(decision_date),
                    "reason": "ret20_not_fully_observable_from_confirmed_bars",
                    "min_forward_bar_count": int(counts.min()) if len(counts) else 0,
                }
            )
    return audited[audited["decision_date"].isin(keep_dates)].reset_index(drop=True), excluded_dates


def _metric(frame: pd.DataFrame, col: str, kind: str = "mean") -> float | None:
    values = pd.to_numeric(frame.get(col), errors="coerce").dropna()
    if values.empty:
        return None
    if kind == "median":
        return float(values.median())
    return float(values.mean())


def label_comparison_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    def group_metrics(frame: pd.DataFrame) -> dict[str, Any]:
        ret20 = pd.to_numeric(frame.get("ret20"), errors="coerce").dropna()
        return {
            "sample_count": int(len(frame)),
            "date_count": int(frame["decision_date"].nunique()) if "decision_date" in frame else 0,
            "code_count": int(frame["code"].astype(str).nunique()) if "code" in frame else 0,
            "mean_ret20": _metric(frame, "ret20"),
            "bad_rate_ret20_lt_minus_5pct": float((ret20 < -0.05).mean()) if not ret20.empty else None,
            "severe_rate_ret20_lt_minus_10pct": float((ret20 < -0.10).mean()) if not ret20.empty else None,
        }

    def compare(name: str, left: pd.DataFrame, right: pd.DataFrame) -> dict[str, Any]:
        lm = group_metrics(left)
        rm = group_metrics(right)
        delta = None if lm["mean_ret20"] is None or rm["mean_ret20"] is None else lm["mean_ret20"] - rm["mean_ret20"]
        return {
            "comparison": name,
            "left": lm,
            "right": rm,
            "mean_ret20_delta": delta,
            "sample_insufficient": lm["sample_count"] < 10 or rm["sample_count"] < 10 or rows["decision_date"].nunique() < 5,
        }

    family = rows["research_candidate_source_family"] if "research_candidate_source_family" in rows else pd.Series([""] * len(rows), index=rows.index)
    starter = rows[rows["manual_judgment"].eq("starter_ready")]
    return {
        "starter_ready_vs_all_non_ready": compare("starter_ready_vs_all_non_ready", starter, rows[~rows["manual_judgment"].eq("starter_ready")]),
        "starter_ready_vs_wait_for_trigger": compare("starter_ready_vs_wait_for_trigger", starter, rows[rows["manual_judgment"].eq("wait_for_trigger")]),
        "starter_ready_vs_avoid": compare("starter_ready_vs_avoid", starter, rows[rows["manual_judgment"].eq("avoid")]),
        "pullback_starter_ready_vs_breakout_starter_ready": compare(
            "pullback_starter_ready_vs_breakout_starter_ready",
            starter[family.loc[starter.index].eq("pullback_reclaim_source")],
            starter[family.loc[starter.index].eq("breakout_retest_source")],
        ),
        "early_trend_starter_ready_vs_other_starter_ready": compare(
            "early_trend_starter_ready_vs_other_starter_ready",
            starter[family.loc[starter.index].eq("early_trend_source")],
            starter[~family.loc[starter.index].eq("early_trend_source")],
        ),
    }


def decide_replay(rows: pd.DataFrame, comparisons: dict[str, Any]) -> str:
    if rows.empty:
        return "blocked_missing_historical_v3_watch_source"
    all_non_ready = comparisons["starter_ready_vs_all_non_ready"]
    ready = all_non_ready["left"]
    non_ready = all_non_ready["right"]
    date_count = int(rows["decision_date"].nunique())
    ready_n = int(ready["sample_count"])
    ready_mean = ready["mean_ret20"]
    non_ready_mean = non_ready["mean_ret20"]
    ready_bad = ready["bad_rate_ret20_lt_minus_5pct"]
    non_ready_bad = non_ready["bad_rate_ret20_lt_minus_5pct"]
    ready_severe = ready["severe_rate_ret20_lt_minus_10pct"]
    non_ready_severe = non_ready["severe_rate_ret20_lt_minus_10pct"]
    if ready_n == 0 or non_ready["sample_count"] == 0:
        return "sample_insufficient"
    if ready_mean is None or non_ready_mean is None or ready_bad is None or non_ready_bad is None:
        return "sample_insufficient"
    bad_lower = ready_bad < non_ready_bad
    severe_lower = ready_severe is not None and non_ready_severe is not None and ready_severe <= non_ready_severe
    if ready_mean <= non_ready_mean and not bad_lower:
        return "worse_than_non_ready" if ready_mean < non_ready_mean else "no_clear_separation"
    if ready_mean > non_ready_mean and bad_lower and severe_lower:
        if date_count < 12 or ready_n < 30:
            return "promising_but_underpowered"
        return "validated_separation_candidate"
    if date_count < 12 or ready_n < 30:
        return "sample_insufficient"
    return "no_clear_separation"


def any_comparison_underpowered(comparisons: dict[str, Any]) -> bool:
    return any(bool(v.get("sample_insufficient")) for v in comparisons.values() if isinstance(v, dict))


def run(
    family_source_root: Path,
    chart_review_root: Path,
    output_root: Path,
    db_path: Path | None = None,
    min_year: int = 2019,
) -> Path:
    out = output_root / f"{_now_tag()}-starter-candidate-chart-review-historical-replay-v1"
    out.mkdir(parents=True, exist_ok=True)
    source_path = family_source_root / "candidate_family_source_rows.csv"
    if not source_path.exists():
        raise RuntimeError(f"missing family source rows: {source_path}")
    historical_roots = discover_historical_chart_review_roots(chart_review_root)
    selected_db = db_path or outcome_audit.select_confirmed_db(20260508)
    source_report = confirmed_source_report(selected_db)
    confirmed_max_date = int(source_report["confirmed_max_date"])
    all_dates = read_candidate_dates(source_path)
    selected_dates, excluded_dates = monthly_grid_dates(all_dates, confirmed_max_date, min_year=min_year)
    candidate_rows = read_rows_for_dates(source_path, selected_dates)

    watch_parts: list[pd.DataFrame] = []
    zero_watch_dates: list[dict[str, Any]] = []
    for decision_date, date_rows in candidate_rows.groupby("decision_date"):
        watch = rebuild_watch_rows(date_rows.copy())
        if watch.empty:
            zero_watch_dates.append({"candidate_date": int(decision_date), "reason": "no_v3_watch_candidates_rebuilt"})
        else:
            watch_parts.append(watch)
    watch_rows = pd.concat(watch_parts, ignore_index=True) if watch_parts else pd.DataFrame()
    labeled = apply_chart_review_labels(watch_rows, selected_db) if not watch_rows.empty else pd.DataFrame()
    forward_bars = outcome_audit.load_forward_bars(selected_db, labeled) if not labeled.empty else pd.DataFrame()
    replay_rows, ret20_excluded = full_ret20_dates(labeled, forward_bars) if not labeled.empty else (pd.DataFrame(), [])
    replay_rows.to_csv(out / "historical_replay_rows.csv", index=False)

    metrics = outcome_audit.bucket_metrics(replay_rows) if not replay_rows.empty else {}
    comparisons = label_comparison_metrics(replay_rows) if not replay_rows.empty else {}
    decision = decide_replay(replay_rows, comparisons)
    comparison_underpowered = any_comparison_underpowered(comparisons)
    source_ok = bool(not forward_bars.empty and set(forward_bars["source"].dropna().unique()).issubset({"pan", "txt", "confirmed"}))
    no_lookahead_passes = bool(source_ok and not replay_rows.empty and (pd.to_numeric(replay_rows["forward_bar_count"], errors="coerce") >= 20).all())

    _write_json(out / "label_bucket_metrics.json", {"metrics_by_label": metrics})
    _write_json(out / "label_comparison_metrics.json", comparisons)
    _write_json(
        out / "trigger_invalidation_audit.json",
        {
            "trigger_hit_rate_by_label": {k: v.get("trigger_hit_rate") for k, v in metrics.items()},
            "invalidation_hit_rate_by_label": {k: v.get("invalidation_hit_rate") for k, v in metrics.items()},
            "trigger_then_ret20_metrics": {k: v.get("trigger_then_ret20_mean") for k, v in metrics.items()},
        },
    )
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "passes": no_lookahead_passes,
            "feature_labels_use_bars_through_as_of_date_only": True,
            "future_outcomes_start_after_decision_date": True,
            "future_outcomes_recomputed_from_runtime_confirmed_bars": True,
            "csv_forward_labels_used": False,
            "confirmed_bars_only": source_ok,
            "bar_sources": sorted(forward_bars["source"].dropna().unique().tolist()) if not forward_bars.empty else [],
        },
    )
    _write_json(
        out / "source_coverage.json",
        {
            **source_report,
            "family_source_rows_path": source_path,
            "historical_chart_review_roots_found": historical_roots,
            "historical_chart_review_root_count": len(historical_roots),
            "historical_v3_watch_snapshots_found": len(historical_roots) > 1,
            "replay_rows_count": int(len(replay_rows)),
            "date_count": int(replay_rows["decision_date"].nunique()) if not replay_rows.empty else 0,
            "code_count": int(replay_rows["code"].astype(str).nunique()) if not replay_rows.empty else 0,
            "runtime_db_write": False,
            "meemee_changed": False,
            "production_ranking_changed": False,
        },
    )
    _write_json(
        out / "date_selection_audit.json",
        {
            "policy": "monthly_grid_last_available_candidate_date_per_month_non_cherry_picked",
            "min_year": min_year,
            "confirmed_max_date": confirmed_max_date,
            "selected_dates": selected_dates,
            "selected_date_count": len(selected_dates),
            "excluded_dates": excluded_dates + zero_watch_dates + ret20_excluded,
        },
    )
    _write_json(
        out / "replay_contract.json",
        {
            "axis_id": AXIS_ID,
            "boundary": "TRADEX-only",
            "candidate_reconstruction": "review_v2.select_candidates + review_v2.classify; watch candidates only",
            "chart_label_logic": "tradex_starter_candidate_chart_review_pack_v1.judge_candidate",
            "outcome_logic": "tradex_starter_candidate_chart_review_outcome_audit_v1.audit_rows",
            "date_policy": "fixed monthly grid; no cherry-picked dates",
            "confirmed_runtime_bars_only": True,
            "label_threshold_tuning": False,
            "manual_override": False,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
        },
    )
    counts = {label: int((replay_rows["manual_judgment"] == label).sum()) for label in ["starter_ready", "watch_continue", "wait_for_trigger", "avoid"]} if not replay_rows.empty else {}
    _write_json(
        out / "historical_replay_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "sample_insufficient": comparison_underpowered or decision in {"sample_insufficient", "promising_but_underpowered"},
            "comparison_underpowered": comparison_underpowered,
            "sample_count": int(len(replay_rows)),
            "date_count": int(replay_rows["decision_date"].nunique()) if not replay_rows.empty else 0,
            "code_count": int(replay_rows["code"].astype(str).nunique()) if not replay_rows.empty else 0,
            "manual_judgment_counts": counts,
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "confirmed_source_only": source_ok,
            "runtime_db_path": selected_db,
            "historical_snapshots_found": len(historical_roots) > 1,
            "blocker": None if not replay_rows.empty else "blocked_missing_historical_v3_watch_source",
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family-source-root", type=Path, default=DEFAULT_FAMILY_SOURCE_ROOT)
    parser.add_argument("--chart-review-root", type=Path, default=DEFAULT_CHART_REVIEW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--min-year", type=int, default=2019)
    args = parser.parse_args()
    print(run(args.family_source_root, args.chart_review_root, args.output_root, args.db_path, args.min_year))


if __name__ == "__main__":
    main()
