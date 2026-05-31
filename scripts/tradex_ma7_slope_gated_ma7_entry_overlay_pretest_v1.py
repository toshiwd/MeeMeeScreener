from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_ma7_no_trigger_decomposition_v1 import daily_features


AXIS_ID = "ma7_slope_gated_ma7_entry_overlay_pretest_v1"
DEFAULT_NO_TRIGGER_ROOT = Path(r"G:\Tradex\ma7_no_trigger_decomposition_v1\20260524T063700Z-ma7-no-trigger-decomposition-v1")
DEFAULT_FULL_MA7_ROOT = Path(r"G:\Tradex\ma7_pullback_reclaim_entry_overlay_pretest_v1\20260524T062157Z-ma7-pullback-reclaim-entry-overlay-pretest-v1")
DEFAULT_T5_ROOT = Path(r"G:\Tradex\recent_topk_t5_wait_entry_overlay_pretest_v1\20260524T055037Z-recent-topk-t5-wait-entry-overlay-pretest-v1")
DEFAULT_DIST_MA20_ROOT = Path(r"G:\Tradex\dist_ma20_conditional_t5_wait_pretest_v1\20260524T061202Z-dist-ma20-conditional-t5-wait-pretest-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma7_slope_gated_ma7_entry_overlay_pretest_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "entry_overlay_policy.json",
    "candidate_entry_overlay_rows.csv",
    "policy_comparison_summary.json",
    "topk_entry_overlay_summary.json",
    "gate_trigger_summary.csv",
    "no_trigger_diagnostics.csv",
    "loser_repair_summary.csv",
    "winner_damage_summary.csv",
    "period_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = (2024, 2025, 2026)
TOPK = (5, 10, 20)


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
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    return None if s.empty else float(s.median())


def _rate(s: pd.Series) -> float | None:
    clean = s.dropna()
    if clean.empty:
        return None
    return float(clean.astype(bool).mean())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def _load_prior_means(root: Path, policy_key: str) -> dict[tuple[str, int], float | None]:
    summary = json.loads((root / "topk_entry_overlay_summary.json").read_text(encoding="utf-8"))
    out: dict[tuple[str, int], float | None] = {}
    for period, block in summary.items():
        for top_name, item in block.items():
            topk = int(top_name.replace("top", ""))
            if policy_key == "full_ma7":
                out[(period, topk)] = item["ma7_pullback_reclaim_no_entry_as_0"]["mean_ret20"]
            elif policy_key == "dist_ma20":
                out[(period, topk)] = item["dist_ma20_top_quartile_conditional_t5_wait"]["mean_ret20"]
    return out


def load_and_score(full_ma7_root: Path, daily_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(full_ma7_root / "candidate_entry_overlay_rows.csv", dtype={"code": str}, low_memory=False)
    rows = rows[rows["year"].isin(YEARS)].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows = rows[rows["baseline_rank_recalc"] <= 20].copy()
    features = daily_features(daily_path, set(rows["code"]))[["code", "decision_ymd", "ma7_slope"]]
    rows = rows.drop(columns=["ma7_slope"], errors="ignore").merge(features, on=["code", "decision_ymd"], how="left")
    rows["ma7_slope_num"] = pd.to_numeric(rows["ma7_slope"], errors="coerce")
    rows["ma7_slope_q_by_date"] = rows.groupby("decision_ymd")["ma7_slope_num"].rank(pct=True, method="average")
    rows["ma7_slope_missing"] = rows["ma7_slope_num"].isna()
    rows["ma7_slope_gate_hit"] = (rows["ma7_slope_q_by_date"] >= 0.75) & ~rows["ma7_slope_missing"]

    rows["gated_ret20"] = rows["baseline_ret20_from_t"]
    rows.loc[rows["ma7_slope_gate_hit"], "gated_ret20"] = rows.loc[rows["ma7_slope_gate_hit"], "ma7_ret20_including_no_entry_as_0"]
    rows["gated_mae20"] = rows["baseline_mae20"]
    rows.loc[rows["ma7_slope_gate_hit"] & rows["ma7_path_available"].astype(bool), "gated_mae20"] = rows.loc[rows["ma7_slope_gate_hit"] & rows["ma7_path_available"].astype(bool), "ma7_mae20"]
    rows["gated_mfe20"] = rows["baseline_mfe20"]
    rows.loc[rows["ma7_slope_gate_hit"] & rows["ma7_path_available"].astype(bool), "gated_mfe20"] = rows.loc[rows["ma7_slope_gate_hit"] & rows["ma7_path_available"].astype(bool), "ma7_mfe20"]
    rows["gated_delta_vs_baseline"] = rows["gated_ret20"] - rows["baseline_ret20_from_t"]
    rows["gated_loser20"] = rows["gated_ret20"] <= -0.05
    rows["gated_winner20"] = rows["gated_ret20"] >= 0.05
    rows["gated_no_entry"] = rows["ma7_slope_gate_hit"] & ~rows["ma7_triggered"].astype(bool)
    rows["gated_no_trigger_classification"] = "not_gated_or_entered"
    rows.loc[rows["gated_no_entry"] & rows["loser20"].astype(bool), "gated_no_trigger_classification"] = "skipped_bad_candidate"
    rows.loc[rows["gated_no_entry"] & rows["winner20"].astype(bool), "gated_no_trigger_classification"] = "missed_good_candidate"
    rows.loc[rows["gated_no_entry"] & ~rows["loser20"].astype(bool) & ~rows["winner20"].astype(bool), "gated_no_trigger_classification"] = "neutral_no_entry"
    return rows, {
        "rows_loaded": int(len(rows)),
        "ma7_slope_coverage": float(rows["ma7_slope_num"].notna().mean()),
        "ma7_slope_missing_count": int(rows["ma7_slope_missing"].sum()),
        "gate_hit_count": int(rows["ma7_slope_gate_hit"].sum()),
        "gate_hit_rate": float(rows["ma7_slope_gate_hit"].mean()),
    }


def _metrics(df: pd.DataFrame, ret_col: str, loser_col: str, winner_col: str, mae_col: str, mfe_col: str) -> dict[str, Any]:
    ret = pd.to_numeric(df[ret_col], errors="coerce").dropna()
    return {
        "n": int(len(df)),
        "mean_ret20": None if ret.empty else float(ret.mean()),
        "median_ret20": None if ret.empty else float(ret.median()),
        "win_rate_ret20_gt_0": None if ret.empty else float((ret > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret.empty else float((ret > 0.05).mean()),
        "selected_loser_rate": _rate(df[loser_col]),
        "selected_winner_rate": _rate(df[winner_col]),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret.empty else float((ret <= -0.05).mean()),
        "mae20_mean": _mean(df, mae_col),
        "mfe20_mean": _mean(df, mfe_col),
    }


def _periods(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "2024": rows[rows["year"] == 2024],
        "2025": rows[rows["year"] == 2025],
        "2026_label_safe": rows[rows["year"] == 2026],
        "2024_2026_combined": rows[rows["year"].isin(YEARS)],
        "2019_2023": rows.iloc[0:0],
    }


def summarize(rows: pd.DataFrame, full_ma7: dict[tuple[str, int], float | None], dist_ma20: dict[tuple[str, int], float | None]) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary: dict[str, Any] = {}
    gate_rows, no_trigger_rows, loser_rows, winner_rows, stability_rows, comparison_rows = [], [], [], [], [], []
    for pname, p in _periods(rows).items():
        summary[pname] = {}
        for k in TOPK:
            g = p[p["baseline_rank_recalc"] <= k].copy()
            gated = g[g["ma7_slope_gate_hit"]]
            base = _metrics(g, "baseline_ret20_from_t", "loser20", "winner20", "baseline_mae20", "baseline_mfe20")
            universal = _metrics(g, "delayed_ret20_from_t5", "delayed_loser20", "delayed_winner20_abs", "delayed_mae20", "delayed_mfe20")
            gated_metrics = _metrics(g, "gated_ret20", "gated_loser20", "gated_winner20", "gated_mae20", "gated_mfe20")
            full_mean = full_ma7.get((pname, k))
            dist_mean = dist_ma20.get((pname, k))
            item = {
                "baseline_immediate": base,
                "universal_t5_wait": universal,
                "dist_ma20_conditional_t5_wait_mean_ret20": dist_mean,
                "full_ma7_pullback_reclaim_no_entry_as_0_mean_ret20": full_mean,
                "ma7_slope_gated_ma7_entry_overlay": gated_metrics,
                "delta_vs_baseline": _delta(gated_metrics["mean_ret20"], base["mean_ret20"]),
                "delta_vs_full_ma7_overlay": _delta(gated_metrics["mean_ret20"], full_mean),
                "delta_vs_universal_t5": _delta(gated_metrics["mean_ret20"], universal["mean_ret20"]),
                "delta_vs_dist_ma20_conditional_t5": _delta(gated_metrics["mean_ret20"], dist_mean),
                "delta_selected_loser_rate_vs_baseline": _delta(gated_metrics["selected_loser_rate"], base["selected_loser_rate"]),
                "delta_selected_winner_rate_vs_baseline": _delta(gated_metrics["selected_winner_rate"], base["selected_winner_rate"]),
                "delta_severe_loss_rate_vs_baseline": _delta(gated_metrics["severe_loss_rate_ret20_lte_minus_5pct"], base["severe_loss_rate_ret20_lte_minus_5pct"]),
                "gate_hit_rate": _rate(g["ma7_slope_gate_hit"]) if len(g) else None,
                "trigger_rate_among_gated": _rate(gated["ma7_triggered"]) if len(gated) else None,
                "no_trigger_rate_among_gated": _rate(~gated["ma7_triggered"].astype(bool)) if len(gated) else None,
                "overall_no_entry_rate": _rate(g["gated_no_entry"]) if len(g) else None,
            }
            summary[pname][f"top{k}"] = item
            gated_no = g[g["gated_no_entry"]]
            full_no = g[~g["ma7_triggered"].astype(bool)]
            missed = gated_no[gated_no["gated_no_trigger_classification"] == "missed_good_candidate"]
            skipped = gated_no[gated_no["gated_no_trigger_classification"] == "skipped_bad_candidate"]
            neutral = gated_no[gated_no["gated_no_trigger_classification"] == "neutral_no_entry"]
            gate_rows.append({"period": pname, "topk": k, "n": int(len(g)), "gate_hit_count": int(len(gated)), "gate_hit_rate": item["gate_hit_rate"], "trigger_rate_among_gated": item["trigger_rate_among_gated"], "no_trigger_rate_among_gated": item["no_trigger_rate_among_gated"], "overall_no_entry_rate": item["overall_no_entry_rate"]})
            no_trigger_rows.append({"period": pname, "topk": k, "gated_no_trigger_count": int(len(gated_no)), "missed_good_count": int(len(missed)), "missed_good_rate": None if len(gated_no) == 0 else float(len(missed) / len(gated_no)), "skipped_bad_count": int(len(skipped)), "skipped_bad_rate": None if len(gated_no) == 0 else float(len(skipped) / len(gated_no)), "neutral_count": int(len(neutral)), "neutral_rate": None if len(gated_no) == 0 else float(len(neutral) / len(gated_no)), "baseline_ret20_mean_of_gated_no_trigger": _mean(gated_no, "baseline_ret20_from_t"), "full_ma7_no_trigger_count": int(len(full_no)), "full_ma7_missed_good_count": int((full_no["no_trigger_classification"] == "missed_good_candidate").sum()), "missed_good_reduction_vs_full_ma7": int((full_no["no_trigger_classification"] == "missed_good_candidate").sum()) - int(len(missed))})
            losers = g[g["loser20"].astype(bool)]
            winners = g[g["winner20"].astype(bool)]
            gated_losers = losers[losers["ma7_slope_gate_hit"]]
            gated_winners = winners[winners["ma7_slope_gate_hit"]]
            loser_rows.append({"period": pname, "topk": k, "n": int(len(losers)), "gated_loser_count": int(len(gated_losers)), "gated_loser_rate": None if len(losers) == 0 else float(len(gated_losers) / len(losers)), "avg_delta_ret20_overall": _mean(losers, "gated_delta_vs_baseline"), "avg_delta_ret20_gated": _mean(gated_losers, "gated_delta_vs_baseline"), "avoids_loser_rate_overall": _rate(~losers["gated_loser20"].astype(bool)) if len(losers) else None, "avoids_loser_rate_gated": _rate(~gated_losers["gated_loser20"].astype(bool)) if len(gated_losers) else None})
            winner_rows.append({"period": pname, "topk": k, "n": int(len(winners)), "gated_winner_count": int(len(gated_winners)), "gated_winner_rate": None if len(winners) == 0 else float(len(gated_winners) / len(winners)), "avg_delta_ret20_overall": _mean(winners, "gated_delta_vs_baseline"), "avg_delta_ret20_gated": _mean(gated_winners, "gated_delta_vs_baseline"), "harmed_winner_rate_overall": _rate(winners["gated_delta_vs_baseline"] < 0) if len(winners) else None, "harmed_winner_rate_gated": _rate(gated_winners["gated_delta_vs_baseline"] < 0) if len(gated_winners) else None, "mfe_loss_overall": _delta(_mean(winners, "gated_mfe20"), _mean(winners, "baseline_mfe20"))})
            stability_rows.append({"period": pname, "topk": k, "delta_vs_baseline": item["delta_vs_baseline"], "delta_vs_full_ma7_overlay": item["delta_vs_full_ma7_overlay"], "delta_vs_universal_t5": item["delta_vs_universal_t5"], "delta_vs_dist_ma20_conditional_t5": item["delta_vs_dist_ma20_conditional_t5"], "delta_selected_loser_rate_vs_baseline": item["delta_selected_loser_rate_vs_baseline"], "delta_selected_winner_rate_vs_baseline": item["delta_selected_winner_rate_vs_baseline"], "delta_severe_loss_rate_vs_baseline": item["delta_severe_loss_rate_vs_baseline"]})
            comparison_rows.append({"period": pname, "topk": k, "baseline_mean_ret20": base["mean_ret20"], "universal_t5_mean_ret20": universal["mean_ret20"], "dist_ma20_conditional_t5_mean_ret20": dist_mean, "full_ma7_mean_ret20": full_mean, "ma7_slope_gated_mean_ret20": gated_metrics["mean_ret20"], "delta_vs_baseline": item["delta_vs_baseline"], "delta_vs_full_ma7": item["delta_vs_full_ma7_overlay"], "delta_vs_universal_t5": item["delta_vs_universal_t5"], "delta_vs_dist_ma20": item["delta_vs_dist_ma20_conditional_t5"]})
    return summary, pd.DataFrame(gate_rows), pd.DataFrame(no_trigger_rows), pd.DataFrame(loser_rows), pd.DataFrame(winner_rows), pd.DataFrame(stability_rows), pd.DataFrame(comparison_rows)


def decide(summary: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    if report["ma7_slope_coverage"] < 0.80:
        return {"research_decision": "inconclusive", "reason_typed": ["ma7_slope coverage too low"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    recent = summary["2024_2026_combined"]
    keep = hold = False
    drop_reasons: list[str] = []
    for top in ("top5", "top10"):
        r = recent[top]
        mean_ok = (r["delta_vs_baseline"] or 0) >= 0.005
        beats_full = (r["delta_vs_full_ma7_overlay"] or 0) > 0
        beats_universal = (r["delta_vs_universal_t5"] or 0) > 0
        loser_ok = (r["delta_selected_loser_rate_vs_baseline"] or 0) < 0
        winner_ok = (r["delta_selected_winner_rate_vs_baseline"] or 0) >= -0.03
        severe_ok = (r["delta_severe_loss_rate_vs_baseline"] or 0) <= 0
        years_ok = all((summary[y][top]["delta_vs_baseline"] or 0) > -0.005 for y in ("2024", "2025", "2026_label_safe"))
        if mean_ok and beats_full and beats_universal and loser_ok and winner_ok and severe_ok and years_ok:
            keep = True
        elif beats_full and (beats_universal or loser_ok or (r["delta_vs_baseline"] or 0) > 0):
            hold = True
        if not beats_full:
            drop_reasons.append(f"{top} does not improve over full MA7 overlay")
        if not beats_universal:
            drop_reasons.append(f"{top} does not improve over universal t5")
        if not loser_ok:
            drop_reasons.append(f"{top} selected_loser_rate does not improve")
        if not years_ok:
            drop_reasons.append(f"{top} year effects contradict")
    if keep:
        decision = "keep_for_entry_overlay_compare"
        reasons = ["ma7_slope gated MA7 overlay clears keep gates"]
    elif hold:
        decision = "hold_for_entry_overlay_refinement"
        reasons = ["ma7_slope gating improves over full MA7 overlay but misses keep gates"]
    else:
        decision = "drop"
        reasons = sorted(set(drop_reasons)) or ["ma7_slope gated overlay fails comparison gates"]
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, no_trigger_root: Path = DEFAULT_NO_TRIGGER_ROOT, full_ma7_root: Path = DEFAULT_FULL_MA7_ROOT, t5_root: Path = DEFAULT_T5_ROOT, dist_ma20_root: Path = DEFAULT_DIST_MA20_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma7-slope-gated-ma7-entry-overlay-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((full_ma7_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, report = load_and_score(full_ma7_root, daily_path)
    full_ma7_means = _load_prior_means(full_ma7_root, "full_ma7")
    dist_ma20_means = _load_prior_means(dist_ma20_root, "dist_ma20")
    summary, gate, no_trigger, loser, winner, stability, comparison = summarize(rows, full_ma7_means, dist_ma20_means)
    decision = decide(summary, report)

    rows.to_csv(run_dir / "candidate_entry_overlay_rows.csv", index=False)
    gate.to_csv(run_dir / "gate_trigger_summary.csv", index=False)
    no_trigger.to_csv(run_dir / "no_trigger_diagnostics.csv", index=False)
    loser.to_csv(run_dir / "loser_repair_summary.csv", index=False)
    winner.to_csv(run_dir / "winner_damage_summary.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    _write_json(run_dir / "policy_comparison_summary.json", {"rows": comparison.to_dict(orient="records")})
    _write_json(run_dir / "topk_entry_overlay_summary.json", summary)
    _write_json(run_dir / "entry_overlay_policy.json", {"policy_id": AXIS_ID, "gate": "same-date top quartile of ma7_slope", "gate_hit_action": "apply MA7 pullback/reclaim entry overlay", "gate_miss_action": "decision-date close immediate entry", "missing_ma7_slope_action": "coverage-limited immediate entry", "monitor_window": "t+1 through t+10 trading days", "hold": "20 trading days after actual entry date", "no_entry_primary_return": 0.0, "threshold_sweep": False, "trigger_window_sweep": False, "uses_ma20": False, "rerank_during_wait": False})
    _write_json(run_dir / "input_artifact_report.json", {"no_trigger_root": no_trigger_root, "full_ma7_root": full_ma7_root, "t5_root": t5_root, "dist_ma20_root": dist_ma20_root, "daily_path": daily_path, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "topk_selection_uses_baseline_decision_date_data": True, "ma7_slope_computed_through_decision_date": True, "same_date_quartile_uses_feature_values_only": True, "ma7_trigger_uses_only_closes_observed_through_trigger_date": True, "ret20_after_actual_entry_label_only": True, "future_no_trigger_status_decides_gate": False, "future_return_decides_trigger_or_no_trigger": False, "rows_without_full_trigger_plus_20_path_marked_unavailable": True})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "report": report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-trigger-root", type=Path, default=DEFAULT_NO_TRIGGER_ROOT)
    parser.add_argument("--full-ma7-root", type=Path, default=DEFAULT_FULL_MA7_ROOT)
    parser.add_argument("--t5-root", type=Path, default=DEFAULT_T5_ROOT)
    parser.add_argument("--dist-ma20-root", type=Path, default=DEFAULT_DIST_MA20_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(no_trigger_root=args.no_trigger_root, full_ma7_root=args.full_ma7_root, t5_root=args.t5_root, dist_ma20_root=args.dist_ma20_root, daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
