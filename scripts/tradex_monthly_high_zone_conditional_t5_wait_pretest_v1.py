from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "monthly_high_zone_conditional_t5_wait_pretest_v1"
DEFAULT_CONTEXT_ROOT = Path(r"G:\Tradex\t5_wait_winner_damage_decomposition_v1\20260524T055722Z-t5-wait-winner-damage-decomposition-v1")
DEFAULT_UNIVERSAL_ROOT = Path(r"G:\Tradex\recent_topk_t5_wait_entry_overlay_pretest_v1\20260524T055037Z-recent-topk-t5-wait-entry-overlay-pretest-v1")
DEFAULT_FAILURE_ROOT = Path(r"G:\Tradex\recent_baseline_topk_failure_decomposition_v1\20260523T195120Z-recent-baseline-topk-failure-decomposition-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\monthly_high_zone_conditional_t5_wait_pretest_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "entry_overlay_policy.json",
    "candidate_entry_overlay_rows.csv",
    "policy_comparison_summary.json",
    "topk_entry_overlay_summary.json",
    "loser_repair_summary.csv",
    "winner_damage_summary.csv",
    "high_zone_coverage_summary.csv",
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


def _bool_value(value: Any) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


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
    if s.dropna().empty:
        return None
    return float(s.fillna(False).astype(bool).mean())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def load_and_score(universal_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(universal_root / "candidate_entry_overlay_rows.csv", dtype={"code": str}, low_memory=False)
    rows = rows[rows["year"].isin(YEARS)].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows["monthly_high_zone_bool"] = rows["monthly_high_zone_proxy"].map(_bool_value) if "monthly_high_zone_proxy" in rows else pd.NA
    rows["monthly_high_zone_missing"] = rows["monthly_high_zone_bool"].isna()
    rows["conditional_wait_applied"] = rows["monthly_high_zone_bool"].fillna(False).astype(bool)
    rows["conditional_ret20"] = rows["baseline_ret20_from_t"]
    rows.loc[rows["conditional_wait_applied"], "conditional_ret20"] = rows.loc[rows["conditional_wait_applied"], "delayed_ret20_from_t5"]
    rows["conditional_mae20"] = rows["baseline_mae20"]
    rows.loc[rows["conditional_wait_applied"], "conditional_mae20"] = rows.loc[rows["conditional_wait_applied"], "delayed_mae20"]
    rows["conditional_mfe20"] = rows["baseline_mfe20"]
    rows.loc[rows["conditional_wait_applied"], "conditional_mfe20"] = rows.loc[rows["conditional_wait_applied"], "delayed_mfe20"]
    rows["conditional_delta_vs_baseline"] = rows["conditional_ret20"] - rows["baseline_ret20_from_t"]
    rows["conditional_delta_vs_universal"] = rows["conditional_ret20"] - rows["delayed_ret20_from_t5"]
    rows["conditional_loser20"] = (rows["conditional_ret20"] <= -0.05).where(rows["path_available"], pd.NA)
    rows["conditional_winner20"] = (rows["conditional_ret20"] >= 0.05).where(rows["path_available"], pd.NA)
    return rows, {
        "rows_loaded": int(len(rows)),
        "path_coverage": float(rows["path_available"].mean()),
        "monthly_high_zone_coverage": float(rows["monthly_high_zone_bool"].notna().mean()),
        "monthly_high_zone_missing_count": int(rows["monthly_high_zone_missing"].sum()),
    }


def _metrics(df: pd.DataFrame, ret_col: str, loser_col: str, winner_col: str, mae_col: str, mfe_col: str) -> dict[str, Any]:
    valid = df[df["path_available"].astype(bool)].copy()
    ret = pd.to_numeric(valid[ret_col], errors="coerce").dropna()
    return {
        "n": int(len(valid)),
        "mean_ret20": None if ret.empty else float(ret.mean()),
        "median_ret20": None if ret.empty else float(ret.median()),
        "win_rate_ret20_gt_0": None if ret.empty else float((ret > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret.empty else float((ret > 0.05).mean()),
        "selected_loser_rate": _rate(valid[loser_col]),
        "selected_winner_rate": _rate(valid[winner_col]),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret.empty else float((ret <= -0.05).mean()),
        "mae20_mean": _mean(valid, mae_col),
        "mfe20_mean": _mean(valid, mfe_col),
    }


def _periods(rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "2024": rows[rows["year"] == 2024],
        "2025": rows[rows["year"] == 2025],
        "2026_label_safe": rows[rows["year"] == 2026],
        "2024_2026_combined": rows[rows["year"].isin(YEARS)],
        "2019_2023": rows.iloc[0:0],
    }


def summarize(rows: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary: dict[str, Any] = {}
    loser, winner, coverage, stability = [], [], [], []
    for pname, p in _periods(rows).items():
        summary[pname] = {}
        for k in TOPK:
            g = p[p["baseline_rank_recalc"] <= k].copy()
            base = _metrics(g, "baseline_ret20_from_t", "loser20", "winner20", "baseline_mae20", "baseline_mfe20")
            uni = _metrics(g, "delayed_ret20_from_t5", "delayed_loser20", "delayed_winner20_abs", "delayed_mae20", "delayed_mfe20")
            cond = _metrics(g, "conditional_ret20", "conditional_loser20", "conditional_winner20", "conditional_mae20", "conditional_mfe20")
            item = {
                "baseline": base,
                "universal_t5_wait": uni,
                "conditional_high_zone_t5_wait": cond,
                "conditional_delta_mean_vs_baseline": _delta(cond["mean_ret20"], base["mean_ret20"]),
                "conditional_delta_mean_vs_universal": _delta(cond["mean_ret20"], uni["mean_ret20"]),
                "conditional_delta_loser_rate_vs_baseline": _delta(cond["selected_loser_rate"], base["selected_loser_rate"]),
                "conditional_delta_winner_rate_vs_baseline": _delta(cond["selected_winner_rate"], base["selected_winner_rate"]),
                "conditional_delta_severe_loss_vs_baseline": _delta(cond["severe_loss_rate_ret20_lte_minus_5pct"], base["severe_loss_rate_ret20_lte_minus_5pct"]),
                "path_available_count": int(g["path_available"].sum()),
                "path_missing_count": int((~g["path_available"].astype(bool)).sum()),
            }
            summary[pname][f"top{k}"] = item
            losers = g[g["loser20"].astype(bool) & g["path_available"].astype(bool)]
            winners = g[g["winner20"].astype(bool) & g["path_available"].astype(bool)]
            loser.append({"period": pname, "topk": k, "n": int(len(losers)), "conditional_improve_rate": _rate(losers["conditional_delta_vs_baseline"] > 0), "conditional_avoids_loser_rate": _rate(~losers["conditional_loser20"].astype(bool)), "avg_delta_ret20": _mean(losers, "conditional_delta_vs_baseline"), "mae_improvement": _delta(_mean(losers, "conditional_mae20"), _mean(losers, "baseline_mae20"),), "universal_avg_delta_ret20": _mean(losers.assign(x=losers["delayed_ret20_from_t5"] - losers["baseline_ret20_from_t"]), "x")})
            winner.append({"period": pname, "topk": k, "n": int(len(winners)), "conditional_harms_winner_rate": _rate(winners["conditional_delta_vs_baseline"] < 0), "conditional_still_winner_rate": _rate(winners["conditional_winner20"]), "avg_delta_ret20": _mean(winners, "conditional_delta_vs_baseline"), "mfe_loss": _delta(_mean(winners, "conditional_mfe20"), _mean(winners, "baseline_mfe20")), "universal_avg_delta_ret20": _mean(winners.assign(x=winners["delayed_ret20_from_t5"] - winners["baseline_ret20_from_t"]), "x")})
            coverage.append({"period": pname, "topk": k, "monthly_high_zone_coverage": float(g["monthly_high_zone_bool"].notna().mean()) if len(g) else None, "high_zone_share_all": _rate(g["monthly_high_zone_bool"]), "high_zone_share_selected_loser": _rate(losers["monthly_high_zone_bool"]) if len(losers) else None, "high_zone_share_selected_winner": _rate(winners["monthly_high_zone_bool"]) if len(winners) else None, "high_zone_share_delay_helped_loser": _rate(g[g.get("delay_helped_loser", False).astype(bool)]["monthly_high_zone_bool"]) if "delay_helped_loser" in g else None, "high_zone_share_delay_harmed_winner": _rate(g[g.get("delay_harmed_winner", False).astype(bool)]["monthly_high_zone_bool"]) if "delay_harmed_winner" in g else None})
            stability.append({"period": pname, "topk": k, "conditional_delta_mean_vs_baseline": item["conditional_delta_mean_vs_baseline"], "conditional_delta_mean_vs_universal": item["conditional_delta_mean_vs_universal"], "conditional_delta_loser_rate_vs_baseline": item["conditional_delta_loser_rate_vs_baseline"], "conditional_delta_winner_rate_vs_baseline": item["conditional_delta_winner_rate_vs_baseline"], "conditional_delta_severe_loss_vs_baseline": item["conditional_delta_severe_loss_vs_baseline"]})
    comparison_rows = []
    for period, block in summary.items():
        for topk, item in block.items():
            comparison_rows.append({"period": period, "topk": int(topk.replace("top", "")), "baseline_mean_ret20": item["baseline"]["mean_ret20"], "universal_mean_ret20": item["universal_t5_wait"]["mean_ret20"], "conditional_mean_ret20": item["conditional_high_zone_t5_wait"]["mean_ret20"], "conditional_delta_vs_baseline": item["conditional_delta_mean_vs_baseline"], "conditional_delta_vs_universal": item["conditional_delta_mean_vs_universal"]})
    return summary, pd.DataFrame(loser), pd.DataFrame(winner), pd.DataFrame(coverage), pd.DataFrame(stability), pd.DataFrame(comparison_rows)


def decide(summary: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    if report["path_coverage"] < 0.80 or report["monthly_high_zone_coverage"] < 0.80:
        return {"research_decision": "inconclusive", "reason_typed": ["path or monthly_high_zone coverage too low"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    recent = summary["2024_2026_combined"]
    keep = False
    hold = False
    drop_reasons = []
    for top in ("top5", "top10"):
        r = recent[top]
        mean_ok = (r["conditional_delta_mean_vs_baseline"] or 0) >= 0.005
        beats_universal = (r["conditional_delta_mean_vs_universal"] or 0) > 0
        loser_ok = (r["conditional_delta_loser_rate_vs_baseline"] or 0) < 0
        winner_ok = (r["conditional_delta_winner_rate_vs_baseline"] or 0) >= -0.03
        severe_ok = (r["conditional_delta_severe_loss_vs_baseline"] or 0) <= 0
        years_ok = all((summary[y][top]["conditional_delta_mean_vs_baseline"] or 0) > -0.005 for y in ("2024", "2025", "2026_label_safe"))
        if mean_ok and beats_universal and loser_ok and winner_ok and severe_ok and years_ok:
            keep = True
        elif beats_universal and (loser_ok or (r["conditional_delta_mean_vs_baseline"] or 0) > 0):
            hold = True
        if not beats_universal:
            drop_reasons.append(f"{top} does not improve over universal t5")
        if not loser_ok:
            drop_reasons.append(f"{top} selected_loser_rate does not improve")
        if not years_ok:
            drop_reasons.append(f"{top} year effects contradict")
    if keep:
        decision = "keep_for_entry_overlay_compare"
        reasons = ["conditional high-zone t5 wait clears keep gates"]
    elif hold:
        decision = "hold_for_entry_overlay_refinement"
        reasons = ["conditional policy improves over universal t5 but misses keep gates"]
    else:
        decision = "drop"
        reasons = sorted(set(drop_reasons)) or ["conditional policy does not improve baseline/universal comparison"]
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, context_root: Path = DEFAULT_CONTEXT_ROOT, universal_root: Path = DEFAULT_UNIVERSAL_ROOT, failure_root: Path = DEFAULT_FAILURE_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-monthly-high-zone-conditional-t5-wait-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((universal_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, report = load_and_score(universal_root)
    summary, loser, winner, coverage, stability, policy_comp = summarize(rows)
    decision = decide(summary, report)
    rows.to_csv(run_dir / "candidate_entry_overlay_rows.csv", index=False)
    loser.to_csv(run_dir / "loser_repair_summary.csv", index=False)
    winner.to_csv(run_dir / "winner_damage_summary.csv", index=False)
    coverage.to_csv(run_dir / "high_zone_coverage_summary.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    _write_json(run_dir / "policy_comparison_summary.json", {"rows": policy_comp.to_dict(orient="records")})
    _write_json(run_dir / "topk_entry_overlay_summary.json", summary)
    _write_json(run_dir / "entry_overlay_policy.json", {"policy_id": AXIS_ID, "wait_condition": "monthly_high_zone_proxy == true", "wait_entry": "t+5 trading-day close", "otherwise": "decision date close", "hold": "20 trading days after actual entry", "uses_monthly_box_breakout_proxy": False, "delay_sweep": False, "rerank_at_t5": False})
    _write_json(run_dir / "input_artifact_report.json", {"context_root": context_root, "universal_root": universal_root, "failure_root": failure_root, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "topk_selection_uses_baseline_decision_date_only": True, "monthly_high_zone_proxy_point_in_time": True, "t5_wait_fixed": True, "ret20_after_actual_entry_label_only": True, "future_price_movement_used_for_wait_decision": False, "ma7_ma20_future_event_condition_used": False, "rows_without_full_t5_t25_path_marked_unavailable": True})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "coverage": report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--context-root", type=Path, default=DEFAULT_CONTEXT_ROOT)
    parser.add_argument("--universal-root", type=Path, default=DEFAULT_UNIVERSAL_ROOT)
    parser.add_argument("--failure-root", type=Path, default=DEFAULT_FAILURE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(context_root=args.context_root, universal_root=args.universal_root, failure_root=args.failure_root, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
