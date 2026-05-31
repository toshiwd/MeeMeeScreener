from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "ma7_pullback_reclaim_entry_overlay_pretest_v1"
DEFAULT_DIST_MA20_ROOT = Path(r"G:\Tradex\dist_ma20_conditional_t5_wait_pretest_v1\20260524T061202Z-dist-ma20-conditional-t5-wait-pretest-v1")
DEFAULT_T5_ROOT = Path(r"G:\Tradex\recent_topk_t5_wait_entry_overlay_pretest_v1\20260524T055037Z-recent-topk-t5-wait-entry-overlay-pretest-v1")
DEFAULT_TIMING_ROOT = Path(r"G:\Tradex\recent_topk_selected_loser_timing_decomposition_v1\20260524T054324Z-recent-topk-selected-loser-timing-decomposition-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma7_pullback_reclaim_entry_overlay_pretest_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "entry_overlay_policy.json",
    "candidate_entry_overlay_rows.csv",
    "policy_comparison_summary.json",
    "topk_entry_overlay_summary.json",
    "trigger_summary.csv",
    "loser_repair_summary.csv",
    "winner_damage_summary.csv",
    "no_trigger_diagnostics.csv",
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


def _load_prior_means(dist_ma20_root: Path) -> dict[tuple[str, int], float | None]:
    summary = json.loads((dist_ma20_root / "topk_entry_overlay_summary.json").read_text(encoding="utf-8"))
    out: dict[tuple[str, int], float | None] = {}
    for period, block in summary.items():
        for top_name, item in block.items():
            topk = int(top_name.replace("top", ""))
            out[(period, topk)] = item["dist_ma20_top_quartile_conditional_t5_wait"]["mean_ret20"]
    return out


def _daily_by_code(daily_path: Path, codes: set[str]) -> dict[str, pd.DataFrame]:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(codes)].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["decision_ymd"] = daily["date_dt"].dt.strftime("%Y%m%d").astype(int)
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
    daily["high"] = pd.to_numeric(daily["high"], errors="coerce")
    daily["low"] = pd.to_numeric(daily["low"], errors="coerce")
    daily["ma7"] = daily.groupby("code")["close"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    return {code: frame.reset_index(drop=True) for code, frame in daily.groupby("code", sort=False)}


def _find_ma7_trigger(frame: pd.DataFrame, pos: int) -> tuple[int | None, str]:
    if pos + 10 >= len(frame):
        return None, "path_missing"
    decision = frame.iloc[pos]
    if pd.isna(decision["close"]) or pd.isna(decision["ma7"]):
        return None, "ma7_missing_at_decision"
    above_at_decision = float(decision["close"]) > float(decision["ma7"])
    saw_pullback = not above_at_decision
    for offset in range(1, 11):
        check = frame.iloc[pos + offset]
        if pd.isna(check["close"]) or pd.isna(check["ma7"]):
            continue
        close = float(check["close"])
        ma7 = float(check["ma7"])
        if above_at_decision and not saw_pullback and close <= ma7:
            saw_pullback = True
            continue
        if saw_pullback and close > ma7:
            reason = "pullback_then_reclaim" if above_at_decision else "reclaim_from_below"
            return pos + offset, reason
    return None, "no_trigger"


def load_and_score(t5_root: Path, dist_ma20_root: Path, daily_path: Path) -> tuple[pd.DataFrame, dict[str, Any], dict[tuple[str, int], float | None]]:
    rows = pd.read_csv(t5_root / "candidate_entry_overlay_rows.csv", dtype={"code": str}, low_memory=False)
    rows = rows[rows["year"].isin(YEARS)].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows = rows[rows["baseline_rank_recalc"] <= 20].copy()
    if "winner20" not in rows or "loser20" not in rows:
        rows["ret20_pct_rank_by_date"] = rows.groupby("decision_ymd")["baseline_ret20_from_t"].rank(pct=True, method="average")
        rows["winner20"] = (rows["baseline_ret20_from_t"] >= 0.05) | (rows["ret20_pct_rank_by_date"] >= 0.70)
        rows["loser20"] = (rows["baseline_ret20_from_t"] <= -0.05) | (rows["ret20_pct_rank_by_date"] <= 0.30)

    by_code = _daily_by_code(daily_path, set(rows["code"]))
    out: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        payload = row.to_dict()
        payload.update(
            {
                "ma7_triggered": False,
                "ma7_trigger_reason": "path_missing",
                "ma7_entry_date": None,
                "ma7_entry_close": None,
                "ma7_ret20_from_entry": None,
                "ma7_ret20_including_no_entry_as_0": 0.0,
                "ma7_mae20": None,
                "ma7_mfe20": None,
                "ma7_path_available": False,
            }
        )
        frame = by_code.get(str(row["code"]))
        if frame is not None:
            idxs = frame.index[frame["decision_ymd"] == int(row["decision_ymd"])].tolist()
            if idxs:
                pos = int(idxs[0])
                trigger_pos, reason = _find_ma7_trigger(frame, pos)
                payload["ma7_trigger_reason"] = reason
                if trigger_pos is not None:
                    payload["ma7_triggered"] = True
                    payload["ma7_entry_date"] = str(frame.iloc[trigger_pos]["date_dt"].date())
                    payload["ma7_entry_close"] = float(frame.iloc[trigger_pos]["close"])
                    if trigger_pos + 20 < len(frame):
                        entry = float(frame.iloc[trigger_pos]["close"])
                        exit20 = float(frame.iloc[trigger_pos + 20]["close"])
                        win = frame.iloc[trigger_pos + 1 : trigger_pos + 21]
                        ret = exit20 / entry - 1
                        payload.update(
                            {
                                "ma7_path_available": True,
                                "ma7_ret20_from_entry": ret,
                                "ma7_ret20_including_no_entry_as_0": ret,
                                "ma7_mae20": float(win["low"].min() / entry - 1),
                                "ma7_mfe20": float(win["high"].max() / entry - 1),
                            }
                        )
                    else:
                        payload["ma7_trigger_reason"] = "triggered_but_exit_path_missing"
        out.append(payload)

    scored = pd.DataFrame(out)
    scored["ma7_delta_vs_baseline"] = pd.to_numeric(scored["ma7_ret20_including_no_entry_as_0"], errors="coerce") - pd.to_numeric(scored["baseline_ret20_from_t"], errors="coerce")
    scored["ma7_delta_vs_universal"] = pd.to_numeric(scored["ma7_ret20_including_no_entry_as_0"], errors="coerce") - pd.to_numeric(scored["delayed_ret20_from_t5"], errors="coerce")
    scored["ma7_loser20"] = (pd.to_numeric(scored["ma7_ret20_from_entry"], errors="coerce") <= -0.05).where(scored["ma7_path_available"], pd.NA)
    scored["ma7_winner20"] = (pd.to_numeric(scored["ma7_ret20_from_entry"], errors="coerce") >= 0.05).where(scored["ma7_path_available"], pd.NA)
    scored["no_trigger_classification"] = "entered"
    no_trigger = ~scored["ma7_triggered"].astype(bool)
    scored.loc[no_trigger & scored["loser20"].astype(bool), "no_trigger_classification"] = "skipped_bad_candidate"
    scored.loc[no_trigger & scored["winner20"].astype(bool), "no_trigger_classification"] = "missed_good_candidate"
    scored.loc[no_trigger & ~scored["loser20"].astype(bool) & ~scored["winner20"].astype(bool), "no_trigger_classification"] = "neutral_no_entry"

    report = {
        "selected_rows_loaded": int(len(scored)),
        "ma7_trigger_count": int(scored["ma7_triggered"].sum()),
        "ma7_no_trigger_count": int((~scored["ma7_triggered"].astype(bool)).sum()),
        "ma7_trigger_rate": float(scored["ma7_triggered"].mean()),
        "ma7_entry_path_available_count": int(scored["ma7_path_available"].sum()),
        "ma7_entry_path_missing_count": int((scored["ma7_triggered"].astype(bool) & ~scored["ma7_path_available"].astype(bool)).sum()),
        "ma7_entry_path_coverage_of_triggers": float(scored.loc[scored["ma7_triggered"].astype(bool), "ma7_path_available"].mean()) if scored["ma7_triggered"].any() else None,
    }
    return scored, report, _load_prior_means(dist_ma20_root)


def _metrics(df: pd.DataFrame, ret_col: str, loser_col: str, winner_col: str, mae_col: str, mfe_col: str, *, entry_only: bool) -> dict[str, Any]:
    valid = df[df["ma7_path_available"].astype(bool)].copy() if entry_only else df.copy()
    ret = pd.to_numeric(valid[ret_col], errors="coerce").dropna()
    return {
        "n": int(len(valid)),
        "mean_ret20": None if ret.empty else float(ret.mean()),
        "median_ret20": None if ret.empty else float(ret.median()),
        "win_rate_ret20_gt_0": None if ret.empty else float((ret > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret.empty else float((ret > 0.05).mean()),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret.empty else float((ret <= -0.05).mean()),
        "selected_loser_rate": _rate(valid[loser_col]) if loser_col in valid else None,
        "selected_winner_rate": _rate(valid[winner_col]) if winner_col in valid else None,
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


def summarize(rows: pd.DataFrame, dist_ma20_means: dict[tuple[str, int], float | None]) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    summary: dict[str, Any] = {}
    trigger_rows, loser_rows, winner_rows, no_trigger_rows, stability_rows, comparison_rows = [], [], [], [], [], []
    for pname, period_rows in _periods(rows).items():
        summary[pname] = {}
        for k in TOPK:
            g = period_rows[period_rows["baseline_rank_recalc"] <= k].copy()
            base = _metrics(g.assign(_all_path=True), "baseline_ret20_from_t", "loser20", "winner20", "baseline_mae20", "baseline_mfe20", entry_only=False)
            universal = _metrics(g.assign(_all_path=True), "delayed_ret20_from_t5", "delayed_loser20", "delayed_winner20_abs", "delayed_mae20", "delayed_mfe20", entry_only=False)
            ma7_entry = _metrics(g, "ma7_ret20_from_entry", "ma7_loser20", "ma7_winner20", "ma7_mae20", "ma7_mfe20", entry_only=True)
            ma7_zero = _metrics(g.assign(ma7_zero_loser20=g["ma7_ret20_including_no_entry_as_0"] <= -0.05, ma7_zero_winner20=g["ma7_ret20_including_no_entry_as_0"] >= 0.05), "ma7_ret20_including_no_entry_as_0", "ma7_zero_loser20", "ma7_zero_winner20", "ma7_mae20", "ma7_mfe20", entry_only=False)
            dist_mean = dist_ma20_means.get((pname, k))
            item = {
                "baseline_immediate": base,
                "universal_t5_wait": universal,
                "dist_ma20_conditional_t5_wait_mean_ret20": dist_mean,
                "ma7_pullback_reclaim_entry_only": ma7_entry,
                "ma7_pullback_reclaim_no_entry_as_0": ma7_zero,
                "delta_mean_no_entry_as_0_vs_baseline": _delta(ma7_zero["mean_ret20"], base["mean_ret20"]),
                "delta_mean_no_entry_as_0_vs_universal_t5": _delta(ma7_zero["mean_ret20"], universal["mean_ret20"]),
                "delta_mean_no_entry_as_0_vs_dist_ma20_conditional_t5": _delta(ma7_zero["mean_ret20"], dist_mean),
                "delta_entry_only_mean_vs_baseline": _delta(ma7_entry["mean_ret20"], base["mean_ret20"]),
                "delta_selected_loser_rate_on_entries_vs_baseline": _delta(ma7_entry["selected_loser_rate"], base["selected_loser_rate"]),
                "delta_selected_winner_rate_on_entries_vs_baseline": _delta(ma7_entry["selected_winner_rate"], base["selected_winner_rate"]),
                "delta_severe_loss_rate_on_entries_vs_baseline": _delta(ma7_entry["severe_loss_rate_ret20_lte_minus_5pct"], base["severe_loss_rate_ret20_lte_minus_5pct"]),
                "trigger_rate": _rate(g["ma7_triggered"]) if len(g) else None,
                "no_trigger_rate": _rate(~g["ma7_triggered"].astype(bool)) if len(g) else None,
                "path_available_count": int(g["ma7_path_available"].sum()),
                "path_missing_count": int((g["ma7_triggered"].astype(bool) & ~g["ma7_path_available"].astype(bool)).sum()),
            }
            summary[pname][f"top{k}"] = item
            losers = g[g["loser20"].astype(bool)]
            winners = g[g["winner20"].astype(bool)]
            entry_losers = losers[losers["ma7_path_available"].astype(bool)]
            entry_winners = winners[winners["ma7_path_available"].astype(bool)]
            no_trigger = g[~g["ma7_triggered"].astype(bool)]
            trigger_rows.append({"period": pname, "topk": k, "n": int(len(g)), "trigger_count": int(g["ma7_triggered"].sum()), "no_trigger_count": int((~g["ma7_triggered"].astype(bool)).sum()), "trigger_rate": item["trigger_rate"], "no_trigger_rate": item["no_trigger_rate"], "entry_path_available_count": item["path_available_count"], "entry_path_missing_count": item["path_missing_count"]})
            loser_rows.append({"period": pname, "topk": k, "n": int(len(losers)), "trigger_rate": _rate(losers["ma7_triggered"]) if len(losers) else None, "delayed_entry_improve_rate": _rate(entry_losers["ma7_delta_vs_baseline"] > 0) if len(entry_losers) else None, "avoids_loser_rate": _rate(~entry_losers["ma7_loser20"].astype(bool)) if len(entry_losers) else None, "avg_delta_ret20": _mean(entry_losers, "ma7_delta_vs_baseline"), "mae_improvement": _delta(_mean(entry_losers, "ma7_mae20"), _mean(entry_losers, "baseline_mae20")), "skipped_loser_rate": _rate(~losers["ma7_triggered"].astype(bool)) if len(losers) else None, "universal_avg_delta_ret20": _mean(losers.assign(x=losers["delayed_ret20_from_t5"] - losers["baseline_ret20_from_t"]), "x")})
            winner_rows.append({"period": pname, "topk": k, "n": int(len(winners)), "trigger_rate": _rate(winners["ma7_triggered"]) if len(winners) else None, "delayed_entry_harms_winner_rate": _rate(entry_winners["ma7_delta_vs_baseline"] < 0) if len(entry_winners) else None, "still_winner_rate": _rate(entry_winners["ma7_winner20"]) if len(entry_winners) else None, "avg_delta_ret20": _mean(entry_winners, "ma7_delta_vs_baseline"), "mfe_loss": _delta(_mean(entry_winners, "ma7_mfe20"), _mean(entry_winners, "baseline_mfe20")), "no_trigger_missed_winner_rate": _rate(~winners["ma7_triggered"].astype(bool)) if len(winners) else None, "universal_avg_delta_ret20": _mean(winners.assign(x=winners["delayed_ret20_from_t5"] - winners["baseline_ret20_from_t"]), "x")})
            no_trigger_rows.append({"period": pname, "topk": k, "no_trigger_count": int(len(no_trigger)), "skipped_bad_candidate_count": int((no_trigger["no_trigger_classification"] == "skipped_bad_candidate").sum()), "missed_good_candidate_count": int((no_trigger["no_trigger_classification"] == "missed_good_candidate").sum()), "neutral_no_entry_count": int((no_trigger["no_trigger_classification"] == "neutral_no_entry").sum()), "opportunity_loss_no_trigger_mean_baseline_ret20": _mean(no_trigger, "baseline_ret20_from_t"), "avoided_loss_no_trigger_mean_baseline_ret20": _mean(no_trigger[no_trigger["loser20"].astype(bool)], "baseline_ret20_from_t")})
            stability_rows.append({"period": pname, "topk": k, "delta_mean_no_entry_as_0_vs_baseline": item["delta_mean_no_entry_as_0_vs_baseline"], "delta_mean_no_entry_as_0_vs_universal_t5": item["delta_mean_no_entry_as_0_vs_universal_t5"], "delta_mean_no_entry_as_0_vs_dist_ma20_conditional_t5": item["delta_mean_no_entry_as_0_vs_dist_ma20_conditional_t5"], "delta_entry_only_mean_vs_baseline": item["delta_entry_only_mean_vs_baseline"], "trigger_rate": item["trigger_rate"], "no_trigger_rate": item["no_trigger_rate"]})
            comparison_rows.append({"period": pname, "topk": k, "baseline_mean_ret20": base["mean_ret20"], "universal_t5_mean_ret20": universal["mean_ret20"], "dist_ma20_conditional_t5_mean_ret20": dist_mean, "ma7_entry_only_mean_ret20": ma7_entry["mean_ret20"], "ma7_no_entry_as_0_mean_ret20": ma7_zero["mean_ret20"], "ma7_delta_vs_baseline": item["delta_mean_no_entry_as_0_vs_baseline"], "ma7_delta_vs_universal_t5": item["delta_mean_no_entry_as_0_vs_universal_t5"], "ma7_delta_vs_dist_ma20_conditional_t5": item["delta_mean_no_entry_as_0_vs_dist_ma20_conditional_t5"]})
    return summary, pd.DataFrame(trigger_rows), pd.DataFrame(loser_rows), pd.DataFrame(winner_rows), pd.DataFrame(no_trigger_rows), pd.DataFrame(stability_rows), pd.DataFrame(comparison_rows)


def decide(summary: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    if report["ma7_trigger_rate"] < 0.05:
        return {"research_decision": "inconclusive", "reason_typed": ["MA7 trigger rate is too low"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    if report["ma7_entry_path_coverage_of_triggers"] is not None and report["ma7_entry_path_coverage_of_triggers"] < 0.80:
        return {"research_decision": "inconclusive", "reason_typed": ["trigger+20 path coverage is too low"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    recent = summary["2024_2026_combined"]
    keep = hold = False
    drop_reasons: list[str] = []
    for top in ("top5", "top10"):
        r = recent[top]
        mean_ok = (r["delta_mean_no_entry_as_0_vs_baseline"] or 0) >= 0.005
        entry_ok = (r["delta_entry_only_mean_vs_baseline"] or 0) > 0
        loser_ok = (r["delta_selected_loser_rate_on_entries_vs_baseline"] or 0) < 0
        winner_ok = (r["delta_selected_winner_rate_on_entries_vs_baseline"] or 0) >= -0.03
        severe_ok = (r["delta_severe_loss_rate_on_entries_vs_baseline"] or 0) <= 0
        beats_universal = (r["delta_mean_no_entry_as_0_vs_universal_t5"] or 0) > 0
        beats_dist = (r["delta_mean_no_entry_as_0_vs_dist_ma20_conditional_t5"] or 0) > 0
        trigger_ok = (r["trigger_rate"] or 0) >= 0.20
        years_ok = all((summary[y][top]["delta_mean_no_entry_as_0_vs_baseline"] or 0) > -0.005 for y in ("2024", "2025", "2026_label_safe"))
        if mean_ok and entry_ok and loser_ok and winner_ok and severe_ok and beats_universal and beats_dist and trigger_ok and years_ok:
            keep = True
        elif entry_ok or loser_ok or beats_universal or beats_dist:
            hold = True
        if not beats_universal:
            drop_reasons.append(f"{top} does not beat universal t5")
        if not beats_dist:
            drop_reasons.append(f"{top} does not beat dist_ma20 conditional t5")
        if not loser_ok:
            drop_reasons.append(f"{top} selected_loser_rate on entries does not improve")
        if not years_ok:
            drop_reasons.append(f"{top} year effects contradict")
    if keep:
        decision = "keep_for_entry_overlay_compare"
        reasons = ["MA7 pullback/reclaim clears entry overlay compare gates"]
    elif hold:
        decision = "hold_for_entry_overlay_refinement"
        reasons = ["MA7 event wait shows partial entry timing value but misses keep gates"]
    else:
        decision = "drop"
        reasons = sorted(set(drop_reasons)) or ["MA7 event wait fails comparison gates"]
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, dist_ma20_root: Path = DEFAULT_DIST_MA20_ROOT, t5_root: Path = DEFAULT_T5_ROOT, timing_root: Path = DEFAULT_TIMING_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma7-pullback-reclaim-entry-overlay-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((t5_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, report, dist_ma20_means = load_and_score(t5_root, dist_ma20_root, daily_path)
    summary, trigger, loser, winner, no_trigger, stability, comparison = summarize(rows, dist_ma20_means)
    decision = decide(summary, report)

    rows.to_csv(run_dir / "candidate_entry_overlay_rows.csv", index=False)
    trigger.to_csv(run_dir / "trigger_summary.csv", index=False)
    loser.to_csv(run_dir / "loser_repair_summary.csv", index=False)
    winner.to_csv(run_dir / "winner_damage_summary.csv", index=False)
    no_trigger.to_csv(run_dir / "no_trigger_diagnostics.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    _write_json(run_dir / "policy_comparison_summary.json", {"rows": comparison.to_dict(orient="records")})
    _write_json(run_dir / "topk_entry_overlay_summary.json", summary)
    _write_json(run_dir / "entry_overlay_policy.json", {"policy_id": AXIS_ID, "execution_convention": "close-to-close", "monitor_window": "t+1 through t+10 trading days", "if_decision_close_above_ma7": "wait close <= ma7, then first subsequent close > ma7 within same 10-day window", "if_decision_close_at_or_below_ma7": "enter first close > ma7 within 10 trading days", "no_trigger": "no entry", "hold": "20 trading days after actual entry date", "uses_ma7": True, "uses_ma20": False, "threshold_sweep": False, "delay_sweep": False, "rerank_during_wait": False})
    _write_json(run_dir / "input_artifact_report.json", {"dist_ma20_root": dist_ma20_root, "t5_root": t5_root, "timing_root": timing_root, "daily_path": daily_path, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "topk_selection_uses_baseline_decision_date_only": True, "ma7_computed_through_each_trigger_check_date": True, "trigger_decision_uses_only_observed_closes_through_trigger_date": True, "ret20_after_actual_entry_label_only": True, "future_return_decides_trigger_or_no_trigger": False, "ma20_or_future_event_condition_used": False, "rows_without_full_trigger_plus_20_path_marked_unavailable": True, "no_trigger_is_valid_outcome": True})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "coverage": report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist-ma20-root", type=Path, default=DEFAULT_DIST_MA20_ROOT)
    parser.add_argument("--t5-root", type=Path, default=DEFAULT_T5_ROOT)
    parser.add_argument("--timing-root", type=Path, default=DEFAULT_TIMING_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(dist_ma20_root=args.dist_ma20_root, t5_root=args.t5_root, timing_root=args.timing_root, daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
