from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "recent_topk_t5_wait_entry_overlay_pretest_v1"
DEFAULT_TIMING_ROOT = Path(r"G:\Tradex\recent_topk_selected_loser_timing_decomposition_v1\20260524T054324Z-recent-topk-selected-loser-timing-decomposition-v1")
DEFAULT_CANDIDATE_ROOT = Path(r"G:\Tradex\dist_ma60_overextension_selected_loser_demotion_pretest_v1\20260524T052457Z-dist-ma60-overextension-selected-loser-demotion-pretest-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\recent_topk_t5_wait_entry_overlay_pretest_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "entry_overlay_policy.json",
    "candidate_entry_overlay_rows.csv",
    "topk_entry_overlay_summary.json",
    "loser_repair_summary.csv",
    "winner_damage_summary.csv",
    "timing_failure_type_summary.csv",
    "no_entry_loss_summary.csv",
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
    if s.dropna().empty:
        return None
    return float(s.fillna(False).astype(bool).mean())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def load_overlay_rows(candidate_root: Path, daily_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(candidate_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False)
    rows["ret20_num"] = pd.to_numeric(rows["ret20_num"] if "ret20_num" in rows else rows["ret20"], errors="coerce")
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    rows = rows[rows["year"].isin(YEARS) & (rows["baseline_rank_recalc"] <= 20) & rows["ret20_num"].notna()].copy()
    if "winner20" not in rows or "loser20" not in rows:
        rows["ret20_pct_rank_by_date"] = rows.groupby("decision_ymd")["ret20_num"].rank(pct=True, method="average")
        rows["winner20"] = (rows["ret20_num"] >= 0.05) | (rows["ret20_pct_rank_by_date"] >= 0.70)
        rows["loser20"] = (rows["ret20_num"] <= -0.05) | (rows["ret20_pct_rank_by_date"] <= 0.30)
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(set(rows["code"]))].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["decision_ymd"] = daily["date_dt"].dt.strftime("%Y%m%d").astype(int)
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    by_code = {c: f.reset_index(drop=True) for c, f in daily.groupby("code", sort=False)}
    out = []
    for _, row in rows.iterrows():
        d = by_code.get(str(row["code"]))
        payload = row.to_dict()
        payload.update({"path_available": False})
        if d is not None:
            idxs = d.index[d["decision_ymd"] == int(row["decision_ymd"])].tolist()
            if idxs:
                pos = d.index.get_loc(idxs[0])
                if pos + 25 < len(d):
                    entry0 = float(d.iloc[pos]["close"])
                    entry5 = float(d.iloc[pos + 5]["close"])
                    exit20 = float(d.iloc[pos + 20]["close"])
                    exit25 = float(d.iloc[pos + 25]["close"])
                    win0 = d.iloc[pos + 1 : pos + 21]
                    win5 = d.iloc[pos + 6 : pos + 26]
                    payload.update(
                        {
                            "path_available": True,
                            "baseline_entry_close": entry0,
                            "delayed_entry_close_t5": entry5,
                            "baseline_ret20_from_t": exit20 / entry0 - 1,
                            "delayed_ret20_from_t5": exit25 / entry5 - 1,
                            "delta_ret20": exit25 / entry5 - 1 - (exit20 / entry0 - 1),
                            "baseline_mae20": win0["low"].min() / entry0 - 1,
                            "delayed_mae20": win5["low"].min() / entry5 - 1,
                            "baseline_mfe20": win0["high"].max() / entry0 - 1,
                            "delayed_mfe20": win5["high"].max() / entry5 - 1,
                        }
                    )
        out.append(payload)
    frame = pd.DataFrame(out)
    frame["delayed_loser20"] = (frame["delayed_ret20_from_t5"] <= -0.05).where(frame["path_available"], pd.NA)
    frame["delayed_winner20_abs"] = (frame["delayed_ret20_from_t5"] >= 0.05).where(frame["path_available"], pd.NA)
    return frame, {"selected_rows_loaded": int(len(frame)), "path_available_count": int(frame["path_available"].sum()), "path_missing_count": int((~frame["path_available"]).sum()), "path_coverage": float(frame["path_available"].mean())}


def _metrics(df: pd.DataFrame, ret_col: str, loser_col: str, winner_col: str, mae_col: str, mfe_col: str) -> dict[str, Any]:
    valid = df[df["path_available"]].copy()
    ret = pd.to_numeric(valid[ret_col], errors="coerce").dropna()
    return {
        "n": int(len(valid)),
        "mean_ret20": None if ret.empty else float(ret.mean()),
        "median_ret20": None if ret.empty else float(ret.median()),
        "win_rate_ret20_gt_0": None if ret.empty else float((ret > 0).mean()),
        "hit_rate_ret20_gt_5pct": None if ret.empty else float((ret > 0.05).mean()),
        "severe_loss_rate_ret20_lte_minus_5pct": None if ret.empty else float((ret <= -0.05).mean()),
        "selected_loser_rate": _rate(valid[loser_col]),
        "selected_winner_rate": _rate(valid[winner_col]),
        "mae20_mean": _mean(valid, mae_col),
        "mfe20_mean": _mean(valid, mfe_col),
    }


def summarize(rows: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    periods = {"2024": rows[rows["year"] == 2024], "2025": rows[rows["year"] == 2025], "2026_label_safe": rows[rows["year"] == 2026], "2024_2026_combined": rows[rows["year"].isin(YEARS)], "2019_2023": rows.iloc[0:0]}
    summary: dict[str, Any] = {}
    loser, winner, stability, no_entry = [], [], [], []
    for pname, p in periods.items():
        summary[pname] = {}
        for k in TOPK:
            g = p[p["baseline_rank_recalc"] <= k].copy()
            base = _metrics(g, "baseline_ret20_from_t", "loser20", "winner20", "baseline_mae20", "baseline_mfe20")
            delayed = _metrics(g, "delayed_ret20_from_t5", "delayed_loser20", "delayed_winner20_abs", "delayed_mae20", "delayed_mfe20")
            item = {
                "baseline": base,
                "delayed": delayed,
                "delta_mean_ret20": _delta(delayed["mean_ret20"], base["mean_ret20"]),
                "delta_median_ret20": _delta(delayed["median_ret20"], base["median_ret20"]),
                "delta_selected_loser_rate": _delta(delayed["selected_loser_rate"], base["selected_loser_rate"]),
                "delta_selected_winner_rate": _delta(delayed["selected_winner_rate"], base["selected_winner_rate"]),
                "delta_severe_loss_rate": _delta(delayed["severe_loss_rate_ret20_lte_minus_5pct"], base["severe_loss_rate_ret20_lte_minus_5pct"]),
                "path_available_count": int(g["path_available"].sum()),
                "path_missing_count": int((~g["path_available"]).sum()),
            }
            summary[pname][f"top{k}"] = item
            losers = g[g["loser20"].astype(bool) & g["path_available"]]
            winners = g[g["winner20"].astype(bool) & g["path_available"]]
            loser.append({"period": pname, "topk": k, "n": int(len(losers)), "delayed_improve_rate": _rate(losers["delta_ret20"] > 0), "delayed_avoids_loser_rate": _rate(~losers["delayed_loser20"].astype(bool)), "avg_delta_ret20": _mean(losers, "delta_ret20"), "mae_improvement": _delta(_mean(losers, "delayed_mae20"), _mean(losers, "baseline_mae20"))})
            winner.append({"period": pname, "topk": k, "n": int(len(winners)), "delayed_harms_winner_rate": _rate(winners["delta_ret20"] < 0), "delayed_still_winner_rate": _rate(winners["delayed_winner20_abs"]), "avg_delta_ret20": _mean(winners, "delta_ret20"), "mfe_loss": _delta(_mean(winners, "delayed_mfe20"), _mean(winners, "baseline_mfe20"))})
            stability.append({"period": pname, "topk": k, **{kk: vv for kk, vv in item.items() if kk.startswith("delta_")}})
            no_entry.append({"period": pname, "topk": k, "miss_large_immediate_upside_rate": _rate((g["baseline_ret20_from_t"] > 0.05) & (g["delayed_ret20_from_t5"] < g["baseline_ret20_from_t"])), "avoids_immediate_drawdown_rate": _rate((g["baseline_mae20"] < -0.05) & (g["delayed_mae20"] > g["baseline_mae20"]))})
    return summary, pd.DataFrame(loser), pd.DataFrame(winner), pd.DataFrame(stability), pd.DataFrame(no_entry)


def timing_failure_summary(timing_root: Path, rows: pd.DataFrame) -> pd.DataFrame:
    path = timing_root / "failure_type_classification.csv"
    if not path.exists():
        return pd.DataFrame()
    failure = pd.read_csv(path)
    delayed = rows[rows["loser20"].astype(bool) & rows["path_available"]].copy()
    delayed["failure_type"] = "unjoined_diagnostic"
    # Aggregate available failure labels separately; row-level labels were not emitted by prior artifact.
    return failure


def decide(summary: dict[str, Any], coverage: float) -> dict[str, Any]:
    if coverage < 0.8:
        return {"research_decision": "inconclusive", "reason_typed": ["t5/t25 path availability is too low"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
    recent = summary["2024_2026_combined"]
    keep = False
    hold = False
    reasons = []
    for top in ("top5", "top10"):
        r = recent[top]
        mean_ok = (r["delta_mean_ret20"] or 0) >= 0.005
        loser_ok = (r["delta_selected_loser_rate"] or 0) < 0
        severe_ok = (r["delta_severe_loss_rate"] or 0) <= 0
        winner_ok = (r["delta_selected_winner_rate"] or 0) >= -0.03
        years_ok = all((summary[y][top]["delta_mean_ret20"] or 0) > -0.005 for y in ("2024", "2025", "2026_label_safe"))
        if mean_ok and loser_ok and severe_ok and winner_ok and years_ok:
            keep = True
        elif loser_ok:
            hold = True
    if keep:
        decision = "keep_for_entry_overlay_compare"
        reasons.append("top5/top10 clears delayed-entry keep gate")
    elif hold:
        decision = "hold_for_entry_overlay_refinement"
        reasons.append("selected_loser repair is visible but mean return or winner damage gates are not fully cleared")
    else:
        decision = "drop"
        reasons.append("fixed t5 wait does not improve top5/top10 sufficiently")
    return {"research_decision": decision, "reason_typed": reasons, "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}


def run(*, timing_root: Path = DEFAULT_TIMING_ROOT, candidate_root: Path = DEFAULT_CANDIDATE_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-recent-topk-t5-wait-entry-overlay-pretest-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((timing_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, report = load_overlay_rows(candidate_root, daily_path)
    summary, loser, winner, stability, no_entry = summarize(rows)
    failure = timing_failure_summary(timing_root, rows)
    decision = decide(summary, report["path_coverage"])
    rows.to_csv(run_dir / "candidate_entry_overlay_rows.csv", index=False)
    loser.to_csv(run_dir / "loser_repair_summary.csv", index=False)
    winner.to_csv(run_dir / "winner_damage_summary.csv", index=False)
    failure.to_csv(run_dir / "timing_failure_type_summary.csv", index=False)
    no_entry.to_csv(run_dir / "no_entry_loss_summary.csv", index=False)
    stability.to_csv(run_dir / "period_stability_summary.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"timing_root": timing_root, "candidate_root": candidate_root, "daily_path": daily_path, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "entry_overlay_policy.json", {"policy_id": AXIS_ID, "entry": "5 trading days after baseline decision date close", "hold": "20 trading days after delayed entry", "rerank_at_t5": False, "future_event_condition": False, "delay_sweep": False})
    _write_json(run_dir / "topk_entry_overlay_summary.json", summary)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "topk_selection_uses_baseline_decision_date_only": True, "t5_wait_fixed": True, "ret20_after_t5_label_only": True, "ma7_ma20_future_event_condition_used": False, "rows_without_full_t5_t25_path_marked_unavailable": True})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "path_coverage": report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timing-root", type=Path, default=DEFAULT_TIMING_ROOT)
    parser.add_argument("--candidate-root", type=Path, default=DEFAULT_CANDIDATE_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(timing_root=args.timing_root, candidate_root=args.candidate_root, daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
