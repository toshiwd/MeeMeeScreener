from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "recent_topk_selected_loser_timing_decomposition_v1"
DEFAULT_CANDIDATE_ROOT = Path(r"G:\Tradex\dist_ma60_overextension_selected_loser_demotion_pretest_v1\20260524T052457Z-dist-ma60-overextension-selected-loser-demotion-pretest-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\recent_topk_selected_loser_timing_decomposition_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "selected_loser_timing_profile.csv",
    "loser_vs_winner_timing_contrast.csv",
    "delayed_entry_diagnostic_summary.csv",
    "failure_type_classification.csv",
    "next_axis_candidates.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
YEARS = (2024, 2025, 2026)
TOPK = (5, 10)
PATH_FEATURES = ("ret1", "ret3", "ret5", "ret10", "ret20_path", "mae5", "mae10", "mae20_path", "mfe5", "mfe10", "mfe20_path")
CONTEXT_FEATURES = (
    "dist_ma7_pct",
    "dist_ma20_pct",
    "dist_ma60_pct",
    "upper_wick_ratio",
    "failed_high_update",
    "large_bearish_candle",
    "above7_streak",
    "above20_streak",
    "above60_streak",
    "realized_vol20",
    "atr14_pct",
    "close_below_ma7",
    "close_below_ma20",
    "ma7_slope_down",
    "ma20_slope_down",
)


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


def _rate(series: pd.Series) -> float | None:
    if series.dropna().empty:
        return None
    return float(series.fillna(False).astype(bool).mean())


def _delta(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def _streak(cond: pd.Series) -> pd.Series:
    cond = cond.fillna(False).astype(bool)
    groups = cond.ne(cond.shift()).cumsum()
    return cond.groupby(groups).cumcount().add(1).where(cond, 0)


def prepare_daily(daily_path: Path, codes: set[str]) -> pd.DataFrame:
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(codes)].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily["decision_ymd"] = daily["date_dt"].dt.strftime("%Y%m%d").astype(int)
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    g = daily.groupby("code", group_keys=False)
    for n in (7, 20, 60):
        daily[f"ma{n}"] = g["close"].transform(lambda s, n=n: s.rolling(n, min_periods=n).mean())
        daily[f"ma{n}_slope"] = g[f"ma{n}"].transform(lambda s: s.diff(5) / s.shift(5))
        daily[f"dist_ma{n}_pct"] = daily["close"] / daily[f"ma{n}"] - 1
        daily[f"above{n}_streak"] = g.apply(lambda x, n=n: _streak(x["close"] > x[f"ma{n}"])).reset_index(level=0, drop=True)
    daily["upper_wick_ratio"] = (daily["high"] - daily[["open", "close"]].max(axis=1)) / (daily["high"] - daily["low"]).replace(0, pd.NA)
    daily["lower_wick_ratio"] = (daily[["open", "close"]].min(axis=1) - daily["low"]) / (daily["high"] - daily["low"]).replace(0, pd.NA)
    daily["large_bearish_candle"] = (daily["close"] < daily["open"]) & ((daily["open"] - daily["close"]) / daily["close"].replace(0, pd.NA) > 0.03)
    daily["failed_high_update"] = (daily["high"] >= g["high"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).max())) & (daily["close"] < daily["open"])
    daily["realized_vol20"] = g["close"].transform(lambda s: s.pct_change().rolling(20, min_periods=10).std())
    tr = pd.concat(
        [
            daily["high"] - daily["low"],
            (daily["high"] - g["close"].shift(1)).abs(),
            (daily["low"] - g["close"].shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    daily["atr14_pct"] = tr.groupby(daily["code"]).transform(lambda s: s.rolling(14, min_periods=7).mean()) / daily["close"]
    daily["close_below_ma7"] = daily["close"] < daily["ma7"]
    daily["close_below_ma20"] = daily["close"] < daily["ma20"]
    daily["ma7_slope_down"] = daily["ma7_slope"] < 0
    daily["ma20_slope_down"] = daily["ma20_slope"] < 0
    return daily


def future_path_for_row(row: pd.Series, daily_by_code: dict[str, pd.DataFrame]) -> dict[str, Any]:
    code = str(row["code"])
    ymd = int(row["decision_ymd"])
    d = daily_by_code.get(code)
    if d is None:
        return {"path_available": False}
    idxs = d.index[d["decision_ymd"] == ymd].tolist()
    if not idxs:
        return {"path_available": False}
    pos = d.index.get_loc(idxs[0])
    base_close = float(d.iloc[pos]["close"])
    out: dict[str, Any] = {"path_available": True, "decision_close": base_close}
    for h in (1, 3, 5, 10, 20):
        if pos + h < len(d):
            out[f"ret{h}" if h != 20 else "ret20_path"] = float(d.iloc[pos + h]["close"] / base_close - 1)
        win = d.iloc[pos + 1 : min(len(d), pos + h + 1)]
        if not win.empty:
            out[f"mae{h}" if h != 20 else "mae20_path"] = float(win["low"].min() / base_close - 1)
            out[f"mfe{h}" if h != 20 else "mfe20_path"] = float(win["high"].max() / base_close - 1)
    for delay in (1, 3, 5):
        if pos + delay < len(d) and pos + delay + 20 < len(d):
            entry = float(d.iloc[pos + delay]["close"])
            out[f"delayed_t{delay}_ret20"] = float(d.iloc[pos + delay + 20]["close"] / entry - 1)
            out[f"delayed_t{delay}_improves_ret20"] = out[f"delayed_t{delay}_ret20"] > row["ret20_num"]
            out[f"delayed_t{delay}_avoids_loser"] = out[f"delayed_t{delay}_ret20"] > -0.05
            fwin = d.iloc[pos + delay + 1 : min(len(d), pos + delay + 21)]
            if not fwin.empty:
                out[f"delayed_t{delay}_mae20"] = float(fwin["low"].min() / entry - 1)
    for ma, window in (("ma7", 10), ("ma20", 15)):
        event = None
        for off in range(1, window + 1):
            if pos + off >= len(d):
                break
            r = d.iloc[pos + off]
            if pd.notna(r[ma]) and r["low"] <= r[ma] <= r["high"]:
                event = off
                break
            if pd.notna(r[ma]) and r["close"] >= r[ma] and d.iloc[pos + off - 1]["close"] < d.iloc[pos + off - 1][ma]:
                event = off
                break
        out[f"{ma}_touch_reclaim_offset"] = event
        if event is not None and pos + event + 20 < len(d):
            entry = float(d.iloc[pos + event]["close"])
            out[f"{ma}_delayed_ret20"] = float(d.iloc[pos + event + 20]["close"] / entry - 1)
            out[f"{ma}_delayed_improves_ret20"] = out[f"{ma}_delayed_ret20"] > row["ret20_num"]
            out[f"{ma}_delayed_avoids_loser"] = out[f"{ma}_delayed_ret20"] > -0.05
    return out


def build_timing_rows(candidate_root: Path, daily_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = pd.read_csv(candidate_root / "candidate_rows_scored.csv", dtype={"code": str}, low_memory=False)
    rows["ret20_num"] = pd.to_numeric(rows["ret20_num"] if "ret20_num" in rows else rows["ret20"], errors="coerce")
    rows = rows[rows["year"].isin(YEARS) & rows["ret20_num"].notna() & (pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce") <= 10)].copy()
    rows["baseline_rank_recalc"] = pd.to_numeric(rows["baseline_rank_recalc"], errors="coerce")
    if "winner20" not in rows or "loser20" not in rows:
        rows["ret20_pct_rank_by_date"] = rows.groupby("decision_ymd")["ret20_num"].rank(pct=True, method="average")
        rows["winner20"] = (rows["ret20_num"] >= 0.05) | (rows["ret20_pct_rank_by_date"] >= 0.70)
        rows["loser20"] = (rows["ret20_num"] <= -0.05) | (rows["ret20_pct_rank_by_date"] <= 0.30)
    daily = prepare_daily(daily_path, set(rows["code"].astype(str)))
    feature_cols = ["code", "decision_ymd", *CONTEXT_FEATURES]
    rows = rows.merge(daily[[c for c in feature_cols if c in daily.columns]], on=["code", "decision_ymd"], how="left", suffixes=("", "_calc"))
    daily_by_code = {code: frame.reset_index(drop=True) for code, frame in daily.groupby("code", sort=False)}
    path_rows = [future_path_for_row(row, daily_by_code) for _, row in rows.iterrows()]
    out = pd.concat([rows.reset_index(drop=True), pd.DataFrame(path_rows)], axis=1)
    return out, {"selected_rows_loaded": int(len(out)), "path_coverage": float(out["path_available"].mean()), "daily_path": daily_path}


def timing_profile(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in YEARS:
        for topk in TOPK:
            selected = rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] <= topk)].copy()
            losers = selected[selected["loser20"].astype(bool)]
            out.append(
                {
                    "year": year,
                    "topk": topk,
                    "selected_loser_n": int(len(losers)),
                    "decline_ret1_rate": _rate(losers["ret1"] < 0),
                    "decline_ret3_rate": _rate(losers["ret3"] < 0),
                    "decline_ret5_rate": _rate(losers["ret5"] < 0),
                    "recover_after_mae5_rate": _rate((losers["mae5"] < -0.03) & (losers["mfe20_path"] > 0.03)),
                    "t3_delay_improve_rate": _rate(losers["delayed_t3_improves_ret20"]),
                    "t5_delay_improve_rate": _rate(losers["delayed_t5_improves_ret20"]),
                    "ma7_delay_improve_rate": _rate(losers["ma7_delayed_improves_ret20"]),
                    "ma20_delay_improve_rate": _rate(losers["ma20_delayed_improves_ret20"]),
                    "t3_delay_avg_improvement": _delta(_mean(losers, "delayed_t3_ret20"), _mean(losers, "ret20_num")),
                    "t5_delay_avg_improvement": _delta(_mean(losers, "delayed_t5_ret20"), _mean(losers, "ret20_num")),
                }
            )
    return pd.DataFrame(out)


def timing_contrast(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    features = [*PATH_FEATURES, "delayed_t3_ret20", "delayed_t5_ret20", "ma7_delayed_ret20", "ma20_delayed_ret20", *CONTEXT_FEATURES]
    for year in YEARS:
        for topk in TOPK:
            selected = rows[(rows["year"] == year) & (rows["baseline_rank_recalc"] <= topk)].copy()
            losers = selected[selected["loser20"].astype(bool)]
            winners = selected[selected["winner20"].astype(bool)]
            for f in features:
                if f not in rows:
                    continue
                lm, wm = _mean(losers, f), _mean(winners, f)
                out.append({"year": year, "topk": topk, "feature": f, "selected_loser_mean": lm, "selected_winner_mean": wm, "diff_loser_minus_winner": _delta(lm, wm), "loser_coverage": float(losers[f].notna().mean()) if len(losers) else None, "winner_coverage": float(winners[f].notna().mean()) if len(winners) else None})
    return pd.DataFrame(out)


def classify_failures(rows: pd.DataFrame) -> pd.DataFrame:
    losers = rows[rows["loser20"].astype(bool)].copy()
    late = (pd.to_numeric(losers.get("dist_ma20_pct"), errors="coerce") > 0.08) & ((losers.get("upper_wick_ratio", 0) > 0.25) | losers.get("failed_high_update", False).fillna(False) | (losers.get("ret3", 0) < 0))
    early = (losers.get("close_below_ma7", False).fillna(False) | losers.get("close_below_ma20", False).fillna(False) | losers.get("ma7_slope_down", False).fillna(False)) & (losers.get("delayed_t5_improves_ret20", False).fillna(False) | losers.get("ma7_delayed_improves_ret20", False).fillna(False))
    bad = ~(losers.get("delayed_t3_improves_ret20", False).fillna(False) | losers.get("delayed_t5_improves_ret20", False).fillna(False) | losers.get("ma7_delayed_improves_ret20", False).fillna(False) | losers.get("ma20_delayed_improves_ret20", False).fillna(False))
    labels = []
    for i in losers.index:
        if bool(late.loc[i]):
            labels.append("late_exhaustion_entry")
        elif bool(early.loc[i]):
            labels.append("early_pullback_entry")
        elif bool(bad.loc[i]):
            labels.append("true_bad_candidate")
        else:
            labels.append("noisy_unclear")
    losers["failure_type"] = labels
    return losers.groupby(["year", "failure_type"]).size().reset_index(name="n")


def delayed_summary(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for cohort, frame in (("selected_loser", rows[rows["loser20"].astype(bool)]), ("selected_winner", rows[rows["winner20"].astype(bool)])):
        for method in ("delayed_t3", "delayed_t5", "ma7_delayed", "ma20_delayed"):
            ret_col = f"{method}_ret20"
            improve_col = f"{method}_improves_ret20"
            avoid_col = f"{method}_avoids_loser"
            out.append({"cohort": cohort, "method": method, "n": int(len(frame)), "coverage": float(frame[ret_col].notna().mean()) if ret_col in frame else 0.0, "mean_ret20": _mean(frame, ret_col), "improve_rate": _rate(frame[improve_col]) if improve_col in frame else None, "avoid_loser_rate": _rate(frame[avoid_col]) if avoid_col in frame else None})
    return pd.DataFrame(out)


def decide(profile: pd.DataFrame, delayed: pd.DataFrame, failure: pd.DataFrame) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    loser_t5 = delayed[(delayed["cohort"] == "selected_loser") & (delayed["method"] == "delayed_t5")]
    winner_t5 = delayed[(delayed["cohort"] == "selected_winner") & (delayed["method"] == "delayed_t5")]
    loser_improve = float(loser_t5["improve_rate"].iloc[0] or 0) if not loser_t5.empty else 0.0
    winner_improve = float(winner_t5["improve_rate"].iloc[0] or 0) if not winner_t5.empty else 0.0
    ft = failure.groupby("failure_type")["n"].sum().sort_values(ascending=False)
    main_type = str(ft.index[0]) if not ft.empty else "noisy_unclear"
    axes = [
        {"axis_name": "t5_pullback_wait_diagnostic", "use": "entry_delay", "evidence": f"selected loser t5 improve rate {loser_improve:.2%}", "recommended_next": "pretest" if loser_improve - winner_improve > 0.10 else "hold"},
        {"axis_name": "ma7_touch_reclaim_wait_diagnostic", "use": "pullback_wait", "evidence": "diagnostic MA7 delayed entry improvement", "recommended_next": "hold"},
        {"axis_name": "late_exhaustion_entry_guard_diagnostic", "use": "exhaustion_veto", "evidence": f"main failure type {main_type}", "recommended_next": "pretest" if main_type == "late_exhaustion_entry" else "hold"},
    ]
    if loser_improve - winner_improve > 0.10:
        decision = "entry_timing_axis_found"
        reason = "selected losers improve more than selected winners under delayed entry diagnostic"
    elif main_type == "true_bad_candidate":
        decision = "true_bad_candidate_axis_needed"
        reason = "selected losers remain bad under delayed entries"
    else:
        decision = "no_clear_timing_signal"
        reason = "delayed entry does not clearly help selected losers more than winners"
    return {"research_decision": decision, "main_failure_type": main_type, "reason_typed": [reason], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}, axes


def run(*, candidate_root: Path = DEFAULT_CANDIDATE_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-recent-topk-selected-loser-timing-decomposition-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    source_audit = json.loads((candidate_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    rows, report = build_timing_rows(candidate_root, daily_path)
    profile = timing_profile(rows)
    contrast = timing_contrast(rows)
    delayed = delayed_summary(rows)
    failure = classify_failures(rows)
    decision, axes = decide(profile, delayed, failure)
    profile.to_csv(run_dir / "selected_loser_timing_profile.csv", index=False)
    contrast.to_csv(run_dir / "loser_vs_winner_timing_contrast.csv", index=False)
    delayed.to_csv(run_dir / "delayed_entry_diagnostic_summary.csv", index=False)
    failure.to_csv(run_dir / "failure_type_classification.csv", index=False)
    _write_json(run_dir / "next_axis_candidates.json", {"candidates": axes, "not_policies": True})
    _write_json(run_dir / "input_artifact_report.json", {"candidate_root": candidate_root, "daily_path": daily_path, "source_no_lookahead_audit": source_audit.get("audit_result"), **report})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "source_audit_result": source_audit.get("audit_result"), "decision_date_features_use_past_only": True, "future_path_is_diagnostic_label_only": True, "no_entry_policy_created": True, "threshold_sweep": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidate-root", type=Path, default=DEFAULT_CANDIDATE_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(candidate_root=args.candidate_root, daily_path=args.daily_path, output_root=args.output_root)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
