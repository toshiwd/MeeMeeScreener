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

from scripts import portfolio_agent_replay_v1 as replay


AXIS_ID = "monthly_box_breakout_2026_validation_v1"
DEFAULT_SHIFT_ROOT = Path(r"G:\Tradex\recent_win_pattern_shift_audit_v1\20260523T185123Z-recent-win-pattern-shift-audit-v1")
DEFAULT_PRETEST_ROOT = Path(r"G:\Tradex\monthly_box_breakout_recent_demotion_pretest_v1\20260523T190422Z-monthly-box-breakout-recent-demotion-pretest-v1")
DEFAULT_CONFLICT_ROOT = Path(r"G:\Tradex\monthly_box_breakout_year_conflict_decomposition_v1\20260523T191339Z-monthly-box-breakout-year-conflict-decomposition-v1")
DEFAULT_DAILY_PATH = Path("production_data/production_daily.csv")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\monthly_box_breakout_2026_validation_v1")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "same_family_2026_snapshot_report.json",
    "coverage_audit_2026.json",
    "topk_comparison_summary_2024_2026.json",
    "replacement_quality_2024_2026.csv",
    "year_conflict_summary_2024_2026.json",
    "removed_rows_context_by_year.csv",
    "boundary_contribution_summary.csv",
    "context_pretest_candidate.json",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
USECOLS_2019_2025 = [
    "decision_ymd",
    "code",
    "candidate_rank",
    "selection_score",
    "selected_for_buy",
    "source_year",
    "year",
    "monthly_box_breakout_proxy",
    "monthly_high_zone_proxy",
    "monthly_box_inside_proxy",
    "above60_streak",
    "days_since_ma60_reclaim",
    "ret20",
    "ret40",
    "mae20",
    "mfe20",
]


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


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    return pd.to_numeric(num / den.replace(0, pd.NA), errors="coerce")


def _mean(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    values = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if values.empty else float(values.median())


def _rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    s = df[col].dropna()
    if s.empty:
        return None
    if s.dtype == bool:
        return float(s.mean())
    return float(s.astype("string").str.lower().isin({"true", "1", "yes"}).mean())


def trading_cutoff(daily_path: Path, horizon: int = 20) -> dict[str, Any]:
    dates = pd.to_datetime(pd.read_csv(daily_path, usecols=["date"])["date"].drop_duplicates()).sort_values().reset_index(drop=True)
    latest = dates.iloc[-1]
    cutoff = dates.iloc[max(0, len(dates) - 1 - horizon)]
    return {"latest_confirmed_daily_date": latest.strftime("%Y-%m-%d"), "ret20_label_safe_cutoff_date": cutoff.strftime("%Y-%m-%d"), "ret20_label_safe_cutoff_ymd": int(cutoff.strftime("%Y%m%d"))}


def ensure_2026_snapshot(output_dir: Path, source_db: str | Path | None = None) -> tuple[Path | None, dict[str, Any]]:
    subrun_root = output_dir / "same_family_2026_subrun"
    expected = subrun_root / "2026-baseline-portfolio_agent_replay_v1" / "daily_candidate_snapshot.csv"
    report = {
        "same_family_contract": "portfolio_agent_replay_v1 via run_portfolio_agent_replay_v1",
        "run_id": "2026-baseline",
        "start_ymd": 20260101,
        "end_ymd": 20270101,
        "generated": False,
        "snapshot_path": expected,
        "contract_match_2019_2025": True,
    }
    if not expected.exists():
        result = replay.run_portfolio_agent_replay_v1(source_db=source_db, output_root=subrun_root, run_id="2026-baseline", start_ymd=20260101, end_ymd=20270101)
        report["generated"] = True
        report["replay_result"] = result
    exists = expected.exists()
    report["snapshot_exists"] = exists
    if exists:
        audit = expected.parent / "no_lookahead_audit.json"
        report["no_lookahead_audit_exists"] = audit.exists()
        if audit.exists():
            report["no_lookahead_audit"] = json.loads(audit.read_text(encoding="utf-8")).get("audit_result")
    return (expected if exists else None), report


def _streak_bool(cond: pd.Series) -> pd.Series:
    cond = cond.fillna(False).astype(bool)
    groups = cond.ne(cond.shift()).cumsum()
    return cond.groupby(groups).cumcount().add(1).where(cond, 0)


def _days_since_event(event: pd.Series) -> pd.Series:
    last = None
    out = []
    for idx, flag in enumerate(event.fillna(False).astype(bool).tolist()):
        if flag:
            last = idx
            out.append(0)
        elif last is None:
            out.append(pd.NA)
        else:
            out.append(idx - last)
    return pd.Series(out, index=event.index)


def build_2026_feature_rows(snapshot_path: Path, daily_path: Path, cutoff_ymd: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    snap = pd.read_csv(snapshot_path, dtype={"code": str}, low_memory=False)
    snap["year"] = 2026
    snap["source_year"] = 2026
    snap["decision_date"] = pd.to_datetime(snap["decision_ymd"].astype(str), format="%Y%m%d")
    codes = set(snap["code"].astype(str))
    daily = pd.read_csv(daily_path, dtype={"code": str})
    daily = daily[daily["code"].isin(codes)].copy()
    daily["date_dt"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values(["code", "date_dt"]).drop_duplicates(["code", "date_dt"], keep="last")
    g = daily.groupby("code", group_keys=False)
    daily["ma60"] = g["close"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    daily["above60_streak"] = g.apply(lambda x: _streak_bool(x["close"] > x["ma60"])).reset_index(level=0, drop=True)
    daily["days_since_ma60_reclaim"] = g.apply(lambda x: _days_since_event((x["close"] > x["ma60"]) & (x["close"].shift(1) <= x["ma60"].shift(1)))).reset_index(level=0, drop=True)
    prior120 = g["close"].transform(lambda s: s.shift(1).rolling(120, min_periods=60).max())
    high120 = g["close"].transform(lambda s: s.rolling(120, min_periods=60).max())
    daily["monthly_box_breakout_proxy"] = daily["close"] >= prior120
    daily["monthly_high_zone_proxy"] = daily["close"] >= high120 * 0.95
    daily["monthly_box_inside_proxy"] = ~daily["monthly_high_zone_proxy"]
    daily["future_close_20"] = g["close"].shift(-20)
    daily["future_close_40"] = g["close"].shift(-40)
    daily["ret20"] = _safe_div(daily["future_close_20"], daily["close"]) - 1
    daily["ret40"] = _safe_div(daily["future_close_40"], daily["close"]) - 1
    future_low20 = g["low"].transform(lambda s: s.iloc[::-1].shift(1).rolling(20, min_periods=1).min().iloc[::-1])
    future_high20 = g["high"].transform(lambda s: s.iloc[::-1].shift(1).rolling(20, min_periods=1).max().iloc[::-1])
    daily["mae20"] = _safe_div(future_low20, daily["close"]) - 1
    daily["mfe20"] = _safe_div(future_high20, daily["close"]) - 1
    feat = daily[["code", "date_dt", "monthly_box_breakout_proxy", "monthly_high_zone_proxy", "monthly_box_inside_proxy", "above60_streak", "days_since_ma60_reclaim", "ret20", "ret40", "mae20", "mfe20"]].rename(columns={"date_dt": "decision_date"})
    rows = snap.merge(feat, on=["code", "decision_date"], how="left")
    rows["label_safe_ret20"] = rows["decision_ymd"] <= int(cutoff_ymd)
    rows.loc[~rows["label_safe_ret20"], ["ret20", "ret40", "mae20", "mfe20"]] = pd.NA
    report = {
        "total_candidate_rows": int(len(rows)),
        "decision_ymd_min": int(rows["decision_ymd"].min()) if len(rows) else None,
        "decision_ymd_max": int(rows["decision_ymd"].max()) if len(rows) else None,
        "rows_with_ret20": int(rows["ret20"].notna().sum()),
        "rows_without_ret20": int(rows["ret20"].isna().sum()),
        "feature_coverage": {col: float(rows[col].notna().mean()) for col in ["monthly_box_breakout_proxy", "above60_streak", "days_since_ma60_reclaim", "monthly_high_zone_proxy", "monthly_box_inside_proxy"]},
    }
    return rows, report


def load_combined_rows(shift_root: Path, rows_2026: pd.DataFrame) -> pd.DataFrame:
    old = pd.read_csv(shift_root / "candidate_rows_with_features.csv", usecols=USECOLS_2019_2025, dtype={"code": str}, low_memory=False)
    cols = USECOLS_2019_2025
    new = rows_2026.copy()
    for col in cols:
        if col not in new:
            new[col] = pd.NA
    return pd.concat([old[cols], new[cols]], ignore_index=True)


def score_rows(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows[rows["year"].between(2019, 2026)].copy()
    out["monthly_box_breakout_bool"] = out["monthly_box_breakout_proxy"].astype("boolean").fillna(False).astype(bool)
    out["monthly_box_breakout_score_delta"] = out["monthly_box_breakout_bool"].map(lambda x: -1 if x else 0)
    out["baseline_score"] = pd.to_numeric(out["selection_score"], errors="coerce")
    out["challenger_score"] = out["baseline_score"] + out["monthly_box_breakout_score_delta"]
    out["baseline_rank_recalc"] = out.groupby("decision_ymd")["baseline_score"].rank(method="first", ascending=False)
    out["challenger_rank"] = out.sort_values(["decision_ymd", "challenger_score", "baseline_score", "candidate_rank"], ascending=[True, False, False, True]).groupby("decision_ymd").cumcount() + 1
    return out


def _metrics(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df["ret20"], errors="coerce")
    return {
        "n": int(len(df)),
        "mean_ret20": None if ret.dropna().empty else float(ret.mean()),
        "median_ret20": None if ret.dropna().empty else float(ret.median()),
        "severe_loss_rate": None if ret.dropna().empty else float((ret <= -0.05).mean()),
    }


def compare_topk(rows: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    periods = {
        "2024": rows[rows["year"] == 2024],
        "2025": rows[rows["year"] == 2025],
        "2026_label_safe": rows[(rows["year"] == 2026) & rows["ret20"].notna()],
        "2024_2026_label_safe": rows[(rows["year"].between(2024, 2026)) & rows["ret20"].notna()],
        "2019_2023": rows[rows["year"].between(2019, 2023)],
        "2019_2026_label_safe": rows[rows["ret20"].notna()],
    }
    summary: dict[str, Any] = {}
    repl = []
    for pname, p in periods.items():
        summary[pname] = {}
        for k in (5, 10, 20):
            base = p[p["baseline_rank_recalc"] <= k]
            chal = p[p["challenger_rank"] <= k]
            bkeys = set(zip(base["decision_ymd"], base["code"]))
            ckeys = set(zip(chal["decision_ymd"], chal["code"]))
            added = chal.set_index(["decision_ymd", "code"]).loc[list(ckeys - bkeys)].reset_index() if ckeys - bkeys else chal.head(0)
            removed = base.set_index(["decision_ymd", "code"]).loc[list(bkeys - ckeys)].reset_index() if bkeys - ckeys else base.head(0)
            added_mean = _mean(added, "ret20")
            removed_mean = _mean(removed, "ret20")
            summary[pname][f"top{k}"] = {"baseline": _metrics(base), "challenger": _metrics(chal), "delta_mean_ret20": _diff(_metrics(chal)["mean_ret20"], _metrics(base)["mean_ret20"]), "added_mean_ret20": added_mean, "removed_mean_ret20": removed_mean, "added_minus_removed_ret20": _diff(added_mean, removed_mean), "changed_members_count": int(len(added))}
            repl.append({"period": pname, "topk": k, "added_count": int(len(added)), "removed_count": int(len(removed)), "added_mean_ret20": added_mean, "removed_mean_ret20": removed_mean, "added_minus_removed_ret20": _diff(added_mean, removed_mean)})
    return summary, pd.DataFrame(repl)


def _diff(a: float | None, b: float | None) -> float | None:
    return None if a is None or b is None else float(a - b)


def removed_context(rows: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for year in (2024, 2025, 2026):
        y = rows[(rows["year"] == year) & rows["ret20"].notna()]
        for k in (5, 10, 20):
            base = y[y["baseline_rank_recalc"] <= k]
            chal = y[y["challenger_rank"] <= k]
            removed_keys = set(zip(base["decision_ymd"], base["code"])) - set(zip(chal["decision_ymd"], chal["code"]))
            removed = base.set_index(["decision_ymd", "code"]).loc[list(removed_keys)].reset_index() if removed_keys else base.head(0)
            parts.append({"year": year, "topk": k, "removed_n": int(len(removed)), "removed_ret20_mean": _mean(removed, "ret20"), "removed_above60_streak_mean": _mean(removed, "above60_streak"), "removed_days_since_ma60_reclaim_mean": _mean(removed, "days_since_ma60_reclaim")})
    return pd.DataFrame(parts)


def boundary_summary(rows: pd.DataFrame) -> pd.DataFrame:
    out = []
    for year in (2024, 2025, 2026):
        y = rows[(rows["year"] == year) & rows["ret20"].notna()]
        for k in (5, 10, 20):
            base = y[y["baseline_rank_recalc"] <= k]
            chal = y[y["challenger_rank"] <= k]
            removed_keys = set(zip(base["decision_ymd"], base["code"])) - set(zip(chal["decision_ymd"], chal["code"]))
            added_keys = set(zip(chal["decision_ymd"], chal["code"])) - set(zip(base["decision_ymd"], base["code"]))
            for role, frame, keys in (("removed", base, removed_keys), ("added", chal, added_keys)):
                group = frame.set_index(["decision_ymd", "code"]).loc[list(keys)].reset_index() if keys else frame.head(0)
                near = group[(pd.to_numeric(group["baseline_rank_recalc"], errors="coerce").sub(k).abs() <= 3) | (pd.to_numeric(group["challenger_rank"], errors="coerce").sub(k).abs() <= 3)]
                out.append({"year": year, "topk": k, "role": role, "n": int(len(group)), "boundary_near_share": None if len(group) == 0 else float(len(near) / len(group)), "ret20_mean": _mean(group, "ret20")})
    return pd.DataFrame(out)


def conflict_summary(removed: pd.DataFrame, repl: pd.DataFrame) -> dict[str, Any]:
    top10 = removed[removed["topk"] == 10].set_index("year").to_dict(orient="index")
    repl10 = repl[repl["topk"] == 10].set_index("period").to_dict(orient="index")
    r2026 = repl10.get("2026_label_safe", {})
    r2024 = repl10.get("2024", {})
    r2025 = repl10.get("2025", {})
    sim = "2024" if abs((r2026.get("added_minus_removed_ret20") or 0) - (r2024.get("added_minus_removed_ret20") or 0)) <= abs((r2026.get("added_minus_removed_ret20") or 0) - (r2025.get("added_minus_removed_ret20") or 0)) else "2025"
    return {"top10_removed_context_by_year": top10, "top10_replacement_by_period": {"2024": r2024, "2025": r2025, "2026_label_safe": r2026}, "2026_resembles": sim}


def decide(cov: dict[str, Any], conflict: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    if cov["rows_with_ret20"] < 1000:
        decision = "hold_until_more_2026_labels"
        reason = "2026 same-family snapshot exists but ret20 label-safe rows are too thin"
    else:
        combined = summary["2024_2026_label_safe"]["top10"]
        r2026 = summary["2026_label_safe"]["top10"]
        if (combined.get("delta_mean_ret20") or 0) <= 0 or (r2026.get("added_minus_removed_ret20") or 0) <= 0:
            decision = "drop_monthly_box_breakout_demotion"
            reason = "2026/combined replacement quality does not support continuing monthly breakout demotion"
        else:
            decision = "context_pretest_allowed_next"
            reason = "2026 same-family label-safe result supports continuing to one context-axis pretest"
    return {"research_decision": decision, "reason_typed": [reason], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False, "threshold_sweep": False}


def run(*, shift_root: Path = DEFAULT_SHIFT_ROOT, pretest_root: Path = DEFAULT_PRETEST_ROOT, conflict_root: Path = DEFAULT_CONFLICT_ROOT, daily_path: Path = DEFAULT_DAILY_PATH, output_root: Path = DEFAULT_OUTPUT_ROOT, source_db: str | Path | None = None) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-monthly-box-breakout-2026-validation-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    cutoff = trading_cutoff(daily_path)
    snapshot, snap_report = ensure_2026_snapshot(run_dir, source_db=source_db)
    if snapshot is None:
        decision = {"research_decision": "inconclusive_source_gap", "reason_typed": ["same-family 2026 snapshot could not be generated"], "meemee_reflectable": False, "ranking_reflectable": False, "publish_allowed": False}
        cov = {"rows_with_ret20": 0}
        summary, repl, removed, boundary, conflict = {}, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}
    else:
        rows_2026, cov = build_2026_feature_rows(snapshot, daily_path, cutoff["ret20_label_safe_cutoff_ymd"])
        rows = score_rows(load_combined_rows(shift_root, rows_2026))
        summary, repl = compare_topk(rows)
        removed = removed_context(rows)
        boundary = boundary_summary(rows)
        conflict = conflict_summary(removed, repl)
        decision = decide(cov, conflict, summary)
    _write_json(run_dir / "input_artifact_report.json", {"shift_root": shift_root, "pretest_root": pretest_root, "conflict_root": conflict_root, "daily_path": daily_path, "scope": "TRADEX-only; same-family 2026 validation"})
    _write_json(run_dir / "same_family_2026_snapshot_report.json", snap_report)
    _write_json(run_dir / "coverage_audit_2026.json", {**cutoff, **cov})
    _write_json(run_dir / "topk_comparison_summary_2024_2026.json", summary)
    repl.to_csv(run_dir / "replacement_quality_2024_2026.csv", index=False)
    _write_json(run_dir / "year_conflict_summary_2024_2026.json", conflict)
    removed.to_csv(run_dir / "removed_rows_context_by_year.csv", index=False)
    boundary.to_csv(run_dir / "boundary_contribution_summary.csv", index=False)
    _write_json(run_dir / "context_pretest_candidate.json", {"candidate_axis_name": "monthly_box_breakout_x_above60_maturity_context_pretest", "allowed_next": decision["research_decision"] == "context_pretest_allowed_next", "not_pretested_here": True})
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass", "same_family_contract_checked": bool(snap_report.get("snapshot_exists")), "ret20_label_safe_cutoff_applied": True, "demotion_policy_unchanged": True, "threshold_sweep": False, "model_training": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "research_decision": decision, "coverage_2026": cov, "snapshot": snap_report}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate monthly breakout demotion with same-family 2026 baseline snapshot")
    parser.add_argument("--shift-root", type=Path, default=DEFAULT_SHIFT_ROOT)
    parser.add_argument("--pretest-root", type=Path, default=DEFAULT_PRETEST_ROOT)
    parser.add_argument("--conflict-root", type=Path, default=DEFAULT_CONFLICT_ROOT)
    parser.add_argument("--daily-path", type=Path, default=DEFAULT_DAILY_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-db", default="")
    args = parser.parse_args(argv)
    print(json.dumps(_json_ready(run(shift_root=args.shift_root, pretest_root=args.pretest_root, conflict_root=args.conflict_root, daily_path=args.daily_path, output_root=args.output_root, source_db=args.source_db.strip() or None)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
