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

from scripts.tradex_ma60_above_60plus_pattern_audit_v1 import DEFAULT_PRODUCTION_CSV, add_features, load_daily_frame, resolve_input


AXIS_ID = "ma60_above_60plus_guard_overlay_replay_v1"
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\ma60_above_60plus_stay_guard_pretest_v1\20260523T131433Z-ma60-above-60plus-stay-guard-pretest-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_guard_overlay_replay_v1")
PORTFOLIO_REPLAY_ROOT = Path(r"G:\Tradex\portfolio_agent_replay_v1")
ACTUAL_CONTEXT_ROOT = Path(r"G:\Tradex\decision_context_reconstruction_v1")
REQUIRED_GUARD_INPUTS = ("selected_guard_rules.json", "guard_hit_rows.csv", "guard_vs_baseline_summary.json", "stay_simulation_summary.json", "no_lookahead_audit.json")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "cohort_availability_report.json",
    "policy_a_stay_guard_exit_delay_summary.json",
    "policy_b_reduce_suppression_summary.json",
    "policy_c_short_veto_summary.json",
    "overlay_hit_rows.csv",
    "overlay_event_ledger.jsonl",
    "period_stability_summary.csv",
    "regime_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
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
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
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


def discover_inputs() -> dict[str, list[Path]]:
    portfolio_runs = sorted([p for p in PORTFOLIO_REPLAY_ROOT.glob("*") if p.is_dir()], key=lambda p: p.name, reverse=True) if PORTFOLIO_REPLAY_ROOT.exists() else []
    actual_runs = sorted([p for p in ACTUAL_CONTEXT_ROOT.glob("*") if p.is_dir()], key=lambda p: p.name, reverse=True) if ACTUAL_CONTEXT_ROOT.exists() else []
    out = {"portfolio_runs": [], "actual_context_runs": []}
    for run in portfolio_runs:
        if (run / "daily_candidate_snapshot.csv").exists() or (run / "orders_ledger.csv").exists():
            out["portfolio_runs"].append(run)
    for run in actual_runs:
        if (run / "actual_trade_decision_context.csv").exists():
            out["actual_context_runs"].append(run)
    return out


def _load_csvs(paths: list[Path], name: str) -> pd.DataFrame:
    frames = []
    for root in paths:
        p = root / name
        if p.exists():
            df = pd.read_csv(p)
            df["source_artifact"] = str(p)
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_cohorts(discovered: dict[str, list[Path]]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    candidates = _load_csvs(discovered["portfolio_runs"][:8], "daily_candidate_snapshot.csv")
    orders = _load_csvs(discovered["portfolio_runs"][:8], "orders_ledger.csv")
    actual = _load_csvs(discovered["actual_context_runs"][:4], "actual_trade_decision_context.csv")
    long_candidates = candidates[candidates.get("selected_for_buy", False).astype(bool)].copy() if not candidates.empty and "selected_for_buy" in candidates else pd.DataFrame()
    reduce_exit = orders[orders.get("action", "").astype(str).str.lower().isin(["sell", "exit", "reduce"])].copy() if not orders.empty and "action" in orders else pd.DataFrame()
    shorts = actual[actual.get("side", "").astype(str).str.lower().str.contains("short|sell")].copy() if not actual.empty and "side" in actual else pd.DataFrame()
    report = {
        "long_candidate_rows": {"available": not long_candidates.empty, "row_count": int(len(long_candidates)), "source_count": len(discovered["portfolio_runs"][:8])},
        "reduce_or_exit_decision_rows": {"available": not reduce_exit.empty, "row_count": int(len(reduce_exit)), "source_count": len(discovered["portfolio_runs"][:8])},
        "short_or_sell_candidate_rows": {"available": not shorts.empty, "row_count": int(len(shorts)), "source_count": len(discovered["actual_context_runs"][:4])},
        "actual_held_or_bought_rows": {"available": not orders.empty, "row_count": int(len(orders)), "source_count": len(discovered["portfolio_runs"][:8])},
    }
    return {"long_candidates": long_candidates, "reduce_exit": reduce_exit, "shorts": shorts}, report


def _date_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    text = str(value)
    if text.endswith(".0"):
        text = text[:-2]
    try:
        if "-" in text:
            return int(pd.Timestamp(text).strftime("%Y%m%d"))
        return int(text)
    except Exception:
        return None


def prepare_guard_windows(guard_rows: pd.DataFrame, featured_daily: pd.DataFrame) -> pd.DataFrame:
    dates_by_code = {code: group["ymd"].tolist() for code, group in featured_daily.sort_values(["code", "ymd"]).groupby("code")}
    rows = []
    for row in guard_rows.to_dict("records"):
        code = str(row["code"])
        anchor = _date_int(row["anchor_date"])
        if anchor is None or code not in dates_by_code:
            continue
        dates = dates_by_code[code]
        future = [d for d in dates if d >= anchor]
        if not future:
            continue
        rows.append({"code": code, "anchor_type": row["anchor_type"], "anchor_ymd": anchor, "guard_active_until_ymd": future[min(20, len(future)-1)], "guard_rule_ids": row.get("guard_rule_ids", "")})
    return pd.DataFrame(rows)


def attach_guard(rows: pd.DataFrame, windows: pd.DataFrame, *, date_col: str, code_col: str = "code") -> pd.DataFrame:
    if rows.empty or windows.empty or date_col not in rows or code_col not in rows:
        return pd.DataFrame()
    out = rows.copy()
    out["_code"] = out[code_col].astype(str)
    out["_ymd"] = out[date_col].map(_date_int)
    hits = []
    by_code = {c: g for c, g in windows.groupby("code")}
    for idx, r in out.iterrows():
        g = by_code.get(str(r["_code"]))
        if g is None or r["_ymd"] is None:
            continue
        active = g[(g["anchor_ymd"] <= int(r["_ymd"])) & (g["guard_active_until_ymd"] >= int(r["_ymd"]))]
        if active.empty:
            continue
        best = active.iloc[-1]
        item = r.to_dict()
        item["guard_hit"] = True
        item["guard_anchor_type"] = best["anchor_type"]
        item["guard_anchor_ymd"] = int(best["anchor_ymd"])
        item["guard_rule_ids"] = best.get("guard_rule_ids", "")
        hits.append(item)
    return pd.DataFrame(hits)


def _path(featured: pd.DataFrame, code: str, ymd: int, days: int = 40) -> pd.DataFrame:
    g = featured[(featured["code"].astype(str) == str(code)) & (featured["ymd"] >= int(ymd))].sort_values("ymd").head(days + 1)
    return g.copy()


def _overlay_long_metrics(row: dict[str, Any], featured: pd.DataFrame, *, decision_col: str, baseline_return_col: str | None = None) -> dict[str, Any]:
    code = str(row.get("code") or row.get("symbol"))
    ymd = _date_int(row.get(decision_col))
    p = _path(featured, code, ymd or 0, 40)
    if p.empty:
        return {}
    entry = float(p.iloc[0]["c"])
    exit_row = p.iloc[min(20, len(p)-1)]
    exit_reason = "max_days"
    for _, x in p.iloc[1:21].iterrows():
        close = float(x["c"])
        if pd.notna(x.get("ma20")) and close <= float(x["ma20"]):
            exit_row = x; exit_reason = "ma20_break"; break
        if pd.notna(x.get("ma60")) and close <= float(x["ma60"]):
            exit_row = x; exit_reason = "ma60_break"; break
        if len(p) > 1 and pd.notna(x.get("volume_ratio_ma20")) and float(x.get("volume_ratio_ma20") or 0) >= 1.5:
            prev = p[p["ymd"] < x["ymd"]].tail(1)
            if not prev.empty and close / float(prev.iloc[0]["c"]) - 1 <= -0.05:
                exit_row = x; exit_reason = "bearish_break"; break
    overlay_ret = float(exit_row["c"] / entry - 1.0) if entry > 0 else None
    baseline_ret = _safe_float(row.get(baseline_return_col)) if baseline_return_col else 0.0
    lows, highs = p.iloc[1:21]["l"], p.iloc[1:21]["h"]
    return {
        "overlay_return": overlay_ret,
        "baseline_return": baseline_ret,
        "delta_return": None if overlay_ret is None or baseline_ret is None else overlay_ret - baseline_ret,
        "ret20_from_decision": _hret(p, entry, 20),
        "ret40_from_decision": _hret(p, entry, 40),
        "mae_extension": None if lows.empty else float(lows.min() / entry - 1),
        "mfe_extension": None if highs.empty else float(highs.max() / entry - 1),
        "extension_exit_reason": exit_reason,
        "helped_delay": overlay_ret is not None and baseline_ret is not None and overlay_ret > baseline_ret,
        "harmful_delay": overlay_ret is not None and baseline_ret is not None and overlay_ret < baseline_ret,
    }


def _hret(p: pd.DataFrame, entry: float, h: int) -> float | None:
    if len(p) <= h or entry <= 0:
        return None
    return float(p.iloc[h]["c"] / entry - 1)


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
    except Exception:
        return None
    return f if math.isfinite(f) else None


def _summ(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty:
        return {"available": False, "n_overlay_hits": 0}
    return {
        "available": True,
        "n_overlay_hits": int(len(rows)),
        "baseline_return_mean": _mean(rows, "baseline_return"),
        "overlay_return_mean": _mean(rows, "overlay_return"),
        "delta_return_mean": _mean(rows, "delta_return"),
        "delta_return_median": _median(rows, "delta_return"),
        "ret20_from_decision_mean": _mean(rows, "ret20_from_decision"),
        "ret40_from_decision_mean": _mean(rows, "ret40_from_decision"),
        "mae_extension_mean": _mean(rows, "mae_extension"),
        "mfe_extension_mean": _mean(rows, "mfe_extension"),
        "helped_delay_count": int(rows.get("helped_delay", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "harmful_delay_count": int(rows.get("harmful_delay", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "helped_delay_rate": _bool_rate(rows, "helped_delay"),
        "harmful_delay_rate": _bool_rate(rows, "harmful_delay"),
        "extension_exit_reason_counts": rows.get("extension_exit_reason", pd.Series(dtype=str)).value_counts(dropna=False).to_dict(),
    }


def _mean(df: pd.DataFrame, c: str) -> float | None:
    s = pd.to_numeric(df[c], errors="coerce").dropna() if c in df else pd.Series(dtype=float)
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, c: str) -> float | None:
    s = pd.to_numeric(df[c], errors="coerce").dropna() if c in df else pd.Series(dtype=float)
    return None if s.empty else float(s.median())


def _bool_rate(df: pd.DataFrame, c: str) -> float | None:
    if c not in df or df.empty:
        return None
    return float(df[c].fillna(False).astype(bool).mean())


def policy_c_metrics(short_hits: pd.DataFrame, featured: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for r in short_hits.to_dict("records"):
        code = str(r.get("symbol") or r.get("code"))
        ymd = _date_int(r.get("entry_date") or r.get("entry_bar_date_used"))
        p = _path(featured, code, ymd or 0, 40)
        if p.empty:
            continue
        entry = float(p.iloc[0]["c"])
        ret20 = _hret(p, entry, 20)
        ret40 = _hret(p, entry, 40)
        short20 = None if ret20 is None else -ret20
        short40 = None if ret40 is None else -ret40
        rows.append({**r, "policy": "C_short_veto", "ret20_after_veto": ret20, "ret40_after_veto": ret40, "short_baseline_forward_return_20": short20, "short_baseline_forward_return_40": short40, "avoided_loss": ret20 is not None and ret20 > 0, "missed_gain": ret20 is not None and ret20 < 0})
    return pd.DataFrame(rows)


def _short_summary(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"available": False, "n_short_veto_hits": 0}
    return {"available": True, "n_short_veto_hits": int(len(df)), "short_baseline_forward_return_20_mean": _mean(df, "short_baseline_forward_return_20"), "ret20_after_veto_mean": _mean(df, "ret20_after_veto"), "ret40_after_veto_mean": _mean(df, "ret40_after_veto"), "short_veto_help_rate": _bool_rate(df, "avoided_loss"), "short_veto_harm_rate": _bool_rate(df, "missed_gain")}


def classify(a: dict[str, Any], b: dict[str, Any], c: dict[str, Any]) -> dict[str, Any]:
    best = []
    for name, s in [("stay_guard", a), ("reduce_suppression", b)]:
        mean_delta = s.get("delta_return_mean")
        median_delta = s.get("delta_return_median")
        harmful = s.get("harmful_delay_rate")
        helped = s.get("helped_delay_rate")
        if (
            s.get("n_overlay_hits", 0) >= 100
            and mean_delta is not None
            and mean_delta > 0
            and median_delta is not None
            and median_delta >= 0
            and harmful is not None
            and helped is not None
            and harmful <= helped
        ):
            best.append(name)
    if c.get("n_short_veto_hits", 0) >= 100 and (c.get("short_veto_help_rate") or 0) >= (c.get("short_veto_harm_rate") or 1):
        best.append("short_veto")
    if best:
        return {"research_decision": "overlay_supported", "best_supported_use": best, "reason_typed": ["at least one overlay cohort passed sample, return, and harm/help gates"], "no_lookahead_safe": True}
    if any((s.get("n_overlay_hits", 0) > 0) for s in [a, b, c]):
        return {"research_decision": "weak_overlay", "best_supported_use": [], "reason_typed": ["overlay hits exist but support gates are incomplete"], "no_lookahead_safe": True}
    return {"research_decision": "inconclusive", "best_supported_use": [], "reason_typed": ["no usable overlay hits after matching guard windows to decision rows"], "no_lookahead_safe": True}


def run(*, guard_root: Path = DEFAULT_GUARD_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT, production_csv: Path = DEFAULT_PRODUCTION_CSV) -> dict[str, Any]:
    run_dir = output_root / f"{_now_tag()}-ma60-above-60plus-guard-overlay-replay-v1"
    run_dir.mkdir(parents=True, exist_ok=True)
    missing = [n for n in REQUIRED_GUARD_INPUTS if not (guard_root / n).exists()]
    guard = pd.read_csv(guard_root / "guard_hit_rows.csv")
    guard_audit = json.loads((guard_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    discovered = discover_inputs()
    cohorts, avail = load_cohorts(discovered)
    min_ymd = min(_date_int(x) for x in guard["anchor_date"].dropna())
    max_ymd = 20261231
    res = resolve_input(production_csv=production_csv)
    daily = load_daily_frame(res, start_ymd=min_ymd - 10000, end_ymd=max_ymd)
    featured = add_features(daily)
    windows = prepare_guard_windows(guard, featured)
    long_hits = attach_guard(cohorts["long_candidates"], windows, date_col="decision_ymd")
    exit_hits = attach_guard(cohorts["reduce_exit"], windows, date_col="decision_ymd")
    short_hits = attach_guard(cohorts["shorts"], windows, date_col="entry_date", code_col="symbol")
    a_rows = pd.DataFrame([{**r, "policy": "A_stay_guard_exit_delay", **_overlay_long_metrics(r, featured, decision_col="decision_ymd", baseline_return_col="post_ret_20")} for r in long_hits.to_dict("records")])
    b_rows = pd.DataFrame([{**r, "policy": "B_reduce_suppression_guard", **_overlay_long_metrics(r, featured, decision_col="decision_ymd", baseline_return_col="realized_return")} for r in exit_hits.to_dict("records")])
    c_rows = policy_c_metrics(short_hits, featured)
    a_sum, b_sum, c_sum = _summ(a_rows), _summ(b_rows), _short_summary(c_rows)
    decision = classify(a_sum, b_sum, c_sum)
    overlay = pd.concat([a_rows, b_rows, c_rows], ignore_index=True, sort=False)
    overlay.to_csv(run_dir / "overlay_hit_rows.csv", index=False)
    with (run_dir / "overlay_event_ledger.jsonl").open("w", encoding="utf-8") as fh:
        for row in overlay.to_dict("records"):
            fh.write(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n")
    period = overlay.assign(period=lambda d: d.get("_ymd", pd.Series(dtype=float)).astype("Int64").astype(str).str[:6] if not d.empty and "_ymd" in d else "").groupby(["policy", "period"], dropna=False).agg(n=("policy", "size"), delta_return_mean=("delta_return", "mean") if "delta_return" in overlay else ("policy", "size")).reset_index() if not overlay.empty else pd.DataFrame(columns=["policy", "period", "n", "delta_return_mean"])
    period.to_csv(run_dir / "period_stability_summary.csv", index=False)
    regime = overlay.groupby(["policy", "guard_anchor_type"], dropna=False).agg(n=("policy", "size")).reset_index() if not overlay.empty and "guard_anchor_type" in overlay else pd.DataFrame(columns=["policy", "guard_anchor_type", "n"])
    regime.to_csv(run_dir / "regime_stability_summary.csv", index=False)
    _write_json(run_dir / "input_artifact_report.json", {"guard_root": guard_root, "missing_guard_inputs": missing, "guard_no_lookahead_audit": guard_audit.get("audit_result"), "discovered_inputs": {k: [str(p) for p in v[:8]] for k, v in discovered.items()}, "daily_source": res.path})
    _write_json(run_dir / "cohort_availability_report.json", avail)
    _write_json(run_dir / "policy_a_stay_guard_exit_delay_summary.json", a_sum)
    _write_json(run_dir / "policy_b_reduce_suppression_summary.json", b_sum)
    _write_json(run_dir / "policy_c_short_veto_summary.json", c_sum)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(run_dir / "no_lookahead_audit.json", {"audit_result": "pass" if guard_audit.get("audit_result") == "pass" else "source_audit_not_pass", "guard_feature_source": "previous guard_hit_rows.csv", "overlay_window": "20 trading days after guard anchor", "policies_combined": False, "threshold_sweep": False, "model_training": False, "runtime_db_write": False})
    _write_json(run_dir / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": run_dir, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((run_dir / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(run_dir), "decision": decision, "policy_a": a_sum, "policy_b": b_sum, "policy_c": c_sum}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--guard-root", type=Path, default=DEFAULT_GUARD_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    p.add_argument("--production-csv", type=Path, default=DEFAULT_PRODUCTION_CSV)
    args = p.parse_args(argv)
    print(json.dumps(_json_ready(run(guard_root=args.guard_root, output_root=args.output_root, production_csv=args.production_csv)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
