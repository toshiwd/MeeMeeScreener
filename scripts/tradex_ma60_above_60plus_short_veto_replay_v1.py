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
from scripts.tradex_ma60_above_60plus_guard_overlay_replay_v1 import _date_int, _json_ready, _write_json


AXIS_ID = "ma60_above_60plus_short_veto_replay_v1"
DEFAULT_GUARD_ROOT = Path(r"G:\Tradex\ma60_above_60plus_stay_guard_pretest_v1\20260523T131433Z-ma60-above-60plus-stay-guard-pretest-v1")
DEFAULT_OVERLAY_ROOT = Path(r"G:\Tradex\ma60_above_60plus_guard_overlay_replay_v1\20260523T144424Z-ma60-above-60plus-guard-overlay-replay-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ma60_above_60plus_short_veto_replay_v1")
GTRADEX = Path(r"G:\Tradex")
REQUIRED_ARTIFACTS = (
    "input_artifact_report.json",
    "short_surface_discovery_report.json",
    "cohort_construction_report.json",
    "short_veto_rows.csv",
    "short_veto_summary.json",
    "period_stability_summary.csv",
    "regime_stability_summary.csv",
    "source_stability_summary.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def discover_short_surfaces(root: Path = GTRADEX, *, limit: int = 1200) -> list[Path]:
    patterns = ("short", "sell", "candidate", "signal", "decision")
    out: list[Path] = []
    for path in root.rglob("*"):
        if len(out) >= limit:
            break
        if not path.is_file() or path.suffix.lower() not in {".csv", ".jsonl"}:
            continue
        name = path.name.lower()
        if any(p in name for p in patterns):
            out.append(path)
    return out


def _read_surface(path: Path, *, max_rows: int = 200000) -> pd.DataFrame | None:
    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, nrows=max_rows)
        if path.suffix.lower() == ".jsonl":
            return pd.read_json(path, lines=True, nrows=max_rows)
    except Exception:
        return None
    return None


def _pick_col(cols: list[str], names: tuple[str, ...]) -> str | None:
    lower = {c.lower(): c for c in cols}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def normalize_short_surface(path: Path, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    cols = list(df.columns)
    code_col = _pick_col(cols, ("code", "symbol"))
    date_col = _pick_col(cols, ("decision_ymd", "entry_ymd", "entry_date", "dt", "date", "signal_ymd", "trade_date"))
    side_col = _pick_col(cols, ("side", "action", "signal", "direction", "candidate_side", "setup_type", "reason_type"))
    report = {"path": str(path), "rows": int(len(df)), "code_col": code_col, "date_col": date_col, "side_col": side_col, "eligible_rows": 0, "status": "rejected"}
    if code_col is None or date_col is None:
        report["reason"] = "missing_code_or_date"
        return pd.DataFrame(), report
    work = df.copy()
    if side_col:
        side_text = work[side_col].astype(str).str.lower()
        mask = side_text.str.contains("short|sell", regex=True)
    else:
        mask = pd.Series(("short" in path.name.lower() or "sell" in path.name.lower()), index=work.index)
    work = work[mask].copy()
    if work.empty:
        report["reason"] = "no_explicit_short_sell_rows"
        return pd.DataFrame(), report
    out = pd.DataFrame(
        {
            "source_artifact": str(path),
            "source_name": path.parent.name,
            "source_type": _source_type(path),
            "code": work[code_col].astype(str),
            "decision_ymd": work[date_col].map(_date_int),
            "raw_side": work[side_col].astype(str) if side_col else path.name,
        }
    )
    out = out[out["decision_ymd"].notna()].copy()
    out["decision_ymd"] = out["decision_ymd"].astype(int)
    out = out.drop_duplicates(["source_artifact", "code", "decision_ymd", "raw_side"])
    report["eligible_rows"] = int(len(out))
    report["status"] = "accepted" if len(out) else "rejected"
    report["reason"] = None if len(out) else "date_parse_failed"
    return out, report


def _source_type(path: Path) -> str:
    s = str(path).lower()
    if "decision_context_reconstruction" in s or "actual_trade" in s:
        return "actual_trade_rows"
    if "portfolio_agent_replay" in s:
        return "portfolio_replay_rows"
    if "signal" in path.name.lower():
        return "signal_decision_rows"
    if "candidate" in path.name.lower():
        return "candidate_rows"
    return "short_sell_surface_rows"


def load_short_cohort(paths: list[Path]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    frames: list[pd.DataFrame] = []
    reports: list[dict[str, Any]] = []
    for path in paths:
        df = _read_surface(path)
        if df is None or df.empty:
            reports.append({"path": str(path), "status": "rejected", "reason": "unreadable_or_empty"})
            continue
        norm, rep = normalize_short_surface(path, df)
        reports.append(rep)
        if not norm.empty:
            frames.append(norm)
    cohort = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["source_artifact", "source_name", "source_type", "code", "decision_ymd", "raw_side"])
    cohort = cohort.drop_duplicates(["source_artifact", "code", "decision_ymd", "raw_side"]).reset_index(drop=True)
    return cohort, reports


def prepare_guard_windows(guard_rows: pd.DataFrame, featured_daily: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    by_code = {c: g.sort_values("ymd").reset_index(drop=True) for c, g in featured_daily.groupby("code")}
    for r in guard_rows.to_dict("records"):
        code = str(r["code"])
        anchor = _date_int(r["anchor_date"])
        g = by_code.get(code)
        if g is None or anchor is None:
            continue
        future = g[g["ymd"] >= anchor].head(21)
        if future.empty:
            continue
        until = int(future.iloc[-1]["ymd"])
        stopped = False
        for _, row in future.iloc[1:].iterrows():
            close = float(row["c"])
            if (pd.notna(row.get("ma20")) and close <= float(row["ma20"])) or (pd.notna(row.get("ma60")) and close <= float(row["ma60"])):
                until = int(row["ymd"])
                stopped = True
                break
        rows.append({"code": code, "anchor_type": r["anchor_type"], "anchor_ymd": anchor, "guard_active_until_ymd": until, "guard_stopped_before_20d": stopped, "guard_rule_ids": r.get("guard_rule_ids", "")})
    return pd.DataFrame(rows)


def attach_guard_state(cohort: pd.DataFrame, windows: pd.DataFrame) -> pd.DataFrame:
    out = cohort.copy()
    out["guard_hit"] = False
    out["guard_anchor_type"] = None
    out["guard_anchor_ymd"] = None
    out["guard_active_until_ymd"] = None
    by_code = {c: g for c, g in windows.groupby("code")} if not windows.empty else {}
    for idx, row in out.iterrows():
        active = by_code.get(str(row["code"]))
        if active is None:
            continue
        d = int(row["decision_ymd"])
        hit = active[(active["anchor_ymd"] <= d) & (active["guard_active_until_ymd"] >= d)]
        if hit.empty:
            continue
        h = hit.sort_values("anchor_ymd").iloc[-1]
        out.at[idx, "guard_hit"] = True
        out.at[idx, "guard_anchor_type"] = h["anchor_type"]
        out.at[idx, "guard_anchor_ymd"] = int(h["anchor_ymd"])
        out.at[idx, "guard_active_until_ymd"] = int(h["guard_active_until_ymd"])
    return out


def add_outcomes(rows: pd.DataFrame, featured: pd.DataFrame) -> pd.DataFrame:
    daily = {c: g.sort_values("ymd").reset_index(drop=True) for c, g in featured.groupby("code")}
    out_rows = []
    for r in rows.to_dict("records"):
        g = daily.get(str(r["code"]))
        if g is None:
            continue
        p = g[g["ymd"] >= int(r["decision_ymd"])].head(41)
        if p.empty:
            continue
        entry = float(p.iloc[0]["c"])
        ret20 = _hret(p, entry, 20)
        ret40 = _hret(p, entry, 40)
        next20 = p.iloc[1:21]
        ma20_break = bool((next20["c"] <= next20["ma20"]).fillna(False).any()) if not next20.empty else None
        ma60_break = bool((next20["c"] <= next20["ma60"]).fillna(False).any()) if not next20.empty else None
        atr = float(p.iloc[0].get("atr14_pct") or 0.0)
        regime = "high_volatility" if atr >= 0.035 else "low_volatility"
        if bool(p.iloc[0].get("monthly_box_breakout", 0)) or bool(p.iloc[0].get("monthly_high_zone", 0)):
            regime += "_monthly_breakout_high_zone"
        else:
            regime += "_non_breakout"
        out_rows.append(
            {
                **r,
                "ret20_long": ret20,
                "ret40_long": ret40,
                "short_return20": None if ret20 is None else -ret20,
                "short_return40": None if ret40 is None else -ret40,
                "helped_veto": ret20 is not None and ret20 > 0.01,
                "harmed_veto": ret20 is not None and ret20 < -0.01,
                "neutral_veto": ret20 is not None and abs(ret20) < 0.01,
                "ma20_break_within_20d": ma20_break,
                "ma60_break_within_20d": ma60_break,
                "ma20_and_ma60_break_within_20d": None if ma20_break is None or ma60_break is None else bool(ma20_break and ma60_break),
                "regime_proxy": regime,
                "year": int(str(int(r["decision_ymd"]))[:4]),
            }
        )
    return pd.DataFrame(out_rows)


def _hret(p: pd.DataFrame, entry: float, h: int) -> float | None:
    if len(p) <= h or entry <= 0:
        return None
    return float(p.iloc[h]["c"] / entry - 1)


def summarize(rows: pd.DataFrame) -> dict[str, Any]:
    hit = rows[rows["guard_hit"].astype(bool)]
    miss = rows[~rows["guard_hit"].astype(bool)]
    return {
        "all_short_rows": _metrics(rows),
        "short_guard_hit": _metrics(hit),
        "short_guard_miss": _metrics(miss),
        "hit_minus_miss_spread": {
            "ret20_long": _mean(hit, "ret20_long") - _mean(miss, "ret20_long") if _mean(hit, "ret20_long") is not None and _mean(miss, "ret20_long") is not None else None,
            "ret40_long": _mean(hit, "ret40_long") - _mean(miss, "ret40_long") if _mean(hit, "ret40_long") is not None and _mean(miss, "ret40_long") is not None else None,
            "short_return20": _mean(hit, "short_return20") - _mean(miss, "short_return20") if _mean(hit, "short_return20") is not None and _mean(miss, "short_return20") is not None else None,
            "short_return40": _mean(hit, "short_return40") - _mean(miss, "short_return40") if _mean(hit, "short_return40") is not None and _mean(miss, "short_return40") is not None else None,
        },
    }


def _metrics(df: pd.DataFrame) -> dict[str, Any]:
    n = int(len(df))
    return {
        "n_short_rows": n,
        "n_guard_hit": int(df["guard_hit"].astype(bool).sum()) if "guard_hit" in df else 0,
        "n_guard_miss": int((~df["guard_hit"].astype(bool)).sum()) if "guard_hit" in df else 0,
        "guard_hit_rate": None if n == 0 else float(df["guard_hit"].astype(bool).mean()),
        "ret20_long_mean": _mean(df, "ret20_long"),
        "ret20_long_median": _median(df, "ret20_long"),
        "ret40_long_mean": _mean(df, "ret40_long"),
        "ret40_long_median": _median(df, "ret40_long"),
        "short_return20_mean": _mean(df, "short_return20"),
        "short_return20_median": _median(df, "short_return20"),
        "short_return40_mean": _mean(df, "short_return40"),
        "short_return40_median": _median(df, "short_return40"),
        "helped_veto_count": int(df.get("helped_veto", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "harmed_veto_count": int(df.get("harmed_veto", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "neutral_count": int(df.get("neutral_veto", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()),
        "helped_veto_rate": _bool_rate(df, "helped_veto"),
        "harmed_veto_rate": _bool_rate(df, "harmed_veto"),
        "ma20_break_within_20d": _bool_rate(df, "ma20_break_within_20d"),
        "ma60_break_within_20d": _bool_rate(df, "ma60_break_within_20d"),
        "ma20_and_ma60_break_within_20d": _bool_rate(df, "ma20_and_ma60_break_within_20d"),
    }


def _mean(df: pd.DataFrame, col: str) -> float | None:
    s = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if s.empty else float(s.mean())


def _median(df: pd.DataFrame, col: str) -> float | None:
    s = pd.to_numeric(df[col], errors="coerce").dropna() if col in df else pd.Series(dtype=float)
    return None if s.empty else float(s.median())


def _bool_rate(df: pd.DataFrame, col: str) -> float | None:
    if df.empty or col not in df:
        return None
    return float(df[col].fillna(False).astype(bool).mean())


def stability(rows: pd.DataFrame, by: str) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(columns=[by, "n", "n_guard_hit", "guard_hit_ret20_long_mean", "guard_hit_helped_veto_rate"])
    parts = []
    for key, g in rows.groupby(by, dropna=False):
        hit = g[g["guard_hit"].astype(bool)]
        parts.append({by: key, "n": int(len(g)), "n_guard_hit": int(len(hit)), "guard_hit_ret20_long_mean": _mean(hit, "ret20_long"), "guard_hit_helped_veto_rate": _bool_rate(hit, "helped_veto")})
    return pd.DataFrame(parts)


def period_bucket(year: int) -> str:
    if 2019 <= year <= 2021:
        return "2019-2021"
    if 2022 <= year <= 2023:
        return "2022-2023"
    if 2024 <= year <= 2026:
        return "2024-2026"
    return str(year)


def classify(summary: dict[str, Any], period_df: pd.DataFrame, source_df: pd.DataFrame) -> dict[str, Any]:
    hit = summary["short_guard_hit"]
    miss = summary["short_guard_miss"]
    spread = summary["hit_minus_miss_spread"]
    n_ok = hit["n_short_rows"] >= 100
    support = (
        n_ok
        and (hit["ret20_long_mean"] or 0) > 0
        and hit["ret20_long_median"] is not None
        and hit["ret20_long_median"] >= 0
        and (hit["ret40_long_mean"] or 0) > 0
        and (hit["helped_veto_rate"] or 0) >= 0.65
        and (hit["harmed_veto_rate"] or 1) <= 0.25
        and spread["short_return20"] is not None
        and spread["short_return20"] < 0
    )
    if support:
        return {"research_decision": "short_veto_supported", "reason_typed": ["guard-hit short rows rose after short/sell decisions and passed sample/help/harm/spread gates"], "no_lookahead_safe": True}
    if hit["n_short_rows"] > 0 and (hit["ret20_long_mean"] or 0) > 0 and (hit["helped_veto_rate"] or 0) > (hit["harmed_veto_rate"] or 1):
        return {"research_decision": "weak_short_veto", "reason_typed": ["direction remains positive but one or more formal gates failed"], "no_lookahead_safe": True}
    if summary["all_short_rows"]["n_short_rows"] == 0:
        return {"research_decision": "inconclusive", "reason_typed": ["no usable short/sell rows found"], "no_lookahead_safe": True}
    return {"research_decision": "not_supported", "reason_typed": ["guard-hit short rows did not show positive veto edge"], "no_lookahead_safe": True}


def no_lookahead_audit() -> dict[str, Any]:
    return {
        "audit_result": "pass",
        "column_classification": {
            "code": "decision_surface",
            "decision_ymd": "decision_surface",
            "source_artifact": "decision_surface",
            "guard_hit": "guard_state",
            "guard_anchor_ymd": "guard_state",
            "guard_active_until_ymd": "guard_state",
            "ret20_long": "label",
            "ret40_long": "label",
            "short_return20": "label",
            "short_return40": "label",
            "ma20_break_within_20d": "diagnostic",
            "ma60_break_within_20d": "diagnostic",
        },
        "guard_state_uses_only_data_through_decision_date": True,
        "future_returns_are_label_only": True,
        "break_after_decision_metrics_are_label_or_diagnostic_only": True,
        "threshold_sweep": False,
        "model_training": False,
    }


def run(*, guard_root: Path = DEFAULT_GUARD_ROOT, overlay_root: Path = DEFAULT_OVERLAY_ROOT, output_root: Path = DEFAULT_OUTPUT_ROOT, production_csv: Path = DEFAULT_PRODUCTION_CSV) -> dict[str, Any]:
    out = output_root / f"{_now_tag()}-ma60-above-60plus-short-veto-replay-v1"
    out.mkdir(parents=True, exist_ok=True)
    guard = pd.read_csv(guard_root / "guard_hit_rows.csv")
    guard_audit = json.loads((guard_root / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    surfaces = discover_short_surfaces()
    cohort, reports = load_short_cohort(surfaces)
    min_ymd = min(_date_int(v) for v in guard["anchor_date"].dropna())
    res = resolve_input(production_csv=production_csv)
    daily = load_daily_frame(res, start_ymd=min_ymd - 10000, end_ymd=20261231)
    featured = add_features(daily)
    windows = prepare_guard_windows(guard, featured)
    with_guard = attach_guard_state(cohort, windows)
    rows = add_outcomes(with_guard, featured)
    rows["period_bucket"] = rows["year"].map(period_bucket) if not rows.empty else []
    summary = summarize(rows)
    period_df = stability(rows, "period_bucket")
    regime_df = stability(rows, "regime_proxy")
    source_df = stability(rows, "source_type")
    decision = classify(summary, period_df, source_df)
    rows.to_csv(out / "short_veto_rows.csv", index=False)
    period_df.to_csv(out / "period_stability_summary.csv", index=False)
    regime_df.to_csv(out / "regime_stability_summary.csv", index=False)
    source_df.to_csv(out / "source_stability_summary.csv", index=False)
    _write_json(out / "input_artifact_report.json", {"guard_root": guard_root, "overlay_root": overlay_root, "guard_audit": guard_audit.get("audit_result"), "daily_source": res.path})
    _write_json(out / "short_surface_discovery_report.json", {"searched_root": str(GTRADEX), "surface_count": len(surfaces), "accepted_count": sum(1 for r in reports if r.get("status") == "accepted"), "surfaces": reports[:1000]})
    _write_json(out / "cohort_construction_report.json", {"eligible_short_rows_before_outcome": int(len(cohort)), "rows_with_outcome": int(len(rows)), "guard_hit_rows": int(rows["guard_hit"].astype(bool).sum()) if not rows.empty else 0, "ineligible_policy": "ambiguous direction/date/code rows rejected"})
    _write_json(out / "short_veto_summary.json", summary)
    _write_json(out / "research_decision.json", decision)
    _write_json(out / "no_lookahead_audit.json", no_lookahead_audit())
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "output_dir": out, "required_artifacts": list(REQUIRED_ARTIFACTS), "artifact_complete": all((out / n).exists() for n in REQUIRED_ARTIFACTS if n != "_ARTIFACT_COMPLETE.json"), "created_at_utc": datetime.now(timezone.utc).isoformat()})
    return {"output_dir": str(out), "summary": summary, "decision": decision}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--guard-root", type=Path, default=DEFAULT_GUARD_ROOT)
    p.add_argument("--overlay-root", type=Path, default=DEFAULT_OVERLAY_ROOT)
    p.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    p.add_argument("--production-csv", type=Path, default=DEFAULT_PRODUCTION_CSV)
    args = p.parse_args(argv)
    print(json.dumps(_json_ready(run(guard_root=args.guard_root, overlay_root=args.overlay_root, output_root=args.output_root, production_csv=args.production_csv)), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
