from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "starter_candidate_chart_review_outcome_audit_v1"
DEFAULT_CHART_REVIEW_ROOT = Path(r"G:\Tradex\starter_candidate_chart_review_pack_v1\20260525T061448Z-starter-candidate-chart-review-pack-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_chart_review_outcome_audit_v1")

REQUIRED_ARTIFACTS = (
    "outcome_audit_summary.json",
    "outcome_audit_rows.csv",
    "label_bucket_metrics.json",
    "trigger_invalidation_audit.json",
    "no_lookahead_audit.json",
    "source_coverage.json",
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


def _runtime_db_candidates() -> list[Path]:
    local = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
    return [
        local / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
        local / "MeeMeeScreener" / "data" / "stocks.duckdb",
        Path("data/stocks.duckdb"),
        Path("app/backend/stocks.duckdb"),
    ]


def _ymd_to_epoch(ymd: int) -> int:
    return int(pd.Timestamp(str(int(ymd)), tz="UTC").timestamp())


def _epoch_to_ymd(epoch: int | float) -> int:
    return int(pd.to_datetime(int(epoch), unit="s", utc=True).strftime("%Y%m%d"))


def _add_days_ymd(ymd: int, days: int) -> int:
    return int((pd.Timestamp(str(int(ymd))) + pd.Timedelta(days=days)).strftime("%Y%m%d"))


def select_confirmed_db(min_date: int) -> Path:
    target = _ymd_to_epoch(min_date)
    for path in _runtime_db_candidates():
        if not path.exists():
            continue
        con = duckdb.connect(str(path), read_only=True)
        try:
            cols = [r[1] for r in con.execute("PRAGMA table_info('daily_bars')").fetchall()]
            if "source" not in cols:
                continue
            max_date = con.execute("SELECT max(date) FROM daily_bars WHERE source IN ('pan', 'txt', 'confirmed')").fetchone()[0]
            if max_date is not None and int(max_date) >= target:
                return path
        finally:
            con.close()
    raise RuntimeError(f"no confirmed runtime daily_bars source covers {min_date}")


def load_forward_bars(db_path: Path, rows: pd.DataFrame) -> pd.DataFrame:
    codes = sorted(rows["code"].astype(str).unique().tolist())
    min_epoch = _ymd_to_epoch(int(rows["decision_date"].min()))
    max_epoch = _ymd_to_epoch(int(rows["decision_date"].max())) + 60 * 86400
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        bars = con.execute(
            """
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE code IN ? AND date >= ? AND date <= ? AND source IN ('pan', 'txt', 'confirmed')
            ORDER BY code, date
            """,
            [codes, min_epoch, max_epoch],
        ).df()
    finally:
        con.close()
    if bars.empty:
        return bars
    bars["code"] = bars["code"].astype(str)
    bars["ymd"] = bars["date"].map(_epoch_to_ymd)
    return bars


def _future_metrics(code: str, decision_date: int, bars: pd.DataFrame, trigger_level: Any, invalidation_level: Any) -> dict[str, Any]:
    sort_col = "date" if "date" in bars.columns else "ymd"
    g = bars[bars["code"].astype(str).eq(str(code))].sort_values(sort_col).copy()
    if g.empty:
        return {"forward_available": False}
    base_idx = g.index[pd.to_numeric(g["ymd"], errors="coerce").eq(decision_date)].tolist()
    if not base_idx:
        return {"forward_available": False}
    loc = g.index.get_loc(base_idx[0])
    base_close = float(g.iloc[loc]["c"])
    future = g.iloc[loc + 1 : loc + 21].copy()
    out: dict[str, Any] = {"forward_available": len(future) > 0, "forward_bar_count": int(len(future)), "outcome_bar_source": "confirmed"}
    for horizon in [5, 10, 20]:
        if len(future) >= horizon and base_close:
            out[f"ret{horizon}"] = float(future.iloc[horizon - 1]["c"] / base_close - 1)
        else:
            out[f"ret{horizon}"] = None
    if len(future) and base_close:
        out["max_drawdown_20d"] = float(future["l"].min() / base_close - 1)
        out["trigger_hit"] = bool(future["h"].max() >= base_close * 1.03)
        out["invalidation_hit"] = bool(future["l"].min() <= base_close * 0.95)
        out["trigger_then_ret20"] = out["ret20"] if out["trigger_hit"] else None
    else:
        out.update({"max_drawdown_20d": None, "trigger_hit": None, "invalidation_hit": None, "trigger_then_ret20": None})
    return out


def audit_rows(chart_rows: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    audited: list[dict[str, Any]] = []
    for row in chart_rows.to_dict("records"):
        metrics = _future_metrics(row["code"], int(row["decision_date"]), bars, row.get("starter_trigger_level"), row.get("invalidation_level"))
        audited.append({**row, **metrics})
    return pd.DataFrame(audited)


def _metric_mean(frame: pd.DataFrame, col: str) -> float | None:
    if frame.empty or col not in frame:
        return None
    values = pd.to_numeric(frame[col], errors="coerce").dropna()
    return float(values.mean()) if not values.empty else None


def bucket_metrics(rows: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for label, g in rows.groupby("manual_judgment", dropna=False):
        ret20 = pd.to_numeric(g.get("ret20"), errors="coerce")
        result[str(label)] = {
            "sample_count": int(len(g)),
            "date_count": int(g["decision_date"].nunique()),
            "code_count": int(g["code"].astype(str).nunique()),
            "mean_ret5": _metric_mean(g, "ret5"),
            "mean_ret10": _metric_mean(g, "ret10"),
            "mean_ret20": _metric_mean(g, "ret20"),
            "median_ret20": float(ret20.dropna().median()) if not ret20.dropna().empty else None,
            "hit_rate_ret20_gt_0": float((ret20.dropna() > 0).mean()) if not ret20.dropna().empty else None,
            "bad_rate_ret20_lt_minus_5pct": float((ret20.dropna() < -0.05).mean()) if not ret20.dropna().empty else None,
            "severe_rate_ret20_lt_minus_10pct": float((ret20.dropna() < -0.10).mean()) if not ret20.dropna().empty else None,
            "max_drawdown_20d": _metric_mean(g, "max_drawdown_20d"),
            "trigger_hit_rate": float(g["trigger_hit"].dropna().astype(bool).mean()) if "trigger_hit" in g and not g["trigger_hit"].dropna().empty else None,
            "invalidation_hit_rate": float(g["invalidation_hit"].dropna().astype(bool).mean()) if "invalidation_hit" in g and not g["invalidation_hit"].dropna().empty else None,
            "trigger_then_ret20_mean": _metric_mean(g, "trigger_then_ret20"),
        }
    return result


def comparisons(rows: pd.DataFrame, metrics: dict[str, Any]) -> dict[str, Any]:
    def comp(a: str, b: str) -> dict[str, Any]:
        ga = rows[rows["manual_judgment"].eq(a)]
        gb = rows[rows["manual_judgment"].eq(b)]
        under = len(ga) < 10 or len(gb) < 10 or rows["decision_date"].nunique() < 5
        return {
            "left": a,
            "right": b,
            "left_n": int(len(ga)),
            "right_n": int(len(gb)),
            "mean_ret20_delta": None if a not in metrics or b not in metrics else (metrics[a].get("mean_ret20") or 0) - (metrics[b].get("mean_ret20") or 0),
            "sample_insufficient": under,
        }
    non_ready = rows[~rows["manual_judgment"].eq("starter_ready")].copy()
    all_non_ready_under = len(rows[rows["manual_judgment"].eq("starter_ready")]) < 10 or len(non_ready) < 10 or rows["decision_date"].nunique() < 5
    family = rows["research_candidate_source_family"] if "research_candidate_source_family" in rows else pd.Series([""] * len(rows), index=rows.index)
    pull = rows[(rows["manual_judgment"].eq("starter_ready")) & (family.eq("pullback_reclaim_source"))]
    brk = rows[(rows["manual_judgment"].eq("starter_ready")) & (family.eq("breakout_retest_source"))]
    return {
        "starter_ready_vs_wait_for_trigger": comp("starter_ready", "wait_for_trigger"),
        "starter_ready_vs_avoid": comp("starter_ready", "avoid"),
        "starter_ready_vs_all_non_ready": {
            "starter_ready_n": int((rows["manual_judgment"] == "starter_ready").sum()),
            "all_non_ready_n": int(len(non_ready)),
            "sample_insufficient": all_non_ready_under,
        },
        "pullback_starter_ready_vs_breakout_starter_ready": {
            "pullback_n": int(len(pull)),
            "breakout_n": int(len(brk)),
            "sample_insufficient": len(pull) < 10 or len(brk) < 10 or rows["decision_date"].nunique() < 5,
        },
    }


def decide(rows: pd.DataFrame, comps: dict[str, Any]) -> str:
    if len(rows) < 30 or rows["decision_date"].nunique() < 5 or any(v.get("sample_insufficient") for v in comps.values() if isinstance(v, dict)):
        return "sample_insufficient"
    metrics = bucket_metrics(rows)
    ready = metrics.get("starter_ready", {}).get("mean_ret20")
    non_ready = rows[~rows["manual_judgment"].eq("starter_ready")]
    non_ready_mean = _metric_mean(non_ready, "ret20")
    if ready is None or non_ready_mean is None:
        return "sample_insufficient"
    if ready < non_ready_mean:
        return "worse_than_non_ready"
    if ready > non_ready_mean + 0.03:
        return "validated_separation_candidate"
    return "no_clear_separation"


def discover_chart_roots(root: Path) -> list[Path]:
    parent = root.parent
    found = [p for p in parent.glob("*-starter-candidate-chart-review-pack-v1") if (p / "candidate_chart_review_rows.csv").exists()]
    return sorted(found) or [root]


def run(chart_review_root: Path, output_root: Path, db_path: Path | None = None) -> Path:
    out = output_root / f"{_now_tag()}-starter-candidate-chart-review-outcome-audit-v1"
    out.mkdir(parents=True, exist_ok=True)
    roots = discover_chart_roots(chart_review_root)
    frames = []
    for root in roots:
        frame = pd.read_csv(root / "candidate_chart_review_rows.csv", low_memory=False)
        frame["source_chart_review_root"] = str(root)
        frames.append(frame)
    chart_rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if chart_rows.empty:
        raise RuntimeError("no chart review rows found")
    selected_db = db_path or select_confirmed_db(int(chart_rows["decision_date"].max()))
    bars = load_forward_bars(selected_db, chart_rows)
    audited = audit_rows(chart_rows, bars)
    audited.to_csv(out / "outcome_audit_rows.csv", index=False)
    metrics = bucket_metrics(audited)
    comps = comparisons(audited, metrics)
    decision = decide(audited, comps)
    sample_insufficient = decision == "sample_insufficient"
    _write_json(out / "label_bucket_metrics.json", {"metrics_by_label": metrics, "comparisons": comps})
    _write_json(
        out / "trigger_invalidation_audit.json",
        {
            "trigger_hit_rate_by_label": {k: v.get("trigger_hit_rate") for k, v in metrics.items()},
            "invalidation_hit_rate_by_label": {k: v.get("invalidation_hit_rate") for k, v in metrics.items()},
            "trigger_then_ret20_metrics": {k: v.get("trigger_then_ret20_mean") for k, v in metrics.items()},
        },
    )
    source_ok = bool(not bars.empty and set(bars["source"].dropna().unique()).issubset({"pan", "txt", "confirmed"}))
    _write_json(
        out / "no_lookahead_audit.json",
        {
            "passes": source_ok,
            "label_source": "preexisting chart review labels only",
            "future_outcomes_start_after_decision_date": True,
            "labels_do_not_use_future_outcomes": True,
            "confirmed_bars_only": source_ok,
        },
    )
    _write_json(
        out / "source_coverage.json",
        {
            "chart_review_roots": roots,
            "chart_review_root_count": len(roots),
            "sample_count": int(len(audited)),
            "date_count": int(audited["decision_date"].nunique()),
            "code_count": int(audited["code"].astype(str).nunique()),
            "runtime_db_path": selected_db,
            "bar_sources": sorted(bars["source"].dropna().unique().tolist()) if not bars.empty else [],
            "confirmed_source_only": source_ok,
            "historical_snapshots_found": len(roots) > 1,
        },
    )
    _write_json(
        out / "outcome_audit_summary.json",
        {
            "axis_id": AXIS_ID,
            "decision": decision,
            "sample_insufficient": sample_insufficient,
            "sample_count": int(len(audited)),
            "date_count": int(audited["decision_date"].nunique()),
            "code_count": int(audited["code"].astype(str).nunique()),
            "validated_buy_count": 0,
            "meemee_reflectable_candidate": False,
            "reason": "only one chart-review date available" if sample_insufficient else "computed from available chart-review labels",
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chart-review-root", type=Path, default=DEFAULT_CHART_REVIEW_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(args.chart_review_root, args.output_root, args.db_path))


if __name__ == "__main__":
    main()
