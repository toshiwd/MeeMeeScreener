from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sys
from contextlib import ExitStack
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.analysis import ranking_backtest_service
from app.backend.services.ml import rankings_cache
from app.backend.services.tradex_experiment_store import write_json
from app.db.session import get_conn_for_path
from app.core.config import config

DEFAULT_BUCKETS = (5, 10, 20)
DEFAULT_ROUND_TRIP_COST = 0.002
QUALITY_SCORE_QUANTILE = 0.75
QUALITY_SCRIPT_SCHEMA_VERSION = "ranking_entry_quality_backtest_v1"


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError("date must be YYYY-MM-DD or YYYYMMDD")


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _summary_from_returns(values: pd.Series) -> dict[str, Any]:
    arr = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=np.float64, copy=False)
    if arr.size == 0:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "win_rate": None,
            "profit_factor": None,
            "sum": None,
            "mdd": None,
        }
    gains = float(arr[arr > 0.0].sum())
    losses = float(-arr[arr < 0.0].sum())
    if losses <= 1e-12:
        profit_factor = float("inf") if gains > 0.0 else 0.0
    else:
        profit_factor = float(gains / losses)
    growth = np.clip(1.0 + arr, 1e-6, 1e6)
    log_equity = np.cumsum(np.log(growth))
    log_equity = np.clip(log_equity, -60.0, 60.0)
    equity = np.exp(log_equity)
    peak = np.maximum.accumulate(equity)
    drawdown = np.where(peak > 0.0, equity / peak - 1.0, 0.0)
    return {
        "n": int(arr.size),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "win_rate": float(np.mean(arr > 0.0)),
        "profit_factor": profit_factor,
        "sum": float(arr.sum()),
        "mdd": float(max(0.0, -drawdown.min())),
    }


def _monthly_summary(panel: pd.DataFrame, *, return_col: str) -> dict[str, Any]:
    if panel.empty or return_col not in panel.columns:
        return {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None}
    frame = panel.copy()
    frame["month"] = frame["as_of"].astype(str).str.slice(0, 6)
    monthly = frame.groupby("month", sort=True)[return_col].mean().dropna()
    if monthly.empty:
        return {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None}
    return {
        "month_count": int(monthly.size),
        "positive_month_rate": float((monthly > 0.0).mean()),
        "worst_month": float(monthly.min()),
        "best_month": float(monthly.max()),
    }


def _overlap_metrics(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty:
        return {"daily_overlap_rate": None, "daily_turnover_rate": None}
    sets_by_day: dict[int, set[str]] = {}
    for as_of, group in panel.groupby("as_of", sort=True):
        sets_by_day[int(as_of)] = {str(code) for code in group["code"].tolist() if str(code).strip()}
    if len(sets_by_day) <= 1:
        return {"daily_overlap_rate": None, "daily_turnover_rate": None}
    overlap_values: list[float] = []
    turnover_values: list[float] = []
    previous: set[str] | None = None
    for _, current in sorted(sets_by_day.items()):
        if previous is None:
            previous = current
            continue
        denom = max(len(previous), len(current), 1)
        overlap = len(previous & current) / float(denom)
        overlap_values.append(float(overlap))
        turnover_values.append(float(1.0 - overlap))
        previous = current
    return {
        "daily_overlap_rate": _mean(overlap_values),
        "daily_turnover_rate": _mean(turnover_values),
    }


def _code_concentration(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty or "code" not in panel.columns:
        return {"unique_codes": 0, "top_code_share": None, "top5_code_share": None}
    counts = panel["code"].astype(str).value_counts(dropna=True)
    total = int(counts.sum()) if not counts.empty else 0
    if total <= 0 or counts.empty:
        return {"unique_codes": 0, "top_code_share": None, "top5_code_share": None}
    shares = counts / float(total)
    return {
        "unique_codes": int(counts.size),
        "top_code_share": float(shares.iloc[0]),
        "top5_code_share": float(shares.head(5).sum()),
    }


def _apply_round_trip_cost(series: pd.Series, *, round_trip_cost: float) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values - float(round_trip_cost)


def _select_daily_bucket(
    panel: pd.DataFrame,
    *,
    bucket_size: int,
    variant: str,
    round_trip_cost: float,
) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()

    frame = panel.copy()
    frame["setupType"] = frame["setupType"].fillna("watch").astype(str)
    frame["displayScore"] = pd.to_numeric(frame.get("displayScore"), errors="coerce")
    frame["entryQualified"] = frame["entryQualified"] == True  # noqa: E712

    def _pick_group(group: pd.DataFrame) -> pd.DataFrame:
        working = group.copy()
        if variant == "all":
            pass
        elif variant == "entryQualified":
            working = working[working["entryQualified"]].copy()
        elif variant == "entryQualifiedBreakout":
            working = working[working["entryQualified"] & working["setupType"].eq("breakout")].copy()
        elif variant == "entryQualifiedWatch":
            working = working[working["entryQualified"] & working["setupType"].eq("watch")].copy()
        elif variant == "entryQualifiedTopQuartileScore":
            qualified = working[working["entryQualified"]].copy()
            if qualified.empty:
                return qualified.iloc[0:0].copy()
            score_series = qualified["displayScore"].dropna()
            if score_series.empty:
                return qualified.sort_values(["rank", "code"], ascending=[True, True], kind="stable").head(bucket_size).copy()
            cutoff = float(score_series.quantile(QUALITY_SCORE_QUANTILE))
            selected = qualified[qualified["displayScore"].fillna(-np.inf) >= cutoff].copy()
            if selected.empty:
                selected = qualified.sort_values(
                    ["displayScore", "rank", "code"],
                    ascending=[False, True, True],
                    kind="stable",
                ).head(max(1, int(math.ceil(len(qualified) * (1.0 - QUALITY_SCORE_QUANTILE)))))
            working = selected
        else:
            raise ValueError(f"unknown variant: {variant}")

        working = working.sort_values(["rank", "displayScore", "code"], ascending=[True, False, True], kind="stable")
        working = working.head(int(bucket_size)).copy()
        if working.empty:
            return working
        working["forward_return_5_net"] = _apply_round_trip_cost(working["forward_return_5"], round_trip_cost=round_trip_cost)
        working["forward_return_20_net"] = _apply_round_trip_cost(working["forward_return_20"], round_trip_cost=round_trip_cost)
        working["forward_return_60_net"] = _apply_round_trip_cost(working["forward_return_60"], round_trip_cost=round_trip_cost)
        return working

    selected_frames: list[pd.DataFrame] = []
    for _, group in frame.groupby("as_of", sort=False):
        picked = _pick_group(group)
        if not picked.empty:
            selected_frames.append(picked)
    if not selected_frames:
        return frame.iloc[0:0].copy()
    return pd.concat(selected_frames, ignore_index=True)


def _cohort_summary(panel: pd.DataFrame, *, round_trip_cost: float) -> dict[str, Any]:
    if panel.empty:
        return {
            "sample_count": 0,
            "daily_count": 0,
            "avg_per_day": None,
            "return_summary": {},
            "return_summary_net": {},
            "overlap": {"daily_overlap_rate": None, "daily_turnover_rate": None},
            "concentration": {"unique_codes": 0, "top_code_share": None, "top5_code_share": None},
            "monthly": {"month_count": 0, "positive_month_rate": None, "worst_month": None, "best_month": None},
        }
    summary = {
        "sample_count": int(len(panel)),
        "daily_count": int(panel["as_of"].nunique()) if "as_of" in panel.columns else 0,
        "avg_per_day": float(len(panel) / max(1, int(panel["as_of"].nunique()))) if "as_of" in panel.columns else None,
        "overlap": _overlap_metrics(panel),
        "concentration": _code_concentration(panel),
        "monthly": _monthly_summary(panel, return_col="forward_return_20_net"),
    }
    for horizon in (5, 20, 60):
        raw_col = f"forward_return_{horizon}"
        net_col = f"forward_return_{horizon}_net"
        summary[f"return_{horizon}"] = _summary_from_returns(panel[raw_col] if raw_col in panel.columns else pd.Series(dtype=float))
        summary[f"return_{horizon}_net"] = _summary_from_returns(panel[net_col] if net_col in panel.columns else pd.Series(dtype=float))
    return summary


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not math.isfinite(value):
        return "-"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _render_markdown(payload: dict[str, Any]) -> str:
    period = payload.get("period") if isinstance(payload.get("period"), dict) else {}
    lines = [
        "# Ranking Entry Quality Backtest",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- period: {period.get('start_date')} .. {period.get('end_date')}",
        f"- round_trip_cost: {payload.get('round_trip_cost')}",
        f"- bucket_sizes: {payload.get('bucket_sizes')}",
        "",
        "## Verdict",
        f"- usable: {payload.get('verdict')}",
        "",
        "## Variant Summary",
    ]
    for variant_name, variant_payload in (payload.get("variants") or {}).items():
        if not isinstance(variant_payload, dict):
            continue
        top10 = variant_payload.get("top10") if isinstance(variant_payload.get("top10"), dict) else {}
        net20 = top10.get("return_20_net") if isinstance(top10, dict) else {}
        lines.append(
            f"- {variant_name}: sample={top10.get('sample_count')}, days={top10.get('daily_count')}, "
            f"net20_mean={_fmt(net20.get('mean'))}, net20_pf={_fmt(net20.get('profit_factor'))}, "
            f"net20_mdd={_fmt(net20.get('mdd'))}, top_code_share={_fmt(top10.get('concentration', {}).get('top_code_share'))}"
        )
    lines.append("")
    lines.append("## Top10 Detail")
    for variant_name, variant_payload in (payload.get("variants") or {}).items():
        if not isinstance(variant_payload, dict):
            continue
        top10 = variant_payload.get("top10") if isinstance(variant_payload.get("top10"), dict) else {}
        lines.append(f"### {variant_name}")
        lines.append(f"- raw20_mean: {_fmt((top10.get('return_20') or {}).get('mean'))}")
        lines.append(f"- net20_mean: {_fmt((top10.get('return_20_net') or {}).get('mean'))}")
        lines.append(f"- net20_pf: {_fmt((top10.get('return_20_net') or {}).get('profit_factor'))}")
        lines.append(f"- net20_mdd: {_fmt((top10.get('return_20_net') or {}).get('mdd'))}")
        lines.append(f"- positive_month_rate: {_fmt((top10.get('monthly') or {}).get('positive_month_rate'))}")
        lines.append(f"- top_code_share: {_fmt((top10.get('concentration') or {}).get('top_code_share'))}")
        lines.append("")
    return "\n".join(lines)


def _build_variant_report(panel: pd.DataFrame, *, bucket_sizes: tuple[int, ...], round_trip_cost: float) -> dict[str, Any]:
    variants = {
        "all": "全銘柄",
        "entryQualified": "entryQualified",
        "entryQualifiedBreakout": "entryQualified + breakout",
        "entryQualifiedWatch": "entryQualified + watch",
        "entryQualifiedTopQuartileScore": "entryQualified + top quartile score",
    }
    out: dict[str, Any] = {}
    for variant_name, label in variants.items():
        variant_payload: dict[str, Any] = {"label": label, "bucket_summaries": {}}
        for bucket in bucket_sizes:
            selected = _select_daily_bucket(
                panel,
                bucket_size=int(bucket),
                variant=variant_name,
                round_trip_cost=round_trip_cost,
            )
            summary = _cohort_summary(selected, round_trip_cost=round_trip_cost)
            variant_payload["bucket_summaries"][f"top{int(bucket)}"] = summary
        variant_payload["top10"] = variant_payload["bucket_summaries"].get("top10")
        out[variant_name] = variant_payload
    return out


def _resolve_default_output_dir(output_dir: Path | None) -> Path:
    if output_dir is not None:
        return output_dir
    return Path("tmp") / "ranking_entry_quality_backtest"


def _run_raw_backtest(
    *,
    start_date: date | None,
    end_date: date | None,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    working_dir = output_dir / "_working"
    working_dir.mkdir(parents=True, exist_ok=True)
    source_db = Path(config.DB_PATH).expanduser().resolve()
    working_db = working_dir / source_db.name
    if not working_db.exists() or working_db.stat().st_mtime < source_db.stat().st_mtime:
        shutil.copy2(source_db, working_db)
    with get_conn_for_path(str(working_db), timeout_sec=2.5, read_only=False) as conn:
        cols = {
            str(row[0]).strip().lower()
            for row in conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'daily_bars'
                ORDER BY ordinal_position
                """
            ).fetchall()
            if row and row[0] is not None
        }
        if "source" not in cols:
            conn.execute("ALTER TABLE daily_bars ADD COLUMN source TEXT")
        conn.execute("UPDATE daily_bars SET source = COALESCE(source, 'pan')")

    conn_factory = lambda: get_conn_for_path(str(working_db), timeout_sec=2.5, read_only=True)
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {"MEEMEE_ENABLE_DUCKDB_READ_ONLY": "1"}))
        stack.enter_context(patch.dict(os.environ, {"STOCKS_DB_PATH": str(working_db)}))
        stack.enter_context(patch.object(ranking_backtest_service, "get_conn", conn_factory))
        stack.enter_context(patch.object(rankings_cache, "get_conn", conn_factory))
        return ranking_backtest_service.run_raw_ranking_backtest(
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
        )


def run_ranking_entry_quality_backtest(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: Path | None = None,
    bucket_sizes: tuple[int, ...] = DEFAULT_BUCKETS,
    round_trip_cost: float = DEFAULT_ROUND_TRIP_COST,
) -> dict[str, Any]:
    root = _resolve_default_output_dir(output_dir)
    raw_payload = _run_raw_backtest(
        start_date=start_date,
        end_date=end_date,
        output_dir=root,
    )
    panel_path = root / "daily_selection_panel.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(f"panel parquet not found: {panel_path}")
    panel = pd.read_parquet(panel_path)
    if panel.empty:
        raise RuntimeError("ranking selection panel is empty")

    variants = _build_variant_report(panel, bucket_sizes=bucket_sizes, round_trip_cost=float(round_trip_cost))
    all_top10 = ((variants.get("all") or {}).get("top10")) or {}
    entry_top10 = ((variants.get("entryQualified") or {}).get("top10")) or {}
    breakout_top10 = ((variants.get("entryQualifiedBreakout") or {}).get("top10")) or {}
    highscore_top10 = ((variants.get("entryQualifiedTopQuartileScore") or {}).get("top10")) or {}
    verdict = "watch"
    entry_net20 = _safe_float((entry_top10.get("return_20_net") or {}).get("mean"))
    entry_pf20 = _safe_float((entry_top10.get("return_20_net") or {}).get("profit_factor"))
    entry_daily = _safe_float(entry_top10.get("daily_count"))
    highscore_net20 = _safe_float((highscore_top10.get("return_20_net") or {}).get("mean"))
    highscore_pf20 = _safe_float((highscore_top10.get("return_20_net") or {}).get("profit_factor"))
    breakout_net20 = _safe_float((breakout_top10.get("return_20_net") or {}).get("mean"))
    if (
        entry_net20 is not None
        and entry_pf20 is not None
        and entry_net20 > 0.0
        and entry_pf20 >= 1.2
        and entry_daily is not None
        and entry_daily >= 30
        and highscore_net20 is not None
        and highscore_net20 >= entry_net20
    ):
        verdict = "usable"
    elif entry_net20 is not None and entry_net20 > 0.0:
        verdict = "watch"
    else:
        verdict = "not_usable_yet"

    comparison = {
        "baseline_all_top10_mean20_net": _safe_float((all_top10.get("return_20_net") or {}).get("mean")),
        "baseline_entryQualified_top10_mean20_net": _safe_float((entry_top10.get("return_20_net") or {}).get("mean")),
        "entryQualified_top10_mean20_net": entry_net20,
        "entryQualified_breakout_top10_mean20_net": breakout_net20,
        "entryQualified_topQuartileScore_top10_mean20_net": highscore_net20,
    }
    comparison["lift_vs_baseline_top10_net20"] = (
        None
        if comparison["baseline_all_top10_mean20_net"] is None or entry_net20 is None
        else float(entry_net20 - comparison["baseline_all_top10_mean20_net"])
    )
    comparison["lift_vs_baseline_entryQualified_net20"] = (
        None
        if comparison["baseline_entryQualified_top10_mean20_net"] is None or entry_net20 is None
        else float(entry_net20 - comparison["baseline_entryQualified_top10_mean20_net"])
    )

    payload = {
        "schema_version": QUALITY_SCRIPT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": raw_payload.get("period"),
        "round_trip_cost": float(round_trip_cost),
        "bucket_sizes": list(bucket_sizes),
        "raw_backtest": {
            "selection_variant": raw_payload.get("selection_variant"),
            "ranking_contract": raw_payload.get("ranking_contract"),
            "cohort_metrics": raw_payload.get("cohort_metrics"),
            "coverage_metrics": raw_payload.get("coverage_metrics"),
        },
        "comparison": comparison,
        "variants": variants,
        "verdict": verdict,
    }
    write_json(root / "ranking_entry_quality_backtest.json", payload)
    (root / "ranking_entry_quality_backtest.md").write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "output_dir": str(root),
        "payload": payload,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ranking entry-quality backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--round-trip-cost", type=float, default=DEFAULT_ROUND_TRIP_COST)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_ranking_entry_quality_backtest(
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
        round_trip_cost=float(args.round_trip_cost),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
