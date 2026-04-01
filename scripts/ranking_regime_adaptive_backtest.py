from __future__ import annotations

import argparse
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.backend.services.tradex_experiment_store import write_json
from app.backend.services.tradex_experiment_service import (
    TRADEX_EVAL_REGIME_DOWN_TAGS,
    TRADEX_EVAL_REGIME_FLAT_TAGS,
    TRADEX_EVAL_REGIME_UP_TAGS,
)
from app.backend.services.analysis.ranking_backtest_service import _date_expr
from app.core.config import config
from app.db.session import get_conn_for_path
from scripts import ranking_entry_quality_backtest as quality_backtest
from scripts import ranking_state_fusion_backtest as fusion_backtest
from app.backend.services.analysis.strategy_backtest_service import _classify_market_regime_row

DEFAULT_BUCKETS = (5, 10, 20)
DEFAULT_ROUND_TRIP_COST = 0.002
REGIME_SCRIPT_SCHEMA_VERSION = "ranking_regime_adaptive_backtest_v1"
DEFAULT_LABEL_VERSION = "v1"


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


def _cohort_summary(panel: pd.DataFrame) -> dict[str, Any]:
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


def _coarse_regime_bucket(regime_id: str) -> str:
    raw = str(regime_id or "").strip().lower()
    if raw in TRADEX_EVAL_REGIME_UP_TAGS:
        return "up"
    if raw in TRADEX_EVAL_REGIME_DOWN_TAGS:
        return "down"
    if raw in TRADEX_EVAL_REGIME_FLAT_TAGS:
        return "flat"
    return "flat"


def _load_market_regime_lookup(
    *,
    working_db: Path,
    start_date: date | None,
    end_date: date | None,
    label_version: str,
) -> pd.DataFrame:
    start_dt = int(start_date.strftime("%Y%m%d")) if start_date is not None else None
    end_dt = int(end_date.strftime("%Y%m%d")) if end_date is not None else None
    warmup_start = start_date - timedelta(days=120) if start_date is not None else None
    if warmup_start is not None:
        start_dt = int(warmup_start.strftime("%Y%m%d"))
    dt_expr = _date_expr("b.date")
    index_dt_expr = _date_expr("b.date")
    with get_conn_for_path(str(working_db), timeout_sec=2.5, read_only=True) as conn:
        frame = conn.execute(
            f"""
            WITH base0 AS (
                SELECT
                    {dt_expr} AS dt,
                    b.code AS code,
                    b.c AS close,
                    b.h AS h,
                    b.l AS l,
                    LAG(b.c, 1) OVER (PARTITION BY b.code ORDER BY {dt_expr}) AS prev_close
                FROM daily_bars b
                WHERE b.c IS NOT NULL
                  AND {dt_expr} BETWEEN ? AND ?
            ),
            base1 AS (
                SELECT
                    dt,
                    code,
                    close,
                    prev_close,
                    AVG(close) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS ma20,
                    AVG(close) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS ma60,
                    GREATEST(
                        h - l,
                        ABS(h - COALESCE(prev_close, close)),
                        ABS(l - COALESCE(prev_close, close))
                    ) AS tr
                FROM base0
            ),
            base2 AS (
                SELECT
                    *,
                    AVG(tr) OVER (
                        PARTITION BY code
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS atr20
                FROM base1
            ),
            daily_cross AS (
                SELECT
                    dt,
                    AVG(
                        CASE
                            WHEN ma20 IS NOT NULL AND ABS(ma20) > 1e-12 AND close > ma20 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS breadth_above_ma20,
                    AVG(
                        CASE
                            WHEN ma60 IS NOT NULL AND ABS(ma60) > 1e-12 AND close > ma60 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS breadth_above_ma60,
                    AVG(
                        CASE
                            WHEN prev_close IS NOT NULL AND ABS(prev_close) > 1e-12 AND close > prev_close THEN 1.0
                            ELSE 0.0
                        END
                    ) AS advancers_ratio
                FROM base2
                GROUP BY dt
            ),
            index_base0 AS (
                SELECT
                    {index_dt_expr} AS dt,
                    b.c AS close,
                    b.h AS h,
                    b.l AS l,
                    LAG(b.c, 1) OVER (ORDER BY {index_dt_expr}) AS prev_close
                FROM daily_bars b
                WHERE b.code = '1001'
                  AND b.c IS NOT NULL
                  AND {index_dt_expr} BETWEEN ? AND ?
            ),
            index_base1 AS (
                SELECT
                    dt,
                    close,
                    AVG(close) OVER (
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS ma20,
                    AVG(close) OVER (
                        ORDER BY dt
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) AS ma60,
                    GREATEST(
                        h - l,
                        ABS(h - COALESCE(prev_close, close)),
                        ABS(l - COALESCE(prev_close, close))
                    ) AS tr
                FROM index_base0
            ),
            index_base2 AS (
                SELECT
                    *,
                    AVG(tr) OVER (
                        ORDER BY dt
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS atr20
                FROM index_base1
            ),
            index_cross AS (
                SELECT
                    dt,
                    CASE
                        WHEN ma20 IS NOT NULL AND ABS(ma20) > 1e-12 THEN (close - ma20) / ma20
                        ELSE NULL
                    END AS index_close_vs_ma20,
                    CASE
                        WHEN ma60 IS NOT NULL AND ABS(ma60) > 1e-12 THEN (close - ma60) / ma60
                        ELSE NULL
                    END AS index_close_vs_ma60,
                    CASE
                        WHEN atr20 IS NOT NULL AND close > 0 THEN atr20 / close
                        ELSE NULL
                    END AS market_atr_pct
                FROM index_base2
            )
            SELECT
                d.dt,
                d.breadth_above_ma20,
                d.breadth_above_ma60,
                d.advancers_ratio,
                x.index_close_vs_ma20,
                x.index_close_vs_ma60,
                x.market_atr_pct
            FROM daily_cross d
            LEFT JOIN index_cross x USING (dt)
            ORDER BY d.dt ASC
            """,
            [int(start_dt), int(end_dt), int(start_dt), int(end_dt)],
        ).df()

    if frame.empty:
        raise RuntimeError("market regime features could not be loaded")

    rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        regime_id, regime_score = _classify_market_regime_row(
            pd.Series(record),
            high_vol_threshold=0.04,
            high_dispersion_threshold=0.04,
        )
        rows.append(
            {
                "dt": int(record["dt"]),
                "regime_id": str(regime_id),
                "regime_score": float(regime_score),
            }
        )
    result = pd.DataFrame(rows)
    result["regime_bucket"] = result["regime_id"].map(_coarse_regime_bucket)
    return result


@dataclass(frozen=True)
class RegimePlan:
    name: str
    label: str
    up_recipe: str | None
    up_bucket: int
    flat_recipe: str | None
    flat_bucket: int
    down_recipe: str | None
    down_bucket: int
    unknown_recipe: str | None = None
    unknown_bucket: int = 0


PLAN_MAP: dict[str, RegimePlan] = {
    "baseline": RegimePlan(
        name="baseline",
        label="baseline top10",
        up_recipe="baseline",
        up_bucket=10,
        flat_recipe="baseline",
        flat_bucket=10,
        down_recipe="baseline",
        down_bucket=10,
        unknown_recipe="baseline",
        unknown_bucket=10,
    ),
    "regime_adaptive_hold": RegimePlan(
        name="regime_adaptive_hold",
        label="up=breakout_first, flat=bounded_prior_v2, down=skip",
        up_recipe="breakout_first",
        up_bucket=10,
        flat_recipe="bounded_prior_v2",
        flat_bucket=10,
        down_recipe=None,
        down_bucket=0,
        unknown_recipe="bounded_prior_v2",
        unknown_bucket=10,
    ),
    "regime_adaptive_selective": RegimePlan(
        name="regime_adaptive_selective",
        label="up=breakout_first, flat=bounded_prior_v2, down=quality_breakout_gate top5",
        up_recipe="breakout_first",
        up_bucket=10,
        flat_recipe="bounded_prior_v2",
        flat_bucket=10,
        down_recipe="quality_breakout_gate",
        down_bucket=5,
        unknown_recipe="bounded_prior_v2",
        unknown_bucket=10,
    ),
}


def _recipe_map() -> dict[str, fusion_backtest.SelectionRecipe]:
    return {recipe.name: recipe for recipe in fusion_backtest.RECIPES}


def _select_panel_for_plan(
    panel: pd.DataFrame,
    *,
    regime_lookup: pd.DataFrame | None,
    plan: RegimePlan,
    bucket_size: int,
) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()
    frame = panel.copy()
    if regime_lookup is not None and not regime_lookup.empty:
        merge_frame = regime_lookup.copy()
        if "regime_bucket" not in merge_frame.columns:
            merge_frame["regime_bucket"] = merge_frame["regime_id"].map(_coarse_regime_bucket)
        merge_frame = merge_frame[["dt", "regime_id", "regime_bucket"]].copy()
        frame = frame.merge(merge_frame, how="left", left_on="as_of", right_on="dt")
    else:
        frame["regime_id"] = "neutral_range"
        frame["regime_bucket"] = "flat"

    recipes = _recipe_map()
    selected_frames: list[pd.DataFrame] = []
    for _, group in frame.groupby("as_of", sort=False):
        regime_bucket = str(group["regime_bucket"].iloc[0] if "regime_bucket" in group.columns and not group.empty else "flat")
        if regime_bucket == "up":
            recipe_name = plan.up_recipe
            regime_bucket_size = plan.up_bucket
        elif regime_bucket == "down":
            recipe_name = plan.down_recipe
            regime_bucket_size = plan.down_bucket
        else:
            recipe_name = plan.flat_recipe
            regime_bucket_size = plan.flat_bucket
        if recipe_name is None or int(regime_bucket_size) <= 0:
            continue
        recipe = recipes.get(str(recipe_name))
        if recipe is None:
            raise KeyError(f"unknown recipe: {recipe_name}")
        effective_bucket = int(bucket_size)
        if plan.name != "baseline":
            effective_bucket = min(int(bucket_size), int(regime_bucket_size))
        picked = fusion_backtest._select_for_recipe(  # type: ignore[attr-defined]
            group.drop(columns=["dt"], errors="ignore"),
            bucket_size=int(effective_bucket),
            recipe=recipe,
        )
        if picked.empty:
            continue
        picked = picked.copy()
        picked["regime_bucket"] = regime_bucket
        selected_frames.append(picked)
    if not selected_frames:
        return frame.iloc[0:0].copy()
    combined = pd.concat(selected_frames, ignore_index=True)
    return combined


def _build_variant_report(
    panel: pd.DataFrame,
    *,
    regime_lookup: pd.DataFrame,
    bucket_sizes: tuple[int, ...],
    round_trip_cost: float,
) -> dict[str, Any]:
    variants: dict[str, Any] = {}
    for plan in PLAN_MAP.values():
        variant_payload: dict[str, Any] = {"label": plan.label, "bucket_summaries": {}}
        for bucket in bucket_sizes:
            selected = _select_panel_for_plan(
                panel,
                regime_lookup=regime_lookup,
                plan=plan,
                bucket_size=int(bucket),
            )
            if not selected.empty:
                selected = selected.copy()
                selected["forward_return_5_net"] = _apply_round_trip_cost(selected["forward_return_5"], round_trip_cost=round_trip_cost)
                selected["forward_return_20_net"] = _apply_round_trip_cost(selected["forward_return_20"], round_trip_cost=round_trip_cost)
                selected["forward_return_60_net"] = _apply_round_trip_cost(selected["forward_return_60"], round_trip_cost=round_trip_cost)
            variant_payload["bucket_summaries"][f"top{int(bucket)}"] = _cohort_summary(selected)
        variant_payload["top10"] = variant_payload["bucket_summaries"].get("top10")
        variants[plan.name] = variant_payload
    return variants


def _regime_bucket_summary(panel: pd.DataFrame) -> dict[str, Any]:
    if panel.empty or "regime_bucket" not in panel.columns:
        return {}
    out: dict[str, Any] = {}
    for bucket, group in panel.groupby("regime_bucket", sort=True):
        if not str(bucket).strip():
            continue
        out[str(bucket)] = _cohort_summary(group.copy())
    return out


def _render_markdown(payload: dict[str, Any]) -> str:
    period = payload.get("period") if isinstance(payload.get("period"), dict) else {}
    lines = [
        "# Ranking Regime Adaptive Backtest",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- period: {period.get('start_date')} .. {period.get('end_date')}",
        f"- label_version: {payload.get('label_version')}",
        f"- round_trip_cost: {payload.get('round_trip_cost')}",
        "",
        "## Verdict",
        f"- usable: {payload.get('verdict')}",
        "",
        "## Plan Summary",
    ]
    for plan_name, variant_payload in (payload.get("variants") or {}).items():
        if not isinstance(variant_payload, dict):
            continue
        top10 = variant_payload.get("top10") if isinstance(variant_payload.get("top10"), dict) else {}
        net20 = top10.get("return_20_net") if isinstance(top10, dict) else {}
        lines.append(
            f"- {plan_name}: sample={top10.get('sample_count')}, days={top10.get('daily_count')}, "
            f"net20_mean={_fmt(net20.get('mean'))}, net20_pf={_fmt(net20.get('profit_factor'))}, "
            f"net20_mdd={_fmt(net20.get('mdd'))}, top_code_share={_fmt(top10.get('concentration', {}).get('top_code_share'))}"
        )
    lines.append("")
    lines.append("## Regime Summary")
    for bucket, bucket_payload in (payload.get("regime_summary") or {}).items():
        if not isinstance(bucket_payload, dict):
            continue
        net20 = (bucket_payload.get("return_20_net") or {}) if isinstance(bucket_payload.get("return_20_net"), dict) else {}
        lines.append(
            f"- {bucket}: sample={bucket_payload.get('sample_count')}, net20_mean={_fmt(net20.get('mean'))}, "
            f"net20_pf={_fmt(net20.get('profit_factor'))}, mdd={_fmt(net20.get('mdd'))}"
        )
    lines.append("")
    return "\n".join(lines)


def _run_raw_backtest(*, start_date: date | None, end_date: date | None, output_dir: Path) -> dict[str, Any]:
    return quality_backtest._run_raw_backtest(start_date=start_date, end_date=end_date, output_dir=output_dir)  # type: ignore[attr-defined]


def _resolve_default_output_dir(output_dir: Path | None) -> Path:
    if output_dir is not None:
        return output_dir
    return Path("tmp") / "ranking_regime_adaptive_backtest"


def _resolve_existing_panel_path(panel_path: Path | None = None) -> Path | None:
    if panel_path is not None:
        return panel_path
    for candidate in (
        Path("tmp") / "ranking_state_fusion_backtest" / "daily_selection_panel.parquet",
        Path("tmp") / "ranking_entry_quality_backtest" / "daily_selection_panel.parquet",
    ):
        if candidate.exists():
            return candidate
    return None


def run_ranking_regime_adaptive_backtest(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: Path | None = None,
    panel_path: Path | None = None,
    bucket_sizes: tuple[int, ...] = DEFAULT_BUCKETS,
    round_trip_cost: float = DEFAULT_ROUND_TRIP_COST,
    label_version: str = DEFAULT_LABEL_VERSION,
) -> dict[str, Any]:
    root = _resolve_default_output_dir(output_dir)
    raw_payload: dict[str, Any] | None = None
    selected_panel_path = _resolve_existing_panel_path(panel_path)
    if selected_panel_path is None:
        raw_payload = _run_raw_backtest(start_date=start_date, end_date=end_date, output_dir=root)
        selected_panel_path = root / "daily_selection_panel.parquet"
        if not selected_panel_path.exists():
            raise FileNotFoundError(f"panel parquet not found: {selected_panel_path}")
    panel = pd.read_parquet(selected_panel_path)
    if panel.empty:
        raise RuntimeError("ranking selection panel is empty")

    source_root = selected_panel_path.parent
    working_db = source_root / "_working" / Path(config.DB_PATH).name
    if not working_db.exists():
        raise FileNotFoundError(f"working db not found: {working_db}")

    period: dict[str, Any] = {}
    if raw_payload is not None and isinstance(raw_payload.get("period"), dict):
        period = raw_payload.get("period") or {}
    else:
        period = {
            "start_date": str(int(panel["as_of"].min())),
            "end_date": str(int(panel["as_of"].max())),
        }

    analysis_start_date = start_date or _parse_date(str(period.get("start_date") or "")) or _parse_date(str(period.get("start") or ""))
    analysis_end_date = end_date or _parse_date(str(period.get("end_date") or "")) or _parse_date(str(period.get("end") or ""))
    if analysis_start_date is None or analysis_end_date is None:
        raise RuntimeError("backtest period could not be resolved from inputs or panel dates")

    regime_lookup = _load_market_regime_lookup(
        working_db=working_db,
        start_date=analysis_start_date,
        end_date=analysis_end_date,
        label_version=label_version,
    )
    regime_lookup = regime_lookup.copy()
    regime_lookup["regime_bucket"] = regime_lookup["regime_id"].map(_coarse_regime_bucket)

    variants = _build_variant_report(
        panel,
        regime_lookup=regime_lookup,
        bucket_sizes=bucket_sizes,
        round_trip_cost=float(round_trip_cost),
    )

    baseline_top10 = ((variants.get("baseline") or {}).get("top10")) or {}
    adaptive_hold_top10 = ((variants.get("regime_adaptive_hold") or {}).get("top10")) or {}
    adaptive_selective_top10 = ((variants.get("regime_adaptive_selective") or {}).get("top10")) or {}

    baseline_net20 = _safe_float((baseline_top10.get("return_20_net") or {}).get("mean"))
    hold_net20 = _safe_float((adaptive_hold_top10.get("return_20_net") or {}).get("mean"))
    selective_net20 = _safe_float((adaptive_selective_top10.get("return_20_net") or {}).get("mean"))
    baseline_pf20 = _safe_float((baseline_top10.get("return_20_net") or {}).get("profit_factor"))
    hold_pf20 = _safe_float((adaptive_hold_top10.get("return_20_net") or {}).get("profit_factor"))
    selective_pf20 = _safe_float((adaptive_selective_top10.get("return_20_net") or {}).get("profit_factor"))

    comparison = {
        "baseline_top10_mean20_net": baseline_net20,
        "regime_adaptive_hold_top10_mean20_net": hold_net20,
        "regime_adaptive_selective_top10_mean20_net": selective_net20,
        "baseline_top10_pf20": baseline_pf20,
        "regime_adaptive_hold_top10_pf20": hold_pf20,
        "regime_adaptive_selective_top10_pf20": selective_pf20,
        "lift_vs_baseline_hold_net20": None if baseline_net20 is None or hold_net20 is None else float(hold_net20 - baseline_net20),
        "lift_vs_baseline_selective_net20": None if baseline_net20 is None or selective_net20 is None else float(selective_net20 - baseline_net20),
    }

    best_variant = max(
        (
            ("baseline", baseline_net20),
            ("regime_adaptive_hold", hold_net20),
            ("regime_adaptive_selective", selective_net20),
        ),
        key=lambda item: float("-inf") if item[1] is None else float(item[1]),
    )
    best_variant_pf = _safe_float((variants.get(best_variant[0]) or {}).get("top10", {}).get("return_20_net", {}).get("profit_factor"))
    verdict = "watch"
    if (
        best_variant[1] is not None
        and baseline_net20 is not None
        and best_variant[1] > baseline_net20
        and best_variant_pf is not None
        and best_variant_pf >= 1.2
    ):
        verdict = "usable"
    elif best_variant[1] is not None and best_variant[1] > 0.0:
        verdict = "watch"
    else:
        verdict = "not_usable_yet"

    regime_frame = panel.merge(
        regime_lookup[["dt", "regime_bucket"]],
        how="left",
        left_on="as_of",
        right_on="dt",
    ).copy()
    for horizon in (5, 20, 60):
        column = f"forward_return_{horizon}"
        if column in regime_frame.columns:
            regime_frame[f"{column}_net"] = _apply_round_trip_cost(regime_frame[column], round_trip_cost=float(round_trip_cost))

    payload = {
        "schema_version": REGIME_SCRIPT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": period,
        "round_trip_cost": float(round_trip_cost),
        "label_version": str(label_version),
        "bucket_sizes": list(bucket_sizes),
        "raw_backtest": {
            "selection_variant": raw_payload.get("selection_variant") if raw_payload else None,
            "ranking_contract": raw_payload.get("ranking_contract") if raw_payload else None,
            "cohort_metrics": raw_payload.get("cohort_metrics") if raw_payload else None,
            "coverage_metrics": raw_payload.get("coverage_metrics") if raw_payload else None,
        },
        "regime_summary": _regime_bucket_summary(regime_frame),
        "comparison": comparison,
        "variants": variants,
        "best_variant": best_variant[0],
        "best_variant_net20": best_variant[1],
        "verdict": verdict,
        "recommendation": "prefer regime-adaptive gating only if it improves PF without losing top10 net20",
    }
    write_json(root / "ranking_regime_adaptive_backtest.json", payload)
    (root / "ranking_regime_adaptive_backtest.md").write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "output_dir": str(root),
        "payload": payload,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run regime adaptive ranking backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--panel-path", default=None)
    parser.add_argument("--round-trip-cost", type=float, default=DEFAULT_ROUND_TRIP_COST)
    parser.add_argument("--label-version", default=DEFAULT_LABEL_VERSION)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    run_ranking_regime_adaptive_backtest(
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
        panel_path=Path(args.panel_path).expanduser().resolve() if args.panel_path else None,
        round_trip_cost=float(args.round_trip_cost),
        label_version=str(args.label_version),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
