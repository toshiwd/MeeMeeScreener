from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from scripts import ranking_entry_quality_backtest as quality_backtest
from app.backend.services.tradex_experiment_store import write_json

DEFAULT_BUCKETS = (5, 10, 20)
DEFAULT_ROUND_TRIP_COST = 0.002
FUSION_SCRIPT_SCHEMA_VERSION = "ranking_state_fusion_backtest_v1"


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


def _score_panel(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()
    frame = panel.copy()
    frame["displayScore"] = pd.to_numeric(frame.get("displayScore"), errors="coerce")
    frame["entryQualified"] = frame["entryQualified"] == True  # noqa: E712
    max_rank = frame.groupby("as_of")["rank"].transform("max").astype(float).clip(lower=1.0)
    frame["rank_score"] = 1.0 - (frame["rank"].astype(float) - 1.0) / max_rank
    frame["displayScore_norm"] = frame.groupby("as_of")["displayScore"].transform(
        lambda s: (s - s.min()) / (s.max() - s.min()) if len(s.dropna()) > 1 and float(s.max() - s.min()) > 0.0 else 0.0
    ).fillna(0.0)
    frame["is_breakout"] = frame["setupType"].astype(str).eq("breakout")
    frame["is_watch"] = frame["setupType"].astype(str).eq("watch")
    frame["is_reject"] = frame["setupType"].astype(str).eq("reject")
    return frame


@dataclass(frozen=True)
class SelectionRecipe:
    name: str
    label: str
    mode: str
    entry_only: bool = False
    require_breakout: bool = False
    require_non_reject: bool = False
    rank_priority: tuple[str, ...] = ("rank",)
    bonus_entry_qualified: float = 0.0
    bonus_breakout: float = 0.0
    bonus_watch: float = 0.0
    penalty_reject: float = 0.0
    bonus_display_score: float = 0.0


RECIPES: tuple[SelectionRecipe, ...] = (
    SelectionRecipe(name="baseline", label="baseline top10", mode="rank"),
    SelectionRecipe(name="quality_only", label="entryQualified only", mode="filter", entry_only=True),
    SelectionRecipe(
        name="quality_breakout_gate",
        label="entryQualified + breakout gate",
        mode="filter",
        entry_only=True,
        require_breakout=True,
    ),
    SelectionRecipe(
        name="quality_non_reject_gate",
        label="entryQualified + non-reject gate",
        mode="filter",
        entry_only=True,
        require_non_reject=True,
    ),
    SelectionRecipe(
        name="breakout_first",
        label="breakout first state proxy",
        mode="priority",
        rank_priority=("state_priority", "rank"),
        bonus_entry_qualified=0.0,
        bonus_breakout=0.0,
        bonus_watch=0.0,
        penalty_reject=0.0,
        bonus_display_score=0.0,
    ),
    SelectionRecipe(
        name="bounded_prior_v1",
        label="bounded prior v1",
        mode="weighted",
        bonus_entry_qualified=0.015,
        bonus_breakout=0.050,
        bonus_watch=0.005,
        penalty_reject=-0.020,
        bonus_display_score=0.005,
    ),
    SelectionRecipe(
        name="bounded_prior_v2",
        label="bounded prior v2",
        mode="weighted",
        bonus_entry_qualified=0.020,
        bonus_breakout=0.060,
        bonus_watch=0.005,
        penalty_reject=-0.030,
        bonus_display_score=0.005,
    ),
)


def _select_for_recipe(panel: pd.DataFrame, *, bucket_size: int, recipe: SelectionRecipe) -> pd.DataFrame:
    if panel.empty:
        return panel.copy()
    frame = _score_panel(panel)

    def _pick_group(group: pd.DataFrame) -> pd.DataFrame:
        working = group.copy()
        if recipe.mode == "rank":
            pass
        elif recipe.mode == "filter":
            if recipe.entry_only:
                working = working[working["entryQualified"]].copy()
            if recipe.require_breakout:
                working = working[working["is_breakout"]].copy()
            if recipe.require_non_reject:
                working = working[~working["is_reject"]].copy()
        elif recipe.mode == "priority":
            working["state_priority"] = np.select(
                [working["is_breakout"], working["is_watch"], working["is_reject"]],
                [0, 1, 3],
                default=2,
            )
            working = working.sort_values(list(recipe.rank_priority), ascending=[True] * len(recipe.rank_priority), kind="stable")
        elif recipe.mode == "weighted":
            working["fused_score"] = (
                working["rank_score"]
                + recipe.bonus_entry_qualified * working["entryQualified"].astype(float)
                + recipe.bonus_breakout * working["is_breakout"].astype(float)
                + recipe.bonus_watch * working["is_watch"].astype(float)
                + recipe.penalty_reject * working["is_reject"].astype(float)
                + recipe.bonus_display_score * working["displayScore_norm"].astype(float)
            )
            working = working.sort_values(["fused_score", "rank"], ascending=[False, True], kind="stable")
        else:
            raise ValueError(f"unknown recipe mode: {recipe.mode}")

        if recipe.mode != "priority":
            working = working.sort_values(["rank", "displayScore", "code"], ascending=[True, False, True], kind="stable")
            if recipe.mode == "weighted":
                working = working.sort_values(["fused_score", "rank", "code"], ascending=[False, True, True], kind="stable")
            elif recipe.mode == "filter":
                working = working.sort_values(["rank", "displayScore", "code"], ascending=[True, False, True], kind="stable")

        working = working.head(int(bucket_size)).copy()
        if working.empty:
            return working
        return working

    selected: list[pd.DataFrame] = []
    for _, group in frame.groupby("as_of", sort=False):
        picked = _pick_group(group)
        if not picked.empty:
            selected.append(picked)
    if not selected:
        return frame.iloc[0:0].copy()
    return pd.concat(selected, ignore_index=True)


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


def _render_markdown(payload: dict[str, Any]) -> str:
    period = payload.get("period") if isinstance(payload.get("period"), dict) else {}
    lines = [
        "# Ranking State Fusion Backtest",
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


def _resolve_default_output_dir(output_dir: Path | None) -> Path:
    if output_dir is not None:
        return output_dir
    return Path("tmp") / "ranking_state_fusion_backtest"


def _build_variant_report(panel: pd.DataFrame, *, bucket_sizes: tuple[int, ...], round_trip_cost: float) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for recipe in RECIPES:
        variant_payload: dict[str, Any] = {"label": recipe.label, "bucket_summaries": {}}
        for bucket in bucket_sizes:
            selected = _select_for_recipe(panel, bucket_size=int(bucket), recipe=recipe)
            if not selected.empty:
                selected = selected.copy()
                selected["forward_return_5_net"] = _apply_round_trip_cost(selected["forward_return_5"], round_trip_cost=round_trip_cost)
                selected["forward_return_20_net"] = _apply_round_trip_cost(selected["forward_return_20"], round_trip_cost=round_trip_cost)
                selected["forward_return_60_net"] = _apply_round_trip_cost(selected["forward_return_60"], round_trip_cost=round_trip_cost)
            summary = _cohort_summary(selected)
            variant_payload["bucket_summaries"][f"top{int(bucket)}"] = summary
        variant_payload["top10"] = variant_payload["bucket_summaries"].get("top10")
        out[recipe.name] = variant_payload
    return out


def _run_raw_backtest(
    *,
    start_date: date | None,
    end_date: date | None,
    output_dir: Path,
) -> dict[str, Any]:
    return quality_backtest._run_raw_backtest(start_date=start_date, end_date=end_date, output_dir=output_dir)  # type: ignore[attr-defined]


def run_ranking_state_fusion_backtest(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    output_dir: Path | None = None,
    bucket_sizes: tuple[int, ...] = DEFAULT_BUCKETS,
    round_trip_cost: float = DEFAULT_ROUND_TRIP_COST,
) -> dict[str, Any]:
    root = _resolve_default_output_dir(output_dir)
    raw_payload = _run_raw_backtest(start_date=start_date, end_date=end_date, output_dir=root)
    panel_path = root / "daily_selection_panel.parquet"
    if not panel_path.exists():
        raise FileNotFoundError(f"panel parquet not found: {panel_path}")
    panel = pd.read_parquet(panel_path)
    if panel.empty:
        raise RuntimeError("ranking selection panel is empty")

    variants = _build_variant_report(panel, bucket_sizes=bucket_sizes, round_trip_cost=float(round_trip_cost))
    baseline_top10 = ((variants.get("baseline") or {}).get("top10")) or {}
    quality_top10 = ((variants.get("quality_only") or {}).get("top10")) or {}
    breakout_gate_top10 = ((variants.get("quality_breakout_gate") or {}).get("top10")) or {}
    non_reject_top10 = ((variants.get("quality_non_reject_gate") or {}).get("top10")) or {}
    bounded_v1_top10 = ((variants.get("bounded_prior_v1") or {}).get("top10")) or {}
    bounded_v2_top10 = ((variants.get("bounded_prior_v2") or {}).get("top10")) or {}

    baseline_net20 = _safe_float((baseline_top10.get("return_20_net") or {}).get("mean"))
    quality_net20 = _safe_float((quality_top10.get("return_20_net") or {}).get("mean"))
    breakout_net20 = _safe_float((breakout_gate_top10.get("return_20_net") or {}).get("mean"))
    non_reject_net20 = _safe_float((non_reject_top10.get("return_20_net") or {}).get("mean"))
    bounded_v1_net20 = _safe_float((bounded_v1_top10.get("return_20_net") or {}).get("mean"))
    bounded_v2_net20 = _safe_float((bounded_v2_top10.get("return_20_net") or {}).get("mean"))

    comparison = {
        "baseline_top10_mean20_net": baseline_net20,
        "quality_only_top10_mean20_net": quality_net20,
        "quality_breakout_gate_top10_mean20_net": breakout_net20,
        "quality_non_reject_gate_top10_mean20_net": non_reject_net20,
        "bounded_prior_v1_top10_mean20_net": bounded_v1_net20,
        "bounded_prior_v2_top10_mean20_net": bounded_v2_net20,
        "lift_vs_baseline_quality_net20": None if baseline_net20 is None or quality_net20 is None else float(quality_net20 - baseline_net20),
        "lift_vs_baseline_breakout_gate_net20": None if baseline_net20 is None or breakout_net20 is None else float(breakout_net20 - baseline_net20),
        "lift_vs_baseline_non_reject_gate_net20": None if baseline_net20 is None or non_reject_net20 is None else float(non_reject_net20 - baseline_net20),
        "lift_vs_baseline_bounded_v1_net20": None if baseline_net20 is None or bounded_v1_net20 is None else float(bounded_v1_net20 - baseline_net20),
        "lift_vs_baseline_bounded_v2_net20": None if baseline_net20 is None or bounded_v2_net20 is None else float(bounded_v2_net20 - baseline_net20),
    }

    best_variant = max(
        (
            ("quality_only", quality_net20),
            ("quality_breakout_gate", breakout_net20),
            ("quality_non_reject_gate", non_reject_net20),
            ("bounded_prior_v1", bounded_v1_net20),
            ("bounded_prior_v2", bounded_v2_net20),
        ),
        key=lambda item: float("-inf") if item[1] is None else float(item[1]),
    )
    verdict = "watch"
    if (
        best_variant[1] is not None
        and baseline_net20 is not None
        and best_variant[1] > baseline_net20
        and _safe_float((variants.get(best_variant[0]) or {}).get("top10", {}).get("return_20_net", {}).get("profit_factor")) is not None
        and _safe_float((variants.get(best_variant[0]) or {}).get("top10", {}).get("return_20_net", {}).get("profit_factor")) >= 1.2
    ):
        verdict = "usable"
    elif best_variant[1] is not None and best_variant[1] > 0.0:
        verdict = "watch"
    else:
        verdict = "not_usable_yet"

    payload = {
        "schema_version": FUSION_SCRIPT_SCHEMA_VERSION,
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
        "best_variant": best_variant[0],
        "best_variant_net20": best_variant[1],
        "verdict": verdict,
        "recommendation": "keep baseline as screen, apply only bounded prior or state gate if it improves PF without dropping net20",
    }
    write_json(root / "ranking_state_fusion_backtest.json", payload)
    (root / "ranking_state_fusion_backtest.md").write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "output_dir": str(root),
        "payload": payload,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ranking state fusion backtest")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--round-trip-cost", type=float, default=DEFAULT_ROUND_TRIP_COST)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    result = run_ranking_state_fusion_backtest(
        start_date=_parse_date(args.start_date),
        end_date=_parse_date(args.end_date),
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
        round_trip_cost=float(args.round_trip_cost),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
