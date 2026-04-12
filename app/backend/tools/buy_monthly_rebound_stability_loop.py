from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.backend.tools import buy_monthly_rebound_relearn as rebound
from app.backend.tools import sell_path_relearn as sell_path

DEFAULT_REPORT_DIR = Path("G:/Tradex/reports")


@dataclass(frozen=True)
class BuyMonthlyReboundStabilityLoopConfig:
    lookback_days: int = rebound.DEFAULT_LOOKBACK_DAYS
    report_dir: Path = DEFAULT_REPORT_DIR
    min_rule_count: int = 20
    min_window_count: int = 5
    daily_body_mins: tuple[float, ...] = (0.04, 0.05)
    daily_close_pos_mins: tuple[float, ...] = (0.70, 0.72)
    daily_lower_wick_mins: tuple[float, ...] = (0.12, 0.14)
    daily_upper_wick_max: float = 0.12
    monthly_body_min: float = 0.06
    review_day_1: int = 3
    review_day_2: int = 5
    review_1_mfe_min: float = 0.01
    review_1_ret_min: float = 0.0
    review_2_mfe_min: float = 0.02
    review_2_ret_min: float = 0.0


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _window_label(signal_dt: pd.Series) -> pd.Series:
    signal_dt = pd.to_datetime(signal_dt.astype(str), format="%Y%m%d", errors="coerce")
    return signal_dt.dt.to_period("Q").astype(str)


def _window_stats(frame: pd.DataFrame, *, min_window_count: int) -> dict[str, Any]:
    if frame.empty:
        return {
            "window_count": 0,
            "active_window_count": 0,
            "mean20_mean": None,
            "mean20_std": None,
            "mean20_min": None,
            "mean20_p25": None,
            "win20_mean": None,
            "win20_min": None,
            "coverage_ratio": None,
        }
    work = frame.copy()
    work["window"] = _window_label(work["signal_dt"])
    window_rows: list[dict[str, Any]] = []
    for window, group in work.groupby("window", sort=True):
        if len(group) < int(min_window_count):
            continue
        long20 = pd.to_numeric(group["forward_return_20"], errors="coerce")
        window_rows.append(
            {
                "window": str(window),
                "count": int(len(group)),
                "mean20": float(long20.mean()),
                "win20": float((long20 > 0).mean()),
            }
        )
    if not window_rows:
        return {
            "window_count": 0,
            "active_window_count": 0,
            "mean20_mean": None,
            "mean20_std": None,
            "mean20_min": None,
            "mean20_p25": None,
            "win20_mean": None,
            "win20_min": None,
            "coverage_ratio": 0.0,
            "window_rows": [],
        }
    window_df = pd.DataFrame(window_rows)
    return {
        "window_count": int(work["window"].nunique()),
        "active_window_count": int(len(window_df)),
        "mean20_mean": float(window_df["mean20"].mean()),
        "mean20_std": float(window_df["mean20"].std(ddof=0)) if len(window_df) > 1 else 0.0,
        "mean20_min": float(window_df["mean20"].min()),
        "mean20_p25": float(window_df["mean20"].quantile(0.25)),
        "win20_mean": float(window_df["win20"].mean()),
        "win20_min": float(window_df["win20"].min()),
        "coverage_ratio": float(len(window_df) / max(1, int(work["window"].nunique()))),
        "window_rows": window_rows,
    }


def _bucket_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "monthly_rebound_context": frame["monthly_rebound_context"],
        "monthly_capitulation_context": frame["monthly_capitulation_context"],
        "monthly_ma_reclaim": frame["monthly_ma_reclaim"],
        "daily_reversal_up": frame["daily_reversal_up"],
        "daily_hammer_like": frame["daily_hammer_like"],
        "daily_bullish_close": frame["daily_bullish_close"],
        "monthly_bear_stack": frame["monthly_zone"] == "bear_stack",
        "monthly_bear_extension": frame["monthly_zone"] == "bear_extension",
        "monthly_sideways": frame["monthly_zone"] == "sideways",
        "monthly_bear_stack__reversal_up": (frame["monthly_zone"] == "bear_stack") & frame["daily_reversal_up"],
        "monthly_bear_extension__reversal_up": (frame["monthly_zone"] == "bear_extension") & frame["daily_reversal_up"],
        "monthly_sideways__reversal_up": frame["monthly_sideways_context"] & frame["daily_reversal_up"],
        "monthly_bear_stack__ma_reclaim": (frame["monthly_zone"] == "bear_stack") & frame["monthly_ma_reclaim"],
        "monthly_bear_extension__ma_reclaim": (frame["monthly_zone"] == "bear_extension") & frame["monthly_ma_reclaim"],
        "monthly_sideways__ma_reclaim": frame["monthly_sideways_context"] & frame["monthly_ma_reclaim"],
        "monthly_bear_stack__capitulation": (frame["monthly_zone"] == "bear_stack") & frame["monthly_capitulation_context"],
        "monthly_sideways__capitulation": frame["monthly_sideways_context"] & frame["monthly_capitulation_context"],
    }


def _regime_filter_masks(frame: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "all": pd.Series(True, index=frame.index),
        "no_risk_on_range": frame["regime_id"] != "risk_on_range",
        "no_risk_on_range_or_trend": ~frame["regime_id"].isin({"risk_on_range", "risk_on_trend"}),
        "only_neutral_or_off_or_rebound": frame["regime_id"].isin({"neutral_range", "risk_off_trend", "capitulation_rebound"}),
    }


def _summary(frame: pd.DataFrame, *, label: str, big_rebound_min: float) -> dict[str, Any]:
    return rebound._summary(frame, label=label, big_rebound_min=big_rebound_min)  # type: ignore[attr-defined]


def _evaluate_bucket(
    frame: pd.DataFrame,
    *,
    bucket: str,
    mask: pd.Series,
    min_rule_count: int,
    min_window_count: int,
    big_rebound_min: float,
    regime_filter_name: str = "all",
) -> dict[str, Any]:
    subset = frame[mask.fillna(False)].copy()
    payload = _summary(subset, label=bucket, big_rebound_min=big_rebound_min)
    payload["bucket"] = bucket
    payload["regime_filter"] = regime_filter_name
    payload["window_stats"] = _window_stats(subset, min_window_count=min_window_count)
    window_stats = payload["window_stats"]
    count = int(payload.get("count") or 0)
    mean20 = float(payload.get("mean20") or 0.0) if payload.get("mean20") is not None else 0.0
    win20 = float(payload.get("win20") or 0.0) if payload.get("win20") is not None else 0.0
    min_window_mean20 = float(window_stats.get("mean20_min") or 0.0) if window_stats.get("mean20_min") is not None else 0.0
    mean_window_mean20 = float(window_stats.get("mean20_mean") or 0.0) if window_stats.get("mean20_mean") is not None else 0.0
    std_window_mean20 = float(window_stats.get("mean20_std") or 0.0) if window_stats.get("mean20_std") is not None else 0.0
    coverage_ratio = float(window_stats.get("coverage_ratio") or 0.0) if window_stats.get("coverage_ratio") is not None else 0.0
    active_window_count = int(window_stats.get("active_window_count") or 0)
    stability_score = (
        0.05 * mean20
        + 0.35 * win20
        + 0.75 * min_window_mean20
        + 0.25 * mean_window_mean20
        - 0.20 * std_window_mean20
        + 0.10 * coverage_ratio
    )
    if count < int(min_rule_count):
        stability_score -= 10.0
    if active_window_count <= 0:
        stability_score -= 5.0
    payload["stability_score"] = float(stability_score)
    payload["mean20"] = float(mean20)
    payload["win20"] = float(win20)
    payload["min_window_mean20"] = float(min_window_mean20)
    payload["mean_window_mean20"] = float(mean_window_mean20)
    payload["std_window_mean20"] = float(std_window_mean20)
    payload["coverage_ratio"] = float(coverage_ratio)
    payload["active_window_count"] = int(active_window_count)
    return payload


def _build_candidate_rows(frame: pd.DataFrame, *, config: BuyMonthlyReboundStabilityLoopConfig) -> list[dict[str, Any]]:
    buckets = _bucket_masks(frame)
    regime_filters = _regime_filter_masks(frame)
    evaluated: list[dict[str, Any]] = []
    for bucket in (
        "monthly_bear_stack__ma_reclaim",
        "monthly_bear_stack__reversal_up",
        "monthly_rebound_context",
        "monthly_bear_extension__ma_reclaim",
        "monthly_bear_extension__reversal_up",
        "monthly_sideways__ma_reclaim",
        "monthly_sideways__reversal_up",
        "monthly_bear_stack__capitulation",
        "monthly_sideways__capitulation",
        "monthly_bear_stack",
        "monthly_bear_extension",
        "monthly_sideways",
        "daily_reversal_up",
        "daily_hammer_like",
        "daily_bullish_close",
    ):
        if bucket not in buckets:
            continue
        for regime_filter_name, regime_mask in regime_filters.items():
            combined_mask = buckets[bucket] & regime_mask
            evaluated.append(
                _evaluate_bucket(
                    frame,
                    bucket=bucket,
                    mask=combined_mask,
                    min_rule_count=int(config.min_rule_count),
                    min_window_count=int(config.min_window_count),
                    big_rebound_min=0.10,
                    regime_filter_name=regime_filter_name,
                )
            )
    evaluated.sort(
        key=lambda row: (
            float(row.get("stability_score") or -999.0),
            float(row.get("mean20") or -999.0),
            float(row.get("win20") or -999.0),
            int(row.get("count") or 0),
            str(row.get("bucket") or ""),
        ),
        reverse=True,
    )
    return evaluated


def _config_grid(config: BuyMonthlyReboundStabilityLoopConfig) -> list[rebound.BuyMonthlyReboundConfig]:
    grid: list[rebound.BuyMonthlyReboundConfig] = []
    for body_min, close_pos_min, lower_wick_min in product(
        config.daily_body_mins,
        config.daily_close_pos_mins,
        config.daily_lower_wick_mins,
    ):
        grid.append(
            rebound.BuyMonthlyReboundConfig(
                lookback_days=int(config.lookback_days),
                report_dir=Path(config.report_dir),
                min_rule_count=int(config.min_rule_count),
                daily_body_min=float(body_min),
                daily_close_pos_min=float(close_pos_min),
                daily_lower_wick_min=float(lower_wick_min),
                daily_upper_wick_max=float(config.daily_upper_wick_max),
                monthly_body_min=float(config.monthly_body_min),
                big_rebound_min=0.10,
            )
        )
    return grid


def run_buy_monthly_rebound_stability_loop(*, config: BuyMonthlyReboundStabilityLoopConfig = BuyMonthlyReboundStabilityLoopConfig()) -> dict[str, Any]:
    frame, as_of_ymd = rebound._build_buy_monthly_rebound_frame(config=rebound.BuyMonthlyReboundConfig(  # type: ignore[attr-defined]
        lookback_days=int(config.lookback_days),
        report_dir=Path(config.report_dir),
        min_rule_count=int(config.min_rule_count),
    ))
    if frame.empty or as_of_ymd <= 0:
        return {"ok": False, "reason": "frame_empty"}

    candidate_runs: list[dict[str, Any]] = []
    for candidate_config in _config_grid(config):
        candidate_frame, candidate_as_of = rebound._build_buy_monthly_rebound_frame(config=candidate_config)  # type: ignore[attr-defined]
        if candidate_frame.empty or candidate_as_of <= 0:
            continue
        candidate_rows = _build_candidate_rows(candidate_frame, config=config)
        best_candidate = candidate_rows[0] if candidate_rows else {}
        candidate_runs.append(
            {
                "config": {
                    "daily_body_min": float(candidate_config.daily_body_min),
                    "daily_close_pos_min": float(candidate_config.daily_close_pos_min),
                    "daily_lower_wick_min": float(candidate_config.daily_lower_wick_min),
                    "daily_upper_wick_max": float(candidate_config.daily_upper_wick_max),
                    "monthly_body_min": float(candidate_config.monthly_body_min),
                    "review_day_1": int(config.review_day_1),
                    "review_day_2": int(config.review_day_2),
                },
                "as_of_ymd": int(candidate_as_of),
                "best_candidate": best_candidate,
                "candidate_rows": candidate_rows[:10],
            }
        )

    candidate_runs = sorted(
        candidate_runs,
        key=lambda row: (
            float((row.get("best_candidate") or {}).get("stability_score") or -999.0),
            float((row.get("best_candidate") or {}).get("mean20") or -999.0),
            float((row.get("best_candidate") or {}).get("win20") or -999.0),
            int(((row.get("best_candidate") or {}).get("count")) or 0),
            json.dumps(row.get("config") or {}, ensure_ascii=False, sort_keys=True),
        ),
        reverse=True,
    )
    top_candidate = candidate_runs[0] if candidate_runs else {}
    safe_candidate = max(
        candidate_runs,
        key=lambda row: (
            float((row.get("best_candidate") or {}).get("min_window_mean20") or -999.0),
            float((row.get("best_candidate") or {}).get("mean20") or -999.0),
            float((row.get("best_candidate") or {}).get("win20") or -999.0),
            int(((row.get("best_candidate") or {}).get("count")) or 0),
            json.dumps(row.get("config") or {}, ensure_ascii=False, sort_keys=True),
        ),
    ) if candidate_runs else {}
    return {
        "ok": True,
        "as_of_ymd": int(as_of_ymd),
        "lookback_days": int(config.lookback_days),
        "min_rule_count": int(config.min_rule_count),
        "min_window_count": int(config.min_window_count),
        "candidate_count": len(candidate_runs),
        "top_candidate": top_candidate,
        "safe_candidate": safe_candidate,
        "candidate_runs": candidate_runs,
        "summary": {
            "top_bucket": (top_candidate.get("best_candidate") or {}).get("bucket"),
            "top_mean20": (top_candidate.get("best_candidate") or {}).get("mean20"),
            "top_win20": (top_candidate.get("best_candidate") or {}).get("win20"),
            "top_stability_score": (top_candidate.get("best_candidate") or {}).get("stability_score"),
            "top_window_floor": (top_candidate.get("best_candidate") or {}).get("min_window_mean20"),
            "top_window_coverage": (top_candidate.get("best_candidate") or {}).get("coverage_ratio"),
            "safe_bucket": (safe_candidate.get("best_candidate") or {}).get("bucket"),
            "safe_regime_filter": (safe_candidate.get("best_candidate") or {}).get("regime_filter"),
            "safe_mean20": (safe_candidate.get("best_candidate") or {}).get("mean20"),
            "safe_win20": (safe_candidate.get("best_candidate") or {}).get("win20"),
            "safe_window_floor": (safe_candidate.get("best_candidate") or {}).get("min_window_mean20"),
        },
    }


def _write_markdown_report(result: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# Buy Monthly Rebound Stability Loop",
        "",
        f"- ok: `{result.get('ok')}`",
        f"- as_of_ymd: `{result.get('as_of_ymd')}`",
        f"- candidate_count: `{result.get('candidate_count')}`",
        f"- top_bucket: `{(result.get('summary') or {}).get('top_bucket')}`",
        f"- top_mean20: `{sell_path._fmt_pct((result.get('summary') or {}).get('top_mean20'))}`",
        f"- top_win20: `{sell_path._fmt_pct((result.get('summary') or {}).get('top_win20'))}`",
        f"- top_stability_score: `{sell_path._fmt_num((result.get('summary') or {}).get('top_stability_score'))}`",
        f"- top_window_floor: `{sell_path._fmt_pct((result.get('summary') or {}).get('top_window_floor'))}`",
        f"- top_window_coverage: `{sell_path._fmt_pct((result.get('summary') or {}).get('top_window_coverage'))}`",
        f"- safe_bucket: `{(result.get('summary') or {}).get('safe_bucket')}`",
        f"- safe_regime_filter: `{(result.get('summary') or {}).get('safe_regime_filter')}`",
        f"- safe_mean20: `{sell_path._fmt_pct((result.get('summary') or {}).get('safe_mean20'))}`",
        f"- safe_win20: `{sell_path._fmt_pct((result.get('summary') or {}).get('safe_win20'))}`",
        f"- safe_window_floor: `{sell_path._fmt_pct((result.get('summary') or {}).get('safe_window_floor'))}`",
        "",
        "## Top Candidates",
        "",
        "| rank | bucket | count | mean20 | win20 | min_window_mean20 | stability_score | coverage_ratio | config |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for idx, row in enumerate(result.get("candidate_runs") or [], start=1):
        best = row.get("best_candidate") or {}
        cfg = row.get("config") or {}
        lines.append(
            "| {rank} | {bucket} | {count} | {mean20} | {win20} | {window_floor} | {score} | {coverage} | {config} |".format(
                rank=idx,
                bucket=best.get("bucket"),
                count=best.get("count"),
                mean20=sell_path._fmt_pct(best.get("mean20")),
                win20=sell_path._fmt_pct(best.get("win20")),
                window_floor=sell_path._fmt_pct(best.get("min_window_mean20")),
                score=sell_path._fmt_num(best.get("stability_score")),
                coverage=sell_path._fmt_pct(best.get("coverage_ratio")),
                config=json.dumps(_jsonable(cfg), ensure_ascii=False, sort_keys=True),
            )
        )
        if idx >= 10:
            break
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json_report(result: dict[str, Any], output_path: Path) -> None:
    output_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Loop buy monthly rebound analysis to find stable winning rules")
    defaults = BuyMonthlyReboundStabilityLoopConfig()
    parser.add_argument("--lookback-days", type=int, default=defaults.lookback_days)
    parser.add_argument("--min-rule-count", type=int, default=defaults.min_rule_count)
    parser.add_argument("--min-window-count", type=int, default=defaults.min_window_count)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--prefix", default="buy_monthly_rebound_stability_loop")
    args = parser.parse_args(argv)
    result = run_buy_monthly_rebound_stability_loop(
        config=BuyMonthlyReboundStabilityLoopConfig(
            lookback_days=int(args.lookback_days),
            report_dir=Path(args.report_dir),
            min_rule_count=int(args.min_rule_count),
            min_window_count=int(args.min_window_count),
        )
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    json_path = report_dir / f"{args.prefix}_{stamp}.json"
    md_path = report_dir / f"{args.prefix}_{stamp}.md"
    _write_json_report(result, json_path)
    _write_markdown_report(result, md_path)
    print(json.dumps({"ok": result.get("ok"), "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
