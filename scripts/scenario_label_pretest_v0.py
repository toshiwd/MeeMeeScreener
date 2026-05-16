from __future__ import annotations

import argparse
import json
import math
import random
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DAILY_CSV = REPO_ROOT / "production_data" / "production_daily.csv"
DEFAULT_SECTOR_CSV = REPO_ROOT / "production_data" / "sector.csv"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\scenario_label_pretest_v0")

SETUP_LABELS = [
    "uptrend_high_zone",
    "pullback_above_ma20",
    "ma7_reclaim",
    "ma20_reclaim",
    "high_zone_failure_candidate",
    "ma20_break_candidate",
    "box_breakout_attempt",
    "compression_release",
]

OUTCOME_LABELS = [
    "uptrend_continuation",
    "pullback_then_reclaim",
    "high_zone_failure",
    "ma20_breakdown",
    "range_continuation",
    "false_breakout",
    "mixed_or_unclear",
]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def _pct(numerator: float, denominator: float) -> float:
    if denominator == 0 or math.isnan(denominator):
        return float("nan")
    return (numerator / denominator - 1.0) * 100.0


def _json_default(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )


def _load_bars(path: Path, max_codes: int | None) -> pd.DataFrame:
    usecols = ["code", "date", "open", "high", "low", "close", "volume"]
    df = pd.read_csv(path, usecols=usecols, dtype={"code": "string"})
    df = df.rename(
        columns={"open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"}
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["code", "date", "o", "h", "l", "c"])
    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["o", "h", "l", "c"])
    df["code"] = df["code"].astype(str)
    if max_codes:
        codes = sorted(df["code"].unique())[:max_codes]
        df = df[df["code"].isin(codes)].copy()
    return df.sort_values(["code", "date"]).reset_index(drop=True)


def _load_sector(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["code", "sector33_code", "sector33_name"])
    sector = pd.read_csv(path, dtype={"code": "string"})
    if "code" not in sector.columns:
        return pd.DataFrame(columns=["code", "sector33_code", "sector33_name"])
    sector["code"] = sector["code"].astype(str)
    return sector[["code", "sector33_code", "sector33_name"]].drop_duplicates("code")


def _add_features(group: pd.DataFrame) -> pd.DataFrame:
    g = group.copy()
    g["ma7"] = g["c"].rolling(7, min_periods=7).mean()
    g["ma20"] = g["c"].rolling(20, min_periods=20).mean()
    g["ma60"] = g["c"].rolling(60, min_periods=60).mean()
    g["prev_c"] = g["c"].shift(1)
    g["prev_ma7"] = g["ma7"].shift(1)
    g["prev_ma20"] = g["ma20"].shift(1)
    g["prev_20_high"] = g["h"].shift(1).rolling(20, min_periods=20).max()
    g["prev_60_high"] = g["h"].shift(1).rolling(60, min_periods=60).max()
    g["vol20"] = g["v"].shift(1).rolling(20, min_periods=10).mean()
    g["range_pct"] = (g["h"] - g["l"]) / g["c"]
    g["range20"] = g["range_pct"].shift(1).rolling(20, min_periods=10).mean()
    g["range60"] = g["range_pct"].shift(1).rolling(60, min_periods=30).mean()
    g["body"] = (g["c"] - g["o"]).abs()
    g["upper_wick"] = g["h"] - g[["o", "c"]].max(axis=1)
    g["lower_wick"] = g[["o", "c"]].min(axis=1) - g["l"]
    return g


def _assign_setup_label(row: pd.Series) -> str | None:
    if row["event_high_zone_upper_wick_bearish"]:
        return "high_zone_failure_candidate"
    if row["event_ma20_break"]:
        return "ma20_break_candidate"
    if row["event_pullback_near_ma20"]:
        return "pullback_above_ma20"
    if row["event_ma7_reclaim"]:
        return "ma7_reclaim"
    if row["event_ma20_reclaim"]:
        return "ma20_reclaim"
    if row["event_compression_breakout_attempt"]:
        return "compression_release"
    if row["event_20d_high_update"] or row["event_60d_high_zone_approach"]:
        return "uptrend_high_zone"
    if row["event_volume_spike"]:
        return "box_breakout_attempt"
    return None


def _extract_anchors(bars: pd.DataFrame) -> pd.DataFrame:
    work = bars.copy()
    work["event_20d_high_update"] = work["h"] >= work["prev_20_high"]
    work["event_60d_high_zone_approach"] = work["c"] >= work["prev_60_high"] * 0.97
    work["event_ma7_reclaim"] = (work["prev_c"] < work["prev_ma7"]) & (work["c"] >= work["ma7"])
    work["event_ma20_reclaim"] = (work["prev_c"] < work["prev_ma20"]) & (work["c"] >= work["ma20"])
    work["event_ma20_break"] = (work["prev_c"] >= work["prev_ma20"]) & (work["c"] < work["ma20"])
    work["event_pullback_near_ma20"] = (
        (work["c"] >= work["ma20"])
        & (work["l"] <= work["ma20"] * 1.02)
        & (work["ma20"] > work["ma60"])
    )
    work["event_high_zone_upper_wick_bearish"] = (
        (work["c"] >= work["prev_60_high"] * 0.95)
        & (work["c"] < work["o"])
        & (work["upper_wick"] > work["body"] * 1.5)
    )
    work["event_compression_breakout_attempt"] = (
        (work["range20"] < work["range60"] * 0.75)
        & (work["c"] >= work["prev_20_high"] * 0.99)
        & (work["v"] >= work["vol20"] * 1.2)
    )
    work["event_volume_spike"] = work["v"] >= work["vol20"] * 2.0
    event_cols = [c for c in work.columns if c.startswith("event_")]
    anchors = work[work[event_cols].any(axis=1)].copy()
    anchors["setup_label"] = anchors.apply(_assign_setup_label, axis=1)
    anchors = anchors.dropna(subset=["setup_label", "ma20", "prev_60_high"])
    anchors["anchor_id"] = [f"A{i:09d}" for i in range(len(anchors))]
    anchors["broad_regime"] = anchors.apply(_broad_regime, axis=1)
    return anchors


def _broad_regime(row: pd.Series) -> str:
    if row["c"] >= row["ma20"] and row["ma20"] >= row["ma60"]:
        return "uptrend"
    if row["c"] < row["ma20"] and row["ma20"] < row["ma60"]:
        return "downtrend"
    return "mixed"


def _add_forward_outcomes(anchors: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    merged = anchors.dropna(
        subset=["future_c_5", "future_c_10", "future_c_20", "future_low_20", "future_high_20"]
    ).copy()
    merged["forward_5d_return_pct"] = (merged["future_c_5"] / merged["c"] - 1.0) * 100.0
    merged["forward_10d_return_pct"] = (merged["future_c_10"] / merged["c"] - 1.0) * 100.0
    merged["forward_20d_return_pct"] = (merged["future_c_20"] / merged["c"] - 1.0) * 100.0
    merged["max_drawdown_20d_pct"] = (merged["future_low_20"] / merged["c"] - 1.0) * 100.0
    merged["high_update_within_20d"] = merged["future_high_20"] > merged["prev_60_high"]
    merged["ma20_break_within_20d"] = merged["future_ma20_break_count_20"] > 0
    merged["ma20_reclaim_after_break"] = (
        merged["ma20_break_within_20d"] & (merged["future_ma20_reclaim_count_20"] > 0)
    )
    merged["forward_outcome_label"] = merged.apply(
        lambda row: _assign_outcome_label(row.to_dict()), axis=1
    )
    return merged.drop(
        columns=[
            "future_c_5",
            "future_c_10",
            "future_c_20",
            "future_low_20",
            "future_high_20",
            "future_ma20_break_count_20",
            "future_ma20_reclaim_count_20",
        ]
    )


def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
    featured = (
        bars.groupby("code", group_keys=False)
        .apply(_add_features, include_groups=False)
        .reset_index(drop=True)
    )
    codes_dates = bars[["code", "date"]].reset_index(drop=True)
    featured[["code", "date"]] = codes_dates[["code", "date"]]
    forward = (
        featured.groupby("code", group_keys=False)
        .apply(_add_forward_features, include_groups=False)
        .reset_index(drop=True)
    )
    forward[["code", "date"]] = featured[["code", "date"]]
    return forward


def _future_rolling(series: pd.Series, window: int, op: str) -> pd.Series:
    future = series.shift(-1)
    rev = future.iloc[::-1]
    rolling = rev.rolling(window, min_periods=window)
    if op == "min":
        out = rolling.min()
    elif op == "max":
        out = rolling.max()
    elif op == "sum":
        out = rolling.sum()
    else:
        raise ValueError(op)
    return out.iloc[::-1]


def _add_forward_features(group: pd.DataFrame) -> pd.DataFrame:
    g = group.sort_values("date").copy()
    ma20 = g["c"].rolling(20, min_periods=20).mean()
    break_flag = (g["c"] < ma20).astype(float)
    reclaim_flag = (g["c"] >= ma20).astype(float)
    g["future_c_5"] = g["c"].shift(-5)
    g["future_c_10"] = g["c"].shift(-10)
    g["future_c_20"] = g["c"].shift(-20)
    g["future_low_20"] = _future_rolling(g["l"], 20, "min")
    g["future_high_20"] = _future_rolling(g["h"], 20, "max")
    g["future_ma20_break_count_20"] = _future_rolling(break_flag, 20, "sum")
    g["future_ma20_reclaim_count_20"] = _future_rolling(reclaim_flag, 20, "sum")
    return g


def _assign_outcome_label(row: dict[str, Any]) -> str:
    ret20 = float(row["forward_20d_return_pct"])
    dd20 = float(row["max_drawdown_20d_pct"])
    if row["high_update_within_20d"] and ret20 >= 3.0 and dd20 >= -8.0:
        return "uptrend_continuation"
    if row["ma20_break_within_20d"] and row["ma20_reclaim_after_break"] and ret20 >= 0:
        return "pullback_then_reclaim"
    if row["ma20_break_within_20d"] and ret20 <= -3.0:
        return "ma20_breakdown"
    if row["high_update_within_20d"] and ret20 <= -2.0:
        return "false_breakout"
    if ret20 <= -4.0 and dd20 <= -8.0:
        return "high_zone_failure"
    if abs(ret20) < 2.0 and dd20 > -8.0:
        return "range_continuation"
    return "mixed_or_unclear"


def _time_blocks(dates: pd.Series) -> pd.Series:
    ranks = dates.rank(method="first")
    return pd.qcut(ranks, q=3, labels=["early", "middle", "late"])


def _summarize_subset(df: pd.DataFrame, group_name: str) -> dict[str, Any]:
    if df.empty:
        return {"group": group_name, "sample_count": 0}
    return {
        "group": group_name,
        "sample_count": int(len(df)),
        "forward_5d_mean": _to_float(df["forward_5d_return_pct"].mean()),
        "forward_5d_median": _to_float(df["forward_5d_return_pct"].median()),
        "forward_10d_mean": _to_float(df["forward_10d_return_pct"].mean()),
        "forward_10d_median": _to_float(df["forward_10d_return_pct"].median()),
        "forward_20d_mean": _to_float(df["forward_20d_return_pct"].mean()),
        "forward_20d_median": _to_float(df["forward_20d_return_pct"].median()),
        "positive_20d_rate": _to_float((df["forward_20d_return_pct"] > 0).mean()),
        "max_drawdown_20d_median": _to_float(df["max_drawdown_20d_pct"].median()),
        "worst_20d_10pct": _to_float(df["forward_20d_return_pct"].quantile(0.10)),
        "ma20_break_rate_20d": _to_float(df["ma20_break_within_20d"].mean()),
        "high_update_rate_20d": _to_float(df["high_update_within_20d"].mean()),
        "outcome_distribution": {
            k: int(v) for k, v in Counter(df["forward_outcome_label"]).most_common()
        },
    }


def _delta(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    if pd.isna(left) or pd.isna(right):
        return None
    return float(left) - float(right)


def _sample_baseline(
    anchors: pd.DataFrame,
    eligible: pd.DataFrame,
    *,
    mode: str,
    seed: int,
) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = []
    eligible_by_date = {date: g for date, g in eligible.groupby("date")}
    eligible_by_regime_date = {
        (regime, date): g for (regime, date), g in eligible.groupby(["broad_regime", "date"])
    }
    for anchor in anchors.itertuples(index=False):
        if mode == "same_date":
            pool = eligible_by_date.get(anchor.date)
        elif mode == "same_broad_regime":
            pool = eligible_by_regime_date.get((anchor.broad_regime, anchor.date))
        elif mode == "ma_condition":
            pool = eligible_by_regime_date.get((anchor.broad_regime, anchor.date))
        else:
            pool = None
        if pool is None or pool.empty:
            continue
        pick = pool.iloc[rng.randrange(len(pool))]
        rows.append(pick.to_dict())
    return pd.DataFrame(rows)


def _eligible_baseline_rows(bars: pd.DataFrame) -> pd.DataFrame:
    valid = bars.dropna(subset=["ma20", "ma60", "prev_60_high"]).copy()
    valid["broad_regime"] = valid.apply(_broad_regime, axis=1)
    valid["anchor_id"] = [f"B{i:09d}" for i in range(len(valid))]
    return valid


def _period_stability(df: pd.DataFrame, baseline_by_label: dict[str, pd.DataFrame]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    work = df.copy()
    work["time_block"] = _time_blocks(work["date"])
    for label, g in work.groupby("setup_label"):
        label_blocks: dict[str, Any] = {}
        base = baseline_by_label.get(label, pd.DataFrame()).copy()
        if not base.empty:
            base["time_block"] = _time_blocks(base["date"])
        stable_positive = 0
        stable_warning = 0
        for block, gb in g.groupby("time_block"):
            bb = base[base["time_block"] == block] if not base.empty else pd.DataFrame()
            label_med = gb["forward_20d_return_pct"].median()
            base_med = bb["forward_20d_return_pct"].median() if not bb.empty else float("nan")
            delta = label_med - base_med if not math.isnan(base_med) else float("nan")
            if not math.isnan(delta) and delta > 0:
                stable_positive += 1
            if not math.isnan(delta) and delta < 0:
                stable_warning += 1
            label_blocks[str(block)] = {
                "sample_count": int(len(gb)),
                "baseline_sample_count": int(len(bb)),
                "forward_20d_median": _to_float(label_med),
                "baseline_forward_20d_median": _to_float(base_med),
                "median_delta_pct_points": _to_float(delta),
            }
        result[label] = {
            "blocks": label_blocks,
            "positive_delta_blocks": stable_positive,
            "negative_delta_blocks": stable_warning,
            "stable_positive_2_of_3": stable_positive >= 2,
            "stable_negative_2_of_3": stable_warning >= 2,
        }
    return result


def _concentration(df: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for label, g in df.groupby("setup_label"):
        code_counts = Counter(g["code"])
        sector_counts = Counter(g["sector33_name"].fillna("unknown"))
        n = len(g)
        result[label] = {
            "sample_count": int(n),
            "top_code_share": _to_float(max(code_counts.values()) / n) if n else None,
            "top_codes": [{"code": k, "count": int(v)} for k, v in code_counts.most_common(10)],
            "top_sector_share": _to_float(max(sector_counts.values()) / n) if n else None,
            "top_sectors": [
                {"sector": str(k), "count": int(v)} for k, v in sector_counts.most_common(10)
            ],
            "dominated_by_one_code": bool(n and max(code_counts.values()) / n > 0.20),
            "dominated_by_one_sector": bool(n and max(sector_counts.values()) / n > 0.40),
        }
    return result


def _decide(
    label_metrics: dict[str, Any],
    baseline_compare: dict[str, Any],
    stability: dict[str, Any],
    concentration: dict[str, Any],
) -> dict[str, Any]:
    keep: list[str] = []
    warning: list[str] = []
    drop: list[str] = []
    reasons: list[str] = []
    for label in SETUP_LABELS:
        metrics = label_metrics.get(label, {})
        compare = baseline_compare.get(label, {}).get("random_same_date", {})
        conc = concentration.get(label, {})
        stable = stability.get(label, {})
        n = int(metrics.get("sample_count") or 0)
        median_delta = compare.get("forward_20d_median_delta_pct_points")
        pos_delta = compare.get("positive_20d_rate_delta_points")
        dd_delta = compare.get("max_drawdown_20d_median_delta_pct_points")
        break_delta = compare.get("ma20_break_rate_delta_points")
        dominated = conc.get("dominated_by_one_code") or conc.get("dominated_by_one_sector")
        if (
            n >= 100
            and median_delta is not None
            and median_delta >= 0.8
            and pos_delta is not None
            and pos_delta >= 0.05
            and dd_delta is not None
            and dd_delta >= 0
            and stable.get("stable_positive_2_of_3")
            and not dominated
        ):
            keep.append(label)
            continue
        if (
            n >= 100
            and median_delta is not None
            and median_delta <= -0.8
            and break_delta is not None
            and break_delta >= 0.08
            and dd_delta is not None
            and dd_delta < 0
            and stable.get("stable_negative_2_of_3")
        ):
            warning.append(label)
            continue
        drop.append(label)
    if keep:
        decision = "keep_pretest"
        next_step = "test_entry_triggers"
        reasons.append("at_least_one_setup_label_met_keep_pretest_thresholds")
    elif warning:
        decision = "hold_redesign"
        next_step = "test_holding_review_modifier"
        reasons.append("no_keep_label_but_warning_labels_separated_breakdown_risk")
    else:
        decision = "drop_pretest"
        next_step = "stop"
        reasons.append("no_setup_label_met_keep_or_warning_thresholds")
    if drop:
        reasons.append("some_or_all_labels_failed_sample_delta_stability_or_concentration_rules")
    return {
        "decision": decision,
        "keep_labels": keep,
        "warning_labels": warning,
        "drop_labels": drop,
        "main_reasons": reasons,
        "next_recommended_step": next_step,
    }


def run(args: argparse.Namespace) -> Path:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = Path(args.output_root) / run_id
    raw_bars = _load_bars(Path(args.daily_csv), args.max_codes)
    bars = _prepare_bars(raw_bars)
    sector = _load_sector(Path(args.sector_csv))
    anchors = _extract_anchors(bars)
    anchors = _add_forward_outcomes(anchors, bars)
    anchors = anchors.merge(sector, on="code", how="left")
    eligible = _eligible_baseline_rows(bars)
    eligible = _add_forward_outcomes(eligible, bars)
    eligible = eligible.merge(sector, on="code", how="left")

    label_metrics = {
        label: _summarize_subset(anchors[anchors["setup_label"] == label], label)
        for label in SETUP_LABELS
    }

    baseline_compare: dict[str, Any] = {}
    baseline_by_label: dict[str, pd.DataFrame] = {}
    for label in SETUP_LABELS:
        label_anchors = anchors[anchors["setup_label"] == label]
        same_date = _sample_baseline(label_anchors, eligible, mode="same_date", seed=args.seed)
        same_regime = _sample_baseline(
            label_anchors, eligible, mode="same_broad_regime", seed=args.seed + 17
        )
        ma_condition = _sample_baseline(
            label_anchors, eligible, mode="ma_condition", seed=args.seed + 31
        )
        baseline_by_label[label] = same_date
        baseline_compare[label] = {}
        for name, base in [
            ("random_same_date", same_date),
            ("random_same_broad_regime", same_regime),
            ("simple_ma_condition", ma_condition),
        ]:
            base_summary = _summarize_subset(base, name)
            label_summary = label_metrics[label]
            baseline_compare[label][name] = {
                "baseline_metrics": base_summary,
                "forward_20d_median_delta_pct_points": _delta(
                    label_summary.get("forward_20d_median"),
                    base_summary.get("forward_20d_median"),
                ),
                "positive_20d_rate_delta_points": _delta(
                    label_summary.get("positive_20d_rate"),
                    base_summary.get("positive_20d_rate"),
                ),
                "max_drawdown_20d_median_delta_pct_points": _delta(
                    label_summary.get("max_drawdown_20d_median"),
                    base_summary.get("max_drawdown_20d_median"),
                ),
                "ma20_break_rate_delta_points": _delta(
                    label_summary.get("ma20_break_rate_20d"),
                    base_summary.get("ma20_break_rate_20d"),
                ),
            }
        baseline_compare[label]["champion_ranking_baseline"] = {
            "status": "not_safely_available",
            "reason": "pretest used historical bars only; no point_in_time champion ranking baseline was joined",
        }

    stability = _period_stability(anchors, baseline_by_label)
    concentration = _concentration(anchors)
    decision = _decide(label_metrics, baseline_compare, stability, concentration)

    dictionary = {
        "version": "scenario_label_pretest_v0",
        "setup_labels": SETUP_LABELS,
        "outcome_labels": OUTCOME_LABELS,
        "setup_label_contract": "past_only_features_up_to_anchor_date",
        "forward_outcome_contract": "future_bars_used_only_for_outcome_measurement",
        "event_points": [
            "20_day_high_update",
            "60_day_high_zone_approach",
            "ma7_reclaim",
            "ma20_reclaim",
            "ma20_break",
            "pullback_near_ma20",
            "high_zone_upper_wick_bearish",
            "compression_breakout_attempt",
            "volume_spike",
        ],
    }
    event_cols = [c for c in anchors.columns if c.startswith("event_")]
    anchor_summary = {
        "source_daily_csv": str(Path(args.daily_csv).resolve()),
        "source_sector_csv": str(Path(args.sector_csv).resolve()),
        "row_count": int(len(raw_bars)),
        "code_count": int(raw_bars["code"].nunique()),
        "date_min": str(raw_bars["date"].min().date()),
        "date_max": str(raw_bars["date"].max().date()),
        "anchor_count": int(len(anchors)),
        "anchor_count_by_setup_label": {
            k: int(v) for k, v in Counter(anchors["setup_label"]).most_common()
        },
        "event_counts": {col: int(anchors[col].sum()) for col in event_cols},
        "max_codes_limit": args.max_codes,
    }
    summary = {
        "run_id": run_id,
        "research_phase": "effectiveness_judgment_pretest",
        "boundary": "TRADEX-only",
        "authoritative_json": "pretest_decision.json",
        "decision": decision,
        "fixed_evaluation_conditions": {
            "same_universe": "production_daily.csv eligible daily bars",
            "same_period": [anchor_summary["date_min"], anchor_summary["date_max"]],
            "same_top_k": "not_applicable_label_pretest",
            "same_regime_condition": "reported through random_same_broad_regime baseline",
            "same_cost_slippage": "not_applicable_no_trade_execution_model",
            "same_artifact_detail_level": "label_metrics_plus_baseline_compare",
        },
        "not_changed": [
            "MeeMee UI",
            "MeeMee ranking or signal logic",
            "runtime DB",
            "TRADEX champion/challenger registry",
            "publish or promotion state",
        ],
    }

    _write_json(output_dir / "scenario_label_dictionary_v0.json", dictionary)
    _write_json(output_dir / "anchor_extraction_summary.json", anchor_summary)
    _write_json(output_dir / "setup_label_metrics.json", label_metrics)
    _write_json(output_dir / "baseline_compare.json", baseline_compare)
    _write_json(output_dir / "period_stability.json", stability)
    _write_json(output_dir / "concentration_audit.json", concentration)
    _write_json(output_dir / "pretest_decision.json", decision)
    _write_json(output_dir / "scenario_pretest_summary.json", summary)
    _write_json(
        output_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "run_id": run_id,
            "artifact_dir": str(output_dir),
            "required_artifacts": [
                "_ARTIFACT_COMPLETE.json",
                "scenario_label_dictionary_v0.json",
                "anchor_extraction_summary.json",
                "scenario_pretest_summary.json",
                "setup_label_metrics.json",
                "baseline_compare.json",
                "period_stability.json",
                "concentration_audit.json",
                "pretest_decision.json",
            ],
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    )
    return output_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TRADEX scenario label pretest v0")
    parser.add_argument("--daily-csv", default=str(DEFAULT_DAILY_CSV))
    parser.add_argument("--sector-csv", default=str(DEFAULT_SECTOR_CSV))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--seed", type=int, default=20260511)
    parser.add_argument("--max-codes", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    out = run(parse_args())
    print(out)
