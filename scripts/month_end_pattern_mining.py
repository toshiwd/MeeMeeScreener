from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path
import sys
from typing import Any

import duckdb
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config
from scripts.month_end_shape_study import ROUND_TRIP_COST_DEFAULT, _build_events, _summary_from_returns


def _period_label(entry_year: int) -> str:
    if entry_year <= 2002:
        return "1994-2002"
    if entry_year <= 2011:
        return "2003-2011"
    if entry_year <= 2019:
        return "2012-2019"
    return "2020-2026"


def _prepare_events(round_trip_cost: float) -> tuple[pd.DataFrame, dict[str, Any]]:
    with duckdb.connect(str(config.DB_PATH)) as con:
        built = _build_events(con)
    events = built.events.copy()
    if events.empty:
        return events, built.stats
    events["ret_long_net"] = events["ret_1m"] - float(round_trip_cost)
    events["ret_short_net"] = -events["ret_1m"] - float(round_trip_cost)
    entry_dt = pd.to_datetime(events["entry_date"], errors="coerce")
    events["entry_year"] = entry_dt.dt.year.fillna(0).astype(int)
    events["period"] = events["entry_year"].apply(_period_label)
    return events, built.stats


def _summarize_subset(df: pd.DataFrame, target_col: str) -> dict[str, Any]:
    arr = df[target_col].to_numpy(dtype=np.float64, copy=False)
    return _summary_from_returns(arr)


def _univariate_scan(
    events: pd.DataFrame,
    *,
    target_col: str,
    min_samples: int,
) -> list[dict[str, Any]]:
    features = ["entry_offset", "trend_bucket", "box_state", "dist_bucket", "cnt60_bucket", "cnt100_bucket"]
    out: list[dict[str, Any]] = []
    baseline = _summary_from_returns(events[target_col].to_numpy(dtype=np.float64))
    baseline_mean = float(baseline["mean"] or 0.0)
    for feat in features:
        grouped = events.groupby(feat, dropna=False)
        for value, group in grouped:
            stats = _summarize_subset(group, target_col)
            if int(stats["n"]) < int(min_samples):
                continue
            out.append(
                {
                    "feature": feat,
                    "value": str(value),
                    "n": int(stats["n"]),
                    "mean": stats["mean"],
                    "win_rate": stats["win_rate"],
                    "pf": stats["pf"],
                    "quality": stats["quality"],
                    "lift_mean_vs_all": float((stats["mean"] or 0.0) - baseline_mean),
                    "p10": stats["p10"],
                    "cvar10": stats["cvar10"],
                }
            )
    out.sort(key=lambda row: (-(row["quality"] or -999), -(row["mean"] or -999), -(row["win_rate"] or -999)))
    return out[:80]


def _pairwise_scan(
    events: pd.DataFrame,
    *,
    target_col: str,
    min_samples: int,
) -> list[dict[str, Any]]:
    features = ["entry_offset", "trend_bucket", "box_state", "dist_bucket", "cnt60_bucket", "cnt100_bucket"]
    out: list[dict[str, Any]] = []
    baseline = _summary_from_returns(events[target_col].to_numpy(dtype=np.float64))
    baseline_mean = float(baseline["mean"] or 0.0)
    for left, right in itertools.combinations(features, 2):
        grouped = events.groupby([left, right], dropna=False)
        for keys, group in grouped:
            stats = _summarize_subset(group, target_col)
            if int(stats["n"]) < int(min_samples):
                continue
            out.append(
                {
                    "feature_left": left,
                    "value_left": str(keys[0]),
                    "feature_right": right,
                    "value_right": str(keys[1]),
                    "n": int(stats["n"]),
                    "mean": stats["mean"],
                    "win_rate": stats["win_rate"],
                    "pf": stats["pf"],
                    "quality": stats["quality"],
                    "lift_mean_vs_all": float((stats["mean"] or 0.0) - baseline_mean),
                    "p10": stats["p10"],
                    "cvar10": stats["cvar10"],
                }
            )
    out.sort(key=lambda row: (-(row["quality"] or -999), -(row["mean"] or -999), -(row["win_rate"] or -999)))
    return out[:120]


def _build_atoms(events: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "trend_stack_up": events["trend_bucket"] == "stack_up",
        "trend_up_any": events["trend_bucket"].isin(["stack_up", "up"]),
        "trend_stack_down": events["trend_bucket"] == "stack_down",
        "trend_weak": events["trend_bucket"].isin(["mixed", "down", "na"]),
        "box_breakout_up": events["box_state"] == "breakout_up",
        "box_upper": events["box_state"] == "box_upper",
        "box_lower": events["box_state"] == "box_lower",
        "box_lower_4p": (events["box_state"] == "box_lower") & (events["box_months"].fillna(0) >= 4),
        "box_below": events["box_state"] == "below_box",
        "dist_near": events["dist_bucket"] == "near",
        "dist_extended": events["dist_bucket"] == "extended",
        "dist_far_below": events["dist_bucket"] == "far_below",
        "dist_overheat": events["dist_bucket"] == "overheat",
        "cnt60_lt10": events["cnt60_up"] < 10,
        "cnt60_30_99": (events["cnt60_up"] >= 30) & (events["cnt60_up"] < 100),
        "cnt60_ge100": events["cnt60_up"] >= 100,
        "cnt100_lt20": events["cnt100_up"] < 20,
        "cnt100_20_199": (events["cnt100_up"] >= 20) & (events["cnt100_up"] < 200),
        "cnt100_ge200": events["cnt100_up"] >= 200,
        "offset_m1": events["entry_offset"] == "M-1",
        "offset_m2": events["entry_offset"] == "M-2",
        "offset_m3": events["entry_offset"] == "M-3",
    }


def _evaluate_rule_stability(
    events: pd.DataFrame,
    *,
    mask: pd.Series,
    target_col: str,
    min_samples_period: int,
) -> dict[str, Any]:
    period_rows: list[dict[str, Any]] = []
    for period, group in events[mask].groupby("period"):
        stats = _summarize_subset(group, target_col)
        if int(stats["n"]) < int(min_samples_period):
            continue
        period_rows.append(
            {
                "period": str(period),
                "n": int(stats["n"]),
                "mean": stats["mean"],
                "win_rate": stats["win_rate"],
                "quality": stats["quality"],
            }
        )
    if not period_rows:
        return {
            "periods": [],
            "stable_positive_periods": 0,
            "stable_negative_periods": 0,
            "stability_score": 0.0,
        }
    pos = sum(1 for row in period_rows if (row["mean"] or 0.0) > 0.0)
    neg = sum(1 for row in period_rows if (row["mean"] or 0.0) < 0.0)
    stability_score = float((pos - neg) / max(1.0, float(len(period_rows))))
    return {
        "periods": period_rows,
        "stable_positive_periods": int(pos),
        "stable_negative_periods": int(neg),
        "stability_score": stability_score,
    }


def _rule_mining(
    events: pd.DataFrame,
    *,
    target_col: str,
    min_samples: int,
    max_atoms: int,
    min_samples_period: int,
) -> list[dict[str, Any]]:
    atoms = _build_atoms(events)
    keys = list(atoms.keys())
    baseline = _summary_from_returns(events[target_col].to_numpy(dtype=np.float64))
    baseline_mean = float(baseline["mean"] or 0.0)
    out: list[dict[str, Any]] = []
    for k in range(1, max_atoms + 1):
        for combo in itertools.combinations(keys, k):
            mask = pd.Series(True, index=events.index)
            for atom_name in combo:
                mask = mask & atoms[atom_name]
            subset = events[mask]
            stats = _summarize_subset(subset, target_col)
            n = int(stats["n"])
            if n < int(min_samples):
                continue
            stability = _evaluate_rule_stability(
                events,
                mask=mask,
                target_col=target_col,
                min_samples_period=min_samples_period,
            )
            out.append(
                {
                    "rule": " & ".join(combo),
                    "n": n,
                    "mean": stats["mean"],
                    "win_rate": stats["win_rate"],
                    "pf": stats["pf"],
                    "quality": stats["quality"],
                    "p10": stats["p10"],
                    "cvar10": stats["cvar10"],
                    "lift_mean_vs_all": float((stats["mean"] or 0.0) - baseline_mean),
                    "stability": stability,
                }
            )
    out.sort(
        key=lambda row: (
            -(row["quality"] or -999),
            -(row["stability"]["stability_score"] if isinstance(row.get("stability"), dict) else -999),
            -(row["mean"] or -999),
        )
    )
    return out[:200]


def _monthly_consistency(
    events: pd.DataFrame,
    *,
    rules: list[dict[str, Any]],
    target_col: str,
    top_n: int,
) -> list[dict[str, Any]]:
    atoms = _build_atoms(events)

    def _rule_mask(rule_text: str) -> pd.Series:
        parts = [part.strip() for part in rule_text.split("&")]
        mask = pd.Series(True, index=events.index)
        for part in parts:
            atom = atoms.get(part)
            if atom is None:
                return pd.Series(False, index=events.index)
            mask = mask & atom
        return mask

    out: list[dict[str, Any]] = []
    for row in rules[:top_n]:
        rule_text = str(row["rule"])
        mask = _rule_mask(rule_text)
        subset = events[mask]
        if subset.empty:
            continue
        monthly = subset.groupby("entry_month", as_index=False)[target_col].mean()
        month_stats = _summary_from_returns(monthly[target_col].to_numpy(dtype=np.float64, copy=False))
        out.append(
            {
                "rule": rule_text,
                "trade_level_n": int(row["n"]),
                "month_level_n": int(month_stats["n"]),
                "month_level_mean": month_stats["mean"],
                "month_level_win_rate": month_stats["win_rate"],
                "month_level_quality": month_stats["quality"],
            }
        )
    out.sort(key=lambda r: (-(r["month_level_quality"] or -999), -(r["month_level_mean"] or -999)))
    return out


def run_mining(
    *,
    round_trip_cost: float,
    min_samples_uni: int,
    min_samples_pair: int,
    min_samples_rule: int,
    min_samples_period: int,
    max_atoms: int,
) -> dict[str, Any]:
    events, stats = _prepare_events(round_trip_cost=round_trip_cost)
    if events.empty:
        return {"meta": {**stats, "round_trip_cost": round_trip_cost}, "error": "no_events"}

    long_uni = _univariate_scan(events, target_col="ret_long_net", min_samples=min_samples_uni)
    short_uni = _univariate_scan(events, target_col="ret_short_net", min_samples=min_samples_uni)
    long_pair = _pairwise_scan(events, target_col="ret_long_net", min_samples=min_samples_pair)
    short_pair = _pairwise_scan(events, target_col="ret_short_net", min_samples=min_samples_pair)
    long_rules = _rule_mining(
        events,
        target_col="ret_long_net",
        min_samples=min_samples_rule,
        max_atoms=max_atoms,
        min_samples_period=min_samples_period,
    )
    short_rules = _rule_mining(
        events,
        target_col="ret_short_net",
        min_samples=min_samples_rule,
        max_atoms=max_atoms,
        min_samples_period=min_samples_period,
    )
    long_monthly = _monthly_consistency(events, rules=long_rules, target_col="ret_long_net", top_n=40)
    short_monthly = _monthly_consistency(events, rules=short_rules, target_col="ret_short_net", top_n=40)

    overall_long = _summary_from_returns(events["ret_long_net"].to_numpy(dtype=np.float64, copy=False))
    overall_short = _summary_from_returns(events["ret_short_net"].to_numpy(dtype=np.float64, copy=False))

    return {
        "meta": {
            **stats,
            "round_trip_cost": float(round_trip_cost),
            "min_samples_uni": int(min_samples_uni),
            "min_samples_pair": int(min_samples_pair),
            "min_samples_rule": int(min_samples_rule),
            "min_samples_period": int(min_samples_period),
            "max_atoms": int(max_atoms),
            "date_min": str(events["entry_date"].min()),
            "date_max": str(events["entry_date"].max()),
        },
        "overall": {
            "long": overall_long,
            "short": overall_short,
        },
        "method_univariate": {
            "long_top": long_uni[:40],
            "short_top": short_uni[:40],
        },
        "method_pairwise": {
            "long_top": long_pair[:60],
            "short_top": short_pair[:60],
        },
        "method_rule_mining": {
            "long_top": long_rules[:80],
            "short_top": short_rules[:80],
        },
        "method_monthly_consistency": {
            "long_top": long_monthly[:30],
            "short_top": short_monthly[:30],
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Mine month-end entry patterns with multiple methods")
    parser.add_argument("--output", type=Path, default=Path("tmp/month_end_pattern_mining.json"))
    parser.add_argument("--round-trip-cost", type=float, default=ROUND_TRIP_COST_DEFAULT)
    parser.add_argument("--min-samples-uni", type=int, default=2000)
    parser.add_argument("--min-samples-pair", type=int, default=1200)
    parser.add_argument("--min-samples-rule", type=int, default=1500)
    parser.add_argument("--min-samples-period", type=int, default=200)
    parser.add_argument("--max-atoms", type=int, default=3)
    args = parser.parse_args()

    payload = run_mining(
        round_trip_cost=float(args.round_trip_cost),
        min_samples_uni=max(100, int(args.min_samples_uni)),
        min_samples_pair=max(100, int(args.min_samples_pair)),
        min_samples_rule=max(200, int(args.min_samples_rule)),
        min_samples_period=max(50, int(args.min_samples_period)),
        max_atoms=max(1, min(4, int(args.max_atoms))),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] wrote {args.output}")
    print(json.dumps(payload.get("meta", {}), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
