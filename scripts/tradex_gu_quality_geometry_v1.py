from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

try:
    from scripts import tradex_gu_first_pullback_challenger_v1 as gu
except ImportError:
    import tradex_gu_first_pullback_challenger_v1 as gu


AXIS_ID = "tradex_gu_quality_geometry_v1"
DEFAULT_OUT = Path(r"G:\Tradex\tradex_gu_quality_geometry_v1")
VARIANTS = {
    "fast_shallow": {"age_min": 1, "age_max": 3, "depth_atr_max": 0.5},
    "balanced": {"age_min": 1, "age_max": 5, "depth_atr_max": 1.0},
    "mature": {"age_min": 4, "age_max": 7, "depth_atr_max": 1.0},
}


def load_atr(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute("""
            select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,
                   cast(code as varchar) code, atr14
            from ml_feature_daily
            where dt between epoch(date '2023-12-01') and epoch(date '2026-12-31')
            order by code, signal_ymd
        """).fetchdf()


def attach_geometry(shape: pd.DataFrame, bars: pd.DataFrame, atr: pd.DataFrame) -> pd.DataFrame:
    prices = bars[["signal_ymd", "code", "o", "c"]].copy().sort_values(["code", "signal_ymd"])
    prices["gap"] = prices.o / prices.groupby("code").c.shift(1) - 1.0
    lag_columns = []
    date_lag_columns = []
    grouped = prices.groupby("code").gap
    grouped_dates = prices.groupby("code").signal_ymd
    for lag in range(1, 8):
        column = f"gap_lag{lag}"
        prices[column] = grouped.shift(lag)
        lag_columns.append(column)
        date_column = f"date_lag{lag}"
        prices[date_column] = grouped_dates.shift(lag)
        date_lag_columns.append(date_column)
    gaps = prices[lag_columns].to_numpy(float)
    valid = np.isfinite(gaps) & (gaps >= 0.03)
    chosen = valid.argmax(axis=1)
    has_gu = valid.any(axis=1)
    dates = prices[date_lag_columns].to_numpy(float)
    prices["gu_age"] = np.where(has_gu, chosen + 1, np.nan)
    prices["gu_anchor_ymd"] = np.where(has_gu, dates[np.arange(len(prices)), chosen], np.nan)
    out = shape.merge(prices[["signal_ymd", "code", "gu_age", "gu_anchor_ymd"]], on=["signal_ymd", "code"], how="left", validate="one_to_one")
    out = out.merge(atr, on=["signal_ymd", "code"], how="left", validate="one_to_one")
    out["pullback_depth_atr"] = (out.close - out.ma7).abs() / out.atr14
    return out


def rank_variant(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    spec = VARIANTS[name]
    out = frame.copy().sort_values(["code", "signal_ymd"])
    out["quality_hit"] = (
        out.family_hit
        & out.gu_age.between(spec["age_min"], spec["age_max"])
        & out.pullback_depth_atr.le(spec["depth_atr_max"])
    )
    previous_quality = out.groupby("code").quality_hit.shift(1, fill_value=False)
    previous_anchor = out.groupby("code").gu_anchor_ymd.shift(1)
    out["setup_initial_signal"] = out.quality_hit & (~previous_quality | out.gu_anchor_ymd.ne(previous_anchor))
    out["setup_initial_signal_ymd"] = out.signal_ymd.where(out.setup_initial_signal).astype("Int64")
    base_score = out.score - out.family_hit.astype(float) * 100.0
    age_center = (spec["age_min"] + spec["age_max"]) / 2.0
    age_fit = (1.0 - (out.gu_age - age_center).abs() / 3.0).clip(-1.0, 1.0).fillna(-1.0)
    depth_fit = (1.0 - out.pullback_depth_atr / spec["depth_atr_max"]).clip(-2.0, 1.0).fillna(-2.0)
    out["score"] = base_score + out.quality_hit.astype(float) * 100.0 + age_fit + depth_fit
    out = out.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    out["rank"] = out.groupby("signal_ymd").cumcount() + 1
    out["percentile"] = 1.0 - (out["rank"] - 1) / out.groupby("signal_ymd").code.transform("size")
    out["top10"] = out["rank"] <= 10
    out["variant"] = name
    return out


def choose_variant(frame: pd.DataFrame, calendar_days: int) -> tuple[str, dict[str, Any]]:
    results: dict[str, Any] = {}
    for name in VARIANTS:
        ranked = rank_variant(frame, name)
        events = ranked[ranked.signal_ymd.between(20240101, 20241231) & ranked.top10 & ranked.trade_return_h10.notna()]
        results[name] = gu._metrics(events, calendar_days)
        results[name]["quality_hit_top10_rate"] = float(events.quality_hit.mean()) if len(events) else None
    # Prespecified deterministic selection: PF first, expectancy second, then declaration order.
    selected = max(VARIANTS, key=lambda n: (results[n]["profit_factor"] or -np.inf, results[n]["expectancy"] or -np.inf))
    return selected, results


def _period_metrics(scored: pd.DataFrame, baseline: pd.DataFrame, calendar: list[int], periods: dict[str, tuple[int, int]]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    top = scored[scored.top10 & scored.trade_return_h10.notna()].copy()
    metrics: dict[str, Any] = {}; branching: dict[str, Any] = {}; coverage: dict[str, Any] = {}
    for split, (start, end) in periods.items():
        days = sum(start <= day <= end for day in calendar)
        challenger = top[top.signal_ymd.between(start, end)]
        champion = baseline[baseline.signal_ymd.between(start, end)]
        eligible = scored[scored.signal_ymd.between(start, end) & scored.target_before_stop20.notna()]
        movers = eligible[eligible.realized_mover20.eq(1)]
        metrics[split] = {
            "challenger_top10": gu._metrics(challenger, days),
            "meemee_buy_top10": gu._metrics(champion, days),
            "precision": float(challenger.target_before_stop20.mean()) if len(challenger) else None,
            "recall": float(challenger.realized_mover20.sum() / movers.realized_mover20.sum()) if len(movers) else None,
            "mover_count": int(len(movers)),
            "quality_hit_top10_rate": float(challenger.quality_hit.mean()) if len(challenger) else None,
        }
        branching[split] = gu._branching(top, baseline, "BUY", start, end)
        coverage[split] = gu._rank_coverage(scored, start, end)
    return metrics, branching, coverage


def _adoption_gates(metrics: dict[str, Any], branching: dict[str, Any]) -> dict[str, Any]:
    gates: dict[str, Any] = {}
    for split in ("validation", "shadow"):
        challenger = metrics[split]["challenger_top10"]
        champion = metrics[split]["meemee_buy_top10"]
        gates[split] = {
            "pf_ge_1_30": (challenger["profit_factor"] or 0.0) >= 1.30,
            "expectancy_positive": (challenger["expectancy"] or 0.0) > 0.0,
            "pf_improves_vs_meemee": challenger["profit_factor"] is not None and champion["profit_factor"] is not None and challenger["profit_factor"] > champion["profit_factor"],
            "expectancy_improves_vs_meemee": challenger["expectancy"] is not None and champion["expectancy"] is not None and challenger["expectancy"] > champion["expectancy"],
            "cvar_non_degrade": challenger["cvar10"] is not None and champion["cvar10"] is not None and challenger["cvar10"] >= champion["cvar10"],
            "drawdown_non_degrade": challenger["max_drawdown"] is not None and champion["max_drawdown"] is not None and challenger["max_drawdown"] >= champion["max_drawdown"],
            "branch_ge_20pct": (branching[split]["top10"]["changed_day_rate"] or 0.0) >= 0.20,
        }
    return gates


def generate(db_path: Path, out_root: Path) -> Path:
    features, bars, rankings, source_coverage = gu.load_source(db_path)
    shape = gu.build_gu_scores(features, bars)
    geometry = attach_geometry(shape, bars, load_atr(db_path))
    with_outcomes = gu.attach_outcomes(geometry, bars, "BUY")
    calendar = sorted(features.signal_ymd.unique().tolist())
    train_days = sum(20240101 <= day <= 20241231 for day in calendar)
    selected, train_variants = choose_variant(with_outcomes, train_days)
    scored = rank_variant(with_outcomes, selected)
    scored = scored[scored.signal_ymd >= 20240101].copy()
    latest = int(features.signal_ymd.max())
    periods = {**gu.PERIODS, "shadow": (20260101, latest)}
    scored["split"] = np.select([scored.signal_ymd < 20250101, scored.signal_ymd < 20260101], ["train", "validation"], default="shadow")
    baseline = rankings.merge(
        scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20", "realized_mover20"]],
        on=["signal_ymd", "code"], how="left", validate="many_to_one"
    )
    baseline = baseline[baseline.trade_return_h10.notna()].copy()
    metrics, branching, rank_coverage = _period_metrics(scored, baseline, calendar, periods)
    gates = _adoption_gates(metrics, branching)
    decision = "keep" if all(all(values.values()) for values in gates.values()) else "drop"
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    columns = ["signal_ymd", "code", "side", "family_hit", "initial_signal", "initial_signal_ymd", "gu_anchor_ymd", "gu_age", "pullback_depth_atr", "quality_hit", "setup_initial_signal", "setup_initial_signal_ymd", "variant", "score", "rank", "percentile", "top10", "split", "target_before_stop20", "realized_mover20", "trade_return_h10"]
    scored[columns].to_parquet(root / "all_symbol_daily_scores.parquet", index=False)
    scored[scored.top10 & scored.trade_return_h10.notna()].to_parquet(root / "challenger_top10_events.parquet", index=False)
    baseline.to_parquet(root / "meemee_buy_top10_events.parquet", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "shape": "unchanged GU first pullback: prior 1-7 gap>=3%, close within MA7 +/-3%, close>MA20",
            "axis": "GU age plus ATR-normalized MA7 pullback depth as one quality geometry",
            "variants": VARIANTS, "variant_selection": "2024 PF then expectancy; maximum three prespecified variants",
            "selected_variant": selected, "splits": periods,
            "ranking": "all symbols ranked daily; quality non-hit symbols retained",
            "trade_evaluation": "next-open TP8/SL5/H10/10bp; stop-first and same-bar both=loss",
            "baseline": "formal DB ranking_appearance_daily BUY top10 ranking:trade:top50:v1",
            "shadow_tuning": False, "fallback": False,
        },
        "source_artifacts": [{"path": str(db_path), "sha256": gu._sha(db_path)}],
        "source_coverage": source_coverage, "train_variant_comparison": train_variants,
        "rank_coverage": rank_coverage, "metrics": metrics, "branching": branching,
        "adoption_gates": gates,
        "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_2024_gu_quality_geometry_2025_validation_2026_shadow_gate"},
        "remaining_risks": ["Existing comparison contract counts next-open gap-through against fixed TP/SL levels; this execution bias is retained unchanged for comparability."],
        "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False,
        "production_ranking_changed": False, "meemee_changed": False,
    }
    compare = root / "compare.json"; gu._write_json(compare, payload)
    gu._write_json(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "compare": str(compare), "complete": True})
    return compare


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path, required=True); parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
