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


AXIS_ID = "tradex_gu_regime_adaptation_v1"
DEFAULT_OUT = Path(r"G:\Tradex\tradex_gu_regime_adaptation_v1")


def load_regime(db_path: Path) -> pd.DataFrame:
    with duckdb.connect(str(db_path), read_only=True) as con:
        columns = {row[0] for row in con.execute("describe ml_feature_daily").fetchall()}
        if "breadth_above_ma20" not in columns:
            raise ValueError("FEATURE_COVERAGE_MISSING:breadth_above_ma20")
        frame = con.execute("""
            select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,
                   median(breadth_above_ma20) breadth_above_ma20
            from ml_feature_daily
            where dt between epoch(date '2024-01-01') and epoch(date '2026-12-31')
            group by dt order by signal_ymd
        """).fetchdf()
    return frame


def apply_regime_priority(scored: pd.DataFrame, regime: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    train_regime = regime[regime.signal_ymd.between(20240101, 20241231)].breadth_above_ma20.dropna()
    if train_regime.empty:
        raise ValueError("TRAIN_REGIME_COVERAGE_INSUFFICIENT")
    threshold = float(train_regime.median())
    out = scored.merge(regime, on="signal_ymd", how="left", validate="many_to_one")
    if out[out.signal_ymd.between(20240101, 20261231)].breadth_above_ma20.isna().any():
        raise ValueError("REGIME_COVERAGE_MISSING")
    out["regime_fit"] = out.breadth_above_ma20 >= threshold
    # Remove the original unconditional family bonus and restore it only in the fixed regime.
    out["score"] = out.score - out.family_hit.astype(float) * 100.0
    out["score"] += (out.family_hit & out.regime_fit).astype(float) * 100.0
    out = out.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    out["rank"] = out.groupby("signal_ymd").cumcount() + 1
    out["percentile"] = 1.0 - (out["rank"] - 1) / out.groupby("signal_ymd").code.transform("size")
    out["top10"] = out["rank"] <= 10
    return out, threshold


def generate(db_path: Path, out_root: Path) -> Path:
    features, bars, rankings, coverage = gu.load_source(db_path)
    regime = load_regime(db_path)
    shape = gu.build_gu_scores(features, bars)
    ranked, threshold = apply_regime_priority(shape, regime)
    scored = gu.attach_outcomes(ranked, bars, "BUY")
    scored = scored[scored.signal_ymd >= 20240101].copy()
    latest = int(features.signal_ymd.max())
    periods = {**gu.PERIODS, "shadow": (20260101, latest)}
    scored["split"] = np.select(
        [scored.signal_ymd < 20250101, scored.signal_ymd < 20260101],
        ["train", "validation"], default="shadow"
    )
    challenger_top = scored[scored.top10 & scored.trade_return_h10.notna()].copy()
    baseline = rankings.merge(
        scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20", "realized_mover20"]],
        on=["signal_ymd", "code"], how="left", validate="many_to_one"
    )
    baseline = baseline[baseline.trade_return_h10.notna()].copy()
    calendar = sorted(features.signal_ymd.unique().tolist())
    metrics: dict[str, Any] = {}
    branching: dict[str, Any] = {}
    rank_coverage: dict[str, Any] = {}
    regime_days: dict[str, Any] = {}
    for split, (start, end) in periods.items():
        days = sum(start <= day <= end for day in calendar)
        challenger = challenger_top[challenger_top.signal_ymd.between(start, end)]
        champion = baseline[baseline.signal_ymd.between(start, end)]
        eligible = scored[scored.signal_ymd.between(start, end) & scored.target_before_stop20.notna()]
        movers = eligible[eligible.realized_mover20.eq(1)]
        metrics[split] = {
            "challenger_top10": gu._metrics(challenger, days),
            "meemee_buy_top10": gu._metrics(champion, days),
            "recall": float(challenger.realized_mover20.sum() / movers.realized_mover20.sum()) if len(movers) else None,
            "mover_count": int(len(movers)),
            "family_hit_top10_rate": float(challenger.family_hit.mean()) if len(challenger) else None,
        }
        branching[split] = gu._branching(challenger_top, baseline, "BUY", start, end)
        rank_coverage[split] = gu._rank_coverage(scored, start, end)
        daily = regime[regime.signal_ymd.between(start, end)]
        regime_days[split] = {
            "days": int(len(daily)),
            "fit_days": int((daily.breadth_above_ma20 >= threshold).sum()),
            "fit_rate": float((daily.breadth_above_ma20 >= threshold).mean()) if len(daily) else None,
        }
    validation = metrics["validation"]["challenger_top10"]
    champion = metrics["validation"]["meemee_buy_top10"]
    br = branching["validation"]["top10"]
    gates = {
        "pf_ge_1_30": (validation["profit_factor"] or 0.0) >= 1.30,
        "pf_delta_ge_0_10": validation["profit_factor"] is not None and champion["profit_factor"] is not None and validation["profit_factor"] - champion["profit_factor"] >= 0.10,
        "calendar_expectancy_improves": validation["calendar_expectancy"] is not None and champion["calendar_expectancy"] is not None and validation["calendar_expectancy"] > champion["calendar_expectancy"],
        "cvar_non_degrade": validation["cvar10"] is not None and champion["cvar10"] is not None and validation["cvar10"] >= champion["cvar10"],
        "drawdown_non_degrade": validation["max_drawdown"] is not None and champion["max_drawdown"] is not None and validation["max_drawdown"] >= champion["max_drawdown"],
        "frequency_ge_weekly_one": validation["signals_per_week"] >= 1.0,
        "branch_ge_20pct": (br["changed_day_rate"] or 0.0) >= 0.20,
    }
    decision = "keep" if all(gates.values()) else "drop"
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    columns = ["signal_ymd", "code", "side", "prior_gap7", "ma7_distance", "above_ma20", "family_hit", "initial_signal", "initial_signal_ymd", "breadth_above_ma20", "regime_fit", "score", "rank", "percentile", "top10", "split", "target_before_stop20", "realized_mover20", "trade_return_h10"]
    scored[columns].to_parquet(root / "all_symbol_daily_scores.parquet", index=False)
    challenger_top.to_parquet(root / "challenger_top10_events.parquet", index=False)
    baseline.to_parquet(root / "meemee_buy_top10_events.parquet", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "shape": "unchanged GU first pullback: prior 1-7 gap>=3%, close within MA7 +/-3%, close>MA20",
            "axis": "one observable market regime indicator only: breadth_above_ma20",
            "regime_threshold": threshold,
            "threshold_source": "2024 daily median without outcome search",
            "priority": "GU family receives rank bonus only when breadth>=threshold; all symbols ranked on non-fit days",
            "trade_evaluation": "next-open TP8/SL5/H10/10bp; stop-first and same-bar both=loss",
            "splits": periods,
            "baseline": "formal DB ranking_appearance_daily BUY top10 ranking:trade:top50:v1",
            "shadow_tuning": False,
            "fallback": False,
        },
        "source_artifacts": [{"path": str(db_path), "sha256": gu._sha(db_path)}],
        "source_coverage": coverage,
        "regime_days": regime_days,
        "rank_coverage": rank_coverage,
        "metrics": metrics,
        "branching": branching,
        "validation_keep_gates": gates,
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "review_only",
            "reason_type": "fixed_2024_breadth_median_gu_priority_2025_validation",
        },
        "train_warning": "2024 GU family train PF was only 1.038 before adding regime adaptation; validation strength must not erase weak train evidence",
        "shadow_tuning_used": False,
        "silent_fallback_used": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    compare = root / "compare.json"
    gu._write_json(compare, payload)
    gu._write_json(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "compare": str(compare), "complete": True})
    return compare


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
