from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

try:
    from scripts.tradex_early_signal_first_detect_v1 import _branching, _metrics, attach_outcomes
except ModuleNotFoundError:  # Direct execution: the script directory is already on sys.path.
    from tradex_early_signal_first_detect_v1 import _branching, _metrics, attach_outcomes


AXIS_ID = "tradex_gu_first_pullback_challenger_v1"
DEFAULT_OUT = Path(r"G:\Tradex\tradex_gu_first_pullback_challenger_v1")
PERIODS = {
    "train": (20240101, 20241231),
    "validation": (20250101, 20251231),
    "shadow": (20260101, 20261231),
}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float):
        return None if not np.isfinite(value) else value
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(value), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_source(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        required = {"dt", "code", "close", "ma7", "ma20"}
        available = {row[0] for row in con.execute("describe ml_feature_daily").fetchall()}
        missing = sorted(required - available)
        if missing:
            raise ValueError("FEATURE_COVERAGE_MISSING:" + ",".join(missing))
        features = con.execute("""
            select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,
                   cast(code as varchar) code, close, ma7, ma20
            from ml_feature_daily
            where dt between epoch(date '2023-12-01') and epoch(date '2026-12-31')
            order by code, signal_ymd
        """).fetchdf()
        bars = con.execute("""
            select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,
                   cast(code as varchar) code, o, h, l, c
            from daily_bars where source='pan'
            order by code, signal_ymd
        """).fetchdf()
        rankings = con.execute("""
            select dt signal_ymd, cast(code as varchar) code, rank baseline_rank
            from ranking_appearance_daily
            where ranking_logic_version='ranking:trade:top50:v1'
              and dir='up' and rank<=10 and dt between 20240101 and 20261231
            order by signal_ymd, baseline_rank, code
        """).fetchdf()
    coverage = {
        "feature_min_date": int(features.signal_ymd.min()),
        "feature_max_date": int(features.signal_ymd.max()),
        "feature_rows": int(len(features)),
        "feature_codes": int(features.code.nunique()),
        "pan_min_date": int(bars.signal_ymd.min()),
        "pan_max_date": int(bars.signal_ymd.max()),
        "pan_rows": int(len(bars)),
        "pan_codes": int(bars.code.nunique()),
        "ranking_min_date": int(rankings.signal_ymd.min()),
        "ranking_max_date": int(rankings.signal_ymd.max()),
        "ranking_rows_top10": int(len(rankings)),
    }
    return features, bars, rankings, coverage


def build_gu_scores(features: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    prices = bars[["signal_ymd", "code", "o", "c"]].copy().sort_values(["code", "signal_ymd"])
    prices["gap"] = prices.o / prices.groupby("code").c.shift(1) - 1.0
    # The current session is excluded: the qualifying GU must be 1-7 sessions earlier.
    prices["prior_gap7"] = prices.groupby("code").gap.transform(
        lambda s: s.shift(1).rolling(7, min_periods=1).max()
    )
    frame = features.merge(prices[["signal_ymd", "code", "prior_gap7"]], on=["signal_ymd", "code"], how="inner", validate="one_to_one")
    frame["ma7_distance"] = (frame.close / frame.ma7 - 1.0).abs()
    frame["above_ma20"] = frame.close / frame.ma20 - 1.0
    frame["family_hit"] = (
        frame.prior_gap7.ge(0.03)
        & frame.ma7_distance.le(0.03)
        & frame.above_ma20.gt(0.0)
    )
    # Fixed continuous ordering inside and outside the family. No outcome fitting is used.
    gap_component = (frame.prior_gap7.fillna(-0.03) / 0.03).clip(-1.0, 3.0)
    pullback_component = (1.0 - frame.ma7_distance.fillna(1.0) / 0.03).clip(-3.0, 1.0)
    trend_component = (frame.above_ma20.fillna(-0.10) / 0.03).clip(-3.0, 3.0)
    frame["score"] = frame.family_hit.astype(float) * 100.0 + gap_component + pullback_component + trend_component
    previous_hit = frame.groupby("code").family_hit.shift(1, fill_value=False)
    frame["initial_signal"] = frame.family_hit & ~previous_hit
    initial_dates = frame.signal_ymd.where(frame.initial_signal)
    frame["initial_signal_ymd"] = initial_dates.groupby(frame.code).ffill().astype("Int64")
    frame = frame.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    frame["rank"] = frame.groupby("signal_ymd").cumcount() + 1
    frame["percentile"] = 1.0 - (frame["rank"] - 1) / frame.groupby("signal_ymd").code.transform("size")
    frame["top10"] = frame["rank"] <= 10
    frame["side"] = "BUY"
    return frame


def _rank_coverage(scored: pd.DataFrame, start: int, end: int) -> dict[str, Any]:
    counts = scored[scored.signal_ymd.between(start, end)].groupby("signal_ymd").size()
    return {
        "days": int(len(counts)),
        "min_ranked": int(counts.min()) if len(counts) else 0,
        "median_ranked": float(counts.median()) if len(counts) else 0.0,
        "all_days_ranked": bool(len(counts) and (counts > 0).all()),
    }


def generate(db_path: Path, out_root: Path) -> Path:
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    features, bars, rankings, coverage = load_source(db_path)
    scored = attach_outcomes(build_gu_scores(features, bars), bars, "BUY")
    scored = scored[scored.signal_ymd >= 20240101].copy()
    latest = int(features.signal_ymd.max())
    periods = {**PERIODS, "shadow": (20260101, latest)}
    scored["split"] = np.select(
        [scored.signal_ymd < 20250101, scored.signal_ymd < 20260101],
        ["train", "validation"], default="shadow"
    )
    model_top = scored[scored.top10 & scored.trade_return_h10.notna()].copy()
    baseline = rankings.merge(
        scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20", "realized_mover20"]],
        on=["signal_ymd", "code"], how="left", validate="many_to_one"
    )
    baseline = baseline[baseline.trade_return_h10.notna()].copy()
    calendar = sorted(features.signal_ymd.unique().tolist())
    metrics: dict[str, Any] = {}
    branching: dict[str, Any] = {}
    rank_coverage: dict[str, Any] = {}
    for split, (start, end) in periods.items():
        days = sum(start <= day <= end for day in calendar)
        challenger = model_top[model_top.signal_ymd.between(start, end)]
        champion = baseline[baseline.signal_ymd.between(start, end)]
        eligible = scored[scored.signal_ymd.between(start, end) & scored.target_before_stop20.notna()]
        movers = eligible[eligible.realized_mover20.eq(1)]
        metrics[split] = {
            "challenger_top10": _metrics(challenger, days),
            "meemee_buy_top10": _metrics(champion, days),
            "recall": float(challenger.realized_mover20.sum() / movers.realized_mover20.sum()) if len(movers) else None,
            "mover_count": int(len(movers)),
            "family_hit_top10_rate": float(challenger.family_hit.mean()) if len(challenger) else None,
        }
        branching[split] = _branching(model_top, baseline, "BUY", start, end)
        rank_coverage[split] = _rank_coverage(scored, start, end)
    v = metrics["validation"]["challenger_top10"]
    b = metrics["validation"]["meemee_buy_top10"]
    br = branching["validation"]["top10"]
    gates = {
        "pf_ge_1_30": (v["profit_factor"] or 0.0) >= 1.30,
        "pf_delta_ge_0_10": v["profit_factor"] is not None and b["profit_factor"] is not None and v["profit_factor"] - b["profit_factor"] >= 0.10,
        "calendar_expectancy_improves": v["calendar_expectancy"] is not None and b["calendar_expectancy"] is not None and v["calendar_expectancy"] > b["calendar_expectancy"],
        "cvar_non_degrade": v["cvar10"] is not None and b["cvar10"] is not None and v["cvar10"] >= b["cvar10"],
        "drawdown_non_degrade": v["max_drawdown"] is not None and b["max_drawdown"] is not None and v["max_drawdown"] >= b["max_drawdown"],
        "frequency_ge_weekly_one": v["signals_per_week"] >= 1.0,
        "branch_ge_20pct": (br["changed_day_rate"] or 0.0) >= 0.20,
    }
    decision = "keep" if all(gates.values()) else "drop" if not gates["pf_ge_1_30"] else "hold"
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    columns = ["signal_ymd", "code", "side", "prior_gap7", "ma7_distance", "above_ma20", "family_hit", "initial_signal", "initial_signal_ymd", "score", "rank", "percentile", "top10", "split", "target_before_stop20", "realized_mover20", "trade_return_h10"]
    scored[columns].to_parquet(root / "all_symbol_daily_scores.parquet", index=False)
    model_top.to_parquet(root / "challenger_top10_events.parquet", index=False)
    baseline.to_parquet(root / "meemee_buy_top10_events.parquet", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "axis": "GU first pullback shape family only",
            "family_definition": "prior 1-7 sessions max gap>=3%; current close within MA7 +/-3%; current close>MA20",
            "initial_signal_date": "first false-to-true family transition per code; never back-selected from outcome",
            "ranking": "all feature-covered symbols ranked daily by fixed GU component score; no candidate suppression",
            "splits": periods,
            "trade_evaluation": "next-open TP8/SL5/H10/10bp; stop-first and same-bar both=loss",
            "baseline": "formal DB ranking_appearance_daily BUY top10 ranking:trade:top50:v1",
            "shadow_tuning": False,
            "fallback": False,
        },
        "source_artifacts": [{"path": str(db_path), "sha256": _sha(db_path)}],
        "source_coverage": coverage,
        "rank_coverage": rank_coverage,
        "metrics": metrics,
        "branching": branching,
        "validation_keep_gates": gates,
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "review_only",
            "reason_type": "fixed_2024_gu_first_pullback_2025_validation",
        },
        "shadow_tuning_used": False,
        "silent_fallback_used": False,
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    compare_path = root / "compare.json"
    _write_json(compare_path, payload)
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "compare": str(compare_path), "complete": True})
    return compare_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
