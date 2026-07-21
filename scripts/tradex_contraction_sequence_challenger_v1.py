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
except ModuleNotFoundError:
    from tradex_early_signal_first_detect_v1 import _branching, _metrics, attach_outcomes


AXIS_ID = "tradex_contraction_sequence_challenger_v1"
DEFAULT_OUT = Path(r"G:\Tradex\tradex_contraction_sequence_challenger_v1")
VARIANTS = (0.04, 0.06, 0.08)
PERIODS = {"train": (20240101, 20241231), "validation": (20250101, 20251231), "shadow": (20260101, 20261231)}


def _ready(v: Any) -> Any:
    if isinstance(v, dict): return {str(k): _ready(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)): return [_ready(x) for x in v]
    if isinstance(v, np.integer): return int(v)
    if isinstance(v, np.floating): return None if not np.isfinite(v) else float(v)
    if isinstance(v, float): return None if not np.isfinite(v) else v
    if isinstance(v, Path): return str(v)
    return v


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_ready(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""): h.update(block)
    return h.hexdigest()


def load_source(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        cols = {r[0] for r in con.execute("describe ml_feature_daily").fetchall()}
        required = {"dt", "code", "vol_ratio5_20", "range_pct"}
        missing = sorted(required - cols)
        if missing: raise ValueError("FEATURE_COVERAGE_MISSING:" + ",".join(missing))
        features = con.execute("""
            select cast(strftime(to_timestamp(dt),'%Y%m%d') as int) signal_ymd,
                   cast(code as varchar) code, vol_ratio5_20, range_pct
            from ml_feature_daily
            where dt between epoch(date '2023-10-01') and epoch(date '2026-12-31')
            order by code, signal_ymd
        """).fetchdf()
        bars = con.execute("""
            select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,
                   cast(code as varchar) code, o,h,l,c
            from daily_bars where source='pan' order by code, signal_ymd
        """).fetchdf()
        rankings = con.execute("""
            select dt signal_ymd, cast(code as varchar) code, rank baseline_rank
            from ranking_appearance_daily
            where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and rank<=10
              and dt between 20240101 and 20261231
            order by signal_ymd, baseline_rank, code
        """).fetchdf()
    coverage = {
        "feature_min_date": int(features.signal_ymd.min()), "feature_max_date": int(features.signal_ymd.max()),
        "feature_rows": int(len(features)), "feature_codes": int(features.code.nunique()),
        "pan_min_date": int(bars.signal_ymd.min()), "pan_max_date": int(bars.signal_ymd.max()),
        "pan_rows": int(len(bars)), "pan_codes": int(bars.code.nunique()),
        "ranking_min_date": int(rankings.signal_ymd.min()), "ranking_max_date": int(rankings.signal_ymd.max()),
        "ranking_rows_top10": int(len(rankings)),
    }
    return features, bars, rankings, coverage


def _sequence(part: pd.DataFrame, width_limit: float) -> pd.DataFrame:
    p = part.sort_values("signal_ymd").copy().reset_index(drop=True)
    prior_hi10 = p.h.shift(1).rolling(10, min_periods=10).max()
    prior_lo10 = p.l.shift(1).rolling(10, min_periods=10).min()
    p["prior10_width"] = prior_hi10 / prior_lo10 - 1.0
    p["prior20_high"] = p.h.shift(1).rolling(20, min_periods=20).max()
    p["prior5_range_median"] = p.range_pct.shift(1).rolling(5, min_periods=5).median()
    contraction_condition = p.prior10_width.le(width_limit).fillna(False).to_numpy()
    dry_condition = p.vol_ratio5_20.le(0.8).fillna(False).to_numpy()
    breakout_condition = (p.c.gt(p.prior20_high) & p.range_pct.ge(1.2 * p.prior5_range_median)).fillna(False).to_numpy()
    n = len(p); contraction = np.zeros(n, dtype=bool); dry = np.zeros(n, dtype=bool); breakout = np.zeros(n, dtype=bool)
    c_dates = np.full(n, np.nan); d_dates = np.full(n, np.nan); b_dates = np.full(n, np.nan)
    c_i: int | None = None; d_i: int | None = None
    for i in range(n):
        if c_i is None or (d_i is None and i - c_i > 10) or (d_i is not None and i - d_i > 10):
            c_i = None; d_i = None
        if c_i is None and contraction_condition[i]: c_i = i; contraction[i] = True
        if c_i is not None and d_i is None and i >= c_i and i - c_i <= 10 and dry_condition[i]: d_i = i; dry[i] = True
        if d_i is not None and i >= d_i and i - d_i <= 10 and breakout_condition[i]:
            breakout[i] = True; b_dates[i] = p.signal_ymd.iloc[i]
            c_dates[i] = p.signal_ymd.iloc[c_i]; d_dates[i] = p.signal_ymd.iloc[d_i]
            c_i = None; d_i = None
        if c_i is not None:
            c_dates[i] = p.signal_ymd.iloc[c_i]
        if d_i is not None:
            d_dates[i] = p.signal_ymd.iloc[d_i]
    p["contraction_initial"] = contraction; p["dry_up_initial"] = dry; p["breakout_initial"] = breakout
    p["contraction_ymd"] = pd.array(c_dates, dtype="Int64")
    p["dry_up_ymd"] = pd.array(d_dates, dtype="Int64")
    p["breakout_ymd"] = pd.array(b_dates, dtype="Int64")
    return p


def build_scores(features: pd.DataFrame, bars: pd.DataFrame, width_limit: float) -> pd.DataFrame:
    frame = features.merge(bars, on=["signal_ymd", "code"], how="inner", validate="one_to_one")
    out = pd.concat([_sequence(p, width_limit) for _, p in frame.groupby("code", sort=False)], ignore_index=True)
    stage = np.select([out.breakout_initial, out.dry_up_ymd.notna(), out.contraction_ymd.notna()], [3.0, 2.0, 1.0], default=0.0)
    width_quality = (width_limit - out.prior10_width.fillna(width_limit + .10)).clip(-.10, width_limit)
    dry_quality = (0.8 - out.vol_ratio5_20.fillna(2.0)).clip(-1.2, .8)
    breakout_quality = (out.c / out.prior20_high - 1.0).replace([np.inf, -np.inf], np.nan).fillna(-.2).clip(-.2, .2)
    out["family_hit"] = out.breakout_initial
    out["score"] = stage * 100.0 + width_quality * 10.0 + dry_quality + breakout_quality
    out = out.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    out["rank"] = out.groupby("signal_ymd").cumcount() + 1
    out["percentile"] = 1.0 - (out["rank"] - 1) / out.groupby("signal_ymd").code.transform("size")
    out["top10"] = out["rank"] <= 10; out["side"] = "BUY"
    return out


def _coverage(scored: pd.DataFrame, start: int, end: int) -> dict[str, Any]:
    counts = scored[scored.signal_ymd.between(start, end)].groupby("signal_ymd").size()
    return {"days": int(len(counts)), "min_ranked": int(counts.min()) if len(counts) else 0,
            "median_ranked": float(counts.median()) if len(counts) else 0.0,
            "all_days_ranked": bool(len(counts) and counts.gt(0).all())}


def generate(db_path: Path, out_root: Path) -> Path:
    if not db_path.exists(): raise FileNotFoundError(db_path)
    features, bars, rankings, coverage = load_source(db_path)
    latest = int(features.signal_ymd.max()); periods = {**PERIODS, "shadow": (20260101, latest)}
    scored_by_variant = {}
    train_variants = {}
    calendar = sorted(features[features.signal_ymd >= 20240101].signal_ymd.unique())
    train_days = sum(20240101 <= d <= 20241231 for d in calendar)
    for width in VARIANTS:
        scored = attach_outcomes(build_scores(features, bars, width), bars, "BUY")
        scored = scored[scored.signal_ymd >= 20240101].copy()
        top = scored[scored.top10 & scored.trade_return_h10.notna()]
        metric = _metrics(top[top.signal_ymd.between(20240101, 20241231)], train_days)
        key = f"width_{int(width*100):02d}pct"
        scored_by_variant[key] = scored; train_variants[key] = metric
    eligible = [(k, m) for k, m in train_variants.items() if m["expectancy"] is not None and m["expectancy"] > 0 and m["profit_factor"] is not None]
    if not eligible: raise ValueError("NO_2024_VARIANT_WITH_POSITIVE_EXPECTANCY")
    selected_key, _ = max(eligible, key=lambda x: (x[1]["profit_factor"], x[1]["expectancy"], x[0]))
    scored = scored_by_variant[selected_key]
    selected_width = int(selected_key.split("_")[1].replace("pct", "")) / 100.0
    scored["split"] = np.select([scored.signal_ymd < 20250101, scored.signal_ymd < 20260101], ["train", "validation"], default="shadow")
    model_top = scored[scored.top10 & scored.trade_return_h10.notna()].copy()
    baseline = rankings.merge(scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20", "realized_mover20"]], on=["signal_ymd", "code"], how="left", validate="many_to_one")
    baseline = baseline[baseline.trade_return_h10.notna()].copy()
    metrics = {}; branching = {}; rank_coverage = {}
    for split, (start, end) in periods.items():
        days = sum(start <= d <= end for d in calendar)
        ch = model_top[model_top.signal_ymd.between(start, end)]; base = baseline[baseline.signal_ymd.between(start, end)]
        metrics[split] = {"challenger_top10": _metrics(ch, days), "meemee_buy_top10": _metrics(base, days), "family_hit_top10_rate": float(ch.family_hit.mean()) if len(ch) else None}
        branching[split] = _branching(model_top, baseline, "BUY", start, end)
        rank_coverage[split] = _coverage(scored, start, end)
    gates = {}
    for split in ("validation", "shadow"):
        ch = metrics[split]["challenger_top10"]; base = metrics[split]["meemee_buy_top10"]
        gates[split] = {
            "pf_ge_1_30": (ch["profit_factor"] or 0) >= 1.30,
            "expectancy_positive": ch["expectancy"] is not None and ch["expectancy"] > 0,
            "pf_improves_vs_meemee": ch["profit_factor"] is not None and base["profit_factor"] is not None and ch["profit_factor"] > base["profit_factor"],
            "expectancy_improves_vs_meemee": ch["expectancy"] is not None and base["expectancy"] is not None and ch["expectancy"] > base["expectancy"],
            "cvar_non_degrade": ch["cvar10"] is not None and base["cvar10"] is not None and ch["cvar10"] >= base["cvar10"],
            "drawdown_non_degrade": ch["max_drawdown"] is not None and base["max_drawdown"] is not None and ch["max_drawdown"] >= base["max_drawdown"],
        }
    adopted = all(all(x.values()) for x in gates.values())
    decision = "keep" if adopted else ("hold" if gates["validation"]["pf_ge_1_30"] and gates["shadow"]["pf_ge_1_30"] else "drop")
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    cols = ["signal_ymd","code","side","prior10_width","vol_ratio5_20","range_pct","prior20_high","prior5_range_median","contraction_initial","dry_up_initial","breakout_initial","contraction_ymd","dry_up_ymd","breakout_ymd","family_hit","score","rank","percentile","top10","split","trade_return_h10"]
    scored[cols].to_parquet(root / "all_symbol_daily_scores.parquet", index=False)
    model_top.to_parquet(root / "challenger_top10_events.parquet", index=False); baseline.to_parquet(root / "meemee_buy_top10_events.parquet", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {"axis": "PIT contraction -> dry-up -> breakout BUY sequence", "variants_train_only": [4,6,8], "variant_selection": "2024 maximum PF among expectancy>0; tie by expectancy", "contraction": "prior 10 sessions high/low width <= selected variant", "dry_up": "first within 10 sessions after contraction with vol_ratio5_20<=0.8", "breakout": "first within 10 sessions after dry-up with close>prior 20-session high and range_pct>=1.2*prior 5-session median", "stage_dates": "first date retained; all rolling comparisons exclude current bar where specified", "ranking": "all eligible symbols ranked daily; no candidate suppression", "splits": periods, "trade_evaluation": "next-open TP8/SL5/H10/10bp; stop-first and same-bar both=loss", "baseline": "formal DB MeeMee BUY top10 ranking:trade:top50:v1", "shadow_tuning": False, "fallback": False},
        "train_variant_metrics": train_variants, "selected_variant": {"key": selected_key, "width_limit": selected_width},
        "source_artifacts": [{"path": str(db_path), "sha256": _sha(db_path)}], "source_coverage": coverage,
        "rank_coverage": rank_coverage, "metrics": metrics, "branching": branching, "adoption_gates": gates,
        "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "2024_fixed_variant_2025_validation_2026_untouched_shadow"},
        "shadow_tuning_used": False, "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    compare = root / "compare.json"; _write(compare, payload); _write(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "compare": str(compare), "complete": True})
    return compare


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--db", type=Path, required=True); parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(); print(generate(args.db, args.out))


if __name__ == "__main__": main()
