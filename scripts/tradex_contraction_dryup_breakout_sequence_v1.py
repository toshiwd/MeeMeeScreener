from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

try:
    from scripts import tradex_gu_first_pullback_challenger_v1 as common
except ImportError:
    import tradex_gu_first_pullback_challenger_v1 as common


AXIS_ID = "tradex_contraction_dryup_breakout_sequence_v1"
DEFAULT_OUT = Path(r"G:\Tradex\tradex_contraction_dryup_breakout_sequence_v1")
WIDTH_VARIANTS = {"width_4pct": 0.04, "width_6pct": 0.06, "width_8pct": 0.08}
MAX_STAGE_GAP = 10


def load_source(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        bars = con.execute("""
            select cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,
                   cast(code as varchar) code, o,h,l,c,v
            from daily_bars where source='pan'
              and date between epoch(date '2023-10-01') and epoch(date '2026-12-31')
            order by code, signal_ymd
        """).fetchdf()
        rankings = con.execute("""
            select dt signal_ymd, cast(code as varchar) code, rank baseline_rank
            from ranking_appearance_daily
            where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and rank<=10
              and dt between 20240101 and 20261231
            order by signal_ymd, baseline_rank, code
        """).fetchdf()
    coverage = {
        "pan_min_date": int(bars.signal_ymd.min()), "pan_max_date": int(bars.signal_ymd.max()),
        "pan_rows": int(len(bars)), "pan_codes": int(bars.code.nunique()),
        "ranking_min_date": int(rankings.signal_ymd.min()), "ranking_max_date": int(rankings.signal_ymd.max()),
        "ranking_rows_top10": int(len(rankings)),
    }
    return bars, rankings, coverage


def build_observables(bars: pd.DataFrame) -> pd.DataFrame:
    out = bars.copy().sort_values(["code", "signal_ymd"])
    grouped = out.groupby("code", group_keys=False)
    prior10_high = grouped.h.transform(lambda s: s.shift(1).rolling(10, min_periods=10).max())
    prior10_low = grouped.l.transform(lambda s: s.shift(1).rolling(10, min_periods=10).min())
    out["prior10_width"] = (prior10_high - prior10_low) / out.c
    out["vol_ratio5_20"] = grouped.v.transform(lambda s: s.rolling(5, min_periods=5).mean() / s.rolling(20, min_periods=20).mean())
    out["prior20_high"] = grouped.h.transform(lambda s: s.shift(1).rolling(20, min_periods=20).max())
    out["range_pct"] = (out.h - out.l) / out.c
    out["prior5_range_median"] = grouped.apply(
        lambda p: p.assign(_value=((p.h - p.l) / p.c).shift(1).rolling(5, min_periods=5).median())
    ).reset_index(drop=True)._value.to_numpy()
    out["dry_hit"] = out.vol_ratio5_20 <= 0.8
    out["breakout_hit"] = (out.c > out.prior20_high) & (out.range_pct >= 1.2 * out.prior5_range_median)
    return out


def sequence_variant(observables: pd.DataFrame, name: str) -> pd.DataFrame:
    width = WIDTH_VARIANTS[name]
    parts: list[pd.DataFrame] = []
    for _, part in observables.groupby("code", sort=False):
        part = part.sort_values("signal_ymd").copy().reset_index(drop=True)
        n = len(part)
        contraction = part.prior10_width.le(width).fillna(False).to_numpy(bool)
        dry = part.dry_hit.fillna(False).to_numpy(bool)
        breakout = part.breakout_hit.fillna(False).to_numpy(bool)
        contraction_first = contraction & ~np.r_[False, contraction[:-1]]
        c_date = np.full(n, np.nan); d_date = np.full(n, np.nan); sequence = np.zeros(n, dtype=bool)
        state = 0; c_idx = -1; d_idx = -1
        for i in range(n):
            if state == 1 and i - c_idx > MAX_STAGE_GAP:
                state = 0
            if state == 2 and i - d_idx > MAX_STAGE_GAP:
                state = 0
            if contraction_first[i] and state == 0:
                state = 1; c_idx = i
            if state == 1 and i >= c_idx and dry[i]:
                state = 2; d_idx = i
            if state == 2 and i > d_idx and i - d_idx <= MAX_STAGE_GAP and breakout[i]:
                sequence[i] = True; c_date[i] = part.signal_ymd.iloc[c_idx]; d_date[i] = part.signal_ymd.iloc[d_idx]
                state = 0; c_idx = -1; d_idx = -1
        part["contraction_first"] = contraction_first
        part["sequence_hit"] = sequence
        part["contraction_first_ymd"] = pd.array(c_date, dtype="Int64")
        part["dry_first_ymd"] = pd.array(d_date, dtype="Int64")
        part["breakout_first_ymd"] = part.signal_ymd.where(part.sequence_hit).astype("Int64")
        parts.append(part)
    out = pd.concat(parts, ignore_index=True)
    contraction_fit = (1.0 - out.prior10_width / width).clip(-3.0, 1.0).fillna(-3.0)
    dry_fit = (1.0 - out.vol_ratio5_20 / 0.8).clip(-3.0, 1.0).fillna(-3.0)
    price_expand = (out.c / out.prior20_high - 1.0).clip(-0.10, 0.10).fillna(-0.10) / 0.02
    range_expand = (out.range_pct / out.prior5_range_median - 1.2).clip(-2.0, 2.0).fillna(-2.0)
    out["score"] = out.sequence_hit.astype(float) * 100.0 + contraction_fit + dry_fit + price_expand + range_expand
    out = out.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    out["rank"] = out.groupby("signal_ymd").cumcount() + 1
    out["percentile"] = 1.0 - (out["rank"] - 1) / out.groupby("signal_ymd").code.transform("size")
    out["top10"] = out["rank"] <= 10
    out["side"] = "BUY"; out["variant"] = name
    return out


def choose_variant(frame: pd.DataFrame, bars: pd.DataFrame, train_days: int) -> tuple[str, dict[str, Any]]:
    results: dict[str, Any] = {}
    for name in WIDTH_VARIANTS:
        ranked = common.attach_outcomes(sequence_variant(frame, name), bars, "BUY")
        events = ranked[ranked.signal_ymd.between(20240101, 20241231) & ranked.top10 & ranked.trade_return_h10.notna()]
        results[name] = common._metrics(events, train_days)
        results[name]["sequence_hit_top10_rate"] = float(events.sequence_hit.mean()) if len(events) else None
        results[name]["sequence_events"] = int(ranked[ranked.signal_ymd.between(20240101, 20241231)].sequence_hit.sum())
    selected = max(WIDTH_VARIANTS, key=lambda n: (results[n]["profit_factor"] or -np.inf, results[n]["expectancy"] or -np.inf))
    return selected, results


def adoption_gates(metrics: dict[str, Any]) -> dict[str, Any]:
    gates = {}
    for split in ("validation", "shadow"):
        challenger = metrics[split]["challenger_top10"]; champion = metrics[split]["meemee_buy_top10"]
        gates[split] = {
            "pf_ge_1_30": (challenger["profit_factor"] or 0.0) >= 1.30,
            "expectancy_positive": (challenger["expectancy"] or 0.0) > 0,
            "pf_improves_vs_meemee": challenger["profit_factor"] is not None and champion["profit_factor"] is not None and challenger["profit_factor"] > champion["profit_factor"],
            "expectancy_improves_vs_meemee": challenger["expectancy"] is not None and champion["expectancy"] is not None and challenger["expectancy"] > champion["expectancy"],
            "cvar_non_degrade": challenger["cvar10"] is not None and champion["cvar10"] is not None and challenger["cvar10"] >= champion["cvar10"],
            "drawdown_non_degrade": challenger["max_drawdown"] is not None and champion["max_drawdown"] is not None and challenger["max_drawdown"] >= champion["max_drawdown"],
        }
    return gates


def generate(db_path: Path, out_root: Path) -> Path:
    bars, rankings, source_coverage = load_source(db_path)
    observables = build_observables(bars)
    calendar = sorted(observables.signal_ymd.unique().tolist())
    train_days = sum(20240101 <= d <= 20241231 for d in calendar)
    selected, train_variants = choose_variant(observables, bars, train_days)
    scored = common.attach_outcomes(sequence_variant(observables, selected), bars, "BUY")
    scored = scored[scored.signal_ymd >= 20240101].copy()
    latest = int(scored.signal_ymd.max()); periods = {**common.PERIODS, "shadow": (20260101, latest)}
    scored["split"] = np.select([scored.signal_ymd < 20250101, scored.signal_ymd < 20260101], ["train", "validation"], default="shadow")
    top = scored[scored.top10 & scored.trade_return_h10.notna()].copy()
    baseline = rankings.merge(scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20", "realized_mover20"]], on=["signal_ymd", "code"], how="left", validate="many_to_one")
    baseline = baseline[baseline.trade_return_h10.notna()].copy()
    metrics: dict[str, Any] = {}; branching: dict[str, Any] = {}; coverage: dict[str, Any] = {}
    for split, (start, end) in periods.items():
        days = sum(start <= d <= end for d in calendar)
        challenger = top[top.signal_ymd.between(start, end)]; champion = baseline[baseline.signal_ymd.between(start, end)]
        eligible = scored[scored.signal_ymd.between(start, end) & scored.target_before_stop20.notna()]
        movers = eligible[eligible.realized_mover20.eq(1)]
        metrics[split] = {"challenger_top10": common._metrics(challenger, days), "meemee_buy_top10": common._metrics(champion, days),
                          "recall": float(challenger.realized_mover20.sum()/movers.realized_mover20.sum()) if len(movers) else None,
                          "mover_count": int(len(movers)), "sequence_hit_top10_rate": float(challenger.sequence_hit.mean()) if len(challenger) else None,
                          "sequence_events": int(scored[scored.signal_ymd.between(start,end)].sequence_hit.sum())}
        branching[split] = common._branching(top, baseline, "BUY", start, end)
        coverage[split] = common._rank_coverage(scored, start, end)
    gates = adoption_gates(metrics); decision = "keep" if all(all(v.values()) for v in gates.values()) else "drop"
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; root.mkdir(parents=True, exist_ok=False)
    cols = ["signal_ymd","code","side","prior10_width","vol_ratio5_20","prior20_high","range_pct","prior5_range_median","contraction_first","dry_hit","breakout_hit","contraction_first_ymd","dry_first_ymd","breakout_first_ymd","sequence_hit","variant","score","rank","percentile","top10","split","target_before_stop20","realized_mover20","trade_return_h10"]
    scored[cols].to_parquet(root/"all_symbol_daily_scores.parquet",index=False); top.to_parquet(root/"challenger_top10_events.parquet",index=False); baseline.to_parquet(root/"meemee_buy_top10_events.parquet",index=False)
    payload = {"schema_version":f"{AXIS_ID}.compare.v1","artifact_role":"authoritative","axis_id":AXIS_ID,"research_phase":"effectiveness_judgment",
      "fixed_evaluation_conditions":{"axis":"PIT ordered contraction -> dry-up -> expansion breakout sequence","contraction":"prior10 high-low width / current close <= selected width","dry_up":"first later date within10 sessions with vol_ratio5_20<=0.8","breakout":"first later date within10 sessions with close>prior20 high and range_pct>=1.2*prior5 median","variants":WIDTH_VARIANTS,"selected_variant":selected,"variant_selection":"2024 PF then expectancy; width only","ranking":"all symbols daily; non-sequence retained","splits":periods,"trade_evaluation":"next-open TP8/SL5/H10/10bp; stop-first and same-bar both=loss","baseline":"formal DB MeeMee BUY top10","shadow_tuning":False,"fallback":False},
      "source_artifacts":[{"path":str(db_path),"sha256":common._sha(db_path)}],"source_coverage":source_coverage,"train_variant_comparison":train_variants,"rank_coverage":coverage,"metrics":metrics,"branching":branching,"adoption_gates":gates,
      "decision":{"candidate_local_decision":decision,"authoritative_rollup_decision":"review_only","reason_type":"fixed_2024_sequence_width_2025_validation_2026_shadow_gate"},"shadow_tuning_used":False,"silent_fallback_used":False,"runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False}
    compare=root/"compare.json";common._write_json(compare,payload);common._write_json(root/"_ARTIFACT_COMPLETE.json",{"axis_id":AXIS_ID,"compare":str(compare),"complete":True});return compare


def main()->None:
    p=argparse.ArgumentParser();p.add_argument("--db",type=Path,required=True);p.add_argument("--out",type=Path,default=DEFAULT_OUT);a=p.parse_args();print(generate(a.db,a.out))


if __name__=="__main__":main()
