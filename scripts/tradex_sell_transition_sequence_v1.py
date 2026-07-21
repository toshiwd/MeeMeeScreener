from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from scripts import tradex_early_signal_first_detect_v1 as base
except ImportError:  # Direct execution from the scripts directory.
    import tradex_early_signal_first_detect_v1 as base


AXIS_ID = "tradex_sell_transition_sequence_v1"
DEFAULT_OUT = Path(r"G:\Tradex\sell_transition_sequence_v1")


def classify_transitions(frame: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    """Classify a fixed bearish four-step sequence using current and trailing rows only."""
    joined = frame.merge(bars, on=["code", "signal_ymd"], how="inner", validate="one_to_one")
    records: list[pd.DataFrame] = []
    for code, part in joined.sort_values("signal_ymd").groupby("code", sort=False):
        p = part.reset_index(drop=True).copy()
        hi5 = p.h.rolling(5, min_periods=5).max()
        lo5 = p.l.rolling(5, min_periods=5).min()
        prior_hi5 = hi5.shift(5)
        prior_lo5 = lo5.shift(5)
        lower_pair = (hi5 < prior_hi5) & (lo5 < prior_lo5)
        stage = np.zeros(len(p), dtype=np.int8)
        completed = np.zeros(len(p), dtype=bool)
        start_ymd = np.full(len(p), np.nan)
        bounce_ymd = np.full(len(p), np.nan)
        failure_ymd = np.full(len(p), np.nan)
        state = 0
        age = 0
        start_date = bounce_date = failure_date = np.nan
        start_prior_high = running_low = bounce_high = np.nan
        for i in range(len(p)):
            if state == 0 and bool(lower_pair.iloc[i]):
                state, age = 1, 0
                start_date = float(p.signal_ymd.iloc[i])
                start_prior_high = float(prior_hi5.iloc[i])
                running_low = float(lo5.iloc[i])
            elif state == 1:
                age += 1
                running_low = min(running_low, float(p.l.iloc[i]))
                if age <= 10 and p.c.iloc[i] >= running_low * 1.03 and p.c.iloc[i] < p.ma20.iloc[i]:
                    state, age = 2, 0
                    bounce_date = float(p.signal_ymd.iloc[i])
                    bounce_high = float(p.h.iloc[i])
                elif age > 10:
                    state = 0
            elif state == 2:
                age += 1
                bounce_high = max(bounce_high, float(p.h.iloc[i]))
                failed = bounce_high < start_prior_high and p.c.iloc[i] <= bounce_high * .98 and p.c.iloc[i] < p.ma20.iloc[i]
                if age <= 10 and failed:
                    state, age = 3, 0
                    failure_date = float(p.signal_ymd.iloc[i])
                elif age > 10:
                    state = 0
            elif state == 3:
                age += 1
                if age <= 5 and p.l.iloc[i] < running_low and p.close_ret2.iloc[i] < 0:
                    state, age = 4, 0
                    completed[i] = True
                elif age > 5:
                    state = 0
            stage[i] = state
            start_ymd[i], bounce_ymd[i], failure_ymd[i] = start_date, bounce_date, failure_date
            if state == 4:
                state = 0
                start_date = bounce_date = failure_date = np.nan
                start_prior_high = running_low = bounce_high = np.nan
        p["transition_stage"] = stage
        p["transition_complete"] = completed
        p["transition_start_ymd"] = start_ymd
        p["transition_bounce_ymd"] = bounce_ymd
        p["transition_failure_ymd"] = failure_ymd
        # Ranking covers every symbol. Stage dominates; tie-breakers are fixed past-only shape strength.
        lower_strength = ((prior_hi5 - hi5) / prior_hi5).clip(-.2, .2).fillna(-.2)
        below_ma = ((p.ma20 - p.c) / p.ma20).clip(-.2, .2).fillna(-.2)
        p["score"] = stage.astype(float) * 10.0 + lower_strength + below_ma
        records.append(p)
    out = pd.concat(records, ignore_index=True)
    out = out.sort_values(["signal_ymd", "score", "code"], ascending=[True, False, True])
    out["rank"] = out.groupby("signal_ymd").cumcount() + 1
    out["top10"] = out["rank"] <= 10
    return out


def _metrics(frame: pd.DataFrame, universe: pd.DataFrame) -> dict[str, Any]:
    valid = frame[frame.trade_return_h10.notna()].copy()
    result = base._metrics(valid, valid.signal_ymd.nunique())
    positives = int(universe.target_before_stop20.eq(1).sum())
    result["recall"] = float(valid.target_before_stop20.eq(1).sum() / positives) if positives else None
    result["rank_coverage"] = float(valid.groupby("signal_ymd").size().eq(10).mean()) if len(valid) else 0.0
    return result


def generate(db_path: Path, out_root: Path) -> Path:
    features, bars, source = base.load_source(db_path)
    rankings = source.pop("rankings")
    sell = base.attach_outcomes(base.attach_past_features(features, "SELL"), bars, "SELL")
    scored = classify_transitions(sell, bars)
    scored["side"] = "SELL"
    scored["split"] = np.select(
        [scored.signal_ymd < 20250101, scored.signal_ymd < 20260101],
        ["train", "validation"], default="shadow",
    )
    top = scored[scored.top10].copy()
    baseline = rankings[rankings.dir.eq("down")].rename(columns={"rank": "baseline_rank"})
    baseline["side"] = "SELL"
    baseline = baseline.merge(
        scored[["signal_ymd", "code", "side", "trade_return_h10", "target_before_stop20"]],
        on=["signal_ymd", "code", "side"], how="left", validate="many_to_one",
    )
    periods = {
        "train": (20240101, 20241231),
        "validation": (20250101, 20251231),
        "shadow": (20260101, int(scored.signal_ymd.max())),
    }
    metrics: dict[str, Any] = {}
    branching: dict[str, Any] = {}
    for split, (start, end) in periods.items():
        u = scored[scored.signal_ymd.between(start, end)]
        metrics[split] = {
            "transition": _metrics(top[top.signal_ymd.between(start, end)], u),
            "meemee": _metrics(baseline[baseline.signal_ymd.between(start, end)], u),
            "completed_events": int(u.transition_complete.sum()),
        }
        branching[split] = base._branching(top, baseline, "SELL", start, end)
    val = metrics["validation"]["transition"]
    decision = "keep" if (val.get("profit_factor") or 0) >= 1.3 and (val.get("expectancy") or 0) > 0 else "drop"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = out_root / f"{stamp}-{AXIS_ID}"
    out.mkdir(parents=True, exist_ok=False)
    top_cols = ["signal_ymd", "code", "score", "rank", "transition_stage", "transition_complete",
                "transition_start_ymd", "transition_bounce_ymd", "transition_failure_ymd",
                "target_before_stop20", "realized_mover20", "trade_return_h10", "split"]
    scored[top_cols].to_parquet(out / "all_symbol_sell_ranks.parquet", index=False)
    top[top_cols].to_parquet(out / "transition_top10_events.parquet", index=False)
    scored[scored.transition_complete][top_cols].to_parquet(out / "transition_completed_events.parquet", index=False)
    baseline.to_parquet(out / "meemee_sell_top10_events.parquet", index=False)
    compare = {
        "artifact_role": "authoritative",
        "axis_id": AXIS_ID,
        "fixed_evaluation_conditions": {
            "train_definition": "one fixed rule set defined on 2024 only; no threshold search",
            "sequence": "lower 5d high+low -> <=10d >=3% rebound below MA20 -> <=10d rebound high below prior high and >=2% fade below MA20 -> <=5d new low with ret2<0",
            "selection": "all symbols ranked daily; top10; no candidate suppression",
            "execution": "next-open TP8/SL5/H10/10bp",
            "baseline": "MeeMee ranking_appearance_daily down top10",
            "splits": {"train": [20240101, 20241231], "validation": [20250101, 20251231], "shadow": [20260101, int(scored.signal_ymd.max())]},
            "shadow_tuning": False, "fallback": False,
        },
        "source": source, "metrics": metrics, "branching": branching,
        "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only"},
    }
    base._write_json(out / "compare.json", compare)
    base._write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "compare_sha256": base._sha(out / "compare.json")})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.db, args.out))


if __name__ == "__main__":
    main()
