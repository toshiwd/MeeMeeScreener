from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


AXIS_ID = "tradex_nikkei225_daily_state_ledger_v1"


def _annotate_code(part: pd.DataFrame) -> pd.DataFrame:
    part = part.sort_values("ymd").copy().reset_index(drop=True)
    n = len(part)
    ma20_state, ma7_sequence, support_state, compression_state, rejection_state, stretch_state = (["unknown"] * n for _ in range(6))
    below20_history: list[bool] = []
    ma7_break: dict[str, Any] | None = None
    broken_support: dict[str, Any] | None = None
    compression_run = 0
    compression_box: tuple[float, float] | None = None
    prior_rejection: dict[str, float] | None = None
    for index, row in part.iterrows():
        close, high, low, atr = float(row.c), float(row.h), float(row.l), float(row.atr14 or 0)
        below20 = close < float(row.ma20); below20_history.append(below20)
        below3 = sum(below20_history[-3:])
        if int(row.reclaim_ma20) == 1:
            ma20_state[index] = "reclaim_candidate"
        elif len(below20_history) >= 2 and not below20 and not below20_history[-2]:
            ma20_state[index] = "confirmed_reclaim"
        elif int(row.cross_ma20) == 1:
            ma20_state[index] = "first_break"
        elif below3 >= 2:
            ma20_state[index] = "persistent_below"
        else:
            ma20_state[index] = "above_or_mixed"

        prior_low5 = float(part.loc[max(0, index - 5):index - 1, "l"].min()) if index else low
        if ma7_break is not None:
            ma7_break["age"] += 1; ma7_break["max_close"] = max(ma7_break["max_close"], close)
            invalidated = close > ma7_break["rebound_high"]
            second = ma7_break["age"] <= 10 and close < prior_low5 and close < float(row.ma7) and close < float(row.ma20) and ma7_break["max_close"] <= ma7_break["rebound_high"]
            weak = high >= float(row.ma7) and close < float(row.ma7)
            if invalidated or ma7_break["age"] > 10:
                ma7_sequence[index] = "invalidated" if invalidated else "expired"; ma7_break = None
            elif second:
                ma7_sequence[index] = "second_break"; ma7_break = None
            elif weak:
                ma7_sequence[index] = "weak_rebound"
            else:
                ma7_sequence[index] = "after_ma7_break"
        else:
            ma7_sequence[index] = "intact"
        if ma7_break is None and int(row.cross_ma7) == 1:
            ma7_break = {"age": 0, "max_close": close, "rebound_high": max(float(row.ma7), close + 2 * atr)}
            ma7_sequence[index] = "ma7_break"

        if broken_support is not None:
            broken_support["age"] += 1; level = broken_support["level"]
            reclaimed = close > level + .35 * atr
            retest = high >= level - .35 * atr and close < level
            second = broken_support.get("retested", False) and close < broken_support["break_low"]
            if reclaimed or broken_support["age"] > 10:
                support_state[index] = "reclaimed" if reclaimed else "expired"; broken_support = None
            elif second:
                support_state[index] = "resistance_second_break"; broken_support = None
            elif retest:
                broken_support["retested"] = True; support_state[index] = "retest_rejected"
            else:
                support_state[index] = "broken_waiting_retest"
        else:
            support_state[index] = "intact"
        if broken_support is None and int(row.support_break) == 1 and np.isfinite(row.support20):
            broken_support = {"age": 0, "level": float(row.support20), "break_low": low, "retested": False}
            support_state[index] = "support_break"

        compressed = float(row.range20_pct) <= .10
        if compressed:
            compression_run += 1
            if compression_run == 1: compression_box = (float(row.support20), float(row.resistance20))
            compression_state[index] = "forming" if compression_run <= 4 else "established" if compression_run <= 10 else "prolonged"
        else:
            if compression_box is not None and compression_run >= 5:
                lower, upper = compression_box
                compression_state[index] = "down_break" if close < lower else "up_break" if close > upper else "released_inside"
            else:
                compression_state[index] = "none"
            compression_run = 0; compression_box = None

        if prior_rejection is not None:
            if low >= prior_rejection["low"] - .2 * atr and close > prior_rejection["mid"]:
                rejection_state[index] = "confirmed"
            elif low < prior_rejection["low"]:
                rejection_state[index] = "failed"
            else:
                rejection_state[index] = "unconfirmed"
            prior_rejection = None
        else:
            rejection_state[index] = "none"
        if float(row.lower_wick_ratio) >= .25 and float(row.close_pos) >= .25:
            prior_rejection = {"low": low, "mid": (high + low) / 2}; rejection_state[index] = "candidate"

        extreme = int(row.oversold_risk) == 1 and (float(row.dist_ma20_atr) <= -2 or float(row.ret5) <= -.08)
        stretch_state[index] = "extreme" if extreme else "extended" if int(row.oversold_risk) == 1 else "normal"

    part["ma20_structure_state"] = ma20_state
    part["ma7_sequence_state"] = ma7_sequence
    part["support_transition_state"] = support_state
    part["compression_state"] = compression_state
    part["lower_rejection_state"] = rejection_state
    part["stretch_state"] = stretch_state
    part["pressure_state"] = np.where((part.bear_count5 >= 3) & (part.bear_body5_atr >= 1), "dominant", np.where(part.bear_count5 >= 3, "building", "balanced"))
    return part


def run(input_parquet: Path, output_root: Path) -> Path:
    frame = duckdb.connect().execute(f"SELECT * FROM read_parquet('{input_parquet.as_posix()}') ORDER BY code,ymd").fetchdf()
    annotated = pd.concat([_annotate_code(part) for _, part in frame.groupby("code", sort=False)], ignore_index=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); output = output_root / f"{stamp}-{AXIS_ID}"; output.mkdir(parents=True, exist_ok=False)
    parquet = output / "daily_state_ledger.parquet"; annotated.to_parquet(parquet, index=False, compression="zstd")
    state_columns = ["ma20_structure_state","ma7_sequence_state","support_transition_state","compression_state","lower_rejection_state","stretch_state","pressure_state"]
    counts = {column: {str(key): int(value) for key, value in annotated[column].value_counts().items()} for column in state_columns}
    audit = {"schema_version": f"{AXIS_ID}.audit.v1", "artifact_role": "authoritative", "generated_at": datetime.now(timezone.utc).isoformat(), "source_parquet": str(input_parquet), "output_parquet": str(parquet), "rows": len(annotated), "codes": int(annotated.code.nunique()), "min_ymd": int(annotated.ymd.min()), "max_ymd": int(annotated.ymd.max()), "state_counts": counts, "point_in_time_contract": {"features": "t or earlier only", "next_day_confirmation": "recorded first at t+1, never backfilled", "outcomes": "preserved label columns, not used in state transitions"}, "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}}
    (output / "audit.json").write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "audit": str(output / "audit.json"), "parquet": str(parquet)}, indent=2) + "\n", encoding="utf-8"); return output


def main() -> None:
    parser=argparse.ArgumentParser();parser.add_argument("--input-parquet",required=True,type=Path);parser.add_argument("--output-root",type=Path,default=Path(r"G:\Tradex\tradex_nikkei225_daily_state_ledger_v1"));args=parser.parse_args();print(run(args.input_parquet,args.output_root))


if __name__=="__main__":main()
