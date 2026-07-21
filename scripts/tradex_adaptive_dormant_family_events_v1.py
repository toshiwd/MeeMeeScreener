from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for value in (str(ROOT), str(ROOT / "scripts")):
    if value not in sys.path:
        sys.path.insert(0, value)

import tradex_gu_first_pullback_axis_v1 as gu
import tradex_long_base_breakout_axis_v1 as base
import tradex_volatility_contraction_breakout_axis_v1 as contraction


AXIS_ID = "tradex_adaptive_dormant_family_events_v1"
OUT = Path(r"G:\Tradex\adaptive_dormant_family_events_v1")
DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb")


def top_three(frame: pd.DataFrame, feature: str, ascending: bool = True) -> pd.DataFrame:
    ordered = frame.sort_values(["ymd", feature, "code"], ascending=[True, ascending, True]).copy()
    ordered["rank"] = ordered.groupby("ymd").cumcount() + 1
    return ordered[ordered["rank"] <= 3].copy()


def simulate(frame: pd.DataFrame, rule: str, close10: dict[tuple[str, int], float] | None = None) -> pd.DataFrame:
    rows = []
    for item in frame.to_dict("records"):
        entry = float(item["next_open"])
        ret, exit_offset = None, 10
        for offset in range(1, 11):
            if float(item[f"l{offset}"]) <= entry * .95:
                ret, exit_offset = -.05, offset
                break
            if float(item[f"h{offset}"]) >= entry * 1.08:
                ret, exit_offset = .08, offset
                break
        if ret is None:
            if rule == "base_breakout":
                ret = close10[(str(item["code"]), int(item["d10"]))] / entry - 1
            else:
                ret = float(item["exit_c"]) / entry - 1
        rows.append({
            "side": "buy", "code": str(item["code"]), "signal_date": pd.to_datetime(str(int(item["ymd"]))),
            "entry_date": pd.to_datetime(int(item["next_entry_date"]), unit="s"), "ret": ret,
            "rule": rule, "exit_offset": exit_offset,
        })
    return pd.DataFrame(rows)


def run() -> Path:
    contraction_raw = contraction.raw()
    clean_set = contraction_raw.copy()
    clean_set["volume_ratio"] = clean_set.v / clean_set.av20
    clean_set = top_three(clean_set, "volume_ratio", ascending=False)
    contraction_set = contraction_raw[(contraction_raw.hi10 / contraction_raw.lo10 - 1) <= .06].copy()
    contraction_set["width10"] = contraction_set.hi10 / contraction_set.lo10 - 1
    contraction_set = top_three(contraction_set, "width10")

    gu_raw = gu.raw()
    gu_set = top_three(gu_raw[gu_raw.gu_gap >= .03].copy(), "gu_gap", ascending=False)

    base_raw = base.raw()
    base_set = base_raw[(base_raw.hi60 / base_raw.lo60 - 1) <= .20].copy()
    base_set["width60"] = base_set.hi60 / base_set.lo60 - 1
    base_set = top_three(base_set, "width60")
    lookup = base_set[["code", "d10"]].copy()
    lookup["code"] = lookup.code.astype(str)
    with duckdb.connect(str(DB), read_only=True) as db:
        db.register("exit_dates", lookup)
        closes = db.execute("""
          SELECT x.code,x.d10,b.c close10 FROM exit_dates x
          JOIN daily_bars b ON b.source='pan' AND b.code=x.code AND b.date=x.d10
        """).fetchdf()
    closes["code"] = closes.code.astype(str)
    close_map = {(row.code, int(row.d10)): float(row.close10) for row in closes.itertuples()}

    events = pd.concat([
        simulate(clean_set, "clean_breakout"),
        simulate(contraction_set, "volatility_contraction_breakout"),
        simulate(gu_set, "gu_first_pullback"),
        simulate(base_set, "base_breakout", close_map),
    ], ignore_index=True).sort_values(["entry_date", "rule", "code"])
    now = datetime.now(timezone.utc)
    output = OUT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    output.mkdir(parents=True)
    events.to_csv(output / "dormant_family_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.inventory.v1", "artifact_role": "authoritative",
        "fixed_variants": {"clean_breakout": "base shape without width ceiling; top3 by volume ratio", "volatility_contraction_max_width": .06, "gu_minimum_gap": .03, "base_maximum_width": .20},
        "common_execution": {"entry": "next open", "take_profit": .08, "stop_loss": .05, "maximum_sessions": 10, "same_bar": "stop_first"},
        "counts": events.rule.value_counts().to_dict(), "latest_signal_date": events.signal_date.max().strftime("%Y-%m-%d"),
        "runtime_db_write": False, "production_ranking_changed": False, "automatic_trading": False,
    }
    path = output / "inventory.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(path)
    return path


if __name__ == "__main__":
    run()
