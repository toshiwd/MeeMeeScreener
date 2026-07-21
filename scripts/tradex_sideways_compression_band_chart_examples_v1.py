from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


def iso(ymd: int) -> str:
    text = str(int(ymd))
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def run(db_path: Path, bands_path: Path, output: Path) -> dict:
    table = pd.read_parquet(bands_path)
    sideways = table[table.sideways_decision == "SIDEWAYS"].copy()
    groups = [
        sideways[(sideways.close_break_direction == "UP") & sideways.settled_break_direction.eq("UP")].sort_values("close_break_day").head(1),
        sideways[(sideways.close_break_direction == "DOWN") & sideways.settled_break_direction.eq("DOWN")].sort_values("close_break_day").head(1),
        sideways[sideways.false_close_break_without_2day_settle].sort_values("close_break_day").head(1),
        sideways[sideways.close_break_direction.eq("UNRESOLVED")].head(1),
    ]
    selected = pd.concat(groups).drop_duplicates("case_id")
    codes = sorted(selected.code.unique())
    placeholders = ",".join("?" for _ in codes)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        bars = connection.execute(f"""
            select cast(code as varchar) code, cast(strftime(to_timestamp(date), '%Y%m%d') as integer) ymd
            from daily_bars where source='pan' and cast(code as varchar) in ({placeholders}) order by code, ymd
        """, codes).fetchdf()
    bars["code"] = bars.code.astype(str).str.zfill(4)
    examples = []
    for row in selected.itertuples(index=False):
        series = bars[bars.code == row.code].drop_duplicates("ymd").reset_index(drop=True)
        pos = series.index[series.ymd == row.ymd].tolist()[0]
        future_dates = series.iloc[pos + 1 : pos + 21].ymd.astype(int).tolist()
        markers = [{"date": iso(row.ymd), "label": "横ばい認識", "kind": "research-neutral"}]
        if pd.notna(row.close_break_day):
            direction = row.close_break_direction
            markers.append({"date": iso(future_dates[int(row.close_break_day) - 1]), "label": f"終値{direction}抜け", "kind": "research-up" if direction == "UP" else "research-down"})
        if pd.notna(row.settled_break_day):
            direction = row.settled_break_direction
            markers.append({"date": iso(future_dates[int(row.settled_break_day) - 1]), "label": f"2日定着{direction}", "kind": "research-up" if direction == "UP" else "research-down"})
        as_of_index = min(pos + 20, len(series) - 1)
        examples.append({
            "case_id": row.case_id, "code": row.code, "base_date": iso(row.ymd), "as_of": iso(int(series.iloc[as_of_index].ymd)),
            "markers": markers,
            "research_band": {"startDate": iso(row.band_band_start_ymd), "endDate": iso(row.ymd), "lower": row.band_lower, "upper": row.band_upper},
            "close_break_direction": row.close_break_direction, "close_break_day": None if pd.isna(row.close_break_day) else int(row.close_break_day),
            "settled_break_direction": row.settled_break_direction,
        })
    payload = {"schema_version": "tradex_sideways_compression_band_chart_examples_v1", "review_only": True, "examples": examples}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--bands", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.bands, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
