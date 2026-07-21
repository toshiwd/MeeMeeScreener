from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd


DEFAULT_CASES = ("SW090", "SW059", "SW046", "SW091")


def iso(ymd: int) -> str:
    text = str(int(ymd))
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def build_examples(db_path: Path, checkpoint_path: Path, case_ids: tuple[str, ...]) -> list[dict]:
    table = pd.read_parquet(checkpoint_path)
    selected = table[(table["checkpoint"] == 5) & table["case_id"].isin(case_ids)].copy()
    if set(selected["case_id"]) != set(case_ids):
        raise ValueError("requested case is missing from checkpoint table")
    codes = sorted(selected["code"].astype(str).str.zfill(4).unique())
    placeholders = ",".join("?" for _ in codes)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        bars = connection.execute(
            f"""
            select cast(code as varchar) code,
                   cast(strftime(to_timestamp(date), '%Y%m%d') as integer) ymd
            from daily_bars where source='pan' and cast(code as varchar) in ({placeholders})
            order by code, ymd
            """,
            codes,
        ).fetchdf()
    bars["code"] = bars["code"].astype(str).str.zfill(4)
    day1 = table[table["checkpoint"] == 1].set_index("case_id")
    examples = []
    for row in selected.set_index("case_id").loc[list(case_ids)].itertuples():
        history = bars[bars["code"] == str(row.code).zfill(4)].drop_duplicates("ymd").reset_index(drop=True)
        positions = history.index[history["ymd"] == int(row.ymd)].tolist()
        if len(positions) != 1:
            raise ValueError(f"base date missing for {row.Index}")
        index = positions[0]
        dates = history.iloc[index : index + 21]["ymd"].astype(int).tolist()
        target_arrow = "↑" if bool(row.target_up) else "↓"
        predicted_up = float(row.move_atr) > 0
        predicted_arrow = "↑" if predicted_up else "↓"
        correct = predicted_up == bool(row.target_up)
        markers = [{"date": iso(dates[0]), "label": "横ばい基準", "kind": "research-neutral"}]
        day1_row = day1.loc[row.Index]
        if abs(float(day1_row.move_atr)) >= 0.5:
            early_up = float(day1_row.move_atr) > 0
            markers.append({"date": iso(dates[1]), "label": f"1日暫定{'↑' if early_up else '↓'}", "kind": "research-up" if early_up else "research-down"})
        if int(row.first_hit_day) > 5:
            markers.append({
                "date": iso(dates[5]),
                "label": f"5日判定{predicted_arrow}{' 正解' if correct else ' 外れ'}",
                "kind": "research-up" if predicted_up else "research-down",
            })
        else:
            markers.append({"date": iso(dates[5]), "label": "5日:確定済み", "kind": "research-neutral"})
        markers.append({
            "date": iso(dates[int(row.first_hit_day)]), "label": f"2ATR確定{target_arrow}",
            "kind": "research-up" if bool(row.target_up) else "research-down",
        })
        examples.append({
            "case_id": row.Index, "code": str(row.code).zfill(4), "base_date": iso(dates[0]),
            "as_of": iso(dates[20]), "target_direction": "UP" if bool(row.target_up) else "DOWN",
            "first_hit_day": int(row.first_hit_day), "day5_move_atr": float(row.move_atr),
            "day5_prediction_correct": bool(correct), "markers": markers,
        })
    return examples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--checkpoints", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases", default=",".join(DEFAULT_CASES))
    args = parser.parse_args()
    cases = tuple(item.strip() for item in args.cases.split(",") if item.strip())
    payload = {
        "schema_version": "tradex_sideways_direction_chart_examples_v1",
        "capture_owner": "MeeMee", "research_owner": "TRADEX", "review_only": True,
        "examples": build_examples(args.db, args.checkpoints, cases),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
