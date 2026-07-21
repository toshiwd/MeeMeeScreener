from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score


WINDOWS = (10, 15, 20, 30, 40, 50, 60)
FORWARD_DAYS = 20


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def efficiency(values: np.ndarray) -> float:
    path = float(np.abs(np.diff(values)).sum())
    return float(abs(values[-1] - values[0]) / path) if path else 0.0


def slope_share(values: np.ndarray) -> float:
    span = float(np.max(values) - np.min(values))
    if len(values) < 2 or span <= 0:
        return 0.0
    x = np.arange(len(values), dtype=float)
    x -= x.mean()
    return float(abs(np.dot(x, values - values.mean()) / np.square(x).sum() * (len(values) - 1) / span))


def true_range(frame: pd.DataFrame) -> pd.Series:
    previous = frame.c.shift(1)
    return pd.concat([(frame.h - frame.l).abs(), (frame.h - previous).abs(), (frame.l - previous).abs()], axis=1).max(axis=1)


def infer_band(history: pd.DataFrame) -> dict:
    history = history.tail(140).reset_index(drop=True)
    if len(history) < max(WINDOWS):
        raise ValueError("at least 60 prior bars are required")
    atr = true_range(history)
    atr60 = float(atr.tail(60).mean())
    atr14 = float(atr.tail(14).mean())
    candidates = []
    for window in WINDOWS:
        part = history.tail(window)
        upper, lower = float(part.h.max()), float(part.l.min())
        width = upper - lower
        midpoint = (part.h.to_numpy(float) + part.l.to_numpy(float)) / 2.0
        close = part.c.to_numpy(float)
        normalized_width = width / atr60 / np.sqrt(window) if atr60 > 0 else np.nan
        center_drift = float(abs(midpoint[-1] - midpoint[0]) / width) if width > 0 else 0.0
        score = normalized_width + efficiency(close) + slope_share(close) + center_drift
        candidates.append({
            "window": window, "band_start_ymd": int(part.ymd.iloc[0]),
            "lower": lower, "upper": upper, "width": width,
            "width_pct": width / float(part.c.iloc[-1]),
            "width_atr60": width / atr60 if atr60 > 0 else np.nan,
            "normalized_width": normalized_width,
            "efficiency": efficiency(close), "slope_share": slope_share(close),
            "center_drift": center_drift, "score": score,
        })
    best = min(candidates, key=lambda row: (row["score"], -row["window"]))
    return {
        **{f"band_{key}": value for key, value in best.items()},
        "atr14_atr60": atr14 / atr60 if atr60 > 0 else np.nan,
    }


def breakout_labels(future: pd.DataFrame, lower: float, upper: float) -> dict:
    future = future.head(FORWARD_DAYS).reset_index(drop=True)
    wick_direction = "UNRESOLVED"
    wick_day = None
    close_direction = "UNRESOLVED"
    close_day = None
    settle_direction = "UNRESOLVED"
    settle_day = None
    previous_close_side = None
    for index, row in enumerate(future.itertuples(index=False), start=1):
        wick_up, wick_down = float(row.h) > upper, float(row.l) < lower
        close_side = "UP" if float(row.c) > upper else "DOWN" if float(row.c) < lower else "INSIDE"
        if wick_day is None and (wick_up or wick_down):
            wick_day = index
            wick_direction = "BOTH" if wick_up and wick_down else "UP" if wick_up else "DOWN"
        if close_day is None and close_side != "INSIDE":
            close_day, close_direction = index, close_side
        if settle_day is None and close_side in ("UP", "DOWN") and previous_close_side == close_side:
            settle_day, settle_direction = index, close_side
        previous_close_side = close_side
    false_close_break = close_day is not None and settle_day is None
    return {
        "wick_break_direction": wick_direction, "wick_break_day": wick_day,
        "close_break_direction": close_direction, "close_break_day": close_day,
        "settled_break_direction": settle_direction, "settled_break_day": settle_day,
        "false_close_break_without_2day_settle": false_close_break,
        "forward_rows": int(len(future)),
    }


def load_bars(db_path: Path, codes: list[str]) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in codes)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        bars = connection.execute(f"""
            select cast(code as varchar) code,
                   cast(strftime(to_timestamp(date), '%Y%m%d') as integer) ymd,
                   cast(o as double) o, cast(h as double) h, cast(l as double) l,
                   cast(c as double) c, cast(v as double) v
            from daily_bars where source='pan' and c>0 and h>=l
              and cast(code as varchar) in ({placeholders})
            order by code, ymd
        """, codes).fetchdf()
    bars["code"] = bars.code.astype(str).str.zfill(4)
    return bars.drop_duplicates(["code", "ymd"], keep="last")


def run(db_path: Path, human_path: Path, sealed_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    sealed = pd.read_parquet(sealed_path)[["case_id", "code", "ymd", "year", "sample_group"]]
    labels = human.merge(sealed, on=["case_id", "code", "ymd"], validate="one_to_one")
    labels = labels[labels.sideways_decision != "BORDERLINE"].copy()
    labels["code"] = labels.code.astype(str).str.zfill(4)
    bars = load_bars(db_path, sorted(labels.code.unique()))
    grouped = {code: part.reset_index(drop=True) for code, part in bars.groupby("code", sort=False)}
    rows = []
    for case in labels.itertuples(index=False):
        series = grouped[case.code]
        position = series.index[series.ymd == int(case.ymd)].tolist()
        if len(position) != 1:
            raise ValueError(f"base bar missing for {case.case_id}")
        index = position[0]
        band = infer_band(series.iloc[: index + 1])
        outcome = breakout_labels(series.iloc[index + 1 : index + 1 + FORWARD_DAYS], band["band_lower"], band["band_upper"])
        rows.append({
            "case_id": case.case_id, "code": case.code, "ymd": int(case.ymd), "year": int(case.year),
            "sideways_decision": case.sideways_decision, "confidence": case.confidence,
            **band, **outcome,
        })
    table = pd.DataFrame(rows)
    table.to_parquet(output / "compression_bands.parquet", index=False)
    decided = table[table.sideways_decision.isin(["SIDEWAYS", "NOT_SIDEWAYS"])].copy()
    target = decided.sideways_decision.eq("SIDEWAYS")
    width_auc = float(roc_auc_score(target, -decided.band_normalized_width))
    sideways = decided[target]
    non_sideways = decided[~target]
    breakout_counts = sideways.close_break_direction.value_counts().to_dict()
    settled_counts = sideways.settled_break_direction.value_counts().to_dict()
    payload = {
        "schema_version": "tradex_sideways_compression_band_v1",
        "artifact_role": "authoritative_human_sideways_compression_band_and_breakout_labels",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "human_freeze_sha256": sha256(human_path),
            "sealed_sha256": sha256(sealed_path), "future_features_used_for_band": False,
            "band_windows": list(WINDOWS), "forward_days": FORWARD_DAYS,
            "break_definition": "first close outside frozen pre-event high-low band",
            "settle_definition": "two consecutive closes beyond the same frozen boundary",
        },
        "compression_separation": {
            "sideways_rows": int(len(sideways)), "not_sideways_rows": int(len(non_sideways)),
            "sideways_median_normalized_width": float(sideways.band_normalized_width.median()),
            "not_sideways_median_normalized_width": float(non_sideways.band_normalized_width.median()),
            "narrower_is_sideways_auc": width_auc,
        },
        "human_sideways_breakouts": {
            "first_close_break_counts": {str(k): int(v) for k, v in breakout_counts.items()},
            "two_close_settle_counts": {str(k): int(v) for k, v in settled_counts.items()},
            "median_close_break_day": float(sideways.close_break_day.dropna().median()) if sideways.close_break_day.notna().any() else None,
            "false_close_break_without_2day_settle_count": int(sideways.false_close_break_without_2day_settle.sum()),
        },
        "judgment": "hold_boundary_requires_chart_review",
        "not_changed": ["existing box detector", "existing sideways detector", "MeeMee", "ranking", "runtime DB", "trade rules", "direction predictor"],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha256(compare)}, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--human", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.human, args.sealed, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
