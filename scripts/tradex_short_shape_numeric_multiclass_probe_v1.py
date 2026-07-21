from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, confusion_matrix
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path
from tradex_short_shape_bad_avoidance_probe_v1 import _bucket, _daily_rows, _numeric_features_for


AXIS_ID = "short_shape_numeric_multiclass_probe_v1"
DEFAULT_EXTREME_TRAIN_DIR = Path(
    r"G:\Tradex\short_shape_labeled_screenshot_dataset_light144_recent_v1"
    r"\combined_light144_recent_dataset_v1"
)
DEFAULT_HOLDOUT_DIR = Path(
    r"G:\Tradex\short_shape_unbiased_holdout_v1"
    r"\combined_unbiased_holdout80_v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_numeric_multiclass_probe_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _ma(values: list[float], end_index: int, period: int) -> float | None:
    start = end_index - period + 1
    if start < 0:
        return None
    return sum(values[start : end_index + 1]) / period


def _neutral_rows_from_db(db_path: Path, *, exclude_keys: set[str], max_rows: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    codes = [str(row[0]) for row in conn.execute("SELECT DISTINCT code FROM daily_bars ORDER BY code").fetchall()]
    try:
        for code in codes:
            if len(rows) >= max_rows:
                break
            bars = _daily_rows(conn, code)
            if len(bars) < 280:
                continue
            closes = [float(row[4]) for row in bars]
            highs = [float(row[2]) for row in bars]
            lows = [float(row[3]) for row in bars]
            volumes = [float(row[5] or 0) for row in bars]
            last_taken = -999
            for index in range(80, len(bars) - 20):
                as_of = int(bars[index][0])
                if as_of < 20150101:
                    continue
                if index - last_taken < 40:
                    continue
                key = f"{code}:{as_of}"
                if key in exclude_keys:
                    continue
                ma20 = _ma(closes, index, 20)
                ma60 = _ma(closes, index, 60)
                vol20_prev = _ma(volumes, index - 1, 20)
                if ma20 is None or ma60 is None or not vol20_prev:
                    continue
                open_ = float(bars[index][1])
                high = float(bars[index][2])
                low = float(bars[index][3])
                close = float(bars[index][4])
                range_ = high - low
                if range_ <= 0:
                    continue
                upper_wick_ratio = (high - max(open_, close)) / range_
                previous_high20 = max(highs[index - 20 : index])
                high_zone_wick = close > previous_high20 and upper_wick_ratio >= 0.25 and volumes[index] / vol20_prev < 1.8
                ma_bear_pullback20 = close < ma20 and high >= ma20 and ma20 < ma60 and close < open_
                if not high_zone_wick and not ma_bear_pullback20:
                    continue
                future = bars[index + 1 : index + 21]
                ret20 = float(future[-1][4]) / close - 1.0
                mae20 = min(float(row[3]) for row in future) / close - 1.0
                mfe20 = max(float(row[2]) for row in future) / close - 1.0
                if ret20 <= -0.08 and mae20 <= -0.10:
                    continue
                if ret20 >= 0.06 or mfe20 >= 0.08:
                    continue
                rows.append(
                    {
                        "sample_key": key,
                        "code": code,
                        "as_of": as_of,
                        "purpose_outcome_class": "neutral_shape",
                        "ret20": round(ret20, 8),
                        "MAE20": round(mae20, 8),
                        "MFE20": round(mfe20, 8),
                    }
                )
                last_taken = index
                if len(rows) >= max_rows:
                    break
    finally:
        conn.close()
    return rows


def _class(row: dict[str, Any]) -> str:
    return str(row.get("purpose_outcome_class") or "")


def run(*, extreme_train_dir: Path, holdout_dir: Path, output_root: Path, db_path: Path) -> Path:
    extreme_train_rows = [row for row in _read_jsonl(extreme_train_dir / "label_ledger.jsonl") if _class(row) in {"good_short_shape", "bad_short_shape"}]
    holdout_rows = _read_jsonl(holdout_dir / "label_ledger.jsonl")
    exclude = {str(row["sample_key"]) for row in extreme_train_rows + holdout_rows}
    neutral_train_rows = _neutral_rows_from_db(db_path, exclude_keys=exclude, max_rows=80)
    train_rows = extreme_train_rows + neutral_train_rows
    x_train, train_rows = _numeric_features_for(train_rows, db_path)
    x_holdout, holdout_rows = _numeric_features_for(holdout_rows, db_path)
    classes = ["bad_short_shape", "neutral_shape", "good_short_shape"]
    y_train = np.array([classes.index(_class(row)) for row in train_rows])
    y_holdout = np.array([classes.index(_class(row)) for row in holdout_rows])
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=3000, class_weight="balanced", C=0.2))
    model.fit(x_train, y_train)
    prob = model.predict_proba(x_holdout)
    pred = model.predict(x_holdout)
    scored = []
    for row, row_prob in zip(holdout_rows, prob):
        bad_p = float(row_prob[classes.index("bad_short_shape")])
        good_p = float(row_prob[classes.index("good_short_shape")])
        scored.append({**row, "bad_probability": bad_p, "good_probability": good_p, "accept_score": good_p - bad_p})
    scored.sort(key=lambda row: row["accept_score"], reverse=True)
    baseline = _bucket(holdout_rows)
    top10 = _bucket(scored[:10])
    top20 = _bucket(scored[:20])
    helped = top20.get("bad_rate", 1) <= baseline["bad_rate"] - 0.05 and top20.get("good_rate", 0) >= baseline["good_rate"]
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "fixed_evaluation_conditions": {
            "train_extremes": str(extreme_train_dir),
            "train_neutral_source": "confirmed non-yahoo daily_bars sampled with same setup families, excluding holdout keys",
            "holdout": str(holdout_dir),
            "feature_contract": "numeric MA/range/wick/volume structure only; no image features",
        },
        "class_counts": {
            "train_bad": int((y_train == classes.index("bad_short_shape")).sum()),
            "train_neutral": int((y_train == classes.index("neutral_shape")).sum()),
            "train_good": int((y_train == classes.index("good_short_shape")).sum()),
            "holdout_bad": int((y_holdout == classes.index("bad_short_shape")).sum()),
            "holdout_neutral": int((y_holdout == classes.index("neutral_shape")).sum()),
            "holdout_good": int((y_holdout == classes.index("good_short_shape")).sum()),
        },
        "multiclass_metrics": {
            "balanced_accuracy": float(balanced_accuracy_score(y_holdout, pred)),
            "confusion_matrix_class_order": classes,
            "confusion_matrix": confusion_matrix(y_holdout, pred, labels=[0, 1, 2]).tolist(),
        },
        "baseline": baseline,
        "accept_score_buckets": {
            "top10": top10,
            "top20": top20,
            "bottom20": _bucket(scored[-20:]),
        },
        "decision": {
            "candidate_local_decision": "keep_numeric_multiclass_for_larger_oos" if helped else "drop_numeric_multiclass_current_form",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "top20 accept_score lowered bad_rate by at least 5 points without lowering good_rate" if helped else "numeric 3-class did not clear the bad-rate and good-rate holdout gate",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "numeric_multiclass_probe.json", report)
    _write_json(output_root / "latest_numeric_multiclass_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extreme-train-dir", type=Path, default=DEFAULT_EXTREME_TRAIN_DIR)
    parser.add_argument("--holdout-dir", type=Path, default=DEFAULT_HOLDOUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(
        extreme_train_dir=args.extreme_train_dir,
        holdout_dir=args.holdout_dir,
        output_root=args.output_root,
        db_path=args.db_path or resolve_runtime_stock_db_path(),
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
