from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import duckdb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, confusion_matrix, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from tradex_short_shape_image_feature_probe_v1 import _feature_vector

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "short_shape_bad_avoidance_probe_v1"
DEFAULT_TRAIN_DIR = Path(
    r"G:\Tradex\short_shape_labeled_screenshot_dataset_light144_recent_v1"
    r"\combined_light144_recent_dataset_v1"
)
DEFAULT_HOLDOUT_DIR = Path(
    r"G:\Tradex\short_shape_unbiased_holdout_v1"
    r"\combined_unbiased_holdout80_v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_bad_avoidance_probe_v1")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _class(row: dict[str, Any]) -> str:
    return str(row.get("purpose_outcome_class") or "")


def _x(rows: list[dict[str, Any]]) -> np.ndarray:
    return np.array([_feature_vector(Path(str(row["saved_path"]))) for row in rows])


def _daily_rows(conn: duckdb.DuckDBPyConnection, code: str) -> list[tuple[int, float, float, float, float, float]]:
    return conn.execute(
        """
        WITH normalized AS (
            SELECT
                CASE
                    WHEN length(CAST(abs(date) AS VARCHAR)) = 8 THEN CAST(date AS INTEGER)
                    ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER)
                END AS ymd,
                o, h, l, c, v
            FROM daily_bars
            WHERE code = ?
              AND COALESCE(source, 'pan') <> 'yahoo'
        )
        SELECT ymd, o, h, l, c, v
        FROM normalized
        WHERE o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL
        ORDER BY ymd
        """,
        [code],
    ).fetchall()


def _ma(values: list[float], end_index: int, period: int) -> float | None:
    start = end_index - period + 1
    if start < 0:
        return None
    return sum(values[start : end_index + 1]) / period


def _numeric_features_for(rows: list[dict[str, Any]], db_path: Path) -> tuple[np.ndarray, list[dict[str, Any]]]:
    cache: dict[str, list[tuple[int, float, float, float, float, float]]] = {}
    features: list[list[float]] = []
    kept: list[dict[str, Any]] = []
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for row in rows:
            code = str(row["code"])
            as_of = int(row["as_of"])
            if code not in cache:
                cache[code] = _daily_rows(conn, code)
            bars = cache[code]
            ymds = [int(item[0]) for item in bars]
            if as_of not in ymds:
                continue
            index = ymds.index(as_of)
            if index < 60:
                continue
            closes = [float(item[4]) for item in bars]
            highs = [float(item[2]) for item in bars]
            lows = [float(item[3]) for item in bars]
            volumes = [float(item[5] or 0) for item in bars]
            open_ = float(bars[index][1])
            high = float(bars[index][2])
            low = float(bars[index][3])
            close = float(bars[index][4])
            range_ = high - low
            if range_ <= 0:
                continue
            ma7 = _ma(closes, index, 7)
            ma20 = _ma(closes, index, 20)
            ma60 = _ma(closes, index, 60)
            ma20_prev5 = _ma(closes, index - 5, 20)
            vol20_prev = _ma(volumes, index - 1, 20)
            if ma7 is None or ma20 is None or ma60 is None or ma20_prev5 is None or not vol20_prev:
                continue
            low60 = min(lows[index - 59 : index + 1])
            high60 = max(highs[index - 59 : index + 1])
            high20_prev = max(highs[index - 20 : index])
            body_ratio = abs(close - open_) / range_
            upper_wick_ratio = (high - max(open_, close)) / range_
            lower_wick_ratio = (min(open_, close) - low) / range_
            features.append(
                [
                    close / ma7 - 1.0,
                    close / ma20 - 1.0,
                    close / ma60 - 1.0,
                    ma20 / ma60 - 1.0,
                    ma20 / ma20_prev5 - 1.0,
                    (close - low60) / (high60 - low60) if high60 > low60 else 0.5,
                    close / high20_prev - 1.0 if high20_prev else 0.0,
                    body_ratio,
                    upper_wick_ratio,
                    lower_wick_ratio,
                    volumes[index] / vol20_prev,
                    1.0 if close < ma7 else 0.0,
                    1.0 if close < ma20 else 0.0,
                    1.0 if high >= ma20 and close < ma20 else 0.0,
                    1.0 if close < open_ else 0.0,
                ]
            )
            kept.append(row)
    finally:
        conn.close()
    return np.array(features), kept


def _bucket(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"n": 0}
    return {
        "n": len(rows),
        "good_rate": sum(1 for row in rows if _class(row) == "good_short_shape") / len(rows),
        "bad_rate": sum(1 for row in rows if _class(row) == "bad_short_shape") / len(rows),
        "neutral_rate": sum(1 for row in rows if _class(row) == "neutral_shape") / len(rows),
        "avg_ret20": sum(float(row["ret20"]) for row in rows) / len(rows),
        "avg_MAE20": sum(float(row["MAE20"]) for row in rows) / len(rows),
        "avg_MFE20": sum(float(row["MFE20"]) for row in rows) / len(rows),
    }


def _threshold_probe(scored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    probes: list[dict[str, Any]] = []
    for max_bad_prob in (0.10, 0.20, 0.30, 0.40, 0.50):
        kept = [row for row in scored if row["image_bad_short_probability"] <= max_bad_prob]
        probes.append({"filter": {"max_bad_probability": max_bad_prob}, **_bucket(kept)})
    return probes


def _evaluate_modality(
    *,
    modality: str,
    x_train: np.ndarray,
    train_rows: list[dict[str, Any]],
    x_holdout: np.ndarray,
    holdout_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    y_train_bad = np.array([1 if _class(row) == "bad_short_shape" else 0 for row in train_rows])
    y_holdout_bad = np.array([1 if _class(row) == "bad_short_shape" else 0 for row in holdout_rows])
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced", C=0.2))
    model.fit(x_train, y_train_bad)
    bad_prob = model.predict_proba(x_holdout)[:, 1]
    pred_bad = (bad_prob >= 0.50).astype(int)
    scored: list[dict[str, Any]] = []
    for row, prob in zip(holdout_rows, bad_prob):
        scored.append({**row, "bad_probability": float(prob)})
    scored.sort(key=lambda row: row["bad_probability"])
    baseline = _bucket(holdout_rows)
    filters = _threshold_probe([dict(row, image_bad_short_probability=row["bad_probability"]) for row in scored])
    best_filter = None
    for row in filters:
        if row.get("n", 0) < 10:
            continue
        if best_filter is None or (row.get("bad_rate", 1), -row.get("good_rate", 0), row.get("avg_MFE20", 1)) < (
            best_filter.get("bad_rate", 1),
            -best_filter.get("good_rate", 0),
            best_filter.get("avg_MFE20", 1),
        ):
            best_filter = row
    helped = bool(best_filter and best_filter.get("bad_rate", 1) <= baseline["bad_rate"] - 0.05 and best_filter.get("n", 0) >= 10)
    return {
        "modality": modality,
        "bad_vs_nonbad_holdout_metrics": {
            "roc_auc": float(roc_auc_score(y_holdout_bad, bad_prob)),
            "balanced_accuracy_at_050": float(balanced_accuracy_score(y_holdout_bad, pred_bad)),
            "confusion_matrix": confusion_matrix(y_holdout_bad, pred_bad).tolist(),
        },
        "baseline": baseline,
        "score_bucket_summary": {
            "lowest_bad_probability_10": _bucket(scored[:10]),
            "lowest_bad_probability_20": _bucket(scored[:20]),
            "highest_bad_probability_10": _bucket(scored[-10:]),
            "highest_bad_probability_20": _bucket(scored[-20:]),
        },
        "threshold_filters": filters,
        "best_filter_by_bad_rate": best_filter,
        "candidate_local_decision": "keep_bad_avoidance_for_larger_oos" if helped else "drop_bad_avoidance",
    }


def run(*, train_dir: Path, holdout_dir: Path, output_root: Path, db_path: Path) -> Path:
    train_rows = [row for row in _read_jsonl(train_dir / "label_ledger.jsonl") if _class(row) in {"good_short_shape", "bad_short_shape"}]
    holdout_rows = _read_jsonl(holdout_dir / "label_ledger.jsonl")
    x_train_image = _x(train_rows)
    x_holdout_image = _x(holdout_rows)
    x_train_numeric, train_numeric_rows = _numeric_features_for(train_rows, db_path)
    x_holdout_numeric, holdout_numeric_rows = _numeric_features_for(holdout_rows, db_path)
    common_train = {(row["code"], row["as_of"]): index for index, row in enumerate(train_numeric_rows)}
    common_holdout = {(row["code"], row["as_of"]): index for index, row in enumerate(holdout_numeric_rows)}
    image_train_common = []
    numeric_train_common = []
    train_rows_common = []
    for idx, row in enumerate(train_rows):
        key = (row["code"], row["as_of"])
        if key in common_train:
            image_train_common.append(x_train_image[idx])
            numeric_train_common.append(x_train_numeric[common_train[key]])
            train_rows_common.append(row)
    image_holdout_common = []
    numeric_holdout_common = []
    holdout_rows_common = []
    for idx, row in enumerate(holdout_rows):
        key = (row["code"], row["as_of"])
        if key in common_holdout:
            image_holdout_common.append(x_holdout_image[idx])
            numeric_holdout_common.append(x_holdout_numeric[common_holdout[key]])
            holdout_rows_common.append(row)
    modality_reports = [
        _evaluate_modality(modality="image_only", x_train=x_train_image, train_rows=train_rows, x_holdout=x_holdout_image, holdout_rows=holdout_rows),
        _evaluate_modality(modality="numeric_structure_only", x_train=x_train_numeric, train_rows=train_numeric_rows, x_holdout=x_holdout_numeric, holdout_rows=holdout_numeric_rows),
        _evaluate_modality(
            modality="image_plus_numeric_structure",
            x_train=np.hstack([np.array(image_train_common), np.array(numeric_train_common)]),
            train_rows=train_rows_common,
            x_holdout=np.hstack([np.array(image_holdout_common), np.array(numeric_holdout_common)]),
            holdout_rows=holdout_rows_common,
        ),
    ]
    best_report = max(
        modality_reports,
        key=lambda row: (
            -float(row["best_filter_by_bad_rate"]["bad_rate"]) if row.get("best_filter_by_bad_rate") else -1.0,
            float(row["bad_vs_nonbad_holdout_metrics"]["roc_auc"]),
        ),
    )
    helped = best_report["candidate_local_decision"].startswith("keep")
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "train_dir": str(train_dir),
        "holdout_dir": str(holdout_dir),
        "fixed_evaluation_conditions": {
            "train_source": "light144 extreme good/bad only",
            "holdout_source": "unbiased holdout80 with good/bad/neutral",
            "feature_contract": "image features, numeric structure features, and image+numeric compared under same holdout",
            "model": "StandardScaler + LogisticRegression C=0.2 class_weight=balanced",
        },
        "class_counts": {
            "train_good": sum(1 for row in train_rows if _class(row) == "good_short_shape"),
            "train_bad": sum(1 for row in train_rows if _class(row) == "bad_short_shape"),
            "holdout_good": sum(1 for row in holdout_rows if _class(row) == "good_short_shape"),
            "holdout_bad": sum(1 for row in holdout_rows if _class(row) == "bad_short_shape"),
            "holdout_neutral": sum(1 for row in holdout_rows if _class(row) == "neutral_shape"),
        },
        "modality_reports": modality_reports,
        "best_modality_report": best_report,
        "decision": {
            "candidate_local_decision": "keep_bad_avoidance_for_larger_oos" if helped else "drop_bad_avoidance_image_only",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": (
                "bad-risk image score reduced holdout bad_rate by at least 5 points with n>=10"
                if helped
                else "bad-risk image score did not materially reduce bad outcomes on neutral-heavy holdout"
            ),
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "bad_avoidance_probe.json", report)
    _write_json(output_root / "latest_bad_avoidance_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--train-dir", type=Path, default=DEFAULT_TRAIN_DIR)
    parser.add_argument("--holdout-dir", type=Path, default=DEFAULT_HOLDOUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(train_dir=args.train_dir, holdout_dir=args.holdout_dir, output_root=args.output_root, db_path=args.db_path or resolve_runtime_stock_db_path()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
