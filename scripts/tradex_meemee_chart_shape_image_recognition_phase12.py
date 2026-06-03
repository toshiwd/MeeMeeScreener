from __future__ import annotations

import argparse
import json
import sys
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backend.services.chart_shape_service import classify_daily_chart_shape
from scripts import tradex_meemee_image_pattern_cluster_discovery_phase11 as phase11


AXIS_ID = "meemee_chart_shape_image_recognition_phase12"
DEFAULT_PHASE7_DIR = Path(r"G:\Tradex\meemee_training_preparation_phase7\20260602T080906Z-meemee_training_preparation_phase7")
DEFAULT_PHASE11_ROOT = Path(r"G:\Tradex\meemee_image_pattern_cluster_discovery_phase11")
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_chart_shape_image_recognition_phase12")
WINDOW = 20
MIN_LABEL_COUNT = 100
RANDOM_SEED = 20260602


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_bars(db_path: Path) -> dict[str, list[tuple[int, float, float, float, float, float]]]:
    with duckdb.connect(str(db_path), read_only=True) as conn:
        rows = conn.execute(
            """
            SELECT code,
                   CASE
                     WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                     WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                     ELSE CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                   END AS ymd,
                   o, h, l, c, v
            FROM daily_bars
            WHERE lower(coalesce(source, '')) <> 'yahoo'
            ORDER BY code, ymd
            """
        ).fetchall()
    by_code: dict[str, list[tuple[int, float, float, float, float, float]]] = defaultdict(list)
    for code, ymd, open_, high, low, close, volume in rows:
        if ymd is None:
            continue
        by_code[str(code)].append((int(ymd), float(open_), float(high), float(low), float(close), float(volume or 0.0)))
    return by_code


def _labels(rows: list[dict[str, Any]], bars_by_code: dict[str, list[tuple[int, float, float, float, float, float]]]) -> list[str]:
    labels: list[str] = []
    for row in rows:
        bars = bars_by_code.get(str(row["code"])) or []
        dates = [bar[0] for bar in bars]
        end = bisect_right(dates, int(row["as_of"]))
        shape = classify_daily_chart_shape(bars[max(0, end - WINDOW - 1):end], requested_window=WINDOW)
        labels.append(str(shape["shape_label"]))
    return labels


def _metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, Any]:
    return {
        "count": int(len(y_true)),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "macro_f1": float(f1_score(y_true, y_pred, average="macro", zero_division=0)),
    }


def run(*, phase7_dir: Path, phase11_root: Path, db_path: Path, output_root: Path) -> Path:
    manifest_path = phase7_dir / "training_manifest.jsonl"
    rows = phase11._bundle_rows(phase11._read_jsonl(manifest_path))
    cache_key = phase11._cache_key(manifest_path, rows)
    features = phase11._features(rows, workers=1, cache_dir=phase11_root / "feature_cache" / "all", cache_key=cache_key)
    labels = _labels(rows, _load_bars(db_path))
    train_counts: dict[str, int] = defaultdict(int)
    for row, label in zip(rows, labels):
        if row["split"] == "train":
            train_counts[label] += 1
    supported = sorted(label for label, count in train_counts.items() if count >= MIN_LABEL_COUNT)
    filtered = [(row, feature, label) for row, feature, label in zip(rows, features, labels) if label in supported and row["split"] != "embargo"]
    train = [(feature, label) for row, feature, label in filtered if row["split"] == "train"]
    if len(supported) < 2:
        raise RuntimeError("Not enough supported chart-shape labels")
    model = SGDClassifier(loss="log_loss", random_state=RANDOM_SEED)
    model.fit(np.vstack([feature for feature, _ in train]), np.asarray([label for _, label in train]))
    metrics: dict[str, Any] = {}
    ledger: list[dict[str, Any]] = []
    for split in ("validation", "test"):
        subset = [(row, feature, label) for row, feature, label in filtered if row["split"] == split]
        y_true = np.asarray([label for _, _, label in subset])
        y_pred = model.predict(np.vstack([feature for _, feature, _ in subset]))
        metrics[split] = _metrics(y_true, y_pred)
        for (row, _, label), predicted in zip(subset, y_pred):
            ledger.append({"image_sample_key": row["image_sample_key"], "code": row["code"], "as_of": row["as_of"], "split": split, "shape_label": label, "predicted_shape_label": str(predicted), "matched": bool(label == predicted)})
    decision = "keep_for_manual_pattern_review" if metrics["validation"]["macro_f1"] >= 0.25 and metrics["test"]["macro_f1"] >= 0.25 else "drop_image_shape_recognition_not_reliable"
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    compare = {
        "schema_version": "tradex_meemee_chart_shape_image_recognition_phase12_compare_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "label_source": "app.backend.services.chart_shape_service.classify_daily_chart_shape",
        "label_window": WINDOW,
        "image_feature_source": str(phase11_root / "feature_cache" / "all"),
        "labels_used_as_training_targets_only": True,
        "supported_labels": supported,
        "train_label_counts": dict(sorted(train_counts.items())),
        "metrics": metrics,
    }
    _write_json(output_dir / "chart_shape_image_recognition_compare.json", compare)
    with (output_dir / "chart_shape_image_recognition_ledger.jsonl").open("w", encoding="utf-8") as handle:
        for row in ledger:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    audit = {
        "schema_version": "tradex_meemee_chart_shape_image_recognition_phase12_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "authoritative_rollup_decision": decision,
        "reason_type": "validation_and_test_macro_f1_gate",
        "metrics": metrics,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "meemee_reflectable": False,
        "automatic_trade_action": False,
        "non_scope": ["D4/D5 double-top classifier", "profitability claim", "MeeMee reflection", "production ranking"],
    }
    _write_json(output_dir / "research_decision.json", audit)
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output_dir), **audit})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase7-dir", type=Path, default=DEFAULT_PHASE7_DIR)
    parser.add_argument("--phase11-root", type=Path, default=DEFAULT_PHASE11_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(phase7_dir=args.phase7_dir, phase11_root=args.phase11_root, db_path=args.db_path, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
