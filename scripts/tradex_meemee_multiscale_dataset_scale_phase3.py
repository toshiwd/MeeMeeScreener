from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


AXIS_ID = "meemee_multiscale_dataset_scale_phase3"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_multiscale_dataset_scale_phase3")
DEFAULT_DB_PATH = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
SCALES = {"micro": 30, "short": 60, "structure": 120, "macro": 240}
MIN_HISTORY_ROWS = 240
LABEL_HORIZON = 20


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _sha_key(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()[:24]


def _round(value: float) -> float:
    return round(float(value), 8)


def load_month_end_samples(conn: duckdb.DuckDBPyConnection, *, start_ym: int, end_ym: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        WITH normalized AS (
            SELECT
                code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN CAST(date AS INTEGER)
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                h, l, c
            FROM daily_bars
            WHERE COALESCE(source, 'pan') <> 'yahoo'
        ),
        sequenced AS (
            SELECT
                code, ymd, h, l, c,
                CAST(FLOOR(ymd / 100) AS INTEGER) AS ym,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY ymd) AS history_rows,
                ROW_NUMBER() OVER (PARTITION BY code, CAST(FLOOR(ymd / 100) AS INTEGER) ORDER BY ymd DESC) AS month_desc,
                LEAD(ymd, 1) OVER (PARTITION BY code ORDER BY ymd) AS label_start_as_of,
                LEAD(ymd, 20) OVER (PARTITION BY code ORDER BY ymd) AS label_end_as_of,
                LEAD(c, 5) OVER (PARTITION BY code ORDER BY ymd) AS close_5,
                LEAD(c, 10) OVER (PARTITION BY code ORDER BY ymd) AS close_10,
                LEAD(c, 20) OVER (PARTITION BY code ORDER BY ymd) AS close_20,
                MAX(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS high_20,
                MIN(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) AS low_20
            FROM normalized
            WHERE ymd IS NOT NULL AND c IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL
        )
        SELECT code, ymd, history_rows, label_start_as_of, label_end_as_of, c, close_5, close_10, close_20, high_20, low_20
        FROM sequenced
        WHERE month_desc = 1
          AND ym BETWEEN ? AND ?
          AND history_rows >= ?
          AND label_end_as_of IS NOT NULL
        ORDER BY ymd, code
        """,
        [start_ym, end_ym, MIN_HISTORY_ROWS],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for code, as_of, history_rows, label_start, label_end, close, close5, close10, close20, high20, low20 in rows:
        sample_key = _sha_key(code, as_of, "confirmed_non_yahoo", "monthly_multiscale_v1")
        out.append({
            "image_sample_key": sample_key,
            "code": str(code),
            "as_of": int(as_of),
            "available_history_rows": int(history_rows),
            "label_start_as_of": int(label_start),
            "label_end_as_of": int(label_end),
            "ret5": _round(float(close5) / float(close) - 1.0),
            "ret10": _round(float(close10) / float(close) - 1.0),
            "ret20": _round(float(close20) / float(close) - 1.0),
            "MFE20": _round(float(high20) / float(close) - 1.0),
            "MAE20": _round(float(low20) / float(close) - 1.0),
            "labels_used_in_image_rendering": False,
        })
    return out


def add_relative_labels(rows: list[dict[str, Any]]) -> None:
    by_date: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_date[int(row["as_of"])].append(row)
    for date_rows in by_date.values():
        ordered = sorted(date_rows, key=lambda row: (float(row["ret20"]), str(row["code"])))
        count = len(ordered)
        edge = max(1, math.ceil(count * 0.15))
        bottom_keys = {row["image_sample_key"] for row in ordered[:edge]}
        top_keys = {row["image_sample_key"] for row in ordered[-edge:]}
        for row in date_rows:
            key = row["image_sample_key"]
            row["future_top15_by_ret20"] = key in top_keys
            row["future_bottom15_by_ret20"] = key in bottom_keys
            row["neutral_middle70"] = key not in top_keys and key not in bottom_keys


def assign_split(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    unique_dates = sorted({int(row["as_of"]) for row in rows})
    if len(unique_dates) < 12:
        raise RuntimeError("dataset-scale split requires at least 12 monthly anchors")
    validation_start = unique_dates[int(len(unique_dates) * 0.60)]
    test_start = unique_dates[int(len(unique_dates) * 0.80)]
    assignments: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for row in rows:
        as_of = int(row["as_of"])
        label_end = int(row["label_end_as_of"])
        if as_of < validation_start and label_end >= validation_start:
            split, reason = "embargo", "label_window_crosses_validation_boundary"
        elif as_of < validation_start:
            split, reason = "train", None
        elif as_of < test_start and label_end >= test_start:
            split, reason = "embargo", "label_window_crosses_test_boundary"
        elif as_of < test_start:
            split, reason = "validation", None
        else:
            split, reason = "test", None
        counts[split] += 1
        assignments.append({
            "image_sample_key": row["image_sample_key"],
            "code": row["code"],
            "as_of": as_of,
            "label_start_as_of": int(row["label_start_as_of"]),
            "label_end_as_of": label_end,
            "split": split,
            "embargo_reason": reason,
        })
    non_embargo = [row for row in assignments if row["split"] != "embargo"]
    train = [row for row in assignments if row["split"] == "train"]
    validation = [row for row in assignments if row["split"] == "validation"]
    leakage = {
        "schema_version": "tradex_meemee_multiscale_dataset_scale_phase3_leakage_audit_v1",
        "validation_start_as_of": validation_start,
        "test_start_as_of": test_start,
        "future_label_window_overlap_train_validation": any(row["label_end_as_of"] >= validation_start for row in train),
        "future_label_window_overlap_validation_test": any(row["label_end_as_of"] >= test_start for row in validation),
        "same_as_of_across_train_validation_test": any(
            len({row["split"] for row in non_embargo if row["as_of"] == as_of}) > 1 for as_of in unique_dates
        ),
        "split_counts": dict(sorted(counts.items())),
    }
    leakage["split_leakage_audit_passed"] = (
        all(counts[name] > 0 for name in ("train", "validation", "test", "embargo"))
        and not leakage["future_label_window_overlap_train_validation"]
        and not leakage["future_label_window_overlap_validation_test"]
        and not leakage["same_as_of_across_train_validation_test"]
    )
    return assignments, leakage


def class_balance(rows: list[dict[str, Any]], assignments: list[dict[str, Any]]) -> dict[str, Any]:
    split_by_key = {row["image_sample_key"]: row["split"] for row in assignments}
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        split = split_by_key[row["image_sample_key"]]
        label = "top15" if row["future_top15_by_ret20"] else "bottom15" if row["future_bottom15_by_ret20"] else "neutral70"
        counts[split][label] += 1
    by_split = {
        split: {"sample_count": sum(counter.values()), **dict(sorted(counter.items()))}
        for split, counter in sorted(counts.items())
    }
    required = ("train", "validation", "test")
    passed = all(
        by_split.get(split, {}).get(label, 0) > 0
        for split in required
        for label in ("top15", "bottom15", "neutral70")
    )
    return {
        "schema_version": "tradex_meemee_multiscale_dataset_scale_phase3_class_balance_v1",
        "by_split": by_split,
        "train_validation_test_all_label_classes_present": passed,
    }


def run(*, db_path: Path, output_root: Path, start_ym: int, end_ym: int) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        labels = load_month_end_samples(conn, start_ym=start_ym, end_ym=end_ym)
    finally:
        conn.close()
    add_relative_labels(labels)
    assignments, leakage = assign_split(labels)
    balance = class_balance(labels, assignments)
    image_plan = [
        {
            "image_sample_key": row["image_sample_key"],
            "code": row["code"],
            "as_of": row["as_of"],
            "scale": scale,
            "bars": bars,
            "image_relpath": f"browser_reference_images/{row['image_sample_key']}_{row['code']}_{row['as_of']}_{scale}_{bars}.png",
        }
        for row in labels
        for scale, bars in SCALES.items()
    ]
    label_keys = {row["image_sample_key"] for row in labels}
    split_keys = {row["image_sample_key"] for row in assignments}
    plan_keys = {(row["image_sample_key"], row["scale"]) for row in image_plan}
    integrity = {
        "schema_version": "tradex_meemee_multiscale_dataset_scale_phase3_integrity_audit_v1",
        "sample_key_unique": len(label_keys) == len(labels),
        "label_split_keys_match": label_keys == split_keys,
        "planned_image_key_scale_unique": len(plan_keys) == len(image_plan),
        "four_scales_planned_per_bundle": len(image_plan) == len(labels) * len(SCALES),
        "labels_used_in_image_rendering": any(row["labels_used_in_image_rendering"] for row in labels),
        "compact_plan_omits_renderer_payload": all("bars_payload" not in row for row in image_plan),
    }
    integrity["data_integrity_audit_passed"] = (
        integrity["sample_key_unique"]
        and integrity["label_split_keys_match"]
        and integrity["planned_image_key_scale_unique"]
        and integrity["four_scales_planned_per_bundle"]
        and not integrity["labels_used_in_image_rendering"]
        and integrity["compact_plan_omits_renderer_payload"]
    )
    export_allowed = (
        leakage["split_leakage_audit_passed"]
        and balance["train_validation_test_all_label_classes_present"]
        and integrity["data_integrity_audit_passed"]
    )
    readiness = {
        "schema_version": "tradex_meemee_multiscale_dataset_scale_phase3_readiness_v1",
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "anchor_cadence": "confirmed_month_end",
        "start_ym": start_ym,
        "end_ym": end_ym,
        "sample_bundle_count": len(labels),
        "planned_canonical_image_count": len(image_plan),
        "split_counts": leakage["split_counts"],
        "split_leakage_audit_passed": leakage["split_leakage_audit_passed"],
        "class_balance_audit_passed": balance["train_validation_test_all_label_classes_present"],
        "data_integrity_audit_passed": integrity["data_integrity_audit_passed"],
        "ready_for_canonical_browser_export": export_allowed,
        "ready_for_model_training_preparation": export_allowed,
        "ready_for_model_training": False,
        "training_block_reason": "canonical_browser_images_not_exported_or_verified",
        "judgment": "pass_phase3_ready_for_canonical_export" if export_allowed else "hold_phase3_scale_audit_failed",
        "non_scope": ["canonical image export execution", "model training", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    }
    _write_json(output_dir / "dataset_scale_contract.json", {
        "schema_version": "tradex_meemee_multiscale_dataset_scale_phase3_contract_v1",
        "boundary_owner": "TRADEX",
        "source_policy": "confirmed_non_yahoo_daily_bars_only",
        "anchor_cadence": "confirmed_month_end",
        "sample_unit": "code_as_of_multiscale_bundle",
        "scales": SCALES,
        "label_horizon_trading_days": LABEL_HORIZON,
        "image_plan_policy": "compact plan only; renderer OHLCV payload resolved in later bounded export batches",
    })
    _write_jsonl(output_dir / "label_ledger.jsonl", labels)
    _write_jsonl(output_dir / "canonical_image_export_plan.jsonl", image_plan)
    _write_jsonl(output_dir / "split_assignment_ledger.jsonl", assignments)
    _write_json(output_dir / "split_leakage_audit.json", leakage)
    _write_json(output_dir / "class_balance_audit.json", balance)
    _write_json(output_dir / "data_integrity_audit.json", integrity)
    _write_json(output_dir / "dataset_scale_readiness.json", readiness)
    _write_json(output_dir / "phase3_audit.json", {
        "schema_version": "tradex_meemee_multiscale_dataset_scale_phase3_audit_v1",
        "generated_at": _utc_now(),
        "authoritative_readiness": readiness,
        "judgment": readiness["judgment"],
    })
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ym", type=int, default=202001)
    parser.add_argument("--end-ym", type=int, default=202604)
    args = parser.parse_args()
    print(run(db_path=args.db_path, output_root=args.output_root, start_ym=args.start_ym, end_ym=args.end_ym))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
