from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


AXIS_ID = "meemee_multiscale_image_dataset_phase1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_multiscale_image_dataset_phase1")
DEFAULT_CODES = ("8714", "1963", "4208", "6326", "4536", "8086", "4042", "7616", "9381", "3186")
DEFAULT_ANCHORS = (20241230, 20250331, 20250630, 20250930, 20251230, 20260331)
SCALES = {"micro": 30, "short": 60, "structure": 120, "macro": 240}
LABEL_HORIZONS = (5, 10, 20)
MIN_HISTORY_ROWS = max(SCALES.values())


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


def _normalized_rows(conn: duckdb.DuckDBPyConnection, code: str) -> list[tuple[int, float, float, float, float, float]]:
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


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 8)


def _forward_labels(rows: list[tuple], anchor_index: int) -> dict[str, Any]:
    close = float(rows[anchor_index][4])
    available = len(rows) > anchor_index + max(LABEL_HORIZONS)
    payload: dict[str, Any] = {
        "label_available": available,
        "labels_used_in_image_rendering": False,
        "label_horizon_trading_days": max(LABEL_HORIZONS),
        "label_start_as_of": int(rows[anchor_index + 1][0]) if len(rows) > anchor_index + 1 else None,
        "label_end_as_of": int(rows[anchor_index + max(LABEL_HORIZONS)][0]) if available else None,
    }
    for horizon in LABEL_HORIZONS:
        future_index = anchor_index + horizon
        payload[f"ret{horizon}"] = _round(float(rows[future_index][4]) / close - 1.0) if len(rows) > future_index else None
    future20 = rows[anchor_index + 1 : anchor_index + 21]
    payload["MFE20"] = _round(max(float(row[2]) for row in future20) / close - 1.0) if len(future20) == 20 else None
    payload["MAE20"] = _round(min(float(row[3]) for row in future20) / close - 1.0) if len(future20) == 20 else None
    return payload


def build_dataset(*, output_root: Path, db_path: Path, codes: tuple[str, ...], requested_anchors: tuple[int, ...]) -> Path:
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    manifest_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for code in codes:
            rows = _normalized_rows(conn, code)
            ymds = [int(row[0]) for row in rows]
            for requested_anchor in requested_anchors:
                eligible = [index for index, ymd in enumerate(ymds) if ymd <= requested_anchor]
                if not eligible:
                    skipped_rows.append({"code": code, "requested_anchor": requested_anchor, "status": "missing_anchor"})
                    status_counts["missing_anchor"] += 1
                    continue
                anchor_index = eligible[-1]
                as_of = ymds[anchor_index]
                if anchor_index + 1 < MIN_HISTORY_ROWS:
                    skipped_rows.append({"code": code, "requested_anchor": requested_anchor, "as_of": as_of, "status": "insufficient_history"})
                    status_counts["insufficient_history"] += 1
                    continue
                labels = _forward_labels(rows, anchor_index)
                if not labels["label_available"]:
                    skipped_rows.append({"code": code, "requested_anchor": requested_anchor, "as_of": as_of, "status": "incomplete_forward_horizon"})
                    status_counts["incomplete_forward_horizon"] += 1
                    continue
                sample_key = _sha_key(code, as_of, "confirmed_non_yahoo", "multiscale_v1")
                history = [list(row) for row in rows[: anchor_index + 1]]
                for scale, bars in SCALES.items():
                    image_name = f"{sample_key}_{code}_{as_of}_{scale}_{bars}.png"
                    manifest_rows.append(
                        {
                            "image_sample_key": sample_key,
                            "code": code,
                            "as_of": as_of,
                            "requested_anchor": requested_anchor,
                            "scale": scale,
                            "bars": bars,
                            "image_relpath": f"browser_reference_images/{image_name}",
                            "available_history_rows": len(history),
                            "bars_payload": history,
                        }
                    )
                label_rows.append({"image_sample_key": sample_key, "code": code, "as_of": as_of, **labels})
                status_counts["renderable_labeled_sample"] += 1
    finally:
        conn.close()

    _write_json(output_dir / "dataset_contract.json", {
        "schema_version": "tradex_meemee_multiscale_image_dataset_phase1_contract_v1",
        "boundary_owner": "TRADEX",
        "canonical_training_renderer": "MeeMee ThumbnailCanvas.drawChart via Playwright browser export",
        "sample_unit": "code_as_of_multiscale_bundle",
        "scales": SCALES,
        "source_policy": "confirmed_non_yahoo_daily_bars_only",
        "point_in_time_policy": "bars_payload contains rows at or before as_of only",
        "label_isolation_policy": "forward outcomes exist only in label_ledger.jsonl and are never renderer inputs",
        "non_scope": ["model training", "probability calibration", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    })
    _write_json(output_dir / "label_contract.json", {
        "schema_version": "tradex_meemee_multiscale_forward_outcome_label_contract_v1",
        "label_horizons_trading_days": LABEL_HORIZONS,
        "labels": ["ret5", "ret10", "ret20", "MFE20", "MAE20"],
        "entry_reference": "as_of confirmed close",
        "future_window": "next 20 confirmed trading bars",
        "label_window_fields": ["label_start_as_of", "label_end_as_of"],
        "labels_used_in_image_rendering": False,
    })
    _write_jsonl(output_dir / "image_manifest.jsonl", manifest_rows)
    _write_jsonl(output_dir / "label_ledger.jsonl", label_rows)
    _write_jsonl(output_dir / "skipped_samples.jsonl", skipped_rows)
    expected_images = len(label_rows) * len(SCALES)
    _write_json(output_dir / "phase1_audit.json", {
        "schema_version": "tradex_meemee_multiscale_image_dataset_phase1_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "db_path": str(db_path),
        "requested_code_count": len(codes),
        "requested_anchor_count": len(requested_anchors),
        "sample_bundle_count": len(label_rows),
        "manifest_image_count": len(manifest_rows),
        "expected_manifest_image_count": expected_images,
        "skipped_sample_count": len(skipped_rows),
        "status_counts": dict(sorted(status_counts.items())),
        "point_in_time_payload_passed": all(max(row["bars_payload"][-1][0], 0) <= row["as_of"] for row in manifest_rows),
        "label_isolation_passed": all("ret20" not in row and "MFE20" not in row and "MAE20" not in row for row in manifest_rows),
        "canonical_browser_export_required": True,
        "judgment": "hold_for_canonical_browser_export" if manifest_rows else "hold_no_renderable_samples",
        "non_scope": ["model training", "production ranking mutation", "runtime DB write", "MeeMee UI mutation"],
    })
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--codes", nargs="*", default=list(DEFAULT_CODES))
    parser.add_argument("--anchors", nargs="*", type=int, default=list(DEFAULT_ANCHORS))
    args = parser.parse_args()
    print(build_dataset(
        output_root=args.output_root,
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        codes=tuple(args.codes),
        requested_anchors=tuple(args.anchors),
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
