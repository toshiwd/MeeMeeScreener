from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path


LABEL_HORIZONS = (5, 10, 20)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _normalize_as_of(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return int(text)
    digits = "".join(ch for ch in text if ch.isdigit())
    return int(digits) if len(digits) == 8 else None


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


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 8)


def _labels_for(rows: list[tuple[int, float, float, float, float, float]], as_of: int) -> dict[str, Any]:
    ymds = [int(row[0]) for row in rows]
    if as_of not in ymds:
        return {"label_available": False, "label_missing_reason": "as_of_not_found"}
    anchor_index = ymds.index(as_of)
    close = float(rows[anchor_index][4])
    available = len(rows) > anchor_index + max(LABEL_HORIZONS)
    payload: dict[str, Any] = {
        "label_available": available,
        "label_missing_reason": None if available else "insufficient_forward_horizon",
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


def join_labels(*, dataset_dir: Path, db_path: Path, purpose_plan_path: Path | None = None) -> dict[str, Any]:
    manifest_path = dataset_dir / "image_manifest.jsonl"
    manifest = _read_jsonl(manifest_path)
    purpose_by_key: dict[str, dict[str, Any]] = {}
    if purpose_plan_path is not None:
        purpose_rows = _read_jsonl(purpose_plan_path)
        purpose_by_key = {str(row.get("sample_key")): row for row in purpose_rows if row.get("sample_key")}
    labels: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    cache: dict[str, list[tuple[int, float, float, float, float, float]]] = {}
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        for row in manifest:
            code = str(row.get("code") or "").strip()
            as_of = _normalize_as_of(row.get("as_of"))
            if not code or as_of is None:
                skipped.append({**row, "status": "missing_code_or_as_of"})
                continue
            if code not in cache:
                cache[code] = _daily_rows(conn, code)
            label_payload = _labels_for(cache[code], as_of)
            output = {
                "sample_key": f"{code}:{as_of}",
                "code": code,
                "as_of": as_of,
                "image_relpath": row.get("image_relpath"),
                "saved_path": row.get("saved_path"),
                **label_payload,
            }
            purpose = purpose_by_key.get(output["sample_key"])
            if purpose:
                output.update({
                    "split": purpose.get("split"),
                    "purpose_outcome_class": purpose.get("outcome_class"),
                    "purpose_ret20": purpose.get("ret20"),
                    "source_rank": purpose.get("rank"),
                    "source_display_score": purpose.get("display_score"),
                    "source_signal_state": purpose.get("signal_state_at_appearance"),
                    "source_setup_type": purpose.get("setup_type_at_appearance"),
                    "source_surface": purpose.get("source_surface"),
                })
            labels.append(output)
            if not label_payload.get("label_available"):
                skipped.append({**output, "status": label_payload.get("label_missing_reason")})
    finally:
        conn.close()

    available = [row for row in labels if row.get("label_available") is True]
    _write_jsonl(dataset_dir / "label_ledger.jsonl", labels)
    _write_jsonl(dataset_dir / "label_join_skipped.jsonl", skipped)
    _write_json(dataset_dir / "dataset_contract.json", {
        "schema_version": "tradex_detail_clean_screenshot_dataset_contract_v1",
        "boundary_owner": "TRADEX",
        "source_image_owner": "MeeMee",
        "sample_unit": "code_as_of_clean_detail_screenshot",
        "image_renderer": "MeeMee DetailView via /detail-shot/:code?cleanScreenshot=1&mainAsOf=YYYY-MM-DD",
        "source_policy": "confirmed_non_yahoo_daily_bars_only_for_labels",
        "point_in_time_policy": "image route receives mainAsOf and labels are joined after rendering",
        "label_isolation_policy": "future outcomes exist only in label_ledger.jsonl and are never renderer inputs",
        "non_scope": ["model training", "prediction adoption", "production ranking mutation", "runtime DB write"],
    })
    audit = {
        "schema_version": "tradex_detail_clean_screenshot_label_join_v1_audit",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "dataset_dir": str(dataset_dir),
        "db_path": str(db_path),
        "purpose_plan_path": str(purpose_plan_path) if purpose_plan_path else None,
        "purpose_plan_rows_joined": sum(1 for row in labels if row.get("split")),
        "purpose_outcome_rows_joined": sum(1 for row in labels if row.get("purpose_outcome_class")),
        "manifest_rows": len(manifest),
        "label_rows": len(labels),
        "label_available_count": len(available),
        "label_unavailable_count": len(labels) - len(available),
        "skipped_count": len(skipped),
        "labels_used_in_image_rendering": False,
        "runtime_db_write": False,
        "judgment": "pass_labeled_shadow_dataset_pilot" if available and len(available) == len(labels) else "hold_incomplete_labeled_shadow_dataset",
    }
    _write_json(dataset_dir / "label_join_audit.json", audit)
    return audit


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--purpose-plan", type=Path, default=None)
    args = parser.parse_args()
    audit = join_labels(
        dataset_dir=args.dataset_dir,
        db_path=args.db_path or resolve_runtime_stock_db_path(),
        purpose_plan_path=args.purpose_plan,
    )
    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
