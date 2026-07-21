from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "tradex_labeled_visual_ledger_index_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\visual_corpus_index_v1")
DEFAULT_LEDGER_PATHS = [
    Path(r"G:\Tradex\short_shape_labeled_screenshot_dataset_light144_recent_v1\combined_light144_recent_dataset_v1\label_ledger.jsonl"),
    Path(r"G:\Tradex\short_shape_unbiased_holdout_v1\combined_unbiased_holdout80_v1\label_ledger.jsonl"),
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, default=str) + "\n" for row in rows), encoding="utf-8")


def _date_key(as_of: Any) -> str:
    text = str(as_of)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _compact(row: dict[str, Any], ledger_path: Path) -> dict[str, Any]:
    code = str(row.get("code"))
    as_of = _date_key(row.get("as_of"))
    return {
        "schema_version": f"{AXIS_ID}_row_v1",
        "key": f"{code}:{as_of}",
        "sample_key": row.get("sample_key"),
        "code": code,
        "as_of": as_of,
        "saved_path": row.get("saved_path"),
        "image_relpath": row.get("image_relpath"),
        "ledger_path": str(ledger_path),
        "label_available": row.get("label_available"),
        "purpose_outcome_class": row.get("purpose_outcome_class"),
        "ret5": row.get("ret5"),
        "ret10": row.get("ret10"),
        "ret20": row.get("ret20"),
        "MFE20": row.get("MFE20"),
        "MAE20": row.get("MAE20"),
        "label_horizon_trading_days": row.get("label_horizon_trading_days"),
        "labels_used_in_image_rendering": row.get("labels_used_in_image_rendering"),
        "source_setup_type": row.get("source_setup_type"),
        "source_surface": row.get("source_surface"),
    }


def _metric(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ret20 = [float(row["ret20"]) for row in rows if row.get("ret20") is not None]
    mae20 = [float(row["MAE20"]) for row in rows if row.get("MAE20") is not None]
    mfe20 = [float(row["MFE20"]) for row in rows if row.get("MFE20") is not None]
    return {
        "n": len(rows),
        "unique_keys": len({row["key"] for row in rows}),
        "avg_ret20": sum(ret20) / len(ret20) if ret20 else None,
        "down20_rate": sum(value < 0 for value in ret20) / len(ret20) if ret20 else None,
        "ret20_lte_minus10_rate": sum(value <= -0.10 for value in ret20) / len(ret20) if ret20 else None,
        "mae20_lte_minus10_rate": sum(value <= -0.10 for value in mae20) / len(mae20) if mae20 else None,
        "mfe20_gte_plus5_rate": sum(value >= 0.05 for value in mfe20) / len(mfe20) if mfe20 else None,
    }


def run(*, ledgers: list[Path], output_root: Path) -> Path:
    run_dir = output_root / f"{_tag()}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    rows: list[dict[str, Any]] = []
    source_summaries = []
    for ledger in ledgers:
        ledger_rows = [_compact(row, ledger) for row in _read_jsonl(ledger)]
        rows.extend(ledger_rows)
        source_summaries.append({"ledger_path": str(ledger), "row_count": len(ledger_rows), "metrics": _metric(ledger_rows)})
    by_class: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_class.setdefault(str(row.get("purpose_outcome_class")), []).append(row)
    class_metrics = {key: _metric(value) for key, value in sorted(by_class.items())}
    _write_jsonl(run_dir / "labeled_visual_ledger_index.jsonl", rows)
    audit = {
        "schema_version": f"{AXIS_ID}_audit_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "research_phase": "infrastructure_stabilization",
        "source_summaries": source_summaries,
        "summary": {
            "row_count": len(rows),
            "unique_key_count": len({row["key"] for row in rows}),
            "metrics_all": _metric(rows),
            "class_metrics": class_metrics,
        },
        "artifacts": {
            "labeled_visual_ledger_index_jsonl": str(run_dir / "labeled_visual_ledger_index.jsonl"),
        },
        "prior_holdout_warning": {
            "image_score_alone": "failed_or_dropped_on_holdout",
            "use_policy": "use labels to guide shape features, not as standalone promotion evidence",
        },
        "decision": {
            "candidate_local_decision": "labeled_visual_ledger_ready",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "existing labeled screenshot ledgers indexed for feature-guided research",
        },
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "production_ranking_changed": False,
    }
    _write_json(run_dir / "labeled_visual_ledger_index_audit.json", audit)
    _write_json(output_root / "latest_labeled_visual_ledger_index_audit.json", {"run_root": str(run_dir), **audit})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--ledger", action="append", type=Path, default=[])
    args = parser.parse_args()
    ledgers = args.ledger or DEFAULT_LEDGER_PATHS
    print(run(ledgers=ledgers, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
