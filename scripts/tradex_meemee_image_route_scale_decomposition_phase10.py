from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import matthews_corrcoef, roc_auc_score


AXIS_ID = "meemee_image_route_scale_decomposition_phase10"
DEFAULT_PHASE8_DIR = Path(r"G:\Tradex\meemee_image_linear_baseline_phase8\20260602T081223Z-meemee_image_linear_baseline_phase8")
DEFAULT_PHASE9_DIR = Path(r"G:\Tradex\meemee_image_cnn_baseline_phase9\20260602T093246Z-meemee_image_cnn_baseline_phase9")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_image_route_scale_decomposition_phase10")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _metrics(rows: list[dict[str, Any]], score_name: str) -> dict[str, Any]:
    binary = [row for row in rows if row["target"] is not None]
    y = np.asarray([row["target"] for row in binary], dtype=np.int64)
    score = np.asarray([row[score_name] for row in binary], dtype=np.float64)
    pred = (score >= 0.5).astype(np.int64)
    return {
        "binary_image_count": len(binary),
        "roc_auc": float(roc_auc_score(y, score)) if len(set(y.tolist())) == 2 else None,
        "mcc": float(matthews_corrcoef(y, pred)) if len(set(y.tolist())) == 2 else None,
    }


def run(*, phase8_dir: Path, phase9_dir: Path, output_root: Path) -> Path:
    linear = _read_jsonl(phase8_dir / "image_score_ledger.jsonl")
    cnn = _read_jsonl(phase9_dir / "cnn_image_score_ledger.jsonl")
    linear_by_key = {(row["image_sample_key"], row["scale"]): row for row in linear}
    cnn_by_key = {(row["image_sample_key"], row["scale"]): row for row in cnn}
    if linear_by_key.keys() != cnn_by_key.keys():
        raise RuntimeError("Phase 8 and Phase 9 score ledger keys differ")
    rows = [
        {**linear_by_key[key], "cnn_image_score": cnn_by_key[key]["cnn_image_score"]}
        for key in sorted(linear_by_key)
    ]
    scales = sorted({row["scale"] for row in rows})
    report: dict[str, Any] = {}
    reusable: list[str] = []
    for scale in scales:
        report[scale] = {}
        for split in ("validation", "test"):
            subset = [row for row in rows if row["scale"] == scale and row["split"] == split]
            report[scale][split] = {
                "linear": _metrics(subset, "image_score"),
                "cnn": _metrics(subset, "cnn_image_score"),
            }
        val = report[scale]["validation"]["linear"]
        test = report[scale]["test"]["linear"]
        if val["roc_auc"] is not None and test["roc_auc"] is not None and val["roc_auc"] > 0.5 and test["roc_auc"] > 0.5 and val["mcc"] is not None and test["mcc"] is not None and val["mcc"] > 0 and test["mcc"] > 0:
            reusable.append(scale)
    decision = "preserve_reusable_scale_signal" if reusable else "close_image_route_no_reusable_stable_scale_signal"
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    audit = {
        "schema_version": "tradex_meemee_image_route_scale_decomposition_phase10_audit_v1",
        "generated_at": _utc_now(),
        "boundary_owner": "TRADEX",
        "phase8_dir": str(phase8_dir),
        "phase9_dir": str(phase9_dir),
        "axis": "scale_only_decomposition",
        "evaluation_conditions_unchanged": True,
        "model_retraining_executed": False,
        "score_ledger_key_count": len(rows),
        "scale_metrics": report,
        "reusable_stable_linear_scales": reusable,
        "decision": decision,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    _write_json(output_dir / "phase10_audit.json", audit)
    _write_json(output_root / "phase10_latest_audit.json", audit)
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase8-dir", type=Path, default=DEFAULT_PHASE8_DIR)
    parser.add_argument("--phase9-dir", type=Path, default=DEFAULT_PHASE9_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(phase8_dir=args.phase8_dir, phase9_dir=args.phase9_dir, output_root=args.output_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
