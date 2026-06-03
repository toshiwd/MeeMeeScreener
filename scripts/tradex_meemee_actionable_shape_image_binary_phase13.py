from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import matthews_corrcoef, roc_auc_score

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import tradex_meemee_chart_shape_image_recognition_phase12 as phase12
from scripts import tradex_meemee_image_pattern_cluster_discovery_phase11 as phase11


AXIS_ID = "meemee_actionable_shape_image_binary_phase13"
DEFAULT_PHASE7_DIR = phase12.DEFAULT_PHASE7_DIR
DEFAULT_PHASE11_ROOT = phase12.DEFAULT_PHASE11_ROOT
DEFAULT_DB_PATH = phase12.DEFAULT_DB_PATH
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_actionable_shape_image_binary_phase13")
FAVORABLE = {"breakout_hold", "gap_up_continuation", "steady_uptrend", "tight_high_flag"}
CAUTION = {"breakout_pullback_fail", "gap_up_stall_fade", "gap_up_upper_wick_failure", "steady_downtrend"}
RANDOM_SEED = 20260602


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _target(label: str) -> int | None:
    if label in FAVORABLE:
        return 1
    if label in CAUTION:
        return 0
    return None


def _metrics(y_true: np.ndarray, score: np.ndarray) -> dict[str, Any]:
    pred = (score >= 0.5).astype(np.int64)
    return {
        "count": int(len(y_true)),
        "positive_count": int((y_true == 1).sum()),
        "negative_count": int((y_true == 0).sum()),
        "roc_auc": float(roc_auc_score(y_true, score)),
        "mcc": float(matthews_corrcoef(y_true, pred)),
    }


def run(*, phase7_dir: Path, phase11_root: Path, db_path: Path, output_root: Path) -> Path:
    manifest_path = phase7_dir / "training_manifest.jsonl"
    rows = phase11._bundle_rows(phase11._read_jsonl(manifest_path))
    cache_key = phase11._cache_key(manifest_path, rows)
    features = phase11._features(rows, workers=1, cache_dir=phase11_root / "feature_cache" / "all", cache_key=cache_key)
    labels = phase12._labels(rows, phase12._load_bars(db_path))
    filtered = [(row, feature, target) for row, feature, label in zip(rows, features, labels) if (target := _target(label)) is not None and row["split"] != "embargo"]
    train = [(feature, target) for row, feature, target in filtered if row["split"] == "train"]
    model = SGDClassifier(loss="log_loss", random_state=RANDOM_SEED)
    model.fit(np.vstack([feature for feature, _ in train]), np.asarray([target for _, target in train]))
    metrics: dict[str, Any] = {}
    for split in ("validation", "test"):
        subset = [(feature, target) for row, feature, target in filtered if row["split"] == split]
        y_true = np.asarray([target for _, target in subset], dtype=np.int64)
        metrics[split] = _metrics(y_true, model.predict_proba(np.vstack([feature for feature, _ in subset]))[:, 1])
    keep = all(metrics[split]["roc_auc"] >= 0.60 and metrics[split]["mcc"] > 0 for split in ("validation", "test"))
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    compare = {
        "schema_version": "tradex_meemee_actionable_shape_image_binary_phase13_compare_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "target_contract": {"favorable": sorted(FAVORABLE), "caution": sorted(CAUTION)},
        "labels_used_as_training_targets_only": True,
        "metrics": metrics,
    }
    _write_json(output_dir / "actionable_shape_image_binary_compare.json", compare)
    decision = {
        "schema_version": "tradex_meemee_actionable_shape_image_binary_phase13_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "authoritative_rollup_decision": "keep_binary_shape_tag_for_manual_review" if keep else "drop_binary_shape_tag_not_reliable",
        "reason_type": "validation_and_test_auc_mcc_gate",
        "metrics": metrics,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "meemee_reflectable": False,
        "automatic_trade_action": False,
        "non_scope": ["profitability claim", "MeeMee reflection", "production ranking", "automatic trade action"],
    }
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_root / "latest_research_decision.json", {"run_root": str(output_dir), **decision})
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
