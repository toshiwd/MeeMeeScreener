from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import SGDClassifier

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import tradex_meemee_actionable_shape_image_binary_phase13 as phase13
from scripts import tradex_meemee_chart_shape_image_recognition_phase12 as phase12
from scripts import tradex_meemee_image_pattern_cluster_discovery_phase11 as phase11


AXIS_ID = "meemee_actionable_shape_profitability_phase14"
DEFAULT_PHASE7_DIR = phase12.DEFAULT_PHASE7_DIR
DEFAULT_PHASE11_ROOT = phase12.DEFAULT_PHASE11_ROOT
DEFAULT_DB_PATH = phase12.DEFAULT_DB_PATH
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\meemee_actionable_shape_profitability_phase14")
RANDOM_SEED = 20260602


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _summary(ret20: np.ndarray) -> dict[str, Any]:
    return {
        "count": int(len(ret20)),
        "ret20_mean": float(ret20.mean()),
        "ret20_median": float(np.median(ret20)),
        "positive_ret20_rate": float((ret20 > 0).mean()),
        "winner_ret20_gt_10pct_rate": float((ret20 >= 0.10).mean()),
        "bad_ret20_lt_minus_5pct_rate": float((ret20 <= -0.05).mean()),
    }


def _compare(rows: list[tuple[dict[str, Any], np.ndarray, int]], score: np.ndarray) -> dict[str, Any]:
    ret20 = np.asarray([row["ret20"] for row, _, _ in rows], dtype=np.float64)
    selected = ret20[score >= 0.5]
    baseline = _summary(ret20)
    favorable = _summary(selected)
    return {
        "baseline": baseline,
        "predicted_favorable": favorable,
        "delta": {
            "selected_count": favorable["count"] - baseline["count"],
            "ret20_mean": favorable["ret20_mean"] - baseline["ret20_mean"],
            "ret20_median": favorable["ret20_median"] - baseline["ret20_median"],
            "positive_ret20_rate": favorable["positive_ret20_rate"] - baseline["positive_ret20_rate"],
            "winner_ret20_gt_10pct_rate": favorable["winner_ret20_gt_10pct_rate"] - baseline["winner_ret20_gt_10pct_rate"],
            "bad_ret20_lt_minus_5pct_rate": favorable["bad_ret20_lt_minus_5pct_rate"] - baseline["bad_ret20_lt_minus_5pct_rate"],
        },
    }


def run(*, phase7_dir: Path, phase11_root: Path, db_path: Path, output_root: Path) -> Path:
    manifest_path = phase7_dir / "training_manifest.jsonl"
    rows = phase11._bundle_rows(phase11._read_jsonl(manifest_path))
    cache_key = phase11._cache_key(manifest_path, rows)
    features = phase11._features(rows, workers=1, cache_dir=phase11_root / "feature_cache" / "all", cache_key=cache_key)
    labels = phase12._labels(rows, phase12._load_bars(db_path))
    filtered = [(row, feature, target) for row, feature, label in zip(rows, features, labels) if (target := phase13._target(label)) is not None and row["split"] != "embargo"]
    train = [(feature, target) for row, feature, target in filtered if row["split"] == "train"]
    model = SGDClassifier(loss="log_loss", random_state=RANDOM_SEED)
    model.fit(np.vstack([feature for feature, _ in train]), np.asarray([target for _, target in train]))
    compare: dict[str, Any] = {}
    for split in ("validation", "test"):
        subset = [(row, feature, target) for row, feature, target in filtered if row["split"] == split]
        score = model.predict_proba(np.vstack([feature for _, feature, _ in subset]))[:, 1]
        compare[split] = _compare(subset, score)
    keep = all(
        compare[split]["delta"]["ret20_mean"] >= 0.005
        and compare[split]["delta"]["positive_ret20_rate"] >= 0.02
        and compare[split]["delta"]["bad_ret20_lt_minus_5pct_rate"] <= 0
        for split in ("validation", "test")
    )
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    payload = {
        "schema_version": "tradex_meemee_actionable_shape_profitability_phase14_compare_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "fixed_target_contract": {"favorable": sorted(phase13.FAVORABLE), "caution": sorted(phase13.CAUTION)},
        "compare": compare,
    }
    _write_json(output_dir / "actionable_shape_profitability_compare.json", payload)
    decision = {
        "schema_version": "tradex_meemee_actionable_shape_profitability_phase14_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "authoritative_rollup_decision": "keep_for_shadow_manual_review" if keep else "drop_no_stable_profitability_lift",
        "reason_type": "validation_and_test_ret20_quality_gate",
        "compare": compare,
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
        "meemee_reflectable": False,
        "automatic_trade_action": False,
        "non_scope": ["MeeMee reflection", "production ranking", "automatic trade action", "validated buy claim"],
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
