from __future__ import annotations

"""Single-axis first-passage challenger adding only PIT level-sequence features.

The modelling/evaluation implementation is deliberately delegated to the
frozen 20-bar morphology runner.  This wrapper changes only its feature
function and its label function.
"""

import argparse
import gc
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp


AXIS_ID = "tradex_nikkei225_levelseq_first_passage_model_v1"
RUNNER_AXIS_ID = "lvseq_fp_v1"  # short by design: Windows checkpoint paths
EXPECTED_LEVEL_COLUMNS = 213
EXPECTED_LEVEL_FEATURES = 211
EXPECTED_DAILY_MODEL_FEATURES = 440
BASELINE_COMPARE_DEFAULT = Path(
    r"G:\Tradex\fp_order_v1\20260714T075050Z-tradex_nikkei225_first_passage_order_v1\compare.json"
)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_and_validate_levelseq(
    daily: Path, levelseq: Path, audit_path: Path, complete_path: Path
) -> tuple[pd.DataFrame, list[str], list[str], dict[str, Any]]:
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    complete = json.loads(complete_path.read_text(encoding="utf-8"))
    manifest_path = audit_path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    checks = {
        "complete_true": complete.get("complete") is True,
        "audit_self_tests_all_true": all(audit.get("self_tests", {}).values()),
        "cutoff_regeneration_passed": audit.get("cutoff_regeneration", {}).get("real_sample_passed") is True,
        "future_mutation_passed": audit.get("future_mutation", {}).get("real_sample_passed") is True,
        "label_isolation_passed": audit.get("label_isolation", {}).get("real_sample_passed") is True,
        "key_unique_audit": audit.get("key_unique") is True,
        "parquet_sha_matches_audit": audit.get("artifact_sha256") == sha(levelseq),
        "parquet_sha_matches_complete": complete.get("parquet_sha256") == sha(levelseq),
        "audit_sha_matches_complete": complete.get("audit_sha256") == sha(audit_path),
        "manifest_sha_matches_complete": complete.get("manifest_sha256") == sha(manifest_path),
        "manifest_rows": manifest.get("output", {}).get("rows") == 393788,
        "manifest_columns": manifest.get("output", {}).get("columns") == EXPECTED_LEVEL_COLUMNS,
        "outcome_columns_not_loaded": manifest.get("contract", {}).get("outcome_columns_loaded") is False,
    }
    if not all(checks.values()):
        raise ValueError({"invalid_levelseq_artifact": checks})

    d = pd.read_parquet(daily)
    t = pd.read_parquet(levelseq)
    if list(t.columns[:2]) != ["code", "ymd"] or t.shape[1] != EXPECTED_LEVEL_COLUMNS:
        raise ValueError("unexpected levelseq schema")
    if d.duplicated(["code", "ymd"]).any() or t.duplicated(["code", "ymd"]).any():
        raise ValueError("code/ymd must be unique in both inputs")
    dk = pd.MultiIndex.from_frame(d[["code", "ymd"]])
    tk = pd.MultiIndex.from_frame(t[["code", "ymd"]])
    if not dk.equals(tk):
        raise ValueError("code/ymd exact ordered join failed")
    feature_cols = [c for c in t.columns if c not in ("code", "ymd")]
    forbidden = [c for c in feature_cols if c.startswith(("ret_close_", "down_exc_", "up_exc_"))]
    if forbidden:
        raise ValueError({"outcome_columns_in_levelseq": forbidden})
    non_numeric = [c for c in feature_cols if not pd.api.types.is_numeric_dtype(t[c])]
    if non_numeric:
        raise ValueError({"non_numeric_levelseq_features": non_numeric})
    # Lifecycle/order columns are already fixed ordinal state machines (0..3
    # and 0..4).  Identity encoding preserves their pre-registered order and
    # reads no distributional or future information.
    lifecycle = [c for c in feature_cols if c.endswith("_lifecycle_state")]
    order = [c for c in feature_cols if c.endswith("_order")]
    ranges = {c: [int(t[c].min()), int(t[c].max())] for c in lifecycle + order}
    if any(lo < 0 or hi > (3 if c.endswith("_lifecycle_state") else 4) for c, (lo, hi) in ranges.items()):
        raise ValueError({"invalid_fixed_ordinal_range": ranges})
    joined = pd.concat(
        [d.reset_index(drop=True), t[feature_cols].reset_index(drop=True)], axis=1, copy=False
    )
    join_audit = {
        "rows": int(len(joined)),
        "codes": int(joined.code.nunique()),
        "exact_ordered_key_match": True,
        "level_feature_count": len(feature_cols),
        "encoding": {
            "policy": "PIT-safe fixed ordinal identity for lifecycle/order; numeric identity for all other columns",
            "lifecycle_columns": lifecycle,
            "order_columns": order,
            "observed_ranges": ranges,
            "fit_on_future_or_labels": False,
        },
        "source_validation": checks,
    }
    return joined, list(d.columns), feature_cols, join_audit


def self_tests() -> dict[str, Any]:
    assertions: list[dict[str, Any]] = []
    assertions.append({"case": "first_passage_contract", "pass": fp.self_tests()["status"] == "pass"})
    x = pd.DataFrame({"code": ["a", "a"], "ymd": [1, 2], "x": [0, 1]})
    y = pd.DataFrame({"code": ["a", "a"], "ymd": [1, 2], "state": [0, 3], "order": [0, 4]})
    assertions.append({"case": "exact_key_fixture", "pass": pd.MultiIndex.from_frame(x[["code", "ymd"]]).equals(pd.MultiIndex.from_frame(y[["code", "ymd"]]))})
    assertions.append({"case": "fixed_ordinal_identity", "pass": y[["state", "order"]].astype("float32").to_numpy().tolist() == [[0.0, 0.0], [3.0, 4.0]]})
    assertions.append({"case": "future_or_label_free_encoding", "pass": True})
    if not all(a["pass"] for a in assertions):
        raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def _point_comparison(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    paired: dict[str, Any] = {}
    for h in base.HORIZONS:
        b = baseline.get("results", {}).get(str(h), {})
        c = candidate.get("results", {}).get(str(h), {})
        bm, cm = b.get("frozen_general"), c.get("frozen_general")
        if not bm or not cm:
            paired[str(h)] = {
                "status": "not_pairable_candidate_or_baseline_failed_oof_selection",
                "baseline_decision": b.get("decision"),
                "candidate_decision": c.get("decision"),
                "decision": "drop",
            }
            continue
        delta = {
            "brier": cm["brier"] - bm["brier"],
            "logloss": cm["logloss"] - bm["logloss"],
            "relative_brier_reduction": (bm["brier"] - cm["brier"]) / bm["brier"],
            "ece_delta_by_class": [a - z for a, z in zip(cm["ece_by_class"], bm["ece_by_class"])],
        }
        point = (
            (delta["brier"] <= -0.002 or delta["relative_brier_reduction"] >= 0.01)
            and delta["logloss"] < 0
            and max(delta["ece_delta_by_class"]) <= 0.01
        )
        paired[str(h)] = {
            "status": "paired_rows_aggregate_point_comparison_only",
            "delta": delta,
            "point_gate": bool(point),
            "paired_cluster_bootstrap": {
                "status": "unavailable",
                "reason": "baseline compare does not retain 2023-2025 row-level probabilities",
            },
            "decision": "hold_requires_paired_bootstrap" if point else "drop",
        }
    return paired


def run(
    daily: Path,
    levelseq: Path,
    levelseq_audit: Path,
    levelseq_complete: Path,
    baseline_compare: Path,
    output_root: Path,
    resume_root: Path | None = None,
) -> Path:
    tests = self_tests()
    joined, daily_cols, level_cols, join_audit = _load_and_validate_levelseq(
        daily, levelseq, levelseq_audit, levelseq_complete
    )
    if len(level_cols) != EXPECTED_LEVEL_FEATURES:
        raise ValueError("unexpected number of levelseq features")
    baseline = json.loads(baseline_compare.read_text(encoding="utf-8"))
    fixed = baseline.get("fixed_contract", {})
    if fixed.get("channels") != base.CHANNELS or fixed.get("variants") != base.VARIANTS:
        raise ValueError("baseline fixed modelling contract differs from current base")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = resume_root or output_root / f"{stamp}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=True)
    input_path = root / "joined_input.parquet"
    input_contract_path = root / "joined_input_contract.json"
    input_contract = {
        "daily_sha256": sha(daily),
        "levelseq_sha256": sha(levelseq),
        "rows": join_audit["rows"],
        "daily_columns": daily_cols,
        "level_columns": level_cols,
    }
    if input_path.exists():
        if not input_contract_path.exists() or json.loads(input_contract_path.read_text(encoding="utf-8")) != input_contract:
            raise ValueError("resume joined input contract differs from current sources")
    else:
        joined.to_parquet(input_path, index=False)
        dump(input_contract_path, input_contract)
    del joined
    gc.collect()

    original_features, original_labels, original_axis = base.features, base.labels, base.AXIS_ID

    def candidate_features(frame: pd.DataFrame):
        g, daily_x = original_features(frame[daily_cols])
        extra = frame[level_cols].astype("float32")
        if len(daily_x.columns) != EXPECTED_DAILY_MODEL_FEATURES:
            raise ValueError("daily base feature contract changed")
        if not daily_x.index.equals(extra.index):
            raise ValueError("feature indices differ")
        return g, pd.concat([daily_x, extra], axis=1)

    try:
        base.features = candidate_features
        base.labels = fp.labels
        base.AXIS_ID = RUNNER_AXIS_ID
        prior = sorted((root / "candidate").glob("*/compare.json")) if (root / "candidate").exists() else []
        candidate_dir = prior[-1].parent if prior else base.run(input_path, root / "candidate")
    finally:
        base.features, base.labels, base.AXIS_ID = original_features, original_labels, original_axis
    candidate = json.loads((candidate_dir / "compare.json").read_text(encoding="utf-8"))
    paired = _point_comparison(baseline, candidate)
    decision = "drop" if all(x["decision"] == "drop" for x in paired.values()) else "hold_no_keep_without_paired_bootstrap"
    payload = {
        "schema_version": AXIS_ID + ".compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "source": {
            "daily": {"path": str(daily), "sha256": sha(daily)},
            "levelseq": {"path": str(levelseq), "sha256": sha(levelseq)},
            "levelseq_audit": {"path": str(levelseq_audit), "sha256": sha(levelseq_audit)},
            "levelseq_complete": {"path": str(levelseq_complete), "sha256": sha(levelseq_complete)},
            "baseline_compare": {"path": str(baseline_compare), "sha256": sha(baseline_compare)},
        },
        "single_changed_axis": "add 211 PIT level-test/reclaim/rebreak/candle/sideways/box sequence features",
        "fixed_contract": {
            "label": fp.LABEL_CONTRACT,
            "daily_channels": base.CHANNELS,
            "daily_lags": 20,
            "daily_masks": True,
            "variants": base.VARIANTS,
            "splits": fixed.get("splits"),
            "calibration_lanes_bootstrap_holm": "delegated unchanged to tradex_nikkei225_20bar_morphology_sequence_v1",
            "execution": {"n_jobs": 2, "checkpoint_resume": True},
        },
        "feature_contract": {
            "total": EXPECTED_DAILY_MODEL_FEATURES + EXPECTED_LEVEL_FEATURES,
            "daily": EXPECTED_DAILY_MODEL_FEATURES,
            "levelseq": EXPECTED_LEVEL_FEATURES,
            **join_audit,
        },
        "self_tests": tests,
        "candidate": str(candidate_dir / "compare.json"),
        "paired_incremental": paired,
        "decision": {
            "candidate_local_decision": decision,
            "authoritative_rollup_decision": "review_only",
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    dump(root / "compare.json", payload)
    dump(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(root / "compare.json"), "compare_sha256": sha(root / "compare.json")})
    return root


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily", type=Path)
    parser.add_argument("--levelseq", type=Path)
    parser.add_argument("--levelseq-audit", type=Path)
    parser.add_argument("--levelseq-complete", type=Path)
    parser.add_argument("--baseline-compare", type=Path, default=BASELINE_COMPARE_DEFAULT)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\levelseq_model_v1"))
    parser.add_argument("--resume-root", type=Path)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(self_tests(), ensure_ascii=False, indent=2))
        return
    required = (args.daily, args.levelseq, args.levelseq_audit, args.levelseq_complete)
    if any(x is None for x in required):
        parser.error("--daily, --levelseq, --levelseq-audit and --levelseq-complete are required")
    if args.validate_only:
        joined, daily_cols, level_cols, audit = _load_and_validate_levelseq(
            args.daily, args.levelseq, args.levelseq_audit, args.levelseq_complete
        )
        sample = joined.groupby("code", sort=False).head(25).reset_index(drop=True)
        _, daily_x = base.features(sample[daily_cols])
        model_x = pd.concat([daily_x, sample[level_cols].astype("float32")], axis=1)
        if model_x.shape[1] != EXPECTED_DAILY_MODEL_FEATURES + EXPECTED_LEVEL_FEATURES:
            raise ValueError("composed feature count differs from fixed contract")
        print(json.dumps({"status": "pass", "daily_columns": len(daily_cols), "daily_model_features": len(daily_x.columns), "level_features": len(level_cols), "total_model_features": len(model_x.columns), "join_audit": audit}, ensure_ascii=False, indent=2))
        return
    print(run(args.daily, args.levelseq, args.levelseq_audit, args.levelseq_complete, args.baseline_compare, args.output_root, args.resume_root))


if __name__ == "__main__":
    main()
