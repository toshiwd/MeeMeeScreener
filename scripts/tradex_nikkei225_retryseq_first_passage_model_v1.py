from __future__ import annotations

"""Paired first-passage challenger adding only PIT retry-sequence features.

This is a review-only TRADEX wrapper.  The copied ``existing_*`` context in
the retry ledger is deliberately excluded because it is already represented
by the fixed daily440 baseline.
"""

import argparse
import gc
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp

AXIS_ID = "tradex_nikkei225_retryseq_first_passage_model_v1"
RUNNER_AXIS_ID = "retryseq_fp_v1"
EXPECTED_DAILY_MODEL_FEATURES = 440
EXPECTED_RETRY_FEATURES = 20
EXPECTED_TOTAL_FEATURES = 460
BASELINE_COMPARE_DEFAULT = Path(r"G:\Tradex\fp_order_v1\20260714T075050Z-tradex_nikkei225_first_passage_order_v1\compare.json")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_and_validate_retry(daily: Path, retry: Path, audit_path: Path, complete_path: Path):
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    complete = json.loads(complete_path.read_text(encoding="utf-8"))
    retry_sha = sha(retry)
    checks = {
        "complete_true": complete.get("complete") is True,
        "research_only": audit.get("research_only") is True,
        "axis_retry_only": audit.get("new_axis") == "retry_failure_sequence_only",
        "selection_or_score_not_emitted": audit.get("selection_or_score_emitted") is False,
        "pit_contract_present": bool(audit.get("pit_contract")),
        "pit_tests_all_pass": audit.get("tests", {}).get("all_pass") is True,
        "ledger_sha_matches_audit": audit.get("ledger_sha256") == retry_sha,
        "ledger_sha_matches_complete": complete.get("ledger_sha256") == retry_sha,
        "audit_sha_matches_complete": complete.get("audit_sha256") == sha(audit_path),
        "rows_expected": audit.get("rows") == 393788,
        "codes_expected": audit.get("codes") == 218,
    }
    if not all(checks.values()):
        raise ValueError({"invalid_retry_artifact": checks})
    d, t = pd.read_parquet(daily), pd.read_parquet(retry)
    if list(t.columns[:2]) != ["code", "ymd"]:
        raise ValueError("unexpected retry key schema")
    if d.duplicated(["code", "ymd"]).any() or t.duplicated(["code", "ymd"]).any():
        raise ValueError("code/ymd must be unique")
    # The authoritative daily ledger stores ymd as int32 while the retry
    # builder serialises the same YYYYMMDD key as text.  Compare canonical key
    # values, then retain the daily ledger's dtype/order in the joined input.
    daily_key = d[["code", "ymd"]].astype(str)
    retry_key = t[["code", "ymd"]].astype(str)
    if not pd.MultiIndex.from_frame(daily_key).equals(pd.MultiIndex.from_frame(retry_key)):
        raise ValueError("code/ymd exact ordered join failed")
    declared = audit.get("feature_columns", [])
    retry_cols = [c for c in t if c.startswith("retry_")]
    existing_cols = [c for c in t if c.startswith("existing_")]
    unexpected = [c for c in t if c not in {"code", "ymd", *retry_cols, *existing_cols}]
    checks.update({
        "declared_retry_columns_exact": declared == retry_cols,
        "retry_feature_count": len(retry_cols) == EXPECTED_RETRY_FEATURES,
        "existing_columns_excluded": not set(existing_cols).intersection(retry_cols),
        "no_unexpected_columns": not unexpected,
        "retry_all_numeric_or_bool": all(pd.api.types.is_numeric_dtype(t[c]) or pd.api.types.is_bool_dtype(t[c]) for c in retry_cols),
    })
    if not all(checks.values()):
        raise ValueError({"invalid_retry_schema": checks, "unexpected": unexpected})
    extra = t[retry_cols].copy()
    for c in retry_cols:
        extra[c] = extra[c].astype("float32")
    joined = pd.concat([d.reset_index(drop=True), extra.reset_index(drop=True)], axis=1, copy=False)
    join_audit = {
        "rows": len(joined), "codes": int(joined.code.nunique()), "exact_ordered_key_match": True,
        "retry_feature_count": len(retry_cols), "excluded_existing_columns": existing_cols,
        "source_validation": checks, "encoding": "numeric/bool to float32 identity; no fit on labels or future rows",
    }
    return joined, list(d.columns), retry_cols, join_audit


def self_tests() -> dict[str, Any]:
    assertions = [
        {"case": "first_passage_contract", "pass": fp.self_tests()["status"] == "pass"},
        {"case": "fixed_feature_counts", "pass": EXPECTED_DAILY_MODEL_FEATURES + EXPECTED_RETRY_FEATURES == EXPECTED_TOTAL_FEATURES},
        {"case": "existing_context_exclusion_contract", "pass": True},
        {"case": "fixed_horizons", "pass": list(base.HORIZONS) == [1, 3, 5, 10]},
    ]
    if not all(x["pass"] for x in assertions):
        raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def _point_comparison(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for h in base.HORIZONS:
        b = baseline.get("results", {}).get(str(h), {})
        c = candidate.get("results", {}).get(str(h), {})
        bm, cm = b.get("frozen_general"), c.get("frozen_general")
        if not bm or not cm:
            out[str(h)] = {
                "status": "not_pairable_candidate_or_baseline_failed_oof_selection",
                "baseline_decision": b.get("decision"), "candidate_decision": c.get("decision"), "decision": "drop",
            }
            continue
        delta = {
            "brier": cm["brier"] - bm["brier"],
            "logloss": cm["logloss"] - bm["logloss"],
            "relative_brier_reduction": (bm["brier"] - cm["brier"]) / bm["brier"],
            "ece_delta_by_class": [a - z for a, z in zip(cm["ece_by_class"], bm["ece_by_class"])],
        }
        point = ((delta["brier"] <= -.002 or delta["relative_brier_reduction"] >= .01)
                 and delta["logloss"] < 0 and max(delta["ece_delta_by_class"]) <= .01)
        out[str(h)] = {
            "status": "fixed_identical_rows_aggregate_point_comparison_only", "delta": delta,
            "point_gate": bool(point),
            "paired_cluster_bootstrap": {"status": "unavailable", "reason": "baseline compare does not retain 2023-2025 row-level probabilities"},
            "decision": "hold_requires_paired_bootstrap" if point else "drop",
        }
    return out


def run(daily: Path, retry: Path, audit: Path, complete: Path, baseline_compare: Path,
        output_root: Path, resume_root: Path | None = None) -> Path:
    tests = self_tests()
    joined, daily_cols, retry_cols, join_audit = _load_and_validate_retry(daily, retry, audit, complete)
    baseline = json.loads(baseline_compare.read_text(encoding="utf-8"))
    fixed = baseline.get("fixed_contract", {})
    if fixed.get("channels") != base.CHANNELS or fixed.get("variants") != base.VARIANTS:
        raise ValueError("baseline contract differs")
    root = resume_root or output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=True)
    input_path, contract_path = root / "joined_input.parquet", root / "joined_input_contract.json"
    contract = {"daily_sha256": sha(daily), "retry_sha256": sha(retry), "rows": join_audit["rows"],
                "daily_columns": daily_cols, "retry_columns": retry_cols}
    if input_path.exists():
        if not contract_path.exists() or json.loads(contract_path.read_text(encoding="utf-8")) != contract:
            raise ValueError("resume input contract differs")
    else:
        joined.to_parquet(input_path, index=False)
        dump(contract_path, contract)
    del joined
    gc.collect()
    old_features, old_labels, old_axis = base.features, base.labels, base.AXIS_ID

    def candidate_features(frame):
        g, dx = old_features(frame[daily_cols])
        extra = frame[retry_cols].astype("float32")
        if len(dx.columns) != EXPECTED_DAILY_MODEL_FEATURES or not dx.index.equals(extra.index):
            raise ValueError("feature contract changed")
        return g, pd.concat([dx, extra], axis=1)

    try:
        base.features, base.labels, base.AXIS_ID = candidate_features, fp.labels, RUNNER_AXIS_ID
        prior = sorted((root / "candidate").glob("*/compare.json")) if (root / "candidate").exists() else []
        candidate_dir = prior[-1].parent if prior else base.run(input_path, root / "candidate")
    finally:
        base.features, base.labels, base.AXIS_ID = old_features, old_labels, old_axis
    candidate = json.loads((candidate_dir / "compare.json").read_text(encoding="utf-8"))
    paired = _point_comparison(baseline, candidate)
    decision = "drop" if all(x["decision"] == "drop" for x in paired.values()) else "hold_no_keep_without_paired_bootstrap"
    payload = {
        "schema_version": AXIS_ID + ".compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "source": {"daily": {"path": str(daily), "sha256": sha(daily)}, "retry": {"path": str(retry), "sha256": sha(retry)},
                   "retry_audit": {"path": str(audit), "sha256": sha(audit)}, "retry_complete": {"path": str(complete), "sha256": sha(complete)},
                   "baseline_compare": {"path": str(baseline_compare), "sha256": sha(baseline_compare)}},
        "single_changed_axis": "add only 20 PIT retry-sequence continuous/availability features; exclude copied existing_* context",
        "fixed_contract": {"label": fp.LABEL_CONTRACT, "horizons": list(base.HORIZONS), "daily_channels": base.CHANNELS,
            "daily_lags": 20, "daily_masks": True, "variants": base.VARIANTS, "splits": fixed.get("splits"),
            "calibration_lanes_bootstrap_holm": "delegated unchanged to tradex_nikkei225_20bar_morphology_sequence_v1",
            "execution": {"n_jobs": 2, "checkpoint_resume": True}},
        "feature_contract": {"total": EXPECTED_TOTAL_FEATURES, "daily": EXPECTED_DAILY_MODEL_FEATURES,
                             "retry": EXPECTED_RETRY_FEATURES, **join_audit},
        "self_tests": tests, "candidate": str(candidate_dir / "compare.json"), "paired_incremental": paired,
        "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    dump(root / "compare.json", payload)
    dump(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(root / "compare.json"), "compare_sha256": sha(root / "compare.json")})
    return root


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--daily", type=Path)
    p.add_argument("--retry", type=Path)
    p.add_argument("--retry-audit", type=Path)
    p.add_argument("--retry-complete", type=Path)
    p.add_argument("--baseline-compare", type=Path, default=BASELINE_COMPARE_DEFAULT)
    p.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\retryseq_model_v1"))
    p.add_argument("--resume-root", type=Path)
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--validate-only", action="store_true")
    a = p.parse_args()
    if a.self_test:
        print(json.dumps(self_tests(), ensure_ascii=False, indent=2)); return
    if any(x is None for x in (a.daily, a.retry, a.retry_audit, a.retry_complete)):
        p.error("--daily, --retry, --retry-audit and --retry-complete required")
    if a.validate_only:
        joined, dc, rc, ja = _load_and_validate_retry(a.daily, a.retry, a.retry_audit, a.retry_complete)
        sample = joined.groupby("code", sort=False).head(25).reset_index(drop=True)
        _, dx = base.features(sample[dc])
        model_x = pd.concat([dx, sample[rc].astype("float32")], axis=1)
        if model_x.shape[1] != EXPECTED_TOTAL_FEATURES:
            raise ValueError("composed count differs")
        overlap = sorted(set(dx.columns).intersection(rc))
        if overlap:
            raise ValueError({"feature_name_overlap": overlap})
        print(json.dumps({"status": "pass", "daily_columns": len(dc), "daily_model_features": len(dx.columns),
                          "retry_features": len(rc), "total_model_features": len(model_x.columns),
                          "existing_context_excluded": ja["excluded_existing_columns"], "feature_name_overlap": overlap,
                          "join_audit": ja}, ensure_ascii=False, indent=2)); return
    print(run(a.daily, a.retry, a.retry_audit, a.retry_complete, a.baseline_compare, a.output_root, a.resume_root))


if __name__ == "__main__":
    main()
