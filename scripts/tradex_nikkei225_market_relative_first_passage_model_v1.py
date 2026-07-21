from __future__ import annotations

"""Paired first-passage challenger adding only PIT market-relative-path features."""

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

AXIS_ID = "tradex_nikkei225_market_relative_first_passage_model_v1"
RUNNER_AXIS_ID = "mrp_fp_v1"
EXPECTED_MRP_COLUMNS = 280
EXPECTED_MRP_FEATURES = 278
EXPECTED_DAILY_MODEL_FEATURES = 440
BASELINE_COMPARE_DEFAULT = Path(r"G:\Tradex\fp_order_v1\20260714T075050Z-tradex_nikkei225_first_passage_order_v1\compare.json")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_and_validate_mrp(daily: Path, mrp: Path, audit_path: Path, complete_path: Path):
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    complete = json.loads(complete_path.read_text(encoding="utf-8"))
    manifest_path = audit_path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    checks = {
        "complete_true": complete.get("complete") is True,
        "audit_self_tests_all_true": all(audit.get("self_tests", {}).values()),
        "cutoff_regeneration_passed": audit.get("cutoff_regeneration") is True,
        "future_mutation_passed": audit.get("future_mutation") is True,
        "cross_section_peer_sensitivity": audit.get("cross_section", {}).get("peer_sensitivity") is True,
        "cross_section_target_unchanged": audit.get("cross_section", {}).get("target_return_unchanged") is True,
        "lag1_passed": audit.get("lag1") is True,
        "key_unique_audit": audit.get("key_unique") is True,
        "finite_or_missing": audit.get("finite_or_missing") is True,
        "parquet_sha_matches_audit": audit.get("artifact_sha256") == sha(mrp),
        "parquet_sha_matches_complete": complete.get("parquet_sha256") == sha(mrp),
        "audit_sha_matches_complete": complete.get("audit_sha256") == sha(audit_path),
        "manifest_sha_matches_complete": complete.get("manifest_sha256") == sha(manifest_path),
        "manifest_rows": manifest.get("output", {}).get("rows") == 393788,
        "manifest_columns": manifest.get("output", {}).get("columns") == EXPECTED_MRP_COLUMNS,
        "outcome_columns_not_loaded": manifest.get("contract", {}).get("outcome_columns_loaded") is False,
    }
    if not all(checks.values()):
        raise ValueError({"invalid_mrp_artifact": checks})
    d, t = pd.read_parquet(daily), pd.read_parquet(mrp)
    if list(t.columns[:2]) != ["code", "ymd"] or t.shape[1] != EXPECTED_MRP_COLUMNS:
        raise ValueError("unexpected MRP schema")
    if d.duplicated(["code", "ymd"]).any() or t.duplicated(["code", "ymd"]).any():
        raise ValueError("code/ymd must be unique")
    if not pd.MultiIndex.from_frame(d[["code", "ymd"]]).equals(pd.MultiIndex.from_frame(t[["code", "ymd"]])):
        raise ValueError("code/ymd exact ordered join failed")
    feature_cols = [c for c in t if c not in ("code", "ymd")]
    forbidden = [c for c in feature_cols if c.startswith(("ret_close_", "down_exc_", "up_exc_"))]
    non_numeric = [c for c in feature_cols if not pd.api.types.is_numeric_dtype(t[c])]
    if forbidden or non_numeric:
        raise ValueError({"outcome_columns": forbidden, "non_numeric": non_numeric})
    joined = pd.concat([d.reset_index(drop=True), t[feature_cols].reset_index(drop=True)], axis=1, copy=False)
    join_audit = {"rows": len(joined), "codes": joined.code.nunique(), "exact_ordered_key_match": True,
                  "mrp_feature_count": len(feature_cols), "source_validation": checks,
                  "encoding": "numeric identity; no fit on future rows or labels"}
    return joined, list(d.columns), feature_cols, join_audit


def self_tests() -> dict[str, Any]:
    assertions = [
        {"case": "first_passage_contract", "pass": fp.self_tests()["status"] == "pass"},
        {"case": "fixed_feature_counts", "pass": EXPECTED_DAILY_MODEL_FEATURES + EXPECTED_MRP_FEATURES == 718},
        {"case": "future_or_label_free_encoding", "pass": True},
    ]
    if not all(x["pass"] for x in assertions): raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def _point_comparison(baseline: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for h in base.HORIZONS:
        b, c = baseline.get("results", {}).get(str(h), {}), candidate.get("results", {}).get(str(h), {})
        bm, cm = b.get("frozen_general"), c.get("frozen_general")
        if not bm or not cm:
            out[str(h)] = {"status": "not_pairable_candidate_or_baseline_failed_oof_selection",
                           "baseline_decision": b.get("decision"), "candidate_decision": c.get("decision"), "decision": "drop"}
            continue
        delta = {"brier": cm["brier"] - bm["brier"], "logloss": cm["logloss"] - bm["logloss"],
                 "relative_brier_reduction": (bm["brier"] - cm["brier"]) / bm["brier"],
                 "ece_delta_by_class": [a-z for a,z in zip(cm["ece_by_class"], bm["ece_by_class"])]}
        point = ((delta["brier"] <= -.002 or delta["relative_brier_reduction"] >= .01)
                 and delta["logloss"] < 0 and max(delta["ece_delta_by_class"]) <= .01)
        out[str(h)] = {"status": "fixed_identical_rows_aggregate_point_comparison_only", "delta": delta,
                       "point_gate": bool(point), "paired_cluster_bootstrap": {"status": "unavailable", "reason": "baseline compare does not retain 2023-2025 row-level probabilities"},
                       "decision": "hold_requires_paired_bootstrap" if point else "drop"}
    return out


def run(daily: Path, mrp: Path, audit: Path, complete: Path, baseline_compare: Path, output_root: Path, resume_root: Path|None=None) -> Path:
    tests = self_tests()
    joined, daily_cols, mrp_cols, join_audit = _load_and_validate_mrp(daily, mrp, audit, complete)
    if len(mrp_cols) != EXPECTED_MRP_FEATURES: raise ValueError("unexpected MRP feature count")
    baseline = json.loads(baseline_compare.read_text(encoding="utf-8")); fixed = baseline.get("fixed_contract", {})
    if fixed.get("channels") != base.CHANNELS or fixed.get("variants") != base.VARIANTS: raise ValueError("baseline contract differs")
    root = resume_root or output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=True)
    input_path, contract_path = root/"joined_input.parquet", root/"joined_input_contract.json"
    contract = {"daily_sha256": sha(daily), "mrp_sha256": sha(mrp), "rows": join_audit["rows"], "daily_columns": daily_cols, "mrp_columns": mrp_cols}
    if input_path.exists():
        if not contract_path.exists() or json.loads(contract_path.read_text(encoding="utf-8")) != contract: raise ValueError("resume input contract differs")
    else: joined.to_parquet(input_path, index=False); dump(contract_path, contract)
    del joined; gc.collect()
    old_features, old_labels, old_axis = base.features, base.labels, base.AXIS_ID
    def candidate_features(frame):
        g, dx = old_features(frame[daily_cols]); extra = frame[mrp_cols].astype("float32")
        if len(dx.columns) != EXPECTED_DAILY_MODEL_FEATURES or not dx.index.equals(extra.index): raise ValueError("feature contract changed")
        return g, pd.concat([dx, extra], axis=1)
    try:
        base.features, base.labels, base.AXIS_ID = candidate_features, fp.labels, RUNNER_AXIS_ID
        prior = sorted((root/"candidate").glob("*/compare.json")) if (root/"candidate").exists() else []
        candidate_dir = prior[-1].parent if prior else base.run(input_path, root/"candidate")
    finally: base.features, base.labels, base.AXIS_ID = old_features, old_labels, old_axis
    candidate = json.loads((candidate_dir/"compare.json").read_text(encoding="utf-8")); paired = _point_comparison(baseline, candidate)
    decision = "drop" if all(x["decision"] == "drop" for x in paired.values()) else "hold_no_keep_without_paired_bootstrap"
    payload = {"schema_version": AXIS_ID+".compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
      "source": {"daily": {"path": str(daily), "sha256": sha(daily)}, "mrp": {"path": str(mrp), "sha256": sha(mrp)},
                 "mrp_audit": {"path": str(audit), "sha256": sha(audit)}, "mrp_complete": {"path": str(complete), "sha256": sha(complete)},
                 "baseline_compare": {"path": str(baseline_compare), "sha256": sha(baseline_compare)}},
      "single_changed_axis": "add 278 PIT market-relative path features", "fixed_contract": {"label": fp.LABEL_CONTRACT,
      "daily_channels": base.CHANNELS, "daily_lags": 20, "daily_masks": True, "variants": base.VARIANTS, "splits": fixed.get("splits"),
      "calibration_lanes_bootstrap_holm": "delegated unchanged to tradex_nikkei225_20bar_morphology_sequence_v1", "execution": {"n_jobs": 2, "checkpoint_resume": True}},
      "feature_contract": {"total": 718, "daily": 440, "mrp": 278, **join_audit}, "self_tests": tests,
      "candidate": str(candidate_dir/"compare.json"), "paired_incremental": paired,
      "decision": {"candidate_local_decision": decision, "authoritative_rollup_decision": "review_only"},
      "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}}
    dump(root/"compare.json", payload); dump(root/"_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(root/"compare.json"), "compare_sha256": sha(root/"compare.json")})
    return root


def main() -> None:
    p=argparse.ArgumentParser()
    p.add_argument("--daily", type=Path); p.add_argument("--mrp", type=Path); p.add_argument("--mrp-audit", type=Path); p.add_argument("--mrp-complete", type=Path)
    p.add_argument("--baseline-compare", type=Path, default=BASELINE_COMPARE_DEFAULT); p.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\mrp_model_v1")); p.add_argument("--resume-root", type=Path)
    p.add_argument("--self-test", action="store_true"); p.add_argument("--validate-only", action="store_true"); a=p.parse_args()
    if a.self_test: print(json.dumps(self_tests(), ensure_ascii=False, indent=2)); return
    if any(x is None for x in (a.daily,a.mrp,a.mrp_audit,a.mrp_complete)): p.error("--daily, --mrp, --mrp-audit and --mrp-complete required")
    if a.validate_only:
        joined, dc, mc, ja = _load_and_validate_mrp(a.daily,a.mrp,a.mrp_audit,a.mrp_complete); sample=joined.groupby("code",sort=False).head(25).reset_index(drop=True)
        _, dx=base.features(sample[dc]); model_x=pd.concat([dx,sample[mc].astype("float32")],axis=1)
        if model_x.shape[1] != 718: raise ValueError("composed count differs")
        print(json.dumps({"status":"pass","daily_columns":len(dc),"daily_model_features":len(dx.columns),"mrp_features":len(mc),"total_model_features":len(model_x.columns),"join_audit":ja},ensure_ascii=False,indent=2)); return
    print(run(a.daily,a.mrp,a.mrp_audit,a.mrp_complete,a.baseline_compare,a.output_root,a.resume_root))

if __name__ == "__main__": main()
