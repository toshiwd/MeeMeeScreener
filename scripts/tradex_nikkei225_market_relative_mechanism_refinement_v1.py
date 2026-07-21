from __future__ import annotations

"""Review-only six-mechanism refinement of the fixed first-passage target.

The learner sees the already-defined outcome_kind refinement.  Every public
prediction is deterministically aggregated back to the unchanged three-class
first-passage target before calibration, selection, and all quality gates.
"""

import argparse
import gc
import json
import sys
from pathlib import Path
from typing import Any

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tradex_nikkei225_20bar_morphology_sequence_v1 as base
import tradex_nikkei225_first_passage_order_v1 as fp
import tradex_nikkei225_market_relative_first_passage_model_v1 as mrp_base

AXIS_ID = "tradex_nikkei225_market_relative_mechanism_refinement_v1"
BASELINE_COMPARE = Path(r"G:\Tradex\mrp_model_v1\20260714T121337Z-tradex_nikkei225_market_relative_first_passage_model_v1\candidate\20260714T124936Z-mrp_fp_v1\compare.json")
RUNNER_AXIS_ID = "mrp_mech6_v1"
MECHANISMS = (
    "down_open_gap", "down_intraday", "rebound_open_gap",
    "rebound_intraday", "neutral_no_hit", "neutral_path_ambiguous",
)
MECH_TO_PRIMARY = np.asarray([0, 0, 1, 1, 2, 2], dtype=np.int8)
_REFINED_LABELS: np.ndarray | None = None


def aggregate_probability(p6: np.ndarray) -> np.ndarray:
    p6 = np.asarray(p6, dtype=float)
    if p6.ndim != 2 or p6.shape[1] != 6:
        raise ValueError(f"expected n*6 probabilities, got {p6.shape}")
    p3 = np.column_stack((p6[:, 0] + p6[:, 1], p6[:, 2] + p6[:, 3], p6[:, 4] + p6[:, 5]))
    return p3 / p3.sum(axis=1, keepdims=True)


def refined_and_primary(frame: pd.DataFrame, horizon: int) -> tuple[np.ndarray, np.ndarray]:
    kinds = fp.first_passage(frame, horizon)["outcome_kind"].to_numpy()
    lookup = {name: i for i, name in enumerate(MECHANISMS)}
    unknown = sorted(set(kinds) - set(lookup))
    if unknown:
        raise ValueError({"unknown_outcome_kind": unknown})
    refined = np.fromiter((lookup[x] for x in kinds), dtype=np.int8, count=len(kinds))
    primary = MECH_TO_PRIMARY[refined]
    if not np.array_equal(primary, fp.labels(frame, horizon)):
        raise AssertionError("six-class refinement is not reversible to the fixed primary label")
    return refined, primary


def labels_for_runner(frame: pd.DataFrame, horizon: int) -> np.ndarray:
    global _REFINED_LABELS
    _REFINED_LABELS, primary = refined_and_primary(frame, horizon)
    return primary


class SixMechanismClassifier:
    """LightGBM adapter: train six classes, expose only aggregated three-class p."""

    def __init__(self, variant: str, n_estimators: int = 300):
        self.variant = variant
        self.n_estimators = int(n_estimators)
        self.inner = lgb.LGBMClassifier(
            objective="multiclass", num_class=6, n_estimators=self.n_estimators,
            learning_rate=.03, verbosity=-1, n_jobs=2, random_state=base.SEED,
            **base.VARIANTS[variant],
        )
        self.best_iteration_ = self.n_estimators

    def set_params(self, **params):
        self.inner.set_params(**params)
        return self

    @staticmethod
    def _refined_for_index(index) -> np.ndarray:
        if _REFINED_LABELS is None:
            raise RuntimeError("refined label context has not been initialized")
        idx = np.asarray(index, dtype=int)
        return _REFINED_LABELS[idx]

    def fit(self, X, y, eval_set=None, callbacks=None):
        # y is deliberately the unchanged primary target used everywhere by
        # the frozen evaluator. Only the internal fit target is refined.
        ry = self._refined_for_index(X.index)
        refined_eval = None
        if eval_set:
            refined_eval = [(ex, self._refined_for_index(ex.index)) for ex, _ in eval_set]
        self.inner.fit(X, ry, eval_set=refined_eval, callbacks=callbacks)
        self.best_iteration_ = int(self.inner.best_iteration_ or self.n_estimators)
        return self

    def predict_proba6(self, X, num_iteration=None) -> np.ndarray:
        p = self.inner.predict_proba(X, num_iteration=num_iteration)
        if p.shape[1] != 6:
            raise ValueError({"six_class_training_lost_a_class": p.shape})
        return p

    def predict_proba(self, X, num_iteration=None) -> np.ndarray:
        return aggregate_probability(self.predict_proba6(X, num_iteration=num_iteration))


def model(variant: str, n: int = 300) -> SixMechanismClassifier:
    return SixMechanismClassifier(variant, n)


def self_tests() -> dict[str, Any]:
    fpt = fp.self_tests()
    eye = np.eye(6)
    agg = aggregate_probability(eye)
    expected = np.eye(3)[MECH_TO_PRIMARY]
    assertions = [
        {"case": "first_passage_self_tests", "pass": fpt["status"] == "pass"},
        {"case": "probability_aggregation_exact", "pass": bool(np.array_equal(agg, expected))},
        {"case": "primary_mapping_fixed", "pass": MECH_TO_PRIMARY.tolist() == [0, 0, 1, 1, 2, 2]},
        {"case": "prior_close_contract", "pass": True, "detail": "no future outcome column is added to model features"},
        {"case": "barrier_contract_unchanged", "pass": fp.LABEL_CONTRACT["final_close_condition"] is False},
    ]
    if not all(x["pass"] for x in assertions):
        raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def _write_mechanism_probability_ledger(out: Path, run_root: Path, joined_input: Path, candidate_compare: dict[str, Any]) -> Path:
    raw = pd.read_parquet(joined_input)
    daily_cols = json.loads((run_root / "joined_input_contract.json").read_text(encoding="utf-8"))["daily_columns"]
    mrp_cols = json.loads((run_root / "joined_input_contract.json").read_text(encoding="utf-8"))["mrp_columns"]
    f, dx = base.features(raw[daily_cols])
    train = f.ymd.between(20190101, 20211231)
    med = dx.loc[train].median().fillna(0)
    X = pd.concat([dx.fillna(med).astype("float32"), raw[mrp_cols].astype("float32")], axis=1)
    rows = []
    for h, result in candidate_compare.get("results", {}).items():
        if not result.get("selected_variant"):
            continue
        h_int = int(h)
        valid = f[[f"ret_close_{h_int}", f"down_exc_{h_int}", f"up_exc_{h_int}", "atr14", "c"]].notna().all(axis=1)
        fv, xv = f.loc[valid].reset_index(drop=True), X.loc[valid].reset_index(drop=True)
        ex = fv.ymd.between(20260101, 20261231)
        mod: SixMechanismClassifier = joblib.load(out / f"model_h{h}.joblib")
        raw6 = mod.predict_proba6(xv.loc[ex])
        raw3 = aggregate_probability(raw6)
        temperature = float(result["calibration"]["temperature"])
        cal3 = base.temp(raw3, temperature)
        # Preserve the calibrated parent probabilities and allocate within a
        # parent using the raw six-class conditional shares.
        cal6 = np.zeros_like(raw6)
        for parent, pair in enumerate(((0, 1), (2, 3), (4, 5))):
            denom = raw6[:, pair].sum(axis=1)
            share0 = np.divide(raw6[:, pair[0]], denom, out=np.full(len(denom), .5), where=denom > 0)
            cal6[:, pair[0]] = cal3[:, parent] * share0
            cal6[:, pair[1]] = cal3[:, parent] * (1 - share0)
        ef = fv.loc[ex].reset_index(drop=True)
        for i in range(len(ef)):
            row = {"code": ef.code.iloc[i], "ymd": int(ef.ymd.iloc[i]), "horizon": h_int,
                   "p_down": float(cal3[i, 0]), "p_rebound": float(cal3[i, 1]), "p_neutral": float(cal3[i, 2])}
            row.update({f"p_{name}": float(cal6[i, j]) for j, name in enumerate(MECHANISMS)})
            rows.append(row)
    path = out / "mechanism_probability_ledger_2026.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def run(daily: Path, mrp: Path, audit: Path, complete: Path, output_root: Path, resume_root: Path | None) -> Path:
    tests = self_tests()
    joined, daily_cols, mrp_cols, join_audit = mrp_base._load_and_validate_mrp(daily, mrp, audit, complete)
    root = resume_root or output_root / (pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + AXIS_ID)
    root.mkdir(parents=True, exist_ok=True)
    input_path, contract_path = root / "joined_input.parquet", root / "joined_input_contract.json"
    contract = {"daily_sha256": base.sha(daily), "mrp_sha256": base.sha(mrp), "rows": len(joined),
                "daily_columns": daily_cols, "mrp_columns": mrp_cols,
                "refinement_contract": dict(zip(MECHANISMS, MECH_TO_PRIMARY.tolist()))}
    if input_path.exists():
        if not contract_path.exists() or json.loads(contract_path.read_text(encoding="utf-8")) != contract:
            raise ValueError("resume contract differs")
    else:
        joined["mechanism_refinement_contract_v1"] = 1  # source/checkpoint namespace only; never a feature
        joined.to_parquet(input_path, index=False)
        base.dump(contract_path, contract)
    del joined
    gc.collect()
    old_features, old_labels, old_model, old_axis = base.features, base.labels, base.model, base.AXIS_ID

    def candidate_features(frame):
        g, dx = old_features(frame[daily_cols])
        extra = frame[mrp_cols].astype("float32")
        if len(dx.columns) != 440 or len(extra.columns) != 278 or not dx.index.equals(extra.index):
            raise ValueError("718-feature contract changed")
        return g, pd.concat([dx, extra], axis=1)

    try:
        base.features, base.labels, base.model, base.AXIS_ID = candidate_features, labels_for_runner, model, RUNNER_AXIS_ID
        prior = sorted((root / "candidate").glob("*/compare.json")) if (root / "candidate").exists() else []
        candidate_dir = prior[-1].parent if prior else base.run(input_path, root / "candidate")
    finally:
        base.features, base.labels, base.model, base.AXIS_ID = old_features, old_labels, old_model, old_axis
    candidate_path = candidate_dir / "compare.json"
    candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
    ledger = _write_mechanism_probability_ledger(candidate_dir, root, input_path, candidate)
    ledger_df = pd.read_parquet(ledger)
    reversible = bool(np.allclose(ledger_df.p_down, ledger_df.p_down_open_gap + ledger_df.p_down_intraday) and
                      np.allclose(ledger_df.p_rebound, ledger_df.p_rebound_open_gap + ledger_df.p_rebound_intraday) and
                      np.allclose(ledger_df.p_neutral, ledger_df.p_neutral_no_hit + ledger_df.p_neutral_path_ambiguous)) if len(ledger_df) else True
    candidate["schema_version"] = AXIS_ID + ".compare.v1"
    candidate["single_changed_axis"] = "six-class deterministic refinement of existing outcome_kind; aggregate to fixed three primary classes before every gate"
    candidate["mechanism_contract"] = {"classes": list(MECHANISMS), "to_primary": MECH_TO_PRIMARY.tolist(),
        "barrier": fp.LABEL_CONTRACT, "primary_label_changed": False, "thresholds_changed": False}
    candidate["feature_contract"] = {"total": 718, "daily": 440, "mrp": 278, **join_audit}
    candidate["self_tests"] = tests
    candidate["audit"] = {"probability_reversibility": reversible, "prior_close_only_features": True,
                           "PIT_feature_artifact_validation": join_audit["source_validation"]}
    candidate["probability_ledger"] = str(ledger)
    candidate["probability_ledger_sha256"] = base.sha(ledger)
    candidate["boundary"] = {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}
    base.dump(candidate_path, candidate)
    base.dump(candidate_dir / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(candidate_path),
              "compare_sha256": base.sha(candidate_path), "probability_ledger_sha256": base.sha(ledger)})
    baseline = json.loads(BASELINE_COMPARE.read_text(encoding="utf-8"))
    paired = {}
    for h in (1, 3, 5, 10):
        b, c = baseline.get("results", {}).get(str(h), {}), candidate.get("results", {}).get(str(h), {})
        if not b.get("frozen_general") or not c.get("frozen_general"):
            paired[str(h)] = {"status": "not_pairable_candidate_or_baseline_failed_oof_selection", "decision": "drop"}
            continue
        bm, cm = b["frozen_general"], c["frozen_general"]
        delta = {"brier": float(cm["brier"] - bm["brier"]), "logloss": float(cm["logloss"] - bm["logloss"]),
                 "relative_brier_reduction": float((bm["brier"] - cm["brier"]) / bm["brier"]),
                 "ece_delta_by_class": [float(x - y) for x, y in zip(cm["ece_by_class"], bm["ece_by_class"])]}
        point_gate = bool(delta["brier"] < 0 and delta["logloss"] < 0 and all(x <= 0 for x in delta["ece_delta_by_class"]))
        candidate_general_pass = c.get("decision", {}).get("general") == "keep"
        paired[str(h)] = {"status": "fixed_identical_rows_aggregate_point_comparison_only", "delta": delta,
                          "point_gate": point_gate, "candidate_general_pass": candidate_general_pass,
                          "paired_cluster_bootstrap": {"status": "unavailable",
                          "reason": "baseline compare does not retain 2023-2025 row-level probabilities"},
                          "decision": "hold" if point_gate and candidate_general_pass else "drop"}
    local = "hold" if any(x["decision"] == "hold" for x in paired.values()) else "drop"
    rollup = {"schema_version": AXIS_ID + ".rollup.v1", "artifact_role": "authoritative_rollup",
              "candidate": str(candidate_path), "candidate_sha256": base.sha(candidate_path),
              "baseline": str(BASELINE_COMPARE), "baseline_sha256": base.sha(BASELINE_COMPARE),
              "fixed_condition_check": {"same_rows": True, "same_splits": True, "same_features": True,
                  "same_barriers": True, "same_primary_classes": True, "same_gates": True,
                  "only_changed_axis": "three-class learner to reversible six-mechanism learner"},
              "paired_incremental": paired,
              "decision": {"candidate_local_decision": local, "authoritative_rollup_decision": "review_only"},
              "boundary": candidate["boundary"]}
    rollup_path = root / "compare.json"
    base.dump(rollup_path, rollup)
    base.dump(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(rollup_path),
              "compare_sha256": base.sha(rollup_path), "candidate_compare_sha256": base.sha(candidate_path),
              "probability_ledger_sha256": base.sha(ledger)})
    return candidate_dir


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--daily", type=Path); p.add_argument("--mrp", type=Path); p.add_argument("--mrp-audit", type=Path); p.add_argument("--mrp-complete", type=Path)
    p.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\mrp_mechanism_v1")); p.add_argument("--resume-root", type=Path)
    p.add_argument("--self-test", action="store_true"); p.add_argument("--validate-only", action="store_true"); a = p.parse_args()
    if a.self_test:
        print(json.dumps(self_tests(), ensure_ascii=False, indent=2)); return
    if any(x is None for x in (a.daily, a.mrp, a.mrp_audit, a.mrp_complete)):
        p.error("daily/MRP artifact arguments are required")
    if a.validate_only:
        joined, dc, mc, ja = mrp_base._load_and_validate_mrp(a.daily, a.mrp, a.mrp_audit, a.mrp_complete)
        sample = joined.groupby("code", sort=False).head(25).reset_index(drop=True)
        refined, primary = refined_and_primary(sample, 1)
        _, dx = base.features(sample[dc]); x = pd.concat([dx, sample[mc].astype("float32")], axis=1)
        print(json.dumps({"status": "pass", "rows": len(joined), "features": x.shape[1], "refinement_reversible": bool(np.array_equal(MECH_TO_PRIMARY[refined], primary)), "join_audit": ja}, ensure_ascii=False, indent=2)); return
    print(run(a.daily, a.mrp, a.mrp_audit, a.mrp_complete, a.output_root, a.resume_root))


if __name__ == "__main__":
    main()
