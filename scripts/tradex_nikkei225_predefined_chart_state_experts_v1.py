from __future__ import annotations

"""Pre-registered chart-state experts for first-passage daily assessment.

The router is deterministic and is evaluated on the full PIT level-sequence
ledger.  Daily 20-bar features and first-passage labels are also computed on
the full daily panel *before* state filtering; otherwise a state's next
eligible row would incorrectly become its next trading day.
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
import tradex_nikkei225_levelseq_first_passage_model_v1 as level_model


AXIS_ID = "tradex_nikkei225_predefined_chart_state_experts_v1"
RUNNER_AXIS_PREFIX = "stx_v1"
STATES = (
    "BREAKDOWN_REBREAK_NO_RESET",
    "SUPPORT_EXHAUSTION_RECLAIM",
    "SIDEWAYS_FAILED_BREAK",
)
UNJUDGEABLE = "UNJUDGEABLE"
ELIGIBILITY = {
    "scope": "train subset only; evaluated separately for every state/horizon/fold",
    "rows": 3000, "codes": 100, "months": "distinct_state_months >= min(36, distinct_train_calendar_months)", "each_label": 250,
    "oof": "each 2019-2021 expanding train subset; failed fold has no test prediction",
    "fit_2022": "all available 2019-2021 train rows",
    "fit_frozen_2023_2025": "all available rows through 2022",
    "failure": "UNJUDGEABLE; no threshold or eligibility relaxation",
}
CONTRACT_CORRECTION = {
    "reason": "source_starts_20190104_no_pre2019_rows",
    "removed": "global ymd < 20190101 eligibility",
    "replacement": "train-subset-only eligibility at each OOF fold and later fit stage",
    "router_changed": False,
    "structural_month_correction": {
        "reason": "oof_train_max_33_months_made_fixed36_impossible",
        "old": "distinct_state_months >= 36",
        "new": "distinct_state_months >= min(36, distinct_train_calendar_months)",
        "other_thresholds_changed": False,
    },
}
EXPECTED_DAILY_FEATURES = 440
EXPECTED_LEVEL_FEATURES = 211


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _true(s: pd.Series) -> pd.Series:
    return s.fillna(0).eq(1)


def assign_states(level: pd.DataFrame) -> tuple[pd.Series, dict[str, Any]]:
    """Apply the frozen mutually-exclusive router in its registered priority."""
    s20, m20, m60 = "support20_prior", "ma20", "ma60"
    required = [
        "support20_prior_lifecycle_state", "support20_prior_no_reset_below",
        "ma60_lifecycle_state", "ma60_no_reset_below", "ma20_lifecycle_state",
        "ma20_no_reset_below", "ma7_level_z", "ma7_consecutive_below",
        "support20_prior_test_count20", "support20_prior_test_age",
        "support20_prior_test_depth_slope20", "support20_prior_level_z",
        "box_age20", "box_history_missing", "box_breakout_reentry_order",
        "box_last_failed_age20", "box_clear_break_confirmed2",
        "box_same_day_both_failed_ambiguous",
    ]
    missing = [c for c in required if c not in level]
    if missing:
        raise ValueError({"missing_router_columns": missing})
    # Priority 1. "same level" refers to support20_prior or MA60.
    lifecycle3 = level[f"{s20}_lifecycle_state"].eq(3) | level[f"{m60}_lifecycle_state"].eq(3)
    lifecycle1_no_reset = (
        (level[f"{s20}_lifecycle_state"].eq(1) & _true(level[f"{s20}_no_reset_below"]))
        | (level[f"{m60}_lifecycle_state"].eq(1) & _true(level[f"{m60}_no_reset_below"]))
    )
    ma20_below = level[f"{m20}_lifecycle_state"].isin([1, 3]) & level["ma7_level_z"].lt(-0.25) & level["ma7_consecutive_below"].ge(2)
    breakdown = lifecycle3 | lifecycle1_no_reset | ma20_below

    # Priority 2.  The exhaustion fallback is an exact five-way conjunction.
    lifecycle2 = level[[f"{s20}_lifecycle_state", f"{m20}_lifecycle_state", f"{m60}_lifecycle_state"]].eq(2).any(axis=1)
    exhaustion = (
        level["support20_prior_test_count20"].ge(3)
        & level["support20_prior_test_age"].le(2)
        & level["support20_prior_test_depth_slope20"].gt(0)
        & level["support20_prior_level_z"].ge(-0.25)
        & level["support20_prior_level_z"].lt(0.10)
    )
    support = (~breakdown) & (lifecycle2 | exhaustion)

    # Priority 3. Missing box history is explicitly ineligible.
    sideways_raw = (
        level["box_history_missing"].fillna(1).eq(0)
        & level["box_age20"].gt(0)
        & level["box_breakout_reentry_order"].ne(0)
        & level["box_last_failed_age20"].le(5)
        & level["box_clear_break_confirmed2"].eq(0)
        & level["box_same_day_both_failed_ambiguous"].eq(0)
    )
    sideways = (~breakdown) & (~support) & sideways_raw
    state = pd.Series(UNJUDGEABLE, index=level.index, dtype="object")
    state.loc[breakdown] = STATES[0]
    state.loc[support] = STATES[1]
    state.loc[sideways] = STATES[2]
    contract = {
        "priority": list(STATES),
        "unmatched_or_missing": UNJUDGEABLE,
        "actual_column_mapping": {
            "support20_prior": s20, "ma20": m20, "ma60": m60,
            "latest_test_age": "support20_prior_test_age",
            "test_depth_progression_slope": "support20_prior_test_depth_slope20",
            "level_z": "support20_prior_level_z",
            "box_history": "box_history_missing == 0 and box_age20 > 0",
            "same_day_both_ambiguity": "box_same_day_both_failed_ambiguous",
        },
        "rules": {
            STATES[0]: "support20_prior or ma60 lifecycle=3; OR same level lifecycle=1 and no_reset_below=1; OR ma20 lifecycle in(1,3) and ma7_level_z<-.25 and ma7_consecutive_below>=2",
            STATES[1]: "if not state1: any support20_prior/ma20/ma60 lifecycle=2; OR support20 test_count20>=3 AND test_age<=2 AND depth_slope20>0 AND -.25<=level_z<.10",
            STATES[2]: "if not state1/2: box history and age>0 and reentry_order!=0 and last_failed_age<=5 and clear_break_confirmed2=0 and same_day_both_failed_ambiguous=0",
        },
    }
    return state, contract


def _prepare(daily: Path, levelseq: Path, audit: Path, complete: Path) -> tuple[pd.DataFrame, list[str], list[str], dict[str, Any]]:
    joined, daily_cols, level_cols, join_audit = level_model._load_and_validate_levelseq(daily, levelseq, audit, complete)
    if len(level_cols) != EXPECTED_LEVEL_FEATURES:
        raise ValueError("level feature count changed")
    ordered, daily_x = base.features(joined[daily_cols])
    if len(daily_x.columns) != EXPECTED_DAILY_FEATURES or not ordered[["code", "ymd"]].reset_index(drop=True).equals(joined[["code", "ymd"]].reset_index(drop=True)):
        raise ValueError("daily feature/key contract changed")
    state, router = assign_states(joined[level_cols])
    model_cols = [f"dx__{c}" for c in daily_x] + [f"lx__{c}" for c in level_cols]
    prepared = ordered.reset_index(drop=True).copy()
    prepared["chart_state"] = state.reset_index(drop=True)
    dx = daily_x.reset_index(drop=True).copy(); dx.columns = [f"dx__{c}" for c in dx]
    lx = joined[level_cols].reset_index(drop=True).astype("float32"); lx.columns = [f"lx__{c}" for c in lx]
    prepared = pd.concat([prepared, dx, lx], axis=1, copy=False)
    for h in base.HORIZONS:
        prepared[f"fp_label_{h}"] = fp.labels(ordered, h)
    return prepared, model_cols, level_cols, {"join": join_audit, "router": router}


def _eligibility_snapshot(d: pd.DataFrame, horizon: int, distinct_train_calendar_months: int) -> dict[str, Any]:
    labels = pd.Series(d[f"fp_label_{horizon}"]).value_counts().reindex([0, 1, 2], fill_value=0).astype(int).tolist()
    state_months = int(d.ymd.astype(str).str[:6].nunique())
    required_months = min(36, int(distinct_train_calendar_months))
    checks = {
        "rows": len(d) >= ELIGIBILITY["rows"],
        "codes": d.code.nunique() >= ELIGIBILITY["codes"],
        "months": state_months >= required_months,
        "each_label": min(labels) >= ELIGIBILITY["each_label"],
    }
    return {"n": int(len(d)), "codes": int(d.code.nunique()), "months": state_months,
            "distinct_train_calendar_months": int(distinct_train_calendar_months), "required_state_months": required_months,
            "label_counts": labels, "checks": checks, "eligible": all(checks.values())}


def eligibility(prepared: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    global_oof_months = prepared.loc[prepared.ymd.between(20190101, 20211231), "ymd"].astype(str).str[:6].astype(int)
    fixed_oof_blocks = base.blocks(global_oof_months)
    for state in STATES:
        sd = prepared.loc[prepared.chart_state.eq(state)].copy()
        out[state] = {}
        for h in base.HORIZONS:
            valid = sd[[f"ret_close_{h}", f"down_exc_{h}", f"up_exc_{h}", "atr14", "c"]].notna().all(axis=1)
            d = sd.loc[valid]
            train = d.ymd.between(20190101, 20211231)
            folds = []
            for fold, (fitm, testm) in enumerate(fixed_oof_blocks):
                fit = train & d.ymd.astype(str).str[:6].astype(int).isin(fitm)
                snap = _eligibility_snapshot(d.loc[fit], h, len(fitm))
                folds.append({"fold": fold, "fit_months": list(map(int, fitm)), "test_months": list(map(int, testm)),
                              **snap, "test_prediction": "enabled" if snap["eligible"] else "UNJUDGEABLE"})
            calendar_2019_2021 = prepared.loc[prepared.ymd.between(20190101, 20211231), "ymd"].astype(str).str[:6].nunique()
            calendar_through_2022 = prepared.loc[prepared.ymd.le(20221231), "ymd"].astype(str).str[:6].nunique()
            fit_2022 = _eligibility_snapshot(d.loc[train], h, int(calendar_2019_2021))
            fit_frozen = _eligibility_snapshot(d.loc[d.ymd.le(20221231)], h, int(calendar_through_2022))
            out[state][str(h)] = {
                "oof_folds": folds, "eligible_oof_folds": sum(x["eligible"] for x in folds),
                "fit_2022": fit_2022, "fit_frozen_2023_2025": fit_frozen,
                "failure_policy": ELIGIBILITY["failure"],
            }
    return out


def self_tests() -> dict[str, Any]:
    cols = [
        "support20_prior_lifecycle_state", "support20_prior_no_reset_below", "ma60_lifecycle_state", "ma60_no_reset_below",
        "ma20_lifecycle_state", "ma20_no_reset_below", "ma7_level_z", "ma7_consecutive_below",
        "support20_prior_test_count20", "support20_prior_test_age", "support20_prior_test_depth_slope20", "support20_prior_level_z",
        "box_age20", "box_history_missing", "box_breakout_reentry_order", "box_last_failed_age20", "box_clear_break_confirmed2", "box_same_day_both_failed_ambiguous",
    ]
    x = pd.DataFrame(0.0, index=range(5), columns=cols)
    x["box_history_missing"] = 1
    x.loc[0, "support20_prior_lifecycle_state"] = 3
    x.loc[1, ["support20_prior_test_count20", "support20_prior_test_depth_slope20"]] = [3, .1]
    x.loc[1, ["support20_prior_test_age", "support20_prior_level_z"]] = [2, 0]
    x.loc[2, ["box_history_missing", "box_age20", "box_breakout_reentry_order", "box_last_failed_age20"]] = [0, 1, 1, 5]
    # Priority: row 3 satisfies both state 1 and state 2, state 1 must win.
    x.loc[3, ["ma60_lifecycle_state", "support20_prior_test_count20", "support20_prior_test_age", "support20_prior_test_depth_slope20", "support20_prior_level_z"]] = [3, 3, 1, .2, 0]
    got, _ = assign_states(x)
    expected = [STATES[0], STATES[1], STATES[2], STATES[0], UNJUDGEABLE]
    assertions = [
        {"case": "router_priority_and_unmatched", "pass": got.tolist() == expected, "got": got.tolist()},
        {"case": "first_passage_contract", "pass": fp.self_tests()["status"] == "pass"},
        {"case": "eligibility_is_train_subset_only", "pass": ELIGIBILITY["scope"].startswith("train subset only")},
        {"case": "contract_correction_does_not_change_router", "pass": CONTRACT_CORRECTION["router_changed"] is False},
        {"case": "structural_month_cap_only", "pass": CONTRACT_CORRECTION["structural_month_correction"]["other_thresholds_changed"] is False},
        {"case": "family_size", "pass": len(STATES) * len(base.HORIZONS) == 12},
    ]
    if not all(a["pass"] for a in assertions):
        raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def _family_holm(state_results: dict[str, Any]) -> dict[str, Any]:
    families: dict[str, Any] = {}
    specs = {"general": ("general_primary_p",), "SELL": ("lanes", "SELL", "primary_p"), "REBOUND_RISK": ("lanes", "REBOUND_RISK", "primary_p")}
    for family, path in specs.items():
        ps = {}
        for state, compare in state_results.items():
            for h, result in compare.get("results", {}).items():
                cur: Any = result
                for key in path:
                    cur = cur.get(key, {}) if isinstance(cur, dict) else {}
                if isinstance(cur, (int, float)) and np.isfinite(cur):
                    ps[f"{state}|h{h}"] = float(cur)
        families[family] = base.holm(ps) if ps else {}
    return families


def run(daily: Path, levelseq: Path, audit: Path, complete: Path, output_root: Path, resume_root: Path | None = None) -> Path:
    tests = self_tests()
    prepared, model_cols, _, prep_audit = _prepare(daily, levelseq, audit, complete)
    elig = eligibility(prepared)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = resume_root or output_root / f"{stamp}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=True)
    state_ledger = prepared[["code", "ymd", "chart_state"] + [f"fp_label_{h}" for h in base.HORIZONS]].copy()
    ledger_path = root / "state_assignment_ledger.parquet"
    if not ledger_path.exists(): state_ledger.to_parquet(ledger_path, index=False)
    original_features, original_labels, original_axis = base.features, base.labels, base.AXIS_ID
    state_results: dict[str, Any] = {}
    try:
        def fixed_features(frame: pd.DataFrame):
            g = frame.sort_values(["code", "ymd"]).copy()
            return g, g[model_cols].astype("float32")
        def fixed_labels(frame: pd.DataFrame, horizon: int):
            return frame[f"fp_label_{horizon}"].to_numpy(dtype=np.int8)
        base.features, base.labels = fixed_features, fixed_labels
        for i, state in enumerate(STATES):
            eligible_horizons = [h for h in base.HORIZONS if elig[state][str(h)]["eligible_oof_folds"] > 0]
            if not eligible_horizons:
                state_results[state] = {"eligibility": elig[state], "decision": "UNJUDGEABLE_NO_ELIGIBLE_OOF_FOLD"}
                continue
            incomplete = {str(h): [f["fold"] for f in elig[state][str(h)]["oof_folds"] if not f["eligible"]] for h in eligible_horizons}
            if any(incomplete.values()) or len(eligible_horizons) != len(base.HORIZONS):
                # The frozen base runner cannot omit only selected folds or
                # horizons. Refuse rather than silently train ineligible data.
                raise RuntimeError({"partial_fold_eligibility_not_supported_safely": state, "ineligible_folds": incomplete})
            state_path = root / f"state_{i + 1}_input.parquet"
            if not state_path.exists(): prepared.loc[prepared.chart_state.eq(state)].to_parquet(state_path, index=False)
            base.AXIS_ID = f"{RUNNER_AXIS_PREFIX}_s{i + 1}"
            prior = sorted((root / f"state_{i + 1}_model").glob("*/compare.json"))
            out = prior[-1].parent if prior else base.run(state_path, root / f"state_{i + 1}_model")
            state_results[state] = json.loads((out / "compare.json").read_text(encoding="utf-8"))
    finally:
        base.features, base.labels, base.AXIS_ID = original_features, original_labels, original_axis
    family = _family_holm({k: v for k, v in state_results.items() if "results" in v})
    payload = {
        "schema_version": AXIS_ID + ".compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "source": {"daily": {"path": str(daily), "sha256": sha(daily)}, "levelseq": {"path": str(levelseq), "sha256": sha(levelseq)}},
        "fixed_contract": {"router": prep_audit["router"], "eligibility": ELIGIBILITY, "contract_correction": CONTRACT_CORRECTION, "label": fp.LABEL_CONTRACT,
                           "features": {"daily20": EXPECTED_DAILY_FEATURES, "levelseq": EXPECTED_LEVEL_FEATURES, "total": len(model_cols)},
                           "variants": base.VARIANTS, "splits_calibration_lanes_bootstrap": "unchanged base runner", "family_holm": "3 states x 4 horizons; separately general/SELL/REBOUND_RISK", "n_jobs": 2},
        "eligibility": elig, "state_counts_all_dates": state_ledger.chart_state.value_counts().to_dict(), "state_assignment_ledger": {"path": str(ledger_path), "sha256": sha(ledger_path)},
        "state_results": state_results, "family_holm": family, "self_tests": tests,
        "decision": {"candidate_local_decision": "review_results_only", "unkept_or_ineligible": UNJUDGEABLE, "authoritative_rollup_decision": "review_only"},
        "diagnostic_policy": "2023-2025 diagnostics cannot change router or eligibility",
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    compare = root / "compare.json"; dump(compare, payload)
    dump(root / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(compare), "compare_sha256": sha(compare), "state_assignment_ledger_sha256": sha(ledger_path)})
    return root


def validate(daily: Path, levelseq: Path, audit: Path, complete: Path, output_root: Path) -> Path:
    prepared, model_cols, _, prep_audit = _prepare(daily, levelseq, audit, complete)
    elig = eligibility(prepared); tests = self_tests()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = output_root / f"{stamp}-{AXIS_ID}-validation"; out.mkdir(parents=True)
    ledger = prepared[["code", "ymd", "chart_state"] + [f"fp_label_{h}" for h in base.HORIZONS]]
    ledger_path = out / "state_assignment_ledger.parquet"; ledger.to_parquet(ledger_path, index=False)
    formal_command = (
        f'python scripts/{Path(__file__).name} --daily "{daily}" --levelseq "{levelseq}" '
        f'--levelseq-audit "{audit}" --levelseq-complete "{complete}" --output-root "{output_root}"'
    )
    payload = {"schema_version": AXIS_ID + ".validation.v1", "status": "pass", "training_started": False,
               "router": prep_audit["router"], "eligibility_contract": ELIGIBILITY, "contract_correction": CONTRACT_CORRECTION,
               "eligibility": elig, "model_feature_count": len(model_cols),
               "formal_run_prepared_not_started": formal_command,
               "state_counts_all_dates": ledger.chart_state.value_counts().to_dict(), "self_tests": tests,
               "ledger": {"path": str(ledger_path), "sha256": sha(ledger_path)},
               "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False}}
    audit_path = out / "validation.json"; dump(audit_path, payload)
    dump(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "training_started": False, "validation_sha256": sha(audit_path), "ledger_sha256": sha(ledger_path)})
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--daily", type=Path); p.add_argument("--levelseq", type=Path); p.add_argument("--levelseq-audit", type=Path); p.add_argument("--levelseq-complete", type=Path)
    p.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\stateexp_v1")); p.add_argument("--resume-root", type=Path)
    p.add_argument("--self-test", action="store_true"); p.add_argument("--validate-only", action="store_true")
    a = p.parse_args()
    if a.self_test: print(json.dumps(self_tests(), ensure_ascii=False, indent=2)); return
    if any(x is None for x in (a.daily, a.levelseq, a.levelseq_audit, a.levelseq_complete)):
        p.error("--daily, --levelseq, --levelseq-audit and --levelseq-complete are required")
    out = validate(a.daily, a.levelseq, a.levelseq_audit, a.levelseq_complete, a.output_root) if a.validate_only else run(a.daily, a.levelseq, a.levelseq_audit, a.levelseq_complete, a.output_root, a.resume_root)
    print(out)


if __name__ == "__main__":
    main()
