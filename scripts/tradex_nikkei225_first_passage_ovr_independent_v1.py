from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

import tradex_nikkei225_20bar_morphology_sequence_v1 as formal
import tradex_nikkei225_first_passage_order_v1 as fp
import tradex_nikkei225_first_passage_two_stage_v1 as common


AXIS_ID = "fpovr_v1"
SEED = formal.SEED
HEADS = ((0, "down"), (1, "rebound"), (2, "neutral"))


def normalize_ovr(q: np.ndarray) -> np.ndarray:
    q = np.clip(np.asarray(q, dtype=float), 1e-9, 1 - 1e-9)
    denominator = q.sum(axis=1, keepdims=True)
    return q / denominator


def self_tests() -> dict[str, Any]:
    label_test = fp.self_tests()
    raw = np.asarray([[.2, .3, .5], [.9, .9, .2], [0., 0., 0.]])
    got = normalize_ovr(raw)
    checks = [
        {"name": "first_passage_contract", "pass": label_test["status"] == "pass"},
        {"name": "simplex", "pass": bool(np.allclose(got.sum(axis=1), 1.0))},
        {"name": "proportional_normalization", "pass": bool(np.allclose(got[1], [.45, .45, .10]))},
        {"name": "finite_at_zero", "pass": bool(np.isfinite(got[2]).all())},
        {"name": "class_order", "pass": bool(np.argmax(got[0]) == 2)},
    ]
    if not all(x["pass"] for x in checks):
        raise AssertionError(checks)
    return {"status": "pass", "assertions": checks}


def _checkpoint_paths(root: Path, horizon: int, variant: str, fold: int, head: str,
                      contract: dict[str, Any]) -> tuple[Path, Path, str]:
    key = formal.canon_sha(contract)
    npz = root / f"h{horizon}{variant}{head[0]}f{fold}_{key[:8]}.npz"
    return npz, npz.with_suffix(".json"), key


def _fit_or_resume(x: pd.DataFrame, y: np.ndarray, fit: np.ndarray, test: np.ndarray,
                   variant: str, npz: Path, meta_path: Path,
                   contract: dict[str, Any]) -> tuple[np.ndarray, int, bool]:
    key = formal.canon_sha(contract)
    reused = False
    if npz.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        with np.load(npz) as saved:
            pred = saved["prediction"]
            iteration = int(saved["best_iteration"][0])
        reused = (meta.get("contract_sha256") == key
                  and meta.get("complete") is True
                  and pred.shape == (int(test.sum()),)
                  and meta.get("npz_sha256") == formal.sha(npz))
    if not reused:
        model = common.binary_model(variant)
        model.fit(x.loc[fit], y[fit], eval_set=[(x.loc[test], y[test])],
                  callbacks=[common.lgb.early_stopping(30, verbose=False)])
        iteration = int(model.best_iteration_)
        pred = model.predict_proba(x.loc[test], num_iteration=iteration)[:, 1]
        formal.atomic_npz(npz, prediction=pred,
                          best_iteration=np.asarray([iteration], dtype=np.int32))
        formal.atomic_json(meta_path, {
            **contract, "contract_sha256": key, "prediction_rows": int(test.sum()),
            "npz_sha256": formal.sha(npz), "complete": True,
        })
    return pred, iteration, reused


def _general_bootstrap(f: pd.DataFrame, y: np.ndarray, p: np.ndarray,
                       horizon: int) -> tuple[dict[str, Any], float]:
    one = np.eye(3)[y]
    prevalence = np.bincount(y, minlength=3) / len(y)
    constant = np.tile(prevalence, (len(y), 1))
    brier_delta = np.sum((p - one) ** 2, axis=1) - np.sum((constant - one) ** 2, axis=1)
    loss_delta = (-np.log(np.clip(p[np.arange(len(y)), y], 1e-12, 1))
                  + np.log(np.clip(prevalence[y], 1e-12, 1)))
    month = f.ymd.astype(str).str[:6].to_numpy()
    boots = {}
    for ci, (cluster, groups) in enumerate((("code", f.code.to_numpy()), ("month", month))):
        boots[cluster] = {
            "brier": formal.cluster_boot(groups, {"x": brier_delta, "n": np.ones(len(y))},
                                           lambda d: d["x"] / d["n"], SEED + horizon + ci * 100),
            "logloss": formal.cluster_boot(groups, {"x": loss_delta, "n": np.ones(len(y))},
                                            lambda d: d["x"] / d["n"], SEED + horizon + ci * 100 + 10),
        }
    primary = max(v[k]["p_ge0"] for v in boots.values() for k in ("brier", "logloss"))
    return boots, float(primary)


def run(input_path: Path, output_root: Path) -> Path:
    tests = self_tests()
    source_sha = formal.sha(input_path)
    raw = pd.read_parquet(input_path)
    frame, x = formal.features(raw)
    names = list(x)
    train_all = frame.ymd.between(20190101, 20211231)
    med = x.loc[train_all].median().fillna(0)
    x = x.fillna(med).astype("float32")
    feature_sha = formal.canon_sha(names)
    median_sha = formal.canon_sha(med.to_dict())
    ckroot = output_root / "_ck" / source_sha[:8]
    ckroot.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}
    checkpoint_audit: list[dict[str, Any]] = []
    probability_rows: list[dict[str, Any]] = []
    saved_models: dict[int, dict[str, Any]] = {}

    for horizon in formal.HORIZONS:
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}",
                    f"up_exc_{horizon}", "atr14", "c"]
        valid = frame[required].notna().all(axis=1)
        f = frame.loc[valid].reset_index(drop=True)
        xv = x.loc[valid].reset_index(drop=True)
        y3 = fp.labels(f, horizon)
        train = f.ymd.between(20190101, 20211231)
        train_months = f.loc[train, "ymd"].astype(str).str[:6].astype(int)
        month_series = f.ymd.astype(str).str[:6].astype(int)
        variants: dict[str, Any] = {}

        for variant in formal.VARIANTS:
            raw_oof = np.full((len(f), 3), np.nan)
            iterations: dict[str, list[int]] = {name: [] for _, name in HEADS}
            for fold, (fit_months, test_months) in enumerate(formal.blocks(train_months)):
                fit = train & month_series.isin(fit_months)
                test = train & month_series.isin(test_months)
                if not test.any():
                    continue
                for class_id, head in HEADS:
                    target = (y3 == class_id).astype(np.int8)
                    if np.unique(target[fit]).size < 2:
                        continue
                    contract = {
                        "axis": AXIS_ID, "source_sha256": source_sha,
                        "horizon": horizon, "variant": variant, "fold": fold,
                        "head": head, "positive_class_id": class_id,
                        "fit_months": list(map(int, fit_months)),
                        "test_months": list(map(int, test_months)),
                        "fit_rows": int(fit.sum()), "test_rows": int(test.sum()),
                        "n_jobs": 2, "feature_sha256": feature_sha,
                        "median_sha256": median_sha,
                        "label_sha256": hashlib.sha256(target[test].tobytes()).hexdigest(),
                    }
                    npz, meta, key = _checkpoint_paths(ckroot, horizon, variant, fold, head, contract)
                    pred, iteration, reused = _fit_or_resume(
                        xv[names], target, fit, test, variant, npz, meta, contract)
                    raw_oof[np.flatnonzero(test), class_id] = pred
                    iterations[head].append(iteration)
                    checkpoint_audit.append({
                        "horizon": horizon, "variant": variant, "fold": fold,
                        "head": head, "contract_sha256": key, "meta_path": str(meta),
                        "meta_sha256": formal.sha(meta), "npz_path": str(npz),
                        "npz_sha256": formal.sha(npz), "reused": reused,
                    })
            joint = np.isfinite(raw_oof).all(axis=1)
            q = normalize_ovr(raw_oof[joint])
            head_scores = {
                head: common._binary_scores((y3[joint] == class_id).astype(np.int8), raw_oof[joint, class_id])
                for class_id, head in HEADS
            }
            score = formal.scores(y3[joint], q)
            variants[variant] = {
                "head_oof": head_scores, "normalized_oof": score,
                "oof_rows": int(joint.sum()),
                "median_iteration": {
                    head: int(np.median(values)) if values else None
                    for head, values in iterations.items()
                },
            }

        eligible = [
            v for v, r in variants.items()
            if all(s["brier_diff"] < 0 for s in r["head_oof"].values())
            and r["normalized_oof"]["brier"] < r["normalized_oof"]["constant_brier"]
            and r["normalized_oof"]["logloss"] < r["normalized_oof"]["constant_logloss"]
            and max(r["normalized_oof"]["ece_by_class"]) <= .08
            and min(r["normalized_oof"]["argmax_share"]) >= .01
        ]
        selected = min(eligible, key=lambda v: variants[v]["normalized_oof"]["logloss"]) if eligible else None
        if selected is None:
            results[str(horizon)] = {
                "variants": variants, "selected_variant": None,
                "eligibility": {
                    "all_three_head_brier_diff_lt_0": True,
                    "normalized_brier_and_logloss_beat_constant": True,
                    "max_ece_le": .08, "min_argmax_share_ge": .01,
                },
                "decision": {"general": "drop_no_oof_variant", "SELL": "drop", "REBOUND_RISK": "drop"},
            }
            continue

        iterations = variants[selected]["median_iteration"]
        models: dict[str, Any] = {}
        raw_probability = np.empty((len(f), 3), dtype=float)
        for class_id, head in HEADS:
            model = common.binary_model(selected, iterations[head])
            target = (y3 == class_id).astype(np.int8)
            model.fit(xv.loc[train, names], target[train])
            models[head] = model
            raw_probability[:, class_id] = model.predict_proba(xv[names])[:, 1]

        cal1 = f.ymd.between(20220101, 20220630)
        selection = f.ymd.between(20220701, 20220930)
        reference = f.ymd.between(20220101, 20220930)
        calibrated = np.empty_like(raw_probability)
        calibration: dict[str, Any] = {}
        for class_id, head in HEADS:
            target = (y3 == class_id).astype(np.int8)
            t1 = common._fit_binary_temperature(target[cal1], raw_probability[cal1, class_id])
            selected_loss = log_loss(target[selection], common._apply_binary_temperature(raw_probability[selection, class_id], t1), labels=[0, 1])
            identity_loss = log_loss(target[selection], raw_probability[selection, class_id], labels=[0, 1])
            method = "temperature" if selected_loss < identity_loss else "identity"
            temperature = common._fit_binary_temperature(target[reference], raw_probability[reference, class_id]) if method == "temperature" else 1.0
            calibrated[:, class_id] = common._apply_binary_temperature(raw_probability[:, class_id], temperature)
            calibration[head] = {"method": method, "temperature": temperature,
                                 "selection_temperature_logloss": selected_loss,
                                 "selection_identity_logloss": identity_loss}
        p = normalize_ovr(calibrated)
        ev = f.ymd.between(20230101, 20251231)
        frozen = formal.scores(y3[ev], p[ev])
        general_boot, primary = _general_bootstrap(f.loc[ev].reset_index(drop=True), y3[ev], p[ev], horizon)
        yearly = []
        for year in (2023, 2024, 2025):
            z = f.ymd.between(year * 10000 + 101, year * 10000 + 1231)
            score = formal.scores(y3[z], p[z])
            yearly.append({"year": year, **score,
                           "brier_diff": score["brier"] - score["constant_brier"],
                           "logloss_diff": score["logloss"] - score["constant_logloss"]})
        calibration_ok = (
            max(frozen["ece_by_class"]) <= .05
            and max(frozen["max_gap_by_class"]) <= .10
            and all(v is not None and .8 <= v <= 1.2 for v in frozen["slope_by_class"])
            and all(v is not None and abs(v) <= .10 for v in frozen["intercept_by_class"])
            and min(frozen["argmax_share"]) >= .05
            and max(abs(v) for v in frozen["mean_probability_gap"]) <= .05
        )
        yearly_ok = (all(v["brier_diff"] < 0 and v["brier_diff"] < .005 for v in yearly)
                     and sum(v["logloss_diff"] < 0 and max(v["ece_by_class"]) <= .08 for v in yearly) >= 2)
        boot_ok = all(v[k]["ci"][1] < 0 for v in general_boot.values() for k in ("brier", "logloss"))
        general = (frozen["brier"] < frozen["constant_brier"]
                   and frozen["logloss"] < frozen["constant_logloss"]
                   and calibration_ok and yearly_ok and boot_ok)
        lanes = common._evaluate_lanes(f, y3, p, horizon)
        head_frozen = {}
        head_bootstrap = {}
        head_primary = {}
        for class_id, head in HEADS:
            target = (y3 == class_id).astype(np.int8)
            head_frozen[head] = common._binary_scores(target[ev], calibrated[ev, class_id])
            head_bootstrap[head] = common._stage_bootstrap(
                f.loc[ev].reset_index(drop=True), target[ev], calibrated[ev, class_id],
                SEED + horizon + 2000 + class_id * 100)
            head_primary[head] = max(v["p_ge0"] for v in head_bootstrap[head].values())
        results[str(horizon)] = {
            "variants": variants, "selected_variant": selected,
            "eligibility": {"eligible_variants": eligible},
            "iteration": iterations, "calibration": calibration,
            "frozen_general": frozen, "general_yearly": yearly,
            "general_bootstrap": general_boot, "general_primary_p": primary,
            "head_frozen": head_frozen, "head_bootstrap": head_bootstrap,
            "head_primary_p": head_primary, "lanes": lanes,
            "decision": {
                "general": "provisional_keep" if general else "drop",
                "SELL": lanes["SELL"]["decision"] if general else "diagnostic_hold_general_failed",
                "REBOUND_RISK": lanes["REBOUND_RISK"]["decision"] if general else "diagnostic_hold_general_failed",
            },
        }
        saved_models[horizon] = models
        for j in np.flatnonzero(f.ymd.between(20260101, 20261231)):
            probability_rows.append({
                "code": f.code.iloc[j], "ymd": int(f.ymd.iloc[j]), "horizon": horizon,
                "p_down": p[j, 0], "p_rebound": p[j, 1], "p_neutral": p[j, 2],
                "q_down": calibrated[j, 0], "q_rebound": calibrated[j, 1],
                "q_neutral": calibrated[j, 2], "selected_variant": selected,
            })

    general_holm = formal.holm({int(h): r["general_primary_p"] for h, r in results.items() if "general_primary_p" in r})
    for h, audit in general_holm.items():
        r = results[str(h)]
        r["general_holm"] = audit
        r["decision"]["general"] = "keep" if audit["pass"] and r["decision"]["general"] == "provisional_keep" else "drop"
    for head in (name for _, name in HEADS):
        audits = formal.holm({int(h): r["head_primary_p"][head] for h, r in results.items() if "head_primary_p" in r})
        for h, audit in audits.items():
            results[str(h)].setdefault("head_holm", {})[head] = audit
    for lane in ("SELL", "REBOUND_RISK"):
        audits = formal.holm({int(h): r["lanes"][lane]["primary_p"] for h, r in results.items()
                              if "primary_p" in r.get("lanes", {}).get(lane, {})})
        hs = sorted(audits)
        for i, h in enumerate(hs):
            r = results[str(h)]
            lane_result = r["lanes"][lane]
            lane_result["holm"] = audits[h]
            keep = (audits[h]["pass"] and r["decision"]["general"] == "keep"
                    and lane_result["decision"] == "provisional_keep")
            lane_result["decision"] = "keep" if keep else (
                "diagnostic_hold_general_failed" if r["decision"]["general"] != "keep" else "drop")
            bad = False
            for neighbor in [x for x in hs if abs(hs.index(x) - i) == 1]:
                neighbor_lane = results[str(neighbor)]["lanes"][lane]
                m, b = neighbor_lane["frozen_2023_2025"], neighbor_lane["baseline"]
                bad |= m["precision"] - b["main"] < 0 or m["opposite"] - b["opposite"] > .05
            lane_result["adjacent_horizon_veto"] = bool(bad)
            if bad:
                lane_result["decision"] = "drop"
            r["decision"][lane] = lane_result["decision"]

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = output_root / f"{stamp}-{AXIS_ID}"
    out.mkdir(parents=True, exist_ok=False)
    formal.dump(out / "self_tests.json", tests)
    ledger_path = out / "probability_ledger_2026.parquet"
    pd.DataFrame(probability_rows).to_parquet(ledger_path, index=False)
    model_artifacts = {}
    for horizon, models in saved_models.items():
        model_artifacts[str(horizon)] = {}
        for head, model in models.items():
            path = out / f"model_h{horizon}_{head}.joblib"
            joblib.dump(model, path)
            model_artifacts[str(horizon)][head] = {"path": str(path), "sha256": formal.sha(path)}
    payload = {
        "schema_version": AXIS_ID + ".compare.v1", "artifact_role": "authoritative",
        "research_phase": "comparison_stabilization",
        "source": {"path": str(input_path), "sha256": source_sha},
        "fixed_contract": {
            "label": fp.LABEL_CONTRACT,
            "heads": {head: f"class_{class_id}_vs_rest" for class_id, head in HEADS},
            "same_variant_all_heads": True,
            "calibration": "head_specific_temperature selected on 2022H2 after fitting on 2022H1; refit 2022M1-M9",
            "composition": "q_k=calibrated independent sigmoid; p_k=q_k/sum(q)",
            "n_jobs": 2, "splits": "same expanding 2019-2021 OOF blocks as morphology v1",
            "threshold_selection": "2022Q4 only", "frozen_evaluation": "2023-2025",
        },
        "results": results, "checkpoint_audit": checkpoint_audit,
        "artifacts": {
            "probability_ledger_2026": {"path": str(ledger_path), "sha256": formal.sha(ledger_path)},
            "models": model_artifacts,
            "self_tests": {"path": str(out / "self_tests.json"), "sha256": formal.sha(out / "self_tests.json")},
        },
        "audit_scope": {
            "head_specific_oof": True, "head_specific_temperature": True,
            "head_specific_bootstrap": True, "head_specific_holm": True,
            "frozen_general_lane_gate": True, "checkpoint_content_hashes": True,
        },
        "decision": {"candidate_local_decision": "review_results_only",
                     "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False,
                     "runtime_db_write": False, "production_ranking_changed": False},
    }
    compare_path = out / "compare.json"
    formal.dump(compare_path, payload)
    formal.dump(out / "_ARTIFACT_COMPLETE.json", {
        "complete": True, "compare": str(compare_path),
        "compare_sha256": formal.sha(compare_path),
        "probability_ledger_sha256": formal.sha(ledger_path),
        "model_sha256": {h: {k: v["sha256"] for k, v in heads.items()}
                         for h, heads in model_artifacts.items()},
    })
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\fpovr_v1"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    print(json.dumps(self_tests(), ensure_ascii=False, indent=2) if args.self_test
          else run(args.input, args.output_root))


if __name__ == "__main__":
    main()
