from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import lightgbm as lgb
import joblib
import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar
from sklearn.metrics import log_loss

import tradex_nikkei225_20bar_morphology_sequence_v1 as formal
import tradex_nikkei225_first_passage_order_v1 as fp

AXIS_ID = "fp2_v1"
SEED = formal.SEED


def binary_model(variant: str, n_estimators: int = 300) -> lgb.LGBMClassifier:
    return lgb.LGBMClassifier(
        objective="binary", n_estimators=int(n_estimators), learning_rate=.03,
        verbosity=-1, n_jobs=2, random_state=SEED, **formal.VARIANTS[variant]
    )


def _binary_scores(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    p = np.clip(np.asarray(p, float), 1e-9, 1 - 1e-9)
    prevalence = float(np.mean(y))
    constant = np.full(len(y), prevalence)
    brier = float(np.mean((p - y) ** 2))
    constant_brier = float(np.mean((constant - y) ** 2))
    return {
        "n": int(len(y)), "positive_count": int(np.sum(y)),
        "prevalence": prevalence,
        "logloss": float(log_loss(y, p, labels=[0, 1])),
        "constant_logloss": float(log_loss(y, constant, labels=[0, 1])),
        "brier": brier, "constant_brier": constant_brier,
        "brier_diff": brier - constant_brier,
    }


def _fit_binary_temperature(y: np.ndarray, p: np.ndarray) -> float:
    logits = np.log(np.clip(p, 1e-9, 1 - 1e-9) / np.clip(1 - p, 1e-9, 1))
    def objective(t: float) -> float:
        q = 1 / (1 + np.exp(-logits / t))
        return log_loss(y, q, labels=[0, 1])
    return float(minimize_scalar(objective, bounds=(.25, 4), method="bounded").x)


def _apply_binary_temperature(p: np.ndarray, temperature: float) -> np.ndarray:
    logits = np.log(np.clip(p, 1e-9, 1 - 1e-9) / np.clip(1 - p, 1e-9, 1))
    return 1 / (1 + np.exp(-logits / temperature))


def compose_probabilities(p_hit: np.ndarray, p_rebound_given_hit: np.ndarray) -> np.ndarray:
    p_hit = np.clip(np.asarray(p_hit, float), 0, 1)
    p_rebound_given_hit = np.clip(np.asarray(p_rebound_given_hit, float), 0, 1)
    out = np.column_stack((p_hit * (1 - p_rebound_given_hit),
                           p_hit * p_rebound_given_hit, 1 - p_hit))
    return out / out.sum(axis=1, keepdims=True)


def self_tests() -> dict[str, Any]:
    base = fp.self_tests()
    got = compose_probabilities(np.array([0., .25, 1.]), np.array([.8, .8, .8]))
    expected = np.array([[0., 0., 1.], [.05, .20, .75], [.20, .80, 0.]])
    checks = [
        {"name": "first_passage_contract", "pass": base["status"] == "pass"},
        {"name": "composition", "pass": bool(np.allclose(got, expected))},
        {"name": "simplex", "pass": bool(np.allclose(got.sum(1), 1))},
        {"name": "stage_b_conditioning", "pass": bool(np.allclose(got[:, 1] / np.where(got[:, :2].sum(1) > 0, got[:, :2].sum(1), 1), [0, .8, .8]))},
    ]
    if not all(x["pass"] for x in checks):
        raise AssertionError(checks)
    return {"status": "pass", "assertions": checks}


def _checkpoint_paths(root: Path, horizon: int, variant: str, fold: int, stage: str,
                      contract: dict[str, Any]) -> tuple[Path, Path, str]:
    key = formal.canon_sha(contract)
    npz = root / f"h{horizon}{variant}{stage}f{fold}_{key[:8]}.npz"
    return npz, npz.with_suffix(".json"), key


def _fit_or_resume(x: pd.DataFrame, y: np.ndarray, fit: np.ndarray, test: np.ndarray,
                   variant: str, npz: Path, meta_path: Path, contract: dict[str, Any]) -> tuple[np.ndarray, int, bool]:
    key = formal.canon_sha(contract)
    reused = False
    if npz.exists() and meta_path.exists():
        meta = json.loads(meta_path.read_text(encoding="utf-8")); saved = np.load(npz)
        pred = saved["prediction"]; iteration = int(saved["best_iteration"][0])
        reused = meta.get("contract_sha256") == key and pred.shape == (int(test.sum()),)
    if not reused:
        model = binary_model(variant)
        model.fit(x.loc[fit], y[fit], eval_set=[(x.loc[test], y[test])],
                  callbacks=[lgb.early_stopping(30, verbose=False)])
        iteration = int(model.best_iteration_)
        pred = model.predict_proba(x.loc[test], num_iteration=iteration)[:, 1]
        formal.atomic_npz(npz, prediction=pred, best_iteration=np.asarray([iteration], np.int32))
        formal.atomic_json(meta_path, {**contract, "contract_sha256": key,
                                      "prediction_rows": int(test.sum()), "complete": True})
    return pred, iteration, reused


def _stage_bootstrap(f: pd.DataFrame, y: np.ndarray, p: np.ndarray, seed: int) -> dict[str, Any]:
    loss = (p - y) ** 2 - (np.mean(y) - y) ** 2
    month = f.ymd.astype(str).str[:6].to_numpy()
    out = {}
    for offset, (cluster, groups) in enumerate((("code", f.code.to_numpy()), ("month", month))):
        out[cluster] = formal.cluster_boot(groups, {"x": loss, "n": np.ones(len(loss))},
                                           lambda d: d["x"] / d["n"], seed + offset * 1000)
    return out


def _evaluate_lanes(f: pd.DataFrame, y: np.ndarray, p: np.ndarray, horizon: int) -> dict[str, Any]:
    q4 = f.ymd.between(20221001, 20221231); ev = f.ymd.between(20230101, 20251231)
    lanes = {}
    for lane in ("SELL", "REBOUND_RISK"):
        threshold, q4_metric = formal.choose_threshold(f.loc[q4].reset_index(drop=True), y[q4], p[q4], lane, horizon)
        if threshold is None:
            lanes[lane] = {"decision": "drop_no_2022_threshold"}; continue
        ef = f.loc[ev].assign(month=f.loc[ev].ymd.astype(str).str[:6]).reset_index(drop=True)
        ey, ep = y[ev], p[ev]; metric = formal.metric(ef, ey, ep, lane, threshold, horizon)
        main, opposite = ((0, 1) if lane == "SELL" else (1, 0)); gate = formal.FINAL[lane][horizon]
        yearly = {}; absolute_years = 0; direction = True
        for year in (2023, 2024, 2025):
            z = f.ymd.between(year * 10000 + 101, year * 10000 + 1231)
            m = formal.metric(f.loc[z].reset_index(drop=True), y[z], p[z], lane, threshold, horizon)
            bm, bo = float((y[z] == main).mean()), float((y[z] == opposite).mean())
            m["main_uplift"] = None if not m["n"] else m["precision"] - bm
            m["opposite_delta"] = None if not m["n"] else m["opposite"] - bo
            direction &= bool(m["n"] and m["main_uplift"] > 0 and m["opposite_delta"] <= .02
                              and (lane != "SELL" or m["mean_return"] < 0))
            absolute_years += int(bool(m["n"] and m["precision"] >= gate[1] and m["opposite"] <= gate[2]
                                       and (lane != "SELL" or m["mean_return"] <= gate[3])))
            yearly[str(year)] = m
        base_main, base_opp = float((ey == main).mean()), float((ey == opposite).mean())
        absolute = (metric["n"] >= gate[0] and metric["codes"] >= 100 and metric["months"] >= 24
                    and .01 <= metric["coverage"] <= .15 and metric["max_code"] <= .05 and metric["max_month"] <= .15
                    and metric["precision"] >= gate[1] and metric["opposite"] <= gate[2]
                    and (lane != "SELL" or metric["mean_return"] <= gate[3])
                    and metric["precision"] >= base_main + .05 and metric["opposite"] <= base_opp - .03
                    and absolute_years >= 2)
        selected = metric["mask"].astype(float)
        values = {"n": np.ones(len(ey)), "sel": selected, "main": (ey == main).astype(float),
                  "opp": (ey == opposite).astype(float), "sm": selected * (ey == main),
                  "so": selected * (ey == opposite), "ret": ef[f"ret_close_{horizon}"].to_numpy(),
                  "sr": selected * ef[f"ret_close_{horizon}"].to_numpy()}
        def statistic(d: dict[str, float], key: str) -> float:
            return (d["sm"] / d["sel"] - d["main"] / d["n"] if key == "main" else
                    d["so"] / d["sel"] - d["opp"] / d["n"] if key == "opp" else
                    d["sr"] / d["sel"] - d["ret"] / d["n"])
        boots = {}
        for ci, (cluster, groups) in enumerate((("code", ef.code.to_numpy()), ("month", ef.month.to_numpy()))):
            keys = ("main", "opp", "ret") if lane == "SELL" else ("main", "opp")
            boots[cluster] = {key: formal.cluster_boot(groups, values, lambda d, k=key: statistic(d, k),
                                                       SEED + horizon + ci * 100 + list(keys).index(key) * 10)
                              for key in keys}
        boot_ok = all(b["main"]["ci"][0] > 0 and b["opp"]["ci"][1] < 0
                      and (lane != "SELL" or b["ret"]["ci"][1] < 0) for b in boots.values())
        primary = max([b["main"]["p_le0"] for b in boots.values()] +
                      [b["opp"]["p_ge0"] for b in boots.values()] +
                      ([b["ret"]["p_ge0"] for b in boots.values()] if lane == "SELL" else []))
        lanes[lane] = {"threshold": {"p_main": threshold[0], "p_opposite": threshold[1]},
                       "q4": {k: v for k, v in q4_metric.items() if k != "mask"},
                       "frozen_2023_2025": {k: v for k, v in metric.items() if k != "mask"},
                       "yearly": {yr: {k: v for k, v in m.items() if k != "mask"} for yr, m in yearly.items()},
                       "bootstrap": boots, "primary_p": primary,
                       "baseline": {"main": base_main, "opposite": base_opp},
                       "gate": {"absolute": absolute, "direction": direction, "bootstrap": boot_ok},
                       "decision": "provisional_keep" if absolute and direction and boot_ok else "drop"}
    return lanes


def run(input_path: Path, output_root: Path) -> Path:
    tests = self_tests(); source_sha = formal.sha(input_path)
    raw = pd.read_parquet(input_path); frame, x = formal.features(raw)
    names = list(x); train_all = frame.ymd.between(20190101, 20211231)
    med = x.loc[train_all].median().fillna(0); x = x.fillna(med).astype("float32")
    ckroot = output_root / "_ck" / source_sha[:8]; ckroot.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}; checkpoint_audit = []; probability_rows = []; saved_models = {}
    for horizon in formal.HORIZONS:
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}", "atr14", "c"]
        valid = frame[required].notna().all(axis=1)
        f = frame.loc[valid].reset_index(drop=True); xv = x.loc[valid].reset_index(drop=True)
        y3 = fp.labels(f, horizon); y_hit = (y3 != 2).astype(np.int8)
        hit = y_hit == 1; y_side = (y3 == 1).astype(np.int8)
        train = f.ymd.between(20190101, 20211231)
        months = f.loc[train, "ymd"].astype(str).str[:6].astype(int)
        variants: dict[str, Any] = {}
        for variant in formal.VARIANTS:
            pa = np.full(len(f), np.nan); pb = np.full(len(f), np.nan); iterations_a = []; iterations_b = []
            for fold, (fit_months, test_months) in enumerate(formal.blocks(months)):
                month_series = f.ymd.astype(str).str[:6].astype(int)
                fit = train & month_series.isin(fit_months); test = train & month_series.isin(test_months)
                if not test.any(): continue
                common = {"axis": AXIS_ID, "source_sha256": source_sha, "horizon": horizon,
                          "variant": variant, "fold": fold, "fit_months": list(map(int, fit_months)),
                          "test_months": list(map(int, test_months)), "n_jobs": 2,
                          "feature_sha256": formal.canon_sha(names), "median_sha256": formal.canon_sha(med.to_dict())}
                for stage, target, fit_mask, test_mask, sink, iterations in (
                    ("A", y_hit, fit, test, pa, iterations_a),
                    ("B", y_side, fit & hit, test & hit, pb, iterations_b),
                ):
                    if not test_mask.any() or np.unique(target[fit_mask]).size < 2: continue
                    contract = {**common, "stage": stage, "fit_rows": int(fit_mask.sum()), "test_rows": int(test_mask.sum()),
                                "label_sha256": hashlib.sha256(target[test_mask].tobytes()).hexdigest()}
                    npz, meta, key = _checkpoint_paths(ckroot, horizon, variant, fold, stage, contract)
                    pred, iteration, reused = _fit_or_resume(xv[names], target, fit_mask, test_mask, variant, npz, meta, contract)
                    sink[np.flatnonzero(test_mask)] = pred; iterations.append(iteration)
                    checkpoint_audit.append({"horizon": horizon, "variant": variant, "fold": fold,
                                             "stage": stage, "contract_sha256": key, "path": str(meta), "reused": reused})
            za = np.isfinite(pa); zb = hit & np.isfinite(pb); joint = za & ((~hit) | np.isfinite(pb))
            # Stage-B is undefined for neutral rows; its value cannot affect the neutral probability.
            pb_full = np.where(np.isfinite(pb), pb, .5)
            combined = compose_probabilities(pa[joint], pb_full[joint])
            variants[variant] = {
                "stage_a_oof": _binary_scores(y_hit[za], pa[za]),
                "stage_b_hit_only_oof": _binary_scores(y_side[zb], pb[zb]),
                "composed_oof": formal.scores(y3[joint], combined),
                "oof_rows": int(joint.sum()),
                "median_iteration_a": int(np.median(iterations_a)) if iterations_a else None,
                "median_iteration_b": int(np.median(iterations_b)) if iterations_b else None,
            }
        eligible = [v for v, r in variants.items()
                    if r["stage_a_oof"]["brier_diff"] < 0 and r["stage_b_hit_only_oof"]["brier_diff"] < 0
                    and r["composed_oof"]["brier"] < r["composed_oof"]["constant_brier"]
                    and max(r["composed_oof"]["ece_by_class"]) <= .08]
        selected = min(eligible, key=lambda v: variants[v]["composed_oof"]["logloss"]) if eligible else None
        if selected is None:
            results[str(horizon)] = {"variants": variants, "selected_variant": None,
                                     "decision": {"general": "drop_no_oof_variant", "SELL": "drop", "REBOUND_RISK": "drop"}}
            continue
        va = variants[selected]; na, nb = va["median_iteration_a"], va["median_iteration_b"]
        model_a, model_b = binary_model(selected, na), binary_model(selected, nb)
        model_a.fit(xv.loc[train, names], y_hit[train]); model_b.fit(xv.loc[train & hit, names], y_side[train & hit])
        raw_a = model_a.predict_proba(xv[names])[:, 1]; raw_b = model_b.predict_proba(xv[names])[:, 1]
        cal1 = f.ymd.between(20220101, 20220630); selection = f.ymd.between(20220701, 20220930)
        reference = f.ymd.between(20220101, 20220930)
        ta1 = _fit_binary_temperature(y_hit[cal1], raw_a[cal1])
        use_a = "temperature" if log_loss(y_hit[selection], _apply_binary_temperature(raw_a[selection], ta1)) < log_loss(y_hit[selection], raw_a[selection]) else "identity"
        ta = _fit_binary_temperature(y_hit[reference], raw_a[reference]) if use_a == "temperature" else 1.
        cb1, sb1 = cal1 & hit, selection & hit
        tb1 = _fit_binary_temperature(y_side[cb1], raw_b[cb1])
        use_b = "temperature" if log_loss(y_side[sb1], _apply_binary_temperature(raw_b[sb1], tb1)) < log_loss(y_side[sb1], raw_b[sb1]) else "identity"
        rb = reference & hit; tb = _fit_binary_temperature(y_side[rb], raw_b[rb]) if use_b == "temperature" else 1.
        pa, pb = _apply_binary_temperature(raw_a, ta), _apply_binary_temperature(raw_b, tb)
        composed = compose_probabilities(pa, pb); ev = f.ymd.between(20230101, 20251231)
        frozen = formal.scores(y3[ev], composed[ev]); one = np.eye(3)[y3[ev]]
        prevalence = np.bincount(y3[ev], minlength=3) / ev.sum(); constant = np.tile(prevalence, (ev.sum(), 1))
        brier_delta = np.sum((composed[ev] - one) ** 2, 1) - np.sum((constant - one) ** 2, 1)
        loss_delta = -np.log(np.clip(composed[ev][np.arange(ev.sum()), y3[ev]], 1e-12, 1)) + np.log(np.clip(prevalence[y3[ev]], 1e-12, 1))
        ef = f.loc[ev].assign(month=f.loc[ev].ymd.astype(str).str[:6]).reset_index(drop=True); general_boot = {}
        for cluster, groups in (("code", ef.code.to_numpy()), ("month", ef.month.to_numpy())):
            general_boot[cluster] = {
                "brier": formal.cluster_boot(groups, {"x": brier_delta, "n": np.ones(len(brier_delta))}, lambda d: d["x"] / d["n"], SEED + horizon),
                "logloss": formal.cluster_boot(groups, {"x": loss_delta, "n": np.ones(len(loss_delta))}, lambda d: d["x"] / d["n"], SEED + horizon + 10),
            }
        yearly = []
        for year in (2023, 2024, 2025):
            z = f.ymd.between(year * 10000 + 101, year * 10000 + 1231); score = formal.scores(y3[z], composed[z])
            yearly.append({"year": year, **score, "brier_diff": score["brier"] - score["constant_brier"],
                           "logloss_diff": score["logloss"] - score["constant_logloss"]})
        calibration_ok = (max(frozen["ece_by_class"]) <= .05 and max(frozen["max_gap_by_class"]) <= .10
                          and all(v is not None and .8 <= v <= 1.2 for v in frozen["slope_by_class"])
                          and all(v is not None and abs(v) <= .10 for v in frozen["intercept_by_class"])
                          and min(frozen["argmax_share"]) >= .05 and max(abs(v) for v in frozen["mean_probability_gap"]) <= .05)
        yearly_ok = (all(v["brier_diff"] < 0 and v["brier_diff"] < .005 for v in yearly)
                     and sum(v["logloss_diff"] < 0 and max(v["ece_by_class"]) <= .08 for v in yearly) >= 2)
        boot_ok = all(value[key]["ci"][1] < 0 for value in general_boot.values() for key in ("brier", "logloss"))
        primary = max(value[key]["p_ge0"] for value in general_boot.values() for key in ("brier", "logloss"))
        general = frozen["brier"] < frozen["constant_brier"] and frozen["logloss"] < frozen["constant_logloss"] and calibration_ok and yearly_ok and boot_ok
        stage_a_frozen = _binary_scores(y_hit[ev], pa[ev]); stage_b_ev = ev & hit
        stage_b_frozen = _binary_scores(y_side[stage_b_ev], pb[stage_b_ev])
        stage_boot = {"A": _stage_bootstrap(f.loc[ev].reset_index(drop=True), y_hit[ev], pa[ev], SEED + horizon + 2000),
                      "B": _stage_bootstrap(f.loc[stage_b_ev].reset_index(drop=True), y_side[stage_b_ev], pb[stage_b_ev], SEED + horizon + 3000)}
        stage_primary = {stage: max(v["p_ge0"] for v in boots.values()) for stage, boots in stage_boot.items()}
        lanes = _evaluate_lanes(f, y3, composed, horizon)
        results[str(horizon)] = {"variants": variants, "selected_variant": selected,
            "iteration": {"A": na, "B": nb}, "calibration": {"A": {"method": use_a, "temperature": ta}, "B": {"method": use_b, "temperature": tb}},
            "frozen_general": frozen, "general_yearly": yearly, "general_bootstrap": general_boot, "general_primary_p": primary,
            "stage_frozen": {"A": stage_a_frozen, "B_hit_only": stage_b_frozen}, "stage_bootstrap": stage_boot,
            "stage_primary_p": stage_primary, "lanes": lanes,
            "decision": {"general": "provisional_keep" if general else "drop", "SELL": lanes["SELL"]["decision"] if general else "diagnostic_hold_general_failed", "REBOUND_RISK": lanes["REBOUND_RISK"]["decision"] if general else "diagnostic_hold_general_failed"}}
        saved_models[horizon] = {"A": model_a, "B": model_b}
        exploratory = f.ymd.between(20260101, 20261231)
        for j in np.flatnonzero(exploratory):
            probability_rows.append({"code": f.code.iloc[j], "ymd": int(f.ymd.iloc[j]), "horizon": horizon,
                                     "p_down": composed[j, 0], "p_rebound": composed[j, 1], "p_neutral": composed[j, 2],
                                     "p_hit": pa[j], "p_rebound_given_hit": pb[j], "selected_variant": selected})
    general_holm = formal.holm({int(h): r["general_primary_p"] for h, r in results.items() if "general_primary_p" in r})
    for h, audit in general_holm.items():
        results[str(h)]["general_holm"] = audit
        results[str(h)]["decision"]["general"] = "keep" if audit["pass"] and results[str(h)]["decision"]["general"] == "provisional_keep" else "drop"
    for stage in ("A", "B"):
        audits = formal.holm({int(h): r["stage_primary_p"][stage] for h, r in results.items() if "stage_primary_p" in r})
        for h, audit in audits.items(): results[str(h)].setdefault("stage_holm", {})[stage] = audit
    for lane in ("SELL", "REBOUND_RISK"):
        audits = formal.holm({int(h): r["lanes"][lane]["primary_p"] for h, r in results.items() if "primary_p" in r.get("lanes", {}).get(lane, {})})
        hs = sorted(audits)
        for i, h in enumerate(hs):
            r = results[str(h)]; r["lanes"][lane]["holm"] = audits[h]
            keep = audits[h]["pass"] and r["decision"]["general"] == "keep" and r["lanes"][lane]["decision"] == "provisional_keep"
            r["lanes"][lane]["decision"] = "keep" if keep else ("diagnostic_hold_general_failed" if r["decision"]["general"] != "keep" else "drop")
            bad = False
            for neighbor in [x for x in hs if abs(hs.index(x) - i) == 1]:
                q = results[str(neighbor)]["lanes"][lane]; m, b = q["frozen_2023_2025"], q["baseline"]
                bad |= m["precision"] - b["main"] < 0 or m["opposite"] - b["opposite"] > .05
            r["lanes"][lane]["adjacent_horizon_veto"] = bad
            if bad: r["lanes"][lane]["decision"] = "drop"
            r["decision"][lane] = r["lanes"][lane]["decision"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = output_root / f"{stamp}-{AXIS_ID}"; out.mkdir(parents=True, exist_ok=False)
    formal.dump(out / "self_tests.json", tests)
    pd.DataFrame(probability_rows).to_parquet(out / "probability_ledger_2026.parquet", index=False)
    for h, pair in saved_models.items():
        joblib.dump(pair["A"], out / f"model_h{h}_stageA.joblib"); joblib.dump(pair["B"], out / f"model_h{h}_stageB.joblib")
    payload = {
        "schema_version": AXIS_ID + ".compare.v1", "artifact_role": "authoritative",
        "research_phase": "comparison_stabilization",
        "source": {"path": str(input_path), "sha256": source_sha},
        "fixed_contract": {"label": fp.LABEL_CONTRACT, "stage_a": "hit_vs_neutral",
                           "stage_b": "down_vs_rebound_on_actual_hit_only", "composition": {
                               "p_down": "p_hit*(1-p_rebound_given_hit)",
                               "p_rebound": "p_hit*p_rebound_given_hit", "p_neutral": "1-p_hit"},
                           "same_variant_both_stages": True, "n_jobs": 2,
                           "splits": "same expanding 2019-2021 OOF blocks as morphology v1"},
        "results": results, "checkpoint_audit": checkpoint_audit,
        "probability_ledger": str(out / "probability_ledger_2026.parquet"),
        "audit_scope": {"stage_specific_oof": True, "stage_specific_bootstrap": True,
                        "stage_specific_holm": True, "frozen_general_lane_gate": True},
        "decision": {"candidate_local_decision": "review_results_only",
                     "authoritative_rollup_decision": "review_only"},
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False,
                     "production_ranking_changed": False},
    }
    formal.dump(out / "compare.json", payload)
    formal.dump(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(out / "compare.json"),
                                                   "sha256": formal.sha(out / "compare.json")})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\fp2_v1"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        print(json.dumps(self_tests(), ensure_ascii=False, indent=2))
    else:
        print(run(args.input, args.output_root))


if __name__ == "__main__":
    main()
