from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import HORIZONS, _labels


AXIS_ID = "tradex_nikkei225_mature_ma60_streak_context_v1"
PERIODS = {
    "development_2019_2024": (20190101, 20241231),
    "locked_validation_2025": (20250101, 20251231),
    "exploratory_2026": (20260101, 20260713),
}
BASE_COVARIATES = [
    "pre_ret10", "ret5", "pos20", "range20_pct", "atr_pct", "dist_ma20_atr", "dist_ma60_atr",
    "ma20_slope5_atr", "ma60_slope5_atr", "volume_ratio20", "market_breadth_ma20", "market_breadth_ma60",
]


def _add_streak(frame: pd.DataFrame) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for _, part in frame.sort_values(["code", "ymd"]).groupby("code", sort=False):
        part = part.copy()
        above = (part["c"] > part["ma60"]).to_numpy()
        streak = np.zeros(len(part), dtype=int)
        value = 0
        for index, flag in enumerate(above):
            value = value + 1 if flag else 0
            streak[index] = value
        part["above_ma60_streak_pit"] = pd.Series(streak, index=part.index).shift(1).fillna(0).astype(int)
        part["atr_pct"] = part["atr14"] / part["c"]
        for column in BASE_COVARIATES:
            if column == "atr_pct":
                continue
            part[column] = part[column].shift(1)
        part["atr_pct"] = part["atr_pct"].shift(1)
        part["ma60_streak_bin"] = pd.cut(
            part["above_ma60_streak_pit"], bins=[-1, 19, 59, np.inf], labels=["0_19", "20_59", "60_plus"]
        ).astype(str)
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def _metrics(frame: pd.DataFrame, labels: np.ndarray, horizon: int) -> dict[str, Any]:
    if len(frame) == 0:
        return {"n": 0, "codes": 0, "months": 0, "downside_rate": None, "rebound_rate": None, "mean_return": None}
    return {
        "n": int(len(frame)), "codes": int(frame["code"].nunique()),
        "months": int(frame["ymd"].astype(str).str[:6].nunique()),
        "downside_rate": float((labels == 0).mean()), "rebound_rate": float((labels == 1).mean()),
        "neutral_rate": float((labels == 2).mean()),
        "mean_return": float(frame[f"ret_close_{horizon}"].mean()),
    }


def _bootstrap(
    frame: pd.DataFrame, labels: np.ndarray, mature: np.ndarray, horizon: int,
    cluster: str, iterations: int, seed: int,
) -> dict[str, Any]:
    work = frame[["code", "ymd"]].copy()
    work["mature"] = mature.astype(int)
    work["under"] = (~mature).astype(int)
    work["down_mature"] = (labels == 0).astype(int) * work["mature"]
    work["down_under"] = (labels == 0).astype(int) * work["under"]
    work["rebound_mature"] = (labels == 1).astype(int) * work["mature"]
    work["rebound_under"] = (labels == 1).astype(int) * work["under"]
    returns = frame[f"ret_close_{horizon}"].to_numpy()
    work["return_mature"] = returns * work["mature"]
    work["return_under"] = returns * work["under"]
    work["cluster"] = work["code"].astype(str) if cluster == "code" else work["ymd"].astype(str).str[:6]
    columns = [
        "mature", "under", "down_mature", "down_under", "rebound_mature", "rebound_under",
        "return_mature", "return_under",
    ]
    values = work.groupby("cluster", sort=False)[columns].sum().to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    down_delta: list[float] = []
    rebound_reduction: list[float] = []
    return_delta: list[float] = []
    for _ in range(iterations):
        sample = values[rng.integers(0, len(values), len(values))].sum(axis=0)
        mn, un, md, ud, mr, ur, mret, uret = sample
        if mn <= 0 or un <= 0:
            continue
        down_delta.append(md / mn - ud / un)
        rebound_reduction.append(ur / un - mr / mn)
        return_delta.append(mret / mn - uret / un)
    result: dict[str, Any] = {"cluster": cluster, "iterations": len(down_delta)}
    for name, values_list in (
        ("downside_uplift", down_delta), ("rebound_reduction", rebound_reduction),
        ("mean_return_delta", return_delta),
    ):
        array = np.asarray(values_list)
        result[f"{name}_mean"] = float(array.mean())
        result[f"{name}_ci95"] = [float(np.quantile(array, .025)), float(np.quantile(array, .975))]
        if name in {"downside_uplift", "rebound_reduction"}:
            result[f"{name}_p_one_sided"] = float((np.sum(array <= 0) + 1) / (len(array) + 1))
        else:
            result[f"{name}_p_one_sided"] = float((np.sum(array >= 0) + 1) / (len(array) + 1))
    return result


def _holm(p_values: dict[str, float]) -> dict[str, float]:
    ordered = sorted(p_values.items(), key=lambda item: item[1])
    adjusted: dict[str, float] = {}
    running = 0.0
    for index, (key, value) in enumerate(ordered):
        running = max(running, min(1.0, (len(ordered) - index) * value))
        adjusted[key] = running
    return adjusted


def _fit_propensity(development: pd.DataFrame, covariates: list[str]) -> tuple[StandardScaler, LogisticRegression]:
    scaler = StandardScaler().fit(development[covariates])
    model = LogisticRegression(C=1.0, max_iter=1000, random_state=20260714)
    model.fit(scaler.transform(development[covariates]), (development["above_ma60_streak_pit"] >= 60).astype(int))
    return scaler, model


def _apply_propensity(frame: pd.DataFrame, covariates: list[str], scaler: StandardScaler, model: LogisticRegression) -> pd.DataFrame:
    frame = frame.dropna(subset=covariates).copy()
    probability = np.clip(model.predict_proba(scaler.transform(frame[covariates]))[:, 1], 1e-6, 1 - 1e-6)
    frame["propensity"] = probability
    frame["propensity_logit"] = np.log(probability / (1 - probability))
    frame["_treated"] = frame["above_ma60_streak_pit"] >= 60
    frame["year_quarter"] = frame["ymd"].astype(str).str[:4] + "Q" + (((frame["ymd"] // 100) % 100 - 1) // 3 + 1).astype(str)
    return frame


def _match_1_to_3(frame: pd.DataFrame, caliper: float) -> tuple[pd.DataFrame, dict[str, Any]]:
    selected: list[int] = []
    treated_total = int(frame["_treated"].sum())
    treated_matched = 0
    for _, group in frame.groupby("year_quarter", sort=True):
        treated = group[group["_treated"]].sort_values(["ymd", "code"])
        available = set(group.index[~group["_treated"]].tolist())
        for treated_index, row in treated.iterrows():
            ranked = sorted(available, key=lambda index: abs(float(frame.at[index, "propensity_logit"]) - float(row["propensity_logit"])))
            picks = [index for index in ranked if abs(float(frame.at[index, "propensity_logit"]) - float(row["propensity_logit"])) <= caliper][:3]
            if len(picks) < 3:
                continue
            selected.append(treated_index)
            selected.extend(picks)
            available.difference_update(picks)
            treated_matched += 1
    matched = frame.loc[selected].copy() if selected else frame.iloc[0:0].copy()
    balance = {"treated_total": treated_total, "treated_matched": treated_matched, "treatment_match_rate": treated_matched / treated_total if treated_total else 0.0}
    return matched, balance


def _balance(matched: pd.DataFrame, covariates: list[str]) -> dict[str, Any]:
    treated = matched[matched["_treated"]]
    control = matched[~matched["_treated"]]
    smd: dict[str, float] = {}
    variance_ratio: dict[str, float | None] = {}
    for column in covariates:
        pooled = np.sqrt((treated[column].var(ddof=1) + control[column].var(ddof=1)) / 2)
        smd[column] = float((treated[column].mean() - control[column].mean()) / pooled) if pooled > 0 else 0.0
        control_variance = control[column].var(ddof=1)
        variance_ratio[column] = float(treated[column].var(ddof=1) / control_variance) if control_variance > 0 else None
    control_counts = control.groupby(["code", "ymd"]).size().to_numpy(dtype=float)
    ess = float(control_counts.sum() ** 2 / np.square(control_counts).sum()) if len(control_counts) else 0.0
    return {
        "smd": smd, "variance_ratio": variance_ratio, "max_abs_smd": max((abs(value) for value in smd.values()), default=None),
        "variance_ratio_all_0p5_to_2": all(value is not None and .5 <= value <= 2 for value in variance_ratio.values()),
        "control_ess": ess,
    }


def _matched_period_result(matched: pd.DataFrame, horizon: int) -> dict[str, Any]:
    required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}"]
    part = matched.dropna(subset=required).copy().reset_index(drop=True)
    labels = _labels(part, horizon)
    treated = part["_treated"].to_numpy(dtype=bool)
    result = {
        "mature_60_plus": _metrics(part.loc[treated], labels[treated], horizon),
        "matched_under_60": _metrics(part.loc[~treated], labels[~treated], horizon),
    }
    if treated.any() and (~treated).any():
        result["bootstrap"] = {
            "code": _bootstrap(part, labels, treated, horizon, "code", 2000, 20260714 + horizon),
            "month": _bootstrap(part, labels, treated, horizon, "month", 2000, 20260814 + horizon),
        }
    else:
        result["bootstrap"] = None
    return result


def run(feature_parquet: Path, bull_ledger: Path, support_ledger: Path, output_root: Path) -> Path:
    feature = pd.read_parquet(feature_parquet)
    feature = _add_streak(feature)
    bull = pd.read_parquet(bull_ledger)
    support = pd.read_parquet(support_ledger)
    bull = bull[bull["bull_failure_state"] == "S5_REBREAK_CONFIRMED"].copy()
    bull["event_age"] = bull["bull_failure_state_age"]
    bull["impulse_amplitude"] = (bull["bull_anchor_high"] - bull["bull_anchor_open"]) / bull["atr14"]
    bull["retrace_depth"] = (bull["bull_anchor_open"] - bull["bull_retrace_low"]) / bull["atr14"]
    bull["rebound_strength"] = (bull["bull_rebound_peak_high"] - bull["bull_retrace_low"]) / bull["atr14"]
    support = support[support["support_rebreak_state"] == "REBREAK_CONFIRMED"].copy()
    support["event_age"] = support["support_state_age"]
    support["touch_count"] = support["support_touch_count"]
    support["break_depth"] = (support["support_frozen_level"] - support["support_first_break_low"]) / support["atr14"]
    support["retest_age"] = support["support_state_age"]
    events = {
        "bull_impulse_s5": (bull[["code", "ymd", "event_age", "impulse_amplitude", "retrace_depth", "rebound_strength"]], ["event_age", "impulse_amplitude", "retrace_depth", "rebound_strength"]),
        "support_fatigue_rebreak": (support[["code", "ymd", "event_age", "touch_count", "break_depth", "retest_age"]], ["event_age", "touch_count", "break_depth", "retest_age"]),
    }
    results: dict[str, Any] = {}
    event_assignments: list[pd.DataFrame] = []
    primary_p: dict[str, float] = {}
    for event_name, (keys, family_covariates) in events.items():
        joined = feature.merge(keys.assign(event_name=event_name), on=["code", "ymd"], how="inner")
        covariates = BASE_COVARIATES + family_covariates
        joined = joined.dropna(subset=covariates).copy()
        joined["event_name"] = event_name
        event_assignments.append(joined[["code", "ymd", "event_name", "above_ma60_streak_pit", "ma60_streak_bin"]])
        development = joined[joined["ymd"].between(*PERIODS["development_2019_2024"])].copy()
        treatment_counts = (development["above_ma60_streak_pit"] >= 60).value_counts().to_dict()
        if len(treatment_counts) < 2:
            results[event_name] = {
                "boundary": "propensity_not_identifiable_single_treatment_class",
                "development_treatment_counts": {str(key): int(value) for key, value in treatment_counts.items()},
                "family_decision": "hold_boundary_not_matched",
            }
            continue
        scaler, propensity = _fit_propensity(development, covariates)
        logit_sd = float(np.std(np.log(np.clip(propensity.predict_proba(scaler.transform(development[covariates]))[:, 1], 1e-6, 1 - 1e-6) / np.clip(1 - propensity.predict_proba(scaler.transform(development[covariates]))[:, 1], 1e-6, 1))))
        caliper = .20 * logit_sd
        matched_by_period: dict[str, pd.DataFrame] = {}
        match_audit: dict[str, Any] = {}
        for period_name, (start, end) in PERIODS.items():
            period = joined[joined["ymd"].between(start, end)].copy()
            scored = _apply_propensity(period, covariates, scaler, propensity)
            matched, audit = _match_1_to_3(scored, caliper)
            balance = _balance(matched, covariates) if len(matched) else {"smd": {}, "variance_ratio": {}, "max_abs_smd": None, "variance_ratio_all_0p5_to_2": False, "control_ess": 0.0}
            audit.update(balance)
            audit["balance_pass"] = bool(
                audit["treatment_match_rate"] >= .80 and audit["max_abs_smd"] is not None and audit["max_abs_smd"] <= .10
                and audit["variance_ratio_all_0p5_to_2"]
                and (period_name != "locked_validation_2025" or audit["control_ess"] >= 40)
            )
            matched_by_period[period_name] = matched
            match_audit[period_name] = audit
        event_result: dict[str, Any] = {}
        for horizon in HORIZONS:
            horizon_result: dict[str, Any] = {}
            for period_name in PERIODS:
                period_raw = joined[joined["ymd"].between(*PERIODS[period_name])].dropna(subset=[f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}"]).copy().reset_index(drop=True)
                raw_labels = _labels(period_raw, horizon)
                bins = {
                    name: _metrics(period_raw.loc[period_raw["ma60_streak_bin"] == name], raw_labels[period_raw["ma60_streak_bin"].to_numpy() == name], horizon)
                    for name in ("0_19", "20_59", "60_plus")
                }
                comparison = _matched_period_result(matched_by_period[period_name], horizon)
                horizon_result[period_name] = {"bins": bins, "mature_vs_under60": comparison}
            event_result[str(horizon)] = horizon_result
        dev5 = event_result["5"]["development_2019_2024"]["mature_vs_under60"]
        val5 = event_result["5"]["locked_validation_2025"]["mature_vs_under60"]
        dev_m, dev_c = dev5["mature_60_plus"], dev5["matched_under_60"]
        val_m, val_c = val5["mature_60_plus"], val5["matched_under_60"]
        def differences(left: dict[str, Any], right: dict[str, Any]) -> dict[str, float | None]:
            return {
                "downside_uplift": left["downside_rate"] - right["downside_rate"] if left["downside_rate"] is not None and right["downside_rate"] is not None else None,
                "rebound_delta": left["rebound_rate"] - right["rebound_rate"] if left["rebound_rate"] is not None and right["rebound_rate"] is not None else None,
                "mean_return_delta": left["mean_return"] - right["mean_return"] if left["mean_return"] is not None and right["mean_return"] is not None else None,
            }
        dev_diff, val_diff = differences(dev_m, dev_c), differences(val_m, val_c)
        balance_pass = match_audit["development_2019_2024"]["balance_pass"] and match_audit["locked_validation_2025"]["balance_pass"]
        sample_pass = dev_m["n"] >= 150 and dev_m["codes"] >= 80 and dev_m["months"] >= 36 and val_m["n"] >= 40 and val_m["codes"] >= 30 and val_m["months"] >= 8
        point_pass = bool(dev_diff["downside_uplift"] is not None and dev_diff["downside_uplift"] >= .05 and dev_diff["rebound_delta"] <= -.03 and dev_diff["mean_return_delta"] <= -.005 and dev_m["mean_return"] <= -.01 and val_diff["downside_uplift"] >= .05 and val_diff["rebound_delta"] <= -.03 and val_diff["mean_return_delta"] <= -.005 and val_m["mean_return"] <= -.01)
        ci_pass = False
        if dev5["bootstrap"] and val5["bootstrap"]:
            ci_pass = all(item["downside_uplift_ci95"][0] > 0 and item["rebound_reduction_ci95"][0] > 0 and item["mean_return_delta_ci95"][1] < 0 for item in [*dev5["bootstrap"].values(), *val5["bootstrap"].values()])
            primary_p[event_name] = max(
                item[key]
                for item in val5["bootstrap"].values()
                for key in ("downside_uplift_p_one_sided", "rebound_reduction_p_one_sided", "mean_return_delta_p_one_sided")
            )
        guardrail_pass = True
        for guard_h in (3, 10):
            for period_name in ("development_2019_2024", "locked_validation_2025"):
                item = event_result[str(guard_h)][period_name]["mature_vs_under60"]
                diff = differences(item["mature_60_plus"], item["matched_under_60"])
                guardrail_pass &= bool(diff["downside_uplift"] is not None and diff["downside_uplift"] >= 0 and diff["rebound_delta"] < .03 and diff["mean_return_delta"] <= 0)
        dev_years = []
        for year in range(2019, 2025):
            year_match = matched_by_period["development_2019_2024"][(matched_by_period["development_2019_2024"]["ymd"] // 10000) == year]
            item = _matched_period_result(year_match, 5)
            diff = differences(item["mature_60_plus"], item["matched_under_60"])
            dev_years.append({"year": year, "differences": diff, "direction_pass": bool(diff["downside_uplift"] is not None and diff["downside_uplift"] > 0 and diff["rebound_delta"] < 0 and diff["mean_return_delta"] < 0)})
        yearly_pass = sum(item["direction_pass"] for item in dev_years) >= 4
        event_result["matching"] = {"covariates": covariates, "caliper_logit": caliper, "audit": match_audit}
        event_result["primary_h5_gate"] = {"balance": balance_pass, "sample": sample_pass, "point": point_pass, "cluster_ci": ci_pass, "guardrail_h3_h10": guardrail_pass, "development_years_4_of_6": yearly_pass, "development_years": dev_years}
        results[event_name] = event_result

    holm = _holm(primary_p)
    for event_name, result in results.items():
        if "primary_h5_gate" not in result:
            continue
        result["primary_h5_gate"]["primary_p"] = primary_p.get(event_name)
        result["primary_h5_gate"]["holm_adjusted_p"] = holm.get(event_name)
        result["primary_h5_gate"]["holm_alpha_05"] = holm.get(event_name, 1.0) <= .05
        val = result["5"]["locked_validation_2025"]["mature_vs_under60"]
        mature, control = val["mature_60_plus"], val["matched_under_60"]
        if mature["downside_rate"] is None or control["downside_rate"] is None:
            result["family_decision"] = "hold_boundary_not_matched"
        else:
            down_delta = mature["downside_rate"] - control["downside_rate"]
            rebound_delta = mature["rebound_rate"] - control["rebound_rate"]
            mean_delta = mature["mean_return"] - control["mean_return"]
            full_pass = all(result["primary_h5_gate"].get(key, False) for key in ("balance", "sample", "point", "cluster_ci", "guardrail_h3_h10", "development_years_4_of_6", "holm_alpha_05"))
            if full_pass:
                result["family_decision"] = "keep_review_only"
            elif down_delta <= 0 or rebound_delta >= .03 or mean_delta >= 0:
                result["family_decision"] = "drop"
            else:
                result["family_decision"] = "hold_boundary_not_matched"

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    assignment_path = output / "mature_ma60_streak_event_assignments.parquet"
    pd.concat(event_assignments, ignore_index=True).to_parquet(assignment_path, index=False, compression="zstd")
    spec = {
        "threshold": 60, "streak_time": "t-1", "families": sorted(events), "matching": "year-quarter exact plus propensity nearest 1:3 caliper 0.2 SD logit",
        "base_covariates": BASE_COVARIATES, "bootstrap": 2000,
    }
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment", "source_feature_parquet": str(feature_parquet),
        "source_ledgers": {"bull_impulse": str(bull_ledger), "support_fatigue": str(support_ledger)},
        "source_hashes": {
            "feature": hashlib.sha256(feature_parquet.read_bytes()).hexdigest(),
            "bull": hashlib.sha256(bull_ledger.read_bytes()).hexdigest(),
            "support": hashlib.sha256(support_ledger.read_bytes()).hexdigest(),
        },
        "assignment_ledger": str(assignment_path),
        "fixed_conditions": {
            "single_axis": "PIT consecutive closes above MA60 through t-1 at unchanged base event date",
            "streak_bins": ["0-19", "20-59", "60+"], "mature_threshold": 60,
            "threshold_origin": "pre-registered from user Misumi case and prior fixed family value; never changed using 2025",
            "labels": "existing rebound-priority three-class labels", "horizons": list(HORIZONS),
            "periods": PERIODS, "bootstrap": "code and month cluster, 2000 iterations",
            "event_definitions_changed": False,
            "matching": "family-specific development-fit propensity; exact year-quarter plus nearest 1:3, caliper .2 SD logit; validation transform only",
        },
        "non_overlap_with_closed_family": {
            "closed_family": "ma60_above_60plus_short_veto_family_closure_v1",
            "old_closure_artifact": "G:\\Tradex\\ma60_above_60plus_short_veto_family_closure_v1\\20260523T184058Z-ma60-above-60plus-short-veto-family-closure-v1\\compare.json",
            "difference": "closed family tested MA60 maturity as a standalone/general family; this axis only stratifies two already-fixed daily sequence event dates without changing membership",
            "does_not_revive_ma60_short_veto": True,
            "automatic_reopen": False, "production_adoption": False,
            "new_spec_hash": hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest(),
        },
        "results": results,
        "decision": {
            "candidate_local_decision": "keep_review_only" if any(item["family_decision"] == "keep_review_only" for item in results.values()) else "hold_boundary_not_matched" if any(item["family_decision"] == "hold_boundary_not_matched" for item in results.values()) else "drop_context_axis",
            "authoritative_rollup_decision": "review_only",
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
        "remaining_risks": ["60+ event samples may be sparse", "current Nikkei225 registry is survivorship-biased", "2026 remains exploratory"],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "compare": str(compare), "assignment_ledger": str(assignment_path)}, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feature-parquet", required=True, type=Path)
    parser.add_argument("--bull-ledger", required=True, type=Path)
    parser.add_argument("--support-ledger", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_nikkei225_mature_ma60_streak_context_v1"))
    args = parser.parse_args()
    print(run(args.feature_parquet, args.bull_ledger, args.support_ledger, args.output_root))


if __name__ == "__main__":
    main()
