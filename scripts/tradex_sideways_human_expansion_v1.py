from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


HORIZONS = (5, 10, 20)
PERMUTATIONS = 20_000
SEED = 20260719


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_bars(db_path: Path, codes: list[str]) -> pd.DataFrame:
    placeholders = ",".join("?" for _ in codes)
    query = f"""
        select cast(code as varchar) code,
               cast(strftime(to_timestamp(date), '%Y%m%d') as integer) ymd,
               cast(o as double) o, cast(h as double) h, cast(l as double) l,
               cast(c as double) c, cast(v as double) v
        from daily_bars
        where source='pan' and c>0 and h>=l and cast(code as varchar) in ({placeholders})
        order by code, ymd
    """
    with duckdb.connect(str(db_path), read_only=True) as connection:
        frame = connection.execute(query, codes).fetchdf()
    frame["code"] = frame["code"].astype(str).str.zfill(4)
    return frame.drop_duplicates(["code", "ymd"], keep="last")


def attach_outcomes(labels: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    grouped = {code: frame.sort_values("ymd").reset_index(drop=True) for code, frame in bars.groupby("code", sort=False)}
    rows = []
    for case in labels.itertuples(index=False):
        history = grouped[str(case.code).zfill(4)]
        positions = np.flatnonzero(history["ymd"].to_numpy() == int(case.ymd))
        if len(positions) != 1:
            raise ValueError(f"event date not uniquely found for {case.case_id}")
        index = int(positions[0])
        if index < 14 or index + max(HORIZONS) >= len(history):
            raise ValueError(f"incomplete ATR or future horizon for {case.case_id}")
        past = history.iloc[index - 14 : index + 1]
        previous_close = history["c"].shift(1).iloc[index - 14 : index + 1]
        true_range = pd.concat([
            past["h"] - past["l"],
            (past["h"] - previous_close).abs(),
            (past["l"] - previous_close).abs(),
        ], axis=1).max(axis=1)
        atr14 = float(true_range.tail(14).mean())
        close0 = float(history.iloc[index]["c"])
        pre_range15 = float(past["h"].max() - past["l"].min())
        row: dict[str, float | int | str | bool] = {
            "case_id": case.case_id, "atr14": atr14, "pre_range15": pre_range15,
        }
        for horizon in HORIZONS:
            future = history.iloc[index + 1 : index + horizon + 1]
            up_atr = (float(future["h"].max()) - close0) / atr14
            down_atr = (close0 - float(future["l"].min())) / atr14
            max_excursion = max(up_atr, down_atr)
            row[f"up_atr_{horizon}"] = up_atr
            row[f"down_atr_{horizon}"] = down_atr
            row[f"max_excursion_atr_{horizon}"] = max_excursion
            row[f"future_range_atr_{horizon}"] = (float(future["h"].max()) - float(future["l"].min())) / atr14
            row[f"abs_close_move_atr_{horizon}"] = abs(float(future.iloc[-1]["c"]) - close0) / atr14
            row[f"max_excursion_pre_range_{horizon}"] = max(up_atr, down_atr) * atr14 / pre_range15 if pre_range15 > 0 else np.nan
            row[f"expansion_2atr_{horizon}"] = bool(max_excursion >= 2.0)
        rows.append(row)
    return labels.merge(pd.DataFrame(rows), on="case_id", validate="one_to_one")


def fixed_effect_difference(values: np.ndarray, labels: np.ndarray, strata: np.ndarray) -> float:
    x = labels.astype(float).copy()
    y = values.astype(float).copy()
    for stratum in np.unique(strata):
        mask = strata == stratum
        x[mask] -= x[mask].mean()
        y[mask] -= y[mask].mean()
    denominator = float(np.dot(x, x))
    return float(np.dot(x, y) / denominator) if denominator > 0 else np.nan


def stratified_permutation(values: np.ndarray, labels: np.ndarray, strata: np.ndarray, *, permutations: int = PERMUTATIONS) -> tuple[float, float]:
    observed = fixed_effect_difference(values, labels, strata)
    rng = np.random.default_rng(SEED)
    indexes = [np.flatnonzero(strata == stratum) for stratum in np.unique(strata)]
    exceed = 0
    for _ in range(permutations):
        shuffled = labels.copy()
        for index in indexes:
            shuffled[index] = rng.permutation(shuffled[index])
        statistic = fixed_effect_difference(values, shuffled, strata)
        exceed += int(statistic >= observed)
    return observed, float((exceed + 1) / (permutations + 1))


def metric_comparison(frame: pd.DataFrame, column: str) -> dict:
    values = frame[column].astype(float).to_numpy()
    labels = frame["human_sideways"].astype(bool).to_numpy()
    strata = (frame["year"].astype(str) + "|" + frame["sample_group"].astype(str)).to_numpy()
    adjusted, p_value = stratified_permutation(values, labels, strata)
    sideways, control = values[labels], values[~labels]
    return {
        "sideways_n": int(len(sideways)), "control_n": int(len(control)),
        "sideways_mean": float(np.mean(sideways)), "control_mean": float(np.mean(control)),
        "raw_mean_difference": float(np.mean(sideways) - np.mean(control)),
        "sideways_median": float(np.median(sideways)), "control_median": float(np.median(control)),
        "stratum_adjusted_difference": adjusted, "one_sided_stratified_permutation_p": p_value,
    }


def run(db_path: Path, human_path: Path, sealed_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    sealed = pd.read_parquet(sealed_path)[["case_id", "code", "ymd", "year", "sample_group"]]
    labels = human.merge(sealed, on=["case_id", "code", "ymd"], validate="one_to_one")
    labels = labels[labels["sideways_decision"] != "BORDERLINE"].copy()
    labels["human_sideways"] = labels["sideways_decision"].eq("SIDEWAYS")
    bars = load_bars(db_path, sorted(labels["code"].astype(str).str.zfill(4).unique()))
    outcomes = attach_outcomes(labels, bars)
    outcomes.to_parquet(output / "event_outcomes.parquet", index=False)
    comparisons: dict[str, dict] = {}
    for horizon in HORIZONS:
        for metric in ("max_excursion_atr", "future_range_atr", "abs_close_move_atr", "max_excursion_pre_range", "expansion_2atr"):
            column = f"{metric}_{horizon}"
            comparisons[column] = metric_comparison(outcomes, column)
    yearly = []
    for year, part in outcomes.groupby("year", sort=True):
        positive = part[part["human_sideways"]]["max_excursion_atr_20"]
        negative = part[~part["human_sideways"]]["max_excursion_atr_20"]
        yearly.append({
            "year": int(year), "sideways_n": int(len(positive)), "control_n": int(len(negative)),
            "max_excursion_atr_20_difference": float(positive.mean() - negative.mean()) if len(positive) and len(negative) else None,
        })
    primary_continuous = comparisons["max_excursion_atr_20"]
    primary_binary = comparisons["expansion_2atr_20"]
    positive_years = sum(row["max_excursion_atr_20_difference"] is not None and row["max_excursion_atr_20_difference"] > 0 for row in yearly)
    gates = {
        "max_excursion_uplift_ge_0_25_atr": primary_continuous["stratum_adjusted_difference"] >= 0.25,
        "max_excursion_permutation_p_le_0_05": primary_continuous["one_sided_stratified_permutation_p"] <= 0.05,
        "expansion_rate_uplift_ge_0_10": primary_binary["stratum_adjusted_difference"] >= 0.10,
        "expansion_rate_permutation_p_le_0_05": primary_binary["one_sided_stratified_permutation_p"] <= 0.05,
        "positive_years_ge_4": positive_years >= 4,
    }
    passed = all(gates.values())
    any_statistical = gates["max_excursion_permutation_p_le_0_05"] or gates["expansion_rate_permutation_p_le_0_05"]
    judgment = "keep_human_sideways_as_expansion_foundation" if passed else "hold_partial_expansion_evidence" if any_statistical else "drop_human_sideways_expansion_hypothesis"
    payload = {
        "schema_version": "tradex_sideways_human_expansion_v1",
        "artifact_role": "authoritative_human_sideways_directionless_expansion_comparison",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "human_freeze_sha256": sha256(human_path), "sealed_sha256": sha256(sealed_path),
            "sideways_definition": "frozen human SIDEWAYS", "control": "frozen human NOT_SIDEWAYS",
            "borderline_policy": "excluded", "horizons": list(HORIZONS), "expansion_threshold_atr": 2.0,
            "adjustment_strata": ["year", "original_blind_sample_group"], "permutations": PERMUTATIONS,
            "direction_used": False, "costs": "not_applicable", "runtime_db_write": False,
        },
        "counts": {"sideways": int(outcomes["human_sideways"].sum()), "control": int((~outcomes["human_sideways"]).sum()), "borderline_excluded": int((human["sideways_decision"] == "BORDERLINE").sum())},
        "comparisons": comparisons, "yearly": yearly, "positive_year_count": positive_years,
        "precommitted_gate_results": gates, "judgment": judgment,
        "not_changed": ["sideways detector", "MeeMee", "ranking", "runtime DB", "buy/sell rules"],
    }
    compare_path = output / "compare.json"
    compare_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha256(compare_path)}, indent=2) + "\n", encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--human", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.db, args.human, args.sealed, args.output), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
