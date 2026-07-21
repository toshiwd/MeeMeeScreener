from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

if __package__:
    from scripts.tradex_sideways_human_expansion_v1 import load_bars, sha256
else:
    from tradex_sideways_human_expansion_v1 import load_bars, sha256


CHECKPOINTS = tuple(range(0, 6))
HORIZON = 20
STAGES = ("price_move", "breakout", "candle_volume")


def first_hit_direction(future: pd.DataFrame, *, upper: float, lower: float) -> tuple[str, int | None]:
    for day, row in enumerate(future.itertuples(index=False), start=1):
        hit_up = float(row.h) >= upper
        hit_down = float(row.l) <= lower
        if hit_up and hit_down:
            return "SAME_DAY_BOTH", day
        if hit_up:
            return "UP", day
        if hit_down:
            return "DOWN", day
    return "UNRESOLVED", None


def checkpoint_features(history: pd.DataFrame, index: int, checkpoint: int, atr14: float, pre_high: float, pre_low: float) -> dict[str, float]:
    close0 = float(history.iloc[index]["c"])
    current = history.iloc[index + checkpoint]
    observed = history.iloc[index : index + checkpoint + 1]
    previous_close = float(history.iloc[index + checkpoint - 1]["c"]) if checkpoint > 0 else float(history.iloc[index - 1]["c"])
    pre = history.iloc[index - 59 : index + 1]
    pre_range = pre_high - pre_low
    closes = observed["c"].to_numpy(float)
    path = float(np.abs(np.diff(closes)).sum()) if len(closes) > 1 else 0.0
    net = float(closes[-1] - closes[0])
    candle_high, candle_low = float(current["h"]), float(current["l"])
    candle_open, candle_close = float(current["o"]), float(current["c"])
    candle_range = candle_high - candle_low
    volume20 = float(pre["v"].tail(20).mean())
    return {
        "move_atr": (candle_close - close0) / atr14,
        "move_pre_range": (candle_close - close0) / pre_range if pre_range > 0 else np.nan,
        "path_efficiency_signed": net / path if path > 0 else 0.0,
        "max_up_close_atr": (float(observed["c"].max()) - close0) / atr14,
        "max_down_close_atr": (close0 - float(observed["c"].min())) / atr14,
        "pre_close_pos15": (close0 - pre_low) / pre_range if pre_range > 0 else 0.5,
        "pre_ret5": close0 / float(history.iloc[index - 5]["c"]) - 1.0,
        "pre_ret20": close0 / float(history.iloc[index - 20]["c"]) - 1.0,
        "close_above_pre_high_atr": (candle_close - pre_high) / atr14,
        "close_below_pre_low_atr": (pre_low - candle_close) / atr14,
        "high_break_atr": (float(observed["h"].max()) - pre_high) / atr14,
        "low_break_atr": (pre_low - float(observed["l"].min())) / atr14,
        "current_pre_range_position": (candle_close - pre_low) / pre_range if pre_range > 0 else 0.5,
        "body_atr": (candle_close - candle_open) / atr14,
        "upper_wick_atr": (candle_high - max(candle_open, candle_close)) / atr14,
        "lower_wick_atr": (min(candle_open, candle_close) - candle_low) / atr14,
        "candle_close_position": (candle_close - candle_low) / candle_range if candle_range > 0 else 0.5,
        "gap_atr": (candle_open - previous_close) / atr14,
        "volume_to_pre20": float(current["v"]) / volume20 if volume20 > 0 else np.nan,
    }


def build_checkpoint_table(labels: pd.DataFrame, bars: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    grouped = {code: frame.sort_values("ymd").reset_index(drop=True) for code, frame in bars.groupby("code", sort=False)}
    rows, label_counts = [], {"UP": 0, "DOWN": 0, "SAME_DAY_BOTH": 0, "UNRESOLVED": 0}
    for case in labels.itertuples(index=False):
        history = grouped[str(case.code).zfill(4)]
        positions = np.flatnonzero(history["ymd"].to_numpy() == int(case.ymd))
        if len(positions) != 1:
            raise ValueError(f"event date not uniquely found for {case.case_id}")
        index = int(positions[0])
        if index < 60 or index + HORIZON >= len(history):
            raise ValueError(f"incomplete event history for {case.case_id}")
        past15 = history.iloc[index - 14 : index + 1]
        previous = history["c"].shift(1).iloc[index - 14 : index + 1]
        true_range = pd.concat([
            past15["h"] - past15["l"], (past15["h"] - previous).abs(), (past15["l"] - previous).abs(),
        ], axis=1).max(axis=1)
        atr14 = float(true_range.tail(14).mean())
        close0 = float(history.iloc[index]["c"])
        future = history.iloc[index + 1 : index + HORIZON + 1]
        direction, first_day = first_hit_direction(future, upper=close0 + 2 * atr14, lower=close0 - 2 * atr14)
        label_counts[direction] += 1
        if direction not in {"UP", "DOWN"}:
            continue
        pre_high, pre_low = float(past15["h"].max()), float(past15["l"].min())
        for checkpoint in CHECKPOINTS:
            rows.append({
                "case_id": case.case_id, "code": str(case.code).zfill(4), "ymd": int(case.ymd),
                "year": int(case.year), "checkpoint": checkpoint, "target_up": direction == "UP",
                "first_hit_day": int(first_day), "known_by_checkpoint": int(first_day) <= checkpoint,
                **checkpoint_features(history, index, checkpoint, atr14, pre_high, pre_low),
            })
    return pd.DataFrame(rows), label_counts


def stage_features() -> dict[str, list[str]]:
    price = ["move_atr", "move_pre_range", "path_efficiency_signed", "max_up_close_atr", "max_down_close_atr", "pre_close_pos15", "pre_ret5", "pre_ret20"]
    breakout = ["close_above_pre_high_atr", "close_below_pre_low_atr", "high_break_atr", "low_break_atr", "current_pre_range_position"]
    candle = ["body_atr", "upper_wick_atr", "lower_wick_atr", "candle_close_position", "gap_atr", "volume_to_pre20"]
    return {"price_move": price, "breakout": [*price, *breakout], "candle_volume": [*price, *breakout, *candle]}


def model(c_value: float) -> Pipeline:
    return Pipeline([
        ("impute", SimpleImputer(strategy="median")), ("scale", RobustScaler()),
        ("model", LogisticRegression(C=c_value, class_weight="balanced", max_iter=2000, random_state=19)),
    ])


def metrics(frame: pd.DataFrame) -> dict:
    decided = frame[frame["decided"]].copy()
    coverage = len(decided) / len(frame) if len(frame) else 0.0
    if not len(decided):
        return {"eligible_count": int(len(frame)), "decided_count": 0, "coverage": coverage, "accuracy": None, "balanced_accuracy": None, "up_recall": None, "down_recall": None, "minimum_year_accuracy": None, "gate_pass": False}
    target, prediction = decided["target_up"], decided["prediction"]
    year_accuracy = decided.groupby("year").apply(lambda part: float((part["target_up"] == part["prediction"]).mean()), include_groups=False)
    up_recall = float(recall_score(target, prediction, pos_label=True, zero_division=0))
    down_recall = float(recall_score(target, prediction, pos_label=False, zero_division=0))
    result = {
        "eligible_count": int(len(frame)), "decided_count": int(len(decided)), "coverage": float(coverage),
        "accuracy": float(accuracy_score(target, prediction)),
        "balanced_accuracy": float((up_recall + down_recall) / 2),
        "up_recall": up_recall,
        "down_recall": down_recall,
        "minimum_year_accuracy": float(year_accuracy.min()),
    }
    result["gate_pass"] = bool(
        result["coverage"] >= 0.50 and result["accuracy"] >= 0.70 and result["balanced_accuracy"] >= 0.70
        and result["up_recall"] >= 0.60 and result["down_recall"] >= 0.60 and result["minimum_year_accuracy"] >= 0.60
    )
    return result


def select_hyperparameters(train: pd.DataFrame, features: list[str]) -> tuple[float, float]:
    candidates = []
    years = sorted(train["year"].unique())
    for c_value in (0.03, 0.1, 0.3, 1.0, 3.0):
        probabilities = pd.Series(index=train.index, dtype=float)
        for year in years:
            fit, valid = train[train["year"] != year], train[train["year"] == year]
            estimator = model(c_value).fit(fit[features], fit["target_up"])
            probabilities.loc[valid.index] = estimator.predict_proba(valid[features])[:, 1]
        for margin in (0.0, 0.05, 0.10, 0.15, 0.20, 0.25):
            scored = train[["target_up", "year"]].copy()
            scored["probability"] = probabilities
            scored["decided"] = (probabilities >= 0.5 + margin) | (probabilities <= 0.5 - margin)
            scored["prediction"] = probabilities >= 0.5
            result = metrics(scored)
            candidates.append((int(result["gate_pass"]), result["balanced_accuracy"] or 0, result["coverage"], -margin, -c_value, c_value, margin))
    best = max(candidates)
    return float(best[-2]), float(best[-1])


def evaluate(table: pd.DataFrame, features: list[str]) -> tuple[dict, pd.DataFrame]:
    parts = []
    for year in sorted(table["year"].unique()):
        train, test = table[table["year"] != year], table[table["year"] == year]
        c_value, margin = select_hyperparameters(train, features)
        estimator = model(c_value).fit(train[features], train["target_up"])
        probability = estimator.predict_proba(test[features])[:, 1]
        part = test[["case_id", "year", "checkpoint", "target_up", "first_hit_day"]].copy()
        part["probability"] = probability
        part["prediction"] = probability >= 0.5
        part["decided"] = (probability >= 0.5 + margin) | (probability <= 0.5 - margin)
        part["selected_c"] = c_value
        part["selected_margin"] = margin
        parts.append(part)
    predictions = pd.concat(parts).sort_values("case_id").reset_index(drop=True)
    return metrics(predictions), predictions


def evaluate_signed_move_rule(table: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    parts = []
    thresholds = (0.0, 0.10, 0.25, 0.50, 0.75, 1.0)
    for year in sorted(table["year"].unique()):
        train, test = table[table["year"] != year], table[table["year"] == year]
        candidates = []
        for threshold in thresholds:
            scored = train[["target_up", "year"]].copy()
            scored["prediction"] = train["move_atr"] > 0
            scored["decided"] = train["move_atr"].abs() >= threshold
            result = metrics(scored)
            candidates.append((int(result["gate_pass"]), result["balanced_accuracy"] or 0, result["coverage"], -threshold, threshold))
        selected_threshold = float(max(candidates)[-1])
        part = test[["case_id", "year", "checkpoint", "target_up", "first_hit_day", "move_atr"]].copy()
        part["probability"] = np.nan
        part["prediction"] = part["move_atr"] > 0
        part["decided"] = part["move_atr"].abs() >= selected_threshold
        part["selected_c"] = np.nan
        part["selected_margin"] = selected_threshold
        parts.append(part)
    predictions = pd.concat(parts).sort_values("case_id").reset_index(drop=True)
    return metrics(predictions), predictions


def run(db_path: Path, human_path: Path, sealed_path: Path, output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    human = pd.read_parquet(human_path)
    sealed = pd.read_parquet(sealed_path)[["case_id", "code", "ymd", "year"]]
    labels = human.merge(sealed, on=["case_id", "code", "ymd"], validate="one_to_one")
    labels = labels[labels["sideways_decision"] == "SIDEWAYS"].copy()
    bars = load_bars(db_path, sorted(labels["code"].astype(str).str.zfill(4).unique()))
    table, label_counts = build_checkpoint_table(labels, bars)
    table.to_parquet(output / "checkpoint_features.parquet", index=False)
    results, rule_results, ledgers = [], [], []
    for checkpoint in CHECKPOINTS:
        checkpoint_all = table[table["checkpoint"] == checkpoint]
        eligible = checkpoint_all[checkpoint_all["first_hit_day"] > checkpoint].copy()
        known_share = float(checkpoint_all["known_by_checkpoint"].mean())
        for stage, features in stage_features().items():
            result, predictions = evaluate(eligible, features)
            results.append({"checkpoint": checkpoint, "stage": stage, "feature_count": len(features), "known_direction_share_by_checkpoint": known_share, **result})
            predictions.insert(1, "stage", stage)
            ledgers.append(predictions)
        rule_result, rule_predictions = evaluate_signed_move_rule(eligible)
        rule_results.append({"checkpoint": checkpoint, "stage": "signed_move_atr_rule", "known_direction_share_by_checkpoint": known_share, **rule_result})
        rule_predictions.insert(1, "stage", "signed_move_atr_rule")
        ledgers.append(rule_predictions)
    pd.concat(ledgers, ignore_index=True).to_parquet(output / "loyo_direction_predictions.parquet", index=False)
    passing = [row for row in results if row["gate_pass"]]
    selected = min(passing, key=lambda row: (row["checkpoint"], STAGES.index(row["stage"]))) if passing else max(results, key=lambda row: (row["balanced_accuracy"] or 0, row["coverage"], -row["checkpoint"]))
    passing_rules = [row for row in rule_results if row["gate_pass"]]
    selected_rule = min(passing_rules, key=lambda row: row["checkpoint"]) if passing_rules else max(rule_results, key=lambda row: (row["balanced_accuracy"] or 0, row["coverage"], -row["checkpoint"]))
    payload = {
        "schema_version": "tradex_sideways_human_direction_checkpoint_v1",
        "artifact_role": "authoritative_human_sideways_earliest_direction_checkpoint",
        "review_only": True,
        "fixed_conditions": {
            "database": str(db_path), "human_freeze_sha256": sha256(human_path), "sealed_sha256": sha256(sealed_path),
            "population": "frozen human SIDEWAYS only", "label": "first 2ATR barrier within 20 sessions",
            "same_day_both_and_unresolved_policy": "excluded", "checkpoint_population": "first_hit_day greater than checkpoint",
            "validation": "nested_leave_one_year_out", "future_features_used": False,
        },
        "label_counts": label_counts,
        "gate": {"coverage_min": 0.50, "accuracy_min": 0.70, "balanced_accuracy_min": 0.70, "up_recall_min": 0.60, "down_recall_min": 0.60, "minimum_year_accuracy_min": 0.60},
        "results": results,
        "signed_move_rule_results": rule_results,
        "earliest_passing_checkpoint": int(selected["checkpoint"]) if passing else None,
        "selected_result": selected,
        "earliest_passing_signed_move_rule_checkpoint": int(selected_rule["checkpoint"]) if passing_rules else None,
        "selected_signed_move_rule_result": selected_rule,
        "judgment": "keep_earliest_direction_checkpoint_review_only" if passing else "hold_no_direction_checkpoint_passed_gate",
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
