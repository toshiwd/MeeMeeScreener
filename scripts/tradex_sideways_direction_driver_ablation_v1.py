from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, balanced_accuracy_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


AXIS_ID = "tradex_sideways_direction_driver_ablation_v1"
DEFAULT_SOURCE = Path(r"G:\Tradex\tradex_sideways_state_definition_v1\20260717T132208Z-tradex_sideways_state_definition_v1")
DEFAULT_OUT = Path(r"G:\Tradex\tradex_sideways_direction_driver_ablation_v1")
CHECKPOINT = 2
INCREMENT_GATE = 0.01
MIN_SAMPLE = 1_000

BASE_FEATURES = (
    "direction_efficiency", "signed_slope_share", "close_pos15", "candle_close_pos",
    "body_atr", "upper_wick_atr", "lower_wick_atr", "ret5", "ret20", "ret60",
    "ma20_ma60", "volume5_20", "atr14_atr60",
)
PRESTART_FEATURES = tuple(f"{name}_pre" for name in BASE_FEATURES)
DAY2_FEATURES = tuple(f"{name}_day2" for name in BASE_FEATURES)
FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "price_initial_only": ("move_since_start_2",),
    "prestart_context_only": PRESTART_FEATURES,
    "day2_candle_only": tuple(f"{name}_day2" for name in ("candle_close_pos", "body_atr", "upper_wick_atr", "lower_wick_atr")),
    "day2_range_position_only": tuple(f"{name}_day2" for name in ("direction_efficiency", "signed_slope_share", "close_pos15")),
    "day2_volume_volatility_only": ("volume5_20_day2", "atr14_atr60_day2"),
    "day2_higher_trend_only": tuple(f"{name}_day2" for name in ("ret20", "ret60", "ma20_ma60")),
    "price_plus_prestart": ("move_since_start_2", *PRESTART_FEATURES),
    "price_plus_day2_candle": ("move_since_start_2", "candle_close_pos_day2", "body_atr_day2", "upper_wick_atr_day2", "lower_wick_atr_day2"),
    "price_plus_day2_range": ("move_since_start_2", "direction_efficiency_day2", "signed_slope_share_day2", "close_pos15_day2"),
    "price_plus_day2_volume_volatility": ("move_since_start_2", "volume5_20_day2", "atr14_atr60_day2"),
    "price_plus_day2_higher_trend": ("move_since_start_2", "ret20_day2", "ret60_day2", "ma20_ma60_day2"),
    "full_day2_existing": DAY2_FEATURES,
    "price_plus_all_context": ("move_since_start_2", *PRESTART_FEATURES, *DAY2_FEATURES),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def load_fixed_population(source_root: Path, db_path: Path) -> pd.DataFrame:
    snapshots_path = source_root / "direction_snapshots.parquet"
    events_path = source_root / "event_ledger.parquet"
    snapshots = pd.read_parquet(snapshots_path)
    pre = snapshots[snapshots.checkpoint == 0][["event_id", *BASE_FEATURES]].rename(
        columns={name: f"{name}_pre" for name in BASE_FEATURES}
    )
    day2 = snapshots[snapshots.checkpoint == CHECKPOINT][
        ["event_id", "code", "signal_ymd", "direction_up", "decision_day", *BASE_FEATURES]
    ].rename(columns={name: f"{name}_day2" for name in BASE_FEATURES})
    events = pd.read_parquet(events_path)
    resolved = events[
        (events.definition_variant == "direction_efficiency_20")
        & events.realized_direction_20.isin(["up", "down"])
    ].copy().reset_index(drop=True)
    resolved["event_id"] = np.arange(len(resolved), dtype=np.int64)
    event_keys = resolved[["event_id", "code", "signal_ymd", "c"]].rename(columns={"c": "start_close"})
    with duckdb.connect(str(db_path), read_only=True) as con:
        con.register("event_keys", event_keys)
        price = con.execute(
            """
            with bars as (
              select cast(code as varchar) code,
                     cast(strftime(to_timestamp(date),'%Y%m%d') as integer) signal_ymd,
                     lead(c, 2) over(partition by code order by date) snapshot_close
              from daily_bars where source='pan'
            )
            select e.event_id, e.start_close, b.snapshot_close
            from event_keys e join bars b using(code, signal_ymd)
            """
        ).fetchdf()
    price["move_since_start_2"] = price.snapshot_close / price.start_close - 1.0
    fixed = day2.merge(pre, on="event_id", how="inner", validate="one_to_one")
    fixed = fixed.merge(price[["event_id", "move_since_start_2"]], on="event_id", how="inner", validate="one_to_one")
    return fixed.replace([np.inf, -np.inf], np.nan)


def _model(features: tuple[str, ...]) -> Pipeline:
    return Pipeline(
        [
            (
                "prepare",
                ColumnTransformer(
                    [
                        (
                            "numeric",
                            Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]),
                            list(features),
                        )
                    ]
                ),
            ),
            ("model", LogisticRegression(max_iter=500, class_weight="balanced", C=0.3)),
        ]
    )


def _metrics(model: Pipeline, frame: pd.DataFrame, features: tuple[str, ...]) -> tuple[dict[str, Any], np.ndarray]:
    probability = model.predict_proba(frame[list(features)])[:, 1]
    prediction = (probability >= 0.5).astype(np.int8)
    return (
        {
            "sample_count": int(len(frame)),
            "accuracy": float(accuracy_score(frame.direction_up, prediction)),
            "balanced_accuracy": float(balanced_accuracy_score(frame.direction_up, prediction)),
            "roc_auc": float(roc_auc_score(frame.direction_up, probability)),
            "up_share": float(frame.direction_up.mean()),
        },
        probability,
    )


def incremental_gate(candidate: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    surfaces: dict[str, Any] = {}
    for split in ("validation", "test"):
        balanced_delta = candidate[split]["balanced_accuracy"] - baseline[split]["balanced_accuracy"]
        auc_delta = candidate[split]["roc_auc"] - baseline[split]["roc_auc"]
        surfaces[split] = {
            "balanced_accuracy_delta": balanced_delta,
            "roc_auc_delta": auc_delta,
            "sample_count_ge_min": candidate[split]["sample_count"] >= MIN_SAMPLE,
            "balanced_accuracy_delta_ge_1pp": balanced_delta >= INCREMENT_GATE,
            "roc_auc_delta_ge_1pp": auc_delta >= INCREMENT_GATE,
        }
    passed = all(all(value for key, value in row.items() if key.endswith("_min") or key.endswith("_1pp")) for row in surfaces.values())
    return {"surfaces": surfaces, "pass": passed}


def _incremental_pass(result: dict[str, Any]) -> bool:
    gate = result.get("incremental_vs_price_initial")
    return bool(gate and gate.get("pass"))


def evaluate(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    train = frame[frame.signal_ymd <= 20211231]
    validation = frame[frame.signal_ymd.between(20220101, 20231231)]
    test = frame[frame.signal_ymd >= 20240101]
    results: dict[str, Any] = {}
    scores: list[pd.DataFrame] = []
    for name, features in FEATURE_GROUPS.items():
        model = _model(features)
        model.fit(train[list(features)], train.direction_up)
        validation_metrics, validation_probability = _metrics(model, validation, features)
        test_metrics, test_probability = _metrics(model, test, features)
        results[name] = {
            "features": features,
            "train_sample_count": int(len(train)),
            "validation": validation_metrics,
            "test": test_metrics,
        }
        scores.append(pd.DataFrame({"event_id": validation.event_id, "split": "validation", "variant": name, "direction_up": validation.direction_up, "probability_up": validation_probability}))
        scores.append(pd.DataFrame({"event_id": test.event_id, "split": "test", "variant": name, "direction_up": test.direction_up, "probability_up": test_probability}))
    baseline = results["price_initial_only"]
    for name, result in results.items():
        result["incremental_vs_price_initial"] = incremental_gate(result, baseline) if name != "price_initial_only" else None
    return results, pd.concat(scores, ignore_index=True)


def generate(source_root: Path, db_path: Path, out_root: Path) -> Path:
    frame = load_fixed_population(source_root, db_path)
    results, scores = evaluate(frame)
    baseline = results["price_initial_only"]
    passing = [name for name, row in results.items() if _incremental_pass(row)]
    best = max(
        results,
        key=lambda name: (
            results[name]["validation"]["roc_auc"] + results[name]["test"]["roc_auc"],
            results[name]["validation"]["balanced_accuracy"] + results[name]["test"]["balanced_accuracy"],
        ),
    )
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    scores.to_parquet(root / "direction_ablation_scores.parquet", index=False)
    fixed = {
        "source_root": str(source_root),
        "checkpoint": CHECKPOINT,
        "population": "checkpoint=2 unresolved events from authoritative sideways direction snapshots",
        "label": "which 2ATR barrier is reached first within 20 sessions",
        "event_dedup": "minimum 20 trading sessions per code",
        "splits": {"train": "through 2021", "validation": "2022-2023", "test": "2024+"},
        "changed_axis": "feature group only",
        "increment_gate": {"balanced_accuracy_delta": INCREMENT_GATE, "roc_auc_delta": INCREMENT_GATE, "must_pass": ["validation", "test"]},
        "runtime_db_write": False, "meemee_changed": False, "production_ranking_changed": False,
    }
    driver = {
        "schema_version": f"{AXIS_ID}.direction_driver_compare.v1", "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": fixed,
        "baseline": "price_initial_only", "variants": results, "passing_incremental_variants": passing,
        "best_fixed_condition_variant": best,
    }
    leaderboard = {
        "schema_version": f"{AXIS_ID}.feature_group_leaderboard.v1", "artifact_role": "authoritative",
        "rows": [
            {
                "variant": name,
                "validation_balanced_accuracy": row["validation"]["balanced_accuracy"],
                "validation_roc_auc": row["validation"]["roc_auc"],
                "test_balanced_accuracy": row["test"]["balanced_accuracy"],
                "test_roc_auc": row["test"]["roc_auc"],
                "incremental_gate_pass": _incremental_pass(row),
            }
            for name, row in results.items()
        ],
        "leader": best,
    }
    decision = "keep_independent_context" if passing else "drop_independent_context_price_move_explains_signal"
    compare = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "fixed_evaluation_conditions": fixed,
        "source_artifacts": [
            {"path": str(source_root / "direction_timing.json"), "sha256": _sha256(source_root / "direction_timing.json")},
            {"path": str(source_root / "direction_snapshots.parquet"), "sha256": _sha256(source_root / "direction_snapshots.parquet")},
            {"path": str(db_path), "sha256": _sha256(db_path)},
        ],
        "authoritative_result": {"price_initial_only": baseline, "best_variant": best, "passing_incremental_variants": passing},
        "decision": {"candidate_local_decision": decision, "session_aggregate_decision": decision, "authoritative_rollup_decision": "review_only", "reason_type": "fixed_oos_feature_group_ablation_vs_two_day_price_move"},
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None, "selection_divergence_reason": "not a ranking challenger; direction-driver ablation only"},
        "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    _write_json(root / "direction_driver_compare.json", driver)
    _write_json(root / "feature_group_leaderboard.json", leaderboard)
    _write_json(root / "compare.json", compare)
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "complete": True, "compare": str(root / "compare.json")})
    return root / "compare.json"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    print(generate(args.source, args.db, args.out))


if __name__ == "__main__":
    main()
