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


AXIS_ID = "tradex_sideways_direction_threshold_v1"
DEFAULT_SOURCE = Path(r"G:\Tradex\tradex_sideways_state_definition_v1\20260717T132208Z-tradex_sideways_state_definition_v1")
DEFAULT_OUT = Path(r"G:\Tradex\tradex_sideways_direction_threshold_v1")
THRESHOLDS_ATR = (0.00, 0.25, 0.50, 0.75, 1.00)
CHECKPOINT = 2
ACCURACY_GATE = 0.65
SIDE_PRECISION_GATE = 0.60
COVERAGE_GATE = 0.20
MIN_SAMPLE = 1_000
YEAR_ACCURACY_GATE = 0.60
YEAR_SIDE_PRECISION_GATE = 0.55
YEAR_COVERAGE_GATE = 0.10
YEAR_MIN_SAMPLE = 500
YEAR_MIN_SIDE_DECISIONS = 50


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def load_fixed_population(source_root: Path, db_path: Path) -> pd.DataFrame:
    snapshots = pd.read_parquet(source_root / "direction_snapshots.parquet")
    population = snapshots[snapshots.checkpoint == CHECKPOINT][
        ["event_id", "code", "signal_ymd", "direction_up", "decision_day"]
    ].copy()
    events = pd.read_parquet(source_root / "event_ledger.parquet")
    resolved = events[
        (events.definition_variant == "direction_efficiency_20")
        & events.realized_direction_20.isin(["up", "down"])
    ].copy().reset_index(drop=True)
    resolved["event_id"] = np.arange(len(resolved), dtype=np.int64)
    keys = resolved[["event_id", "code", "signal_ymd", "c", "atr14"]].rename(columns={"c": "start_close", "atr14": "start_atr14"})
    keys = keys[keys.event_id.isin(population.event_id)]
    with duckdb.connect(str(db_path), read_only=True) as con:
        con.register("event_keys", keys)
        price = con.execute(
            """
            with bars as (
              select cast(code as varchar) code,
                     cast(strftime(to_timestamp(date),'%Y%m%d') as integer) signal_ymd,
                     lead(c, 2) over(partition by code order by date) checkpoint_close
              from daily_bars where source='pan'
            )
            select e.event_id,e.start_close,e.start_atr14,b.checkpoint_close
            from event_keys e join bars b using(code,signal_ymd)
            """
        ).fetchdf()
    frame = population.merge(price, on="event_id", how="inner", validate="one_to_one")
    frame["move_atr_2"] = (frame.checkpoint_close - frame.start_close) / frame.start_atr14
    frame["split"] = np.select(
        [frame.signal_ymd <= 20211231, frame.signal_ymd <= 20231231],
        ["train", "validation"],
        default="test",
    )
    frame["year"] = frame.signal_ymd // 10000
    return frame.replace([np.inf, -np.inf], np.nan).dropna(subset=["move_atr_2"])


def classify_state(move_atr: pd.Series, threshold: float) -> pd.Series:
    values = move_atr.to_numpy(dtype=float)
    if threshold == 0.0:
        states = np.select([values > 0.0, values < 0.0], ["up", "down"], default="unresolved")
    else:
        states = np.select([values >= threshold, values <= -threshold], ["up", "down"], default="unresolved")
    return pd.Series(states, index=move_atr.index, dtype="object")


def state_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    decided = frame[frame.direction_state.isin(["up", "down"])]
    up = decided[decided.direction_state == "up"]
    down = decided[decided.direction_state == "down"]
    correct = ((decided.direction_state == "up") & decided.direction_up.eq(1)) | ((decided.direction_state == "down") & decided.direction_up.eq(0))
    return {
        "sample_count": int(len(frame)),
        "decided_count": int(len(decided)),
        "up_decided_count": int(len(up)),
        "down_decided_count": int(len(down)),
        "coverage": float(len(decided) / len(frame)) if len(frame) else None,
        "direction_accuracy": float(correct.mean()) if len(decided) else None,
        "up_precision": float(up.direction_up.mean()) if len(up) else None,
        "down_precision": float((1 - down.direction_up).mean()) if len(down) else None,
    }


def surface_gate(metrics: dict[str, Any]) -> dict[str, bool]:
    return {
        "sample_count_ge_min": metrics["sample_count"] >= MIN_SAMPLE,
        "direction_accuracy_ge_65pct": (metrics["direction_accuracy"] or 0.0) >= ACCURACY_GATE,
        "up_precision_ge_60pct": (metrics["up_precision"] or 0.0) >= SIDE_PRECISION_GATE,
        "down_precision_ge_60pct": (metrics["down_precision"] or 0.0) >= SIDE_PRECISION_GATE,
        "coverage_ge_20pct": (metrics["coverage"] or 0.0) >= COVERAGE_GATE,
    }


def yearly_gate(metrics: dict[str, Any]) -> dict[str, bool]:
    return {
        "sample_count_ge_min": metrics["sample_count"] >= YEAR_MIN_SAMPLE,
        "up_decided_count_ge_min": metrics["up_decided_count"] >= YEAR_MIN_SIDE_DECISIONS,
        "down_decided_count_ge_min": metrics["down_decided_count"] >= YEAR_MIN_SIDE_DECISIONS,
        "direction_accuracy_ge_60pct": (metrics["direction_accuracy"] or 0.0) >= YEAR_ACCURACY_GATE,
        "up_precision_ge_55pct": (metrics["up_precision"] or 0.0) >= YEAR_SIDE_PRECISION_GATE,
        "down_precision_ge_55pct": (metrics["down_precision"] or 0.0) >= YEAR_SIDE_PRECISION_GATE,
        "coverage_ge_10pct": (metrics["coverage"] or 0.0) >= YEAR_COVERAGE_GATE,
    }


def evaluate_thresholds(frame: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    results: dict[str, Any] = {}
    ledger_parts: list[pd.DataFrame] = []
    for threshold in THRESHOLDS_ATR:
        evaluated = frame.copy()
        evaluated["threshold_atr"] = threshold
        evaluated["direction_state"] = classify_state(evaluated.move_atr_2, threshold)
        split_results: dict[str, Any] = {}
        for split in ("validation", "test"):
            metrics = state_metrics(evaluated[evaluated.split == split])
            gates = surface_gate(metrics)
            split_results[split] = {"metrics": metrics, "gates": gates, "pass": all(gates.values())}
        yearly: dict[str, Any] = {}
        for year in sorted(evaluated.loc[evaluated.year >= 2022, "year"].unique().tolist()):
            metrics = state_metrics(evaluated[evaluated.year == year])
            gates = yearly_gate(metrics)
            yearly[str(year)] = {"metrics": metrics, "gates": gates, "pass": all(gates.values())}
        results[f"atr_{threshold:.2f}"] = {
            "threshold_atr": threshold,
            "splits": split_results,
            "yearly": yearly,
            "overall_gate_pass": all(row["pass"] for row in split_results.values()),
            "yearly_stability_pass": all(row["pass"] for row in yearly.values()),
        }
        ledger_parts.append(evaluated)
    return results, pd.concat(ledger_parts, ignore_index=True)


def select_minimum_threshold(results: dict[str, Any]) -> float | None:
    passing = [
        row["threshold_atr"]
        for row in results.values()
        if row["overall_gate_pass"] and row["yearly_stability_pass"]
    ]
    return min(passing) if passing else None


def state_contract_descriptions(threshold: float | None) -> dict[str, str | None]:
    if threshold is None:
        return {"up": None, "down": None, "unresolved": "no threshold passed"}
    if threshold == 0.0:
        return {
            "up": "two-session close move > 0 ATR14",
            "down": "two-session close move < 0 ATR14",
            "unresolved": "two-session close move = 0 ATR14",
        }
    return {
        "up": f"two-session close move >= +{threshold:.2f} ATR14",
        "down": f"two-session close move <= -{threshold:.2f} ATR14",
        "unresolved": f"absolute two-session close move < {threshold:.2f} ATR14",
    }


def generate(source_root: Path, db_path: Path, out_root: Path) -> Path:
    frame = load_fixed_population(source_root, db_path)
    results, ledger = evaluate_thresholds(frame)
    selected = select_minimum_threshold(results)
    root = out_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True, exist_ok=False)
    ledger.to_parquet(root / "direction_state_ledger.parquet", index=False)
    fixed = {
        "source_root": str(source_root), "checkpoint": CHECKPOINT,
        "population": "checkpoint=2 events unresolved before checkpoint; complete 20-session outcome only",
        "label": "which 2ATR barrier is reached first within 20 sessions",
        "changed_axis": "absolute two-session close move divided by start ATR14 threshold only",
        "thresholds_atr": THRESHOLDS_ATR,
        "splits": {"train": "through 2021", "validation": "2022-2023", "test": "2024+"},
        "runtime_db_write": False, "meemee_changed": False, "production_ranking_changed": False,
    }
    threshold_compare = {
        "schema_version": f"{AXIS_ID}.threshold_compare.v1", "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": fixed,
        "variants": results, "selected_minimum_threshold_atr": selected,
    }
    contract = {
        "schema_version": f"{AXIS_ID}.direction_state_contract.v1", "artifact_role": "authoritative",
        "selected_threshold_atr": selected,
        "state_precedence": [
            "if a 2ATR barrier has already been reached by checkpoint 2, preserve that confirmed direction",
            "otherwise apply the two-session close-move threshold to the unresolved population",
        ],
        "states": state_contract_descriptions(selected),
        "scope": "direction state only; not a buy/sell/entry signal",
        "judgment": "keep" if selected is not None else "hold_no_threshold_passed",
    }
    yearly = {
        "schema_version": f"{AXIS_ID}.yearly_stability.v1", "artifact_role": "authoritative",
        "selected_threshold_atr": selected,
        "selected_yearly": results[f"atr_{selected:.2f}"]["yearly"] if selected is not None else None,
        "all_variants": {name: row["yearly"] for name, row in results.items()},
    }
    compare = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "fixed_evaluation_conditions": fixed,
        "source_artifacts": [
            {"path": str(source_root / "direction_timing.json"), "sha256": _sha256(source_root / "direction_timing.json")},
            {"path": str(source_root / "direction_snapshots.parquet"), "sha256": _sha256(source_root / "direction_snapshots.parquet")},
            {"path": str(db_path), "sha256": _sha256(db_path)},
        ],
        "authoritative_result": {"selected_minimum_threshold_atr": selected, "selected_metrics": results[f"atr_{selected:.2f}"] if selected is not None else None},
        "decision": {
            "candidate_local_decision": "keep_three_state_contract" if selected is not None else "hold_no_threshold_passed",
            "session_aggregate_decision": "keep_three_state_contract" if selected is not None else "hold_no_threshold_passed",
            "authoritative_rollup_decision": "review_only",
            "reason_type": "fixed_validation_test_and_yearly_threshold_gates",
        },
        "observed_branching": {"changed_top5_members_count": None, "changed_top10_members_count": None, "changed_rank_count": None, "selection_divergence_reason": "not a ranking challenger; three-state direction threshold only"},
        "silent_fallback_used": False, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    _write_json(root / "threshold_compare.json", threshold_compare)
    _write_json(root / "direction_state_contract.json", contract)
    _write_json(root / "yearly_stability.json", yearly)
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
