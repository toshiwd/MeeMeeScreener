from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import resolve_runtime_stock_db_path
from tradex_short_shape_bad_avoidance_probe_v1 import _bucket, _numeric_features_for
from tradex_short_shape_numeric_multiclass_probe_v1 import _neutral_rows_from_db


AXIS_ID = "short_shape_numeric_rule_probe_v1"
DEFAULT_EXTREME_TRAIN_DIR = Path(
    r"G:\Tradex\short_shape_labeled_screenshot_dataset_light144_recent_v1"
    r"\combined_light144_recent_dataset_v1"
)
DEFAULT_HOLDOUT_DIR = Path(
    r"G:\Tradex\short_shape_unbiased_holdout_v1"
    r"\combined_unbiased_holdout80_v1"
)
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_shape_numeric_rule_probe_v1")
FEATURE_NAMES = [
    "close_vs_ma7",
    "close_vs_ma20",
    "close_vs_ma60",
    "ma20_vs_ma60",
    "ma20_slope5",
    "range_pos60",
    "close_vs_prev_high20",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "volume_ratio20",
    "close_below_ma7",
    "close_below_ma20",
    "high_touched_ma20_close_below",
    "bear_candle",
]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _class(row: dict[str, Any]) -> str:
    return str(row.get("purpose_outcome_class") or "")


def _mask_for_clause(x: np.ndarray, clause: dict[str, Any]) -> np.ndarray:
    col = FEATURE_NAMES.index(clause["feature"])
    if clause["op"] == "<=":
        return x[:, col] <= clause["threshold"]
    return x[:, col] >= clause["threshold"]


def _apply_rule(x: np.ndarray, clauses: list[dict[str, Any]]) -> np.ndarray:
    mask = np.ones(x.shape[0], dtype=bool)
    for clause in clauses:
        mask &= _mask_for_clause(x, clause)
    return mask


def _rate(rows: list[dict[str, Any]], klass: str) -> float:
    return sum(1 for row in rows if _class(row) == klass) / len(rows) if rows else 0.0


def _candidate_clauses(x: np.ndarray) -> list[dict[str, Any]]:
    clauses: list[dict[str, Any]] = []
    for index, name in enumerate(FEATURE_NAMES):
        values = x[:, index]
        for q in (0.2, 0.35, 0.5, 0.65, 0.8):
            threshold = float(np.quantile(values, q))
            clauses.append({"feature": name, "op": "<=", "threshold": threshold})
            clauses.append({"feature": name, "op": ">=", "threshold": threshold})
    return clauses


def _summarize_rule(x: np.ndarray, rows: list[dict[str, Any]], clauses: list[dict[str, Any]]) -> dict[str, Any]:
    mask = _apply_rule(x, clauses)
    selected = [row for row, keep in zip(rows, mask) if bool(keep)]
    return {"clauses": clauses, **_bucket(selected)}


def run(*, extreme_train_dir: Path, holdout_dir: Path, output_root: Path, db_path: Path) -> Path:
    extreme_train_rows = [row for row in _read_jsonl(extreme_train_dir / "label_ledger.jsonl") if _class(row) in {"good_short_shape", "bad_short_shape"}]
    holdout_rows_raw = _read_jsonl(holdout_dir / "label_ledger.jsonl")
    exclude = {str(row["sample_key"]) for row in extreme_train_rows + holdout_rows_raw}
    neutral_train_rows = _neutral_rows_from_db(db_path, exclude_keys=exclude, max_rows=80)
    train_rows_raw = extreme_train_rows + neutral_train_rows
    x_train, train_rows = _numeric_features_for(train_rows_raw, db_path)
    x_holdout, holdout_rows = _numeric_features_for(holdout_rows_raw, db_path)
    train_baseline = _bucket(train_rows)
    holdout_baseline = _bucket(holdout_rows)
    clauses = _candidate_clauses(x_train)
    rule_specs: list[list[dict[str, Any]]] = [[clause] for clause in clauses]
    for left, right in combinations(clauses, 2):
        if left["feature"] == right["feature"]:
            continue
        rule_specs.append([left, right])
    train_ranked: list[dict[str, Any]] = []
    for rule in rule_specs:
        summary = _summarize_rule(x_train, train_rows, rule)
        if summary["n"] < 20:
            continue
        if summary["good_rate"] < train_baseline["good_rate"]:
            continue
        if summary["bad_rate"] > train_baseline["bad_rate"] - 0.05:
            continue
        train_ranked.append(summary)
    train_ranked.sort(key=lambda row: (row["bad_rate"], -row["good_rate"], -row["n"]))
    tested: list[dict[str, Any]] = []
    for train_summary in train_ranked[:30]:
        holdout_summary = _summarize_rule(x_holdout, holdout_rows, train_summary["clauses"])
        tested.append({"train": train_summary, "holdout": holdout_summary})
    keep = [
        row for row in tested
        if row["holdout"]["n"] >= 10
        and row["holdout"]["bad_rate"] <= holdout_baseline["bad_rate"] - 0.05
        and row["holdout"]["good_rate"] >= holdout_baseline["good_rate"]
    ]
    report = {
        "schema_version": AXIS_ID,
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "boundary_owner": "TRADEX",
        "fixed_evaluation_conditions": {
            "train_extremes": str(extreme_train_dir),
            "train_neutral_source": "confirmed non-yahoo daily_bars sampled with same setup families, excluding holdout keys",
            "holdout": str(holdout_dir),
            "feature_contract": "numeric MA/range/wick/volume structure only",
            "rule_search": "single and two-clause quantile threshold rules selected on train, evaluated on holdout",
        },
        "train_baseline": train_baseline,
        "holdout_baseline": holdout_baseline,
        "candidate_rule_count": len(rule_specs),
        "train_survivor_count": len(train_ranked),
        "tested_holdout_count": len(tested),
        "top_tested_rules": tested[:10],
        "keep_rules": keep[:10],
        "decision": {
            "candidate_local_decision": "keep_numeric_rule_for_larger_oos" if keep else "drop_numeric_rule_current_form",
            "authoritative_rollup_decision": "research_candidate_not_trade_signal",
            "reason": "at least one train-selected simple rule cleared holdout bad-rate and good-rate gates" if keep else "no train-selected simple numeric rule cleared holdout gates",
        },
        "production_ranking_changed": False,
        "runtime_db_write": False,
        "meemee_unchanged": True,
    }
    output_dir = output_root / f"{_tag()}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    _write_json(output_dir / "numeric_rule_probe.json", report)
    _write_json(output_root / "latest_numeric_rule_probe.json", {"run_root": str(output_dir), **report})
    return output_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extreme-train-dir", type=Path, default=DEFAULT_EXTREME_TRAIN_DIR)
    parser.add_argument("--holdout-dir", type=Path, default=DEFAULT_HOLDOUT_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    args = parser.parse_args()
    print(run(
        extreme_train_dir=args.extreme_train_dir,
        holdout_dir=args.holdout_dir,
        output_root=args.output_root,
        db_path=args.db_path or resolve_runtime_stock_db_path(),
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
