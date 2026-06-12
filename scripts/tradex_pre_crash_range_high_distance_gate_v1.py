from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tradex_pre_crash_shape_false_positive_escape_v1 import _build_pattern_events, _load_daily
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "pre_crash_range_high_distance_gate_v1"
DEFAULT_OUTPUT_ROOT = Path("G:/Tradex/pre_crash_range_high_distance_gate_v1")
RANGE_20_MIN_VALUES = (0.16, 0.20, 0.24, 0.28)
RANGE_40_MIN_VALUES = (0.12, 0.16, 0.20)
DIST_HIGH_MAX_VALUES = (-0.12, -0.16, -0.20, -0.24)
MIN_CANDIDATES = 500
MIN_SYMBOLS = 80
MIN_MONTHS = 18


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_ready(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _rate(series: pd.Series) -> float:
    return float(series.mean()) if len(series) else 0.0


def _candidate_summary(df: pd.DataFrame, mask: pd.Series, gate: dict[str, Any], baseline_rate: float) -> dict[str, Any]:
    group = df[mask].copy()
    n = int(len(group))
    symbols = int(group["code"].nunique()) if n else 0
    months = int(group["month"].nunique()) if n else 0
    crash_rate = _rate(group["crash20"]) if n else 0.0
    pass_size = n >= MIN_CANDIDATES and symbols >= MIN_SYMBOLS and months >= MIN_MONTHS
    return {
        **gate,
        "n": n,
        "symbols": symbols,
        "months": months,
        "crash_n": int(group["crash20"].sum()) if n else 0,
        "crash_rate": crash_rate,
        "crash_rate_lift_vs_baseline": crash_rate - baseline_rate,
        "mean_future_min20": float(group["future_min20"].mean()) if n else None,
        "median_future_min20": float(group["future_min20"].median()) if n else None,
        "mean_future_ret20": float(group["future_ret20"].mean()) if n else None,
        "escape_rate": _rate(group["escape_bullish_candle_found"]) if n else 0.0,
        "sample_gate_pass": pass_size,
        "decision": "hold_for_pretest" if pass_size and crash_rate >= baseline_rate + 0.04 else "drop_or_diagnostic_only",
    }


def _evaluate_gates(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    df = pd.DataFrame(rows)
    if df.empty:
        return {}, [], []
    baseline = {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "crash_n": int(df["crash20"].sum()),
        "crash_rate": _rate(df["crash20"]),
        "mean_future_min20": float(df["future_min20"].mean()),
        "escape_rate": _rate(df["escape_bullish_candle_found"]),
    }
    out: list[dict[str, Any]] = []
    for r20 in RANGE_20_MIN_VALUES:
        for r40 in RANGE_40_MIN_VALUES:
            for dh in DIST_HIGH_MAX_VALUES:
                gate = {
                    "gate_id": f"range20_ge_{r20:.2f}__range40_ge_{r40:.2f}__dist_high_le_{dh:.2f}",
                    "range_20_0_min": r20,
                    "range_40_20_min": r40,
                    "dist_prior_80_high_max": dh,
                }
                mask = (
                    (df["range_20_0"] >= r20)
                    & (df["range_40_20"] >= r40)
                    & (df["dist_prior_80_high"] <= dh)
                )
                out.append(_candidate_summary(df, mask, gate, float(baseline["crash_rate"])))
    out.sort(
        key=lambda row: (
            row["decision"] == "hold_for_pretest",
            float(row["crash_rate_lift_vs_baseline"]),
            int(row["n"]),
        ),
        reverse=True,
    )
    best_rows = [row for row in out if row["decision"] == "hold_for_pretest"]
    if not best_rows:
        best_rows = out[:5]
    examples: list[dict[str, Any]] = []
    for gate in best_rows[:3]:
        mask = (
            (df["range_20_0"] >= gate["range_20_0_min"])
            & (df["range_40_20"] >= gate["range_40_20_min"])
            & (df["dist_prior_80_high"] <= gate["dist_prior_80_high_max"])
        )
        sample = df[mask].sort_values("future_min20").head(30).copy()
        sample["gate_id"] = gate["gate_id"]
        examples.extend(sample.to_dict(orient="records"))
    return baseline, out, examples


def run(db_path: Path, output_root: Path, code_limit: int | None) -> Path:
    run_dir = output_root / _run_id()
    runtime_status = inspect_runtime_stock_db(runtime_db_path=db_path)
    daily = _load_daily(db_path, code_limit)
    rows, event_meta = _build_pattern_events(daily)
    baseline, leaderboard, examples = _evaluate_gates(rows)
    hold = [row for row in leaderboard if row.get("decision") == "hold_for_pretest"]
    decision = {
        "authoritative_decision": "hold_for_pretest" if hold else "drop_no_gate_with_sufficient_lift",
        "candidate_local_decision": hold[0] if hold else (leaderboard[0] if leaderboard else None),
        "reason": "range_expansion_and_deeper_high_distance_gate_evaluated_on_typical_pattern_population",
        "promotion_allowed": False,
        "meemee_reflection": False,
        "production_ranking_change": False,
        "runtime_db_write": False,
    }
    contract = {
        "axis_id": AXIS_ID,
        "research_phase": "branching_generation",
        "fixed_evaluation_conditions": {
            "universe": "runtime_stock_db daily_bars confirmed PAN rows",
            "population": "typical shape pattern matched events from pre_crash_shape_false_positive_escape_v1 classifier",
            "gate_axis": "range_20_0 + range_40_20 + dist_prior_80_high only",
            "crash_definition": "future_min20 <= -0.15",
            "same_period": "20200101-20991231",
            "cost_slippage": "not_applied_diagnostic_only",
            "borrow_lending": "not_available_short_side_theoretical_only",
            "no_lookahead": "gate features use rows at or before signal date",
        },
        "non_scope": [
            "no MeeMee reflection",
            "no runtime DB writes",
            "no production ranking or candidate-generator mutation",
            "no validated short entry claim",
            "no exit rule change",
            "no multi-axis redesign",
        ],
    }
    _write_json(run_dir / "evaluation_contract.json", contract)
    _write_json(
        run_dir / "run_manifest.json",
        {
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "db_path": str(db_path),
            "output_dir": str(run_dir),
            "code_limit": code_limit,
            "runtime_status": runtime_status,
            "raw_rows": int(len(daily)),
            "event_meta": event_meta,
        },
    )
    _write_json(run_dir / "gate_leaderboard.json", {"baseline": baseline, "gates": leaderboard})
    _write_jsonl(run_dir / "gate_examples.jsonl", examples)
    _write_json(run_dir / "research_decision.json", decision)
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "evaluation_contract.json",
                "run_manifest.json",
                "gate_leaderboard.json",
                "gate_examples.jsonl",
                "research_decision.json",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--code-limit", type=int, default=None)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.code_limit))


if __name__ == "__main__":
    main()
