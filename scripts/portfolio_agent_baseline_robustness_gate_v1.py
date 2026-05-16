from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts import portfolio_agent_replay_v1 as replay


AXIS_ID = "portfolio_agent_baseline_robustness_gate_v1"
SCHEMA_PREFIX = "tradex_portfolio_agent_baseline_robustness_gate_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\portfolio_agent_baseline_robustness_gate_v1")
BASELINE_ID = "portfolio_agent_replay_v1"
PERIODS = (
    (2019, 20190101, 20200101),
    (2020, 20200101, 20210101),
    (2021, 20210101, 20220101),
    (2022, 20220101, 20230101),
    (2023, 20230101, 20240101),
    (2024, 20240101, 20250101),
    (2025, 20250101, 20260101),
)
DECISIONS = (
    "baseline_keep_for_research_foundation",
    "baseline_hold_due_to_mixed_years",
    "baseline_drop_due_to_severe_drawdown",
    "baseline_requires_regime_filter",
    "baseline_requires_risk_control_redesign",
)
REQUIRED_ARTIFACTS = (
    "baseline_robustness_summary.json",
    "yearly_results.csv",
    "yearly_benchmark_comparison.csv",
    "yearly_failure_modes.csv",
    "drawdown_summary.csv",
    "orders_summary.csv",
    "baseline_robustness_decision.json",
    "_ARTIFACT_COMPLETE.json",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _order_counts(path: Path) -> dict[str, int]:
    frame = pd.read_csv(path)
    if frame.empty or "action" not in frame.columns:
        return {"order_count": 0, "stop_count": 0, "exit_count": 0, "buy_count": 0}
    counts = frame["action"].astype(str).value_counts().to_dict()
    return {
        "order_count": int(len(frame)),
        "stop_count": int(counts.get("stop", 0)),
        "exit_count": int(counts.get("exit", 0)),
        "buy_count": int(counts.get("buy", 0)),
    }


def _collect_year_result(year: int, output_dir: Path, result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    failure = _read_json(output_dir / "failure_diagnosis_summary.json")
    complete = _read_json(output_dir / "_ARTIFACT_COMPLETE.json")
    benchmark = failure.get("benchmark", {})
    metrics = failure.get("metrics", {})
    final_equity = float(metrics.get("final_equity", result.get("final_equity")))
    total_return = float(metrics.get("total_return", result.get("total_return")))
    benchmark_return = benchmark.get("market_benchmark_total_return")
    benchmark_return = None if benchmark_return is None else float(benchmark_return)
    excess_return = None if benchmark_return is None else total_return - benchmark_return
    max_drawdown = float(metrics.get("max_drawdown"))
    orders = _order_counts(output_dir / "orders_ledger.csv")
    accounting = complete.get("accounting_reconciliation", {})
    row = {
        "year": year,
        "run_dir": str(output_dir),
        "final_equity": final_equity,
        "total_return": total_return,
        "benchmark_return": benchmark_return,
        "excess_return": excess_return,
        "max_drawdown": max_drawdown,
        "order_count": orders["order_count"],
        "win_rate": metrics.get("win_rate"),
        "stop_count": orders["stop_count"],
        "exit_count": orders["exit_count"],
        "primary_failure_mode": failure.get("primary_failure_mode"),
        "no_lookahead_audit": complete.get("no_lookahead_audit"),
        "accounting_reconciliation": accounting.get("status"),
        "next_open_execution": complete.get("next_open_execution"),
        "required_artifacts_all_present": complete.get("required_artifacts_all_present"),
        "critical_logs_non_empty": complete.get("critical_logs_non_empty"),
        "benchmark_status": benchmark.get("benchmark_status", metrics.get("benchmark_status")),
        "benchmark_code": benchmark.get("benchmark_code"),
    }
    benchmark_row = {
        "year": year,
        "portfolio_return": total_return,
        "benchmark_return": benchmark_return,
        "excess_return": excess_return,
        "benchmark_status": row["benchmark_status"],
        "benchmark_code": row["benchmark_code"],
        "benchmark_positive_portfolio_negative": bool(benchmark_return is not None and benchmark_return > 0 and total_return < 0),
    }
    failure_row = {
        "year": year,
        "primary_failure_mode": failure.get("primary_failure_mode"),
        "secondary_risks": "|".join(failure.get("secondary_risks", [])),
        "bought_weak_candidate_count": metrics.get("bought_weak_candidate_count"),
        "missed_winner_count": metrics.get("missed_winner_count"),
        "profit_factor": metrics.get("profit_factor"),
        "cost_return_drag": metrics.get("cost_return_drag"),
    }
    drawdown_row = {
        "year": year,
        "max_drawdown": max_drawdown,
        "severe_drawdown_fail": bool(max_drawdown <= -0.35),
        "total_return": total_return,
        "severe_return_fail": bool(total_return <= -0.30),
    }
    orders_row = {
        "year": year,
        "order_count": orders["order_count"],
        "buy_count": orders["buy_count"],
        "stop_count": orders["stop_count"],
        "exit_count": orders["exit_count"],
        "run_dir": str(output_dir),
    }
    return row, benchmark_row, failure_row, drawdown_row, orders_row


def _decide(yearly_rows: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    severe_drawdown_years = [row["year"] for row in yearly_rows if float(row["max_drawdown"]) <= -0.35]
    severe_return_years = [row["year"] for row in yearly_rows if float(row["total_return"]) <= -0.30]
    benchmark_underperform_years = [
        row["year"]
        for row in yearly_rows
        if row.get("excess_return") is not None and float(row["excess_return"]) < 0
    ]
    positive_benchmark_negative_portfolio = [
        row["year"]
        for row in yearly_rows
        if row.get("benchmark_return") is not None and float(row["benchmark_return"]) > 0 and float(row["total_return"]) < 0
    ]
    profitable_years = [row["year"] for row in yearly_rows if float(row["total_return"]) > 0]
    evidence = {
        "severe_drawdown_years": severe_drawdown_years,
        "severe_return_years": severe_return_years,
        "benchmark_underperform_years": benchmark_underperform_years,
        "benchmark_underperform_year_count": len(benchmark_underperform_years),
        "positive_benchmark_negative_portfolio_years": positive_benchmark_negative_portfolio,
        "profitable_years": profitable_years,
        "profitable_year_count": len(profitable_years),
    }
    if severe_drawdown_years:
        if len(severe_drawdown_years) >= 2:
            return "baseline_requires_risk_control_redesign", "multiple_severe_drawdown_years", evidence
        return "baseline_drop_due_to_severe_drawdown", "any_year_max_drawdown_lte_minus35", evidence
    if positive_benchmark_negative_portfolio:
        return "baseline_requires_regime_filter", "positive_benchmark_negative_portfolio_year_detected", evidence
    if len(benchmark_underperform_years) >= 3:
        return "baseline_requires_regime_filter", "multiple_benchmark_underperformance_years", evidence
    if len(profitable_years) >= 5:
        return "baseline_keep_for_research_foundation", "profitable_most_years_without_severe_drawdown", evidence
    return "baseline_hold_due_to_mixed_years", "mixed_yearly_results", evidence


def run_baseline_robustness_gate(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    gate_id: str | None = None,
) -> dict[str, Any]:
    gate_root = Path(output_root) / (gate_id or "baseline-2019-2025-robustness-gate")
    subrun_root = gate_root / "subruns"
    gate_root.mkdir(parents=True, exist_ok=True)
    yearly_rows: list[dict[str, Any]] = []
    benchmark_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    drawdown_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []

    for year, start_ymd, end_ymd in PERIODS:
        result = replay.run_portfolio_agent_replay_v1(
            source_db=source_db,
            output_root=subrun_root,
            run_id=f"{year}-baseline",
            start_ymd=start_ymd,
            end_ymd=end_ymd,
        )
        output_dir = Path(result["output_dir"])
        yearly, benchmark, failure, drawdown, orders = _collect_year_result(year, output_dir, result)
        yearly_rows.append(yearly)
        benchmark_rows.append(benchmark)
        failure_rows.append(failure)
        drawdown_rows.append(drawdown)
        order_rows.append(orders)

    decision, reason, evidence = _decide(yearly_rows)
    _write_csv(gate_root / "yearly_results.csv", yearly_rows)
    _write_csv(gate_root / "yearly_benchmark_comparison.csv", benchmark_rows)
    _write_csv(gate_root / "yearly_failure_modes.csv", failure_rows)
    _write_csv(gate_root / "drawdown_summary.csv", drawdown_rows)
    _write_csv(gate_root / "orders_summary.csv", order_rows)
    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "baseline_id": BASELINE_ID,
        "fixed_conditions": {
            "baseline_policy_changed": False,
            "execution": "next_session_open",
            "same_cost_slippage": True,
            "parameter_tuning": False,
            "threshold_sweep": False,
            "optimization": False,
        },
        "periods": [{"year": year, "start_ymd": start, "end_ymd": end} for year, start, end in PERIODS],
        "decision": decision,
        "reason_type": reason,
        "evidence": evidence,
        "scope": {
            "tradex_only": True,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "ranking_changed": False,
            "publish_registry_changed": False,
            "policy_promotion_allowed": False,
        },
    }
    _write_json(gate_root / "baseline_robustness_summary.json", summary)
    _write_json(
        gate_root / "baseline_robustness_decision.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "decision_candidates": list(DECISIONS),
            "decision": decision,
            "decision_count": 1,
            "reason_type": reason,
            "evidence": evidence,
            "baseline_id": BASELINE_ID,
            "policy_promotion_allowed": False,
            "meemee_reflectable": False,
        },
    )
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "complete": True,
        "required_artifacts_all_present": all((gate_root / artifact).exists() for artifact in REQUIRED_ARTIFACTS if artifact != "_ARTIFACT_COMPLETE.json"),
        "baseline_id": BASELINE_ID,
        "decision": decision,
        "decision_count": 1,
        "period_count": len(PERIODS),
        "parameter_tuning": False,
        "threshold_sweep": False,
        "optimization": False,
        "silent_fallback_used": False,
        "meemee_reflectable": False,
        "policy_promotion_allowed": False,
    }
    _write_json(gate_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(gate_root), "decision": decision, "reason_type": reason, "evidence": evidence}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run baseline portfolio agent robustness gate.")
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--gate-id", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_baseline_robustness_gate(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        gate_id=args.gate_id.strip() or None,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
