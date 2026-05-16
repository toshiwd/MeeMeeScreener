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

from scripts import portfolio_agent_replay_c1506_fixed_v1 as c1506


AXIS_ID = "portfolio_agent_fixed_challenger_robustness_gate_v1"
SCHEMA_PREFIX = "tradex_portfolio_agent_fixed_challenger_robustness_gate_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\portfolio_agent_fixed_challenger_robustness_gate_v1")
CHALLENGER_ID = "c1506"
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
    "keep_for_further_validation",
    "hold_due_to_mixed_years",
    "drop_due_to_oos_failure",
    "drop_due_to_severe_drawdown",
    "drop_due_to_benchmark_underperformance",
)
REQUIRED_ARTIFACTS = (
    "robustness_gate_summary.json",
    "yearly_results.csv",
    "yearly_benchmark_comparison.csv",
    "yearly_failure_modes.csv",
    "drawdown_summary.csv",
    "orders_summary.csv",
    "robustness_decision.json",
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


def _max_drawdown_from_equity(path: Path) -> float:
    frame = pd.read_csv(path)
    equity = pd.to_numeric(frame["equity"], errors="coerce")
    peak = equity.cummax()
    return float((equity / peak - 1.0).min())


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


def _collect_year_result(year: int, output_dir: Path, result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    failure = _read_json(output_dir / "failure_diagnosis_summary.json")
    complete = _read_json(output_dir / "_ARTIFACT_COMPLETE.json")
    benchmark = failure.get("benchmark", {})
    metrics = failure.get("metrics", {})
    final_equity = float(metrics.get("final_equity", result.get("final_equity")))
    total_return = float(metrics.get("total_return", result.get("total_return")))
    benchmark_return = benchmark.get("market_benchmark_total_return")
    benchmark_return = None if benchmark_return is None else float(benchmark_return)
    excess_return = None if benchmark_return is None else total_return - benchmark_return
    max_drawdown = float(metrics.get("max_drawdown", _max_drawdown_from_equity(output_dir / "equity_curve.csv")))
    orders = _order_counts(output_dir / "orders_ledger.csv")
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
        "accounting_reconciliation": complete.get("accounting_reconciliation", {}).get("status"),
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
    return row, benchmark_row, failure_row, drawdown_row


def _decide(yearly_rows: list[dict[str, Any]]) -> tuple[str, str, dict[str, Any]]:
    severe_return_years = [row["year"] for row in yearly_rows if float(row["total_return"]) <= -0.30]
    severe_drawdown_years = [row["year"] for row in yearly_rows if float(row["max_drawdown"]) <= -0.35]
    positive_benchmark_negative_portfolio = [
        row["year"]
        for row in yearly_rows
        if row.get("benchmark_return") is not None and float(row["benchmark_return"]) > 0 and float(row["total_return"]) < 0
    ]
    benchmark_underperform_years = [
        row["year"]
        for row in yearly_rows
        if row.get("excess_return") is not None and float(row["excess_return"]) < 0
    ]
    evidence = {
        "severe_return_years": severe_return_years,
        "severe_drawdown_years": severe_drawdown_years,
        "positive_benchmark_negative_portfolio_years": positive_benchmark_negative_portfolio,
        "benchmark_underperform_years": benchmark_underperform_years,
        "benchmark_underperform_year_count": len(benchmark_underperform_years),
    }
    if severe_drawdown_years:
        return "drop_due_to_severe_drawdown", "any_year_max_drawdown_lte_minus35", evidence
    if severe_return_years:
        return "drop_due_to_oos_failure", "any_year_total_return_lte_minus30", evidence
    if positive_benchmark_negative_portfolio or len(benchmark_underperform_years) >= 2:
        return "drop_due_to_benchmark_underperformance", "benchmark_underperformance_gate_failed", evidence
    if benchmark_underperform_years:
        return "hold_due_to_mixed_years", "one_year_benchmark_underperformance", evidence
    return "keep_for_further_validation", "all_years_passed_drop_gates", evidence


def run_robustness_gate(
    *,
    source_db: str | Path | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    gate_id: str | None = None,
) -> dict[str, Any]:
    gate_root = Path(output_root) / (gate_id or f"{CHALLENGER_ID}-robustness-gate")
    subrun_root = gate_root / "subruns"
    gate_root.mkdir(parents=True, exist_ok=True)
    yearly_rows: list[dict[str, Any]] = []
    benchmark_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    drawdown_rows: list[dict[str, Any]] = []
    order_rows: list[dict[str, Any]] = []

    for year, start_ymd, end_ymd in PERIODS:
        result = c1506.run_c1506_fixed_replay(
            source_db=source_db,
            output_root=subrun_root,
            run_id=f"{year}-{CHALLENGER_ID}-fixed",
            start_ymd=start_ymd,
            end_ymd=end_ymd,
        )
        output_dir = Path(result["output_dir"])
        yearly, benchmark, failure, drawdown = _collect_year_result(year, output_dir, result)
        yearly_rows.append(yearly)
        benchmark_rows.append(benchmark)
        failure_rows.append(failure)
        drawdown_rows.append(drawdown)
        order_rows.append(
            {
                "year": year,
                "order_count": yearly["order_count"],
                "buy_count": _order_counts(output_dir / "orders_ledger.csv")["buy_count"],
                "stop_count": yearly["stop_count"],
                "exit_count": yearly["exit_count"],
                "run_dir": str(output_dir),
            }
        )

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
        "challenger_id": CHALLENGER_ID,
        "fixed_conditions": {
            "stop_loss": c1506.C1506_STOP_LOSS,
            "profit_target": None,
            "profit_target_disabled": True,
            "max_holding_trading_days": c1506.C1506_MAX_HOLDING_TRADING_DAYS,
            "execution": "next_session_open",
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
    _write_json(gate_root / "robustness_gate_summary.json", summary)
    _write_json(
        gate_root / "robustness_decision.json",
        {
            "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "decision_candidates": list(DECISIONS),
            "decision": decision,
            "decision_count": 1,
            "reason_type": reason,
            "evidence": evidence,
            "challenger_id": CHALLENGER_ID,
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
        "challenger_id": CHALLENGER_ID,
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
    parser = argparse.ArgumentParser(description="Run fixed challenger robustness gate.")
    parser.add_argument("--source-db", default="")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--gate-id", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_robustness_gate(
        source_db=args.source_db.strip() or None,
        output_root=args.output_root,
        gate_id=args.gate_id.strip() or None,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
