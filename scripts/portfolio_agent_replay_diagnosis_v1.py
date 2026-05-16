from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "portfolio_agent_replay_v1_run_diagnosis_v1"
SCHEMA_PREFIX = "tradex_portfolio_agent_replay_diagnosis_v1"
BIG_WINNER_RET20 = 0.10
WEAK_SELECTED_RET20 = 0.0
STOP_LOSS_THRESHOLD = -0.06

REQUIRED_SOURCE_ARTIFACTS = (
    "run_config.json",
    "failure_diagnosis_summary.json",
    "equity_curve.csv",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "daily_candidate_snapshot.csv",
    "daily_action_ledger.jsonl",
    "rejected_candidates.csv",
    "post_run_outcome_labels.csv",
    "no_lookahead_audit.json",
    "selection_feature_manifest.json",
)

OUTPUT_ARTIFACTS = (
    "replay_diagnosis_summary.json",
    "trade_contribution.csv",
    "monthly_equity_summary.csv",
    "top_winners_losers.csv",
    "missed_winner_cases.csv",
    "bought_weak_candidate_cases.csv",
    "held_loser_too_long_cases.csv",
    "cost_drag_summary.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

NEXT_AXIS_CANDIDATES = ("missed_winner", "bought_weak_candidate", "held_loser_too_long", "cost_drag")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_ready(item) for item in value]
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
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _json_text(payload: Any) -> str:
    return json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, default=str)


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(payload) + "\n", encoding="utf-8")
    return path


def _write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    frame.to_csv(path, index=False)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _ymd_to_timestamp(value: Any) -> pd.Timestamp:
    return pd.to_datetime(str(int(value)), format="%Y%m%d")


def _load_frame(root: Path, name: str) -> pd.DataFrame:
    path = root / name
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact: {path}")
    return pd.read_csv(path)


def _require_sources(root: Path) -> dict[str, bool]:
    return {name: (root / name).exists() for name in REQUIRED_SOURCE_ARTIFACTS}


def build_trade_contribution(orders: pd.DataFrame, positions: pd.DataFrame, equity: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame()
    buys = orders[(orders["order_status"] == "filled") & (orders["action"] == "buy")].copy()
    exits = orders[(orders["order_status"] == "filled") & (orders["action"].isin(["exit", "stop"]))].copy()
    rows: list[dict[str, Any]] = []
    latest_positions = positions.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions.empty else pd.DataFrame()
    final_equity = float(equity.iloc[-1]["equity"]) if not equity.empty else None
    for _idx, buy in buys.iterrows():
        position_id = str(buy.get("position_id"))
        code = str(buy.get("code"))
        exit_rows = exits[exits["position_id"].astype(str) == position_id]
        buy_cost = _safe_float(buy.get("cost_amount")) or 0.0
        buy_notional = _safe_float(buy.get("notional")) or 0.0
        if not exit_rows.empty:
            exit_row = exit_rows.iloc[-1]
            realized_pnl = _safe_float(exit_row.get("realized_pnl")) or 0.0
            exit_cost = _safe_float(exit_row.get("cost_amount")) or 0.0
            total_cost = buy_cost + exit_cost
            contribution = realized_pnl
            status = "closed"
            exit_ymd = int(exit_row["execution_ymd"])
            holding_days = None
            gross_pnl_before_cost = realized_pnl + total_cost
        else:
            pos = latest_positions[latest_positions["position_id"].astype(str) == position_id]
            if pos.empty:
                realized_pnl = 0.0
                contribution = 0.0
                total_cost = buy_cost
                status = "unknown_open_position_missing"
                exit_ymd = None
                holding_days = None
                gross_pnl_before_cost = 0.0
            else:
                last = pos.iloc[-1]
                realized_pnl = 0.0
                contribution = _safe_float(last.get("unrealized_pnl")) or 0.0
                total_cost = buy_cost
                status = "open"
                exit_ymd = None
                holding_days = int(last.get("holding_days")) if not pd.isna(last.get("holding_days")) else None
                gross_pnl_before_cost = contribution + total_cost
        rows.append(
            {
                "position_id": position_id,
                "code": code,
                "status": status,
                "entry_decision_ymd": int(buy["decision_ymd"]),
                "entry_ymd": int(buy["execution_ymd"]),
                "exit_ymd": exit_ymd,
                "buy_notional": buy_notional,
                "total_cost": total_cost,
                "gross_pnl_before_cost": gross_pnl_before_cost,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": contribution if status == "open" else 0.0,
                "net_contribution": contribution,
                "contribution_ratio_of_final_equity": None if not final_equity else contribution / final_equity,
                "holding_days": holding_days,
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    symbol = (
        frame.groupby("code", as_index=False)
        .agg(
            position_count=("position_id", "count"),
            net_contribution=("net_contribution", "sum"),
            realized_pnl=("realized_pnl", "sum"),
            unrealized_pnl=("unrealized_pnl", "sum"),
            total_cost=("total_cost", "sum"),
            gross_pnl_before_cost=("gross_pnl_before_cost", "sum"),
        )
        .sort_values(["net_contribution", "code"], ascending=[False, True], kind="stable")
    )
    symbol["contribution_rank"] = range(1, len(symbol) + 1)
    total_positive = symbol.loc[symbol["net_contribution"] > 0, "net_contribution"].sum()
    total_negative = abs(symbol.loc[symbol["net_contribution"] < 0, "net_contribution"].sum())
    symbol["share_of_positive_contribution"] = symbol["net_contribution"].apply(lambda value: value / total_positive if value > 0 and total_positive else 0.0)
    symbol["share_of_negative_damage"] = symbol["net_contribution"].apply(lambda value: abs(value) / total_negative if value < 0 and total_negative else 0.0)
    return symbol


def build_top_winners_losers(trade_contribution: pd.DataFrame) -> pd.DataFrame:
    if trade_contribution.empty:
        return pd.DataFrame()
    winners = trade_contribution.sort_values(["net_contribution", "code"], ascending=[False, True], kind="stable").head(10).copy()
    winners["bucket"] = "winner"
    losers = trade_contribution.sort_values(["net_contribution", "code"], ascending=[True, True], kind="stable").head(10).copy()
    losers["bucket"] = "loser"
    return pd.concat([winners, losers], ignore_index=True)


def build_monthly_equity_summary(equity: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if equity.empty:
        return pd.DataFrame(), {"status": "empty"}
    work = equity.copy()
    work["date"] = work["ymd"].apply(_ymd_to_timestamp)
    work["month"] = work["date"].dt.to_period("M").astype(str)
    rows: list[dict[str, Any]] = []
    for month, group in work.groupby("month", sort=True):
        group = group.sort_values("ymd", kind="stable")
        start_equity = float(group.iloc[0]["equity"])
        end_equity = float(group.iloc[-1]["equity"])
        bench_start = _safe_float(group.iloc[0].get("market_benchmark_equity"))
        bench_end = _safe_float(group.iloc[-1].get("market_benchmark_equity"))
        rows.append(
            {
                "month": month,
                "start_ymd": int(group.iloc[0]["ymd"]),
                "end_ymd": int(group.iloc[-1]["ymd"]),
                "start_equity": start_equity,
                "end_equity": end_equity,
                "monthly_return": end_equity / start_equity - 1.0 if start_equity else None,
                "benchmark_monthly_return": (bench_end / bench_start - 1.0) if bench_start and bench_end else None,
                "min_equity": float(group["equity"].min()),
                "max_equity": float(group["equity"].max()),
                "end_cash": float(group.iloc[-1]["cash"]),
                "end_open_position_count": int(group.iloc[-1]["open_position_count"]),
            }
        )
    monthly = pd.DataFrame(rows)

    running_peak = work["equity"].cummax()
    drawdown = work["equity"] / running_peak - 1.0
    trough_idx = int(drawdown.idxmin())
    trough_row = work.loc[trough_idx]
    peak_mask = work.index <= trough_idx
    peak_idx = int(work.loc[peak_mask, "equity"].idxmax())
    peak_row = work.loc[peak_idx]
    recovery = work[(work.index > trough_idx) & (work["equity"] >= float(peak_row["equity"]))]
    recovery_ymd = None if recovery.empty else int(recovery.iloc[0]["ymd"])
    recovery_days = None if recovery.empty else int((recovery.iloc[0]["date"] - trough_row["date"]).days)
    best_month = monthly.sort_values(["monthly_return", "month"], ascending=[False, True], kind="stable").iloc[0]
    worst_month = monthly.sort_values(["monthly_return", "month"], ascending=[True, True], kind="stable").iloc[0]
    summary = {
        "max_drawdown": float(drawdown.min()),
        "max_drawdown_peak_ymd": int(peak_row["ymd"]),
        "max_drawdown_trough_ymd": int(trough_row["ymd"]),
        "max_drawdown_recovery_ymd": recovery_ymd,
        "max_drawdown_recovery_days": recovery_days,
        "best_month": str(best_month["month"]),
        "best_month_return": float(best_month["monthly_return"]),
        "worst_month": str(worst_month["month"]),
        "worst_month_return": float(worst_month["monthly_return"]),
    }
    return monthly, summary


def build_missed_winner_cases(
    rejected: pd.DataFrame,
    labels: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    merged = rejected.merge(labels, on=["decision_ymd", "code"], how="left")
    missed = merged[(merged["was_selected"] == False) & (pd.to_numeric(merged["post_ret_20"], errors="coerce") >= BIG_WINNER_RET20)].copy()
    if missed.empty:
        return missed
    selected = labels[labels["was_selected"] == True][["decision_ymd", "code", "post_ret_20"]].rename(
        columns={"code": "selected_instead_code", "post_ret_20": "selected_instead_post_ret_20"}
    )
    selected = selected.sort_values(["decision_ymd", "selected_instead_post_ret_20"], ascending=[True, False], kind="stable").groupby("decision_ymd", as_index=False).head(1)
    missed = missed.merge(selected, on="decision_ymd", how="left")
    candidate_cols = ["decision_ymd", "code", "score_components_json"]
    if set(candidate_cols).issubset(candidates.columns):
        missed = missed.merge(candidates[candidate_cols], on=["decision_ymd", "code"], how="left")
    missed["opportunity_gap_vs_selected"] = pd.to_numeric(missed["post_ret_20"], errors="coerce") - pd.to_numeric(missed["selected_instead_post_ret_20"], errors="coerce")
    return missed.sort_values(["opportunity_gap_vs_selected", "post_ret_20", "decision_ymd"], ascending=[False, False, True], kind="stable")


def build_bought_weak_candidate_cases(
    labels: pd.DataFrame,
    candidates: pd.DataFrame,
) -> pd.DataFrame:
    selected = labels[labels["was_selected"] == True].copy()
    rejected = labels[labels["was_selected"] == False].copy()
    best_rejected = (
        rejected.sort_values(["decision_ymd", "post_ret_20"], ascending=[True, False], kind="stable")
        .groupby("decision_ymd", as_index=False)
        .head(1)[["decision_ymd", "code", "post_ret_20"]]
        .rename(columns={"code": "best_rejected_code", "post_ret_20": "best_rejected_post_ret_20"})
    )
    selected = selected.merge(best_rejected, on="decision_ymd", how="left")
    selected["underperformance_vs_best_rejected"] = pd.to_numeric(selected["best_rejected_post_ret_20"], errors="coerce") - pd.to_numeric(selected["post_ret_20"], errors="coerce")
    weak = selected[(pd.to_numeric(selected["post_ret_20"], errors="coerce") <= WEAK_SELECTED_RET20) | (selected["underperformance_vs_best_rejected"] > 0.10)].copy()
    candidate_cols = ["decision_ymd", "code", "candidate_rank", "selection_score", "score_components_json"]
    if set(candidate_cols).issubset(candidates.columns):
        weak = weak.merge(candidates[candidate_cols], on=["decision_ymd", "code"], how="left")
    weak["weakness_visible_at_decision_time"] = weak.apply(
        lambda row: bool((_safe_float(row.get("selection_score")) or 0.0) <= 10.0 or (_safe_float(row.get("candidate_rank")) or 999.0) > 3.0),
        axis=1,
    )
    return weak.sort_values(["underperformance_vs_best_rejected", "post_ret_20"], ascending=[False, True], kind="stable")


def build_held_loser_cases(orders: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    exits = orders[(orders["order_status"] == "filled") & (orders["action"].isin(["exit", "stop"])) & (pd.to_numeric(orders["realized_return"], errors="coerce") < 0.0)].copy()
    rows: list[dict[str, Any]] = []
    for _idx, exit_row in exits.iterrows():
        position_id = str(exit_row.get("position_id"))
        history = positions[positions["position_id"].astype(str) == position_id].sort_values("ymd", kind="stable").copy()
        if history.empty:
            continue
        history["return_from_cost_basis"] = pd.to_numeric(history["unrealized_pnl"], errors="coerce") / pd.to_numeric(history["cost_basis"], errors="coerce")
        min_idx = int(history["return_from_cost_basis"].idxmin())
        mae_row = history.loc[min_idx]
        stop_hits = history[history["return_from_cost_basis"] <= STOP_LOSS_THRESHOLD]
        first_stop_ymd = None if stop_hits.empty else int(stop_hits.iloc[0]["ymd"])
        exit_ymd = int(exit_row["execution_ymd"])
        stop_delay_sessions = None if first_stop_ymd is None else int((history[history["ymd"] > first_stop_ymd]["ymd"] <= exit_ymd).sum())
        rows.append(
            {
                "position_id": position_id,
                "code": str(exit_row.get("code")),
                "exit_action": str(exit_row.get("action")),
                "exit_reason_type": str(exit_row.get("reason_type")),
                "entry_ymd": int(history.iloc[0]["entry_ymd"]),
                "exit_ymd": exit_ymd,
                "holding_days_at_exit": int(history["holding_days"].max()),
                "realized_return": float(exit_row["realized_return"]),
                "realized_pnl": float(exit_row["realized_pnl"]),
                "mae_return_from_cost_basis": float(mae_row["return_from_cost_basis"]),
                "mae_ymd": int(mae_row["ymd"]),
                "first_stop_threshold_ymd": first_stop_ymd,
                "stop_delay_sessions": stop_delay_sessions,
                "earlier_stop_might_help": bool(first_stop_ymd is not None and str(exit_row.get("reason_type")) != "stop_loss"),
            }
        )
    return pd.DataFrame(rows).sort_values(["realized_return", "mae_return_from_cost_basis"], ascending=[True, True], kind="stable") if rows else pd.DataFrame()


def build_cost_drag_summary(orders: pd.DataFrame, trade_contribution: pd.DataFrame, failure: dict[str, Any]) -> dict[str, Any]:
    filled = orders[orders["order_status"] == "filled"] if not orders.empty else pd.DataFrame()
    total_cost = float(pd.to_numeric(filled.get("cost_amount", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()) if not filled.empty else 0.0
    gross_profit = float(pd.to_numeric(trade_contribution.get("gross_pnl_before_cost", pd.Series(dtype=float)), errors="coerce").clip(lower=0).sum()) if not trade_contribution.empty else 0.0
    net_profit = float(failure.get("metrics", {}).get("final_equity", 0.0) - failure.get("metrics", {}).get("initial_cash", 0.0))
    order_count_by_action = filled["action"].value_counts().to_dict() if not filled.empty else {}
    churn = trade_contribution[
        trade_contribution["net_contribution"].abs() <= trade_contribution["total_cost"].clip(lower=1.0)
    ].copy() if not trade_contribution.empty else pd.DataFrame()
    return {
        "schema_version": f"{SCHEMA_PREFIX}_cost_drag_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "total_cost": total_cost,
        "net_profit": net_profit,
        "gross_profit_positive_only": gross_profit,
        "cost_as_pct_of_gross_profit": total_cost / gross_profit if gross_profit else None,
        "cost_as_pct_of_net_profit": total_cost / net_profit if net_profit else None,
        "order_count_by_action": {str(k): int(v) for k, v in order_count_by_action.items()},
        "unnecessary_churn_candidate_count": int(len(churn)),
        "unnecessary_churn_candidate_codes": churn.sort_values("total_cost", ascending=False)["code"].head(20).astype(str).tolist() if not churn.empty else [],
    }


def choose_next_axis(
    *,
    missed: pd.DataFrame,
    weak: pd.DataFrame,
    held: pd.DataFrame,
    cost_summary: dict[str, Any],
    trade_contribution: pd.DataFrame,
) -> dict[str, Any]:
    missed_gap = float(pd.to_numeric(missed.get("opportunity_gap_vs_selected", pd.Series(dtype=float)), errors="coerce").clip(lower=0).sum()) if not missed.empty else 0.0
    weak_gap = float(pd.to_numeric(weak.get("underperformance_vs_best_rejected", pd.Series(dtype=float)), errors="coerce").clip(lower=0).sum()) if not weak.empty else 0.0
    held_loss = abs(float(pd.to_numeric(held.get("realized_pnl", pd.Series(dtype=float)), errors="coerce").clip(upper=0).sum())) if not held.empty else 0.0
    cost_drag = float(cost_summary.get("total_cost") or 0.0)
    top3_winners_ratio = 0.0
    if not trade_contribution.empty:
        positives = trade_contribution[trade_contribution["net_contribution"] > 0].sort_values("net_contribution", ascending=False)
        total_positive = float(positives["net_contribution"].sum())
        top3_winners_ratio = float(positives.head(3)["net_contribution"].sum() / total_positive) if total_positive else 0.0
    axis_scores = {
        "missed_winner": {"raw_scale": missed_gap, "win_path_risk": "medium_high", "single_axis_testability": "medium"},
        "bought_weak_candidate": {"raw_scale": weak_gap, "win_path_risk": "low", "single_axis_testability": "high"},
        "held_loser_too_long": {"raw_scale": held_loss, "win_path_risk": "medium", "single_axis_testability": "high"},
        "cost_drag": {"raw_scale": cost_drag, "win_path_risk": "medium", "single_axis_testability": "medium"},
    }
    selected = "bought_weak_candidate"
    reason = "default_guardrail_axis: reduce bad buys before expanding recall"
    if weak_gap <= 0.0 and held_loss > 0.0:
        selected = "held_loser_too_long"
        reason = "no material bought_weak_candidate gap; realized loser damage is larger"
    elif weak_gap <= 0.0 and missed_gap > 0.0:
        selected = "missed_winner"
        reason = "no material weak-buy gap; missed winners dominate"
    elif cost_drag > max(held_loss, weak_gap * 1_000_000.0) and cost_drag > 0:
        selected = "cost_drag"
        reason = "transaction costs dominate observable weak-buy and loser damage"
    return {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "candidate_axes": list(NEXT_AXIS_CANDIDATES),
        "selected_next_axis": selected,
        "reason": reason,
        "axis_scores": axis_scores,
        "selection_criteria": {
            "improvement_headroom": "large enough in current run",
            "protect_existing_win_path": True,
            "single_axis_verifiability": True,
            "next_replay_diff_comparable": True,
        },
        "diagnostic_flags": {
            "top3_winners_ratio_of_positive_contribution": top3_winners_ratio,
            "initial_user_recommended_axis": "bought_weak_candidate",
        },
        "explicit_non_scope": [
            "no replay condition changes",
            "no sell rule changes in this diagnosis",
            "no rerun",
            "no MeeMee reflection",
        ],
    }


def _restore_replay_artifact_complete(root: Path, source_complete: dict[str, Any]) -> None:
    if source_complete.get("axis_id") == "portfolio_agent_replay_v1" and source_complete.get("required_artifacts_all_present") is True:
        _write_json(root / "_ARTIFACT_COMPLETE.json", source_complete)


def run_portfolio_agent_replay_diagnosis_v1(*, run_root: str | Path, output_dir: str | Path | None = None) -> dict[str, Any]:
    root = Path(run_root).expanduser().resolve()
    source_status = _require_sources(root)
    missing = [name for name, exists in source_status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing source artifacts at {root}: {missing}")
    out = Path(output_dir).expanduser().resolve() if output_dir else root / "diagnosis_v1"
    out.mkdir(parents=True, exist_ok=True)

    run_config = _read_json(root / "run_config.json")
    failure = _read_json(root / "failure_diagnosis_summary.json")
    complete_source = _read_json(root / "_ARTIFACT_COMPLETE.json") if (root / "_ARTIFACT_COMPLETE.json").exists() else {}
    no_lookahead = _read_json(root / "no_lookahead_audit.json")
    selection_manifest = _read_json(root / "selection_feature_manifest.json")
    equity = _load_frame(root, "equity_curve.csv")
    orders = _load_frame(root, "orders_ledger.csv")
    positions = _load_frame(root, "positions_ledger.csv")
    candidates = _load_frame(root, "daily_candidate_snapshot.csv")
    rejected = _load_frame(root, "rejected_candidates.csv")
    labels = _load_frame(root, "post_run_outcome_labels.csv")

    trade_contribution = build_trade_contribution(orders, positions, equity)
    top_winners_losers = build_top_winners_losers(trade_contribution)
    monthly, equity_diag = build_monthly_equity_summary(equity)
    missed = build_missed_winner_cases(rejected, labels, candidates)
    weak = build_bought_weak_candidate_cases(labels, candidates)
    held = build_held_loser_cases(orders, positions)
    cost_summary = build_cost_drag_summary(orders, trade_contribution, failure)
    next_axis = choose_next_axis(missed=missed, weak=weak, held=held, cost_summary=cost_summary, trade_contribution=trade_contribution)

    initial_cash = float(run_config.get("portfolio", {}).get("initial_cash_jpy") or failure.get("metrics", {}).get("initial_cash") or 0.0)
    final_equity = float(failure.get("metrics", {}).get("final_equity") or equity.iloc[-1]["equity"])
    portfolio_return = final_equity / initial_cash - 1.0 if initial_cash else None
    benchmark_status = str(failure.get("benchmark", {}).get("benchmark_status") or equity.iloc[-1].get("benchmark_status"))
    benchmark_equity = None
    if not equity.empty and "market_benchmark_equity" in equity.columns:
        benchmark_values = pd.to_numeric(equity["market_benchmark_equity"], errors="coerce").dropna()
        if not benchmark_values.empty:
            benchmark_equity = float(benchmark_values.iloc[-1])
    benchmark_return = (benchmark_equity / initial_cash - 1.0) if benchmark_equity and initial_cash else None
    positives = trade_contribution[trade_contribution["net_contribution"] > 0] if not trade_contribution.empty else pd.DataFrame()
    negatives = trade_contribution[trade_contribution["net_contribution"] < 0] if not trade_contribution.empty else pd.DataFrame()
    positive_total = float(positives["net_contribution"].sum()) if not positives.empty else 0.0
    negative_total = abs(float(negatives["net_contribution"].sum())) if not negatives.empty else 0.0
    top3_winner_ratio = float(positives.sort_values("net_contribution", ascending=False).head(3)["net_contribution"].sum() / positive_total) if positive_total else None
    top3_loser_ratio = float(abs(negatives.sort_values("net_contribution", ascending=True).head(3)["net_contribution"].sum()) / negative_total) if negative_total else None

    diagnosis = {
        "schema_version": f"{SCHEMA_PREFIX}_replay_diagnosis_summary_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_run_root": str(root),
        "source_gate_status": {
            "required_artifacts_all_present": bool(complete_source.get("required_artifacts_all_present")),
            "source_complete": bool(complete_source.get("complete")),
            "no_lookahead_audit": no_lookahead.get("audit_result"),
            "selection_feature_manifest": selection_manifest.get("audit_result"),
            "accounting_reconciliation": (complete_source.get("accounting_reconciliation") or {}).get("status"),
            "rerun_performed": False,
            "conditions_changed": False,
            "silent_fallback_used": False,
        },
        "benchmark_comparison": {
            "portfolio_return": portfolio_return,
            "benchmark_status": benchmark_status,
            "benchmark_code": failure.get("benchmark", {}).get("benchmark_code"),
            "benchmark_return": benchmark_return,
            "excess_return": None if benchmark_return is None or portfolio_return is None else portfolio_return - benchmark_return,
        },
        "contribution_summary": {
            "symbol_count": int(len(trade_contribution)),
            "largest_winner": trade_contribution.sort_values("net_contribution", ascending=False).head(1).to_dict("records")[0] if not trade_contribution.empty else None,
            "largest_loser": trade_contribution.sort_values("net_contribution", ascending=True).head(1).to_dict("records")[0] if not trade_contribution.empty else None,
            "top3_winners_contribution_ratio": top3_winner_ratio,
            "top3_losers_damage_ratio": top3_loser_ratio,
        },
        "equity_curve_diagnosis": equity_diag,
        "case_counts": {
            "missed_winner_cases": int(len(missed)),
            "bought_weak_candidate_cases": int(len(weak)),
            "held_loser_too_long_cases": int(len(held)),
        },
        "next_axis": next_axis["selected_next_axis"],
        "next_axis_reason": next_axis["reason"],
    }

    paths: dict[str, str] = {}
    paths["replay_diagnosis_summary.json"] = str(_write_json(out / "replay_diagnosis_summary.json", diagnosis))
    paths["trade_contribution.csv"] = str(_write_csv(out / "trade_contribution.csv", trade_contribution))
    paths["monthly_equity_summary.csv"] = str(_write_csv(out / "monthly_equity_summary.csv", monthly))
    paths["top_winners_losers.csv"] = str(_write_csv(out / "top_winners_losers.csv", top_winners_losers))
    paths["missed_winner_cases.csv"] = str(_write_csv(out / "missed_winner_cases.csv", missed))
    paths["bought_weak_candidate_cases.csv"] = str(_write_csv(out / "bought_weak_candidate_cases.csv", weak))
    paths["held_loser_too_long_cases.csv"] = str(_write_csv(out / "held_loser_too_long_cases.csv", held))
    paths["cost_drag_summary.json"] = str(_write_json(out / "cost_drag_summary.json", cost_summary))
    paths["next_axis_decision.json"] = str(_write_json(out / "next_axis_decision.json", next_axis))
    existing_outputs = {name: (out / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"}
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "generated_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_run_root": str(root),
        "artifact_root": str(out),
        "source_artifacts_present": source_status,
        "output_artifacts_present": existing_outputs,
        "complete": all(source_status.values()) and all(existing_outputs.values()),
        "rerun_performed": False,
        "conditions_changed": False,
        "next_axis_selected_count": 1 if next_axis["selected_next_axis"] in NEXT_AXIS_CANDIDATES else 0,
        "selected_next_axis": next_axis["selected_next_axis"],
        "silent_fallback_used": False,
    }
    paths["_ARTIFACT_COMPLETE.json"] = str(_write_json(out / "_ARTIFACT_COMPLETE.json", complete))
    _restore_replay_artifact_complete(root, complete_source)
    return {
        "run_root": str(root),
        "output_dir": str(out),
        "paths": paths,
        "complete": complete["complete"],
        "portfolio_return": portfolio_return,
        "benchmark_return": benchmark_return,
        "excess_return": diagnosis["benchmark_comparison"]["excess_return"],
        "selected_next_axis": next_axis["selected_next_axis"],
        "case_counts": diagnosis["case_counts"],
        "silent_fallback_used": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args(argv)
    result = run_portfolio_agent_replay_diagnosis_v1(run_root=args.run_root, output_dir=args.output_dir.strip() or None)
    print(_json_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
