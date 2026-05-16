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


AXIS_ID = "stop_case_reconciliation_v1"
SCHEMA_PREFIX = "tradex_stop_case_reconciliation_v1"
DEFAULT_OUTPUT_DIR_NAME = "stop_case_reconciliation_v1"

SOURCE_ARTIFACTS = (
    "stop_too_wide_pretest_v1/stop_too_wide_pretest_summary.json",
    "stop_too_wide_pretest_v1/baseline_vs_stop_comparison.json",
    "stop_too_wide_pretest_v1/next_axis_decision.json",
    "stop_too_wide_pretest_v1/stop_triggered_cases.csv",
    "stop_too_wide_pretest_v1/stop_overlay_orders_ledger.csv",
    "stop_too_wide_pretest_v1/stop_overlay_positions_ledger.csv",
    "stop_too_wide_pretest_v1/stop_overlay_equity_curve.csv",
    "stop_too_wide_pretest_v1/saved_loss_cases.csv",
    "stop_too_wide_pretest_v1/false_stop_recovery_cases.csv",
    "stop_too_wide_pretest_v1/missed_profit_after_stop_cases.csv",
    "candidate_lifecycle_audit_v1/candidate_lifecycle_summary.json",
    "diagnosis_v1",
    "trade_reflection_audit_v1",
    "orders_ledger.csv",
    "positions_ledger.csv",
    "equity_curve.csv",
)

OUTPUT_ARTIFACTS = (
    "stop_case_reconciliation_summary.json",
    "stop_case_path_comparison.csv",
    "stop_direct_vs_portfolio_impact.csv",
    "freed_cash_redeployment_cases.csv",
    "false_stop_recovery_diagnosis.csv",
    "true_saved_loss_cases.csv",
    "stop_damage_cases.csv",
    "stop_drawdown_relief_cases.csv",
    "equity_delta_attribution.json",
    "next_axis_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

DECISIONS = (
    "keep_for_replay_challenger",
    "hold_for_partial_stop_design",
    "hold_for_position_sizing_haircut",
    "drop_due_to_false_stop_recovery",
    "drop_due_to_unexplained_attribution",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
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


def _write_csv(path: Path, rows: Iterable[dict[str, Any]] | pd.DataFrame, columns: list[str] | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if columns is not None:
        if frame.empty:
            frame = pd.DataFrame(columns=columns)
        else:
            for column in columns:
                if column not in frame.columns:
                    frame[column] = None
    frame.to_csv(path, index=False)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing required artifact: {path}")
    return pd.read_csv(path)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _source_status(run_root: Path) -> dict[str, bool]:
    return {name: (run_root / name).exists() for name in SOURCE_ARTIFACTS}


def _position_contribution(orders: pd.DataFrame, positions: pd.DataFrame) -> pd.DataFrame:
    buys = orders[(orders["action"] == "buy") & (orders["order_status"] == "filled")].copy()
    exits = orders[(orders["action"].isin(["exit", "stop"])) & (orders["order_status"] == "filled")].copy()
    latest = positions.sort_values("ymd", kind="stable").groupby("position_id", sort=False).tail(1) if not positions.empty else pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _idx, buy in buys.iterrows():
        pid = str(buy.get("position_id"))
        baseline_pid = str(buy.get("baseline_position_id")) if "baseline_position_id" in buy and not pd.isna(buy.get("baseline_position_id")) else pid
        exit_rows = exits[exits["position_id"].astype(str) == pid]
        if not exit_rows.empty:
            exit_row = exit_rows.iloc[-1]
            pnl = _safe_float(exit_row.get("realized_pnl")) or 0.0
            exit_ymd = int(exit_row["execution_ymd"])
            exit_reason = exit_row.get("reason_type")
            status = "closed"
        else:
            pos = latest[latest["position_id"].astype(str) == pid]
            pnl = (_safe_float(pos.iloc[-1].get("unrealized_pnl")) or 0.0) if not pos.empty else 0.0
            exit_ymd = None
            exit_reason = "open_at_run_end"
            status = "open"
        rows.append(
            {
                "position_id": pid,
                "baseline_position_id": baseline_pid,
                "code": str(buy.get("code")),
                "entry_decision_ymd": int(buy.get("decision_ymd")),
                "entry_ymd": int(buy.get("execution_ymd")),
                "entry_price": buy.get("execution_price"),
                "shares": buy.get("shares"),
                "buy_notional": buy.get("notional"),
                "buy_cost": buy.get("cost_amount"),
                "exit_ymd": exit_ymd,
                "exit_reason": exit_reason,
                "status": status,
                "net_contribution": pnl,
            }
        )
    return pd.DataFrame(rows)


def _drawdown_series(equity: pd.Series) -> pd.Series:
    peak = equity.cummax()
    return equity / peak - 1.0


def build_stop_case_path_comparison(stops: pd.DataFrame, baseline_contrib: pd.DataFrame) -> pd.DataFrame:
    if stops.empty:
        return pd.DataFrame()
    base = baseline_contrib.rename(
        columns={
            "baseline_position_id": "baseline_position_id",
            "entry_ymd": "entry_date",
            "net_contribution": "baseline_hold_pnl",
        }
    )
    out = stops.merge(
        base[["baseline_position_id", "entry_date", "baseline_hold_pnl", "exit_ymd", "exit_reason"]],
        on="baseline_position_id",
        how="left",
    )
    out = out.rename(
        columns={
            "stop_realized_pnl": "stop_exit_pnl",
            "baseline_exit_ymd": "baseline_exit_date",
            "delta_vs_baseline": "direct_delta_pnl",
            "post_stop_max_up20": "max_recovery_after_stop",
        }
    )
    keep = [
        "code",
        "baseline_position_id",
        "entry_date",
        "stop_trigger_date",
        "stop_exit_date",
        "baseline_exit_date",
        "stop_exit_pnl",
        "baseline_hold_pnl",
        "direct_delta_pnl",
        "post_stop_ret5",
        "post_stop_ret10",
        "post_stop_ret20",
        "max_recovery_after_stop",
        "recovery_type",
    ]
    return out[[column for column in keep if column in out.columns]].sort_values("direct_delta_pnl", ascending=False, kind="stable")


def build_direct_vs_portfolio_impact(
    baseline_contrib: pd.DataFrame,
    overlay_contrib: pd.DataFrame,
    stops: pd.DataFrame,
) -> pd.DataFrame:
    stop_pids = set(stops.get("baseline_position_id", pd.Series(dtype=str)).astype(str))
    base = baseline_contrib.groupby("baseline_position_id", as_index=False).agg(
        code=("code", "last"),
        baseline_entry_ymd=("entry_ymd", "min"),
        baseline_exit_ymd=("exit_ymd", "last"),
        baseline_shares=("shares", "sum"),
        baseline_contribution=("net_contribution", "sum"),
    )
    over = overlay_contrib.groupby("baseline_position_id", as_index=False).agg(
        overlay_entry_ymd=("entry_ymd", "min"),
        overlay_exit_ymd=("exit_ymd", "last"),
        overlay_shares=("shares", "sum"),
        overlay_contribution=("net_contribution", "sum"),
    )
    merged = base.merge(over, on="baseline_position_id", how="outer")
    merged["baseline_contribution"] = pd.to_numeric(merged["baseline_contribution"], errors="coerce").fillna(0.0)
    merged["overlay_contribution"] = pd.to_numeric(merged["overlay_contribution"], errors="coerce").fillna(0.0)
    merged["delta_contribution"] = merged["overlay_contribution"] - merged["baseline_contribution"]
    merged["is_stop_triggered_case"] = merged["baseline_position_id"].astype(str).isin(stop_pids)
    merged["impact_component"] = merged["is_stop_triggered_case"].map({True: "direct_stop_pnl_delta", False: "portfolio_path_delta"})
    merged["path_effect_type"] = "same_baseline_position_changed"
    merged.loc[merged["overlay_entry_ymd"].isna(), "path_effect_type"] = "baseline_position_not_taken_in_overlay"
    merged.loc[merged["baseline_entry_ymd"].isna(), "path_effect_type"] = "overlay_position_not_in_baseline"
    merged.loc[(merged["overlay_entry_ymd"].notna()) & (merged["baseline_entry_ymd"].notna()) & (merged["overlay_shares"] != merged["baseline_shares"]), "path_effect_type"] = "position_size_changed"
    return merged.sort_values("delta_contribution", ascending=False, kind="stable")


def build_freed_cash_cases(impact: pd.DataFrame, stops: pd.DataFrame) -> pd.DataFrame:
    if impact.empty:
        return pd.DataFrame()
    stop_dates = sorted(pd.to_numeric(stops.get("stop_exit_date", pd.Series(dtype=float)), errors="coerce").dropna().astype(int).tolist())
    rows: list[dict[str, Any]] = []
    for row in impact[~impact["is_stop_triggered_case"]].to_dict("records"):
        entry_ymd = row.get("overlay_entry_ymd") if not pd.isna(row.get("overlay_entry_ymd")) else row.get("baseline_entry_ymd")
        prior_stops = [ymd for ymd in stop_dates if entry_ymd is not None and not pd.isna(entry_ymd) and ymd <= int(entry_ymd)]
        if not prior_stops and float(row.get("delta_contribution") or 0.0) <= 0:
            continue
        effect = str(row.get("path_effect_type"))
        if effect == "baseline_position_not_taken_in_overlay":
            effect = "slot_or_exposure_path_avoided_baseline_position"
        elif effect == "position_size_changed":
            effect = "freed_cash_or_cash_path_changed_position_size"
        elif effect == "overlay_position_not_in_baseline":
            effect = "freed_cash_enabled_overlay_only_buy"
        rows.append(
            {
                "freed_cash_date": prior_stops[-1] if prior_stops else None,
                "replacement_or_next_buy_code": row.get("code"),
                "baseline_position_id": row.get("baseline_position_id"),
                "replacement_pnl": row.get("overlay_contribution"),
                "baseline_pnl": row.get("baseline_contribution"),
                "baseline_would_not_have_bought_flag": bool(row.get("path_effect_type") == "overlay_position_not_in_baseline"),
                "redeployment_gain_estimate": row.get("delta_contribution"),
                "effect_type": effect,
            }
        )
    return pd.DataFrame(rows).sort_values("redeployment_gain_estimate", ascending=False, kind="stable") if rows else pd.DataFrame()


def build_false_stop_recovery_diagnosis(false_stop: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in false_stop.to_dict("records"):
        max_up = _safe_float(row.get("post_stop_max_up20")) or 0.0
        delta = _safe_float(row.get("delta_vs_baseline")) or 0.0
        ret20 = _safe_float(row.get("post_stop_ret20"))
        if delta > 0:
            classification = "stop_was_still_portfolio_positive_due_to_redeployment"
        elif max_up >= 0.08:
            classification = "recovered_strongly_after_stop"
        elif ret20 is not None and ret20 <= 0:
            classification = "recovered_but_not_profitable"
        else:
            classification = "stop_too_tight_candidate"
        rows.append({**row, "false_stop_classification": classification})
    return pd.DataFrame(rows)


def build_true_saved_loss_cases(saved: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in saved.to_dict("records"):
        recovery_type = str(row.get("recovery_type"))
        post20 = _safe_float(row.get("post_stop_ret20"))
        baseline_reason = str(row.get("baseline_exit_reason"))
        if recovery_type == "no_recovery" or (post20 is not None and post20 < -0.05):
            classification = "continued_downtrend_saved"
        elif "stop" in baseline_reason:
            classification = "baseline_exit_too_late_saved"
        elif "gap" in str(row.get("candidate_snapshot_status")).lower():
            classification = "gap_risk_saved"
        else:
            classification = "true_breakdown_saved"
        rows.append({**row, "saved_loss_classification": classification})
    return pd.DataFrame(rows)


def build_stop_damage_cases(missed: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in missed.to_dict("records"):
        max_up = _safe_float(row.get("post_stop_max_up20")) or 0.0
        if max_up >= 0.08:
            classification = "large_shakeout_profit_cut"
        elif max_up >= 0.03:
            classification = "moderate_recovery_profit_cut"
        else:
            classification = "direct_pnl_damage_without_large_recovery"
        rows.append({**row, "stop_damage_classification": classification, "missed_profit": -float(row.get("delta_vs_baseline") or 0.0)})
    return pd.DataFrame(rows)


def build_drawdown_relief(baseline_equity: pd.DataFrame, overlay_equity: pd.DataFrame) -> pd.DataFrame:
    merged = baseline_equity[["ymd", "equity"]].rename(columns={"equity": "baseline_equity"}).merge(
        overlay_equity[["ymd", "equity"]].rename(columns={"equity": "stop_equity"}),
        on="ymd",
        how="inner",
    )
    merged["baseline_drawdown"] = _drawdown_series(pd.to_numeric(merged["baseline_equity"], errors="coerce"))
    merged["stop_drawdown"] = _drawdown_series(pd.to_numeric(merged["stop_equity"], errors="coerce"))
    merged["drawdown_relief"] = merged["stop_drawdown"] - merged["baseline_drawdown"]
    merged["equity_delta"] = merged["stop_equity"] - merged["baseline_equity"]
    return merged[merged["drawdown_relief"] > 0.03].sort_values("drawdown_relief", ascending=False, kind="stable")


def build_attribution(
    comparison_metrics: dict[str, Any],
    impact: pd.DataFrame,
    stops: pd.DataFrame,
) -> dict[str, Any]:
    total_delta = float(comparison_metrics.get("delta_final_equity") or 0.0)
    stop_pids = set(stops.get("baseline_position_id", pd.Series(dtype=str)).astype(str))
    direct = float(impact[impact["baseline_position_id"].astype(str).isin(stop_pids)]["delta_contribution"].sum()) if not impact.empty else 0.0
    non_stop = impact[~impact["baseline_position_id"].astype(str).isin(stop_pids)].copy() if not impact.empty else pd.DataFrame()
    freed_mask = non_stop["path_effect_type"].isin(["position_size_changed", "overlay_position_not_in_baseline"]) if not non_stop.empty else pd.Series(dtype=bool)
    freed = float(non_stop.loc[freed_mask, "delta_contribution"].sum()) if not non_stop.empty else 0.0
    exposure = float(non_stop.loc[~freed_mask, "delta_contribution"].sum()) if not non_stop.empty else 0.0
    residual = total_delta - direct - freed - exposure
    cost_delta = float(comparison_metrics.get("cost_delta") or 0.0)
    explained = direct + freed + exposure
    return {
        "schema_version": f"{SCHEMA_PREFIX}_equity_delta_attribution_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "baseline_final_equity": comparison_metrics.get("baseline_final_equity"),
        "stop_final_equity": comparison_metrics.get("stop_final_equity"),
        "delta_final_equity": total_delta,
        "direct_stop_pnl_delta": direct,
        "freed_cash_redeployment_delta": freed,
        "cost_delta": cost_delta,
        "cost_delta_interpretation": "overlay_minus_baseline; net contribution components already include trade costs",
        "exposure_delta": exposure,
        "residual_unexplained_delta": residual,
        "explained_delta": explained,
        "explained_ratio": None if total_delta == 0 else explained / total_delta,
        "residual_abs_ratio": None if total_delta == 0 else abs(residual) / abs(total_delta),
        "dominant_component": max(
            {
                "direct_stop_pnl_delta": abs(direct),
                "freed_cash_redeployment_delta": abs(freed),
                "exposure_delta": abs(exposure),
                "residual_unexplained_delta": abs(residual),
            }.items(),
            key=lambda item: item[1],
        )[0],
        "notes": [
            "direct, freed_cash_redeployment, and exposure are net-of-cost position contribution components",
            "cost_delta is provided as a required diagnostic but is not added again to avoid double counting",
        ],
    }


def _decision(attribution: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, str]:
    residual_ratio = float(attribution.get("residual_abs_ratio") or 0.0)
    false_count = int(metrics.get("false_stop_recovery_count") or 0)
    stop_count = max(1, int(metrics.get("stop_trigger_count") or 0))
    delta = float(metrics.get("delta_final_equity") or 0.0)
    dd_delta = float(metrics.get("delta_max_drawdown") or 0.0)
    direct = float(attribution.get("direct_stop_pnl_delta") or 0.0)
    exposure = float(attribution.get("exposure_delta") or 0.0)
    if residual_ratio > 0.10:
        return "drop_due_to_unexplained_attribution", "residual_unexplained_delta_too_large"
    if delta > 0 and dd_delta > 0 and direct > 0 and false_count <= stop_count // 2:
        return "keep_for_replay_challenger", "equity_drawdown_and_direct_stop_effect_clean"
    if false_count / stop_count >= 0.5 and delta <= 0:
        return "drop_due_to_false_stop_recovery", "false_stop_recovery_high_and_no_equity_gain"
    if delta > 0 and dd_delta > 0 and false_count / stop_count >= 0.5:
        return "hold_for_partial_stop_design", "portfolio_improved_but_false_stop_recovery_high"
    if exposure > abs(direct) and delta > 0:
        return "hold_for_position_sizing_haircut", "portfolio_gain_dominated_by_exposure_path_not_direct_stop"
    return "hold_for_position_sizing_haircut", "mixed_attribution_requires_risk_sizing_review"


def run_stop_case_reconciliation_v1(run_root: str | Path, output_root: str | Path | None = None) -> dict[str, Any]:
    run_root = Path(run_root)
    output_root = Path(output_root) if output_root is not None else run_root / DEFAULT_OUTPUT_DIR_NAME
    status = _source_status(run_root)
    missing = [name for name, exists in status.items() if not exists]
    if missing:
        raise FileNotFoundError(f"missing required source artifacts: {missing}")

    stop_root = run_root / "stop_too_wide_pretest_v1"
    comparison = _read_json(stop_root / "baseline_vs_stop_comparison.json")
    stop_summary = _read_json(stop_root / "stop_too_wide_pretest_summary.json")
    lifecycle = _read_json(run_root / "candidate_lifecycle_audit_v1" / "candidate_lifecycle_summary.json")
    stops = _load_frame(stop_root / "stop_triggered_cases.csv")
    overlay_orders = _load_frame(stop_root / "stop_overlay_orders_ledger.csv")
    overlay_positions = _load_frame(stop_root / "stop_overlay_positions_ledger.csv")
    overlay_equity = _load_frame(stop_root / "stop_overlay_equity_curve.csv")
    saved = _load_frame(stop_root / "saved_loss_cases.csv")
    false_stop = _load_frame(stop_root / "false_stop_recovery_cases.csv")
    missed = _load_frame(stop_root / "missed_profit_after_stop_cases.csv")
    baseline_orders = _load_frame(run_root / "orders_ledger.csv")
    baseline_positions = _load_frame(run_root / "positions_ledger.csv")
    baseline_equity = _load_frame(run_root / "equity_curve.csv")

    baseline_contrib = _position_contribution(baseline_orders, baseline_positions)
    overlay_contrib = _position_contribution(overlay_orders, overlay_positions)
    path_comparison = build_stop_case_path_comparison(stops, baseline_contrib)
    impact = build_direct_vs_portfolio_impact(baseline_contrib, overlay_contrib, stops)
    freed = build_freed_cash_cases(impact, stops)
    false_diag = build_false_stop_recovery_diagnosis(false_stop)
    true_saved = build_true_saved_loss_cases(saved)
    damage = build_stop_damage_cases(missed)
    drawdown = build_drawdown_relief(baseline_equity, overlay_equity)
    metrics = comparison.get("metrics", {})
    attribution = build_attribution(metrics, impact, stops)
    decision, reason = _decision(attribution, metrics)

    _write_csv(output_root / "stop_case_path_comparison.csv", path_comparison)
    _write_csv(output_root / "stop_direct_vs_portfolio_impact.csv", impact)
    _write_csv(output_root / "freed_cash_redeployment_cases.csv", freed, columns=["freed_cash_date", "replacement_or_next_buy_code", "baseline_position_id", "replacement_pnl", "baseline_pnl", "baseline_would_not_have_bought_flag", "redeployment_gain_estimate", "effect_type"])
    _write_csv(output_root / "false_stop_recovery_diagnosis.csv", false_diag)
    _write_csv(output_root / "true_saved_loss_cases.csv", true_saved)
    _write_csv(output_root / "stop_damage_cases.csv", damage)
    _write_csv(output_root / "stop_drawdown_relief_cases.csv", drawdown)
    _write_json(output_root / "equity_delta_attribution.json", attribution)

    next_axis = {
        "schema_version": f"{SCHEMA_PREFIX}_next_axis_decision_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "decision_candidates": list(DECISIONS),
        "decision": decision,
        "decision_count": 1,
        "reason_type": reason,
        "metrics": {
            **metrics,
            "direct_stop_pnl_delta": attribution["direct_stop_pnl_delta"],
            "freed_cash_redeployment_delta": attribution["freed_cash_redeployment_delta"],
            "exposure_delta": attribution["exposure_delta"],
            "residual_unexplained_delta": attribution["residual_unexplained_delta"],
            "residual_abs_ratio": attribution["residual_abs_ratio"],
        },
        "policy": {
            "replay_rerun": False,
            "stop_threshold_changed": False,
            "threshold_sweep": False,
            "policy_promotion": False,
            "single_axis_only": True,
        },
    }
    _write_json(output_root / "next_axis_decision.json", next_axis)

    summary = {
        "schema_version": f"{SCHEMA_PREFIX}_summary_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "source_artifacts": status,
        "scope": {
            "tradex_only": True,
            "derived_artifacts_only": True,
            "replay_rerun": False,
            "stop_threshold_changed": False,
            "threshold_sweep": False,
            "policy_promotion": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "ranking_changed": False,
            "publish_registry_changed": False,
        },
        "input_decisions": {
            "stop_too_wide_decision": stop_summary.get("decision"),
            "stop_too_wide_reason": stop_summary.get("decision_reason_type"),
            "candidate_lifecycle_selected_next_axis": lifecycle.get("selected_next_axis"),
        },
        "metrics": {
            **metrics,
            "stop_case_path_rows": int(len(path_comparison)),
            "portfolio_impact_rows": int(len(impact)),
            "freed_cash_redeployment_rows": int(len(freed)),
            "false_stop_recovery_diagnosis_rows": int(len(false_diag)),
            "true_saved_loss_rows": int(len(true_saved)),
            "stop_damage_rows": int(len(damage)),
            "drawdown_relief_rows": int(len(drawdown)),
        },
        "attribution": attribution,
        "decision": decision,
        "decision_reason_type": reason,
    }
    _write_json(output_root / "stop_case_reconciliation_summary.json", summary)
    complete = {
        "schema_version": f"{SCHEMA_PREFIX}_artifact_complete_v1",
        "axis_id": AXIS_ID,
        "generated_at": _utc_now(),
        "run_root": str(run_root),
        "output_root": str(output_root),
        "complete": True,
        "required_artifacts_all_present": all((output_root / name).exists() for name in OUTPUT_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "source_artifacts_all_present": all(status.values()),
        "decision": decision,
        "decision_count": 1,
        "replay_rerun": False,
        "stop_threshold_changed": False,
        "threshold_sweep": False,
        "policy_promotion": False,
        "silent_fallback_used": False,
        "artifact_logs_non_empty": {
            "stop_case_path_comparison": len(path_comparison) > 0,
            "stop_direct_vs_portfolio_impact": len(impact) > 0,
            "freed_cash_redeployment_cases": len(freed) > 0,
            "false_stop_recovery_diagnosis": len(false_diag) > 0,
            "true_saved_loss_cases": len(true_saved) > 0,
            "stop_damage_cases": len(damage) > 0,
            "stop_drawdown_relief_cases": len(drawdown) > 0,
        },
    }
    _write_json(output_root / "_ARTIFACT_COMPLETE.json", complete)
    return {"complete": True, "output_root": str(output_root), "decision": decision, "attribution": attribution}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile stop case direct attribution against portfolio impact.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    print(_json_text(run_stop_case_reconciliation_v1(args.run_root, args.output_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
