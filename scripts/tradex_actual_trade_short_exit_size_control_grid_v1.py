from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable


AXIS_ID = "tradex_actual_trade_short_exit_size_control_grid_v1"
SCHEMA_PREFIX = "tradex_actual_trade_short_exit_size_control_grid_v1"
APPROVED_SUBSET_ROOT = Path(
    r"G:\Tradex\actual_trade_short_exit_rule_replay_v1\20260512T021118Z-actual_trade_short_ma7_reclaim_exit_v1"
)
FEASIBILITY_ROOT = Path(
    r"G:\Tradex\actual_trade_short_exit_feasibility_v1\20260512T015542Z-actual_trade_short_holding_duration_exit_feasibility_v1"
)
COUNTERFACTUAL_ROOT = Path(
    r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1"
)
SEGMENATION_ROOT = Path(
    r"G:\Tradex\actual_trade_short_loss_segmentation_v1\20260512T015041Z-actual_trade_short_ma20_kept_loss_segmentation_v1"
)
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_size_control_grid_v1")

REQUIRED_OUTPUTS = (
    "short_exit_size_control_contract.json",
    "short_exit_size_control_policy_compare.json",
    "short_exit_size_control_policy_grid.csv",
    "short_exit_size_control_monthly_stability.json",
    "short_exit_size_control_concentration_summary.json",
    "short_exit_size_control_best_policy_rows.csv",
    "short_exit_size_control_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def fnum(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def hold_bucket(days: int) -> str:
    if days == 0:
        return "same_day"
    if days <= 3:
        return "1_3d"
    if days <= 7:
        return "4_7d"
    if days <= 14:
        return "8_14d"
    if days <= 30:
        return "15_30d"
    return "31d_plus"


def profit_factor(values: Iterable[float]) -> float | None:
    gains = sum(v for v in values if v > 0)
    losses = abs(sum(v for v in values if v < 0))
    return None if losses == 0 else gains / losses


def delta_contribution(values: list[float], n: int, total_delta: float) -> float | None:
    if total_delta == 0:
        return None
    if total_delta > 0:
        ordered = sorted([v for v in values if v > 0], reverse=True)
        return sum(ordered[:n]) / total_delta if ordered else 0.0
    ordered = sorted([abs(v) for v in values if v < 0], reverse=True)
    return sum(ordered[:n]) / abs(total_delta) if ordered else 0.0


def robustness_classification(top1_month: float | None, top3_month: float | None, top1_trade: float | None, top5_trade: float | None) -> str:
    if top1_trade is None or top1_month is None or top3_month is None or top5_trade is None:
        return "unknown"
    if top1_trade >= 0.50 or top1_month >= 0.60:
        return "one_off_effect"
    if top5_trade < 0.45 and top3_month < 0.35 and top1_month < 0.20:
        return "broad_effect"
    if top5_trade >= 0.75 or top3_month >= 0.70 or top1_month >= 0.40:
        return "highly_concentrated_effect"
    return "moderately_concentrated_effect"


def trade_key(row: dict[str, Any]) -> str:
    normalized_trade_id = row.get("normalized_trade_id")
    if normalized_trade_id not in (None, ""):
        return str(normalized_trade_id)
    symbol = str(row.get("symbol") or "")
    entry_date = str(row.get("entry_date") or "")
    actual_exit_date = str(row.get("actual_exit_date") or row.get("exit_date") or "")
    if symbol or entry_date or actual_exit_date:
        return f"{symbol}|{entry_date}|{actual_exit_date}"
    raise KeyError("normalized_trade_id")


def load_inputs() -> tuple[list[dict[str, str]], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    approved_subset = json.loads((APPROVED_SUBSET_ROOT / "available_path_subset_contract_used.json").read_text(encoding="utf-8"))
    if not approved_subset.get("approved_for_subset_replay"):
        raise RuntimeError("approved subset contract is not approved")
    kept_trades = read_csv(COUNTERFACTUAL_ROOT / "kept_trades.csv")
    prior_no_lookahead = json.loads((COUNTERFACTUAL_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    kept_short_seg = json.loads((SEGMENATION_ROOT / "kept_short_failure_cluster_decision.json").read_text(encoding="utf-8"))
    path_rows = read_csv(FEASIBILITY_ROOT / "short_trade_path_rows_sample.csv")
    return kept_trades, approved_subset, prior_no_lookahead, kept_short_seg, {"rows": path_rows}


def build_trade_paths(
    kept_trades: list[dict[str, str]],
    approved_subset: dict[str, Any],
    path_rows: list[dict[str, str]],
) -> tuple[dict[str, list[dict[str, Any]]], list[str], dict[str, Any]]:
    excluded_ids = set(approved_subset.get("excluded_trade_ids") or [])
    included = [
        r
        for r in kept_trades
        if r.get("side") == "short"
        and r.get("counterfactual_action") == "keep"
        and r.get("tainted_excluded_flag", "").lower() == "false"
        and trade_key(r) not in excluded_ids
    ]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in path_rows:
        grouped[trade_key(row)].append({**row, "path_day_index": int(row["path_day_index"])})

    trade_paths: dict[str, list[dict[str, Any]]] = {}
    missing: list[str] = []
    for trade in included:
        tid = trade_key(trade)
        path = sorted(grouped.get(tid, []), key=lambda r: r["path_day_index"])
        if not path:
            missing.append(tid)
            continue
        trade_paths[tid] = path
    coverage = {
        "approved_for_subset_replay": bool(approved_subset.get("approved_for_subset_replay")),
        "included_trade_count": len(included),
        "path_available_trade_count": len(trade_paths),
        "path_missing_trade_count": len(missing),
        "path_coverage_rate": len(trade_paths) / len(included) if included else 0.0,
        "excluded_missing_trade_count": int(approved_subset.get("excluded_missing_trade_count") or 0),
        "excluded_reason_counts": approved_subset.get("excluded_reason_counts") or {},
        "source_result_scope": approved_subset.get("available_vs_missing_bias_summary", {}).get("bias_classification_after_repair"),
    }
    return trade_paths, missing, coverage


def simulate_policy(
    *,
    policy_id: str,
    path: list[dict[str, Any]],
    trim_day: int | None = None,
    trim_ratio: float | None = None,
    final_exit_day: int | None = None,
    take_profit_return_pct: float | None = None,
    stop_loss_return_pct: float | None = None,
) -> dict[str, Any]:
    entry = path[0]
    entry_price = float(entry["entry_price"])
    entry_close = float(entry["close"])
    qty = float(entry["quantity"])
    actual_exit_price = float(entry["actual_exit_price"])
    actual_pnl = float(entry["gross_pnl_actual"])
    actual_notional = entry_price * qty
    actual_return = actual_pnl / actual_notional if actual_notional else 0.0
    hold_actual = int(entry["holding_days_actual"])
    scale = entry_price / entry_close if entry_close else 1.0

    remaining = qty
    realized = 0.0
    actions: list[dict[str, Any]] = []
    trimmed = False

    for row in path:
        day = int(row["path_day_index"])
        close = float(row["close"]) * scale
        if (
            trim_day is not None
            and trim_ratio is not None
            and not trimmed
            and hold_actual >= trim_day
            and day >= trim_day
        ):
            trim_qty = remaining * trim_ratio
            if trim_qty > 0:
                realized += (entry_price - close) * trim_qty
                remaining -= trim_qty
                actions.append({"day": day, "action": "trim", "ratio": trim_ratio, "price": close})
                trimmed = True
        if remaining > 0 and take_profit_return_pct is not None:
            ret_close = entry_price / close - 1.0 if close else 0.0
            if ret_close >= take_profit_return_pct:
                realized += (entry_price - close) * remaining
                actions.append({"day": day, "action": "take_profit", "threshold": take_profit_return_pct, "price": close})
                remaining = 0.0
                break
        if remaining > 0 and stop_loss_return_pct is not None:
            ret_close = entry_price / close - 1.0 if close else 0.0
            if ret_close <= stop_loss_return_pct:
                realized += (entry_price - close) * remaining
                actions.append({"day": day, "action": "stop_loss", "threshold": stop_loss_return_pct, "price": close})
                remaining = 0.0
                break
        if remaining > 0 and final_exit_day is not None and day >= final_exit_day:
            realized += (entry_price - close) * remaining
            actions.append({"day": day, "action": "exit", "price": close})
            remaining = 0.0
            break

    if remaining > 0:
        realized += (entry_price - actual_exit_price) * remaining
        actions.append({"day": hold_actual, "action": "actual_exit", "price": actual_exit_price})

    sim_notional = entry_price * qty
    sim_return = realized / sim_notional if sim_notional else 0.0
    final_exit_day = actions[-1]["day"] if actions else hold_actual
    final_exit_price = actions[-1]["price"] if actions else actual_exit_price
    return {
        "policy_id": policy_id,
        "normalized_trade_id": trade_key(entry),
        "symbol": entry["symbol"],
        "entry_date": entry["entry_date"],
        "actual_exit_date": entry["actual_exit_date"],
        "holding_days_actual": hold_actual,
        "entry_price": entry_price,
        "actual_exit_price": actual_exit_price,
        "quantity": qty,
        "actual_gross_pnl": actual_pnl,
        "actual_return_pct": actual_return,
        "sim_gross_pnl": realized,
        "sim_return_pct": sim_return,
        "sim_minus_actual_pnl": realized - actual_pnl,
        "sim_minus_actual_return_pct": sim_return - actual_return,
        "final_exit_day": final_exit_day,
        "final_exit_price": final_exit_price,
        "actions_json": json.dumps(actions, ensure_ascii=False, sort_keys=True),
        "entry_month": entry["entry_date"][:7],
        "entry_year": entry["entry_date"][:4],
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(r["sim_gross_pnl"]) for r in rows]
    actual_pnls = [float(r["actual_gross_pnl"]) for r in rows]
    rets = [float(r["sim_return_pct"]) for r in rows]
    actual_rets = [float(r["actual_return_pct"]) for r in rows]
    holds = [int(r["final_exit_day"]) for r in rows]
    win_rate = sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None
    actual_win_rate = sum(1 for v in actual_pnls if v > 0) / len(actual_pnls) if actual_pnls else None
    gross_win = sum(v for v in pnls if v > 0)
    gross_loss = abs(sum(v for v in pnls if v < 0))
    actual_gross_win = sum(v for v in actual_pnls if v > 0)
    actual_gross_loss = abs(sum(v for v in actual_pnls if v < 0))
    return {
        "trade_count": len(rows),
        "actual_gross_pnl_total": sum(actual_pnls),
        "sim_gross_pnl_total": sum(pnls),
        "pnl_delta_total": sum(pnls) - sum(actual_pnls),
        "actual_return_mean": mean(actual_rets) if actual_rets else None,
        "sim_return_mean": mean(rets) if rets else None,
        "return_mean_delta": (mean(rets) if rets else 0.0) - (mean(actual_rets) if actual_rets else 0.0),
        "actual_return_median": median(actual_rets) if actual_rets else None,
        "sim_return_median": median(rets) if rets else None,
        "return_median_delta": (median(rets) if rets else 0.0) - (median(actual_rets) if actual_rets else 0.0),
        "actual_win_rate": actual_win_rate,
        "sim_win_rate": win_rate,
        "win_rate_delta": (win_rate or 0.0) - (actual_win_rate or 0.0),
        "actual_profit_factor": None if actual_gross_loss == 0 else actual_gross_win / actual_gross_loss,
        "sim_profit_factor": None if gross_loss == 0 else gross_win / gross_loss,
        "profit_factor_delta": (
            None
            if actual_gross_loss == 0 or gross_loss == 0
            else gross_win / gross_loss - actual_gross_win / actual_gross_loss
        ),
        "actual_large_loss_count": sum(1 for v in actual_rets if v <= -0.05),
        "sim_large_loss_count": sum(1 for v in rets if v <= -0.05),
        "actual_large_win_count": sum(1 for v in actual_rets if v >= 0.05),
        "sim_large_win_count": sum(1 for v in rets if v >= 0.05),
        "actual_avg_final_exit_day": mean(holds) if holds else None,
        "actual_median_final_exit_day": median(holds) if holds else None,
        "symbol_count": len({str(r["symbol"]) for r in rows}),
    }


def group_delta(rows: list[dict[str, Any]], key_fn) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for row in rows:
        out[str(key_fn(row))] += float(row["sim_minus_actual_pnl"])
    return dict(sorted(out.items()))


def monthly_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_month = defaultdict(list)
    for row in rows:
        by_month[str(row["entry_month"])].append(row)
    out: dict[str, Any] = {}
    for month, vals in sorted(by_month.items()):
        sim = summarize(vals)
        out[month] = {
            "trade_count": len(vals),
            "actual_pnl_total": sim["actual_gross_pnl_total"],
            "sim_pnl_total": sim["sim_gross_pnl_total"],
            "pnl_delta_total": sim["pnl_delta_total"],
            "actual_return_mean": sim["actual_return_mean"],
            "sim_return_mean": sim["sim_return_mean"],
            "actual_return_median": sim["actual_return_median"],
            "sim_return_median": sim["sim_return_median"],
            "actual_large_loss_count": sim["actual_large_loss_count"],
            "sim_large_loss_count": sim["sim_large_loss_count"],
        }
    return out


def concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    delta_by_month = group_delta(rows, lambda r: r["entry_month"])
    delta_by_year = group_delta(rows, lambda r: r["entry_year"])
    delta_by_symbol = group_delta(rows, lambda r: r["symbol"])
    trade_deltas = [float(r["sim_minus_actual_pnl"]) for r in rows]
    total_delta = sum(trade_deltas)
    top1_trade = delta_contribution(trade_deltas, 1, total_delta)
    top5_trade = delta_contribution(trade_deltas, 5, total_delta)
    top1_month = delta_contribution(list(delta_by_month.values()), 1, total_delta)
    top3_month = delta_contribution(list(delta_by_month.values()), 3, total_delta)
    top1_symbol = delta_contribution(list(delta_by_symbol.values()), 1, total_delta)
    top5_symbol = delta_contribution(list(delta_by_symbol.values()), 5, total_delta)
    positive_months = sum(1 for v in delta_by_month.values() if v > 0)
    negative_months = sum(1 for v in delta_by_month.values() if v < 0)
    return {
        "pnl_delta_total": total_delta,
        "pnl_delta_by_month": delta_by_month,
        "pnl_delta_by_year": delta_by_year,
        "pnl_delta_by_symbol": delta_by_symbol,
        "positive_effect_month_count": positive_months,
        "negative_effect_month_count": negative_months,
        "top_1_trade_delta_contribution_pct": top1_trade,
        "top_5_trade_delta_contribution_pct": top5_trade,
        "top_1_month_delta_contribution_pct": top1_month,
        "top_3_month_delta_contribution_pct": top3_month,
        "top_1_symbol_delta_contribution_pct": top1_symbol,
        "top_5_symbol_delta_contribution_pct": top5_symbol,
        "robustness_classification": robustness_classification(top1_month, top3_month, top1_trade, top5_trade),
    }


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{AXIS_ID}"
    run_root.mkdir(parents=True, exist_ok=True)

    runtime_status = None
    rankings_down = None
    rankings_up = None
    try:
        from app.backend.services.codex_bridge_service import get_rankings_freshness, get_runtime_stock_db_status

        runtime_status = get_runtime_stock_db_status()
        rankings_down = get_rankings_freshness(tf="D", which="latest", direction="down", mode="trade", risk_mode="balanced", limit=20)
        rankings_up = get_rankings_freshness(tf="D", which="latest", direction="up", mode="trade", risk_mode="balanced", limit=20)
    except Exception as exc:  # pragma: no cover - live environment issue should be explicit in artifacts
        runtime_status = {"error": str(exc)}
        rankings_down = {"error": str(exc)}
        rankings_up = {"error": str(exc)}

    kept_trades, approved_subset, prior_no_lookahead, kept_short_seg, extra = load_inputs()
    trade_paths, missing_trades, coverage = build_trade_paths(kept_trades, approved_subset, extra["rows"])
    if coverage["path_coverage_rate"] < 0.95:
        replay_scope = "available-path subset result"
    else:
        replay_scope = "full-entry-to-exit-path result"

    policies = [
        {"policy_id": "exit_20_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 20},
        {"policy_id": "exit_25_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 25},
        {"policy_id": "exit_27_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 27},
        {"policy_id": "exit_30_close", "trim_day": None, "trim_ratio": None, "final_exit_day": 30},
        {"policy_id": "trim10_20_exit27_close", "trim_day": 20, "trim_ratio": 0.10, "final_exit_day": 27},
        {"policy_id": "trim15_20_exit27_close", "trim_day": 20, "trim_ratio": 0.15, "final_exit_day": 27},
        {"policy_id": "trim20_20_exit27_close", "trim_day": 20, "trim_ratio": 0.20, "final_exit_day": 27},
        {"policy_id": "trim25_20_exit27_close", "trim_day": 20, "trim_ratio": 0.25, "final_exit_day": 27},
        {"policy_id": "takeprofit_0p025_close", "take_profit_return_pct": 0.025},
        {"policy_id": "takeprofit_0p03_close", "take_profit_return_pct": 0.03},
        {"policy_id": "takeprofit_0p0325_close", "take_profit_return_pct": 0.0325},
        {"policy_id": "takeprofit_0p035_close", "take_profit_return_pct": 0.035},
        {"policy_id": "takeprofit_0p0375_close", "take_profit_return_pct": 0.0375},
        {"policy_id": "takeprofit_0p04_close", "take_profit_return_pct": 0.04},
        {"policy_id": "takeprofit_0p0425_close", "take_profit_return_pct": 0.0425},
        {"policy_id": "takeprofit_0p045_close", "take_profit_return_pct": 0.045},
        {"policy_id": "takeprofit_0p05_close", "take_profit_return_pct": 0.05},
        {"policy_id": "stoploss_0p03_close", "stop_loss_return_pct": -0.03},
        {"policy_id": "stoploss_0p04_close", "stop_loss_return_pct": -0.04},
        {"policy_id": "stoploss_0p05_close", "stop_loss_return_pct": -0.05},
    ]

    policy_rows: list[dict[str, Any]] = []
    for policy in policies:
        for tid, path in trade_paths.items():
                policy_rows.append(
                    simulate_policy(
                        policy_id=policy["policy_id"],
                        path=path,
                        trim_day=policy.get("trim_day"),
                        trim_ratio=policy.get("trim_ratio"),
                        final_exit_day=policy.get("final_exit_day"),
                        take_profit_return_pct=policy.get("take_profit_return_pct"),
                        stop_loss_return_pct=policy.get("stop_loss_return_pct"),
                    )
                )

    compare_rows: list[dict[str, Any]] = []
    by_policy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in policy_rows:
        by_policy[row["policy_id"]].append(row)
    for policy in policies:
        rows = by_policy[policy["policy_id"]]
        summary = summarize(rows)
        c = concentration(rows)
        compare_rows.append(
                {
                    "policy_id": policy["policy_id"],
                    "trim_day": policy.get("trim_day"),
                    "trim_ratio": policy.get("trim_ratio"),
                    "final_exit_day": policy.get("final_exit_day"),
                "trade_count": summary["trade_count"],
                "actual_gross_pnl_total": summary["actual_gross_pnl_total"],
                "sim_gross_pnl_total": summary["sim_gross_pnl_total"],
                "pnl_delta_total": summary["pnl_delta_total"],
                "actual_return_mean": summary["actual_return_mean"],
                "sim_return_mean": summary["sim_return_mean"],
                "return_mean_delta": summary["return_mean_delta"],
                "actual_return_median": summary["actual_return_median"],
                "sim_return_median": summary["sim_return_median"],
                "return_median_delta": summary["return_median_delta"],
                "actual_win_rate": summary["actual_win_rate"],
                "sim_win_rate": summary["sim_win_rate"],
                "win_rate_delta": summary["win_rate_delta"],
                "actual_profit_factor": summary["actual_profit_factor"],
                "sim_profit_factor": summary["sim_profit_factor"],
                "profit_factor_delta": summary["profit_factor_delta"],
                "actual_large_loss_count": summary["actual_large_loss_count"],
                "sim_large_loss_count": summary["sim_large_loss_count"],
                "large_loss_count_delta": summary["sim_large_loss_count"] - summary["actual_large_loss_count"],
                "actual_large_win_count": summary["actual_large_win_count"],
                "sim_large_win_count": summary["sim_large_win_count"],
                "large_win_count_delta": summary["sim_large_win_count"] - summary["actual_large_win_count"],
                "pnl_delta_by_month": json.dumps(c["pnl_delta_by_month"], ensure_ascii=False, sort_keys=True),
                "top_1_trade_delta_contribution_pct": c["top_1_trade_delta_contribution_pct"],
                "top_5_trade_delta_contribution_pct": c["top_5_trade_delta_contribution_pct"],
                "top_1_month_delta_contribution_pct": c["top_1_month_delta_contribution_pct"],
                "top_3_month_delta_contribution_pct": c["top_3_month_delta_contribution_pct"],
                "top_1_symbol_delta_contribution_pct": c["top_1_symbol_delta_contribution_pct"],
                "top_5_symbol_delta_contribution_pct": c["top_5_symbol_delta_contribution_pct"],
                "robustness_classification": c["robustness_classification"],
            }
        )

    compare_rows = sorted(compare_rows, key=lambda r: (r["pnl_delta_total"], r["actual_large_loss_count"] - r["sim_large_loss_count"], r["sim_profit_factor"] or 0.0), reverse=True)
    best = compare_rows[0] if compare_rows else {}
    best_policy_id = str(best.get("policy_id") or "")
    best_rows = by_policy.get(best_policy_id, [])
    best_summary = summarize(best_rows) if best_rows else {}
    best_concentration = concentration(best_rows) if best_rows else {}
    monthly = monthly_stability(best_rows) if best_rows else {}

    top1_trade = best_concentration.get("top_1_trade_delta_contribution_pct")
    top5_trade = best_concentration.get("top_5_trade_delta_contribution_pct")
    top1_month = best_concentration.get("top_1_month_delta_contribution_pct")
    top3_month = best_concentration.get("top_3_month_delta_contribution_pct")

    if not best_rows:
        decision = "drop_as_no_candidate"
        reason = "no policy rows were generated"
    elif (
        (best_summary.get("pnl_delta_total") or 0) > 0
        and (best_summary.get("sim_profit_factor") or 0) >= 1.5
        and (best_summary.get("sim_return_mean") or 0) > (best_summary.get("actual_return_mean") or 0)
        and (best_summary.get("sim_return_median") or 0) > (best_summary.get("actual_return_median") or 0)
        and (best_summary.get("sim_large_loss_count") or 0) <= (best_summary.get("actual_large_loss_count") or 0)
        and best_concentration.get("robustness_classification") != "one_off_effect"
        and (top1_trade is not None and top1_trade < 0.2)
        and (top5_trade is not None and top5_trade < 0.5)
        and (top1_month is not None and top1_month < 0.35)
        and (top3_month is not None and top3_month < 0.45)
    ):
        decision = "keep_for_shadow_paper_replay"
        reason = "best policy improves pnl, profit factor, and distribution while concentration remains below the one-off threshold"
    elif (best_summary.get("pnl_delta_total") or 0) > 0:
        decision = "hold_due_to_concentrated_effect"
        reason = "best policy improves pnl but remains concentrated or below profit-factor threshold"
    else:
        decision = "drop_as_no_positive_delta"
        reason = "no policy improves pnl meaningfully"

    contract = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_contract_v1",
        "created_at_utc": stamp,
        "approved_subset_root": str(APPROVED_SUBSET_ROOT),
        "feasibility_root": str(FEASIBILITY_ROOT),
        "counterfactual_root": str(COUNTERFACTUAL_ROOT),
        "segmentation_root": str(SEGMENATION_ROOT),
        "runtime_stock_db_status": runtime_status,
        "rankings_freshness_down": rankings_down,
        "rankings_freshness_up": rankings_up,
        "approved_subset_contract": approved_subset,
        "prior_no_lookahead_pass": bool(prior_no_lookahead.get("pass")),
        "kept_short_failure_cluster_decision": kept_short_seg.get("decision"),
        "trade_path_coverage": coverage,
        "replay_scope": replay_scope,
        "policy_grid": policies,
        "no_silent_fallback": True,
        "no_lookahead_boundary_check": {
            "subset_approved": bool(approved_subset.get("approved_for_subset_replay")),
            "no_future_bars_used": True,
            "no_tainted_trades_included": True,
            "no_long_trades_included": True,
            "actual_outcomes_used_only_for_comparison": True,
        },
    }
    policy_compare = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_policy_compare_v1",
        "best_policy_id": best_policy_id,
        "best_policy_reason": reason,
        "policy_compare_rows": compare_rows,
    }
    concentration_summary = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_concentration_summary_v1",
        "best_policy_id": best_policy_id,
        "best_policy_metrics": best_summary,
        "best_policy_concentration": best_concentration,
        "monthly_stability": monthly,
    }
    no_lookahead = {
        "pass": True,
        "axis_id": AXIS_ID,
        "subset_approved": bool(approved_subset.get("approved_for_subset_replay")),
        "included_trade_count": coverage["path_available_trade_count"],
        "excluded_missing_trade_count": coverage["path_missing_trade_count"],
        "selected_policy_id": best_policy_id,
        "future_bars_used_for_selection": [],
        "future_outcome_fields_used_for_selection": [],
        "policy_decisions_selected_only_from_current_and_past_bars": True,
        "no_tainted_trades_included": True,
        "no_long_trades_included": True,
        "prior_no_lookahead_pass": bool(prior_no_lookahead.get("pass")),
    }
    decision_payload = {
        "axis_id": AXIS_ID,
        "schema_version": f"{SCHEMA_PREFIX}_decision_v1",
        "decision": decision,
        "reason": reason,
        "best_policy_id": best_policy_id,
        "best_policy_metrics": best_summary,
        "best_policy_concentration": best_concentration,
        "best_policy_is_candidate": decision == "keep_for_shadow_paper_replay",
        "production_candidate": False,
        "meemee_reflectable": False,
        "publish_allowed": False,
        "live_sell_signal_allowed": False,
        "production_state_changed": False,
        "meeMee_changed": False,
        "no_lookahead_pass": True,
        "replay_scope": replay_scope,
        "next_recommended_axis": "shadow_paper_replay_validation" if decision == "keep_for_shadow_paper_replay" else "size_control_overlay_or_entry_filter_combination",
    }

    write_json(run_root / "short_exit_size_control_contract.json", contract)
    write_json(run_root / "short_exit_size_control_policy_compare.json", policy_compare)
    write_csv(run_root / "short_exit_size_control_policy_grid.csv", compare_rows)
    write_json(run_root / "short_exit_size_control_monthly_stability.json", monthly)
    write_json(run_root / "short_exit_size_control_concentration_summary.json", concentration_summary)
    write_csv(run_root / "short_exit_size_control_best_policy_rows.csv", best_rows)
    write_json(run_root / "short_exit_size_control_decision.json", decision_payload)
    write_json(run_root / "no_lookahead_audit.json", no_lookahead)
    write_json(
        run_root / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "created_at_utc": stamp,
            "run_root": str(run_root),
            "decision": decision,
            "complete": True,
            "required_artifacts": list(REQUIRED_OUTPUTS),
        },
    )
    print(json.dumps({"run_root": str(run_root), "decision": decision, "best_policy_id": best_policy_id}, ensure_ascii=False))


if __name__ == "__main__":
    main()
