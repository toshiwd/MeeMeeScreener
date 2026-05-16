from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


CANDIDATE_NAME = "actual_trade_short_ma20_kept_loss_segmentation_v1"
PRIOR_ROOT = Path(r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1")
BREADTH_ROOT = Path(r"G:\Tradex\actual_trade_rule_breadth_audit_v1\20260512T014455Z-actual_trade_short_ma20_regime_filter_v1_breadth_audit")
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_loss_segmentation_v1")


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


def profit_factor(pnls: list[float]) -> float | None:
    gains = sum(v for v in pnls if v > 0)
    losses = abs(sum(v for v in pnls if v < 0))
    return None if losses == 0 else gains / losses


def summarize(rows: list[dict[str, Any]], full: bool = True) -> dict[str, Any]:
    pnls = [fnum(r, "gross_pnl") or 0.0 for r in rows]
    rets = [fnum(r, "gross_return_pct") or 0.0 for r in rows]
    holds = [int(float(r.get("holding_days") or 0)) for r in rows]
    notionals = [fnum(r, "notional_entry") or 0.0 for r in rows]
    out = {
        "trade_count": len(rows),
        "gross_pnl_total": sum(pnls),
        "gross_return_mean": mean(rets) if rets else None,
        "win_rate_gross": sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None,
        "large_loss_count": sum(1 for r in rows if (fnum(r, "gross_return_pct") or 0.0) <= -0.05),
        "large_win_count": sum(1 for r in rows if (fnum(r, "gross_return_pct") or 0.0) >= 0.05),
    }
    if full:
        notional_total = sum(notionals)
        out.update(
            {
                "gross_return_median": median(rets) if rets else None,
                "avg_holding_days": mean(holds) if holds else None,
                "median_holding_days": median(holds) if holds else None,
                "profit_factor_gross": profit_factor(pnls),
                "notional_entry_total": notional_total,
                "pnl_per_notional": sum(pnls) / notional_total if notional_total else None,
            }
        )
    return out


def group(rows: list[dict[str, Any]], key_fn, full: bool = True) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row))].append(row)
    return {k: summarize(v, full=full) for k, v in sorted(groups.items())}


def holding_bucket(row: dict[str, Any]) -> str:
    d = int(float(row.get("holding_days") or 0))
    if d == 0:
        return "same_day"
    if d <= 3:
        return "1_3d"
    if d <= 7:
        return "4_7d"
    if d <= 14:
        return "8_14d"
    if d <= 30:
        return "15_30d"
    return "31d_plus"


def bucket(value: float | None, cuts: list[tuple[float, str]], high_label: str) -> str:
    if value is None:
        return "missing"
    for cutoff, label in cuts:
        if value < cutoff:
            return label
    return high_label


def loss_concentration(rows: list[dict[str, Any]], key_fn) -> tuple[dict[str, float], float | None, float | None]:
    losses = group_loss(rows, key_fn)
    total_loss = sum(abs(v) for v in losses.values() if v < 0)
    vals = sorted([abs(v) for v in losses.values() if v < 0], reverse=True)
    if not total_loss:
        return losses, None, None
    return losses, sum(vals[:1]) / total_loss if vals else 0.0, sum(vals[:3]) / total_loss if vals else 0.0


def group_loss(rows: list[dict[str, Any]], key_fn) -> dict[str, float]:
    groups: dict[str, float] = defaultdict(float)
    for row in rows:
        groups[str(key_fn(row))] += fnum(row, "gross_pnl") or 0.0
    return dict(sorted(groups.items()))


def loss_trade_contribution(rows: list[dict[str, Any]], n: int) -> float | None:
    losses = sorted([abs(fnum(r, "gross_pnl") or 0.0) for r in rows if (fnum(r, "gross_pnl") or 0.0) < 0], reverse=True)
    total = sum(losses)
    return None if total == 0 else sum(losses[:n]) / total


def status(row: dict[str, Any]) -> str:
    s = row.get("short_ma20_entry_status") or ""
    if s == "short_ma20_missing":
        return "short_ma20_unknown"
    return s or "short_ma20_unknown"


def classify_failure(status_summary: dict[str, Any], holding_summary: dict[str, Any], concentration: dict[str, Any]) -> tuple[str, str, str]:
    aligned = status_summary.get("short_ma20_aligned", {})
    price = status_summary.get("short_ma20_price_only", {})
    aligned_loss = abs(min(0.0, aligned.get("gross_pnl_total") or 0.0))
    price_loss = abs(min(0.0, price.get("gross_pnl_total") or 0.0))
    long_hold_loss = sum(abs(min(0.0, v.get("gross_pnl_total") or 0.0)) for k, v in holding_summary.items() if k in {"15_30d", "31d_plus"})
    total_loss = sum(abs(min(0.0, v.get("gross_pnl_total") or 0.0)) for v in status_summary.values())
    if total_loss == 0:
        return "needs_more_fields", "needs_more_fields", "no remaining kept-short loss to classify"
    if (concentration.get("top_1_trade_loss_contribution_pct") or 0) > 0.35 or (concentration.get("top_5_trade_loss_contribution_pct") or 0) > 0.65:
        return "large_loss_outlier_cluster", "test_large_loss_control_next", "losses are driven by a small number of large losing trades"
    if (concentration.get("top_1_symbol_loss_contribution_pct") or 0) > 0.35 or (concentration.get("top_1_month_loss_contribution_pct") or 0) > 0.35:
        return "symbol_concentration_cluster", "test_symbol_or_month_exclusion_next", "losses are highly concentrated by symbol or month"
    if long_hold_loss / total_loss >= 0.55:
        return "holding_duration_cluster", "test_holding_duration_exit_rule_next", "losses are primarily concentrated in longer holding duration buckets"
    if price_loss >= aligned_loss * 1.25 and price.get("trade_count", 0) >= 50:
        return "slope_price_only_cluster", "test_slope_filter_next", "price_only bucket dominates remaining kept-short loss"
    if aligned_loss >= price_loss * 1.25 and aligned.get("trade_count", 0) >= 50:
        return "aligned_trend_failure_cluster", "test_large_loss_control_next", "largest remaining losses occur even when price and MA20 slope are aligned"
    return "mixed_no_single_cluster", "test_large_loss_control_next", "no single entry bucket dominates; inspect large-loss control before adding slope"


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    kept = read_csv(PRIOR_ROOT / "kept_trades.csv")
    kept_shorts = [r for r in kept if r.get("side") == "short" and r.get("counterfactual_action") == "keep" and r.get("tainted_excluded_flag", "").lower() == "false"]
    prior_no_lookahead = json.loads((PRIOR_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    prior_summary = json.loads((PRIOR_ROOT / "counterfactual_summary.json").read_text(encoding="utf-8"))
    breadth_decision = json.loads((BREADTH_ROOT / "short_ma20_rule_final_decision.json").read_text(encoding="utf-8"))

    status_summary = group(kept_shorts, status)
    holding_summary = group(kept_shorts, holding_bucket)
    status_holding = group(kept_shorts, lambda r: f"{status(r)}|{holding_bucket(r)}")
    entry_context = {
        "by_close_vs_ma20_pct": group(kept_shorts, lambda r: bucket(fnum(r, "close_vs_ma20_pct"), [(-0.05, "below_-5pct"), (0.0, "below_0_to_-5pct"), (0.05, "above_0_to_5pct")], "above_5pct"), full=False),
        "by_ma20_slope_5d": group(kept_shorts, lambda r: bucket(fnum(r, "ma20_slope_5d"), [(-0.03, "down_strong"), (0.0, "down_mild"), (0.03, "up_mild")], "up_strong"), full=False),
        "by_close_vs_ma60_pct": group(kept_shorts, lambda r: bucket(fnum(r, "close_vs_ma60_pct"), [(-0.05, "below_-5pct"), (0.0, "below_0_to_-5pct"), (0.05, "above_0_to_5pct")], "above_5pct"), full=False),
        "by_ma60_slope_5d": group(kept_shorts, lambda r: bucket(fnum(r, "ma60_slope_5d"), [(-0.03, "down_strong"), (0.0, "down_mild"), (0.03, "up_mild")], "up_strong"), full=False),
        "by_drawdown_20d": group(kept_shorts, lambda r: bucket(fnum(r, "drawdown_20d"), [(-0.10, "deep"), (-0.03, "moderate"), (0.0, "shallow")], "positive_or_zero"), full=False),
        "by_runup_20d": group(kept_shorts, lambda r: bucket(fnum(r, "runup_20d"), [(0.03, "low"), (0.10, "moderate"), (0.25, "high")], "very_high"), full=False),
        "by_distance_from_20d_high_pct": group(kept_shorts, lambda r: bucket(fnum(r, "distance_from_20d_high_pct"), [(-0.10, "far_below_high"), (-0.03, "moderate_below_high"), (0.0, "near_high")], "at_or_above_high"), full=False),
        "by_distance_from_20d_low_pct": group(kept_shorts, lambda r: bucket(fnum(r, "distance_from_20d_low_pct"), [(0.03, "near_low"), (0.10, "moderate_above_low"), (0.25, "far_above_low")], "very_far_above_low"), full=False),
        "by_vol_ratio_t_20": group(kept_shorts, lambda r: bucket(fnum(r, "vol_ratio_t_20"), [(0.75, "low"), (1.25, "normal"), (2.0, "high")], "very_high"), full=False),
        "by_close_position_in_range": group(kept_shorts, lambda r: bucket(fnum(r, "close_position_in_range"), [(0.25, "low_close"), (0.50, "lower_mid"), (0.75, "upper_mid")], "high_close"), full=False),
        "by_partial_entry_flag": group(kept_shorts, lambda r: str(r.get("partial_entry_flag")), full=False),
        "by_partial_exit_flag": group(kept_shorts, lambda r: str(r.get("partial_exit_flag")), full=False),
    }

    year_loss, top1_year, top3_year = loss_concentration(kept_shorts, lambda r: r["entry_date"][:4])
    month_loss, top1_month, top3_month = loss_concentration(kept_shorts, lambda r: r["entry_date"][:7])
    symbol_loss, top1_symbol, top3_symbol = loss_concentration(kept_shorts, lambda r: r["symbol"])
    # top5 symbol separately
    symbol_losses = sorted([abs(v) for v in symbol_loss.values() if v < 0], reverse=True)
    symbol_total_loss = sum(symbol_losses)
    top5_symbol = None if symbol_total_loss == 0 else sum(symbol_losses[:5]) / symbol_total_loss
    concentration = {
        "loss_by_year": year_loss,
        "loss_by_month": month_loss,
        "loss_by_symbol": symbol_loss,
        "top_1_month_loss_contribution_pct": top1_month,
        "top_3_month_loss_contribution_pct": top3_month,
        "top_1_symbol_loss_contribution_pct": top1_symbol,
        "top_5_symbol_loss_contribution_pct": top5_symbol,
        "top_1_trade_loss_contribution_pct": loss_trade_contribution(kept_shorts, 1),
        "top_5_trade_loss_contribution_pct": loss_trade_contribution(kept_shorts, 5),
        "largest_losing_trades": sorted(kept_shorts, key=lambda r: fnum(r, "gross_pnl") or 0.0)[:20],
        "largest_winning_trades": sorted(kept_shorts, key=lambda r: fnum(r, "gross_pnl") or 0.0, reverse=True)[:20],
    }
    cluster, next_decision, reason = classify_failure(status_summary, holding_summary, concentration)
    decision = {
        "candidate_name": CANDIDATE_NAME,
        "decision": next_decision,
        "failure_cluster_classification": cluster,
        "reason": reason,
        "prior_rule_final_decision": breadth_decision.get("final_decision"),
        "kept_short_trade_count": len(kept_shorts),
        "kept_short_gross_pnl_total": sum(fnum(r, "gross_pnl") or 0.0 for r in kept_shorts),
        "no_new_rule_applied": True,
        "no_future_bars_used": True,
        "outcomes_used_only_for_diagnostic_summaries": True,
        "tainted_trades_excluded": True,
        "long_trades_not_modified": True,
        "provisional_yahoo_data_used": False,
    }

    segmentation = {
        "candidate_name": CANDIDATE_NAME,
        "source_counterfactual_root": str(PRIOR_ROOT),
        "source_breadth_root": str(BREADTH_ROOT),
        "prior_rule_name": "actual_trade_short_ma20_regime_filter_v1",
        "kept_short_summary": summarize(kept_shorts),
        "status_segmentation": status_summary,
        "holding_duration_segmentation": holding_summary,
        "status_x_holding_days_segmentation": status_holding,
        "entry_context_segmentation": entry_context,
        "concentration_summary": concentration,
        "failure_cluster_decision": decision,
        "boundary_check": {
            "no_new_trading_rule_applied": True,
            "no_counterfactual_filter_run": True,
            "no_exit_simulation_run": True,
            "no_future_bars_used_as_entry_context": bool(prior_no_lookahead.get("pass")),
            "tainted_trades_excluded": True,
            "long_trades_not_modified": True,
            "gross_only_pnl": True,
        },
    }

    write_json(run_root / "kept_short_loss_segmentation.json", segmentation)
    write_json(run_root / "kept_short_status_segmentation.json", status_summary)
    write_json(run_root / "kept_short_holding_duration_segmentation.json", {"by_holding_days": holding_summary, "by_status_x_holding_days": status_holding})
    write_json(run_root / "kept_short_entry_context_segmentation.json", entry_context)
    write_json(run_root / "kept_short_concentration_summary.json", concentration)
    write_json(run_root / "kept_short_failure_cluster_decision.json", decision)
    kept_losers = [r for r in kept_shorts if (fnum(r, "gross_pnl") or 0.0) < 0]
    write_csv(run_root / "kept_short_loss_trades.csv", kept_losers)
    write_csv(run_root / "kept_short_largest_losers.csv", concentration["largest_losing_trades"])
    write_csv(run_root / "kept_short_largest_winners.csv", concentration["largest_winning_trades"])
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": next_decision,
        "complete": True,
        "required_artifacts": [
            "kept_short_loss_segmentation.json",
            "kept_short_status_segmentation.json",
            "kept_short_holding_duration_segmentation.json",
            "kept_short_entry_context_segmentation.json",
            "kept_short_concentration_summary.json",
            "kept_short_failure_cluster_decision.json",
            "kept_short_loss_trades.csv",
            "kept_short_largest_losers.csv",
            "kept_short_largest_winners.csv",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": next_decision, "cluster": cluster}, ensure_ascii=False))


if __name__ == "__main__":
    main()
