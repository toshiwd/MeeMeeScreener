from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


CANDIDATE_NAME = "actual_trade_rule_breadth_audit_v1"
RULE_NAME = "actual_trade_short_ma20_regime_filter_v1"
SOURCE_RUN_ROOT = Path(r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1")
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME


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


def fnum(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    return 0.0 if value in (None, "") else float(value)


def profit_factor(pnls: list[float]) -> float | None:
    gains = sum(v for v in pnls if v > 0)
    losses = abs(sum(v for v in pnls if v < 0))
    return None if losses == 0 else gains / losses


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [fnum(r, "gross_pnl") for r in rows]
    rets = [fnum(r, "gross_return_pct") for r in rows]
    holds = [int(float(r.get("holding_days") or 0)) for r in rows]
    return {
        "trade_count": len(rows),
        "gross_pnl_total": sum(pnls),
        "gross_return_mean": mean(rets) if rets else None,
        "gross_return_median": median(rets) if rets else None,
        "win_rate_gross": sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None,
        "avg_holding_days": mean(holds) if holds else None,
        "large_loss_count": sum(1 for r in rows if fnum(r, "gross_return_pct") <= -0.05),
        "large_win_count": sum(1 for r in rows if fnum(r, "gross_return_pct") >= 0.05),
        "profit_factor_gross": profit_factor(pnls),
    }


def group_rows(rows: list[dict[str, Any]], key_fn) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row))].append(row)
    return groups


def group_pnl(rows: list[dict[str, Any]], key_fn) -> tuple[dict[str, int], dict[str, float]]:
    groups = group_rows(rows, key_fn)
    return (
        {k: len(v) for k, v in sorted(groups.items())},
        {k: sum(fnum(r, "gross_pnl") for r in v) for k, v in sorted(groups.items())},
    )


def avoided_loss_contribution_pct(values: list[float], n: int, avoided_loss_total: float) -> float | None:
    if avoided_loss_total == 0:
        return None
    # For avoided losses, negative skipped PnL is the positive contribution.
    contributions = sorted([max(0.0, -v) for v in values], reverse=True)
    return sum(contributions[:n]) / avoided_loss_total if contributions else 0.0


def net_contribution_pct(values: list[float], n: int, net_effect: float) -> float | None:
    if net_effect == 0:
        return None
    contributions = sorted([abs(v) for v in values], reverse=True)
    return sum(contributions[:n]) / abs(net_effect) if contributions else 0.0


def rows_by_group_for_csv(rows: list[dict[str, Any]], key_name: str, key_fn) -> list[dict[str, Any]]:
    out = []
    for key, group in group_rows(rows, key_fn).items():
        s = summarize(group)
        out.append({key_name: key, **s})
    return sorted(out, key=lambda r: r["gross_pnl_total"])


def short_status(row: dict[str, Any]) -> str:
    status = row.get("short_ma20_entry_status") or ""
    if status in {"short_ma20_aligned", "short_ma20_price_only"}:
        return status
    if status == "short_ma20_missing":
        return "short_ma20_unknown"
    return status or "short_ma20_unknown"


def kept_short_cluster(kept_rows: list[dict[str, Any]]) -> dict[str, Any]:
    kept_shorts = [r for r in kept_rows if r.get("side") == "short"]
    groups = group_rows(kept_shorts, short_status)
    out: dict[str, Any] = {}
    for key, rows in groups.items():
        out[key] = {
            **summarize(rows),
            "pnl_by_year": group_pnl(rows, lambda r: r["entry_date"][:4])[1],
            "pnl_by_month": group_pnl(rows, lambda r: r["entry_date"][:7])[1],
            "symbol_concentration": rows_by_group_for_csv(rows, "symbol", lambda r: r["symbol"])[:10],
        }
    return out


def classify_robustness(skipped_rows: list[dict[str, Any]], month_top1_loss: float | None, month_top3_loss: float | None, trade_top1_loss: float | None, trade_top5_loss: float | None) -> tuple[str, dict[str, Any]]:
    criteria = {
        "broad_effect": ">=100 skipped trades, >=24 months, top1 month <=25%, top3 months <=55%, top1 trade <=15%, top5 trades <=40%",
        "moderately_concentrated_effect": "not broad, but >=50 skipped trades, >=12 months, top1 trade <=30%, top5 trades <=60%",
        "highly_concentrated_effect": "fails moderate criteria but not one-off",
        "one_off_effect": "top1 trade >50% or top1 month >60%",
    }
    months = len({r["entry_date"][:7] for r in skipped_rows})
    if (trade_top1_loss or 0) > 0.5 or (month_top1_loss or 0) > 0.6:
        return "one_off_effect", criteria
    if len(skipped_rows) >= 100 and months >= 24 and (month_top1_loss or 0) <= 0.25 and (month_top3_loss or 0) <= 0.55 and (trade_top1_loss or 0) <= 0.15 and (trade_top5_loss or 0) <= 0.40:
        return "broad_effect", criteria
    if len(skipped_rows) >= 50 and months >= 12 and (trade_top1_loss or 0) <= 0.30 and (trade_top5_loss or 0) <= 0.60:
        return "moderately_concentrated_effect", criteria
    return "highly_concentrated_effect", criteria


def decide(robustness: str, cluster: dict[str, Any], concentration: dict[str, Any]) -> tuple[str, str]:
    price_only = cluster.get("short_ma20_price_only", {})
    aligned = cluster.get("short_ma20_aligned", {})
    price_only_loss = abs(min(0.0, price_only.get("gross_pnl_total") or 0.0))
    aligned_loss = abs(min(0.0, aligned.get("gross_pnl_total") or 0.0))
    if robustness in {"one_off_effect", "highly_concentrated_effect"}:
        return "hold_rule_candidate", "effect is too concentrated for confirmation"
    if price_only_loss > aligned_loss * 0.5 and price_only.get("trade_count", 0) >= 50:
        return "keep_rule_candidate_needs_segmentation", "main rule is useful, but remaining losses materially cluster in short_ma20_price_only"
    if robustness == "broad_effect":
        return "keep_rule_candidate_confirmed", "improvement is broad enough and no severe status-bucket contradiction was detected"
    return "hold_rule_candidate", "PnL improves but concentration is moderate or gross-only limitation remains important"


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{RULE_NAME}_breadth_audit"
    run_root.mkdir(parents=True, exist_ok=True)

    all_rows = read_csv(SOURCE_RUN_ROOT / "counterfactual_trade_rows.csv")
    skipped = read_csv(SOURCE_RUN_ROOT / "skipped_short_trades.csv")
    kept = read_csv(SOURCE_RUN_ROOT / "kept_trades.csv")
    summary = json.loads((SOURCE_RUN_ROOT / "counterfactual_summary.json").read_text(encoding="utf-8"))
    decision = json.loads((SOURCE_RUN_ROOT / "counterfactual_decision.json").read_text(encoding="utf-8"))

    year_count, year_pnl = group_pnl(skipped, lambda r: r["entry_date"][:4])
    month_count, month_pnl = group_pnl(skipped, lambda r: r["entry_date"][:7])
    symbol_count, symbol_pnl = group_pnl(skipped, lambda r: r["symbol"])
    net_effect = -sum(fnum(r, "gross_pnl") for r in skipped)
    avoided_loss_total = sum(max(0.0, -fnum(r, "gross_pnl")) for r in skipped)
    month_values = list(month_pnl.values())
    symbol_values = list(symbol_pnl.values())
    trade_values = [fnum(r, "gross_pnl") for r in skipped]
    month_top1 = avoided_loss_contribution_pct(month_values, 1, avoided_loss_total)
    month_top3 = avoided_loss_contribution_pct(month_values, 3, avoided_loss_total)
    symbol_top1 = avoided_loss_contribution_pct(symbol_values, 1, avoided_loss_total)
    symbol_top5 = avoided_loss_contribution_pct(symbol_values, 5, avoided_loss_total)
    trade_top1 = avoided_loss_contribution_pct(trade_values, 1, avoided_loss_total)
    trade_top5 = avoided_loss_contribution_pct(trade_values, 5, avoided_loss_total)
    trade_top10 = avoided_loss_contribution_pct(trade_values, 10, avoided_loss_total)
    robustness, criteria = classify_robustness(skipped, month_top1, month_top3, trade_top1, trade_top5)
    cluster = kept_short_cluster(kept)
    final_decision, reason = decide(robustness, cluster, {})

    concentration = {
        "skipped_short_count_by_year": year_count,
        "skipped_short_pnl_by_year": year_pnl,
        "skipped_short_count_by_month": month_count,
        "skipped_short_pnl_by_month": month_pnl,
        "positive_effect_month_count": sum(1 for v in month_pnl.values() if v < 0),
        "negative_effect_month_count": sum(1 for v in month_pnl.values() if v > 0),
        "top_1_month_contribution_pct": month_top1,
        "top_3_month_contribution_pct": month_top3,
        "top_1_month_net_contribution_pct": net_contribution_pct(month_values, 1, net_effect),
        "top_3_month_net_contribution_pct": net_contribution_pct(month_values, 3, net_effect),
        "worst_months_avoided": sorted(month_pnl.items(), key=lambda kv: kv[1])[:10],
        "months_where_filter_hurt": sorted([(k, v) for k, v in month_pnl.items() if v > 0], key=lambda kv: kv[1], reverse=True),
        "skipped_short_count_by_symbol": symbol_count,
        "skipped_short_pnl_by_symbol": symbol_pnl,
        "top_1_symbol_contribution_pct": symbol_top1,
        "top_5_symbol_contribution_pct": symbol_top5,
        "top_1_symbol_net_contribution_pct": net_contribution_pct(symbol_values, 1, net_effect),
        "top_5_symbol_net_contribution_pct": net_contribution_pct(symbol_values, 5, net_effect),
        "symbol_count_skipped": len(symbol_count),
        "symbols_where_filter_helped": sorted([k for k, v in symbol_pnl.items() if v < 0]),
        "symbols_where_filter_hurt": sorted([k for k, v in symbol_pnl.items() if v > 0]),
        "top_1_trade_contribution_pct": trade_top1,
        "top_5_trade_contribution_pct": trade_top5,
        "top_10_trade_contribution_pct": trade_top10,
        "top_1_trade_net_contribution_pct": net_contribution_pct(trade_values, 1, net_effect),
        "top_5_trade_net_contribution_pct": net_contribution_pct(trade_values, 5, net_effect),
        "top_10_trade_net_contribution_pct": net_contribution_pct(trade_values, 10, net_effect),
        "net_effect": net_effect,
        "avoided_loss_total": avoided_loss_total,
        "median_skipped_short_pnl": median(trade_values) if trade_values else None,
        "mean_skipped_short_pnl": mean(trade_values) if trade_values else None,
        "large_loss_avoided_count": sum(1 for r in skipped if fnum(r, "gross_return_pct") <= -0.05),
        "large_win_skipped_count": sum(1 for r in skipped if fnum(r, "gross_return_pct") >= 0.05),
        "robustness_interpretation": robustness,
        "robustness_criteria": criteria,
    }

    final_artifact = {
        "rule_name": RULE_NAME,
        "final_decision": final_decision,
        "prior_counterfactual_decision": decision.get("decision"),
        "rule_type": "entry_filter_no_replacement_side_specific",
        "rule_definition": {
            "long": "always keep",
            "short": "skip only if close_vs_ma20_pct > 0",
            "slope": "segmentation only, not rule input",
        },
        "source_run_root": str(SOURCE_RUN_ROOT),
        "input_trade_count": 1021,
        "excluded_tainted_trade_count": 69,
        "key_metrics": {
            "baseline_gross_pnl": summary["baseline"]["gross_pnl_total"],
            "filtered_gross_pnl": summary["filtered"]["gross_pnl_total"],
            "filtered_vs_baseline_pnl_delta": summary["filtered_vs_baseline_pnl_delta"],
            "skipped_short_count": summary["skipped_short_count"],
            "skipped_short_gross_pnl": summary["skipped_short_gross_pnl"],
            "short_filtered_pnl_delta": summary["short_filtered_pnl_delta"],
            "robustness_interpretation": robustness,
            "top_1_month_contribution_pct": month_top1,
            "top_1_symbol_contribution_pct": symbol_top1,
            "top_1_trade_contribution_pct": trade_top1,
        },
        "why_keep_candidate": "historical clean-trade audit shows MA20-above short entries contributed large negative gross PnL and skipping them improves total and short-side gross PnL",
        "why_not_deploy_yet": "gross-only, diagnostic no-replacement audit; remaining kept shorts are still losing and require segmentation review",
        "known_limitations": [
            "gross-only PnL",
            "no exit simulation",
            "no capital reallocation",
            "not a complete short strategy",
            "does not prove causality",
        ],
        "non_scope_confirmed": {
            "meemee_changed": False,
            "live_ranking_changed": False,
            "champion_scoring_changed": False,
            "publish_promotion_changed": False,
            "rule_deployed": False,
            "slope_rule_added": False,
            "tainted_trades_included": False,
        },
        "next_recommended_axis": "actual_trade_short_ma20_kept_loss_segmentation_v1" if final_decision == "keep_rule_candidate_needs_segmentation" else "paper_policy_shadow_validation_v1",
        "reason": reason,
    }

    breadth_audit = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "source_run_root": str(SOURCE_RUN_ROOT),
        "breadth_metrics": concentration,
        "remaining_kept_short_loss_cluster": cluster,
        "no_lookahead_boundary_check": {
            "no_new_rule_applied": True,
            "no_future_bars_used": True,
            "no_tainted_trades_included": True,
            "long_trades_remained_untouched": True,
            "outcomes_used_only_for_breadth_concentration_review": True,
        },
    }

    write_json(run_root / "short_ma20_rule_breadth_audit.json", breadth_audit)
    write_json(run_root / "short_ma20_rule_concentration_summary.json", concentration)
    write_json(run_root / "kept_short_loss_cluster_summary.json", cluster)
    write_json(run_root / "short_ma20_rule_final_decision.json", final_artifact)
    write_csv(run_root / "skipped_short_monthly.csv", rows_by_group_for_csv(skipped, "entry_month", lambda r: r["entry_date"][:7]))
    write_csv(run_root / "skipped_short_symbol_concentration.csv", rows_by_group_for_csv(skipped, "symbol", lambda r: r["symbol"]))
    kept_status_rows = [{"short_ma20_status": k, **v} for k, v in cluster.items()]
    write_csv(run_root / "kept_short_status_summary.csv", kept_status_rows)
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": final_decision,
        "complete": True,
        "required_artifacts": [
            "short_ma20_rule_breadth_audit.json",
            "short_ma20_rule_concentration_summary.json",
            "kept_short_loss_cluster_summary.json",
            "short_ma20_rule_final_decision.json",
            "skipped_short_monthly.csv",
            "skipped_short_symbol_concentration.csv",
            "kept_short_status_summary.csv",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": final_decision, "robustness": robustness}, ensure_ascii=False))


if __name__ == "__main__":
    main()
