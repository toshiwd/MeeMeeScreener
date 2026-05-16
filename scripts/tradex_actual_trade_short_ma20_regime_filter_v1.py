from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable


CANDIDATE_NAME = "actual_trade_counterfactual_rule_audit_v1"
RULE_NAME = "actual_trade_short_ma20_regime_filter_v1"
CONTEXT_ROOT = Path(r"G:\Tradex\decision_context_reconstruction_v1\20260512T011542Z-decision_context_reconstruction_v1")
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
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


def boolish(value: Any) -> bool:
    return str(value).lower() == "true"


def profit_factor(pnls: list[float]) -> float | None:
    gains = sum(v for v in pnls if v > 0)
    losses = abs(sum(v for v in pnls if v < 0))
    return None if losses == 0 else gains / losses


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(r["gross_pnl"]) for r in rows if r.get("gross_pnl") not in (None, "")]
    rets = [float(r["gross_return_pct"]) for r in rows if r.get("gross_return_pct") not in (None, "")]
    holds = [int(float(r["holding_days"])) for r in rows if r.get("holding_days") not in (None, "")]
    notionals = [float(r["notional_entry"]) for r in rows if r.get("notional_entry") not in (None, "")]
    pnl_total = sum(pnls) if pnls else 0.0
    notional_total = sum(notionals) if notionals else 0.0
    return {
        "trade_count": len(rows),
        "long_trade_count": sum(1 for r in rows if r.get("side") == "long"),
        "short_trade_count": sum(1 for r in rows if r.get("side") == "short"),
        "gross_pnl_total": pnl_total,
        "gross_return_mean": mean(rets) if rets else None,
        "gross_return_median": median(rets) if rets else None,
        "win_rate_gross": sum(1 for v in pnls if v > 0) / len(pnls) if pnls else None,
        "avg_holding_days": mean(holds) if holds else None,
        "median_holding_days": median(holds) if holds else None,
        "large_loss_count": sum(1 for r in rows if float(r.get("gross_return_pct") or 0) <= -0.05),
        "large_win_count": sum(1 for r in rows if float(r.get("gross_return_pct") or 0) >= 0.05),
        "profit_factor_gross": profit_factor(pnls),
        "average_pnl_per_trade": mean(pnls) if pnls else None,
        "median_pnl_per_trade": median(pnls) if pnls else None,
        "notional_entry_total": notional_total,
        "pnl_per_notional": pnl_total / notional_total if notional_total else None,
    }


def short_status(row: dict[str, Any]) -> str:
    if row["side"] != "short":
        return "long_not_applicable"
    close_vs = fnum(row, "close_vs_ma20_pct")
    slope = fnum(row, "ma20_slope_5d")
    if close_vs is None:
        return "short_ma20_missing"
    if close_vs > 0:
        return "short_ma20_against"
    if slope is None:
        return "short_ma20_price_only"
    if slope <= 0:
        return "short_ma20_aligned"
    return "short_ma20_price_only"


def allowed(row: dict[str, Any]) -> tuple[bool, str]:
    if row["side"] == "long":
        return True, "long_always_kept"
    close_vs = fnum(row, "close_vs_ma20_pct")
    if close_vs is None:
        return True, "short_missing_close_vs_ma20_kept_no_silent_exclusion"
    if close_vs > 0:
        return False, "short_close_above_ma20"
    return True, "short_close_at_or_below_ma20"


def bucket_close_vs(value: float | None) -> str:
    if value is None:
        return "missing"
    if value < -0.05:
        return "below_-5pct"
    if value < 0:
        return "below_0_to_-5pct"
    if value < 0.05:
        return "above_0_to_5pct"
    return "above_5pct"


def bucket_slope(value: float | None) -> str:
    if value is None:
        return "missing"
    if value < -0.03:
        return "down_strong"
    if value < 0:
        return "down_mild"
    if value < 0.03:
        return "up_mild"
    return "up_strong"


def grouped(rows: list[dict[str, Any]], fn: Callable[[dict[str, Any]], str]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[fn(row)].append(row)
    return {key: summarize(value) for key, value in sorted(groups.items())}


def delta(a: Any, b: Any) -> float | None:
    if a is None or b is None:
        return None
    return float(a) - float(b)


def decide(baseline: dict[str, Any], filtered: dict[str, Any], skipped_short: dict[str, Any], short_baseline: dict[str, Any], short_kept: dict[str, Any], monthly: dict[str, Any]) -> dict[str, Any]:
    skipped_count = skipped_short["trade_count"]
    pnl_delta = delta(filtered["gross_pnl_total"], baseline["gross_pnl_total"]) or 0.0
    short_pnl_delta = delta(short_kept["gross_pnl_total"], short_baseline["gross_pnl_total"]) or 0.0
    short_ret_delta = delta(short_kept["gross_return_mean"], short_baseline["gross_return_mean"]) or 0.0
    short_wr_delta = delta(short_kept["win_rate_gross"], short_baseline["win_rate_gross"]) or 0.0
    monthly_improved = sum(1 for v in monthly.values() if (v.get("skipped_short_gross_pnl") or 0) < 0)
    concentration_ok = monthly_improved >= 4 and skipped_count >= 50
    if skipped_count < 30:
        decision = "insufficient_data"
        reason = "too few skipped short trades"
    elif pnl_delta > 100000 and short_pnl_delta > 100000 and skipped_short["gross_pnl_total"] < -100000 and short_ret_delta >= -0.001 and short_wr_delta >= -0.02 and concentration_ok:
        decision = "keep_rule_candidate"
        reason = "skipped short trades had materially negative contribution and improvement was not tiny-sample concentrated"
    elif pnl_delta > 0 and short_pnl_delta > 0:
        decision = "needs_more_segmentation"
        reason = "PnL improves, but return/win-rate or month breadth needs segmentation before keep"
    else:
        decision = "drop_rule_candidate"
        reason = "short-side filter did not improve the ledger meaningfully"
    return {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "decision": decision,
        "reason": reason,
        "baseline_trade_count": baseline["trade_count"],
        "filtered_trade_count": filtered["trade_count"],
        "skipped_short_count": skipped_count,
        "filtered_vs_baseline_pnl_delta": pnl_delta,
        "short_filtered_pnl_delta": short_pnl_delta,
        "short_filtered_return_mean_delta": short_ret_delta,
        "short_filtered_win_rate_delta": short_wr_delta,
        "interpretation_limit": "diagnostic entry-filter/no-replacement audit only; no causal claim and no net performance claim",
    }


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{RULE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)
    rows = read_csv(CONTEXT_ROOT / "actual_trade_decision_context.csv")
    source_no_lookahead = json.loads((CONTEXT_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    source_summary = json.loads((CONTEXT_ROOT / "context_reconstruction_summary.json").read_text(encoding="utf-8"))

    audited: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    skipped_short: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    for row in rows:
        if not boolish(row.get("clean_subset_flag")) or boolish(row.get("tainted_excluded_flag")):
            violations.append({"trade_id": row.get("normalized_trade_id"), "reason": "non_clean_or_tainted_row"})
        keep, reason = allowed(row)
        out = dict(row)
        out["short_ma20_entry_status"] = short_status(row)
        out["counterfactual_allowed"] = keep
        out["counterfactual_action"] = "keep" if keep else "skip"
        out["counterfactual_reason"] = reason
        audited.append(out)
        if keep:
            kept.append(out)
        else:
            skipped_short.append(out)

    baseline = summarize(audited)
    filtered = summarize(kept)
    skipped = summarize(skipped_short)
    short_baseline_rows = [r for r in audited if r["side"] == "short"]
    short_kept_rows = [r for r in kept if r["side"] == "short"]
    short_baseline = summarize(short_baseline_rows)
    short_kept = summarize(short_kept_rows)

    monthly = {}
    for key, group in grouped(skipped_short, lambda r: r["entry_date"][:7]).items():
        monthly[key] = {"skipped_short_gross_pnl": group["gross_pnl_total"], **group}
    year_summary = grouped(audited, lambda r: r["entry_date"][:4])

    counterfactual_summary = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "counterfactual_type": "entry_filter_no_replacement_side_specific",
        "baseline": baseline,
        "filtered": filtered,
        "skipped": skipped,
        "skipped_trade_count": skipped["trade_count"],
        "skipped_short_count": skipped["short_trade_count"],
        "skipped_short_gross_pnl": skipped["gross_pnl_total"],
        "filtered_gross_pnl_total": filtered["gross_pnl_total"],
        "filtered_vs_baseline_pnl_delta": delta(filtered["gross_pnl_total"], baseline["gross_pnl_total"]),
        "filtered_vs_baseline_return_mean_delta": delta(filtered["gross_return_mean"], baseline["gross_return_mean"]),
        "filtered_vs_baseline_win_rate_delta": delta(filtered["win_rate_gross"], baseline["win_rate_gross"]),
        "short_filtered_pnl_delta": delta(short_kept["gross_pnl_total"], short_baseline["gross_pnl_total"]),
        "short_filtered_return_mean_delta": delta(short_kept["gross_return_mean"], short_baseline["gross_return_mean"]),
        "short_filtered_win_rate_delta": delta(short_kept["win_rate_gross"], short_baseline["win_rate_gross"]),
        "kept_short_count": short_kept["trade_count"],
        "skipped_short_by_status_summary": grouped(skipped_short, lambda r: r["short_ma20_entry_status"]),
        "kept_short_by_status_summary": grouped(short_kept_rows, lambda r: r["short_ma20_entry_status"]),
        "monthly_summary": monthly,
        "year_summary": year_summary,
        "holding_days_bucket_summary": grouped(audited, lambda r: f"{r['counterfactual_action']}|{r['side']}|{r['holding_days_bucket']}"),
        "partial_entry_exit_summary": grouped(audited, lambda r: f"{r['counterfactual_action']}|{r['side']}|pe={r['partial_entry_flag']}|px={r['partial_exit_flag']}"),
        "long_trades_changed": False,
        "capital_reallocated": False,
        "replacement_trades_used": False,
    }
    decision = decide(baseline, filtered, skipped, short_baseline, short_kept, monthly)

    short_side_summary = {
        "baseline_short": short_baseline,
        "kept_short": short_kept,
        "skipped_short": skipped,
        "short_filtered_pnl_delta": counterfactual_summary["short_filtered_pnl_delta"],
        "short_filtered_return_mean_delta": counterfactual_summary["short_filtered_return_mean_delta"],
        "short_filtered_win_rate_delta": counterfactual_summary["short_filtered_win_rate_delta"],
        "skipped_short_by_status_summary": counterfactual_summary["skipped_short_by_status_summary"],
        "kept_short_by_status_summary": counterfactual_summary["kept_short_by_status_summary"],
    }

    bucket_summary = {
        "note": "descriptive summaries only; no causal claims",
        "by_side": grouped(audited, lambda r: r["side"]),
        "by_short_ma20_entry_status": grouped(audited, lambda r: r["short_ma20_entry_status"]),
        "by_close_vs_ma20_pct_bucket": grouped(audited, lambda r: f"{r['side']}|{bucket_close_vs(fnum(r, 'close_vs_ma20_pct'))}"),
        "by_ma20_slope_5d_bucket": grouped(audited, lambda r: f"{r['side']}|{bucket_slope(fnum(r, 'ma20_slope_5d'))}"),
        "by_holding_days_bucket": grouped(audited, lambda r: f"{r['side']}|{r['holding_days_bucket']}"),
        "by_partial_entry_flag": grouped(audited, lambda r: f"{r['side']}|partial_entry={r['partial_entry_flag']}"),
        "by_partial_exit_flag": grouped(audited, lambda r: f"{r['side']}|partial_exit={r['partial_exit_flag']}"),
        "by_entry_year": grouped(audited, lambda r: r["entry_date"][:4]),
        "by_entry_month": grouped(audited, lambda r: r["entry_date"][:7]),
    }

    no_lookahead = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "pass": not violations and bool(source_no_lookahead.get("pass")) and len(rows) == source_summary.get("clean_trade_count"),
        "violations": violations,
        "filter_fields_used": ["side", "close_vs_ma20_pct"],
        "segmentation_fields_used": ["ma20_slope_5d"],
        "filter_uses_only_decision_context_fields": True,
        "filter_fields_came_from_entry_date_context": bool(source_no_lookahead.get("pass")),
        "exit_or_post_entry_fields_used_for_filter": False,
        "actual_outcomes_used_only_for_evaluation": True,
        "tainted_69_trades_included": False,
        "provisional_yahoo_bars_used": False,
        "long_trades_modified": False,
        "included_trade_count": len(rows),
    }

    complete = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision["decision"],
        "complete": True,
        "required_artifacts": [
            "counterfactual_summary.json",
            "counterfactual_decision.json",
            "baseline_trade_summary.json",
            "filtered_trade_summary.json",
            "skipped_trade_summary.json",
            "short_side_summary.json",
            "bucket_outcome_summary.json",
            "counterfactual_trade_rows.csv",
            "skipped_short_trades.csv",
            "kept_trades.csv",
            "no_lookahead_audit.json",
        ],
    }

    write_json(run_root / "counterfactual_summary.json", counterfactual_summary)
    write_json(run_root / "counterfactual_decision.json", decision)
    write_json(run_root / "baseline_trade_summary.json", baseline)
    write_json(run_root / "filtered_trade_summary.json", filtered)
    write_json(run_root / "skipped_trade_summary.json", skipped)
    write_json(run_root / "short_side_summary.json", short_side_summary)
    write_json(run_root / "bucket_outcome_summary.json", bucket_summary)
    write_csv(run_root / "counterfactual_trade_rows.csv", audited)
    write_csv(run_root / "skipped_short_trades.csv", skipped_short)
    write_csv(run_root / "kept_trades.csv", kept)
    write_json(run_root / "no_lookahead_audit.json", no_lookahead)
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": decision["decision"], "skipped_short": len(skipped_short)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
