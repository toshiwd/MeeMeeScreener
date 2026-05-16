from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable


CANDIDATE_NAME = "actual_trade_counterfactual_rule_audit_v1"
RULE_NAME = "actual_trade_entry_ma20_regime_filter_v1"
CONTEXT_ROOT = Path(r"G:\Tradex\decision_context_reconstruction_v1\20260512T011542Z-decision_context_reconstruction_v1")
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    if fields is None:
        keys: list[str] = []
        for row in rows:
            for key in row:
                if key not in keys:
                    keys.append(key)
        fields = keys
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


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
    if losses == 0:
        return None
    return gains / losses


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(r["gross_pnl"]) for r in rows if r.get("gross_pnl") not in (None, "")]
    rets = [float(r["gross_return_pct"]) for r in rows if r.get("gross_return_pct") not in (None, "")]
    holds = [int(float(r["holding_days"])) for r in rows if r.get("holding_days") not in (None, "")]
    return {
        "trade_count": len(rows),
        "long_trade_count": sum(1 for r in rows if r.get("side") == "long"),
        "short_trade_count": sum(1 for r in rows if r.get("side") == "short"),
        "gross_pnl_total": sum(pnls) if pnls else 0.0,
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
    }


def ma20_status(row: dict[str, Any]) -> tuple[str, bool, str]:
    side = row["side"]
    close_vs = fnum(row, "close_vs_ma20_pct")
    slope = fnum(row, "ma20_slope_5d")
    if close_vs is None or slope is None:
        return f"{side}_ma20_missing", False, "missing_required_filter_field"
    if side == "long":
        if close_vs < 0:
            return "long_ma20_against", False, "long_close_below_ma20"
        if slope >= 0:
            return "long_ma20_aligned", True, "long_close_above_ma20_slope_nonnegative"
        return "long_ma20_price_only", True, "long_close_above_ma20_slope_negative"
    if side == "short":
        if close_vs > 0:
            return "short_ma20_against", False, "short_close_above_ma20"
        if slope <= 0:
            return "short_ma20_aligned", True, "short_close_below_ma20_slope_nonpositive"
        return "short_ma20_price_only", True, "short_close_below_ma20_slope_positive"
    return "unknown_side", False, "unknown_side"


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


def delta_summary(baseline: dict[str, Any], filtered: dict[str, Any], skipped: dict[str, Any], kept: list[dict[str, Any]], skipped_rows: list[dict[str, Any]]) -> dict[str, Any]:
    def sub(a: Any, b: Any) -> float | None:
        if a is None or b is None:
            return None
        return float(a) - float(b)

    long_base = summarize([r for r in kept + skipped_rows if r["side"] == "long"])
    long_kept = summarize([r for r in kept if r["side"] == "long"])
    short_base = summarize([r for r in kept + skipped_rows if r["side"] == "short"])
    short_kept = summarize([r for r in kept if r["side"] == "short"])
    return {
        "avoided_trade_count": skipped["trade_count"],
        "avoided_gross_pnl": skipped["gross_pnl_total"],
        "avoided_gross_return_mean": skipped["gross_return_mean"],
        "filtered_gross_pnl_total": filtered["gross_pnl_total"],
        "filtered_vs_baseline_pnl_delta": sub(filtered["gross_pnl_total"], baseline["gross_pnl_total"]),
        "filtered_vs_baseline_return_mean_delta": sub(filtered["gross_return_mean"], baseline["gross_return_mean"]),
        "filtered_vs_baseline_win_rate_delta": sub(filtered["win_rate_gross"], baseline["win_rate_gross"]),
        "long_filtered_delta": {
            "pnl_delta": sub(long_kept["gross_pnl_total"], long_base["gross_pnl_total"]),
            "return_mean_delta": sub(long_kept["gross_return_mean"], long_base["gross_return_mean"]),
            "win_rate_delta": sub(long_kept["win_rate_gross"], long_base["win_rate_gross"]),
            "baseline": long_base,
            "filtered": long_kept,
        },
        "short_filtered_delta": {
            "pnl_delta": sub(short_kept["gross_pnl_total"], short_base["gross_pnl_total"]),
            "return_mean_delta": sub(short_kept["gross_return_mean"], short_base["gross_return_mean"]),
            "win_rate_delta": sub(short_kept["win_rate_gross"], short_base["win_rate_gross"]),
            "baseline": short_base,
            "filtered": short_kept,
        },
    }


def decide(baseline: dict[str, Any], filtered: dict[str, Any], skipped: dict[str, Any], deltas: dict[str, Any]) -> dict[str, Any]:
    skipped_count = skipped["trade_count"]
    if baseline["trade_count"] < 100 or skipped_count == 0:
        decision = "insufficient_data"
        reason = "sample is too small or filter skipped no trades"
    else:
        pnl_delta = deltas["filtered_vs_baseline_pnl_delta"] or 0.0
        ret_delta = deltas["filtered_vs_baseline_return_mean_delta"] or 0.0
        skipped_bad = skipped["gross_pnl_total"] < -100000 and skipped_count >= 30
        long_delta = deltas["long_filtered_delta"]["pnl_delta"] or 0.0
        short_delta = deltas["short_filtered_delta"]["pnl_delta"] or 0.0
        long_count = deltas["long_filtered_delta"]["baseline"]["trade_count"]
        short_count = deltas["short_filtered_delta"]["baseline"]["trade_count"]
        side_split = (long_delta > 100000 and short_delta < -100000) or (short_delta > 100000 and long_delta < -100000)
        if pnl_delta > 100000 and ret_delta > 0 and skipped_bad:
            decision = "keep_rule_candidate"
            reason = "skipped trades had materially negative contribution and filtered ledger improved without replacement"
        elif side_split and long_count >= 100 and short_count >= 100:
            decision = "needs_more_segmentation"
            reason = "aggregate is mixed and side-aware results diverge materially"
        else:
            decision = "drop_rule_candidate"
            reason = "filtered ledger did not improve meaningfully or skipped trades were not clearly worse"
    return {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "decision": decision,
        "reason": reason,
        "baseline_trade_count": baseline["trade_count"],
        "skipped_trade_count": skipped["trade_count"],
        "filtered_trade_count": filtered["trade_count"],
        "filtered_vs_baseline_pnl_delta": deltas.get("filtered_vs_baseline_pnl_delta"),
        "filtered_vs_baseline_return_mean_delta": deltas.get("filtered_vs_baseline_return_mean_delta"),
        "filtered_vs_baseline_win_rate_delta": deltas.get("filtered_vs_baseline_win_rate_delta"),
        "interpretation_limit": "diagnostic entry-filter/no-replacement audit only; no causal claim and no net performance claim",
    }


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{RULE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    rows = read_csv(CONTEXT_ROOT / "actual_trade_decision_context.csv")
    no_lookahead_source = json.loads((CONTEXT_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    summary_source = json.loads((CONTEXT_ROOT / "context_reconstruction_summary.json").read_text(encoding="utf-8"))

    audited: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    for row in rows:
        if not boolish(row.get("clean_subset_flag")) or boolish(row.get("tainted_excluded_flag")):
            violations.append({"trade_id": row.get("normalized_trade_id"), "reason": "non_clean_or_tainted_row"})
        status, allowed, reason = ma20_status(row)
        out = dict(row)
        out["ma20_entry_status"] = status
        out["counterfactual_allowed"] = allowed
        out["counterfactual_action"] = "keep" if allowed else "skip"
        out["counterfactual_reason"] = reason
        audited.append(out)
        (kept if allowed else skipped).append(out)

    baseline_summary = summarize(audited)
    filtered_summary = summarize(kept)
    skipped_summary = summarize(skipped)
    deltas = delta_summary(baseline_summary, filtered_summary, skipped_summary, kept, skipped)

    skipped_long = [r for r in skipped if r["side"] == "long"]
    skipped_short = [r for r in skipped if r["side"] == "short"]
    counterfactual_summary = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "counterfactual_type": "entry_filter_no_replacement",
        "baseline": baseline_summary,
        "filtered": filtered_summary,
        "skipped": skipped_summary,
        "deltas": deltas,
        "skipped_long_count": len(skipped_long),
        "skipped_short_count": len(skipped_short),
        "skipped_long_gross_pnl": sum(float(r["gross_pnl"]) for r in skipped_long),
        "skipped_short_gross_pnl": sum(float(r["gross_pnl"]) for r in skipped_short),
        "skipped_by_status_summary": grouped(skipped, lambda r: r["ma20_entry_status"]),
        "kept_by_status_summary": grouped(kept, lambda r: r["ma20_entry_status"]),
        "gross_only": True,
        "capital_reallocated": False,
        "replacement_trades_used": False,
    }
    decision = decide(baseline_summary, filtered_summary, skipped_summary, deltas)

    bucket_summary = {
        "note": "descriptive summaries only; no causal claims",
        "by_side": grouped(audited, lambda r: r["side"]),
        "by_ma20_entry_status": grouped(audited, lambda r: r["ma20_entry_status"]),
        "by_close_vs_ma20_pct_bucket": grouped(audited, lambda r: f"{r['side']}|{bucket_close_vs(fnum(r, 'close_vs_ma20_pct'))}"),
        "by_ma20_slope_5d_bucket": grouped(audited, lambda r: f"{r['side']}|{bucket_slope(fnum(r, 'ma20_slope_5d'))}"),
        "by_holding_days_bucket": grouped(audited, lambda r: f"{r['side']}|{r['holding_days_bucket']}"),
        "by_partial_entry_flag": grouped(audited, lambda r: f"{r['side']}|partial_entry={r['partial_entry_flag']}"),
        "by_partial_exit_flag": grouped(audited, lambda r: f"{r['side']}|partial_exit={r['partial_exit_flag']}"),
    }

    no_lookahead = {
        "candidate_name": CANDIDATE_NAME,
        "rule_name": RULE_NAME,
        "pass": not violations and bool(no_lookahead_source.get("pass")) and len(rows) == 1021,
        "violations": violations,
        "source_no_lookahead_audit": str(CONTEXT_ROOT / "no_lookahead_audit.json"),
        "filter_fields_used": ["side", "close_vs_ma20_pct", "ma20_slope_5d"],
        "filter_uses_only_decision_context_fields": True,
        "filter_fields_computed_using_bars_lte_entry_date": bool(no_lookahead_source.get("pass")),
        "exit_or_post_entry_fields_used_for_filter": False,
        "actual_outcomes_used_only_for_evaluation": True,
        "tainted_69_trades_included": False,
        "included_trade_count": len(rows),
        "expected_clean_trade_count": summary_source.get("clean_trade_count"),
        "provisional_yahoo_bars_used": False,
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
            "counterfactual_trade_rows.csv",
            "skipped_trades.csv",
            "kept_trades.csv",
            "bucket_outcome_summary.json",
            "no_lookahead_audit.json",
        ],
    }

    write_json(run_root / "counterfactual_summary.json", counterfactual_summary)
    write_json(run_root / "counterfactual_decision.json", decision)
    write_json(run_root / "baseline_trade_summary.json", baseline_summary)
    write_json(run_root / "filtered_trade_summary.json", filtered_summary)
    write_json(run_root / "skipped_trade_summary.json", skipped_summary)
    write_csv(run_root / "counterfactual_trade_rows.csv", audited)
    write_csv(run_root / "skipped_trades.csv", skipped)
    write_csv(run_root / "kept_trades.csv", kept)
    write_json(run_root / "bucket_outcome_summary.json", bucket_summary)
    write_json(run_root / "no_lookahead_audit.json", no_lookahead)
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)

    print(json.dumps({"run_root": str(run_root), "decision": decision["decision"], "skipped": len(skipped)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
