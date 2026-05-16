from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_short_exit_path_bar_gap_audit_v1"
FEASIBILITY_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_feasibility_v1\20260512T015542Z-actual_trade_short_holding_duration_exit_feasibility_v1")
PRIOR_ROOT = Path(r"G:\Tradex\actual_trade_counterfactual_rule_audit_v1\20260512T013658Z-actual_trade_short_ma20_regime_filter_v1")
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME


AUDIT_FIELDS = [
    "normalized_trade_id", "symbol", "entry_date", "actual_exit_date",
    "entry_price", "exit_price", "quantity", "gross_pnl", "holding_days_actual",
    "account_type", "broker", "missing_reason_current", "any_daily_bars_exist",
    "first_available_daily_bar_date", "last_available_daily_bar_date",
    "entry_date_before_first_bar", "entry_date_after_last_bar", "exit_date_after_last_bar",
    "nearest_prior_bar_date", "nearest_next_bar_date", "entry_bar_shift_possible",
    "exit_bar_shift_possible", "symbol_code_normalization_candidate", "suspected_cause",
    "repair_action",
]


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


def parse_d(value: str) -> date:
    return datetime.fromisoformat(value).date()


def epoch_to_date(value: int) -> date:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).date()


def date_to_epoch(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def fnum(row: dict[str, Any], key: str) -> float:
    value = row.get(key)
    return 0.0 if value in (None, "") else float(value)


def load_bar_dates(symbols: set[str]) -> dict[str, list[date]]:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        placeholders = ",".join(["?"] * len(symbols))
        rows = con.execute(
            f"""
            SELECT code, date
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN ({placeholders})
            ORDER BY code, date
            """,
            sorted(symbols),
        ).fetchall()
    finally:
        con.close()
    out: dict[str, list[date]] = defaultdict(list)
    for code, d in rows:
        out[str(code)].append(epoch_to_date(d))
    return out


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
        "median_holding_days": median(holds) if holds else None,
        "symbol_count": len({r["symbol"] for r in rows}),
        "year_distribution": dict(Counter(r["entry_date"][:4] for r in rows)),
        "month_distribution": dict(Counter(r["entry_date"][:7] for r in rows)),
        "account_type_distribution": dict(Counter(r.get("account_type", "") for r in rows)),
        "side_distribution": dict(Counter(r.get("side", "") for r in rows)),
        "large_loss_count": sum(1 for r in rows if fnum(r, "gross_return_pct") <= -0.05),
        "large_win_count": sum(1 for r in rows if fnum(r, "gross_return_pct") >= 0.05),
    }


def classify_bias(available: dict[str, Any], missing: dict[str, Any]) -> tuple[str, str]:
    if missing["trade_count"] == 0:
        return "missing_paths_low_bias", "no missing paths"
    pnl_diff = abs((missing.get("gross_return_mean") or 0) - (available.get("gross_return_mean") or 0))
    hold_diff = abs((missing.get("avg_holding_days") or 0) - (available.get("avg_holding_days") or 0))
    missing_years = set(missing["year_distribution"].keys())
    avail_years = set(available["year_distribution"].keys())
    unique_year_bias = len(missing_years - avail_years) > 0
    if missing["trade_count"] / max(available["trade_count"] + missing["trade_count"], 1) > 0.25:
        return "missing_paths_high_bias", "missing paths exceed 25% of target population"
    if pnl_diff > 0.01 or hold_diff > 10 or unique_year_bias:
        return "missing_paths_moderate_bias", "missing group differs materially by return, holding days, or year coverage"
    return "missing_paths_low_bias", "missing group appears broadly similar on available diagnostics"


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    missing_rows = read_csv(FEASIBILITY_ROOT / "short_trade_path_missing.csv")
    kept_rows = read_csv(PRIOR_ROOT / "kept_trades.csv")
    kept_shorts = [r for r in kept_rows if r.get("side") == "short" and r.get("counterfactual_action") == "keep" and r.get("tainted_excluded_flag", "").lower() == "false"]
    missing_ids = {r["normalized_trade_id"] for r in missing_rows}
    missing_trades = [r for r in kept_shorts if r["normalized_trade_id"] in missing_ids]
    available_trades = [r for r in kept_shorts if r["normalized_trade_id"] not in missing_ids]
    symbols = {r["symbol"] for r in missing_trades}
    bars_by_symbol = load_bar_dates(symbols)

    audit_rows: list[dict[str, Any]] = []
    for trade in missing_trades:
        symbol = trade["symbol"]
        entry = parse_d(trade["entry_date"])
        exit_d = parse_d(trade["exit_date"])
        dates = bars_by_symbol.get(symbol, [])
        prior = [d for d in dates if d <= entry]
        nxt = [d for d in dates if d >= entry]
        exit_prior = [d for d in dates if d <= exit_d]
        first = dates[0] if dates else None
        last = dates[-1] if dates else None
        if not dates:
            cause = "missing_daily_bars_for_symbol"
            action = "load_missing_pan_history"
        elif entry < first:
            cause = "insufficient_pan_history"
            action = "load_missing_pan_history"
        elif not prior and nxt:
            cause = "entry_date_non_trading_day_shift_needed"
            action = "allow_prior_trading_day_shift"
        elif symbol.endswith("0") and not dates:
            cause = "symbol_code_mismatch"
            action = "normalize_symbol_code"
        else:
            cause = "data_source_gap"
            action = "manual_mapping_required"
        audit_rows.append(
            {
                "normalized_trade_id": trade["normalized_trade_id"],
                "symbol": symbol,
                "entry_date": trade["entry_date"],
                "actual_exit_date": trade["exit_date"],
                "entry_price": trade["entry_price"],
                "exit_price": trade["exit_price"],
                "quantity": trade["quantity"],
                "gross_pnl": trade["gross_pnl"],
                "holding_days_actual": trade["holding_days"],
                "account_type": trade.get("account_type", ""),
                "broker": trade.get("broker", ""),
                "missing_reason_current": next((r["missing_reason"] for r in missing_rows if r["normalized_trade_id"] == trade["normalized_trade_id"]), "entry_bar_missing"),
                "any_daily_bars_exist": bool(dates),
                "first_available_daily_bar_date": first.isoformat() if first else None,
                "last_available_daily_bar_date": last.isoformat() if last else None,
                "entry_date_before_first_bar": bool(first and entry < first),
                "entry_date_after_last_bar": bool(last and entry > last),
                "exit_date_after_last_bar": bool(last and exit_d > last),
                "nearest_prior_bar_date": prior[-1].isoformat() if prior else None,
                "nearest_next_bar_date": nxt[0].isoformat() if nxt else None,
                "entry_bar_shift_possible": bool(prior),
                "exit_bar_shift_possible": bool(exit_prior),
                "symbol_code_normalization_candidate": None,
                "suspected_cause": cause,
                "repair_action": action,
            }
        )

    cause_counts = Counter(r["suspected_cause"] for r in audit_rows)
    repair_counts = Counter(r["repair_action"] for r in audit_rows)
    available_summary = summarize(available_trades)
    missing_summary = summarize(missing_trades)
    bias_class, bias_reason = classify_bias(available_summary, missing_summary)
    mostly_repairable = sum(v for k, v in repair_counts.items() if k in {"allow_prior_trading_day_shift", "normalize_symbol_code", "map_old_symbol_code", "load_missing_pan_history"}) / max(len(audit_rows), 1) >= 0.7
    if mostly_repairable:
        decision = "repair_paths_before_replay"
        reason = "missing paths are mostly repairable through PAN history or deterministic mapping repair"
    elif bias_class == "missing_paths_low_bias" and len(available_trades) >= 300:
        decision = "approve_available_path_subset_replay"
        reason = "missing paths appear low-bias and available subset is sufficiently large"
    elif any(k in cause_counts for k in {"symbol_code_mismatch", "delisted_or_code_changed", "non_equity_or_fund_instrument"}):
        decision = "needs_manual_mapping"
        reason = "missing paths require symbol or instrument mapping review"
    else:
        decision = "insufficient_path_data"
        reason = "missing paths are too large or biased for replay"

    cause_summary = {
        "missing_path_trade_count": len(audit_rows),
        "suspected_cause_counts": dict(cause_counts),
        "repair_action_counts": dict(repair_counts),
        "repairable_count": sum(v for k, v in repair_counts.items() if k != "not_repairable"),
    }
    bias = {
        "available_path_group": available_summary,
        "missing_path_group": missing_summary,
        "bias_classification": bias_class,
        "reason": bias_reason,
    }
    recommendations = {
        "decision": decision,
        "reason": reason,
        "repair_actions": dict(repair_counts),
        "recommended_next_action": "repair_pan_history_or_bar_mapping_before_exit_replay" if decision == "repair_paths_before_replay" else "draft_available_path_subset_contract",
        "do_not_run_exit_replay_yet": decision != "approve_available_path_subset_replay",
    }
    audit = {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "reason": reason,
        "source_feasibility_root": str(FEASIBILITY_ROOT),
        "source_counterfactual_root": str(PRIOR_ROOT),
        "missing_path_cause_summary": cause_summary,
        "available_vs_missing_path_bias_check": bias,
        "path_repair_recommendations": recommendations,
        "boundary_no_lookahead_check": {
            "exit_rule_tested": False,
            "post_entry_strategy_decision_selected": False,
            "yahoo_or_provisional_fallback_used": False,
            "tainted_trades_included": False,
            "long_trades_included": False,
            "outcomes_used_only_for_bias_coverage_diagnostics": True,
        },
    }

    write_json(run_root / "short_exit_path_bar_gap_audit.json", audit)
    write_csv(run_root / "missing_path_trade_audit.csv", audit_rows, AUDIT_FIELDS)
    write_json(run_root / "missing_path_cause_summary.json", cause_summary)
    write_json(run_root / "available_vs_missing_path_bias_check.json", bias)
    write_json(run_root / "path_repair_recommendations.json", recommendations)
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision,
        "complete": True,
        "required_artifacts": [
            "short_exit_path_bar_gap_audit.json",
            "missing_path_trade_audit.csv",
            "missing_path_cause_summary.json",
            "available_vs_missing_path_bias_check.json",
            "path_repair_recommendations.json",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": decision, "missing": len(audit_rows)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
