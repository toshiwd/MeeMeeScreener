from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_short_ma7_reclaim_effect_decomposition_v1"
PRIOR_ROOT = Path(r"G:\Tradex\actual_trade_short_exit_rule_replay_v1\20260512T021243Z-actual_trade_short_ma7_reclaim_exit_v1")
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
OUT_BASE = Path(r"G:\Tradex\actual_trade_short_exit_rule_diagnostics_v1")


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


def date_to_epoch(d: date) -> int:
    return int(datetime.combine(d, time.min, tzinfo=timezone.utc).timestamp())


def epoch_to_date(value: int) -> date:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).date()


def fnum(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def avg(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def ma(values: list[float], n: int) -> float | None:
    return avg(values[-n:]) if len(values) >= n else None


def slope(values: list[float], n: int, lag: int = 5) -> float | None:
    if len(values) < n + lag:
        return None
    cur = avg(values[-n:])
    prev = avg(values[-n - lag : -lag])
    return cur / prev - 1.0 if cur is not None and prev not in (None, 0) else None


def fetch_bars(symbols: set[str], max_exit: date) -> dict[str, list[dict[str, Any]]]:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        placeholders = ",".join(["?"] * len(symbols))
        rows = con.execute(
            f"""
            SELECT code, date, o, h, l, c, v, source
            FROM daily_bars
            WHERE source = 'pan'
              AND code IN ({placeholders})
              AND date <= ?
            ORDER BY code, date
            """,
            [*sorted(symbols), date_to_epoch(max_exit)],
        ).fetchall()
    finally:
        con.close()
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for code, d, o, h, l, c, v, source in rows:
        out[str(code)].append(
            {"date": int(d), "d": epoch_to_date(int(d)), "o": o, "h": h, "l": l, "c": c, "v": v, "source": source}
        )
    return out


def build_path(row: dict[str, Any], bars_by_symbol: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    bars = bars_by_symbol.get(str(row["symbol"]), [])
    entry_date = parse_d(row["entry_date"])
    exit_date = parse_d(row["actual_exit_date"])
    prior_entry = [b for b in bars if b["d"] <= entry_date]
    prior_exit = [b for b in bars if b["d"] <= exit_date]
    if not prior_entry or not prior_exit:
        return []
    start_idx = bars.index(prior_entry[-1])
    end_idx = bars.index(prior_exit[-1])
    closes = [float(b["c"]) for b in bars[:start_idx]]
    entry_price = fnum(row, "entry_price") or 0.0
    entry_bar_close = float(bars[start_idx]["c"])
    scale = entry_price / entry_bar_close if entry_bar_close else 1.0
    out: list[dict[str, Any]] = []
    for idx, b in enumerate(bars[start_idx : end_idx + 1]):
        close_raw = float(b["c"])
        close = close_raw * scale
        closes.append(close_raw)
        ma7_raw = ma(closes, 7)
        ma20_raw = ma(closes, 20)
        ma60_raw = ma(closes, 60)
        ma7 = ma7_raw * scale if ma7_raw is not None else None
        ma20 = ma20_raw * scale if ma20_raw is not None else None
        ma60 = ma60_raw * scale if ma60_raw is not None else None
        short_ret = entry_price / close - 1.0 if close else None
        out.append(
            {
                "path_date": b["d"].isoformat(),
                "path_day_index": idx,
                "close": close,
                "close_raw": close_raw,
                "ma7": ma7,
                "ma20": ma20,
                "ma60": ma60,
                "close_vs_ma7_pct": close / ma7 - 1.0 if ma7 not in (None, 0) else None,
                "close_vs_ma20_pct": close / ma20 - 1.0 if ma20 not in (None, 0) else None,
                "ma7_reclaim_flag_for_short": close > ma7 if ma7 is not None else None,
                "ma20_reclaim_flag_for_short": close > ma20 if ma20 is not None else None,
                "unrealized_return_for_short": short_ret,
                "unrealized_pnl_for_short": (entry_price - close) * (fnum(row, "quantity") or 0.0),
                "ma7_slope_5d": slope(closes, 7),
                "ma20_slope_5d": slope(closes, 20),
            }
        )
    return out


def holding_bucket(days: int) -> str:
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


def reclaim_day_bucket(value: Any) -> str:
    if value in (None, ""):
        return "no_reclaim"
    day = int(float(value))
    if day <= 3:
        return "1_3d"
    if day <= 7:
        return "4_7d"
    if day <= 14:
        return "8_14d"
    if day <= 30:
        return "15_30d"
    return "31d_plus"


def trigger_return_bucket(value: float | None) -> str:
    if value is None:
        return "no_trigger"
    if value > 0.002:
        return "profitable_at_trigger"
    if value < -0.002:
        return "losing_at_trigger"
    return "flat_at_trigger"


def favorable_bucket(value: float | None) -> str:
    if value is None or value <= 0:
        return "no_profit"
    if value < 0.02:
        return "small_profit"
    if value < 0.05:
        return "medium_profit"
    return "large_profit"


def summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "trade_count": 0,
            "gross_pnl_actual_total": 0.0,
            "sim_gross_pnl_total": 0.0,
            "pnl_delta_total": 0.0,
            "avg_sim_minus_actual_pnl": None,
            "median_sim_minus_actual_pnl": None,
            "win_rate_actual": None,
            "win_rate_sim": None,
            "avg_holding_days_actual": None,
            "avg_holding_days_sim": None,
            "large_loss_count_actual": 0,
            "large_loss_count_sim": 0,
        }
    actual = [fnum(r, "gross_pnl_actual") or 0.0 for r in rows]
    sim = [fnum(r, "sim_gross_pnl") or 0.0 for r in rows]
    deltas = [fnum(r, "sim_minus_actual_pnl") or 0.0 for r in rows]
    holds_actual = [int(float(r.get("holding_days_actual") or 0)) for r in rows]
    holds_sim = [int(float(r.get("holding_days_simulated") or 0)) for r in rows]
    actual_ret = [fnum(r, "actual_return") or 0.0 for r in rows]
    sim_ret = [fnum(r, "sim_return") or 0.0 for r in rows]
    return {
        "trade_count": len(rows),
        "gross_pnl_actual_total": sum(actual),
        "sim_gross_pnl_total": sum(sim),
        "pnl_delta_total": sum(deltas),
        "avg_sim_minus_actual_pnl": mean(deltas),
        "median_sim_minus_actual_pnl": median(deltas),
        "win_rate_actual": sum(1 for p in actual if p > 0) / len(rows),
        "win_rate_sim": sum(1 for p in sim if p > 0) / len(rows),
        "avg_holding_days_actual": mean(holds_actual),
        "avg_holding_days_sim": mean(holds_sim),
        "large_loss_count_actual": sum(1 for r in actual_ret if r <= -0.05),
        "large_loss_count_sim": sum(1 for r in sim_ret if r <= -0.05),
    }


def group_summary(rows: list[dict[str, Any]], key_fn) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row))].append(row)
    return {k: summary(v) for k, v in sorted(groups.items())}


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    prior_summary = json.loads((PRIOR_ROOT / "short_ma7_reclaim_exit_summary.json").read_text(encoding="utf-8"))
    prior_decision = json.loads((PRIOR_ROOT / "short_ma7_reclaim_exit_decision.json").read_text(encoding="utf-8"))
    prior_no = json.loads((PRIOR_ROOT / "no_lookahead_audit.json").read_text(encoding="utf-8"))
    subset_contract = json.loads((PRIOR_ROOT / "available_path_subset_contract_used.json").read_text(encoding="utf-8"))
    trade_rows = read_csv(PRIOR_ROOT / "short_ma7_reclaim_exit_trade_rows.csv")

    max_exit = max(parse_d(r["actual_exit_date"]) for r in trade_rows)
    bars_by_symbol = fetch_bars({r["symbol"] for r in trade_rows}, max_exit)

    enriched: list[dict[str, Any]] = []
    insufficient_count = 0
    for row in trade_rows:
        path = build_path(row, bars_by_symbol)
        if not path:
            insufficient_count += 1
            continue
        trigger_idx = int(float(row["ma7_reclaim_day_index"])) if row.get("ma7_reclaim_day_index") not in (None, "") else None
        trigger = path[trigger_idx] if trigger_idx is not None and trigger_idx < len(path) else None
        before = [p for p in path if trigger_idx is None or p["path_day_index"] <= trigger_idx]
        full = path
        rets_before = [p["unrealized_return_for_short"] for p in before if p["unrealized_return_for_short"] is not None]
        rets_full = [p["unrealized_return_for_short"] for p in full if p["unrealized_return_for_short"] is not None]
        max_fav_before = max(rets_before) if rets_before else None
        max_adv_before = min(rets_before) if rets_before else None
        max_fav_full = max(rets_full) if rets_full else None
        max_adv_full = min(rets_full) if rets_full else None
        day_fav_before = next((p["path_day_index"] for p in before if p["unrealized_return_for_short"] == max_fav_before), None)
        day_adv_before = next((p["path_day_index"] for p in before if p["unrealized_return_for_short"] == max_adv_before), None)
        day_fav_full = next((p["path_day_index"] for p in full if p["unrealized_return_for_short"] == max_fav_full), None)
        day_adv_full = next((p["path_day_index"] for p in full if p["unrealized_return_for_short"] == max_adv_full), None)
        trigger_ret = trigger.get("unrealized_return_for_short") if trigger else None
        return_from_best_to_trigger = (trigger_ret - max_fav_before) if trigger_ret is not None and max_fav_before is not None else None
        actual_pnl = fnum(row, "gross_pnl_actual") or 0.0
        sim_pnl = fnum(row, "sim_gross_pnl") or 0.0
        delta = fnum(row, "sim_minus_actual_pnl") or 0.0
        actual_ret = fnum(row, "gross_return_actual") or 0.0
        sim_ret = fnum(row, "sim_gross_return_pct") or 0.0
        effect = "improved_trade" if delta > 0 else "worsened_trade" if delta < 0 else "unchanged_trade"
        became_prof_before_trigger = any((r or 0) > 0 for r in rets_before)
        became_prof_full = any((r or 0) > 0 for r in rets_full)
        days_profitable_before = sum(1 for r in rets_before if (r or 0) > 0)
        post_trigger_rets = [p["unrealized_return_for_short"] for p in full if trigger_idx is not None and p["path_day_index"] > trigger_idx and p["unrealized_return_for_short"] is not None]
        post_trigger_best = max(post_trigger_rets) if post_trigger_rets else None
        post_trigger_improvement = (post_trigger_best - trigger_ret) if post_trigger_best is not None and trigger_ret is not None else None
        special: list[str] = []
        if actual_ret <= -0.05 and sim_ret > actual_ret:
            special.append("large_loss_reduced")
        if actual_pnl > 0 and sim_pnl < actual_pnl:
            special.append("winner_cut_short")
        if -50000 < actual_pnl < 0 and sim_pnl < actual_pnl:
            special.append("small_loss_accelerated")
        if actual_pnl < 0 and became_prof_before_trigger and delta > 0:
            special.append("profit_given_back_prevented")
        if row.get("ma7_reclaim_triggered", "").lower() == "true" and delta < 0 and post_trigger_improvement is not None and post_trigger_improvement > 0.02:
            special.append("false_alarm_reclaim")
        if not special:
            special.append("none")
        out = {
            "normalized_trade_id": row["normalized_trade_id"],
            "symbol": row["symbol"],
            "entry_date": row["entry_date"],
            "actual_exit_date": row["actual_exit_date"],
            "entry_price": fnum(row, "entry_price"),
            "actual_exit_price": fnum(row, "actual_exit_price"),
            "sim_exit_date": row["sim_exit_date"],
            "sim_exit_price": fnum(row, "sim_exit_price"),
            "gross_pnl_actual": actual_pnl,
            "sim_gross_pnl": sim_pnl,
            "sim_minus_actual_pnl": delta,
            "actual_return": actual_ret,
            "sim_return": sim_ret,
            "holding_days_actual": int(float(row.get("holding_days_actual") or 0)),
            "holding_days_simulated": int(float(row.get("holding_days_simulated") or 0)),
            "ma7_reclaim_triggered": row.get("ma7_reclaim_triggered", "").lower() == "true",
            "ma7_reclaim_day_index": trigger_idx,
            "entry_day_already_above_ma7": row.get("entry_day_already_above_ma7", "").lower() == "true",
            "trigger_close_vs_ma7_pct": trigger.get("close_vs_ma7_pct") if trigger else None,
            "trigger_close_vs_ma20_pct": trigger.get("close_vs_ma20_pct") if trigger else None,
            "trigger_ma20_reclaimed_flag": trigger.get("ma20_reclaim_flag_for_short") if trigger else None,
            "trigger_unrealized_pnl": trigger.get("unrealized_pnl_for_short") if trigger else None,
            "trigger_unrealized_return": trigger_ret,
            "max_favorable_return_before_trigger": max_fav_before,
            "max_adverse_return_before_trigger": max_adv_before,
            "became_profitable_before_trigger": became_prof_before_trigger,
            "days_profitable_before_trigger": days_profitable_before,
            "day_of_max_favorable_before_trigger": day_fav_before,
            "day_of_max_adverse_before_trigger": day_adv_before,
            "profit_given_back_at_trigger": -return_from_best_to_trigger if return_from_best_to_trigger is not None and return_from_best_to_trigger < 0 else 0.0,
            "return_from_best_to_trigger": return_from_best_to_trigger,
            "max_favorable_return_to_actual_exit": max_fav_full,
            "max_adverse_return_to_actual_exit": max_adv_full,
            "became_profitable_before_actual_exit": became_prof_full,
            "day_of_max_favorable": day_fav_full,
            "day_of_max_adverse": day_adv_full,
            "ma20_reclaim_before_actual_exit": any(p["ma20_reclaim_flag_for_short"] is True for p in full),
            "ma7_reclaim_before_actual_exit": any(p["ma7_reclaim_flag_for_short"] is True for p in full),
            "short_ma20_status": row.get("short_ma20_entry_status") or "short_ma20_unknown",
            "close_vs_ma20_pct": fnum(row, "close_vs_ma20_pct"),
            "ma20_slope_5d": fnum(row, "ma20_slope_5d"),
            "holding_days_actual_bucket": holding_bucket(int(float(row.get("holding_days_actual") or 0))),
            "ma7_reclaim_day_bucket": reclaim_day_bucket(trigger_idx),
            "effect_classification": effect,
            "special_group_labels": "|".join(special),
            "false_alarm_reclaim": "false_alarm_reclaim" in special,
            "winner_cut_short": "winner_cut_short" in special,
            "large_loss_reduced": "large_loss_reduced" in special,
            "profit_given_back_prevented": "profit_given_back_prevented" in special,
        }
        enriched.append(out)

    improved = [r for r in enriched if r["effect_classification"] == "improved_trade"]
    worsened = [r for r in enriched if r["effect_classification"] == "worsened_trade"]
    unchanged = [r for r in enriched if r["effect_classification"] == "unchanged_trade"]
    large_loss_reductions = [r for r in enriched if r["large_loss_reduced"]]
    false_alarms = [r for r in enriched if r["false_alarm_reclaim"]]
    winner_cut = [r for r in enriched if r["winner_cut_short"]]

    large_loss_findings = {
        "count": len(large_loss_reductions),
        "total_loss_avoided": sum(r["sim_minus_actual_pnl"] for r in large_loss_reductions),
        "average_ma7_reclaim_day": mean([r["ma7_reclaim_day_index"] for r in large_loss_reductions if r["ma7_reclaim_day_index"] is not None]) if large_loss_reductions else None,
        "average_max_favorable_return_before_trigger": mean([r["max_favorable_return_before_trigger"] for r in large_loss_reductions if r["max_favorable_return_before_trigger"] is not None]) if large_loss_reductions else None,
        "proportion_became_profitable_before_trigger": sum(1 for r in large_loss_reductions if r["became_profitable_before_trigger"]) / len(large_loss_reductions) if large_loss_reductions else None,
        "proportion_trigger_after_day_5": sum(1 for r in large_loss_reductions if (r["ma7_reclaim_day_index"] or 0) > 5) / len(large_loss_reductions) if large_loss_reductions else None,
        "proportion_trigger_after_day_10": sum(1 for r in large_loss_reductions if (r["ma7_reclaim_day_index"] or 0) > 10) / len(large_loss_reductions) if large_loss_reductions else None,
        "proportion_trigger_after_day_15": sum(1 for r in large_loss_reductions if (r["ma7_reclaim_day_index"] or 0) > 15) / len(large_loss_reductions) if large_loss_reductions else None,
        "proportion_ma20_reclaimed_at_trigger": sum(1 for r in large_loss_reductions if r["trigger_ma20_reclaimed_flag"] is True) / len(large_loss_reductions) if large_loss_reductions else None,
    }
    worsened_findings = {
        "count": len(worsened),
        "total_opportunity_cost": sum(abs(r["sim_minus_actual_pnl"]) for r in worsened),
        "average_ma7_reclaim_day": mean([r["ma7_reclaim_day_index"] for r in worsened if r["ma7_reclaim_day_index"] is not None]) if worsened else None,
        "proportion_winner_cut_short": len(winner_cut) / len(worsened) if worsened else None,
        "proportion_false_alarm_reclaim": len(false_alarms) / len(worsened) if worsened else None,
        "average_post_trigger_improvement_before_actual_exit": mean([
            r["max_favorable_return_to_actual_exit"] - (r["trigger_unrealized_return"] or 0.0)
            for r in worsened
            if r["trigger_unrealized_return"] is not None and r["max_favorable_return_to_actual_exit"] is not None
        ]) if worsened else None,
        "proportion_trigger_day_1_to_3": sum(1 for r in worsened if r["ma7_reclaim_day_bucket"] == "1_3d") / len(worsened) if worsened else None,
    }
    time_profit_summary = {
        "by_ma7_reclaim_day_bucket": group_summary(enriched, lambda r: r["ma7_reclaim_day_bucket"]),
        "by_trigger_unrealized_return": group_summary(enriched, lambda r: trigger_return_bucket(r["trigger_unrealized_return"])),
        "by_max_favorable_return_before_trigger": group_summary(enriched, lambda r: favorable_bucket(r["max_favorable_return_before_trigger"])),
        "by_day_and_trigger_return": group_summary(enriched, lambda r: f"{r['ma7_reclaim_day_bucket']}|{trigger_return_bucket(r['trigger_unrealized_return'])}"),
        "by_profit_and_trigger_return": group_summary(enriched, lambda r: f"{favorable_bucket(r['max_favorable_return_before_trigger'])}|{trigger_return_bucket(r['trigger_unrealized_return'])}"),
    }

    early_worsened_rate = worsened_findings["proportion_trigger_day_1_to_3"] or 0.0
    large_loss_late_rate = large_loss_findings["proportion_trigger_after_day_5"] or 0.0
    large_loss_profit_rate = large_loss_findings["proportion_became_profitable_before_trigger"] or 0.0
    false_alarm_rate = worsened_findings["proportion_false_alarm_reclaim"] or 0.0
    ma20_at_trigger_rate = large_loss_findings["proportion_ma20_reclaimed_at_trigger"] or 0.0
    if insufficient_count:
        decision = "needs_more_fields"
        reason = f"{insufficient_count} rows lacked reconstructable path context"
    elif large_loss_profit_rate >= 0.55 and false_alarm_rate < 0.55:
        decision = "test_profit_then_ma_reclaim_next"
        reason = "large-loss reductions mostly had prior favorable excursion before MA7 reclaim"
    elif early_worsened_rate >= 0.45 and large_loss_late_rate >= 0.45:
        decision = "test_time_plus_ma_reclaim_next"
        reason = "early MA7 exits explain many worsened trades while later triggers reduce large losses"
    elif ma20_at_trigger_rate >= 0.60:
        decision = "test_ma20_reclaim_next"
        reason = "large-loss reductions frequently had MA20 reclaimed at trigger"
    else:
        decision = "drop_ma_reclaim_family"
        reason = "improved and worsened groups do not show a separable MA reclaim structure"

    payload = {
        "candidate_name": CANDIDATE_NAME,
        "source_replay_root": str(PRIOR_ROOT),
        "prior_decision": prior_decision.get("decision"),
        "result_scope": "available-path subset diagnostic",
        "trade_count": len(enriched),
        "classification_counts": {
            "improved_trade": len(improved),
            "worsened_trade": len(worsened),
            "unchanged_trade": len(unchanged),
            "large_loss_reduced": len(large_loss_reductions),
            "winner_cut_short": len(winner_cut),
            "false_alarm_reclaim": len(false_alarms),
        },
        "improved_vs_worsened": {
            "improved": summary(improved),
            "worsened": summary(worsened),
            "unchanged": summary(unchanged),
        },
        "large_loss_reduction_findings": large_loss_findings,
        "worsened_findings": worsened_findings,
        "boundary_no_lookahead_check": {
            "new_exit_rule_tested": False,
            "threshold_selected_as_rule": False,
            "outcomes_used_only_for_diagnostics": True,
            "only_320_available_path_subset_trades_used": len(enriched) == 320,
            "missing_71_excluded": True,
            "long_or_tainted_trades_included": False,
            "yahoo_or_provisional_bars_used": False,
            "full_ledger_claim_made": False,
            "prior_no_lookahead_pass": bool(prior_no.get("pass")),
        },
        "prior_summary_snapshot": {
            "actual_subset_baseline": prior_summary.get("actual_subset_baseline"),
            "simulated_ma7_reclaim_exit": prior_summary.get("simulated_ma7_reclaim_exit"),
            "deltas": prior_summary.get("deltas"),
        },
        "subset_contract_excluded_count": subset_contract.get("excluded_missing_trade_count"),
    }
    decision_payload = {
        "candidate_name": CANDIDATE_NAME,
        "decision": decision,
        "reason": reason,
        "dominant_evidence": {
            "large_loss_profit_rate": large_loss_profit_rate,
            "early_worsened_rate": early_worsened_rate,
            "large_loss_late_rate": large_loss_late_rate,
            "false_alarm_rate": false_alarm_rate,
            "ma20_at_trigger_rate": ma20_at_trigger_rate,
        },
        "result_scope": "available-path subset diagnostic",
        "next_rule_not_implemented": True,
    }

    write_json(run_root / "short_ma7_reclaim_effect_decomposition.json", payload)
    write_json(run_root / "short_ma7_reclaim_effect_decomposition_decision.json", decision_payload)
    write_csv(run_root / "short_ma7_reclaim_improved_trades.csv", improved)
    write_csv(run_root / "short_ma7_reclaim_worsened_trades.csv", worsened)
    write_csv(run_root / "short_ma7_reclaim_large_loss_reductions.csv", large_loss_reductions)
    write_csv(run_root / "short_ma7_reclaim_false_alarms.csv", false_alarms)
    write_json(run_root / "short_ma7_reclaim_time_profit_bucket_summary.json", time_profit_summary)
    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": decision,
        "complete": True,
        "required_artifacts": [
            "short_ma7_reclaim_effect_decomposition.json",
            "short_ma7_reclaim_effect_decomposition_decision.json",
            "short_ma7_reclaim_improved_trades.csv",
            "short_ma7_reclaim_worsened_trades.csv",
            "short_ma7_reclaim_large_loss_reductions.csv",
            "short_ma7_reclaim_false_alarms.csv",
            "short_ma7_reclaim_time_profit_bucket_summary.json",
        ],
    }
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)
    print(json.dumps({"run_root": str(run_root), "decision": decision, "improved": len(improved), "worsened": len(worsened)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
