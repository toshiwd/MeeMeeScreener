from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.tradex_high_zone_intraday_add_v1 import intraday_trigger
    from scripts.tradex_short_state_transition_replay_v1 import HORIZONS, load_events
except ModuleNotFoundError:
    from tradex_high_zone_intraday_add_v1 import intraday_trigger
    from tradex_short_state_transition_replay_v1 import HORIZONS, load_events


AXIS_ID = "tradex_high_zone_three_stage_entry_v1"
FAMILY = "high_zone_climax"
POLICIES = {
    "next_open_all": (1.00, 0.00, 0.00),
    "starter25_early25_strong50": (0.25, 0.25, 0.50),
    "starter25_early50_strong25": (0.25, 0.50, 0.25),
    "starter50_early25_strong25": (0.50, 0.25, 0.25),
}


def _value(row: pd.Series, name: str) -> float | None:
    value = row.get(name)
    return None if value is None or pd.isna(value) else float(value)


def price_band(close: float) -> str:
    if close < 900: return "under_900"
    if close < 3000: return "900_to_3000"
    if close < 5000: return "3000_to_5000"
    if close < 10000: return "5000_to_10000"
    return "10000_and_over"


def strong_down_trigger(row: pd.Series, after_offset: int, max_wait: int = 7) -> tuple[int, float] | None:
    signal_close, signal_low = float(row["c"]), float(row["l"])
    five_pct_level = signal_close * 0.95
    for offset in range(max(1, after_offset), max_wait + 1):
        open_ = _value(row, f"o{offset}")
        high = _value(row, f"h{offset}")
        low = _value(row, f"l{offset}")
        close = _value(row, f"c{offset}")
        ma7 = _value(row, f"ma7{offset}")
        prior_high = float(row["h"]) if offset == 1 else _value(row, f"h{offset - 1}")
        if None in (open_, high, low, close, ma7, prior_high):
            continue
        if low <= five_pct_level:
            return offset, open_ if open_ <= five_pct_level else five_pct_level
        structural = close < signal_low and close < ma7 and high < prior_high
        if structural:
            return offset, close
    return None


def _legs(row: pd.Series, policy: str) -> tuple[list[tuple[int, float, float]], int | None, int | None]:
    starter, early_weight, strong_weight = POLICIES[policy]
    open1 = _value(row, "o1")
    if open1 is None:
        return [], None, None
    legs = [(1, open1, starter)]
    if policy == "next_open_all":
        return legs, None, None
    early = intraday_trigger(row, "early_failure")
    if early is None:
        return legs, None, None
    legs.append((early[0], early[1], early_weight))
    strong = strong_down_trigger(row, early[0])
    if strong is not None:
        legs.append((strong[0], strong[1], strong_weight))
    return legs, early[0], None if strong is None else strong[0]


def replay(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, source in events.iterrows():
        first_drop = next((i for i in range(1, 21) if (_value(source, f"l{i}") or float("inf")) <= float(source["c"]) * 0.95), None)
        for policy in POLICIES:
            legs, early_offset, strong_offset = _legs(source, policy)
            full_size = math.isclose(sum(weight for _, _, weight in legs), 1.0)
            record: dict[str, Any] = {
                "family": FAMILY, "code": str(source["code"]), "signal_ymd": int(source["signal_ymd"]),
                "signal_close": float(source["c"]), "price_band": price_band(float(source["c"])),
                "policy": policy, "state": "entry" if legs else "unavailable", "entry_offset": 1 if legs else None,
                "entry_price": legs[0][1] if legs else None, "wait_days": 1 if legs else None, "sideways_20d": False,
                "early_offset": early_offset, "strong_offset": strong_offset, "full_size": full_size,
                "drop5_event": first_drop is not None,
                "full_size_before_drop": bool(first_drop is not None and (policy == "next_open_all" or (strong_offset is not None and strong_offset <= first_drop))),
            }
            for horizon in HORIZONS:
                exit_day = 1 + horizon
                exit_close = _value(source, f"c{exit_day}")
                active = [(offset, price, weight) for offset, price, weight in legs if offset <= exit_day]
                record[f"ret{horizon}"] = None if exit_close is None or not active else sum(weight * (1 - exit_close / price) for _, price, weight in active)
                path = []
                for day in range(1, exit_day + 1):
                    high = _value(source, f"h{day}")
                    day_legs = [(offset, price, weight) for offset, price, weight in legs if offset <= day]
                    if high is not None and day_legs:
                        path.append(sum(weight * (1 - high / price) for _, price, weight in day_legs))
                record[f"mae{horizon}"] = min(path) if path else None
            rows.append(record)
    return pd.DataFrame(rows)


def _pf(values: pd.Series) -> float | None:
    losses = -values[values < 0].sum()
    return None if losses <= 0 else float(values[values > 0].sum() / losses)


def metrics(frame: pd.DataFrame) -> dict[str, Any]:
    entered, drops = frame[frame.state == "entry"], frame[(frame.state == "entry") & frame.drop5_event]
    result = {
        "signal_count": int(len(frame)), "entry_count": int(len(entered)), "participation_capture_rate": float(len(entered) / len(frame)),
        "early_trigger_rate": float(entered.early_offset.notna().mean()), "strong_trigger_rate": float(entered.strong_offset.notna().mean()),
        "full_size_rate": float(entered.full_size.mean()), "drop5_event_count": int(len(drops)),
        "drop5_participation_capture_rate": 1.0 if len(drops) else None,
        "full_size_before_drop_rate": float(drops.full_size_before_drop.mean()) if len(drops) else None,
    }
    for horizon in HORIZONS:
        values, adverse = entered[f"ret{horizon}"].dropna(), entered[f"mae{horizon}"].dropna()
        result[f"h{horizon}"] = {"n": int(len(values)), "mean": float(values.mean()), "median": float(values.median()), "win_rate": float((values > 0).mean()), "profit_factor": _pf(values), "loss_le_minus5_rate": float((values <= -0.05).mean()), "loss_le_minus10_rate": float((values <= -0.10).mean()), "mean_mae": float(adverse.mean()), "worst_mae": float(adverse.min())}
    return result


def stability(frame: pd.DataFrame) -> dict[str, Any]:
    part = frame.copy(); part["year"] = part.signal_ymd.astype(str).str[:4]; part["month"] = part.signal_ymd.astype(str).str[:6]
    yearly = [{"year": key, **metrics(group)} for key, group in part.groupby("year")]
    monthly = [{"month": key, **metrics(group)} for key, group in part.groupby("month")]
    bands = [{"price_band": key, **metrics(group)} for key, group in part.groupby("price_band")]
    ey = [x for x in yearly if x["entry_count"] >= 5]; em = [x for x in monthly if x["entry_count"] >= 5]; eb = [x for x in bands if x["entry_count"] >= 10]
    return {"yearly": yearly, "eligible_year_count": len(ey), "positive_mean_ret10_year_rate": sum((x["h10"]["mean"] or 0) > 0 for x in ey) / len(ey) if ey else None, "eligible_month_count": len(em), "positive_mean_ret10_month_rate": sum((x["h10"]["mean"] or 0) > 0 for x in em) / len(em) if em else None, "by_price_band": bands, "eligible_price_band_count": len(eb), "positive_mean_ret10_price_band_rate": sum((x["h10"]["mean"] or 0) > 0 for x in eb) / len(eb) if eb else None}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict): return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list): return [_json_ready(v) for v in value]
    if isinstance(value, Path): return str(value)
    if hasattr(value, "item"): return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)): return None
    return value


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int) -> Path:
    events = load_events(db_path, start_ymd, end_ymd); events = events[events.family == FAMILY].copy(); ledger = replay(events)
    results = {p: metrics(ledger[ledger.policy == p]) for p in POLICIES}; stable = {p: stability(ledger[ledger.policy == p]) for p in POLICIES}; baseline = results["next_open_all"]
    challengers = {}
    for policy in list(POLICIES)[1:]:
        item = results[policy]
        checks = {"drop5_capture_at_least_90pct": item["drop5_participation_capture_rate"] >= 0.90, "full_size_before_drop_at_least_75pct": item["full_size_before_drop_rate"] >= 0.75, "mean_ret10_not_worse": item["h10"]["mean"] >= baseline["h10"]["mean"], "pf10_not_worse": (item["h10"]["profit_factor"] or 0) >= (baseline["h10"]["profit_factor"] or 0), "loss10_rate_not_worse": item["h10"]["loss_le_minus10_rate"] <= baseline["h10"]["loss_le_minus10_rate"], "positive_year_rate_at_least_75pct": stable[policy]["positive_mean_ret10_year_rate"] >= 0.75, "positive_price_band_rate_at_least_75pct": stable[policy]["positive_mean_ret10_price_band_rate"] >= 0.75}
        decision = "keep" if all(checks.values()) else ("hold" if checks["drop5_capture_at_least_90pct"] and checks["pf10_not_worse"] else "drop")
        challengers[policy] = {"candidate_local_decision": decision, "checks": checks, "metrics": item, "stability": stable[policy]}
    keepers = [p for p,x in challengers.items() if x["candidate_local_decision"] == "keep"]; holds = [p for p,x in challengers.items() if x["candidate_local_decision"] == "hold"]; pool = keepers or holds
    leader = max(pool, key=lambda p: results[p]["h10"]["mean"]) if pool else None; decision = "keep" if keepers else ("hold" if holds else "drop")
    payload = {"schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment", "fixed_evaluation_conditions": {"universe": "same high_zone_climax top5/day signals", "period": {"start_ymd": start_ymd, "end_ymd": end_ymd}, "changed_axis": "three-stage exposure path only; signal membership fixed", "stages": "starter next open; early GU-failure/signal-low touch; strong 5pct drop or close below signal low and MA7 with lower high", "horizons": list(HORIZONS), "costs": "ignored_by_user_request", "runtime_db_write": False, "meemee_reflection": False}, "source": {"db_path": str(db_path), "event_count": int(len(events)), "ledger_count": int(len(ledger))}, "baseline": {"policy": "next_open_all", "metrics": baseline, "stability": stable["next_open_all"]}, "challengers": challengers, "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "all signals receive starter; only exposure path changes"}, "decision": {"candidate_local_decision": decision, "session_aggregate_decision": decision, "authoritative_rollup_decision": f"{decision}_high_zone_three_stage_entry", "selected_policy": leader if decision == "keep" else None, "research_leader": leader, "reason_type": "capture_expectancy_tail_and_stability_pass" if decision == "keep" else ("pf_improves_but_one_or_more_absolute_gates_fail" if decision == "hold" else "three_stage_does_not_improve_tradeoff")}, "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False}
    run_dir = output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"; run_dir.mkdir(parents=True, exist_ok=False)
    ledger.to_parquet(run_dir / "three_stage_ledger.parquet", index=False); (run_dir / "compare.json").write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"); (run_dir / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"status": "complete", "required_files": ["compare.json", "three_stage_ledger.parquet", "_ARTIFACT_COMPLETE.json"]}, indent=2) + "\n", encoding="utf-8"); return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--db-path", type=Path, required=True); parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_high_zone_three_stage_entry_v1")); parser.add_argument("--start-ymd", type=int, default=20160101); parser.add_argument("--end-ymd", type=int, default=20260617); args = parser.parse_args(); print(run(args.db_path,args.output_root,args.start_ymd,args.end_ymd)); return 0


if __name__ == "__main__": raise SystemExit(main())
