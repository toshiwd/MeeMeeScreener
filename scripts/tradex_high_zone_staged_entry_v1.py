from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.tradex_short_state_transition_replay_v1 import HORIZONS, load_events
except ModuleNotFoundError:
    from tradex_short_state_transition_replay_v1 import HORIZONS, load_events


AXIS_ID = "tradex_high_zone_staged_entry_v1"
FAMILY = "high_zone_climax"
POLICIES = {
    "next_open_all": (1.00, 0.00),
    "starter25_confirm75": (0.25, 0.75),
    "starter50_confirm50": (0.50, 0.50),
}


def _value(row: pd.Series, name: str) -> float | None:
    value = row.get(name)
    return None if value is None or pd.isna(value) else float(value)


def confirmation_offset(row: pd.Series, max_wait: int = 5) -> int | None:
    signal_low = float(row["l"])
    for offset in range(1, max_wait + 1):
        values = {name: _value(row, f"{name}{offset}") for name in ("o", "h", "l", "c")}
        prior_close = float(row["c"]) if offset == 1 else _value(row, f"c{offset - 1}")
        prior_high = float(row["h"]) if offset == 1 else _value(row, f"h{offset - 1}")
        if any(value is None for value in (*values.values(), prior_close, prior_high)):
            continue
        span = max(values["h"] - values["l"], 1e-9)
        close_pos = (values["c"] - values["l"]) / span
        direct_break = values["c"] < signal_low
        structured_weakness = values["c"] < values["o"] and values["c"] < prior_close and values["h"] < prior_high and close_pos <= 0.50
        if direct_break or structured_weakness:
            return offset
    return None


def _legs(row: pd.Series, policy: str) -> list[tuple[int, float, float]]:
    initial, add = POLICIES[policy]
    open1 = _value(row, "o1")
    if open1 is None:
        return []
    legs = [(1, open1, initial)]
    offset = confirmation_offset(row)
    if add > 0 and offset is not None:
        price = _value(row, f"c{offset}")
        if price is not None:
            legs.append((offset, price, add))
    return legs


def replay(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, source in events.iterrows():
        confirm = confirmation_offset(source)
        future_lows = [_value(source, f"l{i}") for i in range(1, 21)]
        first_drop = next((i for i, low in enumerate(future_lows, 1) if low is not None and low <= float(source["c"]) * 0.95), None)
        for policy in POLICIES:
            legs = _legs(source, policy)
            record: dict[str, Any] = {
                "family": FAMILY,
                "code": str(source["code"]),
                "signal_ymd": int(source["signal_ymd"]),
                "policy": policy,
                "state": "entry" if legs else "unavailable",
                "entry_offset": 1 if legs else None,
                "entry_price": legs[0][1] if legs else None,
                "wait_days": 1 if legs else None,
                "sideways_20d": False,
                "confirmation_offset": confirm,
                "full_size": math.isclose(sum(weight for _, _, weight in legs), 1.0),
                "drop5_event": first_drop is not None,
                "full_size_before_drop": bool(first_drop is not None and (policy == "next_open_all" or (confirm is not None and confirm <= first_drop))),
            }
            for horizon in HORIZONS:
                exit_day = 1 + horizon
                exit_close = _value(source, f"c{exit_day}")
                active = [(offset, price, weight) for offset, price, weight in legs if offset <= exit_day]
                record[f"ret{horizon}"] = None if exit_close is None or not active else sum(weight * (1.0 - exit_close / price) for _, price, weight in active)
                daily_pnl = []
                for day in range(1, exit_day + 1):
                    high = _value(source, f"h{day}")
                    day_legs = [(offset, price, weight) for offset, price, weight in legs if offset <= day]
                    if high is not None and day_legs:
                        daily_pnl.append(sum(weight * (1.0 - high / price) for _, price, weight in day_legs))
                record[f"mae{horizon}"] = min(daily_pnl) if daily_pnl else None
            rows.append(record)
    return pd.DataFrame(rows)


def _pf(values: pd.Series) -> float | None:
    gains = values[values > 0].sum()
    losses = -values[values < 0].sum()
    return None if losses <= 0 else float(gains / losses)


def metrics(frame: pd.DataFrame) -> dict[str, Any]:
    entered = frame[frame.state == "entry"]
    drop_events = entered[entered.drop5_event]
    result = {
        "signal_count": int(len(frame)),
        "entry_count": int(len(entered)),
        "participation_capture_rate": float(len(entered) / len(frame)) if len(frame) else None,
        "confirmation_rate": float(entered.confirmation_offset.notna().mean()) if len(entered) else None,
        "full_size_rate": float(entered.full_size.mean()) if len(entered) else None,
        "drop5_event_count": int(len(drop_events)),
        "drop5_participation_capture_rate": 1.0 if len(drop_events) else None,
        "full_size_before_drop_rate": float(drop_events.full_size_before_drop.mean()) if len(drop_events) else None,
    }
    for horizon in HORIZONS:
        values = entered[f"ret{horizon}"].dropna()
        adverse = entered[f"mae{horizon}"].dropna()
        result[f"h{horizon}"] = {
            "n": int(len(values)), "mean": float(values.mean()), "median": float(values.median()),
            "win_rate": float((values > 0).mean()), "profit_factor": _pf(values),
            "loss_le_minus5_rate": float((values <= -0.05).mean()),
            "loss_le_minus10_rate": float((values <= -0.10).mean()),
            "mean_mae": float(adverse.mean()), "worst_mae": float(adverse.min()),
        }
    return result


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict): return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list): return [_json_ready(v) for v in value]
    if isinstance(value, Path): return str(value)
    if hasattr(value, "item"): return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)): return None
    return value


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int) -> Path:
    events = load_events(db_path, start_ymd, end_ymd)
    events = events[events.family == FAMILY].copy()
    ledger = replay(events)
    results = {policy: metrics(ledger[ledger.policy == policy]) for policy in POLICIES}
    baseline = results["next_open_all"]
    challengers = {}
    for policy in list(POLICIES)[1:]:
        item = results[policy]
        checks = {
            "participation_capture_at_least_90pct": (item["participation_capture_rate"] or 0) >= 0.90,
            "drop5_capture_at_least_90pct": (item["drop5_participation_capture_rate"] or 0) >= 0.90,
            "mean_ret10_not_worse": (item["h10"]["mean"] or -999) >= (baseline["h10"]["mean"] or -999),
            "pf10_not_worse": (item["h10"]["profit_factor"] or 0) >= (baseline["h10"]["profit_factor"] or 0),
            "loss10_rate_better": (item["h10"]["loss_le_minus10_rate"] or 1) < (baseline["h10"]["loss_le_minus10_rate"] or 1),
        }
        decision = "keep" if all(checks.values()) else ("hold" if checks["drop5_capture_at_least_90pct"] and checks["loss10_rate_better"] else "drop")
        challengers[policy] = {"candidate_local_decision": decision, "checks": checks, "metrics": item}
    keepers = [p for p, x in challengers.items() if x["candidate_local_decision"] == "keep"]
    holds = [p for p, x in challengers.items() if x["candidate_local_decision"] == "hold"]
    leader_pool = keepers or holds
    leader = max(leader_pool, key=lambda p: results[p]["h10"]["mean"]) if leader_pool else None
    decision = "keep" if keepers else ("hold" if holds else "drop")
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {"universe": "same high_zone_climax top5/day signals", "period": {"start_ymd": start_ymd, "end_ymd": end_ymd}, "changed_axis": "position staging only; signal membership fixed", "confirmation": "within 5 sessions: close below signal low OR bearish lower-close lower-high close-in-lower-half", "horizons": list(HORIZONS), "costs": "ignored_by_user_request", "runtime_db_write": False, "meemee_reflection": False},
        "source": {"db_path": str(db_path), "event_count": int(len(events)), "ledger_count": int(len(ledger))},
        "baseline": {"policy": "next_open_all", "metrics": baseline}, "challengers": challengers,
        "observed_branching": {"changed_top5_members_count": 0, "changed_top10_members_count": 0, "changed_rank_count": 0, "selection_divergence_reason": "all signals receive starter position; only exposure path changes"},
        "decision": {"candidate_local_decision": decision, "session_aggregate_decision": decision, "authoritative_rollup_decision": f"{decision}_high_zone_staged_entry", "selected_policy": leader if decision == "keep" else None, "research_leader": leader, "reason_type": "capture_and_expectancy_gates_pass" if decision == "keep" else ("capture_kept_but_expectancy_gate_incomplete" if decision == "hold" else "staging_did_not_improve_tradeoff")},
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_changed": False,
    }
    run_dir = output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    ledger.to_parquet(run_dir / "staged_entry_ledger.parquet", index=False)
    (run_dir / "compare.json").write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (run_dir / "_ARTIFACT_COMPLETE.json").write_text(json.dumps({"status": "complete", "required_files": ["compare.json", "staged_entry_ledger.parquet", "_ARTIFACT_COMPLETE.json"]}, indent=2) + "\n", encoding="utf-8")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_high_zone_staged_entry_v1"))
    parser.add_argument("--start-ymd", type=int, default=20220101)
    parser.add_argument("--end-ymd", type=int, default=20260617)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.start_ymd, args.end_ymd))
    return 0


if __name__ == "__main__": raise SystemExit(main())
