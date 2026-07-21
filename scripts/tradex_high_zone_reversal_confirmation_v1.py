from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from scripts.tradex_short_state_transition_replay_v1 import HORIZONS, add_outcomes, load_events, metrics
except ModuleNotFoundError:  # Direct script execution adds scripts/ to sys.path.
    from tradex_short_state_transition_replay_v1 import HORIZONS, add_outcomes, load_events, metrics


AXIS_ID = "tradex_high_zone_reversal_confirmation_v1"
FAMILY = "high_zone_climax"
POLICIES = ("next_open", "second_down_close_2d", "balanced_reversal_3d", "strict_reversal_3d")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _write(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _value(row: pd.Series, name: str) -> float | None:
    value = row.get(name)
    return None if value is None or pd.isna(value) else float(value)


def choose_confirmation(row: pd.Series, policy: str) -> dict[str, Any]:
    if policy == "next_open":
        price = _value(row, "o1")
        return {"state": "entry" if price else "unavailable", "entry_offset": 1 if price else None, "entry_price": price, "wait_days": 1 if price else None}

    signal_close = float(row["c"])
    signal_high = float(row["h"])
    max_wait = 2 if policy == "second_down_close_2d" else 3
    for offset in range(1, max_wait + 1):
        open_ = _value(row, f"o{offset}")
        high = _value(row, f"h{offset}")
        low = _value(row, f"l{offset}")
        close = _value(row, f"c{offset}")
        prior_close = signal_close if offset == 1 else _value(row, f"c{offset - 1}")
        if None in (open_, high, low, close, prior_close):
            continue
        down_relation = close < open_ and close < prior_close
        if policy == "second_down_close_2d" and down_relation:
            return {"state": "entry", "entry_offset": offset, "entry_price": close, "wait_days": offset}
        span = max(high - low, 1e-9)
        close_range_pos = (close - low) / span
        highs = [_value(row, f"h{i}") for i in range(1, offset + 1)]
        max_high = max(value for value in highs if value is not None)
        no_continuation = max_high <= signal_high * (1.05 if policy == "balanced_reversal_3d" else 1.03)
        close_cap = close <= signal_close * (1.02 if policy == "balanced_reversal_3d" else 1.00)
        weak_close = close_range_pos <= (0.55 if policy == "balanced_reversal_3d" else 0.40)
        if down_relation and no_continuation and close_cap and weak_close:
            return {"state": "entry", "entry_offset": offset, "entry_price": close, "wait_days": offset}

    lows = [_value(row, f"l{i}") for i in range(1, max_wait + 1)]
    missed = any(low is not None and low <= signal_close * 0.95 for low in lows)
    return {"state": "missed_drop" if missed else "no_entry", "entry_offset": None, "entry_price": None, "wait_days": max_wait}


def replay(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, source in events.iterrows():
        for policy in POLICIES:
            outcome = add_outcomes(source, choose_confirmation(source, policy))
            rows.append({
                "family": FAMILY,
                "code": str(source["code"]),
                "signal_ymd": int(source["signal_ymd"]),
                "policy": policy,
                "state": outcome["state"],
                "entry_offset": outcome.get("entry_offset"),
                "entry_price": outcome.get("entry_price"),
                "wait_days": outcome.get("wait_days"),
                "sideways_20d": False,
                **{f"ret{h}": outcome[f"ret{h}"] for h in HORIZONS},
                **{f"mae{h}": outcome[f"mae{h}"] for h in HORIZONS},
            })
    return pd.DataFrame(rows)


def _decision(candidate: dict[str, Any], baseline: dict[str, Any]) -> tuple[str, dict[str, bool]]:
    checks = {
        "entry_count_at_least_30": candidate["entry_count"] >= 30,
        "entry_rate_at_least_25pct": (candidate["entry_rate"] or 0) >= 0.25,
        "missed_drop_rate_at_most_25pct": (candidate["missed_drop_rate"] or 1) <= 0.25,
        "mean_ret10_positive": (candidate["h10"]["mean"] or -999) > 0,
        "pf10_at_least_1_2": (candidate["h10"]["profit_factor"] or 0) >= 1.2,
        "loss10_rate_at_most_10pct": (candidate["h10"]["loss_le_minus10_rate"] or 1) <= 0.10,
        "loss10_rate_better_than_baseline": (candidate["h10"]["loss_le_minus10_rate"] or 1) < (baseline["h10"]["loss_le_minus10_rate"] or 1),
        "worst_mae_at_least_minus50pct": (candidate["h10"]["worst_mae"] or -999) >= -0.50,
    }
    if all(checks.values()):
        return "keep", checks
    if checks["entry_count_at_least_30"] and checks["mean_ret10_positive"] and checks["loss10_rate_better_than_baseline"]:
        return "hold", checks
    return "drop", checks


def _period_stability(ledger: pd.DataFrame, policy: str) -> dict[str, Any]:
    part = ledger[ledger.policy == policy].copy()
    part["year"] = part.signal_ymd.astype(str).str[:4]
    yearly = [{"year": year, **metrics(group)} for year, group in part.groupby("year")]
    eligible = [item for item in yearly if item["entry_count"] >= 5]
    return {
        "yearly": yearly,
        "eligible_year_count": len(eligible),
        "positive_mean_ret10_year_rate": (sum((item["h10"]["mean"] or 0) > 0 for item in eligible) / len(eligible)) if eligible else None,
        "loss10_gate_pass_year_rate": (sum((item["h10"]["loss_le_minus10_rate"] or 1) <= 0.10 for item in eligible) / len(eligible)) if eligible else None,
    }


def _tail_cases(ledger: pd.DataFrame, policy: str) -> list[dict[str, Any]]:
    part = ledger[(ledger.policy == policy) & (ledger.state == "entry") & (ledger.ret10 <= -0.10)].copy()
    part = part.sort_values("ret10").head(20)
    return [
        {"code": row.code, "signal_ymd": int(row.signal_ymd), "entry_offset": int(row.entry_offset), "ret10": float(row.ret10), "mae10": float(row.mae10)}
        for row in part.itertuples()
    ]


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int) -> Path:
    events = load_events(db_path, start_ymd, end_ymd)
    events = events[events.family == FAMILY].copy()
    ledger = replay(events)
    results = {policy: metrics(ledger[ledger.policy == policy]) for policy in POLICIES}
    baseline = results["next_open"]
    decisions = {}
    for policy in POLICIES[1:]:
        decision, checks = _decision(results[policy], baseline)
        decisions[policy] = {
            "candidate_local_decision": decision,
            "checks": checks,
            "metrics": results[policy],
            "period_stability": _period_stability(ledger, policy),
            "tail_cases": _tail_cases(ledger, policy),
        }
    keepers = [name for name, item in decisions.items() if item["candidate_local_decision"] == "keep"]
    holds = [name for name, item in decisions.items() if item["candidate_local_decision"] == "hold"]
    best = max(keepers or holds, key=lambda name: results[name]["h10"]["profit_factor"] or 0) if keepers or holds else None
    rollup = "keep" if keepers else ("hold" if holds else "drop")
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "same high_zone_climax events as state-transition replay; top5/day",
            "period": {"start_ymd": start_ymd, "end_ymd": end_ymd},
            "changed_axis": "post-signal composite reversal confirmation only",
            "horizons": list(HORIZONS),
            "costs": "ignored_by_user_request",
            "runtime_db_write": False,
            "meemee_reflection": False,
        },
        "source": {"db_path": str(db_path), "event_count": int(len(events)), "ledger_count": int(len(ledger))},
        "baseline": {"policy": "next_open", "metrics": baseline},
        "challengers": decisions,
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": {policy: int((ledger[ledger.policy == policy].state == "entry").sum()) for policy in POLICIES[1:]},
            "selection_divergence_reason": "signal membership fixed; entry requires a multi-condition reversal state within 2 or 3 sessions",
        },
        "decision": {
            "candidate_local_decision": rollup,
            "session_aggregate_decision": rollup,
            "authoritative_rollup_decision": f"{rollup}_high_zone_composite_confirmation",
            "selected_policy": best if rollup == "keep" else None,
            "research_leader": best,
            "reason_type": "absolute_tail_and_supply_gates_pass" if rollup == "keep" else ("tail_improved_but_absolute_gate_incomplete" if rollup == "hold" else "composite_confirmation_did_not_control_tail"),
        },
        "runtime_db_write": False,
        "production_ranking_changed": False,
        "meemee_changed": False,
    }
    run_dir = output_root / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    run_dir.mkdir(parents=True, exist_ok=False)
    ledger.to_parquet(run_dir / "transition_ledger.parquet", index=False)
    _write(run_dir / "compare.json", payload)
    _write(run_dir / "_ARTIFACT_COMPLETE.json", {"status": "complete", "required_files": ["compare.json", "transition_ledger.parquet", "_ARTIFACT_COMPLETE.json"]})
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_high_zone_reversal_confirmation_v1"))
    parser.add_argument("--start-ymd", type=int, default=20220101)
    parser.add_argument("--end-ymd", type=int, default=20260617)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.start_ymd, args.end_ymd))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
