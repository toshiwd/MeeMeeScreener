from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "tradex_short_state_transition_replay_v1"
FAMILIES = ("low20_break_relative_weakness", "high_zone_climax")
HORIZONS = (5, 10, 20)


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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _future_columns(max_offset: int = 25) -> str:
    columns: list[str] = []
    for offset in range(1, max_offset + 1):
        for column in ("date", "o", "h", "l", "c", "ma7"):
            columns.append(f"lead({column},{offset}) over w as {column}{offset}")
    return ",\n".join(columns)


def load_events(db_path: Path, start_ymd: int, end_ymd: int) -> pd.DataFrame:
    future = _future_columns()
    sql = f"""
    with base as (
      select code,date,o,h,l,c,v,
        avg(c) over(partition by code order by date rows between 6 preceding and current row) ma7,
        avg(c) over(partition by code order by date rows between 19 preceding and current row) ma20,
        lag(c,20) over(partition by code order by date) c_lag20,
        max(h) over(partition by code order by date rows between 59 preceding and current row) high60,
        min(l) over(partition by code order by date rows between 59 preceding and current row) low60
      from daily_bars where source='pan'
    ), future as (
      select *,{future}
      from base window w as(partition by code order by date)
    ), joined as (
      select b.*,f.low20_dist,f.breakout20_down,f.rel_ret20,
        (b.c-b.l)/nullif(b.h-b.l,0) close_range_pos,
        (b.c-b.low60)/nullif(b.high60-b.low60,0) close_pos60,
        b.c/nullif(b.ma20,0)-1 dist_ma20,
        b.c/nullif(b.c_lag20,0)-1 ret20
      from future b left join feature_frame_daily f on f.code=b.code and f.dt=b.date
    ), tagged as (
      select *,
        case when low20_dist<=0.02 and breakout20_down<=-0.03 and rel_ret20<=-0.05 then 1 else 0 end low_hit,
        case when ret20>=0.80 and dist_ma20>=0.08 and close_range_pos>=0.90 and close_pos60>=0.98 then 1 else 0 end high_hit
      from joined
    ), highs as (
      select *,row_number() over(partition by date order by (ret20+dist_ma20+close_range_pos+close_pos60) desc,code) high_rank
      from tagged where high_hit=1
    )
    select 'low20_break_relative_weakness' as family,tagged.*,cast(null as bigint) as high_rank from tagged
      where low_hit=1 and cast(strftime(to_timestamp(date),'%Y%m%d') as integer) between ? and ? and c_lag20 is not null and c25 is not null
    union all
    select 'high_zone_climax' as family,* from highs
      where high_rank<=5 and cast(strftime(to_timestamp(date),'%Y%m%d') as integer) between ? and ? and c_lag20 is not null and c25 is not null
    order by family,code,date
    """
    with duckdb.connect(str(db_path), read_only=True) as conn:
        frame = conn.execute(sql, [start_ymd, end_ymd, start_ymd, end_ymd]).fetchdf()
    frame["signal_ymd"] = pd.to_datetime(frame["date"], unit="s", utc=True).dt.strftime("%Y%m%d").astype(int)
    return frame


def _value(row: pd.Series, name: str) -> float | None:
    value = row.get(name)
    return None if value is None or pd.isna(value) else float(value)


def _bullish_denial(row: pd.Series, offset: int) -> bool:
    values = [_value(row, f"{name}{offset}") for name in ("o", "h", "l", "c", "ma7")]
    if any(value is None for value in values):
        return False
    open_, high, low, close, ma7 = values
    span = max(high - low, 1e-9)
    return close > open_ and (close - low) / span >= 0.70 and (close - open_) / span >= 0.50 and close > ma7


def choose_transition(row: pd.Series, policy: str) -> dict[str, Any]:
    family = str(row["family"])
    signal_close = float(row["c"])
    if policy == "signal_close":
        return {"state": "entry", "entry_offset": 0, "entry_price": signal_close, "wait_days": 0}
    if policy == "next_open":
        price = _value(row, "o1")
        return {"state": "entry" if price else "unavailable", "entry_offset": 1 if price else None, "entry_price": price, "wait_days": 1 if price else None}
    max_wait = 5 if family == FAMILIES[0] else 2
    for offset in range(1, max_wait + 1):
        if _bullish_denial(row, offset):
            return {"state": "denied", "entry_offset": None, "entry_price": None, "wait_days": offset}
        if family == FAMILIES[0]:
            high = _value(row, f"h{offset}")
            open_ = _value(row, f"o{offset}")
            ma7 = float(row["ma7"])
            if high is not None and open_ is not None and high >= ma7 * 0.99:
                return {"state": "entry", "entry_offset": offset, "entry_price": max(open_, ma7), "wait_days": offset}
        else:
            open_ = _value(row, f"o{offset}")
            close = _value(row, f"c{offset}")
            prior = signal_close if offset == 1 else _value(row, f"c{offset-1}")
            if open_ is not None and close is not None and prior is not None and close < open_ and close < prior:
                return {"state": "entry", "entry_offset": offset, "entry_price": close, "wait_days": offset}
    lows = [_value(row, f"l{offset}") for offset in range(1, max_wait + 1)]
    missed = any(low is not None and low <= signal_close * 0.95 for low in lows)
    return {"state": "missed_drop" if missed else "no_entry", "entry_offset": None, "entry_price": None, "wait_days": max_wait}


def add_outcomes(row: pd.Series, transition: dict[str, Any]) -> dict[str, Any]:
    result = dict(transition)
    entry_offset = transition.get("entry_offset")
    entry_price = transition.get("entry_price")
    if transition["state"] != "entry" or entry_offset is None or entry_price is None:
        for horizon in HORIZONS:
            result[f"ret{horizon}"] = None
            result[f"mae{horizon}"] = None
        return result
    for horizon in HORIZONS:
        exit_offset = entry_offset + horizon
        exit_close = float(row["c"]) if exit_offset == 0 else _value(row, f"c{exit_offset}")
        highs = [float(row["h"])] if entry_offset == 0 else []
        highs.extend(_value(row, f"h{offset}") for offset in range(max(1, entry_offset), exit_offset + 1))
        highs = [value for value in highs if value is not None]
        result[f"ret{horizon}"] = None if exit_close is None else 1.0 - exit_close / entry_price
        result[f"mae{horizon}"] = None if not highs else 1.0 - max(highs) / entry_price
    return result


def replay(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, source in frame.iterrows():
        for policy in ("signal_close", "next_open", "family_wait"):
            transition = add_outcomes(source, choose_transition(source, policy))
            close20 = _value(source, "c20")
            highs20 = [_value(source, f"h{i}") for i in range(1, 21)]
            lows20 = [_value(source, f"l{i}") for i in range(1, 21)]
            valid_highs = [x for x in highs20 if x is not None]
            valid_lows = [x for x in lows20 if x is not None]
            sideways = bool(close20 is not None and abs(close20 / float(source["c"]) - 1.0) <= 0.03 and valid_highs and valid_lows and (max(valid_highs) - min(valid_lows)) / float(source["c"]) <= 0.08)
            rows.append({
                "family": str(source["family"]),
                "code": str(source["code"]),
                "signal_ymd": int(source["signal_ymd"]),
                "policy": policy,
                "state": transition["state"],
                "entry_offset": transition.get("entry_offset"),
                "entry_price": transition.get("entry_price"),
                "wait_days": transition.get("wait_days"),
                "sideways_20d": sideways,
                **{f"signal_ret{h}": None if _value(source, f"c{h}") is None else 1.0 - _value(source, f"c{h}") / float(source["c"]) for h in HORIZONS},
                **{f"ret{h}": transition[f"ret{h}"] for h in HORIZONS},
                **{f"mae{h}": transition[f"mae{h}"] for h in HORIZONS},
            })
    return pd.DataFrame(rows)


def _profit_factor(values: pd.Series) -> float | None:
    clean = values.dropna()
    gains = clean[clean > 0].sum()
    losses = -clean[clean < 0].sum()
    return None if losses <= 0 else float(gains / losses)


def metrics(frame: pd.DataFrame) -> dict[str, Any]:
    entered = frame[frame.state == "entry"]
    result: dict[str, Any] = {
        "signal_count": int(len(frame)),
        "entry_count": int(len(entered)),
        "entry_rate": float(len(entered) / len(frame)) if len(frame) else None,
        "denial_rate": float((frame.state == "denied").mean()) if len(frame) else None,
        "missed_drop_rate": float((frame.state == "missed_drop").mean()) if len(frame) else None,
        "no_entry_rate": float((frame.state == "no_entry").mean()) if len(frame) else None,
        "sideways_20d_rate": float(frame.sideways_20d.mean()) if len(frame) else None,
        "mean_wait_days": float(entered.wait_days.mean()) if len(entered) else None,
    }
    for horizon in HORIZONS:
        values = entered[f"ret{horizon}"].dropna()
        adverse = entered[f"mae{horizon}"].dropna()
        result[f"h{horizon}"] = {
            "n": int(len(values)),
            "mean": float(values.mean()) if len(values) else None,
            "median": float(values.median()) if len(values) else None,
            "win_rate": float((values > 0).mean()) if len(values) else None,
            "profit_factor": _profit_factor(values),
            "loss_le_minus5_rate": float((values <= -0.05).mean()) if len(values) else None,
            "loss_le_minus10_rate": float((values <= -0.10).mean()) if len(values) else None,
            "mean_mae": float(adverse.mean()) if len(adverse) else None,
            "worst_mae": float(adverse.min()) if len(adverse) else None,
        }
    return result


def _period_breakdown(frame: pd.DataFrame, policy: str) -> dict[str, Any]:
    part = frame[frame.policy == policy].copy()
    part["year"] = part.signal_ymd.astype(str).str[:4]
    part["month"] = part.signal_ymd.astype(str).str[:6]
    return {
        "yearly": [{"year": key, **metrics(group)} for key, group in part.groupby("year")],
        "monthly": [{"month": key, **metrics(group)} for key, group in part.groupby("month")],
    }


def retry_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    wait = frame[frame.policy == "family_wait"].sort_values(["family", "code", "signal_ymd"])
    denied = wait[wait.state == "denied"]
    retries: list[float] = []
    repeated_denials = 0
    for _, event in denied.iterrows():
        later = wait[(wait.family == event.family) & (wait.code == event.code) & (wait.signal_ymd > event.signal_ymd)]
        later = later[later.signal_ymd.astype(str).map(lambda x: (pd.Timestamp(x) - pd.Timestamp(str(event.signal_ymd))).days <= 35)]
        if later.empty:
            continue
        retry = later.iloc[0]
        if retry.state == "entry" and pd.notna(retry.ret10):
            retries.append(float(retry.ret10))
        elif retry.state == "denied":
            repeated_denials += 1
    return {
        "denied_signal_count": int(len(denied)),
        "retry_entry_count": len(retries),
        "retry_mean_ret10": float(pd.Series(retries).mean()) if retries else None,
        "retry_win_rate10": float((pd.Series(retries) > 0).mean()) if retries else None,
        "repeated_denial_count": repeated_denials,
    }


def denial_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    denied = frame[(frame.policy == "family_wait") & (frame.state == "denied")]
    result: dict[str, Any] = {"denied_signal_count": int(len(denied))}
    for horizon in HORIZONS:
        values = denied[f"signal_ret{horizon}"].dropna()
        result[f"h{horizon}"] = {
            "n": int(len(values)),
            "counterfactual_short_mean": float(values.mean()) if len(values) else None,
            "up_after_denial_rate": float((values < 0).mean()) if len(values) else None,
            "up_gt5_after_denial_rate": float((values <= -0.05).mean()) if len(values) else None,
        }
    return result


def monthly_stability(frame: pd.DataFrame) -> dict[str, Any]:
    part = frame[frame.policy == "family_wait"].copy()
    part["month"] = part.signal_ymd.astype(str).str[:6]
    month_metrics = [metrics(group) for _, group in part.groupby("month")]
    eligible = [item for item in month_metrics if item["entry_count"] >= 5]
    return {
        "eligible_month_count": len(eligible),
        "positive_mean_ret10_month_rate": sum(1 for item in eligible if (item["h10"]["mean"] or 0) > 0) / len(eligible) if eligible else None,
        "win_rate50_month_rate": sum(1 for item in eligible if (item["h10"]["win_rate"] or 0) >= 0.5) / len(eligible) if eligible else None,
    }


def decide(family_frame: pd.DataFrame) -> dict[str, Any]:
    next_metrics = metrics(family_frame[family_frame.policy == "next_open"])
    wait_metrics = metrics(family_frame[family_frame.policy == "family_wait"])
    n_ok = (wait_metrics["entry_count"] or 0) >= 30
    supply_ok = (wait_metrics["entry_rate"] or 0) >= 0.25
    missed_ok = (wait_metrics["missed_drop_rate"] or 0) <= 0.25
    mean_ok = (wait_metrics["h10"]["mean"] or -999) >= (next_metrics["h10"]["mean"] or -999)
    pf_ok = (wait_metrics["h10"]["profit_factor"] or 0) >= (next_metrics["h10"]["profit_factor"] or 0)
    tail_ok = (wait_metrics["h10"]["loss_le_minus5_rate"] or 1) <= (next_metrics["h10"]["loss_le_minus5_rate"] or 1)
    absolute_tail_ok = (wait_metrics["h10"]["loss_le_minus10_rate"] or 1) <= 0.10
    absolute_mae_ok = (wait_metrics["h10"]["worst_mae"] or -999) >= -0.50
    checks = {
        "entry_count_at_least_30": n_ok,
        "entry_rate_at_least_25pct": supply_ok,
        "missed_drop_rate_at_most_25pct": missed_ok,
        "mean_ret10_not_worse": mean_ok,
        "pf10_not_worse": pf_ok,
        "loss5_rate_not_worse": tail_ok,
        "loss10_rate_at_most_10pct": absolute_tail_ok,
        "worst_mae_at_least_minus50pct": absolute_mae_ok,
    }
    if all(checks.values()):
        decision = "keep"
    elif tail_ok and (not missed_ok or not supply_ok):
        decision = "hold"
    else:
        decision = "drop"
    next_open_ready = bool(
        next_metrics["entry_count"] >= 30
        and (next_metrics["h10"]["mean"] or -999) > 0
        and (next_metrics["h10"]["profit_factor"] or 0) >= 1.2
        and (next_metrics["h10"]["win_rate"] or 0) >= 0.55
        and (next_metrics["h10"]["loss_le_minus10_rate"] or 1) <= 0.10
        and (next_metrics["h10"]["worst_mae"] or -999) >= -0.50
    )
    selected_policy = "family_wait" if decision == "keep" else ("next_open" if next_open_ready else None)
    return {
        "candidate_local_decision": decision,
        "selected_policy": selected_policy,
        "operational_policy_decision": "keep" if selected_policy else "drop",
        "next_open_ready": next_open_ready,
        "checks": checks,
        "next_open": next_metrics,
        "family_wait": wait_metrics,
        "denial_followthrough": denial_metrics(family_frame),
        "retry_after_denial": retry_metrics(family_frame),
        "monthly_stability": monthly_stability(family_frame),
    }


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int) -> Path:
    events = load_events(db_path, start_ymd, end_ymd)
    ledger = replay(events)
    family_results: dict[str, Any] = {}
    for family in FAMILIES:
        part = ledger[ledger.family == family]
        decision = decide(part)
        decision["period_breakdown"] = _period_breakdown(part, "family_wait")
        family_results[family] = decision
    retry = retry_metrics(ledger)
    operational_decisions = [family_results[family]["operational_policy_decision"] for family in FAMILIES]
    rollup = "keep" if operational_decisions == ["keep", "keep"] else ("hold" if "keep" in operational_decisions else "drop")
    changed = int((ledger[ledger.policy == "family_wait"].state == "entry").sum())
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "fixed_evaluation_conditions": {
            "universe": "all daily_bars source=pan with feature coverage",
            "period": {"start_ymd": start_ymd, "end_ymd": end_ymd},
            "families": {
                FAMILIES[0]: "low20_dist<=0.02 and breakout20_down<=-0.03 and rel_ret20<=-0.05",
                FAMILIES[1]: "ret20>=0.80 and dist_ma20>=0.08 and close_range_pos>=0.90 and close_pos60>=0.98; top5/day",
            },
            "changed_axis": "entry state transition only",
            "policies": {"baseline": "next_open", "wait_low20": "first MA7 retest within 5 sessions unless bullish denial", "wait_high_zone": "second down close within 2 sessions unless bullish denial"},
            "horizons": list(HORIZONS),
            "costs": "ignored_by_user_request",
            "runtime_db_write": False,
            "meemee_reflection": False,
        },
        "source": {"db_path": str(db_path), "event_count": int(len(events)), "ledger_count": int(len(ledger)), "family_event_counts": dict(Counter(events.family))},
        "family_results": family_results,
        "retry_after_denial": retry,
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": changed,
            "selection_divergence_reason": "entry timing and denial path changed; signal membership fixed",
        },
        "decision": {
            "candidate_local_decision": rollup,
            "session_aggregate_decision": rollup,
            "authoritative_rollup_decision": f"{rollup}_mixed_family_transition_policy",
            "reason_type": "low20_wait_keep_high_zone_tail_risk_hold" if rollup == "hold" else ("all_family_operational_policies_keep" if rollup == "keep" else "all_family_operational_policies_failed"),
            "family_wait_decisions": {family: family_results[family]["candidate_local_decision"] for family in FAMILIES},
            "selected_policies": {family: family_results[family]["selected_policy"] for family in FAMILIES},
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
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\tradex_short_state_transition_replay_v1"))
    parser.add_argument("--start-ymd", type=int, default=20220101)
    parser.add_argument("--end-ymd", type=int, default=20260617)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.start_ymd, args.end_ymd))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
