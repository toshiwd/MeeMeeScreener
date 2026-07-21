from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.tradex_short_support_break_exit_grid_v1 import clean, simulate


AXIS_ID = "tradex_frozen_two_sided_union_2026_v1"
DB = Path(r"G:\Tradex\scratch\source_snapshots\nightly_candidate_20260713_20260713T002453985795Z.duckdb")
BUY = Path(r"G:\Tradex\leaf_cap4_slot24_2026_matured_oos_v1\20260713T045756Z-tradex_leaf_cap4_slot24_2026_matured_oos_v1\event_ledger_2026.csv")
SELL = Path(r"G:\Tradex\short_leaf20_event_ledger_v1\20260713T044404Z-tradex_short_leaf20_event_ledger_v1\event_ledger.parquet")
OUT = Path(r"G:\Tradex\frozen_two_sided_union_2026_v1")
BEGIN, END, COST = 20260105, 20260605, 0.001


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def pf(values: pd.Series) -> float | None:
    gains = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    return gains / losses if losses else None


def metrics(rows: pd.DataFrame, calendar: list[int]) -> dict:
    ret = rows["net_ret"].astype(float) if len(rows) else pd.Series(dtype=float)
    daily_active = rows.groupby("signal_ymd", sort=True)["net_ret"].mean() if len(rows) else pd.Series(dtype=float)
    daily = daily_active.reindex(calendar, fill_value=0.0)
    losses, wins = ret[ret < 0], ret[ret > 0]
    cut = float(ret.quantile(0.10)) if len(ret) else None
    equity = (1.0 + daily).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    weeks = pd.to_datetime(pd.Series(calendar).astype(str)).dt.strftime("%G-W%V").nunique()
    return {
        "n": int(len(rows)),
        "signal_days": int(rows["signal_ymd"].nunique()) if len(rows) else 0,
        "events_per_calendar_week": float(len(rows) / weeks) if weeks else None,
        "signal_days_per_calendar_week": float(rows["signal_ymd"].nunique() / weeks) if len(rows) and weeks else 0.0,
        "profit_factor": pf(ret),
        "daily_profit_factor": pf(daily_active),
        "expectancy": float(ret.mean()) if len(ret) else None,
        "calendar_expectancy": float(daily.mean()) if len(daily) else None,
        "win_rate": float((ret > 0).mean()) if len(ret) else None,
        "payoff_ratio": float(wins.mean() / -losses.mean()) if len(wins) and len(losses) else None,
        "p05": float(ret.quantile(0.05)) if len(ret) else None,
        "cvar10": float(ret[ret <= cut].mean()) if cut is not None else None,
        "max_drawdown_return": float(drawdown.min()) if len(drawdown) else 0.0,
        "net_return_sum": float(ret.sum()) if len(ret) else 0.0,
    }


def branching(candidate: pd.DataFrame, baseline: pd.DataFrame, calendar: list[int]) -> dict:
    days = []
    for ymd in calendar:
        for side in ("buy", "sell"):
            left = candidate[(candidate.signal_ymd == ymd) & (candidate.side == side)].sort_values(["source_rank", "code"])["code"].astype(str).tolist()
            right = baseline[(baseline.signal_ymd == ymd) & (baseline.side == side)].sort_values(["rank", "code"])["code"].astype(str).tolist()
            union = set(left) | set(right)
            days.append({
                "signal_ymd": ymd, "side": side,
                "changed_members_count": len(set(left) ^ set(right)),
                "changed_rank_count": sum(a != b for a, b in zip(left, right)) + abs(len(left) - len(right)),
                "jaccard": len(set(left) & set(right)) / len(union) if union else 1.0,
                "candidate_empty": not left, "baseline_empty": not right,
                "one_side_empty": bool(left) != bool(right),
            })
    changed = [row for row in days if row["changed_members_count"] > 0]
    return {
        "comparison_level": "executed_top3_or_fewer_by_date_and_side",
        "changed_top5_members_count": None,
        "changed_top10_members_count": None,
        "top5_top10_unavailable_reason": "frozen rule ledgers contain executed events, not preselection top5/top10 candidate boards",
        "changed_top3_members_count": int(sum(row["changed_members_count"] for row in days)),
        "changed_rank_count": int(sum(row["changed_rank_count"] for row in days)),
        "changed_date_side_count": len(changed),
        "changed_date_side_rate": len(changed) / len(days) if days else None,
        "mean_jaccard": sum(row["jaccard"] for row in days) / len(days) if days else None,
        "one_side_empty_count": sum(row["one_side_empty"] for row in days),
        "selection_divergence_reason": "frozen shape rules versus MeeMee direction ranks; same execution contracts and rank-history dates",
        "boundary_metrics": {"date_side_cells": len(days), "candidate_event_count": len(candidate), "baseline_event_count": len(baseline)},
        "days": days,
    }


def load_frozen(buy_path: Path, sell_path: Path) -> pd.DataFrame:
    buy = pd.read_csv(buy_path)
    buy["signal_ymd"] = pd.to_datetime(buy["date"], unit="s", utc=True).dt.strftime("%Y%m%d").astype(int)
    buy["net_ret"] = buy["pnl_yen"].astype(float) / buy["invested_yen"].astype(float)
    buy["side"] = "buy"
    buy["source_rank"] = buy.groupby("signal_ymd")["tie_gap_ma60"].rank(method="first", ascending=False).astype(int)
    buy = buy.rename(columns={"next_entry_date": "entry_date"})
    sell = pd.read_parquet(sell_path)
    sell = sell.rename(columns={"entry_ymd": "entry_date"})
    sell["net_ret"] = sell["ret"].astype(float) - COST
    sell["side"] = "sell"
    sell["source_rank"] = sell.groupby("signal_ymd")["code"].rank(method="first").astype(int)
    keep = ["code", "signal_ymd", "entry_date", "side", "source_rank", "net_ret"]
    union = pd.concat([buy[keep], sell[keep]], ignore_index=True)
    union["code"] = union["code"].astype(str)
    return union[union["signal_ymd"].between(BEGIN, END)].sort_values(["signal_ymd", "side", "source_rank", "code"]).reset_index(drop=True)


def _short_rows(db: Path) -> pd.DataFrame:
    # The frozen short execution contract needs the signal low and eleven forward bars.
    fields = []
    for i in range(1, 12):
        prefix = "e" if i == 1 else f"f{i-1}"
        fields += [f"lead({col},{i}) over w {prefix}_{col}" for col in ("h", "l", "c")]
    query = f"""
    with b as (
      select code, cast(strftime(to_timestamp(date),'%Y%m%d') as int) ymd, l,
             {','.join(fields)}
      from daily_bars where source='pan'
      window w as(partition by code order by date)
    )
    select * from b where ymd between {BEGIN} and {END} and f10_c is not null and e_l<=l
    """
    with duckdb.connect(str(db), read_only=True) as con:
        frame = con.execute(query).fetchdf().rename(columns={"ymd": "signal_ymd"})
    frame["code"] = frame["code"].astype(str)
    return frame


def load_meemee_baseline(db: Path, calendar: list[int]) -> tuple[pd.DataFrame, dict]:
    with duckdb.connect(str(db), read_only=True) as con:
        ranks = con.execute(
            """select cast(code as varchar) code, dt signal_ymd, dir side, rank
               from ranking_appearance_daily
               where ranking_logic_version='ranking:trade:top50:v1'
                 and dir in ('up','down') and rank<=5 and dt between ? and ?""",
            [BEGIN, END],
        ).fetchdf()
        bars = con.execute(
            """with b as (
                 select cast(code as varchar) code,
                   cast(strftime(to_timestamp(date),'%Y%m%d') as int) signal_ymd,
                   c, lead(o) over w next_open,
                   lead(c,10) over w close10,
                   lead(h,1) over w h1,lead(h,2) over w h2,lead(h,3) over w h3,lead(h,4) over w h4,lead(h,5) over w h5,
                   lead(h,6) over w h6,lead(h,7) over w h7,lead(h,8) over w h8,lead(h,9) over w h9,lead(h,10) over w h10,
                   lead(l,1) over w l1,lead(l,2) over w l2,lead(l,3) over w l3,lead(l,4) over w l4,lead(l,5) over w l5,
                   lead(l,6) over w l6,lead(l,7) over w l7,lead(l,8) over w l8,lead(l,9) over w l9,lead(l,10) over w l10
                 from daily_bars where source='pan' window w as(partition by code order by date))
               select * from b where signal_ymd between ? and ? and close10 is not null and next_open/c-1<=0""",
            [BEGIN, END],
        ).fetchdf()
    ranks["side"] = ranks["side"].map({"up": "buy", "down": "sell"})
    buy = ranks[ranks.side.eq("buy")].merge(bars, on=["code", "signal_ymd"], how="inner")
    outcomes = []
    for row in buy.to_dict("records"):
        td = next((i for i in range(1, 11) if row[f"h{i}"] >= row["next_open"] * 1.08), 99)
        sd = next((i for i in range(1, 11) if row[f"l{i}"] <= row["next_open"] * 0.95), 99)
        gross = -0.05 if sd <= 10 and sd <= td else 0.08 if td <= 10 else row["close10"] / row["next_open"] - 1
        outcomes.append({"code": row["code"], "signal_ymd": row["signal_ymd"], "side": "buy", "rank": row["rank"], "net_ret": gross - COST})
    short_bars = _short_rows(db)
    sell = ranks[ranks.side.eq("sell")].merge(short_bars, on=["code", "signal_ymd"], how="inner")
    for row in sell.to_dict("records"):
        outcome = simulate({k: clean(v) for k, v in row.items()}, 0.10, 0.05, 10)
        outcomes.append({"code": row["code"], "signal_ymd": row["signal_ymd"], "side": "sell", "rank": row["rank"], "net_ret": float(outcome["ret"]) - COST})
    frame = pd.DataFrame(outcomes)
    selected = frame.sort_values(["signal_ymd", "side", "rank", "code"]).groupby(["signal_ymd", "side"]).head(3).reset_index(drop=True)
    availability = {
        "authoritative_down_ranking_exists": bool((ranks.side == "sell").any()),
        "down_ranking_dates": int(ranks.loc[ranks.side == "sell", "signal_ymd"].nunique()),
        "selection": "each side: authoritative top5, execution-eligible, rank ascending top3",
        "calendar_dates": len(calendar),
    }
    return selected, availability


def generate(db: Path, buy: Path, sell: Path, out: Path) -> Path:
    with duckdb.connect(str(db), read_only=True) as con:
        calendar = [int(x[0]) for x in con.execute(
            """select distinct dt from ranking_appearance_daily
               where ranking_logic_version='ranking:trade:top50:v1' and dir='up' and dt between ? and ? order by dt""",
            [BEGIN, END],
        ).fetchall()]
    frozen = load_frozen(buy, sell)
    baseline, baseline_status = load_meemee_baseline(db, calendar)
    fm, bm = metrics(frozen, calendar), metrics(baseline, calendar)
    side_metrics = {side: metrics(frozen[frozen.side == side], calendar) for side in ("buy", "sell")}
    total_net = sum(item["net_return_sum"] for item in side_metrics.values())
    for item in side_metrics.values():
        item["net_return_sum_share"] = item["net_return_sum"] / total_net if total_net else None
    baseline_side = {side: metrics(baseline[baseline.side == side], calendar) for side in ("buy", "sell")}
    observed_branching = branching(frozen, baseline, calendar)
    stop_cost_floor = -0.05 - COST
    gates = {
        "profit_factor_gte_1_30": bool((fm["profit_factor"] or 0) >= 1.30),
        "events_per_calendar_week_gte_1": bool((fm["events_per_calendar_week"] or 0) >= 1.0),
        "p05_consistent_with_5pct_stop_plus_10bp": bool((fm["p05"] or -1) >= stop_cost_floor - 0.0001),
    }
    comparison = {
        "profit_factor_delta": None if fm["profit_factor"] is None or bm["profit_factor"] is None else fm["profit_factor"] - bm["profit_factor"],
        "daily_profit_factor_delta": None if fm["daily_profit_factor"] is None or bm["daily_profit_factor"] is None else fm["daily_profit_factor"] - bm["daily_profit_factor"],
        "calendar_expectancy_delta": None if fm["calendar_expectancy"] is None or bm["calendar_expectancy"] is None else fm["calendar_expectancy"] - bm["calendar_expectancy"],
    }
    now = datetime.now(timezone.utc)
    root = out / f"{now.strftime('%Y%m%dT%H%M%SZ')}-{AXIS_ID}"
    root.mkdir(parents=True)
    frozen.to_csv(root / "frozen_union_events.csv", index=False)
    baseline.to_csv(root / "meemee_buy_sell_baseline_events.csv", index=False)
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "generated_at": now.isoformat(),
        "research_phase": "effectiveness_judgment",
        "axis_changed": "composition_only_frozen_buy_plus_frozen_sell",
        "fixed_evaluation_conditions": {
            "period": [BEGIN, END], "ranking_history_dates": len(calendar),
            "buy_rule": "pinned leaf 9/14/20 ledger; gap<=0; TP8/SL5/H10",
            "sell_rule": "pinned support-break breadth>=40%; next-day signal-low; TP10/SL5/H10",
            "adverse_fill_cost_each_side": COST,
            "daily_basket": "equal weight by signal date; inactive ranking-history dates are zero",
            "p05_floor": stop_cost_floor,
            "rule_parameters_changed": False,
        },
        "frozen_union": fm,
        "per_side_contribution": side_metrics,
        "meemee_buy_sell_rank_asc_baseline": {"status": baseline_status, "combined": bm, "per_side": baseline_side},
        "comparison_vs_meemee": comparison,
        "observed_branching": observed_branching,
        "absolute_goal_gates": {**gates, "pass": all(gates.values())},
        "decision": {
            "candidate_local_decision": "keep" if all(gates.values()) else "drop",
            "authoritative_rollup_decision": "research_only",
            "reason_type": "absolute_union_gates_pass" if all(gates.values()) else "union_pf_below_1_30_and_meemee_baseline_underperformance",
        },
        "source_hashes": {"buy_ledger": sha256(buy), "sell_ledger": sha256(sell), "db_snapshot": sha256(db)},
        "runtime_db_write": False, "production_ranking_changed": False, "meemee_unchanged": True,
    }
    target = root / "compare.json"
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return target


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DB)
    parser.add_argument("--buy", type=Path, default=BUY)
    parser.add_argument("--sell", type=Path, default=SELL)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    print(generate(args.db, args.buy, args.sell, args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
