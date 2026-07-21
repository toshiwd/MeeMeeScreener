"""Validate MAE timing, fixed stops, and fixed exits for the final review cohort."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


STOPS = [2, 3, 4, 5, 6, 8]
HORIZONS = [5, 10, 20]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def stats(values):
    values = pd.Series(values, dtype=float)
    return {
        "n": int(len(values)),
        "mean": float(values.mean()),
        "median": float(values.median()),
        "positive_rate": float((values > 0).mean()),
        "worst": float(values.min()),
        "best": float(values.max()),
    }


def bootstrap(values, seed):
    values = np.asarray(values, dtype=float)
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(10000, len(values)), replace=True).mean(axis=1)
    return {
        "samples": 10000,
        "mean_ci95_low": float(np.quantile(draws, 0.025)),
        "mean_ci95_high": float(np.quantile(draws, 0.975)),
        "probability_mean_gt_zero": float((draws > 0).mean()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=Path, required=True)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--parent-compare", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    a = ap.parse_args()
    a.output.mkdir(parents=True, exist_ok=False)

    events = pd.read_parquet(a.events)
    events = events.loc[events.price_ok & events.liquidity_ok].copy()
    con = duckdb.connect(str(a.db), read_only=True)
    bars = con.execute(
        """select code, strftime(to_timestamp(date),'%Y%m%d')::integer ymd, o, h, l, c
           from daily_bars where code in (select unnest(?)) order by code,date""",
        [events.code.unique().tolist()],
    ).fetchdf()
    con.close()
    bars.code = bars.code.astype(str).str.zfill(4)

    path_rows = []
    event_rows = []
    for event in events.itertuples(index=False):
        path = bars.loc[(bars.code.eq(event.code)) & (bars.ymd.ge(event.entry_ymd))].head(20).copy()
        if len(path) != 20:
            raise ValueError(f"incomplete 20-session path for {event.code} {event.entry_ymd}")
        path["holding_day"] = np.arange(1, 21)
        path["open_ret_pct"] = (path.o / event.entry_open - 1) * 100
        path["high_ret_pct"] = (path.h / event.entry_open - 1) * 100
        path["low_ret_pct"] = (path.l / event.entry_open - 1) * 100
        path["close_ret_pct"] = (path.c / event.entry_open - 1) * 100
        path["entry_open"] = event.entry_open
        path["entry_ymd"] = event.entry_ymd
        path_rows.append(path)
        mae_row = path.loc[path.low_ret_pct.idxmin()]
        mfe_row = path.loc[path.high_ret_pct.idxmax()]
        row = {
            "code": event.code,
            "entry_ymd": int(event.entry_ymd),
            "entry_open": float(event.entry_open),
            "mae20_pct": float(mae_row.low_ret_pct),
            "mae20_day": int(mae_row.holding_day),
            "mfe20_pct": float(mfe_row.high_ret_pct),
            "mfe20_day": int(mfe_row.holding_day),
        }
        for horizon in HORIZONS:
            row[f"close_ret{horizon}_pct"] = float(path.iloc[horizon - 1].close_ret_pct)
        event_rows.append(row)
    ledger = pd.DataFrame(event_rows)
    paths = pd.concat(path_rows, ignore_index=True)

    stop_results = {}
    for stop in STOPS:
        stop_returns = {horizon: [] for horizon in HORIZONS}
        hit_events = []
        for event in ledger.itertuples(index=False):
            path = paths.loc[(paths.code.eq(event.code)) & (paths.entry_ymd.eq(event.entry_ymd))]
            stop_price = event.entry_open * (1 - stop / 100)
            hit = path.loc[path.l.le(stop_price)]
            hit_day = None
            hit_return = None
            if not hit.empty:
                first = hit.iloc[0]
                hit_day = int(first.holding_day)
                hit_return = (
                    float(first.open_ret_pct) if first.o <= stop_price else -float(stop)
                )
                hit_events.append(
                    {"code": event.code, "entry_ymd": event.entry_ymd, "day": hit_day, "return_pct": hit_return}
                )
            for horizon in HORIZONS:
                if hit_day is not None and hit_day <= horizon:
                    stop_returns[horizon].append(hit_return)
                else:
                    stop_returns[horizon].append(getattr(event, f"close_ret{horizon}_pct"))
        stopped_future_winners = 0
        for hit in hit_events:
            final_ret = ledger.loc[
                ledger.code.eq(hit["code"]) & ledger.entry_ymd.eq(hit["entry_ymd"]), "close_ret20_pct"
            ].iloc[0]
            stopped_future_winners += int(final_ret > 0)
        stop_results[str(stop)] = {
            "hit_count": len(hit_events),
            "hit_rate": len(hit_events) / len(ledger),
            "stopped_future_20d_winner_count": stopped_future_winners,
            "returns": {str(horizon): stats(stop_returns[horizon]) for horizon in HORIZONS},
            "hit_events": hit_events,
        }

    exits = {str(horizon): stats(ledger[f"close_ret{horizon}_pct"]) for horizon in HORIZONS}
    mae = {
        "distribution": stats(ledger.mae20_pct),
        "quantiles": {
            str(q): float(ledger.mae20_pct.quantile(q))
            for q in [0.10, 0.25, 0.50, 0.75, 0.90]
        },
        "day_counts": {str(int(day)): int(count) for day, count in ledger.mae20_day.value_counts().sort_index().items()},
        "day1_rate": float((ledger.mae20_day == 1).mean()),
        "within_day5_rate": float((ledger.mae20_day <= 5).mean()),
    }
    mfe = {
        "distribution": stats(ledger.mfe20_pct),
        "day_counts": {str(int(day)): int(count) for day, count in ledger.mfe20_day.value_counts().sort_index().items()},
    }
    checks = {
        "five_pct_stop_preserves_all_events": stop_results["5"]["hit_count"] == 0,
        "four_pct_stop_hits_future_winners": stop_results["4"]["stopped_future_20d_winner_count"] > 0,
        "twenty_day_mean_gt_ten_day": exits["20"]["mean"] > exits["10"]["mean"],
        "twenty_day_mean_gt_five_day": exits["20"]["mean"] > exits["5"]["mean"],
        "twenty_day_positive_rate_ge_0.90": exits["20"]["positive_rate"] >= 0.90,
        "twenty_day_bootstrap_probability_gt_zero_ge_0.95": (
            bootstrap(ledger.close_ret20_pct, 20260731)["probability_mean_gt_zero"] >= 0.95
        ),
    }
    result = {
        "schema_version": "tradex_monthly_micro_gu_trade_management_v1.compare.v1",
        "artifact_role": "authoritative_monthly_micro_gu_trade_management",
        "review_only": True,
        "research_fallback": True,
        "research_phase": "effectiveness_judgment",
        "fixed_conditions": {
            "cohort": "price 1200-8000 yen and previous-20-session median traded value >=2m yen",
            "entry": "next-session open after 0-0.5% GU",
            "stop_axis_pct": STOPS,
            "exit_axis_sessions": HORIZONS,
            "stop_execution": "if open gaps below stop use open, otherwise stop price on intraday low breach",
            "costs": "ignored_by_project_rule",
            "future_selection_columns": [],
            "weekly_inputs": [],
        },
        "authoritative_result": {
            "events": int(len(ledger)),
            "mae20": mae,
            "mfe20": mfe,
            "fixed_exits": exits,
            "fixed_stops": stop_results,
            "twenty_day_bootstrap": bootstrap(ledger.close_ret20_pct, 20260731),
            "gate_checks": checks,
        },
        "observed_branching": {
            "changed_top5_members_count": None,
            "changed_top10_members_count": None,
            "changed_rank_count": None,
            "selection_divergence_reason": "trade-management comparison on a fixed final cohort; no candidate branching",
            "stop_hit_counts": {stop: stop_results[stop]["hit_count"] for stop in stop_results},
        },
        "judgment": {
            "candidate_local_decision": {
                "entry": "keep_next_open",
                "stop": "hold_unvalidated_catastrophe_stop_at_minus5_pct",
                "exit": "keep_20_sessions",
            },
            "session_aggregate_decision": "keep_20_session_hold_avoid_tight_stops",
            "authoritative_rollup_decision": "keep_next_open_20_session_exit_review_only_minus5_catastrophe_stop_provisional",
            "reason_type": "mae_boundary_preserves_winners_and_20_session_exit_dominates_fixed_shorter_exits",
        },
        "not_changed": [
            "candidate selection",
            "price band",
            "liquidity floor",
            "volume priority tag",
            "body priority tag",
            "MeeMee",
            "ranking",
            "runtime DB",
            "production logic",
        ],
        "remaining_risks": [
            "all fifteen in-sample events finish positive at day 20, so stop-loss protection is not validated on cohort losers",
            "minus-five-percent stop is inferred from winner preservation, not loss reduction",
            "intraday order fill and price limits are simplified",
            "monthly range definition remains research-fallback",
        ],
    }
    ledger.to_parquet(a.output / "trade_management_event_ledger.parquet", index=False)
    paths.to_parquet(a.output / "trade_management_20d_paths.parquet", index=False)
    cp = a.output / "compare.json"
    cp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "sources": {
            "events": {"path": str(a.events.resolve()), "sha256": sha(a.events)},
            "db": {"path": str(a.db.resolve()), "read_only": True},
            "parent_compare": {"path": str(a.parent_compare.resolve()), "sha256": sha(a.parent_compare)},
        },
        "selected_events": int(len(ledger)),
        "future_selection_columns": [],
        "weekly_columns_used": [],
        "event_ledger_sha256": sha(a.output / "trade_management_event_ledger.parquet"),
        "path_ledger_sha256": sha(a.output / "trade_management_20d_paths.parquet"),
        "compare_sha256": sha(cp),
    }
    (a.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (a.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(cp)}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(a.output), "mae": mae, "exits": exits, "stop_hits": {k: v["hit_count"] for k, v in stop_results.items()}, "checks": checks}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
