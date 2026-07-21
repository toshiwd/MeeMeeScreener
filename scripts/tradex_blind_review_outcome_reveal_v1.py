"""Reveal next-open five-session outcomes after blind model annotations are sealed."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


SELL_ENTRY_ACTIONS = {"PROBE", "CORE", "ADD", "REENTRY_PROBE"}
MANAGEMENT_ACTIONS = {"TAKE_PROFIT_FULL_HEDGE"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evaluate(group: pd.DataFrame, signal_ymd: int) -> dict:
    index = group.index[group.ymd.eq(signal_ymd)]
    if len(index) != 1 or int(index[0]) + 5 >= len(group):
        return {"status": "censored"}
    entry_index = int(index[0]) + 1
    window = group.iloc[entry_index:entry_index + 5]
    entry = float(window.iloc[0].o)
    target, stop = entry * 0.97, entry * 1.03
    exit_price = float(window.iloc[-1].c)
    exit_reason = "horizon_close"
    outcome = "N"
    exit_ymd = int(window.iloc[-1].ymd)
    for row in window.itertuples():
        if row.o >= stop:
            exit_price, exit_reason, outcome, exit_ymd = float(row.o), "gap_stop", "R", int(row.ymd)
            break
        if row.o <= target:
            exit_price, exit_reason, outcome, exit_ymd = float(row.o), "gap_target", "D", int(row.ymd)
            break
        if row.h >= stop and row.l <= target:
            exit_price, exit_reason, outcome, exit_ymd = stop, "same_bar_stop_first", "R", int(row.ymd)
            break
        if row.h >= stop:
            exit_price, exit_reason, outcome, exit_ymd = stop, "stop", "R", int(row.ymd)
            break
        if row.l <= target:
            exit_price, exit_reason, outcome, exit_ymd = target, "target", "D", int(row.ymd)
            break
    return {
        "status": "complete",
        "entry_ymd": int(window.iloc[0].ymd),
        "exit_ymd": exit_ymd,
        "entry_open": entry,
        "exit_price_fixed3": exit_price,
        "exit_reason_fixed3": exit_reason,
        "outcome_fixed3": outcome,
        "return_fixed3_pct": 100 * (entry - exit_price) / entry,
        "return_h5_close_pct": 100 * (entry - float(window.iloc[-1].c)) / entry,
        "mfe_short_5_pct": 100 * (entry - float(window.l.min())) / entry,
        "mae_short_5_pct": 100 * (entry - float(window.h.max())) / entry,
        "h5_ymd": int(window.iloc[-1].ymd),
    }


def stats(frame: pd.DataFrame) -> dict:
    complete = frame[frame.status.eq("complete")]
    values = complete.return_fixed3_pct.dropna()
    gain, loss = values[values > 0].sum(), -values[values < 0].sum()
    return {
        "n": len(frame), "completed": len(complete),
        "D": int(complete.outcome_fixed3.eq("D").sum()),
        "R": int(complete.outcome_fixed3.eq("R").sum()),
        "N": int(complete.outcome_fixed3.eq("N").sum()),
        "mean_fixed3_pct": None if values.empty else float(values.mean()),
        "mean_h5_close_pct": None if complete.empty else float(complete.return_h5_close_pct.mean()),
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": None if values.empty else float(values.min()),
    }


def portfolio_stats(frame: pd.DataFrame) -> dict:
    complete = frame[frame.status.eq("complete")].sort_values(["exit_ymd", "code"])
    values = complete.return_fixed3_pct.reset_index(drop=True)
    equity = values.cumsum()
    drawdown = equity - equity.cummax() if len(equity) else pd.Series(dtype=float)
    run = max_run = 0
    for value in values:
        run = run + 1 if value < 0 else 0
        max_run = max(max_run, run)
    concurrent = 0
    for day in sorted(set(complete.entry_ymd).union(complete.exit_ymd)):
        concurrent = max(concurrent, int(((complete.entry_ymd <= day) & (complete.exit_ymd >= day)).sum()))
    return {
        "sum_return_units_pct": float(values.sum()),
        "max_drawdown_units_pct": 0.0 if drawdown.empty else float(drawdown.min()),
        "max_loss_streak": max_run,
        "max_concurrent": concurrent,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--board", type=Path, required=True)
    parser.add_argument("--sealed", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    board = pd.read_parquet(args.board)
    sealed = pd.read_parquet(args.sealed)
    if bool(sealed.outcome_joined.any()) or not board[["case_id", "code", "ymd"]].equals(sealed[["case_id", "code", "ymd"]]):
        raise RuntimeError("seal is not outcome-free or key parity failed")
    codes = sealed.code.astype(str).str.zfill(4).unique().tolist()
    connection = duckdb.connect(str(args.db), read_only=True)
    prices = connection.execute(
        "select code, strftime(to_timestamp(date), '%Y%m%d')::integer ymd, o, h, l, c "
        "from daily_bars where code in (select unnest(?)) order by code, date",
        [codes],
    ).fetchdf()
    prices.code = prices.code.astype(str).str.zfill(4)
    histories = {code: group.reset_index(drop=True) for code, group in prices.groupby("code")}

    rows = []
    for item in sealed.itertuples():
        outcome = evaluate(histories[str(item.code).zfill(4)], int(item.ymd))
        role = "sell_entry" if item.model_action in SELL_ENTRY_ACTIONS else "management" if item.model_action in MANAGEMENT_ACTIONS else "avoid"
        rows.append({**item._asdict(), "evaluation_role": role, **outcome})
    ledger = pd.DataFrame(rows)
    ledger_path = args.output / "outcome_reveal_ledger.parquet"
    ledger.to_parquet(ledger_path, index=False)

    primary = ledger[(ledger.evaluation_role == "sell_entry") & (ledger.bucket != "BREAKDOWN_REJECTED")]
    avoid = ledger[ledger.evaluation_role == "avoid"]
    management = ledger[ledger.evaluation_role == "management"]
    compare = {
        "schema_version": "tradex_blind_review_outcome_reveal_v1.compare.v1",
        "artifact_role": "authoritative_blind_outcome_reveal",
        "review_only": True,
        "fixed_conditions": {
            "selection_frozen_before_reveal": True,
            "execution": "next_session_open",
            "horizon_sessions": 5,
            "barriers": "short target -3%, stop +3%, same-bar stop-first",
            "h5_return": "next-session open to fifth-session close",
            "costs": "ignored",
            "weekly_inputs": [],
            "breakdown_role": "diagnostic_negative_control_excluded_from_primary_denominator",
        },
        "primary_sell_entry": {**stats(primary), **portfolio_stats(primary)},
        "primary_sell_entry_by_bucket": {bucket: stats(group) for bucket, group in primary.groupby("bucket")},
        "primary_sell_entry_by_action": {action: stats(group) for action, group in primary.groupby("model_action")},
        "by_action": {action: stats(group) for action, group in ledger.groupby("model_action")},
        "by_bucket": {bucket: stats(group) for bucket, group in ledger.groupby("bucket")},
        "avoid_diagnostic": stats(avoid),
        "management_diagnostic": stats(management),
        "judgment": {"decision": "hold_pending_user_action_review"},
        "not_changed": ["fixed model rules", "MeeMee ranking", "runtime DB", "weekly exclusion"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(compare, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "board_sha256": sha(args.board),
        "sealed_sha256": sha(args.sealed),
        "db_path": str(args.db.resolve()),
        "db_read_only": True,
        "rows": len(ledger),
        "completed": int(ledger.status.eq("complete").sum()),
        "weekly_columns_used": [],
        "future_selection_columns_used": [],
        "outcome_ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), **compare["primary_sell_entry"]}, indent=2))


if __name__ == "__main__":
    main()
