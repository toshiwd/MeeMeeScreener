from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import duckdb


CANDIDATE_NAME = "actual_trade_ledger_normalization_v1"
SOURCE_DB = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb")
SOURCE_CSV = Path(r"C:\Users\enish\AppData\Local\MeeMeeScreener\data\csv\楽天証券取引履歴.csv")
PREVIOUS_INVENTORY_ROOT = Path(
    r"G:\Tradex\actual_trade_ledger_inventory_v1\20260511T120844Z-actual_trade_ledger_inventory_v1"
)
OUT_BASE = Path(r"G:\Tradex") / CANDIDATE_NAME

CLOSED_FIELDS = [
    "normalized_trade_id",
    "source",
    "broker",
    "account_type",
    "symbol",
    "side",
    "entry_date",
    "exit_date",
    "entry_price",
    "exit_price",
    "quantity",
    "notional_entry",
    "notional_exit",
    "gross_pnl",
    "gross_return_pct",
    "holding_days",
    "entry_event_count",
    "exit_event_count",
    "partial_entry_flag",
    "partial_exit_flag",
    "round_start_event_id",
    "round_end_event_id",
    "source_event_ids",
    "fees",
    "tax",
    "net_pnl",
    "net_return_pct",
    "pnl_is_gross_only",
    "normalization_status",
    "normalization_notes",
]

OPEN_FIELDS = [
    "open_trade_id",
    "symbol",
    "side",
    "entry_date",
    "entry_price",
    "open_quantity",
    "entry_event_count",
    "source_event_ids",
    "last_seen_date",
    "normalization_status",
    "normalization_notes",
]

AMBIGUOUS_FIELDS = [
    "event_id",
    "broker",
    "exec_dt",
    "symbol",
    "action",
    "qty",
    "price",
    "source_row_hash",
    "transaction_type",
    "side_type",
    "margin_type",
    "ambiguity_reason",
]


@dataclass
class Event:
    event_id: str
    broker: str
    exec_dt: datetime
    symbol: str
    action: str
    qty: float
    price: float | None
    source_row_hash: str
    transaction_type: str | None
    side_type: str | None
    margin_type: str | None


@dataclass
class RoundState:
    trade_id: str
    source: str
    broker: str
    account_type: str
    symbol: str
    side: str
    quantity: float = 0.0
    total_entry_qty: float = 0.0
    exit_quantity: float = 0.0
    entry_notional: float = 0.0
    exit_notional: float = 0.0
    entry_date: datetime | None = None
    last_seen_date: datetime | None = None
    entry_event_ids: list[str] = field(default_factory=list)
    exit_event_ids: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def entry_price(self) -> float | None:
        if self.total_entry_qty <= 0:
            return None
        return self.entry_notional / self.total_entry_qty


def now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def as_jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=as_jsonable), encoding="utf-8")


def write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: as_jsonable(row.get(k)) for k in fields})


def load_source_schema() -> dict[str, Any]:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        report: dict[str, Any] = {
            "source_priority": [
                "stocks.duckdb::trade_events",
                "broker_csv",
                "stocks.duckdb::position_rounds",
            ],
            "source_paths": {
                "trade_events_db": str(SOURCE_DB),
                "broker_csv": str(SOURCE_CSV),
                "previous_inventory_root": str(PREVIOUS_INVENTORY_ROOT),
            },
            "tables": {},
            "files": {},
        }
        for table in ("trade_events", "position_rounds"):
            cols = con.execute(f"PRAGMA table_info('{table}')").fetchall()
            report["tables"][table] = {
                "columns": [{"name": r[1], "type": r[2], "notnull": bool(r[3]), "pk": bool(r[5])} for r in cols],
                "row_count": con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0],
            }
            if table == "trade_events":
                report["tables"][table]["action_counts"] = [
                    {
                        "action": r[0],
                        "transaction_type": r[1],
                        "side_type": r[2],
                        "margin_type": r[3],
                        "count": r[4],
                    }
                    for r in con.execute(
                        """
                        SELECT action, transaction_type, side_type, margin_type, COUNT(*)
                        FROM trade_events
                        GROUP BY 1,2,3,4
                        ORDER BY 5 DESC
                        """
                    ).fetchall()
                ]
        report["files"]["broker_csv"] = {
            "path": str(SOURCE_CSV),
            "exists": SOURCE_CSV.exists(),
            "size_bytes": SOURCE_CSV.stat().st_size if SOURCE_CSV.exists() else None,
            "modified_at": datetime.fromtimestamp(SOURCE_CSV.stat().st_mtime).isoformat() if SOURCE_CSV.exists() else None,
        }
        return report
    finally:
        con.close()


def load_events() -> list[Event]:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT
              COALESCE(CAST(id AS VARCHAR), source_row_hash) AS event_id,
              broker,
              exec_dt,
              symbol,
              action,
              qty,
              price,
              source_row_hash,
              transaction_type,
              side_type,
              margin_type
            FROM trade_events
            ORDER BY exec_dt, source_row_hash
            """
        ).fetchall()
    finally:
        con.close()
    return [
        Event(
            event_id=str(r[0] or ""),
            broker=str(r[1] or ""),
            exec_dt=r[2],
            symbol=str(r[3] or ""),
            action=str(r[4] or ""),
            qty=float(r[5] or 0),
            price=float(r[6]) if r[6] is not None else None,
            source_row_hash=str(r[7] or ""),
            transaction_type=r[8],
            side_type=r[9],
            margin_type=r[10],
        )
        for r in rows
    ]


def classify_event(event: Event) -> tuple[str | None, str | None, str | None, str | None]:
    action = event.action
    account_type = "unknown"
    side = None
    flow = None
    reason = None

    if action == "SPOT_BUY":
        account_type, side, flow = "spot", "long", "entry"
    elif action == "SPOT_SELL":
        account_type, side, flow = "spot", "long", "exit"
    elif action == "MARGIN_OPEN_LONG":
        account_type, side, flow = f"margin_long:{event.margin_type or 'unknown'}", "long", "entry"
    elif action == "MARGIN_CLOSE_LONG":
        account_type, side, flow = f"margin_long:{event.margin_type or 'unknown'}", "long", "exit"
    elif action == "MARGIN_OPEN_SHORT":
        account_type, side, flow = f"margin_short:{event.margin_type or 'unknown'}", "short", "entry"
    elif action == "MARGIN_CLOSE_SHORT":
        account_type, side, flow = f"margin_short:{event.margin_type or 'unknown'}", "short", "exit"
    elif not action:
        reason = "missing_trade_type"
    elif action in {"SPOT_IN", "SPOT_OUT", "DELIVERY_SHORT", "MARGIN_SWAP_TO_SPOT"}:
        reason = "cannot_distinguish_long_short"
    else:
        reason = "unknown_open_close_flag"

    if event.exec_dt is None:
        reason = "missing_date"
    elif event.qty <= 0:
        reason = "missing_quantity"
    elif event.price is None or event.price <= 0:
        reason = "missing_price"

    return account_type, side, flow, reason


def weighted_avg(old_notional: float, old_qty: float, price: float, qty: float) -> tuple[float, float]:
    return old_notional + price * qty, old_qty + qty


def make_closed_row(state: RoundState, exit_event: Event) -> dict[str, Any]:
    closed_qty = state.exit_quantity
    entry_price = state.entry_notional / state.total_entry_qty if state.total_entry_qty else None
    exit_price = state.exit_notional / closed_qty if closed_qty else None
    if state.side == "short":
        gross_pnl = (state.entry_notional - state.exit_notional)
    else:
        gross_pnl = (state.exit_notional - state.entry_notional)
    gross_return = gross_pnl / state.entry_notional if state.entry_notional else None
    entry_date = state.entry_date or exit_event.exec_dt
    exit_date = exit_event.exec_dt
    holding_days = (exit_date.date() - entry_date.date()).days
    source_event_ids = state.entry_event_ids + state.exit_event_ids
    return {
        "normalized_trade_id": state.trade_id,
        "source": state.source,
        "broker": state.broker,
        "account_type": state.account_type,
        "symbol": state.symbol,
        "side": state.side,
        "entry_date": entry_date.date().isoformat(),
        "exit_date": exit_date.date().isoformat(),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "quantity": closed_qty,
        "notional_entry": state.entry_notional,
        "notional_exit": state.exit_notional,
        "gross_pnl": gross_pnl,
        "gross_return_pct": gross_return,
        "holding_days": holding_days,
        "entry_event_count": len(state.entry_event_ids),
        "exit_event_count": len(state.exit_event_ids),
        "partial_entry_flag": len(state.entry_event_ids) > 1,
        "partial_exit_flag": len(state.exit_event_ids) > 1,
        "round_start_event_id": state.entry_event_ids[0] if state.entry_event_ids else None,
        "round_end_event_id": exit_event.event_id,
        "source_event_ids": "|".join(source_event_ids),
        "fees": None,
        "tax": None,
        "net_pnl": None,
        "net_return_pct": None,
        "pnl_is_gross_only": True,
        "normalization_status": "closed",
        "normalization_notes": "; ".join(state.notes),
    }


def ambiguous_row(event: Event, reason: str) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "broker": event.broker,
        "exec_dt": event.exec_dt.date().isoformat() if event.exec_dt else None,
        "symbol": event.symbol,
        "action": event.action,
        "qty": event.qty,
        "price": event.price,
        "source_row_hash": event.source_row_hash,
        "transaction_type": event.transaction_type,
        "side_type": event.side_type,
        "margin_type": event.margin_type,
        "ambiguity_reason": reason,
    }


def normalize_events(events: list[Event]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    open_states: dict[tuple[str, str, str, str], RoundState] = {}
    closed: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    used_event_ids: set[str] = set()
    round_seq = 0

    for event in events:
        account_type, side, flow, reason = classify_event(event)
        if reason or not account_type or not side or not flow:
            ambiguous.append(ambiguous_row(event, reason or "unknown_open_close_flag"))
            continue

        key = (event.broker, account_type, event.symbol, side)
        state = open_states.get(key)

        if flow == "entry":
            if state is None:
                round_seq += 1
                state = RoundState(
                    trade_id=f"ATL-{round_seq:06d}",
                    source="trade_events",
                    broker=event.broker,
                    account_type=account_type,
                    symbol=event.symbol,
                    side=side,
                    entry_date=event.exec_dt,
                    last_seen_date=event.exec_dt,
                )
                open_states[key] = state
            else:
                state.notes.append("partial_entry")
            state.entry_notional, state.quantity = weighted_avg(state.entry_notional, state.quantity, event.price or 0, event.qty)
            state.total_entry_qty += event.qty
            state.entry_event_ids.append(event.event_id)
            state.last_seen_date = event.exec_dt
            used_event_ids.add(event.event_id)
            continue

        if flow == "exit":
            if state is None or state.quantity <= 0:
                ambiguous.append(ambiguous_row(event, "quantity_goes_negative_unexpectedly"))
                continue
            if event.qty > state.quantity + 1e-9:
                ambiguous.append(ambiguous_row(event, "quantity_goes_negative_unexpectedly"))
                state.notes.append(f"exit_exceeds_open_qty:{event.event_id}")
                continue
            if event.qty < state.quantity - 1e-9:
                state.notes.append("partial_exit")
                state.exit_notional += (event.price or 0) * event.qty
                state.exit_quantity += event.qty
                state.exit_event_ids.append(event.event_id)
                state.last_seen_date = event.exec_dt
                state.quantity -= event.qty
                used_event_ids.add(event.event_id)
                continue

            state.exit_notional += (event.price or 0) * event.qty
            state.exit_quantity += event.qty
            state.exit_event_ids.append(event.event_id)
            used_event_ids.add(event.event_id)
            closed.append(make_closed_row(state, event))
            del open_states[key]

    open_rows: list[dict[str, Any]] = []
    for state in open_states.values():
        source_event_ids = state.entry_event_ids + state.exit_event_ids
        open_rows.append(
            {
                "open_trade_id": state.trade_id,
                "symbol": state.symbol,
                "side": state.side,
                "entry_date": state.entry_date.date().isoformat() if state.entry_date else None,
                "entry_price": state.entry_price,
                "open_quantity": state.quantity,
                "entry_event_count": len(state.entry_event_ids),
                "source_event_ids": "|".join(source_event_ids),
                "last_seen_date": state.last_seen_date.date().isoformat() if state.last_seen_date else None,
                "normalization_status": "open",
                "normalization_notes": "; ".join([*state.notes, "position_not_flat_by_end"]),
            }
        )

    diagnostics = {
        "events_used_count": len(used_event_ids),
        "events_unused_count": max(0, len(events) - len(used_event_ids)),
    }
    return closed, open_rows, ambiguous, diagnostics


def load_position_rounds_count() -> int:
    con = duckdb.connect(str(SOURCE_DB), read_only=True)
    try:
        return int(con.execute("SELECT COUNT(*) FROM position_rounds").fetchone()[0])
    finally:
        con.close()


def summarize(events: list[Event], closed: list[dict[str, Any]], open_rows: list[dict[str, Any]], ambiguous: list[dict[str, Any]], diagnostics: dict[str, Any]) -> dict[str, Any]:
    gross_pnls = [float(r["gross_pnl"]) for r in closed if r.get("gross_pnl") is not None]
    gross_returns = [float(r["gross_return_pct"]) for r in closed if r.get("gross_return_pct") is not None]
    holding_days = [int(r["holding_days"]) for r in closed if r.get("holding_days") is not None]
    date_values = [e.exec_dt.date().isoformat() for e in events if e.exec_dt]
    position_rounds_count = load_position_rounds_count()
    ambiguity_rate = len(ambiguous) / len(events) if events else 1.0
    conservation_errors = [r for r in ambiguous if r.get("ambiguity_reason") == "quantity_goes_negative_unexpectedly"]
    decision = "ready_for_context_reconstruction"
    reason = "closed trades were reconstructed from structured execution events"
    if not closed:
        decision = "insufficient_data"
        reason = "no closed trades could be reconstructed"
    elif ambiguity_rate > 0.05 or conservation_errors:
        decision = "needs_manual_mapping"
        reason = "closed trades exist but ambiguous event or quantity conservation rate is too high"

    return {
        "decision": decision,
        "decision_reason": reason,
        "source_used": "stocks.duckdb::trade_events",
        "source_paths": {
            "trade_events_db": str(SOURCE_DB),
            "broker_csv": str(SOURCE_CSV),
        },
        "raw_event_row_count": len(events),
        "events_parsed_count": len(events),
        "events_used_count": diagnostics["events_used_count"],
        "events_unused_count": diagnostics["events_unused_count"],
        "closed_trade_count": len(closed),
        "open_trade_count": len(open_rows),
        "ambiguous_event_count": len(ambiguous),
        "ambiguity_rate": ambiguity_rate,
        "date_min": min(date_values) if date_values else None,
        "date_max": max(date_values) if date_values else None,
        "symbol_count": len({e.symbol for e in events if e.symbol}),
        "long_trade_count": sum(1 for r in closed if r.get("side") == "long"),
        "short_trade_count": sum(1 for r in closed if r.get("side") == "short"),
        "gross_pnl_total": sum(gross_pnls) if gross_pnls else 0.0,
        "net_pnl_total": None,
        "gross_return_mean": mean(gross_returns) if gross_returns else None,
        "gross_return_median": median(gross_returns) if gross_returns else None,
        "win_rate_gross": (sum(1 for v in gross_pnls if v > 0) / len(gross_pnls)) if gross_pnls else None,
        "avg_holding_days": mean(holding_days) if holding_days else None,
        "median_holding_days": median(holding_days) if holding_days else None,
        "partial_entry_count": sum(1 for r in closed if r.get("partial_entry_flag")),
        "partial_exit_count": sum(1 for r in closed if r.get("partial_exit_flag")),
        "partial_entry_rate": (sum(1 for r in closed if r.get("partial_entry_flag")) / len(closed)) if closed else None,
        "partial_exit_rate": (sum(1 for r in closed if r.get("partial_exit_flag")) / len(closed)) if closed else None,
        "same_day_trade_count": sum(1 for r in closed if r.get("holding_days") == 0),
        "position_rounds_row_count": position_rounds_count,
        "position_rounds_difference_vs_closed_trades": len(closed) - position_rounds_count,
        "unmatched_position_rounds_count": None,
        "quantity_conservation_error_count": len(conservation_errors),
        "context_features_computed": False,
        "ready_for_next_step": decision == "ready_for_context_reconstruction",
        "next_recommended_axis": "decision_context_reconstruction_v1" if decision == "ready_for_context_reconstruction" else "manual_trade_mapping_review_v1",
    }


def validation_payload(summary: dict[str, Any], closed: list[dict[str, Any]], open_rows: list[dict[str, Any]], ambiguous: list[dict[str, Any]]) -> dict[str, Any]:
    required_populated = bool(closed) and all(
        r.get("entry_date")
        and r.get("exit_date")
        and r.get("entry_price") is not None
        and r.get("exit_price") is not None
        and r.get("side")
        and r.get("quantity")
        and r.get("gross_return_pct") is not None
        for r in closed
    )
    return {
        "candidate_name": CANDIDATE_NAME,
        "decision": summary["decision"],
        "reason": summary["decision_reason"],
        "source_used": summary["source_used"],
        "required_closed_schema_populated": required_populated,
        "open_trades_separated": True,
        "ambiguous_events_separated": True,
        "context_features_computed": False,
        "future_market_bars_used": False,
        "major_conservation_errors": summary["quantity_conservation_error_count"] > 0,
        "checks": {
            "closed_trades_generated": bool(closed),
            "entry_exit_dates_populated": required_populated,
            "side_populated": required_populated,
            "quantity_populated": required_populated,
            "gross_pnl_or_return_computable": required_populated,
            "ambiguity_rate": summary["ambiguity_rate"],
            "open_trade_count": len(open_rows),
            "ambiguous_event_count": len(ambiguous),
        },
    }


def main() -> None:
    stamp = now_stamp()
    run_root = OUT_BASE / f"{stamp}-{CANDIDATE_NAME}"
    run_root.mkdir(parents=True, exist_ok=True)

    schema_report = load_source_schema()
    events = load_events()
    closed, open_rows, ambiguous, diagnostics = normalize_events(events)
    summary = summarize(events, closed, open_rows, ambiguous, diagnostics)
    validation = validation_payload(summary, closed, open_rows, ambiguous)

    closed_json = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "source": "stocks.duckdb::trade_events",
        "context_features_computed": False,
        "closed_trade_count": len(closed),
        "rows": closed,
    }

    complete = {
        "candidate_name": CANDIDATE_NAME,
        "created_at_utc": stamp,
        "run_root": str(run_root),
        "decision": summary["decision"],
        "complete": True,
        "required_artifacts": [
            "normalized_trade_ledger.csv",
            "normalized_trade_ledger.json",
            "open_trade_ledger.csv",
            "ambiguous_trade_events.csv",
            "trade_normalization_summary.json",
            "trade_normalization_validation.json",
            "trade_source_schema_report.json",
        ],
    }

    write_csv(run_root / "normalized_trade_ledger.csv", CLOSED_FIELDS, closed)
    write_json(run_root / "normalized_trade_ledger.json", closed_json)
    write_csv(run_root / "open_trade_ledger.csv", OPEN_FIELDS, open_rows)
    write_csv(run_root / "ambiguous_trade_events.csv", AMBIGUOUS_FIELDS, ambiguous)
    write_json(run_root / "trade_normalization_summary.json", summary)
    write_json(run_root / "trade_normalization_validation.json", validation)
    write_json(run_root / "trade_source_schema_report.json", schema_report)
    write_json(run_root / "_ARTIFACT_COMPLETE.json", complete)

    print(json.dumps({"run_root": str(run_root), "decision": summary["decision"], "closed_trade_count": len(closed)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
