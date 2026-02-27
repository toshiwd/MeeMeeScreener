from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.backend.services.toredex_config import load_toredex_config
from app.backend.services.toredex_models import REASON_ID_SET
from app.backend.services.toredex_paths import ensure_monthly_paths, resolve_runs_root
from app.db.session import get_conn


@dataclass
class Lot:
    ticker: str
    side: str
    units: int
    entry_price: float
    open_reason: str
    open_asof: date


@dataclass
class CloseFragment:
    ticker: str
    side: str
    units: int
    open_reason: str
    close_reason: str
    open_asof: date
    close_asof: date
    entry_price: float
    exit_price: float
    pnl: float


def _to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError("empty date")
    return datetime.strptime(text, "%Y-%m-%d").date()


def _normalize_db_date_expr(column: str) -> str:
    return (
        f"CASE "
        f"WHEN {column} BETWEEN 19000101 AND 20991231 THEN {column} "
        f"WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000), '%Y%m%d') AS INTEGER) "
        f"WHEN {column} >= 100000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER) "
        f"ELSE NULL END"
    )


def _pnl_value(side: str, entry_price: float, exit_price: float, units: int, unit_notional: float) -> float:
    if entry_price <= 0:
        return 0.0
    if side.upper() == "SHORT":
        pct = (entry_price - exit_price) / entry_price
    else:
        pct = (exit_price - entry_price) / entry_price
    return float(units) * float(unit_notional) * float(pct)


def _load_season_config(season_id: str) -> tuple[dict[str, Any], float]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT config_json FROM toredex_seasons WHERE season_id = ?",
            [season_id],
        ).fetchone()
    config: dict[str, Any] = {}
    if row and row[0]:
        try:
            loaded = json.loads(str(row[0]))
            if isinstance(loaded, dict):
                config = loaded
        except Exception:
            config = {}
    max_per_ticker = float(config.get("maxPerTicker") or 10_000_000)
    unit_notional = max_per_ticker / 10.0
    return config, unit_notional


def _load_trades(season_id: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                "asOf",
                trade_id,
                ticker,
                side,
                delta_units,
                price,
                reason_id
            FROM toredex_trades
            WHERE season_id = ?
            ORDER BY "asOf" ASC, created_at ASC, trade_id ASC
            """,
            [season_id],
        ).fetchall()
    trades: list[dict[str, Any]] = []
    for row in rows:
        trades.append(
            {
                "asOf": _to_date(row[0]),
                "trade_id": str(row[1]),
                "ticker": str(row[2]),
                "side": str(row[3]).upper(),
                "delta": int(row[4]),
                "price": float(row[5]),
                "reason": str(row[6]),
            }
        )
    return trades


def _load_latest_close_map(tickers: set[str], end_asof: date) -> dict[str, float]:
    if not tickers:
        return {}
    ymd = int(end_asof.strftime("%Y%m%d"))
    close_map: dict[str, float] = {}
    norm_expr = _normalize_db_date_expr("date")
    with get_conn() as conn:
        for ticker in sorted(tickers):
            row = conn.execute(
                f"""
                SELECT c
                FROM daily_bars
                WHERE code = ?
                  AND {norm_expr} IS NOT NULL
                  AND {norm_expr} <= ?
                ORDER BY {norm_expr} DESC
                LIMIT 1
                """,
                [ticker, ymd],
            ).fetchone()
            if row and row[0] is not None:
                close_map[ticker] = float(row[0])
    return close_map


def _compute_scorecard(season_id: str) -> dict[str, Any]:
    config, unit_notional = _load_season_config(season_id)
    trades = _load_trades(season_id)
    if not trades:
        return {
            "season_id": season_id,
            "unit_notional": unit_notional,
            "start_asof": None,
            "end_asof": None,
            "rows": [],
            "closed_events": [],
        }

    lots_by_key: dict[tuple[str, str], list[Lot]] = defaultdict(list)
    closed_events: list[CloseFragment] = []

    open_event_count: dict[str, int] = defaultdict(int)
    open_event_units: dict[str, int] = defaultdict(int)
    close_event_count: dict[str, int] = defaultdict(int)
    close_event_units: dict[str, int] = defaultdict(int)

    for trade in trades:
        ticker = trade["ticker"]
        side = trade["side"]
        delta = int(trade["delta"])
        price = float(trade["price"])
        reason = str(trade["reason"])
        asof = trade["asOf"]
        key = (ticker, side)

        if delta > 0:
            lots_by_key[key].append(
                Lot(
                    ticker=ticker,
                    side=side,
                    units=delta,
                    entry_price=price,
                    open_reason=reason,
                    open_asof=asof,
                )
            )
            open_event_count[reason] += 1
            open_event_units[reason] += delta
            continue

        if delta < 0:
            close_event_count[reason] += 1
            close_units = abs(delta)
            close_event_units[reason] += close_units
            remain = close_units
            queue = lots_by_key.get(key, [])
            while remain > 0 and queue:
                lot = queue[0]
                take = min(remain, lot.units)
                pnl = _pnl_value(
                    side=side,
                    entry_price=lot.entry_price,
                    exit_price=price,
                    units=take,
                    unit_notional=unit_notional,
                )
                closed_events.append(
                    CloseFragment(
                        ticker=ticker,
                        side=side,
                        units=take,
                        open_reason=lot.open_reason,
                        close_reason=reason,
                        open_asof=lot.open_asof,
                        close_asof=asof,
                        entry_price=lot.entry_price,
                        exit_price=price,
                        pnl=pnl,
                    )
                )
                lot.units -= take
                remain -= take
                if lot.units <= 0:
                    queue.pop(0)
            if remain > 0:
                raise RuntimeError(
                    f"season={season_id} asOf={asof.isoformat()} ticker={ticker} side={side}: close exceeds position"
                )
            continue

        raise RuntimeError(f"delta_units must not be zero: trade_id={trade['trade_id']}")

    start_asof = trades[0]["asOf"]
    end_asof = trades[-1]["asOf"]

    unresolved_tickers = {lot.ticker for lots in lots_by_key.values() for lot in lots if lot.units > 0}
    close_map = _load_latest_close_map(unresolved_tickers, end_asof)

    open_realized_pnl: dict[str, float] = defaultdict(float)
    open_unrealized_pnl: dict[str, float] = defaultdict(float)
    open_closed_units: dict[str, int] = defaultdict(int)
    open_remaining_units: dict[str, int] = defaultdict(int)
    open_win_units: dict[str, int] = defaultdict(int)
    open_eval_units: dict[str, int] = defaultdict(int)

    close_realized_pnl: dict[str, float] = defaultdict(float)
    close_win_units: dict[str, int] = defaultdict(int)
    close_eval_units: dict[str, int] = defaultdict(int)

    for ev in closed_events:
        open_realized_pnl[ev.open_reason] += ev.pnl
        open_closed_units[ev.open_reason] += ev.units
        open_eval_units[ev.open_reason] += ev.units
        if ev.pnl > 0:
            open_win_units[ev.open_reason] += ev.units

        close_realized_pnl[ev.close_reason] += ev.pnl
        close_eval_units[ev.close_reason] += ev.units
        if ev.pnl > 0:
            close_win_units[ev.close_reason] += ev.units

    for lots in lots_by_key.values():
        for lot in lots:
            if lot.units <= 0:
                continue
            mark = close_map.get(lot.ticker)
            if mark is None:
                continue
            pnl = _pnl_value(
                side=lot.side,
                entry_price=lot.entry_price,
                exit_price=mark,
                units=lot.units,
                unit_notional=unit_notional,
            )
            open_unrealized_pnl[lot.open_reason] += pnl
            open_remaining_units[lot.open_reason] += lot.units
            open_eval_units[lot.open_reason] += lot.units
            if pnl > 0:
                open_win_units[lot.open_reason] += lot.units

    all_reasons = set(REASON_ID_SET)
    all_reasons.update(open_event_count.keys())
    all_reasons.update(close_event_count.keys())
    rows: list[dict[str, Any]] = []

    for reason in sorted(all_reasons):
        origin_total = open_realized_pnl[reason] + open_unrealized_pnl[reason]
        open_units_eval = open_eval_units[reason]
        close_units_eval = close_eval_units[reason]
        row = {
            "reason_id": reason,
            "open_events": int(open_event_count[reason]),
            "open_units": int(open_event_units[reason]),
            "origin_closed_units": int(open_closed_units[reason]),
            "origin_open_units": int(open_remaining_units[reason]),
            "origin_realized_pnl": float(round(open_realized_pnl[reason], 6)),
            "origin_unrealized_pnl": float(round(open_unrealized_pnl[reason], 6)),
            "origin_total_pnl": float(round(origin_total, 6)),
            "origin_unit_win_rate": (
                float(round(open_win_units[reason] / open_units_eval, 6)) if open_units_eval > 0 else None
            ),
            "close_events": int(close_event_count[reason]),
            "close_units": int(close_event_units[reason]),
            "close_realized_pnl": float(round(close_realized_pnl[reason], 6)),
            "close_unit_win_rate": (
                float(round(close_win_units[reason] / close_units_eval, 6)) if close_units_eval > 0 else None
            ),
        }
        rows.append(row)

    rows.sort(key=lambda r: (-(r["origin_total_pnl"] or 0.0), r["reason_id"]))

    closed_rows = [
        {
            "ticker": ev.ticker,
            "side": ev.side,
            "units": ev.units,
            "open_reason": ev.open_reason,
            "close_reason": ev.close_reason,
            "open_asof": ev.open_asof.isoformat(),
            "close_asof": ev.close_asof.isoformat(),
            "entry_price": float(round(ev.entry_price, 6)),
            "exit_price": float(round(ev.exit_price, 6)),
            "pnl": float(round(ev.pnl, 6)),
        }
        for ev in closed_events
    ]

    return {
        "season_id": season_id,
        "unit_notional": unit_notional,
        "config": config,
        "start_asof": start_asof.isoformat(),
        "end_asof": end_asof.isoformat(),
        "rows": rows,
        "closed_events": closed_rows,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build TOREDEX reason scorecard CSV")
    parser.add_argument("--season-id", required=True)
    parser.add_argument("--out", default=None, help="Output CSV path (default: runs/<season>/monthly/reason_scorecard.csv)")
    args = parser.parse_args()

    result = _compute_scorecard(str(args.season_id))

    cfg = load_toredex_config()
    runs_root = resolve_runs_root(cfg.runs_dir)
    monthly_paths = ensure_monthly_paths(runs_root, str(args.season_id))

    out_path = Path(args.out) if args.out else Path(monthly_paths["real_dir"]) / "reason_scorecard.csv"
    closed_out = out_path.with_name("reason_closed_events.csv")
    json_out = out_path.with_name("reason_scorecard_summary.json")

    score_rows = result.get("rows") if isinstance(result.get("rows"), list) else []
    closed_rows = result.get("closed_events") if isinstance(result.get("closed_events"), list) else []

    _write_csv(
        out_path,
        score_rows,
        [
            "reason_id",
            "open_events",
            "open_units",
            "origin_closed_units",
            "origin_open_units",
            "origin_realized_pnl",
            "origin_unrealized_pnl",
            "origin_total_pnl",
            "origin_unit_win_rate",
            "close_events",
            "close_units",
            "close_realized_pnl",
            "close_unit_win_rate",
        ],
    )

    _write_csv(
        closed_out,
        closed_rows,
        [
            "ticker",
            "side",
            "units",
            "open_reason",
            "close_reason",
            "open_asof",
            "close_asof",
            "entry_price",
            "exit_price",
            "pnl",
        ],
    )

    payload = {
        "season_id": result.get("season_id"),
        "start_asof": result.get("start_asof"),
        "end_asof": result.get("end_asof"),
        "unit_notional": result.get("unit_notional"),
        "scorecard_csv": str(out_path),
        "closed_events_csv": str(closed_out),
        "rows": int(len(score_rows)),
        "closed_events": int(len(closed_rows)),
    }
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
