from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
from typing import Any

from app.db.session import get_conn


class ToredexRepository:
    def __init__(self, conn: Any | None = None) -> None:
        self._conn = conn

    @contextmanager
    def _conn_ctx(self):
        if self._conn is not None:
            yield self._conn
            return
        with get_conn() as conn:
            yield conn

    def ensure_season(
        self,
        *,
        season_id: str,
        mode: str,
        start_date: date,
        initial_cash: float,
        policy_version: str,
        config_json: str,
        config_hash: str,
    ) -> None:
        with self._conn_ctx() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO toredex_seasons (
                    season_id,
                    mode,
                    start_date,
                    end_date,
                    initial_cash,
                    policy_version,
                    config_json,
                    config_hash,
                    created_at
                ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    season_id,
                    mode,
                    start_date,
                    int(initial_cash),
                    policy_version,
                    config_json,
                    config_hash,
                ],
            )
            row = conn.execute(
                "SELECT config_hash, policy_version FROM toredex_seasons WHERE season_id = ?",
                [season_id],
            ).fetchone()
        if not row:
            raise RuntimeError(f"season create failed: {season_id}")
        if row[0] is not None and str(row[0]) != str(config_hash):
            raise RuntimeError("K_POLICY_INCONSISTENT: season config_hash mismatch")
        if row[1] is not None and str(row[1]) != str(policy_version):
            raise RuntimeError("K_POLICY_INCONSISTENT: season policy_version mismatch")

    def get_season(self, season_id: str) -> dict[str, Any] | None:
        with self._conn_ctx() as conn:
            row = conn.execute(
                """
                SELECT
                    season_id,
                    mode,
                    start_date,
                    end_date,
                    initial_cash,
                    policy_version,
                    config_json,
                    config_hash,
                    created_at
                FROM toredex_seasons
                WHERE season_id = ?
                """,
                [season_id],
            ).fetchone()
        if not row:
            return None
        return {
            "season_id": row[0],
            "mode": row[1],
            "start_date": row[2],
            "end_date": row[3],
            "initial_cash": float(row[4]) if row[4] is not None else None,
            "policy_version": row[5],
            "config_json": row[6],
            "config_hash": row[7],
            "created_at": row[8],
        }

    def get_latest_available_asof(self) -> int | None:
        with self._conn_ctx() as conn:
            row = conn.execute(
                """
                SELECT MAX(
                    CASE
                        WHEN date BETWEEN 19000101 AND 20991231 THEN date
                        WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                        WHEN date >= 100000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                        ELSE NULL
                    END
                )
                FROM daily_bars
                """
            ).fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    def get_close_map(self, *, as_of: date, tickers: list[str]) -> dict[str, float]:
        uniq: list[str] = []
        seen: set[str] = set()
        for ticker in tickers:
            text = str(ticker or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            uniq.append(text)
        if not uniq:
            return {}

        placeholders = ",".join(["?"] * len(uniq))
        as_of_ymd = int(as_of.strftime("%Y%m%d"))
        params: list[Any] = [*uniq, as_of_ymd]
        query = f"""
            SELECT
                code,
                c AS close
            FROM daily_bars
            WHERE code IN ({placeholders})
              AND CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 100000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                  END = ?
        """
        with self._conn_ctx() as conn:
            rows = conn.execute(query, params).fetchall()

        out: dict[str, float] = {}
        for row in rows:
            ticker = str(row[0] or "").strip()
            if not ticker:
                continue
            try:
                close = float(row[1])
            except Exception:
                continue
            if close > 0:
                out[ticker] = close
        return out

    def has_market_close_on_day(self, as_of: date) -> bool:
        as_of_ymd = int(as_of.strftime("%Y%m%d"))
        query = """
            SELECT 1
            FROM daily_bars
            WHERE CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 100000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                  END = ?
            LIMIT 1
        """
        with self._conn_ctx() as conn:
            row = conn.execute(query, [as_of_ymd]).fetchone()
        return bool(row)

    def get_snapshot_row(self, season_id: str, as_of: date) -> dict[str, Any] | None:
        with self._conn_ctx() as conn:
            row = conn.execute(
                """
                SELECT snapshot_path, snapshot_hash, payload_json
                FROM toredex_daily_snapshots
                WHERE season_id = ? AND "asOf" = ?
                """,
                [season_id, as_of],
            ).fetchone()
        if not row:
            return None
        return {
            "snapshot_path": row[0],
            "snapshot_hash": row[1],
            "payload_json": row[2],
        }

    def get_decision_row(self, season_id: str, as_of: date) -> dict[str, Any] | None:
        with self._conn_ctx() as conn:
            row = conn.execute(
                """
                SELECT decision_path, decision_hash, payload_json
                FROM toredex_decisions
                WHERE season_id = ? AND "asOf" = ?
                """,
                [season_id, as_of],
            ).fetchone()
        if not row:
            return None
        return {
            "decision_path": row[0],
            "decision_hash": row[1],
            "payload_json": row[2],
        }

    def save_snapshot(
        self,
        *,
        season_id: str,
        as_of: date,
        snapshot_path: str,
        snapshot_hash: str,
        payload_json: str,
    ) -> None:
        existing = self.get_snapshot_row(season_id, as_of)
        if existing:
            if str(existing.get("snapshot_hash") or "") != str(snapshot_hash):
                raise RuntimeError("K_POLICY_INCONSISTENT: snapshot hash conflict")
            return
        with self._conn_ctx() as conn:
            conn.execute(
                """
                INSERT INTO toredex_daily_snapshots (
                    season_id,
                    "asOf",
                    snapshot_path,
                    snapshot_hash,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [season_id, as_of, snapshot_path, snapshot_hash, payload_json],
            )

    def save_decision(
        self,
        *,
        season_id: str,
        as_of: date,
        decision_path: str,
        decision_hash: str,
        payload_json: str,
    ) -> None:
        existing = self.get_decision_row(season_id, as_of)
        if existing:
            if str(existing.get("decision_hash") or "") != str(decision_hash):
                raise RuntimeError("K_POLICY_INCONSISTENT: decision hash conflict")
            return
        with self._conn_ctx() as conn:
            conn.execute(
                """
                INSERT INTO toredex_decisions (
                    season_id,
                    "asOf",
                    decision_path,
                    decision_hash,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [season_id, as_of, decision_path, decision_hash, payload_json],
            )

    def get_positions(self, season_id: str) -> list[dict[str, Any]]:
        with self._conn_ctx() as conn:
            rows = conn.execute(
                """
                SELECT
                    ticker,
                    side,
                    units,
                    avg_price,
                    stage,
                    opened_at,
                    holding_days,
                    pnl_pct
                FROM toredex_positions
                WHERE season_id = ?
                ORDER BY ticker, side
                """,
                [season_id],
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "ticker": row[0],
                    "side": row[1],
                    "units": int(row[2]),
                    "avgPrice": float(row[3]),
                    "stage": row[4],
                    "openedAt": row[5].isoformat() if hasattr(row[5], "isoformat") else str(row[5]),
                    "holdingDays": int(row[6]) if row[6] is not None else 0,
                    "pnlPct": float(row[7]) if row[7] is not None else 0.0,
                }
            )
        return out

    def replace_positions(self, season_id: str, positions: list[dict[str, Any]]) -> None:
        with self._conn_ctx() as conn:
            conn.execute("DELETE FROM toredex_positions WHERE season_id = ?", [season_id])
            if not positions:
                return
            values: list[list[Any]] = []
            for pos in positions:
                units = int(pos.get("units") or 0)
                if units <= 0:
                    continue
                values.append(
                    [
                        season_id,
                        str(pos.get("ticker")),
                        str(pos.get("side") or "LONG"),
                        units,
                        float(pos.get("avgPrice") or 0.0),
                        str(pos.get("stage") or "PROBE"),
                        date.fromisoformat(str(pos.get("openedAt"))),
                        int(pos.get("holdingDays") or 0),
                        float(pos.get("pnlPct") or 0.0),
                    ]
                )
            if values:
                conn.executemany(
                    """
                    INSERT INTO toredex_positions (
                        season_id,
                        ticker,
                        side,
                        units,
                        avg_price,
                        stage,
                        opened_at,
                        holding_days,
                        pnl_pct
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )

    def save_trades(self, trades: list[dict[str, Any]]) -> None:
        if not trades:
            return
        with self._conn_ctx() as conn:
            rows: list[list[Any]] = []
            for trade in trades:
                rows.append(
                    [
                        trade["season_id"],
                        trade["asOf"],
                        trade["trade_id"],
                        trade["ticker"],
                        trade["side"],
                        trade["delta_units"],
                        trade["price"],
                        trade["reason_id"],
                        trade.get("fees_bps", 0.0),
                        trade.get("slippage_bps", 0.0),
                        trade.get("borrow_bps_annual", 0.0),
                        trade.get("notional", 0.0),
                        trade.get("fees_cost", 0.0),
                        trade.get("slippage_cost", 0.0),
                        trade.get("borrow_cost", 0.0),
                    ]
                )
            conn.executemany(
                """
                INSERT OR IGNORE INTO toredex_trades (
                    season_id,
                    "asOf",
                    trade_id,
                    ticker,
                    side,
                    delta_units,
                    price,
                    reason_id,
                    fees_bps,
                    slippage_bps,
                    borrow_bps_annual,
                    notional,
                    fees_cost,
                    slippage_cost,
                    borrow_cost,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                rows,
            )

    def save_daily_metrics(self, metric: dict[str, Any]) -> None:
        with self._conn_ctx() as conn:
            conn.execute(
                "DELETE FROM toredex_daily_metrics WHERE season_id = ? AND \"asOf\" = ?",
                [metric["season_id"], metric["asOf"]],
            )
            conn.execute(
                """
                INSERT INTO toredex_daily_metrics (
                    season_id,
                    "asOf",
                    cash,
                    equity,
                    daily_pnl,
                    cum_pnl,
                    cum_return_pct,
                    max_drawdown_pct,
                    holdings_count,
                    goal20_reached,
                    goal30_reached,
                    game_over,
                    gross_daily_pnl,
                    gross_cum_pnl,
                    gross_cum_return_pct,
                    net_daily_pnl,
                    net_cum_pnl,
                    net_cum_return_pct,
                    fees_cost_daily,
                    slippage_cost_daily,
                    borrow_cost_daily,
                    fees_cost_cum,
                    slippage_cost_cum,
                    borrow_cost_cum,
                    turnover_notional_daily,
                    turnover_notional_cum,
                    turnover_pct_daily,
                    long_units,
                    short_units,
                    gross_units,
                    net_units,
                    net_exposure_pct,
                    risk_gate_pass,
                    risk_gate_reason,
                    cost_sensitivity_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    metric["season_id"],
                    metric["asOf"],
                    metric["cash"],
                    metric["equity"],
                    metric["daily_pnl"],
                    metric["cum_pnl"],
                    metric["cum_return_pct"],
                    metric["max_drawdown_pct"],
                    metric["holdings_count"],
                    metric["goal20_reached"],
                    metric["goal30_reached"],
                    metric["game_over"],
                    metric.get("gross_daily_pnl"),
                    metric.get("gross_cum_pnl"),
                    metric.get("gross_cum_return_pct"),
                    metric.get("net_daily_pnl"),
                    metric.get("net_cum_pnl"),
                    metric.get("net_cum_return_pct"),
                    metric.get("fees_cost_daily"),
                    metric.get("slippage_cost_daily"),
                    metric.get("borrow_cost_daily"),
                    metric.get("fees_cost_cum"),
                    metric.get("slippage_cost_cum"),
                    metric.get("borrow_cost_cum"),
                    metric.get("turnover_notional_daily"),
                    metric.get("turnover_notional_cum"),
                    metric.get("turnover_pct_daily"),
                    metric.get("long_units"),
                    metric.get("short_units"),
                    metric.get("gross_units"),
                    metric.get("net_units"),
                    metric.get("net_exposure_pct"),
                    metric.get("risk_gate_pass"),
                    metric.get("risk_gate_reason"),
                    json.dumps(metric.get("cost_sensitivity"), ensure_ascii=False, sort_keys=True)
                    if isinstance(metric.get("cost_sensitivity"), list)
                    else None,
                ],
            )

    def get_latest_metrics(self, season_id: str, *, before_or_equal: date | None = None) -> dict[str, Any] | None:
        query = """
            SELECT
                "asOf",
                cash,
                equity,
                daily_pnl,
                cum_pnl,
                cum_return_pct,
                max_drawdown_pct,
                holdings_count,
                goal20_reached,
                goal30_reached,
                game_over,
                gross_daily_pnl,
                gross_cum_pnl,
                gross_cum_return_pct,
                net_daily_pnl,
                net_cum_pnl,
                net_cum_return_pct,
                fees_cost_daily,
                slippage_cost_daily,
                borrow_cost_daily,
                fees_cost_cum,
                slippage_cost_cum,
                borrow_cost_cum,
                turnover_notional_daily,
                turnover_notional_cum,
                turnover_pct_daily,
                long_units,
                short_units,
                gross_units,
                net_units,
                net_exposure_pct,
                risk_gate_pass,
                risk_gate_reason,
                cost_sensitivity_json
            FROM toredex_daily_metrics
            WHERE season_id = ?
        """
        params: list[Any] = [season_id]
        if before_or_equal is not None:
            query += " AND \"asOf\" <= ?"
            params.append(before_or_equal)
        query += " ORDER BY \"asOf\" DESC LIMIT 1"
        with self._conn_ctx() as conn:
            row = conn.execute(query, params).fetchone()
        if not row:
            return None
        return {
            "asOf": row[0],
            "cash": float(row[1]),
            "equity": float(row[2]),
            "daily_pnl": float(row[3]),
            "cum_pnl": float(row[4]),
            "cum_return_pct": float(row[5]),
            "max_drawdown_pct": float(row[6]),
            "holdings_count": int(row[7]),
            "goal20_reached": bool(row[8]),
            "goal30_reached": bool(row[9]),
            "game_over": bool(row[10]),
            "gross_daily_pnl": float(row[11]) if row[11] is not None else None,
            "gross_cum_pnl": float(row[12]) if row[12] is not None else None,
            "gross_cum_return_pct": float(row[13]) if row[13] is not None else None,
            "net_daily_pnl": float(row[14]) if row[14] is not None else None,
            "net_cum_pnl": float(row[15]) if row[15] is not None else None,
            "net_cum_return_pct": float(row[16]) if row[16] is not None else None,
            "fees_cost_daily": float(row[17]) if row[17] is not None else None,
            "slippage_cost_daily": float(row[18]) if row[18] is not None else None,
            "borrow_cost_daily": float(row[19]) if row[19] is not None else None,
            "fees_cost_cum": float(row[20]) if row[20] is not None else None,
            "slippage_cost_cum": float(row[21]) if row[21] is not None else None,
            "borrow_cost_cum": float(row[22]) if row[22] is not None else None,
            "turnover_notional_daily": float(row[23]) if row[23] is not None else None,
            "turnover_notional_cum": float(row[24]) if row[24] is not None else None,
            "turnover_pct_daily": float(row[25]) if row[25] is not None else None,
            "long_units": int(row[26]) if row[26] is not None else None,
            "short_units": int(row[27]) if row[27] is not None else None,
            "gross_units": int(row[28]) if row[28] is not None else None,
            "net_units": int(row[29]) if row[29] is not None else None,
            "net_exposure_pct": float(row[30]) if row[30] is not None else None,
            "risk_gate_pass": bool(row[31]) if row[31] is not None else None,
            "risk_gate_reason": str(row[32]) if row[32] is not None else None,
            "cost_sensitivity": (
                json.loads(str(row[33]))
                if row[33] is not None and str(row[33]).strip()
                else []
            ),
        }

    def get_decision_payload(self, season_id: str, as_of: date) -> dict[str, Any] | None:
        row = self.get_decision_row(season_id, as_of)
        if not row:
            return None
        payload_json = row.get("payload_json")
        if not payload_json:
            return None
        try:
            data = json.loads(str(payload_json))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def get_snapshot_payload(self, season_id: str, as_of: date) -> dict[str, Any] | None:
        row = self.get_snapshot_row(season_id, as_of)
        if not row:
            return None
        payload_json = row.get("payload_json")
        if not payload_json:
            return None
        try:
            data = json.loads(str(payload_json))
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def count_trades_on_day(self, season_id: str, as_of: date) -> int:
        with self._conn_ctx() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM toredex_trades WHERE season_id = ? AND \"asOf\" = ?",
                [season_id, as_of],
            ).fetchone()
        return int(row[0]) if row else 0

    def save_log(self, season_id: str, as_of: date, log_path: str, kind: str) -> None:
        with self._conn_ctx() as conn:
            conn.execute(
                "DELETE FROM toredex_logs WHERE season_id = ? AND \"asOf\" = ? AND kind = ?",
                [season_id, as_of, kind],
            )
            conn.execute(
                """
                INSERT INTO toredex_logs (season_id, "asOf", log_path, kind)
                VALUES (?, ?, ?, ?)
                """,
                [season_id, as_of, log_path, kind],
            )

    def set_season_end_date(self, season_id: str, end_date: date) -> None:
        with self._conn_ctx() as conn:
            conn.execute(
                "UPDATE toredex_seasons SET end_date = ? WHERE season_id = ?",
                [end_date, season_id],
            )

    def get_trade_reason_counts(self, season_id: str) -> list[dict[str, Any]]:
        with self._conn_ctx() as conn:
            rows = conn.execute(
                """
                SELECT reason_id, COUNT(*) AS cnt
                FROM toredex_trades
                WHERE season_id = ?
                GROUP BY reason_id
                ORDER BY cnt DESC, reason_id ASC
                """,
                [season_id],
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "reason_id": str(row[0]),
                    "count": int(row[1]),
                }
            )
        return out

    def get_worst_month_return_pct(self, season_id: str, *, before_or_equal: date | None = None) -> float | None:
        cond = ""
        params: list[Any] = [season_id]
        if before_or_equal is not None:
            cond = "AND \"asOf\" <= ?"
            params.append(before_or_equal)
        query = f"""
            WITH month_last AS (
                SELECT strftime("asOf", '%Y-%m') AS ym, MAX("asOf") AS last_day
                FROM toredex_daily_metrics
                WHERE season_id = ?
                {cond}
                GROUP BY 1
            ),
            month_equity AS (
                SELECT m.ym, d.equity
                FROM month_last m
                JOIN toredex_daily_metrics d
                  ON d.season_id = ? AND d."asOf" = m.last_day
            ),
            month_ret AS (
                SELECT
                    ym,
                    equity,
                    LAG(equity) OVER (ORDER BY ym) AS prev_equity
                FROM month_equity
            )
            SELECT MIN((equity / prev_equity - 1.0) * 100.0)
            FROM month_ret
            WHERE prev_equity IS NOT NULL AND prev_equity > 0
        """
        params.append(season_id)
        with self._conn_ctx() as conn:
            row = conn.execute(query, params).fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])

    def get_max_turnover_pct_per_month(self, season_id: str, *, before_or_equal: date | None = None) -> float | None:
        cond = ""
        params: list[Any] = [season_id]
        if before_or_equal is not None:
            cond = "AND \"asOf\" <= ?"
            params.append(before_or_equal)
        query = f"""
            SELECT MAX(month_turnover)
            FROM (
                SELECT strftime("asOf", '%Y-%m') AS ym, SUM(COALESCE(turnover_pct_daily, 0.0)) AS month_turnover
                FROM toredex_daily_metrics
                WHERE season_id = ?
                {cond}
                GROUP BY 1
            ) t
        """
        with self._conn_ctx() as conn:
            row = conn.execute(query, params).fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])

    def get_max_abs_net_units(self, season_id: str, *, before_or_equal: date | None = None) -> float | None:
        cond = ""
        params: list[Any] = [season_id]
        if before_or_equal is not None:
            cond = "AND \"asOf\" <= ?"
            params.append(before_or_equal)
        query = f"""
            SELECT MAX(ABS(COALESCE(net_units, 0)))
            FROM toredex_daily_metrics
            WHERE season_id = ?
            {cond}
        """
        with self._conn_ctx() as conn:
            row = conn.execute(query, params).fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])

    def has_optimization_result(
        self,
        *,
        config_hash: str,
        stage: str,
        start_date: date,
        end_date: date,
        operating_mode: str,
    ) -> bool:
        with self._conn_ctx() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM toredex_optimization_runs
                WHERE config_hash = ?
                  AND stage = ?
                  AND start_date = ?
                  AND end_date = ?
                  AND operating_mode = ?
                LIMIT 1
                """,
                [config_hash, stage, start_date, end_date, operating_mode],
            ).fetchone()
        return bool(row)

    def save_optimization_result(self, payload: dict[str, Any]) -> None:
        with self._conn_ctx() as conn:
            conn.execute(
                """
                DELETE FROM toredex_optimization_runs
                WHERE config_hash = ?
                  AND stage = ?
                  AND start_date = ?
                  AND end_date = ?
                  AND operating_mode = ?
                """,
                [
                    str(payload.get("config_hash") or ""),
                    str(payload.get("stage") or ""),
                    payload.get("start_date"),
                    payload.get("end_date"),
                    str(payload.get("operating_mode") or "champion"),
                ],
            )
            conn.execute(
                """
                INSERT INTO toredex_optimization_runs (
                    run_id,
                    config_hash,
                    git_commit,
                    operating_mode,
                    season_id,
                    stage,
                    stage_order,
                    start_date,
                    end_date,
                    status,
                    score_net_return_pct,
                    max_drawdown_pct,
                    worst_month_pct,
                    turnover_pct_avg,
                    net_exposure_units_max,
                    metrics_json,
                    artifact_path,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    str(payload.get("run_id") or ""),
                    str(payload.get("config_hash") or ""),
                    str(payload.get("git_commit") or ""),
                    str(payload.get("operating_mode") or "champion"),
                    str(payload.get("season_id") or ""),
                    str(payload.get("stage") or ""),
                    int(payload.get("stage_order") or 0),
                    payload.get("start_date"),
                    payload.get("end_date"),
                    str(payload.get("status") or "success"),
                    payload.get("score_net_return_pct"),
                    payload.get("max_drawdown_pct"),
                    payload.get("worst_month_pct"),
                    payload.get("turnover_pct_avg"),
                    payload.get("net_exposure_units_max"),
                    payload.get("metrics_json"),
                    payload.get("artifact_path"),
                ],
            )

    def get_optimization_result(
        self,
        *,
        config_hash: str,
        stage: str,
        start_date: date,
        end_date: date,
        operating_mode: str,
    ) -> dict[str, Any] | None:
        with self._conn_ctx() as conn:
            row = conn.execute(
                """
                SELECT
                    run_id,
                    config_hash,
                    git_commit,
                    operating_mode,
                    season_id,
                    stage,
                    stage_order,
                    start_date,
                    end_date,
                    status,
                    score_net_return_pct,
                    max_drawdown_pct,
                    worst_month_pct,
                    turnover_pct_avg,
                    net_exposure_units_max,
                    metrics_json,
                    artifact_path,
                    created_at
                FROM toredex_optimization_runs
                WHERE config_hash = ?
                  AND stage = ?
                  AND start_date = ?
                  AND end_date = ?
                  AND operating_mode = ?
                LIMIT 1
                """,
                [config_hash, stage, start_date, end_date, operating_mode],
            ).fetchone()
        if not row:
            return None
        return {
            "run_id": str(row[0]),
            "config_hash": str(row[1]),
            "git_commit": str(row[2]),
            "operating_mode": str(row[3]),
            "season_id": str(row[4]),
            "stage": str(row[5]),
            "stage_order": int(row[6]) if row[6] is not None else 0,
            "start_date": row[7],
            "end_date": row[8],
            "status": str(row[9]),
            "score_net_return_pct": float(row[10]) if row[10] is not None else None,
            "max_drawdown_pct": float(row[11]) if row[11] is not None else None,
            "worst_month_pct": float(row[12]) if row[12] is not None else None,
            "turnover_pct_avg": float(row[13]) if row[13] is not None else None,
            "net_exposure_units_max": float(row[14]) if row[14] is not None else None,
            "metrics_json": str(row[15]) if row[15] is not None else "",
            "artifact_path": str(row[16]) if row[16] is not None else "",
            "created_at": row[17],
        }
