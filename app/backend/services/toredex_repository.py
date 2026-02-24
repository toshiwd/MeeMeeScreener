from __future__ import annotations

from datetime import date
import json
from typing import Any

from app.db.session import get_conn


class ToredexRepository:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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

    def get_snapshot_row(self, season_id: str, as_of: date) -> dict[str, Any] | None:
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        with get_conn() as conn:
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
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                rows,
            )

    def save_daily_metrics(self, metric: dict[str, Any]) -> None:
        with get_conn() as conn:
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
                    game_over
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                game_over
            FROM toredex_daily_metrics
            WHERE season_id = ?
        """
        params: list[Any] = [season_id]
        if before_or_equal is not None:
            query += " AND \"asOf\" <= ?"
            params.append(before_or_equal)
        query += " ORDER BY \"asOf\" DESC LIMIT 1"
        with get_conn() as conn:
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
        with get_conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM toredex_trades WHERE season_id = ? AND \"asOf\" = ?",
                [season_id, as_of],
            ).fetchone()
        return int(row[0]) if row else 0

    def save_log(self, season_id: str, as_of: date, log_path: str, kind: str) -> None:
        with get_conn() as conn:
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
