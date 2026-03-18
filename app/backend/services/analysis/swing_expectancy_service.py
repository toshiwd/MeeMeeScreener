from __future__ import annotations

from datetime import datetime, timedelta, timezone
from functools import lru_cache
from threading import Lock
from typing import Any

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
from app.db.session import get_conn

_TABLE_NAME = "swing_setup_stats_daily"
_DEFAULT_HORIZONS: tuple[int, ...] = (10, 15, 20, 25)
_DEFAULT_LOOKBACK_DAYS = 720
_SHRINK_K = 120.0
_REFRESH_LOCK = Lock()
_REFRESH_RUN_LOCK = Lock()
_LATEST_ENSURE_LOCK = Lock()
_LATEST_ENSURE_CACHE: dict[str, Any] = {
    "checked_at": 0.0,
    "source_asof_ymd": None,
    "stats_asof_ymd": None,
}
_LATEST_ENSURE_MIN_INTERVAL_SEC = 30.0


def _to_ymd_expr(column: str) -> str:
    return f"""
        CASE
            WHEN {column} BETWEEN 19000101 AND 20991231 THEN {column}
            WHEN {column} >= 1000000000000 THEN CAST(strftime(to_timestamp({column} / 1000), '%Y%m%d') AS INTEGER)
            WHEN {column} >= 1000000000 THEN CAST(strftime(to_timestamp({column}), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _table_column_names(conn, table_name: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    except Exception:
        return set()
    out: set[str] = set()
    for row in rows:
        if len(row) < 2:
            continue
        name = row[1]
        if name is None:
            continue
        out.add(str(name).lower())
    return out


def ensure_swing_setup_stats_schema(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
            as_of_ymd INTEGER,
            side TEXT,
            setup_type TEXT,
            horizon_days INTEGER,
            samples INTEGER,
            win_rate DOUBLE,
            mean_ret DOUBLE,
            p25_ret DOUBLE,
            p10_ret DOUBLE,
            max_adverse DOUBLE,
            computed_at TIMESTAMP,
            PRIMARY KEY (as_of_ymd, side, setup_type, horizon_days)
        )
        """
    )


def _resolve_latest_asof_ymd(conn, as_of_ymd: int | None) -> int | None:
    ymd_expr = _to_ymd_expr("date")
    query = f"SELECT MAX({ymd_expr}) FROM daily_bars"
    params: list[Any] = []
    if as_of_ymd is not None:
        query += f" WHERE {ymd_expr} <= ?"
        params.append(int(as_of_ymd))
    row = conn.execute(query, params).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _resolve_start_ymd(as_of_ymd: int, lookback_days: int) -> int:
    base = datetime.strptime(str(as_of_ymd), "%Y%m%d")
    return int((base - timedelta(days=max(30, int(lookback_days)))).strftime("%Y%m%d"))


def _insert_horizon_rows(conn, *, as_of_ymd: int, start_ymd: int, horizon_days: int) -> None:
    pred_ymd_expr = _to_ymd_expr("m.dt")
    bar_ymd_expr = _to_ymd_expr("b.date")
    pred_cols = _table_column_names(conn, "ml_pred_20d")
    p_down_source_expr = "m.p_down" if "p_down" in pred_cols else "NULL"
    sql = f"""
        WITH pred AS (
            SELECT
                m.code AS code,
                {pred_ymd_expr} AS ymd,
                m.p_up AS p_up,
                COALESCE({p_down_source_expr}, CASE WHEN m.p_up IS NOT NULL THEN 1.0 - m.p_up ELSE NULL END) AS p_down,
                m.p_turn_up AS p_turn_up,
                m.p_turn_down AS p_turn_down,
                m.ev20_net AS ev20_net
            FROM ml_pred_20d m
        ),
        bars AS (
            SELECT
                b.code AS code,
                {bar_ymd_expr} AS ymd,
                b.c AS close,
                b.h AS high,
                b.l AS low
            FROM daily_bars b
        ),
        joined AS (
            SELECT
                b.code,
                b.ymd,
                b.close,
                b.high,
                b.low,
                p.p_up,
                p.p_down,
                p.p_turn_up,
                p.p_turn_down,
                p.ev20_net
            FROM bars b
            JOIN pred p ON p.code = b.code AND p.ymd = b.ymd
            WHERE b.ymd IS NOT NULL AND b.ymd BETWEEN ? AND ?
        ),
        windowed AS (
            SELECT
                *,
                LEAD(close, {int(horizon_days)}) OVER (PARTITION BY code ORDER BY ymd) AS fut_close,
                MIN(low) OVER (
                    PARTITION BY code ORDER BY ymd
                    ROWS BETWEEN 1 FOLLOWING AND {int(horizon_days)} FOLLOWING
                ) AS fut_min_low,
                MAX(high) OVER (
                    PARTITION BY code ORDER BY ymd
                    ROWS BETWEEN 1 FOLLOWING AND {int(horizon_days)} FOLLOWING
                ) AS fut_max_high
            FROM joined
        ),
        unioned AS (
            SELECT
                'long' AS side,
                CASE
                    WHEN COALESCE(p_up, 0.5) >= 0.60 AND COALESCE(p_turn_up, 0.5) >= 0.52 THEN 'breakout'
                    WHEN COALESCE(p_turn_up, 0.5) >= 0.58 THEN 'rebound'
                    WHEN COALESCE(ev20_net, 0.0) >= 0.0 AND COALESCE(p_up, 0.5) >= 0.55 THEN 'continuation'
                    WHEN COALESCE(p_up, 0.5) >= 0.50 THEN 'accumulation'
                    ELSE 'watch'
                END AS setup_type,
                (fut_close - close) / NULLIF(close, 0) AS ret,
                (fut_min_low - close) / NULLIF(close, 0) AS max_adverse
            FROM windowed
            WHERE fut_close IS NOT NULL AND fut_min_low IS NOT NULL

            UNION ALL

            SELECT
                'short' AS side,
                CASE
                    WHEN COALESCE(p_down, 0.5) >= 0.60 AND COALESCE(p_turn_down, 0.5) >= 0.55 THEN 'breakdown'
                    WHEN COALESCE(p_down, 0.5) >= 0.53 AND COALESCE(p_turn_down, 0.5) >= 0.53 THEN 'pressure'
                    WHEN COALESCE(ev20_net, 0.0) <= 0.0 AND COALESCE(p_down, 0.5) >= 0.55 THEN 'continuation'
                    ELSE 'watch'
                END AS setup_type,
                (close - fut_close) / NULLIF(close, 0) AS ret,
                (close - fut_max_high) / NULLIF(close, 0) AS max_adverse
            FROM windowed
            WHERE fut_close IS NOT NULL AND fut_max_high IS NOT NULL
        )
        INSERT INTO {_TABLE_NAME} (
            as_of_ymd,
            side,
            setup_type,
            horizon_days,
            samples,
            win_rate,
            mean_ret,
            p25_ret,
            p10_ret,
            max_adverse,
            computed_at
        )
        SELECT
            ?,
            side,
            setup_type,
            ?,
            COUNT(*) AS samples,
            AVG(CASE WHEN ret > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
            AVG(ret) AS mean_ret,
            quantile_cont(ret, 0.25) AS p25_ret,
            quantile_cont(ret, 0.10) AS p10_ret,
            MIN(max_adverse) AS max_adverse,
            CURRENT_TIMESTAMP
        FROM unioned
        WHERE ret IS NOT NULL
        GROUP BY side, setup_type
    """
    conn.execute(sql, [int(start_ymd), int(as_of_ymd), int(as_of_ymd), int(horizon_days)])


def refresh_swing_setup_stats(
    *,
    as_of_ymd: int | None = None,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    horizons: tuple[int, ...] = _DEFAULT_HORIZONS,
) -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        return {"ok": False, "reason": "legacy_analysis_disabled", "as_of_ymd": None, "rows": 0}
    with _REFRESH_RUN_LOCK:
        with get_conn() as conn:
            ensure_swing_setup_stats_schema(conn)
            if not (_table_exists(conn, "daily_bars") and _table_exists(conn, "ml_pred_20d")):
                return {"ok": False, "reason": "source_tables_missing", "as_of_ymd": None, "rows": 0}
            resolved_as_of = _resolve_latest_asof_ymd(conn, as_of_ymd)
            if resolved_as_of is None:
                return {"ok": False, "reason": "no_asof", "as_of_ymd": None, "rows": 0}
            start_ymd = _resolve_start_ymd(resolved_as_of, lookback_days)
            conn.execute(f"DELETE FROM {_TABLE_NAME} WHERE as_of_ymd = ?", [int(resolved_as_of)])
            for horizon in horizons:
                if int(horizon) <= 0:
                    continue
                _insert_horizon_rows(
                    conn,
                    as_of_ymd=int(resolved_as_of),
                    start_ymd=int(start_ymd),
                    horizon_days=int(horizon),
                )
            row = conn.execute(
                f"SELECT COUNT(*) FROM {_TABLE_NAME} WHERE as_of_ymd = ?",
                [int(resolved_as_of)],
            ).fetchone()
            inserted = int(row[0] or 0) if row else 0
        _load_snapshot_cached.cache_clear()
        return {"ok": True, "as_of_ymd": int(resolved_as_of), "rows": int(inserted)}


def ensure_latest_swing_setup_stats(
    *,
    min_interval_sec: float = _LATEST_ENSURE_MIN_INTERVAL_SEC,
) -> dict[str, Any]:
    if is_legacy_analysis_disabled():
        return {"ok": False, "reason": "legacy_analysis_disabled", "as_of_ymd": None, "rows": 0}
    interval = float(max(1.0, min_interval_sec))
    now_ts = datetime.now(timezone.utc).timestamp()
    with _LATEST_ENSURE_LOCK:
        checked_at = float(_LATEST_ENSURE_CACHE.get("checked_at") or 0.0)
        if now_ts - checked_at < interval:
            return {
                "ok": True,
                "reason": "throttled",
                "as_of_ymd": _LATEST_ENSURE_CACHE.get("stats_asof_ymd"),
                "rows": None,
            }

        with get_conn() as conn:
            ensure_swing_setup_stats_schema(conn)
            if not (_table_exists(conn, "daily_bars") and _table_exists(conn, "ml_pred_20d")):
                _LATEST_ENSURE_CACHE.update(
                    {"checked_at": now_ts, "source_asof_ymd": None, "stats_asof_ymd": None}
                )
                return {"ok": False, "reason": "source_tables_missing", "as_of_ymd": None, "rows": 0}

            source_asof_ymd = _resolve_latest_asof_ymd(conn, None)
            if source_asof_ymd is None:
                _LATEST_ENSURE_CACHE.update(
                    {"checked_at": now_ts, "source_asof_ymd": None, "stats_asof_ymd": None}
                )
                return {"ok": False, "reason": "no_asof", "as_of_ymd": None, "rows": 0}

            row = conn.execute(f"SELECT MAX(as_of_ymd) FROM {_TABLE_NAME}").fetchone()
            stats_asof_ymd = int(row[0]) if row and row[0] is not None else None

        _LATEST_ENSURE_CACHE.update(
            {
                "checked_at": now_ts,
                "source_asof_ymd": int(source_asof_ymd),
                "stats_asof_ymd": stats_asof_ymd,
            }
        )

        if stats_asof_ymd is not None and int(stats_asof_ymd) >= int(source_asof_ymd):
            return {
                "ok": True,
                "reason": "up_to_date",
                "as_of_ymd": int(stats_asof_ymd),
                "rows": None,
            }

        refreshed = refresh_swing_setup_stats(as_of_ymd=int(source_asof_ymd))
        _LATEST_ENSURE_CACHE.update(
            {
                "checked_at": datetime.now(timezone.utc).timestamp(),
                "source_asof_ymd": int(source_asof_ymd),
                "stats_asof_ymd": refreshed.get("as_of_ymd"),
            }
        )
        return refreshed


@lru_cache(maxsize=24)
def _load_snapshot_cached(as_of_ymd: int | None) -> dict[str, Any]:
    with get_conn() as conn:
        ensure_swing_setup_stats_schema(conn)
        if as_of_ymd is None:
            row = conn.execute(f"SELECT MAX(as_of_ymd) FROM {_TABLE_NAME}").fetchone()
        else:
            row = conn.execute(
                f"SELECT MAX(as_of_ymd) FROM {_TABLE_NAME} WHERE as_of_ymd <= ?",
                [int(as_of_ymd)],
            ).fetchone()
        resolved_as_of = int(row[0]) if row and row[0] is not None else None
        if resolved_as_of is None:
            return {"as_of_ymd": None, "rows": [], "by_key": {}, "side_means": {}}

        rows = conn.execute(
            f"""
            SELECT
                side,
                setup_type,
                horizon_days,
                samples,
                win_rate,
                mean_ret,
                p25_ret,
                p10_ret,
                max_adverse
            FROM {_TABLE_NAME}
            WHERE as_of_ymd = ?
            """,
            [int(resolved_as_of)],
        ).fetchall()

    out_rows: list[dict[str, Any]] = []
    by_key: dict[tuple[str, str, int], dict[str, Any]] = {}
    side_sums: dict[tuple[str, int], float] = {}
    side_counts: dict[tuple[str, int], int] = {}
    for row in rows:
        side = str(row[0] or "")
        setup_type = str(row[1] or "")
        horizon_days = int(row[2] or 0)
        payload = {
            "side": side,
            "setup_type": setup_type,
            "horizon_days": horizon_days,
            "samples": int(row[3] or 0),
            "win_rate": float(row[4]) if row[4] is not None else None,
            "mean_ret": float(row[5]) if row[5] is not None else None,
            "p25_ret": float(row[6]) if row[6] is not None else None,
            "p10_ret": float(row[7]) if row[7] is not None else None,
            "max_adverse": float(row[8]) if row[8] is not None else None,
        }
        out_rows.append(payload)
        by_key[(side, setup_type, horizon_days)] = payload
        if payload["mean_ret"] is not None and payload["samples"] > 0:
            side_key = (side, horizon_days)
            side_sums[side_key] = float(side_sums.get(side_key, 0.0)) + float(payload["mean_ret"]) * float(
                payload["samples"]
            )
            side_counts[side_key] = int(side_counts.get(side_key, 0)) + int(payload["samples"])

    side_means: dict[tuple[str, int], float] = {}
    for side_key, weighted_sum in side_sums.items():
        denom = float(max(1, side_counts.get(side_key, 0)))
        side_means[side_key] = float(weighted_sum / denom)

    return {
        "as_of_ymd": int(resolved_as_of),
        "rows": out_rows,
        "by_key": by_key,
        "side_means": side_means,
    }


def _normalize_setup_type(side: str, setup_type: str | None) -> str:
    text = str(setup_type or "").strip().lower()
    if side == "long":
        if text in {"breakout", "breakout20", "long_breakout_p2", "long_breakout"}:
            return "breakout"
        if text in {"rebound", "turn", "long_reversal_p1"}:
            return "rebound"
        if text in {"accumulation", "long_pullback_p3"}:
            return "accumulation"
        if text in {"continuation"}:
            return "continuation"
        return "watch"
    if text in {
        "breakdown",
        "short_crash_top_p3",
        "short_failed_high_p1",
        "short_box_fail_p2",
        "short_entry",
        "short_decision_down",
    }:
        return "breakdown"
    if text in {"pressure", "short_downtrend_p4", "short_ma20_break_p5"}:
        return "pressure"
    if text in {"continuation"}:
        return "continuation"
    return "watch"


def resolve_setup_expectancy(
    *,
    side: str,
    setup_type: str | None,
    horizon_days: int = 20,
    as_of_ymd: int | None = None,
    shrink_k: float = _SHRINK_K,
) -> dict[str, Any]:
    direction = "long" if str(side).strip().lower() == "long" else "short"
    normalized_setup = _normalize_setup_type(direction, setup_type)
    target_horizon = int(max(1, horizon_days))

    snapshot = _load_snapshot_cached(as_of_ymd)
    if not snapshot.get("rows"):
        with _REFRESH_LOCK:
            snapshot = _load_snapshot_cached(as_of_ymd)
            if not snapshot.get("rows"):
                refresh_swing_setup_stats(as_of_ymd=as_of_ymd)
                snapshot = _load_snapshot_cached(as_of_ymd)

    row = snapshot.get("by_key", {}).get((direction, normalized_setup, target_horizon))
    side_mean = snapshot.get("side_means", {}).get((direction, target_horizon))

    samples = int((row or {}).get("samples") or 0)
    mean_ret = (row or {}).get("mean_ret")
    if mean_ret is not None:
        mean_ret = float(mean_ret)
    if side_mean is not None:
        side_mean = float(side_mean)
    if side_mean is None:
        side_mean = float(mean_ret) if mean_ret is not None else 0.0
    if mean_ret is None:
        mean_ret = float(side_mean)

    alpha = float(samples) / float(samples + max(1.0, float(shrink_k)))
    shrunk_mean = float(alpha * float(mean_ret) + (1.0 - alpha) * float(side_mean))
    return {
        "asOfYmd": snapshot.get("as_of_ymd"),
        "side": direction,
        "setupType": normalized_setup,
        "horizonDays": target_horizon,
        "samples": int(samples),
        "winRate": float((row or {}).get("win_rate")) if (row or {}).get("win_rate") is not None else None,
        "meanRet": float(mean_ret),
        "shrunkMeanRet": float(shrunk_mean),
        "p25Ret": float((row or {}).get("p25_ret")) if (row or {}).get("p25_ret") is not None else None,
        "p10Ret": float((row or {}).get("p10_ret")) if (row or {}).get("p10_ret") is not None else None,
        "maxAdverse": float((row or {}).get("max_adverse")) if (row or {}).get("max_adverse") is not None else None,
        "sideMeanRet": float(side_mean),
    }


def compute_atr_pct_and_liquidity20d(daily_rows: list[tuple[Any, ...]]) -> tuple[float | None, float | None]:
    if not daily_rows:
        return None, None
    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    turnovers: list[float] = []
    for row in daily_rows:
        if len(row) < 5:
            continue
        close = row[4]
        high = row[2] if len(row) > 2 else None
        low = row[3] if len(row) > 3 else None
        volume = row[5] if len(row) > 5 else None
        try:
            c = float(close)
            h = float(high)
            l = float(low)
        except Exception:
            continue
        closes.append(c)
        highs.append(h)
        lows.append(l)
        try:
            v = float(volume) if volume is not None else 0.0
        except Exception:
            v = 0.0
        turnovers.append(max(0.0, c * max(0.0, v)))

    if len(closes) < 2:
        liquidity = None
        if turnovers:
            liquidity = float(sum(turnovers[-20:]) / float(min(20, len(turnovers))))
        return None, liquidity

    trs: list[float] = []
    for idx in range(1, len(closes)):
        prev_close = closes[idx - 1]
        tr = max(
            highs[idx] - lows[idx],
            abs(highs[idx] - prev_close),
            abs(lows[idx] - prev_close),
        )
        trs.append(float(max(0.0, tr)))
    atr_period = min(14, len(trs))
    atr = sum(trs[-atr_period:]) / float(max(1, atr_period))
    last_close = closes[-1]
    atr_pct = float(atr / last_close) if last_close > 0 else None
    liquidity = float(sum(turnovers[-20:]) / float(min(20, len(turnovers)))) if turnovers else None
    return atr_pct, liquidity
