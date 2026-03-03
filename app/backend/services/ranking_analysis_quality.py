from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

import duckdb

from app.core.config import config as core_config
from app.db.session import get_conn_for_path

TARGET_SCOPE = "ranking_analysis"
_GATE_CACHE_LOCK = Lock()
_GATE_CACHE: dict[str, Any] = {"loaded_at": None, "gates": None}
_GATE_CACHE_TTL_SEC = max(60, int(os.getenv("MEEMEE_RANK_GATE_CACHE_TTL_SEC", "300")))


def _connect_quality_db(*, read_only: bool):
    return get_conn_for_path(
        str(core_config.DB_PATH),
        timeout_sec=2.5,
        read_only=bool(read_only),
    )


def _now_jst() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=9)


def _ymd_int(dt: datetime) -> int:
    return int(dt.strftime("%Y%m%d"))


def _clip01(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _normalize_ymd(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    if 19_000_101 <= value <= 21_001_231:
        return value
    if value >= 1_000_000_000_000:
        try:
            return int(datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%Y%m%d"))
        except Exception:
            return None
    if value >= 1_000_000_000:
        try:
            return int(datetime.fromtimestamp(value, tz=timezone.utc).strftime("%Y%m%d"))
        except Exception:
            return None
    return None


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _ensure_quality_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ranking_analysis_quality_daily (
            as_of_ymd INTEGER,
            scope TEXT,
            precision_top30_20d DOUBLE,
            avg_ret20_net DOUBLE,
            ece DOUBLE,
            samples INTEGER,
            decision_match_rate DOUBLE,
            decision_match_samples INTEGER,
            rolling_precision_delta_pt DOUBLE,
            rolling_avg_ret_delta DOUBLE,
            rolling_target_met BOOLEAN,
            up_gate_defensive DOUBLE,
            up_gate_balanced DOUBLE,
            up_gate_aggressive DOUBLE,
            table_health_json TEXT,
            alerts_json TEXT,
            computed_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY(as_of_ymd, scope)
        )
        """
    )
    # Backfill columns for existing DBs that were created before quality extensions.
    for name, sql_type in (
        ("decision_match_rate", "DOUBLE"),
        ("decision_match_samples", "INTEGER"),
        ("rolling_precision_delta_pt", "DOUBLE"),
        ("rolling_avg_ret_delta", "DOUBLE"),
        ("rolling_target_met", "BOOLEAN"),
    ):
        if not _column_exists(conn, "ranking_analysis_quality_daily", name):
            conn.execute(f"ALTER TABLE ranking_analysis_quality_daily ADD COLUMN {name} {sql_type}")


@dataclass(frozen=True)
class TableSpec:
    name: str
    required_columns: tuple[str, ...]
    consumer_fit: float
    recency_profile: str
    freshness_expr: str | None


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec("daily_bars", ("code", "date", "c"), 1.00, "daily", "date"),
    TableSpec("daily_ma", ("code", "date", "ma20", "ma60"), 0.75, "daily", "date"),
    TableSpec("monthly_bars", ("code", "month", "c"), 0.80, "monthly", "month"),
    TableSpec("feature_snapshot_daily", ("code", "dt", "close", "ma20"), 0.80, "daily", "dt"),
    TableSpec("ml_pred_20d", ("code", "dt", "p_up"), 1.00, "daily", "dt"),
    TableSpec("phase_pred_daily", ("code", "dt", "early_score", "late_score", "body_score"), 0.90, "daily", "dt"),
    TableSpec("sell_analysis_daily", ("code", "dt", "p_down", "p_turn_down"), 0.95, "daily", "dt"),
    TableSpec("stock_scores", ("code", "score_a", "score_b", "updated_at"), 0.70, "daily", "updated_at"),
    TableSpec("stock_meta", ("code", "name", "score"), 0.70, "static", None),
    TableSpec("tickers", ("code", "name"), 0.70, "static", None),
    TableSpec("industry_master", ("code", "sector33_code", "sector33_name"), 0.60, "static", None),
    TableSpec("earnings_planned", ("code", "planned_date"), 0.55, "event", "planned_date"),
    TableSpec("ex_rights", ("code", "ex_date"), 0.55, "event", "COALESCE(last_rights_date, ex_date)"),
    TableSpec("edinetdb_analysis", ("edinet_code", "asof_date", "payload_json"), 0.30, "event", "asof_date"),
    TableSpec(
        "ranking_edinet_audit_daily",
        ("as_of_ymd", "code", "edinet_score_bonus"),
        0.45,
        "daily",
        "as_of_ymd",
    ),
)


def _score_freshness(*, max_ymd: int | None, as_of_ymd: int, profile: str) -> float:
    if profile == "static":
        return 1.0
    if max_ymd is None:
        return 0.0
    try:
        as_of_dt = datetime.strptime(str(as_of_ymd), "%Y%m%d")
        max_dt = datetime.strptime(str(max_ymd), "%Y%m%d")
    except Exception:
        return 0.0
    lag_days = max(0, int((as_of_dt.date() - max_dt.date()).days))
    if profile == "monthly":
        if lag_days <= 31:
            return 1.0
        if lag_days <= 62:
            return 0.75
        if lag_days <= 124:
            return 0.50
        return 0.20
    if profile == "event":
        if lag_days <= 7:
            return 1.0
        if lag_days <= 30:
            return 0.70
        if lag_days <= 90:
            return 0.40
        return 0.20
    if lag_days <= 1:
        return 1.0
    if lag_days <= 3:
        return 0.85
    if lag_days <= 7:
        return 0.65
    if lag_days <= 30:
        return 0.35
    return 0.10


def _coerce_freshness_ymd(conn: duckdb.DuckDBPyConnection, table_name: str, expr: str | None) -> int | None:
    if not expr:
        return None
    if not _table_exists(conn, table_name):
        return None
    try:
        row = conn.execute(f"SELECT MAX({expr}) FROM {table_name}").fetchone()
    except Exception:
        return None
    if not row:
        return None
    raw = row[0]
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return int(raw.strftime("%Y%m%d"))
    if hasattr(raw, "strftime"):
        try:
            return int(raw.strftime("%Y%m%d"))
        except Exception:
            return None
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        if "-" in text:
            try:
                return int(datetime.fromisoformat(text[:10]).strftime("%Y%m%d"))
            except Exception:
                return None
        if text.isdigit():
            return _normalize_ymd(int(text))
        return None
    return _normalize_ymd(raw)


def _column_exists(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = ? AND column_name = ?
        LIMIT 1
        """,
        [table_name, column_name],
    ).fetchone()
    return row is not None


def _optional_column_expr(
    conn: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    column_name: str,
    sql_type: str = "DOUBLE",
) -> str:
    return column_name if _column_exists(conn, table_name, column_name) else f"NULL::{sql_type}"


def _table_health_snapshot(conn: duckdb.DuckDBPyConnection, *, as_of_ymd: int) -> list[dict[str, Any]]:
    universe_row = conn.execute("SELECT COUNT(DISTINCT code) FROM daily_bars").fetchone()
    universe_codes = int(universe_row[0] or 0) if universe_row else 0
    table_health: list[dict[str, Any]] = []
    for spec in TABLE_SPECS:
        exists = _table_exists(conn, spec.name)
        row_count = 0
        distinct_codes = None
        if exists:
            try:
                row = conn.execute(f"SELECT COUNT(*) FROM {spec.name}").fetchone()
                row_count = int(row[0] or 0) if row else 0
            except Exception:
                row_count = 0
            if _column_exists(conn, spec.name, "code"):
                try:
                    row = conn.execute(f"SELECT COUNT(DISTINCT code) FROM {spec.name}").fetchone()
                    distinct_codes = int(row[0] or 0) if row else 0
                except Exception:
                    distinct_codes = 0
        coverage = (
            _clip01(float(distinct_codes) / float(max(1, universe_codes)))
            if distinct_codes is not None
            else (1.0 if row_count > 0 else 0.0)
        )
        completeness_hits = 0
        for column in spec.required_columns:
            if exists and _column_exists(conn, spec.name, column):
                completeness_hits += 1
        completeness = (
            float(completeness_hits) / float(max(1, len(spec.required_columns)))
            if spec.required_columns
            else (1.0 if exists else 0.0)
        )
        freshness_ymd = _coerce_freshness_ymd(conn, spec.name, spec.freshness_expr)
        freshness = _score_freshness(max_ymd=freshness_ymd, as_of_ymd=as_of_ymd, profile=spec.recency_profile)
        consumer_fit = _clip01(spec.consumer_fit)
        util_score = (
            0.40 * coverage
            + 0.30 * freshness
            + 0.20 * consumer_fit
            + 0.10 * completeness
        )
        status = "Active" if util_score >= 0.75 else "Partial" if util_score >= 0.40 else "Dormant"
        table_health.append(
            {
                "table": spec.name,
                "exists": bool(exists),
                "rows": int(row_count),
                "distinctCodes": int(distinct_codes) if isinstance(distinct_codes, int) else None,
                "coverage": round(float(coverage), 4),
                "freshness": round(float(freshness), 4),
                "consumerFit": round(float(consumer_fit), 4),
                "completeness": round(float(completeness), 4),
                "utilScore": round(float(util_score), 4),
                "status": status,
                "freshnessAsOf": freshness_ymd,
            }
        )
    table_health.sort(key=lambda item: (item.get("utilScore", 0.0), item.get("table", "")))
    return table_health


def _aggregate_rank_rows(rows: list[tuple[Any, ...]]) -> dict[str, Any]:
    bins: dict[int, dict[str, float]] = {}
    samples = 0
    wins = 0
    ret_sum = 0.0
    for row in rows:
        p_up = _safe_float(row[2] if len(row) > 2 else None)
        ret = _safe_float(row[3] if len(row) > 3 else None)
        if p_up is None or ret is None:
            continue
        samples += 1
        if ret > 0:
            wins += 1
        ret_sum += ret
        bucket = max(0, min(9, int(math.floor(p_up * 10.0))))
        item = bins.setdefault(bucket, {"count": 0.0, "pred_sum": 0.0, "out_sum": 0.0})
        item["count"] += 1.0
        item["pred_sum"] += p_up
        item["out_sum"] += 1.0 if ret > 0 else 0.0

    if samples <= 0:
        return {
            "samples": 0,
            "precision": None,
            "avgRet20Net": None,
            "ece": None,
        }

    precision = float(wins / samples)
    avg_ret = float(ret_sum / samples)
    ece = 0.0
    for bin_item in bins.values():
        count = int(bin_item["count"])
        if count <= 0:
            continue
        avg_pred = float(bin_item["pred_sum"] / count)
        avg_out = float(bin_item["out_sum"] / count)
        ece += abs(avg_pred - avg_out) * (count / samples)

    return {
        "samples": int(samples),
        "precision": round(precision, 6),
        "avgRet20Net": round(avg_ret, 6),
        "ece": round(float(ece), 6),
    }


def _estimate_prob_up_gates(rows: list[tuple[Any, ...]]) -> dict[str, float]:
    thresholds = [round(0.50 + 0.01 * idx, 2) for idx in range(0, 26)]
    best_threshold = 0.55
    best_score = -1.0
    for threshold in thresholds:
        chosen = [row for row in rows if (_safe_float(row[2] if len(row) > 2 else None) or 0.0) >= threshold]
        if len(chosen) < 150:
            continue
        chosen_precision = sum(
            1
            for row in chosen
            if (_safe_float(row[3] if len(row) > 3 else None) or -1.0) > 0
        ) / len(chosen)
        chosen_avg_ret = sum((_safe_float(row[3] if len(row) > 3 else None) or 0.0) for row in chosen) / len(chosen)
        score = float(chosen_precision + max(0.0, min(0.04, chosen_avg_ret + 0.02)))
        if score > best_score:
            best_score = score
            best_threshold = threshold
    return {
        "defensive": round(min(0.75, best_threshold + 0.03), 3),
        "balanced": round(best_threshold, 3),
        "aggressive": round(max(0.50, best_threshold - 0.02), 3),
    }


def _compute_rolling_6m(
    *,
    rows: list[tuple[Any, ...]],
    as_of_ymd: int,
) -> dict[str, Any]:
    as_of_dt = datetime.strptime(str(as_of_ymd), "%Y%m%d")
    recent_start = int((as_of_dt - timedelta(days=180)).strftime("%Y%m%d"))
    baseline_start = int((as_of_dt - timedelta(days=360)).strftime("%Y%m%d"))

    recent_rows = [row for row in rows if recent_start <= int(row[0]) <= as_of_ymd]
    baseline_rows = [row for row in rows if baseline_start <= int(row[0]) < recent_start]

    recent_stats = _aggregate_rank_rows(recent_rows)
    baseline_stats = _aggregate_rank_rows(baseline_rows)

    recent_precision = _safe_float(recent_stats.get("precision"))
    baseline_precision = _safe_float(baseline_stats.get("precision"))
    recent_avg_ret = _safe_float(recent_stats.get("avgRet20Net"))
    baseline_avg_ret = _safe_float(baseline_stats.get("avgRet20Net"))

    delta_precision_pt = None
    delta_avg_ret = None
    meets_target = None
    if (
        recent_precision is not None
        and baseline_precision is not None
        and recent_avg_ret is not None
        and baseline_avg_ret is not None
        and int(baseline_stats.get("samples") or 0) >= 300
    ):
        delta_precision_pt = (recent_precision - baseline_precision) * 100.0
        delta_avg_ret = recent_avg_ret - baseline_avg_ret
        meets_target = bool(delta_precision_pt >= 2.0 and delta_avg_ret >= 0.0)

    return {
        "recentDays": 180,
        "baselineDays": 180,
        "recentSamples": int(recent_stats.get("samples") or 0),
        "baselineSamples": int(baseline_stats.get("samples") or 0),
        "recentPrecisionTop30_20d": recent_precision,
        "baselinePrecisionTop30_20d": baseline_precision,
        "deltaPrecisionPt": round(float(delta_precision_pt), 4) if delta_precision_pt is not None else None,
        "recentAvgRet20Net": recent_avg_ret,
        "baselineAvgRet20Net": baseline_avg_ret,
        "deltaAvgRet20Net": round(float(delta_avg_ret), 6) if delta_avg_ret is not None else None,
        "meetsTarget": meets_target,
    }


def _compute_decision_alignment(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of_ymd: int,
    up_prob_gate: float | None,
) -> dict[str, Any]:
    try:
        from app.backend.services.analysis_decision import build_analysis_decision
    except Exception:
        return {
            "matchRate": None,
            "matched": 0,
            "sampleSize": 0,
            "target": 0.95,
            "meetsTarget": None,
            "byDirection": {"up": {"matched": 0, "sampleSize": 0}, "down": {"matched": 0, "sampleSize": 0}},
        }

    if not _table_exists(conn, "ml_pred_20d") or not _column_exists(conn, "ml_pred_20d", "p_up"):
        return {
            "matchRate": None,
            "matched": 0,
            "sampleSize": 0,
            "target": 0.95,
            "meetsTarget": None,
            "byDirection": {"up": {"matched": 0, "sampleSize": 0}, "down": {"matched": 0, "sampleSize": 0}},
        }

    p_up_10_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_up_10")
    p_up_5_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_up_5")
    p_down_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_down")
    p_turn_up_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_turn_up")
    p_turn_down_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_turn_down")
    p_turn_down_20_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_turn_down_20")
    p_turn_down_10_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_turn_down_10")
    p_turn_down_5_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_turn_down_5")
    ev20_net_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="ev20_net")

    rows = conn.execute(
        f"""
        WITH preds_base AS (
            SELECT
                code,
                CASE
                    WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                    WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt / 1000), '%Y%m%d') AS INTEGER)
                    WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                COALESCE(p_up, {p_up_10_expr}, {p_up_5_expr}) AS p_up,
                {p_down_expr} AS p_down,
                {p_turn_up_expr} AS p_turn_up,
                COALESCE({p_turn_down_expr}, {p_turn_down_20_expr}, {p_turn_down_10_expr}, {p_turn_down_5_expr}) AS p_turn_down,
                {ev20_net_expr} AS ev20_net
            FROM ml_pred_20d
        ),
        preds AS (
            SELECT *
            FROM preds_base
            WHERE ymd = ?
        ),
        up_items AS (
            SELECT
                'up' AS direction,
                code,
                p_up,
                p_down,
                p_turn_up,
                p_turn_down,
                ev20_net,
                ROW_NUMBER() OVER (ORDER BY p_up DESC NULLS LAST, code ASC) AS rn
            FROM preds
            WHERE p_up IS NOT NULL
        ),
        down_items AS (
            SELECT
                'down' AS direction,
                code,
                p_up,
                p_down,
                p_turn_up,
                p_turn_down,
                ev20_net,
                ROW_NUMBER() OVER (ORDER BY COALESCE(p_down, 1.0 - p_up) DESC NULLS LAST, code ASC) AS rn
            FROM preds
            WHERE COALESCE(p_down, 1.0 - p_up) IS NOT NULL
        )
        SELECT direction, code, p_up, p_down, p_turn_up, p_turn_down, ev20_net
        FROM up_items
        WHERE rn <= 30
        UNION ALL
        SELECT direction, code, p_up, p_down, p_turn_up, p_turn_down, ev20_net
        FROM down_items
        WHERE rn <= 30
        """,
        [int(as_of_ymd)],
    ).fetchall()

    if not rows:
        return {
            "matchRate": None,
            "matched": 0,
            "sampleSize": 0,
            "target": 0.95,
            "meetsTarget": None,
            "byDirection": {"up": {"matched": 0, "sampleSize": 0}, "down": {"matched": 0, "sampleSize": 0}},
        }

    up_gate = _safe_float(up_prob_gate)
    if up_gate is None:
        up_gate = 0.58
    down_prob_gate = 0.52
    down_turn_gate = 0.52

    matched = 0
    sample_size = 0
    by_direction: dict[str, dict[str, Any]] = {"up": {"matched": 0, "sampleSize": 0}, "down": {"matched": 0, "sampleSize": 0}}
    for row in rows:
        direction = str(row[0] or "").strip().lower()
        if direction not in {"up", "down"}:
            continue
        p_up = _safe_float(row[2])
        p_down = _safe_float(row[3])
        if p_down is None and p_up is not None:
            p_down = 1.0 - p_up
        p_turn_down = _safe_float(row[5])
        decision = build_analysis_decision(
            analysis_p_up=p_up,
            analysis_p_down=p_down,
            analysis_p_turn_up=_safe_float(row[4]),
            analysis_p_turn_down=p_turn_down,
            analysis_ev_net=_safe_float(row[6]),
            playbook_up_score_bonus=None,
            playbook_down_score_bonus=None,
            additive_signals=None,
            sell_analysis=None,
        )
        tone = str((decision or {}).get("tone") or "").strip().lower()
        if tone not in {"up", "down", "neutral"}:
            continue

        if direction == "up":
            entry_qualified = p_up is not None and p_up >= up_gate
        else:
            entry_qualified = (
                p_down is not None
                and p_down >= down_prob_gate
                and p_turn_down is not None
                and p_turn_down >= down_turn_gate
            )
        expected = direction if entry_qualified else "neutral"
        sample_size += 1
        by_direction[direction]["sampleSize"] = int(by_direction[direction]["sampleSize"]) + 1
        if tone == expected:
            matched += 1
            by_direction[direction]["matched"] = int(by_direction[direction]["matched"]) + 1

    for direction in ("up", "down"):
        dir_samples = int(by_direction[direction]["sampleSize"])
        dir_matched = int(by_direction[direction]["matched"])
        by_direction[direction]["matchRate"] = round(float(dir_matched / dir_samples), 6) if dir_samples > 0 else None

    match_rate = float(matched / sample_size) if sample_size > 0 else None
    meets_target = bool(match_rate >= 0.95) if match_rate is not None else None
    return {
        "matchRate": round(match_rate, 6) if match_rate is not None else None,
        "matched": int(matched),
        "sampleSize": int(sample_size),
        "target": 0.95,
        "meetsTarget": meets_target,
        "byDirection": by_direction,
    }


def _compute_kpi_snapshot(conn: duckdb.DuckDBPyConnection, *, as_of_ymd: int) -> dict[str, Any]:
    as_of_dt = datetime.strptime(str(as_of_ymd), "%Y%m%d")
    lookback_start = int((as_of_dt - timedelta(days=180)).strftime("%Y%m%d"))
    baseline_start = int((as_of_dt - timedelta(days=360)).strftime("%Y%m%d"))
    if not _table_exists(conn, "ml_pred_20d") or not _column_exists(conn, "ml_pred_20d", "p_up"):
        return {
            "precisionTop30_20d": None,
            "avgRet20Net": None,
            "ece": None,
            "samples": 0,
            "lookbackDays": 180,
            "rolling6m": {
                "recentDays": 180,
                "baselineDays": 180,
                "recentSamples": 0,
                "baselineSamples": 0,
                "recentPrecisionTop30_20d": None,
                "baselinePrecisionTop30_20d": None,
                "deltaPrecisionPt": None,
                "recentAvgRet20Net": None,
                "baselineAvgRet20Net": None,
                "deltaAvgRet20Net": None,
                "meetsTarget": None,
            },
            "reestimatedProbGate": {
                "defensive": 0.58,
                "balanced": 0.55,
                "aggressive": 0.53,
            },
        }

    p_up_10_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_up_10")
    p_up_5_expr = _optional_column_expr(conn, table_name="ml_pred_20d", column_name="p_up_5")
    pred_non_null_checks = ["p_up IS NOT NULL"]
    if p_up_10_expr != "NULL::DOUBLE":
        pred_non_null_checks.append("p_up_10 IS NOT NULL")
    if p_up_5_expr != "NULL::DOUBLE":
        pred_non_null_checks.append("p_up_5 IS NOT NULL")
    pred_non_null_where = " OR ".join(pred_non_null_checks)

    rows = conn.execute(
        f"""
        WITH bars AS (
            SELECT
                code,
                CASE
                    WHEN date BETWEEN 19000101 AND 20991231 THEN date
                    WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                    WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                c AS close
            FROM daily_bars
            WHERE c IS NOT NULL
        ),
        bars_fwd AS (
            SELECT
                code,
                ymd,
                close,
                LEAD(close, 20) OVER (PARTITION BY code ORDER BY ymd) AS close_fwd20
            FROM bars
            WHERE ymd IS NOT NULL
        ),
        preds AS (
            SELECT
                code,
                CASE
                    WHEN dt BETWEEN 19000101 AND 20991231 THEN dt
                    WHEN dt >= 1000000000000 THEN CAST(strftime(to_timestamp(dt / 1000), '%Y%m%d') AS INTEGER)
                    WHEN dt >= 1000000000 THEN CAST(strftime(to_timestamp(dt), '%Y%m%d') AS INTEGER)
                    ELSE NULL
                END AS ymd,
                -- KPI is 20-business-day based; prioritize p_up (20D) and use shorter horizons only as fallback.
                COALESCE(p_up, {p_up_10_expr}, {p_up_5_expr}) AS p_up
            FROM ml_pred_20d
            WHERE {pred_non_null_where}
        ),
        joined AS (
            SELECT
                p.ymd,
                p.code,
                p.p_up,
                CASE
                    WHEN b.close IS NULL OR b.close_fwd20 IS NULL OR b.close <= 0 THEN NULL
                    ELSE (b.close_fwd20 / b.close - 1.0) - 0.002
                END AS ret20_net
            FROM preds p
            JOIN bars_fwd b
                ON b.code = p.code
               AND b.ymd = p.ymd
            WHERE p.ymd BETWEEN ? AND ?
        ),
        ranked AS (
            SELECT
                ymd,
                code,
                p_up,
                ret20_net,
                ROW_NUMBER() OVER (PARTITION BY ymd ORDER BY p_up DESC, code ASC) AS rn
            FROM joined
            WHERE ret20_net IS NOT NULL
        )
        SELECT ymd, code, p_up, ret20_net
        FROM ranked
        WHERE rn <= 30
        ORDER BY ymd, rn
        """,
        [baseline_start, as_of_ymd],
    ).fetchall()

    if not rows:
        return {
            "precisionTop30_20d": None,
            "avgRet20Net": None,
            "ece": None,
            "samples": 0,
            "lookbackDays": 180,
            "rolling6m": {
                "recentDays": 180,
                "baselineDays": 180,
                "recentSamples": 0,
                "baselineSamples": 0,
                "recentPrecisionTop30_20d": None,
                "baselinePrecisionTop30_20d": None,
                "deltaPrecisionPt": None,
                "recentAvgRet20Net": None,
                "baselineAvgRet20Net": None,
                "deltaAvgRet20Net": None,
                "meetsTarget": None,
            },
            "reestimatedProbGate": {
                "defensive": 0.58,
                "balanced": 0.55,
                "aggressive": 0.53,
            },
        }

    recent_rows = [row for row in rows if lookback_start <= int(row[0]) <= as_of_ymd]
    recent_stats = _aggregate_rank_rows(recent_rows)
    rolling_6m = _compute_rolling_6m(rows=rows, as_of_ymd=as_of_ymd)
    gates = _estimate_prob_up_gates(recent_rows)
    return {
        "precisionTop30_20d": recent_stats.get("precision"),
        "avgRet20Net": recent_stats.get("avgRet20Net"),
        "ece": recent_stats.get("ece"),
        "samples": int(recent_stats.get("samples") or 0),
        "lookbackDays": 180,
        "rolling6m": rolling_6m,
        "reestimatedProbGate": gates,
    }


def _build_alerts(*, table_health: list[dict[str, Any]], kpi_snapshot: dict[str, Any]) -> list[str]:
    alerts: list[str] = []
    for item in table_health:
        table = str(item.get("table") or "")
        status = str(item.get("status") or "")
        coverage = _safe_float(item.get("coverage")) or 0.0
        if table in {"daily_bars", "ml_pred_20d", "phase_pred_daily", "sell_analysis_daily"} and status != "Active":
            alerts.append(f"{table}: core table status={status}")
        if table == "stock_scores" and coverage < 0.60:
            alerts.append(f"stock_scores: coverage={coverage:.2f} (<0.60 target)")
        if table in {"edinetdb_analysis", "ranking_edinet_audit_daily"} and status == "Dormant":
            alerts.append(f"{table}: dormant (feature-flag/samples unmet)")
    precision = _safe_float(kpi_snapshot.get("precisionTop30_20d"))
    ece = _safe_float(kpi_snapshot.get("ece"))
    if precision is None:
        alerts.append("KPI: precisionTop30_20d unavailable")
    elif precision < 0.52:
        alerts.append(f"KPI: precisionTop30_20d={precision:.3f} below 0.52")
    if ece is not None and ece > 0.08:
        alerts.append(f"KPI: ece={ece:.3f} above 0.08")
    rolling = kpi_snapshot.get("rolling6m") if isinstance(kpi_snapshot, dict) else None
    if isinstance(rolling, dict):
        meets_target = rolling.get("meetsTarget")
        delta_precision_pt = _safe_float(rolling.get("deltaPrecisionPt"))
        delta_avg_ret = _safe_float(rolling.get("deltaAvgRet20Net"))
        if meets_target is False:
            precision_text = f"{delta_precision_pt:+.2f}pt" if delta_precision_pt is not None else "n/a"
            ret_text = f"{delta_avg_ret:+.4f}" if delta_avg_ret is not None else "n/a"
            alerts.append(f"KPI rolling6m target unmet: precision={precision_text}, avgRet20Net={ret_text}")
    alignment = kpi_snapshot.get("decisionAlignment") if isinstance(kpi_snapshot, dict) else None
    if isinstance(alignment, dict):
        match_rate = _safe_float(alignment.get("matchRate"))
        if match_rate is None:
            alerts.append("Decision alignment: unavailable")
        elif match_rate < 0.95:
            alerts.append(f"Decision alignment: matchRate={match_rate:.3f} below 0.95")
    return alerts


def _persist_snapshot(
    conn: duckdb.DuckDBPyConnection,
    *,
    as_of_ymd: int,
    table_health: list[dict[str, Any]],
    kpi_snapshot: dict[str, Any],
    alerts: list[str],
) -> None:
    _ensure_quality_table(conn)
    gates = kpi_snapshot.get("reestimatedProbGate") if isinstance(kpi_snapshot, dict) else {}
    alignment = kpi_snapshot.get("decisionAlignment") if isinstance(kpi_snapshot, dict) else {}
    rolling = kpi_snapshot.get("rolling6m") if isinstance(kpi_snapshot, dict) else {}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        """
        INSERT INTO ranking_analysis_quality_daily (
            as_of_ymd,
            scope,
            precision_top30_20d,
            avg_ret20_net,
            ece,
            samples,
            decision_match_rate,
            decision_match_samples,
            rolling_precision_delta_pt,
            rolling_avg_ret_delta,
            rolling_target_met,
            up_gate_defensive,
            up_gate_balanced,
            up_gate_aggressive,
            table_health_json,
            alerts_json,
            computed_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(as_of_ymd, scope) DO UPDATE SET
            precision_top30_20d = excluded.precision_top30_20d,
            avg_ret20_net = excluded.avg_ret20_net,
            ece = excluded.ece,
            samples = excluded.samples,
            decision_match_rate = excluded.decision_match_rate,
            decision_match_samples = excluded.decision_match_samples,
            rolling_precision_delta_pt = excluded.rolling_precision_delta_pt,
            rolling_avg_ret_delta = excluded.rolling_avg_ret_delta,
            rolling_target_met = excluded.rolling_target_met,
            up_gate_defensive = excluded.up_gate_defensive,
            up_gate_balanced = excluded.up_gate_balanced,
            up_gate_aggressive = excluded.up_gate_aggressive,
            table_health_json = excluded.table_health_json,
            alerts_json = excluded.alerts_json,
            computed_at = excluded.computed_at,
            updated_at = excluded.updated_at
        """,
        [
            int(as_of_ymd),
            TARGET_SCOPE,
            _safe_float(kpi_snapshot.get("precisionTop30_20d")),
            _safe_float(kpi_snapshot.get("avgRet20Net")),
            _safe_float(kpi_snapshot.get("ece")),
            int(kpi_snapshot.get("samples") or 0),
            _safe_float(alignment.get("matchRate") if isinstance(alignment, dict) else None),
            int(alignment.get("sampleSize") or 0) if isinstance(alignment, dict) else 0,
            _safe_float(rolling.get("deltaPrecisionPt") if isinstance(rolling, dict) else None),
            _safe_float(rolling.get("deltaAvgRet20Net") if isinstance(rolling, dict) else None),
            bool(rolling.get("meetsTarget")) if isinstance(rolling.get("meetsTarget"), bool) else None,
            _safe_float(gates.get("defensive") if isinstance(gates, dict) else None),
            _safe_float(gates.get("balanced") if isinstance(gates, dict) else None),
            _safe_float(gates.get("aggressive") if isinstance(gates, dict) else None),
            json.dumps(table_health, ensure_ascii=False),
            json.dumps(alerts, ensure_ascii=False),
            now,
            now,
        ],
    )


def compute_ranking_analysis_quality_snapshot(
    *,
    as_of_ymd: int | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    as_of = int(as_of_ymd) if as_of_ymd is not None else _ymd_int(_now_jst())
    with _connect_quality_db(read_only=(not persist)) as conn:
        table_health = _table_health_snapshot(conn, as_of_ymd=as_of)
        kpi_snapshot = _compute_kpi_snapshot(conn, as_of_ymd=as_of)
        gates = kpi_snapshot.get("reestimatedProbGate") if isinstance(kpi_snapshot, dict) else {}
        up_gate = _safe_float((gates or {}).get("balanced")) if isinstance(gates, dict) else None
        kpi_snapshot["decisionAlignment"] = _compute_decision_alignment(
            conn,
            as_of_ymd=as_of,
            up_prob_gate=up_gate,
        )
        alerts = _build_alerts(table_health=table_health, kpi_snapshot=kpi_snapshot)
        if persist:
            _persist_snapshot(
                conn,
                as_of_ymd=as_of,
                table_health=table_health,
                kpi_snapshot=kpi_snapshot,
                alerts=alerts,
            )
    return {
        "as_of": as_of,
        "table_health": table_health,
        "kpi_snapshot": kpi_snapshot,
        "alerts": alerts,
    }


def get_latest_prob_up_gates() -> dict[str, float] | None:
    now = datetime.now(timezone.utc)
    with _GATE_CACHE_LOCK:
        loaded_at = _GATE_CACHE.get("loaded_at")
        payload = _GATE_CACHE.get("gates")
        if isinstance(loaded_at, datetime) and isinstance(payload, dict):
            age_sec = (now - loaded_at).total_seconds()
            if age_sec <= float(_GATE_CACHE_TTL_SEC):
                return payload

    try:
        with _connect_quality_db(read_only=True) as conn:
            if not _table_exists(conn, "ranking_analysis_quality_daily"):
                return None
            row = conn.execute(
                """
                SELECT up_gate_defensive, up_gate_balanced, up_gate_aggressive
                FROM ranking_analysis_quality_daily
                WHERE scope = ?
                ORDER BY as_of_ymd DESC
                LIMIT 1
                """,
                [TARGET_SCOPE],
            ).fetchone()
            if not row:
                return None
            gates = {
                "defensive": _safe_float(row[0]) or 0.58,
                "balanced": _safe_float(row[1]) or 0.55,
                "aggressive": _safe_float(row[2]) or 0.53,
            }
    except Exception:
        return None

    with _GATE_CACHE_LOCK:
        _GATE_CACHE["loaded_at"] = now
        _GATE_CACHE["gates"] = gates
    return gates


def get_ranking_analysis_review(
    *,
    days: int = 7,
    min_occurrence: int = 2,
) -> dict[str, Any]:
    window_days = max(1, min(int(days or 7), 90))
    threshold = max(1, int(min_occurrence or 2))
    snapshots: list[dict[str, Any]] = []
    alert_count_map: dict[str, int] = {}
    table_count_map: dict[str, int] = {}
    latest_table_state: dict[str, dict[str, Any]] = {}

    try:
        with _connect_quality_db(read_only=True) as conn:
            if not _table_exists(conn, "ranking_analysis_quality_daily"):
                return {
                    "as_of": None,
                    "windowDays": window_days,
                    "minOccurrence": threshold,
                    "snapshots": [],
                    "reviewTargets": [],
                    "alertsFrequency": [],
                }
            rows = conn.execute(
                """
                SELECT
                    as_of_ymd,
                    precision_top30_20d,
                    avg_ret20_net,
                    ece,
                    decision_match_rate,
                    decision_match_samples,
                    rolling_precision_delta_pt,
                    rolling_avg_ret_delta,
                    rolling_target_met,
                    table_health_json,
                    alerts_json
                FROM ranking_analysis_quality_daily
                WHERE scope = ?
                ORDER BY as_of_ymd DESC
                LIMIT ?
                """,
                [TARGET_SCOPE, window_days],
            ).fetchall()
    except Exception:
        rows = []

    for row in rows:
        as_of_ymd = int(row[0] or 0)
        table_health_raw = row[9]
        alerts_raw = row[10]
        try:
            table_health = json.loads(table_health_raw) if table_health_raw else []
        except Exception:
            table_health = []
        try:
            alerts = json.loads(alerts_raw) if alerts_raw else []
        except Exception:
            alerts = []
        if not isinstance(table_health, list):
            table_health = []
        if not isinstance(alerts, list):
            alerts = []

        for alert in alerts:
            text = str(alert or "").strip()
            if not text:
                continue
            alert_count_map[text] = int(alert_count_map.get(text, 0)) + 1

        for item in table_health:
            if not isinstance(item, dict):
                continue
            table_name = str(item.get("table") or "").strip()
            if not table_name:
                continue
            status = str(item.get("status") or "").strip()
            if status not in {"Partial", "Dormant"}:
                continue
            table_count_map[table_name] = int(table_count_map.get(table_name, 0)) + 1
            latest_table_state.setdefault(table_name, item)

        snapshots.append(
            {
                "as_of": as_of_ymd if as_of_ymd > 0 else None,
                "kpi": {
                    "precisionTop30_20d": _safe_float(row[1]),
                    "avgRet20Net": _safe_float(row[2]),
                    "ece": _safe_float(row[3]),
                    "decisionMatchRate": _safe_float(row[4]),
                    "decisionMatchSamples": int(row[5] or 0),
                    "rollingDeltaPrecisionPt": _safe_float(row[6]),
                    "rollingDeltaAvgRet20Net": _safe_float(row[7]),
                    "rollingTargetMet": bool(row[8]) if isinstance(row[8], bool) else None,
                },
                "alerts": [str(item) for item in alerts if str(item or "").strip()],
            }
        )

    review_targets: list[dict[str, Any]] = []
    for table_name, count in table_count_map.items():
        if count < threshold:
            continue
        latest = latest_table_state.get(table_name) or {}
        review_targets.append(
            {
                "type": "table_health",
                "table": table_name,
                "status": str(latest.get("status") or ""),
                "occurrence": int(count),
                "latestUtilScore": _safe_float(latest.get("utilScore")),
                "latestCoverage": _safe_float(latest.get("coverage")),
                "latestFreshness": _safe_float(latest.get("freshness")),
            }
        )

    for alert_text, count in alert_count_map.items():
        if count < threshold:
            continue
        review_targets.append(
            {
                "type": "alert",
                "message": alert_text,
                "occurrence": int(count),
            }
        )

    review_targets.sort(
        key=lambda item: (
            -int(item.get("occurrence") or 0),
            str(item.get("table") or item.get("message") or ""),
        )
    )
    alerts_frequency = [
        {"message": key, "occurrence": int(value)}
        for key, value in sorted(alert_count_map.items(), key=lambda pair: (-pair[1], pair[0]))
    ]
    latest_as_of = snapshots[0].get("as_of") if snapshots else None
    return {
        "as_of": latest_as_of,
        "windowDays": window_days,
        "minOccurrence": threshold,
        "snapshots": snapshots,
        "reviewTargets": review_targets,
        "alertsFrequency": alerts_frequency,
    }
