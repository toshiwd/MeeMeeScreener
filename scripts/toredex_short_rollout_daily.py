from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.services.toredex_runner import run_live
from app.core.config import config


DEFAULT_PROD_SEASON = "toredex_live_short_hybrid_prod_20260304"


def _load_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"config file must be object: {path}")
    return payload


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _build_override(
    *,
    override_json_path: str | None,
    operating_mode: str | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if override_json_path:
        out.update(_load_json(override_json_path))
    if operating_mode:
        mode = str(operating_mode).strip().lower()
        if mode in {"champion", "challenger"}:
            out["operatingMode"] = mode
    return out


def _resolve_db_path(explicit: str | None) -> str:
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    env = os.getenv("STOCKS_DB_PATH")
    if env:
        return str(Path(env).expanduser().resolve())
    return str(config.DB_PATH)


def _fetch_window_metrics(
    *,
    conn: duckdb.DuckDBPyConnection,
    season_id: str,
    window_days: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          "asOf",
          net_cum_return_pct,
          max_drawdown_pct,
          risk_gate_pass,
          risk_gate_reason,
          short_units,
          long_units
        FROM toredex_daily_metrics
        WHERE season_id = ?
        ORDER BY "asOf" DESC
        LIMIT ?
        """,
        [season_id, int(window_days)],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "as_of": row[0].isoformat() if isinstance(row[0], date) else str(row[0]),
                "net_cum_return_pct": float(row[1]) if row[1] is not None else None,
                "max_drawdown_pct": float(row[2]) if row[2] is not None else None,
                "risk_gate_pass": _to_bool(row[3]),
                "risk_gate_reason": str(row[4] or ""),
                "short_units": int(row[5] or 0),
                "long_units": int(row[6] or 0),
            }
        )
    out.reverse()
    return out


def _fetch_short_entries(
    *,
    conn: duckdb.DuckDBPyConnection,
    season_id: str,
    window_days: int,
) -> int:
    row = conn.execute(
        """
        WITH last_days AS (
          SELECT "asOf"
          FROM toredex_daily_metrics
          WHERE season_id = ?
          ORDER BY "asOf" DESC
          LIMIT ?
        )
        SELECT COUNT(*)
        FROM toredex_trades t
        WHERE t.season_id = ?
          AND UPPER(t.side) = 'SHORT'
          AND t.delta_units > 0
          AND t."asOf" IN (SELECT "asOf" FROM last_days)
        """,
        [season_id, int(window_days), season_id],
    ).fetchone()
    return int((row or [0])[0] or 0)


def _fetch_short_entries_by_day(
    *,
    conn: duckdb.DuckDBPyConnection,
    season_id: str,
    window_days: int,
) -> dict[str, int]:
    rows = conn.execute(
        """
        WITH last_days AS (
          SELECT "asOf"
          FROM toredex_daily_metrics
          WHERE season_id = ?
          ORDER BY "asOf" DESC
          LIMIT ?
        )
        SELECT
          t."asOf",
          COUNT(*) AS short_entries
        FROM toredex_trades t
        WHERE t.season_id = ?
          AND UPPER(t.side) = 'SHORT'
          AND t.delta_units > 0
          AND t."asOf" IN (SELECT "asOf" FROM last_days)
        GROUP BY t."asOf"
        ORDER BY t."asOf"
        """,
        [season_id, int(window_days), season_id],
    ).fetchall()
    out: dict[str, int] = {}
    for row in rows:
        day_key = row[0].isoformat() if isinstance(row[0], date) else str(row[0])
        out[day_key] = int(row[1] or 0)
    return out


def _fetch_crash_boost_entries_by_day(
    *,
    conn: duckdb.DuckDBPyConnection,
    season_id: str,
    window_days: int,
) -> dict[str, int]:
    rows = conn.execute(
        """
        WITH last_days AS (
          SELECT "asOf"
          FROM toredex_daily_metrics
          WHERE season_id = ?
          ORDER BY "asOf" DESC
          LIMIT ?
        )
        SELECT
          d."asOf",
          d.payload_json
        FROM toredex_decisions d
        WHERE d.season_id = ?
          AND d."asOf" IN (SELECT "asOf" FROM last_days)
        ORDER BY d."asOf"
        """,
        [season_id, int(window_days), season_id],
    ).fetchall()
    out: dict[str, int] = {}
    for row in rows:
        day_key = row[0].isoformat() if isinstance(row[0], date) else str(row[0])
        payload_text = str(row[1] or "")
        if not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except Exception:
            continue
        actions = payload.get("actions") if isinstance(payload, dict) else []
        if not isinstance(actions, list):
            continue
        count = 0
        for action in actions:
            if not isinstance(action, dict):
                continue
            side = str(action.get("side") or "").upper()
            delta_units = int(action.get("deltaUnits") or 0)
            notes = str(action.get("notes") or "").upper()
            if side == "SHORT" and delta_units > 0 and "CRASH_DIP_BOOST" in notes:
                count += 1
        if count > 0:
            out[day_key] = count
    return out


def _build_daily_observations(
    *,
    metrics: list[dict[str, Any]],
    short_entries_by_day: dict[str, int],
    crash_boost_entries_by_day: dict[str, int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric in metrics:
        as_of = str(metric.get("as_of") or "")
        row = dict(metric)
        row["short_entries"] = int(short_entries_by_day.get(as_of, 0))
        row["crash_boost_entries"] = int(crash_boost_entries_by_day.get(as_of, 0))
        rows.append(row)
    return rows


def _summarize_window(
    *,
    metrics: list[dict[str, Any]],
    short_entries: int,
    crash_boost_entries: int,
    rollback_max_dd: float,
) -> dict[str, Any]:
    if not metrics:
        return {
            "window_days": 0,
            "metric_days": 0,
            "short_entries": int(short_entries),
            "crash_boost_entries": int(crash_boost_entries),
            "pass_days": 0,
            "all_pass": False,
            "min_max_drawdown_pct": None,
            "promotion_gate_pass": False,
            "rollback_trigger": False,
            "rollback_reason": "NO_METRICS",
            "latest": None,
        }

    pass_days = sum(1 for row in metrics if _to_bool(row.get("risk_gate_pass")))
    all_pass = pass_days == len(metrics)
    dd_values = [float(row["max_drawdown_pct"]) for row in metrics if row.get("max_drawdown_pct") is not None]
    min_dd = min(dd_values) if dd_values else None
    latest = metrics[-1]

    promotion_gate_pass = bool(
        short_entries >= 1
        and all_pass
        and (min_dd is not None and min_dd >= float(rollback_max_dd))
    )
    rollback_trigger = bool(
        (not _to_bool(latest.get("risk_gate_pass")))
        or (
            latest.get("max_drawdown_pct") is not None
            and float(latest["max_drawdown_pct"]) < float(rollback_max_dd)
        )
    )
    rollback_reason = ""
    if rollback_trigger:
        if not _to_bool(latest.get("risk_gate_pass")):
            rollback_reason = str(latest.get("risk_gate_reason") or "RISK_GATE_FAIL")
        else:
            rollback_reason = f"MAX_DD({latest.get('max_drawdown_pct')} < {rollback_max_dd})"

    return {
        "window_days": len(metrics),
        "metric_days": len(metrics),
        "short_entries": int(short_entries),
        "crash_boost_entries": int(crash_boost_entries),
        "pass_days": int(pass_days),
        "all_pass": bool(all_pass),
        "min_max_drawdown_pct": float(min_dd) if min_dd is not None else None,
        "promotion_gate_pass": bool(promotion_gate_pass),
        "rollback_trigger": bool(rollback_trigger),
        "rollback_reason": rollback_reason,
        "latest": latest,
    }


def _default_output_path(season_id: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in season_id)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("tmp") / f"toredex_short_rollout_daily_{safe}_{ts}.json"


def _default_daily_log_path(season_id: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in season_id)
    return Path("tmp") / f"toredex_short_rollout_observations_{safe}.jsonl"


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False))
        handle.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TOREDEX short rollout live + monitor gate status.")
    parser.add_argument("--season-id", default=DEFAULT_PROD_SEASON)
    parser.add_argument("--asof", default="")
    parser.add_argument("--operating-mode", default="champion", choices=["champion", "challenger"])
    parser.add_argument("--config-override-json", default="")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--window-days", type=int, default=10)
    parser.add_argument("--rollback-max-dd", type=float, default=-8.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--monitor-only", action="store_true")
    parser.add_argument("--output", default="")
    parser.add_argument("--daily-log-path", default="")
    parser.add_argument("--append-log", action="store_true")
    parser.add_argument("--fail-on-rollback", action="store_true")
    args = parser.parse_args()

    season_id = str(args.season_id).strip()
    if not season_id:
        raise ValueError("season-id is required")
    window_days = max(1, int(args.window_days))

    override = _build_override(
        override_json_path=(str(args.config_override_json).strip() or None),
        operating_mode=(str(args.operating_mode).strip() or None),
    )

    run_result: dict[str, Any] | None = None
    if not bool(args.monitor_only):
        run_result = run_live(
            season_id=season_id,
            as_of=(str(args.asof).strip() or None),
            dry_run=bool(args.dry_run),
            config_override=override if override else None,
        )

    db_path = _resolve_db_path(str(args.db_path).strip() or None)
    with duckdb.connect(db_path, read_only=True) as conn:
        metrics = _fetch_window_metrics(conn=conn, season_id=season_id, window_days=window_days)
        short_entries = _fetch_short_entries(conn=conn, season_id=season_id, window_days=window_days)
        short_entries_by_day = _fetch_short_entries_by_day(conn=conn, season_id=season_id, window_days=window_days)
        crash_boost_entries_by_day = _fetch_crash_boost_entries_by_day(
            conn=conn,
            season_id=season_id,
            window_days=window_days,
        )

    daily_observations = _build_daily_observations(
        metrics=metrics,
        short_entries_by_day=short_entries_by_day,
        crash_boost_entries_by_day=crash_boost_entries_by_day,
    )
    crash_boost_entries = sum(int(v or 0) for v in crash_boost_entries_by_day.values())

    summary = _summarize_window(
        metrics=metrics,
        short_entries=short_entries,
        crash_boost_entries=crash_boost_entries,
        rollback_max_dd=float(args.rollback_max_dd),
    )
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "season_id": season_id,
        "window_days": int(window_days),
        "rollback_max_dd": float(args.rollback_max_dd),
        "config_override": override,
        "run_result": run_result,
        "summary": summary,
        "metrics_window": metrics,
        "daily_observations": daily_observations,
    }

    out_path = Path(str(args.output).strip()) if str(args.output).strip() else _default_output_path(season_id)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[toredex_short_rollout_daily] wrote {out_path}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if bool(args.append_log):
        latest = summary.get("latest") if isinstance(summary.get("latest"), dict) else {}
        log_row = {
            "generated_at_utc": payload["generated_at_utc"],
            "season_id": season_id,
            "window_days": int(window_days),
            "short_entries": int(summary.get("short_entries") or 0),
            "crash_boost_entries": int(summary.get("crash_boost_entries") or 0),
            "pass_days": int(summary.get("pass_days") or 0),
            "all_pass": bool(summary.get("all_pass")),
            "min_max_drawdown_pct": summary.get("min_max_drawdown_pct"),
            "promotion_gate_pass": bool(summary.get("promotion_gate_pass")),
            "rollback_trigger": bool(summary.get("rollback_trigger")),
            "rollback_reason": str(summary.get("rollback_reason") or ""),
            "latest": {
                "as_of": latest.get("as_of"),
                "net_cum_return_pct": latest.get("net_cum_return_pct"),
                "max_drawdown_pct": latest.get("max_drawdown_pct"),
                "risk_gate_pass": latest.get("risk_gate_pass"),
                "risk_gate_reason": latest.get("risk_gate_reason"),
                "short_units": latest.get("short_units"),
                "long_units": latest.get("long_units"),
                "short_entries": short_entries_by_day.get(str(latest.get("as_of") or ""), 0),
                "crash_boost_entries": crash_boost_entries_by_day.get(str(latest.get("as_of") or ""), 0),
            },
        }
        log_path = (
            Path(str(args.daily_log_path).strip())
            if str(args.daily_log_path).strip()
            else _default_daily_log_path(season_id)
        )
        _append_jsonl(log_path, log_row)
        print(f"[toredex_short_rollout_daily] appended {log_path}")

    if bool(args.fail_on_rollback) and bool(summary.get("rollback_trigger")):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
