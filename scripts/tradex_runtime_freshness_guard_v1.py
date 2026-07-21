from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from scripts.tradex_market_calendar_v1 import market_calendar_metadata, previous_japan_market_business_day


AXIS_ID = "runtime_freshness_guard_v1"
DEFAULT_MAX_STALE_CALENDAR_DAYS = 4


def _date_from_any(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() and len(text) == 8:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text[:10])


def _latest_source_dates(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    rows = con.execute(
        """
SELECT
  coalesce(source, 'pan') AS source,
  max(to_timestamp(date)::date) AS max_date,
  count(*) AS rows
FROM daily_bars
GROUP BY 1
ORDER BY 1
"""
    ).fetchall()
    return [
        {
            "source": str(source),
            "max_date": max_date.isoformat() if max_date else None,
            "rows": int(rows_count or 0),
        }
        for source, max_date, rows_count in rows
    ]


def _ranking_appearance_freshness(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'ranking_appearance_daily'"
    ).fetchone()[0]
    if not table_exists:
        return []
    rows = con.execute(
        """
SELECT
  ranking_logic_version,
  dir,
  max(dt) AS max_dt,
  count(*) AS rows
FROM ranking_appearance_daily
GROUP BY 1, 2
ORDER BY 1, 2
"""
    ).fetchall()
    return [
        {
            "ranking_logic_version": str(logic),
            "dir": str(direction),
            "max_date": _date_from_any(max_dt).isoformat() if _date_from_any(max_dt) else None,
            "rows": int(rows_count or 0),
        }
        for logic, direction, max_dt, rows_count in rows
    ]


def build_runtime_freshness_guard(
    *,
    db_path: Path,
    max_stale_calendar_days: int = DEFAULT_MAX_STALE_CALENDAR_DAYS,
    min_confirmed_date: str | None = None,
    require_expected_latest: bool = True,
    today: date | None = None,
) -> dict[str, Any]:
    today = today or date.today()
    min_date = _date_from_any(min_confirmed_date) if min_confirmed_date else None
    expected_latest = previous_japan_market_business_day(today) if require_expected_latest else None
    effective_min_date = max([d for d in [min_date, expected_latest] if d is not None], default=None)
    with duckdb.connect(str(db_path), read_only=True) as con:
        daily_sources = _latest_source_dates(con)
        ranking_appearance = _ranking_appearance_freshness(con)

    confirmed = next((row for row in daily_sources if row.get("source") == "pan"), None)
    confirmed_date = _date_from_any(confirmed.get("max_date")) if confirmed else None
    stale_days = (today - confirmed_date).days if confirmed_date else None
    stale_by_age = stale_days is None or stale_days > int(max_stale_calendar_days)
    stale_by_min_date = bool(effective_min_date and confirmed_date and confirmed_date < effective_min_date)
    missing_min_date = bool(effective_min_date and confirmed_date is None)
    pass_gate = not stale_by_age and not stale_by_min_date and not missing_min_date

    return {
        "axis_id": AXIS_ID,
        "db_path": str(db_path),
        "today": today.isoformat(),
        "confirmed_source": "pan",
        "confirmed_max_date": confirmed_date.isoformat() if confirmed_date else None,
        "confirmed_stale_calendar_days": stale_days,
        "max_stale_calendar_days": int(max_stale_calendar_days),
        "min_confirmed_date": min_date.isoformat() if min_date else None,
        "require_expected_latest": bool(require_expected_latest),
        "expected_latest_confirmed_date": expected_latest.isoformat() if expected_latest else None,
        "expected_latest_calendar": market_calendar_metadata() if require_expected_latest else None,
        "effective_min_confirmed_date": effective_min_date.isoformat() if effective_min_date else None,
        "daily_sources": daily_sources,
        "ranking_appearance_freshness": ranking_appearance,
        "pass": pass_gate,
        "decision": "pass" if pass_gate else "fail_stale_runtime_daily_bars",
        "failure_reasons": [
            reason
            for reason, active in [
                ("confirmed_daily_bars_missing", confirmed_date is None),
                ("confirmed_daily_bars_too_old", stale_by_age),
                ("confirmed_daily_bars_before_effective_min_confirmed_date", stale_by_min_date or missing_min_date),
            ]
            if active
        ],
        "update_instruction": {
            "owner": "MeeMee",
            "reason": "TRADEX selection must not treat stale confirmed bars as latest",
            "api_endpoint": "POST /api/jobs/txt-update",
            "legacy_api_endpoint": "POST /api/txt_update/run",
        },
    }


def assert_runtime_freshness(
    *,
    db_path: Path,
    max_stale_calendar_days: int = DEFAULT_MAX_STALE_CALENDAR_DAYS,
    min_confirmed_date: str | None = None,
    require_expected_latest: bool = True,
) -> dict[str, Any]:
    guard = build_runtime_freshness_guard(
        db_path=db_path,
        max_stale_calendar_days=max_stale_calendar_days,
        min_confirmed_date=min_confirmed_date,
        require_expected_latest=require_expected_latest,
    )
    if not guard["pass"]:
        raise RuntimeError(
            "runtime freshness gate failed: "
            f"confirmed_max_date={guard['confirmed_max_date']}, "
            f"stale_days={guard['confirmed_stale_calendar_days']}, "
            f"max_stale_days={guard['max_stale_calendar_days']}, "
            f"min_confirmed_date={guard['min_confirmed_date']}, "
            f"expected_latest_confirmed_date={guard['expected_latest_confirmed_date']}, "
            f"reasons={','.join(guard['failure_reasons'])}; "
            "run MeeMee txt update before TRADEX selection"
        )
    return guard
