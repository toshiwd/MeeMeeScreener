from __future__ import annotations

from datetime import date, timedelta


AXIS_ID = "tradex_market_calendar_v1"
SUPPORTED_YEAR_MIN = 2018
SUPPORTED_YEAR_MAX = 2031


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (n - 1))


def _vernal_equinox_day(year: int) -> int:
    # Cabinet-office dates are fixed yearly, but this approximation is accurate
    # for the supported 2018-2031 freshness-gate window.
    return int(20.8431 + 0.242194 * (year - 1980) - int((year - 1980) / 4))


def _autumnal_equinox_day(year: int) -> int:
    return int(23.2488 + 0.242194 * (year - 1980) - int((year - 1980) / 4))


def _base_japan_holidays(year: int) -> set[date]:
    holidays = {
        date(year, 1, 1),
        _nth_weekday(year, 1, 0, 2),  # Coming of Age Day
        date(year, 2, 11),
        date(year, 2, 23),
        date(year, 3, _vernal_equinox_day(year)),
        date(year, 4, 29),
        date(year, 5, 3),
        date(year, 5, 4),
        date(year, 5, 5),
        _nth_weekday(year, 7, 0, 3),  # Marine Day
        date(year, 8, 11),
        _nth_weekday(year, 9, 0, 3),  # Respect for the Aged Day
        date(year, 9, _autumnal_equinox_day(year)),
        _nth_weekday(year, 10, 0, 2),  # Sports Day
        date(year, 11, 3),
        date(year, 11, 23),
    }
    if year == 2019:
        holidays.update({date(2019, 4, 30), date(2019, 5, 1), date(2019, 5, 2), date(2019, 10, 22)})
    if year == 2020:
        holidays.discard(_nth_weekday(2020, 7, 0, 3))
        holidays.discard(date(2020, 8, 11))
        holidays.discard(_nth_weekday(2020, 10, 0, 2))
        holidays.update({date(2020, 7, 23), date(2020, 7, 24), date(2020, 8, 10)})
    if year == 2021:
        holidays.discard(_nth_weekday(2021, 7, 0, 3))
        holidays.discard(date(2021, 8, 11))
        holidays.discard(_nth_weekday(2021, 10, 0, 2))
        holidays.update({date(2021, 7, 22), date(2021, 7, 23), date(2021, 8, 8)})
    return holidays


def _with_substitute_and_citizen_holidays(year: int) -> set[date]:
    holidays = set()
    for target_year in (year - 1, year, year + 1):
        if SUPPORTED_YEAR_MIN <= target_year <= SUPPORTED_YEAR_MAX:
            holidays.update(_base_japan_holidays(target_year))

    additions: set[date] = set()
    for holiday in sorted(holidays):
        if holiday.weekday() == 6:
            substitute = holiday + timedelta(days=1)
            while substitute in holidays or substitute in additions:
                substitute += timedelta(days=1)
            additions.add(substitute)
    holidays.update(additions)

    current = date(year, 1, 2)
    while current <= date(year, 12, 30):
        if current not in holidays and (current - timedelta(days=1)) in holidays and (current + timedelta(days=1)) in holidays:
            holidays.add(current)
        current += timedelta(days=1)
    return {day for day in holidays if day.year == year}


def market_calendar_metadata() -> dict[str, object]:
    return {
        "axis_id": AXIS_ID,
        "market": "JPX/TSE",
        "source": "local_japan_holiday_rules_plus_tse_year_end_new_year_closure",
        "supported_year_min": SUPPORTED_YEAR_MIN,
        "supported_year_max": SUPPORTED_YEAR_MAX,
        "limitations": [
            "does not fetch official exchange calendar online",
            "does not include future ad-hoc exchange closures outside encoded public holiday rules",
        ],
    }


def is_japan_market_holiday(value: date) -> bool:
    if value.year < SUPPORTED_YEAR_MIN or value.year > SUPPORTED_YEAR_MAX:
        raise ValueError(f"year outside supported market calendar range: {value.year}")
    if value.month == 1 and value.day in {1, 2, 3}:
        return True
    if value.month == 12 and value.day == 31:
        return True
    return value in _with_substitute_and_citizen_holidays(value.year)


def is_japan_market_business_day(value: date) -> bool:
    return value.weekday() < 5 and not is_japan_market_holiday(value)


def previous_japan_market_business_day(value: date) -> date:
    candidate = value - timedelta(days=1)
    while not is_japan_market_business_day(candidate):
        candidate -= timedelta(days=1)
    return candidate
