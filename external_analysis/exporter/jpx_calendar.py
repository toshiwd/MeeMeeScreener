from __future__ import annotations

from datetime import date

from external_analysis.exporter.source_reader import connect_source_db, normalize_market_date


def int_to_date(value: int) -> date:
    text = str(int(value))
    return date(int(text[0:4]), int(text[4:6]), int(text[6:8]))


def load_jpx_calendar(source_db_path: str | None = None) -> list[int]:
    conn = connect_source_db(source_db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT date FROM daily_bars WHERE date IS NOT NULL ORDER BY date"
        ).fetchall()
    finally:
        conn.close()
    return [int(normalize_market_date(row[0])) for row in rows]


def build_trading_index(trading_dates: list[int]) -> dict[int, int]:
    return {int(trade_date): idx for idx, trade_date in enumerate(trading_dates)}


def offset_trading_date(trading_dates: list[int], trade_date: int, offset: int) -> int | None:
    index_by_date = build_trading_index(trading_dates)
    current_index = index_by_date.get(int(trade_date))
    if current_index is None:
        return None
    target_index = current_index + int(offset)
    if target_index < 0 or target_index >= len(trading_dates):
        return None
    return int(trading_dates[target_index])


def window_trading_dates(trading_dates: list[int], trade_date: int, lookback: int, lookforward: int) -> list[int]:
    index_by_date = build_trading_index(trading_dates)
    current_index = index_by_date.get(int(trade_date))
    if current_index is None:
        return []
    start_index = current_index - int(lookback)
    end_index = current_index + int(lookforward)
    if start_index < 0 or end_index >= len(trading_dates):
        return []
    return [int(value) for value in trading_dates[start_index : end_index + 1]]
