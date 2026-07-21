from scripts.tradex_sideways_direction_chart_examples_v1 import iso


def test_iso_formats_integer_trading_date() -> None:
    assert iso(20260720) == "2026-07-20"
