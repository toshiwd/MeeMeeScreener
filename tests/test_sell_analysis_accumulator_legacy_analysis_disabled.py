import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.services.analysis import sell_analysis_accumulator


def test_accumulate_sell_analysis_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise():
        raise AssertionError("get_conn should not be called")

    monkeypatch.setattr(sell_analysis_accumulator, "get_conn", _raise)

    result = sell_analysis_accumulator.accumulate_sell_analysis(lookback_days=5, anchor_dt=20260313)

    assert result == {
        "target_dates": [],
        "last_dt": None,
        "rows_last_dt": 0,
        "disabled": True,
    }


def test_accumulate_sell_analysis_for_dates_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    def _raise():
        raise AssertionError("get_conn should not be called")

    monkeypatch.setattr(sell_analysis_accumulator, "get_conn", _raise)

    result = sell_analysis_accumulator.accumulate_sell_analysis_for_dates(target_dates=[20260312, 20260313])

    assert result == {
        "target_dates": [20260312, 20260313],
        "last_dt": 20260313,
        "rows_last_dt": 0,
        "disabled": True,
    }
