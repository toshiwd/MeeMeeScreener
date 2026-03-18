import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.infra.duckdb.screener_repo import ScreenerRepository


def test_screener_repo_phase_pred_short_circuits_when_legacy_analysis_disabled(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    repo = ScreenerRepository("C:/tmp/stocks.duckdb")

    def _raise():
        raise AssertionError("_get_read_conn should not be called")

    monkeypatch.setattr(repo, "_get_read_conn", _raise)

    assert repo.fetch_phase_pred_map({"1301": 20260313, "1302": None}) == {}
