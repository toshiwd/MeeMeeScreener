import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.infra.duckdb.stock_repo import StockRepository


def test_stock_repo_legacy_analysis_reads_short_circuit(monkeypatch):
    monkeypatch.setenv("MEEMEE_DISABLE_LEGACY_ANALYSIS", "1")

    repo = StockRepository("C:/tmp/stocks.duckdb")

    def _raise():
        raise AssertionError("_get_read_conn should not be called")

    monkeypatch.setattr(repo, "_get_read_conn", _raise)

    assert repo.get_phase_pred("1301", None) is None
    assert repo.get_ml_analysis_pred("1301", None) is None
    assert repo.get_analysis_timeline("1301", None) == []
    assert repo.get_buy_stage_precision("1301", None) is None
    assert repo.get_sell_analysis_snapshot("1301", None) is None
    assert repo.get_latest_ml_pred_map(["1301"]) == {}
