import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.infra.duckdb.stock_repo import StockRepository


def test_stock_repo_read_conn_uses_read_only():
    repo = StockRepository("C:/tmp/stocks.duckdb")

    ctx = repo._get_read_conn()

    assert ctx._read_only is True


def test_stock_repo_write_conn_uses_rw():
    repo = StockRepository("C:/tmp/stocks.duckdb")

    ctx = repo._get_write_conn()

    assert ctx._read_only is False
