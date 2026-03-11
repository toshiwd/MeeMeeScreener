import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.edinetdb.repository import EdinetdbRepository
from app.backend.tdnetdb.repository import TdnetdbRepository


def test_edinet_repo_read_conn_uses_read_only():
    repo = EdinetdbRepository("C:/tmp/edinet.duckdb")

    ctx = repo._connect_read()

    assert ctx._read_only is True


def test_tdnet_repo_read_conn_uses_read_only():
    repo = TdnetdbRepository("C:/tmp/tdnet.duckdb")

    ctx = repo._connect_read()

    assert ctx._read_only is True
