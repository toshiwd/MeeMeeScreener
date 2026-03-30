from __future__ import annotations

from pathlib import Path
import sys

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.edinetdb.schema import ensure_edinetdb_schema_at_path
from app.backend.services.ml.edinet_rank_features import load_edinet_rank_features


def test_load_edinet_rank_features_marks_empty_tables_separately(tmp_path) -> None:
    db_path = tmp_path / "stocks.duckdb"
    ensure_edinetdb_schema_at_path(db_path)

    conn = duckdb.connect(str(db_path))
    try:
        result = load_edinet_rank_features(conn, ["1301"], 20260330)
    finally:
        conn.close()

    assert result["1301"]["edinetStatus"] == "empty_tables"
