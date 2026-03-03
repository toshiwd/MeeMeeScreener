from __future__ import annotations

# Compatibility wrapper:
# `app.db.session` is the canonical DuckDB connection layer.
from app.db.session import (  # noqa: F401
    get_conn,
    get_conn_for_path,
    lifespan,
    try_get_conn,
    try_get_conn_for_path,
)

