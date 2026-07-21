from __future__ import annotations

from app.backend.api import watchlist_routes
from app.backend.api.routers import grid


def test_grid_filters_persisted_snapshot_to_active_watchlist(tmp_path, monkeypatch):
    code_path = tmp_path / "code.txt"
    code_path.write_text("1111\n", encoding="utf-8")
    monkeypatch.setattr(grid, "resolve_watchlist_path", lambda: str(code_path))
    monkeypatch.setattr(
        grid.screener_snapshot_service,
        "get_screener_snapshot_response",
        lambda **_: {
            "items": [
                {"code": "1111", "name": "active"},
                {"code": "2222", "name": "excluded but cached"},
            ],
            "rowCount": 2,
        },
    )

    response = grid.get_screener_rows(screener_repo=object(), stock_repo=object())

    assert response["items"] == [{"code": "1111", "name": "active"}]
    assert response["rowCount"] == 1


def test_watchlist_remove_purges_runtime_rows_by_default(tmp_path, monkeypatch):
    code_path = tmp_path / "code.txt"
    code_path.write_text("1111\n2222\n", encoding="utf-8")
    deleted: list[str] = []
    monkeypatch.setattr(watchlist_routes, "resolve_watchlist_path", lambda: str(code_path))
    monkeypatch.setattr(watchlist_routes, "delete_ticker_db_rows", lambda code: deleted.append(code) or {"daily_bars": 3})
    monkeypatch.setattr(watchlist_routes, "invalidate_screener_cache", lambda: None)
    monkeypatch.setattr(watchlist_routes, "schedule_screener_snapshot_refresh", lambda **_: None)

    response = watchlist_routes.watchlist_remove({"code": "2222", "deleteArtifacts": False})

    assert response["ok"] is True
    assert response["deleteDb"] is True
    assert response["dbDeletedTotal"] == 3
    assert deleted == ["2222"]
    assert code_path.read_text(encoding="utf-8") == "1111\n"
