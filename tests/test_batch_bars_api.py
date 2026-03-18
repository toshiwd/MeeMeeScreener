from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import app.backend.api.routers.bars as bars_module
from app.backend.api.dependencies import get_stock_repo


class _FakeRepo:
    def get_daily_bars_batch(self, codes, limit, asof_dt=None):
        rows = [
            (20260310, 100.0, 110.0, 95.0, 105.0, 1000.0),
            (20260311, 106.0, 112.0, 101.0, 111.0, 1200.0),
            (20260312, 111.0, 115.0, 109.0, 114.0, 900.0),
        ]
        return {code: rows[-limit:] for code in codes}

    def get_monthly_bars_batch(self, codes, limit, asof_dt=None):
        rows = [
            (202601, 90.0, 101.0, 88.0, 100.0, 10000.0),
            (202602, 100.0, 108.0, 96.0, 107.0, 11000.0),
            (202603, 107.0, 109.0, 103.0, 108.0, 5000.0),
        ]
        return {code: rows[-limit:] for code in codes}


def _build_client() -> TestClient:
    app = FastAPI()
    app.include_router(bars_module.router)
    app.dependency_overrides[get_stock_repo] = lambda: _FakeRepo()
    return TestClient(app)


def test_batch_bars_v3_skips_monthly_box_detection_when_include_boxes_is_false(monkeypatch) -> None:
    calls: list[int] = []

    def _fail_if_called(rows, range_basis="body", max_range_pct=0.2):
        calls.append(len(rows))
        return [{"startTime": 1, "endTime": 2}]

    monkeypatch.setattr(bars_module, "detect_boxes", _fail_if_called)
    client = _build_client()

    response = client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["monthly"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": False,
        },
    )

    assert response.status_code == 200
    payload = response.json()["items"]["7203"]["monthly"]
    assert payload["bars"]
    assert payload["boxes"] == []
    assert calls == []


def test_batch_bars_v3_returns_monthly_boxes_when_include_boxes_is_true(monkeypatch) -> None:
    calls: list[int] = []

    def _fake_detect_boxes(rows, range_basis="body", max_range_pct=0.2):
        calls.append(len(rows))
        return [{"startTime": 1, "endTime": 2}]

    monkeypatch.setattr(bars_module, "detect_boxes", _fake_detect_boxes)
    client = _build_client()

    response = client.post(
        "/api/batch_bars_v3",
        json={
            "codes": ["7203"],
            "timeframes": ["monthly"],
            "limit": 24,
            "includeProvisional": True,
            "includeBoxes": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()["items"]["7203"]["monthly"]
    assert payload["bars"]
    assert payload["boxes"] == [{"startTime": 1, "endTime": 2}]
    assert calls == [3]
