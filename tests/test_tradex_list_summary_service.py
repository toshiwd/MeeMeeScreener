from __future__ import annotations

from typing import Any

from app.backend.services import tradex_list_summary_service as service


class _FakeRepo:
    pass


def setup_function(_function) -> None:
    service.reset_tradex_list_summary_cache()


def test_build_tradex_list_summary_snapshot_maps_detail_output_and_caches(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_build_detail(*, code: str, asof_dt: int | None, repo: Any, enabled: bool | None = None):
        calls["count"] += 1
        return {
            "available": True,
            "reason": None,
            "analysis": {
                "symbol": code,
                "asof": "2026-03-19",
                "side_ratios": {"buy": 0.72, "neutral": 0.18, "sell": 0.10},
                "confidence": 0.84,
                "reasons": ["tone=up", "pattern=breakout", "ignored"],
                "publish_readiness": {
                    "ready": True,
                    "status": "ready",
                    "reasons": ["validation_pass"],
                    "candidate_key": "candidate:7203",
                    "approved": True,
                },
                "override_state": {"present": False},
            },
        }

    monkeypatch.setattr(service, "build_tradex_detail_analysis_snapshot", fake_build_detail)

    first = service.build_tradex_list_summary_snapshot(
        items=[{"code": "7203", "asof": "2026-03-19"}],
        repo=_FakeRepo(),
        enabled=True,
        scope="visible",
    )
    second = service.build_tradex_list_summary_snapshot(
        items=[{"code": "7203", "asof": "2026-03-19"}],
        repo=_FakeRepo(),
        enabled=True,
        scope="visible",
    )

    assert first["available"] is True
    assert first["reason"] is None
    assert first["scope"] == "visible"
    assert first["items"][0]["code"] == "7203"
    assert first["items"][0]["dominant_tone"] == "buy"
    assert first["items"][0]["confidence"] == 0.84
    assert first["items"][0]["reasons"] == ["tone=up", "pattern=breakout"]
    assert first["items"][0]["publish_readiness"]["ready"] is True
    assert second["items"][0]["code"] == "7203"
    assert calls["count"] == 1


def test_build_tradex_list_summary_snapshot_caches_unavailable_analysis(monkeypatch) -> None:
    calls = {"count": 0}

    def fake_build_detail(*, code: str, asof_dt: int | None, repo: Any, enabled: bool | None = None):
        calls["count"] += 1
        return {"available": False, "reason": "analysis unavailable", "analysis": None}

    monkeypatch.setattr(service, "build_tradex_detail_analysis_snapshot", fake_build_detail)

    first = service.build_tradex_list_summary_snapshot(
        items=[{"code": "7203", "asof": None}],
        repo=_FakeRepo(),
        enabled=True,
        scope="favorites",
    )
    second = service.build_tradex_list_summary_snapshot(
        items=[{"code": "7203", "asof": None}],
        repo=_FakeRepo(),
        enabled=True,
        scope="favorites",
    )

    assert first["available"] is False
    assert first["reason"] == "analysis unavailable"
    assert first["items"][0]["available"] is False
    assert first["items"][0]["reason"] == "analysis unavailable"
    assert second["items"][0]["reason"] == "analysis unavailable"
    assert calls["count"] == 1


def test_build_tradex_list_summary_snapshot_degrades_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "build_tradex_detail_analysis_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not run")),
    )

    result = service.build_tradex_list_summary_snapshot(
        items=[{"code": "7203", "asof": None}],
        repo=_FakeRepo(),
        enabled=False,
        scope="grid-visible",
    )

    assert result == {"available": False, "reason": "feature flag disabled", "scope": "grid-visible", "items": []}

