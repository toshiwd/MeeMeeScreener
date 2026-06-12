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


def test_short_lifecycle_overlay_is_opt_in(monkeypatch) -> None:
    monkeypatch.delenv("MEEMEE_ENABLE_TRADEX_LIST_SUMMARY", raising=False)
    monkeypatch.delenv("MEEMEE_ENABLE_TRADEX_SHORT_LIFECYCLE_OVERLAY", raising=False)

    assert service.is_tradex_list_summary_enabled() is False


def test_build_tradex_list_summary_snapshot_adds_read_only_short_lifecycle_without_detail(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_load_short_lifecycle_by_code",
        lambda: (
            {
                "5016": {
                    "code": "5016",
                    "lifecycle_rank": 1,
                    "lifecycle_state": "Probe",
                    "signal_ymd": 20260526,
                    "expected_downside_pct": 0.1378,
                    "risk_reward_to_sl8": 1.51,
                    "lifecycle_reasons": ["setup_ready_confirmed_continuation"],
                }
            },
            {"available": True, "created_at": "2026-06-05T02:48:15+00:00", "artifact_path": "G:/Tradex/board.json"},
        ),
    )

    result = service.build_tradex_list_summary_snapshot(
        items=[{"code": "5016", "asof": "2026-06-05"}],
        repo=_FakeRepo(),
        enabled=True,
        detail_enabled=False,
        scope="ranking-visible",
    )

    item = result["items"][0]
    assert result["available"] is True
    assert item["available"] is True
    assert item["dominant_tone"] is None
    assert item["short_lifecycle"]["state"] == "Probe"
    assert item["short_lifecycle"]["expected_downside_pct"] == 0.1378
    assert item["short_lifecycle"]["review_only"] is True

