from __future__ import annotations

from app.backend.core import market_reference_refresh_job


def test_submit_market_reference_refresh_queues_taisyaku_and_tdnet(monkeypatch) -> None:
    submissions: list[tuple[str, dict, bool]] = []

    class _DummyJobManager:
        def submit(self, job_type, payload, unique, message):
            submissions.append((job_type, payload, unique))
            return f"job-{job_type}"

    monkeypatch.setattr(market_reference_refresh_job, "job_manager", _DummyJobManager())

    result = market_reference_refresh_job.submit_market_reference_refresh(source="test")

    assert result == {
        "taisyaku_job_id": "job-taisyaku_import",
        "tdnet_job_id": "job-tdnet_import",
    }
    assert submissions == [
        ("taisyaku_import", {"source": "test"}, True),
        ("tdnet_import", {"source": "test", "limit": 500}, True),
    ]
