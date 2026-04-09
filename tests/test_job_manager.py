from __future__ import annotations

from app.backend.core import jobs


def test_job_manager_separates_lanes_and_dedupes_keys(monkeypatch) -> None:
    jobs.JobManager._instance = None
    monkeypatch.setattr(jobs.JobManager, "_start_worker", lambda self, lane=None: None)
    monkeypatch.setattr(jobs.JobManager, "_ensure_worker", lambda self, lane: None)
    monkeypatch.setattr(jobs.JobManager, "_update_db", lambda self, *args, **kwargs: None)
    monkeypatch.setattr(jobs.JobManager, "is_active", lambda self, job_type: False)

    manager = jobs.JobManager()
    manager.register_handler("demo", lambda job_id, payload: None)

    maintenance_job_id = manager.submit(
        "demo",
        payload={"source": "test"},
        lane="maintenance",
        dedupe_key="demo:maintenance",
    )
    duplicate_job_id = manager.submit(
        "demo",
        payload={"source": "test"},
        lane="maintenance",
        dedupe_key="demo:maintenance",
    )
    authoritative_job_id = manager.submit(
        "demo",
        payload={"source": "test"},
        lane="authoritative",
        dedupe_key="demo:authoritative",
    )

    assert maintenance_job_id is not None
    assert duplicate_job_id is None
    assert authoritative_job_id is not None
    assert manager._queues["maintenance"].qsize() == 1
    assert manager._queues["authoritative"].qsize() == 1

    stats = manager.get_lane_stats()
    assert stats["maintenance"]["queue_size"] == 1
    assert stats["authoritative"]["queue_size"] == 1
