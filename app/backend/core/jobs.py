
import threading
import queue
import uuid
import json
import time
import traceback
import logging
from datetime import datetime
from typing import Callable, Any

try:
    from app.db.session import get_conn
    from app.core.config import config
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from db import get_conn  # type: ignore
    from core.config import config  # type: ignore

logger = logging.getLogger(__name__)
STALE_JOB_HOURS = 2
PROCESS_BOOT_AT = datetime.now()
_JOB_LANES = ("authoritative", "maintenance")

def cleanup_stale_jobs() -> None:
    try:
        with get_conn() as conn:
            conn.execute(
                f"""
                UPDATE sys_jobs
                SET status = 'failed',
                    finished_at = CURRENT_TIMESTAMP,
                    error = 'stale_job',
                    message = 'Stale job cleanup'
                WHERE status = 'queued'
                  AND (created_at < CURRENT_TIMESTAMP - INTERVAL '{STALE_JOB_HOURS} hours'
                       OR created_at < ?)
                """,
                [PROCESS_BOOT_AT]
            )
            conn.execute(
                """
                UPDATE sys_jobs
                SET status = 'failed',
                    finished_at = CURRENT_TIMESTAMP,
                    error = 'stale_job_from_previous_process',
                    message = 'Stale running job from previous process'
                WHERE status IN ('running', 'cancel_requested')
                  AND COALESCE(started_at, created_at) < ?
                """,
                [PROCESS_BOOT_AT]
            )
            print("[JobManager] Stale jobs cleaned up.")
    except Exception as e:
        logger.error(f"Failed to cleanup stale jobs: {e}")

class JobManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(JobManager, cls).__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        self._queues = {lane: queue.Queue() for lane in _JOB_LANES}
        self._handlers: dict[str, Callable] = {}
        self._stop_event = threading.Event()
        self._worker_threads: dict[str, threading.Thread | None] = {lane: None for lane in _JOB_LANES}
        self._active_job_ids: dict[str, str | None] = {lane: None for lane in _JOB_LANES}
        self._cancel_lock = threading.Lock()
        self._cancel_requested_ids: set[str] = set()
        self._status_cache_lock = threading.Lock()
        self._status_cache: dict[str, dict[str, Any]] = {}
        self._dedupe_lock = threading.Lock()
        self._active_dedupe_keys: set[str] = set()
        
        # Start worker
        self._start_worker()

    def _to_sort_ts(self, value: Any) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return 0.0
            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
            except Exception:
                return 0.0
        return 0.0

    def _update_status_cache(
        self,
        *,
        job_id: str,
        job_type: str | None,
        status: str | None = None,
        created_at: Any = None,
        started_at: Any = None,
        finished_at: Any = None,
        progress: Any = None,
        message: Any = None,
        error: Any = None,
        lane: str | None = None,
        dedupe_key: str | None = None,
    ) -> None:
        if not job_id:
            return
        with self._status_cache_lock:
            current = dict(self._status_cache.get(job_id, {}))
            current["id"] = job_id
            if job_type is not None:
                current["type"] = job_type
            if status is not None:
                current["status"] = status
            if created_at is not None:
                current["created_at"] = created_at
            elif status == "queued" and "created_at" not in current:
                current["created_at"] = datetime.now()
            if started_at is not None:
                current["started_at"] = started_at
            if finished_at is not None:
                current["finished_at"] = finished_at
            if progress is not None:
                current["progress"] = progress
            if message is not None:
                current["message"] = message
            if error is not None:
                current["error"] = error
            if lane is not None:
                current["lane"] = lane
            if dedupe_key is not None:
                current["dedupe_key"] = dedupe_key
            self._status_cache[job_id] = current
            if len(self._status_cache) > 5000:
                oldest = min(
                    self._status_cache.keys(),
                    key=lambda key: self._to_sort_ts(
                        self._status_cache.get(key, {}).get("created_at")
                    ),
                )
                self._status_cache.pop(oldest, None)

    def get_cached_status(self, job_id: str) -> dict | None:
        with self._status_cache_lock:
            row = self._status_cache.get(job_id)
            return dict(row) if row else None

    def get_cached_current(self) -> dict | None:
        with self._status_cache_lock:
            active = [
                dict(value)
                for value in self._status_cache.values()
                if value.get("status") in ("queued", "running", "cancel_requested")
            ]
        if not active:
            return None
        active.sort(
            key=lambda row: (
                self._to_sort_ts(row.get("started_at")),
                self._to_sort_ts(row.get("created_at")),
            ),
            reverse=True,
        )
        return active[0]

    def get_cached_history(self, limit: int = 20) -> list[dict]:
        resolved_limit = max(1, int(limit or 20))
        with self._status_cache_lock:
            rows = [dict(value) for value in self._status_cache.values()]
        rows.sort(key=lambda row: self._to_sort_ts(row.get("created_at")), reverse=True)
        return rows[:resolved_limit]

    def update_status_cache_only(
        self,
        *,
        job_id: str,
        job_type: str | None,
        status: str | None = None,
        created_at: Any = None,
        started_at: Any = None,
        finished_at: Any = None,
        progress: Any = None,
        message: Any = None,
        error: Any = None,
        lane: str | None = None,
        dedupe_key: str | None = None,
    ) -> None:
        self._update_status_cache(
            job_id=job_id,
            job_type=job_type,
            status=status,
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
            progress=progress,
            message=message,
            error=error,
            lane=lane,
            dedupe_key=dedupe_key,
        )

    def _mark_cancel_requested(self, job_id: str) -> None:
        with self._cancel_lock:
            self._cancel_requested_ids.add(job_id)

    def _clear_cancel_requested(self, job_id: str) -> None:
        with self._cancel_lock:
            self._cancel_requested_ids.discard(job_id)

    def _is_cancel_requested(self, job_id: str) -> bool:
        with self._cancel_lock:
            return job_id in self._cancel_requested_ids

    def is_cancel_requested(self, job_id: str) -> bool:
        if self._is_cancel_requested(job_id):
            return True
        status = self.get_status(job_id)
        if not status:
            return False
        return status.get("status") in ("cancel_requested", "canceled")

    def _start_worker(self, lane: str | None = None):
        lanes = (lane,) if lane else _JOB_LANES
        for resolved_lane in lanes:
            worker = self._worker_threads.get(resolved_lane)
            if worker and worker.is_alive():
                continue
            worker = threading.Thread(
                target=self._worker_loop,
                args=(resolved_lane,),
                daemon=True,
                name=f"JobWorker-{resolved_lane}",
            )
            self._worker_threads[resolved_lane] = worker
            worker.start()

    def _ensure_worker(self, lane: str) -> None:
        worker = self._worker_threads.get(lane)
        if worker and worker.is_alive():
            return
        print(f"[JobManager] Worker thread for lane={lane} was not alive. Restarting...")
        self._start_worker(lane)

    def _normalize_lane(self, lane: str | None) -> str:
        text = str(lane or "authoritative").strip().lower()
        return text if text in _JOB_LANES else "authoritative"

    def _reserve_dedupe_key(self, dedupe_key: str | None) -> bool:
        if not dedupe_key:
            return True
        with self._dedupe_lock:
            if dedupe_key in self._active_dedupe_keys:
                return False
            self._active_dedupe_keys.add(dedupe_key)
            return True

    def _release_dedupe_key(self, dedupe_key: str | None) -> None:
        if not dedupe_key:
            return
        with self._dedupe_lock:
            self._active_dedupe_keys.discard(dedupe_key)

    def register_handler(self, job_type: str, handler: Callable[[str, dict], None]):
        """
        Register a handler for a job type.
        Handler signature: (job_id: str, payload: dict) -> None
        """
        self._handlers[job_type] = handler
        print(f"[JobManager] Registered handler for: {job_type}")

    def is_active(self, job_type: str) -> bool:
        """Check if a job of the given type is currently queued or running."""
        with get_conn() as conn:
            cleanup_stale_jobs()
            count = conn.execute(
                "SELECT COUNT(*) FROM sys_jobs WHERE type = ? AND status IN ('queued', 'running', 'cancel_requested')",
                [job_type]
            ).fetchone()[0]
            return count > 0

    def submit(
        self,
        job_type: str,
        payload: dict | None = None,
        unique: bool = False,
        *,
        message: str = "Waiting in queue...",
        progress: int | None = 0,
        lane: str = "authoritative",
        dedupe_key: str | None = None,
    ) -> str | None:
        """
        Submit a job.
        If unique=True and the job type is already active, returns None.
        """
        resolved_lane = self._normalize_lane(lane)
        self._ensure_worker(resolved_lane)
        print(f"[JobManager] submit called: type={job_type}, unique={unique}, lane={resolved_lane}")
        print(f"[JobManager] registered handlers: {list(self._handlers.keys())}")
        
        # check basic handler existence (optional, but good for fast fail)
        if job_type not in self._handlers:
            logger.warning(f"Submitting job for unknown type {job_type}")
            print(f"[JobManager] WARNING: No handler for {job_type}")
        
        if unique and self.is_active(job_type):
            logger.warning(f"Job type {job_type} is already active. Skipping submission.")
            print(f"[JobManager] Job {job_type} already active, skipping")
            return None
        if not self._reserve_dedupe_key(dedupe_key):
            logger.info("Job dedupe skipped type=%s lane=%s dedupe_key=%s", job_type, resolved_lane, dedupe_key)
            return None

        job_id = str(uuid.uuid4())
        print(f"[JobManager] Created job_id: {job_id}")
        payload = payload or {}
        self._update_status_cache(
            job_id=job_id,
            job_type=job_type,
            status="queued",
            created_at=datetime.now(),
            progress=progress or 0,
            message=message,
            error=None,
            lane=resolved_lane,
            dedupe_key=dedupe_key,
        )
        # Persist initial status
        self._update_db(job_id, job_type, "queued", progress=progress, message=message)
        
        self._queues[resolved_lane].put({
            "id": job_id,
            "type": job_type,
            "payload": payload,
            "lane": resolved_lane,
            "dedupe_key": dedupe_key,
        })
        print(
            f"[JobManager] Job {job_id} queued lane={resolved_lane}, "
            f"queue size: {self._queues[resolved_lane].qsize()}"
        )
        return job_id

    def cancel(self, job_id: str) -> bool:
        try:
            with get_conn() as conn:
                row = conn.execute(
                    "SELECT type, status FROM sys_jobs WHERE id = ?",
                    [job_id],
                ).fetchone()
            if not row:
                return False

            job_type = row[0]
            status = row[1]
            if status in ("success", "failed", "canceled", "skipped"):
                return False

            self._mark_cancel_requested(job_id)
            if status == "queued":
                self._update_db(
                    job_id,
                    job_type,
                    "canceled",
                    finished_at=datetime.now(),
                    message="Canceled before start",
                    error="canceled",
                )
                return True

            self._update_db(
                job_id,
                job_type,
                "cancel_requested",
                message="Cancellation requested",
            )
            return True
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

    def get_status(self, job_id: str) -> dict | None:
        try:
            with get_conn() as conn:
                row = conn.execute(
                    "SELECT id, type, status, created_at, started_at, finished_at, progress, message, error FROM sys_jobs WHERE id = ?",
                    [job_id]
                ).fetchone()
                if not row:
                    return None
                self._update_status_cache(
                    job_id=str(row[0]),
                    job_type=str(row[1]) if row[1] is not None else None,
                    status=str(row[2]) if row[2] is not None else None,
                    created_at=row[3],
                    started_at=row[4],
                    finished_at=row[5],
                    progress=row[6],
                    message=row[7],
                    error=row[8],
                )
                # col mapping depends on select order
                return {
                    "id": row[0],
                    "type": row[1],
                    "status": row[2],
                    "created_at": row[3],
                    "started_at": row[4],
                    "finished_at": row[5],
                    "progress": row[6],
                    "message": row[7],
                    "error": row[8]
                }
        except Exception as e:
            logger.error(f"Error fetching status for {job_id}: {e}")
            cached = self.get_cached_status(job_id)
            return cached if cached is not None else None

    def get_history(self, limit: int = 20) -> list[dict]:
        try:
            with get_conn() as conn:
                rows = conn.execute(
                    f"SELECT id, type, status, created_at, finished_at, message FROM sys_jobs ORDER BY created_at DESC LIMIT {limit}"
                ).fetchall()
                result = []
                for r in rows:
                    row_payload = {
                        "id": r[0],
                        "type": r[1],
                        "status": r[2],
                        "created_at": r[3],
                        "finished_at": r[4],
                        "message": r[5]
                    }
                    result.append(row_payload)
                    self._update_status_cache(
                        job_id=str(r[0]),
                        job_type=str(r[1]) if r[1] is not None else None,
                        status=str(r[2]) if r[2] is not None else None,
                        created_at=r[3],
                        finished_at=r[4],
                        message=r[5],
                    )
                return result
        except Exception as e:
            logger.error(f"Error fetching history: {e}")
            return self.get_cached_history(limit=limit)

    def get_lane_stats(self) -> dict[str, dict[str, Any]]:
        stats: dict[str, dict[str, Any]] = {}
        now = datetime.now().timestamp()
        with self._status_cache_lock:
            rows = [dict(value) for value in self._status_cache.values()]
        for lane in _JOB_LANES:
            oldest_age_sec: float | None = None
            queued = [
                row
                for row in rows
                if row.get("lane", "authoritative") == lane and row.get("status") == "queued"
            ]
            for row in queued:
                created_ts = self._to_sort_ts(row.get("created_at"))
                if created_ts <= 0:
                    continue
                age = max(0.0, now - created_ts)
                if oldest_age_sec is None or age > oldest_age_sec:
                    oldest_age_sec = age
            stats[lane] = {
                "queue_size": self._queues[lane].qsize(),
                "active_job_id": self._active_job_ids.get(lane),
                "oldest_queued_age_sec": oldest_age_sec,
            }
        return stats

    def _worker_loop(self, lane: str):
        logger.info("JobManager Worker Started lane=%s", lane)
        print(f"[JobManager] Worker thread started lane={lane}")
        while not self._stop_event.is_set():
            try:
                item = self._queues[lane].get(timeout=1.0) # Check stop event every sec
                print(f"[JobManager] Worker got item lane={lane}: {item.get('type')} / {item.get('id')}")
                self._process_item(item)
                self._queues[lane].task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker Loop Error: {e}")
                print(f"[JobManager] Worker loop error: {e}")
                traceback.print_exc()
                time.sleep(1)

    def _process_item(self, item: dict):
        job_id = item["id"]
        job_type = item["type"]
        payload = item["payload"]
        lane = self._normalize_lane(item.get("lane"))
        dedupe_key = str(item.get("dedupe_key") or "").strip() or None
        self._active_job_ids[lane] = job_id
        print(f"[JobManager] Processing job lane={lane}: {job_type} / {job_id}")

        status = self.get_status(job_id)
        if self._is_cancel_requested(job_id) or (status and status["status"] == "canceled"):
            self._update_db(
                job_id,
                job_type,
                "canceled",
                finished_at=datetime.now(),
                message="Canceled before start",
                error="canceled",
            )
            self._clear_cancel_requested(job_id)
            self._active_job_ids[lane] = None
            self._release_dedupe_key(dedupe_key)
            return
        
        handler = self._handlers.get(job_type)
        if not handler:
            print(f"[JobManager] ERROR: No handler for {job_type}")
            self._update_db(job_id, job_type, "failed", error=f"No handler for type {job_type}")
            self._active_job_ids[lane] = None
            self._release_dedupe_key(dedupe_key)
            return

        try:
            # Running
            print(f"[JobManager] Starting handler for {job_type}")
            self._update_db(job_id, job_type, "running", started_at=datetime.now(), message="Processing...")
            
            # Execute
            handler(job_id, payload)
            print(f"[JobManager] Handler completed for {job_type}")
            
            # Success (Handler should generally not raise if success, or it manages partials)
            # We assume if handler returns, it's done. 
            # Handler can update progress/message during exec via callback wrapper?
            # For now, we final update.
            # Check if handler marked it failed?
            # We'll just mark success if status is still running.
            status = self.get_status(job_id)
            if self._is_cancel_requested(job_id) or (status and status["status"] == "cancel_requested"):
                self._update_db(
                    job_id,
                    job_type,
                    "canceled",
                    finished_at=datetime.now(),
                    message="Canceled",
                    error="canceled",
                )
            elif status and status["status"] == "running":
                self._update_db(job_id, job_type, "success", finished_at=datetime.now(), progress=100, message="Completed")
                
        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            print(f"[JobManager] Job {job_id} failed with exception: {e}")
            traceback.print_exc()
            if self._is_cancel_requested(job_id):
                self._update_db(
                    job_id,
                    job_type,
                    "canceled",
                    finished_at=datetime.now(),
                    message="Canceled",
                    error="canceled",
                )
            else:
                self._update_db(job_id, job_type, "failed", finished_at=datetime.now(), error=str(e), message="Internal Error")
        finally:
            self._clear_cancel_requested(job_id)
            self._active_job_ids[lane] = None
            self._release_dedupe_key(dedupe_key)

    def _update_db(self, job_id, job_type, status, created_at=None, started_at=None, finished_at=None, progress=None, message=None, error=None):
        self._update_status_cache(
            job_id=str(job_id),
            job_type=str(job_type) if job_type is not None else None,
            status=str(status) if status is not None else None,
            created_at=created_at,
            started_at=started_at,
            finished_at=finished_at,
            progress=progress,
            message=message,
            error=error,
        )
        try:
             with get_conn() as conn:
                if status == "running" and self._is_cancel_requested(job_id):
                    status = "cancel_requested"
                # Upsert logic or just update?
                # "queued" is insert. others update.
                # simpler: INSERT OR REPLACE? Or separate logic.
                if status == "queued":
                    conn.execute(
                        """
                        INSERT INTO sys_jobs (id, type, status, created_at, progress, message)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
                        """,
                        [job_id, job_type, status, progress or 0, message]
                    )
                else:
                    # distinct updates
                    fields = ["status = ?"]
                    params = [status]
                    
                    if started_at:
                        fields.append("started_at = ?")
                        params.append(started_at)
                    if finished_at:
                        fields.append("finished_at = ?")
                        params.append(finished_at)
                    if progress is not None:
                        fields.append("progress = ?")
                        params.append(progress)
                    if message:
                        fields.append("message = ?")
                        params.append(message)
                    if error:
                        fields.append("error = ?")
                        params.append(error)
                        
                    params.append(job_id)
                    
                    sql = f"UPDATE sys_jobs SET {', '.join(fields)} WHERE id = ?"
                    conn.execute(sql, params)
        except Exception as e:
            logger.error(f"DB Update failed for job {job_id}: {e}")
            print(f"[JobManager] DB Update failed: {e}")

# Global Access
job_manager = JobManager()
