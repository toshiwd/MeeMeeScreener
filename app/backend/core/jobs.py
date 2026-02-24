
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
        self._queue = queue.Queue()
        self._handlers: dict[str, Callable] = {}
        self._stop_event = threading.Event()
        self._worker_thread = None
        self._active_job_id = None
        self._cancel_lock = threading.Lock()
        self._cancel_requested_ids: set[str] = set()
        
        # Start worker
        self._start_worker()

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

    def _start_worker(self):
        if self._worker_thread and self._worker_thread.is_alive():
            return
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="JobWorker")
        self._worker_thread.start()

    def _ensure_worker(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        print("[JobManager] Worker thread was not alive. Restarting...")
        self._start_worker()

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
    ) -> str | None:
        """
        Submit a job.
        If unique=True and the job type is already active, returns None.
        """
        self._ensure_worker()
        print(f"[JobManager] submit called: type={job_type}, unique={unique}")
        print(f"[JobManager] registered handlers: {list(self._handlers.keys())}")
        
        # check basic handler existence (optional, but good for fast fail)
        if job_type not in self._handlers:
            logger.warning(f"Submitting job for unknown type {job_type}")
            print(f"[JobManager] WARNING: No handler for {job_type}")
        
        if unique and self.is_active(job_type):
            logger.warning(f"Job type {job_type} is already active. Skipping submission.")
            print(f"[JobManager] Job {job_type} already active, skipping")
            return None

        job_id = str(uuid.uuid4())
        print(f"[JobManager] Created job_id: {job_id}")
        payload = payload or {}
        # Persist initial status
        self._update_db(job_id, job_type, "queued", progress=progress, message=message)
        
        self._queue.put({
            "id": job_id,
            "type": job_type,
            "payload": payload
        })
        print(f"[JobManager] Job {job_id} queued, queue size: {self._queue.qsize()}")
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
            if status in ("success", "failed", "canceled"):
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
            return None

    def get_history(self, limit: int = 20) -> list[dict]:
        try:
            with get_conn() as conn:
                rows = conn.execute(
                    f"SELECT id, type, status, created_at, finished_at, message FROM sys_jobs ORDER BY created_at DESC LIMIT {limit}"
                ).fetchall()
                result = []
                for r in rows:
                    result.append({
                        "id": r[0],
                        "type": r[1],
                        "status": r[2],
                        "created_at": r[3],
                        "finished_at": r[4],
                        "message": r[5]
                    })
                return result
        except Exception as e:
            logger.error(f"Error fetching history: {e}")
            return []

    def _worker_loop(self):
        logger.info("JobManager Worker Started")
        print("[JobManager] Worker thread started")
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=1.0) # Check stop event every sec
                print(f"[JobManager] Worker got item: {item.get('type')} / {item.get('id')}")
                self._process_item(item)
                self._queue.task_done()
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
        self._active_job_id = job_id
        print(f"[JobManager] Processing job: {job_type} / {job_id}")

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
            self._active_job_id = None
            return
        
        handler = self._handlers.get(job_type)
        if not handler:
            print(f"[JobManager] ERROR: No handler for {job_type}")
            self._update_db(job_id, job_type, "failed", error=f"No handler for type {job_type}")
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
            self._active_job_id = None

    def _update_db(self, job_id, job_type, status, created_at=None, started_at=None, finished_at=None, progress=None, message=None, error=None):
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
