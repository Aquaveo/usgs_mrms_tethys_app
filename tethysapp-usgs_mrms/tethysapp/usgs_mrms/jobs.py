"""Background job manager for flood-alert (EWS) runs.

Runs one pipeline at a time in a worker thread. Dedup and ownership are enforced
in the database and a replica-wide heartbeat covers every job this replica owns,
so any number of portal replicas can run a manager safely. Mirrors
fimserve_viewer's job manager.
"""

import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Callable, Optional, Tuple

from .job_store import JobStore
from .model import JobStatus

# runner(job_dict, progress_callback) -> result_key
JobRunner = Callable[[dict, Callable[[str], None]], str]

HEARTBEAT_SECONDS = 30
STALE_TIMEOUT_SECONDS = 180


def worker_identity() -> str:
    """Identity of this replica and process, recorded on owned jobs."""
    return f"{socket.gethostname()}:{os.getpid()}"


class JobManager:
    """Submits, executes, and tracks background flood-alert jobs."""

    def __init__(self, store: Optional[JobStore] = None, max_workers: int = 1):
        self.store = store or JobStore()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="floodalert")
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def heartbeat_loop(self) -> None:
        """Refresh heartbeats for this replica's active jobs, forever."""
        ticker = threading.Event()
        while not ticker.wait(HEARTBEAT_SECONDS):
            with suppress(Exception):
                self.store.touch_owned(worker_identity())

    def submit(self, state: str, run_id: str, key: str, params: dict, runner: JobRunner) -> Tuple[dict, bool]:
        """Queue a job, or return the already-active job with the same key.

        Returns ``(job, created)`` where ``created`` is False when an active
        duplicate was found.
        """
        self.store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
        job, created = self.store.create_or_get_active(
            state=state, run_id=run_id, key=key, params=params, owner=worker_identity()
        )
        if created:
            self.executor.submit(self.execute, job["id"], runner)
        return job, created

    def execute(self, job_id: str, runner: JobRunner) -> None:
        """Run one owned job to completion, recording progress and outcome."""
        if not self.store.claim(job_id, worker_identity()):
            return
        job = self.store.get(job_id)
        if job is None:
            return

        def progress(message: str = "") -> None:
            self.store.update_progress(job_id, message)

        try:
            result_key = runner(job, progress)
            self.store.finish(job_id, JobStatus.SUCCESS, "Flood alert generated successfully.", result_key)
        except Exception as exc:
            self.store.finish(job_id, JobStatus.ERROR, str(exc))

    def get(self, job_id: str) -> Optional[dict]:
        return self.store.get(job_id)

    def active_for_state(self, state: str) -> Optional[dict]:
        self.store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
        return self.store.find_active_for_state(state)


manager_instance: Optional[JobManager] = None
manager_lock = threading.Lock()


def get_job_manager() -> JobManager:
    """Return the process-wide :class:`JobManager`, creating it on first use."""
    global manager_instance
    with manager_lock:
        if manager_instance is None:
            manager_instance = JobManager()
        return manager_instance
