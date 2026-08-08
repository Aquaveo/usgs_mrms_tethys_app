"""Database model for background flood-alert (EWS) jobs.

Job dedup, ownership, and status live in the app ``jobs_db`` persistent store
(Postgres in the portal), so every replica coordinates through the same rows.
Mirrors the fimserve_viewer job model.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Column, DateTime, Index, String, Text, text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class JobStatus:
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"
    INTERRUPTED = "interrupted"

    ACTIVE = (QUEUED, RUNNING)


def utcnow() -> datetime:
    """Current timezone-aware UTC time."""
    return datetime.now(timezone.utc)


def new_job_id() -> str:
    """Short unique identifier for a job."""
    return uuid.uuid4().hex[:12]


class Job(Base):
    """One flood-alert run request and its lifecycle state."""

    __tablename__ = "flood_alert_jobs"

    id = Column(String(12), primary_key=True, default=new_job_id)
    key = Column(String(255), nullable=False)      # dedup key: "<STATE>:<run_id>"
    state = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    params = Column(JSON, nullable=False, default=dict)   # {start, end, workers}
    status = Column(String(16), nullable=False, default=JobStatus.QUEUED)
    message = Column(Text, nullable=False, default="")
    result_key = Column(Text, nullable=False, default="")  # default_storage prefix for the run
    claimed_by = Column(String(255), nullable=False, default="")
    heartbeat_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        # At most one ACTIVE job per key, enforced by the DB across all replicas.
        Index(
            "uq_flood_alert_jobs_active_key",
            "key",
            unique=True,
            postgresql_where=text("status IN ('queued', 'running')"),
            sqlite_where=text("status IN ('queued', 'running')"),
        ),
    )

    def is_active(self) -> bool:
        return self.status in JobStatus.ACTIVE

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "key": self.key,
            "state": self.state,
            "run_id": self.run_id,
            "params": self.params,
            "status": self.status,
            "message": self.message,
            "result_key": self.result_key,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def init_jobs_db(engine, first_time):
    """Create the job tables in the app persistent store."""
    Base.metadata.create_all(engine)
