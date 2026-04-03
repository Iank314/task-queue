import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="pending")
    priority = Column(Integer, nullable=False, default=0)
    attempts = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)
    result = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    run_after = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class DeadLetterJob(Base):
    __tablename__ = "dead_letter_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_job_id = Column(UUID(as_uuid=True), nullable=False)
    type = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=False)
    priority = Column(Integer, nullable=False, default=0)
    attempts = Column(Integer, nullable=False)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    failed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
