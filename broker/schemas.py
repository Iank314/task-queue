from pydantic import BaseModel
from typing import Any, Optional
from uuid import UUID


class SubmitJobRequest(BaseModel):
    type: str
    payload: dict[str, Any]
    priority: int = 0


class JobResponse(BaseModel):
    id: UUID
    type: str
    status: str
    payload: dict[str, Any]
    priority: int
    attempts: int
    max_attempts: int
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None


class JobCreatedResponse(BaseModel):
    id: UUID
