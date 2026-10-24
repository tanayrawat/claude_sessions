"""Contract for what we accept into silver. Everything else is rejected to the dead-letter table.

extra="ignore": the provider adding a field must never break the pipeline (it stays in bronze,
so we can backfill it later). Missing/invalid *required* fields must be loud and quarantined.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class User(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    login: str


class Label(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str


class Issue(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: int
    number: int
    title: str
    state: Literal["open", "closed"]
    user: User | None = None  # deleted accounts come back null
    labels: list[Label] = Field(default_factory=list)
    comments: int = 0
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None = None
    pull_request: dict | None = None  # the issues endpoint also returns PRs; this key is the only tell


def utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None)
