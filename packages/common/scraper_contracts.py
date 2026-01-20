"""
Shared data contracts between the Flask app and the scraper worker service.

These dataclasses define the shape of queue messages, job metadata, and
utility helpers so both services can evolve independently while speaking the
same language.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Literal

ScraperJobType = Literal[
    "scrape",
    "single_url_refresh",
    "delete_content",
    "verification",
    "site_delete",
    "api_target_scrape",
]


def _now_iso() -> str:
    """Return a UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ScraperJobRequest:
    """
    Represents a single unit of work for the scraper worker.

    Attributes:
        job_id: MongoDB ObjectId string referencing the stored job document.
        job_type: High-level type for routing (see ScraperJobType).
        payload: Arbitrary JSON payload with type-specific metadata.
        priority: Optional priority hint (normal, high, low).
        requested_by: Optional user identifier.
        requested_at: ISO timestamp for auditing/debugging.
        version: Contract version so we can evolve safely.
    """

    job_id: str
    job_type: ScraperJobType
    payload: Dict[str, Any] = field(default_factory=dict)
    priority: str = "normal"
    requested_by: Optional[str] = None
    requested_at: str = field(default_factory=_now_iso)
    version: str = "v1"

    def to_message(self) -> str:
        """Serialize the request to a JSON message for queue transport."""
        return json.dumps(asdict(self), default=str)

    @staticmethod
    def from_message(message_body: str) -> "ScraperJobRequest":
        """Deserialize a JSON message into a ScraperJobRequest."""
        data = json.loads(message_body)
        return ScraperJobRequest(**data)


@dataclass
class ScraperQueueConfig:
    """Configuration for connecting to the job queue."""

    queue_url: str
    region_name: str
    message_group_id: Optional[str] = None  # For FIFO queues


__all__ = [
    "ScraperJobRequest",
    "ScraperQueueConfig",
    "ScraperJobType",
]

