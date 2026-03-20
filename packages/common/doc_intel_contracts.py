"""
Shared data contracts between the Flask app and the doc-intel worker service.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

DocumentIntelligenceJobType = Literal["ingest", "build_package"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DocumentIntelligenceJobRequest:
    job_id: str
    job_type: DocumentIntelligenceJobType
    payload: Dict[str, Any] = field(default_factory=dict)
    priority: str = "normal"
    requested_by: Optional[str] = None
    requested_at: str = field(default_factory=_now_iso)
    version: str = "v1"

    def to_message(self) -> str:
        return json.dumps(asdict(self), default=str)

    @staticmethod
    def from_message(message_body: str) -> "DocumentIntelligenceJobRequest":
        return DocumentIntelligenceJobRequest(**json.loads(message_body))


@dataclass
class DocumentIntelligenceQueueConfig:
    queue_url: str
    region_name: str
    message_group_id: Optional[str] = None


__all__ = [
    "DocumentIntelligenceJobRequest",
    "DocumentIntelligenceJobType",
    "DocumentIntelligenceQueueConfig",
]
