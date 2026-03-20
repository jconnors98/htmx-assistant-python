from __future__ import annotations

import threading
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from packages.common.doc_intel_contracts import (
    DocumentIntelligenceJobRequest,
    DocumentIntelligenceQueueConfig,
)


class DocumentIntelligenceClientMode(str, Enum):
    LOCAL = "local"
    REMOTE = "remote"


class DocumentIntelligenceClient:
    """Dispatch doc-intel work locally or through SQS."""

    def __init__(
        self,
        *,
        mode: str,
        jobs_collection,
        environment: str,
        job_processor=None,
        sqs_client=None,
        queue_config: Optional[DocumentIntelligenceQueueConfig] = None,
    ) -> None:
        self.mode = DocumentIntelligenceClientMode(mode.lower())
        self.jobs_collection = jobs_collection
        self.environment = environment

        if self.mode == DocumentIntelligenceClientMode.LOCAL:
            if job_processor is None:
                raise ValueError("job_processor is required for local doc-intel mode")
            self._backend = _LocalDocIntelBackend(job_processor)
        else:
            if sqs_client is None or queue_config is None:
                raise ValueError("SQS client and queue config are required for remote doc-intel mode")
            self._backend = _SQSDocIntelBackend(sqs_client, queue_config)

    @property
    def is_remote(self) -> bool:
        return self.mode == DocumentIntelligenceClientMode.REMOTE

    def queue_ingest(
        self,
        *,
        session_id: str,
        mode_id: str,
        mode_name: str,
        uploaded_files: list[Dict[str, Any]],
        requested_by: Optional[str] = None,
        auto_dispatch: bool = True,
    ) -> str:
        job_doc = {
            "job_type": "ingest",
            "session_id": session_id,
            "mode_id": mode_id,
            "mode_name": mode_name,
            "status": "queued",
            "progress": {
                "files_total": len(uploaded_files),
                "files_completed": 0,
                "files_failed": 0,
                "phase": "queued",
            },
            "uploaded_files": uploaded_files,
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "requested_by": requested_by,
            "environment": self.environment,
        }
        job_id = str(self.jobs_collection.insert_one(job_doc).inserted_id)
        if auto_dispatch or self.is_remote:
            self.dispatch_ingest(job_id, session_id, mode_id, mode_name, uploaded_files, requested_by=requested_by)
        return job_id

    def dispatch_ingest(
        self,
        job_id: str,
        session_id: str,
        mode_id: str,
        mode_name: str,
        uploaded_files: list[Dict[str, Any]],
        *,
        requested_by: Optional[str] = None,
    ) -> None:
        self._backend.dispatch(
            DocumentIntelligenceJobRequest(
                job_id=job_id,
                job_type="ingest",
                payload={
                    "session_id": session_id,
                    "mode_id": mode_id,
                    "mode_name": mode_name,
                    "uploaded_files": uploaded_files,
                },
                requested_by=requested_by,
            )
        )

    def queue_package_build(
        self,
        *,
        session_id: str,
        mode_id: str,
        mode_name: str,
        package_id: str,
        output: str,
        plan: Dict[str, Any],
        requested_by: Optional[str] = None,
        auto_dispatch: bool = True,
    ) -> str:
        job_doc = {
            "job_type": "build_package",
            "session_id": session_id,
            "mode_id": mode_id,
            "mode_name": mode_name,
            "package_id": package_id,
            "status": "queued",
            "progress": {
                "phase": "queued",
                "items_total": 0,
                "items_completed": 0,
            },
            "output": output,
            "plan": plan,
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "requested_by": requested_by,
            "environment": self.environment,
        }
        job_id = str(self.jobs_collection.insert_one(job_doc).inserted_id)
        if auto_dispatch or self.is_remote:
            self.dispatch_package_build(
                job_id,
                session_id,
                mode_id,
                mode_name,
                package_id,
                output,
                plan,
                requested_by=requested_by,
            )
        return job_id

    def dispatch_package_build(
        self,
        job_id: str,
        session_id: str,
        mode_id: str,
        mode_name: str,
        package_id: str,
        output: str,
        plan: Dict[str, Any],
        *,
        requested_by: Optional[str] = None,
    ) -> None:
        self._backend.dispatch(
            DocumentIntelligenceJobRequest(
                job_id=job_id,
                job_type="build_package",
                payload={
                    "session_id": session_id,
                    "mode_id": mode_id,
                    "mode_name": mode_name,
                    "package_id": package_id,
                    "output": output,
                    "plan": plan,
                },
                requested_by=requested_by,
            )
        )


class _LocalDocIntelBackend:
    def __init__(self, processor) -> None:
        self.processor = processor

    def dispatch(self, request: DocumentIntelligenceJobRequest) -> None:
        worker = threading.Thread(
            target=self.processor.run_job_request,
            args=(request,),
            daemon=True,
        )
        worker.start()


class _SQSDocIntelBackend:
    def __init__(self, sqs_client, queue_config: DocumentIntelligenceQueueConfig) -> None:
        self.sqs_client = sqs_client
        self.queue_config = queue_config

    def dispatch(self, request: DocumentIntelligenceJobRequest) -> None:
        kwargs = {
            "QueueUrl": self.queue_config.queue_url,
            "MessageBody": request.to_message(),
        }
        if self.queue_config.message_group_id:
            kwargs["MessageGroupId"] = self.queue_config.message_group_id
        self.sqs_client.send_message(**kwargs)
