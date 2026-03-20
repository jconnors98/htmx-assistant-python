from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from bson import ObjectId

from packages.common.doc_intel_contracts import DocumentIntelligenceJobRequest


class DocumentIntelligenceJobProcessor:
    """Execute queued document-intelligence jobs and update job documents."""

    def __init__(self, *, service, jobs_collection, environment: str = "prod") -> None:
        self.service = service
        self.jobs_collection = jobs_collection
        self.environment = environment

    def run_job_request(self, request: DocumentIntelligenceJobRequest) -> None:
        if request.job_type == "ingest":
            self.run_ingest_job(request.job_id, request.payload)
            return
        if request.job_type == "build_package":
            self.run_package_job(request.job_id, request.payload)
            return
        raise ValueError(f"Unsupported doc-intel job type: {request.job_type}")

    def run_ingest_job(self, job_id: str, payload: Dict[str, Any]) -> None:
        job_oid = self._normalize_id(job_id)
        try:
            self.jobs_collection.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "status": "in_progress",
                        "started_at": datetime.utcnow(),
                        "environment": self.environment,
                        "progress.phase": "processing",
                    }
                },
            )

            def update_progress(progress: Dict[str, Any]) -> None:
                update_doc = {"progress": progress}
                if progress.get("phase"):
                    update_doc["updated_at"] = datetime.utcnow()
                self.jobs_collection.update_one({"_id": job_oid}, {"$set": update_doc})

            result = self.service.process_ingest_job(payload, progress_callback=update_progress)
            self.jobs_collection.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "status": "completed",
                        "result": result,
                        "completed_at": datetime.utcnow(),
                        "progress": {
                            "phase": "completed",
                            "files_total": result.get("files_total", 0),
                            "files_completed": result.get("ingested", 0),
                            "files_failed": len(result.get("errors", [])),
                        },
                    }
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.jobs_collection.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "completed_at": datetime.utcnow(),
                        "progress.phase": "failed",
                    }
                },
            )
            self.service.mark_project_failed(payload.get("session_id"), str(exc))

    def run_package_job(self, job_id: str, payload: Dict[str, Any]) -> None:
        job_oid = self._normalize_id(job_id)
        try:
            self.jobs_collection.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "status": "in_progress",
                        "started_at": datetime.utcnow(),
                        "environment": self.environment,
                        "progress.phase": "building",
                    }
                },
            )
            result = self.service.process_package_job(payload)
            self.jobs_collection.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "status": "completed",
                        "result": result,
                        "completed_at": datetime.utcnow(),
                        "progress.phase": "completed",
                    }
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.jobs_collection.update_one(
                {"_id": job_oid},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "completed_at": datetime.utcnow(),
                        "progress.phase": "failed",
                    }
                },
            )
            self.service.mark_package_failed(
                payload.get("session_id"),
                payload.get("package_id"),
                str(exc),
                job_id=str(job_oid),
            )

    @staticmethod
    def _normalize_id(value):
        return value if isinstance(value, ObjectId) else ObjectId(value)
