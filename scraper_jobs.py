"""
Reusable job execution primitives for scraping-related workloads.

The main Flask app enqueues jobs while the scraper worker executes them.
This module centralizes the logic so local (single-instance) and remote
workers behave the same way.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from bson import ObjectId


class JobCancelledError(Exception):
    """Raised when a job document no longer exists (deleted/cancelled)."""


class ScrapeJobProcessor:
    """Executes scraping-related jobs and updates MongoDB job documents."""

    def __init__(self, scraping_service, jobs_collection, environment: str = "prod"):
        self.scraping_service = scraping_service
        self.jobs_collection = jobs_collection
        self.environment = environment
        self._scraped_content = getattr(scraping_service, "scraped_content_collection", None)

    # --------------------------------------------------------------------- #
    # Mode scraping
    # --------------------------------------------------------------------- #
    def run_scrape_job(
        self,
        job_id,
        mode_name: str,
        user_id: str,
        resume_state: Optional[Dict[str, Any]] = None,
    ):
        """Execute a mode scraping job (crawl + ingest)."""
        if self.jobs_collection is None:
            raise RuntimeError("Job collection is required for scrape jobs")

        job_id = self._normalize_id(job_id)

        try:
            self._ensure_job_active(job_id)
            status_update = {
                "status": "in_progress",
                "environment": self.environment,
            }
            if resume_state:
                status_update["resumed_at"] = datetime.utcnow()
            else:
                status_update["started_at"] = datetime.utcnow()

            self.jobs_collection.update_one({"_id": job_id}, {"$set": status_update})

            def update_progress(progress_data):
                try:
                    self._ensure_job_active(job_id)
                    progress_fields = {}
                    for key in [
                        "current_site",
                        "total_pages",
                        "scraped_pages",
                        "reused_pages",
                        "failed_pages",
                    ]:
                        if key in progress_data and progress_data[key] is not None:
                            progress_fields[key] = progress_data[key]

                    if progress_data.get("phase"):
                        progress_fields["phase"] = progress_data["phase"]
                    if progress_data.get("urls_discovered") is not None:
                        progress_fields["urls_discovered"] = progress_data["urls_discovered"]

                    update_doc = {}
                    if progress_fields:
                        update_doc["progress"] = progress_fields

                    checkpoint_payload = progress_data.get("checkpoint")
                    if checkpoint_payload is not None:
                        update_doc["checkpoint"] = checkpoint_payload
                        update_doc["checkpoint_updated_at"] = datetime.utcnow()

                    if update_doc:
                        self.jobs_collection.update_one({"_id": job_id}, {"$set": update_doc})
                except JobCancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    print(f"Error updating progress for job {job_id}: {exc}")

            result = self.scraping_service.scrape_mode_sites(
                mode_name,
                user_id,
                progress_callback=update_progress,
                resume_state=resume_state,
            )

            progress_update = {
                "total_sites": result.get("total_sites", 0),
                "total_pages": result.get("total_pages_scraped", 0)
                + result.get("total_pages_reused", 0),
                "scraped_pages": result.get("total_pages_scraped", 0),
                "reused_pages": result.get("total_pages_reused", 0),
                "failed_pages": result.get("total_pages_failed", 0),
            }

            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "completed",
                        "result": result,
                        "progress": progress_update,
                        "completed_at": datetime.utcnow(),
                    },
                    "$unset": {"checkpoint": "", "checkpoint_updated_at": ""},
                },
            )

            print(f"Scrape job {job_id}: completed")

        except JobCancelledError:
            print(f"Scrape job {job_id}: cancelled (job document deleted)")
        except Exception as exc:  # noqa: BLE001
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "completed_at": datetime.utcnow(),
                    }
                },
            )
            print(f"Scrape job {job_id}: failed ({exc})")

    # --------------------------------------------------------------------- #
    # Single URL refresh
    # --------------------------------------------------------------------- #
    def run_single_url_refresh(
        self,
        job_id,
        content_id: str,
        url: str,
        mode_name: str,
        user_id: str,
    ):
        """Refresh a single scraped content document."""
        if self.jobs_collection is None or self._scraped_content is None:
            raise RuntimeError("Job and content collections are required for refresh jobs")

        job_id = self._normalize_id(job_id)
        content_oid = self._normalize_id(content_id)

        try:
            self._ensure_job_active(job_id)
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "in_progress",
                        "started_at": datetime.utcnow(),
                        "content_id": str(content_oid),
                    }
                },
            )

            content_doc = self._scraped_content.find_one({"_id": content_oid})
            if not content_doc:
                raise ValueError("Content document not found")
            self._ensure_job_active(job_id)

            content, title, error = self.scraping_service.scrape_url(url)
            if error:
                raise RuntimeError(error)

            scraped_at = datetime.utcnow()
            old_file_id = content_doc.get("openai_file_id")
            if old_file_id and self.scraping_service.vector_store_id:
                try:
                    self.scraping_service.client.files.delete(old_file_id)
                    self.scraping_service.client.vector_stores.files.delete(
                        vector_store_id=self.scraping_service.vector_store_id,
                        file_id=old_file_id,
                    )
                except Exception as exc:  # noqa: BLE001
                    print(f"Failed to delete old vector file {old_file_id}: {exc}")

            openai_file_id = self.scraping_service.upload_to_vector_store(
                content, mode_name, url, title, scraped_at
            )

            update_doc = {
                "title": title,
                "content": content,
                "scraped_at": scraped_at,
                "openai_file_id": openai_file_id,
                "status": "active",
                "error_message": None,
                "metadata": {
                    "word_count": len(content.split()),
                    "char_count": len(content),
                },
            }

            self._scraped_content.update_one({"_id": content_oid}, {"$set": update_doc})

            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "completed",
                        "result": {
                            "content_id": str(content_oid),
                            "title": title,
                            "word_count": len(content.split()),
                        },
                        "completed_at": datetime.utcnow(),
                    }
                },
            )

        except JobCancelledError:
            print(f"Refresh job {job_id}: cancelled (job document deleted)")
        except Exception as exc:  # noqa: BLE001
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "completed_at": datetime.utcnow(),
                    }
                },
            )
            print(f"Refresh job {job_id}: failed ({exc})")

    # --------------------------------------------------------------------- #
    # Content deletion
    # --------------------------------------------------------------------- #
    def run_delete_job(self, job_id, content_id: str, mode_name: Optional[str] = None):
        """Delete scraped content or unlink it from a mode."""
        if self.jobs_collection is None:
            raise RuntimeError("Job collection is required for delete jobs")

        job_id = self._normalize_id(job_id)

        try:
            self._ensure_job_active(job_id)
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "in_progress",
                        "started_at": datetime.utcnow(),
                        "content_id": content_id,
                    }
                },
            )

            self._ensure_job_active(job_id)
            success = self.scraping_service.delete_scraped_content(content_id, mode_name=mode_name)
            if not success:
                raise RuntimeError("Content not found or already deleted")

            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "completed",
                        "completed_at": datetime.utcnow(),
                    }
                },
            )
        except JobCancelledError:
            print(f"Delete job {job_id}: cancelled (job document deleted)")
        except Exception as exc:  # noqa: BLE001
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "completed_at": datetime.utcnow(),
                    }
                },
            )
            print(f"Delete job {job_id}: failed ({exc})")

    # --------------------------------------------------------------------- #
    # Content verification
    # --------------------------------------------------------------------- #
    def run_verification_job(self, job_id, batch_size: int, filters: Optional[Dict[str, Any]] = None):
        """Execute a verification job (re-scrape & compare content)."""
        if self.jobs_collection is None:
            raise RuntimeError("Job collection is required for verification jobs")

        job_id = self._normalize_id(job_id)

        try:
            self._ensure_job_active(job_id)
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "in_progress",
                        "started_at": datetime.utcnow(),
                    }
                },
            )

            def update_progress(progress_data):
                try:
                    self._ensure_job_active(job_id)
                    progress_payload = {
                        "current_page": progress_data.get("current_page", 0),
                        "total_pages": progress_data.get("total_pages", 0),
                        "verified_unchanged": progress_data.get("verified_unchanged", 0),
                        "verified_updated": progress_data.get("verified_updated", 0),
                        "failed": progress_data.get("failed", 0),
                    }
                    if progress_data.get("url"):
                        progress_payload["current_url"] = progress_data["url"]
                    if progress_data.get("base_domain"):
                        progress_payload["current_domain"] = progress_data["base_domain"]
                    if progress_data.get("modes") is not None:
                        progress_payload["current_modes"] = progress_data["modes"]

                    self.jobs_collection.update_one(
                        {"_id": job_id}, {"$set": {"progress": progress_payload}}
                    )
                except JobCancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    print(f"Error updating verification progress: {exc}")

            result = self.scraping_service.verify_scraped_content(
                batch_size=batch_size,
                progress_callback=update_progress,
                filters=filters,
            )

            progress_update = {
                "total_pages": result.get("total_checked", 0),
                "verified_unchanged": result.get("verified_unchanged", 0),
                "verified_updated": result.get("verified_updated", 0),
                "failed": result.get("failed", 0),
            }

            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "completed",
                        "result": result,
                        "progress": progress_update,
                        "completed_at": datetime.utcnow(),
                    }
                },
            )

        except JobCancelledError:
            print(f"Verification job {job_id}: cancelled (job document deleted)")
        except Exception as exc:  # noqa: BLE001
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(exc),
                        "completed_at": datetime.utcnow(),
                    }
                },
            )
            print(f"Verification job {job_id}: failed ({exc})")

    # ------------------------------------------------------------------ #
    def _normalize_id(self, value):
        """Convert incoming IDs to ObjectId."""
        if isinstance(value, ObjectId):
            return value
        return ObjectId(value)

    def _ensure_job_active(self, job_id):
        """Raise JobCancelledError if the job document has been deleted."""
        if self.jobs_collection is None:
            return
        exists = self.jobs_collection.find_one({"_id": job_id}, {"_id": 1})
        if not exists:
            raise JobCancelledError(f"Job {job_id} no longer exists")


__all__ = ["ScrapeJobProcessor"]

