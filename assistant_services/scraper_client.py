from __future__ import annotations

import threading
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

from packages.common.scraper_contracts import ScraperJobRequest, ScraperQueueConfig
from scraper_jobs import ScrapeJobProcessor


class ScraperClientMode(str, Enum):
    LOCAL = "local"
    REMOTE = "remote"


class ScraperClient:
    """Dispatch scraping work either locally or through an SQS-backed worker."""

    def __init__(
        self,
        *,
        mode: str,
        jobs_collection,
        scraper_environment: str,
        scraping_service=None,
        sqs_client=None,
        queue_config: Optional[ScraperQueueConfig] = None,
    ):
        self.mode = ScraperClientMode(mode.lower())
        self.jobs_collection = jobs_collection
        self.environment = scraper_environment

        if self.mode == ScraperClientMode.LOCAL:
            if scraping_service is None:
                raise ValueError("scraping_service is required for local mode")
            processor = ScrapeJobProcessor(scraping_service, jobs_collection, environment=scraper_environment)
            self._backend = _LocalScraperBackend(processor)
        else:
            if sqs_client is None or queue_config is None:
                raise ValueError("SQS client and queue config are required for remote mode")
            self._backend = _SQSScraperBackend(sqs_client, queue_config)

    # ------------------------------------------------------------------ #
    # Job creation helpers
    # ------------------------------------------------------------------ #
    def queue_mode_scrape(
        self,
        *,
        mode_name: str,
        user_id: str,
        mode_id: str,
        scrape_sites: List[str],
        resume_state: Optional[Dict[str, Any]] = None,
        auto_dispatch: bool = True,
    ):
        """Create or resume a scraping job for a mode."""
        normalized_sites = [site.strip() for site in (scrape_sites or []) if site and site.strip()]
        if not normalized_sites:
            raise ValueError("No valid sites provided for scraping")

        job_doc = {
            "job_type": "scrape",
            "mode_id": str(mode_id),
            "mode_name": mode_name,
            "user_id": user_id,
            "status": "queued",
            "progress": {
                "total_sites": len(normalized_sites),
                "current_site": 0,
                "total_pages": 0,
                "scraped_pages": 0,
                "reused_pages": 0,
                "failed_pages": 0,
            },
            "checkpoint": {
                "pending_sites": normalized_sites,
            },
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment,
        }

        if resume_state:
            job_doc["checkpoint"] = resume_state

        job_id = self.jobs_collection.insert_one(job_doc).inserted_id
        if auto_dispatch or self.is_remote:
            self.dispatch_mode_scrape(job_id, mode_name, user_id, resume_state, mode_id=mode_id)
        return job_id

    def dispatch_mode_scrape(self, job_id, mode_name, user_id, resume_state=None, *, mode_id: Optional[str] = None):
        """Send a scrape job to the configured backend."""
        self._backend.dispatch_mode_scrape(str(job_id), mode_name, user_id, resume_state, mode_id=mode_id)

    def resume_mode_scrape(self, job_doc: Dict[str, Any]):
        """Re-dispatch an in-progress job."""
        self.dispatch_mode_scrape(
            job_doc["_id"],
            job_doc.get("mode_name"),
            job_doc.get("user_id"),
            job_doc.get("checkpoint"),
            mode_id=job_doc.get("mode_id"),
        )

    def queue_single_url_refresh(
        self,
        *,
        content_id: str,
        url: str,
        mode_name: str,
        user_id: str,
        auto_dispatch: bool = True,
    ):
        job_doc = {
            "job_type": "single_url_refresh",
            "status": "queued",
            "content_id": content_id,
            "mode_name": mode_name,
            "user_id": user_id,
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment,
        }
        job_id = self.jobs_collection.insert_one(job_doc).inserted_id
        if auto_dispatch or self.is_remote:
            self.dispatch_single_url_refresh(job_id, content_id, url, mode_name, user_id)
        return job_id

    def dispatch_single_url_refresh(self, job_id, content_id, url, mode_name, user_id):
        self._backend.dispatch_single_url_refresh(str(job_id), content_id, url, mode_name, user_id)

    def queue_delete_content(
        self,
        *,
        content_id: str,
        user_id: str,
        mode_name: Optional[str] = None,
        auto_dispatch: bool = True,
    ):
        job_doc = {
            "job_type": "delete_content",
            "status": "queued",
            "content_id": content_id,
            "mode_name": mode_name,
            "user_id": user_id,
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment,
        }
        job_id = self.jobs_collection.insert_one(job_doc).inserted_id
        if auto_dispatch or self.is_remote:
            self.dispatch_delete_content(job_id, content_id, mode_name)
        return job_id

    def dispatch_delete_content(self, job_id, content_id, mode_name):
        self._backend.dispatch_delete_content(str(job_id), content_id, mode_name)

    def queue_verification(
        self,
        *,
        batch_size: int = 500,
        auto_dispatch: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        mode_name: Optional[str] = None,
        base_domain: Optional[str] = None,
    ):
        job_doc = {
            "job_type": "verification",
            "status": "queued",
            "progress": {
                "current_page": 0,
                "total_pages": 0,
                "verified_unchanged": 0,
                "verified_updated": 0,
                "failed": 0,
            },
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment,
            "filters": filters or None,
            "mode": mode_name,
            "base_domain": base_domain,
        }
        job_id = self.jobs_collection.insert_one(job_doc).inserted_id
        if auto_dispatch or self.is_remote:
            self.dispatch_verification(job_id, batch_size, filters)
        return job_id

    def dispatch_verification(self, job_id, batch_size: int, filters: Optional[Dict[str, Any]] = None):
        self._backend.dispatch_verification(str(job_id), batch_size, filters)

    def queue_site_delete(
        self,
        *,
        mode_id: str,
        mode_name: str,
        domain: str,
        user_id: str,
        auto_dispatch: bool = True,
    ):
        job_doc = {
            "job_type": "site_delete",
            "status": "queued",
            "mode_id": str(mode_id),
            "mode_name": mode_name,
            "domain": domain,
            "user_id": user_id,
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment,
        }
        job_id = self.jobs_collection.insert_one(job_doc).inserted_id
        if auto_dispatch or self.is_remote:
            self.dispatch_site_delete(job_id, mode_name, domain)
        return job_id

    def dispatch_site_delete(self, job_id, mode_name, domain):
        self._backend.dispatch_site_delete(str(job_id), mode_name, domain)

    def queue_api_target_scrape(
        self,
        *,
        url: str,
        target: Dict[str, Any],
        user_id: str,
        options: Optional[Dict[str, Any]] = None,
        timeout_ms: int = 30000,
        auto_dispatch: bool = True,
    ):
        """Create an API target scraping job and optionally dispatch it."""
        url = (url or "").strip()
        if not url:
            raise ValueError("url is required")
        if not isinstance(target, dict) or not (target.get("type") or "").strip():
            raise ValueError("target.type is required")
        selectors = target.get("selectors")
        if selectors is None or not isinstance(selectors, dict):
            raise ValueError("target.selectors must be an object")
        if options is not None and not isinstance(options, dict):
            raise ValueError("options must be an object")

        job_doc = {
            "job_type": "api_target_scrape",
            "status": "queued",
            "user_id": user_id,
            "url": url,
            "options": options or None,
            "target": target,
            "timeout_ms": int(timeout_ms or 30000),
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment,
        }
        job_id = self.jobs_collection.insert_one(job_doc).inserted_id
        if auto_dispatch or self.is_remote:
            self.dispatch_api_target_scrape(job_id, url, target, user_id=user_id, options=options, timeout_ms=timeout_ms)
        return job_id

    def dispatch_api_target_scrape(
        self,
        job_id,
        url: str,
        target: Dict[str, Any],
        *,
        user_id: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        timeout_ms: int = 30000,
    ):
        """Send an api_target_scrape job to the configured backend."""
        self._backend.dispatch_api_target_scrape(
            str(job_id),
            url,
            target,
            user_id=user_id,
            options=options,
            timeout_ms=int(timeout_ms or 30000),
        )

    # ------------------------------------------------------------------ #
    def get_verification_statistics(self) -> Dict[str, Any]:
        """Expose verification stats for schedulers."""
        if self.mode != ScraperClientMode.LOCAL:
            return {}
        return self._backend.scraping_service.get_verification_statistics()

    def scrape_mode_synchronously(self, mode_name: str, user_id: str, *, mode_id: Optional[str] = None):
        """Execute a mode scrape synchronously (local only)."""
        if self.mode != ScraperClientMode.LOCAL:
            raise RuntimeError("Synchronous scraping is only available in local mode")
        return self._backend.scraping_service.scrape_mode_sites(mode_name, user_id, mode_id=mode_id)

    @property
    def is_remote(self) -> bool:
        return self.mode == ScraperClientMode.REMOTE


# ---------------------------------------------------------------------- #
# Local backend
# ---------------------------------------------------------------------- #
class _LocalScraperBackend:
    def __init__(self, job_processor: ScrapeJobProcessor):
        self.job_processor = job_processor
        self.scraping_service = job_processor.scraping_service

    def dispatch_mode_scrape(self, job_id: str, mode_name: str, user_id: str, resume_state=None, *, mode_id: Optional[str] = None):
        thread = threading.Thread(
            target=self.job_processor.run_scrape_job,
            args=(job_id, mode_name, user_id, resume_state),
            kwargs={"mode_id": mode_id},
            daemon=True,
            name=f"ScrapeJob-{mode_name}",
        )
        thread.start()

    def dispatch_single_url_refresh(self, job_id, content_id, url, mode_name, user_id):
        thread = threading.Thread(
            target=self.job_processor.run_single_url_refresh,
            args=(job_id, content_id, url, mode_name, user_id),
            daemon=True,
            name=f"RefreshJob-{content_id}",
        )
        thread.start()

    def dispatch_delete_content(self, job_id, content_id, mode_name):
        thread = threading.Thread(
            target=self.job_processor.run_delete_job,
            args=(job_id, content_id, mode_name),
            daemon=True,
            name=f"DeleteJob-{content_id}",
        )
        thread.start()

    def dispatch_verification(self, job_id, batch_size: int, filters: Optional[Dict[str, Any]] = None):
        thread = threading.Thread(
            target=self.job_processor.run_verification_job,
            args=(job_id, batch_size, filters),
            daemon=True,
            name=f"VerificationJob-{job_id}",
        )
        thread.start()

    def dispatch_site_delete(self, job_id, mode_name, domain):
        thread = threading.Thread(
            target=self.job_processor.run_site_delete_job,
            args=(job_id, mode_name, domain),
            daemon=True,
            name=f"SiteDelete-{domain}",
        )
        thread.start()

    def dispatch_api_target_scrape(
        self,
        job_id: str,
        url: str,
        target: Dict[str, Any],
        *,
        user_id: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        timeout_ms: int = 30000,
    ):
        thread = threading.Thread(
            target=self.job_processor.run_api_target_scrape,
            args=(job_id, url, options, target),
            kwargs={"user_id": user_id, "timeout_ms": int(timeout_ms or 30000)},
            daemon=True,
            name=f"ApiTargetScrape-{job_id}",
        )
        thread.start()


# ---------------------------------------------------------------------- #
# Remote backend
# ---------------------------------------------------------------------- #
class _SQSScraperBackend:
    def __init__(self, sqs_client, queue_config: ScraperQueueConfig):
        self._sqs = sqs_client
        self._config = queue_config

    def dispatch_mode_scrape(self, job_id, mode_name, user_id, resume_state=None, *, mode_id: Optional[str] = None):
        payload = {
            "mode_name": mode_name,
            "user_id": user_id,
            "mode_id": mode_id,
            "resume_state": resume_state,
        }
        self._send_request(job_id, "scrape", payload)

    def dispatch_single_url_refresh(self, job_id, content_id, url, mode_name, user_id):
        payload = {
            "content_id": content_id,
            "url": url,
            "mode_name": mode_name,
            "user_id": user_id,
        }
        self._send_request(job_id, "single_url_refresh", payload)

    def dispatch_delete_content(self, job_id, content_id, mode_name):
        payload = {
            "content_id": content_id,
            "mode_name": mode_name,
        }
        self._send_request(job_id, "delete_content", payload)

    def dispatch_verification(self, job_id, batch_size: int, filters: Optional[Dict[str, Any]] = None):
        payload = {"batch_size": batch_size, "filters": filters}
        self._send_request(job_id, "verification", payload)

    def dispatch_site_delete(self, job_id, mode_name, domain):
        payload = {"mode_name": mode_name, "domain": domain}
        self._send_request(job_id, "site_delete", payload)

    def dispatch_api_target_scrape(
        self,
        job_id: str,
        url: str,
        target: Dict[str, Any],
        *,
        user_id: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
        timeout_ms: int = 30000,
    ):
        payload = {
            "url": url,
            "options": options or None,
            "target": target,
            "user_id": user_id,
            "timeout_ms": int(timeout_ms or 30000),
        }
        self._send_request(job_id, "api_target_scrape", payload)

    def _send_request(self, job_id: str, job_type: str, payload: Dict[str, Any]):
        request = ScraperJobRequest(
            job_id=str(job_id),
            job_type=job_type,
            payload=payload,
        )
        params = {
            "QueueUrl": self._config.queue_url,
            "MessageBody": request.to_message(),
        }
        if self._config.message_group_id:
            params["MessageGroupId"] = self._config.message_group_id
            params["MessageDeduplicationId"] = str(uuid4())

        self._sqs.send_message(**params)

