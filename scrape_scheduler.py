from __future__ import annotations

"""
Background scheduler for automated website scraping.
Uses APScheduler to run daily/weekly scraping tasks.
"""

import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from bson import ObjectId
from decouple import config


class ScrapeScheduler:
    """Scheduler for automated scraping of configured websites."""

    def __init__(
        self,
        modes_collection,
        jobs_collection=None,
        *,
        scraper_client,
        scraping_service=None,
        doc_intelligence_service=None,
        execution_mode: str | None = None,
    ):
        if scraper_client is None:
            raise ValueError("scraper_client is required for ScrapeScheduler")
        self.scraper_client = scraper_client
        self.scraping_service = scraping_service
        self.doc_intelligence_service = doc_intelligence_service
        self.modes_collection = modes_collection
        self.jobs_collection = jobs_collection
        self.scheduler = BackgroundScheduler()
        self._running = False
        self.max_concurrent_jobs = max(1, int(config("SCRAPER_MAX_CONCURRENT_JOBS", default="1")))
        self.max_verification_jobs = max(1, int(config("VERIFICATION_MAX_CONCURRENT_JOBS", default="1")))
        self._job_semaphores = {
            "scrape": threading.BoundedSemaphore(self.max_concurrent_jobs),
            "verification": threading.BoundedSemaphore(self.max_verification_jobs),
        }

        if scraping_service:
            self.environment = "dev" if getattr(scraping_service, "local_dev_mode", False) else "prod"
            self.scraping_service.verification_scheduler = self
        else:
            self.environment = config("SCRAPER_ENVIRONMENT", default="prod")

        self.execution_mode = (execution_mode or ("local" if scraping_service else "remote")).lower()
    
    def start(self):
        """Start the scheduler."""
        if not self._running:
            # Schedule daily check at 2 AM
            self.scheduler.add_job(
                self._run_daily_scrapes,
                CronTrigger(hour=2, minute=0),
                id='daily_scrapes',
                name='Daily Scraping Check',
                replace_existing=True
            )
            
            # Schedule weekly check on Sundays at 3 AM
            self.scheduler.add_job(
                self._run_weekly_scrapes,
                CronTrigger(day_of_week='sun', hour=3, minute=0),
                id='weekly_scrapes',
                name='Weekly Scraping Check',
                replace_existing=True
            )
            
            # Schedule content verification every 4 hours
            self.scheduler.add_job(
                self._run_content_verification,
                CronTrigger(hour='*/4'),  # Every 4 hours
                id='content_verification',
                name='Content Verification',
                replace_existing=True
            )

            # Schedule document intelligence cleanup every 1 hour
            if self.doc_intelligence_service:
                self.scheduler.add_job(
                    self._run_doc_intel_cleanup,
                    CronTrigger(hour='*'),  # Every hour
                    id='doc_intel_cleanup',
                    name='Doc Intel Cleanup',
                    replace_existing=True
                )
            
            self.scheduler.start()
            self._running = True
            print("Scrape scheduler started (includes content verification and doc intel cleanup)")
            self._resume_incomplete_jobs()
    
    def stop(self):
        """Stop the scheduler."""
        if self._running:
            self.scheduler.shutdown()
            self._running = False
            print("Scrape scheduler stopped")
    
    def _run_doc_intel_cleanup(self):
        """Run cleanup for expired document intelligence files."""
        if not self.doc_intelligence_service:
            return
            
        try:
            print(f"Running doc intel cleanup at {datetime.utcnow()}")
            cleaned_count = self.doc_intelligence_service.cleanup_expired_documents()
            print(f"Doc intel cleanup completed: {cleaned_count} projects cleaned up.")
        except Exception as e:
            print(f"Error running doc intel cleanup: {e}")

    def _run_daily_scrapes(self):
        """Run scraping for all modes configured with daily frequency."""
        print(f"Running daily scrapes at {datetime.utcnow()}")
        
        try:
            # Find all modes with daily scraping enabled
            modes = self.modes_collection.find({
                "scrape_frequency": "daily",
                "scrape_sites": {"$exists": True, "$ne": []}
            })
            
            for mode_doc in modes:
                mode_name = mode_doc.get("name")
                user_id = mode_doc.get("user_id")
                
                if not mode_name or not user_id:
                    continue
                
                # Check if we should scrape (avoid duplicate scrapes within 20 hours)
                last_scraped = mode_doc.get("last_scraped_at")
                if last_scraped and (datetime.utcnow() - last_scraped) < timedelta(hours=20):
                    print(f"Skipping {mode_name} - scraped recently")
                    continue
                
                print(f"Queueing daily scrape for mode: {mode_name}")
                try:
                    self._enqueue_mode_scrape(mode_doc, trigger_label="daily")
                except Exception as e:
                    print(f"Error queueing scrape for mode {mode_name}: {e}")
        
        except Exception as e:
            print(f"Error in daily scrape job: {e}")
    
    def _run_weekly_scrapes(self):
        """Run scraping for all modes configured with weekly frequency."""
        print(f"Running weekly scrapes at {datetime.utcnow()}")
        
        try:
            # Find all modes with weekly scraping enabled
            modes = self.modes_collection.find({
                "scrape_frequency": "weekly",
                "scrape_sites": {"$exists": True, "$ne": []}
            })
            
            for mode_doc in modes:
                mode_name = mode_doc.get("name")
                user_id = mode_doc.get("user_id")
                
                if not mode_name or not user_id:
                    continue
                
                # Check if we should scrape (avoid duplicate scrapes within 6 days)
                last_scraped = mode_doc.get("last_scraped_at")
                if last_scraped and (datetime.utcnow() - last_scraped) < timedelta(days=6):
                    print(f"Skipping {mode_name} - scraped recently")
                    continue
                
                print(f"Queueing weekly scrape for mode: {mode_name}")
                
                try:
                    self._enqueue_mode_scrape(mode_doc, trigger_label="weekly")
                except Exception as e:
                    print(f"Error queueing weekly scrape for mode {mode_name}: {e}")
        
        except Exception as e:
            print(f"Error in weekly scrape job: {e}")
    
    def _run_content_verification(self):
        """Run content verification for scraped pages."""
        print(f"Running content verification at {datetime.utcnow()}")
        
        try:
            if self.scraper_client.is_remote or not self.scraping_service:
                job_id = self.scraper_client.queue_verification(batch_size=20)
                print(f"Queued remote verification job {job_id}")
                return

            # Get statistics first
            stats = self.scraper_client.get_verification_statistics()
            pending_count = stats.get("pending_verification", 0)
            
            if pending_count == 0:
                print("No content pending verification")
                return
            
            print(f"Found {pending_count} pages pending verification")
            
            # Verify a batch of 20 pages
            result = self.scraping_service.verify_scraped_content(batch_size=20)
            
            print(f"Verification result: {result}")
            
        except Exception as e:
            print(f"Error in content verification job: {e}")
    
    def trigger_immediate_scrape(self, mode_name: str, user_id: str):
        """
        Trigger an immediate scrape for a specific mode (bypass schedule).
        
        Args:
            mode_name: Name of the mode to scrape
            user_id: User ID (owner of the mode)
            
        Returns:
            Scraping results
        """
        print(f"Triggering immediate scrape for mode: {mode_name}")
        if self.scraper_client.is_remote:
            mode_doc = self.modes_collection.find_one({"name": mode_name, "user_id": user_id}) or {}
            if not mode_doc:
                raise ValueError("Mode not found or not owned by the user")
            mode_id = str(mode_doc.get("_id"))
            scrape_sites = mode_doc.get("scrape_sites", [])
            if not scrape_sites:
                raise ValueError("Mode has no configured scrape sites")
            # Update timestamp when a run is initiated/queued
            try:
                self.modes_collection.update_one(
                    {"_id": mode_doc.get("_id")},
                    {"$set": {"last_scraped_at": datetime.utcnow()}},
                )
            except Exception as e:
                print(f"Error updating last_scraped_at for immediate scrape: {e}")
            job_id = self.scraper_client.queue_mode_scrape(
                mode_name=mode_name,
                user_id=user_id,
                mode_id=mode_id,
                scrape_sites=scrape_sites,
                auto_dispatch=True,
            )
            return {
                "status": "queued",
                "job_id": str(job_id),
                "mode_name": mode_name,
            }
        return self.scraper_client.scrape_mode_synchronously(mode_name, user_id)
    
    def _enqueue_mode_scrape(self, mode_doc, trigger_label: str = "manual"):
        mode_name = mode_doc.get("name")
        user_id = mode_doc.get("user_id")
        mode_id = str(mode_doc.get("_id"))
        scrape_sites = mode_doc.get("scrape_sites", [])

        # Update timestamp when a scheduled/manual enqueue occurs
        try:
            if mode_doc.get("_id"):
                self.modes_collection.update_one(
                    {"_id": mode_doc.get("_id")},
                    {"$set": {"last_scraped_at": datetime.utcnow()}},
                )
            else:
                self.modes_collection.update_one(
                    {"name": mode_name, "user_id": user_id},
                    {"$set": {"last_scraped_at": datetime.utcnow()}},
                )
        except Exception as e:
            print(f"Error updating last_scraped_at for enqueue ({trigger_label}) on mode '{mode_name}': {e}")

        auto_dispatch = self.scraper_client.is_remote
        job_id = self.scraper_client.queue_mode_scrape(
            mode_name=mode_name,
            user_id=user_id,
            mode_id=mode_id,
            scrape_sites=scrape_sites,
            auto_dispatch=auto_dispatch,
        )

        if not auto_dispatch:
            self._start_local_scrape_thread(job_id, mode_name, user_id)

        print(f"[{trigger_label}] queued scrape job {job_id} for mode '{mode_name}'")
        return job_id

    def _start_local_scrape_thread(self, job_id, mode_name, user_id, resume_state=None):
        def run_with_slot():
            with self._job_slot("scrape", job_id):
                self.scraper_client.dispatch_mode_scrape(job_id, mode_name, user_id, resume_state)

        thread = threading.Thread(
            target=run_with_slot,
            daemon=True,
            name=f"ScrapeJob-{mode_name}",
        )
        thread.start()

    def _start_local_verification_thread(self, job_id, batch_size: int, filters: Optional[Dict[str, Any]] = None):
        def verification_with_slot():
            with self._job_slot("verification", job_id):
                self.scraper_client.dispatch_verification(job_id, batch_size, filters)

        thread = threading.Thread(
            target=verification_with_slot,
            daemon=True,
            name=f"VerificationJob-{job_id}",
        )
        thread.start()

    @contextmanager
    def _job_slot(self, job_type: str, job_id):
        semaphore = self._job_semaphores.get(job_type)
        if semaphore is None:
            semaphore = threading.BoundedSemaphore(1)
            self._job_semaphores[job_type] = semaphore

        semaphore.acquire()
        try:
            print(f"Job {job_id} ({job_type}): acquired execution slot")
            yield
        finally:
            semaphore.release()
            print(f"Job {job_id} ({job_type}): released execution slot")
    
    def _resume_incomplete_jobs(self):
        """Resume any in-progress scraping jobs for this environment."""
        if self.jobs_collection is None:
            return
        
        job_type_filter = {"$or": [
            {"job_type": {"$exists": False}},
            {"job_type": "scrape"}
        ]}
        if self.environment == "prod":
            env_filter = {"$or": [
                {"environment": "prod"},
                {"environment": {"$exists": False}}
            ]}
        else:
            env_filter = {"environment": self.environment}
        
        query = {
            "$and": [
                {"status": "in_progress"},
                job_type_filter,
                env_filter
            ]
        }
        
        orphaned_jobs = list(self.jobs_collection.find(query))
        if not orphaned_jobs:
            return
        
        print(f"Resuming {len(orphaned_jobs)} in-progress scraping job(s) for environment '{self.environment}'")
        
        for job in orphaned_jobs:
            job_id = job.get("_id")
            mode_name = job.get("mode_name")
            user_id = job.get("user_id")
            resume_state = job.get("checkpoint")
            
            if not job_id or not mode_name or not user_id:
                continue
            
            if self.scraper_client.is_remote:
                self.scraper_client.dispatch_mode_scrape(job_id, mode_name, user_id, resume_state=resume_state)
            else:
                self._start_local_scrape_thread(job_id, mode_name, user_id, resume_state=resume_state)
            
            self.jobs_collection.update_one(
                {"_id": job_id},
                {"$set": {
                    "resume_attempted_at": datetime.utcnow(),
                    "environment": self.environment
                }}
            )
    
    def trigger_background_scrape(self, mode_name: str, user_id: str, mode_id: str, scrape_sites: list):
        """
        Trigger a scrape that runs in the background (non-blocking).
        Creates a job record and runs scraping in a separate thread.
        
        Args:
            mode_name: Name of the mode to scrape
            user_id: User ID (owner of the mode)
            mode_id: Mode document ID
            scrape_sites: List of sites to scrape
            
        Returns:
            Job ID for tracking
        """
        if self.jobs_collection is None:
            raise RuntimeError("Jobs collection not configured for background scraping")
        
        normalized_sites = [
            site.strip() for site in (scrape_sites or []) if site and site.strip()
        ]
        if not normalized_sites:
            raise ValueError("No valid sites provided for scraping")

        # Update timestamp when a manual background scrape is initiated/queued
        try:
            self.modes_collection.update_one(
                {"_id": ObjectId(mode_id)},
                {"$set": {"last_scraped_at": datetime.utcnow()}},
            )
        except Exception as e:
            print(f"Error updating last_scraped_at for background scrape: {e}")
        
        job_id = self.scraper_client.queue_mode_scrape(
            mode_name=mode_name,
            user_id=user_id,
            mode_id=mode_id,
            scrape_sites=normalized_sites,
            auto_dispatch=self.scraper_client.is_remote,
        )
        
        if not self.scraper_client.is_remote:
            self._start_local_scrape_thread(job_id, mode_name, user_id)
        
        print(f"Started background scraping job {job_id} for mode: {mode_name}")
        return job_id
    
    def get_job_status(self, job_id):
        """
        Get the current status of a scraping job.
        
        Args:
            job_id: Job document ID (can be string or ObjectId)
            
        Returns:
            Job document or None if not found
        """
        if self.jobs_collection is None:
            return None
        
        if isinstance(job_id, str):
            job_id = ObjectId(job_id)
        
        return self.jobs_collection.find_one({"_id": job_id})
    
    def trigger_immediate_verification(self, batch_size: int = 50):
        """
        Trigger an immediate content verification run (bypass schedule).
        
        Args:
            batch_size: Number of pages to verify
            
        Returns:
            Verification results
        """
        print(f"Triggering immediate verification for {batch_size} pages")
        if self.scraper_client.is_remote or not self.scraping_service:
            raise RuntimeError("Immediate verification is only available in local mode")
        return self.scraping_service.verify_scraped_content(batch_size=batch_size)
    
    def trigger_background_verification(
        self,
        batch_size: int = 100,
        content_ids: Optional[List[str]] = None,
        mode_name: Optional[str] = None,
        base_domain: Optional[str] = None,
    ):
        """
        Trigger a verification run in the background (non-blocking).
        Creates a job record and runs verification in a separate thread.
        
        Args:
            batch_size: Number of pages to verify
            
        Returns:
            Job ID for tracking
        """
        if self.jobs_collection is None:
            raise RuntimeError("Jobs collection not configured for background verification")
        
        filters: Dict[str, Any] = {}
        if content_ids:
            filters["content_ids"] = content_ids
        if mode_name:
            filters["mode_name"] = mode_name
        if not filters:
            filters = None
        
        job_id = self.scraper_client.queue_verification(
            batch_size=batch_size,
            auto_dispatch=self.scraper_client.is_remote,
            filters=filters,
            mode_name=mode_name,
            base_domain=base_domain,
        )

        # If verification is scoped to a specific mode, update its "last scrape" timestamp immediately.
        # (Verification can re-scrape pages; this keeps the UI timestamp current even before completion.)
        if mode_name:
            try:
                update_filter: Dict[str, Any] = {"name": mode_name}
                if filters and isinstance(filters, dict) and filters.get("user_id"):
                    update_filter["user_id"] = filters["user_id"]
                self.modes_collection.update_many(
                    update_filter,
                    {"$set": {"last_scraped_at": datetime.utcnow()}},
                )
            except Exception as e:
                print(f"Error updating last_scraped_at for verification enqueue: {e}")
        
        if not self.scraper_client.is_remote:
            self._start_local_verification_thread(job_id, batch_size, filters)
        
        print(f"Started background verification job {job_id}")
        return job_id

