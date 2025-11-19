"""
Background scheduler for automated website scraping.
Uses APScheduler to run daily/weekly scraping tasks.
"""

import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from bson import ObjectId
from decouple import config


class ScrapeScheduler:
    """Scheduler for automated scraping of configured websites."""

    def __init__(self, scraping_service, modes_collection, jobs_collection=None):
        """
        Initialize the scrape scheduler.
        
        Args:
            scraping_service: ScrapingService instance
            modes_collection: MongoDB modes collection
            jobs_collection: MongoDB scraping_jobs collection (optional, for background jobs)
        """
        self.scraping_service = scraping_service
        self.modes_collection = modes_collection
        self.jobs_collection = jobs_collection
        self.scheduler = BackgroundScheduler()
        self._running = False
        self.max_concurrent_jobs = max(1, int(config("SCRAPER_MAX_CONCURRENT_JOBS", default="1")))
        self.max_verification_jobs = max(1, int(config("VERIFICATION_MAX_CONCURRENT_JOBS", default="1")))
        self._job_semaphores = {
            "scrape": threading.BoundedSemaphore(self.max_concurrent_jobs),
            "verification": threading.BoundedSemaphore(self.max_verification_jobs)
        }
        self.environment = "dev" if getattr(self.scraping_service, "local_dev_mode", False) else "prod"
        
        # Link scheduler back to scraping service for automatic verification triggers
        self.scraping_service.verification_scheduler = self
    
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
            
            self.scheduler.start()
            self._running = True
            print("Scrape scheduler started (includes content verification every 4 hours)")
            self._resume_incomplete_jobs()
    
    def stop(self):
        """Stop the scheduler."""
        if self._running:
            self.scheduler.shutdown()
            self._running = False
            print("Scrape scheduler stopped")
    
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
                
                print(f"Running daily scrape for mode: {mode_name}")
                
                try:
                    result = self.scraping_service.scrape_mode_sites(mode_name, user_id)
                    print(f"Scrape result for {mode_name}: {result}")
                except Exception as e:
                    print(f"Error scraping mode {mode_name}: {e}")
        
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
                
                print(f"Running weekly scrape for mode: {mode_name}")
                
                try:
                    result = self.scraping_service.scrape_mode_sites(mode_name, user_id)
                    print(f"Scrape result for {mode_name}: {result}")
                except Exception as e:
                    print(f"Error scraping mode {mode_name}: {e}")
        
        except Exception as e:
            print(f"Error in weekly scrape job: {e}")
    
    def _run_content_verification(self):
        """Run content verification for scraped pages."""
        print(f"Running content verification at {datetime.utcnow()}")
        
        try:
            # Get statistics first
            stats = self.scraping_service.get_verification_statistics()
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
        return self.scraping_service.scrape_mode_sites(mode_name, user_id)
    
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
            
            def run_with_slot(job_id=job_id, mode_name=mode_name, user_id=user_id, resume_state=resume_state):
                with self._job_slot("scrape", job_id):
                    self._run_background_scrape_job(job_id, mode_name, user_id, resume_state=resume_state)
            
            thread = threading.Thread(
                target=run_with_slot,
                daemon=True,
                name=f"ScrapeJob-{mode_name}-resume"
            )
            thread.start()
            
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
        
        # Create job document
        job_doc = {
            "job_type": "scrape",
            "mode_id": mode_id,
            "mode_name": mode_name,
            "user_id": user_id,
            "status": "queued",
            "progress": {
                "total_sites": len(normalized_sites),
                "current_site": 0,
                "current_site_name": None,
                "total_pages": 0,
                "scraped_pages": 0,
                "reused_pages": 0,
                "failed_pages": 0
            },
            "checkpoint": {
                "pending_sites": normalized_sites
            },
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "environment": self.environment
        }
        
        job_result = self.jobs_collection.insert_one(job_doc)
        job_id = job_result.inserted_id
        
        # Start scraping in background thread
        def run_with_slot():
            with self._job_slot("scrape", job_id):
                self._run_background_scrape_job(job_id, mode_name, user_id)
        
        thread = threading.Thread(
            target=run_with_slot,
            daemon=True,
            name=f"ScrapeJob-{mode_name}"
        )
        thread.start()
        
        print(f"Started background scraping job {job_id} for mode: {mode_name}")
        return job_id
    
    def _run_background_scrape_job(self, job_id, mode_name: str, user_id: str, resume_state=None):
        """
        Execute a scraping job in the background.
        Updates job status throughout the process.
        
        Args:
            job_id: Job document ID
            mode_name: Name of the mode to scrape
            user_id: User ID
            resume_state: Optional checkpoint information to continue from a previous run
        """
        try:
            # Update status to in_progress
            status_update = {
                "status": "in_progress",
                "environment": self.environment
            }
            if resume_state:
                status_update["resumed_at"] = datetime.utcnow()
            else:
                status_update["started_at"] = datetime.utcnow()
            
            self.jobs_collection.update_one(
                {"_id": job_id},
                {"$set": status_update}
            )
            
            print(f"Job {job_id}: Starting scrape for mode '{mode_name}'")
            
            # Define progress callback to update job status
            def update_progress(progress_data):
                """Update the job document with current progress."""
                try:
                    progress_fields = {}
                    for key in ["current_site", "total_pages", "scraped_pages", "reused_pages", "failed_pages"]:
                        if key in progress_data and progress_data[key] is not None:
                            progress_fields[key] = progress_data[key]
                    
                    # Add phase and URL discovery info if provided
                    if progress_data.get("phase"):
                        progress_fields["phase"] = progress_data.get("phase")
                    if progress_data.get("urls_discovered") is not None:
                        progress_fields["urls_discovered"] = progress_data.get("urls_discovered")
                    
                    update_doc = {}
                    if progress_fields:
                        update_doc["progress"] = progress_fields
                    
                    checkpoint_payload = progress_data.get("checkpoint")
                    if checkpoint_payload is not None:
                        update_doc["checkpoint"] = checkpoint_payload
                        update_doc["checkpoint_updated_at"] = datetime.utcnow()
                    
                    if update_doc:
                        self.jobs_collection.update_one(
                            {"_id": job_id},
                            {"$set": update_doc}
                        )
                except Exception as e:
                    print(f"Error updating progress: {e}")
            
            # Run the actual scraping with progress callback
            print("scraping mode sites")
            result = self.scraping_service.scrape_mode_sites(
                mode_name, 
                user_id,
                progress_callback=update_progress,
                resume_state=resume_state
            )
            
            # Extract progress information from result
            progress_update = {
                "total_sites": result.get("total_sites", 0),
                "total_pages": result.get("total_pages_scraped", 0) + result.get("total_pages_reused", 0),
                "scraped_pages": result.get("total_pages_scraped", 0),
                "reused_pages": result.get("total_pages_reused", 0),
                "failed_pages": result.get("total_pages_failed", 0)
            }
            
            # Update with successful results
            self.jobs_collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "status": "completed",
                        "result": result,
                        "progress": progress_update,
                        "completed_at": datetime.utcnow()
                    },
                    "$unset": {
                        "checkpoint": "",
                        "checkpoint_updated_at": ""
                    }
                }
            )
            
            print(f"Job {job_id}: Completed successfully")
            print(f"  - Pages scraped: {result.get('total_pages_scraped', 0)}")
            print(f"  - Pages reused: {result.get('total_pages_reused', 0)}")
            print(f"  - Pages failed: {result.get('total_pages_failed', 0)}")
        
        except Exception as e:
            # Handle errors
            error_msg = str(e)
            print(f"Job {job_id}: Failed with error: {error_msg}")
            
            self.jobs_collection.update_one(
                {"_id": job_id},
                {"$set": {
                    "status": "failed",
                    "error": error_msg,
                    "completed_at": datetime.utcnow()
                }}
            )
    
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
        return self.scraping_service.verify_scraped_content(batch_size=batch_size)
    
    def trigger_background_verification(self, batch_size: int = 100):
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
        
        # Create job document
        job_doc = {
            "job_type": "verification",
            "status": "queued",
            "progress": {
                "current_page": 0,
                "total_pages": 0,
                "verified_unchanged": 0,
                "verified_updated": 0,
                "failed": 0
            },
            "result": None,
            "error": None,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None
        }
        
        job_result = self.jobs_collection.insert_one(job_doc)
        job_id = job_result.inserted_id
        
        # Start verification in background thread
        def verification_with_slot():
            with self._job_slot("verification", job_id):
                self._run_background_verification_job(job_id, batch_size)
        
        thread = threading.Thread(
            target=verification_with_slot,
            daemon=True,
            name=f"VerificationJob-{job_id}"
        )
        thread.start()
        
        print(f"Started background verification job {job_id}")
        return job_id
    
    def _run_background_verification_job(self, job_id, batch_size: int):
        """
        Execute a verification job in the background.
        Updates job status throughout the process.
        
        Args:
            job_id: Job document ID
            batch_size: Number of pages to verify
        """
        try:
            # Update status to in_progress
            self.jobs_collection.update_one(
                {"_id": job_id},
                {"$set": {
                    "status": "in_progress",
                    "started_at": datetime.utcnow()
                }}
            )
            
            print(f"Job {job_id}: Starting content verification")
            
            # Define progress callback to update job status
            def update_progress(progress_data):
                """Update the job document with current progress."""
                try:
                    progress_payload = {
                        "current_page": progress_data.get("current_page", 0),
                        "total_pages": progress_data.get("total_pages", 0),
                        "verified_unchanged": progress_data.get("verified_unchanged", 0),
                        "verified_updated": progress_data.get("verified_updated", 0),
                        "failed": progress_data.get("failed", 0)
                    }
                    
                    current_url = progress_data.get("url")
                    if current_url:
                        progress_payload["current_url"] = current_url
                    
                    base_domain = progress_data.get("base_domain")
                    if base_domain:
                        progress_payload["current_domain"] = base_domain
                    
                    doc_modes = progress_data.get("modes")
                    if doc_modes is not None:
                        progress_payload["current_modes"] = doc_modes
                    
                    primary_mode = progress_data.get("primary_mode")
                    if not primary_mode and doc_modes:
                        primary_mode = doc_modes[0]
                    if primary_mode:
                        progress_payload["current_mode"] = primary_mode
                    
                    self.jobs_collection.update_one(
                        {"_id": job_id},
                        {"$set": {"progress": progress_payload}}
                    )
                except Exception as e:
                    print(f"Error updating progress: {e}")
            
            # Run the actual verification with progress callback
            result = self.scraping_service.verify_scraped_content(
                batch_size=batch_size,
                progress_callback=update_progress
            )
            
            # Extract progress information from result
            progress_update = {
                "total_pages": result.get("total_checked", 0),
                "verified_unchanged": result.get("verified_unchanged", 0),
                "verified_updated": result.get("verified_updated", 0),
                "failed": result.get("failed", 0)
            }
            
            # Update with successful results
            self.jobs_collection.update_one(
                {"_id": job_id},
                {"$set": {
                    "status": "completed",
                    "result": result,
                    "progress": progress_update,
                    "completed_at": datetime.utcnow()
                }}
            )
            
            print(f"Job {job_id}: Verification completed successfully")
            print(f"  - Unchanged: {result.get('verified_unchanged', 0)}")
            print(f"  - Updated: {result.get('verified_updated', 0)}")
            print(f"  - Failed: {result.get('failed', 0)}")
        
        except Exception as e:
            # Handle errors
            error_msg = str(e)
            print(f"Job {job_id}: Failed with error: {error_msg}")
            
            self.jobs_collection.update_one(
                {"_id": job_id},
                {"$set": {
                    "status": "failed",
                    "error": error_msg,
                    "completed_at": datetime.utcnow()
                }}
            )

