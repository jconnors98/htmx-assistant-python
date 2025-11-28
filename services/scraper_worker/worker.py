from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
from decouple import config
from openai import OpenAI
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Ensure the repo root is importable so we can reuse shared modules
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from packages.common.scraper_contracts import ScraperJobRequest, ScraperQueueConfig  # noqa: E402
from scraper_jobs import ScrapeJobProcessor  # noqa: E402
from assistant_services.scraper_client import ScraperClient  # noqa: E402
from scraping_service import ScrapingService  # noqa: E402


log = logging.getLogger("scraper-worker")
logging.basicConfig(
    level=getattr(logging, config("LOG_LEVEL", default="INFO").upper()),
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class _WorkerVerificationScheduler:
    """Minimal scheduler shim so scraping_service can queue verification jobs."""

    def __init__(self, scraper_client: ScraperClient):
        self.scraper_client = scraper_client

    def trigger_background_verification(
        self,
        batch_size: int = 100,
        content_ids: Optional[List[str]] = None,
        mode_name: Optional[str] = None,
        base_domain: Optional[str] = None,
    ):
        filters: Dict[str, Any] = {}
        if content_ids:
            filters["content_ids"] = content_ids
        if mode_name:
            filters["mode_name"] = mode_name
        if not filters:
            filters = None

        job_id = self.scraper_client.queue_verification(
            batch_size=batch_size,
            auto_dispatch=True,
            filters=filters,
            mode_name=mode_name,
            base_domain=base_domain,
        )
        log.info(
            "Queued background verification job %s (batch=%s, content_ids=%s, mode=%s, base_domain=%s)",
            job_id,
            batch_size,
            len(content_ids or []),
            mode_name,
            base_domain,
        )
        return job_id


def _build_mongo_client():
    uri = config("MONGO_URI")
    return MongoClient(uri, server_api=ServerApi("1"))


def _build_sqs_client(region_name: str):
    return boto3.client(
        "sqs",
        region_name=region_name,
        aws_access_key_id=config("AWS_ACCESS_KEY_ID", default=None),
        aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY", default=None),
    )


def _dispatch_request(request: ScraperJobRequest, processor: ScrapeJobProcessor):
    payload: Dict[str, Any] = request.payload or {}
    log.info("Processing job %s (%s)", request.job_id, request.job_type)

    if request.job_type == "scrape":
        processor.run_scrape_job(
            request.job_id,
            payload.get("mode_name"),
            payload.get("user_id"),
            resume_state=payload.get("resume_state"),
        )
    elif request.job_type == "single_url_refresh":
        processor.run_single_url_refresh(
            request.job_id,
            payload["content_id"],
            payload["url"],
            payload.get("mode_name"),
            payload.get("user_id"),
        )
    elif request.job_type == "delete_content":
        processor.run_delete_job(
            request.job_id,
            payload["content_id"],
            mode_name=payload.get("mode_name"),
        )
    elif request.job_type == "verification":
        processor.run_verification_job(
            request.job_id,
            batch_size=int(payload.get("batch_size", 50)),
            filters=payload.get("filters"),
        )
    elif request.job_type == "site_delete":
        processor.run_site_delete_job(
            request.job_id,
            mode_name=payload["mode_name"],
            domain=payload["domain"],
        )
    else:
        raise ValueError(f"Unsupported job type: {request.job_type}")


def main():
    local_dev_mode = config("LOCAL_DEV_MODE", default="false").lower()
    # if local_dev_mode != "true":
    #     os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/bitnami/playwright-browsers")
    #     os.environ.setdefault("XDG_CACHE_HOME", "/opt/bitnami/playwright-cache")

    queue_url = config("SCRAPER_SQS_QUEUE_URL")
    region = config("SCRAPER_SQS_REGION", default=config("COGNITO_REGION", default="us-east-1"))
    message_group_id = config("SCRAPER_SQS_MESSAGE_GROUP_ID", default=None) or None
    wait_seconds = int(config("SCRAPER_SQS_WAIT_TIME", default="10"))
    max_messages = int(config("SCRAPER_SQS_MAX_MESSAGES", default="5"))
    visibility_timeout = int(config("SCRAPER_SQS_VISIBILITY_TIMEOUT", default="300"))
    idle_sleep = int(config("SCRAPER_IDLE_SLEEP_SECONDS", default="5"))
    scraper_environment = config("SCRAPER_ENVIRONMENT", default="prod")

    mongo_client = _build_mongo_client()
    db = mongo_client.get_database(config("MONGO_DB", default="bcca-assistant"))
    jobs_collection = db.get_collection("scraping_jobs")

    openai_client = OpenAI(api_key=config("OPENAI_API_KEY"))
    sqs = _build_sqs_client(region)
    queue_config = ScraperQueueConfig(
        queue_url=queue_url,
        region_name=region,
        message_group_id=message_group_id,
    )
    scraper_client = ScraperClient(
        mode="remote",
        jobs_collection=jobs_collection,
        scraper_environment=scraper_environment,
        sqs_client=sqs,
        queue_config=queue_config,
    )

    scraping_service = ScrapingService(
        client=openai_client,
        mongo_db=db,
        vector_store_id=config("OPENAI_VECTOR_STORE_ID", default=None),
    )
    scraping_service.verification_scheduler = _WorkerVerificationScheduler(scraper_client)
    processor = ScrapeJobProcessor(
        scraping_service=scraping_service,
        jobs_collection=jobs_collection,
        environment=scraper_environment,
    )

    log.info("Scraper worker started. Queue=%s Region=%s", queue_url, region)

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                WaitTimeSeconds=wait_seconds,
                MaxNumberOfMessages=max_messages,
                VisibilityTimeout=visibility_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed to receive messages: %s", exc)
            time.sleep(idle_sleep)
            continue

        messages = response.get("Messages", [])
        if not messages:
            time.sleep(idle_sleep)
            continue

        for msg in messages:
            receipt = msg["ReceiptHandle"]
            try:
                request = ScraperJobRequest.from_message(msg["Body"])
                _dispatch_request(request, processor)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception as exc:  # noqa: BLE001
                log.exception(
                    "Job failed (request_id=%s). Leaving message for retry. Body=%s",
                    msg.get("MessageId"),
                    json.dumps(msg["Body"]) if isinstance(msg["Body"], dict) else msg["Body"],
                )
                # Message will become visible again after the visibility timeout


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Worker interrupted by user. Shutting down.")

