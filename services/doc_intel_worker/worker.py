from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path

import boto3
from decouple import config
from openai import OpenAI
from pymongo import MongoClient
from pymongo.server_api import ServerApi

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from document_intelligence_jobs import DocumentIntelligenceJobProcessor  # noqa: E402
from document_intelligence_service import DocumentIntelligenceService  # noqa: E402
from document_intelligence_storage import DocumentIntelligenceStorage  # noqa: E402
from packages.common.doc_intel_contracts import DocumentIntelligenceJobRequest  # noqa: E402
from tools import DocumentToolbox  # noqa: E402
from tools.mongo_audit import AuditedDatabase  # noqa: E402


log = logging.getLogger("doc-intel-worker")
logging.basicConfig(
    level=getattr(logging, config("LOG_LEVEL", default="INFO").upper()),
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _build_mongo_client():
    return MongoClient(config("MONGO_URI"), server_api=ServerApi("1"))


def _build_sqs_client(region_name: str):
    return boto3.client(
        "sqs",
        region_name=region_name,
        aws_access_key_id=config("AWS_ACCESS_KEY_ID", default=None),
        aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY", default=None),
    )


def _build_s3_client(region_name: str):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=config("AWS_ACCESS_KEY_ID", default=None),
        aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY", default=None),
    )


def main():
    region = config("DOC_INTEL_SQS_REGION", default=config("COGNITO_REGION", default="us-east-1"))
    queue_url = config("DOC_INTEL_SQS_QUEUE_URL")
    wait_seconds = int(config("DOC_INTEL_SQS_WAIT_TIME", default="10"))
    max_messages = int(config("DOC_INTEL_SQS_MAX_MESSAGES", default="3"))
    visibility_timeout = int(config("DOC_INTEL_SQS_VISIBILITY_TIMEOUT", default="600"))
    idle_sleep = int(config("DOC_INTEL_IDLE_SLEEP_SECONDS", default="5"))
    storage_dir = config("DOC_INTEL_STORAGE_DIR", default="/tmp/doc_intel")
    storage_backend = config("DOC_INTEL_STORAGE_BACKEND", default="s3").lower()
    s3_bucket = config("DOC_INTEL_S3_BUCKET", default=config("S3_BUCKET", default=None))
    storage_prefix = config("DOC_INTEL_S3_PREFIX", default="doc_intel")
    worker_environment = config("DOC_INTEL_WORKER_ENVIRONMENT", default="prod")

    mongo_client = _build_mongo_client()
    db = AuditedDatabase(mongo_client.get_database(config("MONGO_DB", default="bcca-assistant")))
    modes_collection = db.get_collection("modes")
    projects_collection = db.get_collection("document_projects")
    documents_collection = db.get_collection("document_project_documents")
    jobs_collection = db.get_collection("document_intel_jobs")

    openai_client = OpenAI(api_key=config("OPENAI_API_KEY"))
    sqs = _build_sqs_client(region)
    s3 = _build_s3_client(region)

    toolbox = DocumentToolbox(openai_client=openai_client, storage_dir=storage_dir)
    storage = DocumentIntelligenceStorage(
        backend=storage_backend,
        local_storage_dir=storage_dir,
        s3_client=s3 if storage_backend == "s3" else None,
        bucket=s3_bucket if storage_backend == "s3" else None,
        key_prefix=storage_prefix,
        presign_expiry_seconds=int(config("DOC_INTEL_SIGNED_URL_SECONDS", default="3600")),
    )
    service = DocumentIntelligenceService(
        modes_collection=modes_collection,
        projects_collection=projects_collection,
        documents_collection=documents_collection,
        jobs_collection=jobs_collection,
        storage_dir=storage_dir,
        toolbox=toolbox,
        storage=storage,
        doc_client=None,
        expiry_minutes=int(config("DOC_INTEL_EXPIRY_MINUTES", default="30")),
        max_pdf_seconds=int(config("DOC_INTEL_MAX_PDF_SECONDS", default="90")),
        max_zip_depth=int(config("DOC_INTEL_MAX_ZIP_DEPTH", default="3")),
        max_zip_members=int(config("DOC_INTEL_MAX_ZIP_MEMBERS", default="500")),
        max_zip_member_size_bytes=int(config("DOC_INTEL_MAX_ZIP_MEMBER_MB", default="200")) * 1024 * 1024,
        max_zip_total_size_bytes=int(config("DOC_INTEL_MAX_ZIP_TOTAL_MB", default="2048")) * 1024 * 1024,
        max_zip_compression_ratio=float(config("DOC_INTEL_MAX_ZIP_COMPRESSION_RATIO", default="150")),
        max_text_file_bytes=int(config("DOC_INTEL_MAX_TEXT_FILE_MB", default="20")) * 1024 * 1024,
        max_text_chars=int(config("DOC_INTEL_MAX_TEXT_CHARS", default="180000")),
        max_excerpt_chars=int(config("DOC_INTEL_MAX_TEXT_EXCERPT_CHARS", default="24000")),
        max_embed_chars=int(config("DOC_INTEL_MAX_EMBED_CHARS", default="24000")),
        max_image_pixels=int(config("DOC_INTEL_MAX_IMAGE_PIXELS", default="35000000")),
    )
    processor = DocumentIntelligenceJobProcessor(
        service=service,
        jobs_collection=jobs_collection,
        environment=worker_environment,
    )

    log.info("Doc-intel worker started. Queue=%s Region=%s", queue_url, region)

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                WaitTimeSeconds=wait_seconds,
                MaxNumberOfMessages=max_messages,
                VisibilityTimeout=visibility_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed to receive doc-intel messages: %s", exc)
            time.sleep(idle_sleep)
            continue

        messages = response.get("Messages", [])
        if not messages:
            time.sleep(idle_sleep)
            continue

        for message in messages:
            receipt = message["ReceiptHandle"]
            try:
                request = DocumentIntelligenceJobRequest.from_message(message["Body"])
                processor.run_job_request(request)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            except Exception as exc:  # noqa: BLE001
                log.exception(
                    "Doc-intel job failed (request_id=%s). Leaving message for retry. Body=%s Error=%s",
                    message.get("MessageId"),
                    json.dumps(message["Body"]) if isinstance(message["Body"], dict) else message["Body"],
                    exc,
                )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Doc-intel worker interrupted by user. Shutting down.")
